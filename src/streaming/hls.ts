/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.ts: HLS streaming request handlers for PrismCast.
 */
import type { Channel, Nullable, ResolvedSiteProfile } from "../types/index.js";
import { LOG, delay, formatError, runWithStreamContext, startTimer } from "../utils/index.js";
import type { Request, Response } from "express";
import { StreamSetupError, createPageWithCapture, setupStream } from "./setup.js";
import { createHLSState, getAllStreams, getStream, getStreamCount, registerStream, updateLastAccess } from "./registry.js";
import { createInitialStreamStatus, emitStreamAdded } from "./statusEmitter.js";
import { deleteChannelStreamId, getChannelStreamId, isTerminationInitiated, setChannelStreamId, terminateStream } from "./lifecycle.js";
import { emitCurrentSystemStatus, isLoginModeActive, unregisterManagedPage } from "../browser/index.js";
import { getAllChannels, isPredefinedChannelDisabled } from "../config/userChannels.js";
import { getInitSegment, getPlaylist, getSegment, waitForPlaylist } from "./hlsSegments.js";
import { getResolvedChannel, resolveProviderKey } from "../config/providers.js";
import { CONFIG } from "../config/index.js";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.js";
import type { StreamRegistryEntry } from "./registry.js";
import type { TabReplacementHandlerFactory } from "./setup.js";
import type { TabReplacementResult } from "./monitor.js";
import { createFMP4Segmenter } from "./fmp4Segmenter.js";
import { createHash } from "node:crypto";
import { registerClient } from "./clients.js";
import { triggerShowNameUpdate } from "./showInfo.js";

/* This module handles HLS (HTTP Live Streaming) output using fMP4 (fragmented MP4) segments. HLS mode uses MP4/AAC capture from puppeteer-stream, which is then
 * segmented natively without any external dependencies. The overall flow is:
 *
 * 1. Client requests playlist at /hls/:name/stream.m3u8 (predefined channel) or /play?url=...&profile=... (ad-hoc URL)
 * 2. If no stream exists, we call initializeStream() which runs setupStream() and creates a native fMP4 segmenter
 * 3. The segmenter parses the MP4 stream and produces init.mp4 (codec config) + segment0.m4s, segment1.m4s, ...
 * 4. We store init segment and media segments in memory, return playlist to client
 * 5. Client fetches init.mp4 once, then media segments at /hls/:name/segmentN.m4s
 * 6. Idle timeout terminates streams with no recent segment requests
 *
 * Shared streams: If multiple clients request the same channel (or the same ad-hoc URL with the same profile), they share one segmenter. The first client triggers
 * stream creation, and subsequent clients get the existing playlist and segments. Ad-hoc streams are identified by a synthetic key ("play-<hash>") derived from the
 * URL and profile, allowing them to use the same channelToStreamId deduplication mechanism as predefined channels.
 */

// Channel Validation.

/**
 * Result of channel validation. On success, contains the resolved channel and provider key. On failure, contains the HTTP error details. The body field carries
 * either a plain string (for text error responses) or an object (for JSON error responses), so callers can use typeof to pick res.send() vs res.json().
 */
export type ValidateChannelResult =
  { channel: Channel; resolvedKey: string; valid: true } |
  { body: Record<string, string> | string; statusCode: number; valid: false };

// Login mode error body used by both validateChannel() and handlePlayStream() to ensure consistent response format.
const LOGIN_MODE_BODY: Record<string, string> = { error: "Login in progress", message: "Please complete authentication before starting new streams." };

/**
 * Validates a channel name for streaming. Performs all fast, synchronous checks: disabled status, provider resolution, channel lookup, and login mode. Returns a
 * discriminated union so callers can handle success and failure without coupling to Express response objects.
 *
 * This is extracted from ensureChannelStream() so it can be called by both HLS and MPEG-TS code paths without duplicating the validation logic.
 *
 * @param channelName - The channel key to validate.
 * @returns Validation result with channel data on success, or error details on failure.
 */
export function validateChannel(channelName: string): ValidateChannelResult {

  if(isPredefinedChannelDisabled(channelName)) {

    return { body: "Channel is disabled.", statusCode: 404, valid: false };
  }

  // Resolve provider selection. For multi-provider channels, this returns the user's selected provider key (e.g., "espn-disneyplus"). For single-provider channels
  // or if no selection exists, it returns the canonical key unchanged.
  const resolvedKey = resolveProviderKey(channelName);

  // Get the resolved channel with inheritance applied. For provider variants, this merges the variant's properties with inherited properties from the canonical
  // entry (name, stationId).
  const channel = getResolvedChannel(resolvedKey);

  // Fall back to getAllChannels if the resolved channel doesn't exist (e.g., for ad-hoc streams or non-grouped channels).
  const effectiveChannel = channel ?? getAllChannels()[channelName];

  // Log a warning if a provider selection resolved to a missing variant (e.g., variant was removed from channels after selection was saved).
  if(!channel && (resolvedKey !== channelName)) {

    LOG.warn("Provider '%s' not found for channel '%s'. Using default provider.", resolvedKey, channelName);
  }

  // Runtime check needed even though TypeScript thinks channel is always defined (Record indexing quirk).
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!effectiveChannel) {

    return { body: "Channel not found.", statusCode: 404, valid: false };
  }

  // Block new stream requests while login mode is active. This prevents the browser from being disrupted during authentication.
  if(isLoginModeActive()) {

    return { body: LOGIN_MODE_BODY, statusCode: 503, valid: false };
  }

  return { channel: effectiveChannel, resolvedKey, valid: true };
}

/**
 * Sends a validation error response to the client. Handles both plain text bodies (via res.send) and object bodies (via res.json).
 * @param validation - The failed validation result.
 * @param res - Express response object.
 */
export function sendValidationError(validation: { body: Record<string, string> | string; statusCode: number }, res: Response): void {

  if(typeof validation.body === "object") {

    res.status(validation.statusCode).json(validation.body);
  } else {

    res.status(validation.statusCode).send(validation.body);
  }
}

// Public Endpoint Handlers.

/**
 * Ensures a stream is running for a channel. If no stream exists, starts one. If a stream startup is in progress (-1 sentinel), waits for it to complete. Returns
 * the stream ID if successful, or null if an error occurred (with the error response already sent to the client).
 *
 * The existing-stream check runs first so that ad-hoc streams (registered under synthetic keys like "play-a1b2c3d4") can be served without failing the
 * "Channel not found" check.
 *
 * For channels with multiple providers (e.g., ESPN via ESPN.com or Disney+), the user's provider selection is resolved before looking up the channel definition.
 * The stream is registered under the canonical key (channelName) for deduplication, but uses the resolved provider's URL and settings.
 *
 * @param channelName - The channel key (or synthetic ad-hoc key) to stream.
 * @param req - Express request object (for profile override and client IP).
 * @param res - Express response object (for error responses).
 * @returns The stream ID if a stream is running, or null if an error occurred.
 */
export async function ensureChannelStream(channelName: string, req: Request, res: Response): Promise<Nullable<number>> {

  // Check for an existing stream first. This must happen before channel validation so that ad-hoc streams (registered under synthetic keys like "play-a1b2c3d4") can
  // be served by the standard HLS playlist handler without failing the "Channel not found" check. A stream in channelToStreamId was already validated when it was
  // started, so no re-validation is needed.
  const streamId = getChannelStreamId(channelName);

  // If a stream is already running (not a startup-in-progress sentinel), return it directly.
  if((streamId !== undefined) && (streamId !== -1)) {

    return streamId;
  }

  // If a startup is in progress (-1 sentinel), another request is already starting this stream. Poll until the real stream ID appears or we timeout.
  if(streamId === -1) {

    return awaitStreamReady(channelName, res);
  }

  // No existing stream — validate the channel and start a new one. Channel validation is only needed for new streams because existing streams were already validated
  // at startup time.
  const validation = validateChannel(channelName);

  if(!validation.valid) {

    sendValidationError(validation, res);

    return null;
  }

  // Start the stream using the resolved channel's URL. The stream is registered under channelName (canonical key) for deduplication, but uses the resolved
  // provider's definition.
  const newStreamId = await startHLSStream(channelName, validation.channel.url, req, res, validation.channel);

  if(newStreamId === null) {

    // Error response already sent by startHLSStream.
    return null;
  }

  return newStreamId;
}

/**
 * Handles HLS playlist requests. Ensures a stream is running for the channel (blocking until ready if a new stream must start), then returns the playlist.
 *
 * Route: GET /hls/:name/stream.m3u8
 *
 * @param req - Express request object.
 * @param res - Express response object.
 */
export async function handleHLSPlaylist(req: Request, res: Response): Promise<void> {

  const channelName = (req.params as { name?: string }).name;

  if(!channelName) {

    res.status(400).send("Channel name is required.");

    return;
  }

  const clientAddress = req.ip ?? req.socket.remoteAddress ?? "unknown";

  const streamId = await ensureChannelStream(channelName, req, res);

  if(streamId === null) {

    return;
  }

  await sendPlaylistResponse(streamId, clientAddress, res);
}

/**
 * Handles HLS segment requests. Returns the requested segment from memory. Supports both the fMP4 initialization segment (init.mp4) and media segments (.m4s).
 *
 * Route: GET /hls/:name/:segment
 *
 * @param req - Express request object.
 * @param res - Express response object.
 */
export function handleHLSSegment(req: Request, res: Response): void {

  const channelName = (req.params as { name?: string }).name;
  const segmentName = (req.params as { segment?: string }).segment;

  if(!channelName || !segmentName) {

    res.status(400).send("Channel name and segment name are required.");

    return;
  }

  const streamId = getChannelStreamId(channelName);

  if((streamId === undefined) || (streamId === -1)) {

    res.status(404).send("Stream not found.");

    return;
  }

  // Handle init segment (init.mp4) separately from media segments (.m4s).
  if(segmentName === "init.mp4") {

    const initSegment = getInitSegment(streamId);

    if(!initSegment) {

      res.status(404).send("Init segment not found.");

      return;
    }

    updateLastAccess(streamId);
    sendSegment(initSegment, res);

    return;
  }

  // Handle media segments (.m4s).
  const segment = getSegment(streamId, segmentName);

  if(!segment) {

    res.status(404).send("Segment not found.");

    return;
  }

  updateLastAccess(streamId);
  sendSegment(segment, res);
}

// Ad-Hoc Streaming.

/**
 * Handles ad-hoc stream requests for arbitrary URLs. Generates a deterministic synthetic key from the URL and profile, starts a stream if none exists, and redirects
 * to the standard HLS playlist path. This enables streaming URLs that are not predefined as channels.
 *
 * The synthetic key includes the profile so that the same URL with different profiles produces separate streams. The "play-" prefix prevents collisions with
 * predefined channel names.
 *
 * Route: GET /play?url=<url>&profile=<name>
 *
 * @param req - Express request object.
 * @param res - Express response object.
 */
export async function handlePlayStream(req: Request, res: Response): Promise<void> {

  const url = (req.query.url as string | undefined)?.trim();

  if(!url) {

    res.status(400).send("The url query parameter is required.");

    return;
  }

  const clickSelector = req.query.clickSelector as string | undefined;
  const clickToPlay = req.query.clickToPlay === "true";
  const profileOverride = req.query.profile as string | undefined;
  const selector = req.query.selector as string | undefined;

  // Generate a deterministic synthetic key from the trimmed URL, profile, selector, clickToPlay, and clickSelector. Including these ensures that the same URL with
  // different options produces separate streams. The newline delimiter is safe since URLs cannot contain literal newlines.
  const channelName = "play-" + createHash("sha256").update(
    url + "\n" + (profileOverride ?? "") + "\n" + (selector ?? "") + "\n" + (clickToPlay ? "1" : "") + "\n" + (clickSelector ?? "")
  ).digest("hex").slice(0, 8);

  // Check for an existing stream.
  const streamId = getChannelStreamId(channelName);

  // If a stream is already running, redirect immediately.
  if((streamId !== undefined) && (streamId !== -1)) {

    res.redirect(302, "/hls/" + channelName + "/stream.m3u8");

    return;
  }

  // If a startup is in progress (-1 sentinel), another request is already starting this stream. Poll until the real stream ID appears or we timeout, then redirect.
  if(streamId === -1) {

    const resolvedId = await awaitStreamReady(channelName, res);

    if(resolvedId === null) {

      // Error response already sent by awaitStreamReady.
      return;
    }

    res.redirect(302, "/hls/" + channelName + "/stream.m3u8");

    return;
  }

  // Block new stream requests while login mode is active.
  if(isLoginModeActive()) {

    res.status(503).json(LOGIN_MODE_BODY);

    return;
  }

  // Capture client IP for Channels DVR API integration.
  const clientAddress: Nullable<string> = req.ip ?? req.socket.remoteAddress ?? null;

  // Start a new ad-hoc stream. initializeStream handles capture setup, segmenter creation, and event emission.
  try {

    const newStreamId = await initializeStream({ channelName, channelSelector: selector, clickSelector, clickToPlay, clientAddress, profileOverride, url });

    if(newStreamId === null) {

      res.status(500).send("Stream terminated during startup.");

      return;
    }
  } catch(error) {

    if(error instanceof StreamSetupError) {

      if(error.statusCode === 503) {

        res.setHeader("Retry-After", "10");
      }

      res.status(error.statusCode).send(error.userMessage);

      return;
    }

    LOG.error("Unexpected error during ad-hoc stream setup: %s.", formatError(error));

    res.status(500).send("Internal server error.");

    return;
  }

  res.redirect(302, "/hls/" + channelName + "/stream.m3u8");
}

// Startup Polling.

/**
 * Polls for a stream startup to complete. The -1 sentinel in channelToStreamId signals that startup is in progress. Returns the resolved stream ID on success, null
 * if startup failed (sentinel removed), or undefined if the timeout expired while startup is still active.
 *
 * This is the shared inner polling loop used by both awaitStreamReady() (which sends error responses) and awaitStreamReadySilent() (which does not).
 *
 * @param channelName - The channel name (or synthetic ad-hoc key) to poll.
 * @returns The resolved stream ID, null if startup failed, or undefined if timed out.
 */
async function pollStreamReady(channelName: string): Promise<Nullable<number> | undefined> {

  const pollInterval = 200;
  const deadline = Date.now() + CONFIG.streaming.navigationTimeout;

  while(Date.now() < deadline) {

    // eslint-disable-next-line no-await-in-loop
    await delay(pollInterval);

    const streamId = getChannelStreamId(channelName);

    // The startup failed and the sentinel was removed.
    if(streamId === undefined) {

      return null;
    }

    // Real stream ID is now available.
    if(streamId !== -1) {

      return streamId;
    }
  }

  // Timed out waiting for the startup to complete.
  return undefined;
}

/**
 * Waits for a stream startup to complete. This is used when a second request arrives while the first is still starting the stream. The -1 sentinel in
 * channelToStreamId signals that startup is in progress. We poll until the sentinel is replaced with a real stream ID, removed (startup failed), or the
 * timeout expires.
 *
 * On failure, the appropriate error response is sent to the client and null is returned.
 *
 * @param channelName - The channel name (or synthetic ad-hoc key) to poll.
 * @param res - Express response object for sending error responses on failure.
 * @returns The resolved stream ID on success, or null if startup failed or timed out (error response already sent).
 */
async function awaitStreamReady(channelName: string, res: Response): Promise<Nullable<number>> {

  const result = await pollStreamReady(channelName);

  // Startup failed (sentinel removed).
  if(result === null) {

    res.status(500).send("Stream startup failed.");

    return null;
  }

  // Timed out.
  if(result === undefined) {

    res.setHeader("Retry-After", "5");
    res.status(503).send("Stream is starting. Please retry.");

    return null;
  }

  return result;
}

/**
 * Waits for a stream startup to complete without sending any HTTP responses. Used by MPEG-TS when headers have already been flushed and error responses cannot be
 * sent.
 *
 * @param channelName - The channel name (or synthetic ad-hoc key) to poll.
 * @returns The resolved stream ID on success, or null if startup failed or timed out.
 */
export async function awaitStreamReadySilent(channelName: string): Promise<Nullable<number>> {

  const result = await pollStreamReady(channelName);

  // Both null (failed) and undefined (timed out) map to null for the silent variant.
  if((result === null) || (result === undefined)) {

    return null;
  }

  return result;
}

// Response Helpers.

/**
 * Sends the playlist for a stream, waiting for the first playlist if needed. Handles client registration and access tracking. This is the shared pattern used by
 * multiple code paths in handleHLSPlaylist() to avoid duplicating the get-wait-check-register-send sequence.
 * @param streamId - The numeric stream ID.
 * @param clientAddress - Client address for tracking.
 * @param res - Express response object.
 */
async function sendPlaylistResponse(streamId: number, clientAddress: string, res: Response): Promise<void> {

  // Try to get an existing playlist first.
  let playlist = getPlaylist(streamId);

  // If no playlist yet, wait for the first one.
  if(!playlist) {

    const playlistReady = await waitForPlaylist(streamId, CONFIG.streaming.navigationTimeout);

    if(!playlistReady) {

      res.setHeader("Retry-After", "5");
      res.status(503).send("Stream is starting. Please retry.");

      return;
    }

    playlist = getPlaylist(streamId);

    if(!playlist) {

      res.status(500).send("Playlist not available.");

      return;
    }

    // Log the time from stream start to first playlist delivery. This only fires for the initial playlist wait, not for subsequent playlist polls.
    const stream = getStream(streamId);

    if(stream) {

      const elapsed = ((Date.now() - stream.startTime.getTime()) / 1000).toFixed(3);

      LOG.debug("timing:hls", "Playlist delivered to client in %ss.", elapsed);
    }
  }

  updateLastAccess(streamId);
  registerClient(streamId, clientAddress, "hls");
  sendPlaylist(playlist, res);
}

/**
 * Sends a playlist string as an HLS response with appropriate headers.
 * @param playlist - The M3U8 playlist content.
 * @param res - Express response object.
 */
function sendPlaylist(playlist: string, res: Response): void {

  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
  res.send(playlist);
}

/**
 * Sends a segment buffer as a video/mp4 response with appropriate headers.
 * @param data - The segment data.
 * @param res - Express response object.
 */
function sendSegment(data: Buffer, res: Response): void {

  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Content-Type", "video/mp4");
  res.send(data);
}

// Stream Lifecycle.

/**
 * Cleans up resources from a stream that was terminated during setup before the segmenter was stored in the registry. This handles the rare race condition where
 * another code path (e.g., circuit breaker) calls terminateStream() between registerStream() and the segmenter assignment.
 *
 * terminateStream() already cleaned up the page, monitor, channel mapping, and registry entry. We only need to stop the segmenter, which was created after
 * termination occurred and therefore wasn't cleaned up.
 * @param segmenter - The orphaned fMP4 segmenter instance to stop.
 */
function cleanupOrphanedSetup(segmenter: FMP4SegmenterResult): void {

  LOG.debug("streaming:setup", "Stream was terminated during setup. Stopping orphaned segmenter.");
  segmenter.stop();
}

/**
 * Creates a tab replacement handler for recovery from unresponsive browser tabs. When the monitor detects 3+ consecutive evaluate timeouts, it calls this handler to:
 * 1. Stop the current segmenter and FFmpeg process
 * 2. Close the unresponsive page
 * 3. Create a fresh page with new capture
 * 4. Create a new segmenter piped to the new capture
 * 5. Update the registry with the new resources
 * 6. Return the new page and context for the monitor to continue
 *
 * The handler preserves existing HLS segments and marks a discontinuity so clients know the stream parameters may have changed.
 *
 * @param numericStreamId - The stream's numeric ID for registry lookups.
 * @param streamId - The stream's string ID for logging.
 * @param channelName - The channel name (or synthetic ad-hoc key like "play-a1b2c3d4") used as the store key for error callbacks and termination.
 * @param url - The URL to navigate to.
 * @param profile - The site profile for video handling.
 * @param metadataComment - Optional comment to embed in FFmpeg output metadata.
 * @param onCircuitBreak - Callback for circuit breaker trips during replacement.
 * @returns A handler function that performs tab replacement, or null if the stream no longer exists.
 */
function createTabReplacementHandler(
  numericStreamId: number,
  streamId: string,
  channelName: string,
  url: string,
  profile: ResolvedSiteProfile,
  metadataComment: string | undefined,
  onCircuitBreak: () => void
): () => Promise<Nullable<TabReplacementResult>> {

  return async (): Promise<Nullable<TabReplacementResult>> => {

    const tabElapsed = startTimer();

    // Get the current stream entry.
    const stream = getStream(numericStreamId);

    if(!stream) {

      LOG.debug("recovery:tab", "Tab replacement requested but stream %s no longer exists.", streamId);

      return null;
    }

    // Get the current init segment, segment index, and per-track timestamps from the old segmenter before stopping it. The init segment enables discontinuity
    // suppression when codec parameters are unchanged, the segment index allows the new segmenter to continue numbering, and the track timestamps ensure monotonic
    // baseMediaDecodeTime across capture restarts.
    const currentInitSegment = stream.segmenter?.getInitSegment();
    const currentInitVersion = stream.segmenter?.getInitVersion() ?? 0;
    const currentSegmentIndex = stream.segmenter?.getSegmentIndex() ?? 0;
    const currentTrackTimestamps = stream.segmenter?.getTrackTimestamps();

    // Destroy the OLD capture stream first. This MUST happen before closing the page to ensure chrome.tabCapture releases the capture. Without this, the new
    // getStream() call would hang with "Cannot capture a tab with an active stream" error.
    if(stream.rawCaptureStream && !stream.rawCaptureStream.destroyed) {

      LOG.debug("recovery:tab", "Destroying old capture stream for tab replacement.");
      stream.rawCaptureStream.destroy();
    }

    // Stop the current segmenter if it exists.
    if(stream.segmenter) {

      LOG.debug("recovery:tab", "Stopping current segmenter for tab replacement.");
      stream.segmenter.stop();
    }

    // Stop the FFmpeg process if it exists.
    if(stream.ffmpegProcess) {

      LOG.debug("recovery:tab", "Stopping FFmpeg process for tab replacement.");
      stream.ffmpegProcess.kill();
    }

    // Close the current page.
    const oldPage = stream.page;

    unregisterManagedPage(oldPage);

    if(!oldPage.isClosed()) {

      LOG.debug("recovery:tab", "Closing unresponsive page for tab replacement.");

      oldPage.close().catch((error) => {

        LOG.debug("recovery:tab", "Page close error during tab replacement: %s.", formatError(error));
      });
    }

    LOG.debug("timing:tab", "Old tab cleanup complete. (+%sms)", tabElapsed());

    // Create a new page with capture.
    let captureResult;

    try {

      captureResult = await createPageWithCapture({

        comment: metadataComment,
        onFFmpegError: (error) => {

          LOG.error("FFmpeg error during tab replacement recovery: %s.", formatError(error));
          onCircuitBreak();
        },
        profile,
        streamId,
        url
      });
    } catch(error) {

      LOG.warn("Failed to create new page during tab replacement: %s.", formatError(error));

      return null;
    }

    LOG.debug("timing:tab", "New page with capture created. (+%sms)", tabElapsed());

    // Create a new segmenter for the new capture stream. Continue from the current segment index for playlist continuity, pass the per-track timestamp counters
    // for monotonic baseMediaDecodeTime, and mark the first segment with a discontinuity tag so clients know the stream parameters may have changed.
    const newSegmenter = createFMP4Segmenter({

      initialTrackTimestamps: currentTrackTimestamps,

      onError: (error: Error) => {

        if(isTerminationInitiated(numericStreamId)) {

          return;
        }

        LOG.error("Segmenter error after tab replacement for %s: %s.", channelName, formatError(error));

        terminateStream(numericStreamId, channelName, "stream processing error after recovery");
        void emitCurrentSystemStatus();
      },

      onStop: () => {

        if(isTerminationInitiated(numericStreamId)) {

          return;
        }

        LOG.error("Segmenter stopped unexpectedly after tab replacement for %s.", channelName);

        terminateStream(numericStreamId, channelName, "stream ended unexpectedly after recovery");
        void emitCurrentSystemStatus();
      },

      pendingDiscontinuity: true,
      previousInitSegment: currentInitSegment,
      startingInitVersion: currentInitVersion,
      startingSegmentIndex: currentSegmentIndex,
      streamId: numericStreamId
    });

    // Pipe the new capture to the new segmenter.
    newSegmenter.pipe(captureResult.captureStream);

    // Update the registry entry with the new resources.
    stream.ffmpegProcess = captureResult.ffmpegProcess;
    stream.page = captureResult.page;
    stream.rawCaptureStream = captureResult.rawCaptureStream;
    stream.segmenter = newSegmenter;

    LOG.info("Tab replacement complete. New capture started with segment continuity.");

    LOG.debug("timing:tab", "Tab replacement complete. Total: %sms.", tabElapsed());

    return {

      context: captureResult.context,
      page: captureResult.page
    };
  };
}

// Stream Initialization.

/**
 * Options for initializing a stream.
 */
interface InitializeStreamOptions {

  // Channel definition. Undefined for ad-hoc URL streams.
  channel?: Channel;

  // Channel selector for multi-channel sites (e.g., "E-_East" for usanetwork.com/live). Only used for ad-hoc streams; predefined channels get their selector from
  // the channel definition via getProfileForChannel.
  channelSelector?: string;

  // Key for channelToStreamId registration and cleanup. For predefined channels, this is the channel key (e.g., "nbc"). For ad-hoc streams, this is the synthetic
  // hash key (e.g., "play-a1b2c3d4"). This value is used consistently for circuit breaker callbacks, tab replacement, and terminateStream.
  channelName: string;

  // Client IP address for Channels DVR API integration.
  clientAddress: Nullable<string>;

  // Click selector for play button overlays on ad-hoc streams. When set, also enables clickToPlay behavior.
  clickSelector?: string;

  // Whether to click an element to start playback. When true without clickSelector, clicks the video element.
  clickToPlay?: boolean;

  // Profile name to override auto-detection, from query parameter.
  profileOverride?: string;

  // The URL to stream.
  url: string;
}

/**
 * Initializes a new HLS stream. This is the shared stream startup logic used by both channel-based and ad-hoc streams. It handles browser capture setup, segmenter
 * creation, stream registration, and event emission.
 *
 * A -1 sentinel is set in channelToStreamId during setup to prevent duplicate stream starts. On success, the sentinel is replaced with the real stream ID. On
 * failure, the sentinel is removed and the error is re-thrown for the caller to handle HTTP error responses appropriately (channel-based streams need HDHomeRun
 * headers, ad-hoc streams do not).
 *
 * @param options - Stream initialization options.
 * @returns The stream ID on success, or null if the stream was terminated during the narrow setup window (orphaned setup race condition).
 * @throws StreamSetupError if setup fails, or Error for unexpected failures.
 */
export async function initializeStream(options: InitializeStreamOptions): Promise<Nullable<number>> {

  const { channel, channelName, channelSelector, clickSelector, clickToPlay, clientAddress, profileOverride, url } = options;

  // Set a -1 sentinel to prevent duplicate stream starts while we're setting up.
  const startupSentinel = -1;

  setChannelStreamId(channelName, startupSentinel);

  let setup;

  // Circuit breaker callback — terminate the stream on unrecoverable errors.
  const onCircuitBreak = (): void => {

    const streamId = getChannelStreamId(channelName);

    if((streamId !== undefined) && (streamId !== startupSentinel)) {

      terminateStream(streamId, channelName, "too many errors");
      void emitCurrentSystemStatus();
    }
  };

  // Factory to create the tab replacement handler. Called by setupStream after stream IDs are generated, allowing the handler to be created with access to those IDs.
  const tabReplacementFactory: TabReplacementHandlerFactory = (numericStreamId, streamId, profile, metadataComment) => {

    return createTabReplacementHandler(numericStreamId, streamId, channelName, url, profile, metadataComment, onCircuitBreak);
  };

  // If at capacity, try to reclaim an idle stream before starting setup. This avoids rejecting new requests when idle streams can be freed.
  if(getStreamCount() >= CONFIG.streaming.maxConcurrentStreams) {

    reclaimIdleStream();
  }

  try {

    // Pass channelName to setupStream only for predefined channels. For ad-hoc streams, omitting it causes generateStreamId to derive the stream ID string from the
    // URL (e.g., "foxsports-abc123"), which is more informative in logs than the synthetic hash key.
    setup = await setupStream(
      {

        channel,
        channelName: channel ? channelName : undefined,
        channelSelector: channel ? undefined : channelSelector,
        clickSelector: channel ? undefined : clickSelector,
        clickToPlay: channel ? undefined : clickToPlay,
        onTabReplacementFactory: tabReplacementFactory,
        profileOverride,
        url
      },
      onCircuitBreak
    );
  } catch(error) {

    // Remove startup sentinel on failure and re-throw for the caller to handle error responses.
    deleteChannelStreamId(channelName);

    throw error;
  }

  // Update the channel mapping with the real stream ID.
  setChannelStreamId(channelName, setup.numericStreamId);

  // Continue within stream context for consistent logging.
  return runWithStreamContext(
    { channelName: channel?.name, streamId: setup.streamId, url: setup.url },
    // eslint-disable-next-line @typescript-eslint/require-await
    async () => {

      // Register with null segmenter first because segmenter callbacks (onError, onStop) need the stream to exist in the registry for cleanup logic. The segmenter is
      // assigned immediately after creation below.
      registerStream({

        channelName: channel?.name ?? null,
        clientAddress,
        ffmpegProcess: setup.ffmpegProcess,
        hls: createHLSState(),
        id: setup.numericStreamId,
        info: {

          lastPlaylistRequest: Date.now(),
          storeKey: channelName
        },
        mpegTsClientCount: 0,
        page: setup.page,
        profile: setup.profile,
        rawCaptureStream: setup.rawCaptureStream,
        segmenter: null,
        startTime: setup.startTime,
        stopMonitor: setup.stopMonitor,
        streamIdStr: setup.streamId,
        url: setup.url
      });

      // Create the native fMP4 segmenter to parse the MP4/AAC stream into HLS segments.
      const segmenter = createFMP4Segmenter({

        onError: (error: Error) => {

          // Skip error handling if termination was already initiated.
          if(isTerminationInitiated(setup.numericStreamId)) {

            return;
          }

          LOG.error("Segmenter error for %s: %s.", channelName, formatError(error));

          terminateStream(setup.numericStreamId, channelName, "stream processing error");
          void emitCurrentSystemStatus();
        },

        onStop: () => {

          // Skip handling if termination was already initiated.
          if(isTerminationInitiated(setup.numericStreamId)) {

            return;
          }

          LOG.error("Segmenter stopped unexpectedly for %s.", channelName);

          terminateStream(setup.numericStreamId, channelName, "stream ended unexpectedly");
          void emitCurrentSystemStatus();
        },

        streamId: setup.numericStreamId
      });

      // Pipe the capture stream to the segmenter.
      segmenter.pipe(setup.captureStream);

      // Store the segmenter reference in the registry.
      const stream = getStream(setup.numericStreamId);

      if(stream) {

        stream.segmenter = segmenter;
      } else {

        // Stream was terminated during setup (rare race condition). Clean up the orphaned segmenter.
        cleanupOrphanedSetup(segmenter);

        return null;
      }

      const captureMode = CONFIG.streaming.captureMode === "ffmpeg" ? "FFmpeg" : "Native";
      const displayName = channel?.name ?? url;

      const tuneTime = ((Date.now() - setup.startTime.getTime()) / 1000).toFixed(1);

      LOG.info("Streaming %s (%s, %s). Tuned in %ss.", displayName, setup.profileName, captureMode, tuneTime);

      // Emit stream added event.
      emitStreamAdded(createInitialStreamStatus({

        channelName: channel?.name ?? null,
        numericStreamId: setup.numericStreamId,
        providerName: setup.providerName,
        startTime: setup.startTime,
        url: setup.url
      }));
      void emitCurrentSystemStatus();

      // Trigger show name lookup for the new stream.
      triggerShowNameUpdate();

      return setup.numericStreamId;
    }
  );
}

// Channel Stream Startup.

/**
 * Starts a new HLS stream for a predefined channel. Delegates to initializeStream() for the actual setup. Error responses are sent directly to the client, including
 * HDHomeRun-specific headers for capacity errors.
 *
 * @param channelName - The channel key (canonical key for stream registration and deduplication).
 * @param url - The URL to stream (from the resolved provider).
 * @param req - Express request object (for profile override and client IP).
 * @param res - Express response object (for error responses).
 * @param channel - The resolved channel definition (with inheritance applied for provider variants).
 * @returns The stream ID if successful, null if an error occurred (error response already sent).
 */
async function startHLSStream(channelName: string, url: string, req: Request, res: Response, channel?: Channel): Promise<Nullable<number>> {

  const profileOverride = req.query.profile as string | undefined;
  const clientAddress: Nullable<string> = req.ip ?? req.socket.remoteAddress ?? null;

  try {

    return await initializeStream({ channel, channelName, clientAddress, profileOverride, url });
  } catch(error) {

    if(error instanceof StreamSetupError) {

      if(error.statusCode === 503) {

        res.setHeader("Retry-After", "10");
        res.setHeader("X-HDHomeRun-Error", "All Tuners In Use");
      }

      res.status(error.statusCode).send(error.userMessage);

      return null;
    }

    LOG.error("Unexpected error during HLS stream setup: %s.", formatError(error));

    res.status(500).send("Internal server error.");

    return null;
  }
}

// Idle Detection.

/**
 * Checks for idle streams and terminates them. Called periodically by the idle detection interval.
 */
export function cleanupIdleStreams(): void {

  const streams = getAllStreams();
  const now = Date.now();
  let terminatedCount = 0;

  for(const stream of streams) {

    // Skip streams with active MPEG-TS clients. These streams are still being consumed even if no HLS playlist requests have been made recently.
    if(stream.mpegTsClientCount > 0) {

      continue;
    }

    const idleTime = now - stream.info.lastPlaylistRequest;

    if(idleTime >= CONFIG.hls.idleTimeout) {

      terminateStream(stream.id, stream.info.storeKey, "no active clients");
      terminatedCount++;
    }
  }

  // Emit system status once after all idle streams are terminated.
  if(terminatedCount > 0) {

    void emitCurrentSystemStatus();
  }
}

/**
 * Attempts to reclaim a single idle stream to free capacity for a new request. Finds the stream that has been idle the longest and terminates it. A stream is
 * considered idle when it has no MPEG-TS clients and its last access exceeds the idle timeout. This is called when the concurrent stream limit is reached, allowing
 * channel-surfing users to get new streams without being rejected while abandoned streams linger.
 * @returns True if a stream was reclaimed, false if no idle streams exist.
 */
function reclaimIdleStream(): boolean {

  const streams = getAllStreams();
  const now = Date.now();
  let oldest: Nullable<StreamRegistryEntry> = null;

  for(const stream of streams) {

    // Skip streams with active MPEG-TS clients.
    if(stream.mpegTsClientCount > 0) {

      continue;
    }

    const idleTime = now - stream.info.lastPlaylistRequest;

    // Only consider streams that have exceeded the idle timeout, and pick the one that has been idle the longest.
    if((idleTime >= CONFIG.hls.idleTimeout) && (!oldest || (stream.info.lastPlaylistRequest < oldest.info.lastPlaylistRequest))) {

      oldest = stream;
    }
  }

  if(!oldest) {

    return false;
  }

  LOG.info("Reclaiming idle stream %s (%s) to free capacity.", oldest.id, oldest.info.storeKey);

  terminateStream(oldest.id, oldest.info.storeKey, "reclaimed for new stream");
  void emitCurrentSystemStatus();

  return true;
}
