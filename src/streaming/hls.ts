/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.ts: HLS streaming request handlers for PrismCast.
 */
import type { HLSState, StreamRegistryEntry } from "./registry.ts";
import { LOG, formatError, runWithStreamContext, startTimer } from "../utils/index.ts";
import type { Nullable, ResolvedChannel, ResolvedSiteProfile } from "../types/index.ts";
import type { Request, Response } from "express";
import { StreamSetupError, createPageWithCapture, generateStreamId, setupStream } from "./setup.ts";
import { createHLSState, getAllStreams, getNextStreamId, getStream, getStreamCount, registerStream, updateLastAccess } from "./registry.ts";
import { createInitialStreamStatus, emitStreamAdded } from "./statusEmitter.ts";
import { deleteResumeData, getResumeSegmentIndex, peekResumeData } from "./hlsResume.ts";
import { emitCurrentSystemStatus, isLoginModeActive, unregisterManagedPage } from "../browser/index.ts";
import { generatePrerollPlaylist, getPrerollCodec, getPrerollSegmentCount, isPrerollReady } from "./preroll.ts";
import { getAllChannels, getChannelLogo, isPredefinedChannelDisabled } from "../config/userChannels.ts";
import { getAudioPlaylist, getAudioSegment, getInitSegment, getPlaylist, getSegment, getVideoPlaylist, waitForPlaylist } from "./hlsSegments.ts";
import { getAuthDomainForChannel, getResolvedChannel, getServiceTagForChannel, isChannelAvailableByService, resolveServiceKey } from "../config/services.ts";
import { getChannelStreamId, isTerminationInitiated, setChannelStreamId, terminateStream } from "./lifecycle.ts";
import { getEffectiveCaptureCodec, isCaptureHardwareAccelerated } from "./codec.ts";
import { markChannelFailure, markChannelSuccess } from "../config/health.ts";
import { CONFIG } from "../config/index.ts";
import type { CaptureCodec } from "./codec.ts";
import type { StreamSetupResult } from "./setup.ts";
import type { TabReplacementHandlerFactory } from "./setup.ts";
import type { TabReplacementResult } from "./recovery.ts";
import { attemptNativeStreaming } from "../native/index.ts";
import { clearProbeCache } from "../native/probe.ts";
import { createFMP4Segmenter } from "./fmp4Segmenter.ts";
import { createHash } from "node:crypto";
import { getProviderBySlug } from "../browser/channelSelection.ts";
import { registerClient } from "./clients.ts";
import { suppressPageAudio } from "../browser/video.ts";
import { triggerShowNameUpdate } from "./showInfo.ts";

/* This module handles HLS (HTTP Live Streaming) output using fMP4 (fragmented MP4) segments. HLS mode uses MP4/AAC capture from puppeteer-stream, which is then
 * segmented natively without any external dependencies. The stream initialization flow has three phases:
 *
 * Phase 1 - Registration (synchronous, in the request handler):
 *   The client requests a playlist. If no stream exists and preroll is available, a pending registry entry is registered immediately with a deferred preroll timer.
 *   The client receives a valid, playable playlist on the first request - no blocking wait for the real stream. If preroll is unavailable, the request blocks until
 *   stream setup completes (fallback path).
 *
 * Phase 2 - Browser setup (async, fire-and-forget from the request handler):
 *   setupStream() creates the browser page, navigates to the URL, initializes playback, and starts capture. This produces the StreamSetupResult with the capture
 *   stream, page, profile, and monitor. The pending registry entry is filled in with these references.
 *
 * Phase 3 - Streaming pipeline (async, after browser setup):
 *   If the service's manifest is interceptable, native HLS streaming is attempted via startNativeProxy(). If native is viable, the capture pipeline is stopped and
 *   the proxy takes over. Otherwise, createCaptureSegmenter() creates the fMP4 segmenter and pipes the capture stream. When the first real playlist arrives
 *   (from either the segmenter or native proxy), the preroll timer is cancelled and the client receives live content on the next poll.
 *
 * Shared streams: If multiple clients request the same channel (or the same ad-hoc URL with the same profile, selector, clickToPlay, and clickSelector), they share
 * one stream. The first client triggers stream creation, and subsequent clients get the existing playlist and segments. Ad-hoc streams are identified by a
 * synthetic key ("play-<hash>") derived from the URL, profile, selector, clickToPlay, and clickSelector, allowing them to use the same channelToStreamId
 * deduplication mechanism as predefined channels.
 */

// Delay before seeding the preroll playlist in milliseconds. If stream setup completes before this timer fires, the preroll is skipped and the client receives real
// content directly. This ensures fast-tuning services (native HLS at 2-3s, most capture services at 4-7s) never see preroll, while slow services (Xfinity/Cox at
// 13-15s) get preroll content after the delay to prevent HTTP timeouts.
const PREROLL_DELAY_MS = 9000;

/**
 * Builds the onError/onStop callbacks for createFMP4Segmenter. Both callbacks share the same termination chain (skip-if-already-terminating guard, log error,
 * terminate stream, emit current system status); the only differences between callsites are the log-message and termination-reason suffixes used to mark the
 * post-tab-replacement scenario. Centralizing the shape prevents drift if the termination contract evolves.
 * @param streamId - The numeric stream id.
 * @param channelName - The channel name for log messages.
 * @param options - Optional logSuffix appended to the log message; optional reasonSuffix appended to the termination reason. Both default to no suffix.
 * @returns onError and onStop callbacks ready to spread into createFMP4Segmenter options.
 */
function buildSegmenterTerminationHandlers(streamId: number, channelName: string,
  options: { logSuffix?: string; reasonSuffix?: string } = {}): { onError: (error: Error) => void; onStop: () => void } {

  const logSfx = options.logSuffix ? " " + options.logSuffix : "";
  const reasonSfx = options.reasonSuffix ? " " + options.reasonSuffix : "";

  return {

    onError: (error: Error): void => {

      if(isTerminationInitiated(streamId)) {

        return;
      }

      LOG.error("Segmenter error" + logSfx + " for %s: %s.", channelName, formatError(error));

      terminateStream(streamId, channelName, "stream processing error" + reasonSfx);
      void emitCurrentSystemStatus();
    },
    onStop: (): void => {

      if(isTerminationInitiated(streamId)) {

        return;
      }

      LOG.error("Segmenter stopped unexpectedly" + logSfx + " for %s.", channelName);

      terminateStream(streamId, channelName, "stream ended unexpectedly" + reasonSfx);
      void emitCurrentSystemStatus();
    }
  };
}

// Channel Validation.

/**
 * Result of channel validation. On success, contains the resolved channel and service key. On failure, contains the HTTP error details. The body field carries
 * either a plain string (for text error responses) or an object (for JSON error responses), so callers can use typeof to pick res.send() vs res.json().
 */
export type ValidateChannelResult =
  { channel: ResolvedChannel; resolvedKey: string; valid: true } |
  { body: Record<string, string> | string; statusCode: number; valid: false };

// Login mode error body used by both validateChannel() and handlePlayStream() to ensure consistent response format.
const LOGIN_MODE_BODY: Record<string, string> = { error: "Login in progress", message: "Please complete authentication before starting new streams." };

/**
 * Validates a channel name for streaming. Performs all fast, synchronous checks: disabled status, service resolution, channel lookup, service-filter availability,
 * and login mode. Returns a discriminated union so callers can handle success and failure without coupling to Express response objects.
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

  // Resolve service selection. For multi-service channels, this returns the user's selected service key (e.g., "espn-disneyplus"). For single-service channels
  // or if no selection exists, it returns the canonical key unchanged.
  const resolvedKey = resolveServiceKey(channelName);

  // Get the resolved channel with inheritance applied. For service variants, this merges the variant's properties with inherited properties from the canonical
  // entry (name, stationId).
  const channel = getResolvedChannel(resolvedKey);

  // Fall back to getAllChannels if the resolved channel doesn't exist (e.g., for ad-hoc streams or non-grouped channels).
  const effectiveChannel = channel ?? getAllChannels()[channelName];

  // Log a warning if a service selection resolved to a missing variant (e.g., variant was removed from channels after selection was saved).
  if(!channel && (resolvedKey !== channelName)) {

    LOG.warn("Service '%s' not found for channel '%s'. Using default service.", resolvedKey, channelName);
  }

  // effectiveChannel may be undefined: getResolvedChannel can return undefined and the getAllChannels() fallback is also possibly-undefined under
  // noUncheckedIndexedAccess, so we guard before use.

  if(!effectiveChannel) {

    return { body: "Channel not found.", statusCode: 404, valid: false };
  }

  // Enforce the user's active service filter at the streaming boundary. A channel whose every variant tag is excluded by enabledServices is structurally hidden
  // from the M3U playlist (built from getVisibleChannels), from the HDHomeRun lineup (iterates getAllChannels which filters through the same predicate), and
  // from getAllChannels itself. Letting a stream request through here would diverge from those views: capture a browser tab for a service the user explicitly
  // excluded, increment recovery budget on that service, and surface confusing logs for a channel the user cannot see. The predicate takes the canonical key
  // (channelName here is the URL-supplied canonical, not the post-resolveServiceKey variant) and is the single source of truth for service-filter visibility -
  // the same predicate already used by getVisibleChannels, the channel table renderer, and the bulk-action allowlist builders.
  if(!isChannelAvailableByService(channelName)) {

    return { body: "Channel not available.", statusCode: 404, valid: false };
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

// Capacity Reservation.

/**
 * Pure predicate for the concurrent-stream capacity decision. Returns true when a new stream may be admitted given the current active count and the configured
 * limit. Extracted as a standalone function so the boundary arithmetic is pinned by a unit test without driving a browser. The count passed in must be the count
 * BEFORE the new stream's pending entry is registered, so the new stream is naturally excluded from its own capacity check; admitting the final slot
 * (active === maxConcurrent - 1) returns true.
 * @param activeCount - Number of streams currently registered, excluding the stream being admitted.
 * @param maxConcurrent - The configured concurrent-stream limit.
 * @returns True if a slot is available for the new stream.
 */
export function hasStreamCapacity(activeCount: number, maxConcurrent: number): boolean {

  return activeCount < maxConcurrent;
}

/**
 * Reserves a concurrent-stream slot at the registration site - the single source of truth for the capacity decision across both the preroll path
 * (ensureChannelStream) and the blocking path (initializeStream). It evaluates capacity against the current registry count BEFORE the new stream's pending entry is
 * registered, so the new stream is excluded from its own check. setupStream never re-checks capacity; this registration-site reservation is the sole authority for
 * the decision, so at the boundary (one slot free) the registration-site gate admits the stream and no downstream check can double-count it against its own slot.
 * When the limit is reached, a single idle stream is reclaimed to free a slot; reservation succeeds when either a slot was already free or one was reclaimed.
 * @returns True if a slot was reserved (room existed or an idle stream was reclaimed), false if the limit is reached and no idle stream could be reclaimed.
 */
function reserveStreamSlot(): boolean {

  if(hasStreamCapacity(getStreamCount(), CONFIG.streaming.maxConcurrentStreams)) {

    return true;
  }

  // At capacity. Reclaim the longest-idle stream to make room; reservation succeeds only if one was actually freed.
  return reclaimIdleStream();
}

// Public Endpoint Handlers.

/**
 * Ensures a stream is running for a channel. If no stream exists, registers a pending stream with a preroll playlist (when available) and launches async setup. If a
 * stream already exists (either fully set up or pending), returns its ID immediately.
 *
 * The existing-stream check runs first so that ad-hoc streams (registered under synthetic keys like "play-a1b2c3d4") can be served without failing the
 * "Channel not found" check.
 *
 * For channels with multiple services (e.g., ESPN via ESPN.com or Disney+), the user's service selection is resolved before looking up the channel definition.
 * The stream is registered under the canonical key (channelName) for deduplication, but uses the resolved service's URL and settings.
 *
 * @param channelName - The channel key (or synthetic ad-hoc key) to stream.
 * @param req - Express request object (for profile override and client IP).
 * @param res - Express response object (for error responses).
 * @returns The stream ID if a stream is running or pending, or null if an error occurred.
 */
export async function ensureChannelStream(channelName: string, req: Request, res: Response): Promise<Nullable<number>> {

  // Check for an existing stream first. This must happen before channel validation so that ad-hoc streams (registered under synthetic keys like "play-a1b2c3d4") can
  // be served by the standard HLS playlist handler without failing the "Channel not found" check. A stream in channelToStreamId was already validated when it was
  // started, so no re-validation is needed. With pending stream registration, this always returns a real stream ID (never a sentinel).
  const streamId = getChannelStreamId(channelName);

  if(streamId !== undefined) {

    return streamId;
  }

  // No existing stream - validate the channel and start a new one. Channel validation is only needed for new streams because existing streams were already validated
  // at startup time.
  const validation = validateChannel(channelName);

  if(!validation.valid) {

    sendValidationError(validation, res);

    return null;
  }

  // When the preroll is available, register a pending stream immediately with a preroll playlist so the client gets a valid playlist on the first request. The full
  // stream setup runs asynchronously. When the preroll is not available (FFmpeg missing or failed), fall back to blocking initialization.
  const prerollCodec = getPrerollCodec();

  if(isPrerollReady(prerollCodec)) {

    // Reserve a capacity slot before registering the pending entry. The pending entry occupies a registry slot the instant it is registered, so the capacity
    // decision must happen here - while the new stream is still excluded from the count - to avoid a later self-counting check rejecting it after the client has
    // already received a preroll playlist. reserveStreamSlot is the single source of truth for this decision and reclaims an idle stream when at the limit. On
    // failure we send a proper 503 here, before any registration or preroll response.
    if(!reserveStreamSlot()) {

      res.setHeader("Retry-After", "10");
      res.setHeader("X-HDHomeRun-Error", "All Tuners In Use");
      res.status(503).send("Maximum concurrent streams (" + String(CONFIG.streaming.maxConcurrentStreams) + ") reached. Try again later.");

      return null;
    }

    const clientAddress: Nullable<string> = req.ip ?? req.socket.remoteAddress ?? null;
    const pending = registerPendingStream(channelName, validation.channel, clientAddress, req, prerollCodec);

    // Launch async setup. Errors are caught here to clean up the pending entry and prevent unhandled rejections.
    void completeStreamSetup({

      channel: validation.channel,
      channelName,
      clientAddress,
      numericStreamId: pending.numericStreamId,
      profileOverride: req.query["profile"] as string | undefined,
      streamIdStr: pending.streamIdStr,
      url: validation.channel.url
    }).catch((error: unknown) => {

      handleSetupFailure(pending.numericStreamId, channelName, validation.channel, error);
    });

    return pending.numericStreamId;
  }

  // Fallback: no preroll available. Block until the stream is fully set up.
  return startHLSStream(channelName, validation.channel.url, req, res, validation.channel);
}

/**
 * Handles HLS playlist requests. If a stream exists (fully set up or pending with preroll), returns the playlist immediately. If no stream exists and the preroll is
 * available, registers a pending stream with a preroll playlist and returns it immediately while setup runs asynchronously. Falls back to blocking initialization when
 * the preroll is not available.
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
 * Handles HLS segment requests. Returns the requested segment from memory. Supports the fMP4 initialization segment (init.mp4), capture-mode media segments
 * (.m4s), native-mode video segments (.ts), and audio segments for streams with separate audio renditions.
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

  if(streamId === undefined) {

    res.status(404).send("Stream not found.");

    return;
  }

  // Handle init segment (init.mp4) separately from media segments (.m4s, .ts).
  if(segmentName === "init.mp4") {

    const initSegment = getInitSegment(streamId);

    if(!initSegment) {

      res.status(404).send("Init segment not found.");

      return;
    }

    updateLastAccess(streamId);
    sendSegment(initSegment, "init.mp4", res);

    return;
  }

  // Handle media segments (.m4s and .ts). Check both video and audio segment stores.
  const segment = getSegment(streamId, segmentName) ?? getAudioSegment(streamId, segmentName);

  if(!segment) {

    res.status(404).send("Segment not found.");

    return;
  }

  updateLastAccess(streamId);
  sendSegment(segment, segmentName, res);
}

/**
 * Handles HLS variant playlist requests for streams with separate audio renditions. Serves video.m3u8 or audio.m3u8 depending on the requested playlist name.
 * Returns 404 for streams that don't have separate audio.
 *
 * Route: GET /hls/:name/video.m3u8 and GET /hls/:name/audio.m3u8
 *
 * @param req - Express request object.
 * @param res - Express response object.
 */
export function handleHLSVariantPlaylist(req: Request, res: Response): void {

  const channelName = (req.params as { name?: string }).name;

  // Extract the playlist filename from the URL path. The route is registered as two explicit paths (/video.m3u8 and /audio.m3u8) rather than a parameterized route,
  // so there is no :playlist param to read.
  const lastSlash = req.path.lastIndexOf("/");
  const playlistName = lastSlash >= 0 ? req.path.slice(lastSlash + 1) : undefined;

  if(!channelName || !playlistName) {

    res.status(400).send("Channel name and playlist name are required.");

    return;
  }

  const streamId = getChannelStreamId(channelName);

  if(streamId === undefined) {

    res.status(404).send("Stream not found.");

    return;
  }

  let playlist: string | undefined;

  if(playlistName === "video.m3u8") {

    playlist = getVideoPlaylist(streamId);
  } else if(playlistName === "audio.m3u8") {

    playlist = getAudioPlaylist(streamId);
  }

  if(!playlist) {

    res.status(404).send("Playlist not found.");

    return;
  }

  updateLastAccess(streamId);
  sendPlaylist(playlist, res);
}

// Ad-Hoc Streaming.

/**
 * Handles ad-hoc stream requests for arbitrary URLs. Generates a deterministic synthetic key from the URL, profile, selector, clickToPlay, and clickSelector, starts
 * a stream if none exists, and redirects to the standard HLS playlist path. This enables streaming URLs that are not predefined as channels.
 *
 * The synthetic key includes the profile, selector, clickToPlay, and clickSelector so that the same URL with different options produces separate streams. The
 * "play-" prefix prevents collisions with predefined channel names.
 *
 * Route: GET /play?url=<url>&profile=<name>&selector=<selector>&clickToPlay=<bool>&clickSelector=<selector>
 *
 * @param req - Express request object.
 * @param res - Express response object.
 */
export async function handlePlayStream(req: Request, res: Response): Promise<void> {

  const url = (req.query["url"] as string | undefined)?.trim();

  if(!url) {

    res.status(400).send("The url query parameter is required.");

    return;
  }

  const clickSelector = req.query["clickSelector"] as string | undefined;
  const clickToPlay = req.query["clickToPlay"] === "true";
  const profileOverride = req.query["profile"] as string | undefined;
  const selector = req.query["selector"] as string | undefined;

  // Generate a deterministic synthetic key from the trimmed URL, profile, selector, clickToPlay, and clickSelector. Including these ensures that the same URL with
  // different options produces separate streams. The newline delimiter is safe since URLs cannot contain literal newlines.
  const channelName = "play-" + createHash("sha256").update(
    url + "\n" + (profileOverride ?? "") + "\n" + (selector ?? "") + "\n" + (clickToPlay ? "1" : "") + "\n" + (clickSelector ?? "")
  ).digest("hex").slice(0, 8);

  // Check for an existing stream (either fully set up or pending). With pending stream registration, the stream ID is always a real ID.
  const streamId = getChannelStreamId(channelName);

  if(streamId !== undefined) {

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

  // Start a new ad-hoc stream. initializeStream handles pending registration, capture setup, segmenter creation, and event emission.
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

// Response Helpers.

/**
 * Sends the playlist for a stream. With the deferred preroll timer, the playlist may not be available immediately after stream registration - it arrives when either
 * the timer fires (seeding preroll) or the segmenter produces real content, whichever comes first. This function awaits the playlistReady promise to handle that
 * window. For the blocking fallback path (no FFmpeg / no preroll), the playlist is guaranteed to exist because initializeStream blocks until the segmenter produces
 * it. Returns 404 only if the stream was terminated or the playlist wait timed out.
 * @param streamId - The numeric stream ID.
 * @param clientAddress - Client address for tracking.
 * @param res - Express response object.
 */
async function sendPlaylistResponse(streamId: number, clientAddress: string, res: Response): Promise<void> {

  let playlist = getPlaylist(streamId);

  // The playlist may not be populated yet if the deferred preroll timer hasn't fired and the segmenter hasn't produced content. Wait for the playlistReady promise
  // which resolves when either source provides a playlist. The 30-second timeout covers pathological cases like setup hanging.
  if(!playlist) {

    const ready = await waitForPlaylist(streamId, 30000);

    if(ready) {

      playlist = getPlaylist(streamId);
    }
  }

  if(!playlist) {

    res.status(404).send("Stream not found.");

    return;
  }

  updateLastAccess(streamId);
  registerClient(streamId, clientAddress, "hls");

  // When still serving preroll, regenerate the progressive playlist on each poll so the sliding window advances based on elapsed wall-clock time. This simulates a
  // live stream - the client sees new segments appear on each poll and keeps playing without stalling. Once real content arrives, hasRealPlaylist becomes true and
  // the segmenter's playlist takes over.
  const stream = getStream(streamId);

  if(stream && !stream.hls.hasRealPlaylist) {

    if(stream.hls.prerollBaseUrl && stream.hls.prerollCodec && stream.hls.prerollStartTime) {

      playlist = generatePrerollPlaylist(stream.hls.prerollBaseUrl, stream.hls.prerollCodec, stream.hls.resumeSegmentIndex, stream.hls.prerollStartTime);
    }

    LOG.debug("streaming:preroll", "Serving preroll playlist for stream %d.", streamId);
  }

  if(stream?.preTuned) {

    stream.preTuned = false;

    LOG.debug("streaming:pretune", "Cleared pretune flag for stream %d on first client connection from %s.", streamId, clientAddress);
  }

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
 * Sends a segment buffer with appropriate Content-Type headers. MPEG-TS segments (.ts) get video/MP2T, fMP4 segments (.m4s and init.mp4) get video/mp4.
 * @param data - The segment data.
 * @param segmentName - The segment filename (used to determine Content-Type).
 * @param res - Express response object.
 */
function sendSegment(data: Buffer, segmentName: string, res: Response): void {

  const contentType = segmentName.endsWith(".ts") ? "video/MP2T" : "video/mp4";

  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Content-Type", contentType);
  res.send(data);
}

// Stream Lifecycle.

// Maps vertical resolution heights to standard display labels. Used by formatNativeQuality() to convert "1920x1080" -> "1080p".
const RESOLUTION_LABELS: Record<string, string> = { "1080": "1080p", "2160": "4K", "360": "360p", "480": "480p", "720": "720p" };

/**
 * Formats the native HLS quality string for the "Streaming..." log line. Combines codec, bandwidth (as Mbps), and resolution (as standard label like "1080p")
 * into a compact suffix. Returns an empty string when no values are available, or a comma-prefixed string like ", H264 12.1Mbps 1080p" for inclusion in the log.
 * @param bandwidth - Declared bandwidth in bits per second from the manifest. Zero when absent.
 * @param codec - Video codec label (e.g., "H264", "HEVC"), or null when absent.
 * @param resolution - Resolution string from the manifest (e.g., "1920x1080"), or null when absent.
 * @returns Formatted quality string for log output.
 */
function formatNativeQuality(bandwidth: number, codec: Nullable<string>, resolution: Nullable<string>): string {

  const parts: string[] = [];

  if(codec) {

    parts.push(codec);
  }

  if(bandwidth > 0) {

    parts.push((bandwidth / 1000000).toFixed(1) + "Mbps");
  }

  if(resolution) {

    // Map the vertical resolution to a standard label. Parse the height from "WIDTHxHEIGHT" format.
    const height = resolution.split("x")[1];

    parts.push((height ? RESOLUTION_LABELS[height] : undefined) ?? resolution);
  }

  if(parts.length === 0) {

    return "";
  }

  return ", " + parts.join(" ");
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
 * Note: This handler is only invoked for capture-mode streams. Native-mode streams bypass video element monitoring entirely (the monitor early-returns for native
 * streams), so evaluate timeouts that trigger tab replacement cannot occur.
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
    const oldSegmenter = stream.captureSession?.segmenter;
    const currentInitSegment = oldSegmenter?.getInitSegment();
    const currentInitVersion = oldSegmenter?.getInitVersion() ?? 0;
    const currentSegmentIndex = oldSegmenter?.getSegmentIndex() ?? 0;
    const currentSessionStats = oldSegmenter?.getSessionStats();
    const currentTrackTimestamps = oldSegmenter?.getTrackTimestamps();

    // Dispose the OLD capture pipeline. The CaptureSession kills the FFmpeg child first (setting its shuttingDown flag before the capture stream's EOF can reach
    // FFmpeg's stdin), then destroys the capture stream (which MUST happen before the old page is closed below, so chrome.tabCapture releases the capture and the
    // new getStream() does not hang with "Cannot capture a tab with an active stream"), then stops the segmenter. The new pipeline is constructed fresh further down.
    if(stream.captureSession) {

      LOG.debug("recovery:tab", "Disposing old capture pipeline for tab replacement.");
      stream.captureSession.dispose();
    }

    // Close the current page. The page may be null for pending entries whose async setup has not yet completed.
    const oldPage = stream.page;

    if(oldPage) {

      unregisterManagedPage(oldPage);

      if(!oldPage.isClosed()) {

        LOG.debug("recovery:tab", "Closing unresponsive page for tab replacement.");

        oldPage.close().catch((error: unknown) => {

          LOG.debug("recovery:tab", "Page close error during tab replacement: %s.", formatError(error));
        });
      }
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
        tabReplacement: true,
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

      ...buildSegmenterTerminationHandlers(numericStreamId, channelName, { logSuffix: "after tab replacement", reasonSuffix: "after recovery" }),
      pendingDiscontinuity: true,
      previousInitSegment: currentInitSegment,
      priorSessionStats: currentSessionStats,
      startingInitVersion: currentInitVersion,
      startingSegmentIndex: currentSegmentIndex,
      streamId: numericStreamId
    });

    // Wire the new segmenter into the new capture session (attachSegmenter pipes the session's capture output into it), then install the session and page on the
    // registry entry.
    captureResult.captureSession.attachSegmenter(newSegmenter);
    stream.captureSession = captureResult.captureSession;
    stream.page = captureResult.page;

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

  // Resolved channel definition (with canonical-to-variant identity inheritance applied). Undefined for ad-hoc URL streams.
  channel?: ResolvedChannel;

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

  // Whether this stream is being pretuned ahead of a scheduled recording. Pretuned streams are exempt from idle timeout until a real client connects.
  preTuned?: boolean;

  // When true, the requesting client is an MPEG-TS consumer (e.g., Plex via HDHomeRun). Channels with separate audio renditions (e.g., Google DAI on BET/VH1)
  // cannot be natively streamed to MPEG-TS clients because the independent video and audio MPEG-TS segments have incompatible PAT/PMT tables from ad splicing.
  // These channels fall back to capture mode for MPEG-TS clients but use native streaming for HLS clients (Channels DVR).
  mpegTsClient?: boolean;

  // Profile name to override auto-detection, from query parameter.
  profileOverride?: string;

  // The URL to stream.
  url: string;
}

/**
 * Initializes a new HLS stream. This is the blocking wrapper used by callers that need to wait for the full setup to complete (pretune, MPEG-TS, ad-hoc play).
 * Registers a pending stream entry with the channel-to-stream mapping, then awaits the full async setup.
 *
 * For the non-blocking path used by HLS playlist requests, see registerPendingStream() + completeStreamSetup() called from ensureChannelStream().
 *
 * @param options - Stream initialization options.
 * @returns The stream ID on success, or null if the stream was terminated during setup.
 * @throws StreamSetupError if setup fails, or Error for unexpected failures.
 */
export async function initializeStream(options: InitializeStreamOptions): Promise<Nullable<number>> {

  const { channel, channelName, url } = options;

  // Reserve a capacity slot before registering the pending entry. This is the same single-source-of-truth gate used by the preroll path, applied here so the
  // blocking callers (pretune, MPEG-TS, ad-hoc play) gate capacity at the registration site - while the new stream is still excluded from the count - rather than
  // relying on a downstream self-counting check that would double-count this stream against its own slot and spuriously reject the final slot. On failure we throw
  // the same StreamSetupError(503) the callers already handle, so their error responses and HDHomeRun headers are unchanged.
  if(!reserveStreamSlot()) {

    throw new StreamSetupError(
      "Concurrent stream limit reached.",
      503,
      "Maximum concurrent streams (" + String(CONFIG.streaming.maxConcurrentStreams) + ") reached. Try again later."
    );
  }

  // Allocate stream IDs. For predefined channels, use the channel name for the stream ID prefix. For ad-hoc streams, omit it so generateStreamId derives a prefix
  // from the URL (e.g., "foxsports-abc123"), which is more informative in logs.
  const numericStreamId = getNextStreamId();
  const streamIdStr = generateStreamId(channel ? channelName : undefined, url);

  // Register a pending entry in the stream registry. This allows concurrent requests for the same channel to find the stream immediately.
  createPendingEntry({ ...options, hls: createHLSState(), numericStreamId, streamIdStr });

  try {

    return await completeStreamSetup({ ...options, numericStreamId, streamIdStr });
  } catch(error) {

    // Skip logging - callers (startHLSStream, handlePlayStream, handleMpegTsStream) handle the re-thrown error with their own error responses and logging.
    handleSetupFailure(numericStreamId, channelName, channel, error, false);

    throw error;
  }
}

// Pending Stream Registration.

/**
 * Result of registering a pending stream.
 */
interface PendingStreamResult {

  // The numeric stream ID.
  numericStreamId: number;

  // The string stream ID for logging.
  streamIdStr: string;
}

/**
 * Registers a pending stream entry in the registry with deferred preroll. This is the synchronous Phase 1 of the two-phase stream initialization used by the HLS
 * playlist handler. The pending entry has a real stream ID but no playlist yet - the response is held until either the preroll timer fires (after PREROLL_DELAY_MS)
 * or real content arrives from the segmenter/native proxy. This ensures that fast-tuning streams (native, most capture services) skip preroll entirely, while slow
 * streams (Xfinity/Cox at 13-15s) get preroll content after the delay.
 * @param channelName - The channel key for registration and deduplication.
 * @param channel - The resolved channel definition.
 * @param clientAddress - Client IP address for Channels DVR API integration.
 * @param req - Express request object for deriving the base URL.
 * @param codec - The preroll codec variant to use for this stream.
 * @returns The allocated stream IDs.
 */
function registerPendingStream(channelName: string, channel: ResolvedChannel, clientAddress: Nullable<string>, req: Request, codec: CaptureCodec): PendingStreamResult {

  const numericStreamId = getNextStreamId();
  const streamIdStr = generateStreamId(channelName, channel.url);

  // Derive the base URL from the request for absolute preroll segment URLs.
  const protocol = req.protocol;
  const forwardedHost = req.get("x-forwarded-host");
  const host = forwardedHost ? (forwardedHost.split(",")[0] ?? "").trim() : req.get("host");
  const fallbackHost = CONFIG.server.host + ":" + String(CONFIG.server.port);
  const baseUrl = protocol + "://" + (host ?? fallbackHost);

  // Create HLS state with a deferred preroll timer. The timer fires after PREROLL_DELAY_MS - if stream setup hasn't completed by then, preroll is seeded and the
  // playlist response is unblocked. For resume streams, the preroll's MEDIA-SEQUENCE is offset by the saved segment index so it continues from the prior session's
  // sequence range. The timer is cancelled by completeStreamSetup() when the segmenter or native proxy is created, preventing races.
  const hls = createHLSState();

  // Snapshot the resume segment index once at registration. Both the preroll timer callback and completeStreamSetup() use this single snapshot, eliminating the TTL
  // race that would occur if each read the resume map independently at different times.
  hls.resumeSegmentIndex = getResumeSegmentIndex(channelName) ?? 0;

  // Capture the stream start time at registration. This timestamp is used for the registry's startTime field (stream age display, etc.).
  const streamStartTime = new Date();

  if(isPrerollReady(codec)) {

    // Set preroll metadata immediately at registration so the segmenter and native proxy can read it regardless of whether the deferred timer has fired. The segment
    // count, codec, and base URL are structural properties of the preroll system - they determine segment index offsets, codec-aware URL paths, and composite playlist
    // behavior. The timer controls only the client-facing timing (when the standalone preroll playlist begins serving). Decoupling these ensures correct behavior for
    // fast-tune/slow-proxy scenarios (e.g., Fox native at 5s tune but 15s proxy first poll) where setup completes before the timer fires.
    hls.prerollBaseUrl = baseUrl;
    hls.prerollCodec = codec;
    hls.prerollSegmentCount = getPrerollSegmentCount(codec);

    hls.prerollTimer = setTimeout(() => {

      // Record the preroll start time and seed the initial progressive playlist. On subsequent polls, sendPlaylistResponse() regenerates the playlist with an
      // advancing window based on elapsed time from this start time, simulating a live stream.
      hls.prerollStartTime = new Date();
      hls.playlist = generatePrerollPlaylist(baseUrl, codec, hls.resumeSegmentIndex, hls.prerollStartTime);
      hls.signalPlaylistReady();
    }, PREROLL_DELAY_MS);
  }

  // Register the pending entry.
  createPendingEntry({ channel, channelName, clientAddress, hls, numericStreamId, preTuned: false, streamIdStr, streamStartTime, url: channel.url });

  return { numericStreamId, streamIdStr };
}

// Pending Entry Helpers.

/**
 * Options for creating a pending stream entry.
 */
interface CreatePendingEntryOptions {

  // Resolved channel definition (with canonical-to-variant identity inheritance applied). Undefined for ad-hoc URL streams.
  channel?: ResolvedChannel;

  // Key for channelToStreamId registration.
  channelName: string;

  // Client IP address for Channels DVR API integration.
  clientAddress?: Nullable<string>;

  // Pre-created HLS state. The caller seeds it with preroll content when applicable.
  hls: HLSState;

  // Pre-allocated numeric stream ID.
  numericStreamId: number;

  // Whether this stream is being pretuned ahead of a scheduled recording.
  preTuned?: boolean;

  // Pre-allocated string stream ID for logging.
  streamIdStr: string;

  // Wall-clock time when the stream was created, for PROGRAM-DATE-TIME anchoring. Defaults to now if not provided.
  streamStartTime?: Date;

  // The URL to stream.
  url: string;
}

/**
 * Creates a pending stream entry in the registry and sets the channel-to-stream mapping. The entry has null page, profile, segmenter, and other browser-related
 * fields that are filled in asynchronously by completeStreamSetup(). This is the shared core for both the non-blocking preroll path (registerPendingStream) and the
 * blocking path (initializeStream).
 * @param options - Pending entry options.
 */
function createPendingEntry(options: CreatePendingEntryOptions): void {

  const { channel, channelName, hls, numericStreamId, streamIdStr, url } = options;

  registerStream({

    captureCodec: null,
    captureSession: null,
    channelName: channel?.name ?? null,
    clientAddress: options.clientAddress ?? null,
    hardwareAccelerated: false,
    hls,
    id: numericStreamId,
    info: {

      lastPlaylistRequest: Date.now(),
      storeKey: channelName
    },
    monitor: null,
    mpegTsClientCount: 0,
    nativeBandwidth: 0,
    nativeProxy: null,
    nativeResolution: null,
    page: null,
    preTuned: options.preTuned ?? false,
    profile: null,
    startTime: options.streamStartTime ?? new Date(),
    streamIdStr,
    streamingMode: "capture",
    url
  });

  setChannelStreamId(channelName, numericStreamId);
}

/**
 * Handles stream setup failure by marking channel health, terminating the pending entry, and optionally logging the error. The blocking path (initializeStream) skips
 * logging because it re-throws for callers to handle. The non-blocking path (ensureChannelStream .catch) logs because there is no outer handler.
 * @param numericStreamId - The pending entry's numeric stream ID.
 * @param channelName - The channel key for health tracking and termination.
 * @param channel - The channel definition, or undefined for ad-hoc streams (no health tracking).
 * @param error - The error that caused the failure.
 * @param logError - Whether to log the error. False when the caller will re-throw (blocking path), true when fire-and-forget (non-blocking path).
 */
function handleSetupFailure(numericStreamId: number, channelName: string, channel: ResolvedChannel | undefined, error: unknown, logError = true): void {

  // Mark channel health as failed. Only for predefined channels (channel is defined). Ad-hoc URL streams have no persistent channel identity.
  if(channel) {

    const failVariantKey = resolveServiceKey(channelName);
    const failAuthDomain = getAuthDomainForChannel(failVariantKey);

    markChannelFailure(channelName, failAuthDomain);
  }

  // Clean up the pending entry.
  terminateStream(numericStreamId, channelName, "setup failed");
  void emitCurrentSystemStatus();

  // Only log when the error won't be caught and logged by an outer handler (non-blocking fire-and-forget path).
  if(logError) {

    if(error instanceof StreamSetupError) {

      LOG.warn("Stream setup failed for %s: %s.", channelName, error.userMessage);
    } else {

      LOG.error("Unexpected error during stream setup for %s: %s.", channelName, formatError(error));
    }
  }
}

// Async Stream Setup - Native and Capture Path Helpers.

/**
 * Result from attempting native streaming. Contains the fields needed by the caller to log quality info and emit status.
 */
interface NativeStreamingResult {

  codec: Nullable<string>;
  quality: string;
}

/**
 * Attempts to upgrade the stream from capture to native HLS. Consumes the manifest interception already finalized by setupStream(), probes the manifest, creates a
 * native proxy, stops the capture pipeline, suppresses page audio, and updates the registry entry. Returns the codec and formatted quality string on success, or
 * null if native is not viable.
 * @param setup - The stream setup result from setupStream().
 * @param numericStreamId - The stream's numeric ID.
 * @param channelName - The channel key for logging and cache operations.
 * @param url - The stream URL for the native proxy.
 * @param mpegTsClient - Whether the client is an MPEG-TS consumer.
 * @returns The native codec and quality info, or null if native streaming was not viable.
 */
async function startNativeProxy(setup: StreamSetupResult, numericStreamId: number, channelName: string, url: string,
  mpegTsClient?: boolean): Promise<Nullable<NativeStreamingResult>> {

  if(!setup.manifestInterception) {

    return null;
  }

  // The manifest interception was finalized and verified during setupStream(). By the time we get here, setup.manifestInterception.promise has already resolved
  // (or is about to) with the URL setupStream verified. We just consume it.

  // Read the preroll segment count from the pending entry to pass to the native coordinator. This value is set at registration time (not in the timer callback),
  // so it's available regardless of whether the deferred preroll timer has fired. The proxy uses it for segment index offset. The base URL for composite playlists
  // is read dynamically from the stream's HLS state at playlist generation time.
  const pendingForNative = getStream(numericStreamId);
  const nativePrerollSegmentCount = pendingForNative?.hls.prerollSegmentCount ?? 0;

  const nativeResult = await attemptNativeStreaming({

    channelName,
    interceptionPromise: setup.manifestInterception.promise,
    mpegTsClient,
    onError: (error) => {

      if(isTerminationInitiated(numericStreamId)) {

        return;
      }

      // Log the error and clear the probe cache. The proxy has already stopped itself (set errorThresholdReached + stopped). The monitor detects
      // hasErrored() on the next 2-second tick and triggers L3 fallback to capture mode, preserving the stream for the DVR client.
      LOG.warn("Native proxy error for %s: %s. Falling back to capture.", channelName, error);

      clearProbeCache(channelName);
    },
    page: setup.page,
    prerollCodec: pendingForNative?.hls.prerollCodec ?? "h264",
    prerollSegmentCount: nativePrerollSegmentCount,
    streamId: numericStreamId,
    streamIdStr: setup.streamId,
    url
  });

  if(!nativeResult) {

    return null;
  }

  const currentStream = getStream(numericStreamId);

  if(!currentStream) {

    nativeResult.proxy.stop();

    return null;
  }

  // Dispose the capture pipeline - native streaming replaces it entirely. No segmenter has been attached yet (native is attempted before createCaptureSegmenter), so
  // disposal kills the FFmpeg child and destroys the capture stream.
  setup.captureSession.dispose();

  // Consume the persisted capture-mode resume data for this channel. Resume data seeds an fMP4 segmenter's starting sequence and init segment for capture continuity
  // across restarts; createCaptureSegmenter consumes it on the capture path. On this native-upgrade path that segmenter is never created, so without this delete the
  // resume entry would linger until TTL and could be mis-applied to a later capture-mode stream for the same channel. Native streaming carries its own MPEG-TS
  // sequencing, so the capture resume state is obsolete the moment we commit to native.
  deleteResumeData(channelName);

  // Update the registry entry to reflect native mode. For streams with separate audio, clear preroll state - preroll is muxed video+audio and can't be
  // split into separate renditions. For muxed-audio streams, preserve preroll state so the proxy can build composite playlists with preroll entries.
  currentStream.captureSession = null;
  currentStream.hls.hasAudio = nativeResult.hasAudio;

  if(nativeResult.hasAudio) {

    currentStream.hls.prerollBaseUrl = null;
    currentStream.hls.prerollCodec = null;
    currentStream.hls.prerollSegmentCount = 0;
  }

  currentStream.captureCodec = nativeResult.codec;
  currentStream.hardwareAccelerated = false;
  currentStream.nativeBandwidth = nativeResult.bandwidth;
  currentStream.nativeProxy = nativeResult.proxy;
  currentStream.nativeResolution = nativeResult.resolution;
  currentStream.streamingMode = "native";

  // Start the native proxy. Signal init segment readiness immediately - native MPEG-TS segments carry their own PAT/PMT codec configuration in every
  // segment, so there is no separate init segment to wait for. Without this, MPEG-TS clients block on waitForInitSegment() and time out before the proxy's
  // first poll cycle completes.
  nativeResult.proxy.start();
  currentStream.hls.signalInitSegmentReady();

  // Suppress audio on the browser page. The page stays alive for token refresh but the video element's audio is not part of the native stream - without
  // suppression, it plays audibly on the local machine.
  await suppressPageAudio(setup.page);

  LOG.debug("native:coordinator", "Capture pipeline stopped for %s. Native proxy active.", channelName);

  return { codec: nativeResult.codec, quality: formatNativeQuality(nativeResult.bandwidth, nativeResult.codec, nativeResult.resolution) };
}

/**
 * Creates the fMP4 segmenter for capture mode streams. Reads resume data, creates the segmenter with preroll and resume configuration, and attaches it to the
 * capture session (which pipes the session's capture output into it). Resume data is consumed only after the segmenter is successfully attached to a non-disposed
 * session, ensuring it survives if the stream was terminated during setup.
 * @param setup - The stream setup result from setupStream().
 * @param numericStreamId - The stream's numeric ID.
 * @param channelName - The channel key for resume data and logging.
 * @returns True if the segmenter was attached, false if the stream was terminated during setup (the session was already disposed).
 */
function createCaptureSegmenter(setup: StreamSetupResult, numericStreamId: number, channelName: string): boolean {

  // Peek at resume data from a previous shutdown without consuming it. The data is consumed (deleted) only after the segmenter is successfully created and
  // stored in the registry. This ensures resume data survives if segmenter creation fails - the next stream start retries with the same resume state instead
  // of losing it and causing an HLS sequence reset.
  const resumeData = peekResumeData(channelName);
  const currentStream = getStream(numericStreamId);
  const prerollSegmentCount = currentStream?.hls.prerollSegmentCount ?? 0;

  // When preroll is active, use the snapshotted resume index (stored on HLS state at registration) so the segmenter's starting index is guaranteed to match
  // the preroll playlist's MEDIA-SEQUENCE offset. When preroll is inactive, use the resume data directly - no preroll playlist to be consistent with.
  const baseSegmentIndex = (prerollSegmentCount > 0) ? (currentStream?.hls.resumeSegmentIndex ?? 0) : (resumeData?.segmentIndex ?? 0);

  // Create the fMP4 segmenter. The starting segment index accounts for both the resume offset and the preroll segment range. When preroll is active, the
  // segmenter includes preroll entries in its sliding window via the compositor. The pending discontinuity at the preroll-to-real boundary is always needed -
  // previousInitSegment is only passed without preroll, because the preroll init segment differs from the real init and the discontinuity must not be
  // suppressed by an init-match comparison against the prior session.
  const segmenter = createFMP4Segmenter({

    ...(resumeData ? {

      initialTrackTimestamps: resumeData.trackTimestamps,
      ...((prerollSegmentCount === 0) ? { previousInitSegment: resumeData.initSegment } : {}),
      startingInitVersion: resumeData.initVersion
    } : {}),

    ...((prerollSegmentCount > 0) ? {

      prerollBaseUrl: currentStream?.hls.prerollBaseUrl ?? null,
      prerollCodec: currentStream?.hls.prerollCodec ?? "h264",
      prerollSegmentCount
    } : {}),

    ...((resumeData || (prerollSegmentCount > 0)) ? {

      pendingDiscontinuity: true,
      startingSegmentIndex: baseSegmentIndex + prerollSegmentCount
    } : {}),

    ...buildSegmenterTerminationHandlers(numericStreamId, channelName),
    streamId: numericStreamId
  });

  // Attach the segmenter to the capture session. attachSegmenter pipes the session's capture output into the segmenter. If the stream was terminated mid-setup, the
  // session is already disposed and attachSegmenter stops the orphan segmenter instead of wiring it - the single home for that rare-race cleanup.
  setup.captureSession.attachSegmenter(segmenter);

  if(setup.captureSession.disposed) {

    // The stream was terminated during setup (rare race). attachSegmenter already stopped the orphan; leave the resume data intact so the next stream start for this
    // channel can retry with it rather than losing it to an HLS sequence reset.
    return false;
  }

  // The segmenter is created, piped, and owned by the session. Consume the resume data so it is not reused on a subsequent stream start for the same channel.
  if(resumeData) {

    deleteResumeData(channelName);
  }

  return true;
}

/**
 * Options for completing stream setup.
 */
interface CompleteStreamSetupOptions extends InitializeStreamOptions {

  // Pre-allocated numeric stream ID from the pending registration.
  numericStreamId: number;

  // Pre-allocated string stream ID for logging.
  streamIdStr: string;
}

/**
 * Completes the async portion of stream initialization. Creates the browser page, navigates to the URL, sets up capture, creates the segmenter, and fills in the
 * pending registry entry. If the pending entry was terminated during setup (e.g., idle timeout) before this function reaches it, it releases the now-orphaned
 * setup resources via setup.cleanup() and returns null. On a thrown error, it does not clean up the pending entry itself - callers reach handleSetupFailure(), which
 * terminates the pending entry via terminateStream().
 *
 * This is the Phase 2 of the two-phase stream initialization. For the non-blocking HLS path, it runs as fire-and-forget via `void`. For the blocking path
 * (initializeStream), it is awaited directly.
 *
 * @param options - Stream setup options including pre-allocated IDs.
 * @returns The stream ID on success, or null if the stream was terminated during setup.
 * @throws StreamSetupError if setup fails, or Error for unexpected failures.
 */
async function completeStreamSetup(options: CompleteStreamSetupOptions): Promise<Nullable<number>> {

  const { channel, channelName, channelSelector, clickSelector, clickToPlay, mpegTsClient, numericStreamId, profileOverride, streamIdStr, url } = options;

  // Circuit breaker callback - terminate the stream on unrecoverable errors.
  const onCircuitBreak = (): void => {

    const currentStreamId = getChannelStreamId(channelName);

    if(currentStreamId !== undefined) {

      terminateStream(currentStreamId, channelName, "too many errors");
      void emitCurrentSystemStatus();
    }
  };

  // Factory to create the tab replacement handler. Called by setupStream after resolving the profile, allowing the handler to be created with access to all context.
  const tabReplacementFactory: TabReplacementHandlerFactory = (_, streamId, profile, metadataComment) => {

    return createTabReplacementHandler(numericStreamId, streamId, channelName, url, profile, metadataComment, onCircuitBreak);
  };

  // Capacity was already reserved at the registration site (reserveStreamSlot in ensureChannelStream for the preroll path, and in initializeStream for the blocking
  // path) before this stream's pending entry was registered, so the new stream is excluded from its own capacity check. We deliberately do NOT re-check or reclaim
  // here: the pending entry is now counted, so a count-based check would double-count this stream against its own slot and could evict a healthy peer at the
  // legitimate boundary. The registration-site reservation is the single source of truth for the capacity decision.

  // Pass the pre-allocated IDs to setupStream so it uses them instead of generating new ones. This ensures the abort controller, health monitor, and tab replacement
  // handler all reference the same stream identity as the pending registry entry. Pass channelName only for predefined channels - for ad-hoc streams, omitting it
  // causes generateStreamId to derive the stream ID string from the URL (e.g., "foxsports-abc123"), which is more informative in logs.
  const setup = await setupStream(
    {

      channel,
      channelName: channel ? channelName : undefined,
      channelSelector: channel ? undefined : channelSelector,
      clickSelector: channel ? undefined : clickSelector,
      clickToPlay: channel ? undefined : clickToPlay,
      numericStreamId,
      onTabReplacementFactory: tabReplacementFactory,
      profileOverride,
      streamId: streamIdStr,
      url
    },
    onCircuitBreak
  );

  // Fill in the pending registry entry with the real browser state. The entry was registered in Phase 1 (registerPendingStream or initializeStream).
  const stream = getStream(numericStreamId);

  if(!stream) {

    // Stream was terminated during setup (e.g., idle timeout on the pending entry). Clean up setupStream resources.
    setup.cleanup().catch((error: unknown) => {

      LOG.debug("streaming:setup", "Cleanup error for terminated pending stream: %s.", formatError(error));
    });

    return null;
  }

  stream.captureSession = setup.captureSession;
  stream.monitor = setup.monitor;
  stream.page = setup.page;
  stream.profile = setup.profile;
  stream.startTime = setup.startTime;
  stream.url = setup.url;

  // Continue within stream context for consistent logging.
  return runWithStreamContext(
    { channelName: channel?.name, streamId: setup.streamId, url: setup.url },
    async () => {

      // The deferred preroll timer is NOT cancelled here. It continues running until the first real playlist arrives (cancelled in updatePlaylist() in
      // hlsSegments.ts). This is critical for native streams where the browser setup completes quickly but the native proxy's first poll cycle can take 10-15+
      // seconds. If we cancelled the timer at setup completion, the client would have no playlist during the proxy's first poll - the preroll timer fires after
      // PREROLL_DELAY_MS and provides content during that gap. For the capture path, the segmenter produces its first playlist within ~2 seconds of creation, so
      // the timer is cancelled almost immediately after setup anyway.

      // Attempt native streaming if a manifest interception handle is available. If native is viable, the capture pipeline is stopped and the proxy takes over.
      let nativeCodec: Nullable<string> = null;
      let nativeQuality = "";
      let streamingMode: "capture" | "native" = "capture";

      const nativeStreamResult = await startNativeProxy(setup, numericStreamId, channelName, url, mpegTsClient);

      if(nativeStreamResult) {

        streamingMode = "native";
        nativeCodec = nativeStreamResult.codec;
        nativeQuality = nativeStreamResult.quality;
      }

      // If native streaming was not viable or not attempted, create the fMP4 segmenter for capture mode.
      if(streamingMode === "capture") {

        if(!createCaptureSegmenter(setup, numericStreamId, channelName)) {

          return null;
        }
      }

      const effectiveCodec = getEffectiveCaptureCodec();
      const captureHwAccel = isCaptureHardwareAccelerated();

      // Prefix the codec label with U+26A1, the high-voltage / lightning-bolt glyph, as a visual marker in the "Streaming..." log line that capture is
      // hardware-accelerated. The glyph is written as an escape so the source stays ASCII; it renders as the lightning bolt in the log output.
      const ffmpegCodec = captureHwAccel ? ("\u26A1 " + effectiveCodec.toUpperCase()) : effectiveCodec.toUpperCase();
      const captureMode = (streamingMode === "native") ? ("native HLS" + nativeQuality) :
        (CONFIG.streaming.captureMode === "ffmpeg" ? "FFmpeg [" + ffmpegCodec + "]" : "Native fMP4");
      const displayName = channel?.name ?? url;

      const tuneTime = ((Date.now() - setup.startTime.getTime()) / 1000).toFixed(1);

      LOG.info("Streaming %s: %s, %s, %s. Tuned in %ss%s.", displayName, setup.serviceName, setup.profileName, captureMode,
        tuneTime, setup.directTune ? " (direct)" : "");

      // Mark channel health as successful. Only for predefined channels (channel is defined). Ad-hoc URL streams have no persistent channel identity. Domain
      // auth is conditionally skipped when the provider module defines validateTune and the tuned channel does not prove paid access (e.g., Sling Freestream).
      if(channel) {

        const successVariantKey = resolveServiceKey(channelName);
        const successAuthDomain = getAuthDomainForChannel(successVariantKey);
        const successServiceTag = getServiceTagForChannel(successVariantKey);
        const provider = getProviderBySlug(successServiceTag);
        const markAuth = !provider?.validateTune || provider.validateTune(channel.channelSelector ?? channelName);

        markChannelSuccess(channelName, successAuthDomain, markAuth);
      }

      // Update the registry entry with codec and hardware acceleration state, then emit the stream added event for the dashboard. Native streams set their codec
      // and quality fields earlier (when the native proxy is created), so only capture mode needs updating here.
      const streamCodec = (streamingMode === "native") ? nativeCodec : effectiveCodec.toUpperCase();
      const hwAccelerated = (streamingMode !== "native") && captureHwAccel;
      const currentEntry = getStream(numericStreamId);

      if(currentEntry) {

        currentEntry.captureCodec = streamCodec;
        currentEntry.hardwareAccelerated = hwAccelerated;
      }

      emitStreamAdded(createInitialStreamStatus({

        captureCodec: streamCodec,
        channelName: channel?.name ?? null,
        hardwareAccelerated: hwAccelerated,
        logoUrl: getChannelLogo(channelName) ?? "",
        numericStreamId,
        serviceName: setup.serviceName,
        startTime: setup.startTime,
        streamingMode,
        url: setup.url
      }));
      void emitCurrentSystemStatus();

      // Trigger show name lookup for the new stream.
      triggerShowNameUpdate();

      return numericStreamId;
    }
  );
}

// Channel Stream Startup.

/**
 * Starts a new HLS stream for a predefined channel using the blocking path. Used as a fallback when the preroll is not available (no FFmpeg). Delegates to
 * initializeStream() for the actual setup. Error responses are sent directly to the client, including HDHomeRun-specific headers for capacity errors.
 *
 * @param channelName - The channel key (canonical key for stream registration and deduplication).
 * @param url - The URL to stream (from the resolved service).
 * @param req - Express request object (for profile override and client IP).
 * @param res - Express response object (for error responses).
 * @param channel - The resolved channel definition (with inheritance applied for service variants).
 * @returns The stream ID if successful, null if an error occurred (error response already sent).
 */
async function startHLSStream(channelName: string, url: string, req: Request, res: Response, channel?: ResolvedChannel): Promise<Nullable<number>> {

  const profileOverride = req.query["profile"] as string | undefined;
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
 * Returns all streams that have exceeded the idle timeout and have no active MPEG-TS clients. Pretuned streams are excluded - they have no clients by design and
 * the pretune module manages their lifecycle via a safety timeout. The result is sorted by last access time (oldest first) so callers can efficiently pick the
 * longest-idle stream for reclamation.
 * @returns Idle streams sorted by last access time ascending (oldest first).
 */
function getIdleStreams(): StreamRegistryEntry[] {

  const now = Date.now();

  return getAllStreams()
    .filter((stream) => !stream.preTuned && (stream.mpegTsClientCount === 0) && ((now - stream.info.lastPlaylistRequest) >= CONFIG.hls.idleTimeout))
    .sort((a, b) => a.info.lastPlaylistRequest - b.info.lastPlaylistRequest);
}

/**
 * Checks for idle streams and terminates them. Called periodically by the idle detection interval.
 */
export function cleanupIdleStreams(): void {

  const idle = getIdleStreams();

  for(const stream of idle) {

    terminateStream(stream.id, stream.info.storeKey, "no active clients");
  }

  // Emit system status once after all idle streams are terminated.
  if(idle.length > 0) {

    void emitCurrentSystemStatus();
  }
}

/**
 * Attempts to reclaim a single idle stream to free capacity for a new request. Terminates the stream that has been idle the longest. This is called when the
 * concurrent stream limit is reached, allowing channel-surfing users to get new streams without being rejected while abandoned streams linger.
 * @returns True if a stream was reclaimed, false if no idle streams exist.
 */
function reclaimIdleStream(): boolean {

  const idle = getIdleStreams();

  if(idle.length === 0) {

    return false;
  }

  const oldest = idle[0];

  if(!oldest) {

    return false;
  }

  LOG.info("Reclaiming idle stream %s (%s) to free capacity.", oldest.id, oldest.info.storeKey);

  terminateStream(oldest.id, oldest.info.storeKey, "reclaimed for new stream");
  void emitCurrentSystemStatus();

  return true;
}
