/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.ts: HLS streaming request handlers for PrismCast.
 */
import { LOG, delay, formatError, runWithStreamContext } from "../utils/index.js";
import type { Nullable, ResolvedSiteProfile } from "../types/index.js";
import type { Request, Response } from "express";
import { StreamSetupError, createPageWithCapture, setupStream } from "./setup.js";
import { createHLSState, getAllStreams, getStream, getStreamCount, registerStream, updateLastAccess } from "./registry.js";
import { createInitialStreamStatus, emitStreamAdded } from "./statusEmitter.js";
import { deleteChannelStreamId, getChannelStreamId, isTerminationInitiated, setChannelStreamId, terminateStream } from "./lifecycle.js";
import { emitCurrentSystemStatus, isLoginModeActive, unregisterManagedPage } from "../browser/index.js";
import { getAllChannels, isPredefinedChannelDisabled } from "../config/userChannels.js";
import { getInitSegment, getPlaylist, getSegment, waitForPlaylist } from "./hlsSegments.js";
import { CONFIG } from "../config/index.js";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.js";
import type { StreamRegistryEntry } from "./registry.js";
import type { TabReplacementHandlerFactory } from "./setup.js";
import type { TabReplacementResult } from "./monitor.js";
import { createFMP4Segmenter } from "./fmp4Segmenter.js";
import { registerClient } from "./clients.js";
import { triggerShowNameUpdate } from "./showInfo.js";

/*
 * HLS STREAMING
 *
 * This module handles HLS (HTTP Live Streaming) output using fMP4 (fragmented MP4) segments. HLS mode uses MP4/AAC capture from puppeteer-stream, which is then
 * segmented natively without any external dependencies. The overall flow is:
 *
 * 1. Client requests playlist at /hls/:name/stream.m3u8
 * 2. If no stream exists, we call setupStream() and create a native fMP4 segmenter
 * 3. The segmenter parses the MP4 stream and produces init.mp4 (codec config) + segment0.m4s, segment1.m4s, ...
 * 4. We store init segment and media segments in memory, return playlist to client
 * 5. Client fetches init.mp4 once, then media segments at /hls/:name/segmentN.m4s
 * 6. Idle timeout terminates streams with no recent segment requests
 *
 * Shared streams: If multiple clients request the same channel, they share one segmenter. The first client triggers stream creation, and subsequent clients get the
 * existing playlist and segments.
 */

// ─────────────────────────────────────────────────────────────
// Public Endpoint Handlers
// ─────────────────────────────────────────────────────────────

/**
 * Ensures a stream is running for a channel. If no stream exists, starts one. If a stream startup is in progress (placeholder), waits for it to complete. Returns the
 * stream ID if successful, or null if an error occurred (with the error response already sent to the client).
 *
 * This is the shared entry point for both HLS and MPEG-TS handlers. It handles channel validation, login mode blocking, and concurrent startup deduplication.
 *
 * @param channelName - The channel key to stream.
 * @param req - Express request object (for profile override and client IP).
 * @param res - Express response object (for error responses).
 * @returns The stream ID if a stream is running, or null if an error occurred.
 */
export async function ensureChannelStream(channelName: string, req: Request, res: Response): Promise<number | null> {

  // Check if this is a disabled predefined channel.
  if(isPredefinedChannelDisabled(channelName)) {

    res.status(404).send("Channel is disabled.");

    return null;
  }

  const channels = getAllChannels();
  const channel = channels[channelName];

  // Runtime check needed even though TypeScript thinks channel is always defined (Record indexing quirk).
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!channel) {

    res.status(404).send("Channel not found.");

    return null;
  }

  // Check for an existing stream.
  let streamId = getChannelStreamId(channelName);

  // If a stream is already running (not a placeholder), return it directly.
  if((streamId !== undefined) && (streamId !== -1)) {

    return streamId;
  }

  // No stream exists — start a new one.
  if(streamId === undefined) {

    // Block new stream requests while login mode is active. This prevents the browser from being disrupted during authentication.
    if(isLoginModeActive()) {

      res.status(503).json({

        error: "Login in progress",
        message: "Please complete authentication before starting new streams."
      });

      return null;
    }

    const newStreamId = await startHLSStream(channelName, channel.url, req, res);

    if(newStreamId === null) {

      // Error response already sent by startHLSStream.
      return null;
    }

    return newStreamId;
  }

  // Placeholder (-1) means another request is already starting this stream. Poll until the real stream ID appears or we timeout.
  const pollInterval = 200;
  const deadline = Date.now() + CONFIG.streaming.navigationTimeout;

  while(Date.now() < deadline) {

    // eslint-disable-next-line no-await-in-loop
    await delay(pollInterval);

    streamId = getChannelStreamId(channelName);

    // The startup failed and the placeholder was removed.
    if(streamId === undefined) {

      res.status(500).send("Stream startup failed.");

      return null;
    }

    // Real stream ID is now available.
    if(streamId !== -1) {

      return streamId;
    }
  }

  // Timed out waiting for the placeholder to resolve.
  res.setHeader("Retry-After", "5");
  res.status(503).send("Stream is starting. Please retry.");

  return null;
}

/**
 * Handles HLS playlist requests. Ensures a stream is running for the requested channel, waits for the first playlist to be produced, then returns it.
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

  const streamId = await ensureChannelStream(channelName, req, res);

  if(streamId === null) {

    // Error response already sent by ensureChannelStream.
    return;
  }

  // Capture client address for client tracking.
  const clientAddress = req.ip ?? req.socket.remoteAddress ?? "unknown";

  // If a playlist is already available, return it immediately.
  const existingPlaylist = getPlaylist(streamId);

  if(existingPlaylist) {

    updateLastAccess(streamId);
    registerClient(streamId, clientAddress, "hls");

    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.send(existingPlaylist);

    return;
  }

  // Wait for the first playlist to be ready.
  const playlistReady = await waitForPlaylist(streamId, CONFIG.streaming.navigationTimeout);

  if(!playlistReady) {

    res.setHeader("Retry-After", "5");
    res.status(503).send("Stream is starting. Please retry.");

    return;
  }

  const playlist = getPlaylist(streamId);

  if(!playlist) {

    res.status(500).send("Playlist not available.");

    return;
  }

  updateLastAccess(streamId);
  registerClient(streamId, clientAddress, "hls");

  res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
  res.send(playlist);
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

  if(streamId === undefined) {

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

    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Content-Type", "video/mp4");
    res.send(initSegment);

    return;
  }

  // Handle media segments (.m4s).
  const segment = getSegment(streamId, segmentName);

  if(!segment) {

    res.status(404).send("Segment not found.");

    return;
  }

  updateLastAccess(streamId);

  res.setHeader("Content-Type", "video/mp4");
  res.send(segment);
}

// ─────────────────────────────────────────────────────────────
// Stream Lifecycle
// ─────────────────────────────────────────────────────────────

/**
 * Cleans up resources from a stream that was terminated during setup before the segmenter was stored in the registry. This handles the rare race condition where
 * another code path (e.g., circuit breaker) calls terminateStream() between registerStream() and the segmenter assignment.
 *
 * terminateStream() already cleaned up the page, monitor, channel mapping, and registry entry. We only need to stop the segmenter, which was created after
 * termination occurred and therefore wasn't cleaned up.
 * @param segmenter - The orphaned fMP4 segmenter instance to stop.
 */
function cleanupOrphanedSetup(segmenter: FMP4SegmenterResult): void {

  LOG.warn("Stream was terminated during setup. Stopping orphaned segmenter.");
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
 * @param channelName - The channel name for error callbacks.
 * @param url - The URL to navigate to.
 * @param profile - The site profile for video handling.
 * @param onCircuitBreak - Callback for circuit breaker trips during replacement.
 * @returns A handler function that performs tab replacement, or null if the stream no longer exists.
 */
function createTabReplacementHandler(
  numericStreamId: number,
  streamId: string,
  channelName: string,
  url: string,
  profile: ResolvedSiteProfile,
  onCircuitBreak: () => void
): () => Promise<TabReplacementResult | null> {

  return async (): Promise<TabReplacementResult | null> => {

    // Get the current stream entry.
    const stream = getStream(numericStreamId);

    if(!stream) {

      LOG.warn("Tab replacement requested but stream %s no longer exists.", streamId);

      return null;
    }

    // Get the current segment index from the old segmenter before stopping it. This allows the new segmenter to continue numbering from where we left off.
    const currentSegmentIndex = stream.segmenter?.getSegmentIndex() ?? 0;

    // Destroy the OLD capture stream first. This MUST happen before closing the page to ensure chrome.tabCapture releases the capture. Without this, the new
    // getStream() call would hang with "Cannot capture a tab with an active stream" error.
    if(stream.rawCaptureStream && !stream.rawCaptureStream.destroyed) {

      LOG.debug("Destroying old capture stream for tab replacement.");
      stream.rawCaptureStream.destroy();
    }

    // Stop the current segmenter if it exists.
    if(stream.segmenter) {

      LOG.debug("Stopping current segmenter for tab replacement.");
      stream.segmenter.stop();
    }

    // Stop the FFmpeg process if it exists.
    if(stream.ffmpegProcess) {

      LOG.debug("Stopping FFmpeg process for tab replacement.");
      stream.ffmpegProcess.kill();
    }

    // Close the current page.
    const oldPage = stream.page;

    unregisterManagedPage(oldPage);

    if(!oldPage.isClosed()) {

      LOG.debug("Closing unresponsive page for tab replacement.");

      oldPage.close().catch((error) => {

        LOG.warn("Page close error during tab replacement: %s.", formatError(error));
      });
    }

    // Create a new page with capture.
    let captureResult;

    try {

      captureResult = await createPageWithCapture({

        onFFmpegError: (error) => {

          LOG.error("FFmpeg error during tab replacement recovery: %s.", formatError(error));
          onCircuitBreak();
        },
        profile,
        streamId,
        url
      });
    } catch(error) {

      LOG.error("Failed to create new page during tab replacement: %s.", formatError(error));

      return null;
    }

    // Create a new segmenter for the new capture stream. Continue from the current segment index for playlist continuity, and mark the first segment with a
    // discontinuity tag so clients know the stream parameters may have changed.
    const newSegmenter = createFMP4Segmenter({

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

        LOG.warn("Segmenter stopped unexpectedly after tab replacement for %s.", channelName);

        terminateStream(numericStreamId, channelName, "stream ended unexpectedly after recovery");
        void emitCurrentSystemStatus();
      },

      pendingDiscontinuity: true,
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

    return {

      context: captureResult.context,
      page: captureResult.page
    };
  };
}

/**
 * Starts a new HLS stream for a channel. Sets up the browser capture, creates a native fMP4 segmenter, and registers the stream.
 * @param channelName - The channel key.
 * @param url - The URL to stream.
 * @param req - Express request object (for profile override).
 * @param res - Express response object (for error responses).
 * @returns The stream ID if successful, null if an error occurred.
 */
async function startHLSStream(channelName: string, url: string, req: Request, res: Response): Promise<number | null> {

  const channels = getAllChannels();
  const channel = channels[channelName];
  const profileOverride = req.query.profile as string | undefined;

  // Capture client IP for Channels DVR API integration. This identifies the DVR server for show info lookup.
  const clientAddress: Nullable<string> = req.ip ?? req.socket.remoteAddress ?? null;

  // Create a placeholder to prevent duplicate stream starts while we're setting up.
  const placeholderStreamId = -1;

  setChannelStreamId(channelName, placeholderStreamId);

  let setup;

  // Circuit breaker callback - terminate the HLS stream. Defined separately so it can be reused by both setupStream and the tab replacement handler.
  const onCircuitBreak = (): void => {

    const streamId = getChannelStreamId(channelName);

    if((streamId !== undefined) && (streamId !== placeholderStreamId)) {

      terminateStream(streamId, channelName, "too many errors");
      void emitCurrentSystemStatus();
    }
  };

  // Factory to create the tab replacement handler. This is called by setupStream after stream IDs are generated, allowing the handler to be created with access to
  // those IDs. The handler also needs the channelName and circuit breaker callback which we capture here.
  const tabReplacementFactory: TabReplacementHandlerFactory = (numericStreamId, streamId, profile) => {

    return createTabReplacementHandler(numericStreamId, streamId, channelName, url, profile, onCircuitBreak);
  };

  // If at capacity, try to reclaim an idle stream before starting setup. This avoids rejecting new requests when idle streams can be freed.
  if(getStreamCount() >= CONFIG.streaming.maxConcurrentStreams) {

    reclaimIdleStream();
  }

  try {

    setup = await setupStream(
      {

        channel,
        channelName,
        onTabReplacementFactory: tabReplacementFactory,
        profileOverride,
        url
      },
      onCircuitBreak
    );
  } catch(error) {

    // Remove placeholder on failure.
    deleteChannelStreamId(channelName);

    if(error instanceof StreamSetupError) {

      if(error.statusCode === 503) {

        res.setHeader("Retry-After", "5");
        res.setHeader("X-HDHomeRun-Error", "All Tuners In Use");
      }

      res.status(error.statusCode).send(error.userMessage);

      return null;
    }

    LOG.error("Unexpected error during HLS stream setup: %s.", formatError(error));

    res.status(500).send("Internal server error.");

    return null;
  }

  // Update the channel mapping with the real stream ID.
  setChannelStreamId(channelName, setup.numericStreamId);

  // Continue within stream context for consistent logging. The async is required by runWithStreamContext's signature.
  return runWithStreamContext(
    { channelName: setup.channelName ?? undefined, streamId: setup.streamId, url: setup.url },
    // eslint-disable-next-line @typescript-eslint/require-await
    async () => {

      // Register with null segmenter first because segmenter callbacks (onError, onStop) need the stream to exist in the registry for cleanup logic. The segmenter is
      // assigned immediately after creation below.
      registerStream({

        channelName: setup.channelName,
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

          LOG.error("Segmenter error for channel %s: %s.", channelName, formatError(error));

          terminateStream(setup.numericStreamId, channelName, "stream processing error");
          void emitCurrentSystemStatus();
        },

        onStop: () => {

          // Skip handling if termination was already initiated.
          if(isTerminationInitiated(setup.numericStreamId)) {

            return;
          }

          LOG.warn("Segmenter stopped unexpectedly for channel %s.", channelName);

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

      LOG.info("Streaming %s (%s, %s).", channel.name, setup.profileName, captureMode);

      // Emit stream added event.
      emitStreamAdded(createInitialStreamStatus({

        channelName: setup.channelName,
        numericStreamId: setup.numericStreamId,
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

// ─────────────────────────────────────────────────────────────
// Idle Detection
// ─────────────────────────────────────────────────────────────

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
  let oldest: StreamRegistryEntry | null = null;

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
