/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mpegts.ts: MPEG-TS streaming handler for PrismCast.
 */
import type { Channel, Nullable } from "../types/index.js";
import { LOG, formatError, spawnMpegTsRemuxer } from "../utils/index.js";
import type { Request, Response } from "express";
import { awaitStreamReadySilent, initializeStream, sendValidationError, validateChannel } from "./hls.js";
import { getStream, updateLastAccess } from "./registry.js";
import { registerClient, unregisterClient } from "./clients.js";
import { CONFIG } from "../config/index.js";
import { StreamSetupError } from "./setup.js";
import { getChannelStreamId } from "./lifecycle.js";
import { waitForInitSegment } from "./hlsSegments.js";

/* This module provides a continuous MPEG-TS byte stream from the same capture pipeline used for HLS. It is designed for HDHomeRun-compatible clients (such as Plex)
 * that expect raw MPEG-TS when tuning a channel. The existing capture → segmenter → HLS segments flow is unchanged. Each MPEG-TS client gets its own FFmpeg remuxer
 * that converts stored fMP4 segments to MPEG-TS with codec copy (no transcoding).
 *
 * Data flow per client:
 * 1. Validate channel and check for existing stream
 * 2. If new stream needed, flush HTTP 200 headers immediately (so the client sees "connection accepted")
 * 3. initializeStream() starts the capture, or awaitStreamReadySilent() waits for an in-progress startup
 * 4. Wait for the init segment (ftyp+moov codec configuration)
 * 5. Spawn FFmpeg: -f mp4 -i pipe:0 -c copy -f mpegts pipe:1
 * 6. Write init segment + existing media segments to FFmpeg stdin
 * 7. Subscribe to segment events for new segments in real time
 * 8. Pipe FFmpeg stdout to the HTTP response as video/mp2t
 * 9. On client disconnect or stream termination, kill FFmpeg and clean up
 *
 * The header flush in step 2 prevents client timeouts. Without it, the client receives zero bytes until the entire stream setup completes (4-10+ seconds), which may
 * exceed the client's connection timeout.
 */

// Public Endpoint Handler.

/**
 * Handles MPEG-TS stream requests. Validates the channel, flushes HTTP headers early for new streams, then ensures a capture is running, waits for the init segment,
 * spawns a per-client FFmpeg remuxer, and streams the output.
 *
 * For new streams, headers are flushed before stream setup begins so the client sees an immediate 200 response. This prevents timeout failures during the 4-10+
 * second startup sequence. The trade-off is that error responses cannot be sent after the flush — failures are logged server-side and the connection is closed.
 *
 * Route: GET /stream/:name
 *
 * @param req - Express request object.
 * @param res - Express response object.
 */
export async function handleMpegTsStream(req: Request, res: Response): Promise<void> {

  const channelName = (req.params as { name?: string }).name;

  if(!channelName) {

    res.status(400).send("Channel name is required.");

    return;
  }

  // Check for an existing stream first. If one exists, we can skip validation and header flushing.
  const existingStreamId = getChannelStreamId(channelName);

  // Fast path: a real stream already exists. No early flush needed — the stream data will flow quickly.
  if((existingStreamId !== undefined) && (existingStreamId !== -1)) {

    await serveMpegTsStream(existingStreamId, channelName, req, res);

    return;
  }

  // If no existing stream or startup in progress, validate the channel before flushing headers. This ensures we can still return proper error responses for invalid
  // channels, disabled channels, and login mode. Store the validated channel for use during stream initialization below.
  let validatedChannel: Channel | undefined;

  if(existingStreamId === undefined) {

    const validation = validateChannel(channelName);

    if(!validation.valid) {

      sendValidationError(validation, res);

      return;
    }

    validatedChannel = validation.channel;
  }

  // Flush HTTP 200 headers immediately. The client sees "connection accepted, data coming" and waits patiently. After this point, we cannot send error status codes —
  // failures will close the connection with no data.
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "close");
  res.setHeader("Content-Type", "video/mp2t");
  res.flushHeaders();

  // Acquire the stream. If a startup is in progress (another request started it), poll silently. Otherwise, start a new stream via initializeStream().
  let streamId: Nullable<number>;

  if(existingStreamId === -1) {

    // Another request is already starting this stream. Wait silently (no error responses possible after flush).
    streamId = await awaitStreamReadySilent(channelName);

    if(streamId === null) {

      LOG.warn("MPEG-TS stream startup failed for %s (startup did not complete).", channelName);
      res.end();

      return;
    }
  } else {

    // Start a new stream directly. validatedChannel is guaranteed set: this branch runs only when existingStreamId === undefined, which requires successful
    // validation above. Since headers are already flushed, errors are logged and the connection is closed.
    if(!validatedChannel) {

      res.end();

      return;
    }

    try {

      streamId = await initializeStream({

        channel: validatedChannel,
        channelName,
        clientAddress: req.ip ?? req.socket.remoteAddress ?? null,
        profileOverride: req.query.profile as string | undefined,
        url: validatedChannel.url
      });
    } catch(error) {

      if(error instanceof StreamSetupError) {

        LOG.warn("MPEG-TS stream startup failed for %s: %s.", channelName, error.userMessage);
      } else {

        LOG.warn("MPEG-TS stream startup failed for %s: %s.", channelName, formatError(error));
      }

      res.end();

      return;
    }

    if(streamId === null) {

      LOG.warn("MPEG-TS stream startup failed for %s (terminated during setup).", channelName);
      res.end();

      return;
    }
  }

  await serveMpegTsStream(streamId, channelName, req, res);
}

// Internal Helpers.

/**
 * Serves the MPEG-TS stream once a stream ID is available. Waits for the init segment, spawns the FFmpeg remuxer, and pipes the output to the response. This is the
 * shared implementation used by both the fast path (existing stream) and the flush path (new stream).
 *
 * @param streamId - The numeric stream ID.
 * @param channelName - The channel name for logging.
 * @param req - Express request object.
 * @param res - Express response object.
 */
async function serveMpegTsStream(streamId: number, channelName: string, req: Request, res: Response): Promise<void> {

  // Wait for the init segment to be available. The init segment contains codec configuration (ftyp+moov boxes) that FFmpeg needs before it can process media segments.
  const initReady = await waitForInitSegment(streamId, CONFIG.streaming.navigationTimeout);

  if(!initReady) {

    if(!res.headersSent) {

      res.setHeader("Retry-After", "5");
      res.status(503).send("Stream is starting. Please retry.");
    } else {

      LOG.warn("MPEG-TS init segment timeout for %s.", channelName);
      res.end();
    }

    return;
  }

  // Get the stream from the registry and verify it's still alive with a valid init segment.
  const stream = getStream(streamId);

  if(!stream?.hls.initSegment) {

    if(!res.headersSent) {

      res.status(500).send("Stream no longer available.");
    } else {

      res.end();
    }

    return;
  }

  // Capture client address for client tracking. Captured before any async operations so it remains consistent in the cleanup closure, even if the request object
  // becomes unreliable after disconnect.
  const clientAddress = req.ip ?? req.socket.remoteAddress ?? "unknown";

  // Increment the MPEG-TS client counter to prevent idle timeout while this client is connected.
  stream.mpegTsClientCount++;
  updateLastAccess(streamId);
  registerClient(streamId, clientAddress, "mpegts");

  const streamLog = LOG.withStreamId(stream.streamIdStr);

  // Track which segments have been written to FFmpeg stdin to avoid duplicates during the catchup phase. When we subscribe to segment events and then write
  // existing segments, a new segment could arrive via the event that we also encounter in the existing segment iteration. The Set prevents writing it twice.
  const sentSegments = new Set<string>();
  let cleanedUp = false;

  // We declare cleanup as a let initialized to a no-op so the error callback and stdin error handler can reference it before the real implementation is assigned. It
  // is reassigned to the real cleanup function immediately after all handlers are defined, before any asynchronous events can fire.
  let cleanup: () => void = () => { /* No-op until real cleanup is assigned below. */ };

  // Spawn an FFmpeg process to remux fMP4 to MPEG-TS. The process reads concatenated fMP4 (init segment + media segments) from stdin and outputs a continuous
  // MPEG-TS stream on stdout. Video (H264) and audio (AAC) are copied without transcoding.
  const remuxer = spawnMpegTsRemuxer((error) => {

    streamLog.debug("streaming:mpegts", "MPEG-TS remuxer error: %s.", formatError(error));
    cleanup();

    if(!res.writableEnded) {

      res.end();
    }
  }, stream.streamIdStr);

  // Handler for new media segments. Writes each segment to FFmpeg stdin and updates the last access timestamp to prevent idle timeout.
  const onSegment = (filename: string, data: Buffer): void => {

    if(cleanedUp || sentSegments.has(filename)) {

      return;
    }

    sentSegments.add(filename);
    remuxer.stdin.write(data);
    updateLastAccess(streamId);
  };

  // Handler for stream termination. Ends FFmpeg stdin gracefully so it can flush remaining data and exit cleanly.
  const onTerminated = (): void => {

    if(cleanedUp) {

      return;
    }

    remuxer.stdin.end();
  };

  // Suppress errors from writing to a closed FFmpeg stdin. This can happen during cleanup when the capture stream closes before we stop writing.
  remuxer.stdin.on("error", () => {

    cleanup();
  });

  // Assign the real cleanup function. This is idempotent — the cleanedUp flag ensures it only runs once regardless of which event triggers it first (client
  // disconnect, stream termination, or FFmpeg error).
  cleanup = (): void => {

    if(cleanedUp) {

      return;
    }

    cleanedUp = true;

    // Decrement the client counter. Re-read the stream from the registry since it may have been unregistered during stream termination.
    const currentStream = getStream(streamId);

    if(currentStream) {

      currentStream.mpegTsClientCount = Math.max(0, currentStream.mpegTsClientCount - 1);

      // When the last MPEG-TS client disconnects, reset the idle timer so the stream gets the standard idle timeout grace period before cleanup. This gives
      // channel-surfing users time to switch back without the stream being torn down immediately.
      if(currentStream.mpegTsClientCount === 0) {

        updateLastAccess(streamId);
      }
    }

    unregisterClient(streamId, clientAddress, "mpegts");

    stream.hls.segmentEmitter.off("segment", onSegment);
    stream.hls.segmentEmitter.off("terminated", onTerminated);
    remuxer.kill();

    streamLog.debug("streaming:mpegts", "MPEG-TS client disconnected.");
  };

  // Clean up when the client disconnects. Registered immediately after cleanup is assigned to minimize the window where a disconnect could be missed.
  req.on("close", () => {

    cleanup();
  });

  // Subscribe to segment events BEFORE writing existing segments to avoid missing any segments added during the catchup phase.
  stream.hls.segmentEmitter.on("segment", onSegment);
  stream.hls.segmentEmitter.on("terminated", onTerminated);

  // Set response headers if they haven't been flushed yet (fast path for existing streams).
  if(!res.headersSent) {

    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "close");
    res.setHeader("Content-Type", "video/mp2t");
  }

  // Pipe FFmpeg stdout to the HTTP response. When FFmpeg exits (either from stdin ending or being killed), stdout closes and the response ends automatically.
  remuxer.stdout.pipe(res);

  // Write the init segment first — FFmpeg needs the ftyp and moov boxes before it can process any media segments.
  remuxer.stdin.write(stream.hls.initSegment);

  // Write all existing media segments to provide immediate playback catchup. The sentSegments Set deduplicates against any segments received via the event handler
  // during this iteration.
  for(const [ filename, data ] of stream.hls.segments) {

    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(cleanedUp) {

      break;
    }

    sentSegments.add(filename);
    remuxer.stdin.write(data);
  }

  streamLog.debug("streaming:mpegts", "MPEG-TS client connected.");
}
