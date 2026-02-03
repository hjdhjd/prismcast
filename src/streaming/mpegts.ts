/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mpegts.ts: MPEG-TS streaming handler for PrismCast.
 */
import { LOG, formatError, spawnMpegTsRemuxer } from "../utils/index.js";
import type { Request, Response } from "express";
import { getStream, updateLastAccess } from "./registry.js";
import { registerClient, unregisterClient } from "./clients.js";
import { CONFIG } from "../config/index.js";
import { ensureChannelStream } from "./hls.js";
import { waitForInitSegment } from "./hlsSegments.js";

/*
 * MPEG-TS STREAMING
 *
 * This module provides a continuous MPEG-TS byte stream from the same capture pipeline used for HLS. It is designed for HDHomeRun-compatible clients (such as Plex)
 * that expect raw MPEG-TS when tuning a channel. The existing capture → segmenter → HLS segments flow is unchanged. Each MPEG-TS client gets its own FFmpeg remuxer
 * that converts stored fMP4 segments to MPEG-TS with codec copy (no transcoding).
 *
 * Data flow per client:
 * 1. ensureChannelStream() starts or reuses an HLS capture
 * 2. Wait for the init segment (ftyp+moov codec configuration)
 * 3. Spawn FFmpeg: -f mp4 -i pipe:0 -c copy -f mpegts pipe:1
 * 4. Write init segment + existing media segments to FFmpeg stdin
 * 5. Subscribe to segment events for new segments in real time
 * 6. Pipe FFmpeg stdout to the HTTP response as video/mp2t
 * 7. On client disconnect or stream termination, kill FFmpeg and clean up
 */

// Public Endpoint Handler.

/**
 * Handles MPEG-TS stream requests. Ensures a capture is running for the requested channel, waits for the init segment, spawns a per-client FFmpeg remuxer to convert
 * fMP4 to MPEG-TS, and streams the output to the HTTP response.
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

  // Ensure a stream is running for this channel. This validates the channel, starts a stream if needed, and handles concurrent startup deduplication.
  const streamId = await ensureChannelStream(channelName, req, res);

  if(streamId === null) {

    // Error response already sent by ensureChannelStream.
    return;
  }

  // Wait for the init segment to be available. The init segment contains codec configuration (ftyp+moov boxes) that FFmpeg needs before it can process media segments.
  const initReady = await waitForInitSegment(streamId, CONFIG.streaming.navigationTimeout);

  if(!initReady) {

    res.setHeader("Retry-After", "5");
    res.status(503).send("Stream is starting. Please retry.");

    return;
  }

  // Get the stream from the registry and verify it's still alive with a valid init segment.
  const stream = getStream(streamId);

  if(!stream || !stream.hls.initSegment) {

    res.status(500).send("Stream no longer available.");

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
  let cleanup: () => void = () => {};

  // Spawn an FFmpeg process to remux fMP4 to MPEG-TS. The process reads concatenated fMP4 (init segment + media segments) from stdin and outputs a continuous
  // MPEG-TS stream on stdout. Video (H264) and audio (AAC) are copied without transcoding.
  const remuxer = spawnMpegTsRemuxer((error) => {

    streamLog.debug("MPEG-TS remuxer error: %s.", formatError(error));
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

    streamLog.debug("MPEG-TS client disconnected.");
  };

  // Clean up when the client disconnects. Registered immediately after cleanup is assigned to minimize the window where a disconnect could be missed.
  req.on("close", () => {

    cleanup();
  });

  // Subscribe to segment events BEFORE writing existing segments to avoid missing any segments added during the catchup phase.
  stream.hls.segmentEmitter.on("segment", onSegment);
  stream.hls.segmentEmitter.on("terminated", onTerminated);

  // Set response headers for MPEG-TS streaming.
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "close");
  res.setHeader("Content-Type", "video/mp2t");

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

  streamLog.debug("MPEG-TS client connected.");
}
