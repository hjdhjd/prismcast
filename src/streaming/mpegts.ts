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
import type { StreamRegistryEntry } from "./registry.js";
import { StreamSetupError } from "./setup.js";
import { getChannelStreamId } from "./lifecycle.js";
import { waitForInitSegment } from "./hlsSegments.js";

/* This module provides a continuous MPEG-TS byte stream for HDHomeRun-compatible clients (such as Plex) that expect raw MPEG-TS when tuning a channel. Two delivery
 * modes are supported:
 *
 * Capture mode (fMP4 → MPEG-TS): The capture pipeline produces fMP4 segments. Each MPEG-TS client gets its own FFmpeg remuxer that converts fMP4 to MPEG-TS with
 * codec copy (no transcoding). FFmpeg reads the init segment + media segments from stdin and outputs a continuous MPEG-TS stream on stdout, piped to the HTTP response.
 *
 * Native mode (MPEG-TS pass-through): The native HLS proxy already produces MPEG-TS segments. These are written directly to the HTTP response without any remuxing.
 *
 * Both modes share the same client lifecycle via connectMpegTsClient(): register the client, subscribe to segment events for real-time delivery, write existing
 * segments for catchup, and clean up on disconnect. The header flush for new streams prevents client timeouts during the 4-10+ second startup sequence.
 */

// Public Endpoint Handler.

/**
 * Handles MPEG-TS stream requests. Validates the channel, flushes HTTP headers early for new streams, then ensures a stream is running and delegates to the
 * mode-appropriate serving path (FFmpeg remuxer for capture mode, direct pass-through for native mode).
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
  res.setHeader("Content-Type", "video/mpeg");
  res.setHeader("transferMode.dlna.org", "Streaming");
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
        mpegTsClient: true,
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
 * Serves the MPEG-TS stream once a stream ID is available. Waits for init segment readiness, then delegates to mode-specific serving: capture mode spawns an FFmpeg
 * remuxer to convert fMP4 to MPEG-TS, while native mode passes .ts segments directly to the response. Both paths share client lifecycle via connectMpegTsClient().
 *
 * @param streamId - The numeric stream ID.
 * @param channelName - The channel name for logging.
 * @param req - Express request object.
 * @param res - Express response object.
 */
async function serveMpegTsStream(streamId: number, channelName: string, req: Request, res: Response): Promise<void> {

  // Wait for the init segment to be available. For capture-mode streams, this waits for the fMP4 init segment (ftyp+moov). For native-mode streams,
  // signalInitSegmentReady() was called immediately during setup, so this returns instantly.
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

  // Get the stream from the registry.
  const stream = getStream(streamId);

  if(!stream) {

    if(!res.headersSent) {

      res.status(500).send("Stream no longer available.");
    } else {

      res.end();
    }

    return;
  }

  // For native-mode streams, segments are already MPEG-TS — write them directly to the response without FFmpeg.
  if(stream.streamingMode === "native") {

    connectMpegTsClient({

      logLabel: "Native MPEG-TS",
      onStreamTerminated: () => {

        if(!res.writableEnded) {

          res.end();
        }
      },
      req,
      res,
      stream,
      streamId,
      writeSegment: (data) => {

        if(!res.writableEnded) {

          res.write(data);
        }
      }
    });

    return;
  }

  // For capture-mode streams, verify the init segment is available (required by FFmpeg).
  if(!stream.hls.initSegment) {

    if(!res.headersSent) {

      res.status(500).send("Stream no longer available.");
    } else {

      res.end();
    }

    return;
  }

  // Spawn an FFmpeg process to remux fMP4 to MPEG-TS. The process reads concatenated fMP4 (init segment + media segments) from stdin and outputs a continuous
  // MPEG-TS stream on stdout. Video (H264) and audio (AAC) are copied without transcoding. We declare cleanup as a let so the error callback can reference it before
  // connectMpegTsClient assigns the real implementation — all assignments happen synchronously before any async events can fire.
  let cleanup: () => void = () => { /* No-op until connectMpegTsClient assigns the real cleanup below. */ };

  const streamLog = LOG.withStreamId(stream.streamIdStr);

  const remuxer = spawnMpegTsRemuxer((error) => {

    streamLog.debug("streaming:mpegts", "MPEG-TS remuxer error: %s.", formatError(error));
    cleanup();

    if(!res.writableEnded) {

      res.end();
    }
  }, stream.streamIdStr);

  // Suppress errors from writing to a closed FFmpeg stdin. This can happen during cleanup when the capture stream closes before we stop writing.
  remuxer.stdin.on("error", () => {

    cleanup();
  });

  cleanup = connectMpegTsClient({

    beforeCatchup: () => {

      // Pipe FFmpeg stdout to the HTTP response. When FFmpeg exits (either from stdin ending or being killed), stdout closes and the response ends automatically.
      remuxer.stdout.pipe(res);

      // Write the init segment first — FFmpeg needs the ftyp and moov boxes before it can process any media segments.
      remuxer.stdin.write(stream.hls.initSegment);
    },
    extraCleanup: () => {

      remuxer.kill();
    },
    logLabel: "MPEG-TS",
    onStreamTerminated: () => {

      remuxer.stdin.end();
    },
    req,
    res,
    stream,
    streamId,
    writeSegment: (data) => {

      remuxer.stdin.write(data);
    }
  });
}

/**
 * Sets up a single MPEG-TS client session with shared lifecycle management. Handles client registration, segment event subscription, response headers, catchup
 * delivery of existing segments, and cleanup on disconnect. The caller provides mode-specific callbacks for segment writing and stream termination.
 *
 * @param options.beforeCatchup - Optional callback invoked after event subscription and headers but before writing existing segments. Used by the capture path to
 *   pipe FFmpeg output and write the init segment before catchup begins.
 * @param options.extraCleanup - Optional callback invoked during cleanup for mode-specific teardown (e.g., killing the FFmpeg remuxer).
 * @param options.logLabel - Label for connect/disconnect debug messages (e.g., "MPEG-TS", "Native MPEG-TS").
 * @param options.onStreamTerminated - Callback invoked when the stream emits a "terminated" event. Capture mode ends FFmpeg stdin; native mode ends the response.
 * @param options.req - Express request object.
 * @param options.res - Express response object.
 * @param options.stream - The stream registry entry.
 * @param options.streamId - The numeric stream ID.
 * @param options.writeSegment - Callback to write segment data to the output target (FFmpeg stdin or HTTP response).
 * @returns Cleanup function. The capture path wires this to the FFmpeg error handler; the native path does not need it.
 */
function connectMpegTsClient({ beforeCatchup, extraCleanup, logLabel, onStreamTerminated, req, res, stream, streamId, writeSegment }: {
  beforeCatchup?: () => void;
  extraCleanup?: () => void;
  logLabel: string;
  onStreamTerminated: () => void;
  req: Request;
  res: Response;
  stream: StreamRegistryEntry;
  streamId: number;
  writeSegment: (data: Buffer) => void;
}): () => void {

  const clientAddress = req.ip ?? req.socket.remoteAddress ?? "unknown";

  // Increment the MPEG-TS client counter to prevent idle timeout while this client is connected.
  stream.mpegTsClientCount++;
  updateLastAccess(streamId);
  registerClient(streamId, clientAddress, "mpegts");

  const streamLog = LOG.withStreamId(stream.streamIdStr);

  // Track which segments have been written to avoid duplicates during the catchup phase. When we subscribe to segment events and then write existing segments, a new
  // segment could arrive via the event that we also encounter in the existing segment iteration. The Set prevents writing it twice.
  const sentSegments = new Set<string>();
  let cleanedUp = false;

  // Handler for new media segments. Writes each segment to the output target and updates the last access timestamp to prevent idle timeout.
  const onSegment = (filename: string, data: Buffer): void => {

    if(cleanedUp || sentSegments.has(filename)) {

      return;
    }

    sentSegments.add(filename);
    writeSegment(data);
    updateLastAccess(streamId);
  };

  // Handler for stream termination.
  const onTerminated = (): void => {

    if(cleanedUp) {

      return;
    }

    onStreamTerminated();
  };

  // Idempotent cleanup function. The cleanedUp flag ensures it runs only once regardless of which event triggers it first (client disconnect, stream termination, or
  // output error).
  const cleanup = (): void => {

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
    extraCleanup?.();

    streamLog.debug("streaming:mpegts", "%s client disconnected.", logLabel);
  };

  // Clean up when the client disconnects.
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
    res.setHeader("Content-Type", "video/mpeg");
    res.setHeader("transferMode.dlna.org", "Streaming");
  }

  // Run any mode-specific initialization before writing catchup segments.
  beforeCatchup?.();

  // Write all existing media segments to provide immediate playback catchup. The sentSegments Set deduplicates against any segments received via the event handler
  // during this iteration.
  for(const [ filename, data ] of stream.hls.segments) {

    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(cleanedUp) {

      break;
    }

    sentSegments.add(filename);
    writeSegment(data);
  }

  streamLog.debug("streaming:mpegts", "%s client connected.", logLabel);

  return cleanup;
}

