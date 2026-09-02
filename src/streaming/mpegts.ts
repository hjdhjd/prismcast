/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mpegts.ts: MPEG-TS streaming handler for PrismCast.
 */
import { LOG, formatError, resolveFFmpegPath, spawnMpegTsRemuxer } from "../utils/index.ts";
import type { Request, Response } from "express";
import { getNamedInitSegment, waitForInitSegment } from "./hlsSegments.ts";
import { getStream, updateLastAccess } from "./registry.ts";
import { initializeStream, sendValidationError, validateChannel } from "./hls.ts";
import { registerClient, unregisterClient } from "./clients.ts";
import { CONFIG } from "../config/index.ts";
import type { Nullable } from "../types/index.ts";
import type { StreamRegistryEntry } from "./registry.ts";
import { StreamSetupError } from "./setup.ts";
import { getChannelStreamId } from "./lifecycle.ts";

/* This module provides a continuous MPEG-TS byte stream for HDHomeRun-compatible clients (such as Plex) that expect raw MPEG-TS when tuning a channel. It supports
 * multiple delivery modes:
 *
 * Capture mode (fMP4 -> MPEG-TS): The capture pipeline produces fMP4 segments. Each MPEG-TS client gets its own FFmpeg remuxer that converts fMP4 to MPEG-TS with
 * codec copy (no transcoding). FFmpeg reads the init segment + media segments from stdin and outputs a continuous MPEG-TS stream on stdout, piped to the HTTP response.
 *
 * Native mode (MPEG-TS pass-through): when the relayed source is itself MPEG-TS, its segments are written directly to the HTTP response without any remuxing.
 *
 * Native mode (fMP4 -> MPEG-TS): when the relayed source is fMP4/CMAF, its fragments are not MPEG-TS, so the stream takes the same codec-copy remuxer the capture
 * path uses. The initialization segment FFmpeg is primed with comes from the relay's per-track store rather than a local encoder.
 *
 * Both modes share the same client lifecycle via connectMpegTsClient(): register the client, subscribe to segment events for real-time delivery, write existing
 * segments for catchup, and clean up on disconnect. The header flush for new streams prevents client timeouts during the 4-10+ second startup sequence.
 *
 * Every connection follows the pipeline beneath it. A tab replacement installs a fresh capture whose encoder can come back with other parameters, and a native
 * stream can fall back to capture, which starts producing fMP4 where it produced MPEG-TS. A connection's output was primed for the pipeline it joined, so each
 * connection watches the stream's initSegment announcement and ends itself when the initialization now in effect is not what it was primed for, letting the
 * player reconnect into the new pipeline through its own retry. A byte-identical initialization carries the same decoder parameters and changes nothing.
 */

// Public Endpoint Handler.

/**
 * Handles MPEG-TS stream requests. Validates the channel, flushes HTTP headers early for new streams, then ensures a stream is running and delegates to the
 * mode-appropriate serving path (FFmpeg remuxer for capture mode, direct pass-through for native mode).
 *
 * For new streams, headers are flushed before stream setup begins so the client sees an immediate 200 response. This prevents timeout failures during the 4-10+
 * second startup sequence. The trade-off is that error responses cannot be sent after the flush - failures are logged server-side and the connection is closed.
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

  // Fast path: a stream already exists (either fully set up or a pending entry from an HLS request). Serve it directly.
  if(existingStreamId !== undefined) {

    await serveMpegTsStream(existingStreamId, channelName, req, res);

    return;
  }

  // No existing stream - validate the channel before flushing headers. This ensures we can still return proper error responses for invalid channels, disabled
  // channels, and login mode.
  const validation = validateChannel(channelName);

  if(!validation.valid) {

    sendValidationError(validation, res);

    return;
  }

  // Flush HTTP 200 headers immediately. The client sees "connection accepted, data coming" and waits patiently. After this point, we cannot send error status codes -
  // failures will close the connection with no data.
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "close");
  res.setHeader("Content-Type", "video/mpeg");
  res.setHeader("transferMode.dlna.org", "Streaming");
  res.flushHeaders();

  // Start a new stream directly. initializeStream blocks until setup completes (no preroll for MPEG-TS clients). Since headers are already flushed, errors are logged
  // and the connection is closed.
  let streamId: Nullable<number>;

  try {

    streamId = await initializeStream({

      channel: validation.channel,
      channelName,
      clientAddress: req.ip ?? req.socket.remoteAddress ?? null,
      mpegTsClient: true,
      profileOverride: req.query["profile"] as string | undefined,
      url: validation.channel.url
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

  await serveMpegTsStream(streamId, channelName, req, res);
}

/**
 * Resolves the initialization segment an MPEG-TS client's remuxer must be primed with, or null when the stream needs none. This is the one place the two
 * initialization storage shapes are reconciled: a capture stream's single slot, and a native fMP4 relay's per-track store, whose video initialization is the one
 * the remuxer reads. The store's undefined is normalized to null here so this module speaks one vocabulary for absence.
 *
 * Native streams that are not fMP4 return null because they need no initialization at all - an MPEG-TS source carries its codec configuration in every segment,
 * and a null container reaches this only on a feed the DRM path abandoned. Callers must therefore select the delivery branch from the container rather than from
 * this result, which cannot tell a stream that wants pass-through apart from an fMP4 stream whose initialization is genuinely missing.
 *
 * @param stream - The stream registry entry to resolve against.
 * @returns The initialization segment to prime the remuxer with, or null when there is none.
 */
export function resolveMpegTsInitSource(stream: StreamRegistryEntry): Nullable<Buffer> {

  const identity = stream.identity;

  if(identity.mode !== "native") {

    return stream.hls.initSegment;
  }

  if(identity.nativeContainer !== "fmp4") {

    return null;
  }

  const videoInitName = stream.hls.currentInitNames.video;

  if(videoInitName === null) {

    return null;
  }

  return getNamedInitSegment(stream.id, videoInitName) ?? null;
}

/**
 * Reports whether the initialization segment now in effect puts a connection on a pipeline its output cannot carry. A connection primed with nothing is a
 * pass-through of an MPEG-TS source, so any initialization at all means the stream now produces fMP4 that a video/mpeg socket cannot carry. A primed connection is
 * on a new pipeline when the bytes differ, which is the same byte equality the segmenter reads its own discontinuity from. Byte-identical means the same decoder
 * parameters, so a same-parameter replacement leaves the connection undisturbed.
 *
 * @param primed - The initialization segment the connection's output was primed with, or null when it was primed with none.
 * @param incoming - The initialization segment now in effect.
 * @returns True when the connection is on a pipeline its output cannot carry.
 */
export function initSegmentChangesPipeline(primed: Nullable<Buffer>, incoming: Buffer): boolean {

  return (primed === null) || !incoming.equals(primed);
}

// Internal Helpers.

/**
 * Serves the MPEG-TS stream once a stream ID is available. Waits for init segment readiness, then delegates by container: a native relay of an MPEG-TS source
 * passes its segments straight to the response, while capture output and a native relay of an fMP4 source both take the FFmpeg codec-copy remuxer. Both paths
 * share client lifecycle via connectMpegTsClient().
 *
 * @param streamId - The numeric stream ID.
 * @param channelName - The channel name for logging.
 * @param req - Express request object.
 * @param res - Express response object.
 */
async function serveMpegTsStream(streamId: number, channelName: string, req: Request, res: Response): Promise<void> {

  // Wait for the init segment to be available. For capture-mode streams, this waits for the fMP4 init segment (ftyp+moov). A native fMP4 relay waits for its
  // video track's first upstream initialization - the same track this connection's remuxer resolves, so the guard below and this wait watch one thing. Every
  // other native stream had signalInitSegmentReady() called during setup, so this returns instantly.
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

  /* Resolve the FFmpeg binary before the stream is read. From the registry read onward the mode branch, the priming, the spawn, and the subscription all run in
   * one synchronous frame, so an initialization stored after that frame reaches the connection as an initSegment event rather than landing unseen between the
   * priming and the subscription. The resolver is memoized, so a pass-through connection pays one cached read for a value it never uses.
   *
   * The fallback to "ffmpeg" leaves spawn() to a PATH lookup when the resolver found no binary; the spawn then fails with ENOENT if PATH is also empty.
   */
  const ffmpegBin = (await resolveFFmpegPath()) ?? "ffmpeg";

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

  // Native-mode streams whose source is already MPEG-TS need no remuxing - their segments are written directly to the response. A native fMP4 relay falls
  // through to the remux path below instead, because its fragments are not MPEG-TS and piping them raw would put fMP4 bytes on a video/mpeg socket. The branch
  // reads the container rather than the resolved initialization, since a null initialization cannot tell a pass-through source apart from an fMP4 source whose
  // initialization is missing - those two need opposite handling.
  if((stream.identity.mode === "native") && (stream.identity.nativeContainer !== "fmp4")) {

    connectMpegTsClient({

      endDelivery: () => {

        if(!res.writableEnded) {

          res.end();
        }
      },
      logLabel: "Native MPEG-TS",
      primedInit: null,
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

  /* Resolve the initialization segment once for this connection. The one value serves the guard below, the priming the remuxer receives, and every comparison
   * the connection makes against a later announcement, so they can never disagree about which initialization this connection was built for.
   *
   * Nothing asynchronous separates this read from the subscription that follows, so a re-store can only land once the connection is already subscribed to hear it.
   */
  const initSource = resolveMpegTsInitSource(stream);

  // Both remux sources need an initialization segment before FFmpeg can process any fragment: capture's own encoder output, and a relayed fMP4 source's
  // upstream initialization.
  if(!initSource) {

    if(!res.headersSent) {

      res.status(500).send("Stream no longer available.");
    } else {

      res.end();
    }

    return;
  }

  // Spawn an FFmpeg process to remux fMP4 to MPEG-TS. The process reads concatenated fMP4 (init segment + media segments) from stdin and outputs a continuous
  // MPEG-TS stream on stdout. Video (H264) and audio (AAC) are copied without transcoding. We declare cleanup as a let so the error callback can reference it before
  // connectMpegTsClient assigns the real implementation - all assignments happen synchronously before any async events can fire.
  let cleanup: () => void = () => { /* No-op until connectMpegTsClient assigns the real cleanup below. */ };

  const streamLog = LOG.withStreamId(stream.streamIdStr);

  const remuxer = spawnMpegTsRemuxer(ffmpegBin, (error) => {

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

      // Write the init segment first - FFmpeg needs the ftyp and moov boxes before it can process any media segments.
      remuxer.stdin.write(initSource);
    },
    endDelivery: () => {

      remuxer.stdin.end();
    },
    extraCleanup: () => {

      remuxer.kill();
    },
    logLabel: "MPEG-TS",
    primedInit: initSource,
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
 * @param options.endDelivery - Callback invoked when the stream terminates and when the pipeline beneath the connection changes. The remux path ends FFmpeg's
 *   stdin so the remuxer flushes and its stdout closes the response; the pass-through path ends the response.
 * @param options.extraCleanup - Optional callback invoked during cleanup for mode-specific teardown (e.g., killing the FFmpeg remuxer).
 * @param options.logLabel - Label for connect/disconnect debug messages (e.g., "MPEG-TS", "Native MPEG-TS").
 * @param options.primedInit - The initialization segment the connection's output was primed with, or null when it was primed with none.
 * @param options.req - Express request object.
 * @param options.res - Express response object.
 * @param options.stream - The stream registry entry.
 * @param options.streamId - The numeric stream ID.
 * @param options.writeSegment - Callback to write segment data to the output target (FFmpeg stdin or HTTP response).
 * @returns Cleanup function. The capture path wires this to the FFmpeg error handler; the native path does not need it.
 */
function connectMpegTsClient({ beforeCatchup, endDelivery, extraCleanup, logLabel, primedInit, req, res, stream, streamId, writeSegment }: {
  beforeCatchup?: () => void;
  endDelivery: () => void;
  extraCleanup?: () => void;
  logLabel: string;
  primedInit: Nullable<Buffer>;
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

  // Set when the connection is being brought to a graceful end, so delivery stops at once while the output drains and the close that follows runs the cleanup.
  let ending = false;

  // Handler for new media segments. Writes each segment to the output target and updates the last access timestamp to prevent idle timeout.
  const onSegment = (filename: string, data: Buffer): void => {

    if(ending || cleanedUp || sentSegments.has(filename)) {

      return;
    }

    sentSegments.add(filename);
    writeSegment(data);
    updateLastAccess(streamId);
  };

  /* Brings the connection to a graceful end exactly once, whichever event asks first. Delivery stops the moment the flag is set, because a write
   * behind an output that is already draining would raise and take the remuxer down mid-flush. The output's own close then reaches cleanup through the request's
   * close event, exactly as a client disconnect does, so the client accounting and the unsubscription keep their one path.
   */
  const endConnection = (): void => {

    if(ending || cleanedUp) {

      return;
    }

    ending = true;
    endDelivery();
  };

  // Handler for stream termination.
  const onTerminated = (): void => {

    endConnection();
  };

  /* Handler for the initialization now in effect, which the stream announces on every store whichever segmenter produced it. This connection's output was primed
   * for one pipeline, so an announcement that does not match that priming means the pipeline beneath the connection changed and the output cannot carry
   * what the stream produces. A byte-identical initialization carries the same decoder parameters, so a same-parameter replacement is ignored.
   */
  const onInitSegment = (data: Buffer): void => {

    if(ending || cleanedUp || !initSegmentChangesPipeline(primedInit, data)) {

      return;
    }

    streamLog.info("Ending the %s client connection - the pipeline beneath it changed, and the player reconnects to pick up the new parameters.", logLabel);
    endConnection();
  };

  // Cleanup function that is safe to call more than once. The cleanedUp flag ensures the underlying work runs only once regardless of which event triggers it first
  // (client disconnect, stream termination, or output error).
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

    stream.hls.segmentEmitter.off("initSegment", onInitSegment);
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
  stream.hls.segmentEmitter.on("initSegment", onInitSegment);
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

    // The linter sees cleanedUp as always false in this synchronous scope, but cleanup() can flip it true from the client-disconnect, stream-termination, or
    // output-error path, so the guard stops the catch-up loop the moment the connection is torn down mid-iteration.
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

