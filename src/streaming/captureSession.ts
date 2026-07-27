/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * captureSession.ts: The capture-pipeline composite for PrismCast.
 *
 * A capture-mode stream is fed by an ordered pipeline of three resources: the raw capture stream from puppeteer-stream (a Matroska feed in FFmpeg mode, a raw
 * fMP4 feed in native-fMP4 mode), an optional FFmpeg child that remuxes Matroska to fMP4, and the fMP4 segmenter that consumes the pipeline output. These three
 * resources require a specific teardown order that is not simply construction reversed, which is why a flat LIFO stack cannot express it. CaptureSession encapsulates
 * that order behind a single Disposable so every owner that tears a capture pipeline down does so identically, and no owner ever has to know the internal order.
 *
 * Data flow (FFmpeg mode):
 *
 *   rawCaptureStream (Matroska)  --pipeline-->  ffmpeg.stdin ... ffmpeg.stdout (fMP4)  --pipe-->  segmenter
 *
 * Data flow (native-fMP4 mode, no FFmpeg):
 *
 *   rawCaptureStream (fMP4)  --pipe-->  segmenter
 *
 * Teardown order is kill -> destroy -> stop. Step 1 must precede step 2 for correctness:
 *
 *   1. Kill FFmpeg FIRST. kill() sets FFmpeg's internal shuttingDown flag unconditionally, so its exit handler treats whatever follows as an expected exit and
 *      never misfires the onError callback. This must precede step 2 because destroying the capture stream carries an EOF down the pipeline to FFmpeg's stdin, and
 *      an FFmpeg flush that exits non-zero would otherwise look like a spurious error. Pinning kill-first here is correctness-by-construction, not a fix for an
 *      active misfire: kill() always runs ahead of the capture stream's destroy, so shuttingDown is set before FFmpeg's inherently asynchronous exit event can run
 *      its handler, whatever the exit code. Because that guarantee comes from the fixed step order rather than from destroy and kill happening in the same
 *      synchronous frame, an await inserted between the steps would remain safe - the flush-triggered non-zero exit can never look like a spurious error.
 *   2. Destroy the raw capture stream. destroy() schedules the stream's close emission; puppeteer-stream's close handler then calls STOP_RECORDING in the capture
 *      extension on a later tick, provided the browser is still connected. That "still connected" guarantee is owned by the CALLER (which must not tear the browser
 *      down before disposing), not by disposal ordering. Without STOP_RECORDING, Chrome's tabCapture state lingers and subsequent getStream() calls hang with
 *      "Cannot capture a tab with an active stream." Destroying the stream also carries EOF to FFmpeg's stdin (when present), draining the pipeline.
 *   3. Stop the segmenter. Its input is the pipeline output (FFmpeg stdout, or the raw capture stream in native-fMP4 mode), which has now ended; stop() detaches
 *      its listeners and flushes the parser.
 *
 * Every underlying operation is safe to call more than once (FFmpeg.kill() guards its SIGTERM send on ffmpeg.killed, Readable.destroy() guards on destroyed,
 * segmenter.stop() guards on stopped), and the session adds its own disposed flag so a double dispose is a cheap no-op. All three operations are synchronous, so
 * the composite is a synchronous Disposable: the asynchronous aftermath (SIGTERM delivery, the STOP_RECORDING chain, the capture pipeline settling) is
 * fire-and-forget by design; no caller needs to await it. Keeping disposal synchronous keeps terminateStream() synchronous and the recovery hot
 * path allocation-free.
 *
 * Scope: the composite owns ONLY the three pipeline resources. It deliberately does NOT own the browser page, the managed-page registration, or window
 * minimization - those are the stream owner's concern and remain caller-owned steps (unregisterManagedPage, page.close, minimizeBrowserWindow) that must still
 * bracket dispose() at each teardown site. dispose() is the FULL-teardown entry point: tab replacement disposes the old session and constructs a fresh one rather
 * than mutating a live session in place.
 */
import type { FFmpegProcess } from "../utils/index.ts";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.ts";
import { LOG } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import type { Readable } from "node:stream";

// Types.

/**
 * The composite handle owning a capture-mode stream's pipeline resources. Implements the project's dispose() convention and TC39 Symbol.dispose so owners may tear
 * the pipeline down explicitly (terminateStream, tab replacement, the native upgrade) or via a scope-bound "using" declaration during setup. The segmenter is
 * attached after construction because it is created one phase later than the capture stream and FFmpeg child (see attachSegmenter).
 */
export interface CaptureSession extends Disposable {

  // Attaches the fMP4 segmenter and pipes the pipeline output into it. Called once, after the segmenter is created. If the session was already disposed (the stream
  // was terminated mid-setup), the incoming segmenter is stopped immediately instead of being wired to a torn-down pipeline, so callers never have to detect and
  // stop an orphaned segmenter themselves.
  readonly attachSegmenter: (segmenter: FMP4SegmenterResult) => void;

  // Whether the session has been disposed. Once true, dispose() is a no-op and attachSegmenter() stops its argument rather than wiring it.
  readonly disposed: boolean;

  // Tears the pipeline down in the kill -> destroy -> stop order. Idempotent and safe to call from any teardown path. Aliased to [Symbol.dispose].
  readonly dispose: () => void;

  // The attached fMP4 segmenter, or null before attachSegmenter() has run. Exposed read-only so the registry, monitor, and shutdown resume-state collector can
  // read segment indices, init segments, and session statistics without reaching past the session. The reference is retained after disposal because the segmenter's
  // accumulated statistics remain valid and readable after stop(), even though every current caller reads them before disposing.
  readonly segmenter: Nullable<FMP4SegmenterResult>;

  // TC39 explicit resource management hook. Aliases dispose() so "using session = createCaptureSession(...)" produces deterministic teardown at scope exit,
  // including on a thrown error.
  readonly [Symbol.dispose]: () => void;
}

/**
 * Options for createCaptureSession. The segmenter is intentionally absent: it is created in a later phase and wired via the returned handle's attachSegmenter().
 */
export interface CreateCaptureSessionOptions {

  // The FFmpeg child remuxing Matroska to fMP4, or null in native-fMP4 capture mode where the raw capture stream is already fMP4 and needs no transcoding.
  readonly ffmpegProcess: Nullable<FFmpegProcess>;

  // The raw capture stream from puppeteer-stream. Destroyed during disposal to release Chrome's tabCapture state while the browser is still connected.
  readonly rawCaptureStream: Readable;
}

// Factory.

/**
 * Creates a CaptureSession over an already-acquired capture stream and optional FFmpeg child. The pipeline output - the stream the segmenter will consume - is
 * derived here: FFmpeg's stdout when transcoding, otherwise the raw capture stream itself.
 * @param options - The capture stream and optional FFmpeg child to own.
 * @returns A CaptureSession handle.
 */
export function createCaptureSession(options: CreateCaptureSessionOptions): CaptureSession {

  const { ffmpegProcess, rawCaptureStream } = options;

  // The pipeline output the segmenter consumes. In FFmpeg mode this is FFmpeg's fMP4 stdout; in native-fMP4 mode the raw capture stream is already fMP4 and is its
  // own output. Computed once because both handles are stable for the session's lifetime.
  const outputStream: Readable = ffmpegProcess?.stdout ?? rawCaptureStream;

  let disposed = false;
  let segmenter: Nullable<FMP4SegmenterResult> = null;

  const attachSegmenter = (incoming: FMP4SegmenterResult): void => {

    // The stream was terminated before the segmenter could be wired (a rare mid-setup race). Stop the orphan rather than piping it into a pipeline that has already
    // been torn down; the debug breadcrumb keeps this rare race observable. This is the single home for that cleanup, so callers never replicate it.
    if(disposed) {

      LOG.debug("streaming:setup", "Segmenter attached to an already-disposed capture session; stopping the orphan.");
      incoming.stop();

      return;
    }

    segmenter = incoming;

    // Pipe the pipeline output into the segmenter. The segmenter attaches its own data/end/error listeners to the output stream.
    incoming.pipe(outputStream);
  };

  const dispose = (): void => {

    if(disposed) {

      return;
    }

    disposed = true;

    // Step 1: kill FFmpeg first so its shuttingDown flag is set before the capture stream's EOF can reach its stdin. See the module header for why the order matters.
    ffmpegProcess?.kill();

    // Step 2: destroy the raw capture stream to fire STOP_RECORDING while the browser is still connected and to carry EOF down the pipeline.
    if(!rawCaptureStream.destroyed) {

      rawCaptureStream.destroy();
    }

    // Step 3: stop the segmenter, whose input pipeline has now ended.
    segmenter?.stop();
  };

  return {

    attachSegmenter,
    dispose,
    [Symbol.dispose]: dispose,

    get disposed(): boolean {

      return disposed;
    },

    get segmenter(): Nullable<FMP4SegmenterResult> {

      return segmenter;
    }
  };
}
