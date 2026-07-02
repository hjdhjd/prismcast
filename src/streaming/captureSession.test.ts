/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * captureSession.test.ts: Unit tests for the capture-pipeline composite. createCaptureSession owns three resources (raw capture stream, optional FFmpeg child, fMP4
 * segmenter) and exposes a single Disposable whose teardown runs kill -> destroy -> stop. These tests pin that order against synthetic doubles - in particular the
 * correctness-critical first step (FFmpeg is killed, setting its shuttingDown flag, before the capture stream is destroyed and carries EOF to FFmpeg's stdin), the
 * idempotency of a repeated dispose, the native-fMP4 (no FFmpeg) and segmenter-less (setup-phase / native-upgrade) shapes, the "using" scope-bound path, and the
 * orphaned-segmenter fold where attaching a segmenter to an already-disposed session stops it instead of wiring it. The composite is pure orchestration over the
 * three handles' own (already covered) idempotent operations, so synthetic doubles fully exercise it without a real browser capture, FFmpeg child, or fMP4 feed.
 */
import { describe, test } from "node:test";
import type { FFmpegProcess } from "../utils/index.ts";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.ts";
import type { Nullable } from "../types/index.ts";
import type { Readable } from "node:stream";
import assert from "node:assert/strict";
import { createCaptureSession } from "./captureSession.ts";

// A recording of every teardown operation across the three resources, shared by a rig's doubles so tests can assert ordering and call counts.
interface RigCalls {

  destroyCount: number;
  killCount: number;
  order: string[];
  shuttingDownAtDestroy: Nullable<boolean>;
  stopCount: number;
}

// A rig bundles the three synthetic resources plus the shared call record and a reader for the segmenter's pipe target.
interface Rig {

  calls: RigCalls;
  ffmpegProcess: FFmpegProcess;
  pipedTo: () => Nullable<Readable>;
  rawCaptureStream: Readable;
  segmenter: FMP4SegmenterResult;
  stdout: Readable;
}

/* createRig builds the three synthetic capture resources wired to a shared call record. The FFmpeg double's kill() sets a closure-scoped shuttingDown flag that the
 * capture stream's destroy() snapshots, so a test can prove the flag was already set at the moment EOF would have reached FFmpeg. startDestroyed seeds an
 * already-destroyed capture stream to exercise the destroy guard.
 */
function createRig(options: { startDestroyed?: boolean } = {}): Rig {

  const { startDestroyed = false } = options;

  const calls: RigCalls = { destroyCount: 0, killCount: 0, order: [], shuttingDownAtDestroy: null, stopCount: 0 };

  let destroyed = startDestroyed;
  let ffmpegShuttingDown = false;
  let pipedTo: Nullable<Readable> = null;

  // FFmpeg's fMP4 stdout - the segmenter's pipe target in FFmpeg mode. A tagged object suffices; the session only forwards it to segmenter.pipe().
  const stdout = { tag: "ffmpeg-stdout" } as unknown as Readable;

  // The session only ever touches kill() and stdout, so the double carries just those two members (the cast satisfies the rest of the FFmpegProcess contract).
  const ffmpegProcess = {

    kill: (): void => {

      calls.killCount++;
      ffmpegShuttingDown = true;
      calls.order.push("kill");
    },
    stdout
  } as unknown as FFmpegProcess;

  const rawCaptureStream = {

    destroy: (): void => {

      calls.destroyCount++;
      destroyed = true;
      calls.shuttingDownAtDestroy = ffmpegShuttingDown;
      calls.order.push("destroy");
    },

    get destroyed(): boolean {

      return destroyed;
    }
  } as unknown as Readable;

  const segmenter = {

    pipe: (target: Readable): void => {

      pipedTo = target;
    },
    stop: (): void => {

      calls.stopCount++;
      calls.order.push("stop");
    }
  } as unknown as FMP4SegmenterResult;

  return { calls, ffmpegProcess, pipedTo: (): Nullable<Readable> => pipedTo, rawCaptureStream, segmenter, stdout };
}

describe("createCaptureSession - teardown order", () => {

  test("disposes in kill -> destroy -> stop order with the segmenter attached", () => {

    const rig = createRig();
    const session = createCaptureSession({ ffmpegProcess: rig.ffmpegProcess, rawCaptureStream: rig.rawCaptureStream });

    session.attachSegmenter(rig.segmenter);
    session.dispose();

    assert.deepEqual(rig.calls.order, [ "kill", "destroy", "stop" ]);
  });

  test("kills FFmpeg before destroying the capture stream so shuttingDown is set before EOF reaches stdin", () => {

    // This is the correctness-critical invariant the composite exists to guarantee. Destroying the capture stream carries an EOF down the pipeline to FFmpeg's
    // stdin; if FFmpeg flushed and exited non-zero before kill() set its shuttingDown flag, its exit handler would misfire onError. The flag must already be set.
    const rig = createRig();
    const session = createCaptureSession({ ffmpegProcess: rig.ffmpegProcess, rawCaptureStream: rig.rawCaptureStream });

    session.attachSegmenter(rig.segmenter);
    session.dispose();

    assert.equal(rig.calls.shuttingDownAtDestroy, true);
  });

  test("native-fMP4 mode (no FFmpeg) skips the kill step and tears down capture then segmenter", () => {

    const rig = createRig();
    const session = createCaptureSession({ ffmpegProcess: null, rawCaptureStream: rig.rawCaptureStream });

    session.attachSegmenter(rig.segmenter);
    session.dispose();

    assert.deepEqual(rig.calls.order, [ "destroy", "stop" ]);
  });

  test("disposes capture and FFmpeg when no segmenter has been attached", () => {

    // The setup-phase failure paths and the native upgrade dispose the session before a segmenter is wired. Teardown must still kill FFmpeg and destroy the capture
    // stream, and must not throw on the absent segmenter.
    const rig = createRig();
    const session = createCaptureSession({ ffmpegProcess: rig.ffmpegProcess, rawCaptureStream: rig.rawCaptureStream });

    assert.equal(session.segmenter, null);

    session.dispose();

    assert.deepEqual(rig.calls.order, [ "kill", "destroy" ]);
  });

  test("does not re-destroy an already-destroyed capture stream", () => {

    const rig = createRig({ startDestroyed: true });
    const session = createCaptureSession({ ffmpegProcess: rig.ffmpegProcess, rawCaptureStream: rig.rawCaptureStream });

    session.attachSegmenter(rig.segmenter);
    session.dispose();

    assert.equal(rig.calls.destroyCount, 0);
    assert.deepEqual(rig.calls.order, [ "kill", "stop" ]);
  });
});

describe("createCaptureSession - idempotency", () => {

  test("a second dispose is a no-op", () => {

    const rig = createRig();
    const session = createCaptureSession({ ffmpegProcess: rig.ffmpegProcess, rawCaptureStream: rig.rawCaptureStream });

    session.attachSegmenter(rig.segmenter);
    session.dispose();
    session.dispose();

    assert.equal(rig.calls.killCount, 1);
    assert.equal(rig.calls.destroyCount, 1);
    assert.equal(rig.calls.stopCount, 1);
    assert.equal(session.disposed, true);
  });

  test("[Symbol.dispose] is the same disposer as dispose() and shares the disposed guard", () => {

    // The "using" contract rests on [Symbol.dispose] being the exact dispose closure, so a regression that pointed it at a second, independently-flagged closure
    // would break idempotency without any other test noticing. Pin both the identity and that a repeated dispose through the Symbol alias is a no-op.
    const rig = createRig();
    const session = createCaptureSession({ ffmpegProcess: rig.ffmpegProcess, rawCaptureStream: rig.rawCaptureStream });

    session.attachSegmenter(rig.segmenter);

    assert.equal(session[Symbol.dispose], session.dispose);

    session[Symbol.dispose]();
    session[Symbol.dispose]();

    assert.equal(rig.calls.killCount, 1);
    assert.equal(rig.calls.destroyCount, 1);
    assert.equal(rig.calls.stopCount, 1);
  });

  test("disposes deterministically at scope exit via the using declaration", () => {

    const rig = createRig();

    {

      using session = createCaptureSession({ ffmpegProcess: rig.ffmpegProcess, rawCaptureStream: rig.rawCaptureStream });

      session.attachSegmenter(rig.segmenter);

      assert.equal(session.disposed, false);
    }

    assert.deepEqual(rig.calls.order, [ "kill", "destroy", "stop" ]);
  });
});

describe("createCaptureSession - segmenter wiring", () => {

  test("pipes the segmenter to FFmpeg stdout in FFmpeg mode", () => {

    const rig = createRig();
    const session = createCaptureSession({ ffmpegProcess: rig.ffmpegProcess, rawCaptureStream: rig.rawCaptureStream });

    session.attachSegmenter(rig.segmenter);

    assert.equal(rig.pipedTo(), rig.stdout);
    assert.equal(session.segmenter, rig.segmenter);
  });

  test("pipes the segmenter to the raw capture stream in native-fMP4 mode", () => {

    const rig = createRig();
    const session = createCaptureSession({ ffmpegProcess: null, rawCaptureStream: rig.rawCaptureStream });

    session.attachSegmenter(rig.segmenter);

    assert.equal(rig.pipedTo(), rig.rawCaptureStream);
  });

  test("stops the orphan and does not wire it when attaching to an already-disposed session", () => {

    // The session contract owns orphaned-segmenter cleanup: a stream terminated mid-setup disposes the session before the segmenter is created, so the later attach
    // must stop the orphan rather than pipe it into a torn-down pipeline.
    const rig = createRig();
    const session = createCaptureSession({ ffmpegProcess: rig.ffmpegProcess, rawCaptureStream: rig.rawCaptureStream });

    session.dispose();
    session.attachSegmenter(rig.segmenter);

    assert.equal(rig.calls.stopCount, 1);
    assert.equal(rig.pipedTo(), null);
    assert.equal(session.segmenter, null);
  });

  test("retains the segmenter reference after disposal so post-stop statistics remain readable", () => {

    const rig = createRig();
    const session = createCaptureSession({ ffmpegProcess: rig.ffmpegProcess, rawCaptureStream: rig.rawCaptureStream });

    session.attachSegmenter(rig.segmenter);
    session.dispose();

    assert.equal(session.segmenter, rig.segmenter);
  });
});
