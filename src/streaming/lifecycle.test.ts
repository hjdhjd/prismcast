/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lifecycle.test.ts: Unit tests for the stream lifecycle module - the documented SSOT for stream termination. lifecycle.ts owns three concerns: the channel-name
 * to stream-id lookup index (channelToStreamId), the in-flight termination guard set (terminationInitiated), and the authoritative terminateStream() function that
 * tears down every resource associated with a stream. The terminateStream tests focus on the contract: safe repeated calls, registry removal, channel-mapping
 * cleanup, preroll-timer cancellation, abort-controller signaling, segmenter.stop() invocation, FFmpeg.kill() invocation, and "terminated" event emission.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { deleteChannelStreamId, getChannelStreamId, isTerminationInitiated, setChannelStreamId, terminateStream } from "./lifecycle.ts";
import { getNextStreamId, getStream, makePendingCaptureIdentity, registerStream } from "./registry.ts";
import { makeNativeIdentity, makeRegistryEntry } from "./registry.helpers.ts";
import type { CaptureSession } from "./captureSession.ts";
import type { FFmpegProcess } from "../utils/index.ts";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.ts";
import type { NativeProxy } from "../native/proxy.ts";
import type { Nullable } from "../types/index.ts";
import type { Readable } from "node:stream";
import type { StreamRegistryEntry } from "./registry.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { createCaptureSession } from "./captureSession.ts";
import { registerAbortController } from "../utils/index.ts";
import { setGracefulShutdown } from "../browser/index.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* makeSegmenter returns a stub satisfying the FMP4SegmenterResult shape that lifecycle's terminateStream calls into. We track which methods were invoked so tests
 * can assert on the cleanup ordering.
 */
function makeSegmenter(): { calls: string[]; segmenter: FMP4SegmenterResult } {

  const calls: string[] = [];

  const segmenter = {

    getInitSegment: () => null,
    getInitVersion: () => 0,
    getKeyframeStats: () => ({


      averageKeyframeIntervalMs: 0,
      indeterminateCount: 0,
      keyframeCount: 0,
      maxKeyframeIntervalMs: 0,
      minKeyframeIntervalMs: 0,
      nonKeyframeCount: 0,
      segmentsWithoutLeadingKeyframe: 0
    }),
    getLastSegmentHasVideo: () => null,
    getLastSegmentSize: () => 0,
    getSegmentIndex: () => 0,
    getSessionStats: () => ({


      malformedMoofCount: 0,
      syncSpreadCount: 0,
      syncSpreadMaxMs: 0,
      syncSpreadMinMs: 0,
      syncSpreadSumMs: 0,
      tabReplacementCount: 0
    }),
    getTrackTimestamps: () => new Map(),
    markDiscontinuity: () => { calls.push("markDiscontinuity"); },
    pipe: () => { calls.push("pipe"); },
    stop: () => { calls.push("stop"); }
  } as unknown as FMP4SegmenterResult;

  return { calls, segmenter };
}

// Synthetic capture handles a lifecycle test can supply to makeCaptureSession; each is optional so a test provides only the handle whose teardown it asserts on.
interface CaptureSessionParts {

  ffmpeg?: { kill: () => void };
  rawStream?: { destroy: () => void; destroyed: boolean };
  segmenter?: FMP4SegmenterResult;
}

/* makeCaptureSession builds a real CaptureSession over synthetic capture handles so the lifecycle tests exercise the production teardown path: terminateStream
 * disposes the session, which kills the FFmpeg child, destroys the capture stream, and stops the segmenter in order. Omitted parts get inert defaults.
 */
function makeCaptureSession(parts: CaptureSessionParts = {}): CaptureSession {

  const rawCaptureStream = (parts.rawStream ?? { destroy: (): void => { /* inert default */ }, destroyed: false }) as unknown as Readable;
  const ffmpegProcess = (parts.ffmpeg ?? null) as unknown as Nullable<FFmpegProcess>;
  const session = createCaptureSession({ ffmpegProcess, rawCaptureStream });

  if(parts.segmenter) {

    session.attachSegmenter(parts.segmenter);
  }

  return session;
}

describe("getChannelStreamId / setChannelStreamId / deleteChannelStreamId", () => {

  test("set then get round-trips a channel-to-stream mapping", () => {

    setChannelStreamId("alpha", 101);

    assert.equal(getChannelStreamId("alpha"), 101);

    deleteChannelStreamId("alpha");
  });

  test("returns undefined for unknown channels", () => {

    assert.equal(getChannelStreamId("never-set"), undefined);
  });

  test("delete removes the mapping", () => {

    setChannelStreamId("beta", 202);
    deleteChannelStreamId("beta");

    assert.equal(getChannelStreamId("beta"), undefined);
  });

  test("delete is a no-op for an unknown channel (repeat-safe)", () => {

    assert.doesNotThrow(() => {

      deleteChannelStreamId("never-existed");
    });
  });

  test("set with the same channel name overwrites the previous mapping", () => {

    setChannelStreamId("gamma", 1);
    setChannelStreamId("gamma", 2);

    assert.equal(getChannelStreamId("gamma"), 2, "second set wins");

    deleteChannelStreamId("gamma");
  });
});

describe("isTerminationInitiated", () => {

  test("returns false for streams that have not been terminated", () => {

    assert.equal(isTerminationInitiated(999), false);
  });

  test("returns false after a terminateStream call completes (the guard set is cleaned up)", () => {

    // The flag is set when terminateStream begins and cleared when it finishes. Since terminateStream runs synchronously, observing the flag transition requires
    // either intercepting mid-run (we don't have a hook) or trusting the post-run cleared state. We verify the cleared state here.
    const entry = makeRegistryEntry();

    registerStream(entry);
    terminateStream(entry.id, entry.channelName ?? "", "test reason");

    assert.equal(isTerminationInitiated(entry.id), false, "flag cleared after termination completes");
  });
});

describe("terminateStream", () => {

  beforeEach(() => {

    // We disable graceful shutdown by default so the page-close branch executes if reached. Tests that need graceful shutdown enable it explicitly.
    setGracefulShutdown(false);
    mock.timers.enable({ apis: ["Date"], now: 1700000000000 });
  });

  afterEach(() => {

    setGracefulShutdown(false);
    mock.timers.reset();
  });

  test("removes the stream from the registry", () => {

    const entry = makeRegistryEntry();

    registerStream(entry);
    assert.notEqual(getStream(entry.id), undefined, "stream registered before termination");

    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.equal(getStream(entry.id), undefined, "stream gone from registry after terminateStream");
  });

  test("removes the channel-to-stream mapping when it points at this stream", () => {

    const entry = makeRegistryEntry({ channelName: "delta" });

    registerStream(entry);
    setChannelStreamId("delta", entry.id);

    terminateStream(entry.id, "delta", "test");

    assert.equal(getChannelStreamId("delta"), undefined, "channel mapping cleaned up");
  });

  test("does NOT remove the channel-to-stream mapping when it points at a different stream", () => {

    // Defensive: if a fresh stream has already replaced this one in the channel mapping, we must not delete that fresh stream's mapping. Locks the
    // channelToStreamId.get(channelName) === streamId check.
    const oldId = getNextStreamId();
    const oldEntry = makeRegistryEntry({ channelName: "epsilon", id: oldId });

    registerStream(oldEntry);

    // Imagine a new stream took over the mapping while old was being torn down.
    setChannelStreamId("epsilon", oldId + 1);

    terminateStream(oldId, "epsilon", "test");

    assert.equal(getChannelStreamId("epsilon"), oldId + 1, "newer stream's mapping preserved");

    // Cleanup
    deleteChannelStreamId("epsilon");
  });

  test("calling twice does not crash and the second call is a no-op", () => {

    // The terminationInitiated guard ensures double-termination is silently absorbed. Locks the contract that callers can issue redundant terminate calls without
    // worrying about stack overflows or double-stop calls into the segmenter.
    const { calls, segmenter } = makeSegmenter();
    const entry = makeRegistryEntry({ identity: { ...makePendingCaptureIdentity(), captureSession: makeCaptureSession({ segmenter }) } });

    registerStream(entry);

    terminateStream(entry.id, entry.channelName ?? "", "first");
    terminateStream(entry.id, entry.channelName ?? "", "second");

    assert.equal(calls.filter((c) => c === "stop").length, 1, "segmenter.stop called exactly once across two terminate calls");
  });

  test("cancels a pending preroll timer so it cannot fire after termination", () => {

    let timerFired = false;
    const entry = makeRegistryEntry();

    entry.hls.prerollTimer = setTimeout(() => { timerFired = true; }, 10);
    registerStream(entry);

    terminateStream(entry.id, entry.channelName ?? "", "test");

    // The timer handle on the entry must be cleared.
    assert.equal(entry.hls.prerollTimer, null);
    // The timer should not fire even if we waited 200ms; we do not wait here because clearTimeout is synchronous and reliable.
    assert.equal(timerFired, false);
  });

  test("aborts a registered AbortController for the stream", () => {

    const controller = new AbortController();
    const entry = makeRegistryEntry({ streamIdStr: "alpha-12345" });

    registerAbortController("alpha-12345", controller);
    registerStream(entry);

    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.equal(controller.signal.aborted, true, "abort controller signaled on termination");
  });

  test("destroys the raw capture stream when present", () => {

    // terminateStream disposes the capture session, which destroys the raw capture stream (its first teardown step for a no-FFmpeg session).
    let destroyed = false;
    const session = makeCaptureSession({ rawStream: { destroy: () => { destroyed = true; }, destroyed: false } });
    const entry = makeRegistryEntry({ identity: { ...makePendingCaptureIdentity(), captureSession: session } });

    registerStream(entry);
    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.equal(destroyed, true, "rawCaptureStream.destroy called");
  });

  test("does NOT call destroy when the raw capture stream is already destroyed", () => {

    // Negative test: avoid double-destroy. The session's destroyed guard prevents re-firing puppeteer-stream's close handler.
    let destroyed = false;
    const session = makeCaptureSession({ rawStream: { destroy: () => { destroyed = true; }, destroyed: true } });
    const entry = makeRegistryEntry({ identity: { ...makePendingCaptureIdentity(), captureSession: session } });

    registerStream(entry);
    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.equal(destroyed, false, "destroy not called on already-destroyed stream");
  });

  test("invokes ffmpegProcess.kill() and segmenter.stop() during cleanup", () => {

    // terminateStream disposes the capture session, which kills the FFmpeg child and stops the segmenter (the composite owns the kill-before-stop order; this test
    // confirms terminateStream routes the teardown through it so both run).
    const order: string[] = [];
    const { calls, segmenter } = makeSegmenter();
    const session = makeCaptureSession({ ffmpeg: { kill: () => { order.push("ffmpeg.kill"); } }, segmenter });
    const entry = makeRegistryEntry({ identity: { ...makePendingCaptureIdentity(), captureSession: session } });

    registerStream(entry);
    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.ok(order.includes("ffmpeg.kill"), "ffmpeg.kill was invoked");
    assert.ok(calls.includes("stop"), "segmenter.stop was invoked");
  });

  test("disposes the monitor handle when present", () => {

    // terminateStream reads the monitor's metrics in the prologue and disposes the handle in disposeStreamResources. This test asserts that the handle's dispose runs.
    let monitorDisposed = false;
    const dispose = (): void => { monitorDisposed = true; };

    const monitor: NonNullable<StreamRegistryEntry["monitor"]> = {

      dispose,
      getMetrics: () => ({

        currentRecoveryMethod: null,
        currentRecoveryStartTime: null,
        pageNavigationAttempts: 0,
        pageNavigationSuccesses: 0,
        playUnmuteAttempts: 0,
        playUnmuteSuccesses: 0,
        sourceReloadAttempts: 0,
        sourceReloadSuccesses: 0,
        tabReplacementAttempts: 0,
        tabReplacementSuccesses: 0,
        totalRecoveryTimeMs: 0
      }),
      [Symbol.dispose]: dispose
    };

    const entry = makeRegistryEntry({ monitor });

    registerStream(entry);
    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.equal(monitorDisposed, true, "monitor.dispose invoked on termination");
  });

  test("emits a 'terminated' event on the segmentEmitter and removes all listeners", () => {

    let terminatedCount = 0;
    const entry = makeRegistryEntry();

    entry.hls.segmentEmitter.on("terminated", () => terminatedCount++);
    registerStream(entry);

    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.equal(terminatedCount, 1, "terminated event fired once");
    assert.equal(entry.hls.segmentEmitter.listenerCount("terminated"), 0, "all listeners removed");
  });

  test("closes the page outside of graceful shutdown", () => {

    let closeCalled = false;
    const page = {


      close: async () => { closeCalled = true; },
      isClosed: () => false
    } as unknown as StreamRegistryEntry["page"];

    const entry = makeRegistryEntry({ page });

    registerStream(entry);
    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.equal(closeCalled, true, "page.close called when not in graceful shutdown");
  });

  test("skips closing the page during graceful shutdown (avoid 'Target closed' error)", () => {

    // Negative test: during shutdown closeBrowser() handles every page; lifecycle must not double-close.
    setGracefulShutdown(true);

    let closeCalled = false;
    const page = {


      close: async () => { closeCalled = true; },
      isClosed: () => false
    } as unknown as StreamRegistryEntry["page"];

    const entry = makeRegistryEntry({ page });

    registerStream(entry);
    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.equal(closeCalled, false, "page.close suppressed during graceful shutdown");
  });

  test("skips closing an already-closed page", () => {

    // Negative test: double-close on a closed page would throw "Target closed". The isClosed guard must prevent that.
    let closeCalled = false;
    const page = {


      close: async () => { closeCalled = true; },
      isClosed: () => true
    } as unknown as StreamRegistryEntry["page"];

    const entry = makeRegistryEntry({ page });

    registerStream(entry);
    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.equal(closeCalled, false, "close not called on already-closed page");
  });

  test("is safe to call for a stream that was never registered", () => {

    // Negative test: terminateStream must not throw when given an unknown ID. The implementation does best-effort cleanup of the channel mapping and registry,
    // both of which are no-ops in this case.
    assert.doesNotThrow(() => {

      terminateStream(999999, "ghost-channel", "test");
    });
  });

  test("stops the native proxy when present", () => {

    let stopped = false;

    const nativeProxy = {


      getStats: () => ({ fetchErrors: 0, segmentsFetched: 0, tokenRefreshes: 0 }),
      stop: () => { stopped = true; }
    } as unknown as NativeProxy;

    const entry = makeRegistryEntry({ identity: makeNativeIdentity({ nativeProxy }) });

    registerStream(entry);
    terminateStream(entry.id, entry.channelName ?? "", "test");

    assert.equal(stopped, true, "nativeProxy.stop invoked");
  });
});
