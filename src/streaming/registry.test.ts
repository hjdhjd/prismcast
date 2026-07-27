/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * registry.test.ts: Unit tests for the stream registry SSOT. registry.ts owns the in-memory state for every active streaming session: the streamRegistry Map keyed by
 * numeric stream ID, the monotonic streamIdCounter, and the createHLSState/getStreamMemoryUsage helpers used across capture, native, and lifecycle code paths. These
 * tests lock the registry's contract: register/unregister round-trips, ID monotonicity, getAllStreams snapshot independence, lookup with getStream, byte counter
 * arithmetic in getStreamMemoryUsage, and the shape of a freshly-minted HLSState.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { cancelPrerollTimer, createHLSState, getAllStreams, getLastSegmentHasVideo, getLastSegmentSize, getNextStreamId, getStream, getStreamCount,
  getStreamMemoryUsage, getTotalSegmentMemory, registerStream, unregisterStream, updateLastAccess } from "./registry.ts";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.ts";
import type { Readable } from "node:stream";
import type { StreamRegistryEntry } from "./registry.ts";
import assert from "node:assert/strict";
import { createCaptureSession } from "./captureSession.ts";
import { makeRegistryEntry } from "./registry.helpers.ts";

/* entryWithSegmenter builds a registry entry whose capture session exposes the given (partial) segmenter, so the getLastSegment* getters can be exercised through
 * the production read path (entry.captureSession?.segmenter?.getX()). A no-op pipe is supplied because attachSegmenter pipes the segmenter to the session's capture
 * output; the test doubles only implement the one getter under assertion.
 */
function entryWithSegmenter(segmenter: Record<string, unknown>): StreamRegistryEntry {

  const session = createCaptureSession({ ffmpegProcess: null, rawCaptureStream: { destroy: (): void => { /* inert */ }, destroyed: false } as unknown as Readable });

  session.attachSegmenter({ pipe: (): void => { /* inert */ }, ...segmenter } as unknown as FMP4SegmenterResult);

  return makeRegistryEntry({ captureSession: session });
}

/* clearRegistry removes any entries left behind by previous tests. The registry is module-scoped, so beforeEach in each describe must reset it to keep tests
 * independent. We iterate getAllStreams() and unregister each by id rather than reaching into private state, exercising the public API.
 */
function clearRegistry(): void {

  for(const entry of getAllStreams()) {

    unregisterStream(entry.id);
  }
}

describe("getNextStreamId", () => {

  test("returns strictly increasing values across sequential calls", () => {

    const a = getNextStreamId();
    const b = getNextStreamId();
    const c = getNextStreamId();

    assert.ok(b > a, "second ID must be greater than first");
    assert.ok(c > b, "third ID must be greater than second");
  });

  test("does not repeat IDs even across many invocations", () => {

    // Boundary: lock the no-collision contract by collecting 100 IDs and verifying their cardinality. The registry depends on this for its Map keying.
    const seen = new Set<number>();

    for(let i = 0; i < 100; i++) {

      seen.add(getNextStreamId());
    }

    assert.equal(seen.size, 100, "100 calls produced 100 distinct IDs");
  });
});

describe("registerStream / getStream", () => {

  beforeEach(() => {

    clearRegistry();
  });

  test("round-trips an entry through register and get", () => {

    const entry = makeRegistryEntry({ streamIdStr: "abc-1234" });

    registerStream(entry);

    const fetched = getStream(entry.id);

    assert.equal(fetched, entry, "getStream returns the same object reference");
    assert.equal(entry.streamIdStr, "abc-1234", "and it carries the registered fields");
  });

  test("returns undefined for an unknown stream ID", () => {

    // Negative test: looking up a never-registered ID must return undefined, not throw and not return a default.
    assert.equal(getStream(999_999), undefined, "unknown ID returns undefined");
  });

  test("registering with the same ID twice replaces the existing entry", () => {

    // The registry is a Map, so set() overwrites. Locks the last-write-wins contract.
    const id = getNextStreamId();
    const first = makeRegistryEntry({ id, streamIdStr: "first" });
    const second = makeRegistryEntry({ id, streamIdStr: "second" });

    registerStream(first);
    registerStream(second);

    assert.equal(getStream(id)?.streamIdStr, "second", "second register replaces first");
    assert.equal(getStreamCount(), 1, "still only one entry in the registry");
  });
});

describe("unregisterStream", () => {

  beforeEach(() => {

    clearRegistry();
  });

  test("removes an entry so subsequent getStream returns undefined", () => {

    const entry = makeRegistryEntry();

    registerStream(entry);
    assert.equal(getStream(entry.id), entry, "registered before unregister");

    unregisterStream(entry.id);
    assert.equal(getStream(entry.id), undefined, "gone after unregister");
  });

  test("is a no-op when called with an unknown ID", () => {

    // Negative test: the registry must tolerate stale unregister calls (e.g., double cleanup paths) without throwing.
    assert.doesNotThrow(() => {

      unregisterStream(123_456_789);
    });
  });

  test("does not affect other registered entries", () => {

    const a = makeRegistryEntry({ streamIdStr: "a" });
    const b = makeRegistryEntry({ streamIdStr: "b" });

    registerStream(a);
    registerStream(b);

    unregisterStream(a.id);

    assert.equal(getStream(a.id), undefined, "a is gone");
    assert.equal(getStream(b.id), b, "b is still present");
    assert.equal(getStreamCount(), 1);
  });
});

describe("getAllStreams", () => {

  beforeEach(() => {

    clearRegistry();
  });

  test("returns an empty array when the registry is empty", () => {

    assert.deepEqual(getAllStreams(), []);
  });

  test("returns every registered entry", () => {

    const a = makeRegistryEntry();
    const b = makeRegistryEntry();
    const c = makeRegistryEntry();

    registerStream(a);
    registerStream(b);
    registerStream(c);

    const all = getAllStreams();

    assert.equal(all.length, 3);
    assert.ok(all.includes(a));
    assert.ok(all.includes(b));
    assert.ok(all.includes(c));
  });

  test("returns a snapshot - mutating the result does not modify the registry", () => {

    // The registry uses Array.from(map.values()) which produces a fresh array. Locks the snapshot contract so tests and callers don't accidentally mutate registry
    // state via the returned array.
    const entry = makeRegistryEntry();

    registerStream(entry);

    const snapshot = getAllStreams();

    snapshot.length = 0;

    assert.equal(getStreamCount(), 1, "registry unchanged by mutating the snapshot array");
    assert.equal(getStream(entry.id), entry, "entry still retrievable via getStream");
  });
});

describe("getStreamCount", () => {

  beforeEach(() => {

    clearRegistry();
  });

  test("returns 0 for an empty registry", () => {

    assert.equal(getStreamCount(), 0);
  });

  test("reflects each register and unregister", () => {

    const a = makeRegistryEntry();
    const b = makeRegistryEntry();

    registerStream(a);
    assert.equal(getStreamCount(), 1);

    registerStream(b);
    assert.equal(getStreamCount(), 2);

    unregisterStream(a.id);
    assert.equal(getStreamCount(), 1);

    unregisterStream(b.id);
    assert.equal(getStreamCount(), 0);
  });
});

describe("updateLastAccess", () => {

  beforeEach(() => {

    clearRegistry();
    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("sets lastPlaylistRequest to Date.now() for a known stream", () => {

    const entry = makeRegistryEntry();

    entry.info.lastPlaylistRequest = 0;

    registerStream(entry);
    updateLastAccess(entry.id);

    assert.equal(entry.info.lastPlaylistRequest, 1_700_000_000_000, "stamped to current Date.now()");
  });

  test("is a no-op for an unknown stream", () => {

    // Negative test: calling updateLastAccess for an unregistered ID must not throw. This protects the idle-detection path from race conditions where a stream
    // terminates between client poll and registry lookup.
    assert.doesNotThrow(() => {

      updateLastAccess(999);
    });
  });
});

describe("createHLSState", () => {

  test("returns an HLSState with empty segment storage and zero counters", () => {

    const state = createHLSState();

    assert.equal(state.audioPlaylist, "");
    assert.equal(state.audioSegmentBytes, 0);
    assert.equal(state.audioSegments.size, 0);
    assert.equal(state.hasAudio, false);
    assert.equal(state.hasRealPlaylist, false);
    assert.equal(state.initSegment, null);
    assert.equal(state.playlist, "");
    assert.equal(state.prerollBaseUrl, null);
    assert.equal(state.prerollCodec, null);
    assert.equal(state.prerollSegmentCount, 0);
    assert.equal(state.prerollStartTime, null);
    assert.equal(state.prerollTimer, null);
    assert.equal(state.resumeSegmentIndex, 0);
    assert.equal(state.segmentBytes, 0);
    assert.equal(state.segments.size, 0);
    assert.equal(state.videoPlaylist, "");
  });

  test("wires the initSegmentReady promise to its signal function", async () => {

    // Validates the Promise.withResolvers contract - calling signal resolves the promise. Used by MPEG-TS consumers to wait for codec configuration.
    const state = createHLSState();

    let resolved = false;

    void state.initSegmentReady.then(() => { resolved = true; });

    assert.equal(resolved, false, "promise unresolved before signal");

    state.signalInitSegmentReady();

    // Yield a microtask so .then can run.
    await Promise.resolve();

    assert.equal(resolved, true, "promise resolved after signal");
  });

  test("wires the playlistReady promise to its signal function", async () => {

    const state = createHLSState();

    let resolved = false;

    void state.playlistReady.then(() => { resolved = true; });

    state.signalPlaylistReady();

    await Promise.resolve();

    assert.equal(resolved, true, "playlistReady promise resolves after signal");
  });

  test("returns a new EventEmitter on each call (no shared emitter)", () => {

    // Two HLSStates must not share their segment emitter. Locks the per-stream isolation contract so MPEG-TS clients on stream A don't receive events from stream B.
    const a = createHLSState();
    const b = createHLSState();

    assert.notEqual(a.segmentEmitter, b.segmentEmitter, "fresh emitter per state");
  });

  test("the segment emitter has a max listener limit of 20", () => {

    // Locks the multi-MPEG-TS-client capacity. Default Node EventEmitter caps at 10 - if the implementation accidentally drops setMaxListeners, this test surfaces it.
    const state = createHLSState();

    assert.equal(state.segmentEmitter.getMaxListeners(), 20);
  });
});

describe("cancelPrerollTimer", () => {

  test("disarms an armed timer so its callback never runs and nulls the handle", async () => {

    // Arm a real short timer that flips a flag, cancel it, then wait well past the original delay. A helper that failed to clear the timer would let the callback run
    // and flip the flag - this assertion fails against a broken no-op helper, which is what makes it discriminating rather than merely asserting the handle is null.
    const state = createHLSState();

    let fired = false;

    state.prerollTimer = setTimeout(() => {

      fired = true;
    }, 20);

    cancelPrerollTimer(state);

    await new Promise((resolve) => setTimeout(resolve, 60));

    assert.equal(fired, false, "the cancelled timer's callback never ran");
    assert.equal(state.prerollTimer, null, "the timer handle is nulled after cancellation");
  });

  test("is a no-op when no timer is armed and leaves the handle null", () => {

    // A fresh state has a null handle; cancelling must neither throw nor invent a handle.
    const state = createHLSState();

    assert.equal(state.prerollTimer, null, "fresh state has no armed timer");

    assert.doesNotThrow(() => {

      cancelPrerollTimer(state);
    });

    assert.equal(state.prerollTimer, null, "the handle stays null after a no-op cancel");
  });
});

describe("getStreamMemoryUsage", () => {

  test("returns zero for a fresh entry with no segments", () => {

    const entry = makeRegistryEntry();
    const usage = getStreamMemoryUsage(entry);

    assert.equal(usage.initSegment, 0);
    assert.equal(usage.segments, 0);
    assert.equal(usage.total, 0);
  });

  test("reads init segment size from the buffer length", () => {

    const entry = makeRegistryEntry();

    entry.hls.initSegment = Buffer.alloc(1234);

    const usage = getStreamMemoryUsage(entry);

    assert.equal(usage.initSegment, 1234);
    assert.equal(usage.segments, 0);
    assert.equal(usage.total, 1234);
  });

  test("reads segment bytes from the running counters (O(1) read)", () => {

    // The implementation deliberately uses entry.hls.segmentBytes / audioSegmentBytes counters maintained by storeSegmentToMap, not iteration over the segment Map.
    // We verify that getStreamMemoryUsage trusts the counters - if the contract changes to iterate, this test catches it.
    const entry = makeRegistryEntry();

    entry.hls.segmentBytes = 500_000;
    entry.hls.audioSegmentBytes = 100_000;

    const usage = getStreamMemoryUsage(entry);

    assert.equal(usage.segments, 600_000, "video + audio counters summed");
    assert.equal(usage.total, 600_000, "no init -> total equals segments");
  });

  test("totals init plus video plus audio segment bytes", () => {

    const entry = makeRegistryEntry();

    entry.hls.initSegment = Buffer.alloc(1_000);
    entry.hls.segmentBytes = 50_000;
    entry.hls.audioSegmentBytes = 20_000;

    const usage = getStreamMemoryUsage(entry);

    assert.equal(usage.initSegment, 1_000);
    assert.equal(usage.segments, 70_000);
    assert.equal(usage.total, 71_000, "init (1000) + video (50000) + audio (20000)");
  });
});

describe("getTotalSegmentMemory", () => {

  beforeEach(() => {

    clearRegistry();
  });

  test("returns 0 when no streams are registered", () => {

    assert.equal(getTotalSegmentMemory(), 0);
  });

  test("sums memory across every registered stream", () => {

    const a = makeRegistryEntry();
    const b = makeRegistryEntry();

    a.hls.initSegment = Buffer.alloc(1_000);
    a.hls.segmentBytes = 10_000;

    b.hls.segmentBytes = 30_000;
    b.hls.audioSegmentBytes = 5_000;

    registerStream(a);
    registerStream(b);

    assert.equal(getTotalSegmentMemory(), 1_000 + 10_000 + 30_000 + 5_000, "sums every entry's total");
  });
});

describe("getLastSegmentHasVideo", () => {

  test("returns null when no capture session exists on the entry", () => {

    // Boundary: pending stream entries (and native-mode entries) have captureSession === null. The getter must not crash on that path.
    const entry = makeRegistryEntry({ captureSession: null });

    assert.equal(getLastSegmentHasVideo(entry), null);
  });

  test("returns the value reported by the segmenter when present", () => {

    const entry = entryWithSegmenter({ getLastSegmentHasVideo: (): boolean => true });

    assert.equal(getLastSegmentHasVideo(entry), true);
  });

  test("forwards a null result from the segmenter", () => {

    // Segmenters return null when the video trackId is unknown - locks the pass-through contract.
    const entry = entryWithSegmenter({ getLastSegmentHasVideo: (): null => null });

    assert.equal(getLastSegmentHasVideo(entry), null);
  });
});

describe("getLastSegmentSize", () => {

  test("returns null when no capture session exists on the entry", () => {

    const entry = makeRegistryEntry({ captureSession: null });

    assert.equal(getLastSegmentSize(entry), null);
  });

  test("returns the size reported by the segmenter", () => {

    const entry = entryWithSegmenter({ getLastSegmentSize: (): number => 12_345 });

    assert.equal(getLastSegmentSize(entry), 12_345);
  });

  test("returns null when the segmenter returns undefined / nullish (??)", () => {

    // The getter uses ?? null to coerce an absent return into null. This locks the coercion path.
    const entry = entryWithSegmenter({ getLastSegmentSize: (): undefined => undefined });

    assert.equal(getLastSegmentSize(entry), null);
  });
});
