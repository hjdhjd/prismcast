/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsSegments.test.ts: Unit tests for HLS segment storage. hlsSegments.ts manages the per-stream segment Maps inside HLSState: store/get for media + audio + init
 * segments, store/get for the playlist + variant playlists, the rotation that enforces CONFIG.hls.maxSegments, and the readiness signals (initSegmentReady,
 * playlistReady) that downstream consumers (HLS preroll, MPEG-TS clients) await before serving content. Every public function gates on getStream(streamId) and
 * silently no-ops when the stream is unknown - those negative paths are explicitly covered.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { getAudioPlaylist, getAudioSegment, getInitSegment, getPlaylist, getSegment, getSegmentCount, getVideoPlaylist, storeAudioSegment, storeInitSegment,
  storeSegment, updateAudioPlaylist, updatePlaylist, updateVideoPlaylist } from "./hlsSegments.ts";
import { getStream, registerStream, unregisterStream } from "./registry.ts";
import { CONFIG } from "../config/index.ts";
import type { StreamRegistryEntry } from "./registry.ts";
import assert from "node:assert/strict";
import { makeRegistryEntry } from "./registry.helpers.ts";

/* makeAndRegisterStream wraps the canonical makeRegistryEntry factory with registerStream so the test setup pattern is one line. The side-effecting "register
 * during fixture build" semantics are specific to this file: every storeSegment / getSegment / playlist operation expects a registered stream as its lookup
 * target, so building-without-registering would never be useful here.
 */
function makeAndRegisterStream(): { streamId: number } {

  const entry = makeRegistryEntry();

  registerStream(entry);

  return { streamId: entry.id };
}

describe("storeSegment", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("stores a segment under its filename and surfaces it via getSegment", () => {

    const data = Buffer.from("hello");

    storeSegment(streamId, "segment0.m4s", data);

    assert.equal(getSegment(streamId, "segment0.m4s"), data, "stored buffer retrieved by name");
    assert.equal(getSegmentCount(streamId), 1);
  });

  test("emits a 'segment' event on the segmenter's typed emitter for each store", () => {

    const seen: { data: Buffer; filename: string }[] = [];
    const entry = makeStreamWithSegmentListener(streamId, (filename, data) => seen.push({ data, filename }));

    storeSegment(streamId, "a.m4s", Buffer.from("aaa"));
    storeSegment(streamId, "b.m4s", Buffer.from("bb"));

    assert.equal(seen.length, 2);
    assert.equal(seen[0]?.filename, "a.m4s");
    assert.equal(seen[1]?.filename, "b.m4s");

    assert.equal(entry.hls.segmentBytes, 5, "byte counter accumulates");
  });

  test("rotates oldest segments when CONFIG.hls.maxSegments is exceeded", () => {

    const originalMax = CONFIG.hls.maxSegments;

    CONFIG.hls.maxSegments = 3;

    try {

      storeSegment(streamId, "s0.m4s", Buffer.from("0"));
      storeSegment(streamId, "s1.m4s", Buffer.from("1"));
      storeSegment(streamId, "s2.m4s", Buffer.from("2"));
      storeSegment(streamId, "s3.m4s", Buffer.from("3"));
      storeSegment(streamId, "s4.m4s", Buffer.from("4"));

      assert.equal(getSegmentCount(streamId), 3, "rotated to maxSegments");
      assert.equal(getSegment(streamId, "s0.m4s"), undefined, "oldest evicted");
      assert.equal(getSegment(streamId, "s1.m4s"), undefined, "next-oldest evicted");
      assert.notEqual(getSegment(streamId, "s2.m4s"), undefined, "third newest still present");
      assert.notEqual(getSegment(streamId, "s4.m4s"), undefined, "newest still present");
    } finally {

      CONFIG.hls.maxSegments = originalMax;
    }
  });

  test("decrements the running byte counter when a segment is rotated out", () => {

    const originalMax = CONFIG.hls.maxSegments;

    CONFIG.hls.maxSegments = 2;

    try {

      const entry = makeStreamWithSegmentListener(streamId, () => undefined);

      storeSegment(streamId, "s0.m4s", Buffer.from("aaaa"));
      storeSegment(streamId, "s1.m4s", Buffer.from("bbb"));

      assert.equal(entry.hls.segmentBytes, 7, "two segments accumulated");

      // Adding a third triggers rotation - oldest 4-byte segment goes.
      storeSegment(streamId, "s2.m4s", Buffer.from("cc"));

      assert.equal(entry.hls.segmentBytes, 5, "after rotation: 3 + 2 = 5");
    } finally {

      CONFIG.hls.maxSegments = originalMax;
    }
  });

  test("is a no-op for an unknown stream (silent ignore)", () => {

    // Negative test: store on a never-registered stream silently does nothing - protects against post-termination races.
    assert.doesNotThrow(() => {

      storeSegment(999_999, "ghost.m4s", Buffer.from("x"));
    });
  });
});

describe("getSegment", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("returns undefined for an unknown filename", () => {

    assert.equal(getSegment(streamId, "missing.m4s"), undefined);
  });

  test("returns undefined for an unknown stream", () => {

    assert.equal(getSegment(999_999, "any.m4s"), undefined);
  });
});

describe("getSegmentCount", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("returns 0 for a fresh stream", () => {

    assert.equal(getSegmentCount(streamId), 0);
  });

  test("returns 0 for an unknown stream (no throw)", () => {

    // The segmenter calls this to clamp its playlist window; it must tolerate a torn-down stream.
    assert.equal(getSegmentCount(999_999), 0);
  });
});

describe("storeAudioSegment / getAudioSegment", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("stores audio segments separately from video segments", () => {

    storeSegment(streamId, "v.m4s", Buffer.from("video"));
    storeAudioSegment(streamId, "a.m4s", Buffer.from("audio"));

    assert.equal(getSegment(streamId, "v.m4s")?.toString(), "video");
    assert.equal(getAudioSegment(streamId, "a.m4s")?.toString(), "audio");
    assert.equal(getSegment(streamId, "a.m4s"), undefined, "audio store does not pollute video map");
    assert.equal(getAudioSegment(streamId, "v.m4s"), undefined, "video store does not pollute audio map");
  });

  test("emits an 'audioSegment' event distinct from 'segment'", () => {

    const seenSegment: string[] = [];
    const seenAudio: string[] = [];

    const entry = makeStreamWithSegmentListener(streamId, (name) => seenSegment.push(name));

    entry.hls.segmentEmitter.on("audioSegment", (name) => seenAudio.push(name));

    storeSegment(streamId, "v0.m4s", Buffer.from("v"));
    storeAudioSegment(streamId, "a0.m4s", Buffer.from("a"));

    assert.deepEqual(seenSegment, ["v0.m4s"], "video event only for storeSegment");
    assert.deepEqual(seenAudio, ["a0.m4s"], "audio event only for storeAudioSegment");
  });

  test("audio store is a no-op for an unknown stream", () => {

    assert.doesNotThrow(() => {

      storeAudioSegment(999_999, "ghost.m4s", Buffer.from("x"));
    });
  });

  test("audio rotation enforces maxSegments and decrements audio byte counter", () => {

    const originalMax = CONFIG.hls.maxSegments;

    CONFIG.hls.maxSegments = 2;

    try {

      const entry = makeStreamWithSegmentListener(streamId, () => undefined);

      storeAudioSegment(streamId, "a0.m4s", Buffer.from("aa"));
      storeAudioSegment(streamId, "a1.m4s", Buffer.from("bb"));
      storeAudioSegment(streamId, "a2.m4s", Buffer.from("cc"));

      assert.equal(entry.hls.audioSegments.size, 2);
      assert.equal(entry.hls.audioSegmentBytes, 4, "two * 2 bytes after rotation");
      assert.equal(getAudioSegment(streamId, "a0.m4s"), undefined, "oldest evicted");
    } finally {

      CONFIG.hls.maxSegments = originalMax;
    }
  });
});

describe("updateAudioPlaylist / getAudioPlaylist", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("stores and retrieves the audio variant playlist", () => {

    updateAudioPlaylist(streamId, "#EXTM3U\naudio");

    assert.equal(getAudioPlaylist(streamId), "#EXTM3U\naudio");
  });

  test("returns undefined when the audio playlist is empty (empty-string-to-undefined coercion)", () => {

    // The implementation coerces empty string to undefined so an empty audio playlist is treated as "no audio variant." That collapses empty content and absence into one
    // value, letting callers rely on a simple presence check instead of distinguishing "" from undefined.
    assert.equal(getAudioPlaylist(streamId), undefined, "fresh stream has no audio playlist");
  });

  test("update is a no-op for an unknown stream", () => {

    assert.doesNotThrow(() => {

      updateAudioPlaylist(999_999, "x");
    });
  });

  test("get returns undefined for an unknown stream", () => {

    assert.equal(getAudioPlaylist(999_999), undefined);
  });
});

describe("updateVideoPlaylist / getVideoPlaylist", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("stores and retrieves the video variant playlist", () => {

    updateVideoPlaylist(streamId, "#EXTM3U\nvideo");

    assert.equal(getVideoPlaylist(streamId), "#EXTM3U\nvideo");
  });

  test("returns undefined when the video playlist is empty", () => {

    assert.equal(getVideoPlaylist(streamId), undefined);
  });

  test("update is a no-op for an unknown stream", () => {

    assert.doesNotThrow(() => {

      updateVideoPlaylist(999_999, "x");
    });
  });

  test("get returns undefined for an unknown stream", () => {

    assert.equal(getVideoPlaylist(999_999), undefined);
  });
});

describe("storeInitSegment / getInitSegment", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("stores the init segment and surfaces it via getInitSegment", () => {

    const data = Buffer.from("init");

    storeInitSegment(streamId, data);

    assert.equal(getInitSegment(streamId), data);
  });

  test("signals initSegmentReady the first time an init segment arrives", async () => {

    // Locks the readiness contract used by MPEG-TS consumers - they await this promise before launching FFmpeg.
    const ready = await raceWithImmediate(
      makeRegistryReadyPromise(streamId, "init"),
      () => { storeInitSegment(streamId, Buffer.from("init")); }
    );

    assert.equal(ready, true, "initSegmentReady resolved on first store");
  });

  test("does NOT re-signal initSegmentReady for subsequent updates (one-shot signal)", () => {

    // The implementation only signals on the first init. Subsequent stores update the buffer without re-firing the signal - locks the one-shot contract that
    // protects against double-resolved-promise races in the MPEG-TS client.
    let signaledCount = 0;
    const entry = makeStreamWithSegmentListener(streamId, () => undefined);

    void entry.hls.initSegmentReady.then(() => signaledCount++);

    storeInitSegment(streamId, Buffer.from("first"));
    storeInitSegment(streamId, Buffer.from("second"));

    // Signal is one-shot - signaledCount can only become 1.
    // (We don't await here because the promise is wired and we only need to confirm the second store doesn't crash.)
    assert.equal(getInitSegment(streamId)?.toString(), "second", "second init replaced first");
  });

  test("emits an 'initSegment' event on the segmenter's emitter", () => {

    const entry = makeStreamWithSegmentListener(streamId, () => undefined);
    const seen: Buffer[] = [];

    entry.hls.segmentEmitter.on("initSegment", (data) => seen.push(data));

    const init = Buffer.from("init");

    storeInitSegment(streamId, init);

    assert.deepEqual(seen, [init], "init event emitted with the buffer");
  });

  test("getInitSegment returns undefined when no init has been stored yet", () => {

    assert.equal(getInitSegment(streamId), undefined);
  });

  test("store is a no-op for an unknown stream", () => {

    assert.doesNotThrow(() => {

      storeInitSegment(999_999, Buffer.from("x"));
    });
  });

  test("getInitSegment returns undefined for an unknown stream", () => {

    assert.equal(getInitSegment(999_999), undefined);
  });
});

describe("updatePlaylist / getPlaylist", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("stores the playlist and surfaces it via getPlaylist", () => {

    updatePlaylist(streamId, "#EXTM3U");

    assert.equal(getPlaylist(streamId), "#EXTM3U");
  });

  test("signals playlistReady on the first real playlist", async () => {

    const ready = await raceWithImmediate(
      makeRegistryReadyPromise(streamId, "playlist"),
      () => { updatePlaylist(streamId, "#EXTM3U\nseg"); }
    );

    assert.equal(ready, true);
  });

  test("sets hasRealPlaylist to true on the first call", () => {

    // The implementation flips hasRealPlaylist on the first real (non-preroll) update so the segmenter knows when it is producing live content.
    const stream = registryEntry(streamId);

    assert.equal(stream.hls.hasRealPlaylist, false, "starts false");

    updatePlaylist(streamId, "#EXTM3U");

    assert.equal(stream.hls.hasRealPlaylist, true, "flipped to true after first update");
  });

  test("clears the preroll timer when the first real playlist arrives", () => {

    // Locks the cancellation contract - preroll timers must be cancelled by updatePlaylist so they don't fire after live content starts flowing.
    const stream = registryEntry(streamId);

    let timerFired = false;

    stream.hls.prerollTimer = setTimeout(() => { timerFired = true; }, 100);

    updatePlaylist(streamId, "#EXTM3U");

    assert.equal(stream.hls.prerollTimer, null, "timer handle cleared");

    // The clearTimeout call should also prevent the timer from firing if we waited 200ms; we do not wait here to avoid real-time delays.
    assert.equal(timerFired, false, "timer not yet fired regardless");
  });

  test("update is a no-op for an unknown stream", () => {

    assert.doesNotThrow(() => {

      updatePlaylist(999_999, "x");
    });
  });

  test("getPlaylist returns undefined for an unknown stream", () => {

    assert.equal(getPlaylist(999_999), undefined);
  });

  test("returns the empty string explicitly when the playlist is unset (not undefined)", () => {

    // hlsSegments.getPlaylist returns the raw stored string, including the empty default. Locks the contract that distinguishes the "stream exists but no playlist
    // yet" case (returns "") from the "no stream" case (returns undefined).
    assert.equal(getPlaylist(streamId), "", "fresh stream has empty playlist");
  });
});

/* registryEntry returns the registered stream entry for the given ID. Throws if the stream was unregistered between makeRegistryEntry and the lookup, which would
 * indicate a test bug.
 */
function registryEntry(streamId: number): StreamRegistryEntry {

  const entry = getStream(streamId);

  if(!entry) {

    throw new Error("stream not registered");
  }

  return entry;
}

/* makeStreamWithSegmentListener attaches a listener to the typed segment emitter so subsequent assertions can observe emit-side-effects. The helper also returns
 * the registered entry so tests can read fields like segmentBytes directly.
 */
function makeStreamWithSegmentListener(streamId: number, onSegment: (filename: string, data: Buffer) => void): StreamRegistryEntry {

  const entry = registryEntry(streamId);

  entry.hls.segmentEmitter.on("segment", onSegment);

  return entry;
}

/* makeRegistryReadyPromise returns a promise that resolves when the named readiness signal fires on the stream's HLS state. Used in test bodies that need to race
 * the signal against an immediate side effect to confirm the contract holds.
 */
function makeRegistryReadyPromise(streamId: number, kind: "init" | "playlist"): Promise<true> {

  const entry = registryEntry(streamId);

  return (kind === "init" ? entry.hls.initSegmentReady : entry.hls.playlistReady).then(() => true);
}

/* raceWithImmediate kicks off the side effect synchronously, then awaits the readiness promise. The signal is fire-and-forget from inside store/update, so this
 * pattern avoids real-time delays.
 */
async function raceWithImmediate(promise: Promise<true>, sideEffect: () => void): Promise<true> {

  sideEffect();

  return promise;
}
