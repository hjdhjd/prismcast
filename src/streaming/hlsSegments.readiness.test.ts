/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsSegments.readiness.test.ts: Unit tests for the playlist + init-segment readiness signals exposed by hlsSegments.ts - waitForPlaylist and waitForInitSegment.
 * Storage primitives (storeSegment, getSegment, audio/playlist/init variants) live in hlsSegments.test.ts.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { registerStream, unregisterStream } from "./registry.ts";
import { storeInitSegment, updatePlaylist, waitForInitSegment, waitForPlaylist } from "./hlsSegments.ts";
import assert from "node:assert/strict";
import { makeFakeClock } from "../utils/clock.helpers.ts";
import { makeRegistryEntry } from "./registry.helpers.ts";

/* makeAndRegisterStream wraps the canonical makeRegistryEntry factory with registerStream so the test setup pattern is one line.
 */
function makeAndRegisterStream(): { streamId: number } {

  const entry = makeRegistryEntry();

  registerStream(entry);

  return { streamId: entry.id };
}

describe("waitForPlaylist", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("returns true when the playlist becomes ready before the timeout", async () => {

    // Resolve the playlist before the wait so the inner promise wins the race. With the fake clock's pass-through raceWithTimeout, the result is driven by the
    // inner promise's resolution; no real timer is involved and no scheduling order matters.
    updatePlaylist(streamId, "#EXTM3U");

    const { clock } = makeFakeClock();
    const ready = await waitForPlaylist(streamId, 1_000, clock);

    assert.equal(ready, true);
  });

  test("returns false when the timeout fires before any playlist arrives", async () => {

    // The fake clock's raceWithTimeout rejects immediately to simulate the timer winning. waitForReady's .catch maps the rejection to false. Locks the contract
    // without depending on real-time delay budgets.
    const { clock } = makeFakeClock({

      raceWithTimeout: async (_promise, timeoutMs) => {

        throw new Error("timed out after " + String(timeoutMs) + "ms.");
      }
    });

    const ready = await waitForPlaylist(streamId, 5, clock);

    assert.equal(ready, false);
  });

  test("returns false for an unknown stream", async () => {

    // The early-return path never reaches the clock, so default-arg is fine; locks the early-return contract independently of clock injection.
    assert.equal(await waitForPlaylist(999_999, 5), false);
  });
});

describe("waitForInitSegment", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("returns true when the init segment becomes ready before the timeout", async () => {

    // Resolve the init segment before the wait so the inner promise wins the race; the fake clock's pass-through raceWithTimeout forwards the resolved value.
    storeInitSegment(streamId, Buffer.from("init"));

    const { clock } = makeFakeClock();
    const ready = await waitForInitSegment(streamId, 1_000, clock);

    assert.equal(ready, true);
  });

  test("returns false when the timeout fires before any init segment arrives", async () => {

    const { clock } = makeFakeClock({

      raceWithTimeout: async (_promise, timeoutMs) => {

        throw new Error("timed out after " + String(timeoutMs) + "ms.");
      }
    });

    const ready = await waitForInitSegment(streamId, 5, clock);

    assert.equal(ready, false);
  });

  test("returns false for an unknown stream", async () => {

    assert.equal(await waitForInitSegment(999_999, 5), false);
  });
});

describe("readiness waits resolve on stream termination", () => {

  test("waitForPlaylist resolves false promptly when the stream terminates, without resolving playlistReady", async () => {

    const entry = makeRegistryEntry();

    registerStream(entry);

    // Probe the readiness promise itself: termination must settle the WAIT via the third race arm without resolving playlistReady, whose contract stays pure. We
    // attach the probe before the emit and flush microtasks after, so an implementation that resolved the promise on termination would flip the flag and be caught.
    let readinessSettled = false;

    void entry.hls.playlistReady.then(() => {

      readinessSettled = true;
    });

    // The default fake clock forwards the inner promise and never fires a timeout, so only the readiness or terminated arm can settle the race.
    const { clock } = makeFakeClock();
    const pending = waitForPlaylist(entry.id, 30000, clock);

    // Emit terminated directly on the entry's emitter. The real terminateStream additionally strips every listener, which would mask the hygiene assertions below, so
    // these tests drive the emitter directly to isolate the race behavior.
    entry.hls.segmentEmitter.emit("terminated");

    const ready = await pending;

    await Promise.resolve();

    assert.equal(ready, false, "the terminated arm resolves the wait false");
    assert.equal(readinessSettled, false, "playlistReady itself is never resolved by termination");

    unregisterStream(entry.id);
  });

  test("waitForInitSegment resolves false promptly when the stream terminates, without resolving initSegmentReady", async () => {

    const entry = makeRegistryEntry();

    registerStream(entry);

    let readinessSettled = false;

    void entry.hls.initSegmentReady.then(() => {

      readinessSettled = true;
    });

    const { clock } = makeFakeClock();
    const pending = waitForInitSegment(entry.id, 30000, clock);

    entry.hls.segmentEmitter.emit("terminated");

    const ready = await pending;

    await Promise.resolve();

    assert.equal(ready, false, "the terminated arm resolves the wait false");
    assert.equal(readinessSettled, false, "initSegmentReady itself is never resolved by termination");

    unregisterStream(entry.id);
  });

  test("removes the terminated listener after both the terminated-wins and readiness-wins outcomes", async () => {

    // Terminated-wins: baseline is zero, the wait attaches one, and settling via the terminated arm returns it to zero.
    const terminatedEntry = makeRegistryEntry();

    registerStream(terminatedEntry);

    assert.equal(terminatedEntry.hls.segmentEmitter.listenerCount("terminated"), 0, "no terminated listeners before the wait");

    const { clock: terminatedClock } = makeFakeClock();
    const terminatedPending = waitForPlaylist(terminatedEntry.id, 30000, terminatedClock);

    assert.equal(terminatedEntry.hls.segmentEmitter.listenerCount("terminated"), 1, "the wait attaches exactly one terminated listener");

    terminatedEntry.hls.segmentEmitter.emit("terminated");

    await terminatedPending;

    assert.equal(terminatedEntry.hls.segmentEmitter.listenerCount("terminated"), 0, "the terminated listener is removed after the terminated arm wins");

    unregisterStream(terminatedEntry.id);

    // Readiness-wins: the same lifecycle, settled by the playlist becoming ready instead of by termination.
    const readyEntry = makeRegistryEntry();

    registerStream(readyEntry);

    const { clock: readyClock } = makeFakeClock();
    const readyPending = waitForPlaylist(readyEntry.id, 30000, readyClock);

    assert.equal(readyEntry.hls.segmentEmitter.listenerCount("terminated"), 1, "the wait attaches exactly one terminated listener");

    updatePlaylist(readyEntry.id, "#EXTM3U");

    await readyPending;

    assert.equal(readyEntry.hls.segmentEmitter.listenerCount("terminated"), 0, "the terminated listener is removed after the readiness arm wins");

    unregisterStream(readyEntry.id);
  });
});
