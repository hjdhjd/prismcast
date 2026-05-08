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

    // The fake clock's raceWithTimeout throws synchronously to simulate the timer winning. waitForReady's .catch maps the throw to false. Locks the contract
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
