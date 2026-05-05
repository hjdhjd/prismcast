/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clock.helpers.test.ts: Unit tests for the makeFakeClock test factory. Locks the documented contract of the test-side counterpart to realClock so consumers
 * (retry.test.ts today, future modules tomorrow) can rely on the factory's behavior without having to re-derive it from the source.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { makeFakeClock } from "./clock.helpers.ts";

describe("makeFakeClock", () => {

  test("returns a handle exposing the clock and a sleeps array", () => {

    // Locks the structural shape of the handle. A future change that drops `sleeps` or renames it fails here before any consumer breaks.
    const handle = makeFakeClock();

    assert.equal(typeof handle.clock, "object");
    assert.equal(typeof handle.clock.now, "function");
    assert.equal(typeof handle.clock.raceWithTimeout, "function");
    assert.equal(typeof handle.clock.sleep, "function");
    assert.ok(Array.isArray(handle.sleeps));
    assert.equal(handle.sleeps.length, 0, "sleeps starts empty");
  });

  test("default sleep records the requested duration and resolves", async () => {

    const { clock, sleeps } = makeFakeClock();

    await clock.sleep(150);

    assert.deepEqual(sleeps, [150]);
  });

  test("default sleep records multiple invocations in call order", async () => {

    // Locks the schedule-recording contract that retry's backoff tests rely on (assert.deepEqual on sleeps verifies both count and ordered values).
    const { clock, sleeps } = makeFakeClock();

    await clock.sleep(100);
    await clock.sleep(200);
    await clock.sleep(50);

    assert.deepEqual(sleeps, [ 100, 200, 50 ]);
  });

  test("default sleep yields at least one microtask before resolving", async () => {

    // The fake's sleep matches real delay()'s async semantics by yielding once. Without the yield, awaiters would resume synchronously after sleep, which is
    // structurally distinct from production. We verify the yield by interleaving a microtask between scheduling sleep and observing its completion - if sleep
    // resolved synchronously, the microtask would run *after* the post-sleep observation rather than before.
    const { clock } = makeFakeClock();
    const ordering: string[] = [];

    const sleepPromise = clock.sleep(0).then(() => {

      ordering.push("after-sleep");
    });

    await Promise.resolve().then(() => {

      ordering.push("microtask");
    });

    await sleepPromise;

    // The microtask scheduled after sleep ran before sleep's continuation - confirming sleep yields rather than resolving synchronously.
    assert.deepEqual(ordering, [ "microtask", "after-sleep" ]);
  });

  test("default raceWithTimeout forwards the inner promise's resolved value", async () => {

    const { clock } = makeFakeClock();

    const value = await clock.raceWithTimeout(Promise.resolve("forwarded"), 1_000);

    assert.equal(value, "forwarded");
  });

  test("default raceWithTimeout propagates the inner promise's rejection unchanged", async () => {

    // The default behavior is pass-through, so a rejected inner promise rejects the race with the same reason. Tests that want to simulate the timer winning
    // the race override raceWithTimeout explicitly; the default never invents a timeout.
    const { clock } = makeFakeClock();

    await assert.rejects(

      () => clock.raceWithTimeout(Promise.reject(new Error("inner failure")), 1_000),
      /inner failure/
    );
  });

  test("default now returns 0", () => {

    // Locked at 0 (rather than left undefined) so consumers that read now() get a deterministic value without needing to override.
    const { clock } = makeFakeClock();

    assert.equal(clock.now(), 0);
  });

  test("sleep override replaces the default and skips the sleeps recorder", async () => {

    // When a test overrides sleep(), it owns the recording. The shared sleeps array stays empty unless the override pushes into it itself - this is the
    // documented contract (helpers comment notes "tests that override sleep() own their own recording").
    const { clock, sleeps } = makeFakeClock({

      sleep: async () => {

        await Promise.resolve();
      }
    });

    await clock.sleep(500);

    assert.equal(sleeps.length, 0, "sleeps array is not populated when sleep() is overridden");
  });

  test("raceWithTimeout override replaces the default - common case is throwing the timeout error", async () => {

    // The canonical use of the override is simulating the timer winning the race. retry.test.ts's "propagates a timeout error" test relies on this exact
    // pattern; locking it here means future changes to the override mechanism are caught at the helper boundary rather than at the consumer.
    const { clock } = makeFakeClock({

      raceWithTimeout: async (_promise, timeoutMs, timeoutError) => {

        throw timeoutError ?? new Error("timed out after " + String(timeoutMs) + "ms.");
      }
    });

    await assert.rejects(

      () => clock.raceWithTimeout(new Promise<string>(() => { /* never resolves */ }), 750),
      /timed out after 750ms/
    );
  });

  test("now override replaces the default - common case is locking a deterministic timestamp", () => {

    const { clock } = makeFakeClock({ now: () => 12_345 });

    assert.equal(clock.now(), 12_345);
  });

  test("overriding one method leaves the other defaults intact", async () => {

    // Locks the partial-override semantics: callers should be able to override only the methods they care about, with everything else at its default. A future
    // refactor that accidentally couples overrides (e.g., requiring all-or-nothing) fails here.
    const { clock, sleeps } = makeFakeClock({ now: () => 999 });

    assert.equal(clock.now(), 999, "now override applied");

    await clock.sleep(42);

    assert.deepEqual(sleeps, [42], "sleep stayed at the default and recorded");

    const value = await clock.raceWithTimeout(Promise.resolve("ok"), 1);

    assert.equal(value, "ok", "raceWithTimeout stayed at the default and forwarded");
  });

  test("the sleeps array exposed on the handle is the same reference the inner sleep mutates", async () => {

    // Locks the by-reference contract: callers can capture `sleeps` once and observe updates as the function under test schedules sleeps. If a future refactor
    // accidentally swapped to copy-on-read, consumers would see an empty array forever.
    const { clock, sleeps } = makeFakeClock();
    const captured = sleeps;

    await clock.sleep(10);
    await clock.sleep(20);

    assert.equal(captured, sleeps, "the captured reference still points at the live array");
    assert.deepEqual(captured, [ 10, 20 ]);
  });
});
