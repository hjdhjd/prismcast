/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clock.helpers.test.ts: Unit tests for the makeFakeClock and makeAdvancingClock test factories. Locks the documented contract of the test-side counterparts
 * to realClock so every Clock-consuming test (retry.test.ts, timing.test.ts, delay.test.ts, cdp.test.ts, and others) can rely on the factories' behavior
 * without re-deriving it from the source.
 */
import { describe, test } from "node:test";
import { makeAdvancingClock, makeFakeClock } from "./clock.helpers.ts";
import assert from "node:assert/strict";

describe("makeFakeClock", () => {

  test("returns a handle exposing the clock and a sleeps array", () => {

    // Locks the structural shape of the handle. A future change that drops `sleeps` or renames it fails here before any consumer breaks.
    const handle = makeFakeClock();

    assert.equal(typeof handle.clock, "object", "the handle should carry a clock object");
    assert.equal(typeof handle.clock.now, "function", "the clock should expose now()");
    assert.equal(typeof handle.clock.sleep, "function", "the clock should expose sleep()");
    assert.equal(typeof handle.clock.waitWithTimeout, "function", "the clock should expose waitWithTimeout()");
    assert.ok(Array.isArray(handle.sleeps), "the handle should carry a sleeps array");
    assert.equal(handle.sleeps.length, 0, "sleeps starts empty");
  });

  test("default sleep records the requested duration and resolves", async () => {

    const { clock, sleeps } = makeFakeClock();

    await clock.sleep(150);

    assert.deepEqual(sleeps, [150], "the requested duration should be recorded");
  });

  test("default sleep records multiple invocations in call order", async () => {

    // Locks the schedule-recording contract that retry's backoff tests rely on (assert.deepEqual on sleeps verifies both count and ordered values).
    const { clock, sleeps } = makeFakeClock();

    await clock.sleep(100);
    await clock.sleep(200);
    await clock.sleep(50);

    assert.deepEqual(sleeps, [ 100, 200, 50 ], "every requested duration should be recorded in call order");
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
    assert.deepEqual(ordering, [ "microtask", "after-sleep" ], "the interleaved microtask should run before sleep's continuation");
  });

  test("default waitWithTimeout forwards the inner promise's resolved value", async () => {

    const { clock } = makeFakeClock();

    const value = await clock.waitWithTimeout(Promise.resolve("forwarded"), 1000);

    assert.equal(value, "forwarded", "the default race should hand back the inner promise unchanged");
  });

  test("default waitWithTimeout propagates the inner promise's rejection unchanged", async () => {

    // The default behavior is pass-through, so a rejected inner promise rejects the race with the same reason. Tests that want to simulate the timer winning
    // the race override waitWithTimeout explicitly; the default never invents a timeout.
    const { clock } = makeFakeClock();

    await assert.rejects(

      () => clock.waitWithTimeout(Promise.reject(new Error("inner failure")), 1000),
      /inner failure/
    );
  });

  test("default now returns 0", () => {

    // Locked at 0 (rather than left undefined) so consumers that read now() get a deterministic value without needing to override.
    const { clock } = makeFakeClock();

    assert.equal(clock.now(), 0, "a fake clock reads zero until the test says otherwise");
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

  test("waitWithTimeout override replaces the default - common case is throwing the timeout error", async () => {

    // The canonical use of the override is simulating the timer winning the race. retry.test.ts's "propagates a timeout error" test relies on this exact
    // pattern; locking it here means future changes to the override mechanism are caught at the helper boundary rather than at the consumer.
    const { clock } = makeFakeClock({

      waitWithTimeout: async (_promise, timeoutMs, timeoutError) => {

        throw timeoutError ?? new Error("timed out after " + String(timeoutMs) + "ms.");
      }
    });

    const { promise: never } = Promise.withResolvers<string>();

    await assert.rejects(

      () => clock.waitWithTimeout(never, 750),
      /timed out after 750ms/
    );
  });

  test("now override replaces the default - common case is locking a deterministic timestamp", () => {

    const { clock } = makeFakeClock({ now: () => 12345 });

    assert.equal(clock.now(), 12345, "the override's timestamp should be the one read");
  });

  test("overriding one method leaves the other defaults intact", async () => {

    // Locks the partial-override semantics: callers should be able to override only the methods they care about, with everything else at its default. A future
    // refactor that accidentally couples overrides (e.g., requiring all-or-nothing) fails here.
    const { clock, sleeps } = makeFakeClock({ now: () => 999 });

    assert.equal(clock.now(), 999, "now override applied");

    await clock.sleep(42);

    assert.deepEqual(sleeps, [42], "sleep stayed at the default and recorded");

    const value = await clock.waitWithTimeout(Promise.resolve("ok"), 1);

    assert.equal(value, "ok", "waitWithTimeout stayed at the default and forwarded");
  });

  test("the sleeps array exposed on the handle is the same reference the inner sleep mutates", async () => {

    // Locks the by-reference contract: callers can capture `sleeps` once and observe updates as the function under test schedules sleeps. If a future refactor
    // accidentally swapped to copy-on-read, consumers would see an empty array forever.
    const { clock, sleeps } = makeFakeClock();
    const captured = sleeps;

    await clock.sleep(10);
    await clock.sleep(20);

    assert.equal(captured, sleeps, "the captured reference still points at the live array");
    assert.deepEqual(captured, [ 10, 20 ], "the captured reference should see every recorded sleep");
  });

  test("two independent clocks have independent sleeps arrays", async () => {

    const a = makeFakeClock();
    const b = makeFakeClock();

    await a.clock.sleep(11);
    await b.clock.sleep(22);

    assert.deepEqual(a.sleeps, [11], "the first clock should record only its own sleep");
    assert.deepEqual(b.sleeps, [22], "the second clock should record only its own sleep");
  });
});

describe("makeAdvancingClock", () => {

  test("each sleep moves now() forward by exactly the duration it was asked for", async () => {

    // The whole point of this variant: a consumer that measures elapsed time reads a clock that the sleeps it schedules actually move. The default fake freezes
    // now() at zero, which would make any ceiling unreachable.
    const { clock } = makeAdvancingClock();

    assert.equal(clock.now(), 0, "an untouched advancing clock still reads zero");

    await clock.sleep(25);

    assert.equal(clock.now(), 25, "one sleep advances the clock by its own duration");

    await clock.sleep(75);

    assert.equal(clock.now(), 100, "durations accumulate");
  });

  test("records every sleep in call order, exactly as the default fake does", async () => {

    // The recording contract is the same one consumers already assert against, so a test can move between the two factories without changing its assertions.
    const { clock, sleeps } = makeAdvancingClock();

    await clock.sleep(10);
    await clock.sleep(20);

    assert.deepEqual(sleeps, [ 10, 20 ], "every requested duration is recorded in call order");
  });

  test("keeps the default waitWithTimeout, which forwards the inner promise", async () => {

    // Only now() and sleep() differ from the default fake; everything else is inherited, so a consumer bounding a promise behaves identically under either.
    const { clock } = makeAdvancingClock();

    assert.equal(await clock.waitWithTimeout(Promise.resolve("forwarded"), 1000), "forwarded", "the inner promise is handed back unchanged");
  });

  test("two independent advancing clocks keep separate time", async () => {

    const a = makeAdvancingClock();
    const b = makeAdvancingClock();

    await a.clock.sleep(40);

    assert.equal(a.clock.now(), 40, "the first clock advanced");
    assert.equal(b.clock.now(), 0, "the second clock is untouched");
    assert.deepEqual(b.sleeps, [], "and recorded nothing");
  });
});
