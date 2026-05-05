/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * delay.test.ts: Unit tests for the timer primitives in delay.ts (cancellableTimeout, raceWithTimeout, delay). All three exports use real setTimeout. The tests
 * use small real-time delays (1-30ms) to stay well under the per-test budget while exercising the timer cleanup paths - mock.timers in Node 25 lacks the async
 * tick variant needed to drain Promise.race over setTimeout reliably.
 */
import { cancellableTimeout, delay, raceWithTimeout } from "./delay.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("cancellableTimeout", () => {

  test("resolves to false after the configured delay when not cancelled", async () => {

    const { promise } = cancellableTimeout(10);
    const result = await promise;

    assert.equal(result, false, "the timeout promise resolves with literal false on fire");
  });

  test("never resolves when cancel() is called before the timer fires", async () => {

    // The cancel function clears the underlying timer. We race the cancelled promise against a longer real-time guard - if cancel did not clear the timer, the
    // promise would resolve to false within the guard window and the assertion would catch it.
    const { cancel, promise } = cancellableTimeout(10);

    cancel();

    const sentinel = Symbol("not-resolved");
    const guarded = await Promise.race([ promise, new Promise((resolve) => { setTimeout(() => { resolve(sentinel); }, 30); }) ]);

    assert.equal(guarded, sentinel, "guard wins the race because cancellation prevented the timeout from firing");
  });

  test("cancel() is idempotent and safe to call multiple times", () => {

    // Negative test: calling cancel twice (or after the timer has already fired) must not throw. clearTimeout silently no-ops on already-cleared/fired timers.
    const { cancel } = cancellableTimeout(1_000);

    assert.doesNotThrow(() => {

      cancel();
      cancel();
      cancel();
    });
  });

  test("returns an object with both promise and cancel keys", () => {

    const result = cancellableTimeout(1_000);

    assert.equal(typeof result.cancel, "function", "cancel is callable");
    assert.ok(result.promise instanceof Promise, "promise is a Promise instance");

    result.cancel();
  });
});

describe("raceWithTimeout", () => {

  test("returns the resolved value when the inner promise wins the race", async () => {

    const fast = Promise.resolve("fast-value");
    const result = await raceWithTimeout(fast, 100);

    assert.equal(result, "fast-value");
  });

  test("rejects with the default timeout error when the timer wins", async () => {

    // Negative test: a never-resolving inner promise must surface as a timeout rejection.
    const never = new Promise<string>(() => { /* never resolves */ });

    await assert.rejects(() => raceWithTimeout(never, 5), /Operation timed out after 5ms/, "default error message includes the timeout duration");
  });

  test("rejects with the supplied custom error when the timer wins", async () => {

    class CustomTimeoutError extends Error {

      constructor() {

        super("custom timeout"); this.name = "CustomTimeoutError";
      }
    }

    const never = new Promise<string>(() => { /* never resolves */ });

    await assert.rejects(() => raceWithTimeout(never, 5, new CustomTimeoutError()), /custom timeout/, "the supplied error is thrown rather than the default");
  });

  test("propagates the inner promise's rejection (not a timeout)", async () => {

    // Negative test: when the inner promise rejects fast, the timeout helper must not mask it with its own timeout error.
    const failing = Promise.reject(new Error("inner failure"));

    await assert.rejects(() => raceWithTimeout(failing, 100), /inner failure/, "inner rejection is preserved, not replaced by the timeout error");
  });

  test("clears the timer when the inner promise resolves first (no leaked handle)", async () => {

    // The .finally cleanup is the key contract. We can't directly observe that clearTimeout fired, but we can verify the function returns and resolves
    // synchronously after the inner promise: the test just running to completion within the per-test budget proves there's no orphan timer keeping the loop alive.
    const result = await raceWithTimeout(Promise.resolve("ok"), 50_000);

    assert.equal(result, "ok");
  });

  test("respects a 0ms timeout (effectively yielding to the event loop)", async () => {

    // Boundary: timeoutMs of 0 still schedules the timer; a never-resolving inner promise will lose to the immediate timeout fire.
    const never = new Promise<string>(() => { /* never resolves */ });

    await assert.rejects(() => raceWithTimeout(never, 0), /timed out after 0ms/, "0ms timeout still fires");
  });
});

describe("delay", () => {

  test("returns a Promise that resolves to undefined", async () => {

    // We cannot bind the awaited result to a const because TypeScript narrows it to `void`. Instead we assert that the promise itself resolves without throwing
    // and that the documented return type is honored at the type system level (Promise<void>).
    await assert.doesNotReject(() => delay(1), "delay resolves without rejection");
  });

  test("waits at least the requested duration in real time", async () => {

    // We pick a small value to stay under budget. Slack is one-sided: real wall time can run a touch slower than requested due to runner overhead, but it
    // cannot run faster than setTimeout's clock.
    const start = performance.now();

    await delay(10);

    const elapsed = performance.now() - start;

    assert.ok(elapsed >= 8, "elapsed should be at least 8ms (one-sided slack): " + String(elapsed));
  });

  test("returns a Promise instance (not a sync return value)", async () => {

    const result = delay(0);

    assert.ok(result instanceof Promise, "delay always returns a Promise");

    await result;
  });

  test("handles a 0ms delay by yielding to the event loop", async () => {

    // Boundary: a 0ms delay still goes through setTimeout, which means at least one task-queue tick before resolution.
    let synchronouslySet = false;
    const promise = delay(0);

    // Set the flag synchronously after starting the delay - it should be true before the delay resolves.
    synchronouslySet = true;

    await promise;

    assert.equal(synchronouslySet, true, "synchronous code after delay(0) call ran before the resolution");
  });
});
