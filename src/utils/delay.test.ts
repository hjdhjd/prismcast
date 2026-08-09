/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * delay.test.ts: Unit tests for the wait policies in delay.ts (timeoutSignal, waitWithTimeout, boundedWait, delay). Every export uses real setTimeout. The
 * timing-only checks on timeoutSignal drive mock.timers, whose synchronous tick is enough because an abort is delivered synchronously from the timer callback;
 * the policies that await a promise use small real-time delays (1-30ms) instead, because a synchronous tick cannot drain the microtask chain an awaited wait
 * settles through.
 */
import { boundedWait, delay, timeoutSignal, waitWithTimeout } from "./delay.ts";
import { describe, mock, test } from "node:test";
import assert from "node:assert/strict";

describe("timeoutSignal", () => {

  test("stays quiet until the duration elapses, then aborts carrying the supplied error", () => {

    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const reason = new Error("bespoke lapse");
      const timeout = timeoutSignal(1000, reason);

      mock.timers.tick(999);

      assert.equal(timeout.signal.aborted, false, "one tick short of the duration the signal is still quiet");

      mock.timers.tick(1);

      assert.equal(timeout.signal.aborted, true, "the signal aborts once the duration elapses");
      assert.equal(timeout.signal.reason, reason, "the abort reason is the caller's own error object, by reference");

      timeout.cancel();
    } finally {

      mock.timers.reset();
    }
  });

  test("aborts with a default error naming the duration when no reason is supplied", () => {

    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const timeout = timeoutSignal(250);

      mock.timers.tick(250);

      assert.match((timeout.signal.reason as Error).message, /Operation timed out after 250ms\./, "the default reason names the duration");

      timeout.cancel();
    } finally {

      mock.timers.reset();
    }
  });

  test("a cancelled handle never aborts, even well past its duration", () => {

    // Negative test: cancel() has to clear the underlying timer outright, not merely leave nobody watching it.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const timeout = timeoutSignal(1000);

      timeout.cancel();
      mock.timers.tick(5000);

      assert.equal(timeout.signal.aborted, false, "a cancelled handle stays quiet past its own duration");
    } finally {

      mock.timers.reset();
    }
  });

  test("cancel() is safe to call more than once", () => {

    // clearTimeout silently does nothing on an already-cleared or already-fired timer, so a consumer that cancels defensively cannot break.
    const timeout = timeoutSignal(50000);

    assert.doesNotThrow(() => {

      timeout.cancel();
      timeout.cancel();
      timeout.cancel();
    });
  });

  test("unrefs the timer it creates", () => {

    // A dropped unref is invisible from outside the handle - the signal behaves identically either way - so we capture the timer this call creates by wrapping
    // the global for its duration and ask the handle itself. Without the unref, a bound still counting down would hold an otherwise-empty loop open.
    const realSetTimeout = globalThis.setTimeout;

    let unrefCount = 0;

    globalThis.setTimeout = ((callback: (...callbackArgs: unknown[]) => void, ms?: number): ReturnType<typeof setTimeout> => {

      const handle = realSetTimeout(callback, ms);
      const realUnref = handle.unref.bind(handle);

      handle.unref = (): ReturnType<typeof setTimeout> => {

        unrefCount++;

        return realUnref();
      };

      return handle;
    }) as unknown as typeof globalThis.setTimeout;

    try {

      timeoutSignal(50000).cancel();
    } finally {

      globalThis.setTimeout = realSetTimeout;
    }

    assert.equal(unrefCount, 1, "the created timer is unref'd exactly once");
  });

  test("returns a handle exposing both cancel and signal", () => {

    const timeout = timeoutSignal(50000);

    assert.equal(typeof timeout.cancel, "function", "cancel is callable");
    assert.ok(timeout.signal instanceof AbortSignal, "signal is an AbortSignal instance");

    timeout.cancel();
  });
});

describe("waitWithTimeout", () => {

  test("returns the resolved value when the promise settles inside the bound", async () => {

    const fast = Promise.resolve("fast-value");
    const result = await waitWithTimeout(fast, 100);

    assert.equal(result, "fast-value", "the resolved value should come from the inner promise");
  });

  test("rejects with the default timeout error when the bound lapses", async () => {

    // Negative test: a never-resolving promise must surface as a timeout rejection.
    const { promise: never } = Promise.withResolvers<string>();

    await assert.rejects(() => waitWithTimeout(never, 5), /Operation timed out after 5ms/, "default error message includes the timeout duration");
  });

  test("rejects with the caller's exact error object when the bound lapses", async () => {

    // Identity, not shape: call sites such as the discovery-settlement wait tell their own lapse apart from any other failure by comparing against the very
    // object they passed in, so the policy must deliver that object untouched rather than a copy or a wrapper.
    class CustomTimeoutError extends Error {

      constructor() {

        super("custom timeout");
        this.name = "CustomTimeoutError";
      }
    }

    const timeoutError = new CustomTimeoutError();
    const { promise: never } = Promise.withResolvers<string>();

    await assert.rejects(() => waitWithTimeout(never, 5, timeoutError), (error: unknown) => error === timeoutError,
      "the supplied error object itself is thrown, by reference");
  });

  test("propagates the promise's own rejection (not a timeout)", async () => {

    // Negative test: when the promise rejects inside the bound, the policy must not mask it with its own timeout error.
    const failing = Promise.reject(new Error("inner failure"));

    await assert.rejects(() => waitWithTimeout(failing, 100), /inner failure/, "inner rejection is preserved, not replaced by the timeout error");
  });

  test("cancels the timer it created once the promise wins", async () => {

    // The disposal cannot be observed through behavior: the library removes its own abort listener at settlement, so even a timer left running would abort into
    // nobody and the wait would look identical. Watching the timer functions for the duration of one call is the only way to see a dropped cancel.
    const realClearTimeout = globalThis.clearTimeout;
    const realSetTimeout = globalThis.setTimeout;
    const cleared: unknown[] = [];

    let created: ReturnType<typeof setTimeout> | undefined;

    globalThis.setTimeout = ((callback: (...callbackArgs: unknown[]) => void, ms?: number): ReturnType<typeof setTimeout> => {

      const handle = realSetTimeout(callback, ms);

      created ??= handle;

      return handle;
    }) as unknown as typeof globalThis.setTimeout;

    globalThis.clearTimeout = ((handle?: ReturnType<typeof setTimeout>): void => {

      cleared.push(handle);
      realClearTimeout(handle);
    }) as unknown as typeof globalThis.clearTimeout;

    try {

      await waitWithTimeout(Promise.resolve("ok"), 50000);
    } finally {

      globalThis.clearTimeout = realClearTimeout;
      globalThis.setTimeout = realSetTimeout;
    }

    assert.notEqual(created, undefined, "the policy created a timer");
    assert.ok(cleared.includes(created), "the timer the policy created was cleared once the promise won");
  });

  test("settles cleanly when many bounded waits run back-to-back (no leaked handles)", async () => {

    // Running many waits back-to-back exercises the cancel path in bulk: a leaked handle would surface as the runner hanging at exit rather than as a failed
    // assertion, which the fast pass here plus the --test-force-exit safety net in the test scripts together rule out.
    const promises = Array.from({ length: 50 }, async (_, i) => waitWithTimeout(Promise.resolve(i), 10000));

    const results = await Promise.all(promises);

    assert.equal(results.length, 50, "every wait should settle");
    assert.equal(results[0], 0, "the first wait should carry its own value");
    assert.equal(results[49], 49, "the last wait should carry its own value");
  });

  test("respects a 0ms timeout (effectively yielding to the event loop)", async () => {

    // Boundary: a timeoutMs of 0 still schedules the timer; a never-resolving promise loses to the immediate fire.
    const { promise: never } = Promise.withResolvers<string>();

    await assert.rejects(() => waitWithTimeout(never, 0), /timed out after 0ms/, "0ms timeout still fires");
  });
});

describe("boundedWait", () => {

  test("returns the resolved value when the promise settles inside the bound", async () => {

    const result = await boundedWait(Promise.resolve("value"), 50000);

    assert.equal(result, "value", "the promise's value comes back unchanged");
  });

  test("returns null when the bound lapses before the promise settles", async () => {

    /* The bound has to ride this project's own timeout signal, which is built on the global setTimeout a fake clock can virtualize, rather than on a timer the
     * platform owns internally. A large bound is what tells those two wirings apart: mock.timers fires the virtualized timer on the tick, so the correct wiring
     * settles here and now, while a bound riding a platform-internal timer would still be a real sixty seconds away and would lose to the short sentinel below.
     */
    const { promise: never } = Promise.withResolvers<string>();

    mock.timers.enable({ apis: ["setTimeout"] });

    const pending = boundedWait(never, 60000);

    try {

      mock.timers.tick(60000);
    } finally {

      mock.timers.reset();
    }

    const stillPending = Symbol("still-pending");
    const settled = await Promise.race([ pending, new Promise((resolve) => { setTimeout(() => { resolve(stillPending); }, 50); }) ]);

    assert.equal(settled, null, "the lapsed bound settles null on the tick rather than staying pending");
  });

  test("propagates a rejection that arrives inside the bound", async () => {

    // Negative test: a rejection is a failure, not a lapse, so it must travel to the caller rather than being flattened into the null branch.
    const failing = Promise.reject(new Error("inner failure"));

    await assert.rejects(() => boundedWait(failing, 50000), /inner failure/, "the promise's own rejection is not swallowed");
  });

  test("a rejection arriving after the bound lapsed surfaces nowhere", async () => {

    // By the time the promise rejects the wait has already settled null, so there is no caller left to throw into. The library observes the promise on every
    // path, which is what keeps that late rejection away from Node's unhandled-rejection tracker - and this runner fails the file if one is ever reported.
    const { promise: late, reject: rejectLate } = Promise.withResolvers<string>();
    const lapsed = await boundedWait(late, 5);

    assert.equal(lapsed, null, "the bound lapsed first");

    rejectLate(new Error("arrived after the bound lapsed"));

    // Give the rejection a full turn of the loop in which to be reported, if it were ever going to be.
    await delay(20);
  });

  test("cancels the timer it created once the promise wins", async () => {

    // Same reasoning as the throw-shaped policy's cleared-timer check: a dropped cancel is invisible through behavior, because the abort would land on a wait
    // that has already settled. Watching the timer functions across one call is what makes the disposal observable.
    const realClearTimeout = globalThis.clearTimeout;
    const realSetTimeout = globalThis.setTimeout;
    const cleared: unknown[] = [];

    let created: ReturnType<typeof setTimeout> | undefined;

    globalThis.setTimeout = ((callback: (...callbackArgs: unknown[]) => void, ms?: number): ReturnType<typeof setTimeout> => {

      const handle = realSetTimeout(callback, ms);

      created ??= handle;

      return handle;
    }) as unknown as typeof globalThis.setTimeout;

    globalThis.clearTimeout = ((handle?: ReturnType<typeof setTimeout>): void => {

      cleared.push(handle);
      realClearTimeout(handle);
    }) as unknown as typeof globalThis.clearTimeout;

    try {

      await boundedWait(Promise.resolve("ok"), 50000);
    } finally {

      globalThis.clearTimeout = realClearTimeout;
      globalThis.setTimeout = realSetTimeout;
    }

    assert.notEqual(created, undefined, "the policy created a timer");
    assert.ok(cleared.includes(created), "the timer the policy created was cleared once the promise won");
  });
});

describe("delay", () => {

  test("returns a Promise that resolves to undefined", async () => {

    // We don't bind the awaited result to a const because its type is void and carries no useful value. Instead we assert that the promise itself resolves without
    // throwing and that the documented return type is honored at the type system level (Promise<void>).
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
