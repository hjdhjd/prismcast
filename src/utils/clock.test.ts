/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clock.test.ts: Unit tests for the realClock default. The Clock interface itself is a structural type so it has no runtime behavior to test; what we test is
 * realClock - that it exposes the documented Clock methods at the right shapes and delegates to the underlying delay()/waitWithTimeout()/performance.now()
 * in ways the Clock consumers across the codebase can rely on.
 */
import { delay, waitWithTimeout } from "./delay.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { realClock } from "./clock.ts";

// The sleep the advancing check brackets, and the floor the elapsed reading is held to. The floor sits below the requested duration because a timer may fire a
// fraction early, and well above zero because a clock that did not move at all is the failure this check exists to catch.
const ADVANCE_FLOOR_MS = 8;
const ADVANCE_SLEEP_MS = 10;

describe("realClock", () => {

  test("exposes the documented Clock surface", () => {

    // Locks the structural contract: a future change that drops a method or renames one fails this test before any consumer breaks.
    assert.equal(typeof realClock.now, "function", "realClock.now is a function");
    assert.equal(typeof realClock.sleep, "function", "realClock.sleep is a function");
    assert.equal(typeof realClock.waitWithTimeout, "function", "realClock.waitWithTimeout is a function");
  });

  test("now() returns a finite non-negative number", () => {

    // performance.now() returns a high-resolution timestamp in milliseconds since the runtime started. We do not lock an exact value (it is monotonically
    // increasing on every call), but we verify the shape - a finite non-negative number suitable for elapsed-time calculations.
    const value = realClock.now();

    assert.equal(typeof value, "number", "now() should return a number");
    assert.ok(Number.isFinite(value), "performance.now produces a finite number");
    assert.ok(value >= 0, "performance.now is non-negative");
  });

  test("now() reads real time - finite, never backwards, and advancing across a sleep", async () => {

    const before = realClock.now();

    assert.ok(Number.isFinite(before), "the first reading should be a finite timestamp");

    await realClock.sleep(ADVANCE_SLEEP_MS);

    const after = realClock.now();

    assert.ok(Number.isFinite(after), "the second reading should be finite too");
    assert.ok(after >= before, "the clock should never run backwards");
    assert.ok((after - before) >= ADVANCE_FLOOR_MS, "the clock should advance by roughly the slept duration");
  });

  test("sleep() resolves after the requested delay", async () => {

    // We use a 1ms delay so the test stays well under the per-test budget. The point is to verify that sleep() returns a promise that resolves; the exact wall
    // time is not the contract, only that the promise settles. assert.doesNotReject is the canonical idiom for "this promise settles cleanly" - it asserts the
    // contract directly rather than relying on a post-await tautology.
    await assert.doesNotReject(() => realClock.sleep(1), "sleep returns a promise that settles per the Clock contract");
  });

  test("waitWithTimeout() returns the inner promise's value when it resolves before the timeout", async () => {

    const value = await realClock.waitWithTimeout(Promise.resolve("won"), 1000);

    assert.equal(value, "won", "the winning promise's value should come back unchanged");
  });

  test("waitWithTimeout() throws when the timeout fires before the inner promise resolves", async () => {

    // The inner promise is structured never to settle, so the 1ms timer is the only branch that can win. The thrown error matches the documented default
    // message format.
    const { promise: never } = Promise.withResolvers<string>();

    await assert.rejects(() => realClock.waitWithTimeout(never, 1), /timed out after 1ms/);
  });

  test("realClock.waitWithTimeout is the same reference as waitWithTimeout from delay.ts (SSOT delegation)", () => {

    // The realClock literal pulls waitWithTimeout from delay.ts as a reference, not a wrapper. Locking reference identity protects against a future refactor
    // that inlines the implementation here and silently shadows the delay.ts SSOT - that would survive existing behavior tests but introduce a divergence
    // between the two paths (direct delay.ts users vs. clock injection users).
    assert.equal(realClock.waitWithTimeout, waitWithTimeout, "delegation by reference, not by wrapper");
  });

  test("realClock.sleep is the same reference as delay from delay.ts (SSOT delegation)", () => {

    // Symmetric with the waitWithTimeout case. realClock.sleep must be the delay function itself, not a re-implementation that could drift.
    assert.equal(realClock.sleep, delay, "delegation by reference, not by wrapper");
  });

  test("waitWithTimeout() throws the supplied custom error when one is provided", async () => {

    class CustomTimeoutError extends Error {

      constructor() {

        super("custom timeout");
      }
    }

    // The inner promise is structured never to settle, so the supplied error must be the one the timeout race surfaces.
    const { promise: never } = Promise.withResolvers<string>();

    await assert.rejects(() => realClock.waitWithTimeout(never, 1, new CustomTimeoutError()), (err: unknown) => err instanceof CustomTimeoutError);
  });
});
