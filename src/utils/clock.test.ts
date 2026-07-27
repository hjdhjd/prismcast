/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clock.test.ts: Unit tests for the realClock default. The Clock interface itself is a structural type so it has no runtime behavior to test; what we test is
 * realClock - that it exposes the documented Clock methods at the right shapes and delegates to the underlying delay()/raceWithTimeout()/performance.now()
 * in ways the Clock consumers across the codebase can rely on.
 */
import { delay, raceWithTimeout } from "./delay.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { realClock } from "./clock.ts";

describe("realClock", () => {

  test("exposes the documented Clock surface", () => {

    // Locks the structural contract: a future change that drops a method or renames one fails this test before any consumer breaks.
    assert.equal(typeof realClock.now, "function", "realClock.now is a function");
    assert.equal(typeof realClock.raceWithTimeout, "function", "realClock.raceWithTimeout is a function");
    assert.equal(typeof realClock.sleep, "function", "realClock.sleep is a function");
  });

  test("now() returns a finite non-negative number", () => {

    // performance.now() returns a high-resolution timestamp in milliseconds since the runtime started. We do not lock an exact value (it is monotonically
    // increasing on every call), but we verify the shape - a finite non-negative number suitable for elapsed-time calculations.
    const value = realClock.now();

    assert.equal(typeof value, "number");
    assert.ok(Number.isFinite(value), "performance.now produces a finite number");
    assert.ok(value >= 0, "performance.now is non-negative");
  });

  test("now() is monotonically non-decreasing across consecutive reads", () => {

    // Two reads of performance.now back-to-back produce values where the second is >= the first. This is the contract the rest of the codebase (e.g. timing.ts's
    // startTimer) relies on.
    const a = realClock.now();
    const b = realClock.now();

    assert.ok(b >= a, "second read is at or after the first: " + String(b) + " >= " + String(a));
  });

  test("sleep() resolves after the requested delay", async () => {

    // We use a 1ms delay so the test stays well under the per-test budget. The point is to verify that sleep() returns a promise that resolves; the exact wall
    // time is not the contract, only that the promise settles. assert.doesNotReject is the canonical idiom for "this promise settles cleanly" - it asserts the
    // contract directly rather than relying on a post-await tautology.
    await assert.doesNotReject(() => realClock.sleep(1), "sleep returns a promise that settles per the Clock contract");
  });

  test("raceWithTimeout() returns the inner promise's value when it resolves before the timeout", async () => {

    const value = await realClock.raceWithTimeout(Promise.resolve("won"), 1_000);

    assert.equal(value, "won");
  });

  test("raceWithTimeout() throws when the timeout fires before the inner promise resolves", async () => {

    // The inner promise never resolves; the 1ms timer wins the race. The thrown error matches the documented default message format.
    await assert.rejects(

      () => realClock.raceWithTimeout(new Promise<string>(() => { /* never resolves */ }), 1),
      /timed out after 1ms/
    );
  });

  test("realClock.raceWithTimeout is the same reference as raceWithTimeout from delay.ts (SSOT delegation)", () => {

    // The realClock literal pulls raceWithTimeout from delay.ts as a reference, not a wrapper. Locking reference identity protects against a future refactor
    // that inlines the implementation here and silently shadows the delay.ts SSOT - that would survive existing behavior tests but introduce a divergence
    // between the two paths (direct delay.ts users vs. clock injection users).
    assert.equal(realClock.raceWithTimeout, raceWithTimeout, "delegation by reference, not by wrapper");
  });

  test("realClock.sleep is the same reference as delay from delay.ts (SSOT delegation)", () => {

    // Symmetric with the raceWithTimeout case. realClock.sleep must be the delay function itself, not a re-implementation that could drift.
    assert.equal(realClock.sleep, delay, "delegation by reference, not by wrapper");
  });

  test("raceWithTimeout() throws the supplied custom error when one is provided", async () => {

    class CustomTimeoutError extends Error {

      constructor() {

        super("custom timeout");
      }
    }

    // The inner promise never resolves; the supplied error must be the one the timeout race surfaces.
    await assert.rejects(

      () => realClock.raceWithTimeout(new Promise<string>(() => { /* never resolves */ }), 1, new CustomTimeoutError()),
      (err: unknown) => err instanceof CustomTimeoutError
    );
  });
});
