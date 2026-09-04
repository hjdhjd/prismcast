/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * timing.test.ts: Unit tests for the startTimer closure in timing.ts. Time is read through an injected Clock (see clock.ts) so the tests use a fake clock with a
 * controllable now() function - no real-time delays, no busy-waits, no slack budgets. The default-arg wiring to realClock is locked in a separate test that
 * exercises the production path without depending on any specific elapsed value.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { makeFakeClock } from "./clock.helpers.ts";
import { startTimer } from "./timing.ts";

describe("startTimer", () => {

  test("returns 0 when read at the same time it was created", () => {

    // Boundary: the smallest possible interval. With both reads at the same fake-clock value, the closure returns 0 deterministically (no slack needed).
    const { clock } = makeFakeClock({ now: () => 0 });

    const elapsed = startTimer(clock);

    assert.equal(elapsed(), 0);
  });

  test("returns a non-negative integer rounded from the elapsed delta", () => {

    // The closure rounds the delta. With now() returning 4.6 then 9.3, the delta is 4.7 and the rounded value is 5.
    let mockTime = 4.6;
    const { clock } = makeFakeClock({ now: () => mockTime });

    const elapsed = startTimer(clock);

    mockTime = 9.3;

    const value = elapsed();

    assert.equal(value, 5, "Math.round(9.3 - 4.6) = 5");
    assert.equal(typeof value, "number");
    assert.equal(value, Math.round(value), "result is an integer");
  });

  test("captures the start time at creation, not at first read", () => {

    // Locks the closure semantic: the start value is fixed when startTimer() returns; subsequent advances of the clock change the elapsed read but not the start.
    let mockTime = 100;
    const { clock } = makeFakeClock({ now: () => mockTime });

    const elapsed = startTimer(clock);

    mockTime = 250;
    assert.equal(elapsed(), 150, "first read sees the delta from the captured start");

    mockTime = 1000;
    assert.equal(elapsed(), 900, "second read still measures from the original start, not the previous read");
  });

  test("reports a non-decreasing value across multiple reads when the clock is monotonic", () => {

    // The fake clock's now() returns the current mockTime; advancing the variable monotonically gives a monotonic clock. The closure's reads must reflect that.
    let mockTime = 0;
    const { clock } = makeFakeClock({ now: () => mockTime });

    const elapsed = startTimer(clock);

    mockTime = 10;
    const a = elapsed();

    mockTime = 10;
    const b = elapsed();

    mockTime = 25;
    const c = elapsed();

    assert.ok(b >= a, "second read >= first when the clock did not advance");
    assert.ok(c >= b, "third read >= second after the clock advanced");
    assert.equal(a, 10);
    assert.equal(b, 10);
    assert.equal(c, 25);
  });

  test("reflects a clock advance of N ms as an elapsed value of N", () => {

    // The deterministic equivalent of "real-time delay" - we advance the fake clock by 30 and verify the closure reports exactly 30. No slack, no flake.
    let mockTime = 1000;
    const { clock } = makeFakeClock({ now: () => mockTime });

    const elapsed = startTimer(clock);

    mockTime = 1030;

    assert.equal(elapsed(), 30);
  });

  test("each call to startTimer creates an independent closure with its own captured start", () => {

    // Negative test: two timers must not share state. We start the second after advancing the clock; reading both should show different elapsed values measured
    // from each one's own start.
    let mockTime = 0;
    const { clock } = makeFakeClock({ now: () => mockTime });

    const a = startTimer(clock);

    mockTime = 5;

    const b = startTimer(clock);

    mockTime = 12;

    assert.equal(a(), 12, "timer A measures from start=0, reads at now=12");
    assert.equal(b(), 7, "timer B measures from start=5, reads at now=12");
  });

  test("default-arg wires through to realClock when no clock is supplied", () => {

    // Locks the default-argument behavior so a future refactor that breaks the optional doesn't pass unnoticed. We verify the value-shape contract (number,
    // non-negative, integer) without asserting any specific elapsed value, since realClock reads performance.now() and the test runtime's elapsed time is
    // not part of the contract.
    const elapsed = startTimer();
    const value = elapsed();

    assert.equal(typeof value, "number");
    assert.ok(value >= 0, "elapsed is non-negative");
    assert.equal(value, Math.round(value), "elapsed is an integer (the implementation rounds)");
  });
});
