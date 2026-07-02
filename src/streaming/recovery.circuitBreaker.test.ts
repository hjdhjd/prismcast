/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * recovery.circuitBreaker.test.ts: Unit tests for the circuit-breaker and failure-window primitives in recovery.ts - checkCircuitBreaker, resetCircuitBreaker,
 * recordFailure, and resetFailureWindow. Issue classification helpers live in recovery.test.ts; metrics tracking lives in recovery.metrics.test.ts.
 */
import type { CircuitBreakerState, FailureWindowState } from "./recovery.ts";
import { checkCircuitBreaker, recordFailure, resetCircuitBreaker, resetFailureWindow } from "./recovery.ts";
import { describe, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import assert from "node:assert/strict";

describe("checkCircuitBreaker", () => {

  // The circuit breaker pulls thresholds from CONFIG.recovery; we read those values at module scope so the tests adapt to whatever the running config holds.
  // The function takes `now` as a parameter, so no mock.timers is needed - it's a pure function modulo the CONFIG read.
  const threshold = CONFIG.recovery.circuitBreakerThreshold;
  const window = CONFIG.recovery.circuitBreakerWindow;

  function freshState(): CircuitBreakerState {

    return { firstFailureTime: null, totalFailureCount: 0 };
  }

  test("does not trip on the first failure even at the threshold-1 count", () => {

    const state = freshState();

    for(let i = 0; i < (threshold - 1); i++) {

      const result = checkCircuitBreaker(state, 1_000 + i);

      assert.equal(result.shouldTrip, false, "should not trip below threshold (count " + String(i + 1) + ")");
    }

    assert.equal(state.totalFailureCount, threshold - 1, "state count should equal threshold-1");
  });

  test("trips when the threshold count is reached within the window", () => {

    const state = freshState();
    let result = checkCircuitBreaker(state, 1_000);

    for(let i = 1; i < threshold; i++) {

      result = checkCircuitBreaker(state, 1_000 + i);
    }

    assert.equal(result.shouldTrip, true, "threshold-th failure trips the breaker");
    assert.equal(result.totalCount, threshold, "total reflects the threshold count");
    assert.equal(result.withinWindow, true, "all failures fell within the window");
  });

  test("resets the count when a failure occurs outside the window", () => {

    const state = freshState();

    // Two failures inside a baseline window starting at t=1000.
    checkCircuitBreaker(state, 1_000);
    checkCircuitBreaker(state, 1_500);

    assert.equal(state.totalFailureCount, 2, "two failures recorded inside the window");

    // Third failure beyond the window from the first - state must be reset to count 1, with a fresh start time.
    const result = checkCircuitBreaker(state, 1_000 + window + 1);

    assert.equal(result.withinWindow, false, "post-window failure should report withinWindow=false");
    assert.equal(state.totalFailureCount, 1, "count reset to 1 after the window expires");
    assert.equal(state.firstFailureTime, 1_000 + window + 1, "first-failure timestamp updated to the new failure");
  });

  test("populates firstFailureTime on the very first failure", () => {

    const state = freshState();

    assert.equal(state.firstFailureTime, null, "starts unset");

    checkCircuitBreaker(state, 42);

    assert.equal(state.firstFailureTime, 42, "first call populates the timestamp");
  });

  test("returns withinWindow=true when the elapsed delta is window minus one millisecond (boundary)", () => {

    // The implementation uses `(now - firstFailureTime) < window`, so a delta equal to (window - 1) is inside the window. Lock the off-by-one boundary.
    const state = freshState();

    checkCircuitBreaker(state, 1_000);
    const result = checkCircuitBreaker(state, 1_000 + window - 1);

    assert.equal(result.withinWindow, true, "delta of window-1 ms is still inside");
  });

  test("returns withinWindow=false when elapsed equals the window exactly (the off-by-one boundary)", () => {

    const state = freshState();

    checkCircuitBreaker(state, 1_000);
    const result = checkCircuitBreaker(state, 1_000 + window);

    assert.equal(result.withinWindow, false, "delta of exactly the window is OUTSIDE per the strict-less-than comparison");
  });
});

describe("resetCircuitBreaker", () => {

  test("clears the count and the first-failure timestamp", () => {

    const state: CircuitBreakerState = { firstFailureTime: 42, totalFailureCount: 7 };

    resetCircuitBreaker(state);

    assert.equal(state.firstFailureTime, null);
    assert.equal(state.totalFailureCount, 0);
  });

  test("is idempotent on an already-fresh state", () => {

    const state: CircuitBreakerState = { firstFailureTime: null, totalFailureCount: 0 };

    resetCircuitBreaker(state);

    assert.equal(state.firstFailureTime, null);
    assert.equal(state.totalFailureCount, 0);
  });
});

describe("recordFailure (failure-window primitive)", () => {

  // recordFailure is the SSOT the circuit breaker delegates to. These tests pass explicit bounds (not CONFIG) to pin the parameterized contract the browser
  // supervisor will rely on - the same primitive must serve consumers with different tolerances. `now` is a parameter, so no timer mocking is needed.
  const OPTIONS = { threshold: 3, windowMs: 1000 };

  function freshState(): FailureWindowState {

    return { firstFailureTime: null, totalFailureCount: 0 };
  }

  test("does not trip below the threshold within the window", () => {

    const state = freshState();
    const r1 = recordFailure(state, 0, OPTIONS);
    const r2 = recordFailure(state, 100, OPTIONS);

    assert.equal(r1.tripped, false);
    assert.equal(r2.tripped, false);
    assert.equal(r2.totalCount, 2);
    assert.equal(r2.withinWindow, true);
  });

  test("trips exactly when the threshold count is reached within the window", () => {

    const state = freshState();

    recordFailure(state, 0, OPTIONS);
    recordFailure(state, 100, OPTIONS);

    const r3 = recordFailure(state, 200, OPTIONS);

    assert.equal(r3.tripped, true);
    assert.equal(r3.totalCount, 3);
  });

  test("restarts the window when a failure arrives after it has lapsed", () => {

    const state = freshState();

    recordFailure(state, 0, OPTIONS);
    recordFailure(state, 100, OPTIONS);

    // This failure is outside the 1000ms window anchored at t=0, so the window restarts from it.
    const lapsed = recordFailure(state, 1500, OPTIONS);

    assert.equal(lapsed.withinWindow, false, "the lapsed failure is outside the prior window");
    assert.equal(lapsed.tripped, false, "a restarted window cannot trip on its first failure");
    assert.equal(state.totalFailureCount, 1, "the window restarts from this failure");
    assert.equal(state.firstFailureTime, 1500, "the window re-anchors to this failure");
  });

  test("honors the supplied bounds rather than any single config value (the reuse contract)", () => {

    // The same two-failure sequence trips a strict window but not a tolerant one - proving the primitive is parameterized for consumers with different tolerances
    // (the per-stream breaker vs. the browser supervisor), not hard-wired to CONFIG.recovery.
    const strict = freshState();
    const tolerant = freshState();

    recordFailure(strict, 0, { threshold: 2, windowMs: 1000 });

    const strictTrip = recordFailure(strict, 100, { threshold: 2, windowMs: 1000 });

    assert.equal(strictTrip.tripped, true, "the strict window trips at 2");

    recordFailure(tolerant, 0, { threshold: 5, windowMs: 1000 });

    const tolerantNoTrip = recordFailure(tolerant, 100, { threshold: 5, windowMs: 1000 });

    assert.equal(tolerantNoTrip.tripped, false, "the tolerant window does not trip at 2");
  });
});

describe("resetFailureWindow", () => {

  test("clears the count and the first-failure timestamp", () => {

    const state: FailureWindowState = { firstFailureTime: 42, totalFailureCount: 7 };

    resetFailureWindow(state);

    assert.equal(state.firstFailureTime, null);
    assert.equal(state.totalFailureCount, 0);
  });
});
