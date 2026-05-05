/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * recovery.test.ts: Unit tests for the pure recovery primitives in recovery.ts - circuit breaker logic, recovery metrics tracking, issue classification, and
 * the assorted formatting helpers. recovery.ts is the documented SSOT for recovery decisions, and the bulk of its surface area is pure functions that don't
 * require browser or stream state, so it earns the most coverage-per-test of any module in the codebase.
 */
import { RECOVERY_METHODS, checkCircuitBreaker, createRecoveryMetrics, formatIssueType, formatRecoveryDuration, formatRecoveryMetricsSummary, getIssueCategory,
  getIssueDescription, getRecoveryMethod, getTotalRecoveryAttempts, recordRecoveryAttempt, recordRecoverySuccess, resetCircuitBreaker } from "./recovery.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import type { CircuitBreakerState } from "./recovery.ts";
import type { VideoState } from "../types/index.ts";
import assert from "node:assert/strict";

/* makeVideoState builds a VideoState literal with sensible defaults. Tests override only the fields they care about, mirroring the factory pattern from the
 * test conventions. We keep this inline rather than a separate streaming.helpers.ts because no other test file currently needs VideoState construction; if a
 * second consumer appears we'll lift it out.
 */
function makeVideoState(overrides: Partial<VideoState> = {}): VideoState {

  return {

    currentTime: 0,
    ended: false,
    error: false,
    muted: false,
    networkState: 1,
    paused: false,
    readyState: 4,
    time: 0,
    videoHeight: 720,
    videoWidth: 1280,
    volume: 1,
    ...overrides
  };
}

describe("RECOVERY_METHODS", () => {

  test("declares the four expected method names with stable string values", () => {

    // The constant is also consumed by recordRecoveryAttempt/recordRecoverySuccess via the ATTEMPT_FIELDS and SUCCESS_FIELDS mappings; changing any value here
    // would break the metrics counter routing, so we lock the string identities.
    assert.equal(RECOVERY_METHODS.pageNavigation, "page navigation", "pageNavigation literal");
    assert.equal(RECOVERY_METHODS.playUnmute, "play/unmute", "playUnmute literal");
    assert.equal(RECOVERY_METHODS.sourceReload, "source reload", "sourceReload literal");
    assert.equal(RECOVERY_METHODS.tabReplacement, "tab replacement", "tabReplacement literal");
  });
});

describe("createRecoveryMetrics", () => {

  test("returns a fresh object with every counter at zero and recovery state cleared", () => {

    const metrics = createRecoveryMetrics();

    assert.equal(metrics.currentRecoveryStartTime, null, "no recovery in progress");
    assert.equal(metrics.currentRecoveryMethod, null, "no method recorded");
    assert.equal(metrics.pageNavigationAttempts, 0);
    assert.equal(metrics.pageNavigationSuccesses, 0);
    assert.equal(metrics.playUnmuteAttempts, 0);
    assert.equal(metrics.playUnmuteSuccesses, 0);
    assert.equal(metrics.sourceReloadAttempts, 0);
    assert.equal(metrics.sourceReloadSuccesses, 0);
    assert.equal(metrics.tabReplacementAttempts, 0);
    assert.equal(metrics.tabReplacementSuccesses, 0);
    assert.equal(metrics.totalRecoveryTimeMs, 0);
  });

  test("returns a distinct object each call (no shared reference)", () => {

    const a = createRecoveryMetrics();
    const b = createRecoveryMetrics();

    a.pageNavigationAttempts = 5;

    assert.equal(b.pageNavigationAttempts, 0, "mutating one instance must not affect another");
  });
});

describe("getTotalRecoveryAttempts", () => {

  test("sums every method's attempt counter", () => {

    const metrics = createRecoveryMetrics();

    metrics.pageNavigationAttempts = 2;
    metrics.playUnmuteAttempts = 3;
    metrics.sourceReloadAttempts = 4;
    metrics.tabReplacementAttempts = 1;

    assert.equal(getTotalRecoveryAttempts(metrics), 10, "sum across all four methods");
  });

  test("returns 0 for fresh metrics", () => {

    assert.equal(getTotalRecoveryAttempts(createRecoveryMetrics()), 0);
  });

  test("ignores success counters (only attempts feed into total)", () => {

    const metrics = createRecoveryMetrics();

    metrics.playUnmuteAttempts = 1;
    metrics.playUnmuteSuccesses = 99;

    assert.equal(getTotalRecoveryAttempts(metrics), 1, "successes must not pollute the attempt total");
  });
});

describe("getIssueDescription", () => {

  test("returns 'paused' for the paused category", () => {

    assert.equal(getIssueDescription("paused"), "paused");
  });

  test("returns 'buffering' for the buffering category", () => {

    assert.equal(getIssueDescription("buffering"), "buffering");
  });

  test("returns 'stalled' for the other category (default branch)", () => {

    assert.equal(getIssueDescription("other"), "stalled");
  });
});

describe("getRecoveryMethod", () => {

  test("level 1 maps to play/unmute", () => {

    assert.equal(getRecoveryMethod(1), RECOVERY_METHODS.playUnmute);
  });

  test("level 2 maps to source reload", () => {

    assert.equal(getRecoveryMethod(2), RECOVERY_METHODS.sourceReload);
  });

  test("level 3 maps to page navigation (default branch)", () => {

    assert.equal(getRecoveryMethod(3), RECOVERY_METHODS.pageNavigation);
  });

  test("level 0 falls through to page navigation (default branch boundary)", () => {

    // Boundary: 0 is below the documented levels (1, 2, 3). The switch's default branch should claim it rather than returning undefined.
    assert.equal(getRecoveryMethod(0), RECOVERY_METHODS.pageNavigation);
  });

  test("negative and large levels also fall through to page navigation", () => {

    // The switch has no upper bound or negative guard; both fall through to default. Locking this keeps the contract explicit.
    assert.equal(getRecoveryMethod(-1), RECOVERY_METHODS.pageNavigation, "negative levels");
    assert.equal(getRecoveryMethod(Number.MAX_SAFE_INTEGER), RECOVERY_METHODS.pageNavigation, "absurdly large levels");
  });
});

describe("recordRecoveryAttempt", () => {

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("increments the attempt counter for the named method", () => {

    const metrics = createRecoveryMetrics();

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.playUnmute);

    assert.equal(metrics.playUnmuteAttempts, 1, "play/unmute counter incremented");
    assert.equal(metrics.pageNavigationAttempts, 0, "other counters untouched");
    assert.equal(metrics.sourceReloadAttempts, 0);
    assert.equal(metrics.tabReplacementAttempts, 0);
  });

  test("records the recovery start time and method on the metrics object", () => {

    const metrics = createRecoveryMetrics();

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.sourceReload);

    assert.equal(metrics.currentRecoveryStartTime, 1_700_000_000_000, "start time captured from Date.now()");
    assert.equal(metrics.currentRecoveryMethod, RECOVERY_METHODS.sourceReload, "current method tracked");
  });

  test("does NOT increment any counter when given an unknown method name", () => {

    // Negative test: the function silently no-ops the counter increment if the method isn't in ATTEMPT_FIELDS. The current/start fields still update because
    // the implementation captures them unconditionally - that's the contract; locking it.
    const metrics = createRecoveryMetrics();

    recordRecoveryAttempt(metrics, "totally-bogus-method");

    assert.equal(getTotalRecoveryAttempts(metrics), 0, "no counter should have moved for unknown method");
    assert.equal(metrics.currentRecoveryMethod, "totally-bogus-method", "current method still tracked verbatim");
  });

  test("counts each call distinctly across repeated invocations", () => {

    const metrics = createRecoveryMetrics();

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.tabReplacement);
    recordRecoveryAttempt(metrics, RECOVERY_METHODS.tabReplacement);
    recordRecoveryAttempt(metrics, RECOVERY_METHODS.tabReplacement);

    assert.equal(metrics.tabReplacementAttempts, 3, "three sequential attempts increment the counter three times");
  });
});

describe("recordRecoverySuccess", () => {

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("increments the success counter and clears in-progress recovery state", () => {

    const metrics = createRecoveryMetrics();

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.playUnmute);
    mock.timers.tick(2_500);
    recordRecoverySuccess(metrics, RECOVERY_METHODS.playUnmute);

    assert.equal(metrics.playUnmuteSuccesses, 1, "success counter incremented");
    assert.equal(metrics.currentRecoveryStartTime, null, "in-progress start cleared");
    assert.equal(metrics.currentRecoveryMethod, null, "in-progress method cleared");
  });

  test("accumulates the elapsed recovery duration into totalRecoveryTimeMs", () => {

    const metrics = createRecoveryMetrics();

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.sourceReload);
    mock.timers.tick(1_000);
    recordRecoverySuccess(metrics, RECOVERY_METHODS.sourceReload);

    mock.timers.tick(50_000);
    recordRecoveryAttempt(metrics, RECOVERY_METHODS.sourceReload);
    mock.timers.tick(2_500);
    recordRecoverySuccess(metrics, RECOVERY_METHODS.sourceReload);

    assert.equal(metrics.totalRecoveryTimeMs, 3_500, "two successful recoveries sum 1000ms + 2500ms");
  });

  test("does NOT accumulate duration when called without a preceding attempt (no start time recorded)", () => {

    // Negative test: if a caller invokes success without first calling attempt, the start time is null. The function must guard against that and not contribute
    // a bogus duration (Date.now() - null would coerce to a huge number).
    const metrics = createRecoveryMetrics();

    recordRecoverySuccess(metrics, RECOVERY_METHODS.playUnmute);

    assert.equal(metrics.totalRecoveryTimeMs, 0, "no duration accumulated when start time was never set");
    assert.equal(metrics.playUnmuteSuccesses, 1, "but the success counter still increments");
  });

  test("ignores unknown method names for counter increment but still clears in-progress state", () => {

    const metrics = createRecoveryMetrics();

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.tabReplacement);
    mock.timers.tick(500);
    recordRecoverySuccess(metrics, "totally-bogus-method");

    assert.equal(metrics.tabReplacementSuccesses, 0, "unknown method does not increment any success counter");
    assert.equal(metrics.currentRecoveryMethod, null, "in-progress state still cleared");
    assert.equal(metrics.totalRecoveryTimeMs, 500, "duration still accumulated since start time was set");
  });
});

describe("formatRecoveryDuration", () => {

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_010_500 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("formats elapsed milliseconds since startTime as seconds with one decimal", () => {

    // 10500ms elapsed -> "10.5s".
    assert.equal(formatRecoveryDuration(1_700_000_000_000), "10.5s");
  });

  test("rounds to the nearest tenth at the boundary", () => {

    // 1499ms -> "1.5s" via toFixed(1).
    assert.equal(formatRecoveryDuration(1_700_000_009_001), "1.5s");
  });

  test("returns 0.0s for a startTime equal to now (zero elapsed)", () => {

    // Boundary: zero elapsed.
    assert.equal(formatRecoveryDuration(1_700_000_010_500), "0.0s");
  });

  test("handles a future startTime (negative elapsed) without throwing", () => {

    // Boundary: clock skew or out-of-order calls. The function does not guard against negative input; we lock the resulting "-N.Ns" rather than crashing.
    assert.equal(formatRecoveryDuration(1_700_000_011_500), "-1.0s");
  });
});

describe("formatRecoveryMetricsSummary", () => {

  test("returns the no-recoveries-needed message when no attempts were recorded", () => {

    assert.equal(formatRecoveryMetricsSummary(createRecoveryMetrics()), "No recoveries needed.");
  });

  test("formats successes with a per-method breakdown and average duration", () => {

    const metrics = createRecoveryMetrics();

    metrics.sourceReloadAttempts = 5;
    metrics.sourceReloadSuccesses = 5;
    metrics.pageNavigationAttempts = 3;
    metrics.pageNavigationSuccesses = 3;
    metrics.totalRecoveryTimeMs = 33_600;

    const summary = formatRecoveryMetricsSummary(metrics);

    assert.match(summary, /Recoveries: 8/, "total successes count");
    assert.match(summary, /5x source reload/, "source-reload breakdown");
    assert.match(summary, /3x page navigation/, "page-navigation breakdown");
    assert.match(summary, /avg 4\.2s/, "average across 8 successes -> 33600ms / 8 = 4200ms");
  });

  test("reports the attempted-but-failed case when there are attempts but zero successes", () => {

    const metrics = createRecoveryMetrics();

    metrics.playUnmuteAttempts = 4;

    assert.equal(formatRecoveryMetricsSummary(metrics), "Recoveries: 4 attempted, 0 succeeded.");
  });

  test("zero average is reported as the attempted-failed case when no successes occurred", () => {

    // Boundary: this exercises the "totalSuccesses === 0" branch alongside the "attempted but never succeeded" message path. The current code reaches the
    // attempted-failed branch because `parts` is empty, so we lock that surfacing.
    const metrics = createRecoveryMetrics();

    metrics.tabReplacementAttempts = 1;

    assert.equal(formatRecoveryMetricsSummary(metrics), "Recoveries: 1 attempted, 0 succeeded.");
  });
});

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

describe("formatIssueType", () => {

  test("returns 'unknown' when no flags are set", () => {

    assert.equal(formatIssueType(makeVideoState(), false, false), "unknown");
  });

  test("reports paused alone when only paused is true", () => {

    assert.equal(formatIssueType(makeVideoState({ paused: true }), false, false), "paused");
  });

  test("reports ended alone when only ended is true", () => {

    assert.equal(formatIssueType(makeVideoState({ ended: true }), false, false), "ended");
  });

  test("reports error alone when only error is true", () => {

    assert.equal(formatIssueType(makeVideoState({ error: true }), false, false), "error");
  });

  test("reports buffering when stalled and isBuffering both hold", () => {

    assert.equal(formatIssueType(makeVideoState(), true, true), "buffering");
  });

  test("reports stalled when stalled holds but isBuffering does not", () => {

    assert.equal(formatIssueType(makeVideoState(), true, false), "stalled");
  });

  test("joins multiple concurrent issues with comma+space", () => {

    // The function appends issues in fixed order: paused, ended, error, buffering/stalled. The output mirrors that order.
    const result = formatIssueType(makeVideoState({ ended: true, paused: true }), true, true);

    assert.equal(result, "paused, ended, buffering");
  });
});

describe("getIssueCategory", () => {

  test("returns 'other' when error is set (highest priority)", () => {

    assert.equal(getIssueCategory(makeVideoState({ error: true, paused: true }), true, true), "other", "error wins over every other flag");
  });

  test("returns 'other' when ended is set (highest priority alongside error)", () => {

    assert.equal(getIssueCategory(makeVideoState({ ended: true, paused: true }), true, true), "other", "ended wins over every other flag");
  });

  test("returns 'buffering' when isBuffering is true and no error/ended is set", () => {

    assert.equal(getIssueCategory(makeVideoState(), false, true), "buffering");
  });

  test("returns 'buffering' when stalled with low readyState", () => {

    // readyState < 3 with stalled=true treats as effective buffering even without isBuffering=true.
    assert.equal(getIssueCategory(makeVideoState({ readyState: 2 }), true, false), "buffering");
  });

  test("returns 'paused' when paused is the only signal (and not buffering)", () => {

    assert.equal(getIssueCategory(makeVideoState({ paused: true }), false, false), "paused");
  });

  test("returns 'buffering' when stalled at readyState=3 (no other signals)", () => {

    // The current implementation hits the "stalled without low readyState" fallthrough and routes to buffering. Lock the contract.
    assert.equal(getIssueCategory(makeVideoState({ readyState: 3 }), true, false), "buffering");
  });

  test("returns 'other' when nothing is set (catch-all)", () => {

    // Boundary: a clean state with no flags should not trigger any recovery; the category falls through to "other".
    assert.equal(getIssueCategory(makeVideoState(), false, false), "other");
  });

  test("buffering wins over paused when both are set", () => {

    // The order in the function is: error/ended -> buffering -> stalled+lowReady -> paused. So buffering is checked first and short-circuits.
    assert.equal(getIssueCategory(makeVideoState({ paused: true }), false, true), "buffering");
  });
});
