/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * recovery.metrics.test.ts: Unit tests for recovery metrics tracking - createRecoveryMetrics, getTotalRecoveryAttempts, recordRecoveryAttempt,
 * recordRecoverySuccess, formatRecoveryDuration, and formatRecoveryMetricsSummary. Issue classification helpers live in recovery.test.ts; circuit-breaker
 * primitives live in recovery.circuitBreaker.test.ts.
 */
import { RECOVERY_METHODS, createRecoveryMetrics, formatRecoveryDuration, formatRecoveryMetricsSummary, getTotalRecoveryAttempts, recordRecoveryAttempt,
  recordRecoverySuccess } from "./recovery.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import assert from "node:assert/strict";

// The reference instant every mocked clock in this file counts from, so a row's expected timestamps read as offsets rather than absolute epochs.
const BASE_TIME_MS = 1700000000000;

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
describe("recordRecoveryAttempt", () => {

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: BASE_TIME_MS });
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

    assert.equal(metrics.currentRecoveryStartTime, BASE_TIME_MS, "start time captured from Date.now()");
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

    mock.timers.enable({ apis: ["Date"], now: BASE_TIME_MS });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("increments the success counter and clears in-progress recovery state", () => {

    const metrics = createRecoveryMetrics();

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.playUnmute);
    mock.timers.tick(2500);
    recordRecoverySuccess(metrics, RECOVERY_METHODS.playUnmute);

    assert.equal(metrics.playUnmuteSuccesses, 1, "success counter incremented");
    assert.equal(metrics.currentRecoveryStartTime, null, "in-progress start cleared");
    assert.equal(metrics.currentRecoveryMethod, null, "in-progress method cleared");
  });

  test("accumulates the elapsed recovery duration into totalRecoveryTimeMs", () => {

    const metrics = createRecoveryMetrics();

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.sourceReload);
    mock.timers.tick(1000);
    recordRecoverySuccess(metrics, RECOVERY_METHODS.sourceReload);

    mock.timers.tick(50000);
    recordRecoveryAttempt(metrics, RECOVERY_METHODS.sourceReload);
    mock.timers.tick(2500);
    recordRecoverySuccess(metrics, RECOVERY_METHODS.sourceReload);

    assert.equal(metrics.totalRecoveryTimeMs, 3500, "two successful recoveries sum 1000ms + 2500ms");
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

    mock.timers.enable({ apis: ["Date"], now: 1700000010500 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("formats elapsed milliseconds since startTime as seconds with one decimal", () => {

    // 10500ms elapsed -> "10.5s".
    assert.equal(formatRecoveryDuration(BASE_TIME_MS), "10.5s");
  });

  test("rounds to the nearest tenth at the boundary", () => {

    // 1499ms -> "1.5s" via toFixed(1).
    assert.equal(formatRecoveryDuration(1700000009001), "1.5s");
  });

  test("returns 0.0s for a startTime equal to now (zero elapsed)", () => {

    // Boundary: zero elapsed.
    assert.equal(formatRecoveryDuration(1700000010500), "0.0s");
  });

  test("handles a future startTime (negative elapsed) without throwing", () => {

    // Boundary: clock skew or out-of-order calls. The function does not guard against negative input; we lock the resulting "-N.Ns" rather than crashing.
    assert.equal(formatRecoveryDuration(1700000011500), "-1.0s");
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
    metrics.totalRecoveryTimeMs = 33600;

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
