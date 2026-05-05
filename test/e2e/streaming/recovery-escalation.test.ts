/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * recovery-escalation.test.ts: Integration coverage for the recovery state machine viewed as a sequence rather than a set of isolated functions. The unit
 * tier (src/streaming/recovery.test.ts) pins each function's behavior in isolation; this suite pins the orchestration the playback monitor performs at run
 * time - escalation through recovery levels, accumulation across multiple cycles, the trip-and-reset arc on the circuit breaker, and the architectural
 * separation between metrics state and circuit-breaker state. The regression class this catches: someone reorders or merges the recovery state objects in a
 * way that passes the unit suite (each function still works in isolation) but corrupts the multi-step flow that production actually runs.
 *
 * Architectural notes the implementation surfaced (worth recording so the next reader does not re-derive them):
 *
 *   1. recovery.ts is intentionally Clock-port-free. The Clock seam in src/utils/clock.ts is reserved for nested async chains where mock.timers.tick cannot
 *      drain the runtime - retryOperation is the canonical case. recovery.ts uses Date.now() in shallow synchronous contexts (recordRecoveryAttempt /
 *      recordRecoverySuccess for elapsed-time accumulation) and accepts a `now` argument in checkCircuitBreaker. Both shapes are tested via mock.timers.enable
 *      and synthetic timestamps respectively - exactly the pattern the Clock-port docstring recommends as the default.
 *
 *   2. The "60-second sustained-healthy reset" lives in monitor.ts, not recovery.ts. The monitor observes sustained playback and then calls
 *      resetCircuitBreaker(state) as its policy decision. From recovery.ts's perspective, "reset" is just the explicit function call - the time observation
 *      is monitor-side state machinery. This suite pins the recovery-side hook (resetCircuitBreaker collapses the breaker to a fresh state) and treats the
 *      monitor's time policy as out-of-scope. A separate suite exercising monitor.ts orchestration would need a Page stub and is not built here.
 *
 *   3. The harness (createIntegrationContext / initializePersistence) is omitted on purpose. recovery.ts has no module-level singletons, no persistence, and
 *      no I/O. Importing the harness for consistency would obscure what is being tested - a reader would expect filesystem state to matter, find none, and
 *      have to reverse-engineer the actual scope. Better to keep the test surface honest: this is a sequence-driven integration test of pure state mutators.
 */
import type { CircuitBreakerState, RecoveryMetrics } from "../../../src/streaming/recovery.ts";
import { RECOVERY_METHODS, checkCircuitBreaker, createRecoveryMetrics, getRecoveryMethod, getTotalRecoveryAttempts, recordRecoveryAttempt,
  recordRecoverySuccess, resetCircuitBreaker } from "../../../src/streaming/recovery.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { CONFIG } from "../../../src/config/index.ts";
import assert from "node:assert/strict";

describe("recovery state machine - escalation, accumulation, and breaker reset", () => {

  // The unit suite uses 1_700_000_000_000 as its baseline; we match that so test failures across both tiers anchor on the same wall-clock for consistency.
  const baseTime = 1_700_000_000_000;

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: baseTime });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  function freshBreaker(): CircuitBreakerState {

    return { firstFailureTime: null, totalFailureCount: 0 };
  }

  test("a single recovery cycle escalates L1 -> L2 -> L3 with method-specific metrics accumulating along the way", async () => {

    /* The monitor's escalation flow when each lower level fails to restore playback. Production sequences this as: detect issue, record an L1 attempt
     * (play/unmute), observe that recovery did not restore healthy playback, escalate to L2 (source reload), record that attempt, observe failure again,
     * escalate to L3 (page navigation), record that attempt, observe success, record the success against L3. The integration value over the unit suite is
     * pinning the *temporal sequence* of state mutations - each step's accumulated state must reflect everything before it.
     */
    const metrics: RecoveryMetrics = createRecoveryMetrics();

    // L1 attempt: lowest-cost recovery, tried first for paused/buffering issues that often clear from a play() call. After this call, current method tracks
    // play/unmute; one play/unmute attempt counted; nothing else moved.
    recordRecoveryAttempt(metrics, getRecoveryMethod(1));

    assert.equal(metrics.currentRecoveryMethod, RECOVERY_METHODS.playUnmute, "L1 maps to play/unmute");
    assert.equal(metrics.playUnmuteAttempts, 1, "L1 attempt incremented play/unmute counter");
    assert.equal(metrics.currentRecoveryStartTime, baseTime, "L1 attempt captures start time");

    // 800ms passes; L1 did not restore healthy playback. The monitor escalates to L2 without recording an L1 success.
    mock.timers.tick(800);

    recordRecoveryAttempt(metrics, getRecoveryMethod(2));

    assert.equal(metrics.currentRecoveryMethod, RECOVERY_METHODS.sourceReload, "escalation flips current method to source reload");
    assert.equal(metrics.sourceReloadAttempts, 1, "L2 attempt incremented source-reload counter");
    assert.equal(metrics.playUnmuteAttempts, 1, "L1's counter is unchanged by L2 escalation");
    assert.equal(metrics.currentRecoveryStartTime, baseTime + 800, "L2 attempt captures the post-tick start time");

    // 2000ms passes; L2 also failed. Escalate to L3.
    mock.timers.tick(2_000);

    recordRecoveryAttempt(metrics, getRecoveryMethod(3));

    assert.equal(metrics.currentRecoveryMethod, RECOVERY_METHODS.pageNavigation, "L3 maps to page navigation");
    assert.equal(metrics.pageNavigationAttempts, 1, "L3 attempt incremented page-navigation counter");

    // 1500ms passes; L3 succeeds. The monitor records a success against the page-navigation method, which clears the in-progress fields.
    mock.timers.tick(1_500);

    recordRecoverySuccess(metrics, RECOVERY_METHODS.pageNavigation);

    assert.equal(metrics.currentRecoveryMethod, null, "success clears the in-progress method");
    assert.equal(metrics.currentRecoveryStartTime, null, "success clears the in-progress start time");
    assert.equal(metrics.pageNavigationSuccesses, 1, "the L3 success counter increments");
    assert.equal(metrics.playUnmuteSuccesses, 0, "L1's success counter remains zero (L1 never restored healthy playback)");
    assert.equal(metrics.sourceReloadSuccesses, 0, "L2's success counter remains zero (L2 never restored healthy playback)");

    // Total attempts across all methods reflects the three escalations exactly.
    assert.equal(getTotalRecoveryAttempts(metrics), 3, "exactly three attempts recorded across the cycle");

    // totalRecoveryTimeMs accumulates only on success and only since the most recent attempt's start time. The L3 attempt started at baseTime+2800 and
    // succeeded 1500ms later, so the duration credited is 1500ms - L1 and L2 do not contribute because they had no recordRecoverySuccess paired with them.
    assert.equal(metrics.totalRecoveryTimeMs, 1_500, "duration credited equals only the L3 attempt-to-success delta");
  });

  test("two consecutive recovery cycles accumulate metrics; nothing resets between cycles unless a reset call happens", async () => {

    /* Production's monitor never resets metrics on its own - the metrics object lives for the stream's lifetime and feeds the termination summary. This test
     * pins that lifetime semantics: a successful cycle followed by a healthy interval followed by a second cycle leaves the per-method counters and total
     * duration as the SUM of both cycles.
     */
    const metrics: RecoveryMetrics = createRecoveryMetrics();

    // Cycle 1: L1 succeeds in 600ms.
    recordRecoveryAttempt(metrics, getRecoveryMethod(1));
    mock.timers.tick(600);
    recordRecoverySuccess(metrics, RECOVERY_METHODS.playUnmute);

    // Healthy interval - 30 seconds of clean playback. Time advances; no metrics calls.
    mock.timers.tick(30_000);

    // Cycle 2: L1 fails (no success), escalates to L2 which succeeds in 2000ms.
    recordRecoveryAttempt(metrics, getRecoveryMethod(1));
    mock.timers.tick(500);
    recordRecoveryAttempt(metrics, getRecoveryMethod(2));
    mock.timers.tick(2_000);
    recordRecoverySuccess(metrics, RECOVERY_METHODS.sourceReload);

    // Both cycles' attempts are reflected.
    assert.equal(metrics.playUnmuteAttempts, 2, "two L1 attempts across both cycles");
    assert.equal(metrics.sourceReloadAttempts, 1, "one L2 attempt in cycle 2");
    assert.equal(getTotalRecoveryAttempts(metrics), 3, "three attempts total");

    // Successes: L1 in cycle 1, L2 in cycle 2. L1 in cycle 2 had no success.
    assert.equal(metrics.playUnmuteSuccesses, 1, "one L1 success (cycle 1)");
    assert.equal(metrics.sourceReloadSuccesses, 1, "one L2 success (cycle 2)");

    // Total duration is the sum of cycle 1's L1 (600ms) and cycle 2's L2 (2000ms). Cycle 2's L1 had no success so its duration is not credited.
    assert.equal(metrics.totalRecoveryTimeMs, 2_600, "totalRecoveryTimeMs sums both successful recoveries");
  });

  test("circuit breaker accumulates failures within the window, trips at threshold, and starts fresh after an explicit reset", async () => {

    /* The breaker's role: terminate the stream when recovery has clearly stopped helping. Production policy: monitor records every recovery failure into the
     * breaker; once the threshold count is reached within the window, the breaker trips and the stream terminates. After sustained healthy playback (the
     * 60-second policy in monitor.ts), the monitor calls resetCircuitBreaker - clearing the count so a fresh failure window can begin. This test pins that
     * trip-and-reset arc as a sequence: count up to the trip; then reset; then verify the next failure starts a clean window.
     */
    const breaker = freshBreaker();
    const threshold = CONFIG.recovery.circuitBreakerThreshold;

    // Drive failures one millisecond apart so they all fall comfortably inside the window. The threshold-1th call must NOT trip; the threshold-th call must.
    let lastResult = checkCircuitBreaker(breaker, baseTime);

    assert.equal(lastResult.shouldTrip, false, "the very first failure does not trip");
    assert.equal(breaker.firstFailureTime, baseTime, "first failure populates the window's start timestamp");

    for(let i = 1; i < (threshold - 1); i++) {

      lastResult = checkCircuitBreaker(breaker, baseTime + i);

      assert.equal(lastResult.shouldTrip, false, "failures below threshold do not trip (count " + String(i + 1) + ")");
    }

    // Threshold-th failure - this must trip. Diagnostic info on the result mirrors the breaker state.
    lastResult = checkCircuitBreaker(breaker, baseTime + threshold);

    assert.equal(lastResult.shouldTrip, true, "the threshold-th failure trips the breaker");
    assert.equal(lastResult.totalCount, threshold, "totalCount on the trip result equals the threshold");
    assert.equal(lastResult.withinWindow, true, "the trip occurred inside the window");
    assert.equal(breaker.totalFailureCount, threshold, "breaker state count equals the threshold post-trip");

    // The monitor's "60s sustained-healthy" policy fires - we simulate that by ticking time forward (only relevant for narrative completeness; the breaker's
    // internal logic is time-agnostic) and then calling the explicit reset hook.
    mock.timers.tick(60_000);

    resetCircuitBreaker(breaker);

    assert.equal(breaker.firstFailureTime, null, "post-reset window start is cleared");
    assert.equal(breaker.totalFailureCount, 0, "post-reset count is zeroed");

    // A new failure after reset must start a fresh window with count=1, no trip.
    const postReset = checkCircuitBreaker(breaker, baseTime + 60_000 + 1);

    assert.equal(postReset.shouldTrip, false, "the first failure after reset does not trip");
    assert.equal(postReset.totalCount, 1, "post-reset count starts at 1, not at threshold-1");
    assert.equal(breaker.firstFailureTime, baseTime + 60_000 + 1, "first-failure timestamp anchors a fresh window");
  });

  test("metrics state and circuit-breaker state are independent: recording a recovery success does not touch the breaker", async () => {

    /* The architectural separation: metrics track per-method attempt/success counters; the breaker tracks failure-count-within-window. A recovery success
     * absolutely does NOT mean "the breaker should clear" - the breaker counts the FAILURE that preceded the recovery, not the recovery itself, and only
     * clears on the monitor's explicit reset call after sustained healthy playback. This test pins the non-coupling so a refactor that "helpfully" decrements
     * the breaker on success would fail loudly here.
     */
    const metrics: RecoveryMetrics = createRecoveryMetrics();
    const breaker = freshBreaker();

    // Two failures land in the breaker, mirroring two recovery cycles whose root issues counted as failures from the monitor's perspective.
    checkCircuitBreaker(breaker, baseTime);
    checkCircuitBreaker(breaker, baseTime + 100);

    assert.equal(breaker.totalFailureCount, 2, "two failures recorded in the breaker before any recovery success");

    // A recovery cycle now succeeds: L1 attempt, then L1 success.
    recordRecoveryAttempt(metrics, getRecoveryMethod(1));
    mock.timers.tick(750);
    recordRecoverySuccess(metrics, RECOVERY_METHODS.playUnmute);

    assert.equal(metrics.playUnmuteSuccesses, 1, "metrics reflect the recovery success");

    // The breaker count is unchanged - recovery success does not decrement, clear, or otherwise touch it. Only resetCircuitBreaker can do that, and only the
    // monitor decides when to call it.
    assert.equal(breaker.totalFailureCount, 2, "recovery success does NOT decrement the breaker count");
    assert.equal(breaker.firstFailureTime, baseTime, "recovery success does NOT clear the window's start timestamp");

    // Confirm the converse: resetting the breaker does not touch the metrics either.
    resetCircuitBreaker(breaker);

    assert.equal(metrics.playUnmuteSuccesses, 1, "breaker reset does NOT touch the metrics counters");
    assert.equal(metrics.totalRecoveryTimeMs, 750, "breaker reset does NOT touch the accumulated recovery time");
  });
});
