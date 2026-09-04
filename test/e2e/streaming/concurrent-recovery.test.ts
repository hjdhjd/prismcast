/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * concurrent-recovery.test.ts: Asserts the per-stream isolation rule for the recovery state machine. PrismCast's design philosophy demands that "a problem
 * with one stream should never affect other streams" - the canonical statement of failure isolation in this codebase. The recovery layer realizes that
 * philosophy by making the state objects (RecoveryMetrics, CircuitBreakerState) per-stream values that the monitor instantiates one-per-stream and that the
 * helpers in src/streaming/recovery.ts mutate by parameter, never via module-level singletons. A regression that introduced shared state - a global breaker
 * count, a static metrics counter, an internal cache keyed by anything other than the passed-in object reference - would silently couple two streams' failure
 * modes: one stream's escalation would advance another's; one stream's tripped breaker would terminate another; one stream's reset would clear another's
 * accumulated counters.
 *
 * Phase 1's recovery-escalation.test.ts asserts the single-stream sequence (escalation, accumulation, trip-and-reset). This suite asserts the independent axis: with
 * two streams driven through state mutations in interleaved order, neither's state must influence the other's. The integration value here is precisely what
 * the unit suite cannot exercise alone - the unit suite tests one state object at a time, by construction. Independence-under-concurrency is not visible at
 * that resolution.
 *
 * Architectural notes (mirroring recovery-escalation.test.ts so the precedent is followed exactly):
 *
 *   1. recovery.ts is intentionally Clock-port-free. We use mock.timers.enable({ apis: ["Date"] }) to control Date.now() inside recordRecoveryAttempt and
 *      recordRecoverySuccess, and pass an explicit `now` argument into checkCircuitBreaker - the same shape the prior recovery suite uses.
 *
 *   2. The harness (createIntegrationContext / initializePersistence) is omitted on purpose. recovery.ts has no module-level singletons, no persistence, and no
 *      I/O - that is in fact the guarantee under test. Importing the harness would introduce filesystem state that is irrelevant to per-stream isolation and
 *      would mislead a reader into expecting it to matter.
 *
 *   3. "Concurrent" here is interleaved synchronous mutation, not parallel async execution. Recovery state mutators are synchronous and deterministic; the
 *      regression class to catch is shared-by-reference state, which surfaces under interleaved mutation regardless of whether the calls are async.
 */
import type { CircuitBreakerState, RecoveryMetrics } from "../../../src/streaming/recovery.ts";
import { RECOVERY_METHODS, checkCircuitBreaker, createRecoveryMetrics, getRecoveryMethod, recordRecoveryAttempt, recordRecoverySuccess,
  resetCircuitBreaker } from "../../../src/streaming/recovery.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { CONFIG } from "../../../src/config/index.ts";
import assert from "node:assert/strict";

describe("recovery state machine - per-stream isolation across concurrent streams", () => {

  // Same baseline as recovery-escalation.test.ts so test-failure timestamps are consistent across both tiers.
  const baseTime = 1700000000000;

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: baseTime });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  function freshBreaker(): CircuitBreakerState {

    return { firstFailureTime: null, totalFailureCount: 0 };
  }

  test("two streams escalate independently; advancing stream A through L1->L2->L3 leaves stream B's metrics at zero", async () => {

    /* Two streams each have their own RecoveryMetrics object. The monitor in production constructs one-per-stream and never threads them across streams; the
     * helpers in recovery.ts take a metrics argument and mutate it in place. The rule: writes to streamA's metrics must not be visible in streamB's
     * metrics, and vice versa. A regression that introduced a singleton (e.g., a module-level "lastRecoveryMethod") would break this immediately - either
     * streamB would see streamA's increments, or streamA's reads would surface streamB's last write.
     *
     * We escalate stream A through L1, L2, L3 with attempt records, and assert that stream B's per-method counters all remain zero. The interleaving (alternate
     * mutations against A and a no-op observation against B) is what would catch any code path that "helpfully" copied state across instances.
     */
    const streamA: RecoveryMetrics = createRecoveryMetrics();
    const streamB: RecoveryMetrics = createRecoveryMetrics();

    // Sanity: independent object references at construction. A factory regression that returned a shared singleton would fail here before any mutation.
    assert.notEqual(streamA, streamB, "createRecoveryMetrics must return distinct objects for distinct callers");

    recordRecoveryAttempt(streamA, getRecoveryMethod(1));
    mock.timers.tick(800);
    recordRecoveryAttempt(streamA, getRecoveryMethod(2));
    mock.timers.tick(2000);
    recordRecoveryAttempt(streamA, getRecoveryMethod(3));

    // Stream A's counters reflect the three attempts, one per level.
    assert.equal(streamA.playUnmuteAttempts, 1, "stream A's L1 attempt counter incremented");
    assert.equal(streamA.sourceReloadAttempts, 1, "stream A's L2 attempt counter incremented");
    assert.equal(streamA.pageNavigationAttempts, 1, "stream A's L3 attempt counter incremented");

    // Stream B has not been touched - every field that recordRecoveryAttempt mutates remains at its initial value. We assert the four attempt counters plus the
    // two current-recovery markers (currentRecoveryMethod, currentRecoveryStartTime) - the full set of fields that an attempt write touches - so a bug that
    // leaks via any of them surfaces with a precise assertion message. The success counters and totalRecoveryTimeMs are not exercised by attempts, so they are
    // not asserted here.
    assert.equal(streamB.playUnmuteAttempts, 0, "stream B's L1 counter must remain 0 - stream A's escalation must not leak into stream B");
    assert.equal(streamB.sourceReloadAttempts, 0, "stream B's L2 counter must remain 0");
    assert.equal(streamB.pageNavigationAttempts, 0, "stream B's L3 counter must remain 0");
    assert.equal(streamB.tabReplacementAttempts, 0, "stream B's tab-replacement counter must remain 0");
    assert.equal(streamB.currentRecoveryMethod, null, "stream B's currentRecoveryMethod must remain null - stream A's in-progress state must not leak");
    assert.equal(streamB.currentRecoveryStartTime, null, "stream B's currentRecoveryStartTime must remain null");

    // The contrapositive: now drive stream B through a single L1 attempt and confirm that streamA's L1 counter does not double-count.
    recordRecoveryAttempt(streamB, getRecoveryMethod(1));

    assert.equal(streamB.playUnmuteAttempts, 1, "stream B's L1 attempt counter increments on its own attempt");
    assert.equal(streamA.playUnmuteAttempts, 1, "stream A's L1 counter must remain 1 - stream B's attempt must not leak back into stream A");
  });

  test("circuit breaker trips per-stream: pushing stream A past threshold leaves stream B's breaker untouched", async () => {

    /* The per-stream isolation rule for breakers. Production policy: each stream has its own CircuitBreakerState; the monitor calls checkCircuitBreaker against the
     * per-stream state, never against a shared one. A regression that aliased the breaker (e.g., a module-level WeakMap that mis-keyed) would surface here as
     * stream B's breaker carrying stream A's failure count.
     *
     * We drive stream A to the trip threshold and assert: stream A's result.shouldTrip is true on the threshold-th call; stream A's totalFailureCount equals
     * threshold; and stream B's state is byte-equivalent to a freshly-constructed breaker (totalFailureCount=0, firstFailureTime=null). The byte-equivalent
     * check is the strongest available - any leak (count, timestamp, or both) surfaces as a non-equal assertion.
     */
    const breakerA = freshBreaker();
    const breakerB = freshBreaker();
    const threshold = CONFIG.recovery.circuitBreakerThreshold;

    // Drive breakerA to the threshold. Each failure is recorded one millisecond apart so they all fall inside the configured window. The final call must trip;
    // the calls before it must not.
    for(let i = 0; i < (threshold - 1); i++) {

      checkCircuitBreaker(breakerA, baseTime + i);
    }

    const trippingResult = checkCircuitBreaker(breakerA, baseTime + threshold);

    assert.equal(trippingResult.shouldTrip, true, "stream A's breaker trips at the threshold-th failure");
    assert.equal(breakerA.totalFailureCount, threshold, "stream A's breaker count equals the threshold post-trip");

    // Stream B's breaker is byte-equivalent to a fresh breaker - never recorded a failure.
    assert.deepEqual(breakerB, freshBreaker(), "stream B's breaker must be untouched - stream A's failures must not leak into stream B");

    // Confirm the converse direction: a single failure on stream B advances B's count without touching A's tripped state. (A trip does not "lock" the
    // breaker structurally; it just reports shouldTrip=true. Production decides what to do based on that flag.)
    const bResult = checkCircuitBreaker(breakerB, baseTime + threshold + 1);

    assert.equal(bResult.shouldTrip, false, "stream B's first failure does not trip - it has its own window");
    assert.equal(breakerB.totalFailureCount, 1, "stream B's count is now 1 - independent of stream A");
    assert.equal(breakerA.totalFailureCount, threshold, "stream A's count is still at the threshold - stream B's failure must not affect stream A");
  });

  test("resetting one stream's breaker does not affect another stream's breaker or its metrics", async () => {

    /* Both streams have escalated and accumulated state - both metrics carry attempts, both breakers carry counts. Calling resetCircuitBreaker on stream A's
     * breaker must zero only that breaker. Stream A's metrics, stream B's breaker, and stream B's metrics must all be unchanged. This asserts the four-way
     * non-coupling: (A.breaker -> A.metrics, A.breaker -> B.breaker, A.breaker -> B.metrics).
     *
     * The test shape follows the metric/breaker independence test in recovery-escalation.test.ts but extends it across two streams. A regression that "helpfully"
     * cleared related state on a reset call (e.g., zeroing metrics on the same stream, or sympathetically resetting any breaker with a matching firstFailureTime)
     * fails one of the four assertions below.
     */
    const metricsA: RecoveryMetrics = createRecoveryMetrics();
    const metricsB: RecoveryMetrics = createRecoveryMetrics();
    const breakerA = freshBreaker();
    const breakerB = freshBreaker();

    // Both streams accumulate one attempt each in their metrics, and one failure each in their breakers.
    recordRecoveryAttempt(metricsA, getRecoveryMethod(1));
    mock.timers.tick(500);
    recordRecoverySuccess(metricsA, RECOVERY_METHODS.playUnmute);
    checkCircuitBreaker(breakerA, baseTime);

    recordRecoveryAttempt(metricsB, getRecoveryMethod(2));
    mock.timers.tick(500);
    recordRecoverySuccess(metricsB, RECOVERY_METHODS.sourceReload);
    checkCircuitBreaker(breakerB, baseTime + 100);

    // Snapshot the fields that should NOT change after the reset. We use a structural snapshot rather than per-field assertions so a regression that touches a
    // field we forgot to assert on still surfaces.
    const breakerBSnapshot = { ...breakerB };
    const metricsASnapshot = { ...metricsA };
    const metricsBSnapshot = { ...metricsB };

    // The reset under test: only stream A's breaker.
    resetCircuitBreaker(breakerA);

    // Direct effect: stream A's breaker is zeroed.
    assert.equal(breakerA.firstFailureTime, null, "stream A's breaker firstFailureTime cleared by reset");
    assert.equal(breakerA.totalFailureCount, 0, "stream A's breaker totalFailureCount zeroed by reset");

    // Non-effects: every other piece of state is byte-equivalent to its pre-reset snapshot.
    assert.deepEqual(breakerB, breakerBSnapshot, "stream B's breaker must be unchanged by stream A's reset");
    assert.deepEqual(metricsA, metricsASnapshot, "stream A's metrics must be unchanged by its own breaker reset");
    assert.deepEqual(metricsB, metricsBSnapshot, "stream B's metrics must be unchanged by stream A's breaker reset");
  });
});
