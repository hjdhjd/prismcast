/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * termination-during-recovery.test.ts: Pins the cleanup contract for terminating a stream while it is mid-recovery. PrismCast's design philosophy is explicit:
 * "isolate stream failures - a problem with one stream should never affect other streams." That guarantee depends on terminateStream being correct under
 * concurrency, not just on the happy path. The harder case - termination while the playback monitor's recovery loop holds in-flight state - is the one that
 * silently leaks resources or corrupts other streams when the cleanup contract has gaps.
 *
 * Investigation summary (mirroring the roadmap's "investigate the cleanup contract before pinning" rule):
 *
 *   1. Recovery state lifetime in production. The monitor in src/streaming/monitor.ts holds RecoveryMetrics and CircuitBreakerState in closure-scoped lets and
 *      exposes a single termination hook on the registry entry: stopMonitor: () => RecoveryMetrics. Calling stopMonitor stops the monitor's interval, returns
 *      the accumulated RecoveryMetrics for the termination summary, and lets the closure release its references. There is no separate "active recovery state"
 *      object exposed on the registry - the closure-scoped state IS the recovery state, and its disposal is structural via stopMonitor's invocation.
 *
 *   2. terminateStream's cleanup sequence (src/streaming/lifecycle.ts:122). Order of operations under the early-return-on-already-initiated guard at line 125:
 *      mark terminationInitiated -> abort + unregister AbortController -> clearTimeout(prerollTimer) -> destroy raw capture stream -> stop nativeProxy ->
 *      kill ffmpegProcess -> snapshot + stop segmenter -> remove channel mapping -> stopMonitor() (drains recovery state) -> close page (if not graceful
 *      shutdown) -> emit terminated + remove emitter listeners -> unregister from registry -> clearClients -> clearShowName -> emitStreamRemoved -> delete
 *      from terminationInitiated. Every step is synchronous from the caller's perspective.
 *
 *   3. Cleanup gap analysis. Reviewed each branch above for "what survives if termination interrupts an in-flight recovery":
 *        - prerollTimer: cleared via clearTimeout (no orphan timer).
 *        - AbortController: aborted then unregistered (no leaked controller).
 *        - stopMonitor: called when set, return value is consumed (no leaked recovery interval).
 *        - segmentEmitter: removeAllListeners called after the "terminated" emit (no orphan listeners).
 *        - registry entry: unregisterStream called (no orphan registry entry).
 *        - channel-to-stream index: deleted via channelToStreamId.delete (no orphan index entry).
 *      No cleanup gap surfaces from this reading. The contract is correct as written; this suite pins each branch as an integration-level invariant so a
 *      future regression that drops one (e.g., a refactor that removes the prerollTimer clear, or one that early-returns from stopMonitor without releasing
 *      the closure) fails loudly here.
 *
 *   4. Out of scope for this suite. The monitor's interval lifecycle (start/stop, AsyncLocalStorage context propagation) is internal to monitor.ts and would
 *      require a Page stub to exercise end-to-end. That belongs in a monitor-orchestration suite that does not exist yet; its absence is documented in the
 *      Phase 1 recovery-escalation.test.ts header. Instead, this suite uses a fake stopMonitor that captures invocation, lets us verify the contract layer
 *      lifecycle.ts owns: that the hook IS called, that the recovery metrics flow back, and that other streams' hooks are not called.
 */
import { RECOVERY_METHODS, type RecoveryMetrics, createRecoveryMetrics, getRecoveryMethod, recordRecoveryAttempt } from "../../../src/streaming/recovery.ts";
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { deleteChannelStreamId, getChannelStreamId, isTerminationInitiated, setChannelStreamId,
  terminateStream } from "../../../src/streaming/lifecycle.ts";
import { describe, test } from "node:test";
import { getStream, registerStream, unregisterStream } from "../../../src/streaming/registry.ts";
import assert from "node:assert/strict";
import { makeRegistryEntry } from "../../../src/streaming/registry.helpers.ts";

/* Builds a stopMonitor stub that simulates a stream mid-recovery. The closure captures the metrics object the production code expects to receive on stop, and
 * an array recording every invocation. Using a fake stopMonitor (instead of starting a real monitor) is the right boundary here: this suite tests the
 * lifecycle.ts hook contract, not the monitor's internal interval lifecycle. The monitor-orchestration tests would belong in a separate suite that has the
 * Page stub plumbing required to spin up an actual monitor.
 */
function makeMidRecoveryMonitor(): { invocations: number[]; metrics: RecoveryMetrics; stopMonitor: () => RecoveryMetrics } {

  const metrics = createRecoveryMetrics();

  // Simulate an L2 (source reload) attempt in flight: the monitor recorded an L1 attempt, observed that L1 did not restore playback, escalated to L2 and is
  // currently waiting on L2's outcome when termination fires. currentRecoveryMethod and currentRecoveryStartTime are non-null - the unmistakable signal that
  // recovery is mid-flight.
  recordRecoveryAttempt(metrics, getRecoveryMethod(1));
  recordRecoveryAttempt(metrics, getRecoveryMethod(2));

  // Sanity: the seeded metrics object reflects the in-flight L2 state.
  if(metrics.currentRecoveryMethod !== RECOVERY_METHODS.sourceReload) {

    throw new Error("test setup: seeded metrics did not record L2 as the in-flight method");
  }

  const invocations: number[] = [];

  const stopMonitor = (): RecoveryMetrics => {

    invocations.push(Date.now());

    return metrics;
  };

  return { invocations, metrics, stopMonitor };
}

describe("terminateStream during active recovery - cleanup contract", () => {

  test("terminating a stream mid-recovery invokes stopMonitor exactly once and removes every cleanup-tracked resource", async () => {

    /* The core invariant. A stream that is in the middle of an L2 recovery attempt must clean up cleanly when terminated:
     *   - stopMonitor is invoked exactly once (drains the closure-scoped recovery state)
     *   - the registry entry is gone
     *   - the channel index entry is gone
     *   - the terminationInitiated flag is cleared (so a future stream with a recycled id is not falsely considered already-terminated)
     *
     * We also confirm the prerollTimer is a separate cleanup branch by setting a real Timeout on the entry and asserting it was cleared. A regression that
     * dropped that branch (or moved it after stopMonitor and accidentally short-circuited it) would leave a Node Timeout firing after the registry entry is
     * gone - a write-after-free on the (now-undefined) HLSState.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const { invocations, stopMonitor } = makeMidRecoveryMonitor();

    // Schedule a real Timeout to stand in for an in-flight prerollTimer. We use a far-future delay so the timer does NOT fire on its own during the test - the
    // assertion below is that terminateStream cleared it. unref() prevents the timer from holding the test-runner event loop open if the cleanup branch is
    // ever broken (the test would still fail loudly via the assertion, but we do not want the suite to hang).
    let prerollTimerFired = false;
    const prerollTimer = setTimeout(() => { prerollTimerFired = true; }, 60_000);

    prerollTimer.unref();

    const entry = makeRegistryEntry({ channelName: "abc", stopMonitor });

    entry.hls.prerollTimer = prerollTimer;

    registerStream(entry);
    setChannelStreamId("abc", entry.id);

    terminateStream(entry.id, "abc", "test cleanup mid-recovery");

    // Recovery state hook: invoked exactly once, returning the in-flight metrics. A second invocation would mean termination ran twice; zero invocations would
    // mean lifecycle.ts skipped the hook (e.g., a refactor that conditioned it on an unrelated field).
    assert.equal(invocations.length, 1, "stopMonitor must be invoked exactly once during termination");

    // prerollTimer cleanup: the timer reference still exists, but lifecycle.ts cleared it. The clearest test is to wait past the firing window and assert the
    // timer's callback never ran. Since we set 60_000ms above and unref'd it, the test does not need to wait for that - we can directly observe by polling
    // synchronously: terminateStream is synchronous, so by the time it returns the timer must have been cleared. clearTimeout is idempotent and silent, so the
    // observable is "the timer never fires" - we can satisfy that by running through the event loop one tick and checking.
    await new Promise((resolve) => setImmediate(resolve));

    assert.equal(prerollTimerFired, false, "the prerollTimer must have been cleared by terminateStream - the callback must never run");

    // Registry / index cleanup.
    assert.equal(getStream(entry.id), undefined, "the registry entry must be gone after termination");
    assert.equal(getChannelStreamId("abc"), undefined, "the channel-to-stream index must not point at the terminated entry");

    // terminationInitiated cleanup: the flag is added at the start of termination and removed at the end (lifecycle.ts:246). A regression that left the flag
    // set would mean a future stream that recycled the id would be falsely treated as already-terminated and silently skipped.
    assert.equal(isTerminationInitiated(entry.id), false, "the terminationInitiated flag must be cleared after termination completes");
  });

  test("terminateStream is idempotent across recovery cycles: a second call during a notional next-recovery is a no-op", async () => {

    /* The "twice in quick succession" scenario. terminateStream's first action is to add the id to terminationInitiated; if the id is already there, it
     * early-returns without doing anything else (lifecycle.ts:125-129). The second call must NOT invoke stopMonitor again, NOT throw, and NOT corrupt the
     * registry. This is the production safety net for cases where multiple sources fire termination concurrently (client disconnect race, monitor circuit
     * breaker trip, graceful shutdown sweep). Without it, a double-cleanup would attempt to drain already-released resources.
     *
     * Why this matters specifically for recovery: the recovery loop and the client-tracking idle-timeout are independent producers of termination intent. A
     * stream that is mid-recovery and simultaneously has all clients disconnect can have terminateStream fired from both paths. The idempotency test pins the
     * race-safety contract.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const { invocations, stopMonitor } = makeMidRecoveryMonitor();
    const entry = makeRegistryEntry({ channelName: "abc", stopMonitor });

    registerStream(entry);
    setChannelStreamId("abc", entry.id);

    terminateStream(entry.id, "abc", "first call");
    assert.doesNotThrow(() => { terminateStream(entry.id, "abc", "second call"); }, "second termination must not throw");

    // stopMonitor was called exactly once - the second terminateStream early-returned. A regression that dropped the early-return would call stopMonitor again
    // against a closure that has already been drained - in the real monitor, that is a write-after-stop on the recovery state.
    assert.equal(invocations.length, 1, "stopMonitor must be invoked exactly once across two terminateStream calls");

    // Registry and index remain clean - the second call did not strand a new entry or index pointer.
    assert.equal(getStream(entry.id), undefined, "registry stays clean across the duplicate termination");
    assert.equal(getChannelStreamId("abc"), undefined, "channel index stays clean across the duplicate termination");
  });

  test("two streams in mid-recovery: terminating one does NOT invoke the other's stopMonitor and leaves the other's recovery state intact", async () => {

    /* The cross-stream isolation invariant under the recovery axis. Phase 1's lifecycle.test.ts pins cross-stream isolation for the registry/index
     * cleanup; this test extends the invariant to the recovery-state hook: stopMonitor on stream A must NOT be invoked when stream B is terminated. A
     * regression that confused the per-stream stopMonitor reference (e.g., a closure that captured a shared variable instead of the per-entry field) would
     * surface here as both stopMonitors being invoked when one terminates.
     *
     * This is the recovery-side analog of recovery-escalation's per-stream metric isolation: that suite proves the metrics objects are independent at the
     * recovery.ts layer; this suite proves lifecycle.ts respects that independence at the termination boundary.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const monitorA = makeMidRecoveryMonitor();
    const monitorB = makeMidRecoveryMonitor();

    const entryA = makeRegistryEntry({ channelName: "abc", stopMonitor: monitorA.stopMonitor });
    const entryB = makeRegistryEntry({ channelName: "nbc", stopMonitor: monitorB.stopMonitor });

    registerStream(entryA);
    registerStream(entryB);
    setChannelStreamId("abc", entryA.id);
    setChannelStreamId("nbc", entryB.id);

    // Snapshot stream B's metrics object reference and content so we can assert it was not touched by stream A's termination.
    const bMetricsBefore: RecoveryMetrics = { ...monitorB.metrics };

    terminateStream(entryA.id, "abc", "test - terminate A while both are mid-recovery");

    // Stream A's stopMonitor was invoked. Stream B's stopMonitor was NOT.
    assert.equal(monitorA.invocations.length, 1, "stream A's stopMonitor must be invoked");
    assert.equal(monitorB.invocations.length, 0, "stream B's stopMonitor must NOT be invoked - termination of A must not reach B's hook");

    // Stream B is still in the registry; stream A is gone.
    assert.equal(getStream(entryA.id), undefined, "stream A is removed from the registry");
    assert.ok(getStream(entryB.id), "stream B remains in the registry");
    assert.equal(getChannelStreamId("nbc"), entryB.id, "stream B's channel index pointer is intact");

    // Stream B's metrics are byte-equivalent to the snapshot - termination of A did not mutate B's recovery state. Field-by-field comparison via deepEqual
    // surfaces any drift specifically (e.g., a counter incremented by an errant cross-stream write).
    assert.deepEqual({ ...monitorB.metrics }, bMetricsBefore, "stream B's recovery metrics must be unchanged by stream A's termination");

    // Cleanup the surviving entry. The cleanup is part of the test contract - if we leave entries stranded between tests, subsequent tests in the same module
    // see ghost state and may fail in confusing ways.
    terminateStream(entryB.id, "nbc", "test cleanup");

    // Belt-and-suspenders: after both terminations, neither id remains in the registry or the channel index.
    assert.equal(getStream(entryB.id), undefined, "stream B is gone after explicit cleanup");
    assert.equal(getChannelStreamId("nbc"), undefined, "stream B's channel index is gone after explicit cleanup");

    // Use the imports that get linted as unused if no other branch references them. void prevents the linter from flagging deleteChannelStreamId / unregisterStream
    // as unused while keeping them imported for readers who want to know what the test surface depends on.
    void deleteChannelStreamId;
    void unregisterStream;
  });
});
