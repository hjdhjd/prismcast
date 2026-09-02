/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * launchGovernor.test.ts: Unit tests for the browser relaunch governor - the loop-safe CLOSED/OPEN/HALF-OPEN decision core. The functions take `now` as a
 * parameter (the same shape as the per-stream circuit breaker), so these tests are fully deterministic with literal timestamps and need no timer mocking.
 */
import { canAttemptLaunch, createLaunchGovernorState, noteLaunchFailure, noteLaunchSuccess, noteReadinessLost,
  noteSustainedHealth } from "./launchGovernor.ts";
import { describe, test } from "node:test";
import type { LaunchGovernorPolicy } from "./launchGovernor.ts";
import assert from "node:assert/strict";

// Main policy for the normal scenarios: trips on the 3rd failure within a 10s window; cooldown ladder 1s -> 5s -> 20s; 30s of sustained health resets to CLOSED.
const POLICY: LaunchGovernorPolicy = { cooldownLadderMs: [ 1000, 5000, 20000 ], failureThreshold: 3, failureWindowMs: 10000, healthHoldMs: 30000 };

// Cap policy: every failure trips (threshold 1) within a wide window, so the escalation ladder and its cap are easy to drive in isolation.
const CAP_POLICY: LaunchGovernorPolicy = { cooldownLadderMs: [ 1000, 2000, 3000 ], failureThreshold: 1, failureWindowMs: 1000000, healthHoldMs: 30000 };

// Trial policy: production's ratio, where the shortest cooldown rung is at least as long as the failure window, so the failure window always lapses while cooling.
const TRIAL_POLICY: LaunchGovernorPolicy = { cooldownLadderMs: [ 300000, 900000 ], failureThreshold: 3, failureWindowMs: 300000, healthHoldMs: 120000 };

describe("launchGovernor: CLOSED state", () => {

  test("a fresh governor permits a launch and is not cooling", () => {

    const state = createLaunchGovernorState();

    assert.equal(canAttemptLaunch(state, 0), true);
    assert.equal(state.cooldownUntil, null);
    assert.equal(state.cooldownLevel, 0);
  });

  test("failures below the threshold do not trip - the first failures relaunch immediately", () => {

    const state = createLaunchGovernorState();
    const first = noteLaunchFailure(state, 0, POLICY);
    const second = noteLaunchFailure(state, 100, POLICY);

    assert.equal(first.tripped, false);
    assert.equal(second.tripped, false);
    assert.equal(canAttemptLaunch(state, 200), true, "below the threshold a relaunch is still permitted immediately");
  });
});

describe("launchGovernor: tripping into OPEN (cooling)", () => {

  test("trips on the threshold failure and cools down for the first ladder rung", () => {

    const state = createLaunchGovernorState();

    noteLaunchFailure(state, 0, POLICY);
    noteLaunchFailure(state, 100, POLICY);

    const trip = noteLaunchFailure(state, 200, POLICY);

    assert.equal(trip.tripped, true);
    assert.equal(state.cooldownLevel, 1);
    assert.equal(trip.cooldownUntil, 200 + 1000, "cooldown ends one first-rung duration after the tripping failure");
    assert.equal(canAttemptLaunch(state, 500), false, "no launch while cooling");
    assert.equal(canAttemptLaunch(state, 1200), true, "launch permitted once the cooldown elapses (HALF-OPEN trial)");
  });
});

describe("launchGovernor: escalating cooldown ladder", () => {

  test("each successive trip cools down for the next-longer rung, capped at the final rung", () => {

    // threshold 1 + wide window: every failure trips, so we can drive escalation directly.
    const state = createLaunchGovernorState();

    const t1 = noteLaunchFailure(state, 0, CAP_POLICY);

    assert.equal(state.cooldownLevel, 1);
    assert.equal(t1.cooldownUntil, 0 + 1000, "first trip uses rung[0]");

    const t2 = noteLaunchFailure(state, 10, CAP_POLICY);

    assert.equal(state.cooldownLevel, 2);
    assert.equal(t2.cooldownUntil, 10 + 2000, "second trip uses rung[1]");

    const t3 = noteLaunchFailure(state, 20, CAP_POLICY);

    assert.equal(state.cooldownLevel, 3);
    assert.equal(t3.cooldownUntil, 20 + 3000, "third trip uses rung[2]");

    const t4 = noteLaunchFailure(state, 30, CAP_POLICY);

    assert.equal(state.cooldownLevel, 3, "the level caps at the ladder length");
    assert.equal(t4.cooldownUntil, 30 + 3000, "further trips stay on the final rung");
  });
});

describe("launchGovernor: health-gated reset", () => {

  test("sustained readiness for healthHoldMs resets the governor to CLOSED", () => {

    const state = createLaunchGovernorState();

    // Trip the governor.
    noteLaunchFailure(state, 0, POLICY);
    noteLaunchFailure(state, 100, POLICY);
    noteLaunchFailure(state, 200, POLICY);
    assert.equal(state.cooldownLevel, 1);

    // A trial later succeeds and the browser becomes ready.
    noteLaunchSuccess(state, 1500);

    // Before the hold elapses, no reset.
    assert.equal(noteSustainedHealth(state, 1500 + 29999, POLICY), false, "a not-yet-sustained ready period does not reset");
    assert.equal(state.cooldownLevel, 1, "still elevated before the hold");

    // After the hold elapses, the governor resets to CLOSED.
    assert.equal(noteSustainedHealth(state, 1500 + 30000, POLICY), true, "sustained readiness resets to CLOSED");
    assert.equal(state.cooldownLevel, 0);
    assert.equal(state.cooldownUntil, null);
    assert.equal(state.failure.totalFailureCount, 0, "the failure window is cleared");
    assert.equal(canAttemptLaunch(state, 1500 + 30000), true);
  });

  test("reports the reset exactly once - a second tick after the reset returns false", () => {

    // Regression: readySince stays anchored for the browser's whole ready life, so without the "nothing to reset" guard every tick after the hold would keep
    // returning true and the adapter would log the recovery on every 30-second check. The reset must be reported exactly once per recovery.
    const state = createLaunchGovernorState();

    noteLaunchFailure(state, 0, POLICY);
    noteLaunchFailure(state, 100, POLICY);
    noteLaunchFailure(state, 200, POLICY);
    noteLaunchSuccess(state, 1500);

    assert.equal(noteSustainedHealth(state, 1500 + 30000, POLICY), true, "the tick that performs the reset reports true");
    assert.equal(noteSustainedHealth(state, 1500 + 60000, POLICY), false, "a now-CLOSED governor reports no further reset");
    assert.equal(noteSustainedHealth(state, 1500 + 90000, POLICY), false, "and keeps reporting false on later ticks");
  });

  test("a healthy governor that never degraded never reports a reset, however long it stays ready", () => {

    // The common case: a browser launches cleanly and stays ready. noteSustainedHealth must stay false the whole time so the adapter never logs a spurious recovery.
    const state = createLaunchGovernorState();

    noteLaunchSuccess(state, 0);

    assert.equal(noteSustainedHealth(state, 30000, POLICY), false, "no prior degradation means nothing to reset");
    assert.equal(noteSustainedHealth(state, 600000, POLICY), false, "still nothing to reset after ten minutes of continuous health");
  });

  test("a momentary success does not reset - flapping accrues and escalates", () => {

    const state = createLaunchGovernorState();

    // Trip once (level 1).
    noteLaunchFailure(state, 0, POLICY);
    noteLaunchFailure(state, 100, POLICY);
    noteLaunchFailure(state, 200, POLICY);
    assert.equal(state.cooldownLevel, 1);

    // The cooldown elapses, a trial briefly succeeds...
    noteLaunchSuccess(state, 1200);

    // ...but capture dies again before the health hold elapses. The failure must accrue against the un-reset window and escalate, not restart at level 1.
    const reflap = noteLaunchFailure(state, 1300, POLICY);

    assert.equal(reflap.tripped, true, "the un-reset window is already at threshold, so the next failure re-trips");
    assert.equal(state.cooldownLevel, 2, "flapping escalates the cooldown rather than resetting it");
    assert.equal(state.readySince, null, "the failed launch clears the readiness anchor");
  });
});

describe("launchGovernor: readiness loss", () => {

  test("noteReadinessLost clears the anchor so a stale ready period cannot satisfy a later reset", () => {

    const state = createLaunchGovernorState();

    noteLaunchSuccess(state, 0);
    noteReadinessLost(state);

    assert.equal(state.readySince, null);
    assert.equal(noteSustainedHealth(state, 1000000, POLICY), false, "with the anchor cleared, even a long elapsed time does not reset");
  });
});

describe("launchGovernor: full HALF-OPEN trial cycle", () => {

  test("trip -> cooldown -> trial success -> sustained health -> recovered to CLOSED", () => {

    const state = createLaunchGovernorState();

    // Trip.
    noteLaunchFailure(state, 0, POLICY);
    noteLaunchFailure(state, 100, POLICY);
    const trip = noteLaunchFailure(state, 200, POLICY);

    assert.equal(canAttemptLaunch(state, 800), false, "cooling: no trial yet");

    // Cooldown elapses -> a trial is permitted (HALF-OPEN).
    const trialAt = trip.cooldownUntil ?? 0;

    assert.equal(canAttemptLaunch(state, trialAt), true);

    // The trial succeeds; readiness then holds long enough to reset.
    noteLaunchSuccess(state, trialAt);
    assert.equal(noteSustainedHealth(state, trialAt + 30000, POLICY), true);
    assert.equal(state.cooldownLevel, 0, "recovered to CLOSED after a sustained, verified trial");
  });
});

describe("launchGovernor: a failed HALF-OPEN trial", () => {

  test("a trial failure trips and escalates even though the cooldown outlasted the failure window", () => {

    const state = createLaunchGovernorState();

    noteLaunchFailure(state, 0, TRIAL_POLICY);
    noteLaunchFailure(state, 100, TRIAL_POLICY);
    noteLaunchFailure(state, 200, TRIAL_POLICY);
    assert.equal(state.cooldownLevel, 1, "the third failure trips to the first rung");

    // The cooldown elapsed and the trial that followed it failed. The failure window lapsed while cooling, so the trial flag is the only thing separating this
    // failure from a forgiven transient.
    const trial = noteLaunchFailure(state, 500300, TRIAL_POLICY, true);

    assert.equal(trial.tripped, true, "a failed trial trips whatever the failure window says");
    assert.equal(state.cooldownLevel, 2, "and escalates one rung along the ladder");
    assert.equal(trial.cooldownUntil, 500300 + 900000, "cooling for the second rung, measured from the trial's failure");
  });

  test("an ordinary failure after the window lapses is still forgiven and leaves the ladder and cooldown untouched", () => {

    // The counterpart assertion: the same lapsed-window arithmetic without the trial flag must keep today's forgiveness, so the trial rule cannot be mistaken for a
    // blanket "any failure after a trip re-trips". Like every sibling here this drives the functions on literal inputs; production reaches this state when a trial
    // succeeds, readiness is lost before the health hold elapses, and the fresh launch that follows fails.
    const state = createLaunchGovernorState();

    noteLaunchFailure(state, 0, TRIAL_POLICY);
    noteLaunchFailure(state, 100, TRIAL_POLICY);

    const trip = noteLaunchFailure(state, 200, TRIAL_POLICY);
    const lapsed = noteLaunchFailure(state, 500300, TRIAL_POLICY);

    assert.equal(lapsed.tripped, false, "a lapsed window forgives an ordinary failure");
    assert.equal(lapsed.cooldownUntil, null, "and reports no new cooldown");
    assert.equal(state.failure.totalFailureCount, 1, "the window restarted from this failure");
    assert.equal(state.cooldownUntil, trip.cooldownUntil, "the cooldown set by the earlier trip is untouched");
    assert.equal(state.cooldownLevel, 1, "and the ladder does not move");
  });
});
