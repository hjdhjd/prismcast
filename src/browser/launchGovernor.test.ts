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
