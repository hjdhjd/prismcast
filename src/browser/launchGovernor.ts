/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * launchGovernor.ts: The browser relaunch governor - the loop-safe decision core for the browser supervisor.
 */
import type { FailureWindowResult, FailureWindowState } from "../streaming/recovery.ts";
import { recordFailure, resetFailureWindow } from "../streaming/recovery.ts";

/* This module is the pure decision core that keeps browser relaunch from becoming a teardown/restart loop. The browser supervisor in browser/index.ts owns the
 * Chrome instance and the lifecycle states (absent / launching / ready / degraded / trialing); this governor owns the throttling judgment those states turn on. It
 * is the classic circuit breaker (CLOSED / OPEN / HALF-OPEN) expressed as pure functions over an explicit state, with `now` passed in by the caller - the same
 * shape as the per-stream circuit breaker (checkCircuitBreaker), so it is deterministically unit-testable with literal timestamps and no timer mocking.
 *
 * The throttling judgment, in one place:
 *
 *   - CLOSED: launches are allowed; a launch failure accrues against a shared FailureWindow. The first failure relaunches immediately (no penalty for the common
 *     transient), so ordinary single-crash recovery stays fast.
 *   - OPEN (cooling): once the window trips (too many failed launches recently), the governor enters a cooldown during which no launch is allowed - the supervisor's
 *     `degraded` state. The cooldown duration escalates along a ladder on each successive trip, so a persistent failure backs off rather than thrashing Chrome.
 *   - HALF-OPEN (trial): when the cooldown elapses, one launch is allowed again - the supervisor's `trialing` state. A trial failure trips the window again and
 *     escalates the cooldown; a trial success enters `ready`.
 *   - Reset is health-gated, not success-gated: the failure window and the cooldown escalation clear only after the browser has been continuously ready for
 *     healthHoldMs. A momentary success does not reset, so flapping (ready -> dies -> ready) still accrues toward a trip exactly like a hard failure.
 *
 * The governor never decides to exit the process or to relaunch on its own - it only answers "may a launch be attempted now, and what does this outcome do to the
 * throttle?" The supervisor maps those answers onto the lifecycle states and performs the actual Chrome work.
 */

/**
 * The governor's throttling state. The caller (browser supervisor) owns one of these for the process lifetime and threads it through the functions below.
 */
export interface LaunchGovernorState {

  // The escalation index into the cooldown ladder. Zero when CLOSED; incremented (capped at the ladder length) on each trip so successive trips cool down longer.
  cooldownLevel: number;

  // The timestamp before which no launch may be attempted, or null when not cooling. Set when the window trips; cleared by the health-gated reset.
  cooldownUntil: number | null;

  // The shared failure-accrual window over failed launch attempts.
  failure: FailureWindowState;

  // The timestamp at which the browser most recently became ready, or null when not ready. Anchors the sustained-health reset; cleared whenever readiness is lost.
  readySince: number | null;
}

/**
 * The policy bounds the governor enforces. Injected by the supervisor (sourced from config with conservative defaults) rather than hard-coded here, so the pure
 * core stays testable with arbitrary bounds and the operator can tune tolerance without code changes.
 */
export interface LaunchGovernorPolicy {

  // Escalating cooldown durations in milliseconds, indexed by (cooldownLevel - 1) and clamped to the last entry. Each successive trip cools down for the next-longer
  // duration; the final entry is the ceiling.
  readonly cooldownLadderMs: readonly number[];

  // Number of failed launches within failureWindowMs that trips the governor into a cooldown.
  readonly failureThreshold: number;

  // The sliding window over launch failures, in milliseconds.
  readonly failureWindowMs: number;

  // How long the browser must remain continuously ready before the failure window and cooldown escalation reset to CLOSED.
  readonly healthHoldMs: number;
}

/**
 * The outcome of recording a launch failure: whether the governor has now tripped into a cooldown, and until when.
 */
export interface LaunchFailureOutcome {

  // The timestamp until which launches are now blocked, or null when the governor remains CLOSED (below the trip threshold).
  readonly cooldownUntil: number | null;

  // Whether this failure tripped the governor into (or kept it in) a cooldown - i.e. the supervisor should be `degraded`.
  readonly tripped: boolean;
}

/**
 * Creates a fresh governor state: CLOSED, no failures recorded, not cooling, not ready.
 * @returns A new LaunchGovernorState.
 */
export function createLaunchGovernorState(): LaunchGovernorState {

  return { cooldownLevel: 0, cooldownUntil: null, failure: { firstFailureTime: null, totalFailureCount: 0 }, readySince: null };
}

/**
 * Reports whether a launch may be attempted now. False only while cooling (a cooldown is set and has not yet elapsed); true when CLOSED or once the cooldown has
 * elapsed (the HALF-OPEN trial). This is the gate the supervisor consults before launching, which is what decouples relaunch from request arrival - during a
 * cooldown, requests are rejected fast without spawning Chrome.
 * @param state - The governor state.
 * @param now - The current timestamp in milliseconds.
 * @returns True when a launch attempt is permitted.
 */
export function canAttemptLaunch(state: LaunchGovernorState, now: number): boolean {

  return (state.cooldownUntil === null) || (now >= state.cooldownUntil);
}

/**
 * Records a failed launch attempt and reports the resulting throttle. The failure accrues against the window; when the window trips, the governor enters a cooldown
 * whose duration is the next rung of the escalating ladder (capped at the final rung) and clears the readiness anchor. Below the threshold the governor stays
 * CLOSED, so the first failures relaunch immediately.
 * @param state - The governor state to update.
 * @param now - The current timestamp in milliseconds.
 * @param policy - The policy bounds.
 * @returns Whether the governor tripped into a cooldown and until when.
 */
export function noteLaunchFailure(state: LaunchGovernorState, now: number, policy: LaunchGovernorPolicy): LaunchFailureOutcome {

  // A failed launch means the browser is not ready; drop the readiness anchor so a prior ready period cannot satisfy a later health-gated reset.
  state.readySince = null;

  const result: FailureWindowResult = recordFailure(state.failure, now, { threshold: policy.failureThreshold, windowMs: policy.failureWindowMs });

  if(!result.tripped) {

    return { cooldownUntil: null, tripped: false };
  }

  // The window tripped: escalate one rung along the cooldown ladder (capped at its length) and cool down for that duration.
  state.cooldownLevel = Math.min(state.cooldownLevel + 1, policy.cooldownLadderMs.length);

  const rung = policy.cooldownLadderMs[state.cooldownLevel - 1] ?? 0;

  state.cooldownUntil = now + rung;

  return { cooldownUntil: state.cooldownUntil, tripped: true };
}

/**
 * Records that a launch succeeded and the browser is ready. Anchors the sustained-health clock but does NOT reset the failure window or cooldown escalation - that
 * is deliberately health-gated (see noteSustainedHealth) so a momentary success cannot wipe out accrued failures and let flapping evade the trip.
 * @param state - The governor state to update.
 * @param now - The current timestamp in milliseconds.
 */
export function noteLaunchSuccess(state: LaunchGovernorState, now: number): void {

  state.readySince = now;
}

/**
 * Records that readiness was lost mid-life (the browser disconnected or its capture subsystem died). Clears the readiness anchor so the sustained-health reset
 * cannot fire off a readiness period that has already ended. Does not itself count a failure - the supervisor calls noteLaunchFailure when a subsequent relaunch
 * attempt fails.
 * @param state - The governor state to update.
 */
export function noteReadinessLost(state: LaunchGovernorState): void {

  state.readySince = null;
}

/**
 * Resets the governor to CLOSED if the browser has been continuously ready for at least healthHoldMs. Called on the supervisor's periodic health tick. Returns
 * whether it just reset, so the caller can log the recovery. The hold is what makes the reset health-gated rather than success-gated: only sustained readiness -
 * not a brief success between failures - clears the accrued failures and the cooldown escalation.
 * @param state - The governor state to update.
 * @param now - The current timestamp in milliseconds.
 * @param policy - The policy bounds.
 * @returns True when this call reset the governor to CLOSED.
 */
export function noteSustainedHealth(state: LaunchGovernorState, now: number, policy: LaunchGovernorPolicy): boolean {

  if((state.readySince === null) || ((now - state.readySince) < policy.healthHoldMs)) {

    return false;
  }

  resetFailureWindow(state.failure);

  state.cooldownLevel = 0;
  state.cooldownUntil = null;

  return true;
}
