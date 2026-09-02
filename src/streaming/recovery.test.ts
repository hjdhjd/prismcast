/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * recovery.test.ts: Unit tests for the issue-classification primitives in recovery.ts - RECOVERY_METHODS sentinel, getIssueDescription, getRecoveryMethod,
 * formatIssueType, getIssueCategory, and isCaptureInfrastructureError. Metrics tracking lives in recovery.metrics.test.ts; circuit-breaker primitives live in
 * recovery.circuitBreaker.test.ts.
 */
import { CAPTURE_PROBE_TIMEOUT_MESSAGE, STREAM_INIT_TIMEOUT_MESSAGE } from "./setup.ts";
import { RECOVERY_METHODS, classifyNativeSegmentHealth, computeNextRecoveryLevel, deriveStreamHealth, describeResolutionOutcome, formatIssueType,
  getIssueCategory, getIssueDescription, getRecoveryMethod, isCaptureInfrastructureError, isResolutionDegraded, resolutionAreaRatio, shouldTriggerRecovery,
  updateResolutionPeak } from "./recovery.ts";
import { describe, test } from "node:test";
import { CaptureTurnTimeoutError } from "./captureLock.ts";
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

describe("isCaptureInfrastructureError", () => {

  test("matches each capture-infrastructure signature", () => {

    // The capture-infrastructure signatures the classifier owns - consumed by both the 503 back-off decision and the browser supervisor's readiness detection.
    assert.equal(isCaptureInfrastructureError(new Error("Cannot capture a tab with an active stream")), true);
    assert.equal(isCaptureInfrastructureError(new Error("Capture queue wait timed out.")), true);
    assert.equal(isCaptureInfrastructureError(new Error("Stream initialization timed out.")), true);
    assert.equal(isCaptureInfrastructureError(new Error("Could not start video source")), true);
    assert.equal(isCaptureInfrastructureError(new Error("The capture extension is not ready on this browser.")), true);
    assert.equal(isCaptureInfrastructureError(new Error("No active tab was found for capture.")), true);
  });

  test("accepts either an Error or a bare string", () => {

    // setup.ts passes a pre-extracted message string; the supervisor may pass an Error. Both normalize to the same judgment.
    assert.equal(isCaptureInfrastructureError("Capture queue wait timed out."), true);
    assert.equal(isCaptureInfrastructureError(new Error("Capture queue wait timed out.")), true);
  });

  test("does not match site- or stream-specific failures", () => {

    // A navigation or page error is not a capture-infrastructure fault; it must classify as non-capture so the setup path returns 500, not 503.
    assert.equal(isCaptureInfrastructureError(new Error("net::ERR_NAME_NOT_RESOLVED")), false);
    assert.equal(isCaptureInfrastructureError(new Error("Video element not found.")), false);
    assert.equal(isCaptureInfrastructureError(""), false);
  });


  test("the three production capture-timeout messages are exact and classify as capture-infrastructure errors", () => {

    // Pin the actual production constants (not restated literals) so a message change that would silently break the "timed out" classification is caught here. The
    // turn-timeout message lives on its error class in captureLock.ts; the stream-init and probe messages live on their setup.ts constants.
    assert.equal(new CaptureTurnTimeoutError().message, "Capture queue wait timed out.");
    assert.equal(STREAM_INIT_TIMEOUT_MESSAGE, "Stream initialization timed out.");
    assert.equal(CAPTURE_PROBE_TIMEOUT_MESSAGE, "Capture probe timed out.");
    assert.equal(isCaptureInfrastructureError(new CaptureTurnTimeoutError().message), true);
    assert.equal(isCaptureInfrastructureError(STREAM_INIT_TIMEOUT_MESSAGE), true);
    assert.equal(isCaptureInfrastructureError(CAPTURE_PROBE_TIMEOUT_MESSAGE), true);
  });
});

describe("deriveStreamHealth", () => {

  test("an error state reports 'error', outranking every lower signal", () => {

    // hasError takes top precedence: even with a buffering signal and a stall count also present, the health is "error".
    assert.equal(deriveStreamHealth({ escalationLevel: 0, hasError: true, isBuffering: true, stallCount: 5 }), "error");
  });

  test("a page-reload-level escalation (>= 3) reports 'error' even without an error flag", () => {

    // Escalation level 3 is the page-reload tier - treated as equally severe as an error state.
    assert.equal(deriveStreamHealth({ escalationLevel: 3, hasError: false, isBuffering: true, stallCount: 5 }), "error");
  });

  test("an active escalation of 1-2 reports 'recovering', outranking buffering and stalls", () => {

    assert.equal(deriveStreamHealth({ escalationLevel: 1, hasError: false, isBuffering: true, stallCount: 5 }), "recovering");
    assert.equal(deriveStreamHealth({ escalationLevel: 2, hasError: false, isBuffering: false, stallCount: 0 }), "recovering");
  });

  test("buffering reports 'buffering' once error and escalation are clear, outranking a stall count", () => {

    assert.equal(deriveStreamHealth({ escalationLevel: 0, hasError: false, isBuffering: true, stallCount: 5 }), "buffering");
  });

  test("a positive stall count reports 'stalled' when nothing higher applies", () => {

    assert.equal(deriveStreamHealth({ escalationLevel: 0, hasError: false, isBuffering: false, stallCount: 1 }), "stalled");
  });

  test("a clean slate reports 'healthy'", () => {

    assert.equal(deriveStreamHealth({ escalationLevel: 0, hasError: false, isBuffering: false, stallCount: 0 }), "healthy");
  });
});

describe("shouldTriggerRecovery", () => {

  // A neutral baseline where no condition fires; each test flips only the fields its scenario needs.
  const base = {

    hasEnded: false,
    hasError: false,
    isPaused: false,
    isProgressing: true,
    pauseCount: 0,
    productionStalled: false,
    stallCount: 0,
    stallCountThreshold: 3,
    withinBufferingGrace: false,
    withinRecoveryGrace: false
  };

  test("never triggers inside the post-recovery grace window, even when every other condition would fire", () => {

    // The grace window short-circuits: a stream with an error, an ended flag, and a stalled pipeline still does not trigger while recovering.
    assert.equal(shouldTriggerRecovery({ ...base, hasEnded: true, hasError: true, productionStalled: true, withinRecoveryGrace: true }), false);
  });

  test("triggers on an error state outside the grace window", () => {

    assert.equal(shouldTriggerRecovery({ ...base, hasError: true }), true);
  });

  test("triggers on an ended stream (live streams should not end)", () => {

    assert.equal(shouldTriggerRecovery({ ...base, hasEnded: true }), true);
  });

  test("triggers on a persistent pause strictly past the stall-count hysteresis, but not at or below it", () => {

    assert.equal(shouldTriggerRecovery({ ...base, isPaused: true, pauseCount: 4 }), true);
    assert.equal(shouldTriggerRecovery({ ...base, isPaused: true, pauseCount: 3 }), false, "exactly at the threshold does not trigger");
  });

  test("the buffering grace window suppresses a persistent pause", () => {

    // Even past the pause hysteresis, being inside the buffering grace window filters the transient rebuffer pause.
    assert.equal(shouldTriggerRecovery({ ...base, isPaused: true, pauseCount: 4, withinBufferingGrace: true }), false);
  });

  test("triggers on a persistent stall past the hysteresis, suppressed by the buffering grace and not fired at the threshold", () => {

    assert.equal(shouldTriggerRecovery({ ...base, isProgressing: false, stallCount: 4 }), true);
    assert.equal(shouldTriggerRecovery({ ...base, isProgressing: false, stallCount: 4, withinBufferingGrace: true }), false, "buffering grace suppresses the stall");
    assert.equal(shouldTriggerRecovery({ ...base, isProgressing: false, stallCount: 3 }), false, "exactly at the threshold does not trigger");
  });

  test("triggers when the capture pipeline has stalled, regardless of the playback flags", () => {

    assert.equal(shouldTriggerRecovery({ ...base, productionStalled: true }), true);
  });

  test("does not trigger on a healthy, progressing stream", () => {

    assert.equal(shouldTriggerRecovery(base), false);
  });
});

describe("computeNextRecoveryLevel", () => {

  test("a paused stream from a clean slate (level 0) escalates to L1 (play/unmute)", () => {

    assert.equal(computeNextRecoveryLevel({ currentEscalationLevel: 0, issueCategory: "paused", sourceReloadAttempted: false }), 1);
  });

  test("a paused stream already past level 0 skips L1 and escalates to L2 when no source reload has been attempted", () => {

    // The L1 fast-path only applies from a clean slate; once escalated, a paused issue follows the same source-reload path as the others.
    assert.equal(computeNextRecoveryLevel({ currentEscalationLevel: 1, issueCategory: "paused", sourceReloadAttempted: false }), 2);
  });

  test("a buffering or other issue escalates to L2 (source reload) on the first attempt", () => {

    assert.equal(computeNextRecoveryLevel({ currentEscalationLevel: 0, issueCategory: "buffering", sourceReloadAttempted: false }), 2);
    assert.equal(computeNextRecoveryLevel({ currentEscalationLevel: 0, issueCategory: "other", sourceReloadAttempted: false }), 2);
  });

  test("escalates to L3 (page navigation) once a source reload has already been attempted", () => {

    assert.equal(computeNextRecoveryLevel({ currentEscalationLevel: 2, issueCategory: "buffering", sourceReloadAttempted: true }), 3);
    assert.equal(computeNextRecoveryLevel({ currentEscalationLevel: 2, issueCategory: "paused", sourceReloadAttempted: true }), 3);
  });
});

describe("classifyNativeSegmentHealth", () => {

  // A target duration of 1000ms puts the staleness tiers at 2000ms (stalled), 4000ms (first escalation), and 6000ms.
  const targetDurationMs = 1000;

  test("active fetch errors report 'recovering' with no escalation, outranking any staleness", () => {

    // consecutiveErrors takes precedence: even a 7x-stale stream reports recovering, not stalled, and does not escalate here (the proxy retries internally).
    assert.deepEqual(classifyNativeSegmentHealth({ consecutiveErrors: 2, lastSegmentTime: 1, recoveryAttempts: 0, stalenessMs: 7000, targetDurationMs }),
      { action: "none", health: "recovering", issueType: "fetch errors" });
  });

  test("below the 2x staleness threshold the stream is healthy, and exactly 2x is not yet stale", () => {

    assert.deepEqual(classifyNativeSegmentHealth({ consecutiveErrors: 0, lastSegmentTime: 1, recoveryAttempts: 0, stalenessMs: 1999, targetDurationMs }),
      { action: "none", health: "healthy", issueType: null });
    // The comparison is strict: exactly 2000ms is not stale.
    assert.equal(classifyNativeSegmentHealth({ consecutiveErrors: 0, lastSegmentTime: 1, recoveryAttempts: 0, stalenessMs: 2000, targetDurationMs }).health, "healthy");
  });

  test("staleness is ignored until at least one segment has been produced (lastSegmentTime 0)", () => {

    // A brand-new stream with no segment yet must not be flagged stalled even when the staleness clock has run well past the threshold.
    assert.deepEqual(classifyNativeSegmentHealth({ consecutiveErrors: 0, lastSegmentTime: 0, recoveryAttempts: 0, stalenessMs: 9000, targetDurationMs }),
      { action: "none", health: "healthy", issueType: null });
  });

  test("between 2x and 4x staleness reports 'stalled' without escalating", () => {

    assert.deepEqual(classifyNativeSegmentHealth({ consecutiveErrors: 0, lastSegmentTime: 1, recoveryAttempts: 0, stalenessMs: 3000, targetDurationMs }),
      { action: "none", health: "stalled", issueType: "segment stall" });
  });

  test("past 4x on the first attempt escalates to L2", () => {

    assert.deepEqual(classifyNativeSegmentHealth({ consecutiveErrors: 0, lastSegmentTime: 1, recoveryAttempts: 0, stalenessMs: 5000, targetDurationMs }),
      { action: "l2", health: "stalled", issueType: "segment stall" });
  });

  test("the first attempt always escalates to L2 (reload first), however stale - it does not skip straight to L3", () => {

    // The least-disruptive-first principle: on the first stalled tick the stream gets a page reload no matter how stale it is, rather than jumping to the
    // heavier capture fallback. Even a 7x-stale stream with no prior attempt escalates to L2, not L3.
    assert.deepEqual(classifyNativeSegmentHealth({ consecutiveErrors: 0, lastSegmentTime: 1, recoveryAttempts: 0, stalenessMs: 7000, targetDurationMs }),
      { action: "l2", health: "stalled", issueType: "segment stall" });
  });

  test("a still-stalled stream after a page reload has been attempted escalates to L3 (the reload did not resolve it)", () => {

    // recoveryAttempts > 0 means a reload already fired; a stream still past the 4x threshold on the next tick is the "the reload did not work" signal, so it
    // escalates to the L3 capture fallback.
    assert.deepEqual(classifyNativeSegmentHealth({ consecutiveErrors: 0, lastSegmentTime: 1, recoveryAttempts: 1, stalenessMs: 5000, targetDurationMs }),
      { action: "l3", health: "stalled", issueType: "segment stall" });
  });
});

describe("updateResolutionPeak", () => {

  test("establishes the peak from the first reading, unaccepted", () => {

    // Before the first reading there is nothing to measure against, so the reading itself becomes the standard. It starts unaccepted because no ladder has run.
    assert.deepEqual(updateResolutionPeak({ peak: null, reading: { height: 720, width: 1280 } }), { accepted: false, height: 720, width: 1280 });
  });

  test("a larger-area reading replaces the record and clears an acceptance", () => {

    /* The source proving it can do better than the level the ladder settled for is exactly the case where the ladder should be allowed to run again. Clearing
     * accepted along with the dimensions is what makes that happen, and it is why the two facts share one record rather than living in separate fields.
     */
    const peak = updateResolutionPeak({ peak: { accepted: true, height: 450, width: 800 }, reading: { height: 1080, width: 1920 } });

    assert.deepEqual(peak, { accepted: false, height: 1080, width: 1920 });
  });

  test("a smaller-area reading leaves the record untouched, acceptance included", () => {

    // The drop is the thing the peak exists to measure, so it must not move the standard. An acceptance granted at this peak survives the drop that follows it.
    const peak = { accepted: true, height: 450, width: 800 };

    assert.deepEqual(updateResolutionPeak({ peak, reading: { height: 234, width: 416 } }), peak);
  });

  test("a wider but smaller-area reading does not grow the peak", () => {

    /* 1600x600 is 960000 pixels against 1280x1080's 1382400 - wider, but less picture. A per-dimension maxima would synthesize a 1600x1080 record equal to neither
     * input, so the deepEqual against the original record is what tells the two implementations apart.
     */
    const peak = { accepted: false, height: 1080, width: 1280 };

    assert.deepEqual(updateResolutionPeak({ peak, reading: { height: 600, width: 1600 } }), peak);
  });

  test("a reading equal in area leaves the record in place", () => {

    // The boundary. Replacing on equality would clear an acceptance every time the picture merely held steady at its peak, re-arming the ladder for no reason.
    const peak = { accepted: true, height: 720, width: 1280 };

    assert.deepEqual(updateResolutionPeak({ peak, reading: { height: 1280, width: 720 } }), peak);
  });
});

describe("resolutionAreaRatio", () => {

  test("reports the reading's share of the peak by area", () => {

    /* The field's stuck rendition: 416x234 against an 800x450 peak is 27 percent of the picture. The same pair read per-dimension is 52 percent, which is the
     * number a threshold at one half would let through, so pinning the area reading is what pins the whole detector's sensitivity.
     */
    const ratio = resolutionAreaRatio({ peak: { accepted: false, height: 450, width: 800 }, reading: { height: 234, width: 416 } });

    assert.equal(Number(ratio.toFixed(2)), 0.27);
  });

  test("reports one for a reading back at its peak", () => {

    assert.equal(resolutionAreaRatio({ peak: { accepted: false, height: 720, width: 1280 }, reading: { height: 720, width: 1280 } }), 1);
  });
});

describe("isResolutionDegraded", () => {

  test("calls the field's stuck rendition degraded where a per-dimension test would not", () => {

    // 27 percent by area is well under half; 52 percent by either dimension is not. This row is the reason the helper measures area at all.
    const degraded = isResolutionDegraded({ peak: { accepted: false, height: 450, width: 800 }, reading: { height: 234, width: 416 }, threshold: 0.5 });

    assert.equal(degraded, true);
  });

  test("a reading at exactly half the peak's area is not degraded", () => {

    // The strict-less-than boundary. 1280x720 is 921600 pixels and 640x720 is 460800 - exactly half - so the threshold reads as the floor of healthy, not the
    // ceiling of degraded.
    const degraded = isResolutionDegraded({ peak: { accepted: false, height: 720, width: 1280 }, reading: { height: 720, width: 640 }, threshold: 0.5 });

    assert.equal(degraded, false);
  });

  test("a modest drop from the peak is not degraded", () => {

    // 800x450 against a 1024x576 peak is 61 percent by area: visibly less picture, but not the collapse the ladder exists to recover, and the one field case the
    // peak rule deliberately forgoes.
    const degraded = isResolutionDegraded({ peak: { accepted: false, height: 576, width: 1024 }, reading: { height: 450, width: 800 }, threshold: 0.5 });

    assert.equal(degraded, false);
  });
});

describe("describeResolutionOutcome", () => {

  test("a reading back at the peak's area is restored", () => {

    const outcome = describeResolutionOutcome({ peak: { accepted: true, height: 720, width: 1280 }, reading: { height: 720, width: 1280 } });

    assert.equal(outcome, "restored");
  });

  test("a reading above the peak's area is also restored", () => {

    // A reading larger than the record can only happen on the tick that also replaces the peak, and calling that "improved" would understate it.
    const outcome = describeResolutionOutcome({ peak: { accepted: true, height: 720, width: 1280 }, reading: { height: 1080, width: 1920 } });

    assert.equal(outcome, "restored");
  });

  test("a reading short of the peak's area is improved", () => {

    // Above the degradation threshold but below the best the stream has shown: the episode is over, but the picture is not all the way back.
    const outcome = describeResolutionOutcome({ peak: { accepted: true, height: 720, width: 1280 }, reading: { height: 576, width: 1024 } });

    assert.equal(outcome, "improved");
  });
});
