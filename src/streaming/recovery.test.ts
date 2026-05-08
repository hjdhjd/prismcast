/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * recovery.test.ts: Unit tests for the issue-classification primitives in recovery.ts - RECOVERY_METHODS sentinel, getIssueDescription, getRecoveryMethod,
 * formatIssueType, and getIssueCategory. Metrics tracking lives in recovery.metrics.test.ts; circuit-breaker primitives live in recovery.circuitBreaker.test.ts.
 */
import { RECOVERY_METHODS, formatIssueType, getIssueCategory, getIssueDescription, getRecoveryMethod } from "./recovery.ts";
import { describe, test } from "node:test";
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
