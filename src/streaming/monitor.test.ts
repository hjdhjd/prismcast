/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * monitor.test.ts: Unit-test surface for the playback health monitor. monitor.ts exposes one public function - monitorPlaybackHealth - which spawns a 1Hz
 * setInterval, calls page.evaluate() against a real Puppeteer Page on every tick to read the HTML video element's state, classifies playback issues via the
 * recovery primitives, and orchestrates recovery actions (play/unmute, source reload, page navigation, tab replacement). Honest test coverage for this
 * orchestration requires (1) a real Chrome browser to host the video element, (2) at least one segmenter and FFmpeg subprocess to detect the tiny-segment
 * threshold, and (3) timer control across AsyncLocalStorage boundaries that node:test's mock.timers does not adequately support. The pure helpers it relies on -
 * checkCircuitBreaker, getIssueCategory, formatIssueType, recordRecoveryAttempt/Success, RECOVERY_METHODS, getRecoveryMethod - all live in recovery.ts and are
 * already covered by recovery.test.ts. monitorPlaybackHealth itself is therefore deferred to e2e.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { monitorPlaybackHealth } from "./monitor.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("monitorPlaybackHealth", () => {

  test("is exported as a function with the documented signature", () => {

    // Smoke test: lock the public-API contract that other modules import. If a future refactor inadvertently turns monitorPlaybackHealth into a default export
    // or removes it, callers (notably setup.ts) break - this test surfaces the change.
    assert.equal(typeof monitorPlaybackHealth, "function", "monitorPlaybackHealth is a function");
  });
});
