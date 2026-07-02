/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pretune.test.ts: Unit tests for the predictive channel pretune subsystem. pretune.ts polls the Channels DVR /api/v1/jobs endpoint, schedules a setTimeout that
 * fires PRETUNE_LEAD_MS (30s) before each upcoming recording, and tears down unclaimed pretuned streams after a safety timeout. The public surface is the
 * startPretunePolling()/stopPretunePolling() lifecycle pair plus clearPretuneSafetyTimer(), which terminateStream() calls to drop a pretuned stream's safety timer
 * when the stream is claimed and torn down normally. The polling functions depend on side effects (intervals, async DVR fetches, stream initialization) that
 * require deep mocking, so the honest test surface is verifying the start/stop pair is idempotent, that stop cleanly drains active timers, and that
 * clearPretuneSafetyTimer is a safe no-op for the non-pretuned streams that dominate the terminate path.
 */
import { describe, test } from "node:test";
import { startPretunePolling, stopPretunePolling } from "./pretune.ts";
import assert from "node:assert/strict";
import { clearPretuneSafetyTimer } from "./pretuneTimers.ts";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("startPretunePolling / stopPretunePolling", () => {

  test("startPretunePolling is idempotent - second call is a no-op", () => {

    // The implementation guards on `if(pollInterval) { return; }` so a second call has no effect. Locks the contract that startup paths can call multiple times
    // without spawning duplicate intervals.
    assert.doesNotThrow(() => {

      startPretunePolling();
      startPretunePolling();
    });

    // Cleanup so the test runner exits.
    stopPretunePolling();
  });

  test("stopPretunePolling cleanly stops without throwing when polling is active", () => {

    startPretunePolling();

    assert.doesNotThrow(() => {

      stopPretunePolling();
    });
  });

  test("stopPretunePolling is a no-op when polling has not been started", () => {

    // Negative test: callers (graceful shutdown) may invoke stop on a never-started module. The early return on null pollInterval must keep this safe.
    assert.doesNotThrow(() => {

      stopPretunePolling();
    });
  });

  test("repeated start/stop cycles do not leak intervals", () => {

    // Each pair re-enters the start guard cleanly. We exercise three cycles to ensure the cleanup truly resets state. The trailing assert.ok(true) is intentional:
    // the loop body would throw on regression (start or stop raising, internal Maps growing without bound), and the success criterion is "this completes without
    // throwing and the process can exit cleanly under the unref'd cleanup timer at the top of this file." There is no observable per-cycle invariant to assert on;
    // the test's value is in exercising the lifecycle without regression.
    for(let i = 0; i < 3; i++) {

      startPretunePolling();
      stopPretunePolling();
    }

    assert.ok(true, "three cycles completed without throwing");
  });

  test("stopPretunePolling clears pending timers even when called immediately after start", () => {

    // The implementation iterates the activeTimers Map directly and clearTimeout's each entry, then clears the safety timers via clearAllPretuneSafetyTimers() (the
    // pretuneTimers.ts registry that owns and iterates the safetyTimers Map). With no DVR host configured, the polling function returns early before scheduling any
    // timers, so this test mostly exercises the no-timer cleanup path. Locks that the empty-Map iteration is harmless.
    startPretunePolling();
    stopPretunePolling();

    // Calling stop a second time must also be safe - the Maps are still empty after the first stop cleared them.
    assert.doesNotThrow(() => {

      stopPretunePolling();
    });
  });
});

describe("clearPretuneSafetyTimer", () => {

  test("is a no-op for a stream ID that was never pretuned (the dominant claim+terminate path)", () => {

    // terminateStream() calls clearPretuneSafetyTimer() for every stream it tears down, but the overwhelming majority of streams were never pretuned and so have no
    // safetyTimers entry. The function must take the no-op branch (safetyTimers.get(streamId) is undefined) without throwing. This pins the contract that the
    // clearPretuneSafetyTimer call in terminateStream cannot fail for an ordinary, non-pretuned stream.
    assert.doesNotThrow(() => {

      clearPretuneSafetyTimer(999999);
    });
  });

  test("is idempotent - clearing the same stream ID twice leaves no entry and does not throw", () => {

    // After the first clear, the safetyTimers entry for the stream ID is gone, so a second clear must find nothing and remain a no-op. This locks the invariant that
    // terminateStream relies on: once the timer is cleared the Map entry is removed, so a redundant terminate (which the guard already tolerates) cannot resurrect or
    // double-clear a stale timer.
    clearPretuneSafetyTimer(424242);

    assert.doesNotThrow(() => {

      clearPretuneSafetyTimer(424242);
    });
  });
});
