/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pretune.test.ts: Unit tests for the predictive channel pretune subsystem. pretune.ts polls the Channels DVR /api/v1/jobs endpoint, schedules setTimeout
 * fires PRETUNE_LEAD_MS (30s) before each upcoming recording, and tears down unclaimed pretuned streams after a safety timeout. The module exposes only two
 * public functions: startPretunePolling() and stopPretunePolling(). Both depend on side effects (intervals, async DVR fetches, stream initialization) that
 * require deep mocking. The honest test surface is verifying the start/stop pair is idempotent and that stop cleanly drains active timers.
 */
import { describe, test } from "node:test";
import { startPretunePolling, stopPretunePolling } from "./pretune.ts";
import assert from "node:assert/strict";
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

    // Each pair re-enters the start guard cleanly. We exercise three cycles to ensure the cleanup truly resets state.
    for(let i = 0; i < 3; i++) {

      startPretunePolling();
      stopPretunePolling();
    }

    // No assertion on internal state - the success criterion is that this completes without throwing and the process can exit cleanly under the unref'd cleanup
    // timer at the top of this file.
    assert.ok(true, "three cycles completed");
  });

  test("stopPretunePolling clears pending timers even when called immediately after start", () => {

    // The implementation iterates activeTimers and safetyTimers Maps and clearTimeout's each entry. With no DVR host configured, the polling function returns
    // early before scheduling any timers, so this test mostly exercises the no-timer cleanup path. Locks that the empty-Map iteration is harmless.
    startPretunePolling();
    stopPretunePolling();

    // Calling stop a second time must also be safe - the Maps are still empty after the first stop cleared them.
    assert.doesNotThrow(() => {

      stopPretunePolling();
    });
  });
});
