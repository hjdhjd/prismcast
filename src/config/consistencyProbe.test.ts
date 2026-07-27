/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consistencyProbe.test.ts: Unit tests for the cross-store consistency probe. Coverage focuses on the public runConsistencyProbeAtStartup entrypoint and its
 * checks (service tag filter, variant canonical references, domain profile references). The probe runs against the actual loaded module-level state;
 * tests verify it does not throw under the unit-test default state (empty user data) and tolerates being called repeatedly.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { runConsistencyProbeAtStartup } from "./consistencyProbe.ts";

describe("runConsistencyProbeAtStartup", () => {

  test("runs to completion without throwing on the default state", async () => {

    /* The probe runs each registered check function against the loaded user data. Under the unit-test default (no user channels, no user profiles) every check
     * returns no issues and the function completes silently. The contract here: probe never throws even when issues are present; the most we can fail on is an
     * unhandled rejection from the implementation.
     */
    await assert.doesNotReject(() => runConsistencyProbeAtStartup());
  });

  test("is idempotent across repeated invocations", async () => {

    // Boundary: calling the probe a second time must not double-count or fail. Under the unit-test default, every check returns no issues, so the probe
    // short-circuits at the empty-issues guard before any logging or auto-fix runs... the repeated call is safe because nothing is generated, not because an
    // auto-fix is a no-op.
    await runConsistencyProbeAtStartup();
    await assert.doesNotReject(() => runConsistencyProbeAtStartup());
  });
});
