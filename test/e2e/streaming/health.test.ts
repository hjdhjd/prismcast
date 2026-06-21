/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.test.ts: Integration coverage for the channel health and domain auth state. Health is recorded via fire-and-forget calls (markChannelSuccess /
 * markChannelFailure / markDomainAuth) that update an in-memory map and trigger a debounced flush to disk. The state hydrates from disk at startup via
 * loadHealthState (called by initializePersistence). This suite verifies the round-trip: mark, wait for flush, simulate restart, verify state hydrates.
 *
 * The 1481f0b fix (stale SSE updates after tab reload) is the structural concern: hydration must reflect the actual disk state, not stale module-state from
 * a prior process. Each integration test runs in its own context so state cannot leak across tests; within a test, we exercise the mark -> flush -> reload
 * cycle.
 */
import { createIntegrationContext, initializePersistence, waitForHealthFlush } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { getChannelHealth, getDomainAuth, getHealthSnapshot, loadHealthState, markChannelFailure, markChannelSuccess,
  markDomainAuth } from "../../../src/config/health.ts";
import assert from "node:assert/strict";

describe("channel health state", () => {

  test("markChannelSuccess records the timestamp and status, retrievable via getChannelHealth", async () => {

    /* The basic happy path: mark, retrieve, assert the recorded status matches. We capture timestamps with a window so test execution time variance does not
     * cause false failures.
     */
    await using ctx = await createIntegrationContext();

    // The void marks ctx as intentionally held only for its await-using disposal protocol so the no-unused-binding lint does not fire on the disposable.
    void ctx;
    await initializePersistence(ctx);

    const before = Date.now();

    markChannelSuccess("abc", "abc.com");

    const after = Date.now();

    const health = getChannelHealth("abc", "abc.com");

    assert.ok(health, "health record should exist after mark");
    assert.equal(health.status, "success", "status reflects the mark");
    assert.ok(health.timestamp >= before, "timestamp is after the mark started");
    assert.ok(health.timestamp <= after, "timestamp is before the mark finished");
  });

  test("markChannelFailure overrides a prior success on the same channel/domain", async () => {

    /* When a tune fails after previously succeeding, the channel's health flips. Last-write-wins on the same channel/domain key.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    markChannelSuccess("abc", "abc.com");
    markChannelFailure("abc", "abc.com");

    const health = getChannelHealth("abc", "abc.com");

    assert.equal(health?.status, "failed", "later failure overrides prior success");
  });

  test("markDomainAuth records a domain timestamp visible via getDomainAuth", async () => {

    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    markDomainAuth("hulu.com");

    assert.ok(getDomainAuth("hulu.com"), "domain auth timestamp should be recorded");
  });

  test("getHealthSnapshot returns the full in-memory state shape", async () => {

    /* The snapshot is what SSE handlers serialize when a client connects. Its shape must include channels and domains; the per-key counts should match the
     * marks we issued.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    markChannelSuccess("abc", "abc.com");
    markChannelSuccess("nbc", "nbc.com");
    markDomainAuth("hulu.com");

    const snapshot = getHealthSnapshot();

    assert.ok("channels" in snapshot, "snapshot has channels field");
    assert.ok("domains" in snapshot, "snapshot has domains field");

    // The marks above should be reflected; we don't pin the exact key shape because that's an internal serialization detail.
    assert.ok(Object.keys(snapshot.channels).length >= 2, "at least 2 channel entries in snapshot");
    assert.ok(Object.keys(snapshot.domains).length >= 1, "at least 1 domain entry in snapshot");
  });

  test("health state persists across a load cycle (save -> simulated restart -> load)", async () => {

    /* The full round-trip: mark, wait for the debounced flush, then re-call loadHealthState (simulating a restart that reads from disk). The marked state
     * should be present after reload.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    markChannelSuccess("abc", "abc.com");
    markDomainAuth("abc.com");

    await waitForHealthFlush();

    // Simulated restart: reload from disk. loadHealthState reads the persisted file and rehydrates the in-memory maps.
    await loadHealthState();

    const health = getChannelHealth("abc", "abc.com");

    assert.ok(health, "channel health should hydrate from disk");
    assert.equal(health.status, "success", "status preserved across reload");

    assert.ok(getDomainAuth("abc.com"), "domain auth should hydrate from disk");
  });
});
