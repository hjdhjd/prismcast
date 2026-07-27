/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * healthBridge.test.ts: Tests for the reactive bridge that translates health/auth state changes into channel table patches over SSE. The bridge enforces the
 * single-source-of-truth rule for channel row presentation - server renders, client applies. These tests pin the routing behavior, the affected-keys
 * resolution, the snapshot-time catch-up patch, and the unsubscribe contract.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { buildSnapshotChannelPatch, installHealthBridge, resolveAffectedKeys } from "./healthBridge.ts";
import { loadHealthState, markChannelFailure, markChannelSuccess, markDomainAuth } from "../../../config/health.ts";
import { mkdtemp, rm } from "node:fs/promises";
import assert from "node:assert/strict";
import { initializeDataDir } from "../../../config/paths.ts";
import { initializeUserChannels } from "../../../config/userChannels.ts";
import os from "node:os";
import path from "node:path";
import { subscribeToStatus } from "../../../streaming/statusEmitter.ts";

describe("resolveAffectedKeys", () => {

  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-bridge-keys-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    // Clear the module-scope channelHealth and domainAuth maps by re-loading from the fresh (empty) data dir. Without this, in-memory state from prior tests
    // bleeds into the snapshot patch and the affected-keys resolution.
    await loadHealthState();
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("returns just the event's channelKey when no domain is supplied", () => {

    const keys = resolveAffectedKeys({ channelKey: "nbc", domain: "", status: "success", timestamp: 0 });

    assert.deepEqual(keys, ["nbc"]);
  });

  test("returns an empty array when neither channelKey nor domain is set (defensive against a malformed event)", () => {

    const keys = resolveAffectedKeys({ channelKey: "", domain: "", status: "success", timestamp: 0 });

    assert.deepEqual(keys, []);
  });

  test("returns just the channelKey when the domain matches no channels in the listing", () => {

    const keys = resolveAffectedKeys({ channelKey: "nbc", domain: "no-channel-matches-this-domain.invalid", status: "success", timestamp: 0 });

    assert.deepEqual(keys, ["nbc"]);
  });

  test("includes every channel sharing the event's resolved auth domain alongside the explicit channelKey, deduplicated", () => {

    // ABC is a multi-service predefined channel with site at abc.com. The canonical key resolves to the site service variant whose URL extracts to abc.com,
    // so an event for that domain should include "abc" in the affected set. Also pass the same key as channelKey to verify Set-based dedup.
    const keys = resolveAffectedKeys({ channelKey: "abc", domain: "abc.com", status: "success", timestamp: 0 });

    assert.equal(new Set(keys).size, keys.length, "no duplicates");
    assert.ok(keys.includes("abc"), "channelKey present");
  });
});

describe("installHealthBridge", () => {

  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-bridge-install-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    // Clear the module-scope channelHealth and domainAuth maps by re-loading from the fresh (empty) data dir. Without this, in-memory state from prior tests
    // bleeds into the snapshot patch and the affected-keys resolution.
    await loadHealthState();

    // Suppress the 2-second debounced flushHealthState() write timer that markChannelSuccess() schedules, so the test process can exit cleanly.
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(async () => {

    mock.timers.reset();
    await rm(dir, { force: true, recursive: true });
  });

  test("emits a channelUpdate event with a ChannelTablePatch when a health event fires", () => {

    const captured: { event: string; data: unknown }[] = [];
    const unsubscribeStatus = subscribeToStatus((event, data) => { captured.push({ data, event }); });
    const unsubscribeBridge = installHealthBridge();

    try {

      markChannelSuccess("nbc", "nbc.com");

      const channelUpdates = captured.filter((e) => e.event === "channelUpdate");

      assert.equal(channelUpdates.length, 1, "exactly one channelUpdate emission per health event");

      const patch = channelUpdates[0]?.data as { rows: { action: string; key: string }[] } | undefined;

      assert.ok(patch, "patch payload present");
      assert.ok(patch.rows.length >= 1, "patch carries at least the row for the channelKey");
      assert.ok(patch.rows.some((r) => r.key === "nbc"), "patch includes the channelKey row");
    } finally {

      unsubscribeBridge();
      unsubscribeStatus();
    }
  });

  test("emits a patch for a failure event (the health icon and login button both reflect the new state)", () => {

    const captured: { event: string; data: unknown }[] = [];
    const unsubscribeStatus = subscribeToStatus((event, data) => { captured.push({ data, event }); });
    const unsubscribeBridge = installHealthBridge();

    try {

      markChannelFailure("nbc", "nbc.com");

      const channelUpdates = captured.filter((e) => e.event === "channelUpdate");

      assert.equal(channelUpdates.length, 1, "failure events route through the bridge same as successes");
    } finally {

      unsubscribeBridge();
      unsubscribeStatus();
    }
  });

  test("emits a patch for markDomainAuth even though the event carries an empty channelKey", () => {

    /* markDomainAuth fires when a domain action proves auth without tuning a specific channel (e.g., precaching landing succeeds). The event's channelKey is
     * empty, so the affected-keys logic must rely on the domain alone to enumerate sibling rows.
     */
    const captured: { event: string; data: unknown }[] = [];
    const unsubscribeStatus = subscribeToStatus((event, data) => { captured.push({ data, event }); });
    const unsubscribeBridge = installHealthBridge();

    try {

      markDomainAuth("abc.com");

      const channelUpdates = captured.filter((e) => e.event === "channelUpdate");

      assert.equal(channelUpdates.length, 1, "domain-only events still produce a patch when the domain matches at least one channel");
    } finally {

      unsubscribeBridge();
      unsubscribeStatus();
    }
  });

  test("unsubscribe halts further emissions - the bridge stops responding once torn down", () => {

    const captured: { event: string; data: unknown }[] = [];
    const unsubscribeStatus = subscribeToStatus((event, data) => { captured.push({ data, event }); });
    const unsubscribeBridge = installHealthBridge();

    markChannelSuccess("nbc", "nbc.com");
    unsubscribeBridge();

    const beforeUnsubscribe = captured.filter((e) => e.event === "channelUpdate").length;

    try {

      markChannelSuccess("abc", "abc.com");

      const afterUnsubscribe = captured.filter((e) => e.event === "channelUpdate").length;

      assert.equal(afterUnsubscribe, beforeUnsubscribe, "no channelUpdate events fire after the bridge unsubscribes");
    } finally {

      unsubscribeStatus();
    }
  });
});

describe("buildSnapshotChannelPatch", () => {

  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-bridge-snapshot-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    // Clear the module-scope channelHealth and domainAuth maps by re-loading from the fresh (empty) data dir. Without this, in-memory state from prior tests
    // bleeds into the snapshot patch and the affected-keys resolution.
    await loadHealthState();
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(async () => {

    mock.timers.reset();
    await rm(dir, { force: true, recursive: true });
  });

  test("returns a patch with an empty rows array when no health or domain auth state exists (fresh install case)", () => {

    const patch = buildSnapshotChannelPatch();

    assert.ok(patch, "patch returned");
    assert.ok(Array.isArray(patch.rows), "rows is an array");
    assert.deepEqual(patch.rows, [], "no rows when no health state");
  });

  test("returns a patch covering every channel touched by the in-memory health state (channel entries + domain siblings)", () => {

    markChannelSuccess("nbc", "nbc.com");
    markDomainAuth("abc.com");

    const patch = buildSnapshotChannelPatch();

    assert.ok(patch.rows.length >= 1, "snapshot patch covers at least the explicitly-recorded channel");
    assert.ok(patch.rows.some((r) => r.key === "nbc"), "snapshot patch covers the recorded channelKey");
  });
});
