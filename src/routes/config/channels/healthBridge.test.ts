/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * healthBridge.test.ts: Tests for the reactive bridge that translates health/auth state changes into channel table patches over SSE. The bridge enforces the
 * single-source-of-truth rule for channel row presentation - server renders, client applies. These tests assert the routing behavior, the affected-keys
 * resolution, the reach of the domain fan-out across rows the listing hides, the snapshot-time catch-up patch, and the unsubscribe contract.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { buildSnapshotChannelPatch, installHealthBridge, resolveAffectedKeys } from "./healthBridge.ts";
import { disablePredefinedChannels, enablePredefinedChannels, getChannelListing, initializeUserChannels } from "../../../config/userChannels.ts";
import { loadHealthState, markChannelFailure, markChannelSuccess, markDomainAuth } from "../../../config/health.ts";
import { mkdtemp, rm } from "node:fs/promises";
import assert from "node:assert/strict";
import { initializeDataDir } from "../../../config/paths.ts";
import { mutateEnabledServices } from "../../../config/services.ts";
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

describe("domain fan-out across rows the listing hides", () => {

  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-bridge-hidden-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    // Clear the module-scope channelHealth and domainAuth maps by re-loading from the fresh (empty) data dir. Without this, in-memory state from prior tests
    // bleeds into the snapshot patch and the affected-keys resolution.
    await loadHealthState();
  });

  afterEach(async () => {

    mock.timers.reset();
    await rm(dir, { force: true, recursive: true });
  });

  test("a disabled predefined channel sharing the domain stays in the affected set and the snapshot patch", async () => {

    /* A disabled row is hidden by a class rather than removed from the table, and the client applies an update to it wherever it sits. A fan-out that skipped
     * it would leave the operator toggling "show disabled" onto a health indicator frozen at whatever it showed when the row was last rendered. The catalog is
     * restored inside the test so the rows that follow see the same channel set this one started from.
     */
    await disablePredefinedChannels(["abc"]);

    try {

      const hidden = getChannelListing().find((entry) => entry.key === "abc");

      assert.equal(hidden?.enabled, false, "the channel is disabled, so the listing hides its row");

      const affected = resolveAffectedKeys({ channelKey: "", domain: "abc.com", status: "success", timestamp: 0 });

      assert.ok(affected.includes("abc"), "a disabled channel sharing the domain belongs in the affected set");

      // Suppress the debounced flushHealthState() write timer that markDomainAuth schedules, so nothing writes into the temp directory after it is removed.
      mock.timers.enable({ apis: ["setTimeout"] });
      markDomainAuth("abc.com");

      assert.ok(buildSnapshotChannelPatch().rows.some((row) => row.key === "abc"), "the catch-up snapshot patch carries the disabled channel's row");
    } finally {

      mock.timers.reset();
      await enablePredefinedChannels(["abc"]);
    }
  });

  test("a channel the service filter excludes stays in the affected set and the snapshot patch", async () => {

    /* The service filter hides a row the same way the disabled flag does. "msg" is the channel to test it with because it carries only the DirecTV tag, so a
     * filter naming another service excludes it; a channel carrying the "direct" tag could not be excluded by any filter at all, since that tag counts as
     * enabled whatever the filter says.
     */
    await mutateEnabledServices(["hulu"]);

    try {

      const hidden = getChannelListing().find((entry) => entry.key === "msg");

      assert.equal(hidden?.availableByService, false, "the service filter excludes the channel, so the listing hides its row");

      const affected = resolveAffectedKeys({ channelKey: "", domain: "directv.com", status: "success", timestamp: 0 });

      assert.ok(affected.includes("msg"), "a service-filtered channel sharing the domain belongs in the affected set");

      mock.timers.enable({ apis: ["setTimeout"] });
      markDomainAuth("directv.com");

      assert.ok(buildSnapshotChannelPatch().rows.some((row) => row.key === "msg"), "the catch-up snapshot patch carries the service-filtered channel's row");
    } finally {

      mock.timers.reset();
      await mutateEnabledServices([]);
    }
  });
});
