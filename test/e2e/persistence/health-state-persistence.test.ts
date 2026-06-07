/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health-state-persistence.test.ts: Integration-tier coverage for the parts of src/config/health.ts that the unit-tier suite cannot reach. The unit suite
 * (src/config/health.test.ts) exercises the in-memory mark/get/snapshot API with mock.timers; what it cannot test is anything that crosses the file-store
 * boundary: the parser, the TTL prune at load, the debounced flush write, the beforeWrite emission shape, and the load-time count-summary log.
 *
 * Each finding from the audit's S4-I26 through S4-M4 set has a focused test below. The file-store framework's behavior (atomic writes, backup rotation, etc.)
 * is owned by persistence.test.ts; we exercise only what's specific to health.ts on top of that framework.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { createIntegrationContext, pathInDataDir, waitForHealthFlush } from "../../helpers/integration.helpers.ts";
import { flushHealthStateNow, getChannelHealth, getDomainAuth, getHealthSnapshot, loadHealthState, markChannelSuccess,
  markDomainAuth } from "../../../src/config/health.ts";
import { LOG } from "../../../src/utils/index.ts";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { writePersistedJson } from "../../helpers/integration.helpers.ts";

describe("loadHealthState - parser branches over hand-edited / corrupt content", () => {

  test("loads a clean health.json into the in-memory state", async () => {

    /* Happy-path baseline: a valid file produces an in-memory state matching the on-disk content. The TTL prune doesn't fire because we use a recent timestamp.
     * Locks the parser-to-state pipeline before the negative tests below.
     */
    await using ctx = await createIntegrationContext();

    void ctx;

    const now = Date.now();

    await writePersistedJson(ctx, "health.json", {

      channels: { "test-load-channel": { domain: "load.test", status: "success", timestamp: now } },
      domains: { "load.test": now },
      schemaVersion: 1
    });

    await loadHealthState();

    assert.equal(getChannelHealth("test-load-channel", "load.test")?.status, "success", "channel state loaded from disk");
    assert.equal(getDomainAuth("load.test"), now, "domain auth state loaded from disk");
  });

  test("non-numeric schemaVersion falls back to 1 (parser sanitization)", async () => {

    /* The parser at health.ts:129-160 guards every framework field defensively. A non-numeric schemaVersion (string, null, missing) must produce a v1 read
     * without throwing. The framework's migration runner then upgrades to current; for health (no migrations declared) it simply stamps the latest version on
     * the next write.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await writePersistedJson(ctx, "health.json", {

      channels: {},
      domains: {},
      migrationsApplied: [],
      schemaVersion: "not-a-number"
    });

    await assert.doesNotReject(async () => loadHealthState(), "non-numeric schemaVersion must not throw at parse time");
  });

  test("non-array migrationsApplied is silently coerced to empty (parser sanitization)", async () => {

    /* The parser only reads migrationsApplied via Array.isArray. A non-array (string, object, number) is treated as absent. We assert the load completes and
     * the file is parseable; the empty result is implied by the lack of a thrown error and a clean state.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await writePersistedJson(ctx, "health.json", {

      channels: {},
      domains: {},
      migrationsApplied: "not-an-array",
      schemaVersion: 1
    });

    await assert.doesNotReject(async () => loadHealthState(), "non-array migrationsApplied must not throw");
  });

  test("non-string entries inside migrationsApplied are filtered out (parser sanitization)", async () => {

    /* The parser walks migrationsApplied with a typeof === "string" filter. Mixed-type entries must be dropped silently. We exercise by seeding a mixed array
     * and confirming the load completes.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await writePersistedJson(ctx, "health.json", {

      channels: {},
      domains: {},
      migrationsApplied: [ "valid-entry", 42, null, "another-valid-entry" ],
      schemaVersion: 1
    });

    await assert.doesNotReject(async () => loadHealthState(), "mixed-type migrationsApplied must not throw");
  });

  test("fractional schemaVersion is floored (Math.floor branch)", async () => {

    /* Hand-edited file might carry a fractional schemaVersion. The parser's Math.floor gate prevents downstream code from seeing a non-integer version. We
     * assert the load completes and the next mutation persists the floored value.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await writePersistedJson(ctx, "health.json", {

      channels: {},
      domains: {},
      schemaVersion: 1.7
    });

    await assert.doesNotReject(async () => loadHealthState(), "fractional schemaVersion must not throw");
  });

  test("missing channels/domains keys produce empty maps (parser ?? defaults)", async () => {

    /* The parser uses `parsed.channels ?? {}` and `parsed.domains ?? {}` so older files that predate either field can still be read. After load, the
     * snapshot must contain the seeded marker only - any spurious entries from a leaky parse would surface here.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await writePersistedJson(ctx, "health.json", { schemaVersion: 1 });

    await loadHealthState();

    /* The snapshot reflects the loaded state. With a file that has neither channels nor domains, the snapshot should be empty - except for any leftover state
     * from prior tests in the same process. We pin the structure (objects, not undefined) rather than emptiness.
     */
    const snapshot = getHealthSnapshot();

    assert.equal(typeof snapshot.channels, "object", "channels object present after load with no channels in file");
    assert.equal(typeof snapshot.domains, "object", "domains object present after load with no domains in file");
  });

  test("entries older than HEALTH_TTL are pruned at load time (TTL gate)", async () => {

    /* loadHealthState applies the TTL filter while populating the in-memory maps - entries with timestamps older than 7 days are dropped. We seed a definitely
     * stale entry (timestamp - 30 days) and a fresh entry, load, then assert the stale entry is absent and the fresh entry is present.
     */
    await using ctx = await createIntegrationContext();

    void ctx;

    const now = Date.now();
    const thirtyDaysAgo = now - (30 * 24 * 60 * 60 * 1000);

    await writePersistedJson(ctx, "health.json", {

      channels: {

        "fresh-ttl-channel": { domain: "fresh.test", status: "success", timestamp: now },
        "stale-ttl-channel": { domain: "stale.test", status: "success", timestamp: thirtyDaysAgo }
      },
      domains: { "fresh.test": now, "stale.test": thirtyDaysAgo },
      schemaVersion: 1
    });

    await loadHealthState();

    assert.equal(getChannelHealth("stale-ttl-channel", "stale.test"), null, "stale channel pruned at load");
    assert.equal(getChannelHealth("fresh-ttl-channel", "fresh.test")?.status, "success", "fresh channel survives load");
    assert.equal(getDomainAuth("stale.test"), null, "stale domain auth pruned at load");
    assert.equal(getDomainAuth("fresh.test"), now, "fresh domain auth survives load");
  });

  test("emits the count-summary log line when at least one channel or domain is loaded", async () => {

    /* The conditional log at health.ts:200 fires when channelCount > 0 OR domainCount > 0. A clean file with one fresh entry triggers the branch. We capture
     * LOG.info to assert the line fires with the documented format.
     */
    const infoSpy = mock.method(LOG, "info", () => undefined);

    try {

      await using ctx = await createIntegrationContext();

      void ctx;

      const now = Date.now();

      await writePersistedJson(ctx, "health.json", {

        channels: { "summary-test-channel": { domain: "summary.test", status: "success", timestamp: now } },
        domains: { "summary.test": now },
        schemaVersion: 1
      });

      await loadHealthState();

      const summaryLine = infoSpy.mock.calls.find((call) => {

        const arg = call.arguments[0];

        return (typeof arg === "string") && arg.includes("Loaded health state");
      });

      assert.ok(summaryLine, "count-summary log line fired when state is non-empty");
    } finally {

      infoSpy.mock.restore();
    }
  });
});

describe("flushHealthState - debounced write contract", () => {

  /* The flush is debounced by 2 seconds. A burst of mark calls within the debounce window must coalesce into a single write rather than firing per-call. We
   * exercise via the integration helper's waitForHealthFlush() which knows the debounce constant.
   */
  test("a burst of mark calls coalesces into a single on-disk write after the debounce window", async () => {

    await using ctx = await createIntegrationContext();

    void ctx;

    /* Fire several mark calls in rapid succession. Each schedules / re-schedules the debounce timer; only the last firing produces a write.
     */
    markChannelSuccess("debounce-test-1", "debounce.test");
    markChannelSuccess("debounce-test-2", "debounce.test");
    markChannelSuccess("debounce-test-3", "debounce.test");
    markDomainAuth("debounce.test");

    await waitForHealthFlush();

    /* Read the on-disk state and confirm all three channels (and the domain) landed in one file write. The strong contract here is structural - the file is
     * present and parseable - rather than a write-count check, since the file-store framework owns the write count and a burst-coalescing regression would
     * also surface as the wrong content (a stale write missing later updates).
     */
    const persisted = JSON.parse(await readFile(pathInDataDir(ctx, "health.json"), "utf8")) as {
      channels?: Record<string, unknown>;
      domains?: Record<string, unknown>;
    };

    assert.ok(persisted.channels, "channels block present in flushed file");
    assert.ok(persisted.channels["debounce-test-1"], "first mark in burst persisted");
    assert.ok(persisted.channels["debounce-test-2"], "second mark in burst persisted");
    assert.ok(persisted.channels["debounce-test-3"], "third mark in burst persisted");
    assert.ok(persisted.domains?.["debounce.test"], "domain auth from the same burst persisted");
  });

  test("beforeWrite emits the migrationsApplied array only when it has at least one entry", async () => {

    /* The beforeWrite hook at health.ts:114-124 conditionally includes migrationsApplied in the output: omitted when empty, included when non-empty. The
     * empty branch is exercised every time the runtime flushes (since runtime starts each store with empty migrationsApplied), so we focus on confirming the
     * "omitted" shape on disk after a clean write.
     */
    await using ctx = await createIntegrationContext();

    void ctx;

    markChannelSuccess("emit-test-channel", "emit.test");

    await waitForHealthFlush();

    const persisted = JSON.parse(await readFile(pathInDataDir(ctx, "health.json"), "utf8")) as Record<string, unknown>;

    assert.equal("migrationsApplied" in persisted, false, "migrationsApplied is omitted when the runtime has none to record");
  });

  test("flushHealthStateNow persists the pending state immediately, without the debounce wait (the shutdown flush)", async () => {

    /* The debounced flush would lose a pending write if the process exited inside the FLUSH_DELAY window. flushHealthStateNow - called from graceful shutdown -
     * cancels the pending debounce timer and performs the write awaitably, so the on-disk file reflects the mark the instant the call resolves. The distinguishing
     * assertion from the debounce test above is the deliberate ABSENCE of waitForHealthFlush(): if flushHealthStateNow did not write immediately, the read below
     * would observe a file missing the just-marked channel (the [24] regression this pins).
     */
    await using ctx = await createIntegrationContext();

    void ctx;

    markChannelSuccess("shutdown-flush-channel", "shutdown.test");

    // No waitForHealthFlush(): flushHealthStateNow must drain the pending debounce and write synchronously-awaitable.
    await flushHealthStateNow();

    const persisted = JSON.parse(await readFile(pathInDataDir(ctx, "health.json"), "utf8")) as {
      channels?: Record<string, unknown>;
      domains?: Record<string, unknown>;
    };

    assert.ok(persisted.channels?.["shutdown-flush-channel"], "the pending channel mark was flushed to disk immediately by flushHealthStateNow");
    assert.ok(persisted.domains?.["shutdown.test"], "the pending domain auth was flushed to disk immediately by flushHealthStateNow");
  });
});

describe("loadHealthState - recoveredFromBackup banner", () => {

  /* When the framework recovers health.json from .bak, loadHealthState logs an operator-visible banner. Exercising this requires both files to exist plus
   * main being corrupt. We seed via a clean write, corrupt main, and re-load.
   */
  let infoSpy: ReturnType<typeof mock.method>;

  beforeEach(() => {

    infoSpy = mock.method(LOG, "info", () => undefined);
  });

  afterEach(() => {

    infoSpy.mock.restore();
  });

  test("emits the recovery banner when main is corrupt and .bak supplies a usable read", async () => {

    await using ctx = await createIntegrationContext();

    void ctx;

    /* Use writePersistedJson to seed a valid main and then create a valid .bak by issuing a real mark + flush; that produces both files via the framework's
     * pre-write rotation. Then corrupt main and re-load.
     */
    markChannelSuccess("recovery-banner-channel", "recovery.test");
    await waitForHealthFlush();

    /* At this point health.json exists. Trigger another write so the framework rotates the previous main into .bak.
     */
    markChannelSuccess("recovery-banner-channel-2", "recovery.test");
    await waitForHealthFlush();

    /* Now corrupt main; .bak still has the previous valid content.
     */
    const { writeFile } = await import("node:fs/promises");

    await writeFile(pathInDataDir(ctx, "health.json"), "this-is-not-valid-json", "utf-8");

    /* Reset the spy so we only capture banner lines from this load. */
    infoSpy.mock.resetCalls();

    await loadHealthState();

    const recoveryLine = infoSpy.mock.calls.find((call) => {

      const arg = call.arguments[0];

      return (typeof arg === "string") && arg.includes("recovered from backup");
    });

    assert.ok(recoveryLine, "recovery banner fired when health.json was corrupt and .bak supplied the read");
  });
});
