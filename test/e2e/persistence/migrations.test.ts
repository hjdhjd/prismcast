/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * migrations.test.ts: Integration coverage for the channel-store schema migration runner against real on-disk fixtures. Unit tests in persistence.test.ts
 * exercise the migration FRAMEWORK (run-in-order, gap detection, forward-compat, re-running safely) against synthetic stores; this suite exercises the actual
 * production migrations declared in userChannels.ts against canonical historical fixtures, catching regressions where a future migration breaks an older
 * shape's upgrade path. The two production migrations are:
 *
 *   - schema v1 -> v2: stamp canonicalKey on hyphenated user channel entries whose identity matches a predefined canonical.
 *   - schema v2 -> v3: rename foxcom service references to foxone (and the special case fox-site -> fox-foxone).
 *
 * Each test seeds a fresh data directory with a v1 channels.json (no schemaVersion field, treated as version 1 by the framework), then drives the migration
 * runner through the initializePersistence helper. That helper calls ensureAllMigrated() first, which runs each store's ensureMigrated() and persists the
 * upgrade to disk; only afterward does it call the load functions (initializeUserChannels) to hydrate module state from the already-migrated, already-persisted
 * content. After ensureAllMigrated has run, the on-disk file is re-read to verify the upgrade landed durably (not just in memory).
 */
import { createIntegrationContext, initializePersistence, readPersistedJson, writePersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("persistence migrations", () => {

  test("v1 channels file with no schemaVersion field migrates to v3 and records the audit trail", async () => {

    /* Seed: a v1-shape channels.json (no schemaVersion key). The framework's parser treats absent schemaVersion as 1, so the runner must apply v2 and v3
     * sequentially. After ensureAllMigrated runs (driven by initializePersistence), the on-disk file should carry schemaVersion: 3 and a migrationsApplied
     * audit list with both descriptions in order.
     */
    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "channels.json", {

      // No schemaVersion - framework treats as v1.
    });

    await initializePersistence(ctx);

    const persisted = await readPersistedJson(ctx, "channels.json");

    assert.ok((typeof persisted === "object") && (persisted !== null), "channels.json should be a JSON object");
    assert.equal((persisted as { schemaVersion: unknown }).schemaVersion, 3, "schemaVersion should be at the current version");

    const migrationsApplied = (persisted as { migrationsApplied?: unknown }).migrationsApplied;

    assert.ok(Array.isArray(migrationsApplied), "migrationsApplied audit trail should be present");
    assert.equal(migrationsApplied.length, 2, "two migrations should have been applied (v1->v2 and v2->v3)");
    assert.match(migrationsApplied[0] as string, /canonicalKey/i, "first migration should be the canonicalKey stamping");
    assert.match(migrationsApplied[1] as string, /foxcom|foxone/i, "second migration should be the foxcom rename");
  });

  test("v2 channels with foxcom service selection rename to foxone after migration", async () => {

    /* Seed a v2-shape channels.json with a serviceSelection referencing a -foxcom variant on the fox canonical. The v2->v3 migration renames it to -foxone.
     * We use the fox canonical specifically because it's a real predefined canonical with a real fox-foxone variant - any other canonical would have its
     * selection cleaned up at startup as referencing a non-existent variant (initializeUserChannels validates selections against the rebuilt service group
     * taxonomy).
     */
    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "channels.json", {

      schemaVersion: 2,
      serviceSelections: { fox: "fox-foxcom" }
    });

    await initializePersistence(ctx);

    const persisted = await readPersistedJson(ctx, "channels.json");
    const selections = (persisted as { serviceSelections?: Record<string, string> }).serviceSelections;

    assert.ok(selections, "serviceSelections should still be present after migration");
    assert.equal(selections["fox"], "fox-foxone", "fox-foxcom should rename to fox-foxone");
  });

  test("foxcom-suffixed channel keys are renamed to foxone-suffixed during v2->v3", async () => {

    await using ctx = await createIntegrationContext();

    /* The v2->v3 migration renames channel KEYS that end in -foxcom to -foxone. We seed a v2 file with a fox-foxcom variant entry whose URL DIFFERS from the
     * predefined fox-foxone (a custom-mirror URL) so normalizeChannelDeltas does not strip it as redundant - if the user's stored entry matched the
     * predefined defaults exactly, normalization would correctly drop it as a no-op delta. The custom URL ensures the migration's key-rename is observable
     * after normalization.
     */
    await writePersistedJson(ctx, "channels.json", {

      "fox-foxcom": { canonicalKey: "fox", url: "https://custom-fox-mirror.example.test/" },
      schemaVersion: 2
    });

    await initializePersistence(ctx);

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.ok("fox-foxone" in persisted, "renamed key fox-foxone should be present");
    assert.equal("fox-foxcom" in persisted, false, "old key fox-foxcom should be gone");
    assert.equal((persisted["fox-foxone"] as { url: string }).url, "https://custom-fox-mirror.example.test/", "the renamed entry should preserve its url");
  });

  test("a file already at the current schema version is a no-op (repeat-safe)", async () => {

    /* Seed a file that already declares schemaVersion: 3. The runner should treat it as up-to-date and not record any new entries in migrationsApplied. We
     * also re-call initializePersistence a second time to prove no migrations are re-applied even after a full reload cycle.
     */
    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "channels.json", {

      migrationsApplied: ["seeded-historical-entry"],
      schemaVersion: 3
    });

    await initializePersistence(ctx);

    const after = await readPersistedJson(ctx, "channels.json") as { migrationsApplied: unknown[]; schemaVersion: number };

    assert.equal(after.schemaVersion, 3, "schemaVersion should still be 3");
    assert.deepEqual(after.migrationsApplied, ["seeded-historical-entry"], "no new migrations should be appended when already at current");
  });

  test("forward-compatible read: a file declaring a newer schemaVersion is preserved without rolling back", async () => {

    /* If a future PrismCast version writes schemaVersion: 99 and an older binary reads it, the framework logs and proceeds without applying migrations -
     * downgrading would be more dangerous than tolerating unknown fields. We assert this contract: the file survives an init pass without losing its declared
     * version or its content.
     */
    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "channels.json", {

      "future-channel": { name: "Future", url: "https://example.test/future" },
      schemaVersion: 99
    });

    await initializePersistence(ctx);

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.equal(persisted["schemaVersion"], 99, "newer schemaVersion should be preserved on disk");
    assert.ok("future-channel" in persisted, "the unknown-future-shape entry should not be lost");
  });
});

describe("persistence migrations - chain ordering and repeat safety across stores", () => {

  /* This suite asserts the runner's behavior across two regimes that the per-step tests above did not directly exercise:
   *
   *   1. Chain coverage: a v1-fixture boot must apply v2 AND v3 in chronological order, with each step's transformation visible in the final on-disk state. The
   *      per-step tests in the prior describe block cover individual steps; these tests exercise both ends of the chain together. We exercise the chain on both
   *      stores that carry migrations (config.json's enabledProviders/foxcom/dvrHost migrations and channels.json's canonicalKey/foxcom-rename migrations) so the
   *      runner's contract holds independent of which store registers it.
   *
   *   2. Skip-already-applied no-op: a v2 fixture must skip v2 and apply only v3; a v3 fixture must skip both. The skip-fully-current case is covered by
   *      the existing test verifying that a file already at the current schema version is a no-op; this block adds the partial-skip case so the runner's
   *      version-walking loop is asserted end-to-end.
   *
   * Migration descriptions are hard-coded constants in production (configMigrations and channelsMigrations registries). Tests reference them via inline string
   * literals because the registries are module-internal - the constants live in exactly one place in production, so a future rename forces a deliberate test
   * update rather than a silent drift. This is the same SSOT shape the existing tests use (regex match on the description); these tests tighten it to exact
   * equality so the guarantee that migrations run in chain order is enforced structurally.
   */

  // Production-canonical migration descriptions. Sourced from configMigrations / channelsMigrations in src/config/userConfig.ts and src/config/userChannels.ts. See
  // the block comment above for why these live as test-side constants.
  const CONFIG_V2_DESCRIPTION = "Rename legacy provider-themed channel field names and foxcom service tag to foxone";
  const CONFIG_V3_DESCRIPTION = "Move dvrHost into channelsDvr.host (split legacy host:port format)";
  const CHANNELS_V2_DESCRIPTION = "Stamp canonicalKey on legacy user channel variant entries";
  const CHANNELS_V3_DESCRIPTION = "Rename foxcom service references to foxone and persist legacy field name cleanup";

  test("v1 config.json with v2 + v3 legacy fields chains both transformations and records descriptions in version order", async () => {

    /* Seed a config.json with no schemaVersion (treated as v1), an enabledProviders array (v2 renames to enabledServices and remaps foxcom -> foxone), AND a
     * legacy dvrHost with an embedded port (v3 splits into channelsDvr.host + channelsDvr.port). After ensureAllMigrated runs, all three transformations must be
     * visible together and migrationsApplied must list v2's description before v3's.
     */
    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "config.json", {

      channels: { enabledProviders: [ "hulu", "foxcom" ] },
      dvrHost: "192.168.1.5:9999"

      // No schemaVersion - framework treats as v1.
    });

    await initializePersistence(ctx);

    const persisted = await readPersistedJson(ctx, "config.json") as {
      channels?: { enabledServices?: unknown[]; enabledProviders?: unknown };
      channelsDvr?: { host?: string; port?: number };
      migrationsApplied?: unknown[];
      schemaVersion?: number;
    };

    assert.equal(persisted.schemaVersion, 3, "config.json should be at the current schema version after the chain");

    // v2's transformations: enabledProviders renamed AND foxcom remapped to foxone.
    assert.equal(persisted.channels?.enabledProviders, undefined, "legacy enabledProviders key should be deleted by v2");
    assert.deepEqual(persisted.channels?.enabledServices, [ "hulu", "foxone" ], "v2 should rename enabledProviders and remap foxcom to foxone");

    // v3's transformation: dvrHost split into channelsDvr.host + channelsDvr.port.
    const channelsDvr = persisted.channelsDvr ?? {};

    assert.equal(channelsDvr.host, "192.168.1.5", "v3 should split host portion into channelsDvr.host");
    assert.equal(channelsDvr.port, 9999, "v3 should split embedded port into channelsDvr.port");
    assert.equal((persisted as { dvrHost?: unknown }).dvrHost, undefined, "legacy dvrHost field should be deleted by v3");

    // Application order is the runner's structural rule: applied[0] is the version it ran first.
    assert.deepEqual(persisted.migrationsApplied, [ CONFIG_V2_DESCRIPTION, CONFIG_V3_DESCRIPTION ], "migrationsApplied should list v2 before v3");
  });

  test("v1 channels.json chains canonicalKey stamping (v2) and foxcom rename (v3) in a single boot", async () => {

    /* Seed a v1 channels.json that simultaneously triggers BOTH channel migrations: a hyphenated user entry that v2 will canonicalKey-stamp AND a serviceSelection
     * referencing -foxcom that v3 will rename. After the boot, the canonicalKey is stamped AND the selection is renamed AND migrationsApplied lists both
     * descriptions in version order.
     *
     * The hyphenated entry has a key whose prefix is a real predefined canonical (abc) and a custom URL only - no identity fields are set, so the v2
     * shape-compatibility classifier (collectLegacyVariantStamps) sees every CHANNEL_IDENTITY_KEY as undefined-on-stored-side and stamps the entry. Identity
     * divergence (e.g., a custom name) would correctly mark the entry as a user standalone and skip it; we deliberately avoid that path. The fox-foxcom
     * selection refers to the well-known fox-foxone variant post-rename. Together these prove that v2 and v3 ran on the same boot against the same file.
     */
    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "channels.json", {

      "abc-custom": { url: "https://abc-custom.example.test/" },
      serviceSelections: { fox: "fox-foxcom" }

      // No schemaVersion - framework treats as v1.
    });

    await initializePersistence(ctx);

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & {
      migrationsApplied?: unknown[];
      schemaVersion?: number;
      serviceSelections?: Record<string, string>;
    };

    assert.equal(persisted.schemaVersion, 3, "channels.json should be at the current schema version after the chain");

    // v2's transformation: canonicalKey stamped on the hyphenated entry whose prefix matches a predefined canonical.
    const abcEntry = persisted["abc-custom"] as { canonicalKey?: string; url?: string } | undefined;

    assert.ok(abcEntry, "abc-custom entry should still exist after migration");
    assert.equal(abcEntry.canonicalKey, "abc", "v2 should stamp canonicalKey on the hyphenated user entry");

    // v3's transformation: foxcom selection renamed to foxone.
    assert.equal(persisted.serviceSelections?.["fox"], "fox-foxone", "v3 should rename fox-foxcom to fox-foxone");

    // Application order: v2 description before v3 description in migrationsApplied.
    assert.deepEqual(persisted.migrationsApplied, [ CHANNELS_V2_DESCRIPTION, CHANNELS_V3_DESCRIPTION ], "migrationsApplied should list v2 before v3");
  });

  test("v2 config.json skips v2 and applies only v3 (partial-chain repeat safety)", async () => {

    /* The runner walks from currentVersion + 1 up to currentSchemaVersion. A file declaring schemaVersion: 2 must skip v2 and apply only v3 - migrationsApplied
     * carries exactly one new entry (v3's description), schemaVersion ends at 3, the v3 transformation is visible. The structural assertion is the migrationsApplied
     * shape: only v3's description is appended, never v2's. (Note: the persisted file is rewritten by filterDefaults on every save, which strips legacy
     * unknown keys regardless of migration state; we therefore assert the chain-runner contract via migrationsApplied + schemaVersion + the v3 transformation,
     * not via legacy-key survival.)
     */
    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "config.json", {

      dvrHost: "10.0.0.5",
      migrationsApplied: ["seeded-v2-applied"],
      schemaVersion: 2
    });

    await initializePersistence(ctx);

    const persisted = await readPersistedJson(ctx, "config.json") as {
      channelsDvr?: { host?: string };
      migrationsApplied?: unknown[];
      schemaVersion?: number;
    };

    assert.equal(persisted.schemaVersion, 3, "schemaVersion should advance to 3");

    // The positive case: v3 ran exactly once.
    assert.equal(persisted.channelsDvr?.host, "10.0.0.5", "v3 should split dvrHost into channelsDvr.host");

    // The structural rule: migrationsApplied carries v3's description only - never v2's. The seeded historical entry is preserved by the recordMigration hook's
    // append-only contract.
    assert.deepEqual(persisted.migrationsApplied, [ "seeded-v2-applied", CONFIG_V3_DESCRIPTION ],
      "the seeded v2-applied entry should be preserved and only v3's description appended");
    assert.equal(persisted.migrationsApplied.includes(CONFIG_V2_DESCRIPTION), false,
      "v2 was already declared as applied; its description must not be re-appended");
  });

  test("v3 channels.json skips both migrations (full-chain repeat safety)", async () => {

    /* A file already at the current schema version must not have any migration re-applied. The existing no-op-on-repeat test in the prior describe block covers an
     * empty fixture; here we use a richer fixture whose contents would be visibly mutated if either migration ran erroneously. We seed a -foxcom-suffixed key
     * (which v3 would rename if executed) and assert it survives byte-for-byte. The negative observation (no rename) is the structural assertion.
     */
    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "channels.json", {

      "abc-foxcom": { canonicalKey: "abc", url: "https://abc-foxcom.example.test/" },
      migrationsApplied: ["seeded-historical"],
      schemaVersion: 3
    });

    await initializePersistence(ctx);

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & {
      migrationsApplied?: unknown[];
      schemaVersion?: number;
    };

    assert.equal(persisted.schemaVersion, 3, "schemaVersion should still be 3 (no migration ran)");
    assert.deepEqual(persisted.migrationsApplied, ["seeded-historical"], "no new migration entries should be appended when already at current");

    // The negative case: v3 was skipped. The -foxcom-suffixed key survives.
    assert.ok("abc-foxcom" in persisted, "v3 was skipped; the -foxcom-suffixed key must survive byte-for-byte");
    assert.equal("abc-foxone" in persisted, false, "v3 was skipped; no foxone-renamed shadow entry should be created");
  });
});
