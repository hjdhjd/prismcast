/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userConfig.migrations.test.ts: Focused coverage for the schema-migration apply functions exported from userConfig.ts - one-time correctness invariants
 * applied at read time. If a migration gets a transformation wrong, every subsequent session sees corrupted state, so each architectural choice gets a test
 * that pins it explicitly: tests document the design via the assertions, not via comments alone.
 *
 * v2 (applyChannelsProviderRenameMigration): renames legacy provider-themed channel fields and the foxcom service tag.
 * v3 (applyDvrHostNamespaceMigration): moves top-level dvrHost into channelsDvr.host, splitting any embedded host:port form at the LAST colon so bracketed
 *   IPv6 ([::1]:8089) survives. Invalid trailing portion (non-numeric, out-of-range port) falls back to host-only handling rather than fabricating a port.
 *
 * Other migration framework behavior (audit-trail recording, schema-version stamping, version sequencing) is owned by the persistence framework and tested
 * in src/config/persistence.test.ts; this file is exclusively about the per-version transformation correctness.
 */
import { applyChannelsProviderRenameMigration, applyDvrHostNamespaceMigration } from "./userConfig.ts";
import { describe, test } from "node:test";
import type { UserConfig } from "./userConfig.ts";
import assert from "node:assert/strict";

// makePreV3 builds the on-disk shape a v2 config carries: the legacy top-level dvrHost field plus whatever channelsDvr state already exists. The literal cast
// reflects that dvrHost is no longer declared on the current UserConfig type; on disk it predates the v3 namespace move.
function makePreV3(overrides: { dvrHost?: string; channelsDvr?: { host?: string; port?: number } } = {}): UserConfig {

  return overrides;
}

// requireChannelsDvr narrows data.channelsDvr from optional to defined for assertion sites. The migration always populates it when there was a legacy dvrHost,
// so each test asserts that contract first and then reads through the narrowed reference.
function requireChannelsDvr(data: UserConfig): { host?: string; port?: number } {

  assert.ok(data.channelsDvr, "migration must populate channelsDvr after running on a dvrHost-bearing input");

  return data.channelsDvr;
}

// requireChannels narrows data.channels from optional to a permissive record for assertion sites in the v2 tests. The cast through Record<string, unknown> is
// load-bearing: the legacy keys (enabledProviders, precacheProviders) are not declared on UserChannelsConfig, so reading them through the typed view would be
// a compile error. Tests assert presence/absence at the bracket-access level; the record shape mirrors what the migration mutates.
function requireChannels(data: UserConfig): Record<string, unknown> {

  assert.ok(data.channels, "test fixture must include a channels block");

  return data.channels as Record<string, unknown>;
}

describe("v3 dvrHost namespace migration - host:port splitting", () => {

  test("a bare host (no colon) migrates to channelsDvr.host with no port change", () => {

    /* The simplest case: the legacy field already contains a host-only value. The migration moves it to channelsDvr.host verbatim and does not touch the
     * port - whatever the user (or the default) had for channelsDvr.port stays exactly as-is.
     */
    const data = makePreV3({ dvrHost: "192.168.1.5" });

    applyDvrHostNamespaceMigration(data);

    const channelsDvr = requireChannelsDvr(data);

    assert.equal(channelsDvr.host, "192.168.1.5", "host moves to channelsDvr.host verbatim");
    assert.equal(channelsDvr.port, undefined, "no embedded port means port stays untouched");
    assert.equal((data as { dvrHost?: string }).dvrHost, undefined, "the legacy top-level field is removed");
  });

  test("a host:port value with no user-set port splits the port into channelsDvr.port", () => {

    /* The legacy file format that motivated the migration: a host:port string at the top level. The host portion lands at channelsDvr.host (host-only) and
     * the port portion lands at channelsDvr.port. The user has not customized the port (no channelsDvr.port set), so the embedded port is the most informed
     * value available - migrate it.
     */
    const data = makePreV3({ dvrHost: "192.168.1.5:9000" });

    applyDvrHostNamespaceMigration(data);

    const channelsDvr = requireChannelsDvr(data);

    assert.equal(channelsDvr.host, "192.168.1.5", "host portion lands at channelsDvr.host (no embedded colon)");
    assert.equal(channelsDvr.port, 9000, "embedded port migrates because the user had not customized it");
    assert.equal((data as { dvrHost?: string }).dvrHost, undefined, "the legacy top-level field is removed");
  });

  test("a host:port value with a user-set port splits the host but preserves the user-set port", () => {

    /* The collision case: the legacy file has dvrHost: "host:8089" AND channelsDvr.port already set to a non-default value. The user's explicit choice wins
     * over the legacy embedded value - the host is still split out, but the embedded port is discarded. A warning belongs to operator-visible logs (we do not
     * assert on that here; the behavioural contract is that channelsDvr.port stays at its explicit value).
     */
    const data = makePreV3({ channelsDvr: { port: 9999 }, dvrHost: "192.168.1.5:8089" });

    applyDvrHostNamespaceMigration(data);

    const channelsDvr = requireChannelsDvr(data);

    assert.equal(channelsDvr.host, "192.168.1.5", "host portion lands at channelsDvr.host even when port collides");
    assert.equal(channelsDvr.port, 9999, "the user-set port wins; embedded port is discarded");
    assert.equal((data as { dvrHost?: string }).dvrHost, undefined, "the legacy top-level field is removed");
  });

  test("a bracketed IPv6 host:port form splits at the LAST colon so the brackets survive on the host portion", () => {

    /* The architectural reason last-colon split was chosen over first-colon split: bracketed IPv6 forms like [::1]:8089 are the canonical IPv6+port wire
     * format, and a first-colon split would fragment the address. This test pins the choice - a future refactor to split-on-first-colon would fail loudly here
     * with the host portion truncated to "[" or similar. The bracketed form is unlikely in production (auto-discovery feeds IPv4), but disk files may carry
     * hand-edited content the framework cannot vet, so the migration handles it correctly.
     */
    const data = makePreV3({ dvrHost: "[::1]:8089" });

    applyDvrHostNamespaceMigration(data);

    const channelsDvr = requireChannelsDvr(data);

    assert.equal(channelsDvr.host, "[::1]", "the bracketed IPv6 host portion is preserved verbatim");
    assert.equal(channelsDvr.port, 8089, "the trailing numeric portion migrates as the port");
    assert.equal((data as { dvrHost?: string }).dvrHost, undefined, "the legacy top-level field is removed");
  });

  test("a value with a non-numeric port suffix falls back to host-only (no fabricated port)", () => {

    /* The defensive contract: when the trailing portion after the last colon is not a parseable port number, the entire input is treated as host-only rather
     * than fabricating NaN, 0, or a parseInt-truncated value. A regression to "best-effort parseInt" would fail this test - "host:notaport" passing through
     * Number() yields NaN, and a refactor that does not validate the result would coerce NaN through the migration and corrupt downstream port comparisons.
     */
    const data = makePreV3({ dvrHost: "host:notaport" });

    applyDvrHostNamespaceMigration(data);

    const channelsDvr = requireChannelsDvr(data);

    assert.equal(channelsDvr.host, "host:notaport", "the entire input is treated as host-only when the trailing portion is non-numeric");
    assert.equal(channelsDvr.port, undefined, "no port is fabricated from the unparseable trailing portion");
    assert.equal((data as { dvrHost?: string }).dvrHost, undefined, "the legacy top-level field is removed");
  });

  test("a value with an out-of-range numeric port suffix falls back to host-only", () => {

    /* Boundary on the port-validation side of the same defensive contract: a trailing portion that parses as a number but falls outside the valid TCP port
     * range (1..65535) is rejected - the migration treats the whole input as host-only. Pins the validation gate so a future refactor that drops the range
     * check (just Number.isInteger or just typeof === "number") cannot quietly migrate bogus port values.
     */
    const data = makePreV3({ dvrHost: "host:99999" });

    applyDvrHostNamespaceMigration(data);

    const channelsDvr = requireChannelsDvr(data);

    assert.equal(channelsDvr.host, "host:99999", "out-of-range port treated as host-only");
    assert.equal(channelsDvr.port, undefined, "no port migrated from the out-of-range value");
  });

  test("a config without a dvrHost field migrates cleanly (no error, no spurious channelsDvr population)", () => {

    /* The clean upgrade case: a v2 config from a user who never had a DVR host discovered has no dvrHost field at all. The migration must early-return without
     * creating an empty channelsDvr block - filterDefaults at write time would prune an empty object, but a half-populated `{ host: "" }` would survive and
     * mislead the next session into thinking discovery had run. Pin: missing legacy field means no migration-side mutation.
     */
    const data = makePreV3({});

    assert.doesNotThrow(() => { applyDvrHostNamespaceMigration(data); }, "missing dvrHost must not throw");

    assert.equal(data.channelsDvr, undefined, "channelsDvr is NOT populated when no legacy dvrHost was present");
    assert.equal((data as { dvrHost?: string }).dvrHost, undefined, "no field is fabricated where there was none");
  });
});

describe("v2 channels provider->service rename migration", () => {

  test("renames channels.enabledProviders to channels.enabledServices verbatim", () => {

    /* The simplest rename: a v1 config carries the legacy enabledProviders array. The migration moves the value to enabledServices and removes the legacy key.
     * The array contents are not interpreted at this stage - the foxcom remap operates separately on enabledServices after the rename has run.
     */
    const data = { channels: { enabledProviders: [ "hulu", "yttv" ] } } as unknown as UserConfig;

    applyChannelsProviderRenameMigration(data);

    const channels = requireChannels(data);

    assert.deepEqual(channels["enabledServices"], [ "hulu", "yttv" ], "values move to enabledServices");
    assert.equal(channels["enabledProviders"], undefined, "the legacy key is deleted");
  });

  test("renames channels.precacheProviders to channels.precacheServices verbatim", () => {

    /* Companion rename to enabledProviders. Same shape, same outcome. Pinning both renames separately catches a regression where one is preserved but the other
     * is silently dropped during a refactor.
     */
    const data = { channels: { precacheProviders: ["sling"] } } as unknown as UserConfig;

    applyChannelsProviderRenameMigration(data);

    const channels = requireChannels(data);

    assert.deepEqual(channels["precacheServices"], ["sling"], "values move to precacheServices");
    assert.equal(channels["precacheProviders"], undefined, "the legacy key is deleted");
  });

  test("remaps the legacy foxcom service tag to foxone inside enabledServices", () => {

    /* Companion to the channels.json v3 migration that renamed foxcom in channel keys. The config's enabledServices array is the service-tag filter; any
     * "foxcom" entry must be remapped to "foxone" so the filter continues to match the new tag. Other entries are unchanged.
     */
    const data = { channels: { enabledServices: [ "hulu", "foxcom", "yttv" ] } } as unknown as UserConfig;

    applyChannelsProviderRenameMigration(data);

    const channels = requireChannels(data);

    assert.deepEqual(channels["enabledServices"], [ "hulu", "foxone", "yttv" ], "foxcom remaps to foxone in place; other tags unchanged");
  });

  test("a name collision (both legacy and current keys present) keeps the current value and deletes the legacy key", () => {

    /* The hand-edited collision: an operator added enabledServices manually before the migration ran, but the legacy enabledProviders is also still there. The
     * current name reflects deliberate operator intent, so the migration preserves it and discards the legacy. Pin: current wins, legacy deleted regardless.
     */
    const data = { channels: { enabledProviders: ["old"], enabledServices: ["new"] } } as unknown as UserConfig;

    applyChannelsProviderRenameMigration(data);

    const channels = requireChannels(data);

    assert.deepEqual(channels["enabledServices"], ["new"], "current name wins on collision");
    assert.equal(channels["enabledProviders"], undefined, "legacy key is deleted regardless of which side won");
  });

  test("an already-migrated config is a no-op (idempotent on the field-rename side)", () => {

    /* The schema-migration framework runs migrations in order from the file's stored version up to current. If something replays this migration on already-v2
     * data (no legacy keys, only current names), the migration must not corrupt or duplicate state. Pin: input shape == output shape when no legacy keys are
     * present and the foxcom remap has nothing to do.
     */
    const data = { channels: { enabledServices: [ "hulu", "foxone" ], precacheServices: ["yttv"] } } as unknown as UserConfig;

    applyChannelsProviderRenameMigration(data);

    const channels = requireChannels(data);

    assert.deepEqual(channels["enabledServices"], [ "hulu", "foxone" ], "current values unchanged");
    assert.deepEqual(channels["precacheServices"], ["yttv"], "current values unchanged");
  });

  test("a config without a channels block returns cleanly without throwing or fabricating one", () => {

    /* The defensive early-return: a v1 config with no channels block at all (default-only, never customized channel state) must not crash the migration. The
     * migration must NOT fabricate an empty channels block either - filterDefaults at write time would prune it, but a half-populated channels block would
     * survive serialization and mislead downstream readers about whether the user has customized anything.
     */
    const data: UserConfig = {};

    assert.doesNotThrow(() => { applyChannelsProviderRenameMigration(data); }, "missing channels block must not throw");

    assert.equal(data.channels, undefined, "no channels block is fabricated");
  });
});
