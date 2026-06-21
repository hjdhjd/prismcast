/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cross-store-isolation.test.ts: Integration-level invariant - the production file stores (channels, config, profiles) operate against the same data directory
 * without trampling each other. Each store owns its own file, its own .bak rotation, and its own snapshot lineage. A regression in one store's persistence
 * path - a stale path resolver, a misrouted .bak, a write that targets the wrong file - would silently corrupt unrelated state.
 *
 * Why integration coverage adds value here: persistence.test.ts (unit tier) verifies the file-store framework primitives in isolation against synthetic stores.
 * It does not verify that the real production stores - each with their own beforeWrite/validate/migrations - coexist correctly. A historical settings-save bug
 * reached into channels-adjacent state - the disabled-channel list, the service filter, and the HDHomeRun device ID - and lost it. That was structurally a
 * cross-store concern: a config save reached into channels-adjacent state and lost it. Tests at this tier guard against that class of regression.
 *
 * The mutations below use the public mutator surface of each module (mutateChannels, mutateConfig, mutateProfiles) rather than reaching into the private file
 * stores - this exercises every layer above the framework (validators, beforeWrite transforms, post-write cache hydration, side effects) exactly as production
 * hits them. The first describe block excludes health from its byte-comparison assertions because health persists via a debounced timer, which makes
 * immediate byte snapshots timing-sensitive. Health's cross-store isolation is instead covered by this file's second describe block
 * ("health.json cross-store isolation"), which waits on the documented flush window before snapshotting.
 *
 * The on-disk shape of channels.json is flattened by prepareChannelsForWrite: schemaVersion, migrationsApplied, and channel entries are top-level keys
 * (channels are NOT nested under a "channels" property). Assertions therefore read raw bytes or operate on the flattened top-level keys rather than assuming
 * the in-memory ChannelsFileData shape.
 */
import { createIntegrationContext, initializePersistence, pathInDataDir, waitForHealthFlush } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { markChannelSuccess, markDomainAuth } from "../../../src/config/health.ts";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { mutateConfig } from "../../../src/config/userConfig.ts";
import { mutateProfiles } from "../../../src/config/userProfiles.ts";
import { readFile } from "node:fs/promises";

// Files this suite cares about. captureAllFiles reads each entry's main file and its .bak counterpart.
const PERSISTED_FILES = [ "channels.json", "config.json", "profiles.json" ] as const;

/**
 * Reads every persisted file (and its .bak when present) into a map keyed by filename. Missing files are recorded as null so absent-vs-empty distinctions
 * surface in assertions. Used as a snapshot pair: capture before, capture after a targeted mutation, diff to confirm no other file changed.
 */
async function captureAllFiles(dataDir: string): Promise<Record<string, string | null>> {

  const ctx = { dataDir, registerCleanup: (): void => undefined };

  // Read every (main, .bak) pair in parallel - the reads are independent and ordering carries no meaning. The flatMap unrolls the per-file pair into a flat
  // list of filenames we can fan out via Promise.all.
  const targets = PERSISTED_FILES.flatMap((name) => [ name, name + ".bak" ]);

  const results = await Promise.all(targets.map(async (filename) => {

    try {

      return [ filename, await readFile(pathInDataDir(ctx, filename), "utf8") ] as const;
    } catch {

      return [ filename, null ] as const;
    }
  }));

  return Object.fromEntries(results);
}

describe("persistence cross-store isolation", () => {

  test("each store persists to its own file at first write; no cross-contamination of file content", async () => {

    /* The three stores write to three distinct filenames. After exercising each via its public mutator, we verify by content sniff that no file leaked into
     * another's bytes. Assertions are content-based (substring match on a uniquely-tagged value injected by each mutation): if a store's write targeted the
     * wrong file, the wrong file would carry the tag.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const channelTag = "tag-channels-DGS3JX";
    const configTag = "tag-config-7QM5LR";
    const profileTag = "tag-profile-K9TZNB";

    await mutateChannels((data) => {

      data.channels[channelTag] = { name: channelTag, url: "https://example.test/" + channelTag };
    });

    await mutateConfig((data) => {

      data.channelsDvr = { host: configTag };
    });

    await mutateProfiles((data) => {

      data.profiles[profileTag] = { description: profileTag };
    });

    const channelsRaw = await readFile(pathInDataDir(ctx, "channels.json"), "utf8");
    const configRaw = await readFile(pathInDataDir(ctx, "config.json"), "utf8");
    const profilesRaw = await readFile(pathInDataDir(ctx, "profiles.json"), "utf8");

    // Each tag must appear in exactly one file - the file the store owns. A regression that misroutes writes would surface as a tag bleeding into the wrong
    // file or vanishing from its expected one.
    assert.match(channelsRaw, new RegExp(channelTag), "channels.json carries the channel tag");
    assert.doesNotMatch(configRaw, new RegExp(channelTag), "config.json must not carry channel data");
    assert.doesNotMatch(profilesRaw, new RegExp(channelTag), "profiles.json must not carry channel data");

    assert.match(configRaw, new RegExp(configTag), "config.json carries the config tag");
    assert.doesNotMatch(channelsRaw, new RegExp(configTag), "channels.json must not carry config data");
    assert.doesNotMatch(profilesRaw, new RegExp(configTag), "profiles.json must not carry config data");

    assert.match(profilesRaw, new RegExp(profileTag), "profiles.json carries the profile tag");
    assert.doesNotMatch(channelsRaw, new RegExp(profileTag), "channels.json must not carry profile data");
    assert.doesNotMatch(configRaw, new RegExp(profileTag), "config.json must not carry profile data");
  });

  test("mutating one store leaves the other stores' files byte-for-byte unchanged", async () => {

    /* The strongest possible isolation guarantee: after a targeted write to one store, the on-disk bytes of every other store's file (main and .bak) are
     * indistinguishable from a snapshot taken immediately before. Any drift - even whitespace, even a no-op rewrite - constitutes a leak.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Prime each store with at least one write so all three files exist on disk.
    await mutateConfig((data) => { data.channelsDvr = { host: "127.0.0.1" }; });
    await mutateChannels((data) => { data.channels["seed-channel"] = { name: "Seed", url: "https://example.test/seed" }; });
    await mutateProfiles((data) => { data.profiles["seed-profile"] = { description: "Seed" }; });

    // Take the snapshot. Any drift after the targeted write should not affect any pair other than the one being mutated.
    const before = await captureAllFiles(ctx.dataDir);

    // Targeted mutation: add another channel. Only channels.json (and its .bak after the rotate) should change.
    await mutateChannels((data) => { data.channels["new-channel"] = { name: "New", url: "https://example.test/new" }; });

    const after = await captureAllFiles(ctx.dataDir);

    assert.notEqual(after["channels.json"], before["channels.json"], "channels.json should change after mutateChannels");

    // Every other store's file - main AND .bak - MUST be byte-for-byte identical.
    for(const name of [ "config.json", "profiles.json" ] as const) {

      assert.equal(after[name], before[name], name + " must not change when mutateChannels runs");
      assert.equal(after[name + ".bak"], before[name + ".bak"], name + ".bak must not change when mutateChannels runs");
    }
  });

  test("concurrent mutations across different stores complete cleanly with each store's invariants intact", async () => {

    /* Each store's own queue serializes its own writes, but writes to DIFFERENT stores have no shared queue - they run in parallel. The framework's atomic
     * write (temp + rename) plus per-store .bak isolation must be sufficient to handle this without interleaving artifacts.
     *
     * We launch three mutations in parallel and assert each persisted file reflects exactly the change that store made, with no values bleeding across stores
     * or being lost.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const tag = "concurrent-" + String(Date.now());

    await Promise.all([

      mutateChannels((data) => { data.channels[tag + "-ch"] = { name: tag + "-ch", url: "https://example.test/ch" }; }),
      mutateConfig((data) => { data.channelsDvr = { host: tag + "-host" }; }),
      mutateProfiles((data) => { data.profiles[tag + "-pf"] = { description: tag + "-pf" }; })
    ]);

    const channelsRaw = await readFile(pathInDataDir(ctx, "channels.json"), "utf8");
    const configRaw = await readFile(pathInDataDir(ctx, "config.json"), "utf8");
    const profilesRaw = await readFile(pathInDataDir(ctx, "profiles.json"), "utf8");

    // Each tag landed in its own file.
    assert.match(channelsRaw, new RegExp(tag + "-ch"), "channels file should contain the channel tag");
    assert.match(configRaw, new RegExp(tag + "-host"), "config file should contain the config tag");
    assert.match(profilesRaw, new RegExp(tag + "-pf"), "profiles file should contain the profile tag");

    // No tag bled across files. Concurrent writes did not interleave content.
    assert.doesNotMatch(channelsRaw, new RegExp(tag + "-host"), "channels file must not carry the config tag");
    assert.doesNotMatch(channelsRaw, new RegExp(tag + "-pf"), "channels file must not carry the profile tag");
    assert.doesNotMatch(configRaw, new RegExp(tag + "-ch"), "config file must not carry the channel tag");
    assert.doesNotMatch(configRaw, new RegExp(tag + "-pf"), "config file must not carry the profile tag");
    assert.doesNotMatch(profilesRaw, new RegExp(tag + "-ch"), "profiles file must not carry the channel tag");
    assert.doesNotMatch(profilesRaw, new RegExp(tag + "-host"), "profiles file must not carry the config tag");
  });

  test("each store maintains its own .bak rotation independent of the others", async () => {

    /* Per-store .bak: when channels.json is overwritten, only channels.json.bak rotates. config.json.bak must not rotate. The framework writes .bak via
     * copyFile against the same path() resolver each store carries, so the path-resolution layer already enforces this - but it is the kind of invariant
     * that breaks subtly in a refactor (e.g., a future change that uses getDataDir() directly instead of options.path()).
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Two writes to channels (so .bak exists), one write to config.
    await mutateChannels((data) => { data.channels["v1"] = { name: "V1", url: "https://example.test/v1" }; });
    await mutateChannels((data) => { data.channels["v2"] = { name: "V2", url: "https://example.test/v2" }; });
    await mutateConfig((data) => { data.channelsDvr = { host: "first" }; });

    // channels.json.bak should hold the v1-only snapshot from the prior write.
    const channelsBak = await readFile(pathInDataDir(ctx, "channels.json.bak"), "utf8");

    assert.match(channelsBak, /"v1"/, "channels.json.bak should contain the prior version");
    assert.doesNotMatch(channelsBak, /"v2"/, "channels.json.bak should NOT contain the latest version (that is in main)");

    // config.json.bak should not exist yet (only one write so far, .bak is created on the second write).
    await assert.rejects(
      () => readFile(pathInDataDir(ctx, "config.json.bak"), "utf8"),
      /ENOENT/,
      "config.json.bak should not exist after a single config write"
    );

    // A second config write rotates config.json.bak. channels.json.bak must NOT rotate as a side effect.
    const channelsBakBefore = channelsBak;

    await mutateConfig((data) => { data.channelsDvr = { host: "second" }; });

    const channelsBakAfter = await readFile(pathInDataDir(ctx, "channels.json.bak"), "utf8");
    const configBak = await readFile(pathInDataDir(ctx, "config.json.bak"), "utf8");

    assert.equal(channelsBakAfter, channelsBakBefore, "channels.json.bak should not rotate when config writes");
    assert.match(configBak, /"first"/, "config.json.bak should now hold the prior config value");
    assert.doesNotMatch(configBak, /"second"/, "config.json.bak should not contain the latest value");
  });
});

describe("health.json cross-store isolation", () => {

  /* health.json sits in the same data directory as channels / config / profiles but is owned by a separate file store with its own debounced write path
   * (FLUSH_DELAY = 2000ms in src/config/health.ts). The original cross-store-isolation suite excluded it because its debounce makes byte-comparison timing
   * tricky; this block adds the missing coverage by waiting on the documented flush window.
   *
   * Two invariants are pinned:
   *   1. A mutation to channels / config / profiles must leave health.json byte-identical. A regression that mis-routes a write into health.json (e.g., a path
   *      resolver bug) would silently break the health indicators with no test failure today.
   *   2. A mutation to health.json must leave the other three stores byte-identical. The same routing failure mode in the other direction.
   *
   * Together they pin health.json's place in the four-file store ecosystem: each file is owned exclusively by its module, and no module's writes leak across.
   */

  test("mutations to channels, config, and profiles leave health.json byte-identical", async () => {

    /* Seed health state with a channel success and a domain auth, wait past the flush window, then snapshot health.json. Drive sequential mutations against
     * each of the other three stores, wait again, and re-snapshot. The bytes must match exactly - no accidental rewrite, no debounce-related drift.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    markChannelSuccess("abc", "abc.com");
    markDomainAuth("hulu.com");

    await waitForHealthFlush();

    const before = await readFile(pathInDataDir(ctx, "health.json"), "utf8");

    await mutateChannels((data) => { data.channels["seed-channel"] = { name: "Seed", url: "https://example.test/seed" }; });
    await mutateConfig((config) => { config.channelsDvr = { host: "192.168.1.50" }; });
    await mutateProfiles((profiles) => { profiles.profiles["seed-profile"] = { description: "Seed" }; });

    // Wait once more in case any of the mutations spuriously triggered a health write that needs to drain. If health.json is byte-identical after this point,
    // we have positive evidence that the unrelated stores' writes did not leak into health.json across the flush window.
    await waitForHealthFlush();

    const after = await readFile(pathInDataDir(ctx, "health.json"), "utf8");

    assert.equal(after, before, "health.json must be byte-identical after mutations to other stores");
  });

  test("a health mutation leaves channels.json, config.json, and profiles.json byte-identical", async () => {

    /* The contrapositive: writing to health.json (via the public mark functions, going through its debounce + file store) must not perturb the other three
     * files. We seed each of the three with a known value first so they all have on-disk content to compare, snapshot them, fire a health mark, wait for the
     * flush, then re-snapshot.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateChannels((data) => { data.channels["seed-channel"] = { name: "Seed", url: "https://example.test/seed" }; });
    await mutateConfig((config) => { config.channelsDvr = { host: "127.0.0.1" }; });
    await mutateProfiles((profiles) => { profiles.profiles["seed-profile"] = { description: "Seed" }; });

    const channelsBefore = await readFile(pathInDataDir(ctx, "channels.json"), "utf8");
    const configBefore = await readFile(pathInDataDir(ctx, "config.json"), "utf8");
    const profilesBefore = await readFile(pathInDataDir(ctx, "profiles.json"), "utf8");

    markChannelSuccess("abc", "abc.com");

    await waitForHealthFlush();

    const channelsAfter = await readFile(pathInDataDir(ctx, "channels.json"), "utf8");
    const configAfter = await readFile(pathInDataDir(ctx, "config.json"), "utf8");
    const profilesAfter = await readFile(pathInDataDir(ctx, "profiles.json"), "utf8");

    assert.equal(channelsAfter, channelsBefore, "channels.json must not change when health is written");
    assert.equal(configAfter, configBefore, "config.json must not change when health is written");
    assert.equal(profilesAfter, profilesBefore, "profiles.json must not change when health is written");
  });
});
