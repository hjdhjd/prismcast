/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * upgrade-preservation.test.ts: Integration coverage for the bug class where one save path silently drops state owned by another. A save to one store never
 * drops fields another writer owns, and a config save never touches channels.json. The describe blocks carry the commit ids of the canonical instances as
 * regression provenance:
 *
 *   - 4afa8a0 (v1.3.2): saving the settings form wiped the disabled channel list, the service filter, and the HDHomeRun device ID, because the form's POST
 *     handler overwrote config.json wholesale with form-only values rather than merging them into the existing shape.
 *
 *   - 1c549e8 (v1.9.1): user-set channel numbers and station IDs on local-affiliate variants were lost across upgrades, because an upgrade migration normalized
 *     variant entries against the predefined base too aggressively, treating user-authored identity fields on variants as redundant overrides.
 *
 * The 4afa8a0 block asserts that a store read-modify-write touching only a form-managed field keeps the system-state fields beside it. Every row writes through
 * the store-level mutateConfig; test/e2e/routes/settings-preservation.test.ts is the suite that drives the POST /config handler and mergeConfigValues. The
 * 1c549e8 block guards cross-store isolation: it seeds a user customization in the channels store, drives a config-store save, and asserts the channels file is
 * byte-for-byte unchanged - a config save must never reach into and mutate the channels store. It does not guard the migration-normalization failure itself,
 * which src/config/userChannels.migration.test.ts covers next to the migration.
 */
import { createIntegrationContext, initializePersistence, pathInDataDir, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { SEEDED_DEVICE_ID } from "../../../src/config/index.helpers.ts";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { mutateConfig } from "../../../src/config/userConfig.ts";
import { readFile } from "node:fs/promises";

describe("settings-save preservation (catches the 4afa8a0 family)", () => {

  test("a partial config update does not wipe disabledPredefined", async () => {

    /* The 4afa8a0 bug shape: the user has disabled some predefined channels (CONFIG.channels.disabledPredefined is non-empty); they then save the settings
     * form, which submits only CONFIG_METADATA fields; the saved config.json has empty disabledPredefined. This row asserts the store-level half of that
     * guarantee: a mutateConfig that touches only a form-shape field keeps disabledPredefined. The POST /config path through mergeConfigValues is driven by
     * settings-preservation.test.ts.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed: write the disabled predefined list straight to the file through the store-level mutateConfig.
    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.disabledPredefined = [ "abc-hulu", "nbc-yttv" ];
    });

    // Drive a partial form-shape update touching ONLY a form-managed field (server.port). This is structurally equivalent to what the settings POST handler
    // does when a user submits the settings form without changing channel state.
    await mutateConfig((config) => {

      config.server ??= {};
      config.server.port = 9999;
    });

    const persisted = await readPersistedJson(ctx, "config.json") as { channels: { disabledPredefined: string[] }; server: { port: number } };

    assert.equal(persisted.server.port, 9999, "form field landed (non-default port survives filterDefaults)");
    assert.deepEqual(persisted.channels.disabledPredefined, [ "abc-hulu", "nbc-yttv" ], "disabledPredefined must survive a settings save");
  });

  test("a partial config update does not wipe channels.enabledServices (the service filter)", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.enabledServices = [ "hulu", "sling" ];
    });

    await mutateConfig((config) => {

      config.server ??= {};
      config.server.port = 9999;
    });

    const persisted = await readPersistedJson(ctx, "config.json") as { channels: { enabledServices: string[] } };

    assert.deepEqual(persisted.channels.enabledServices, [ "hulu", "sling" ], "enabledServices must survive a settings save");
  });

  test("a partial config update does not wipe hdhr.deviceId", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateConfig((config) => {

      config.hdhr ??= {};
      config.hdhr.deviceId = SEEDED_DEVICE_ID;
    });

    await mutateConfig((config) => {

      config.server ??= {};
      config.server.port = 9999;
    });

    const persisted = await readPersistedJson(ctx, "config.json") as { hdhr: { deviceId: string } };

    assert.equal(persisted.hdhr.deviceId, SEEDED_DEVICE_ID, "hdhr.deviceId must survive a settings save");
  });

  test("a partial config update does not wipe the auto-discovered channelsDvr.host field", async () => {

    /* channelsDvr.host is auto-discovered at runtime by showInfo.ts. The settings form does not manage it - it is not in CONFIG_METADATA, so a
     * wholesale-overwrite save would drop it without the explicit-preservation block in filterDefaults. This test asserts that the merge-save path keeps it.
     * channelsDvr.host holds a bare host; a port never appears in it.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateConfig((config) => {

      config.channelsDvr = { host: "192.168.1.50" };
    });

    await mutateConfig((config) => {

      config.server ??= {};
      config.server.port = 9999;
    });

    const persisted = await readPersistedJson(ctx, "config.json") as { channelsDvr: { host: string } };

    assert.equal(persisted.channelsDvr.host, "192.168.1.50", "channelsDvr.host must survive a settings save");
  });
});

describe("channel customization preservation (catches the 1c549e8 family)", () => {

  test("a user-set channelNumber on a predefined-canonical override survives a config save (cross-store isolation)", async () => {

    /* The bug class: a save targeting one store reaches into another and damages it. Concretely we set a custom channelNumber on a real predefined canonical
     * (abc), which is the canonical-override pattern the channels tab produces when the user types a number in the inline-editable cell. Then we run a
     * config-side save through mutateConfig and assert the channel customization is byte-identical before and after on disk.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Set a custom channel number on the predefined abc canonical. Going through mutateChannels exercises the real beforeWrite/normalize/validate stack.
    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7 };
    });

    const channelsBefore = await readFile(pathInDataDir(ctx, "channels.json"), "utf8");

    // Now drive a settings-style save. Even though it touches an unrelated store, the channels file must not change.
    await mutateConfig((config) => {

      config.server ??= {};
      config.server.port = 9999;
    });

    const channelsAfter = await readFile(pathInDataDir(ctx, "channels.json"), "utf8");

    assert.equal(channelsAfter, channelsBefore, "channels.json must be byte-for-byte identical before and after the config save");
  });

  test("a user-set stationId on a variant binding survives a config save", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    /* The variant case: stationId is an identity field, so the override lands on the canonical entry. A save that wiped channel state on the wrong path
     * would corrupt this. We seed and then drive an unrelated config save.
     */
    await mutateChannels((data) => {

      data.channels["abc"] = { stationId: "10068" };
    });

    const channelsBefore = await readFile(pathInDataDir(ctx, "channels.json"), "utf8");

    await mutateConfig((config) => {

      config.channelsDvr = { host: "10.0.0.1" };
    });

    const channelsAfter = await readFile(pathInDataDir(ctx, "channels.json"), "utf8");

    assert.equal(channelsAfter, channelsBefore, "channels.json must be byte-for-byte identical before and after the config save");
  });
});

