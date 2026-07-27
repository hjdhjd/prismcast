/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * config-import-export.test.ts: HTTP-level integration coverage for the two whole-config transfer endpoints in src/routes/config/settings.ts - POST /config/import
 * and GET /config/export. These are the layer-replacement counterparts to the settings-form save path pinned by settings-preservation.test.ts: where the form
 * save merges a partial CONFIG_METADATA body into the existing config, import replaces the entire user-settings layer while leaving the system-state layer intact.
 *
 * The key distinction is the two-layer model. CONFIG_METADATA is the single source of truth for which fields are user settings (port, timeouts, quality
 * preset, HLS segment duration, ...) versus system state (channelsDvr.host, hdhr.deviceId, disabledPredefined, enabledServices, ...). Import clears every
 * CONFIG_METADATA-tracked path before merging the imported body, so a metadata field ABSENT from the import reverts to its default (absent on disk equals default
 * because the file-store framework's filterDefaults strips default-equal values), while every system-state field survives untouched because it is never in
 * CONFIG_METADATA and therefore never cleared. Export is the inverse read side: it serializes readConfig() as sorted-key JSON with an attachment disposition so a
 * browser download round-trips byte-for-byte back through import.
 *
 * settings-preservation.test.ts pins the merge-preserves-non-form-fields rule for the form save; this suite pins the clear-then-merge rule for import
 * and the sorted-key attachment rule for export, which the form-save suite does not exercise.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { getNestedValue, mutateConfig } from "../../../src/config/userConfig.ts";
import assert from "node:assert/strict";
import { getDefaults } from "../../../src/config/index.ts";
import { stringifySorted } from "../../../src/utils/index.ts";

describe("POST /config/import - replaces the user-settings layer, preserves system state", () => {

  test("an omitted metadata field reverts to default while system-state fields survive untouched", async () => {

    /* Seed a dirty config that spans both layers: hls.segmentDuration is a CONFIG_METADATA field set to a non-default value, and channels.disabledPredefined plus
     * hdhr.deviceId are system-state fields (not in CONFIG_METADATA). We then POST an import that sets a DIFFERENT metadata field (channelsDvr.port) and omits
     * hls.segmentDuration entirely. The handler must clear every CONFIG_METADATA path before merging, so the omitted hls.segmentDuration reverts to its default
     * (absent on disk, because filterDefaults strips default-equal values), the imported channelsDvr.port lands, and both system-state fields survive because
     * they are never cleared.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // The seeded segment duration must differ from the default, otherwise "reverts to default" would be indistinguishable from "was left alone". We assert the
    // gap up front so the test fails loudly if the default ever changes to collide with the seed.
    const defaultSegmentDuration = getNestedValue(getDefaults(), "hls.segmentDuration");
    const seededSegmentDuration = 5;

    assert.notEqual(seededSegmentDuration, defaultSegmentDuration, "the seeded metadata value must differ from its default for the revert to be observable");

    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.disabledPredefined = [ "abc-hulu", "nbc-yttv" ];
      config.hdhr ??= {};
      config.hdhr.deviceId = "ABCD1234";
      config.hls ??= {};
      config.hls.segmentDuration = seededSegmentDuration;
    });

    const response = await fetch(urlFor("/config/import"), {

      body: JSON.stringify({ channelsDvr: { port: 9999 } }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "a valid import responds 200; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "config.json");

    // The omitted metadata field was cleared and not re-supplied, so it is absent on disk - which, under filterDefaults, is exactly the reverted-to-default state.
    // A regression that stopped clearing user settings before merge would leave the seeded 5 on disk and fail here.
    assert.equal(getNestedValue(persisted, "hls.segmentDuration"), undefined, "an omitted metadata field must revert to its default (absent on disk)");

    // The imported metadata field landed on the freshly-cleared layer.
    assert.equal(getNestedValue(persisted, "channelsDvr.port"), 9999, "a metadata field present in the import must be applied");

    // Both system-state fields are outside CONFIG_METADATA, so the clear-then-merge never touches them.
    assert.deepEqual(getNestedValue(persisted, "channels.disabledPredefined"), [ "abc-hulu", "nbc-yttv" ], "disabledPredefined (system state) must survive import");
    assert.equal(getNestedValue(persisted, "hdhr.deviceId"), "ABCD1234", "hdhr.deviceId (system state) must survive import");
  });

  test("a non-object import body is rejected with a 400 validation error", async () => {

    /* Boundary: the handler rejects anything that is not a plain object (null, arrays, primitives) before touching the config. An array body must produce a 400
     * with the standard { error, success: false } envelope and must not mutate config.json.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.disabledPredefined = ["abc-hulu"];
    });

    const before = await readPersistedJson(ctx, "config.json");

    const response = await fetch(urlFor("/config/import"), {

      body: JSON.stringify([ "not", "an", "object" ]),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "an array import body is a validation error");

    const body = await response.json() as { error: string; success: boolean };

    assert.equal(body.success, false, "the error envelope carries success: false");
    assert.equal(typeof body.error, "string", "the error envelope carries an error message");

    const after = await readPersistedJson(ctx, "config.json");

    assert.deepEqual(after, before, "a rejected import must not modify config.json");
  });
});

describe("GET /config/export - sorted-key attachment round-trips the current config", () => {

  test("serializes readConfig() as sorted-key JSON with an attachment disposition header", async () => {

    /* Seed a distinctive config across both layers, then export it. The response must carry an attachment Content-Disposition (so browsers download rather than
     * render), be sorted-key JSON (stringifySorted is the SSOT for persisted and exported serialization), and parse back to exactly the current on-disk config.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.disabledPredefined = [ "abc-hulu", "nbc-yttv" ];
      config.channelsDvr = { host: "192.168.1.50", port: 8089 };
      config.hls ??= {};
      config.hls.segmentDuration = 5;
    });

    const response = await fetch(urlFor("/config/export"));

    assert.equal(response.status, 200, "the export responds 200");
    assert.equal(response.headers.get("content-disposition"), "attachment; filename=\"prismcast-config.json\"",
      "the export sets the attachment disposition with the documented filename");
    assert.ok((response.headers.get("content-type") ?? "").includes("application/json"), "the export is served as JSON");

    const rawBody = await response.text();

    // The body parses to a JSON object.
    const parsed = JSON.parse(rawBody) as Record<string, unknown>;

    // Sorted-key rule: re-serializing the parsed value through the SSOT serializer must reproduce the exact bytes the handler sent (including the trailing
    // newline it appends). A regression that swapped stringifySorted for a plain JSON.stringify would reorder keys and break this equality.
    assert.equal(rawBody, stringifySorted(parsed) + "\n", "the export body must be sorted-key JSON with a trailing newline");

    // Round-trip: the exported config must equal the current on-disk config that readConfig() reads. Both go through the same file-store read, so a deep-equality
    // check pins that export ships the live config rather than a stale or synthesized copy.
    const persisted = await readPersistedJson(ctx, "config.json");

    assert.deepEqual(parsed, persisted, "the exported config must round-trip to the current on-disk config");
    assert.deepEqual(getNestedValue(parsed, "channelsDvr.host"), "192.168.1.50", "the seeded system-state field appears in the export");
    assert.deepEqual(getNestedValue(parsed, "hls.segmentDuration"), 5, "the seeded metadata field appears in the export");
  });
});
