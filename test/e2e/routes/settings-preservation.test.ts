/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * settings-preservation.test.ts: HTTP-level integration coverage for the settings-form save path. The handler at POST /config receives a form-shape body
 * (CONFIG_METADATA fields), merges it into the existing config via mergeConfigValues, and persists. The historical bug 4afa8a0 wiped non-form fields
 * (disabledPredefined, enabledServices, hdhr.deviceId) when the save was wholesale-overwriting the existing config; the merge-based fix preserves them.
 *
 * persistence/upgrade-preservation.test.ts already covers the same rules by driving mutateConfig directly. This suite adds end-to-end HTTP coverage so a
 * regression in the route-handler path (e.g., the body-parser change, the validation step, the merge call site) surfaces here even when the underlying
 * mutateConfig rule still holds.
 *
 * The file is split into two cohesive blocks:
 *
 *   1. Hand-picked named-fingerprint tests for the highest-leverage 4afa8a0-class fields (disabledPredefined, enabledServices, hdhr.deviceId,
 *      channelsDvr.host) plus the empty-form-body no-op boundary. These remain on purpose, even though the parameterized sweep below also covers each of those
 *      fields - the named tests carry historical-incident context in their messages and serve as low-cost belt-and-suspenders against the most user-visible
 *      regression class. If the parameterized sweep ever skips or mis-seeds one of those fields, the named test still catches the underlying bug.
 *
 *   2. Suite 17 - parameterized preservation sweep driven directly off PRESERVED_FIELDS, the production registry. Adding a new preserved field is one
 *      line in src/config/userConfig.ts (the registry) plus one line in this file's seed table; the sweep then automatically asserts preservation for the new
 *      field. The drift-check test at the top of the sweep block fails loudly if the seed table and the registry get out of sync. This is the structural
 *      counter to the next 4afa8a0: a regression on a field nobody hand-picked for a test surfaces here automatically the moment it's added to the registry.
 */
import { PRESERVED_FIELDS, getNestedValue, mutateConfig, setNestedValue } from "../../../src/config/userConfig.ts";
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import type { PreservedField } from "../../../src/config/userConfig.ts";
import assert from "node:assert/strict";

describe("POST /config - settings-form save preserves non-form fields", () => {

  test("disabledPredefined survives a settings-form POST", async () => {

    /* Seed the dirty state via mutateConfig (the same path the toggle endpoint uses), then POST a form-shape body that touches only a CONFIG_METADATA field
     * (server.port). The handler's mergeConfigValues call must preserve the disabledPredefined list. We assert the on-disk file directly because the route
     * also schedules a restart when running as a service - running outside a service is the test harness's behavior, so the restart-skip branch fires.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.disabledPredefined = [ "abc-hulu", "nbc-yttv" ];
    });

    const response = await fetch(urlFor("/config"), {

      body: JSON.stringify({ server: { port: 9999 } }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "settings POST should succeed");

    const persisted = await readPersistedJson(ctx, "config.json") as { channels: { disabledPredefined: string[] } };

    assert.deepEqual(persisted.channels.disabledPredefined, [ "abc-hulu", "nbc-yttv" ], "disabledPredefined must survive the form save");
  });

  test("enabledServices (the service filter) survives a settings-form POST", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.enabledServices = [ "hulu", "sling" ];
    });

    const response = await fetch(urlFor("/config"), {

      body: JSON.stringify({ server: { port: 9999 } }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200);

    const persisted = await readPersistedJson(ctx, "config.json") as { channels: { enabledServices: string[] } };

    assert.deepEqual(persisted.channels.enabledServices, [ "hulu", "sling" ], "enabledServices must survive the form save");
  });

  test("hdhr.deviceId survives a settings-form POST", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateConfig((config) => {

      config.hdhr ??= {};
      config.hdhr.deviceId = "12345678";
    });

    const response = await fetch(urlFor("/config"), {

      body: JSON.stringify({ server: { port: 9999 } }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200);

    const persisted = await readPersistedJson(ctx, "config.json") as { hdhr: { deviceId: string } };

    assert.equal(persisted.hdhr.deviceId, "12345678", "hdhr.deviceId must survive the form save");
  });

  test("channelsDvr.host survives a settings-form POST", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateConfig((config) => {

      config.channelsDvr = { host: "192.168.1.50" };
    });

    const response = await fetch(urlFor("/config"), {

      body: JSON.stringify({ server: { port: 9999 } }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200);

    const persisted = await readPersistedJson(ctx, "config.json") as { channelsDvr: { host: string } };

    assert.equal(persisted.channelsDvr.host, "192.168.1.50", "channelsDvr.host must survive the form save");
  });

  test("an empty form body is a no-op against the existing config (no fields to merge)", async () => {

    /* Boundary: the handler iterates CONFIG_METADATA fields and skips any value that comes back undefined. An empty body produces no field reads, no merge,
     * and an unchanged on-disk config. We seed dirty state then post {} - the channels.json-adjacent state must survive untouched.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.disabledPredefined = ["abc-hulu"];
    });

    const before = await readPersistedJson(ctx, "config.json");

    const response = await fetch(urlFor("/config"), {

      body: JSON.stringify({}),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "an empty form is accepted (no validation error)");

    const after = await readPersistedJson(ctx, "config.json");

    assert.deepEqual(after, before, "empty-form save must not modify config.json");
  });
});

/* Test-side seed values, keyed by PRESERVED_FIELDS path. Kept in this file (not in the production module) so test fixture data stays out of production code, per
 * the operational rule. A dedicated drift-check test at the top of the sweep asserts this table's keys exactly match the registry's paths - any new entry added
 * to PRESERVED_FIELDS without a matching seed here fails the suite loudly before any sub-test runs, and any orphan seed without a matching registry entry fails
 * the same way.
 *
 * Each value is chosen to differ from its DEFAULTS counterpart so filterDefaults preserves it (the seed must differ from the default to survive default-filtering).
 * schemaVersion and migrationsApplied
 * are framework-managed metadata; the values used here mirror what the runtime would already write (current schema version 3 / a synthetic migration-applied
 * marker), so they round-trip without colliding with the file-store framework's migration runner.
 */
const SEED_VALUES: Record<string, unknown> = {

  "channels.channelSortDirection": "desc",
  "channels.channelSortField": "channelNumber",
  "channels.disabledPredefined": [ "abc-hulu", "nbc-yttv" ],
  "channels.enabledServices": [ "hulu", "sling" ],
  "channels.precacheServices": ["hulu"],
  "channels.visibleColumns": [ "channelNumber", "name", "service" ],
  "channelsDvr.host": "192.168.1.50",
  "hdhr.deviceId": "ABCD1234",
  "logging.debugFilter": "browser:*",
  "migrationsApplied": ["test-suite-17-marker"],
  "schemaVersion": 3,
  "streaming.captureCodecs": ["h264"]
};

describe("POST /config - parameterized preservation sweep over PRESERVED_FIELDS", () => {

  /* Suite 17 - the structural counter to "the next 4afa8a0 lands on a field nobody hand-picked for a test." The sweep iterates the production registry directly,
   * seeds a non-default value for each entry, POSTs a settings form that touches a different CONFIG_METADATA field (server.port: 9999 - the canonical Phase 1
   * pattern), and asserts the seeded value survives byte-identical on disk. The registry is the single source of truth; a new preserved field added to the
   * registry is automatically covered by this sweep without any test edit beyond adding its seed value to the table above.
   */

  test("test-side seed table and PRESERVED_FIELDS registry agree on coverage", () => {

    /* Drift check: the seed table's keys must equal the registry's paths exactly - no missing seeds (would fail with a confusing per-field error in a sub-test
     * below), no orphan seeds (would silently grow the table with stale entries). Comparing sorted arrays surfaces both failure modes in one assertion.
     */
    const registryPaths = PRESERVED_FIELDS.map((field: PreservedField) => field.path).toSorted();
    const seedPaths = Object.keys(SEED_VALUES).toSorted();

    assert.deepEqual(seedPaths, registryPaths, "SEED_VALUES keys must equal PRESERVED_FIELDS paths exactly. Update the seed table when adding to the registry.");
  });

  for(const field of PRESERVED_FIELDS) {

    test("preserves " + field.path + " across a settings-form POST", async () => {

      /* Per-field shape: seed the value, POST a form that touches a different field, assert the seeded value survives byte-identical on disk. The seed value
       * is looked up from SEED_VALUES (drift-checked above); we never duplicate the field list inside this loop body.
       */
      await using ctx = await createIntegrationContext();

      await initializePersistence(ctx);

      const { urlFor } = await bootApp(ctx);

      const seed = SEED_VALUES[field.path];

      await mutateConfig((config) => {

        setNestedValue(config as Record<string, unknown>, field.path, seed);
      });

      const response = await fetch(urlFor("/config"), {

        body: JSON.stringify({ server: { port: 9999 } }),
        headers: { "content-type": "application/json" },
        method: "POST"
      });

      assert.equal(response.status, 200, "settings POST should succeed for " + field.path + "; body: " + (await response.clone().text()).slice(0, 200));

      const persisted = await readPersistedJson(ctx, "config.json");
      const persistedValue = getNestedValue(persisted, field.path);

      assert.deepEqual(persistedValue, seed, "field " + field.path + " must survive a settings-form POST byte-identical to the seeded value");
    });
  }
});

/* Text a user types or pastes into the settings form arrives with whatever the clipboard carried. A path copied out of a terminal brings a trailing newline; a
 * value copied out of a web page can bring a zero-width space. Both are invisible in the field and both break the consumer downstream - a Chrome executable path
 * with a stray character fails to launch. The form save routes host, path, and free-string values through the shared data-collection sanitizer, so what lands on
 * disk is the visible content of what was submitted.
 *
 * These pins verify the stored bytes rather than the handler's response, because a pre-I/O assertion cannot tell a trimmed value from a padded one.
 */
describe("POST /config - text settings are sanitized before they are persisted", () => {

  test("a padded path value is stored trimmed and stripped of non-printable characters", async () => {

    /* The fixture embeds a zero-width space between two visible segments as well as surrounding padding. A bare trim would leave that character in the middle of
     * the stored path, so this assertion is what distinguishes the shared sanitizer from a trim.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config"), {

      body: JSON.stringify({ paths: { logFile: "  /var/log/prism​cast.log\n" } }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "the settings POST should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "config.json");

    assert.equal(getNestedValue(persisted, "paths.logFile"), "/var/log/prismcast.log", "the stored path carries only the visible content of what was submitted");
  });

  test("a padded host value is stored trimmed", async () => {

    // host and path share one arm of the parse switch alongside free strings, so pinning a second type proves the arm rather than a single setting.
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config"), {

      body: JSON.stringify({ server: { host: "  127.0.0.1  " } }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "the settings POST should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "config.json");

    assert.equal(getNestedValue(persisted, "server.host"), "127.0.0.1", "the stored host carries no padding");
  });
});
