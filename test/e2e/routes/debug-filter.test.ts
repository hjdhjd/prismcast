/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * debug-filter.test.ts: HTTP-level integration coverage for how POST /debug persists the filter it applies. The page applies the filter to the runtime at once
 * and then saves it through the validated save every writer of the settings surface takes, so these rows drive a toggle through the real file store and the
 * reconcile:
 *
 *   1. A toggle against a file a hand edit left failing validation applies the filter, logs one warning that it was not persisted, writes nothing, and still
 *      answers its redirect.
 *   2. A toggle whose save picks up a hand-edited restart-class value from the file holds that value for a restart and logs the restart outcome, as a settings
 *      save of the value would.
 *
 * Each row boots its own integration context and calls initializeConfiguration() after createIntegrationContext and before initializePersistence, so CONFIG
 * and the loaded snapshot are read from that row's own data directory rather than inherited from an earlier row in the same process. Each row clears the
 * runtime filter before it boots and once it ends, so no row inherits another's filter.
 */
import type { BootedApp, IntegrationContext } from "../../helpers/integration.helpers.ts";
import { CONFIG, getConfigurationGap, initializeConfiguration } from "../../../src/config/index.ts";
import { LOG, getCurrentPattern, initDebugFilter } from "../../../src/utils/index.ts";
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson, writePersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

/**
 * Boots a row: a fresh data directory, the configuration read from it, the stores hydrated, and the routes listening.
 * @param ctx - The row's integration context.
 * @returns The booted app.
 */
async function boot(ctx: IntegrationContext): Promise<BootedApp> {

  await initializeConfiguration();
  await initializePersistence(ctx);

  return bootApp(ctx);
}

/**
 * Posts a pattern to the debug page the way its hidden form does, without following the redirect.
 * @param app - The booted app.
 * @param pattern - The filter pattern to apply.
 * @returns The response, its body drained.
 */
async function postToggle(app: BootedApp, pattern: string): Promise<Response> {

  const response = await fetch(app.urlFor("/debug"), { body: new URLSearchParams({ pattern }), method: "POST", redirect: "manual" });

  await response.text();

  return response;
}

describe("POST /debug - the filter applies at once and persists through the validated save", () => {

  test("a toggle against a file that fails validation applies the filter, logs that it was not persisted, writes nothing, and still redirects", async (t) => {

    initDebugFilter("");
    t.after(() => {

      initDebugFilter("");
    });

    await using ctx = await createIntegrationContext();
    const app = await boot(ctx);

    // A hand edit after boot leaves a port the hard-error check refuses. The boot itself refuses to start on such a file, so only an edit made while the process
    // runs can reach the toggle.
    await writePersistedJson(ctx, "config.json", { server: { port: 0 } });

    const warn = t.mock.method(LOG, "warn", () => undefined);
    const response = await postToggle(app, "tuning:hulu, recovery");
    const notPersisted = warn.mock.calls.filter((call) => call.arguments[0] === "The debug filter is applied but was not persisted: %s.");

    assert.equal(response.status, 303, "the redirect answers");
    assert.equal(response.headers.get("location"), "/debug");
    assert.equal(getCurrentPattern(), "tuning:hulu,recovery", "the filter is applied at once");
    assert.equal(notPersisted.length, 1, "one warning says the filter was not persisted");
    assert.equal(notPersisted[0]?.arguments[1], "PORT must be at least 1, but it is 0", "the warning carries the reason the save was refused");
    assert.deepEqual(await readPersistedJson(ctx, "config.json"), { server: { port: 0 } }, "the refused save wrote nothing");
    assert.equal(CONFIG.logging.debugFilter, "", "the refused save never reached CONFIG");
  });

  test("a toggle whose save picks up a hand-edited restart-class value holds it for a restart and logs the restart outcome", async (t) => {

    initDebugFilter("");
    t.after(() => {

      initDebugFilter("");
    });

    await using ctx = await createIntegrationContext();
    const app = await boot(ctx);

    await writePersistedJson(ctx, "config.json", { server: { port: 6000 } });

    const info = t.mock.method(LOG, "info", () => undefined);
    const response = await postToggle(app, "tuning:hulu");
    const restartOutcome = info.mock.calls.filter((call) => call.arguments[0] === "Configuration saved. Please restart PrismCast for changes to take effect.");

    assert.equal(response.status, 303, "the redirect answers");
    assert.equal(restartOutcome.length, 1, "the save earned the restart the hand edit calls for, and the page logged it");
    assert.equal(CONFIG.logging.debugFilter, "tuning:hulu", "the filter is committed");
    assert.equal(CONFIG.server.port, 5589, "the restart-class value stays out of the running configuration");
    assert.deepEqual(getConfigurationGap().held.map((change) => change.path), ["server.port"], "the hand-edited value is pending a restart");
  });
});
