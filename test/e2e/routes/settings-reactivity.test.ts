/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * settings-reactivity.test.ts: HTTP-level integration coverage for how a saved setting reaches the running process. Every save goes through the real file
 * store and the reconcile behind saveConfiguration, so these rows hold the model end to end:
 *
 *   1. A restart-class save lands on disk and stays out of CONFIG, answering its path under changes.deferred, and the gap accessor holds it until a restart;
 *      a later unrelated save defers nothing, and a save that writes the running value back empties the pending set. The response's pending view and the
 *      page's marker follow the same gap.
 *   2. A live save commits at once and answers its path under changes.applied.
 *   3. A save whose candidate fails validation answers 400 with the reason, and the file's bytes and CONFIG stay as they were.
 *   4. A leaf the process writes and the file holds - the setup flag - keeps its running value across a save that does not change it.
 *   5. An import of channel display state takes effect live and schedules no restart.
 *   6. An import of the service filter and the Channels DVR host takes effect live: the running filter and the running host follow it, and nothing is held
 *      for a restart.
 *
 * Each row boots its own integration context and calls initializeConfiguration() after createIntegrationContext and before initializePersistence, so CONFIG
 * and the loaded snapshot are read from that row's own data directory rather than inherited from an earlier row in the same process.
 */
import type { BootedApp, IntegrationContext } from "../../helpers/integration.helpers.ts";
import { CONFIG, getConfigurationGap, initializeConfiguration } from "../../../src/config/index.ts";
import type { SettingsSaveChanges, SettingsSaveData } from "../../../src/routes/config/settings.ts";
import { bootApp, createIntegrationContext, initializePersistence, pathInDataDir, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { getAllChannels, getPredefinedChannels, isPredefinedChannelDisabled, markSetupCompleted } from "../../../src/config/userChannels.ts";
import assert from "node:assert/strict";
import { getDvrHost } from "../../../src/streaming/showInfo.ts";
import { getEnabledServices } from "../../../src/config/services.ts";
import { getNestedValue } from "../../../src/config/userConfig.ts";
import { readFile } from "node:fs/promises";

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
 * Posts a JSON body to a route and returns the status and the parsed body.
 * @param app - The booted app.
 * @param route - The route to post to.
 * @param body - The JSON body.
 * @returns The status code and the response body.
 */
async function post(app: BootedApp, route: string, body: unknown): Promise<{ body: Record<string, unknown>; status: number }> {

  const response = await fetch(app.urlFor(route), { body: JSON.stringify(body), headers: { "Content-Type": "application/json" }, method: "POST" });

  return { body: await response.json() as Record<string, unknown>, status: response.status };
}

/**
 * Reads the outcome by setting a save or import answered with.
 * @param body - The response body.
 * @returns The body's changes member.
 */
function changesOf(body: Record<string, unknown>): SettingsSaveChanges {

  return body["changes"] as SettingsSaveChanges;
}

/**
 * Reads the configuration file's raw bytes, or null when the file does not exist yet.
 * @param ctx - The row's integration context.
 * @returns The file's contents, or null.
 */
async function readConfigBytes(ctx: IntegrationContext): Promise<string | null> {

  try {

    return await readFile(pathInDataDir(ctx, "config.json"), "utf8");
  } catch {

    return null;
  }
}

describe("POST /config - each class reaches the running process as its rule states", () => {

  test("a restart-class save leaves CONFIG unchanged with the value on disk, answers its path as deferred, and stays pending", async () => {

    await using ctx = await createIntegrationContext();
    const app = await boot(ctx);

    const { body, status } = await post(app, "/config", { server: { port: 6000 } });

    assert.equal(status, 200);
    assert.deepEqual(changesOf(body).deferred, ["server.port"], "the save holds the change for a restart");
    assert.deepEqual(changesOf(body).applied, [], "nothing is applied live");
    assert.equal(CONFIG.server.port, 5589, "the running configuration keeps the port the listener is bound to");
    assert.equal(getNestedValue(await readPersistedJson(ctx, "config.json"), "server.port"), 6000, "the value is on disk");
    assert.deepEqual(getConfigurationGap().held.map((change) => change.path), ["server.port"]);
  });

  test("a live save commits at once and answers its path as applied", async () => {

    await using ctx = await createIntegrationContext();
    const app = await boot(ctx);

    const { body, status } = await post(app, "/config", { playback: { stallThreshold: 0.2 } });

    assert.equal(status, 200);
    assert.deepEqual(changesOf(body).applied, ["playback.stallThreshold"]);
    assert.deepEqual(changesOf(body).deferred, [], "nothing is held for a restart");
    assert.deepEqual(changesOf(body).nextStream, [], "the response carries the next-stream list");
    assert.equal(CONFIG.playback.stallThreshold, 0.2);
  });

  test("an unrelated live save after a restart-class save defers nothing while the earlier change stays pending, and the cancelling save empties both",
    async () => {

      await using ctx = await createIntegrationContext();
      const app = await boot(ctx);

      assert.deepEqual(changesOf((await post(app, "/config", { server: { port: 6000 } })).body).deferred, ["server.port"],
        "precondition: the port is held for a restart");

      const unrelated = await post(app, "/config", { playback: { stallThreshold: 0.2 } });

      assert.deepEqual(changesOf(unrelated.body).deferred, [], "the unrelated save schedules no restart");
      assert.deepEqual(getConfigurationGap().held.map((change) => change.path), ["server.port"], "the earlier change is still pending");

      const cancelling = await post(app, "/config", { server: { port: 5589 } });

      assert.deepEqual(changesOf(cancelling.body).deferred, [], "writing the running value back schedules no restart");
      assert.deepEqual(getConfigurationGap().held, [], "nothing is pending any more");
    });

  test("a candidate that fails a cross-field check answers 400 with the port-conflict reason, and the file's bytes and CONFIG stay as they were", async () => {

    await using ctx = await createIntegrationContext();
    const app = await boot(ctx);

    assert.equal((await post(app, "/config", { playback: { stallThreshold: 0.2 } })).status, 200, "precondition: a valid save wrote the file");

    const bytesBefore = await readConfigBytes(ctx);
    const running = structuredClone(CONFIG);

    assert.notEqual(bytesBefore, null, "precondition: the file exists");

    // HDHomeRun emulation is on by default and the server binds every interface, so an HDHomeRun port equal to the server port is a conflict.
    const { body, status } = await post(app, "/config", { hdhr: { port: CONFIG.server.port } });

    assert.equal(status, 400);
    assert.match(String(body["error"]), /conflicts with the main server port/);
    assert.equal(await readConfigBytes(ctx), bytesBefore, "the refused save wrote nothing");
    assert.deepEqual(CONFIG, running, "the running configuration is unchanged");
  });
});

describe("POST /config - a leaf the process writes keeps its running value", () => {

  test("an empty save with the setup flag true in CONFIG and on disk defers nothing and leaves the flag true", async () => {

    await using ctx = await createIntegrationContext();
    const app = await boot(ctx);

    await markSetupCompleted();

    assert.equal(getNestedValue(await readPersistedJson(ctx, "config.json"), "channels.setupCompleted"), true, "the setup flag is on disk");
    assert.equal(CONFIG.channels.setupCompleted, true, "precondition: the flag is set in CONFIG");
    assert.deepEqual(changesOf((await post(app, "/config", { server: { port: 6000 } })).body).deferred, ["server.port"],
      "precondition: a restart-class save answers its path as deferred");

    const { body, status } = await post(app, "/config", {});

    assert.equal(status, 200);
    assert.deepEqual(changesOf(body).deferred, []);
    assert.equal(CONFIG.channels.setupCompleted, true, "the flag survives the save");
  });
});

describe("POST /config/import - channel display state takes effect live", () => {

  test("an import that disables a predefined channel answers its path as applied, schedules no restart, and the listing follows", async () => {

    await using ctx = await createIntegrationContext();
    const app = await boot(ctx);

    const key = Object.keys(getPredefinedChannels()).find((candidate) => candidate in getAllChannels());

    assert.ok(key !== undefined, "precondition: a predefined channel is listed");
    assert.equal(isPredefinedChannelDisabled(key), false, "precondition: the channel starts enabled");

    const { body, status } = await post(app, "/config/import", { channels: { disabledPredefined: [key] } });

    assert.equal(status, 200);
    assert.deepEqual(changesOf(body).applied, ["channels.disabledPredefined"], "the disabled list is realized live");
    assert.deepEqual(changesOf(body).deferred, []);
    assert.equal(body["willRestart"], false, "no restart is scheduled");
    assert.equal(isPredefinedChannelDisabled(key), true, "the channel is disabled at once");
    assert.equal(key in getAllChannels(), false, "the listing no longer offers it");
  });
});

describe("POST /config/import - the service filter and the DVR host take effect live", () => {

  test("an import changing the service filter and the DVR host answers both paths as applied, schedules no restart, and the running values follow", async () => {

    /* The filter and the host sit outside the settings metadata and their running copies are not CONFIG alone: the running filter is the services module's cache, and the
     * DVR host is read through getDvrHost. Each has a handler that realizes the candidate's value, so the import lands live. The host is a reserved name that
     * never resolves, so the logo population the host change starts reaches no DVR.
     */
    await using ctx = await createIntegrationContext();
    const app = await boot(ctx);

    assert.deepEqual(getEnabledServices(), [], "precondition: no service filter is running");
    assert.equal(getDvrHost(), null, "precondition: no DVR host is known");

    const { body, status } = await post(app, "/config/import", { channels: { enabledServices: ["hulu"] }, channelsDvr: { host: "settings-dvr.example.invalid" } });

    assert.equal(status, 200);
    assert.deepEqual(changesOf(body).applied, [ "channels.enabledServices", "channelsDvr.host" ], "the filter and the host are realized live");
    assert.deepEqual(changesOf(body).deferred, [], "nothing is held for a restart");
    assert.equal(body["willRestart"], false, "no restart is scheduled");
    assert.deepEqual(getEnabledServices(), ["hulu"], "the running filter follows the import");
    assert.equal(getDvrHost(), "settings-dvr.example.invalid", "the running DVR host follows the import");
  });
});

describe("POST /config - the pending view the response carries and the page renders", () => {

  test("a restart-class save answers its pending entry and the page marks it, a later save carries it again, and writing the running value back clears it",
    async () => {

      await using ctx = await createIntegrationContext();
      const app = await boot(ctx);
      const marker = "Takes effect after PrismCast restarts. Running value: " + String(CONFIG.server.port) + ".";
      const page = async (): Promise<string> => (await fetch(app.urlFor("/"))).text();
      const labelStart = (html: string): number => html.indexOf("<label class=\"form-label\" for=\"server-port\">");
      const portLabel = (html: string): string => html.slice(labelStart(html), html.indexOf("</label>", labelStart(html)));

      const saved = await post(app, "/config", { server: { port: 6000 } });
      const savedData = saved.body as unknown as SettingsSaveData & { message: string };

      assert.equal(saved.status, 200);
      assert.deepEqual(savedData.changes.deferred, ["server.port"], "the save holds the port for a restart");
      assert.equal(savedData.willRestart, false, "manual mode schedules no restart");
      assert.equal(savedData.deferred, false);
      assert.equal(savedData.activeStreams, 0);
      assert.equal(savedData.message, "Configuration saved. Restart PrismCast for the settings marked Restart to take effect.");
      assert.deepEqual(savedData.pending, [{ kind: "restart", path: "server.port", text: marker }], "the save's own response carries its pending entry");

      const marked = await page();

      assert.ok(marked.includes("<div class=\"form-pending\" data-pending-path=\"server.port\">" + marker + "</div>"), "the page shows the port's marker");
      assert.ok(portLabel(marked).includes("class=\"badge badge-restart\""), "the port's own label carries the restart badge");

      const unrelated = (await post(app, "/config", { playback: { stallThreshold: 0.2 } })).body as unknown as SettingsSaveData;

      assert.deepEqual(unrelated.changes.applied, ["playback.stallThreshold"], "the unrelated save applies its own value");
      assert.deepEqual(unrelated.changes.deferred, [], "and holds nothing of its own");
      assert.deepEqual(unrelated.pending.map((entry) => entry.path), ["server.port"], "the earlier save's entry is still pending");

      const cancelling = (await post(app, "/config", { server: { port: CONFIG.server.port } })).body as unknown as SettingsSaveData;

      assert.deepEqual(cancelling.pending, [], "writing the running value back leaves nothing pending");
      assert.ok((await page()).includes("<div class=\"form-pending\" data-pending-path=\"server.port\" hidden></div>"), "the page's slot is hidden again");
    });
});
