/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cross-store-consistency.test.ts: Integration coverage for the rule that keeps an unknown service tag out of the running filter. The service filter is a list
 * the user saves in config.json, and whether a tag in it is known depends on another store - the tags come from the loaded channels and the user's domain
 * mappings - so the rule spans stores. The services module states it once, in applyServiceFilter, and applies it at boot once the service groups are built and
 * at every reconcile that changes the list: the running filter is the saved list restricted to the known tags, the file and CONFIG keep the list as saved, and
 * one warning names each tag the restriction ignores. The file keeps the user's list because a partial store load can shrink the known set, and persisting the
 * restriction would then delete tags that are legitimate once every store loads.
 *
 * The consistency probe reports what an operator must act on and changes nothing, and an unknown tag is not one of those, so the boot rows run it and assert it
 * leaves the file as it was. The probe's own checks are covered in consistency-probe.test.ts.
 *
 * Each row seeds its own data directory and calls initializeConfiguration() after createIntegrationContext and before initializePersistence, so CONFIG and the
 * loaded snapshot are read from that row's file rather than inherited from an earlier row in the same process.
 */
import type { BootedApp, IntegrationContext } from "../../helpers/integration.helpers.ts";
import { CONFIG, initializeConfiguration, saveConfiguration } from "../../../src/config/index.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { bootApp, createIntegrationContext, initializePersistence, pathInDataDir, readPersistedJson, writePersistedJson } from "../../helpers/integration.helpers.ts";
import { getEnabledServices, isServiceTagEnabled, setEnabledServices } from "../../../src/config/services.ts";
import type { LogEntry } from "../../../src/utils/logEmitter.ts";
import assert from "node:assert/strict";
import { getNestedValue } from "../../../src/config/userConfig.ts";
import { readFile } from "node:fs/promises";
import { runConsistencyProbeAtStartup } from "../../../src/config/consistencyProbe.ts";
import { subscribeToLogs } from "../../../src/utils/logEmitter.ts";

// Every log entry emitted during a row, so a row can count the warnings that name a tag the restriction ignored.
let captured: LogEntry[];

let unsubscribe: () => void;

beforeEach(() => {

  captured = [];
  unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
});

afterEach(() => {

  unsubscribe();
});

/**
 * Boots a row from a seeded configuration file: the file written, the configuration read from it, and the stores hydrated, which builds the running filter.
 * @param ctx - The row's integration context.
 * @param enabledServices - The service list the configuration file holds.
 */
async function bootWithServices(ctx: IntegrationContext, enabledServices: readonly string[]): Promise<void> {

  await writePersistedJson(ctx, "config.json", { channels: { enabledServices } });
  await initializeConfiguration();
  await initializePersistence(ctx);
}

/**
 * Reads the service list the configuration file holds.
 * @param ctx - The row's integration context.
 * @returns The persisted list, or undefined when the file holds none.
 */
async function persistedServices(ctx: IntegrationContext): Promise<unknown> {

  return getNestedValue(await readPersistedJson(ctx, "config.json"), "channels.enabledServices");
}

/**
 * Counts the warnings that name a tag.
 * @param tag - The tag a warning must name.
 * @returns How many captured warnings name it.
 */
function warningsNaming(tag: string): number {

  return captured.filter((entry) => (entry.level === "warn") && entry.message.includes(tag)).length;
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

describe("the running service filter is the saved list restricted to the known tags", () => {

  test("a file holding an unknown tag boots with the running filter excluding it, the file and CONFIG keeping it, and one warning naming it", async () => {

    await using ctx = await createIntegrationContext();

    const saved = [ "hulu", "sling", "unknown-tag-xyz", "spectrum" ];

    await bootWithServices(ctx, saved);

    assert.deepEqual(getEnabledServices(), [ "hulu", "sling", "spectrum" ], "the running filter keeps the known tags in their saved order");
    assert.deepEqual(CONFIG.channels.enabledServices, saved, "the running configuration holds the list as saved");
    assert.deepEqual(await persistedServices(ctx), saved, "the file keeps the user's list");
    assert.equal(warningsNaming("unknown-tag-xyz"), 1, "one warning names the ignored tag");

    // The probe reports only what an operator must act on, so it neither warns about the tag again nor rewrites the file.
    await runConsistencyProbeAtStartup();

    assert.equal(warningsNaming("unknown-tag-xyz"), 1, "the probe adds no warning about the tag");
    assert.deepEqual(await persistedServices(ctx), saved, "the probe leaves the file's list as saved");

    // The file and CONFIG agree on the list, so a save of an unrelated live value finds nothing to hold for the restart and leaves the filter as it is.
    const app = await bootApp(ctx);
    const { body, status } = await post(app, "/config", { playback: { stallThreshold: 0.2 } });

    assert.equal(status, 200);
    assert.equal(body["appliedCount"], 1, "precondition: the unrelated live value is applied");
    assert.equal(body["deferredCount"], 0, "nothing is held for a restart");
    assert.equal(body["willRestart"], false, "no restart is scheduled");
    assert.deepEqual(getEnabledServices(), [ "hulu", "sling", "spectrum" ], "the running filter is unchanged by the unrelated save");
    assert.deepEqual(await persistedServices(ctx), saved, "the file still keeps the user's list");
  });

  test("a save that writes a list holding an unknown tag restricts the running filter, and the file and CONFIG keep the list as saved", async () => {

    await using ctx = await createIntegrationContext();

    await bootWithServices(ctx, []);

    const saved = [ "hulu", "sling", "unknown-tag-xyz" ];
    const outcome = await saveConfiguration((config) => {

      config.channels ??= {};
      config.channels.enabledServices = [...saved];
    });

    assert.deepEqual(outcome.applied.map((change) => change.path), ["channels.enabledServices"], "the list is realized live");
    assert.deepEqual(outcome.deferred, [], "and held for no restart");
    assert.deepEqual(getEnabledServices(), [ "hulu", "sling" ], "the running filter excludes the unknown tag");
    assert.deepEqual(CONFIG.channels.enabledServices, saved, "the running configuration holds the list as saved");
    assert.deepEqual(await persistedServices(ctx), saved, "the file keeps the user's list");
    assert.equal(warningsNaming("unknown-tag-xyz"), 1, "one warning names the ignored tag");
  });

  test("a file of known tags boots with the running filter equal to it and no restriction warning, and the probe leaves the file as it was", async () => {

    await using ctx = await createIntegrationContext();

    await bootWithServices(ctx, ["hulu"]);

    assert.deepEqual(getEnabledServices(), ["hulu"], "the running filter is the saved list");

    const before = await readFile(pathInDataDir(ctx, "config.json"), "utf8");

    await runConsistencyProbeAtStartup();

    assert.equal(await readFile(pathInDataDir(ctx, "config.json"), "utf8"), before, "the probe leaves the file byte-for-byte as it was");
    assert.equal(warningsNaming("Ignoring unrecognized service tags"), 0, "the restriction ignores nothing, so it warns about nothing");
  });

  test("an empty list boots with no filter, so every service is enabled", async () => {

    await using ctx = await createIntegrationContext();

    // A filter left by an earlier boot in the same process is what the boot must replace.
    setEnabledServices(["hulu"]);

    assert.equal(isServiceTagEnabled("sling"), false, "precondition: a filter excluding sling is running");

    await bootWithServices(ctx, []);

    assert.deepEqual(getEnabledServices(), [], "the running filter is empty");
    assert.equal(isServiceTagEnabled("sling"), true, "an empty filter enables every service");
    await assert.doesNotReject(() => runConsistencyProbeAtStartup(), "the probe handles an empty list cleanly");
  });
});
