/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.reload.test.ts: Tests for saveConfiguration and the reconcile behind it, plus the capture correction a boot and every save make. The contracts exercised:
 *
 *   1. Classes decide what a save commits: a restart-class value lands on disk and in the loaded snapshot and stays out of CONFIG, a live value is committed
 *      once its handler realizes it, and a value a handler refuses is never committed and is retried by every later save.
 *
 *   2. A save reports its own outcome: deferred holds the restart-class changes this save introduced that still differ from the running value, rejected holds
 *      the refusals of changes this save asked for, and the gap accessor answers which saved settings the process does not yet reflect.
 *
 *   3. A save writes nothing it refuses: a candidate that fails validation, a file that fails to parse, and a file that cannot be read all leave the file,
 *      CONFIG, and the loaded snapshot exactly as they were.
 *
 *   4. Saves are serialized on the store's queue, held through each save's reconcile. The rows on overlapping saves run on the real store in
 *      index.ordering.test.ts, because the double this suite runs on models no chain.
 *
 *   5. The loaded snapshot moves with the commit: a reader that does not wait on the store's queue sees CONFIG and the snapshot only as a completed reconcile
 *      left them, and a boot records it from the file it read.
 *
 *   6. The boot and the outcome line report exactly what happened: one warning naming the failure the store reported, and no outcome line for a save with
 *      nothing to report.
 *
 *   7. A capture value the server cannot capture with is corrected rather than refused: a save carrying one commits the correction and logs one warning for
 *      each correction once its write lands, a save the validation or the store refuses logs none, the file holds the corrected value, and a value the
 *      environment supplies is corrected in every candidate and never written.
 *
 *   8. A save corrects the DeviceID on the file it writes while the emulation is enabled: a stored id that fails its checksum takes the running one, a save
 *      that turns the emulation on carries a generated id in its own write, and a save the validation or the store refuses announces no DeviceID correction.
 *
 *   9. A process write records the leaves the process owns: it commits exactly those leaves to CONFIG and the loaded snapshot, dispatches only their handlers
 *      whatever CONFIG held before, validates nothing, and leaves every other difference between CONFIG and the file to the next settings save; a write the
 *      store refuses commits its leaves to CONFIG alone with one warning and resolves, and a rejection raised once its write has landed rejects the call.
 *
 *  10. The runtime debug filter follows a saved filter through its registered handler: a save whose gap does not hold the filter leaves a filter the debug page
 *      applied ahead of its save in place, a save that changes the filter applies it, and a filter an environment source owns stays applied.
 *
 * config/index.ts composes its disk persistence behind the injectable ConfigStore port. Every row runs against the in-memory store double of index.helpers.ts,
 * built fresh for each row, whose doc comment states the store rules it keeps. Each row re-initializes CONFIG and the loaded snapshot from the stored file it
 * names through initializeConfiguration, so no row inherits another's state.
 */
import * as indexModule from "./index.ts";
import { LOG, getCurrentPattern, initDebugFilter, validateDeviceId } from "../utils/index.ts";
import { READ_FAILURE_MESSAGE, WRITE_FAILURE_MESSAGE, makeMemoryConfigStore } from "./index.helpers.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { computeConfigDiff, registerConfigChangeHandler, resetConfigChangeHandlers } from "./reactivity.ts";
import type { ChangeRejection } from "./reactivity.ts";
import { FileStoreParseError } from "./persistence.ts";
import type { MemoryConfigStore } from "./index.helpers.ts";
import type { Nullable } from "../types/index.ts";
import type { UserConfig } from "./userConfig.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The outcome line a reconcile logs when its save reported something.
const OUTCOME_LINE = "Configuration saved and reconciled with the running process.";

let store: MemoryConfigStore = makeMemoryConfigStore();

/**
 * Saves through the store double.
 * @param mutator - The change to apply to the held file.
 * @returns The save's outcome.
 */
async function save(mutator: (current: UserConfig) => void): Promise<Awaited<ReturnType<typeof indexModule.saveConfiguration>>> {

  return indexModule.saveConfiguration(mutator, store);
}

/**
 * Returns the paths of a list of changes, the shape most assertions compare.
 * @param changes - The changes.
 * @returns Their paths, in order.
 */
function paths(changes: readonly { readonly path: string }[]): string[] {

  return changes.map((change) => change.path);
}

/**
 * Registers a handler for one full path that refuses every change it is given while the flag it reads is set, the way the HDHomeRun surface refuses a port
 * another listener holds. The unit suite never loads the HDHomeRun module, so a row that needs a refusal registers this one.
 * @param path - The full configuration path the handler owns.
 * @param occupied - Reads whether the handler refuses at the time it runs.
 */
function registerRefusingHandler(path: string, occupied: () => boolean): void {

  registerConfigChangeHandler(path, async (changes): Promise<readonly ChangeRejection[]> => {

    if(!occupied()) {

      return [];
    }

    return changes.map((change) => ({ path: change.path, reason: "Port " + String(change.current) + " is occupied, so the change was not applied." }));
  });
}

beforeEach(async () => {

  store = makeMemoryConfigStore();
  resetConfigChangeHandlers();

  // The debug-filter handler registers at module load, so the reset above drops it and every row takes it back, as a suite that resets the registry must.
  registerConfigChangeHandler("logging.debugFilter", indexModule.applyDebugFilterChange);
  initDebugFilter("");

  await indexModule.initializeConfiguration(undefined, store);
});

afterEach(() => {

  resetConfigChangeHandlers();
  initDebugFilter("");
});

describe("saveConfiguration - each class reaches the running configuration as its rule states", () => {

  test("a restart-class save lands on disk and in the loaded snapshot, stays out of CONFIG, and is deferred and held", async () => {

    const result = await save((current) => { current.server = { port: 6000 }; });

    assert.equal(store.file.server?.port, 6000, "the value is on disk");
    assert.equal(indexModule.getLoadedConfiguration().server.port, 6000, "the loaded snapshot holds the saved value");
    assert.equal(indexModule.CONFIG.server.port, 5589, "the running configuration keeps the port the listener is bound to");
    assert.deepEqual(paths(result.deferred), ["server.port"], "the save holds the change for a restart");
    assert.deepEqual(paths(result.applied), []);
    assert.deepEqual(paths(indexModule.getConfigurationGap().held), ["server.port"], "the gap reports the change pending a restart");
  });

  test("a live save with no handler is committed and reported applied", async () => {

    const result = await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.equal(indexModule.CONFIG.playback.stallThreshold, 0.2);
    assert.deepEqual(paths(result.applied), ["playback.stallThreshold"]);
    assert.deepEqual(paths(result.deferred), []);
  });

  test("the debug filter commits live in its canonical form and reaches the runtime filter", async () => {

    const result = await save((current) => { current.logging = { debugFilter: "tuning:hulu, recovery" }; });

    assert.equal(indexModule.CONFIG.logging.debugFilter, "tuning:hulu,recovery", "CONFIG holds the canonical form");
    assert.equal(getCurrentPattern(), "tuning:hulu,recovery", "the runtime filter follows the committed value");
    assert.deepEqual(paths(result.applied), ["logging.debugFilter"]);
  });

  test("a handler reads the candidate running configuration while CONFIG still holds the previous value", async () => {

    let seen: Nullable<{ candidate: number; committed: number }> = null;

    registerConfigChangeHandler("playback.", async (_changes, next) => {

      seen = { candidate: next.playback.stallThreshold, committed: indexModule.CONFIG.playback.stallThreshold };

      return [];
    });

    await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(seen, { candidate: 0.2, committed: 0.1 }, "the handler ran with the change in the candidate and not yet in CONFIG");
    assert.equal(indexModule.CONFIG.playback.stallThreshold, 0.2, "the change is committed once the handler realized it");
  });
});

describe("saveConfiguration - a refused live change is never committed and is retried by every later save", () => {

  test("a refused port is reported by the save that asked for it, kept out of CONFIG, and left on disk", async () => {

    registerRefusingHandler("hdhr.port", () => true);

    const result = await save((current) => { current.hdhr = { port: 5005 }; });

    assert.deepEqual(result.rejected.map((refusal) => refusal.change.path), ["hdhr.port"]);
    assert.equal(result.rejected[0]?.reason, "Port 5005 is occupied, so the change was not applied.", "the handler's reason is carried verbatim");
    assert.equal(indexModule.CONFIG.hdhr.port, 5004, "the running configuration keeps the bound port");
    assert.equal(store.file.hdhr?.port, 5005, "the saved value stays on disk");
    assert.deepEqual(paths(indexModule.getConfigurationGap().live), ["hdhr.port"], "the gap reports the change as unrealized");
  });

  test("two consecutive refusals leave CONFIG on the running value after both", async () => {

    registerRefusingHandler("hdhr.port", () => true);

    await save((current) => { current.hdhr = { port: 5005 }; });
    await save((current) => { current.hdhr = { port: 5006 }; });

    assert.equal(indexModule.CONFIG.hdhr.port, 5004);
  });

  test("a refused port is retried by an unrelated save once the port is free, and is realized and reported applied", async () => {

    let occupied = true;

    registerRefusingHandler("hdhr.port", () => occupied);

    await save((current) => { current.hdhr = { port: 5005 }; });

    assert.equal(indexModule.CONFIG.hdhr.port, 5004, "precondition: the port was refused");

    occupied = false;

    const result = await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.equal(indexModule.CONFIG.hdhr.port, 5005, "the later save realized the port");
    assert.deepEqual(paths(result.applied), [ "hdhr.port", "playback.stallThreshold" ], "a value that finally took effect is reported applied");
  });

  test("an unrelated save while the port is still occupied reports its own outcome and no stale refusal", async () => {

    registerRefusingHandler("hdhr.port", () => true);

    const first = await save((current) => { current.hdhr = { port: 5005 }; });

    assert.equal(first.rejected.length, 1, "precondition: the first save reported the refusal");

    const result = await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(result.rejected, [], "the refusal belongs to the save that asked for the port");
    assert.deepEqual(paths(result.applied), ["playback.stallThreshold"]);
    assert.equal(indexModule.CONFIG.hdhr.port, 5004, "the port is still refused");
  });

  test("a refused port beside a realized friendly name in one save keeps the port out of CONFIG and commits the friendly name", async () => {

    // The commit sets each realized leaf on its own, so a refused change stays out of CONFIG beside a realized change in the same category.
    registerRefusingHandler("hdhr.port", () => true);

    const result = await save((current) => { current.hdhr = { ...current.hdhr, friendlyName: "PrismCast Den", port: 5005 }; });

    assert.deepEqual(result.rejected.map((refusal) => refusal.change.path), ["hdhr.port"], "the save reports the port rejected");
    assert.equal(indexModule.CONFIG.hdhr.port, 5004, "the running configuration keeps the port the boot read");
    assert.equal(indexModule.CONFIG.hdhr.friendlyName, "PrismCast Den", "the friendly name, which no handler refused, is committed");
  });
});

describe("saveConfiguration - deferred answers for the save that introduced a restart-class change", () => {

  test("a later unrelated live save defers nothing while the earlier change stays pending", async () => {

    const first = await save((current) => { current.server = { port: 6000 }; });

    assert.deepEqual(paths(first.deferred), ["server.port"], "precondition: the first save deferred the port");

    const result = await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(paths(result.deferred), [], "the unrelated save schedules no restart");
    assert.deepEqual(paths(indexModule.getConfigurationGap().held), ["server.port"], "the earlier change is still pending");
  });

  test("a save that writes the running value back defers nothing and empties the pending set", async () => {

    await save((current) => { current.server = { port: 6000 }; });

    assert.deepEqual(paths(indexModule.getConfigurationGap().held), ["server.port"], "precondition: the change is pending");

    const result = await save((current) => { current.server = { port: 5589 }; });

    assert.deepEqual(paths(result.deferred), [], "cancelling the change schedules no restart");
    assert.deepEqual(paths(indexModule.getConfigurationGap().held), [], "nothing is pending any more");
  });
});

describe("getConfigurationGap - the settings surface alone", () => {

  test("a leaf the process wrote ahead of the file is not reported as a saved setting the process has not realized", () => {

    indexModule.CONFIG.channels.setupCompleted = true;

    assert.deepEqual(paths(computeConfigDiff(indexModule.CONFIG, indexModule.getLoadedConfiguration())), ["channels.setupCompleted"],
      "precondition: the running configuration is ahead of the loaded snapshot");
    assert.deepEqual(indexModule.getConfigurationGap(), { held: [], live: [], nextStream: [] });
  });
});

describe("saveConfiguration - a refused save writes nothing and moves nothing", () => {

  test("a candidate that fails validation is refused with the reason, and the file, CONFIG and the loaded snapshot are unchanged", async () => {

    await save((current) => { current.server = { port: 6000 }; });

    const loaded = indexModule.getLoadedConfiguration();
    const running = indexModule.CONFIG;
    const fileBefore = structuredClone(store.file);
    const writesBefore = store.writes;

    assert.equal(loaded.server.port, 6000, "precondition: the loaded snapshot sits apart from the defaults");

    await assert.rejects(save((current) => { current.server = { port: 0 }; }), (error: unknown) => {

      assert.ok(error instanceof indexModule.ConfigurationRejectedError);
      assert.match(error.message, /^PORT must be at least 1, but it is 0\.$/);

      return true;
    });

    assert.equal(store.writes, writesBefore, "nothing was written");
    assert.deepEqual(store.file, fileBefore);
    assert.equal(indexModule.getLoadedConfiguration(), loaded, "the loaded snapshot is the same object");
    assert.equal(indexModule.CONFIG, running, "CONFIG is the same object");
    assert.equal(indexModule.CONFIG.server.port, 5589);
  });

  test("a hard error is refused, and a debug-filter change bundled with it never reaches the runtime filter", async () => {

    await assert.rejects(save((current) => {

      current.logging = { debugFilter: "tuning:hulu" };
      current.server = { port: 0 };
    }), { message: "PORT must be at least 1, but it is 0.", name: "ConfigurationRejectedError" });

    assert.equal(getCurrentPattern(), "", "the runtime debug filter is untouched");
    assert.equal(indexModule.CONFIG.server.port, 5589);
    assert.equal(store.file.logging, undefined, "nothing was written");
  });

  test("an object at a setting that takes a single value is refused by name, with nothing written and CONFIG unchanged", async () => {

    const writesBefore = store.writes;

    await assert.rejects(save((current) => {

      current.hdhr = { friendlyName: { nested: "value" } as unknown as string };
    }), { message: "hdhr.friendlyName holds an object, which this setting does not accept.", name: "ConfigurationRejectedError" });

    assert.equal(store.writes, writesBefore, "nothing was written");
    assert.equal(indexModule.CONFIG.hdhr.friendlyName, "PrismCast");
  });

  test("a list at a setting that takes a single value is refused by name, with nothing written and CONFIG unchanged", async () => {

    const writesBefore = store.writes;

    await assert.rejects(save((current) => {

      current.hdhr = { friendlyName: [ "Den", "Office" ] as unknown as string };
    }), { message: "hdhr.friendlyName holds a list, which this setting does not accept.", name: "ConfigurationRejectedError" });

    assert.equal(store.writes, writesBefore, "nothing was written");
    assert.equal(indexModule.CONFIG.hdhr.friendlyName, "PrismCast");
  });

  test("a monitor interval below its floor is refused by name, with nothing written and CONFIG unchanged", async () => {

    const writesBefore = store.writes;

    await assert.rejects(save((current) => { current.playback = { monitorInterval: 499 }; }),
      { message: "MONITOR_INTERVAL must be at least 500, but it is 499.", name: "ConfigurationRejectedError" });

    assert.equal(store.writes, writesBefore, "nothing was written");
    assert.equal(indexModule.CONFIG.playback.monitorInterval, 2000);
  });

  test("a monitor interval above its ceiling is refused by name, with nothing written and CONFIG unchanged", async () => {

    const writesBefore = store.writes;

    await assert.rejects(save((current) => { current.playback = { monitorInterval: 30001 }; }),
      { message: "MONITOR_INTERVAL must be at most 30000, but it is 30001.", name: "ConfigurationRejectedError" });

    assert.equal(store.writes, writesBefore, "nothing was written");
    assert.equal(indexModule.CONFIG.playback.monitorInterval, 2000);
  });

  test("a stale page cleanup interval below its floor is refused by name, with nothing written and CONFIG unchanged", async () => {

    const writesBefore = store.writes;

    await assert.rejects(save((current) => { current.recovery = { stalePageCleanupInterval: 9999 }; }),
      { message: "STALE_PAGE_CLEANUP_INTERVAL must be at least 10000, but it is 9999.", name: "ConfigurationRejectedError" });

    assert.equal(store.writes, writesBefore, "nothing was written");
    assert.equal(indexModule.CONFIG.recovery.stalePageCleanupInterval, 60000);
  });

  test("a stale page cleanup interval above its ceiling is refused by name, with nothing written and CONFIG unchanged", async () => {

    const writesBefore = store.writes;

    await assert.rejects(save((current) => { current.recovery = { stalePageCleanupInterval: 600001 }; }),
      { message: "STALE_PAGE_CLEANUP_INTERVAL must be at most 600000, but it is 600001.", name: "ConfigurationRejectedError" });

    assert.equal(store.writes, writesBefore, "nothing was written");
    assert.equal(indexModule.CONFIG.recovery.stalePageCleanupInterval, 60000);
  });

  test("an HDHomeRun port equal to the server port on an all-interfaces host is refused, and the next valid save completes without a second read", async () => {

    const readsBefore = store.reads;

    await assert.rejects(save((current) => { current.hdhr = { port: 5589 }; }), /conflicts with the main server port/);
    assert.equal(store.file.hdhr?.port, undefined, "the refused save wrote nothing");

    const result = await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(paths(result.applied), ["playback.stallThreshold"], "the next save completes");
    assert.equal(store.file.hdhr?.port, undefined, "the refused port never reached the file");
    assert.equal(store.reads, readsBefore, "a save reconciles the candidate its mutation built, without reading the file again");
  });

  test("a file that fails to parse refuses the save inside the store, with nothing written or reconciled", async () => {

    await save((current) => { current.server = { port: 6000 }; });

    const loaded = indexModule.getLoadedConfiguration();
    const running = indexModule.CONFIG;

    assert.equal(loaded.server.port, 6000, "precondition: the loaded snapshot sits apart from the defaults");

    store.armedFailure = "parse";

    await assert.rejects(save((current) => { current.playback = { stallThreshold: 0.2 }; }), FileStoreParseError);

    assert.equal(indexModule.getLoadedConfiguration(), loaded, "the loaded snapshot is the same object");
    assert.equal(indexModule.CONFIG, running, "CONFIG is the same object");
    assert.equal(indexModule.CONFIG.playback.stallThreshold, 0.1);
  });

  test("a file that cannot be read refuses the save inside the store, with nothing written or reconciled", async () => {

    await save((current) => { current.server = { port: 6000 }; });

    const loaded = indexModule.getLoadedConfiguration();
    const running = indexModule.CONFIG;

    assert.equal(loaded.server.port, 6000, "precondition: the loaded snapshot sits apart from the defaults");

    store.armedFailure = "read";

    await assert.rejects(save((current) => { current.playback = { stallThreshold: 0.2 }; }), { message: READ_FAILURE_MESSAGE });

    assert.equal(indexModule.getLoadedConfiguration(), loaded, "the loaded snapshot is the same object");
    assert.equal(indexModule.CONFIG, running, "CONFIG is the same object");
    assert.equal(indexModule.CONFIG.playback.stallThreshold, 0.1);
  });
});

describe("saveConfiguration - the loaded snapshot moves with the commit", () => {

  test("a handler reads the snapshot from before the save and a gap without the path it is realizing, and the snapshot moves once the commit lands",
    async () => {

      const dispatchedPath = "playback.stallThreshold";
      const before = indexModule.getLoadedConfiguration();
      let seen: Nullable<{ inHeld: boolean; inLive: boolean; inNextStream: boolean; loadedUnchanged: boolean }> = null;

      // The handler records what a reader that does not wait on the store's queue sees while the change is being dispatched. It asserts nothing itself,
      // because a throw inside a handler is a rejection of its bucket rather than a failed test.
      registerConfigChangeHandler("playback.", async () => {

        const gap = indexModule.getConfigurationGap();

        seen = {

          inHeld: paths(gap.held).includes(dispatchedPath),
          inLive: paths(gap.live).includes(dispatchedPath),
          inNextStream: paths(gap.nextStream).includes(dispatchedPath),
          loadedUnchanged: indexModule.getLoadedConfiguration() === before
        };

        return [];
      });

      const result = await save((current) => { current.playback = { stallThreshold: 0.2 }; });

      assert.deepEqual(paths(result.applied), [dispatchedPath], "precondition: the change was dispatched and realized");
      assert.deepEqual(seen, { inHeld: false, inLive: false, inNextStream: false, loadedUnchanged: true },
        "while the handler ran, no member of the gap listed the dispatched path and the snapshot was the one from before the save");
      assert.notEqual(indexModule.getLoadedConfiguration(), before, "the snapshot moved once the reconcile completed");
      assert.equal(indexModule.getLoadedConfiguration().playback.stallThreshold, 0.2, "the snapshot holds the saved value");
      assert.deepEqual(indexModule.getConfigurationGap(), { held: [], live: [], nextStream: [] }, "CONFIG and the snapshot agree once the commit lands");
    });

  test("the boot records the loaded snapshot from the file it read, replacing the one the last reconcile recorded", async () => {

    await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.equal(indexModule.getLoadedConfiguration().playback.stallThreshold, 0.2, "precondition: the last reconcile recorded the saved value");

    store.file = { playback: { stallThreshold: 0.3 } };
    await indexModule.initializeConfiguration(undefined, store);

    assert.equal(indexModule.getLoadedConfiguration().playback.stallThreshold, 0.3, "the snapshot holds the value the boot read");
    assert.deepEqual(indexModule.getConfigurationGap(), { held: [], live: [], nextStream: [] }, "CONFIG and the snapshot agree after the boot");
  });
});

describe("the boot warning and the outcome line", () => {

  const PARSE_WARNING = "The configuration file is not valid JSON and has no usable backup, so the configuration starts from the defaults and saves are refused " +
    "until it is.";
  const READ_WARNING = "The configuration file could not be read, so the configuration starts from the defaults and saves are refused until it can be.";

  test("a store that reports a read failure logs the read-failure warning once and no parse-failure warning, and the boot starts from the defaults",
    async (t) => {

      const warn = t.mock.method(LOG, "warn", () => undefined);

      store.file = { server: { port: 6000 } };
      store.armedFailure = "read";
      await indexModule.initializeConfiguration(undefined, store);

      const messages = warn.mock.calls.map((call) => call.arguments[0]);

      assert.equal(messages.filter((message) => message === READ_WARNING).length, 1, "the read failure is named once");
      assert.equal(messages.filter((message) => message === PARSE_WARNING).length, 0, "no parse failure is reported");
      assert.equal(indexModule.CONFIG.server.port, 5589, "the configuration starts from the defaults");
    });

  test("a store that reports a parse failure logs the parse-failure warning once and no read-failure warning, and the boot starts from the defaults",
    async (t) => {

      const warn = t.mock.method(LOG, "warn", () => undefined);

      store.file = { server: { port: 6000 } };
      store.armedFailure = "parse";
      await indexModule.initializeConfiguration(undefined, store);

      const messages = warn.mock.calls.map((call) => call.arguments[0]);

      assert.equal(messages.filter((message) => message === PARSE_WARNING).length, 1, "the parse failure is named once");
      assert.equal(messages.filter((message) => message === READ_WARNING).length, 0, "no read failure is reported");
      assert.equal(indexModule.CONFIG.server.port, 5589, "the configuration starts from the defaults");
    });

  test("a save that changes a setting logs the outcome line once with its bucket counts, and a save with every bucket empty logs none", async (t) => {

    const info = t.mock.method(LOG, "info", () => undefined);
    const outcomeLines = (): unknown[][] => info.mock.calls.filter((call) => call.arguments[0] === OUTCOME_LINE).map((call) => [...call.arguments]);

    await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(outcomeLines(), [[ OUTCOME_LINE, { applied: 1, deferred: 0, nextStream: 0, rejected: 0 } ]],
      "precondition: a save that changes a setting logs the line once");

    info.mock.resetCalls();

    const result = await save(() => { /* A save that changes nothing. */ });

    assert.deepEqual(result, { applied: [], deferred: [], nextStream: [], rejected: [] }, "precondition: every bucket is empty");
    assert.deepEqual(outcomeLines(), [], "a save with nothing to report logs no outcome line");
  });
});

/* A capture value the server cannot capture with is corrected in every candidate, the boot's and each save's, and the store's write hook stores the file's own
 * values corrected. The rows that set a capture environment variable restore the environment afterward, so the next row's boot reads none of it.
 */
describe("the boot's capture correction and the saves after it", () => {

  const BASELINE_WARNING = "The configured capture codecs omit the H.264 baseline, so it is restored.";
  const MODE_WARNING = "Native capture mode is unavailable because of a Chrome fMP4 MediaRecorder defect, so FFmpeg capture is in use.";
  const UNRECOGNIZED_WARNING = "The configured capture codecs include identifiers the server does not recognize, so they are ignored.";
  const CAPTURE_WARNINGS = new Set<unknown>([ BASELINE_WARNING, MODE_WARNING, UNRECOGNIZED_WARNING ]);
  const ORIGINAL_ENV = { ...process.env };

  /**
   * Answers the capture warnings a mocked LOG.warn received, in the order they were logged, so a row compares the whole list and a repeated or missing warning
   * fails it.
   * @param calls - The mock's recorded calls.
   * @returns The capture warnings among them.
   */
  function captureWarningsIn(calls: readonly { readonly arguments: readonly unknown[] }[]): unknown[] {

    return calls.map((call) => call.arguments[0]).filter((message) => CAPTURE_WARNINGS.has(message));
  }

  afterEach(() => {

    for(const key of Object.keys(process.env)) {

      Reflect.deleteProperty(process.env, key);
    }

    Object.assign(process.env, ORIGINAL_ENV);
  });

  test("the loaded snapshot follows the boot's capture correction, so a save that changes nothing reports nothing", async () => {

    // The opening save shows that this row's saves report what they change, so the empty outcome at the end is a reading rather than a default.
    const opening = await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(paths(opening.applied), ["playback.stallThreshold"], "precondition: a save reports the setting it changed");

    store.file = { streaming: { captureMode: "native" } };
    await indexModule.initializeConfiguration(undefined, store);

    const loaded = indexModule.getLoadedConfiguration();

    assert.equal(indexModule.CONFIG.streaming.captureMode, "ffmpeg");
    assert.deepEqual([ loaded.streaming.captureMode, loaded.streaming.captureCodecs ],
      [ indexModule.CONFIG.streaming.captureMode, indexModule.CONFIG.streaming.captureCodecs ], "the loaded snapshot holds the capture values CONFIG holds");

    const result = await save(() => { /* A save that changes nothing. */ });

    assert.deepEqual(result, { applied: [], deferred: [], nextStream: [], rejected: [] });
  });

  test("a save of native capture mode commits FFmpeg capture with one warning, stores no capture mode, and reports nothing rejected", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);
    const result = await save((current) => { current.streaming = { captureMode: "native" }; });

    assert.equal(indexModule.CONFIG.streaming.captureMode, "ffmpeg", "the running configuration holds FFmpeg capture");
    assert.equal(indexModule.getLoadedConfiguration().streaming.captureMode, "ffmpeg", "the loaded snapshot holds FFmpeg capture");
    assert.deepEqual(captureWarningsIn(warn.mock.calls), [MODE_WARNING], "the correction warns once");
    assert.equal(store.file.streaming?.captureMode, undefined, "the file holds no capture mode, because the corrected mode is the default");
    assert.deepEqual(result.rejected, [], "nothing is rejected");
  });

  test("a save after a boot that corrected the stored capture mode logs no capture warning", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { streaming: { captureMode: "native" } };
    await indexModule.initializeConfiguration(undefined, store);

    assert.deepEqual(captureWarningsIn(warn.mock.calls), [MODE_WARNING], "precondition: the boot corrected the stored mode with one warning");
    assert.equal(store.file.streaming?.captureMode, undefined, "precondition: the boot's write stored the correction");

    warn.mock.resetCalls();

    await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(captureWarningsIn(warn.mock.calls), [], "the save builds its candidate from a file that needs no correction");
  });

  test("a save while the environment supplies a native mode and a list with an unrecognized codec warns once per correction and writes neither value",
    async (t) => {

      process.env["CAPTURE_MODE"] = "native";

      // The list's correction, ["h264"], differs from the default and would survive filterDefaults, so a leak of the environment's list into the file shows
      // here, where the mode cannot show one because its one correction is the default.
      process.env["CAPTURE_CODECS"] = "h264,av1";
      await indexModule.initializeConfiguration(undefined, store);

      const warn = t.mock.method(LOG, "warn", () => undefined);

      await save((current) => { current.playback = { stallThreshold: 0.2 }; });

      assert.deepEqual(captureWarningsIn(warn.mock.calls), [ MODE_WARNING, UNRECOGNIZED_WARNING ],
        "the save's candidate re-applies the environment, so each correction warns again, once");
      assert.equal(store.file.streaming?.captureMode, undefined, "the environment's mode is never written");
      assert.equal(store.file.streaming?.captureCodecs, undefined, "the environment's codec list is never written");
    });

  test("a save the hard-error check refuses beside a stored native capture mode logs no capture warning", async (t) => {

    // The environment's list and the stored mode each need a correction in the save's candidate, so a warning logged before the validation shows here.
    process.env["CAPTURE_CODECS"] = "hevc";
    store.file = { streaming: { captureMode: "native" } };

    const warn = t.mock.method(LOG, "warn", () => undefined);

    await assert.rejects(save((current) => { current.server = { port: 0 }; }), indexModule.ConfigurationRejectedError);

    assert.deepEqual(captureWarningsIn(warn.mock.calls), [], "a save the validation refuses announces none of its candidate's corrections");
  });

  test("a save of a playback setting while the environment supplies a list without the baseline logs the baseline warning once", async (t) => {

    process.env["CAPTURE_CODECS"] = "hevc";

    const warn = t.mock.method(LOG, "warn", () => undefined);

    await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(captureWarningsIn(warn.mock.calls), [BASELINE_WARNING], "a landed save announces its candidate's correction once");
  });

  test("a save the store refuses at the write beside a stored native capture mode logs no capture warning", async (t) => {

    // The store refuses once the save's callback has run and its validation has passed, so a warning logged inside the callback shows here.
    process.env["CAPTURE_CODECS"] = "hevc";
    store.file = { streaming: { captureMode: "native" } };
    store.armedFailure = "write";

    const warn = t.mock.method(LOG, "warn", () => undefined);

    await assert.rejects(save((current) => { current.playback = { stallThreshold: 0.2 }; }), { message: WRITE_FAILURE_MESSAGE });

    assert.deepEqual(captureWarningsIn(warn.mock.calls), [], "a save the store refuses announces none of its candidate's corrections");
    assert.equal(store.file.streaming?.captureMode, "native", "the file keeps its stored mode, because the refused write kept nothing");
  });
});

/* A settings save corrects the DeviceID on the file object it writes while the emulation is enabled. Every row starts from the boot of the suite's empty file,
 * which generated the running DeviceID and stored it, and a row that needs the emulation off boots again from a file that turns it off.
 */
describe("saveConfiguration - the DeviceID correction", () => {

  const DEVICE_ID_GENERATED = "An HDHomeRun DeviceID was generated.";
  const DEVICE_ID_REPLACED = "The configured HDHomeRun DeviceID fails its checksum, so a valid one takes its place.";

  /**
   * Answers the DeviceID lines among a mocked logger method's recorded calls, in the order they were logged.
   * @param calls - The mock's recorded calls.
   * @returns Each DeviceID line's arguments.
   */
  function deviceIdLines(calls: readonly { readonly arguments: readonly unknown[] }[]): unknown[][] {

    return calls.filter((call) => [ DEVICE_ID_GENERATED, DEVICE_ID_REPLACED ].includes(call.arguments[0] as string)).map((call) => [...call.arguments]);
  }

  test("a save whose mutator stores a DeviceID that fails its checksum keeps the running id in the file and in CONFIG, with the warning once", async (t) => {

    const running = indexModule.CONFIG.hdhr.deviceId;
    const warn = t.mock.method(LOG, "warn", () => undefined);

    assert.equal(store.file.hdhr?.deviceId, running, "precondition: the boot stored its generated DeviceID");

    const result = await save((current) => { current.hdhr = { ...current.hdhr, deviceId: "10000000" }; });
    const saved = store.file;

    assert.equal(saved.hdhr?.deviceId, running, "the file keeps the running DeviceID");
    assert.equal(indexModule.CONFIG.hdhr.deviceId, running, "CONFIG keeps the running DeviceID");
    assert.deepEqual(deviceIdLines(warn.mock.calls), [[ DEVICE_ID_REPLACED, { configured: "10000000", using: running.toUpperCase() } ]]);
    assert.deepEqual(result.rejected, [], "the save reports no rejection for the DeviceID");
  });

  test("a save over a file whose DeviceID was removed by hand stores the running id in the file and in CONFIG and logs no DeviceID line", async (t) => {

    const running = indexModule.CONFIG.hdhr.deviceId;
    const warn = t.mock.method(LOG, "warn", () => undefined);
    const info = t.mock.method(LOG, "info", () => undefined);

    assert.equal(store.file.hdhr?.deviceId, running, "precondition: the boot stored its generated DeviceID");

    // A hand edit removes the stored id once the boot has run, so the save's candidate holds an empty id and the running one takes its place.
    store.file = { ...store.file, hdhr: {} };
    await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.equal(store.file.playback?.stallThreshold, 0.2, "precondition: the save landed");
    assert.equal(store.file.hdhr?.deviceId, running, "the file stores the running DeviceID");
    assert.equal(indexModule.CONFIG.hdhr.deviceId, running, "CONFIG keeps the running DeviceID");
    assert.deepEqual(deviceIdLines([ ...warn.mock.calls, ...info.mock.calls ]), [], "an empty stored id that took the running one announces nothing");
  });

  test("a save that turns the emulation on over a file that holds no DeviceID writes a generated id in that same write", async (t) => {

    const info = t.mock.method(LOG, "info", () => undefined);

    store.file = { hdhr: { enabled: false } };
    await indexModule.initializeConfiguration(undefined, store);

    assert.equal(indexModule.CONFIG.hdhr.deviceId, "", "precondition: the disabled boot generated no DeviceID");

    const writesBefore = store.writes;

    info.mock.resetCalls();
    await save((current) => { current.hdhr = { ...current.hdhr, enabled: true }; });

    assert.equal(store.writes, writesBefore + 1, "the save wrote once");
    assert.ok(validateDeviceId(store.file.hdhr?.deviceId ?? ""), "the file holds a DeviceID that passes its checksum");
    assert.equal(indexModule.CONFIG.hdhr.deviceId, store.file.hdhr?.deviceId, "CONFIG runs the DeviceID the file holds");
    assert.deepEqual(deviceIdLines(info.mock.calls), [[ DEVICE_ID_GENERATED, { deviceId: indexModule.CONFIG.hdhr.deviceId.toUpperCase() } ]]);
  });

  test("a save of another setting on a disabled emulation with no stored DeviceID leaves the file with none", async () => {

    store.file = { hdhr: { enabled: false } };
    await indexModule.initializeConfiguration(undefined, store);

    await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.equal(store.file.playback?.stallThreshold, 0.2, "precondition: the save landed");
    assert.equal(store.file.hdhr?.deviceId, undefined, "the file holds no DeviceID");
  });

  test("a save the hard-error check refuses beside a stored DeviceID that fails its checksum logs no DeviceID line", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);
    const info = t.mock.method(LOG, "info", () => undefined);

    store.file = { ...store.file, hdhr: { deviceId: "10000000" } };

    await assert.rejects(save((current) => { current.server = { port: 0 }; }), indexModule.ConfigurationRejectedError);

    assert.deepEqual(deviceIdLines([ ...warn.mock.calls, ...info.mock.calls ]), [], "a refused save announces no DeviceID correction");
  });

  test("a save the store refuses at the write beside a stored DeviceID that fails its checksum logs no DeviceID line", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);
    const info = t.mock.method(LOG, "info", () => undefined);

    store.file = { ...store.file, hdhr: { deviceId: "10000000" } };
    store.armedFailure = "write";

    await assert.rejects(save((current) => { current.playback = { stallThreshold: 0.2 }; }), { message: WRITE_FAILURE_MESSAGE });

    assert.deepEqual(deviceIdLines([ ...warn.mock.calls, ...info.mock.calls ]), [], "a save the store refused announces no DeviceID correction");
  });
});

/* A process write records leaves the process already holds and owns. Every row starts from the boot of the suite's empty file, so the held file carries the
 * DeviceID that boot generated and nothing else, and CONFIG and the loaded snapshot hold the defaults.
 */
describe("writeProcessFields - the process write", () => {

  const REFUSED_WRITE = "The configuration file refused a process write, so the new value applies to the running configuration alone.";
  const HANDLER_REFUSED = "A configuration handler refused a value the process owns, so the running configuration keeps it regardless.";
  const MODE_WARNING = "Native capture mode is unavailable because of a Chrome fMP4 MediaRecorder defect, so FFmpeg capture is in use.";

  /**
   * Writes one process leaf through the store double.
   * @param fields - Answers the leaves to write from the stored configuration it is handed.
   */
  async function write(fields: (stored: Readonly<UserConfig>) => indexModule.ProcessFieldValues): Promise<void> {

    await indexModule.writeProcessFields(fields, store);
  }

  /**
   * Counts the calls among a mocked logger method's recorded calls that carry one message.
   * @param calls - The mock's recorded calls.
   * @param message - The message to count.
   * @returns How many calls carried it.
   */
  function countOf(calls: readonly { readonly arguments: readonly unknown[] }[], message: string): number {

    return calls.filter((call) => call.arguments[0] === message).length;
  }

  test("a write beside a hand edit commits only its own leaf, leaves the edit for the next settings save, and that save defers the edit", async () => {

    // The hand edit lands in the file once the boot has read it, so CONFIG and the loaded snapshot hold the port the boot read and the edit is in neither.
    store.file = { ...store.file, server: { port: 6000 } };

    await write(() => ({ "channels.channelSortField": "channelNumber" }));

    assert.equal(store.file.server?.port, 6000, "the held file keeps the hand edit");
    assert.equal(store.file.channels?.channelSortField, "channelNumber", "the held file carries the written leaf beside it");
    assert.equal(indexModule.CONFIG.channels.channelSortField, "channelNumber", "CONFIG holds the written leaf");
    assert.equal(indexModule.getLoadedConfiguration().channels.channelSortField, "channelNumber", "the loaded snapshot holds the written leaf");
    assert.equal(indexModule.CONFIG.server.port, 5589, "CONFIG keeps the port the boot read");
    assert.equal(indexModule.getLoadedConfiguration().server.port, 5589, "the loaded snapshot keeps the port the boot read");
    assert.deepEqual(paths(indexModule.getConfigurationGap().held), [], "the process write reports nothing pending a restart");

    const result = await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(paths(result.deferred), ["server.port"], "the next settings save reconciles the hand edit and defers it for a restart");
  });

  test("a write whose list equals the one CONFIG holds dispatches the list's handler once", async () => {

    let calls = 0;

    registerConfigChangeHandler("channels.enabledServices", async () => {

      calls++;

      return [];
    });

    await write(() => ({ "channels.enabledServices": [...indexModule.CONFIG.channels.enabledServices] }));

    assert.equal(calls, 1, "the handler re-derives its state from the written list even though CONFIG already held it");
  });

  test("a write the store refuses at the read commits the leaf to CONFIG alone, dispatches its handler, logs the refusal once, and resolves", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);
    let calls = 0;

    registerConfigChangeHandler("channels.channelSortField", async () => {

      calls++;

      return [];
    });

    store.armedFailure = "read";

    await write(() => ({ "channels.channelSortField": "channelNumber" }));

    assert.equal(indexModule.CONFIG.channels.channelSortField, "channelNumber", "the running configuration holds the leaf");
    assert.equal(indexModule.getLoadedConfiguration().channels.channelSortField, "name", "the loaded snapshot keeps the file's value");
    assert.equal(calls, 1, "the leaf's handler was dispatched");
    assert.equal(countOf(warn.mock.calls, REFUSED_WRITE), 1, "the refusal is logged once");
  });

  test("a write the store refuses once its callback ran answers its leaves again from a clone of CONFIG and commits that answer to CONFIG alone",
    async (t) => {

      // The held file and CONFIG disagree on the list, so the answer fields gives on each tells which one the refused path committed.
      const warn = t.mock.method(LOG, "warn", () => undefined);
      const handed: Readonly<UserConfig>[] = [];

      store.file = { ...store.file, channels: { disabledPredefined: ["a"] } };
      store.armedFailure = "write";

      assert.deepEqual(indexModule.CONFIG.channels.disabledPredefined, [], "precondition: CONFIG holds the empty list the boot read");

      await write((stored) => {

        handed.push(stored);

        const list = stored.channels?.disabledPredefined;

        return { "channels.disabledPredefined": [ ...(Array.isArray(list) ? list : []), "x" ] };
      });

      assert.equal(handed.length, 2, "fields ran on the file the store read and again on the refused path");
      assert.notEqual(handed[1], indexModule.CONFIG, "the refused path handed fields an object other than CONFIG");
      assert.deepEqual(store.file.channels?.disabledPredefined, ["a"], "the held file is as it was");
      assert.deepEqual(indexModule.CONFIG.channels.disabledPredefined, ["x"], "CONFIG holds the answer fields gave on its clone of CONFIG");
      assert.deepEqual(indexModule.getLoadedConfiguration().channels.disabledPredefined, [], "the loaded snapshot keeps the file's value");
      assert.equal(countOf(warn.mock.calls, REFUSED_WRITE), 1, "the refusal is logged once");
    });

  test("a handler that refuses a process leaf leaves the leaf committed and logs the refusal once", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    registerRefusingHandler("channels.channelSortField", () => true);

    await write(() => ({ "channels.channelSortField": "channelNumber" }));

    assert.equal(indexModule.CONFIG.channels.channelSortField, "channelNumber", "the process owns the leaf, so it is committed regardless");
    assert.equal(countOf(warn.mock.calls, HANDLER_REFUSED), 1, "the refusal is logged once");
  });

  test("a file holding an object at a setting that takes a single value takes a process write, which validates nothing", async (t) => {

    // A save would refuse this file by its shape; a process write records its own leaf and leaves the rest of the file to the next save.
    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { ...store.file, playback: { stallThreshold: { hand: "edited" } as unknown as number } };

    const writesBefore = store.writes;

    await write(() => ({ "channels.channelSortField": "channelNumber" }));

    assert.equal(store.writes, writesBefore + 1, "the double counted one write more");
    assert.equal(store.file.channels?.channelSortField, "channelNumber", "the held file carries the leaf");
    assert.equal(indexModule.getLoadedConfiguration().channels.channelSortField, "channelNumber", "the loaded snapshot holds the leaf");
    assert.equal(countOf(warn.mock.calls, REFUSED_WRITE), 0, "the write was not refused");
  });

  test("a file holding a number where the channels category belongs takes a process write of a channels leaf", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { ...store.file, channels: 5 as unknown as UserConfig["channels"] };

    const writesBefore = store.writes;

    await write(() => ({ "channels.channelSortField": "channelNumber" }));

    assert.equal(store.writes, writesBefore + 1, "the double counted one write more");
    assert.deepEqual(store.file.channels, { channelSortField: "channelNumber" }, "the held file holds a category with the leaf where the number stood");
    assert.equal(countOf(warn.mock.calls, REFUSED_WRITE), 0, "the write was not refused");
  });

  test("a process write dispatches only its own leaves, leaving a refused settings change undispatched and in the gap", async () => {

    let calls = 0;

    registerConfigChangeHandler("playback.", async (changes): Promise<readonly ChangeRejection[]> => {

      calls++;

      return changes.map((change) => ({ path: change.path, reason: "The playback setting cannot be applied." }));
    });

    await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    const callsAfterSave = calls;

    assert.deepEqual(paths(indexModule.getConfigurationGap().live), ["playback.stallThreshold"], "precondition: the refused change is in the gap");

    await write(() => ({ "channels.channelSortField": "channelNumber" }));

    assert.equal(calls, callsAfterSave, "the playback handler was not called again");
    assert.equal(indexModule.CONFIG.playback.stallThreshold, 0.1, "CONFIG keeps the running stall threshold");
    assert.deepEqual(paths(indexModule.getConfigurationGap().live), ["playback.stallThreshold"], "the refused change stays in the gap for the next save");
  });

  test("a process write logs no reconcile outcome line and no correction warning", async (t) => {

    // The stored native capture mode is one a candidate's build would correct with a warning, and the process write builds no candidate.
    const info = t.mock.method(LOG, "info", () => undefined);
    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { ...store.file, streaming: { captureMode: "native" } };

    await write(() => ({ "channels.channelSortField": "channelNumber" }));

    assert.equal(countOf(info.mock.calls, OUTCOME_LINE), 0, "no reconcile outcome line");
    assert.equal(countOf(warn.mock.calls, MODE_WARNING), 0, "no correction warning");
  });

  test("a follow-up that rejects once the write has landed rejects the call with its error and takes no refused-write path", async (t) => {

    // A path no setting or state entry classifies passes the store's write and then makes the class resolver throw inside the follow-up's commit.
    const warn = t.mock.method(LOG, "warn", () => undefined);
    const writesBefore = store.writes;
    let fieldsCalls = 0;

    await assert.rejects(write(() => {

      fieldsCalls++;

      return { "channels.unclassified": 1 } as unknown as indexModule.ProcessFieldValues;
    }), { message: "The configuration path channels.unclassified carries no reactivity class." });

    assert.equal(fieldsCalls, 1, "fields ran once, on the file the store read");
    assert.equal(store.writes, writesBefore + 1, "the write landed before the follow-up rejected");
    assert.equal(Object.hasOwn(indexModule.CONFIG.channels, "unclassified"), false, "CONFIG does not hold the leaf");
    assert.equal(Object.hasOwn(indexModule.getLoadedConfiguration().channels, "unclassified"), false,
      "the loaded snapshot does not hold the leaf, because it moves only once the commit has completed");
    assert.equal(countOf(warn.mock.calls, REFUSED_WRITE), 0, "no refused-write warning");
  });

  test("a landed process write followed by one the store refuses at the read takes the refused path for the second", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    await write(() => ({ "channels.channelSortField": "channelNumber" }));

    store.armedFailure = "read";

    await write(() => ({ "channels.channelSortDirection": "desc" }));

    assert.equal(countOf(warn.mock.calls, REFUSED_WRITE), 1, "the second write's refusal is logged once");
    assert.equal(indexModule.CONFIG.channels.channelSortDirection, "desc", "the second write's leaf is committed to CONFIG");
  });
});

/* The debug page applies its filter to the runtime before its save is queued, so the runtime pattern can run ahead of CONFIG. Each row sets it ahead the same
 * way, through initDebugFilter once the boot has run, and reads what a save leaves applied.
 */
describe("saveConfiguration - the runtime debug filter follows a saved filter through its handler", () => {

  test("a save of a playback change leaves a runtime pattern set ahead of CONFIG applied", async () => {

    initDebugFilter("tuning:hulu");

    await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.equal(getCurrentPattern(), "tuning:hulu", "a save whose gap does not hold the filter leaves the runtime filter as it stands");
  });

  test("a filter save the hard-error check refuses, followed by an unrelated save, leaves a runtime pattern set ahead applied", async () => {

    initDebugFilter("tuning:hulu");

    await assert.rejects(save((current) => {

      current.logging = { debugFilter: "tuning:hulu" };
      current.server = { port: 0 };
    }), indexModule.ConfigurationRejectedError);

    await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.equal(getCurrentPattern(), "tuning:hulu", "the refused save wrote nothing, and the unrelated save left the runtime filter as it stands");
  });

  test("an import-shaped save whose filter equals CONFIG's leaves a runtime pattern set ahead applied", async () => {

    initDebugFilter("tuning:hulu");

    await save((current) => {

      current.logging = { debugFilter: indexModule.CONFIG.logging.debugFilter };
      current.playback = { stallThreshold: 0.2 };
    });

    assert.equal(getCurrentPattern(), "tuning:hulu", "a filter equal to the saved one is no change, so the runtime filter stays");
  });

  test("a save that changes the filter applies it to the runtime", async () => {

    initDebugFilter("tuning:hulu");

    await save((current) => { current.logging = { debugFilter: "recovery" }; });

    assert.equal(getCurrentPattern(), "recovery", "the handler applies the changed filter");
  });

  test("a filter an environment source owns, set before the boot, stays applied through a save of another filter", async () => {

    // The runtime filter set before the boot stands in for PRISMCAST_DEBUG or --debug: the boot reads it as owned by a higher-priority source.
    initDebugFilter("streaming:showinfo");
    await indexModule.initializeConfiguration(undefined, store);

    await save((current) => { current.logging = { debugFilter: "recovery" }; });

    assert.equal(indexModule.CONFIG.logging.debugFilter, "recovery", "precondition: the save committed its filter");
    assert.equal(getCurrentPattern(), "streaming:showinfo", "the owned runtime filter stays");
  });
});
