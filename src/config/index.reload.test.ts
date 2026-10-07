/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.reload.test.ts: Tests for saveConfiguration and the reconcile behind it, plus the persistCoercedConfig write-back. The contracts exercised:
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
 *   4. Saves are serialized: overlapping saves commit in the order their writes landed, and a refused save leaves the next one free to complete.
 *
 *   5. The loaded snapshot moves with the commit: a reader outside the reconcile queue sees CONFIG and the snapshot only as a completed reconcile left them.
 *
 *   6. The boot and the outcome line report exactly what happened: one warning naming the failure the store reported, and no outcome line for a save with
 *      nothing to report.
 *
 * config/index.ts composes its disk persistence behind the injectable ConfigStore port. Every row runs against the store double below, which holds a file in
 * memory, applies a save's mutation to a copy of it and keeps the copy only when the mutation completes - the store's own rule that a throw writes nothing - and
 * refuses the mutation, and reports the failure from its read, when a row arms a parse or read failure. Each row re-initializes CONFIG and the loaded snapshot
 * from that file through initializeConfiguration, so no row inherits another's state.
 */
import * as indexModule from "./index.ts";
import { LOG, getCurrentPattern, initDebugFilter } from "../utils/index.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { closePuppeteerStreamWssOnIdle, flushMicrotasks } from "../testing.helpers.ts";
import { computeConfigDiff, registerConfigChangeHandler, resetConfigChangeHandlers } from "./reactivity.ts";
import type { ChangeRejection } from "./reactivity.ts";
import { FileStoreParseError } from "./persistence.ts";
import type { Nullable } from "../types/index.ts";
import type { UserConfig } from "./userConfig.ts";
import assert from "node:assert/strict";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The store double's state: the file it holds, the failure a row arms, and how many reads and writes it served.
let file: UserConfig = {};
let armedFailure: Nullable<"parse" | "read"> = null;
let reads = 0;
let writes = 0;

const READ_FAILURE_MESSAGE = "The configuration file /memory/config.json could not be read, so nothing was written.";

// The outcome line a reconcile logs when its save reported something.
const OUTCOME_LINE = "Configuration saved and reconciled with the running process.";

/* The injected config store, typed as the production ConfigStore port so the double cannot drift from it. mutateConfig refuses on an armed failure exactly as
 * the file store does, before the callback runs, and otherwise runs the callback against a copy of the held file, keeping the copy only when the callback
 * returns, so a callback that throws writes nothing. readConfig answers an armed failure the way the file store does, with the defaults and the member that
 * names the failure.
 */
const io: indexModule.ConfigStore = {

  mutateConfig: async (fn) => {

    switch(armedFailure) {

      case "parse": {

        throw new FileStoreParseError("configuration", "/memory/config.json", "Unexpected token.");
      }

      case "read": {

        throw new Error(READ_FAILURE_MESSAGE);
      }

      default: {

        break;
      }
    }

    const working = structuredClone(file);

    fn(working);
    file = working;
    writes++;
  },
  readConfig: async () => {

    reads++;

    switch(armedFailure) {

      case "parse": {

        return { config: {}, parseError: true, parseErrorMessage: "Unexpected token.", readError: false };
      }

      case "read": {

        return { config: {}, parseError: false, readError: true };
      }

      default: {

        return { config: structuredClone(file), parseError: false, readError: false };
      }
    }
  }
};

/**
 * Saves through the store double.
 * @param mutator - The change to apply to the held file.
 * @returns The save's outcome.
 */
async function save(mutator: (current: UserConfig) => void): Promise<Awaited<ReturnType<typeof indexModule.saveConfiguration>>> {

  return indexModule.saveConfiguration(mutator, io);
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

  file = {};
  armedFailure = null;
  reads = 0;
  writes = 0;
  resetConfigChangeHandlers();
  initDebugFilter("");

  await indexModule.initializeConfiguration(undefined, io);
});

afterEach(() => {

  resetConfigChangeHandlers();
  initDebugFilter("");
});

describe("saveConfiguration - each class reaches the running configuration as its rule states", () => {

  test("a restart-class save lands on disk and in the loaded snapshot, stays out of CONFIG, and is deferred and held", async () => {

    const result = await save((current) => { current.server = { port: 6000 }; });

    assert.equal(file.server?.port, 6000, "the value is on disk");
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

  test("a process write that lands while a handler runs survives the reconcile's commit, even beside a realized change in its own category", async () => {

    // The handler writes a process-owned leaf of the category whose live change it is realizing, the way the HDHomeRun handler writes a generated DeviceID,
    // so a commit that assigned whole categories from the candidate would overwrite it.
    registerConfigChangeHandler("hdhr.", async () => {

      indexModule.CONFIG.hdhr.deviceId = "1234abcd";

      return [];
    });

    await save((current) => { current.hdhr = { friendlyName: "Den" }; });

    assert.equal(indexModule.CONFIG.hdhr.friendlyName, "Den", "the realized change is committed");
    assert.equal(indexModule.CONFIG.hdhr.deviceId, "1234abcd", "the write made during the dispatch is still in CONFIG");
  });
});

describe("saveConfiguration - a refused live change is never committed and is retried by every later save", () => {

  test("a refused port is reported by the save that asked for it, kept out of CONFIG, and left on disk", async () => {

    registerRefusingHandler("hdhr.port", () => true);

    const result = await save((current) => { current.hdhr = { port: 5005 }; });

    assert.deepEqual(result.rejected.map((refusal) => refusal.change.path), ["hdhr.port"]);
    assert.equal(result.rejected[0]?.reason, "Port 5005 is occupied, so the change was not applied.", "the handler's reason is carried verbatim");
    assert.equal(indexModule.CONFIG.hdhr.port, 5004, "the running configuration keeps the bound port");
    assert.equal(file.hdhr?.port, 5005, "the saved value stays on disk");
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
    const fileBefore = structuredClone(file);
    const writesBefore = writes;

    assert.equal(loaded.server.port, 6000, "precondition: the loaded snapshot sits apart from the defaults");

    await assert.rejects(save((current) => { current.server = { port: 0 }; }), (error: unknown) => {

      assert.ok(error instanceof indexModule.ConfigurationRejectedError);
      assert.match(error.message, /^PORT must be at least 1, but it is 0\.$/);

      return true;
    });

    assert.equal(writes, writesBefore, "nothing was written");
    assert.deepEqual(file, fileBefore);
    assert.equal(indexModule.getLoadedConfiguration(), loaded, "the loaded snapshot is the same object");
    assert.equal(indexModule.CONFIG, running, "CONFIG is the same object");
    assert.equal(indexModule.CONFIG.server.port, 5589);
  });

  test("a native capture mode is refused, and a debug-filter change bundled with it never reaches the runtime filter", async () => {

    await assert.rejects(save((current) => {

      current.logging = { debugFilter: "tuning:hulu" };
      current.streaming = { captureMode: "native" };
    }), /capture mode/);

    assert.equal(getCurrentPattern(), "", "the runtime debug filter is untouched");
    assert.equal(indexModule.CONFIG.streaming.captureMode, "ffmpeg");
    assert.equal(file.logging, undefined, "nothing was written");
  });

  test("an object at a setting that takes a single value is refused by name, with nothing written and CONFIG unchanged", async () => {

    const writesBefore = writes;

    await assert.rejects(save((current) => {

      current.hdhr = { friendlyName: { nested: "value" } as unknown as string };
    }), { message: "hdhr.friendlyName holds an object, which this setting does not accept.", name: "ConfigurationRejectedError" });

    assert.equal(writes, writesBefore, "nothing was written");
    assert.equal(indexModule.CONFIG.hdhr.friendlyName, "PrismCast");
  });

  test("a list at a setting that takes a single value is refused by name, with nothing written and CONFIG unchanged", async () => {

    const writesBefore = writes;

    await assert.rejects(save((current) => {

      current.hdhr = { friendlyName: [ "Den", "Office" ] as unknown as string };
    }), { message: "hdhr.friendlyName holds a list, which this setting does not accept.", name: "ConfigurationRejectedError" });

    assert.equal(writes, writesBefore, "nothing was written");
    assert.equal(indexModule.CONFIG.hdhr.friendlyName, "PrismCast");
  });

  test("an HDHomeRun port equal to the server port on an all-interfaces host is refused, and the next valid save completes without a second read", async () => {

    const readsBefore = reads;

    await assert.rejects(save((current) => { current.hdhr = { port: 5589 }; }), /conflicts with the main server port/);
    assert.equal(file.hdhr, undefined, "the refused save wrote nothing");

    const result = await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(paths(result.applied), ["playback.stallThreshold"], "the next save completes");
    assert.equal(file.hdhr, undefined, "the refused port never reached the file");
    assert.equal(reads, readsBefore, "a save reconciles the candidate its mutation built, without reading the file again");
  });

  test("a file that fails to parse refuses the save inside the store, with nothing written or reconciled", async () => {

    await save((current) => { current.server = { port: 6000 }; });

    const loaded = indexModule.getLoadedConfiguration();
    const running = indexModule.CONFIG;

    assert.equal(loaded.server.port, 6000, "precondition: the loaded snapshot sits apart from the defaults");

    armedFailure = "parse";

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

    armedFailure = "read";

    await assert.rejects(save((current) => { current.playback = { stallThreshold: 0.2 }; }), { message: READ_FAILURE_MESSAGE });

    assert.equal(indexModule.getLoadedConfiguration(), loaded, "the loaded snapshot is the same object");
    assert.equal(indexModule.CONFIG, running, "CONFIG is the same object");
    assert.equal(indexModule.CONFIG.playback.stallThreshold, 0.1);
  });
});

/* Reconciles are serialized on a module-level queue, so two overlapping saves cannot interleave into a state where the reconcile holding the older candidate
 * commits last. The first row holds the first save's handler open while the second save is issued, which is the shape a slow handler and a quick second save
 * produce.
 */
describe("saveConfiguration - serialized saves", () => {

  test("overlapping saves commit in the order their writes landed, leaving CONFIG on the newest value", async () => {

    const entered = Promise.withResolvers<null>();
    const gate = Promise.withResolvers<null>();
    let calls = 0;

    registerConfigChangeHandler("playback.", async () => {

      calls++;

      if(calls === 1) {

        entered.resolve(null);
        await gate.promise;
      }

      return [];
    });

    const older = save((current) => { current.playback = { stallThreshold: 0.2 }; });
    const newer = save((current) => { current.playback = { stallThreshold: 0.3 }; });

    // Hold the first save's handler open until the second save has had every turn it needs to run to completion if nothing serialized it. Unserialized, the
    // second reconcile would commit 0.3 here and the first would then overwrite it with its older 0.2 once released.
    await entered.promise;
    await flushMicrotasks(100);

    gate.resolve(null);

    await older;
    await newer;

    assert.equal(indexModule.CONFIG.playback.stallThreshold, 0.3, "the running configuration ends on the later save's value");
  });

  test("a refused save rejects its own caller and leaves the next save free to complete", async () => {

    const refused = save((current) => { current.server = { port: 0 }; });
    const completed = save((current) => { current.playback = { stallThreshold: 0.2 }; });

    await assert.rejects(refused, indexModule.ConfigurationRejectedError);

    const result = await completed;

    assert.deepEqual(paths(result.applied), ["playback.stallThreshold"]);
    assert.equal(file.server, undefined, "the refused mutation never reached the file");
  });
});

describe("saveConfiguration - the loaded snapshot moves with the commit", () => {

  test("a handler reads the snapshot from before the save and a gap without the path it is realizing, and the snapshot moves once the commit lands",
    async () => {

      const dispatchedPath = "playback.stallThreshold";
      const before = indexModule.getLoadedConfiguration();
      let seen: Nullable<{ inHeld: boolean; inLive: boolean; inNextStream: boolean; loadedUnchanged: boolean }> = null;

      // The handler records what a reader outside the reconcile queue sees while the change is being dispatched. It asserts nothing itself, because a throw
      // inside a handler is a rejection of its bucket rather than a failed test.
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
});

describe("the boot warning and the outcome line", () => {

  const PARSE_WARNING = "The configuration file is not valid JSON and has no usable backup, so the configuration starts from the defaults and saves are refused " +
    "until it is.";
  const READ_WARNING = "The configuration file could not be read, so the configuration starts from the defaults and saves are refused until it can be.";

  test("a store that reports a read failure logs the read-failure warning once and no parse-failure warning, and the boot starts from the defaults",
    async (t) => {

      const warn = t.mock.method(LOG, "warn", () => undefined);

      file = { server: { port: 6000 } };
      armedFailure = "read";
      await indexModule.initializeConfiguration(undefined, io);

      const messages = warn.mock.calls.map((call) => call.arguments[0]);

      assert.equal(messages.filter((message) => message === READ_WARNING).length, 1, "the read failure is named once");
      assert.equal(messages.filter((message) => message === PARSE_WARNING).length, 0, "no parse failure is reported");
      assert.equal(indexModule.CONFIG.server.port, 5589, "the configuration starts from the defaults");
    });

  test("a store that reports a parse failure logs the parse-failure warning once and no read-failure warning, and the boot starts from the defaults",
    async (t) => {

      const warn = t.mock.method(LOG, "warn", () => undefined);

      file = { server: { port: 6000 } };
      armedFailure = "parse";
      await indexModule.initializeConfiguration(undefined, io);

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

describe("startup coercion and persistCoercedConfig", () => {

  test("the loaded snapshot follows the startup coercion, so a save that changes nothing reports nothing", async () => {

    // The opening save shows that this row's saves report what they change, so the empty outcome at the end is a reading rather than a default.
    const opening = await save((current) => { current.playback = { stallThreshold: 0.2 }; });

    assert.deepEqual(paths(opening.applied), ["playback.stallThreshold"], "precondition: a save reports the setting it changed");

    file = { streaming: { captureMode: "native" } };
    await indexModule.initializeConfiguration(undefined, io);

    assert.equal(indexModule.getLoadedConfiguration().streaming.captureMode, "native", "precondition: the snapshot was loaded from the native file");

    indexModule.validateConfiguration();

    assert.equal(indexModule.CONFIG.streaming.captureMode, "ffmpeg");
    assert.equal(indexModule.getLoadedConfiguration().streaming.captureMode, "ffmpeg", "the loaded snapshot holds the coerced value");

    await indexModule.persistCoercedConfig(io);

    const result = await save(() => { /* A save that changes nothing. */ });

    assert.deepEqual(result, { applied: [], deferred: [], nextStream: [], rejected: [] });
  });

  test("writes the coerced capture settings to disk when validateConfiguration coerced a value", async () => {

    file = { streaming: { captureMode: "native" } };
    await indexModule.initializeConfiguration(undefined, io);
    indexModule.validateConfiguration();

    await indexModule.persistCoercedConfig(io);

    assert.equal(writes, 1, "the coercion is written back to disk");
    assert.equal(file.streaming?.captureMode, "ffmpeg", "the persisted capture mode is the coerced ffmpeg value");
  });

  test("does not write to disk when no coercion was needed", async () => {

    indexModule.validateConfiguration();
    await indexModule.persistCoercedConfig(io);

    assert.equal(writes, 0, "no write-back without a coercion");
  });
});
