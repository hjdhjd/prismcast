/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ordering.test.ts: Tests for the order of overlapping configuration writes, run on the real file store. Every write the configuration layer makes holds
 * the store's queue through its follow-up, a save's reconcile or a process write's commit, so no write reads the file while another write's reconcile or
 * commit is running. The contracts exercised:
 *
 *   1. Overlapping saves commit in the order their writes landed, and a refused save rejects its own caller and leaves the next save free to complete.
 *
 *   2. A write queued while a save's handler runs reads the file only once that save's reconcile has committed and moved the loaded snapshot.
 *
 *   3. Every loss path the configuration layer's operations close stays closed: a process write started beside a save, queued behind a waiting save, or landing
 *      on a leaf a save is dispatching, and overlapping display-preference writes, each leave the file and CONFIG holding every written value.
 *
 *   4. The predefined writer's enables and disables land as one write and one dispatch, and a predefined call with no key to change, like a display-preference
 *      call with no field, writes and dispatches nothing.
 *
 * The in-memory double of index.helpers.ts models no chain, so every row here runs on the real store in a temporary data directory the row initializes, and
 * boots through initializeConfiguration with the default store. The store's read, write and readback are file I/O that microtask turns never complete, so a
 * row that holds a gate while another write is pending waits a bounded real-time interval before it opens the gate or reads what ran, and every gate opens in a
 * finally, so a failed row leaves no later row waiting on the store's chain.
 */
import { CONFIG, ConfigurationRejectedError, getLoadedConfiguration, initializeConfiguration, saveConfiguration } from "./index.ts";
import { beforeEach, describe, test } from "node:test";
import { closePuppeteerStreamWssOnIdle, withTempDir } from "../testing.helpers.ts";
import { disablePredefinedChannels, markSetupCompleted, mutateChannelDisplayPrefs, updatePredefinedChannels } from "./userChannels.ts";
import { getConfigFilePath, initializeDataDir } from "./paths.ts";
import { mutateConfig, readConfig } from "./userConfig.ts";
import { registerConfigChangeHandler, resetConfigChangeHandlers } from "./reactivity.ts";
import type { Nullable } from "../types/index.ts";
import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import os from "node:os";
import { stat } from "node:fs/promises";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The real time a row gives a pending write to reach its callback were the store's queue free: the read, write and readback of a small file, with room to spare.
const FILE_IO_SETTLE_MS = 300;

/**
 * Runs a row in a data directory of its own: the configuration store reads and writes there, the configuration boots from that directory's empty file through
 * the default store, and the data directory points back at os.tmpdir(), a directory that exists, once the row ends and its directory is removed.
 * @param row - The row's body.
 */
async function inDataDir(row: () => Promise<void>): Promise<void> {

  await withTempDir(async (dir) => {

    initializeDataDir(dir);

    try {

      await initializeConfiguration();
      await row();
    } finally {

      initializeDataDir(os.tmpdir());
    }
  });
}

/**
 * Reads the inode of the configuration file. Every landed write renames a new temporary file over the file, so an inode that is the same after a call shows the
 * store wrote nothing; the boot of the row's data directory has created the file by then, because it writes the DeviceID it generates.
 * @returns The configuration file's inode.
 */
async function configFileInode(): Promise<number> {

  return (await stat(getConfigFilePath())).ino;
}

beforeEach(() => {

  resetConfigChangeHandlers();
});

describe("saveConfiguration - overlapping saves on the store's one queue", () => {

  test("overlapping saves commit in the order their writes landed, leaving CONFIG on the newest value", async () => {

    await inDataDir(async () => {

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

      const older = saveConfiguration((current) => { current.playback = { stallThreshold: 0.2 }; });
      const newer = saveConfiguration((current) => { current.playback = { stallThreshold: 0.3 }; });

      await entered.promise;

      // Hold the first save's handler open long enough for the second save's write and reconcile to run to completion were nothing serializing them. Unserialized,
      // the second reconcile would commit 0.3 here and the first would then overwrite it with its older 0.2 once released.
      try {

        await delay(FILE_IO_SETTLE_MS);
      } finally {

        gate.resolve(null);
      }

      await Promise.all([ older, newer ]);

      assert.equal(CONFIG.playback.stallThreshold, 0.3, "the running configuration ends on the later save's value");
    });
  });

  test("a refused save rejects its own caller and leaves the next save free to complete", async () => {

    await inDataDir(async () => {

      const refused = saveConfiguration((current) => { current.server = { port: 0 }; });
      const completed = saveConfiguration((current) => { current.playback = { stallThreshold: 0.2 }; });

      await assert.rejects(refused, ConfigurationRejectedError);

      const result = await completed;

      assert.deepEqual(result.applied.map((change) => change.path), ["playback.stallThreshold"]);
      assert.equal((await readConfig()).config.server, undefined, "the refused mutation never reached the file");
    });
  });

  test("a write queued while a save's handler runs reads the file only once that save's reconcile has finished", async () => {

    await inDataDir(async () => {

      const entered = Promise.withResolvers<null>();
      const gate = Promise.withResolvers<null>();
      let seen: Nullable<{ committed: number; loaded: number }> = null;
      let callbackRan = false;
      let ranWhileHeld = true;

      registerConfigChangeHandler("playback.", async () => {

        entered.resolve(null);
        await gate.promise;

        return [];
      });

      const saved = saveConfiguration((current) => { current.playback = { stallThreshold: 0.2 }; });

      await entered.promise;

      // The store-level write stands in for a writer outside the configuration layer, queued while the save's handler holds its reconcile open. Its callback
      // records what the save had committed by the time the write read the file.
      const written = mutateConfig((current) => {

        callbackRan = true;
        seen = { committed: CONFIG.playback.stallThreshold, loaded: getLoadedConfiguration().playback.stallThreshold };
        current.channelsDvr = { host: "dvr.example.test" };
      });

      try {

        await delay(FILE_IO_SETTLE_MS);
        ranWhileHeld = callbackRan;
      } finally {

        gate.resolve(null);
      }

      await Promise.all([ saved, written ]);

      assert.equal(ranWhileHeld, false, "the write's callback did not run while the save's handler held the reconcile");
      assert.deepEqual(seen, { committed: 0.2, loaded: 0.2 }, "the write read the file once the reconcile had committed the save and moved the snapshot");
      assert.equal((await readConfig()).config.channelsDvr?.host, "dvr.example.test", "the queued write landed");
    });
  });
});

/* One row per loss path the configuration layer's operations close (the design note's section "Wave 3 design"). Each starts its writes the way the path needs
 * them and asserts that the file and CONFIG hold every written value once the writes settle. The fifth path, the HDHomeRun handler's own device-id write, has
 * no row, because no handler writes the store.
 */
describe("the loss paths - every write lands in the file and in CONFIG", () => {

  test("path 1: a process write started in the same turn as a save, before the save's callback reads the file, survives the save's reconcile", async () => {

    await inDataDir(async () => {

      const saved = saveConfiguration((current) => { current.playback = { stallThreshold: 0.2 }; });
      const marked = markSetupCompleted();

      await Promise.all([ saved, marked ]);

      const file = (await readConfig()).config;

      assert.equal(file.channels?.setupCompleted, true, "the file holds the setup flag");
      assert.equal(CONFIG.channels.setupCompleted, true, "CONFIG holds the setup flag");
      assert.equal(file.playback?.stallThreshold, 0.2, "the file holds the saved stall threshold");
      assert.equal(CONFIG.playback.stallThreshold, 0.2, "CONFIG holds the saved stall threshold");
    });
  });

  test("path 2: a process write queued behind a save whose handler waits and a second save survives each save's reconcile", async () => {

    await inDataDir(async () => {

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

      const first = saveConfiguration((current) => { current.playback = { stallThreshold: 0.2 }; });

      await entered.promise;

      const second = saveConfiguration((current) => { current.playback = { stallThreshold: 0.3 }; });
      const disabled = disablePredefinedChannels(["abc"]);

      // Hold the first save's handler open long enough for the writes queued behind it to run to completion were nothing serializing them.
      try {

        await delay(FILE_IO_SETTLE_MS);
      } finally {

        gate.resolve(null);
      }

      await Promise.all([ first, second, disabled ]);

      const file = (await readConfig()).config;

      assert.deepEqual(file.channels?.disabledPredefined, ["abc"], "the file holds the disabled key");
      assert.deepEqual(CONFIG.channels.disabledPredefined, ["abc"], "CONFIG holds the disabled key");
      assert.equal(file.playback?.stallThreshold, 0.3, "the file holds the later save's value");
      assert.equal(CONFIG.playback.stallThreshold, 0.3, "CONFIG holds the later save's value");
    });
  });

  test("path 3: a process write of a leaf a save's dispatch is realizing waits for the dispatch and lands after it", async () => {

    await inDataDir(async () => {

      const entered = Promise.withResolvers<null>();
      const gate = Promise.withResolvers<null>();

      registerConfigChangeHandler("playback.", async () => {

        entered.resolve(null);
        await gate.promise;

        return [];
      });

      const saved = saveConfiguration((current) => {

        current.channels = { ...current.channels, channelSortField: "channelNumber" };
        current.playback = { stallThreshold: 0.2 };
      });

      await entered.promise;

      const sorted = mutateChannelDisplayPrefs({ channelSortField: "service" });

      try {

        await delay(FILE_IO_SETTLE_MS);
      } finally {

        gate.resolve(null);
      }

      await Promise.all([ saved, sorted ]);

      const file = (await readConfig()).config;

      assert.equal(file.channels?.channelSortField, "service", "the file holds the display write's sort field");
      assert.equal(CONFIG.channels.channelSortField, "service", "CONFIG holds the display write's sort field");
      assert.equal(file.playback?.stallThreshold, 0.2, "the file holds the save's stall threshold");
      assert.equal(CONFIG.playback.stallThreshold, 0.2, "CONFIG holds the save's stall threshold");
    });
  });

  test("path 4: overlapping display-preference writes setting different fields each land", async () => {

    await inDataDir(async () => {

      await Promise.all([ mutateChannelDisplayPrefs({ channelSortField: "service" }), mutateChannelDisplayPrefs({ channelSortDirection: "desc" }) ]);

      const file = (await readConfig()).config;

      assert.equal(file.channels?.channelSortField, "service", "the file holds the first write's field");
      assert.equal(file.channels.channelSortDirection, "desc", "the file holds the second write's field");
      assert.equal(CONFIG.channels.channelSortField, "service", "CONFIG holds the first write's field");
      assert.equal(CONFIG.channels.channelSortDirection, "desc", "CONFIG holds the second write's field");
    });
  });
});

describe("updatePredefinedChannels - one write of the disabled list", () => {

  /**
   * Registers a handler on the disabled list that counts its calls and refuses nothing.
   * @returns Reads how many times the handler has been called.
   */
  function countDisabledListDispatches(): () => number {

    let calls = 0;

    registerConfigChangeHandler("channels.disabledPredefined", async () => {

      calls++;

      return [];
    });

    return () => calls;
  }

  test("the enables are applied before the disables, so a key named in each list ends disabled, all in one write and one dispatch", async () => {

    await inDataDir(async () => {

      await disablePredefinedChannels(["nbc"]);

      const dispatches = countDisabledListDispatches();

      await updatePredefinedChannels({ disable: ["abc"], enable: [ "abc", "nbc" ] });

      assert.deepEqual((await readConfig()).config.channels?.disabledPredefined, ["abc"], "the file holds the key named in each list and not the key only enabled");
      assert.deepEqual(CONFIG.channels.disabledPredefined, ["abc"], "CONFIG holds the same list");
      assert.equal(dispatches(), 1, "the change was one write, dispatched once");
    });
  });

  test("a call with no key to enable or disable writes nothing and dispatches nothing", async () => {

    await inDataDir(async () => {

      const dispatches = countDisabledListDispatches();
      const inode = await configFileInode();

      await updatePredefinedChannels({ disable: [], enable: [] });

      assert.equal(await configFileInode(), inode, "the store wrote nothing");
      assert.equal(dispatches(), 0, "nothing was dispatched");
    });
  });
});

describe("mutateChannelDisplayPrefs - a call with no field", () => {

  test("a call that supplies no field writes nothing and dispatches nothing", async () => {

    await inDataDir(async () => {

      let dispatches = 0;

      registerConfigChangeHandler("channels.", async () => {

        dispatches++;

        return [];
      });

      const inode = await configFileInode();

      await mutateChannelDisplayPrefs({});

      assert.equal(await configFileInode(), inode, "the store wrote nothing");
      assert.equal(dispatches, 0, "nothing was dispatched");
    });
  });
});
