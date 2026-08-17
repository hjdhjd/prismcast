/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * providerLineups.test.ts: Unit tests for the persisted provider lineup store. The module owns one in-memory map backed by provider-lineups.json, and its whole
 * value rests on four behaviors: a write replaces exactly one provider's slice and nothing else, an empty walk never reaches the file, a discredited watch URL is
 * dropped without losing the channel it belonged to, and neither write path can fail the caller that produced it.
 *
 * The store is created at module load against the production path resolver, so these tests point that resolver at a temp directory and exercise the real file
 * store end to end - the same shape health.test.ts uses for its own module-level store. Failure injection is a data directory whose path is occupied by a regular
 * file, which makes the framework's directory creation and its atomic write fail on the real filesystem without any monkey-patching.
 */
import { describe, test } from "node:test";
import { evictPersistedWatchUrl, getPersistedLineup, getPersistedWatchUrl, loadProviderLineups, persistProviderLineup } from "./providerLineups.ts";
import { getProviderLineupsFilePath, initializeDataDir } from "./paths.ts";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { LOG } from "../utils/index.ts";
import assert from "node:assert/strict";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

// The on-disk envelope, as the assertions below read it back.
interface OnDiskLineups {

  providers: Record<string, { channelSelector: string; name: string; watchUrl?: string }[]>;
  schemaVersion: number;
}

/**
 * Reads provider-lineups.json from the currently-pointed data directory.
 * @returns The parsed file contents.
 */
async function readLineupsFile(): Promise<OnDiskLineups> {

  return JSON.parse(await readFile(getProviderLineupsFilePath(), "utf8")) as OnDiskLineups;
}

/**
 * Waits for the store's serialized write queue to drain. The file store runs one mutation at a time, and persistProviderLineup awaits its own, so awaiting a write
 * issued after a fire-and-forget one is a positive signal that the earlier write has finished - which is what keeps a test's temp directory from being removed
 * out from under an eviction's in-flight write.
 * @returns A promise that resolves once every write issued before this call has settled.
 */
async function settleStoreWrites(): Promise<void> {

  await persistProviderLineup("queue-drain-marker", [{ channelSelector: "Marker", name: "Marker" }]);
}

/**
 * Points the production data-dir resolver at a path occupied by a regular file, so every store write fails on the real filesystem. Directory creation fails with
 * EEXIST and the atomic write fails with ENOTDIR, so the injection holds whichever of the two the store reaches first - it does not depend on whether an earlier
 * write in this process already created its data directory.
 * @param dir - The temp directory to place the blocking file in.
 * @returns A promise that resolves once the resolver points at the unusable path.
 */
async function pointAtUnwritableDataDir(dir: string): Promise<void> {

  const blocked = path.join(dir, "blocked");

  await writeFile(blocked, "This path is a file, so every write beneath it fails.", "utf8");

  initializeDataDir(blocked);
}

describe("persistProviderLineup", () => {

  test("writes a provider's lineup and serves it back through every accessor", async () => {

    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadProviderLineups();
      await persistProviderLineup("hbomax", [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/hbo" }]);

      assert.deepEqual(getPersistedLineup("hbomax"), [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/hbo" }]);
      assert.equal(getPersistedWatchUrl("hbomax", "HBO"), "https://play.hbomax.test/channel/watch/hbo");
      assert.equal(getPersistedLineup("never-persisted"), null, "an unknown slug reads as absent rather than empty");
      assert.equal(getPersistedWatchUrl("never-persisted", "HBO"), null, "an unknown slug has no watch URL");
    });
  });

  test("stamps the schema version into the file on the first write", async () => {

    /* The version marker has to be real on disk from the first write, because the file store framework's migration runner reads it to decide what to apply. A
     * store that declared currentSchemaVersion without the rest of the quartet would write no marker at all and leave a future migration with nothing to key on.
     */
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadProviderLineups();
      await persistProviderLineup("yttv", [{ channelSelector: "CNN", name: "CNN", watchUrl: "https://tv.youtube.test/watch/cnn" }]);

      assert.equal((await readLineupsFile()).schemaVersion, 1, "the first write carries the current schema version");
    });
  });

  test("replaces only the named provider's slice, leaving every other provider untouched", async () => {

    // The pin that catches a whole-record overwrite: two providers in one file, one of them rewritten. A write that replaced the envelope rather than the slice
    // would silently erase the other provider's lineup, and the failure would only surface on a later boot that needed it.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadProviderLineups();
      await persistProviderLineup("spectrum", [{ channelSelector: "ESPN", name: "ESPN", watchUrl: "https://watch.spectrum.test/livetv?tmsid=1" }]);
      await persistProviderLineup("yttv", [{ channelSelector: "CNN", name: "CNN", watchUrl: "https://tv.youtube.test/watch/cnn" }]);
      await persistProviderLineup("yttv", [{ channelSelector: "ESPN", name: "ESPN", watchUrl: "https://tv.youtube.test/watch/espn" }]);

      const onDisk = await readLineupsFile();

      assert.deepEqual(onDisk.providers["spectrum"], [{ channelSelector: "ESPN", name: "ESPN", watchUrl: "https://watch.spectrum.test/livetv?tmsid=1" }],
        "the untouched provider's slice survives a sibling's rewrite");
      assert.deepEqual(onDisk.providers["yttv"], [{ channelSelector: "ESPN", name: "ESPN", watchUrl: "https://tv.youtube.test/watch/espn" }],
        "the rewritten provider's slice is replaced wholesale rather than merged");
      assert.equal(getPersistedWatchUrl("yttv", "CNN"), null, "a channel the new lineup no longer carries is gone from memory too");
    });
  });

  test("treats an empty lineup as a no-op rather than an erasure", async () => {

    // The zero-channel walk this whole feature exists to survive must never be the thing that destroys the hints. A guard here and a guard at the recorder both
    // have to hold, because either one alone would let some future caller through.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadProviderLineups();
      await persistProviderLineup("hbomax", [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/hbo" }]);
      await persistProviderLineup("hbomax", []);

      assert.equal(getPersistedWatchUrl("hbomax", "HBO"), "https://play.hbomax.test/channel/watch/hbo", "the populated slice survives an empty walk");
      assert.deepEqual((await readLineupsFile()).providers["hbomax"]?.length, 1, "the empty walk never reached the file either");
    });
  });

  test("matches the channel selector case-insensitively", async () => {

    // Every provider's live cache keys on a lowercased selector, and the channel records users edit carry whatever case the provider's guide displayed. The
    // persisted lookup has to agree with both or a hint written under one spelling is invisible to the tune that needs it.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadProviderLineups();
      await persistProviderLineup("hbomax", [{ channelSelector: "HBO Hits", name: "HBO Hits", watchUrl: "https://play.hbomax.test/channel/watch/hits" }]);

      assert.equal(getPersistedWatchUrl("hbomax", "hbo hits"), "https://play.hbomax.test/channel/watch/hits", "a lowercased selector finds the entry");
      assert.equal(getPersistedWatchUrl("hbomax", "HBO HITS"), "https://play.hbomax.test/channel/watch/hits", "an uppercased selector finds the same entry");
    });
  });

  test("warns and resolves when the store write fails", async (t) => {

    /* A lineup write is the durable half of a discovery that already succeeded. If it could reject, a full disk would fail the walk that produced it, the precache
     * accounting that counted it, and the discovery endpoint's response - so the containment is the feature's own precondition, not defensive padding.
     */
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });

    await withTempDir(async (dir) => {

      await pointAtUnwritableDataDir(dir);

      await assert.doesNotReject(async () => persistProviderLineup("unwritable-case", [{ channelSelector: "HBO", name: "HBO" }]),
        "a failing store write never rejects into the caller");

      assert.ok(warn.mock.calls.some((call) => String(call.arguments[0]).includes("Failed to persist the channel lineup")),
        "the failure is reported rather than swallowed silently");
      assert.deepEqual(getPersistedLineup("unwritable-case"), [{ channelSelector: "HBO", name: "HBO" }],
        "the in-memory lineup still reflects what the walk found, so this session's tunes can use it");
    });
  });
});

describe("loadProviderLineups", () => {

  test("hydrates the in-memory lineups from an existing file", async () => {

    // The cold-boot path the whole feature turns on: a file written by an earlier session is what the first tune of this one reads.
    await withTempDir(async (dir) => {

      const dataDir = path.join(dir, "data");

      await mkdir(dataDir, { recursive: true });

      initializeDataDir(dataDir);

      await writeFile(getProviderLineupsFilePath(), JSON.stringify({

        providers: { hbomax: [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/prior-session" }] },
        schemaVersion: 1
      }), "utf8");

      await loadProviderLineups();

      assert.equal(getPersistedWatchUrl("hbomax", "HBO"), "https://play.hbomax.test/channel/watch/prior-session",
        "the prior session's watch URL is available before this session has walked a guide");
    });
  });

  test("replaces the in-memory lineups rather than merging into them", async () => {

    // A load is a statement about what the file holds. Merging would let a provider dropped from the file linger in memory for the life of the process.
    await withTempDir(async (dir) => {

      const dataDir = path.join(dir, "data");

      await mkdir(dataDir, { recursive: true });

      initializeDataDir(dataDir);

      await persistProviderLineup("stale-provider", [{ channelSelector: "HBO", name: "HBO" }]);

      await writeFile(getProviderLineupsFilePath(), JSON.stringify({ providers: {}, schemaVersion: 1 }), "utf8");

      await loadProviderLineups();

      assert.equal(getPersistedLineup("stale-provider"), null, "a provider absent from the file is absent from memory after a load");
    });
  });

  test("starts empty when no file exists yet", async () => {

    // First run. The file store returns its defaults on ENOENT, so the load is a clean no-op rather than an error path.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await persistProviderLineup("first-run-case", [{ channelSelector: "HBO", name: "HBO" }]);

      await withTempDir(async (freshDir) => {

        initializeDataDir(freshDir);

        await assert.doesNotReject(async () => loadProviderLineups(), "a missing file loads cleanly");
        assert.equal(getPersistedLineup("first-run-case"), null, "nothing carries over from the prior data directory");
      });
    });
  });
});

describe("evictPersistedWatchUrl", () => {

  test("drops the discredited URL, keeps its channel, and leaves its siblings alone", async () => {

    /* Eviction is what keeps a stale hint from costing more than one tune, and dropping the whole row instead of the URL would quietly shrink the lineup every
     * time a channel failed - the datalist would lose channels the provider still carries.
     */
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadProviderLineups();
      await persistProviderLineup("hbomax", [

        { channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/hbo" },
        { channelSelector: "HBO Hits", name: "HBO Hits", watchUrl: "https://play.hbomax.test/channel/watch/hits" }
      ]);

      evictPersistedWatchUrl("hbomax", "hbo");

      // What the lineup has to look like afterwards, in memory and on disk alike: the evicted channel reduced to its identity, its sibling untouched.
      const expected = [ { channelSelector: "HBO", name: "HBO" },
        { channelSelector: "HBO Hits", name: "HBO Hits", watchUrl: "https://play.hbomax.test/channel/watch/hits" } ];

      assert.equal(getPersistedWatchUrl("hbomax", "HBO"), null, "the discredited URL is gone");
      assert.equal(getPersistedWatchUrl("hbomax", "HBO Hits"), "https://play.hbomax.test/channel/watch/hits", "the sibling channel keeps its URL");
      assert.deepEqual(getPersistedLineup("hbomax"), expected, "the evicted channel keeps its identity row");

      await settleStoreWrites();

      assert.deepEqual((await readLineupsFile()).providers["hbomax"], expected,
        "the eviction reached the file, so the next boot does not resurrect the discredited URL");
    });
  });

  test("is a no-op for an unknown provider, an unknown channel, and a channel with no URL", async () => {

    // The eviction runs from a tune-failure path that knows nothing about what the store holds, so every miss has to be silent rather than exceptional.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadProviderLineups();
      await persistProviderLineup("hulu", [{ channelSelector: "CNN", name: "CNN" }]);

      assert.doesNotThrow(() => {

        evictPersistedWatchUrl("never-persisted", "CNN");
        evictPersistedWatchUrl("hulu", "not-a-channel");
        evictPersistedWatchUrl("hulu", "CNN");
      }, "every miss is silent");

      assert.deepEqual(getPersistedLineup("hulu"), [{ channelSelector: "CNN", name: "CNN" }], "an identity-only lineup is unchanged by an eviction");
    });
  });

  test("never throws when the store write behind it fails", async () => {

    // The eviction's write is fire-and-forget on a failure path. A rejection escaping it would surface as an unhandled rejection rather than as a failed tune.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      await loadProviderLineups();
      await persistProviderLineup("unwritable-evict", [{ channelSelector: "HBO", name: "HBO", watchUrl: "https://play.hbomax.test/channel/watch/hbo" }]);

      await pointAtUnwritableDataDir(dir);

      assert.doesNotThrow(() => evictPersistedWatchUrl("unwritable-evict", "HBO"), "the synchronous eviction never throws");
      assert.equal(getPersistedWatchUrl("unwritable-evict", "HBO"), null, "the in-memory hint is gone regardless of the write's fate");

      // Drain the queue inside the test, so a rejection escaping the fire-and-forget write's internal catch surfaces here rather than after the suite has moved on.
      await settleStoreWrites();
    });
  });
});
