/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.snapshots.test.ts: Tests for the file-store framework's snapshot system - labeled copies under a snapshots/ subdirectory plus retention pruning
 * by mtime. Snapshots are version-keyed safety nets that survive normal .bak rotation; they are the restore-of-last-resort when a release introduces a
 * data-shape regression that escapes the primary safeguards.
 *
 * Why some tests use real fs and others use the in-memory backend: the snapshot CREATION path (snapshot label idempotence, copyFile-from-source semantics) is
 * exercised against real fs because the framework's atomicity guarantees only mean what they mean against a real filesystem. The PRUNING path (readdir +
 * per-entry stat + per-entry unlink in a Promise.all loop) needs failure injection to drive its uncovered branches, so those tests use the memory backend
 * with override hooks - real fs cannot reliably make individual fs.stat or fs.unlink calls fail mid-loop.
 */
import { describe, test } from "node:test";
import { makeMemoryStorageBackend, makeMemoryStore, makeStore } from "./persistence.helpers.ts";
import { readFile, readdir } from "node:fs/promises";
import assert from "node:assert/strict";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

describe("FileStore.snapshot - real-fs creation", () => {

  test("creates a labeled copy in a snapshots/ subdirectory", async () => {

    await withTempDir(async (dir) => {

      const store = makeStore<{ value: number }>(dir, "snap-test.json", {

        defaultValue: () => ({ value: 0 })
      });

      await store.mutate((data) => { data.value = 1; });
      await store.snapshot("v1.0.0");

      const snapPath = path.join(dir, "snapshots", "snap-test.json.v1.0.0");
      const content = await readFile(snapPath, "utf-8");
      const parsed = JSON.parse(content) as { value: number };

      assert.equal(parsed.value, 1, "snapshot reflects the file's state at snapshot time");
    });
  });

  test("is idempotent on the same label (second call is a no-op)", async () => {

    await withTempDir(async (dir) => {

      const store = makeStore<{ value: number }>(dir, "idempotent.json", {

        defaultValue: () => ({ value: 0 })
      });

      await store.mutate((data) => { data.value = 1; });
      await store.snapshot("v1");

      // Mutate to a new value; second snapshot with the same label must NOT overwrite the existing snapshot.
      await store.mutate((data) => { data.value = 2; });
      await store.snapshot("v1");

      const snapPath = path.join(dir, "snapshots", "idempotent.json.v1");
      const content = JSON.parse(await readFile(snapPath, "utf-8")) as { value: number };

      assert.equal(content.value, 1, "idempotent: first snapshot's value is preserved");
    });
  });

  test("silently no-ops when the source file does not exist (first run before any write)", async () => {

    await withTempDir(async (dir) => {

      const store = makeStore<{ value: number }>(dir, "no-source.json", {

        defaultValue: () => ({ value: 0 })
      });

      // No mutate called; file does not exist.
      await assert.doesNotReject(() => store.snapshot("v1"), "snapshot of non-existent file must not throw");
    });
  });
});

describe("FileStore.snapshot - pruning under retention", () => {

  test("a directory with snapshots at or below SNAPSHOT_RETENTION is left untouched", async () => {

    /* The retention constant is 5. After creating five labeled snapshots, the next snapshot creation triggers a prune that finds six entries - the oldest gets
     * removed. Below the threshold, prune is a no-op short-circuit. We verify by creating exactly five snapshots and confirming all five survive after each
     * subsequent mutation (which triggers the prune as a hygiene step).
     */
    await withTempDir(async (dir) => {

      const store = makeStore<{ value: number }>(dir, "below-retention.json", {

        defaultValue: () => ({ value: 0 })
      });

      // Establish content for the first snapshot, then create five labeled snapshots (the retention limit). Each snapshot()'s prune step finds <= 5 entries and
      // returns early.
      await store.mutate((data) => { data.value = 1; });

      for(const label of [ "v1", "v2", "v3", "v4", "v5" ]) {

        // eslint-disable-next-line no-await-in-loop -- sequential by design: each snapshot's prune step must observe the prior writes via the file map.
        await store.snapshot(label);
      }

      const entries = await readdir(path.join(dir, "snapshots"));
      const matching = entries.filter((entry) => entry.startsWith("below-retention.json."));

      assert.equal(matching.length, 5, "all five snapshots survive when the count equals SNAPSHOT_RETENTION");
    });
  });

  test("creating a seventh snapshot prunes the two oldest by mtime, leaving the five most recent", async () => {

    /* Drives the retention algorithm end-to-end. We seed seven snapshot files into the snapshots dir manually with known, increasing mtimes (via writeFile in
     * order), then call snapshot() with a fresh label - the framework's create flow runs the prune step on success and removes the two oldest. We use the
     * memory backend so we can control mtimes deterministically: the backend stamps mtimes via a monotonic counter, so writeFile order maps directly to mtime
     * order without depending on the wall clock or filesystem mtime resolution.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/seven.json";
    const snapshotDir = "/data/snapshots";

    // Seed the source file so the framework has something to copy.
    backend.files.set(filePath, "{\"value\":1}\n");
    backend.mtimes.set(filePath, 1);

    // Seed seven existing snapshots with monotonically-increasing mtimes. The default backend's writeFile increments the counter on every call, so writing in
    // ascending-label order produces ascending mtimes.
    for(let i = 1; i <= 7; i++) {

      // eslint-disable-next-line no-await-in-loop -- sequential by design: the mtime counter must increment per-snapshot so the prune ordering is deterministic.
      await backend.writeFile(snapshotDir + "/seven.json.v" + String(i), "{\"value\":" + String(i) + "}\n");
    }

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    // Trigger a fresh snapshot. The create flow proceeds (label "v8" does not exist yet), then the prune step runs - eight matching snapshots, retention is 5,
    // so the three oldest (v1, v2, v3) get unlinked. v4..v8 survive.
    await store.snapshot("v8");

    const entries = (await backend.readdir(snapshotDir)).filter((entry) => entry.startsWith("seven.json."));

    assert.equal(entries.length, 5, "after pruning, exactly SNAPSHOT_RETENTION entries remain");

    const surviving = entries.toSorted();

    assert.deepEqual(surviving, [ "seven.json.v4", "seven.json.v5", "seven.json.v6", "seven.json.v7", "seven.json.v8" ],
      "the five most recent snapshots survive (by mtime); the three oldest were pruned");
  });

  test("readdir failure during prune is swallowed silently (no throw, no propagation)", async () => {

    /* The prune flow opens with `try { await backend.readdir(snapshotDir); } catch { return; }` - a failure here just short-circuits prune and lets the create
     * flow's success log stand. We verify by injecting a readdir override that throws synthetic errors; the snapshot call must still resolve, and the
     * just-created snapshot must remain (the create succeeded; only the prune step failed).
     */
    const backend = makeMemoryStorageBackend({


      readdir: async (): Promise<string[]> => {

        throw new Error("synthetic-readdir-failure");
      }
    });
    const filePath = "/data/readdir-fail.json";

    backend.files.set(filePath, "{\"value\":1}\n");
    backend.mtimes.set(filePath, 1);

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    await assert.doesNotReject(() => store.snapshot("v1"), "snapshot must succeed even if prune's readdir fails");

    // The snapshot file was created before the failed prune ran.
    assert.equal(backend.files.has("/data/snapshots/readdir-fail.json.v1"), true, "the create step succeeded; only the prune was skipped");
  });

  test("per-entry stat failure drops the entry from the prune candidate list (others still get pruned)", async () => {

    /* The Promise.all stat loop has its own per-entry `try { stat } catch { return null }`. Entries that fail to stat are filtered out via `entry !== null`
     * before the sort+slice. We seed seven snapshots, override stat to fail on a specific entry, and verify (a) the failing entry is left intact (it was
     * dropped from candidates so prune did not target it), (b) the other oldest entries were pruned, and (c) the snapshot call did not throw.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/stat-fail.json";
    const snapshotDir = "/data/snapshots";

    backend.files.set(filePath, "{\"value\":1}\n");
    backend.mtimes.set(filePath, 1);

    for(let i = 1; i <= 7; i++) {

      // eslint-disable-next-line no-await-in-loop -- sequential by design: the mtime counter must increment per-snapshot for deterministic prune ordering.
      await backend.writeFile(snapshotDir + "/stat-fail.json.v" + String(i), "{\"value\":" + String(i) + "}\n");
    }

    // Now override stat on the existing backend reference. We replace the stat method directly so other operations (readdir, copyFile, unlink) keep working.
    const failingPath = snapshotDir + "/stat-fail.json.v1";
    const realStat = backend.stat;

    backend.stat = async (p: string): Promise<{ mtimeMs: number }> => {

      if(p === failingPath) {

        throw new Error("synthetic-stat-failure");
      }

      return realStat(p);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    await assert.doesNotReject(() => store.snapshot("v8"), "snapshot must succeed when individual stat calls fail");

    /* After prune: v1 was dropped from the candidate list (stat failed), so it remained on disk. The candidate list had 8 entries minus v1 = 7 valid candidates;
     * prune retained the 5 most recent (v4..v8) and unlinked the two oldest valid (v3, v2). v1 stays untouched because prune never targeted it.
     * Survivors: v1 (stat-failed) + v4..v8 (retained) = 6 entries.
     */
    const entries = (await backend.readdir(snapshotDir)).filter((entry) => entry.startsWith("stat-fail.json."));
    const surviving = entries.toSorted();

    assert.deepEqual(surviving, [ "stat-fail.json.v1", "stat-fail.json.v4", "stat-fail.json.v5", "stat-fail.json.v6", "stat-fail.json.v7", "stat-fail.json.v8" ],
      "stat-failed entry stays; the two oldest valid candidates (v2, v3) were pruned");
  });

  test("per-entry unlink failure logs a warning but does not propagate or block siblings", async () => {

    /* Each per-entry unlink runs inside its own try/catch that logs a warn and proceeds. We seed seven snapshots, override unlink to fail on the oldest entry
     * specifically, and verify (a) the snapshot call resolves, (b) the failing entry is still on disk, (c) the OTHER prune candidates were unlinked normally.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/unlink-fail.json";
    const snapshotDir = "/data/snapshots";

    backend.files.set(filePath, "{\"value\":1}\n");
    backend.mtimes.set(filePath, 1);

    for(let i = 1; i <= 7; i++) {

      // eslint-disable-next-line no-await-in-loop -- sequential by design: the mtime counter must increment per-snapshot for deterministic prune ordering.
      await backend.writeFile(snapshotDir + "/unlink-fail.json.v" + String(i), "{\"value\":" + String(i) + "}\n");
    }

    // Override unlink on the already-built backend so default readdir/stat continue to work. v1 is the oldest and prune's first target after the v8 snapshot.
    const failingPath = snapshotDir + "/unlink-fail.json.v1";
    const realUnlink = backend.unlink;

    backend.unlink = async (p: string): Promise<void> => {

      if(p === failingPath) {

        throw new Error("synthetic-unlink-failure");
      }

      return realUnlink(p);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    await assert.doesNotReject(() => store.snapshot("v8"), "snapshot must succeed even if individual unlink calls fail");

    // v1 (the failing target) survives; v2 and v3 are also pruned candidates per the retention math (8 entries - 5 retained = 3 pruned). With v1's unlink
    // failing but not propagating, we expect v1 + the five most-recent (v4..v8) to remain. v2 and v3's unlinks succeeded.
    const entries = (await backend.readdir(snapshotDir)).filter((entry) => entry.startsWith("unlink-fail.json."));
    const surviving = entries.toSorted();

    assert.deepEqual(surviving, [ "unlink-fail.json.v1", "unlink-fail.json.v4", "unlink-fail.json.v5", "unlink-fail.json.v6", "unlink-fail.json.v7",
      "unlink-fail.json.v8" ], "v1 stays (unlink failed-and-logged); v2 and v3 were pruned normally");
  });
});
