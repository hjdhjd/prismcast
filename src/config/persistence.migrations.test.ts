/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.migrations.test.ts: Tests for the file-store framework's declarative migration runner. The runner walks pending migrations from the file's
 * current schema version up to the store's currentSchemaVersion, mutating data in memory and recording each application via setSchemaVersion / recordMigration.
 *
 * The split-by-concern coverage owned here:
 *   - Forward run (file at v1, current at v2): one migration applied, schema stamped.
 *   - Forward-compatible read (file at v99, current at v1): no migrations attempted; data passes through.
 *   - No-op middle path (file already at currentSchemaVersion with migrations declared): zero migrations applied.
 *   - Migration-map gap (file at v1, current at v3, no v2 migration): runner throws.
 *   - ensureMigrated empty when current; ensureMigrated persists upgrade when stale.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { readFile, writeFile } from "node:fs/promises";
import type { LogEntry } from "../utils/logEmitter.ts";
import type { Migration } from "./persistence.ts";
import assert from "node:assert/strict";
import { makeStore } from "./persistence.helpers.ts";
import path from "node:path";
import { subscribeToLogs } from "../utils/logEmitter.ts";
import { withTempDir } from "../testing.helpers.ts";

/* Every log entry emitted for the duration of a test. The row that asserts how often an upgrade is announced reads it the way an operator reads the Logs tab -
 * by level and message - rather than by swapping the logger, which is the observation shape persistence.integrity.test.ts establishes for this suite. Reset per
 * test so one test's framework output cannot leak into another's count.
 */
let captured: LogEntry[];

let unsubscribe: () => void;

beforeEach(() => {

  captured = [];
  unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
});

afterEach(() => {

  unsubscribe();
});

describe("FileStore.read - migrations", () => {

  test("runs migrations in memory and returns the upgraded data", async () => {

    await withTempDir(async (dir) => {

      // File is at v1; current is v2 with one migration that adds a field.
      await writeFile(path.join(dir, "migrate.json"), JSON.stringify({ schemaVersion: 1 }));

      const migrations: Record<number, Migration<{ migratedField?: string; schemaVersion?: number }>> = {

        2: {

          apply: (data): void => { data.migratedField = "applied"; },
          description: "add migratedField"
        }
      };
      const store = makeStore<{ migratedField?: string; schemaVersion?: number }>(dir, "migrate.json", {

        currentSchemaVersion: 2,
        defaultValue: () => ({ schemaVersion: 2 }),
        migrations
      });
      const result = await store.read();

      assert.equal(result.data.migratedField, "applied");
      assert.equal(result.data.schemaVersion, 2);
      assert.equal(result.migrationResult.fromVersion, 1);
      assert.equal(result.migrationResult.toVersion, 2);
      assert.deepEqual(result.migrationResult.applied, ["add migratedField"]);
    });
  });

  test("throws when the migrations map has a gap (programmer error)", async () => {

    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "gap.json"), JSON.stringify({ schemaVersion: 1 }));

      const migrations: Record<number, Migration<{ schemaVersion?: number }>> = {

        // No v2 migration; v3 declared. The runner walks 2,3,... and surfaces the missing v2.
        3: { apply: () => undefined, description: "v3" }
      };
      const store = makeStore<{ schemaVersion?: number }>(dir, "gap.json", {

        currentSchemaVersion: 3,
        defaultValue: () => ({ schemaVersion: 3 }),
        migrations
      });

      await assert.rejects(() => store.read(), /missing a migration to schema version 2/);
    });
  });

  test("forward-compatible read: file with newer version logs and proceeds without migrations", async () => {

    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "newer.json"), JSON.stringify({ futureField: "x", schemaVersion: 99 }));

      const store = makeStore<{ futureField?: string; schemaVersion?: number }>(dir, "newer.json", {

        currentSchemaVersion: 1,
        defaultValue: () => ({ schemaVersion: 1 })
      });
      const result = await store.read();

      assert.equal(result.data.schemaVersion, 99, "newer version preserved");
      assert.equal(result.data.futureField, "x");
      assert.deepEqual(result.migrationResult.applied, [], "no migrations applied");
    });
  });

  test("no-op middle path: file already at currentSchemaVersion with migrations declared applies zero migrations", async () => {

    /* This asserts the gap between "no migrations declared" (an unversioned store - the runner returns early via the !options.migrations guard) and the
     * forward-compatible newer-than-current case. The middle path is when migrations ARE declared, the file IS versioned, and the file's version equals (not
     * exceeds) currentSchemaVersion. The while-loop's condition (currentVersion < options.currentSchemaVersion) immediately fails, no migrations apply, and
     * the result reports applied=[] with fromVersion === toVersion === the current schema version.
     *
     * The upgrade test keeps the file below currentSchemaVersion and the forward-compatible test declares no migrations at all, so neither exercises the exact
     * boundary where the file's version equals currentSchemaVersion with a migration declared at that version. This test is the one that directly asserts the no-op:
     * a migration keyed at the current version is not re-executed once the file already sits there, and the result reports a clean pass-through (applied empty,
     * fromVersion === toVersion).
     */
    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "current.json"), JSON.stringify({ schemaVersion: 2, value: 42 }));

      let migrationApplied = false;
      const migrations: Record<number, Migration<{ schemaVersion?: number; value?: number }>> = {

        2: {

          apply: (): void => { migrationApplied = true; },
          description: "v2 upgrade"
        }
      };
      const store = makeStore<{ schemaVersion?: number; value?: number }>(dir, "current.json", {

        currentSchemaVersion: 2,
        defaultValue: () => ({ schemaVersion: 2 }),
        migrations
      });
      const result = await store.read();

      assert.equal(migrationApplied, false, "the v2 migration body MUST NOT run when the file is already at v2");
      assert.deepEqual(result.migrationResult.applied, [], "applied is empty when no migrations needed");
      assert.equal(result.migrationResult.fromVersion, 2, "fromVersion equals the file's schemaVersion");
      assert.equal(result.migrationResult.toVersion, 2, "toVersion equals fromVersion when nothing was applied");
      assert.equal(result.data.value, 42, "data passes through unchanged");
    });
  });
});

describe("FileStore.ensureMigrated", () => {

  test("returns empty migration result when file is already at current schema version", async () => {

    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "ok.json"), JSON.stringify({ schemaVersion: 1, value: 1 }));

      const store = makeStore<{ schemaVersion?: number; value: number }>(dir, "ok.json", {

        currentSchemaVersion: 1,
        defaultValue: () => ({ schemaVersion: 1, value: 0 })
      });

      const result = await store.ensureMigrated();

      assert.deepEqual(result.applied, [], "no migrations applied since already current");
    });
  });

  test("persists migration upgrade when file was at older version", async () => {

    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "upgrade.json"), JSON.stringify({ schemaVersion: 1 }));

      const migrations: Record<number, Migration<{ schemaVersion?: number; upgraded?: boolean }>> = {

        2: {

          apply: (data): void => { data.upgraded = true; },
          description: "v2 upgrade"
        }
      };
      const store = makeStore<{ schemaVersion?: number; upgraded?: boolean }>(dir, "upgrade.json", {

        currentSchemaVersion: 2,
        defaultValue: () => ({ schemaVersion: 2 }),
        migrations
      });

      const result = await store.ensureMigrated();

      assert.deepEqual(result.applied, ["v2 upgrade"]);

      // Re-read the file from disk to confirm the upgrade was persisted.
      const written = JSON.parse(await readFile(path.join(dir, "upgrade.json"), "utf-8")) as { schemaVersion?: number; upgraded?: boolean };

      assert.equal(written.schemaVersion, 2, "upgraded version persisted");
      assert.equal(written.upgraded, true, "migration body persisted");
    });
  });

  test("an upgrade the boot step persists is announced once, not once per read", async () => {

    /* ensureMigrated reads twice on a boot that has an upgrade to persist: once to decide, and once inside the queued write that performs it. The migration
     * line describes the upgrade that landed on disk, so it belongs to the write's read alone, and an upgrade boot shows one line per upgraded store rather
     * than a pair for every one of them.
     */
    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "announce-once.json"), JSON.stringify({ schemaVersion: 1 }));

      const migrations: Record<number, Migration<{ schemaVersion?: number; upgraded?: boolean }>> = {

        2: {

          apply: (data): void => { data.upgraded = true; },
          description: "v2 upgrade"
        }
      };
      const store = makeStore<{ schemaVersion?: number; upgraded?: boolean }>(dir, "announce-once.json", {

        currentSchemaVersion: 2,
        defaultValue: () => ({ schemaVersion: 2 }),
        migrations
      });

      await store.ensureMigrated();

      const migrated = captured.filter((line) => (line.level === "info") && /Migrated .* schema/.test(line.message));

      assert.equal(migrated.length, 1, "one upgrade, one line, across both of the boot step's reads");
      assert.match(migrated[0]?.message ?? "", /test-announce-once\.json/, "and it names the store whose file was upgraded");

      const written = JSON.parse(await readFile(path.join(dir, "announce-once.json"), "utf-8")) as { schemaVersion?: number };

      assert.equal(written.schemaVersion, 2, "the upgrade the line describes is the one that landed on disk");
    });
  });
});
