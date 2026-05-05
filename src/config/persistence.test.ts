/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.test.ts: Unit tests for the transactional file store framework. The framework is the SSOT for atomic writes, serialized mutations, declarative
 * schema migrations, post-write integrity verification, and snapshot management - every config file (channels, config, profiles, health) goes through it.
 * Tests construct ad-hoc stores against tmp-scoped paths so they exercise the full I/O pipeline without affecting any production-data file.
 */
import { type FileStore, FileStoreParseError, type Migration, type ValidationIssue, createFileStore } from "./persistence.ts";
import { describe, test } from "node:test";
import { readFile, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import { initializeDataDir } from "./paths.ts";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

// makeStore builds a FileStore in the supplied data dir. Tests pass a unique filename per test so concurrent stores don't collide on the global registry.
function makeStore<T>(dir: string, filename: string, options: {
  currentSchemaVersion?: number;
  defaultValue?: () => T;
  migrations?: Record<number, Migration<T>>;
  parse?: (raw: string) => T;
  validate?: (prev: T, next: T) => ValidationIssue[];
} = {}): FileStore<T> {

  // Make sure the data dir is set so persistence.ts can call getDataDir() during the first write.
  initializeDataDir(dir);

  return createFileStore<T>({

    currentSchemaVersion: options.currentSchemaVersion,
    defaultValue: options.defaultValue ?? ((): T => ({} as T)),
    getSchemaVersion: options.currentSchemaVersion ? ((data: T): number => (data as { schemaVersion?: number }).schemaVersion ?? 1) : undefined,
    label: "test-" + filename,
    migrations: options.migrations,
    parse: options.parse ?? ((raw: string): T => JSON.parse(raw) as T),
    path: (): string => path.join(dir, filename),
    setSchemaVersion: options.currentSchemaVersion ?
      ((data: T, version: number): void => { (data as { schemaVersion?: number }).schemaVersion = version; }) :
      undefined,
    validate: options.validate
  });
}

describe("FileStoreParseError", () => {

  test("formats message with label, path, and underlying parse message", () => {

    const err = new FileStoreParseError("config", "/tmp/foo.json", "Unexpected token");

    assert.match(err.message, /config/);
    assert.match(err.message, /\/tmp\/foo\.json/);
    assert.match(err.message, /Unexpected token/);
    assert.equal(err.name, "FileStoreParseError");
  });

  test("is a subclass of Error so callers can catch with instanceof", () => {

    const err = new FileStoreParseError("x", "y", "z");

    assert.ok(err instanceof Error);
  });
});

describe("createFileStore - construction validation", () => {

  test("throws when migrations are declared but currentSchemaVersion is missing", () => {

    assert.throws(() => createFileStore<unknown>({

      defaultValue: () => ({}),
      label: "test",
      migrations: { 2: { apply: () => undefined, description: "x" } },
      parse: (raw: string): unknown => JSON.parse(raw) as unknown,
      path: () => "/tmp/x.json"
    }), /missing currentSchemaVersion/);
  });
});

describe("FileStore.read", () => {

  test("returns the default value when the file does not exist (first run)", async () => {

    await withTempDir(async (dir) => {

      const store = makeStore<{ items: number[] }>(dir, "missing.json", {

        defaultValue: () => ({ items: [] })
      });
      const result = await store.read();

      assert.deepEqual(result.data, { items: [] });
      assert.equal(result.parseError, false);
      assert.equal(result.recoveredFromBackup, false);
    });
  });

  test("returns parsed data when the file exists and is valid", async () => {

    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "valid.json"), JSON.stringify({ items: [ 1, 2, 3 ] }));

      const store = makeStore<{ items: number[] }>(dir, "valid.json", {

        defaultValue: () => ({ items: [] })
      });
      const result = await store.read();

      assert.deepEqual(result.data.items, [ 1, 2, 3 ]);
      assert.equal(result.parseError, false);
    });
  });

  test("recovers from .bak when the main file is corrupt and .bak is valid", async () => {

    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "corrupt.json"), "{not valid json");
      await writeFile(path.join(dir, "corrupt.json.bak"), JSON.stringify({ items: ["recovered"] }));

      const store = makeStore<{ items: string[] }>(dir, "corrupt.json", {

        defaultValue: () => ({ items: [] })
      });
      const result = await store.read();

      assert.deepEqual(result.data.items, ["recovered"]);
      assert.equal(result.recoveredFromBackup, true);
    });
  });

  test("falls back to defaults with parseError=true when both main and .bak are corrupt", async () => {

    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "double-corrupt.json"), "{not valid");
      await writeFile(path.join(dir, "double-corrupt.json.bak"), "{also not valid");

      const store = makeStore<{ items: string[] }>(dir, "double-corrupt.json", {

        defaultValue: () => ({ items: ["default"] })
      });
      const result = await store.read();

      assert.deepEqual(result.data.items, ["default"]);
      assert.equal(result.parseError, true);
      assert.ok(result.parseErrorMessage, "parseErrorMessage populated");
    });
  });

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
});

describe("FileStore.mutate", () => {

  test("performs an atomic write (temp + rename) with stringifySorted output", async () => {

    await withTempDir(async (dir) => {

      const store = makeStore<{ items: string[] }>(dir, "atomic.json", {

        defaultValue: () => ({ items: [] })
      });

      await store.mutate((data) => {

        data.items.push("a");
        data.items.push("b");
      });

      const written = await readFile(path.join(dir, "atomic.json"), "utf-8");
      const parsed = JSON.parse(written) as { items: string[] };

      assert.deepEqual(parsed.items, [ "a", "b" ]);
      assert.ok(written.endsWith("\n"), "trailing newline added by stringifySorted output");
    });
  });

  test("creates a .bak copy of the previous content before overwriting", async () => {

    await withTempDir(async (dir) => {

      const store = makeStore<{ value: number }>(dir, "bak-test.json", {

        defaultValue: () => ({ value: 0 })
      });

      await store.mutate((data) => { data.value = 1; });
      await store.mutate((data) => { data.value = 2; });

      const bakContent = await readFile(path.join(dir, "bak-test.json.bak"), "utf-8");
      const bakParsed = JSON.parse(bakContent) as { value: number };

      assert.equal(bakParsed.value, 1, ".bak holds the prior good state (value=1) before the second write set value=2");
    });
  });

  test("throws FileStoreParseError when the main file is corrupt and recovery fails", async () => {

    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "corrupt-mutate.json"), "{not valid");
      // No usable .bak.

      const store = makeStore<{ value: number }>(dir, "corrupt-mutate.json", {

        defaultValue: () => ({ value: 0 })
      });

      await assert.rejects(
        () => store.mutate((data) => { data.value = 99; }),
        FileStoreParseError,
        "FileStoreParseError surfaces when both main and .bak fail"
      );
    });
  });

  test("serializes concurrent mutations (queue-based ordering)", async () => {

    await withTempDir(async (dir) => {

      const store = makeStore<{ counter: number }>(dir, "queue.json", {

        defaultValue: () => ({ counter: 0 })
      });

      // Fire many concurrent increments. Without serialization they would race and the final value would be < 5.
      await Promise.all([

        store.mutate((data) => { data.counter++; }),
        store.mutate((data) => { data.counter++; }),
        store.mutate((data) => { data.counter++; }),
        store.mutate((data) => { data.counter++; }),
        store.mutate((data) => { data.counter++; })
      ]);

      const written = await readFile(path.join(dir, "queue.json"), "utf-8");
      const parsed = JSON.parse(written) as { counter: number };

      assert.equal(parsed.counter, 5, "all five increments landed (proves serialization)");
    });
  });

  test("subsequent mutations succeed after one throws (queue does not break)", async () => {

    await withTempDir(async (dir) => {

      const store = makeStore<{ value: number }>(dir, "throwing.json", {

        defaultValue: () => ({ value: 0 })
      });

      await assert.rejects(() => store.mutate(() => {

        throw new Error("synthetic mutation error");
      }), /synthetic mutation error/);

      // Subsequent mutation should succeed - the queue must not have broken.
      await store.mutate((data) => { data.value = 42; });

      const written = JSON.parse(await readFile(path.join(dir, "throwing.json"), "utf-8")) as { value: number };

      assert.equal(written.value, 42);
    });
  });

  test("calls the validator with prev and next state and surfaces issues via log", async () => {

    await withTempDir(async (dir) => {

      let captured: { next: { value: number } | null; prev: { value: number } | null } = { next: null, prev: null };

      const store = makeStore<{ value: number }>(dir, "validator.json", {

        defaultValue: () => ({ value: 0 }),
        validate: (prev, next): ValidationIssue[] => {

          captured = { next: { ...next }, prev: { ...prev } };

          return [];
        }
      });

      await store.mutate((data) => { data.value = 1; });
      await store.mutate((data) => { data.value = 2; });

      assert.equal(captured.prev?.value, 1, "validator received prev state");
      assert.equal(captured.next?.value, 2, "validator received next state");
    });
  });

  test("post-write readback verifies the written content equals the intended write", async () => {

    // The post-write check reads back what was written. If the readback matches, the mutate completes; if not, it would throw.
    await withTempDir(async (dir) => {

      const store = makeStore<{ value: number }>(dir, "readback.json", {

        defaultValue: () => ({ value: 0 })
      });

      await store.mutate((data) => { data.value = 7; });

      const written = await readFile(path.join(dir, "readback.json"), "utf-8");
      const parsed = JSON.parse(written) as { value: number };

      assert.equal(parsed.value, 7, "post-write readback contract: file content matches what we wrote");
    });
  });
});

describe("FileStore.snapshot", () => {

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
});
