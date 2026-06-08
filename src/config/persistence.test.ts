/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.test.ts: Core unit tests for the transactional file store framework. The framework is the SSOT for atomic writes, serialized mutations, declarative
 * schema migrations, post-write integrity verification, and snapshot management - every config file (channels, config, profiles, health) goes through it.
 *
 * This file owns the framework's CORE behaviors - error class, construction validation, read happy paths, mutate happy paths, queue serialization. Three
 * sibling files (persistence.snapshots.test.ts, persistence.integrity.test.ts, persistence.migrations.test.ts) own the snapshot system, the integrity-and-
 * recovery branches, and the migration runner respectively. The split is by concern, not alphabet, so each file's title corresponds directly to a section of
 * the framework's contract.
 */
import { FileStoreParseError, createFileStore } from "./persistence.ts";
import { describe, test } from "node:test";
import { makeMemoryStorageBackend, makeMemoryStore, makeStore } from "./persistence.helpers.ts";
import { readFile, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

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

  test("the name override survives a throw/catch round-trip and remains queryable on the caught instance", () => {

    /* Route handlers catch FileStoreParseError specifically to return HTTP 400 rather than 500 - they rely on the .name override (rather than instanceof) when
     * the error has crossed a serialization boundary or has been wrapped in another error's `cause` chain. We pin both: catching the thrown error reads name
     * correctly, and a synthetic AggregateError that wraps it via cause leaves the inner name intact for inspection.
     */
    const original = new FileStoreParseError("channels", "/tmp/x.json", "boom");

    try {

      throw original;
    } catch(caught) {

      assert.ok(caught instanceof FileStoreParseError, "caught instance is structurally a FileStoreParseError");
      assert.equal((caught).name, "FileStoreParseError", ".name override visible after catch");
    }

    // Wrap via cause to mimic the route-handler pattern where a higher-level error reports the parse error as its underlying cause. The inner instance keeps
    // its overridden name, even though the outer Error's name is the default "Error".
    const wrapper = new Error("higher-level failure", { cause: original });

    assert.equal((wrapper.cause as Error).name, "FileStoreParseError", ".name preserved through cause-chain wrapping");
    assert.equal(wrapper.name, "Error", "wrapper carries its own name; the override is scoped to the inner instance");
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

describe("FileStore.read - core paths", () => {

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
});

describe("FileStore.mutate - core paths", () => {

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

  test("queue continuity holds under rapid concurrent mutations where every other one rejects", async () => {

    /* The "subsequent succeeds after one throws" test pins continuity at low cadence. The queue's promise-chain reference is `queue = operation.catch(() => {})`
     * - a single empty catch installed once per dispatch. A regression that broke the chain reference (e.g., dropping the catch, replacing with the original
     * promise rather than the swallowed one, or short-circuiting on the first rejection) would surface only under a burst where multiple rejections interleave
     * with successes - the low-cadence test would still pass.
     *
     * We fire 10 concurrent mutations: even-indexed ones throw, odd-indexed ones write a unique value. Even-indexed promises must reject; odd-indexed promises
     * must resolve and produce a final on-disk state equal to the last odd value written. A regression that broke the queue would surface as an unhandled
     * rejection, an uncaught throw from a later odd-indexed call, or a missing tail value on disk.
     */
    await withTempDir(async (dir) => {

      const store = makeStore<{ value: number }>(dir, "burst.json", {

        defaultValue: () => ({ value: 0 })
      });

      const promises = [];
      const expectedFinalValue = 9;

      for(let i = 0; i < 10; i++) {

        if((i % 2) === 0) {

          promises.push(store.mutate(() => {

            throw new Error("burst-reject-" + String(i));
          }));
        } else {

          promises.push(store.mutate((data) => { data.value = i; }));
        }
      }

      // Use Promise.allSettled so we can inspect every outcome rather than short-circuiting on the first rejection.
      const results = await Promise.allSettled(promises);

      for(let i = 0; i < 10; i++) {

        if((i % 2) === 0) {

          assert.equal(results[i]?.status, "rejected", "even-indexed mutation #" + String(i) + " must reject");
        } else {

          assert.equal(results[i]?.status, "fulfilled", "odd-indexed mutation #" + String(i) + " must resolve - queue chain remains intact across rejections");
        }
      }

      // The serialization guarantee says queued mutations apply in dispatch order. The last successful mutation set value=9, so the on-disk state must reflect
      // that - any earlier queue break would freeze value at an earlier number.
      const written = JSON.parse(await readFile(path.join(dir, "burst.json"), "utf-8")) as { value: number };

      assert.equal(written.value, expectedFinalValue, "final on-disk value reflects the last successful mutation - queue chain intact end-to-end");
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

describe("FileStore.mutate - corrupt-main rotation guard", () => {

  test("a mutate that follows a failed .bak restore does not rotate the corrupt main into .bak (good copy survives)", async () => {

    /* This pins the layered protection for the catastrophic data-loss window: when the main file is corrupt and read() recovers from .bak in memory but the
     * on-disk restore-write fails, the .bak holds the ONLY good copy. A subsequent mutate's pre-write backup step must NOT copy the still-corrupt main over
     * .bak - doing so would destroy that last good copy.
     *
     * We force the restore-write to fail by overriding rename for the recovery's temp-to-main path, leaving main corrupt on disk. The recovery still surfaces
     * recovered=true with the .bak's content, so the in-memory state the mutate operates on is good. We then assert that after the mutate: .bak still holds the
     * original good content (it was NOT clobbered by the corrupt main) and main holds the freshly-mutated good content (the atomic write replaced it).
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/rotation-guard.json";
    const goodBakContent = "{\"value\":99}\n";

    // Seed: corrupt main, valid .bak. read() detects the corrupt main, recovers from .bak, and attempts to restore main.
    backend.files.set(filePath, "{not valid json");
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", goodBakContent);
    backend.mtimes.set(filePath + ".bak", 2);

    // Fail the recovery restore-write. tryRecoverFromBackup restores via <filePath>.recover.tmp -> filePath; we trip exactly that rename so main stays corrupt
    // on disk. The post-mutate atomic rename (<filePath>.tmp -> filePath) flows through the default so the user's mutation still lands.
    const realRename = backend.rename;

    backend.rename = async (source: string, destination: string): Promise<void> => {

      if((source === (filePath + ".recover.tmp")) && (destination === filePath)) {

        throw new Error("synthetic-restore-rename-failure");
      }

      return realRename(source, destination);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    // The mutate reads (recovering from .bak in memory), applies the change, and writes. The corrupt main must never be rotated into .bak.
    await store.mutate((data) => { data.value = 1234; });

    // The good .bak survived: it still holds the original recovered content, NOT the corrupt main. This is the heart of the data-loss guard.
    assert.equal(backend.files.get(filePath + ".bak"), goodBakContent, ".bak retains the only good copy - the corrupt main was never rotated into it");

    // The atomic write replaced the corrupt main with the freshly-mutated good data.
    const mainParsed = JSON.parse(backend.files.get(filePath) ?? "") as { value: number };

    assert.equal(mainParsed.value, 1234, "main file holds the freshly-mutated good data after the atomic write");
  });
});

describe("FileStore - recovery and mutate use distinct temp paths", () => {

  test("a read-triggered recovery writes to a recovery-specific temp path, distinct from the mutate temp path", async () => {

    /* read() does not acquire the mutate serialization lock, so a read-triggered recovery can overlap an in-flight mutate. If recovery and mutate both wrote
     * the SAME <filePath>.tmp, the recovery's write or rename could clobber the mutate's in-flight temp before the mutate's rename. The framework gives recovery
     * a distinct suffix (.recover.tmp) so the two write paths never collide.
     *
     * We pin the contract directly: drive a recovery (corrupt main, valid .bak) and a normal write (a subsequent mutate), recording every temp path the
     * framework writes. The recovery must use <filePath>.recover.tmp and the mutate must use <filePath>.tmp - two distinct keys that can never overwrite one
     * another.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/distinct-temp.json";

    // Seed: corrupt main, valid .bak so the first read triggers recovery (which writes its restore temp), and the subsequent mutate writes its own atomic temp.
    backend.files.set(filePath, "{not valid json");
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", "{\"value\":7}\n");
    backend.mtimes.set(filePath + ".bak", 2);

    // Record every temp-family write target so we can assert the recovery and the mutate used distinct suffixes.
    const tempWrites: string[] = [];
    const realWriteFile = backend.writeFile;

    backend.writeFile = async (p: string, content: string): Promise<void> => {

      if(p.endsWith(".tmp")) {

        tempWrites.push(p);
      }

      return realWriteFile(p, content);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    // The mutate's internal read recovers from .bak (restore-write to .recover.tmp), then the atomic write lands the mutation via .tmp.
    await store.mutate((data) => { data.value = 11; });

    assert.ok(tempWrites.includes(filePath + ".recover.tmp"), "recovery restore-write targets the recovery-specific temp path");
    assert.ok(tempWrites.includes(filePath + ".tmp"), "the atomic mutate-write targets the plain temp path");
    assert.notEqual(filePath + ".recover.tmp", filePath + ".tmp", "recovery and mutate temp paths are distinct - they can never clobber each other");
  });
});
