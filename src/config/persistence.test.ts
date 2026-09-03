/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.test.ts: Core unit tests for the transactional file store framework. The framework is the SSOT for atomic writes, serialized mutations, declarative
 * schema migrations, post-write integrity verification, and snapshot management - every config file (channels, config, profiles, health) goes through it.
 *
 * This file owns the framework's CORE behaviors - error class, construction validation, read happy paths, mutate happy paths, queue serialization - plus the
 * write-ownership rule that ties reads and mutates together: a read recovers a corrupt main from .bak in memory and writes nothing, the corrupt-main rotation
 * guard keeps the good backup, and the durable restore lands under the queue at the boot step or through the next mutate. Three sibling files
 * (persistence.snapshots.test.ts, persistence.integrity.test.ts, persistence.migrations.test.ts) own the snapshot system, the remaining integrity-and-recovery
 * branches, and the migration runner respectively. The split is by concern, not alphabet, so each file's title corresponds directly to a section of the
 * framework's contract.
 */
import { FileStoreParseError, createFileStore } from "./persistence.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { makeMemoryStorageBackend, makeMemoryStore, makeStore } from "./persistence.helpers.ts";
import { readFile, writeFile } from "node:fs/promises";
import type { LogEntry } from "../utils/logEmitter.ts";
import type { Migration } from "./persistence.ts";
import assert from "node:assert/strict";
import path from "node:path";
import { subscribeToLogs } from "../utils/logEmitter.ts";
import { withTempDir } from "../testing.helpers.ts";

/* Every log entry emitted for the duration of a test. The rows that assert how often the framework announces a repair read it the way an operator reads the
 * Logs tab - by level and message - rather than by swapping the logger, which is the observation shape persistence.integrity.test.ts establishes for this
 * suite. Reset per test so one test's framework output cannot leak into another's count.
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
     * the error has crossed a serialization boundary or has been wrapped in another error's `cause` chain. We assert both: catching the thrown error reads name
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

  test("a bare read announces the recovery it performed, once, with the parse failure that prompted it", async () => {

    /* A caller reading on its own is the only reporter of what that read found: no boot step has repaired the file ahead of it, and no write follows to
     * announce anything. The warning therefore belongs to this read, carries the corrupt-main parse message an operator needs to identify the damage, and
     * appears exactly once for the one recovery that happened.
     */
    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "announce-once.json"), "{not valid json");
      await writeFile(path.join(dir, "announce-once.json.bak"), JSON.stringify({ items: ["recovered"] }));

      const store = makeStore<{ items: string[] }>(dir, "announce-once.json", {

        defaultValue: () => ({ items: [] })
      });

      await store.read();

      const recoveries = captured.filter((line) => (line.level === "warn") && /Recovered .* from backup/.test(line.message));

      assert.equal(recoveries.length, 1, "one recovery, one warning");
      assert.match(recoveries[0]?.message ?? "", /test-announce-once\.json/, "the warning names the store whose file was recovered");
      assert.match(recoveries[0]?.message ?? "", /main file was corrupt/, "and carries the parse failure that prompted it");
    });
  });

  test("a recovered read answers with the public result shape and no field the store keeps for its own log lines", async () => {

    /* The store knows more about a recovery than the result exposes: the parse message the warning prints is read from disk and never handed to a caller,
     * because it is text for the log rather than data to act on. This row is the boundary check - what a caller can see on a recovered read is exactly the
     * members the result type declares for that path.
     */
    await withTempDir(async (dir) => {

      await writeFile(path.join(dir, "public-shape.json"), "{not valid json");
      await writeFile(path.join(dir, "public-shape.json.bak"), JSON.stringify({ items: ["recovered"] }));

      const store = makeStore<{ items: string[] }>(dir, "public-shape.json", {

        defaultValue: () => ({ items: [] })
      });
      const result = await store.read();

      assert.deepEqual(Object.keys(result).toSorted(), [ "data", "migrationResult", "parseError", "recoveredFromBackup" ],
        "a recovered read carries the members of the read result and nothing else");
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

    /* The "subsequent succeeds after one throws" test asserts continuity at low cadence. The queue's promise-chain reference is `queue = operation.catch(() => {})`
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

  test("a mutate against a corrupt main does not rotate it into .bak (the good copy survives)", async () => {

    /* This asserts the layered protection for the catastrophic data-loss window: when the main file is corrupt, the .bak holds the ONLY good copy, and a
     * mutate's pre-write backup step must NOT copy the still-corrupt main over it - doing so would destroy that last good copy.
     *
     * Reaching that window takes no injection. A read recovers the backup in memory and writes nothing, so the main file on disk is still corrupt when
     * doMutate's backup step runs, which is exactly the state the guard exists for. We assert both halves of the outcome: .bak still holds the original good
     * content (it was NOT clobbered by the corrupt main) and main holds the freshly-mutated good content (the atomic write replaced it).
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/rotation-guard.json";
    const goodBakContent = "{\"value\":99}\n";

    // Seed: corrupt main, valid .bak. The mutate's internal read recovers from .bak and reports recoveredFromBackup, which is what trips the guard.
    backend.files.set(filePath, "{not valid json");
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", goodBakContent);
    backend.mtimes.set(filePath + ".bak", 2);

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

describe("FileStore.read - recovery in memory, the durable restore under the queue", () => {

  test("a bare read against a corrupt main returns the backup's data and writes nothing at all", async () => {

    /* read() does not hold the mutate queue, so it must not write. A write outside the queue can interleave with a mutate's own temp-plus-rename and put stale
     * bytes over data the mutate had already committed, which is a silent revert of a save the caller was told had succeeded. The recovery a read performs is
     * therefore in-memory only, and the recorded write log below asserts that directly - not that the read wrote the right thing, but that it wrote nothing.
     *
     * The mutate that follows is what makes the file good on disk, and it goes through the single temp path every writer shares. Sharing is safe precisely
     * because every writer holds the queue, so no two temp writes for a path are ever in flight together.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/read-writes-nothing.json";
    const corruptMain = "{not valid json";

    backend.files.set(filePath, corruptMain);
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", "{\"value\":7}\n");
    backend.mtimes.set(filePath + ".bak", 2);

    // Record every write target and every rename, so "wrote nothing" is read off the backend rather than inferred from the file's contents.
    const writes: string[] = [];
    const renames: string[] = [];
    const realWriteFile = backend.writeFile;
    const realRename = backend.rename;

    backend.writeFile = async (p: string, content: string): Promise<void> => {

      writes.push(p);

      return realWriteFile(p, content);
    };

    backend.rename = async (source: string, destination: string): Promise<void> => {

      renames.push(source + " -> " + destination);

      return realRename(source, destination);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });
    const result = await store.read();

    assert.equal(result.recoveredFromBackup, true, "the read recovered the backup's contents");
    assert.deepEqual(result.data, { value: 7 }, "and returned the backup's data");
    assert.deepEqual(writes, [], "the read issued no write at all");
    assert.deepEqual(renames, [], "and no rename");
    assert.equal(backend.files.get(filePath), corruptMain, "the corrupt bytes are still on disk, waiting for a writer that holds the queue");

    // The mutate is the durable repair, and it lands through the one temp path.
    await store.mutate((data) => { data.value = 11; });

    assert.deepEqual(writes, [filePath + ".tmp"], "the mutate's atomic write is the only write, through the single temp path");
    assert.deepEqual(renames, [filePath + ".tmp -> " + filePath], "and the single rename that puts it in place");

    const mainParsed = JSON.parse(backend.files.get(filePath) ?? "") as { value: number };

    assert.equal(mainParsed.value, 11, "the main file now holds the mutated good data");
  });

  test("a mutate that meets a corrupt main announces the repair it is about to persist, once", async () => {

    /* A mutate at run time is the other place a repair happens, and no boot step ran ahead of it to have announced anything. Its own read is what finds the
     * damage and what reports it, so the warning appears once for the write that fixes the file - the same one-line-per-repair rule the boot step follows.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/mutate-announces.json";

    backend.files.set(filePath, "{not valid json");
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", "{\"value\":3}\n");
    backend.mtimes.set(filePath + ".bak", 2);

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    await store.mutate((data) => { data.value += 1; });

    const recoveries = captured.filter((line) => (line.level === "warn") && /Recovered .* from backup/.test(line.message));

    assert.equal(recoveries.length, 1, "the mutate's own read announces the recovery once");

    const mainParsed = JSON.parse(backend.files.get(filePath) ?? "") as { value: number };

    assert.equal(mainParsed.value, 4, "and the write persisted the recovered data with the mutation applied");
  });

  test("a bare read parked inside its recovery cannot revert a mutate that completed while it waited", async () => {

    /* The race the in-memory recovery closes. A bare read reaches its backup read and parks there; a mutate runs start to finish meanwhile; then the read is
     * released. Because the read writes nothing, the file ends up holding the mutate's data and the read still answers with the backup's data - two correct
     * answers about two different moments.
     *
     * Both outcomes of this rig are worth naming, because the difference between them is the whole reason the restore sits under the queue. In a design where
     * a read restores the main file itself, the released read's rename lands after the mutate's and the file ends up reverted to the backup's content, losing a
     * save the caller was told had succeeded. Giving the two paths distinct temp suffixes does not help: the collision is not between two temp files, it is
     * between two renames over the same main file, one of them outside the queue.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/read-mutate-race.json";

    backend.files.set(filePath, "{not valid json");
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", "{\"value\":99}\n");
    backend.mtimes.set(filePath + ".bak", 2);

    /* Park the FIRST read of the .bak and let every later one through. The bare read reads main before .bak, so the first .bak read is unambiguously the bare
     * read's; the mutate's own internal read then flows through untouched, which is what lets the mutate finish while the bare read is still parked.
     */
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: parked, resolve: releaseRead } = Promise.withResolvers<void>();
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: reachedBackup, resolve: markReachedBackup } = Promise.withResolvers<void>();
    let parkedOnce = false;
    const realReadFile = backend.readFile;

    backend.readFile = async (p: string): Promise<string> => {

      if(!parkedOnce && (p === (filePath + ".bak"))) {

        parkedOnce = true;
        markReachedBackup();

        await parked;
      }

      return realReadFile(p);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    // Start the bare read and wait until it is parked on its backup read.
    const pendingRead = store.read();

    await reachedBackup;

    // The mutate runs start to finish while the read is parked, and commits its own content over the corrupt main.
    await store.mutate((data) => { data.value = 4242; });

    const mutatedMain = backend.files.get(filePath);

    assert.match(mutatedMain ?? "", /4242/, "sanity: the mutate committed its data over the corrupt main while the read was parked");

    // Release the parked read.
    releaseRead();

    const result = await pendingRead;

    assert.equal(result.recoveredFromBackup, true, "the released read still reports the recovery it performed");
    assert.deepEqual(result.data, { value: 99 }, "and answers with the backup's data, which is what it read");
    assert.equal(backend.files.get(filePath), mutatedMain, "the mutate's committed content is exactly what is on disk - the read reverted nothing");
  });
});

describe("FileStore.ensureMigrated - the durable restore at boot", () => {

  test("a corrupt main with a valid backup is rewritten under the queue, and a repeat call writes nothing", async () => {

    /* Boot is where a corrupt main file becomes a good main file on disk. ensureMigrated already persists an in-memory schema upgrade through a no-op mutate;
     * an in-memory backup recovery is the same kind of debt - state the process holds that the disk does not - so it is settled the same way and in the same
     * place, under the queue.
     *
     * The repeat call is the other half of the contract: once the file on disk parses, a later boot has nothing to persist and issues no write at all. Without
     * that, every boot would rewrite a file it had already repaired.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/boot-restore.json";
    const bakContent = "{\"value\":42}\n";

    backend.files.set(filePath, "{not valid json");
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", bakContent);
    backend.mtimes.set(filePath + ".bak", 2);

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    await store.ensureMigrated();

    const mainParsed = JSON.parse(backend.files.get(filePath) ?? "") as { value: number };

    assert.equal(mainParsed.value, 42, "the main file carries the backup's data after the boot step");
    assert.equal(backend.files.get(filePath + ".bak"), bakContent, ".bak is untouched - the rotation is skipped on a recovered read");

    // Second call: the repaired file on disk parses, so there is nothing to persist. Recording from here means the log covers only the repeat call.
    const writes: string[] = [];
    const realWriteFile = backend.writeFile;

    backend.writeFile = async (p: string, content: string): Promise<void> => {

      writes.push(p);

      return realWriteFile(p, content);
    };

    await store.ensureMigrated();

    assert.deepEqual(writes, [], "a repeat boot step over a repaired file is a pure read");
  });

  test("a boot that repairs a corrupt main announces the recovery once, not once per read", async () => {

    /* The boot step reads the file twice when it has something to repair: once to decide whether the disk needs a write, and once inside the queued write that
     * performs it. Only the second is the event an operator cares about - a repair that landed on disk - so the decision is taken through the store's silent
     * path and the Logs tab shows one line for the one repair rather than a duplicate for the read that only looked.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/recovery-announced-once.json";

    backend.files.set(filePath, "{not valid json");
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", "{\"value\":42}\n");
    backend.mtimes.set(filePath + ".bak", 2);

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    await store.ensureMigrated();

    const recoveries = captured.filter((line) => (line.level === "warn") && /Recovered .* from backup/.test(line.message));

    assert.equal(recoveries.length, 1, "one repair, one warning, across both of the boot step's reads");
    assert.match(recoveries[0]?.message ?? "", /test-mem-recovery-announced-once\.json/, "and it names the store whose file was repaired");

    const mainParsed = JSON.parse(backend.files.get(filePath) ?? "") as { value: number };

    assert.equal(mainParsed.value, 42, "the write the warning describes is the one that repaired the file");
  });

  test("a main file and a backup that are both unparseable leave the boot step a pure read rather than a failure", async () => {

    /* The boundary between the repairs the boot step persists. A file with no usable backup has nothing to recover, so read() reports parseError with no
     * migrations and no recovery, and the boot step must take neither branch: attempting the no-op mutate would hit doMutate's corruption guard and throw
     * FileStoreParseError out of startup, turning an unreadable settings file into a server that will not start.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/doubly-corrupt.json";
    const corruptMain = "{not valid json";
    const corruptBak = "{also not valid";

    backend.files.set(filePath, corruptMain);
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", corruptBak);
    backend.mtimes.set(filePath + ".bak", 2);

    const writes: string[] = [];
    const realWriteFile = backend.writeFile;

    backend.writeFile = async (p: string, content: string): Promise<void> => {

      writes.push(p);

      return realWriteFile(p, content);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });
    const result = await store.ensureMigrated();

    assert.deepEqual(result.applied, [], "no migrations were applied");
    assert.deepEqual(writes, [], "and no write was attempted");
    assert.equal(backend.files.get(filePath), corruptMain, "the main file is left for an operator to deal with");
    assert.equal(backend.files.get(filePath + ".bak"), corruptBak, "and so is the backup");

    /* The boot step says nothing about a file it could not repair, because it repaired nothing. The failure still reaches the operator: every store reads its
     * own file after the boot step, and that read reports it once, from the caller that is about to work with defaults.
     */
    const unusable = (): number => captured.filter((line) => (line.level === "warn") && line.message.includes("no usable backup available")).length;

    assert.equal(unusable(), 0, "the boot step announced nothing about a file it could not repair");

    await store.read();

    assert.equal(unusable(), 1, "and the read that follows reports the unusable file once");
  });

  test("a file from a newer build leaves the boot step a pure read, and the read that follows reports it once", async () => {

    /* A file whose schemaVersion sits above what this build understands is passed through rather than migrated, so the boot step has nothing to persist and
     * nothing to announce. The limitation still reaches the operator through the store's own read, on the same rule every other line follows: whoever acts on
     * what the read found is the one that reports it. The store declares a migration map because the runner returns before the version comparison without one.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/newer-build.json";
    const newerFile = "{\"schemaVersion\":99,\"value\":5}\n";

    backend.files.set(filePath, newerFile);
    backend.mtimes.set(filePath, 1);

    const writes: string[] = [];
    const realWriteFile = backend.writeFile;

    backend.writeFile = async (p: string, content: string): Promise<void> => {

      writes.push(p);

      return realWriteFile(p, content);
    };

    const migrations: Record<number, Migration<{ schemaVersion?: number; value?: number }>> = {

      2: {

        apply: (data): void => { data.value = 0; },
        description: "v2 upgrade"
      }
    };
    const store = makeMemoryStore<{ schemaVersion?: number; value?: number }>(backend, filePath, {

      currentSchemaVersion: 2,
      defaultValue: () => ({ schemaVersion: 2, value: 0 }),
      migrations
    });

    await store.ensureMigrated();

    const newer = (): number => captured.filter((line) => (line.level === "warn") && line.message.includes("newer than this build supports")).length;

    assert.deepEqual(writes, [], "there is nothing to persist for a file this build cannot migrate");
    assert.equal(newer(), 0, "and the boot step announces nothing");
    assert.equal(backend.files.get(filePath), newerFile, "the newer file is left exactly as it was found");

    await store.read();

    assert.equal(newer(), 1, "the read that follows reports the newer file once");
  });
});
