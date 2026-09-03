/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.test.ts: Unit tests for the file-based logger's basic write and buffer paths - initializeFileLogger + writeLogEntry happy paths, flushLogBuffer,
 * flushLogBufferSync, the existing-file initialization branch, the periodic flushTimer interval, the trim/flush write-ordering race between a real trim
 * and a concurrent append, and shutdown's drain of an in-flight trim. Additional trim-path unit tests (computeTrimmedLogContent, checkAndTrimFile branches)
 * live in fileLogger.trim.test.ts; lifecycle and error-recovery paths live in fileLogger.lifecycle.test.ts.
 *
 * The module holds module-scope state (initialization status, write buffer, file path) that persists across tests. Each test scopes its filesystem state to a
 * temp directory via withTempDir and calls shutdownFileLogger() in afterEach to reset the singleton between cases. The flush timer would run in the background
 * during real use; we always shut it down to avoid cross-test interference.
 */
import { afterEach, describe, mock, test } from "node:test";
import { flushLogBuffer, flushLogBufferSync, initializeFileLogger, shutdownFileLogger, writeLogEntry } from "./fileLogger.ts";
import { readFile, stat, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import fs from "node:fs";
import { initDebugFilter } from "./debugFilter.ts";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

describe("initializeFileLogger and writeLogEntry", () => {

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("creates the log file when it does not exist", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      // The init step writes an empty file. We can read it back to verify creation.
      const content = await readFile(logPath, "utf-8");

      assert.equal(content, "");
    });
  });

  test("creates parent directories when they do not exist", async () => {

    // Boundary: log path inside a non-existent subdirectory. The function must mkdir -p.
    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "nested", "subdir", "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      const content = await readFile(logPath, "utf-8");

      assert.equal(content, "");
    });
  });

  test("writeLogEntry buffers content and flushLogBuffer persists it", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      writeLogEntry("info", "Hello, world.", null);

      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");

      assert.match(content, /Hello, world\./, "info-level message persisted");
      assert.match(content, /^\[\d{4}\/\d{2}\/\d{2}/, "timestamp prefix in yyyy/mm/dd format");
    });
  });

  test("writes warn-level entries with the [WARN] prefix", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      writeLogEntry("warn", "This is a warning.", "yellow");

      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");

      assert.match(content, /\[WARN\]/);
      assert.match(content, /This is a warning\./);
    });
  });

  test("writes error-level entries with the [ERROR] prefix", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      writeLogEntry("error", "Something failed.", "red");

      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");

      assert.match(content, /\[ERROR\]/);
      assert.match(content, /Something failed\./);
    });
  });

  test("writes debug entries with the category-suffixed [DEBUG:cat] prefix", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      writeLogEntry("debug", "Detailed trace.", "cyan", "tuning:hulu");

      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");

      assert.match(content, /\[DEBUG:tuning:hulu\]/);
      assert.match(content, /Detailed trace\./);
    });
  });

  test("info-level messages have no level prefix (the default branch)", async () => {

    // Negative test: info-level entries are special-cased to omit the [INFO] prefix because they are the default.
    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      writeLogEntry("info", "Plain info.", null);

      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");

      assert.doesNotMatch(content, /\[INFO\]/, "no [INFO] prefix on info messages");
      assert.match(content, /Plain info\./);
    });
  });

  test("multiple writes accumulate and flush atomically", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      writeLogEntry("info", "First.", null);
      writeLogEntry("info", "Second.", null);
      writeLogEntry("info", "Third.", null);

      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");
      const lines = content.split("\n").filter((line) => line.length > 0);

      assert.equal(lines.length, 3);
    });
  });

  test("writeLogEntry is a no-op when the file logger has not been initialized", async () => {

    // Negative test: writes that happen before init must not crash. shutdownFileLogger() (in afterEach) returns to the uninitialized state, so calling
    // writeLogEntry now should be silently dropped.
    await shutdownFileLogger();

    assert.doesNotThrow(() => {

      writeLogEntry("info", "ignored", null);
    });
  });
});

describe("flushLogBuffer", () => {

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("returns without writing when the buffer is empty", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");

      assert.equal(content, "", "no content written when buffer is empty");
    });
  });

  test("does not throw when called before init (boundary)", async () => {

    await shutdownFileLogger();
    await assert.doesNotReject(() => flushLogBuffer());
  });
});

describe("flushLogBufferSync", () => {

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("synchronously persists buffered entries", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      writeLogEntry("info", "Sync write.", null);

      flushLogBufferSync();

      const content = await readFile(logPath, "utf-8");

      assert.match(content, /Sync write\./);
    });
  });

  test("does not throw when buffer is empty", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      assert.doesNotThrow(() => { flushLogBufferSync(); });
    });
  });

  test("does not throw when called before init", async () => {

    await shutdownFileLogger();
    assert.doesNotThrow(() => { flushLogBufferSync(); });
  });
});

describe("initializeFileLogger - existing-file branch", () => {

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("preserves existing file content when the log file already exists", async () => {

    // Boundary: when the log file already has content from a previous run, init must NOT truncate it - it should stat the existing size and append new entries
    // alongside what was already there. This asserts the stat-existing-file branch of initializeFileLogger in fileLogger.ts (the case where stat succeeds and
    // approximateSize is seeded from the existing file size rather than the ENOENT create-empty path).
    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");
      const previousContent = "[2026/01/01 12:00:00.000 PM] Previous run line 1.\n[2026/01/01 12:00:00.001 PM] Previous run line 2.\n";

      await writeFile(logPath, previousContent, "utf-8");

      await initializeFileLogger(logPath, 1_000_000);

      // Append a new entry and flush.
      writeLogEntry("info", "New run line.", null);
      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");

      // Both the previous content and the new entry should be present.
      assert.match(content, /Previous run line 1\./, "first previous line preserved");
      assert.match(content, /Previous run line 2\./, "second previous line preserved");
      assert.match(content, /New run line\./, "new entry appended");
    });
  });
});

describe("flushTimer interval", () => {

  /* The periodic flush timer is set up in initializeFileLogger and fires every FLUSH_INTERVAL_MS (1000) milliseconds. Tests above exercise flushLogBuffer
   * directly; here we exercise the timer-driven flush path by enabling mock.timers and ticking past the interval boundary.
   */

  afterEach(async () => {

    await shutdownFileLogger();
    mock.timers.reset();
  });

  test("buffered writes flush automatically when the periodic timer fires", async () => {

    mock.timers.enable({ apis: [ "setInterval", "setTimeout" ] });

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "timer.log");

      await initializeFileLogger(logPath, 1_000_000);

      writeLogEntry("info", "Buffered entry.", null);

      // Before the tick, the buffer holds the entry but the file is empty (init wrote "").
      const beforeContent = await readFile(logPath, "utf-8");

      assert.equal(beforeContent, "", "file empty before periodic flush");

      // Tick past the FLUSH_INTERVAL_MS boundary. The setInterval callback fires void flushLogBuffer().
      mock.timers.tick(1100);

      // The flush is async (void Promise) - drain the microtask queue so the appendFile completes.
      await new Promise((resolve) => setImmediate(resolve));
      await new Promise((resolve) => setImmediate(resolve));

      const afterContent = await readFile(logPath, "utf-8");

      assert.match(afterContent, /Buffered entry\./, "periodic timer flushed the buffered entry to disk");
    });
  });
});

describe("trim/flush write-ordering (interleave race)", () => {

  /* trimLogFile reads the log file, computes a trimmed snapshot, then renames a temp file over the original. flushLogBuffer appends to the same file. Without
   * serialization, an appendFile that lands between the trim's readFile and its rename is silently discarded: the rename overwrites the file with the snapshot
   * taken before the append, dropping those log lines. The fix routes both paths through a single write-ordering chain so a flush can never interleave with an
   * in-flight trim. These tests assert that a flush issued concurrently with a trim is not lost.
   */

  afterEach(async () => {

    await shutdownFileLogger();
    initDebugFilter("");
  });

  test("a flush interleaved with an in-flight trim is not lost", async () => {

    // Debug off so the debug-active gate does not suppress the trim.
    initDebugFilter("");

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "race.log");
      const maxSize = 16384;

      // Pre-seed the log file with content that already exceeds maxSize. checkAndTrimFile reads the on-disk size, so the first size-check fire triggers a trim.
      // Each seed line is a complete entry so the newline-aligned cut is deterministic.
      const seedLine = "[2026/01/01 12:00:00.000 PM] Pre-seeded oversized history line.\n";
      const seedContent = seedLine.repeat(400);

      await writeFile(logPath, seedContent, "utf-8");

      assert.ok(seedContent.length > maxSize, "seed content exceeds maxSize");

      await initializeFileLogger(logPath, maxSize);

      // Write 100 entries to fire the size-check modulo gate, which schedules a trim via `void checkAndTrimFile()` inside writeLogEntry. The trim begins racing
      // its readFile/rename against any concurrent flush.
      for(let i = 0; i < 100; i++) {

        writeLogEntry("info", "Filler " + String(i), null);
      }

      // In the same turn, push a distinctive marker entry and flush it. This append is exactly the operation that could be discarded if it landed between the
      // trim's readFile and its rename. With the write-ordering chain in place, the flush is serialized against the trim and the marker must survive.
      const marker = "RACE_MARKER_must_survive_the_trim";

      writeLogEntry("info", marker + ".", null);

      // A single flush drains every buffered entry (fillers and marker) as one append. We await it to ensure the append has settled on the shared write chain.
      await flushLogBuffer();

      // Poll until the trim has actually fired (file dropped below the seeded size), proving a trim genuinely interleaved with the flush rather than the marker
      // simply being appended to an un-trimmed file. The poll keeps the test deterministic across host timing.
      const deadline = Date.now() + 3000;

      let postTrimSize = seedContent.length;

      while(Date.now() < deadline) {

        // eslint-disable-next-line no-await-in-loop
        const s = await stat(logPath);

        postTrimSize = s.size;

        if(postTrimSize < seedContent.length) {

          break;
        }

        // eslint-disable-next-line no-await-in-loop
        await new Promise((resolve) => setTimeout(resolve, 25));
      }

      assert.ok(postTrimSize < seedContent.length, "a trim actually fired (file shrank below the seed size)");

      // The decisive assertion: despite the concurrent trim rewriting the whole file, the marker line that was flushed during the trim is present on disk.
      const finalContent = await readFile(logPath, "utf-8");

      assert.match(finalContent, new RegExp(marker), "the flushed marker survived the concurrent trim");
    });
  });

  test("serialized trim and flush both land regardless of enqueue order", async () => {

    // Complementary assertion: explicitly enqueue a flush and let a trim fire from the size check, then assert both the trimmed shape and the appended content coexist.
    // This guards the write-ordering guarantee rather than a single timing arrangement.
    initDebugFilter("");

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "order.log");
      const maxSize = 16384;

      const seedLine = "[2026/01/01 12:00:00.000 PM] Seed line for ordering test.\n";
      const seedContent = seedLine.repeat(400);

      await writeFile(logPath, seedContent, "utf-8");
      await initializeFileLogger(logPath, maxSize);

      // Buffer a marker first, then fire the size check via the filler loop. The flush is issued after the loop so the trim is already enqueued ahead of it.
      const marker = "ORDERED_MARKER_survives";

      writeLogEntry("info", marker + ".", null);

      for(let i = 0; i < 100; i++) {

        writeLogEntry("info", "Pad " + String(i), null);
      }

      await flushLogBuffer();

      // Allow the void-scheduled trim to settle behind the flush on the shared chain.
      const deadline = Date.now() + 3000;

      let postTrimSize = seedContent.length;

      while(Date.now() < deadline) {

        // eslint-disable-next-line no-await-in-loop
        const s = await stat(logPath);

        postTrimSize = s.size;

        if(postTrimSize < seedContent.length) {

          break;
        }

        // eslint-disable-next-line no-await-in-loop
        await new Promise((resolve) => setTimeout(resolve, 25));
      }

      assert.ok(postTrimSize < seedContent.length, "the trim fired and shrank the file");

      const finalContent = await readFile(logPath, "utf-8");

      assert.match(finalContent, new RegExp(marker), "the marker flushed alongside the trim survived");
    });
  });

  test("shutdown settles only after an in-flight trim completes, and its final flush lands after the rename", async (t) => {

    /* The drain, exercised against a real trim held open at its rename. The row takes over fs.promises.rename - the same object fileLogger destructured at load -
     * so the trim reaches its critical moment and stops there until this test releases it. With the trim parked, a marker is buffered and shutdown is called
     * without awaiting, which is what lets the row observe that shutdown has NOT settled while the rename is pending.
     *
     * Detector: without the drain, shutdown flushes the marker synchronously into the file the parked rename is about to replace, and the rename then overwrites
     * it with the pre-append snapshot - the last entries of the run silently discarded. The row asserts both halves of the fix: shutdown waits for the rename, and
     * the marker is on disk afterwards.
     *
     * The held window is a handful of microtask turns rather than a sleep, so the 1000 ms periodic flush timer cannot fire inside it and flush the marker onto
     * the chain by itself, which would make the marker survive for a reason other than the drain.
     */
    initDebugFilter("");

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "drain.log");
      const maxSize = 16384;
      const seedLine = "[2026/01/01 12:00:00.000 PM] Seed line for the shutdown drain test.\n";
      const seedContent = seedLine.repeat(400);

      await writeFile(logPath, seedContent, "utf-8");

      assert.ok(seedContent.length > maxSize, "seed content exceeds maxSize");

      await initializeFileLogger(logPath, maxSize);

      const realRename = fs.promises.rename;
      const renameReached = Promise.withResolvers<true>();
      const releaseRename = Promise.withResolvers<true>();

      t.mock.method(fs.promises, "rename", async (from: string, to: string): Promise<void> => {

        renameReached.resolve(true);

        await releaseRename.promise;

        return realRename.call(fs.promises, from, to);
      });

      // Fire the size check so a trim enqueues on the write chain and runs into the parked rename.
      for(let i = 0; i < 100; i++) {

        writeLogEntry("info", "Filler " + String(i), null);
      }

      await renameReached.promise;

      // Buffer the marker while the trim is parked. This entry is exactly what a shutdown without the drain would append into the doomed file.
      const marker = "DRAIN_MARKER_lands_after_the_trim";

      writeLogEntry("info", marker + ".", null);

      let settled = false;

      const shutdownPromise = shutdownFileLogger().then(() => { settled = true; });

      // Give shutdown several turns to run. It cannot get past the drain while the rename is parked, so it must still be pending here.
      for(let i = 0; i < 5; i++) {

        // eslint-disable-next-line no-await-in-loop
        await new Promise((resolve) => setImmediate(resolve));
      }

      assert.equal(settled, false, "shutdown is still waiting while the trim's rename is in flight");

      releaseRename.resolve(true);

      await shutdownPromise;

      assert.equal(settled, true, "shutdown settles once the trim completes");

      const finalContent = await readFile(logPath, "utf-8");

      assert.match(finalContent, new RegExp(marker), "the final flush landed after the rename, so the marker is on disk");
      assert.ok(finalContent.length < seedContent.length, "the trim genuinely ran - the file is smaller than the seed");
    });
  });
});
