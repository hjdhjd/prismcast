/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.test.ts: Unit tests for the file-based logger's basic write and buffer paths - initializeFileLogger + writeLogEntry happy paths, flushLogBuffer,
 * flushLogBufferSync, the existing-file initialization branch, and the periodic flushTimer interval. The trim path lives in fileLogger.trim.test.ts; lifecycle
 * and error-recovery paths live in fileLogger.lifecycle.test.ts.
 *
 * The module holds module-scope state (initialization status, write buffer, file path) that persists across tests. Each test scopes its filesystem state to a
 * temp directory via withTempDir and calls shutdownFileLogger() in afterEach to reset the singleton between cases. The flush timer would run in the background
 * during real use; we always shut it down to avoid cross-test interference.
 */
import { afterEach, describe, mock, test } from "node:test";
import { flushLogBuffer, flushLogBufferSync, initializeFileLogger, shutdownFileLogger, writeLogEntry } from "./fileLogger.ts";
import { readFile, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

describe("initializeFileLogger and writeLogEntry", () => {

  afterEach(() => {

    shutdownFileLogger();
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

  test("writeLogEntry is a no-op when the file logger has not been initialized", () => {

    // Negative test: writes that happen before init must not crash. shutdownFileLogger() (in afterEach) returns to the uninitialized state, so calling
    // writeLogEntry now should be silently dropped.
    shutdownFileLogger();

    assert.doesNotThrow(() => {

      writeLogEntry("info", "ignored", null);
    });
  });
});

describe("flushLogBuffer", () => {

  afterEach(() => {

    shutdownFileLogger();
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

    shutdownFileLogger();
    await assert.doesNotReject(() => flushLogBuffer());
  });
});

describe("flushLogBufferSync", () => {

  afterEach(() => {

    shutdownFileLogger();
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

  test("does not throw when called before init", () => {

    shutdownFileLogger();
    assert.doesNotThrow(() => { flushLogBufferSync(); });
  });
});

describe("initializeFileLogger - existing-file branch", () => {

  afterEach(() => {

    shutdownFileLogger();
  });

  test("preserves existing file content when the log file already exists", async () => {

    // Boundary: when the log file already has content from a previous run, init must NOT truncate it - it should stat the existing size and append new entries
    // alongside what was already there. This pins the stat-existing-file branch (line 97-99 in fileLogger.ts).
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

  afterEach(() => {

    shutdownFileLogger();
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

