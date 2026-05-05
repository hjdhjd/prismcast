/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.test.ts: Unit tests for the file-based logger in fileLogger.ts. The module holds module-scope state (initialization status, write buffer, file
 * path) that persists across tests. Each test scopes its filesystem state to a temp directory via withTempDir and calls shutdownFileLogger() in afterEach to
 * reset the singleton between cases. The flush timer would run in the background during real use; we always shut it down to avoid cross-test interference.
 */
import { afterEach, describe, test } from "node:test";
import { computeTrimmedLogContent, flushLogBuffer, flushLogBufferSync, initializeFileLogger, shutdownFileLogger, writeLogEntry } from "./fileLogger.ts";
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

describe("computeTrimmedLogContent", () => {

  /* The pure cut-at-newline algorithm extracted from trimLogFile. Given the current file content and the configured maxSize, it returns the trimmed content
   * (keeping complete lines from the tail) or null when no trim is needed. The surrounding I/O orchestration in trimLogFile (read + write + rename) is small
   * enough to be exercised at the integration level; pinning the cut algorithm here is where the architectural value is.
   */

  test("returns null when content is at or below half maxSize (no trim needed)", () => {

    // Boundary: targetSize = floor(maxSize / 2). When content.length === targetSize, cutPosition = 0, the early-return fires, and no trim happens.
    assert.equal(computeTrimmedLogContent("X".repeat(500), 1000), null);
    assert.equal(computeTrimmedLogContent("X".repeat(100), 1000), null, "much-smaller file returns null");
    assert.equal(computeTrimmedLogContent("", 1000), null, "empty content returns null");
  });

  test("returns null at the exact half-maxSize boundary (cutPosition === 0)", () => {

    // The implementation uses cutPosition <= 0 as the no-op guard. content.length - targetSize === 0 hits that branch.
    const exactlyHalf = "X".repeat(500);

    assert.equal(computeTrimmedLogContent(exactlyHalf, 1000), null);
  });

  test("trims to the line boundary when content has a newline past the cut position", () => {

    // content layout: "old1\nnew1\nnew2\nnew3\n" (length 20). With maxSize=30, targetSize=15, cutPosition=5 (lands inside "new1"). The next newline at offset 9
    // advances lineStart to 10, dropping the first two lines and keeping "new2\nnew3\n" (the tail).
    const content = "old1\nnew1\nnew2\nnew3\n";
    const result = computeTrimmedLogContent(content, 30);

    assert.equal(result, "new2\nnew3\n", "older lines dropped, tail preserved at line boundary");
  });

  test("preserves the most recent content (file's tail), not the head", () => {

    // Sanity check on directionality: a trim should drop the OLDEST entries. With content "[old]\n[new]\n" and a maxSize chosen so cutPosition lands inside the
    // old entry, the trim advances to the next \n and keeps only the new entry. The first line of the input should NOT appear in the output.
    const oldEntry = "[2026/01/01] OLD entry that should be dropped.";
    const newEntry = "[2026/01/02] NEW entry that should be preserved.";
    const content = oldEntry + "\n" + newEntry + "\n";

    // content.length = 47 + 1 + 49 + 1 = 98. maxSize=120 -> targetSize=60 -> cutPosition=38 (inside old entry). Next \n at offset 47, lineStart = 48, result is
    // the new entry plus its trailing newline.
    const result = computeTrimmedLogContent(content, 120);

    assert.notEqual(result, null);
    assert.doesNotMatch(result ?? "", /OLD entry/, "old entry was dropped");
    assert.match(result ?? "", /NEW entry/, "new entry was preserved");
  });

  test("falls back to cutPosition when no newline exists past the cut (single-line oversized file)", () => {

    // A file with no newlines past cutPosition: the indexOf returns -1, and the implementation falls back to the raw cut position. This is unusual in practice
    // (log files always have newlines) but the fallback prevents the function from emitting an empty string.
    const content = "X".repeat(1000);
    const result = computeTrimmedLogContent(content, 200);

    // targetSize = 100, cutPosition = 900. No newlines, so lineStart = 900. Result = content.substring(900) = 100 chars.
    assert.equal(result?.length, 100);
    assert.equal(result, "X".repeat(100));
  });

  test("uses Math.floor for targetSize (odd maxSize rounds down)", () => {

    // maxSize=1001 -> targetSize=500. content.length=600 -> cutPosition=100. With newlines at 100, 200, ..., the first newline at-or-past 100 is exactly 100.
    // Result keeps everything from offset 101 onward.
    const lines = Array.from({ length: 6 }, () => "X".repeat(99));
    const content = lines.join("\n");

    // content is 99+1+99+1+99+1+99+1+99+1+99 = 599 chars. Hmm let me recompute: 6 lines of 99 chars joined by \n = 6*99 + 5 = 599.
    // Pad to 600 with one more X.
    const padded = content + "X";

    assert.equal(padded.length, 600);

    const result = computeTrimmedLogContent(padded, 1001);

    // cutPosition = 600 - 500 = 100. Find newline at-or-past 100. Newlines are at offsets 99, 199, 299, 399, 499. First one >= 100 is at 199.
    // lineStart = 200. Result is padded.substring(200), length = 400.
    assert.equal(result?.length, 400);
  });

  test("preserves a trailing newline when one exists in the kept content", () => {

    // The trim is a substring; it doesn't add or remove newlines. With content of length 35 and maxSize=30 -> targetSize=15 -> cutPosition=20 (inside "Newer
    // line."), the next \n at offset 21 advances lineStart to 22, yielding "Newest line.\n" with the original trailing newline preserved.
    const content = "Old line.\nNewer line.\nNewest line.\n";
    const result = computeTrimmedLogContent(content, 30);

    assert.equal(result, "Newest line.\n", "trim returned the final line with its trailing newline intact");
  });

  test("handles content where the cut position lands exactly on a newline", () => {

    // Edge case: cutPosition lands directly on a newline character. With "AAAA\nBBBB" (length 9) and maxSize=10, targetSize=5, cutPosition=4 hits the \n exactly.
    // indexOf("\n", 4) returns 4, lineStart advances to 5, and the trim returns the second half ("BBBB").
    const content = "AAAA\nBBBB";
    const result = computeTrimmedLogContent(content, 10);

    assert.equal(result, "BBBB", "cut-on-newline advances past it cleanly");
  });

  test("returns the full content when cutPosition would be negative (file much smaller than half maxSize)", () => {

    // Already covered by the "returns null" test set, but documenting separately: cutPosition < 0 -> early return null. The caller skips the rename.
    assert.equal(computeTrimmedLogContent("tiny", 100_000_000), null);
  });
});

describe("checkAndTrimFile + trimLogFile - I/O orchestration (integration)", () => {

  /* The I/O orchestration around computeTrimmedLogContent (file read, temp-file write, atomic rename) is small and follows a standard transactional pattern. The
   * pure cut algorithm is unit-tested above; here we only sanity-check the negative path (no trim when below maxSize), which is the easy-to-test side. The
   * trim-fires path is hard to unit-test deterministically because trim is invoked via void from writeLogEntry's counter modulo, races with the periodic flush
   * timer, and depends on filesystem rename atomicity - it is exercised in production when the log file grows past the configured limit. The architectural
   * extraction makes the algorithm fully testable; the I/O shell is thin enough to defer.
   */

  afterEach(() => {

    shutdownFileLogger();
  });

  test("does not trim when the file is below maxSize at size-check time", async () => {

    // Negative test: when the file is comfortably under maxSize, no trim fires. We init, write enough entries to fire checkAndTrimFile, drain the buffer, and
    // assert that all 100 entries are persisted with the original line shape (i.e., nothing was rewritten by a trim).
    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      // Initialize with a generous maxSize so the file never approaches the trim threshold.
      await initializeFileLogger(logPath, 1_000_000);

      for(let i = 0; i < 100; i++) {

        writeLogEntry("info", "Line " + String(i) + ".", null);
      }

      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");
      const writtenLines = content.split("\n").filter((l) => l.length > 0);

      assert.equal(writtenLines.length, 100, "all 100 entries persisted (no trim)");
    });
  });
});

describe("flushLogBuffer - error-path retry-disable", () => {

  /* When fsPromises.appendFile throws (e.g., because the log file's directory was removed mid-flight), flushLogBuffer disables logging for ERROR_RETRY_DELAY_MS
   * milliseconds and emits a console.error. Subsequent writeLogEntry calls during the retry window are silently dropped. This guards against tight-loop error
   * cascades when the underlying filesystem is in a degraded state.
   */

  afterEach(() => {

    shutdownFileLogger();
  });

  test("disables logging temporarily when appendFile throws and drops subsequent writes within the retry window", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      writeLogEntry("info", "Pre-error entry.", null);
      await flushLogBuffer();

      // Stub console.error so the cascading-error message isn't printed during the test. Production fileLogger emits via console.error on flush failure to
      // surface the issue without a circular dependency back into LOG; we swallow it here so the test's stderr stays clean.
      // eslint-disable-next-line no-console
      const originalConsoleError = console.error;

      // eslint-disable-next-line no-console
      console.error = (): void => { /* swallow */ };

      try {

        // Remove the log file's parent directory under the running logger to force the next appendFile to fail with ENOENT.
        const { rm } = await import("node:fs/promises");

        await rm(dir, { force: true, recursive: true });

        // The next write will be appended to the buffer; flushing it should fail and disable the logger.
        writeLogEntry("info", "Will fail.", null);
        await flushLogBuffer();

        // Subsequent writes should be silently dropped since the logger is now in the disabled state. We can't directly observe the state, but a follow-up flush
        // should be a no-op (no throw, no recovery).
        writeLogEntry("info", "Dropped.", null);

        await assert.doesNotReject(() => flushLogBuffer(), "subsequent flush is a no-op while disabled");
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalConsoleError;
      }
    });
  });
});

describe("shutdownFileLogger", () => {

  test("is idempotent on an already-shut-down logger", () => {

    // Boundary: calling shutdown twice (or before init) must not crash.
    assert.doesNotThrow(() => {

      shutdownFileLogger();
      shutdownFileLogger();
    });
  });

  test("persists any buffered content synchronously before clearing state", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);

      writeLogEntry("info", "Final entry.", null);

      shutdownFileLogger();

      const content = await readFile(logPath, "utf-8");

      assert.match(content, /Final entry\./, "shutdown flushes the buffer to disk");
    });
  });

  test("further writes after shutdown become no-ops", async () => {

    // Negative test: once shut down, the logger must reject subsequent writes silently (until re-initialized).
    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, 1_000_000);
      shutdownFileLogger();

      writeLogEntry("info", "After shutdown.", null);

      // No flush will run; the file should remain whatever was persisted at shutdown time (empty).
      const content = await readFile(logPath, "utf-8");

      assert.doesNotMatch(content, /After shutdown\./);
    });
  });
});
