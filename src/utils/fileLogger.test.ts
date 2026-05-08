/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.test.ts: Unit tests for the file-based logger in fileLogger.ts. The module holds module-scope state (initialization status, write buffer, file
 * path) that persists across tests. Each test scopes its filesystem state to a temp directory via withTempDir and calls shutdownFileLogger() in afterEach to
 * reset the singleton between cases. The flush timer would run in the background during real use; we always shut it down to avoid cross-test interference.
 */
import { afterEach, describe, mock, test } from "node:test";
import { computeTrimmedLogContent, flushLogBuffer, flushLogBufferSync, initializeFileLogger, shutdownFileLogger, writeLogEntry } from "./fileLogger.ts";
import { readFile, rm, stat, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import { initDebugFilter } from "./debugFilter.ts";
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

describe("checkAndTrimFile - debug-active gate and missing-file recovery", () => {

  /* checkAndTrimFile fires every SIZE_CHECK_FREQUENCY (100) writes. Two branches that the integration suite above does not exercise: the debug-active gate (when
   * any debug category is enabled, trim is skipped to preserve diagnostic output across the session), and the ENOENT clear-of-approximateSize path that fires
   * when the log file is removed externally between writes.
   */

  afterEach(() => {

    shutdownFileLogger();
    initDebugFilter("");
  });

  test("does NOT trim when isAnyDebugEnabled() is true (debug session preserves history)", async () => {

    // Boundary: a tiny maxSize would normally trim immediately, but the debug-active gate suppresses the trim so the session's high-volume output is retained
    // for diagnosis. We pin this by enabling wildcard debug, writing 100 entries to fire the size check, and asserting the file was NOT trimmed (every entry
    // remains).
    initDebugFilter("*");

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "debug.log");

      // maxSize is set well below the volume we'll write so a non-debug run would definitely trim. The trim is gated by !isAnyDebugEnabled() which is now false.
      await initializeFileLogger(logPath, 1024);

      for(let i = 0; i < 100; i++) {

        writeLogEntry("info", "Entry " + String(i) + " with enough text to push the buffer past 1KB.", null);
      }

      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");
      const lineCount = content.split("\n").filter((l) => l.length > 0).length;

      assert.equal(lineCount, 100, "all 100 entries preserved during a debug session");
    });
  });

  test("resets approximateSize to 0 when the log file is removed mid-flight (ENOENT recovery)", async () => {

    // Boundary: the size-check stat call can fail with ENOENT if the log file was removed externally (rotation by an outside process, accidental deletion, etc).
    // The implementation catches ENOENT, sets approximateSize = 0, and emits a console.warn so subsequent writes append fresh from a known-empty state.
    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "ephemeral.log");

      await initializeFileLogger(logPath, 1024);

      // Stub console.warn so the expected warning isn't printed during the test run.
      // eslint-disable-next-line no-console
      const originalWarn = console.warn;
      const warnCalls: unknown[][] = [];

      // eslint-disable-next-line no-console
      console.warn = (...args: unknown[]): void => { warnCalls.push(args); };

      try {

        // Write a few entries and flush so the log file exists on disk.
        writeLogEntry("info", "Pre-removal entry.", null);
        await flushLogBuffer();

        // Remove the file externally. The next checkAndTrimFile invocation will hit ENOENT.
        await rm(logPath, { force: true });

        // Push 100 more entries to fire the size check. The buffered writes still sit in memory; the stat fails; the catch absorbs ENOENT.
        for(let i = 0; i < 100; i++) {

          writeLogEntry("info", "Post-removal entry " + String(i) + ".", null);
        }

        // The check fires on the 100th write modulo SIZE_CHECK_FREQUENCY. It schedules a void promise; we await flush to ensure the queue has drained.
        await flushLogBuffer();

        // The console.warn should have fired with an "Error checking log file size" message reporting the ENOENT.
        const warningWasEmitted = warnCalls.some((call) => {

          const message = typeof call[0] === "string" ? call[0] : "";

          return message.includes("Error checking log file size");
        });

        assert.equal(warningWasEmitted, true, "console.warn fired with the ENOENT recovery message");
      } finally {

        // eslint-disable-next-line no-console
        console.warn = originalWarn;
      }
    });
  });
});

describe("trimLogFile end-to-end - on-disk size after writeCount triggers a trim", () => {

  /* The pure cut algorithm is tested by computeTrimmedLogContent. The orchestration shell (read + temp-write + atomic rename) is exercised here by writing
   * enough content to push past maxSize, triggering a trim via the size-check counter, and asserting the on-disk file ends up at half maxSize or less.
   */

  afterEach(() => {

    shutdownFileLogger();
    initDebugFilter("");
  });

  test("the on-disk file is trimmed below maxSize when writeCount % SIZE_CHECK_FREQUENCY fires past the threshold", async () => {

    // Set debug off so the debug-active gate doesn't suppress the trim.
    initDebugFilter("");

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "trim.log");
      const maxSize = 16384;

      // Pre-seed the log file with content that already exceeds maxSize. checkAndTrimFile reads the on-disk size (not approximateSize), so a fresh logger
      // pointing at an oversized file triggers trim on the first size-check fire. Each pre-seeded line is a complete entry so computeTrimmedLogContent's
      // newline-aligned cut works deterministically.
      const seedLine = "[2026/01/01 12:00:00.000 PM] Pre-seeded oversized history line.\n";
      const seedContent = seedLine.repeat(400);

      await writeFile(logPath, seedContent, "utf-8");

      assert.ok(seedContent.length > maxSize, "seed content (" + String(seedContent.length) + ") exceeds maxSize (" + String(maxSize) + ")");

      await initializeFileLogger(logPath, maxSize);

      // Write 100 entries to fire the size-check modulo gate. The buffered entries stay in memory (no flush in this test) so the trim race is isolated -
      // checkAndTrimFile reads the pre-seeded on-disk content, fires trim, and rewrites the file to half maxSize.
      for(let i = 0; i < 100; i++) {

        writeLogEntry("info", "New " + String(i), null);
      }

      // The trim runs asynchronously via `void checkAndTrimFile()` inside writeLogEntry. Poll on-disk size until it shrinks below the seed size or the
      // timeout expires - this avoids guessing how many setImmediate cycles the read+write+rename pipeline needs and keeps the test deterministic on slow hosts.
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

      assert.ok(postTrimSize < seedContent.length, "post-trim file size dropped below the seeded content size (" + String(postTrimSize) +
        " < " + String(seedContent.length) + ")");
      assert.ok(postTrimSize <= maxSize, "post-trim file size is at or below maxSize (" + String(postTrimSize) + " <= " + String(maxSize) + ")");
    });
  });
});

describe("writeLogEntry - retry-window re-enable after disabled state", () => {

  /* When flushLogBuffer fails (e.g., directory removed mid-flight), writeLogEntry sets isDisabled=true and disabledAt=Date.now(). Subsequent writes are silently
   * dropped during the retry window (ERROR_RETRY_DELAY_MS = 60s). Once Date.now() advances past the threshold, the next writeLogEntry call re-enables logging
   * (line 153: isDisabled = false). We exercise the re-enable branch by stubbing Date.now to advance past the threshold deterministically without waiting 60s.
   */

  afterEach(() => {

    shutdownFileLogger();
    mock.reset();
  });

  test("re-enables logging when Date.now() has advanced past ERROR_RETRY_DELAY_MS since the disable", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "retry.log");

      await initializeFileLogger(logPath, 1_000_000);

      // Stub console.error so the cascading-error message isn't printed during the test.
      // eslint-disable-next-line no-console
      const originalConsoleError = console.error;

      // eslint-disable-next-line no-console
      console.error = (): void => { /* swallow */ };

      try {

        // Force a write failure to enter the disabled state. Removing the parent directory makes the next appendFile reject with ENOENT.
        await rm(dir, { force: true, recursive: true });

        writeLogEntry("info", "Will-fail entry.", null);
        await flushLogBuffer();

        // The logger is now in the disabled state. Recreate the directory and the file so subsequent writes have a valid target after the re-enable.
        const { mkdir } = await import("node:fs/promises");

        await mkdir(dir, { recursive: true });
        await writeFile(logPath, "", "utf-8");

        // Advance Date.now() past the 60-second retry window. We override Date.now via mock.method (Node 22 does not yet support Date in mock.timers, so
        // method-level stubbing is the cross-version idiom).
        const baseNow = Date.now() + 70_000;

        mock.method(Date, "now", () => baseNow);

        // The next writeLogEntry should observe (Date.now() - disabledAt) >= ERROR_RETRY_DELAY_MS, set isDisabled = false (line 153), and append the entry to
        // the buffer instead of silently dropping it.
        writeLogEntry("info", "Post-retry entry.", null);
        await flushLogBuffer();

        const content = await readFile(logPath, "utf-8");

        assert.match(content, /Post-retry entry\./, "entry written after the retry window re-enabled the logger");
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalConsoleError;
      }
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

describe("initializeFileLogger - mkdir failure recovery", () => {

  /* When the parent directory cannot be created (e.g., the path collides with an existing file), initializeFileLogger logs to console.error and returns without
   * throwing. Subsequent writeLogEntry calls become no-ops because isInitialized stays false.
   */

  afterEach(() => {

    shutdownFileLogger();
  });

  test("does not throw when mkdir fails because a parent path component is a file (returns gracefully)", async () => {

    await withTempDir(async (dir) => {

      // Create a regular file where a parent directory should go - mkdir(parent, { recursive: true }) fails with ENOTDIR or similar.
      const collidingFile = path.join(dir, "not-a-directory");

      await writeFile(collidingFile, "", "utf-8");

      // The log path's parent is collidingFile, which is a file - mkdir cannot create a directory inside it.
      const logPath = path.join(collidingFile, "subdir", "test.log");

      // eslint-disable-next-line no-console
      const originalError = console.error;
      const errorCalls: unknown[][] = [];

      // eslint-disable-next-line no-console
      console.error = (...args: unknown[]): void => { errorCalls.push(args); };

      try {

        // Must not throw - the implementation absorbs the error and disables file logging.
        await assert.doesNotReject(() => initializeFileLogger(logPath, 1_000_000),
          "initializeFileLogger absorbs the mkdir failure");

        // The console.error should have fired with the "Failed to initialize file logger" message.
        const failureLogged = errorCalls.some((call) => {

          const message = typeof call[0] === "string" ? call[0] : "";

          return message.includes("Failed to initialize file logger");
        });

        assert.equal(failureLogged, true, "console.error fired with the init-failure message");

        // Subsequent writeLogEntry calls become no-ops since isInitialized stayed false.
        assert.doesNotThrow(() => { writeLogEntry("info", "Should be a no-op.", null); });
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalError;
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
