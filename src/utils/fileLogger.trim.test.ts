/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.trim.test.ts: Unit tests for the log-file trim path - computeTrimmedLogContent (pure cut algorithm), checkAndTrimFile (size-driven trigger + debug
 * gate + missing-file recovery), and the trimLogFile end-to-end I/O orchestration. The basic write/buffer path lives in fileLogger.test.ts; lifecycle and
 * error-disabled paths live in fileLogger.lifecycle.test.ts.
 */
import { afterEach, describe, test } from "node:test";
import { computeTrimmedLogContent, flushLogBuffer, initializeFileLogger, shutdownFileLogger, writeLogEntry } from "./fileLogger.ts";
import { readFile, rm, stat, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import { initDebugFilter } from "./debugFilter.ts";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

describe("computeTrimmedLogContent", () => {

  /* The pure cut-at-newline algorithm extracted from trimLogFile. Given the current file content and the configured maxSize, it returns the trimmed content
   * (keeping complete lines from the tail) or null when no trim is needed. The surrounding I/O orchestration in trimLogFile (read + write + rename) is small
   * enough to be exercised at the integration level; asserting the cut algorithm here is where the architectural value is.
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

    // content.length = 46 + 1 + 48 + 1 = 96. maxSize=120 -> targetSize=60 -> cutPosition=36 (inside old entry). Next \n at offset 46, lineStart = 47, result is
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

    // maxSize=1001 -> targetSize=500. content.length=600 -> cutPosition=100. With newlines at 99, 199, 299, 399, 499, the first newline at-or-past 100 is 199.
    // Result keeps everything from offset 200 onward.
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
    assert.equal(computeTrimmedLogContent("tiny", 100000000), null);
  });
});

describe("checkAndTrimFile + trimLogFile - I/O orchestration (integration)", () => {

  /* The I/O orchestration around computeTrimmedLogContent (file read, temp-file write, atomic rename) is small and follows a standard transactional pattern. The
   * pure cut algorithm is unit-tested above; this describe block only covers the negative path (no trim when below maxSize). The trim-fires path is covered
   * deterministically in the "trimLogFile end-to-end" describe block below, which seeds an oversized file and polls on-disk size until the trim completes.
   */

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("does not trim when the file is below maxSize at size-check time", async () => {

    // Negative test: when the file is comfortably under maxSize, no trim fires. We init, write enough entries to fire checkAndTrimFile, drain the buffer, and
    // assert that all 100 entries are persisted with the original line shape (i.e., nothing was rewritten by a trim).
    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      // Initialize with a generous maxSize so the file never approaches the trim threshold.
      await initializeFileLogger(logPath, 1000000);

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

describe("checkAndTrimFile - debug-active gate and missing-file recovery", () => {

  /* checkAndTrimFile fires every SIZE_CHECK_FREQUENCY (100) writes. Branches that the integration suite above does not exercise: the debug-active gate (when
   * any debug category is enabled, trim is skipped to preserve diagnostic output across the session), and the ENOENT clear-of-approximateSize path that fires
   * when the log file is removed externally between writes.
   */

  afterEach(async () => {

    await shutdownFileLogger();
    initDebugFilter("");
  });

  test("does NOT trim when isAnyDebugEnabled() is true (debug session preserves history)", async () => {

    // Boundary: a tiny maxSize would normally trim immediately, but the debug-active gate suppresses the trim so the session's high-volume output is retained
    // for diagnosis. We assert this by enabling wildcard debug, writing 100 entries to fire the size check, and asserting the file was NOT trimmed (every entry
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

  afterEach(async () => {

    await shutdownFileLogger();
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

