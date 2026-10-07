/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.trim.test.ts: Unit tests for the log-file trim path - computeTrimmedLogContent (pure byte cut), checkAndTrimFile (size-driven trigger + debug
 * gate + missing-file recovery), the trimLogFile end-to-end I/O orchestration, setMaxLogSize (a saved limit's immediate check), and the guard that starts a
 * size check or a trim only while the logger is open on a file accepting writes. The basic write/buffer path lives in fileLogger.test.ts; lifecycle and
 * error-disabled paths live in fileLogger.lifecycle.test.ts.
 */
import { TestClock, settle } from "homebridge-plugin-utils/testing";
import { afterEach, beforeEach, describe, test } from "node:test";
import { computeTrimmedLogContent, flushLogBuffer, initializeFileLogger, setMaxLogSize, shutdownFileLogger, writeLogEntry } from "./fileLogger.ts";
import { readFile, rm, stat, writeFile } from "node:fs/promises";
import type { Nullable } from "../types/index.ts";
import type { TestContext } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { initDebugFilter } from "./debugFilter.ts";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

describe("computeTrimmedLogContent", () => {

  /* The pure byte cut extracted from trimLogFile. Given the file's bytes and the configured maxSize, it returns the trimmed bytes (keeping complete lines from
   * the tail) or null when no trim is needed. The surrounding I/O orchestration in trimLogFile (read + write + rename) is small enough to be exercised at the
   * integration level; asserting the cut here is where the architectural value is. The ASCII rows state their content as text, so each byte offset below is
   * also a character offset, and the multi-byte rows compare exact bytes.
   */

  // Runs the cut over a string's UTF-8 bytes and decodes what it keeps, so an ASCII row states its content and its expected tail as text.
  const trimText = (content: string, maxSize: number): Nullable<string> => computeTrimmedLogContent(Buffer.from(content), maxSize)?.toString("utf8") ?? null;

  test("returns null when content is at or below half maxSize (no trim needed)", () => {

    // Boundary: targetSize = floor(maxSize / 2). When the byte length equals targetSize, cutPosition = 0, the early-return fires, and no trim happens.
    assert.equal(trimText("X".repeat(500), 1000), null);
    assert.equal(trimText("X".repeat(100), 1000), null, "much-smaller file returns null");
    assert.equal(trimText("", 1000), null, "empty content returns null");
  });

  test("returns null at the exact half-maxSize boundary (cutPosition === 0)", () => {

    // The implementation uses cutPosition <= 0 as the no-op guard. A byte length minus targetSize of zero hits that branch.
    const exactlyHalf = "X".repeat(500);

    assert.equal(trimText(exactlyHalf, 1000), null);
  });

  test("trims to the line boundary when content has a newline past the cut position", () => {

    // content layout: "old1\nnew1\nnew2\nnew3\n" (20 bytes). With maxSize=30, targetSize=15, cutPosition=5 (lands inside "new1"). The first newline byte at or
    // past the cut is at offset 9, so the kept tail starts at 10, dropping the first two lines and keeping "new2\nnew3\n" (the tail).
    const content = "old1\nnew1\nnew2\nnew3\n";
    const result = trimText(content, 30);

    assert.equal(result, "new2\nnew3\n", "older lines dropped, tail preserved at line boundary");
  });

  test("preserves the most recent content (file's tail), not the head", () => {

    // Sanity check on directionality: a trim should drop the OLDEST entries. With content "[old]\n[new]\n" and a maxSize chosen so cutPosition lands inside the
    // old entry, the kept tail starts after the next newline byte and holds only the new entry. The first line of the input should NOT appear in the output.
    const oldEntry = "[2026/01/01] OLD entry that should be dropped.";
    const newEntry = "[2026/01/02] NEW entry that should be preserved.";
    const content = oldEntry + "\n" + newEntry + "\n";

    // The content is 46 + 1 + 48 + 1 = 96 bytes. maxSize=120 -> targetSize=60 -> cutPosition=36 (inside the old entry). The next newline byte is at offset 46,
    // so the kept tail starts at 47: the new entry plus its trailing newline.
    const result = trimText(content, 120);

    assert.notEqual(result, null);
    assert.doesNotMatch(result ?? "", /OLD entry/, "old entry was dropped");
    assert.match(result ?? "", /NEW entry/, "new entry was preserved");
  });

  test("falls back to cutPosition when no newline exists past the cut (single-line oversized file)", () => {

    // A file with no newline past cutPosition keeps everything from the cut, advanced past any continuation bytes, which ASCII never has. This is unusual in
    // practice (log files always have newlines) but it keeps the trim from emitting an empty result.
    const content = "X".repeat(1000);
    const result = trimText(content, 200);

    // targetSize = 100, cutPosition = 900. No newline, and byte 900 begins a character, so the kept tail is the last 100 bytes.
    assert.equal(result?.length, 100);
    assert.equal(result, "X".repeat(100));
  });

  test("uses Math.floor for targetSize (odd maxSize rounds down)", () => {

    // maxSize=1001 -> targetSize=500. Six lines of 99 bytes joined by newlines are 6 * 99 + 5 = 599 bytes, and one more byte pads the content to 600, so
    // cutPosition=100.
    const lines = Array.from({ length: 6 }, () => "X".repeat(99));
    const content = lines.join("\n");
    const padded = content + "X";

    assert.equal(padded.length, 600);

    const result = trimText(padded, 1001);

    // The newline bytes sit at offsets 99, 199, 299, 399 and 499, and the first at or past 100 is 199, so the kept tail starts at 200 and is 400 bytes long.
    assert.equal(result?.length, 400);
  });

  test("preserves a trailing newline when one exists in the kept content", () => {

    // The kept tail is a subarray; it doesn't add or remove newlines. With 35 bytes and maxSize=30 -> targetSize=15 -> cutPosition=20 (inside "Newer line."),
    // the next newline byte at offset 21 moves the start to 22, yielding "Newest line.\n" with the original trailing newline preserved.
    const content = "Old line.\nNewer line.\nNewest line.\n";
    const result = trimText(content, 30);

    assert.equal(result, "Newest line.\n", "trim returned the final line with its trailing newline intact");
  });

  test("handles content where the cut position lands exactly on a newline", () => {

    // Edge case: cutPosition lands directly on a newline byte. With "AAAA\nBBBB" (9 bytes) and maxSize=10, targetSize=5, cutPosition=4 hits the newline
    // exactly, so the kept tail starts at 5 and the trim returns the second half ("BBBB").
    const content = "AAAA\nBBBB";
    const result = trimText(content, 10);

    assert.equal(result, "BBBB", "cut-on-newline advances past it cleanly");
  });

  test("returns the full content when cutPosition would be negative (file much smaller than half maxSize)", () => {

    // Already covered by the "returns null" test set, but documenting separately: cutPosition < 0 -> early return null. The caller skips the rename.
    assert.equal(trimText("tiny", 100000000), null);
  });

  test("measures multi-byte text in bytes and keeps the tail after the first newline byte past the cut", () => {

    // Each character here is 3 bytes, so each line is 10 bytes and the content is 40. maxSize=30 -> targetSize=15 -> cutPosition=25, and the first newline
    // byte at or past the cut is at 29, so the kept tail is exactly the last line. A cut computed from the decoded string's length would keep three lines.
    const result = computeTrimmedLogContent(Buffer.from("中中中\n".repeat(4)), 30);

    assert.deepEqual(result, Buffer.from("中中中\n"), "the kept tail is exactly the last line's bytes");
  });

  test("advances a cut inside a multi-byte character to the next character's lead byte when no newline follows", () => {

    // One ASCII byte and ten 3-byte characters are 31 bytes. maxSize=20 -> targetSize=10 -> cutPosition=21, the last byte of the seventh character, and no
    // newline follows, so the cut advances to the eighth character's lead byte at 22 and the kept tail is exactly the last three characters.
    const result = computeTrimmedLogContent(Buffer.from("a" + "中".repeat(10)), 20);

    assert.deepEqual(result, Buffer.from("中中中"), "the kept tail opens on a whole character");
  });

  test("keeps nothing when the cut advances to the end of a buffer that stops inside a multi-byte character", () => {

    // Six ASCII bytes and the first two bytes of a 3-byte character are 8 bytes, the end of a file whose last write stopped inside a character. maxSize=2 ->
    // targetSize=1 -> cutPosition=7, a continuation byte with no newline after it, so the cut advances to the buffer's end and the kept tail is empty.
    const result = computeTrimmedLogContent(Buffer.from("abcdef中").subarray(0, 8), 2);

    assert.deepEqual(result, Buffer.alloc(0), "the advance stops at the buffer's end and keeps nothing");
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

  /* checkAndTrimFile fires every SIZE_CHECK_FREQUENCY writes. Branches that the integration suite above does not exercise: the debug-active gate (when
   * any debug category is enabled, trim is skipped to preserve diagnostic output across the session), and the ENOENT path that warns and keeps running when
   * the log file is removed externally between writes.
   */

  afterEach(async () => {

    await shutdownFileLogger();
    initDebugFilter("");
  });

  test("does NOT trim when isAnyDebugEnabled() is true (debug session preserves history)", async (t) => {

    /* Boundary: the file is above maxSize, so the size check the row's last write fires would trim it, but the debug-active gate suppresses the trim so the
     * session's high-volume output is retained for diagnosis. The file is seeded above the limit before the writes, because the entries still sit in the buffer
     * when the check reads the size, so a file holding only those entries would never reach the trim. The row holds the check at its stat, releases it and lets
     * it decide before the flush, so a trim the check queued lands ahead of the flush and before the read.
     */
    initDebugFilter("*");

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "debug.log");

      await initializeFileLogger(logPath, 1024, new TestClock());
      await writeFile(logPath, SEED_CONTENT, "utf-8");

      const statSpy = spyOnStat(t, { parked: true });

      for(let i = 0; i < SIZE_CHECK_FREQUENCY; i++) {

        writeLogEntry("info", "Entry " + String(i) + " with enough text to push the buffer past 1KB.", null);
      }

      // The last write fired the size check, which has read the seeded file's size and waits before its decision.
      await statSpy.reached;
      statSpy.release();
      await settle();
      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");

      assert.ok(content.startsWith(SEED_FIRST_LINE), "the seeded history survived the size check");
      assert.equal(content.split("\n").filter((line) => line.includes("Entry ")).length, SIZE_CHECK_FREQUENCY, "every entry the row wrote is present");
    });
  });

  test("warns and keeps running when the log file is removed mid-flight (ENOENT at the size check)", async () => {

    // Boundary: the size-check stat call can fail with ENOENT if the log file was removed externally (rotation by an outside process, accidental deletion, etc).
    // The implementation catches the error and emits a console.warn; the next append recreates the file, so logging continues into an empty one.
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

        // The check fires on the write whose count is a multiple of SIZE_CHECK_FREQUENCY, as a voided promise whose stat does not go through the write chain.
        // Awaiting the flush is a settling point only: it drains the write chain, not the check, so the row relies on the check's stat settling within it.
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
   * enough content to push past maxSize, triggering a trim via the size-check counter, and asserting the on-disk file shrinks below the seed and ends at or
   * below maxSize.
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

      // Pre-seed the log file with content that already exceeds maxSize. checkAndTrimFile reads the on-disk size, so a fresh logger
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

// The logger's size-check frequency, restated here because it is private to fileLogger.ts. Each write-path row reaches the check on exactly this write.
const SIZE_CHECK_FREQUENCY = 100;

// The limits the setter rows move between. The seed below sits above the smaller and below the larger.
const SMALL_LIMIT = 4096;
const LARGE_LIMIT = 1000000;

// The seed's first line differs from every line after it, so a file that still opens with it is a file no trim has touched.
const SEED_FIRST_LINE = "[2026/01/01 12:00:00.000 PM] The first seeded line.\n";
const SEED_CONTENT = SEED_FIRST_LINE + "[2026/01/01 12:00:00.000 PM] A seeded history line.\n".repeat(200);

/**
 * A spy on fs.promises.stat, the object the logger reads through.
 */
interface StatSpy {

  // How many times the logger has called stat since the spy was installed.
  readonly calls: () => number;

  // Settles once a parked stat has read the file and is waiting for its release.
  readonly reached: Promise<true>;

  // Lets a parked stat return to the logger.
  readonly release: () => void;
}

/**
 * Installs a stat spy that counts its calls and returns the real stat. A parked spy reads the real stat first and then waits for the row's release, so a
 * release resumes the logger in microtasks rather than on a filesystem turn. The test context restores the real stat when the row ends.
 * @param t - The row's test context.
 * @param options - Spy options.
 * @param options.parked - Whether each stat waits for the row's release before it returns.
 * @returns The spy's call count, its reached signal, and its release.
 */
function spyOnStat(t: TestContext, { parked = false }: { parked?: boolean } = {}): StatSpy {

  const realStat = fs.promises.stat;
  const reached = Promise.withResolvers<true>();
  const release = Promise.withResolvers<true>();
  const spy = t.mock.method(fs.promises, "stat", async (file: fs.PathLike): Promise<fs.Stats> => {

    const stats = await realStat(file);

    if(parked) {

      reached.resolve(true);

      await release.promise;
    }

    return stats;
  });

  return { calls: (): number => spy.mock.callCount(), reached: reached.promise, release: (): void => { release.resolve(true); } };
}

describe("setMaxLogSize - a saved limit checks the open file at once", () => {

  /* The setter assigns the limit and runs the size check itself, so a saved smaller limit trims an oversized file without waiting for the periodic check. Each
   * row writes its file directly rather than through the logger, so no log entry lands between the setter and its assertion, and the debug filter is cleared,
   * because an active debug session skips every trim. The row on an active debug session is the one that turns the filter on, to hold the setter's check to
   * that skip.
   */

  beforeEach(() => {

    initDebugFilter("");
  });

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("a smaller limit trims a file above it to at most half of it", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "shrink.log");

      await initializeFileLogger(logPath, LARGE_LIMIT, new TestClock());
      await writeFile(logPath, SEED_CONTENT, "utf-8");
      await setMaxLogSize(SMALL_LIMIT);

      const { size } = await stat(logPath);

      assert.ok(size <= (SMALL_LIMIT / 2), "the file holds at most half the new limit (" + String(size) + " <= " + String(SMALL_LIMIT / 2) + ")");
    });
  });

  test("a larger limit leaves a file below it unchanged", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "grow.log");

      await initializeFileLogger(logPath, SMALL_LIMIT, new TestClock());
      await writeFile(logPath, SEED_CONTENT, "utf-8");
      await setMaxLogSize(LARGE_LIMIT);

      const { size } = await stat(logPath);

      assert.equal(size, Buffer.byteLength(SEED_CONTENT), "the file is the size the seed wrote");
    });
  });

  test("a smaller limit leaves a file above it untrimmed while debug logging is active", async (t) => {

    // An active debug session skips every trim, the setter's check included, so the session's history survives a saved smaller limit.
    initDebugFilter("*");

    try {

      await withTempDir(async (dir) => {

        const logPath = path.join(dir, "debug-shrink.log");

        await initializeFileLogger(logPath, LARGE_LIMIT, new TestClock());
        await writeFile(logPath, SEED_CONTENT, "utf-8");

        const renameSpy = t.mock.method(fs.promises, "rename");

        await setMaxLogSize(SMALL_LIMIT);

        const { size } = await stat(logPath);

        assert.equal(renameSpy.mock.callCount(), 0, "no trim renamed the file during the debug session");
        assert.equal(size, Buffer.byteLength(SEED_CONTENT), "the file is the size the seed wrote");
      });
    } finally {

      initDebugFilter("");
    }
  });
});

describe("the size check and the trim start only while the logger is open on a file accepting writes", () => {

  /* A trim that starts while the logger is closing lands on the write chain outside the drain shutdown awaits, and a paused file takes no size check. Each row
   * seeds its file above the smaller limit, and every wait is a signal the row controls or a promise that settles, so neither the guarded logger nor a logger
   * missing a guard leaves a row waiting on the test timeout. Each logger runs on a test clock, so no periodic flush lands inside a row.
   */

  beforeEach(() => {

    initDebugFilter("");
  });

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("a save while the logger is closing starts no size check", async (t) => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "closing-save.log");

      await initializeFileLogger(logPath, LARGE_LIMIT, new TestClock());
      await writeFile(logPath, SEED_CONTENT, "utf-8");

      const statSpy = spyOnStat(t);

      // The shutdown enters closing before its first await, so the setter after it meets a closing logger.
      const shutdown = shutdownFileLogger();

      await Promise.all([ shutdown, setMaxLogSize(SMALL_LIMIT) ]);

      assert.equal(statSpy.calls(), 0, "no size check started while the logger was closing");
      assert.ok(fs.readFileSync(logPath, "utf-8").startsWith(SEED_FIRST_LINE), "the file still opens with the seed's first line");
    });
  });

  test("the write path's size check starts nothing while the logger is closing", async (t) => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "closing-write.log");

      await initializeFileLogger(logPath, SMALL_LIMIT, new TestClock());
      await writeFile(logPath, SEED_CONTENT, "utf-8");

      const statSpy = spyOnStat(t);

      // The write count starts from zero, because every shutdown resets it, so these writes stop one short of the size check and the write after the shutdown
      // below is the one that reaches it.
      for(let entry = 1; entry < SIZE_CHECK_FREQUENCY; entry++) {

        writeLogEntry("info", "Entry " + String(entry) + " written while the logger is open.", null);
      }

      const shutdown = shutdownFileLogger();

      writeLogEntry("info", "The entry that reaches the size check while the logger is closing.", null);

      await shutdown;

      assert.equal(statSpy.calls(), 0, "the write path's size check started nothing while the logger was closing");
    });
  });

  test("a check that passed its guard while the logger was open queues no trim once the logger is closing", async (t) => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "retest.log");
      const realAppendFile = fs.promises.appendFile;
      const appendReached = Promise.withResolvers<true>();
      const releaseAppend = Promise.withResolvers<true>();

      await initializeFileLogger(logPath, LARGE_LIMIT, new TestClock());
      await writeFile(logPath, SEED_CONTENT, "utf-8");

      // The append parks until released and then writes for real, so it holds the write chain, and with it the shutdown's drain, while the row moves the state.
      t.mock.method(fs.promises, "appendFile", async (file: fs.PathLike | fs.promises.FileHandle, data: string | Uint8Array, options?: unknown): Promise<void> => {

        appendReached.resolve(true);

        await releaseAppend.promise;

        return realAppendFile.call(fs.promises, file, data, options as BufferEncoding);
      });

      const renameSpy = t.mock.method(fs.promises, "rename");
      const statSpy = spyOnStat(t, { parked: true });

      writeLogEntry("info", "The entry whose append holds the write chain.", null);

      const flush = flushLogBuffer();

      await appendReached.promise;

      // The setter's check passes its guard while the logger is open and parks inside its stat.
      const setter = setMaxLogSize(SMALL_LIMIT);

      await statSpy.reached;

      // The shutdown enters closing with its drain held on the parked append.
      const shutdown = shutdownFileLogger();

      // Releasing the stat lets the check go on to the trim, and within this one turn the trim makes its own test against the closing logger.
      statSpy.release();
      await settle();

      releaseAppend.resolve(true);

      await Promise.all([ flush, setter, shutdown ]);

      assert.equal(renameSpy.mock.callCount(), 0, "no trim was queued behind the drain");
      assert.ok(fs.readFileSync(logPath, "utf-8").startsWith(SEED_FIRST_LINE), "the file still opens with the seed's first line");
    });
  });

  test("a save while the file is paused starts no size check", async (t) => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "paused.log");
      const realAppendFile = fs.promises.appendFile;

      let appendCalls = 0;

      await initializeFileLogger(logPath, LARGE_LIMIT, new TestClock());
      await writeFile(logPath, SEED_CONTENT, "utf-8");

      // The first append fails, which pauses the file as a disk refusing writes does; any later append writes for real.
      t.mock.method(fs.promises, "appendFile", async (file: fs.PathLike | fs.promises.FileHandle, data: string | Uint8Array, options?: unknown): Promise<void> => {

        appendCalls++;

        if(appendCalls === 1) {

          throw new Error("The disk is full.");
        }

        return realAppendFile.call(fs.promises, file, data, options as BufferEncoding);
      });

      const statSpy = spyOnStat(t);

      // eslint-disable-next-line no-console
      const originalError = console.error;

      // eslint-disable-next-line no-console
      console.error = (): void => undefined;

      try {

        writeLogEntry("info", "The entry whose failed append pauses the file.", null);
        await flushLogBuffer();
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalError;
      }

      await setMaxLogSize(SMALL_LIMIT);

      assert.equal(statSpy.calls(), 0, "a paused file took no size check");
    });
  });

  test("a check that passed its guard before the file paused queues no trim", async (t) => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "paused-retest.log");
      const realAppendFile = fs.promises.appendFile;

      let appendCalls = 0;

      await initializeFileLogger(logPath, LARGE_LIMIT, new TestClock());
      await writeFile(logPath, SEED_CONTENT, "utf-8");

      // The first append fails, which pauses the file as a disk refusing writes does; any later append writes for real.
      t.mock.method(fs.promises, "appendFile", async (file: fs.PathLike | fs.promises.FileHandle, data: string | Uint8Array, options?: unknown): Promise<void> => {

        appendCalls++;

        if(appendCalls === 1) {

          throw new Error("The disk is full.");
        }

        return realAppendFile.call(fs.promises, file, data, options as BufferEncoding);
      });

      const renameSpy = t.mock.method(fs.promises, "rename");
      const statSpy = spyOnStat(t, { parked: true });

      // The setter's check passes its guard while the file accepts writes and parks inside its stat.
      const setter = setMaxLogSize(SMALL_LIMIT);

      await statSpy.reached;

      // The failed flush pauses the file while the stat is parked, because the flush runs on the write chain and the parked stat holds no place on it.
      // eslint-disable-next-line no-console
      const originalError = console.error;

      // eslint-disable-next-line no-console
      console.error = (): void => undefined;

      try {

        writeLogEntry("info", "The entry whose failed append pauses the file.", null);
        await flushLogBuffer();
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalError;
      }

      // Releasing the stat lets the check go on to the trim, whose own test then meets the paused file.
      statSpy.release();
      await setter;

      assert.equal(renameSpy.mock.callCount(), 0, "no trim was queued for the paused file");
      assert.ok(fs.readFileSync(logPath, "utf-8").startsWith(SEED_FIRST_LINE), "the file still opens with the seed's first line");
    });
  });
});

