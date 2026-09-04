/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.lifecycle.test.ts: Unit tests for the file logger's lifecycle and error-recovery paths - flushLogBuffer's error-path retry-disable, writeLogEntry's
 * retry-window re-enable, initializeFileLogger's mkdir-failure recovery, and shutdownFileLogger. Basic writes/buffers live in fileLogger.test.ts; trim-path
 * tests live in fileLogger.trim.test.ts.
 */
import { afterEach, describe, mock, test } from "node:test";
import { flushLogBuffer, initializeFileLogger, shutdownFileLogger, writeLogEntry } from "./fileLogger.ts";
import { readFile, rm, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

// A size cap far above anything these rows write, so no trim runs and the assertions read the file exactly as it was appended.
const MAX_LOG_SIZE = 1000000;

// The bound the module's shutdown waits under for its write chain to drain, restated here so the row that lapses it reads against a name rather than a number.
const SHUTDOWN_DRAIN_BOUND_MS = 5000;

describe("flushLogBuffer - error-path retry-disable", () => {

  /* When fsPromises.appendFile throws (e.g., because the log file's directory was removed mid-flight), flushLogBuffer disables logging for ERROR_RETRY_DELAY_MS
   * milliseconds and emits a console.error. Subsequent writeLogEntry calls during the retry window are silently dropped. This guards against tight-loop error
   * cascades when the underlying filesystem is in a degraded state.
   */

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("disables logging temporarily when appendFile throws and drops subsequent writes within the retry window", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, MAX_LOG_SIZE);

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

describe("writeLogEntry - retry-window re-enable after disabled state", () => {

  /* When flushLogBuffer fails (e.g., directory removed mid-flight), its catch pauses the file the logger is open on. Subsequent writes are silently dropped
   * during the retry window (ERROR_RETRY_DELAY_MS = 60s). Once Date.now() advances past the threshold, the next writeLogEntry call re-opens the file and
   * clears the pause. We exercise that branch by stubbing Date.now to advance past the threshold deterministically without waiting 60s.
   */

  afterEach(async () => {

    await shutdownFileLogger();
    mock.reset();
  });

  test("re-enables logging when Date.now() has advanced past ERROR_RETRY_DELAY_MS since the disable", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "retry.log");

      await initializeFileLogger(logPath, MAX_LOG_SIZE);

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

        // Advance Date.now() past the 60-second retry window. We stub only Date.now via mock.method rather than enabling mock.timers with the Date API, because
        // mock.timers would also take over setInterval and freeze the logger's live flush-timer interval running throughout this test.
        const baseNow = Date.now() + 70000;

        mock.method(Date, "now", () => baseNow);

        // The next writeLogEntry should observe the retry delay elapsed, clear the pause on the file it holds open, and append the entry to the buffer
        // instead of silently dropping it.
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

describe("initializeFileLogger - mkdir failure recovery", () => {

  /* When the parent directory cannot be created (e.g., the path collides with an existing file), initializeFileLogger logs to console.error and returns without
   * throwing. Subsequent writeLogEntry calls become no-ops because the logger is off and the startup window is closed - the rows above have already spent
   * this process's window, and a failed initialization leaves it in any case. What a failure does to a window still open is asserted in
   * fileLogger.startup-failure.test.ts.
   */

  afterEach(async () => {

    await shutdownFileLogger();
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
        await assert.doesNotReject(() => initializeFileLogger(logPath, MAX_LOG_SIZE),
          "initializeFileLogger absorbs the mkdir failure");

        // The console.error should have fired with the "Failed to initialize file logger" message.
        const failureLogged = errorCalls.some((call) => {

          const message = typeof call[0] === "string" ? call[0] : "";

          return message.includes("Failed to initialize file logger");
        });

        assert.equal(failureLogged, true, "console.error fired with the init-failure message");

        // Subsequent writeLogEntry calls become no-ops: the logger is off and the startup window is closed, so nothing holds the entry.
        assert.doesNotThrow(() => { writeLogEntry("info", "Should be a no-op.", null); });
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalError;
      }
    });
  });
});

describe("shutdownFileLogger", () => {

  /* The rows here leave loggers running: the later-initialization row opens a second file, and the rows below park appends and a mkdir on one. The reset
   * releases the flush timer that would otherwise hold the process open past the last row, and returns the clock APIs the rows enable.
   */
  afterEach(async () => {

    await shutdownFileLogger();
    mock.timers.reset();
  });

  test("is a no-op on an already-shut-down logger", async () => {

    // Boundary: calling shutdown twice (or before init) must not crash. The assertion is doesNotReject rather than doesNotThrow because shutdown awaits the write
    // chain's drain, and an await cannot sit inside a synchronous callback.
    await assert.doesNotReject(async () => {

      await shutdownFileLogger();
      await shutdownFileLogger();
    });
  });

  test("persists any buffered content synchronously before clearing state", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, MAX_LOG_SIZE);

      writeLogEntry("info", "Final entry.", null);

      await shutdownFileLogger();

      const content = await readFile(logPath, "utf-8");

      assert.match(content, /Final entry\./, "shutdown flushes the buffer to disk");
    });
  });

  test("writes a line logged after shutdown straight to the file the logger closed", async () => {

    /* Once the logger has shut down there is no buffer left to hold an entry and no flush left to carry it, so the entry goes to the run's own file at once.
     * The exit handler's Chrome cleanup logs from exactly this position.
     */
    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, MAX_LOG_SIZE);
      await shutdownFileLogger();

      writeLogEntry("info", "After shutdown.", null);

      const content = await readFile(logPath, "utf-8");

      assert.match(content, /After shutdown\./, "the closed file takes the entry without a flush");
    });
  });

  test("a later initialization takes the writes, and the file the previous run closed receives nothing further", async () => {

    /* The closed file belongs to one run. A new initialization supersedes it, so an entry logged under the second run must land in the second file and
     * nowhere else - otherwise a suite that initializes twice would leak its lines into the first run's file.
     */
    await withTempDir(async (dir) => {

      const firstPath = path.join(dir, "first.log");
      const secondPath = path.join(dir, "second.log");

      await initializeFileLogger(firstPath, MAX_LOG_SIZE);
      await shutdownFileLogger();

      writeLogEntry("info", "Belongs to the first run.", null);

      await initializeFileLogger(secondPath, MAX_LOG_SIZE);

      writeLogEntry("info", "Belongs to the second run.", null);
      await flushLogBuffer();

      const firstContent = await readFile(firstPath, "utf-8");
      const secondContent = await readFile(secondPath, "utf-8");

      assert.match(firstContent, /Belongs to the first run\./, "the entry logged after the first shutdown stays in the first run's file");
      assert.doesNotMatch(firstContent, /Belongs to the second run\./, "a new initialization redirects writes away from the closed file");
      assert.match(secondContent, /Belongs to the second run\./, "the second run's file takes the entries logged under it");
      assert.doesNotMatch(secondContent, /Belongs to the first run\./, "the first run's tail does not follow into the second run's file");
    });
  });

  test("drops a line logged after an initialization that failed, rather than sending it to the file the previous run closed", async () => {

    /* An initialization that fails leaves the logger off, where a line is dropped, rather than leaving the file the previous run closed in place. The move
     * to off at the start of every initialization is what keeps a failed run's lines out of the run before it.
     */
    await withTempDir(async (dir) => {

      const firstPath = path.join(dir, "first.log");
      const collidingFile = path.join(dir, "not-a-directory");

      await initializeFileLogger(firstPath, MAX_LOG_SIZE);
      await shutdownFileLogger();

      // A regular file where a parent directory should go: mkdir(parent, { recursive: true }) fails with ENOTDIR or similar.
      await writeFile(collidingFile, "", "utf-8");

      // Stub console.error so the initialization-failure message isn't printed during the test.
      // eslint-disable-next-line no-console
      const originalConsoleError = console.error;

      // eslint-disable-next-line no-console
      console.error = (): void => { /* swallow */ };

      try {

        await initializeFileLogger(path.join(collidingFile, "subdir", "second.log"), MAX_LOG_SIZE);

        writeLogEntry("info", "Logged after the failed initialization.", null);
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalConsoleError;
      }

      const firstContent = await readFile(firstPath, "utf-8");

      assert.doesNotMatch(firstContent, /Logged after the failed initialization\./, "the closed file must not take a line from a later, failed run");
    });
  });

  test("a write failure whose append settles after shutdown leaves the run that follows open", async (t) => {

    /* The shutdown drain is bounded, so a shutdown can complete while an append is still in flight. When that append then fails, the failure belongs to the
     * run that ended: a later initialization must stay open and take its lines. The row parks the append, lapses the drain bound under mock timers, opens a
     * second file, releases the failure, and asserts the second file still takes a line. Both timer APIs are virtual, so the periodic flush cannot slip a
     * write of its own between the row's writes and flushes.
     */
    mock.timers.enable({ apis: [ "setInterval", "setTimeout" ] });

    await withTempDir(async (dir) => {

      const firstPath = path.join(dir, "first.log");
      const secondPath = path.join(dir, "second.log");
      const realAppendFile = fs.promises.appendFile;
      const appendReached = Promise.withResolvers<true>();
      const releaseAppend = Promise.withResolvers<true>();

      // The first append parks until released and then fails; every later append runs for real.
      t.mock.method(fs.promises, "appendFile", async (file: fs.PathLike | fs.promises.FileHandle, data: string | Uint8Array, options?: unknown): Promise<void> => {

        if(file === firstPath) {

          appendReached.resolve(true);

          await releaseAppend.promise;

          throw new Error("The disk is full.");
        }

        return realAppendFile.call(fs.promises, file, data, options as BufferEncoding);
      });

      // eslint-disable-next-line no-console
      const originalError = console.error;

      // eslint-disable-next-line no-console
      console.error = (): void => undefined;

      try {

        await initializeFileLogger(firstPath, MAX_LOG_SIZE);

        writeLogEntry("info", "Parked in the first run.", null);

        const parkedFlush = flushLogBuffer();

        await appendReached.promise;

        const shutdown = shutdownFileLogger();

        mock.timers.tick(SHUTDOWN_DRAIN_BOUND_MS);

        await shutdown;
        await initializeFileLogger(secondPath, MAX_LOG_SIZE);

        releaseAppend.resolve(true);

        await parkedFlush;

        writeLogEntry("info", "Written by the second run.", null);
        await flushLogBuffer();
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalError;
      }

      const secondContent = await readFile(secondPath, "utf-8");

      assert.match(secondContent, /Written by the second run\./, "the failure of the first run's append does not pause the second run");
    });
  });

  test("the final flush attempts a line logged during the drain even when the file was paused before the shutdown", async (t) => {

    /* A pause bounds a cascade of periodic retries against a disk that is refusing writes, and the final flush is one attempt rather than a cascade, so a line
     * logged while the drain runs is written whether or not the file was paused going in. The row parks two appends on one file: the first fails and pauses
     * the file, and the second is still in flight when the shutdown starts, which holds the drain open long enough to log line C into it. Line A goes down
     * with the append that failed, line B is the one that queued behind it, and line C is the one the final flush has to attempt. Only setInterval is virtual
     * here, because the drain's own bound has to stay real for the shutdown to settle on the released append rather than on a tick.
     */
    mock.timers.enable({ apis: ["setInterval"] });

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "drain-pause.log");
      const realAppendFile = fs.promises.appendFile;
      const firstReached = Promise.withResolvers<true>();
      const releaseFirst = Promise.withResolvers<true>();
      const secondReached = Promise.withResolvers<true>();
      const releaseSecond = Promise.withResolvers<true>();

      let appendCall = 0;

      // The first append parks and then fails, pausing the file; the second parks and then writes for real, holding the drain open while the row logs into it.
      t.mock.method(fs.promises, "appendFile", async (file: fs.PathLike | fs.promises.FileHandle, data: string | Uint8Array, options?: unknown): Promise<void> => {

        appendCall++;

        if(appendCall === 1) {

          firstReached.resolve(true);

          await releaseFirst.promise;

          throw new Error("The disk is full.");
        }

        if(appendCall === 2) {

          secondReached.resolve(true);

          await releaseSecond.promise;
        }

        return realAppendFile.call(fs.promises, file, data, options as BufferEncoding);
      });

      // eslint-disable-next-line no-console
      const originalError = console.error;

      // eslint-disable-next-line no-console
      console.error = (): void => undefined;

      try {

        await initializeFileLogger(logPath, MAX_LOG_SIZE);

        writeLogEntry("info", "Line A, carried by the append that fails.", null);

        const firstFlush = flushLogBuffer();

        await firstReached.promise;

        writeLogEntry("info", "Line B, carried by the append that queues behind it.", null);

        const secondFlush = flushLogBuffer();

        releaseFirst.resolve(true);

        await firstFlush;
        await secondReached.promise;

        // The file is paused and its second append is in flight, so the shutdown below enters the drain with both conditions in place.
        const shutdown = shutdownFileLogger();

        writeLogEntry("info", "Line C, logged while the drain runs.", null);

        releaseSecond.resolve(true);

        await secondFlush;
        await shutdown;
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalError;
      }

      const content = await readFile(logPath, "utf-8");

      assert.doesNotMatch(content, /Line A,/, "the append that failed took its entries with it");
      assert.match(content, /Line B,/, "the append that queued behind the failure still wrote its entries");
      assert.match(content, /Line C,/, "the final flush attempts a line the drain buffered, whether or not the file was paused before the shutdown");
    });
  });

  test("drops a line logged inside a later initialization's own awaits rather than sending it to the file the previous run closed", async (t) => {

    /* An initialization supersedes the file the last run closed, and it does so before it awaits anything, so the gap its own mkdir and stat open is a gap in
     * which no file can take a line. The row parks the mkdir of a second initialization, logs into that gap, and asserts the line reached neither the file the
     * first run closed nor the file the second run is still opening.
     */
    mock.timers.enable({ apis: ["setInterval"] });

    await withTempDir(async (dir) => {

      const firstPath = path.join(dir, "first.log");
      const secondPath = path.join(dir, "second.log");
      const realMkdir = fs.promises.mkdir;
      const mkdirReached = Promise.withResolvers<true>();
      const releaseMkdir = Promise.withResolvers<true>();

      await initializeFileLogger(firstPath, MAX_LOG_SIZE);
      await shutdownFileLogger();

      // The second initialization's mkdir parks, which holds that initialization inside its own awaits while the row logs a line into the gap.
      t.mock.method(fs.promises, "mkdir", async (dirPath: fs.PathLike, options?: unknown): Promise<string | undefined> => {

        mkdirReached.resolve(true);

        await releaseMkdir.promise;

        return realMkdir.call(fs.promises, dirPath, options as fs.MakeDirectoryOptions & { recursive: true });
      });

      const initialization = initializeFileLogger(secondPath, MAX_LOG_SIZE);

      await mkdirReached.promise;

      writeLogEntry("info", "Logged inside the second initialization.", null);

      releaseMkdir.resolve(true);

      await initialization;
      await flushLogBuffer();

      const firstContent = await readFile(firstPath, "utf-8");
      const secondContent = await readFile(secondPath, "utf-8");

      assert.doesNotMatch(firstContent, /Logged inside the second initialization\./, "the file the previous run closed is superseded before the awaits begin");
      assert.doesNotMatch(secondContent, /Logged inside the second initialization\./, "the file this run is opening cannot take a line before it is open");
    });
  });
});
