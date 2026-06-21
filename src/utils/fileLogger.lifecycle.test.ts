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
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

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

describe("writeLogEntry - retry-window re-enable after disabled state", () => {

  /* When flushLogBuffer fails (e.g., directory removed mid-flight), writeLogEntry sets isDisabled=true and disabledAt=Date.now(). Subsequent writes are silently
   * dropped during the retry window (ERROR_RETRY_DELAY_MS = 60s). Once Date.now() advances past the threshold, the next writeLogEntry call re-enables logging
   * (it clears isDisabled). We exercise the re-enable branch by stubbing Date.now to advance past the threshold deterministically without waiting 60s.
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

        // The next writeLogEntry should observe (Date.now() - disabledAt) >= ERROR_RETRY_DELAY_MS, clear the isDisabled flag, and append the entry to
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
