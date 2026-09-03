/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.startup-fallback.test.ts: Unit tests for the synchronous flush's fallback path - where the startup window's entries go when the process exits
 * before the logger ever initialized. The window is per process and closes at the first initializeFileLogger call, so this file's first row must be that
 * first initialization; both rows here write into the still-open window and the second row's initialization is the one that closes it, which is why the
 * unwritable-fallback row is ordered first. The test runner gives each file its own process, so the window is this file's to spend.
 */
import { afterEach, describe, test } from "node:test";
import { flushLogBuffer, flushLogBufferSync, initializeFileLogger, shutdownFileLogger, writeLogEntry } from "./fileLogger.ts";
import { readFile, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

// A size cap far above anything these rows write, so no trim runs and the assertions read the file exactly as it was appended.
const MAX_LOG_SIZE = 1000000;

describe("flushLogBufferSync with a fallback path", () => {

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("reports a fallback path whose parent cannot be created to the console rather than throwing", async () => {

    await withTempDir(async (dir) => {

      // A regular file where a parent directory should go: mkdirSync(parent, { recursive: true }) fails with ENOTDIR or similar.
      const collidingFile = path.join(dir, "not-a-directory");

      await writeFile(collidingFile, "", "utf-8");

      const unwritablePath = path.join(collidingFile, "nested", "fallback.log");

      writeLogEntry("info", "Entry the exit cannot place.", null);

      // eslint-disable-next-line no-console
      const originalError = console.error;
      const errorCalls: unknown[][] = [];

      // eslint-disable-next-line no-console
      console.error = (...args: unknown[]): void => { errorCalls.push(args); };

      try {

        // The exit handler runs on a process that is already leaving; a read-only or unusable data directory must not turn that into a throw.
        assert.doesNotThrow(() => { flushLogBufferSync(unwritablePath); });
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalError;
      }

      const failureLogged = errorCalls.some((call) => (typeof call[0] === "string") && call[0].includes("Failed to write final log entries"));

      assert.equal(failureLogged, true, "the write failure reaches the console instead of the caller");
    });
  });

  test("writes the window's entries to the fallback path, creating its parent directory, and clears the buffer", async () => {

    await withTempDir(async (dir) => {

      const fallbackPath = path.join(dir, "does", "not", "exist", "fallback.log");
      const logPath = path.join(dir, "test.log");

      writeLogEntry("info", "First buffered entry.", null);
      writeLogEntry("warn", "Second buffered entry.", "yellow");

      // A SIGTERM during boot reaches shutdown before any initialization, where it is a no-op that leaves the window's entries for the exit handler below.
      await shutdownFileLogger();

      flushLogBufferSync(fallbackPath);

      const fallbackContent = await readFile(fallbackPath, "utf-8");

      assert.match(fallbackContent, /First buffered entry\./, "the fallback receives the window's entries");
      assert.match(fallbackContent, /Second buffered entry\./, "the fallback receives every entry, not just the first");

      // Initializing afterwards proves the fallback emptied the buffer: a flush now has nothing of the window's left to append.
      await initializeFileLogger(logPath, MAX_LOG_SIZE);
      await flushLogBuffer();

      const logContent = await readFile(logPath, "utf-8");

      assert.equal(logContent, "", "the entries the fallback wrote are gone from the buffer");
    });
  });
});
