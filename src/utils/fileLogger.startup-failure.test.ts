/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.startup-failure.test.ts: Unit tests for what a failed initialization does to the startup window - it closes the window and discards what the
 * window held, so nothing from before the failure reaches the file a later initialization opens. The window is per process and closes at the first
 * initializeFileLogger call, so this file's first row must be that first initialization: a row ordered ahead of it would consume the window and leave
 * nothing to assert. The test runner gives each file its own process, so the window is this file's to spend. The mkdir-failure shape reused here is the one
 * fileLogger.lifecycle.test.ts uses for the same error.
 */
import { afterEach, describe, test } from "node:test";
import { flushLogBuffer, initializeFileLogger, shutdownFileLogger, writeLogEntry } from "./fileLogger.ts";
import { readFile, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

// A size cap far above anything these rows write, so no trim runs and the assertions read the file exactly as it was appended.
const MAX_LOG_SIZE = 1000000;

describe("a failed initialization and the startup window", () => {

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("discards the window's entries and closes it, so a later initialization opens a file holding nothing from before", async () => {

    await withTempDir(async (dir) => {

      // A regular file where a parent directory should go: mkdir(parent, { recursive: true }) fails with ENOTDIR or similar.
      const collidingFile = path.join(dir, "not-a-directory");

      await writeFile(collidingFile, "", "utf-8");

      const doomedPath = path.join(collidingFile, "subdir", "test.log");
      const goodPath = path.join(dir, "good.log");

      writeLogEntry("info", "Held by the window.", null);

      // eslint-disable-next-line no-console
      const originalError = console.error;

      // eslint-disable-next-line no-console
      console.error = (): void => undefined;

      try {

        await initializeFileLogger(doomedPath, MAX_LOG_SIZE);
      } finally {

        // eslint-disable-next-line no-console
        console.error = originalError;
      }

      // The window is closed at this point, so this entry has nowhere to wait.
      writeLogEntry("info", "Logged between the failure and the success.", null);

      await initializeFileLogger(goodPath, MAX_LOG_SIZE);

      writeLogEntry("info", "Logged after the good initialization.", null);

      await flushLogBuffer();

      const content = await readFile(goodPath, "utf-8");

      assert.doesNotMatch(content, /Held by the window\./, "a failed initialization discards what the window held");
      assert.doesNotMatch(content, /Logged between the failure and the success\./, "the window stays closed after a failure");
      assert.match(content, /Logged after the good initialization\./, "writes resume normally once a later initialization succeeds");
    });
  });
});
