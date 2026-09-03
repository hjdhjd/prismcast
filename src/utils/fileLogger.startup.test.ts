/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.startup.test.ts: Unit tests for the startup window - the entries logged before the file logger initializes and the bound the window keeps them
 * under. The window is per process and closes at the first initializeFileLogger call, so this file's first row must be that first initialization: a row
 * ordered ahead of it would consume the window and leave nothing to assert. The test runner gives each file its own process, so the window is this file's to
 * spend. Basic write/buffer paths live in fileLogger.test.ts, lifecycle and error paths in fileLogger.lifecycle.test.ts.
 */
import { afterEach, describe, test } from "node:test";
import { flushLogBuffer, initializeFileLogger, shutdownFileLogger, writeLogEntry } from "./fileLogger.ts";
import assert from "node:assert/strict";
import path from "node:path";
import { readFile } from "node:fs/promises";
import { withTempDir } from "../testing.helpers.ts";

// A size cap far above anything these rows write, so no trim runs and the assertions read the file exactly as it was appended.
const MAX_LOG_SIZE = 1000000;

// The bound the module keeps the startup window under, restated here so the assertions below read against a name rather than a bare number.
const STARTUP_BUFFER_LIMIT = 1000;

// How many entries this file logs before initializing: enough to overrun the bound and see which end of the buffer survives.
const OVERRUN_COUNT = STARTUP_BUFFER_LIMIT + 5;

describe("the startup window", () => {

  afterEach(async () => {

    await shutdownFileLogger();
  });

  test("holds entries logged before initialization, bounded to the newest, and writes them ahead of later entries", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      // Every one of these lands before the logger has a file, which is the position of the boot messages the window exists for.
      for(let i = 1; i <= OVERRUN_COUNT; i++) {

        writeLogEntry("info", "Startup entry " + String(i) + ".", null);
      }

      await initializeFileLogger(logPath, MAX_LOG_SIZE);

      writeLogEntry("info", "Post-initialization entry.", null);

      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");
      const lines = content.split("\n").filter((line) => line.length > 0);

      assert.equal(lines.length, STARTUP_BUFFER_LIMIT + 1, "the window keeps its bound and the entry logged after initialization joins the flush");
      assert.equal(lines.at(0)?.endsWith("Startup entry " + String(OVERRUN_COUNT - STARTUP_BUFFER_LIMIT + 1) + "."), true,
        "the oldest entries are the ones the bound drops");
      assert.equal(lines.at(-2)?.endsWith("Startup entry " + String(OVERRUN_COUNT) + "."), true, "the newest startup entry survives");
      assert.equal(lines.at(-1)?.endsWith("Post-initialization entry."), true, "startup entries precede everything logged after initialization");

      // Each entry carries its own timestamp prefix rather than the moment of the flush that wrote it.
      for(const line of lines) {

        assert.match(line, /^\[\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [AP]M\] /, "every written entry keeps its own timestamp prefix");
      }
    });
  });

  test("drops a write after the first initialization and its shutdown, because the window does not reopen", async () => {

    await withTempDir(async (dir) => {

      const logPath = path.join(dir, "test.log");

      await initializeFileLogger(logPath, MAX_LOG_SIZE);
      await shutdownFileLogger();

      writeLogEntry("info", "After the window closed.", null);

      // Re-initializing is something only a test suite does, and the entry above must not surface in the file it opens.
      await initializeFileLogger(logPath, MAX_LOG_SIZE);
      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");

      assert.doesNotMatch(content, /After the window closed\./);
    });
  });
});
