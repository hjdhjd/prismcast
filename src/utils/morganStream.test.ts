/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * morganStream.test.ts: Unit tests for the Morgan logging stream adapter in morganStream.ts. The adapter routes Morgan output to console.log when console mode
 * is enabled, otherwise to writeLogEntry on the file logger. We exercise both branches by toggling setConsoleLogging and stubbing console.log. For the file-logger
 * branch we initialize the file logger against a temporary directory, drive the morgan stream through writeLogEntry, flush, and assert the payload round-trips to
 * disk; we also cover the uninitialized no-op boundary case (an empty payload must not throw).
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import assert from "node:assert/strict";
import { createMorganStream } from "./morganStream.ts";
import { setConsoleLogging } from "./logger.ts";

describe("createMorganStream", () => {

  let logCalls: unknown[][];

  beforeEach(() => {

    logCalls = [];

    mock.method(globalThis.console, "log", (...args: unknown[]): void => { logCalls.push(args); });
  });

  afterEach(() => {

    setConsoleLogging(false);
    mock.reset();
  });

  test("returns an options object with a write method", () => {

    const stream = createMorganStream();

    assert.equal(typeof stream.write, "function", "Morgan stream interface requires a write callable");
  });

  test("routes to console.log with a timestamp prefix when console logging is enabled", () => {

    setConsoleLogging(true);
    const stream = createMorganStream();

    stream.write("GET /index.html 200 5ms\n");

    assert.equal(logCalls.length, 1, "console.log invoked once");

    const message = String(logCalls[0]?.[0]);

    assert.match(message, /^\[\d{4}\/\d{2}\/\d{2} /, "timestamp prefix matches yyyy/mm/dd format");
    assert.match(message, /GET \/index\.html 200 5ms\]?$/, "Morgan payload preserved (newline trimmed)");
  });

  test("trims the trailing newline added by Morgan", () => {

    setConsoleLogging(true);
    const stream = createMorganStream();

    stream.write("payload\n");

    const message = String(logCalls[0]?.[0]);

    assert.doesNotMatch(message, /\n/, "no embedded newline in the console output");
  });

  test("routes to the file logger with the trimmed payload when console logging is disabled", async () => {

    // The file-logging branch must reach writeLogEntry, which appends to the active log file. A negative-only assertion ("did not call console.log") would
    // pass even if the writeLogEntry call were silently dropped because the file logger is uninitialized. Here we initialize the file logger against a temp
    // dir, drive the morgan stream, flush, and inspect the on-disk content - that asserts the round-trip from morgan stream to file.
    const { flushLogBuffer, initializeFileLogger, shutdownFileLogger } = await import("./fileLogger.ts");
    const { mkdtemp, readFile, rm } = await import("node:fs/promises");
    const { join } = await import("node:path");
    const { tmpdir } = await import("node:os");

    const dir = await mkdtemp(join(tmpdir(), "prismcast-morgan-test-"));

    try {

      const logPath = join(dir, "morgan.log");

      await initializeFileLogger(logPath, 1_000_000);
      setConsoleLogging(false);

      const stream = createMorganStream();

      stream.write("GET /api/items 200 12ms\n");
      await flushLogBuffer();

      const content = await readFile(logPath, "utf-8");

      assert.match(content, /GET \/api\/items 200 12ms/, "morgan payload reached the log file via writeLogEntry");

      // The trim contract: the trailing newline morgan added must not appear in the file logger's output as a doubled separator. The file logger appends its
      // own "\n" at line termination, so the persisted entry should end with exactly one newline after the payload.
      assert.doesNotMatch(content, /12ms\s*\n\s*\n/, "no double newline (trim removed morgan's trailing newline before writeLogEntry appended its own)");
      assert.equal(logCalls.length, 0, "console.log not called in file-logging mode");
    } finally {

      shutdownFileLogger();
      await rm(dir, { force: true, recursive: true });
    }
  });

  test("does not throw when called with the empty string in file-logging mode", () => {

    // Boundary: trim of an empty string yields "". The function must accept it without throwing.
    setConsoleLogging(false);
    const stream = createMorganStream();

    assert.doesNotThrow(() => { stream.write(""); });
  });

  test("does not throw when called with the empty string in console mode", () => {

    setConsoleLogging(true);
    const stream = createMorganStream();

    assert.doesNotThrow(() => { stream.write(""); });
    assert.equal(logCalls.length, 1, "an empty payload still produces a console.log call");
  });

  test("each write produces exactly one console.log call (no batching)", () => {

    setConsoleLogging(true);
    const stream = createMorganStream();

    stream.write("first\n");
    stream.write("second\n");
    stream.write("third\n");

    assert.equal(logCalls.length, 3);
  });
});
