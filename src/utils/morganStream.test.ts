/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * morganStream.test.ts: Unit tests for the Morgan logging stream adapter in morganStream.ts. The adapter routes Morgan output to console.log when console mode
 * is enabled, otherwise to writeLogEntry on the file logger. We exercise both branches by toggling setConsoleLogging and stubbing console.log; the file-logger
 * branch is no-op in test (writeLogEntry early-returns when uninitialized) but we verify it does not throw.
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

  test("does NOT call console.log when console logging is disabled (file-logger branch)", () => {

    // Negative test: when file-logging mode is active, the adapter calls writeLogEntry instead. The file logger is uninitialized in the test environment so the
    // call is a no-op, but it must not route to console.log.
    setConsoleLogging(false);
    const stream = createMorganStream();

    stream.write("GET / 200\n");

    assert.equal(logCalls.length, 0, "console.log not called in file-logging mode");
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
