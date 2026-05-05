/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cliOutput.test.ts: Unit tests for the print and printError CLI helpers in cliOutput.ts. The two functions delegate to console.log and console.error so the
 * tests stub those methods via mock.method on globalThis.console. Used by service and upgrade subcommands that run before the file logger is initialized.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { print, printError } from "./cliOutput.ts";
import assert from "node:assert/strict";

describe("print", () => {

  let logCalls: unknown[][];

  beforeEach(() => {

    logCalls = [];

    // Stub console.log to capture every call. mock.method auto-restores via mock.reset() in afterEach.
    mock.method(globalThis.console, "log", (...args: unknown[]): void => {

      logCalls.push(args);
    });
  });

  afterEach(() => {

    mock.reset();
  });

  test("writes the message to console.log", () => {

    print("hello");

    assert.equal(logCalls.length, 1, "console.log invoked exactly once");
    assert.deepEqual(logCalls[0], ["hello"], "console.log received the message verbatim");
  });

  test("forwards the empty string to console.log", () => {

    // Boundary: empty string is still a valid CLI message (e.g., a blank separator line).
    print("");

    assert.equal(logCalls.length, 1);
    assert.deepEqual(logCalls[0], [""]);
  });

  test("does not invoke console.error", () => {

    // Negative test: print() must route through console.log only - mistakenly hitting stderr would corrupt CLI workflows that pipe stdout.
    const errorCalls: unknown[][] = [];

    mock.method(globalThis.console, "error", (...args: unknown[]): void => {

      errorCalls.push(args);
    });

    print("anything");

    assert.equal(errorCalls.length, 0);
  });

  test("invokes console.log once per call (no batching, no swallowing)", () => {

    print("first");
    print("second");
    print("third");

    assert.equal(logCalls.length, 3, "three calls produce three console.log invocations");
    assert.deepEqual(logCalls[0], ["first"]);
    assert.deepEqual(logCalls[1], ["second"]);
    assert.deepEqual(logCalls[2], ["third"]);
  });
});

describe("printError", () => {

  let errorCalls: unknown[][];

  beforeEach(() => {

    errorCalls = [];

    mock.method(globalThis.console, "error", (...args: unknown[]): void => {

      errorCalls.push(args);
    });
  });

  afterEach(() => {

    mock.reset();
  });

  test("writes the message to console.error", () => {

    printError("oops");

    assert.equal(errorCalls.length, 1);
    assert.deepEqual(errorCalls[0], ["oops"]);
  });

  test("forwards the empty string to console.error (boundary)", () => {

    printError("");

    assert.equal(errorCalls.length, 1);
    assert.deepEqual(errorCalls[0], [""]);
  });

  test("does not invoke console.log", () => {

    // Negative test: printError() must route through console.error only.
    const logCalls: unknown[][] = [];

    mock.method(globalThis.console, "log", (...args: unknown[]): void => {

      logCalls.push(args);
    });

    printError("anything");

    assert.equal(logCalls.length, 0);
  });

  test("invokes console.error once per call (no batching)", () => {

    printError("first");
    printError("second");

    assert.equal(errorCalls.length, 2);
    assert.deepEqual(errorCalls[0], ["first"]);
    assert.deepEqual(errorCalls[1], ["second"]);
  });
});
