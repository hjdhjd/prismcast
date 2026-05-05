/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * errors.test.ts: Unit tests for the error formatting helpers in errors.ts. Both exports are pure functions; the tests cover the three input-shape branches in
 * formatError (Error instance, duck-typed message, fallback to String()) and the three documented unrecoverable patterns in isSessionClosedError.
 */
import { describe, test } from "node:test";
import { formatError, isSessionClosedError } from "./errors.ts";
import assert from "node:assert/strict";

describe("formatError", () => {

  test("extracts the message from an Error instance", () => {

    assert.equal(formatError(new Error("boom")), "boom");
  });

  test("extracts the message from a custom Error subclass", () => {

    class CustomError extends Error {

      constructor(message: string) {

        super(message);

        this.name = "CustomError";
      }
    }

    assert.equal(formatError(new CustomError("custom failure")), "custom failure");
  });

  test("extracts the message from a duck-typed object with a string message field", () => {

    // The implementation handles non-Error objects that quack like errors (e.g., DOMException-like objects from the browser context). The check guards against
    // null/undefined and requires `typeof message === "string"` before reading.
    assert.equal(formatError({ message: "duck typed" }), "duck typed");
  });

  test("falls back to String() coercion for plain primitives", () => {

    assert.equal(formatError("just a string"), "just a string");
    assert.equal(formatError(42), "42");
    assert.equal(formatError(true), "true");
  });

  test("falls back to String() coercion for null and undefined", () => {

    // Negative test: the duck-type branch guards `error && (typeof ... === "string")`, so null/undefined skip the message extraction and land on String() instead.
    assert.equal(formatError(null), "null");
    assert.equal(formatError(undefined), "undefined");
  });

  test("falls back to String() when the message field is non-string", () => {

    // Negative test: a `message` field that isn't a string is rejected by the typeof check and the function falls through to String() coercion. The result is
    // the [object Object] of String() coercion - we lock that as the documented behavior.
    const result = formatError({ message: 123 });

    assert.equal(result, "[object Object]");
  });

  test("strips a single trailing period", () => {

    assert.equal(formatError(new Error("boom.")), "boom");
  });

  test("strips multiple consecutive trailing punctuation marks", () => {

    // The regex /[.!?]+$/ greedily strips a run of trailing punctuation, not just the final character.
    assert.equal(formatError(new Error("boom!?.")), "boom");
    assert.equal(formatError(new Error("really??")), "really");
  });

  test("preserves interior punctuation", () => {

    // Negative test: the strip pattern is anchored to end-of-string, so a period in the middle must survive.
    assert.equal(formatError(new Error("Step 1. Open the file.")), "Step 1. Open the file");
  });

  test("returns the empty string for an Error with an empty message", () => {

    // Boundary: an Error("") has message "", which the strip pattern leaves alone.
    assert.equal(formatError(new Error("")), "");
  });

  test("returns the empty string for the empty string input", () => {

    assert.equal(formatError(""), "");
  });
});

describe("isSessionClosedError", () => {

  test("returns true for 'Target closed' errors", () => {

    assert.equal(isSessionClosedError(new Error("Target closed")), true);
    assert.equal(isSessionClosedError(new Error("Protocol error: Target closed.")), true, "phrase embedded in larger message also matches");
  });

  test("returns true for 'Session closed' errors", () => {

    assert.equal(isSessionClosedError(new Error("Session closed.")), true);
    assert.equal(isSessionClosedError(new Error("Most likely the page has been closed. Session closed mid-call.")), true);
  });

  test("returns true for 'detached Frame' errors", () => {

    assert.equal(isSessionClosedError(new Error("Attempted to use detached Frame")), true);
  });

  test("returns false for unrelated error messages", () => {

    // Negative test: ordinary errors should pass straight through without false positives.
    assert.equal(isSessionClosedError(new Error("Network request failed")), false);
    assert.equal(isSessionClosedError(new Error("HTTP 500")), false);
  });

  test("returns false for the empty string", () => {

    // Boundary: empty input cannot match any pattern.
    assert.equal(isSessionClosedError(""), false);
  });

  test("matching is case-sensitive (Target closed != target closed)", () => {

    // Locking the contract: the patterns are exact substrings, not case-insensitive. Browser-emitted errors use the documented capitalization.
    assert.equal(isSessionClosedError(new Error("target closed")), false, "lowercase variant does not match");
  });

  test("returns true for non-Error inputs that contain a matching string", () => {

    // The function delegates message extraction to formatError, which handles non-Error inputs. A plain string carrying the marker still matches.
    assert.equal(isSessionClosedError("Target closed"), true);
    assert.equal(isSessionClosedError({ message: "detached Frame in evaluator" }), true);
  });

  test("returns false for null and undefined inputs", () => {

    // Negative test: nullish inputs are coerced to the strings "null"/"undefined", neither of which contains any pattern.
    assert.equal(isSessionClosedError(null), false);
    assert.equal(isSessionClosedError(undefined), false);
  });
});
