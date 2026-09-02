/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * errors.test.ts: Unit tests for the error formatting helpers in errors.ts. Every export in this module is a pure function; the tests cover the
 * input-shape branches in formatError (Error instance, duck-typed message, fallback to String()), the documented unrecoverable patterns in
 * isSessionClosedError, and the exact-phrase matching in isPageDeathError - including the errors carrying a page-death word in an unrelated sense, which are
 * what prove the phrase matching is not a word search.
 */
import { describe, test } from "node:test";
import { formatError, isPageDeathError, isSessionClosedError } from "./errors.ts";
import { EvaluateTimeoutError } from "./evaluate.ts";
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

  test("matching ignores case (target closed == TARGET CLOSED)", () => {

    // Locking the contract: the same dead state reaches callers with different capitalization depending on which browser surface raised it, and every
    // capitalization means the target is equally gone.
    assert.equal(isSessionClosedError(new Error("target closed")), true, "lowercase variant matches");
    assert.equal(isSessionClosedError(new Error("TARGET CLOSED")), true, "uppercase variant matches");
    assert.equal(isSessionClosedError(new Error("session CLOSED unexpectedly")), true, "mixed-case variant matches");
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

describe("isPageDeathError", () => {

  test("returns true for each execution-context and frame phrasing, in any capitalization", () => {

    // The CDP family this predicate exists to catch. Capitalization varies by the surface that raised the error, so every phrase is asserted in a different case.
    assert.equal(isPageDeathError(new Error("Execution context was destroyed")), true);
    assert.equal(isPageDeathError(new Error("EXECUTION CONTEXT IS NOT AVAILABLE")), true);
    assert.equal(isPageDeathError(new Error("Cannot find context with specified id")), true);
    assert.equal(isPageDeathError(new Error("Frame got detached")), true);
    assert.equal(isPageDeathError(new Error("frame was detached")), true);
  });

  test("returns true for the session-closed family it composes, in any capitalization", () => {

    // The union's other half. These reach the predicate through isSessionClosedError, so the composition is what this asserts.
    assert.equal(isPageDeathError(new Error("TARGET CLOSED")), true);
    assert.equal(isPageDeathError(new Error("session closed")), true);
    assert.equal(isPageDeathError(new Error("Attempted to use detached Frame '5D2393C3BF7A9BFEAB6C38D638EA01D8'")), true);
  });

  test("returns false for errors carrying a page-death word in an unrelated sense", () => {

    // These are the cases that prove the matching is by phrase, not by word: each carries "destroyed", "detached", or "context" describing something that is
    // not a dead page. Treating them as page death would route ordinary failures into page-death handling, where they suppress real signal.
    assert.equal(isPageDeathError(new Error("The object was destroyed elsewhere")), false, "an unrelated destruction is not page death");
    assert.equal(isPageDeathError(new Error("detached observer callback")), false, "a detached callback is not page death");
    assert.equal(isPageDeathError(new Error("browsing context lost")), false, "a lost browsing context is not one of the CDP phrasings");
  });

  test("returns false for a message holding every page-death word without any of the phrases", () => {

    // Rules out a word-pair implementation: "frame", "context", and "detached" are all present, and no exact phrase is.
    assert.equal(isPageDeathError(new Error("iframe context was unexpectedly detached during navigation")), false);
  });

  test("returns false for ordinary tune and timeout failures", () => {

    // The failures that ARE evidence about their own subject. A cached-URL tune that fails this way must still evict the entry, so a false positive here would
    // reintroduce exactly the eviction the classifier exists to prevent - in the opposite direction.
    assert.equal(isPageDeathError(new Error("Waiting for selector `video` failed")), false);
    assert.equal(isPageDeathError(new EvaluateTimeoutError(15000)), false, "a timeout says the page is slow, not gone");
  });

  test("returns false for the empty string, null, and undefined", () => {

    // Boundary: nothing to match against, and the nullish inputs coerce to "null"/"undefined" through formatError.
    assert.equal(isPageDeathError(""), false);
    assert.equal(isPageDeathError(null), false);
    assert.equal(isPageDeathError(undefined), false);
  });

  test("classifies non-Error inputs through formatError", () => {

    // The predicate takes unknown and delegates message extraction, so a bare string or a duck-typed carrier classifies the same as an Error would.
    assert.equal(isPageDeathError("Execution context was destroyed"), true);
    assert.equal(isPageDeathError({ message: "Frame was detached during evaluation" }), true);
  });
});

