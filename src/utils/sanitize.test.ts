/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * sanitize.test.ts: Unit tests for the non-printable-character sanitizers in sanitize.ts. The two exports share a global regex (NON_PRINTABLE_PATTERN) whose
 * lastIndex must be reset before each .test() call - the tests deliberately call containsNonPrintable() multiple times in a row to lock that the reset works.
 * Special characters in test inputs are written as \uXXXX escapes rather than literal Unicode codepoints so the test source itself stays plain ASCII; embedding
 * U+2028 or U+FEFF literally would break editor tooling and lint linebreak-style detection.
 */
import { containsNonPrintable, sanitizeString } from "./sanitize.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("sanitizeString", () => {

  test("returns clean input verbatim except for surrounding whitespace", () => {

    assert.equal(sanitizeString("hello"), "hello");
    assert.equal(sanitizeString("  hello  "), "hello", "trim still applies even on clean input");
  });

  test("returns the empty string unchanged (boundary)", () => {

    assert.equal(sanitizeString(""), "");
  });

  test("strips null bytes interleaved between visible characters", () => {

    // The classic copy-paste corruption case noted in the module's design comments.
    assert.equal(sanitizeString("h\x00e\x00l\x00l\x00o"), "hello");
  });

  test("strips C0 control characters except TAB, LF, CR", () => {

    // C0 controls below SP (0x20) except 0x09/0x0A/0x0D should be removed.
    assert.equal(sanitizeString("a\x01b\x02c\x07d"), "abcd", "all sub-TAB controls removed");
    assert.equal(sanitizeString("a\x0Bb\x0Cc"), "abc", "vertical tab and form feed removed");
  });

  test("preserves TAB, LF, and CR characters (multi-line text use case)", () => {

    // Negative test: the regex deliberately excludes 0x09, 0x0A, 0x0D from the strip set so multi-line text fields survive sanitization.
    assert.equal(sanitizeString("a\tb"), "a\tb");
    assert.equal(sanitizeString("line1\nline2"), "line1\nline2");
    assert.equal(sanitizeString("line1\r\nline2"), "line1\r\nline2");
  });

  test("strips DEL (0x7F)", () => {

    assert.equal(sanitizeString("a\x7Fb"), "ab");
  });

  test("strips the BOM (U+FEFF) at the beginning, middle, or end", () => {

    assert.equal(sanitizeString("\uFEFFhello"), "hello", "leading BOM");
    assert.equal(sanitizeString("hel\uFEFFlo"), "hello", "interior BOM");
    assert.equal(sanitizeString("hello\uFEFF"), "hello", "trailing BOM");
  });

  test("strips zero-width spaces and directional markers (U+200B - U+200F)", () => {

    assert.equal(sanitizeString("a\u200Bb"), "ab", "zero-width space");
    assert.equal(sanitizeString("a\u200Cb"), "ab", "zero-width non-joiner");
    assert.equal(sanitizeString("a\u200Eb"), "ab", "left-to-right mark");
  });

  test("strips line and paragraph separators (U+2028, U+2029)", () => {

    assert.equal(sanitizeString("a\u2028b"), "ab");
    assert.equal(sanitizeString("a\u2029b"), "ab");
  });

  test("trims surrounding whitespace after stripping non-printable content", () => {

    // Order: replace, then trim. A run of non-printables surrounded by spaces must collapse to no leading/trailing whitespace at all.
    assert.equal(sanitizeString("  \x00hello\x00  "), "hello");
  });

  test("returns an empty string when the input is entirely non-printable", () => {

    // Boundary: the regex strips everything, then trim has nothing to do, leaving "".
    assert.equal(sanitizeString("\x00\x01\x02"), "");
    assert.equal(sanitizeString("\uFEFF\u200B"), "");
  });
});

describe("containsNonPrintable", () => {

  test("returns false for the empty string (boundary)", () => {

    assert.equal(containsNonPrintable(""), false);
  });

  test("returns false for fully-printable content", () => {

    assert.equal(containsNonPrintable("hello world"), false);
    assert.equal(containsNonPrintable("café - menu"), false);
  });

  test("returns true when a single null byte is present", () => {

    assert.equal(containsNonPrintable("h\x00ello"), true);
  });

  test("returns false for strings containing only TAB/LF/CR (these are allowed)", () => {

    assert.equal(containsNonPrintable("a\tb\nc\rd"), false);
  });

  test("returns true for BOM, zero-width spaces, and other Unicode special markers", () => {

    assert.equal(containsNonPrintable("\uFEFFhello"), true, "BOM");
    assert.equal(containsNonPrintable("a\u200Bb"), true, "zero-width space");
    assert.equal(containsNonPrintable("a\u2028b"), true, "line separator");
  });

  test("returns deterministic results across repeated calls (lastIndex reset works)", () => {

    // The shared global regex would, without the lastIndex reset, alternate between true/false on repeated calls because RegExp.prototype.test() advances
    // lastIndex on a successful match. Locking this is the entire reason containsNonPrintable explicitly resets lastIndex.
    const value = "h\x00ello";

    assert.equal(containsNonPrintable(value), true, "first call detects");
    assert.equal(containsNonPrintable(value), true, "second call also detects");
    assert.equal(containsNonPrintable(value), true, "third call also detects");
  });

  test("returns false consistently across repeated calls on clean input", () => {

    const clean = "hello";

    assert.equal(containsNonPrintable(clean), false);
    assert.equal(containsNonPrintable(clean), false);
  });
});
