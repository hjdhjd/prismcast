/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * html.test.ts: Unit tests for the escapeHtml primitive in html.ts. The function is a single pure regex replace - the tests pin the exact mapping for each of the
 * five HTML metacharacters and lock the surrounding-text passthrough so a future regression in the replacements table or pattern would surface immediately.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { escapeHtml } from "./html.ts";

describe("escapeHtml", () => {

  test("escapes the ampersand to &amp;", () => {

    assert.equal(escapeHtml("Tom & Jerry"), "Tom &amp; Jerry");
  });

  test("escapes the less-than sign to &lt;", () => {

    assert.equal(escapeHtml("a < b"), "a &lt; b");
  });

  test("escapes the greater-than sign to &gt;", () => {

    assert.equal(escapeHtml("a > b"), "a &gt; b");
  });

  test("escapes the double-quote to &quot;", () => {

    assert.equal(escapeHtml("She said \"hi\""), "She said &quot;hi&quot;");
  });

  test("escapes the apostrophe to &#39;", () => {

    assert.equal(escapeHtml("it's"), "it&#39;s");
  });

  test("escapes every special character in a single pass", () => {

    // The pattern is global and must catch every metacharacter, not just the first occurrence. A single-char-per-call regression would surface here.
    assert.equal(escapeHtml("<script>alert(\"x&y\");</script>"), "&lt;script&gt;alert(&quot;x&amp;y&quot;);&lt;/script&gt;");
  });

  test("returns the empty string unchanged (boundary)", () => {

    assert.equal(escapeHtml(""), "");
  });

  test("returns input unchanged when it contains no metacharacters", () => {

    // Negative test: the function must be a no-op when nothing matches the pattern. Locks the contract that benign strings pass through verbatim.
    assert.equal(escapeHtml("hello world 123"), "hello world 123");
  });

  test("preserves Unicode characters that are not in the metacharacter set", () => {

    assert.equal(escapeHtml("café — menu"), "café — menu");
  });

  test("escapes repeated metacharacters at every position", () => {

    assert.equal(escapeHtml("&&&"), "&amp;&amp;&amp;");
    assert.equal(escapeHtml("<<>>"), "&lt;&lt;&gt;&gt;");
  });
});
