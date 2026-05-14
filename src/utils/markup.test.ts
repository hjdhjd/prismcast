/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * markup.test.ts: Unit tests for the markup-escape primitives in markup.ts. The two surface functions (escapeHtml, escapeXml) share an internal regex callback;
 * the suite pins the exact entity mapping for each metacharacter for both flavors, locks surrounding-text passthrough, and asserts the apostrophe entity differs
 * between HTML5 (&#39;) and XML (&apos;) so a future regression in either entity table or the shared regex surfaces immediately.
 */
import { describe, test } from "node:test";
import { escapeHtml, escapeXml } from "./markup.ts";
import assert from "node:assert/strict";

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

describe("escapeXml", () => {

  test("escapes the ampersand to &amp;", () => {

    assert.equal(escapeXml("Tom & Jerry"), "Tom &amp; Jerry");
  });

  test("escapes the less-than sign to &lt;", () => {

    assert.equal(escapeXml("a < b"), "a &lt; b");
  });

  test("escapes the greater-than sign to &gt;", () => {

    assert.equal(escapeXml("a > b"), "a &gt; b");
  });

  test("escapes the double-quote to &quot;", () => {

    assert.equal(escapeXml("She said \"hi\""), "She said &quot;hi&quot;");
  });

  test("escapes the apostrophe to &apos; (XML named entity, not the HTML5 numeric form)", () => {

    // Boundary: this is the one place the XML table diverges from the HTML table. The XML 1.0 spec defines &apos; as one of the five predefined entities and
    // launchd's plist parser expects this form; the HTML escaper emits &#39; instead. Pinning both forms here would catch a future merge that crossed the tables.
    assert.equal(escapeXml("it's"), "it&apos;s");
    assert.equal(escapeHtml("it's"), "it&#39;s");
  });

  test("escapes every special character in a single pass", () => {

    assert.equal(escapeXml("<key>Tom & Jerry's \"show\"</key>"), "&lt;key&gt;Tom &amp; Jerry&apos;s &quot;show&quot;&lt;/key&gt;");
  });

  test("returns the empty string unchanged (boundary)", () => {

    assert.equal(escapeXml(""), "");
  });

  test("returns input unchanged when it contains no metacharacters", () => {

    assert.equal(escapeXml("PrismCast service"), "PrismCast service");
  });

  test("preserves Unicode characters that are not in the metacharacter set", () => {

    assert.equal(escapeXml("café — menu"), "café — menu");
  });
});
