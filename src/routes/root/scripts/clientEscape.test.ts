/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clientEscape.test.ts: Unit tests for the single client-side HTML-escape source of truth. clientEscapeHtml ships to the browser via Function.prototype.toString()
 * and therefore cannot import the server-side markup.escapeHtml, so the two are separate function objects; this suite is the one byte-parity guard that keeps them
 * identical. It also pins the entity contract directly and the shape of the window.escapeHtml assignment that the shared utilities script emits. The runtime
 * validity of the emitted assignment (that the .toString()-serialized body parses and installs window.escapeHtml in a real DOM) is exercised by the shared-runtime
 * suite, which executes the shared utilities script and calls window.escapeHtml.
 */
import { clientEscapeHtml, generateClientEscapeAssignment } from "./clientEscape.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { escapeHtml as markupEscapeHtml } from "../../../utils/markup.ts";

describe("clientEscapeHtml", () => {

  test("encodes the five HTML special characters as entities, leaving ordinary text untouched", () => {

    /* The five characters that can break out of a text or attribute context - & < > " ' - must each render as an entity. We pin each character to its entity: the
     * ampersand, the angle brackets, the double quote (the attribute-breakout vector), and the apostrophe (HTML5 numeric reference &#39;). Assertion order is
     * presentational, not meaningful for correctness: the escaper is a single character-class regex pass that matches and replaces each character exactly once,
     * so escaping order is irrelevant to correctness. Ordinary alphanumerics pass through verbatim.
     */
    assert.equal(clientEscapeHtml("&"), "&amp;");
    assert.equal(clientEscapeHtml("<"), "&lt;");
    assert.equal(clientEscapeHtml(">"), "&gt;");
    assert.equal(clientEscapeHtml("\""), "&quot;");
    assert.equal(clientEscapeHtml("'"), "&#39;");
    assert.equal(clientEscapeHtml("NBC News"), "NBC News", "ordinary text must pass through unchanged");
  });

  test("encodes a script-injection payload so it cannot break out of a text or attribute context", () => {

    /* The canonical XSS vector: a channel name, show name, or URL carrying a tag and a quoted attribute. After escaping, the angle brackets and quotes are inert
     * entities, so the browser renders the literal text instead of executing the payload. The double-quote coverage is what makes the result safe inside the
     * title/alt/value/data-* attribute positions the client renderers concatenate into.
     */
    const payload = "<img src=x onerror=\"alert(1)\">";

    assert.equal(clientEscapeHtml(payload), "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;");
    assert.ok(!clientEscapeHtml(payload).includes("<"), "no raw '<' may survive escaping");
    assert.ok(!clientEscapeHtml(payload).includes("\""), "no raw '\"' may survive escaping");
  });
});

describe("clientEscapeHtml parity with the markup.escapeHtml single source of truth", () => {

  test("is byte-identical to markup.escapeHtml across a mixed corpus and every special character (drift guard)", () => {

    /* The client escaper and the server escaper must encode the same five characters to the same entities. clientEscapeHtml cannot import markup.escapeHtml because
     * its body ships to the browser verbatim, so the two are necessarily separate function objects; this parity guard makes that separation safe. A future edit to
     * either escaper that changed an entity (for example swapping the HTML5 numeric apostrophe &#39; for the XML &apos;) would fail to merge here.
     */
    const corpus = "Tom & Jerry's <b>\"Live\" Show</b> & more — channels/streams?a=1&b=2";

    assert.equal(clientEscapeHtml(corpus), markupEscapeHtml(corpus), "the client escaper must match markup.escapeHtml byte-for-byte");

    // Sweep every special character individually to catch a single-entity divergence the combined corpus could mask.
    for(const char of [ "&", "<", ">", "\"", "'" ]) {

      assert.equal(clientEscapeHtml(char), markupEscapeHtml(char), "clientEscapeHtml('" + char + "') must match markup.escapeHtml('" + char + "')");
    }
  });
});

describe("generateClientEscapeAssignment", () => {

  test("emits statements that install window.escapeHtml backed by an IIFE-local const alias", () => {

    /* The shared utilities script concatenates this fragment near the top of its IIFE. It must install the global surface (window.escapeHtml) that every later
     * client script resolves and the local const alias that shared.ts's own renderers call without a global property lookup. We pin both so a refactor that dropped
     * either binding - leaving cross-script callers or in-IIFE callers with an undefined escapeHtml - surfaces here.
     */
    const snippet = generateClientEscapeAssignment();

    assert.match(snippet, /const escapeHtml\s*=\s*function/, "the fragment must define the local const escapeHtml alias");
    assert.match(snippet, /window\.escapeHtml\s*=\s*escapeHtml;/, "the fragment must install window.escapeHtml from the local alias");
  });
});
