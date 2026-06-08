/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clientUrl.test.ts: Unit tests for the single client-side URL-safety source of truth. clientSafeUrl ships to the browser via Function.prototype.toString() and
 * gates the scheme of any URL placed in a URL-bearing HTML attribute. This suite pins the scheme allowlist (http/https/relative pass, everything else collapses to
 * ""), the scheme-obfuscation defenses the platform URL parser provides (mixed case, embedded tab/newline, leading whitespace), and the shape of the window.safeUrl
 * assignment that the shared utilities script emits. The runtime validity of the emitted assignment is exercised by the shared-runtime suite, which executes the
 * shared utilities script and calls window.safeUrl.
 */
import { clientSafeUrl, generateClientSafeUrlAssignment } from "./clientUrl.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("clientSafeUrl", () => {

  test("passes through http, https, relative, and protocol-relative URLs unchanged", () => {

    /* These are the legitimate shapes a logo or icon URL takes: an absolute http(s) URL from operator config or the Channels DVR logo API, a relative path, or a
     * protocol-relative reference. None carries a dangerous scheme, so each returns verbatim (the caller still HTML-escapes it for the attribute).
     */
    assert.equal(clientSafeUrl("https://logo.example/x.png"), "https://logo.example/x.png");
    assert.equal(clientSafeUrl("http://logo.example/x.png"), "http://logo.example/x.png");
    assert.equal(clientSafeUrl("/assets/logo.png"), "/assets/logo.png");
    assert.equal(clientSafeUrl("logo.png"), "logo.png");
    assert.equal(clientSafeUrl("//cdn.example/x.png"), "//cdn.example/x.png");
    assert.equal(clientSafeUrl(""), "");
  });

  test("collapses dangerous schemes to the empty string so the attribute renders inert", () => {

    /* The vectors that matter when a URL lands in an attribute: javascript: (script execution in href/some src contexts), data: (inline document/script payloads),
     * vbscript:/file:/blob: (legacy and local-resource schemes). Each must return "" so the rendered src/href is empty and loads nothing.
     */
    assert.equal(clientSafeUrl("javascript:alert(1)"), "");
    assert.equal(clientSafeUrl("data:text/html,<script>alert(1)</script>"), "");
    assert.equal(clientSafeUrl("vbscript:msgbox(1)"), "");
    assert.equal(clientSafeUrl("file:///etc/passwd"), "");
  });

  test("defeats scheme-obfuscation tricks via the platform URL parser", () => {

    /* A hand-rolled scheme regex is fragile against the normalizations the WHATWG URL parser performs: the scheme is case-insensitive, leading whitespace and
     * control characters are stripped, and tabs/newlines embedded anywhere in the URL are removed before parsing. All of these resolve to the javascript: scheme
     * and must be rejected. This is the reason clientSafeUrl delegates to new URL rather than matching a prefix.
     */
    assert.equal(clientSafeUrl("JaVaScRiPt:alert(1)"), "");
    assert.equal(clientSafeUrl("  javascript:alert(1)"), "");
    assert.equal(clientSafeUrl("java\tscript:alert(1)"), "");
    assert.equal(clientSafeUrl("java\nscript:alert(1)"), "");
  });
});

describe("generateClientSafeUrlAssignment", () => {

  test("emits statements that install window.safeUrl backed by an IIFE-local const alias", () => {

    /* The shared utilities script concatenates this fragment near the top of its IIFE alongside the escape assignment. It must install the global surface
     * (window.safeUrl) and the local const alias that shared.ts's own renderers call without a global property lookup. We pin both so a refactor that dropped
     * either binding surfaces here.
     */
    const snippet = generateClientSafeUrlAssignment();

    assert.match(snippet, /const safeUrl\s*=\s*function/, "the fragment must define the local const safeUrl alias");
    assert.match(snippet, /window\.safeUrl\s*=\s*safeUrl;/, "the fragment must install window.safeUrl from the local alias");
  });
});
