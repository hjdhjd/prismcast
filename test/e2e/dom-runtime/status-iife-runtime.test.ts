/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * status-iife-runtime.test.ts: DOM-runtime coverage for the IIFE wiring layer of status.ts - the seam between the pure handler module and the window.* trampolines
 * that the page's onclick attributes call. The sibling suites cover the surface around this seam but not the seam itself:
 *
 *   - status.test.ts (next to status.ts) pins the SHAPE of the emitted string: "the script contains window.toggleStreamPopover = ...". It never executes the
 *     script, so any runtime collision is invisible to it.
 *   - status-handlers-runtime.test.ts imports the handlers as TypeScript free-standing functions and calls them with a synthetic HandlerContext literal. It never
 *     goes through window.toggleStreamPopover, so the trampoline binding pattern is never exercised.
 *
 * The bug class this suite catches lives in that gap. In particular: when the emitted script is parsed as a classic <script> block, function declarations at top
 * level become properties of the global object. The IIFE then assigns window.toggleStreamPopover = () => toggleStreamPopover(ctx), which - if the arrow body's
 * bare-identifier lookup resolves through the now-shadowed global - calls itself instead of the underlying handler. A single click on the header button blows
 * the stack with RangeError. Neither sibling suite reproduces this because neither evaluates the emitted script in a classic-script context with the function
 * declarations and the window assignments living in the same global scope.
 *
 * The harness loads the served landing-page HTML, stubs EventSource on the synthetic Window (happy-dom v20 does not implement it), runs the shared utilities
 * script to install the externals (channelTable, dropdowns, copyToClipboard) that the status IIFE captures at init time, then runs the status script. Tests
 * call window.toggleStreamPopover, window.toggleStreamDetails, and window.copyOverviewPlaylistUrl through the same surface the page's onclick attributes use.
 */
import { describe, test } from "node:test";
import type { DisposableDomTestContext } from "../../helpers/dom.helpers.ts";
import assert from "node:assert/strict";
import { createDomTestContext } from "../../helpers/dom.helpers.ts";

/**
 * Boot a DOM context, stub EventSource, run the shared utilities script, then run the status script. Returns the context with window.toggleStreamPopover,
 * window.toggleStreamDetails, and window.copyOverviewPlaylistUrl wired up exactly as a real browser would see them after the page finishes loading.
 *
 * Script execution order matters: the status IIFE captures the externals namespace by evaluating bare identifiers (channelDisplayHtml, channelTable,
 * copyToClipboard, dropdowns) at init time. Running shared.ts first installs those globals so the capture sees the real values. Reversing the order would
 * crash the IIFE with ReferenceError before the trampolines are bound.
 */
async function setupStatusIifeRuntime(): Promise<DisposableDomTestContext> {

  const ctx = await createDomTestContext();

  // Stub EventSource. The IIFE calls new EventSource('/streams/status') inside connectStatusSSE, and happy-dom v20 does not implement the class. The stub
  // satisfies the surface the IIFE exercises: addEventListener registration, close on reconnect, onerror assignment. No events are dispatched - the test
  // body asserts on the trampoline wiring, not on event delivery.
  ctx.evaluate([

    "window.EventSource = function() {",
    "  this.addEventListener = function() {};",
    "  this.close = function() {};",
    "  this.onerror = null;",
    "};"
  ].join("\n"));

  // Run the shared utilities script. The window.channelTable assignment is unique to shared.ts, so the predicate selects it without ambiguity.
  const sharedRan = ctx.runScripts((s) => s.content.includes("window.channelTable"));

  if(sharedRan.length !== 1) {

    throw new Error("setupStatusIifeRuntime: expected exactly one shared utilities script; got " + String(sharedRan.length));
  }

  // Run the status script. The window.toggleStreamPopover assignment is unique to status.ts, so the predicate selects it without ambiguity.
  const statusRan = ctx.runScripts((s) => s.content.includes("window.toggleStreamPopover"));

  if(statusRan.length !== 1) {

    throw new Error("setupStatusIifeRuntime: expected exactly one status script; got " + String(statusRan.length));
  }

  return ctx;
}

describe("status.ts: emitted IIFE wiring (script-tag runtime)", () => {

  test("window.toggleStreamPopover invokes the underlying handler without infinite recursion", async () => {

    /* The regression this pins: prior to the fix at status.ts:86, the IIFE bound window.toggleStreamPopover = () => toggleStreamPopover(ctx). Because function
     * declarations at script-tag top level create properties on the global object, the assignment overwrote the global binding for toggleStreamPopover; the
     * arrow's bare-identifier lookup then resolved back to the arrow itself, blowing the stack with RangeError on the first invocation. The fix captures the
     * original function reference in an IIFE-local const before reassigning the global. This test calls window.toggleStreamPopover and asserts that no
     * RangeError (or any other error) propagates - if the trampoline ever recurses on itself again, the assertion catches it before release.
     */
    await using ctx = await setupStatusIifeRuntime();

    assert.equal(ctx.evaluate("typeof window.toggleStreamPopover"), "function", "toggleStreamPopover must be wired as a function on window");
    assert.doesNotThrow(() => ctx.evaluate("window.toggleStreamPopover()"), "toggleStreamPopover must not throw - empty streamData bails the body without error");
  });

  test("window.toggleStreamDetails invokes the underlying handler without infinite recursion", async () => {

    /* Same trampoline pattern as toggleStreamPopover, same shadow-the-global risk. The test pins the invariant for the sibling trampoline so a future refactor
     * cannot reintroduce the bug for toggleStreamDetails alone.
     */
    await using ctx = await setupStatusIifeRuntime();

    assert.equal(ctx.evaluate("typeof window.toggleStreamDetails"), "function", "toggleStreamDetails must be wired as a function on window");

    // Unknown id is fine - the underlying renderStreamsTable iterates the (empty) streamData and no-ops without consulting the id.
    assert.doesNotThrow(() => ctx.evaluate("window.toggleStreamDetails('nonexistent-id')"), "toggleStreamDetails must not throw on an unknown id");
  });

  test("window.copyOverviewPlaylistUrl invokes the underlying handler without infinite recursion", async () => {

    /* Same trampoline pattern again. This is the third and final window.* binding the IIFE installs. The triplet is covered as a set so adding a fourth
     * trampoline in the future would prompt the author to extend this suite alongside the IIFE change.
     */
    await using ctx = await setupStatusIifeRuntime();

    assert.equal(ctx.evaluate("typeof window.copyOverviewPlaylistUrl"), "function", "copyOverviewPlaylistUrl must be wired as a function on window");

    // Missing #overview-playlist-url is fine - the handler body bails out before invoking copyToClipboard.
    assert.doesNotThrow(() => ctx.evaluate("window.copyOverviewPlaylistUrl()"), "copyOverviewPlaylistUrl must not throw when the playlist url element is absent");
  });

  test("window.copyOverviewPlaylistUrl reaches the handler body and delegates to ctx.externals.copyToClipboard", async () => {

    /* The recursion checks above prove the trampolines don't loop on themselves, but they cannot distinguish "trampoline correctly delegates to handler" from
     * "trampoline silently no-ops in a way that swallows the bug". This test pins the positive case: when called with a populated DOM, copyOverviewPlaylistUrl
     * must actually invoke the externals.copyToClipboard surface the IIFE captured at init time.
     *
     * We replace window.copyToClipboard with a recording spy AFTER shared.ts ran (which installed the real implementation) but the spy assignment happens
     * after the IIFE already captured the reference - so we need a slightly different approach: replace it BEFORE the status IIFE runs. We do that inline below
     * instead of through the shared setup helper because this is the only test that needs the spy.
     */
    await using ctx = await createDomTestContext();

    ctx.evaluate([

      "window.EventSource = function() {",
      "  this.addEventListener = function() {};",
      "  this.close = function() {};",
      "  this.onerror = null;",
      "};"
    ].join("\n"));

    const sharedRan = ctx.runScripts((s) => s.content.includes("window.channelTable"));

    assert.equal(sharedRan.length, 1, "exactly one shared utilities script should run");

    // Install the spy AFTER shared.ts (which set the real copyToClipboard) but BEFORE the status IIFE (which captures the reference). The IIFE's externals
    // object reads the bare identifier copyToClipboard at construction time, so replacing window.copyToClipboard between the two scripts is what plants the
    // spy inside ctx.externals.
    ctx.evaluate([

      "window.copyToClipboardCalls = [];",
      "window.copyToClipboard = function(text, message) {",
      "  window.copyToClipboardCalls.push({ text: text, message: message });",
      "};"
    ].join("\n"));

    // Seed the DOM with a #overview-playlist-url element so the handler body has something to read. The rendered landing page provides this, but we install
    // an explicit value here so the assertion below has a known textContent to compare against.
    ctx.evaluate([

      "var existing = document.getElementById('overview-playlist-url');",
      "if(!existing) {",
      "  existing = document.createElement('span');",
      "  existing.id = 'overview-playlist-url';",
      "  document.body.appendChild(existing);",
      "}",
      "existing.textContent = 'http://example.test/playlist.m3u';"
    ].join("\n"));

    const statusRan = ctx.runScripts((s) => s.content.includes("window.toggleStreamPopover"));

    assert.equal(statusRan.length, 1, "exactly one status script should run");

    ctx.evaluate("window.copyOverviewPlaylistUrl()");

    const calls = ctx.evaluateJson("window.copyToClipboardCalls") as { text: string; message: string }[];

    assert.equal(calls.length, 1, "copyOverviewPlaylistUrl should delegate exactly once to copyToClipboard");

    const [firstCall] = calls;

    assert.ok(firstCall, "first recorded call must exist after the length assertion above");
    assert.equal(firstCall.text, "http://example.test/playlist.m3u", "copyToClipboard should receive the textContent of #overview-playlist-url");
    assert.match(firstCall.message, /Playlist URL copied/, "copyToClipboard should receive the operator-facing success message");
  });
});
