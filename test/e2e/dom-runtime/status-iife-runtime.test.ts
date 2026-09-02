/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * status-iife-runtime.test.ts: DOM-runtime coverage for the IIFE wiring layer of status.ts - the boundary between the pure handler module and the window.*
 * trampolines that the project-wide action dispatcher invokes via its registerAction handlers. The sibling suites cover the surface around this boundary but not
 * the boundary itself:
 *
 *   - status.test.ts (next to status.ts) asserts the SHAPE of the emitted string: "the script contains window.toggleStreamPopover = ...". It never executes the
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
 * The harness loads the served landing-page HTML, stubs EventSource on the synthetic Window (happy-dom does not implement it), runs the shared utilities
 * script to install the externals (channelTable, dropdowns, copyToClipboard) that the status IIFE captures at init time, then runs the status script. Tests
 * call window.toggleStreamPopover, window.toggleStreamDetails, and window.copyOverviewPlaylistUrl directly - the same trampolines the project-wide dispatcher
 * invokes when a click matches a data-click-action attribute in the rendered page.
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

  // Stub EventSource. The IIFE calls new EventSource('/streams/status') inside connectStatusSSE, and happy-dom does not implement the class. The stub
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

/**
 * Builds a StreamSummary payload as a JSON literal for embedding in an evaluate() expression. Only the fields the render path reads are populated; the count
 * assertions care about how many entries reach the state map, not what those entries hold.
 * @param id - The stream id, which is the key the handlers file the entry under.
 * @returns The payload serialized as a JSON literal.
 */
function streamPayload(id: string): string {

  return JSON.stringify({

    clientCount: 0,
    clients: [],
    duration: 0,
    health: "healthy",
    id,
    memoryBytes: 0,
    pageReloadsInWindow: 0,
    recoveryAttempts: 0,
    startTime: new Date().toISOString(),
    url: "https://example.test/watch"
  });
}

describe("status.ts: emitted IIFE wiring (script-tag runtime)", () => {

  test("window.toggleStreamPopover invokes the underlying handler without infinite recursion", async () => {

    /* This test asserts the trampoline-wiring rule for window.toggleStreamPopover. Because classic-script top-level function declarations create properties on the
     * global object, a naive window.toggleStreamPopover = () => toggleStreamPopover(ctx) would shadow the global binding for toggleStreamPopover; the arrow's
     * bare-identifier lookup would then resolve back to the arrow itself, blowing the stack with RangeError on the first invocation. The IIFE avoids this by capturing
     * the original function reference in an IIFE-local const before reassigning the global. This test calls window.toggleStreamPopover and asserts that no
     * RangeError (or any other error) propagates - if the trampoline ever recurses on itself, the assertion catches it before release.
     */
    await using ctx = await setupStatusIifeRuntime();

    assert.equal(ctx.evaluate("typeof window.toggleStreamPopover"), "function", "toggleStreamPopover must be wired as a function on window");
    assert.doesNotThrow(() => ctx.evaluate("window.toggleStreamPopover()"), "toggleStreamPopover must not throw - empty streamData bails the body without error");
  });

  test("window.toggleStreamDetails invokes the underlying handler without infinite recursion", async () => {

    /* Same trampoline pattern as toggleStreamPopover, same shadow-the-global risk. The test asserts the rule for the sibling trampoline so a future refactor
     * cannot reintroduce the bug for toggleStreamDetails alone.
     */
    await using ctx = await setupStatusIifeRuntime();

    assert.equal(ctx.evaluate("typeof window.toggleStreamDetails"), "function", "toggleStreamDetails must be wired as a function on window");

    // Unknown id is fine - the underlying renderStreamsTable iterates the (empty) streamData and no-ops without consulting the id.
    assert.doesNotThrow(() => ctx.evaluate("window.toggleStreamDetails('nonexistent-id')"), "toggleStreamDetails must not throw on an unknown id");
  });

  test("window.copyOverviewPlaylistUrl invokes the underlying handler without infinite recursion", async () => {

    /* Same trampoline pattern again. This is the last window.* binding the IIFE installs today. Every trampoline the IIFE installs is covered as a set, so
     * adding another should extend this suite alongside the IIFE change.
     */
    await using ctx = await setupStatusIifeRuntime();

    assert.equal(ctx.evaluate("typeof window.copyOverviewPlaylistUrl"), "function", "copyOverviewPlaylistUrl must be wired as a function on window");

    // Missing #overview-playlist-url is fine - the handler body bails out before invoking copyToClipboard.
    assert.doesNotThrow(() => ctx.evaluate("window.copyOverviewPlaylistUrl()"), "copyOverviewPlaylistUrl must not throw when the playlist url element is absent");
  });

  test("window.copyOverviewPlaylistUrl reaches the handler body and delegates to ctx.externals.copyToClipboard", async () => {

    /* The recursion checks above prove the trampolines don't loop on themselves, but they cannot distinguish "trampoline correctly delegates to handler" from
     * "trampoline silently no-ops in a way that swallows the bug". This test asserts the positive case: when called with a populated DOM, copyOverviewPlaylistUrl
     * must actually invoke the externals.copyToClipboard surface the IIFE captured at init time.
     *
     * We install a recording spy on window.copyToClipboard between the shared-utilities script and the status script. The IIFE captures the bare identifier
     * copyToClipboard at construction time, so the spy must be planted after shared.ts installs the real implementation but before the status IIFE runs. We do
     * that inline below instead of through the shared setup helper because this is the only test that needs the spy.
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

describe("status.ts: the active stream count channel (script-tag runtime)", () => {

  test("window.activeStreamCount reports the live stream count and tracks it as streams come and go", async () => {

    /* config.ts's restart dialog and upgrade flow read the stream count from this channel, and neither can be trusted further than the channel itself. The
     * sibling assertion in status.test.ts asserts that the assignment is emitted; it cannot say what number the getter yields, because it never runs the script.
     * This test does, and it reaches the count the only honest way: the state object the getter closes over is IIFE-local by design, so the test drives it
     * through the SSE handlers the IIFE registers rather than reaching around them.
     *
     * Tracking matters as much as the initial reading. The channel is a getter over live state rather than a value captured at definition time, so a snapshot
     * taken once would satisfy the first assertion and still be wrong for every later one. Adding and then removing streams is what separates the two.
     */
    await using ctx = await createDomTestContext();

    // The shared setup's EventSource stub discards its listeners. This one records them so the test can deliver events, which is the only route into the IIFE's
    // private state.
    ctx.evaluate([

      "window.harnessSseListeners = {};",
      "window.EventSource = function() {",
      "  this.addEventListener = function(type, handler) {",
      "    (window.harnessSseListeners[type] = window.harnessSseListeners[type] || []).push(handler);",
      "  };",
      "  this.close = function() {};",
      "  this.onerror = null;",
      "};",
      "window.harnessDispatch = function(type, payload) {",
      "  var handlers = window.harnessSseListeners[type] || [];",
      "  for(var i = 0; i < handlers.length; i++) { handlers[i]({ data: JSON.stringify(payload) }); }",
      "};"
    ].join("\n"));

    const sharedRan = ctx.runScripts((s) => s.content.includes("window.channelTable"));

    assert.equal(sharedRan.length, 1, "exactly one shared utilities script should run");

    const statusRan = ctx.runScripts((s) => s.content.includes("window.toggleStreamPopover"));

    assert.equal(statusRan.length, 1, "exactly one status script should run");

    assert.equal(ctx.evaluate("typeof window.activeStreamCount"), "number", "the channel must read as a number, which is what config.ts's typeof guard tests");
    assert.equal(ctx.evaluate("window.activeStreamCount"), 0, "a page with no streams reports zero");

    ctx.evaluate("window.harnessDispatch('streamAdded', " + streamPayload("s1") + ")");

    assert.equal(ctx.evaluate("window.activeStreamCount"), 1, "the count follows the first stream added");

    ctx.evaluate("window.harnessDispatch('streamAdded', " + streamPayload("s2") + ")");

    assert.equal(ctx.evaluate("window.activeStreamCount"), 2, "the count follows a second stream added");

    ctx.evaluate("window.harnessDispatch('streamRemoved', { id: 's1' })");

    assert.equal(ctx.evaluate("window.activeStreamCount"), 1, "the count follows a stream removed");

    ctx.evaluate("window.harnessDispatch('streamRemoved', { id: 's2' })");

    // Reaching zero is the reading that authorizes a deferred restart in config.ts, so it is worth asserting outright rather than inferring from the decrements.
    assert.equal(ctx.evaluate("window.activeStreamCount"), 0, "the count returns to zero once the last stream is gone");
  });
});
