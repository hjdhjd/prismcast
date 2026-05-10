/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * status.ts: Client-side JavaScript generator for the PrismCast status display. The pure handler logic lives in status.handlers.ts as ports-and-adapters TS
 * functions over a HandlerContext; this file is the boundary that builds the script string.
 *
 * The generator emits three sections in order:
 *
 *   1. The script-side constants (HANDLER_CONSTANTS) as `const NAME = <JSON.stringify(value)>;` lines. The handler bodies reference these by their TS-side
 *      identifiers; emitting them via JSON.stringify keeps the constants single-sourced rather than hand-mirrored as string templates.
 *
 *   2. The pure handler functions (HANDLER_FUNCTIONS) via Function.prototype.toString() concatenation. Each function declaration emits its full source; the
 *      browser parses them as siblings in script-tag scope, so cross-handler references resolve naturally. TypeScript type annotations strip to whitespace via
 *      Node's strip-types and to nothing via tsc compilation; the runtime body is identical between test and production.
 *
 *   3. A small IIFE that constructs the initial state, resolves the window.* externals, opens the EventSource, dispatches incoming events to the extracted
 *      handlers, attaches the window-bound trampolines (toggleStreamPopover, toggleStreamDetails, copyOverviewPlaylistUrl), and starts the periodic timers
 *      (1-second duration tick, 45-second staleness watchdog) plus the visibility-driven reconnect listener and the iPad tooltip initializer.
 *
 * Every function the IIFE references is one of: a sibling extracted via .toString() (resolves at the script-tag top scope), or a browser global (Date,
 * EventSource, JSON, document, setInterval, window). The IIFE has no closures over server-side TS state.
 */
import { HANDLER_CONSTANTS, HANDLER_FUNCTIONS } from "./status.handlers.ts";

/**
 * Generates the inline <script> block that drives the status display. Returns a complete <script>...</script> string ready to embed in the page wrapper.
 * @returns The script string.
 */
export function generateStatusScript(): string {

  // Section 1: emit each constant as a `const name = <json>;` declaration. Stable serialization (Object.entries are emitted in declaration order from
  // status.handlers.ts) keeps generation deterministic across calls.
  const constantDecls = HANDLER_CONSTANTS.map((c) => "const " + c.name + " = " + JSON.stringify(c.value) + ";").join("\n");

  // Section 2: emit each handler/helper as its source via Function.prototype.toString(). Function declarations are hoisted in script-tag scope, so cross-handler
  // references resolve regardless of declaration order; the order chosen in HANDLER_FUNCTIONS reflects the logical groups (formatters, renderers, mutators,
  // schedulers, handlers, trampolines).
  const handlerSources = HANDLER_FUNCTIONS.map((fn) => fn.toString()).join("\n\n");

  // Section 3: the IIFE. Wires the runtime side - constructs the state and externals, opens the EventSource, dispatches events, attaches window.* trampolines,
  // starts the timers. The handler-side state mutations all go through the ctx that is built here. The shared sibling-script values (channelDisplayHtml,
  // channelTable, copyToClipboard, dropdowns) are referenced as bare identifiers, matching the existing client-side idiom - shared.ts assigns them via
  // window.X = ... before this script loads, and bare identifiers in non-strict script-tag scope resolve through the global object. updateRestartDialogStatus
  // is wrapped in a typeof guard inside a getter because config.ts loads after status.ts; a streamRemoved event arriving before config.ts has registered the
  // callback must not throw.
  //
  // Trampoline capture: the handlers toggleStreamPopover, toggleStreamDetails, and copyOverviewPlaylistUrl share names with the window.* properties we bind
  // them to. In classic-script scope, function declarations live as properties of the global object, so assigning window.toggleStreamPopover = ... would
  // overwrite the global binding...subsequent bare-identifier lookups inside the arrow body would then resolve back to the arrow itself, causing infinite
  // recursion. We capture the original function references in IIFE-local consts first; the arrow bodies close over those locals instead of the global.
  const iife = [

    "(function() {",
    "  const state = {",
    "    expandedStreams: {},",
    "    hiddenSince: 0,",
    "    lastStatusEventTime: Date.now(),",
    "    popoverRenderPending: false,",
    "    streamData: {},",
    "    systemData: null,",
    "    tableRenderPending: false",
    "  };",
    "  const externals = {",
    "    channelDisplayHtml: channelDisplayHtml,",
    "    channelTable: channelTable,",
    "    copyToClipboard: copyToClipboard,",
    "    dropdowns: dropdowns,",
    "    get updateRestartDialogStatus() { return (typeof updateRestartDialogStatus !== 'undefined') ? updateRestartDialogStatus : undefined; }",
    "  };",
    "  const ctx = { document: document, externals: externals, state: state };",
    "  let statusEventSource = null;",
    "  function connectStatusSSE() {",
    "    if(statusEventSource) { statusEventSource.close(); }",
    "    statusEventSource = new EventSource('/streams/status');",
    "    state.lastStatusEventTime = Date.now();",
    "    function on(event, handler) {",
    "      statusEventSource.addEventListener(event, (e) => {",
    "        state.lastStatusEventTime = Date.now();",
    "        if(handler) { handler(e); }",
    "      });",
    "    }",
    "    on('heartbeat');",
    "    on('snapshot', (e) => handleSnapshot(JSON.parse(e.data), ctx));",
    "    on('streamAdded', (e) => handleStreamAdded(JSON.parse(e.data), ctx));",
    "    on('streamRemoved', (e) => handleStreamRemoved(JSON.parse(e.data), ctx));",
    "    on('streamHealthChanged', (e) => handleStreamHealthChanged(JSON.parse(e.data), ctx));",
    "    on('systemStatusChanged', (e) => handleSystemStatusChanged(JSON.parse(e.data), ctx));",
    "    on('healthChanged', (e) => handleHealthChanged(JSON.parse(e.data), ctx));",
    "    on('channelUpdate', (e) => handleChannelUpdate(JSON.parse(e.data), ctx));",
    "    statusEventSource.onerror = () => handleSseError(ctx);",
    "  }",
    "  const togglePopoverImpl = toggleStreamPopover;",
    "  const toggleDetailsImpl = toggleStreamDetails;",
    "  const copyPlaylistImpl = copyOverviewPlaylistUrl;",
    "  window.toggleStreamPopover = () => togglePopoverImpl(ctx);",
    "  window.toggleStreamDetails = (id) => toggleDetailsImpl(id, ctx);",
    "  window.copyOverviewPlaylistUrl = () => copyPlaylistImpl(ctx);",
    "  connectStatusSSE();",
    "  setInterval(() => updateDurations(ctx), 1000);",
    "  setInterval(() => {",
    "    if((Date.now() - state.lastStatusEventTime) > 45000) { connectStatusSSE(); }",
    "  }, 45000);",
    "  document.addEventListener('visibilitychange', () => handleVisibilityChange(ctx, connectStatusSSE));",
    "  initIPadTooltips(ctx);",
    "})();"
  ].join("\n");

  return [ "<script>", constantDecls, handlerSources, iife, "</script>" ].join("\n");
}
