/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * status.test.ts: Unit tests for the client-side status display script generator. The module exports a single function that returns an HTML <script> block
 * containing the SSE handlers for stream and system status, the streams table renderer, and the popover/tooltip support code. We test the generated string for
 * structural properties - presence of expected SSE event handlers, render functions, and helpers - without executing the script in any DOM runtime.
 */
import { describe, test } from "node:test";
import { HANDLER_FUNCTIONS } from "./status.handlers.ts";
import assert from "node:assert/strict";
import { clientEscapeHtml } from "./clientEscape.ts";
import { closePuppeteerStreamWssOnIdle } from "../../../testing.helpers.ts";
import { generateBadge } from "../../components.ts";
import { generateStatusScript } from "./status.ts";
import vm from "node:vm";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("generateStatusScript", () => {

  test("returns a non-empty <script>...</script> block", () => {

    const script = generateStatusScript();

    assert.equal(typeof script, "string");
    assert.ok(script.length > 1000, "script should be substantial; got " + String(script.length));
    assert.match(script, /^<script>/);
    assert.match(script, /<\/script>$/);
  });

  test("does not produce template-literal artifacts", () => {

    const script = generateStatusScript();

    assert.doesNotMatch(script, /\$\{undefined\}/);
    assert.doesNotMatch(script, /\[object Object\]/);
  });

  test("declares the SSE EventSource for /streams/status", () => {

    // The status script subscribes to /streams/status. The EventSource construction is the binding point that drives every subsequent stream/system update.
    const script = generateStatusScript();

    assert.match(script, /new EventSource\(['"]\/streams\/status['"]\)/);
    assert.match(script, /function connectStatusSSE\(/);
  });

  test("registers handlers for the documented SSE event types", () => {

    // The server emits: heartbeat, snapshot, streamAdded, streamRemoved, streamHealthChanged, systemStatusChanged, channelUpdate. Each must be wired so the page
    // reacts to that event. Channel health and domain auth changes arrive as channelUpdate patches built by healthBridge, never as a dedicated event type.
    const script = generateStatusScript();
    const events = [ "heartbeat", "snapshot", "streamAdded", "streamRemoved", "streamHealthChanged", "systemStatusChanged", "channelUpdate" ];

    for(const event of events) {

      const re = new RegExp("on\\(['\"]" + event + "['\"]");

      assert.match(script, re, "should register handler for SSE event " + event);
    }
  });

  test("renders streams via renderStreamsTable and provides scheduling wrappers", () => {

    // renderStreamsTable does the full rebuild path. scheduleTableRender / schedulePopoverRender batch updates within a single rAF so multiple SSE events in
    // rapid succession produce one DOM write rather than back-to-back rebuilds.
    const script = generateStatusScript();

    assert.match(script, /function renderStreamsTable\(/);
    assert.match(script, /function scheduleTableRender\(/);
    assert.match(script, /function schedulePopoverRender\(/);
    assert.match(script, /requestAnimationFrame/);
  });

  test("provides updateStreamRow as the targeted-update path", () => {

    // updateStreamRow is the targeted update path that avoids destroying image elements between health ticks. It must coexist with renderStreamsTable.
    const script = generateStatusScript();

    assert.match(script, /function updateStreamRow\(/);
  });

  test("declares helper formatters for duration, bytes, time, time-ago, and last-issue", () => {

    // formatClients reuses the module-scope clientTypeLabels map; the remaining formatters are pure numeric and string transforms. Their presence locks the
    // public surface that the SSE handlers depend on. Patterns are loose on the parameter close-paren because Node's strip-types replaces TypeScript annotations
    // with whitespace, so the emitted-script source has padding between the parameter name and ")".
    const script = generateStatusScript();

    assert.match(script, /function formatDuration\(seconds\b/);
    assert.match(script, /function formatBytes\(bytes\b/);
    assert.match(script, /function formatTime\(isoString\b/);
    assert.match(script, /function formatTimeAgo\(ts\b/);
    assert.match(script, /function formatLastIssue\(s\b/);
    assert.match(script, /function formatAutoRecovery\(s\b/);
    assert.match(script, /function formatClients\(s\b/);
  });

  test("declares the recovering-level label resolver with a case arm per ladder level and the level >= 4 branch", () => {

    // getRecoveringLabel is the level-based mapping for in-progress recovery. Its switch has a case arm for each of the escalation ladder's levels 1, 2, and 3,
    // and its default holds a level >= 4 branch for levels past the ladder's maximum. We confirm each case arm and the level >= 4 branch are present in source.
    // The parameter regex is loose to tolerate the type-annotation-to-whitespace substitution Node's strip-types performs.
    const script = generateStatusScript();

    assert.match(script, /function getRecoveringLabel\(level\b/);
    assert.match(script, /case 1:/);
    assert.match(script, /case 2:/);
    assert.match(script, /case 3:/);
    assert.match(script, /level >= 4/);
  });

  test("exposes the popover toggle, copy URL helper, and restart status callback on window", () => {

    // toggleStreamPopover is triggered by the project-wide action dispatcher via data-click-action on the header stream-count button, and copyOverviewPlaylistUrl
    // is dispatched the same way from the Quick Start copy button. updateRestartDialogStatus is referenced by the streamRemoved handler to advance a deferred
    // restart, via ctx.externals.updateRestartDialogStatus?.(). The optional-chain expression encodes "call only if present" semantics.
    const script = generateStatusScript();

    assert.match(script, /window\.toggleStreamPopover\s*=/);
    assert.match(script, /window\.copyOverviewPlaylistUrl\s*=/);
    assert.match(script, /externals\.updateRestartDialogStatus\?\.\(\)/);
  });

  test("publishes the active stream count on window as a getter over the live state", () => {

    /* config.ts's restart dialog and upgrade flow both read the active stream count from this channel, and config.ts's script is emitted first on the page, so
     * the channel has to exist by the time either of them runs. A substring assertion proves the channel is emitted and that it is defined as a getter over
     * state.streamData rather than a value captured once; it cannot prove the number it yields is right, which is what the dom-runtime consumption assertions cover
     * from the other side.
     */
    const script = generateStatusScript();

    assert.match(script, /Object\.defineProperty\(window, ['"]activeStreamCount['"]/);
    assert.match(script, /get: \(\) => Object\.keys\(state\.streamData\)\.length/);
  });

  test("registers the staleness watchdog at 45-second intervals for SSE reconnect", () => {

    // The status SSE has a 45-second staleness check that reconnects if no event arrives within the window. The interval must be present.
    const script = generateStatusScript();

    assert.match(script, /lastStatusEventTime/);
    assert.match(script, /45000/, "45-second staleness interval");
    assert.match(script, /setInterval\(/);
  });

  test("registers the visibilitychange listener for hidden-page reconnection", () => {

    // When the page returns from being hidden for >30s, the script reconnects to status SSE and re-activates the current tab.
    const script = generateStatusScript();

    assert.match(script, /document\.addEventListener\(['"]visibilitychange['"]/);
    assert.match(script, /document\.hidden/);
  });

  test("routes channelUpdate patches through the channelTable namespace", () => {

    // handleChannelUpdate is the sole client entry point for channel row changes - both snapshot catch-up (data.channelPatch) and live events flow through
    // channelTable.applyPatch. There are no client-side composers of row state to assert; that is the rule healthBridge enforces server-side.
    const script = generateStatusScript();

    assert.match(script, /function handleChannelUpdate\(/);
    assert.match(script, /channelTable\.applyPatch/);
  });

  test("starts the 1-second duration update interval at the bottom of the script", () => {

    // updateDurations is invoked every second to refresh on-screen stream durations. The setInterval call is required for the live counter to advance.
    // The call passes the context explicitly via an arrow trampoline (setInterval(() => updateDurations(ctx), 1000)) because the handler takes ctx rather than
    // reading from module-scope state.
    const script = generateStatusScript();

    assert.match(script, /setInterval\(\(\) => updateDurations\(ctx\),\s*1000\)/);
  });

  test("ships the badge builder and its serializer, which run in the browser with the client escaper alone", () => {

    /* The stream table draws its badges through the server's own builder, which the registry ships with the serializer it calls. Their sources run here in a bare
     * context holding the client escaper as escapeHtml and nothing else - the one binding the browser lends them - so a builder that reached for any other
     * helper would throw here as it would in the browser, while a Node row calling the imported builder would pass.
     */
    const sourceOf = (name: string): string => {

      const fn = HANDLER_FUNCTIONS.find((candidate) => candidate.name === name);

      assert.ok(fn, "the registry ships " + name);

      return fn.toString();
    };
    const run = (call: string): unknown => vm.runInNewContext(sourceOf("serializeAttrs") + "\n" + sourceOf("generateBadge") + "\n" + call,
      { escapeHtml: clientEscapeHtml });

    assert.equal(run("generateBadge('Native', { title: 'Native HLS', variant: 'flag' })"), generateBadge("Native", { title: "Native HLS", variant: "flag" }),
      "the shipped builder draws the native badge the server's builder draws");
    assert.equal(run("generateBadge('⚡ <h264>', { title: 'Hardware accelerated', variant: 'flag' })"),
      generateBadge("⚡ <h264>", { title: "Hardware accelerated", variant: "flag" }), "the shipped builder draws and escapes the hardware badge as the server does");

    const script = generateStatusScript();

    assert.match(script, /function generateBadge\(/, "the emitted script declares the badge builder");
    assert.match(script, /function serializeAttrs\(/, "the emitted script declares the attribute serializer");
  });

  test("returns identical output across calls (pure derivation)", () => {

    assert.equal(generateStatusScript(), generateStatusScript());
  });

  test("balances parentheses across the entire generated script", () => {

    const script = generateStatusScript();
    const opens = (script.match(/\(/g) ?? []).length;
    const closes = (script.match(/\)/g) ?? []).length;

    assert.equal(opens, closes, "paren balance (opens=" + String(opens) + ", closes=" + String(closes) + ")");
  });
});
