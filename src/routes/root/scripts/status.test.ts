/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * status.test.ts: Unit tests for the client-side status display script generator. The module exports a single function that returns an HTML <script> block
 * containing the SSE handlers for stream and system status, the streams table renderer, and the popover/tooltip support code. We test the generated string for
 * structural invariants - presence of expected SSE event handlers, render functions, and helpers - without executing the script in any DOM runtime.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../../../testing.helpers.ts";
import { generateStatusScript } from "./status.ts";

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

    // The server emits: heartbeat, snapshot, streamAdded, streamRemoved, streamHealthChanged, systemStatusChanged, healthChanged, channelUpdate. Each must be
    // wired so the page reacts to that event.
    const script = generateStatusScript();
    const events = [ "heartbeat", "snapshot", "streamAdded", "streamRemoved", "streamHealthChanged", "systemStatusChanged", "healthChanged", "channelUpdate" ];

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

    // The helpers all reuse module-scope maps (clientTypeLabels, nativeResolutionLabels, rowTints, healthLabels). Their presence locks the public surface that
    // the SSE handlers depend on. Patterns are loose on the parameter close-paren because Node's strip-types replaces TypeScript annotations with whitespace,
    // so the emitted-script source has padding between the parameter name and ")".
    const script = generateStatusScript();

    assert.match(script, /function formatDuration\(seconds\b/);
    assert.match(script, /function formatBytes\(bytes\b/);
    assert.match(script, /function formatTime\(isoString\b/);
    assert.match(script, /function formatTimeAgo\(ts\b/);
    assert.match(script, /function formatLastIssue\(s\b/);
    assert.match(script, /function formatAutoRecovery\(s\b/);
    assert.match(script, /function formatClients\(s\b/);
  });

  test("declares the recovering-level label resolver with all four documented levels", () => {

    // getRecoveringLabel is the level-based mapping for in-progress recovery. The four documented escalation levels are 1, 2, 3, and 4+. We confirm each case
    // arm is present in source. The parameter regex is loose to tolerate the type-annotation-to-whitespace substitution Node's strip-types performs.
    const script = generateStatusScript();

    assert.match(script, /function getRecoveringLabel\(level\b/);
    assert.match(script, /case 1:/);
    assert.match(script, /case 2:/);
    assert.match(script, /case 3:/);
    assert.match(script, /level >= 4/);
  });

  test("exposes the popover toggle, copy URL helper, and restart status callback on window", () => {

    // toggleStreamPopover is wired to the header button onclick. copyOverviewPlaylistUrl is invoked from the Quick Start copy button. updateRestartDialogStatus
    // is referenced by the streamRemoved handler to advance a deferred restart -- post-refactor, via ctx.externals.updateRestartDialogStatus?.() rather than
    // the original `if(typeof updateRestartDialogStatus === "function")` guard. The optional-chain expression encodes the same "call only if present" semantics.
    const script = generateStatusScript();

    assert.match(script, /window\.toggleStreamPopover\s*=/);
    assert.match(script, /window\.copyOverviewPlaylistUrl\s*=/);
    assert.match(script, /externals\.updateRestartDialogStatus\?\.\(\)/);
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

  test("registers the channel and domain auth health updaters", () => {

    // applyHealthSnapshot is invoked from the snapshot SSE event. updateChannelHealth and updateDomainAuth are the targeted updaters for individual events.
    const script = generateStatusScript();

    assert.match(script, /function updateChannelHealth\(/);
    assert.match(script, /function updateDomainAuth\(/);
    assert.match(script, /function applyHealthSnapshot\(/);
  });

  test("starts the 1-second duration update interval at the bottom of the script", () => {

    // updateDurations is invoked every second to refresh on-screen stream durations. The setInterval call is required for the live counter to advance.
    // Post-refactor the call passes the context explicitly via an arrow trampoline (setInterval(() => updateDurations(ctx), 1000)) instead of a bare reference,
    // because the extracted handler takes ctx rather than reading from module-scope state.
    const script = generateStatusScript();

    assert.match(script, /setInterval\(\(\) => updateDurations\(ctx\),\s*1000\)/);
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
