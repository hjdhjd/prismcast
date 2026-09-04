/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * status-handlers-runtime.test.ts: DOM-runtime coverage for the PrismCast status display script (src/routes/root/scripts/status.handlers.ts). The unit suite next
 * to status.ts asserts the SHAPE of the emitted string ("the script defines window.toggleStreamPopover"); this suite asserts the RUNTIME BEHAVIOR of every handler,
 * formatter, renderer, and DOM mutator the status display drives.
 *
 * The bug class this tier catches is the one most likely to bite the operator-facing UI: an off-by-one in formatDuration's threshold ladder, a wrong CSS-variable
 * token in renderHealthCellContent, a missed branch in updateStreamRow's structural-change classifier, a stale fallback in buildStreamPopoverContent, a typo in
 * the system-health element id. These regressions ship past the unit suite (the string shape is still right) and past the rendering suite (the static HTML shell
 * is still right) but blow up the moment a status event arrives and the operator stares at a wrong number, a missing badge, or an empty popover.
 *
 * Architectural note: this suite is structurally different from the sibling DOM-runtime suites (shared/channels/config). Those scripts have their handler
 * logic tangled with their IIFE init code, so the only way to exercise their behavior is to execute the emitted script string in a synthetic DOM via runScripts.
 * status.handlers.ts exposes every handler, formatter, renderer, and DOM mutator as free-standing TypeScript functions over a HandlerContext literal. That makes
 * them directly importable as TS - this suite calls them with synthetic context literals instead of running the emitted script. The trade-off is masterclass-worthy:
 * the cleaner production architecture earns simpler tests.
 *
 * Harness usage: createDomTestContext is reused for the synthetic Document. The bootApp listener is incidental (we don't fetch from these handlers), but the page
 * HTML provides realistic structural fixtures (#streams-tbody, #system-health, #stream-count, #stream-popover-menu, #toast-container) that the DOM mutators rely
 * on. For tests that don't touch the DOM (pure formatters, pure renderers), no context is constructed - the function is called with literal inputs. For tests that
 * exercise the rAF schedulers (updateStreamRow's fall-through, handleStreamHealthChanged's structural-change branch), we install a synchronous-fire stub so the
 * scheduled callback runs inline and post-rAF state is observable on the next assertion line.
 *
 * Pattern guidance for adding tests:
 *
 *   - Assert the contract, not historical incidents. "renderStreamsTable emits a row per stream with the data-id attribute" is the contract; a regression that dropped
 *     the data-id attribute from rendered rows is a symptom to derive coverage from but not the test name.
 *   - For pure functions, prefer literal inputs and direct return-value assertions. No DOM context, no externals plumbing.
 *   - For DOM mutators, build the HandlerContext via the file-local makeHandlerContext factory and let createDomTestContext provide the rendered page fixtures.
 *   - For SSE handlers, the assertion targets are state mutations, recorded externals invocations, and DOM side effects.
 *   - When a runtime rule reveals a real bug, assert current (buggy) behavior with a FIX-PENDING comment showing exactly which assertion to flip post-fix.
 *     Do NOT fix the production module in this suite - fixes are a separate authorized arc.
 */
import * as handlers from "../../../src/routes/root/scripts/status.handlers.ts";
import type { ClientExternals, ClientState, HandlerContext, SnapshotPayload, StreamSummary } from "../../../src/routes/root/scripts/status.handlers.ts";
import { after, before, describe, test } from "node:test";
import type { DisposableDomTestContext } from "../../helpers/dom.helpers.ts";
import assert from "node:assert/strict";
import { clientEscapeHtml } from "../../../src/routes/root/scripts/clientEscape.ts";
import { createDomTestContext } from "../../helpers/dom.helpers.ts";

// The bits-per-second a native stream reports in the rows that assert how the codec detail line renders a native bitrate.
const NATIVE_BANDWIDTH = 5000000;

/**
 * Recorded invocation log for the stubbed externals. Tests inspect this to assert that handlers delegated to the right window.* surface with the right arguments.
 * Numeric counters track call counts for shapeless surfaces (close, updateRestartDialogStatus); arrays track argument tuples for surfaces whose shape matters.
 */
interface ExternalsRecorder {

  applyPatchCalls: unknown[];
  channelDisplayCalls: { logoUrl: string | undefined; logoClass: string; name: string; textClass: string }[];
  copyToClipboardCalls: { message: string; text: string }[];
  dropdownsCloseCount: number;
  updateRestartDialogStatusCount: number;
}

/**
 * Builder shape returned by makeHandlerContext. The recorder is exposed alongside the context so tests can assert on call counts without reaching into closures.
 */
interface HandlerContextHarness {

  ctx: HandlerContext;
  recorder: ExternalsRecorder;
}

/**
 * Builds a HandlerContext literal over the supplied document. Externals are stubbed with recording surfaces so tests can assert on invocation shape. State
 * defaults to handlers.createInitialState(); callers can supply a partial override (most commonly a pre-seeded streamData/systemData/expandedStreams) which is merged on
 * top of the defaults. The optional `updateRestartDialogStatusDefined` flag controls whether the optional config.ts trampoline is wired - set to false to assert
 * the streamRemoved-without-restart-dialog branch.
 */
function makeHandlerContext(document: Document, options?: {
  readonly state?: Partial<ClientState>;
  readonly updateRestartDialogStatusDefined?: boolean;
}): HandlerContextHarness {

  const recorder: ExternalsRecorder = {

    applyPatchCalls: [],
    channelDisplayCalls: [],
    copyToClipboardCalls: [],
    dropdownsCloseCount: 0,
    updateRestartDialogStatusCount: 0
  };

  // Production's channelDisplayHtml renders an <img> + <span> for the logo path and a bare <span> when the logo is missing. The test stub mirrors the bare-span
  // shape for both branches so assertions on rendered HTML can match against a deterministic, readable substring like "<span class=\"channel-text\">Foo</span>".
  // Tests that need to verify logo-vs-text branching read the channelDisplayCalls log instead of inspecting the HTML.
  const externals: ClientExternals = {

    channelDisplayHtml(logoUrl, name, logoClass, textClass): string {

      recorder.channelDisplayCalls.push({ logoClass, logoUrl, name, textClass });

      return "<span class=\"" + textClass + "\">" + name + "</span>";
    },
    channelTable: { applyPatch: (patch: unknown): void => { recorder.applyPatchCalls.push(patch); } },
    copyToClipboard: (text: string, message: string): void => { recorder.copyToClipboardCalls.push({ message, text }); },
    dropdowns: { close: (): void => { recorder.dropdownsCloseCount++; } },
    updateRestartDialogStatus: (options?.updateRestartDialogStatusDefined === false) ? undefined : (): void => { recorder.updateRestartDialogStatusCount++; }
  };

  const state: ClientState = { ...handlers.createInitialState(), ...(options?.state ?? {}) };

  return {

    ctx: { document, externals, state },
    recorder
  };
}

/**
 * Minimal StreamSummary factory. Defaults reflect a fresh, healthy capture stream with no clients - tests override the fields they care about. Spread-merge keeps
 * the call site readable: makeStream({ id: "5", health: "stalled", clientCount: 3 }) reads as a positive description of the test scenario.
 */
function makeStream(overrides?: Partial<StreamSummary>): StreamSummary {

  return {

    clientCount: 0,
    clients: [],
    duration: 0,
    health: "healthy",
    id: "stream-1",
    memoryBytes: 0,
    pageReloadsInWindow: 0,
    recoveryAttempts: 0,
    startTime: new Date().toISOString(),
    url: "https://example.test/watch",
    ...(overrides ?? {})
  };
}

/* Global stubs. Two browser globals that the handlers reference as free identifiers must be seeded on globalThis for the duration of this file:
 *
 *   - requestAnimationFrame: Node does not provide it; the schedulers in status.handlers.ts reference it as a free identifier. Synchronous-fire is the simplest
 *     model so the scheduled callback runs inline and the test's next assertion sees post-rAF state.
 *   - escapeHtml: the shared client-escape SSOT. Production installs window.escapeHtml via shared.ts and the handlers and pure formatters reference it as a bare
 *     global (it is intentionally not a context port - see the ClientExternals doc in status.handlers.ts). The suite provides the real clientEscapeHtml so the
 *     formatters escape exactly as they do in the browser.
 *
 * The before/after pair scopes both installs to this file; we restore the originals (typically undefined) on teardown so other test files are unaffected.
 */
interface EscapeHtmlGlobal {

  escapeHtml?: (value: string) => string;
}

let originalRaf: typeof globalThis.requestAnimationFrame | undefined;

before(() => {

  originalRaf = globalThis.requestAnimationFrame;

  globalThis.requestAnimationFrame = (cb: FrameRequestCallback): number => {

    cb(0);

    return 0;
  };

  (globalThis as EscapeHtmlGlobal).escapeHtml = clientEscapeHtml;
});

after(() => {

  if(originalRaf === undefined) {

    delete (globalThis as { requestAnimationFrame?: typeof requestAnimationFrame }).requestAnimationFrame;
  } else {

    globalThis.requestAnimationFrame = originalRaf;
  }

  delete (globalThis as EscapeHtmlGlobal).escapeHtml;
});

/**
 * Helper: cast the happy-dom Document to lib.dom Document. The structural compatibility is high enough that handler code runs cleanly against happy-dom's
 * Document, but the TS types diverge (happy-dom has its own Document class). The cast is unsafe-by-types and safe-by-runtime. It is specific to this suite because
 * we import the handlers as TypeScript and call them with a happy-dom Document directly; the sibling shared/channels/config runtime suites take a structurally
 * different approach (executing emitted script strings via runScripts) and so do not need this Document cast at all.
 */
function asDomDocument(ctx: DisposableDomTestContext): Document {

  return ctx.document as unknown as Document;
}

describe("status.handlers: createInitialState", () => {

  test("returns the canonical empty state with rAF gates open and watchdog timestamps zeroed", () => {

    /* The IIFE in status.ts does NOT call createInitialState; it hand-mirrors this shape as its own `const state` object literal (status.ts:55-63), while the tests
     * use createInitialState as the default for HandlerContext fixtures. The two are parallel definitions kept in sync by hand, so the field shape has to stay
     * stable: any new field added here must also be added to the IIFE's literal in status.ts, and vice versa. This assertion is exactly the drift guard - we assert
     * every field so a regression that drops or renames any of them surfaces here.
     */
    const state = handlers.createInitialState();

    assert.deepEqual(state, {

      expandedStreams: {},
      hiddenSince: 0,
      lastStatusEventTime: 0,
      popoverRenderPending: false,
      streamData: {},
      systemData: null,
      tableRenderPending: false
    });
  });

  test("two calls return independent objects so test fixtures and the IIFE seed do not share mutable state", () => {

    /* If createInitialState returned a shared singleton, mutating one fixture's state would corrupt the next test's. We assert independence by mutating the first
     * call's expandedStreams and asserting the second call's expandedStreams is still empty.
     */
    const a = handlers.createInitialState();
    const b = handlers.createInitialState();

    a.expandedStreams["s1"] = true;
    a.streamData["s1"] = makeStream({ id: "s1" });

    assert.deepEqual(b.expandedStreams, {}, "second call's expandedStreams must not see the first call's mutation");
    assert.deepEqual(b.streamData, {}, "second call's streamData must not see the first call's mutation");
  });
});

describe("status.handlers: HANDLER_CONSTANTS registry", () => {

  test("exposes the five script-side constants in declaration order", () => {

    /* HANDLER_CONSTANTS is the SSOT for the constants the emitted script needs. status.ts iterates it to emit `const NAME = <json>;` lines; tests iterate it to
     * assert the names and values. A regression that adds a new constant must register it here so the emitted script can reference it; a regression that drops
     * one must update the script bodies that referenced it. Either way, this assertion is the canary.
     */
    const names = handlers.HANDLER_CONSTANTS.map((c) => c.name);

    assert.deepEqual(names, [ "clientTypeLabels", "healthColorVars", "healthLabels", "resolutionLabels", "rowTints" ]);
  });

  test("healthColorVars carries CSS-variable tokens for every health tag the wire emits", () => {

    /* The health-state values (healthy/buffering/recovering/stalled/error) are the StreamSummary.health union. Each must map to a `var(--*)` token so the badge dot
     * picks up the theme color. We assert presence of every key plus the var() prefix so a refactor that swapped to literal hex codes (which would break theming)
     * surfaces here.
     */
    const entry = handlers.HANDLER_CONSTANTS.find((c) => c.name === "healthColorVars");

    assert.ok(entry, "healthColorVars constant must be registered");

    const value = entry.value as Record<string, string>;

    for(const key of [ "buffering", "error", "healthy", "recovering", "stalled" ]) {

      const token = value[key];

      assert.ok(typeof token === "string", "healthColorVars must carry a value for '" + key + "'");
      assert.match(token, /^var\(--/, "healthColorVars['" + key + "'] must use a CSS-variable token, not a literal color");
    }
  });

  test("healthLabels excludes 'recovering' so getRecoveringLabel can dispatch by escalation level", () => {

    /* The recovering state has level-dependent labels (Resuming playback / Syncing to live / Reloading player / Reloading page) that the static label map cannot
     * express. The lookup-and-fallback pattern in getHealthBadge depends on healthLabels NOT carrying a 'recovering' entry - if it did, the static label would
     * win and the level-specific labels would never surface.
     */
    const entry = handlers.HANDLER_CONSTANTS.find((c) => c.name === "healthLabels");
    const value = entry?.value as Record<string, string>;

    assert.ok(!Object.hasOwn(value, "recovering"),
      "healthLabels must NOT carry a 'recovering' key - getRecoveringLabel owns that dispatch");
  });

  test("rowTints maps every health tag to a row background tint, with healthy mapping to transparent", () => {

    /* The healthy row must have no tint so the table looks clean during normal operation; only problem states (buffering/recovering/stalled/error) get colored
     * backgrounds. The 'transparent' literal for healthy is the contract - a regression that mapped healthy to a var() token would visibly tint every healthy row.
     */
    const entry = handlers.HANDLER_CONSTANTS.find((c) => c.name === "rowTints");
    const value = entry?.value as Record<string, string>;

    assert.equal(value["healthy"], "transparent");

    for(const key of [ "buffering", "error", "recovering", "stalled" ]) {

      assert.match(value[key] ?? "", /^var\(--/, "rowTints['" + key + "'] must use a CSS-variable token");
    }
  });
});

describe("status.handlers: formatDuration (pure)", () => {

  test("under 60 seconds renders as 'Ns'", () => {

    /* The threshold ladder: <60s renders raw seconds with an 's' suffix. Edge cases asserted: 0 (corner), 1 (singular), 59 (just-under boundary).
     */
    assert.equal(handlers.formatDuration(0), "0s");
    assert.equal(handlers.formatDuration(1), "1s");
    assert.equal(handlers.formatDuration(59), "59s");
  });

  test("60s through 3599s renders as 'Nm Ns' with second-level precision", () => {

    /* The minute tier keeps seconds visible so operators can see sub-minute precision during recovery flows. We assert the boundary (60s -> '1m 0s') and a mid-range
     * value (125s -> '2m 5s'), plus the upper boundary (3599s -> '59m 59s' just before crossing into hours).
     */
    assert.equal(handlers.formatDuration(60), "1m 0s");
    assert.equal(handlers.formatDuration(125), "2m 5s");
    assert.equal(handlers.formatDuration(3599), "59m 59s");
  });

  test("3600s and above renders as 'Nh Nm' (seconds dropped, minutes truncated)", () => {

    /* Once a stream is hours old the seconds become noise; minutes is the right precision. We assert the boundary (3600s -> '1h 0m'), a multi-hour case (7325s ->
     * '2h 2m', proving the floor-on-minutes), and a long-running case (90000s -> '25h 0m', proving hours don't roll over to days).
     */
    assert.equal(handlers.formatDuration(3600), "1h 0m");
    assert.equal(handlers.formatDuration(7325), "2h 2m");
    assert.equal(handlers.formatDuration(90000), "25h 0m");
  });
});

describe("status.handlers: formatBytes (pure)", () => {

  test("0 bytes renders as the literal '0 B'", () => {

    /* The zero case has its own branch so empty buffers don't render as '0.0 KB'. Asserting '0 B' as the exact string ensures the special case stays in.
     */
    assert.equal(handlers.formatBytes(0), "0 B");
  });

  test("under 1024 bytes renders as 'N B' with no fractional part", () => {

    assert.equal(handlers.formatBytes(1), "1 B");
    assert.equal(handlers.formatBytes(1023), "1023 B");
  });

  test("KB tier (1024-1048575) renders one decimal place", () => {

    /* The KB threshold is 1024 (binary), not 1000. We assert the boundary, a halfway value (1536 -> 1.5 KB), and the upper boundary (1048575 -> 1024.0 KB just under
     * the MB threshold).
     */
    assert.equal(handlers.formatBytes(1024), "1.0 KB");
    assert.equal(handlers.formatBytes(1536), "1.5 KB");
    assert.equal(handlers.formatBytes(1048575), "1024.0 KB");
  });

  test("MB tier (1048576+) renders one decimal place", () => {

    assert.equal(handlers.formatBytes(1048576), "1.0 MB");
    assert.equal(handlers.formatBytes(2097152), "2.0 MB");
    assert.equal(handlers.formatBytes(10485760), "10.0 MB");
  });
});

describe("status.handlers: formatTime (pure)", () => {

  test("same-day timestamps render as '{H}:{MM} {AM|PM}' with a 12-hour clock", () => {

    /* Same-day rendering omits the date prefix. We construct ISO strings that, after Date parsing, will be on today's date - using new Date() and adjusting hours
     * keeps the test resilient to time-zone drift (the production code uses local time via getHours/getMinutes).
     */
    const now = new Date();
    const sameDay = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 6, 54, 0).toISOString();

    assert.equal(handlers.formatTime(sameDay), "6:54 AM");

    const sameDayPm = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 18, 5, 0).toISOString();

    assert.equal(handlers.formatTime(sameDayPm), "6:05 PM", "minute < 10 must be zero-padded");
  });

  test("midnight (hour=0) renders as 12 AM and noon (hour=12) renders as 12 PM", () => {

    /* The 12-hour wrap - hours = hours % 12, then fall back to 12 when the result is zero - is a bug-prone corner. We assert both midnight and noon so a regression
     * that drops the fallback (e.g., leaving only hours = hours % 12) would render midnight as 0:00 AM instead of 12:00 AM.
     */
    const now = new Date();
    const midnight = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0).toISOString();
    const noon = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 12, 0, 0).toISOString();

    assert.equal(handlers.formatTime(midnight), "12:00 AM");
    assert.equal(handlers.formatTime(noon), "12:00 PM");
  });

  test("cross-day timestamps prepend the month abbreviation and day", () => {

    /* When the formatted date falls on a different calendar day than 'now', the output gains a 'Mon D, ' prefix. We assert the prefix shape using a fixed historical
     * date (Jan 14, 2024 6:54 AM local time) - the abbreviation 'Jan' and the literal day '14' must appear before the time.
     */
    const distantPast = new Date(2024, 0, 14, 6, 54, 0).toISOString();
    const out = handlers.formatTime(distantPast);

    assert.match(out, /^Jan 14, 6:54 AM$/, "cross-day output must carry 'Jan 14, ' as the date prefix");
  });
});

describe("status.handlers: formatTimeAgo (pure)", () => {

  test("less than 60 seconds renders as 'just now'", () => {

    assert.equal(handlers.formatTimeAgo(Date.now()), "just now");
    assert.equal(handlers.formatTimeAgo(Date.now() - 30000), "just now");
  });

  test("minutes tier renders 'N minute(s) ago' with singular/plural agreement", () => {

    /* The singular vs plural branch is the bug-prone corner. We assert both: 1 minute (singular) and 5 minutes (plural).
     */
    assert.equal(handlers.formatTimeAgo(Date.now() - 60000), "1 minute ago");
    assert.equal(handlers.formatTimeAgo(Date.now() - 300000), "5 minutes ago");
  });

  test("hours tier renders 'N hour(s) ago'", () => {

    assert.equal(handlers.formatTimeAgo(Date.now() - 3600000), "1 hour ago");
    assert.equal(handlers.formatTimeAgo(Date.now() - 7200000), "2 hours ago");
  });

  test("days tier renders 'N day(s) ago'", () => {

    assert.equal(handlers.formatTimeAgo(Date.now() - 86400000), "1 day ago");
    assert.equal(handlers.formatTimeAgo(Date.now() - 172800000), "2 days ago");
  });
});

describe("status.handlers: getDomain (pure)", () => {

  test("returns the last two hostname parts for multi-segment hostnames", () => {

    /* The display label collapses watch.example.com to example.com so the popover stays scannable. We assert the multi-segment, two-segment, and 5+-segment cases
     * so a regression that switched the slice direction (which would render 'watch.example' for watch.example.com) surfaces here.
     */
    assert.equal(handlers.getDomain("https://watch.example.com/path"), "example.com");
    assert.equal(handlers.getDomain("https://example.com/path"), "example.com", "two-segment hostname returns as-is");
    assert.equal(handlers.getDomain("https://a.b.c.d.e.com/path"), "e.com", "5+ segments take the last two");
  });

  test("returns the original URL string when URL.parse cannot parse it", () => {

    /* The early-return guard for unparseable URLs returns the input verbatim. Asserting this branch ensures we don't crash on malformed strings the server might
     * emit during identification races.
     */
    assert.equal(handlers.getDomain("not a url at all"), "not a url at all");
  });

  test("handles localhost as a single-segment hostname (no slice past start)", () => {

    /* Localhost has only one hostname segment. The (parts.length > 2) ? slice(-2) : parts.join('.') branch routes localhost through the join path, which renders
     * the literal 'localhost'. Asserting this case ensures local-development URLs don't render as 'localhost.localhost' or similar nonsense.
     */
    assert.equal(handlers.getDomain("http://localhost:5589/stream"), "localhost");
  });

  test("returns an IPv4 host whole, matching the server-side extractDomain", () => {

    /* Detector for the dotted-quad check: without it the last-two-parts rule renders "10.0.1.50" as "1.50" in the popover while the server's own extractDomain
     * renders the full address, and the two sides of the mirror would disagree on what "the domain" means for a stream tuned by address. The bracketed IPv6 row
     * is parity - URL.hostname yields it with no dots, so the part-count branch already returned it whole.
     */
    assert.equal(handlers.getDomain("http://10.0.1.50:5589/hls/nbc/stream.m3u8"), "10.0.1.50");
    assert.equal(handlers.getDomain("http://192.168.1.1/"), "192.168.1.1", "a four-part private address is not shortened either");
    assert.equal(handlers.getDomain("http://[2001:db8::1]:5589/stream"), "[2001:db8::1]", "a bracketed IPv6 literal stays whole");
  });
});

describe("status.handlers: getRecoveringLabel (pure)", () => {

  test("dispatches level 1-3 to the named labels and level 4+ to 'Reloading page'", () => {

    /* The escalation ladder defined in monitor.ts: level 1 (play/unmute) -> 'Resuming playback', level 2 (source reload) -> 'Syncing to live', level 3 (page
     * navigation) -> 'Reloading player'. Level 4+ -> 'Reloading page' is defensive padding since escalationLevel maxes at 3 in production. Asserting the full ladder
     * catches a regression in any single branch.
     */
    assert.equal(handlers.getRecoveringLabel(1), "Resuming playback");
    assert.equal(handlers.getRecoveringLabel(2), "Syncing to live");
    assert.equal(handlers.getRecoveringLabel(3), "Reloading player");
    assert.equal(handlers.getRecoveringLabel(4), "Reloading page");
    assert.equal(handlers.getRecoveringLabel(99), "Reloading page", "anything >=4 renders as 'Reloading page'");
  });

  test("falls through to 'Recovering' when level is 0 or negative", () => {

    /* The default branch's >= 4 ternary returns the bare 'Recovering' label for level 0/negative. This is the safe-default case for streams that entered the
     * recovering state before an escalation level was assigned.
     */
    assert.equal(handlers.getRecoveringLabel(0), "Recovering");
    assert.equal(handlers.getRecoveringLabel(-1), "Recovering");
  });
});

describe("status.handlers: getHealthBadge (pure)", () => {

  test("renders a status-dot span with the matching CSS-variable color and the label", () => {

    /* The badge HTML is two spans: the dot (color-coded) and the label (text). We assert both pieces for the healthy case so a regression that swapped the dot
     * character (&#9679; is a black circle), the color binding, or the label dispatch surfaces here.
     */
    const html = handlers.getHealthBadge("healthy", 0);

    assert.match(html, /style="color: var\(--stream-healthy\);"/, "healthy state must use the stream-healthy CSS variable");
    assert.match(html, /Healthy/, "label must render");
    assert.match(html, /&#9679;/, "dot character must be the black-circle entity");
  });

  test("recovering state delegates to getRecoveringLabel for the level-specific label", () => {

    const html = handlers.getHealthBadge("recovering", 2);

    assert.match(html, /Syncing to live/, "level 2 must surface as 'Syncing to live'");
    assert.match(html, /var\(--stream-recovering\)/, "recovering must use the stream-recovering CSS variable");
  });

  test("unknown health state falls back to the supplied string and the muted color token", () => {

    /* The (healthLabels[health] ?? health) and (healthColorVars[health] ?? var(--text-muted)) fallbacks let the badge render gracefully if the wire emits a health
     * value the script does not recognize. We assert both fallbacks against a synthetic 'mystery' state.
     */
    const html = handlers.getHealthBadge("mystery", 0);

    assert.match(html, /var\(--text-muted\)/, "unknown state must fall back to the muted color token");
    assert.match(html, /mystery/, "unknown state's label must echo the input string");
  });
});

describe("status.handlers: formatLastIssue (pure)", () => {

  test("'None' when the stream has not yet hit a health event", () => {

    assert.equal(handlers.formatLastIssue(makeStream()), "None");
  });

  test("recovered suffix when health is healthy and an issue type/time is present", () => {

    /* The status suffix is '(recovered)' when health is currently healthy (the issue resolved) and '(recovering)' otherwise. We assert both branches.
     */
    const stream = makeStream({

      health: "healthy",
      lastIssueTime: new Date(2024, 0, 14, 6, 54, 0).toISOString(),
      lastIssueType: "stall"
    });

    const out = handlers.formatLastIssue(stream);

    assert.match(out, /^Stall at /, "label is sentence-cased issue type plus 'at'");
    assert.match(out, / \(recovered\)$/, "healthy state appends '(recovered)'");
  });

  test("recovering suffix when health is anything other than healthy", () => {

    const stream = makeStream({

      health: "stalled",
      lastIssueTime: new Date(2024, 0, 14, 6, 54, 0).toISOString(),
      lastIssueType: "buffering"
    });

    assert.match(handlers.formatLastIssue(stream), / \(recovering\)$/);
  });
});

describe("status.handlers: formatAutoRecovery (pure)", () => {

  test("'N/A' when no recovery attempts have happened", () => {

    assert.equal(handlers.formatAutoRecovery(makeStream({ recoveryAttempts: 0 })), "N/A");
  });

  test("singular 'attempt' for 1; plural 'attempts' for 2+", () => {

    assert.equal(handlers.formatAutoRecovery(makeStream({ recoveryAttempts: 1 })), "1 attempt");
    assert.equal(handlers.formatAutoRecovery(makeStream({ recoveryAttempts: 5 })), "5 attempts");
  });

  test("appends page-reload tail when pageReloadsInWindow > 0, with singular/plural agreement", () => {

    assert.equal(handlers.formatAutoRecovery(makeStream({ pageReloadsInWindow: 1, recoveryAttempts: 3 })), "3 attempts, 1 page reload");
    assert.equal(handlers.formatAutoRecovery(makeStream({ pageReloadsInWindow: 4, recoveryAttempts: 5 })), "5 attempts, 4 page reloads");
  });

  test("omits page-reload tail when pageReloadsInWindow is 0", () => {

    /* The reloads > 0 guard is the contract: zero reloads must not surface a ', 0 page reloads' suffix that would clutter the detail panel.
     */
    assert.equal(handlers.formatAutoRecovery(makeStream({ pageReloadsInWindow: 0, recoveryAttempts: 2 })), "2 attempts");
  });
});

describe("status.handlers: formatClients (pure)", () => {

  test("'None' when clientCount is 0", () => {

    assert.equal(handlers.formatClients(makeStream()), "None");
  });

  test("single client renders as '{count} {Type}' with the type label resolved via clientTypeLabels", () => {

    /* The HLS / MPEG-TS labels come from the clientTypeLabels constant. We assert both labels here - a regression that dropped a key would surface as the raw type
     * string ('hls' lowercase) instead of the canonical label ('HLS' all-caps).
     */
    assert.equal(handlers.formatClients(makeStream({ clientCount: 1, clients: [{ count: 1, type: "hls" }] })), "1 HLS");
    assert.equal(handlers.formatClients(makeStream({ clientCount: 1, clients: [{ count: 1, type: "mpegts" }] })), "1 MPEG-TS");
  });

  test("multiple client types join with comma+space", () => {

    const stream = makeStream({

      clientCount: 3,
      clients: [
        { count: 2, type: "hls" },
        { count: 1, type: "mpegts" }
      ]
    });

    assert.equal(handlers.formatClients(stream), "2 HLS, 1 MPEG-TS");
  });

  test("unknown client type falls back to the raw string", () => {

    /* Defensive fallback: an unrecognized type renders verbatim instead of dropping out of the comma list. Assert a synthetic 'rtsp' entry to verify.
     */
    const stream = makeStream({

      clientCount: 1,
      clients: [{ count: 1, type: "rtsp" }]
    });

    assert.equal(handlers.formatClients(stream), "1 rtsp");
  });
});

describe("status.handlers: renderHealthCellContent (pure)", () => {

  test("no client indicator when clientCount is 0; just the badge", () => {

    /* The client-count chip is gated on clientCount > 0. Asserting the negative case ensures an empty stream doesn't render a stray '&#9673; 0' marker.
     */
    const html = handlers.renderHealthCellContent(makeStream({ clientCount: 0 }));

    assert.ok(!html.includes("client-count"), "no client-count chip when clientCount is 0");
    assert.match(html, /Healthy/);
  });

  test("singular 'client' tooltip on the chip when clientCount is 1", () => {

    /* The title attribute carries the count + 'client'/'clients'. Asserting the singular case covers the agreement branch.
     */
    const html = handlers.renderHealthCellContent(makeStream({ clientCount: 1 }));

    assert.match(html, /title="1 client"/);
    assert.match(html, /class="client-count"/);
  });

  test("plural 'clients' tooltip when clientCount is greater than 1", () => {

    const html = handlers.renderHealthCellContent(makeStream({ clientCount: 4 }));

    assert.match(html, /title="4 clients"/);
  });

  test("recovering streams pick up the level-specific label via getHealthBadge", () => {

    /* The renderer composes the chip and the badge. We assert the composition - a recovering stream at level 3 must surface 'Reloading player' inside the cell.
     */
    const html = handlers.renderHealthCellContent(makeStream({ escalationLevel: 3, health: "recovering" }));

    assert.match(html, /Reloading player/);
  });
});

describe("status.handlers: renderDetailCodec (pure)", () => {

  test("capture mode without hardware acceleration renders 'Codec: {captureCodec} (Capture)'", () => {

    const html = handlers.renderDetailCodec(makeStream({ captureCodec: "h264" }));

    assert.match(html, /<strong>Codec:<\/strong> h264 \(Capture\)$/);
  });

  test("hardware-accelerated capture prefixes the lightning bolt", () => {

    /* The hardware-accelerated branch prepends the literal lightning-bolt character (⚡, U+26A1); non-accelerated does not - we assert its presence.
     */
    const html = handlers.renderDetailCodec(makeStream({ captureCodec: "h264", hardwareAccelerated: true }));

    assert.match(html, /⚡ h264/);
  });

  test("native HLS without captureCodec renders 'Native HLS (Native HLS)'", () => {

    /* When the streaming mode is native and no capture codec is reported, both labels collapse to 'Native HLS'. The mode label still appears in parentheses,
     * which is intentional for parallelism with the capture-mode rendering.
     */
    const html = handlers.renderDetailCodec(makeStream({ streamingMode: "native" }));

    assert.match(html, /Native HLS \(Native HLS\)/);
  });

  test("native HLS with bandwidth + resolution appends the quality suffix", () => {

    /* The suffix shape: ' - {Mbps}Mbps {resolution-label}'. The resolution label comes from the shared label map keyed by the height (the second component of
     * a 'WIDTHxHEIGHT' string). We assert the full happy-path: 5Mbps + 1920x1080 -> ' - 5.0Mbps 1080p'.
     */
    const html = handlers.renderDetailCodec(makeStream({

      nativeBandwidth: NATIVE_BANDWIDTH,
      nativeResolution: "1920x1080",
      streamingMode: "native"
    }));

    assert.match(html, / - 5\.0Mbps 1080p$/);
  });

  test("native HLS with unrecognized resolution height falls back to the raw resolution string", () => {

    /* The label-or-raw-string fallback ensures non-standard resolutions still render. A 1234x999 stream (height 999 is not in the label map) must surface the raw
     * '1234x999' instead of dropping the resolution.
     */
    const html = handlers.renderDetailCodec(makeStream({

      nativeBandwidth: NATIVE_BANDWIDTH,
      nativeResolution: "1234x999",
      streamingMode: "native"
    }));

    assert.match(html, / - 5\.0Mbps 1234x999$/);
  });

  test("capture mode with both resolutions appends the source-and-capture suffix", () => {

    /* The operator-facing point of the pair: a 720p source captured at 1080p is visible as such rather than reading as a 1080p stream. The suffix names the source
     * first because that is the fact the operator cannot otherwise see; the capture half is the configured surface.
     */
    const html = handlers.renderDetailCodec(makeStream({

      captureCodec: "hevc",
      captureResolution: "1920x1080",
      sourceResolution: "1280x720"
    }));

    assert.match(html, / - 720p source, 1080p capture$/);
  });

  test("capture mode with only a source resolution appends the source half alone", () => {

    // End-anchored on purpose: with no capture size to name, the suffix has to end after "source" rather than trailing a comma into nothing.
    const html = handlers.renderDetailCodec(makeStream({ captureCodec: "hevc", sourceResolution: "1280x720" }));

    assert.match(html, / - 720p source$/);
  });

  test("capture mode entity-encodes an unlabeled source resolution", () => {

    // The capture branch shares the native branch's fallback, so a resolution string with no standard label reaches the page escaped rather than raw.
    const html = handlers.renderDetailCodec(makeStream({ captureCodec: "hevc", sourceResolution: "<script>evil</script>" }));

    assert.match(html, /&lt;script&gt;evil&lt;\/script&gt; source/, "the unlabeled source falls back to the entity-encoded raw value");
    assert.doesNotMatch(html, /<script>/, "no raw tag may survive in the codec line");
  });

  test("native mode reports its variant quality and never the source pair", () => {

    /* A native stream's nativeResolution is both its source and its output, so the pair would be a duplicate reading of the same number. The negative assertion is
     * what proves the mode gate: a stream carrying a sourceResolution still renders the native suffix alone.
     */
    const html = handlers.renderDetailCodec(makeStream({

      nativeBandwidth: NATIVE_BANDWIDTH,
      nativeResolution: "1920x1080",
      sourceResolution: "1280x720",
      streamingMode: "native"
    }));

    assert.match(html, / - 5\.0Mbps 1080p$/);
    assert.doesNotMatch(html, /source/, "the capture suffix never appears on a native stream");
  });

  test("capture mode with no captureCodec renders 'Unknown'", () => {

    /* The two-step ternary: captureCodec ? ... : (streamingMode === 'native' ? 'Native HLS' : 'Unknown'). When no codec is reported and the mode is anything
     * other than native, we fall back to the literal 'Unknown'. Asserting this corner case prevents a regression that emits an empty string or 'undefined'.
     */
    const html = handlers.renderDetailCodec(makeStream());

    assert.match(html, /<strong>Codec:<\/strong> Unknown \(Capture\)$/);
  });
});

describe("status.handlers: renderDetailStarted (pure)", () => {

  test("renders 'Started: {time}' when no clients are connected", () => {

    /* The clientSuffix is gated on clientCount > 0; zero clients produce no suffix. We assert the unsuffixed shape against a 6:54 AM same-day time so the formatTime
     * collaboration is visible in the assertion target.
     */
    const stream = makeStream({

      startTime: new Date(new Date().getFullYear(), new Date().getMonth(), new Date().getDate(), 6, 54, 0).toISOString()
    });

    const html = handlers.renderDetailStarted(stream);

    assert.equal(html, "<strong>Started:</strong> 6:54 AM");
  });

  test("renders 'Started: {time} · {clients}' when clients are connected", () => {

    /* The middot separator (·) is part of the contract. The clients are formatted via formatClients, which we've already asserted. We assert the composition.
     */
    const stream = makeStream({

      clientCount: 1,
      clients: [{ count: 1, type: "hls" }],
      startTime: new Date(new Date().getFullYear(), new Date().getMonth(), new Date().getDate(), 6, 54, 0).toISOString()
    });

    assert.match(handlers.renderDetailStarted(stream), /<strong>Started:<\/strong> 6:54 AM &middot; 1 HLS$/);
  });
});

describe("status.handlers: updateSystemStatus (DOM mutator)", () => {

  test("no-op when systemData is null (the pre-snapshot state)", () => {

    /* The handler short-circuits when systemData is null. We assert by snapshotting #stream-count's textContent before and after - a regression that wrote
     * default-zero values would change the snapshot.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));
      const before = ctx.document.getElementById("stream-count")?.textContent;

      handlers.updateSystemStatus(harness.ctx);

      assert.equal(ctx.document.getElementById("stream-count")?.textContent, before, "no mutation when systemData is null");
    })();
  });

  test("renders connected-state badge with the healthy color when browser.connected is true and active is 0", () => {

    /* Connected with zero streams: green dot in the health span, '0 streams' textContent on the count button, no 'clickable' class. We assert all three so a
     * regression in any one surfaces here.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), { state: { systemData: { browser: { captureImpaired: false, connected: true },
        streams: { active: 0, limit: 5 } } } });

      handlers.updateSystemStatus(harness.ctx);

      const healthEl = ctx.document.getElementById("system-health");
      const streamEl = ctx.document.getElementById("stream-count");

      assert.match(healthEl?.innerHTML ?? "", /var\(--stream-healthy\)/, "connected state must use the healthy color");
      assert.ok(streamEl, "stream-count element must exist on the rendered page");
      assert.equal(streamEl.textContent, "0 streams");
      assert.equal(streamEl.classList.contains("clickable"), false, "zero-stream state must NOT carry the clickable class");
    })();
  });

  test("renders 'Browser offline' when browser.connected is false", () => {

    /* The offline branch swaps the color and appends a label. We assert the literal 'Browser offline' so a regression that renamed the label surfaces here.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), { state: { systemData: { browser: { captureImpaired: false, connected: false },
        streams: { active: 0, limit: 5 } } } });

      handlers.updateSystemStatus(harness.ctx);

      const healthEl = ctx.document.getElementById("system-health");

      assert.match(healthEl?.innerHTML ?? "", /Browser offline/);
      assert.match(healthEl?.innerHTML ?? "", /var\(--stream-error\)/, "offline state must use the error color");
    })();
  });

  test("renders the relaunch-pending state when the browser can no longer start captures", () => {

    /* The third header state. The color and the label are both asserted, and so is the tooltip, because the tooltip is the only place the interface explains why a
     * tune is being refused - a state rendered without it would leave a user watching a failing tune with nothing to read. The offline label is asserted absent so
     * a regression that fell through to the disconnected branch surfaces here.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), { state: { systemData: { browser: { captureImpaired: true, connected: true },
        streams: { active: 1, limit: 5 } } } });

      handlers.updateSystemStatus(harness.ctx);

      const healthEl = ctx.document.getElementById("system-health");

      assert.match(healthEl?.innerHTML ?? "", /var\(--stream-recovering\)/, "the relaunch-pending state must use the recovering color");
      assert.match(healthEl?.innerHTML ?? "", /Browser relaunch pending/);
      assert.match(healthEl?.innerHTML ?? "", /title="[^"]*can no longer start captures/, "the label carries a tooltip explaining the refusal");
      assert.doesNotMatch(healthEl?.innerHTML ?? "", /Browser offline/, "a marked but connected browser is not offline");
    })();
  });

  test("renders 'Browser offline' rather than relaunch-pending when a marked browser is also disconnected", () => {

    // Precedence at the interface, matching the health endpoint's: a disconnected browser is serving nothing, so it outranks a mark that says it can still serve
    // what it started. A branch order that tested the mark first would render the wrong state here.
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), { state: { systemData: { browser: { captureImpaired: true, connected: false },
        streams: { active: 0, limit: 5 } } } });

      handlers.updateSystemStatus(harness.ctx);

      const healthEl = ctx.document.getElementById("system-health");

      assert.match(healthEl?.innerHTML ?? "", /Browser offline/);
      assert.match(healthEl?.innerHTML ?? "", /var\(--stream-error\)/, "offline state must use the error color");
      assert.doesNotMatch(healthEl?.innerHTML ?? "", /Browser relaunch pending/, "disconnected outranks the mark");
    })();
  });

  test("renders '{active}/{limit} streams' and adds clickable class when active > 0", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), { state: { systemData: { browser: { captureImpaired: false, connected: true },
        streams: { active: 3, limit: 8 } } } });

      handlers.updateSystemStatus(harness.ctx);

      const streamEl = ctx.document.getElementById("stream-count");

      assert.ok(streamEl, "stream-count element must exist on the rendered page");
      assert.equal(streamEl.textContent, "3/8 streams");
      assert.equal(streamEl.classList.contains("clickable"), true, "non-zero stream state must add the clickable class");
    })();
  });

  test("closes an open stream popover when transitioning to zero streams", () => {

    /* The active = 0 branch removes 'show' from the popover menu so a hover-opened popover doesn't linger after the last stream ends. We open the menu first
     * (.classList.add('show')) so the assertion has a positive starting state.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();

      ctx.document.getElementById("stream-popover-menu")?.classList.add("show");

      const harness = makeHandlerContext(asDomDocument(ctx), { state: { systemData: { browser: { captureImpaired: false, connected: true },
        streams: { active: 0, limit: 5 } } } });

      handlers.updateSystemStatus(harness.ctx);

      assert.equal(ctx.document.getElementById("stream-popover-menu")?.classList.contains("show"), false,
        "popover menu must be closed when streams transition to zero");
    })();
  });
});

describe("status.handlers: buildStreamPopoverContent (DOM mutator)", () => {

  test("renders one popover row per stream with the channel name, duration, and color-coded dot", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const startTime = new Date(Date.now() - 65000).toISOString();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: {

          streamData: {

            "1": makeStream({ channel: "NBC", id: "1", startTime }),
            "2": makeStream({ health: "stalled", id: "2", serviceName: "Hulu Live", startTime })
          }
        }
      });

      const menu = ctx.document.getElementById("stream-popover-menu") as unknown as Element;

      handlers.buildStreamPopoverContent(menu, harness.ctx);

      const rows = menu.querySelectorAll(".stream-popover-row");

      assert.equal(rows.length, 2, "one popover row per stream");
      assert.match(menu.innerHTML, /NBC/);
      assert.match(menu.innerHTML, /Hulu Live/);
      assert.match(menu.innerHTML, /var\(--stream-stalled\)/, "second stream's stalled state must color its dot");
      assert.match(menu.innerHTML, /1m 5s/, "duration must be computed from now-startTime and rendered via formatDuration");
    })();
  });

  test("falls back to getDomain when both channel and serviceName are empty strings (not just nullish)", () => {

    /* The fallback chain uses || semantics, not ?? - this matters because the server may emit empty strings during identification races. The chain: channel ||
     * serviceName || handlers.getDomain(url). We assert the all-empty case to confirm the URL-derived label surfaces.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: {

          streamData: {

            "1": makeStream({ channel: "", id: "1", serviceName: "", url: "https://watch.example.com/path" })
          }
        }
      });

      const menu = ctx.document.getElementById("stream-popover-menu") as unknown as Element;

      handlers.buildStreamPopoverContent(menu, harness.ctx);

      assert.equal(harness.recorder.channelDisplayCalls.length, 1);
      assert.equal(harness.recorder.channelDisplayCalls[0]?.name, "example.com",
        "empty channel and serviceName must fall through to handlers.getDomain(url) -> 'example.com'");
    })();
  });

  test("hardware-accelerated streams render the lightning bolt badge", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "1": makeStream({ channel: "NBC", hardwareAccelerated: true, id: "1" }) } }
      });

      const menu = ctx.document.getElementById("stream-popover-menu") as unknown as Element;

      handlers.buildStreamPopoverContent(menu, harness.ctx);

      assert.match(menu.innerHTML, /Hardware accelerated/);
    })();
  });

  test("appends the showName as a separate span when present", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "1": makeStream({ channel: "NBC", id: "1", showName: "Today" }) } }
      });

      const menu = ctx.document.getElementById("stream-popover-menu") as unknown as Element;

      handlers.buildStreamPopoverContent(menu, harness.ctx);

      const showSpan = menu.querySelector(".stream-popover-show");

      assert.ok(showSpan, "showName must surface as a .stream-popover-show span");
      assert.equal(showSpan.textContent, "Today");
    })();
  });

  test("HTML-escapes the showName so injected markup cannot break out of the popover text context", () => {

    /* The showName is external data (Channels DVR show lookups) and is concatenated directly into the popover innerHTML. An unescaped value carrying < or > would
     * break out of the text context and inject a live element. We seed a show name with a <b> tag plus an ampersand and apostrophe and assert the raw innerHTML
     * carries the angle brackets and ampersand as entities, with no live tag surviving. We inspect innerHTML (raw markup) rather than textContent, which would
     * decode the entities and mask a missing escape. Note: the DOM serializer round-trips a quote/apostrophe in text content back to the literal character (both
     * are inert there), so this DOM-level test asserts the breakout-prevention guarantee; the byte-exact &quot;/&#39; entity contract is asserted in the clientEscape
     * parity suite (clientEscape.test.ts).
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "1": makeStream({ channel: "NBC", id: "1", showName: "Tom & Jerry's <b>Live</b>" }) } }
      });

      const menu = ctx.document.getElementById("stream-popover-menu") as unknown as Element;

      handlers.buildStreamPopoverContent(menu, harness.ctx);

      const showSpan = menu.querySelector(".stream-popover-show");

      assert.ok(showSpan, "showName must surface as a .stream-popover-show span");
      assert.match(showSpan.innerHTML, /Tom &amp; Jerry's &lt;b&gt;Live&lt;\/b&gt;/,
        "angle brackets and the ampersand in the show name must be entity-encoded in the rendered markup");
      assert.equal(showSpan.querySelector("b"), null, "no live <b> element may be parsed out of the escaped show name");
    })();
  });
});

describe("status.handlers: updateStreamPopover (DOM mutator)", () => {

  test("no-op when the popover is closed (the menu does not carry .show)", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const menu = ctx.document.getElementById("stream-popover-menu");

      menu?.classList.remove("show");

      const harness = makeHandlerContext(asDomDocument(ctx), { state: { streamData: { "1": makeStream({ id: "1" }) } } });

      const before = menu?.innerHTML ?? "";

      handlers.updateStreamPopover(harness.ctx);

      assert.equal(menu?.innerHTML, before, "closed popover must not be re-rendered");
    })();
  });

  test("auto-closes the popover when streamData is empty (no streams left)", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const menu = ctx.document.getElementById("stream-popover-menu");

      menu?.classList.add("show");

      const harness = makeHandlerContext(asDomDocument(ctx), { state: { streamData: {} } });

      handlers.updateStreamPopover(harness.ctx);

      assert.equal(menu?.classList.contains("show"), false, "popover must auto-close when no streams remain");
    })();
  });

  test("rebuilds the popover content when open and streams are present", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const menu = ctx.document.getElementById("stream-popover-menu");

      menu?.classList.add("show");

      const harness = makeHandlerContext(asDomDocument(ctx), { state: { streamData: { "1": makeStream({ channel: "NBC", id: "1" }) } } });

      handlers.updateStreamPopover(harness.ctx);

      assert.match(menu?.innerHTML ?? "", /NBC/);
      assert.equal(menu?.classList.contains("show"), true, "popover stays open when content is rebuilt");
    })();
  });
});

describe("status.handlers: renderStreamsTable (DOM mutator)", () => {

  test("renders the empty-row placeholder when streamData is empty", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));

      handlers.renderStreamsTable(harness.ctx);

      const tbody = ctx.document.getElementById("streams-tbody");

      assert.match(tbody?.innerHTML ?? "", /class="empty-row"/);
      assert.match(tbody?.innerHTML ?? "", /No active streams/);
    })();
  });

  test("renders one .stream-row per stream with a data-id attribute matching the stream id", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: {

          streamData: {

            "alpha": makeStream({ channel: "NBC", id: "alpha" }),
            "beta": makeStream({ channel: "ABC", id: "beta" })
          }
        }
      });

      handlers.renderStreamsTable(harness.ctx);

      const rows = ctx.document.querySelectorAll(".stream-row");

      assert.equal(rows.length, 2);

      const ids = Array.from(rows).map((r) => r.getAttribute("data-id")).sort();

      assert.deepEqual(ids, [ "alpha", "beta" ]);
    })();
  });

  test("preserves Object.entries insertion order so streams appear in the order they were added", () => {

    /* JavaScript's Object preserves insertion order for string keys. The renderer iterates Object.entries, which means the order of streamData mutation is the
     * order of the rendered rows. We seed the streamData with explicit insertion order and assert that the rendered order matches.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const streamData: Record<string, StreamSummary> = {};

      streamData["zulu"] = makeStream({ channel: "Z", id: "zulu" });
      streamData["alpha"] = makeStream({ channel: "A", id: "alpha" });
      streamData["mike"] = makeStream({ channel: "M", id: "mike" });

      const harness = makeHandlerContext(asDomDocument(ctx), { state: { streamData } });

      handlers.renderStreamsTable(harness.ctx);

      const rows = ctx.document.querySelectorAll(".stream-row");
      const ids = Array.from(rows).map((r) => r.getAttribute("data-id"));

      assert.deepEqual(ids, [ "zulu", "alpha", "mike" ], "rendering must preserve insertion order, not sort alphabetically");
    })();
  });

  test("native streams render the 'Native' badge", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "n": makeStream({ id: "n", streamingMode: "native" }) } }
      });

      handlers.renderStreamsTable(harness.ctx);

      const tbody = ctx.document.getElementById("streams-tbody");

      assert.match(tbody?.innerHTML ?? "", /class="native-badge" title="Native HLS"/);
    })();
  });

  test("expanded streams render a .stream-details row with the URL, codec, last issue, recovery, and memory metrics", () => {

    /* The expanded detail panel is the secondary row that shows when expandedStreams[id] is true. We seed expandedStreams + a stream with rich metadata and
     * confirm every metric label is rendered.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: {

          expandedStreams: { "x": true },
          streamData: {

            "x": makeStream({

              captureCodec: "h264",
              clientCount: 2,
              clients: [{ count: 2, type: "hls" }],
              id: "x",
              lastIssueTime: new Date(2024, 0, 14, 6, 54, 0).toISOString(),
              lastIssueType: "stall",
              memoryBytes: 1572864,
              pageReloadsInWindow: 1,
              recoveryAttempts: 3,
              url: "https://watch.example.test/play"
            })
          }
        }
      });

      handlers.renderStreamsTable(harness.ctx);

      const detailRow = ctx.document.querySelector(".stream-details[data-id=\"x\"]");

      assert.ok(detailRow, "expanded streams must render a sibling .stream-details row");
      assert.match(detailRow.innerHTML, /watch\.example\.test\/play/);
      assert.match(detailRow.innerHTML, /h264 \(Capture\)/);
      assert.match(detailRow.innerHTML, /Stall at /);
      assert.match(detailRow.innerHTML, /3 attempts, 1 page reload/);
      assert.match(detailRow.innerHTML, /1\.5 MB/);
    })();
  });

  test("collapsed streams render only the primary row, no details sibling", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "y": makeStream({ id: "y" }) } }
      });

      handlers.renderStreamsTable(harness.ctx);

      assert.equal(ctx.document.querySelector(".stream-details[data-id=\"y\"]"), null,
        "collapsed streams must not render a stream-details row");
    })();
  });

  test("row tint binds to the stream's health state", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: {

          streamData: {

            "h": makeStream({ health: "healthy", id: "h" }),
            "s": makeStream({ health: "stalled", id: "s" })
          }
        }
      });

      handlers.renderStreamsTable(harness.ctx);

      const healthyRow = ctx.document.querySelector(".stream-row[data-id=\"h\"]") as unknown as { getAttribute(name: string): string | null } | null;
      const stalledRow = ctx.document.querySelector(".stream-row[data-id=\"s\"]") as unknown as { getAttribute(name: string): string | null } | null;

      assert.match(healthyRow?.getAttribute("style") ?? "", /background-color: transparent/);
      assert.match(stalledRow?.getAttribute("style") ?? "", /background-color: var\(--stream-tint-stalled\)/);
    })();
  });

  test("HTML-escapes the showName in the stream-show cell so injected markup cannot break out of the cell", () => {

    /* The show cell is built by concatenating the show name directly into the table innerHTML. External show-name data carrying < or > must be entity-encoded so
     * it cannot break out of the cell and inject a live element. We inspect the cell's innerHTML (raw markup) rather than textContent, which would decode the
     * entities and hide a missing escape. The strict &quot;/&#39; entity contract is asserted in the clientEscape parity suite; here we assert the breakout-prevention
     * guarantee that survives the DOM serializer's round-trip.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "x": makeStream({ id: "x", showName: "A & B <i>X</i>" }) } }
      });

      handlers.renderStreamsTable(harness.ctx);

      const showCell = ctx.document.querySelector(".stream-row[data-id=\"x\"] .stream-show");

      assert.ok(showCell, "the stream-show cell must render");
      assert.match(showCell.innerHTML, /A &amp; B &lt;i&gt;X&lt;\/i&gt;/,
        "angle brackets and the ampersand in the show name must be entity-encoded in the cell markup");
      assert.equal(showCell.querySelector("i"), null, "no live <i> element may be parsed out of the escaped show name");
    })();
  });

  test("HTML-escapes the stream URL in the expanded detail panel so query-string ampersands and injected markup are inert", () => {

    /* The expanded detail panel concatenates the stream URL directly into the .details-url innerHTML. Stream URLs routinely carry & in their query strings, and a
     * user-supplied channel URL could carry markup characters. The ampersand and any angle brackets must be entity-encoded. We seed a URL with an ampersand-laden
     * query string plus an injected angle-bracket payload and assert the raw innerHTML carries those as entities with no live element parsed out. The strict
     * &quot;/&#39; entity contract is asserted in the clientEscape parity suite; here we assert the breakout-prevention guarantee that survives the DOM serializer.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: {

          expandedStreams: { "x": true },
          streamData: { "x": makeStream({ id: "x", url: "https://watch.example.test/play?a=1&b=2><img src=x>" }) }
        }
      });

      handlers.renderStreamsTable(harness.ctx);

      const urlEl = ctx.document.querySelector(".stream-details[data-id=\"x\"] .details-url");

      assert.ok(urlEl, "the expanded detail panel must render a .details-url element");
      assert.match(urlEl.innerHTML, /a=1&amp;b=2&gt;&lt;img src=x&gt;/,
        "the URL's ampersand and angle brackets must be entity-encoded in the rendered markup");
      assert.equal(urlEl.querySelector("img"), null, "no live <img> element may be parsed out of the escaped URL");
    })();
  });
});

describe("status.handlers: updateStreamRow (DOM mutator)", () => {

  test("falls back to scheduleTableRender when the row does not exist yet (race between streamAdded and streamHealthChanged)", () => {

    /* The fall-through scenario: an event arrives for a stream the table hasn't rendered yet. The handler queues a full-table render via the rAF scheduler. With
     * synchronous-fire rAF installed at module scope, the table render fires inline.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const stream = makeStream({ channel: "NBC", id: "alpha" });
      const harness = makeHandlerContext(asDomDocument(ctx), { state: { streamData: { "alpha": stream } } });

      handlers.updateStreamRow(stream, harness.ctx);

      const row = ctx.document.querySelector(".stream-row[data-id=\"alpha\"]");

      assert.ok(row, "scheduleTableRender must have rendered the row");
      assert.equal(harness.ctx.state.tableRenderPending, false, "rAF callback must reset the pending gate");
    })();
  });

  test("updates only the health cell, show cell, and row tint when the row already exists", () => {

    /* The targeted update path is the cheap one: only mutates the cells that change between health ticks. We render a healthy row, then call updateStreamRow
     * with a stalled-state version of the same stream and confirm the row's class structure is intact (no full-row destroy/recreate).
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "alpha": makeStream({ channel: "NBC", id: "alpha" }) } }
      });

      handlers.renderStreamsTable(harness.ctx);

      const rowBefore = ctx.document.querySelector(".stream-row[data-id=\"alpha\"]");
      const updated = makeStream({ channel: "NBC", health: "stalled", id: "alpha", showName: "Today Show" });

      handlers.updateStreamRow(updated, harness.ctx);

      const rowAfter = ctx.document.querySelector(".stream-row[data-id=\"alpha\"]");

      assert.equal(rowBefore, rowAfter, "row element identity must be preserved (no re-render)");
      assert.match(rowAfter?.getAttribute("style") ?? "", /background-color: var\(--stream-tint-stalled\)/);
      assert.match(rowAfter?.querySelector(".stream-show")?.textContent ?? "", /Today Show/);
      assert.match(rowAfter?.querySelector(".stream-health")?.innerHTML ?? "", /Stalled/);
    })();
  });

  test("updates expanded-row detail metrics when the stream is expanded", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: {

          expandedStreams: { "x": true },
          streamData: { "x": makeStream({ id: "x", memoryBytes: 0, recoveryAttempts: 0 }) }
        }
      });

      handlers.renderStreamsTable(harness.ctx);

      const updated = makeStream({ id: "x", memoryBytes: 1572864, pageReloadsInWindow: 0, recoveryAttempts: 3 });

      handlers.updateStreamRow(updated, harness.ctx);

      const detail = ctx.document.querySelector(".stream-details[data-id=\"x\"]");

      assert.match(detail?.querySelector(".details-recovery")?.innerHTML ?? "", /3 attempts/);
      assert.match(detail?.querySelector(".details-memory")?.innerHTML ?? "", /1\.5 MB/);
    })();
  });
});

describe("status.handlers: toggleStreamDetails (DOM mutator)", () => {

  test("flips expandedStreams[id] and re-renders the table", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: {

          streamData: { "alpha": makeStream({ id: "alpha" }) }
        }
      });

      handlers.renderStreamsTable(harness.ctx);

      assert.equal(ctx.document.querySelector(".stream-details[data-id=\"alpha\"]"), null,
        "pre-condition: collapsed");

      handlers.toggleStreamDetails("alpha", harness.ctx);

      assert.equal(harness.ctx.state.expandedStreams["alpha"], true, "expanded state flipped to true");
      assert.ok(ctx.document.querySelector(".stream-details[data-id=\"alpha\"]"),
        "details row must render after toggle");

      handlers.toggleStreamDetails("alpha", harness.ctx);

      assert.equal(harness.ctx.state.expandedStreams["alpha"], false, "expanded state flipped back to false");
      assert.equal(ctx.document.querySelector(".stream-details[data-id=\"alpha\"]"), null,
        "details row must be removed after second toggle");
    })();
  });
});

describe("status.handlers: updateDurations (DOM mutator)", () => {

  test("refreshes every #duration-{id} element from the live now-startTime delta", () => {

    /* The renderer initially writes the duration; updateDurations refreshes it on every 1-second tick. We render a fresh row, then advance by setting a startTime
     * that's 65s in the past and calling handlers.updateDurations. The cell's textContent should reflect the new computation, not the original render.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "x": makeStream({ id: "x" }) } }
      });

      handlers.renderStreamsTable(harness.ctx);

      // Replace the entry with a startTime 65s in the past so the next updateDurations tick computes a fresh duration. StreamSummary.startTime is readonly, so
      // we substitute a new factory-built object rather than mutating the field in place.
      harness.ctx.state.streamData["x"] = makeStream({ id: "x", startTime: new Date(Date.now() - 65000).toISOString() });

      handlers.updateDurations(harness.ctx);

      const cell = ctx.document.getElementById("duration-x");

      assert.match(cell?.textContent ?? "", /1m 5s/);
    })();
  });

  test("schedules a popover refresh so the popover's duration column ticks alongside the table", () => {

    /* updateDurations calls schedulePopoverRender which calls updateStreamPopover via rAF. With sync-fire rAF, an open popover should be re-rendered. We seed an
     * open popover and verify its content was rebuilt by checking the channelDisplayCalls log on the recorder.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();

      ctx.document.getElementById("stream-popover-menu")?.classList.add("show");

      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "x": makeStream({ channel: "NBC", id: "x" }) } }
      });

      handlers.updateDurations(harness.ctx);

      assert.ok(harness.recorder.channelDisplayCalls.length >= 1,
        "schedulePopoverRender must have triggered buildStreamPopoverContent (channelDisplayHtml is invoked once per stream row)");
    })();
  });
});

describe("status.handlers: handleSnapshot (SSE handler)", () => {

  test("replaces all client state with the snapshot's contents and re-renders the system status, table, and popover", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: {

          // Pre-populate stale state to confirm the handler clears it.
          streamData: { "stale": makeStream({ id: "stale" }) }
        }
      });

      const snapshot: SnapshotPayload = {

        streams: [makeStream({ channel: "NBC", id: "1" })],
        system: { browser: { captureImpaired: false, connected: true }, streams: { active: 1, limit: 5 } }
      };

      handlers.handleSnapshot(snapshot, harness.ctx);

      assert.equal(harness.ctx.state.systemData?.streams.active, 1, "systemData must be replaced with snapshot value");
      assert.deepEqual(Object.keys(harness.ctx.state.streamData), ["1"], "streamData must be cleared and rebuilt from the snapshot");
      assert.equal(ctx.document.getElementById("stream-count")?.textContent, "1/5 streams", "system status DOM must be re-rendered");

      const rows = ctx.document.querySelectorAll(".stream-row");

      assert.equal(rows.length, 1, "table must be re-rendered from the snapshot's streams array");
    })();
  });

  test("forwards the optional channelPatch to channelTable.applyPatch for SSE-reconnect catch-up", () => {

    /* The snapshot's channelPatch field carries the server-rendered catch-up for any rows whose health/auth state changed during the disconnect gap. It travels
     * through the same applyPatch primitive as live channelUpdate events so client-side row state has a single ingress point.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));
      const channelPatch = { counts: { disabled: 0, enabled: 3, predefined: 3, total: 3, user: 0 }, rows: [{ action: "update", key: "nbc" }] };
      const snapshot: SnapshotPayload = {

        channelPatch,
        streams: [],
        system: { browser: { captureImpaired: false, connected: true }, streams: { active: 0, limit: 5 } }
      };

      handlers.handleSnapshot(snapshot, harness.ctx);

      assert.equal(harness.recorder.applyPatchCalls.length, 1, "channelPatch must be forwarded to channelTable.applyPatch exactly once");
      assert.deepEqual(harness.recorder.applyPatchCalls[0], channelPatch, "applyPatch must receive the patch verbatim");
    })();
  });

  test("skips the channelPatch forwarding when the field is absent (snapshot delivered before any health state existed)", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));
      const snapshot: SnapshotPayload = {

        streams: [],
        system: { browser: { captureImpaired: false, connected: true }, streams: { active: 0, limit: 5 } }
      };

      handlers.handleSnapshot(snapshot, harness.ctx);

      assert.equal(harness.recorder.applyPatchCalls.length, 0, "no patch is forwarded when channelPatch is absent");
    })();
  });
});

describe("status.handlers: handleStreamAdded (SSE handler)", () => {

  test("inserts the new stream into streamData and re-renders the table", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));

      handlers.handleStreamAdded(makeStream({ channel: "NBC", id: "new1" }), harness.ctx);

      assert.equal(harness.ctx.state.streamData["new1"]?.channel, "NBC");
      assert.ok(ctx.document.querySelector(".stream-row[data-id=\"new1\"]"), "table must show the new row after streamAdded");
    })();
  });
});

describe("status.handlers: handleStreamRemoved (SSE handler)", () => {

  test("drops the stream from streamData AND from expandedStreams", () => {

    /* Removing a stream must clean up its expandedStreams entry too - leaving the entry behind would leak memory and could cause a future stream with the same id
     * to render expanded by default. We seed both maps and confirm both are cleared.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: {

          expandedStreams: { "gone": true, "stay": true },
          streamData: { "gone": makeStream({ id: "gone" }), "stay": makeStream({ id: "stay" }) }
        }
      });

      handlers.handleStreamRemoved({ id: "gone" }, harness.ctx);

      assert.ok(!Object.hasOwn(harness.ctx.state.streamData, "gone"),
        "streamData entry must be deleted");
      assert.ok(!Object.hasOwn(harness.ctx.state.expandedStreams, "gone"),
        "expandedStreams entry must be deleted to avoid stale-id ghosts");
      assert.equal(harness.ctx.state.expandedStreams["stay"], true, "unrelated entries must remain untouched");
    })();
  });

  test("invokes externals.updateRestartDialogStatus when registered (config.ts has wired it)", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "x": makeStream({ id: "x" }) } }
      });

      handlers.handleStreamRemoved({ id: "x" }, harness.ctx);

      assert.equal(harness.recorder.updateRestartDialogStatusCount, 1,
        "the restart-dialog trampoline must fire once per stream removal");
    })();
  });

  test("does not throw when externals.updateRestartDialogStatus is undefined (config.ts not yet loaded)", () => {

    /* The optional-chain ?.() guard handles the early-page-load race where status.ts opens its EventSource before config.ts has registered the trampoline. We
     * confirm the handler completes cleanly with the externals.updateRestartDialogStatus left as undefined.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "x": makeStream({ id: "x" }) } },
        updateRestartDialogStatusDefined: false
      });

      // Must not throw.
      handlers.handleStreamRemoved({ id: "x" }, harness.ctx);

      assert.ok(!Object.hasOwn(harness.ctx.state.streamData, "x"));
    })();
  });
});

describe("status.handlers: handleStreamHealthChanged (SSE handler)", () => {

  test("ignores events for unknown stream ids (the streamRemoved race)", () => {

    /* If a healthChanged event arrives for a stream that's already been removed, the handler must early-return without populating the empty state with a partial
     * update. We assert by confirming streamData stays empty after the event.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));

      handlers.handleStreamHealthChanged(makeStream({ id: "ghost" }), harness.ctx);

      assert.deepEqual(harness.ctx.state.streamData, {}, "unknown id must not seed state");
    })();
  });

  test("uses the targeted updateStreamRow path when nothing structural changed (same logo, same mode, same hardwareAccelerated, same captureCodec)", () => {

    /* The cheap path: the row stays in place, only the health cell is mutated. We pre-render, then deliver an event whose only diff is the health field.
     * The row identity must be preserved.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const original = makeStream({ captureCodec: "h264", channel: "NBC", id: "x", logoUrl: "x.png", streamingMode: "capture" });
      const harness = makeHandlerContext(asDomDocument(ctx), { state: { streamData: { "x": original } } });

      handlers.renderStreamsTable(harness.ctx);

      const rowBefore = ctx.document.querySelector(".stream-row[data-id=\"x\"]");

      handlers.handleStreamHealthChanged({ ...original, health: "stalled" }, harness.ctx);

      const rowAfter = ctx.document.querySelector(".stream-row[data-id=\"x\"]");

      assert.equal(rowBefore, rowAfter, "non-structural change must preserve row element identity (no full re-render)");
      assert.match(rowAfter?.querySelector(".stream-health")?.innerHTML ?? "", /Stalled/);
    })();
  });

  test("triggers a full table render when a structural field changes (logoUrl, streamingMode, hardwareAccelerated, captureCodec)", () => {

    /* Structural changes can affect the badge column, the channel display, or the row's overall shape - cheap-path patches can leave the DOM in a stale
     * intermediate state. The handler routes structural changes through scheduleTableRender, which (with sync-fire rAF) re-renders the whole table inline.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const original = makeStream({ captureCodec: "h264", channel: "NBC", id: "x", streamingMode: "capture" });
      const harness = makeHandlerContext(asDomDocument(ctx), { state: { streamData: { "x": original } } });

      handlers.renderStreamsTable(harness.ctx);

      // streamingMode change is structural - triggers full re-render.
      handlers.handleStreamHealthChanged({ ...original, streamingMode: "native" }, harness.ctx);

      const rowAfter = ctx.document.querySelector(".stream-row[data-id=\"x\"]");

      assert.match(rowAfter?.innerHTML ?? "", /class="native-badge" title="Native HLS"/,
        "structural change must surface via full table render");
    })();
  });
});

describe("status.handlers: handleSystemStatusChanged (SSE handler)", () => {

  test("replaces systemData and re-renders the system status header", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));

      handlers.handleSystemStatusChanged({ browser: { captureImpaired: false, connected: false }, streams: { active: 2, limit: 10 } }, harness.ctx);

      assert.equal(harness.ctx.state.systemData?.browser.connected, false);
      assert.match(ctx.document.getElementById("system-health")?.innerHTML ?? "", /Browser offline/);
      assert.equal(ctx.document.getElementById("stream-count")?.textContent, "2/10 streams");
    })();
  });
});

describe("status.handlers: handleChannelUpdate (SSE handler)", () => {

  test("forwards the patch to externals.channelTable.applyPatch verbatim", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));
      const patch = { counts: { disabled: 0, enabled: 7, predefined: 5, total: 7, user: 2 }, rows: [] };

      handlers.handleChannelUpdate(patch, harness.ctx);

      assert.equal(harness.recorder.applyPatchCalls.length, 1);
      assert.equal(harness.recorder.applyPatchCalls[0], patch, "patch must be forwarded by reference");
    })();
  });
});

describe("status.handlers: handleSseError (SSE handler)", () => {

  test("replaces the system-health badge with the stalled color and 'Updates paused' label", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));

      handlers.handleSseError(harness.ctx);

      const html = ctx.document.getElementById("system-health")?.innerHTML ?? "";

      assert.match(html, /Updates paused/);
      assert.match(html, /var\(--stream-stalled\)/);
    })();
  });

  test("no-op when the system-health element is absent (defensive guard)", () => {

    /* Defensive: if the page somehow loads without the system-health element (a rendering bug or a partial fragment), the handler must not throw on the optional
     * element. We synthesize a document with no #system-health and confirm the call completes cleanly.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();

      ctx.document.getElementById("system-health")?.remove();

      const harness = makeHandlerContext(asDomDocument(ctx));

      // Must not throw.
      handlers.handleSseError(harness.ctx);
    })();
  });
});

describe("status.handlers: toggleStreamPopover (window-bound trampoline)", () => {

  test("does nothing when streamData is empty", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));

      handlers.toggleStreamPopover(harness.ctx);

      assert.equal(harness.recorder.dropdownsCloseCount, 0, "no streams means no popover work");
      assert.equal(ctx.document.getElementById("stream-popover-menu")?.classList.contains("show"), false);
    })();
  });

  test("opens the popover when closed: closes other dropdowns first, then builds content and adds .show", () => {

    /* The opening sequence: dropdowns.close (so other open dropdowns dismiss before the popover takes over) -> buildStreamPopoverContent (populates the menu) ->
     * .classList.add('show') (reveals it). We assert all three side effects in order via the recorder + DOM state.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "x": makeStream({ channel: "NBC", id: "x" }) } }
      });

      handlers.toggleStreamPopover(harness.ctx);

      assert.equal(harness.recorder.dropdownsCloseCount, 1, "dropdowns.close must fire before opening");
      assert.equal(ctx.document.getElementById("stream-popover-menu")?.classList.contains("show"), true);
      assert.match(ctx.document.getElementById("stream-popover-menu")?.innerHTML ?? "", /NBC/);
    })();
  });

  test("closes the popover when already open (delegates to dropdowns.close, does NOT re-open)", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const menu = ctx.document.getElementById("stream-popover-menu");

      menu?.classList.add("show");

      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "x": makeStream({ channel: "NBC", id: "x" }) } }
      });

      handlers.toggleStreamPopover(harness.ctx);

      assert.equal(harness.recorder.dropdownsCloseCount, 1, "dropdowns.close must fire to dismiss");

      // Note: dropdowns.close() in production removes .show; the stub does not, but the !isOpen guard means we don't re-add it. We assert that reopening does not happen.
      assert.equal(menu?.classList.contains("show"), true,
        "stub dropdowns.close does not remove .show; the test asserts the no-re-open rule via the absence of buildStreamPopoverContent (no channelDisplayCalls)");
      assert.equal(harness.recorder.channelDisplayCalls.length, 0,
        "buildStreamPopoverContent must NOT fire on the close path");
    })();
  });
});

describe("status.handlers: copyOverviewPlaylistUrl (window-bound trampoline)", () => {

  test("delegates to externals.copyToClipboard with the textContent of #overview-playlist-url", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));

      handlers.copyOverviewPlaylistUrl(harness.ctx);

      assert.equal(harness.recorder.copyToClipboardCalls.length, 1);

      const call = harness.recorder.copyToClipboardCalls[0]!;

      assert.equal(call.message, "Playlist URL copied to clipboard.");
      assert.match(call.text, /\/playlist$/, "the URL must come from #overview-playlist-url's textContent (server-rendered)");
    })();
  });

  test("no-op when the overview-playlist-url element is absent", () => {

    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();

      ctx.document.getElementById("overview-playlist-url")?.remove();

      const harness = makeHandlerContext(asDomDocument(ctx));

      handlers.copyOverviewPlaylistUrl(harness.ctx);

      assert.equal(harness.recorder.copyToClipboardCalls.length, 0,
        "missing element must NOT trigger a clipboard write");
    })();
  });
});

describe("status.handlers: handleVisibilityChange (lifecycle helper)", () => {

  test("when document.hidden is true, stamps state.hiddenSince with Date.now() and does not reconnect", () => {

    /* The transition into hidden mode just records the timestamp - the reconnect logic only fires on the visible-again branch. We assert: hiddenSince becomes
     * non-zero, reconnect is not invoked.
     */
    let reconnectCalls = 0;
    const fakeDoc = { hidden: true } as unknown as Document;
    const harness = makeHandlerContext(fakeDoc);

    handlers.handleVisibilityChange(harness.ctx, () => { reconnectCalls++; });

    assert.notEqual(harness.ctx.state.hiddenSince, 0, "hiddenSince must be stamped");
    assert.equal(reconnectCalls, 0, "reconnect must NOT fire on the hidden transition");
  });

  test("returns to visible after >30s hidden: calls reconnect, dispatches tabactivated, resets hiddenSince", () => {

    /* The reconnect-on-return-from-hidden branch is gated on (hiddenSince > 0) && (now - hiddenSince > 30000). We seed hiddenSince to 31s ago and confirm all
     * three side effects: reconnect call, hiddenSince reset to 0, tabactivated dispatched.
     */
    let reconnectCalls = 0;
    const dispatchedEvents: { type: string; detail?: unknown }[] = [];
    const activeTab = { getAttribute: (n: string): string => (n === "data-category") ? "channels" : "" };
    const fakeDoc = {

      dispatchEvent: (event: { detail?: unknown; type: string }): boolean => {

        dispatchedEvents.push({ detail: event.detail, type: event.type });

        return true;
      },
      hidden: false,
      querySelector: (sel: string): typeof activeTab | null => (sel === ".tab-btn.active") ? activeTab : null
    } as unknown as Document;

    const harness = makeHandlerContext(fakeDoc, { state: { hiddenSince: Date.now() - 31000 } });

    handlers.handleVisibilityChange(harness.ctx, () => { reconnectCalls++; });

    assert.equal(reconnectCalls, 1);
    assert.equal(harness.ctx.state.hiddenSince, 0, "hiddenSince must be reset");
    assert.equal(dispatchedEvents.length, 1);
    assert.equal(dispatchedEvents[0]?.type, "tabactivated");
  });

  test("returns to visible after <30s hidden: just clears hiddenSince, no reconnect", () => {

    /* Short visibility blips don't warrant a reconnect (the SSE connection should still be alive). We seed hiddenSince to 5s ago, confirm reconnect is not called
     * and hiddenSince is cleared.
     */
    let reconnectCalls = 0;
    const fakeDoc = { hidden: false } as unknown as Document;
    const harness = makeHandlerContext(fakeDoc, { state: { hiddenSince: Date.now() - 5000 } });

    handlers.handleVisibilityChange(harness.ctx, () => { reconnectCalls++; });

    assert.equal(reconnectCalls, 0);
    assert.equal(harness.ctx.state.hiddenSince, 0);
  });

  test("returns to visible without prior hidden state: no reconnect, hiddenSince stays 0", () => {

    let reconnectCalls = 0;
    const fakeDoc = { hidden: false } as unknown as Document;
    const harness = makeHandlerContext(fakeDoc);

    handlers.handleVisibilityChange(harness.ctx, () => { reconnectCalls++; });

    assert.equal(reconnectCalls, 0);
    assert.equal(harness.ctx.state.hiddenSince, 0);
  });
});

describe("status.handlers: initIPadTooltips (lifecycle helper)", () => {

  test("no-op on devices that support hover (matchMedia('(hover: none)').matches === false)", () => {

    /* The early-return guard means desktop browsers (which can hover) skip the tooltip element creation. We confirm by asserting no .btn-icon-tooltip element
     * appears in the document body after the call. This needs globalThis.window with matchMedia stubbed to return matches=false.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();

      // Inject a window stub so the function's `window.matchMedia(...)` reference resolves. happy-dom's window.matchMedia exists but may default to matches=false
      // anyway; we override explicitly to assert the desktop branch.
      const previousWindow = (globalThis as { window?: unknown }).window;

      (globalThis as { window?: unknown }).window = { matchMedia: (): { matches: boolean } => ({ matches: false }) };

      try {

        const harness = makeHandlerContext(asDomDocument(ctx));

        handlers.initIPadTooltips(harness.ctx);

        assert.equal(ctx.document.querySelector(".btn-icon-tooltip"), null,
          "desktop devices must NOT have a tooltip element appended");
      } finally {

        if(previousWindow === undefined) {

          delete (globalThis as { window?: unknown }).window;
        } else {

          (globalThis as { window?: unknown }).window = previousWindow;
        }
      }
    })();
  });

  test("appends a .btn-icon-tooltip element to the body when matchMedia('(hover: none)').matches === true", () => {

    /* The mobile/iPad branch creates and appends a single shared tooltip element. We assert its presence and class. We do not exercise the mouseenter/mouseleave
     * handlers - those depend on getBoundingClientRect, which happy-dom returns as a zero-sized rect, making the positioning logic unobservable. The element's
     * existence is the witness that the init path fired.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const previousWindow = (globalThis as { window?: unknown }).window;

      (globalThis as { window?: unknown }).window = {

        matchMedia: (): { matches: boolean } => ({ matches: true }),
        scrollX: 0,
        scrollY: 0
      };

      try {

        const harness = makeHandlerContext(asDomDocument(ctx));

        handlers.initIPadTooltips(harness.ctx);

        const tip = ctx.document.querySelector(".btn-icon-tooltip");

        assert.ok(tip, "iPad/mobile devices must have a shared tooltip element appended to body");
      } finally {

        if(previousWindow === undefined) {

          delete (globalThis as { window?: unknown }).window;
        } else {

          (globalThis as { window?: unknown }).window = previousWindow;
        }
      }
    })();
  });
});

describe("status.handlers: render schedulers (rAF gates)", () => {

  test("scheduleTableRender is a no-op on repeat within a frame: repeated calls before the rAF fires schedule only one render", async () => {

    /* We swap rAF for a deferred-fire stub for this test only - the module-scope synchronous-fire stub would defeat the test by firing each call inline. The
     * deferred stub queues the callbacks; we manually fire them after the schedule round-trip. The pending-flag gate must prevent more than one schedule from
     * being recorded. The try/finally MUST live inside the async body so the cleanup runs after the assertions, not synchronously before them.
     */
    const rafQueue: FrameRequestCallback[] = [];
    const previousRaf = globalThis.requestAnimationFrame;

    globalThis.requestAnimationFrame = (cb: FrameRequestCallback): number => {

      rafQueue.push(cb);

      return rafQueue.length;
    };

    try {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));

      handlers.scheduleTableRender(harness.ctx);
      handlers.scheduleTableRender(harness.ctx);
      handlers.scheduleTableRender(harness.ctx);

      assert.equal(rafQueue.length, 1, "multiple scheduleTableRender calls within a frame must coalesce into one rAF");
      assert.equal(harness.ctx.state.tableRenderPending, true, "pending gate must be set");

      // Fire the queued callback to drain the gate.
      rafQueue[0]!(0);

      assert.equal(harness.ctx.state.tableRenderPending, false, "rAF callback must reset the gate");

      // After draining, a second call must schedule a fresh rAF - the gate is open again.
      handlers.scheduleTableRender(harness.ctx);

      assert.equal(rafQueue.length, 2, "post-drain calls must schedule a fresh rAF");
    } finally {

      globalThis.requestAnimationFrame = previousRaf;
    }
  });

  test("schedulePopoverRender follows the same gating contract as scheduleTableRender", async () => {

    const rafQueue: FrameRequestCallback[] = [];
    const previousRaf = globalThis.requestAnimationFrame;

    globalThis.requestAnimationFrame = (cb: FrameRequestCallback): number => {

      rafQueue.push(cb);

      return rafQueue.length;
    };

    try {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx));

      handlers.schedulePopoverRender(harness.ctx);
      handlers.schedulePopoverRender(harness.ctx);

      assert.equal(rafQueue.length, 1);
      assert.equal(harness.ctx.state.popoverRenderPending, true);

      rafQueue[0]!(0);

      assert.equal(harness.ctx.state.popoverRenderPending, false);
    } finally {

      globalThis.requestAnimationFrame = previousRaf;
    }
  });
});

describe("status.handlers: status-field render-boundary escaping", () => {

  /* These fields are server-provided and constrained (captureCodec is drawn from a small, closed set of recognized codecs, nativeResolution is regex-sanitized to
   * digits-x-digits at the probe, lastIssueType and client types are internal enums), so they carry no live attacker vector now. The render boundary escapes them
   * anyway, uniformly, so the status display's safety does not depend on a provenance argument that could silently break if a source loosened (nativeResolution,
   * for instance, derives from the upstream HLS manifest). Each test feeds a crafted value and asserts it survives as entities with no live element parsed out.
   */

  test("renderDetailCodec entity-encodes the captureCodec", () => {

    const out = handlers.renderDetailCodec(makeStream({ captureCodec: "<b>x</b>", hardwareAccelerated: true }));

    assert.match(out, /&lt;b&gt;x&lt;\/b&gt;/, "the codec must be entity-encoded in the codec line");
    assert.doesNotMatch(out, /<b>/, "no raw tag may survive in the codec line");
  });

  test("renderDetailCodec entity-encodes the nativeResolution fallback when the height is not a known label", () => {

    const out = handlers.renderDetailCodec(makeStream({ nativeBandwidth: NATIVE_BANDWIDTH, nativeResolution: "<script>evil</script>", streamingMode: "native" }));

    assert.match(out, /&lt;script&gt;evil&lt;\/script&gt;/, "the unmapped resolution must fall back to the entity-encoded raw value");
    assert.doesNotMatch(out, /<script>/, "no raw tag may survive in the codec line");
  });

  test("formatLastIssue entity-encodes the lastIssueType", () => {

    const out = handlers.formatLastIssue(makeStream({ lastIssueTime: new Date().toISOString(), lastIssueType: "<i>nav</i>" }));

    assert.match(out, /&lt;i&gt;nav&lt;\/i&gt;/, "the issue label must be entity-encoded");
    assert.doesNotMatch(out, /<i>/, "no raw tag may survive in the issue label");
  });

  test("formatClients entity-encodes an unrecognized client type", () => {

    const out = handlers.formatClients(makeStream({ clientCount: 2, clients: [{ count: 2, type: "<img src=x onerror=alert(1)>" }] }));

    assert.match(out, /&lt;img/, "an unmapped client type must be entity-encoded");
    assert.doesNotMatch(out, /<img/, "no raw tag may survive in the client breakdown");
  });

  test("renderStreamsTable entity-encodes the captureCodec in the hardware-accelerated badge", () => {

    /* The native badge in the table concatenates captureCodec into innerHTML. We render a crafted hardware-accelerated stream and assert the badge markup carries
     * the codec as entities with no live element parsed out of the table.
     */
    return (async (): Promise<void> => {

      await using ctx = await createDomTestContext();
      const harness = makeHandlerContext(asDomDocument(ctx), {

        state: { streamData: { "x": makeStream({ captureCodec: "<svg onload=alert(1)>", hardwareAccelerated: true, id: "x" }) } }
      });

      handlers.renderStreamsTable(harness.ctx);

      const badge = ctx.document.querySelector(".stream-row[data-id=\"x\"] .native-badge");

      assert.ok(badge, "the hardware-accelerated badge must render");
      assert.match(badge.innerHTML, /&lt;svg/, "the codec must be entity-encoded in the badge markup");
      assert.equal(ctx.document.querySelector(".stream-row[data-id=\"x\"] svg"), null, "no live <svg> may be parsed out of the escaped codec");
    })();
  });
});

describe("status.handlers - module export inventory", () => {

  /* The export {} block at the bottom of status.handlers.ts is the documented test surface - DOM-runtime suite imports it via `import * as handlers`, the
   * production-emitted script consumes the same functions via Function.prototype.toString() in HANDLER_FUNCTIONS, and a regression that adds an export without
   * adding it to either consumer would silently leak into the public surface. We assert the export set the same way icons.test.ts asserts the icon list - a closed
   * set test that fails when an identifier is added or removed.
   */

  // The full set of value exports - functions and constants. TypeScript-only types and interfaces (StreamSummary, HandlerContext, etc.) are erased at runtime
  // and do not appear in the namespace object's keys, so the list below is exclusively the runtime-observable surface.
  const EXPECTED_EXPORTS: readonly string[] = [
    "HANDLER_CONSTANTS",
    "HANDLER_FUNCTIONS",
    "buildStreamPopoverContent",
    "copyOverviewPlaylistUrl",
    "createInitialState",
    "formatAutoRecovery",
    "formatBytes",
    "formatClients",
    "formatDuration",
    "formatLastIssue",
    "formatTime",
    "formatTimeAgo",
    "getDomain",
    "getHealthBadge",
    "getRecoveringLabel",
    "handleChannelUpdate",
    "handleSnapshot",
    "handleSseError",
    "handleStreamAdded",
    "handleStreamHealthChanged",
    "handleStreamRemoved",
    "handleSystemStatusChanged",
    "handleVisibilityChange",
    "initIPadTooltips",
    "renderDetailCodec",
    "renderDetailStarted",
    "renderHealthCellContent",
    "renderStreamsTable",
    "schedulePopoverRender",
    "scheduleTableRender",
    "toggleStreamDetails",
    "toggleStreamPopover",
    "updateDurations",
    "updateStreamPopover",
    "updateStreamRow",
    "updateSystemStatus"
  ];

  test("module exports exactly the documented surface (no unexpected additions, no missing entries)", () => {

    // Object.keys on the namespace import gives the value-export keys in source order. We sort both lists for a deterministic comparison; the closed-set check
    // forces every new export to be added to EXPECTED_EXPORTS so the test surface stays explicit.
    const actualKeys = Object.keys(handlers).toSorted();
    const expectedSorted = [...EXPECTED_EXPORTS].toSorted();

    assert.deepEqual(actualKeys, expectedSorted, "module exports should match the EXPECTED_EXPORTS list exactly");
  });

  test("HANDLER_FUNCTIONS includes every emittable function the production script body needs", () => {

    // The HANDLER_FUNCTIONS array drives the emitted script body in generateStatusScript. Every function that needs to ship to the browser must be in this
    // array - missing one means the runtime breaks at the call site. HANDLER_CONSTANTS and createInitialState are NOT emitted (createInitialState is called only
    // by status.ts at IIFE start). The lower-bound check guards against an accidental drop without re-asserting the exact count on every additive change.
    assert.ok(handlers.HANDLER_FUNCTIONS.length > 25, "HANDLER_FUNCTIONS contains the documented script-body functions");

    for(const fn of handlers.HANDLER_FUNCTIONS) {

      assert.equal(typeof fn, "function", "every HANDLER_FUNCTIONS entry is a function");
      assert.ok(fn.name.length > 0, "every HANDLER_FUNCTIONS entry has a non-empty name (Function.prototype.toString relies on it)");
    }
  });
});
