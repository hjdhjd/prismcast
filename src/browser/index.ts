/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Browser lifecycle management for PrismCast.
 */
import type { Browser, LaunchOptions, Page } from "puppeteer-core";
import type { BrowserLifecycle, BrowserPurpose, CaptureImpairment } from "./browserSupervisor.ts";
import type { ChangeRejection, ConfigChange } from "../config/reactivity.ts";
import type { Config, Nullable } from "../types/index.ts";
import { LOG, boundedWait, evaluateWithAbort, formatError, isProcessRunning, listProcesses, setChromeUserAgent, startTimer } from "../utils/index.ts";
import { TimerRegistry, systemClock } from "homebridge-plugin-utils";
import { clearLoginState, isLoginModeActive, setLoginDeps } from "./login.ts";
import { getAllStreams, getStreamCount, hasActiveCaptureStreams, hasEstablishedStreams, isCaptureIdentity } from "../streaming/registry.ts";
import { getCachedTabId, installStrayOpenTabReaper, onTabActivation } from "./tabSelection.ts";
import { getChromeDataDir, getDataDir, getExtensionDir } from "../config/paths.ts";
import { getExtensionPage, launch } from "puppeteer-stream";
import { getGpuCapabilities, setGpuCapabilities } from "./display.ts";
import { minimizeWindow, readWindowPlacement, unminimizeWindow, withCDPSession } from "./cdp.ts";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "homebridge-plugin-utils";
import { EXTENSION_READY_EXPRESSION } from "./tabCapture.ts";
import type { GpuCapabilities } from "./display.ts";
import type { LaunchGovernorPolicy } from "./launchGovernor.ts";
import type { ProcessInfo } from "../utils/index.ts";
import type { SystemStatus } from "../streaming/statusEmitter.ts";
import type { WindowPlacement } from "./cdp.ts";
import { clearChannelSelectionCaches } from "./channelSelection.ts";
import { createBrowserSupervisor } from "./browserSupervisor.ts";
import { createWindowVisibilitySync } from "./windowSync.ts";
import { emitSystemStatusChanged } from "../streaming/statusEmitter.ts";
import { evaluateStalePages } from "./pageStaleness.ts";
import fs from "node:fs";
import { getPresetViewport } from "../config/presets.ts";
import path from "node:path";
import { launch as puppeteerLaunch } from "puppeteer-core";
import { registerConfigChangeHandler } from "../config/reactivity.ts";
import { startPrecaching } from "./precaching.ts";

const { promises: fsPromises } = fs;

/* Global variables maintain the application's runtime state across all operations. We minimize global state where possible, but some values must be shared across
 * the application lifecycle:
 *
 * - supervisor: The browser capture-readiness supervisor. It is the single source of truth for the shared Chrome instance and its lifecycle (every phase from
 *   absent to closing), so all streaming sessions use one Chrome process via supervisor.acquire(). It holds one discriminated-union lifecycle state that captures
 *   the browser reference, its launch timestamp, and whether a launch is in flight, and routes every relaunch through one loop-safe governor.
 *
 * - currentChromeVersion: The one piece of per-browser metadata the adapter holds directly, captured when the browser becomes ready and surfaced by the
 *   health endpoint.
 *
 * Stream tracking and ID generation live in streaming/registry.ts for unified stream management across all output types (HLS, MPEG-TS, etc.). Filesystem path
 * resolution for persistent data (the Chrome profile and the streaming extension files) is centralized in config/paths.ts and resolved on demand rather than held
 * as module state here; ensureDataDirectory() below creates the data directory at startup.
 */

// The Chrome version string (e.g., "Chrome/144.0.7559.110") captured when the browser becomes capture-ready in launchReadyBrowser. Cleared when the browser
// disconnects or is closed. This is the one piece of per-browser metadata the adapter holds directly; the browser instance, its launch timestamp, and the
// launch mutex live inside the supervisor's lifecycle state, which is the single source of truth for "what is the browser doing, and is it usable?".
let currentChromeVersion: Nullable<string> = null;

/* The browser relaunch governor's escalating cooldown ladder: 5 minutes, then 15, then 60. This is the escalation SHAPE - a design constant - rather than an
 * operational tolerance, so it stays in code while the scalar tolerances (failure threshold, window, health hold) are operator-tunable via CONFIG.recovery. Each
 * successive trip cools down for the next-longer rung; the final rung is the ceiling.
 */
const RELAUNCH_COOLDOWN_LADDER_MS: readonly number[] = [ 5 * 60 * 1000, 15 * 60 * 1000, 60 * 60 * 1000 ];

/* How long Chrome is given to exit after SIGTERM, and then after SIGKILL. The escalation is SIGTERM-first so Chrome can flush its profile databases (LevelDB,
 * extension state, session storage) instead of having them corrupted by an immediate kill, and the SIGTERM window is generous because containerized environments
 * with software rendering and shared CPU may need all of it. Every path that signals Chrome - the orderly close of a running instance and the startup sweep of
 * stale ones - shares this pair, so the escalation behaves identically wherever it runs.
 */
const TERM_WAIT_MS = 5000;
const KILL_WAIT_MS = 2000;

/* The worst case a browser teardown can take before the Chrome process is certainly gone. The supervisor's closing state hands this to requests that arrive
 * mid-drain as their retry horizon, so it is derived from the waits above rather than restated: the bound cannot drift from what the teardown actually allows.
 */
const BROWSER_TEARDOWN_DRAIN_BOUND_MS = TERM_WAIT_MS + KILL_WAIT_MS;

/**
 * Builds the browser relaunch governor's policy from live configuration. The supervisor's policy port is a getter, so this is read fresh at each governor decision -
 * an operator's change to the recovery.relaunch* settings takes effect at the next governor decision without reconstructing the supervisor. The scalar
 * tolerances come from CONFIG.recovery (conservative, biased eager-for-the-first-failure: the first failures cost no cooldown; only
 * repeated failures within the window trip the escalating cooldown); the cooldown ladder is the fixed escalation shape above.
 * @returns The current launch governor policy.
 */
function buildRelaunchPolicy(): LaunchGovernorPolicy {

  return {

    cooldownLadderMs: RELAUNCH_COOLDOWN_LADDER_MS,
    failureThreshold: CONFIG.recovery.relaunchFailureThreshold,
    failureWindowMs: CONFIG.recovery.relaunchFailureWindow,
    healthHoldMs: CONFIG.recovery.relaunchHealthHold
  };
}

/* The one browser capture-readiness supervisor for the process lifetime. It owns the lifecycle state (one discriminated union covering every phase from absent to
 * closing) that unifies the browser reference, launch promise, and launch timestamp, and routes every relaunch through one loop-safe governor. The adapter
 * injects the impure ports: launchReadyBrowser (spawn Chrome and run the readiness gate), closeBrowserInstance (teardown), systemClock.now (time),
 * buildRelaunchPolicy (live config bounds), and onSupervisorStateChange (the loud degraded alarm and the recovery notice). All browser access flows through it:
 * getCurrentBrowser is acquire(); the non-launching reads derive from current() and currentLaunchTime(). The injected ports are hoisted function declarations, so
 * referencing them here is safe even though they are defined further down the module.
 */
const supervisor = createBrowserSupervisor({ close: closeBrowserInstance, launch: launchReadyBrowser, now: (): number => systemClock.now(),
  onStateChange: onSupervisorStateChange, policy: buildRelaunchPolicy });

/**
 * Observes supervisor lifecycle transitions purely for operator-visible signals; it never affects the transition (the supervisor treats it as best-effort, so a
 * throwing logger cannot corrupt the lifecycle). It raises the loud degraded alarm when the relaunch governor trips, the capture-impairment alarm when a published
 * browser is marked as unable to start captures, an info notice when capture readiness is restored after a degraded period, and emits the SSE system status
 * whenever a ready browser is published. It reports rather than acts: the caller holding a verdict is what drives any recovery from it, because a restart begun
 * from inside this observer would re-enter the supervisor while the transition that notified it is still on the stack.
 * @param next - The state being entered.
 * @param previous - The state being left.
 */
function onSupervisorStateChange(next: BrowserLifecycle, previous: BrowserLifecycle): void {

  // The governor just tripped: relaunches are paused while the browser's capture system cools down. We log loudly at ERROR with the cooldown horizon so the
  // condition is never invisible - the enforceable form of "it is impossible to be silently un-tunable."
  if((next.kind === "degraded") && (previous.kind !== "degraded")) {

    const cooldownMinutes = Math.max(1, Math.round((next.until - systemClock.now()) / 60000));

    LOG.error("The browser capture system has degraded and the relaunch governor has tripped: %s Relaunches are paused for approximately %d minute(s) while it " +
      "cools down; new stream requests will receive a 503 back-off until it recovers.", next.reason, cooldownMinutes);
  } else if((next.kind === "ready") && ((previous.kind === "trialing") || (previous.kind === "degraded"))) {

    // Capture readiness was restored by a successful trial after a degraded period. The ordinary first launch (absent -> launching -> ready) is intentionally
    // silent; only a recovery from trialing/degraded is worth an operator notice.
    LOG.info("The browser capture system has recovered and is serving captures again.");
  } else if((next.kind === "ready") && (next.impairment !== null) && ((previous.kind !== "ready") || (previous.impairment === null))) {

    // The published browser has been marked as unable to start captures. The transition carrying the mark happens once per instance, so the alarm fires once per
    // instance too, and it says what an operator watching a stalled tune needs: the running captures are unaffected, new requests back off, and the cure arrives
    // on its own as soon as nothing on this browser holds a page.
    LOG.error("The browser can no longer start captures (%s). Its running captures continue, new stream requests receive a 503 back-off, and it will relaunch as " +
      "soon as nothing holds a page on it.", next.impairment.reason);
  }

  // The browser's connectivity is part of the SSE system status, so emit when a ready browser is published. Readiness-loss emits are owned by handleBrowserDisconnect
  // (genuine disconnect) and the shutdown path; emitSystemStatusChanged dedupes, so a redundant emit is a cheap no-op.
  if(next.kind === "ready") {

    void emitCurrentSystemStatus();
  }
}

/* The capture-readiness probe is the capability tier of the launch gate: a real capture acquisition against a throwaway page on the instance being launched - the
 * authoritative "can this browser actually capture?" predicate that must run at every (re)launch, not only boot. It lives in streaming/setup.ts (which owns the
 * capture lock and the probe policy) and is injected here via setCaptureProbe, because setup.ts already depends on this module:
 * injecting the function rather than importing it keeps the dependency one-directional and breaks the cycle, mirroring the loginDeps setter/getter
 * injection pattern between login.ts and index.ts. The probe must also take the local instance as a parameter rather than re-entering getCurrentBrowser, since
 * launchReadyBrowser IS the in-flight launch - re-entering acquire() would join its own pending promise and deadlock.
 */
type CaptureProbe = (browser: Browser) => Promise<void>;

/* The capture-readiness probe (capability tier of the launch gate). Null until streaming/setup.ts injects the real capture probe at module load, which the import
 * order places ahead of any launch: index.ts value-imports app.ts, app.ts value-imports streaming/hls.ts, and hls.ts value-imports streaming/setup.ts, so the
 * injection has run by the time index.ts's own body calls startServer. launchReadyBrowser refuses to publish a browser if it is somehow still null (see the call
 * site), rather than serving an unverified one.
 */
let captureProbe: Nullable<CaptureProbe> = null;

/**
 * Injects the capture-readiness probe used as the capability tier of the launch gate. Called once from streaming/setup.ts at module load (which always precedes any
 * launch, since the streaming layer is imported during server startup). Separating the wiring from the call keeps browser/index.ts from importing
 * streaming/setup.ts, which already imports this module, so the dependency stays one-directional.
 * @param probe - The probe to run against a freshly-launched browser; it resolves when the browser can capture and rejects when it cannot.
 */
export function setCaptureProbe(probe: CaptureProbe): void {

  captureProbe = probe;
}

/* Stream teardown belongs to the streaming layer: terminateStream (streaming/lifecycle.ts) owns the whole cleanup sequence - segmenter, monitor, page, registry,
 * client tracking and SSE events - and a readiness loss here runs that same sequence rather than a second one that could drift from it. It is injected via
 * setStreamTerminator because lifecycle.ts already depends on this module for the shutdown flag, the window sync and the managed-page registry: injecting the
 * function rather than importing it keeps that dependency one-directional, the same boundary setCaptureProbe draws with streaming/setup.ts. The terminator's
 * optional clock parameter is left off this type because the browser layer has no clock to offer and takes the streaming layer's default.
 */
type StreamTerminator = (streamId: number, channelName: string, reason: string) => void;

/* The authoritative stream terminator. Null until streaming/lifecycle.ts injects terminateStream at module load, which the import order places ahead of any
 * browser launch: index.ts value-imports app.ts, app.ts value-imports streaming/lifecycle.ts, and a module body finishes before the body of anything that
 * imports it, so the injection has run by the time index.ts's own body calls startServer - the earliest moment a launch, and so a disconnect, can happen. A
 * still-null terminator at readiness loss is reported at ERROR and the teardown carries on (see the call site), because losing the rest of the teardown on top
 * of the stream cleanup would make a recoverable situation worse.
 */
let streamTerminator: Nullable<StreamTerminator> = null;

/**
 * Injects the authoritative stream terminator used when browser readiness is lost. Called once from streaming/lifecycle.ts at module load. Separating the wiring
 * from the call keeps browser/index.ts from importing streaming/lifecycle.ts, which already imports this module, so the dependency stays one-directional.
 * @param terminator - The function that tears down one stream and all of the resources it owns.
 */
export function setStreamTerminator(terminator: StreamTerminator): void {

  streamTerminator = terminator;
}

// The identity of the periodic sweep on the stale-page owner's registry.
const STALE_PAGE_SWEEP_KEY = "sweep";

/**
 * The stale-page owner: the registry its periodic sweep is armed on, which closes browser pages no active stream is using so a long session cannot exhaust
 * resources, and the clock the start received. The clock is kept beside the registry because a re-arm rebuilds the sweep's callback, which reads that clock.
 */
interface StalePageSweep {

  // The clock the sweep's interval arms on and whose reading each sweep judges staleness against.
  readonly clock: Clock;

  // The registry the sweep's interval lives on, disposed by the stop so the sweep can never outlive the owner.
  readonly timers: TimerRegistry;
}

// The running stale-page sweep. Null until started, and again once stopped.
let stalePageSweep: Nullable<StalePageSweep> = null;

/* Opportunistic browser restart state. Chrome accumulates memory pressure, GPU process issues, and general flakiness over multi-hour sessions with continuous
 * media playback. We proactively restart Chrome after it has been running for BROWSER_MAX_AGE, waiting for a quiet period with zero active streams before
 * executing the restart. After the restart, a fresh browser is launched immediately so it is ready for the next stream request.
 */

// Maximum browser uptime before considering a restart (6 hours).
const BROWSER_MAX_AGE = 6 * 60 * 60 * 1000;

// Duration of the quiet period (zero streams) required before executing the restart (5 minutes).
const BROWSER_RESTART_QUIET_PERIOD = 5 * 60 * 1000;

// How often to check whether the browser qualifies for a restart (30 seconds).
const BROWSER_RESTART_CHECK_INTERVAL = 30000;

// Why a restart is running. The maintenance cause is the age-driven one the quiet period gates; the impairment cause is the relaunch of a browser that can no
// longer start captures, which waits on nothing depending on the browser instead of on age. The routine's log line, its trigger, and the idleness it waits for
// differ by cause; everything else it does is shared.
export type BrowserRestartCause = "impairment" | "maintenance";

// The identity of the periodic eligibility check on the restart owner's registry.
const RESTART_CHECK_KEY = "check";

// The identity of the quiet-period countdown on the restart owner's registry.
const RESTART_QUIET_KEY = "quiet";

/* The restart owner's timers, keyed on one registry built on the clock its start receives: the periodic eligibility check, and - while the browser has exceeded
 * BROWSER_MAX_AGE and we are waiting for BROWSER_RESTART_QUIET_PERIOD to elapse with zero active streams - the quiet-period countdown, which a stream starting
 * during the wait cancels. Holding both on one registry is what makes the stop drain the countdown along with the check. Null until the check is started, and
 * again once it is stopped.
 */
let restartTimers: Nullable<TimerRegistry> = null;

/**
 * Cancels the scheduled-restart quiet period if one is pending, so the countdown it was running cannot fire against a browser it no longer describes. Reads the
 * module binding rather than a handed-in registry because executeBrowserRestart calls it as well, which also makes it a no-op once the owner has been stopped.
 */
function cancelRestartQuietTimer(): void {

  restartTimers?.clear(RESTART_QUIET_KEY);
}

// The process-wide graceful-shutdown state. app.ts shutdown() sets it early, and closeBrowser() sets it as a fallback for direct calls. While it is set, the
// disconnect path stays quiet (no error log and no status emit) and the restart paths stand down; stream termination still runs. This prevents false
// "unexpected disconnect" errors during graceful shutdown.
let gracefulShutdownInProgress = false;

/**
 * Returns true if graceful shutdown is in progress.
 */
export function isGracefulShutdown(): boolean {

  return gracefulShutdownInProgress;
}

/**
 * Sets the graceful shutdown flag. Call this at the start of shutdown, before terminating streams, so that page close errors are suppressed.
 */
export function setGracefulShutdown(value: boolean): void {

  gracefulShutdownInProgress = value;
}

/* We track pages that PrismCast creates to distinguish them from pages that might be opened by other means (manually by the user, by site popups, etc.). Only pages we
 * create should be subject to stale page cleanup. This prevents the cleanup from interfering with pages the user opened for debugging or pages created by
 * streaming sites for authentication flows.
 *
 * We use a WeakMap to associate Page objects with unique string IDs. The WeakMap allows garbage collection of Page objects when they're no longer referenced
 * elsewhere, while the ID strings provide stable identifiers for comparison and staleness tracking.
 */

// Counter for generating unique page IDs. Each managed page gets a unique ID when registered.
let managedPageIdCounter = 0;

// WeakMap from Page objects to their assigned unique IDs. Using a WeakMap allows the Page to be garbage collected when no longer referenced.
const pageToId = new WeakMap<Page, string>();

// Set of IDs for pages created by PrismCast. Pages are registered immediately after creation and unregistered during cleanup. Only pages with IDs in this set are
// candidates for stale page cleanup.
const managedPageIds = new Set<string>();

// Map from page ID to timestamp when a page was first observed as potentially stale (not associated with an active stream). Pages must remain in this state for
// the configured grace period before being closed. This prevents race conditions where pages are briefly untracked during initialization or cleanup transitions.
const potentiallyStalePages = new Map<string, number>();

/* Set of IDs for pages an operation owns for its whole duration while nothing the cleanup walk reads records that ownership. Stream setup is one such owner: it
 * writes the registry's page reference only once it completes, which on a slow tune is long enough for the walk to see the page as unowned and close it out from
 * under the setup driving it. A channel discovery walk is another: its page is never a stream page, so no registry entry will ever speak for it, and the walk's
 * own cleanup is what releases it. Membership here exempts the page from staleness for exactly the window its owner holds it. An entry leaves the set when
 * unregisterManagedPage releases the page as its owner finishes, when the cleanup walk sees the registry record the ownership the mark stood in for, and when
 * clearPageTracking wipes the collections at the end of a browser session.
 */
const inFlightPageIds = new Set<string>();

/* The pages that live in a window of their own rather than the shared one. A CDP window command acts on the window of the page whose session carries it, so
 * which page a command travels on decides which window moves...and a page in its own window would move that one. Membership here is what keeps such a page
 * out of every site that reaches for a convenient page to command the shared window through.
 *
 * Keyed on the page and never cleared. The fact is intrinsic to how the page was created rather than a phase of its life, an entry dies with the page that
 * holds it, and the picker below already declines a closed page - so unregistration, teardown order, and clearPageTracking have nothing to maintain here.
 */
const ownWindowPages = new WeakSet<Page>();

/**
 * Reports whether a page may carry a CDP command aimed at the shared window: it exists, it is still open, and it does not live in a window of its own. The
 * parameter accepts the absent case so every caller narrows through this one test rather than guarding beside it.
 * @param page - The candidate page, which may be absent.
 * @returns True when the page can carry a shared-window command.
 */
export function isCarrierPage(page: Nullable<Page>): page is Page {

  return !!page && !page.isClosed() && !ownWindowPages.has(page);
}

/**
 * Picks the page a shared-window command should travel on from the pages a browser has open, taking the first that qualifies.
 * @param pages - The candidate pages, in the order the browser reports them.
 * @returns The page to carry the command, or null when none of them qualifies.
 */
export function pickCarrierPage(pages: readonly Page[]): Nullable<Page> {

  return pages.find(isCarrierPage) ?? null;
}

/* Which window Chrome's own tables call the shared one, keyed on the browser that owns it. The value belongs to CDP's window table and is never comparable with
 * the capture extension's chrome.windows ids: those are two independent tables, and this is the only place the shared window's identity is recorded.
 *
 * Keyed on the browser and never cleared, for the reason the own-window mark above is: the identity is a fact about how that browser was created, an entry dies
 * with the browser holding it, and a relaunch records its own. An id that outlived its browser is exactly what would place a capture tab in the wrong window
 * while every check agreed, so there is deliberately no way to write one a live browser did not report.
 */
const sharedWindowIds = new WeakMap<Browser, number>();

/* The window each page was found in, cached for the page's life in the tabIds idiom of tabSelection.ts. The lookup below attaches a CDP session the shared helper
 * does not detach, so reading a candidate fresh on every tune would leave a session behind on each launch-era page every time. A page a user drags into another
 * window keeps the window it was first read in; a stale entry costs a carrier that has since moved out of the shared window, which the placement confirmation
 * catches and answers with the plain create rather than with a tab in the wrong place.
 */
const pageWindowIds = new WeakMap<Page, number>();

// The URL prefix the capture extension's own pages carry. The resolver reads it to prefer a plain-origin carrier, since an open evaluated on the extension's
// options page is territory nothing has measured.
const EXTENSION_PAGE_PREFIX = "chrome-extension://";

/**
 * Reads the window a page sits in, answering from the cache once the page has been read.
 * @param page - The page to locate.
 * @returns The window's CDP id, or undefined when the window cannot be read at all - a closed page, a target that yields no window, or a CDP failure.
 */
async function readPageWindow(page: Page): Promise<number | undefined> {

  const cached = pageWindowIds.get(page);

  if(cached !== undefined) {

    return cached;
  }

  const windowId = await withCDPSession(page, async (_session, id): Promise<number> => id);

  if(windowId !== undefined) {

    pageWindowIds.set(page, windowId);
  }

  return windowId;
}

/**
 * Records which window the shared one is, reading it from a page that sits in it.
 *
 * This is the one way an identity is written. The launch path calls it once the launch-era pages exist, so every browser this process publishes carries the
 * answer before anything asks for a capture tab.
 * @param browser - The browser whose shared window is being recorded.
 * @param page - A page that sits in that window.
 */
export async function noteSharedWindow(browser: Browser, page: Page): Promise<void> {

  const windowId = await readPageWindow(page);

  if(windowId === undefined) {

    LOG.debug("browser:lifecycle", "The shared browser window could not be identified, so capture tabs will open wherever Chrome places them.");

    return;
  }

  sharedWindowIds.set(browser, windowId);
}

/**
 * Picks the page an opener-anchored tab should be opened from: one the browser has open, living in the shared window rather than a window of its own, and
 * confirmed to sit in the window this process recorded.
 *
 * A plain-origin page is preferred over the capture extension's own options page, which is the carrier of last resort. The options page is recognized by the URL
 * prefix its own pages carry rather than through the library's resolver, whose wait for a missing extension would spend thirty seconds inside a caller's turn.
 * When no identity was ever recorded the confirmation has nothing to test against, and the first qualifying page is the answer: an anchor to some window of the
 * browser's is still better than letting Chrome choose one.
 * @param browser - The browser to resolve a carrier in.
 * @returns The page to evaluate the open on, or null when none of the browser's pages qualifies.
 */
export async function resolveSharedWindowCarrier(browser: Browser): Promise<Nullable<Page>> {

  const sharedWindowId = sharedWindowIds.get(browser);
  const candidates = (await browser.pages()).filter(isCarrierPage);

  for(const page of [ ...candidates.filter((candidate) => !candidate.url().startsWith(EXTENSION_PAGE_PREFIX)),
    ...candidates.filter((candidate) => candidate.url().startsWith(EXTENSION_PAGE_PREFIX)) ]) {

    if(sharedWindowId === undefined) {

      return page;
    }

    // eslint-disable-next-line no-await-in-loop -- A candidate is only asked for its window once the candidates ahead of it have failed to qualify.
    if((await readPageWindow(page)) === sharedWindowId) {

      return page;
    }
  }

  return null;
}

/**
 * Reports whether a page ended up in the shared window.
 *
 * Advisory when no identity was ever recorded: there is nothing to contradict the placement with, so the answer is true and the caller proceeds. A window that
 * cannot be READ is the opposite answer, though, and deliberately so - the shared lookup reports undefined for a closed page, an empty response, and a CDP
 * failure alike, and reading any of those as a confirmation would confirm a wrong-window tab in exactly the case this check exists for.
 * @param page - The page to test.
 * @returns True when the page sits in the recorded window, or when no window was ever recorded.
 */
export async function confirmSharedWindowPlacement(page: Page): Promise<boolean> {

  const sharedWindowId = sharedWindowIds.get(page.browser());

  if(sharedWindowId === undefined) {

    return true;
  }

  return (await readPageWindow(page)) === sharedWindowId;
}

// Login mode management. State and functions live in login.ts; re-exported here so existing consumers don't need import path changes. clearLoginState,
// isLoginModeActive, and setLoginDeps are imported above; the first two for internal use, setLoginDeps for one-time initialization below.
export { clearLoginState, isLoginModeActive };
export type { LoginStatus } from "./login.ts";
export { endLoginMode, getLoginPage, getLoginStatus, setLoginModeEndObserver, startLoginMode } from "./login.ts";

// Re-export the supervisor's acquire() rejection classes through the browser surface so the stream-setup layer can map them to a 503 back-off without reaching into
// the supervisor module directly. Each signals a transient "retry me" condition: BrowserUnavailableError while the relaunch governor is cooling,
// BrowserSupersededError when a launch was abandoned mid-flight by a readiness-loss, and BrowserCaptureImpairedError when the published browser can no longer start
// captures and is waiting for its streams to end. The purpose and impairment types cross the same boundary, so a caller declares what it needs the browser for and
// reads the mark without importing the supervisor.
export { BrowserCaptureImpairedError, BrowserSupersededError, BrowserUnavailableError } from "./browserSupervisor.ts";
export type { BrowserPurpose, CaptureImpairment } from "./browserSupervisor.ts";

/* The one window-presentation executor for the process lifetime. Its collaborators all live in this module: the registry predicate that says whether capture is
 * reading the compositor, login mode's own flag, the shutdown gate, the two CDP primitives, and a page resolver built on the supervisor.
 *
 * The resolver prefers the page its caller handed over, borrows any page the browser already has open when there is none, and creates a temporary page only when
 * the browser has nothing open at all. A temporary page is the only resolution that carries a dispose, so the executor releases what the resolver created and
 * leaves a borrowed page alone. Both the preference and the borrow go through the shared carrier rule, because a command travels on a page's own session and
 * so acts on that page's window: a page living in a window of its own would move that window instead of the shared one, whether it arrived as the caller's
 * preference or as the first page the browser happened to report.
 *
 * The instance stays private to this module and every caller reaches it through the exported function just below. That split is what makes the symbol safe to
 * reference at module-evaluation time: a function declaration is hoisted, so the setLoginDeps call further down - and any sibling module whose own body
 * evaluates while this module is still evaluating, precaching.ts among them - resolves it whatever the declaration order turns out to be. A bare exported const
 * would leave those readers in the temporal dead zone, which no placement inside this file can fix for a reader in another file.
 */
const windowVisibilitySync = createWindowVisibilitySync({

  hasActiveCaptureStreams,
  isLoginModeActive,
  isShuttingDown: isGracefulShutdown,
  minimize: minimizeWindow,

  resolvePage: async (preferred: Nullable<Page>): Promise<Nullable<{ dispose: Nullable<() => Promise<void>>; page: Page }>> => {

    const browser = supervisor.current();

    if(!browser?.connected) {

      return null;
    }

    if(isCarrierPage(preferred)) {

      return { dispose: null, page: preferred };
    }

    const borrowed = pickCarrierPage(await browser.pages());

    if(borrowed) {

      return { dispose: null, page: borrowed };
    }

    // A page that exists only to carry a CDP command never needs its tab selected.
    const temporary = await browser.newPage({ background: true });

    // Registered so stale page cleanup recognizes the page as ours for the moment it exists.
    registerManagedPage(temporary);

    return {

      dispose: async (): Promise<void> => {

        unregisterManagedPage(temporary);

        try {

          await temporary.close();
        } catch(error) {

          // The browser may already have taken the page with it. Nothing downstream depends on the close succeeding.
          LOG.debug("browser:lifecycle", "Could not close the temporary window-sync page: %s.", formatError(error));
        }
      },
      page: temporary
    };
  },
  unminimize: unminimizeWindow
});

/**
 * Brings the shared browser window into agreement with the visibility policy. This is the one entry point every layer uses - streaming, recovery, login,
 * precaching, and startup all call it and none of them decide window state themselves.
 * @param page - The page whose CDP session should carry the command, when the caller has one in hand. The executor falls back to any open page, or a temporary
 * one, when it is omitted or the page has closed.
 * @returns A promise that resolves once a pass that began at or after this call has completed.
 */
export async function syncWindowVisibility(page?: Page): Promise<void> {

  return windowVisibilitySync(page);
}

// Inject login's dependency set. This breaks the circular dependency (login needs getBrowserInstance and the window sync, index needs login functions) using the
// same setter/getter pattern as setChromeUserAgent in chromeFetch.ts. Both accessors are hoisted function declarations, so this call reads them at module
// evaluation time regardless of where they sit in the file. No clock is supplied, so login runs on the system clock the port defaults to.
setLoginDeps({ getBrowserInstance, syncWindowVisibility });

/**
 * Computes the current system status and emits it to SSE subscribers. Called when browser state changes significantly or when streams are added/removed.
 */
export async function emitCurrentSystemStatus(): Promise<void> {

  let pageCount = 0;

  // The published browser, or null when the supervisor is not in its ready state. Connectivity and page count derive from it - there is no separate browser
  // reference to consult.
  const browser = supervisor.current();

  try {

    if(browser?.connected) {

      const pages = await browser.pages();

      pageCount = pages.length;
    }
  } catch(_error) {

    // Ignore errors getting page count.
  }

  const memUsage = process.memoryUsage();

  const status: SystemStatus = {

    browser: {

      /* Read at compose time, beside the connectivity read below rather than as a snapshot taken before the page-count await. This function is called un-awaited
       * from many sites, so two calls can be in flight around the instant a mark lands; a pre-mark snapshot settling after the mark's own emit would broadcast
       * false over the cached true and the dedupe would honor it. Reading here makes every emit describe the state at the moment consumers receive it, so the
       * later of two racing emits is also the truer one.
       */
      captureImpaired: supervisor.captureImpairment() !== null,
      connected: !!browser && browser.connected,
      pageCount
    },
    memory: {

      heapUsed: memUsage.heapUsed,
      rss: memUsage.rss
    },
    streams: {

      active: getStreamCount(),
      limit: CONFIG.streaming.maxConcurrentStreams
    },
    uptime: process.uptime()
  };

  emitSystemStatusChanged(status);
}

/**
 * Registers a page as managed by PrismCast. This should be called immediately after creating a page via browser.newPage(). Registered pages are tracked for stale
 * page cleanup, while unregistered pages (manually opened, site popups, etc.) are left alone.
 *
 * Each registered page receives a unique ID that persists for the page's lifetime, read back through the Page reference by getManagedPageId. The staleness map,
 * the in-flight set and the stale page cleanup's decision core all work on this stable string key rather than on Page objects.
 * @param page - The Puppeteer Page to register.
 * @param options - Registration options. Set inFlight when an operation owns the page for its duration but nothing the stale page cleanup reads records that
 *   ownership - a stream setup that has not yet written its page into the registry, or a discovery walk whose page belongs to no stream at all - so the cleanup
 *   never closes a page out from under the operation driving it.
 */
export function registerManagedPage(page: Page, options: { inFlight?: boolean } = {}): void {

  // Generate a unique ID for this page.
  const pageId = "page-" + String(++managedPageIdCounter);

  // Associate the Page object with its ID.
  pageToId.set(page, pageId);

  // Track the ID as managed.
  managedPageIds.add(pageId);

  // Exempt the page from staleness for as long as the operation that owns it is in flight.
  if(options.inFlight) {

    inFlightPageIds.add(pageId);
  }
}

/**
 * Unregisters a page from PrismCast's management. This should be called when a page is being closed intentionally (during stream cleanup). Unregistering prevents the
 * stale page cleanup from racing with intentional page closure.
 * @param page - The Puppeteer Page to unregister.
 */
export function unregisterManagedPage(page: Page): void {

  const pageId = pageToId.get(page);

  if(pageId) {

    managedPageIds.delete(pageId);

    // Also remove from potentially stale tracking since we're intentionally closing it.
    potentiallyStalePages.delete(pageId);

    // Release any in-flight mark. The page is leaving our management, so the exemption it carried has nothing left to protect.
    inFlightPageIds.delete(pageId);

    // Note: We don't delete from pageToId because WeakMap handles cleanup automatically when the Page is garbage collected.
  }
}

/**
 * Gets the managed page ID for a page, if it exists.
 * @param page - The Puppeteer Page to look up.
 * @returns The page ID if the page is managed, undefined otherwise.
 */
function getManagedPageId(page: Page): string | undefined {

  return pageToId.get(page);
}

/**
 * Discards every page-tracking collection. The ids they hold are scoped to one browser session's pages, so they mean nothing once that session ends and would
 * otherwise carry into the next one - which matters most for the scheduled restart, where the process lives on across the swap. Every path that ends a PUBLISHED
 * browser session routes through here, which is why the collections are cleared in one place rather than at each of those paths. A launch that is superseded
 * before it is ever published does not, and needs no clear: it never held stream pages, and its readiness-gate probe pages are released at their own registration
 * sites. The WeakMap needs no clear - it releases its entries when the Page objects are collected.
 */
function clearPageTracking(): void {

  managedPageIds.clear();
  potentiallyStalePages.clear();
  inFlightPageIds.clear();
}

/**
 * Ensures the data directory exists, creating it if necessary. This should be called during application startup before any operations that depend on the data
 * directory (like browser launch or extension preparation).
 *
 * The data directory stores:
 * - Chrome profile data (cookies, local storage, session state)
 * - Extension files (when running as a packaged executable)
 */
export async function ensureDataDirectory(): Promise<void> {

  try {

    await fsPromises.mkdir(getDataDir(), { recursive: true });

    LOG.debug("browser:lifecycle", "Data directory ready: %s.", getDataDir());
  } catch(error) {

    LOG.error("Failed to create data directory %s: %s.", getDataDir(), formatError(error));

    throw error;
  }

  // Purge on-disk artifacts retired in earlier releases. The data directory is the right boundary for this work - it runs once per startup, after the directory
  // exists, before any subsequent step reads it. Future retirements add an entry to RETIRED_ARTIFACTS; neither purgeLegacyArtifacts nor this call site changes.
  await purgeLegacyArtifacts();
}

/* The retirement registry. Each entry documents one on-disk artifact that earlier PrismCast versions wrote and the current code no longer maintains, along with
 * the version that retired it and a short explanation of what replaced it. Adding a future retirement is one entry appended to the list - the loop in
 * purgeLegacyArtifacts picks it up automatically and the structured metadata becomes part of the codebase's history. The list is data, not logic: the "what"
 * and "why" of every retirement is captured here in one place rather than scattered across cleanup call sites.
 */
interface RetiredArtifact {

  // The filename inside the data directory (relative path, no leading slash).
  readonly filename: string;

  // The PrismCast version in which this artifact stopped being written. Informational; appears in the debug log on successful purge.
  readonly retiredIn: string;

  // One-sentence rationale for why this artifact is no longer needed. Informational; appears in the debug log on successful purge.
  readonly replacedBy: string;
}

const RETIRED_ARTIFACTS: readonly RetiredArtifact[] = [

  {

    filename: "chrome.pid",
    replacedBy: "OS process-table discovery in utils/processInspector",
    retiredIn: "1.10.3"
  }
];

/**
 * Removes every on-disk artifact in RETIRED_ARTIFACTS. Failures are non-fatal: a missing file is the steady-state expectation (fresh installs and any
 * post-first-run startup) and any other I/O error is logged but does not interrupt startup - the user's running configuration is not at risk from a leftover
 * artifact. The data-directory boundary in ensureDataDirectory is the natural caller: it runs once per startup, after the directory exists, before any
 * subsequent step reads it.
 */
async function purgeLegacyArtifacts(): Promise<void> {

  for(const artifact of RETIRED_ARTIFACTS) {

    const filePath = path.join(getDataDir(), artifact.filename);

    try {

      // eslint-disable-next-line no-await-in-loop
      await fsPromises.unlink(filePath);

      LOG.debug("browser:lifecycle", "Purged legacy artifact %s (retired in %s, replaced by %s).", filePath, artifact.retiredIn, artifact.replacedBy);
    } catch(error: unknown) {

      if((error as NodeJS.ErrnoException).code !== "ENOENT") {

        LOG.warn("Failed to remove legacy artifact %s: %s.", filePath, formatError(error));
      }
    }
  }
}

/* These functions handle the Chrome browser lifecycle: startup, cleanup, and instance management. The browser is a shared resource used by all streaming sessions,
 * so careful lifecycle management is essential for reliability. Key considerations:
 *
 * - Single browser instance: We use one Chrome process for all streams to minimize resource overhead. Each stream gets its own tab (page) within that browser.
 *
 * - Profile locking: Chrome locks its user data directory while running. If a previous instance crashed without releasing the lock, we must kill it before
 *   launching a new browser.
 *
 * - Crash recovery: The browser can crash or disconnect unexpectedly. When this happens, we clean up all active streams (they cannot continue without a browser)
 *   and reset state so the next stream request will launch a fresh browser.
 *
 * - Extension initialization: The puppeteer-stream extension needs time after browser launch to inject its recording APIs. We wait for this initialization before
 *   attempting to capture streams.
 */

/**
 * Synchronous sleep using Atomics.wait(). This is a cross-platform replacement for execSync("sleep N") that works on all platforms without shelling out.
 * Required because killStaleChrome() runs in the synchronous process.on("exit") handler where async operations are not available.
 * @param ms - Duration to sleep in milliseconds.
 */
function syncSleep(ms: number): void {

  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * Reports whether a process's command line runs Chrome on the given profile directory: the first stage of the kill filter and of the holder test, so they agree
 * on which processes are on the profile.
 * @param commandLine - The process's command line as the OS reports it.
 * @param profileDir - The Chrome user-data-dir to match against.
 * @returns True when the command line carries the profile flag for exactly that directory.
 */
function usesProfile(commandLine: string, profileDir: string): boolean {

  const target = "--user-data-dir=" + profileDir;

  // Chrome puppeteer launches with --user-data-dir=<path> (equals form, no quotes). We also verify the character following the path is whitespace or
  // end-of-string, otherwise "/x/y" would match "--user-data-dir=/x/yz".
  const idx = commandLine.indexOf(target);

  if(idx === -1) {

    return false;
  }

  const after = commandLine.charAt(idx + target.length);

  return (after === "") || (after === " ") || (after === "\t");
}

/**
 * Reports whether a Chrome process on our profile is ours to terminate: the ownership stage of the kill filter, and the stage the holder test reads to decide
 * which Chrome the sweep spares, so the sweep never keeps the files of a Chrome it ends nor removes those of one it spares. The parent-child relationship in the
 * OS process table is the ownership proof, so no in-memory flag is consulted.
 * @param entry - The process-table row of a Chrome process on our profile.
 * @param ownPid - The current process's PID.
 * @param isProcessAlive - Predicate that returns true when the given PID is currently a live process.
 * @returns True when we spawned the process or its parent is no longer alive.
 */
function isOwnedChrome(entry: ProcessInfo, ownPid: number, isProcessAlive: (pid: number) => boolean): boolean {

  // Ours if we spawned it (ppid is us) or if its parent is no longer alive (orphaned from a previous PrismCast that died). A live unrelated parent owns it, not
  // us.
  return (entry.ppid === ownPid) || !isProcessAlive(entry.ppid);
}

/**
 * Identifies Chrome processes that this PrismCast instance is responsible for terminating. The filter has two stages: command-line discovery (anything using
 * our profile directory) followed by ownership verification (we spawned it, or its parent is no longer alive). The ownership stage is structural - we do not
 * rely on an in-memory flag; the parent-child relationship in the OS process table IS the ownership proof, which means this function is safe to call from any
 * code path, including from a rejected-duplicate startup's exit handler.
 *
 * Why ownership matters. A duplicate PrismCast that the instance guard rejects must NOT signal Chrome that belongs to the legitimate holder. Process-table
 * discovery lets the OS itself tell us who owns what: a Chrome whose ppid is a live unrelated PID belongs to that parent, not to us.
 * @param processes - The current process table snapshot.
 * @param profileDir - The Chrome user-data-dir to match against.
 * @param ownPid - The current process's PID (anything whose parent is us is ours to kill).
 * @param isProcessAlive - Predicate that returns true when the given PID is currently a live process. Injected so tests do not depend on real PIDs.
 * @returns The PIDs to terminate, in process-table order.
 */
export function findChromeProcessesUsingProfile(processes: readonly ProcessInfo[], profileDir: string, ownPid: number,
  isProcessAlive: (pid: number) => boolean): number[] {

  return processes.filter((p): boolean => usesProfile(p.commandLine, profileDir) && isOwnedChrome(p, ownPid, isProcessAlive)).map((p) => p.pid);
}

/**
 * Finds a live Chrome outside this process that holds our profile directory. A Chrome holds the profile when it roots a tree of processes on the profile and the
 * kill filter's ownership stage spares it, its parent alive and not this process. The test reads the root because Chrome's helpers carry the profile flag with
 * the Chrome main as their parent, so a helper's parent, the main, is a live process that is not this one, and every helper of this process's own Chrome would
 * otherwise read as a holder. It reads the ownership stage rather than a liveness test of its own so it stays in step with the kill filter: the sweep never keeps
 * the files of a Chrome it ends nor removes those of one it spares. A helper whose parent lacks the profile flag reads as a root, so that error keeps the lock files
 * rather than removing them, and the next sweep with no Chrome alive removes them.
 * @param processes - The current process table snapshot: the sweep's scan from before its kill loop, or the teardown's scan once its Chrome has exited.
 * @param profileDir - The Chrome user-data-dir to match against.
 * @param ownPid - The current process's PID (a Chrome whose parent is us is ours, never a holder).
 * @param isProcessAlive - Predicate that returns true when the given PID is currently a live process. Injected so tests do not depend on real PIDs.
 * @returns The PID of the first holder in process-table order, or null when no live Chrome outside this process holds the profile.
 */
export function findProfileHolder(processes: readonly ProcessInfo[], profileDir: string, ownPid: number,
  isProcessAlive: (pid: number) => boolean): Nullable<number> {

  const onProfile = processes.filter((p): boolean => usesProfile(p.commandLine, profileDir));
  const profilePids = new Set(onProfile.map((p) => p.pid));

  return onProfile.find((p): boolean => !profilePids.has(p.ppid) && !isOwnedChrome(p, ownPid, isProcessAlive))?.pid ?? null;
}

/**
 * Ensures a clean slate for browser launch by terminating any stale Chrome processes and removing orphaned profile lock files. Chrome locks its profile
 * directory while running; if a previous instance crashed without releasing the lock, we cannot launch a new browser with the same profile. Discovery is done
 * via the OS process table (utils/processInspector) and filtered to Chrome processes using our profile directory whose ownership belongs to us - either because
 * we spawned them (ppid === process.pid) or because their parent is no longer alive (orphaned from a previous instance).
 *
 * The termination strategy escalates from SIGTERM to SIGKILL. SIGTERM is sent first, giving Chrome up to 5 seconds to flush its profile databases (LevelDB,
 * extension state, session storage) and exit cleanly. If Chrome does not exit, SIGKILL is sent as a fallback. This escalation is critical when called from the
 * process exit handler: Chrome may be running normally (e.g., after a capture probe timeout) and an immediate SIGKILL would corrupt its profile databases,
 * poisoning the Docker volume for subsequent container restarts.
 *
 * The ownership filter and the holder test together make this function safe to call from any context, including a rejected-duplicate startup's exit handler: a
 * duplicate that never spawned Chrome finds nothing matching its ownership criteria, signals nothing, and leaves the profile's lock files to the live Chrome
 * that holds them.
 *
 * Called at startup before launching the browser and from the process exit handler as a crash recovery fallback. Safe to call when no stale processes or files
 * exist - the discovery and lock-file cleanup are both no-ops in the empty case.
 */
export function killStaleChrome(): void {

  const profileDir = getChromeDataDir(CONFIG);
  const POLL_INTERVAL_MS = 200;

  // The kill filter and the lock-file removal's holder test read the one scan taken before the kill loop, because a scan after it would read the helpers of each
  // Chrome the loop ends, adopted by a live process once their main exits, as live holders.
  const processes = listProcesses();
  const pidsToKill = findChromeProcessesUsingProfile(processes, profileDir, process.pid, isProcessRunning);

  for(const pid of pidsToKill) {

    try {

      // Send SIGTERM first to give Chrome a chance to flush its profile databases (LevelDB, extension state, session storage) before exiting. This is critical
      // when called from the process exit handler - Chrome may be running normally (e.g., after a capture probe timeout) and SIGKILL would corrupt its profile
      // databases, poisoning the Docker volume for subsequent restarts.
      process.kill(pid, "SIGTERM");

      LOG.debug("browser:lifecycle", "Sent SIGTERM to Chrome process %d.", pid);

      if(!waitForChromeExit(pid, TERM_WAIT_MS, POLL_INTERVAL_MS)) {

        // SIGTERM didn't work. Escalate to SIGKILL. Orphaned Chrome processes (from a crashed parent or previous container) may not respond to SIGTERM.
        LOG.debug("browser:lifecycle", "Chrome did not exit after SIGTERM. Escalating to SIGKILL.");

        try {

          process.kill(pid, "SIGKILL");
        } catch(_error) {

          // ESRCH - Chrome exited between the poll check and the kill call.
        }

        if(!waitForChromeExit(pid, KILL_WAIT_MS, POLL_INTERVAL_MS)) {

          LOG.warn("Chrome process %d did not exit after %dms of signal escalation. Proceeding anyway.", pid, TERM_WAIT_MS + KILL_WAIT_MS);
        }
      }
    } catch(error: unknown) {

      // ESRCH means the process exited between the process-table scan above and the signal, which is a benign race.
      if((error as NodeJS.ErrnoException).code !== "ESRCH") {

        LOG.warn("Failed to signal Chrome process %d: %s.", pid, formatError(error));
      }
    }
  }

  cleanStaleProfileFiles(processes, profileDir);
}

/**
 * Polls until the Chrome process with the given PID has exited, or the timeout expires. Uses process.kill(pid, 0) to check process existence - throws ESRCH
 * when the process is gone. The wait runs inside the synchronous exit handler, so it reads the port for its instants and sleeps synchronously between them
 * using Atomics.wait() for cross-platform compatibility.
 * @param pid - The Chrome process ID to wait for.
 * @param timeoutMs - Maximum time to wait in milliseconds.
 * @param pollIntervalMs - Time between existence checks in milliseconds.
 * @returns True if the process exited within the timeout, false otherwise.
 */
function waitForChromeExit(pid: number, timeoutMs: number, pollIntervalMs: number): boolean {

  const deadline = systemClock.now() + timeoutMs;

  while(systemClock.now() < deadline) {

    if(!isProcessRunning(pid)) {

      return true;
    }

    syncSleep(pollIntervalMs);
  }

  return !isProcessRunning(pid);
}

/**
 * Removes the profile's lock files and the DevTools port file unless a live Chrome outside this process holds the profile. Chrome writes these while running and
 * removes them on clean shutdown, but an unclean exit (container kill, SIGKILL, crash) leaves them behind. Stale lock files prevent Chrome from acquiring the
 * profile, and a stale DevToolsActivePort can confuse the Puppeteer connection. This is the one removal of those files, so each caller, the stale-Chrome sweep and
 * the teardown primitive, reaches them through the holder test, and none removes them from under a live holder.
 * @param processes - The process table the holder test reads: the sweep's scan from before its kill loop, or the teardown's scan once its Chrome has exited.
 * @param profileDir - The Chrome user data directory path.
 */
export function cleanStaleProfileFiles(processes: readonly ProcessInfo[], profileDir: string): void {

  const holder = findProfileHolder(processes, profileDir, process.pid, isProcessRunning);

  /* The lock and port files belong to whichever Chrome holds the profile, so they are stale only when no live Chrome outside this process holds it. Removing
   * them from under a live holder, such as the instance a refused second start found, would let the next Chrome launched on the profile start beside the
   * running one instead of meeting its singleton.
   */
  if(holder !== null) {

    LOG.debug("browser:lifecycle", "Kept the profile lock files of live Chrome process %d.", holder);

    return;
  }

  // Chrome's profile lock mechanism uses three symlinks: SingletonLock (hostname-PID pair), SingletonCookie (numeric verification token), and SingletonSocket
  // (path to the IPC socket). All three must be removed for Chrome to acquire a fresh lock. DevToolsActivePort contains the debugging port from the previous
  // session and is irrelevant when launching a new browser instance.
  const staleFiles = [ "DevToolsActivePort", "SingletonCookie", "SingletonLock", "SingletonSocket" ];

  for(const file of staleFiles) {

    const filePath = path.join(profileDir, file);

    try {

      fs.unlinkSync(filePath);

      LOG.debug("browser:lifecycle", "Removed stale profile file: %s.", file);
    } catch(error: unknown) {

      // ENOENT means the file doesn't exist, which is the expected case after a clean shutdown. Any other error (permissions, filesystem issues) is worth
      // logging as a warning since it could prevent Chrome from starting.
      if((error as NodeJS.ErrnoException).code !== "ENOENT") {

        LOG.warn("Failed to remove stale profile file %s: %s.", file, formatError(error));
      }
    }
  }
}

/**
 * Returns the object stored under a key, replacing an absent or non-object value with a fresh object so the caller always has somewhere to write. Chrome's
 * Preferences file nests its settings several levels deep and a young profile has not written most of them yet, so a seed has to build the intermediate levels
 * as it descends. A value that is present but not an object cannot be merged into and cannot appear in a file Chrome itself wrote, so we replace it rather than
 * abandon the seed.
 * @param parent - The object holding the key.
 * @param key - The key whose object value is wanted.
 * @returns The object stored under the key, created if it was absent or unusable.
 */
function ensureObjectAt(parent: Record<string, unknown>, key: string): Record<string, unknown> {

  const existing = parent[key];

  if((typeof existing === "object") && (existing !== null) && !Array.isArray(existing)) {

    return existing as Record<string, unknown>;
  }

  const created: Record<string, unknown> = {};

  parent[key] = created;

  return created;
}

/**
 * Seeds the extension developer-mode preference into the Chrome profile so the capture extension loads. Chrome loads an unpacked extension only when the profile
 * has extension developer mode enabled, and PrismCast's capture extension is loaded unpacked...without the flag the extension never registers and the capture
 * probe fails the launch gate. We write the preference into the profile ourselves rather than asking the user to find the toggle on Chrome's extensions page, and
 * we merge it into whatever the file already holds so every other profile setting survives. Chrome merges a Preferences file it finds on startup, so a file
 * carrying only this flag is a valid starting point for a profile that has never been launched.
 *
 * Nothing here is allowed to break a launch. A profile we cannot read or cannot write earns one warning and the launch proceeds without the seed: Chrome rewrites
 * a Preferences file it cannot parse, so the next launch seeds the replacement.
 *
 * @param profileDir - The Chrome user data directory holding the profile.
 */
export function seedProfilePreferences(profileDir: string): void {

  const profileDefaultDir = path.join(profileDir, "Default");
  const preferencesPath = path.join(profileDefaultDir, "Preferences");

  let preferences: Record<string, unknown> = {};

  try {

    const parsed: unknown = JSON.parse(fs.readFileSync(preferencesPath, "utf8"));

    // A Preferences file that parses to anything other than an object is one we have no way to merge into, so we leave it alone and let Chrome regenerate it.
    if((typeof parsed !== "object") || (parsed === null) || Array.isArray(parsed)) {

      LOG.warn("The Chrome profile preferences at %s are not a JSON object, so extension developer mode was not seeded.", preferencesPath);

      return;
    }

    preferences = parsed as Record<string, unknown>;
  } catch(error: unknown) {

    // A missing file is the fresh-profile case and seeds a new file below. Any other failure - unparseable JSON, a permissions problem, a file held open by
    // another process - earns one warning and no write.
    if((error as NodeJS.ErrnoException).code !== "ENOENT") {

      LOG.warn("Unable to load the Chrome profile preferences at %s, so extension developer mode was not seeded: %s.", preferencesPath, formatError(error));

      return;
    }
  }

  const extensionPreferences = ensureObjectAt(preferences, "extensions");
  const uiPreferences = ensureObjectAt(extensionPreferences, "ui");

  // The flag is already set, so there is nothing to write. Rewriting the file on every launch would churn a file Chrome reads at startup for no gain.
  if(uiPreferences["developer_mode"] === true) {

    return;
  }

  uiPreferences["developer_mode"] = true;

  try {

    // The Default directory does not exist on a profile Chrome has never launched, so the seed creates it before writing the file Chrome merges on its first run.
    fs.mkdirSync(profileDefaultDir, { recursive: true });
    fs.writeFileSync(preferencesPath, JSON.stringify(preferences) + "\n", "utf8");

    LOG.debug("browser:lifecycle", "Seeded extension developer mode into the Chrome profile preferences at %s.", preferencesPath);
  } catch(error: unknown) {

    LOG.warn("Unable to write the Chrome profile preferences at %s, so extension developer mode was not seeded: %s.", preferencesPath, formatError(error));
  }
}

/**
 * Locates the Google Chrome executable on the system. The CHROME_BIN environment variable takes precedence, allowing operators to specify a non-standard
 * installation. Otherwise, we search common installation paths across macOS, Linux, and Windows.
 *
 * @returns Path to the Chrome executable.
 * @throws If no Chrome installation is found.
 */
export function getExecutablePath(): string {

  // Environment variable override takes precedence. This is useful for containerized deployments or non-standard installations.
  if(CONFIG.browser.executablePath) {

    return CONFIG.browser.executablePath;
  }

  // Check standard Google Chrome installation paths across platforms.
  const paths = [

    // macOS. Applications are typically in /Applications with .app bundles containing the actual executable.
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",

    // Linux. Chrome packages install to /usr/bin with naming conventions that vary by distribution.
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",

    // Windows. Both 64-bit (Program Files) and 32-bit (Program Files (x86)) installations are checked.
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
  ];

  // Return the first path that exists on the filesystem.
  const found = paths.find(fs.existsSync);

  if(found) {

    return found;
  }

  throw new Error("No Chrome installation found. Set CHROME_BIN environment variable.");
}

/* Chrome extension ID for puppeteer-stream's bundled capture extension. This is the deterministic ID Chrome assigns based on the extension's public key, mirrored
 * from their dist at node_modules/puppeteer-stream/dist/PuppeteerStream.js (the extensionId constant). We pass it below via --allowlisted-extension-id as a
 * defensive duplicate of the flag puppeteer-stream's own launch() call already re-adds; see the "--allowlisted-extension-id" entry in buildLaunchOptions for
 * the full rationale.
 */
const PUPPETEER_STREAM_EXTENSION_ID = "jjndjgheafjngoipoacpjgeicjeomjli";

// Chrome's documented value for disabling the device-scale-factor part of a metrics override, leaving the display's own density in effect. Naming it keeps the
// sentinel from reading as a literal density of zero.
const NATIVE_DENSITY = 0;

/**
 * Assembles the configuration options for launching Chrome with Puppeteer. These options are critical for reliable streaming:
 *
 * - Chrome flags configure the browser for unattended video playback without user interaction
 * - Ignored default args prevent Puppeteer from disabling features we need (extensions, audio, component updates)
 * - A persistent user data directory retains cookies and login state across restarts
 * - Pipe mode provides a faster, more reliable connection than WebSocket
 * @returns Puppeteer launch options.
 */
export function buildLaunchOptions(): LaunchOptions & { defaultViewport: null } {

  return {

    /* Chrome command-line arguments. Each flag serves a specific purpose for reliable streaming:
     *
     * --allow-running-insecure-content: Some streaming sites serve mixed HTTP/HTTPS content. Without this flag, the browser blocks HTTP resources on HTTPS
     *   pages, which can break video players that load some assets over HTTP.
     *
     * --allowlisted-extension-id=<extension-id>: Restores Chrome's global allowlist for puppeteer-stream's capture extension. puppeteer-stream's own launch()
     *   call already re-adds this exact flag (see the addToArgs call for extensionId in node_modules/puppeteer-stream/dist/PuppeteerStream.js), so this entry
     *   is a defensive duplicate: if a future puppeteer-stream release drops the flag again in favor of granting activeTab via a synthetic keystroke,
     *   CDP-synthesized keystrokes do not satisfy chrome.commands under automation (the renderer sees the event but the browser-process accelerator dispatcher
     *   does not), so capture would be denied at the API level (see github.com/Flam3rboy/puppeteer-stream/issues/206) without this fallback in place. Confirm
     *   whether this duplicate is still warranted whenever the locked puppeteer-stream version changes.
     *
     * --autoplay-policy=no-user-gesture-required: Allows video and audio to play without requiring a user click first. Essential for automated streaming
     *   since we cannot simulate genuine user interaction for autoplay policy purposes.
     *
     * --disable-background-media-suspend: Prevents Chrome from pausing media when the tab is backgrounded or the window is minimized. Critical since we
     *   minimize the browser to reduce GPU usage but still need media to play.
     *
     * --disable-background-networking: Reduces unnecessary network activity from background Chrome services (Safe Browsing updates, etc). This reduces
     *   resource usage and potential interference with stream capture.
     *
     * --disable-background-timer-throttling: Prevents Chrome from throttling JavaScript timers in background tabs. Video players often use timers for
     *   playback state management, and throttling can cause stuttering or stalls.
     *
     * --disable-backgrounding-occluded-windows: Prevents Chrome from reducing activity when the window is covered by other windows. Similar to the timer
     *   throttling issue, this ensures consistent playback even when the browser isn't visible.
     *
     * --disable-blink-features=AutomationControlled: Hides the navigator.webdriver property that indicates automated control. Some sites detect and block
     *   automated browsers; this flag helps avoid that detection.
     *
     * --disable-notifications: Prevents notification permission prompts and popups that could interfere with video capture or require user interaction.
     *
     * --hide-crash-restore-bubble: Suppresses the "Chrome didn't shut down correctly" dialog that appears after a crash. This prevents the dialog from
     *   blocking the viewport during capture.
     *
     * --hide-scrollbars: Removes scrollbars from the viewport to ensure the video fills the entire capture area without UI chrome.
     *
     * --no-first-run: Skips the first-run experience dialogs and setup wizard that would require user interaction.
     */
    args: [

      "--allow-running-insecure-content",
      "--allowlisted-extension-id=" + PUPPETEER_STREAM_EXTENSION_ID,
      "--autoplay-policy=no-user-gesture-required",
      "--disable-background-media-suspend",
      "--disable-background-networking",
      "--disable-background-timer-throttling",
      "--disable-backgrounding-occluded-windows",
      "--disable-blink-features=AutomationControlled",
      "--disable-notifications",
      "--hide-crash-restore-bubble",
      "--hide-scrollbars",
      "--no-first-run"
    ],

    /* No launch-wide viewport, stated as an explicit null because an omitted option is not the same thing: Puppeteer reads an absent defaultViewport as its own
     * 800x600 default and applies that override to every page it creates, while a null leaves each page carrying no override at all. Two consequences follow, and
     * both are the point. A page renders at whatever surface PrismCast declares on it and nothing else - the capture surface on a capture page, the preset-sized
     * layout on the discovery and capture-probe pages - so no page inherits an emulation it never asked for. And puppeteer-stream derives Chrome's --window-size
     * and --ozone-override-screen-size flags from a sized default, where a --window-size flag stops Chrome restoring the placement it persisted in the profile, so
     * with no default the window's size and placement are Chrome's and the user's.
     *
     * The return type names the null so the key cannot be dropped as dead configuration without a compile error, since its absence is silently a viewport rather
     * than none.
     */
    defaultViewport: null,

    // Path to the Chrome executable, either from environment variable or autodetected.
    executablePath: getExecutablePath(),

    /* PrismCast owns process signals, so the launcher's own listeners are off. Left on, the puppeteer launcher installs handlers for SIGHUP, SIGINT, and SIGTERM
     * on the node process and, on SIGTERM or SIGHUP, ends Chrome's entire process group with SIGKILL - and on SIGINT it kills the group and then exits the process
     * outright. Each of them races the shutdown handler app.ts installs for the same signal and preempts closeBrowserInstance's SIGTERM ladder below, so Chrome
     * gets no shutdown at all and forfeits whatever it had pending: a window placement inside its save debounce, its session state. With the listeners off, every
     * one of these signals reaches PrismCast's own handlers, the graceful path runs, and Chrome exits through the ladder with its pending writes committed.
     */
    handleSIGHUP: false,
    handleSIGINT: false,
    handleSIGTERM: false,

    // Run Chrome in headed (visible) mode, not headless. The puppeteer-stream extension captures the compositor's output for a real window, which a headless
    // browser does not have. The window is on screen while any capture stream runs or a login session is active, and minimized otherwise.
    headless: false,

    /* Prevent Puppeteer from adding certain default arguments that would interfere with streaming:
     *
     * --disable-component-extensions-with-background-pages: We need extension background pages for puppeteer-stream to function.
     *
     * --disable-component-update: We want component updates for codec support and security patches.
     *
     * --disable-default-apps: Default apps don't interfere, but we keep them for consistency with normal Chrome behavior.
     *
     * --disable-extensions: We absolutely need extensions enabled for puppeteer-stream to work. This is the most critical override.
     *
     * --enable-automation: This sets navigator.webdriver=true, which some sites use to detect and block automated browsers. We disable this detection by
     *   not setting this flag (and using --disable-blink-features=AutomationControlled above).
     *
     * --enable-blink-features=IdleDetection: Idle detection can interfere with background playback by triggering "user idle" events.
     *
     * --mute-audio: We need audio capture, so audio must not be muted. The puppeteer-stream extension captures both video and audio.
     */
    ignoreDefaultArgs: [

      "--disable-component-extensions-with-background-pages",
      "--disable-component-update",
      "--disable-default-apps",
      "--disable-extensions",
      "--enable-automation",
      "--enable-blink-features=IdleDetection",
      "--mute-audio"
    ],

    // Use pipe mode for browser communication instead of WebSocket. Pipe mode is faster and more reliable, especially under load. It runs the DevTools Protocol
    // over a dedicated pair of pipes (file descriptors 3 and 4) rather than a network socket.
    pipe: true,

    // Persistent user data directory for Chrome profile. This directory stores cookies, local storage, and other session data. By persisting this across
    // restarts, sites remember login state and don't require re-authentication.
    userDataDir: getChromeDataDir(CONFIG)
  };
}

/**
 * Declares a device-metrics override on a page: the configured quality preset's dimensions at the density the caller names. Every surface this module declares is
 * built here, so the preset reaches a page's emulation through exactly one read and one command.
 * @param page - The page to declare the surface on.
 * @param deviceScaleFactor - The pixel density to declare. NATIVE_DENSITY leaves the display's own density in effect.
 * @returns The dimensions declared on the page.
 */
async function declareSurface(page: Page, deviceScaleFactor: number): Promise<{ height: number; width: number }> {

  const viewport = getPresetViewport(CONFIG);

  await page.setViewport({ deviceScaleFactor, height: viewport.height, width: viewport.width });

  return { height: viewport.height, width: viewport.width };
}

/**
 * Emulates the capture surface on a page that is about to be captured: the configured quality preset's dimensions, declared at the pixel density the display
 * actually has. Chrome composes an active tab's capture from the window presentation unless the page's device-metrics override declares a density explicitly
 * and that density matches the display's real one, so the density is read from the page itself - a page carrying no declared density reports the display's own -
 * and declared back before capture acquires the page. The declared dimensions are returned so the caller holds its capture constraints to the surface that was
 * emulated rather than reading the preset a second time.
 * @param page - The page to emulate the capture surface on.
 * @returns The dimensions declared on the page.
 */
export async function emulateCaptureSurface(page: Page): Promise<{ height: number; width: number }> {

  const reportedDensity = await evaluateWithAbort(page, (): number => window.devicePixelRatio);

  // A reading that is not a positive finite number is no measurement at all, and the compositor still needs an explicit density...1 is the density of an
  // unscaled display, which is the safest thing to declare when the page cannot say.
  const densityIsUsable = Number.isFinite(reportedDensity) && (reportedDensity > 0);
  const deviceScaleFactor = densityIsUsable ? reportedDensity : 1;

  if(!densityIsUsable) {

    LOG.warn("The capture page reported a pixel density of %s. Emulating the capture surface at a density of %d instead.", reportedDensity, deviceScaleFactor);
  }

  const surface = await declareSurface(page, deviceScaleFactor);

  LOG.debug("browser:lifecycle", "Emulated the capture surface at %dx%d with a pixel density of %s.", surface.width, surface.height, deviceScaleFactor);

  return surface;
}

/**
 * Emulates the layout surface on a page that is laid out but never captured: the configured quality preset's dimensions at the display's own density. The guide
 * walks were written against that layout and the launch-gate capture probe acquires against it, so declaring it is what keeps both on the surface they were
 * validated on. The density stays the display's, because nothing reads this page's pixels.
 * @param page - The page to emulate the layout surface on.
 * @returns The dimensions declared on the page.
 */
export async function emulateLayoutSurface(page: Page): Promise<{ height: number; width: number }> {

  return declareSurface(page, NATIVE_DENSITY);
}

/**
 * Re-issues a capture page's own standing device-metrics override. Chrome composes the capture of a selected tab from the window's fitted presentation rather than
 * from the emulated surface, and re-sending the page's standing override is what moves the composition back to the emulated surface, while a capture already
 * composing that surface is left exactly as it was. Callable at any time, from anywhere, at any frequency, because the values sent are the ones Puppeteer has
 * already declared on this page - nothing about the page's emulation changes.
 *
 * The re-issue goes through the page's own emulation session, the one Puppeteer keeps on the page's primary target, because the override is only as durable as
 * the session that holds it and Chrome restores the window's view size when a session that declared one detaches.
 * @param page - The page to re-affirm. A page carrying no explicitly declared density is left alone.
 * @throws Whatever the viewport setter rejects with. Each trigger site decides whether that matters to it.
 */
export async function reaffirmCaptureSurface(page: Page): Promise<void> {

  /* An explicitly declared, positive density is precisely the mark of a capture page, which is what makes this function safe to fire at any page from any trigger.
   * A page PrismCast has not emulated carries no viewport at all and falls out on the first test; a page emulated for layout declares the display's own density
   * through Chrome's disable value of 0, which the positive test excludes.
   */
  const viewport = page.viewport();

  if(!viewport) {

    return;
  }

  const deviceScaleFactor = viewport.deviceScaleFactor;

  if((typeof deviceScaleFactor !== "number") || !(deviceScaleFactor > 0)) {

    return;
  }

  await page.setViewport(viewport);
}

/**
 * Derives the creation bounds for a window that should open exactly where an existing window rests. The four numbers always travel, so the new window lands on
 * the same frame. The state travels only when it is "maximized", because that is the one state Chrome saves alongside the frame and a maximized window mirrored
 * as a normal one would drop it. Every other state is left off deliberately: a fullscreen window's frame is its whole screen and a normal window at that frame
 * is the right stand-in for it, and a minimized window reports the frame it will return to, which is a placement worth inheriting without the minimization.
 * @param placement - The placement of the window to mirror.
 * @returns The bounds to create the new window with.
 */
export function mirrorPlacement(placement: WindowPlacement): { height: number; left: number; top: number; width: number; windowState?: "maximized" } {

  const { height, left, top, width } = placement;

  return (placement.windowState === "maximized") ? { height, left, top, width, windowState: "maximized" } : { height, left, top, width };
}

/**
 * Opens a page for a channel guide discovery walk in a browser window of its own, and marks it as belonging to that window.
 *
 * A document renders only while Chrome presents it: the active tab of a window the desktop is showing, or a page Chrome counts as captured. The shared window
 * rests minimized whenever nothing is capturing and no sign-in holds it on screen, with its selected tab belonging to the user. A guide walk needs its page to
 * render - an observer-driven channel rail fills its tiles from rendering updates, a virtualized grid re-renders as the walk scrolls it, and every wait the walk
 * makes polls on the page's animation frames - yet it is never captured, so it has no claim on the shared window's presentation and no business moving the
 * user's selection. A window of its own resolves the tab half: the page is that window's active tab from the moment it exists. The window is created in the
 * background, so Chrome shows it inactive and moves no focus (measured 2026-08-31), and such a window's document is one Chrome does not present on its own: it
 * reports itself hidden and unfocused and delivers no animation frame beyond a document load's first ones (measured 2026-09-13). Focus emulation resolves the
 * presentation half, for the page's whole life - Puppeteer's own emulateFocusedPage carries the mechanism, on the page's own session.
 *
 * The window opens at the shared window's own placement, so the window placement Chrome persists for the profile never changes - readWindowPlacement carries
 * the reasoning. The caller declares the layout surface on the page and owns its registration, and closing the page closes the window with it.
 * @param browser - The browser to open the window in.
 * @returns The page, as the active tab of its own window.
 */
export async function createDiscoveryPage(browser: Browser): Promise<Page> {

  const carrier = pickCarrierPage(await browser.pages());
  const placement = carrier ? await readWindowPlacement(carrier) : null;

  // The mark is set with nothing awaited between the creation and it, so no page read can observe this page as a candidate to carry a shared-window command.
  const page = await browser.newPage({ background: true, type: "window", windowBounds: placement ? mirrorPlacement(placement) : undefined });

  ownWindowPages.add(page);

  /* The page is presented from before its first load, so every wait a walk makes - a visible selector, a condition polled from inside the page - runs as it
   * would in a window the desktop is showing. Chrome counts a focus-emulated page as captured, which is what presents its document, and Puppeteer keeps the
   * state on the page's own session for as long as the page lives (measured 2026-09-13). The mark above stays first: nothing is awaited between the creation
   * and it. A page that refuses the emulation is one no walk can use, and nothing else holds it yet - the caller never receives it and the managed-page sweep
   * never sees it - so the creator closes it, which closes the window with it, before the failure propagates.
   */
  try {

    await page.emulateFocusedPage(true);
  } catch(error) {

    try {

      await page.close();
    } catch {

      // The page is already gone, which is the state the close was asking for.
    }

    throw error;
  }

  LOG.debug("browser:lifecycle", "Opened the discovery page in a window of its own, %s.",
    placement ? "mirroring the shared window's placement" : "with no shared window to read a placement from");

  return page;
}

/* The follow-up re-issue schedule a tab activation runs, expressed as offsets in milliseconds from the activation's own invocation rather than as gaps between
 * shots. Chrome switches an activated tab's capture to the window's fitted presentation only after the focus event has fired, so the immediate re-issue that
 * event triggers lands too early to stick (measured 2026-08-30: a selected capture tab's recording stayed fitted for upwards of forty seconds, until the
 * monitor's periodic re-affirmation healed it completely with the tab still selected). These offsets bracket the switch so one shot lands just past it, the last
 * rung allowing for platforms slower than the one measured. Every shot beyond the one that sticks is a no-op by reaffirmCaptureSurface's own contract, and the
 * monitor's periodic re-affirmation remains the backstop bounding anything the whole ladder misses.
 */
const ACTIVATION_REAFFIRM_LADDER_MS: readonly number[] = [ 250, 750, 1500, 3000 ];

/* The heal enrolled for each capture page, which is the trigger the selection primitive's activation report fires. Keying on the page rather than on a tab id is
 * what makes the enrollment expire on its own: an entry lives exactly as long as the page it heals. The callback held here is the very instance that page's focus
 * binding invokes, so the two triggers share one generation counter and an activation arriving through either route supersedes an in-flight ladder rather than
 * running a second one beside it.
 */
const activationHeals = new WeakMap<Page, () => Promise<void>>();

/**
 * Builds the callback the page's focus binding invokes: an immediate re-issue, then a ladder of follow-ups at ACTIVATION_REAFFIRM_LADDER_MS's offsets from this
 * invocation. The ladder is what makes the heal land, because the compositor's switch outlasts the focus event announcing it - the immediate shot alone cannot
 * reach the far side of the switch, so later shots have to.
 *
 * Invocations supersede rather than accumulate. A generation counter makes the newest invocation the owner of the rung schedule: the duplicate focus events a
 * single activation fires collapse into one schedule a few milliseconds newer than the first, while an activation that genuinely arrives later - a second
 * stream's tab give-back landing on this page, another click - gets a full bracket timed from its own moment instead of inheriting the tail of an in-flight one.
 * The counter lives in this closure, so a callback supersedes only the ladders it started itself and two streams' pages cannot reach each other's schedules.
 *
 * Nothing cancels a superseded ladder, because nothing has to: it returns at its next generation check, a shot against a page mid-teardown rejects into the
 * swallow below, and a shot that never settles leaves its ladder awaiting a promise nobody holds. The counter needs no reset for the same reason - a newer
 * invocation is itself the invalidation - and the schedule holds no timers, only awaited sleeps.
 *
 * A rejection is swallowed into a debug line because a focus event races page teardown by nature - the tab a user just selected can be the one a terminating
 * stream is closing - and the periodic re-affirmation corrects anything a lost re-issue leaves behind.
 * @param page - The capture page this callback re-affirms.
 * @param reaffirm - The re-issue to invoke. A parameter rather than a direct call so the callback's behavior can be driven on its own: what it does with the page
 *                   it was built for, what schedule it keeps, and what it does with a rejection, is the whole of its contract.
 * @param clock - The time source the ladder's waits run on. Defaults to the system clock.
 * @returns The callback, which never rejects. Its promise settles once the ladder is spent, which the page-side listener does not wait on.
 */
export function makeFocusReaffirmCallback(page: Page, reaffirm: (page: Page) => Promise<void>, clock: Clock = systemClock): () => Promise<void> {

  // The generation this closure hands out. It only ever increments: an invocation claims the next one and owns the rung schedule until a later invocation claims
  // a higher one.
  let generation = 0;

  // One rung's re-issue. The offset it was scheduled at is carried on the debug line, so a swallowed failure says which shot spoke; the immediate shot reports zero.
  const fireShot = async (offsetMs: number): Promise<void> => {

    try {

      await reaffirm(page);
    } catch(error) {

      LOG.debug("browser:lifecycle", "Could not re-affirm the capture surface after a tab activation (+%sms): %s.", offsetMs, formatError(error));
    }
  };

  return async (): Promise<void> => {

    const mine = ++generation;

    await fireShot(0);

    let previousOffsetMs = 0;

    for(const offsetMs of ACTIVATION_REAFFIRM_LADDER_MS) {

      // eslint-disable-next-line no-await-in-loop -- The pacing is the point: each rung waits out the distance left to its own offset before anything else runs.
      await clock.delay(offsetMs - previousOffsetMs);

      // A newer activation owns the schedule, and it carries its own rungs timed from its own moment, so this ladder has nothing left to contribute.
      if(generation !== mine) {

        return;
      }

      // eslint-disable-next-line no-await-in-loop -- Sequential by design: a rung's re-issue settles before the wait toward the next rung begins.
      await fireShot(offsetMs);

      previousOffsetMs = offsetMs;
    }
  };
}

/**
 * The collaborators a page's heal is built from: the time source its ladder's waits run on, and the re-issue each of its shots performs. Injected as a default
 * parameter in the shape CreatePageWithCaptureDeps, TabCaptureDeps, FullscreenDeps, and PrecachingDeps already use, so a test drives the enrolled callback through
 * the install point itself rather than around it. The port holds no state, and production call sites pass nothing.
 */
export interface ActivationHealDeps {

  readonly clock: Clock;
  readonly reaffirm: (page: Page) => Promise<void>;
}

// The production collaborators.
const defaultActivationHealDeps: ActivationHealDeps = { clock: systemClock, reaffirm: reaffirmCaptureSurface };

/**
 * Installs the tab-activation heal on a capture page. Chrome composes the capture of a selected tab from the window's fitted presentation, so the instant a
 * capture tab becomes the selected one that capture starts recording a clipped view of itself; the re-issue ladder this installs moves the composition back to
 * the emulated surface about a second later.
 *
 * The heal has two triggers and one ladder between them. The exposed binding is fired by a focus listener inside the page, which covers the activation that
 * arrives with a user's click focusing the browser window. The enrollment in activationHeals is fired by the selection primitive's activation report, which
 * covers every activation PrismCast performs for itself - the give-back returning a user to a capture tab, the selection step, a re-assert - and a captured page
 * hears none of those: capture holds it visible and the extension's update moves no operating-system focus, so no page event fires at all (measured 2026-08-30).
 * Both triggers hold the same callback instance, which is what lets them share its generation counter instead of running two schedules against one page.
 *
 * The listener is registered through evaluateOnNewDocument so it survives the page's navigations, exactly as the shared video-selector helper does, and the
 * binding survives them on Puppeteer's own terms.
 * @param page - The capture page to install the heal on.
 * @param deps - The clock and the re-issue the ladder runs on. Defaults to the production pair.
 */
export async function installActivationHeal(page: Page, deps: ActivationHealDeps = defaultActivationHealDeps): Promise<void> {

  const callback = makeFocusReaffirmCallback(page, deps.reaffirm, deps.clock);

  // Enrolled ahead of the binding, so the report-side trigger is live from the first moment either trigger could be. An exposeFunction that fails takes the page
  // down with it, and the entry goes when the page does.
  activationHeals.set(page, callback);

  await page.exposeFunction("__prismcastReaffirmSurface", callback);

  await page.evaluateOnNewDocument((): void => {

    window.addEventListener("focus", (): void => {

      void window.__prismcastReaffirmSurface?.();
    });
  });
}

/**
 * Heals the capture page showing in a tab the selection primitive has just activated.
 *
 * The report names a tab and what needs healing is a page, so the match walks the registry for a capture-mode stream whose page was identified as that tab. A
 * report matching nothing is the ordinary case rather than a failure - a give-back returning the selection to the user's own tab, a native stream that composes
 * no capture at all, a page registered before its setup reached the install point - and every one of them is a quiet no-op.
 * @param tabId - The tab the selection primitive activated.
 * @param deps - The registry read and the page-to-tab lookup the match runs through. Defaults to the production pair.
 */
export function healActivatedCaptureTab(tabId: number, deps: { readonly getAllStreams: typeof getAllStreams; readonly getCachedTabId: typeof getCachedTabId } =
  { getAllStreams, getCachedTabId }): void {

  for(const entry of deps.getAllStreams()) {

    // The mode is read through the registry's own published predicate rather than off the entry, keeping this layer's knowledge of a stream's shape to what the
    // registry chooses to publish. A stream's page is nullable, because an entry is registered before its setup has produced one, so the narrowing comes ahead
    // of the lookup that needs a page to ask about.
    if(!isCaptureIdentity(entry) || (entry.page === null)) {

      continue;
    }

    if(deps.getCachedTabId(entry.page) !== tabId) {

      continue;
    }

    // A page whose install point has not run yet has nothing enrolled to fire, and the monitor's periodic re-affirmation is what covers it in the meantime.
    void activationHeals.get(entry.page)?.();
  }
}

/* The extension-side trigger, subscribed once for the life of the process. Every activation PrismCast performs goes through the selection primitive and is
 * reported by it, which is the only way an activation of a captured page can be known at all; the page's own focus listener covers the one activation the
 * primitive does not perform, a user's click that focuses the window. A browser relaunch changes neither module, so nothing here is re-subscribed.
 */
onTabActivation((tabId: number): void => {

  healActivatedCaptureTab(tabId);
});

/**
 * Custom launch function that modifies Chrome arguments when running as a packaged executable. The packaged version cannot load extensions from node_modules
 * (which is bundled inside the executable), so we point the extension paths to our extracted extension files in the data directory.
 * @param opts - The launch options to modify.
 * @returns The launched browser instance.
 */
async function launchWithCustomArgs(opts: LaunchOptions): Promise<Browser> {

  // When running as a packaged executable (process.pkg is set by the pkg bundler), we add Chrome's native unpacked-extension flags, --disable-extensions-except
  // and --load-extension, each naming our extracted extension files. puppeteer-stream points opts.enableExtensions at its own node_modules-relative extension
  // directory, which puppeteer-core passes to a CDP browser.installExtension() call after Chrome starts, and that path does not exist at that location in the
  // packaged executable. This function leaves opts.enableExtensions as puppeteer-stream set it, so puppeteer-core still attempts that install, without awaiting it.
  if(process.pkg) {

    const extensionPath = getExtensionDir();

    // Drop any extension flags the args already carry, so our pair pointing at the extracted extension appears exactly once.
    opts.args = (opts.args ?? [])
      .filter((arg: string): boolean => !arg.startsWith("--load-extension=") && !arg.startsWith("--disable-extensions-except="))
      .concat([ "--disable-extensions-except=" + extensionPath, "--load-extension=" + extensionPath ]);
  }

  return puppeteerLaunch(opts);
}

/**
 * Formats the GPU capabilities into a human-readable suffix for the "Chrome ready" log line. The renderer string is already cleaned (ANGLE wrapper and Metal
 * prefix stripped) at detection time, so this function uses it directly and appends hardware-accelerated codec names in brackets when available.
 * @param gpu - The detected GPU capabilities.
 * @returns A formatted string like " (GPU: Apple M1 [H264, HEVC])" or " (software rendering)".
 */
function formatGpuSuffix(gpu: GpuCapabilities): string {

  const codecs = [

    gpu.av1HardwareEncoding && "AV1",
    gpu.h264HardwareEncoding && "H264",
    gpu.hevcHardwareEncoding && "HEVC"
  ].filter(Boolean);

  if(codecs.length > 0) {

    return " (GPU: " + gpu.renderer + " [" + codecs.join(", ") + "])";
  }

  // No hardware encoding available. A GPU may be present for rendering but lack hardware encoding - show the GPU name without codecs if we have a non-trivial
  // renderer string, otherwise label as software rendering.
  if(gpu.renderer && (gpu.renderer !== "unknown")) {

    return " (GPU: " + gpu.renderer + ")";
  }

  return " (software rendering)";
}

/**
 * Probes the browser's GPU hardware-encoding capabilities. It queries CDP SystemInfo.getInfo for renderer identity and H.264/HEVC/AV1 hardware encoding support,
 * falls back to a MediaRecorder capability probe where the CDP data is incomplete, and caches the result for codec selection to read.
 *
 * The probe runs against an existing page where one is open, and a temporary page otherwise.
 * @param browser - The browser instance to use for detection.
 */
async function detectBrowserCapabilities(browser: Browser): Promise<void> {

  let tempPage: Nullable<Page> = null;

  try {

    // Try to use an existing page first to avoid window activation issues on macOS. The pick goes through the shared carrier rule, because the restore below
    // acts on the window of whichever page carries it and a page living in its own window would restore that one instead of the shared one.
    let targetPage: Nullable<Page> = pickCarrierPage(await browser.pages());

    if(!targetPage) {

      // A probe page never needs its tab selected either.
      tempPage = await browser.newPage({ background: true });
      targetPage = tempPage;
    }

    /* Restore the window to its normal state before probing. Chrome restores window state from the persistent user data directory, so after a scheduled browser
     * restart the window can come up minimized, and the GPU probe wants an environment representative of the one capture runs in.
     */
    await unminimizeWindow(targetPage);

    // Detect GPU capabilities via CDP SystemInfo.getInfo. This is the authoritative source for GPU identity and hardware encoding capabilities - it runs at the
    // browser level (no page context or secure context required) and returns the actual list of hardware-accelerated video encoding profiles.
    try {

      const cdpSession = await browser.target().createCDPSession();

      try {

        const sysInfo = await cdpSession.send("SystemInfo.getInfo") as {
          gpu: {
            devices: { deviceString: string; driverVendor: string; driverVersion: string; vendorString: string }[];
            featureStatus?: Record<string, string>;
            videoEncoding: { maxFramerateDenominator: number; maxFramerateNumerator: number; maxResolution: { height: number; width: number }; profile: string }[];
          };
          modelName: string;
        };

        // Extract the GPU renderer from the primary device. The WebGL unmasked renderer provides a richer string (includes ANGLE backend info), so we query
        // that as well and prefer it when available.
        const deviceName = sysInfo.gpu.devices[0]?.deviceString ?? "unknown";

        // Get the unmasked WebGL renderer for a more descriptive GPU identity string.
        const webglRenderer = await evaluateWithAbort(targetPage, (): string => {

          const canvas = document.createElement("canvas");
          const gl = canvas.getContext("webgl");

          if(!gl) {

            return "unknown";
          }

          const ext = gl.getExtension("WEBGL_debug_renderer_info");

          return ext ? String(gl.getParameter(ext.UNMASKED_RENDERER_WEBGL)) : String(gl.getParameter(gl.RENDERER));
        });

        // Extract the meaningful GPU name. ANGLE wraps the actual GPU identity: "ANGLE (Vendor, GPU Name, API Version)". The GPU name is the second
        // comma-separated field. A non-ANGLE renderer string is used as is, and the device string from CDP is used only when WebGL reports the masked
        // "WebKit WebGL" or nothing at all.
        let renderer = webglRenderer;
        const anglePart = /^ANGLE \([^,]+, ([^,]+)/.exec(webglRenderer)?.[1];

        if(anglePart) {

          renderer = anglePart.trim();

          // Strip the "ANGLE Metal Renderer: " prefix that macOS adds.
          const metalPrefix = "ANGLE Metal Renderer: ";

          if(renderer.startsWith(metalPrefix)) {

            renderer = renderer.slice(metalPrefix.length);
          }
        } else if((webglRenderer === "WebKit WebGL") || (webglRenderer === "unknown")) {

          renderer = deviceName;
        }

        // Determine hardware encoding capability. Two paths:
        // 1. featureStatus.video_encode === "enabled" - authoritative Chrome-level flag indicating the platform's hardware encoding framework is active
        //    (VideoToolbox on macOS, VA-API on Linux, DXVA on Windows). When enabled, H.264 hardware encoding is always available.
        // 2. videoEncoding profile array - lists specific hardware-accelerated codec profiles (e.g., "H264 Main", "HEVC Main"). Populated on Linux/Windows
        //    via VA-API/DXVA but empty on macOS where VideoToolbox doesn't enumerate through this interface.
        const videoEncodeEnabled = sysInfo.gpu.featureStatus?.["video_encode"] === "enabled";
        const h264FromProfiles = sysInfo.gpu.videoEncoding.some((e) => e.profile.startsWith("H264"));
        const hevcFromProfiles = sysInfo.gpu.videoEncoding.some((e) => e.profile.startsWith("HEVC"));
        const av1FromProfiles = sysInfo.gpu.videoEncoding.some((e) => e.profile.startsWith("AV1"));

        // H.264 hardware encoding is available when either the feature flag or the profile list confirms it.
        const h264Hardware = videoEncodeEnabled || h264FromProfiles;

        // HEVC and AV1 hardware encoding: check the profile list first (authoritative on Linux/Windows). On macOS (empty profile list), probe via MediaRecorder
        // in the page context - MediaRecorder.isTypeSupported works in non-secure contexts unlike VideoEncoder.
        let hevcHardware = hevcFromProfiles;
        let av1Hardware = av1FromProfiles;

        if(videoEncodeEnabled && (!hevcHardware || !av1Hardware)) {

          const [ hevcSupported, av1Supported ] = await evaluateWithAbort(targetPage, (): [boolean, boolean] => {

            if(typeof MediaRecorder === "undefined") {

              return [ false, false ];
            }

            return [

              MediaRecorder.isTypeSupported("video/mp4;codecs=hvc1.1.6.L93.B0"),
              MediaRecorder.isTypeSupported("video/mp4;codecs=av01.0.08M.08")
            ];
          });

          hevcHardware ||= hevcSupported;
          av1Hardware ||= av1Supported;
        }

        setGpuCapabilities({ av1HardwareEncoding: av1Hardware, h264HardwareEncoding: h264Hardware, hevcHardwareEncoding: hevcHardware, renderer });

        LOG.debug("browser:lifecycle", "GPU detection: device=%s, renderer=%s, H.264=%s, HEVC=%s, AV1=%s, video_encode=%s, encoding profiles=%s.",
          deviceName, renderer, h264Hardware, hevcHardware, av1Hardware, sysInfo.gpu.featureStatus?.["video_encode"] ?? "unknown",
          sysInfo.gpu.videoEncoding.map((e) => e.profile).join(", ") || "none");

      } finally {

        void cdpSession.detach().catch(() => { /* Session may already be detached. */ });
      }
    } catch(gpuError) {

      LOG.debug("browser:lifecycle", "GPU detection failed: %s.", String(gpuError));
    }

  } catch(error) {

    LOG.warn("Browser capability detection failed: %s. Hardware encoding capabilities are unknown for this session.", formatError(error));
  } finally {

    // Clean up temporary page if we created one.
    if(tempPage) {

      try {

        await tempPage.close();
      } catch(_closeError) {

        // Ignore close errors.
      }
    }
  }
}

/**
 * Relinquishes the current browser's capture readiness and tears down everything that depended on it. This is the single source of truth for "the published browser
 * is no longer usable", and the disconnect handler is its one caller: the browser is gone, so nothing that depended on it can be salvaged. It drops the
 * supervisor's readiness first (which supersedes any launch in flight, clears the governor's health anchor, and transitions the lifecycle to absent so the next
 * request relaunches through the gate and governor), clears the adapter-held metadata and caches, ends login mode, terminates every active stream (they were
 * capturing on the now-unusable browser), and emits status. The caller logs the specific cause. A browser that is still connected but can no longer start captures
 * takes the far gentler noteBrowserCaptureImpaired path instead, which keeps its running captures.
 * @param streamTerminationReason - The reason recorded against each terminated stream, for the stream-end logs.
 */
function relinquishBrowserReadiness(streamTerminationReason: string): void {

  // Drop readiness first: supersede any in-flight launch, clear the governor's health anchor, and move the lifecycle to absent.
  supervisor.noteReadinessLost();

  // Clear the adapter-held Chrome version and the cached user agent so stale values are not served before the next ready browser. We do not track Chrome's PID
  // directly - killStaleChrome discovers orphans via the OS process table on the next startup, which removes a class of state that could go stale.
  currentChromeVersion = null;
  setChromeUserAgent(null);

  // Cancel any pending scheduled-restart quiet timer since the browser this readiness applied to is gone.
  cancelRestartQuietTimer();

  // Clear all channel selection caches. Cached state (guide row positions, discovered page URLs) belongs to the old browser session.
  clearChannelSelectionCaches();

  // End login mode if it was active. We use clearLoginState() rather than endLoginMode() because the browser may already be gone and we do not want to attempt any
  // browser operations (page close, window minimize).
  if(clearLoginState() && !gracefulShutdownInProgress) {

    LOG.info("Login mode ended due to browser readiness loss.");
  }

  /* Terminate every active stream through the streaming layer's authoritative terminator, so a readiness loss cleans up exactly the way every other termination
   * path does. Kept even during graceful shutdown as a defensive measure - termination is safe to call more than once, so streams the caller already tore down
   * leave this a harmless pass over an empty array.
   */
  const activeStreams = getAllStreams();

  if(streamTerminator) {

    for(const streamInfo of activeStreams) {

      streamTerminator(streamInfo.id, streamInfo.info.storeKey, streamTerminationReason);
    }
  } else {

    /* An unwired terminator means the streaming layer never loaded, so nothing here can run the streams' cleanup sequence. Report it and keep going: the page
     * tracking and status emission below are still worth doing, and abandoning them would compound a wiring failure with a half-finished teardown.
     */
    LOG.error("Stream cleanup was skipped on browser readiness loss because no stream terminator is wired. Active streams left untouched: %s.",
      activeStreams.length);
  }

  // The session those streams captured on is over, so whatever page tracking survived their termination belongs to a browser that is gone.
  clearPageTracking();

  // Emit system status after stream cleanup. Skip during graceful shutdown since no clients are listening and the process is exiting.
  if(!gracefulShutdownInProgress) {

    void emitCurrentSystemStatus();
  }
}

/**
 * Handles browser disconnection events by relinquishing readiness and terminating all active streams. Called when the browser crashes, is closed externally, or
 * otherwise loses its connection. It runs only for a genuine, unsolicited disconnect: every intentional teardown removes this listener via closeBrowserInstance
 * first, so a scheduled restart, an orphan close, or an invalidation never reaches here. The browser is already gone, so there is nothing to close - relinquish
 * readiness and the next request relaunches a fresh, gate-verified browser.
 */
function handleBrowserDisconnect(): void {

  // Announce the unexpected disconnect before tearing down (the message says streams will be terminated, which relinquish then does). Suppressed during a full
  // server shutdown, where app.ts shutdown() (or closeBrowser() as a fallback) set the flag and the disconnect is intentional.
  if(!gracefulShutdownInProgress) {

    LOG.error("Browser disconnected unexpectedly. All active streams will be terminated.");
  }

  relinquishBrowserReadiness("browser disconnect");
}

/**
 * Records that a still-connected browser can no longer start captures - a mid-life capture death that no "disconnected" event would surface - and runs the
 * relaunch that cures it. This is the single recovery action for a browser that is alive and still serving: the mark lives on the supervisor's ready state, so its
 * running captures continue untouched, new stream requests are refused at acquire() with a 503 back-off, the recovery ladder stops offering tab replacement, and
 * the relaunch waits until nothing depends on the browser. Exported for the streaming layer to call once its probe or its wedge has produced the verdict.
 *
 * The returned promise settles once the relaunch this mark triggered has settled, which is what a caller that means to use the fresh browser awaits: the teardown
 * holds the supervisor in its draining state, where acquire() rejects rather than joins, so acquiring any earlier draws that rejection instead of the new
 * instance. A caller with nothing waiting on the outcome voids it and the relaunch proceeds on its own.
 *
 * The restart trigger uses this function's return value rather than the supervisor's transition observer, deliberately. The observer runs inside transition(), so a
 * restart begun there would call noteReadinessLost re-entrantly while the marking transition's notification is still on the stack. The observer stays a reporter -
 * the alarm and the status emit - and the caller that holds the verdict acts on it once the transition has completed.
 * @param browser - The specific browser instance the caller verified as unable to start captures.
 * @param reason - A short description of the evidence behind the verdict, carried in the alarm log and the impairment record.
 * @returns A promise settling once the relaunch this mark triggered has settled, or at once when the verdict landed on nothing.
 */
export async function noteBrowserCaptureImpaired(browser: Browser, reason: string): Promise<void> {

  // A false answer means the verdict landed on nothing: the instance was superseded by a disconnect and relaunch while the caller was confirming it, or the browser
  // already carries a mark whose alarm and status emit have already fired. Either way there is no new state to act on.
  if(!supervisor.noteCaptureImpaired(browser, reason)) {

    return;
  }

  await restartBrowserIfImpairedAndIdle();
}

/**
 * Provides access to the capture-ready browser, launching one if needed. This is the single gated entry point for all browser access: it delegates to the
 * supervisor's acquire(), which returns the ready browser, joins an in-flight launch (single-flight, so concurrent callers never contend on Chrome's profile lock),
 * lazily launches when absent, or - while the relaunch governor is cooling after repeated failures - rejects fast with a BrowserUnavailableError WITHOUT spawning
 * Chrome (the loop bound). The launch it drives runs the readiness gate, so a returned browser is verified capture-ready, not merely connected.
 *
 * The purpose is what the caller intends to do with the browser, and it is the gate on a marked one: a "capture" caller is refused, because that is precisely the
 * operation the browser can no longer perform, while a "page" caller - precaching, the startup warm-up, the relaunch itself - is served as usual.
 * @param purpose - Whether the caller goes on to start a capture or only needs a page to open.
 * @returns The capture-ready browser instance.
 * @throws BrowserUnavailableError while the governor is cooling, BrowserCaptureImpairedError when a capture purpose meets a browser that can no longer start
 *   captures, BrowserSupersededError if an in-flight launch was abandoned by a readiness-loss, or the underlying launch error when a launch attempt fails.
 */
export async function getCurrentBrowser(purpose: BrowserPurpose): Promise<Browser> {

  return supervisor.acquire(purpose);
}

/**
 * The supervisor's `launch` port: spawns Chrome, runs the readiness gate, performs post-launch initialization (display detection, version/UA capture, precaching),
 * and resolves ONLY with a capture-ready browser. It builds into a local instance and publishes nothing - the supervisor owns publication and transitions to
 * "ready" only after this resolves. A launch that fails the gate tears down its own Chrome here and throws, so a broken instance is never handed up; the supervisor
 * counts the failure and decides whether to relaunch immediately or cool down. The gate throws rather than logging a failed extension load as a warning and serving
 * the broken browser anyway, so only a verified-capturing instance is ever published.
 * @returns The capture-ready browser instance.
 * @throws If the launch or the readiness gate fails.
 */
async function launchReadyBrowser(): Promise<Browser> {

  const browserElapsed = startTimer();

  // Seed the profile's extension developer-mode flag before Chrome reads the profile. Chrome loads the unpacked capture extension only when the flag is set, and
  // this is the one function every launch passes through, so it is also the only place that runs before the first-ever launch on a fresh install.
  seedProfilePreferences(getChromeDataDir(CONFIG));

  // The launch function from puppeteer-stream wraps standard Puppeteer launch to inject the streaming extension. We pass our custom launch function that handles
  // packaged-executable extension paths. This happens on first stream request, after a browser crash, during server warmup, or during a governed relaunch.
  const browser = await launch({ launch: launchWithCustomArgs }, buildLaunchOptions());

  try {

    LOG.debug("timing:browser", "Chrome process spawned. (+%sms)", browserElapsed());

    // The init timeout is live, so it is read once per launch: the error below then names the bound the wait applied, and a save made during a handshake applies
    // at the next launch.
    const initTimeout = CONFIG.browser.initTimeout;

    // Readiness gate, handshake tier (cheap, on-suspicion). Poll for the puppeteer-stream extension to finish initializing - it injects a START_RECORDING function
    // into its options page context, so its presence is the extension's own readiness signal. We poll rather than fixed-delay so the browser is ready as soon as the
    // extension loads (typically 200-500ms). On failure this THROWS rather than warning-and-proceeding: an unregistered extension means chrome.tabs is undefined and
    // every capture acquisition would hang, so the instance is not capture-ready and must not be published. We reclassify the raw waitForFunction timeout into a
    // capture-infrastructure error carrying "timed out" so the setup layer maps it to a 503 back-off (the same as the capability-tier probe failure), rather than a
    // 500 the client would not back off from - an unregistered extension is a capture-infrastructure fault, and a fresh relaunch usually clears it.
    try {

      const extensionPage = await getExtensionPage(browser);

      await extensionPage.waitForFunction(EXTENSION_READY_EXPRESSION, { timeout: initTimeout });
    } catch(handshakeError) {

      throw new Error("The capture extension handshake timed out after " + String(initTimeout) + " ms.", { cause: handshakeError });
    }

    LOG.debug("timing:browser", "Extension initialized. (+%sms)", browserElapsed());

    /* The tab the window rests on. It is created once and selected by construction because nothing asks otherwise, so whatever the window shows when nothing has
     * asked for a tab is a blank page rather than the capture extension's own options page - which the library opens selected at launch. It is left unmanaged, as
     * stale page cleanup judges only the pages stream setup and discovery create; it carries no correctness duty, so it needs no re-validation, and a user
     * closing it costs nothing.
     *
     * It is also the page the shared window's identity is read from. This tab is in that window by construction, and reading it here - before the capture probe,
     * the capability detection, or any stream - is what lets every later capture tab be anchored to the window rather than placed by Chrome.
     */
    const restingTab = await browser.newPage();

    await noteSharedWindow(browser, restingTab);

    // Readiness gate, capability tier (the authoritative arbiter). Run the injected capture probe - a real capture acquisition against a throwaway page on THIS
    // instance - so "ready" means "really captured," not merely "the extension handshake responded." This predicate must run at every (re)launch:
    // it exercises the exact acquisition path that fails when the extension is unregistered. A probe failure throws, so the supervisor counts the launch failure
    // and the browser is never published.
    //
    // If the probe is not wired (the injection point left unset by a refactor - impossible in the normal import order, which always wires it before any launch),
    // we reject the launch rather than publish a handshake-only browser: serving an unverified browser would be the "proceed and hope" path this design
    // eliminates. The supervisor counts the rejected launch and, on repetition, degrades loudly.
    if(!captureProbe) {

      throw new Error("The capture-readiness probe is not wired; refusing to publish a browser whose capture capability was not verified.");
    }

    await captureProbe(browser);

    LOG.debug("timing:browser", "Capture probe complete. (+%sms)", browserElapsed());

    // Probe the browser's capabilities before the browser is published ready. Codec selection and preroll generation both read the cached GPU capabilities, so
    // they have to be in hand before anything downstream can choose a capture codec.
    await detectBrowserCapabilities(browser);

    LOG.debug("timing:browser", "Browser capability detection complete. (+%sms)", browserElapsed());

    // Capture the Chrome version and User-Agent. The version is logged for diagnostics (correlating browser behavior changes with specific Chrome releases) and
    // surfaced by the health endpoint; the User-Agent lets server-side fetch() calls to service CDNs match Chrome's identity.
    const chromeVersion = await browser.version();
    const userAgent = await browser.userAgent();

    currentChromeVersion = chromeVersion;
    setChromeUserAgent(userAgent);

    const gpu = getGpuCapabilities();
    const gpuSuffix = gpu ? formatGpuSuffix(gpu) : "";

    LOG.info("Chrome ready: %s%s.", chromeVersion, gpuSuffix);

    LOG.debug("timing:browser", "Browser ready. Total: %sms.", browserElapsed());

    // Start background precaching of selected service channel lineups. Fire-and-forget - the delay startPrecaching() arms on its own clock defers the work until
    // after this launch settles and the supervisor has published the ready browser, so its getCurrentBrowser() resolves immediately rather than re-entering this
    // launch.
    startPrecaching();

    /* The reaper belongs to the browser instance the supervisor is about to publish: puppeteer drops a closed browser's listeners along with the instance, and
     * a relaunch comes back through this same path and installs its own. It goes in ahead of the disconnect handler so that handler stays the last step before
     * publication, for the reason its own comment gives.
     */
    installStrayOpenTabReaper(browser);

    // Arm the disconnect handler only now, as the very last step before the supervisor publishes this browser as ready. It is deliberately NOT armed earlier: during
    // the gate/init window above, a Chrome crash surfaces as a thrown init step (CDP and waitForFunction reject on a dead browser), which the supervisor counts as a
    // launch failure and feeds to the governor - keeping the relaunch loop bounded even if Chrome dies repeatedly during init. Arming the handler earlier would let
    // its noteReadinessLost() bump the supervisor's launch generation and the launch would be treated as superseded (uncounted), defeating the loop bound. There is
    // no gap: every statement from the last await to this return is synchronous, so a disconnect cannot be delivered between this registration and publication.
    browser.on("disconnected", handleBrowserDisconnect);

    return browser;
  } catch(error) {

    LOG.error("Failed to launch browser: %s.", formatError(error));

    // The gate (or post-launch init) failed. Clear the adapter-held metadata and tear down the Chrome instance we just spawned before propagating, so a failed
    // launch never leaks a process and the next governed relaunch starts from a clean profile. The disconnect handler is not yet armed on this instance (it is armed
    // only on the success path above), so this teardown never re-enters handleBrowserDisconnect - which is exactly what lets the supervisor count this as a launch
    // failure rather than a supersession.
    currentChromeVersion = null;
    setChromeUserAgent(null);

    await closeBrowserInstance(browser);

    throw error;
  }
}

/**
 * Returns the Chrome version string captured when the browser launched, or null if the browser is not connected.
 * @returns The Chrome version string (e.g., "Chrome/144.0.7559.110") or null.
 */
export function getChromeVersion(): Nullable<string> {

  return currentChromeVersion;
}

/**
 * Returns the current browser instance, or null if not launched. Unlike getCurrentBrowser(), this does not lazily launch. Used by modules that need to check
 * browser state without triggering a launch (e.g., login mode checking connectivity before opening a tab).
 * @returns The browser instance, or null if not running.
 */
export function getBrowserInstance(): Nullable<Browser> {

  return supervisor.current();
}

/**
 * Returns the impairment recorded on the published browser, or null when nothing is published or the published browser can still start captures. Like
 * getBrowserInstance this does not launch, so callers that only need to know whether captures can be started - the recovery ladder, the status composition, the
 * health endpoint - read it without touching the lifecycle.
 * @returns The impairment record, or null.
 */
export function getCaptureImpairment(): Nullable<CaptureImpairment> {

  return supervisor.captureImpairment();
}

/**
 * Checks if the browser is currently connected and usable. This is a synchronous check that can be used before attempting browser operations.
 * @returns True if the browser is connected and ready for use, false otherwise.
 */
export function isBrowserConnected(): boolean {

  const browser = supervisor.current();

  return !!browser && browser.connected;
}

/**
 * Gets all open browser pages (tabs). This is used by the health check endpoint to report page count.
 * @returns Array of pages, or empty array if the browser is not connected.
 */
export async function getBrowserPages(): Promise<Page[]> {

  // Guard against calling this when no ready browser is running.
  const browser = supervisor.current();

  if(!browser?.connected) {

    return [];
  }

  try {

    return await browser.pages();
  } catch(_error) {

    // If getting pages fails (browser disconnecting, etc.), return empty array rather than throwing.
    return [];
  }
}

/**
 * Tears down a specific Chrome instance and is the single teardown primitive: the supervisor's `close` port (for disposing an orphaned superseded launch), the
 * launch-failure cleanup in launchReadyBrowser, the scheduled-restart teardown in executeBrowserRestart, and the full-server closeBrowser all route through it. It
 * owns no lifecycle state - the supervisor is the single source of truth for that. It first removes the disconnect listener so this intentional teardown does not
 * trip handleBrowserDisconnect: the SIGTERM-induced "disconnected" event can arrive after this function returns, and without the removal it could supersede a fresh
 * launch the caller has already started (the late-disconnect race).
 *
 * Chrome termination uses Puppeteer's ChildProcess handle and its `exit` event for detection:
 *
 * - browser.close() first runs puppeteer-stream's pass that closes every page but the extension's capture page and queries the extension's tabs, then sends CDP
 *   Browser.close and waits for Chrome's process to exit, with no bound of its own on that wait.
 * - browser.disconnect() drops the DevTools connection instantly but orphans Chrome as a Node child process, creating a zombie that process.kill(pid, 0) cannot detect.
 * - Synchronous polling (Atomics.wait) blocks the event loop, preventing Node from processing SIGCHLD to reap the child - Chrome becomes a zombie regardless
 *   of how SIGTERM was sent.
 *
 * Instead, we send SIGTERM through the ChildProcess handle and listen for the `exit` event. This keeps the event loop running so Node can process SIGCHLD and reap
 * Chrome properly. The exit event fires only after the process is fully reaped - no zombies, no polling, no event loop blocking. The await on the exit event is what
 * lets the caller relaunch immediately afterward without contending on Chrome's profile lock.
 * @param browser - The Chrome instance to terminate.
 */
async function closeBrowserInstance(browser: Browser): Promise<void> {

  // Remove the disconnect handler before signalling. Every call here is an intentional teardown, so the resulting "disconnected" event must not invoke the
  // unexpected-disconnect handler - which would clear caches, log an error, and (critically) call noteReadinessLost(), superseding any launch the caller starts next.
  browser.off("disconnected", handleBrowserDisconnect);

  // Send SIGTERM through Puppeteer's ChildProcess handle and wait for the `exit` event. The ChildProcess handle is only available when Puppeteer launched Chrome
  // (not when connecting to an existing browser), but PrismCast always launches Chrome directly.
  const chromeProcess = browser.process();

  if(chromeProcess?.pid && !chromeProcess.killed) {

    // Listen for the exit event before sending the signal. The event fires after the OS reaps the process, so there is no zombie window. The promise only ever
    // resolves, so a null from either bounded wait below means the exit never came.
    const { promise: exitPromise, resolve: signalExit } = Promise.withResolvers<true>();

    chromeProcess.on("exit", () => { signalExit(true); });

    chromeProcess.kill("SIGTERM");

    LOG.debug("browser:lifecycle", "Sent SIGTERM to Chrome process %d.", chromeProcess.pid);

    // Wait for Chrome to exit after SIGTERM, with a bound. If Chrome doesn't exit in time, escalate to SIGKILL.
    const exitedAfterTerm = await boundedWait(exitPromise, TERM_WAIT_MS);

    if(!exitedAfterTerm) {

      // SIGTERM didn't work within the bound. Escalate to SIGKILL. A wedged or hung Chrome can fail to exit on SIGTERM within the bound.
      LOG.debug("browser:lifecycle", "Chrome did not exit after SIGTERM. Escalating to SIGKILL.");

      chromeProcess.kill("SIGKILL");

      // The same exit promise serves the second wait: if it already resolved, this returns its value immediately.
      await boundedWait(exitPromise, KILL_WAIT_MS);
    }
  }

  // Disconnect Puppeteer's DevTools connection (the CDP pipe) after Chrome has exited. This cleans up Puppeteer's internal state (event listeners, pending CDP
  // calls) without waiting for an orderly close on a dead connection. We catch the rejection: disconnect() on a connection whose underlying transport already died of
  // an unclean Chrome exit can reject, and an unhandled rejection on this fire-and-forget call would crash the process during an otherwise-successful teardown.
  if(browser.connected) {

    browser.disconnect().catch((error: unknown) => {

      LOG.debug("browser:lifecycle", "Ignoring browser disconnect error during teardown: %s.", formatError(error));
    });
  }

  /* The lock files go through the holder test on a scan taken here, once this process's Chrome has exited, because only a scan after the exit can see a Chrome
   * another instance launched on a shared profile in its place. This process's own Chrome never reads as that holder: a main still running after the bounded
   * waits is ours by the ownership test, and its helpers are not roots. A helper still shutting down after its main exited has been adopted by a live process
   * and reads as a holder, so that teardown keeps the files, the adopted-orphan limit the sweep shares, and the next startup sweep with no Chrome alive removes them.
   */
  cleanStaleProfileFiles(listProcesses(), getChromeDataDir(CONFIG));
}

/**
 * Closes the browser and cleans up resources during full server shutdown. After this call the supervisor reports absent and any subsequent stream request launches
 * a fresh browser. It retires the current instance from the lifecycle (so an in-flight launch is superseded and the metadata is cleared), then delegates the actual
 * Chrome teardown to closeBrowserInstance. The graceful-shutdown flag is set so handleBrowserDisconnect, if it runs for any reason, stays quiet.
 */
export async function closeBrowser(): Promise<void> {

  // Ensure the flag is set so the disconnect handler stays quiet. Normally set earlier by app.ts shutdown(), but set here as a fallback for direct calls.
  setGracefulShutdown(true);

  // Capture the ready browser before retiring it from the lifecycle. noteReadinessLost() supersedes any launch in flight and transitions to absent; we then clear
  // the adapter-held metadata so nothing stale is served.
  const browser = supervisor.current();

  supervisor.noteReadinessLost();

  // The session is ending, so its page ids are spent. The call sits ahead of the early return below so the clear happens whether or not there was a browser to
  // close.
  clearPageTracking();

  currentChromeVersion = null;
  setChromeUserAgent(null);

  if(!browser) {

    return;
  }

  // Readiness was relinquished first, because that is what supersedes an in-flight launch; publishing the teardown synchronously, before any await, then keeps the
  // launch window shut for the whole drain so nothing spawns a second Chrome against the profile lock this one still holds.
  const teardown = closeBrowserInstance(browser);

  supervisor.noteTeardownBegun(teardown, BROWSER_TEARDOWN_DRAIN_BOUND_MS);

  await teardown;
}

/* Over time, browser pages (tabs) may accumulate if cleanup fails during stream termination. This can happen due to race conditions, errors during cleanup, or
 * edge cases in stream lifecycle management. Each orphaned page consumes memory and may continue running JavaScript, so we periodically clean them up.
 *
 * The cleanup has several safeguards to prevent closing pages that shouldn't be closed:
 *
 * 1. Only managed pages: We only consider pages that PrismCast created (tracked in managedPageIds). Pages opened manually by the user for debugging, or pages opened
 *    by streaming sites (OAuth popups, etc.) are left alone.
 *
 * 2. Managed page IDs: Each managed page is assigned a string ID ("page-" plus a counter) by registerManagedPage and read back through the Page reference by
 *    getManagedPageId. The staleness map, the in-flight set and the decision core all work on these stable string keys rather than on Page objects.
 *
 * 3. Grace period: Pages must be observed as potentially stale for a configurable grace period before being closed. This handles race conditions where pages are
 *    briefly untracked during stream initialization or cleanup.
 *
 * 4. Minimum page preservation: We always keep at least one page open to prevent Chrome from exiting.
 *
 * 5. In-flight exemption: Pages an operation still holds (tracked in inFlightPageIds) are never considered stale. The registry records a stream's page only once
 *    setup completes, and a discovery page reaches the registry at no point at all, so without this a slow tune or a running walk would lose its own page.
 *
 * The safeguards are expressed as rules in browser/pageStaleness.ts, which decides from a snapshot what to close, track, forget, and unmark. This function is the
 * I/O shell around that decision: it reads Chrome's page list, applies the decision to the tracking collections, and performs the closes.
 */

/**
 * Cleans up browser pages that are not associated with active streams. This function runs periodically to catch any pages that were not properly closed during
 * stream termination.
 *
 * The cleanup uses a multi-stage filtering process:
 * 1. Only consider pages we created (in managedPageIds)
 * 2. Exclude pages associated with active streams, and pages an operation still holds in flight
 * 3. Apply a grace period before closing (to handle race conditions)
 * 4. Preserve at least one page to keep the browser alive
 * @param now - The instant the sweep judges staleness against, read from the interval's own clock so the staleness clocks this sweep starts and the ones it
 * later reads are stamped on one time source.
 */
export async function cleanupStalePages(now: number): Promise<void> {

  // Guard against calling this when no ready browser is running.
  const browser = supervisor.current();

  if(!browser?.connected) {

    return;
  }

  try {

    const pages = await browser.pages();

    // If there's only one page or fewer, we must preserve it to keep the browser alive. Don't attempt cleanup.
    if(pages.length <= 1) {

      return;
    }

    // Build a set of page IDs for pages currently in use by active streams.
    const activePageIds = new Set<string>();

    for(const streamInfo of getAllStreams()) {

      if(streamInfo.page) {

        const pageId = getManagedPageId(streamInfo.page);

        if(pageId) {

          activePageIds.add(pageId);
        }
      }
    }

    // Project the browser's pages into the shape the decision core reads: the managed ids in the browser's own order, with undefined standing in for pages we
    // did not create, plus a lookup back to the Page objects so the ids it returns can be resolved to something closable.
    const idToPage = new Map<string, Page>();
    const pageIds: (string | undefined)[] = [];

    for(const page of pages) {

      const pageId = getManagedPageId(page);

      pageIds.push(pageId);

      if(pageId !== undefined) {

        idToPage.set(pageId, page);
      }
    }

    // The staleness judgment - clocks, exemptions, the dead-entry sweep, and the preserve-one budget - belongs to the pure core; this function only carries it out.
    const actions = evaluateStalePages({ activePageIds, gracePeriodMs: CONFIG.recovery.stalePageGracePeriod, inFlightPageIds, now, pageIds,
      staleFirstSeen: potentiallyStalePages });

    // Bring the tracking collections in line with the decision before any close runs, so a close that fails cannot leave the bookkeeping half-applied.
    for(const pageId of actions.forgetTrackedIds) {

      potentiallyStalePages.delete(pageId);
    }

    for(const pageId of actions.startTrackingIds) {

      potentiallyStalePages.set(pageId, now);
    }

    for(const pageId of actions.clearInFlightIds) {

      inFlightPageIds.delete(pageId);
    }

    let closedCount = 0;

    for(const pageId of actions.closeIds) {

      // Every id the core returns for closing came from the page list built above, so this resolves; the check is what narrows it to a Page.
      const page = idToPage.get(pageId);

      if(!page) {

        continue;
      }

      try {

        // Unregister the page before closing to prevent any race with re-registration.
        managedPageIds.delete(pageId);

        potentiallyStalePages.delete(pageId);

        // eslint-disable-next-line no-await-in-loop
        await page.close();

        closedCount++;
      } catch(_error) {

        // Page may have already been closed between our check and the close attempt. This is expected in race conditions.
      }
    }

    // Log only if we actually closed something, to avoid log spam from idle cleanup runs.
    if(closedCount > 0) {

      LOG.debug("browser:lifecycle", "Cleaned up %s stale page(s).", closedCount);
    }
  } catch(error) {

    // Cleanup failure is not critical - log it at debug level and let the next interval retry.
    LOG.debug("browser:lifecycle", "Stale page cleanup failed: %s.", formatError(error));
  }
}

/**
 * Arms the stale-page sweep's interval on the owner's registry: cleanupStalePages at the owner's clock reading, every interval. Arming under the sweep's key
 * replaces an interval already armed there, so a re-arm leaves one sweep, on the new cadence from the moment it is armed.
 * @param sweep - The running stale-page owner.
 * @param interval - Milliseconds between sweeps.
 */
function armStalePageSweep(sweep: StalePageSweep, interval: number): void {

  sweep.timers.setInterval(STALE_PAGE_SWEEP_KEY, () => { void cleanupStalePages(sweep.clock.now()); }, interval);
}

/**
 * Starts the periodic stale page cleanup. This should be called once during server startup, after the browser is initialized. The sweep runs indefinitely until
 * stopStalePageCleanup() is called (typically during graceful shutdown), and a saved interval re-arms it through applyStalePageCleanupChanges(). A second start
 * while one is running changes nothing.
 * @param clock - The clock the sweep's interval arms on and whose reading each sweep judges staleness against, so the cadence and the judgment share one time
 * source. Defaults to the system clock.
 */
export function startStalePageCleanup(clock: Clock = systemClock): void {

  if(stalePageSweep) {

    return;
  }

  const sweep: StalePageSweep = { clock, timers: new TimerRegistry({ clock }) };

  armStalePageSweep(sweep, CONFIG.recovery.stalePageCleanupInterval);

  stalePageSweep = sweep;
}

/**
 * Stops the periodic stale page cleanup. This should be called during graceful shutdown to prevent the sweep from running after we've started shutting down the
 * browser and streams. Disposing the registry drains the interval, and a stop with nothing running is a no-op.
 */
export function stopStalePageCleanup(): void {

  stalePageSweep?.timers.dispose();
  stalePageSweep = null;
}

/**
 * Re-arms the running stale-page sweep at a saved cleanup interval. The re-arm restarts the cadence from the save, so the next sweep runs one full interval after
 * it, and the grace period needs nothing here, because each sweep already reads it when it runs. The handler reads the running sweep and never creates one, so a
 * save before the boot's start or after the stop arms nothing, and a save can never resurrect a stopped sweep. The interval comes from the candidate, because
 * the reconcile commits CONFIG only after its handlers run. Re-arming cannot fail, so the handler refuses nothing.
 * @param _changes - The change to the cleanup interval; the candidate carries the interval, so the handler reads that instead.
 * @param next - The candidate running configuration.
 * @returns No rejections.
 */
export async function applyStalePageCleanupChanges(_changes: readonly ConfigChange[], next: Readonly<Config>): Promise<readonly ChangeRejection[]> {

  if(stalePageSweep) {

    const interval = next.recovery.stalePageCleanupInterval;

    armStalePageSweep(stalePageSweep, interval);
    LOG.debug("browser:lifecycle", "Stale page cleanup re-armed at %d ms.", interval);
  }

  return [];
}

// Module-load side effect: register the handler once per process, as every config-change handler registers, so it is in place before the first save can reach
// the reconcile.
registerConfigChangeHandler("recovery.stalePageCleanupInterval", applyStalePageCleanupChanges);

/* Browser restart functions. One routine performs the restart; what differs is the cause that reaches it.
 *
 * The maintenance cause is opportunistic and age-driven: the check runs on a 30-second interval and, when the browser exceeds BROWSER_MAX_AGE with zero active
 * streams, starts a quiet period timer. The quiet timer is cancelled if a stream starts, ensuring active viewers are never disrupted. When the timer expires, the
 * browser is closed and immediately re-launched.
 *
 * The impairment cause is a repair rather than hygiene: a browser that can no longer start captures is unusable for new tunes no matter how young it is, so age and
 * the quiet period do not apply to it. It relaunches the moment nothing depends on the browser any more, which the mark itself, every stream end, and the periodic
 * tick each check for.
 */

/**
 * The registry facts a restart's idleness decision is made from. One shape, read by readRestartFacts and judged by isBrowserIdleForRestart, so the decision stays
 * a pure function of stated facts rather than of whatever each guard happened to read.
 */
interface RestartFacts {

  // Whether any registered stream holds its page, and so would lose it to a teardown.
  readonly establishedStreams: boolean;

  // How many pages an operation currently holds for its own duration, a tune mid-setup above all.
  readonly inFlightPages: number;

  // How many entries the registry holds, pending ones included.
  readonly streamCount: number;
}

/**
 * Decides whether the browser may be torn down for the given cause, from facts read at the call. Each cause asks a different question of the same registry, so
 * the decision is stated once here rather than spelled out at each guard.
 *
 * Maintenance is opportunistic housekeeping, so it waits for an empty registry outright: a pending entry is a tune in progress, and replacing the browser under
 * one would fail it for nothing better than a fresher instance. Impairment is a repair the tunes themselves are waiting on, so it asks the narrower question of
 * what a teardown would actually destroy - a stream established on this browser, whose entry holds its page, or a page an operation still holds in flight. A tune
 * refused a capture start holds neither while it waits for the relaunch, so the very pending entry that maintenance would defer to is not a reason to leave an
 * unusable browser in place. A tune that has acquired the browser but not yet opened its page falls outside every dependency source for the same reason and
 * deliberately so: it holds nothing a relaunch would destroy, neither a page nor a capture, and its own capture start would meet the impaired browser in any
 * case, so the relaunch may run under it.
 * @param cause - Why the restart wants to run.
 * @param facts - The registry facts read at the call.
 * @returns True when the browser may be torn down for this cause.
 */
export function isBrowserIdleForRestart(cause: BrowserRestartCause, facts: RestartFacts): boolean {

  switch(cause) {

    case "impairment": {

      return !facts.establishedStreams && (facts.inFlightPages === 0);
    }

    case "maintenance": {

      return facts.streamCount === 0;
    }
  }
}

/**
 * Reads the registry facts the idleness decision is made from, at the moment of the call.
 * @returns The facts.
 */
function readRestartFacts(): RestartFacts {

  return { establishedStreams: hasEstablishedStreams(), inFlightPages: inFlightPageIds.size, streamCount: getStreamCount() };
}

/**
 * Relaunches a browser that can no longer start captures, as soon as nothing depends on it. Every trigger routes here - the mark itself, each stream termination,
 * and the periodic restart check as the backstop for a moment when the other two could not act (login mode above all) - so the decision lives in one place rather
 * than being re-derived by each. Idleness rather than age is the condition, because the mark makes the browser useless for new tunes immediately while its running
 * captures are still worth finishing, so the earliest safe moment is exactly the moment the last thing depending on this browser lets go of it.
 *
 * The returned promise settles once the relaunch has settled, for the caller that goes on to acquire the fresh browser; a trigger with nothing waiting on the
 * outcome voids it. An early return resolves at once, because there is nothing for such a caller to wait for.
 * @returns A promise settling once the relaunch has settled, or at once when no relaunch runs.
 */
export async function restartBrowserIfImpairedAndIdle(): Promise<void> {

  if(supervisor.captureImpairment() === null) {

    return;
  }

  // A marked browser's restart belongs to this path, so a maintenance quiet period pending from before the mark is retired the first time any trigger observes the
  // mark - whether or not the browser is idle yet - rather than being left to fire against the browser the relaunch will have replaced.
  cancelRestartQuietTimer();

  if(!isBrowserIdleForRestart("impairment", readRestartFacts())) {

    return;
  }

  await executeBrowserRestart("impairment");
}

/**
 * Checks whether the browser qualifies for a restart. Called periodically by the restart check interval. The check skips when any of these conditions hold:
 * graceful shutdown in progress, login mode active, browser not ready. A marked browser is handed to the impairment path and the tick ends there. Otherwise the
 * tick drives the supervisor's health-gated governor reset and applies the maintenance rules: skip below the age threshold, cancel any pending quiet timer while
 * active streams exist (streams started during the quiet period reset the countdown), and otherwise start a quiet timer if one is not already running.
 * @param timers - The restart owner's registry, whose quiet period this check arms and reads. Handed in by the interval closure, so the check never reads the
 * nullable module binding and needs no null branch of its own.
 */
function checkBrowserRestart(timers: TimerRegistry): void {

  // Skip if the server is shutting down or login mode is active.
  if(gracefulShutdownInProgress || isLoginModeActive()) {

    return;
  }

  // Read the ready browser and its launch time from the supervisor. Both are non-null only in the ready state, so a single guard covers "no ready browser." The
  // launch time is read from the supervisor's clock (systemClock.now), so age is measured against that same clock rather than against any other time source.
  const browser = supervisor.current();
  const launchTime = supervisor.currentLaunchTime();

  if(!browser?.connected || (launchTime === null)) {

    return;
  }

  /* A marked browser waits for idleness rather than for age, so the maintenance rules below have nothing to say about it. It is not evidence of sustained health
   * either - the governor's reset waits for the fresh browser - so this tick neither feeds the health hold nor logs the recovery notice for it. The stream-end
   * trigger normally restarts it well before this tick runs; the tick is the backstop for a trigger that could not act, a login session active at the time above
   * all. The quiet period this tick would otherwise cancel below is retired inside the call, so the early return leaves no timer unobserved.
   */
  if(supervisor.captureImpairment() !== null) {

    void restartBrowserIfImpairedAndIdle();

    return;
  }

  // Health-gated governor reset. On every eligible tick, tell the supervisor the browser is still ready; once it has been continuously ready for the policy's
  // hold, this resets the relaunch governor to its normal state and returns true, so we log the recovery exactly once.
  if(supervisor.noteSustainedHealth()) {

    LOG.info("Browser capture readiness has been sustained; the relaunch governor has reset to its normal state.");
  }

  // Skip if the browser has not exceeded the maximum age.
  const age = systemClock.now() - launchTime;

  if(age < BROWSER_MAX_AGE) {

    return;
  }

  // If there are active streams, cancel any pending quiet timer and return. Streams that start during the quiet period reset the countdown.
  if(getStreamCount() > 0) {

    if(timers.has(RESTART_QUIET_KEY)) {

      LOG.debug("browser:lifecycle", "Browser restart quiet period cancelled - streams are active.");
    }

    cancelRestartQuietTimer();

    return;
  }

  // No active streams and the browser is old enough. Start the quiet timer if one is not already running.
  if(!timers.has(RESTART_QUIET_KEY)) {

    LOG.debug("browser:lifecycle", "Browser uptime exceeds threshold. Quiet period started - restart will proceed if no streams start within %s minutes.",
      Math.round(BROWSER_RESTART_QUIET_PERIOD / 60000));

    timers.setTimeout(RESTART_QUIET_KEY, () => {

      void executeBrowserRestart("maintenance");
    }, BROWSER_RESTART_QUIET_PERIOD);
  }
}

/**
 * Executes a browser restart: a final guard check, then the current instance is retired and torn down and a fresh one is launched in its place. The guard's
 * idleness test and the log line that announces the restart both depend on the cause; the teardown and relaunch sequence is shared by every cause.
 * @param cause - Why the restart is running, for the guard's idleness test and the announcement.
 */
async function executeBrowserRestart(cause: BrowserRestartCause): Promise<void> {

  // Retire any pending quiet period. For a maintenance restart this is the timer that just fired, and clearing a fired handle costs nothing. For an impairment
  // restart it may be a countdown started before the mark, which has to go: the browser it was measured against is about to be replaced, and letting it fire would
  // restart the fresh instance a second time.
  cancelRestartQuietTimer();

  // Final guard: re-check all preconditions. Conditions may have changed during the quiet period (e.g., a stream started just before the timer fired, login mode
  // was activated, or the browser disconnected on its own). Reading current()/currentLaunchTime() together keeps the ready-state check and the age source
  // consistent. The idleness question is asked for this restart's own cause, because what a maintenance sweep must defer to and what a repair must defer to are
  // not the same set of streams.
  const browser = supervisor.current();
  const launchTime = supervisor.currentLaunchTime();

  if(gracefulShutdownInProgress || isLoginModeActive() || !isBrowserIdleForRestart(cause, readRestartFacts()) || !browser?.connected || (launchTime === null)) {

    LOG.debug("browser:lifecycle", "Browser restart aborted - preconditions no longer met.");

    return;
  }

  const age = systemClock.now() - launchTime;
  const hours = Math.floor(age / 3600000);
  const minutes = Math.floor((age % 3600000) / 60000);

  if(cause === "impairment") {

    LOG.info("Restarting the browser because it can no longer start captures (uptime: %sh %sm).", hours, minutes);
  } else {

    LOG.info("Restarting browser for scheduled maintenance (uptime: %sh %sm).", hours, minutes);
  }

  try {

    // Retire the current instance from the lifecycle, tear it down, then acquire a fresh one through the supervisor. We do NOT touch the graceful-shutdown flag (the
    // server is not shutting down): closeBrowserInstance removes the disconnect listener, which is what makes this intentional teardown quiet. acquire() publishes
    // "ready" only after the readiness gate passes, so the completion log below is truthful: it verifies capture capability before claiming readiness, not mere liveness.
    supervisor.noteReadinessLost();

    // The restart swaps the whole Chrome session inside a living process, so the retiring session's page ids must not carry into the fresh one.
    clearPageTracking();

    // Readiness was relinquished first, because that is what supersedes an in-flight launch; publishing the teardown synchronously, before any await, then keeps the
    // launch window shut for the whole drain so nothing spawns a second Chrome against the profile lock this one still holds.
    const teardown = closeBrowserInstance(browser);

    supervisor.noteTeardownBegun(teardown, BROWSER_TEARDOWN_DRAIN_BOUND_MS);

    await teardown;

    /* The relaunch ends the browser session the channel-selection caches describe (guide row positions, discovered page URLs, watch URLs), exactly as the
     * disconnect path ends it, so they are cleared here for the same reason. The clear waits for the process to be gone rather than running beside the teardown,
     * because a page of the retiring session can still deliver a response event while Chrome drains - the Comcast channelmap listener writes its caches from
     * exactly such an event - and a clear issued ahead of the drain would be undone by it.
     */
    clearChannelSelectionCaches();

    // The preconditions were checked before the teardown, but a shutdown can begin during the seconds it takes, and relaunching then would spawn Chrome into a
    // dying process. Re-check on the far side of the await, for the same reason the guard above re-checks on the far side of the quiet period.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the shutdown path sets this while the teardown is awaited; TS cannot see that.
    if(gracefulShutdownInProgress) {

      LOG.debug("browser:lifecycle", "Browser restart relaunch declined because shutdown began while the previous instance was closing.");

      return;
    }

    // Launch a fresh browser instance so it is ready for the next stream request. The relaunch needs nothing but the browser itself, and the instance it replaces
    // may be refusing captures, so it acquires for a page.
    await getCurrentBrowser("page");

    // Bring the fresh window into agreement with the policy. A scheduled restart only runs against an idle registry, so this settles it minimized; a stream
    // arriving while the relaunch is in flight triggers its own sync during establishment.
    await syncWindowVisibility();

    LOG.info("Browser restart complete. Fresh instance is ready.");
  } catch(error) {

    LOG.error("Browser restart failed: %s.", formatError(error));
  }
}

/**
 * Starts the periodic browser restart eligibility check. This should be called once during server startup, after the browser is initialized. The check runs
 * indefinitely until stopBrowserRestartChecking() is called (typically during graceful shutdown). A second start while one is running changes nothing.
 * @param clock - The clock the check's interval and the quiet period it arms both run on. Defaults to the system clock.
 */
export function startBrowserRestartChecking(clock: Clock = systemClock): void {

  if(restartTimers) {

    return;
  }

  const timers = new TimerRegistry({ clock });

  timers.setInterval(RESTART_CHECK_KEY, () => { checkBrowserRestart(timers); }, BROWSER_RESTART_CHECK_INTERVAL);

  restartTimers = timers;
}

/**
 * Stops the periodic browser restart eligibility check. This should be called during graceful shutdown to prevent a restart from racing with server shutdown.
 * Disposing the registry drains the check and any pending quiet period together, and a stop with nothing running is a no-op.
 */
export function stopBrowserRestartChecking(): void {

  restartTimers?.dispose();
  restartTimers = null;
}

/* When running as a packaged executable (created by the `pkg` tool), the application is bundled into a single binary. Node modules like puppeteer-stream are
 * included in the bundle, but Chrome cannot load extensions from within the packaged binary - it needs actual files on the filesystem.
 *
 * To solve this, we extract the puppeteer-stream extension files to the application's data directory during startup. This happens only when process.pkg is
 * defined (indicating we're running as a packaged executable).
 *
 * The extracted files are:
 * - background.js: The extension's service worker that handles media capture
 * - manifest.json: The extension manifest declaring permissions and capabilities
 * - options.html/options.js: The extension's capture host. options.js defines START_RECORDING on the page's global scope, puppeteer-stream opens options.html as
 *   the extension page it resolves, the readiness handshake polls for START_RECORDING there, and every capture acquisition runs through it. The manifest also
 *   declares it as options_page.
 */

/**
 * Extracts the Puppeteer Stream extension files when running as a packaged executable. This copies the extension files from within the packaged binary to the
 * filesystem where Chrome can load them.
 *
 * When running from source (not packaged), this function does nothing - puppeteer-stream can load the extension directly from node_modules.
 * @throws If extension extraction fails.
 */
export async function prepareExtension(): Promise<void> {

  // Only needed when running as a packaged executable.
  if(!process.pkg) {

    return;
  }

  try {

    // The extension files are extracted to the extension directory within the data directory (ensured to exist before this function is called).
    const out = getExtensionDir();

    // Create the extension directory if it doesn't exist.
    try {

      await fsPromises.mkdir(out, { recursive: true });
    } catch(error) {

      LOG.error("Failed to create extension directory: %s.", formatError(error));

      throw error;
    }

    // The extension files that need to be extracted. These are the files from puppeteer-stream's extension directory.
    const files = [ "background.js", "manifest.json", "options.html", "options.js" ];

    for(const file of files) {

      try {

        // Copy each file from the packaged location (relative to the executable) to the data directory. The source path assumes the executable is in the
        // same directory as node_modules (which is how pkg packages the application).
        // eslint-disable-next-line no-await-in-loop
        await fsPromises.copyFile(
          path.join(path.dirname(process.execPath), "node_modules", "puppeteer-stream", "extension", file),
          path.join(out, file)
        );
      } catch(error) {

        LOG.error("Failed to copy extension file %s: %s.", file, formatError(error));

        throw error;
      }
    }

    LOG.debug("browser:lifecycle", "Extension files prepared successfully.");
  } catch(error) {

    LOG.error("Extension preparation failed: %s.", formatError(error));

    throw error;
  }
}

/* Re-export the capture acquisition for the streaming module. The browser directory owns every conversation with puppeteer-stream - index.ts launches it and
 * tabCapture.ts speaks its extension's protocol - so no other layer imports the library, and the streaming layer asks this module for a capture the same way it
 * asks it for a browser.
 */
export { acquireCaptureStream } from "./tabCapture.ts";
export type { CaptureStream, CaptureStreamOptions } from "./tabCapture.ts";
