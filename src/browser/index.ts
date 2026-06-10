/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Browser lifecycle management for PrismCast.
 */
import type { Browser, LaunchOptions, Page } from "puppeteer-core";
import { LOG, cancellableTimeout, delay, evaluateWithAbort, formatError, isProcessRunning, listProcesses, realClock, startTimer } from "../utils/index.ts";
import { clearLoginState, isLoginModeActive, setBrowserAccessors } from "./login.ts";
import { getAllStreams, getStreamCount } from "../streaming/registry.ts";
import { getChromeDataDir, getDataDir, getExtensionDir } from "../config/paths.ts";
import { getEffectivePreset, getPresetViewport } from "../config/presets.ts";
import { getExtensionPage, getStream, launch } from "puppeteer-stream";
import { getGpuCapabilities, setBrowserChrome, setGpuCapabilities, setMaxSupportedViewport } from "./display.ts";
import { resizeAndMinimizeWindow, unminimizeWindow } from "./cdp.ts";
import type { BrowserLifecycle } from "./browserSupervisor.ts";
import { CONFIG } from "../config/index.ts";
import type { GpuCapabilities } from "./display.ts";
import type { LaunchGovernorPolicy } from "./launchGovernor.ts";
import type { Nullable } from "../types/index.ts";
import type { ProcessInfo } from "../utils/index.ts";
import type { SystemStatus } from "../streaming/statusEmitter.ts";
import { clearChannelSelectionCaches } from "./channelSelection.ts";
import { createBrowserSupervisor } from "./browserSupervisor.ts";
import { emitSystemStatusChanged } from "../streaming/statusEmitter.ts";
import fs from "node:fs";
import path from "node:path";
import { launch as puppeteerLaunch } from "puppeteer-core";
import { setChromeUserAgent } from "../utils/index.ts";
import { startPrecaching } from "./precaching.ts";
import { terminateStream } from "../streaming/lifecycle.ts";

const { promises: fsPromises } = fs;

/* Global variables maintain the application's runtime state across all operations. We minimize global state where possible, but some values must be shared across
 * the application lifecycle:
 *
 * - supervisor: The browser capture-readiness supervisor. It is the single source of truth for the shared Chrome instance and its lifecycle (absent / launching /
 *   ready / degraded / trialing), so all streaming sessions use one Chrome process via supervisor.acquire(). It subsumes what used to be three scattered fields
 *   (the browser reference, its launch timestamp, and the launch mutex) into one discriminated-union state, and routes every relaunch through one loop-safe governor.
 *
 * - currentChromeVersion: The one piece of per-browser metadata the adapter still holds directly, captured when the browser becomes ready and surfaced by the
 *   health endpoint.
 *
 * - dataDir: The filesystem location for persistent data (Chrome profile, extension files). Resolved via config/paths.ts, which is created on startup if it doesn't
 *   exist.
 *
 * Stream tracking and ID generation live in streaming/registry.ts for unified stream management across all output types (HLS, MPEG-TS, etc.).
 */

// The Chrome version string (e.g., "Chrome/144.0.7559.110") captured when the browser becomes capture-ready in launchReadyBrowser. Cleared when the browser
// disconnects or is closed. This is the one piece of per-browser metadata the adapter still holds directly; the browser instance, its launch timestamp, and the
// launch mutex now live inside the supervisor's lifecycle state, which is the single source of truth for "what is the browser doing, and is it usable?".
let currentChromeVersion: Nullable<string> = null;

/* The browser relaunch governor's policy. Conservative and biased eager-for-the-first-failure: the first relaunch failures cost no cooldown (the common transient
 * recovers in seconds), and only repeated failures within the window trip the governor into an escalating cooldown. These named constants are lifted to recovery.*
 * configuration in a later increment; defining them here keeps the supervisor wiring complete and tunable in one place meanwhile.
 */
const RELAUNCH_POLICY: LaunchGovernorPolicy = {

  // Escalating cooldown ladder: 5 minutes, then 15, then 60. Each successive trip cools down for the next-longer duration; the final rung is the ceiling.
  cooldownLadderMs: [ 5 * 60 * 1000, 15 * 60 * 1000, 60 * 60 * 1000 ],

  // Trip into a cooldown after three failed relaunches within the window.
  failureThreshold: 3,

  // The five-minute window over which relaunch failures accrue toward a trip.
  failureWindowMs: 5 * 60 * 1000,

  // Two minutes of continuous capture-readiness resets the governor to its normal state, so a brief flap cannot wipe out accrued failures.
  healthHoldMs: 2 * 60 * 1000
};

/* The one browser capture-readiness supervisor for the process lifetime. It owns the lifecycle state (absent/launching/ready/degraded/trialing) that subsumes the
 * former currentBrowser + browserLaunchPromise + browserLaunchTime trio, and routes every relaunch through one loop-safe governor. The adapter injects the impure
 * ports: launchReadyBrowser (spawn Chrome and run the readiness gate), closeBrowserInstance (teardown), realClock.now (time), and onSupervisorStateChange (the loud
 * degraded alarm and the recovery notice). All browser access flows through it: getCurrentBrowser is acquire(); the non-launching reads derive from current() and
 * currentLaunchTime(). The injected ports are hoisted function declarations, so referencing them here is safe even though they are defined further down the module.
 */
const supervisor = createBrowserSupervisor({ close: closeBrowserInstance, launch: launchReadyBrowser, now: realClock.now, onStateChange: onSupervisorStateChange,
  policy: RELAUNCH_POLICY });

/**
 * Observes supervisor lifecycle transitions purely for operator-visible signals; it never affects the transition (the supervisor treats it as best-effort, so a
 * throwing logger cannot corrupt the lifecycle). It raises the loud degraded alarm when the relaunch governor trips, an info notice when capture readiness is
 * restored after a degraded period, and emits the SSE system status whenever a ready browser is published.
 * @param next - The state being entered.
 * @param previous - The state being left.
 */
function onSupervisorStateChange(next: BrowserLifecycle, previous: BrowserLifecycle): void {

  // The governor just tripped: relaunches are paused while the browser's capture system cools down. We log loudly at ERROR with the cooldown horizon so the
  // condition is never invisible - the enforceable form of "it is impossible to be silently un-tunable."
  if((next.kind === "degraded") && (previous.kind !== "degraded")) {

    const cooldownMinutes = Math.max(1, Math.round((next.until - realClock.now()) / 60000));

    LOG.error("The browser capture system has degraded and the relaunch governor has tripped: %s Relaunches are paused for approximately %d minute(s) while it " +
      "cools down; new stream requests will receive a 503 back-off until it recovers.", next.reason, cooldownMinutes);
  } else if((next.kind === "ready") && ((previous.kind === "trialing") || (previous.kind === "degraded"))) {

    // Capture readiness was restored by a successful trial after a degraded period. The ordinary first launch (absent -> launching -> ready) is intentionally
    // silent; only a recovery from trialing/degraded is worth an operator notice.
    LOG.info("The browser capture system has recovered and is serving captures again.");
  }

  // The browser's connectivity is part of the SSE system status, so emit when a ready browser is published. Readiness-loss emits are owned by handleBrowserDisconnect
  // (genuine disconnect) and the shutdown path; emitSystemStatusChanged dedupes, so a redundant emit is a cheap no-op.
  if(next.kind === "ready") {

    void emitCurrentSystemStatus();
  }
}

/* The capture-readiness probe is the capability tier of the launch gate: a real getStream against a throwaway page on the instance being launched - the authoritative
 * "can this browser actually capture?" predicate that the original outage proved must run at every (re)launch, not only boot. It lives in streaming/setup.ts (which
 * owns getStream and the unrecoverable stale-mutex process.exit precedent) and is injected here via setCaptureProbe, because setup.ts already depends on this module:
 * injecting the function rather than importing it keeps the dependency one-directional and breaks the cycle, mirroring the browserAccessors seam between login.ts and
 * index.ts. The probe must also take the local instance as a parameter rather than re-entering getCurrentBrowser, since launchReadyBrowser IS the in-flight launch -
 * re-entering acquire() would join its own pending promise and deadlock.
 */
type CaptureProbe = (browser: Browser) => Promise<void>;

/* The capture-readiness probe (capability tier of the launch gate). Null until streaming/setup.ts injects the real getStream probe at module load, which the import
 * order guarantees runs before any launch: app.ts imports the streaming layer, whose module bodies evaluate during import resolution, before startServer's warm-up.
 * launchReadyBrowser refuses to publish a browser if it is somehow still null (see the call site), rather than serving an unverified one.
 */
let captureProbe: Nullable<CaptureProbe> = null;

/**
 * Injects the capture-readiness probe used as the capability tier of the launch gate. Called once from streaming/setup.ts at module load (which always precedes any
 * launch, since the streaming layer is imported during server startup). Separating the wiring from the call keeps browser/index.ts free of a streaming-layer import.
 * @param probe - The probe to run against a freshly-launched browser; it resolves when the browser can capture and rejects otherwise (or exits the process for the
 *   unrecoverable stale-mutex case).
 */
export function setCaptureProbe(probe: CaptureProbe): void {

  captureProbe = probe;
}

// The data directory stores Chrome's profile data and the streaming extension files. Path resolution is centralized in config/paths.ts.

// The stale page cleanup interval handle, stored so we can clear it during graceful shutdown. The interval periodically checks for browser pages that are not
// associated with active streams and closes them to prevent resource exhaustion.
let stalePageCleanupInterval: Nullable<ReturnType<typeof setInterval>> = null;

/* Opportunistic browser restart state. Chrome accumulates memory pressure, GPU process issues, and general flakiness over multi-hour sessions with continuous
 * media playback. We proactively restart Chrome after it has been running for BROWSER_MAX_AGE, waiting for a quiet period with zero active streams before
 * executing the restart. After the restart, a fresh browser is launched immediately so it is ready for the next stream request.
 */

// Maximum browser uptime before considering a restart (6 hours).
const BROWSER_MAX_AGE = 6 * 60 * 60 * 1000;

// Duration of the quiet period (zero streams) required before executing the restart (5 minutes).
const BROWSER_RESTART_QUIET_PERIOD = 5 * 60 * 1000;

// How often to check whether the browser qualifies for a restart (30 seconds).
const BROWSER_RESTART_CHECK_INTERVAL = 30_000;

// Timer handle for the quiet period countdown. When set, the browser has exceeded BROWSER_MAX_AGE and we are waiting for BROWSER_RESTART_QUIET_PERIOD to
// elapse with zero active streams. Cancelled if a stream starts during the quiet period.
let restartQuietTimer: Nullable<ReturnType<typeof setTimeout>> = null;

// Interval handle for the periodic restart eligibility check.
let restartCheckInterval: Nullable<ReturnType<typeof setInterval>> = null;

// Flag indicating that the browser is being closed intentionally via closeBrowser(). When true, the disconnect handler skips error logging and stream termination
// since these are handled by the shutdown code path. This prevents false "unexpected disconnect" errors during graceful shutdown.
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

// Login mode management. State and functions live in login.ts; re-exported here so existing consumers don't need import path changes. clearLoginState,
// isLoginModeActive, and setBrowserAccessors are imported above; the first two for internal use, setBrowserAccessors for one-time initialization below.
export { clearLoginState, isLoginModeActive };
export { type LoginStatus, endLoginMode, getLoginPage, getLoginStatus, startLoginMode } from "./login.ts";

// Re-export the supervisor's acquire() rejection classes through the browser surface so the stream-setup layer can map them to a 503 back-off without reaching into
// the supervisor module directly. Both signal a transient "retry me" condition: BrowserUnavailableError while the relaunch governor is cooling, BrowserSupersededError
// when a launch was abandoned mid-flight by a readiness-loss.
export { BrowserSupersededError, BrowserUnavailableError } from "./browserSupervisor.ts";

// Inject browser accessors into the login module. This breaks the circular dependency (login needs getBrowserInstance/minimizeBrowserWindow, index needs login
// functions) using the same setter/getter pattern as setChromeUserAgent in chromeFetch.ts. Function declarations are hoisted, so both accessors are available here.
setBrowserAccessors({ getBrowserInstance, minimizeBrowserWindow });

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
 * Each registered page receives a unique ID that persists for the page's lifetime. This ID is used for comparison and staleness tracking, avoiding potential
 * issues with Page object reference identity.
 * @param page - The Puppeteer Page to register.
 */
export function registerManagedPage(page: Page): void {

  // Generate a unique ID for this page.
  const pageId = "page-" + String(++managedPageIdCounter);

  // Associate the Page object with its ID.
  pageToId.set(page, pageId);

  // Track the ID as managed.
  managedPageIds.add(pageId);
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
  // exists, before any subsequent step reads it. Future retirements add a single line to purgeLegacyArtifacts; the call site here does not change.
  await purgeLegacyArtifacts();
}

/* The retirement registry. Each entry documents one on-disk artifact that earlier PrismCast versions wrote and the current code no longer maintains, along with
 * the version that retired it and a short explanation of what replaced it. Adding a future retirement is a single-line append - the loop in purgeLegacyArtifacts
 * picks it up automatically and the structured metadata becomes part of the codebase's history. The list is data, not logic: the "what" and "why" of every
 * retirement is captured here in one place rather than scattered across cleanup call sites.
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
 * Identifies Chrome processes that this PrismCast instance is responsible for terminating. The filter has two stages: command-line discovery (anything using
 * our profile directory) followed by ownership verification (we spawned it, or its parent is no longer alive). The ownership stage is structural - we do not
 * rely on an in-memory flag; the parent-child relationship in the OS process table IS the ownership proof, which means this function is safe to call from any
 * code path, including from a rejected-duplicate startup's exit handler.
 *
 * Why ownership matters. A duplicate PrismCast that the instance guard rejects must NOT signal Chrome that belongs to the legitimate holder. With PID-based
 * cleanup the only defense was an in-memory ownsChromeCleanup flag set at startup. With process-table discovery, the OS itself tells us who owns what: a Chrome
 * whose ppid is a live unrelated PID belongs to that parent, not to us.
 * @param processes - The current process table snapshot.
 * @param profileDir - The Chrome user-data-dir to match against.
 * @param ownPid - The current process's PID (anything whose parent is us is ours to kill).
 * @param isProcessAlive - Predicate that returns true when the given PID is currently a live process. Injected so tests do not depend on real PIDs.
 * @returns The PIDs to terminate, in process-table order.
 */
export function findChromeProcessesUsingProfile(processes: readonly ProcessInfo[], profileDir: string, ownPid: number,
  isProcessAlive: (pid: number) => boolean): number[] {

  const target = "--user-data-dir=" + profileDir;

  return processes.filter((p): boolean => {

    // Stage 1: command-line match. Chrome puppeteer launches with --user-data-dir=<path> (equals form, no quotes). We also verify the character following
    // the path is whitespace or end-of-string, otherwise "/x/y" would match "--user-data-dir=/x/yz".
    const idx = p.commandLine.indexOf(target);

    if(idx === -1) {

      return false;
    }

    const after = p.commandLine.charAt(idx + target.length);

    if((after !== "") && (after !== " ") && (after !== "\t")) {

      return false;
    }

    // Stage 2: ownership verification. Kill if we spawned it (ppid is us) or if its parent is no longer alive (orphaned from a previous PrismCast that died).
    // Skip if its parent is a live unrelated PID - that process owns it, not us.
    return (p.ppid === ownPid) || !isProcessAlive(p.ppid);
  }).map((p) => p.pid);
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
 * The ownership filter makes this function safe to call from any context, including a rejected-duplicate startup's exit handler: a duplicate that never
 * spawned Chrome will find nothing matching its ownership criteria and signal nothing.
 *
 * Called at startup before launching the browser and from the process exit handler as a crash recovery fallback. Safe to call when no stale processes or files
 * exist - the discovery and lock-file cleanup are both no-ops in the empty case.
 */
export function killStaleChrome(): void {

  const profileDir = getChromeDataDir(CONFIG);
  const POLL_INTERVAL_MS = 200;
  const pidsToKill = findChromeProcessesUsingProfile(listProcesses(), profileDir, process.pid, isProcessRunning);

  for(const pid of pidsToKill) {

    try {

      // Send SIGTERM first to give Chrome a chance to flush its profile databases (LevelDB, extension state, session storage) before exiting. This is critical
      // when called from the process exit handler - Chrome may be running normally (e.g., after a capture probe timeout) and SIGKILL would corrupt its profile
      // databases, poisoning the Docker volume for subsequent restarts.
      process.kill(pid, "SIGTERM");

      LOG.debug("browser:lifecycle", "Sent SIGTERM to Chrome process %d.", pid);

      // Wait up to 5 seconds for Chrome to flush its databases and exit after SIGTERM. Containerized environments with software rendering and shared CPU may
      // need the full window.
      const TERM_WAIT_MS = 5000;

      if(!waitForChromeExit(pid, TERM_WAIT_MS, POLL_INTERVAL_MS)) {

        // SIGTERM didn't work. Escalate to SIGKILL. Orphaned Chrome processes (from a crashed parent or previous container) may not respond to SIGTERM.
        LOG.debug("browser:lifecycle", "Chrome did not exit after SIGTERM. Escalating to SIGKILL.");

        try {

          process.kill(pid, "SIGKILL");
        } catch(_error) {

          // ESRCH - Chrome exited between the poll check and the kill call.
        }

        const KILL_WAIT_MS = 2000;

        if(!waitForChromeExit(pid, KILL_WAIT_MS, POLL_INTERVAL_MS)) {

          LOG.warn("Chrome process %d did not exit after %dms of signal escalation. Proceeding anyway.", pid, TERM_WAIT_MS + KILL_WAIT_MS);
        }
      }
    } catch(error: unknown) {

      // ESRCH means the process does not exist - expected when there are no stale processes from a clean shutdown, or in Docker where the PID belongs to a
      // previous container's PID namespace.
      if((error as NodeJS.ErrnoException).code !== "ESRCH") {

        LOG.warn("Failed to signal Chrome process %d: %s.", pid, formatError(error));
      }
    }
  }

  // Remove stale lock and port files left behind by an unclean Chrome exit.
  cleanStaleProfileFiles(profileDir);
}

/**
 * Polls until the Chrome process with the given PID has exited, or the timeout expires. Uses process.kill(pid, 0) to check process existence - throws ESRCH
 * when the process is gone. Between polls, sleeps synchronously using Atomics.wait() for cross-platform compatibility.
 * @param pid - The Chrome process ID to wait for.
 * @param timeoutMs - Maximum time to wait in milliseconds.
 * @param pollIntervalMs - Time between existence checks in milliseconds.
 * @returns True if the process exited within the timeout, false otherwise.
 */
function waitForChromeExit(pid: number, timeoutMs: number, pollIntervalMs: number): boolean {

  const deadline = Date.now() + timeoutMs;

  while(Date.now() < deadline) {

    if(!isProcessRunning(pid)) {

      return true;
    }

    syncSleep(pollIntervalMs);
  }

  return !isProcessRunning(pid);
}

/**
 * Removes stale Chrome profile lock files and the DevTools port file. Chrome writes these while running and removes them on clean shutdown, but an unclean exit
 * (container kill, SIGKILL, crash) leaves them behind. Stale lock files prevent Chrome from acquiring the profile, and a stale DevToolsActivePort can confuse the
 * Puppeteer connection.
 * @param profileDir - The Chrome user data directory path.
 */
function cleanStaleProfileFiles(profileDir: string): void {

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
 * from their dist at node_modules/puppeteer-stream/dist/PuppeteerStream.js (the extensionId constant). We use it below in --allowlisted-extension-id to work around
 * the broken activeTab grant path introduced in puppeteer-stream 3.0.22...see the "--allowlisted-extension-id" entry in buildLaunchOptions for the full rationale.
 */
const PUPPETEER_STREAM_EXTENSION_ID = "jjndjgheafjngoipoacpjgeicjeomjli";

/**
 * Assembles the configuration options for launching Chrome with Puppeteer. These options are critical for reliable streaming:
 *
 * - Chrome flags configure the browser for unattended video playback without user interaction
 * - Ignored default args prevent Puppeteer from disabling features we need (extensions, audio, component updates)
 * - A persistent user data directory retains cookies and login state across restarts
 * - Pipe mode provides a faster, more reliable connection than WebSocket
 * @returns Puppeteer launch options.
 */
export function buildLaunchOptions(): LaunchOptions {

  return {

    /* Chrome command-line arguments. Each flag serves a specific purpose for reliable streaming:
     *
     * --allow-running-insecure-content: Some streaming sites serve mixed HTTP/HTTPS content. Without this flag, the browser blocks HTTP resources on HTTPS
     *   pages, which can break video players that load some assets over HTTP.
     *
     * --allowlisted-extension-id=<extension-id>: Restores Chrome's global allowlist for puppeteer-stream's capture extension. puppeteer-stream 3.0.22 stopped
     *   injecting this flag in favor of granting activeTab via a synthetic Cmd/Ctrl+Shift+Y keystroke, but CDP-synthesized keystrokes do not satisfy
     *   chrome.commands under automation (the renderer sees the event but the browser-process accelerator dispatcher does not), so the new flow fails to grant
     *   activeTab and capture is denied at the API level (see github.com/Flam3rboy/puppeteer-stream/issues/206). Re-emitting the flag ourselves bypasses the
     *   broken activeTab path. Remove this entry and the PUPPETEER_STREAM_EXTENSION_ID constant when issue #206 closes with a real fix.
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
     *
     * --window-size: Sets the initial window size to match our configured viewport dimensions. This is later adjusted via CDP to account for browser chrome.
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
      "--no-first-run",
      "--window-size=" + String(getPresetViewport(CONFIG).width) + "," + String(getPresetViewport(CONFIG).height)
    ],

    // Disable Puppeteer's default viewport constraints. We manage viewport sizing ourselves via CDP to account for browser chrome (toolbars, borders) and
    // ensure the content area matches our target dimensions exactly.
    defaultViewport: null,

    // Path to the Chrome executable, either from environment variable or autodetected.
    executablePath: getExecutablePath(),

    // Run Chrome in headed (visible) mode, not headless. The puppeteer-stream extension requires a visible browser window to capture the screen. We minimize
    // the window after launch to reduce GPU usage while still allowing capture.
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

    // Use pipe mode for browser communication instead of WebSocket. Pipe mode is faster and more reliable, especially under load. It uses stdin/stdout for
    // the DevTools Protocol connection rather than a network socket.
    pipe: true,

    // Persistent user data directory for Chrome profile. This directory stores cookies, local storage, and other session data. By persisting this across
    // restarts, sites remember login state and don't require re-authentication.
    userDataDir: getChromeDataDir(CONFIG)
  };
}

/**
 * Custom launch function that modifies Chrome arguments when running as a packaged executable. The packaged version cannot load extensions from node_modules
 * (which is bundled inside the executable), so we point the extension paths to our extracted extension files in the data directory.
 * @param opts - The launch options to modify.
 * @returns The launched browser instance.
 */
async function launchWithCustomArgs(opts: LaunchOptions): Promise<Browser> {

  // When running as a packaged executable (process.pkg is set by the pkg bundler), we need to replace the extension paths. The puppeteer-stream library adds
  // --load-extension and --disable-extensions-except arguments pointing to node_modules, but these paths don't exist in the packaged executable. We replace
  // them with paths to our extracted extension files.
  if(process.pkg) {

    const extensionPath = getExtensionDir(CONFIG);

    // Remove any existing extension arguments and add our own pointing to the extracted extension.
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
 * Detects the maximum supported viewport dimensions based on the user's display. This function measures the available screen space and subtracts browser chrome to
 * determine the largest viewport we can use for video capture.
 *
 * The detection uses a temporary page (or existing page if available) to evaluate screen dimensions via JavaScript. The result is cached in the display module for
 * use by the preset system when determining effective viewport.
 * @param browser - The browser instance to use for detection.
 */
async function detectDisplayDimensions(browser: Browser): Promise<void> {

  let tempPage: Nullable<Page> = null;
  let usingTempPage = false;

  try {

    // Try to use an existing page first to avoid window activation issues on macOS.
    const existingPages = await browser.pages();
    let targetPage: Nullable<Page> = existingPages.find((p) => !p.isClosed()) ?? null;

    if(!targetPage) {

      tempPage = await browser.newPage();
      usingTempPage = true;
      targetPage = tempPage;
    }

    // Ensure the window is in normal state before measuring. Chrome restores window state from the persistent user data directory, so after a scheduled browser
    // restart the window may launch minimized. A minimized window reports outerWidth/outerHeight as 0 while innerWidth/innerHeight retains the viewport dimensions,
    // producing negative chrome measurements that poison all subsequent window sizing.
    await unminimizeWindow(targetPage);

    // Measure display dimensions and browser chrome via JavaScript. The measurement is retried if chrome dimensions are negative, which indicates the macOS window
    // manager has not yet finished the minimize-to-normal state transition (the animation is asynchronous relative to the CDP command).
    let dimensions: { availHeight: number; availWidth: number; chromeHeight: number; chromeWidth: number } | undefined;

    for(let attempt = 0; attempt < 3; attempt++) {

      // eslint-disable-next-line no-await-in-loop
      dimensions = await evaluateWithAbort(targetPage, (): { availHeight: number; availWidth: number; chromeHeight: number; chromeWidth: number } => {

        return {

          // Available screen dimensions (excludes taskbar, dock, menu bar).
          availHeight: screen.availHeight,
          availWidth: screen.availWidth,

          // Browser chrome dimensions (title bar, toolbar, borders).
          chromeHeight: window.outerHeight - window.innerHeight,
          chromeWidth: window.outerWidth - window.innerWidth
        };
      });

      if((dimensions.chromeWidth >= 0) && (dimensions.chromeHeight >= 0)) {

        break;
      }

      // Chrome dimensions are negative - the window manager is still transitioning. Wait briefly and remeasure.
      if(attempt < 2) {

        LOG.debug("browser:lifecycle", "Display detection measured negative chrome dimensions (%s\u00d7%s, attempt %s). Retrying after window state settles.",
          dimensions.chromeWidth, dimensions.chromeHeight, attempt + 1);

        // eslint-disable-next-line no-await-in-loop
        await delay(100);
      }
    }

    // If all attempts produced negative chrome dimensions, skip caching to avoid poisoning window sizing. The preset system will use the configured preset without
    // degradation, and resizeAndMinimizeWindow will fall back to measuring chrome dimensions via page.evaluate() on each call.
    if(!dimensions) {

      return;
    }

    if((dimensions.chromeWidth < 0) || (dimensions.chromeHeight < 0)) {

      LOG.warn("Display detection produced invalid chrome dimensions after 3 attempts (%s\u00d7%s). Window sizing may be incorrect.",
        dimensions.chromeWidth, dimensions.chromeHeight);

      return;
    }

    // Calculate maximum viewport: available screen space minus browser chrome.
    const maxWidth = dimensions.availWidth - dimensions.chromeWidth;
    const maxHeight = dimensions.availHeight - dimensions.chromeHeight;

    // Cache the results for use by the preset system and window sizing.
    setBrowserChrome(dimensions.chromeWidth, dimensions.chromeHeight);
    setMaxSupportedViewport(maxWidth, maxHeight);

    LOG.debug("browser:lifecycle", "Display detection complete: screen %s\u00d7%s, chrome %s\u00d7%s, max viewport %s\u00d7%s.",
      dimensions.availWidth, dimensions.availHeight,
      dimensions.chromeWidth, dimensions.chromeHeight,
      maxWidth, maxHeight);

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
        // comma-separated field. For non-ANGLE renderers, use the device string from CDP.
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

    // Check if the configured preset needs to be degraded and warn the user.
    const presetResult = getEffectivePreset(CONFIG);

    if(presetResult.degraded && presetResult.maxViewport) {

      LOG.warn("Display supports maximum %s\u00d7%s. Configured %s preset will use %s instead.",
        presetResult.maxViewport.width, presetResult.maxViewport.height,
        presetResult.configuredPreset.id, presetResult.effectivePreset.id);
    }
  } catch(error) {

    LOG.warn("Display detection failed: %s. Preset degradation will not be available.", formatError(error));
  } finally {

    // Clean up temporary page if we created one.
    if(usingTempPage && tempPage) {

      try {

        await tempPage.close();
      } catch(_closeError) {

        // Ignore close errors.
      }
    }
  }
}

/**
 * Handles browser disconnection events by cleaning up all active streams and resetting browser state. This function is called when the browser crashes, is closed
 * manually, or loses its connection for any other reason.
 *
 * When the browser disconnects, all pages (tabs) within it are immediately invalid and cannot be used. Any active streams using those pages will fail if they try
 * to interact with them. We proactively clean up by:
 * 1. Telling the supervisor readiness was lost (it transitions to absent), so the next stream request launches a fresh browser through the gate and governor
 * 2. Stopping all health monitors (they would fail trying to check page state)
 * 3. Removing all entries from activeStreams (the pages are gone)
 *
 * This ensures streams fail gracefully rather than hanging indefinitely trying to use closed pages. It runs only for a genuine, unsolicited disconnect; every
 * intentional teardown removes this listener via closeBrowserInstance first.
 */
function handleBrowserDisconnect(): void {

  // Readiness is lost. Tell the supervisor first: this supersedes any launch in flight, clears the governor's health anchor, and transitions the lifecycle to
  // absent, so getCurrentBrowser() relaunches through the gate and governor on the next call. This handler only ever runs for a genuine, unsolicited disconnect -
  // closeBrowserInstance removes this listener before every intentional teardown, so a scheduled restart or orphan close never reaches here.
  supervisor.noteReadinessLost();

  // Clear the adapter-held Chrome version and the cached user agent so stale values are not served between now and the next ready browser. We do not track Chrome's
  // PID directly - killStaleChrome discovers orphans via the OS process table on the next startup, which removes a class of state that could go stale.
  currentChromeVersion = null;
  setChromeUserAgent(null);

  // Cancel any pending restart quiet timer since the browser is already gone.
  if(restartQuietTimer) {

    clearTimeout(restartQuietTimer);
    restartQuietTimer = null;
  }

  // Clear all channel selection caches. Cached state (guide row positions, discovered page URLs) may be stale in a new browser session.
  clearChannelSelectionCaches();

  // Clear login state if login mode was active. We use clearLoginState() rather than endLoginMode() because the browser is already gone and we don't want to
  // attempt any browser operations (page close, window minimize).
  if(clearLoginState() && !gracefulShutdownInProgress) {

    LOG.info("Login mode ended due to browser disconnect.");
  }

  // Only log the error for unexpected disconnects. During graceful shutdown, closeBrowser() set the flag and this disconnect is intentional.
  if(!gracefulShutdownInProgress) {

    LOG.error("Browser disconnected unexpectedly. All active streams will be terminated.");
  }

  // Clean up all active streams using the authoritative terminateStream function for consistent cleanup. This is kept even during graceful shutdown as a defensive
  // measure - terminateStream() is idempotent, so if streams were already terminated by the caller, this harmlessly iterates an empty array.
  const streams = getAllStreams();

  for(const streamInfo of streams) {

    terminateStream(streamInfo.id, streamInfo.info.storeKey, "browser disconnect");
  }

  // Emit system status after stream cleanup. Skip during graceful shutdown since no clients are listening and the process is exiting.
  if(!gracefulShutdownInProgress) {

    void emitCurrentSystemStatus();
  }
}

/**
 * Provides access to the capture-ready browser, launching one if needed. This is the single gated entry point for all browser access: it delegates to the
 * supervisor's acquire(), which returns the ready browser, joins an in-flight launch (single-flight, so concurrent callers never contend on Chrome's profile lock),
 * lazily launches when absent, or - while the relaunch governor is cooling after repeated failures - rejects fast with a BrowserUnavailableError WITHOUT spawning
 * Chrome (the loop bound). The launch it drives runs the readiness gate, so a returned browser is verified capture-ready, not merely connected.
 * @returns The capture-ready browser instance.
 * @throws BrowserUnavailableError while the governor is cooling, BrowserSupersededError if an in-flight launch was abandoned by a readiness-loss, or the underlying
 *   launch error when a launch attempt fails.
 */
export async function getCurrentBrowser(): Promise<Browser> {

  return supervisor.acquire();
}

/**
 * The supervisor's `launch` port: spawns Chrome, runs the readiness gate, performs post-launch initialization (display detection, version/UA capture, precaching),
 * and resolves ONLY with a capture-ready browser. It builds into a local instance and publishes nothing - the supervisor owns publication and transitions to
 * "ready" only after this resolves. A launch that fails the gate tears down its own Chrome here and throws, so a broken instance is never handed up; the supervisor
 * counts the failure and decides whether to relaunch immediately or cool down. This is the fix for the original outage, where a failed extension load was logged as
 * a warning and the broken browser served anyway.
 * @returns The capture-ready browser instance.
 * @throws If the launch or the readiness gate fails.
 */
async function launchReadyBrowser(): Promise<Browser> {

  const browserElapsed = startTimer();

  // The launch function from puppeteer-stream wraps standard Puppeteer launch to inject the streaming extension. We pass our custom launch function that handles
  // packaged-executable extension paths. This happens on first stream request, after a browser crash, during server warmup, or during a governed relaunch.
  const browser = await launch({ launch: launchWithCustomArgs }, buildLaunchOptions());

  try {

    LOG.debug("timing:browser", "Chrome process spawned. (+%sms)", browserElapsed());

    // Readiness gate, handshake tier (cheap, on-suspicion). Poll for the puppeteer-stream extension to finish initializing - it injects a START_RECORDING function
    // into its options page context, so its presence is the extension's own readiness signal. We poll rather than fixed-delay so the browser is ready as soon as the
    // extension loads (typically 200-500ms). On failure this THROWS rather than warning-and-proceeding: an unregistered extension means chrome.tabs is undefined and
    // every getStream() would hang, so the instance is not capture-ready and must not be published. We reclassify the raw waitForFunction timeout into a
    // capture-infrastructure error carrying "timed out" so the setup layer maps it to a 503 back-off (the same as the capability-tier probe failure), rather than a
    // 500 the client would not back off from - an unregistered extension is a capture-infrastructure fault, and a fresh relaunch usually clears it.
    try {

      const extensionPage = await getExtensionPage(browser);

      await extensionPage.waitForFunction("typeof START_RECORDING === 'function'", { timeout: CONFIG.browser.initTimeout });
    } catch(handshakeError) {

      throw new Error("The capture extension handshake timed out after " + String(CONFIG.browser.initTimeout) + " ms.", { cause: handshakeError });
    }

    LOG.debug("timing:browser", "Extension initialized. (+%sms)", browserElapsed());

    // Readiness gate, capability tier (the authoritative arbiter). Run the injected capture probe - a real getStream against a throwaway page on THIS instance - so
    // "ready" means "really captured," not merely "the extension handshake responded." This is the predicate the original outage proved must run at every (re)launch:
    // it exercises the exact getStream path that hangs when the extension is unregistered. A probe failure throws, so the supervisor counts the launch failure and the
    // browser is never published; the unrecoverable stale-mutex case exits the process from inside the probe, since a Chrome restart cannot fix a leaked module mutex.
    //
    // If the probe is not wired (the seam left unset by a refactor - impossible in the normal import order, which always wires it before any launch), we reject the
    // launch rather than publish a handshake-only browser: serving an unverified browser would be the "proceed and hope" path this design eliminates. The supervisor
    // counts the rejected launch and, on repetition, degrades loudly.
    if(!captureProbe) {

      throw new Error("The capture-readiness probe is not wired; refusing to publish a browser whose capture capability was not verified.");
    }

    await captureProbe(browser);

    LOG.debug("timing:browser", "Capture probe complete. (+%sms)", browserElapsed());

    // Detect display dimensions to determine the maximum supported viewport. This must happen before streaming so the preset system can degrade to a smaller preset
    // if needed.
    await detectDisplayDimensions(browser);

    LOG.debug("timing:browser", "Display detection complete. (+%sms)", browserElapsed());

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

    // Start background precaching of selected service channel lineups. Fire-and-forget - the setTimeout inside startPrecaching() defers the work until after this
    // launch settles and the supervisor has published the ready browser, so its getCurrentBrowser() resolves immediately rather than re-entering this launch.
    startPrecaching();

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
 * Checks if the browser is currently connected and usable. This is a synchronous check that can be used before attempting browser operations.
 * @returns True if the browser is connected and ready for use, false otherwise.
 */
export function isBrowserConnected(): boolean {

  const browser = supervisor.current();

  return !!browser && browser.connected;
}

/**
 * Resizes the browser window to the effective viewport and minimizes it. This function combines viewport sizing with minimization to ensure the window is
 * properly sized before being minimized. The resize uses the effective viewport from getEffectiveViewport(), which accounts for display size constraints and
 * preset degradation.
 *
 * To avoid issues with creating temporary pages (which can cause the window to restore on macOS), we prefer using an existing page if one is available. Only if
 * no pages exist do we create a temporary page.
 */
export async function minimizeBrowserWindow(): Promise<void> {

  // Guard against calling this when no browser is running.
  const browser = supervisor.current();

  if(!browser?.connected) {

    return;
  }

  let tempPage: Nullable<Page> = null;
  let usingTempPage = false;

  try {

    // Try to use an existing page first. Creating a new page can cause the window to restore/activate on macOS, which defeats the purpose of minimizing.
    const existingPages = await browser.pages();
    let targetPage: Nullable<Page> = existingPages.find((p) => !p.isClosed()) ?? null;

    // If no existing pages, we must create a temporary one. This is less ideal but necessary to get a CDP session target.
    if(!targetPage) {

      tempPage = await browser.newPage();
      usingTempPage = true;

      // Register the temp page so stale cleanup knows it's ours.
      registerManagedPage(tempPage);
      targetPage = tempPage;
    }

    // Delegate to resizeAndMinimizeWindow for the actual CDP operations. This ensures consistent resize+minimize behavior and maintains a single source of
    // truth for the viewport sizing logic.
    await resizeAndMinimizeWindow(targetPage);

    // Clean up the temporary page if we created one.
    if(usingTempPage && tempPage) {

      unregisterManagedPage(tempPage);

      await tempPage.close();
    }
  } catch(error) {

    // If we created a temp page, make sure to unregister it even on error.
    if(usingTempPage && tempPage) {

      unregisterManagedPage(tempPage);

      try {

        await tempPage.close();
      } catch(_closeError) {

        // Ignore close errors during error handling.
      }
    }

    // Resizing/minimizing is not critical - log a warning but don't fail the operation.
    LOG.debug("browser:lifecycle", "Could not resize and minimize browser window: %s.", formatError(error));
  }
}

/**
 * Gets all open browser pages (tabs). This is used by the health check endpoint to report page count and by stale page cleanup to find orphaned pages.
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
 * - browser.close() sends CDP Browser.close and waits for WebSocket teardown, which hangs 3-5 seconds even after Chrome exits.
 * - browser.disconnect() drops the WebSocket instantly but orphans Chrome as a Node child process, creating a zombie that process.kill(pid, 0) cannot detect.
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

    const TERM_WAIT_MS = 5000;
    const KILL_WAIT_MS = 2000;

    // Listen for the exit event before sending the signal. The event fires after the OS reaps the process, so there is no zombie window. Resolves to true so
    // Promise.race can distinguish exit from timeout.
    const { promise: exitPromise, resolve: signalExit } = Promise.withResolvers<true>();

    chromeProcess.on("exit", () => { signalExit(true); });

    chromeProcess.kill("SIGTERM");

    LOG.debug("browser:lifecycle", "Sent SIGTERM to Chrome process %d.", chromeProcess.pid);

    // Wait for Chrome to exit after SIGTERM, with a timeout. If Chrome doesn't exit in time, escalate to SIGKILL.
    const termTimeout = cancellableTimeout(TERM_WAIT_MS);

    const exitedAfterTerm = await Promise.race([ exitPromise, termTimeout.promise ]);

    termTimeout.cancel();

    if(!exitedAfterTerm) {

      // SIGTERM didn't work within the timeout. Escalate to SIGKILL. Orphaned Chrome processes (from a crashed parent or previous container) may not
      // respond to SIGTERM.
      LOG.debug("browser:lifecycle", "Chrome did not exit after SIGTERM. Escalating to SIGKILL.");

      chromeProcess.kill("SIGKILL");

      const killTimeout = cancellableTimeout(KILL_WAIT_MS);

      await Promise.race([ exitPromise, killTimeout.promise ]);

      killTimeout.cancel();
    }
  }

  // Disconnect the Puppeteer WebSocket after Chrome has exited. This cleans up Puppeteer's internal state (event listeners, pending CDP calls) without waiting for
  // the WebSocket close handshake to complete on a dead connection. We catch the rejection: disconnect() on a connection whose underlying transport already died of
  // an unclean Chrome exit can reject, and an unhandled rejection on this fire-and-forget call would crash the process during an otherwise-successful teardown.
  if(browser.connected) {

    browser.disconnect().catch((error: unknown) => {

      LOG.debug("browser:lifecycle", "Ignoring browser disconnect error during teardown: %s.", formatError(error));
    });
  }

  // Remove stale Chrome profile lock files left behind by the disconnected browser. No per-PID state to clear here - killStaleChrome on the next launch will
  // discover any leftover Chrome via the OS process table.
  cleanStaleProfileFiles(getChromeDataDir(CONFIG));
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

  currentChromeVersion = null;
  setChromeUserAgent(null);

  if(!browser) {

    return;
  }

  await closeBrowserInstance(browser);
}

/* Over time, browser pages (tabs) may accumulate if cleanup fails during stream termination. This can happen due to race conditions, errors during cleanup, or
 * edge cases in stream lifecycle management. Each orphaned page consumes memory and may continue running JavaScript, so we periodically clean them up.
 *
 * The cleanup has several safeguards to prevent closing pages that shouldn't be closed:
 *
 * 1. Only managed pages: We only consider pages that PrismCast created (tracked in managedPageIds). Pages opened manually by the user for debugging, or pages opened
 *    by streaming sites (OAuth popups, etc.) are left alone.
 *
 * 2. Target ID comparison: We use target IDs (strings) instead of Page object references for comparison. Puppeteer may return different wrapper objects for the
 *    same underlying page, making reference comparison unreliable.
 *
 * 3. Grace period: Pages must be observed as potentially stale for a configurable grace period before being closed. This handles race conditions where pages are
 *    briefly untracked during stream initialization or cleanup.
 *
 * 4. Minimum page preservation: We always keep at least one page open to prevent Chrome from exiting.
 */

/**
 * Cleans up browser pages that are not associated with active streams. This function runs periodically to catch any pages that were not properly closed during
 * stream termination.
 *
 * The cleanup uses a multi-stage filtering process:
 * 1. Only consider pages we created (in managedPageIds)
 * 2. Exclude pages associated with active streams
 * 3. Apply a grace period before closing (to handle race conditions)
 * 4. Preserve at least one page to keep the browser alive
 */
export async function cleanupStalePages(): Promise<void> {

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

    const now = Date.now();

    const gracePeriod = CONFIG.recovery.stalePageGracePeriod;

    // Build a list of pages that are candidates for cleanup. A page is a candidate if:
    // - It has a managed page ID (was created by PrismCast)
    // - It is not associated with any active stream
    // - It has been stale for longer than the grace period
    const candidatePages: { page: Page; pageId: string }[] = [];

    // Track which managed page IDs we've seen in the current browser pages. Used for cleanup of stale tracking data.
    const currentManagedIds = new Set<string>();

    for(const page of pages) {

      const pageId = getManagedPageId(page);

      // Skip pages we didn't create. This preserves manually opened pages and site popups.
      if(!pageId) {

        continue;
      }

      currentManagedIds.add(pageId);

      // Skip pages associated with active streams.
      if(activePageIds.has(pageId)) {

        // If this page was previously marked as potentially stale, remove it from tracking since it's now active.
        potentiallyStalePages.delete(pageId);

        continue;
      }

      // This page is potentially stale. Track when we first observed it as such.
      if(!potentiallyStalePages.has(pageId)) {

        potentiallyStalePages.set(pageId, now);

        // Don't close it yet - wait for the grace period.
        continue;
      }

      // Check if the grace period has elapsed.
      const firstSeenStale = potentiallyStalePages.get(pageId) ?? now;

      if((now - firstSeenStale) < gracePeriod) {

        // Grace period hasn't elapsed yet. Leave this page alone for now.
        continue;
      }

      // This page has been stale for longer than the grace period. It's a candidate for cleanup.
      candidatePages.push({ page, pageId });
    }

    // Clean up the potentiallyStalePages map by removing entries for pages that no longer exist. This handles cases where pages were closed by other means.
    for(const trackedId of potentiallyStalePages.keys()) {

      if(!currentManagedIds.has(trackedId)) {

        potentiallyStalePages.delete(trackedId);
      }
    }

    // Calculate how many pages we can close while still keeping at least one page open.
    const maxToClose = Math.max(0, pages.length - 1 - activePageIds.size);

    const pagesToClose = candidatePages.slice(0, maxToClose);

    let closedCount = 0;

    for(const { page, pageId } of pagesToClose) {

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

    // Cleanup failure is not critical - log a warning and try again next interval.
    LOG.debug("browser:lifecycle", "Stale page cleanup failed: %s.", formatError(error));
  }
}

/**
 * Starts the periodic stale page cleanup interval. This should be called once during server startup, after the browser is initialized. The interval runs
 * indefinitely until stopStalePageCleanup() is called (typically during graceful shutdown).
 */
export function startStalePageCleanup(): void {

  stalePageCleanupInterval = setInterval(() => { void cleanupStalePages(); }, CONFIG.recovery.stalePageCleanupInterval);
}

/**
 * Stops the stale page cleanup interval. This should be called during graceful shutdown to prevent the cleanup from running after we've started shutting down
 * the browser and streams.
 */
export function stopStalePageCleanup(): void {

  if(stalePageCleanupInterval) {

    clearInterval(stalePageCleanupInterval);

    stalePageCleanupInterval = null;
  }
}

/* Opportunistic browser restart functions. The check runs on a 30-second interval and, when the browser exceeds BROWSER_MAX_AGE with zero active streams, starts
 * a quiet period timer. The quiet timer is cancelled if a stream starts, ensuring active viewers are never disrupted. When the timer expires, the browser is
 * closed and immediately re-launched.
 */

/**
 * Checks whether the browser qualifies for an opportunistic restart. Called periodically by the restart check interval. The check skips when any of these
 * conditions hold: graceful shutdown in progress, login mode active, browser not ready, browser age below threshold. On every eligible tick it also drives the
 * supervisor's health-gated governor reset. If active streams exist, any pending quiet timer is cancelled (streams started during the quiet period reset the
 * countdown). Otherwise a quiet timer is started if one is not already running.
 */
function checkBrowserRestart(): void {

  // Skip if the server is shutting down or login mode is active.
  if(gracefulShutdownInProgress || isLoginModeActive()) {

    return;
  }

  // Read the ready browser and its launch time from the supervisor. Both are non-null only in the ready state, so a single guard covers "no ready browser." The
  // launch time is on the supervisor's clock (realClock.now), so age must be measured against the same clock - not Date.now() - or the units would not match.
  const browser = supervisor.current();
  const launchTime = supervisor.currentLaunchTime();

  if(!browser?.connected || (launchTime === null)) {

    return;
  }

  // Health-gated governor reset. On every eligible tick, tell the supervisor the browser is still ready; once it has been continuously ready for the policy's
  // hold, this resets the relaunch governor to its normal state and returns true, so we log the recovery exactly once.
  if(supervisor.noteSustainedHealth()) {

    LOG.info("Browser capture readiness has been sustained; the relaunch governor has reset to its normal state.");
  }

  // Skip if the browser has not exceeded the maximum age.
  const age = realClock.now() - launchTime;

  if(age < BROWSER_MAX_AGE) {

    return;
  }

  // If there are active streams, cancel any pending quiet timer and return. Streams that start during the quiet period reset the countdown.
  if(getStreamCount() > 0) {

    if(restartQuietTimer) {

      LOG.debug("browser:lifecycle", "Browser restart quiet period cancelled - streams are active.");

      clearTimeout(restartQuietTimer);
      restartQuietTimer = null;
    }

    return;
  }

  // No active streams and the browser is old enough. Start the quiet timer if one is not already running.
  if(!restartQuietTimer) {

    LOG.debug("browser:lifecycle", "Browser uptime exceeds threshold. Quiet period started - restart will proceed if no streams start within %s minutes.",
      Math.round(BROWSER_RESTART_QUIET_PERIOD / 60000));

    restartQuietTimer = setTimeout(() => {

      void executeBrowserRestart();
    }, BROWSER_RESTART_QUIET_PERIOD);
  }
}

/**
 * Executes the opportunistic browser restart after the quiet period has elapsed. Performs a final guard check before proceeding, then closes the browser and
 * immediately re-launches a fresh instance.
 */
async function executeBrowserRestart(): Promise<void> {

  // Clear the timer handle.
  restartQuietTimer = null;

  // Final guard: re-check all preconditions. Conditions may have changed during the quiet period (e.g., a stream started just before the timer fired, login mode
  // was activated, or the browser disconnected on its own). Reading current()/currentLaunchTime() together keeps the ready-state check and the age source consistent.
  const browser = supervisor.current();
  const launchTime = supervisor.currentLaunchTime();

  if(gracefulShutdownInProgress || isLoginModeActive() || (getStreamCount() > 0) || !browser?.connected || (launchTime === null)) {

    LOG.debug("browser:lifecycle", "Browser restart aborted - preconditions no longer met.");

    return;
  }

  const age = realClock.now() - launchTime;
  const hours = Math.floor(age / 3600000);
  const minutes = Math.floor((age % 3600000) / 60000);

  LOG.info("Restarting browser for scheduled maintenance (uptime: %sh %sm).", hours, minutes);

  try {

    // Retire the current instance from the lifecycle, tear it down, then acquire a fresh one through the supervisor. We do NOT touch the graceful-shutdown flag (the
    // server is not shutting down): closeBrowserInstance removes the disconnect listener, which is what makes this intentional teardown quiet. acquire() publishes
    // "ready" only after the readiness gate passes, so the completion log below is finally truthful - unlike the prior liveness-only "ready" claim the outage exposed.
    supervisor.noteReadinessLost();

    await closeBrowserInstance(browser);

    // Launch a fresh browser instance so it is ready for the next stream request.
    await getCurrentBrowser();

    // Minimize the new window to reduce GPU usage and desktop clutter.
    await minimizeBrowserWindow();

    LOG.info("Browser restart complete. Fresh instance is ready.");
  } catch(error) {

    LOG.error("Browser restart failed: %s.", formatError(error));
  }
}

/**
 * Starts the periodic browser restart eligibility check. This should be called once during server startup, after the browser is initialized. The interval runs
 * indefinitely until stopBrowserRestartChecking() is called (typically during graceful shutdown).
 */
export function startBrowserRestartChecking(): void {

  restartCheckInterval = setInterval(checkBrowserRestart, BROWSER_RESTART_CHECK_INTERVAL);
}

/**
 * Stops the browser restart checking interval and cancels any pending quiet timer. This should be called during graceful shutdown to prevent a restart from
 * racing with server shutdown.
 */
export function stopBrowserRestartChecking(): void {

  if(restartCheckInterval) {

    clearInterval(restartCheckInterval);
    restartCheckInterval = null;
  }

  if(restartQuietTimer) {

    clearTimeout(restartQuietTimer);
    restartQuietTimer = null;
  }
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
 * - options.html/options.js: Extension options page (not used by our automation, but required by the manifest)
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
    const out = getExtensionDir(CONFIG);

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

// Re-export getStream from puppeteer-stream for use by the streaming module. This keeps all puppeteer-stream imports centralized in the browser module.
export { getStream };
