/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Browser lifecycle management for PrismCast.
 */
import type { Browser, LaunchOptions, Page } from "puppeteer-core";
import { LOG, evaluateWithAbort, formatError } from "../utils/index.js";
import { getAllStreams, getStreamCount } from "../streaming/registry.js";
import { getEffectivePreset, getPresetViewport } from "../config/presets.js";
import { getStream, launch } from "puppeteer-stream";
import { resizeAndMinimizeWindow, unminimizeWindow } from "./cdp.js";
import { setBrowserChrome, setMaxSupportedViewport } from "./display.js";
import { CONFIG } from "../config/index.js";
import type { Nullable } from "../types/index.js";
import type { SystemStatus } from "../streaming/statusEmitter.js";
import { emitSystemStatusChanged } from "../streaming/statusEmitter.js";
import { execSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { launch as puppeteerLaunch } from "puppeteer-core";
import { terminateStream } from "../streaming/lifecycle.js";

const { promises: fsPromises } = fs;

/*
 * APPLICATION STATE
 *
 * Global variables maintain the application's runtime state across all operations. We minimize global state where possible, but some values must be shared across
 * the application lifecycle:
 *
 * - currentBrowser: The shared browser instance. All streaming sessions use a single Chrome process to avoid the overhead of launching multiple browsers. This is
 *   created on first stream request (or during warmup) and persists until the application shuts down or the browser crashes.
 *
 * - dataDir: The filesystem location for persistent data (Chrome profile, extension files). This is always ~/.prismcast, which is created on startup if it doesn't
 *   exist.
 *
 * Stream tracking and ID generation have been moved to streaming/registry.ts for unified stream management across all output types (direct WebM, HLS, etc.).
 */

// The shared browser instance used by all streaming sessions. Created on first stream request or during warmup. Set to null when the browser is not running or
// has disconnected.
let currentBrowser: Nullable<Browser> = null;

// The data directory stores Chrome's profile data and the streaming extension files. This is always ~/.prismcast, which provides a consistent location for
// user-specific data regardless of how PrismCast is run.
const dataDir = path.join(os.homedir(), ".prismcast");

// The stale page cleanup interval handle, stored so we can clear it during graceful shutdown. The interval periodically checks for browser pages that are not
// associated with active streams and closes them to prevent resource exhaustion.
let stalePageCleanupInterval: Nullable<ReturnType<typeof setInterval>> = null;

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

/*
 * MANAGED PAGE TRACKING
 *
 * We track pages that PrismCast creates to distinguish them from pages that might be opened by other means (manually by the user, by site popups, etc.). Only pages we
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

/*
 * LOGIN MODE STATE
 *
 * Login mode allows users to authenticate with TV providers directly from the PrismCast web UI. When login mode is active:
 *
 * - A dedicated login tab is open in the browser showing the channel's URL
 * - The browser window is un-minimized so the user can interact with it
 * - New stream requests are blocked to prevent interference with the login process
 * - A 15-minute timeout automatically ends login mode if the user forgets
 *
 * The login page is NOT registered as a managed page to exclude it from stale page cleanup. We manage its lifecycle explicitly through startLoginMode/endLoginMode.
 */

// Whether login mode is currently active.
let loginModeActive = false;

// The browser page (tab) used for login. Set when login starts, cleared when login ends.
let loginPage: Nullable<Page> = null;

// The URL being used for login. Stored for status reporting.
let loginUrl: Nullable<string> = null;

// Timestamp when login mode started. Used for status reporting and timeout calculation.
let loginStartTime: Nullable<number> = null;

// Timeout handle for auto-ending login mode after 15 minutes.
let loginTimeoutHandle: Nullable<ReturnType<typeof setTimeout>> = null;

// Login timeout duration (15 minutes).
const LOGIN_TIMEOUT_MS = 15 * 60 * 1000;

/**
 * Login status information returned by getLoginStatus().
 */
export interface LoginStatus {

  // Whether login mode is currently active.
  active: boolean;

  // Timestamp when login started (milliseconds since epoch), if active.
  startTime: Nullable<number>;

  // The URL being used for login, if active.
  url: Nullable<string>;
}

/**
 * Computes the current system status and emits it to SSE subscribers. Called when browser state changes significantly or when streams are added/removed.
 */
export async function emitCurrentSystemStatus(): Promise<void> {

  let pageCount = 0;

  try {

    if(currentBrowser && currentBrowser.isConnected()) {

      const pages = await currentBrowser.pages();

      pageCount = pages.length;
    }
  } catch(_error) {

    // Ignore errors getting page count.
  }

  const memUsage = process.memoryUsage();

  const status: SystemStatus = {

    browser: {

      connected: !!currentBrowser && currentBrowser.isConnected(),
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
  const pageId = [ "page-", String(++managedPageIdCounter) ].join("");

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
 * Gets the current data directory path. This is where Chrome profile data and extension files are stored.
 * @returns The data directory path.
 */
export function getDataDir(): string {

  return dataDir;
}

/**
 * Ensures the data directory exists, creating it if necessary. This should be called during application startup before any operations that depend on the data
 * directory (like browser launch or extension preparation).
 *
 * The data directory (~/.prismcast) stores:
 * - Chrome profile data (cookies, local storage, session state)
 * - Extension files (when running as a packaged executable)
 */
export async function ensureDataDirectory(): Promise<void> {

  try {

    await fsPromises.mkdir(dataDir, { recursive: true });

    LOG.info("Data directory ready: %s.", dataDir);
  } catch(error) {

    LOG.error("Failed to create data directory %s: %s.", dataDir, formatError(error));

    throw error;
  }
}

/*
 * BROWSER MANAGEMENT
 *
 * These functions handle the Chrome browser lifecycle: startup, cleanup, and instance management. The browser is a shared resource used by all streaming sessions,
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
 * Terminates any Chrome processes that are still using our profile directory from previous runs. Chrome locks its profile directory while running, and if a
 * previous instance crashed without releasing the lock, we cannot launch a new browser with the same profile. This function uses pkill to find and terminate any
 * Chrome processes whose command line contains our profile directory path.
 *
 * This is called at startup before launching the browser to ensure a clean slate. It's safe to call even when no stale processes exist - pkill returns a non-zero
 * exit code when no processes match, which we ignore.
 */
export function killStaleChrome(): void {

  // Build the profile directory path that would appear in Chrome's command-line arguments.
  const profileDir = path.join(dataDir, CONFIG.paths.chromeProfileName);

  try {

    // Use pkill with -f to match against the full command line, not just the process name. This finds Chrome processes that were launched with our profile
    // directory. The command is wrapped in quotes to handle paths with spaces.
    execSync([ "pkill -f \"", profileDir, "\"" ].join(""));

    LOG.info("Killed stale Chromium instances using %s.", profileDir);
  } catch(_error) {

    // When pkill finds no matching processes, it returns a non-zero exit code. This is expected when there are no stale processes from a clean shutdown. We
    // silently ignore this expected case.
  }
}

/**
 * Locates the Chrome or Chromium executable on the system. The CHROME_BIN environment variable takes precedence, allowing operators to specify a non-standard
 * installation. Otherwise, we search common installation paths across macOS, Linux, and Windows for Chrome, Chromium, and Brave browsers.
 *
 * Brave is included because it's Chromium-based and supports the extensions and features we need. The search order prioritizes Google Chrome over Chromium over
 * Brave, as Chrome generally has the best compatibility with puppeteer-stream.
 * @returns Path to the Chrome executable.
 * @throws If no suitable browser is found.
 */
export function getExecutablePath(): string {

  // Environment variable override takes precedence. This is useful for containerized deployments, non-standard installations, or when multiple browser versions
  // are present.
  if(CONFIG.browser.executablePath) {

    return CONFIG.browser.executablePath;
  }

  // Check standard installation paths across platforms. We search in priority order: Google Chrome (best compatibility) -> Chromium -> Brave.
  const paths = [

    // macOS paths. Applications are typically in /Applications with .app bundles containing the actual executable.
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",

    // Linux paths. Browser packages install to /usr/bin with various naming conventions depending on the distribution and package.
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",

    // Windows paths. Both 64-bit (Program Files) and 32-bit (Program Files (x86)) installations are checked.
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

    /*
     * Chrome command-line arguments. Each flag serves a specific purpose for reliable streaming:
     *
     * --allow-running-insecure-content: Some streaming sites serve mixed HTTP/HTTPS content. Without this flag, the browser blocks HTTP resources on HTTPS
     *   pages, which can break video players that load some assets over HTTP.
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
      [ "--window-size=", String(getPresetViewport(CONFIG).width), ",", String(getPresetViewport(CONFIG).height) ].join("")
    ],

    // Disable Puppeteer's default viewport constraints. We manage viewport sizing ourselves via CDP to account for browser chrome (toolbars, borders) and
    // ensure the content area matches our target dimensions exactly.
    defaultViewport: null,

    // Path to the Chrome executable, either from environment variable or autodetected.
    executablePath: getExecutablePath(),

    // Run Chrome in headed (visible) mode, not headless. The puppeteer-stream extension requires a visible browser window to capture the screen. We minimize
    // the window after launch to reduce GPU usage while still allowing capture.
    headless: false,

    /*
     * Prevent Puppeteer from adding certain default arguments that would interfere with streaming:
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
    userDataDir: path.join(dataDir, CONFIG.paths.chromeProfileName)
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

    const extensionPath = path.join(dataDir, CONFIG.paths.extensionDirName);

    // Remove any existing extension arguments and add our own pointing to the extracted extension.
    opts.args = (opts.args ?? [])
      .filter((arg: string): boolean => {

        return !arg.startsWith("--load-extension=") && !arg.startsWith("--disable-extensions-except=");
      })
      .concat([

        [ "--disable-extensions-except=", extensionPath ].join(""),
        [ "--load-extension=", extensionPath ].join("")
      ]);
  }

  return puppeteerLaunch(opts);
}

/**
 * Handles browser disconnection events by cleaning up all active streams and resetting browser state. This function is called when the browser crashes, is closed
 * manually, or loses its connection for any other reason.
 *
 * When the browser disconnects, all pages (tabs) within it are immediately invalid and cannot be used. Any active streams using those pages will fail if they try
 * to interact with them. We proactively clean up by:
 * 1. Setting currentBrowser to null so the next stream request will launch a fresh browser
 * 2. Stopping all health monitors (they would fail trying to check page state)
 * 3. Removing all entries from activeStreams (the pages are gone)
 *
 * This ensures streams fail gracefully rather than hanging indefinitely trying to use closed pages.
 */
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

    // Measure display dimensions and browser chrome via JavaScript.
    const dimensions = await evaluateWithAbort(targetPage, (): { availHeight: number; availWidth: number; chromeHeight: number; chromeWidth: number } => {

      return {

        // Available screen dimensions (excludes taskbar, dock, menu bar).
        availHeight: screen.availHeight,
        availWidth: screen.availWidth,

        // Browser chrome dimensions (title bar, toolbar, borders).
        chromeHeight: window.outerHeight - window.innerHeight,
        chromeWidth: window.outerWidth - window.innerWidth
      };
    });

    // Calculate maximum viewport: available screen space minus browser chrome.
    const maxWidth = dimensions.availWidth - dimensions.chromeWidth;
    const maxHeight = dimensions.availHeight - dimensions.chromeHeight;

    // Cache the results for use by the preset system and window sizing.
    setBrowserChrome(dimensions.chromeWidth, dimensions.chromeHeight);
    setMaxSupportedViewport(maxWidth, maxHeight);

    LOG.debug("Display detection complete: screen %s\u00d7%s, chrome %s\u00d7%s, max viewport %s\u00d7%s.",
      dimensions.availWidth, dimensions.availHeight,
      dimensions.chromeWidth, dimensions.chromeHeight,
      maxWidth, maxHeight);

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

function handleBrowserDisconnect(): void {

  // Clear the browser reference so getCurrentBrowser() will launch a new instance on the next call.
  currentBrowser = null;

  // Clear login state if login mode was active. We clear directly rather than calling endLoginMode() because the browser is already gone and we don't want to
  // attempt any browser operations.
  if(loginModeActive) {

    if(loginTimeoutHandle) {

      clearTimeout(loginTimeoutHandle);
      loginTimeoutHandle = null;
    }

    loginModeActive = false;
    loginPage = null;
    loginUrl = null;
    loginStartTime = null;

    if(!gracefulShutdownInProgress) {

      LOG.info("Login mode ended due to browser disconnect.");
    }
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
 * Provides access to the shared browser instance, launching one if needed. The browser is a shared resource used by all streaming sessions. This function handles:
 *
 * - Returning the existing browser if it's still connected
 * - Launching a new browser if none exists or the previous one disconnected
 * - Waiting for the puppeteer-stream extension to initialize
 * - Minimizing the browser window to reduce GPU usage
 * - Setting up disconnect handlers for crash recovery
 * @returns The browser instance.
 * @throws If the browser cannot be launched.
 */
export async function getCurrentBrowser(): Promise<Browser> {

  // Fast path: if we have a browser and it's still connected, return it immediately. The isConnected() check verifies the DevTools Protocol connection is
  // still alive.
  if(currentBrowser && currentBrowser.isConnected()) {

    return currentBrowser;
  }

  // We need to launch a new browser. This happens on first stream request, after a browser crash, or during server warmup.
  try {

    const options = buildLaunchOptions();

    // The launch function from puppeteer-stream wraps standard Puppeteer launch to inject the streaming extension. We pass our custom launch function that
    // handles packaged executable extension paths.
    currentBrowser = await launch({ launch: launchWithCustomArgs }, options);

    // Register a handler for browser disconnection. This ensures we clean up properly if the browser crashes or is closed unexpectedly.
    currentBrowser.on("disconnected", handleBrowserDisconnect);

    // The streaming extension needs time to initialize after browser launch. It injects recording APIs into the browser context that puppeteer-stream uses
    // to capture media. Without this delay, stream capture may fail silently or produce blank output.
    await new Promise<void>((resolve) => {

      setTimeout(resolve, CONFIG.browser.initTimeout);
    });

    // Detect display dimensions to determine maximum supported viewport. This must happen before we start streaming so the preset system can degrade to a
    // smaller preset if needed.
    await detectDisplayDimensions(currentBrowser);

    // Minimize the browser window to reduce GPU usage and desktop clutter. The browser must be visible (not headless) for capture to work, but minimizing
    // it reduces resource consumption. CDP allows us to control window state without affecting capture.
    await minimizeBrowserWindow();

    // Emit system status update for SSE subscribers.
    await emitCurrentSystemStatus();
  } catch(error) {

    LOG.error("Failed to launch browser: %s.", formatError(error));

    // Clear the browser reference on failure so the next call will attempt to launch again.
    currentBrowser = null;

    throw error;
  }

  return currentBrowser;
}

/**
 * Checks if the browser is currently connected and usable. This is a synchronous check that can be used before attempting browser operations.
 * @returns True if the browser is connected and ready for use, false otherwise.
 */
export function isBrowserConnected(): boolean {

  return !!currentBrowser && currentBrowser.isConnected();
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
  if(!currentBrowser || !currentBrowser.isConnected()) {

    return;
  }

  let tempPage: Nullable<Page> = null;
  let usingTempPage = false;

  try {

    // Try to use an existing page first. Creating a new page can cause the window to restore/activate on macOS, which defeats the purpose of minimizing.
    const existingPages = await currentBrowser.pages();
    let targetPage: Nullable<Page> = existingPages.find((p) => !p.isClosed()) ?? null;

    // If no existing pages, we must create a temporary one. This is less ideal but necessary to get a CDP session target.
    if(!targetPage) {

      tempPage = await currentBrowser.newPage();
      usingTempPage = true;

      // Register the temp page so stale cleanup knows it's ours.
      registerManagedPage(tempPage);
      targetPage = tempPage;
    }

    // Delegate to resizeAndMinimizeWindow for the actual CDP operations. This ensures consistent resize+minimize behavior and maintains a single source of
    // truth for the viewport sizing logic.
    await resizeAndMinimizeWindow(targetPage, true);

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
    LOG.warn("Could not resize and minimize browser window: %s.", formatError(error));
  }
}

/**
 * Gets all open browser pages (tabs). This is used by the health check endpoint to report page count and by stale page cleanup to find orphaned pages.
 * @returns Array of pages, or empty array if the browser is not connected.
 */
export async function getBrowserPages(): Promise<Page[]> {

  // Guard against calling this when no browser is running.
  if(!currentBrowser || !currentBrowser.isConnected()) {

    return [];
  }

  try {

    return await currentBrowser.pages();
  } catch(_error) {

    // If getting pages fails (browser disconnecting, etc.), return empty array rather than throwing.
    return [];
  }
}

/**
 * Closes the browser and cleans up resources. This is called during graceful shutdown to ensure Chrome exits cleanly. After this call, the browser reference is
 * cleared and any subsequent stream requests will launch a fresh browser.
 *
 * The function uses a multi-stage approach to ensure Chrome actually exits:
 * 1. Try browser.close() with a 5-second timeout
 * 2. If that fails or times out, kill the browser process directly
 * 3. As a final fallback, use pkill to terminate any Chrome processes using our profile
 */
export async function closeBrowser(): Promise<void> {

  // Ensure the flag is set so the disconnect handler knows this is intentional. Normally set earlier by app.ts shutdown(), but set here as a fallback for direct
  // calls to closeBrowser().
  setGracefulShutdown(true);

  const browserRef = currentBrowser;

  // Clear the reference early to prevent any new operations from using it.
  currentBrowser = null;

  if(!browserRef) {

    return;
  }

  // Stage 1: Try graceful close with a timeout. We use Promise.race to avoid hanging indefinitely if Chrome is unresponsive.
  let closedGracefully = false;

  if(browserRef.isConnected()) {

    try {

      await Promise.race([
        browserRef.close(),
        new Promise((_, reject) => setTimeout(() => reject(new Error("Browser close timed out")), 5000))
      ]);

      closedGracefully = true;
    } catch(error) {

      const message = formatError(error);

      if(message.includes("timed out")) {

        LOG.warn("Browser did not close within 5 seconds. Forcing termination.");
      } else {

        LOG.debug("Browser close error: %s.", message);
      }
    }
  } else {

    closedGracefully = true;
  }

  // Stage 2: If graceful close failed, try to kill the process directly.
  if(!closedGracefully) {

    try {

      const browserProcess = browserRef.process();

      if(browserProcess && !browserProcess.killed) {

        browserProcess.kill("SIGKILL");
        LOG.debug("Sent SIGKILL to browser process.");
      }
    } catch(error) {

      LOG.debug("Browser process kill error: %s.", formatError(error));
    }
  }

  // Stage 3: As a final fallback, use pkill to ensure no Chrome processes using our profile remain.
  killStaleChrome();
}

/*
 * LOGIN MODE
 *
 * These functions manage the login mode workflow, allowing users to authenticate with TV providers through the browser. The workflow is:
 *
 * 1. User clicks "Login" on a channel in the web UI
 * 2. startLoginMode() opens a new tab with the channel's URL and un-minimizes the browser
 * 3. User completes authentication in the visible browser window
 * 4. User clicks "Done" in the web UI, or closes the tab, or the 15-minute timeout fires
 * 5. endLoginMode() closes the login tab (if still open) and re-minimizes the browser
 *
 * During login mode, new stream requests are blocked to prevent the browser from navigating away or creating conflicting tabs.
 */

/**
 * Starts login mode by opening a new browser tab with the specified URL and un-minimizing the browser window. The user can then authenticate with their TV
 * provider in the visible browser.
 *
 * Login mode blocks new stream requests until it ends (via endLoginMode, tab close detection, or timeout).
 * @param url - The URL to navigate to for authentication.
 * @returns Object indicating success or failure with optional error message.
 */
export async function startLoginMode(url: string): Promise<{ error?: string; success: boolean }> {

  // Check if login mode is already active.
  if(loginModeActive) {

    return { error: "Login is already in progress.", success: false };
  }

  // Ensure browser is available.
  if(!currentBrowser || !currentBrowser.isConnected()) {

    return { error: "Browser is not connected.", success: false };
  }

  try {

    // Create a new page for login. We intentionally do NOT register it as a managed page so stale page cleanup ignores it.
    loginPage = await currentBrowser.newPage();

    // Set up handler for tab close detection. If the user closes the tab manually, we should end login mode automatically.
    loginPage.on("close", () => {

      // Only auto-end if this is still the active login page.
      if(loginModeActive && loginPage) {

        LOG.info("Login tab was closed. Ending login mode.");

        // Use void to handle the promise without awaiting (we're in an event handler).
        void endLoginMode();
      }
    });

    // Navigate to the login URL.
    await loginPage.goto(url, { waitUntil: "domcontentloaded" });

    // Un-minimize the browser window so the user can see and interact with it.
    await unminimizeWindow(loginPage);

    // Set login state.
    loginModeActive = true;
    loginUrl = url;
    loginStartTime = Date.now();

    // Set up the 15-minute timeout.
    loginTimeoutHandle = setTimeout(() => {

      LOG.warn("Login mode timed out after 15 minutes. Ending login mode.");

      void endLoginMode();
    }, LOGIN_TIMEOUT_MS);

    LOG.info("Login mode started for %s.", url);

    return { success: true };
  } catch(error) {

    // Clean up on failure.
    if(loginPage && !loginPage.isClosed()) {

      try {

        await loginPage.close();
      } catch(_closeError) {

        // Ignore close errors.
      }
    }

    loginPage = null;

    return { error: formatError(error), success: false };
  }
}

/**
 * Ends login mode by closing the login tab (if still open) and re-minimizing the browser window. This function is idempotent - it's safe to call multiple times
 * or when login mode is not active.
 *
 * Called by:
 * - User clicking "Done" in the web UI (POST /auth/done)
 * - Tab close detection (user closes the tab manually)
 * - 15-minute timeout
 * - Browser disconnect handler (cleanup)
 */
export async function endLoginMode(): Promise<void> {

  // Clear the timeout if it hasn't fired yet.
  if(loginTimeoutHandle) {

    clearTimeout(loginTimeoutHandle);
    loginTimeoutHandle = null;
  }

  // Close the login page if it's still open.
  if(loginPage && !loginPage.isClosed()) {

    try {

      await loginPage.close();
    } catch(_error) {

      // Ignore close errors - the page may have already been closed.
    }
  }

  // Clear login state.
  const wasActive = loginModeActive;

  loginModeActive = false;
  loginPage = null;
  loginUrl = null;
  loginStartTime = null;

  // Re-minimize the browser window.
  if(wasActive && currentBrowser && currentBrowser.isConnected()) {

    await minimizeBrowserWindow();
  }

  if(wasActive) {

    LOG.info("Login mode ended.");
  }
}

/**
 * Returns whether login mode is currently active. Used by the stream handler to block new stream requests during login.
 * @returns True if login mode is active, false otherwise.
 */
export function isLoginModeActive(): boolean {

  return loginModeActive;
}

/**
 * Returns the current login status including whether active, the URL being used, and when login started. Used by the /auth/status API endpoint.
 * @returns Login status object.
 */
export function getLoginStatus(): LoginStatus {

  return {

    active: loginModeActive,
    startTime: loginStartTime,
    url: loginUrl
  };
}

/*
 * STALE PAGE CLEANUP
 *
 * Over time, browser pages (tabs) may accumulate if cleanup fails during stream termination. This can happen due to race conditions, errors during cleanup, or
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

  // Guard against calling this when no browser is running.
  if(!currentBrowser || !currentBrowser.isConnected()) {

    return;
  }

  try {

    const pages = await currentBrowser.pages();

    // If there's only one page or fewer, we must preserve it to keep the browser alive. Don't attempt cleanup.
    if(pages.length <= 1) {

      return;
    }

    // Build a set of page IDs for pages currently in use by active streams.
    const activePageIds = new Set<string>();

    for(const streamInfo of getAllStreams()) {

      const pageId = getManagedPageId(streamInfo.page);

      if(pageId) {

        activePageIds.add(pageId);
      }
    }

    const now = Date.now();

    const gracePeriod = CONFIG.recovery.stalePageGracePeriod;

    // Build a list of pages that are candidates for cleanup. A page is a candidate if:
    // - It has a managed page ID (was created by PrismCast)
    // - It is not associated with any active stream
    // - It has been stale for longer than the grace period
    const candidatePages: Array<{ page: Page; pageId: string }> = [];

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

      LOG.info("Cleaned up %s stale page(s).", closedCount);
    }
  } catch(error) {

    // Cleanup failure is not critical - log a warning and try again next interval.
    LOG.warn("Stale page cleanup failed: %s.", formatError(error));
  }
}

/**
 * Starts the periodic stale page cleanup interval. This should be called once during server startup, after the browser is initialized. The interval runs
 * indefinitely until stopStalePageCleanup() is called (typically during graceful shutdown).
 */
export function startStalePageCleanup(): void {

  stalePageCleanupInterval = setInterval(cleanupStalePages, CONFIG.recovery.stalePageCleanupInterval);
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

/*
 * EXTENSION PREPARATION
 *
 * When running as a packaged executable (created by the `pkg` tool), the application is bundled into a single binary. Node modules like puppeteer-stream are
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

    // The extension files are extracted to ~/.prismcast/extension (the data directory is ensured to exist before this function is called).
    const out = path.join(dataDir, CONFIG.paths.extensionDirName);

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

    LOG.info("Extension files prepared successfully.");
  } catch(error) {

    LOG.error("Extension preparation failed: %s.", formatError(error));

    throw error;
  }
}

// Re-export getStream from puppeteer-stream for use by the streaming module. This keeps all puppeteer-stream imports centralized in the browser module.
export { getStream };
