/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * login.ts: Login mode management for PrismCast.
 */
import type { Browser, Page } from "puppeteer-core";
import { LOG, formatError } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import { unminimizeWindow } from "./cdp.ts";

/* Login mode allows users to authenticate with TV providers directly from the PrismCast web UI. When login mode is active:
 *
 * - A dedicated login tab is open in the browser showing the channel's URL
 * - The browser window is un-minimized so the user can interact with it
 * - New stream requests are blocked to prevent interference with the login process
 * - A 15-minute timeout automatically ends login mode if the user forgets
 *
 * The login page is NOT registered as a managed page to exclude it from stale page cleanup. We manage its lifecycle explicitly through startLoginMode/endLoginMode.
 *
 * The workflow:
 *
 * 1. User clicks "Login" on a channel in the web UI
 * 2. startLoginMode() opens a new tab with the channel's URL and un-minimizes the browser
 * 3. User completes authentication in the visible browser window
 * 4. User clicks "Done" in the web UI, or closes the tab, or the 15-minute timeout fires
 * 5. endLoginMode() closes the login tab (if still open) and re-minimizes the browser
 *
 * During login mode, new stream requests are blocked to prevent the browser from navigating away or creating conflicting tabs.
 */

// State.

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

// Browser accessor functions injected by browser/index.ts via setBrowserAccessors(). This avoids a circular dependency - login.ts needs getBrowserInstance and
// minimizeBrowserWindow from index.ts, which imports login functions. The setter/getter pattern matches setChromeUserAgent in chromeFetch.ts and
// registerProviderModuleProfile in sites.ts.
interface BrowserAccessors {

  getBrowserInstance: () => Nullable<Browser>;
  minimizeBrowserWindow: () => Promise<void>;
}

let browserAccessors: Nullable<BrowserAccessors> = null;

/**
 * Registers the browser accessor functions for use by login mode. Called once by browser/index.ts at module evaluation time after the accessor functions are defined.
 * @param accessors - The browser accessor functions.
 */
export function setBrowserAccessors(accessors: BrowserAccessors): void {

  browserAccessors = accessors;
}

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
 * Resets all login state variables to their initial values. Shared by endLoginMode() (normal cleanup) and clearLoginState() (browser crash cleanup).
 */
function resetLoginState(): void {

  loginModeActive = false;
  loginPage = null;
  loginUrl = null;
  loginStartTime = null;
}

// Functions.

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
  const browser = browserAccessors?.getBrowserInstance() ?? null;

  if(!browser?.connected) {

    return { error: "Browser is not connected.", success: false };
  }

  try {

    // Create a new page for login. We intentionally do NOT register it as a managed page so stale page cleanup ignores it. The local variable avoids repeated
    // null narrowing on the module-level loginPage reference.
    const page = await browser.newPage();

    loginPage = page;

    // Set up handler for tab close detection. If the user closes the tab manually, we should end login mode automatically.
    page.on("close", () => {

      // Only auto-end if this is still the active login page.
      if(loginModeActive && loginPage) {

        LOG.info("Login tab was closed. Ending login mode.");

        // Use void to handle the promise without awaiting (we're in an event handler).
        void endLoginMode();
      }
    });

    // Navigate to the login URL.
    await page.goto(url, { waitUntil: "domcontentloaded" });

    // Un-minimize the browser window so the user can see and interact with it.
    await unminimizeWindow(page);

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
 *
 * Note: The browser disconnect handler uses clearLoginState() instead of endLoginMode() because the browser is already gone and page close/minimize would fail.
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

  resetLoginState();

  // Re-minimize the browser window.
  if(wasActive) {

    if(browserAccessors?.getBrowserInstance()?.connected) {

      await browserAccessors.minimizeBrowserWindow();
    }

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

/**
 * Returns the active login page if login mode is currently active. Used by the profile test flow to evaluate CSS selectors against the live page DOM.
 * @returns The login page, or null if login mode is not active.
 */
export function getLoginPage(): Nullable<Page> {

  if(!loginModeActive) {

    return null;
  }

  return loginPage;
}

/**
 * Clears all login state without attempting any browser operations. Used by the browser disconnect handler when the browser has already crashed - calling
 * endLoginMode() would fail because it tries to close the page and minimize the browser, which are impossible when the browser is gone.
 * @returns True if login mode was active and was cleared, false if it was not active.
 */
export function clearLoginState(): boolean {

  if(!loginModeActive) {

    return false;
  }

  if(loginTimeoutHandle) {

    clearTimeout(loginTimeoutHandle);
    loginTimeoutHandle = null;
  }

  resetLoginState();

  return true;
}
