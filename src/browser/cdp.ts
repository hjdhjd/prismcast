/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.ts: Chrome DevTools Protocol helpers for PrismCast.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { LOG, evaluateWithAbort, formatError } from "../utils/index.js";
import type { Nullable, UiSize } from "../types/index.js";
import { CONFIG } from "../config/index.js";
import { getBrowserChrome } from "./display.js";
import { getEffectiveViewport } from "../config/presets.js";

/* The Chrome DevTools Protocol (CDP) provides low-level access to Chrome's internal state and capabilities. While Puppeteer abstracts most common operations, some
 * features require direct CDP access:
 *
 * - Window management: Setting window size, position, and state (minimized, maximized, fullscreen). Puppeteer's viewport API controls the content area, but we
 *   need CDP to control the entire window including browser chrome.
 *
 * - Browser-level operations: Operations that affect the browser rather than a specific page, like getting the window ID for a page's target.
 *
 * CDP sessions are created per-page and must be managed carefully:
 * - Sessions can fail if the page or target is closed while we're using it
 * - The "No target with given id" error is common and expected when pages close during operations
 * - We wrap CDP operations in try/catch to handle these transient errors gracefully
 *
 * The withCDPSession helper encapsulates the common pattern of creating a session, getting the window ID, performing an operation, and handling errors.
 */

/**
 * Executes a CDP (Chrome DevTools Protocol) operation with proper session lifecycle management. This helper handles the common pattern of:
 * 1. Creating a CDP session attached to the page's target
 * 2. Getting the browser window ID for the page
 * 3. Calling the provided operation with the session and window ID
 * 4. Gracefully handling errors when the page is closed during the operation
 *
 * The session is created fresh for each call rather than being reused because CDP sessions become invalid when the page navigates or closes. Creating a new
 * session ensures we always have a valid connection.
 * @param page - The Puppeteer page object to create a CDP session for.
 * @param operation - An async function that receives the CDP session and window ID. The operation can use any CDP commands via session.send().
 * @returns The result of the operation, or undefined if the page was closed or an error occurred.
 */
export async function withCDPSession<T>(
  page: Page,
  operation: (session: CDPSession, windowId: number) => Promise<T>
): Promise<T | undefined> {

  // Early exit if the page is already closed. This prevents errors when trying to create a session for a closed page.
  if(page.isClosed()) {

    return undefined;
  }

  try {

    // Create a CDP session attached to the page's target. The session provides access to all CDP domains (Browser, Page, Network, etc.) for this specific
    // target. Each page has its own target in Chrome's DevTools architecture.
    const session = await page.createCDPSession();

    // Get the browser window ID for this page. Chrome organizes pages into windows, and we need the window ID to perform window-level operations like resizing
    // or minimizing. The Browser.getWindowForTarget command returns the window ID for the current target.
    const windowResult = await session.send("Browser.getWindowForTarget") as { windowId?: number };
    const windowId = windowResult.windowId;

    // If we couldn't get a window ID, the target may be in an invalid state. Return undefined to indicate the operation couldn't be performed.
    if(!windowId) {

      return undefined;
    }

    // Execute the caller's operation with the session and window ID.
    return await operation(session, windowId);
  } catch(error) {

    const message = formatError(error);

    // "No target with given id" is a common error that occurs when the page closes during our operation. This is expected during stream termination and
    // shouldn't be logged as a warning since it's not actionable. We also check if the page is closed, as errors during page closure are expected.
    if((message.indexOf("No target with given id") === -1) && !page.isClosed()) {

      LOG.warn("CDP operation failed: %s.", message);
    }

    return undefined;
  }
}

/**
 * Resizes the browser window to match our target viewport dimensions and optionally minimizes it. This function solves the problem of ensuring the video content
 * area exactly matches our configured viewport size.
 *
 * The complication is that browser windows have "chrome" - the title bar, toolbar, borders, and other UI elements that take up space. If we set the window size
 * to 1280x720, the actual content area will be smaller (perhaps 1280x670 after accounting for the toolbar). To get a 1280x720 content area, we need to add the
 * chrome dimensions to our window size.
 *
 * This function:
 * 1. Measures the current chrome dimensions by comparing window.outerWidth/Height to window.innerWidth/Height
 * 2. Sets the window size to viewport + chrome dimensions, giving us the exact viewport size we want
 * 3. Optionally minimizes the window to reduce GPU usage while still allowing capture
 * @param page - The Puppeteer page object.
 * @param shouldMinimize - Whether to minimize the window after resizing. Set to true for stream pages (to reduce GPU usage) and false for debug pages (where
 *   visibility is desired).
 */
export async function resizeAndMinimizeWindow(page: Page, shouldMinimize: boolean): Promise<void> {

  // Early exit if the page is already closed.
  if(page.isClosed()) {

    return;
  }

  // Get browser chrome dimensions. Prefer cached values from display detection, which were measured when the browser was in a known good state. Fall back to
  // measuring via page.evaluate() if cached values aren't available (e.g., during early initialization or after cache clear).
  let uiSize: Nullable<UiSize> = getBrowserChrome();

  if(!uiSize) {

    try {

      uiSize = await evaluateWithAbort(page, (): UiSize => {

        return {

          // Height of chrome = total window height - content height. This includes the title bar, toolbar, and any other vertical UI elements.
          height: window.outerHeight - window.innerHeight,

          // Width of chrome = total window width - content width. This typically includes window borders and any side panels.
          width: window.outerWidth - window.innerWidth
        };
      });
    } catch(_error) {

      // If measuring fails (page closed, navigation in progress, etc.), silently return. The resize is not critical and will be attempted again on the next
      // stream if needed.
      return;
    }
  }

  // Use CDP to set the window bounds. We add the chrome dimensions to our target viewport to get the correct total window size. CDP requires separate calls for
  // dimensions and window state - they cannot be combined in a single call.
  await withCDPSession(page, async (session, windowId) => {

    // First, ensure the window is in "normal" state. If the browser launched with a window size larger than the display, Chrome may have automatically maximized
    // the window. Setting bounds on a maximized window is ignored, so we must restore it to normal state first.
    await session.send("Browser.setWindowBounds", {

      bounds: { windowState: "normal" },
      windowId
    });

    // Set the window size to viewport + chrome. After this, the content area will be exactly our target viewport dimensions.
    const viewport = getEffectiveViewport(CONFIG);

    await session.send("Browser.setWindowBounds", {

      bounds: {

        // Total window height = desired viewport height + chrome height (title bar, toolbar, etc.)
        height: viewport.height + uiSize.height,

        // Total window width = desired viewport width + chrome width (borders, etc.)
        width: viewport.width + uiSize.width
      },
      windowId
    });

    // Optionally minimize the window to reduce GPU usage. This must be a separate CDP call because window state cannot be combined with dimensions. Minimizing
    // doesn't stop video capture - the puppeteer-stream extension captures from the compositor rather than the visible display.
    if(shouldMinimize) {

      // Brief delay to allow Chrome's window manager to finish processing the resize before minimizing. Without this delay, the minimize can be ignored when the
      // window is being significantly resized (e.g., during preset degradation from 1080p to 720p).
      await new Promise<void>((resolve) => setTimeout(resolve, 100));

      await session.send("Browser.setWindowBounds", {

        bounds: { windowState: "minimized" },
        windowId
      });
    }
  });
}

/**
 * Un-minimizes the browser window, restoring it to normal state. This is used when the user needs to interact with the browser, such as during the authentication
 * login flow where the user must complete TV provider authentication in the visible browser window.
 * @param page - The Puppeteer page object.
 */
export async function unminimizeWindow(page: Page): Promise<void> {

  // Early exit if the page is already closed.
  if(page.isClosed()) {

    return;
  }

  await withCDPSession(page, async (session, windowId) => {

    // Restore the window to normal (visible) state.
    await session.send("Browser.setWindowBounds", {

      bounds: { windowState: "normal" },
      windowId
    });
  });
}
