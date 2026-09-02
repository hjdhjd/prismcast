/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.ts: Chrome DevTools Protocol helpers for PrismCast.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { LOG, delay, formatError } from "../utils/index.ts";

/* The Chrome DevTools Protocol (CDP) provides low-level access to Chrome's internal state and capabilities. While Puppeteer abstracts most common operations, some
 * features require direct CDP access:
 *
 * - Window presentation: moving the shared browser window between its normal and minimized states. That state is the only window property this application drives,
 *   and it is not cosmetic: Chrome's tab capture consumes the compositor's output for the shared window, and a minimized window's output is not composed for
 *   capture to read. Which state the window should be in is decided in one place, by decideWindowVisibility in windowSync.ts; these primitives only carry it out.
 *
 * - Capture surface re-affirmation: re-issuing a capture page's own declared device metrics, which is what moves that capture's composition target back to the
 *   emulated surface.
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
    if(!message.includes("No target with given id") && !page.isClosed()) {

      LOG.warn("CDP operation failed: %s.", message);
    }

    return undefined;
  }
}

/**
 * Minimizes the browser window, which keeps the desktop clear and the GPU idle while nothing is capturing. Only the window-visibility executor should call this:
 * the window has to stay on screen for as long as any capture stream is reading the compositor, and that decision belongs to decideWindowVisibility in
 * windowSync.ts.
 * @param page - The Puppeteer page object.
 */
export async function minimizeWindow(page: Page): Promise<void> {

  // Early exit if the page is already closed.
  if(page.isClosed()) {

    return;
  }

  await withCDPSession(page, async (session, windowId) => {

    /* Let the window manager settle before asking for the state change. On macOS, NSWindow state transitions run asynchronously relative to Chrome's
     * acknowledgement of a CDP command, and the page this call arrives on has usually just been created or navigated, which activates the window. A minimize
     * issued into that unfinished transition can be dropped, leaving the window on screen.
     */
    await delay(100);

    await session.send("Browser.setWindowBounds", {

      bounds: { windowState: "minimized" },
      windowId
    });
  });
}

/**
 * Un-minimizes the browser window, restoring it to normal state. The window belongs on screen while a capture stream is reading the compositor's output for it, and
 * while a user is completing TV provider authentication in it. The capability probe calls this directly to make its environment representative of the one capture
 * runs in; every other caller goes through the window-visibility executor, which owns the policy.
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

/**
 * Re-issues a capture page's own declared device metrics. Chrome composes the capture of a selected tab from the window's fitted presentation rather than from the
 * emulated surface, and the override event is what re-selects the composition target: a capture composing the window's view of the page returns to the full
 * emulated surface, and a capture already composing that surface is left exactly as it was. Callable at any time, from anywhere, at any frequency, because the
 * values sent are the ones Puppeteer has already declared on this page - nothing about the page's emulation changes.
 *
 * The command is issued raw rather than through page.setViewport, which drops an override whose values match the standing one. It is the event, not a change of
 * values, that does the work here.
 * @param page - The page to re-affirm. A page carrying no explicitly declared density is left alone.
 * @throws Whatever the CDP send rejects with. Each trigger site decides whether that matters to it.
 */
export async function reaffirmCaptureSurface(page: Page): Promise<void> {

  /* An explicitly declared, positive density is precisely the mark of a capture page: the launch default leaves every other page at native density and the login
   * page clears its emulation outright, so this guard is what makes the function safe to fire at any page from any trigger. The narrowing runs in two steps
   * because the send forwards the page's own dimensions as well: the record is nullable, and its density is optional where CDP's field is required.
   */
  const viewport = page.viewport();

  if(!viewport) {

    return;
  }

  const deviceScaleFactor = viewport.deviceScaleFactor;

  if((typeof deviceScaleFactor !== "number") || !(deviceScaleFactor > 0)) {

    return;
  }

  const session = await page.createCDPSession();

  try {

    await session.send("Emulation.setDeviceMetricsOverride", { deviceScaleFactor, height: viewport.height, mobile: false, width: viewport.width });
  } finally {

    /* The detach carries a catch of its own. A page that dies mid-send takes its session with it, and a throw raised out of this finally would replace the send's
     * rejection with a detach failure the caller can do nothing about - the send's own reason has to reach the caller unaltered.
     */
    try {

      await session.detach();
    } catch {

      // The session is already gone, which is the state the detach was asking for.
    }
  }
}
