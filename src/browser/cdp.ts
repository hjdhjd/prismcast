/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.ts: Chrome DevTools Protocol helpers for PrismCast.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { LOG, delay, formatError, pollUntil, realClock } from "../utils/index.ts";
import type { Clock } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";

/* The Chrome DevTools Protocol (CDP) provides low-level access to Chrome's internal state and capabilities. While Puppeteer abstracts most common operations, some
 * features require direct CDP access:
 *
 * - Window presentation: moving the shared browser window between its normal and minimized states, and reading the state Chrome reports for it. That state is the
 *   only window property this application drives, and it is not cosmetic: Chrome's tab capture consumes the compositor's output for the shared window, and a
 *   minimized window's output is not composed for capture to read. Which state the window should be in is decided in one place, by decideWindowVisibility in
 *   windowSync.ts; these primitives only carry it out and report back what Chrome says came of it.
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

// The wait between one read of the window's state and the next while a restore is in flight. This is a cadence rather than a settle - nothing is being given time
// to happen, Chrome is simply being asked again - and the measured macOS restore completes in roughly a quarter second, so a genuine restore costs about ten reads
// and a window already on screen costs exactly one.
export const WINDOW_STATE_POLL_MS = 25;

// The bound on how long a restore may take to report itself complete. Eight times the measured restore, so a lapse is a genuine fault worth a warning rather than
// a slow-but-healthy transition. Lapsing never blocks the caller: capture proceeds against whatever state Chrome reports.
export const WINDOW_RESTORE_CEILING_MS = 2000;

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
 * Reads the window state Chrome reports for a window, through a session that is already open. Private to this module: the restore confirmation below and the
 * page-level readWindowState both ask Chrome through this one call, so no other site composes the request. A response carrying no bounds, or bounds carrying no
 * state, reads as null rather than throwing, because an absent report is an answer its callers already branch on.
 * @param session - An open CDP session attached to the page's target.
 * @param windowId - The browser window ID that session resolved.
 * @returns The window state Chrome reports, or null when the response carries none.
 */
async function readWindowStateWith(session: CDPSession, windowId: number): Promise<Nullable<string>> {

  const response = await session.send("Browser.getWindowBounds", { windowId }) as { bounds?: { windowState?: string } } | undefined;

  return response?.bounds?.windowState ?? null;
}

/**
 * Reads the window state Chrome reports for the window a page belongs to. Resolves null whenever the state cannot be read at all - a closed page, a target that
 * yields no window ID, or a CDP failure withCDPSession absorbs - so a caller logging this as a diagnostic never has to tell a missing session apart from a
 * missing report.
 * @param page - The Puppeteer page whose window is read.
 * @returns The window state Chrome reports, or null when it cannot be read.
 */
export async function readWindowState(page: Page): Promise<Nullable<string>> {

  return (await withCDPSession(page, readWindowStateWith)) ?? null;
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
 *
 * The contract is a confirmed state, not a fired command: this resolves once Chrome reports the window restored, or once the ceiling lapses with a warning and the
 * window left in whatever state it does report. On macOS a restore runs asynchronously against the acknowledgement of the command that asked for it, and a capture
 * requested against a window still mid-restore is the shape of the 2026-08-26 through 08-28 capture-start failures. A window already on screen confirms on its
 * first read, so the confirmation costs one round trip on the common path.
 * @param page - The Puppeteer page object.
 * @param clock - Clock driving the confirmation cadence and its elapsed measurement. Defaults to realClock; tests inject a fake.
 */
export async function unminimizeWindow(page: Page, clock: Clock = realClock): Promise<void> {

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

    // Confirm the restore against Chrome's own report rather than against the acknowledgement above, which arrives while the window manager is still working.
    const startedAt = clock.now();
    const outcome = await pollUntil({ cadenceMs: WINDOW_STATE_POLL_MS, ceilingMs: WINDOW_RESTORE_CEILING_MS, clock,
      read: (): Promise<Nullable<string>> => readWindowStateWith(session, windowId), until: (state: Nullable<string>): boolean => state === "normal" });

    if(outcome.lapsed) {

      /* A lapse is reported and then stepped past. The window's presentation is the caller's precondition, not its permission: blocking capture on a window that
       * will not report itself restored would convert a presentation fault into a stream failure, which is strictly worse than capturing against a window whose
       * state we have named in the log.
       */
      LOG.warn("The browser window did not report a completed restore within %dms; continuing with the window in its reported state.", WINDOW_RESTORE_CEILING_MS,
        { windowState: outcome.value });
    } else {

      LOG.debug("browser:lifecycle", "The window reported its restore complete after %dms (%d reads).", clock.now() - startedAt, outcome.reads);
    }
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

  /* An explicitly declared, positive density is precisely the mark of a capture page, which is what makes this function safe to fire at any page from any trigger.
   * A page PrismCast has not emulated carries no viewport at all and falls out on the first test; a page emulated for layout declares the display's own density
   * through Chrome's disable value of 0, which the positive test excludes. The narrowing runs in two steps because the send forwards the page's own dimensions as
   * well: the record is nullable, and its density is optional where CDP's field is required.
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
