/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.ts: Chrome DevTools Protocol helpers for PrismCast.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { LOG, delay, formatError, pollUntil } from "../utils/index.ts";
import type { Clock } from "homebridge-plugin-utils";
import type { Nullable } from "../types/index.ts";
import { systemClock } from "homebridge-plugin-utils";

/* The Chrome DevTools Protocol (CDP) provides low-level access to Chrome's internal state and capabilities. While Puppeteer abstracts most common operations, some
 * features require direct CDP access:
 *
 * - Window presentation: moving the shared browser window between its normal and minimized states, and reading the state Chrome reports for it. That state is the
 *   only window property this application drives, and it is not cosmetic: Chrome's tab capture consumes the compositor's output for the shared window, and a
 *   minimized window's output is not composed for capture to read. Which state the window should be in is decided in one place, by decideWindowVisibility in
 *   windowSync.ts; these primitives only carry it out and report back what Chrome says came of it.
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
 * Executes a CDP (Chrome DevTools Protocol) operation against the window holding a page. This helper handles the common pattern of:
 * 1. Creating a CDP session attached to the page's target
 * 2. Getting the browser window ID for the page
 * 3. Calling the provided operation with the session and window ID
 * 4. Gracefully handling errors when the page is closed during the operation
 *
 * Each call attaches a fresh session and never detaches it...the session stays attached until the page's target goes away. A fresh session per call keeps
 * callers stateless, so no caller holds a session that a page close would leave dangling. The price is one more attached session on the page for every call,
 * which is why the window lookup in index.ts caches its answer per page rather than calling again on every tune.
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

/* The shape Chrome answers Browser.getWindowBounds with. Every field is optional because the response carries whatever the window manager has for the window,
 * and each derivation below decides for itself how much of the report it requires.
 */
interface WindowBoundsReport {

  height?: number;
  left?: number;
  top?: number;
  width?: number;
  windowState?: string;
}

/* The frame a window occupies together with the state it is presented in - the whole answer a window created beside an existing one needs, in one record.
 * A window flush against the top-left of the screen reports a left or a top of 0, so a consumer reads these as numbers rather than as truthy values.
 */
export interface WindowPlacement {

  readonly height: number;
  readonly left: number;
  readonly top: number;
  readonly width: number;
  readonly windowState: string;
}

/**
 * Reads the bounds Chrome reports for a window, through a session that is already open. Private to this module and the one place the frame is read: the state
 * derivation below, the restore confirmation, and the page-level placement read all take their answer from this single call, so no other site composes the
 * request. A response carrying no bounds reads as null rather than throwing, because an absent report is an answer every derivation already branches on. The
 * frame comes back for a minimized window too - Chrome reports the frame it is resting at, which is exactly what a window created beside it needs.
 * @param session - An open CDP session attached to the page's target.
 * @param windowId - The browser window ID that session resolved.
 * @returns The bounds Chrome reports, with every field as optional as Chrome leaves it, or null when the response carries none.
 */
async function readWindowBoundsWith(session: CDPSession, windowId: number): Promise<Nullable<WindowBoundsReport>> {

  const response = await session.send("Browser.getWindowBounds", { windowId }) as { bounds?: WindowBoundsReport } | undefined;

  return response?.bounds ?? null;
}

/**
 * Reads the window state Chrome reports for a window, through a session that is already open. Private to this module: the restore confirmation below and the
 * page-level readWindowState both ask Chrome through the shared read above, so no other site composes the request. A response carrying no bounds, or bounds
 * carrying no state, reads as null rather than throwing, because an absent report is an answer its callers already branch on.
 * @param session - An open CDP session attached to the page's target.
 * @param windowId - The browser window ID that session resolved.
 * @returns The window state Chrome reports, or null when the response carries none.
 */
async function readWindowStateWith(session: CDPSession, windowId: number): Promise<Nullable<string>> {

  return (await readWindowBoundsWith(session, windowId))?.windowState ?? null;
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
 * Reads the full placement - frame and state - of the window a page belongs to, for a caller that means to put a second window in the same spot. Resolves null
 * whenever the placement cannot be read completely: a closed page, a target that yields no window ID, a CDP failure withCDPSession absorbs, or a response
 * missing any one of the four numbers or the state. That strictness lives here rather than in the shared read, because a partial frame is useless to the one
 * consumer this exists for while a partial response is still a perfectly good state report.
 *
 * Mirroring the shared window's own placement is what keeps a second window from disturbing the profile: Chrome persists the window placement it will relaunch
 * at from the last window whose bounds changed, within seconds and regardless of which window the user is working in, and any exit that does not flush that
 * preference - a crash, a forced kill, and the relaunch that follows a crash - carries those bounds into the next launch. A window opened anywhere else writes
 * its own frame there and the shared window comes back at it (measured 2026-08-31).
 * @param page - The Puppeteer page whose window is read.
 * @returns The window's placement, or null when it cannot be read completely.
 */
export async function readWindowPlacement(page: Page): Promise<Nullable<WindowPlacement>> {

  return (await withCDPSession(page, async (session, windowId): Promise<Nullable<WindowPlacement>> => {

    const bounds = await readWindowBoundsWith(session, windowId);

    if(!bounds) {

      return null;
    }

    const { height, left, top, width, windowState } = bounds;

    // Each field's presence is a type test rather than a truthiness test: a window flush against the top or the left edge of the screen reports that
    // coordinate as 0, which a truthiness test would discard as a missing placement.
    if((typeof height !== "number") || (typeof left !== "number") || (typeof top !== "number") || (typeof width !== "number") ||
      (typeof windowState !== "string")) {

      return null;
    }

    return { height, left, top, width, windowState };
  })) ?? null;
}

/**
 * Minimizes the browser window, which keeps the desktop clear and the GPU idle while nothing is capturing. Only the window-visibility executor should call this:
 * the window has to stay on screen for as long as any capture stream is reading the compositor, and that decision belongs to decideWindowVisibility in
 * windowSync.ts.
 * @param page - The Puppeteer page object.
 */
export async function minimizeWindow(page: Page): Promise<void> {

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
 * while a user is completing TV provider authentication in it. The GPU capability probe (detectBrowserCapabilities in browser/index.ts) calls this directly to make
 * its environment representative of the one capture runs in; every other caller goes through the window-visibility executor, which owns the policy.
 *
 * The contract is a confirmed state, not a fired command: this resolves once Chrome reports the window restored, or once the ceiling lapses with a warning and the
 * window left in whatever state it does report. On macOS a restore runs asynchronously against the acknowledgement of the command that asked for it, and a capture
 * requested against a window still mid-restore is the shape of the 2026-08-26 through 08-28 capture-start failures. A window already on screen confirms on its
 * first read, so the confirmation costs one round trip on the common path.
 * @param page - The Puppeteer page object.
 * @param clock - Clock driving the confirmation cadence and its elapsed measurement. Defaults to the system clock; tests inject a virtual clock.
 */
export async function unminimizeWindow(page: Page, clock: Clock = systemClock): Promise<void> {

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

    if(outcome.status === "lapsed") {

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
