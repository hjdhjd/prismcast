/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.test.ts: Unit tests for the Chrome DevTools Protocol helpers in cdp.ts. The module exports withCDPSession (the lifecycle wrapper around a CDP session
 * that surfaces the browser window ID), minimizeWindow (the one-shot that puts the shared window into its minimized state), unminimizeWindow (which commands the
 * restore and then confirms it against Chrome's own report), readWindowState (that report, read for a page), and readWindowPlacement (the window's frame and
 * state together, for a caller opening a second window in the same spot).
 * The tests use plain stub objects shaped per the Page and CDPSession contracts - no real browser is launched, and the window's dimensions enter the picture
 * only through the placement read, because the window primitives drive presentation state alone. Which state the window should be in is decided in
 * windowSync.ts and asserted there; these tests cover only the commands each primitive issues, the confirmation the restore waits on, and what each read makes of
 * the report it gets.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { TestClock, advanceThroughSchedule, drainClock, settle } from "homebridge-plugin-utils/testing";
import { WINDOW_RESTORE_CEILING_MS, WINDOW_STATE_POLL_MS, minimizeWindow, readWindowPlacement, readWindowState, unminimizeWindow, withCDPSession } from "./cdp.ts";
import { describe, test } from "node:test";
import type { LogEntry } from "../utils/logEmitter.ts";
import assert from "node:assert/strict";
import { subscribeToLogs } from "../utils/logEmitter.ts";

/* CdpStub captures every send() call so tests can assert on the command sequence. A test that supplies overrideSend replaces the router outright; otherwise
 * send() answers by method name - Browser.getWindowForTarget returns windowId 7 unless getWindowForTargetResponse overrides it, Browser.getWindowBounds reports
 * a normal window state, and every other command resolves with nothing, which is what Chrome's window-state commands themselves return.
 */
interface CdpStub {

  calls: { method: string; params: unknown }[];
  send: (method: string, params?: unknown) => Promise<unknown>;
}

/* makeCdpStub returns a CDPSession-shaped stub. The optional getWindowForTargetResponse override lets a test simulate an invalid target by returning {} (no
 * windowId), and overrideSend lets a test substitute a custom command router (used for the "page closes during operation" case where send() rejects partway).
 */
function makeCdpStub(options: { getWindowForTargetResponse?: { windowId?: number }; overrideSend?: (method: string, params?: unknown) => Promise<unknown> }
  = {}): CdpStub {

  const calls: { method: string; params: unknown }[] = [];

  const send = async (method: string, params?: unknown): Promise<unknown> => {

    calls.push({ method, params });

    if(options.overrideSend) {

      return options.overrideSend(method, params);
    }

    if(method === "Browser.getWindowForTarget") {

      return options.getWindowForTargetResponse ?? { windowId: 7 };
    }

    // The default window is already presented, which is what a restore confirmation asks about. Rows that want a window mid-transition supply their own router.
    if(method === "Browser.getWindowBounds") {

      return { bounds: { windowState: "normal" } };
    }

    return Promise.resolve(undefined);
  };

  return { calls, send };
}

/* makePageStub returns a Page-shaped stub whose createCDPSession resolves with the supplied stub and whose isClosed flag is configurable. The cast through unknown
 * bypasses Puppeteer's wide Page interface while satisfying the production signature.
 */
function makePageStub(options: { cdpStub?: CdpStub; createCDPSessionError?: Error; isClosedReturn?: boolean } = {}): Page {

  const isClosed = options.isClosedReturn ?? false;

  return {

    createCDPSession: async (): Promise<CDPSession> => {

      if(options.createCDPSessionError) {

        throw options.createCDPSessionError;
      }

      return (options.cdpStub ?? makeCdpStub()) as unknown as CDPSession;
    },
    isClosed: (): boolean => isClosed
  } as unknown as Page;
}

/* Runs a body with every emitted log entry captured, and hands back the warnings among them. Subscribing here rather than in a suite-wide hook keeps the
 * subscription's lifetime exactly the body's, which matters because these rows run in the same process as every other unit test file.
 * @param body - The work to run under capture.
 * @returns The warn-level entries emitted while the body ran.
 */
async function captureWarnings(body: () => Promise<void>): Promise<LogEntry[]> {

  const captured: LogEntry[] = [];
  const unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });

  try {

    await body();
  } finally {

    unsubscribe();
  }

  return captured.filter((entry) => entry.level === "warn");
}

/* A CDP router that answers the window-state read from a scripted sequence and every other command the way the default stub does. Once the script runs out the
 * last answer repeats, so a row that wants an endless state supplies a single-entry script.
 * @param states - The window states to answer with, in order.
 * @returns A send override plus the running count of state reads it has served.
 */
function windowStateRouter(states: readonly string[]): { reads: () => number; send: (method: string) => Promise<unknown> } {

  let served = 0;

  const send = async (method: string): Promise<unknown> => {

    if(method === "Browser.getWindowForTarget") {

      return { windowId: 7 };
    }

    if(method === "Browser.getWindowBounds") {

      const state = states[Math.min(served, states.length - 1)];

      served++;

      return { bounds: { windowState: state } };
    }

    return undefined;
  };

  return { reads: (): number => served, send };
}

describe("withCDPSession", () => {

  test("returns undefined and does not create a session when the page is already closed", async () => {

    // Negative test: the early-exit guard prevents a doomed CDP attach against a closed page. The operation must not run.
    let opCalled = false;

    const result = await withCDPSession(makePageStub({ isClosedReturn: true }), async () => {

      opCalled = true;

      return "should-not-reach";
    });

    assert.equal(result, undefined, "closed page returns undefined");
    assert.equal(opCalled, false, "operation must not have been invoked");
  });

  test("invokes the operation with the CDP session and the resolved window ID, returning its result", async () => {

    const cdpStub = makeCdpStub();

    const result = await withCDPSession(makePageStub({ cdpStub }), async (session, windowId) => {

      // The operation receives the same session reference plus the window ID returned by Browser.getWindowForTarget.
      assert.equal(session, cdpStub as unknown as CDPSession, "session passed through");
      assert.equal(windowId, 7, "windowId resolved from Browser.getWindowForTarget");

      return "operation-result";
    });

    assert.equal(result, "operation-result", "operation's return value surfaces verbatim");
  });

  test("returns undefined when Browser.getWindowForTarget yields no windowId (target invalid)", async () => {

    // Boundary: Chrome can return {} for getWindowForTarget when the target is in a transient state. The helper must short-circuit rather than passing 0/undefined
    // into the operation.
    let opCalled = false;

    const cdpStub = makeCdpStub({ getWindowForTargetResponse: {} });

    const result = await withCDPSession(makePageStub({ cdpStub }), async () => {

      opCalled = true;

      return "should-not-reach";
    });

    assert.equal(result, undefined, "missing windowId -> undefined");
    assert.equal(opCalled, false, "operation must not have run");
  });

  test("returns undefined and swallows errors from createCDPSession (page closed during attach)", async () => {

    const failure = new Error("synthetic createCDPSession failure");

    const result = await withCDPSession(makePageStub({ createCDPSessionError: failure }), async () => "should-not-reach");

    assert.equal(result, undefined, "createCDPSession error -> undefined");
  });

  test("returns undefined when the operation itself throws", async () => {

    // Negative test: errors thrown by the caller's operation are caught by the helper and surface as undefined - the caller treats undefined as "operation
    // declined" without distinguishing failure modes. Locks the contract.
    const result = await withCDPSession<string>(makePageStub(), async (): Promise<string> => {

      throw new Error("operation failed");
    });

    assert.equal(result, undefined, "operation throw -> undefined");
  });

  test("absorbs the 'No target with given id' error silently (expected during page closure)", async () => {

    // The implementation has a special case for the "No target with given id" message that suppresses the warning log. We verify the helper still returns
    // undefined and the error is fully absorbed rather than leaking out.
    const result = await withCDPSession(makePageStub({

      createCDPSessionError: new Error("Protocol error: No target with given id found")
    }), async () => "should-not-reach");

    assert.equal(result, undefined, "expected error -> undefined without rethrow");
  });
});

describe("minimizeWindow", () => {

  test("returns silently when the page is already closed (no CDP traffic)", async () => {

    const cdpStub = makeCdpStub();

    await minimizeWindow(makePageStub({ cdpStub, isClosedReturn: true }));

    assert.equal(cdpStub.calls.length, 0, "no CDP calls issued for a closed page");
  });

  test("issues exactly one setWindowBounds call, carrying windowState: minimized and no dimensions", async () => {

    /* The window's size is not this function's business: pages render at the emulated preset viewport, so a dimension write here would be asking the OS for a
     * size nothing reads. The assertion is both halves - one bounds call, and that call carrying state alone.
     */
    const cdpStub = makeCdpStub();

    await minimizeWindow(makePageStub({ cdpStub }));

    const setBoundsCalls = cdpStub.calls.filter((c) => c.method === "Browser.setWindowBounds");

    assert.equal(setBoundsCalls.length, 1, "exactly one setWindowBounds call");

    const bounds = (setBoundsCalls[0]?.params as { bounds?: { height?: number; width?: number; windowState?: string } }).bounds;

    // Comparing the whole bounds object asserts both halves at once: the state that was asked for, and the absence of any dimension key beside it.
    assert.deepEqual(bounds, { windowState: "minimized" }, "the call carries the minimized state and nothing else");
  });

  test("never reads the window bounds back (nothing is being verified)", async () => {

    // A read-back would only be worth its round trip if there were a resize to confirm. There is not: the command carries a window state and nothing else, so a
    // getWindowBounds call here would cost latency on every pass and tell the caller nothing.
    const cdpStub = makeCdpStub();

    await minimizeWindow(makePageStub({ cdpStub }));

    assert.equal(cdpStub.calls.filter((c) => c.method === "Browser.getWindowBounds").length, 0, "no bounds read-back");
  });

  test("never measures the page (the window's content size is not an input)", async () => {

    /* Nothing sizes the window, so a page.evaluate here would be a live DOM read on the capture page for a value no code consumes. The stub records any evaluate
     * the implementation issues.
     */
    const cdpStub = makeCdpStub();

    let evaluateCallCount = 0;

    const page = {

      createCDPSession: async (): Promise<CDPSession> => cdpStub as unknown as CDPSession,
      evaluate: (): Promise<{ height: number; width: number }> => {

        evaluateCallCount += 1;

        return Promise.resolve({ height: 80, width: 0 });
      },
      isClosed: (): boolean => false
    } as unknown as Page;

    await minimizeWindow(page);

    assert.equal(evaluateCallCount, 0, "no page measurement issued");
  });

  test("resolves the window ID once before issuing the state change", async () => {

    // Every CDP entry through withCDPSession resolves the window ID first. The assertion catches a minimize that reached for a window it never looked up.
    const cdpStub = makeCdpStub();

    await minimizeWindow(makePageStub({ cdpStub }));

    assert.equal(cdpStub.calls.filter((c) => c.method === "Browser.getWindowForTarget").length, 1, "window ID resolved exactly once");
    assert.equal(cdpStub.calls[0]?.method, "Browser.getWindowForTarget", "the lookup precedes the state change");
  });

  test("absorbs CDP errors silently (returns without throwing when the session rejects)", async () => {

    // Negative test: minimizing is a best-effort desktop-hygiene act. A target that closed mid-call must not surface an error into a tune or a recovery cycle.
    const cdpStub = makeCdpStub({

      overrideSend: async (method): Promise<unknown> => {

        if(method === "Browser.getWindowForTarget") {

          return { windowId: 7 };
        }

        throw new Error("synthetic CDP rejection");
      }
    });

    await assert.doesNotReject(() => minimizeWindow(makePageStub({ cdpStub })), "minimizeWindow should swallow CDP errors");
  });
});

describe("unminimizeWindow", () => {

  test("returns silently when the page is already closed (no CDP traffic)", async () => {

    const cdpStub = makeCdpStub();

    await unminimizeWindow(makePageStub({ cdpStub, isClosedReturn: true }));

    assert.equal(cdpStub.calls.length, 0, "no CDP calls for a closed page");
  });

  test("issues a single setWindowBounds call with windowState: normal", async () => {

    const cdpStub = makeCdpStub();

    await unminimizeWindow(makePageStub({ cdpStub }));

    const setBoundsCalls = cdpStub.calls.filter((c) => c.method === "Browser.setWindowBounds");

    assert.equal(setBoundsCalls.length, 1, "exactly one setWindowBounds call");
    assert.equal((setBoundsCalls[0]?.params as { bounds?: { windowState?: string } }).bounds?.windowState, "normal",
      "windowState: normal applied");
  });

  test("calls Browser.getWindowForTarget once to resolve the window ID", async () => {

    // Boundary: every CDP entry through withCDPSession resolves the window ID first. We lock that the unminimize path doesn't skip the lookup.
    const cdpStub = makeCdpStub();

    await unminimizeWindow(makePageStub({ cdpStub }));

    const getWindowForTargetCalls = cdpStub.calls.filter((c) => c.method === "Browser.getWindowForTarget");

    assert.equal(getWindowForTargetCalls.length, 1, "window ID resolved exactly once");
  });

  test("absorbs CDP errors silently (returns without throwing when the session rejects)", async () => {

    // Negative test: when the CDP session rejects (e.g., target closed mid-operation), the helper must return without leaking the error. This protects callers
    // like login/end flows that don't have actionable handling for transient CDP failures.
    const cdpStub = makeCdpStub({

      overrideSend: async (method): Promise<unknown> => {

        if(method === "Browser.getWindowForTarget") {

          return { windowId: 7 };
        }

        throw new Error("synthetic CDP rejection");
      }
    });

    await assert.doesNotReject(() => unminimizeWindow(makePageStub({ cdpStub })),
      "unminimizeWindow should swallow CDP errors");
  });

  test("returns after one read when the window already reports normal", async () => {

    /* The confirmation is what makes the restore a state rather than a command, and this is the price it charges on the common path: one round trip, no sleep.
     * A cadence sleep scheduled ahead of the first read would show up here as a recorded duration.
     */
    const cdpStub = makeCdpStub();
    const clock = new TestClock();

    await unminimizeWindow(makePageStub({ cdpStub }), clock);

    assert.equal(cdpStub.calls.filter((c) => c.method === "Browser.getWindowBounds").length, 1, "exactly one state read for a window already on screen");
    assert.deepEqual(clock.requested, [], "no cadence sleep is paid when the first read already confirms");
  });

  test("polls the window state until Chrome reports normal", async () => {

    // macOS acknowledges setWindowBounds while the window manager is still working, so the state reads back minimized for a while. The restore is confirmed by
    // asking again on the cadence, and the command is issued once regardless of how many reads the confirmation takes.
    const router = windowStateRouter([ "minimized", "minimized", "normal" ]);
    const cdpStub = makeCdpStub({ overrideSend: router.send });
    const clock = new TestClock();

    const running = unminimizeWindow(makePageStub({ cdpStub }), clock);

    await settle();
    assert.equal(clock.pending, 1, "the first read did not confirm, so its cadence is parked on the clock");

    await advanceThroughSchedule(clock, [ WINDOW_STATE_POLL_MS, WINDOW_STATE_POLL_MS ]);
    await running;

    const methods = cdpStub.calls.map((call) => call.method);

    assert.equal(methods.filter((method) => method === "Browser.setWindowBounds").length, 1, "the restore is commanded exactly once");
    assert.equal(router.reads(), 3, "the state is read until it reports normal");
    assert.ok(methods.indexOf("Browser.setWindowBounds") < methods.indexOf("Browser.getWindowBounds"), "the command precedes its confirmation");
    assert.deepEqual(clock.requested, [ WINDOW_STATE_POLL_MS, WINDOW_STATE_POLL_MS ], "one cadence sleep between each pair of reads");
  });

  test("stops at the ceiling and warns, leaving the window in its reported state", async () => {

    /* A window that never reports itself restored must not hold capture hostage. The ceiling ends the confirmation, the warning names the state Chrome is
     * actually reporting, and the call returns so the caller proceeds. The read count is derived from the two exported constants rather than restated, so the
     * row keeps stating the relationship if either moves.
     */
    const router = windowStateRouter(["minimized"]);
    const cdpStub = makeCdpStub({ overrideSend: router.send });
    const clock = new TestClock();

    const warnings = await captureWarnings(async () => {

      const running = unminimizeWindow(makePageStub({ cdpStub }), clock);

      await settle();
      assert.equal(clock.pending, 1, "the first read did not confirm, so its cadence is parked on the clock");

      await drainClock(clock);
      await running;
    });

    assert.equal(clock.now(), WINDOW_RESTORE_CEILING_MS, "virtual time advanced by exactly the cadences the ceiling afforded");
    assert.equal(router.reads(), Math.floor(WINDOW_RESTORE_CEILING_MS / WINDOW_STATE_POLL_MS) + 1, "the ceiling affords one read plus one per cadence");
    assert.equal(warnings.length, 1, "exactly one warning");
    assert.match(warnings[0]?.message ?? "", /did not report a completed restore within 2000ms/, "the warning names the restore and its bound");
    assert.match(warnings[0]?.message ?? "", /minimized/, "the warning carries the state Chrome is reporting");
  });

  test("a getWindowBounds rejection on a later read is absorbed like every other CDP error", async () => {

    /* The confirmation reads through the same session the command went out on, so a read that rejects unwinds into withCDPSession's own swallow-with-warn. The
     * read count proves the poll actually reached the rejecting read rather than stopping at the first.
     */
    let reads = 0;

    const cdpStub = makeCdpStub({

      overrideSend: async (method): Promise<unknown> => {

        if(method === "Browser.getWindowForTarget") {

          return { windowId: 7 };
        }

        if(method === "Browser.getWindowBounds") {

          reads++;

          if(reads === 2) {

            throw new Error("synthetic window-bounds rejection");
          }

          return { bounds: { windowState: "minimized" } };
        }

        return undefined;
      }
    });

    const clock = new TestClock();

    const warnings = await captureWarnings(async () => {

      const running = unminimizeWindow(makePageStub({ cdpStub }), clock);

      // The expectation is attached before the clock is driven, so whatever the second read produces is observed rather than left unhandled.
      const settled = assert.doesNotReject(() => running, "a failed state read must not surface into the caller");

      await settle();
      assert.equal(clock.pending, 1, "the first read did not confirm, so its cadence is parked on the clock");

      await drainClock(clock);
      await settled;
    });

    assert.equal(reads, 2, "the poll reached the rejecting read");
    assert.equal(warnings.length, 1, "exactly one warning");
    assert.match(warnings[0]?.message ?? "", /CDP operation failed/, "the rejection took the existing swallow-with-warn path");
  });
});

describe("readWindowState", () => {

  test("reports the state Chrome carries in the window's bounds", async () => {

    const cdpStub = makeCdpStub({ overrideSend: windowStateRouter(["fullscreen"]).send });

    assert.equal(await readWindowState(makePageStub({ cdpStub })), "fullscreen", "the reported state is returned verbatim");
  });

  test("normalizes an unavailable report to null rather than throwing", async () => {

    // Each way the state is not knowable - a closed page, a response carrying no bounds, and a session that rejects - reads as null, because a caller logging
    // this as a diagnostic has nothing different to do about any of them.
    assert.equal(await readWindowState(makePageStub({ isClosedReturn: true })), null, "a closed page reports no state");
    assert.equal(await readWindowState(makePageStub({ cdpStub: makeCdpStub({ overrideSend: async (): Promise<unknown> => ({ windowId: 7 }) }) })), null,
      "a response carrying no bounds reports no state");
    assert.equal(await readWindowState(makePageStub({ createCDPSessionError: new Error("synthetic attach failure") })), null,
      "a session that cannot be attached reports no state");
  });
});

describe("readWindowPlacement", () => {

  /* A bounds router answering with exactly the fields a row hands it, so a row can leave one out and read what the derivation does about it. Every number is
   * distinct, which is what tells a field read from the right place apart from one read from another.
   * @param bounds - The bounds the response carries, or undefined for a response carrying none.
   * @returns A send override shaped like the other routers in this file.
   */
  function windowBoundsRouter(bounds?: Record<string, unknown>): (method: string) => Promise<unknown> {

    return async (method: string): Promise<unknown> => {

      if(method === "Browser.getWindowForTarget") {

        return { windowId: 7 };
      }

      if(method === "Browser.getWindowBounds") {

        return bounds ? { bounds } : {};
      }

      return undefined;
    };
  }

  test("returns the frame and the state Chrome reports for the window", async () => {

    /* The whole answer a window created beside this one needs, read field by field on four distinct numbers: a derivation that transposed a pair, or read the
     * width where the height belongs, passes a deepEqual against a uniform frame and fails here.
     */
    const cdpStub = makeCdpStub({ overrideSend: windowBoundsRouter({ height: 400, left: 10, top: 20, width: 300, windowState: "normal" }) });

    const placement = await readWindowPlacement(makePageStub({ cdpStub }));

    assert.ok(placement, "a complete report reads as a placement");
    assert.equal(placement.height, 400, "the height is the reported height");
    assert.equal(placement.left, 10, "the left is the reported left");
    assert.equal(placement.top, 20, "the top is the reported top");
    assert.equal(placement.width, 300, "the width is the reported width");
    assert.equal(placement.windowState, "normal", "the state travels with the frame");
  });

  test("reads a window flush against the screen's top-left corner as a placement", async () => {

    // The boundary the derivation's type test exists for: a left or a top of 0 is a real coordinate, and a presence check written as a truthiness test would
    // throw this placement away and leave the new window to Chrome's cascade.
    const cdpStub = makeCdpStub({ overrideSend: windowBoundsRouter({ height: 400, left: 0, top: 0, width: 300, windowState: "normal" }) });

    const placement = await readWindowPlacement(makePageStub({ cdpStub }));

    assert.ok(placement, "a window at the corner still reads as a placement");
    assert.equal(placement.left, 0, "a left of zero survives the derivation");
    assert.equal(placement.top, 0, "a top of zero survives the derivation");
  });

  test("normalizes an incomplete or unavailable report to null", async () => {

    // A partial frame is useless to the one consumer this exists for, so anything short of all four numbers and the state reads as no placement at all - as do
    // the three ways the report is simply not obtainable.
    assert.equal(await readWindowPlacement(makePageStub({ cdpStub: makeCdpStub({ overrideSend: windowBoundsRouter() }) })), null,
      "a response carrying no bounds reports no placement");
    assert.equal(await readWindowPlacement(makePageStub({ cdpStub: makeCdpStub({ overrideSend: windowBoundsRouter({ height: 400, left: 10, top: 20,
      windowState: "normal" }) }) })), null, "bounds missing one of the four numbers report no placement");
    assert.equal(await readWindowPlacement(makePageStub({ cdpStub: makeCdpStub({ overrideSend: windowBoundsRouter({ height: 400, left: 10, top: 20,
      width: 300 }) }) })), null, "bounds carrying no state report no placement");
    assert.equal(await readWindowPlacement(makePageStub({ isClosedReturn: true })), null, "a closed page reports no placement");
    assert.equal(await readWindowPlacement(makePageStub({ createCDPSessionError: new Error("synthetic attach failure") })), null,
      "a session that cannot be attached reports no placement");
  });
});
