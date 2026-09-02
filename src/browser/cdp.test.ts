/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.test.ts: Unit tests for the Chrome DevTools Protocol helpers in cdp.ts. The module exports withCDPSession (the lifecycle wrapper around a CDP session
 * that surfaces the browser window ID), minimizeWindow (the one-shot that puts the shared window into its minimized state), unminimizeWindow (which commands the
 * restore and then confirms it against Chrome's own report), readWindowState (that report, read for a page), and reaffirmCaptureSurface (the raw re-issue of a
 * capture page's declared device metrics). The tests use plain stub objects shaped per the Page and CDPSession contracts - no real browser is launched, and the
 * window's dimensions never enter the picture because the two window primitives drive presentation state alone. Which state the window should be in is decided in
 * windowSync.ts and pinned there; these tests cover only the commands each primitive issues and the confirmation the restore waits on.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { WINDOW_RESTORE_CEILING_MS, WINDOW_STATE_POLL_MS, minimizeWindow, readWindowState, reaffirmCaptureSurface, unminimizeWindow,
  withCDPSession } from "./cdp.ts";
import { describe, test } from "node:test";
import { makeAdvancingClock, makeFakeClock } from "../utils/clock.helpers.ts";
import type { LogEntry } from "../utils/logEmitter.ts";
import type { Nullable } from "../types/index.ts";
import assert from "node:assert/strict";
import { subscribeToLogs } from "../utils/logEmitter.ts";

/* CdpStub captures every send() call so tests can assert on the command sequence. The send() implementation routes by method name to either the test-supplied
 * response factory or a sensible default - Browser.getWindowForTarget always returns windowId 7, and every other command resolves with nothing, which is what
 * Chrome's window-state commands themselves return.
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
     * size nothing reads. The pin is both halves - one bounds call, and that call carrying state alone.
     */
    const cdpStub = makeCdpStub();

    await minimizeWindow(makePageStub({ cdpStub }));

    const setBoundsCalls = cdpStub.calls.filter((c) => c.method === "Browser.setWindowBounds");

    assert.equal(setBoundsCalls.length, 1, "exactly one setWindowBounds call");

    const bounds = (setBoundsCalls[0]?.params as { bounds?: { height?: number; width?: number; windowState?: string } }).bounds;

    // Comparing the whole bounds object pins both halves at once: the state that was asked for, and the absence of any dimension key beside it.
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

    /* The chrome-dimension measurement fed the resize target. Nothing sizes the window now, so a page.evaluate here would be a live DOM read on the capture page
     * for a value no code consumes. The stub records any evaluate the implementation issues.
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

    // Every CDP entry through withCDPSession resolves the window ID first. The pin catches a minimize that reached for a window it never looked up.
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
    const { clock, sleeps } = makeFakeClock();

    await unminimizeWindow(makePageStub({ cdpStub }), clock);

    assert.equal(cdpStub.calls.filter((c) => c.method === "Browser.getWindowBounds").length, 1, "exactly one state read for a window already on screen");
    assert.deepEqual(sleeps, [], "no cadence sleep is paid when the first read already confirms");
  });

  test("polls the window state until Chrome reports normal", async () => {

    // macOS acknowledges setWindowBounds while the window manager is still working, so the state reads back minimized for a while. The restore is confirmed by
    // asking again on the cadence, and the command is issued once regardless of how many reads the confirmation takes.
    const router = windowStateRouter([ "minimized", "minimized", "normal" ]);
    const cdpStub = makeCdpStub({ overrideSend: router.send });
    const { clock, sleeps } = makeFakeClock();

    await unminimizeWindow(makePageStub({ cdpStub }), clock);

    const methods = cdpStub.calls.map((call) => call.method);

    assert.equal(methods.filter((method) => method === "Browser.setWindowBounds").length, 1, "the restore is commanded exactly once");
    assert.equal(router.reads(), 3, "the state is read until it reports normal");
    assert.ok(methods.indexOf("Browser.setWindowBounds") < methods.indexOf("Browser.getWindowBounds"), "the command precedes its confirmation");
    assert.deepEqual(sleeps, [ WINDOW_STATE_POLL_MS, WINDOW_STATE_POLL_MS ], "one cadence sleep between each pair of reads");
  });

  test("stops at the ceiling and warns, leaving the window in its reported state", async () => {

    /* A window that never reports itself restored must not hold capture hostage. The ceiling ends the confirmation, the warning names the state Chrome is
     * actually reporting, and the call returns so the caller proceeds. The read count is derived from the two exported constants rather than restated, so the
     * row keeps stating the relationship if either moves.
     */
    const router = windowStateRouter(["minimized"]);
    const cdpStub = makeCdpStub({ overrideSend: router.send });
    const { clock } = makeAdvancingClock();

    const warnings = await captureWarnings(async () => {

      await unminimizeWindow(makePageStub({ cdpStub }), clock);
    });

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

    const { clock } = makeFakeClock();

    const warnings = await captureWarnings(async () => {

      await assert.doesNotReject(() => unminimizeWindow(makePageStub({ cdpStub }), clock), "a failed state read must not surface into the caller");
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

    // Three ways the state is simply not knowable - a closed page, a response carrying no bounds, and a session that rejects - all read as null, because a
    // caller logging this as a diagnostic has nothing different to do about any of them.
    assert.equal(await readWindowState(makePageStub({ isClosedReturn: true })), null, "a closed page reports no state");
    assert.equal(await readWindowState(makePageStub({ cdpStub: makeCdpStub({ overrideSend: async (): Promise<unknown> => ({ windowId: 7 }) }) })), null,
      "a response carrying no bounds reports no state");
    assert.equal(await readWindowState(makePageStub({ createCDPSessionError: new Error("synthetic attach failure") })), null,
      "a session that cannot be attached reports no state");
  });
});

/* A page double for the re-affirmation, which reads the page's own viewport record rather than any window state. It reports whatever viewport a row hands it,
 * records the session it created so the row can read back what was sent and whether the session was released, and can be told to reject either the send or the
 * detach.
 */
interface ReaffirmPageStub {

  detachCalls: number;
  page: Page;
  sends: { method: string; params: unknown }[];
}

/**
 * Builds the re-affirmation page double.
 * @param options - The viewport the page reports and the failures to simulate.
 * @param options.detachError - The error the session's detach rejects with, when the row wants a failing release.
 * @param options.sendError - The error the session's send rejects with, when the row wants a failing command.
 * @param options.viewport - The viewport record page.viewport() answers with. Null models a page carrying no emulation at all.
 * @returns The double plus the recordings a row asserts against.
 */
function makeReaffirmPageStub(options: { detachError?: Error; sendError?: Error;
  viewport?: Nullable<{ deviceScaleFactor?: number; height: number; width: number }>; } = {}): ReaffirmPageStub {

  /* Deliberately not a preset size. No quality preset is 1400x788, so a row's expected values can only be produced by an implementation that reads the page's own
   * viewport record - one that reached for the configured preset instead would send a preset's dimensions and fail here.
   */
  const viewport = (options.viewport === undefined) ? { deviceScaleFactor: 2, height: 788, width: 1400 } : options.viewport;

  const stub: ReaffirmPageStub = {

    detachCalls: 0,
    page: null as unknown as Page,
    sends: []
  };

  stub.page = {

    createCDPSession: async (): Promise<CDPSession> => ({

      detach: async (): Promise<void> => {

        stub.detachCalls++;

        if(options.detachError) {

          throw options.detachError;
        }
      },
      send: async (method: string, params?: unknown): Promise<unknown> => {

        stub.sends.push({ method, params });

        if(options.sendError) {

          throw options.sendError;
        }

        return undefined;
      }
    } as unknown as CDPSession),
    isClosed: (): boolean => false,
    viewport: (): Nullable<{ deviceScaleFactor?: number; height: number; width: number }> => viewport
  } as unknown as Page;

  return stub;
}

describe("reaffirmCaptureSurface", () => {

  test("re-issues the page's own declared metrics once and releases the session", async () => {

    /* The command carries exactly what the page's viewport record holds, plus the non-mobile flag Chrome's own emulation manager sends. Reading the record rather
     * than the configured preset is the whole point: the two agree in production and would diverge silently the moment a preset changed mid-stream.
     */
    const stub = makeReaffirmPageStub();

    await reaffirmCaptureSurface(stub.page);

    assert.equal(stub.sends.length, 1, "exactly one command was sent");
    assert.equal(stub.sends[0]?.method, "Emulation.setDeviceMetricsOverride", "the override command is what re-selects the composition target");
    assert.deepEqual(stub.sends[0].params, { deviceScaleFactor: 2, height: 788, mobile: false, width: 1400 },
      "the page's own declared dimensions and density are what get re-issued");
    assert.equal(stub.detachCalls, 1, "the session is released once the command has been sent");
  });

  test("sends nothing for a page whose declared density is not positive", async () => {

    // The launch default declares a density of zero, which is Chrome's marker for native scaling. Such a page is not a capture page, and the guard is what makes
    // this function safe to fire at any page from any trigger.
    const stub = makeReaffirmPageStub({ viewport: { deviceScaleFactor: 0, height: 1080, width: 1920 } });

    await reaffirmCaptureSurface(stub.page);

    assert.deepEqual(stub.sends, [], "no command is issued against a page carrying no explicit density");
    assert.equal(stub.detachCalls, 0, "no session was created to release");
  });

  test("sends nothing for a page carrying no viewport at all", async () => {

    // The login page clears its emulation outright, leaving a null record. The object itself has to be narrowed, not only the density it would carry.
    const stub = makeReaffirmPageStub({ viewport: null });

    await reaffirmCaptureSurface(stub.page);

    assert.deepEqual(stub.sends, [], "no command is issued against an un-emulated page");
    assert.equal(stub.detachCalls, 0, "no session was created to release");
  });

  test("propagates a failing command to the caller and still releases the session", async () => {

    // Establishment's own re-affirmation runs on a resource stack that unwinds on any throw, so the failure has to reach it rather than being swallowed here.
    const sendError = new Error("synthetic override rejection");
    const stub = makeReaffirmPageStub({ sendError });

    await assert.rejects(() => reaffirmCaptureSurface(stub.page), (error: unknown) => error === sendError,
      "the send's own rejection reaches the caller");

    assert.equal(stub.detachCalls, 1, "the session is released even when the command failed");
  });

  test("keeps the command's own rejection when the release fails too", async () => {

    /* A page dying mid-command takes its session with it, so the release can fail for the same reason the command did. The reason the caller receives has to stay
     * the command's: a release failure raised out of the cleanup would replace a diagnosable error with one nobody can act on.
     */
    const sendError = new Error("synthetic override rejection");
    const stub = makeReaffirmPageStub({ detachError: new Error("synthetic detach rejection"), sendError });

    await assert.rejects(() => reaffirmCaptureSurface(stub.page), (error: unknown) => error === sendError,
      "the caller sees the command's reason, not the release's");

    assert.equal(stub.detachCalls, 1, "the release was still attempted");
  });
});
