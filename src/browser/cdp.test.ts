/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.test.ts: Unit tests for the Chrome DevTools Protocol helpers in cdp.ts. The module exports withCDPSession (the lifecycle wrapper around a CDP session
 * that surfaces the browser window ID), minimizeWindow (the one-shot that puts the shared window into its minimized state), and unminimizeWindow (the inverse
 * one-shot that restores it). The tests use plain stub objects shaped per the Page and CDPSession contracts - no real browser is launched, and the window's
 * dimensions never enter the picture because these primitives drive presentation state alone. Which state the window should be in is decided in windowSync.ts and
 * pinned there; these tests cover only the command each primitive issues.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { describe, test } from "node:test";
import { minimizeWindow, unminimizeWindow, withCDPSession } from "./cdp.ts";
import assert from "node:assert/strict";

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
});
