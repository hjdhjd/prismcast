/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.test.ts: Unit tests for the Chrome DevTools Protocol helpers in cdp.ts. The module exports three functions: withCDPSession (the lifecycle wrapper around a
 * CDP session that surfaces the browser window ID), resizeAndMinimizeWindow (the resize-then-minimize routine that drives Browser.setWindowBounds), and
 * unminimizeWindow (the inverse one-shot that restores window state). The tests use plain stub objects shaped per the Page and CDPSession contracts - no real
 * browser is launched. The browser-chrome dimensions cache in display.ts is primed before each resize test so the page.evaluate fallback never runs.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { afterEach, beforeEach, describe, test } from "node:test";
import { getBrowserChrome, setBrowserChrome } from "./display.ts";
import { resizeAndMinimizeWindow, unminimizeWindow, withCDPSession } from "./cdp.ts";
import { CONFIG } from "../config/index.ts";
import type { Nullable } from "../types/index.ts";
import assert from "node:assert/strict";
import { getEffectiveViewport } from "../config/presets.ts";

/* CdpStub captures every send() call so tests can assert on the command sequence. The send() implementation routes by method name to either the test-supplied
 * response factory or a sensible default - Browser.getWindowForTarget always returns windowId 7, Browser.getWindowBounds returns the most recently set bounds.
 */
interface CdpStub {

  calls: { method: string; params: unknown }[];
  lastBounds: Nullable<{ height?: number; width?: number; windowState?: string }>;
  send: (method: string, params?: unknown) => Promise<unknown>;
}

/* makeCdpStub returns a CDPSession-shaped stub. The optional getWindowForTargetResponse override lets a test simulate an invalid target by returning {} (no
 * windowId), and overrideSend lets a test substitute a custom command router (used for the "page closes during operation" case where send() rejects partway).
 */
function makeCdpStub(options: { getWindowForTargetResponse?: { windowId?: number }; overrideSend?: (method: string, params?: unknown) => Promise<unknown> }
  = {}): CdpStub {

  const calls: { method: string; params: unknown }[] = [];
  let lastBounds: Nullable<{ height?: number; width?: number; windowState?: string }> = null;

  const send = async (method: string, params?: unknown): Promise<unknown> => {

    calls.push({ method, params });

    if(options.overrideSend) {

      return options.overrideSend(method, params);
    }

    if(method === "Browser.getWindowForTarget") {

      return options.getWindowForTargetResponse ?? { windowId: 7 };
    }

    if(method === "Browser.setWindowBounds") {

      // Record the bounds we just set so a subsequent getWindowBounds returns them. This mimics Chrome's normal behavior - the bounds we asked for are reflected
      // back when we read them.
      const bounds = (params as { bounds?: { height?: number; width?: number; windowState?: string } }).bounds;

      if(bounds && (("height" in bounds) || ("width" in bounds))) {

        // Only record dimension changes; pure windowState changes don't update the dimensions cache.
        lastBounds = { height: bounds.height, width: bounds.width };
      }

      return Promise.resolve(undefined);
    }

    if(method === "Browser.getWindowBounds") {

      return { bounds: lastBounds ?? {} };
    }

    return Promise.resolve(undefined);
  };

  return {

    calls,
    get lastBounds(): Nullable<{ height?: number; width?: number; windowState?: string }> {

      return lastBounds;
    },
    set lastBounds(value: Nullable<{ height?: number; width?: number; windowState?: string }>) {

      lastBounds = value;
    },
    send
  };
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

describe("resizeAndMinimizeWindow", () => {

  let originalChrome: Nullable<{ height: number; width: number }>;

  beforeEach(() => {

    originalChrome = getBrowserChrome();

    // Prime the chrome cache so resizeAndMinimizeWindow does not call page.evaluate(). The helper short-circuits to the cached value when present.
    setBrowserChrome(0, 70);
  });

  afterEach(() => {

    if(originalChrome) {

      setBrowserChrome(originalChrome.width, originalChrome.height);
    }
  });

  test("returns silently when the page is already closed (no CDP traffic)", async () => {

    const cdpStub = makeCdpStub();

    await resizeAndMinimizeWindow(makePageStub({ cdpStub, isClosedReturn: true }));

    assert.equal(cdpStub.calls.length, 0, "no CDP calls issued for a closed page");
  });

  test("resizes to viewport+chrome and minimizes when chrome cache is primed and dimensions match on first attempt", async () => {

    // The CDP stub's getWindowBounds reflects whatever was just set, so the readback verification matches on the first attempt and no retries are needed.
    const cdpStub = makeCdpStub();

    await resizeAndMinimizeWindow(makePageStub({ cdpStub }));

    const viewport = getEffectiveViewport(CONFIG);
    const chrome = getBrowserChrome();

    assert.ok(chrome, "chrome cache primed");

    const expectedHeight = viewport.height + chrome.height;
    const expectedWidth = viewport.width + chrome.width;

    // Verify the dimension-setting call carries viewport+chrome.
    const setBoundsDimensionCalls = cdpStub.calls.filter((c) => {

      if(c.method !== "Browser.setWindowBounds") {

        return false;
      }

      const bounds = (c.params as { bounds?: { height?: number; width?: number } }).bounds;

      if(!bounds) {

        return false;
      }

      return (bounds.height === expectedHeight) && (bounds.width === expectedWidth);
    });

    assert.equal(setBoundsDimensionCalls.length, 1, "setWindowBounds called once with viewport+chrome dimensions");

    // Verify a minimize call landed.
    const minimizeCalls = cdpStub.calls.filter((c) => {

      if(c.method !== "Browser.setWindowBounds") {

        return false;
      }

      return (c.params as { bounds?: { windowState?: string } }).bounds?.windowState === "minimized";
    });

    assert.equal(minimizeCalls.length, 1, "minimize call issued exactly once");
  });

  test("issues a normal-state setWindowBounds before each dimension write (so the resize is applied to a non-maximized window)", async () => {

    const cdpStub = makeCdpStub();

    await resizeAndMinimizeWindow(makePageStub({ cdpStub }));

    // The first setWindowBounds call must have windowState: "normal" - this restores from any prior maximized state so the dimension write is applied.
    const firstSetBounds = cdpStub.calls.find((c) => c.method === "Browser.setWindowBounds");

    assert.ok(firstSetBounds, "at least one setWindowBounds call");

    assert.equal((firstSetBounds.params as { bounds?: { windowState?: string } }).bounds?.windowState, "normal",
      "first setWindowBounds carries windowState: normal");
  });

  test("verifies the resize via getWindowBounds (the readback step in the loop)", async () => {

    const cdpStub = makeCdpStub();

    await resizeAndMinimizeWindow(makePageStub({ cdpStub }));

    const getBoundsCalls = cdpStub.calls.filter((c) => c.method === "Browser.getWindowBounds");

    assert.ok(getBoundsCalls.length >= 1, "at least one getWindowBounds verification call");
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
