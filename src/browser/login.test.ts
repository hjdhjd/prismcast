/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * login.test.ts: Unit tests for the login mode state machine in login.ts. The module owns six exports plus a setter (setBrowserAccessors) that injects the
 * browser-side dependencies. Login mode is module-level singleton state, so each test resets the slot via clearLoginState() and re-installs fresh accessors before
 * running. The CDP-backed unminimizeWindow call inside startLoginMode receives a stub Page whose isClosed() returns false, so cdp.ts's early-out guard does not fire;
 * instead the stub omits createCDPSession entirely, so withCDPSession throws when it tries to open a session, catches the error, and returns undefined. That error is
 * swallowed inside withCDPSession and never reaches startLoginMode, so the happy path still succeeds with no real browser, target, or CDP session involved.
 */
import type { Browser, Page } from "puppeteer-core";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { clearLoginState, endLoginMode, getLoginPage, getLoginStatus, isLoginModeActive, setBrowserAccessors, startLoginMode } from "./login.ts";
import type { Nullable } from "../types/index.ts";
import assert from "node:assert/strict";

/* PageStub is the minimal Page surface that startLoginMode and endLoginMode read. We capture the close listeners installed by startLoginMode in onCloseHandlers
 * so tests can dispatch a synthetic "close" event the same way Puppeteer would, without spinning up a real browser.
 */
interface PageStub {

  close: () => Promise<void>;
  closeCalls: number;
  closeShouldThrow: boolean;
  goto: (url: string, options?: unknown) => Promise<void>;
  isClosed: () => boolean;
  isClosedReturn: boolean;
  on: (event: string, handler: () => void) => void;
  onCloseHandlers: (() => void)[];
}

/* makePageStub returns a PageStub that satisfies the Page surface login.ts touches. close() bumps a counter so tests can assert on close-once semantics, and an
 * optional throw flag exercises the catch branch in endLoginMode that swallows close errors. The cast through unknown bypasses Puppeteer's broad Page type while
 * still letting tests pass the stub through the production signature.
 */
function makePageStub(overrides: Partial<{ closeShouldThrow: boolean; isClosedReturn: boolean }> = {}): PageStub {

  const onCloseHandlers: (() => void)[] = [];

  const stub: PageStub = {

    close: async function(): Promise<void> {

      this.closeCalls++;

      if(this.closeShouldThrow) {

        throw new Error("synthetic close failure");
      }
    },
    closeCalls: 0,
    closeShouldThrow: overrides.closeShouldThrow ?? false,
    goto: async (): Promise<void> => Promise.resolve(),
    isClosed: function(): boolean {

      return this.isClosedReturn;
    },
    isClosedReturn: overrides.isClosedReturn ?? false,
    on: (event: string, handler: () => void): void => {

      if(event === "close") {

        onCloseHandlers.push(handler);
      }
    },
    onCloseHandlers
  };

  return stub;
}

/* makeBrowserStub returns a minimal Browser whose newPage() yields the supplied PageStub. The connected flag is configurable so tests can exercise the not-connected
 * early-exit branch in startLoginMode. The cast through unknown bypasses Puppeteer's wide Browser interface.
 */
function makeBrowserStub(options: { connected?: boolean; newPageError?: Error; pageStub?: PageStub } = {}): Browser {

  const connected = options.connected ?? true;

  return {

    connected,
    newPage: async (): Promise<Page> => {

      if(options.newPageError) {

        throw options.newPageError;
      }

      return options.pageStub as unknown as Page;
    }
  } as unknown as Browser;
}

/* installAccessors installs a minimal browser-accessor pair. Tests that need to track minimize calls supply their own counter. The browser reference is captured
 * in a closure so tests can flip its connected flag mid-test by mutating the returned object.
 */
function installAccessors(browser: Nullable<Browser>): { minimizeCalls: number } {

  const counters = { minimizeCalls: 0 };

  setBrowserAccessors({

    getBrowserInstance: (): Nullable<Browser> => browser,
    minimizeBrowserWindow: async (): Promise<void> => {

      counters.minimizeCalls++;
    }
  });

  return counters;
}

describe("isLoginModeActive", () => {

  beforeEach(() => {

    clearLoginState();
  });

  afterEach(() => {

    clearLoginState();
  });

  test("returns false on a fresh module (no login has started)", () => {

    assert.equal(isLoginModeActive(), false, "default state is inactive");
  });

  test("returns true after startLoginMode succeeds", async () => {

    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    const result = await startLoginMode("https://example.test/login");

    assert.equal(result.success, true, "startLoginMode reported success");
    assert.equal(isLoginModeActive(), true, "active flag reflects the active session");
  });

  test("returns false after endLoginMode resolves", async () => {

    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/login");
    await endLoginMode();

    assert.equal(isLoginModeActive(), false, "ended session reverts the flag");
  });
});

describe("getLoginStatus", () => {

  beforeEach(() => {

    clearLoginState();
  });

  afterEach(() => {

    clearLoginState();
  });

  test("returns inactive status with null url and startTime when no login is in progress", () => {

    assert.deepEqual(getLoginStatus(), { active: false, startTime: null, url: null }, "default status snapshot");
  });

  test("reports the URL and a populated startTime once login has started", async () => {

    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });

    try {

      await startLoginMode("https://example.test/x");

      const status = getLoginStatus();

      assert.equal(status.active, true, "active");
      assert.equal(status.url, "https://example.test/x", "stored URL surfaces");
      assert.equal(status.startTime, 1_700_000_000_000, "startTime captured from Date.now()");
    } finally {

      mock.timers.reset();
    }
  });

  test("clears all status fields after endLoginMode", async () => {

    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/x");
    await endLoginMode();

    assert.deepEqual(getLoginStatus(), { active: false, startTime: null, url: null }, "status fully reset after end");
  });
});

describe("getLoginPage", () => {

  beforeEach(() => {

    clearLoginState();
  });

  afterEach(() => {

    clearLoginState();
  });

  test("returns null when login mode is not active (even if a stale page reference is present internally)", () => {

    // The function gates on the active flag - if login has not started, it returns null regardless of any internal state. Locks the contract that callers do not
    // need to check active themselves before calling getLoginPage.
    assert.equal(getLoginPage(), null, "inactive -> null");
  });

  test("returns the active login page after startLoginMode resolves", async () => {

    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/x");

    assert.equal(getLoginPage(), pageStub as unknown as Page, "returned page is the one created by newPage");
  });

  test("returns null after endLoginMode regardless of whether the page reference still exists", async () => {

    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/x");
    await endLoginMode();

    assert.equal(getLoginPage(), null, "ended session returns null");
  });
});

describe("startLoginMode", () => {

  beforeEach(() => {

    clearLoginState();
  });

  afterEach(() => {

    clearLoginState();
  });

  test("returns a success result and records URL plus active state on the happy path", async () => {

    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    const result = await startLoginMode("https://example.test/login");

    assert.deepEqual(result, { success: true }, "happy path returns the documented success shape");
    assert.equal(getLoginStatus().active, true, "active flag set");
    assert.equal(getLoginStatus().url, "https://example.test/login", "URL stored");
  });

  test("returns failure when login mode is already active (refuses to start a second session)", async () => {

    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/first");

    const second = await startLoginMode("https://example.test/second");

    assert.equal(second.success, false, "second start returns failure");
    assert.equal(second.error, "Login is already in progress.", "diagnostic reason surfaces");
    assert.equal(getLoginStatus().url, "https://example.test/first", "URL of the first session is preserved");
  });

  test("returns failure when no browser accessors have been installed (browser unavailable)", async () => {

    setBrowserAccessors({

      getBrowserInstance: (): Nullable<Browser> => null,
      minimizeBrowserWindow: async (): Promise<void> => Promise.resolve()
    });

    const result = await startLoginMode("https://example.test/x");

    assert.equal(result.success, false, "no browser -> failure");
    assert.equal(result.error, "Browser is not connected.", "diagnostic reason surfaces");
    assert.equal(isLoginModeActive(), false, "no state was committed");
  });

  test("returns failure when the browser is disconnected", async () => {

    installAccessors(makeBrowserStub({ connected: false }));

    const result = await startLoginMode("https://example.test/x");

    assert.equal(result.success, false, "disconnected browser -> failure");
    assert.equal(result.error, "Browser is not connected.", "same diagnostic regardless of null vs disconnected");
  });

  test("returns failure with the formatted error when newPage rejects", async () => {

    // Negative test: if browser.newPage() rejects, the catch branch surfaces the formatted error and resets state. The page reference must not leak.
    const failure = new Error("synthetic newPage rejection");

    installAccessors(makeBrowserStub({ newPageError: failure }));

    const result = await startLoginMode("https://example.test/x");

    assert.equal(result.success, false, "newPage failure -> failure result");
    assert.match(result.error ?? "", /synthetic newPage rejection/, "error message preserved verbatim");
    assert.equal(isLoginModeActive(), false, "no partial state committed");
  });

  test("registers a close listener that triggers endLoginMode when the user closes the tab", async () => {

    // The close handler is fire-and-forget (void endLoginMode()). We capture the registered handler, invoke it, then poll for the active flag to clear since the
    // promise it kicks off resolves on the microtask queue.
    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/x");

    assert.equal(pageStub.onCloseHandlers.length, 1, "exactly one close handler registered");

    pageStub.onCloseHandlers[0]?.();

    // Drain the microtask queue so the void endLoginMode() promise can settle. We yield three times: endLoginMode's await chain has two stages (page close +
    // minimize), and a third yield gives margin so the trailing continuation lands before we assert. expectAt-style polling would be heavier than necessary here
    // since the test runs in-process with no real timers.
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    assert.equal(isLoginModeActive(), false, "close handler ran endLoginMode and cleared active state");
  });
});

describe("endLoginMode", () => {

  beforeEach(() => {

    clearLoginState();
  });

  afterEach(() => {

    clearLoginState();
  });

  test("is a no-op when login mode is not active (idempotent)", async () => {

    // Negative test: callers may invoke endLoginMode unconditionally during cleanup. The function must tolerate that without throwing or running browser
    // operations against an inactive session.
    const counters = installAccessors(makeBrowserStub({ pageStub: makePageStub() }));

    await endLoginMode();

    assert.equal(counters.minimizeCalls, 0, "no minimize call when nothing was active");
  });

  test("closes the login page and minimizes the browser when an active session exists", async () => {

    const pageStub = makePageStub();
    const counters = installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/x");
    await endLoginMode();

    assert.equal(pageStub.closeCalls, 1, "page.close called exactly once");
    assert.equal(counters.minimizeCalls, 1, "browser was minimized after the session ended");
  });

  test("skips closing the page when isClosed reports true (avoids redundant close)", async () => {

    const pageStub = makePageStub();
    const counters = installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/x");

    pageStub.isClosedReturn = true;

    await endLoginMode();

    assert.equal(pageStub.closeCalls, 0, "page.close NOT called on an already-closed page");
    assert.equal(counters.minimizeCalls, 1, "browser still minimized regardless");
  });

  test("swallows page.close errors and still completes the cleanup path", async () => {

    // Negative test: page.close() can throw "Target closed" when the tab is in transition. The catch branch must absorb that and let the rest of the cleanup
    // proceed.
    const pageStub = makePageStub({ closeShouldThrow: true });
    const counters = installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/x");

    await assert.doesNotReject(() => endLoginMode(), "endLoginMode should swallow page.close errors");

    assert.equal(counters.minimizeCalls, 1, "browser still minimized despite page.close failure");
    assert.equal(isLoginModeActive(), false, "state still reset");
  });

  test("does not minimize when the browser disconnected during the session", async () => {

    // Boundary: if the browser dropped between start and end, minimizeBrowserWindow should not be called - the accessor returns a disconnected browser. This
    // ensures we don't try to drive CDP against a dead connection.
    const pageStub = makePageStub();
    const browser = makeBrowserStub({ connected: true, pageStub });
    const counters = installAccessors(browser);

    await startLoginMode("https://example.test/x");

    // Simulate the browser losing its connection.
    (browser as unknown as { connected: boolean }).connected = false;

    await endLoginMode();

    assert.equal(counters.minimizeCalls, 0, "no minimize attempt against a disconnected browser");
    assert.equal(isLoginModeActive(), false, "state still cleared");
  });
});

describe("clearLoginState", () => {

  beforeEach(() => {

    clearLoginState();
  });

  afterEach(() => {

    clearLoginState();
  });

  test("returns false when login mode is not active (no work to do)", () => {

    assert.equal(clearLoginState(), false, "inactive -> false");
  });

  test("returns true and resets state when login mode is active", async () => {

    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/x");

    const cleared = clearLoginState();

    assert.equal(cleared, true, "returns true on the active path");
    assert.equal(isLoginModeActive(), false, "active flag cleared");
    assert.deepEqual(getLoginStatus(), { active: false, startTime: null, url: null }, "status fully reset");
  });

  test("does NOT call page.close or minimize (the browser-crash variant of cleanup)", async () => {

    // The contract distinction between clearLoginState and endLoginMode: clearLoginState assumes the browser is gone, so it must not invoke any browser-touching
    // operations. We assert by counting close calls and minimize calls.
    const pageStub = makePageStub();
    const counters = installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/x");
    clearLoginState();

    assert.equal(pageStub.closeCalls, 0, "page.close not invoked on the crash-cleanup path");
    assert.equal(counters.minimizeCalls, 0, "minimize not invoked on the crash-cleanup path");
  });

  test("a second call after a successful clear is a no-op (idempotent)", async () => {

    const pageStub = makePageStub();

    installAccessors(makeBrowserStub({ pageStub }));

    await startLoginMode("https://example.test/x");

    assert.equal(clearLoginState(), true, "first call clears");
    assert.equal(clearLoginState(), false, "second call reports no work");
  });
});
