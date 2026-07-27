/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.loginMode.test.ts: Unit tests for the login-mode 503 branch of validateChannel. Login mode is owned by browser/login.ts, and validateChannel reads it
 * through isLoginModeActive() (re-exported via browser/index.ts). Rather than substitute the accessor, we drive the REAL flag by calling the production
 * startLoginMode() through the injected setBrowserAccessors() override with a stub browser and page - the same override browser/index.ts wires at startup and
 * precaching.revalidation.test.ts uses - so the test exercises the real login-mode mechanism end to end. clearLoginState() resets the flag and cancels the
 * 15-minute login timeout between tests. The companion happy-path and 404 tests live in hls.test.ts.
 */
import type { Browser, Page } from "puppeteer-core";
import { afterEach, beforeEach, describe, test } from "node:test";
import { clearLoginState, setBrowserAccessors, startLoginMode } from "../browser/login.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { validateChannel } from "./hls.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// Minimal login-page stub. startLoginMode opens a page and calls goto/on/unminimizeWindow against it, none of which need real behavior here; the stub mirrors the
// login.test.ts and precaching.revalidation.test.ts shape.
function makeLoginPageStub(): Page {

  return {

    close: async (): Promise<void> => { /* Nothing to close on a stub. */ },
    goto: async (): Promise<void> => { /* Nothing to navigate on a stub. */ },
    isClosed: (): boolean => false,
    on: (): void => { /* Close-handler registration is irrelevant here. */ }
  } as unknown as Page;
}

// A connected stub browser whose newPage yields the login-page stub, so the real startLoginMode reaches its success path (browser.connected + newPage) without a
// real Chrome.
function makeStubBrowser(): Browser {

  return { connected: true, newPage: async (): Promise<Page> => makeLoginPageStub() } as unknown as Browser;
}

beforeEach(() => {

  // Start each test from a clean login state so one test's login mode cannot leak into the next; also cancels any pending 15-minute login timeout.
  clearLoginState();
});

afterEach(() => {

  // Clear state and cancel the login timeout so the 15-minute timer does not hold the runner open.
  clearLoginState();
});

describe("validateChannel - login mode 503 branch", () => {

  test("returns valid: false with status 503 and a structured error body when login mode is active", async () => {

    /* The 503 branch is the only validateChannel arm that returns an OBJECT body (not a string); the discriminated union's body field is Record<string, string>
     * here, which sendValidationError downstream dispatches via res.json rather than res.send. A regression that returned a plain string would still produce a
     * 503 status but would silently lose the structured error/message fields the Channels DVR client uses to surface the login-in-progress hint. We assert on
     * both fields explicitly so any drop in either would surface here.
     */

    // Drive the real login-mode flag true through the production setBrowserAccessors override: startLoginMode opens the stub login page and flips isLoginModeActive().
    setBrowserAccessors({

      getBrowserInstance: (): Browser => makeStubBrowser(),
      minimizeBrowserWindow: async (): Promise<void> => { /* Not exercised in this test. */ }
    });

    await startLoginMode("https://www.stub-login.test/login");

    const result = validateChannel("abc");

    assert.equal(result.valid, false);

    const failure = result as { body: Record<string, string> | string; statusCode: number };

    assert.equal(failure.statusCode, 503, "503 indicates the login-mode block");
    assert.equal(typeof failure.body, "object", "body is an object so sendValidationError dispatches via res.json");

    const body = failure.body as Record<string, string>;

    assert.equal(body["error"], "Login in progress", "error field identifies the block reason");
    assert.match(body["message"] ?? "", /authentication/i, "message field references the authentication-in-progress condition");
  });

  test("does NOT block the request when login mode is inactive (negative control)", () => {

    /* Companion to the previous test: when login mode is inactive (the beforeEach clean state, no startLoginMode call), validateChannel proceeds past the login
     * check and returns a non-503 result. We assert that the result is not the 503 shape - either it is valid: true (success) or it is one of the other 4xx/5xx
     * arms - to lock the symmetry that login mode is the ONLY producer of the 503 contract.
     */
    const result = validateChannel("abc");

    if(!result.valid) {

      const failure = result as { body: Record<string, string> | string; statusCode: number };

      assert.notEqual(failure.statusCode, 503, "non-login-mode validation must not return 503");
    }
  });
});
