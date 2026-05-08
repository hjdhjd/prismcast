/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.loginMode.test.ts: Unit tests for the login-mode 503 branch of validateChannel. The login-mode flag is owned by browser/login.ts and exposed via
 * isLoginModeActive() re-exported from browser/index.ts; the only public path that flips the flag to true is startLoginMode(), which spawns a real Puppeteer
 * tab and is not viable for a unit test. mock.module + dynamic import is the canonical seam for swapping the accessor without driving a browser. The
 * companion happy-path and 404 tests live in hls.test.ts and use static imports - mixing static and dynamic imports of hls.ts in one file would force every
 * test in that file through the mock.module setup, so the seam is isolated here.
 */
import type * as HlsModule from "./hls.ts";
import { before, beforeEach, describe, mock, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

let mockLoginModeActive = false;
let validateChannel: typeof HlsModule.validateChannel;

before(async () => {

  /* Capture the real exports of browser/index.ts so any export not in our override list passes through unchanged. Mocking the barrel without spreading the
   * real exports would leave any incidental access to a non-listed name resolving to undefined, breaking unrelated imports inside hls.ts (emitCurrentSystemStatus
   * and unregisterManagedPage are also imported from the same barrel).
   */
  const realBrowser = await import("../browser/index.ts");

  const browserUrl = new URL("../browser/index.ts", import.meta.url).href;

  // The Node 22 type definitions surface the option as namedExports; the runtime renamed it to exports in a later minor and emits a deprecation warning. We
  // keep namedExports until @types/node catches up - the runtime path is unaffected and the type definition is authoritative for the build. Same precedent
  // as routes/health.test.ts.
  mock.module(browserUrl, {

    namedExports: {

      ...realBrowser,
      isLoginModeActive: (): boolean => mockLoginModeActive
    }
  });

  // Now that the mock is in place, dynamic-import hls.ts so its captured `import { isLoginModeActive } from "../browser/index.ts"` resolves to the mock. A
  // static import at the top of this file would bind the real export before mock.module had a chance to register the override.
  const hlsModule = await import("./hls.ts");

  validateChannel = hlsModule.validateChannel;
});

beforeEach(() => {

  // Reset the mock state to default-off so per-test mutations cannot leak.
  mockLoginModeActive = false;
});

describe("validateChannel - login mode 503 branch", () => {

  test("returns valid: false with status 503 and a structured error body when login mode is active", () => {

    /* The 503 branch is the only validateChannel arm that returns an OBJECT body (not a string); the discriminated union's body field is Record<string, string>
     * here, which sendValidationError downstream dispatches via res.json rather than res.send. A regression that returned a plain string would still produce a
     * 503 status but would silently lose the structured error/message fields the Channels DVR client uses to surface the login-in-progress hint. We assert on
     * both fields explicitly so any drop in either would surface here.
     */
    mockLoginModeActive = true;

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

    /* Companion to the previous test: when isLoginModeActive() returns false (the default), validateChannel proceeds past the login check and returns a
     * non-503 result. We assert that the result is not the 503 shape - either it is valid: true (success) or it is one of the other 4xx/5xx arms - to lock
     * the symmetry that login mode is the ONLY producer of the 503 contract.
     */
    mockLoginModeActive = false;

    const result = validateChannel("abc");

    if(!result.valid) {

      const failure = result as { body: Record<string, string> | string; statusCode: number };

      assert.notEqual(failure.statusCode, 503, "non-login-mode validation must not return 503");
    }
  });
});
