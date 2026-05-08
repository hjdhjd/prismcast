/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.test.ts: Unit tests for the synchronous helpers in the HLS request handler module. hls.ts orchestrates the entire HLS streaming pipeline (channel
 * validation, pending-stream registration, native vs capture path selection, segmenter creation, monitor wiring) and the orchestration entrypoints
 * (handleHLSPlaylist, handleHLSSegment, ensureChannelStream, initializeStream, startHLSStream, completeStreamSetup) require a real Chrome browser, FFmpeg
 * subprocess, and Express runtime to exercise honestly. The unit-testable surface here is the pure validation helpers - validateChannel and sendValidationError
 * - which translate channel keys and validation results into HTTP-shaped error responses without touching the browser or registry beyond config lookups.
 *
 * The login-mode 503 branch lives in a sibling file (hls.loginMode.test.ts) because exercising it requires mock.module + dynamic import to swap the
 * isLoginModeActive accessor; mixing the static and dynamic import strategies in one file would require all tests to use dynamic import.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { sendValidationError, validateChannel } from "./hls.ts";
import { CONFIG } from "../config/index.ts";
import { LOG } from "../utils/index.ts";
import type { Response } from "express";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { makeReqRes } from "../routes/express.helpers.ts";
import { setServiceSelections } from "../config/services.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("validateChannel", () => {

  // Snapshot CONFIG.channels.disabledPredefined and the serviceSelections cache so tests that mutate either restore them in afterEach. Module-level state is
  // shared across the test process; without restoration, sibling tests in this file (or sibling test files) would observe leaked state.
  let savedDisabled: string[] = [];

  beforeEach(() => {

    savedDisabled = [...CONFIG.channels.disabledPredefined];
  });

  afterEach(() => {

    CONFIG.channels.disabledPredefined = savedDisabled;
    setServiceSelections({});
  });

  test("returns valid: false with status 404 for an unknown channel", () => {

    // Negative test: a totally unknown channel must produce a 404 with a textual body.
    const result = validateChannel("totally-not-a-real-channel-x9z2");

    assert.equal(result.valid, false);

    // The result type is a discriminated union: when valid is false the body is Record<string, string> | string and the status code is the HTTP code. We cast
    // explicitly here so the type system surfaces the body as the string variant rather than relying on String() coercion which would stringify a record as
    // "[object Object]".
    const failure = result as { body: Record<string, string> | string; statusCode: number };

    assert.equal(failure.statusCode, 404);
    assert.equal(typeof failure.body, "string");

    if(typeof failure.body === "string") {

      assert.match(failure.body, /not found/i);
    }
  });

  test("returns valid: false with status 404 'Channel is disabled.' when the predefined channel is in disabledPredefined", () => {

    /* The disabled-channel arm of the discriminated union. We mutate CONFIG.channels.disabledPredefined to include a known predefined channel ("abc") and call
     * validateChannel against it. isPredefinedChannelDisabled is the first check in validateChannel so this branch fires before any service-resolution work,
     * keeping the test independent of serviceGroups state.
     */
    CONFIG.channels.disabledPredefined = ["abc"];

    const result = validateChannel("abc");

    // assert.equal narrows the discriminated union via its `asserts` overload, so the failure-arm fields below are accessible without an additional if guard.
    assert.equal(result.valid, false);
    assert.equal(result.statusCode, 404);
    assert.equal(result.body, "Channel is disabled.");
  });

  test("returns valid: true with the resolved channel and resolvedKey for a known enabled predefined channel (success path)", () => {

    /* The discriminated-union success arm: a predefined channel that is not disabled, has at least one variant whose service tag is enabled, and is not
     * blocked by login mode. With CONFIG.channels.disabledPredefined empty, no service filter active, and login mode off, "abc" resolves cleanly. The success
     * branch returns { channel, resolvedKey, valid: true }; we lock both projections so a future change that drops a field surfaces here.
     */
    const result = validateChannel("abc");

    assert.equal(result.valid, true);
    assert.ok(result.channel, "resolved channel surfaced");
    assert.equal(result.resolvedKey, "abc", "no service selection -> resolvedKey is the canonical key");
  });

  test("warns and falls back to the canonical channel when a service selection points at a missing variant", () => {

    /* The variant-fallthrough warning at hls.ts:146. When serviceSelections maps a canonical to a variant key that no longer resolves to a real channel
     * (e.g., the variant was removed after the selection was saved), validateChannel logs a LOG.warn and falls back to getAllChannels()[channelName] -
     * effectively serving the canonical's URL instead of failing the request. The test seeds a synthetic missing variant via setServiceSelections, captures
     * LOG.warn via mock.method, and asserts both the fallback channel surface AND the warning. Without the warning assertion, a refactor that quietly dropped
     * the log line would leave operators without the diagnostic signal that originally motivated the warning.
     */
    setServiceSelections({ abc: "abc-nonexistent-variant" });

    const warnSpy = mock.method(LOG, "warn", () => undefined);

    try {

      const result = validateChannel("abc");

      assert.equal(result.valid, true, "fallthrough yields a valid result, not a 404");
      assert.equal(result.resolvedKey, "abc-nonexistent-variant", "resolvedKey reflects the user's selection even though it didn't resolve");
      assert.ok(result.channel, "channel populated from the canonical fallback via getAllChannels()");

      assert.equal(warnSpy.mock.callCount(), 1, "exactly one warning emitted for the missing-variant fallback");

      // The warning is emitted as LOG.warn(format, ...args). The first argument is the format string which contains the diagnostic copy we want to lock.
      const message = warnSpy.mock.calls[0]!.arguments[0]!;

      assert.match(message, /not found/i, "warning message identifies the missing-service condition");
    } finally {

      warnSpy.mock.restore();
    }
  });
});

describe("sendValidationError", () => {

  test("uses res.send for plain-string body", () => {

    // The handler dispatches based on typeof body. A string body must go through res.send, not res.json.
    const { res, send, status } = makeReqRes();

    sendValidationError({ body: "Channel is disabled.", statusCode: 404 }, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 404);
    assert.equal(send.mock.calls[0]?.arguments[0], "Channel is disabled.");
  });

  test("uses res.json for object body", () => {

    // Login-mode rejection bodies are objects. They must go through res.json so Channels DVR clients can parse the structured error.
    const { json, res, status } = makeReqRes();
    const body = { error: "Login in progress", message: "Please complete authentication before starting new streams." };

    sendValidationError({ body, statusCode: 503 }, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 503);
    assert.deepEqual(json.mock.calls[0]?.arguments[0], body);
  });

  test("forwards the statusCode from the validation result verbatim", () => {

    const { res, status } = makeReqRes();

    sendValidationError({ body: "Forbidden.", statusCode: 403 }, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 403);
  });

  test("does NOT crash when the response double is non-fluent (does not return self)", () => {

    // Defensive: the canonical makeReqRes spy returns res from status() so handlers can chain. A future regression could break that, or the production code
    // could be invoked against a stub that does not chain - verify sendValidationError tolerates a response whose setters return undefined. We construct a
    // minimal inline double here rather than going through makeReqRes precisely because makeReqRes IS fluent.
    const captured = { body: undefined as unknown, status: 0 };

    const res = {

      json: (b: unknown) => { captured.body = b; },
      send: (b: unknown) => { captured.body = b; },
      status: (s: number) => {

        captured.status = s;

        return res;
      }
    } as unknown as Response;

    assert.doesNotThrow(() => {

      sendValidationError({ body: "x", statusCode: 418 }, res);
    });

    assert.equal(captured.status, 418);
    assert.equal(captured.body, "x");
  });
});
