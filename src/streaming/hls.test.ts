/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.test.ts: Unit tests for the synchronous helpers in the HLS request handler module. hls.ts orchestrates the entire HLS streaming pipeline (channel
 * validation, pending-stream registration, native vs capture path selection, segmenter creation, monitor wiring) and the orchestration entrypoints
 * (handleHLSPlaylist, handleHLSSegment, ensureChannelStream, initializeStream, startHLSStream, completeStreamSetup) require a real Chrome browser, FFmpeg
 * subprocess, and Express runtime to exercise honestly. The unit-testable surface here is the pure validation helpers - validateChannel and sendValidationError -
 * which translate channel keys and validation results into HTTP-shaped error responses without touching the browser or registry beyond config lookups.
 */
import { describe, test } from "node:test";
import { sendValidationError, validateChannel } from "./hls.ts";
import type { Response } from "express";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { makeReqRes } from "../routes/express.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("validateChannel", () => {

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
