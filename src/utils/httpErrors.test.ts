/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * httpErrors.test.ts: Unit tests for the request error handler every PrismCast Express application installs. The handler is driven directly with a synthetic request and
 * response, once on each side of the headers-sent branch: before headers are sent it answers 500 itself, and after it hands the error on, so Express's default
 * handler closes the connection. The row asserting that the HDHomeRun server installs it lives in hdhr/index.test.ts, beside the server it reaches.
 */
import { describe, test } from "node:test";
import { LOG } from "./logger.ts";
import assert from "node:assert/strict";
import { handleRequestError } from "./httpErrors.ts";
import { makeReqRes } from "../routes/express.helpers.ts";

describe("handleRequestError", () => {

  // The line the handler logs, with the request it failed on in the context object.
  const LOGGED = [ "A request failed with an unhandled error.", { error: "The route failed", method: "GET", url: "/boom" } ];

  test("answers 500 with a sentence and logs the error while the response has not started", (t) => {

    const logged = t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });
    const next = t.mock.fn((_error?: unknown): void => { /* Captured via the mock. */ });
    const { req, res, send, status } = makeReqRes();

    Object.assign(req, { method: "GET", originalUrl: "/boom" });

    handleRequestError(new Error("The route failed."), req, res, next);

    assert.deepEqual(status.mock.calls.map((call) => call.arguments), [[500]], "the handler answered 500");
    assert.deepEqual(send.mock.calls.map((call) => call.arguments), [["Internal server error."]], "the body is a sentence, never the error's stack");
    assert.equal(next.mock.callCount(), 0, "the handler answered the request itself");
    assert.deepEqual(logged.mock.calls.map((call) => call.arguments), [LOGGED], "the error and its request were logged once");
  });

  test("hands the error to Express's default handler once headers are sent, answering nothing itself", (t) => {

    const logged = t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });
    const error = new Error("The route failed.");
    const next = t.mock.fn((_error?: unknown): void => { /* Captured via the mock. */ });
    const { req, res, send, status } = makeReqRes();

    Object.assign(req, { method: "GET", originalUrl: "/boom" });
    Object.assign(res, { headersSent: true });

    handleRequestError(error, req, res, next);

    assert.deepEqual(next.mock.calls.map((call) => call.arguments), [[error]], "the error went on to the default handler, which closes the connection");
    assert.equal(status.mock.callCount(), 0, "no status was set on a response already started");
    assert.equal(send.mock.callCount(), 0, "nothing was written to a response already started");
    assert.deepEqual(logged.mock.calls.map((call) => call.arguments), [LOGGED], "the error and its request were logged once");
  });
});
