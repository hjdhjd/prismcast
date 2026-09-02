/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * handler.test.ts: Unit tests for the route() wrapper. The wrapper is the SSOT for try/catch in channel-config endpoints - every handler delegates exception
 * routing to it. Tests assert the core guarantees: synchronous handlers run and complete, asynchronous handlers are awaited, thrown errors flow to the envelope's
 * 500/parse-error path, successful handlers do not invoke the error envelope, each wrapped handler carries its own action label, next() is never invoked, and the
 * request and response objects forward through unchanged.
 */
import { describe, mock, test } from "node:test";
import assert from "node:assert/strict";
import { makeReqRes } from "../../../express.helpers.ts";
import { route } from "./handler.ts";

describe("route", () => {

  test("invokes a synchronous handler and resolves without touching the error envelope", async () => {

    const { req, res, status } = makeReqRes();
    let invoked = false;
    const handler = route("test action", () => {

      invoked = true;
    });

    await handler(req, res, () => undefined);

    assert.equal(invoked, true, "handler body must run");
    assert.equal(status.mock.callCount(), 0, "no error path means no status call");
  });

  test("awaits an asynchronous handler before resolving", async () => {

    const { req, res } = makeReqRes();
    let phase = "before";
    const handler = route("test action", async () => {

      // Yield to the microtask queue so we can verify the wrapper waits for the resolution. Without an await, phase would still read "before" when the wrapper
      // synchronously returned.
      await Promise.resolve();
      phase = "after";
    });

    await handler(req, res, () => undefined);

    assert.equal(phase, "after", "wrapper must await the handler's promise chain");
  });

  test("routes synchronous exceptions through sendErrorResponse (500 status)", async () => {

    const { req, res, status } = makeReqRes();
    const handler = route("save channel", () => {

      throw new Error("boom");
    });

    await handler(req, res, () => undefined);

    assert.equal(status.mock.callCount(), 1, "error path must set a status");
    assert.equal(status.mock.calls[0]?.arguments[0], 500, "non-parse Error produces a 500");
  });

  test("routes asynchronous rejections through sendErrorResponse", async () => {

    const { json, req, res, status } = makeReqRes();
    const handler = route("save channel", async () => {

      await Promise.resolve();
      throw new Error("async boom");
    });

    await handler(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 500);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], false);
    assert.match(body["error"] as string, /save channel/, "the action label is woven into the error message");
    assert.match(body["error"] as string, /async boom/, "the underlying error message is preserved");
  });

  test("preserves the action label across multiple wrapped handlers (closure capture is per-call)", async () => {

    // The route factory captures the action label in a closure. Each call must produce an independent handler whose error label matches its own action -
    // sharing a label across handlers would conflate failure attribution between unrelated endpoints.
    const handlerA = route("delete tag", () => {

      throw new Error("a");
    });
    const handlerB = route("rename tag", () => {

      throw new Error("b");
    });

    const { json: jsonA, req: reqA, res: resA } = makeReqRes();
    const { json: jsonB, req: reqB, res: resB } = makeReqRes();

    await handlerA(reqA, resA, () => undefined);
    await handlerB(reqB, resB, () => undefined);

    const bodyA = jsonA.mock.calls[0]?.arguments[0] as Record<string, unknown>;
    const bodyB = jsonB.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(bodyA["error"] as string, /delete tag/, "handler A must use its own label");
    assert.match(bodyB["error"] as string, /rename tag/, "handler B must use its own label");
  });

  test("does not invoke the next() Express callback (the wrapper handles errors itself)", async () => {

    // The wrapper's contract is that it absorbs exceptions and writes the error response. Express middleware would otherwise expect next(err) to escalate,
    // but the channel-config endpoints chose to handle their own errors uniformly. Lock the contract: next is never called.
    const { req, res } = makeReqRes();
    const next = mock.fn();
    const handler = route("save channel", () => {

      throw new Error("x");
    });

    await handler(req, res, next);

    assert.equal(next.mock.callCount(), 0, "next must not be invoked - the wrapper owns error routing");
  });

  test("forwards the request and response objects unchanged to the wrapped handler", async () => {

    // The wrapper is structurally a forwarder; it must not transform req/res before invoking the handler. Any mutation would break call sites that read req
    // params or set custom headers.
    const { req, res } = makeReqRes();
    let observedReq: unknown;
    let observedRes: unknown;
    const handler = route("test", (innerReq, innerRes) => {

      observedReq = innerReq;
      observedRes = innerRes;
    });

    await handler(req, res, () => undefined);

    assert.equal(observedReq, req, "req should pass through by reference");
    assert.equal(observedRes, res, "res should pass through by reference");
  });
});
