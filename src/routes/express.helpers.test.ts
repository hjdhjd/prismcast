/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * express.helpers.test.ts: Tests for the shared Express stub helper. The helper is consumed by route tests across the routes/ tree, so a bug here cascades into
 * misleading results in every dependent suite. The tests below pin every claim the helper makes: route registrations are captured in order, the handler is taken
 * as the last argument so middleware chains work, the stub returns itself for chaining, invoke() resolves a route by exact method+path, status() is chainable
 * with json(), and the default status code is 200 when the handler doesn't call res.status.
 */
import { describe, test } from "node:test";
import { makeExpressStub, makeReqRes } from "./express.helpers.ts";
import assert from "node:assert/strict";
import { firstOf } from "../testing.helpers.ts";

describe("makeExpressStub - registration capture", () => {

  test("records each registration in calls in the order they fire", () => {

    const { app, calls } = makeExpressStub();

    (app as { get: (p: string, h: () => void) => unknown }).get("/first", () => undefined);
    (app as { post: (p: string, h: () => void) => unknown }).post("/second", () => undefined);
    (app as { delete: (p: string, h: () => void) => unknown }).delete("/third", () => undefined);

    assert.deepEqual(calls, [
      { method: "get", path: "/first" },
      { method: "post", path: "/second" },
      { method: "delete", path: "/third" }
    ]);
  });

  test("records the handler reference in routes alongside the method/path", () => {

    const { app, routes } = makeExpressStub();

    function specificHandler(): void { /* no-op */ }

    (app as { post: (p: string, h: typeof specificHandler) => unknown }).post("/foo", specificHandler);

    assert.equal(routes.length, 1);

    const route = firstOf(routes, "route registration");

    assert.equal(route.handler, specificHandler, "the registered function reference is preserved");
    assert.equal(route.method, "post");
    assert.equal(route.path, "/foo");
  });

  test("returns the stub itself from each method so chaining works", () => {

    const { app } = makeExpressStub();

    const typed = app as { get: (p: string, h: () => void) => typeof typed; post: (p: string, h: () => void) => typeof typed };

    const result = typed.get("/a", () => undefined).post("/b", () => undefined);

    assert.equal(result, typed, "chained call returns the same stub");
  });

  test("treats the LAST argument as the handler so middleware chains work", () => {

    // The Express signature is app.<method>(path, ...middleware, handler) - the helper preserves only the final handler reference.
    const { app, routes } = makeExpressStub();

    function middleware(): void { /* no-op */ }

    function handler(): void { /* no-op */ }

    (app as { post: (p: string, m: typeof middleware, h: typeof handler) => unknown }).post("/with-middleware", middleware, handler);

    assert.equal(routes[0]?.handler, handler, "the LAST function argument is captured, not the middleware");
  });

  test("does not record a route entry when no handler function is supplied", () => {

    // Defensive: a malformed registration call (path only, or path with non-function trailing args) should still increment calls but not add to routes.
    const { app, calls, routes } = makeExpressStub();

    (app as { get: (p: string) => unknown }).get("/no-handler");

    assert.equal(calls.length, 1, "the registration was still recorded in calls");
    assert.equal(routes.length, 0, "but no handler was captured in routes");
  });

  test("captures all five HTTP methods (delete, get, patch, post, put)", () => {

    const { app, calls } = makeExpressStub();
    const methods: ("delete" | "get" | "patch" | "post" | "put")[] = [ "delete", "get", "patch", "post", "put" ];

    for(const method of methods) {

      (app as Record<string, (p: string, h: () => void) => unknown>)[method]?.("/" + method, () => undefined);
    }

    assert.deepEqual(calls.map((c) => c.method), methods);
  });
});

describe("makeExpressStub - invoke", () => {

  test("runs the captured handler and captures res.json body with default 200 status", async () => {

    const { app, invoke } = makeExpressStub();

    (app as { get: (p: string, h: (req: unknown, res: { json: (b: unknown) => unknown }) => void) => unknown })
      .get("/health", (_req, res) => { res.json({ ok: true }); });

    const result = await invoke("get", "/health", {});

    assert.equal(result.statusCode, 200, "defaults to 200 when handler does not call res.status");
    assert.deepEqual(result.body, { ok: true });
  });

  test("captures both res.status and res.json when the handler chains them", async () => {

    const { app, invoke } = makeExpressStub();

    (app as { post: (p: string, h: (req: unknown, res: { json: (b: unknown) => unknown; status: (c: number) => { json: (b: unknown) => unknown } }) => void) => unknown })
      .post("/error", (_req, res) => { res.status(400).json({ error: "bad" }); });

    const result = await invoke("post", "/error", {});

    assert.equal(result.statusCode, 400);
    assert.deepEqual(result.body, { error: "bad" });
  });

  test("awaits async handlers before returning", async () => {

    // The handler resolves on a microtask delay; invoke must wait for the promise to settle before reading the captured body.
    const { app, invoke } = makeExpressStub();

    (app as { post: (p: string, h: (req: unknown, res: { json: (b: unknown) => unknown }) => Promise<void>) => unknown })
      .post("/async", async (_req, res) => {

        await Promise.resolve();
        res.json({ async: true });
      });

    const result = await invoke("post", "/async", {});

    assert.deepEqual(result.body, { async: true });
  });

  test("passes the supplied req object through to the handler", async () => {

    // The req shape is opaque to the stub - it just forwards whatever the test passes. This test verifies the forwarding contract by reading a custom field.
    const { app, invoke } = makeExpressStub();

    let seenBody: unknown;

    (app as { post: (p: string, h: (req: { body: unknown }, res: { json: (b: unknown) => unknown }) => void) => unknown })
      .post("/echo", (req, res) => {

        seenBody = req.body;
        res.json({ echoed: true });
      });

    await invoke("post", "/echo", { body: { custom: "value" } });

    assert.deepEqual(seenBody, { custom: "value" });
  });

  test("throws a descriptive error when no route matches the method+path", async () => {

    const { invoke } = makeExpressStub();

    await assert.rejects(
      () => invoke("get", "/never-registered", {}),
      /makeExpressStub\.invoke: no route registered for get \/never-registered/
    );
  });

  test("matches by exact method+path pair (does not parse Express path patterns)", async () => {

    // Tests that need to invoke a route registered with a path parameter must pass the literal path string the production code used. The stub does not match
    // /config/profiles/abc against the registered /config/profiles/:key.
    const { app, invoke } = makeExpressStub();

    (app as { delete: (p: string, h: (req: unknown, res: { json: (b: unknown) => unknown }) => void) => unknown })
      .delete("/config/profiles/:key", (_req, res) => { res.json({ matched: true }); });

    // The literal :key form matches.
    const matched = await invoke("delete", "/config/profiles/:key", {});

    assert.deepEqual(matched.body, { matched: true });

    // A substituted concrete value does not match.
    await assert.rejects(() => invoke("delete", "/config/profiles/abc", {}), /no route registered/);
  });
});

describe("makeExpressStub - isolation", () => {

  test("returns a fresh stub on each call (no shared state across instances)", () => {

    const a = makeExpressStub();
    const b = makeExpressStub();

    (a.app as { get: (p: string, h: () => void) => unknown }).get("/only-on-a", () => undefined);

    assert.equal(a.calls.length, 1);
    assert.equal(b.calls.length, 0, "stub b is not affected by registrations on stub a");
  });
});

describe("makeReqRes", () => {

  test("returns req with default values when no input is supplied", () => {

    const { req } = makeReqRes();

    // Default body is an empty object, default ip is 127.0.0.1, default protocol is "http", default params/query/headers are empty maps. Tests that don't
    // override these inherit the documented neutral defaults.
    assert.deepEqual(req.body, {});
    assert.equal(req.ip, "127.0.0.1");
    assert.equal(req.protocol, "http");
    assert.deepEqual(req.params, {});
    assert.deepEqual(req.query, {});
  });

  test("threads body, params, query, headers, ip, and protocol from input", () => {

    const { req } = makeReqRes({

      body: { name: "ABC" },
      headers: { "x-forwarded-for": "10.0.0.1" },
      ip: "192.168.1.50",
      params: { id: "42" },
      protocol: "https",
      query: { filter: "live" }
    });

    assert.deepEqual(req.body, { name: "ABC" });
    assert.deepEqual(req.params, { id: "42" });
    assert.deepEqual(req.query, { filter: "live" });
    assert.equal(req.ip, "192.168.1.50");
    assert.equal(req.protocol, "https");
  });

  test("req.get(name) is case-insensitive (matches Express semantics)", () => {

    // Express's req.get() lookup is case-insensitive: req.get("X-Forwarded-Host") and req.get("x-forwarded-host") return the same value. Our fake mirrors that
    // contract so handlers tested against it behave identically when they do canonical-case lookups.
    const { req } = makeReqRes({ headers: { "x-forwarded-host": "proxy.example.com" } });

    assert.equal(req.get("X-Forwarded-Host"), "proxy.example.com");
    assert.equal(req.get("x-forwarded-host"), "proxy.example.com");
    assert.equal(req.get("X-FORWARDED-HOST"), "proxy.example.com");
  });

  test("req.get(name) returns undefined for absent headers", () => {

    const { req } = makeReqRes();

    assert.equal(req.get("not-present"), undefined);
  });

  test("req.socket.remoteAddress mirrors req.ip", () => {

    // Some handlers read req.socket.remoteAddress directly (rather than req.ip). The fake keeps the two in sync so handlers that check either field see a
    // consistent address.
    const { req } = makeReqRes({ ip: "10.0.0.99" });

    assert.equal((req.socket as { remoteAddress: string }).remoteAddress, "10.0.0.99");
  });

  test("res.status returns res so handlers can chain res.status(N).json(...)", () => {

    // The chainable contract: res.status(400).json({error: "..."}) is the canonical Express error pattern. The fake's status spy must return res so the chain
    // resolves correctly when the handler executes.
    const { res, status, json } = makeReqRes();

    res.status(400).json({ error: "bad" });

    assert.equal(status.mock.callCount(), 1);
    assert.deepEqual(status.mock.calls[0]?.arguments, [400]);
    assert.equal(json.mock.callCount(), 1);
    assert.deepEqual(json.mock.calls[0]?.arguments, [{ error: "bad" }]);
  });

  test("res.send and res.setHeader spies record their captured arguments", () => {

    const { res, send, setHeader } = makeReqRes();

    res.setHeader("Content-Type", "application/json");
    res.send("body content");

    assert.equal(setHeader.mock.callCount(), 1);
    assert.deepEqual(setHeader.mock.calls[0]?.arguments, [ "Content-Type", "application/json" ]);
    assert.equal(send.mock.callCount(), 1);
    assert.deepEqual(send.mock.calls[0]?.arguments, ["body content"]);
  });

  test("each makeReqRes call returns fresh spy instances (no shared state across pairs)", () => {

    // Two independent pairs must not share spy instances - otherwise a captured call on one pair would leak into the other's mock.calls history.
    const a = makeReqRes();
    const b = makeReqRes();

    a.res.status(200).json({ ok: true });

    assert.equal(a.status.mock.callCount(), 1);
    assert.equal(b.status.mock.callCount(), 0, "second pair's status spy is independent");
  });

  test("req carries an .on listener registration that is a no-op (so tests don't crash on req.on)", () => {

    // Some handlers register req.on("close") for cancellation cleanup. The fake's req.on is a no-op so handler code that calls it doesn't crash; tests that
    // need to drive close events register their own listeners on a real Server instead.
    const { req } = makeReqRes();

    assert.doesNotThrow(() => {

      (req as unknown as { on: (event: string, fn: () => void) => void }).on("close", () => undefined);
    });
  });
});
