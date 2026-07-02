/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * express.helpers.ts: Cross-cutting test helpers for route handlers. Excluded from the build emit by the *.helpers.ts pattern in tsconfig.build.json. Two
 * primary helpers live here:
 *
 * - makeExpressStub() builds a stub Express app double that records every route registration AND captures the registered handler functions, plus an invoke()
 *   driver that runs a captured handler against a synthetic req/res pair and returns the resulting status code and JSON body. Route tests use this to exercise
 *   validation paths and orchestration logic without instantiating Express, real middleware, or the underlying I/O.
 *
 * - makeReqRes(input) builds a synthetic Express req/res pair for tests that exercise handlers directly (without registering them through a stub). The req
 *   carries body/params/query/headers/ip/protocol/get/socket fields wired from the input; the res exposes mock.fn-based json/status/send/setHeader spies that
 *   record their arguments for assertion. Tests destructure whichever fields their scenario inspects.
 *
 * The makeExpressStub handler is taken as the LAST argument to app.<method>(path, ...middleware, handler) so the stub naturally tolerates middleware chains in
 * the call shape; middleware bodies are never executed because we only retain the final handler reference. The stub returns itself from each method so chaining
 * works.
 */
import type { Request, Response } from "express";
import { mock } from "node:test";

/**
 * The minimal handler shape the stub captures. Route tests cast req/res to whatever Express type they need at the assertion site.
 */
export type StubHandler = (req: unknown, res: unknown) => unknown;

/**
 * One captured route registration. method/path identify the route; handler is the function the route's last argument resolved to (skipping any middleware that
 * preceded it in the registration call).
 */
export interface RouteCapture {

  handler: StubHandler;
  method: string;
  path: string;
}

/**
 * The result of invoking a captured handler against a synthetic res. statusCode is whatever the handler set via res.status(...); body is whatever it sent via
 * res.json(...). When the handler doesn't call res.status, statusCode stays at the Express default of 200.
 */
export interface InvokeResult {

  body: unknown;
  statusCode: number;
}

/**
 * The stub Express app surface. `app` satisfies the methods used by route registrations (delete/get/patch/post/put). `calls` is a flat list of every
 * registration recorded in order, useful for shape-of-wiring assertions. `routes` is the same list with the handler reference included. `invoke(method, path,
 * req)` runs the captured handler for the matching route against a synthetic res that records statusCode and JSON body.
 */
export interface ExpressStub {

  app: unknown;
  calls: { method: string; path: string }[];
  invoke: (method: string, path: string, req: Record<string, unknown>) => Promise<InvokeResult>;
  routes: RouteCapture[];
}

/**
 * Builds a fresh Express stub. Tests construct one per test (or per describe via beforeEach) so route registrations don't leak across cases. The stub's invoke()
 * helper resolves the matching route by exact method+path match against the registered list - it does not parse Express path patterns, so tests must invoke with
 * the same literal path string the production code registered (e.g., "/config/profiles/:key", not the substituted form).
 * @returns A fresh ExpressStub instance with empty calls/routes lists.
 */
export function makeExpressStub(): ExpressStub {

  const calls: { method: string; path: string }[] = [];
  const routes: RouteCapture[] = [];

  function record(method: string): (path: string, ...rest: unknown[]) => unknown {

    return (path: string, ...rest: unknown[]): unknown => {

      calls.push({ method, path });

      const handler = rest.at(-1);

      if(typeof handler === "function") {

        routes.push({ handler: handler as StubHandler, method, path });
      }

      return app;
    };
  }

  const app: Record<string, (path: string, ...rest: unknown[]) => unknown> = {

    delete: record("delete"),
    get: record("get"),
    patch: record("patch"),
    post: record("post"),
    put: record("put")
  };

  async function invoke(method: string, path: string, req: Record<string, unknown>): Promise<InvokeResult> {

    const route = routes.find((r) => (r.method === method) && (r.path === path));

    if(!route) {

      throw new Error("makeExpressStub.invoke: no route registered for " + method + " " + path);
    }

    let statusCode = 200;
    let body: unknown;

    const res: Record<string, unknown> = {

      json: (data: unknown): unknown => {

        body = data;

        return res;
      },
      status: (code: number): unknown => {

        statusCode = code;

        return res;
      }
    };

    await route.handler(req, res);

    return { body, statusCode };
  }

  return { app, calls, invoke, routes };
}

/**
 * The shape of the input object accepted by makeReqRes. Every field is optional; tests supply only what their scenario depends on. Defaults: empty body, empty
 * params/query/headers maps, "127.0.0.1" ip and socket.remoteAddress, "http" protocol.
 */
export interface MakeReqResInput {

  body?: unknown;
  headers?: Record<string, string>;
  ip?: string;
  params?: Record<string, string>;
  protocol?: string;
  query?: Record<string, string>;
}

/**
 * The shape returned by makeReqRes. The req and res are typed as Express's Request and Response - tests cast to those interfaces at the boundary so route
 * handler signatures bind without complaint. The mock.fn spies expose their .mock.calls history so tests can inspect captured arguments. The set covers the
 * Response surface that production handlers in this codebase actually call: status, json, send, sendStatus, end, flushHeaders, write, setHeader. The req-side
 * `on` spy records every (event, listener) registration so handlers that wire `req.on("close", fn)` for cleanup can be driven from the test by calling
 * `triggerReqEvent("close")`.
 */
export interface MakeReqResResult {

  end: ReturnType<typeof mock.fn>;
  flushHeaders: ReturnType<typeof mock.fn>;
  json: ReturnType<typeof mock.fn>;
  on: ReturnType<typeof mock.fn>;
  req: Request;
  res: Response;
  send: ReturnType<typeof mock.fn>;
  sendStatus: ReturnType<typeof mock.fn>;
  setHeader: ReturnType<typeof mock.fn>;
  status: ReturnType<typeof mock.fn>;
  triggerReqEvent: (event: string, ...args: unknown[]) => number;
  write: ReturnType<typeof mock.fn>;
}

/**
 * Builds a synthetic Express req/res pair for direct-handler tests. The req carries body/params/query/headers/ip/protocol/get/socket wired from the input; the
 * res exposes mock.fn-based spies for the Response surface enumerated in MakeReqResResult. status() returns the same res so res.status(400).json({...}) chains
 * correctly. The req's get(name) lookup is case-insensitive (matching Express semantics for header retrieval).
 *
 * This is the single source of truth for "fake an Express req/res in a unit test." Tests that only need the request part (e.g., tests of route-detection logic
 * that read req.protocol or req.headers) destructure { req }; tests that need handler-response inspection destructure { req, res, json, status, send,
 * setHeader }.
 *
 * @param input - Optional fields to override the defaults.
 * @returns The req/res pair plus the full mock.fn spy set and triggerReqEvent for response assertions; see MakeReqResResult for the complete list.
 */
export function makeReqRes(input: MakeReqResInput = {}): MakeReqResResult {

  const headers = input.headers ?? {};
  const ip = input.ip ?? "127.0.0.1";
  const end = mock.fn((): undefined => undefined);
  const flushHeaders = mock.fn((): undefined => undefined);
  const json = mock.fn((): undefined => undefined);
  const send = mock.fn((): unknown => undefined);
  const sendStatus = mock.fn((): unknown => undefined);
  const setHeader = mock.fn((): unknown => undefined);
  const write = mock.fn((): boolean => true);

  // status() returns res so handlers can chain res.status(N).json({...}); the remaining spies return undefined, including sendStatus, which callers invoke as a
  // terminal res.sendStatus(N) that needs no return value. We declare status after res so its body can close over res by name, and the direct "as Response" cast on
  // res is safe because the spy call shapes match Response's method signatures.
  const res = { headersSent: false } as Response;
  const status = mock.fn((): Response => res);

  Object.assign(res, { end, flushHeaders, json, send, sendStatus, setHeader, status, write });

  // The on() spy captures every (event, listener) pair so tests can drive synthetic events like req.on("close", ...) - production handlers register a close
  // listener for cancellation cleanup, and the test boundary needs a way to invoke it. We keep the registration table inside the closure so it lives alongside
  // the spy itself, and expose triggerReqEvent() on the result to invoke every captured listener for a given event name.
  const reqListeners = new Map<string, ((...args: unknown[]) => void)[]>();
  const on = mock.fn((event: string, listener: (...args: unknown[]) => void): unknown => {

    const existing = reqListeners.get(event) ?? [];

    existing.push(listener);
    reqListeners.set(event, existing);

    return req;
  });

  const req = {

    body: input.body ?? {},
    get: (name: string): string | undefined => headers[name.toLowerCase()],
    headers,
    ip,
    on,
    params: input.params ?? {},
    protocol: input.protocol ?? "http",
    query: input.query ?? {},
    socket: { remoteAddress: ip }
  } as unknown as Request;

  function triggerReqEvent(event: string, ...args: unknown[]): number {

    const listeners = reqListeners.get(event) ?? [];

    for(const listener of listeners) {

      listener(...args);
    }

    return listeners.length;
  }

  return { end, flushHeaders, json, on, req, res, send, sendStatus, setHeader, status, triggerReqEvent, write };
}
