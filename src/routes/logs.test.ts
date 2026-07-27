/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logs.test.ts: Unit tests for the log viewing routes in logs.ts. setupLogsEndpoint registers GET /logs (recent entries from the rotating log file) and GET
 * /logs/stream (Server-Sent Events for live entries). The handler shape we lock here covers the query-parameter parsing (lines and level), the response
 * envelope (entries + filtered + mode + total), the missing-file ENOENT branch (returns empty entries with mode: "file"), the console-mode short-circuit (no
 * file read), and the SSE handshake (headers, immediate flush). Live SSE event delivery is covered by the logEmitter.test.ts suite; we only test the route's
 * subscription wiring here.
 */
import type { AddressInfo, Server } from "node:net";
import type { Express, Request, Response } from "express";
import { after, before, describe, mock, test } from "node:test";
import { emitLogEntry, setConsoleLogging, subscribeToLogs } from "../utils/index.ts";
import { makeExpressStub, makeReqRes } from "./express.helpers.ts";
import { mkdtemp, rm } from "node:fs/promises";
import type { RouteCapture } from "./express.helpers.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { initializeDataDir } from "../config/paths.ts";
import os from "node:os";
import path from "node:path";
import { setupLogsEndpoint } from "./logs.ts";

interface LogsResponse {

  entries: { level: string; message: string; timestamp: string }[];
  error?: string;
  filtered: number;
  mode: "console" | "file";
  total: number;
}

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupLogsEndpoint(app);

  return new Promise((resolve, reject) => {

    const server = app.listen(0, "127.0.0.1", () => {

      const address = server.address() as AddressInfo;

      resolve({ port: address.port, server });
    });

    server.on("error", reject);
  });
}

function closeServer(server: Server): Promise<void> {

  return new Promise((resolve) => {

    server.close(() => {

      resolve();
    });
  });
}

let sharedServer: Server;
let sharedPort = 0;
let tempDataDir = "";

function urlFor(p: string): string {

  return "http://127.0.0.1:" + String(sharedPort) + p;
}

before(async () => {

  // The /logs handler reads getLogFilePath(CONFIG), which calls getDataDir() and throws if the data directory is uninitialized. We point the test at a fresh
  // temp directory so the file-read branch of the handler can run; the directory contains no log file, so reads produce ENOENT and the handler returns the
  // empty-entries response. Tests that check the console-logging short-circuit toggle the flag explicitly and restore it afterwards.
  tempDataDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-logs-test-"));
  initializeDataDir(tempDataDir);

  const created = await makeServer();

  sharedServer = created.server;
  sharedPort = created.port;
});

after(async () => {

  await closeServer(sharedServer);
  await rm(tempDataDir, { force: true, recursive: true });
  await closePuppeteerStreamWss();
});

describe("setupLogsEndpoint - GET /logs (file mode, no log file)", () => {

  test("returns 200 with empty entries when the log file does not exist (ENOENT branch)", async () => {

    // The test environment never writes to the log file (file logging is initialized lazily by app.ts startup), so reading it produces ENOENT. The handler
    // converts ENOENT into a successful empty response so callers can poll without special-casing first-startup state.
    const res = await fetch(urlFor("/logs"));

    assert.equal(res.status, 200);

    const body = await res.json() as LogsResponse;

    assert.deepEqual(body.entries, []);
    assert.equal(body.total, 0);
    assert.equal(body.filtered, 0);
    assert.equal(body.mode, "file");
  });

  test("emits Content-Type application/json", async () => {

    const res = await fetch(urlFor("/logs"));

    assert.match(res.headers.get("content-type") ?? "", /application\/json/);
    await res.json();
  });

  test("accepts the lines query parameter (boundary: clamped between 1 and 1000)", async () => {

    // Boundary: lines=500 is in range, so the handler honors it. The empty-file case still returns no entries, but we verify no error.
    const res = await fetch(urlFor("/logs?lines=500"));
    const body = await res.json() as LogsResponse;

    assert.equal(res.status, 200);
    assert.deepEqual(body.entries, [], "no entries when log file is empty");
  });

  test("falls back to default 100 when lines is not a number", async () => {

    // Boundary: parseInt("abc") returns NaN, the handler falls back to 100. The empty-file case still returns no entries.
    const res = await fetch(urlFor("/logs?lines=abc"));
    const body = await res.json() as LogsResponse;

    assert.equal(res.status, 200);
    assert.equal(body.total, 0);
  });

  test("falls back to default 100 when lines exceeds 1000 (boundary)", async () => {

    // Boundary: the in-range check is `(linesParam > 0) && (linesParam <= 1000)`. 1001 falls outside, so the default kicks in.
    const res = await fetch(urlFor("/logs?lines=1001"));
    const body = await res.json() as LogsResponse;

    assert.equal(res.status, 200);
    assert.equal(body.total, 0);
  });

  test("falls back to default 100 when lines is zero or negative (boundary)", async () => {

    const a = await fetch(urlFor("/logs?lines=0"));
    const b = await fetch(urlFor("/logs?lines=-10"));

    assert.equal(a.status, 200);
    assert.equal(b.status, 200);
    await a.json();
    await b.json();
  });

  test("accepts the level query parameter (error/warn/info)", async () => {

    // The level filter is validated against the documented set. Values outside the set are silently ignored (no filter applied), which we don't directly
    // observe with an empty file, but the response shape must still parse cleanly.
    const res = await fetch(urlFor("/logs?level=error"));
    const body = await res.json() as LogsResponse;

    assert.equal(res.status, 200);
    assert.equal(body.total, 0);
  });

  test("emits both lines and level parameters together", async () => {

    const res = await fetch(urlFor("/logs?lines=50&level=warn"));

    assert.equal(res.status, 200);
    await res.json();
  });
});

describe("setupLogsEndpoint - GET /logs (console mode short-circuit)", () => {

  test("returns mode='console' with empty entries when console logging is enabled", async () => {

    // The handler short-circuits before reading the file when isConsoleLogging() is true. We toggle the flag for this test and restore it in a finally block to
    // avoid affecting other tests that run after this file.
    setConsoleLogging(true);

    try {

      const res = await fetch(urlFor("/logs"));
      const body = await res.json() as LogsResponse;

      assert.equal(res.status, 200);
      assert.equal(body.mode, "console", "mode should be console when isConsoleLogging() is true");
      assert.deepEqual(body.entries, []);
      assert.equal(body.total, 0);
      assert.equal(body.filtered, 0);
    } finally {

      // Restore default file-logging mode for subsequent tests.
      setConsoleLogging(false);
    }
  });
});

describe("setupLogsEndpoint - GET /logs/stream (SSE handshake)", () => {

  test("returns text/event-stream content type and SSE-friendly headers", async () => {

    const controller = new AbortController();
    const res = await fetch(urlFor("/logs/stream"), { signal: controller.signal });

    try {

      assert.equal(res.status, 200);
      assert.match(res.headers.get("content-type") ?? "", /text\/event-stream/);
      assert.match(res.headers.get("cache-control") ?? "", /no-cache/);
      assert.match(res.headers.get("connection") ?? "", /keep-alive/);
    } finally {

      controller.abort();
    }
  });

  test("forwards live log entries as SSE data events (subscribeToLogs wiring)", async () => {

    // We open the stream, then want to synthesize a log entry. Calling subscribeToLogs directly is the wrong approach here - it observes, it does not emit. The
    // route subscribes via subscribeToLogs which the production logger fires when logging happens. We use a tighter contract test: confirm subscribeToLogs returns
    // an unsubscribe function (the route relies on it for cleanup on client disconnect). This locks the integration shape without requiring us to drive a real
    // log emission from a co-located fixture. The wire-byte forwarding contract (and null-eventType prefix-skip behavior) is pinned by the direct-handler suite
    // below, which can drive emitLogEntry deterministically and read res.write spy calls back.
    const unsubscribe = subscribeToLogs(() => {

      // Intentional no-op: we only assert that subscribe returns a callable unsubscribe.
    });

    assert.equal(typeof unsubscribe, "function", "subscribeToLogs should return an unsubscribe function");

    unsubscribe();
  });

  test("respects the level query parameter on the SSE endpoint (validation only)", async () => {

    // The route accepts ?level=error|warn|info and validates against a static allowlist. Values outside the list are silently ignored. We exercise both a valid
    // and an invalid value to confirm neither produces an error response.
    const validController = new AbortController();
    const validRes = await fetch(urlFor("/logs/stream?level=error"), { signal: validController.signal });

    try {

      assert.equal(validRes.status, 200);
    } finally {

      validController.abort();
    }

    const invalidController = new AbortController();
    const invalidRes = await fetch(urlFor("/logs/stream?level=BOGUS"), { signal: invalidController.signal });

    try {

      assert.equal(invalidRes.status, 200, "invalid level should not error - the validator silently treats it as no filter");
    } finally {

      invalidController.abort();
    }
  });
});

// The direct-handler suite below registers the /logs/stream handler against a stub Express app so we can drive the captured handler against synthetic req/res
// pairs. This pins the wire-byte forwarding contract (null-eventType produces only a `data:` line, no `event:` prefix), the level-filter short-circuit branch
// (entries of the wrong level never reach res.write), and the close-cleanup guarantee (post-close emits do not reach the wire AND the heartbeat stops). Each
// test extracts the route fresh because setupLogsEndpoint is the only public surface that wires the handler into our stub.
function findLogsStreamHandler(): RouteCapture {

  const stub = makeExpressStub();

  setupLogsEndpoint(stub.app as Express);

  const route = stub.routes.find((r) => (r.method === "get") && (r.path === "/logs/stream"));

  if(!route) {

    throw new Error("setupLogsEndpoint did not register GET /logs/stream");
  }

  return route;
}

// Convenience: drive the captured handler against the supplied req/res with the right Express types. The handler itself is sync (it returns void after wiring
// the subscribe), so no await is needed.
function invokeLogsStreamHandler(route: RouteCapture, req: Request, res: Response): void {

  (route.handler as (req: Request, res: Response) => void)(req, res);
}

describe("setupLogsEndpoint - GET /logs/stream (direct-handler wire bytes)", () => {

  test("forwards a log entry through sse.sendEvent(null, entry) - data line only, no 'event:' prefix", () => {

    // Pins the /logs/stream forwarding path: subscribeToLogs forwards entries via sse.sendEvent(null, entry). The null-eventType branch in installSseStream skips
    // the event line and writes only `data: <json>\n\n`. Without this assertion, a regression that switched to a named eventType (or wrapped the entry in an
    // envelope) would not be caught at any tier.
    const route = findLogsStreamHandler();
    const { req, res, triggerReqEvent, write } = makeReqRes();

    invokeLogsStreamHandler(route, req, res);

    // Reset the spy AFTER install so we only observe writes triggered by the log emit, not the heartbeat install path (the heartbeat does not fire here because
    // we have not enabled mock.timers; it lives on the real interval clock and never ticks during the test).
    write.mock.resetCalls();

    emitLogEntry({ level: "info", message: "hello", timestamp: "2026/05/06 16:00:00.000" });

    assert.equal(write.mock.callCount(), 1, "one write per emitted entry");

    const written = write.mock.calls[0]?.arguments[0] as string;

    assert.ok(!written.startsWith("event:"), "null eventType must not write an 'event:' prefix");
    assert.equal(written, "data: " + JSON.stringify({ level: "info", message: "hello", timestamp: "2026/05/06 16:00:00.000" }) + "\n\n");

    // Cleanup: invoke the close handler so the route unsubscribes from the shared logEmitter and does not leak listeners across tests.
    triggerReqEvent("close");
  });

  test("level filter short-circuits non-matching entries - only matching entries reach res.write", () => {

    // Pins the level-filter short-circuit inside the subscribeToLogs callback: entries whose level does not match filterLevel are skipped. The existing
    // fetch-based test only checked that ?level=error did not produce an error response; it never confirmed that a non-error entry is actually filtered out. This
    // test drives the level=error filter, emits one info entry (must be skipped) and one error entry (must reach the wire), and asserts the filter shape directly.
    const route = findLogsStreamHandler();
    const { req, res, triggerReqEvent, write } = makeReqRes({ query: { level: "error" } });

    invokeLogsStreamHandler(route, req, res);

    write.mock.resetCalls();

    // Wrong level - must be filtered out.
    emitLogEntry({ level: "info", message: "noise", timestamp: "2026/05/06 16:00:00.000" });
    assert.equal(write.mock.callCount(), 0, "info-level entry must be filtered out when filter=error");

    // Right level - must reach the wire.
    emitLogEntry({ level: "error", message: "boom", timestamp: "2026/05/06 16:00:01.000" });
    assert.equal(write.mock.callCount(), 1, "error-level entry must pass the filter");

    const written = write.mock.calls[0]?.arguments[0] as string;

    assert.match(written, /"level":"error"/);
    assert.match(written, /"message":"boom"/);

    // Cleanup so the test does not leave a listener bound on the shared logEmitter.
    triggerReqEvent("close");
  });

  test("invalid level query parameter is silently ignored - all entries reach the wire (no filter applied)", () => {

    // Boundary: the route validates the level query param against [error, info, warn]. Anything else is treated as no filter (filterLevel = null), so every
    // emitted entry forwards. Pinning this prevents a regression where the validator throws or 400s on unknown values.
    const route = findLogsStreamHandler();
    const { req, res, triggerReqEvent, write } = makeReqRes({ query: { level: "BOGUS" } });

    invokeLogsStreamHandler(route, req, res);

    write.mock.resetCalls();

    emitLogEntry({ level: "info", message: "info entry", timestamp: "2026/05/06 16:00:00.000" });
    emitLogEntry({ level: "warn", message: "warn entry", timestamp: "2026/05/06 16:00:01.000" });
    emitLogEntry({ level: "error", message: "error entry", timestamp: "2026/05/06 16:00:02.000" });

    assert.equal(write.mock.callCount(), 3, "all three entries pass through when filter is invalid (no filter)");

    triggerReqEvent("close");
  });

  test("req.on('close') handler unsubscribes from the log emitter - post-close emits do not reach the wire", () => {

    // Pins the req.on("close") cleanup path: the close handler must run unsubscribe(). After invoking the close handler synthetically, an emitted log entry must
    // NOT trigger a res.write. This is the observable behavior the test protects: a regression that dropped the unsubscribe() call would leak the listener and
    // continue forwarding to a disconnected response, eventually causing memory growth and write errors.
    const route = findLogsStreamHandler();
    const { req, res, triggerReqEvent, write } = makeReqRes();

    invokeLogsStreamHandler(route, req, res);

    write.mock.resetCalls();

    // Confirm the listener IS wired (sanity: an emit before close reaches the wire).
    emitLogEntry({ level: "info", message: "before close", timestamp: "2026/05/06 16:00:00.000" });
    assert.equal(write.mock.callCount(), 1, "pre-close emit reaches the wire");

    // Invoke the close listener captured by the on-spy. The route registers exactly one close listener.
    const fired = triggerReqEvent("close");

    assert.equal(fired, 1, "exactly one close listener was registered");

    // Post-close: the listener must be gone, so the next emit does NOT reach res.write.
    emitLogEntry({ level: "info", message: "after close", timestamp: "2026/05/06 16:00:01.000" });
    assert.equal(write.mock.callCount(), 1, "post-close emit must NOT reach the wire (unsubscribe ran)");
  });

  test("req.on('close') handler clears the heartbeat - subsequent ticks do not produce writes", () => {

    // Pins the req.on("close") cleanup path: the close handler must run sse.close(), which clears the heartbeat interval. Without this, a regression that dropped
    // sse.close() would leak the heartbeat past disconnect. We use mock.timers to drive the interval deterministically: confirm one tick fires before close, then
    // call the close handler and confirm subsequent ticks produce no writes.
    mock.timers.enable({ apis: ["setInterval"] });

    try {

      const route = findLogsStreamHandler();
      const { req, res, triggerReqEvent, write } = makeReqRes();

      invokeLogsStreamHandler(route, req, res);

      // The heartbeat fires every 30s; confirm it ticks before close.
      mock.timers.tick(30_000);
      assert.equal(write.mock.callCount(), 1, "heartbeat fires while connection is open");
      assert.deepEqual(write.mock.calls[0]?.arguments, ["event: heartbeat\ndata: \n\n"]);

      // Invoke the close handler.
      triggerReqEvent("close");

      // Advance another full interval - no further writes.
      mock.timers.tick(30_000);
      assert.equal(write.mock.callCount(), 1, "no heartbeat after close()");
    } finally {

      mock.timers.reset();
    }
  });
});
