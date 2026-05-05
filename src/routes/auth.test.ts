/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * auth.test.ts: Unit tests for the channel-login routes in auth.ts. setupAuthEndpoint registers POST /auth/login, POST /auth/done, and GET /auth/status. The
 * login flow requires a real Chrome browser to actually authenticate; without one, startLoginMode() rejects with "Browser is not connected." We cover the
 * paths that can be exercised honestly in a unit test: the request validation (missing channel/url), the unknown-channel 404, the login-when-disconnected 409,
 * the noop endLoginMode (idempotent against an inactive state), and the always-on getLoginStatus.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { setupAuthEndpoint } from "./auth.ts";

interface AuthResponse {

  active?: boolean;
  error?: string;
  message?: string;
  startTime?: number | null;
  success?: boolean;
  url?: string | null;
}

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  // Express 5 ships with builtin JSON parser support via app.use; the auth handler reads req.body so we wire it here.
  app.use(express.json());
  setupAuthEndpoint(app);

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

function urlFor(path: string): string {

  return "http://127.0.0.1:" + String(sharedPort) + path;
}

before(async () => {

  const created = await makeServer();

  sharedServer = created.server;
  sharedPort = created.port;
});

after(async () => {

  await closeServer(sharedServer);
  await closePuppeteerStreamWss();
});

describe("setupAuthEndpoint - POST /auth/login (validation paths)", () => {

  test("returns 400 when neither channel nor url is provided", async () => {

    // Negative test: the handler expects either body.channel or body.url. An empty body must produce a 400 with a descriptive error.
    const res = await fetch(urlFor("/auth/login"), {

      body: JSON.stringify({}),
      headers: { "content-type": "application/json" },
      method: "POST"
    });
    const body = await res.json() as AuthResponse;

    assert.equal(res.status, 400);
    assert.equal(body.success, false);
    assert.match(body.error ?? "", /Either channel or url must be provided/);
  });

  test("returns 404 when an unknown channel key is provided", async () => {

    // Negative test: an unknown channel slug short-circuits before any browser interaction. The 404 status confirms the handler routed through the channel
    // lookup branch rather than the URL branch.
    const res = await fetch(urlFor("/auth/login"), {

      body: JSON.stringify({ channel: "totally-not-a-real-channel-x9z2" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });
    const body = await res.json() as AuthResponse;

    assert.equal(res.status, 404);
    assert.equal(body.success, false);
    assert.match(body.error ?? "", /Channel not found/);
  });

  test("returns 409 when a URL is provided but no browser is connected (locks the disconnected branch)", async () => {

    // The test environment never launches Chrome, so startLoginMode() returns success=false with "Browser is not connected." The handler maps that to HTTP 409
    // (Conflict). This is the unhealthy-state branch most production deployments would hit if they tried to login while Chrome was being restarted.
    const res = await fetch(urlFor("/auth/login"), {

      body: JSON.stringify({ url: "https://example.test/login" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });
    const body = await res.json() as AuthResponse;

    assert.equal(res.status, 409);
    assert.equal(body.success, false);
    assert.match(body.error ?? "", /Browser is not connected/);
  });
});

describe("setupAuthEndpoint - POST /auth/done", () => {

  test("returns 200 success even when login mode was not active (idempotent)", async () => {

    // Boundary: endLoginMode is documented as a no-op when login mode isn't running. The endpoint always reports success - no error path - so callers can use
    // it as a defensive cleanup without checking state first.
    const res = await fetch(urlFor("/auth/done"), { method: "POST" });
    const body = await res.json() as AuthResponse;

    assert.equal(res.status, 200);
    assert.equal(body.success, true);
    assert.match(body.message ?? "", /Login mode ended/);
  });

  test("repeated calls are still successful (idempotent across invocations)", async () => {

    // Boundary: this confirms the no-op contract holds across two consecutive calls.
    const a = await fetch(urlFor("/auth/done"), { method: "POST" });
    const aBody = await a.json() as AuthResponse;
    const b = await fetch(urlFor("/auth/done"), { method: "POST" });
    const bBody = await b.json() as AuthResponse;

    assert.equal(aBody.success, true);
    assert.equal(bBody.success, true);
  });
});

describe("setupAuthEndpoint - GET /auth/status", () => {

  test("returns the documented LoginStatus shape (active, startTime, url)", async () => {

    const res = await fetch(urlFor("/auth/status"));
    const body = await res.json() as AuthResponse;

    assert.equal(res.status, 200);
    assert.equal(typeof body.active, "boolean", "active should be a boolean");

    // startTime and url are nullable when not active.
    assert.ok((body.startTime === null) || (typeof body.startTime === "number"), "startTime should be null or number");
    assert.ok((body.url === null) || (typeof body.url === "string"), "url should be null or string");
  });

  test("active=false in a fresh test environment (no login has been started)", async () => {

    // Because POST /auth/login could not have succeeded (no browser), login mode must remain inactive throughout the suite.
    const res = await fetch(urlFor("/auth/status"));
    const body = await res.json() as AuthResponse;

    assert.equal(body.active, false);
    assert.equal(body.startTime, null, "startTime should be null when inactive");
    assert.equal(body.url, null, "url should be null when inactive");
  });

  test("emits Content-Type application/json", async () => {

    const res = await fetch(urlFor("/auth/status"));

    assert.match(res.headers.get("content-type") ?? "", /application\/json/);
    await res.json();
  });
});
