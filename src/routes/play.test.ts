/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * play.test.ts: Unit tests for the ad-hoc URL streaming route in play.ts. The module exports setupPlayEndpoint which registers GET /play and delegates to
 * handlePlayStream in streaming/hls.ts. The full streaming path requires a real Chrome browser - that coverage lives in the e2e suite. Here we cover the
 * synchronous validation branches that run before browser setup: missing url query parameter and route registration.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { setupPlayEndpoint } from "./play.ts";

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupPlayEndpoint(app);

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

describe("setupPlayEndpoint - GET /play (validation paths)", () => {

  test("returns 400 when no url query parameter is provided", async () => {

    // Negative test: handlePlayStream short-circuits with 400 before any browser interaction when url is missing or empty after trim.
    const res = await fetch(urlFor("/play"));

    assert.equal(res.status, 400);

    const body = await res.text();

    assert.match(body, /url query parameter is required/);
  });

  test("returns 400 when the url parameter is whitespace-only (trim collapses to empty)", async () => {

    // Boundary: the handler trims the URL before checking. A whitespace-only value collapses to "" and triggers the same 400 path.
    const res = await fetch(urlFor("/play?url=%20%20%20"));

    assert.equal(res.status, 400);

    const body = await res.text();

    assert.match(body, /url query parameter is required/);
  });

  test("returns 400 when the url parameter is an empty string", async () => {

    const res = await fetch(urlFor("/play?url="));

    assert.equal(res.status, 400);
    await res.text();
  });
});
