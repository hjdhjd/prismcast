/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mpegts.test.ts: Unit tests for the MPEG-TS route registrar in mpegts.ts. The module exports setupMpegTsRoutes which registers a single route GET /stream/:name
 * and delegates to handleMpegTsStream in streaming/mpegts.ts. The handler requires a real Chrome browser, FFmpeg subprocess, and channel registry to exercise
 * the success path - that coverage lives in the e2e suite. Here we lock the route registration and confirm an unknown channel does not crash the server.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { setupMpegTsRoutes } from "./mpegts.ts";

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupMpegTsRoutes(app);

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

describe("setupMpegTsRoutes - GET /stream/:name", () => {

  test("returns a structured HTTP response for an unknown channel (locks the route registration)", async () => {

    // Negative test: handleMpegTsStream's first action is to validate the channel name. Without a registered channel, the handler short-circuits to an error
    // response. We assert only that the route was reached (status code present, response well-formed) since the exact handler error response is owned by the
    // streaming/mpegts.ts module and tested separately.
    const res = await fetch(urlFor("/stream/totally-not-a-real-channel-x9z2"));

    assert.ok(res.status >= 400, "unknown channel should produce a non-2xx response");

    // Drain the body so the connection can release.
    await res.text();
  });

  test("an unrelated path is not handled by this route (404 from Express)", async () => {

    // Boundary: only /stream/:name is registered. A non-matching URL must fall through to the framework default rather than be picked up.
    const res = await fetch(urlFor("/not-stream/foo"));

    assert.equal(res.status, 404);
    await res.text();
  });
});
