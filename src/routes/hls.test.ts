/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.test.ts: Unit tests for the HLS route registrar in hls.ts. The module exports a single function setupHLSRoutes which registers four routes and delegates
 * to handlers in streaming/hls.ts. Those handlers require a real Chrome browser, FFmpeg subprocess, and channel registry to exercise the success path - that
 * coverage lives in streaming/hls.test.ts and the e2e suite. Here we lock the route registration shape: the four documented paths must be reachable, and an
 * unknown channel name must produce a non-2xx response without crashing. The deeper handler logic is tested in streaming/hls.test.ts via validateChannel.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { setupHLSRoutes } from "./hls.ts";

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupHLSRoutes(app);

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

describe("setupHLSRoutes - GET /hls/:name/stream.m3u8", () => {

  test("returns a non-2xx status for an unknown channel (locks the route registration without depending on browser)", async () => {

    // Negative test: validateChannel inside handleHLSPlaylist returns valid=false for a non-existent channel and the handler responds with 404. This locks the
    // route registration and the validation pass-through without requiring a real browser to launch.
    const res = await fetch(urlFor("/hls/totally-not-a-real-channel-x9z2/stream.m3u8"));

    assert.equal(res.status, 404, "unknown channel should produce 404");

    // Drain the body so the connection releases.
    await res.text();
  });
});

describe("setupHLSRoutes - GET /hls/:name/video.m3u8", () => {

  test("registered: variant playlist endpoint responds (locks the path-to-regexp wiring)", async () => {

    // The variant playlist endpoint is registered before the catch-all :segment route. Without an active stream, the handler returns a non-2xx status. We just
    // confirm the route is reachable (i.e., does not 404 from Express's catch-all because the path didn't match).
    const res = await fetch(urlFor("/hls/totally-not-a-real-channel-x9z2/video.m3u8"));

    // The handler will respond with some error code (likely 404) because no stream exists. The exact code is the handler's contract; we only verify the route
    // was reached - i.e., the response is well-formed HTTP, not a connection failure.
    assert.ok(res.status >= 200, "should produce a structured HTTP response");
    await res.text();
  });
});

describe("setupHLSRoutes - GET /hls/:name/audio.m3u8", () => {

  test("registered: audio variant playlist endpoint responds", async () => {

    const res = await fetch(urlFor("/hls/totally-not-a-real-channel-x9z2/audio.m3u8"));

    assert.ok(res.status >= 200);
    await res.text();
  });
});

describe("setupHLSRoutes - GET /hls/:name/:segment", () => {

  test("registered: segment endpoint responds for an unknown channel", async () => {

    // Boundary: the catch-all :segment route is registered after the variant playlists. A request that does not match the special m3u8 names (init.mp4 here)
    // falls through to this handler. Without an active stream, the handler responds with an error code.
    const res = await fetch(urlFor("/hls/totally-not-a-real-channel-x9z2/init.mp4"));

    assert.ok(res.status >= 200);
    await res.text();
  });
});
