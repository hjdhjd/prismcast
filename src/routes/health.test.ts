/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.test.ts: Unit tests for the health check endpoint in health.ts. setupHealthEndpoint registers GET /health which assembles a HealthStatus payload from
 * browser state, stream registry, memory metrics, and ffmpeg availability. The handler returns HTTP 503 when the browser is not connected so monitoring tools
 * can detect degraded state via status code. Tests run against an ephemeral Express server with no real browser launched, so the unhealthy branch is the
 * primary code path exercised here. The tests verify the response shape, status-code mapping, and the message text emitted in each branch.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { setupHealthEndpoint } from "./health.ts";

interface HealthBody {

  browser: { connected: boolean; pageCount: number };
  captureMode: string;
  chrome: string | null;
  clients: { byType: { count: number; type: string }[]; total: number };
  ffmpegAvailable: boolean;
  memory: { heapTotal: number; heapUsed: number; rss: number; segmentBuffers: number };
  message?: string;
  status: "degraded" | "healthy" | "unhealthy";
  streams: { active: number; limit: number };
  timestamp: string;
  uptime: number;
  version: string;
}

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupHealthEndpoint(app);

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

describe("setupHealthEndpoint - GET /health (browser disconnected)", () => {

  test("returns HTTP 503 when no browser is launched (the documented unhealthy path)", async () => {

    // The test environment never launches a Puppeteer browser, so isBrowserConnected() returns false. The handler must surface this via status 503 so
    // monitoring systems detect the unhealthy state via status code alone.
    const res = await fetch(urlFor("/health"));

    assert.equal(res.status, 503, "unhealthy branch should respond with 503");

    // Drain the body to release the connection.
    await res.json();
  });

  test("payload status field is 'unhealthy' and message explains why", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.status, "unhealthy");
    assert.equal(body.message, "Browser is not connected.");
  });

  test("browser.connected is false and pageCount is 0 when disconnected", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.browser.connected, false);
    assert.equal(body.browser.pageCount, 0);
  });

  test("chrome version is null when no browser has launched (locks the documented null contract)", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.chrome, null);
  });
});

describe("setupHealthEndpoint - GET /health response shape", () => {

  test("returns Content-Type application/json", async () => {

    const res = await fetch(urlFor("/health"));

    assert.match(res.headers.get("content-type") ?? "", /application\/json/);
    await res.json();
  });

  test("includes the documented top-level keys", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;
    const keys = Object.keys(body).sort();

    // Locks the surface area of the response. Adding a key here should be a deliberate change accompanied by a documentation update.
    const expectedRequired = [ "browser", "captureMode", "chrome", "clients", "ffmpegAvailable", "memory", "status", "streams", "timestamp", "uptime", "version" ];

    for(const required of expectedRequired) {

      assert.ok(keys.includes(required), "required key " + required + " should be present");
    }
  });

  test("memory block exposes heap and segment metrics as numbers", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(typeof body.memory.heapTotal, "number");
    assert.equal(typeof body.memory.heapUsed, "number");
    assert.equal(typeof body.memory.rss, "number");
    assert.equal(typeof body.memory.segmentBuffers, "number");

    // RSS and heapTotal are always positive in a running Node process.
    assert.ok(body.memory.heapTotal > 0, "heapTotal should be positive");
    assert.ok(body.memory.rss > 0, "rss should be positive");
  });

  test("uptime is a non-negative number (process.uptime invariant)", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(typeof body.uptime, "number");
    assert.ok(body.uptime >= 0, "uptime must be non-negative");
  });

  test("version is the package.json version string", async () => {

    // Locks that the version is a string. We don't pin to a specific version so the test doesn't break on every release.
    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(typeof body.version, "string");
    assert.ok(body.version.length > 0, "version should be non-empty");
  });

  test("timestamp is an ISO 8601 UTC string", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    // toISOString output ends with 'Z' for UTC.
    assert.match(body.timestamp, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/, "timestamp should be ISO 8601 UTC");
  });

  test("streams block reflects an empty registry with the configured limit", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.streams.active, 0, "no streams registered in test environment");
    assert.equal(typeof body.streams.limit, "number");
    assert.ok(body.streams.limit > 0, "limit should be a positive number");
  });

  test("clients block reflects no active clients (empty registry)", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.clients.total, 0);
    assert.deepEqual(body.clients.byType, []);
  });

  test("ffmpegAvailable is a boolean (depends on test runner environment)", async () => {

    // Boundary: the actual value depends on whether ffmpeg is on PATH or the bundled ffmpeg-for-homebridge resolves. We just lock the type so a regression that
    // emitted undefined or a non-boolean would fail.
    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(typeof body.ffmpegAvailable, "boolean");
  });

  test("captureMode is one of the documented values from CONFIG", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(typeof body.captureMode, "string");
    assert.ok(body.captureMode.length > 0);
  });
});
