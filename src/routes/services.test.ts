/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.test.ts: Unit tests for the service channel discovery route in services.ts. setupServicesEndpoint registers GET /services/:slug/channels which
 * dispatches to a provider's discoverChannels function inside a temporary browser page. The full discovery path requires a real Chrome browser and a live
 * service guide - that coverage lives in the e2e suite. Here we cover the synchronous validation branches that run before any browser interaction: unknown
 * slug returns 404 with a descriptive error, and the documented response shape for the unknown-slug branch is locked.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { setupServicesEndpoint } from "./services.ts";

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupServicesEndpoint(app);

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

describe("setupServicesEndpoint - GET /services/:slug/channels (unknown slug)", () => {

  test("returns 404 for an unknown service slug (locks the validation branch)", async () => {

    // Negative test: the handler calls getProviderBySlug() and returns 404 with a descriptive error before any browser interaction. This lets the test exercise
    // the route registration and validation pass-through without a real Chrome browser running.
    const res = await fetch(urlFor("/services/totally-not-a-real-service-x9z2/channels"));

    assert.equal(res.status, 404);
  });

  test("response body for unknown slug includes the slug name in the error message", async () => {

    // The error message is "Unknown service: <slug>." - we lock the format so a regression that loses the specific slug surface as a real diff.
    const res = await fetch(urlFor("/services/madeUpSlug/channels"));
    const body = await res.json() as { error: string };

    assert.match(body.error, /Unknown service/);
    assert.match(body.error, /madeUpSlug/);
  });

  test("emits Content-Type application/json for the 404 response", async () => {

    const res = await fetch(urlFor("/services/bogus/channels"));

    assert.match(res.headers.get("content-type") ?? "", /application\/json/);
    await res.json();
  });

  test("a path that does not match the documented :slug/channels shape falls through to Express's default 404", async () => {

    // Boundary: only /services/:slug/channels is registered. A nearby path like /services should not be picked up by this route.
    const res = await fetch(urlFor("/services"));

    assert.equal(res.status, 404);
    await res.text();
  });

  test("an empty slug segment falls through to Express's default 404 (path-to-regexp does not match)", async () => {

    // Boundary: an empty path segment for :slug doesn't satisfy path-to-regexp, so the route doesn't match. Express returns its default 404.
    const res = await fetch(urlFor("/services//channels"));

    assert.equal(res.status, 404);
    await res.text();
  });
});

describe("setupServicesEndpoint - GET /services/:slug/channels (refresh/lineup query parameters)", () => {

  test("refresh=true with unknown slug still returns 404 (validation runs before refresh handling)", async () => {

    // Boundary: the slug check is the first thing the handler does. The refresh=true branch runs only when a known provider is found, so unknown+refresh still
    // produces a 404 with the standard error.
    const res = await fetch(urlFor("/services/totally-not-a-real-service-x9z2/channels?refresh=true"));

    assert.equal(res.status, 404);
    await res.json();
  });

  test("lineup=true with unknown slug still returns 404", async () => {

    const res = await fetch(urlFor("/services/totally-not-a-real-service-x9z2/channels?lineup=true"));

    assert.equal(res.status, 404);
    await res.json();
  });
});
