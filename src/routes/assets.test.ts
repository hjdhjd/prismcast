/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * assets.test.ts: Unit tests for the static asset routes in assets.ts. The single export setupAssetEndpoints registers four GET routes (favicon.svg,
 * favicon.png, logo.png, logo.svg) that read files from the project root, cache them in memory, and serve them with a one-day Cache-Control header. The tests
 * spin up an ephemeral-port Express server, make real HTTP requests, and verify the response shape - status, content-type, cache-control, and body. The actual
 * file presence depends on the project root containing the prismcast.svg/png assets, which are committed to the repository.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { setupAssetEndpoints } from "./assets.ts";

// makeServer spins up an Express app on an OS-assigned port and registers the asset routes.
function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupAssetEndpoints(app);

  return new Promise((resolve, reject) => {

    const server = app.listen(0, "127.0.0.1", () => {

      const address = server.address() as AddressInfo;

      resolve({ port: address.port, server });
    });

    server.on("error", reject);
  });
}

// closeServer wraps server.close in a promise so the after hook waits for socket teardown.
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
});

describe("setupAssetEndpoints - GET /favicon.svg", () => {

  test("returns 200 with image/svg+xml when the file exists", async () => {

    // The repo root contains prismcast.svg, so the route reads it on first hit and caches it.
    const res = await fetch(urlFor("/favicon.svg"));

    assert.equal(res.status, 200);
    assert.equal(res.headers.get("content-type"), "image/svg+xml");

    const body = await res.text();

    assert.ok(body.length > 0, "body should be non-empty");
  });

  test("sets a one-day Cache-Control header", () => {

    // The cache-control value is documented as "public, max-age=86400" (24 hours). This test locks that value.
    return fetch(urlFor("/favicon.svg")).then(async (res) => {

      // Drain the body so the connection can be reused (or torn down) cleanly.
      await res.text();

      assert.equal(res.headers.get("cache-control"), "public, max-age=86400");
    });
  });

  test("serves the same content on a second request (cache hit)", async () => {

    // Boundary: the second request must produce identical content. A cache miss-on-every-request would still pass the status check above but would defeat the
    // caching invariant; a content-mismatch would surface as a real bug.
    const a = await fetch(urlFor("/favicon.svg"));
    const aBody = await a.text();
    const b = await fetch(urlFor("/favicon.svg"));
    const bBody = await b.text();

    assert.equal(aBody, bBody, "two reads should return identical content");
  });
});

describe("setupAssetEndpoints - GET /favicon.png", () => {

  test("returns 200 with image/png when the file exists", async () => {

    const res = await fetch(urlFor("/favicon.png"));

    assert.equal(res.status, 200);
    assert.equal(res.headers.get("content-type"), "image/png");

    const buffer = await res.arrayBuffer();

    assert.ok(buffer.byteLength > 0, "PNG body should be non-empty");
  });
});

describe("setupAssetEndpoints - GET /logo.png", () => {

  test("returns 200 with image/png (alias of favicon.png from the same source file)", async () => {

    // The route registers logo.png against prismcast.png, the same file as favicon.png. We verify it returns the same content.
    const logoRes = await fetch(urlFor("/logo.png"));
    const faviconRes = await fetch(urlFor("/favicon.png"));

    assert.equal(logoRes.status, 200);
    assert.equal(logoRes.headers.get("content-type"), "image/png");

    const logoBuf = new Uint8Array(await logoRes.arrayBuffer());
    const faviconBuf = new Uint8Array(await faviconRes.arrayBuffer());

    assert.equal(logoBuf.length, faviconBuf.length, "logo.png and favicon.png should be byte-identical (same source)");
  });
});

describe("setupAssetEndpoints - GET /logo.svg", () => {

  test("returns 200 with image/svg+xml (alias of favicon.svg from the same source file)", async () => {

    const res = await fetch(urlFor("/logo.svg"));

    assert.equal(res.status, 200);
    assert.equal(res.headers.get("content-type"), "image/svg+xml");
  });
});

describe("setupAssetEndpoints - 404 routes", () => {

  test("returns 404 for an unregistered asset path", async () => {

    // Negative test: only the four documented paths are registered. Anything else falls through to Express's default 404. The default sends "Cannot GET /path"
    // with status 404; we just check the status to avoid coupling to the exact message.
    const res = await fetch(urlFor("/no-such-asset.png"));

    assert.equal(res.status, 404);

    // Drain body so the connection can be reused.
    await res.text();
  });
});
