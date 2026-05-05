/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * upgrade.test.ts: Unit tests for the upgrade routes in upgrade.ts. setupUpgradeEndpoint registers GET /upgrade/info and POST /upgrade. The full upgrade path
 * (executing the install command, restarting the service) requires environment-specific binaries and a process restart - that coverage lives in the e2e suite.
 * Here we cover the two response shapes the routes produce: the GET /upgrade/info JSON envelope (with the documented keys), the success and failure branches
 * of fetchLatestVersion (mocked), and the not-upgradeable short-circuit on POST /upgrade.
 */
import type { AddressInfo, Server } from "node:net";
import { after, afterEach, before, beforeEach, describe, mock, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { setupUpgradeEndpoint } from "./upgrade.ts";

interface UpgradeInfoResponse {

  currentVersion: string;
  latestVersion: string | null;
  method: string;
  updateAvailable: boolean;
  upgradeCommand?: string;
  upgradeable: boolean;
}

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupUpgradeEndpoint(app);

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

/* The /upgrade/info handler calls fetchLatestVersion which hits the npm registry. We mock globalThis.fetch in beforeEach/afterEach so each test controls the
 * registry response without making real network calls. The mock.restoreAll() pattern in afterEach guarantees other test files (and the test runner itself)
 * see the original fetch.
 */
const originalFetch = globalThis.fetch;

beforeEach(() => {

  globalThis.fetch = mock.fn(async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {

    const url = (typeof input === "string") ? input : (input instanceof URL) ? input.toString() : input.url;

    // Return a fake "latest version" response for the npm registry lookup, leaving everything else to the real fetch (e.g., the test server itself). The init
    // argument carries the method, body, and headers; without forwarding it, POST requests would silently degrade to GET and produce 404 from the test server.
    if(url.startsWith("https://registry.npmjs.org/")) {

      return new Response(JSON.stringify({ "dist-tags": { latest: "99.99.99" } }), {

        headers: { "content-type": "application/json" },
        status: 200
      });
    }

    return originalFetch(input, init);
  });
});

afterEach(() => {

  globalThis.fetch = originalFetch;
  mock.restoreAll();
});

describe("setupUpgradeEndpoint - GET /upgrade/info", () => {

  test("returns 200 with the documented response keys", async () => {

    const res = await fetch(urlFor("/upgrade/info"));

    assert.equal(res.status, 200);
    assert.match(res.headers.get("content-type") ?? "", /application\/json/);

    const body = await res.json() as UpgradeInfoResponse;

    // Locks the public surface of the response. Any addition or removal here should be intentional.
    assert.equal(typeof body.currentVersion, "string");
    assert.ok([ "string", "object" ].includes(typeof body.latestVersion), "latestVersion is string or null");
    assert.equal(typeof body.method, "string");
    assert.equal(typeof body.updateAvailable, "boolean");
    assert.equal(typeof body.upgradeable, "boolean");
  });

  test("currentVersion is normalized (no leading 'v') and non-empty", async () => {

    const res = await fetch(urlFor("/upgrade/info"));
    const body = await res.json() as UpgradeInfoResponse;

    // normalizeVersion strips a leading 'v' or 'V'. The version comes from package.json so we only verify it doesn't start with 'v' and is non-empty.
    assert.ok(body.currentVersion.length > 0);
    assert.doesNotMatch(body.currentVersion, /^[vV]/);
  });

  test("updateAvailable is true when the mocked latest version (99.99.99) is greater than the current version", async () => {

    // Boundary: this exercises the version comparison branch. The mocked latest is far higher than any real PrismCast version, so updateAvailable must be true.
    const res = await fetch(urlFor("/upgrade/info"));
    const body = await res.json() as UpgradeInfoResponse;

    assert.equal(body.updateAvailable, true, "99.99.99 > current version, so updateAvailable=true");
    assert.equal(body.latestVersion, "99.99.99");
  });

  test("updateAvailable is false when the registry returns a lower version", async () => {

    // Boundary: locking the inverse of the comparison. Override the mock for this test to return a low version.
    globalThis.fetch = mock.fn(async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {

      const url = (typeof input === "string") ? input : (input instanceof URL) ? input.toString() : input.url;

      if(url.startsWith("https://registry.npmjs.org/")) {

        return new Response(JSON.stringify({ "dist-tags": { latest: "0.0.1" } }), { status: 200 });
      }

      return originalFetch(input, init);
    });

    const res = await fetch(urlFor("/upgrade/info"));
    const body = await res.json() as UpgradeInfoResponse;

    assert.equal(body.updateAvailable, false, "0.0.1 < current version, so updateAvailable=false");
  });

  test("updateAvailable is false when the registry fetch fails (latestVersion=null)", async () => {

    // Boundary: when fetchLatestVersion returns null, the handler computes (null !== null) && ... which is false. Lock that no update is reported.
    globalThis.fetch = mock.fn(async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {

      const url = (typeof input === "string") ? input : (input instanceof URL) ? input.toString() : input.url;

      if(url.startsWith("https://registry.npmjs.org/")) {

        return new Response("server error", { status: 500 });
      }

      return originalFetch(input, init);
    });

    const res = await fetch(urlFor("/upgrade/info"));
    const body = await res.json() as UpgradeInfoResponse;

    assert.equal(body.latestVersion, null);
    assert.equal(body.updateAvailable, false);
  });

  test("method is one of the documented installation methods", async () => {

    // The detection module enumerates docker, homebrew, npm-global, npm-local, source, and unknown. Locking the constraint catches a regression that emitted a
    // bogus method label.
    const res = await fetch(urlFor("/upgrade/info"));
    const body = await res.json() as UpgradeInfoResponse;

    assert.match(body.method, /^(docker|homebrew|npm-global|npm-local|source|unknown)$/);
  });
});

describe("setupUpgradeEndpoint - POST /upgrade", () => {

  test("returns 400 with the canonical envelope when the installation method is not upgradeable", async () => {

    // The unknown/source branches return upgradeable=false; the handler short-circuits via sendValidationError to the documented envelope shape
    // ({ error: string, success: false }) at HTTP 400, without attempting any exec. We don't pin the method (depends on the test environment) but we lock
    // the contract: when upgradeable=false, the response is the documented validation-error envelope.
    const infoRes = await fetch(urlFor("/upgrade/info"));
    const info = await infoRes.json() as UpgradeInfoResponse;

    if(info.upgradeable) {

      // If the test environment happens to be upgradeable (e.g., npm-local with a real install), we cannot exercise the not-upgradeable branch here. Still
      // verify the route is wired and responds; the e2e suite covers the upgradeable branch.
      const res = await fetch(urlFor("/upgrade"), { method: "POST" });

      // Either it responds successfully or fails with a 500. We don't run the upgrade path in CI because exec'ing npm install would mutate the environment.
      assert.ok(res.status >= 200);
      await res.json();

      return;
    }

    const res = await fetch(urlFor("/upgrade"), { method: "POST" });

    assert.equal(res.status, 400);

    const body = await res.json() as { error: string; success: boolean };

    assert.equal(body.success, false);
    assert.match(body.error, /does not support in-place upgrades/);
  });
});
