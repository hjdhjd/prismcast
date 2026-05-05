/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the landing page route handler. The module exports a single setup function (setupRootEndpoint) that registers three Express
 * routes plus internal HTML generators for the page header, version display, and changelog modal. We attach the setup to a real Express app on an OS-assigned
 * port and exercise the routes via HTTP, then assert structural invariants on the rendered HTML body and the JSON envelopes returned by the version endpoints.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../../testing.helpers.ts";
import express from "express";
import { initializeDataDir } from "../../config/paths.ts";
import { setupRootEndpoint } from "./index.ts";
import { tmpdir } from "node:os";

// makeServer spins up an Express app on an OS-assigned port and registers the root endpoints.
function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupRootEndpoint(app);

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
let sharedTempDir = "";

function urlFor(path: string): string {

  return "http://127.0.0.1:" + String(sharedPort) + path;
}

describe("setupRootEndpoint", () => {

  before(async () => {

    // Initialize a per-suite data directory so the route handler's transitive call to getConfigFilePath has a populated module-level path. We mkdtempSync
    // upfront and tear down in after() rather than using withTempDir because the lifetime spans every test in the describe block.
    sharedTempDir = mkdtempSync(tmpdir() + "/prismcast-root-test-");
    initializeDataDir(sharedTempDir);

    const { port, server } = await makeServer();

    sharedPort = port;
    sharedServer = server;
  });

  after(async () => {

    await closeServer(sharedServer);

    if(sharedTempDir) {

      rmSync(sharedTempDir, { force: true, recursive: true });
    }

    // Drain background-server handles (puppeteer-stream's WebSocketServer, pulled in transitively via routes/playlist.ts -> ... -> browser/index.ts) now that
    // our own Express server has been closed above. Without this drain the test runner would hang on subprocess exit.
    await closePuppeteerStreamWss();
  });

  test("registers the GET / route and the response is a complete HTML document", async () => {

    // The landing page is built from generatePageWrapper("PrismCast", ...) which emits a full <!DOCTYPE html> wrapper. The body contains the page-level chrome
    // the route handler assembles: tab bar, header, status container.
    const res = await fetch(urlFor("/"));
    const body = await res.text();

    assert.equal(res.status, 200, "GET / should return 200");
    assert.match(body, /<!DOCTYPE html>/i, "should be a full HTML document");
    assert.match(body, /<title>[^<]*PrismCast[^<]*<\/title>/, "title should mention PrismCast");
  });

  test("includes the header with logo, title, and version display", async () => {

    // Header chrome from generateHeaderStatusHtml + the inline header generator. The static elements (logo image, h1, version-container) are non-conditional.
    const body = await (await fetch(urlFor("/"))).text();

    assert.match(body, /<div class="header">/, "header div present");
    assert.match(body, /<img src="\/logo\.svg"/, "logo img present");
    assert.match(body, /<h1>PrismCast<\/h1>/, "title h1 present");
    assert.match(body, /class="version-container"/, "version container present");
  });

  test("includes the system status bar with health and stream count placeholders", async () => {

    // generateHeaderStatusHtml renders #system-status with #system-health and #stream-count placeholders that the client-side SSE script populates on load.
    const body = await (await fetch(urlFor("/"))).text();

    assert.match(body, /id="system-status"/);
    assert.match(body, /id="system-health"/);
    assert.match(body, /id="stream-count"/);
  });

  test("includes all six tab buttons for the landing page", async () => {

    // The six tabs are: overview, channels, logs, config, api, help. generateTabButton wraps these with role=tab. We confirm each tab name appears as either a
    // data attribute or an id reference in the tab bar markup.
    const body = await (await fetch(urlFor("/"))).text();

    for(const tab of [ "overview", "channels", "logs", "config", "api", "help" ]) {

      const re = new RegExp("(data-(?:category|tab)=\"" + tab + "\")|(id=\"tab-" + tab + "\")|(tab-btn[^>]*\\b" + tab + "\\b)");

      assert.match(body, re, "should include tab marker for " + tab);
    }
  });

  test("includes the changelog modal with placeholder content", async () => {

    const body = await (await fetch(urlFor("/"))).text();

    assert.match(body, /id="changelog-modal"/);
    assert.match(body, /class="changelog-modal-content"/);
    assert.match(body, /class="changelog-loading"/, "loading placeholder before async fetch");
    assert.match(body, /onclick="closeChangelogModal\(\)"/, "close button wired");
  });

  test("includes the restart modal hidden by default", async () => {

    // The restart modal is rendered hidden and shown via JavaScript when a deferred restart needs operator confirmation. The data is non-conditional - present
    // on every page render.
    const body = await (await fetch(urlFor("/"))).text();

    assert.match(body, /id="restart-dialog"/);
    assert.match(body, /id="restart-stream-count"/);
    assert.match(body, /onclick="cancelPendingRestart\(\)"/);
    assert.match(body, /onclick="forceRestart\(\)"/);
  });

  test("includes the toast container for client-side notifications", async () => {

    // Shared utility script's showToast() appends to #toast-container. The container is rendered server-side so it's available before any script runs.
    const body = await (await fetch(urlFor("/"))).text();

    assert.match(body, /id="toast-container"/);
    assert.match(body, /class="toast-container"/);
  });

  test("inlines styles, tab script, and SSE status script in the document", async () => {

    // The page wrapper interpolates the styles + scripts arguments. We confirm at least one style block (styles open with a CSS rule) and at least one inline
    // script tag (the SSE/status script) are present.
    const body = await (await fetch(urlFor("/"))).text();

    assert.match(body, /<style[^>]*>/i, "at least one style block");
    assert.match(body, /<script[\s>]/i, "at least one inline script");
  });

  test("does not produce template-literal artifacts in the rendered body", async () => {

    // Sanity check for stringified missing values - either undefined leaked into a string or [object Object] from a coerced object.
    const body = await (await fetch(urlFor("/"))).text();

    assert.doesNotMatch(body, /\$\{undefined\}/);
    assert.doesNotMatch(body, /\[object Object\]/);
  });

  test("registers the POST /version/check endpoint and responds with JSON containing currentVersion", async () => {

    // The endpoint always returns { currentVersion, latestVersion, updateAvailable } as JSON. We do not assert on values - the latest version may or may not be
    // populated depending on prior /version/check calls during the process lifetime - but the response shape and JSON content type should hold.
    const res = await fetch(urlFor("/version/check"), { method: "POST" });

    assert.equal(res.status, 200);

    const parsed = await res.json() as Record<string, unknown>;

    assert.ok("currentVersion" in parsed, "currentVersion field present");
    assert.ok("updateAvailable" in parsed, "updateAvailable field present");
    assert.equal(typeof parsed["currentVersion"], "string", "currentVersion should be a string");
  });

  test("registers the GET /version/changelog endpoint and responds with JSON containing displayVersion", async () => {

    const res = await fetch(urlFor("/version/changelog"));

    assert.equal(res.status, 200);

    const parsed = await res.json() as Record<string, unknown>;

    assert.ok("displayVersion" in parsed, "displayVersion field present");
    assert.ok("items" in parsed, "items field present (may be null if not found)");
    assert.ok("updateAvailable" in parsed, "updateAvailable field present");
  });

  test("renders the same HTML body on repeated requests (deterministic page generation)", async () => {

    // The route handler should be deterministic for a given runtime configuration. Two back-to-back requests should produce identical body text. This locks
    // out accidental Date.now() or random number creep in the page generator.
    const a = await (await fetch(urlFor("/"))).text();
    const b = await (await fetch(urlFor("/"))).text();

    assert.equal(a, b, "two identical requests should produce identical pages");
  });

  test("includes the global #toast-container plus the SSE-driven status placeholders", async () => {

    // Spot check that the toast notifications system + system status placeholders both render. These have to coexist for the page to function as designed.
    const body = await (await fetch(urlFor("/"))).text();

    assert.match(body, /id="toast-container"/);
    assert.match(body, /id="system-status"/);
  });
});
