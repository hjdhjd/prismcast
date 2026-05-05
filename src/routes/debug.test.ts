/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * debug.test.ts: Unit tests for the debug logging endpoint in debug.ts. setupDebugEndpoint registers GET /debug (renders the category management page) and
 * POST /debug (applies a new filter pattern and persists it). The page builder is a deterministic HTML generator over DEBUG_CATEGORIES; the POST handler
 * mutates the runtime filter via initDebugFilter and persists to config.json via mutateConfig. Tests run against an Express server with a temp data
 * directory so mutateConfig has a concrete target. The HTML structure is verified by checking for the documented sections and section headers.
 */
import type { AddressInfo, Server } from "node:net";
import { after, afterEach, before, beforeEach, describe, test } from "node:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { CONFIG } from "../config/index.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { initDebugFilter } from "../utils/index.ts";
import { initializeDataDir } from "../config/paths.ts";
import os from "node:os";
import path from "node:path";
import { setupDebugEndpoint } from "./debug.ts";

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  // The POST handler reads req.body via Express's json/urlencoded parsers. Wire the urlencoded parser to match the form submission shape used by the page.
  app.use(express.urlencoded({ extended: true }));
  app.use(express.json());
  setupDebugEndpoint(app);

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
const ORIGINAL_DEBUG_FILTER = CONFIG.logging.debugFilter;

function urlFor(p: string): string {

  return "http://127.0.0.1:" + String(sharedPort) + p;
}

before(async () => {

  // The POST handler persists via mutateConfig, which needs a data directory and a config.json file to exist. We seed a minimal empty JSON object so the
  // configStore can read-modify-write it without complaining about missing/corrupt content.
  tempDataDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-debug-test-"));
  initializeDataDir(tempDataDir);
  await writeFile(path.join(tempDataDir, "config.json"), "{}\n");

  const created = await makeServer();

  sharedServer = created.server;
  sharedPort = created.port;
});

after(async () => {

  await closeServer(sharedServer);
  await rm(tempDataDir, { force: true, recursive: true });

  // Restore the runtime filter and CONFIG state so other test files see the original.
  initDebugFilter(ORIGINAL_DEBUG_FILTER);
  CONFIG.logging.debugFilter = ORIGINAL_DEBUG_FILTER;
  await closePuppeteerStreamWss();
});

describe("setupDebugEndpoint - GET /debug (HTML page render)", () => {

  test("returns 200 with text/html content type", async () => {

    const res = await fetch(urlFor("/debug"));

    assert.equal(res.status, 200);
    assert.match(res.headers.get("content-type") ?? "", /text\/html/);

    // Drain the body so the connection can release.
    await res.text();
  });

  test("page body is a complete HTML document with the documented title", async () => {

    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    assert.match(body, /^<!DOCTYPE html>/, "should start with DOCTYPE");
    assert.match(body, /<title>Debug Logging<\/title>/, "should render the page title");
    assert.match(body, /<\/html>$/, "should close the html element");
  });

  test("renders the page header and intro paragraph", async () => {

    // Locks the documented hero copy.
    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    assert.match(body, /<h1>Debug Logging<\/h1>/);
    assert.match(body, /Select categories to enable debug output/);
  });

  test("renders the action buttons (Apply, Select All, Deselect All)", async () => {

    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    assert.match(body, /onclick="applyPattern\(\)">Apply</);
    assert.match(body, /onclick="selectAll\(true\)">Select All</);
    assert.match(body, /onclick="selectAll\(false\)">Deselect All</);
  });

  test("renders the raw pattern input field", async () => {

    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    assert.match(body, /<input type="text" id="raw-pattern"/);
    assert.match(body, /placeholder="e\.g\. \*,-streaming:ffmpeg or tuning:hulu,recovery"/);
  });

  test("renders the hidden form for POST submission", async () => {

    // The Apply button submits a hidden form that posts the pattern field.
    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    assert.match(body, /<form id="debug-form" method="POST" action="\/debug"/);
    assert.match(body, /<input type="hidden" id="debug-form-pattern" name="pattern"/);
  });

  test("renders the category groups container", async () => {

    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    assert.match(body, /<div class="debug-groups">/);
    // The group container should hold at least one group based on DEBUG_CATEGORIES content.
    assert.match(body, /<div class="debug-group">/);
  });

  test("emits the debug script (selectAll, applyPattern, syncRaw) so checkboxes work", async () => {

    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    // We sample a few function names from the generated script. A regression that lost the script section would surface as missing fns.
    assert.match(body, /function applyPattern/);
    assert.match(body, /function selectAll/);
    assert.match(body, /function syncRawFromCheckboxes/);
    assert.match(body, /function syncCheckboxesFromRaw/);
  });

  test("includes the inline init script that sets indeterminate states", async () => {

    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    // The inline init script preserves indeterminate state across the HTML serialization barrier (the checked attribute cannot express it).
    assert.match(body, /querySelectorAll\('\[data-indeterminate="true"\]'\)/);
  });

  test("includes theme styles in the rendered page (dark mode support)", async () => {

    // generatePageWrapper composes theme + page styles. We verify a known token is present.
    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    assert.match(body, /--surface-page:/, "should include the theme variable declarations");
  });
});

describe("setupDebugEndpoint - POST /debug (filter persistence)", () => {

  beforeEach(() => {

    // Reset the runtime filter and CONFIG before each test so prior pattern state doesn't leak through.
    initDebugFilter("");
    CONFIG.logging.debugFilter = "";
  });

  afterEach(() => {

    // Restore between tests for safety. The after() hook does the final restore.
    initDebugFilter("");
    CONFIG.logging.debugFilter = "";
  });

  test("redirects (HTTP 303 See Other) to /debug after applying a pattern", async () => {

    // The handler responds with res.redirect(303, "/debug"). We disable redirect following so we can verify the 303 status directly.
    const res = await fetch(urlFor("/debug"), {

      body: new URLSearchParams({ pattern: "tuning:hulu" }),
      method: "POST",
      redirect: "manual"
    });

    assert.equal(res.status, 303);
    assert.equal(res.headers.get("location"), "/debug");
    await res.text();
  });

  test("normalizes the pattern (whitespace around commas is stripped)", async () => {

    // Boundary: initDebugFilter normalizes "tuning:hulu, recovery" to "tuning:hulu,recovery". The handler then writes the canonical form into CONFIG.
    const res = await fetch(urlFor("/debug"), {

      body: new URLSearchParams({ pattern: "tuning:hulu, recovery" }),
      method: "POST",
      redirect: "manual"
    });

    await res.text();

    assert.equal(CONFIG.logging.debugFilter, "tuning:hulu,recovery", "in-memory CONFIG should hold the normalized form");
  });

  test("accepts an empty pattern (clears the filter)", async () => {

    // Boundary: a POST with no pattern field sets the filter to "" (empty). The handler treats missing/non-string as empty.
    const res = await fetch(urlFor("/debug"), {

      body: new URLSearchParams(),
      method: "POST",
      redirect: "manual"
    });

    assert.equal(res.status, 303);
    await res.text();
    assert.equal(CONFIG.logging.debugFilter, "");
  });

  test("trims surrounding whitespace from the pattern", async () => {

    // Boundary: the handler calls .trim() on the body field before passing to initDebugFilter.
    const res = await fetch(urlFor("/debug"), {

      body: new URLSearchParams({ pattern: "   tuning:hulu   " }),
      method: "POST",
      redirect: "manual"
    });

    await res.text();
    assert.equal(CONFIG.logging.debugFilter, "tuning:hulu");
  });

  test("accepts the wildcard pattern", async () => {

    const res = await fetch(urlFor("/debug"), {

      body: new URLSearchParams({ pattern: "*" }),
      method: "POST",
      redirect: "manual"
    });

    assert.equal(res.status, 303);
    await res.text();
    assert.equal(CONFIG.logging.debugFilter, "*");
  });

  test("accepts an exclusion pattern (e.g., '*,-streaming:ffmpeg')", async () => {

    // Boundary: the canonical form preserves negation and the wildcard. We lock the round-trip.
    const res = await fetch(urlFor("/debug"), {

      body: new URLSearchParams({ pattern: "*,-streaming:ffmpeg" }),
      method: "POST",
      redirect: "manual"
    });

    assert.equal(res.status, 303);
    await res.text();
    assert.equal(CONFIG.logging.debugFilter, "*,-streaming:ffmpeg");
  });
});
