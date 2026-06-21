/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * debug.test.ts: Unit tests for the debug logging endpoint in debug.ts. setupDebugEndpoint registers GET /debug (renders the category management page) and
 * POST /debug (applies a new filter pattern and persists it). The page builder is a deterministic HTML generator over DEBUG_CATEGORIES; the POST handler
 * mutates the runtime filter via initDebugFilter and persists to config.json via mutateConfig. Tests run against an Express server with a temp data
 * directory so mutateConfig has a concrete target. The HTML structure is verified by checking for the documented sections and section headers.
 */
import type { AddressInfo, Server } from "node:net";
import { DEBUG_CATEGORIES, initDebugFilter } from "../utils/index.ts";
import { after, afterEach, before, beforeEach, describe, test } from "node:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { CONFIG } from "../config/index.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
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

    // serializeAttrs emits attributes in insertion order, so we slice the button tag by label and check the data-debug-action attribute independently rather
    // than asserting a positional layout (which would couple the test to insertion order).
    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    for(const [ label, action ] of [ [ "Apply", "apply" ], [ "Select All", "select-all" ], [ "Deselect All", "deselect-all" ] ] as const) {

      const tagMatch = new RegExp("<button [^>]*>" + label + "</button>").exec(body);

      assert.ok(tagMatch, label + " button is rendered");
      assert.match(tagMatch[0], new RegExp("data-debug-action=\"" + action + "\""), label + " button carries data-debug-action=\"" + action + "\"");
    }
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

  test("renders the category sections container", async () => {

    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    assert.match(body, /<div class="debug-pane">/);
    // The pane should hold at least one section card based on DEBUG_CATEGORIES content.
    assert.match(body, /<section class="debug-section">/);
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

  test("includes the init logic that sets indeterminate states on page load", async () => {

    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    // The HTML "checked" attribute cannot express the indeterminate state, so the script applies it from JS once the rows have rendered. The renderer emits
    // the HTML5 boolean-attribute form (a bare data-indeterminate, no value), so the init selector must also be presence-based, not value-based - any drift
    // here would silently fail to restore the indeterminate state.
    assert.match(body, /document\.querySelectorAll\('\[data-indeterminate\]'\)/);
    assert.match(body, /el\.indeterminate = true/);
  });

  test("includes theme styles in the rendered page (dark mode support)", async () => {

    // generatePageWrapper composes theme + page styles. We verify a known token is present.
    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    assert.match(body, /--surface-page:/, "should include the theme variable declarations");
  });

  test("every row in the pane contains exactly one input, one label, and one description span", async () => {

    // Structural invariant: the row template emits a 1:1:1:1 shape - one wrapper div, one checkbox, one label, one description span - for every variant. This
    // is what lets the four-track grid place every cell into the same column on every row. If the renderer ever drifts (e.g., header rows stop emitting an
    // empty desc span), the counts diverge and this test fails before the visual misalignment can ship.
    const res = await fetch(urlFor("/debug"));
    const body = await res.text();
    const paneMatch = (/<div class="debug-pane">([\s\S]*?)<form id="debug-form"/).exec(body);
    const pane = paneMatch?.[1] ?? "";

    assert.ok(pane.length > 0, "should find the debug-pane container");

    const rowOpenings = (pane.match(/<div class="debug-row\b/g) ?? []).length;
    const checkboxes = (pane.match(/<input type="checkbox"/g) ?? []).length;
    const labels = (pane.match(/<label for="/g) ?? []).length;
    const descs = (pane.match(/<span class="debug-row__desc"/g) ?? []).length;

    assert.ok(rowOpenings > 10, "there should be many rows (sanity check)");
    assert.equal(checkboxes, rowOpenings, "each row should have exactly one checkbox");
    assert.equal(labels, rowOpenings, "each row should have exactly one label");
    assert.equal(descs, rowOpenings, "each row should have exactly one description span");
  });

  test("namespaceless categories render with --standalone; namespaced categories do not", async () => {

    // Variant slotting is what makes alignment work. Standalones must carry the --standalone modifier so the CSS places their cells in the header tracks
    // (label spanning [checkbox]/[desc], description still in [desc]); grouped leaves must NOT carry it so they sit in the default leaf tracks. Drive the
    // expectation directly off DEBUG_CATEGORIES so the test grows with the registry rather than capturing today's list.
    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    for(const cat of DEBUG_CATEGORIES) {

      const isStandalone = !cat.category.includes(":");
      const escapedCategory = cat.category.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const standaloneRegex = new RegExp("<div class=\"debug-row debug-row--standalone\">[\\s\\S]{0,500}?id=\"cat-" + escapedCategory + "\"");
      const leafRegex = new RegExp("<div class=\"debug-row\">[\\s\\S]{0,500}?id=\"cat-" + escapedCategory + "\"");

      if(isStandalone) {

        assert.match(body, standaloneRegex, "namespaceless category '" + cat.category + "' should be in a --standalone row");
      } else {

        assert.match(body, leafRegex, "namespaced category '" + cat.category + "' should be in a default leaf row");
      }
    }
  });

  test("CSS owns the four-track grid on .debug-section and rows inherit via subgrid", async () => {

    // The load-bearing CSS: the section is the grid container that declares the column tracks (named for self-documentation), and every .debug-row inherits
    // those tracks via "grid-template-columns: subgrid". This is the single source of truth for column geometry; if either side is missing the layout falls
    // back to per-row "auto" sizing and descriptions drift across variants.
    const res = await fetch(urlFor("/debug"));
    const body = await res.text();

    assert.match(body, /\.debug-section\b[^}]*grid-template-columns:[^}]*\[indent\][^}]*\[checkbox\][^}]*\[label\][^}]*\[desc\][^}]*\}/);
    assert.match(body, /\.debug-row\b[^}]*grid-template-columns:\s*subgrid/);
  });

  test("checkboxes carry no inline onchange handlers (change events flow through the delegated listener)", async () => {

    // A single delegated change listener on .debug-pane handles every checkbox; the rendered HTML must not embed inline onchange handlers (which would
    // duplicate the dispatch logic into the markup and re-introduce the function-name surface we removed).
    const res = await fetch(urlFor("/debug"));
    const body = await res.text();
    const checkboxTags = body.match(/<input type="checkbox"[^>]*>/g) ?? [];

    assert.ok(checkboxTags.length > 10, "there should be many checkboxes (sanity check)");

    for(const tag of checkboxTags) {

      assert.ok(!tag.includes("onchange="), "checkbox should not carry inline onchange: " + tag);
    }
  });

  test("action buttons carry no inline onclick handlers (clicks flow through the delegated listener)", async () => {

    // The action-bar buttons declare their intent via data-debug-action and the delegated click listener on .debug-actions reads it. The rendered HTML must
    // not embed inline onclick handlers, which would re-introduce a function-name surface between the markup and the script.
    const res = await fetch(urlFor("/debug"));
    const body = await res.text();
    const buttonTags = body.match(/<button[^>]*>/g) ?? [];

    assert.ok(buttonTags.length >= 3, "should render at least the three action buttons");

    for(const tag of buttonTags) {

      assert.ok(!tag.includes("onclick="), "button should not carry inline onclick: " + tag);
      assert.ok(tag.includes("data-debug-action="), "button should declare its action via data-debug-action: " + tag);
    }
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
