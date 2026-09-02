/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the landing page route handler. The module exports setupRootEndpoint, which registers three Express routes plus internal HTML
 * generators for the page header, version display, and changelog modal, and renderVersionHtml, the pure renderer behind the version display. We attach the setup
 * to a real Express app on an OS-assigned port and exercise the routes via HTTP, then assert the rendered HTML body and the JSON envelopes returned by the version
 * endpoints have the expected shape. The renderer is exercised directly, since only a pure call can supply the crafted version strings the network path never
 * produces.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { renderVersionHtml, setupRootEndpoint } from "./index.ts";
import { ACTIONS } from "../clientActions.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../../testing.helpers.ts";
import express from "express";
import { initializeDataDir } from "../../config/paths.ts";
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

    // Drain background-server handles (puppeteer-stream's WebSocketServer, pulled in transitively via routes/root/content.ts -> routes/config/index.ts ->
    // browser/index.ts's closeBrowser import) now that our own Express server has been closed above. Without this drain the test runner would hang on
    // subprocess exit.
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

    // Every tab in the landing page's tabbed interface is wrapped by generateTabButton with role=tab. We confirm each tab name below appears as either a
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
    assert.match(body, /data-click-action="close-changelog-modal"/, "close button wired to the close-changelog-modal action");
  });

  test("includes the restart modal hidden by default", async () => {

    // The restart modal is rendered hidden and shown via JavaScript when a deferred restart needs operator confirmation. The data is non-conditional - present
    // on every page render.
    const body = await (await fetch(urlFor("/"))).text();

    assert.match(body, /id="restart-dialog"/);
    assert.match(body, /id="restart-stream-count"/);
    assert.match(body, /data-click-action="cancel-pending-restart"/);
    assert.match(body, /data-click-action="force-restart"/);
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

  test("every emitted data-*-action attribute resolves to a registered handler (runtime dispatch coverage)", async () => {

    /* Direction 1 of action coverage: every action the rendered HTML emits as a trigger MUST resolve to a window.registerAction handler. Orphans here surface
     * at runtime as silent no-ops with a console warning - clicks just don't do anything. We catch them before shipping.
     *
     * "Emission" covers the two shapes the runtime dispatcher actually sees: a static data-<event>-action="name" HTML attribute, OR a dynamic
     * setAttribute('data-<event>-action', 'name') call inside a script blob (used by handlers that retrofit a button after creation). The dispatcher does
     * not care which path produced the attribute - both end up as the same DOM state - so the test recognizes both.
     *
     * Direction 2 (registered-with-no-emission) is NOT asserted here. The landing page renders one conditional state (empty data fixture); actions that only
     * emit when the user has channels / profiles / deleted tags would false-positive as dead. The static dead-registration check lives in the next test,
     * which scans against the ACTIONS registry directly - typed, complete, fixture-independent.
     */
    const body = await (await fetch(urlFor("/"))).text();

    /* emissionRegexes is the SSOT for the shapes the dispatcher recognizes. Adding a future emission path (e.g., assignment via el.dataset.clickAction) is a
     * one-line edit here; the coverage check stays accurate without touching anything else.
     */
    const emissionRegexes: readonly RegExp[] = [
      /data-(?:click|change|keydown|submit)-action="([^"]+)"/g,
      /setAttribute\(\s*['"]data-(?:click|change|keydown|submit)-action['"]\s*,\s*['"]([^'"]+)['"]/g
    ];
    const emitted = new Set<string>();

    for(const regex of emissionRegexes) {

      for(const match of body.matchAll(regex)) {

        if(match[1] !== undefined) {

          emitted.add(match[1]);
        }
      }
    }

    // Collect every window.registerAction('<name>', ...) call in the embedded scripts.
    const registered = new Set<string>();

    for(const match of body.matchAll(/window\.registerAction\(\s*'([^']+)'\s*,/g)) {

      if(match[1] !== undefined) {

        registered.add(match[1]);
      }
    }

    // Sanity: both sets should be non-trivial (the page renders many actions and the scripts register many handlers).
    assert.ok(emitted.size > 30, "should emit many actions as triggers (sanity check); got " + String(emitted.size));
    assert.ok(registered.size > 30, "should register many action handlers (sanity check); got " + String(registered.size));

    const orphanEmissions = Array.from(emitted).filter((name) => !registered.has(name)).toSorted();

    assert.deepEqual(orphanEmissions, [], "every emitted action must have a registered handler; orphans: " + JSON.stringify(orphanEmissions));
  });

  test("every registered handler corresponds to a name in the ACTIONS registry (static dead-registration check)", async () => {

    /* Direction 2 of action coverage, recast as a static check. Reading rendered HTML cannot give a true negative on dead registrations because emission is
     * conditional on application state - the landing page render here has an empty data fixture, so per-channel buttons, user-profile rows, deleted-tag
     * restore buttons, and other state-gated triggers all sit unrendered, falsely shadowing their registrations.
     *
     * What we can assert structurally is: every registered name must be a value in the ACTIONS registry. ACTIONS is the typed SSOT for action identifiers
     * (clientActions.ts); a registration whose name is NOT a value there means the script blob hand-rolled a string literal instead of routing through the
     * registry, and would be invisible to compile-time renaming - a different but real form of dead registration.
     *
     * The corresponding "ACTIONS key with no source reference anywhere" guarantee already lives in the type system: every ACTIONS.<name> read at a callsite
     * is type-checked against the registry, so removing a key surfaces as compile errors at every consuming site. Combined, the two checks give the same
     * coverage as the runtime bidirectional sweep would, without the conditional-render false positives.
     */
    const body = await (await fetch(urlFor("/"))).text();
    const registered = new Set<string>();

    for(const match of body.matchAll(/window\.registerAction\(\s*'([^']+)'\s*,/g)) {

      if(match[1] !== undefined) {

        registered.add(match[1]);
      }
    }

    const validNames = new Set<string>(Object.values(ACTIONS));
    const unregistered = Array.from(registered).filter((name) => !validNames.has(name)).toSorted();

    assert.deepEqual(unregistered, [], "every registered handler name must come from the ACTIONS registry; off-registry names: " + JSON.stringify(unregistered));
  });

  test("every custom property the document references is also defined in the document", async () => {

    /* A var(--name) reference whose token is defined nowhere resolves to nothing, and the declaration it sits in is simply dropped by the browser - no console
     * error, no visual marker, just a rule that quietly does not apply. The landing page inlines the theme block alongside every stylesheet it renders, so the
     * fetched document holds both halves of the relationship and the check is a set difference over one string.
     *
     * Definitions are read over the WHOLE document rather than only its <style> blocks, so a token declared from a script (an inline style built client-side,
     * for instance) still counts as defined and cannot produce a false failure. Leftovers are listed in the failure message, so a false positive names its own
     * cause instead of leaving the reader to hunt for it.
     */
    const body = await (await fetch(urlFor("/"))).text();
    const referenced = new Set(Array.from(body.matchAll(/var\(\s*(--[a-z0-9-]+)/g), (match) => match[1]));
    const defined = new Set(Array.from(body.matchAll(/(--[a-z0-9-]+)\s*:/g), (match) => match[1]));

    // Sanity: a document that produced neither set would make the difference below vacuously empty.
    assert.ok(referenced.size > 50, "the page should reference many custom properties (sanity check); got " + String(referenced.size));
    assert.ok(defined.size > 50, "the page should define many custom properties (sanity check); got " + String(defined.size));

    const undefinedTokens = Array.from(referenced.difference(defined)).toSorted();

    assert.deepEqual(undefinedTokens, [], "every referenced custom property must be defined in the document; undefined: " + JSON.stringify(undefinedTokens));
  });

  test("every emitted data-<event>-(action|prevent-default|stop-propagation|close-dropdown) attribute uses a supported event type", async () => {

    /* The dispatcher listens for exactly four event types: click, change, keydown, submit. A typo (data-keydon-action, missing "w") or an unsupported event
     * type (data-pointerdown-prevent-default) silently no-ops at runtime - the modifier or action never fires, and nothing catches it. This test walks every
     * data-*-(action|prevent-default|stop-propagation|close-dropdown) attribute in the rendered HTML and asserts the event prefix is in the supported set.
     * Class of bug it catches: any future emission of an attribute name whose event word is misspelled or out of scope for the dispatcher.
     */
    const body = await (await fetch(urlFor("/"))).text();

    const SUPPORTED_EVENTS = new Set([ "change", "click", "keydown", "submit" ]);
    const attrRegex = /data-([a-z]+)-(action|prevent-default|stop-propagation|close-dropdown)\b/g;
    const offenders = new Set<string>();
    let m: RegExpExecArray | null;

    while((m = attrRegex.exec(body)) !== null) {

      const eventName = m[1];

      if((eventName !== undefined) && !SUPPORTED_EVENTS.has(eventName)) {

        offenders.add("data-" + eventName + "-" + String(m[2]));
      }
    }

    assert.deepEqual(Array.from(offenders).sort(), [],
      "every modifier/action attribute must use a supported event type (click/change/keydown/submit); offenders: " +
      JSON.stringify(Array.from(offenders).sort()));
  });
});

describe("renderVersionHtml", () => {

  // A version string carrying markup is the whole point of the renderer being pure: the latest version arrives from the npm registry, and a registry response
  // that carried this payload would inject it straight into the page header if the renderer trusted the string. The network path never has to produce it for
  // the escaping to be worth asserting.
  const HOSTILE_VERSION = "1.2.3<img src=x onerror=1>";

  test("escapes the current version on the no-update branch", () => {

    const html = renderVersionHtml(HOSTILE_VERSION, { latestVersion: null, updateAvailable: false });

    assert.match(html, /&lt;img src=x onerror=1&gt;/, "the markup in the current version must be entity-encoded");
    assert.doesNotMatch(html, /<img/, "no raw tag may reach the rendered markup");
  });

  test("escapes both the current version and the latest version on the update branch", () => {

    const html = renderVersionHtml(HOSTILE_VERSION, { latestVersion: HOSTILE_VERSION, updateAvailable: true });

    // Both interpolation sites sit inside the same anchor, so a single unescaped one is enough to break out of the text node. We count the encoded form to
    // confirm both were escaped rather than just the first.
    assert.equal((html.match(/&lt;img src=x onerror=1&gt;/g) ?? []).length, 2, "both version strings must be entity-encoded");
    assert.doesNotMatch(html, /<img/, "no raw tag may reach the rendered markup");
  });

  test("renders the update anchor with both versions when an update is available", () => {

    // Ordinary semantic versions contain no reserved characters, so escaping is the identity function on them and the markup is exactly what the page header
    // renders. This is the parity half of the escaping rows: escaping must not alter the shape of the normal case.
    const html = renderVersionHtml("1.0.0", { latestVersion: "1.1.0", updateAvailable: true });

    assert.match(html, /<span class="version-container">/, "the update branch renders the plain container");
    assert.match(html, /class="version version-update"/, "the update branch carries the version-update class");
    assert.match(html, /v1\.0\.0 &rarr; v1\.1\.0/, "both versions render with the arrow separator");
    assert.match(html, /class="version-check"/, "the refresh button renders on the update branch");
  });

  test("renders the current-version anchor and no arrow when no update is available", () => {

    const html = renderVersionHtml("1.0.0", { latestVersion: null, updateAvailable: false });

    assert.match(html, /<span class="version-container" id="version-display">/, "the no-update branch renders the identified container");
    assert.match(html, />v1\.0\.0<\/a>/, "the running version renders as the anchor text");
    assert.doesNotMatch(html, /&rarr;/, "no arrow separator without an update");
    assert.doesNotMatch(html, /version-update/, "no update class without an update");
  });

  test("renders the no-update branch when updateAvailable is true but no latest version was recorded", () => {

    // Boundary: the branch reads the flag and the version together, so a truthy flag with a null version falls through to the current-version form rather than
    // rendering an anchor that ends in "v null".
    const html = renderVersionHtml("1.0.0", { latestVersion: null, updateAvailable: true });

    assert.match(html, /id="version-display"/, "a missing latest version falls through to the no-update branch");
    assert.doesNotMatch(html, /&rarr;/, "no arrow separator without a latest version");
  });
});
