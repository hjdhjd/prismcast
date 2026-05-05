/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hash-navigation.test.ts: Integration coverage for the server side of the URL-hash navigation contract. Phase 2.5 Suite 33 was investigation-first - the
 * roadmap framed the question as "what does the server actually do with hash state?" and asked the test author to pin whichever the actual behavior is.
 *
 * Investigation finding (Suite 33): the server's role in tab/hash activation is structurally NIL. The landing page handler at src/routes/root/index.ts:148
 * builds the tab bar with overview always marked active (generateTabButton's third arg) and emits every tab panel and subtab in source-fixed default-active
 * state. No req.query consultation, no hash hint via header, no server-side variation by URL. Tab and subtab activation is wholly client-side - generateTabScript
 * (in routes/ui.ts) and createSubtabSwitcher (in routes/root/scripts/shared.ts) read window.location.hash on the client and toggle classes accordingly.
 *
 * Therefore the server-side contract is narrow: GET / produces stable, URL-independent HTML carrying the default-active markers; deep-link reload behavior is
 * the client controller's responsibility (out of integration-tier scope; reserved for browser-e2e coverage if/when that tier exists). The four tests below pin
 * exactly that:
 *
 *   1. Default-active state on GET / - overview tab marked active, all other tabs inactive. Pins the deterministic baseline.
 *   2. URL query parameters do not influence the response - GET /?tab=channels and GET /?something=else produce HTML byte-identical to GET /. Pins the absence
 *      of server-side hash/query interpretation as a structural invariant.
 *   3. Subtab default-active state inside the rendered tabs - channels tab has the "channels" subtab active, config tab has the "settings" subtab active.
 *      Pins the per-tab subtab convention so a regression that defaulted to a different subtab would surface here, not on a deep-link reload bug report.
 *   4. The 6 tab content generators all render successfully against an empty seed state. Defensive coverage: a content generator that throws on empty data
 *      becomes a 500 on the landing page.
 *
 * Architectural note: an HTML-aware DOM library would simplify the assertions but adds a dependency for one suite. The substring/regex approach matches the
 * pattern established by channels-table.test.ts and wizard-modal.test.ts; the tests stay legible and focused.
 */
import { bootApp, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

// The 6 top-level tabs in source order. Overview is the default active. Drives the parameterized "every other tab is inactive" assertion in test 1.
const TAB_CATEGORIES = [ "overview", "channels", "logs", "config", "api", "help" ] as const;
const DEFAULT_ACTIVE_TAB = "overview";

describe("GET / - server-side hash/tab navigation contract", () => {

  test("default active state: overview tab carries class active and aria-selected=true; every other tab does not", async () => {

    /* The deterministic baseline. The landing page handler builds the tab bar by calling generateTabButton(category, label, isActive). Only overview is invoked
     * with isActive=true. Test pins that contract structurally: for each of the 6 tabs, locate its <button> via data-category and assert its class and
     * aria-selected reflect the correct default-active state. A regression that defaulted to a different tab (or marked multiple tabs active) would surface
     * here as the unexpected tab carrying the active marker.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/"));

    assert.equal(response.status, 200, "landing page should return 200");

    const html = await response.text();

    for(const category of TAB_CATEGORIES) {

      const buttonPattern = new RegExp("<button [^>]*data-category=\"" + category + "\"[^>]*>");
      const buttonMatch = buttonPattern.exec(html);

      assert.ok(buttonMatch, "tab button for " + category + " must be present");

      const tag = buttonMatch[0];
      const expectedActive = (category === DEFAULT_ACTIVE_TAB);

      assert.equal(tag.includes("class=\"tab-btn active"), expectedActive, category + " tab carries class \"active\" iff it is the default-active tab");
      assert.equal(tag.includes("aria-selected=\"true\""), expectedActive, category + " tab carries aria-selected=\"true\" iff it is the default-active tab");
    }
  });

  test("server ignores URL query parameters for tab activation: GET /?tab=channels emits HTML byte-identical to GET /", async () => {

    /* Negative invariant. The server-side contract is "stable HTML regardless of URL state"; the client-side controller is responsible for hash-driven
     * activation after page load. This test pins that absence by comparing two responses: one with a query string, one without. They must be byte-identical.
     *
     * A regression that quietly added req.query.tab handling in app.get("/") would silently shift this contract from "client-only activation" to "split
     * server+client activation," which is exactly the architectural smell the roadmap flagged. The byte-equality assertion catches that regression even when
     * the surface symptom (a different tab marked active server-side) might pass eyeball QA.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const responses = await Promise.all([

      fetch(urlFor("/")),
      fetch(urlFor("/?tab=channels")),
      fetch(urlFor("/?tab=config&something=else"))
    ]);

    for(const response of responses) {

      assert.equal(response.status, 200, "every variant URL should return 200");
    }

    const bodies = await Promise.all(responses.map(async (response) => response.text()));

    assert.equal(bodies[0], bodies[1], "GET /?tab=channels emits the same HTML as GET / (server does not consult req.query for tab activation)");
    assert.equal(bodies[0], bodies[2], "GET / with arbitrary query parameters emits the same HTML as GET / (server query is structurally ignored)");
  });

  test("subtab default-active state: channels tab activates the channels subtab; config tab activates the settings subtab", async () => {

    /* Each of the multi-subtab top-level tabs (channels, config) emits its subtab buttons with exactly one active. The channels tab has subtabs "channels" and
     * "custom-profiles" with "channels" active by default. The config tab has settings + advanced + backup with "settings" active by default. We assert the
     * default-active markers (class active + aria-selected=true) on the expected subtabs and the default-inactive state on the others.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/"));
    const html = await response.text();

    // Channels subtabs.
    const channelsActiveMatch = /<button [^>]*data-channels-subtab="channels"[^>]*>/.exec(html);
    const customProfilesMatch = /<button [^>]*data-channels-subtab="custom-profiles"[^>]*>/.exec(html);

    assert.ok(channelsActiveMatch, "channels-subtab=channels button must be present");
    assert.ok(customProfilesMatch, "channels-subtab=custom-profiles button must be present");
    assert.match(channelsActiveMatch[0], /class="channels-subtab-btn active"/, "channels subtab is default active");
    assert.match(channelsActiveMatch[0], /aria-selected="true"/, "channels subtab carries aria-selected=true");
    assert.doesNotMatch(customProfilesMatch[0], /class="channels-subtab-btn active"/, "custom-profiles subtab is NOT default active");
    assert.match(customProfilesMatch[0], /aria-selected="false"/, "custom-profiles subtab carries aria-selected=false");

    // Config subtabs. The settings subtab is default active; advanced and backup are inactive.
    const settingsMatch = /<button [^>]*data-subtab="settings"[^>]*>/.exec(html);
    const advancedMatch = /<button [^>]*data-subtab="advanced"[^>]*>/.exec(html);
    const backupMatch = /<button [^>]*data-subtab="backup"[^>]*>/.exec(html);

    assert.ok(settingsMatch, "settings subtab button must be present");
    assert.ok(advancedMatch, "advanced subtab button must be present");
    assert.ok(backupMatch, "backup subtab button must be present");
    assert.match(settingsMatch[0], /class="subtab-btn active"/, "settings subtab is default active in config tab");
    assert.doesNotMatch(advancedMatch[0], /class="subtab-btn active"/, "advanced subtab is NOT default active");
    assert.doesNotMatch(backupMatch[0], /class="subtab-btn active"/, "backup subtab is NOT default active");
  });

  test("every tab content panel is non-empty against an empty seed state (defensive: no content generator throws on empty data)", async () => {

    /* Defensive invariant. With a fresh data directory, all user channels, profiles, tags, and disabledPredefined lists are empty. Each of the 6 tab content
     * generators runs against this empty state when GET / is invoked. A generator that threw on empty data (NPE on undefined.length, division by zero, an
     * invariant assuming at least one item) would render the entire landing page as a 500. This test runs the empty-state landing-page render and asserts that
     * (a) it returns 200, (b) every tab's panel <div> is present, (c) every tab's panel contains some HTML body content (not an empty <div>).
     *
     * Suite 39 covers per-tab structural correctness in detail; Suite 33's job here is the basic survival check: empty seed must not crash any tab generator.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/"));

    assert.equal(response.status, 200, "landing page must render 200 against an empty seed (no generator may throw)");

    const html = await response.text();

    for(const category of TAB_CATEGORIES) {

      const panelPattern = new RegExp("<div id=\"panel-" + category + "\"[^>]*class=\"tab-panel(?: active)?\"[^>]*role=\"tabpanel\">([\\s\\S]*?)<\\/div>");
      const panelMatch = panelPattern.exec(html);

      assert.ok(panelMatch, "panel-" + category + " <div> must be rendered");

      const body = panelMatch[1] ?? "";

      assert.ok(body.trim().length > 0, "panel-" + category + " body must contain rendered content (non-empty against empty seed)");
    }
  });
});
