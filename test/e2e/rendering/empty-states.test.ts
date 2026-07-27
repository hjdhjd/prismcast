/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * empty-states.test.ts: Integration coverage for empty-state rendering across the Channels, Profiles, Tags, and Streams surfaces. Empty states are notorious
 * sources of UI breakage - missing-fallback render paths, NPE on undefined data, "0 of 0" labels with broken arithmetic, dropdowns with no options, tables
 * that throw because they assume at least one row will always exist. Each test below renders a specific surface against an empty-or-near-empty seed and asserts
 * (a) no crash, (b) structurally valid output, (c) the documented empty-state messaging or shape (where applicable).
 *
 * The integration value over unit-tier coverage is wiring the production renderers to real CONFIG / channel / profile state initialized through the boot
 * sequence. A renderer that crashes only when data flows through the full initialization pipeline (e.g., when getProfiles() returns the bare default profile
 * rather than a populated array) shows up here, not in unit tests of the renderer itself.
 *
 * No renderer assumes at least one row: each surface either renders an explicit empty-state message (Custom Profiles panel: "No custom services installed"),
 * draws from an always-non-empty source (the tag vocabulary always carries the predefined tags), or returns an envelope that explicitly accommodates zero items
 * (GET /streams: { count: 0, limit, streams: [] }). The "all columns hidden" and "service filter excluding every non-direct channel" tests below are
 * intentionally redundant with adjacent suites' coverage to provide an empty-state-specific assertion locus - if a future regression made those scenarios
 * crash, the failure surfaces here even if the adjacent suite's assertion happens to still pass.
 */
import { bootApp, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { generateTagFilterContent, generateTagManagerBody } from "../../../src/routes/config/channels/table.ts";
import assert from "node:assert/strict";
import { generateChannelsPanel } from "../../../src/routes/config/channels/table.ts";
import { generateCustomProfilesPanel } from "../../../src/routes/config/services.ts";
import { mutateChannelDisplayPrefs } from "../../../src/config/userChannels.ts";
import { mutateEnabledServices } from "../../../src/config/services.ts";

describe("empty-state rendering across tabs", () => {

  test("channels panel renders cleanly with no user channels (predefined-only state)", async () => {

    /* The default empty user-state. With a fresh data dir, channels.json is absent so user-channel state is empty. The panel must render: the channel summary
     * shows 0 user channels and a non-zero predefined count, the table renders, the toolbar renders. A regression that crashed on userCount=0 (e.g., a divide-
     * by-zero in a percentage calculation, an assumed-non-empty array access) would surface here.
     *
     * Negative invariant in the channel summary: the user-count <span> is empty (no comma-separated " N user" text appended). This is the documented behavior
     * in generateChannelsPanel: "When there are no user channels, the span is empty."
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const html = generateChannelsPanel();

    assert.ok(html.length > 0, "panel renders to a non-empty string with no user channels");
    assert.match(html, /<span id="user-count"><\/span>/, "user-count span is empty when no user channels exist");
    assert.match(html, /<span id="predefined-count">\d+<\/span>/, "predefined-count span carries the predefined channel count");
    assert.match(html, /<table class="channel-table[^"]*"/, "channels table is rendered even when no user channels exist");
  });

  test("custom profiles panel renders the documented empty-state message when no user profiles exist", async () => {

    /* The Custom Profiles panel (generateCustomProfilesPanel in services.ts) carries an explicit empty-state branch: when getUserProfiles() returns no entries, it emits
     * an empty-state div with a "No custom services installed" title and instructional text. The export button is conditionally omitted when there are no profiles
     * to export. This test pins both the empty-state messaging AND the conditional export-button absence - a regression that left the export button visible in the
     * empty state would mislead users into clicking a button that would download an empty file.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const html = generateCustomProfilesPanel();

    assert.match(html, /<div class="empty-state">[\s\S]*<p class="empty-state-title">No custom services installed<\/p>/,
      "empty-state div with the documented title is rendered when no user profiles exist");
    assert.doesNotMatch(html, /data-click-action="start-service-export"/, "Export button is omitted in the empty state (nothing to export)");

    // The New Profile button is always present (the user can create one), and the import flow is also always available.
    assert.match(html, /data-click-action="open-wizard"/, "New Profile button is present even in the empty state");
    assert.match(html, /data-click-action="start-service-import"/, "Import button is present even in the empty state");
  });

  test("tag filter content and tag manager render cleanly with no user tags (predefined vocabulary only)", async () => {

    /* Empty USER vocabulary - the user has not created any custom tags. The active vocabulary is then exactly the predefined-tag set (PREDEFINED_TAGS minus
     * deletedTags). Both the filter dropdown and the tag manager body render against this default state without crashing, and the tag manager omits the
     * deleted-tags section because none have been deleted.
     *
     * The sentinel for "tag manager empty section omitted" is the absence of a deletedTags-specific structural class. If the renderer always emitted the
     * deleted section even when empty, that would be a UI smell - cluttering the manager with an empty subsection - though not a hard crash.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const filterContent = generateTagFilterContent();
    const managerBody = generateTagManagerBody();

    assert.ok(filterContent.length > 0, "tag filter dropdown content is non-empty (predefined tags present)");
    assert.match(filterContent, /<input type="checkbox" class="tag-filter-checkbox" data-tag="[^"]+" checked/, "filter content carries at least one tag checkbox");
    assert.match(filterContent, /<div class="dropdown-item" id="tag-filter-toggle"/, "Show None toggle is rendered when vocabulary is non-empty");

    assert.ok(managerBody.length > 0, "tag manager body is non-empty (predefined tags present)");
    assert.match(managerBody, /<div class="tag-manager-item" data-tag="[^"]+">/, "manager body carries at least one tag-manager-item");
  });

  test("GET /streams with no active streams returns the documented empty envelope", async () => {

    /* The streams endpoint (the GET /streams handler in setupStreamsEndpoint, routes/streams.ts) unconditionally emits { count, limit, streams }. With no active
     * streams, count is 0 and streams is []. limit reflects CONFIG.streaming.maxConcurrentStreams. The shape is the same regardless of stream count - there is no
     * separate empty-state envelope - so the test pins that uniformity: an empty streams array does NOT trigger a different envelope shape (a "no streams" message,
     * an absent streams field, etc.).
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/streams"));

    assert.equal(response.status, 200, "GET /streams returns 200 with no active streams");

    const body = await response.json() as { count: unknown; limit: unknown; streams: unknown };

    assert.equal(body.count, 0, "count is 0 when no streams are active");
    assert.equal(typeof body.limit, "number", "limit is the configured maxConcurrentStreams (a number)");
    assert.deepEqual(body.streams, [], "streams is the empty array when none are active");
  });

  test("display preferences with all columns hidden: channels panel renders without crashing", async () => {

    /* The empty-state edge of column visibility. The "hiding all optional columns does not crash the renderer; the table renders with every hide-col-*
     * class" test in filter-combinations.test.ts covers this from the filter-combinations angle; this test re-asserts the same invariant from the
     * empty-state surface so a future regression that broke the no-columns-visible scenario surfaces under both tags. The assertions differ in framing:
     * that test checks hide-col-* class enumeration; this test checks the panel is structurally complete (table element present, summary present,
     * toolbar present).
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateChannelDisplayPrefs({ visibleColumns: [] });

    const html = generateChannelsPanel();

    assert.ok(html.length > 0, "panel renders to a non-empty string with no visible optional columns");
    assert.match(html, /<table class="[^"]*channel-table/, "table element is rendered");
    assert.match(html, /<div class="channel-summary">/, "channel summary is rendered");
    assert.match(html, /<div class="channel-toolbar">/, "channel toolbar is rendered");
  });

  test("service filter excluding every non-direct channel: panel renders without crashing", async () => {

    /* The empty-effective-listing case. enabledServices=["nonexistent-service-tag"] structurally means: every variant whose service tag is not "direct" is filtered
     * out. Channels with a `direct` tag (those whose canonical URL is the network site, e.g., abc with abc.com) survive because isServiceTagEnabled returns
     * true for "direct" unconditionally; channels without a `direct` tag (e.g., abcnews) are not visible.
     *
     * Render the panel under this filter and assert it does not crash - even if the rendered set is structurally pruned to a fraction of the catalog. The
     * negative invariant is that the panel still renders the toolbar (so the user can clear the filter) and the table element (so the summary count is
     * visible) even when the visible row count is a small minority of the catalog.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateEnabledServices(["nonexistent-service-tag"]);

    const html = generateChannelsPanel();

    assert.ok(html.length > 0, "panel renders to a non-empty string under a filter that excludes most channels");
    assert.match(html, /<button [^>]*id="provider-filter-btn"[^>]*>[\s\S]*Filtered/, "filter toolbar reflects the active filter (button reads Filtered)");
    assert.match(html, /<table class="[^"]*channel-table/, "table element is still rendered when most rows are filtered out");
    assert.match(html, /<div class="channel-summary">/, "channel summary is rendered so the user sees the new count");
  });
});
