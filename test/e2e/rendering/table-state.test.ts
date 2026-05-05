/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * table-state.test.ts: Integration coverage for the channel-table state aggregator (buildChannelTableState), the patch builder (buildChannelTablePatch), and
 * the tag-vocabulary renderers (generateTagFilterContent, generateTagManagerBody) - composed against real channel state populated through the integration
 * harness rather than against synthetic listings. The unit tier (src/routes/config/channels/table.test.ts) covers each function's local invariants in
 * isolation: counts.total = predefined+user, counts.enabled+counts.disabled = total, the patch shape carries counts/hdhrCounts/rows/scopeCounts, the tag
 * markers' wrapper classes appear, etc. Those are stable invariants that hold regardless of state. This suite tests the orthogonal surface: how the helpers
 * RESPOND to mutations through real production write paths - disabling a predefined channel, applying a service filter, customizing a field, requesting a
 * patch for multiple keys at once. The bug class this catches is patch-shape regressions (too few or too many rows in the response) and state-class drift
 * (a row missing the disabled/unavailable class even though the underlying state is correct).
 *
 * Companion suite: test/e2e/rendering/channels-table.test.ts pins generateChannelRowHtml's canonical/variant/override visual classes against
 * generateChannelRowHtml directly. This suite extends to the patch path - the same state classes must travel through buildChannelTablePatch onto the rendered
 * row HTML, because every CRUD endpoint response uses that path to refresh the client-side table.
 */
import { afterEach, describe, test } from "node:test";
import { buildChannelTablePatch, buildChannelTableState, generateChannelRowHtml, generateTagFilterContent,
  generateTagManagerBody } from "../../../src/routes/config/channels/table.ts";
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { disablePredefinedChannels, enablePredefinedChannels, mutateChannels } from "../../../src/config/userChannels.ts";
import { getServiceTagForChannel, setEnabledServices } from "../../../src/config/services.ts";
import assert from "node:assert/strict";
import { firstOf } from "../../../src/testing.helpers.ts";
import { getProfiles } from "../../../src/config/profiles.ts";

// PREDEFINED_KEY is documented as a real predefined channel - the existing channels-table integration suite uses it, and table.test.ts at the unit tier uses
// it. It is the canonical test channel for any predefined-state assertion in this codebase.
const PREDEFINED_KEY = "abc";

describe("buildChannelTablePatch - composition against real channel state", () => {

  afterEach(() => {

    // Reset the service filter to the no-filter state. Disabled-predefined state is per-test - each test that disables a channel re-enables it before
    // exiting because mutateChannels writes to the per-test tmp data dir, but the in-memory CONFIG.channels.disabledPredefined list is process-wide.
    setEnabledServices([]);
  });

  test("returns rows ONLY for the affected keys (not the full listing)", async () => {

    /* The most common patch-shape regression: a CRUD endpoint asks for a single-key update and gets the full table back. The wire-level symptom is bloated
     * response bodies on every save; the architectural symptom is patch-shape drift between the SSOT and the consuming endpoints. We seed three real channels
     * (one predefined, one user-only, one user-override on a predefined) and request a patch for ONE of them. The response must contain exactly one row.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["custom-row"] = { name: "Custom", url: "https://example.test/custom" };
      data.channels[PREDEFINED_KEY] = { channelNumber: 99 };
    });

    const patch = buildChannelTablePatch([PREDEFINED_KEY], getProfiles());

    assert.equal(patch.rows.length, 1, "exactly one row when one key is requested");

    const row = firstOf(patch.rows, "first patch row");

    assert.equal(row.key, PREDEFINED_KEY, "row key matches the requested key");
    assert.equal(row.action, "update", "an existing key produces an update action");
  });

  test("the patch row HTML matches generateChannelRowHtml for the same key (single source of truth invariant)", async () => {

    /* The patch builder is documented as the single source of truth for refresh-after-mutation responses, and generateChannelRowHtml is the single source of
     * truth for full page-load row rendering. Both paths must produce byte-identical HTML for the same channel state - if they ever diverge, a save would
     * show different markup than a page reload, and the user-visible bug is "the table looks different after I saved than after I refreshed."
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    // Customize the predefined channel so the row carries the channel-override class, exercising a non-default rendering path.
    await mutateChannels((data) => {

      data.channels[PREDEFINED_KEY] = { channelNumber: 7 };
    });

    const profiles = getProfiles();
    const patchRow = firstOf(buildChannelTablePatch([PREDEFINED_KEY], profiles).rows, "patch row");
    const directRow = generateChannelRowHtml(PREDEFINED_KEY, profiles);

    assert.equal(patchRow.action, "update");
    assert.equal(patchRow.displayRow, directRow.displayRow, "patch displayRow must equal generateChannelRowHtml.displayRow");
    assert.equal(patchRow.editRow, directRow.editRow, "patch editRow must equal generateChannelRowHtml.editRow");
  });

  test("a disabled predefined channel renders with the channel-disabled class in the patch row HTML", async () => {

    /* The user disables a predefined channel via the column toggle. The patch response that flows back must mark the row with the channel-disabled CSS class
     * so the client-side renderer can apply the visual treatment. A regression that drops the class would render disabled channels with the same opacity as
     * enabled ones - operators have no visual cue that a channel is off.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    // Verify the baseline: the row is NOT marked disabled before we disable it. This is the matched-pair invariant.
    const beforeRow = firstOf(buildChannelTablePatch([PREDEFINED_KEY], getProfiles()).rows, "row before disable");

    assert.equal(beforeRow.action, "update");
    assert.doesNotMatch(beforeRow.displayRow ?? "", /\bclass="[^"]*\bchannel-disabled\b/, "enabled rows must NOT carry channel-disabled");

    // Disable, request a fresh patch, assert the class appears.
    await disablePredefinedChannels([PREDEFINED_KEY]);

    const afterRow = firstOf(buildChannelTablePatch([PREDEFINED_KEY], getProfiles()).rows, "row after disable");

    assert.equal(afterRow.action, "update", "the row stays in the patch as an update (not removed) because the channel is hidden, not deleted");
    assert.match(afterRow.displayRow ?? "", /\bclass="[^"]*\bchannel-disabled\b/, "disabled predefined rows MUST carry the channel-disabled class");

    // Cleanup so the suite-level afterEach does not see a non-empty disabled list.
    await enablePredefinedChannels([PREDEFINED_KEY]);
  });

  test("a service-filtered channel renders with the channel-unavailable class in the patch row HTML", async () => {

    /* The service filter (CONFIG.channels.enabledServices) hides channels whose service tag is not in the enabled list. Production keeps the row in the DOM
     * with reduced visibility CSS (channel-unavailable) rather than removing it - the row stays sortable and the count totals stay consistent across filter
     * toggles. We use "amcthrillers" because it carries only sling and yttv variants - no "direct" tag. The "direct" tag is structurally always enabled
     * (isServiceTagEnabled returns true for "direct" regardless of the filter), so a channel like "abc" that has a direct/network-owned variant can never be
     * service-filtered. Any test asserting the channel-unavailable class must use a channel without a direct variant.
     */
    const FILTERABLE_KEY = "amcthrillers";

    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const tag = getServiceTagForChannel(FILTERABLE_KEY);

    assert.ok(tag && (tag.length > 0), "predefined channel must have a resolvable service tag for this scenario to be valid");
    assert.notEqual(tag, "direct", "the channel must NOT have a direct tag - direct is always enabled, defeating the test");

    // Baseline assertion before applying the filter.
    const beforeRow = firstOf(buildChannelTablePatch([FILTERABLE_KEY], getProfiles()).rows, "row before filter");

    assert.doesNotMatch(beforeRow.displayRow ?? "", /\bclass="[^"]*\bchannel-unavailable\b/, "rows with no filter applied must NOT carry channel-unavailable");

    // Apply a filter that includes a deliberately unmatching tag - none of amcthrillers' service tags match "this-tag-matches-nothing".
    setEnabledServices(["this-tag-matches-nothing"]);

    const afterRow = firstOf(buildChannelTablePatch([FILTERABLE_KEY], getProfiles()).rows, "row after filter");

    assert.match(afterRow.displayRow ?? "", /\bclass="[^"]*\bchannel-unavailable\b/, "service-filtered rows MUST carry the channel-unavailable class");
  });
});

describe("buildChannelTableState - composition against real channel state", () => {

  test("disabling a predefined channel decrements counts.enabled and increments counts.disabled (no total drift)", async () => {

    /* The summary counts feed the channels tab header ("12 / 200 channels"). A regression that miscounts disabled channels surfaces as a wrong header number,
     * which operators notice immediately. The unit suite locks the local invariants (total = enabled + disabled, total = predefined + user); this test pins
     * the transition - a single disable mutation must move exactly one channel from enabled to disabled with no change in total or in user/predefined splits.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const before = buildChannelTableState();

    await disablePredefinedChannels([PREDEFINED_KEY]);

    const after = buildChannelTableState();

    assert.equal(after.counts.total, before.counts.total, "total must NOT change - the channel still exists, just disabled");
    assert.equal(after.counts.disabled, before.counts.disabled + 1, "disabled count increments by exactly one");
    assert.equal(after.counts.enabled, before.counts.enabled - 1, "enabled count decrements by exactly one (mirror of disabled)");
    assert.equal(after.counts.predefined, before.counts.predefined, "predefined/user split is unchanged - disable does not move the channel between buckets");

    await enablePredefinedChannels([PREDEFINED_KEY]);
  });

  test("scopeCounts reflect the predefined East/Pacific/all distribution and stay consistent with counts", async () => {

    /* The scopeCounts feed the East/Pacific scope toggle in the channel table header. Each scope reports its own enabled/total, so a regression that
     * miscomputes one would surface as an asymmetric toggle (e.g., "East 50/100" vs "Pacific 0/100" when both should match). We pin the structural invariant:
     * each scope's total is non-negative, enabled <= total, and scopeCounts.all.total bounds the per-scope totals.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const state = buildChannelTableState();

    assert.ok(state.scopeCounts.all.total > 0, "the predefined catalog must be non-empty in the test fixture");
    assert.ok(state.scopeCounts.east.enabled <= state.scopeCounts.east.total, "East enabled cannot exceed East total");
    assert.ok(state.scopeCounts.pacific.enabled <= state.scopeCounts.pacific.total, "Pacific enabled cannot exceed Pacific total");
    assert.ok(state.scopeCounts.east.total <= state.scopeCounts.all.total, "East total cannot exceed all-scope total");
    assert.ok(state.scopeCounts.pacific.total <= state.scopeCounts.all.total, "Pacific total cannot exceed all-scope total");
  });
});

describe("tag vocabulary renderers", () => {

  test("generateTagManagerBody includes a list entry for every tag in the active vocabulary", async () => {

    /* The tag manager modal lists every tag in the active vocabulary so operators can rename or delete them. A regression that omits a tag from the list
     * makes that tag invisible to the operator - they cannot edit it through the UI. We seed two user tags via mutateChannels' tag registry, then assert each
     * one shows up in the rendered HTML as a tag-manager-item with the tag's display name.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.tagRegistry.tags = [ ...data.tagRegistry.tags, "TestTagOne", "TestTagTwo" ];
    });

    const html = generateTagManagerBody();

    assert.match(html, /data-tag="TestTagOne"/, "manager body must include TestTagOne as a tag-manager-item data attribute");
    assert.match(html, /data-tag="TestTagTwo"/, "manager body must include TestTagTwo as a tag-manager-item data attribute");
    assert.match(html, /tag-manager-input/, "the input field is present so operators can add new tags");
  });

  test("generateTagFilterContent emits a filter checkbox for every tag in the active vocabulary", async () => {

    /* The tag filter dropdown drives the channels-table tag column filter. Every active vocabulary tag must appear as a tag-filter-checkbox so operators can
     * toggle column visibility per-tag. The renderer also emits a "Show None" toggle below the divider when the vocabulary is non-empty - we assert that
     * boundary too because an empty divider+toggle on an empty vocabulary would be a layout regression.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.tagRegistry.tags = [ ...data.tagRegistry.tags, "FilterTagAlpha", "FilterTagBeta" ];
    });

    const html = generateTagFilterContent();

    assert.match(html, /class="tag-filter-checkbox"[^>]*data-tag="FilterTagAlpha"/, "filter content emits a checkbox for FilterTagAlpha");
    assert.match(html, /class="tag-filter-checkbox"[^>]*data-tag="FilterTagBeta"/, "filter content emits a checkbox for FilterTagBeta");
    assert.match(html, /id="tag-filter-toggle"/, "non-empty vocabulary emits the Show None toggle below the divider");
  });
});
