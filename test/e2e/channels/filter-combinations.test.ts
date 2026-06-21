/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * filter-combinations.test.ts: Integration coverage for the channel-table panel under combinations of service filter, sort, and column-visibility settings.
 * Each individual mechanism is unit-tested or pinned by a Phase 1/2 suite, but their COMBINATION is not - and combinations are where regressions hide. A
 * mutation that "preserved sort" while quietly resetting visibleColumns, or "applied service filter" while crashing on an empty visibleColumns array, would
 * not surface in any single-dimension test.
 *
 * The architectural surface under test is generateChannelsPanel() in src/routes/config/channels/table.ts. The panel reads:
 *   - enabledServices via getEnabledServices() / getVisibleChannels() (the service filter dimension).
 *   - CONFIG.channels.channelSortField / channelSortDirection (sort dimension).
 *   - CONFIG.channels.visibleColumns (column-visibility dimension).
 *
 * Each test seeds a non-default value on each axis it cares about (via mutateEnabledServices / mutateChannelDisplayPrefs), renders the panel, and asserts
 * structural reflection of the dimension. The "preservation" tests mutate one axis after another and assert the previously-set axis is unchanged at render time.
 *
 * Channels DVR fixture: abcnews has variants {cox, directv, hulu, sling, xfinity, yttv} and no `site`/`direct` tag, so the service filter is load-bearing for it
 * (Suite 26's canonical fixture). amcthrillers has only {sling, yttv} so it falls out of getVisibleChannels under enabledServices=["hulu"]. abc has a `direct`
 * tag (always enabled) so it survives any narrow service filter. The fixture is a real predefined channel set; no user channels are seeded.
 */
import { bootApp, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { generateChannelsPanel } from "../../../src/routes/config/channels/table.ts";
import { mutateChannelDisplayPrefs } from "../../../src/config/userChannels.ts";
import { mutateEnabledServices } from "../../../src/config/services.ts";

describe("generateChannelsPanel - filter / sort / column visibility combinations", () => {

  test("service filter + sort + column visibility combined: each dimension is reflected in the rendered panel", async () => {

    /* The combinatorial baseline. Set a non-default value on every dimension and assert the rendered panel reflects all three:
     *   - enabledServices = ["hulu"] - the service filter chip for Hulu must appear.
     *   - sortField = "channelNumber", sortDirection = "desc" - the table's data-sort-field / data-sort-dir attributes must reflect this.
     *   - visibleColumns = ["channelNumber"] - the table's class list must include hide-col-* for every NON-visible optional column (stationId, profile,
     *     selector, hdhrEnabled, tags), and must NOT include hide-col-chnum for the visible one.
     *
     * A regression in any single dimension would show up here as a failing assertion on its specific marker; a regression in dimension interaction (e.g., a
     * mutation pipeline that resets columns when filter is set) would surface as a divergence between the input mutations and the output markers.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices(["hulu"]);
    await mutateChannelDisplayPrefs({ channelSortDirection: "desc", channelSortField: "channelNumber", visibleColumns: ["channelNumber"] });

    const html = generateChannelsPanel();

    // Service filter dimension: a Hulu chip must be present (provider-chip carries the data-tag attribute) and the toolbar button reads "Filtered."
    assert.match(html, /<button [^>]*id="provider-filter-btn"[^>]*>[\s\S]*Filtered/, "service filter toolbar button reads 'Filtered' when a filter is active");
    assert.match(html, /<span class="provider-chip" data-tag="hulu">/, "service filter chip for hulu must be rendered when enabledServices includes hulu");

    // Sort dimension: the table's data-sort-field and data-sort-dir reflect the mutation.
    assert.match(html, /<table class="[^"]*"[^>]*data-sort-field="channelNumber"[^>]*data-sort-dir="desc"/,
      "table carries data-sort-field=channelNumber and data-sort-dir=desc");

    // Column visibility dimension: visibleColumns=["channelNumber"] means the channelNumber column is visible (no hide-col-chnum class) and every other optional
    // column is hidden (hide-col-* class present). We assert against a representative subset; the full enumeration is not necessary because the loop in the
    // renderer iterates OPTIONAL_COLUMNS uniformly.
    const tableClassMatch = /<table class="([^"]+)"/.exec(html);

    assert.ok(tableClassMatch, "table element must be present");

    const tableClasses = tableClassMatch[1] ?? "";

    assert.equal(tableClasses.includes("hide-col-chnum"), false, "hide-col-chnum must be absent (channelNumber is visible)");
    assert.equal(tableClasses.includes("hide-col-stationid"), true, "hide-col-stationid must be present (stationId is hidden)");
    assert.equal(tableClasses.includes("hide-col-tags"), true, "hide-col-tags must be present (tags is hidden)");
  });

  test("changing sort after a service filter is set leaves the filter active in the rendered panel", async () => {

    /* Mutation-order independence: the order in which the user interacts with filter and sort must not matter. We set the filter first, then change the sort,
     * and assert the filter is STILL active when the panel renders. This pins the absence of any side-effect coupling in mutateChannelDisplayPrefs that would
     * accidentally clear enabledServices.
     *
     * The mutateChannelDisplayPrefs implementation in userChannels.ts reads CONFIG.channels.* for absent fields and writes the merged result; if the merge
     * accidentally projected enabledServices through and reset it, this test would fail.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices(["hulu"]);
    await mutateChannelDisplayPrefs({ channelSortField: "name" });

    const html = generateChannelsPanel();

    assert.match(html, /<span class="provider-chip" data-tag="hulu">/, "service filter chip for hulu must persist after a sort mutation");
    assert.match(html, /data-sort-field="name"/, "sort field reflects the new value");
  });

  test("changing column visibility after a service filter is set leaves the filter active in the rendered panel", async () => {

    /* Symmetric to the sort-change test. visibleColumns is a different shape (array vs. scalar) and a different shouldPreserve predicate (isNonEmptyArray vs.
     * differsFromStringDefault), so the persistence path is independently exercised - a regression in only the array branch of mutateChannelDisplayPrefs would
     * surface here, not in the prior test.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices(["hulu"]);
    await mutateChannelDisplayPrefs({ visibleColumns: [ "channelNumber", "stationId" ] });

    const html = generateChannelsPanel();

    assert.match(html, /<span class="provider-chip" data-tag="hulu">/, "service filter chip for hulu must persist after a visibleColumns mutation");

    const tableClassMatch = /<table class="([^"]+)"/.exec(html);

    assert.ok(tableClassMatch, "table element must be present");

    const tableClasses = tableClassMatch[1] ?? "";

    assert.equal(tableClasses.includes("hide-col-chnum"), false, "channelNumber column must be visible (no hide-col-chnum)");
    assert.equal(tableClasses.includes("hide-col-stationid"), false, "stationId column must be visible (no hide-col-stationid)");
    assert.equal(tableClasses.includes("hide-col-tags"), true, "tags column must be hidden (visibleColumns excludes it)");
  });

  test("clearing the service filter does not reset the previously-set sort or column-visibility preferences", async () => {

    /* The "filters and prefs are distinct" contract. A user clearing the filter should restore the full listing without losing their sort/columns choices -
     * those are display preferences, not filters. mutateEnabledServices([]) writes an empty array and updates the in-memory cache; it MUST not touch
     * channelSortField, channelSortDirection, or visibleColumns. Render after the clear and assert sort + columns survive.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices(["hulu"]);
    await mutateChannelDisplayPrefs({ channelSortDirection: "desc", channelSortField: "channelNumber", visibleColumns: ["channelNumber"] });
    await mutateEnabledServices([]);

    const html = generateChannelsPanel();

    // Service filter cleared: toolbar button reads "All Services" and the chips container is empty.
    assert.match(html, /<button [^>]*id="provider-filter-btn"[^>]*>[\s\S]*All Services/, "service filter button reads 'All Services' when filter is empty");
    assert.doesNotMatch(html, /<span class="provider-chip" data-tag=/, "no provider-chip elements when the filter is cleared");

    // Sort and columns survive.
    assert.match(html, /data-sort-field="channelNumber"[^>]*data-sort-dir="desc"/, "sort survives a clear-filter mutation");

    const tableClassMatch = /<table class="([^"]+)"/.exec(html);

    assert.ok(tableClassMatch, "table element must be present");

    const tableClasses = tableClassMatch[1] ?? "";

    assert.equal(tableClasses.includes("hide-col-stationid"), true, "stationId hide class survives a clear-filter mutation (column was hidden before)");
    assert.equal(tableClasses.includes("hide-col-chnum"), false, "channelNumber visibility survives a clear-filter mutation (column was visible before)");
  });

  test("hiding all optional columns does not crash the renderer; the table renders with every hide-col-* class", async () => {

    /* Edge case from the roadmap: visibleColumns = empty array means EVERY optional column is hidden. The renderer must produce a structurally valid table -
     * the hide classes are applied uniformly, the table element is still emitted, and the panel render returns successfully. A regression that assumed
     * visibleColumns had at least one entry (NPE on a forEach over a derived collection, or an invariant about column counts) would surface here.
     *
     * We use bootApp + a real GET / instead of a bare panel call to verify the FULL landing-page render path also survives. A panel that renders cleanly in
     * isolation but breaks when wrapped by the landing-page handler would slip past a panel-only assertion.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateChannelDisplayPrefs({ visibleColumns: [] });

    const { urlFor } = await bootApp(ctx);
    const response = await fetch(urlFor("/"));

    assert.equal(response.status, 200, "landing page must render 200 with visibleColumns=[]");

    const html = await response.text();

    // Every optional column must carry its hide class. We assert the four primary columns explicitly; the loop in the renderer applies the class uniformly so
    // covering a subset is sufficient.
    const tableClassMatch = /<table class="([^"]+)"/.exec(html);

    assert.ok(tableClassMatch, "table element must be present even when visibleColumns is empty");

    const tableClasses = tableClassMatch[1] ?? "";

    for(const cssClass of [ "hide-col-chnum", "hide-col-hdhr", "hide-col-stationid", "hide-col-profile", "hide-col-selector", "hide-col-tags" ]) {

      assert.ok(tableClasses.includes(cssClass), cssClass + " must be present when its column is hidden");
    }
  });
});
