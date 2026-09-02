/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * table.test.ts: Tests for the channel-table rendering helpers. Coverage focuses on the public surface that other modules consume - the column-definition
 * constants, the buildChannelTableState aggregator, and the buildChannelTablePatch builder. The HTML-generating helpers (generateChannelRowHtml,
 * generateChannelsPanel, generateServiceFilterToolbar, generateTagFilterContent, generateTagManagerBody) emit large strings that are exercised end-to-end by
 * the panel route handlers; we test only that they return non-empty strings and include the expected key fragments rather than diffing the full markup.
 */
import { OPTIONAL_COLUMNS, VALID_OPTIONAL_COLUMNS, buildChannelTablePatch, buildChannelTableState, generateServiceFilterToolbar, generateTagFilterContent,
  generateTagManagerBody } from "./table.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { getActiveTagVocabulary, getChannelEffectiveTags, getChannelListing, initializeUserChannels } from "../../../config/userChannels.ts";
import { loadHealthState, markDomainAuth, markDomainAuthRequired } from "../../../config/health.ts";
import { mkdtemp, rm } from "node:fs/promises";
import { CONFIG } from "../../../config/index.ts";
import assert from "node:assert/strict";
import { firstOf } from "../../../testing.helpers.ts";
import { initializeDataDir } from "../../../config/paths.ts";
import os from "node:os";
import path from "node:path";

describe("OPTIONAL_COLUMNS", () => {

  test("declares the six expected optional column field names", () => {

    // Lock the SSOT for which columns the user can show/hide. The set is consumed by VALID_OPTIONAL_COLUMNS and by the prefs validator, so a regression that
    // adds or removes a column field would surface here as a clear diff before the prefs endpoint quietly accepts an unsupported column.
    const fields = OPTIONAL_COLUMNS.map((c) => c.field).toSorted();

    assert.deepEqual(fields, [ "channelNumber", "channelSelector", "hdhrEnabled", "profile", "stationId", "tags" ]);
  });

  test("every column carries the structural fields the renderer expects (align, cssClass, field, label, width)", () => {

    for(const col of OPTIONAL_COLUMNS) {

      assert.equal(typeof col.align, "string", "align is a string");
      assert.equal(typeof col.cssClass, "string", "cssClass is a string");
      assert.equal(typeof col.field, "string", "field is a string");
      assert.equal(typeof col.label, "string", "label is a string");
      assert.equal(typeof col.width, "string", "width is a string");
    }
  });

  test("preserves a stable column order (number, hdhr, stationId, profile, selector, tags) so the table layout does not silently drift", () => {

    // The order is the rendered column order. Changing it would re-arrange every channel table in production.
    assert.deepEqual(
      OPTIONAL_COLUMNS.map((c) => c.field),
      [ "channelNumber", "hdhrEnabled", "stationId", "profile", "channelSelector", "tags" ]
    );
  });
});

describe("VALID_OPTIONAL_COLUMNS", () => {

  test("contains exactly the field names declared by OPTIONAL_COLUMNS", () => {

    // The validator set is derived from OPTIONAL_COLUMNS. Lock that derivation so a future hand-coded set divergence is impossible.
    assert.equal(VALID_OPTIONAL_COLUMNS.size, OPTIONAL_COLUMNS.length, "size matches column count");

    for(const col of OPTIONAL_COLUMNS) {

      assert.equal(VALID_OPTIONAL_COLUMNS.has(col.field), true, "validator includes " + col.field);
    }
  });

  test("rejects unknown column field names (negative test for the prefs endpoint validator)", () => {

    assert.equal(VALID_OPTIONAL_COLUMNS.has("definitely-not-a-real-column"), false);
    assert.equal(VALID_OPTIONAL_COLUMNS.has(""), false);
  });
});

describe("buildChannelTableState", () => {

  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-table-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("returns counts and scopeCounts derived from the current channel listing", () => {

    const state = buildChannelTableState();

    assert.ok(state.counts, "channel-table state should carry a counts object");
    assert.equal(typeof state.counts.disabled, "number");
    assert.equal(typeof state.counts.enabled, "number");
    assert.equal(typeof state.counts.predefined, "number");
    assert.equal(typeof state.counts.total, "number");
    assert.equal(typeof state.counts.user, "number");
    assert.ok(state.scopeCounts, "channel-table state should carry a scopeCounts object");
    assert.ok(state.scopeCounts.all, "scopeCounts.all should be present");
    assert.ok(state.scopeCounts.east, "scopeCounts.east should be present");
    assert.ok(state.scopeCounts.pacific, "scopeCounts.pacific should be present");
  });

  test("counts.total equals counts.predefined + counts.user (every entry is one or the other)", () => {

    const { counts } = buildChannelTableState();

    assert.equal(counts.total, counts.predefined + counts.user);
  });

  test("counts.enabled + counts.disabled equals counts.total (every entry is enabled or disabled)", () => {

    const { counts } = buildChannelTableState();

    assert.equal(counts.enabled + counts.disabled, counts.total);
  });

  test("accepts a pre-fetched listing parameter rather than recomputing", () => {

    // The parameter is documented as an optimization - tests can verify it doesn't affect the result by passing an empty listing.
    const state = buildChannelTableState([]);

    assert.equal(state.counts.total, 0);
    assert.equal(state.counts.enabled, 0);
    assert.equal(state.counts.disabled, 0);
  });
});

describe("buildChannelTablePatch", () => {

  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-patch-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("returns a patch shape with counts, hdhrCounts, rows, and scopeCounts for an empty affected-keys list", () => {

    const patch = buildChannelTablePatch([], []);

    assert.ok(patch.counts, "channel-table patch should carry a counts object");
    assert.ok(patch.hdhrCounts, "channel-table patch should carry an hdhrCounts object");
    assert.deepEqual(patch.rows, []);
    assert.ok(patch.scopeCounts, "channel-table patch should carry a scopeCounts object");
  });

  test("emits a 'remove' row entry for a key that does not exist in the listing", () => {

    const patch = buildChannelTablePatch(["definitely-not-a-real-key"], []);

    assert.equal(patch.rows.length, 1);

    const row = firstOf(patch.rows, "patch row");

    assert.equal(row.action, "remove");
    assert.equal(row.key, "definitely-not-a-real-key");
  });

  test("emits an 'update' row entry with rendered HTML for a real predefined key", () => {

    // 'abc' is a documented predefined channel and is in the listing by default.
    const patch = buildChannelTablePatch(["abc"], []);

    assert.equal(patch.rows.length, 1);

    const row = firstOf(patch.rows, "patch row");

    assert.equal(row.action, "update");
    assert.equal(row.key, "abc");
    assert.equal(typeof row.displayRow, "string", "displayRow HTML present");
  });
});

describe("login icon tri-state rendering", () => {

  /* The login icon on service-bound channel rows renders one of three domain auth states: verified (health-success, green), needs-sign-in (health-failed, red),
   * or unknown (no color class). We render through buildChannelTablePatch - the same primitive the health bridge uses for reactive row patches - and assert on
   * the login button's class and title. The predefined "abc" channel resolves to the abc.com auth domain, which is what the marks below key on.
   */
  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-loginicon-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    // Reload health state from the fresh (empty) data dir so domain auth residue from other test files cannot color the rows rendered here.
    await loadHealthState();

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced flush timer the mark calls below schedule.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(async () => {

    mock.timers.reset();
    await rm(dir, { force: true, recursive: true });
  });

  // Extracts the rendered display-row HTML for the predefined "abc" channel.
  function renderAbcRow(): string {

    const patch = buildChannelTablePatch(["abc"], []);
    const row = firstOf(patch.rows, "patch row");

    assert.equal(row.action, "update", "abc renders as an update row");
    assert.equal(typeof row.displayRow, "string", "displayRow HTML present");

    return row.displayRow ?? "";
  }

  test("renders the neutral state (no color class, 'not yet verified' title) when the domain is unknown", () => {

    const html = renderAbcRow();

    assert.match(html, /class="btn-icon btn-icon-login"/, "login button carries no color class");
    assert.match(html, /not yet verified/, "title reports the unverified state");
    assert.doesNotMatch(html, /btn-icon-login health-/, "no health color on the login button");
  });

  test("renders the verified state (health-success, 'verified' title) when the domain has success evidence", () => {

    markDomainAuth("abc.com");

    const html = renderAbcRow();

    assert.match(html, /class="btn-icon btn-icon-login health-success"/, "login button carries the verified green class");
    assert.match(html, / verified /, "title reports the verified state");
  });

  test("renders the needs-sign-in state (health-failed, actionable title) when the domain is flagged", () => {

    /* Regression assertion for the needsLogin branch in generateChannelRowHtml's login icon block: the scenario seeds a needsLogin entry for abc.com, so the
     * rendering enters the needsLogin arm - the appended health-failed class and the sign-in title are the mutations under test. The title must lead with the
     * click action (the icon IS the remedy) and carry the detection timestamp.
     */
    markDomainAuthRequired("abc.com");

    const html = renderAbcRow();

    assert.match(html, /class="btn-icon btn-icon-login health-failed"/, "login button carries the needs-sign-in red class");
    assert.match(html, /Click to open this channel in PrismCast&#39;s Chrome to sign in\./, "title leads with the sign-in action");
    assert.match(html, /needs sign-in \(detected /, "title reports the detection state and timestamp");
  });

  test("returns to the neutral state after the flag is cleared by a fresh health reload", async () => {

    /* Round-trip sanity: the rendering derives entirely from current health state, so reloading from an empty data dir (clearing the in-memory flag) must return
     * the row to neutral - there is no cached red state in the renderer.
     */
    markDomainAuthRequired("abc.com");
    assert.match(renderAbcRow(), /btn-icon-login health-failed/, "precondition: flagged renders red");

    mock.timers.reset();
    await loadHealthState();
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });

    assert.match(renderAbcRow(), /class="btn-icon btn-icon-login"/, "reloaded state renders neutral again");
  });
});

describe("generateServiceFilterToolbar", () => {

  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-toolbar-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("returns a non-empty HTML string for the service filter toolbar", () => {

    const html = generateServiceFilterToolbar();

    assert.equal(typeof html, "string");
    assert.ok(html.length > 0, "toolbar HTML must not be empty");
    assert.match(html, /provider-toolbar/, "contains the toolbar root class");
  });
});

describe("generateTagFilterContent", () => {

  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-tagfilter-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("returns a string (may be empty when no tags exist; non-empty when predefined tags are loaded)", () => {

    const html = generateTagFilterContent();

    assert.equal(typeof html, "string");

    // Predefined tags are loaded by initializeUserChannels, so the active vocabulary is non-empty and the helper emits checkbox markup.
    assert.match(html, /tag-filter-checkbox/, "contains the per-tag checkbox class");
  });
});

describe("generateTagManagerBody", () => {

  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-tagmgr-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("returns the tag-manager HTML with the input field, button, and list container", () => {

    const html = generateTagManagerBody();

    assert.match(html, /tag-manager/, "contains the manager root class");
    assert.match(html, /tag-manager-input/, "contains the new-tag input id");
    assert.match(html, /data-click-action="create-tag"/, "contains the create-tag action reference");
  });
});

describe("getTagCounts (via buildChannelTablePatch tagCounts)", () => {

  /* getTagCounts is not exported, so we exercise it through its sole consumer - buildChannelTablePatch, which surfaces the result as patch.tagCounts. These
   * counts drive the Quick Actions tag bulk-toggle tri-states, so the properties under test are: the tags-column visibility gate, the per-tag enabled +
   * service-available numerator, and the shared enabled + service-available denominator. The predefined channel definitions ship with tags, so the seeded
   * vocabulary and listing produce non-trivial counts.
   */
  let dir: string;
  let originalVisibleColumns: string[];

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-tagcounts-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    // Snapshot the visible-column preference so per-test mutation of the tags-column gate cannot leak into other suites that share the CONFIG singleton.
    originalVisibleColumns = CONFIG.channels.visibleColumns;
  });

  afterEach(async () => {

    CONFIG.channels.visibleColumns = originalVisibleColumns;
    await rm(dir, { force: true, recursive: true });
  });

  test("omits tagCounts when the tags column is not visible", () => {

    // The tag bulk-toggle counts only exist when the tags column is shown. With the column hidden the builder must return undefined so the client skips the
    // tag-toggle refresh entirely rather than rendering counts for controls that are not on the page.
    CONFIG.channels.visibleColumns = [];

    const patch = buildChannelTablePatch([], []);

    assert.equal(patch.tagCounts, undefined);
  });

  test("reports every active vocabulary tag, each denominated by the enabled + service-available channel count", () => {

    CONFIG.channels.visibleColumns = ["tags"];

    const patch = buildChannelTablePatch([], []);
    const tagCounts = patch.tagCounts;

    assert.ok(tagCounts, "tagCounts must be present when the tags column is visible and the vocabulary is non-empty");

    // The keys are exactly the active tag vocabulary - every tag the user can toggle, and no others.
    const vocabulary = getActiveTagVocabulary();

    assert.ok(vocabulary.length > 0, "precondition: predefined tags seed a non-empty vocabulary");
    assert.deepEqual(Object.keys(tagCounts).toSorted(), [...vocabulary].toSorted());

    // The per-tag denominator is the number of enabled, service-available channels. buildChannelTableState derives that same figure independently as
    // counts.enabled, so a regression that changes the denominator (for example, dropping the availableByService filter) breaks this cross-check.
    const enabledCount = buildChannelTableState().counts.enabled;

    // Iterate the entries rather than index the record by tag: Object.entries yields the concrete value type, so each denominator/count check reads a defined
    // entry without a per-access undefined narrow. The key-set equality asserted above guarantees these entries are exactly the active vocabulary.
    for(const [ tag, entry ] of Object.entries(tagCounts)) {

      assert.equal(entry.total, enabledCount, "denominator for " + tag + " equals the enabled channel count");
      assert.ok(entry.count >= 0, "count for " + tag + " is non-negative");
      assert.ok(entry.count <= entry.total, "count for " + tag + " does not exceed its denominator");
    }

    // The predefined channels ship with tags, so at least one tag must have a positive count - a guard against a vacuous all-zero result.
    assert.ok(Object.values(tagCounts).some((entry) => entry.count > 0), "at least one predefined tag has a positive channel count");
  });

  test("counts only enabled, service-available channels carrying each tag in their effective tags", () => {

    CONFIG.channels.visibleColumns = ["tags"];

    const patch = buildChannelTablePatch([], []);
    const tagCounts = patch.tagCounts;

    assert.ok(tagCounts, "tagCounts must be present");

    // Recompute the expected per-tag counts from the same inputs the production builder consumes: the current listing filtered to enabled + service-available
    // entries, and each entry's vocabulary-filtered effective tags. This asserts the exact counting contract the Quick Actions toggles rely on.
    const vocabulary = getActiveTagVocabulary();
    const listing = getChannelListing();
    const expected: Record<string, { count: number; total: number }> = {};
    const counts = new Map<string, number>();
    let total = 0;

    for(const tag of vocabulary) {

      counts.set(tag, 0);
    }

    for(const entry of listing) {

      if(!entry.enabled || !entry.availableByService) {

        continue;
      }

      total++;

      for(const tag of getChannelEffectiveTags(entry.channel)) {

        const current = counts.get(tag);

        if(current !== undefined) {

          counts.set(tag, current + 1);
        }
      }
    }

    for(const tag of vocabulary) {

      expected[tag] = { count: counts.get(tag) ?? 0, total };
    }

    assert.deepEqual(tagCounts, expected);
  });
});
