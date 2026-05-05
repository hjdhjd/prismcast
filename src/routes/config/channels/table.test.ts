/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * table.test.ts: Tests for the channel-table rendering helpers. Coverage focuses on the public surface that other modules consume - the column-definition
 * constants, the buildChannelTableState aggregator, and the buildChannelTablePatch builder. The HTML-generating helpers (generateChannelRowHtml,
 * generateChannelsPanel, generateServiceFilterToolbar, generateTagFilterContent, generateTagManagerBody) emit large strings that are exercised end-to-end by
 * the panel route handlers; we test only that they return non-empty strings and include the expected key fragments rather than diffing the full markup.
 */
import { OPTIONAL_COLUMNS, VALID_OPTIONAL_COLUMNS, buildChannelTablePatch, buildChannelTableState, generateServiceFilterToolbar, generateTagFilterContent,
  generateTagManagerBody } from "./table.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import assert from "node:assert/strict";
import { firstOf } from "../../../testing.helpers.ts";
import { initializeDataDir } from "../../../config/paths.ts";
import { initializeUserChannels } from "../../../config/userChannels.ts";
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

    assert.ok(state.counts);
    assert.equal(typeof state.counts.disabled, "number");
    assert.equal(typeof state.counts.enabled, "number");
    assert.equal(typeof state.counts.predefined, "number");
    assert.equal(typeof state.counts.total, "number");
    assert.equal(typeof state.counts.user, "number");
    assert.ok(state.scopeCounts);
    assert.ok(state.scopeCounts.all);
    assert.ok(state.scopeCounts.east);
    assert.ok(state.scopeCounts.pacific);
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

    assert.ok(patch.counts);
    assert.ok(patch.hdhrCounts);
    assert.deepEqual(patch.rows, []);
    assert.ok(patch.scopeCounts);
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
    assert.match(html, /createTag\(\)/, "contains the create handler reference");
  });
});
