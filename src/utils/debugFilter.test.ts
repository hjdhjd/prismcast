/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * debugFilter.test.ts: Unit tests for the debug filter primitives in debugFilter.ts. The module holds module-scope mutable state (includeSet, excludeSet,
 * wildcardEnabled, anyEnabled) that initDebugFilter() resets on each call. Each describe block resets the filter via afterEach (and the isCategoryEnabled
 * block additionally via beforeEach), so tests do not inherit state from one another. The DEBUG_CATEGORIES registry is also covered - it is an SSOT
 * consumed by the /debug UI, so changes to its shape would surface here.
 */
import { DEBUG_CATEGORIES, getCurrentPattern, initDebugFilter, isAnyDebugEnabled, isCategoryEnabled } from "./debugFilter.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";

describe("initDebugFilter", () => {

  afterEach(() => {

    // Reset the module-scope state so subsequent test files do not inherit a configured filter from this one.
    initDebugFilter("");
  });

  test("an empty string disables every category (anyEnabled stays false)", () => {

    initDebugFilter("");

    assert.equal(isAnyDebugEnabled(), false, "no categories enabled after empty pattern");
    assert.equal(isCategoryEnabled("anything:at:all"), false, "no category passes when filter is off");
  });

  test("a wildcard pattern enables every category", () => {

    initDebugFilter("*");

    assert.equal(isAnyDebugEnabled(), true);
    assert.equal(isCategoryEnabled("tuning:hulu"), true);
    assert.equal(isCategoryEnabled("totally:unknown:category"), true, "wildcard matches even unregistered categories");
  });

  test("an exact category match enables only that category", () => {

    initDebugFilter("tuning:hulu");

    assert.equal(isCategoryEnabled("tuning:hulu"), true, "exact match");
    assert.equal(isCategoryEnabled("tuning:sling"), false, "sibling category not enabled");
    assert.equal(isCategoryEnabled("tuning"), false, "parent category not enabled by leaf-only pattern");
  });

  test("a parent prefix enables every sub-category beneath it", () => {

    initDebugFilter("recovery");

    assert.equal(isCategoryEnabled("recovery"), true, "exact-match on the parent itself");
    assert.equal(isCategoryEnabled("recovery:tab"), true, "sub-category via prefix match");
    assert.equal(isCategoryEnabled("recovery:nav"), true, "another sub-category");
    assert.equal(isCategoryEnabled("recoveryverbose"), false, "must match on a colon boundary, not arbitrary prefix");
  });

  test("an exclude pattern (-category) blocks the category even with wildcard active", () => {

    initDebugFilter("*,-streaming:ffmpeg");

    assert.equal(isCategoryEnabled("streaming:hls"), true, "wildcard still allows other categories");
    assert.equal(isCategoryEnabled("streaming:ffmpeg"), false, "explicit exclude blocks even under wildcard");
  });

  test("an exclude pattern blocks the parent and every sub-category", () => {

    // Negative test: -streaming should kill streaming AND all streaming:* sub-categories.
    initDebugFilter("*,-streaming");

    assert.equal(isCategoryEnabled("streaming"), false);
    assert.equal(isCategoryEnabled("streaming:ffmpeg"), false);
    assert.equal(isCategoryEnabled("streaming:hls"), false);
    assert.equal(isCategoryEnabled("recovery:tab"), true, "non-streaming categories still allowed by wildcard");
  });

  test("trims whitespace and ignores empty entries", () => {

    initDebugFilter("  tuning:hulu , , recovery:tab ,");

    assert.equal(isCategoryEnabled("tuning:hulu"), true, "spaces around the entry are stripped");
    assert.equal(isCategoryEnabled("recovery:tab"), true, "second entry survived the empty-entry filtering");
  });

  test("calling init twice replaces the previous configuration entirely", () => {

    initDebugFilter("tuning:hulu");
    assert.equal(isCategoryEnabled("tuning:hulu"), true, "first config takes effect");

    initDebugFilter("recovery:tab");
    assert.equal(isCategoryEnabled("tuning:hulu"), false, "old include was cleared");
    assert.equal(isCategoryEnabled("recovery:tab"), true, "new include is active");
  });

  test("a pattern with only whitespace and commas leaves the filter disabled", () => {

    // Boundary: the split/trim/filter yields zero tokens, so wildcard is false and the parsed sets are empty - initDebugFilter sets anyEnabled to false.
    initDebugFilter("  ,  ,  ");

    assert.equal(isAnyDebugEnabled(), false);
    assert.equal(isCategoryEnabled("tuning:hulu"), false);
  });
});

describe("isCategoryEnabled", () => {

  beforeEach(() => {

    initDebugFilter("");
  });

  afterEach(() => {

    initDebugFilter("");
  });

  test("returns false fast when no debug is configured (anyEnabled false-path)", () => {

    assert.equal(isCategoryEnabled("any:category"), false);
  });

  test("excludes always win over wildcard", () => {

    initDebugFilter("*,-tuning:hulu");

    assert.equal(isCategoryEnabled("tuning:hulu"), false);
    assert.equal(isCategoryEnabled("tuning:sling"), true);
  });

  test("excludes win even over a more-specific include (boundary)", () => {

    // Locking the precedence: even if the user explicitly includes "tuning:hulu" and excludes "tuning", the exclude takes the parent prefix and blocks.
    initDebugFilter("tuning:hulu,-tuning");

    assert.equal(isCategoryEnabled("tuning:hulu"), false, "exclude on parent kills include on child");
  });

  test("includes do NOT match on arbitrary prefix - only on category equality or colon-boundary", () => {

    // Negative test for the matchesAny boundary: "tune" must not match "tuner" via prefix - only "tune" or "tune:..." would match.
    initDebugFilter("tune");

    assert.equal(isCategoryEnabled("tune"), true, "exact match");
    assert.equal(isCategoryEnabled("tune:hulu"), true, "colon-boundary match");
    assert.equal(isCategoryEnabled("tuner"), false, "arbitrary prefix must not match");
  });
});

describe("isAnyDebugEnabled", () => {

  afterEach(() => {

    initDebugFilter("");
  });

  test("returns false before any init is called", () => {

    initDebugFilter("");
    assert.equal(isAnyDebugEnabled(), false);
  });

  test("returns true after wildcard init", () => {

    initDebugFilter("*");
    assert.equal(isAnyDebugEnabled(), true);
  });

  test("returns true after a single-category init", () => {

    initDebugFilter("recovery:tab");
    assert.equal(isAnyDebugEnabled(), true);
  });

  test("returns true after an exclude-only init (counts as configured)", () => {

    // Boundary: even a config that consists of only excludes counts as "any debug enabled" because the user has set a pattern. The exclude has nothing to act
    // on without a wildcard or include alongside it, but the function returns true for the gate.
    initDebugFilter("-streaming:ffmpeg");

    assert.equal(isAnyDebugEnabled(), true);
  });
});

describe("getCurrentPattern", () => {

  afterEach(() => {

    initDebugFilter("");
  });

  test("returns the empty string when nothing is configured", () => {

    initDebugFilter("");
    assert.equal(getCurrentPattern(), "");
  });

  test("includes the wildcard token when wildcard is enabled", () => {

    initDebugFilter("*");
    assert.match(getCurrentPattern(), /^\*/, "wildcard appears at the front");
  });

  test("prefixes excludes with a hyphen", () => {

    initDebugFilter("*,-streaming:ffmpeg,-recovery:tab");
    const pattern = getCurrentPattern();

    assert.match(pattern, /-streaming:ffmpeg/);
    assert.match(pattern, /-recovery:tab/);
  });

  test("emits include patterns as bare names", () => {

    initDebugFilter("tuning:hulu,recovery:tab");
    const pattern = getCurrentPattern();

    assert.match(pattern, /tuning:hulu/);
    assert.match(pattern, /recovery:tab/);
    assert.doesNotMatch(pattern, /-tuning/, "includes are not prefixed with a hyphen");
  });

  test("joins parts with comma", () => {

    initDebugFilter("a,b,c");
    const pattern = getCurrentPattern();

    assert.equal(pattern.split(",").length, 3, "three include entries comma-joined");
  });

  test("emits parts in the documented order: wildcard -> excludes (insertion order) -> includes (insertion order)", () => {

    // The reconstruction order is structural: wildcard first, then excludes, then includes. Both Set iteration order and the implementation's loop preserve
    // insertion order. We pin the exact reconstructed string for a known input so a future reorder (or accidental swap of the loops) surfaces here.
    initDebugFilter("*,-streaming:ffmpeg,tuning:hulu");

    assert.equal(getCurrentPattern(), "*,-streaming:ffmpeg,tuning:hulu",
      "reconstructed string preserves wildcard-first, then excludes, then includes ordering");
  });

  test("preserves multi-entry insertion order within each segment", () => {

    // Boundary: two excludes and two includes should land in their original relative order. If a refactor replaced the Sets with a structure that doesn't
    // guarantee insertion order, this test would fail.
    initDebugFilter("*,-recovery:nav,-streaming:hls,tuning:fox,recovery:tab");

    assert.equal(getCurrentPattern(), "*,-recovery:nav,-streaming:hls,tuning:fox,recovery:tab",
      "exclude order and include order both preserved");
  });
});

describe("DEBUG_CATEGORIES", () => {

  test("is a non-empty readonly array", () => {

    // The /debug UI consumes this registry to render checkboxes. An empty array would silently disable category configuration in the UI.
    assert.ok(DEBUG_CATEGORIES.length > 0, "registry has at least one entry");
  });

  test("every entry has both a category and a description", () => {

    // Locking the shape of each entry. The UI relies on every field - missing any one would break rendering.
    for(const entry of DEBUG_CATEGORIES) {

      assert.equal(typeof entry.category, "string", "category is a string");
      assert.ok(entry.category.length > 0, "category is non-empty: " + JSON.stringify(entry));
      assert.equal(typeof entry.description, "string", "description is a string");
      assert.ok(entry.description.length > 0, "description is non-empty: " + entry.category);
    }
  });

  test("every entry uses a colon-namespaced category or a single-token category", () => {

    // Locking the namespace conventions. Either "category" or "category:sub" is allowed; arbitrary characters are not.
    const validCategoryPattern = /^[a-zA-Z]+(:[a-zA-Z]+)*$/;

    for(const entry of DEBUG_CATEGORIES) {

      assert.match(entry.category, validCategoryPattern, "category " + entry.category + " uses only letters and colons");
    }
  });

  test("entries are sorted alphabetically by category (registry contract)", () => {

    // The module's design comment states "Sorted alphabetically by category" - the /debug UI groups by parent prefix and renders a deterministic checkbox list
    // in registry order. Lock the contract so a future out-of-order entry surfaces as a test failure rather than a UI surprise.
    const categories = DEBUG_CATEGORIES.map((entry) => entry.category);
    const sorted = categories.toSorted();

    assert.deepEqual(categories, sorted, "registry must be sorted alphabetically by category");
  });

  test("category names are unique (no duplicate entries)", () => {

    // Negative test: a duplicate would silently render two checkboxes for the same key.
    const seen = new Set<string>();

    for(const entry of DEBUG_CATEGORIES) {

      assert.equal(seen.has(entry.category), false, "duplicate category: " + entry.category);
      seen.add(entry.category);
    }
  });
});
