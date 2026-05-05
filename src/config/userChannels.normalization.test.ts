/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.normalization.test.ts: Tests for the write-time normalizer and its supporting helpers - normalizeChannelDeltas, normalizeEntryAgainstBase,
 * stripNulls, buildResolvedCanonicals.
 *
 * These tests verify that the normalizer strips redundant fields against the correct base, preserves genuine overrides, drops empty deltas that carry no
 * information, and handles edge cases (dangling canonicals, null-for-clear semantics). Idempotency and roundtrip invariants guard against subtle regressions.
 */
import type { CanonicalChannel, Channel, StoredChannel, StoredChannelMap } from "../types/index.ts";
import { describe, test } from "node:test";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import { __internalForTests } from "./userChannels.ts";
import assert from "node:assert/strict";

const { buildResolvedCanonicals, normalizeChannelDeltas, normalizeEntryAgainstBase, overlayDelta, resolveVariant, stripNulls } = __internalForTests;

describe("stripNulls", () => {

  test("removes null-valued fields and preserves everything else", () => {

    const stored = { channelNumber: null, name: "Test", stationId: "123", tags: null, url: "https://example.com" } as StoredChannel;
    const result = stripNulls(stored);

    assert.deepEqual(result, { name: "Test", stationId: "123", url: "https://example.com" });
  });

  test("returns an empty object when every field is null", () => {

    assert.deepEqual(stripNulls({ channelNumber: null, name: null }), {});
  });

  test("leaves empty-but-non-null fields (empty string, zero) in place", () => {

    const stored = { channelNumber: 0, name: "" } as StoredChannel;
    const result = stripNulls(stored);

    assert.deepEqual(result, stored);
  });
});

describe("normalizeEntryAgainstBase", () => {

  test("returns null when every field matches the base", () => {

    const base = { channelSelector: "ABC", name: "ABC", url: "https://abc.com" } as Channel;
    const stored = { channelSelector: "ABC", name: "ABC", url: "https://abc.com" } as StoredChannel;

    assert.equal(normalizeEntryAgainstBase(stored, base), null);
  });

  test("strips fields that match the base and preserves fields that differ", () => {

    const base = { channelSelector: "ABC", name: "ABC", url: "https://abc.com" } as Channel;
    const stored = { channelSelector: "ABC", name: "ABC Custom", url: "https://abc.com" } as StoredChannel;
    const result = normalizeEntryAgainstBase(stored, base);

    assert.deepEqual(result, { name: "ABC Custom" });
  });

  test("preserves a value that exists on the stored entry but not on the base (e.g. per-variant stationId)", () => {

    // This is the jsfullam case: the base (abc canonical) has no stationId, but the user's variant (abc-kabc) sets one. The delta must preserve it.
    const base = { name: "ABC", url: "https://abc.com" } as Channel;
    const stored = { stationId: "57342" } as StoredChannel;
    const result = normalizeEntryAgainstBase(stored, base);

    assert.deepEqual(result, { stationId: "57342" });
  });

  test("drops null deltas when the base has no such field (clearing a nonexistent field is a no-op)", () => {

    const base = { name: "ABC", url: "https://abc.com" } as Channel;
    const stored = { stationId: null } as StoredChannel;

    assert.equal(normalizeEntryAgainstBase(stored, base), null);
  });

  test("preserves null deltas when the base has the field (explicit clear survives)", () => {

    const base = { name: "ABC", stationId: "12345", url: "https://abc.com" } as Channel;
    const stored = { stationId: null } as StoredChannel;
    const result = normalizeEntryAgainstBase(stored, base);

    assert.deepEqual(result, { stationId: null });
  });

  test("tags array comparison is order-independent (case-sensitive on final values)", () => {

    // sortTags is case-insensitive on sort order but preserves each tag's authored casing. JSON.stringify equality compares the sorted arrays verbatim, so tags
    // match only when both the content and the case agree. Order, however, does not matter - a user who drops tags in a different sequence than the predefined
    // author still gets their delta stripped when the effective tag set is identical.
    const base = { name: "ABC", tags: [ "Local", "News" ], url: "https://abc.com" } as Channel;
    const stored = { tags: [ "News", "Local" ] } as StoredChannel;

    assert.equal(normalizeEntryAgainstBase(stored, base), null, "identical tags in different order should normalize to no delta");
  });

  test("tags array preserved when genuinely different from base", () => {

    const base = { name: "ABC", tags: ["Local"], url: "https://abc.com" } as Channel;
    const stored = { tags: [ "Local", "Sports" ] } as StoredChannel;
    const result = normalizeEntryAgainstBase(stored, base);

    assert.deepEqual(result, { tags: [ "Local", "Sports" ] });
  });

  test("non-delta fields (canonicalKey) are stripped when the base declares the same value", () => {

    const base = { canonicalKey: "abc", channelSelector: "ABC", url: "https://hulu.com/live" } as Channel;
    const stored = { canonicalKey: "abc" } as StoredChannel;

    assert.equal(normalizeEntryAgainstBase(stored, base), null, "empty delta over predefined variant collapses to nothing");
  });

  test("non-delta fields preserved when stored value differs from base", () => {

    const base = { name: "ABC", url: "https://abc.com" } as Channel;
    const stored = { canonicalKey: "abc" } as StoredChannel;
    const result = normalizeEntryAgainstBase(stored, base);

    assert.deepEqual(result, { canonicalKey: "abc" }, "canonicalKey is a meaningful addition when base does not have it");
  });

  test("skips undefined delta fields (they are no-ops)", () => {

    const base = { name: "ABC", url: "https://abc.com" } as Channel;
    const stored = { name: undefined, stationId: "99999" } as StoredChannel;
    const result = normalizeEntryAgainstBase(stored, base);

    assert.deepEqual(result, { stationId: "99999" });
  });
});

describe("normalizeChannelDeltas", () => {

  test("strips a predefined canonical override that matches the predefined exactly", () => {

    const abc = PREDEFINED_CHANNELS["abc"] as CanonicalChannel;
    const channels: StoredChannelMap = { abc: { name: abc.name, url: abc.url } };
    const result = normalizeChannelDeltas(channels);

    assert.equal("abc" in result, false, "an empty override should be dropped entirely");
  });

  test("preserves a predefined canonical override with differing fields", () => {

    const channels: StoredChannelMap = { abc: { name: "ABC Custom" } };
    const result = normalizeChannelDeltas(channels);

    assert.deepEqual(result["abc"], { name: "ABC Custom" });
  });

  test("preserves a user standalone with nulls stripped", () => {

    const channels: StoredChannelMap = { mychannel: { channelNumber: null, name: "My Channel", url: "https://example.com" } };
    const result = normalizeChannelDeltas(channels);

    assert.deepEqual(result["mychannel"], { name: "My Channel", url: "https://example.com" });
  });

  /* The original .mjs jsfullam-scenario test (variant carrying identity overrides) is superseded by the standalone-path test "abc-kabc with divergent identity
   * stays a standalone and resolves as-is" in userChannels.migration.test.ts, which exercises the equivalent WHAT (per-affiliate identity preserved through
   * normalize → resolve) under the current architecture where variants cannot carry identity.
   */

  test("dangling canonical (user variant pointing at missing canonical) preserves the entry rather than silently dropping it", () => {

    /* The WHAT this protects: a user variant whose canonicalKey points at a canonical that does not exist (typo, renamed canonical, removed predefined) must
     * not be silently deleted by the normalizer - the user keeps their binding data. The original test additionally asserted identity survived; under the
     * current architecture identity is unconditionally stripped from variant-shaped entries by filterToDeltaSurface, so we assert the more focused contract:
     * canonicalKey and binding fields survive, and the entry is not removed from the map.
     */
    const channels: StoredChannelMap = {

      "nonexistent-local": {

        canonicalKey: "nonexistent-canonical",
        channelSelector: "FAKE",
        url: "https://example.com"
      }
    };
    const result = normalizeChannelDeltas(channels);
    const dangling = result["nonexistent-local"];

    assert.ok(dangling, "dangling variant must not be silently dropped");
    assert.equal((dangling as { canonicalKey?: string }).canonicalKey, "nonexistent-canonical", "canonicalKey survives");
    assert.equal(dangling.url, "https://example.com", "binding url survives");
    assert.equal(dangling.channelSelector, "FAKE", "binding channelSelector survives");
  });

  test("predefined variant override (user edited a predefined variant) normalizes against the resolved variant view", () => {

    // abc-hulu is a predefined variant inheriting identity from abc. If the user's stored entry has name="ABC" (matching canonical identity), it should strip
    // entirely - the user has not actually overridden anything.
    const channels: StoredChannelMap = { "abc-hulu": { name: "ABC" } };
    const result = normalizeChannelDeltas(channels);

    assert.equal("abc-hulu" in result, false, "empty delta against predefined variant should collapse to nothing");
  });

  /* User identity edits on a predefined variant route to the canonical entry via handlePredefinedEdit in routes/config/channels/endpoints/crud.ts. The
   * equivalent WHAT (user identity edit on a variant via the UI ends up visible after resolution) is exercised by the route-layer test "identity-vs-binding
   * routing (variant active): identity goes to canonical, binding goes to the variant entry" in crud.test.ts.
   */
});

describe("buildResolvedCanonicals", () => {

  test("resolves predefined canonicals without user overrides as the predefined reference itself", () => {

    const result = buildResolvedCanonicals({});

    assert.equal(result["abc"], PREDEFINED_CHANNELS["abc"], "without an override, the reference is preserved (so isUserOverride returns false)");
  });

  test("resolves a predefined canonical with a user override as a fresh object with overridden fields", () => {

    const stored: StoredChannelMap = { abc: { name: "ABC Custom" } };
    const result = buildResolvedCanonicals(stored);

    assert.equal(result["abc"]?.name, "ABC Custom");
    assert.notEqual(result["abc"], PREDEFINED_CHANNELS["abc"], "override should produce a fresh reference");
  });

  test("skips variants (they are resolved in Pass 2 by the caller)", () => {

    // A user variant's key should not appear in the resolved canonical map; it is a variant, not a canonical.
    const stored: StoredChannelMap = { "abc-kabc": { canonicalKey: "abc", channelSelector: "KABC", url: "https://stream.directv.com" } };
    const result = buildResolvedCanonicals(stored);

    assert.equal("abc-kabc" in result, false, "variant key should not land in the canonical map");
  });

  test("resolves a user standalone as a defensive copy", () => {

    const stored: StoredChannelMap = { mychannel: { name: "My Channel", url: "https://example.com" } };
    const result = buildResolvedCanonicals(stored);

    assert.deepEqual(result["mychannel"], stored["mychannel"]);
    assert.notEqual(result["mychannel"], stored["mychannel"], "defensive copy so mutations don't leak");
  });
});

describe("invariants", () => {

  test("normalizeChannelDeltas is idempotent", () => {

    const channels: StoredChannelMap = {

      abc: { name: "ABC Custom" },
      "abc-kabc": {

        canonicalKey: "abc",
        channelNumber: 7,
        channelSelector: "KABC",
        stationId: "57342",
        url: "https://stream.directv.com"
      },
      mychannel: { name: "My Channel", tags: ["Custom"], url: "https://example.com" }
    };
    const once = normalizeChannelDeltas(channels);
    const twice = normalizeChannelDeltas(once);

    assert.deepEqual(once, twice, "normalizing a normalized map must produce the same map");
  });

  test("normalization preserves the resolved view for variant binding overrides (normalize then resolve equals direct resolve)", () => {

    /* The WHAT this protects: normalization is a storage optimization, not a semantic change. For an input that survives normalization - here, a user binding
     * override on a predefined variant - the resolved view must be identical before and after. This is the regression guard against accidentally turning the
     * normalizer into a destructive transform. The original .mjs version used a variant-with-identity input (now invalid); we shift to a binding-override input
     * which is the case the current architecture actually supports.
     */
    const storedBefore: StoredChannelMap = {

      "abc-hulu": {

        canonicalKey: "abc",
        channelSelector: "ABC-CUSTOM",
        url: "https://stream.directv.com"
      }
    };
    const normalized = normalizeChannelDeltas(storedBefore);

    const canonical = PREDEFINED_CHANNELS["abc"] as CanonicalChannel;
    const predefinedVariant = PREDEFINED_CHANNELS["abc-hulu"];
    const resolvedBefore = resolveVariant(canonical, predefinedVariant, storedBefore["abc-hulu"]);
    const resolvedAfter = resolveVariant(canonical, predefinedVariant, normalized["abc-hulu"]);

    assert.equal(resolvedBefore.url, resolvedAfter.url, "resolved URL is identical pre- and post-normalize");
    assert.equal(resolvedBefore.channelSelector, resolvedAfter.channelSelector, "resolved channelSelector is identical pre- and post-normalize");
    assert.equal(resolvedBefore.name, resolvedAfter.name, "resolved name is identical pre- and post-normalize");
    assert.equal(resolvedBefore.canonicalKey, resolvedAfter.canonicalKey, "resolved canonicalKey is identical pre- and post-normalize");
  });

  test("overlayDelta composition is associative for independent field sets", () => {

    // Applying delta1 then delta2 produces the same result as applying their union, provided they touch disjoint fields.
    const base = { name: "ABC", url: "https://abc.com" } as CanonicalChannel;
    const delta1 = { stationId: "99999" } as StoredChannel;
    const delta2 = { channelNumber: 7 } as StoredChannel;

    const sequential = overlayDelta(overlayDelta(base, delta1), delta2);
    const combined = overlayDelta(base, { ...delta1, ...delta2 });

    assert.deepEqual(sequential, combined);
  });
});
