/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.normalization.test.ts: Tests for the write-time normalizer and its supporting helpers - normalizeChannelDeltas, normalizeEntryAgainstBase,
 * stripNulls, buildResolvedCanonicals.
 *
 * These tests verify that the normalizer strips redundant fields against the correct base, preserves genuine overrides, drops empty deltas that carry no
 * information, and handles edge cases (dangling canonicals, null-for-clear semantics). Idempotency and roundtrip invariants guard against subtle regressions.
 */
import type { CanonicalChannel, Channel, ChannelDelta, StoredChannel, StoredChannelMap } from "../types/index.ts";
import { __internalForTests, intersectBindingDeltas, mergeVariantBinding, replaceVariantBinding } from "./userChannels.ts";
import { describe, test } from "node:test";
import { makeChannelsData, normalize } from "./userChannels.helpers.ts";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
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

    // sortTags is case-insensitive on sort order but preserves each tag's authored casing. isDeepStrictEqual then compares the sorted arrays element by element,
    // so tags match only when both the content and the case agree. Order, however, does not matter - a user who drops tags in a different sequence than the
    // predefined author still gets their delta stripped when the effective tag set is identical.
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

/* The three binding-delta helpers (replaceVariantBinding, mergeVariantBinding, intersectBindingDeltas) are public exports consumed by the producer
 * (handlePredefinedEdit) and the storage normalizer (normalizeChannelDeltas). Their semantics are subtle and intentionally distinct - replace vs merge
 * precedence, key-set intersection scoped to the binding partition. The integration tests above exercise them indirectly through full pipeline runs;
 * these focused unit tests pin each function's contract independently so a future refactor that accidentally swaps semantics fails focused, not distant.
 */
describe("replaceVariantBinding", () => {

  test("returns null when both existing and delta are empty (nothing to persist)", () => {

    assert.equal(replaceVariantBinding(undefined, {}), null);
  });

  test("returns null when existing has only binding fields and delta is empty (binding stripped, nothing left)", () => {

    // existing carries only binding fields; the strip-then-apply leaves an empty object, which collapses to null.
    const existing: StoredChannel = { channelSelector: "ABC", url: "https://www.hulu.com/live" };

    assert.equal(replaceVariantBinding(existing, {}), null);
  });

  test("preserves canonicalKey from existing when delta is empty (canonicalKey is non-binding)", () => {

    // canonicalKey is the variant discriminator - not a binding field. The strip leaves it; an empty delta adds nothing; the result has only canonicalKey.
    const existing: StoredChannel = { canonicalKey: "abc", channelSelector: "ABC", url: "https://www.hulu.com/live" };

    assert.deepEqual(replaceVariantBinding(existing, {}), { canonicalKey: "abc" });
  });

  test("strips ALL prior binding fields, then applies the new delta (replace, not merge)", () => {

    // existing has channelSelector + url + profile (all binding). Delta sets only channelSelector. The result has channelSelector from delta and NOTHING else
    // from existing's binding (url and profile are gone). canonicalKey is preserved (non-binding).
    const existing: StoredChannel = { canonicalKey: "abc", channelSelector: "OLD", profile: "huluLive", url: "https://www.hulu.com/live" };
    const result = replaceVariantBinding(existing, { channelSelector: "NEW" });

    assert.deepEqual(result, { canonicalKey: "abc", channelSelector: "NEW" });
  });

  test("returns the delta as-is when existing is undefined", () => {

    const result = replaceVariantBinding(undefined, { channelSelector: "ABC", url: "https://www.hulu.com/live" });

    assert.deepEqual(result, { channelSelector: "ABC", url: "https://www.hulu.com/live" });
  });
});

describe("mergeVariantBinding", () => {

  test("returns delta as-is when existing is undefined (no fields to preserve)", () => {

    const result = mergeVariantBinding(undefined, { url: "https://www.hulu.com/live/east" });

    assert.deepEqual(result, { url: "https://www.hulu.com/live/east" });
  });

  test("existing fields take precedence on conflicting keys (preserve-existing semantics)", () => {

    // Both have channelSelector. Existing wins.
    const existing: StoredChannel = { canonicalKey: "abc", channelSelector: "MyCustomABC" };
    const result = mergeVariantBinding(existing, { channelSelector: "WouldClobber", url: "https://www.hulu.com/live/east" });

    assert.deepEqual(result, { canonicalKey: "abc", channelSelector: "MyCustomABC", url: "https://www.hulu.com/live/east" });
  });

  test("delta fills gaps the existing entry doesn't declare (merge, not replace)", () => {

    // existing has channelSelector but no url; delta supplies url. Both survive in the result.
    const existing: StoredChannel = { canonicalKey: "abc", channelSelector: "MyCustomABC" };
    const result = mergeVariantBinding(existing, { url: "https://www.hulu.com/live/east" });

    assert.deepEqual(result, { canonicalKey: "abc", channelSelector: "MyCustomABC", url: "https://www.hulu.com/live/east" });
  });

  test("preserves canonicalKey on existing alongside delta-supplied binding", () => {

    // canonicalKey is non-binding; merge preserves it via the existing-wins spread alongside any delta-supplied binding fields.
    const existing: StoredChannel = { canonicalKey: "abc", url: "https://www.hulu.com/live" };
    const result = mergeVariantBinding(existing, { channelSelector: "ABC" });

    assert.deepEqual(result, { canonicalKey: "abc", channelSelector: "ABC", url: "https://www.hulu.com/live" });
  });
});

describe("intersectBindingDeltas", () => {

  test("returns binding fields present in both inputs, using primary's values", () => {

    const primary: ChannelDelta = { channelSelector: "FROM_PRIMARY", url: "https://primary.example/" };
    const criterion: ChannelDelta = { channelSelector: "criterion-value-ignored", url: "criterion-url-ignored" };
    const result = intersectBindingDeltas(primary, criterion);

    assert.deepEqual(result, { channelSelector: "FROM_PRIMARY", url: "https://primary.example/" });
  });

  test("excludes binding fields present only in primary (criterion gates which keys propagate)", () => {

    // primary has url but criterion doesn't - url drops out.
    const primary: ChannelDelta = { channelSelector: "ABC", url: "https://www.hulu.com/live" };
    const criterion: ChannelDelta = { channelSelector: "anything" };
    const result = intersectBindingDeltas(primary, criterion);

    assert.deepEqual(result, { channelSelector: "ABC" });
  });

  test("excludes identity fields entirely - result is restricted to CHANNEL_BINDING_KEYS", () => {

    // Both primary and criterion include identity fields (stationId, name). The result keeps only binding fields, even when both sides declare the same
    // identity field. This locks the binding-only filter that prevents identity from leaking into the variant-delta heal path.
    const primary: ChannelDelta = { channelSelector: "ABC", name: "ignored", stationId: "ignored", url: "https://www.hulu.com/live" };
    const criterion: ChannelDelta = { channelSelector: "x", name: "x", stationId: "x", url: "x" };
    const result = intersectBindingDeltas(primary, criterion);

    assert.deepEqual(result, { channelSelector: "ABC", url: "https://www.hulu.com/live" });
  });

  test("returns an empty object when criterion has no binding-key overlap with primary", () => {

    // primary has only url; criterion has only channelSelector. No binding keys appear in both - result is empty.
    const primary: ChannelDelta = { url: "https://www.hulu.com/live" };
    const criterion: ChannelDelta = { channelSelector: "ABC" };

    assert.deepEqual(intersectBindingDeltas(primary, criterion), {});
  });

  test("returns an empty object when both inputs are empty", () => {

    assert.deepEqual(intersectBindingDeltas({}, {}), {});
  });
});

describe("normalizeChannelDeltas", () => {

  test("strips a predefined canonical override that matches the predefined exactly", () => {

    const abc = PREDEFINED_CHANNELS["abc"] as CanonicalChannel;
    const channels: StoredChannelMap = { abc: { name: abc.name, url: abc.url } };
    const result = normalize(channels);

    assert.equal("abc" in result, false, "an empty override should be dropped entirely");
  });

  test("preserves a predefined canonical override with differing fields", () => {

    const channels: StoredChannelMap = { abc: { name: "ABC Custom" } };
    const result = normalize(channels);

    assert.deepEqual(result["abc"], { name: "ABC Custom" });
  });

  test("preserves a user standalone with nulls stripped", () => {

    const channels: StoredChannelMap = { mychannel: { channelNumber: null, name: "My Channel", url: "https://example.com" } };
    const result = normalize(channels);

    assert.deepEqual(result["mychannel"], { name: "My Channel", url: "https://example.com" });
  });

  test("dangling canonical (user variant pointing at missing canonical) preserves the entry rather than silently dropping it", () => {

    /* The WHAT this protects: a user variant whose canonicalKey points at a canonical that does not exist (typo, renamed canonical, removed predefined) must
     * not be silently deleted by the normalizer - the user keeps their binding data. Identity is unconditionally stripped from variant-shaped entries by
     * filterToDeltaSurface upstream, so the surviving contract is narrower than the resolved view: canonicalKey and binding fields survive, and the entry
     * stays in the map.
     */
    const channels: StoredChannelMap = {

      "nonexistent-local": {

        canonicalKey: "nonexistent-canonical",
        channelSelector: "FAKE",
        url: "https://example.com"
      }
    };
    const result = normalize(channels);
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
    const result = normalize(channels);

    assert.equal("abc-hulu" in result, false, "empty delta against predefined variant should collapse to nothing");
  });

  /* User identity edits on a predefined variant route to the canonical entry via handlePredefinedEdit in routes/config/channels/endpoints/crud.ts. The
   * equivalent WHAT (user identity edit on a variant via the UI ends up visible after resolution) is exercised by the route-layer test "identity-vs-binding
   * routing (variant active): identity goes to canonical, binding goes to the variant entry" in crud.test.ts.
   */
});

/* Sibling-variant non-overlap rule (storage invariant). A canonical override's binding fields exist to customize the canonical service's binding - never to
 * express "default this channel to a sibling service." When a canonical override's URL extracts to a sibling variant's domain, the normalizer redirects:
 * binding stripped from canonical, propagated as binding-only override on the matching variant when divergent, serviceSelections updated. The producer
 * (handlePredefinedEdit) and the normalizer share the inferTargetVariant helper as the single source of truth for the rule.
 *
 * Tests assert on the full ChannelsFileData envelope (not just channels) because the rule touches both data.channels and data.serviceSelections atomically.
 */
describe("normalizeChannelDeltas: sibling-variant non-overlap rule", () => {

  test("typical heal: canonical override with binding matching predefined sibling exactly heals to identity-only canonical + serviceSelections redirect", () => {

    /* The user's production data shape: ABC canonical override carries the full binding from the Hulu sibling variant (channelSelector + url) plus a stationId
     * the user added for guide data. The heal strips binding, sets serviceSelections.abc = "abc-hulu", creates no variant override (binding matches predefined
     * variant exactly), and leaves the identity (stationId) on the canonical.
     */
    const data = makeChannelsData({ abc: { channelSelector: "ABC", stationId: "20456", url: "https://www.hulu.com/live" } });

    normalizeChannelDeltas(data);

    assert.deepEqual(data.channels["abc"], { stationId: "20456" }, "canonical retains identity-only delta");
    assert.equal(data.serviceSelections["abc"], "abc-hulu", "serviceSelections records the redirect");
    assert.equal("abc-hulu" in data.channels, false, "no variant override created when binding matches predefined exactly");
  });

  test("divergent binding: canonical override with binding diverging from sibling predefined produces a binding-only variant override", () => {

    // User had a custom URL pointing at hulu.com but with a path that diverges from the predefined Hulu variant URL. The heal preserves that divergence by
    // writing it to the variant entry as a binding-only override.
    const data = makeChannelsData({ abc: { stationId: "20456", url: "https://www.hulu.com/live/east-coast" } });

    normalizeChannelDeltas(data);

    assert.deepEqual(data.channels["abc"], { stationId: "20456" }, "canonical retains identity-only delta");
    assert.equal(data.serviceSelections["abc"], "abc-hulu", "serviceSelections records the redirect");
    assert.deepEqual(data.channels["abc-hulu"], { url: "https://www.hulu.com/live/east-coast" },
      "divergent URL persisted as binding-only variant override (canonicalKey not stored - resolved via predefined variant's canonicalKey)");
  });

  test("identity-only canonical override is unaffected (no binding, no overlap possible)", () => {

    const data = makeChannelsData({ abc: { stationId: "20456" } });

    normalizeChannelDeltas(data);

    assert.deepEqual(data.channels["abc"], { stationId: "20456" }, "identity-only canonical passes through untouched");
    assert.deepEqual(data.serviceSelections, {}, "no redirect because no binding to overlap with");
  });

  test("custom URL with no sibling match: canonical override remains on canonical, no redirect", () => {

    // User has a genuinely custom URL that doesn't match any sibling variant's domain. This is the legitimate "Custom URL" case (Scenario B in
    // buildServiceGroups). The rule does NOT fire because there's no sibling overlap to heal.
    const data = makeChannelsData({ abc: { stationId: "20456", url: "https://example.com/abc-mirror" } });

    normalizeChannelDeltas(data);

    assert.deepEqual(data.channels["abc"], { stationId: "20456", url: "https://example.com/abc-mirror" },
      "custom-URL canonical override preserved as-is");
    assert.deepEqual(data.serviceSelections, {}, "no redirect for custom URL");
  });

  test("idempotency: running the heal twice on the same input yields the same result", () => {

    const dataOnce = makeChannelsData({ abc: { channelSelector: "ABC", stationId: "20456", url: "https://www.hulu.com/live" } });

    normalizeChannelDeltas(dataOnce);

    const dataTwice = makeChannelsData(dataOnce.channels, { serviceSelections: { ...dataOnce.serviceSelections } });

    normalizeChannelDeltas(dataTwice);

    assert.deepEqual(dataOnce.channels, dataTwice.channels, "channels stable across normalizations");
    assert.deepEqual(dataOnce.serviceSelections, dataTwice.serviceSelections, "serviceSelections stable across normalizations");
  });

  test("multiple siblings sharing a domain: deterministic alphabetical pick", () => {

    /* In practice no two predefined sibling variants share a domain (each service has its own URL space), but the rule must still be deterministic if it
     * happens. Construct a synthetic scenario: a user-stored variant `abc-custom1` and the predefined `abc-hulu` both resolve to hulu.com domain. The heal
     * picks the alphabetically-first key (abc-custom1).
     */
    const data = makeChannelsData({

      abc: { stationId: "20456", url: "https://www.hulu.com/live" },
      "abc-custom1": { canonicalKey: "abc", url: "https://www.hulu.com/live" }
    });

    normalizeChannelDeltas(data);

    assert.equal(data.serviceSelections["abc"], "abc-custom1",
      "alphabetically-first sibling wins when multiple siblings share the matching domain");
  });

  test("existing variant override preserved through the heal (heal does not clobber prior variant work)", () => {

    /* Edge case: user already has a variant-stored override on abc-hulu (e.g., custom channelSelector) AND a canonical override that overlaps with hulu's
     * domain. The heal redirects the canonical's serviceSelections without destroying the variant's prior override. The heal uses preserve-existing merge
     * semantics (mergeVariantBinding): when the canonical-derived binding delta is non-null, existing variant fields take precedence on conflicts. When the
     * canonical's binding fully matches the predefined variant (as in this test), the binding delta is null and no variant-level write happens at all - the
     * existing override is left intact except for the standard normalize-against-base pass that strips canonicalKey when it matches the predefined variant.
     */
    const data = makeChannelsData({

      abc: { stationId: "20456", url: "https://www.hulu.com/live" },
      "abc-hulu": { canonicalKey: "abc", channelSelector: "MyCustomABC" }
    });

    normalizeChannelDeltas(data);

    assert.deepEqual(data.channels["abc"], { stationId: "20456" });
    assert.equal(data.serviceSelections["abc"], "abc-hulu");
    assert.deepEqual(data.channels["abc-hulu"], { channelSelector: "MyCustomABC" },
      "prior variant override survives the heal (canonicalKey stripped because it matches predefined variant - documented existing convention)");
  });

  test("divergent binding AND existing variant override: mergeVariantBinding preserves variant fields, canonical-derived fields fill gaps", () => {

    /* The case the previous test does NOT exercise: canonical's binding diverges from the predefined variant (variantDelta is non-null), AND the variant
     * entry already has a prior user override. mergeVariantBinding's preserve-existing semantics fire here - existing variant fields take precedence on
     * conflicts, and the canonical-derived binding only fills fields the variant doesn't already declare. This locks the heal-context merge semantic
     * (distinct from the producer's authoritative-replace via replaceVariantBinding) under test so a future refactor can't silently swap to replace
     * semantics and clobber prior variant work.
     *
     * Setup: canonical has stationId + url=hulu.com/live/east-coast (URL diverges from predefined Hulu's hulu.com/live).
     *        abc-hulu variant entry exists with channelSelector="MyCustomABC" (prior user customization, no URL override).
     *
     * Expected after heal:
     *   - canonical: { stationId } only (binding stripped).
     *   - serviceSelections.abc = "abc-hulu" (redirect recorded).
     *   - abc-hulu: { channelSelector: "MyCustomABC", url: "hulu.com/live/east-coast" } - the canonical's URL fills the gap (variant didn't declare it),
     *     the prior channelSelector survives. canonicalKey stripped by the variant-branch normalize-against-base pass since it matches the predefined.
     */
    const data = makeChannelsData({

      abc: { stationId: "20456", url: "https://www.hulu.com/live/east-coast" },
      "abc-hulu": { canonicalKey: "abc", channelSelector: "MyCustomABC" }
    });

    normalizeChannelDeltas(data);

    assert.deepEqual(data.channels["abc"], { stationId: "20456" }, "canonical retains identity-only delta");
    assert.equal(data.serviceSelections["abc"], "abc-hulu", "serviceSelections records the redirect");
    assert.deepEqual(data.channels["abc-hulu"], { channelSelector: "MyCustomABC", url: "https://www.hulu.com/live/east-coast" },
      "merge preserves existing channelSelector and adds canonical-derived URL");
  });
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
    const once = normalize(channels);
    const twice = normalize(once);

    assert.deepEqual(once, twice, "normalizing a normalized map must produce the same map");
  });

  test("normalization preserves the resolved view for variant binding overrides (normalize then resolve equals direct resolve)", () => {

    /* The WHAT this protects: normalization is a storage optimization, not a semantic change. For an input that survives normalization - here, a user binding
     * override on a predefined variant - the resolved view must be identical before and after. This is the regression guard against accidentally turning the
     * normalizer into a destructive transform.
     */
    const storedBefore: StoredChannelMap = {

      "abc-hulu": {

        canonicalKey: "abc",
        channelSelector: "ABC-CUSTOM",
        url: "https://stream.directv.com"
      }
    };
    const normalized = normalize(storedBefore);

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
