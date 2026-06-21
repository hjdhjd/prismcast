/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.resolution.test.ts: Tests for the channel resolution primitives - classifyEntry, overlayDelta, resolveVariant, resolveStoredChannel, and the
 * public getPredefinedChannel accessor.
 *
 * These are the pure building blocks that power getMergedChannelMap and normalizeChannelDeltas. Unit-testing them in isolation verifies the algorithm without
 * relying on the full initialization pipeline (file store, service groups, config loading).
 */
import type { CanonicalChannel, ResolvedChannel, StoredChannel, VariantChannel } from "../types/index.ts";
import { __internalForTests, getPredefinedChannel, resolveStoredChannel } from "./userChannels.ts";
import { describe, test } from "node:test";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import assert from "node:assert/strict";

const { classifyEntry, overlayDelta, resolveVariant } = __internalForTests;

describe("classifyEntry", () => {

  test("classifies a predefined canonical (no stored delta) as canonical", () => {

    const result = classifyEntry("abc", undefined);

    assert.equal(result.kind, "canonical");
  });

  test("classifies a predefined canonical with a user override as canonical with stored delta", () => {

    const stored = { name: "ABC Custom" } as StoredChannel;
    const result = classifyEntry("abc", stored);

    assert.equal(result.kind, "canonical");
  });

  test("classifies a predefined variant (canonicalKey declared in PREDEFINED_CHANNELS) as variant", () => {

    const result = classifyEntry("abc-hulu", undefined);

    // assert.equal narrows the discriminated union via its assertion-function signature, so result.canonicalKey is reachable below without a separate if guard.
    assert.equal(result.kind, "variant");
    assert.equal(result.canonicalKey, "abc");
  });

  test("classifies a user variant (canonicalKey declared in stored, no predefined) as variant", () => {

    const stored = { canonicalKey: "abc", channelSelector: "KABC", url: "https://stream.directv.com" } as StoredChannel;
    const result = classifyEntry("abc-kabc", stored);

    assert.equal(result.kind, "variant");
    assert.equal(result.canonicalKey, "abc");
  });

  test("classifies a user-only entry with no canonicalKey as standalone", () => {

    const stored = { name: "My Custom Channel", url: "https://example.com/live" } as StoredChannel;
    const result = classifyEntry("mychannel", stored);

    assert.equal(result.kind, "standalone");
  });

  test("user canonicalKey takes precedence over predefined canonicalKey when both declare one", () => {

    // A user override on a predefined variant key: the user's declared canonicalKey wins. This matches production semantics where user data has the highest
    // priority during classification.
    const stored = { canonicalKey: "abc" } as StoredChannel;
    const result = classifyEntry("abc-hulu", stored);

    assert.equal(result.kind, "variant");
    assert.equal(result.canonicalKey, "abc");
  });

  test("throws when neither predefined nor stored has the key", () => {

    // Precondition violation: the function's contract requires at least one source to carry the key. Callers iterate over the union of key sets, so this branch
    // is unreachable in production; the throw exists to surface a bug if a future caller drops the precondition.
    assert.throws(() => classifyEntry("nonexistent-key-that-does-not-exist", undefined), /exists in neither/);
  });
});

describe("overlayDelta", () => {

  test("returns a copy with the base's fields when the delta is empty", () => {

    const base = { name: "ABC", url: "https://abc.com/watch-live" } as ResolvedChannel;
    const result = overlayDelta(base, {});

    assert.deepEqual(result, base);
    assert.notEqual(result, base, "expected a fresh reference, not the base itself");
  });

  test("overlays defined delta fields onto the base", () => {

    const base = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;
    const delta = { name: "ABC Custom" } as StoredChannel;
    const result = overlayDelta(base, delta);

    assert.equal(result.name, "ABC Custom");
    assert.equal(result.url, "https://abc.com");
  });

  test("null in a delta clears the field on the base", () => {

    const base = { name: "ABC", stationId: "12345", url: "https://abc.com" } as ResolvedChannel;
    const delta = { stationId: null } as StoredChannel;
    const result = overlayDelta(base, delta);

    assert.equal(result.name, "ABC");
    assert.equal(result.url, "https://abc.com");
    assert.equal("stationId" in result, false, "null delta should have removed stationId");
  });

  test("undefined in a delta inherits from the base", () => {

    const base = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;
    const delta = { name: undefined } as StoredChannel;
    const result = overlayDelta(base, delta);

    assert.equal(result.name, "ABC");
  });

  test("non-delta fields (canonicalKey) on the stored entry pass through", () => {

    const base = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;
    const delta = { canonicalKey: "abc", url: "https://hulu.com/live" } as StoredChannel;
    const result = overlayDelta(base, delta);

    assert.equal(result.canonicalKey, "abc");
    assert.equal(result.url, "https://hulu.com/live");
  });

  test("defensively copies tags so mutations to the result do not affect the base", () => {

    const base = { name: "ABC", tags: [ "Local", "News" ], url: "https://abc.com" } as ResolvedChannel;
    const result = overlayDelta(base, {});

    result.tags!.push("Mutated");
    assert.deepEqual(base.tags, [ "Local", "News" ], "base.tags should not have been affected by a push on the result");
  });

  test("delta tags replace base tags entirely (not merge)", () => {

    const base = { name: "ABC", tags: [ "Local", "News" ], url: "https://abc.com" } as ResolvedChannel;
    const delta = { tags: ["Sports"] } as StoredChannel;
    const result = overlayDelta(base, delta);

    assert.deepEqual(result.tags, ["Sports"]);
  });
});

describe("resolveVariant", () => {

  test("overlays the predefined variant's service fields onto the canonical", () => {

    const canonical = { name: "ABC", tags: ["Local"], url: "https://abc.com/watch-live" } as ResolvedChannel;
    const predefined = { canonicalKey: "abc", channelSelector: "ABC", url: "https://www.hulu.com/live" } as VariantChannel;
    const result = resolveVariant(canonical, predefined, undefined);

    assert.equal(result.name, "ABC", "identity inherits from canonical");
    assert.equal(result.url, "https://www.hulu.com/live", "service URL comes from variant");
    assert.equal(result.channelSelector, "ABC");
    assert.equal(result.canonicalKey, "abc");
    assert.deepEqual(result.tags, ["Local"], "tags inherit from canonical");
  });

  test("user stored delta overrides binding fields on the predefined variant", () => {

    /* A user stored delta overrides binding fields on a variant, but identity (such as stationId) from the delta is silently dropped: variant identity always
     * inherits from the canonical, and per-affiliate identity is expressed via the standalone classification path. We assert only the binding-override behavior
     * here; the per-affiliate identity path (the abc-kabc divergent-identity case) is exercised by the standalone-classification tests in the migration suite.
     */
    const canonical = { name: "ABC", url: "https://abc.com/watch-live" } as ResolvedChannel;
    const predefined = { canonicalKey: "abc", channelSelector: "ABC", url: "https://www.hulu.com/live" } as VariantChannel;
    const stored = { channelSelector: "ABC-CUSTOM" } as StoredChannel;
    const result = resolveVariant(canonical, predefined, stored);

    assert.equal(result.name, "ABC", "identity inherits from canonical");
    assert.equal(result.channelSelector, "ABC-CUSTOM", "stored binding override wins over predefined variant");
    assert.equal(result.url, "https://www.hulu.com/live", "predefined variant URL survives since stored did not override it");
  });
});

describe("resolveStoredChannel", () => {

  test("resolves a predefined-key stored entry as a delta against the predefined definition", () => {

    const stored = { name: "ABC Custom" } as StoredChannel;
    const result = resolveStoredChannel("abc", stored);
    const base = PREDEFINED_CHANNELS["abc"] as CanonicalChannel;

    assert.equal(result.name, "ABC Custom");
    assert.equal(result.url, base.url, "unoverridden fields inherit from predefined");
    assert.notEqual(result, base, "result is a fresh reference (so it compares !== to PREDEFINED_CHANNELS for isUserOverride)");
  });

  test("returns a defensive copy for a standalone user channel", () => {

    const stored = { name: "My Channel", tags: [ "Custom", "Test" ], url: "https://example.com" } as StoredChannel;
    const result = resolveStoredChannel("custom-channel-key", stored);

    assert.notEqual(result, stored);
    assert.notEqual(result.tags, (stored as { tags: string[] }).tags, "tags should be a defensive copy");
    assert.deepEqual(result, stored, "content should match");
  });
});

describe("getPredefinedChannel", () => {

  test("returns the predefined canonical as-is", () => {

    const result = getPredefinedChannel("abc");

    assert.equal(result, PREDEFINED_CHANNELS["abc"]);
  });

  test("resolves a predefined variant against its canonical so identity fields surface", () => {

    // abc-hulu is a predefined variant of abc. Under the new flattener, the variant entry carries only service-specific fields (URL, channelSelector) plus
    // canonicalKey - identity inherits at resolution time. getPredefinedChannel must surface the resolved view so findMatchingVariant and computePredefinedDelta
    // compare form values against the full identity the user sees.
    const result = getPredefinedChannel("abc-hulu");
    const abcHuluPredefined = PREDEFINED_CHANNELS["abc-hulu"]!;

    assert.ok(result, "abc-hulu should resolve to a defined predefined channel");
    assert.equal(result.name, "ABC", "identity comes from the canonical");
    assert.equal(result.canonicalKey, "abc");
    assert.equal(result.url, abcHuluPredefined.url, "service URL comes from the variant");
  });

  test("returns undefined for unknown keys", () => {

    assert.equal(getPredefinedChannel("not-a-real-channel-xyz"), undefined);
  });
});
