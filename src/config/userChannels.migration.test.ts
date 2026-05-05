/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.migration.test.ts: Tests for collectLegacyVariantStamps (the tightened Migration 1 classifier) and end-to-end regression scenarios tying directly
 * to user-reported bugs.
 *
 * The migration classifier is the defensive line that prevents the old stamp-and-strip data loss. Tests here lock in the contract: stamp shape-compatible legacy
 * variants (safe to normalize as variants), leave user standalones alone (would lose per-variant identity if misclassified). Regression tests verify the fix for
 * jsfullam's case and the canonical-override propagation surfaced during refactor review.
 */
import type { CanonicalChannel, StoredChannelMap } from "../types/index.ts";
import { describe, test } from "node:test";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import { __internalForTests } from "./userChannels.ts";
import assert from "node:assert/strict";

const { buildResolvedCanonicals, classifyEntry, collectLegacyVariantStamps, normalizeChannelDeltas, resolveVariant } = __internalForTests;

describe("collectLegacyVariantStamps", () => {

  test("stamps a legacy full-copy variant whose identity fields match the canonical", () => {

    // Pre-v1.9.1 variants stored all identity fields redundantly from the canonical. These are shape-compatible with a legitimate variant and should be stamped
    // with canonicalKey so the delta model can start normalizing them.
    const abc = PREDEFINED_CHANNELS["abc"] as CanonicalChannel;
    const channels: StoredChannelMap = {

      "abc-hulu": {

        channelSelector: "ABC",
        name: abc.name,
        tags: abc.tags?.slice(),
        url: "https://www.hulu.com/live"
      }
    };

    assert.deepEqual(collectLegacyVariantStamps(channels), ["abc-hulu"]);
  });

  test("refuses to stamp a user standalone whose identity differs from the canonical (the abc-kabc case)", () => {

    // jsfullam's local affiliates: key like abc-kabc with custom stationId/channelNumber. The prefix matches a predefined canonical (abc), but the stored identity
    // differs from the canonical's identity, so Migration 1 must treat this as a user standalone and leave it alone. Stamping would trigger the delta normalizer
    // to strip the custom identity - the exact failure mode that caused the original data loss.
    const channels: StoredChannelMap = {

      "abc-kabc": {

        channelNumber: 7,
        name: "ABC Los Angeles",
        stationId: "57342",
        url: "https://stream.directv.com"
      }
    };

    assert.deepEqual(collectLegacyVariantStamps(channels), [], "user standalone with custom identity must not be stamped");
  });

  test("refuses to stamp entries whose prefix does not match a predefined canonical", () => {

    const channels: StoredChannelMap = { "not-a-real-prefix": { name: "Whatever", url: "https://example.com" } };

    assert.deepEqual(collectLegacyVariantStamps(channels), []);
  });

  test("refuses to stamp entries with no hyphen in the key", () => {

    const channels: StoredChannelMap = { "mychannel": { name: "My Channel", url: "https://example.com" } };

    assert.deepEqual(collectLegacyVariantStamps(channels), []);
  });

  test("skips entries that already declare canonicalKey", () => {

    // An already-stamped entry is already on the new model; re-stamping is a no-op and we must not count it in the return.
    const channels: StoredChannelMap = { "abc-hulu": { canonicalKey: "abc", channelSelector: "ABC" } };

    assert.deepEqual(collectLegacyVariantStamps(channels), []);
  });

  test("stamps entries with no identity fields at all (shape-compatible by vacuous truth)", () => {

    // A variant that has only service-specific fields like URL and channelSelector is shape-compatible - there is no identity to disagree with the canonical.
    const channels: StoredChannelMap = {

      "abc-sling": { channelSelector: "ABC", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" }
    };

    assert.deepEqual(collectLegacyVariantStamps(channels), ["abc-sling"]);
  });

  test("handles mixed batches correctly (stamps some, leaves others alone)", () => {

    const abc = PREDEFINED_CHANNELS["abc"] as CanonicalChannel;
    const channels: StoredChannelMap = {

      "abc-hulu": { channelSelector: "ABC", name: abc.name, url: "https://www.hulu.com/live" },
      "abc-kabc": { channelNumber: 7, name: "ABC Los Angeles", stationId: "57342", url: "https://stream.directv.com" },
      "abc-sling": { channelSelector: "ABC", url: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z" },
      "mychannel": { name: "My Channel", url: "https://example.com" }
    };
    const stamps = collectLegacyVariantStamps(channels).toSorted();

    assert.deepEqual(stamps, [ "abc-hulu", "abc-sling" ], "only the shape-compatible legacy variants should be stamped");
  });
});

describe("regression: canonical override propagates to predefined variants", () => {

  test("user override on abc canonical surfaces through resolving abc-hulu", () => {

    // This is the regression flagged during the refactor. Before the fix, applyVariantInheritance lived on the read path and pulled identity from the (possibly
    // overridden) canonical. Removing that function without moving the behavior elsewhere broke canonical override propagation. The new flattener leaves
    // identity off predefined variants, and resolveVariant layers canonical -> predefined -> user, so the override propagates through.
    const storedInput: StoredChannelMap = { "abc": { name: "American Broadcasting Custom" } };

    const resolvedCanonicals = buildResolvedCanonicals(storedInput);
    const abc = resolvedCanonicals["abc"]!;
    const abcHuluPredefined = PREDEFINED_CHANNELS["abc-hulu"]!;
    const resolved = resolveVariant(abc, abcHuluPredefined, undefined);

    assert.equal(resolved.name, "American Broadcasting Custom", "canonical rename propagates to the variant");
    assert.equal(resolved.url, abcHuluPredefined.url, "variant's service URL stays unchanged");
    assert.equal(resolved.channelSelector, abcHuluPredefined.channelSelector, "variant's service selector stays unchanged");
  });

  test("canonical tag override propagates to all variants", () => {

    const storedInput: StoredChannelMap = { "abc": { tags: [ "Local", "MyCustom" ] } };

    const resolvedCanonicals = buildResolvedCanonicals(storedInput);
    const resolved = resolveVariant(resolvedCanonicals["abc"]!, PREDEFINED_CHANNELS["abc-hulu"], undefined);

    assert.deepEqual(resolved.tags, [ "Local", "MyCustom" ]);
  });
});

describe("regression: hyphenated user standalone survives the migration", () => {

  test("abc-kabc with divergent identity stays a standalone and resolves as-is", () => {

    // Full flow: user has stored a standalone entry whose key happens to collide with the variant-shaped pattern. Migration 1 must not stamp canonicalKey.
    // Subsequent normalization must treat the entry as a standalone (null-strip only, no base-matching). Resolution should carry identity verbatim.
    const storedInput: StoredChannelMap = {

      "abc-kabc": {

        channelNumber: 7,
        name: "ABC Los Angeles",
        stationId: "57342",
        url: "https://stream.directv.com"
      }
    };

    // Migration classifier must refuse to stamp.
    assert.deepEqual(collectLegacyVariantStamps(storedInput), []);

    // Normalization (treats as standalone) preserves all user fields. The resulting standalone is shaped like a CanonicalChannel since it carries identity.
    const normalized = normalizeChannelDeltas(storedInput);
    const entry = normalized["abc-kabc"] as CanonicalChannel;

    assert.equal(entry.name, "ABC Los Angeles");
    assert.equal(entry.stationId, "57342");
    assert.equal(entry.channelNumber, 7);
    assert.equal("canonicalKey" in entry, false, "no canonicalKey should have been added");

    // Classification and resolution treat it as a standalone.
    const classification = classifyEntry("abc-kabc", entry);

    assert.equal(classification.kind, "standalone");
  });
});

describe("regression: predefined variant identity inherits correctly with no user data", () => {

  test("abc-hulu (no overrides) resolves with ABC's identity and hulu's service fields", () => {

    // Predefined variants under the new flattener carry only service-specific fields. The read path must layer canonical identity onto them. This is the base
    // case that ensures routine predefined-variant resolution works.
    const abc = PREDEFINED_CHANNELS["abc"] as CanonicalChannel;
    const abcHulu = PREDEFINED_CHANNELS["abc-hulu"]!;

    // A canonical Channel becomes a ResolvedChannel by passing it through the resolver as both canonical and predefined positions; for this test we know the
    // flattener has already populated the canonical's identity, so we cast for the contract checks below.
    const resolvedCanonicals = buildResolvedCanonicals({ "abc": abc });
    const resolved = resolveVariant(resolvedCanonicals["abc"]!, abcHulu, undefined);

    assert.equal(resolved.name, "ABC", "identity inherits from canonical");
    assert.equal(resolved.canonicalKey, "abc");
    assert.equal(resolved.channelSelector, "ABC");
    assert.equal(resolved.url, "https://www.hulu.com/live");
    assert.deepEqual(resolved.tags, abc.tags, "tags inherit from canonical");
  });
});

describe("regression: redundant predefined override collapses to nothing", () => {

  test("user saves the predefined values unchanged - delta normalizer drops the entry entirely", () => {

    // If the user opens the edit form and saves without changing anything, the form submits the currently-displayed (resolved) values. The delta normalizer must
    // recognize every field matches the predefined and drop the entry, letting the predefined shine through unchanged.
    const abc = PREDEFINED_CHANNELS["abc"] as CanonicalChannel;
    const storedInput: StoredChannelMap = {

      "abc": {

        name: abc.name,
        tags: abc.tags?.slice(),
        url: abc.url
      }
    };
    const normalized = normalizeChannelDeltas(storedInput);

    assert.equal("abc" in normalized, false, "empty override collapses to no entry");
  });
});
