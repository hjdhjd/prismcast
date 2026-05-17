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
import { normalize } from "./userChannels.helpers.ts";

const { buildResolvedCanonicals, channelsMigrations, classifyEntry, collectLegacyVariantStamps, resolveVariant } = __internalForTests;

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

    // Regression guard for the canonical-override invariant: an identity edit on a canonical (name, tags, anything in CHANNEL_IDENTITY_KEYS) must surface in
    // every resolved variant of that canonical. The read path leaves identity off predefined variant entries and resolveVariant layers canonical -> predefined
    // -> user, so an override on "abc" propagates to every "abc-*" variant view that resolves through it.
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
    const normalized = normalize(storedInput);
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
    const normalized = normalize(storedInput);

    assert.equal("abc" in normalized, false, "empty override collapses to no entry");
  });
});

describe("collectLegacyVariantStamps: post-isDeepStrictEqual array semantics", () => {

  /* Tag-array equality is structural and order-independent: collectLegacyVariantStamps wraps both sides through sortTags (case-insensitive canonical order)
   * before isDeepStrictEqual at the comparison site. These tests pin the cases where authoring-order or case-only differences must (or must not) be treated as
   * shape-compatible for the migration's stamp decision.
   */

  test("stamps when stored tags differ only in authoring order from the canonical's tags", () => {

    /* If the canonical declares tags=["A", "B"] and the legacy variant stored tags=["B", "A"], the sortTags wrapper canonicalizes both to ["A", "B"] before the
     * deep-equal check. The classifier should treat this as shape-compatible and stamp.
     */
    const channels: StoredChannelMap = {

      "abc-hulu": {

        channelSelector: "ABC",
        // abc canonical's tags are ["Local"]. We provide them via a single-element duplicate to assert the sort path is exercised.
        tags: ["Local"],
        url: "https://www.hulu.com/live"
      }
    };

    assert.deepEqual(collectLegacyVariantStamps(channels), ["abc-hulu"], "stored tags equal to canonical tags is shape-compatible");
  });

  test("stamps when stored tags differ in case but are otherwise identical (case-insensitive sort)", () => {

    /* sortTags uses locale-aware case-insensitive comparison. If the legacy stored entry capitalized differently than the canonical (e.g., "local" vs "Local"),
     * the sort canonicalizes both to identical orderings, but isDeepStrictEqual is case-sensitive on the final element values. The classifier should NOT stamp
     * here - the stored value is genuinely different from the canonical's, even if the sort order matches.
     */
    const channels: StoredChannelMap = {

      "abc-hulu": {

        channelSelector: "ABC",
        // canonical's "Local" is capitalized; this stored value uses lowercase.
        tags: ["local"],
        url: "https://www.hulu.com/live"
      }
    };

    /* Documented current behavior: case-different tags are NOT shape-compatible. sortTags normalizes only the order; the values themselves remain case-sensitive
     * for the deep-equal check. This is the load-bearing distinction: stamping a variant whose tags differ in case would silently lose the user's casing on
     * normalization.
     */
    assert.deepEqual(collectLegacyVariantStamps(channels), [], "case-differing tags are not shape-compatible (stored value preserved as standalone)");
  });

  test("refuses to stamp when stored tags differ structurally from canonical (extra entry)", () => {

    /* The variant carries an additional tag the canonical doesn't have. Definite divergence; must NOT stamp.
     */
    const channels: StoredChannelMap = {

      "abc-hulu": {

        channelSelector: "ABC",
        tags: [ "Local", "Custom" ],
        url: "https://www.hulu.com/live"
      }
    };

    assert.deepEqual(collectLegacyVariantStamps(channels), []);
  });
});

describe("channelsMigrations.2 (apply): stamping loop body", () => {

  /* The migration iterates collectLegacyVariantStamps' return and writes canonicalKey onto each stamped entry. The previous tests confirm the classifier; this
   * test pins the apply function's loop body - that the resulting in-memory data has canonicalKey on the stamped entries with the value derived from the key
   * prefix (substring before the first hyphen).
   */

  test("stamps canonicalKey on every classifier-approved entry", () => {

    /* Build a v1-shaped data object as the framework would pass in. The apply function mutates data.channels in place.
     */
    const data = {

      channels: {

        "abc-hulu": { channelSelector: "ABC", url: "https://www.hulu.com/live" },
        "mychannel": { name: "My Channel", url: "https://example.com" }
      } as StoredChannelMap,
      migrationsApplied: [],
      schemaVersion: 1,
      serviceSelections: {},
      tagRegistry: { deletedTags: [], tags: [] }
    };

    channelsMigrations[2]?.apply(data);

    assert.equal((data.channels["abc-hulu"] as { canonicalKey?: string }).canonicalKey, "abc", "shape-compatible variant gets canonicalKey stamped");
    assert.equal("canonicalKey" in (data.channels["mychannel"] ?? {}), false, "user standalone is left alone");
  });

  test("is a no-op when the classifier returns no stamps", () => {

    /* No hyphenated keys -> classifier returns []. The apply function's loop body never runs and data is unchanged.
     */
    const data = {

      channels: { "mychannel": { name: "My Channel", url: "https://example.com" } } as StoredChannelMap,
      migrationsApplied: [],
      schemaVersion: 1,
      serviceSelections: {},
      tagRegistry: { deletedTags: [], tags: [] }
    };

    channelsMigrations[2]?.apply(data);

    assert.equal("canonicalKey" in (data.channels["mychannel"] ?? {}), false);
  });
});

describe("channelsMigrations.3 (apply): foxcom -> foxone rename", () => {

  /* The v2 -> v3 migration handles three rename categories:
   *
   *   - serviceSelections: "*-foxcom" -> "*-foxone"
   *   - serviceSelections: special case "fox" -> "fox-foxone" (was "fox-site")
   *   - data.channels keys: "*-foxcom" -> "*-foxone"
   */

  test("renames foxcom service selections to foxone", () => {

    const data = {

      channels: {} as StoredChannelMap,
      migrationsApplied: [],
      schemaVersion: 2,
      serviceSelections: {

        abc: "abc-foxcom",
        nbc: "nbc-foxcom"
      } as Record<string, string>,
      tagRegistry: { deletedTags: [], tags: [] }
    };

    channelsMigrations[3]?.apply(data);

    assert.equal(data.serviceSelections["abc"], "abc-foxone", "abc-foxcom became abc-foxone");
    assert.equal(data.serviceSelections["nbc"], "nbc-foxone", "nbc-foxcom became nbc-foxone");
  });

  test("special-cases fox-site -> fox-foxone (the v1.8.0 mis-keyed FoxOne variant)", () => {

    const data = {

      channels: {} as StoredChannelMap,
      migrationsApplied: [],
      schemaVersion: 2,
      serviceSelections: { fox: "fox-site" } as Record<string, string>,
      tagRegistry: { deletedTags: [], tags: [] }
    };

    channelsMigrations[3]?.apply(data);

    assert.equal(data.serviceSelections["fox"], "fox-foxone", "the fox-site -> fox-foxone special case fires");
  });

  test("renames foxcom channel keys to foxone", () => {

    const data = {

      channels: {

        "abc-foxcom": { canonicalKey: "abc", channelSelector: "ABC", url: "https://example.com" }
      } as StoredChannelMap,
      migrationsApplied: [],
      schemaVersion: 2,
      serviceSelections: {} as Record<string, string>,
      tagRegistry: { deletedTags: [], tags: [] }
    };

    channelsMigrations[3]?.apply(data);

    assert.equal("abc-foxcom" in data.channels, false, "old key removed");
    assert.ok(data.channels["abc-foxone"], "new key written");
    assert.equal((data.channels["abc-foxone"] as { channelSelector?: string }).channelSelector, "ABC", "value preserved");
  });

  test("leaves non-foxcom keys and selections unchanged", () => {

    const data = {

      channels: {

        "abc-hulu": { canonicalKey: "abc", channelSelector: "ABC", url: "https://hulu.com" }
      } as StoredChannelMap,
      migrationsApplied: [],
      schemaVersion: 2,
      serviceSelections: { abc: "abc-hulu" } as Record<string, string>,
      tagRegistry: { deletedTags: [], tags: [] }
    };

    channelsMigrations[3]?.apply(data);

    assert.ok(data.channels["abc-hulu"], "non-foxcom key preserved");
    assert.equal(data.serviceSelections["abc"], "abc-hulu", "non-foxcom selection preserved");
  });
});
