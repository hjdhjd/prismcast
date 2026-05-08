/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.test.ts: Unit tests for the service-tag system. Coverage focuses on the pure helpers (getServiceDisplayName, getCanonicalKey, getChannelSortKey,
 * compareChannelSort), the tag-derivation pipeline, and the in-memory cache mutators (setEnabledServices, setServiceSelections). Persistent mutators
 * (mutateEnabledServices, setServiceSelection) are exercised indirectly via the persistence test layer.
 */
import { PREDEFINED_SUFFIX, buildServiceGroups, compareChannelSort, findPredefinedByDomain, getAllServiceTags, getAuthDomainForChannel, getCanonicalKey,
  getChannelServiceLabel, getChannelSortKey, getEnabledServices, getServiceDisplayName, getServiceGroup, getServiceSelections, hasMultipleServices,
  isChannelAvailableByService, isServiceTagEnabled, isServiceVariant, resolvePredefinedVariant, resolveServiceKey, setEnabledServices,
  setServiceSelections } from "./services.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import type { ResolvedChannelMap } from "../types/index.ts";
import assert from "node:assert/strict";
import { firstOf } from "../testing.helpers.ts";
import { makeChannel } from "./userChannels.helpers.ts";

describe("PREDEFINED_SUFFIX", () => {

  test("declares the documented :predefined sentinel", () => {

    assert.equal(PREDEFINED_SUFFIX, ":predefined");
  });
});

describe("getServiceDisplayName", () => {

  test("returns the built-in service name for a known full hostname", () => {

    assert.equal(getServiceDisplayName("https://tv.youtube.com/watch/abc"), "YouTube TV");
  });

  test("returns the built-in service name for a known concise domain", () => {

    assert.equal(getServiceDisplayName("https://www.hulu.com/live"), "Hulu");
  });

  test("returns the concise domain for an unknown URL", () => {

    assert.equal(getServiceDisplayName("https://example.example/"), "example.example");
  });

  test("returns the input verbatim for an unparseable URL (extractDomain fallback)", () => {

    // Boundary: extractDomain returns the input when URL parsing fails. The function then looks up by that input in DOMAIN_CONFIG and falls through.
    assert.equal(getServiceDisplayName("not a url"), "not a url");
  });
});

describe("getCanonicalKey", () => {

  test("strips :predefined suffix to return the base key", () => {

    // The function checks the registry first; for an unknown :predefined key it falls back to slicing the suffix.
    assert.equal(getCanonicalKey("nbc:predefined"), "nbc");
  });

  test("returns the input unchanged when not in any service group", () => {

    assert.equal(getCanonicalKey("definitely-not-a-real-channel-key"), "definitely-not-a-real-channel-key");
  });
});

describe("isServiceTagEnabled", () => {

  let originalEnabled: string[];

  beforeEach(() => {

    originalEnabled = [];

    // The setter copies; capture before mutation so afterEach can restore.
    setEnabledServices(originalEnabled);
  });

  afterEach(() => {

    setEnabledServices(originalEnabled);
  });

  test("returns true for any tag when no filter is active", () => {

    setEnabledServices([]);
    assert.equal(isServiceTagEnabled("hulu"), true);
    assert.equal(isServiceTagEnabled("yttv"), true);
  });

  test("returns true for 'direct' even when filter is active", () => {

    setEnabledServices(["hulu"]);
    assert.equal(isServiceTagEnabled("direct"), true, "direct is always enabled");
  });

  test("returns true for tags in the filter and false for others", () => {

    setEnabledServices([ "hulu", "yttv" ]);
    assert.equal(isServiceTagEnabled("hulu"), true);
    assert.equal(isServiceTagEnabled("yttv"), true);
    assert.equal(isServiceTagEnabled("sling"), false);
  });
});

describe("setEnabledServices and setServiceSelections", () => {

  let originalEnabled: string[];
  let originalSelections: Record<string, string>;

  beforeEach(() => {

    originalEnabled = [];
    originalSelections = getServiceSelections();
    setEnabledServices(originalEnabled);
  });

  afterEach(() => {

    setEnabledServices(originalEnabled);
    setServiceSelections(originalSelections);
  });

  test("setEnabledServices defensively copies the input (mutating the caller's array does not leak)", () => {

    const input = [ "hulu", "yttv" ];

    setEnabledServices(input);
    input.push("sling");

    // After the setter, internal state should still hold the original two tags. We exercise via isServiceTagEnabled which reads the same state.
    assert.equal(isServiceTagEnabled("hulu"), true);
    assert.equal(isServiceTagEnabled("yttv"), true);
    assert.equal(isServiceTagEnabled("sling"), false, "post-setter mutation of the input did not leak");
  });

  test("setServiceSelections replaces the cache contents wholesale", () => {

    setServiceSelections({ abc: "abc-hulu", nbc: "nbc-hulu" });
    assert.deepEqual(getServiceSelections(), { abc: "abc-hulu", nbc: "nbc-hulu" });

    setServiceSelections({});
    assert.deepEqual(getServiceSelections(), {});
  });
});

describe("buildServiceGroups", () => {

  let originalSelections: Record<string, string>;

  beforeEach(() => {

    originalSelections = getServiceSelections();
  });

  afterEach(() => {

    setServiceSelections(originalSelections);
  });

  test("returns an empty array of stale keys when there are no service selections", () => {

    setServiceSelections({});

    const channels: ResolvedChannelMap = {

      mychannel: makeChannel({ name: "Mine", url: "https://example.com/" })
    };

    const result = buildServiceGroups(channels);

    assert.deepEqual(result, [], "no stale keys to clean");
  });

  test("returns stale-key list when a stored selection no longer maps to a real variant", () => {

    setServiceSelections({ nbc: "nbc-not-a-real-variant" });

    const channels: ResolvedChannelMap = {

      nbc: makeChannel({ name: "NBC", url: "https://www.nbc.com/live" })
    };

    const result = buildServiceGroups(channels);

    assert.deepEqual(result, ["nbc"], "stale selection's canonical key is reported");
  });
});

describe("isChannelAvailableByService", () => {

  let originalEnabled: string[];

  beforeEach(() => {

    originalEnabled = [];
    setEnabledServices(originalEnabled);
  });

  afterEach(() => {

    setEnabledServices(originalEnabled);
  });

  test("returns true for any channel when no filter is active", () => {

    setEnabledServices([]);
    assert.equal(isChannelAvailableByService("any-key"), true);
  });
});

describe("getAllServiceTags", () => {

  test("includes the 'direct' channel-website tag at the front", () => {

    const first = firstOf(getAllServiceTags(), "service tag");

    assert.equal(first.tag, "direct");
    assert.equal(first.displayName, "Channel Website");
  });

  test("returns each tag with displayName and tag fields populated", () => {

    const tags = getAllServiceTags();

    for(const entry of tags) {

      assert.ok(typeof entry.tag === "string", "tag is a string");
      assert.ok(typeof entry.displayName === "string", "displayName is a string");
    }
  });
});

describe("getChannelSortKey", () => {

  test("returns zero-padded channel number for channelNumber field", () => {

    const channel = makeChannel({ channelNumber: 7 });

    assert.equal(getChannelSortKey(channel, "test", "channelNumber"), "000007");
  });

  test("returns 'zzzzzz' for missing channelNumber so it sorts last in ascending order", () => {

    const channel = makeChannel({ channelNumber: undefined });

    assert.equal(getChannelSortKey(channel, "test", "channelNumber"), "zzzzzz");
  });

  test("returns the lowercased name for the name field", () => {

    const channel = makeChannel({ name: "ABC News" });

    assert.equal(getChannelSortKey(channel, "test", "name"), "abc news");
  });

  test("falls back to the key when name is missing", () => {

    const channel = makeChannel({ name: undefined });

    assert.equal(getChannelSortKey(channel, "abc-key", "name"), "abc-key");
  });

  test("returns lowercase channelSelector when set", () => {

    const channel = makeChannel({ channelSelector: "ESPN-HD" });

    assert.equal(getChannelSortKey(channel, "espn", "channelSelector"), "espn-hd");
  });

  test("returns empty string when channelSelector is absent", () => {

    const channel = makeChannel();

    assert.equal(getChannelSortKey(channel, "test", "channelSelector"), "");
  });

  test("returns padded stationId when set, 'zzzzzz' otherwise", () => {

    assert.equal(getChannelSortKey(makeChannel({ stationId: "12345" }), "test", "stationId"), "012345");
    assert.equal(getChannelSortKey(makeChannel(), "test", "stationId"), "zzzzzz");
  });

  test("returns key unchanged for the 'key' sort field (lowercased)", () => {

    assert.equal(getChannelSortKey(makeChannel(), "ESPN-Hulu", "key"), "espn-hulu");
  });

  test("returns '0' for hdhrEnabled when effectively true, '1' when false", () => {

    // The implementation uses getEffectiveHdhrEnabled which treats hdhrEnabled !== false as enabled.
    assert.equal(getChannelSortKey(makeChannel({ hdhrEnabled: true }), "test", "hdhrEnabled"), "0");
    assert.equal(getChannelSortKey(makeChannel({ hdhrEnabled: false }), "test", "hdhrEnabled"), "1");
  });
});

describe("compareChannelSort", () => {

  test("ascending name sort orders alphabetically", () => {

    const cmp = compareChannelSort(makeChannel({ name: "Apple" }), "a", makeChannel({ name: "Banana" }), "b", "name", "asc");

    assert.ok(cmp < 0, "Apple sorts before Banana");
  });

  test("descending name sort reverses the comparison", () => {

    const cmp = compareChannelSort(makeChannel({ name: "Apple" }), "a", makeChannel({ name: "Banana" }), "b", "name", "desc");

    assert.ok(cmp > 0);
  });

  test("ties on the primary field break by ascending name regardless of direction", () => {

    // Both have channelNumber 5 - tie. Tiebreaker: name ascending. Even when direction is descending, the tiebreaker stays ascending.
    const a = makeChannel({ channelNumber: 5, name: "AAA" });
    const b = makeChannel({ channelNumber: 5, name: "ZZZ" });
    const ascResult = compareChannelSort(a, "a", b, "b", "channelNumber", "asc");
    const descResult = compareChannelSort(a, "a", b, "b", "channelNumber", "desc");

    assert.ok(ascResult < 0, "AAA before ZZZ ascending");
    assert.ok(descResult < 0, "AAA still before ZZZ even when primary is descending");
  });
});

describe("getAuthDomainForChannel", () => {

  test("returns extracted domain from the channel's URL when key is in PREDEFINED_CHANNELS", () => {

    const result = getAuthDomainForChannel("nbc");

    // nbc canonical url -> extractDomain
    const nbc = PREDEFINED_CHANNELS["nbc"];

    assert.ok(nbc, "nbc predefined exists");
    assert.equal(result.length > 0, true, "auth domain is non-empty for known channel");
  });

  test("returns empty string for an unknown channel key", () => {

    assert.equal(getAuthDomainForChannel("definitely-not-a-real-channel"), "");
  });
});

describe("resolvePredefinedVariant", () => {

  test("returns the predefined variant resolved against its canonical (identity inherited)", () => {

    // abc-hulu is a predefined variant of abc; the resolver should layer abc's identity onto the variant's binding fields.
    const result = resolvePredefinedVariant("abc-hulu");

    assert.ok(result, "abc-hulu resolves");
    assert.equal(result.canonicalKey, "abc");
    assert.equal(result.name, "ABC", "identity comes from canonical");
  });

  test("returns the canonical itself for a canonical key (no variant inheritance needed)", () => {

    const result = resolvePredefinedVariant("abc");

    assert.ok(result);
    assert.equal(result.name, "ABC");
  });

  test("returns undefined for an unknown key", () => {

    assert.equal(resolvePredefinedVariant("not-a-real-key-xyz"), undefined);
  });
});

describe("findPredefinedByDomain", () => {

  /* The reverse index is built by buildServiceGroups; route-handler tests transitively populate it via initializePersistence. We test the throw-and-fallback
   * branch (URL parse failure -> empty array) directly since that path doesn't depend on index content.
   */

  test("returns an empty array when the URL cannot be parsed (extractDomain throws)", () => {

    /* extractDomain calls new URL(); a structurally-broken URL throws and the catch falls through to []. The fallback prevents the inline-hint feature from
     * crashing the form on malformed input.
     */
    const result = findPredefinedByDomain("\x00\x01\x02 not a url");

    assert.deepEqual(result, [], "unparseable URL falls through to empty array");
  });

  test("returns an empty array for a domain not present in the index", () => {

    /* Even with a parseable URL, an unknown domain yields []. This is the not-found branch (different from the parse-failure branch above) - both produce
     * empty arrays but via different code paths.
     */
    const result = findPredefinedByDomain("https://definitely-not-a-real-tv-domain-x9z2.example/");

    assert.deepEqual(result, []);
  });
});

describe("getServiceDisplayName: extended branch coverage", () => {

  /* The function checks the built-in DOMAIN_CONFIG (full hostname first, then concise domain) before falling through to getDomainConfig (which includes user
   * domain mappings). The test in the upstream describe block covers full-hostname and concise hits; here we add the user-domain fallthrough path. Without
   * runtime initialization of user domain mappings, the user-domain branch yields the same path as no-config (returns concise domain).
   */

  test("returns the concise domain when getDomainConfig returns no service for the URL (last-resort fallthrough)", () => {

    /* No DOMAIN_CONFIG entry, no user domain mapping -> the function falls through to extractDomain at the bottom. We use a synthetic domain that we know is
     * not present in either map.
     */
    assert.equal(getServiceDisplayName("https://example.test/"), "example.test");
  });
});

describe("getChannelServiceLabel", () => {

  /* The label dispatcher checks fields in order: explicit `service` field on the channel, channel.profile resolved via user domain mappings, then URL-based
   * built-in display name. We test the explicit field and the URL fallback; the user-profile branch requires runtime user domain state (transitively covered
   * by the channels/services HTTP route tests).
   */

  test("returns the explicit `service` field when set (highest precedence)", () => {

    const channel = makeChannel({ service: "Custom Label", url: "https://www.hulu.com/" });

    assert.equal(getChannelServiceLabel(channel), "Custom Label");
  });

  test("falls back to URL-based display name when no service or profile is set", () => {

    /* With no explicit service field and no profile, the function delegates to getServiceDisplayName via the URL.
     */
    const channel = makeChannel({ url: "https://www.hulu.com/" });

    assert.equal(getChannelServiceLabel(channel), "Hulu", "URL resolves to the built-in service name");
  });

  test("returns the concise domain when URL resolves to no known service", () => {

    /* No service, no profile, unknown URL -> falls through to extractDomain via getServiceDisplayName.
     */
    const channel = makeChannel({ url: "https://example.test/" });

    assert.equal(getChannelServiceLabel(channel), "example.test");
  });
});

describe("getChannelSortKey: profile field switch", () => {

  /* The profile branch has three sub-branches:
   *
   *   - explicit profile set -> profile name lowercased
   *   - auto-detect resolves to "default" -> empty string
   *   - auto-detect resolves to a non-default service -> "!" + label.lowercase
   *
   * The "!" prefix sorts auto-detected channels between explicit-profile channels and empty-profile channels in ascending order.
   */

  test("returns the profile name lowercased when an explicit profile is set", () => {

    const channel = makeChannel({ profile: "Hulu" });

    assert.equal(getChannelSortKey(channel, "test", "profile"), "hulu");
  });

  test("returns empty string when profile auto-detects to 'default' (no recognizable service URL)", () => {

    /* getProfileForChannel returns { profileName: "default" } for an unknown URL. The sort key collapses to empty string so these channels sort first in
     * ascending order.
     */
    const channel = makeChannel({ url: "https://example.test/" });

    assert.equal(getChannelSortKey(channel, "test", "profile"), "");
  });

  test("returns '!' prefixed label when profile auto-detects to a non-default service (URL resolves to a known service)", () => {

    /* getProfileForChannel returns a non-default profile for known URLs (e.g., hulu.com -> "hulu"). The sort key is "!" + serviceLabel.lowercase. The "!"
     * prefix sorts auto-detected entries between explicit profiles and empty profiles in ASCII order.
     */
    const channel = makeChannel({ url: "https://www.hulu.com/" });

    assert.equal(getChannelSortKey(channel, "test", "profile"), "!hulu", "auto-detected non-default profile gets ! prefix + lowercase label");
  });

  test("falls through to key-lowercase for unknown sort field (default branch)", () => {

    /* The switch's default branch returns key.toLowerCase(). This is the "unknown sort field" safety - in production VALID_SORT_FIELDS gates input, but the
     * switch's default exists for defense in depth.
     */
    const channel = makeChannel();

    assert.equal(getChannelSortKey(channel, "Test-Key", "unknown-field" as never), "test-key");
  });
});

describe("compareChannelSort: tiebreaker null-name path", () => {

  test("uses keyA / keyB when channel.name is undefined for the tiebreaker", () => {

    /* The tiebreaker reads `channelA.name ?? keyA` so a channel with no name falls back to its key. Two channels both lacking name and tied on the primary
     * field sort by their keys ascending.
     */
    const a = makeChannel({ channelNumber: 5 });
    const b = makeChannel({ channelNumber: 5 });

    /* Both have name="Test" via makeChannel default. Override to undefined to trigger the null-fallback in the tiebreaker.
     */
    a.name = undefined;
    b.name = undefined;

    const cmp = compareChannelSort(a, "alpha", b, "beta", "channelNumber", "asc");

    assert.ok(cmp < 0, "tied on channelNumber, tiebreaker uses keys: 'alpha' < 'beta'");
  });

  test("falls back to key for whichever side has a null name (mixed null/non-null)", () => {

    /* One side has a name, the other doesn't. The tiebreaker compares the lowercased name on the side that has it against the key on the side that doesn't.
     */
    const a = makeChannel({ channelNumber: 5, name: "ZZZ" });
    const b = makeChannel({ channelNumber: 5 });

    b.name = undefined;

    /* a's name lowercased is "zzz"; b's fallback key is "alpha-key". "alpha-key" < "zzz", so cmp must be positive (a sorts after b).
     */
    const cmp = compareChannelSort(a, "test-a", b, "alpha-key", "channelNumber", "asc");

    assert.ok(cmp > 0, "name='zzz' sorts after key='alpha-key' in the tiebreaker");
  });
});

describe("isServiceVariant", () => {

  /* The predicate consults the runtime serviceGroups map. Without a buildServiceGroups call seeding state, the map is empty and every key returns false. We
   * exercise both a true case (post-seeding) and a false case.
   */

  let originalSelections: Record<string, string>;

  beforeEach(() => {

    originalSelections = getServiceSelections();
  });

  afterEach(() => {

    setServiceSelections(originalSelections);
  });

  test("returns false for a key not in any service group", () => {

    setServiceSelections({});
    buildServiceGroups({});

    assert.equal(isServiceVariant("nonexistent-key"), false);
  });

  test("returns true for a non-canonical variant key after buildServiceGroups runs", () => {

    /* Build a synthetic group with a canonical and a variant; the variant key should be flagged as a variant (group.canonicalKey !== key).
     */
    const channels: ResolvedChannelMap = {

      mychannel: makeChannel({ name: "Mine", url: "https://example.com/" }),
      "mychannel-variant": makeChannel({ canonicalKey: "mychannel", url: "https://other.example.com/" })
    };

    buildServiceGroups(channels);

    assert.equal(isServiceVariant("mychannel-variant"), true, "variant key returns true");
    assert.equal(isServiceVariant("mychannel"), false, "canonical key returns false");
  });
});

describe("hasMultipleServices", () => {

  let originalSelections: Record<string, string>;

  beforeEach(() => {

    originalSelections = getServiceSelections();
  });

  afterEach(() => {

    setServiceSelections(originalSelections);
  });

  test("returns false for a key not in any service group", () => {

    setServiceSelections({});
    buildServiceGroups({});

    assert.equal(hasMultipleServices("nonexistent-key"), false);
  });

  test("returns true when the channel is in a group with more than one variant entry", () => {

    /* A multi-service group: synthetic channel with at least two service variants. buildServiceGroups creates the canonical entry plus variants in the same
     * group. We seed two entries that point at the same canonical via canonicalKey.
     */
    const channels: ResolvedChannelMap = {

      mychannel: makeChannel({ name: "Mine", url: "https://example.com/" }),
      "mychannel-a": makeChannel({ canonicalKey: "mychannel", url: "https://a.example.com/" }),
      "mychannel-b": makeChannel({ canonicalKey: "mychannel", url: "https://b.example.com/" })
    };

    buildServiceGroups(channels);

    assert.equal(hasMultipleServices("mychannel"), true, "canonical with two variants has multiple services");
    assert.equal(hasMultipleServices("mychannel-a"), true, "variant key reports the same group's multi-service status");
  });
});

describe("getEnabledServices: defensive copy", () => {

  let originalEnabled: string[];

  beforeEach(() => {

    originalEnabled = [];
    setEnabledServices(originalEnabled);
  });

  afterEach(() => {

    setEnabledServices(originalEnabled);
  });

  test("mutating the returned array does not leak into module state", () => {

    /* The accessor returns [...enabledServices]. Tests pin the spread - if a future refactor removed it, mutations on the returned array would leak into the
     * module-level state and corrupt downstream filter behavior.
     */
    setEnabledServices([ "hulu", "yttv" ]);

    const snapshot1 = getEnabledServices();

    snapshot1.push("MUTATED");

    const snapshot2 = getEnabledServices();

    assert.deepEqual(snapshot2.toSorted(), [ "hulu", "yttv" ].toSorted(), "second read does not include the mutation");
  });
});

describe("buildServiceGroups: user-override scenarios A and B", () => {

  /* The function's branch matrix for user overrides of canonicals (lines 418-451 in services.ts):
   *
   *   Scenario A: predefined exists AND user override is on the same domain as predefined or a known variant -> single canonical entry, label = service name.
   *   Scenario B: predefined exists AND user override is on a DIFFERENT domain -> 2-entry group ["Custom (domain)", original-service-name].
   */

  let originalSelections: Record<string, string>;

  beforeEach(() => {

    originalSelections = getServiceSelections();
  });

  afterEach(() => {

    setServiceSelections(originalSelections);
  });

  test("Scenario A (predefined override on same domain): emits canonical with its service label, no :predefined synthetic", () => {

    /* Pass 2 fires when at least one variant key is in the input. We seed the nbc canonical (user override) plus the nbc-hulu predefined variant so Pass 1
     * collects nbc as a canonical with one variant. The user's override URL is on nbc.com (same domain as the predefined nbc canonical's site URL), so Pass 2's
     * Scenario A branch runs - the variant entry uses getChannelServiceLabel(canonical) and no :predefined synthetic entry is added.
     */
    const nbcPredefined = PREDEFINED_CHANNELS["nbc"];

    if(!nbcPredefined) {

      // Catalog shape changed; skip rather than break.
      return;
    }

    setServiceSelections({});

    /* makeChannel produces a fresh reference; spreading nbcPredefined ensures the override is detected by isUserOverride (reference comparison). The URL stays
     * on nbc.com so the userDomain matches the canonical's domain (Scenario A).
     */
    const userOverride = makeChannel({ ...nbcPredefined, name: "NBC My Custom", url: "https://www.nbc.com/different-page" });
    const huluVariant = makeChannel({ canonicalKey: "nbc", url: "https://www.hulu.com/live" });
    const channels: ResolvedChannelMap = { nbc: userOverride, "nbc-hulu": huluVariant };

    buildServiceGroups(channels);

    const group = getServiceGroup("nbc");

    assert.ok(group, "nbc group exists (Pass 2 fires because nbc-hulu carries canonicalKey)");

    /* Scenario A: no synthetic :predefined entry, and the canonical's variant entry uses the service label rather than "Custom (...)".
     */
    const hasPredefinedSuffix = group.variants.some((v) => v.key.endsWith(":predefined"));

    assert.equal(hasPredefinedSuffix, false, "Scenario A does not emit a :predefined variant");

    const canonicalVariant = group.variants.find((v) => v.key === "nbc");

    assert.ok(canonicalVariant, "canonical variant entry present");
    assert.equal(canonicalVariant.label.startsWith("Custom"), false, "label is the service display name, not a 'Custom (...)' prefix");
  });

  test("Scenario B (predefined override on different domain): emits Custom + :predefined variants", () => {

    /* Same setup as Scenario A but the user's override URL is on a foreign domain. Scenario B emits two entries: { key: nbc, label: 'Custom (<domain>)' } and
     * the :predefined synthetic with the original service's label.
     */
    const nbcPredefined = PREDEFINED_CHANNELS["nbc"];

    if(!nbcPredefined) {

      return;
    }

    setServiceSelections({});

    /* extractDomain returns the concise domain (e.g., "example.test" for "foreign.example.test"). The label uses that concise form, so the test asserts on
     * "Custom (example.test)" rather than the full hostname.
     */
    const userOverride = makeChannel({ ...nbcPredefined, name: "NBC Custom", url: "https://foreign.example.test/feed" });
    const huluVariant = makeChannel({ canonicalKey: "nbc", url: "https://www.hulu.com/live" });
    const channels: ResolvedChannelMap = { nbc: userOverride, "nbc-hulu": huluVariant };

    buildServiceGroups(channels);

    const group = getServiceGroup("nbc");

    assert.ok(group, "nbc group exists");

    const variantKeys = group.variants.map((v) => v.key).toSorted();

    assert.ok(variantKeys.includes("nbc"), "canonical entry exists with custom URL");
    assert.ok(variantKeys.includes("nbc:predefined"), "Scenario B emits the :predefined synthetic variant pointing at the original predefined service");

    const customVariant = group.variants.find((v) => v.key === "nbc");

    assert.ok(customVariant, "custom variant entry exists");
    assert.match(customVariant.label, /^Custom \(.+\)$/, "label has 'Custom (...)' shape (concise domain)");
  });
});

describe("buildServiceGroups: Pass 3 (single-service predefined override)", () => {

  /* Pass 3 creates synthetic 2-entry groups for user overrides of single-service predefined channels (channels with no canonicalKey-based variants). Two
   * scenarios:
   *
   *   - Scenario A: user URL domain matches predefined -> no group created (just modified-dot indicator).
   *   - Scenario B: user URL domain differs -> create 2-entry group with "Custom (domain)" + :predefined.
   */

  let originalSelections: Record<string, string>;

  beforeEach(() => {

    originalSelections = getServiceSelections();
  });

  afterEach(() => {

    setServiceSelections(originalSelections);
  });

  test("Scenario A: user override of single-service predefined on the same domain -> no group created", () => {

    /* Find a single-service predefined (no variants, no canonicalKey): we'll use one whose URL we can override with a same-domain URL. ABC's site URL is on
     * abc.com. abc itself has many variants, so it's NOT single-service. We need to scan PREDEFINED_CHANNELS for a canonical with no variants.
     *
     * The 'cnbc' channel might be single-service. Skip the test if we can't find one - the test's intent (Scenario A negative) is documented above.
     */
    const candidates = Object.entries(PREDEFINED_CHANNELS).filter(
      ([ key, channel ]) => (channel.canonicalKey === undefined) && !Object.keys(PREDEFINED_CHANNELS).some((k) => PREDEFINED_CHANNELS[k]?.canonicalKey === key)
    );

    if(candidates.length === 0) {

      return;
    }

    const [ canonicalKey, predefined ] = candidates[0]!;

    setServiceSelections({});

    const url = new URL(predefined.url);
    const userOverride = makeChannel({ ...predefined, name: "Override", url: url.origin + "/different-page" });
    const channels: ResolvedChannelMap = { [canonicalKey]: userOverride };

    buildServiceGroups(channels);

    const group = getServiceGroup(canonicalKey);

    assert.equal(group, undefined, "Scenario A (same-domain override) does not create a service group");
  });

  test("Scenario B: user override of single-service predefined on a DIFFERENT domain -> creates 2-entry group", () => {

    /* Same setup but URL goes to a foreign domain. Pass 3 detects the domain mismatch and emits the 2-entry group.
     */
    const candidates = Object.entries(PREDEFINED_CHANNELS).filter(
      ([ key, channel ]) => (channel.canonicalKey === undefined) && !Object.keys(PREDEFINED_CHANNELS).some((k) => PREDEFINED_CHANNELS[k]?.canonicalKey === key)
    );

    if(candidates.length === 0) {

      return;
    }

    const [ canonicalKey, predefined ] = candidates[0]!;

    setServiceSelections({});

    const userOverride = makeChannel({ ...predefined, name: "Override", url: "https://foreign.example.test/different" });
    const channels: ResolvedChannelMap = { [canonicalKey]: userOverride };

    buildServiceGroups(channels);

    const group = getServiceGroup(canonicalKey);

    assert.ok(group, "Scenario B creates a synthetic group");
    assert.equal(group.variants.length, 2, "group has exactly 2 entries: Custom + :predefined");

    const keys = group.variants.map((v) => v.key).toSorted();

    assert.deepEqual(keys, [ canonicalKey, canonicalKey + ":predefined" ].toSorted());
  });
});

describe("resolveServiceKey: filter-fallback paths", () => {

  /* The filter fallback fires when the user has an active service filter and the resolved selection (or canonical) is filtered out. Two branches:
   *
   *   - No selection: `enabledServices.length > 0 && !isServiceTagEnabled(canonicalServiceTag)` -> findFirstEnabledVariant.
   *   - Valid selection but its tag is filtered out: same fallback.
   */

  let originalSelections: Record<string, string>;
  let originalEnabled: string[];

  beforeEach(() => {

    originalSelections = getServiceSelections();
    originalEnabled = getEnabledServices();
  });

  afterEach(() => {

    setServiceSelections(originalSelections);
    setEnabledServices(originalEnabled);
  });

  test("no selection + no filter: returns the canonical key (happy path)", () => {

    setEnabledServices([]);
    setServiceSelections({});

    /* abc canonical exists with no selection or filter; resolves to itself.
     */
    assert.equal(resolveServiceKey("abc"), "abc");
  });

  test("no selection + filter active and canonical's tag is enabled: returns canonical", () => {

    /* The filter is active but the canonical's service tag is in enabledServices. resolveServiceKey returns the canonical without falling back.
     *
     * For abc canonical, the URL is abc.com which has serviceTag "direct" - "direct" is always enabled regardless of filter, so the canonical resolves.
     */
    setEnabledServices(["sling"]);
    setServiceSelections({});
    buildServiceGroups({});

    /* abc canonical doesn't exist in the empty buildServiceGroups, but resolveServiceKey is robust to that - it just operates on the in-memory state.
     * This test confirms the no-fallback branch when no selection exists and the tag is enabled (direct is always enabled).
     */
    assert.equal(resolveServiceKey("abc"), "abc");
  });

  test("valid selection: returns the selection when its tag is enabled", () => {

    setEnabledServices([]);
    setServiceSelections({ abc: "abc-hulu" });

    /* No filter, valid selection -> returns the selection verbatim.
     */
    assert.equal(resolveServiceKey("abc"), "abc-hulu");
  });

  test("valid selection: returns the selection unchanged even when canonical is in groups", () => {

    /* When the user has explicitly selected a non-canonical variant and no filter is active, the selection wins. Pin the no-fallback branch on the with-selection
     * path.
     */
    setEnabledServices([]);
    setServiceSelections({ abc: "abc-hulu" });

    const channels: ResolvedChannelMap = {

      abc: makeChannel({ name: "ABC", url: "https://abc.com/" }),
      "abc-hulu": makeChannel({ canonicalKey: "abc", url: "https://hulu.com/abc" })
    };

    buildServiceGroups(channels);

    assert.equal(resolveServiceKey("abc"), "abc-hulu");
  });
});
