/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.test.ts: Unit tests for the predicates, lookups, and label dispatchers in services.ts - PREDEFINED_SUFFIX, getServiceDisplayName, getCanonicalKey,
 * isServiceTagEnabled, isChannelAvailableByService, getAllServiceTags, getAuthDomainForChannel, resolvePredefinedVariant, findPredefinedByDomain,
 * getChannelServiceLabel, isServiceVariant, hasMultipleServices, getEnabledServices defensive copy, plus the in-memory cache mutators (setEnabledServices,
 * setServiceSelections). Service-group construction lives in services.serviceGroups.test.ts; sort-key computation lives in services.sortKeys.test.ts;
 * persistent mutators (mutateEnabledServices, setServiceSelection) are exercised indirectly via the persistence test layer.
 */
import { PREDEFINED_SUFFIX, buildServiceGroups, findPredefinedByDomain, getAllServiceTags, getAuthDomainForChannel, getCanonicalKey, getChannelServiceLabel,
  getEnabledServices, getServiceDisplayName, getServiceSelections, hasMultipleServices, isChannelAvailableByService, isServiceTagEnabled, isServiceVariant,
  resolvePredefinedVariant, setEnabledServices, setServiceSelections } from "./services.ts";
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

    assert.ok(result, "resolvePredefinedVariant should return the canonical for a canonical key");
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

