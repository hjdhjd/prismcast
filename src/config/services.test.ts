/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.test.ts: Unit tests for the service-tag system. Coverage focuses on the pure helpers (getServiceDisplayName, getCanonicalKey, getChannelSortKey,
 * compareChannelSort), the tag-derivation pipeline, and the in-memory cache mutators (setEnabledServices, setServiceSelections). Persistent mutators
 * (mutateEnabledServices, setServiceSelection) are exercised indirectly via the persistence test layer.
 */
import { PREDEFINED_SUFFIX, buildServiceGroups, compareChannelSort, getAllServiceTags, getAuthDomainForChannel, getCanonicalKey, getChannelSortKey,
  getServiceDisplayName, getServiceSelections, isChannelAvailableByService, isServiceTagEnabled, resolvePredefinedVariant, setEnabledServices,
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
