/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelForm.test.ts: Unit tests for the channel form domain logic. Coverage focuses on the three public exports - channelMatches, computePredefinedDelta,
 * and findMatchingVariant - plus their tag-comparison contract. The module mediates between HTML form values and the channel storage model, so the tests
 * verify the field normalization (default-empty strings, undefined channelNumber, true-by-default hdhrEnabled) and delta-clear semantics.
 */
import { type ChannelFormValues, channelMatches, computePredefinedDelta, findMatchingVariant } from "./channelForm.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { makeChannel } from "./userChannels.helpers.ts";

// makeForm builds a ChannelFormValues literal. All scalar fields default to the empty/undefined sentinel matching an empty form submission.
function makeForm(overrides: Partial<ChannelFormValues> = {}): ChannelFormValues {

  return {

    channelNumber: undefined,
    channelSelector: "",
    guideTitle: "",
    hdhrEnabled: true,
    logoUrl: "",
    name: "",
    profile: "",
    stationId: "",
    url: "",
    ...overrides
  };
}


describe("channelMatches", () => {

  test("returns true when every scalar field and tags match", () => {

    const channel = makeChannel({ channelNumber: 5, name: "ABC", url: "https://abc.com" });
    const form = makeForm({ channelNumber: 5, name: "ABC", url: "https://abc.com" });

    assert.equal(channelMatches(channel, form, []), true);
  });

  test("returns false when a scalar field differs (name)", () => {

    const channel = makeChannel({ name: "ABC", url: "https://abc.com" });
    const form = makeForm({ name: "ABC Custom", url: "https://abc.com" });

    assert.equal(channelMatches(channel, form, []), false);
  });

  test("returns false when channelNumber differs", () => {

    const channel = makeChannel({ channelNumber: 5, name: "ABC", url: "https://abc.com" });
    const form = makeForm({ channelNumber: 6, name: "ABC", url: "https://abc.com" });

    assert.equal(channelMatches(channel, form, []), false);
  });

  test("treats undefined channelNumber on form as matching undefined on channel", () => {

    const channel = makeChannel({ name: "ABC", url: "https://abc.com" });
    const form = makeForm({ channelNumber: undefined, name: "ABC", url: "https://abc.com" });

    assert.equal(channelMatches(channel, form, []), true);
  });

  test("hdhrEnabled comparison treats absent channel field as effectively true", () => {

    // getEffectiveHdhrEnabled returns true for absent or true. Form default is true; the form thus matches a channel with no hdhrEnabled field.
    const channel = makeChannel({ name: "ABC", url: "https://abc.com" });
    const form = makeForm({ hdhrEnabled: true, name: "ABC", url: "https://abc.com" });

    assert.equal(channelMatches(channel, form, []), true);
  });

  test("hdhrEnabled mismatch surfaces (channel has false, form has true)", () => {

    const channel = makeChannel({ hdhrEnabled: false, name: "ABC", url: "https://abc.com" });
    const form = makeForm({ hdhrEnabled: true, name: "ABC", url: "https://abc.com" });

    assert.equal(channelMatches(channel, form, []), false);
  });

  test("tags are compared order-independently", () => {

    const channel = makeChannel({ name: "ABC", tags: [ "Local", "News" ], url: "https://abc.com" });
    const form = makeForm({ name: "ABC", url: "https://abc.com" });

    assert.equal(channelMatches(channel, form, [ "News", "Local" ]), true, "different tag order still matches");
    assert.equal(channelMatches(channel, form, [ "Local", "News" ]), true, "same tag order still matches");
    assert.equal(channelMatches(channel, form, ["Local"]), false, "missing a tag does not match");
  });

  test("tag length mismatch fails fast (boundary)", () => {

    const channel = makeChannel({ name: "ABC", tags: [ "A", "B" ], url: "https://abc.com" });
    const form = makeForm({ name: "ABC", url: "https://abc.com" });

    assert.equal(channelMatches(channel, form, ["A"]), false, "shorter tags array");
    assert.equal(channelMatches(channel, form, [ "A", "B", "C" ]), false, "longer tags array");
  });

  test("tags of equal length but different content do not match (post-isDeepStrictEqual semantics)", () => {

    /* The recently-changed isDeepStrictEqual switch on the tags equality check produces order-independent comparison via sortTags; pinning the equal-length
     * but distinct-content case prevents a regression where the deep-equal call accidentally compares unsorted arrays (which would produce false negatives
     * for reordered tags) or compares sorted-by-different-keys arrays (false positives for reordered-but-different-content tags).
     */
    const channel = makeChannel({ name: "ABC", tags: [ "Local", "News" ], url: "https://abc.com" });
    const form = makeForm({ name: "ABC", url: "https://abc.com" });

    assert.equal(channelMatches(channel, form, [ "Local", "Sports" ]), false, "swapping one tag for a different value of the same length does not match");
    assert.equal(channelMatches(channel, form, [ "Movies", "Kids" ]), false, "swapping every tag for different values of the same length does not match");
  });

  test("returns false when url differs", () => {

    const channel = makeChannel({ name: "ABC", url: "https://abc.com" });
    const form = makeForm({ name: "ABC", url: "https://abc.com/different" });

    assert.equal(channelMatches(channel, form, []), false);
  });
});

describe("computePredefinedDelta", () => {

  test("returns hasChanges=false and an empty delta when form matches the predefined exactly", () => {

    const base = makeChannel({ channelNumber: 5, name: "ABC", url: "https://abc.com" });
    const form = makeForm({ channelNumber: 5, name: "ABC", url: "https://abc.com" });
    const result = computePredefinedDelta(base, form, []);

    assert.equal(result.hasChanges, false);
    assert.deepEqual(result.delta, {});
  });

  test("includes only fields that differ from the base in the delta", () => {

    const base = makeChannel({ name: "ABC", url: "https://abc.com" });
    const form = makeForm({ name: "ABC Custom", url: "https://abc.com" });
    const result = computePredefinedDelta(base, form, []);

    assert.equal(result.hasChanges, true);
    assert.equal(result.delta.name, "ABC Custom");
    assert.equal(result.delta.url, undefined, "url unchanged so absent from delta");
  });

  test("stores null for empty-string fields (cleared by user)", () => {

    const base = makeChannel({ name: "ABC", stationId: "12345", url: "https://abc.com" });
    const form = makeForm({ name: "ABC", stationId: "", url: "https://abc.com" });
    const result = computePredefinedDelta(base, form, []);

    assert.equal(result.hasChanges, true);
    assert.equal(result.delta.stationId, null, "explicit clear stored as null");
  });

  test("stores null for undefined channelNumber when base had a number", () => {

    const base = makeChannel({ channelNumber: 7, name: "ABC", url: "https://abc.com" });
    const form = makeForm({ channelNumber: undefined, name: "ABC", url: "https://abc.com" });
    const result = computePredefinedDelta(base, form, []);

    assert.equal(result.hasChanges, true);
    assert.equal(result.delta.channelNumber, null);
  });

  test("includes tags in the delta only when they differ from base effective tags", () => {

    const base = makeChannel({ name: "ABC", tags: ["Local"], url: "https://abc.com" });
    const form = makeForm({ name: "ABC", url: "https://abc.com" });
    const result = computePredefinedDelta(base, form, [ "Local", "Sports" ]);

    assert.equal(result.hasChanges, true);
    assert.deepEqual(result.delta.tags, [ "Local", "Sports" ], "tags differ from base; preserved in delta");
  });

  test("stores null for tags when user clears them and base had some", () => {

    const base = makeChannel({ name: "ABC", tags: [ "Local", "News" ], url: "https://abc.com" });
    const form = makeForm({ name: "ABC", url: "https://abc.com" });
    const result = computePredefinedDelta(base, form, []);

    assert.equal(result.hasChanges, true);
    assert.equal(result.delta.tags, null, "cleared tags stored as null");
  });

  test("does not store tags in delta when they match base order-insensitively", () => {

    const base = makeChannel({ name: "ABC", tags: [ "Local", "News" ], url: "https://abc.com" });
    const form = makeForm({ name: "ABC", url: "https://abc.com" });
    const result = computePredefinedDelta(base, form, [ "News", "Local" ]);

    assert.equal(result.hasChanges, false);
    assert.equal(result.delta.tags, undefined);
  });

  test("hdhrEnabled change from true (default) to false is captured", () => {

    const base = makeChannel({ name: "ABC", url: "https://abc.com" });
    const form = makeForm({ hdhrEnabled: false, name: "ABC", url: "https://abc.com" });
    const result = computePredefinedDelta(base, form, []);

    assert.equal(result.hasChanges, true);
    assert.equal(result.delta.hdhrEnabled, false);
  });
});

describe("findMatchingVariant", () => {

  test("returns undefined when the canonical key has no service group", () => {

    const form = makeForm({ name: "Whatever", url: "https://example.com" });

    assert.equal(findMatchingVariant("not-a-real-canonical-key-xyz", form, []), undefined);
  });
});
