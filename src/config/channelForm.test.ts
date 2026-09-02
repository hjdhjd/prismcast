/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelForm.test.ts: Unit tests for the channel form domain logic. Coverage focuses on the three public exports - channelMatches, computePredefinedDelta,
 * and findMatchingVariant - plus their tag-comparison contract. The module mediates between HTML form values and the channel storage model, so the tests
 * verify the field normalization (default-empty strings, undefined channelNumber, true-by-default hdhrEnabled) and delta-clear semantics.
 */
import { channelMatches, computePredefinedDelta, findMatchingVariant } from "./channelForm.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { makeChannel } from "./userChannels.helpers.ts";
import { makeForm } from "./channelForm.helpers.ts";

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

  test("forceCapture comparison treats an absent channel field as false", () => {

    // The inverse of hdhrEnabled's default: channelScalar derives (value === true), so anything other than an explicit true reads as unchecked. An untouched
    // channel therefore matches a form whose box is clear, which is what keeps a plain save from writing the field.
    const channel = makeChannel({ name: "ABC", url: "https://abc.com" });
    const form = makeForm({ forceCapture: false, name: "ABC", url: "https://abc.com" });

    assert.equal(channelMatches(channel, form, []), true);
  });

  test("forceCapture mismatch surfaces (channel has true, form has false)", () => {

    const channel = makeChannel({ forceCapture: true, name: "ABC", url: "https://abc.com" });
    const form = makeForm({ forceCapture: false, name: "ABC", url: "https://abc.com" });

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

    /* Tag equality is order-independent (via sortTags) but content-sensitive: equal-length arrays whose contents differ must not match. This case asserts the
     * boundary that the length check alone cannot cover, where the arrays are the same size but hold different tag values.
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

  test("forceCapture checked against an unforced base contributes true to the delta", () => {

    const base = makeChannel({ name: "ABC", url: "https://abc.com" });
    const form = makeForm({ forceCapture: true, name: "ABC", url: "https://abc.com" });
    const result = computePredefinedDelta(base, form, []);

    assert.equal(result.hasChanges, true);
    assert.equal(result.delta.forceCapture, true);
  });

  test("forceCapture unchecked contributes nothing - not a clearing null", () => {

    /* The delta omits the field entirely rather than storing null. Every save path replaces the stored entry wholesale, so an absent field is what turns the
     * override off; a null here would persist as an explicit "no value" and is the shape PATCH's merge path needs, not this one.
     */
    const base = makeChannel({ name: "ABC", url: "https://abc.com" });
    const form = makeForm({ forceCapture: false, name: "ABC Renamed", url: "https://abc.com" });
    const result = computePredefinedDelta(base, form, []);

    assert.equal(result.hasChanges, true, "the rename is still a change");
    assert.equal("forceCapture" in result.delta, false, "the field must be absent, not null");
  });
});

describe("findMatchingVariant", () => {

  test("returns undefined when the canonical key has no service group", () => {

    const form = makeForm({ name: "Whatever", url: "https://example.com" });

    assert.equal(findMatchingVariant("not-a-real-canonical-key-xyz", form, []), undefined);
  });
});
