/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.helpers.test.ts: Tests for the test helpers in userChannels.helpers.ts. Helpers carry the same coverage rigor as production code per the test
 * conventions - the rest of the userChannels suite depends on these factories producing the right shape, so a bug here cascades into misleading test results
 * across migration/normalization/resolution.
 */
import { describe, test } from "node:test";
import { getCanonical, makeChannel, makeChannelDelta, makeStoredCanonical, makeStoredVariant } from "./userChannels.helpers.ts";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import assert from "node:assert/strict";

/* StoredChannel is a union (Channel | ChannelDelta) so reads of optional fields go through bracket-as-Record access; that gives us a uniform read surface
 * regardless of which arm of the union the value falls into.
 */
function read(value: unknown, key: string): unknown {

  return (value as Record<string, unknown>)[key];
}

describe("makeStoredVariant", () => {

  test("returns the abc-hulu default shape when no overrides are passed", () => {

    const variant = makeStoredVariant();

    assert.equal(read(variant, "canonicalKey"), "abc", "default canonicalKey is 'abc'");
    assert.equal(read(variant, "channelSelector"), "ABC", "default channelSelector is 'ABC'");
    assert.equal(read(variant, "url"), "https://www.hulu.com/live", "default url is the hulu live URL");
  });

  test("merges overrides onto the defaults", () => {

    const variant = makeStoredVariant({ canonicalKey: "espn", url: "https://www.espn.com/live" });

    assert.equal(read(variant, "canonicalKey"), "espn", "override wins for canonicalKey");
    assert.equal(read(variant, "url"), "https://www.espn.com/live", "override wins for url");
    assert.equal(read(variant, "channelSelector"), "ABC", "non-overridden default survives");
  });

  test("returns a fresh object on each call (no shared reference)", () => {

    const a = makeStoredVariant();
    const b = makeStoredVariant();

    assert.notEqual(a, b, "two calls produce two distinct references");
  });

  test("accepts the empty overrides object explicitly", () => {

    // Boundary: passing {} explicitly should behave the same as omitting the argument.
    const explicit = makeStoredVariant({});
    const omitted = makeStoredVariant();

    assert.equal(read(explicit, "canonicalKey"), read(omitted, "canonicalKey"));
  });
});

describe("makeStoredCanonical", () => {

  test("returns a minimal ABC default when no overrides are passed", () => {

    const canonical = makeStoredCanonical();

    assert.equal(read(canonical, "name"), "ABC", "default name is 'ABC'");
    assert.equal(read(canonical, "url"), "https://www.abc.com/watch-live", "default url is the ABC live URL");
  });

  test("merges identity overrides onto the defaults", () => {

    const canonical = makeStoredCanonical({

      channelNumber: 7,
      name: "ABC Los Angeles",
      stationId: "57342"
    });

    assert.equal(read(canonical, "name"), "ABC Los Angeles", "override wins for name");
    assert.equal(read(canonical, "channelNumber"), 7, "override carries through for channelNumber");
    assert.equal(read(canonical, "stationId"), "57342", "override carries through for stationId");
    assert.equal(read(canonical, "url"), "https://www.abc.com/watch-live", "non-overridden default survives");
  });

  test("returns a fresh object on each call", () => {

    const a = makeStoredCanonical();
    const b = makeStoredCanonical();

    assert.notEqual(a, b);
  });
});

describe("makeChannelDelta", () => {

  test("returns exactly the fields passed in (no defaults)", () => {

    const delta = makeChannelDelta({ name: "ABC Custom" });

    assert.deepEqual(delta, { name: "ABC Custom" }, "no extra fields are introduced");
  });

  test("returns an empty object when given an empty override (boundary)", () => {

    // Boundary: a delta with no fields is a degenerate but legal shape.
    assert.deepEqual(makeChannelDelta({}), {});
  });

  test("preserves null fields (the explicit-clear semantic in the delta model)", () => {

    const delta = makeChannelDelta({ stationId: null });

    assert.equal(read(delta, "stationId"), null, "null survives intact - it's the clear-this-field signal");
  });

  test("returns a fresh object on each call so callers can mutate independently", () => {

    const a = makeChannelDelta({ name: "X" });
    const b = makeChannelDelta({ name: "X" });

    assert.notEqual(a, b);
  });
});

describe("makeChannel", () => {

  test("returns a minimal ResolvedChannel with neutral defaults", () => {

    const channel = makeChannel();

    assert.equal(channel.name, "Test");
    assert.equal(channel.url, "https://example.com");
  });

  test("merges overrides shallowly on top of defaults", () => {

    const channel = makeChannel({ channelNumber: 7, name: "ABC" });

    assert.equal(channel.name, "ABC");
    assert.equal(channel.channelNumber, 7);
    assert.equal(channel.url, "https://example.com", "non-overridden default survives");
  });

  test("returns a fresh object on each call (no shared reference)", () => {

    const a = makeChannel();
    const b = makeChannel();

    assert.notEqual(a, b);
  });

  test("accepts the empty overrides object explicitly", () => {

    const explicit = makeChannel({});
    const omitted = makeChannel();

    assert.deepEqual(explicit, omitted);
  });

  test("supports tags, stationId, and other identity fields via overrides", () => {

    const channel = makeChannel({ stationId: "12345", tags: [ "Sports", "News" ] });

    assert.deepEqual(channel.tags, [ "Sports", "News" ]);
    assert.equal(channel.stationId, "12345");
  });
});

describe("getCanonical", () => {

  test("returns the predefined canonical entry narrowed to CanonicalChannel", () => {

    const abc = getCanonical("abc");

    // Type-level: narrowing means we can read identity fields without a cast.
    assert.equal(abc.name, "ABC", "narrowed type exposes identity fields");
    assert.equal(abc, PREDEFINED_CHANNELS["abc"], "returns the same reference as the catalog");
  });

  test("throws when the key does not exist in the predefined catalog", () => {

    assert.throws(

      () => getCanonical("definitely-not-a-real-channel-key"),
      /no predefined channel with key/,
      "missing-key error message names the key"
    );
  });

  test("throws when the key exists but resolves to a variant", () => {

    // abc-hulu is a predefined variant - it has canonicalKey set, so the runtime guard rejects it.
    assert.throws(

      () => getCanonical("abc-hulu"),
      /resolves to a variant, not a canonical/,
      "variant-key error message explains the mismatch"
    );
  });
});
