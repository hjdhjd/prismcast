/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.scope.test.ts: Direct unit tests for the small public helpers - pickIdentity (defensive copy + identity-only projection), applyChannelDelta
 * (the merge primitive used by bulk operations), and the predefined-scope filters (getPacificPredefinedKeys, getEastWithPacificPredefinedKeys). These are pure
 * functions over either their argument or the static PREDEFINED_CHANNELS map, so they are unit-testable without runtime initialization.
 */
import type { ChannelDelta, ResolvedChannel, StoredChannel } from "../types/index.ts";
import { applyChannelDelta, getEastWithPacificPredefinedKeys, getPacificPredefinedKeys, pickIdentity } from "./userChannels.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("pickIdentity", () => {

  /* The function copies the CHANNEL_IDENTITY_KEYS partition off a ResolvedChannel into a fresh object, defensively cloning the tags array so callers cannot
   * mutate the source. Used during variant resolution to strip canonical's binding while preserving identity.
   */

  test("copies the identity-only fields and drops binding fields", () => {

    const channel = {

      channelNumber: 7,
      channelSelector: "ABC",
      hdhrEnabled: true,
      logoUrl: "https://example.com/logo.png",
      name: "ABC",
      profile: "default",
      stationId: "12345",
      tags: ["Local"],
      tvgShift: 0,
      url: "https://example.com"
    } as ResolvedChannel;

    const identity = pickIdentity(channel);

    assert.equal(identity.name, "ABC", "name (identity) is copied");
    assert.equal(identity.channelNumber, 7, "channelNumber is copied");
    assert.equal(identity.stationId, "12345", "stationId is copied");
    assert.deepEqual(identity.tags, ["Local"]);
    assert.equal(identity.hdhrEnabled, true);
    assert.equal((identity as Record<string, unknown>)["url"], undefined, "url (binding) is NOT copied");
    assert.equal((identity as Record<string, unknown>)["channelSelector"], undefined, "channelSelector (binding) is NOT copied");
    assert.equal((identity as Record<string, unknown>)["profile"], undefined, "profile (binding) is NOT copied");
  });

  test("returns a fresh object", () => {

    const channel = { name: "ABC", url: "https://example.com" } as ResolvedChannel;
    const identity = pickIdentity(channel);

    assert.notEqual(identity, channel);
  });

  test("defensively copies tags so mutations on the result do not leak to the source", () => {

    /* The defensive copy via tags &&= identity.tags.slice() is the load-bearing line. A regression here would create a shared reference and downstream callers
     * mutating identity.tags (e.g., during variant overlay) would corrupt the canonical's tags array.
     */
    const original = [ "Local", "News" ];
    const channel = { name: "ABC", tags: original, url: "https://example.com" } as ResolvedChannel;
    const identity = pickIdentity(channel);

    identity.tags!.push("Mutated");
    assert.deepEqual(original, [ "Local", "News" ], "source tags array survives mutation on the result");
  });

  test("skips fields that are undefined on the source (does not write them as undefined)", () => {

    /* The for-loop continues on undefined values, so the result has no key for an absent identity field. Verify with `in` rather than equality - hasOwnProperty
     * is the precise contract.
     */
    const channel = { name: "ABC", url: "https://example.com" } as ResolvedChannel;
    const identity = pickIdentity(channel);

    assert.equal("name" in identity, true);
    assert.equal("channelNumber" in identity, false, "absent identity field is not written");
    assert.equal("stationId" in identity, false);
    assert.equal("tags" in identity, false);
  });

  test("returns an empty identity object when source has no identity fields at all", () => {

    /* A pure-binding source (e.g., a ResolvedChannel constructed from variant entries with no identity inheritance) yields an empty identity object.
     */
    const channel = { url: "https://example.com" } as ResolvedChannel;
    const identity = pickIdentity(channel);

    assert.deepEqual(identity, {});
  });

  test("preserves hdhrEnabled=false (the only non-default disable signal)", () => {

    /* hdhrEnabled has a sparse-storage convention where absent or true means enabled. The only way to disable is an explicit false. pickIdentity must preserve
     * the false value so it survives variant resolution intact.
     */
    const channel = { hdhrEnabled: false, name: "ABC", url: "https://example.com" } as ResolvedChannel;
    const identity = pickIdentity(channel);

    assert.equal(identity.hdhrEnabled, false);
  });
});

describe("applyChannelDelta", () => {

  test("returns a fresh object with stored fields preserved and delta fields layered on top", () => {

    const stored = { name: "Old Name", url: "https://old.example.com" } as StoredChannel;
    const delta: ChannelDelta = { name: "New Name" };
    const result = applyChannelDelta(stored, delta);

    assert.equal((result as Record<string, unknown>)["name"], "New Name", "delta wins");
    assert.equal((result as Record<string, unknown>)["url"], "https://old.example.com", "stored field that delta did not touch survives");
    assert.notEqual(result, stored, "fresh reference");
  });

  test("uses an empty stored entry when undefined is passed (no-prior-overrides case)", () => {

    /* The "no entry exists yet" branch: callers like the bulk-tag handler pass undefined when the channel has no prior user override. The function returns just
     * the delta verbatim.
     */
    const result = applyChannelDelta(undefined, { tags: ["Sports"] });

    assert.deepEqual(result, { tags: ["Sports"] });
  });

  test("preserves null delta fields (null-as-clear semantic flows through unchanged)", () => {

    /* Null is the "clear this field" signal; the merge function does not interpret it - it just copies the null forward so the storage normalizer can decide
     * whether to keep or strip it.
     */
    const stored = { name: "ABC", stationId: "12345" } as StoredChannel;
    const result = applyChannelDelta(stored, { stationId: null });

    assert.equal((result as Record<string, unknown>)["stationId"], null);
  });

  test("returns an empty object when both stored is undefined and delta is empty", () => {

    /* Edge case: no prior data and no changes. The function still produces a fresh object so callers can safely assign it to data.channels[key] without worrying
     * about shared references.
     */
    const result = applyChannelDelta(undefined, {});

    assert.deepEqual(result, {});
  });
});

describe("getPacificPredefinedKeys / getEastWithPacificPredefinedKeys", () => {

  /* The two helpers produce disjoint subsets of the predefined catalog: Pacific keys (ending in "p" with an East counterpart) and East keys (NOT ending in "p"
   * with a Pacific counterpart). Both filter via filterPredefinedKeysByTimezone internally and skip service variants.
   */

  test("getPacificPredefinedKeys returns only Pacific-suffixed canonicals whose East counterpart exists", () => {

    const pacific = getPacificPredefinedKeys();

    // Every key in the result must end in "p" (the Pacific naming convention).
    for(const key of pacific) {

      assert.equal(key.endsWith("p"), true, "Pacific key '" + key + "' must end in 'p'");
    }

    // bravop is a known Pacific canonical with bravo as its East counterpart.
    assert.ok(pacific.includes("bravop"), "bravop should be in the Pacific list");
  });

  test("getEastWithPacificPredefinedKeys returns East canonicals that have Pacific counterparts", () => {

    const east = getEastWithPacificPredefinedKeys();

    // Every key in the result must NOT end in "p".
    for(const key of east) {

      assert.equal(key.endsWith("p"), false, "East key '" + key + "' must not end in 'p'");
    }

    // bravo is a known East canonical with bravop as its Pacific counterpart.
    assert.ok(east.includes("bravo"), "bravo should be in the East list");
  });

  test("the two lists are disjoint", () => {

    const pacific = new Set(getPacificPredefinedKeys());
    const east = new Set(getEastWithPacificPredefinedKeys());
    const intersection = pacific.intersection(east);

    assert.equal(intersection.size, 0, "no key appears in both lists");
  });

  test("returns sorted output", () => {

    const pacific = getPacificPredefinedKeys();
    const east = getEastWithPacificPredefinedKeys();

    assert.deepEqual(pacific, [...pacific].toSorted(), "Pacific list is sorted");
    assert.deepEqual(east, [...east].toSorted(), "East list is sorted");
  });
});
