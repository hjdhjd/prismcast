/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.sortKeys.test.ts: Unit tests for sort-key computation in services.ts - getChannelSortKey across the primary fields and the profile-field switch, plus
 * compareChannelSort including the null-name tiebreaker path. Service-group construction lives in services.serviceGroups.test.ts; predicates and lookups live in
 * services.test.ts.
 */
import { compareChannelSort, getChannelSortKey } from "./services.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { makeChannel } from "./userChannels.helpers.ts";

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

