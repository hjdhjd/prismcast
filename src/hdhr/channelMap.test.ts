/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelMap.test.ts: Unit tests for the HDHomeRun channel-number mapping. The module exposes two surfaces - buildChannelMap (rebuild on every call) and
 * getChannelKeyByNumber (lookup) - both of which derive their state from getAllChannels(). The tests exercise the live predefined channel set (no in-test
 * fixtures because the resolver state lives behind a closed module boundary), asserting the structural rules the assignment algorithm must hold: monotonic
 * sort, unique numbers, AUTO_ASSIGN_START floor, deterministic alphabetical ordering on auto-assigned keys, and the name-fallback contract (channel.name ?? key).
 */
import { buildChannelMap, getChannelKeyByNumber } from "./channelMap.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { firstOf } from "../testing.helpers.ts";
import { getAllChannels } from "../config/userChannels.ts";

// AUTO_ASSIGN_START is a private constant in channelMap.ts. The tests duplicate the value here rather than re-export it because the constant defines the
// public contract (numbers >= 1000 are auto-assigned) which would lose meaning if the test could rename it freely.
const AUTO_ASSIGN_START = 1000;

describe("buildChannelMap", () => {

  test("returns an array of entries with the expected shape", () => {

    const map = buildChannelMap();

    assert.ok(Array.isArray(map), "result is an array");
    assert.ok(map.length > 0, "predefined channels populate the map");

    for(const entry of map) {

      assert.equal(typeof entry.key, "string", "key is a string");
      assert.equal(typeof entry.name, "string", "name is a string");
      assert.equal(typeof entry.number, "number", "number is a number");
      assert.ok(Number.isInteger(entry.number), "number is an integer");
    }
  });

  test("sorts entries by ascending channel number", () => {

    const map = buildChannelMap();

    for(let i = 1; i < map.length; i++) {

      const prev = map[i - 1]!.number;
      const curr = map[i]!.number;

      assert.ok(prev <= curr, "entry " + String(i - 1) + " (" + String(prev) + ") sorts before entry " + String(i) + " (" + String(curr) + ")");
    }
  });

  test("emits unique channel numbers across all entries", () => {

    // Auto-assignment is guaranteed to skip explicit numbers; explicit assignments are a Set. Together the contract is: no two entries share a number. Locking
    // this catches a regression where the auto-assignment loop forgets to advance past an explicit collision.
    const map = buildChannelMap();
    const numbers = new Set(map.map((e) => e.number));

    assert.equal(numbers.size, map.length, "every channel number is unique");
  });

  test("uses numbers at or above AUTO_ASSIGN_START when no explicit assignments exist", () => {

    // The current predefined catalog has no explicit channelNumber values, so every entry must come from the auto-assignment path. The first auto-assigned
    // number is AUTO_ASSIGN_START (1000) and the rest follow sequentially.
    const map = buildChannelMap();

    if(map.length === 0) {

      return;
    }

    assert.ok(map.every((e) => e.number >= AUTO_ASSIGN_START), "every number is >= " + String(AUTO_ASSIGN_START));
  });

  test("auto-assigned numbers form a contiguous sequence starting at AUTO_ASSIGN_START", () => {

    // No predefined channel currently sets an explicit number, so the entire output is contiguous: 1000, 1001, 1002, ... up to (1000 + count - 1). A future
    // predefined-channel addition with explicit channelNumber would invalidate this assertion - that is the intent. The lock surfaces such additions for review.
    const map = buildChannelMap();

    for(let i = 0; i < map.length; i++) {

      assert.equal(map[i]!.number, AUTO_ASSIGN_START + i, "entry " + String(i) + " has the expected sequential number");
    }
  });

  test("orders auto-assigned channels alphabetically by key", () => {

    // The algorithm sorts unassigned entries by key.localeCompare before assigning numbers. Since every predefined channel currently lacks an explicit number,
    // the resulting number-sorted output equals the key-sorted input. Locking this guards against accidental sort-key changes (e.g., switching to name).
    const map = buildChannelMap();
    const keys = map.map((e) => e.key);
    const sortedKeys = keys.toSorted((a, b) => a.localeCompare(b));

    assert.deepEqual(keys, sortedKeys, "keys appear in alphabetical order");
  });

  test("includes every HDHR-enabled channel from getAllChannels", () => {

    // Every channel that getAllChannels returns and that is HDHR-enabled must appear exactly once in the map. The predefined set has no hdhrEnabled=false
    // entries today, so the count must equal the source count.
    const all = getAllChannels();
    const expectedCount = Object.values(all).filter((c) => c.hdhrEnabled !== false).length;
    const map = buildChannelMap();

    assert.equal(map.length, expectedCount, "map size matches the HDHR-enabled subset of getAllChannels");
  });

  test("falls back to the key when a channel has no name", () => {

    // The implementation uses channel.name ?? key in two places (explicit and auto-assignment paths). Variant channels in the predefined set commonly lack
    // identity (they inherit from canonical), but resolution layers it back in - so name should be set on the resolved view. We still verify the fallback
    // contract by walking the live data: every entry has a non-empty name, and entries whose source channel has no name use the key.
    const all = getAllChannels();
    const map = buildChannelMap();

    for(const entry of map) {

      assert.ok(entry.name.length > 0, "name is non-empty for " + entry.key);

      const source = all[entry.key];

      if(source && (source.name === undefined)) {

        assert.equal(entry.name, entry.key, "missing-name source falls back to key for " + entry.key);
      }
    }
  });

  test("returns a fresh result on each call (no mutation leakage)", () => {

    // The function rebuilds from the channel map on each call; no caching. Mutating the returned array must not affect subsequent calls.
    const a = buildChannelMap();

    a.push({ key: "fake", name: "Fake", number: 999_999 });

    const b = buildChannelMap();

    assert.notEqual(b.length, a.length, "second call returns the original count, not the mutated length");
    assert.ok(!b.some((e) => e.key === "fake"), "second call does not contain the injected entry");
  });

  test("propagates the stationId field from the source channel onto the entry", () => {

    // The resolver does not synthesize stationIds, so any stationId on a map entry must equal the value from getAllChannels. We confirm by walking the live
    // map and cross-checking each entry against its source.
    const all = getAllChannels();
    const map = buildChannelMap();

    for(const entry of map) {

      const source = all[entry.key];

      assert.ok(source, "every entry corresponds to a source channel");
      assert.equal(entry.stationId, source.stationId, "stationId mirrors the source for " + entry.key);
    }
  });
});

describe("getChannelKeyByNumber", () => {

  test("returns the key for a number that exists in the map", () => {

    // Round-trip: pick the first map entry, look up by its number, expect its key.
    const map = buildChannelMap();
    const first = firstOf(map, "buildChannelMap returns at least one entry");

    assert.equal(getChannelKeyByNumber(first.number), first.key);
  });

  test("returns undefined for a number that is not in the map", () => {

    // Boundary: a number well below AUTO_ASSIGN_START can never be assigned by the auto path and (in the current predefined set) is not used by any explicit
    // assignment, so the lookup returns undefined.
    assert.equal(getChannelKeyByNumber(1), undefined);
  });

  test("returns undefined for a number above the assigned range", () => {

    // Boundary: a number larger than any entry's number falls off the end of the sorted list.
    const map = buildChannelMap();
    const last = map[map.length - 1];

    assert.ok(last, "map has at least one entry");
    assert.equal(getChannelKeyByNumber(last.number + 1_000_000), undefined);
  });

  test("returns undefined for a negative number", () => {

    assert.equal(getChannelKeyByNumber(-1), undefined);
  });

  test("round-trips correctly across a sample of map entries", () => {

    // Sampling across the map (front, middle, back) catches any partial-array bug in the lookup that might pass for the edge cases alone.
    const map = buildChannelMap();

    if(map.length < 3) {

      return;
    }

    const samples = [ map[0]!, map[Math.floor(map.length / 2)]!, map[map.length - 1]! ];

    for(const entry of samples) {

      assert.equal(getChannelKeyByNumber(entry.number), entry.key, "lookup matches for " + entry.key);
    }
  });
});
