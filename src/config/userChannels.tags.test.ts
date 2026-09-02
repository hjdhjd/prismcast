/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.tags.test.ts: Direct unit tests for the tag canonicalization helpers - sortTags and parseTagInput. Both are exported and used as the single source
 * of truth for tag ordering and form-input parsing. Asserting their pure-function semantics here means a regression in either is caught locally rather than via
 * indirect failure in the channels normalizer or the bulk-tag handlers.
 */
import { describe, test } from "node:test";
import { parseTagInput, sortTags } from "./userChannels.ts";
import assert from "node:assert/strict";

describe("sortTags", () => {

  test("returns a new array (does not mutate the input)", () => {

    const input = [ "Sports", "News" ];
    const output = sortTags(input);

    assert.notEqual(output, input, "fresh array");
    assert.deepEqual(input, [ "Sports", "News" ], "input unchanged");
  });

  test("sorts case-insensitively (locale-aware)", () => {

    /* The sort uses localeCompare with sensitivity: "base" so "apple" and "Banana" compare correctly across cases. Alphabetical, not ASCII.
     */
    assert.deepEqual(sortTags([ "Banana", "apple", "Cherry" ]), [ "apple", "Banana", "Cherry" ]);
  });

  test("preserves the casing of each tag's actual value", () => {

    /* Sort is case-insensitive but the values themselves are returned verbatim - we order them, we don't lowercase them.
     */
    const result = sortTags([ "BBB", "aaa", "CCC" ]);

    assert.deepEqual(result, [ "aaa", "BBB", "CCC" ]);
  });

  test("accepts a Set as input (Iterable)", () => {

    const set = new Set([ "Sports", "News", "Local" ]);
    const result = sortTags(set);

    assert.deepEqual(result, [ "Local", "News", "Sports" ]);
  });

  test("accepts a generator as input (Iterable)", () => {

    function *generate(): Generator<string> {

      yield "Sports";
      yield "News";
    }

    assert.deepEqual(sortTags(generate()), [ "News", "Sports" ]);
  });

  test("returns an empty array when given an empty iterable", () => {

    assert.deepEqual(sortTags([]), []);
    assert.deepEqual(sortTags(new Set()), []);
  });

  test("preserves duplicates (does not deduplicate; that's parseTagInput's job)", () => {

    /* Documented current behavior: sortTags is purely an ordering operation. Deduplication happens upstream in parseTagInput. A future caller that needs both
     * ordering and dedup should compose the two helpers explicitly.
     */
    assert.deepEqual(sortTags([ "Sports", "Sports", "News" ]), [ "News", "Sports", "Sports" ]);
  });
});

describe("parseTagInput", () => {

  /* The form-input parser splits on commas, trims each piece, drops empties, deduplicates case-sensitively, and then sorts via sortTags. The case-sensitive dedup
   * is intentional - "Sports" and "sports" are distinct user-authored values and both survive into the array; the sort canonicalizes their order.
   */

  test("returns an empty array for empty input", () => {

    assert.deepEqual(parseTagInput(""), []);
  });

  test("returns an empty array for whitespace-only input", () => {

    /* The early return path checks `if(!raw)` which catches empty strings; whitespace-only follows through the split/trim/filter pipeline and produces an empty
     * array via the filter step.
     */
    assert.deepEqual(parseTagInput("   "), []);
  });

  test("parses a single tag", () => {

    assert.deepEqual(parseTagInput("Sports"), ["Sports"]);
  });

  test("parses multiple comma-separated tags", () => {

    assert.deepEqual(parseTagInput("Sports, News, Local"), [ "Local", "News", "Sports" ]);
  });

  test("trims each tag", () => {

    assert.deepEqual(parseTagInput("  Sports  ,  News  "), [ "News", "Sports" ]);
  });

  test("drops empty tags from successive commas", () => {

    assert.deepEqual(parseTagInput("Sports,,News"), [ "News", "Sports" ]);
  });

  test("drops trailing comma", () => {

    assert.deepEqual(parseTagInput("Sports, News,"), [ "News", "Sports" ]);
  });

  test("deduplicates case-sensitively (Sports and sports both survive as distinct)", () => {

    /* The Set + map(t.trim()) construction is case-sensitive so "Sports" and "sports" are two different tags. Both survive; sortTags orders them next to each
     * other (case-insensitive sort) but preserves both casings.
     */
    const result = parseTagInput("Sports, sports, SPORTS");

    assert.equal(result.length, 3, "all three casings survive");
    assert.deepEqual(result.toSorted(), [ "SPORTS", "Sports", "sports" ].toSorted());
  });

  test("deduplicates exact duplicates", () => {

    assert.deepEqual(parseTagInput("Sports, Sports"), ["Sports"]);
  });

  test("sorts alphabetically (case-insensitive)", () => {

    assert.deepEqual(parseTagInput("Zebra, apple, Banana"), [ "apple", "Banana", "Zebra" ]);
  });
});
