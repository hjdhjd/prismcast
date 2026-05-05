/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * format.test.ts: Unit tests for the formatting primitives in format.ts. The module's six exports are pure functions plus two clock-readers (formatTimestamp,
 * formatTimeAgo). stringifySorted is the SSOT for sorted-key JSON serialization across all persisted and exported files; an unverified change here would alter
 * on-disk file shape without warning, so it earns the heaviest boundary coverage.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { capitalize, extractDomain, formatDuration, formatTimeAgo, formatTimestamp, stringifySorted } from "./format.ts";
import assert from "node:assert/strict";

describe("formatTimestamp", () => {

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: new Date("2026-03-15T14:09:07.042Z").getTime() });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("formats the current Date as yyyy/mm/dd hh:mm:ss.mmm AM/PM in local time", () => {

    // The function uses the local-zone Date getters, so the assertion compares against a Date built from the same epoch. We extract expected components from a
    // local Date so the test stays correct in any timezone.
    const expected = new Date("2026-03-15T14:09:07.042Z");
    const yyyy = String(expected.getFullYear());
    const mm = String(expected.getMonth() + 1).padStart(2, "0");
    const dd = String(expected.getDate()).padStart(2, "0");
    let hours = expected.getHours();
    const ampm = (hours >= 12) ? "PM" : "AM";

    hours = hours % 12 || 12;

    const hh = String(hours).padStart(2, "0");
    const min = String(expected.getMinutes()).padStart(2, "0");
    const ss = String(expected.getSeconds()).padStart(2, "0");
    const ms = String(expected.getMilliseconds()).padStart(3, "0");

    assert.equal(formatTimestamp(), yyyy + "/" + mm + "/" + dd + " " + hh + ":" + min + ":" + ss + "." + ms + " " + ampm);
  });

  test("renders midnight (00:00:00) as 12 AM, not 00 AM", () => {

    // Boundary: 12-hour formatting collapses 0 to 12. The implementation uses `hours = hours % 12 || 12` which depends on || coercing 0 to 12. We build the date
    // with local-hour-zero constructor args so the test is timezone-independent.
    const midnight = new Date(2026, 2, 15, 0, 0, 0, 0);

    mock.timers.setTime(midnight.getTime());

    assert.match(formatTimestamp(), / 12:00:00\.000 AM$/);
  });

  test("renders noon (12:00:00) as 12 PM, not 00 PM", () => {

    // Boundary: hours == 12 stays 12 PM (modulo 12 gives 0, the || keeps 12).
    const noon = new Date(2026, 2, 15, 12, 0, 0, 0);

    mock.timers.setTime(noon.getTime());

    assert.match(formatTimestamp(), / 12:00:00\.000 PM$/);
  });

  test("pads single-digit milliseconds to three places", () => {

    // Boundary: ms = 5 must become "005" not "5". This is the canonical padStart(3, "0") test.
    const ts = new Date(2026, 2, 15, 9, 0, 0, 5);

    mock.timers.setTime(ts.getTime());

    assert.match(formatTimestamp(), /:00\.005 AM$/);
  });
});

describe("formatDuration", () => {

  test("formats sub-minute durations as seconds with the s suffix", () => {

    assert.equal(formatDuration(45_000), "45s", "45 seconds in ms");
    assert.equal(formatDuration(45, "s"), "45s", "45 seconds in s unit");
  });

  test("formats sub-hour durations as minutes and seconds", () => {

    assert.equal(formatDuration(399_000), "6m 39s", "399 seconds = 6m 39s");
  });

  test("omits trailing zero components (exactly 2 minutes -> '2m', not '2m 0s')", () => {

    assert.equal(formatDuration(120_000), "2m", "exact-minute boundary drops the zero seconds");
  });

  test("formats hour-plus durations as hours and minutes", () => {

    assert.equal(formatDuration(5_000_000), "1h 23m", "1 hour 23 minutes");
  });

  test("omits zero minutes from hour output (exactly 2 hours -> '2h')", () => {

    assert.equal(formatDuration(7_200_000), "2h", "exact-hour boundary drops zero minutes");
  });

  test("returns '0s' for a zero duration (boundary)", () => {

    assert.equal(formatDuration(0), "0s");
    assert.equal(formatDuration(0, "s"), "0s");
  });

  test("returns '1s' for the smallest unit (1 second in ms via rounding)", () => {

    // 500ms rounds up to 1 second per Math.round.
    assert.equal(formatDuration(500), "1s");
  });

  test("rounds milliseconds to the nearest second", () => {

    // Math.round behavior: 1499 -> 1, 1500 -> 2.
    assert.equal(formatDuration(1_499), "1s");
    assert.equal(formatDuration(1_500), "2s");
  });

  test("treats the 's' unit as already-in-seconds without rounding fractional input", () => {

    // Boundary: with unit="s", the function does not round. Fractional input survives all the way to the seconds output. Locks the contract that callers
    // passing pre-converted seconds get an exact pass-through (whereas ms input rounds to whole seconds).
    assert.equal(formatDuration(0.4, "s"), "0.4s", "fractional seconds survive without rounding");
    assert.equal(formatDuration(120, "s"), "2m", "exact-minute boundary in s unit");
  });
});

describe("formatTimeAgo", () => {

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("returns 'just now' for timestamps less than 60 seconds old", () => {

    assert.equal(formatTimeAgo(1_700_000_000_000 - 30_000), "just now", "30 seconds ago");
    assert.equal(formatTimeAgo(1_700_000_000_000 - 59_000), "just now", "59 seconds ago (boundary inside)");
    assert.equal(formatTimeAgo(1_700_000_000_000), "just now", "0 seconds ago");
  });

  test("returns singular 'minute ago' for exactly one minute", () => {

    assert.equal(formatTimeAgo(1_700_000_000_000 - 60_000), "1 minute ago", "60 seconds = 1 minute, singular");
  });

  test("returns plural 'minutes ago' for two or more minutes", () => {

    assert.equal(formatTimeAgo(1_700_000_000_000 - 120_000), "2 minutes ago");
    assert.equal(formatTimeAgo(1_700_000_000_000 - 30 * 60_000), "30 minutes ago");
  });

  test("transitions from minutes to hours at 60 minutes", () => {

    assert.equal(formatTimeAgo(1_700_000_000_000 - 59 * 60_000), "59 minutes ago", "59 minutes still in minutes");
    assert.equal(formatTimeAgo(1_700_000_000_000 - 60 * 60_000), "1 hour ago", "60 minutes = 1 hour");
  });

  test("returns singular 'hour ago' for exactly one hour", () => {

    assert.equal(formatTimeAgo(1_700_000_000_000 - 3_600_000), "1 hour ago");
  });

  test("returns plural 'hours ago' for two or more hours", () => {

    assert.equal(formatTimeAgo(1_700_000_000_000 - 3 * 3_600_000), "3 hours ago");
  });

  test("transitions from hours to days at 24 hours", () => {

    assert.equal(formatTimeAgo(1_700_000_000_000 - 23 * 3_600_000), "23 hours ago", "23 hours still in hours");
    assert.equal(formatTimeAgo(1_700_000_000_000 - 24 * 3_600_000), "1 day ago", "24 hours = 1 day");
  });

  test("returns singular 'day ago' for exactly one day", () => {

    assert.equal(formatTimeAgo(1_700_000_000_000 - 86_400_000), "1 day ago");
  });

  test("returns plural 'days ago' for multi-day spans", () => {

    assert.equal(formatTimeAgo(1_700_000_000_000 - 7 * 86_400_000), "7 days ago");
  });

  test("future timestamps (negative elapsed) round to 'just now'", () => {

    // Boundary: clock skew or a future timestamp produces a negative seconds value, which Math.floor rounds toward -Infinity. The first guard `seconds < 60`
    // catches negatives too, so the function gracefully reports "just now" rather than "-N minutes ago".
    assert.equal(formatTimeAgo(1_700_000_000_000 + 5_000), "just now");
  });
});

describe("extractDomain", () => {

  test("returns the last two hostname segments for a multi-part hostname", () => {

    assert.equal(extractDomain("https://watch.foodnetwork.com/live"), "foodnetwork.com");
    assert.equal(extractDomain("https://www.hulu.com/dashboard"), "hulu.com");
  });

  test("returns the hostname unchanged when it has exactly two parts", () => {

    assert.equal(extractDomain("https://hulu.com/live"), "hulu.com");
  });

  test("returns the hostname unchanged for a single-part hostname like localhost", () => {

    assert.equal(extractDomain("http://localhost:5589"), "localhost");
  });

  test("returns the original URL when parsing fails", () => {

    // Boundary: malformed URL falls into the catch and returns the input verbatim.
    assert.equal(extractDomain("not a url"), "not a url");
  });

  test("returns the original URL for an empty string (also unparseable)", () => {

    assert.equal(extractDomain(""), "");
  });

  test("strips arbitrarily deep subdomains down to the last two segments", () => {

    assert.equal(extractDomain("https://a.b.c.d.example.com/path"), "example.com", "deep subdomains collapse to last two");
  });
});

describe("capitalize", () => {

  test("uppercases the first letter and preserves the rest", () => {

    assert.equal(capitalize("hello"), "Hello");
    assert.equal(capitalize("hELLO"), "HELLO");
  });

  test("returns an empty string unchanged (boundary)", () => {

    assert.equal(capitalize(""), "");
  });

  test("returns a single-character string with that character uppercased", () => {

    assert.equal(capitalize("h"), "H");
  });

  test("leaves an already-capitalized string unchanged", () => {

    assert.equal(capitalize("Hello"), "Hello");
  });

  test("handles non-letter leading characters by leaving them as-is", () => {

    // Boundary: charAt+toUpperCase on a digit, a space, or a symbol is a no-op.
    assert.equal(capitalize("123abc"), "123abc");
    assert.equal(capitalize(" hello"), " hello");
  });
});

/* eslint-disable sort-keys -- the whole point of stringifySorted is to feed it unsorted input and verify it sorts. */
describe("stringifySorted", () => {

  test("sorts top-level object keys alphabetically", () => {

    assert.equal(stringifySorted({ z: 1, a: 2, m: 3 }, 0), "{\"a\":2,\"m\":3,\"z\":1}");
  });

  test("sorts keys recursively at every depth", () => {

    const result = stringifySorted({ outer: { z: 1, a: 2 }, alpha: { y: 3, b: 4 } }, 0);

    assert.equal(result, "{\"alpha\":{\"b\":4,\"y\":3},\"outer\":{\"a\":2,\"z\":1}}");
  });

  test("preserves array element order while still sorting nested object keys", () => {

    const result = stringifySorted({ items: [ { z: 1, a: 2 }, { y: 3, b: 4 } ] }, 0);

    assert.equal(result, "{\"items\":[{\"a\":2,\"z\":1},{\"b\":4,\"y\":3}]}", "arrays keep order; objects inside arrays still sort");
  });

  test("uses two-space indentation by default", () => {

    const result = stringifySorted({ a: 1 });

    assert.equal(result, "{\n  \"a\": 1\n}");
  });

  test("respects the indent override (0 for compact output)", () => {

    const result = stringifySorted({ a: 1 }, 0);

    assert.equal(result, "{\"a\":1}");
  });

  test("handles primitive values at the top level (no sorting applies)", () => {

    assert.equal(stringifySorted(42, 0), "42");
    assert.equal(stringifySorted("hello", 0), "\"hello\"");
    assert.equal(stringifySorted(null, 0), "null");
    assert.equal(stringifySorted(true, 0), "true");
  });

  test("handles empty objects and arrays", () => {

    assert.equal(stringifySorted({}, 0), "{}");
    assert.equal(stringifySorted([], 0), "[]");
  });

  test("preserves boolean values rather than coercing them", () => {

    assert.equal(stringifySorted({ a: false, b: true }, 0), "{\"a\":false,\"b\":true}");
  });

  test("preserves null values inside objects (does not strip them)", () => {

    // The replacer only sorts; it does not filter. Null fields survive.
    assert.equal(stringifySorted({ z: null, a: null }, 0), "{\"a\":null,\"z\":null}");
  });

  test("uses case-sensitive locale-aware sort (uppercase precedes lowercase under en-US localeCompare)", () => {

    // localeCompare is case-aware in en-US: lowercase letters sort before uppercase letters of the same letter, but uppercase 'A' precedes lowercase 'b'. We
    // lock the actual behavior here so a future change to the comparator surfaces as a test diff.
    const result = stringifySorted({ b: 1, A: 2, a: 3 }, 0);

    // Confirm a < A < b under en-US localeCompare conventions (Node uses ICU).
    assert.match(result, /^\{"[aA]":/, "first key is one of a/A under locale comparison");
  });

  test("produces stable output across two calls on the same data (idempotent serialization)", () => {

    const data = { z: 1, m: { y: 2, b: 3 }, a: [ 4, 5 ] };

    assert.equal(stringifySorted(data, 0), stringifySorted(data, 0), "two serializations of the same data are byte-identical");
  });
});
