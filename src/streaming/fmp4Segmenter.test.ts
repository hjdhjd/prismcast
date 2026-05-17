/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fmp4Segmenter.test.ts: Unit tests for the formatter helpers in the fMP4 segmenter module. fmp4Segmenter.ts has three exports - createFMP4Segmenter,
 * formatKeyframeStatsSummary, and formatSessionStatsSummary. The two formatters are pure string-builders that earn full coverage here. createFMP4Segmenter pipes a
 * Readable input through createMP4BoxParser, accumulates fragments, stores them via hlsSegments.storeSegment, and emits playlists via hlsSegments.updatePlaylist;
 * its happy path requires real fMP4 fixtures from a Chrome MediaRecorder capture and is deferred to e2e.
 */
import type { KeyframeStats, SessionStats } from "./fmp4Segmenter.ts";
import { describe, test } from "node:test";
import { formatKeyframeStatsSummary, formatSessionStatsSummary } from "./fmp4Segmenter.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* makeKeyframeStats builds a KeyframeStats literal with sensible zeros. Tests override only the fields they assert on.
 */
function makeKeyframeStats(overrides: Partial<KeyframeStats> = {}): KeyframeStats {

  return {

    averageKeyframeIntervalMs: 0,
    indeterminateCount: 0,
    keyframeCount: 0,
    maxKeyframeIntervalMs: 0,
    minKeyframeIntervalMs: 0,
    nonKeyframeCount: 0,
    segmentsWithoutLeadingKeyframe: 0,
    ...overrides
  };
}

/* makeSessionStats builds a SessionStats literal with sensible zeros. Tests override only the fields they assert on.
 */
function makeSessionStats(overrides: Partial<SessionStats> = {}): SessionStats {

  return {

    malformedMoofCount: 0,
    syncSpreadCount: 0,
    syncSpreadMaxMs: 0,
    syncSpreadMinMs: 0,
    syncSpreadSumMs: 0,
    tabReplacementCount: 0,
    ...overrides
  };
}

describe("formatKeyframeStatsSummary", () => {

  test("returns the empty string when no moof boxes were processed", () => {

    // Boundary: zero total moofs short-circuits before formatting. The empty-string contract lets the lifecycle log builder skip the keyframe summary entirely.
    assert.equal(formatKeyframeStatsSummary(makeKeyframeStats()), "");
  });

  test("formats the canonical 100% keyframe case with interval statistics", () => {

    const stats = makeKeyframeStats({


      averageKeyframeIntervalMs: 2_000,
      keyframeCount: 2_490,
      maxKeyframeIntervalMs: 2_100,
      minKeyframeIntervalMs: 1_900
    });

    assert.equal(formatKeyframeStatsSummary(stats), "Keyframes: 2490 of 2490 moofs (100.0%), interval 1.9-2.1s avg 2.0s.");
  });

  test("formats the partial-keyframe case with non-keyframe segments noted", () => {

    const stats = makeKeyframeStats({


      averageKeyframeIntervalMs: 3_100,
      keyframeCount: 85,
      maxKeyframeIntervalMs: 12_400,
      minKeyframeIntervalMs: 1_800,
      nonKeyframeCount: 113,
      segmentsWithoutLeadingKeyframe: 5
    });

    assert.equal(formatKeyframeStatsSummary(stats), "Keyframes: 85 of 198 moofs (42.9%), interval 1.8-12.4s avg 3.1s, 5 segments without leading keyframe.");
  });

  test("uses singular 'segment' when only one segment lacks a leading keyframe", () => {

    // Boundary: pluralization branch.
    const stats = makeKeyframeStats({


      averageKeyframeIntervalMs: 2_000,
      keyframeCount: 99,
      maxKeyframeIntervalMs: 2_100,
      minKeyframeIntervalMs: 1_900,
      segmentsWithoutLeadingKeyframe: 1
    });

    assert.match(formatKeyframeStatsSummary(stats), /1 segment without leading keyframe\.$/);
  });

  test("omits interval statistics when fewer than 2 keyframes were detected", () => {

    // Boundary: a single keyframe doesn't yield a meaningful min/max/avg interval. The formatter must skip the interval suffix in that case.
    const stats = makeKeyframeStats({


      keyframeCount: 1,
      nonKeyframeCount: 5
    });

    const summary = formatKeyframeStatsSummary(stats);

    assert.match(summary, /Keyframes: 1 of 6 moofs/);
    assert.doesNotMatch(summary, /interval/, "no interval phrase when keyframeCount < 2");
  });

  test("counts indeterminate moofs in the totalMoofs denominator", () => {

    // The total includes keyframe + nonKeyframe + indeterminate. Locks the inclusion contract so percentage math reflects all observed boxes.
    const stats = makeKeyframeStats({


      indeterminateCount: 10,
      keyframeCount: 0,
      nonKeyframeCount: 0
    });

    const summary = formatKeyframeStatsSummary(stats);

    assert.match(summary, /Keyframes: 0 of 10 moofs/, "indeterminate moofs included in the total");
  });
});

describe("formatSessionStatsSummary", () => {

  test("returns the empty string when no sync measurements have been recorded", () => {

    // Boundary: zero syncSpreadCount short-circuits before formatting.
    assert.equal(formatSessionStatsSummary(makeSessionStats(), 0), "");
  });

  test("formats the canonical no-events session", () => {

    const stats = makeSessionStats({


      syncSpreadCount: 100,
      syncSpreadMaxMs: 25.7,
      syncSpreadMinMs: 0.7,
      syncSpreadSumMs: 1_200
    });

    assert.equal(formatSessionStatsSummary(stats, 1_725), "Session: 1725 segments, A-V sync: mean 12.0ms, min 0.7ms, max 25.7ms.");
  });

  test("appends tab replacement count when present (singular form)", () => {

    const stats = makeSessionStats({


      syncSpreadCount: 50,
      syncSpreadMaxMs: 24.3,
      syncSpreadMinMs: 1.7,
      syncSpreadSumMs: 525,
      tabReplacementCount: 1
    });

    assert.match(formatSessionStatsSummary(stats, 485), /1 tab replacement\.$/);
  });

  test("appends tab replacement count with plural 's' for multiple replacements", () => {

    const stats = makeSessionStats({


      syncSpreadCount: 50,
      syncSpreadMaxMs: 24.3,
      syncSpreadMinMs: 1.7,
      syncSpreadSumMs: 525,
      tabReplacementCount: 2
    });

    assert.match(formatSessionStatsSummary(stats, 485), /2 tab replacements\.$/);
  });

  test("appends malformed moof count when present (singular form)", () => {

    const stats = makeSessionStats({


      malformedMoofCount: 1,
      syncSpreadCount: 30,
      syncSpreadMaxMs: 30.1,
      syncSpreadMinMs: 2.0,
      syncSpreadSumMs: 456
    });

    assert.match(formatSessionStatsSummary(stats, 100), /1 malformed moof\.$/);
  });

  test("appends malformed moof count with plural 's' for multiple", () => {

    const stats = makeSessionStats({


      malformedMoofCount: 3,
      syncSpreadCount: 30,
      syncSpreadMaxMs: 30.1,
      syncSpreadMinMs: 2.0,
      syncSpreadSumMs: 456
    });

    assert.match(formatSessionStatsSummary(stats, 100), /3 malformed moofs\.$/);
  });

  test("composes both tab replacement and malformed moof segments together", () => {

    const stats = makeSessionStats({


      malformedMoofCount: 3,
      syncSpreadCount: 30,
      syncSpreadMaxMs: 30.1,
      syncSpreadMinMs: 2.0,
      syncSpreadSumMs: 456,
      tabReplacementCount: 1
    });

    const summary = formatSessionStatsSummary(stats, 100);

    assert.match(summary, /1 tab replacement,/);
    assert.match(summary, /3 malformed moofs\.$/);
  });

  test("computes mean A-V sync as syncSpreadSumMs / syncSpreadCount with one decimal", () => {

    // Boundary: the mean computation uses .toFixed(1). 33.45 / 3 = 11.15 -> "11.2" via toFixed rounding.
    const stats = makeSessionStats({


      syncSpreadCount: 3,
      syncSpreadMaxMs: 20,
      syncSpreadMinMs: 5,
      syncSpreadSumMs: 33.45
    });

    assert.match(formatSessionStatsSummary(stats, 1), /mean 11\.2ms/);
  });
});
