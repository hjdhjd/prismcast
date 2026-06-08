/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fmp4Segmenter.test.ts: Unit tests for the pure helpers in the fMP4 segmenter module. The two formatters (formatKeyframeStatsSummary, formatSessionStatsSummary) are
 * pure string-builders that earn full coverage here. The two discontinuity-sequence helpers (pruneDiscontinuityIndices, computeDiscontinuitySequence) are the SSOT for
 * keeping discontinuityIndices bounded over a long stream while preserving a correct, monotonic #EXT-X-DISCONTINUITY-SEQUENCE across the prune boundary - the tests pin
 * both invariants. createFMP4Segmenter pipes a Readable input through createMP4BoxParser, accumulates fragments, stores them via hlsSegments.storeSegment, and emits
 * playlists via hlsSegments.updatePlaylist; its happy path requires real fMP4 fixtures from a Chrome MediaRecorder capture and is deferred to e2e.
 */
import type { KeyframeStats, SessionStats } from "./fmp4Segmenter.ts";
import { computeDiscontinuitySequence, formatKeyframeStatsSummary, formatSessionStatsSummary, pruneDiscontinuityIndices } from "./fmp4Segmenter.ts";
import { describe, test } from "node:test";
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

describe("pruneDiscontinuityIndices", () => {

  test("removes indices strictly below the threshold and returns the count removed", () => {

    // Indices 0, 5, 10 are below the threshold of 12; 12 and 20 are kept (12 is not strictly below).
    const indices = new Set<number>([ 0, 5, 10, 12, 20 ]);

    const removed = pruneDiscontinuityIndices(indices, 12);

    assert.equal(removed, 3);
    assert.deepEqual([...indices].sort((a, b) => a - b), [ 12, 20 ]);
  });

  test("is a no-op returning zero when no index falls below the threshold", () => {

    const indices = new Set<number>([ 30, 31, 99 ]);

    assert.equal(pruneDiscontinuityIndices(indices, 30), 0);
    assert.equal(indices.size, 3);
  });

  test("empties the set and returns the full count when every index is below the threshold", () => {

    const indices = new Set<number>([ 1, 2, 3 ]);

    assert.equal(pruneDiscontinuityIndices(indices, 100), 3);
    assert.equal(indices.size, 0);
  });
});

describe("computeDiscontinuitySequence", () => {

  test("returns undefined when the stream has no discontinuity history", () => {

    // No tracked and no pruned discontinuities means the tag must be omitted entirely - the undefined contract signals that to the playlist builder.
    assert.equal(computeDiscontinuitySequence({ discontinuityIndices: new Set(), prunedDiscontinuityCount: 0, startIndex: 50 }), undefined);
  });

  test("returns 0 when discontinuities exist but none have scrolled below the window start", () => {

    // Discontinuities exist in the window, so the tag is emitted, but its value is 0 because none precede the window start. This is distinct from undefined.
    assert.equal(computeDiscontinuitySequence({ discontinuityIndices: new Set([ 12, 18 ]), prunedDiscontinuityCount: 0, startIndex: 10 }), 0);
  });

  test("counts only tracked indices strictly below the window start", () => {

    // Indices 3 and 8 precede startIndex 10; 10 and 14 do not (10 is not strictly below).
    assert.equal(computeDiscontinuitySequence({ discontinuityIndices: new Set([ 3, 8, 10, 14 ]), prunedDiscontinuityCount: 0, startIndex: 10 }), 2);
  });

  test("adds the pruned count to the tracked-below-start count", () => {

    // Five discontinuities already scrolled off and were pruned; two more are tracked below the window start - the sequence is the sum, 7.
    assert.equal(computeDiscontinuitySequence({ discontinuityIndices: new Set([ 30, 33, 90 ]), prunedDiscontinuityCount: 5, startIndex: 40 }), 7);
  });

  test("emits a value (not undefined) when only pruned discontinuities remain in the history", () => {

    // The set is empty but discontinuities have been pruned, so the history is non-empty and the tag must still be emitted with the pruned count.
    assert.equal(computeDiscontinuitySequence({ discontinuityIndices: new Set(), prunedDiscontinuityCount: 4, startIndex: 200 }), 4);
  });
});

describe("discontinuity-sequence bounded growth and prune-boundary correctness", () => {

  // This integrated test replays the outputSegment() prune loop and the generatePlaylist() sequence computation over a long synthetic stream, asserting two invariants
  // simultaneously: (1) discontinuityIndices stays bounded by the sliding window size, and (2) the emitted DISCONTINUITY-SEQUENCE matches an unbounded oracle at every
  // step - including across the prune boundary where indices begin scrolling out of the set. The oracle reproduces the original unbounded behavior (a full set counted
  // with idx < startIndex), so any divergence after pruning would surface immediately.
  test("stays bounded while reproducing the unbounded discontinuity-sequence oracle at every step", () => {

    const maxSegments = 6;
    const totalSegments = 500;

    // A discontinuity is recorded on every fourth segment, dense enough to keep entries flowing through the window and across the prune boundary repeatedly.
    const discontinuityEvery = 4;

    // Bounded production state mirrors SegmenterState: a pruned set plus a running pruned counter.
    const boundedIndices = new Set<number>();

    let prunedDiscontinuityCount = 0;

    // Oracle state mirrors the original unbounded implementation: a set that is never pruned.
    const oracleIndices = new Set<number>();

    // The maximum size discontinuityIndices ever reaches under bounded pruning. A correct prune keeps this at or below the window span.
    let maxBoundedSize = 0;

    for(let segmentIndex = 0; segmentIndex < totalSegments; segmentIndex++) {

      // Record a discontinuity at this index in both the bounded set and the unbounded oracle.
      if((segmentIndex % discontinuityEvery) === 0) {

        boundedIndices.add(segmentIndex);
        oracleIndices.add(segmentIndex);
      }

      // Advance to the next index exactly as outputSegment() does after storing a segment, then prune to the window floor.
      const nextSegmentIndex = segmentIndex + 1;
      const pruneThreshold = Math.max(0, nextSegmentIndex - maxSegments);

      prunedDiscontinuityCount += pruneDiscontinuityIndices(boundedIndices, pruneThreshold);

      if(boundedIndices.size > maxBoundedSize) {

        maxBoundedSize = boundedIndices.size;
      }

      // Compute the window start the same way generatePlaylist() does for a mature stream (realSegmentCount >= maxSegments), which equals the prune threshold. This is
      // the boundary case the fix must get right: startIndex never dips below the prune threshold, so pruned indices are always strictly below startIndex.
      const startIndex = Math.max(0, nextSegmentIndex - maxSegments);

      const bounded = computeDiscontinuitySequence({ discontinuityIndices: boundedIndices, prunedDiscontinuityCount, startIndex });

      // The oracle: the original unbounded computation - undefined when no discontinuity history exists, otherwise the count of all indices below startIndex.
      let oracle: number | undefined;

      if(oracleIndices.size > 0) {

        let count = 0;

        for(const idx of oracleIndices) {

          if(idx < startIndex) {

            count++;
          }
        }

        oracle = count;
      }

      assert.equal(bounded, oracle, "bounded sequence must equal the unbounded oracle at segment " + String(segmentIndex));
    }

    // Bounded growth: a correct prune never lets the set exceed the number of indices that can coexist within one window span. With a discontinuity every fourth
    // segment and a six-segment window, at most two indices are ever resident, far below the 125 a never-pruned set would accumulate.
    assert.ok(maxBoundedSize <= maxSegments, "discontinuityIndices must stay bounded by the window span, saw " + String(maxBoundedSize));
    assert.equal(oracleIndices.size, Math.ceil(totalSegments / discontinuityEvery), "oracle accumulated every discontinuity, confirming the unbounded baseline");

    // The pruned counter must have absorbed every discontinuity that scrolled out, leaving only the still-resident ones in the bounded set.
    assert.equal(prunedDiscontinuityCount + boundedIndices.size, oracleIndices.size, "pruned count plus resident indices must equal the total discontinuity history");
  });
});
