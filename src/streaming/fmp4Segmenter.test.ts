/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fmp4Segmenter.test.ts: Unit tests for the pure helpers in the fMP4 segmenter module. The two formatters (formatKeyframeStatsSummary, formatSessionStatsSummary) are
 * pure string-builders that earn full coverage here. The two discontinuity-sequence helpers (pruneDiscontinuityIndices, computeDiscontinuitySequence) are the SSOT for
 * keeping discontinuityIndices bounded over a long stream while preserving a correct, monotonic #EXT-X-DISCONTINUITY-SEQUENCE across the prune boundary - the tests
 * assert both properties at once. createFMP4Segmenter pipes a Readable input through createMP4BoxParser, accumulates fragments, stores them via hlsSegments.storeSegment,
 * and emits playlists via hlsSegments.updatePlaylist; it is driven here with synthetic ftyp/moov/moof/mdat boxes against a registered stream, asserting init-segment
 * storage, the fast-path first cut, the segment-duration boundary, final flush, and discontinuity marking. Real Chrome-capture fMP4 remains an e2e concern only for
 * codec/timescale fidelity.
 */
import type { KeyframeStats, SessionStats } from "./fmp4Segmenter.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { computeDiscontinuitySequence, createFMP4Segmenter, formatKeyframeStatsSummary, formatSessionStatsSummary, pruneDiscontinuityIndices } from "./fmp4Segmenter.ts";
import { getInitSegment, getPlaylist, getSegment, getSegmentCount } from "./hlsSegments.ts";
import { registerStream, unregisterStream } from "./registry.ts";
import { CONFIG } from "../config/index.ts";
import { LOG } from "../utils/index.ts";
import { PassThrough } from "node:stream";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { makeRegistryEntry } from "./registry.helpers.ts";

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

  // This integrated test replays the outputSegment() prune loop and the generatePlaylist() sequence computation over a long synthetic stream, asserting two properties
  // at once: (1) discontinuityIndices stays bounded by the sliding window size, and (2) the emitted DISCONTINUITY-SEQUENCE matches an unbounded oracle at every
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

/* makeAndRegisterStream wraps the canonical makeRegistryEntry factory with registerStream so createFMP4Segmenter has a real registered stream to store init
 * segments, media segments, and playlists against - every hlsSegments.ts accessor gates on getStream(streamId) and silently no-ops for an unregistered id.
 */
function makeAndRegisterStream(): { streamId: number } {

  const entry = makeRegistryEntry();

  registerStream(entry);

  return { streamId: entry.id };
}

/* makeBox builds a minimal MP4 box: 4-byte size + 4-byte type + payload. The size includes the 8-byte header. A file-local copy of the same minimal builder
 * used by mp4Parser.fragments.test.ts, per the convention that each mp4Parser.*.test.ts (and this file) defines its own copy rather than sharing one.
 */
function makeBox(type: string, payload: Buffer = Buffer.alloc(0)): Buffer {

  const size = 8 + payload.length;
  const buf = Buffer.alloc(size);

  buf.writeUInt32BE(size, 0);
  buf.write(type, 4, 4, "ascii");
  payload.copy(buf, 8);

  return buf;
}

/* makeTfhd constructs a minimal tfhd (track fragment header) box carrying a trackId and an optional default sample duration - the two fields
 * offsetMoofTimestamps() and its trun-duration accumulation need to process a fragment without throwing.
 */
function makeTfhd(options: { defaultSampleDuration?: number; trackId: number }): Buffer {

  let flags = 0;
  const optional: number[] = [];

  if(options.defaultSampleDuration !== undefined) {

    flags |= 0x000008;
    optional.push(options.defaultSampleDuration);
  }

  // tfhd payload: 4 bytes version+flags, 4 bytes trackId, then optional fields.
  const payload = Buffer.alloc(8 + optional.length * 4);

  payload.writeUInt32BE(flags, 0);
  payload.writeUInt32BE(options.trackId, 4);

  for(let i = 0; i < optional.length; i++) {

    payload.writeUInt32BE(optional[i] ?? 0, 8 + i * 4);
  }

  return makeBox("tfhd", payload);
}

/* makeTfdt constructs a minimal tfdt (track fragment decode time) box with version 0 (32-bit baseMediaDecodeTime).
 */
function makeTfdt(baseMediaDecodeTime: number): Buffer {

  const payload = Buffer.alloc(8);

  payload.writeUInt32BE(0, 0);
  payload.writeUInt32BE(baseMediaDecodeTime, 4);

  return makeBox("tfdt", payload);
}

/* makeTrun constructs a minimal trun (track fragment run) box carrying just a sample count with no per-sample duration bit, so extractTrunTotalDuration()
 * falls back to the parent tfhd's defaultSampleDuration * sampleCount.
 */
function makeTrun(options: { sampleCount: number }): Buffer {

  const payload = Buffer.alloc(8);

  payload.writeUInt32BE(0, 0);
  payload.writeUInt32BE(options.sampleCount, 4);

  return makeBox("trun", payload);
}

/* makeTraf assembles a traf (track fragment) box from its child tfhd/tfdt/trun boxes.
 */
function makeTraf(...children: Buffer[]): Buffer {

  return makeBox("traf", Buffer.concat(children));
}

/* makeMoof assembles a moof (movie fragment) box from its traf children.
 */
function makeMoof(...trafs: Buffer[]): Buffer {

  return makeBox("moof", Buffer.concat(trafs));
}

/* makeFtyp builds a minimal ftyp box. Its payload content is irrelevant to the segmenter - only its raw bytes matter, since storeInitSegment() persists
 * ftyp+moov verbatim and the byte-identical-init suppression test below depends on feeding byte-identical ftyp+moov pairs across two segmenter instances.
 */
function makeFtyp(): Buffer {

  return makeBox("ftyp", Buffer.from("isom"));
}

/* makeMoov builds a minimal moov box with no trak children. parseMoovTrackInfo() and parseMoovCodecConfig() walk zero tracks against this payload and
 * degrade gracefully - an empty timescale map, no identified video track, and a wall-clock EXTINF fallback - exactly as they would for a malformed real moov.
 */
function makeMoov(): Buffer {

  return makeBox("moov", Buffer.alloc(0));
}

/* makeMdat builds an mdat box wrapping the given payload string as its media bytes.
 */
function makeMdat(payload: string): Buffer {

  return makeBox("mdat", Buffer.from(payload));
}

/* makeTestMoof builds a moof carrying one traf for the given track: a tfhd declaring the track and a nominal default sample duration, a tfdt at
 * baseMediaDecodeTime 0, and a one-sample trun. This is the minimal structure offsetMoofTimestamps() needs to process a fragment without throwing; none of
 * the tests below assert on the resulting media-time values, only on segment-cutting and storage behavior.
 */
function makeTestMoof(trackId = 1): Buffer {

  return makeMoof(makeTraf(makeTfhd({ defaultSampleDuration: 2000, trackId }), makeTfdt(0), makeTrun({ sampleCount: 1 })));
}

/* reportCount counts the captured info lines that announce a changed initialization on a continued capture. Reading by prefix rather than by total keeps the
 * rows that use it indifferent to any other info line the segmenter emits on the same drive.
 */
function reportCount(messages: string[]): number {

  return messages.filter((message) => message.startsWith("Capture parameters changed")).length;
}

describe("createFMP4Segmenter", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
    mock.timers.reset();
  });

  test("stores the init segment on moov and bumps the version from a fresh start (no previousInitSegment)", (t) => {

    const infos: string[] = [];

    t.mock.method(LOG, "info", (message: string) => { infos.push(message); });

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ onError, onStop, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    const ftyp = makeFtyp();
    const moov = makeMoov();

    readable.write(ftyp);
    readable.write(moov);

    const expectedInit = Buffer.concat([ ftyp, moov ]);

    assert.equal(getInitSegment(streamId)?.equals(expectedInit), true, "storeInitSegment() persisted ftyp+moov to the registry under this streamId");
    assert.equal(segmenter.getInitSegment()?.equals(expectedInit), true, "the segmenter's own getter mirrors the stored init segment");
    assert.equal(segmenter.getInitVersion(), 1, "a fresh stream (no previousInitSegment) always counts as changed and bumps the version 0 -> 1");
    assert.equal(reportCount(infos), 0, "a fresh start continues nothing, so there is no earlier initialization for it to differ from");
    assert.equal(onError.mock.calls.length, 0, "a well-formed init segment never reports an error");
  });

  test("emits the first segment via the fast path as soon as the second moof arrives, not before", () => {

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ onError, onStop, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());

    const moof1 = makeTestMoof();
    const mdat1 = makeMdat("segment-zero-media");

    readable.write(moof1);
    readable.write(mdat1);

    assert.equal(getSegmentCount(streamId), 0, "nothing is cut until a second moof signals the first fragment is complete");
    assert.equal(segmenter.getSegmentIndex(), 0);

    readable.write(makeTestMoof());

    const segment0 = getSegment(streamId, "segment0.m4s");

    assert.ok(segment0, "the fast path emitted segment0 as soon as the second moof arrived");
    assert.equal(segment0.length, moof1.length + mdat1.length, "segment0 is exactly the first moof+mdat pair, nothing more and nothing less");
    assert.equal(segmenter.getSegmentIndex(), 1);
    assert.equal(onError.mock.calls.length, 0);
  });

  test("cuts the second segment only once elapsed time reaches CONFIG.hls.segmentDuration, never before", () => {

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ onError, onStop, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());

    // Drive to segment0 via the fast path.
    readable.write(makeTestMoof());
    readable.write(makeMdat("m0"));
    readable.write(makeTestMoof());

    assert.equal(segmenter.getSegmentIndex(), 1, "segment0 emitted via the fast path");

    // Accumulate the second fragment without advancing the mocked clock. The moof that arrives here only evaluates the cut decision for the fragment already
    // sitting in the buffer - it does not itself get cut against.
    readable.write(makeMdat("m1"));
    readable.write(makeTestMoof());

    assert.equal(segmenter.getSegmentIndex(), 1, "zero elapsed time is below the segment-duration target, so no cut happens yet");

    // Advance the mocked clock to exactly the segment-duration boundary and feed the next fragment. This is the boundary case (elapsed === target) that a
    // flipped comparison (> instead of >=) would get wrong in either direction.
    mock.timers.tick(CONFIG.hls.segmentDuration * 1000);

    readable.write(makeMdat("m2"));
    readable.write(makeTestMoof());

    assert.equal(segmenter.getSegmentIndex(), 2, "elapsed time reaching the target cuts the second segment");
    assert.equal(onError.mock.calls.length, 0);
  });

  test("flushes any remaining fragment as a final segment and invokes onStop exactly once when the input stream ends", async () => {

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ onError, onStop, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("final"));

    assert.equal(segmenter.getSegmentIndex(), 0, "the lone moof+mdat pair has not been cut yet - only stream end will flush it");

    readable.end();

    // The "end" event fires on a later tick than the end() call itself, so waiting one macrotask guarantees handleEnd() has already run before we assert.
    await new Promise<void>((resolve) => setImmediate(resolve));

    assert.equal(segmenter.getSegmentIndex(), 1, "handleEnd() flushed the buffered fragment as a final segment");
    assert.ok(getSegment(streamId, "segment0.m4s"), "the final segment was stored under the registry");
    assert.equal(onStop.mock.calls.length, 1, "onStop fires exactly once at stream end");
    assert.equal(onError.mock.calls.length, 0);
  });

  test("marks the first emitted segment with a discontinuity when pendingDiscontinuity is set and there is no previous init to compare", () => {

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ onError, onStop, pendingDiscontinuity: true, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("m0"));
    readable.write(makeTestMoof());

    assert.equal(segmenter.getSegmentIndex(), 1, "segment0 emitted via the fast path");
    assert.match(getPlaylist(streamId) ?? "", /#EXT-X-DISCONTINUITY/, "the first segment after a tab replacement carries the discontinuity marker");
    assert.equal(onError.mock.calls.length, 0);
  });

  test("suppresses both the discontinuity marker and the version bump when the new init is byte-identical to the previous one", (t) => {

    const ftyp = makeFtyp();
    const moov = makeMoov();
    const init = Buffer.concat([ ftyp, moov ]);

    const infos: string[] = [];

    t.mock.method(LOG, "info", (message: string) => { infos.push(message); });

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ continuity: { previousInitSegment: init, startingInitVersion: 5 }, onError, onStop, pendingDiscontinuity: true, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(ftyp);
    readable.write(moov);
    readable.write(makeTestMoof());
    readable.write(makeMdat("m0"));
    readable.write(makeTestMoof());

    assert.equal(segmenter.getSegmentIndex(), 1, "segment0 emitted via the fast path");
    assert.equal(segmenter.getInitVersion(), 5, "a byte-identical init never bumps the version - the exact contrast to the fresh-start case above");
    assert.doesNotMatch(getPlaylist(streamId) ?? "", /#EXT-X-DISCONTINUITY/, "an unchanged init suppresses the pending discontinuity marker");
    assert.equal(reportCount(infos), 0, "and an unchanged init reports nothing, because the parameters this capture continues from are the ones it came back with");
    assert.equal(onError.mock.calls.length, 0);
  });

  test("reports a continued capture whose initialization differs from the one it continues from, once", (t) => {

    /* The field measurement, read as a count. The encoder coming back with other parameters is the event every client re-initializes on, and it is invisible in
     * the log today. The row drives a continuation whose initialization genuinely differs and demands exactly one line, alongside the effects that must still
     * follow it: the version bump the map URI is cache-busted with, and the discontinuity marker the playlist carries. The fresh-start row and the byte-identical
     * row are its controls - both assert zero.
     */
    const previousInitSegment = Buffer.concat([ makeBox("ftyp", Buffer.from("iso6")), makeMoov() ]);

    const infos: string[] = [];

    t.mock.method(LOG, "info", (message: string) => { infos.push(message); });

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ continuity: { previousInitSegment, startingInitVersion: 5 }, onError, onStop, pendingDiscontinuity: true, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("m0"));
    readable.write(makeTestMoof());

    assert.equal(reportCount(infos), 1, "a continued capture whose initialization differs is reported exactly once");
    assert.equal(segmenter.getInitVersion(), 6, "and the version bumps, so clients re-fetch the init segment through a fresh map URI");
    assert.match(getPlaylist(streamId) ?? "", /#EXT-X-DISCONTINUITY/, "and the first segment carries the discontinuity marker");
    assert.equal(onError.mock.calls.length, 0);
  });

  test("stop() detaches the input listeners so no further segments are produced, and a second call is a silent no-op", () => {

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ onError, onStop, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("m0"));
    readable.write(makeTestMoof());

    assert.equal(getSegmentCount(streamId), 1, "segment0 emitted before stop()");

    segmenter.stop();

    // Further writes must have no effect: stop() removed the "data" listener, so handleData() can never run again for this input stream.
    readable.write(makeTestMoof());
    readable.write(makeMdat("m1"));
    readable.write(makeTestMoof());

    assert.equal(getSegmentCount(streamId), 1, "stop() detached the input listeners - no further segments are produced after it runs");
    assert.doesNotThrow(() => { segmenter.stop(); }, "a second stop() call is a silent no-op, not a throw");
  });

  test("degrades gracefully on a minimal moov: a null video-track flag, a copyable timestamp map, and a fresh stats object each call", () => {

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ onError, onStop, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("segment-zero-media"));
    readable.write(makeTestMoof());

    const segment0 = getSegment(streamId, "segment0.m4s");

    assert.ok(segment0, "segment0 emitted via the fast path");
    assert.equal(segmenter.getLastSegmentSize(), segment0.length, "getLastSegmentSize mirrors the last stored segment's byte length");
    assert.equal(segmenter.getLastSegmentHasVideo(), null, "a minimal moov never identifies a video track, so the flag stays null rather than false");

    const timestamps = segmenter.getTrackTimestamps();

    assert.ok(timestamps instanceof Map);
    timestamps.set(999, 123n);
    assert.equal(segmenter.getTrackTimestamps().has(999), false, "the getter returns a fresh copy - mutating it must not leak into a subsequent call");

    const statsA = segmenter.getSessionStats();
    const statsB = segmenter.getSessionStats();

    assert.notEqual(statsA, statsB, "getSessionStats returns a new object on every call, not a shared reference");
    assert.deepEqual(statsA, statsB, "the two snapshots nonetheless carry identical field values");
    assert.equal(onError.mock.calls.length, 0);
  });

  test("markDiscontinuity flushes the pending fragment immediately and marks the next output segment with a discontinuity", () => {

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ onError, onStop, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());

    // Accumulate a lone moof with nothing to cut against yet, then flush it explicitly via markDiscontinuity() rather than waiting for a timing-based cut.
    readable.write(makeTestMoof());
    segmenter.markDiscontinuity();

    assert.equal(getSegmentCount(streamId), 1, "markDiscontinuity flushed the accumulated moof as its own segment");
    assert.equal(segmenter.getSegmentIndex(), 1);

    const originalSegmentDuration = CONFIG.hls.segmentDuration;

    CONFIG.hls.segmentDuration = 0;

    try {

      // With the target duration floored at zero, any nonnegative elapsed time satisfies the cut condition on the very next moof, letting the pending
      // discontinuity armed by markDiscontinuity() above attach to this next segment without needing to manipulate the clock.
      readable.write(makeMdat("a"));
      readable.write(makeTestMoof());
    } finally {

      CONFIG.hls.segmentDuration = originalSegmentDuration;
    }

    assert.equal(segmenter.getSegmentIndex(), 2, "the pending fragment was cut into a second segment");
    assert.match(getPlaylist(streamId) ?? "", /#EXT-X-DISCONTINUITY/, "the segment following markDiscontinuity carries the discontinuity marker");
    assert.equal(onError.mock.calls.length, 0);
  });

  test("a continuity carrying prior session statistics counts the handoff as a tab replacement", () => {

    // The presence-keyed half of the continuity contract. Statistics in hand mean a live prior segmenter is being succeeded, which is exactly what a tab
    // replacement is, so the counter advances and the summary at stream end covers the whole session rather than the last leg of it.
    const onError = mock.fn();
    const onStop = mock.fn();

    const segmenter = createFMP4Segmenter({

      continuity: { priorSessionStats: { malformedMoofCount: 2, syncSpreadCount: 0, syncSpreadMaxMs: 0, syncSpreadMinMs: 0, syncSpreadSumMs: 0,
        tabReplacementCount: 1 } },
      onError,
      onStop,
      streamId
    });

    assert.equal(segmenter.getSessionStats().tabReplacementCount, 2, "succeeding a live segmenter counts as one more replacement");
    assert.equal(segmenter.getSessionStats().malformedMoofCount, 2, "and the accumulated statistics carry across");
  });

  test("a continuity without prior session statistics is a fresh session, not a replacement", () => {

    /* The other half, and the reason every continuity member is individually optional. The resume-from-disk path holds timestamps and an index but has no
     * statistics to hand over, so it passes none - and must not be recorded as having replaced a tab. A migration that filled the gap with a zeroed statistics
     * object rather than leaving it absent would mint a phantom replacement into every resumed stream's session summary, and this row is what catches it.
     */
    const onError = mock.fn();
    const onStop = mock.fn();

    const segmenter = createFMP4Segmenter({

      continuity: { initialTrackTimestamps: new Map<number, bigint>([[ 1, 90000n ]]), startingInitVersion: 3, startingSegmentIndex: 42 },
      onError,
      onStop,
      streamId
    });

    assert.equal(segmenter.getSessionStats().tabReplacementCount, 0, "a resume is a fresh session, however much sequence state it carries");
    assert.equal(segmenter.getSegmentIndex(), 42, "while the sequence state it does carry is honoured");
    assert.equal(segmenter.getInitVersion(), 3);
  });

  test("the continuity snapshot reports what a successor needs, read live", () => {

    // The read the swap depends on. It composes the same live values the individual getters expose, so a successor seeded from it continues the exact sequence
    // the segmenter had reached at the instant of the call rather than at some earlier one.
    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ continuity: { startingInitVersion: 9, startingSegmentIndex: 17 }, onError, onStop, streamId });

    const snapshot = segmenter.getContinuitySnapshot();

    assert.equal(snapshot.startingSegmentIndex, 17);
    assert.equal(snapshot.startingInitVersion, 9);
    assert.deepEqual(snapshot.priorSessionStats, segmenter.getSessionStats(), "the snapshot's statistics are the segmenter's own");
    assert.notEqual(snapshot.priorSessionStats, segmenter.getSessionStats(), "handed over as a copy, so a successor cannot mutate this segmenter's state");
  });
});
