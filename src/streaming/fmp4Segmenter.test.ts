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
import { getInitSegment, getPlaylist, getSegment, getSegmentCount, storeSegment } from "./hlsSegments.ts";
import { getStream, registerStream, unregisterStream } from "./registry.ts";
import { CONFIG } from "../config/index.ts";
import { LOG } from "../utils/index.ts";
import { PassThrough } from "node:stream";
import { TestClock } from "homebridge-plugin-utils/testing";
import assert from "node:assert/strict";
import { buildResumeContinuity } from "./hls.ts";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { makeRegistryEntry } from "./registry.helpers.ts";
import { makeStreamSettings } from "../config/streamSettings.helpers.ts";

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


      averageKeyframeIntervalMs: 2000,
      keyframeCount: 2490,
      maxKeyframeIntervalMs: 2100,
      minKeyframeIntervalMs: 1900
    });

    assert.equal(formatKeyframeStatsSummary(stats), "Keyframes: 2490 of 2490 moofs (100.0%), interval 1.9-2.1s avg 2.0s.");
  });

  test("formats the partial-keyframe case with non-keyframe segments noted", () => {

    const stats = makeKeyframeStats({


      averageKeyframeIntervalMs: 3100,
      keyframeCount: 85,
      maxKeyframeIntervalMs: 12400,
      minKeyframeIntervalMs: 1800,
      nonKeyframeCount: 113,
      segmentsWithoutLeadingKeyframe: 5
    });

    assert.equal(formatKeyframeStatsSummary(stats), "Keyframes: 85 of 198 moofs (42.9%), interval 1.8-12.4s avg 3.1s, 5 segments without leading keyframe.");
  });

  test("uses singular 'segment' when only one segment lacks a leading keyframe", () => {

    // Boundary: pluralization branch.
    const stats = makeKeyframeStats({


      averageKeyframeIntervalMs: 2000,
      keyframeCount: 99,
      maxKeyframeIntervalMs: 2100,
      minKeyframeIntervalMs: 1900,
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
      syncSpreadSumMs: 1200
    });

    assert.equal(formatSessionStatsSummary(stats, 1725), "Session: 1725 segments, A-V sync: mean 12.0ms, min 0.7ms, max 25.7ms.");
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

/**
 * One playlist entry as the builder emitted it.
 */
interface ListedEntry {

  // The tags the builder emitted directly above the URL, in order.
  readonly tags: readonly string[];

  // The entry's URL.
  readonly url: string;
}

/* listedEntries reads a segmenter playlist into its entries. The entries begin after the playlist's initial map, which every segmenter playlist carries, so a row
 * reads each entry's own marker, map, date-time and duration rather than counting lines.
 */
function listedEntries(playlist: string): ListedEntry[] {

  const lines = playlist.split("\n");
  const entries: ListedEntry[] = [];
  let tags: string[] = [];

  for(const line of lines.slice(lines.findIndex((candidate) => candidate.startsWith("#EXT-X-MAP:")) + 1)) {

    if(line.length === 0) {

      continue;
    }

    if(line.startsWith("#")) {

      tags.push(line);

      continue;
    }

    entries.push({ tags, url: line });
    tags = [];
  }

  return entries;
}

describe("createFMP4Segmenter", () => {

  let streamId: number;

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
  });

  test("stores the init segment on moov and bumps the version from a fresh start (no previousInitSegment)", (t) => {

    const infos: string[] = [];

    t.mock.method(LOG, "info", (message: string) => { infos.push(message); });

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ onError, onStop, segmentDuration: makeStreamSettings().segmentDuration, streamId });
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
    const segmenter = createFMP4Segmenter({ onError, onStop, segmentDuration: makeStreamSettings().segmentDuration, streamId });
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

  test("cuts the second segment only once elapsed time reaches the segment duration it was constructed with, never before", () => {

    const clock = new TestClock(1700000000000);
    const onError = mock.fn();
    const onStop = mock.fn();
    const { segmentDuration } = makeStreamSettings();
    const segmenter = createFMP4Segmenter({ clock, onError, onStop, segmentDuration, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());

    // Drive to segment0 via the fast path.
    readable.write(makeTestMoof());
    readable.write(makeMdat("m0"));
    readable.write(makeTestMoof());

    assert.equal(segmenter.getSegmentIndex(), 1, "segment0 emitted via the fast path");

    // Accumulate the second fragment without advancing the injected clock. The moof that arrives here only evaluates the cut decision for the fragment already
    // sitting in the buffer - it does not itself get cut against.
    readable.write(makeMdat("m1"));
    readable.write(makeTestMoof());

    assert.equal(segmenter.getSegmentIndex(), 1, "zero elapsed time is below the segment-duration target, so no cut happens yet");

    // Advance the injected clock to exactly the segment-duration boundary and feed the next fragment. This is the boundary case (elapsed === target) that a
    // flipped comparison (> instead of >=) would get wrong in either direction.
    clock.advance(segmentDuration * 1000);

    readable.write(makeMdat("m2"));
    readable.write(makeTestMoof());

    assert.equal(segmenter.getSegmentIndex(), 2, "elapsed time reaching the target cuts the second segment");
    assert.equal(onError.mock.calls.length, 0);
  });

  test("flushes any remaining fragment as a final segment and invokes onStop exactly once when the input stream ends", async () => {

    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ onError, onStop, segmentDuration: makeStreamSettings().segmentDuration, streamId });
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
    const segmenter = createFMP4Segmenter({ onError, onStop, pendingDiscontinuity: true, segmentDuration: makeStreamSettings().segmentDuration, streamId });
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
    const segmenter = createFMP4Segmenter({

      continuity: { previousInitSegment: init, startingInitVersion: 5 },
      onError,
      onStop,
      pendingDiscontinuity: true,
      segmentDuration: makeStreamSettings().segmentDuration,
      streamId
    });
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
    const segmenter = createFMP4Segmenter({

      continuity: { previousInitSegment, startingInitVersion: 5 },
      onError,
      onStop,
      pendingDiscontinuity: true,
      segmentDuration: makeStreamSettings().segmentDuration,
      streamId
    });
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
    const segmenter = createFMP4Segmenter({ onError, onStop, segmentDuration: makeStreamSettings().segmentDuration, streamId });
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
    const segmenter = createFMP4Segmenter({ onError, onStop, segmentDuration: makeStreamSettings().segmentDuration, streamId });
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

    const clock = new TestClock(1700000000000);
    const onError = mock.fn();
    const onStop = mock.fn();
    const { segmentDuration } = makeStreamSettings();
    const segmenter = createFMP4Segmenter({ clock, onError, onStop, segmentDuration, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());

    // Accumulate a lone moof with nothing to cut against yet, then flush it explicitly via markDiscontinuity() rather than waiting for a timing-based cut.
    readable.write(makeTestMoof());
    segmenter.markDiscontinuity();

    assert.equal(getSegmentCount(streamId), 1, "markDiscontinuity flushed the accumulated moof as its own segment");
    assert.equal(segmenter.getSegmentIndex(), 1);

    // Advance the injected clock by the constructed duration, so the next moof satisfies the cut condition and the pending discontinuity armed by
    // markDiscontinuity() above attaches to the segment it cuts.
    clock.advance(segmentDuration * 1000);

    readable.write(makeMdat("a"));
    readable.write(makeTestMoof());

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
      segmentDuration: makeStreamSettings().segmentDuration,
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
      segmentDuration: makeStreamSettings().segmentDuration,
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
    const segmenter = createFMP4Segmenter({

      continuity: { startingInitVersion: 9, startingSegmentIndex: 17 },
      onError,
      onStop,
      segmentDuration: makeStreamSettings().segmentDuration,
      streamId
    });

    const snapshot = segmenter.getContinuitySnapshot();

    assert.equal(snapshot.startingSegmentIndex, 17);
    assert.equal(snapshot.startingInitVersion, 9);
    assert.deepEqual(snapshot.priorSessionStats, segmenter.getSessionStats(), "the snapshot's statistics are the segmenter's own");
    assert.notEqual(snapshot.priorSessionStats, segmenter.getSessionStats(), "handed over as a copy, so a successor cannot mutate this segmenter's state");
  });

  test("the snapshot's segment history and the discontinuity count read the segments produced, on top of the history a segmenter was seeded with", () => {

    /* A segmenter seeded with a pruned count and no listed segment produces two segments, the second marked. Its history reports each segment's measured duration
     * and instant from the injected clock and the marker it tracks, and its discontinuity count adds the marker it tracks to the count it was seeded with, so a
     * count that read one term alone would fall short of every marker the stream has emitted.
     */
    const clock = new TestClock(1700000000000);
    const { segmentDuration } = makeStreamSettings();
    const startedAt = clock.now();
    const segmenter = createFMP4Segmenter({

      clock,
      continuity: { priorSegmentHistory: { discontinuityIndices: new Set(), prunedDiscontinuityCount: 2, segmentDurations: new Map(), segmentTimestamps: new Map() },
        startingSegmentIndex: 30 },
      onError: mock.fn(),
      onStop: mock.fn(),
      segmentDuration,
      streamId
    });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("m30"));

    // Flush the first segment half a second in, which arms the marker for the next one, then cut the marked segment once the duration has elapsed.
    clock.advance(500);
    segmenter.markDiscontinuity();

    const firstAt = clock.now();

    readable.write(makeTestMoof());
    readable.write(makeMdat("m31"));
    clock.advance(segmentDuration * 1000);
    readable.write(makeTestMoof());

    const secondAt = clock.now();

    assert.equal(segmenter.getSegmentIndex(), 32, "precondition: the segmenter produced its two segments from its starting index");
    assert.deepEqual(segmenter.getContinuitySnapshot().priorSegmentHistory, {

      discontinuityIndices: new Set([31]),
      prunedDiscontinuityCount: 2,
      segmentDurations: new Map([ [ 30, (firstAt - startedAt) / 1000 ], [ 31, (secondAt - firstAt) / 1000 ] ]),
      segmentTimestamps: new Map([ [ 30, firstAt ], [ 31, secondAt ] ])
    }, "the history holds the measured durations, the instants and the marker, beside the seeded count");
    assert.equal(segmenter.getDiscontinuityCount(), 3, "the seeded count plus the marker the segmenter tracks");
  });

  test("a segmenter declares and cuts at the duration it was constructed with while the running configuration holds another", () => {

    /* The negative control for the stream's settings. The running configuration is assigned a duration the segmenter was not constructed with, so a segmenter
     * that read the configuration at its playlist or at its cut would declare a target duration of 4 and hold its second segment until 4 seconds elapsed.
     * The fast-path first segment is the only entry in the window when the target is read, and its wall-clock duration floors at a tenth of a second, so the
     * declared target is the floor the segmenter was given.
     */
    const clock = new TestClock(1700000000000);
    const onError = mock.fn();
    const onStop = mock.fn();
    const { segmentDuration } = makeStreamSettings({ segmentDuration: 1 });
    const originalSegmentDuration = CONFIG.hls.segmentDuration;

    CONFIG.hls.segmentDuration = 4;

    try {

      const segmenter = createFMP4Segmenter({ clock, onError, onStop, segmentDuration, streamId });
      const readable = new PassThrough();

      segmenter.pipe(readable);

      readable.write(makeFtyp());
      readable.write(makeMoov());
      readable.write(makeTestMoof());
      readable.write(makeMdat("m0"));
      readable.write(makeTestMoof());

      assert.equal(segmenter.getSegmentIndex(), 1, "precondition: segment0 emitted via the fast path and is the window's only entry");
      assert.match(getPlaylist(streamId) ?? "", /^#EXT-X-TARGETDURATION:1$/m, "the playlist declares the constructed duration, not the running configuration's");

      readable.write(makeMdat("m1"));
      clock.advance(segmentDuration * 1000);
      readable.write(makeTestMoof());

      assert.equal(segmenter.getSegmentIndex(), 2, "the second segment is cut once the constructed duration has elapsed, not the running configuration's");
      assert.equal(onError.mock.calls.length, 0);
    } finally {

      CONFIG.hls.segmentDuration = originalSegmentDuration;
    }
  });

  test("a continuing segmenter lists earlier stored segments it holds no measured duration for at the duration it was constructed with", () => {

    /* A segmenter continuing at a starting index lists the stream's earlier segments, which live on the stream rather than the segmenter, and a segmenter with no
     * measured duration for one lists it at its own segment duration. The running configuration holds another duration, so a segmenter that fell back to the
     * configuration would list those segments at 7 seconds.
     */
    const onError = mock.fn();
    const onStop = mock.fn();
    const { segmentDuration } = makeStreamSettings({ segmentDuration: 3 });
    const originalSegmentDuration = CONFIG.hls.segmentDuration;

    for(const index of [ 0, 1, 2 ]) {

      storeSegment(streamId, "segment" + String(index) + ".m4s", Buffer.from("earlier-" + String(index)));
    }

    CONFIG.hls.segmentDuration = 7;

    try {

      const segmenter = createFMP4Segmenter({ continuity: { startingSegmentIndex: 3 }, onError, onStop, segmentDuration, streamId });
      const readable = new PassThrough();

      segmenter.pipe(readable);

      readable.write(makeFtyp());
      readable.write(makeMoov());
      readable.write(makeTestMoof());
      readable.write(makeMdat("m3"));
      readable.write(makeTestMoof());

      const lines = (getPlaylist(streamId) ?? "").split("\n");

      assert.equal(segmenter.getSegmentIndex(), 4, "precondition: the continuing segmenter produced its first segment at its starting index");

      for(const index of [ 0, 1, 2 ]) {

        const urlLine = lines.indexOf("segment" + String(index) + ".m4s");

        assert.ok(urlLine > 0, "earlier segment " + String(index) + " is in the window");
        assert.equal(lines[urlLine - 1], "#EXTINF:" + segmentDuration.toFixed(3) + ",", "earlier segment " + String(index) + " is listed at the constructed duration");
      }

      assert.equal(onError.mock.calls.length, 0);
    } finally {

      CONFIG.hls.segmentDuration = originalSegmentDuration;
    }
  });

  test("a successor lists its predecessor's segments at their measured durations, date-times and markers, and continues the discontinuity sequence", () => {

    /* The tab replacement's handoff. The predecessor cuts a segment 600 ms past its duration, flushes on markDiscontinuity, and cuts a marked segment a second
     * past it; the successor, built from the predecessor's snapshot on a byte-identical init, lists the long and the marked segment from the predecessor's
     * measurements: the durations and the instants the injected clock gave them, the marker, and the discontinuity sequence it implies. A successor handed no
     * history lists them at the nominal duration with no date-time, no marker and no sequence tag.
     */
    const clock = new TestClock(1700000000000);
    const { segmentDuration } = makeStreamSettings();
    const ftyp = makeFtyp();
    const moov = makeMoov();
    const predecessor = createFMP4Segmenter({ clock, onError: mock.fn(), onStop: mock.fn(), segmentDuration, streamId });
    const before = new PassThrough();

    predecessor.pipe(before);

    before.write(ftyp);
    before.write(moov);
    before.write(makeTestMoof());
    before.write(makeMdat("m0"));
    before.write(makeTestMoof());
    before.write(makeMdat("m1"));
    clock.advance((segmentDuration * 1000) + 600);
    before.write(makeTestMoof());

    const longAt = clock.now();

    predecessor.markDiscontinuity();
    before.write(makeTestMoof());
    before.write(makeMdat("m3"));
    clock.advance((segmentDuration * 1000) + 1000);
    before.write(makeTestMoof());

    const markedAt = clock.now();

    assert.equal(predecessor.getSegmentIndex(), 4, "precondition: the fast-path segment, the long one, the flush and the marked one");

    const continuity = predecessor.getContinuitySnapshot();

    predecessor.stop();

    const successor = createFMP4Segmenter({ clock, continuity, onError: mock.fn(), onStop: mock.fn(), pendingDiscontinuity: true, segmentDuration, streamId });
    const after = new PassThrough();

    successor.pipe(after);

    after.write(ftyp);
    after.write(moov);
    after.write(makeTestMoof());
    after.write(makeMdat("m4"));
    after.write(makeTestMoof());

    const playlist = getPlaylist(streamId) ?? "";
    const entries = listedEntries(playlist);
    const long = entries.find((entry) => entry.url === "segment1.m4s");
    const marked = entries.find((entry) => entry.url === "segment3.m4s");

    assert.equal(successor.getSegmentIndex(), 5, "precondition: the successor produced its first segment");
    assert.deepEqual(long?.tags, [ "#EXT-X-PROGRAM-DATE-TIME:" + new Date(longAt).toISOString(), "#EXTINF:" + (segmentDuration + 0.6).toFixed(3) + "," ],
      "the long segment keeps its measured duration and the instant it was produced");
    assert.deepEqual(marked?.tags, [ "#EXT-X-DISCONTINUITY", "#EXT-X-MAP:URI=\"init.mp4?v=" + String(successor.getInitVersion()) + "\"",
      "#EXT-X-PROGRAM-DATE-TIME:" + new Date(markedAt).toISOString(), "#EXTINF:" + (segmentDuration + 1).toFixed(3) + "," ],
    "the marked segment keeps its marker, its instant and its measured duration, its map naming the successor's init");
    assert.match(playlist, /^#EXT-X-DISCONTINUITY-SEQUENCE:0$/m, "the sequence continues from the predecessor's markers, none of them off the front yet");
  });

  test("a successor handed a pruned count reports it as its discontinuity sequence", () => {

    // The history's pruned count is what the sequence counts for markers already off the front, so a successor seeded with 2 and no marker in its window reports
    // 2 where a successor with no history would omit the tag.
    const segmenter = createFMP4Segmenter({

      continuity: { priorSegmentHistory: { discontinuityIndices: new Set(), prunedDiscontinuityCount: 2, segmentDurations: new Map(), segmentTimestamps: new Map() },
        startingSegmentIndex: 7 },
      onError: mock.fn(),
      onStop: mock.fn(),
      segmentDuration: makeStreamSettings().segmentDuration,
      streamId
    });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("m7"));
    readable.write(makeTestMoof());

    assert.equal(segmenter.getSegmentIndex(), 8, "precondition: the successor produced its first segment");
    assert.match(getPlaylist(streamId) ?? "", /^#EXT-X-DISCONTINUITY-SEQUENCE:2$/m, "the carried pruned count is the sequence");
  });

  test("a successor prunes carried entries below its window exactly as its own, folding a pruned marker into the count", () => {

    /* The successor starts at index 20 with carried entries at 5 and 15, each a duration, an instant and a marker. Its first segment moves the prune floor past
     * 5 and not past 15, so its own snapshot drops index 5 and folds its marker into the pruned count, and keeps index 15's carried values and marker by value.
     */
    const carriedAt = 1700000000000;
    const segmenter = createFMP4Segmenter({

      continuity: {

        priorSegmentHistory: {

          discontinuityIndices: new Set([ 5, 15 ]),
          prunedDiscontinuityCount: 1,
          segmentDurations: new Map([ [ 5, 2.25 ], [ 15, 2.75 ] ]),
          segmentTimestamps: new Map([ [ 5, carriedAt ], [ 15, carriedAt + 20000 ] ])
        },
        startingSegmentIndex: 20
      },
      onError: mock.fn(),
      onStop: mock.fn(),
      segmentDuration: makeStreamSettings().segmentDuration,
      streamId
    });
    const readable = new PassThrough();
    const floor = 21 - CONFIG.hls.maxSegments;

    assert.ok((floor > 5) && (floor <= 15), "precondition: the window's prune floor after one segment falls between the carried indices");

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("m20"));
    readable.write(makeTestMoof());

    const history = segmenter.getContinuitySnapshot().priorSegmentHistory;

    assert.equal(segmenter.getSegmentIndex(), 21, "precondition: the successor produced its first segment");
    assert.ok(history, "the snapshot carries the history");
    assert.equal(history.segmentDurations.has(5), false, "index 5's duration is pruned");
    assert.equal(history.segmentTimestamps.has(5), false, "and its instant");
    assert.equal(history.discontinuityIndices.has(5), false, "and its marker");
    assert.equal(history.segmentDurations.get(15), 2.75, "index 15 keeps its carried duration");
    assert.equal(history.segmentTimestamps.get(15), carriedAt + 20000, "and its carried instant");
    assert.equal(history.discontinuityIndices.has(15), true, "and its marker");
    assert.equal(history.prunedDiscontinuityCount, 2, "the pruned marker folds into the carried count");
  });

  test("a snapshot holds copies, untouched by the marker and the segment the segmenter goes on to produce", () => {

    /* A snapshot taken before the segmenter is marked and produces its next segment must hold no trace of the marker or the segment, because a successor seeded
     * from it would otherwise list a segment its predecessor produced after the swap. Fewer segments than the window holds are produced, so nothing is pruned and
     * the only way a snapshot could gain the new index is by sharing the segmenter's own collections.
     */
    const clock = new TestClock(1700000000000);
    const { segmentDuration } = makeStreamSettings();
    const segmenter = createFMP4Segmenter({ clock, onError: mock.fn(), onStop: mock.fn(), segmentDuration, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("m0"));
    readable.write(makeTestMoof());

    const earlier = segmenter.getContinuitySnapshot().priorSegmentHistory;

    segmenter.markDiscontinuity();
    readable.write(makeTestMoof());
    readable.write(makeMdat("m2"));
    clock.advance(segmentDuration * 1000);
    readable.write(makeTestMoof());

    const later = segmenter.getContinuitySnapshot().priorSegmentHistory;

    assert.equal(segmenter.getSegmentIndex(), 3, "precondition: the segmenter produced the flush and the marked segment after the earlier snapshot");
    assert.ok(segmenter.getSegmentIndex() < CONFIG.hls.maxSegments, "precondition: nothing has been pruned");
    assert.ok(earlier, "the earlier snapshot carries a history");
    assert.ok(later, "and so does the later one");
    assert.equal(earlier.discontinuityIndices.has(2), false, "the earlier snapshot holds no marker for the new segment");
    assert.equal(earlier.segmentDurations.has(2), false, "nor its duration");
    assert.equal(earlier.segmentTimestamps.has(2), false, "nor its instant");
    assert.equal(later.discontinuityIndices.has(2), true, "while the segmenter's next snapshot holds the marker");
    assert.equal(later.segmentDurations.has(2), true, "and the duration");
    assert.equal(later.segmentTimestamps.has(2), true, "and the instant");
  });

  test("a resumed segmenter behind an active preroll marks the preroll's first entry by index and counts it once it leaves the window", () => {

    /* A resume at index 0 and a persisted count of 3 behind a preroll of 2. The segmenter's history holds the marker its preroll playlist put on the preroll's
     * first entry, so its composite lists that marker on the preroll entry and the preroll-to-real marker on its first segment, and reports the persisted count.
     * Once enough real segments push the window past the preroll's first index, that marker is counted in the sequence and only the real segment's remains.
     * Preroll durations fall back to 2 seconds, so the row needs no preroll variant.
     */
    const clock = new TestClock(1700000000000);
    const { segmentDuration } = makeStreamSettings();
    const stream = getStream(streamId);

    assert.ok(stream, "precondition: the stream is registered");
    stream.hls.prerollStartTime = clock.now();

    const segmenter = createFMP4Segmenter({

      clock,
      continuity: buildResumeContinuity({ prerollSegmentCount: 2, registeredPosition: { discontinuityCount: 3, segmentIndex: 0 }, resumeData: null }),
      onError: mock.fn(),
      onStop: mock.fn(),
      pendingDiscontinuity: true,
      prerollBaseUrl: "http://127.0.0.1:1",
      prerollCodec: "h264",
      prerollSegmentCount: 2,
      segmentDuration,
      streamId
    });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("m2"));
    readable.write(makeTestMoof());

    const first = getPlaylist(streamId) ?? "";
    const firstEntries = listedEntries(first);

    assert.equal(segmenter.getSegmentIndex(), 3, "precondition: the first real segment follows the preroll's indices");
    assert.deepEqual(firstEntries.map((entry) => entry.url), [ "http://127.0.0.1:1/preroll/h264/segment0.m4s", "http://127.0.0.1:1/preroll/h264/segment1.m4s",
      "segment2.m4s" ], "the window lists the preroll from its first index, then the real segment");
    assert.deepEqual(firstEntries.map((entry) => entry.tags.includes("#EXT-X-DISCONTINUITY")), [ true, false, true ],
      "the preroll's first entry carries the resume's marker and the real segment the preroll-to-real one, two in all");
    assert.match(first, /^#EXT-X-DISCONTINUITY-SEQUENCE:3$/m, "the persisted count, with no marker off the front yet");

    // Cut the real segments at indices 3 to 10, which moves the window start past the preroll's first index.
    for(const index of [ 3, 4, 5, 6, 7, 8, 9, 10 ]) {

      readable.write(makeMdat("m" + String(index)));
      clock.advance(segmentDuration * 1000);
      readable.write(makeTestMoof());
    }

    const later = getPlaylist(streamId) ?? "";
    const laterEntries = listedEntries(later);

    assert.equal(segmenter.getSegmentIndex(), 11, "precondition: nine real segments follow the preroll");
    assert.match(later, /^#EXT-X-MEDIA-SEQUENCE:1$/m, "the window starts past the preroll's first index");
    assert.deepEqual(laterEntries.filter((entry) => entry.tags.includes("#EXT-X-DISCONTINUITY")).map((entry) => entry.url), ["segment2.m4s"],
      "only the real segment's marker remains in the window");
    assert.match(later, /^#EXT-X-DISCONTINUITY-SEQUENCE:4$/m, "and the resume's marker is counted once it has left");
  });

  test("a segmenter resumed far into a sequence lists its preroll at the media sequence the preroll playlist served, and no segment it never produced", () => {

    /* A resume at index 500 and a persisted count of 3 behind a preroll of 15, under the default window of 10. The preroll occupies indices 500 to 514, so after
     * the first real segment the window starts where the preroll cap, counted from the preroll's first index, puts it: the preroll's last entries, then the real
     * segment at 515. A window that placed the preroll at index 0 would start at 506 and list indices 506 to 514 as real segments the stream never produced.
     */
    const clock = new TestClock(1700000000000);
    const stream = getStream(streamId);

    assert.ok(stream, "precondition: the stream is registered");
    assert.equal(CONFIG.hls.maxSegments, 10, "precondition: the default window");
    stream.hls.prerollStartTime = clock.now();

    const segmenter = createFMP4Segmenter({

      clock,
      continuity: buildResumeContinuity({ prerollSegmentCount: 15, registeredPosition: { discontinuityCount: 3, segmentIndex: 500 }, resumeData: null }),
      onError: mock.fn(),
      onStop: mock.fn(),
      pendingDiscontinuity: true,
      prerollBaseUrl: "http://127.0.0.1:1",
      prerollCodec: "h264",
      prerollSegmentCount: 15,
      segmentDuration: makeStreamSettings().segmentDuration,
      streamId
    });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());
    readable.write(makeTestMoof());
    readable.write(makeMdat("m515"));
    readable.write(makeTestMoof());

    const playlist = getPlaylist(streamId) ?? "";
    const entries = listedEntries(playlist);

    assert.equal(segmenter.getSegmentIndex(), 516, "precondition: the first real segment follows the preroll's indices");
    assert.match(playlist, /^#EXT-X-MEDIA-SEQUENCE:512$/m, "the window starts at the preroll cap counted from the preroll's first index");
    assert.match(playlist, /^#EXT-X-DISCONTINUITY-SEQUENCE:4$/m, "the persisted count plus the resume's marker, which is off the front");
    assert.deepEqual(entries.map((entry) => entry.url), [ "http://127.0.0.1:1/preroll/h264/segment12.m4s", "http://127.0.0.1:1/preroll/h264/segment13.m4s",
      "http://127.0.0.1:1/preroll/h264/segment14.m4s", "segment515.m4s" ], "the preroll's last entries, then the first real segment, and nothing between");
    assert.equal(entries.at(-1)?.tags[0], "#EXT-X-DISCONTINUITY", "the real segment opens with the preroll-to-real marker");
  });
});
