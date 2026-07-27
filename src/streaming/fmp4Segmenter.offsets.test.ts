/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fmp4Segmenter.offsets.test.ts: Per-track PTS-offset application tests for the fMP4 segmenter's moof handler. The segmenter rewrites each traf's tfdt
 * baseMediaDecodeTime by a constant per-track offset so Chrome's real-content PTS continues from the preroll/resume timeline. The offset for a track is finalized the
 * first moof that track appears in; the risk this file pins is double application, where a moof mixing an already-offset track with a newly initializing track re-walks
 * the whole moof and offsets the known track twice. These tests drive createFMP4Segmenter with synthetic ftyp/moov/moof/mdat boxes and a nonzero initialTrackTimestamps
 * basis, then parse the emitted segment bytes and assert each track's tfdt carries exactly one offset application.
 *
 * The moov is deliberately trackless (zero trak children), so state.trackTimescales stays empty and the offset computation engages the initialValue-minus-originalTfdt
 * formula rather than the tab-replacement normalized-reference path - the expected tfdt values are derived from that engaged formula. The box builders are file-local
 * copies per the convention that each mp4Parser/fmp4Segmenter test file defines its own minimal builders rather than sharing one.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { registerStream, unregisterStream } from "./registry.ts";
import { PassThrough } from "node:stream";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { createFMP4Segmenter } from "./fmp4Segmenter.ts";
import { getSegment } from "./hlsSegments.ts";
import { makeRegistryEntry } from "./registry.helpers.ts";
import { offsetMoofTimestamps } from "./mp4Parser.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* makeBox builds a minimal MP4 box: 4-byte size + 4-byte type + payload. The size includes the 8-byte header.
 */
function makeBox(type: string, payload: Buffer = Buffer.alloc(0)): Buffer {

  const size = 8 + payload.length;
  const buf = Buffer.alloc(size);

  buf.writeUInt32BE(size, 0);
  buf.write(type, 4, 4, "ascii");
  payload.copy(buf, 8);

  return buf;
}

/* makeTfhd constructs a minimal tfhd (track fragment header) box carrying a trackId and a default sample duration.
 */
function makeTfhd(options: { defaultSampleDuration?: number; trackId: number }): Buffer {

  let flags = 0;
  const optional: number[] = [];

  if(options.defaultSampleDuration !== undefined) {

    flags |= 0x000008;
    optional.push(options.defaultSampleDuration);
  }

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

/* makeTrun constructs a minimal trun (track fragment run) box carrying just a sample count.
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

/* makeFtyp builds a minimal ftyp box.
 */
function makeFtyp(): Buffer {

  return makeBox("ftyp", Buffer.from("isom"));
}

/* makeMoov builds a minimal moov box with no trak children, so parseMoovTrackInfo walks zero tracks and leaves trackTimescales empty - the condition that keeps the
 * offset computation on the initialValue-minus-originalTfdt formula.
 */
function makeMoov(): Buffer {

  return makeBox("moov", Buffer.alloc(0));
}

/* makeMdat builds an mdat box wrapping the given payload string as its media bytes.
 */
function makeMdat(payload: string): Buffer {

  return makeBox("mdat", Buffer.from(payload));
}

/* videoTraf and audioTraf build a single-track traf with the given tfdt baseMediaDecodeTime. Track 1 is video, track 2 is audio.
 */
function videoTraf(baseMediaDecodeTime: number): Buffer {

  return makeTraf(makeTfhd({ defaultSampleDuration: 2000, trackId: 1 }), makeTfdt(baseMediaDecodeTime), makeTrun({ sampleCount: 1 }));
}

function audioTraf(baseMediaDecodeTime: number): Buffer {

  return makeTraf(makeTfhd({ defaultSampleDuration: 1000, trackId: 2 }), makeTfdt(baseMediaDecodeTime), makeTrun({ sampleCount: 1 }));
}

/* readTrackTfdts extracts the leading moof from a stored segment (a moof box followed by an mdat box) and returns each track's current tfdt baseMediaDecodeTime.
 * offsetMoofTimestamps with an empty map returns each traf's tfdt as originalTfdt and writes it back unchanged (a 0n no-op), so it doubles as a read.
 */
function readTrackTfdts(segment: Buffer): Map<number, bigint> {

  const moofSize = segment.readUInt32BE(0);
  const moof = segment.subarray(0, moofSize);
  const results = offsetMoofTimestamps(moof, new Map());
  const tfdts = new Map<number, bigint>();

  for(const [ trackId, result ] of results) {

    tfdts.set(trackId, result.originalTfdt);
  }

  return tfdts;
}

/* makeAndRegisterStream wraps the canonical makeRegistryEntry factory with registerStream so the segmenter has a real registered stream to store segments against.
 */
function makeAndRegisterStream(): { streamId: number } {

  const entry = makeRegistryEntry();

  registerStream(entry);

  return { streamId: entry.id };
}

describe("fMP4 segmenter per-track offset application", () => {

  let streamId: number;

  // A nonzero offset basis for both tracks. Each initial value is chosen to differ from that track's moof tfdt so the finalized offset is nonzero: an absent or
  // zero audio offset would keep the corrective rewrite from firing, and the mixed-moof pin would pass vacuously even against the unfixed double-application code.
  const initialTrackTimestamps = new Map<number, bigint>([ [ 1, 90000n ], [ 2, 48000n ] ]);

  beforeEach(() => {

    ({ streamId } = makeAndRegisterStream());
  });

  afterEach(() => {

    unregisterStream(streamId);
    mock.timers.reset();
  });

  test("applies each track's offset exactly once in a moof mixing an already-initialized track with a newly initializing one", async () => {

    // Track 1 (video) is initialized on a video-only first moof (offset 90000 - 1000 = 89000). The second moof adds track 2 (audio, offset 48000 - 500 = 47500) while
    // still carrying track 1. The corrective rewrite must offset only the newly initialized audio track, leaving the video track's single application intact. Against
    // the unfixed full-map rewrite the video tfdt would be offset twice (3000 + 89000 + 89000 = 181000); the fix scopes the rewrite so it is offset once (92000).
    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ initialTrackTimestamps, onError, onStop, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());

    // First moof: video only. This finalizes track 1's offset without any mixing.
    readable.write(makeMoof(videoTraf(1000)));
    readable.write(makeMdat("v0"));

    // Second moof: video plus a first-seen audio traf. Its arrival cuts segment0 (the first moof+mdat pair) via the fast path, then this moof is rewritten and buffered.
    readable.write(makeMoof(videoTraf(3000), audioTraf(500)));
    readable.write(makeMdat("va1"));

    readable.end();

    await new Promise<void>((resolve) => setImmediate(resolve));

    const mixedSegment = getSegment(streamId, "segment1.m4s");

    assert.ok(mixedSegment, "the mixed video+audio moof was flushed as segment1 on stream end");

    const tfdts = readTrackTfdts(mixedSegment);

    assert.equal(tfdts.get(1), 92000n, "the already-initialized video track's tfdt carries exactly one offset application (3000 + 89000), not two");
    assert.equal(tfdts.get(2), 48000n, "the newly initialized audio track's tfdt carries its single offset application (500 + 47500)");
    assert.equal(onError.mock.calls.length, 0, "no malformed-moof errors");
  });

  test("applies each track's offset exactly once when both tracks first appear together in the first moof (cold-tune parity)", async () => {

    // The common cold-tune shape: Chrome declares both tracks and the first moof carries both. All tracks are new, so the first call is a pure pass-through and the
    // corrective rewrite offsets both exactly once. This path is byte-identical between the scoped and unscoped rewrite - the pin guards against a regression on it.
    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ initialTrackTimestamps, onError, onStop, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());

    // First moof: both tracks together. A second moof follows only to cut segment0 (this first pair) via the fast path.
    readable.write(makeMoof(videoTraf(1000), audioTraf(500)));
    readable.write(makeMdat("va0"));
    readable.write(makeMoof(videoTraf(5000)));

    await new Promise<void>((resolve) => setImmediate(resolve));

    const firstSegment = getSegment(streamId, "segment0.m4s");

    assert.ok(firstSegment, "the both-tracks first moof was cut as segment0 when the second moof arrived");

    const tfdts = readTrackTfdts(firstSegment);

    assert.equal(tfdts.get(1), 90000n, "video tfdt is its initial value (1000 + 89000), one application");
    assert.equal(tfdts.get(2), 48000n, "audio tfdt is its initial value (500 + 47500), one application");
    assert.equal(onError.mock.calls.length, 0, "no malformed-moof errors");
  });

  test("does not re-run the corrective rewrite on a subsequent moof that introduces no new track (steady-state parity)", async () => {

    // Once both tracks are initialized, a later moof carrying both introduces no new track: newOffsets is empty, the corrective rewrite is skipped, and the first call
    // alone applies each stored offset once. The second moof's tfdts are the original values plus one offset each (3000 + 89000 = 92000, 800 + 47500 = 48300).
    const onError = mock.fn();
    const onStop = mock.fn();
    const segmenter = createFMP4Segmenter({ initialTrackTimestamps, onError, onStop, streamId });
    const readable = new PassThrough();

    segmenter.pipe(readable);

    readable.write(makeFtyp());
    readable.write(makeMoov());

    // First moof: both tracks, initializing their offsets. Second moof: both tracks again, no new track. Its arrival cuts segment0; stream end flushes it as segment1.
    readable.write(makeMoof(videoTraf(1000), audioTraf(500)));
    readable.write(makeMdat("va0"));
    readable.write(makeMoof(videoTraf(3000), audioTraf(800)));
    readable.write(makeMdat("va1"));

    readable.end();

    await new Promise<void>((resolve) => setImmediate(resolve));

    const secondSegment = getSegment(streamId, "segment1.m4s");

    assert.ok(secondSegment, "the second both-tracks moof was flushed as segment1 on stream end");

    const tfdts = readTrackTfdts(secondSegment);

    assert.equal(tfdts.get(1), 92000n, "video tfdt is original plus one stored offset (3000 + 89000)");
    assert.equal(tfdts.get(2), 48300n, "audio tfdt is original plus one stored offset (800 + 47500)");
    assert.equal(onError.mock.calls.length, 0, "no malformed-moof errors");
  });
});
