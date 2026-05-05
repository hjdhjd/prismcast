/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mp4Parser.test.ts: Unit tests for the low-level MP4 box parser. mp4Parser.ts is a pure-function module that walks ISO 14496-12 box structures: streaming chunked
 * input via createMP4BoxParser, child-box iteration via iterateChildBoxes, keyframe detection from sample flags via detectMoofKeyframe, and timestamp rewriting via
 * offsetMoofTimestamps. The tests build minimal synthetic MP4 buffers - just enough box structure to exercise each parser path - rather than real fMP4 fixtures, which
 * would couple the test to a specific encoder.
 */
import type { MP4Box, MP4BoxCallback } from "./mp4Parser.ts";
import { createMP4BoxParser, detectMoofKeyframe, findDescriptor, iterateChildBoxes, offsetMoofTimestamps, parseMoovCodecConfig, parseMoovTrackInfo,
  readDescriptorSize } from "./mp4Parser.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

/* makeBox builds a minimal MP4 box: 4-byte size + 4-byte type + payload. The size includes the 8-byte header. Used throughout to construct synthetic test inputs.
 */
function makeBox(type: string, payload: Buffer = Buffer.alloc(0)): Buffer {

  const size = 8 + payload.length;
  const buf = Buffer.alloc(size);

  buf.writeUInt32BE(size, 0);
  buf.write(type, 4, 4, "ascii");
  payload.copy(buf, 8);

  return buf;
}

/* makeTfhd constructs a minimal tfhd (track fragment header) box. The flags determine which optional fields are present; we currently only use the simplest form
 * (no optional fields except maybe default_sample_flags / default_sample_duration).
 */
function makeTfhd(options: { defaultSampleDuration?: number; defaultSampleFlags?: number; trackId: number }): Buffer {

  let flags = 0;
  const optional: number[] = [];

  if(options.defaultSampleDuration !== undefined) {

    flags |= 0x000008;
    optional.push(options.defaultSampleDuration);
  }

  if(options.defaultSampleFlags !== undefined) {

    flags |= 0x000020;
    optional.push(options.defaultSampleFlags);
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

/* makeTrun constructs a minimal trun (track fragment run) box with a sample count and optional first_sample_flags.
 */
function makeTrun(options: { firstSampleFlags?: number; sampleCount: number }): Buffer {

  let flags = 0;

  if(options.firstSampleFlags !== undefined) {

    flags |= 0x004;
  }

  // trun payload: 4 bytes version+flags, 4 bytes sample_count, 4 bytes first_sample_flags (if present).
  const payloadSize = 8 + (options.firstSampleFlags !== undefined ? 4 : 0);
  const payload = Buffer.alloc(payloadSize);

  payload.writeUInt32BE(flags, 0);
  payload.writeUInt32BE(options.sampleCount, 4);

  if(options.firstSampleFlags !== undefined) {

    payload.writeUInt32BE(options.firstSampleFlags, 8);
  }

  return makeBox("trun", payload);
}

/* makeTfdt constructs a minimal tfdt (track fragment decode time) box with version 0 (32-bit baseMediaDecodeTime).
 */
function makeTfdt(baseMediaDecodeTime: number): Buffer {

  // tfdt payload: 4 bytes version+flags (version 0), 4 bytes baseMediaDecodeTime.
  const payload = Buffer.alloc(8);

  payload.writeUInt32BE(0, 0);
  payload.writeUInt32BE(baseMediaDecodeTime, 4);

  return makeBox("tfdt", payload);
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

describe("createMP4BoxParser", () => {

  test("emits a single complete box pushed in one chunk", () => {

    const seen: MP4Box[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box));
    const data = makeBox("ftyp", Buffer.from("isomavc1", "ascii"));

    parser.push(data);

    assert.equal(seen.length, 1, "one box emitted");
    assert.equal(seen[0]!.type, "ftyp");
    assert.equal(seen[0]!.size, data.length);
  });

  test("emits multiple complete boxes in their pushed order", () => {

    // Locks the parser's no-reorder contract. The fMP4 segmenter relies on box order to identify ftyp+moov vs moof+mdat pairs.
    const seen: string[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box.type));

    parser.push(Buffer.concat([
      makeBox("ftyp"),
      makeBox("moov"),
      makeBox("moof"),
      makeBox("mdat", Buffer.from("data"))
    ]));

    assert.deepEqual(seen, [ "ftyp", "moov", "moof", "mdat" ]);
  });

  test("buffers incomplete data across multiple push calls", () => {

    // Boundary: streaming input rarely arrives on box boundaries. The parser must accumulate partial data and emit only complete boxes.
    const seen: MP4Box[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box));
    const full = makeBox("ftyp", Buffer.from("test"));

    parser.push(full.subarray(0, 4));
    assert.equal(seen.length, 0, "header-only chunk has no complete box yet");

    parser.push(full.subarray(4, 8));
    assert.equal(seen.length, 0, "type field arrived but payload still pending");

    parser.push(full.subarray(8));
    assert.equal(seen.length, 1, "completed once payload arrived");
    assert.equal(seen[0]?.type, "ftyp");
  });

  test("handles a chunk containing more than one complete box", () => {

    const seen: string[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box.type));

    parser.push(Buffer.concat([ makeBox("ftyp"), makeBox("moov"), makeBox("moof") ]));

    assert.deepEqual(seen, [ "ftyp", "moov", "moof" ]);
  });

  test("skips a size-0 box marker (extends-to-end-of-file is invalid for streaming)", () => {

    // Boundary: size 0 in a streaming context means "to end of file" which has no meaning. The parser must skip 1 byte and resync rather than crash.
    const seen: string[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box.type));

    // 4 bytes of zeros (size 0), then a valid ftyp.
    parser.push(Buffer.concat([ Buffer.alloc(4), makeBox("ftyp") ]));

    // The parser must eventually emit ftyp once it resyncs past the zeros. We use a loose check: the ftyp box appears.
    assert.ok(seen.includes("ftyp"), "parser resynced and emitted ftyp after skipping size-0 prefix");
  });

  test("flush clears the buffer and discards any incomplete box", () => {

    // Boundary: flush is the cleanup path. After flush, a partial box left in the buffer must NOT be emitted by a subsequent push.
    const seen: MP4Box[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box));
    const full = makeBox("ftyp", Buffer.from("partial"));

    parser.push(full.subarray(0, 5));
    parser.flush();

    // After flush, push the rest. The parser sees random bytes (not a valid header) so nothing should emit.
    parser.push(full.subarray(5));

    assert.equal(seen.length, 0, "no boxes emitted after flush+continuation");
  });

  test("rejects boxes whose declared size is smaller than the header (corrupt input)", () => {

    // Boundary: a size of 4 (less than the 8-byte minimum header) is invalid. The parser should resync without emitting it.
    const seen: MP4Box[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box));

    const bad = Buffer.alloc(8);

    bad.writeUInt32BE(4, 0);
    bad.write("ftyp", 4);

    parser.push(bad);

    // No boxes should be emitted (the parser advances by 1 byte trying to resync).
    assert.equal(seen.length, 0, "corrupt small-size box rejected");
  });
});

describe("iterateChildBoxes", () => {

  test("iterates each direct child box of a container", () => {

    const traf = makeTraf(makeTfhd({ trackId: 1 }), makeTfdt(0), makeTrun({ sampleCount: 2 }));
    const seen: string[] = [];

    iterateChildBoxes(traf, (type) => seen.push(type));

    assert.deepEqual(seen, [ "tfhd", "tfdt", "trun" ]);
  });

  test("provides correct offset and size for each child", () => {

    // Locks the offset+size contract used by parseTfhd, extractTrunTotalDuration, etc.
    const tfhd = makeTfhd({ trackId: 7 });
    const trun = makeTrun({ sampleCount: 1 });
    const traf = makeTraf(tfhd, trun);

    const seen: { offset: number; size: number; type: string }[] = [];

    iterateChildBoxes(traf, (type, _data, offset, size) => seen.push({ offset, size, type }));

    assert.equal(seen[0]!.type, "tfhd");
    assert.equal(seen[0]!.offset, 8, "first child starts after the 8-byte container header");
    assert.equal(seen[0]!.size, tfhd.length, "size matches tfhd buffer length");
    assert.equal(seen[1]!.offset, 8 + tfhd.length, "second child immediately follows the first");
  });

  test("stops iterating when a child's declared size exceeds the parent's bounds", () => {

    // Boundary: a child claiming to be larger than the container is malformed. The iterator must stop rather than read past the buffer.
    const truncatedChild = Buffer.alloc(8);

    truncatedChild.writeUInt32BE(1024, 0);
    truncatedChild.write("tfhd", 4);

    const traf = makeBox("traf", truncatedChild);
    const seen: string[] = [];

    iterateChildBoxes(traf, (type) => seen.push(type));

    assert.deepEqual(seen, [], "no children emitted when first child exceeds parent");
  });

  test("stops iterating on an invalid child size of zero", () => {

    // Boundary: size 0 = "to end of file" is meaningless inside a parent. The iterator must stop.
    const zeroBox = Buffer.alloc(8);

    zeroBox.writeUInt32BE(0, 0);
    zeroBox.write("tfhd", 4);

    const traf = makeBox("traf", zeroBox);
    const seen: string[] = [];

    iterateChildBoxes(traf, (type) => seen.push(type));

    assert.deepEqual(seen, [], "no children emitted on zero-size child");
  });

  test("returns when the buffer is too small to contain even one child header", () => {

    // Boundary: a container with only the 8-byte parent header has zero children.
    const empty = makeBox("traf");
    const seen: string[] = [];

    iterateChildBoxes(empty, (type) => seen.push(type));

    assert.deepEqual(seen, [], "no children for empty container");
  });
});

describe("detectMoofKeyframe", () => {

  test("returns true when first_sample_flags marks the sample as a sync sample", () => {

    // sample_depends_on === 2 (independent) -> keyframe. Encoded in bits 25-24 of the sample flags.
    const flags = 2 << 24;
    const moof = makeMoof(makeTraf(makeTfhd({ trackId: 1 }), makeTrun({ firstSampleFlags: flags, sampleCount: 1 })));

    assert.equal(detectMoofKeyframe(moof), true);
  });

  test("returns false when first_sample_flags marks the sample as dependent", () => {

    // sample_depends_on === 1 (dependent) -> not keyframe.
    const flags = 1 << 24;
    const moof = makeMoof(makeTraf(makeTfhd({ trackId: 1 }), makeTrun({ firstSampleFlags: flags, sampleCount: 1 })));

    assert.equal(detectMoofKeyframe(moof), false);
  });

  test("returns false when sample_is_non_sync_sample (bit 16) is set", () => {

    // When sample_depends_on is 0 (unknown) but the non-sync bit is set, the sample is explicitly not a sync sample.
    const flags = 1 << 16;
    const moof = makeMoof(makeTraf(makeTfhd({ trackId: 1 }), makeTrun({ firstSampleFlags: flags, sampleCount: 1 })));

    assert.equal(detectMoofKeyframe(moof), false);
  });

  test("treats unknown flags (0) as a sync sample by default per ISO 14496-12", () => {

    // Boundary: sample_depends_on == 0 AND non_sync == 0 means the spec defaults to sync. Locks the default branch behavior.
    const moof = makeMoof(makeTraf(makeTfhd({ trackId: 1 }), makeTrun({ firstSampleFlags: 0, sampleCount: 1 })));

    assert.equal(detectMoofKeyframe(moof), true);
  });

  test("falls back to default_sample_flags from tfhd when trun has no flags", () => {

    // The implementation's flag-source priority chain: trun first_sample_flags -> trun per-sample flags -> tfhd default. We exercise the tfhd-default branch by
    // omitting flags from trun.
    const tfhd = makeTfhd({ defaultSampleFlags: 2 << 24, trackId: 1 });
    const trun = makeTrun({ sampleCount: 1 });
    const moof = makeMoof(makeTraf(tfhd, trun));

    assert.equal(detectMoofKeyframe(moof), true, "tfhd default flags identified the keyframe");
  });

  test("returns null when neither trun nor tfhd carries any flag information", () => {

    // Boundary: no sample-level flag source available -> the function reports indeterminate rather than guessing.
    const moof = makeMoof(makeTraf(makeTfhd({ trackId: 1 }), makeTrun({ sampleCount: 1 })));

    assert.equal(detectMoofKeyframe(moof), null, "indeterminate when no flag source");
  });

  test("a non-keyframe traf overrides a keyframe traf (multi-track precedence)", () => {

    // Audio tracks are always sync (sample_depends_on 0/1), so the only source of sample_depends_on === 2 is video. If any traf reports non-keyframe explicitly,
    // we treat the moof as non-keyframe regardless of other trafs being marked sync. Locks the multi-track precedence rule.
    const audioTraf = makeTraf(makeTfhd({ trackId: 1 }), makeTrun({ firstSampleFlags: 2 << 24, sampleCount: 1 }));
    const videoTraf = makeTraf(makeTfhd({ trackId: 2 }), makeTrun({ firstSampleFlags: 1 << 24, sampleCount: 1 }));
    const moof = makeMoof(audioTraf, videoTraf);

    assert.equal(detectMoofKeyframe(moof), false, "any non-keyframe traf wins");
  });

  test("returns null for a moof with no traf children", () => {

    // Boundary: malformed moof. The function must report indeterminate rather than crash.
    const moof = makeBox("moof");

    assert.equal(detectMoofKeyframe(moof), null);
  });
});

describe("offsetMoofTimestamps", () => {

  test("reads the original tfdt baseMediaDecodeTime without modifying it when offset is zero", () => {

    // Pure pass-through case: empty offsets map, function returns the original tfdt as the originalTfdt result and does not change the buffer's tfdt value.
    const tfhd = makeTfhd({ defaultSampleDuration: 100, trackId: 1 });
    const tfdt = makeTfdt(5_000);
    const trun = makeTrun({ sampleCount: 0 });
    const moof = makeMoof(makeTraf(tfhd, tfdt, trun));

    const result = offsetMoofTimestamps(moof, new Map());

    assert.equal(result.size, 1, "one track result");
    assert.equal(result.get(1)?.originalTfdt, 5_000n, "original tfdt extracted");
  });

  test("returns per-track durations as the sum of sample durations (default sample duration path)", () => {

    // When trun has no per-sample duration flag (0x100), the implementation uses defaultSampleDuration * sampleCount.
    const tfhd = makeTfhd({ defaultSampleDuration: 100, trackId: 1 });
    const tfdt = makeTfdt(0);
    const trun = makeTrun({ sampleCount: 5 });
    const moof = makeMoof(makeTraf(tfhd, tfdt, trun));

    const result = offsetMoofTimestamps(moof, new Map());

    assert.equal(result.get(1)?.duration, 500n, "5 samples * 100 = 500 timescale units");
  });

  test("applies a positive offset to the tfdt and writes it back into the buffer", () => {

    // The function rewrites the tfdt in place. After invocation, reading the buffer at the tfdt offset must show the new value.
    const tfhd = makeTfhd({ trackId: 1 });
    const tfdt = makeTfdt(1_000);
    const trun = makeTrun({ sampleCount: 0 });
    const moof = makeMoof(makeTraf(tfhd, tfdt, trun));

    const offsets = new Map<number, bigint>();

    offsets.set(1, 500n);

    offsetMoofTimestamps(moof, offsets);

    // Re-parse to check the rewritten value. The offset for track 1 was 500, applied to the original 1000 -> new tfdt should be 1500.
    const reread = offsetMoofTimestamps(moof, new Map());

    assert.equal(reread.get(1)?.originalTfdt, 1_500n, "tfdt was rewritten in place");
  });

  test("handles a moof with no traf gracefully (returns empty result)", () => {

    // Boundary: empty moof - the function walks zero trafs and returns an empty map.
    const moof = makeBox("moof");
    const result = offsetMoofTimestamps(moof, new Map());

    assert.equal(result.size, 0);
  });

  test("returns separate results per track in a multi-track moof", () => {

    const audioTraf = makeTraf(makeTfhd({ defaultSampleDuration: 50, trackId: 1 }), makeTfdt(100), makeTrun({ sampleCount: 2 }));
    const videoTraf = makeTraf(makeTfhd({ defaultSampleDuration: 200, trackId: 2 }), makeTfdt(0), makeTrun({ sampleCount: 3 }));
    const moof = makeMoof(audioTraf, videoTraf);

    const result = offsetMoofTimestamps(moof, new Map());

    assert.equal(result.size, 2, "two tracks reported");
    assert.equal(result.get(1)?.originalTfdt, 100n);
    assert.equal(result.get(1)?.duration, 100n, "audio: 2 samples * 50 = 100");
    assert.equal(result.get(2)?.originalTfdt, 0n);
    assert.equal(result.get(2)?.duration, 600n, "video: 3 samples * 200 = 600");
  });
});

describe("parseMoovTrackInfo", () => {

  test("returns an empty map for a moov with no trak boxes", () => {

    // Boundary: the function must tolerate a malformed/empty moov.
    const moov = makeBox("moov");
    const result = parseMoovTrackInfo(moov);

    assert.equal(result.size, 0);
  });

  test("ignores trak boxes that lack required mdhd or hdlr children", () => {

    // The function only emits a track entry when trackId, timescale, and handlerType are all present. A trak with no mdhd/hdlr produces no entry.
    const trak = makeBox("trak", makeBox("tkhd", Buffer.alloc(20)));
    const moov = makeBox("moov", trak);
    const result = parseMoovTrackInfo(moov);

    assert.equal(result.size, 0, "incomplete trak omitted from results");
  });
});

/* The MP4BoxCallback type is exercised by the createMP4BoxParser tests above; we lock the type's shape here as a structural sanity check.
 */
describe("MP4BoxCallback", () => {

  test("accepts a callback receiving an MP4Box object", () => {

    // Type-level check via runtime assertion: the callback should be callable with an MP4Box argument.
    const callback: MP4BoxCallback = (box) => {

      assert.ok(typeof box.type === "string");
      assert.ok(typeof box.size === "number");
      assert.ok(Buffer.isBuffer(box.data));
    };

    const parser = createMP4BoxParser(callback);

    parser.push(makeBox("ftyp"));
  });
});

/* The parseMoovCodecConfig fixture builders construct a synthetic moov > trak > mdia > minf > stbl > stsd > {avc1+avcC, mp4a+esds} hierarchy. Each helper layers
 * the next box level around its child(ren), so a complete moov is assembled by composing the builders bottom-up. Reserved-region offsets match the production
 * parser's expectations: avc1 reserves 78 bytes between its header and child boxes (86 - 8); mp4a reserves 28 bytes (36 - 8); stsd is a FullBox with a 4-byte
 * version/flags region followed by a 4-byte entry_count.
 */
function makeAvcC(profile: number, profileCompatibility: number, level: number): Buffer {

  // avcC payload layout: configurationVersion + AVCProfileIndication + profile_compatibility + AVCLevelIndication + ... (the parser only needs the first 4 bytes).
  const payload = Buffer.alloc(4);

  payload.writeUInt8(1, 0);
  payload.writeUInt8(profile, 1);
  payload.writeUInt8(profileCompatibility, 2);
  payload.writeUInt8(level, 3);

  return makeBox("avcC", payload);
}

function makeAvc1(avcC: Buffer): Buffer {

  // avc1 is a sample entry: 8-byte header (added by makeBox) + 78 bytes of reserved/predefined fields + child boxes. The reserved region is zeroed - the parser
  // skips it entirely.
  const reserved = Buffer.alloc(78);

  return makeBox("avc1", Buffer.concat([ reserved, avcC ]));
}

function makeEsds(objectType: number, sampleRateIndex: number): Buffer {

  // esds payload layout: 4-byte version+flags, then ES_Descriptor. The parser scans for tag 0x05 (DecoderSpecificInfo), skips its size byte(s), then reads two
  // bytes of AudioSpecificConfig: byte0 high 5 bits = objectType, low 3 bits + byte1 high 1 bit = sampleRateIndex.
  const versionFlags = Buffer.alloc(4);
  const byte0 = ((objectType & 0x1F) << 3) | ((sampleRateIndex >> 1) & 0x07);
  const byte1 = ((sampleRateIndex & 0x01) << 7);

  // Construct a minimal descriptor: tag 0x05, single-byte size 0x02, then the two-byte AudioSpecificConfig.
  const descriptor = Buffer.from([ 0x05, 0x02, byte0, byte1 ]);

  return makeBox("esds", Buffer.concat([ versionFlags, descriptor ]));
}

function makeMp4a(esds: Buffer): Buffer {

  // mp4a is a sample entry: 8-byte header (added by makeBox) + 28 bytes of reserved fields + child boxes. The reserved region is zeroed - the parser skips it.
  const reserved = Buffer.alloc(28);

  return makeBox("mp4a", Buffer.concat([ reserved, esds ]));
}

function makeStsd(...sampleEntries: Buffer[]): Buffer {

  // stsd is a FullBox: 4-byte version+flags + 4-byte entry_count + sample entries.
  const versionFlags = Buffer.alloc(4);
  const entryCount = Buffer.alloc(4);

  entryCount.writeUInt32BE(sampleEntries.length, 0);

  return makeBox("stsd", Buffer.concat([ versionFlags, entryCount, ...sampleEntries ]));
}

function makeMoovWithStsd(stsd: Buffer): Buffer {

  // Wrap the stsd in trak > mdia > minf > stbl > stsd. Each level is a single-child container.
  const stbl = makeBox("stbl", stsd);
  const minf = makeBox("minf", stbl);
  const mdia = makeBox("mdia", minf);
  const trak = makeBox("trak", mdia);

  return makeBox("moov", trak);
}

describe("parseMoovCodecConfig", () => {

  test("returns null/null for a moov with no trak boxes", () => {

    // Boundary: the function must tolerate an empty moov without throwing.
    const moov = makeBox("moov");
    const result = parseMoovCodecConfig(moov);

    assert.equal(result.audio, null, "no trak means no audio");
    assert.equal(result.video, null, "no trak means no video");
  });

  test("extracts video codec config from an avc1 sample entry with an avcC child", () => {

    // High profile (100), no compatibility flags, level 4.0 (40) - representative values for a 1080p H.264 stream.
    const avcC = makeAvcC(100, 0, 40);
    const stsd = makeStsd(makeAvc1(avcC));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.deepEqual(result.video, { level: 40, profile: 100, profileCompatibility: 0 });
    assert.equal(result.audio, null, "moov with only video has no audio");
  });

  test("extracts audio codec config from an mp4a sample entry with an esds child", () => {

    // AAC-LC (objectType 2), 48000 Hz (sampleRateIndex 3) - the typical encoder default for H.264+AAC streams.
    const esds = makeEsds(2, 3);
    const stsd = makeStsd(makeMp4a(esds));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.deepEqual(result.audio, { objectType: 2, sampleRateIndex: 3 });
    assert.equal(result.video, null, "moov with only audio has no video");
  });

  test("extracts both video and audio when both sample entries are present", () => {

    // Realistic moov with both track types: typical of a captured stream.
    const avcC = makeAvcC(77, 0, 31);
    const esds = makeEsds(2, 4);
    const stsd = makeStsd(makeAvc1(avcC), makeMp4a(esds));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.deepEqual(result.video, { level: 31, profile: 77, profileCompatibility: 0 });
    assert.deepEqual(result.audio, { objectType: 2, sampleRateIndex: 4 });
  });

  test("ignores avc1 entries shorter than 86 bytes (boundary guard)", () => {

    // The parser refuses to read child boxes from an avc1 that's too short to contain the reserved region. We construct an avc1 with only 70 bytes of payload
    // (78 reserved is required) - the entry should be silently skipped.
    const tinyPayload = Buffer.alloc(70);
    const stsd = makeStsd(makeBox("avc1", tinyPayload));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.equal(result.video, null, "undersized avc1 entry yields no video");
  });

  test("ignores avcC boxes smaller than 12 bytes (the parser needs 4 payload bytes plus the 8-byte header)", () => {

    // An avcC with only 2 payload bytes is below the 12-byte minimum. The parser returns without populating video.
    const stuntedAvcC = makeBox("avcC", Buffer.alloc(2));
    const stsd = makeStsd(makeAvc1(stuntedAvcC));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.equal(result.video, null, "undersized avcC yields no video");
  });

  test("ignores mp4a entries shorter than 36 bytes (boundary guard)", () => {

    // The parser refuses to read child boxes from an mp4a that's too short to contain the reserved region.
    const tinyPayload = Buffer.alloc(20);
    const stsd = makeStsd(makeBox("mp4a", tinyPayload));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.equal(result.audio, null, "undersized mp4a entry yields no audio");
  });

  test("ignores esds boxes smaller than 12 bytes", () => {

    // esds size 8 (just the header) is below the 12-byte minimum.
    const stuntedEsds = makeBox("esds");
    const stsd = makeStsd(makeMp4a(stuntedEsds));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.equal(result.audio, null, "undersized esds yields no audio");
  });

  test("ignores esds boxes that lack the DecoderSpecificInfo tag (0x05)", () => {

    // The descriptor walker recurses into 0x03 (ES_Descriptor) and 0x04 (DecoderConfigDescriptor) but does not find 0x05 anywhere in the tree, so the audio
    // field stays null.
    const versionFlags = Buffer.alloc(4);
    const noTagDescriptor = Buffer.from([ 0x03, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00 ]);
    const esds = makeBox("esds", Buffer.concat([ versionFlags, noTagDescriptor ]));
    const stsd = makeStsd(makeMp4a(esds));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.equal(result.audio, null, "esds with no DecoderSpecificInfo descriptor yields no audio");
  });

  test("does NOT misread a 0x05 byte inside a DecoderConfigDescriptor's payload as DecoderSpecificInfo (regression)", () => {

    /* This is the regression that the previous byte-scan implementation produced. We construct a properly-structured ES_Descriptor tree where the
     * DecoderConfigDescriptor's bitrate fields contain a 0x05 byte by chance. A byte-scan-for-0x05 would land on that byte and mis-decode the next two bytes as
     * AudioSpecificConfig. The structural walker correctly skips past DecoderConfigDescriptor's 13-byte fixed header and only matches the 0x05 tag in the
     * actual DecoderSpecificInfo, so the audio config reflects the real values from the inner payload.
     *
     * Tree layout:
     *   ES_Descriptor (0x03, length 35) {
     *     ES_ID (2 bytes) = 0x00, 0x01
     *     flags (1 byte) = 0x00 (no optional fields)
     *     DecoderConfigDescriptor (0x04, length 23) {
     *       objectTypeIndication (1 byte) = 0x40 (AAC)
     *       streamType/upStream/reserved (1 byte) = 0x05 (the bug-trigger byte: streamType=1 (audio), upStream=0, reserved=1; binary 00000101)
     *       bufferSizeDB (3 bytes) = 0x00, 0x00, 0x00
     *       maxBitrate (4 bytes) = 0x00, 0x00, 0x05, 0x00 (another 0x05 lurking here)
     *       avgBitrate (4 bytes) = 0x00, 0x00, 0x00, 0x00
     *       DecoderSpecificInfo (0x05, length 2) {
     *         AudioSpecificConfig (2 bytes) = 0x12, 0x10  // objectType=2 (AAC-LC), sampleRateIndex=4 (44100 Hz)
     *       }
     *     }
     *   }
     */
    const dsi = Buffer.from([ 0x05, 0x02, 0x12, 0x10 ]);
    const dcd = Buffer.concat([
      Buffer.from([
        0x40,
        0x05,
        0x00, 0x00, 0x00,
        0x00, 0x00, 0x05, 0x00,
        0x00, 0x00, 0x00, 0x00
      ]),
      dsi
    ]);
    const dcdDescriptor = Buffer.concat([ Buffer.from([ 0x04, dcd.length ]), dcd ]);
    const esd = Buffer.concat([
      Buffer.from([ 0x00, 0x01, 0x00 ]),
      dcdDescriptor
    ]);
    const esDescriptor = Buffer.concat([ Buffer.from([ 0x03, esd.length ]), esd ]);
    const versionFlags = Buffer.alloc(4);
    const esds = makeBox("esds", Buffer.concat([ versionFlags, esDescriptor ]));
    const stsd = makeStsd(makeMp4a(esds));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    // The structural walker reaches the actual DecoderSpecificInfo and decodes its AudioSpecificConfig (0x12, 0x10): objectType=2, sampleRateIndex=4.
    assert.deepEqual(result.audio, { objectType: 2, sampleRateIndex: 4 },
      "structural walker correctly skips past DecoderConfigDescriptor's payload bytes (which contained 0x05) and matches only the real DecoderSpecificInfo tag");
  });

  test("skips stsd boxes shorter than 16 bytes (header + version/flags + entry_count)", () => {

    // stsd of size 12 is below the 16-byte minimum (8 header + 4 version+flags + 4 entry_count). The parser returns before iterating entries.
    const stuntedStsd = makeBox("stsd", Buffer.alloc(4));
    const moov = makeMoovWithStsd(stuntedStsd);
    const result = parseMoovCodecConfig(moov);

    assert.equal(result.video, null);
    assert.equal(result.audio, null);
  });

  test("handles multi-byte size encoding in the esds DecoderSpecificInfo descriptor", () => {

    // The descriptor size encoding uses a continuation-bit scheme: bytes with the high bit set are length-prefix bytes; the byte without the high bit is the
    // final size byte. The parser walks past the prefix bytes and reads the AudioSpecificConfig at the byte after the final size byte. This test pins the
    // multi-byte path: tag 0x05, three prefix bytes (each 0x80), one terminator (0x02), then the two-byte AudioSpecificConfig.
    const objectType = 2;
    const sampleRateIndex = 3;
    const byte0 = ((objectType & 0x1F) << 3) | ((sampleRateIndex >> 1) & 0x07);
    const byte1 = ((sampleRateIndex & 0x01) << 7);

    const versionFlags = Buffer.alloc(4);
    const descriptor = Buffer.from([ 0x05, 0x80, 0x80, 0x80, 0x02, byte0, byte1 ]);
    const esds = makeBox("esds", Buffer.concat([ versionFlags, descriptor ]));
    const stsd = makeStsd(makeMp4a(esds));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.deepEqual(result.audio, { objectType: 2, sampleRateIndex: 3 });
  });

  test("ignores trak boxes with no mdia child", () => {

    // A trak that has only a tkhd (no mdia) walks the iteration but produces nothing - the inner mdia callback never matches.
    const trak = makeBox("trak", makeBox("tkhd", Buffer.alloc(20)));
    const moov = makeBox("moov", trak);
    const result = parseMoovCodecConfig(moov);

    assert.equal(result.video, null);
    assert.equal(result.audio, null);
  });

  test("decodes full range of profile/level/compatibility values from avcC", () => {

    // Pin the byte-position contract: profile is byte[9], compatibility is byte[10], level is byte[11] of the avcC box (relative to its 8-byte header). Use
    // distinct values for each so a swap would be detected.
    const avcC = makeAvcC(0xAA, 0xBB, 0xCC);
    const stsd = makeStsd(makeAvc1(avcC));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.deepEqual(result.video, { level: 0xCC, profile: 0xAA, profileCompatibility: 0xBB });
  });

  test("decodes the AudioSpecificConfig bit layout correctly across the byte-0/byte-1 boundary", () => {

    // The sampleRateIndex is split across two bytes: 3 bits from byte0 (bits 0-2) and 1 bit from byte1 (bit 7). When sampleRateIndex is odd, that high bit must
    // come from byte1 - this test confirms the cross-byte bit assembly.
    const esds = makeEsds(5, 7);
    const stsd = makeStsd(makeMp4a(esds));
    const moov = makeMoovWithStsd(stsd);
    const result = parseMoovCodecConfig(moov);

    assert.deepEqual(result.audio, { objectType: 5, sampleRateIndex: 7 });
  });
});

describe("readDescriptorSize", () => {

  test("reads a single-byte size when the high bit is clear", () => {

    // 0x42 = 66 decimal, no continuation bit. Length is 66, payload starts at offset 1.
    const buffer = Buffer.from([ 0x42, 0xAA, 0xBB ]);
    const result = readDescriptorSize(buffer, 0);

    assert.deepEqual(result, { length: 66, payloadStart: 1 });
  });

  test("reads a multi-byte size and accumulates 7 bits per byte", () => {

    // 0x80 (continuation, 0 contributed), 0x80 (continuation, 0), 0x80 (continuation, 0), 0x42 (terminator, contributes 66). Total: ((((0 << 7) | 0) << 7) | 0)
    // << 7) | 66 = 66. Payload starts after all 4 bytes.
    const buffer = Buffer.from([ 0x80, 0x80, 0x80, 0x42, 0xAA ]);
    const result = readDescriptorSize(buffer, 0);

    assert.equal(result.length, 66);
    assert.equal(result.payloadStart, 4);
  });

  test("accumulates non-zero continuation bytes correctly", () => {

    // 0x81 (continuation, contributes 1) followed by 0x02 (terminator, contributes 2). Length = (1 << 7) | 2 = 130.
    const buffer = Buffer.from([ 0x81, 0x02, 0xAA ]);
    const result = readDescriptorSize(buffer, 0);

    assert.equal(result.length, 130);
    assert.equal(result.payloadStart, 2);
  });

  test("returns zero length when the buffer ends mid-encoding (malformed input)", () => {

    // 0x80 0x80 with no terminator: walks off the end. The function returns length 0 to signal a malformed encoding so callers naturally produce empty payloads.
    const buffer = Buffer.from([ 0x80, 0x80 ]);
    const result = readDescriptorSize(buffer, 0);

    assert.equal(result.length, 0, "malformed encoding produces zero length");
  });

  test("respects the offset parameter (reads from the requested position, not from 0)", () => {

    // Buffer: [garbage, 0x42 (size), payload bytes]. Reading from offset 1 should produce length 66, payloadStart 2.
    const buffer = Buffer.from([ 0xFF, 0x42, 0xAA, 0xBB ]);
    const result = readDescriptorSize(buffer, 1);

    assert.deepEqual(result, { length: 66, payloadStart: 2 });
  });
});

describe("findDescriptor", () => {

  test("returns the payload of a top-level descriptor that matches the target tag", () => {

    // Tag 0x05, size 2, payload [0xAA, 0xBB]. Top-level match.
    const buffer = Buffer.from([ 0x05, 0x02, 0xAA, 0xBB ]);
    const result = findDescriptor(buffer, 0x05);

    assert.notEqual(result, null);
    assert.deepEqual(Array.from(result ?? []), [ 0xAA, 0xBB ]);
  });

  test("returns null when no descriptor matches the target tag at any level", () => {

    const buffer = Buffer.from([ 0x06, 0x02, 0xAA, 0xBB ]);
    const result = findDescriptor(buffer, 0x05);

    assert.equal(result, null);
  });

  test("returns null on an empty buffer", () => {

    assert.equal(findDescriptor(Buffer.alloc(0), 0x05), null);
  });

  test("recurses into ES_Descriptor (0x03) and finds the target inside it", () => {

    // ES_Descriptor (0x03, size 7) contains: ES_ID (2 bytes) + flags (1 byte = 0, no optional fields) + nested 0x05 descriptor.
    const dsi = Buffer.from([ 0x05, 0x02, 0xCA, 0xFE ]);
    const esPayload = Buffer.concat([ Buffer.from([ 0x00, 0x01, 0x00 ]), dsi ]);
    const buffer = Buffer.concat([ Buffer.from([ 0x03, esPayload.length ]), esPayload ]);
    const result = findDescriptor(buffer, 0x05);

    assert.notEqual(result, null);
    assert.deepEqual(Array.from(result ?? []), [ 0xCA, 0xFE ]);
  });

  test("recurses into DecoderConfigDescriptor (0x04) skipping its 13-byte fixed header", () => {

    // DecoderConfigDescriptor with the fixed 13-byte header followed by a nested 0x05 descriptor in its payload.
    const dsi = Buffer.from([ 0x05, 0x02, 0x12, 0x10 ]);
    const dcdPayload = Buffer.concat([
      Buffer.from([ 0x40, 0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 ]),
      dsi
    ]);
    const buffer = Buffer.concat([ Buffer.from([ 0x04, dcdPayload.length ]), dcdPayload ]);
    const result = findDescriptor(buffer, 0x05);

    assert.notEqual(result, null);
    assert.deepEqual(Array.from(result ?? []), [ 0x12, 0x10 ]);
  });

  test("does NOT match a 0x05 byte appearing inside a non-container descriptor's payload (the regression)", () => {

    // A 0x06 descriptor (SLConfigDescriptor) whose payload happens to contain a 0x05 byte. The walker must skip the entire payload of 0x06, not pause to inspect
    // its bytes for tags. If this test ever fails, the byte-scan-for-0x05 regression has returned.
    const buffer = Buffer.from([ 0x06, 0x03, 0x05, 0x42, 0x42 ]);
    const result = findDescriptor(buffer, 0x05);

    assert.equal(result, null, "0x05 inside another descriptor's payload is not a tag boundary");
  });

  test("respects the streamDependenceFlag in ES_Descriptor (advances 2 extra bytes for dependsOn_ES_ID)", () => {

    // flags byte = 0x80 sets streamDependenceFlag, so we expect 2 extra bytes of dependsOn_ES_ID before nested descriptors.
    const dsi = Buffer.from([ 0x05, 0x02, 0xAB, 0xCD ]);
    const esPayload = Buffer.concat([
      Buffer.from([ 0x00, 0x01, 0x80, 0x12, 0x34 ]),
      dsi
    ]);
    const buffer = Buffer.concat([ Buffer.from([ 0x03, esPayload.length ]), esPayload ]);
    const result = findDescriptor(buffer, 0x05);

    assert.notEqual(result, null, "skipped dependsOn_ES_ID and found the nested descriptor");
    assert.deepEqual(Array.from(result ?? []), [ 0xAB, 0xCD ]);
  });

  test("respects the URL_Flag in ES_Descriptor (advances by URLlength + 1 bytes)", () => {

    // flags byte = 0x40 sets URL_Flag, so we expect 1 byte of URLlength followed by URLlength bytes of URL text.
    const dsi = Buffer.from([ 0x05, 0x02, 0xBE, 0xEF ]);
    const url = Buffer.from("ab", "ascii");
    const esPayload = Buffer.concat([
      Buffer.from([ 0x00, 0x01, 0x40, url.length ]),
      url,
      dsi
    ]);
    const buffer = Buffer.concat([ Buffer.from([ 0x03, esPayload.length ]), esPayload ]);
    const result = findDescriptor(buffer, 0x05);

    assert.notEqual(result, null, "skipped URL string and found the nested descriptor");
    assert.deepEqual(Array.from(result ?? []), [ 0xBE, 0xEF ]);
  });

  test("respects the OCRstreamFlag in ES_Descriptor (advances 2 extra bytes for OCR_ES_Id)", () => {

    // flags byte = 0x20 sets OCRstreamFlag, so we expect 2 extra bytes of OCR_ES_Id before nested descriptors.
    const dsi = Buffer.from([ 0x05, 0x02, 0xFA, 0xCE ]);
    const esPayload = Buffer.concat([
      Buffer.from([ 0x00, 0x01, 0x20, 0x56, 0x78 ]),
      dsi
    ]);
    const buffer = Buffer.concat([ Buffer.from([ 0x03, esPayload.length ]), esPayload ]);
    const result = findDescriptor(buffer, 0x05);

    assert.notEqual(result, null);
    assert.deepEqual(Array.from(result ?? []), [ 0xFA, 0xCE ]);
  });

  test("walks past a non-matching first descriptor to find a matching sibling", () => {

    // Two top-level descriptors: 0x06 (no match), 0x05 (match). The walker advances past 0x06's full payload to the next sibling.
    const buffer = Buffer.from([
      0x06, 0x02, 0xAA, 0xBB,
      0x05, 0x02, 0xCC, 0xDD
    ]);
    const result = findDescriptor(buffer, 0x05);

    assert.notEqual(result, null);
    assert.deepEqual(Array.from(result ?? []), [ 0xCC, 0xDD ]);
  });

  test("first match wins when multiple descriptors share the target tag", () => {

    // Two 0x05 descriptors at the top level. The walker returns the first.
    const buffer = Buffer.from([
      0x05, 0x02, 0x11, 0x22,
      0x05, 0x02, 0x33, 0x44
    ]);
    const result = findDescriptor(buffer, 0x05);

    assert.deepEqual(Array.from(result ?? []), [ 0x11, 0x22 ]);
  });
});
