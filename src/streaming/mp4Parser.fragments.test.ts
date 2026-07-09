/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mp4Parser.fragments.test.ts: Unit tests for fragment-related parsing in mp4Parser.ts - detectMoofKeyframe (sample-flag inspection) and offsetMoofTimestamps
 * (tfdt/trun rewriting). Basic box parsing lives in mp4Parser.test.ts; moov-track-info parsing lives in mp4Parser.moov.test.ts; codec-config parsing lives in
 * mp4Parser.codec.test.ts.
 */
import { describe, test } from "node:test";
import { detectMoofKeyframe, offsetMoofTimestamps } from "./mp4Parser.ts";
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

    // Audio tracks are always sync (sample_depends_on === 2, or 0 for unknown), so the only source of sample_depends_on === 1 is video. If any traf reports
    // non-keyframe explicitly, we treat the moof as non-keyframe regardless of other trafs being marked sync. Locks the multi-track precedence rule.
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

/* u32 returns a 4-byte big-endian buffer, the wire form of every fixed-width MP4 field the richer builders below emit. Keeping it a named helper makes the byte
 * layout of each synthetic box explicit at the call site.
 */
function u32(value: number): Buffer {

  const buf = Buffer.alloc(4);

  buf.writeUInt32BE(value >>> 0);

  return buf;
}

/* makeTfhdRaw builds a tfhd that can carry the optional fields preceding default_sample_flags - base_data_offset (0x1), sample_description_index (0x2), and
 * default_sample_size (0x10) - which the minimal makeTfhd above cannot. The fields are emitted in ISO 14496-12 order so tests can verify parseTfhd advances its
 * optional-field cursor correctly before reading default_sample_flags / default_sample_duration.
 */
function makeTfhdRaw(options: { baseDataOffset?: bigint; defaultSampleDuration?: number; defaultSampleFlags?: number; defaultSampleSize?: number;
  sampleDescriptionIndex?: number; trackId: number; }): Buffer {

  let flags = 0;
  const fields: Buffer[] = [];

  if(options.baseDataOffset !== undefined) {

    flags |= 0x000001;

    const wide = Buffer.alloc(8);

    wide.writeBigUInt64BE(options.baseDataOffset);
    fields.push(wide);
  }

  if(options.sampleDescriptionIndex !== undefined) {

    flags |= 0x000002;
    fields.push(u32(options.sampleDescriptionIndex));
  }

  if(options.defaultSampleDuration !== undefined) {

    flags |= 0x000008;
    fields.push(u32(options.defaultSampleDuration));
  }

  if(options.defaultSampleSize !== undefined) {

    flags |= 0x000010;
    fields.push(u32(options.defaultSampleSize));
  }

  if(options.defaultSampleFlags !== undefined) {

    flags |= 0x000020;
    fields.push(u32(options.defaultSampleFlags));
  }

  return makeBox("tfhd", Buffer.concat([ u32(flags), u32(options.trackId), ...fields ]));
}

/* makeTrunWithSamples builds a trun carrying per-sample entries, which the minimal makeTrun above cannot. flags is the trun flag word (e.g. 0x100 | 0x200 for
 * per-sample duration and size); samples is one array of already-ordered field values per sample, matching the set flag bits. sample_count is samples.length.
 */
function makeTrunWithSamples(options: { flags: number; samples: number[][] }): Buffer {

  const parts: Buffer[] = [ u32(options.flags), u32(options.samples.length) ];

  for(const sample of options.samples) {

    for(const field of sample) {

      parts.push(u32(field));
    }
  }

  return makeBox("trun", Buffer.concat(parts));
}

/* makeTfdtV1 builds a version-1 tfdt whose baseMediaDecodeTime is a 64-bit value, exercising the high/low 32-bit read-and-write-back path that the version-0
 * makeTfdt above does not.
 */
function makeTfdtV1(baseMediaDecodeTime: bigint): Buffer {

  const payload = Buffer.alloc(12);

  // Version 1 in the high byte of the version+flags word; the 64-bit decode time follows.
  payload.writeUInt32BE(0x01000000, 0);
  payload.writeBigUInt64BE(baseMediaDecodeTime, 4);

  return makeBox("tfdt", payload);
}

/* makeMfhd builds a minimal mfhd (movie fragment header) box. A real moof carries an mfhd sibling ahead of its traf boxes; tests use it to prove the moof walker
 * skips non-traf siblings without perturbing the per-track result.
 */
function makeMfhd(): Buffer {

  return makeBox("mfhd", Buffer.concat([ u32(0), u32(1) ]));
}

describe("detectMoofKeyframe - flag-source resolution and container robustness", () => {

  test("reads the first sample's per-sample flags (0x400) after skipping the duration and size fields", () => {

    // When a trun sets per-sample flags (0x400) but not first_sample_flags (0x004), the first sample's flags live in the first entry, after the per-sample duration
    // (0x100) and size (0x200) fields. We seed the duration and size slots with a non-keyframe pattern (sample_depends_on == 1) and the real flags slot with a
    // keyframe pattern (sample_depends_on == 2); only reading the correct cursor yields a keyframe verdict, so a cursor slip would flip the result to false.
    const trun = makeTrunWithSamples({ flags: 0x100 | 0x200 | 0x400, samples: [[ 0x01000000, 0x01000000, 0x02000000 ]] });
    const moof = makeMoof(makeTraf(makeTfhd({ trackId: 1 }), trun));

    assert.equal(detectMoofKeyframe(moof), true, "per-sample flags of the first entry were read at the correct offset");
  });

  test("walks the tfhd optional-field cursor to read default_sample_flags past base_data_offset, sample_description_index, and default_sample_size", () => {

    // tfhd optional fields appear in flag order: base_data_offset (0x1, 8 bytes), sample_description_index (0x2, 4 bytes), default_sample_size (0x10, 4 bytes),
    // then default_sample_flags (0x20, 4 bytes). We poison every preceding field with a non-keyframe pattern (0x01000000) and set default_sample_flags to a
    // keyframe pattern (0x02000000); the trun carries no flags, so the verdict comes solely from tfhd's default. A miscomputed cursor reads a poisoned field and
    // returns false, so a true result pins the cursor math.
    const tfhd = makeTfhdRaw({ baseDataOffset: 0x0100000001000000n, defaultSampleFlags: 0x02000000, defaultSampleSize: 0x01000000, sampleDescriptionIndex: 0x01000000,
      trackId: 1 });
    const moof = makeMoof(makeTraf(tfhd, makeTrun({ sampleCount: 1 })));

    assert.equal(detectMoofKeyframe(moof), true, "default_sample_flags read at the correct optional-field offset");
  });

  test("returns null without throwing for a truncated trun smaller than its 16-byte minimum", () => {

    // A trun box of only 12 bytes (header plus a version/flags word, no sample_count) is below the 16-byte floor. extractFirstSampleFlags must return null rather
    // than read past the box, so detectMoofKeyframe reports indeterminate. The suite completing proves no out-of-bounds read or hang on truncated fragment input.
    const truncatedTrun = makeBox("trun", u32(0));
    const moof = makeMoof(makeTraf(makeTfhd({ trackId: 1 }), truncatedTrun));

    assert.equal(detectMoofKeyframe(moof), null);
  });

  test("skips a non-traf sibling box (mfhd) in the moof without disturbing the traf verdict", () => {

    // A real moof leads with an mfhd before its traf boxes. The walker must ignore any box whose type is not traf, so the keyframe verdict is determined solely by
    // the traf. A regression that mis-handled siblings would either throw on the mfhd or read its bytes as a traf.
    const moof = makeMoof(makeMfhd(), makeTraf(makeTfhd({ trackId: 1 }), makeTrun({ firstSampleFlags: 2 << 24, sampleCount: 1 })));

    assert.equal(detectMoofKeyframe(moof), true, "the mfhd sibling was skipped and the traf drove the verdict");
  });
});

describe("offsetMoofTimestamps - per-sample durations, 64-bit tfdt, and truncation", () => {

  test("sums per-sample durations when the trun sets the sample-duration flag (0x100)", () => {

    // With per-sample durations present (0x100) the total is the sum of each entry's duration field, not defaultSampleDuration * sampleCount. The entries also carry
    // a size field (0x200), so entrySize is 8 and the duration is the first 4 bytes of each 8-byte entry. Durations 10 + 20 + 30 must total 60.
    const tfhd = makeTfhd({ defaultSampleDuration: 999, trackId: 1 });
    const trun = makeTrunWithSamples({ flags: 0x100 | 0x200, samples: [ [ 10, 0 ], [ 20, 0 ], [ 30, 0 ] ] });
    const moof = makeMoof(makeTraf(tfhd, makeTfdt(0), trun));

    const result = offsetMoofTimestamps(moof, new Map());

    assert.equal(result.get(1)?.duration, 60n, "per-sample durations summed (10 + 20 + 30), ignoring the 999 default");
  });

  test("reads a version-1 (64-bit) tfdt across the 32-bit boundary and round-trips an applied offset", () => {

    // A version-1 tfdt stores baseMediaDecodeTime as two 32-bit halves. We choose a value just past 2^32 (high = 1, low = 5) so a read that ignored the high word
    // would report 5 instead of 4294967301. Applying a +100 offset must write both halves back so a re-read observes 4294967401 - proving the boundary-crossing
    // read and write-back are correct.
    const original = (1n << 32n) | 5n;
    const tfhd = makeTfhd({ trackId: 7 });
    const moof = makeMoof(makeTraf(tfhd, makeTfdtV1(original), makeTrun({ sampleCount: 0 })));

    assert.equal(offsetMoofTimestamps(moof, new Map()).get(7)?.originalTfdt, original, "64-bit tfdt read across the 32-bit boundary");

    const offsets = new Map<number, bigint>([[ 7, 100n ]]);

    offsetMoofTimestamps(moof, offsets);

    assert.equal(offsetMoofTimestamps(moof, new Map()).get(7)?.originalTfdt, original + 100n, "the offset was written back into both 32-bit halves");
  });

  test("leaves originalTfdt at zero without throwing when a version-1 tfdt is truncated below 20 bytes", () => {

    // A version-1 tfdt needs 20 bytes (8 header + 4 version/flags + 8 decode time). A 16-byte version-1 tfdt is truncated; the reader must bail rather than read the
    // missing low word, leaving originalTfdt at its 0n default. The track still reports because its tfhd parsed cleanly.
    const truncatedV1Tfdt = makeBox("tfdt", Buffer.concat([ u32(0x01000000), u32(0) ]));
    const moof = makeMoof(makeTraf(makeTfhd({ trackId: 1 }), truncatedV1Tfdt, makeTrun({ sampleCount: 0 })));

    const result = offsetMoofTimestamps(moof, new Map());

    assert.equal(result.get(1)?.originalTfdt, 0n, "truncated 64-bit tfdt yields the zero default, not a partial read");
  });

  test("drops a track whose tfhd claims default_sample_duration (0x8) but is truncated with no room for it", () => {

    // parseTfhd returns null when a declared optional field overruns the box. A tfhd whose flags claim default_sample_duration (0x8) but whose box ends right after
    // track_ID cannot supply the 4-byte field, so parseTfhd yields null and offsetMoofTimestamps omits the track entirely - a control track with a well-formed tfhd
    // confirms the omission is caused by the overrun, not by the surrounding structure.
    const truncatedTfhd = makeBox("tfhd", Buffer.concat([ u32(0x000008), u32(1) ]));
    const brokenTraf = makeTraf(truncatedTfhd, makeTfdt(0), makeTrun({ sampleCount: 0 }));
    const goodTraf = makeTraf(makeTfhd({ trackId: 2 }), makeTfdt(0), makeTrun({ sampleCount: 0 }));

    const result = offsetMoofTimestamps(makeMoof(brokenTraf, goodTraf), new Map());

    assert.equal(result.has(1), false, "the track with the overrunning tfhd is dropped");
    assert.equal(result.has(2), true, "the well-formed track is still reported");
  });
});
