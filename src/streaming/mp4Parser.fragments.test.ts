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
