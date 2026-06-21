/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mp4Parser.moov.test.ts: Unit tests for parseMoovTrackInfo in mp4Parser.ts - the moov-walker that reads tkhd track IDs and mdhd timescales for both v0 and v1
 * box widths. Basic box parsing lives in mp4Parser.test.ts; fragment parsing lives in mp4Parser.fragments.test.ts; codec-config parsing lives in
 * mp4Parser.codec.test.ts.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { parseMoovTrackInfo } from "./mp4Parser.ts";

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

function makeTkhd(options: { trackId: number; version: 0 | 1 }): Buffer {

  if(options.version === 0) {

    // Payload is 16 bytes (creation + mod + trackId + duration would normally follow, but the parser only reads up to byte 24 within the box so we allocate
    // exactly 16 payload bytes -> 24-byte total box).
    const payload = Buffer.alloc(16);

    payload.writeUInt32BE(options.trackId, 12);

    return makeBox("tkhd", payload);
  }

  // v1: bigger 64-bit creation/modification fields shift trackId to byte 28 within the box, byte 20 within payload.
  const payload = Buffer.alloc(24);

  payload.writeUInt8(1, 0);
  payload.writeUInt32BE(options.trackId, 20);

  return makeBox("tkhd", payload);
}

/* makeMdhd constructs a minimal media header box with timescale at the version-correct offset. v0 puts timescale at byte 20 within the box (byte 12 within
 * payload); v1 at byte 28 (byte 20 within payload), shifted by the 64-bit creation/modification fields.
 */
function makeMdhd(options: { timescale: number; version: 0 | 1 }): Buffer {

  if(options.version === 0) {

    const payload = Buffer.alloc(16);

    payload.writeUInt32BE(options.timescale, 12);

    return makeBox("mdhd", payload);
  }

  const payload = Buffer.alloc(24);

  payload.writeUInt8(1, 0);
  payload.writeUInt32BE(options.timescale, 20);

  return makeBox("mdhd", payload);
}

/* makeHdlr constructs a minimal handler box with a 4-character handler_type at the documented offset. The parser needs childSize >= 20 within the box; the
 * 12-byte payload (version+flags + pre_defined + handler_type) yields exactly that.
 */
function makeHdlr(handlerType: string): Buffer {

  const payload = Buffer.alloc(12);

  payload.write(handlerType, 8, 4, "ascii");

  return makeBox("hdlr", payload);
}

/* makeMdia / makeTrak assemble container boxes from their declared children. Both follow the standard parent-header + concat-children layout used by
 * iterateChildBoxes.
 */
function makeMdia(...children: Buffer[]): Buffer {

  return makeBox("mdia", Buffer.concat(children));
}

function makeTrak(...children: Buffer[]): Buffer {

  return makeBox("trak", Buffer.concat(children));
}

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

  test("extracts track_ID, timescale, and handler_type for a complete tkhd-v0 / mdhd-v0 trak", () => {

    /* The positive path the negative tests above were silent on. Box-offset arithmetic in tkhd/mdhd/hdlr is the load-bearing detail: track_ID at byte 20
     * (tkhd v0), timescale at byte 20 (mdhd v0), handler_type at byte 16 (hdlr). A regression in any single offset would silently produce wrong results - the
     * negative tests would still pass because they assert empty-map outcomes. We construct a complete trak (tkhd + mdia[mdhd + hdlr]) with distinguishable
     * values for each field so a swapped offset surfaces as a wrong number rather than a missing entry.
     */
    const trak = makeTrak(
      makeTkhd({ trackId: 7, version: 0 }),
      makeMdia(
        makeMdhd({ timescale: 90_000, version: 0 }),
        makeHdlr("vide")
      )
    );
    const moov = makeBox("moov", trak);
    const result = parseMoovTrackInfo(moov);

    assert.equal(result.size, 1, "one track surfaced");

    const track = result.get(7);

    assert.ok(track, "result map keyed by track_ID 7");
    assert.equal(track.timescale, 90_000, "timescale read from mdhd v0 at the documented offset");
    assert.equal(track.handlerType, "vide", "handler_type read from hdlr at the documented offset");
  });

  test("extracts track_ID and timescale from version-1 (64-bit) tkhd and mdhd boxes", () => {

    /* Version-1 boxes shift the track_ID and timescale fields past the 64-bit creation/modification timestamps - track_ID at byte 28 in tkhd v1, timescale at
     * byte 28 in mdhd v1. A regression that hard-codes the v0 offset for v1 boxes would
     * produce zero or junk values here.
     */
    const trak = makeTrak(
      makeTkhd({ trackId: 42, version: 1 }),
      makeMdia(
        makeMdhd({ timescale: 48_000, version: 1 }),
        makeHdlr("soun")
      )
    );
    const moov = makeBox("moov", trak);
    const result = parseMoovTrackInfo(moov);

    const track = result.get(42);

    assert.ok(track, "v1 boxes still keyed by their track_ID");
    assert.equal(track.timescale, 48_000, "timescale read from mdhd v1 at byte-28 offset");
    assert.equal(track.handlerType, "soun");
  });

  test("returns separate entries per track for a multi-track moov", () => {

    // Locks the per-track partitioning: two trak boxes produce two map entries with their own (timescale, handlerType) tuples. A regression that overwrote
    // results across trak iterations would collapse to a single entry.
    const audioTrak = makeTrak(
      makeTkhd({ trackId: 1, version: 0 }),
      makeMdia(makeMdhd({ timescale: 48_000, version: 0 }), makeHdlr("soun"))
    );
    const videoTrak = makeTrak(
      makeTkhd({ trackId: 2, version: 0 }),
      makeMdia(makeMdhd({ timescale: 90_000, version: 0 }), makeHdlr("vide"))
    );
    const moov = makeBox("moov", Buffer.concat([ audioTrak, videoTrak ]));
    const result = parseMoovTrackInfo(moov);

    assert.equal(result.size, 2, "two tracks surfaced");
    assert.equal(result.get(1)?.timescale, 48_000);
    assert.equal(result.get(1)?.handlerType, "soun");
    assert.equal(result.get(2)?.timescale, 90_000);
    assert.equal(result.get(2)?.handlerType, "vide");
  });

  test("rejects a track whose mdhd reports timescale 0 (the > 0 guard)", () => {

    // Boundary: the implementation requires timescale > 0 before recording the track. Zero timescale would cause divide-by-zero in downstream conversions to
    // seconds, so rejecting at parse time is the right contract.
    const trak = makeTrak(
      makeTkhd({ trackId: 5, version: 0 }),
      makeMdia(makeMdhd({ timescale: 0, version: 0 }), makeHdlr("vide"))
    );
    const moov = makeBox("moov", trak);
    const result = parseMoovTrackInfo(moov);

    assert.equal(result.size, 0, "timescale 0 rejected");
  });
});
