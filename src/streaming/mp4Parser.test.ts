/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mp4Parser.test.ts: Unit tests for the basic box-parsing primitives in mp4Parser.ts - createMP4BoxParser (streaming chunked input), iterateChildBoxes (recursive
 * walk), and the MP4BoxCallback type contract. Fragment parsing (detectMoofKeyframe, offsetMoofTimestamps) lives in mp4Parser.fragments.test.ts; moov-track-info
 * parsing lives in mp4Parser.moov.test.ts; codec-config parsing lives in mp4Parser.codec.test.ts.
 *
 * The tests build minimal synthetic MP4 buffers - just enough box structure to exercise each parser path - rather than real fMP4 fixtures, which would couple
 * the test to a specific encoder.
 */
import type { MP4Box, MP4BoxCallback } from "./mp4Parser.ts";
import { createMP4BoxParser, iterateChildBoxes } from "./mp4Parser.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

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

/* makeTfhd, makeTrun, makeTfdt, makeTraf, makeMoof construct the fragment-related boxes used by iterateChildBoxes' nested-box assertions. The full fragment
 * parsing tests live in mp4Parser.fragments.test.ts; we keep these helpers here so the iterateChildBoxes describe can construct realistic input.
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

  const payload = Buffer.alloc(8 + optional.length * 4);

  payload.writeUInt32BE(flags, 0);
  payload.writeUInt32BE(options.trackId, 4);

  for(let i = 0; i < optional.length; i++) {

    payload.writeUInt32BE(optional[i] ?? 0, 8 + i * 4);
  }

  return makeBox("tfhd", payload);
}

function makeTrun(options: { firstSampleFlags?: number; sampleCount: number }): Buffer {

  let flags = 0;

  if(options.firstSampleFlags !== undefined) {

    flags |= 0x004;
  }

  const payloadSize = 8 + (options.firstSampleFlags !== undefined ? 4 : 0);
  const payload = Buffer.alloc(payloadSize);

  payload.writeUInt32BE(flags, 0);
  payload.writeUInt32BE(options.sampleCount, 4);

  if(options.firstSampleFlags !== undefined) {

    payload.writeUInt32BE(options.firstSampleFlags, 8);
  }

  return makeBox("trun", payload);
}

function makeTfdt(baseMediaDecodeTime: number): Buffer {

  const payload = Buffer.alloc(8);

  payload.writeUInt32BE(0, 0);
  payload.writeUInt32BE(baseMediaDecodeTime, 4);

  return makeBox("tfdt", payload);
}

function makeTraf(...children: Buffer[]): Buffer {

  return makeBox("traf", Buffer.concat(children));
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

  test("stops iterating on a malformed extended-size child whose 64-bit size is zero (must not hang)", () => {

    /* Boundary mirroring the non-extended zero-size case: a child with sizeField === 1 (extended-size sentinel) whose 64-bit size resolves to zero is malformed.
     * Without the extended-branch lower-bound guard this would advance pos by zero and spin the loop forever, hanging the event loop. The node:test per-test
     * timeout is the backstop that would catch a regression; a clean run proves the guard stops iteration. We build the 16-byte extended header by hand:
     * size field 1, type "tfhd", high 32 bits 0, low 32 bits 0.
     */
    // 16 bytes: 4 (size field) + 4 (type) + 8 (64-bit extended size). Matches the production EXTENDED_HEADER_SIZE.
    const extZeroChild = Buffer.alloc(16);

    extZeroChild.writeUInt32BE(1, 0);
    extZeroChild.write("tfhd", 4, 4, "ascii");
    extZeroChild.writeUInt32BE(0, 8);
    extZeroChild.writeUInt32BE(0, 12);

    const traf = makeBox("traf", extZeroChild);
    const seen: string[] = [];

    iterateChildBoxes(traf, (type) => seen.push(type));

    assert.deepEqual(seen, [], "no children emitted on a zero-valued extended-size child");
  });
});

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
