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
import { firstOf } from "../testing.helpers.ts";

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

  test("does not park toward an impossibly large declared box size; resyncs to the next valid box", () => {

    /* A corrupt header that declares a box larger than the sane ceiling (64 MB) must not make the parser buffer incoming chunks indefinitely waiting
     * for a payload that never arrives. The parser must treat the framing as lost and resync one byte at a time. We prove this is observable: a valid ftyp box that
     * follows the corrupt header emits immediately - it could only do so if the parser refused to wait for the impossible box and resynced past it. The corrupt
     * header's four type bytes are zero so the resync walks cleanly (each zero is a size-0 marker the parser skips one byte at a time) straight onto the ftyp.
     */
    const seen: string[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box.type));

    // 8-byte header declaring a 256 MB box (0x10000000), well past the 64 MB ceiling, with zero type bytes, followed by a real ftyp box.
    const corrupt = Buffer.alloc(8);

    corrupt.writeUInt32BE(0x10000000, 0);

    parser.push(Buffer.concat([ corrupt, makeBox("ftyp") ]));

    assert.ok(seen.includes("ftyp"), "parser resynced past the impossibly large box and emitted the following ftyp");
  });

  test("emits a legitimately large box whose declared size sits at the ceiling", () => {

    // Boundary symmetric with the rejection test above: a box declaring exactly the ceiling size is valid framing and must still be emitted once its bytes arrive.
    // This guards against an off-by-one that would wrongly reject the largest permitted box. We allocate the full ceiling-sized buffer (64 MB) with a header
    // declaring exactly the ceiling, then push it so the box completes and must be emitted.
    const seen: string[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box.type));
    const ceiling = 64 * 1024 * 1024;
    const box = Buffer.alloc(ceiling);

    box.writeUInt32BE(ceiling, 0);
    box.write("mdat", 4, 4, "ascii");

    parser.push(box);

    assert.deepEqual(seen, ["mdat"], "a box declared at exactly the ceiling is accepted and emitted");
  });

  test("does not park indefinitely toward a never-completing oversized box; later valid boxes still flow", () => {

    /* The pending buffer must stay bounded even when a misbehaving source streams chunk after chunk that belongs to a box that never completes. The
     * unbounded case is precisely an oversized declared size: the box ceiling rejects it so the parser never parks the incoming flood. We prove the parser keeps
     * working under this flood by interleaving valid boxes between oversized headers and asserting every valid box still emits - the parser cannot be stuck waiting
     * on the impossible box, because the ceiling forced a resync each time.
     */
    const seen: string[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box.type));

    // An 8-byte header declaring a 1 GB box (0x40000000), far past the 64 MB ceiling, with zero type bytes so the resync walks cleanly to the box that follows. On
    // its own this is exactly the framing that would otherwise grow the buffer without bound as more chunks arrive.
    const oversized = Buffer.alloc(8);

    oversized.writeUInt32BE(0x40000000, 0);

    // Push three rounds of [oversized header, valid box]. The oversized header forces a resync past its bytes; the very next valid box must emit, proving the
    // oversized declaration never trapped the parser into an unbounded wait for a payload that would never arrive.
    for(let i = 0; i < 3; i++) {

      parser.push(Buffer.concat([ oversized, makeBox("moof") ]));
    }

    assert.equal(seen.filter((type) => type === "moof").length, 3, "every valid box emitted despite repeated oversized-box framing");
  });

  test("emits an extended-size box (size field == 1), taking the box size from the low 32 bits of the 64-bit field", () => {

    // The extended-size form signals a 64-bit size with a size field of 1, followed by an 8-byte size at bytes 8-15. For streaming media the high 32 bits are always
    // zero, so the parser takes the low 32 bits as the box size. We build a 24-byte box (16-byte extended header + 8-byte payload) and verify it emits with the size
    // read from the low word, not from the size-field-1 sentinel.
    const seen: MP4Box[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box));
    const box = Buffer.alloc(24);

    box.writeUInt32BE(1, 0);
    box.write("mdat", 4, 4, "ascii");
    box.writeUInt32BE(0, 8);
    box.writeUInt32BE(24, 12);

    parser.push(box);

    assert.equal(seen.length, 1, "the extended-size box emitted");

    const emitted = firstOf(seen);

    assert.equal(emitted.type, "mdat");
    assert.equal(emitted.size, 24, "size taken from the low 32 bits of the extended-size field");
    assert.equal(emitted.data.length, 24, "the emitted box carries its full 24 bytes");
  });

  test("rejects an extended-size box whose high 32 bits are non-zero (a > 4 GB claim) rather than emitting it", () => {

    // A box declaring more than 4 GB via a non-zero high word is not legitimate streaming framing. Were the high word ignored, this otherwise well-formed 24-byte
    // frame would emit as a 24-byte box; the guard must instead resync so nothing is emitted. The distinct low word (24) is what an unguarded parser would wrongly
    // trust, so a zero emit count asserts the high-word rejection.
    const seen: MP4Box[] = [];
    const parser = createMP4BoxParser((box) => seen.push(box));
    const box = Buffer.alloc(24);

    box.writeUInt32BE(1, 0);
    box.write("mdat", 4, 4, "ascii");
    box.writeUInt32BE(1, 8);
    box.writeUInt32BE(24, 12);

    parser.push(box);

    assert.equal(seen.length, 0, "a box claiming > 4 GB via the high word is rejected, not emitted");
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
