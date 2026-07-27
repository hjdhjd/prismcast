/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * codecInference.test.ts: Unit tests for HLS media playlist codec inference. Each exported layer is tested in isolation:
 *
 *  1. findFirstSegmentUrl - playlist parsing and URL resolution, fully synchronous.
 *  2. inferCodecFromTsBuffer - TS PAT/PMT parsing against synthetic byte fixtures, fully synchronous.
 *  3. inferMediaCodec - the async orchestrator that fetches a segment prefix and runs the parser.
 *
 * Synthetic TS fixtures are built by buildTsFixture(), which constructs a one-program PAT and a single PMT with the requested elementary stream types. Real TS
 * captures are large and carry CRC32-correct sections; the parser does not validate CRCs (parsing for codec inference is best-effort), so the fixtures omit
 * the CRC trailer to keep the test setup small while still exercising the production code path.
 */
import { afterEach, describe, mock, test } from "node:test";
import { findFirstSegmentUrl, inferCodecFromTsBuffer, inferMediaCodec } from "./codecInference.ts";
import assert from "node:assert/strict";

/* Fills a 188-byte TS packet with sync byte, PID, payload-unit-start indicator, adaptation_field_control=01 (payload only), and a section payload preceded by
 * pointer_field=0. The remainder of the packet is padded with 0xFF bytes (TS stuffing convention) so the buffer is well-formed even though the parser does not
 * validate the trailing region.
 */
function buildTsPacket(pid: number, sectionPayload: Buffer): Buffer {

  const packet = Buffer.alloc(188, 0xFF);

  // Byte 0: sync_byte (0x47).
  packet[0] = 0x47;

  // Byte 1: TEI(0) | payload_unit_start_indicator(1) | priority(0) | PID_high(5).
  // We always set PUSI=1 because the section starts in this packet.
  packet[1] = 0x40 | ((pid >> 8) & 0x1F);

  // Byte 2: PID_low(8).
  packet[2] = pid & 0xFF;

  // Byte 3: scrambling(00) | adaptation_field_control(01) | continuity_counter(0000).
  // adaptation=01 means "payload only, no adaptation field".
  packet[3] = 0x10;

  // Byte 4 (start of payload): pointer_field=0 (section starts immediately after).
  packet[4] = 0x00;

  // Section payload starts at byte 5.
  sectionPayload.copy(packet, 5);

  return packet;
}

/* Builds a synthetic Program Association Table (PAT) section that declares one program with the given PMT PID. The section omits the trailing 4-byte CRC32
 * because the parser does not validate it, but it still emits a section_length consistent with the section structure: header(3) + body(5 + 4 program record)
 * + CRC(4) = 16 bytes total, with section_length covering everything after the section_length field itself (i.e., 13 bytes).
 */
function buildPatSection(programNumber: number, pmtPid: number): Buffer {

  const section = Buffer.alloc(16, 0x00);

  // Byte 0: table_id (0x00 = PAT).
  section[0] = 0x00;

  // Bytes 1-2: section_syntax_indicator(1) | reserved(011) | section_length(12).
  // section_length = (everything after this field) = 5 (TS id + version + section_no + last_section_no) + 4 (program record) + 4 (CRC) = 13.
  // High nibble: 1011_0000 = 0xB0; low byte: 0x0D.
  section[1] = 0xB0;
  section[2] = 0x0D;

  // Bytes 3-4: transport_stream_id (16 bits). Any value works.
  section[3] = 0x00;
  section[4] = 0x01;

  // Byte 5: reserved(11) | version_number(00000) | current_next_indicator(1).
  section[5] = 0xC1;

  // Byte 6: section_number.
  section[6] = 0x00;

  // Byte 7: last_section_number.
  section[7] = 0x00;

  // Bytes 8-9: program_number (16 bits). Non-zero means this is a PMT entry, not the network PID.
  section[8] = (programNumber >> 8) & 0xFF;
  section[9] = programNumber & 0xFF;

  // Bytes 10-11: reserved(111) | program_map_PID(13 bits).
  section[10] = 0xE0 | ((pmtPid >> 8) & 0x1F);
  section[11] = pmtPid & 0xFF;

  // Bytes 12-15: CRC32 (left as zeros - parser ignores it).

  return section;
}

/* Builds a synthetic Program Map Table (PMT) section with the given list of (stream_type, elementary_PID) pairs. Like the PAT helper, the CRC32 is omitted
 * (zeroed) and section_length is computed to cover everything after the section_length field including the CRC trailer.
 */
function buildPmtSection(programNumber: number, streams: readonly { pid: number; streamType: number }[]): Buffer {

  // Fixed header: table_id(1) + section_syntax/length(2) + program_number(2) + version/current_next(1) + section_number(1) + last_section_number(1) +
  // reserved/PCR_PID(2) + reserved/program_info_length(2) = 12 bytes. Per-stream record: stream_type(1) + reserved/elementary_PID(2) +
  // reserved/ES_info_length(2) = 5 bytes. CRC trailer = 4 bytes.
  const headerLength = 12;
  const recordLength = 5;
  const crcLength = 4;
  const totalSize = headerLength + (streams.length * recordLength) + crcLength;
  const section = Buffer.alloc(totalSize, 0x00);

  // Byte 0: table_id (0x02 = PMT).
  section[0] = 0x02;

  // Bytes 1-2: section_syntax_indicator(1) | reserved(011) | section_length(12). Section length covers bytes 3..end.
  const sectionLength = totalSize - 3;

  section[1] = 0xB0 | ((sectionLength >> 8) & 0x0F);
  section[2] = sectionLength & 0xFF;

  // Bytes 3-4: program_number (16 bits).
  section[3] = (programNumber >> 8) & 0xFF;
  section[4] = programNumber & 0xFF;

  // Byte 5: reserved(11) | version_number(00000) | current_next_indicator(1).
  section[5] = 0xC1;

  // Byte 6: section_number.
  section[6] = 0x00;

  // Byte 7: last_section_number.
  section[7] = 0x00;

  // Bytes 8-9: reserved(111) | PCR_PID(13). PCR_PID is conventionally the video stream's PID; we use 0 for simplicity.
  section[8] = 0xE0;
  section[9] = 0x00;

  // Bytes 10-11: reserved(1111) | program_info_length(12). Zero means no program-level descriptors.
  section[10] = 0xF0;
  section[11] = 0x00;

  // Per-stream records starting at byte 12.
  let cursor = headerLength;

  for(const stream of streams) {

    section[cursor] = stream.streamType;
    section[cursor + 1] = 0xE0 | ((stream.pid >> 8) & 0x1F);
    section[cursor + 2] = stream.pid & 0xFF;
    section[cursor + 3] = 0xF0;
    section[cursor + 4] = 0x00;
    cursor += recordLength;
  }

  // CRC32 left as zeros at cursor..cursor+3.

  return section;
}

/* Composes a synthetic TS-formatted buffer containing a PAT packet referencing one PMT, then a PMT packet declaring the given elementary streams.
 */
function buildTsFixture(streams: readonly { pid: number; streamType: number }[]): Buffer {

  const programNumber = 1;
  const pmtPid = 0x100;
  const patPacket = buildTsPacket(0, buildPatSection(programNumber, pmtPid));
  const pmtPacket = buildTsPacket(pmtPid, buildPmtSection(programNumber, streams));

  return Buffer.concat([ patPacket, pmtPacket ]);
}

describe("findFirstSegmentUrl", () => {

  test("returns the first segment URL after an #EXTINF tag", () => {

    // Happy path: the canonical media-playlist shape. The URL on the line immediately after #EXTINF is the segment.
    const body = "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nsegment0.ts\n#EXTINF:6.0,\nsegment1.ts\n";
    const result = findFirstSegmentUrl(body, "https://cdn.test/path/playlist.m3u8");

    assert.equal(result, "https://cdn.test/path/segment0.ts");
  });

  test("resolves an absolute segment URL verbatim against the base URL", () => {

    // Boundary: an absolute segment URL must not be re-resolved relative to the base. Locks the resolveUrl boundary contract.
    const body = "#EXTINF:6.0,\nhttps://cdn.test/abs/segment.ts\n";
    const result = findFirstSegmentUrl(body, "https://other.test/playlist.m3u8");

    assert.equal(result, "https://cdn.test/abs/segment.ts");
  });

  test("skips comment lines and tag lines that follow #EXTINF", () => {

    // Boundary: a comment line (starts with #) between #EXTINF and the segment URL must be ignored. The walker continues until it sees a non-tag, non-empty line.
    // We do NOT skip a tag line altogether because in real playlists a stray tag would invalidate the playlist - but the walker is permissive enough to keep
    // searching, which protects against benign comment lines.
    const body = "#EXTINF:6.0,\n# a comment line\nsegment0.ts\n";
    const result = findFirstSegmentUrl(body, "https://cdn.test/path/playlist.m3u8");

    assert.equal(result, "https://cdn.test/path/segment0.ts");
  });

  test("returns null when the playlist has no #EXTINF tag", () => {

    // Negative test: a header-only playlist (e.g., a live playlist that has not yet rolled in any segments) must yield null. Without this, codec inference
    // would attempt to fetch a non-segment URL.
    const body = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n";

    assert.equal(findFirstSegmentUrl(body, "https://cdn.test/playlist.m3u8"), null);
  });

  test("returns null when #EXTINF appears at the end of the body with no following URL", () => {

    // Negative test: a truncated playlist whose final line is the #EXTINF tag (no URL after) must yield null rather than reading past the buffer end.
    const body = "#EXTINF:6.0,\n";

    assert.equal(findFirstSegmentUrl(body, "https://cdn.test/playlist.m3u8"), null);
  });
});

describe("inferCodecFromTsBuffer", () => {

  test("returns 'H264' for a PMT declaring stream_type 0x1B", () => {

    // Happy path: stream_type 0x1B is the H.264 elementary stream type assigned by ISO/IEC 13818-1 Amendment 3. The parser must walk PAT -> PMT -> first ES
    // record and return the H264 label.
    const buffer = buildTsFixture([{ pid: 0x101, streamType: 0x1B }]);

    assert.equal(inferCodecFromTsBuffer(buffer), "H264");
  });

  test("returns 'HEVC' for a PMT declaring stream_type 0x24", () => {

    // Happy path: stream_type 0x24 is the HEVC elementary stream type. Locks the codec mapping.
    const buffer = buildTsFixture([{ pid: 0x101, streamType: 0x24 }]);

    assert.equal(inferCodecFromTsBuffer(buffer), "HEVC");
  });

  test("returns the codec from the first recognized video stream when audio precedes video in the PMT", () => {

    // Boundary: PMTs commonly list audio first (ADTS AAC audio, stream_type 0x0F) and video second. The parser must skip past the audio record (which is not in the
    // codec map) and find the video record. We use stream_type 0x0F (audio) followed by 0x1B (H264).
    const buffer = buildTsFixture([
      { pid: 0x100, streamType: 0x0F },
      { pid: 0x101, streamType: 0x1B }
    ]);

    assert.equal(inferCodecFromTsBuffer(buffer), "H264");
  });

  test("returns null when the PMT declares no recognized video stream type", () => {

    // Negative test: a PMT with only audio (no video stream_type in the codec map) must return null. The streaming pipeline still works without a codec label;
    // the status display falls back to no badge.
    const buffer = buildTsFixture([{ pid: 0x100, streamType: 0x0F }]);

    assert.equal(inferCodecFromTsBuffer(buffer), null);
  });

  test("returns null when the buffer contains no PAT packet", () => {

    // Negative test: a buffer with no PAT (PID 0) cannot resolve to a PMT. Locks the early-exit on missing PAT.
    const buffer = Buffer.alloc(188 * 2, 0x00);

    // Set sync byte but use a non-PAT PID throughout.
    for(let offset = 0; offset < buffer.length; offset += 188) {

      buffer[offset] = 0x47;
      // PID 0x100 in bytes 1-2.
      buffer[offset + 1] = 0x41;
      buffer[offset + 2] = 0x00;
    }

    assert.equal(inferCodecFromTsBuffer(buffer), null);
  });

  test("returns null when the PAT references a PMT PID that is not present in the buffer", () => {

    // Negative test: the PAT can correctly reference a PMT PID that the buffer prefix does not include (e.g., the PMT is later in the segment than what we
    // fetched). The parser must handle the missing-PMT case without throwing.
    const programNumber = 1;
    const pmtPid = 0x100;
    const patPacket = buildTsPacket(0, buildPatSection(programNumber, pmtPid));

    // Concatenate just the PAT - no PMT packet.
    assert.equal(inferCodecFromTsBuffer(patPacket), null);
  });

  test("returns null when the buffer is shorter than a single TS packet", () => {

    // Boundary: the parser must not crash on undersized buffers (e.g., a fetch that returned far less data than expected).
    assert.equal(inferCodecFromTsBuffer(Buffer.alloc(50)), null);
  });

  test("returns null when the buffer starts with the wrong sync byte", () => {

    // Negative test: a buffer that does not begin with 0x47 is not a TS stream. Locks the sync-byte check at packet 0.
    const buffer = Buffer.alloc(188 * 2, 0x00);

    assert.equal(inferCodecFromTsBuffer(buffer), null);
  });
});

describe("inferMediaCodec (async orchestrator)", () => {

  afterEach(() => {

    mock.reset();
  });

  test("returns codec=null when the playlist has no segments", async () => {

    // Negative orchestrator path: no #EXTINF in the body means there is no segment to fetch. Must short-circuit without making any HTTP calls.
    let fetchCalls = 0;

    mock.method(globalThis, "fetch", async () => {

      fetchCalls++;

      return new Response("should not fetch", { status: 200 });
    });

    const result = await inferMediaCodec({ baseUrl: "https://cdn.test/playlist.m3u8", playlistBody: "#EXTM3U\n#EXT-X-TARGETDURATION:6\n" });

    assert.equal(result.codec, null);
    assert.equal(fetchCalls, 0, "no fetch when there is no segment");
  });

  test("returns codec=null when the first segment is not .ts (out of scope for this commit)", async () => {

    // Boundary: fMP4 segments (.m4s, .cmfv) need the moov box from #EXT-X-MAP, which is a future extension. The orchestrator must short-circuit without
    // attempting to fetch the segment, because the parser cannot make sense of fMP4 bytes anyway.
    let fetchCalls = 0;

    mock.method(globalThis, "fetch", async () => {

      fetchCalls++;

      return new Response("should not fetch", { status: 200 });
    });

    const result = await inferMediaCodec({

      baseUrl: "https://cdn.test/playlist.m3u8",
      playlistBody: "#EXTM3U\n#EXTINF:2,\nseg.m4s\n"
    });

    assert.equal(result.codec, null);
    assert.equal(fetchCalls, 0, "no fetch for non-TS segment");
  });

  test("returns the inferred codec from a TS segment fetch (happy path)", async () => {

    // Happy path: the playlist references a .ts segment, the fetch returns synthetic TS bytes carrying an H264 PMT, and the orchestrator returns the H264 label.
    const ts = buildTsFixture([{ pid: 0x101, streamType: 0x1B }]);
    const segUrl = "https://cdn.test/path/seg.ts";

    mock.method(globalThis, "fetch", async (url: string | URL): Promise<Response> => {

      if(url.toString() === segUrl) {

        return new Response(new Uint8Array(ts), { status: 206 });
      }

      return new Response("not found", { status: 404 });
    });

    const result = await inferMediaCodec({

      baseUrl: "https://cdn.test/path/playlist.m3u8",
      playlistBody: "#EXTM3U\n#EXTINF:2,\nseg.ts\n"
    });

    assert.equal(result.codec, "H264");
  });

  test("caps the read at 32 KB when the CDN ignores Range and streams the full segment (HTTP 200)", async () => {

    /* When a CDN ignores the Range header it responds 200 OK and streams the entire segment, which can be several megabytes. The orchestrator must
     * read only the documented 32 KB prefix regardless, draining just enough of the body to parse the PAT/PMT and then cancelling the rest. We prove the cap holds
     * by serving a ReadableStream whose first chunk carries the valid TS fixture and whose tail is a multi-megabyte flood emitted in small chunks. The stream counts
     * how many bytes the consumer pulled and whether it was cancelled. A consumer that buffered the whole body unconditionally, rather than capping via the
     * stream-reader loop, would pull every byte; the capped consumer must stop within one chunk of the 32 KB bound and cancel.
     */
    const ts = buildTsFixture([{ pid: 0x101, streamType: 0x1B }]);
    const segUrl = "https://cdn.test/path/seg.ts";
    const cap = 32 * 1024;
    const chunkSize = 4 * 1024;
    const tailBytes = 4 * 1024 * 1024;

    let pulledBytes = 0;
    let cancelled = false;

    mock.method(globalThis, "fetch", async (url: string | URL): Promise<Response> => {

      if(url.toString() !== segUrl) {

        return new Response("not found", { status: 404 });
      }

      let emitted = 0;

      const body = new ReadableStream<Uint8Array>({

        cancel(): void {

          cancelled = true;
        },

        pull(controller): void {

          // First pull delivers the TS fixture so the parser can read the PAT/PMT from the prefix.
          if(emitted === 0) {

            const head = new Uint8Array(ts);

            pulledBytes += head.length;
            emitted += head.length;
            controller.enqueue(head);

            return;
          }

          // Subsequent pulls deliver the multi-megabyte tail in small chunks. A capped consumer stops requesting these well before the tail is exhausted.
          if(emitted >= (ts.length + tailBytes)) {

            controller.close();

            return;
          }

          const chunk = new Uint8Array(chunkSize);

          pulledBytes += chunk.length;
          emitted += chunk.length;
          controller.enqueue(chunk);
        }
      });

      // Status 200 (not 206) signals the CDN ignored the Range header and is streaming the whole segment.
      return new Response(body, { status: 200 });
    });

    const result = await inferMediaCodec({

      baseUrl: "https://cdn.test/path/playlist.m3u8",
      playlistBody: "#EXTM3U\n#EXTINF:2,\nseg.ts\n"
    });

    // The codec is still inferred from the prefix that did arrive.
    assert.equal(result.codec, "H264");

    // The consumer must not have drained the whole body - it stops near the cap and cancels the rest, nowhere near the multi-megabyte tail. We allow up to cap +
    // two chunks of slack: one for the chunk that straddles the cap boundary (accepted in part), and one for the stream's default highWaterMark=1 pull-ahead, which
    // refills the queue with one more chunk after the satisfying read before the cancel lands.
    assert.ok(pulledBytes <= (cap + (2 * chunkSize)), "consumer pulled at most the 32 KB prefix plus a straddling chunk and one pull-ahead, not the multi-megabyte tail");
    assert.equal(cancelled, true, "consumer cancelled the stream once it had the prefix");
  });

  test("returns codec=null when the segment fetch returns a non-2xx response", async () => {

    // Negative path: the CDN may reject the segment fetch (token expired between probe and inference). Codec inference must fail gracefully because the
    // streaming pipeline does not depend on the codec label.
    mock.method(globalThis, "fetch", async (): Promise<Response> => new Response("forbidden", { status: 403 }));

    const result = await inferMediaCodec({

      baseUrl: "https://cdn.test/path/playlist.m3u8",
      playlistBody: "#EXTM3U\n#EXTINF:2,\nseg.ts\n"
    });

    assert.equal(result.codec, null);
  });

  test("returns codec=null when the segment fetch throws", async () => {

    // Negative path: the fetch itself can reject (network error, abort). The orchestrator must surface null rather than propagating.
    mock.method(globalThis, "fetch", async () => {

      throw new Error("synthetic network failure");
    });

    const result = await inferMediaCodec({

      baseUrl: "https://cdn.test/path/playlist.m3u8",
      playlistBody: "#EXTM3U\n#EXTINF:2,\nseg.ts\n"
    });

    assert.equal(result.codec, null);
  });

  test("strips query parameters before checking the .ts extension", async () => {

    // Boundary: HLS segment URIs commonly carry signed-token query strings (e.g., seg.ts?token=abc). The orchestrator must check the path component only, not the
    // raw URL with query, when determining whether to attempt TS parsing.
    const ts = buildTsFixture([{ pid: 0x101, streamType: 0x1B }]);
    const segUrl = "https://cdn.test/path/seg.ts?token=abc";

    mock.method(globalThis, "fetch", async (url: string | URL): Promise<Response> => {

      if(url.toString() === segUrl) {

        return new Response(new Uint8Array(ts), { status: 206 });
      }

      return new Response("not found", { status: 404 });
    });

    const result = await inferMediaCodec({

      baseUrl: "https://cdn.test/path/playlist.m3u8",
      playlistBody: "#EXTM3U\n#EXTINF:2,\nseg.ts?token=abc\n"
    });

    assert.equal(result.codec, "H264");
  });
});

/* Fills a 188-byte TS packet whose adaptation_field_control is 11 (adaptation field followed by payload) - the shape a live TS packet carrying a PCR uses. The
 * adaptation field is left as 0xFF stuffing of the requested length; only its length byte is meaningful to the parser, which must skip past it to reach the payload.
 */
function buildTsPacketWithAdaptation(pid: number, sectionPayload: Buffer, adaptationLength: number): Buffer {

  const packet = Buffer.alloc(188, 0xFF);

  // Byte 0: sync_byte (0x47).
  packet[0] = 0x47;

  // Byte 1: payload_unit_start_indicator(1) plus the high 5 bits of the PID.
  packet[1] = 0x40 | ((pid >> 8) & 0x1F);

  // Byte 2: the low 8 bits of the PID.
  packet[2] = pid & 0xFF;

  // Byte 3: scrambling(00) | adaptation_field_control(11) | continuity_counter(0000). 0x30 selects "adaptation field followed by payload".
  packet[3] = 0x30;

  // Byte 4: adaptation_field_length, the count of adaptation bytes that follow. The stuffing itself stays 0xFF; the parser only consumes the length to skip past it.
  packet[4] = adaptationLength;

  // The payload begins after the 4-byte header, the length byte, and the adaptation field. Its first byte is the pointer_field (0 = the section starts immediately).
  const payloadStart = 5 + adaptationLength;

  packet[payloadStart] = 0x00;
  sectionPayload.copy(packet, payloadStart + 1);

  return packet;
}

/* Builds a PAT section that declares several program records in order. Unlike buildPatSection, this variant emits multiple 4-byte records so tests can exercise the
 * walk that skips a program_number == 0 (network PID) record and continues to the next. The trailing CRC32 is omitted (zeroed) exactly as the single-program helper.
 */
function buildPatSectionMulti(programs: readonly { pid: number; programNumber: number }[]): Buffer {

  // section_length covers everything after the section_length field: 5 fixed bytes (TS id + version + section_no + last_section_no) + 4 per program + 4-byte CRC.
  const bodyLength = 5 + (programs.length * 4) + 4;
  const totalSize = 3 + bodyLength;
  const section = Buffer.alloc(totalSize, 0x00);

  // Byte 0: table_id (0x00 = PAT).
  section[0] = 0x00;

  // Bytes 1-2: section_syntax_indicator(1) | reserved(011) | section_length(12).
  section[1] = 0xB0 | ((bodyLength >> 8) & 0x0F);
  section[2] = bodyLength & 0xFF;

  // Bytes 3-4: transport_stream_id.
  section[3] = 0x00;
  section[4] = 0x01;

  // Byte 5: reserved(11) | version_number(00000) | current_next_indicator(1).
  section[5] = 0xC1;

  // Byte 6: section_number.
  section[6] = 0x00;

  // Byte 7: last_section_number.
  section[7] = 0x00;

  // Program records start at byte 8. A program_number of 0 declares the network PID rather than a PMT PID; the parser must skip such records.
  let cursor = 8;

  for(const program of programs) {

    section[cursor] = (program.programNumber >> 8) & 0xFF;
    section[cursor + 1] = program.programNumber & 0xFF;
    section[cursor + 2] = 0xE0 | ((program.pid >> 8) & 0x1F);
    section[cursor + 3] = program.pid & 0xFF;
    cursor += 4;
  }

  return section;
}

/* Composes a TS fixture whose PAT packet carries a 7-byte adaptation field (the length a PCR-only adaptation uses) ahead of its payload, then a normal PMT packet.
 * Correct parsing must consume adaptation_field_length to land on the real PSI section.
 */
function buildTsFixtureAdaptationPat(streams: readonly { pid: number; streamType: number }[]): Buffer {

  const programNumber = 1;
  const pmtPid = 0x100;
  const patPacket = buildTsPacketWithAdaptation(0, buildPatSection(programNumber, pmtPid), 7);
  const pmtPacket = buildTsPacket(pmtPid, buildPmtSection(programNumber, streams));

  return Buffer.concat([ patPacket, pmtPacket ]);
}

describe("inferCodecFromTsBuffer (adaptation_field_control handling)", () => {

  test("returns null when the PAT packet declares adaptation_field_control=2 (adaptation only, no payload)", () => {

    // Sanity: the unmutated fixture describes an H264 stream, so any difference below is caused solely by the adaptation_field_control mutation.
    assert.equal(inferCodecFromTsBuffer(buildTsFixture([{ pid: 0x101, streamType: 0x1B }])), "H264");

    const buffer = buildTsFixture([{ pid: 0x101, streamType: 0x1B }]);

    // Byte 3 of the first packet is adaptation_field_control. 0x20 sets it to 10 (adaptation only), which carries no payload, so payloadOffsetForPacket returns null
    // and the PAT is unreadable. A parser that treated adaptation-only as payload-bearing would still read the section and wrongly return H264.
    buffer[3] = 0x20;

    assert.equal(inferCodecFromTsBuffer(buffer), null);
  });

  test("skips the adaptation field using adaptation_field_length when adaptation_field_control=3", () => {

    // The PAT packet carries a 7-byte adaptation field ahead of its payload (the live PCR shape). Correct parsing consumes adaptation_field_length to reach the PSI
    // section; a parser that ignored the length would misread the section start and return null instead of H264.
    const buffer = buildTsFixtureAdaptationPat([{ pid: 0x101, streamType: 0x1B }]);

    assert.equal(inferCodecFromTsBuffer(buffer), "H264");
  });
});

describe("inferCodecFromTsBuffer (PAT program walk)", () => {

  test("skips a PAT record whose program_number is 0 (network PID) and continues to the next record", () => {

    // The first program record declares the network PID (program_number 0); the second declares the real PMT PID. The walk must skip the first and resolve the
    // second, where the H264 PMT lives. A parser that returned the first record would resolve the network PID, find no matching PMT packet, and return null.
    const networkPid = 0x1F;
    const pmtPid = 0x100;
    const patSection = buildPatSectionMulti([
      { pid: networkPid, programNumber: 0 },
      { pid: pmtPid, programNumber: 1 }
    ]);
    const patPacket = buildTsPacket(0, patSection);
    const pmtPacket = buildTsPacket(pmtPid, buildPmtSection(1, [{ pid: 0x101, streamType: 0x1B }]));
    const buffer = Buffer.concat([ patPacket, pmtPacket ]);

    assert.equal(inferCodecFromTsBuffer(buffer), "H264");
  });
});

describe("inferCodecFromTsBuffer (malformed PSI handling)", () => {

  test("returns null when the PAT packet has payload_unit_start_indicator unset", () => {

    const buffer = buildTsFixture([{ pid: 0x101, streamType: 0x1B }]);

    // Byte 1 bit 6 (0x40) is PUSI. For the PAT packet (PID 0) clearing PUSI leaves byte 1 at 0x00, marking the packet as carrying section continuation rather than a
    // section start, so sectionOffsetForPacket cannot locate the PSI section and inference returns null.
    buffer[1] = 0x00;

    assert.equal(inferCodecFromTsBuffer(buffer), null);
  });

  test("returns null when the PAT section_length declares more bytes than the buffer holds", () => {

    const patSection = buildPatSection(1, 0x100);

    // Bytes 1-2 hold section_length. Widening it to 0xFFF pushes the section end far past the single-packet buffer; parsePatForPmtPid must reject the section via its
    // sectionEnd > buffer.length guard rather than read out of bounds.
    patSection[1] = 0xBF;
    patSection[2] = 0xFF;

    const buffer = buildTsPacket(0, patSection);

    assert.equal(inferCodecFromTsBuffer(buffer), null);
  });

  test("returns null when the PAT section carries a table_id other than 0x00", () => {

    const patSection = buildPatSection(1, 0x100);

    // table_id 0x00 identifies a PAT. Any other value means this is not a PAT section, so parsePatForPmtPid must reject it and inference returns null.
    patSection[0] = 0x42;

    const buffer = buildTsPacket(0, patSection);

    assert.equal(inferCodecFromTsBuffer(buffer), null);
  });

  test("returns null when the PMT section carries a table_id other than 0x02", () => {

    // A valid PAT points at the PMT PID, but the PMT section's table_id is corrupted away from 0x02, so parsePmtForVideoStreamType must reject it and inference
    // returns null even though a video stream_type is present in the record.
    const programNumber = 1;
    const pmtPid = 0x100;
    const patPacket = buildTsPacket(0, buildPatSection(programNumber, pmtPid));
    const pmtSection = buildPmtSection(programNumber, [{ pid: 0x101, streamType: 0x1B }]);

    pmtSection[0] = 0x03;

    const pmtPacket = buildTsPacket(pmtPid, pmtSection);
    const buffer = Buffer.concat([ patPacket, pmtPacket ]);

    assert.equal(inferCodecFromTsBuffer(buffer), null);
  });
});

describe("inferMediaCodec (fallback and audio-only paths)", () => {

  afterEach(() => {

    mock.reset();
  });

  test("falls back to arrayBuffer() when the response exposes no readable body stream", async () => {

    // Some runtimes and test doubles expose only arrayBuffer() with a null body. readStreamPrefix must fall back to buffering the whole body and truncating to the
    // cap, so a codec is still inferred from the TS bytes. The 64 KB filler past the fixture makes the buffered body exceed the 32 KB cap, exercising the truncation
    // slice; the truncation keeps the leading PAT/PMT so the H264 label still emerges.
    const ts = buildTsFixture([{ pid: 0x101, streamType: 0x1B }]);
    const full = Buffer.concat([ ts, Buffer.alloc(64 * 1024, 0xFF) ]);
    const bodyBytes = new ArrayBuffer(full.length);

    new Uint8Array(bodyBytes).set(full);

    let arrayBufferCalls = 0;

    mock.method(globalThis, "fetch", async (): Promise<Response> => {

      const fake = {

        arrayBuffer: async (): Promise<ArrayBuffer> => {

          arrayBufferCalls++;

          return bodyBytes;
        },
        body: null,
        ok: true,
        status: 206
      };

      return fake as unknown as Response;
    });

    const result = await inferMediaCodec({ baseUrl: "https://cdn.test/path/playlist.m3u8", playlistBody: "#EXTM3U\n#EXTINF:2,\nseg.ts\n" });

    assert.equal(result.codec, "H264");
    assert.equal(arrayBufferCalls, 1, "the null-body path must read via arrayBuffer()");
  });

  test("returns codec=null via the parser else-branch for a TS segment whose PMT lists only audio", async () => {

    // Distinct from the no-segment and non-TS short-circuits: here the segment IS fetched and parsed, but its PMT declares only an audio stream_type (0x0F), so
    // inferCodecFromTsBuffer returns null and the orchestrator reports codec=null through the else-branch. The fetch count proves the segment was actually retrieved.
    const ts = buildTsFixture([{ pid: 0x100, streamType: 0x0F }]);
    const segUrl = "https://cdn.test/path/seg.ts";

    let fetchCalls = 0;

    mock.method(globalThis, "fetch", async (url: string | URL): Promise<Response> => {

      fetchCalls++;

      if(url.toString() === segUrl) {

        return new Response(new Uint8Array(ts), { status: 206 });
      }

      return new Response("not found", { status: 404 });
    });

    const result = await inferMediaCodec({ baseUrl: "https://cdn.test/path/playlist.m3u8", playlistBody: "#EXTM3U\n#EXTINF:2,\nseg.ts\n" });

    assert.equal(result.codec, null);
    assert.ok(fetchCalls > 0, "the segment must be fetched and parsed, unlike the short-circuit null paths");
  });
});
