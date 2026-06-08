/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * codecInference.test.ts: Unit tests for HLS media playlist codec inference. Three layers are tested in isolation:
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

    // Boundary: PMTs commonly list audio first (mp4a stream_type 0x0F) and video second. The parser must skip past the audio record (which is not in the
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

    /* Finding [19]: when a CDN ignores the Range header it responds 200 OK and streams the entire segment, which can be several megabytes. The orchestrator must
     * read only the documented 32 KB prefix regardless, draining just enough of the body to parse the PAT/PMT and then cancelling the rest. We prove the cap holds
     * by serving a ReadableStream whose first chunk carries the valid TS fixture and whose tail is a multi-megabyte flood emitted in small chunks. The stream counts
     * how many bytes the consumer pulled and whether it was cancelled. A consumer that buffered the whole body (the old arrayBuffer() path) would pull every byte;
     * the capped consumer must stop within one chunk of the 32 KB bound and cancel.
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
