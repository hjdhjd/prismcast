/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * codecInference.ts: Inference of the video codec carried by an HLS media playlist by parsing the first segment.
 */
import { LOG, chromeFetch } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import { resolveUrl } from "./probe.ts";

/* Media-only HLS playlists do not declare codec information - that lives in the master playlist's #EXT-X-STREAM-INF CODECS attribute, which the master-playlist
 * resolver consumes today. For media-only feeds (no master playlist exists), the codec must be inferred from the segments themselves so the MediaFeed surfaces
 * the same codec/resolution metadata regardless of which playlist kind arrived. This module provides the inference seam.
 *
 * The inference path supported in this module is the MPEG-TS Program Map Table (PMT). HLS streams using TS segments expose the elementary stream's stream_type
 * directly in the PMT, which is small enough to reach within the first ~2 KB of every segment. PMT-based inference is bounded, deterministic, and avoids the
 * complexity of full codec-config parsing (SPS NALU walking) because the stream_type byte alone is sufficient to label the codec for status display.
 *
 * fMP4 segments declare codec information in the moov box of an init segment referenced by #EXT-X-MAP. That path is intentionally out of scope for this module
 * and lives behind a clean extension seam: future work can dispatch on the segment's file extension or content type and call parseMoovCodecConfig() from
 * src/streaming/mp4Parser.ts. Resolution is not inferred from TS segments because TS PMT does not carry resolution; recovering it would require parsing the SPS
 * NALU inside a video access unit, which is significantly heavier than the PMT-only walk this module performs.
 *
 * On any failure (segment fetch error, malformed TS, unrecognized stream_type) the inference returns null. Codec is informational metadata for the status
 * display - the streaming pipeline functions correctly without it - so the inference is best-effort and never blocks the probe.
 */

// MPEG-TS uses a fixed 188-byte packet size. Every packet starts with the sync byte 0x47 at offset 0.
const TS_PACKET_SIZE = 188;
const TS_SYNC_BYTE = 0x47;

// PID 0 is reserved for the Program Association Table (PAT). Every TS multiplex has exactly one PAT, repeated periodically across packets.
const PAT_PID = 0;

/* Maximum bytes to retrieve from a TS segment for codec inference. PAT and PMT are small (well under 1 KB combined) and always appear within the first few
 * packets of a live segment. 32 KB is a comfortable upper bound that covers hundreds of packets while remaining tiny relative to typical 6-second TS segments
 * (which run from ~500 KB to several MB). The Range header asks the CDN for only this prefix; CDNs that ignore Range fall through to a full-segment fetch,
 * which still works - we only ever read the first MAX_SEGMENT_PROBE_BYTES worth.
 */
const MAX_SEGMENT_PROBE_BYTES = 32 * 1024;

// Timeout for the segment-prefix fetch. The first segment is the only HTTP I/O this module performs and we want to fail fast on slow CDNs - the inference is
// best-effort and the streaming pipeline continues without a codec label if this times out.
const SEGMENT_PROBE_TIMEOUT_MS = 5000;

/* Maps MPEG-TS stream_type values (assigned by ISO/IEC 13818-1 and the H.222.0 amendments) to the human-readable codec labels surfaced by the master-playlist
 * branch of the probe. Keeping the same label vocabulary across both branches lets downstream consumers (the status display) treat MediaFeed.codec uniformly.
 */
const TS_STREAM_TYPE_TO_CODEC: Readonly<Record<number, string>> = {

  // 0x1B is the H.264 elementary stream type assigned by ISO/IEC 13818-1 Amendment 3.
  0x1B: "H264",
  // 0x24 is the HEVC (H.265) elementary stream type assigned by ISO/IEC 13818-1 Amendment 3:2013.
  0x24: "HEVC"
};

/**
 * Options for inferring codec metadata from a media playlist.
 */
export interface InferMediaCodecOptions {

  // The base URL of the playlist, used to resolve relative segment URLs. This is the URL the playlist itself was fetched from, not a master URL above it.
  baseUrl: string;

  // The raw text of an HLS media playlist (#EXTINF segments declared directly).
  playlistBody: string;
}

/**
 * Inferred codec metadata from a media playlist's first segment.
 */
export interface InferredCodec {

  // Human-readable video codec label (e.g., "H264", "HEVC"), null when the segment format does not yield one or inference fails.
  codec: Nullable<string>;
}

/**
 * Locates the first segment URL in a media playlist body. Walks the playlist line by line and returns the first non-tag, non-empty line that follows an
 * #EXTINF tag - this is the segment's URI. The URI is resolved against the playlist's base URL so callers always get an absolute URL.
 *
 * Exported for unit testing - the I/O orchestrator below uses it through the public inferMediaCodec() entry point.
 *
 * @param playlistBody - The raw media playlist body.
 * @param baseUrl - The base URL for resolving a relative segment URI.
 * @returns The absolute URL of the first segment, or null when the playlist contains no segments.
 */
export function findFirstSegmentUrl(playlistBody: string, baseUrl: string): Nullable<string> {

  let pendingExtinf = false;

  for(const rawLine of playlistBody.split("\n")) {

    const line = rawLine.trim();

    if(line.startsWith("#EXTINF")) {

      pendingExtinf = true;

      continue;
    }

    if(!line || line.startsWith("#")) {

      continue;
    }

    if(pendingExtinf) {

      return resolveUrl(line, baseUrl);
    }
  }

  return null;
}

/**
 * Locates the first TS packet in a buffer that carries the given PID. TS packets are 188 bytes each, aligned to a sync byte (0x47) at offset 0 of the packet.
 * The PID is encoded in the second and third header bytes: 13 bits split as the low 5 bits of byte 1 and all 8 bits of byte 2.
 *
 * @param buffer - The TS-formatted byte buffer.
 * @param targetPid - The PID to locate.
 * @returns The offset of the matching packet within the buffer, or null when no such packet is found.
 */
function findPacketOffset(buffer: Buffer, targetPid: number): Nullable<number> {

  for(let offset = 0; (offset + TS_PACKET_SIZE) <= buffer.length; offset += TS_PACKET_SIZE) {

    if(buffer[offset] !== TS_SYNC_BYTE) {

      // Lost sync. The buffer is not a clean TS stream from this offset; abandon.
      return null;
    }

    const byte1 = buffer[offset + 1] ?? 0;
    const byte2 = buffer[offset + 2] ?? 0;
    const pid = ((byte1 & 0x1F) << 8) | byte2;

    if(pid === targetPid) {

      return offset;
    }
  }

  return null;
}

/**
 * Returns the offset of the payload within a TS packet, after the 4-byte header and any optional adaptation field. Returns null when the packet declares no
 * payload (adaptation_field_control values 0 or 2).
 *
 * @param buffer - The TS-formatted byte buffer.
 * @param packetOffset - The offset of the TS packet within the buffer.
 * @returns The offset where the packet's payload begins, or null when the packet has no payload.
 */
function payloadOffsetForPacket(buffer: Buffer, packetOffset: number): Nullable<number> {

  // Bits 4-5 of byte 3 are adaptation_field_control. Values: 0=reserved, 1=payload only, 2=adaptation only (no payload), 3=adaptation followed by payload.
  const adaptationControl = ((buffer[packetOffset + 3] ?? 0) >> 4) & 0x03;

  if((adaptationControl === 0) || (adaptationControl === 2)) {

    return null;
  }

  // No adaptation field; payload starts immediately after the 4-byte header.
  if(adaptationControl === 1) {

    return packetOffset + 4;
  }

  // Adaptation field present and non-empty. Byte 4 is adaptation_field_length: count of bytes following itself in the adaptation field. Skip past the length
  // byte plus the field's payload to land on the actual TS payload.
  const adaptationLength = buffer[packetOffset + 4] ?? 0;

  return packetOffset + 5 + adaptationLength;
}

/**
 * Parses a TS packet's payload as a PSI (Program-Specific Information) section by stripping the pointer_field. Per ISO/IEC 13818-1, when the packet's
 * payload_unit_start_indicator is set, the first byte of the payload is a pointer_field whose value is the count of stuffing bytes between itself and the
 * actual table_id byte. Returns the offset of the table_id within the buffer, or null when bookkeeping rules out a usable section.
 *
 * @param buffer - The TS-formatted byte buffer.
 * @param packetOffset - The offset of the TS packet within the buffer.
 * @param payloadOffset - The offset where the packet's payload begins (per payloadOffsetForPacket).
 * @returns The offset of the table_id, or null when the section cannot be located.
 */
function sectionOffsetForPacket(buffer: Buffer, packetOffset: number, payloadOffset: number): Nullable<number> {

  // Bit 6 of byte 1 is payload_unit_start_indicator (PUSI). PSI sections only begin in packets where PUSI is set; non-start packets carry continuation data we
  // cannot use without reassembling the section across packets.
  const pusi = ((buffer[packetOffset + 1] ?? 0) >> 6) & 0x01;

  if(pusi !== 1) {

    return null;
  }

  const pointerField = buffer[payloadOffset] ?? 0;

  // The pointer_field skips that many bytes of stuffing before the table_id.
  return payloadOffset + 1 + pointerField;
}

/**
 * Parses a Program Association Table (PAT) and returns the first non-network program's PMT PID. The PAT is structured as:
 *   table_id(8) | section_syntax_indicator(1) | reserved(3) | section_length(12) | transport_stream_id(16) | reserved(2) | version(5) | current_next(1)
 *   | section_number(8) | last_section_number(8) | { program_number(16) | reserved(3) | (program_map_PID|network_PID)(13) }* | CRC(32)
 *
 * Programs with program_number == 0 carry the network PID rather than a PMT; we skip those.
 *
 * @param buffer - The TS-formatted byte buffer.
 * @param sectionOffset - The offset of the table_id byte for the PAT section.
 * @returns The first program's PMT PID, or null when the PAT cannot be parsed.
 */
function parsePatForPmtPid(buffer: Buffer, sectionOffset: number): Nullable<number> {

  // table_id is 0x00 for PAT.
  if(buffer[sectionOffset] !== 0x00) {

    return null;
  }

  const sectionLengthHigh = (buffer[sectionOffset + 1] ?? 0) & 0x0F;
  const sectionLengthLow = buffer[sectionOffset + 2] ?? 0;
  const sectionLength = (sectionLengthHigh << 8) | sectionLengthLow;
  const sectionEnd = sectionOffset + 3 + sectionLength;

  if(sectionEnd > buffer.length) {

    return null;
  }

  // Skip header (3 bytes) + transport_stream_id (2) + reserved/version (1) + section_number (1) + last_section_number (1) = 8 bytes total. Then walk the
  // 4-byte program records up to (sectionEnd - 4) to leave room for the CRC.
  let cursor = sectionOffset + 8;
  const programsEnd = sectionEnd - 4;

  while((cursor + 4) <= programsEnd) {

    const programNumber = ((buffer[cursor] ?? 0) << 8) | (buffer[cursor + 1] ?? 0);
    const pmtPidHigh = (buffer[cursor + 2] ?? 0) & 0x1F;
    const pmtPidLow = buffer[cursor + 3] ?? 0;
    const pid = (pmtPidHigh << 8) | pmtPidLow;

    if(programNumber !== 0) {

      return pid;
    }

    cursor += 4;
  }

  return null;
}

/**
 * Parses a Program Map Table (PMT) and returns the stream_type of the first elementary stream that maps to a recognized video codec. The PMT is structured as:
 *   table_id(8) | section_syntax_indicator(1) | reserved(3) | section_length(12) | program_number(16) | reserved(2) | version(5) | current_next(1)
 *   | section_number(8) | last_section_number(8) | reserved(3) | PCR_PID(13) | reserved(4) | program_info_length(12) | descriptors[program_info_length]
 *   | { stream_type(8) | reserved(3) | elementary_PID(13) | reserved(4) | ES_info_length(12) | descriptors[ES_info_length] }* | CRC(32)
 *
 * @param buffer - The TS-formatted byte buffer.
 * @param sectionOffset - The offset of the table_id byte for the PMT section.
 * @returns The video stream_type, or null when no recognized video stream is found.
 */
function parsePmtForVideoStreamType(buffer: Buffer, sectionOffset: number): Nullable<number> {

  // table_id is 0x02 for PMT.
  if(buffer[sectionOffset] !== 0x02) {

    return null;
  }

  const sectionLengthHigh = (buffer[sectionOffset + 1] ?? 0) & 0x0F;
  const sectionLengthLow = buffer[sectionOffset + 2] ?? 0;
  const sectionLength = (sectionLengthHigh << 8) | sectionLengthLow;
  const sectionEnd = sectionOffset + 3 + sectionLength;

  if(sectionEnd > buffer.length) {

    return null;
  }

  // Skip the fixed header to reach program_info_length: 3 (header) + 2 (program_number) + 1 (version/current_next) + 2 (section_number/last_section) + 2
  // (reserved + PCR_PID) = 10 bytes, then 2 bytes for reserved + program_info_length itself.
  const programInfoLengthHigh = (buffer[sectionOffset + 10] ?? 0) & 0x0F;
  const programInfoLengthLow = buffer[sectionOffset + 11] ?? 0;
  const programInfoLength = (programInfoLengthHigh << 8) | programInfoLengthLow;

  // Walk the elementary stream loop, skipping past program_info_length descriptor bytes.
  let cursor = sectionOffset + 12 + programInfoLength;
  const streamsEnd = sectionEnd - 4;

  while((cursor + 5) <= streamsEnd) {

    const streamType = buffer[cursor] ?? 0;
    const esInfoLengthHigh = (buffer[cursor + 3] ?? 0) & 0x0F;
    const esInfoLengthLow = buffer[cursor + 4] ?? 0;
    const esInfoLength = (esInfoLengthHigh << 8) | esInfoLengthLow;

    if(TS_STREAM_TYPE_TO_CODEC[streamType] !== undefined) {

      return streamType;
    }

    cursor += 5 + esInfoLength;
  }

  return null;
}

/**
 * Parses a TS-formatted byte buffer and returns the recognized video codec label, or null when no recognized video stream is present. The buffer is expected to
 * contain the start of a TS segment - the first PAT and PMT packets must be reachable within it. Pure synchronous function with no I/O.
 *
 * Exported for unit testing - the I/O orchestrator below uses it through the public inferMediaCodec() entry point.
 *
 * @param buffer - A buffer containing the start of a TS segment.
 * @returns The codec label (e.g., "H264", "HEVC"), or null when no recognized video stream is found.
 */
export function inferCodecFromTsBuffer(buffer: Buffer): Nullable<string> {

  // Locate the PAT.
  const patOffset = findPacketOffset(buffer, PAT_PID);

  if(patOffset === null) {

    return null;
  }

  const patPayload = payloadOffsetForPacket(buffer, patOffset);

  if(patPayload === null) {

    return null;
  }

  const patSection = sectionOffsetForPacket(buffer, patOffset, patPayload);

  if(patSection === null) {

    return null;
  }

  const pmtPid = parsePatForPmtPid(buffer, patSection);

  if(pmtPid === null) {

    return null;
  }

  // Locate the PMT for the program declared by the PAT.
  const pmtOffset = findPacketOffset(buffer, pmtPid);

  if(pmtOffset === null) {

    return null;
  }

  const pmtPayload = payloadOffsetForPacket(buffer, pmtOffset);

  if(pmtPayload === null) {

    return null;
  }

  const pmtSection = sectionOffsetForPacket(buffer, pmtOffset, pmtPayload);

  if(pmtSection === null) {

    return null;
  }

  const streamType = parsePmtForVideoStreamType(buffer, pmtSection);

  if(streamType === null) {

    return null;
  }

  return TS_STREAM_TYPE_TO_CODEC[streamType] ?? null;
}

/**
 * Fetches the prefix of a TS segment so the caller can parse its PAT/PMT. Sends a Range header asking the CDN for the first MAX_SEGMENT_PROBE_BYTES; CDNs that
 * ignore Range fall through to returning the full segment, which still works because the parser only reads the prefix. Returns null on any HTTP or fetch error
 * so the inference can fail gracefully back to a null codec label.
 *
 * @param url - The absolute segment URL.
 * @returns The fetched prefix, or null on failure.
 */
async function fetchSegmentPrefix(url: string): Promise<Nullable<Buffer>> {

  try {

    const response = await chromeFetch(url, {

      headers: { Range: "bytes=0-" + String(MAX_SEGMENT_PROBE_BYTES - 1) },
      signal: AbortSignal.timeout(SEGMENT_PROBE_TIMEOUT_MS)
    });

    // Accept both 206 Partial Content (Range honored) and 200 OK (Range ignored or unsupported); both deliver bytes the parser can use.
    if(!response.ok && (response.status !== 206)) {

      LOG.debug("native:codec", "Segment prefix fetch returned HTTP %s.", response.status);

      return null;
    }

    return Buffer.from(await response.arrayBuffer());
  } catch(error) {

    LOG.debug("native:codec", "Segment prefix fetch error: %s.", String(error));

    return null;
  }
}

/**
 * Infers codec metadata from a media playlist by fetching the prefix of the first segment and parsing it. Returns an InferredCodec with codec=null when the
 * playlist has no segments, the segment cannot be fetched, or the segment format is unrecognized. Currently supports MPEG-TS segments via PAT/PMT walk; fMP4
 * segment inference (via #EXT-X-MAP and parseMoovCodecConfig) is a future extension.
 *
 * @param options - The playlist body and base URL.
 * @returns The inferred codec metadata.
 */
export async function inferMediaCodec(options: InferMediaCodecOptions): Promise<InferredCodec> {

  const { baseUrl, playlistBody } = options;

  const segUrl = findFirstSegmentUrl(playlistBody, baseUrl);

  if(!segUrl) {

    return { codec: null };
  }

  // Strip query parameters before checking the file extension. HLS segment URIs commonly carry signed-token query strings that would otherwise mask the path.
  const segPath = segUrl.split("?")[0] ?? "";

  // MPEG-TS segments end in .ts (and historically .aac for audio-only, which we ignore for video codec inference).
  if(!segPath.endsWith(".ts")) {

    LOG.debug("native:codec", "Skipping codec inference for non-TS segment: %s.", segPath.slice(0, 120));

    return { codec: null };
  }

  const buffer = await fetchSegmentPrefix(segUrl);

  if(!buffer) {

    return { codec: null };
  }

  const codec = inferCodecFromTsBuffer(buffer);

  if(codec) {

    LOG.debug("native:codec", "Inferred codec %s from first TS segment.", codec);
  } else {

    LOG.debug("native:codec", "TS segment did not yield a recognized video codec.");
  }

  return { codec };
}
