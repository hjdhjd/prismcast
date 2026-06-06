/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * protocol.ts: HDHomeRun UDP wire-protocol codec for PrismCast.
 *
 * Real HDHomeRun devices speak a small binary protocol on UDP port 65001. Discovery, control, and (legacy) tuner programming all flow through the same packet
 * format. This module is the pure-protocol layer: it parses incoming request packets into discriminated structural records, builds outgoing reply packets from
 * structural records, and computes the CRC that brackets every packet. It does no I/O - the transport layer in udp.ts owns sockets, addresses, and CONFIG; the
 * parser/builder pair is fully unit-testable in isolation.
 *
 * Packet layout (all multi-byte header fields are big-endian; the trailing CRC is little-endian):
 *
 *   +--------------------+--------------------+
 *   | packet type (u16)  | payload length (u16)|
 *   +--------------------+--------------------+
 *   | payload (variable, TLV-encoded)         |
 *   +-----------------------------------------+
 *   | CRC-32 (u32, IEEE 802.3, little-endian) |
 *   +-----------------------------------------+
 *
 * Total wire size = 4 header + payloadLength + 4 CRC bytes.
 *
 * TLV layout within the payload: one-byte tag, length, value. Length is one byte when the high bit is clear (values 0-127), or two bytes when set (low 7 bits
 * of the first byte plus the second byte form a little-endian 15-bit length, 0-32767). PrismCast emits only short values so all outgoing TLVs use the one-byte
 * form; the parser handles both for robustness against any compliant client.
 *
 * CRC-32 is the standard IEEE 802.3 polynomial computed over the packet header plus payload (everything except the CRC field itself). Node 22's zlib.crc32
 * matches the wire bit ordering directly - no hand-rolled tables needed.
 */
import type { Nullable } from "../types/index.ts";
import { crc32 } from "node:zlib";

// Packet type codes from libhdhomerun's hdhomerun_pkt.h. The protocol uses a single GETSET_REQ code (0x0004) for both Get and Set requests; the two are
// distinguished by the presence or absence of a value TLV in the payload, not by a separate packet code. Discover/Get are the request-reply pairs PrismCast
// actually answers; Upgrade requests fall through to the transport layer's unsupported handler. The PACKET_UPGRADE_REPLY code is included for completeness
// even though PrismCast never receives nor emits upgrade replies.
export const PACKET_DISCOVER_REQUEST = 0x0002;
export const PACKET_DISCOVER_REPLY = 0x0003;
export const PACKET_GET_REQUEST = 0x0004;
export const PACKET_GET_REPLY = 0x0005;
export const PACKET_UPGRADE_REQUEST = 0x0006;
export const PACKET_UPGRADE_REPLY = 0x0007;

// TLV tag identifiers. The numeric values are part of the wire protocol; do not renumber. Discover packets carry the device-class TLVs (0x01, 0x02, 0x10, 0x2A);
// Get/Set packets carry the name/value/error TLVs (0x03, 0x04, 0x05). The tags match the canonical values published in libhdhomerun's hdhomerun_pkt.h - there
// is no standard tag for a discrete lineup URL, so clients derive it by appending "/lineup.json" to the base URL.
export const TLV_DEVICE_TYPE = 0x01;
export const TLV_DEVICE_ID = 0x02;
export const TLV_GETSET_NAME = 0x03;
export const TLV_GETSET_VALUE = 0x04;
export const TLV_ERROR = 0x05;
export const TLV_TUNER_COUNT = 0x10;
export const TLV_BASE_URL = 0x2A;

// Wildcard sentinel for Discover requests. Clients that want any device send these to filter neither by type nor by id.
export const HDHR_WILDCARD = 0xFFFFFFFF;

/**
 * Parsed shape of an incoming packet. The discriminated union lets the transport layer route by `type` without re-decoding. `unsupported` covers packet types
 * PrismCast does not handle (the transport layer responds with an error reply).
 */
export type ParsedPacket =
  { readonly requestedDeviceId: number; readonly requestedDeviceType: number; readonly type: "discover" } |
  { readonly name: string; readonly type: "get" } |
  { readonly name: string; readonly type: "set"; readonly value: string } |
  { readonly packetType: number; readonly type: "unsupported" };

/**
 * Fields required to build a Discover reply. The transport layer composes these from CONFIG and the requesting client's network position; the protocol layer
 * only knows how to serialize them.
 */
export interface DiscoverReplyFields {

  // Reachable base URL for the HTTP HDHR server (e.g., "http://192.168.1.5:5004").
  readonly baseUrl: string;

  // 32-bit numeric device id derived from CONFIG.hdhr.deviceId's hex string.
  readonly deviceId: number;

  // Device-class tag, typically HDHR_DEVICE_TYPE_TUNER (0x00000001).
  readonly deviceType: number;

  // Configured concurrent stream capacity.
  readonly tunerCount: number;
}

/**
 * Parses an incoming packet. Returns null when the packet is structurally malformed (too short, length mismatch, bad CRC). Otherwise returns the discriminated
 * record describing the packet's intent. Unknown TLVs within a known packet type are silently skipped - the protocol allows extension TLVs that PrismCast does
 * not understand.
 * @param buf - The raw datagram bytes.
 * @returns The parsed packet or null on malformed input.
 */
export function parsePacket(buf: Buffer): Nullable<ParsedPacket> {

  // Minimum packet size is the 4-byte header plus the 4-byte CRC. A datagram smaller than that cannot encode a valid packet.
  if(buf.length < 8) {

    return null;
  }

  const packetType = buf.readUInt16BE(0);
  const payloadLength = buf.readUInt16BE(2);
  const expectedSize = 4 + payloadLength + 4;

  // A packet whose declared length does not match the datagram size is corrupt - either fragmentation or a malformed sender.
  if(buf.length !== expectedSize) {

    return null;
  }

  // The CRC covers the header and payload but not the CRC field itself. We compute over slice(0, header + payload) and compare to the trailing little-endian u32.
  const computedCrc = crc32(buf.subarray(0, 4 + payloadLength));
  const declaredCrc = buf.readUInt32LE(4 + payloadLength);

  if(computedCrc !== declaredCrc) {

    return null;
  }

  const payload = buf.subarray(4, 4 + payloadLength);
  const tlvs = decodeTlvs(payload);

  if(tlvs === null) {

    return null;
  }

  switch(packetType) {

    case PACKET_DISCOVER_REQUEST: {

      // Discover requests may include device-type and device-id filters. Both default to wildcard when missing so a request with no filters matches everything.
      const requestedDeviceType = readU32Tlv(tlvs, TLV_DEVICE_TYPE) ?? HDHR_WILDCARD;
      const requestedDeviceId = readU32Tlv(tlvs, TLV_DEVICE_ID) ?? HDHR_WILDCARD;

      return { requestedDeviceId, requestedDeviceType, type: "discover" };
    }

    case PACKET_GET_REQUEST: {

      const name = readStringTlv(tlvs, TLV_GETSET_NAME);

      if(name === null) {

        return null;
      }

      const value = readStringTlv(tlvs, TLV_GETSET_VALUE);

      // Get requests carry only a name; Set requests carry both a name and a value. The protocol uses the same 0x0004 packet type for both - the presence of
      // the value TLV distinguishes them. This is unusual but documented in libhdhomerun's source.
      if(value !== null) {

        return { name, type: "set", value };
      }

      return { name, type: "get" };
    }

    default: {

      return { packetType, type: "unsupported" };
    }
  }
}

/**
 * Builds a Discover reply packet. The reply is short enough to fit in a single datagram even with all fields populated; the largest contributor is the base
 * URL string which is typically under 50 bytes.
 * @param fields - The device-identity values to serialize.
 * @returns A complete wire-formatted packet ready to send via socket.send.
 */
export function buildDiscoverReply(fields: DiscoverReplyFields): Buffer {

  // Pre-compute each TLV so we can compose them in tag order. The wire order is conventional but not strictly required by the protocol - listing tags in
  // ascending tag order matches real HDHomeRun reply traces and avoids client implementations that assume a specific order. TunerCount is encoded as an 8-bit
  // value because real HDHR devices report it that way (real-world tuner counts fit in a byte; the wire format reflects that).
  const tlvs: Buffer[] = [
    encodeU32Tlv(TLV_DEVICE_TYPE, fields.deviceType),
    encodeU32Tlv(TLV_DEVICE_ID, fields.deviceId),
    encodeU8Tlv(TLV_TUNER_COUNT, fields.tunerCount),
    encodeStringTlv(TLV_BASE_URL, fields.baseUrl)
  ];

  return buildPacket(PACKET_DISCOVER_REPLY, Buffer.concat(tlvs));
}

/**
 * Builds a Get reply packet carrying a name/value pair. Used when answering a known Get key with a real value.
 * @param name - The Get key being answered (echoed back in the reply).
 * @param value - The value returned for that key.
 * @returns A complete wire-formatted packet.
 */
export function buildGetReply(name: string, value: string): Buffer {

  const payload = Buffer.concat([
    encodeStringTlv(TLV_GETSET_NAME, name),
    encodeStringTlv(TLV_GETSET_VALUE, value)
  ]);

  return buildPacket(PACKET_GET_REPLY, payload);
}

/**
 * Builds a Get reply packet carrying an error string. Used when answering a Get for an unknown key or rejecting a Set or Upgrade request. Real HDHomeRun
 * firmware uses "ERROR: ..." prose for the error TLV; we follow the same convention so client error messages remain familiar to operators.
 * @param name - The key being answered (echoed back).
 * @param error - The error string. Should begin with "ERROR: " per HDHR convention.
 * @returns A complete wire-formatted packet.
 */
export function buildErrorReply(name: string, error: string): Buffer {

  const payload = Buffer.concat([
    encodeStringTlv(TLV_GETSET_NAME, name),
    encodeStringTlv(TLV_ERROR, error)
  ]);

  return buildPacket(PACKET_GET_REPLY, payload);
}

/**
 * Composes a complete packet: 4-byte header, payload, 4-byte little-endian CRC of header + payload. The CRC is computed last so the packet body is valid bytes
 * before sealing.
 * @param packetType - The packet type code.
 * @param payload - The TLV-encoded payload bytes.
 * @returns The complete packet.
 */
function buildPacket(packetType: number, payload: Buffer): Buffer {

  const header = Buffer.alloc(4);

  header.writeUInt16BE(packetType, 0);
  header.writeUInt16BE(payload.length, 2);

  const body = Buffer.concat([ header, payload ]);
  const checksum = Buffer.alloc(4);

  checksum.writeUInt32LE(crc32(body), 0);

  return Buffer.concat([ body, checksum ]);
}

/**
 * Encodes a 32-bit unsigned integer TLV. The four-byte value is written big-endian to match the wire protocol.
 * @param tag - The TLV tag.
 * @param value - The unsigned 32-bit value.
 * @returns The TLV bytes (tag + length + value).
 */
function encodeU32Tlv(tag: number, value: number): Buffer {

  const buf = Buffer.alloc(6);

  buf.writeUInt8(tag, 0);
  buf.writeUInt8(4, 1);
  buf.writeUInt32BE(value >>> 0, 2);

  return buf;
}

/**
 * Encodes an 8-bit unsigned integer TLV. Used for fields like tuner count whose values fit in a single byte and are documented as one-byte on the wire.
 * @param tag - The TLV tag.
 * @param value - The unsigned 8-bit value.
 * @returns The TLV bytes (tag + length + value).
 */
function encodeU8Tlv(tag: number, value: number): Buffer {

  const buf = Buffer.alloc(3);

  buf.writeUInt8(tag, 0);
  buf.writeUInt8(1, 1);
  buf.writeUInt8(value & 0xFF, 2);

  return buf;
}

/**
 * Encodes a string TLV. The string is serialized as UTF-8 with a trailing NUL byte (HDHomeRun convention), so a 10-character string occupies 11 bytes of value.
 * Values longer than 127 bytes use the two-byte length encoding; PrismCast never emits values that long, but the encoder remains correct if a future field
 * exceeds the threshold.
 * @param tag - The TLV tag.
 * @param value - The string value.
 * @returns The TLV bytes (tag + length + UTF-8 bytes + NUL).
 */
function encodeStringTlv(tag: number, value: string): Buffer {

  const bodyBytes = Buffer.from(value + "\0", "utf8");
  const length = bodyBytes.length;

  // Length under 128 fits in the single-byte form. The two-byte form is included for correctness even though our outgoing values never need it.
  if(length < 128) {

    const buf = Buffer.alloc(2 + length);

    buf.writeUInt8(tag, 0);
    buf.writeUInt8(length, 1);
    bodyBytes.copy(buf, 2);

    return buf;
  }

  // Two-byte length form: first byte has high bit set carrying low 7 bits of length; second byte carries the high 8 bits. Little-endian.
  const buf = Buffer.alloc(3 + length);

  buf.writeUInt8(tag, 0);
  buf.writeUInt8((length & 0x7F) | 0x80, 1);
  buf.writeUInt8((length >>> 7) & 0xFF, 2);
  bodyBytes.copy(buf, 3);

  return buf;
}

/**
 * Decoded TLV records produced by the parser. Tag is the numeric tag byte; value is the raw value bytes (UTF-8 NUL stripped if it was a string TLV).
 */
interface DecodedTlv {

  readonly tag: number;
  readonly value: Buffer;
}

/**
 * Decodes a TLV-encoded payload into a list of records. Returns null when the payload is structurally malformed (a length field that overruns the remaining
 * bytes). Unknown tags pass through; the caller decides whether to use them.
 * @param payload - The raw payload bytes (header and CRC already stripped).
 * @returns The decoded TLVs or null on malformed input.
 */
function decodeTlvs(payload: Buffer): Nullable<DecodedTlv[]> {

  const tlvs: DecodedTlv[] = [];
  let offset = 0;

  while(offset < payload.length) {

    // Need at least tag + first length byte to make progress; anything shorter is truncation.
    if((offset + 2) > payload.length) {

      return null;
    }

    const tag = payload.readUInt8(offset);
    const lengthByte = payload.readUInt8(offset + 1);
    let length: number;
    let headerSize: number;

    // High bit set on the length byte signals the two-byte length form; clear means single-byte length 0-127.
    if((lengthByte & 0x80) !== 0) {

      if((offset + 3) > payload.length) {

        return null;
      }

      length = (lengthByte & 0x7F) | (payload.readUInt8(offset + 2) << 7);
      headerSize = 3;
    } else {

      length = lengthByte;
      headerSize = 2;
    }

    const valueStart = offset + headerSize;
    const valueEnd = valueStart + length;

    if(valueEnd > payload.length) {

      return null;
    }

    tlvs.push({ tag, value: payload.subarray(valueStart, valueEnd) });
    offset = valueEnd;
  }

  return tlvs;
}

/**
 * Looks up a 32-bit unsigned integer TLV by tag. Returns null when the tag is missing or its value is not exactly four bytes. The big-endian read matches the
 * wire format.
 * @param tlvs - The decoded TLV list.
 * @param tag - The tag to look up.
 * @returns The 32-bit value or null.
 */
function readU32Tlv(tlvs: readonly DecodedTlv[], tag: number): Nullable<number> {

  const entry = tlvs.find((t) => (t.tag === tag));

  if(entry?.value.length !== 4) {

    return null;
  }

  return entry.value.readUInt32BE(0);
}

/**
 * Looks up a string TLV by tag. Returns null when the tag is missing. The trailing NUL byte (HDHomeRun convention) is stripped before decoding so callers
 * receive the bare string.
 * @param tlvs - The decoded TLV list.
 * @param tag - The tag to look up.
 * @returns The string value or null.
 */
function readStringTlv(tlvs: readonly DecodedTlv[], tag: number): Nullable<string> {

  const entry = tlvs.find((t) => (t.tag === tag));

  if(!entry) {

    return null;
  }

  // Strip a single trailing NUL if present; senders that omit the NUL still produce a valid string. .at(-1) returns undefined on an empty buffer, so the zero-
  // length case is handled by the strict-equality check without a separate length guard.
  const trimmed = (entry.value.at(-1) === 0) ? entry.value.subarray(0, -1) : entry.value;

  return trimmed.toString("utf8");
}
