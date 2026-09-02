/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * protocol.helpers.ts: Test-only packet builders for the HDHR UDP wire protocol. Co-located with protocol.ts per the helper-location convention - tests in
 * protocol.test.ts and udp.test.ts both need to manufacture wire-formatted request packets to feed parsePacket or push through a dgram socket. Inlining
 * sealPacket in both files would duplicate the framing logic, so the canonical packet-framing helpers live here.
 *
 * These helpers intentionally do NOT reuse buildPacket / encodeStringTlv from protocol.ts. Tests construct packets by hand so they can exercise the parser
 * with byte sequences the production builders cannot emit (malformed lengths, bad CRCs, unknown packet codes). Sharing the framing constant set with protocol.ts
 * (tag and packet-code numerics) keeps the two layers true to the wire format without coupling test fixtures to the production code path.
 */
import { PACKET_DISCOVER_REQUEST, PACKET_GET_REQUEST, TLV_DEVICE_ID, TLV_DEVICE_TYPE, TLV_GETSET_NAME, TLV_GETSET_VALUE } from "./protocol.ts";
import { crc32 } from "node:zlib";

/**
 * Wraps a payload in the standard HDHR packet frame: 4-byte big-endian header (packet type + payload length), payload, 4-byte little-endian CRC of header +
 * payload. Used as the final step of every test request builder; exposed so tests with non-standard payloads (deliberately malformed, unknown packet codes)
 * can frame their own byte sequences.
 * @param packetType - The 16-bit packet type code.
 * @param payload - The TLV-encoded payload bytes.
 * @returns The complete wire-formatted packet.
 */
export function sealPacket(packetType: number, payload: Buffer): Buffer {

  const header = Buffer.alloc(4);

  header.writeUInt16BE(packetType, 0);
  header.writeUInt16BE(payload.length, 2);

  const body = Buffer.concat([ header, payload ]);
  const checksum = Buffer.alloc(4);

  checksum.writeUInt32LE(crc32(body), 0);

  return Buffer.concat([ body, checksum ]);
}

/**
 * Builds a wire-formatted Discover request with the supplied device-type and device-id filters. Used as input to parsePacket in protocol tests and as a wire
 * packet to send to a bound responder in udp tests.
 * @param deviceType - The device-type filter (HDHR_WILDCARD for "any").
 * @param deviceId - The device-id filter (HDHR_WILDCARD for "any").
 * @returns The complete packet.
 */
export function makeDiscoverRequest(deviceType: number, deviceId: number): Buffer {

  const payload = Buffer.alloc(12);

  // TLV 0x01 Device Type: tag, length=4, four big-endian bytes.
  payload.writeUInt8(TLV_DEVICE_TYPE, 0);
  payload.writeUInt8(4, 1);
  payload.writeUInt32BE(deviceType >>> 0, 2);

  // TLV 0x02 Device ID: tag, length=4, four big-endian bytes.
  payload.writeUInt8(TLV_DEVICE_ID, 6);
  payload.writeUInt8(4, 7);
  payload.writeUInt32BE(deviceId >>> 0, 8);

  return sealPacket(PACKET_DISCOVER_REQUEST, payload);
}

/**
 * Builds a wire-formatted Get or Set request. When valueOrNull is null the result is a Get request (name TLV only); when non-null it is a Set request (name +
 * value TLVs). The protocol uses the same packet code for both - presence of the value TLV is the only distinguishing factor.
 * @param name - The Get/Set key.
 * @param valueOrNull - The Set value, or null for a Get.
 * @returns The complete packet.
 */
export function makeGetRequest(name: string, valueOrNull: string | null = null): Buffer {

  const nameBytes = Buffer.from(name + "\0", "utf8");
  const valueBytes = (valueOrNull !== null) ? Buffer.from(valueOrNull + "\0", "utf8") : null;
  const nameTlv = Buffer.concat([ Buffer.from([ TLV_GETSET_NAME, nameBytes.length ]), nameBytes ]);
  const valueTlv = (valueBytes !== null) ? Buffer.concat([ Buffer.from([ TLV_GETSET_VALUE, valueBytes.length ]), valueBytes ]) : Buffer.alloc(0);

  return sealPacket(PACKET_GET_REQUEST, Buffer.concat([ nameTlv, valueTlv ]));
}
