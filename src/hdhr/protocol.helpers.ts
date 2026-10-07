/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * protocol.helpers.ts: Test-only packet builders and readers for the HDHR UDP wire protocol. Co-located with protocol.ts per the helper-location convention -
 * tests in protocol.test.ts, udp.test.ts and index.test.ts need to manufacture wire-formatted request packets to feed parsePacket or push through a dgram
 * socket, and the responder rows need to exchange a datagram with a bound responder and read the BaseURL its Discover reply advertises. Inlining those in each
 * file would duplicate the framing and the reading, so the canonical helpers live here.
 *
 * These helpers intentionally do NOT reuse buildPacket / encodeStringTlv from protocol.ts. Tests construct packets by hand so they can exercise the parser
 * with byte sequences the production builders cannot emit (malformed lengths, bad CRCs, unknown packet codes). Sharing the framing constant set with protocol.ts
 * (tag and packet-code numerics) keeps the two layers true to the wire format without coupling test fixtures to the production code path.
 */
import { PACKET_DISCOVER_REQUEST, PACKET_GET_REQUEST, TLV_BASE_URL, TLV_DEVICE_ID, TLV_DEVICE_TYPE, TLV_GETSET_NAME, TLV_GETSET_VALUE } from "./protocol.ts";
import { crc32 } from "node:zlib";
import { createSocket } from "node:dgram";

/**
 * Sends a request datagram to a responder bound on 127.0.0.1 and resolves with its first reply, or rejects once the bound passes with none. A row that expects no
 * reply passes a short bound, so its wait is a fraction of a second rather than the full round-trip budget.
 * @param port - The responder's UDP port.
 * @param request - The wire-formatted request packet.
 * @param timeoutMs - How long to wait for a reply.
 * @returns The first reply datagram.
 */
export async function sendAndReceive(port: number, request: Buffer, timeoutMs = 2000): Promise<Buffer> {

  const { promise, resolve, reject } = Promise.withResolvers<Buffer>();
  const client = createSocket("udp4");
  const timer = setTimeout(() => {

    client.close();
    reject(new Error("Timed out waiting for UDP reply."));
  }, timeoutMs);

  client.once("message", (msg) => {

    clearTimeout(timer);
    client.close();
    resolve(msg);
  });

  client.send(request, port, "127.0.0.1", (err) => {

    if(err) {

      clearTimeout(timer);
      client.close();
      reject(err);
    }
  });

  return promise;
}

/**
 * Reads the BaseURL a Discover reply advertises. The reply's TLV payload is walked for the BaseURL tag, and the null terminator the encoder writes is trimmed.
 * @param reply - The Discover reply datagram.
 * @returns The advertised BaseURL, or an empty string when the reply carries none.
 */
export function readBaseUrl(reply: Buffer): string {

  const payload = reply.subarray(4, 4 + reply.readUInt16BE(2));
  let offset = 0;

  while(offset < payload.length) {

    const tag = payload.readUInt8(offset);
    const length = payload.readUInt8(offset + 1);

    if(tag === TLV_BASE_URL) {

      return payload.subarray(offset + 2, offset + 2 + length).toString("utf8").replace(/\0$/, "");
    }

    offset += 2 + length;
  }

  return "";
}

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
