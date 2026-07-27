/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * protocol.test.ts: Unit tests for the HDHR wire-protocol codec. The codec is pure (no I/O, no shared state), so the tests are mechanical: build a packet, dump
 * the bytes, assert offsets and TLV contents; parse a hand-crafted byte sequence, assert the structural record. Tests cover both directions of the protocol
 * boundary (parse and build) and the failure modes (short packets, bad CRC, malformed TLV lengths) that the transport layer relies on the codec to detect.
 *
 * Fixtures are constructed via the shared packet builders in protocol.helpers.ts so the test reads like a spec: each request is built with a clearly-labelled
 * helper, and the resulting packet is verified against its expected byte layout - either by parsing it back through parsePacket or, for reply packets that
 * have no parser, by manually recomputing the CRC over the built bytes. Where literal byte offsets matter (header field positions, CRC placement), we assert
 * them directly so a refactor that silently changes the wire format would fail loudly.
 */
import { HDHR_WILDCARD, PACKET_DISCOVER_REPLY, PACKET_DISCOVER_REQUEST, PACKET_GET_REPLY, PACKET_GET_REQUEST, PACKET_UPGRADE_REQUEST, TLV_BASE_URL, TLV_DEVICE_ID,
  TLV_DEVICE_TYPE, TLV_ERROR, TLV_GETSET_NAME, TLV_GETSET_VALUE, TLV_TUNER_COUNT, buildDiscoverReply, buildErrorReply, buildGetReply, parsePacket } from "./protocol.ts";
import { describe, test } from "node:test";
import { makeDiscoverRequest, makeGetRequest, sealPacket } from "./protocol.helpers.ts";
import assert from "node:assert/strict";
import { crc32 } from "node:zlib";

describe("parsePacket", () => {

  test("returns null for packets too short to contain a header and CRC", () => {

    assert.equal(parsePacket(Buffer.alloc(7)), null, "7 bytes is one short of the 8-byte minimum");
    assert.equal(parsePacket(Buffer.alloc(0)), null, "empty buffer");
  });

  test("returns null when the declared payload length does not match the datagram size", () => {

    const truncated = makeDiscoverRequest(HDHR_WILDCARD, HDHR_WILDCARD).subarray(0, -1);

    assert.equal(parsePacket(truncated), null);
  });

  test("returns null when the CRC does not match the header and payload", () => {

    const packet = makeDiscoverRequest(HDHR_WILDCARD, HDHR_WILDCARD);

    // Flip a payload byte; the CRC was computed before the mutation so it no longer matches.
    packet.writeUInt8(packet.readUInt8(4) ^ 0xFF, 4);

    assert.equal(parsePacket(packet), null);
  });

  test("decodes a Discover request with both device-type and device-id filters", () => {

    const parsed = parsePacket(makeDiscoverRequest(0x00000001, 0x12345678));

    assert.deepEqual(parsed, {

      requestedDeviceId: 0x12345678,
      requestedDeviceType: 0x00000001,
      type: "discover"
    });
  });

  test("decodes a Discover request with wildcard filters", () => {

    const parsed = parsePacket(makeDiscoverRequest(HDHR_WILDCARD, HDHR_WILDCARD));

    assert.deepEqual(parsed, {

      requestedDeviceId: HDHR_WILDCARD,
      requestedDeviceType: HDHR_WILDCARD,
      type: "discover"
    });
  });

  test("decodes a Get request (name TLV only)", () => {

    const parsed = parsePacket(makeGetRequest("/sys/version"));

    assert.deepEqual(parsed, { name: "/sys/version", type: "get" });
  });

  test("decodes a Set request (name + value TLVs)", () => {

    const parsed = parsePacket(makeGetRequest("/tuner0/channel", "auto:5"));

    assert.deepEqual(parsed, { name: "/tuner0/channel", type: "set", value: "auto:5" });
  });

  test("returns unsupported for packet types PrismCast does not handle", () => {

    const upgrade = sealPacket(PACKET_UPGRADE_REQUEST, Buffer.alloc(0));
    const parsed = parsePacket(upgrade);

    assert.deepEqual(parsed, { packetType: PACKET_UPGRADE_REQUEST, type: "unsupported" });
  });

  test("returns unsupported for an unknown packet code with the original code echoed back", () => {

    // Synthetic packet with an unknown type code. The parser should classify it as unsupported so the transport can emit an error reply or drop.
    const parsed = parsePacket(sealPacket(0x00FF, Buffer.alloc(0)));

    assert.deepEqual(parsed, { packetType: 0x00FF, type: "unsupported" });
  });
});

describe("buildDiscoverReply", () => {

  // makeReplyFixture returns a stable set of fields for use across multiple tests so the assertions read cleanly without per-test setup.
  function makeReplyFixture(): { baseUrl: string; deviceId: number; deviceType: number; tunerCount: number } {

    return { baseUrl: "http://192.168.1.5:5004", deviceId: 0x12345678, deviceType: 0x00000001, tunerCount: 4 };
  }

  test("packet type is PACKET_DISCOVER_REPLY", () => {

    const packet = buildDiscoverReply(makeReplyFixture());

    assert.equal(packet.readUInt16BE(0), PACKET_DISCOVER_REPLY);
  });

  test("CRC verifies via parsePacket round-trip - sanity check that the build path produces parseable bytes", () => {

    // We do not have a parseDiscoverReply function (the transport never parses replies it sent), so this test manually recomputes the CRC over the built
    // header and payload bytes and compares it against the CRC the builder wrote into the trailer; a mismatch would mean the build path produces bytes no
    // parser could ever validate.
    const packet = buildDiscoverReply(makeReplyFixture());

    // Manually re-verify the CRC the same way the parser would so the assertion message points at the CRC specifically if it fails.
    const declaredCrc = packet.readUInt32LE(packet.length - 4);
    const computedCrc = crc32(packet.subarray(0, packet.length - 4));

    assert.equal(declaredCrc, computedCrc, "CRC matches over header + payload");
  });

  test("payload contains the four required TLVs (device type, device id, tuner count, base URL)", () => {

    const fields = makeReplyFixture();
    const packet = buildDiscoverReply(fields);
    const payloadLength = packet.readUInt16BE(2);
    const payload = packet.subarray(4, 4 + payloadLength);

    // The TLVs are concatenated; we walk them by tag and verify each in turn rather than asserting raw byte offsets (which would over-constrain the encoder).
    const tlvs = decodeTlvsForTest(payload);
    const deviceTypeTlv = tlvs.find((t) => (t.tag === TLV_DEVICE_TYPE));
    const deviceIdTlv = tlvs.find((t) => (t.tag === TLV_DEVICE_ID));
    const tunerCountTlv = tlvs.find((t) => (t.tag === TLV_TUNER_COUNT));
    const baseUrlTlv = tlvs.find((t) => (t.tag === TLV_BASE_URL));

    assert.ok(deviceTypeTlv, "device type TLV present");
    assert.ok(deviceIdTlv, "device id TLV present");
    assert.ok(tunerCountTlv, "tuner count TLV present");
    assert.ok(baseUrlTlv, "base URL TLV present");

    assert.equal(deviceTypeTlv.value.readUInt32BE(0), fields.deviceType);
    assert.equal(deviceIdTlv.value.readUInt32BE(0), fields.deviceId);
    assert.equal(tunerCountTlv.value.readUInt8(0), fields.tunerCount);
    assert.equal(stripTrailingNul(baseUrlTlv.value), fields.baseUrl);
  });
});

describe("buildGetReply and buildErrorReply", () => {

  test("buildGetReply emits packet type PACKET_GET_REPLY with name and value TLVs", () => {

    const packet = buildGetReply("/sys/version", "1.10.3");
    const payload = packet.subarray(4, 4 + packet.readUInt16BE(2));
    const tlvs = decodeTlvsForTest(payload);
    const nameTlv = tlvs.find((t) => (t.tag === TLV_GETSET_NAME));
    const valueTlv = tlvs.find((t) => (t.tag === TLV_GETSET_VALUE));

    assert.equal(packet.readUInt16BE(0), PACKET_GET_REPLY);
    assert.ok(nameTlv);
    assert.ok(valueTlv);
    assert.equal(stripTrailingNul(nameTlv.value), "/sys/version");
    assert.equal(stripTrailingNul(valueTlv.value), "1.10.3");
  });

  test("buildErrorReply emits packet type PACKET_GET_REPLY with name and error TLVs", () => {

    const packet = buildErrorReply("/tuner0/channel", "ERROR: write protected");
    const payload = packet.subarray(4, 4 + packet.readUInt16BE(2));
    const tlvs = decodeTlvsForTest(payload);
    const nameTlv = tlvs.find((t) => (t.tag === TLV_GETSET_NAME));
    const errorTlv = tlvs.find((t) => (t.tag === TLV_ERROR));

    assert.equal(packet.readUInt16BE(0), PACKET_GET_REPLY);
    assert.ok(nameTlv);
    assert.ok(errorTlv);
    assert.equal(stripTrailingNul(nameTlv.value), "/tuner0/channel");
    assert.equal(stripTrailingNul(errorTlv.value), "ERROR: write protected");
  });
});

describe("Set and Upgrade requests are classified correctly so transport layer can reject them", () => {

  test("PACKET_GET_REQUEST carrying a value TLV decodes as 'set' (the protocol uses one packet code for both Get and Set)", () => {

    const parsed = parsePacket(makeGetRequest("/tuner0/channel", "auto:5"));

    assert.equal(parsed?.type, "set");
  });

  test("PACKET_UPGRADE_REQUEST (0x0006) decodes as 'unsupported' so the transport layer can emit an error reply or drop", () => {

    const parsed = parsePacket(sealPacket(PACKET_UPGRADE_REQUEST, Buffer.alloc(0)));

    assert.equal(parsed?.type, "unsupported");
  });

  test("an unknown packet code (not in the recognized set) parses as 'unsupported' with the original code echoed back", () => {

    // 0x00FF is not a wire-defined HDHR packet type; the parser must route any unrecognized code to the unsupported branch so the transport can decide how to
    // respond. Surfacing the original code in the result lets the transport log the unrecognized type for diagnostics.
    const parsed = parsePacket(sealPacket(0x00FF, Buffer.alloc(0)));

    assert.equal(parsed?.type, "unsupported");
    assert.deepEqual(parsed, { packetType: 0x00FF, type: "unsupported" });
  });
});

describe("parsePacket TLV-level malformation and boundary handling", () => {

  test("returns null when a TLV length field overruns the payload despite a valid frame and CRC", () => {

    // A single TLV declares a 10-byte value but only two value bytes follow. sealPacket computes a correct outer frame length and a matching CRC, so the packet
    // clears the header-size and CRC gates; only the TLV walk inside decodeTlvs detects the overrun, and it must reject the entire packet rather than parse it
    // partially.
    const payload = Buffer.from([ TLV_GETSET_NAME, 10, 0x41, 0x42 ]);
    const packet = sealPacket(PACKET_GET_REQUEST, payload);

    // Prove the outer frame and CRC are internally consistent so the null result is attributable solely to the TLV overrun, not to a frame-size or CRC mismatch.
    assert.equal(packet.readUInt16BE(2), payload.length, "declared payload length matches the datagram size");
    assert.equal(crc32(packet.subarray(0, packet.length - 4)), packet.readUInt32LE(packet.length - 4), "CRC is valid over header and payload");
    assert.equal(parsePacket(packet), null);
  });

  test("returns null for a GETSET_REQ carrying a value TLV but no name TLV", () => {

    // A value-only payload (tag 0x04) with no accompanying name TLV. Without a name there is nothing to dispatch, so the parser must reject rather than invent a
    // nameless get keyed on the empty string.
    const valueBytes = Buffer.from("auto:5\0", "utf8");
    const valueTlv = Buffer.concat([ Buffer.from([ TLV_GETSET_VALUE, valueBytes.length ]), valueBytes ]);

    assert.equal(parsePacket(sealPacket(PACKET_GET_REQUEST, valueTlv)), null);
  });

  test("returns null for a GETSET_REQ with an empty payload (no name TLV at all)", () => {

    // An empty payload decodes to an empty TLV list, so no name TLV is present. The parser must reject the same way it rejects a value-only payload.
    assert.equal(parsePacket(sealPacket(PACKET_GET_REQUEST, Buffer.alloc(0))), null);
  });

  test("a string TLV whose UTF-8+NUL length is >= 128 bytes round-trips through the two-byte extended length form", () => {

    // A 200-character ASCII string plus its trailing NUL is 201 value bytes, which exceeds the 127-byte single-byte length ceiling and forces encodeStringTlv
    // into the two-byte extended header. buildGetReply is the public entry that exercises encodeStringTlv; we then feed the emitted value TLV bytes back through
    // parsePacket's decodeTlvs (by retagging the value TLV as a name TLV) to prove the extended header round-trips to the identical string.
    const longString = "x".repeat(200);
    const reply = buildGetReply("", longString);
    const payloadLength = reply.readUInt16BE(2);
    const payload = reply.subarray(4, 4 + payloadLength);

    // The empty-name TLV occupies three bytes ([ tag, length=1, NUL ]); the long value TLV follows it. Copy the value TLV out so we can mutate its tag.
    const valueTlv = Buffer.from(payload.subarray(3));

    // encodeStringTlv must have emitted the extended header: the length byte carries the high bit and the 15-bit little-endian length reconstructs to 201.
    assert.equal(valueTlv.readUInt8(0), TLV_GETSET_VALUE, "value TLV tag");
    assert.notEqual(valueTlv.readUInt8(1) & 0x80, 0, "extended-length high bit is set on the first length byte");

    const reconstructedLength = (valueTlv.readUInt8(1) & 0x7F) | (valueTlv.readUInt8(2) << 7);

    assert.equal(reconstructedLength, longString.length + 1, "15-bit length equals the UTF-8 bytes plus the trailing NUL");

    // Retag the extended-form value TLV as a name TLV and feed it through parsePacket so decodeTlvs reads the extended length and readStringTlv strips the NUL.
    valueTlv.writeUInt8(TLV_GETSET_NAME, 0);

    const parsed = parsePacket(sealPacket(PACKET_GET_REQUEST, valueTlv));

    assert.deepEqual(parsed, { name: longString, type: "get" });
  });

  test("a Discover device-type TLV whose value is not exactly 4 bytes falls back to wildcard rather than reading a garbage integer", () => {

    // The device-type TLV carries six value bytes instead of the required four. readU32Tlv must reject the wrong-width value and yield null, which parsePacket
    // maps to the wildcard default. A regression that dropped the exact-length guard would read the first four bytes (0xDEADBEEF) as a bogus device-type filter.
    // The device-id TLV is well-formed so the assertion proves only the malformed field falls back while the correct one still decodes.
    const deviceTypeTlv = Buffer.from([ TLV_DEVICE_TYPE, 6, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00 ]);
    const deviceIdTlv = Buffer.from([ TLV_DEVICE_ID, 4, 0x12, 0x34, 0x56, 0x78 ]);
    const packet = sealPacket(PACKET_DISCOVER_REQUEST, Buffer.concat([ deviceTypeTlv, deviceIdTlv ]));

    assert.deepEqual(parsePacket(packet), {

      requestedDeviceId: 0x12345678,
      requestedDeviceType: HDHR_WILDCARD,
      type: "discover"
    });
  });
});

// decodeTlvsForTest is a test-local TLV walker. It mirrors the parser's logic minimally so tests can introspect payload contents without reaching into module-
// private helpers. We support only the single-byte length form because every TLV the builder emits uses it.
function decodeTlvsForTest(payload: Buffer): { tag: number; value: Buffer }[] {

  const tlvs: { tag: number; value: Buffer }[] = [];
  let offset = 0;

  while(offset < payload.length) {

    const tag = payload.readUInt8(offset);
    const length = payload.readUInt8(offset + 1);

    tlvs.push({ tag, value: payload.subarray(offset + 2, offset + 2 + length) });
    offset += 2 + length;
  }

  return tlvs;
}

// stripTrailingNul removes the trailing NUL byte from a string-TLV value so assertions can compare against the bare string the caller passed in.
function stripTrailingNul(buf: Buffer): string {

  return (buf[buf.length - 1] === 0) ? buf.subarray(0, -1).toString("utf8") : buf.toString("utf8");
}
