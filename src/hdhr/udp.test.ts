/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * udp.test.ts: Integration tests for the HDHomeRun UDP transport. Coverage layers:
 *
 *   1. selectLanAddress is exercised as a pure function against synthetic NetworkInterfaceInfo maps so the subnet-match and fallback logic is verified without
 *      relying on the host's actual interface configuration.
 *
 *   2. The UdpSurface node is exercised via dgram loopback on an ephemeral port: each test creates a surface with `await using` (so its [Symbol.asyncDispose]
 *      tears the socket down at scope exit), binds the responder on 127.0.0.1:0, sends a Discover request, and asserts that a structurally valid reply comes
 *      back. The full request-reply round-trip validates the entire path - parser, dispatcher, encoder, and socket send.
 *
 *   3. Get and Set request paths are exercised similarly to confirm the transport composes the correct reply type for each parsed packet.
 *
 * The integration tests run on 127.0.0.1 with an ephemeral port so they cannot collide with a real HDHomeRun device or another emulator on the developer's
 * host. `await using` disposal tears the responder down at the end of each test, so there is no afterEach to forget. Request packet builders (makeDiscoverRequest,
 * makeGetRequest) and the shared framing helper (sealPacket) come from protocol.helpers.ts so both test files speak the same wire format.
 */
import { HDHR_DISCOVERY_PORT, createUdpSurface, selectLanAddress } from "./udp.ts";
import { PACKET_DISCOVER_REPLY, PACKET_GET_REPLY, TLV_BASE_URL, TLV_DEVICE_ID, TLV_DEVICE_TYPE, TLV_ERROR, TLV_GETSET_NAME, TLV_GETSET_VALUE,
  TLV_TUNER_COUNT } from "./protocol.ts";
import { describe, test } from "node:test";
import { makeDiscoverRequest, makeGetRequest } from "./protocol.helpers.ts";
import type { NetworkInterfaceInfo } from "node:os";
import type { UdpSurface } from "./udp.ts";
import assert from "node:assert/strict";
import { createSocket } from "node:dgram";

// makeIPv4 builds a synthetic NetworkInterfaceInfo entry shaped like what os.networkInterfaces() returns. Captures only the fields selectLanAddress reads;
// other fields the runtime would populate are filled with placeholder values to satisfy the type.
function makeIPv4(address: string, netmask: string, internal = false): NetworkInterfaceInfo {

  return {

    address,
    cidr: address + "/24",
    family: "IPv4",
    internal,
    mac: "00:00:00:00:00:00",
    netmask
  };
}

describe("selectLanAddress", () => {

  test("returns the address of the interface whose subnet contains the target", () => {

    // Two non-loopback interfaces. The target 192.168.1.50 lies in the subnet of "en0" but not "eth1"; selectLanAddress should pick en0.
    const interfaces = {

      en0: [makeIPv4("192.168.1.5", "255.255.255.0")],
      eth1: [makeIPv4("10.0.0.5", "255.255.255.0")],
      lo0: [makeIPv4("127.0.0.1", "255.0.0.0", true)]
    };

    assert.equal(selectLanAddress("192.168.1.50", interfaces), "192.168.1.5");
  });

  test("falls back to the first non-loopback IPv4 when no subnet matches", () => {

    // The target 172.16.0.5 is not in either subnet; the function falls back to the first non-loopback address.
    const interfaces = {

      en0: [makeIPv4("192.168.1.5", "255.255.255.0")],
      eth1: [makeIPv4("10.0.0.5", "255.255.255.0")]
    };

    assert.equal(selectLanAddress("172.16.0.5", interfaces), "192.168.1.5");
  });

  test("returns 127.0.0.1 when no non-loopback interfaces are configured", () => {

    // Degenerate case: only loopback is present. The function returns the loopback fallback so the reply still carries a syntactically valid BaseURL.
    const interfaces = { lo0: [makeIPv4("127.0.0.1", "255.0.0.0", true)] };

    assert.equal(selectLanAddress("192.168.1.50", interfaces), "127.0.0.1");
  });

  test("returns 127.0.0.1 for a malformed target address", () => {

    // Malformed input (not four octets) falls through subnet matching and lands on the fallback path.
    const interfaces = {};

    assert.equal(selectLanAddress("not-an-ip", interfaces), "127.0.0.1");
  });
});

describe("UdpSurface - round-trip", () => {

  // sendAndReceive opens a client socket, sends the request to 127.0.0.1:<port>, and resolves with the first reply (or rejects on timeout).
  async function sendAndReceive(port: number, request: Buffer): Promise<Buffer> {

    const { promise, resolve, reject } = Promise.withResolvers<Buffer>();
    const client = createSocket("udp4");
    const timer = setTimeout(() => {

      client.close();
      reject(new Error("Timed out waiting for UDP reply."));
    }, 2000);

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

  // Wildcard Discover request for the Discover round-trip test. The wildcard device type and device ID match any responder, keeping the test focused on the
  // reply assertion.
  function wildcardDiscover(): Buffer {

    return makeDiscoverRequest(0xFFFFFFFF, 0xFFFFFFFF);
  }

  test("Discover request elicits a Discover reply with the four required TLVs", async () => {

    await using surface = createUdpSurface();
    const ok = await surface.ensureUp({ bindAddress: "127.0.0.1", port: 0 });

    assert.equal(ok, true);

    // We did not pass an explicit port; the responder bound an ephemeral port. Retrieve it via the node's boundPort accessor.
    const port = requireBoundPort(surface);
    const reply = await sendAndReceive(port, wildcardDiscover());

    assert.equal(reply.readUInt16BE(0), PACKET_DISCOVER_REPLY);

    const payloadLen = reply.readUInt16BE(2);
    const payload = reply.subarray(4, 4 + payloadLen);
    const tagsSeen = new Set<number>();
    let offset = 0;

    while(offset < payload.length) {

      const tag = payload.readUInt8(offset);
      const length = payload.readUInt8(offset + 1);

      tagsSeen.add(tag);
      offset += 2 + length;
    }

    assert.ok(tagsSeen.has(TLV_DEVICE_TYPE), "DEVICE_TYPE TLV present");
    assert.ok(tagsSeen.has(TLV_DEVICE_ID), "DEVICE_ID TLV present");
    assert.ok(tagsSeen.has(TLV_TUNER_COUNT), "TUNER_COUNT TLV present");
    assert.ok(tagsSeen.has(TLV_BASE_URL), "BASE_URL TLV present");
  });

  test("Get request for /sys/version elicits a Get reply with name and value TLVs", async () => {

    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", port: 0 });

    const port = requireBoundPort(surface);
    const reply = await sendAndReceive(port, makeGetRequest("/sys/version"));

    assert.equal(reply.readUInt16BE(0), PACKET_GET_REPLY);

    const payloadLen = reply.readUInt16BE(2);
    const payload = reply.subarray(4, 4 + payloadLen);
    const tagsSeen = new Set<number>();
    let offset = 0;

    while(offset < payload.length) {

      const tag = payload.readUInt8(offset);
      const length = payload.readUInt8(offset + 1);

      tagsSeen.add(tag);
      offset += 2 + length;
    }

    assert.ok(tagsSeen.has(TLV_GETSET_NAME), "name TLV echoed in reply");
    assert.ok(tagsSeen.has(TLV_GETSET_VALUE), "value TLV present for known key");
    assert.equal(tagsSeen.has(TLV_ERROR), false, "no error TLV when key is recognized");
  });

  test("Get request for an unknown key elicits an error reply", async () => {

    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", port: 0 });

    const port = requireBoundPort(surface);
    const reply = await sendAndReceive(port, makeGetRequest("/sys/totally-not-a-real-key"));

    assert.equal(reply.readUInt16BE(0), PACKET_GET_REPLY);

    const payloadLen = reply.readUInt16BE(2);
    const payload = reply.subarray(4, 4 + payloadLen);
    const tagsSeen = new Set<number>();
    let offset = 0;

    while(offset < payload.length) {

      const tag = payload.readUInt8(offset);
      const length = payload.readUInt8(offset + 1);

      tagsSeen.add(tag);
      offset += 2 + length;
    }

    assert.ok(tagsSeen.has(TLV_ERROR), "error TLV present for unknown key");
    assert.equal(tagsSeen.has(TLV_GETSET_VALUE), false, "no value TLV on error response");
  });

  test("Set request is write-protected: a value-bearing request elicits an error reply", async () => {

    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", port: 0 });

    const port = requireBoundPort(surface);

    // A Get request carrying a value TLV parses as a Set. PrismCast does not implement RTP-style Set control, so the transport must answer with an explicit
    // "write protected" error rather than dropping the packet (a silent drop would leave the client blocked waiting for an ACK).
    const reply = await sendAndReceive(port, makeGetRequest("/tuner0/channel", "auto:0"));

    assert.equal(reply.readUInt16BE(0), PACKET_GET_REPLY);

    const payloadLen = reply.readUInt16BE(2);
    const payload = reply.subarray(4, 4 + payloadLen);
    const values = new Map<number, Buffer>();
    let offset = 0;

    while(offset < payload.length) {

      const tag = payload.readUInt8(offset);
      const length = payload.readUInt8(offset + 1);

      values.set(tag, payload.subarray(offset + 2, offset + 2 + length));
      offset += 2 + length;
    }

    assert.ok(values.has(TLV_GETSET_NAME), "name TLV echoed in the error reply");
    assert.ok(values.has(TLV_ERROR), "error TLV present for a write-protected Set");
    assert.equal(values.has(TLV_GETSET_VALUE), false, "no value TLV on a write-protected Set reply");

    // The error string distinguishes the Set branch from a generic unknown-key error. encodeStringTlv null-terminates the wire value, so trim the terminator.
    const errorText = values.get(TLV_ERROR)?.toString("utf8").replace(/\0$/, "");

    assert.equal(errorText, "ERROR: write protected");
  });

  test("malformed packets are silently dropped (no reply at all)", async () => {

    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", port: 0 });

    const port = requireBoundPort(surface);
    const garbage = Buffer.from([ 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF ]);

    // Expect the sendAndReceive to time out because the responder drops malformed packets without replying.
    await assert.rejects(() => sendAndReceive(port, garbage), /Timed out waiting/);
  });

  test("idempotent ensureUp: calling it twice returns true without rebinding", async () => {

    await using surface = createUdpSurface();
    const first = await surface.ensureUp({ bindAddress: "127.0.0.1", port: 0 });

    assert.equal(first, true);

    // Capture the bound port AFTER the first call and BEFORE the second so the no-op claim is verifiable: a second ensureUp that silently rebound would land on a
    // different ephemeral port, which this baseline diff catches. Comparing the getter against itself would be tautological and could not detect a rebind.
    const firstPort = requireBoundPort(surface);
    const second = await surface.ensureUp({ bindAddress: "127.0.0.1", port: 0 });

    assert.equal(second, true, "second call is a no-op success");
    assert.equal(surface.boundPort, firstPort, "the bound port is unchanged by the idempotent second call");
  });

  test("ensureDown closes the socket and is reusable: a later ensureUp rebinds", async () => {

    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", port: 0 });

    assert.notEqual(surface.boundPort, null, "surface is bound after the first ensureUp");

    await surface.ensureDown();

    assert.equal(surface.boundPort, null, "surface is down after ensureDown");

    // The node is owner-bounded, not scope-poisoned: a fresh ensureUp must rebind cleanly.
    const rebound = await surface.ensureUp({ bindAddress: "127.0.0.1", port: 0 });

    assert.equal(rebound, true, "a stopped surface rebinds on the next ensureUp");
    assert.notEqual(surface.boundPort, null, "surface is bound again after the second ensureUp");
  });

  test("HDHR_DISCOVERY_PORT constant matches the canonical SiliconDust value", () => {

    // Pin the constant so a refactor cannot silently change it. The wire protocol fixes this port; clients hard-code it.
    assert.equal(HDHR_DISCOVERY_PORT, 65001);
  });
});

// requireBoundPort reads the surface's bound port with a load-bearing non-null assertion so each test reads as "send to the responder's port". A malformed test
// setup (forgetting ensureUp) surfaces as a clear failure rather than a confusing port-zero send.
function requireBoundPort(surface: UdpSurface): number {

  const port = surface.boundPort;

  assert.ok(port !== null, "expected responder socket to be bound before reading its port");

  return port;
}
