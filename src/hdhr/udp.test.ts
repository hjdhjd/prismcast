/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * udp.test.ts: Integration tests for the HDHomeRun UDP transport. Coverage layers:
 *
 *   1. selectLanAddress is exercised as a pure function against synthetic NetworkInterfaceInfo maps so the subnet-match and fallback logic is verified without
 *      relying on the host's actual interface configuration.
 *
 *   2. The UdpSurface node is exercised via dgram loopback on an ephemeral port: each test creates a surface with `await using` (so its [Symbol.asyncDispose]
 *      tears the socket down at scope exit), binds the responder on 127.0.0.1:0 with a provider standing in for the HTTP surface's bound port, sends a Discover
 *      request, and asserts that a structurally valid reply comes back. The full request-reply round-trip validates the entire path - parser, dispatcher,
 *      encoder, and socket send - and the reply's BaseURL is decoded against a configured HDHR port held apart from the provider's.
 *
 *   3. Get and Set request paths are exercised similarly to confirm the transport composes the correct reply type for each parsed packet.
 *
 *   4. Negative and failure paths: a valid Upgrade request (which parses as an unsupported type) is dropped without a reply - distinct from the malformed-packet
 *      drop, which fails the parser's length/CRC check - a Discover request addressed to another device type or another device id is dropped without a reply, a
 *      Discover request that arrives while the provider reports no bound HTTP port goes unanswered, and a bind collision on the responder port resolves ensureUp
 *      false at warn level rather than throwing, so the HTTP HDHR surface survives a discovery-port conflict. The no-reply rows for a Discover addressed to
 *      another device and for a Discover with no bound HTTP port pass a shortened receive bound, so their wait is a fraction of a second rather than the full
 *      round-trip budget; the malformed-packet and Upgrade rows wait out the default bound.
 *
 *   5. The bind lifecycle: a second ensureUp call returns true without rebinding, ensureDown closes the socket and leaves the surface
 *      reusable so a later ensureUp rebinds cleanly, and HDHR_DISCOVERY_PORT is asserted against the canonical SiliconDust value so a refactor cannot silently
 *      change it.
 *
 * The integration tests run on 127.0.0.1 with an ephemeral port so they cannot collide with a real HDHomeRun device or another emulator on the developer's
 * host. `await using` disposal tears the responder down at the end of each test, so there is no afterEach to forget. Request packet builders (makeDiscoverRequest,
 * makeGetRequest) and the shared framing helper (sealPacket) come from protocol.helpers.ts so every HDHomeRun test speaks the same wire format.
 */
import { CONFIG, initializeConfiguration } from "../config/index.ts";
import { HDHR_DISCOVERY_PORT, createUdpSurface, selectLanAddress } from "./udp.ts";
import { PACKET_DISCOVER_REPLY, PACKET_GET_REPLY, PACKET_UPGRADE_REQUEST, TLV_BASE_URL, TLV_DEVICE_ID, TLV_DEVICE_TYPE, TLV_ERROR, TLV_GETSET_NAME,
  TLV_GETSET_VALUE, TLV_TUNER_COUNT } from "./protocol.ts";
import { describe, test } from "node:test";
import { makeDiscoverRequest, makeGetRequest, sealPacket } from "./protocol.helpers.ts";
import type { ConfigStore } from "../config/index.ts";
import { HDHR_DEVICE_TYPE_TUNER } from "./identity.ts";
import type { LogEntry } from "../utils/logEmitter.ts";
import type { NetworkInterfaceInfo } from "node:os";
import { SEEDED_DEVICE_ID } from "../config/index.helpers.ts";
import type { UdpSurface } from "./udp.ts";
import assert from "node:assert/strict";
import { createSocket } from "node:dgram";
import { subscribeToLogs } from "../utils/logEmitter.ts";

// The HTTP port the round-trip responders advertise. Nothing binds it: the controller hands a responder the HTTP surface's bound port through a provider, and
// these rows hand it this value the same way.
const HTTP_PORT = 5150;

// httpPortProvider stands in for the HTTP surface's bound-port accessor, the provider every ensureUp call requires.
function httpPortProvider(): number {

  return HTTP_PORT;
}

/**
 * Builds an in-memory config store whose file names only the given HDHomeRun port and a valid DeviceID, so a row can re-initialize CONFIG and the loaded
 * snapshot from it and the boot finds nothing to correct. Only the boot reads through it.
 * @param port - The HDHomeRun port the file names.
 * @returns The store.
 */
function storeNamingHdhrPort(port: number): ConfigStore {

  return {

    mutateConfigThen: async (): Promise<never> => {

      throw new Error("The read-only store takes no writes.");
    },
    readConfig: async () => ({ config: { hdhr: { deviceId: SEEDED_DEVICE_ID, port } }, parseError: false, readError: false })
  };
}

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
  async function sendAndReceive(port: number, request: Buffer, timeoutMs = 2000): Promise<Buffer> {

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

  // Wildcard Discover request for the Discover round-trip test. The wildcard device type and device ID match any responder, keeping the test focused on the
  // reply assertion.
  function wildcardDiscover(): Buffer {

    return makeDiscoverRequest(0xFFFFFFFF, 0xFFFFFFFF);
  }

  test("Discover request elicits a Discover reply with the four required TLVs", async () => {

    await using surface = createUdpSurface();
    const ok = await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

    assert.equal(ok, true);

    // We passed port 0, so the kernel assigned an ephemeral port; retrieve it through the node's boundPort accessor.
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

  test("the Discover reply's BaseURL names the port the provider reports, whatever port CONFIG names", async () => {

    // The configured HDHR port is held apart from the provider's as the negative control: the reply must advertise the port the HTTP surface is bound to.
    await initializeConfiguration(undefined, storeNamingHdhrPort(HTTP_PORT + 1));

    assert.equal(CONFIG.hdhr.port, HTTP_PORT + 1, "precondition: CONFIG names a port other than the provider's");

    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

    const reply = await sendAndReceive(requireBoundPort(surface), wildcardDiscover());
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

    // encodeStringTlv null-terminates the wire value, so trim the terminator before matching. The host is whichever LAN address selectLanAddress picks.
    const baseUrl = values.get(TLV_BASE_URL)?.toString("utf8").replace(/\0$/, "") ?? "";

    assert.match(baseUrl, new RegExp("^http://[0-9.]+:" + String(HTTP_PORT) + "$"), "the BaseURL names the provider's port: " + baseUrl);
  });

  test("a Discover request goes unanswered while the HTTP port provider reports no bound port", async () => {

    // The controller brings discovery down before the HTTP surface, so a provider reporting no port means the HTTP surface is going away, and a reply then would
    // advertise a BaseURL with no listener behind it.
    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider: () => null, port: 0 });

    await assert.rejects(() => sendAndReceive(requireBoundPort(surface), wildcardDiscover(), 300), /Timed out waiting/);
  });

  test("Get request for /sys/version elicits a Get reply with name and value TLVs", async () => {

    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

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

    await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

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

    await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

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

    await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

    const port = requireBoundPort(surface);
    const garbage = Buffer.from([ 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF ]);

    // Expect the sendAndReceive to time out because the responder drops malformed packets without replying.
    await assert.rejects(() => sendAndReceive(port, garbage), /Timed out waiting/);
  });

  test("a Discover request addressed to this device's own id and the tuner type is answered", async () => {

    /* The addressed case. CONFIG.hdhr.deviceId is set to a known value for the row so the request can name this device explicitly rather than relying on the
     * wildcard, which is what separates the id check from the type check in the rows below.
     */
    const originalDeviceId = CONFIG.hdhr.deviceId;

    CONFIG.hdhr.deviceId = "1234ABCD";

    try {

      await using surface = createUdpSurface();

      await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

      const port = requireBoundPort(surface);
      const reply = await sendAndReceive(port, makeDiscoverRequest(HDHR_DEVICE_TYPE_TUNER, 0x1234ABCD));

      assert.equal(reply.readUInt16BE(0), PACKET_DISCOVER_REPLY, "a request naming this device's type and id is answered");
    } finally {

      CONFIG.hdhr.deviceId = originalDeviceId;
    }
  });

  test("a Discover request addressed to a foreign device id is dropped without a reply", async () => {

    /* The detector for the id check. Without it the responder answers every Discover request it can parse, so a client looking for one specific tuner receives an
     * answer from PrismCast as well and can bind to a device it never asked for.
     */
    const originalDeviceId = CONFIG.hdhr.deviceId;

    CONFIG.hdhr.deviceId = "1234ABCD";

    try {

      await using surface = createUdpSurface();

      await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

      const port = requireBoundPort(surface);

      await assert.rejects(() => sendAndReceive(port, makeDiscoverRequest(HDHR_DEVICE_TYPE_TUNER, 0x0BADF00D), 300), /Timed out waiting/);
    } finally {

      CONFIG.hdhr.deviceId = originalDeviceId;
    }
  });

  test("a Discover request addressed to a foreign device type is dropped without a reply", async () => {

    // The type check, exercised with a wildcard id so only the type can decide the outcome. PrismCast presents as a tuner; a request for any other device class
    // is not addressed to it.
    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

    const port = requireBoundPort(surface);

    await assert.rejects(() => sendAndReceive(port, makeDiscoverRequest(0x00000005, 0xFFFFFFFF), 300), /Timed out waiting/);
  });

  test("a valid but unsupported packet type (Upgrade) is dropped without a reply", async () => {

    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

    const port = requireBoundPort(surface);

    // A well-formed, valid-CRC Upgrade request parses cleanly as { type: "unsupported" } and must be dropped WITHOUT a reply. This is a distinct branch from the
    // malformed-drop above: that datagram fails parsePacket's length/CRC check and returns early, whereas this one parses successfully and reaches the
    // case "unsupported" dispatch arm - so a regression that answered unsupported packets would deliver a reply here instead of timing out.
    const upgrade = sealPacket(PACKET_UPGRADE_REQUEST, Buffer.alloc(0));

    await assert.rejects(() => sendAndReceive(port, upgrade), /Timed out waiting/);
  });

  test("repeat-safe ensureUp: calling it twice returns true without rebinding", async () => {

    await using surface = createUdpSurface();
    const first = await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

    assert.equal(first, true);

    // Capture the bound port AFTER the first call and BEFORE the second so the no-op claim is verifiable: a second ensureUp that silently rebound would land on a
    // different ephemeral port, which this baseline diff catches. Comparing the getter against itself would be tautological and could not detect a rebind.
    const firstPort = requireBoundPort(surface);
    const second = await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

    assert.equal(second, true, "second call is a no-op success");
    assert.equal(surface.boundPort, firstPort, "the bound port is unchanged by the second call");
  });

  test("ensureDown closes the socket and is reusable: a later ensureUp rebinds", async () => {

    await using surface = createUdpSurface();

    await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

    assert.notEqual(surface.boundPort, null, "surface is bound after the first ensureUp");

    await surface.ensureDown();

    assert.equal(surface.boundPort, null, "surface is down after ensureDown");

    // The node is owner-bounded, not scope-poisoned: a fresh ensureUp must rebind cleanly.
    const rebound = await surface.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

    assert.equal(rebound, true, "a stopped surface rebinds on the next ensureUp");
    assert.notEqual(surface.boundPort, null, "surface is bound again after the second ensureUp");
  });

  test("a bind collision on the responder port resolves ensureUp false at warn level without throwing", async () => {

    await using first = createUdpSurface();
    const firstOk = await first.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port: 0 });

    assert.equal(firstOk, true, "the first surface binds the ephemeral port");

    const port = requireBoundPort(first);

    // Capture warn-level logs only across the colliding bind so the assertion is scoped to this event and cannot pick up unrelated lifecycle lines.
    const warnings: string[] = [];
    const unsubscribe = subscribeToLogs((entry: LogEntry) => {

      if(entry.level === "warn") {

        warnings.push(entry.message);
      }
    });

    // A second surface binding the SAME address and port collides. reuseAddr is deliberately false, so the kernel returns EADDRINUSE, which the bind-failure
    // handler treats as graceful "discovery unavailable": ensureUp resolves false, never throws or rejects, and the surface stays down so the HTTP HDHR surface
    // keeps working. Binding the first surface's actual ephemeral port keeps the collision deterministic without hard-coding a port that another host process
    // might already hold.
    await using second = createUdpSurface();
    const secondOk = await second.ensureUp({ bindAddress: "127.0.0.1", httpPortProvider, port });

    unsubscribe();

    assert.equal(secondOk, false, "the colliding bind resolves false rather than throwing");
    assert.equal(second.boundPort, null, "the collided surface stays down");
    assert.ok(warnings.some((message) => message.includes("already in use")), "the collision is surfaced at warn level");
  });

  test("HDHR_DISCOVERY_PORT constant matches the canonical SiliconDust value", () => {

    // Assert the constant so a refactor cannot silently change it. The wire protocol fixes this port; clients hard-code it.
    assert.equal(HDHR_DISCOVERY_PORT, 65001);
  });
});

// requireBoundPort reads the surface's bound port with a non-null assertion so each test reads as "send to the responder's port". A forgotten ensureUp call in
// test setup surfaces as a clear failure rather than a confusing port-zero send.
function requireBoundPort(surface: UdpSurface): number {

  const port = surface.boundPort;

  assert.ok(port !== null, "expected responder socket to be bound before reading its port");

  return port;
}
