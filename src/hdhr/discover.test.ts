/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * discover.test.ts: Unit tests for the HDHomeRun discovery endpoints. The endpoints expose Plex's tuner-discovery surface (device.xml, discover.json,
 * lineup.json, lineup_status.json, status.json), and the handler bodies are pure transforms over CONFIG, the channel map, and the stream registry. The tests
 * spin up an ephemeral-port Express server (port 0 -> OS-assigned) so route resolution, header parsing, and content negotiation are exercised end-to-end
 * without binding to the production HDHR port. A single server is shared across the suite to amortize listen/close costs; tests that mutate CONFIG always
 * restore it in a finally block.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import { registerStream, unregisterStream } from "../streaming/registry.ts";
import { CONFIG } from "../config/index.ts";
import assert from "node:assert/strict";
import express from "express";
import { firstOf } from "../testing.helpers.ts";
import { makeRegistryEntry } from "../streaming/registry.helpers.ts";
import { setupHdhrEndpoints } from "./discover.ts";

// makeServer spins up an Express app on an OS-assigned port (0 = let the kernel pick), wires the HDHR endpoints, and returns the server with its bound port.
function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  app.set("trust proxy", true);
  setupHdhrEndpoints(app);

  return new Promise((resolve, reject) => {

    const server = app.listen(0, "127.0.0.1", () => {

      const address = server.address() as AddressInfo;

      resolve({ port: address.port, server });
    });

    server.on("error", reject);
  });
}

// closeServer wraps server.close in a promise so the after hook awaits actual socket teardown.
function closeServer(server: Server): Promise<void> {

  return new Promise((resolve) => {

    server.close(() => {

      resolve();
    });
  });
}

// Shared server reference. Set in the suite-wide before hook so individual tests can compose URLs against the same port.
let sharedServer: Server;
let sharedPort = 0;

// urlFor builds a request URL on the shared server. Centralizing the host/port avoids leaking those literals into every test body.
function urlFor(path: string): string {

  return "http://127.0.0.1:" + String(sharedPort) + path;
}

before(async () => {

  const created = await makeServer();

  sharedServer = created.server;
  sharedPort = created.port;
});

after(async () => {

  await closeServer(sharedServer);
});

describe("setupHdhrEndpoints - GET /device.xml", () => {

  test("returns valid UPnP device XML with text/xml Content-Type and the static device fields", async () => {

    const res = await fetch(urlFor("/device.xml"));
    const body = await res.text();

    assert.equal(res.status, 200);
    assert.match(res.headers.get("content-type") ?? "", /text\/xml/);
    assert.match(body, /<\?xml version="1\.0" encoding="UTF-8"\?>/);
    assert.match(body, /<root xmlns="urn:schemas-upnp-org:device-1-0">/);
    assert.match(body, /<deviceType>urn:schemas-upnp-org:device:MediaServer:1<\/deviceType>/);
    assert.match(body, /<manufacturer>PrismCast<\/manufacturer>/);
    assert.match(body, /<modelName>HDTC-2US<\/modelName>/);
  });

  test("includes the configured friendlyName in the XML body", async () => {

    const original = CONFIG.hdhr.friendlyName;

    CONFIG.hdhr.friendlyName = "TestPrism";

    try {

      const res = await fetch(urlFor("/device.xml"));
      const body = await res.text();

      assert.match(body, /<friendlyName>TestPrism<\/friendlyName>/);
    } finally {

      CONFIG.hdhr.friendlyName = original;
    }
  });

  test("escapes XML metacharacters in the configured friendlyName", async () => {

    const original = CONFIG.hdhr.friendlyName;

    // friendlyName is free text the operator types, so an ampersand or an angle bracket in it reaches the document verbatim unless the handler escapes it. The
    // negative assertions matter as much as the positive ones: a document carrying a raw "<" is one a client cannot parse, whatever else it also contains.
    CONFIG.hdhr.friendlyName = "Prism & <Cast>";

    try {

      const res = await fetch(urlFor("/device.xml"));
      const body = await res.text();

      assert.match(body, /<friendlyName>Prism &amp; &lt;Cast&gt;<\/friendlyName>/);
      assert.doesNotMatch(body, /Prism & </);
    } finally {

      CONFIG.hdhr.friendlyName = original;
    }
  });

  test("escapes XML metacharacters arriving through X-Forwarded-Host", async () => {

    // The advertised URLBase is composed from a client-supplied header that resolveHostname reads without sanitizing, which makes the header a second route to
    // a malformed document. We drive it through X-Forwarded-Host rather than an explicit Host header because Node's fetch overrides Host from the request URL,
    // which would leave the assertion proving nothing.
    const res = await fetch(urlFor("/device.xml"), { headers: { "X-Forwarded-Host": "evil&host<tag>" } });
    const body = await res.text();

    assert.match(body, /<URLBase>http:\/\/evil&amp;host&lt;tag&gt;:/);
    assert.doesNotMatch(body, /evil&host</);
  });

  test("includes an uppercased deviceId in the serialNumber and UDN fields", async () => {

    const original = CONFIG.hdhr.deviceId;

    CONFIG.hdhr.deviceId = "abcd1234";

    try {

      const res = await fetch(urlFor("/device.xml"));
      const body = await res.text();

      assert.match(body, /<serialNumber>ABCD1234<\/serialNumber>/);
      assert.match(body, /<UDN>uuid:ABCD1234<\/UDN>/);
    } finally {

      CONFIG.hdhr.deviceId = original;
    }
  });

  test("derives URLBase from the request Host header when no X-Forwarded-Host is present", async () => {

    const res = await fetch(urlFor("/device.xml"));
    const body = await res.text();

    // The server's Host header is "127.0.0.1:<port>"; resolveHostname strips the port and leaves the bare host.
    assert.match(body, new RegExp("<URLBase>http://127\\.0\\.0\\.1:" + String(CONFIG.hdhr.port) + "</URLBase>"));
  });

  test("resolves hostname from X-Forwarded-Host across the documented variants (proxy, comma list, bracketed IPv6)", async () => {

    // Three sub-cases collapsed: a single proxy hostname survives verbatim; a comma-separated list takes the leftmost entry; a bracketed IPv6 entry preserves
    // the brackets and strips the trailing :port. Each case fires its own fetch; combining them avoids three near-duplicate test bodies.
    const cases: { expected: RegExp; header: string; label: string; missing?: RegExp }[] = [
      { expected: /<URLBase>http:\/\/proxy\.example\.com:/, header: "proxy.example.com", label: "single proxy host" },
      { expected: /first\.example\.com/, header: "first.example.com, second.example.com", label: "comma list takes leftmost", missing: /second\.example\.com/ },
      { expected: /\[::1\]/, header: "[::1]:5004", label: "bracketed IPv6 keeps brackets, drops port" }
    ];

    for(const c of cases) {

      // eslint-disable-next-line no-await-in-loop -- sequential because each case is independent and the test asserts after each fetch.
      const res = await fetch(urlFor("/device.xml"), { headers: { "X-Forwarded-Host": c.header } });
      // eslint-disable-next-line no-await-in-loop -- second await of the same sequential round-trip.
      const body = await res.text();

      assert.match(body, c.expected, c.label);

      if(c.missing) {

        assert.doesNotMatch(body, c.missing, c.label + " - left-only");
      }
    }
  });
});

describe("setupHdhrEndpoints - GET /discover.json", () => {

  test("returns the documented HDHomeRun discovery payload", async () => {

    const res = await fetch(urlFor("/discover.json"));
    const body = await res.json() as Record<string, unknown>;

    assert.equal(res.status, 200);
    assert.equal(body["Manufacturer"], "PrismCast");
    assert.equal(body["ModelNumber"], "HDTC-2US");
    assert.equal(body["FirmwareName"], "hdhomeruntc_atsc");
    assert.equal(typeof body["FirmwareVersion"], "string");
    assert.ok((body["FirmwareVersion"] as string).length > 0, "version is non-empty");
  });

  test("DeviceID and DeviceAuth are emitted in uppercase", async () => {

    const original = CONFIG.hdhr.deviceId;

    CONFIG.hdhr.deviceId = "deadbeef";

    try {

      const res = await fetch(urlFor("/discover.json"));
      const body = await res.json() as Record<string, string>;

      assert.equal(body["DeviceID"], "DEADBEEF");
      assert.equal(body["DeviceAuth"], "DEADBEEF", "DeviceAuth mirrors DeviceID");
    } finally {

      CONFIG.hdhr.deviceId = original;
    }
  });

  test("TunerCount mirrors CONFIG.streaming.maxConcurrentStreams", async () => {

    const res = await fetch(urlFor("/discover.json"));
    const body = await res.json() as Record<string, number>;

    assert.equal(body["TunerCount"], CONFIG.streaming.maxConcurrentStreams);
  });

  test("BaseURL and LineupURL are derived from the request hostname and the configured HDHR port", async () => {

    const res = await fetch(urlFor("/discover.json"));
    const body = await res.json() as Record<string, string>;
    const expectedBase = "http://127.0.0.1:" + String(CONFIG.hdhr.port);

    assert.equal(body["BaseURL"], expectedBase);
    assert.equal(body["LineupURL"], expectedBase + "/lineup.json");
  });
});

describe("setupHdhrEndpoints - GET /lineup.json", () => {

  test("returns an array of lineup entries with the documented fields", async () => {

    const res = await fetch(urlFor("/lineup.json"));
    const body = await res.json() as { AudioCodec: string; GuideName: string; GuideNumber: string; HD: number; URL: string; VideoCodec: string }[];

    assert.equal(res.status, 200);
    assert.ok(Array.isArray(body), "response is an array");
    assert.ok(body.length > 0, "predefined channels populate the lineup");

    const first = body[0]!;

    assert.equal(first.AudioCodec, "AAC");
    assert.equal(first.VideoCodec, "H264");
    assert.equal(first.HD, 1);
    assert.equal(typeof first.GuideName, "string");
    assert.equal(typeof first.GuideNumber, "string", "GuideNumber is a string per the HDHR contract");
    assert.match(first.GuideNumber, /^\d+$/, "GuideNumber is numeric digits");
  });

  test("URL field points at the main server's /stream/ endpoint with the channel key", async () => {

    const res = await fetch(urlFor("/lineup.json"));
    const body = await res.json() as { URL: string }[];
    const first = body[0]!;
    const expectedPrefix = "http://127.0.0.1:" + String(CONFIG.server.port) + "/stream/";

    assert.ok(first.URL.startsWith(expectedPrefix), "URL starts with the main server's stream prefix: " + first.URL);
  });

  test("URL hostname comes from the request, not from a hardcoded value", async () => {

    // A reverse-proxy fronted lineup must point clients at the proxy hostname so they can reach back; the resolver feeds X-Forwarded-Host through.
    const res = await fetch(urlFor("/lineup.json"), { headers: { "X-Forwarded-Host": "proxy.example.com" } });
    const body = await res.json() as { URL: string }[];

    assert.match(body[0]!.URL, new RegExp("^http://proxy\\.example\\.com:" + String(CONFIG.server.port) + "/stream/"));
  });
});

describe("setupHdhrEndpoints - GET /lineup_status.json", () => {

  test("returns a static scan-complete payload", async () => {

    const res = await fetch(urlFor("/lineup_status.json"));
    const body = await res.json() as Record<string, unknown>;

    assert.equal(res.status, 200);
    assert.equal(body["ScanInProgress"], 0);
    assert.equal(body["ScanPossible"], 1);
    assert.equal(body["Source"], "Cable");
    assert.deepEqual(body["SourceList"], ["Cable"]);
  });
});

describe("setupHdhrEndpoints - POST /lineup.post", () => {

  test("acknowledges scan-control posts with a 200 OK", async () => {

    const res = await fetch(urlFor("/lineup.post?scan=start"), { method: "POST" });

    assert.equal(res.status, 200);
  });
});

describe("setupHdhrEndpoints - GET /status.json", () => {

  test("returns one entry per tuner slot when no streams are active (all idle)", async () => {

    const res = await fetch(urlFor("/status.json"));
    const body = await res.json() as Record<string, unknown>[];

    assert.equal(res.status, 200);
    assert.equal(body.length, CONFIG.streaming.maxConcurrentStreams, "tuner count matches the configured limit");

    for(const [ i, tuner ] of body.entries()) {

      assert.equal(tuner["Resource"], "tuner" + String(i), "idle tuner labelled tuner" + String(i));
      assert.deepEqual(Object.keys(tuner).sort(), ["Resource"], "idle tuner has only Resource");
    }
  });

  test("active stream produces a tuner entry with channel info populated from the channel map", async () => {

    // Pick the first key from the predefined map so the tuner entry hydrates from buildChannelMap. We emit a deterministic id (1_000_001) to avoid colliding with
    // any concurrent test stream registration; the registry keys by id.
    const all = await import("../config/userChannels.ts");
    const channels = all.getAllChannels();
    const firstKey = firstOf(Object.keys(channels), "predefined channel key");

    const entry = makeRegistryEntry({ id: 1_000_001, info: { lastPlaylistRequest: 0, storeKey: firstKey } });

    registerStream(entry);

    try {

      const res = await fetch(urlFor("/status.json"));
      const body = await res.json() as Record<string, unknown>[];
      // The first slot is filled by the active stream because streams sort by ascending id; our test id is the only registered one.
      const active = body[0]!;

      assert.equal(active["Resource"], "tuner0");
      assert.equal(active["Frequency"], 0);
      assert.equal(active["SignalQualityPercent"], 100);
      assert.equal(active["SignalStrengthPercent"], 100);
      assert.equal(active["SymbolQualityPercent"], 100);
      assert.equal(typeof active["VctNumber"], "string", "VctNumber comes from the channel map");
      assert.equal(typeof active["VctName"], "string", "VctName comes from the channel map");
    } finally {

      unregisterStream(entry.id);
    }
  });

  test("falls back to stream.channelName for VctName when the channel is missing from the map", async () => {

    // A stream whose storeKey is not in the channel map (e.g., the channel was removed from config after the stream started) should still surface a VctName via
    // the channelName fallback. VctNumber is not produced because the channel has no number to report.
    const entry = makeRegistryEntry({

      channelName: "Removed Channel",
      id: 1_000_002,
      info: { lastPlaylistRequest: 0, storeKey: "definitely-not-a-real-channel-key" }
    });

    registerStream(entry);

    try {

      const res = await fetch(urlFor("/status.json"));
      const body = await res.json() as Record<string, unknown>[];
      const active = body[0]!;

      assert.equal(active["VctName"], "Removed Channel", "fallback name surfaces");
      assert.equal(active["VctNumber"], undefined, "no VctNumber when channel missing from map");
    } finally {

      unregisterStream(entry.id);
    }
  });

  test("normalizes clientAddress for TargetIP across the three documented cases", async () => {

    // Three sub-cases collapsed into one test: IPv6-mapped IPv4 strips the ::ffff: prefix; plain IPv4 passes through; null produces no TargetIP.
    const cases = [
      { address: "::ffff:192.168.1.42", expected: "192.168.1.42", id: 1_000_003 },
      { address: "10.0.0.1", expected: "10.0.0.1", id: 1_000_004 },
      { address: null, expected: undefined, id: 1_000_005 }
    ] as const;

    for(const c of cases) {

      const entry = makeRegistryEntry({

        clientAddress: c.address,
        id: c.id,
        info: { lastPlaylistRequest: 0, storeKey: "definitely-not-a-real-channel-key" }
      });

      registerStream(entry);

      try {

        // eslint-disable-next-line no-await-in-loop -- sequential because each case mutates the shared registry; parallel runs would race.
        const res = await fetch(urlFor("/status.json"));
        // eslint-disable-next-line no-await-in-loop -- second await of the same sequential round-trip.
        const body = await res.json() as Record<string, unknown>[];

        assert.equal(body[0]!["TargetIP"], c.expected, "case for " + String(c.address));
      } finally {

        unregisterStream(entry.id);
      }
    }
  });

  test("fills remaining slots with idle entries after the active stream", async () => {

    const entry = makeRegistryEntry({ id: 1_000_006, info: { lastPlaylistRequest: 0, storeKey: "definitely-not-a-real-channel-key" } });

    registerStream(entry);

    try {

      const res = await fetch(urlFor("/status.json"));
      const body = await res.json() as Record<string, unknown>[];

      // Slots 1..N-1 must be idle entries; only slot 0 carries the active stream.
      for(let i = 1; i < CONFIG.streaming.maxConcurrentStreams; i++) {

        assert.deepEqual(Object.keys(body[i]!).sort(), ["Resource"], "slot " + String(i) + " is idle");
      }

      assert.equal(body.length, CONFIG.streaming.maxConcurrentStreams);
    } finally {

      unregisterStream(entry.id);
    }
  });
});
