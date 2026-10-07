/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * discover.test.ts: Integration coverage for the HDHomeRun emulation endpoints. Plex's DVR setup auto-discovers PrismCast as a virtual HDHomeRun tuner via
 * three endpoints: discover.json (device identity), lineup.json (channel-number to stream-URL map), lineup_status.json (scan status). Each must produce
 * spec-compatible output for Plex to register PrismCast as a tuner; the bug class this suite catches is "field rename or shape change that silently breaks
 * Plex discovery without breaking unit tests."
 *
 * The HDHR endpoints register on a separate Express instance in production (run on a dedicated port to avoid colliding with the main server's HTTP traffic). We mount
 * the same setupHdhrEndpoints function on a dedicated test instance here so the route handlers run with the same wiring they have in production, the provider
 * of the instance's own bound port included.
 */
import type { AddressInfo, Server } from "node:net";
import { CONFIG, initializeConfiguration } from "../../../src/config/index.ts";
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { setupHdhrEndpoints } from "../../../src/hdhr/discover.ts";

/**
 * Boots a separate Express instance for the HDHR endpoints. Returns the port the instance is bound to and an HDHR-specific URL composer, and registers cleanup
 * with the integration context.
 */
async function bootHdhr(ctx: { registerCleanup: (fn: () => Promise<void>) => void }): Promise<{ port: number; urlFor: (path: string) => string }> {

  const app = express();
  let boundPort = 0;

  app.set("trust proxy", true);

  // The endpoints read the port this instance is bound to through the provider, set from the server's own address once it listens, as the HDHomeRun surface
  // does in production.
  setupHdhrEndpoints(app, () => boundPort);

  const server: Server = await new Promise((resolve, reject) => {

    const s = app.listen(0, "127.0.0.1", () => { resolve(s); });

    s.on("error", reject);
  });

  boundPort = (server.address() as AddressInfo).port;

  ctx.registerCleanup(async () => {

    await new Promise<void>((resolve) => { server.close(() => { resolve(); }); });
  });

  return { port: boundPort, urlFor: (subPath: string): string => "http://127.0.0.1:" + String(boundPort) + subPath };
}

describe("HDHR emulation endpoints", () => {

  test("/discover.json returns the documented device-identity shape", async () => {

    /* Plex reads discover.json fields by name; a rename of any of these would silently break tuner registration. We lock the field set here so a refactor
     * surfaces immediately. The values themselves can vary (DeviceID is auto-generated per install), so we assert presence + type, not exact values.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootHdhr(ctx);

    const response = await fetch(urlFor("/discover.json"));

    assert.equal(response.status, 200);

    const body = await response.json() as Record<string, unknown>;

    /* Required fields per the HDHomeRun spec - omission of any of these breaks Plex discovery. The loop below asserts presence for every field; type is
     * spot-checked separately for TunerCount (a number) and LineupURL (matched against the lineup endpoint).
     */
    for(const field of [ "FriendlyName", "ModelNumber", "FirmwareName", "FirmwareVersion", "DeviceID", "DeviceAuth", "BaseURL", "LineupURL" ] as const) {

      assert.ok(field in body, "discover.json should carry " + field);
    }

    assert.equal(typeof body["TunerCount"], "number", "TunerCount is a number");
    assert.match(body["LineupURL"] as string, /\/lineup\.json$/, "LineupURL points at the lineup endpoint");
  });

  test("/lineup.json returns one entry per channel with the documented shape", async () => {

    /* Plex reads each entry's GuideNumber, GuideName, URL fields. We seed a user channelNumber so the lineup carries a deterministic GuideNumber and assert
     * the entry for our seeded channel has the right shape.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7 };
    });

    const { urlFor } = await bootHdhr(ctx);

    const response = await fetch(urlFor("/lineup.json"));

    assert.equal(response.status, 200);

    const lineup = await response.json() as { GuideName: string; GuideNumber: string; URL: string }[];

    assert.ok(Array.isArray(lineup), "lineup is an array");

    const abcEntry = lineup.find((e) => e.GuideName === "ABC");

    assert.ok(abcEntry, "abc should appear in the lineup");
    assert.equal(abcEntry.GuideNumber, "7", "GuideNumber reflects the user-set channelNumber as a string");
    assert.match(abcEntry.URL, /\/stream\/abc/, "URL points at the MPEG-TS stream endpoint for abc");
  });

  test("/lineup_status.json reports a static scan-complete state", async () => {

    /* PrismCast's lineup is config-driven (no scan happens), so the response is a fixed shape that always reports "complete." Plex polls this during initial
     * setup; the field set must match the HDHR spec.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootHdhr(ctx);

    const response = await fetch(urlFor("/lineup_status.json"));

    assert.equal(response.status, 200);

    const body = await response.json() as Record<string, unknown>;

    assert.ok("ScanInProgress" in body, "ScanInProgress field present");
    assert.ok("ScanPossible" in body, "ScanPossible field present");
    assert.ok("Source" in body, "Source field present");
  });

  test("/discover.json and /device.xml advertise the port the serving instance is bound to, whatever port CONFIG names", async () => {

    /* A client follows the BaseURL a document advertises, so the URL has to name the port the answering instance is bound to. The configured port is held apart
     * from the bound one as the negative control, and the row reads its configuration from its own data directory so it inherits nothing from an earlier row.
     */
    await using ctx = await createIntegrationContext();

    await initializeConfiguration();
    await initializePersistence(ctx);

    const { port, urlFor } = await bootHdhr(ctx);
    const original = CONFIG.hdhr.port;

    CONFIG.hdhr.port = port + 1;

    try {

      const discover = await (await fetch(urlFor("/discover.json"))).json() as Record<string, unknown>;
      const deviceXml = await (await fetch(urlFor("/device.xml"))).text();

      assert.equal(discover["BaseURL"], "http://127.0.0.1:" + String(port), "discover.json advertises the bound port");
      assert.equal(discover["LineupURL"], "http://127.0.0.1:" + String(port) + "/lineup.json", "the lineup URL names the bound port");
      assert.match(deviceXml, new RegExp("<URLBase>http://127\\.0\\.0\\.1:" + String(port) + "</URLBase>"), "device.xml advertises the bound port");
    } finally {

      CONFIG.hdhr.port = original;
    }
  });
});
