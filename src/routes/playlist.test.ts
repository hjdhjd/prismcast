/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * playlist.test.ts: Unit tests for the M3U playlist routes in playlist.ts. The module exports two pure helpers (resolveBaseUrl, generatePlaylistContent) and
 * one route registrar (setupPlaylistEndpoint). resolveBaseUrl extracts the public-facing URL from forwarded headers; generatePlaylistContent emits an EXTM3U
 * blob from the merged channel map; the route validates filter and sort query parameters and dispatches to the helpers. The validation paths and edge cases
 * are exercised end-to-end against an Express server, while the helpers are tested directly.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import { generatePlaylistContent, resolveBaseUrl } from "./playlist.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { makeReqRes } from "./express.helpers.ts";
import { setupPlaylistEndpoint } from "./playlist.ts";

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  // Trust proxy enables Express's X-Forwarded-* awareness used by req.protocol; the playlist endpoint depends on it.
  app.set("trust proxy", true);
  setupPlaylistEndpoint(app);

  return new Promise((resolve, reject) => {

    const server = app.listen(0, "127.0.0.1", () => {

      const address = server.address() as AddressInfo;

      resolve({ port: address.port, server });
    });

    server.on("error", reject);
  });
}

function closeServer(server: Server): Promise<void> {

  return new Promise((resolve) => {

    server.close(() => {

      resolve();
    });
  });
}

let sharedServer: Server;
let sharedPort = 0;

function urlFor(p: string): string {

  return "http://127.0.0.1:" + String(sharedPort) + p;
}

before(async () => {

  const created = await makeServer();

  sharedServer = created.server;
  sharedPort = created.port;
});

after(async () => {

  await closeServer(sharedServer);
  await closePuppeteerStreamWss();
});

describe("resolveBaseUrl", () => {

  test("uses the Host header when no X-Forwarded-Host is set", () => {

    const { req } = makeReqRes({ headers: { host: "example.test:5589" }, protocol: "http" });

    assert.equal(resolveBaseUrl(req), "http://example.test:5589");
  });

  test("prefers X-Forwarded-Host over the Host header (proxy compatibility)", () => {

    const { req } = makeReqRes({

      headers: { host: "internal:5000", "x-forwarded-host": "public.example.test" },
      protocol: "https"
    });

    assert.equal(resolveBaseUrl(req), "https://public.example.test");
  });

  test("takes the first host when X-Forwarded-Host contains a comma-separated list", () => {

    // Boundary: layered proxies append host entries with commas. The handler takes the first entry (the original client-facing host) and trims surrounding
    // whitespace.
    const { req } = makeReqRes({

      headers: { host: "internal:5000", "x-forwarded-host": "public.example.test, intermediate.test, edge.test" },
      protocol: "https"
    });

    assert.equal(resolveBaseUrl(req), "https://public.example.test");
  });

  test("trims whitespace around the X-Forwarded-Host first entry", () => {

    const { req } = makeReqRes({

      headers: { "x-forwarded-host": "  spaced.example.test  ,other.test" },
      protocol: "http"
    });

    assert.equal(resolveBaseUrl(req), "http://spaced.example.test");
  });

  test("respects the protocol from req.protocol (Express trusts X-Forwarded-Proto when configured)", () => {

    const { req: httpReq } = makeReqRes({ headers: { host: "h" }, protocol: "http" });
    const { req: httpsReq } = makeReqRes({ headers: { host: "h" }, protocol: "https" });

    assert.equal(resolveBaseUrl(httpReq), "http://h");
    assert.equal(resolveBaseUrl(httpsReq), "https://h");
  });

  test("falls back to the configured server host:port when no Host headers are present (boundary)", () => {

    // Boundary: the handler falls back to CONFIG.server.host + ":" + CONFIG.server.port. We don't pin to specific values; we just lock that the result has the
    // expected protocol prefix and is a non-empty origin string.
    const { req } = makeReqRes({ protocol: "http" });
    const url = resolveBaseUrl(req);

    assert.match(url, /^http:\/\/.+/);
  });

  test("falls back to the configured host when X-Forwarded-Host is present but whitespace-only (no empty-host URL)", () => {

    // Regression: a present-but-empty X-Forwarded-Host trims its first entry to "", which is non-nullish and would slip past ??, yielding a hostless
    // "http:///hls/..." URL. The truthiness guard collapses the empty case onto the configured server settings, so the origin is never empty.
    const { req } = makeReqRes({ headers: { host: "internal:5000", "x-forwarded-host": "   " }, protocol: "http" });
    const url = resolveBaseUrl(req);

    assert.doesNotMatch(url, /^http:\/\/\//, "an empty forwarded host must not produce a hostless URL");
    assert.match(url, /^http:\/\/.+/, "the resolved URL must carry a non-empty host");
  });

  test("falls back to the configured host when X-Forwarded-Host first entry is empty (leading comma)", () => {

    // Boundary: a leading comma makes the first comma-separated entry the empty string. Trimming leaves "", which must trigger the fallback rather than emit an
    // empty host.
    const { req } = makeReqRes({ headers: { host: "internal:5000", "x-forwarded-host": ",public.example.test" }, protocol: "https" });
    const url = resolveBaseUrl(req);

    assert.doesNotMatch(url, /^https:\/\/\//, "a leading-comma forwarded host must not produce a hostless URL");
    assert.match(url, /^https:\/\/.+/, "the resolved URL must carry a non-empty host");
  });
});

describe("generatePlaylistContent", () => {

  test("starts with the M3U header", () => {

    const playlist = generatePlaylistContent("http://example.test");

    assert.match(playlist, /^#EXTM3U/);
  });

  test("includes one #EXTINF line per included channel", () => {

    // Boundary: every channel produces an EXTINF line followed by a stream URL line. The number of EXTINF lines equals the number of channels in the merged
    // map (subject to filtering, which we don't apply here).
    const playlist = generatePlaylistContent("http://example.test");
    const extinfCount = (playlist.match(/^#EXTINF:/gm) ?? []).length;
    const urlCount = (playlist.match(/^http:\/\/example\.test\/hls\//gm) ?? []).length;

    assert.equal(extinfCount, urlCount, "every EXTINF line should be paired with one URL line");
  });

  test("emits stream URLs under /hls/<key>/stream.m3u8", () => {

    const playlist = generatePlaylistContent("http://example.test:5589");

    // Sample any URL line and verify it matches the documented /hls/<key>/stream.m3u8 shape.
    const urlLines = playlist.split("\n").filter((line) => line.startsWith("http"));

    assert.ok(urlLines.length > 0, "expected at least one URL line");

    for(const url of urlLines) {

      assert.match(url, /^http:\/\/example\.test:5589\/hls\/.+\/stream\.m3u8$/);
    }
  });

  test("uses the supplied baseUrl as the URL prefix", () => {

    const a = generatePlaylistContent("http://a.test");
    const b = generatePlaylistContent("https://b.test:9000");

    assert.match(a, /\nhttp:\/\/a\.test\/hls\//);
    assert.match(b, /\nhttps:\/\/b\.test:9000\/hls\//);
  });

  test("excludes all channels when service filter is in include mode and tags do not match (negative test)", () => {

    // The merged channel map contains channels with various services. A filter for a service tag that no channel uses must produce a playlist with the EXTM3U
    // header but no EXTINF lines.
    const playlist = generatePlaylistContent("http://e.test", { exclude: false, tags: ["totally-not-a-real-service-x9"] });
    const extinfCount = (playlist.match(/^#EXTINF:/gm) ?? []).length;

    assert.equal(extinfCount, 0, "no channels should match an unknown service tag");
  });

  test("excludes nothing when exclude filter is empty (locks the no-match=keep-all branch)", () => {

    // Boundary: an exclude filter for an unused service tag means no channels match the exclude list, so all channels remain.
    const allPlaylist = generatePlaylistContent("http://e.test");
    const filteredPlaylist = generatePlaylistContent("http://e.test", { exclude: true, tags: ["totally-not-a-real-service-x9"] });

    const allCount = (allPlaylist.match(/^#EXTINF:/gm) ?? []).length;
    const filteredCount = (filteredPlaylist.match(/^#EXTINF:/gm) ?? []).length;

    assert.equal(allCount, filteredCount, "exclude filter for nonexistent tag should keep all channels");
  });
});

describe("setupPlaylistEndpoint - GET /playlist (validation paths)", () => {

  test("returns 200 with audio/x-mpegurl content type for a clean request", async () => {

    const res = await fetch(urlFor("/playlist"));

    assert.equal(res.status, 200);
    assert.match(res.headers.get("content-type") ?? "", /audio\/x-mpegurl/);

    const body = await res.text();

    assert.match(body, /^#EXTM3U/);
  });

  test("returns 400 when service filter mixes include and exclude tokens", async () => {

    // Negative test: the parser rejects a comma-separated list that mixes include and exclude. The error message should explain the constraint.
    const res = await fetch(urlFor("/playlist?service=hulu,-spectrum"));

    assert.equal(res.status, 400);

    const body = await res.json() as { error: string };

    assert.match(body.error, /Cannot mix include and exclude/);
  });

  test("returns 400 when service filter contains an unknown tag (locks the validation message and validTags hint)", async () => {

    const res = await fetch(urlFor("/playlist?service=totally-not-a-real-service"));

    assert.equal(res.status, 400);

    const body = await res.json() as { error: string; validTags: string[] };

    assert.match(body.error, /Unknown service tag/);
    assert.ok(Array.isArray(body.validTags), "validTags should be an array of accepted values");
  });

  test("returns 400 when tag filter contains an unknown tag", async () => {

    const res = await fetch(urlFor("/playlist?tag=totally-not-a-real-tag"));

    assert.equal(res.status, 400);

    const body = await res.json() as { error: string };

    assert.match(body.error, /Unknown tag/);
  });

  test("returns 400 when service filter is empty or whitespace-only (boundary)", async () => {

    // Boundary: the parser counts zero non-empty tokens for an empty or whitespace-only string and returns "Empty service tag filter."
    const res = await fetch(urlFor("/playlist?service=,,, ,"));

    assert.equal(res.status, 400);

    const body = await res.json() as { error: string };

    assert.match(body.error, /Empty service tag filter/);
  });

  test("returns 400 when sort field is not in VALID_SORT_FIELDS", async () => {

    const res = await fetch(urlFor("/playlist?sort=bogusField"));

    assert.equal(res.status, 400);

    const body = await res.json() as { error: string; validFields: string[] };

    assert.match(body.error, /Invalid sort field: bogusField/);
    assert.ok(Array.isArray(body.validFields), "validFields should list the accepted sort fields");
  });

  test("returns 400 when sort direction is neither asc nor desc", async () => {

    const res = await fetch(urlFor("/playlist?direction=sideways"));

    assert.equal(res.status, 400);

    const body = await res.json() as { error: string; validDirections: string[] };

    assert.match(body.error, /Invalid sort direction: sideways/);
    assert.deepEqual(body.validDirections, [ "asc", "desc" ], "valid directions should be exactly asc and desc");
  });

  test("accepts a valid sort field and direction", async () => {

    // Boundary: valid sort=name + direction=asc should produce a 200 with playlist content.
    const res = await fetch(urlFor("/playlist?sort=name&direction=asc"));

    assert.equal(res.status, 200);
    const body = await res.text();

    assert.match(body, /^#EXTM3U/);
  });

  test("accepts mixed-case direction (lowercased before validation)", async () => {

    // Boundary: the handler trims and lowercases direction before checking. ASC and DESC both work.
    const ascRes = await fetch(urlFor("/playlist?direction=ASC"));
    const descRes = await fetch(urlFor("/playlist?direction=DESC"));

    assert.equal(ascRes.status, 200);
    assert.equal(descRes.status, 200);
    await ascRes.text();
    await descRes.text();
  });

  test("dynamically constructs URLs from the request host", async () => {

    // The handler runs resolveBaseUrl against the test server's actual host. The URLs in the body must include the bound port.
    const res = await fetch(urlFor("/playlist"));
    const body = await res.text();
    const expectedHost = "127.0.0.1:" + String(sharedPort);

    assert.match(body, new RegExp("/hls/[^/]+/stream\\.m3u8"), "should include /hls/<key>/stream.m3u8 paths");

    // Sample one URL line to confirm host:port match.
    const urlLines = body.split("\n").filter((line) => line.startsWith("http"));

    if(urlLines.length > 0) {

      assert.match(urlLines[0]!, new RegExp("^http://" + expectedHost + "/hls/"));
    }
  });
});
