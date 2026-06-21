/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * playlist-generation.test.ts: HTTP-level integration coverage for the M3U playlist endpoint. The unit tier (routes/playlist.test.ts) covers the playlist
 * generators and validation paths against a synthetic server with default predefined channels. This suite tests the live interaction with mutated channel
 * state - a user-set channelNumber surfacing in the playlist, the request-derived host prefixing the stream URLs, and the canonical M3U structure (paired
 * EXTINF and URL lines) - confirming the runtime composition behaves as documented.
 *
 * Why integration coverage adds value: the playlist endpoint reads CONFIG.channels.disabledPredefined, the tag registry, the resolved channel map, and applies
 * filter/sort transformations. A regression where any of those layers stops being read correctly would still pass the unit tier but produce a wrong playlist
 * for users. This suite stresses the cross-layer composition.
 */
import { bootApp, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";

describe("GET /playlist - integration with mutated state", () => {

  test("a user-set channelNumber on a canonical surfaces in the M3U playlist", async () => {

    /* The user sets channelNumber=7 on abc; the playlist's EXTINF line for abc must carry channel-number="7". The wire format is M3U; we assert
     * via substring that the number appears in the playlist body.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7 };
    });

    const response = await fetch(urlFor("/playlist"));

    assert.equal(response.status, 200);

    const body = await response.text();

    /* The user's channelNumber should appear in the abc EXTINF line. The M3U attribute key is channel-number per Channels DVR's M3U conventions. We scope the
     * assertion to the abc line so a channel-number on a different channel doesn't accidentally pass.
     */
    const abcLine = body.split("\n").find((l) => l.includes("ABC") && l.startsWith("#EXTINF"));

    assert.ok(abcLine, "the abc EXTINF line should be present");
    assert.match(abcLine, /channel-number="7"/, "the user-set channelNumber 7 should surface in the EXTINF line");
  });

  test("the playlist URL prefix uses the request-derived host (not a hardcoded value)", async () => {

    /* resolveBaseUrl in playlist.ts derives the URL prefix from req.protocol + Host header (or X-Forwarded-Host). Listening on an ephemeral port, the playlist
     * URLs should reference 127.0.0.1:<port> matching urlFor's composition. A regression that hardcoded the host (or read from a stale config) would surface
     * here as URL prefix mismatch.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { port, urlFor } = await bootApp(ctx);

    const body = await (await fetch(urlFor("/playlist"))).text();
    const expectedPrefix = "http://127.0.0.1:" + String(port) + "/hls/";

    assert.match(body, new RegExp(expectedPrefix.replace(/\./g, "\\.")), "playlist URLs should reference the actual listener address");
  });

  test("the playlist passes the canonical M3U structure (EXTM3U header, paired EXTINF and URL lines)", async () => {

    /* Structural integrity: the playlist must start with #EXTM3U and have one URL line per #EXTINF line. A regression that produces unpaired lines would
     * break clients that consume the M3U strictly.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const body = await (await fetch(urlFor("/playlist"))).text();

    assert.match(body, /^#EXTM3U/, "playlist starts with EXTM3U header");

    const extinfCount = (body.match(/^#EXTINF:/gm) ?? []).length;
    const urlCount = (body.match(/^http/gm) ?? []).length;

    assert.equal(urlCount, extinfCount, "every EXTINF line must be paired with one URL line");
    assert.ok(extinfCount > 0, "at least one channel should be in the playlist (predefined defaults are non-empty)");
  });
});
