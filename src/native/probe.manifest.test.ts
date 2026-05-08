/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * probe.manifest.test.ts: Unit tests for probeManifest, the master-manifest probe orchestrator in probe.ts. The orchestrator fetches a master manifest, picks the
 * highest-bandwidth variant, fetches the variant manifest, and classifies the encryption type by parsing #EXT-X-KEY tags. This file isolates the orchestrator
 * tests from the cache and resolveUrl tests in probe.test.ts so each file stays under the per-file line guideline. The tests substitute globalThis.fetch with
 * mock.method to feed synthetic master/variant/key responses without ever touching the network.
 */
/* eslint-disable sort-keys -- fixture route maps are ordered by HLS resolution chain (master -> variant -> key), not alphabetical key strings, so the logical
 * dependency direction is visible to readers. */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { clearProbeCache, getCachedEncryption, probeManifest } from "./probe.ts";
import assert from "node:assert/strict";

/* makeFetchRouter installs a mock for globalThis.fetch that dispatches to URL-keyed responses. Tests register their fixtures keyed by URL prefix; any request to
 * an unregistered URL returns a 404. This keeps each test focused on the single classification branch it exercises - master returns variant URL, variant returns
 * encryption tag, key URL returns 16 bytes, etc. - without juggling a fragile call-counter pattern.
 */
type FetchHandler = (url: string) => Response | Promise<Response>;

function makeFetchRouter(routes: Record<string, FetchHandler>): void {

  mock.method(globalThis, "fetch", async (url: string | URL): Promise<Response> => {

    const urlStr = url.toString();
    let matched: FetchHandler | null = null;
    let matchedLength = -1;

    for(const prefix of Object.keys(routes)) {

      if(urlStr.startsWith(prefix) && (prefix.length > matchedLength)) {

        matched = routes[prefix] ?? null;
        matchedLength = prefix.length;
      }
    }

    if(!matched) {

      return new Response("not found", { status: 404 });
    }

    return matched(urlStr);
  });
}

describe("probeManifest", () => {

  beforeEach(() => {

    clearProbeCache("probe-channel");
  });

  afterEach(() => {

    mock.reset();
  });

  test("classifies a manifest with no #EXT-X-KEY tag as 'clear'", async () => {

    // Happy path: a manifest with #EXTM3U, #EXT-X-STREAM-INF, and segment URLs but no key tags. This is unencrypted HLS - the classifier must return "clear" so
    // the proxy can pass segments through verbatim.
    const masterUrl = "https://cdn.test/clear-master.m3u8";
    const variantUrl = "https://cdn.test/clear-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1280x720,CODECS=\"avc1.640028,mp4a.40.2\"",
        "clear-variant.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [variantUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-TARGETDURATION:6",
        "#EXTINF:6.0,",
        "seg0.ts",
        ""
      ].join("\n"), { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "probe resolved with a result");
    assert.equal(result.encryption, "clear", "clear classification");
    assert.equal(result.bandwidth, 2000000, "bandwidth parsed from BANDWIDTH attribute");
    assert.equal(result.codec, "H264", "avc1 prefix mapped to H264 label");
    assert.equal(result.resolution, "1280x720", "resolution parsed");
    assert.equal(result.keyUrl, null, "no key URL for clear streams");
    assert.equal(result.audioVariantUrl, null, "no separate audio rendition");
  });

  test("classifies a manifest with #EXT-X-KEY METHOD=NONE as 'clear'", async () => {

    // Boundary: explicit NONE method is functionally identical to no key tag. Locks the spec contract.
    const masterUrl = "https://cdn.test/none-master.m3u8";
    const variantUrl = "https://cdn.test/none-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=500000\nnone-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=NONE\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "probe resolved");
    assert.equal(result.encryption, "clear", "METHOD=NONE classified as clear");
  });

  test("classifies a manifest with AES-128 and an accessible 16-byte key as 'aes128'", async () => {

    // Happy path: the variant has METHOD=AES-128 and a key URI pointing to a 16-byte body. The classifier returns "aes128" with the resolved key URL so the
    // proxy can fetch and decrypt segments.
    const masterUrl = "https://cdn.test/aes-master.m3u8";
    const variantUrl = "https://cdn.test/aes-variant.m3u8";
    const keyUrl = "https://cdn.test/aes-key.bin";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=3000000\naes-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyUrl + "\"\n#EXTINF:6,\nseg.ts\n", { status: 200 }),
      [keyUrl]: () => new Response(Buffer.alloc(16), { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "probe resolved");
    assert.equal(result.encryption, "aes128", "AES-128 with 16-byte key classified as aes128");
    assert.equal(result.keyUrl, keyUrl, "resolved key URL surfaces in the result");
  });

  test("classifies AES-128 with an inaccessible key (HTTP 403) as 'drm'", async () => {

    // Negative test: an AES-128 declaration without a fetchable key cannot be decrypted in Node, so the classifier downgrades to DRM. Important because a 403
    // on the key indicates a real auth issue - we must fall back to capture rather than attempt decryption.
    const masterUrl = "https://cdn.test/aes403-master.m3u8";
    const variantUrl = "https://cdn.test/aes403-variant.m3u8";
    const keyUrl = "https://cdn.test/aes403-key.bin";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\naes403-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyUrl + "\"\n", { status: 200 }),
      [keyUrl]: () => new Response("forbidden", { status: 403 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "probe resolved");
    assert.equal(result.encryption, "drm", "inaccessible key downgrades to DRM");
  });

  test("classifies AES-128 with a wrong-size key (not 16 bytes) as 'drm'", async () => {

    // Boundary: a 200 OK with a 24-byte key is still wrong - HLS AES-128 requires exactly 16 bytes. The classifier must reject and downgrade.
    const masterUrl = "https://cdn.test/aes24-master.m3u8";
    const variantUrl = "https://cdn.test/aes24-variant.m3u8";
    const keyUrl = "https://cdn.test/aes24-key.bin";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\naes24-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyUrl + "\"\n", { status: 200 }),
      [keyUrl]: () => new Response(Buffer.alloc(24), { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.encryption, "drm", "24-byte key downgrades to DRM");
  });

  test("classifies SAMPLE-AES (Widevine) as 'drm'", async () => {

    // SAMPLE-AES requires a CDM that PrismCast does not implement. The classifier must mark it DRM so the caller falls back to capture.
    const masterUrl = "https://cdn.test/wv-master.m3u8";
    const variantUrl = "https://cdn.test/wv-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nwv-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://wv\"\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.encryption, "drm", "SAMPLE-AES classified as DRM");
  });

  test("classifies SAMPLE-AES-CTR as 'drm'", async () => {

    // FairPlay uses SAMPLE-AES-CTR (counter mode). Same DRM downgrade.
    const masterUrl = "https://cdn.test/fp-master.m3u8";
    const variantUrl = "https://cdn.test/fp-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nfp-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES-CTR,URI=\"skd://fp\"\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.encryption, "drm", "SAMPLE-AES-CTR classified as DRM");
  });

  test("classifies AES-128 with no URI attribute as 'drm'", async () => {

    // Negative test: a malformed AES-128 tag with no URI cannot be decrypted - we have no key URL to fetch. Downgrade to DRM.
    const masterUrl = "https://cdn.test/nokey-master.m3u8";
    const variantUrl = "https://cdn.test/nokey-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nnokey-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.encryption, "drm", "AES-128 with missing URI downgrades to DRM");
  });

  test("selects the highest-bandwidth variant when multiple #EXT-X-STREAM-INF entries are present", async () => {

    // The selector picks the largest bandwidth value across all variants. We construct three variants and assert the 4Mbps one wins. This covers the variant
    // selection branch independent of encryption.
    const masterUrl = "https://cdn.test/multi-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=640x360",
        "low.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1920x1080",
        "high.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720",
        "mid.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      "https://cdn.test/high.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "probe resolved");
    assert.equal(result.bandwidth, 4000000, "highest bandwidth variant chosen");
    assert.equal(result.resolution, "1920x1080", "resolution from the chosen variant");
    assert.equal(result.bestVariantUrl, "https://cdn.test/high.m3u8", "URL from the chosen variant");
  });

  test("returns null when the master manifest fetch fails (HTTP 500)", async () => {

    // Negative test: a network-side master fetch failure must surface as null so the caller falls back to capture rather than retrying indefinitely.
    const masterUrl = "https://cdn.test/fail-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("server error", { status: 500 })
    });

    assert.equal(await probeManifest(masterUrl, "probe-channel"), null, "master fetch failure returns null");
  });

  test("returns null when the master manifest contains no variant streams", async () => {

    // Boundary: an empty playlist has no #EXT-X-STREAM-INF entries. The selector returns null and the probe surfaces null so the caller falls back to capture.
    const masterUrl = "https://cdn.test/empty-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-VERSION:3\n", { status: 200 })
    });

    assert.equal(await probeManifest(masterUrl, "probe-channel"), null, "no variants returns null");
  });

  test("returns null when the variant manifest fetch fails", async () => {

    // Negative test: master fetch succeeds, variant fetch fails. The probe must surface null rather than blindly classifying as clear.
    const masterUrl = "https://cdn.test/master-only.m3u8";
    const variantUrl = "https://cdn.test/missing-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nmissing-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("not found", { status: 404 })
    });

    assert.equal(await probeManifest(masterUrl, "probe-channel"), null, "variant fetch failure returns null");
  });

  test("short-circuits with a synthetic DRM result when the cache holds 'drm'", async () => {

    // The cache short-circuit is the optimization that lets DRM channels skip the master/variant fetch on subsequent tunes. We pre-populate the cache by running
    // a real DRM probe, then clear the fetch mock and run a second probe - the second call must return a DRM result without any HTTP traffic.
    const masterUrl = "https://cdn.test/cache-master.m3u8";
    const variantUrl = "https://cdn.test/cache-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\ncache-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://x\"\n", { status: 200 })
    });

    await probeManifest(masterUrl, "probe-channel");
    assert.equal(getCachedEncryption("probe-channel"), "drm", "first probe cached DRM");

    // Reset the mock - if the function re-fetches, the request will fail.
    mock.reset();

    let fetchCalls = 0;

    mock.method(globalThis, "fetch", async () => {

      fetchCalls++;

      return new Response("should not be reached", { status: 500 });
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(fetchCalls, 0, "no fetch calls for cached DRM (short-circuit fires)");
    assert.ok(result, "cached DRM probe still resolves to a non-null result");
    assert.equal(result.encryption, "drm", "result mirrors the cached classification");
    assert.equal(result.bestVariantUrl, "", "synthetic DRM result has empty variant URL");
  });

  test("does NOT short-circuit on cached 'clear' (must re-probe for fresh tokens)", async () => {

    // Boundary: the cache short-circuits for DRM only - clear and aes128 require fresh tokens on each probe because variant URLs and key URLs carry session-bound
    // auth tokens. We pre-populate clear via a real probe, then re-probe with new fixtures and verify the second probe re-fetches.
    const masterUrl = "https://cdn.test/reprobe-master.m3u8";
    const variantUrl = "https://cdn.test/reprobe-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nreprobe-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    await probeManifest(masterUrl, "probe-channel");
    assert.equal(getCachedEncryption("probe-channel"), "clear", "first probe cached clear");

    // Re-fixture: the second probe must observe a real fetch.
    mock.reset();

    let fetchCalls = 0;

    makeFetchRouter({

      [masterUrl]: () => {

        fetchCalls++;

        return new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nreprobe-variant.m3u8\n", { status: 200 });
      },
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    await probeManifest(masterUrl, "probe-channel");

    assert.ok(fetchCalls > 0, "clear cache does not short-circuit; master is re-fetched");
  });

  test("includes the audio rendition URL when the master declares #EXT-X-MEDIA:TYPE=AUDIO with URI", async () => {

    // Channels with separate audio (e.g., Google DAI) have a #EXT-X-MEDIA:TYPE=AUDIO,URI="..." line in the master. The probe must surface the URL so the proxy
    // can poll the audio variant alongside the video variant.
    const masterUrl = "https://cdn.test/dai-master.m3u8";
    const videoVariantUrl = "https://cdn.test/dai-video.m3u8";
    const audioVariantUrl = "https://cdn.test/dai-audio.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"English\",URI=\"" + audioVariantUrl + "\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"audio\"",
        "dai-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoVariantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.audioVariantUrl, audioVariantUrl, "audio rendition URL surfaces from #EXT-X-MEDIA");
  });

  test("returns null audio rendition when #EXT-X-MEDIA:TYPE=AUDIO has no URI (muxed audio)", async () => {

    // Boundary: descriptive-only #EXT-X-MEDIA tags (no URI) indicate muxed audio. The probe must report null, not the URL of a non-existent rendition.
    const masterUrl = "https://cdn.test/muxed-master.m3u8";
    const videoVariantUrl = "https://cdn.test/muxed-video.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"English\",DEFAULT=YES",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"audio\"",
        "muxed-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoVariantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.audioVariantUrl, null, "no URI -> null audio rendition");
  });

  test("maps avc1 codec prefix to 'H264' label", async () => {

    // Lock the codec mapping table - if the prefix table changes silently, downstream UI labels would shift. Test one prefix per family.
    const masterUrl = "https://cdn.test/avc-master.m3u8";
    const variantUrl = "https://cdn.test/avc-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=1000000,CODECS=\"avc1.640028,mp4a.40.2\"",
        "avc-variant.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.codec, "H264", "avc1 -> H264");
  });

  test("maps hvc1 codec prefix to 'HEVC' label", async () => {

    const masterUrl = "https://cdn.test/hevc-master.m3u8";
    const variantUrl = "https://cdn.test/hevc-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=1000000,CODECS=\"hvc1.1.6.L93.B0\"",
        "hevc-variant.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.codec, "HEVC", "hvc1 -> HEVC");
  });

  test("returns null codec when the CODECS attribute is absent", async () => {

    // Boundary: not every manifest declares CODECS. The probe must return null rather than crashing or guessing.
    const masterUrl = "https://cdn.test/nocodec-master.m3u8";
    const variantUrl = "https://cdn.test/nocodec-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=1000000",
        "nocodec-variant.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.codec, null, "absent CODECS -> null label");
  });

  test("returns null codec for an unrecognized codec prefix", async () => {

    // Boundary: a hypothetical future codec prefix not in the table maps to null rather than passing through verbatim. This keeps the codec field's domain
    // bounded to the documented label set.
    const masterUrl = "https://cdn.test/unknown-master.m3u8";
    const variantUrl = "https://cdn.test/unknown-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=1000000,CODECS=\"unkn.123\"",
        "unknown-variant.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.codec, null, "unknown prefix -> null label");
  });

  test("resolves a relative variant URL against the master URL", async () => {

    // Boundary: relative URLs in #EXT-X-STREAM-INF lines are common. The probe must resolve them against the master URL so the variant fetch goes to the right
    // origin path.
    const masterUrl = "https://cdn.test/path/master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nrelative-variant.m3u8\n", { status: 200 }),
      "https://cdn.test/path/relative-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.bestVariantUrl, "https://cdn.test/path/relative-variant.m3u8", "relative URL resolved against master");
  });

  test("populates the cache with the encryption type after a successful clear probe", async () => {

    // Cache population for non-DRM types: even though the clear/aes128 short-circuit is disabled, the type itself is still cached so subsequent calls can read
    // it via getCachedEncryption (used by setup.ts to skip the CDP interceptor for known DRM channels).
    const masterUrl = "https://cdn.test/cache-clear-master.m3u8";
    const variantUrl = "https://cdn.test/cache-clear-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\ncache-clear-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    await probeManifest(masterUrl, "probe-channel");

    assert.equal(getCachedEncryption("probe-channel"), "clear", "clear classification populated in cache");
  });
});

describe("probeManifest: media-only playlists", () => {

  beforeEach(() => {

    clearProbeCache("media-only-channel");
  });

  afterEach(() => {

    mock.reset();
  });

  test("classifies a clear media-only playlist and surfaces the input URL as the variant", async () => {

    // Happy path: a single-level media playlist (#EXTINF only, no #EXT-X-STREAM-INF) is the second of the two HLS playlist kinds. The probe must accept it,
    // produce a MediaFeed with bestVariantUrl == the input URL (the proxy will poll this URL), and classify the encryption from #EXT-X-KEY tags in the body.
    // This is the issue #34 exemplar: an unencrypted Angelcam-shaped media playlist.
    const playlistUrl = "https://cdn.test/media-only.m3u8";
    const playlistBody = [
      "#EXTM3U",
      "#EXT-X-VERSION:3",
      "#EXT-X-TARGETDURATION:6",
      "#EXT-X-MEDIA-SEQUENCE:0",
      "#EXTINF:6,",
      "seg0.ts"
    ].join("\n");

    makeFetchRouter({

      [playlistUrl]: () => new Response(playlistBody, { status: 200 }),
      // The codec inference path will attempt to fetch the first segment. Returning a 404 here is fine - codec defaults to null on segment fetch failure and the
      // probe still succeeds with the rest of the MediaFeed populated.
      "https://cdn.test/seg0.ts": () => new Response("not found", { status: 404 })
    });

    const result = await probeManifest(playlistUrl, "media-only-channel");

    assert.ok(result, "media-only probe resolved");
    assert.equal(result.encryption, "clear", "no #EXT-X-KEY -> clear");
    assert.equal(result.bestVariantUrl, playlistUrl, "input URL becomes the variant URL");
    assert.equal(result.audioVariantUrl, null, "no separate audio rendition for media-only feeds");
    assert.equal(result.bandwidth, 0, "no master metadata -> bandwidth 0");
    assert.equal(result.resolution, null, "no master metadata -> resolution null");
  });

  test("classifies a media-only playlist with AES-128 encryption and an accessible key", async () => {

    // Boundary: AES-128 key tags live on the media playlist regardless of which playlist kind originally arrived. A media-only feed with an accessible key must
    // classify as aes128 and surface the resolved key URL so the proxy can decrypt segments.
    const playlistUrl = "https://cdn.test/media-aes.m3u8";
    const keyUrl = "https://cdn.test/media-aes.key";
    const playlistBody = [
      "#EXTM3U",
      "#EXT-X-TARGETDURATION:6",
      "#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyUrl + "\"",
      "#EXTINF:6,",
      "seg0.ts"
    ].join("\n");

    makeFetchRouter({

      [playlistUrl]: () => new Response(playlistBody, { status: 200 }),
      [keyUrl]: () => new Response(Buffer.alloc(16), { status: 200 }),
      "https://cdn.test/seg0.ts": () => new Response("not found", { status: 404 })
    });

    const result = await probeManifest(playlistUrl, "media-only-channel");

    assert.ok(result, "media-only AES-128 probe resolved");
    assert.equal(result.encryption, "aes128", "AES-128 classification");
    assert.equal(result.keyUrl, keyUrl, "resolved key URL surfaces");
    assert.equal(result.bestVariantUrl, playlistUrl, "input URL becomes the variant URL");
  });

  test("classifies a media-only playlist with a relative key URI by resolving against the playlist URL", async () => {

    // Boundary: relative #EXT-X-KEY URIs must be resolved against the playlist URL itself for media-only feeds, not against any (nonexistent) master URL.
    const playlistUrl = "https://cdn.test/path/media-aes.m3u8";
    const playlistBody = [
      "#EXTM3U",
      "#EXT-X-KEY:METHOD=AES-128,URI=\"keys/segment.key\"",
      "#EXTINF:6,",
      "seg0.ts"
    ].join("\n");

    makeFetchRouter({

      [playlistUrl]: () => new Response(playlistBody, { status: 200 }),
      "https://cdn.test/path/keys/segment.key": () => new Response(Buffer.alloc(16), { status: 200 }),
      "https://cdn.test/path/seg0.ts": () => new Response("not found", { status: 404 })
    });

    const result = await probeManifest(playlistUrl, "media-only-channel");

    assert.equal(result?.keyUrl, "https://cdn.test/path/keys/segment.key", "relative key URL resolved against playlist URL");
  });

  test("returns null when the response body is not a recognizable HLS playlist", async () => {

    // Negative test: a body that classifies as "unknown" (no master signal, no media signal) must surface null so the caller falls back to capture mode without
    // attempting to feed garbage into the proxy.
    const playlistUrl = "https://cdn.test/garbage.m3u8";

    makeFetchRouter({

      [playlistUrl]: () => new Response("<html>not a playlist</html>", { status: 200 })
    });

    assert.equal(await probeManifest(playlistUrl, "media-only-channel"), null);
  });

  test("populates the cache with 'clear' after a successful media-only probe", async () => {

    // Cache population for media-only feeds mirrors the master-derived path - the encryption classification is cached so subsequent setups can short-circuit
    // for known-DRM channels (not relevant here, but the contract is the same regardless of which branch produced the classification).
    const playlistUrl = "https://cdn.test/cache-media-only.m3u8";

    makeFetchRouter({

      [playlistUrl]: () => new Response("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6,\nseg.ts\n", { status: 200 }),
      "https://cdn.test/seg.ts": () => new Response("not found", { status: 404 })
    });

    await probeManifest(playlistUrl, "media-only-channel");

    assert.equal(getCachedEncryption("media-only-channel"), "clear", "clear classification cached for media-only path");
  });
});
