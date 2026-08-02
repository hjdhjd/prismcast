/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * probe.manifest.test.ts: Unit tests for probeManifest, the master-manifest probe orchestrator in probe.ts. The orchestrator fetches a master manifest, ranks the
 * variants by descending bandwidth and takes the first whose manifest fetches, binds the audio rendition to that variant's audio group, and classifies the
 * encryption type by parsing #EXT-X-KEY tags. This file isolates the orchestrator
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

    // Boundary: descriptive-only #EXT-X-MEDIA tags (no URI) indicate muxed audio. The probe must report null, not the URL of a non-existent rendition. The
    // fixture's variant declares AUDIO="audio", so this exercises resolveAudioRendition's declared-group-with-no-URI-bearing-rendition tail path; the
    // no-AUDIO-attribute early return has its own pin in the fallback describe block below.
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

describe("probeManifest: uncovered branches", () => {

  // Mirrors PROBE_CACHE_TTL in probe.ts (24 hours). The constant is module-private, so the TTL test hardcodes the same 24h value and steps Date.now() past it.
  const PROBE_CACHE_TTL = 24 * 60 * 60 * 1000;

  beforeEach(() => {

    clearProbeCache("probe-channel");
  });

  afterEach(() => {

    mock.reset();
  });

  test("returns null when a master playlist has a #EXT-X-STREAM-INF but no following variant URL line", async () => {

    // Boundary distinct from the "unknown" branch: this body DOES classify as "master" because #EXT-X-STREAM-INF is present, but the line after it is a comment
    // (#EXT-X-ENDLIST) rather than a variant URL. selectVariants collects no candidate from that entry and returns an empty list, so resolveMasterPlaylist returns
    // null and the probe surfaces null. The existing "no variant streams" test hits the "unknown" classification branch instead, so this pins the
    // master-classified-but-no-candidates path.
    const masterUrl = "https://cdn.test/dangling-stream-inf.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=1000000",
        "#EXT-X-ENDLIST",
        ""
      ].join("\n"), { status: 200 })
    });

    assert.equal(await probeManifest(masterUrl, "probe-channel"), null, "master with no resolvable variant URL returns null");
  });

  test("ignores an #EXT-X-MEDIA rendition that is not TYPE=AUDIO even when it carries a URI", async () => {

    // resolveAudioRendition must only follow TYPE=AUDIO renditions. A TYPE=SUBTITLES rendition with a URI must never become audioVariantUrl - otherwise the proxy
    // would poll a subtitle playlist as if it were an audio track. We assert audioVariantUrl stays null despite the URI on the subtitle rendition. This fixture's
    // variant declares no AUDIO attribute, so the null-group early return is the path it exercises; the TYPE filter inside a matched group has its own test.
    const masterUrl = "https://cdn.test/subs-master.m3u8";
    const videoVariantUrl = "https://cdn.test/subs-video.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",NAME=\"English\",URI=\"https://cdn.test/subs-track.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
        "subs-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoVariantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "probe resolved");
    assert.equal(result.audioVariantUrl, null, "TYPE=SUBTITLES rendition with a URI is not treated as audio");
  });

  test("returns null when the master fetch itself throws (network error / abort)", async () => {

    // fetchManifestText wraps chromeFetch in a try/catch so a thrown fetch (DNS failure, connection reset, AbortSignal.timeout firing) surfaces as null rather
    // than propagating. probeManifest sees a null body and returns null so the caller falls back to capture instead of crashing.
    const masterUrl = "https://cdn.test/throwing-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => {

        throw new Error("simulated network failure");
      }
    });

    assert.equal(await probeManifest(masterUrl, "probe-channel"), null, "thrown fetch surfaces as null");
  });

  test("downgrades AES-128 to 'drm' when the key fetch throws", async () => {

    // testKeyAccessibility wraps the key fetch in a try/catch and returns false on any throw. A thrown key fetch (network error, abort) must downgrade the
    // classification to drm - not aes128 - because Node cannot decrypt without a fetchable key. Master and variant fetches succeed so only the key path throws.
    const masterUrl = "https://cdn.test/keythrow-master.m3u8";
    const variantUrl = "https://cdn.test/keythrow-variant.m3u8";
    const keyUrl = "https://cdn.test/keythrow-key.bin";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nkeythrow-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyUrl + "\"\n", { status: 200 }),
      [keyUrl]: () => {

        throw new Error("key endpoint unreachable");
      }
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "probe resolved");
    assert.equal(result.encryption, "drm", "thrown key fetch downgrades AES-128 to drm");
    assert.equal(result.keyUrl, null, "no key URL surfaces when the key fetch throws");
  });

  test("expires a cache entry older than PROBE_CACHE_TTL and treats it as a miss", async () => {

    // getCachedEncryption reads Date.now() directly to compute entry age. We seed the cache with a real DRM probe (timestamp = real Date.now()), then mock
    // Date.now() to a value more than 24h past the seed so the entry is stale. getCachedEncryption must delete the entry and return null. To prove the entry was
    // deleted (not merely compared against the clock), we then rewind Date.now() back inside the TTL window and confirm the lookup still misses.
    const masterUrl = "https://cdn.test/ttl-master.m3u8";
    const variantUrl = "https://cdn.test/ttl-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nttl-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://x\"\n", { status: 200 })
    });

    const seededAt = Date.now();

    await probeManifest(masterUrl, "probe-channel");
    assert.equal(getCachedEncryption("probe-channel"), "drm", "seed probe cached drm before any clock manipulation");

    // Advance the clock past the TTL. The seed timestamp is >= seededAt, so this difference is guaranteed to exceed PROBE_CACHE_TTL.
    let nowValue = seededAt + PROBE_CACHE_TTL + 1000;

    mock.method(Date, "now", () => nowValue);

    assert.equal(getCachedEncryption("probe-channel"), null, "entry older than the TTL is treated as a miss");

    // Rewind the clock inside the TTL window. A live entry would now read as fresh; a deleted one stays a miss. This distinguishes deletion from a pure age check.
    nowValue = seededAt;

    assert.equal(getCachedEncryption("probe-channel"), null, "the stale entry was deleted, not merely compared against the clock");
  });
});

describe("probeManifest: variant fallback and audio group binding", () => {

  // Mirrors MAX_VARIANT_FALLBACK_ATTEMPTS in probe.ts (3 attempts). The constant is module-private, so the cap test hardcodes the same bound and declares one more
  // variant than it allows.
  const MAX_VARIANT_FALLBACK_ATTEMPTS = 3;

  /* recordFetch wraps a router handler so the test can observe which URLs the walk touched and in what order. Order and count are the only way to tell a ranked,
   * short-circuiting crawl apart from an unordered or exhaustive one, since several of these fixtures would produce the same feed either way.
   */
  function recordFetch(fetched: string[], respond: () => Response): FetchHandler {

    return (url: string): Response => {

      fetched.push(url);

      return respond();
    };
  }

  // A minimal, valid, unencrypted media playlist body. Fixtures that only need a variant to answer successfully use this.
  function mediaBody(): Response {

    return new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 });
  }

  beforeEach(() => {

    clearProbeCache("probe-channel");
  });

  afterEach(() => {

    mock.reset();
  });

  test("falls back to the next variant when the highest-bandwidth variant fails to fetch", async () => {

    // A master's top variant can be broken while the siblings beneath it are healthy. The 4Mbps variant answers 500 and the 2Mbps one serves a valid media
    // playlist, so the probe must resolve through the sibling rather than abandoning native streaming on the first failure.
    const masterUrl = "https://cdn.test/fallback-master.m3u8";
    const highUrl = "https://cdn.test/fallback-high.m3u8";
    const lowUrl = "https://cdn.test/fallback-low.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1920x1080",
        "fallback-high.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1280x720",
        "fallback-low.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [highUrl]: () => new Response("server error", { status: 500 }),
      [lowUrl]: () => mediaBody()
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "the probe resolves through the fallback variant");
    assert.equal(result.bandwidth, 2000000, "metadata comes from the variant that actually answered");
    assert.equal(result.resolution, "1280x720", "resolution comes from the fallback variant");
    assert.equal(result.bestVariantUrl, lowUrl, "the fallback variant's URL becomes the feed URL");
  });

  test("caps the crawl at the attempt limit and spends it on the top-ranked distinct variants", async () => {

    // The cap bounds the tune-time worst case. Four variants are declared and every fetch fails, so the walk runs to exhaustion: it must stop after the cap, spend
    // each attempt on a different URL, and spend them on the highest-ranked ones.
    const masterUrl = "https://cdn.test/cap-master.m3u8";
    const topUrl = "https://cdn.test/cap-4m.m3u8";
    const secondUrl = "https://cdn.test/cap-3m.m3u8";
    const thirdUrl = "https://cdn.test/cap-2m.m3u8";
    const fourthUrl = "https://cdn.test/cap-1m.m3u8";
    const fetched: string[] = [];

    makeFetchRouter({

      [masterUrl]: recordFetch(fetched, () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=1000000",
        "cap-1m.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=4000000",
        "cap-4m.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
        "cap-2m.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=3000000",
        "cap-3m.m3u8",
        ""
      ].join("\n"), { status: 200 })),
      [topUrl]: recordFetch(fetched, () => new Response("server error", { status: 500 })),
      [secondUrl]: recordFetch(fetched, () => new Response("server error", { status: 500 })),
      [thirdUrl]: recordFetch(fetched, () => new Response("server error", { status: 500 })),
      [fourthUrl]: recordFetch(fetched, () => new Response("server error", { status: 500 }))
    });

    assert.equal(await probeManifest(masterUrl, "probe-channel"), null, "every attempted candidate failed, so the probe surfaces null");
    assert.equal(fetched.length, 1 + MAX_VARIANT_FALLBACK_ATTEMPTS, "one master fetch plus one fetch for each candidate the cap allows");
    assert.deepEqual(fetched, [ masterUrl, topUrl, secondUrl, thirdUrl ], "the three highest-bandwidth variants, each tried once, in descending order");
    assert.ok(!fetched.includes(fourthUrl), "the fourth-ranked variant is never reached");
  });

  test("selects the first document-order variant when the master advertises no bandwidths", async () => {

    // A master whose variants carry no BANDWIDTH attribute parses entirely as zeroes. The stable descending sort keeps document order, so the first declared
    // variant leads the walk and answers on its own.
    const masterUrl = "https://cdn.test/zero-master.m3u8";
    const firstUrl = "https://cdn.test/zero-first.m3u8";
    const secondUrl = "https://cdn.test/zero-second.m3u8";
    const fetched: string[] = [];

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:RESOLUTION=640x360",
        "zero-first.m3u8",
        "#EXT-X-STREAM-INF:RESOLUTION=1280x720",
        "zero-second.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [firstUrl]: recordFetch(fetched, () => mediaBody()),
      [secondUrl]: recordFetch(fetched, () => mediaBody())
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "a master with no advertised bandwidth still resolves");
    assert.equal(result.bestVariantUrl, firstUrl, "the first declared variant wins the all-equal ranking");
    assert.deepEqual(fetched, [firstUrl], "exactly one variant fetch; the second is never reached");
  });

  test("binds the audio rendition to the selected variant's group, matched by exact group id", async () => {

    // Renditions belong to the group their variant names. The master declares an unrelated group first, then a near-miss group whose id contains the selected one
    // as a substring: first-declared-wins lands on "aac-lo", and a substring or startsWith comparison lands on "aac-hi-extra".
    const masterUrl = "https://cdn.test/group-master.m3u8";
    const videoUrl = "https://cdn.test/group-video.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aac-lo\",NAME=\"English\",URI=\"https://cdn.test/group-lo.m3u8\"",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aac-hi-extra\",NAME=\"English\",URI=\"https://cdn.test/group-extra.m3u8\"",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aac-hi\",NAME=\"English\",URI=\"https://cdn.test/group-hi.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"aac-hi\"",
        "group-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoUrl]: () => mediaBody()
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.audioVariantUrl, "https://cdn.test/group-hi.m3u8", "the rendition belonging to the variant's own group is chosen");
  });

  test("yields null audio for a variant that declares no AUDIO attribute, even when the master declares a rendition", async () => {

    // A variant naming no audio group carries its audio inside its own segments, so a rendition declared for some other variant does not apply to it. The rendition
    // here carries a real URI, which is precisely what a master-wide audio parse would hand back.
    const masterUrl = "https://cdn.test/muxed-variant-master.m3u8";
    const videoUrl = "https://cdn.test/muxed-variant-video.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aac\",NAME=\"English\",URI=\"https://cdn.test/muxed-variant-audio.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
        "muxed-variant-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoUrl]: () => mediaBody()
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "the probe resolves");
    assert.equal(result.audioVariantUrl, null, "a variant with no audio group reports muxed audio");
  });

  test("rebinds audio to the group of the variant the fallback actually selected", async () => {

    // The audio must follow the video that answered, not the master's first declaration and not the last candidate in the list. The top variant fails, so the feed
    // is built from the middle one and must carry the middle one's group.
    const masterUrl = "https://cdn.test/rebind-master.m3u8";
    const topUrl = "https://cdn.test/rebind-top.m3u8";
    const middleUrl = "https://cdn.test/rebind-middle.m3u8";
    const bottomUrl = "https://cdn.test/rebind-bottom.m3u8";
    const fetched: string[] = [];

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"English\",URI=\"https://cdn.test/rebind-audio-a.m3u8\"",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"b\",NAME=\"English\",URI=\"https://cdn.test/rebind-audio-b.m3u8\"",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"c\",NAME=\"English\",URI=\"https://cdn.test/rebind-audio-c.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=3000000,AUDIO=\"a\"",
        "rebind-top.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"b\"",
        "rebind-middle.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO=\"c\"",
        "rebind-bottom.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [topUrl]: recordFetch(fetched, () => new Response("server error", { status: 500 })),
      [middleUrl]: recordFetch(fetched, () => mediaBody()),
      [bottomUrl]: recordFetch(fetched, () => mediaBody())
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "the probe resolves through the middle variant");
    assert.equal(result.audioVariantUrl, "https://cdn.test/rebind-audio-b.m3u8", "audio comes from the selected variant's group");
    assert.deepEqual(fetched, [ topUrl, middleUrl ], "the walk stops at the variant that answered");
  });

  test("prefers the group's DEFAULT=YES rendition over an earlier one", async () => {

    // Within a group the default rendition is the one a player would pick, so declaration order alone must not settle it.
    const masterUrl = "https://cdn.test/default-master.m3u8";
    const videoUrl = "https://cdn.test/default-video.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aac\",NAME=\"Commentary\",URI=\"https://cdn.test/default-commentary.m3u8\"",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aac\",NAME=\"English\",DEFAULT=YES,URI=\"https://cdn.test/default-english.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"aac\"",
        "default-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoUrl]: () => mediaBody()
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.audioVariantUrl, "https://cdn.test/default-english.m3u8", "the default rendition wins over the earlier one");
  });

  test("stops at the highest-bandwidth variant when it answers, without touching the ones below it", async () => {

    // Both variants are fetchable, so the feed alone cannot show which one the ranking preferred. The fetch record can: a descending walk touches the 4Mbps URL
    // once and stops, while an ascending or unordered walk reaches for the 2Mbps one and an exhaustive crawl fetches both.
    const masterUrl = "https://cdn.test/order-master.m3u8";
    const highUrl = "https://cdn.test/order-high.m3u8";
    const lowUrl = "https://cdn.test/order-low.m3u8";
    const fetched: string[] = [];

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
        "order-low.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=4000000",
        "order-high.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [highUrl]: recordFetch(fetched, () => mediaBody()),
      [lowUrl]: recordFetch(fetched, () => mediaBody())
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "the probe resolves");
    assert.equal(result.bestVariantUrl, highUrl, "the highest-bandwidth variant is selected");
    assert.deepEqual(fetched, [highUrl], "exactly one variant fetch, aimed at the highest-bandwidth candidate");
  });

  test("reads a variant URI only from the line immediately after its STREAM-INF, never further down", async () => {

    // The selection walk mirrors the membership walk: a STREAM-INF followed by a tag has no variant URI at all. A forward-scanner would skip the tag, claim the
    // bare URI below it as this variant's, and resolve a feed from a URI that belongs to no variant.
    const masterUrl = "https://cdn.test/orphan-master.m3u8";
    const orphanUrl = "https://cdn.test/orphan-child.m3u8";
    const fetched: string[] = [];

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
        "#EXT-X-ENDLIST",
        "orphan-child.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [orphanUrl]: recordFetch(fetched, () => mediaBody())
    });

    assert.equal(await probeManifest(masterUrl, "probe-channel"), null, "a STREAM-INF with no URI line yields no candidate");
    assert.deepEqual(fetched, [], "the orphan URI a forward-scanner would claim is never fetched");
  });

  test("keeps the TYPE=AUDIO filter inside a matched group", async () => {

    // Group membership does not make a rendition audio. A subtitle track sharing the variant's group id is declared first, and following it would have the proxy
    // poll a subtitle playlist as though it were an audio track.
    const masterUrl = "https://cdn.test/type-master.m3u8";
    const videoUrl = "https://cdn.test/type-video.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"grp\",NAME=\"English\",URI=\"https://cdn.test/type-subs.m3u8\"",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"grp\",NAME=\"English\",URI=\"https://cdn.test/type-audio.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"grp\"",
        "type-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoUrl]: () => mediaBody()
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.audioVariantUrl, "https://cdn.test/type-audio.m3u8", "the audio rendition is chosen over the subtitle track in the same group");
  });

  test("does not let a DEFAULT=YES rendition without a URI end the group walk", async () => {

    // A descriptive default declares which rendition a player should prefer without offering a playlist to poll. Treating the default marker as the end of the
    // walk would report muxed audio for a group that does carry a playable rendition further down.
    const masterUrl = "https://cdn.test/descriptive-master.m3u8";
    const videoUrl = "https://cdn.test/descriptive-video.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aac\",NAME=\"English\",DEFAULT=YES",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aac\",NAME=\"Spanish\",URI=\"https://cdn.test/descriptive-spanish.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"aac\"",
        "descriptive-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoUrl]: () => mediaBody()
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.audioVariantUrl, "https://cdn.test/descriptive-spanish.m3u8", "the walk continues past a URI-less default to a playable rendition");
  });

  test("takes the first URI-bearing rendition of the group when none is marked default", async () => {

    // With no default to prefer, the group's declaration order decides, so the remembered candidate must be the first one seen rather than the last.
    const masterUrl = "https://cdn.test/nodefault-master.m3u8";
    const videoUrl = "https://cdn.test/nodefault-video.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aac\",NAME=\"English\",URI=\"https://cdn.test/nodefault-english.m3u8\"",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aac\",NAME=\"Spanish\",URI=\"https://cdn.test/nodefault-spanish.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"aac\"",
        "nodefault-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoUrl]: () => mediaBody()
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.equal(result?.audioVariantUrl, "https://cdn.test/nodefault-english.m3u8", "the first declared rendition of the group is taken");
  });

  test("reports null audio when the variant names a group the master never declares", async () => {

    // A variant pointing at a group with no renditions is a malformed master. The probe still delivers the video feed, and the audio must be null rather than some
    // other group's rendition - the proxy would otherwise pair the video with audio belonging to a different variant.
    const masterUrl = "https://cdn.test/ghost-master.m3u8";
    const videoUrl = "https://cdn.test/ghost-video.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"real\",NAME=\"English\",URI=\"https://cdn.test/ghost-real.m3u8\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"ghost\"",
        "ghost-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoUrl]: () => mediaBody()
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "the video feed still resolves");
    assert.equal(result.audioVariantUrl, null, "an undeclared group yields no audio rather than another group's rendition");
  });

  test("attempts only the top-ranked variant when the caller pins a single attempt", async () => {

    // The token-refresh path pins one attempt so a refresh cannot reselect a different variant underneath a running proxy. A healthy sibling sits below the broken
    // top variant, so an ignored or mis-threaded option shows up as a successful probe instead of a null.
    const masterUrl = "https://cdn.test/pinned-master.m3u8";
    const topUrl = "https://cdn.test/pinned-top.m3u8";
    const siblingUrl = "https://cdn.test/pinned-sibling.m3u8";
    const fetched: string[] = [];

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=4000000",
        "pinned-top.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
        "pinned-sibling.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [topUrl]: recordFetch(fetched, () => new Response("server error", { status: 500 })),
      [siblingUrl]: recordFetch(fetched, () => mediaBody())
    });

    const result = await probeManifest(masterUrl, "probe-channel", { maxVariantAttempts: 1 });

    assert.equal(result, null, "the pinned probe gives up with the top variant rather than falling back");
    assert.deepEqual(fetched, [topUrl], "exactly one variant fetch, aimed at the top-ranked candidate");
  });

  test("drops only the variant whose URI cannot be resolved", async () => {

    // A master can carry a malformed variant reference. Every declared variant is resolved during the walk, not just the one that wins it, so an unguarded resolve
    // would throw out of the walk and take the whole probe with it. The malformed entry outranks the healthy one, so it is reached first.
    const masterUrl = "https://cdn.test/malformed-master.m3u8";
    const healthyUrl = "https://cdn.test/malformed-healthy.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=4000000",
        "//[",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
        "malformed-healthy.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [healthyUrl]: () => mediaBody()
    });

    const result = await probeManifest(masterUrl, "probe-channel");

    assert.ok(result, "the healthy variant still resolves");
    assert.equal(result.bestVariantUrl, healthyUrl, "the unresolvable variant drops out and the healthy one is selected");
  });
});

describe("probeManifest: container classification (T1)", () => {

  beforeEach(() => {

    clearProbeCache("container-channel");
  });

  afterEach(() => {

    mock.reset();
  });

  test("classifies a playlist declaring #EXT-X-MAP as fmp4", async () => {

    // The issue #44 shape: a CMAF media playlist whose fragments are preceded by an initialization segment reference.
    const playlistUrl = "https://cdn.test/container-fmp4.m3u8";
    const playlistBody = [
      "#EXTM3U",
      "#EXT-X-VERSION:7",
      "#EXT-X-TARGETDURATION:6",
      "#EXT-X-MAP:URI=\"video_1init.cmfv\"",
      "#EXTINF:6,",
      "video_1.cmfv"
    ].join("\n");

    makeFetchRouter({

      [playlistUrl]: () => new Response(playlistBody, { status: 200 }),
      "https://cdn.test/video_1.cmfv": () => new Response("not found", { status: 404 })
    });

    const result = await probeManifest(playlistUrl, "container-channel");

    assert.ok(result, "probe resolved");
    assert.equal(result.container, "fmp4", "an EXT-X-MAP declaration means fMP4");
  });

  test("classifies a playlist with no #EXT-X-MAP as ts", async () => {

    const playlistUrl = "https://cdn.test/container-ts.m3u8";
    const playlistBody = [
      "#EXTM3U",
      "#EXT-X-VERSION:3",
      "#EXT-X-TARGETDURATION:6",
      "#EXTINF:6,",
      "seg0.ts"
    ].join("\n");

    makeFetchRouter({

      [playlistUrl]: () => new Response(playlistBody, { status: 200 }),
      "https://cdn.test/seg0.ts": () => new Response("not found", { status: 404 })
    });

    const result = await probeManifest(playlistUrl, "container-channel");

    assert.ok(result, "probe resolved");
    assert.equal(result.container, "ts", "no EXT-X-MAP means self-describing MPEG-TS");
  });

  test("does not false-positive when the tag text appears inside another tag's quoted value", async () => {

    /* The scan is line-anchored, so the literal "#EXT-X-MAP:" carried inside another tag's quoted attribute is not a MAP declaration. A substring scan over the
     * whole body would misclassify this playlist as fMP4 and send the relay hunting for an initialization segment that does not exist.
     */
    const playlistUrl = "https://cdn.test/container-decoy.m3u8";
    const playlistBody = [
      "#EXTM3U",
      "#EXT-X-VERSION:3",
      "#EXT-X-TARGETDURATION:6",
      "#EXT-X-SESSION-DATA:DATA-ID=\"test.note\",VALUE=\"#EXT-X-MAP:URI=\\\"decoy.mp4\\\"\"",
      "#EXTINF:6,",
      "seg0.ts"
    ].join("\n");

    makeFetchRouter({

      [playlistUrl]: () => new Response(playlistBody, { status: 200 }),
      "https://cdn.test/seg0.ts": () => new Response("not found", { status: 404 })
    });

    const result = await probeManifest(playlistUrl, "container-channel");

    assert.ok(result, "probe resolved");
    assert.equal(result.container, "ts", "the decoy URI does not classify the playlist as fMP4");
  });

  test("carries a null container on the DRM cache synthetic", async () => {

    /* A DRM classification aborts native streaming before any media metadata is read, so the container is deliberately absent rather than fabricated. This
     * asserts the synthetic the cache short-circuit returns, which is the one MediaFeed built without a body to classify.
     */
    const playlistUrl = "https://cdn.test/container-drm.m3u8";
    const playlistBody = [
      "#EXTM3U",
      "#EXT-X-TARGETDURATION:6",
      "#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://drm\"",
      "#EXTINF:6,",
      "seg0.ts"
    ].join("\n");

    makeFetchRouter({

      [playlistUrl]: () => new Response(playlistBody, { status: 200 })
    });

    const first = await probeManifest(playlistUrl, "container-channel");

    assert.ok(first, "probe resolved");
    assert.equal(first.encryption, "drm", "the body classifies as DRM");
    assert.equal(first.container, null, "a DRM feed carries no container");

    // The second probe takes the cache short-circuit, which builds its own synthetic MediaFeed.
    const cached = await probeManifest(playlistUrl, "container-channel");

    assert.ok(cached, "the cached probe resolved");
    assert.equal(cached.encryption, "drm", "the cache short-circuit still reports DRM");
    assert.equal(cached.container, null, "the synthetic carries a null container too");
  });
});
