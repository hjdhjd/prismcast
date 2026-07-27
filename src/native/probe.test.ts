/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * probe.test.ts: Unit tests for the cache helpers (getCachedEncryption, clearProbeCache), the URL resolver (resolveUrl), and the playlist classifier
 * (classifyHlsPlaylist) in probe.ts. The probeManifest orchestrator is exercised in the companion probe.manifest.test.ts file - splitting keeps each file under
 * the per-file line guideline. The cache helpers are round-tripped via real probeManifest invocations so the cache state observed in tests matches the
 * production write path; the classifier and URL resolver are pure functions and are tested directly without I/O.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { classifyHlsPlaylist, clearProbeCache, extractChildPlaylistUrls, getCachedEncryption, isLiveMediaPlaylist, probeManifest, resolveUrl } from "./probe.ts";
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

describe("getCachedEncryption", () => {

  beforeEach(() => {

    // Clear any cache state from prior tests so each probe test starts fresh. The cache is module-scoped so cross-test bleed is possible without this reset.
    clearProbeCache("test-channel-1");
    clearProbeCache("test-channel-2");
    clearProbeCache("test-channel-3");
  });

  afterEach(() => {

    mock.reset();
  });

  test("returns null for a channel that has never been probed", () => {

    // Boundary: a fresh channel name with no cache entry must return null, not throw and not return a default classification.
    assert.equal(getCachedEncryption("never-probed-channel"), null, "fresh channel returns null");
  });

  test("returns the cached encryption type after a successful DRM probe", async () => {

    // After a probe completes (even unsuccessfully classifying as DRM), the cache must hold the type so the next call short-circuits. We exercise the cache via
    // a real probe that classifies the variant as DRM (Widevine via SAMPLE-AES).
    const masterUrl = "https://cdn.test/master.m3u8";
    const variantUrl = "https://cdn.test/variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nvariant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://example\"\n", { status: 200 })
    });

    const result = await probeManifest(masterUrl, "test-channel-1");

    assert.ok(result, "probe resolved with a result");
    assert.equal(result.encryption, "drm", "SAMPLE-AES classified as DRM");
    assert.equal(getCachedEncryption("test-channel-1"), "drm", "cache holds the DRM classification");
  });
});

describe("clearProbeCache", () => {

  beforeEach(() => {

    clearProbeCache("clear-test-channel");
  });

  afterEach(() => {

    mock.reset();
  });

  test("removes a previously cached entry so getCachedEncryption returns null", async () => {

    // First: populate the cache via a DRM probe. Then: clear and verify the cache is empty. This is the recovery path that runs when a stream fails - the next
    // attempt must re-probe rather than reading a stale classification.
    const masterUrl = "https://cdn.test/clear-master.m3u8";
    const variantUrl = "https://cdn.test/clear-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nclear-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://x\"\n", { status: 200 })
    });

    await probeManifest(masterUrl, "clear-test-channel");
    assert.equal(getCachedEncryption("clear-test-channel"), "drm", "cache populated after first probe");

    clearProbeCache("clear-test-channel");
    assert.equal(getCachedEncryption("clear-test-channel"), null, "cache empty after clear");
  });

  test("is a no-op when the channel has no cache entry", () => {

    // Negative test: clearing an unknown channel must not throw. The recovery path may run twice in quick succession.
    assert.doesNotThrow(() => {

      clearProbeCache("never-cached-channel");
    });
  });
});

describe("resolveUrl", () => {

  test("returns an http:// URL unchanged (already absolute)", () => {

    // Happy path: the function must short-circuit on already-absolute URLs and return them verbatim.
    assert.equal(resolveUrl("http://example.test/segment.ts", "https://base.test/manifest.m3u8"), "http://example.test/segment.ts");
  });

  test("returns an https:// URL unchanged (already absolute)", () => {

    assert.equal(resolveUrl("https://cdn.test/seg.ts", "https://master.test/index.m3u8"), "https://cdn.test/seg.ts");
  });

  test("resolves a path-relative URL against the base URL", () => {

    // Standard relative-URL resolution per WHATWG URL: the relative segment replaces the base's filename component, preserving the directory.
    assert.equal(resolveUrl("segment0.ts", "https://cdn.test/path/manifest.m3u8"), "https://cdn.test/path/segment0.ts");
  });

  test("resolves a root-relative URL against the base host", () => {

    // Boundary: a leading "/" rebases to the host root of the base URL.
    assert.equal(resolveUrl("/abs/segment.ts", "https://cdn.test/path/manifest.m3u8"), "https://cdn.test/abs/segment.ts");
  });

  test("resolves a parent-directory traversal in the relative URL", () => {

    // Boundary: ../ segments collapse the base path appropriately.
    assert.equal(resolveUrl("../other/seg.ts", "https://cdn.test/a/b/manifest.m3u8"), "https://cdn.test/a/other/seg.ts");
  });

  test("preserves query strings on the relative URL", () => {

    // Query strings carry auth tokens in many HLS providers - the resolver must preserve them through the URL constructor pipeline.
    assert.equal(resolveUrl("seg.ts?token=abc", "https://cdn.test/path/manifest.m3u8"), "https://cdn.test/path/seg.ts?token=abc");
  });

  test("treats a protocol-relative URL (//host/path) as absolute via the base scheme", () => {

    // Boundary: //host/path is technically not absolute by our string check (it doesn't start with http:// or https://), so it falls through to the URL
    // constructor. The constructor inherits the base scheme. Lock the resulting behavior.
    assert.equal(resolveUrl("//other.test/seg.ts", "https://cdn.test/manifest.m3u8"), "https://other.test/seg.ts");
  });
});

describe("classifyHlsPlaylist", () => {

  test("classifies a body containing #EXT-X-STREAM-INF as 'master'", () => {

    // Happy path: the canonical master playlist signal. A body with one or more #EXT-X-STREAM-INF directives is a multivariant playlist and must classify as
    // "master" so the probe takes the variant-resolution branch.
    const body = [
      "#EXTM3U",
      "#EXT-X-VERSION:6",
      "#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1280x720",
      "variant.m3u8"
    ].join("\n");

    assert.equal(classifyHlsPlaylist(body), "master");
  });

  test("classifies a body containing #EXTINF as 'media'", () => {

    // Happy path: a flat media playlist (segments referenced directly via #EXTINF) is the second of the two HLS playlist kinds. The classifier must report it
    // as "media" so the probe can treat the input URL as the variant feed itself.
    const body = [
      "#EXTM3U",
      "#EXT-X-VERSION:3",
      "#EXT-X-TARGETDURATION:6",
      "#EXT-X-MEDIA-SEQUENCE:0",
      "#EXTINF:6.0,",
      "segment0.ts"
    ].join("\n");

    assert.equal(classifyHlsPlaylist(body), "media");
  });

  test("classifies a header-only media playlist (#EXT-X-TARGETDURATION but no #EXTINF yet) as 'media'", () => {

    // Boundary: a media playlist that has been published before any segments have rolled in still has #EXT-X-TARGETDURATION. The classifier must accept that as
    // a media signal so the probe does not reject a momentarily-empty live playlist.
    const body = [
      "#EXTM3U",
      "#EXT-X-VERSION:3",
      "#EXT-X-TARGETDURATION:6",
      "#EXT-X-MEDIA-SEQUENCE:0"
    ].join("\n");

    assert.equal(classifyHlsPlaylist(body), "media");
  });

  test("classifies a master signal as 'master' even when media-shaped tags also appear", () => {

    // Negative test against a tag-collision edge case: if a master playlist somehow includes a stray #EXTINF (e.g., from a malformed CDN response), the master
    // classification must win because #EXT-X-STREAM-INF is unambiguous - only master playlists declare variant streams. We lock the precedence order so a future
    // refactor cannot accidentally invert it.
    const body = [
      "#EXTM3U",
      "#EXTINF:6.0,",
      "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
      "variant.m3u8"
    ].join("\n");

    assert.equal(classifyHlsPlaylist(body), "master");
  });

  test("classifies a body with neither variant nor media signals as 'unknown'", () => {

    // Negative test: a response body that happens to be served as text/* but contains no recognizable HLS directives must classify as "unknown" so consumers
    // (the manifest interceptor in particular) can ignore it without forcing the probe to attempt classification.
    assert.equal(classifyHlsPlaylist("<html><body>not a playlist</body></html>"), "unknown");
    assert.equal(classifyHlsPlaylist(""), "unknown");
    assert.equal(classifyHlsPlaylist("#EXTM3U"), "unknown");
  });

  test("ignores tag-shaped content embedded in non-tag lines", () => {

    // Boundary: HLS tags are only recognized at the start of a line. A URL or comment containing the literal text "#EXTINF" or "#EXT-X-STREAM-INF" must not
    // trigger classification, because the spec defines tags by line-start position. Locks the line-anchored parsing strategy against a regression that would
    // misread a CDN URL containing the substring as a real tag.
    const body = "https://cdn.test/path?fragment=#EXT-X-STREAM-INF\nhttps://cdn.test/path?fragment=#EXTINF\n";

    assert.equal(classifyHlsPlaylist(body), "unknown");
  });

  test("classifies an Angelcam-shaped live media playlist as 'media'", () => {

    // Realistic fixture lifted from the open-issue exemplar (Angelcam): live sliding-window media playlist with a discontinuity sequence and program-date-time
    // segment titles. Locks the contract that real-world media playlists from non-DRM HLS sources classify correctly.
    const body = [
      "#EXTM3U",
      "#EXT-X-VERSION:3",
      "#EXT-X-TARGETDURATION:6",
      "#EXT-X-MEDIA-SEQUENCE:368262",
      "#EXT-X-DISCONTINUITY-SEQUENCE:4",
      "#EXTINF:6,video;1;2026-05-08T23:11:08.073332698+00:00",
      "streaming-master-m1-na8/segment-368262.ts",
      "#EXTINF:6,video;1;2026-05-08T23:11:14.053148111+00:00",
      "streaming-master-m1-na8/segment-368263.ts"
    ].join("\n");

    assert.equal(classifyHlsPlaylist(body), "media");
  });
});

describe("extractChildPlaylistUrls", () => {

  const masterUrl = "https://cdn.test/path/master.m3u8";

  test("resolves STREAM-INF variant URIs relative and absolute against the master URL", () => {

    const body = [
      "#EXTM3U",
      "#EXT-X-STREAM-INF:BANDWIDTH=1000000",
      "relative/variant.m3u8",
      "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
      "https://other.test/abs/variant.m3u8"
    ].join("\n");

    const result = extractChildPlaylistUrls(body, masterUrl);

    assert.deepEqual(result.toSorted(), [ "https://cdn.test/path/relative/variant.m3u8", "https://other.test/abs/variant.m3u8" ]);
  });

  test("skips a STREAM-INF whose next line is a tag rather than a URI (single-line walk, not a forward scan)", () => {

    // The walk reads only the line immediately after each STREAM-INF, matching selectBestVariant. A forward-scanner would instead skip past the tag and claim the
    // orphan URI two lines down; that orphan belongs to no variant, so it must not appear. The valid variant of the first STREAM-INF proves the walk still works.
    const body = [
      "#EXTM3U",
      "#EXT-X-STREAM-INF:BANDWIDTH=1000000",
      "variant-a.m3u8",
      "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
      "#EXT-X-DISCONTINUITY",
      "orphan-child.m3u8"
    ].join("\n");

    const result = extractChildPlaylistUrls(body, masterUrl);

    assert.deepEqual(result, ["https://cdn.test/path/variant-a.m3u8"], "only the first STREAM-INF's directly-following URI is collected");
    assert.ok(!result.includes("https://cdn.test/path/orphan-child.m3u8"), "the orphan URI a forward-scanner would grab is absent");
  });

  test("includes EXT-X-MEDIA URIs of every type and skips descriptive-only renditions without a URI", () => {

    // AUDIO, SUBTITLES, and a VIDEO rendition (a type beyond audio/subtitles) all carry URIs and are collected - the any-type contract. The CLOSED-CAPTIONS entry
    // is descriptive-only (no URI attribute) and is skipped.
    const body = [
      "#EXTM3U",
      "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"English\",URI=\"audio-en.m3u8\"",
      "#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"s\",NAME=\"English\",URI=\"subs-en.m3u8\"",
      "#EXT-X-MEDIA:TYPE=CLOSED-CAPTIONS,GROUP-ID=\"cc\",NAME=\"CC1\",INSTREAM-ID=\"CC1\"",
      "#EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"v\",NAME=\"Angle2\",URI=\"video-angle2.m3u8\"",
      "#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO=\"a\",SUBTITLES=\"s\"",
      "variant.m3u8"
    ].join("\n");

    const result = extractChildPlaylistUrls(body, masterUrl);

    assert.deepEqual(result.toSorted(), [
      "https://cdn.test/path/audio-en.m3u8",
      "https://cdn.test/path/subs-en.m3u8",
      "https://cdn.test/path/variant.m3u8",
      "https://cdn.test/path/video-angle2.m3u8"
    ]);
  });

  test("deduplicates identical child URIs declared by more than one variant", () => {

    const body = [
      "#EXTM3U",
      "#EXT-X-STREAM-INF:BANDWIDTH=1000000",
      "dup.m3u8",
      "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
      "dup.m3u8"
    ].join("\n");

    const result = extractChildPlaylistUrls(body, masterUrl);

    assert.deepEqual(result, ["https://cdn.test/path/dup.m3u8"], "the repeated child URI appears once");
  });

  test("returns an empty array for a master declaring no variants or renditions", () => {

    const body = [ "#EXTM3U", "#EXT-X-VERSION:4", "#EXT-X-INDEPENDENT-SEGMENTS" ].join("\n");

    assert.deepEqual(extractChildPlaylistUrls(body, masterUrl), []);
  });

  test("drops a malformed child URI and keeps the valid one without throwing", () => {

    // The malformed entry is a protocol-relative URI with a space in the host, which throws in new URL against the master base. A garbage string that resolved as a
    // relative path would make the negative control vacuous, so this fixture genuinely throws and must be dropped while the valid variant survives.
    const body = [
      "#EXTM3U",
      "#EXT-X-STREAM-INF:BANDWIDTH=1000000",
      "good/variant.m3u8",
      "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
      "//h ost/x.m3u8"
    ].join("\n");

    const result = extractChildPlaylistUrls(body, masterUrl);

    assert.deepEqual(result, ["https://cdn.test/path/good/variant.m3u8"], "only the valid child survives; the malformed URI is dropped and no throw escapes");
  });
});

describe("isLiveMediaPlaylist", () => {

  test("classifies a bare sliding-window media playlist as live", () => {

    const body = [ "#EXTM3U", "#EXT-X-TARGETDURATION:6", "#EXTINF:6,", "seg-1.ts", "#EXTINF:6,", "seg-2.ts" ].join("\n");

    assert.equal(isLiveMediaPlaylist(body), true);
  });

  test("classifies an EVENT-typed playlist as live because it only appends segments", () => {

    const body = [ "#EXTM3U", "#EXT-X-PLAYLIST-TYPE:EVENT", "#EXT-X-TARGETDURATION:6", "#EXTINF:6,", "seg-1.ts" ].join("\n");

    assert.equal(isLiveMediaPlaylist(body), true);
  });

  test("classifies a playlist carrying #EXT-X-ENDLIST as not live", () => {

    const body = [ "#EXTM3U", "#EXT-X-TARGETDURATION:6", "#EXTINF:6,", "seg-1.ts", "#EXT-X-ENDLIST" ].join("\n");

    assert.equal(isLiveMediaPlaylist(body), false);
  });

  test("classifies a VOD-typed playlist as not live", () => {

    const body = [ "#EXTM3U", "#EXT-X-PLAYLIST-TYPE:VOD", "#EXT-X-TARGETDURATION:6", "#EXTINF:6,", "seg-1.ts" ].join("\n");

    assert.equal(isLiveMediaPlaylist(body), false);
  });

  test("stays live when the ENDLIST token appears only inside a comment line (line-anchored, not a substring test)", () => {

    // A naive body.includes("#EXT-X-ENDLIST") would fire on the token embedded in the comment and wrongly report the playlist complete. The line-anchored walk
    // only matches a line that starts with the tag, so this playlist stays live.
    const body = [ "#EXTM3U", "#EXT-X-TARGETDURATION:6", "# a note mentioning #EXT-X-ENDLIST inline", "#EXTINF:6,", "seg-1.ts" ].join("\n");

    assert.equal(isLiveMediaPlaylist(body), true);
  });
});
