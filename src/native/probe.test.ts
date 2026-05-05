/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * probe.test.ts: Unit tests for the cache helpers (getCachedEncryption, clearProbeCache) and the URL resolver (resolveUrl) in probe.ts. The probeManifest
 * orchestrator is exercised in the companion probe.manifest.test.ts file - splitting keeps each file under the per-file line guideline. The cache helpers are
 * round-tripped via real probeManifest invocations so the cache state observed in tests matches the production write path; the manifest orchestrator's broader
 * classification matrix lives next door.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { clearProbeCache, getCachedEncryption, probeManifest, resolveUrl } from "./probe.ts";
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
