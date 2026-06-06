/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsPlaylistObserver.test.ts: Unit tests for the HLS-aware observer layered on the tab-wide network observer. The observer's contract has four pillars:
 * (1) it filters network responses to `.m3u8` URLs and ignores everything else; (2) it fetches each manifest body via chromeFetch (Node-side, not CDP) and
 * classifies it via classifyHlsPlaylist(); (3) it dedups by URL so a repeatedly-polled chunklist fires the callback exactly once and is fetched exactly once;
 * (4) disposal is idempotent and matches the project's dispose() + Symbol.dispose convention. The fixture (FakeCdpSession, FakeConnection, makeFakeCdpPage) is
 * shared via src/testing/cdp.helpers.ts with the other observers in the stack; globalThis.fetch is mocked locally to feed synthetic manifest bodies into
 * classifyHlsPlaylist.
 */
import { FakeCdpSession, FakeConnection, closePuppeteerStreamWssOnIdle, makeFakeCdpPage, noop } from "../testing.helpers.ts";
import { afterEach, describe, mock, test } from "node:test";
import type { ObservedHlsPlaylist } from "./hlsPlaylistObserver.ts";
import assert from "node:assert/strict";
import { observeHlsPlaylists } from "./hlsPlaylistObserver.ts";

// Schedule background-server cleanup on a 0ms unref'd timer so the runner exits cleanly after the suite resolves.
closePuppeteerStreamWssOnIdle();

// Synthetic manifest bodies. Master declares one variant; media declares one segment; junk is body content that classifyHlsPlaylist returns "unknown" for.
const MASTER_BODY = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nvariant.m3u8\n";
const MEDIA_BODY = "#EXTM3U\n#EXTINF:6,\nsegment.ts\n";
const UNKNOWN_BODY = "<html><body>not an HLS playlist</body></html>";

/* makeFetchCounter installs a globalThis.fetch mock that returns a configurable body per URL and counts invocations per URL. Tests use it to assert that the
 * HLS observer issued exactly one fetch for a deduped URL while still serving a recognizable manifest body.
 */
function makeFetchCounter(bodies: Record<string, string>): { counts: Map<string, number> } {

  const counts = new Map<string, number>();

  mock.method(globalThis, "fetch", async (input: string | URL): Promise<Response> => {

    const url = input.toString();

    counts.set(url, (counts.get(url) ?? 0) + 1);

    const body = bodies[url];

    if(body === undefined) {

      return new Response("not found", { status: 404 });
    }

    return new Response(body, { status: 200 });
  });

  return { counts };
}

describe("observeHlsPlaylists", () => {

  afterEach(() => {

    mock.reset();
  });

  test("returns null when the underlying tab observer cannot be installed (closed page)", async () => {

    // Boundary: the HLS observer is a layered consumer of the tab observer. If the tab observer fails to install (e.g., the page is closed), the HLS observer
    // must propagate the failure rather than returning a half-initialized handle.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession, true), { logCategory: "test:hls", onPlaylist: noop });

    assert.equal(observer, null, "closed page propagates null up through the HLS layer");
  });

  test("ignores responses for non-.m3u8 URLs", async () => {

    // The URL filter is the first gate: anything that does not end in .m3u8 (ignoring query string) must not trigger a body fetch and must not produce a callback.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const { counts } = makeFetchCounter({});
    const observed: ObservedHlsPlaylist[] = [];

    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), {

      logCategory: "test:hls",
      onPlaylist: (p): void => { observed.push(p); }
    });

    assert.ok(observer, "observer installed");

    rootSession.emitResponse("https://cdn.test/page.html");
    rootSession.emitResponse("https://cdn.test/segment.ts");
    rootSession.emitResponse("https://cdn.test/poster.jpg");

    // Allow body fetch promises to settle even though none should be issued.
    await Promise.resolve();

    assert.equal(counts.size, 0, "no body fetches issued for non-.m3u8 URLs");
    assert.equal(observed.length, 0, "no callbacks fired for non-.m3u8 URLs");

    observer.dispose();
  });

  test("delivers a master playlist callback for a master manifest body", async () => {

    // Happy path: a .m3u8 URL whose body parses as a master playlist (`#EXT-X-STREAM-INF` present) must fire the callback with kind="master".
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const masterUrl = "https://cdn.test/master.m3u8";

    makeFetchCounter({ [masterUrl]: MASTER_BODY });

    const observed: ObservedHlsPlaylist[] = [];
    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), {

      logCategory: "test:hls",
      onPlaylist: (p): void => { observed.push(p); }
    });

    assert.ok(observer, "observer installed");

    rootSession.emitResponse(masterUrl);

    // Spin the microtask queue to let the async chromeFetch resolve and the classify dispatch complete.
    await new Promise((r) => setTimeout(r, 10));

    assert.equal(observed.length, 1, "master manifest fires exactly one callback");

    const first = observed[0];

    assert.ok(first, "callback captured");
    assert.equal(first.kind, "master", "kind classified as master");
    assert.equal(first.url, masterUrl, "URL forwarded verbatim");

    observer.dispose();
  });

  test("delivers a media playlist callback for a media manifest body", async () => {

    // Happy path: a .m3u8 URL whose body parses as a media playlist (`#EXTINF` present, no `#EXT-X-STREAM-INF`) must fire the callback with kind="media".
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const mediaUrl = "https://cdn.test/media.m3u8";

    makeFetchCounter({ [mediaUrl]: MEDIA_BODY });

    const observed: ObservedHlsPlaylist[] = [];
    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), {

      logCategory: "test:hls",
      onPlaylist: (p): void => { observed.push(p); }
    });

    assert.ok(observer, "observer installed");

    rootSession.emitResponse(mediaUrl);
    await new Promise((r) => setTimeout(r, 10));

    assert.equal(observed.length, 1, "media manifest fires exactly one callback");

    const first = observed[0];

    assert.ok(first, "callback captured");
    assert.equal(first.kind, "media", "kind classified as media");

    observer.dispose();
  });

  test("skips responses whose body classifies as unknown (no HLS directives)", async () => {

    // Boundary: a .m3u8 URL whose body has no recognizable HLS directives (e.g., the CDN served an error page with HTML body and .m3u8-extension URL) must not
    // fire the callback. The probe gate at classifyHlsPlaylist() is the authority here.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const junkUrl = "https://cdn.test/error.m3u8";

    makeFetchCounter({ [junkUrl]: UNKNOWN_BODY });

    const observed: ObservedHlsPlaylist[] = [];
    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), {

      logCategory: "test:hls",
      onPlaylist: (p): void => { observed.push(p); }
    });

    assert.ok(observer, "observer installed");

    rootSession.emitResponse(junkUrl);
    await new Promise((r) => setTimeout(r, 10));

    assert.equal(observed.length, 0, "unknown body produces no callback");

    observer.dispose();
  });

  test("deduplicates per URL: duplicate observations of the same URL fire the callback exactly once", async () => {

    // The dedup contract that motivates this module's existence as its own layer. hls.js inside an OOPIF polls its chunklist every ~2 seconds; without dedup we
    // would fire the callback once per poll, wasting consumer-side bookkeeping and re-classifying identical bodies. The first observation of a URL triggers the
    // full pipeline; subsequent observations of the same URL are silently dropped.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const chunklistUrl = "https://cdn.test/chunklist.m3u8";

    const { counts } = makeFetchCounter({ [chunklistUrl]: MEDIA_BODY });

    const observed: ObservedHlsPlaylist[] = [];
    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), {

      logCategory: "test:hls",
      onPlaylist: (p): void => { observed.push(p); }
    });

    assert.ok(observer, "observer installed");

    rootSession.emitResponse(chunklistUrl);
    rootSession.emitResponse(chunklistUrl);
    rootSession.emitResponse(chunklistUrl);
    rootSession.emitResponse(chunklistUrl);

    await new Promise((r) => setTimeout(r, 20));

    assert.equal(observed.length, 1, "duplicate URL observations fire the callback exactly once");
    assert.equal(counts.get(chunklistUrl), 1, "duplicate URL observations issue exactly one body fetch");

    observer.dispose();
  });

  test("treats two tokenized URLs to the same playlist as distinct (full-URL key, not pathname)", async () => {

    // Boundary: dedup keys on the full URL including query string. Two requests to the same logical playlist but with different signed-token query strings are
    // observed as distinct because the body returned could differ (e.g., a refresh after a token expired returns a new chunklist with newer segments). This is
    // intentional: the consumer of this module decides whether to collapse them at the URL-canonicalization layer.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const urlA = "https://cdn.test/master.m3u8?token=alpha";
    const urlB = "https://cdn.test/master.m3u8?token=beta";

    makeFetchCounter({ [urlA]: MASTER_BODY, [urlB]: MASTER_BODY });

    const observed: ObservedHlsPlaylist[] = [];
    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), {

      logCategory: "test:hls",
      onPlaylist: (p): void => { observed.push(p); }
    });

    assert.ok(observer, "observer installed");

    rootSession.emitResponse(urlA);
    rootSession.emitResponse(urlB);

    await new Promise((r) => setTimeout(r, 20));

    assert.equal(observed.length, 2, "differently-tokenized URLs each fire a callback");

    observer.dispose();
  });

  test("does not fire the callback for a response that arrives after dispose", async () => {

    // Race-safety contract: a response in flight (chromeFetch issued) may resolve after the observer is disposed. The disposed flag inside the body-fetch path
    // gates the callback to prevent ghost deliveries after teardown.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const url = "https://cdn.test/race.m3u8";

    // Slow-fetch mock: hold the response until we manually resolve, so the test can dispose between observation and resolution.
    let resolveFetch!: (response: Response) => void;
    const pending = new Promise<Response>((resolve) => { resolveFetch = resolve; });

    mock.method(globalThis, "fetch", async (): Promise<Response> => pending);

    const observed: ObservedHlsPlaylist[] = [];
    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), {

      logCategory: "test:hls",
      onPlaylist: (p): void => { observed.push(p); }
    });

    assert.ok(observer, "observer installed");

    rootSession.emitResponse(url);

    // Dispose while the fetch is still pending.
    observer.dispose();

    // Now resolve the fetch. The disposed guard inside the body-fetch path must drop the response.
    resolveFetch(new Response(MASTER_BODY, { status: 200 }));

    await new Promise((r) => setTimeout(r, 20));

    assert.equal(observed.length, 0, "post-dispose response is not delivered");
  });

  test("dispose() is idempotent - a second call is a safe no-op", async () => {

    // Boundary: cleanup paths may invoke dispose from multiple code paths. Locking the no-op contract prevents double-disabling errors that would mask real bugs.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), { logCategory: "test:hls", onPlaylist: noop });

    assert.ok(observer, "observer installed");
    assert.doesNotThrow(() => {

      observer.dispose();
      observer.dispose();
    });
  });

  test("[Symbol.dispose] is wired and identical to dispose()", async () => {

    // Identity contract: dispose() and Symbol.dispose are the same function reference. Same expectation as on TabNetworkObserver; this is what makes the "using"
    // keyword work without behavioral surprises.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), { logCategory: "test:hls", onPlaylist: noop });

    assert.ok(observer, "observer installed");
    assert.equal(typeof observer[Symbol.dispose], "function", "Symbol.dispose hook present");
    assert.equal(observer[Symbol.dispose], observer.dispose, "Symbol.dispose is the same function reference as dispose");

    observer.dispose();
  });

  test("the using keyword triggers disposal at scope exit (normal path)", async () => {

    // End-to-end TC39 ERM contract: at scope exit, V8/Node invokes Symbol.dispose, which calls dispose(), which tears down the underlying tab observer. The
    // observable side effect is Network.disable on the root CDP session, propagated by the tab observer's own disposal path.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);

    {

      using observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), { logCategory: "test:hls", onPlaylist: noop });

      assert.ok(observer, "observer installed inside the using scope");
    }

    const networkDisable = rootSession.sent.find((c) => c.method === "Network.disable");

    assert.ok(networkDisable, "using-scope exit propagates Network.disable through to the root CDP session");
  });

  test("the using keyword triggers disposal even when the scope exits via thrown exception", async () => {

    // Exception-safety contract: TC39 ERM guarantees disposal on the throw path. Same expectation as on TabNetworkObserver - the HLS layer must propagate the
    // guarantee to its underlying observer.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);

    await assert.rejects(async () => {

      using observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), { logCategory: "test:hls", onPlaylist: noop });

      assert.ok(observer, "observer installed inside the using scope");

      throw new Error("simulated failure inside the using scope");
    }, /simulated failure/);

    const networkDisable = rootSession.sent.find((c) => c.method === "Network.disable");

    assert.ok(networkDisable, "throw-path scope exit still propagates Network.disable through to the root CDP session");
  });

  test("does not retry the body fetch when the first fetch fails (failed-fetch-no-retry dedup invariant)", async () => {

    // The dedup design keeps a URL in seenUrls across both successful and failed fetches, so a transient network blip on the first observation does not trigger
    // an immediate retry on the next observation of the same URL. The interceptor timeout in the consumer is the safety net for genuinely missed manifests; the
    // alternative ("only add to seenUrls on success") would invite thrashing on a URL that fails repeatedly. This test locks the chosen semantics so a refactor
    // that flipped the order would fail here.
    const connection = new FakeConnection();
    const rootSession = new FakeCdpSession(connection);
    const url = "https://cdn.test/transient-fail.m3u8";

    // Empty map - all fetches return 404 from the mock.
    const { counts } = makeFetchCounter({});

    const observed: ObservedHlsPlaylist[] = [];
    const observer = await observeHlsPlaylists(makeFakeCdpPage(rootSession), {

      logCategory: "test:hls",
      onPlaylist: (p): void => { observed.push(p); }
    });

    assert.ok(observer, "observer installed");

    // First observation: fetch returns 404, callback NOT fired, but URL is in seenUrls.
    rootSession.emitResponse(url);
    await new Promise((r) => setTimeout(r, 20));

    assert.equal(observed.length, 0, "404 fetch produces no callback");
    assert.equal(counts.get(url), 1, "first observation triggered exactly one fetch");

    // Second observation of the same URL: must NOT retry the fetch even though the first attempt failed.
    rootSession.emitResponse(url);
    await new Promise((r) => setTimeout(r, 20));

    assert.equal(observed.length, 0, "repeat observation still produces no callback");
    assert.equal(counts.get(url), 1, "repeat observation issues NO additional fetch (dedup gate held)");

    observer.dispose();
  });
});
