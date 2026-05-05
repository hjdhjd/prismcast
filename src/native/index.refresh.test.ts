/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.refresh.test.ts: Unit tests for refreshNativeManifest, the token-refresh path of the native streaming coordinator. The function tries a direct manifest
 * re-fetch first (cheap, no browser navigation), falling back to a full page reload when the master URL itself has expired. The page-reload branch is heavily
 * entangled with real Chrome via Puppeteer (page.goto, installManifestInterceptor's CDP listener wiring) and is deferred to e2e coverage; the unit tests here
 * focus on the direct-fetch branch and the early-exit conditions (proxy stopped, page closed). The companion attemptNativeStreaming tests live in index.test.ts.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { afterEach, describe, test } from "node:test";
import type { NativeProxy } from "./proxy.ts";
import assert from "node:assert/strict";
import { clearProbeCache } from "./probe.ts";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { mock } from "node:test";
import { refreshNativeManifest } from "./index.ts";

/* puppeteer-stream's PuppeteerStream module starts a WebSocketServer at import time. index.ts pulls in browser/manifestInterceptor.ts and that triggers the server
 * creation, which keeps the event loop alive after every test resolves. We unref every Server handle and timers to drain handles immediately. The same pattern is
 * documented in streaming/lifecycle.test.ts and the companion index.test.ts.
 */
const originalSetTimeout = globalThis.setTimeout;

globalThis.setTimeout = ((handler: TimerHandler, timeout?: number, ...args: unknown[]): NodeJS.Timeout => {

  const timer = (originalSetTimeout as unknown as (h: TimerHandler, t?: number, ...a: unknown[]) => NodeJS.Timeout)(handler, timeout, ...args);

  timer.unref();

  return timer;
}) as unknown as typeof globalThis.setTimeout;

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* noop is a non-empty function body used wherever we need a stub method to satisfy a contract without doing anything. Using a tiny named function avoids the
 * @typescript-eslint/no-empty-function rule that fires on bare `() => {}`.
 */
function noop(): void {

  return undefined;
}

/* makeFakePage returns a Page stub exposing only the methods refreshNativeManifest exercises on a stopped/closed-page path. The non-reload branches do not call
 * page.goto, so this minimal stub is sufficient.
 */
function makeFakePage(closed = false): Page {

  return {

    isClosed: (): boolean => closed
  } as unknown as Page;
}

/* makeFakeCdpSession returns a stub CDPSession satisfying the surface that removeManifestInterceptor uses. The functions are non-empty no-ops or resolved promises
 * so the cleanup path runs without hitting Puppeteer internals.
 */
function makeFakeCdpSession(): CDPSession {

  return {

    detach: async (): Promise<void> => Promise.resolve(),
    removeAllListeners: (): unknown => undefined,
    send: async (): Promise<unknown> => Promise.resolve(undefined)
  } as unknown as CDPSession;
}

void makeFakeCdpSession;

/* ProxyStubHooks captures the side effects that refreshNativeManifest applies to a proxy. Tests inspect these counters and captured values to assert that the
 * orchestrator called updateVariantUrl/updateAudioVariantUrl/setTokenRefreshTimer the expected number of times with the expected arguments.
 */
interface ProxyStubHooks {

  audioVariantUrl: string;

  isStopped: boolean;

  setTokenRefreshTimerCalls: number;

  variantUrl: string;
}

/* makeFakeProxy returns a NativeProxy-shaped stub paired with mutable hooks. The hooks let tests observe what the orchestrator did to the proxy without poking at
 * the proxy's internal state. The setTokenRefreshTimer hook auto-cancels the supplied timer so tests do not leak handles into the next test.
 */
function makeFakeProxy(hooks: ProxyStubHooks): NativeProxy {

  return {

    getConsecutiveErrors: (): number => 0,
    getLastSegmentSize: (): null => null,
    getLastSegmentTime: (): number => 0,
    getSegmentIndex: (): number => 0,
    getStats: (): { fetchErrors: number; segmentsFetched: number; tokenRefreshes: number } => ({ fetchErrors: 0, segmentsFetched: 0, tokenRefreshes: 0 }),
    getTargetDuration: (): number => 6,
    hasErrored: (): boolean => false,
    isStopped: (): boolean => hooks.isStopped,
    setTokenRefreshTimer: (timer: ReturnType<typeof setTimeout>): void => {

      hooks.setTokenRefreshTimerCalls++;

      // Cancel the timer so the test does not leak it into the next case.
      clearTimeout(timer);
    },
    start: noop,
    stop: noop,
    updateAudioVariantUrl: (newUrl: string): void => {

      hooks.audioVariantUrl = newUrl;
    },
    updateCdpSession: noop,
    updateVariantUrl: (newUrl: string): void => {

      hooks.variantUrl = newUrl;
    }
  };
}

/* makeFetchRouter installs a mock for globalThis.fetch that dispatches by URL prefix. Each test registers fixtures for the master/variant/key URLs it expects
 * the probe to traverse, and any unmatched URL returns 404 so unintended fetches surface as classification failures rather than silent passes.
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

describe("refreshNativeManifest", () => {

  afterEach(() => {

    mock.reset();
  });

  test("returns false when the proxy reports it is already stopped", async () => {

    // Happy negative: the refresh path must short-circuit before doing any network I/O if the proxy is already stopped. We provide a stub proxy with isStopped
    // returning true and assert false is returned with no fetch calls.
    let fetchCalls = 0;

    mock.method(globalThis, "fetch", async () => {

      fetchCalls++;

      return new Response("should not be reached", { status: 500 });
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: true, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    const result = await refreshNativeManifest({

      channelName: "stopped-channel",
      page: makeFakePage(),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "stopped-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "stopped proxy short-circuits to false");
    assert.equal(fetchCalls, 0, "no fetches issued");
  });

  test("returns false when no masterUrl is provided and the page is closed (no recovery path available)", async () => {

    // Boundary: when the L2 recovery path runs (no masterUrl) and the page itself is closed, the function has nothing to do and returns false.
    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    const result = await refreshNativeManifest({

      channelName: "closed-page-channel",
      page: makeFakePage(true),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "closed-page-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "closed page + no masterUrl -> false");
  });

  test("uses the direct-fetch strategy when a masterUrl is provided and updates the proxy variant URL", async () => {

    // Happy path: when the master URL is still valid, the direct fetch returns a fresh manifest. The function calls proxy.updateVariantUrl with the new variant
    // URL. We verify by capturing the updateVariantUrl invocation through the proxy stub.
    const expirySeconds = Math.floor(Date.now() / 1000) + 600;
    const masterUrl = "https://cdn.test/refresh-master.m3u8?exp=" + String(expirySeconds);
    const variantUrl = "https://cdn.test/refresh-variant.m3u8?exp=" + String(expirySeconds);

    makeFetchRouter({

      "https://cdn.test/refresh-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nrefresh-variant.m3u8?exp=" + String(expirySeconds) + "\n", { status: 200 }),
      "https://cdn.test/refresh-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("refresh-channel");

    const result = await refreshNativeManifest({

      channelName: "refresh-channel",
      masterUrl,
      page: makeFakePage(),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "refresh-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "direct refresh succeeds");
    assert.equal(hooks.variantUrl, variantUrl, "proxy variant URL updated");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "next refresh scheduled");
  });

  test("returns false when the direct-fetch probe classifies the new manifest as DRM (refresh aborts)", async () => {

    // Negative test: a service that flips from clear to DRM mid-session (rare but possible during ad pods). The refresh's direct-fetch path must abort and not
    // hand a DRM manifest to the proxy. The function then attempts page reload, which we short-circuit by closing the page.
    const masterUrl = "https://cdn.test/flip-master.m3u8";
    const variantUrl = "https://cdn.test/flip-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nflip-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://x\"\n", { status: 200 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("flip-channel");

    const result = await refreshNativeManifest({

      channelName: "flip-channel",
      masterUrl,
      page: makeFakePage(true),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "flip-stream",
      url: "https://example.test/channel"
    });

    // The function falls through to page reload; the page is closed so it returns false.
    assert.equal(result, false, "DRM flip + closed page -> false");
    assert.equal(hooks.variantUrl, "", "proxy variant URL was NOT updated");
  });

  test("returns false when the direct-fetch probe fails entirely (master 500) and the page is closed", async () => {

    // Negative test: master fetch fails, direct-fetch path returns null, function falls through to page reload, which short-circuits because the page is closed.
    const masterUrl = "https://cdn.test/refresh-fail-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("server error", { status: 500 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    const result = await refreshNativeManifest({

      channelName: "refresh-fail-channel",
      masterUrl,
      page: makeFakePage(true),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "refresh-fail-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "refresh failure with closed page -> false");
  });

  test("updates the audio variant URL when the refreshed manifest declares separate audio", async () => {

    // The direct-fetch path must propagate both the video variant URL and the audio variant URL to the proxy. We capture both calls through the proxy stub and
    // assert they fire with the URLs from the new manifest.
    const masterUrl = "https://cdn.test/refresh-dai-master.m3u8";
    const videoUrl = "https://cdn.test/refresh-dai-video.m3u8";
    const audioUrl = "https://cdn.test/refresh-dai-audio.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"English\",URI=\"" + audioUrl + "\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=3000000,AUDIO=\"audio\"",
        "refresh-dai-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("refresh-dai-channel");

    const result = await refreshNativeManifest({

      channelName: "refresh-dai-channel",
      masterUrl,
      page: makeFakePage(),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "refresh-dai-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "direct refresh succeeds");
    assert.equal(hooks.variantUrl, videoUrl, "video variant URL updated");
    assert.equal(hooks.audioVariantUrl, audioUrl, "audio variant URL updated");
  });
});
