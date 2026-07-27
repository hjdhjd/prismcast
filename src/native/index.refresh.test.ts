/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.refresh.test.ts: Unit tests for refreshNativeManifest, the token-refresh path of the native streaming coordinator. The function tries a direct manifest
 * re-fetch first (cheap, no browser navigation), falling back to a full page reload when the master URL itself has expired. The page-reload branch is heavily
 * entangled with real Chrome via Puppeteer (page.goto, installManifestInterceptor's CDP listener wiring) and is deferred to e2e coverage; the unit tests here
 * focus on the direct-fetch branch and the early-exit conditions (proxy stopped, page closed). The companion attemptNativeStreaming tests live in index.test.ts.
 */
import { afterEach, describe, test } from "node:test";
import { closePuppeteerStreamWssOnIdle, noop } from "../testing.helpers.ts";
import type { NativeProxy } from "./proxy.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { clearProbeCache } from "./probe.ts";
import { mock } from "node:test";
import { refreshNativeManifest } from "./index.ts";

/* closePuppeteerStreamWssOnIdle() performs its own dynamic import of puppeteer-stream, which starts a WebSocketServer; that server is what keeps the event loop
 * alive after every test resolves, and closePuppeteerStreamWssOnIdle() is also what closes it. We unref every Server handle and timers to drain handles
 * immediately. The same pattern is documented in streaming/lifecycle.test.ts and the companion index.test.ts.
 */
const originalSetTimeout = globalThis.setTimeout;

globalThis.setTimeout = ((handler: TimerHandler, timeout?: number, ...args: unknown[]): NodeJS.Timeout => {

  const timer = (originalSetTimeout as unknown as (h: TimerHandler, t?: number, ...a: unknown[]) => NodeJS.Timeout)(handler, timeout, ...args);

  timer.unref();

  return timer;
}) as unknown as typeof globalThis.setTimeout;

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* makeFakePage returns a Page stub exposing only the methods refreshNativeManifest exercises on a stopped/closed-page path. The non-reload branches do not call
 * page.goto, so this minimal stub is sufficient.
 */
function makeFakePage(closed = false): Page {

  return {

    isClosed: (): boolean => closed
  } as unknown as Page;
}

/* ProxyStubHooks captures the side effects that refreshNativeManifest applies to a proxy. Tests inspect these counters and captured values to assert that the
 * orchestrator called updateVariantUrl/updateAudioVariantUrl/setTokenRefreshTimer the expected number of times with the expected arguments.
 */
interface ProxyStubHooks {

  audioVariantUrl: string;

  isStopped: boolean;

  // The delay (in milliseconds) the most recently scheduled token-refresh timer was created with, or null when no refresh has been scheduled. Populated by the
  // setTokenRefreshTimer hook by looking the received timer up in scheduledTimerDelays. Tests use this to pin the refresh CADENCE - whether the reschedule aims at
  // the expiry boundary or degenerates into a tight MIN_REFRESH_DELAY poll.
  lastRefreshDelayMs: number | null;

  setTokenRefreshTimerCalls: number;

  variantUrl: string;
}

/* scheduledTimerDelays records the delay every setTimeout call was scheduled with, keyed by the timer handle it produced. The spyScheduledTimers helper installs a
 * globalThis.setTimeout spy that populates this map so the proxy stub can recover the delay of the timer handed to setTokenRefreshTimer. A WeakMap keeps the entries
 * garbage-collectable once the handles are cleared, and survives across mock.reset (which only restores the spy, not module state).
 */
const scheduledTimerDelays = new WeakMap<object, number>();

/* spyScheduledTimers replaces globalThis.setTimeout with a spy that records each call's delay against the timer it returns, then delegates to the real (unref'd)
 * implementation already installed at the top of this file. Tests that need to assert on the scheduled refresh delay install this spy; mock.reset in afterEach
 * restores the plain unref'd wrapper.
 */
function spyScheduledTimers(): void {

  const wrapped = globalThis.setTimeout;

  mock.method(globalThis, "setTimeout", ((handler: TimerHandler, timeout?: number, ...args: unknown[]): NodeJS.Timeout => {

    const timer = (wrapped as unknown as (h: TimerHandler, t?: number, ...a: unknown[]) => NodeJS.Timeout)(handler, timeout, ...args);

    scheduledTimerDelays.set(timer, timeout ?? 0);

    return timer;
  }) as unknown as typeof globalThis.setTimeout);
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

      // Recover the delay this timer was scheduled with (populated by spyScheduledTimers, if installed) so tests can pin the refresh cadence. Falls back to null
      // when the spy is not active for this test.
      hooks.lastRefreshDelayMs = scheduledTimerDelays.get(timer) ?? null;

      // Cancel the timer so the test does not leak it into the next case.
      clearTimeout(timer);
    },
    start: noop,
    stop: noop,
    updateAudioVariantUrl: (newUrl: string): void => {

      hooks.audioVariantUrl = newUrl;
    },
    updateVariantUrl: (newUrl: string): void => {

      hooks.variantUrl = newUrl;
    },

    [Symbol.dispose]: noop
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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: true, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

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
    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

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

  test("schedules a single boundary-targeted refresh, not a MIN_REFRESH_DELAY busy-loop, when the master token is inside the refresh margin", async () => {

    /* A master URL whose token expires inside the 5-minute refresh margin must resolve to a single reschedule aimed at the actual expiry boundary, not the
     * MIN_REFRESH_DELAY floor (30s), so the schedule never degenerates into a per-cycle re-probe of the still-valid master. With a master expiring in ~90s and a
     * variant carrying no token, the boundary is the master's 90s, so the reschedule must fire at ~90s and not at the 30s floor.
     */
    const expirySeconds = Math.floor(Date.now() / 1000) + 90;
    const masterUrl = "https://cdn.test/inside-margin-master.m3u8?exp=" + String(expirySeconds);

    makeFetchRouter({

      "https://cdn.test/inside-margin-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\ninside-margin-variant.m3u8\n", { status: 200 }),
      "https://cdn.test/inside-margin-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    spyScheduledTimers();

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("inside-margin-channel");

    const result = await refreshNativeManifest({

      channelName: "inside-margin-channel",
      masterUrl,
      page: makeFakePage(),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "inside-margin-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "direct refresh succeeds");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "exactly one refresh is scheduled - a single boundary timer, not a poll");
    assert.notEqual(hooks.lastRefreshDelayMs, null, "the reschedule recorded a delay");

    // The boundary is the master's ~90s expiry. Assert the reschedule fires near that boundary and well above the 30s MIN_REFRESH_DELAY floor that the old
    // busy-loop produced. A 2-second tolerance absorbs the wall-clock read inside scheduleTokenRefresh.
    assert.ok((hooks.lastRefreshDelayMs!) > 30000, "reschedule does NOT collapse to the MIN_REFRESH_DELAY busy-loop floor");
    assert.ok(Math.abs((hooks.lastRefreshDelayMs!) - 90000) <= 2000, "reschedule is aimed at the ~90s expiry boundary");
  });

  test("leads the boundary by TOKEN_REFRESH_MARGIN when the master token has comfortable margin", async () => {

    /* The comfortable-margin regime: when far more than the 5-minute margin remains, the single reschedule fires TOKEN_REFRESH_MARGIN (300s) before the boundary so
     * the fresh manifest is ready well ahead of expiry. With a master expiring in ~900s, the reschedule must fire at ~600s (900 - 300), confirming the margin lead
     * is still applied outside the busy-loop window.
     */
    const expirySeconds = Math.floor(Date.now() / 1000) + 900;
    const masterUrl = "https://cdn.test/comfortable-master.m3u8?exp=" + String(expirySeconds);

    makeFetchRouter({

      "https://cdn.test/comfortable-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\ncomfortable-variant.m3u8\n", { status: 200 }),
      "https://cdn.test/comfortable-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    spyScheduledTimers();

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("comfortable-channel");

    const result = await refreshNativeManifest({

      channelName: "comfortable-channel",
      masterUrl,
      page: makeFakePage(),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "comfortable-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "direct refresh succeeds");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "exactly one refresh scheduled");

    // 900s boundary minus the 300s margin lead -> ~600s. A 2-second tolerance absorbs the wall-clock read.
    assert.ok(Math.abs((hooks.lastRefreshDelayMs!) - 600000) <= 2000, "reschedule leads the boundary by TOKEN_REFRESH_MARGIN");
  });

  test("pins the refresh boundary to the variant expiry when the variant token expires before the master token", async () => {

    /* The variant URL the proxy polls rotates independently of the master and can expire first. The boundary must be the earlier of the two so the proxy never holds
     * a dead variant. With a master valid for ~900s but a variant expiring in ~120s, the boundary is the variant's 120s; the single reschedule must fire near 120s,
     * not near the master's 900s (or its 600s margin lead).
     */
    const masterExpirySeconds = Math.floor(Date.now() / 1000) + 900;
    const variantExpirySeconds = Math.floor(Date.now() / 1000) + 120;
    const masterUrl = "https://cdn.test/variant-bound-master.m3u8?exp=" + String(masterExpirySeconds);
    const variantPath = "variant-bound-variant.m3u8?exp=" + String(variantExpirySeconds);
    const variantUrl = "https://cdn.test/" + variantPath;

    makeFetchRouter({

      "https://cdn.test/variant-bound-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\n" + variantPath + "\n", { status: 200 }),
      "https://cdn.test/variant-bound-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    spyScheduledTimers();

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("variant-bound-channel");

    const result = await refreshNativeManifest({

      channelName: "variant-bound-channel",
      masterUrl,
      page: makeFakePage(),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "variant-bound-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "direct refresh succeeds");
    assert.equal(hooks.variantUrl, variantUrl, "proxy variant URL updated to the short-lived variant");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "exactly one refresh scheduled");

    // The 120s variant boundary is inside the margin, so no lead is applied and the reschedule fires at ~120s. A 2-second tolerance absorbs the wall-clock read.
    assert.ok(Math.abs((hooks.lastRefreshDelayMs!) - 120000) <= 2000, "reschedule is pinned to the earlier variant expiry, not the master expiry");
  });

  test("discards a direct-fetched variant whose token expires within MIN_USABLE_TOKEN_LIFETIME and falls through to page reload", async () => {

    /* The direct-fetch path parses the variant URL's token and rejects a variant that would expire almost immediately (within ~5s), since handing the proxy a
     * variant that dies on the next poll is worse than a page reload that mints a genuinely fresh token. Here the master fetches fine but the variant carries an
     * exp roughly 2 seconds out, inside the MIN_USABLE_TOKEN_LIFETIME floor. tryDirectManifestRefresh returns null, so the refresh falls through to the page-reload
     * strategy; with the page closed that path returns false and the proxy variant URL is never updated.
     */
    const variantExpirySeconds = Math.floor(Date.now() / 1000) + 2;
    const masterUrl = "https://cdn.test/near-expiry-master.m3u8";
    const variantPath = "near-expiry-variant.m3u8?exp=" + String(variantExpirySeconds);

    makeFetchRouter({

      "https://cdn.test/near-expiry-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\n" + variantPath + "\n", { status: 200 }),
      "https://cdn.test/near-expiry-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("near-expiry-channel");

    const result = await refreshNativeManifest({

      channelName: "near-expiry-channel",
      masterUrl,
      page: makeFakePage(true),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "near-expiry-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "near-expired variant discarded, closed page yields false");
    assert.equal(hooks.variantUrl, "", "proxy variant URL was NOT updated from the discarded direct fetch");
    assert.equal(hooks.setTokenRefreshTimerCalls, 0, "no refresh scheduled on the discard-then-fail path");
  });

  test("does not update the proxy and returns false when the proxy is stopped during the direct-fetch probe", async () => {

    /* Staleness guard: the direct-fetch probe is async, and the stream can be terminated while it runs. refreshNativeManifest re-checks proxy.isStopped() after the
     * probe resolves and before touching the proxy. Here the proxy starts running so the initial guard passes, then flips to stopped mid-probe (inside the variant
     * fetch). Because the post-probe isStopped check now observes true, the orchestrator must NOT call updateVariantUrl and must return false, leaving the proxy's
     * variant URL untouched.
     */
    const masterUrl = "https://cdn.test/stop-midprobe-master.m3u8";
    const variantUrl = "https://cdn.test/stop-midprobe-variant.m3u8";

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, setTokenRefreshTimerCalls: 0, variantUrl: "" };

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nstop-midprobe-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => {

        // The stream terminates while the direct-fetch probe is walking the variant playlist. Flipping the stub to stopped here means the post-probe isStopped
        // re-check inside refreshNativeManifest observes a stopped proxy on its next read.
        hooks.isStopped = true;

        return new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 });
      }
    });

    clearProbeCache("stop-midprobe-channel");

    const result = await refreshNativeManifest({

      channelName: "stop-midprobe-channel",
      masterUrl,
      page: makeFakePage(),
      proxy: makeFakeProxy(hooks),
      streamIdStr: "stop-midprobe-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "proxy stopped mid-probe yields false");
    assert.equal(hooks.variantUrl, "", "updateVariantUrl was NOT called after the mid-probe stop");
    assert.equal(hooks.setTokenRefreshTimerCalls, 0, "no refresh rescheduled after the mid-probe stop");
  });
});
