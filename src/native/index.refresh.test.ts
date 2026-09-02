/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.refresh.test.ts: Unit tests for refreshNativeManifest, the token-refresh path of the native streaming coordinator. The function tries a direct manifest
 * re-fetch first (cheap, no browser navigation), falling back to a full page reload when the master URL itself has expired. The page-reload branch is heavily
 * entangled with real Chrome via Puppeteer (page.goto, installManifestInterceptor's CDP listener wiring) and is deferred to e2e coverage; the unit tests here
 * focus on the direct-fetch branch and the early-exit conditions (proxy stopped, page closed). The companion attemptNativeStreaming tests live in index.test.ts.
 */
import type { PipelineShape, ProbeCacheIdentity } from "./probe.ts";
import { afterEach, describe, mock, test } from "node:test";
import { buildProbeCacheStamp, clearProbeCache, getCachedEncryption, probeManifest } from "./probe.ts";
import { closePuppeteerStreamWssOnIdle, noop } from "../testing.helpers.ts";
import type { ManifestInterceptionResult } from "../browser/manifestInterceptor.ts";
import type { NativeProxy } from "./proxy.ts";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import type { RefreshedFeedMetadata } from "./index.ts";
import assert from "node:assert/strict";
import { refreshNativeManifest } from "./index.ts";
import { subscribeToLogs } from "../utils/index.ts";

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
  // setTokenRefreshTimer hook by looking the received timer up in scheduledTimerDelays. Tests use this to assert the refresh CADENCE - whether the reschedule aims at
  // the expiry boundary or degenerates into a tight MIN_REFRESH_DELAY poll.
  lastRefreshDelayMs: number | null;

  // The compatibility envelope this stub reports as the running pipeline's, which the refresh probe selects within. Each test names the shape its own fixtures
  // describe: a stub claiming a topology its manifests do not serve would have every candidate skipped as ineligible, which is a different outcome from the one
  // the test means to assert.
  pipelineShape: PipelineShape;

  setTokenRefreshTimerCalls: number;

  variantUrl: string;
}

/* The two pipeline shapes this file's fixtures describe. Every manifest here serves MPEG-TS segments with no key tags, so the axis that actually varies between
 * tests is whether audio arrives as a separate rendition.
 */
const MUXED_TS_PIPELINE: PipelineShape = { container: "ts", encryption: "clear", separateAudio: false };
const SEPARATE_AUDIO_TS_PIPELINE: PipelineShape = { container: "ts", encryption: "clear", separateAudio: true };

// The third shape, used by the fixture whose candidate declares AES-128: a relay that decrypts as it goes, which is a different pipeline from a clear one.
const AES128_TS_PIPELINE: PipelineShape = { container: "ts", encryption: "aes128", separateAudio: false };

/* scheduledTimerDelays records the delay every setTimeout call was scheduled with, keyed by the timer handle it produced. The spyScheduledTimers helper installs a
 * globalThis.setTimeout spy that populates this map so the proxy stub can recover the delay of the timer handed to setTokenRefreshTimer. A WeakMap keeps the entries
 * garbage-collectable once the handles are cleared, and survives across mock.reset (which only restores the spy, not module state).
 */
const scheduledTimerDelays = new WeakMap<object, number>();

/* The callback of the most recent timer the spy observed. The proxy stub cancels every timer handed to it, so a scheduled refresh never fires on its own; a test
 * that wants to watch the NEXT cycle - the one a fired timer starts - invokes this callback itself. Only tests that install the spy read it, and each of those
 * schedules exactly once before reading, so the module-level lifetime is deterministic.
 */
let lastScheduledCallback: (() => void) | null = null;

/* spyScheduledTimers replaces globalThis.setTimeout with a spy that records each call's delay against the timer it returns, then delegates to the real (unref'd)
 * implementation already installed at the top of this file. Tests that need to assert on the scheduled refresh delay install this spy; mock.reset in afterEach
 * restores the plain unref'd wrapper.
 */
function spyScheduledTimers(): void {

  const wrapped = globalThis.setTimeout;

  lastScheduledCallback = null;

  mock.method(globalThis, "setTimeout", ((handler: TimerHandler, timeout?: number, ...args: unknown[]): NodeJS.Timeout => {

    const timer = (wrapped as unknown as (h: TimerHandler, t?: number, ...a: unknown[]) => NodeJS.Timeout)(handler, timeout, ...args);

    scheduledTimerDelays.set(timer, timeout ?? 0);

    if(typeof handler === "function") {

      lastScheduledCallback = handler as () => void;
    }

    return timer;
  }) as unknown as typeof globalThis.setTimeout);
}

/* makeFakeProxy returns a NativeProxy-shaped stub paired with mutable hooks. The hooks let tests observe what the orchestrator did to the proxy without poking at
 * the proxy's internal state. The setTokenRefreshTimer hook auto-cancels the supplied timer so tests do not leak handles into the next test.
 */
function makeFakeProxy(hooks: ProxyStubHooks): NativeProxy {

  /* The single-flight slot and the consecutive-failure count live inside the stub, the way the real proxy holds them, rather than on the hooks: no assertion reads
   * either one directly. What the tests watch is what the coordinator does with them - one master fetch for two concurrent callers, a retry armed at a delay the
   * count sizes - and those are already visible through the fetch router and the timer spy.
   */
  let pendingRefresh: Promise<boolean> | null = null;
  let refreshFailures = 0;

  return {

    clearRefreshFailures: (): void => {

      refreshFailures = 0;
    },
    getConsecutiveErrors: (): number => 0,
    getLastSegmentSize: (): null => null,
    getLastSegmentTime: (): number => 0,
    getPendingRefresh: (): Promise<boolean> | null => pendingRefresh,
    getPipelineShape: (): PipelineShape => hooks.pipelineShape,
    getSegmentIndex: (): number => 0,
    getStats: (): { fetchErrors: number; segmentsFetched: number; tokenRefreshes: number } => ({ fetchErrors: 0, segmentsFetched: 0, tokenRefreshes: 0 }),
    getTargetDuration: (): number => 6,
    hasErrored: (): boolean => false,
    isStopped: (): boolean => hooks.isStopped,
    noteRefreshFailure: (): number => ++refreshFailures,
    setPendingRefresh: (refresh: Promise<boolean> | null): void => {

      pendingRefresh = refresh;
    },
    setTokenRefreshTimer: (timer: ReturnType<typeof setTimeout>): void => {

      hooks.setTokenRefreshTimerCalls++;

      // Recover the delay this timer was scheduled with (populated by spyScheduledTimers, if installed) so tests can assert the refresh cadence. Falls back to null
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

/* The re-establishment capability every row that never reaches the reload strategy carries. Declining is the default for them: they either short-circuit
 * ahead of the strategy or settle on the direct fetch, so a capability resolving an interception would describe a path they do not take.
 */
const declineReestablishment = async (): Promise<Nullable<ManifestInterceptionResult>> => null;

/* A re-establishment capability paired with the count of times the orchestrator invoked it. The counter is what separates "the refresh reached the same outcome"
 * from "the refresh reached it through the capability", which is the whole point of the reload strategy now delegating instead of navigating itself.
 */
interface ReestablishStub {

  calls: number;
  reestablishManifest: () => Promise<Nullable<ManifestInterceptionResult>>;
}

/* Builds a counting capability stub that resolves whatever interception a test wants the reload strategy to receive.
 *
 * @param result - The interception the capability hands back, or null for a re-establishment that failed.
 * @returns The stub, whose calls field the test reads after the refresh settles.
 */
function makeReestablishStub(result: Nullable<ManifestInterceptionResult> = null): ReestablishStub {

  const stub: ReestablishStub = {

    calls: 0,
    reestablishManifest: async (): Promise<Nullable<ManifestInterceptionResult>> => {

      stub.calls++;

      return result;
    }
  };

  return stub;
}

/* Builds the probe-cache identity for a refresh test. Each test keeps its own channel key - the same key its clearProbeCache call addresses - because the cache
 * is addressed by identity, so a single shared identity would put every test in one slot and let one test's classification answer another's probe. The stamp
 * comes from the production builder over the configured channel URL these tests refresh against, never the master URL, whose token rotates per refresh.
 *
 * @param key - The channel key this test's entry lives under.
 * @param url - The configured binding URL the stamp is derived from.
 * @returns The identity to thread through refreshNativeManifest.
 */
function refreshIdentity(key: string, url = "https://example.test/channel"): ProbeCacheIdentity {

  return { key, stamp: buildProbeCacheStamp({ channelSelector: undefined, profile: undefined, url }) };
}

/* Runs an operation while collecting the warn-level lines the logger emits. A refresh failure's warning is deliberately the operator's message rather than the
 * caller's - nothing is returned to assert on - so the log emitter every subscriber reads is where a test observes whether it was written.
 *
 * @param operation - The refresh call to run.
 * @returns The operation's outcome alongside the warnings emitted while it ran.
 */
async function captureWarnings(operation: () => Promise<boolean>): Promise<{ outcome: boolean; warnings: string[] }> {

  const warnings: string[] = [];

  const unsubscribe = subscribeToLogs((entry) => {

    if(entry.level === "warn") {

      warnings.push(entry.message);
    }
  });

  try {

    return { outcome: await operation(), warnings };
  } finally {

    unsubscribe();
  }
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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: true, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    const result = await refreshNativeManifest({

      channelName: "stopped-channel",
      page: makeFakePage(),
      probeIdentity: refreshIdentity("stopped-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "stopped-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "stopped proxy short-circuits to false");
    assert.equal(fetchCalls, 0, "no fetches issued");
  });

  test("returns false when no masterUrl is provided and the page is closed (no recovery path available)", async () => {

    // Boundary: when the L2 recovery path runs (no masterUrl) and the page itself is closed, the function has nothing to do and returns false.
    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    const result = await refreshNativeManifest({

      channelName: "closed-page-channel",
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("closed-page-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("refresh-channel");

    const result = await refreshNativeManifest({

      channelName: "refresh-channel",
      masterUrl,
      page: makeFakePage(),
      probeIdentity: refreshIdentity("refresh-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
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
    const fetched: string[] = [];

    makeFetchRouter({

      [masterUrl]: () => {

        fetched.push(masterUrl);

        return new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nflip-variant.m3u8\n", { status: 200 });
      },
      [variantUrl]: () => {

        fetched.push(variantUrl);

        return new Response("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://x\"\n", { status: 200 });
      }
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("flip-channel");

    const result = await refreshNativeManifest({

      channelName: "flip-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("flip-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "flip-stream",
      url: "https://example.test/channel"
    });

    // The function falls through to page reload; the page is closed so it returns false.
    assert.equal(result, false, "DRM flip + closed page -> false");
    assert.equal(hooks.variantUrl, "", "proxy variant URL was NOT updated");

    // The candidate was fetched and read, so the refusal came from what its body declares rather than from an eligibility skip that never looked. Without this,
    // a stub whose shape disagreed with these fixtures would produce the same two observables above for an entirely different reason.
    assert.deepEqual(fetched, [ masterUrl, variantUrl ], "the master and its one candidate were both fetched");
  });

  test("returns false when the direct-fetch probe fails entirely (master 500) and the page is closed", async () => {

    // Negative test: master fetch fails, direct-fetch path returns null, function falls through to page reload, which short-circuits because the page is closed.
    const masterUrl = "https://cdn.test/refresh-fail-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("server error", { status: 500 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    const result = await refreshNativeManifest({

      channelName: "refresh-fail-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("refresh-fail-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: SEPARATE_AUDIO_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("refresh-dai-channel");

    const result = await refreshNativeManifest({

      channelName: "refresh-dai-channel",
      masterUrl,
      page: makeFakePage(),
      probeIdentity: refreshIdentity("refresh-dai-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("inside-margin-channel");

    const result = await refreshNativeManifest({

      channelName: "inside-margin-channel",
      masterUrl,
      page: makeFakePage(),
      probeIdentity: refreshIdentity("inside-margin-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("comfortable-channel");

    const result = await refreshNativeManifest({

      channelName: "comfortable-channel",
      masterUrl,
      page: makeFakePage(),
      probeIdentity: refreshIdentity("comfortable-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "comfortable-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "direct refresh succeeds");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "exactly one refresh scheduled");

    // 900s boundary minus the 300s margin lead -> ~600s. A 2-second tolerance absorbs the wall-clock read.
    assert.ok(Math.abs((hooks.lastRefreshDelayMs!) - 600000) <= 2000, "reschedule leads the boundary by TOKEN_REFRESH_MARGIN");
  });

  test("clamps a boundary beyond the platform timer ceiling to the largest delay a timer can carry", async () => {

    /* setTimeout carries its delay as a 32-bit signed millisecond count, and a delay past that ceiling wraps to a value the platform fires immediately - which
     * turns a distant expiry into a refresh cycle running at the speed of the network rather than a timer aimed weeks out. A token good for weeks must therefore
     * schedule at the ceiling, where the refresh fires early and re-derives the boundary from the URLs it holds.
     */

    // Mirrors MAX_TIMER_DELAY_MS in index.ts, which is module-private, so the assertion restates the same ceiling. The expiry sits roughly 34 days out, well past it.
    const MAX_TIMER_DELAY_MS = 2147483647;
    const expirySeconds = Math.floor(Date.now() / 1000) + 3000000;
    const masterUrl = "https://cdn.test/far-future-master.m3u8?exp=" + String(expirySeconds);

    makeFetchRouter({

      "https://cdn.test/far-future-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nfar-future-variant.m3u8?exp=" + String(expirySeconds) + "\n", { status: 200 }),
      "https://cdn.test/far-future-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    spyScheduledTimers();

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("far-future-channel");

    const result = await refreshNativeManifest({

      channelName: "far-future-channel",
      masterUrl,
      page: makeFakePage(),
      probeIdentity: refreshIdentity("far-future-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "far-future-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "direct refresh succeeds");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "exactly one refresh scheduled");
    assert.equal(hooks.lastRefreshDelayMs, MAX_TIMER_DELAY_MS, "the reschedule is clamped to the timer ceiling rather than overflowing it");
  });

  test("holds a boundary that has already passed at the floor delay instead of firing back-to-back", async () => {

    /* A boundary in the past computes a negative distance, and a timer armed with it fires at once: the refresh would re-probe, derive the same past boundary,
     * and re-arm immediately, spinning as fast as the network answers. The floor is what turns a past-due or imminent boundary into one paced retry instead. Here
     * the master's token expired a minute ago and the variant carries none, so the boundary is behind us and the schedule must sit exactly on the floor.
     */

    // Mirrors MIN_REFRESH_DELAY in index.ts, which is module-private.
    const MIN_REFRESH_DELAY = 30000;
    const expirySeconds = Math.floor(Date.now() / 1000) - 60;
    const masterUrl = "https://cdn.test/past-due-master.m3u8?exp=" + String(expirySeconds);

    makeFetchRouter({

      "https://cdn.test/past-due-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\npast-due-variant.m3u8\n", { status: 200 }),
      "https://cdn.test/past-due-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    spyScheduledTimers();

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("past-due-channel");

    const result = await refreshNativeManifest({

      channelName: "past-due-channel",
      masterUrl,
      page: makeFakePage(),
      probeIdentity: refreshIdentity("past-due-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "past-due-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "the refresh itself succeeds - a spent master token does not stop a direct fetch from answering");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "exactly one refresh scheduled");
    assert.equal(hooks.lastRefreshDelayMs, MIN_REFRESH_DELAY, "and it waits the floor delay rather than the past-due boundary's negative distance");
  });

  test("ties the refresh boundary to the variant expiry when the variant token expires before the master token", async () => {

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

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("variant-bound-channel");

    const result = await refreshNativeManifest({

      channelName: "variant-bound-channel",
      masterUrl,
      page: makeFakePage(),
      probeIdentity: refreshIdentity("variant-bound-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "variant-bound-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "direct refresh succeeds");
    assert.equal(hooks.variantUrl, variantUrl, "proxy variant URL updated to the short-lived variant");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "exactly one refresh scheduled");

    // The 120s variant boundary is inside the margin, so no lead is applied and the reschedule fires at ~120s. A 2-second tolerance absorbs the wall-clock read.
    assert.ok(Math.abs((hooks.lastRefreshDelayMs!) - 120000) <= 2000, "reschedule is tied to the earlier variant expiry, not the master expiry");
  });

  test("discards a direct-fetched variant whose token expires within MIN_USABLE_TOKEN_LIFETIME and falls through to page reload", async () => {

    /* The direct-fetch path parses the variant URL's token and rejects a variant that would expire almost immediately (within ~5s), since handing the proxy a
     * variant that dies on the next poll is worse than a page reload that mints a genuinely fresh token. Here the master fetches fine but the variant carries an
     * exp roughly 2 seconds out, inside the MIN_USABLE_TOKEN_LIFETIME floor. tryDirectManifestRefresh returns null, so the refresh falls through to the page-reload
     * strategy; with the page closed that path returns false and the proxy variant URL is never updated. The proxy is still live, so the failure arms the retry
     * that keeps the proactive chain alive, at the delay a first consecutive failure waits.
     */
    // Mirrors MIN_REFRESH_DELAY in index.ts, which is module-private. It is the delay a first consecutive failure retries at.
    const MIN_REFRESH_DELAY = 30000;
    const variantExpirySeconds = Math.floor(Date.now() / 1000) + 2;
    const masterUrl = "https://cdn.test/near-expiry-master.m3u8";
    const variantPath = "near-expiry-variant.m3u8?exp=" + String(variantExpirySeconds);
    const fetched: string[] = [];

    makeFetchRouter({

      "https://cdn.test/near-expiry-master.m3u8": (url) => {

        fetched.push(url);

        return new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\n" + variantPath + "\n", { status: 200 });
      },
      "https://cdn.test/near-expiry-variant.m3u8": (url) => {

        fetched.push(url);

        return new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 });
      }
    });

    spyScheduledTimers();

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("near-expiry-channel");

    const result = await refreshNativeManifest({

      channelName: "near-expiry-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("near-expiry-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "near-expiry-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "near-expired variant discarded, closed page yields false");
    assert.equal(hooks.variantUrl, "", "proxy variant URL was NOT updated from the discarded direct fetch");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "the failed refresh arms its retry");
    assert.equal(hooks.lastRefreshDelayMs, MIN_REFRESH_DELAY, "at the floor delay a first consecutive failure waits");

    // The candidate was fetched and its feed resolved, so the discard came from the token lifetime the fixture encodes rather than from an eligibility skip that
    // never reached it - every observable above would otherwise read the same for a stub whose shape disagreed with these fixtures.
    assert.deepEqual(fetched, [ masterUrl, "https://cdn.test/" + variantPath ], "the master and its one candidate were both fetched");
  });

  test("does not update the proxy and returns false when the proxy is stopped during the direct-fetch probe", async () => {

    /* Staleness guard: the direct-fetch probe is async, and the stream can be terminated while it runs. refreshNativeManifest re-checks proxy.isStopped() after the
     * probe resolves and before touching the proxy. Here the proxy starts running so the initial guard passes, then flips to stopped mid-probe (inside the variant
     * fetch). Because the post-probe isStopped check now observes true, the orchestrator must NOT call updateVariantUrl and must return false, leaving the proxy's
     * variant URL untouched.
     */
    const masterUrl = "https://cdn.test/stop-midprobe-master.m3u8";
    const variantUrl = "https://cdn.test/stop-midprobe-variant.m3u8";

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

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
      probeIdentity: refreshIdentity("stop-midprobe-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "stop-midprobe-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "proxy stopped mid-probe yields false");
    assert.equal(hooks.variantUrl, "", "updateVariantUrl was NOT called after the mid-probe stop");
    assert.equal(hooks.setTokenRefreshTimerCalls, 0, "no refresh rescheduled after the mid-probe stop");
  });

  test("falls back to the healthy sibling when the refreshed master's top variant is broken", async () => {

    /* A refresh runs the same ranked walk a tune runs, narrowed to what the running pipeline can absorb, so a master whose top variant is broken still refreshes
     * through the healthy sibling beneath it rather than failing outright. The sibling here serves a clear MPEG-TS playlist with muxed audio - the shape this
     * stub's pipeline reports - so it is eligible, and binding it is the whole point: the alternative would strand the stream on a dying token because one rung
     * of the ladder went bad. The page is closed, so the page-reload strategy short-circuits and the direct-fetch outcome is what the return value reports.
     */
    const masterUrl = "https://cdn.test/held-master.m3u8";
    const topUrl = "https://cdn.test/held-top.m3u8";
    const siblingUrl = "https://cdn.test/held-sibling.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=4000000",
        "held-top.m3u8",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000",
        "held-sibling.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [siblingUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 }),
      [topUrl]: () => new Response("server error", { status: 500 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("held-channel");

    const result = await refreshNativeManifest({

      channelName: "held-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("held-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "held-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "the refresh succeeds through the sibling beneath the broken top variant");
    assert.equal(hooks.variantUrl, siblingUrl, "the proxy is bound to the sibling the walk selected");
    assert.equal(hooks.audioVariantUrl, "", "and no audio URL is applied, since this pipeline's audio is muxed");

    // Neither of these fixture URLs carries an expiry token, so the boundary computation finds nothing to aim at and no timer is armed. The cadence tests above
    // own that behavior; what this assertion owns is which variant the walk binds.
    assert.equal(hooks.setTokenRefreshTimerCalls, 0, "no boundary exists to schedule against on these tokenless fixtures");
  });

  test("refreshes against a one-segment window, since static-playlist rejection is a tune-admission decision only", async () => {

    /* Tune admission asks whether a playlist is the channel at all; a refresh asks only for fresh URLs describing a feed the proxy is already relaying. So the
     * refresh probe never opts into static-playlist rejection, and a momentarily thin window - one segment at the instant the re-probe lands, which a live edge
     * can present after a discontinuity or a short producer stall - must not tear down a running stream. Here the refreshed variant carries a single segment and
     * the refresh succeeds exactly as it would with a full window.
     */
    const expirySeconds = Math.floor(Date.now() / 1000) + 600;
    const masterUrl = "https://cdn.test/thin-window-master.m3u8?exp=" + String(expirySeconds);
    const variantUrl = "https://cdn.test/thin-window-variant.m3u8?exp=" + String(expirySeconds);

    makeFetchRouter({

      "https://cdn.test/thin-window-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nthin-window-variant.m3u8?exp=" + String(expirySeconds) + "\n", { status: 200 }),
      "https://cdn.test/thin-window-variant.m3u8": () => new Response("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6,\nseg0.ts\n", { status: 200 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("thin-window-channel");

    const result = await refreshNativeManifest({

      channelName: "thin-window-channel",
      masterUrl,
      page: makeFakePage(),
      probeIdentity: refreshIdentity("thin-window-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "thin-window-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "the refresh succeeds against a one-segment window");
    assert.equal(hooks.variantUrl, variantUrl, "the proxy still receives the refreshed variant URL");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "and the next refresh is still scheduled");
  });

  test("records the refreshed classification under the threaded identity, not one derived from the master URL", async () => {

    /* The refresh path is where a probe is furthest from the tune that established the stream: the only URL in scope is the master, whose token rotates on
     * every refresh, and stamping with it would mint a new cache slot each time - the cache would never answer, and the entry it did write would describe
     * nothing durable. So the refresh probes under the stream's own identity, stamped from the configured binding. The two-sided assertion is what gives this
     * assertion teeth: the classification is readable under the threaded identity AND absent under an identity stamped from the master URL.
     */
    const expirySeconds = Math.floor(Date.now() / 1000) + 600;
    const masterUrl = "https://cdn.test/stamp-master.m3u8?token=rotates-every-refresh&exp=" + String(expirySeconds);
    const configuredUrl = "https://example.test/stamp-channel";

    makeFetchRouter({

      "https://cdn.test/stamp-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nstamp-variant.m3u8?exp=" + String(expirySeconds) + "\n", { status: 200 }),
      "https://cdn.test/stamp-variant.m3u8": () => new Response("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6,\nseg0.ts\n#EXTINF:6,\nseg1.ts\n", { status: 200 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };
    const identity = refreshIdentity("stamp-channel", configuredUrl);

    clearProbeCache("stamp-channel");

    const result = await refreshNativeManifest({

      channelName: "stamp-channel",
      masterUrl,
      page: makeFakePage(),
      probeIdentity: identity,
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "stamp-stream",
      url: configuredUrl
    });

    assert.equal(result, true, "the direct-fetch refresh succeeds");
    assert.equal(getCachedEncryption(identity), "clear", "the classification is readable under the identity the refresh was threaded with");
    assert.equal(getCachedEncryption(refreshIdentity("stamp-channel", masterUrl)), null,
      "and is absent under an identity stamped from the master URL, which the refresh must never stamp with");
  });

  test("declines a refreshed master that offers no variant matching the pipeline's audio topology, applying nothing", async () => {

    /* A pipeline whose audio is a separate rendition polls two manifests, so a variant carrying muxed audio cannot serve it: applying such a feed would swap the
     * video URL onto a new CDN session while the audio kept polling the session it is already on, which is a half-refresh that plays until that session's token
     * expires. Selection therefore
     * skips a candidate whose topology differs - and skips it without a fetch, since the master body alone answers the question - so a master offering nothing
     * else leaves the refresh with nothing to apply. The page is closed, so the reload strategy short-circuits and the direct-fetch outcome is what is reported.
     */
    const masterUrl = "https://cdn.test/topology-master.m3u8";
    const variantUrl = "https://cdn.test/topology-muxed.m3u8";
    const fetched: string[] = [];

    makeFetchRouter({

      [masterUrl]: () => {

        fetched.push(masterUrl);

        return new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=3000000\ntopology-muxed.m3u8\n", { status: 200 });
      },
      [variantUrl]: () => {

        fetched.push(variantUrl);

        return new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 });
      }
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: SEPARATE_AUDIO_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("topology-channel");

    const result = await refreshNativeManifest({

      channelName: "topology-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("topology-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "topology-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "a master with no compatible variant refreshes nothing");
    assert.equal(hooks.variantUrl, "", "the video URL is untouched - no half-application");
    assert.equal(hooks.audioVariantUrl, "", "and neither is the audio URL");
    assert.deepEqual(fetched, [masterUrl], "the ineligible candidate was never fetched - the master body alone ruled it out");
  });

  test("declines a candidate whose declared key turns out to be unreachable, which only the finished classification can see", async () => {

    /* The walk reads a candidate's key tags without fetching the key, which is what keeps passing one over cheap - but it also means an AES-128 declaration
     * clears the walk on the strength of the tag alone. Whether that key can actually be fetched is settled afterwards, by the classification that ends the
     * probe, and a key answering 403 leaves a feed no decrypting relay can serve. So the check on the finished classification is what declines here...it is the
     * one axis the walk had no way to see, and the reason the guarantee is made on the finished feed rather than on the walk's own reading.
     */
    const masterUrl = "https://cdn.test/deadkey-master.m3u8";
    const variantUrl = "https://cdn.test/deadkey-variant.m3u8";
    const keyUrl = "https://cdn.test/deadkey.bin";
    const fetched: string[] = [];

    makeFetchRouter({

      [keyUrl]: () => {

        fetched.push(keyUrl);

        return new Response("forbidden", { status: 403 });
      },
      [masterUrl]: () => {

        fetched.push(masterUrl);

        return new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\ndeadkey-variant.m3u8\n", { status: 200 });
      },
      [variantUrl]: () => {

        fetched.push(variantUrl);

        return new Response("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyUrl + "\"\n#EXTINF:2,\nseg.ts\n", { status: 200 });
      }
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: AES128_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("deadkey-channel");

    // The page is closed, so the reload strategy short-circuits and the direct-fetch outcome is what the return value reports.
    const result = await refreshNativeManifest({

      channelName: "deadkey-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("deadkey-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "deadkey-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "a feed whose encryption ends up unserviceable is declined, not applied");
    assert.equal(hooks.variantUrl, "", "the video URL is untouched");
    assert.equal(hooks.audioVariantUrl, "", "and so is the audio URL");
    assert.deepEqual(fetched, [ masterUrl, variantUrl, keyUrl ], "the candidate cleared the walk and its key was fetched - the decline came from that answer");
  });

  test("hands the streaming layer the refreshed feed's quality facts", async () => {

    /* A refresh re-runs selection, so the rung of the ladder it lands on can differ from the one the tune bound. The native layer does not write the registry -
     * it reports upward through this callback, exactly as it reports proxy errors - and the caller's closure records the bandwidth, codec, and resolution the
     * stream is now actually serving.
     */
    const expirySeconds = Math.floor(Date.now() / 1000) + 600;
    const masterUrl = "https://cdn.test/quality-master.m3u8?exp=" + String(expirySeconds);
    const applied: RefreshedFeedMetadata[] = [];

    makeFetchRouter({

      "https://cdn.test/quality-master.m3u8": () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080,CODECS=\"avc1.640028,mp4a.40.2\"",
        "quality-variant.m3u8?exp=" + String(expirySeconds),
        ""
      ].join("\n"), { status: 200 }),
      "https://cdn.test/quality-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("quality-channel");

    const result = await refreshNativeManifest({

      channelName: "quality-channel",
      masterUrl,
      onFeedApplied: (metadata) => {

        applied.push(metadata);
      },
      page: makeFakePage(),
      probeIdentity: refreshIdentity("quality-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "quality-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "the direct-fetch refresh succeeds");
    assert.equal(applied.length, 1, "the quality report fires exactly once for one applied feed");
    assert.deepEqual(applied[0], { bandwidth: 5000000, codec: "H264", resolution: "1920x1080" }, "carrying the bound variant's declared quality");
  });

  test("keeps a refresh successful when the quality report throws", async () => {

    /* The quality report crosses into the streaming layer, whose closure this code does not own. A throw there must not undo an application that has already
     * happened: the URLs are swapped, the stream is playing on fresh tokens, and reporting the outcome as a failure would re-arm a retry against a refresh that
     * worked. The stale status metadata is the smaller harm, so the throw is warned about and the cycle carries on to its reschedule.
     */
    const expirySeconds = Math.floor(Date.now() / 1000) + 600;
    const masterUrl = "https://cdn.test/throwing-master.m3u8?exp=" + String(expirySeconds);
    const variantUrl = "https://cdn.test/throwing-variant.m3u8?exp=" + String(expirySeconds);

    makeFetchRouter({

      "https://cdn.test/throwing-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nthrowing-variant.m3u8?exp=" + String(expirySeconds) + "\n", { status: 200 }),
      "https://cdn.test/throwing-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("throwing-channel");

    const result = await refreshNativeManifest({

      channelName: "throwing-channel",
      masterUrl,
      onFeedApplied: () => {

        throw new Error("the streaming layer's closure failed");
      },
      page: makeFakePage(),
      probeIdentity: refreshIdentity("throwing-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "throwing-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "the refresh still reports the success it achieved");
    assert.equal(hooks.variantUrl, variantUrl, "the URL swap stands");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "and the next refresh is still scheduled");
  });

  test("declines a refresh whose cached classification is DRM, leaving the entry readable for the next tune", async () => {

    /* The probe cache answers a DRM channel without touching the network, which is exactly right for a tune deciding whether to try native streaming at all. A
     * refresh asking the same question is in a different position: its proxy is already relaying a clear stream, and the cache's answer describes a feed no
     * running pipeline can absorb. So the constrained probe declines rather than handing back the sentinel, whose empty variant URL would read as a successful
     * refresh. Declining is not invalidating: the classification is still the channel's, so it stays where it is and the next unconstrained probe still reads it.
     */
    const masterUrl = "https://cdn.test/cached-drm.m3u8";
    const identity = refreshIdentity("cached-drm-channel");
    let masterFetches = 0;

    makeFetchRouter({

      [masterUrl]: () => {

        masterFetches++;

        return new Response("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://x\"\n#EXTINF:6,\nseg0.ts\n", { status: 200 });
      }
    });

    clearProbeCache("cached-drm-channel");

    // Seed the cache the way the field does: an ordinary probe classifies the channel and records it.
    const seeded = await probeManifest(masterUrl, identity);

    assert.equal(seeded?.encryption, "drm", "the seeding probe classified the channel as DRM");
    assert.equal(masterFetches, 1, "and paid one fetch to do it");

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    const result = await refreshNativeManifest({

      channelName: "cached-drm-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: identity,
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "cached-drm-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "the refresh declines rather than applying the cache sentinel");
    assert.equal(hooks.variantUrl, "", "the proxy's URL is untouched - the sentinel carries none to apply");
    assert.equal(hooks.audioVariantUrl, "", "and no audio URL is applied either");

    // The decline left the entry alone, which is the contract: an unconstrained probe still short-circuits on it, spending no fetch to do so.
    assert.equal(getCachedEncryption(identity), "drm", "the classification survives the decline");

    const afterwards = await probeManifest(masterUrl, identity);

    assert.equal(afterwards?.encryption, "drm", "an unconstrained probe still reads the entry");
    assert.equal(masterFetches, 1, "and reads it from the cache, without going back to the network");
  });

  test("carries the quality report through a fired refresh timer into the next cycle", async () => {

    /* Each refresh schedules the next one, so the callback the caller supplied has to survive every hop of that chain rather than only the first. The chain's
     * weak point is the timer callback, which rebuilds the options for the cycle it starts: a member dropped there would go unnoticed, since the stream keeps
     * refreshing perfectly well while its reported quality quietly freezes at whatever the tune bound. This assertion fires the scheduled timer and watches the second
     * cycle report.
     */
    const expirySeconds = Math.floor(Date.now() / 1000) + 600;
    const masterUrl = "https://cdn.test/chained-master.m3u8?exp=" + String(expirySeconds);
    const applied: RefreshedFeedMetadata[] = [];
    const { promise: secondCycleReported, resolve: reportSecondCycle } = Promise.withResolvers<RefreshedFeedMetadata>();

    makeFetchRouter({

      "https://cdn.test/chained-master.m3u8":
        () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=2000000\nchained-variant.m3u8?exp=" + String(expirySeconds) + "\n", { status: 200 }),
      "https://cdn.test/chained-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    spyScheduledTimers();

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("chained-channel");

    const result = await refreshNativeManifest({

      channelName: "chained-channel",
      masterUrl,
      onFeedApplied: (metadata) => {

        applied.push(metadata);

        if(applied.length === 2) {

          reportSecondCycle(metadata);
        }
      },
      page: makeFakePage(),
      probeIdentity: refreshIdentity("chained-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "chained-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, true, "the first cycle succeeds");
    assert.equal(applied.length, 1, "and reports once");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "having armed the timer that starts the next cycle");
    assert.notEqual(lastScheduledCallback, null, "the spy captured that timer's callback");

    // Fire the timer the way the platform would, then wait for the cycle it starts to report.
    lastScheduledCallback?.();

    const second = await secondCycleReported;

    assert.equal(applied.length, 2, "the fired timer's cycle reports too");
    assert.deepEqual(second, { bandwidth: 2000000, codec: null, resolution: null }, "carrying the second cycle's own feed");
  });

  test("shares one attempt between concurrent callers and gives both its real outcome", async () => {

    /* The proactive timer and the monitor's recovery both reach the same proxy, and within one cycle they can arrive together. Two attempts running at once
     * would race the same browser page's navigation, so the second caller joins the attempt already in flight instead of starting a rival. What it joins is the
     * real attempt, not a stand-in outcome: the monitor decides whether to escalate on the boolean it reads back, so a caller told "someone else is handling it"
     * would be steering on a fiction. The gate below holds the first attempt's master fetch open until the second caller has arrived.
     */
    const expirySeconds = Math.floor(Date.now() / 1000) + 600;
    const masterUrl = "https://cdn.test/shared-master.m3u8?exp=" + String(expirySeconds);
    const gate = Promise.withResolvers<string>();
    let masterFetches = 0;

    makeFetchRouter({

      "https://cdn.test/shared-master.m3u8": async () => {

        masterFetches++;

        return new Response(await gate.promise, { status: 200 });
      },
      "https://cdn.test/shared-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("shared-channel");

    const options = {

      channelName: "shared-channel",
      masterUrl,
      page: makeFakePage(),
      probeIdentity: refreshIdentity("shared-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "shared-stream",
      url: "https://example.test/channel"
    };

    const first = refreshNativeManifest(options);
    const second = refreshNativeManifest(options);

    gate.resolve("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nshared-variant.m3u8?exp=" + String(expirySeconds) + "\n");

    const [ firstOutcome, secondOutcome ] = await Promise.all([ first, second ]);

    assert.equal(firstOutcome, true, "the attempt that ran succeeded");
    assert.equal(secondOutcome, true, "and the caller that joined it reads the same outcome");
    assert.equal(masterFetches, 1, "one attempt means one master fetch, not two racing the same page");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "and one reschedule, since only one refresh actually happened");
  });

  test("warns and re-arms with a doubling delay when a refresh fails", async () => {

    /* A refresh that fails takes the proactive chain down with it: the schedule that would have armed the next one is only reached on success. Without a re-arm,
     * one transient failure leaves a stream with no proactive refresh at all until its tokens die. The retry doubles per consecutive failure so a service that
     * is briefly unreachable is retried promptly while one that is genuinely gone is not hammered.
     */

    // Mirrors MIN_REFRESH_DELAY in index.ts, which is module-private, so the assertion restates the same floor.
    const MIN_REFRESH_DELAY = 30000;
    const masterUrl = "https://cdn.test/rearm-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("server error", { status: 500 })
    });

    spyScheduledTimers();

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("rearm-channel");

    // The page is closed, so the reload strategy short-circuits and the whole refresh reports failure.
    const options = {

      channelName: "rearm-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("rearm-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "rearm-stream",
      url: "https://example.test/channel"
    };

    const firstFailure = await captureWarnings(() => refreshNativeManifest(options));

    assert.equal(firstFailure.outcome, false, "the refresh failed");
    assert.equal(firstFailure.warnings.length, 1, "and said so once, at a level an operator sees");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "exactly one retry is armed");
    assert.equal(hooks.lastRefreshDelayMs, MIN_REFRESH_DELAY, "the first retry waits the floor delay");

    const secondFailure = await captureWarnings(() => refreshNativeManifest(options));

    assert.equal(secondFailure.outcome, false, "the second attempt fails too");
    assert.equal(hooks.setTokenRefreshTimerCalls, 2, "and arms its own retry");
    assert.equal(hooks.lastRefreshDelayMs, MIN_REFRESH_DELAY * 2, "which waits twice as long as the first");
  });

  test("stays silent and arms nothing when a refresh fails on a stopped proxy", async () => {

    /* An ordinary stream stop interrupts whatever refresh was in flight, and that is not a fault: there is no stream left to warn about and nothing left to
     * retry for. The failure handling is gated on the proxy still being alive, so a stopped one exits exactly as quietly as it always has.
     */
    const masterUrl = "https://cdn.test/stopped-rearm-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("server error", { status: 500 })
    });

    spyScheduledTimers();

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: true, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("stopped-rearm-channel");

    const { outcome, warnings } = await captureWarnings(() => refreshNativeManifest({

      channelName: "stopped-rearm-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("stopped-rearm-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "stopped-rearm-stream",
      url: "https://example.test/channel"
    }));

    assert.equal(outcome, false, "a stopped proxy still reports failure to its caller");
    assert.deepEqual(warnings, [], "but says nothing to the operator about a stream that is gone");
    assert.equal(hooks.setTokenRefreshTimerCalls, 0, "and arms no retry for it");
  });

  test("restarts the backoff at the floor after a refresh succeeds", async () => {

    /* The backoff describes a run of consecutive failures, so a success ends the run. Without the reset, a stream that stumbled once early on would carry that
     * escalation for the rest of its life and answer its next stumble - hours later, unrelated - with an inflated delay.
     */
    const MIN_REFRESH_DELAY = 30000;
    const expirySeconds = Math.floor(Date.now() / 1000) + 600;
    const masterUrl = "https://cdn.test/reset-master.m3u8?exp=" + String(expirySeconds);
    let cycle = 0;

    makeFetchRouter({

      // Fail, then succeed, then fail: the third cycle is the one under test, and it must not inherit anything from the first.
      "https://cdn.test/reset-master.m3u8": () => {

        cycle++;

        return (cycle === 2) ?
          new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nreset-variant.m3u8?exp=" + String(expirySeconds) + "\n", { status: 200 }) :
          new Response("server error", { status: 500 });
      },
      "https://cdn.test/reset-variant.m3u8": () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    spyScheduledTimers();

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    clearProbeCache("reset-channel");

    const options = {

      channelName: "reset-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("reset-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: declineReestablishment,
      streamIdStr: "reset-stream",
      url: "https://example.test/channel"
    };

    // Each cycle's delay is read into its own snapshot before it is asserted on, so the three assertions compare three separate readings rather than re-reading
    // one mutable field the compiler has already narrowed.
    assert.equal(await refreshNativeManifest(options), false, "the first cycle fails");

    const firstRetryDelay = hooks.lastRefreshDelayMs;

    assert.equal(await refreshNativeManifest(options), true, "the second cycle succeeds");

    const boundaryDelay = hooks.lastRefreshDelayMs;

    assert.equal(await refreshNativeManifest(options), false, "the third cycle fails again");

    const secondRetryDelay = hooks.lastRefreshDelayMs;

    assert.equal(firstRetryDelay, MIN_REFRESH_DELAY, "the first failure armed its retry at the floor delay");
    assert.ok((boundaryDelay ?? 0) > MIN_REFRESH_DELAY, "the success replaced that retry with a boundary-aimed schedule");
    assert.equal(secondRetryDelay, MIN_REFRESH_DELAY, "and the failure after it starts over at the floor, the success having cleared the run");
  });

  test("resolves rather than rejects when arming the retry itself throws", async () => {

    /* Both production callers hand this promise to void or to a bare await, so a rejection escaping here would be unhandled - and it would take the retry chain
     * with it, which is the one thing the failure handling exists to protect. The bookkeeping therefore runs inside the same guard as the attempt: a throw in it
     * is reported and the settled outcome stands.
     */
    const masterUrl = "https://cdn.test/throwing-timer-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("server error", { status: 500 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };

    // A proxy whose timer slot refuses the handle the re-arm hands it - the failure branch's last step, and the one furthest from the caller.
    const proxy: NativeProxy = { ...makeFakeProxy(hooks), setTokenRefreshTimer: (): void => {

      throw new Error("the timer slot refused the handle");
    } };

    clearProbeCache("throwing-timer-channel");

    const { outcome, warnings } = await captureWarnings(() => refreshNativeManifest({

      channelName: "throwing-timer-channel",
      masterUrl,
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("throwing-timer-channel"),
      proxy,
      reestablishManifest: declineReestablishment,
      streamIdStr: "throwing-timer-stream",
      url: "https://example.test/channel"
    }));

    assert.equal(outcome, false, "the promise settles on the outcome the attempt reached");
    assert.equal(warnings.length, 2, "with the failure warning and the one naming the bookkeeping that threw");
  });

  test("re-establishes the channel through the injected capability and applies the manifest it returns", async () => {

    /* The reload strategy's whole contract: it must acquire its manifest by re-running the stream's own tune, not by navigating the page itself. Omitting
     * masterUrl sends the refresh straight past the direct fetch, so the capability is the only thing that can produce the URL the probe then reads. The call
     * counter is what distinguishes this from a refresh that reached the same outcome some other way.
     */
    const masterUrl = "https://cdn.test/reestablish-master.m3u8";
    const variantUrl = "https://cdn.test/reestablish-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nreestablish-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };
    const capability = makeReestablishStub({ manifestUrl: masterUrl, selectedKind: "master" });

    clearProbeCache("reestablish-channel");

    const result = await refreshNativeManifest({

      channelName: "reestablish-channel",
      page: makeFakePage(),
      probeIdentity: refreshIdentity("reestablish-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: capability.reestablishManifest,
      streamIdStr: "reestablish-stream",
      url: "https://example.test/channel"
    });

    assert.equal(capability.calls, 1, "the reload strategy went through the capability");
    assert.equal(result, true, "and the refresh succeeded on what it returned");
    assert.equal(hooks.variantUrl, variantUrl, "the proxy was rebound to the variant the re-established manifest declares");
  });

  test("fails the refresh without touching the proxy when the re-establishment declines", async () => {

    // A null from the capability covers every way the channel could not be re-established - navigation, initialization, or a verifier rejection - and each of
    // them must leave the running proxy exactly as it was rather than half-applying anything.
    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };
    const capability = makeReestablishStub();

    let fetchCalls = 0;

    mock.method(globalThis, "fetch", async () => {

      fetchCalls++;

      return new Response("should not be reached", { status: 500 });
    });

    const result = await refreshNativeManifest({

      channelName: "declined-channel",
      page: makeFakePage(),
      probeIdentity: refreshIdentity("declined-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: capability.reestablishManifest,
      streamIdStr: "declined-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "a declined re-establishment fails the refresh");
    assert.equal(capability.calls, 1, "the capability is invoked exactly once per attempt");
    assert.equal(hooks.variantUrl, "", "the proxy's variant URL is untouched");
    assert.equal(hooks.audioVariantUrl, "", "and so is its audio variant URL");
    assert.equal(hooks.setTokenRefreshTimerCalls, 1, "with only the failure re-arm's timer armed");
    assert.equal(fetchCalls, 0, "nothing was probed, because there was no manifest to probe");
  });

  test("declines a closed page before invoking the capability, so no tune runs against a dead page", async () => {

    // The page-closed guard sits ahead of the re-establishment for a reason: every primitive the capability composes would throw on a closed page, and the
    // counter is what asserts the ordering rather than merely the outcome.
    const hooks: ProxyStubHooks = { audioVariantUrl: "", isStopped: false, lastRefreshDelayMs: null, pipelineShape: MUXED_TS_PIPELINE,
      setTokenRefreshTimerCalls: 0, variantUrl: "" };
    const capability = makeReestablishStub({ manifestUrl: "https://cdn.test/never-read.m3u8", selectedKind: "master" });

    const result = await refreshNativeManifest({

      channelName: "closed-before-capability-channel",
      page: makeFakePage(true),
      probeIdentity: refreshIdentity("closed-before-capability-channel"),
      proxy: makeFakeProxy(hooks),
      reestablishManifest: capability.reestablishManifest,
      streamIdStr: "closed-before-capability-stream",
      url: "https://example.test/channel"
    });

    assert.equal(result, false, "a closed page fails the refresh");
    assert.equal(capability.calls, 0, "and the capability was never invoked");
  });
});
