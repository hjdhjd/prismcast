/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for attemptNativeStreaming, the native streaming coordinator's orchestrator in index.ts. attemptNativeStreaming awaits manifest
 * interception, probes for DRM, and constructs the native proxy. The single describe block in this file covers that orchestrator's branches. The other export,
 * refreshNativeManifest (the token-refresh path that re-probes the master URL or reloads the page), has its own coverage in index.refresh.test.ts. The
 * orchestration branches exercised here are driven by a directly-injected interceptionPromise (resolving to null vs a valid manifest, or rejecting), mocked
 * globalThis.fetch responses for the master/variant/key URLs, and a minimal page stub.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { buildProbeCacheStamp, clearProbeCache } from "./probe.ts";
import { closePuppeteerStreamWssOnIdle, noop } from "../testing.helpers.ts";
import type { AttemptNativeStreamingOptions } from "./index.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { attemptNativeStreaming } from "./index.ts";

/* eslint-disable sort-keys -- fixture route maps are ordered by HLS resolution chain (master -> variant -> key), not alphabetical key strings, so the logical
 * dependency direction is visible to readers. */

/* Sources of post-test handle leakage need draining for this file:
 *
 * 1. puppeteer-stream's PuppeteerStream module starts a WebSocketServer at import time. index.ts pulls in browser/manifestInterceptor.ts which in turn pulls in
 *    browser/index.ts and triggers that server creation, which keeps the event loop alive after every test resolves.
 *
 * 2. attemptNativeStreaming's interception-await timeout does not leak: cancellableTimeout owns the underlying setTimeout, and the orchestrator clears it in
 *    finally via timeout.cancel() when interceptionPromise wins the race. The residual handle leakage drained here is the per-stream token-refresh timer scheduled
 *    by scheduleTokenRefresh (cancelled only on proxy.stop()) plus the puppeteer-stream WebSocketServer from point 1.
 *
 * Strategy: monkey-patch globalThis.setTimeout to call unref() on every Timeout it produces inside this test file. Production code in attemptNativeStreaming has
 * no contract that timers stay reffed; the unref makes the timer non-blocking for event-loop draining without changing its callback firing time. Combined with
 * the Server-handle scan from streaming/lifecycle.test.ts, this lets the file exit cleanly the moment the last test resolves.
 */
const originalSetTimeout = globalThis.setTimeout;

// We replace globalThis.setTimeout with a wrapper that unrefs every timer it produces. Test code paths that rely on setTimeout keeping the loop alive (none here)
// would observe early exit, but the orchestration tests in this file all complete via direct fetch/promise resolution, not by waiting for a timer to fire. The
// double cast through unknown reaches across the wider typeof setTimeout signature (which carries the legacy __promisify__ field) without re-implementing it.
globalThis.setTimeout = ((handler: TimerHandler, timeout?: number, ...args: unknown[]): NodeJS.Timeout => {

  const timer = (originalSetTimeout as unknown as (h: TimerHandler, t?: number, ...a: unknown[]) => NodeJS.Timeout)(handler, timeout, ...args);

  timer.unref();

  return timer;
}) as unknown as typeof globalThis.setTimeout;

// In addition, do a one-shot scan after the file's test queue settles to drain any other handles (notably the WebSocketServer from puppeteer-stream).
// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* makeFakePage returns a minimal Page stub for tests that need to construct the AttemptNativeStreamingOptions; attemptNativeStreaming does not call any methods
 * on the page during the success/failure branches exercised here (it just stores the reference for scheduleTokenRefresh to use).
 */
function makeFakePage(): Page {

  return {

    isClosed: (): boolean => false
  } as unknown as Page;
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

/* makeAttemptOptions builds an AttemptNativeStreamingOptions literal with sensible defaults for the orchestration tests. Tests override the interceptionPromise
 * to inject the desired branch (null vs valid manifest), and channelName to keep the probe cache partitioned across test cases.
 *
 * The probe-cache identity is derived from the resolved channelName so that partitioning holds: the cache is addressed by identity, so a fixed identity would
 * put every test in one slot and make each test's clearProbeCache call address a key nothing reads. The stamp comes from the production builder over the
 * options' own URL, which is the binding an orchestration test has.
 */
function makeAttemptOptions(overrides: Partial<AttemptNativeStreamingOptions> = {}): AttemptNativeStreamingOptions {

  const channelName = overrides.channelName ?? "test-channel";
  const url = overrides.url ?? "https://example.test/channel";

  return {

    channelName,
    interceptionPromise: Promise.resolve(null),
    onError: noop,
    page: makeFakePage(),
    probeIdentity: { key: channelName, stamp: buildProbeCacheStamp({ channelSelector: undefined, profile: undefined, url }) },
    streamId: 1,
    streamIdStr: "test-stream",
    url,
    ...overrides
  };
}

describe("attemptNativeStreaming", () => {

  beforeEach(() => {

    clearProbeCache("test-channel");
  });

  afterEach(() => {

    mock.reset();
  });

  test("returns null when the interception promise resolves to null (no manifest captured)", async () => {

    // Happy negative path: when no manifest was intercepted by the CDP listener, the orchestrator must abandon the native upgrade and return null so the caller
    // falls back to capture mode. This path skips the probe entirely.
    const options = makeAttemptOptions({ interceptionPromise: Promise.resolve(null) });

    const result = await attemptNativeStreaming(options);

    assert.equal(result, null, "no interception -> null");
  });

  test("returns null when the probe classifies the manifest as DRM", async () => {

    // The probe runs on the intercepted master URL and detects SAMPLE-AES (Widevine). The orchestrator must return null so the caller falls back to capture mode.
    // CDP session ownership is internal to the interceptor and disposed automatically when its observer finalizes, so this test asserts only the return-value
    // contract.
    const masterUrl = "https://cdn.test/drm-master.m3u8";
    const variantUrl = "https://cdn.test/drm-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\ndrm-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://x\"\n#EXTINF:6,\nseg0.ts\n#EXTINF:6,\nseg1.ts\n", { status: 200 })
    });

    const options = makeAttemptOptions({

      channelName: "drm-channel-test",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" })
    });

    clearProbeCache("drm-channel-test");

    const result = await attemptNativeStreaming(options);

    assert.equal(result, null, "DRM classification returns null");
  });

  test("returns a NativeStreamResult on the clear-encryption happy path", async () => {

    // The probe classifies the variant as clear (no key tags), so the orchestrator constructs the proxy and returns the result. We stop the proxy immediately to
    // cancel the token refresh timer that scheduleTokenRefresh may have created (it does not, since the master URL has no expiry token, but stop() is harmless).
    const masterUrl = "https://cdn.test/clear-master.m3u8";
    const variantUrl = "https://cdn.test/clear-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720,CODECS=\"avc1.640028\"",
        "clear-variant.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:6,\nseg.ts\n#EXTINF:6,\nseg1.ts\n", { status: 200 })
    });

    const options = makeAttemptOptions({

      channelName: "clear-channel-test",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" })
    });

    clearProbeCache("clear-channel-test");

    const result = await attemptNativeStreaming(options);

    assert.ok(result, "clear classification returns a NativeStreamResult");
    assert.equal(result.bandwidth, 2500000, "bandwidth surfaces from the probe");
    assert.equal(result.codec, "H264", "codec surfaces from the probe");
    assert.equal(result.resolution, "1280x720", "resolution surfaces");
    assert.equal(result.hasAudio, false, "no separate audio rendition");

    // Stop the proxy to cancel any timers it may have set up via scheduleTokenRefresh.
    result.proxy.stop();
  });

  test("returns null when the AES-128 key prefetch fails (key URL inaccessible)", async () => {

    // The probe classifies as aes128 (key URL returns 16 bytes for the probe's accessibility check), but then the orchestrator's pre-fetch hits the same URL and
    // gets back a different result on the second call. We use a counter to flip the response between the probe and the prefetch so we can simulate intermittent
    // key availability - the orchestrator must reject the stream rather than entering native mode with an unfetchable key.
    const masterUrl = "https://cdn.test/aes-prefetch-master.m3u8";
    const variantUrl = "https://cdn.test/aes-prefetch-variant.m3u8";
    const keyUrl = "https://cdn.test/aes-prefetch-key.bin";

    let keyCallCount = 0;

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\naes-prefetch-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyUrl + "\"\n#EXTINF:2,\nseg0.ts\n#EXTINF:2,\nseg1.ts\n", { status: 200 }),
      [keyUrl]: () => {

        keyCallCount++;

        // First call (probe accessibility test) succeeds; second call (orchestrator prefetch) returns a 4xx so the orchestrator must abandon the stream.
        if(keyCallCount === 1) {

          return new Response(Buffer.alloc(16), { status: 200 });
        }

        return new Response("forbidden", { status: 403 });
      }
    });

    const options = makeAttemptOptions({

      channelName: "aes-prefetch-channel",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" })
    });

    clearProbeCache("aes-prefetch-channel");

    const result = await attemptNativeStreaming(options);

    assert.equal(result, null, "key prefetch failure returns null");
  });

  test("returns null for MPEG-TS clients when the stream has a separate audio rendition (incompatible)", async () => {

    // MPEG-TS clients cannot consume a stream with a separate audio rendition because the independent video and audio MPEG-TS segments have incompatible
    // PAT/PMT tables. The orchestrator must reject this combination and fall back to capture mode.
    const masterUrl = "https://cdn.test/dai-master.m3u8";
    const videoUrl = "https://cdn.test/dai-video.m3u8";
    const audioUrl = "https://cdn.test/dai-audio.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"English\",URI=\"" + audioUrl + "\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"audio\"",
        "dai-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n#EXTINF:2,\nseg1.ts\n", { status: 200 })
    });

    const options = makeAttemptOptions({

      channelName: "dai-channel-test",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" }),
      mpegTsClient: true
    });

    clearProbeCache("dai-channel-test");

    const result = await attemptNativeStreaming(options);

    assert.equal(result, null, "MPEG-TS client + separate audio -> null");
  });

  test("returns a NativeStreamResult for HLS clients (no mpegTsClient flag) even with separate audio", async () => {

    // Companion to the previous test: HLS clients (Channels DVR) do support separate audio renditions natively. The orchestrator returns a result with
    // hasAudio=true and the audio URL surfaces via the proxy.
    const masterUrl = "https://cdn.test/dai-hls-master.m3u8";
    const videoUrl = "https://cdn.test/dai-hls-video.m3u8";
    const audioUrl = "https://cdn.test/dai-hls-audio.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response([
        "#EXTM3U",
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"English\",URI=\"" + audioUrl + "\"",
        "#EXT-X-STREAM-INF:BANDWIDTH=3000000,AUDIO=\"audio\"",
        "dai-hls-video.m3u8",
        ""
      ].join("\n"), { status: 200 }),
      [videoUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n#EXTINF:2,\nseg1.ts\n", { status: 200 })
    });

    const options = makeAttemptOptions({

      channelName: "dai-hls-channel",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" })
    });

    clearProbeCache("dai-hls-channel");

    const result = await attemptNativeStreaming(options);

    assert.ok(result, "HLS client + separate audio is supported");
    assert.equal(result.hasAudio, true, "hasAudio reflects the separate audio rendition");

    result.proxy.stop();
  });

  test("returns null when the probe itself fails (master fetch returns 500)", async () => {

    // Negative test: the master fetch returns a server error. probeManifest returns null and the orchestrator returns null.
    const masterUrl = "https://cdn.test/probe-fail-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("server error", { status: 500 })
    });

    const options = makeAttemptOptions({

      channelName: "probe-fail-channel",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" })
    });

    clearProbeCache("probe-fail-channel");

    const result = await attemptNativeStreaming(options);

    assert.equal(result, null, "probe failure returns null");
  });

  test("propagates the onError callback and produces a non-stopped proxy on the happy path", async () => {

    // The onError callback in the options must be forwarded to the proxy so that segment fetch failures escalate to the recovery system. We don't trigger the
    // failure here (that requires the proxy's polling loop), but we lock the contract that the result.proxy is created without throwing on the path that uses the
    // callback and starts in the running state.
    const masterUrl = "https://cdn.test/onerror-master.m3u8";
    const variantUrl = "https://cdn.test/onerror-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nonerror-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n#EXTINF:2,\nseg1.ts\n", { status: 200 })
    });

    let onErrorCalls = 0;

    const options = makeAttemptOptions({

      channelName: "onerror-channel",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" }),
      onError: (): void => {

        onErrorCalls++;
      }
    });

    clearProbeCache("onerror-channel");

    const result = await attemptNativeStreaming(options);

    assert.ok(result, "proxy created on the happy path");
    assert.equal(onErrorCalls, 0, "onError not invoked during construction");
    assert.equal(result.proxy.isStopped(), false, "proxy starts in the running state");

    result.proxy.stop();
  });

  test("returns null when the interception promise rejects (caller error path)", async () => {

    // Negative test: a rejection from the interception promise is caught by the orchestrator and surfaced as null (with a debug log). This ensures a CDP
    // listener bug cannot crash the streaming setup; native mode just declines and capture takes over.
    const options = makeAttemptOptions({

      // The orchestrator's try/catch around the await must convert a rejection into a null return rather than a crash.
      interceptionPromise: Promise.reject(new Error("synthetic interception failure"))
    });

    const result = await attemptNativeStreaming(options);

    assert.equal(result, null, "rejection on interception path -> null");
  });

  test("returns null when interception stalls past INTERCEPTION_AWAIT_TIMEOUT (cancellableTimeout fires)", async () => {

    /* The orchestrator's race between interceptionPromise and cancellableTimeout(INTERCEPTION_AWAIT_TIMEOUT). The "interception resolves first" branch is
     * exercised by every other test in this suite that resolves its interception promise (all but the rejection test above, which exercises the surrounding
     * catch block instead). The "timeout fires first" branch races
     * cancellableTimeout against the interception promise, coerces a false race result to null, and cancels the underlying timer in finally so it does not
     * hold an event loop reference. Without this test, a regression in the race coercion (e.g., dropping the `(result === false) ? null : result` step) or
     * the finally-cancel cleanup would not surface here.
     *
     * We virtualize setTimeout via mock.timers so the 5-second wait is instantaneous in test time. The neverResolving promise simulates a CDP listener that
     * captured no manifest before the deadline. After advancing past INTERCEPTION_AWAIT_TIMEOUT (5000ms) the cancellableTimeout's internal setTimeout fires
     * and resolves the race with `false`, which the orchestrator coerces to null and short-circuits to the "No manifest intercepted" log path. The await
     * then resolves with null without waiting on real wall-clock time.
     */
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      // A promise that intentionally never resolves so cancellableTimeout's setTimeout wins the race. We construct it via Promise.withResolvers and discard
      // the resolvers so nothing can complete the promise from outside; this is the modern equivalent of `new Promise(() => {})` without the empty-executor
      // lint complaint.
      const { promise: neverResolving } = Promise.withResolvers<null>();

      const options = makeAttemptOptions({

        channelName: "timeout-channel",
        interceptionPromise: neverResolving
      });

      clearProbeCache("timeout-channel");

      const resultPromise = attemptNativeStreaming(options);

      // Advance past the 5-second INTERCEPTION_AWAIT_TIMEOUT. cancellableTimeout's setTimeout fires, the race resolves to false, the orchestrator coerces to
      // null, and the function returns through the "No manifest intercepted" branch. A small extra tick (1ms) ensures we are past the timer's exact firing
      // boundary regardless of strict-vs-loose comparison semantics in the runtime's timer wheel.
      mock.timers.tick(5_001);

      const result = await resultPromise;

      assert.equal(result, null, "timeout-wins branch coerces the race result to null");
    } finally {

      mock.timers.reset();
    }
  });

  test("schedules a token refresh when the master URL contains an expiry token", async () => {

    // Boundary: when the master URL embeds an exp= token, the orchestrator schedules a refresh timer on the proxy. The minimum-refresh-delay floor of 30s keeps
    // the refresh from firing during the test. We verify by ensuring proxy.stop() is safe to call and cancels the scheduled timer.
    const expirySeconds = Math.floor(Date.now() / 1000) + 600;
    const masterUrl = "https://cdn.test/exp-master.m3u8?exp=" + String(expirySeconds);
    const variantUrl = "https://cdn.test/exp-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nexp-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n#EXTINF:2,\nseg1.ts\n", { status: 200 })
    });

    const options = makeAttemptOptions({

      channelName: "exp-channel",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" })
    });

    clearProbeCache("exp-channel");

    const result = await attemptNativeStreaming(options);

    assert.ok(result, "exp-token URL still produces a result");

    // proxy.stop() cancels the token refresh timer; if it had not been scheduled correctly, stop() would not crash but the timer would leak. The
    // lifecycle.test.ts pattern (the setTimeout unref wrapper plus closePuppeteerStreamWssOnIdle) backstops handle leakage at the file level if anything escapes.
    assert.doesNotThrow(() => {

      result.proxy.stop();
    });
  });

  test("returns a NativeStreamResult and enters native mode when the AES-128 key is accessible on both the probe and the prefetch", async () => {

    // AES-128 success path: the variant declares METHOD=AES-128 and the key URL returns a stable 16-byte key on BOTH the probe's accessibility check and the
    // coordinator's pre-fetch. Both key fetches must succeed for the orchestrator to commit to native mode. We count the key fetches to pin that the key URL was
    // consulted exactly twice (probe accessibility check plus coordinator prefetch) before the proxy was constructed. A regression that skipped the prefetch, or
    // failed to classify the stream as AES-128, would break this count or return null instead of a NativeStreamResult.
    const masterUrl = "https://cdn.test/aes-ok-master.m3u8";
    const variantUrl = "https://cdn.test/aes-ok-variant.m3u8";
    const keyUrl = "https://cdn.test/aes-ok-key.bin";

    let keyCallCount = 0;

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1500000,CODECS=\"avc1.640028\"\naes-ok-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyUrl + "\"\n#EXTINF:2,\nseg.ts\n#EXTINF:2,\nseg1.ts\n",
        { status: 200 }),
      [keyUrl]: () => {

        keyCallCount++;

        // Every key fetch returns a valid 16-byte key, so both the probe accessibility check and the coordinator prefetch succeed and native mode is entered.
        return new Response(Buffer.alloc(16), { status: 200 });
      }
    });

    const options = makeAttemptOptions({

      channelName: "aes-ok-channel",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" })
    });

    clearProbeCache("aes-ok-channel");

    const result = await attemptNativeStreaming(options);

    assert.ok(result, "accessible AES-128 key enters native mode");
    assert.equal(result.bandwidth, 1500000, "bandwidth surfaces from the probe");
    assert.equal(result.hasAudio, false, "no separate audio rendition on the AES-128 variant");
    assert.equal(keyCallCount, 2, "key URL consulted twice: probe accessibility check plus coordinator prefetch");

    result.proxy.stop();
  });
});


describe("attemptNativeStreaming: container hop (T21)", () => {

  afterEach(() => {

    mock.reset();
  });

  test("carries the probe's fmp4 classification onto the result", async () => {

    // The coordinator must pass the probe's container through untouched, since the registry write and every downstream container decision read it from here.
    const masterUrl = "https://cdn.test/hop-fmp4-master.m3u8";
    const variantUrl = "https://cdn.test/hop-fmp4-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nhop-fmp4-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MAP:URI=\"init.cmfv\"\n#EXTINF:6,\nseg0.cmfv\n#EXTINF:6,\nseg1.cmfv\n",
        { status: 200 })
    });

    clearProbeCache("hop-fmp4-channel");

    const result = await attemptNativeStreaming(makeAttemptOptions({

      channelName: "hop-fmp4-channel",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" })
    }));

    assert.ok(result, "native attempt succeeded");
    assert.equal(result.container, "fmp4", "the fMP4 classification reaches the result");

    result.proxy.stop();
  });

  test("carries the probe's ts classification onto the result", async () => {

    /* The contrasting fixture is what gives the pair its distinguishing power: a coordinator that hardcoded a container, or coerced it, would satisfy a
     * single-value check but cannot satisfy both of these.
     */
    const masterUrl = "https://cdn.test/hop-ts-master.m3u8";
    const variantUrl = "https://cdn.test/hop-ts-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nhop-ts-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6,\nseg0.ts\n#EXTINF:6,\nseg1.ts\n", { status: 200 })
    });

    clearProbeCache("hop-ts-channel");

    const result = await attemptNativeStreaming(makeAttemptOptions({

      channelName: "hop-ts-channel",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" })
    }));

    assert.ok(result, "native attempt succeeded");
    assert.equal(result.container, "ts", "the MPEG-TS classification reaches the result");

    result.proxy.stop();
  });
});

describe("attemptNativeStreaming: static-playlist tune admission", () => {

  afterEach(() => {

    mock.reset();
  });

  test("returns null when the intercepted master's variant is a one-segment session bumper", async () => {

    /* The end-to-end pin on tune admission. The coordinator is the only caller that opts into static-playlist rejection, so a bumper-shaped variant probes to null
     * and the attempt declines down the same path a DRM classification takes - the one the caller reads as "fall back to capture". The fixture mirrors the observed
     * shape: live-tagged CMAF, an initialization segment, and a window of exactly one segment that never advances.
     */
    const masterUrl = "https://cdn.test/bumper-master.m3u8";
    const variantUrl = "https://cdn.test/bumper-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1800000\nbumper-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MAP:URI=\"bumper_init.cmfv\"\n#EXTINF:6.006,\nbumper_1.cmfv\n",
        { status: 200 })
    });

    clearProbeCache("bumper-channel");

    const result = await attemptNativeStreaming(makeAttemptOptions({

      channelName: "bumper-channel",
      interceptionPromise: Promise.resolve({ manifestUrl: masterUrl, selectedKind: "master" })
    }));

    assert.equal(result, null, "a bumper-shaped variant declines the native attempt so capture serves the channel");
  });
});
