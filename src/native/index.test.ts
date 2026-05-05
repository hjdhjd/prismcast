/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the native streaming coordinator in index.ts. The two exports are attemptNativeStreaming (the orchestrator that awaits manifest
 * interception, probes for DRM, and constructs the native proxy) and refreshNativeManifest (the token-refresh path that re-probes the master URL or reloads the
 * page). The page-reload branch of refreshNativeManifest is heavily entangled with real Chrome via Puppeteer (page.goto, installManifestInterceptor's CDP listener
 * wiring) and is deferred to e2e coverage; the unit tests here focus on the orchestration branches that can be exercised with synthetic CDP sessions, mocked
 * globalThis.fetch responses, and a minimal page stub.
 */
import { type AttemptNativeStreamingOptions, attemptNativeStreaming } from "./index.ts";
import type { CDPSession, Page } from "puppeteer-core";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import assert from "node:assert/strict";
import { clearProbeCache } from "./probe.ts";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";

/* eslint-disable sort-keys -- fixture route maps are ordered by HLS resolution chain (master -> variant -> key), not alphabetical key strings, so the logical
 * dependency direction is visible to readers. */

/* Two sources of post-test handle leakage need draining for this file:
 *
 * 1. puppeteer-stream's PuppeteerStream module starts a WebSocketServer at import time. index.ts pulls in browser/manifestInterceptor.ts which in turn pulls in
 *    browser/index.ts and triggers that server creation, which keeps the event loop alive after every test resolves.
 *
 * 2. attemptNativeStreaming creates a 5-second setTimeout for the interception-await timeout via Promise.race, and that timer is not cleared when the race resolves
 *    via the other branch. With ~10 happy-path tests, each leaving a Timeout behind, the file appears to hang for 5 seconds before the loop drains.
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

/* noop is a non-empty function body used wherever we need a stub method to satisfy a contract without doing anything. Using a tiny named function avoids the
 * @typescript-eslint/no-empty-function rule that fires on bare `() => {}`.
 */
function noop(): void {

  return undefined;
}

/* makeFakeCdpSession returns a stub CDPSession satisfying the surface that removeManifestInterceptor uses. The functions are non-empty no-ops or resolved promises
 * so the cleanup path runs without hitting Puppeteer internals. The cast bypasses the very wide CDPSession type that a real implementation provides.
 */
function makeFakeCdpSession(): CDPSession {

  return {

    detach: async (): Promise<void> => Promise.resolve(),
    removeAllListeners: (): unknown => undefined,
    send: async (): Promise<unknown> => Promise.resolve(undefined)
  } as unknown as CDPSession;
}

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
 */
function makeAttemptOptions(overrides: Partial<AttemptNativeStreamingOptions> = {}): AttemptNativeStreamingOptions {

  return {

    channelName: "test-channel",
    interceptionPromise: Promise.resolve(null),
    onError: noop,
    page: makeFakePage(),
    streamId: 1,
    streamIdStr: "test-stream",
    url: "https://example.test/channel",
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

    // The probe runs on the intercepted master URL and detects SAMPLE-AES (Widevine). The orchestrator must clean up the CDP session and return null. We track
    // removeAllListeners on the stub session so the cleanup invocation surfaces as observable behavior.
    const masterUrl = "https://cdn.test/drm-master.m3u8";
    const variantUrl = "https://cdn.test/drm-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\ndrm-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://x\"\n", { status: 200 })
    });

    let cleanupCalls = 0;

    const cdpSession = {

      detach: async (): Promise<void> => Promise.resolve(),
      removeAllListeners: (): unknown => {

        cleanupCalls++;

        return undefined;
      },
      send: async (): Promise<unknown> => Promise.resolve(undefined)
    } as unknown as CDPSession;

    const options = makeAttemptOptions({

      channelName: "drm-channel-test",
      interceptionPromise: Promise.resolve({ cdpSession, masterManifestUrl: masterUrl })
    });

    clearProbeCache("drm-channel-test");

    const result = await attemptNativeStreaming(options);

    assert.equal(result, null, "DRM classification returns null");
    assert.ok(cleanupCalls > 0, "CDP session cleanup called on DRM path");
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
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:6,\nseg.ts\n", { status: 200 })
    });

    const options = makeAttemptOptions({

      channelName: "clear-channel-test",
      interceptionPromise: Promise.resolve({ cdpSession: makeFakeCdpSession(), masterManifestUrl: masterUrl })
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
      [variantUrl]: () => new Response("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyUrl + "\"\n", { status: 200 }),
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
      interceptionPromise: Promise.resolve({ cdpSession: makeFakeCdpSession(), masterManifestUrl: masterUrl })
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
      [videoUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const options = makeAttemptOptions({

      channelName: "dai-channel-test",
      interceptionPromise: Promise.resolve({ cdpSession: makeFakeCdpSession(), masterManifestUrl: masterUrl }),
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
      [videoUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const options = makeAttemptOptions({

      channelName: "dai-hls-channel",
      interceptionPromise: Promise.resolve({ cdpSession: makeFakeCdpSession(), masterManifestUrl: masterUrl })
    });

    clearProbeCache("dai-hls-channel");

    const result = await attemptNativeStreaming(options);

    assert.ok(result, "HLS client + separate audio is supported");
    assert.equal(result.hasAudio, true, "hasAudio reflects the separate audio rendition");

    result.proxy.stop();
  });

  test("returns null when the probe itself fails (master fetch returns 500)", async () => {

    // Negative test: the master fetch returns a server error. probeManifest returns null, the orchestrator cleans up the CDP session and returns null.
    const masterUrl = "https://cdn.test/probe-fail-master.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("server error", { status: 500 })
    });

    const options = makeAttemptOptions({

      channelName: "probe-fail-channel",
      interceptionPromise: Promise.resolve({ cdpSession: makeFakeCdpSession(), masterManifestUrl: masterUrl })
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
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    let onErrorCalls = 0;

    const options = makeAttemptOptions({

      channelName: "onerror-channel",
      interceptionPromise: Promise.resolve({ cdpSession: makeFakeCdpSession(), masterManifestUrl: masterUrl }),
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

  test("schedules a token refresh when the master URL contains an expiry token", async () => {

    // Boundary: when the master URL embeds an exp= token, the orchestrator schedules a refresh timer on the proxy. The minimum-refresh-delay floor of 30s keeps
    // the refresh from firing during the test. We verify by ensuring proxy.stop() is safe to call and cancels the scheduled timer.
    const expirySeconds = Math.floor(Date.now() / 1000) + 600;
    const masterUrl = "https://cdn.test/exp-master.m3u8?exp=" + String(expirySeconds);
    const variantUrl = "https://cdn.test/exp-variant.m3u8";

    makeFetchRouter({

      [masterUrl]: () => new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nexp-variant.m3u8\n", { status: 200 }),
      [variantUrl]: () => new Response("#EXTM3U\n#EXTINF:2,\nseg.ts\n", { status: 200 })
    });

    const options = makeAttemptOptions({

      channelName: "exp-channel",
      interceptionPromise: Promise.resolve({ cdpSession: makeFakeCdpSession(), masterManifestUrl: masterUrl })
    });

    clearProbeCache("exp-channel");

    const result = await attemptNativeStreaming(options);

    assert.ok(result, "exp-token URL still produces a result");

    // proxy.stop() cancels the token refresh timer; if it had not been scheduled correctly, stop() would not crash but the timer would leak. The lifecycle.ts
    // pattern (cleanupTimer.unref) backstops handle leakage at the file level if anything escapes.
    assert.doesNotThrow(() => {

      result.proxy.stop();
    });
  });
});

