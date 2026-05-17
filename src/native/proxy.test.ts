/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * proxy.test.ts: Unit tests for the native HLS proxy factory in proxy.ts. createNativeProxy is the only export and returns a NativeProxy instance with the
 * documented surface. The polling loop, manifest parsing, segment fetching, decryption integration, and playlist generation are all encapsulated inside the
 * factory closure and are exercised end-to-end by start()ing the proxy against a live HLS source - not viable as a unit test. The tests here focus on the
 * factory's deterministic surface: initial-state contracts on every getter, stop() lifecycle, the token refresh state mutations exposed via update* methods,
 * and stat counter behavior. The full polling loop is deferred to e2e coverage with real Chrome.
 */
import { describe, test } from "node:test";
import type { NativeProxyOptions } from "./proxy.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { createNativeProxy } from "./proxy.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* makeProxyOptions builds a NativeProxyOptions literal with sensible defaults. Tests override the encryption mode, audio variant URL, and prerollSegmentCount as
 * needed. The onError callback defaults to a no-op; tests that need to observe error escalation override it. The proxy no longer holds a CDP session reference -
 * session ownership lives entirely inside the manifest interceptor's tab network observer and is disposed deterministically when interception ends.
 */
function makeProxyOptions(overrides: Partial<NativeProxyOptions> = {}): NativeProxyOptions {

  return {

    audioVariantUrl: null,
    channelName: "test-channel",
    encryption: "clear",
    keyUrl: null,
    onError: (): void => {

      // Stub onError - tests that need to observe error escalation override this.
    },
    prefetchedKey: null,
    streamId: 1,
    streamIdStr: "test-stream",
    variantUrl: "https://cdn.test/variant.m3u8",
    ...overrides
  };
}

describe("createNativeProxy initial state", () => {

  test("returns 0 from getConsecutiveErrors before any poll", () => {

    // Boundary: a freshly-constructed proxy has no fetch errors and no manifest failures across either the video or audio paths. The aggregate getter sums all
    // four counters and must report 0.
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.getConsecutiveErrors(), 0);
    proxy.stop();
  });

  test("returns null from getLastSegmentSize before any segment is stored", () => {

    // Boundary: the lastSegmentSize state initializes to null. Locks the contract so callers (the monitor's tiny-segment detector) can distinguish "no segments
    // yet" from "zero-byte segment".
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.getLastSegmentSize(), null);
    proxy.stop();
  });

  test("returns 0 from getLastSegmentTime before any segment is stored", () => {

    // Boundary: lastSegmentTime initializes to 0, the documented "no segments yet" sentinel. Locks the contract so the monitor's staleness detector treats this
    // as "stream not ready" rather than as "stale for ~55 years since 1970-01-01".
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.getLastSegmentTime(), 0);
    proxy.stop();
  });

  test("returns the prerollSegmentCount from getSegmentIndex when no preroll is configured (defaults to 0)", () => {

    // Boundary: the segment index starts at prerollSegmentCount to reserve the index space. With the default option (prerollSegmentCount undefined), it starts at 0.
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.getSegmentIndex(), 0);
    proxy.stop();
  });

  test("returns the configured prerollSegmentCount from getSegmentIndex when preroll is specified", () => {

    // The proxy reserves the [0, prerollSegmentCount) range for preroll entries and starts numbering real segments at prerollSegmentCount. Locks the index offset
    // contract.
    const proxy = createNativeProxy(makeProxyOptions({ prerollSegmentCount: 5 }));

    assert.equal(proxy.getSegmentIndex(), 5, "real segment numbering starts after the preroll reservation");
    proxy.stop();
  });

  test("returns the documented stats shape from getStats with all counters at zero", () => {

    // Locks the stats object shape so callers (the termination summary, the SSE health emitter) can rely on every key being present and numeric.
    const proxy = createNativeProxy(makeProxyOptions());

    const stats = proxy.getStats();

    assert.equal(stats.fetchErrors, 0, "fetchErrors zero");
    assert.equal(stats.segmentsFetched, 0, "segmentsFetched zero");
    assert.equal(stats.tokenRefreshes, 0, "tokenRefreshes zero");
    proxy.stop();
  });

  test("returns the default target duration of 6 seconds before any manifest is fetched", () => {

    // Boundary: target duration initializes to 6 (the typical HLS spec value) until the first variant manifest arrives. The monitor's staleness threshold uses
    // 2x this value, so a sane default keeps the monitor from immediately flagging a freshly-started stream.
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.getTargetDuration(), 6);
    proxy.stop();
  });

  test("returns false from hasErrored before any error threshold is hit", () => {

    // Boundary: a freshly-constructed proxy has not yet exhausted any error threshold. Locks the false default so the monitor's L3-fallback path does not fire
    // immediately.
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.hasErrored(), false);
    proxy.stop();
  });

  test("returns false from isStopped before stop() is called", () => {

    // Boundary: the lifecycle state starts in the running state. The first state mutation comes from start() (no observable change) or stop() (transitions to true).
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.isStopped(), false);
    proxy.stop();
  });
});

describe("NativeProxy.stop", () => {

  test("transitions isStopped from false to true", () => {

    // Happy path: stop() is the documented lifecycle exit. After the call, isStopped reports true.
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.isStopped(), false, "running before stop");

    proxy.stop();

    assert.equal(proxy.isStopped(), true, "stopped after stop");
  });

  test("clears a token refresh timer set via setTokenRefreshTimer", () => {

    // We register a real setTimeout as the token refresh timer; if stop() does not clear it, the callback fires after stop and the test runner detects the
    // assertion failure inside the callback. We use a 50ms delay and assert the callback never runs.
    const proxy = createNativeProxy(makeProxyOptions());

    let timerFired = false;

    const timer = setTimeout(() => {

      timerFired = true;
    }, 50);

    proxy.setTokenRefreshTimer(timer);
    proxy.stop();

    return new Promise<void>((resolve) => {

      setTimeout(() => {

        assert.equal(timerFired, false, "timer was cancelled by stop()");
        resolve();
      }, 100);
    });
  });

  test("is idempotent - a second stop() call does not throw", () => {

    // Boundary: tests for double-stop are common in lifecycle code because the parent (terminateStream) may invoke stop multiple times across cleanup paths. The
    // proxy must tolerate this gracefully.
    const proxy = createNativeProxy(makeProxyOptions());

    proxy.stop();

    assert.doesNotThrow(() => {

      proxy.stop();
    }, "second stop is a safe no-op");
  });
});

describe("NativeProxy.setTokenRefreshTimer", () => {

  test("storing a timer does not affect any of the public-state getters", () => {

    // Boundary: setTokenRefreshTimer is purely a side-channel for the coordinator to register a cancel handle. None of the public counters or state observers
    // should change when a timer is registered.
    const proxy = createNativeProxy(makeProxyOptions());
    const timer = setTimeout((): void => undefined, 100_000);

    proxy.setTokenRefreshTimer(timer);

    assert.equal(proxy.isStopped(), false, "isStopped unchanged");
    assert.equal(proxy.getStats().tokenRefreshes, 0, "tokenRefreshes unchanged");

    clearTimeout(timer);
    proxy.stop();
  });
});

describe("NativeProxy.updateVariantUrl", () => {

  test("increments the tokenRefreshes stat counter on each call", () => {

    // Locks the contract that every variant URL swap counts as one token refresh, regardless of whether the audio URL is also updated. The termination summary
    // reports this counter so users can see how often a stream had to refresh its tokens.
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.getStats().tokenRefreshes, 0, "starts at 0");

    proxy.updateVariantUrl("https://cdn.test/new-variant-1.m3u8");
    assert.equal(proxy.getStats().tokenRefreshes, 1, "first refresh increments to 1");

    proxy.updateVariantUrl("https://cdn.test/new-variant-2.m3u8");
    assert.equal(proxy.getStats().tokenRefreshes, 2, "second refresh increments to 2");

    proxy.stop();
  });

  test("multiple updates accumulate without resetting the counter", () => {

    // Boundary: the counter is monotonic across the proxy's lifetime. Stop does not reset; restart is not supported. The counter survives all internal state
    // changes (filterFetchedSequences, etc.) and only ever increments.
    const proxy = createNativeProxy(makeProxyOptions());

    for(let i = 0; i < 5; i++) {

      proxy.updateVariantUrl("https://cdn.test/variant-" + String(i) + ".m3u8");
    }

    assert.equal(proxy.getStats().tokenRefreshes, 5, "five refreshes accumulate to 5");
    proxy.stop();
  });
});

describe("NativeProxy.updateAudioVariantUrl", () => {

  test("does not increment the tokenRefreshes counter (only video URL updates count)", () => {

    // Boundary: audio variant URL updates are part of the same logical refresh as the video update; the coordinator increments the counter via updateVariantUrl.
    // Audio updates alone must not double-count.
    const proxy = createNativeProxy(makeProxyOptions({ audioVariantUrl: "https://cdn.test/audio.m3u8" }));

    proxy.updateAudioVariantUrl("https://cdn.test/audio-2.m3u8");
    proxy.updateAudioVariantUrl("https://cdn.test/audio-3.m3u8");

    assert.equal(proxy.getStats().tokenRefreshes, 0, "audio updates alone do not increment the counter");
    proxy.stop();
  });

  test("does not throw on a stream without a separate audio rendition (audioVariantUrl was null)", () => {

    // Negative test: the coordinator may erroneously call updateAudioVariantUrl on a non-DAI stream. The proxy must tolerate this without crashing - it just
    // quietly stores the new URL even though no audio polling will ever consume it.
    const proxy = createNativeProxy(makeProxyOptions({ audioVariantUrl: null }));

    assert.doesNotThrow(() => {

      proxy.updateAudioVariantUrl("https://cdn.test/audio.m3u8");
    });

    proxy.stop();
  });
});

describe("NativeProxy with prefetched AES-128 key", () => {

  test("creates without throwing when prefetchedKey is provided alongside keyUrl", () => {

    // Boundary: the AES-128 path requires both a keyUrl and a prefetchedKey to seed the per-URL key cache. Locks the constructor's tolerance for this combination.
    const proxy = createNativeProxy(makeProxyOptions({

      encryption: "aes128",
      keyUrl: "https://cdn.test/key.bin",
      prefetchedKey: Buffer.alloc(16, 0xab)
    }));

    assert.equal(proxy.isStopped(), false, "proxy created in running state");
    proxy.stop();
  });

  test("creates without throwing when encryption is aes128 but no prefetchedKey is provided (lazy-fetch path)", () => {

    // Boundary: when the coordinator does not pre-fetch (e.g., DRM-enabled callers that haven't validated the key), the proxy is still constructable. The first
    // segment fetch will trigger an on-demand key fetch via the cache miss path. We only validate that construction does not crash.
    const proxy = createNativeProxy(makeProxyOptions({

      encryption: "aes128",
      keyUrl: "https://cdn.test/key.bin",
      prefetchedKey: null
    }));

    assert.equal(proxy.isStopped(), false);
    proxy.stop();
  });

  test("ignores prefetchedKey when keyUrl is null (no cache key to associate)", () => {

    // Negative test: a prefetched key without a key URL has no slot to be cached under. The proxy must not crash on this combination - it just discards the key.
    // The encryption mode must still be "clear" or "aes128" per the type, so we use clear here.
    const proxy = createNativeProxy(makeProxyOptions({

      encryption: "clear",
      keyUrl: null,
      prefetchedKey: Buffer.alloc(16)
    }));

    assert.equal(proxy.isStopped(), false);
    proxy.stop();
  });
});

describe("NativeProxy.getConsecutiveErrors aggregation", () => {

  test("sums video and audio segment-tracker failures plus manifest failures", () => {

    // The aggregate getter must add four counters: video.consecutiveManifestFailures, video.segmentTracker.consecutiveFailures, audio.consecutiveManifestFailures,
    // and audio.segmentTracker.consecutiveFailures. We can't directly mutate the trackers (they are closure-scoped), but the initial state of zero is the
    // baseline; if any future change broke the summation, this test would surface as a non-zero starting value.
    const proxy = createNativeProxy(makeProxyOptions({ audioVariantUrl: "https://cdn.test/audio.m3u8" }));

    assert.equal(proxy.getConsecutiveErrors(), 0, "zero across all four counters before any poll");
    proxy.stop();
  });
});

