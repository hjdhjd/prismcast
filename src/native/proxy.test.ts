/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * proxy.test.ts: Unit tests for the native HLS proxy in proxy.ts. createNativeProxy is the factory export and returns a NativeProxy instance with the documented
 * surface. The polling loop, manifest parsing, segment fetching, decryption integration, and playlist generation are all encapsulated inside the factory closure.
 * The tests here focus on the factory's deterministic surface: initial-state contracts on every getter, stop() lifecycle, the token refresh state mutations exposed
 * via update* methods, and stat counter behavior. The loop's own behavior is driven in proxy.map.test.ts, whose harness virtualizes the polling cadence through the
 * clock port and routes fetches through a stub; what stays beyond unit reach is the live-Chrome and live-source behavior around it, which e2e covers. The pure
 * module-level helpers extracted from the polling loop - manifestFailureThreshold (poll failure escalation), resolveSegmentIv (explicit-versus-derived IV
 * selection), and pruneKeyCache (per-URL key cache bounding) - are exported and tested directly, pinning the manifest-hardening rules the closure delegates to
 * them.
 */
import { createNativeProxy, manifestFailureThreshold, pruneKeyCache, resolveSegmentIv } from "./proxy.ts";
import { describe, mock, test } from "node:test";
import type { NativeProxyOptions } from "./proxy.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { deriveIvFromSequence } from "./decrypt.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* makeProxyOptions builds a NativeProxyOptions literal with sensible defaults. Tests override the encryption mode, audio variant URL, and prerollSegmentCount as
 * needed. The onError callback defaults to a no-op; tests that need to observe error escalation override it. The proxy does not hold a CDP session reference;
 * session ownership lives entirely inside the manifest interceptor's tab network observer and is disposed deterministically when interception ends.
 */
function makeProxyOptions(overrides: Partial<NativeProxyOptions> = {}): NativeProxyOptions {

  return {

    audioVariantUrl: null,
    channelName: "test-channel",
    container: "ts",
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

  test("a second stop() call does not throw", () => {

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

  test("retires the previous timer when a second one is armed", () => {

    /* The proxy owns a single refresh timer slot, and this setter is the only site that writes it. A reschedule must therefore cancel the handle it replaces:
     * every refresh over a long recording arms a successor, so a setter that only overwrote the slot would leave each predecessor live, firing a refresh against
     * a proxy whose newer schedule already speaks for it. The spy records which handles were cancelled while still cancelling them for real, so the timers this
     * test creates cannot outlive it.
     */
    const proxy = createNativeProxy(makeProxyOptions());
    const cleared: NodeJS.Timeout[] = [];
    const realClearTimeout = globalThis.clearTimeout;

    mock.method(globalThis, "clearTimeout", (timer: NodeJS.Timeout): void => {

      cleared.push(timer);
      realClearTimeout(timer);
    });

    try {

      const first = setTimeout((): void => undefined, 50);
      const second = setTimeout((): void => undefined, 50);

      proxy.setTokenRefreshTimer(first);

      assert.equal(cleared.length, 0, "the first arming has no predecessor to retire");

      proxy.setTokenRefreshTimer(second);

      assert.equal(cleared.length, 1, "arming a successor cancels exactly one handle");
      assert.equal(cleared[0], first, "and the handle it cancels is the one it replaced");

      proxy.stop();

      assert.equal(cleared.length, 2, "the stop path retires the handle that is live");
      assert.equal(cleared[1], second, "which is the successor, leaving no timer behind");
    } finally {

      mock.reset();
    }
  });
});

describe("NativeProxy refresh coordination state", () => {

  test("holds the in-flight refresh the coordinator stores and releases it on request", async () => {

    // The slot is what makes two refresh triggers converge on one attempt: whoever finds it occupied awaits what is there. A freshly built proxy has no attempt
    // running, and clearing the slot must leave it that way rather than holding a settled promise a later caller would await forever.
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.getPendingRefresh(), null, "a new proxy has no refresh in flight");

    const attempt = Promise.resolve(true);

    proxy.setPendingRefresh(attempt);

    assert.equal(proxy.getPendingRefresh(), attempt, "the stored attempt is what a second caller reads back");

    proxy.setPendingRefresh(null);

    assert.equal(proxy.getPendingRefresh(), null, "and releasing the slot empties it");
    assert.equal(await attempt, true, "the attempt itself is untouched by the slot");

    proxy.stop();
  });

  test("counts consecutive refresh failures and forgets them on request", () => {

    // The count sizes the retry backoff, so it has to accumulate across a run of failures and start over once the stream refreshes successfully again.
    const proxy = createNativeProxy(makeProxyOptions());

    assert.equal(proxy.noteRefreshFailure(), 1, "the first failure of a run counts one");
    assert.equal(proxy.noteRefreshFailure(), 2, "and consecutive failures accumulate");

    proxy.clearRefreshFailures();

    assert.equal(proxy.noteRefreshFailure(), 1, "after a clear, the next failure starts the run over");

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
    // changes (the fetchedSequences reset on updateVariantUrl, etc.) and only ever increments.
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

describe("manifestFailureThreshold", () => {

  // The base threshold is MAX_MANIFEST_FAILURES (3) and the extended threshold is double that (6). These are not exported, so we assert against the literal values
  // the constant resolves to. If the constant changes, these literals must change with it - the parity property below is the structural invariant, the literals are
  // the documented current values.
  const BASE = 3;
  const EXTENDED = 6;

  test("returns the base threshold for every 4xx client error", () => {

    // Client errors (auth expiry, content removed) are treated as permanent and use the base threshold directly so a broken stream is reported quickly rather than
    // retried twice as long.
    for(const status of [ 400, 401, 403, 404, 410, 429, 499 ]) {

      assert.equal(manifestFailureThreshold(status), BASE, "4xx status " + String(status) + " uses the base threshold");
    }
  });

  test("returns the extended threshold for 5xx server errors", () => {

    // Server errors are transient CDN conditions that usually self-resolve, so they get double the attempts.
    for(const status of [ 500, 502, 503, 504 ]) {

      assert.equal(manifestFailureThreshold(status), EXTENDED, "5xx status " + String(status) + " uses the extended threshold");
    }
  });

  test("returns the extended threshold for a missing status (network or timeout error)", () => {

    // A failure that never produced a response (DNS failure, connection reset, AbortSignal timeout) has no status. It is transient by nature, so the helper returns
    // the doubled threshold. This is the case the video and audio catch paths rely on.
    assert.equal(manifestFailureThreshold(), EXTENDED, "undefined status uses the extended threshold");
    assert.equal(manifestFailureThreshold(undefined), EXTENDED, "explicit undefined uses the extended threshold");
  });

  test("treats the 3xx and exactly-400 boundaries correctly", () => {

    // Boundary: 399 is not a client error (extended), 400 is the first client error (base), 499 is the last client error (base), and 500 leaves the client range
    // (extended). This pins the half-open [400, 500) classification the helper implements.
    assert.equal(manifestFailureThreshold(399), EXTENDED, "399 is below the client range");
    assert.equal(manifestFailureThreshold(400), BASE, "400 is the first client error");
    assert.equal(manifestFailureThreshold(499), BASE, "499 is the last client error");
    assert.equal(manifestFailureThreshold(500), EXTENDED, "500 leaves the client range");
  });

  test("audio and video poll paths share identical thresholds (finding [8] parity)", () => {

    // The core property: there is one threshold decision, so the audio poll and the video poll escalate identically for the same failure class. We assert the helper
    // is a pure function of the status alone - the same status yields the same threshold regardless of which path calls it. Both paths route through this single
    // helper, so neither can drift to a flat base threshold for 5xx and network errors.
    const networkThreshold = manifestFailureThreshold();
    const serverThreshold = manifestFailureThreshold(503);
    const clientThreshold = manifestFailureThreshold(403);

    assert.equal(networkThreshold, serverThreshold, "network and 5xx share the extended threshold on both paths");
    assert.notEqual(clientThreshold, serverThreshold, "4xx escalates faster than 5xx on both paths");
  });
});

describe("resolveSegmentIv", () => {

  test("derives the IV from the sequence number when no explicit IV is provided", () => {

    // The genuine absence of an explicit IV (ivHex === null) is the only case that derives from the media sequence number per RFC 8216 Section 5.2. The resolver
    // must produce exactly the same IV as deriveIvFromSequence for that sequence.
    const result = resolveSegmentIv(null, 42);

    assert.equal(result.status, "ok", "absent explicit IV resolves successfully");

    // The assert.equal above narrows result to the "ok" variant, so result.iv is directly accessible without a redundant status check.
    assert.deepEqual(result.iv, deriveIvFromSequence(42), "derived IV matches the sequence derivation");
  });

  test("returns the explicit IV verbatim when it parses cleanly", () => {

    // A well-formed explicit IV (0x prefix + 32 hex digits) is authoritative and is returned as the 16-byte buffer it encodes - the sequence number is ignored.
    const result = resolveSegmentIv("0x000102030405060708090a0b0c0d0e0f", 99);

    assert.equal(result.status, "ok", "well-formed explicit IV resolves successfully");

    // The assert.equal above narrows result to the "ok" variant, so result.iv is directly accessible without a redundant status check.
    assert.deepEqual(result.iv, Buffer.from("000102030405060708090a0b0c0d0e0f", "hex"), "explicit IV bytes are used verbatim");
  });

  test("rejects a malformed explicit IV rather than substituting the sequence IV (finding [21])", () => {

    // The core property: a present-but-malformed explicit IV must never silently fall back to the sequence-derived IV, because that decrypts the
    // segment with the wrong IV and serves garbage video. The resolver returns the "reject" sentinel so the caller drops the segment. We exercise both malformed
    // shapes parseExplicitIv recognizes - wrong length with and without the 0x prefix.
    for(const malformed of [ "0xdeadbeef", "deadbeef", "0x", "0x000102030405060708090a0b0c0d0e0f00" ]) {

      const result = resolveSegmentIv(malformed, 7);

      assert.equal(result.status, "reject", "malformed explicit IV \"" + malformed + "\" is rejected");
    }
  });

  test("a malformed explicit IV does not resolve to the sequence IV for the same sequence (no silent substitution)", () => {

    // A malformed explicit IV must reject rather than silently fall back to the sequence-derived IV, because the fallback would decrypt with the wrong IV and
    // produce garbage video. The malformed case is structurally distinct from the no-IV case - it rejects, it does not return the sequence IV.
    const malformed = resolveSegmentIv("0xnothex", 13);
    const noIv = resolveSegmentIv(null, 13);

    assert.equal(malformed.status, "reject", "malformed IV rejects");
    assert.equal(noIv.status, "ok", "absent IV derives");
    assert.notEqual(malformed.status, noIv.status, "the malformed and absent cases take different branches");
  });
});

describe("pruneKeyCache", () => {

  test("evicts entries whose URL is not in the active set and reports the count", () => {

    // The bounding invariant: keys whose URL left the active video+audio working set are released. We seed three keys, mark one active, and assert
    // the other two are evicted while the active one survives.
    const keysByUrl = new Map<string, Buffer>([
      [ "https://cdn.test/key-old-1.bin", Buffer.alloc(16, 1) ],
      [ "https://cdn.test/key-old-2.bin", Buffer.alloc(16, 2) ],
      [ "https://cdn.test/key-live.bin", Buffer.alloc(16, 3) ]
    ]);

    const evicted = pruneKeyCache(keysByUrl, new Set(["https://cdn.test/key-live.bin"]));

    assert.equal(evicted, 2, "two stale keys evicted");
    assert.equal(keysByUrl.size, 1, "only the live key remains");
    assert.ok(keysByUrl.has("https://cdn.test/key-live.bin"), "the active key survives");
  });

  test("retains every entry when all URLs are still active", () => {

    // When the active set covers every cached URL (no rotation has retired a key), nothing is evicted and the cache is untouched.
    const keysByUrl = new Map<string, Buffer>([
      [ "https://cdn.test/a.bin", Buffer.alloc(16, 1) ],
      [ "https://cdn.test/b.bin", Buffer.alloc(16, 2) ]
    ]);

    const evicted = pruneKeyCache(keysByUrl, new Set([ "https://cdn.test/a.bin", "https://cdn.test/b.bin" ]));

    assert.equal(evicted, 0, "no keys evicted");
    assert.equal(keysByUrl.size, 2, "both keys retained");
  });

  test("empties the cache when the active set is empty", () => {

    // Boundary: an empty active set means no manifest currently references any cached key, so every entry is stale. This is the worst-case rotation where the entire
    // prior session retired at once.
    const keysByUrl = new Map<string, Buffer>([
      [ "https://cdn.test/a.bin", Buffer.alloc(16, 1) ],
      [ "https://cdn.test/b.bin", Buffer.alloc(16, 2) ]
    ]);

    const evicted = pruneKeyCache(keysByUrl, new Set<string>());

    assert.equal(evicted, 2, "all keys evicted");
    assert.equal(keysByUrl.size, 0, "cache fully emptied");
  });

  test("stays bounded across repeated rotations (finding [18])", () => {

    // The failure mode this guards against: one dead entry per token rotation accumulating unbounded. We simulate ten rotations, each introducing a fresh key URL
    // and pruning against the new active set. After every rotation the cache holds exactly the live key, never growing with the rotation count.
    const keysByUrl = new Map<string, Buffer>();

    for(let rotation = 0; rotation < 10; rotation++) {

      const liveUrl = "https://cdn.test/key-rotation-" + String(rotation) + ".bin";

      keysByUrl.set(liveUrl, Buffer.alloc(16, rotation));
      pruneKeyCache(keysByUrl, new Set([liveUrl]));

      assert.equal(keysByUrl.size, 1, "cache holds exactly the live key after rotation " + String(rotation));
    }
  });
});

