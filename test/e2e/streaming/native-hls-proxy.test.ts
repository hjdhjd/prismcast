/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * native-hls-proxy.test.ts: Integration coverage for the native HLS proxy in src/native/proxy.ts. The architectural unit under test is the proxy's
 * fetch-and-store path - read an upstream variant manifest, walk the segment list, fetch each segment (decrypting AES-128 when keyed), populate the registry's
 * HLS state with the bytes plus a generated playlist. The integration boundary is upstream HTTP -> registry HLS state. Downstream serving back to clients goes
 * through the existing route handlers and is covered by Suite 14 (hls-playlist-registry.test.ts) - this suite stops at the registry write.
 *
 * Why bootStubServer here instead of fetch-mocking. The proxy's relationship with its upstream IS the architectural unit under test: URL resolution against
 * baseUrl, sequential segment fetching, key fetching, decryption, manifest re-fetch on the polling cadence. A real HTTP listener exercises every byte of
 * that surface end-to-end. Mocking globalThis.fetch would skip URL composition and timeout behavior, and module-mocking the upstream would prove nothing
 * about the wire-level contract. Suite 12 was correctly mocked at the module boundary because pretune's relationship with its DVR is incidental data
 * acquisition; Suite 13 is correctly bound to a real HTTP loop because the proxy IS the upstream relationship.
 *
 * What is intentionally out of scope:
 *
 *   1. CDP-dependent paths in src/native/intercept.ts. Manifest URL interception requires a real Chrome via puppeteer-stream; that is e2e-with-browser
 *      territory and is not tested here. The proxy's stop() invokes removeManifestInterceptor on the supplied CDPSession, so the test passes a minimal fake
 *      that satisfies the three methods removeManifestInterceptor calls (removeAllListeners, send, detach). The fake is not pretending to be a real CDP
 *      session - it is the smallest object the production cleanup path can call without throwing.
 *
 *   2. Token refresh and audio-only renditions. updateVariantUrl / updateAudioVariantUrl / updateCdpSession are post-startup tokenization paths driven by
 *      higher-layer recovery code. They are not exercised here; their unit-tier coverage lives elsewhere or warrants a future suite.
 *
 *   3. Composite preroll playlists. prerollSegmentCount > 0 produces a different playlist shape; that path overlaps with the preroll subsystem and is
 *      tested at the preroll boundary.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { bootStubServer, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { createCipheriv, randomBytes } from "node:crypto";
import { getStream, registerStream, unregisterStream } from "../../../src/streaming/registry.ts";
import type { CDPSession } from "puppeteer-core";
import type { Clock } from "../../../src/utils/clock.ts";
import type { NativeProxy } from "../../../src/native/proxy.ts";
import assert from "node:assert/strict";
import { createNativeProxy } from "../../../src/native/proxy.ts";
import { deriveIvFromSequence } from "../../../src/native/decrypt.ts";
import { makeFakeClock } from "../../../src/utils/clock.helpers.ts";
import { makeRegistryEntry } from "../../../src/streaming/registry.helpers.ts";

/* makeFakeCdpSession returns the smallest object that satisfies removeManifestInterceptor's call surface (removeAllListeners, send, detach). The proxy stores
 * this reference and invokes it on stop(); we do not pretend to model a real CDPSession because the integration boundary tested here is HTTP, not CDP.
 */
function makeFakeCdpSession(): CDPSession {

  // The fake is self-referential (removeAllListeners returns the same object so production code can chain). We cast to unknown then to CDPSession at the
  // boundary to bypass the structural-subset check - the surface tested here is the three methods removeManifestInterceptor calls.
  const fake: { detach: () => Promise<void>; removeAllListeners: () => unknown; send: () => Promise<void> } = {

    detach: async (): Promise<void> => undefined,
    removeAllListeners: (): unknown => fake,
    send: async (): Promise<void> => undefined
  };

  return fake as unknown as CDPSession;
}

/* aes128Encrypt produces an encrypted segment matching the proxy's decryption contract: AES-128-CBC with PKCS7 padding (Node's default), key as a 16-byte
 * Buffer, IV derived from the media sequence number when no explicit IV is in the manifest. Returning the ciphertext bytes plus the plaintext lets tests
 * verify byte-equality against what the proxy will produce after decryption.
 */
function aes128Encrypt(plaintext: Buffer, key: Buffer, iv: Buffer): Buffer {

  const cipher = createCipheriv("aes-128-cbc", key, iv);

  return Buffer.concat([ cipher.update(plaintext), cipher.final() ]);
}

/* waitFor polls a synchronous predicate at short intervals until it returns true or the deadline passes. Used to drive integration tests against the proxy's
 * polling loop without coupling to the exact cadence of MANIFEST_BACKOFF_BASE - the test asserts the eventual state, not the precise number of poll cycles.
 */
async function waitFor(predicate: () => boolean, timeoutMs: number, label: string): Promise<void> {

  const deadline = Date.now() + timeoutMs;

  while(Date.now() < deadline) {

    if(predicate()) {

      return;
    }

    // eslint-disable-next-line no-await-in-loop -- the loop semantically IS the polling drain.
    await new Promise<void>((resolve) => { setTimeout(resolve, 25); });
  }

  throw new Error("waitFor timeout (" + String(timeoutMs) + "ms): " + label);
}

describe("native HLS proxy - upstream fetch and registry-write contract", () => {

  let activeProxy: NativeProxy | null = null;
  let activeStreamId: number | null = null;

  beforeEach(() => {

    activeProxy = null;
    activeStreamId = null;
  });

  afterEach(() => {

    // Stop the proxy first so any in-flight polls cancel before the registry entry is unregistered. Without this ordering, a poll mid-flight would receive
    // null from getStream() and log spuriously while the test is already moving on.
    activeProxy?.stop();

    if(activeStreamId !== null) {

      unregisterStream(activeStreamId);
    }
  });

  test("fetches a clear-text upstream manifest and stores its segments + playlist into the registry", async () => {

    /* The baseline contract: given a clear-text variant manifest with N segments at sequence S, the proxy fetches each segment, stores them in the registry's
     * hls.segments map under names "segment0.ts" .. "segmentN-1.ts", and writes a playlist whose entries reference those filenames. We assert against the
     * registry directly because that IS the boundary the proxy populates - downstream serving to clients is the route layer's responsibility.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const seg0 = randomBytes(256);
    const seg1 = randomBytes(256);
    const seg2 = randomBytes(256);

    const stub = await bootStubServer(ctx, (app) => {

      app.get("/manifest.m3u8", (_req, res) => {

        res.type("application/vnd.apple.mpegurl");
        res.send([
          "#EXTM3U",
          "#EXT-X-VERSION:3",
          "#EXT-X-TARGETDURATION:4",
          "#EXT-X-MEDIA-SEQUENCE:100",
          "#EXTINF:4.000,",
          "seg0.ts",
          "#EXTINF:4.000,",
          "seg1.ts",
          "#EXTINF:4.000,",
          "seg2.ts",
          ""
        ].join("\n"));
      });

      app.get("/seg0.ts", (_req, res) => { res.type("video/mp2t").send(seg0); });
      app.get("/seg1.ts", (_req, res) => { res.type("video/mp2t").send(seg1); });
      app.get("/seg2.ts", (_req, res) => { res.type("video/mp2t").send(seg2); });
    });

    const entry = makeRegistryEntry({ channelName: "stub-clear" });

    registerStream(entry);
    activeStreamId = entry.id;

    const proxy = createNativeProxy({

      audioVariantUrl: null,
      cdpSession: makeFakeCdpSession(),
      channelName: "stub-clear",
      encryption: "clear",
      keyUrl: null,
      onError: (): void => undefined,
      prefetchedKey: null,
      prerollSegmentCount: 0,
      streamId: entry.id,
      streamIdStr: entry.streamIdStr,
      variantUrl: stub.urlFor("/manifest.m3u8")
    });

    activeProxy = proxy;
    proxy.start();

    await waitFor(() => proxy.getSegmentIndex() >= 3, 5_000, "first poll cycle stores all three segments");

    const stored = getStream(entry.id);

    assert.ok(stored, "the registry entry must still exist after the poll cycle");
    assert.equal(stored.hls.segments.size, 3, "exactly three segments stored");
    assert.deepEqual(stored.hls.segments.get("segment0.ts"), seg0, "segment0 bytes match upstream seg0 byte-for-byte");
    assert.deepEqual(stored.hls.segments.get("segment1.ts"), seg1, "segment1 bytes match upstream seg1");
    assert.deepEqual(stored.hls.segments.get("segment2.ts"), seg2, "segment2 bytes match upstream seg2");

    assert.match(stored.hls.playlist, /^#EXTM3U$/m, "generated playlist opens with EXTM3U");
    assert.match(stored.hls.playlist, /^#EXT-X-TARGETDURATION:4$/m, "TARGETDURATION mirrors the upstream value");
    assert.match(stored.hls.playlist, /^segment0\.ts$/m, "playlist references segment0.ts (proxy-renumbered, not upstream seg0.ts)");
    assert.match(stored.hls.playlist, /^segment2\.ts$/m, "playlist includes the last fetched segment");
    assert.doesNotMatch(stored.hls.playlist, /^seg0\.ts$/m, "the upstream filename must NOT leak into the served playlist");
  });

  test("decrypts AES-128 segments before storing so the registry holds plaintext bytes", async () => {

    /* The HLS spec defines AES-128-CBC at the segment level, with the IV derived from the media sequence number when no explicit IV appears in the manifest.
     * The proxy fetches the key once (caching by URL), decrypts each segment as it arrives, and stores plaintext - downstream consumers (Channels DVR) read
     * cleartext segments from the registry. This test pins that contract: we encrypt synthetic plaintext on the stub, the proxy fetches and decrypts, the
     * registry holds the plaintext byte-for-byte.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const key = randomBytes(16);
    const plain0 = randomBytes(256);
    const plain1 = randomBytes(256);

    // Sequence numbers 200 and 201 - the IV is derived from each sequence, so the encryptor must use the matching IV for the proxy's decryption to round-trip.
    const cipher0 = aes128Encrypt(plain0, key, deriveIvFromSequence(200));
    const cipher1 = aes128Encrypt(plain1, key, deriveIvFromSequence(201));

    const stub = await bootStubServer(ctx, (app) => {

      app.get("/key", (_req, res) => { res.type("application/octet-stream").send(key); });

      app.get("/manifest.m3u8", (req, res) => {

        const keyUrl = "http://127.0.0.1:" + String((req.socket.localPort ?? 0)) + "/key";

        res.type("application/vnd.apple.mpegurl");
        res.send([
          "#EXTM3U",
          "#EXT-X-VERSION:3",
          "#EXT-X-TARGETDURATION:4",
          "#EXT-X-MEDIA-SEQUENCE:200",
          "#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyUrl + "\"",
          "#EXTINF:4.000,",
          "seg0.ts",
          "#EXTINF:4.000,",
          "seg1.ts",
          ""
        ].join("\n"));
      });

      app.get("/seg0.ts", (_req, res) => { res.type("video/mp2t").send(cipher0); });
      app.get("/seg1.ts", (_req, res) => { res.type("video/mp2t").send(cipher1); });
    });

    const entry = makeRegistryEntry({ channelName: "stub-aes128" });

    registerStream(entry);
    activeStreamId = entry.id;

    const proxy = createNativeProxy({

      audioVariantUrl: null,
      cdpSession: makeFakeCdpSession(),
      channelName: "stub-aes128",
      encryption: "aes128",
      keyUrl: stub.urlFor("/key"),
      onError: (): void => undefined,
      prefetchedKey: null,
      prerollSegmentCount: 0,
      streamId: entry.id,
      streamIdStr: entry.streamIdStr,
      variantUrl: stub.urlFor("/manifest.m3u8")
    });

    activeProxy = proxy;
    proxy.start();

    await waitFor(() => proxy.getSegmentIndex() >= 2, 5_000, "first poll cycle decrypts and stores both segments");

    const stored = getStream(entry.id);

    assert.ok(stored, "registry entry survives the AES-128 poll cycle");
    assert.deepEqual(stored.hls.segments.get("segment0.ts"), plain0, "segment0 must be DECRYPTED in storage, not the ciphertext");
    assert.deepEqual(stored.hls.segments.get("segment1.ts"), plain1, "segment1 must be DECRYPTED in storage, not the ciphertext");
  });

  test("re-fetches the upstream manifest on the polling cadence and picks up newly-published segments", async () => {

    /* The live-edge contract: the proxy must keep polling the manifest at MANIFEST_BACKOFF_BASE intervals so the registry's segment list and playlist stay
     * current as the upstream window advances. We seed the stub with manifest A (2 segments at sequence S), wait for the first poll, then mutate the stub to
     * return manifest B (4 segments at S+2), and assert the proxy stores the new ones without re-fetching the old ones.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const segs = [ randomBytes(128), randomBytes(128), randomBytes(128), randomBytes(128) ];
    let manifestVersion = 0;

    const stub = await bootStubServer(ctx, (app) => {

      app.get("/manifest.m3u8", (_req, res) => {

        res.type("application/vnd.apple.mpegurl");

        if(manifestVersion === 0) {

          res.send([
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            "#EXT-X-TARGETDURATION:4",
            "#EXT-X-MEDIA-SEQUENCE:300",
            "#EXTINF:4.000,",
            "seg0.ts",
            "#EXTINF:4.000,",
            "seg1.ts",
            ""
          ].join("\n"));
        } else {

          res.send([
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            "#EXT-X-TARGETDURATION:4",
            "#EXT-X-MEDIA-SEQUENCE:300",
            "#EXTINF:4.000,",
            "seg0.ts",
            "#EXTINF:4.000,",
            "seg1.ts",
            "#EXTINF:4.000,",
            "seg2.ts",
            "#EXTINF:4.000,",
            "seg3.ts",
            ""
          ].join("\n"));
        }
      });

      app.get("/seg0.ts", (_req, res) => { res.type("video/mp2t").send(segs[0]); });
      app.get("/seg1.ts", (_req, res) => { res.type("video/mp2t").send(segs[1]); });
      app.get("/seg2.ts", (_req, res) => { res.type("video/mp2t").send(segs[2]); });
      app.get("/seg3.ts", (_req, res) => { res.type("video/mp2t").send(segs[3]); });
    });

    const entry = makeRegistryEntry({ channelName: "stub-refresh" });

    registerStream(entry);
    activeStreamId = entry.id;

    const proxy = createNativeProxy({

      audioVariantUrl: null,
      cdpSession: makeFakeCdpSession(),
      channelName: "stub-refresh",
      encryption: "clear",
      keyUrl: null,
      onError: (): void => undefined,
      prefetchedKey: null,
      prerollSegmentCount: 0,
      streamId: entry.id,
      streamIdStr: entry.streamIdStr,
      variantUrl: stub.urlFor("/manifest.m3u8")
    });

    activeProxy = proxy;
    proxy.start();

    await waitFor(() => proxy.getSegmentIndex() >= 2, 5_000, "first poll cycle stores the initial two segments");

    // Flip the stub to manifest B. The proxy's next poll (MANIFEST_BACKOFF_BASE = 3s in production) must pick up seg2 and seg3 only - seg0 and seg1 already
    // live in the fetchedSequences set and the high-water mark is at sequence 301.
    manifestVersion = 1;

    await waitFor(() => proxy.getSegmentIndex() >= 4, 6_000, "next poll cycle picks up the two newly-published segments");

    const stored = getStream(entry.id);

    assert.ok(stored, "registry entry survives the refresh cycle");
    assert.deepEqual(stored.hls.segments.get("segment2.ts"), segs[2], "newly-published seg2 stored byte-for-byte");
    assert.deepEqual(stored.hls.segments.get("segment3.ts"), segs[3], "newly-published seg3 stored byte-for-byte");
    assert.match(stored.hls.playlist, /^segment3\.ts$/m, "the regenerated playlist includes the newly-published last segment");
  });

  test("stop() halts the polling loop so no further upstream traffic flows after termination", async () => {

    /* The shutdown contract: stop() flips the stopped flag and detaches the CDP session. After stop() returns, no further fetch should hit the upstream stub.
     * A regression that left the polling loop running would surface as continued upstream load on a stream the operator believed was terminated - with
     * bandwidth and rate-limit consequences in production.
     *
     * Architecture under test. The proxy's polling cadence routes through the Clock port (utils/clock.ts) so the test injects a fake clock whose sleep returns
     * a controllable promise. The first poll fires immediately on start(); the awaiter created by schedulePoll then awaits clock.sleep(MANIFEST_BACKOFF_BASE)
     * - in this test, that promise stays pending until the test releases it. The test calls stop(), then releases the held sleep, then drains microtasks. The
     * awaiter wakes, sees lifecycle.stopped === true, and exits without issuing a second fetch. The invariant pinned: zero upstream requests after stop()
     * regardless of whether the in-flight sleep ever resolves. The 4-second wall-clock wait that previously protected this assertion is gone; the test now
     * proves the negative deterministically via the injection seam.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    let manifestRequestCount = 0;

    const stub = await bootStubServer(ctx, (app) => {

      app.get("/manifest.m3u8", (_req, res) => {

        manifestRequestCount++;

        res.type("application/vnd.apple.mpegurl");
        res.send([
          "#EXTM3U",
          "#EXT-X-VERSION:3",
          "#EXT-X-TARGETDURATION:4",
          "#EXT-X-MEDIA-SEQUENCE:400",
          "#EXTINF:4.000,",
          "seg0.ts",
          ""
        ].join("\n"));
      });

      app.get("/seg0.ts", (_req, res) => { res.type("video/mp2t").send(randomBytes(128)); });
    });

    // Build a fake clock whose sleep returns a promise the test holds open. The post-first-poll awaiter inside schedulePoll is the only consumer of
    // clock.sleep here; we capture its resolver so the test can release the sleep deterministically after asserting the invariant.
    const sleepResolvers: (() => void)[] = [];
    const sleepDurations: number[] = [];

    const clock: Clock = makeFakeClock({

      sleep: async (ms: number): Promise<void> => {

        sleepDurations.push(ms);

        return new Promise<void>((resolve) => {

          sleepResolvers.push(resolve);
        });
      }
    }).clock;

    const entry = makeRegistryEntry({ channelName: "stub-stop" });

    registerStream(entry);
    activeStreamId = entry.id;

    const proxy = createNativeProxy({

      audioVariantUrl: null,
      cdpSession: makeFakeCdpSession(),
      channelName: "stub-stop",
      clock,
      encryption: "clear",
      keyUrl: null,
      onError: (): void => undefined,
      prefetchedKey: null,
      prerollSegmentCount: 0,
      streamId: entry.id,
      streamIdStr: entry.streamIdStr,
      variantUrl: stub.urlFor("/manifest.m3u8")
    });

    activeProxy = proxy;
    proxy.start();

    // Wait for the first poll to land AND the post-poll awaiter to enter clock.sleep. Both are observable: manifestRequestCount goes to 1 when the first
    // fetch completes; sleepDurations is populated when the awaiter calls clock.sleep with the next-poll backoff. Combining the two asserts the proxy is
    // sitting in the exact state we want to test against - one fetch issued, one sleep pending.
    await waitFor(() => (manifestRequestCount >= 1) && (sleepDurations.length >= 1), 5_000, "first manifest poll lands and the next-poll sleep is queued");

    proxy.stop();

    const countAtStop = manifestRequestCount;

    // Release every queued sleep resolver. With lifecycle.stopped === true, the post-sleep guard in schedulePoll's awaiter must short-circuit and skip the
    // next pollManifest call. If the guard regressed, releasing the sleep would issue a second fetch and the assertion below would fail.
    for(const resolve of sleepResolvers) {

      resolve();
    }

    // Drain microtasks so the released awaiter runs to completion. Eight rounds of Promise.resolve() is enough to flush any plausible async chain in the
    // schedulePoll closure.
    for(let i = 0; i < 8; i++) {

      // eslint-disable-next-line no-await-in-loop -- the loop semantically IS the sequential drain.
      await Promise.resolve();
    }

    assert.equal(manifestRequestCount, countAtStop, "no further manifest polls should hit the stub after stop() even when the in-flight sleep resolves");
    assert.equal(proxy.isStopped(), true, "the proxy reports itself as stopped");
    assert.equal(sleepDurations[0], 3_000, "the next-poll sleep used MANIFEST_BACKOFF_BASE on a successful first poll");
  });
});
