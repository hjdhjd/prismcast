/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mpegts.test.ts: Unit tests for the MPEG-TS stream route handler. mpegts.ts exposes one public function - handleMpegTsStream - which validates the channel,
 * flushes HTTP headers, calls into initializeStream() to start a new stream, and serves either FFmpeg-remuxed or native pass-through MPEG-TS bytes to the client.
 * The full happy path requires a real Chrome instance and FFmpeg subprocess and is deferred to e2e. The unit tests cover the synchronous validation branches:
 * missing channel name, channel name not present, the existing-stream fast path (validation bypass), and the disabled-channel rejection arm that flows through
 * sendValidationError. Login mode is validated through the same shared hls.ts validation path and is covered separately, not here; the service-filter rejection arm is
 * likewise not unit-tested in this file (see the explanatory block comment below).
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { handleMpegTsStream, resolveMpegTsInitSource } from "./mpegts.ts";
import { registerStream, unregisterStream } from "./registry.ts";
import { setChannelStreamId, terminateStream } from "./lifecycle.ts";
import { CONFIG } from "../config/index.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { makeRegistryEntry } from "./registry.helpers.ts";
import { makeReqRes } from "../routes/express.helpers.ts";
import { storeNamedInitSegment } from "./hlsSegments.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("handleMpegTsStream", () => {

  // Snapshot CONFIG.channels.disabledPredefined so tests that toggle it can restore the prior value in afterEach. Each test that mutates the array reassigns
  // it; the snapshot ensures sibling tests start from a consistent baseline regardless of execution order.
  let savedDisabled: string[] = [];

  beforeEach(() => {

    savedDisabled = [...CONFIG.channels.disabledPredefined];
  });

  afterEach(() => {

    CONFIG.channels.disabledPredefined = savedDisabled;
  });

  test("returns 400 when the channel name is missing from req.params", async () => {

    // Negative test: the handler reads (req.params as { name?: string }).name. An undefined value should produce 400 rather than crash.
    const { req, res, send, status } = makeReqRes({ ip: "192.168.1.50" });

    await handleMpegTsStream(req, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
    assert.equal(send.mock.calls[0]?.arguments[0], "Channel name is required.");
  });

  test("returns 404 when validateChannel rejects an unknown channel", async () => {

    // The handler delegates channel validation to validateChannel(). For an unknown channel, validateChannel returns { statusCode: 404, body: "Channel not
    // found.", valid: false } and sendValidationError surfaces it as a 404 response.
    const { req, res, status } = makeReqRes({ ip: "192.168.1.50", params: { name: "totally-not-a-real-channel" } });

    await handleMpegTsStream(req, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 404);
  });

  test("returns 404 'Channel is disabled.' when the channel is in disabledPredefined and no stream exists", async () => {

    /* The handler delegates to validateChannel(). For a predefined channel name in CONFIG.channels.disabledPredefined, validateChannel returns
     * { statusCode: 404, body: "Channel is disabled.", valid: false } and sendValidationError surfaces it. This pins the disabled-channel rejection arm of
     * the shared validation path (mirrors the hls.ts disabled-channel rejection through the same shared validation code path).
     */
    CONFIG.channels.disabledPredefined = ["abc"];

    const { req, res, send, status } = makeReqRes({ ip: "192.168.1.50", params: { name: "abc" } });

    await handleMpegTsStream(req, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 404);
    assert.equal(send.mock.calls[0]?.arguments[0], "Channel is disabled.");
  });

  /* The "Channel not available." 404 from the service-filter rejection arm is NOT unit-tested here because the predicate (isChannelAvailableByService) flows
   * through getChannelServiceTags, which depends on serviceGroups - a Map populated only by buildServiceGroups during persistence boot. Without that boot,
   * standalone channels fall back to the "direct" tag which is always-enabled, so the rejection arm never fires from a synthetic seed. The integration tier
   * already pins the rejection end-to-end via test/e2e/streaming/pretune.test.ts ("scheduled job for a channel filtered out by enabledServices does NOT
   * trigger pretune"); a unit-tier test here would have to import the full persistence harness and would not catch any failure mode the integration test does
   * not. Pinning the disabled-channel and unknown-channel rejection arms here is sufficient because they share the same sendValidationError downstream.
   */

  test("existing-stream fast path bypasses validateChannel even for a disabled channel", async () => {

    /* The handler's first decision after extracting the channel name is to look up an existing stream by channel name; if one exists, it skips validateChannel
     * entirely and goes straight to serveMpegTsStream. We register a synthetic stream entry for "abc" AND mark "abc" as disabled, then call the handler. If
     * the validation step fired, we would see status(404) with body "Channel is disabled." The fast path skips that, advances into serveMpegTsStream, and -
     * because our synthetic entry has no real init segment - returns status(500) "Stream no longer available." The presence of 500 (not 404) is the
     * observable proof that validation was bypassed; the body assertion narrows further so a regression that fails validation in a different way (e.g., 404
     * with a different message) would still be caught.
     */
    CONFIG.channels.disabledPredefined = ["abc"];

    const entry = makeRegistryEntry({ channelName: "abc" });

    // Signal init-segment readiness up front so serveMpegTsStream's waitForInitSegment(navigationTimeout) returns true immediately - otherwise the test would
    // wait the full navigationTimeout (10s default) before failing.
    entry.hls.signalInitSegmentReady();
    registerStream(entry);
    setChannelStreamId("abc", entry.id);

    try {

      const { req, res, send, status } = makeReqRes({ ip: "192.168.1.50", params: { name: "abc" } });

      await handleMpegTsStream(req, res);

      assert.equal(status.mock.calls[0]?.arguments[0], 500, "fast path bypassed validation; serveMpegTsStream ran and rejected on missing init segment");
      assert.equal(send.mock.calls[0]?.arguments[0], "Stream no longer available.");
    } finally {

      // Tear down the synthetic stream so subsequent tests start clean. terminateStream handles the channelToStreamId index plus registry state in one call.
      terminateStream(entry.id, "abc", "test cleanup");
      unregisterStream(entry.id);
    }
  });
});

describe("resolveMpegTsInitSource (T14)", () => {

  test("returns the capture path's single init slot for a capture stream", () => {

    const entry = makeRegistryEntry();
    const captureInit = Buffer.from("capture-moov");

    entry.hls.initSegment = captureInit;

    assert.deepEqual(resolveMpegTsInitSource(entry), captureInit);
  });

  test("returns null for a capture stream with no init stored", () => {

    assert.equal(resolveMpegTsInitSource(makeRegistryEntry()), null);
  });

  test("resolves the VIDEO track's init for a native fmp4 stream with both tracks populated", () => {

    /* Both tracks carry distinct bytes on purpose. With only one track populated, a resolver that returned "whichever track has something" would be
     * indistinguishable from one that correctly resolves video - the two-track fixture is what tells them apart.
     */
    const entry = makeRegistryEntry({ nativeContainer: "fmp4", streamingMode: "native" });

    registerStream(entry);

    const videoInit = Buffer.from("video-moov");

    storeNamedInitSegment(entry.id, "audio", "init-a0.mp4", Buffer.from("audio-moov"));
    storeNamedInitSegment(entry.id, "video", "init-v0.mp4", videoInit);

    try {

      assert.deepEqual(resolveMpegTsInitSource(entry), videoInit, "the video track's init is what the remuxer receives");
    } finally {

      unregisterStream(entry.id);
    }
  });

  test("returns null for a native fmp4 stream whose video init has not arrived (the guard rejects)", () => {

    // An audio-only arrival must not satisfy the remux path; the caller's guard turns this null into a 500 rather than priming FFmpeg with the wrong track.
    const entry = makeRegistryEntry({ nativeContainer: "fmp4", streamingMode: "native" });

    registerStream(entry);
    storeNamedInitSegment(entry.id, "audio", "init-a0.mp4", Buffer.from("audio-moov"));

    try {

      assert.equal(resolveMpegTsInitSource(entry), null);
    } finally {

      unregisterStream(entry.id);
    }
  });

  test("returns null for a native ts stream, which rides the pass-through instead", () => {

    const entry = makeRegistryEntry({ nativeContainer: "ts", streamingMode: "native" });

    assert.equal(resolveMpegTsInitSource(entry), null, "an MPEG-TS source needs no init");
  });

  test("returns null for a native stream with a null container (the degradation rule)", () => {

    // A null container reaches the delivery path only on a feed the DRM path abandoned; it degrades to today's pass-through rather than demanding an init.
    const entry = makeRegistryEntry({ nativeContainer: null, streamingMode: "native" });

    assert.equal(resolveMpegTsInitSource(entry), null);
  });

  test("ignores a stale capture init slot on a native ts stream", () => {

    /* The mode is what selects the source, not the presence of bytes. A native stream that somehow carries a capture-slot buffer must still resolve null, or a
     * pass-through stream would be routed into a remux it does not need.
     */
    const entry = makeRegistryEntry({ nativeContainer: "ts", streamingMode: "native" });

    entry.hls.initSegment = Buffer.from("stale-capture-moov");

    assert.equal(resolveMpegTsInitSource(entry), null);
  });
});

describe("serveMpegTsStream: container-aware branch selection", () => {

  test("a native fMP4 stream with no video init reaches the remux guard rather than the pass-through", async () => {

    /* Branch selection reads the container, so an fMP4 relay must NOT take the pass-through that pipes segments raw - its fragments are not MPEG-TS. With no
     * video initialization stored, falling through lands on the remux guard, whose 500 is the observable proof the pass-through was not taken. A container-blind
     * brancher would serve this connection as pass-through and never reach a 500.
     *
     * No FFmpeg is involved: the guard returns before the binary is ever resolved.
     */
    const entry = makeRegistryEntry({ channelName: "fmp4-native-channel", nativeContainer: "fmp4", streamingMode: "native" });

    entry.hls.signalInitSegmentReady();
    registerStream(entry);
    setChannelStreamId("fmp4-native-channel", entry.id);

    try {

      const { req, res, send, status } = makeReqRes({ ip: "192.168.1.50", params: { name: "fmp4-native-channel" } });

      await handleMpegTsStream(req, res);

      assert.equal(status.mock.calls[0]?.arguments[0], 500, "the fMP4 stream fell through to the remux guard");
      assert.equal(send.mock.calls[0]?.arguments[0], "Stream no longer available.");
    } finally {

      terminateStream(entry.id, "fmp4-native-channel", "test cleanup");
      unregisterStream(entry.id);
    }
  });

  test("a native MPEG-TS stream takes the pass-through and is never rejected by the remux guard", async () => {

    /* The contrasting case that gives the pair its distinguishing power: an MPEG-TS relay has no initialization and must still be served. If branch selection
     * ignored the container, this stream would hit the remux guard and be rejected with a 500 despite being perfectly serveable.
     */
    const entry = makeRegistryEntry({ channelName: "ts-native-channel", nativeContainer: "ts", streamingMode: "native" });

    entry.hls.signalInitSegmentReady();
    registerStream(entry);
    setChannelStreamId("ts-native-channel", entry.id);

    try {

      const { req, res, status } = makeReqRes({ ip: "192.168.1.50", params: { name: "ts-native-channel" } });

      await handleMpegTsStream(req, res);

      assert.equal(status.mock.calls.length, 0, "the pass-through serves the stream rather than rejecting it");
    } finally {

      terminateStream(entry.id, "ts-native-channel", "test cleanup");
      unregisterStream(entry.id);
    }
  });
});
