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
import { handleMpegTsStream, initSegmentChangesPipeline, resolveMpegTsInitSource } from "./mpegts.ts";
import { makeNativeIdentity, makeRegistryEntry } from "./registry.helpers.ts";
import { registerStream, unregisterStream } from "./registry.ts";
import { setChannelStreamId, terminateStream } from "./lifecycle.ts";
import { CONFIG } from "../config/index.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
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
     * { statusCode: 404, body: "Channel is disabled.", valid: false } and sendValidationError surfaces it. This asserts the disabled-channel rejection arm of
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
   * already asserts the rejection end-to-end via test/e2e/streaming/pretune.test.ts ("scheduled job for a channel filtered out by enabledServices does NOT
   * trigger pretune"); a unit-tier test here would have to import the full persistence harness and would not catch any failure mode the integration test does
   * not. Asserting the disabled-channel and unknown-channel rejection arms here is sufficient because they share the same sendValidationError downstream.
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
    const entry = makeRegistryEntry({ identity: makeNativeIdentity({ nativeContainer: "fmp4" }) });

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
    const entry = makeRegistryEntry({ identity: makeNativeIdentity({ nativeContainer: "fmp4" }) });

    registerStream(entry);
    storeNamedInitSegment(entry.id, "audio", "init-a0.mp4", Buffer.from("audio-moov"));

    try {

      assert.equal(resolveMpegTsInitSource(entry), null);
    } finally {

      unregisterStream(entry.id);
    }
  });

  test("returns null for a native ts stream, which uses the pass-through instead", () => {

    const entry = makeRegistryEntry({ identity: makeNativeIdentity({ nativeContainer: "ts" }) });

    assert.equal(resolveMpegTsInitSource(entry), null, "an MPEG-TS source needs no init");
  });

  test("returns null for a native stream with a null container (the degradation rule)", () => {

    // A null container reaches the delivery path only on a feed the DRM path abandoned; it degrades to today's pass-through rather than demanding an init.
    const entry = makeRegistryEntry({ identity: makeNativeIdentity({ nativeContainer: null }) });

    assert.equal(resolveMpegTsInitSource(entry), null);
  });

  test("ignores a stale capture init slot on a native ts stream", () => {

    /* The mode is what selects the source, not the presence of bytes. A native stream that somehow carries a capture-slot buffer must still resolve null, or a
     * pass-through stream would be routed into a remux it does not need.
     */
    const entry = makeRegistryEntry({ identity: makeNativeIdentity({ nativeContainer: "ts" }) });

    entry.hls.initSegment = Buffer.from("stale-capture-moov");

    assert.equal(resolveMpegTsInitSource(entry), null);
  });
});

describe("initSegmentChangesPipeline", () => {

  test("a connection primed with nothing ends on any initialization at all", () => {

    /* The native fallback direction. A pass-through connection was primed with no initialization because its source carried none, so the first initialization to
     * take effect says the stream now produces fMP4 that a video/mpeg socket cannot carry, whatever those bytes happen to be.
     */
    assert.equal(initSegmentChangesPipeline(null, Buffer.from("ftyp+moov")), true, "a pass-through connection ends on the first initialization to take effect");
  });

  test("a byte-identical initialization changes nothing, and the comparison reads content rather than identity", () => {

    /* The property the whole design rests on: a replacement whose encoder came back with the same parameters leaves every connection alone. The copy is what
     * makes the row meaningful - a comparison written against object identity would report a change here and end a connection that had no reason to end.
     */
    const primed = Buffer.from("ftyp+moov");

    assert.equal(initSegmentChangesPipeline(primed, Buffer.from(primed)), false, "a same-parameter replacement leaves the connection undisturbed");
  });

  test("an initialization whose bytes differ ends the connection", () => {

    /* The replacement direction. The connection's output was primed for one set of decoder parameters, and fragments built against another would not play, so
     * the connection ends and the player reconnects into the new pipeline.
     */
    assert.equal(initSegmentChangesPipeline(Buffer.from("ftyp+moov"), Buffer.from("ftyp+moov2")), true, "differing bytes put the connection on a new pipeline");
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
    const entry = makeRegistryEntry({ channelName: "fmp4-native-channel", identity: makeNativeIdentity({ nativeContainer: "fmp4" }) });

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
    const entry = makeRegistryEntry({ channelName: "ts-native-channel", identity: makeNativeIdentity({ nativeContainer: "ts" }) });

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

  test("a pass-through connection stops delivering and ends itself the first time an initialization takes effect", async () => {

    /* The native fallback read end to end through a real connection. A pass-through was built for a source that carries its codec configuration in every segment,
     * so the moment a capture's first moov lands the stream is producing fMP4 this connection cannot carry. The row reads its counts in sequence: delivery is
     * live before the announcement, the announcement ends the connection, nothing is delivered behind that end, and a second announcement adds nothing.
     */
    const entry = makeRegistryEntry({ channelName: "ts-flip-channel", identity: makeNativeIdentity({ nativeContainer: "ts" }) });

    entry.hls.signalInitSegmentReady();
    registerStream(entry);
    setChannelStreamId("ts-flip-channel", entry.id);

    try {

      const { end, req, res, triggerReqEvent, write } = makeReqRes({ ip: "192.168.1.50", params: { name: "ts-flip-channel" } });

      await handleMpegTsStream(req, res);

      entry.hls.segmentEmitter.emit("segment", "segment0.m4s", Buffer.from("segment-zero"));
      assert.equal(write.mock.calls.length, 1, "delivery is live while the pipeline beneath the connection is unchanged");

      entry.hls.segmentEmitter.emit("initSegment", Buffer.from("ftyp+moov"));
      assert.equal(end.mock.calls.length, 1, "the stream started producing fMP4, so the connection ended itself");

      entry.hls.segmentEmitter.emit("segment", "segment1.m4s", Buffer.from("segment-one"));
      assert.equal(write.mock.calls.length, 1, "and no segment is delivered behind the end");

      entry.hls.segmentEmitter.emit("initSegment", Buffer.from("ftyp+moov-again"));
      assert.equal(end.mock.calls.length, 1, "a second announcement adds nothing, because the end runs exactly once");

      triggerReqEvent("close");
      assert.equal(entry.mpegTsClientCount, 0, "and the close that follows reaches cleanup through the request's close event");
    } finally {

      terminateStream(entry.id, "ts-flip-channel", "test cleanup");
      unregisterStream(entry.id);
    }
  });

  test("termination and a pipeline change share one funnel, so the connection ends exactly once", async () => {

    /* The triggers meeting at the same callback. Termination ends the connection first, and the announcement that follows must add nothing - a second end on
     * an output already closed is what a funnel with two entrances rather than one would produce.
     */
    const entry = makeRegistryEntry({ channelName: "ts-terminate-channel", identity: makeNativeIdentity({ nativeContainer: "ts" }) });

    entry.hls.signalInitSegmentReady();
    registerStream(entry);
    setChannelStreamId("ts-terminate-channel", entry.id);

    try {

      const { end, req, res } = makeReqRes({ ip: "192.168.1.50", params: { name: "ts-terminate-channel" } });

      await handleMpegTsStream(req, res);

      entry.hls.segmentEmitter.emit("terminated");
      assert.equal(end.mock.calls.length, 1, "the terminated event ended the connection");

      entry.hls.segmentEmitter.emit("initSegment", Buffer.from("ftyp+moov"));
      assert.equal(end.mock.calls.length, 1, "and the pipeline-change trigger found the connection already ending");
    } finally {

      terminateStream(entry.id, "ts-terminate-channel", "test cleanup");
      unregisterStream(entry.id);
    }
  });
});
