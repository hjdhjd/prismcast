/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mpegts.test.ts: Unit tests for the MPEG-TS stream route handler. mpegts.ts exposes one public function - handleMpegTsStream - which validates the channel,
 * flushes HTTP headers, calls into initializeStream() to start a new stream, and serves either FFmpeg-remuxed or native pass-through MPEG-TS bytes to the client.
 * The full happy path requires a real Chrome instance and FFmpeg subprocess and is deferred to e2e. The unit tests cover the synchronous validation branches:
 * missing channel name, channel name not present, the existing-stream fast path (validation bypass), and the disabled-channel rejection arm that flows through
 * sendValidationError. Login mode is validated through the same shared hls.ts seam and is covered separately, not here; the service-filter rejection arm is
 * likewise not unit-tested in this file (see the explanatory block comment below).
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { registerStream, unregisterStream } from "./registry.ts";
import { setChannelStreamId, terminateStream } from "./lifecycle.ts";
import { CONFIG } from "../config/index.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { handleMpegTsStream } from "./mpegts.ts";
import { makeRegistryEntry } from "./registry.helpers.ts";
import { makeReqRes } from "../routes/express.helpers.ts";

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
     * the validation seam (mirrors the hls.ts disabled-channel rejection through the same shared validation code path).
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
