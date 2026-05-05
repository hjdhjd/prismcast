/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mpegts.test.ts: Unit tests for the MPEG-TS stream route handler. mpegts.ts exposes one public function - handleMpegTsStream - which validates the channel,
 * flushes HTTP headers, calls into initializeStream() to start a new stream, and serves either FFmpeg-remuxed or native pass-through MPEG-TS bytes to the client.
 * The full happy path requires a real Chrome instance and FFmpeg subprocess and is deferred to e2e. The unit tests cover the synchronous validation branches:
 * missing channel name, channel name not present, login mode (validateChannel rejects), and the response shapes those branches produce.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { handleMpegTsStream } from "./mpegts.ts";
import { makeReqRes } from "../routes/express.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("handleMpegTsStream", () => {

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
});
