/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * preroll.populated.test.ts: Unit tests for preroll generation and the populated-variant paths in preroll.ts. generatePreroll takes its encoder as a port, so
 * each row hands it the synthetic encoder from preroll.helpers.ts and drives the real resolve, split and store path with no FFmpeg binary: the no-binary skip,
 * the failed and incomplete encodes that degrade to no variant, and a complete encode whose variant every populated reader, the progressive playlist and the
 * routes then answer from.
 *
 * The variant cache lives for the process and has no reset, so the rows run in order: every degradation first, while the cache is still empty, and the complete
 * encode last. The no-variant branches stay in preroll.test.ts, which needs the cache empty for the whole file.
 */
import { LOG, formatResolution } from "../utils/index.ts";
import { SYNTHETIC_FFMPEG_PATH, makeSyntheticFmp4, makeSyntheticPrerollEncoder, syntheticInitSegment } from "./preroll.helpers.ts";
import { buildPrerollInitUri, computeProgressiveReveal, generatePreroll, generatePrerollPlaylist, getPrerollCodec, getPrerollMaxDuration, getPrerollSegmentCount,
  getPrerollSegmentDuration, getPrerollTotalDurationSec, isPrerollReady, setupPrerollRoutes } from "./preroll.ts";
import { describe, test } from "node:test";
import { makeExpressStub, makeReqRes } from "../routes/express.helpers.ts";
import { CONFIG } from "../config/index.ts";
import type { Express } from "express";
import assert from "node:assert/strict";
import { getPresetViewport } from "../config/presets.ts";

// The reference instant the playlist rows count from, so the reveal reads as an offset from the preroll's start.
const BASE_TIME_MS = 1700000000000;

// The external URL the playlist rows build their absolute URIs on.
const BASE_URL = "http://example.test:5589";

// The fragment durations the complete encode answers with: one short fragment among full ones, so a row can tell a stored duration apart from the fallback.
const FRAGMENT_DURATIONS = [ 2, 2, 1.5, 2, 2, 2 ];

describe("generatePreroll and the populated variant", () => {

  test("skips generation with one warning and encodes nothing when no FFmpeg resolves", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const encoder = makeSyntheticPrerollEncoder({ binary: null, output: makeSyntheticFmp4(FRAGMENT_DURATIONS) });

    await generatePreroll(encoder);

    assert.equal(encoder.runs.length, 0, "no encode ran without a binary");
    assert.equal(warn.mock.callCount(), 1, "one warning reports the skip");
    assert.match(String(warn.mock.calls[0]?.arguments[0]), /^No FFmpeg is available for preroll generation/, "and it is the no-binary warning");
    assert.equal(isPrerollReady("h264"), false, "and no variant was stored");
  });

  test("degrades to no variant with a warning naming the codec when the encode fails", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const encoder = makeSyntheticPrerollEncoder({ output: new Error("The encoder crashed.") });

    await generatePreroll(encoder);

    assert.equal(encoder.runs.length, 1, "the baseline variant's encode ran once");
    assert.equal(warn.mock.callCount(), 1, "one warning reports the failure");
    assert.deepEqual(warn.mock.calls[0]?.arguments, [ "Preroll %s generation failed: %s.", "h264", "The encoder crashed." ], "naming the codec and the cause");
    assert.equal(isPrerollReady("h264"), false, "and no variant was stored");
  });

  test("degrades to no variant with a warning naming the codec when the encode yields no media segments", async (t) => {

    // The init segment alone is the output an encode killed before its first fragment leaves behind.
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });

    await generatePreroll(makeSyntheticPrerollEncoder({ output: makeSyntheticFmp4([]) }));

    assert.deepEqual(warn.mock.calls.map((call) => call.arguments), [[ "Preroll %s generation produced incomplete output.", "h264" ]],
      "one warning reports the incomplete output for the codec");
    assert.equal(isPrerollReady("h264"), false, "and no variant was stored");
  });

  test("a complete encode is split and stored, and every populated reader answers from it", async () => {

    const output = makeSyntheticFmp4(FRAGMENT_DURATIONS);
    const encoder = makeSyntheticPrerollEncoder({ output });
    const viewport = getPresetViewport(CONFIG);

    await generatePreroll(encoder);

    // One encode, because the effective codec is the baseline where no browser has reported hardware encoding, run with the resolved binary at the preset's size.
    const [run] = encoder.runs;

    assert.equal(encoder.runs.length, 1, "the baseline variant's encode ran once");
    assert.ok(run, "and was recorded");
    assert.equal(run.ffmpegBin, SYNTHETIC_FFMPEG_PATH, "with the binary the resolver answered");
    assert.ok(run.args.includes("libx264"), "with the baseline codec's encoder");
    assert.ok(run.args.some((arg) => arg.includes("size=" + formatResolution(viewport.width, viewport.height))), "at the configured preset's size");

    assert.equal(isPrerollReady("h264"), true, "the variant is ready");
    assert.equal(getPrerollCodec(), "h264", "and it is the codec new streams take");
    assert.equal(getPrerollSegmentCount("h264"), FRAGMENT_DURATIONS.length, "one media segment per fragment");
    assert.deepEqual(FRAGMENT_DURATIONS.map((_duration, index) => getPrerollSegmentDuration("h264", index)), FRAGMENT_DURATIONS,
      "each segment carries its own fragment's duration, the short one included");
    assert.equal(getPrerollTotalDurationSec("h264"), 11.5, "the total sums the stored durations");
    assert.equal(getPrerollMaxDuration("h264"), 2, "the maximum is the longest stored duration, rounded up");
    assert.equal(computeProgressiveReveal({ codec: "h264", now: BASE_TIME_MS, prerollStartTime: BASE_TIME_MS }), 4, "the initial window is revealed at once");
    assert.equal(computeProgressiveReveal({ codec: "h264", now: BASE_TIME_MS + 3500, prerollStartTime: BASE_TIME_MS }), 5,
      "and the next segment once the elapsed time covers it");
  });

  test("the progressive playlist and the routes serve the stored variant", () => {

    const playlist = generatePrerollPlaylist({ baseUrl: BASE_URL, codec: "h264", now: BASE_TIME_MS, prerollStartTime: BASE_TIME_MS, resumePosition: null });
    const lines = playlist.split("\n");
    const segmentUrls = lines.filter((line) => line.startsWith(BASE_URL + "/preroll/"));

    assert.ok(lines.includes("#EXT-X-MAP:URI=\"" + buildPrerollInitUri(BASE_URL, "h264") + "\""), "the playlist maps the variant's init segment");
    assert.deepEqual(lines.filter((line) => line.startsWith("#EXTINF:")), [ "#EXTINF:2.000,", "#EXTINF:2.000,", "#EXTINF:1.500,", "#EXTINF:2.000," ],
      "the initial window lists the stored durations in order");
    assert.ok(lines.includes("#EXT-X-MEDIA-SEQUENCE:0"), "a fresh stream's playlist starts its media sequence at zero");
    assert.ok(!lines.includes("#EXT-X-DISCONTINUITY"), "and opens no discontinuity");

    const stub = makeExpressStub();

    setupPrerollRoutes(stub.app as Express);

    const initRoute = stub.routes.find((route) => route.path === "/preroll/:codec/init.mp4");
    const segmentRoute = stub.routes.find((route) => route.path === "/preroll/:codec/:segment");

    assert.ok(initRoute && segmentRoute, "both routes are registered");

    const init = makeReqRes({ params: { codec: "h264" } });

    void initRoute.handler(init.req, init.res);

    assert.ok((init.send.mock.calls[0]?.arguments[0] as Buffer).equals(syntheticInitSegment()), "the init route serves the init segment the split kept");

    // The last URL the playlist names, requested by its file name, is a segment the route serves: the playlist and the route agree on the extension.
    const fragmentBytes = (output: Buffer, index: number): Buffer => {

      const initLength = syntheticInitSegment().length;
      const fragmentLength = (output.length - initLength) / FRAGMENT_DURATIONS.length;

      return output.subarray(initLength + (index * fragmentLength), initLength + ((index + 1) * fragmentLength));
    };

    const lastUrl = segmentUrls.at(-1) ?? "";
    const named = makeReqRes({ params: { codec: "h264", segment: lastUrl.slice(lastUrl.lastIndexOf("/") + 1) } });

    void segmentRoute.handler(named.req, named.res);

    assert.ok((named.send.mock.calls[0]?.arguments[0] as Buffer).equals(fragmentBytes(makeSyntheticFmp4(FRAGMENT_DURATIONS), segmentUrls.length - 1)),
      "the segment route serves the fragment the playlist's last URL names");

    const beyond = makeReqRes({ params: { codec: "h264", segment: "segment" + String(FRAGMENT_DURATIONS.length) + ".m4s" } });

    void segmentRoute.handler(beyond.req, beyond.res);

    assert.equal(beyond.status.mock.calls[0]?.arguments[0], 404, "an index past the stored segments answers 404");
    assert.equal(beyond.send.mock.calls[0]?.arguments[0], "Preroll segment not found.", "with the index-range body");
  });
});
