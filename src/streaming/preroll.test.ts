/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * preroll.test.ts: Unit tests for the preroll compositor and accessor functions in preroll.ts. The pure-function exports - getPrerollSegmentCount,
 * getPrerollSegmentDuration, getPrerollTotalDurationSec, getPrerollMaxDuration, getPrerollCodec, isPrerollReady, computePrerollWindow, buildPrerollEntries,
 * computeProgressiveReveal - earn full coverage here. setupPrerollRoutes is also unit-tested here against an Express stub, covering route registration and
 * each 404 branch. spawnAndCollect's deadline and collection semantics are exercised directly with Node child processes; only real-FFmpeg encoding
 * (generatePreroll) remains deferred to e2e.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { buildPrerollEntries, computePrerollWindow, computeProgressiveReveal, computeReveal, generatePrerollPlaylist, getPrerollCodec, getPrerollMaxDuration,
  getPrerollSegmentCount, getPrerollSegmentDuration, getPrerollTotalDurationSec, isPrerollReady, setupPrerollRoutes, spawnAndCollect } from "./preroll.ts";
import { makeExpressStub, makeReqRes } from "../routes/express.helpers.ts";
import type { Express } from "express";
import assert from "node:assert/strict";

describe("isPrerollReady", () => {

  test("returns false for a codec that has not been generated", () => {

    // The preroll variants Map starts empty in tests because generatePreroll() spawns FFmpeg, which we do not run. Locks the negative branch.
    assert.equal(isPrerollReady("h264"), false);
    assert.equal(isPrerollReady("hevc"), false);
  });
});

describe("getPrerollSegmentCount", () => {

  test("returns 0 for a codec without a generated variant", () => {

    // Boundary: caller queries the count for an unprepared codec. The function uses ?? 0 to map an absent variant to a count of 0.
    assert.equal(getPrerollSegmentCount("h264"), 0);
    assert.equal(getPrerollSegmentCount("hevc"), 0);
  });
});

describe("getPrerollSegmentDuration", () => {

  test("returns the documented fallback (2s) when the codec has no variant", () => {

    // Boundary: when no variant exists, the function falls back to 2 seconds rather than throwing or returning undefined. The 2-second fallback matches the
    // typical fMP4 segment duration so playlist math stays consistent.
    assert.equal(getPrerollSegmentDuration("h264", 0), 2);
    assert.equal(getPrerollSegmentDuration("h264", 100), 2);
  });
});

describe("getPrerollTotalDurationSec", () => {

  test("returns 0 when no variant exists for the codec", () => {

    assert.equal(getPrerollTotalDurationSec("h264"), 0);
    assert.equal(getPrerollTotalDurationSec("hevc"), 0);
  });
});

describe("getPrerollMaxDuration", () => {

  test("returns the documented fallback (2) when no variant exists", () => {

    // Boundary: same fallback as getPrerollSegmentDuration. Locks the consistent 2-second-fallback contract that callers rely on for TARGETDURATION computation.
    assert.equal(getPrerollMaxDuration("h264"), 2);
    assert.equal(getPrerollMaxDuration("hevc"), 2);
  });
});

describe("getPrerollCodec", () => {

  test("returns 'h264' as the default fallback when no variant has been generated", () => {

    // The function chains: preferred (from getEffectiveCaptureCodec) -> fallback (other) -> "h264". With no variants generated, every branch falls through to
    // the literal "h264".
    assert.equal(getPrerollCodec(), "h264");
  });
});

describe("computePrerollWindow", () => {

  test("returns 0 when no preroll cap applies and no scrolling is needed (small prerollSegmentCount)", () => {

    // Happy path: with prerollSegmentCount = 0 (no preroll active) and a small currentSegmentIndex, every term in the Math.max collapses to <= 0 and the start
    // index is 0.
    const start = computePrerollWindow({


      currentSegmentIndex: 3,
      maxSegments: 10,
      prerollSegmentCount: 0,
      realSegmentCount: 5
    });

    assert.equal(start, 0);
  });

  test("computes start = currentSegmentIndex - windowSize when sliding window applies", () => {

    // currentSegmentIndex 20, total available 15 (5 preroll + 10 real), max 10. Window size = min(15, 10) = 10. Start should be 20 - 10 = 10.
    const start = computePrerollWindow({


      currentSegmentIndex: 20,
      maxSegments: 10,
      prerollSegmentCount: 5,
      realSegmentCount: 10
    });

    assert.equal(start, 10);
  });

  test("never returns a negative start index (Math.max with 0 floor)", () => {

    // Boundary: with prerollSegmentCount = 0 and a low currentSegmentIndex, the windowed term goes negative. The Math.max(0, ...) guards against this.
    const start = computePrerollWindow({


      currentSegmentIndex: 2,
      maxSegments: 10,
      prerollSegmentCount: 0,
      realSegmentCount: 5
    });

    assert.equal(start, 0);
  });

  test("applies the preroll cap (prerollSegmentCount - 3) to limit how many preroll entries appear", () => {

    // The MAX_PREROLL_IN_WINDOW constant inside preroll.ts is 3. With prerollSegmentCount = 10, the cap should force the start index >= 10 - 3 = 7. Locks the
    // explicit cap that prevents clients from playing through a long tail of preroll before reaching live content.
    const start = computePrerollWindow({


      currentSegmentIndex: 8,
      maxSegments: 100,
      prerollSegmentCount: 10,
      realSegmentCount: 0
    });

    assert.equal(start, 7, "cap forces start to prerollSegmentCount - 3");
  });

  test("preroll cap does not apply when prerollSegmentCount <= 3 (cap floor is non-negative)", () => {

    // Boundary: with prerollSegmentCount = 2, the cap calculation is max(2 - 3, 0) = 0. The cap effectively disappears for short preroll sequences.
    const start = computePrerollWindow({


      currentSegmentIndex: 1,
      maxSegments: 100,
      prerollSegmentCount: 2,
      realSegmentCount: 0
    });

    assert.equal(start, 0);
  });
});

describe("buildPrerollEntries", () => {

  test("returns an empty array when startIndex equals prerollSegmentCount (no entries left)", () => {

    // Boundary: the loop condition is `i < options.prerollSegmentCount`, so equal start and end produces zero iterations.
    const entries = buildPrerollEntries({


      baseUrl: "http://example.test:5589",
      codec: "h264",
      extension: ".m4s",
      prerollSegmentCount: 5,
      startIndex: 5
    });

    assert.deepEqual(entries, []);
  });

  test("returns one entry per segment from startIndex (inclusive) to prerollSegmentCount (exclusive)", () => {

    // With no variant generated, getPrerollSegmentDuration returns the 2-second fallback; we lock the URL construction here.
    const entries = buildPrerollEntries({


      baseUrl: "http://example.test:5589",
      codec: "h264",
      extension: ".m4s",
      prerollSegmentCount: 4,
      startIndex: 1
    });

    assert.equal(entries.length, 3, "entries from index 1, 2, 3");
    assert.equal(entries[0]?.url, "http://example.test:5589/preroll/h264/segment1.m4s");
    assert.equal(entries[1]?.url, "http://example.test:5589/preroll/h264/segment2.m4s");
    assert.equal(entries[2]?.url, "http://example.test:5589/preroll/h264/segment3.m4s");
  });

  test("uses the codec parameter in the URL path", () => {

    const entries = buildPrerollEntries({


      baseUrl: "http://example.test:5589",
      codec: "hevc",
      extension: ".m4s",
      prerollSegmentCount: 1,
      startIndex: 0
    });

    assert.equal(entries[0]?.url, "http://example.test:5589/preroll/hevc/segment0.m4s");
  });

  test("uses the extension parameter for the segment file extension", () => {

    // Parameterized for future format flexibility - locks the contract that .m4s vs other extensions is caller-controlled.
    const entries = buildPrerollEntries({


      baseUrl: "http://example.test:5589",
      codec: "h264",
      extension: ".future-format",
      prerollSegmentCount: 1,
      startIndex: 0
    });

    assert.equal(entries[0]?.url, "http://example.test:5589/preroll/h264/segment0.future-format");
  });

  test("each entry carries the segment duration from the cache (or 2s fallback)", () => {

    // With no variant cached, every entry gets the 2s fallback. Locks that buildPrerollEntries does not invent a duration.
    const entries = buildPrerollEntries({


      baseUrl: "http://example.test:5589",
      codec: "h264",
      extension: ".m4s",
      prerollSegmentCount: 3,
      startIndex: 0
    });

    for(const entry of entries) {

      assert.equal(entry.duration, 2, "fallback duration applied");
    }
  });
});

describe("computeProgressiveReveal", () => {

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("returns 0 when no variant has been generated for the codec", () => {

    // Boundary: the function returns 0 if the variant lookup fails. The compositor's downstream code treats 0 as "no segments visible yet."
    const reveal = computeProgressiveReveal("h264", new Date(1_700_000_000_000));

    assert.equal(reveal, 0);
  });

  test("returns 0 immediately when no variant exists regardless of elapsed time", () => {

    // Same negative path, but with elapsed time advanced. The variant absence dominates.
    mock.timers.tick(60_000);

    const reveal = computeProgressiveReveal("hevc", new Date(1_700_000_000_000));

    assert.equal(reveal, 0);
  });
});

describe("computeReveal", () => {

  test("reveals all segments when totalSegments <= initialWindow", () => {

    // Boundary: Math.min(initialWindow, totalSegments) collapses to totalSegments itself. A mutant that swapped min for max would reveal the default window (4)
    // instead of the true segment count (3).
    const reveal = computeReveal(3, [ 2, 2, 2 ], 0);

    assert.equal(reveal, 3, "all three segments revealed since totalSegments is below the initial window");
  });

  test("returns 0 when totalSegments is 0", () => {

    // Boundary: an empty (or not-yet-seeded) variant has no segments to reveal regardless of elapsed time.
    assert.equal(computeReveal(0, [], 0), 0);
    assert.equal(computeReveal(0, [], 60), 0);
  });

  test("reveals exactly the initial window at elapsedSec 0 when more segments remain", () => {

    // With 10 segments all lasting 2s and the default initial window of 4, no time has elapsed yet - only the initial window is visible.
    const durations = [ 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 ];
    const reveal = computeReveal(10, durations, 0);

    assert.equal(reveal, 4, "only the initial window is visible before any time elapses");
  });

  test("reveals one additional segment as elapsed time crosses each subsequent segment's cumulative threshold", () => {

    /* With 10 segments all lasting 2s and the default initial window of 4, the initial window's duration is 8s. Segment index 4 (the 5th segment, first one
     * beyond the window) becomes visible once elapsed time reaches 2s (8 + 2 - 8). Segment index 5 becomes visible once elapsed time reaches 4s. The strict "<"
     * comparison means "just below" the threshold must still show the prior count, and "at" the threshold must show the count one higher - a flipped comparison
     * (<=) or a dropped break would blur this boundary.
     */
    const durations = [ 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 ];

    assert.equal(computeReveal(10, durations, 1.999), 4, "just below the 2s threshold still shows only the initial window");
    assert.equal(computeReveal(10, durations, 2), 5, "at the 2s threshold, one more segment is revealed");
    assert.equal(computeReveal(10, durations, 3.999), 5, "just below the 4s threshold holds at 5");
    assert.equal(computeReveal(10, durations, 4), 6, "at the 4s threshold, one more segment is revealed");
  });

  test("falls back to a 2-second duration for entries missing from a short durations array", () => {

    /* totalSegments is 6 but durations only covers the first 4 entries (the initial window). Segments 4 and 5 fall through the "?? 2" fallback at both
     * summation sites. If the fallback were dropped, durations[4] would be undefined and the cumulative-duration arithmetic would produce NaN, which fails
     * every "<" comparison and would reveal all 6 segments immediately at elapsedSec 0 instead of holding at the initial window.
     */
    const durations = [ 2, 2, 2, 2 ];

    assert.equal(computeReveal(6, durations, 0), 4, "elapsedSec 0 holds at the initial window even with defaulted trailing durations");
    assert.equal(computeReveal(6, durations, 2), 5, "the defaulted 2s duration for segment 4 still drives the reveal threshold at 2s");
    assert.equal(computeReveal(6, durations, 4), 6, "the defaulted 2s duration for segment 5 still drives the reveal threshold at 4s");
  });

  test("reveals just the initial window when elapsedSec is negative (future start time)", () => {

    // A negative elapsedSec (prerollStartTime in the future) must not reveal anything beyond the initial window - every progressive threshold check fails.
    const durations = [ 2, 2, 2, 2, 2, 2, 2, 2 ];

    assert.equal(computeReveal(8, durations, -5), 4, "negative elapsed time reveals only the initial window");
  });
});

describe("generatePrerollPlaylist", () => {

  test("returns an empty string when no variant has been generated for the codec (early-return branch)", () => {

    /* The composite preroll playlist is only meaningful when actual fMP4 segments have been encoded; with no variant in the cache, the function short-circuits
     * with the empty string and the caller (registerPendingStream) decides whether to fall back to a blocking real-stream wait. The other composition steps
     * (computeProgressiveReveal, buildPrerollEntries, buildPlaylist) collectively require seeded variants - exercising those branches honestly would require
     * spawning FFmpeg, which belongs to integration coverage rather than this unit suite. The early-return path is the one observable surface the unit tier
     * can pin without that subprocess.
     */
    const playlist = generatePrerollPlaylist("http://example.test:5589", "h264", 0, new Date(1_700_000_000_000));

    assert.equal(playlist, "", "no variant -> empty playlist string");
  });

  test("returns an empty string for the alternate codec when neither variant is generated", () => {

    // Companion to the previous test: locks the contract that both codec branches share the same early-return semantics. A regression that hard-coded
    // "h264" in the readiness check would still pass the test above but fail here.
    const playlist = generatePrerollPlaylist("http://example.test:5589", "hevc", 100, new Date(1_700_000_000_000));

    assert.equal(playlist, "", "hevc without a variant also returns the empty string");
  });
});

describe("setupPrerollRoutes", () => {

  test("registers GET /preroll/:codec/init.mp4 and GET /preroll/:codec/:segment on the Express app", () => {

    /* The route registration is the structural contract: the preroll subsystem owns these two URL spaces and nothing else. A regression that renamed or moved
     * a route would surface here as a missing entry in the captured calls list. We assert both routes were registered as GETs at exactly the documented paths.
     */
    const stub = makeExpressStub();

    setupPrerollRoutes(stub.app as Express);

    const initRoute = stub.calls.find((c) => (c.path === "/preroll/:codec/init.mp4"));
    const segmentRoute = stub.calls.find((c) => (c.path === "/preroll/:codec/:segment"));

    assert.ok(initRoute, "init.mp4 route registered");
    assert.equal(initRoute.method, "get", "init.mp4 served via GET");
    assert.ok(segmentRoute, "segment route registered");
    assert.equal(segmentRoute.method, "get", "segment served via GET");
  });

  test("init.mp4 returns 404 'Preroll not available.' for an unknown codec param", () => {

    /* The codec param is gated by the runtime check ((codec === "h264") || (codec === "hevc")). Anything else - "av1", "foo", undefined - must produce 404
     * rather than crashing the lookup. Pins the input-validation branch.
     */
    const stub = makeExpressStub();

    setupPrerollRoutes(stub.app as Express);

    const initRoute = stub.routes.find((r) => (r.path === "/preroll/:codec/init.mp4"));

    assert.ok(initRoute, "init.mp4 handler captured");

    const { req, res, send, status } = makeReqRes({ params: { codec: "av1" } });

    void initRoute.handler(req, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 404, "unknown codec returns 404");
    assert.equal(send.mock.calls[0]?.arguments[0], "Preroll not available.");
  });

  test("init.mp4 returns 404 when the variant Map has no entry for a recognized codec (variant not generated)", () => {

    /* Even when the codec param is recognized, the prerollVariants Map starts empty in tests because generatePreroll() is never called. The handler hits the
     * `if(!variant)` 404 branch.
     */
    const stub = makeExpressStub();

    setupPrerollRoutes(stub.app as Express);

    const initRoute = stub.routes.find((r) => (r.path === "/preroll/:codec/init.mp4"));

    assert.ok(initRoute, "init.mp4 handler captured");

    const { req, res, send, status } = makeReqRes({ params: { codec: "h264" } });

    void initRoute.handler(req, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 404, "h264 with no generated variant returns 404");
    assert.equal(send.mock.calls[0]?.arguments[0], "Preroll not available.");
  });

  test("segment route returns 404 'Preroll not available.' for an unknown codec param", () => {

    // Same codec validation as init.mp4. The segment route's own filename validation only runs after the codec/variant gate passes.
    const stub = makeExpressStub();

    setupPrerollRoutes(stub.app as Express);

    const segmentRoute = stub.routes.find((r) => (r.path === "/preroll/:codec/:segment"));

    assert.ok(segmentRoute, "segment handler captured");

    const { req, res, send, status } = makeReqRes({ params: { codec: "vp9", segment: "segment0.m4s" } });

    void segmentRoute.handler(req, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 404);
    assert.equal(send.mock.calls[0]?.arguments[0], "Preroll not available.");
  });

  test("segment route returns 404 'Preroll not available.' when the variant for a recognized codec has not been generated", () => {

    // Identical reasoning to the init.mp4 variant-absence branch. The segment route's variant lookup happens before the filename regex validation.
    const stub = makeExpressStub();

    setupPrerollRoutes(stub.app as Express);

    const segmentRoute = stub.routes.find((r) => (r.path === "/preroll/:codec/:segment"));

    assert.ok(segmentRoute, "segment handler captured");

    const { req, res, send, status } = makeReqRes({ params: { codec: "h264", segment: "segment0.m4s" } });

    void segmentRoute.handler(req, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 404);
    assert.equal(send.mock.calls[0]?.arguments[0], "Preroll not available.");
  });
});

describe("spawnAndCollect", () => {

  test("kills the child at the deadline and rejects with the timeout message", async () => {

    /* A child that writes a little and then never exits is the hung-encoder shape the deadline exists for. The platform kills it when the deadline passes, and
     * the rejection has to name the timeout rather than surfacing the raw abort error, since that message is what the caller's warning line puts in front of the
     * operator. Real time is used deliberately: the deadline is the subject here, and mocking timers would only measure the mock. The deadline runs from the
     * spawn, so a child killed before its script even executes still rejects through the same path - nothing here depends on the child getting anywhere.
     *
     * The child installs a SIGTERM handler that does nothing, which is the wedged encoder the production comment describes: a process that ignores the polite
     * signal. Passing therefore proves the kill carries SIGKILL strength rather than merely that some signal was sent, since SIGKILL is the one a process cannot
     * trap. The handler costs the success path nothing - an untrappable kill lands just as fast.
     */
    await assert.rejects(spawnAndCollect(process.execPath,
      [ "-e", "process.on(\"SIGTERM\", () => {}); process.stdout.write(\"partial\"); setInterval(() => {}, 1000);" ], 50), /timed out/);
  });

  test("collects stdout into a Buffer when the child exits cleanly inside the deadline", async () => {

    // A fast child under a generous deadline collects its stdout exactly as it would with no deadline at all - the success path must be undisturbed.
    const output = await spawnAndCollect(process.execPath, [ "-e", "process.stdout.write(\"ok\");" ], 5000);

    assert.equal(output.toString(), "ok");
  });

  test("rejects with the exit-code message when the child exits nonzero inside the deadline", async () => {

    // A failing child still reports its own exit code. This is what tells a correct implementation apart from one that reports every failure as a timeout.
    await assert.rejects(spawnAndCollect(process.execPath, [ "-e", "process.exit(3)" ], 5000), /exited with code 3/);
  });
});
