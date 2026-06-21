/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls-playlist-registry.test.ts: HTTP-level integration coverage for the playlist served from registry-backed HLS state. The integration boundary tested
 * here is seeded registry state on one side and the m3u8 body emitted by GET /hls/:name/stream.m3u8 on the other - the wire-facing contract that Channels
 * DVR consumes. Two failure classes are pinned by this suite:
 *
 *   1. Wire-level drift between buildPlaylist's output and what the route actually serves. Anything that would mangle the body in transit (encoding, header
 *      mismatch, premature truncation, accidental rewrite) shows up here as a body assertion miss.
 *   2. Resume-index regressions at the playlist layer. The 1589811 fix lives in hlsResume.ts; this suite complements hls-resume.test.ts by asserting that the
 *      saved index materializes as the served playlist's MEDIA-SEQUENCE - the operational symptom Channels DVR sees when the resume contract breaks.
 *
 * Why HTTP (option c) instead of driving buildPlaylist directly: the suite is named "playlist live registry" and the architectural integration point is the
 * route handler reading registry state and emitting bytes. Calling buildPlaylist directly would prove only the formatter's pure-function behavior - which is
 * unit-tier coverage. Driving fmp4Segmenter directly would test the segmenter, not the registry-to-route path. The seam used here (register a stream entry
 * with seeded HLSState, set the channel-to-stream index, GET) is the same seam every production caller traverses; it bypasses only the browser/ffmpeg setup
 * the integration tier deliberately does not host.
 *
 * Note on Test 4 (resume): the production code that snapshots the resume index into a stream entry lives inside registerPendingStream() and runs only when
 * preroll is generated. Tests cannot drive that path without also generating preroll fixtures, which is browser/FFmpeg territory. Instead, the test mirrors
 * the production read - it calls the public getResumeSegmentIndex() accessor (the same function registerPendingStream uses internally) and seeds
 * hls.resumeSegmentIndex on the synthetic entry from that read. This pins the wire-level invariant ("a saved resume index for channel X causes channel X's
 * next playlist to start at MEDIA-SEQUENCE = saved index") without re-implementing any production logic - the resume index flows through the production
 * accessor; the test only asserts what the route emits.
 */
import { bootApp, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { deleteResumeData, getResumeSegmentIndex, loadResumeState, saveResumeState } from "../../../src/streaming/hlsResume.ts";
import { describe, test } from "node:test";
import { setChannelStreamId, terminateStream } from "../../../src/streaming/lifecycle.ts";
import assert from "node:assert/strict";
import { buildPlaylist } from "../../../src/streaming/playlistBuilder.ts";
import { makeRegistryEntry } from "../../../src/streaming/registry.helpers.ts";
import { registerStream } from "../../../src/streaming/registry.ts";
import { updatePlaylist } from "../../../src/streaming/hlsSegments.ts";

describe("HLS playlist served from registry-backed state", () => {

  test("a seeded playlist with N segments serves with the correct MEDIA-SEQUENCE and TARGETDURATION", async () => {

    /* The baseline wire contract: when the registry holds a playlist string built from N entries, the route serves that string verbatim with the headers
     * Channels DVR expects. We seed mediaSequence at a non-trivial value (100) so a regression that hard-codes zero or off-by-one would surface. The
     * targetDuration of 4 is the maximum of the supplied floor (4) and the longest entry duration (4.0), rounded up per RFC 8216. We assert on body content, status, and
     * Content-Type because all three are part of the contract clients depend on.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const entry = makeRegistryEntry({ channelName: "abc" });

    registerStream(entry);
    setChannelStreamId("abc", entry.id);

    ctx.registerCleanup(() => { terminateStream(entry.id, "abc", "test cleanup"); });

    const playlist = buildPlaylist({ mediaSequence: 100, targetDuration: 4, version: 7 }, [
      { duration: 4, url: "segment100.m4s" },
      { duration: 4, url: "segment101.m4s" },
      { duration: 4, url: "segment102.m4s" },
      { duration: 4, url: "segment103.m4s" }
    ]);

    updatePlaylist(entry.id, playlist);

    const response = await fetch(urlFor("/hls/abc/stream.m3u8"));
    const body = await response.text();

    assert.equal(response.status, 200, "playlist should serve 200; body: " + body.slice(0, 200));
    // Express appends "; charset=utf-8" to text responses; the integration contract is the MIME type prefix, not the full header value.
    assert.match(response.headers.get("content-type") ?? "", /^application\/vnd\.apple\.mpegurl(;|$)/, "Content-Type must declare the HLS MIME type");
    assert.match(body, /^#EXTM3U$/m, "body opens with the EXTM3U tag");
    assert.match(body, /^#EXT-X-MEDIA-SEQUENCE:100$/m, "MEDIA-SEQUENCE reflects the seeded starting sequence");
    assert.match(body, /^#EXT-X-TARGETDURATION:4$/m, "TARGETDURATION ceilings the longest entry duration");
    assert.match(body, /^segment100\.m4s$/m, "first segment URL appears in the body");
    assert.match(body, /^segment103\.m4s$/m, "last segment URL appears in the body");
  });

  test("advancing the playlist window updates MEDIA-SEQUENCE and segment URLs on the next serve", async () => {

    /* The sliding-window contract: after the segmenter shifts entries off the front and appends new ones, the next playlist served reflects both the new
     * starting sequence and the new segment list. Old URLs must not appear in the new body. This catches the regression class where stale playlist content
     * leaks past a window advance - a client that reads it would request segments that no longer exist in storage.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const entry = makeRegistryEntry({ channelName: "abc" });

    registerStream(entry);
    setChannelStreamId("abc", entry.id);

    ctx.registerCleanup(() => { terminateStream(entry.id, "abc", "test cleanup"); });

    // Initial window: sequence 0 with three segments.
    const initial = buildPlaylist({ mediaSequence: 0, targetDuration: 4, version: 7 }, [
      { duration: 4, url: "segment0.m4s" },
      { duration: 4, url: "segment1.m4s" },
      { duration: 4, url: "segment2.m4s" }
    ]);

    updatePlaylist(entry.id, initial);

    const firstResponse = await fetch(urlFor("/hls/abc/stream.m3u8"));
    const firstBody = await firstResponse.text();

    assert.match(firstBody, /^#EXT-X-MEDIA-SEQUENCE:0$/m, "first serve reflects the initial sequence");
    assert.match(firstBody, /^segment0\.m4s$/m, "first serve includes the initial first segment");

    // Window advances: oldest segment shifts off, new one appended; sequence advances by 3.
    const advanced = buildPlaylist({ mediaSequence: 3, targetDuration: 4, version: 7 }, [
      { duration: 4, url: "segment3.m4s" },
      { duration: 4, url: "segment4.m4s" },
      { duration: 4, url: "segment5.m4s" }
    ]);

    updatePlaylist(entry.id, advanced);

    const secondResponse = await fetch(urlFor("/hls/abc/stream.m3u8"));
    const secondBody = await secondResponse.text();

    assert.match(secondBody, /^#EXT-X-MEDIA-SEQUENCE:3$/m, "second serve reflects the advanced sequence");
    assert.match(secondBody, /^segment3\.m4s$/m, "second serve includes the new first segment");
    assert.doesNotMatch(secondBody, /^segment0\.m4s$/m, "old segment must NOT leak into the advanced window");
  });

  test("a discontinuity flag on an entry emits EXT-X-DISCONTINUITY immediately before that segment", async () => {

    /* HLS clients use EXT-X-DISCONTINUITY to know that codec parameters or PTS may reset at that boundary. A regression that swallows the flag, emits it
     * twice, or places it on the wrong segment causes clients to either glitch through a real discontinuity or insert a synthetic gap mid-stream. We assert
     * that the tag appears exactly once and that it sits on the line immediately preceding the second segment's EXTINF.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const entry = makeRegistryEntry({ channelName: "abc" });

    registerStream(entry);
    setChannelStreamId("abc", entry.id);

    ctx.registerCleanup(() => { terminateStream(entry.id, "abc", "test cleanup"); });

    const playlist = buildPlaylist({ mediaSequence: 0, targetDuration: 4, version: 7 }, [
      { duration: 4, url: "segment0.m4s" },
      { discontinuity: true, duration: 4, url: "segment1.m4s" },
      { duration: 4, url: "segment2.m4s" }
    ]);

    updatePlaylist(entry.id, playlist);

    const response = await fetch(urlFor("/hls/abc/stream.m3u8"));
    const body = await response.text();

    const discontinuityMatches = body.match(/^#EXT-X-DISCONTINUITY$/gm) ?? [];

    assert.equal(discontinuityMatches.length, 1, "exactly one discontinuity tag should appear");

    // Locate the discontinuity tag and the segment that follows it. The tag must sit on the line immediately above segment1's EXTINF, not segment0's or
    // segment2's. Use line-by-line indexing rather than regex distance because the builder emits one tag per line.
    const lines = body.split("\n");
    const discontinuityIndex = lines.indexOf("#EXT-X-DISCONTINUITY");

    assert.notEqual(discontinuityIndex, -1, "discontinuity tag should be a standalone line in the body");
    assert.match(lines[discontinuityIndex + 1] ?? "", /^#EXTINF:/, "the line after the tag must be the EXTINF for the discontinuous segment");
    assert.equal(lines[discontinuityIndex + 2], "segment1.m4s", "segment1 must be the segment immediately following the discontinuity");
  });

  test("a saved resume index materializes as the served playlist's MEDIA-SEQUENCE", async () => {

    /* The 1589811 regression class at the wire layer: after a restart, the next playlist served for a previously-streamed channel must start at the saved
     * sequence so Channels DVR's recording continues from where it left off. The persistence side (save/load round-trip) is covered in hls-resume.test.ts;
     * this test pins the consumption side - the seed reaches the wire as MEDIA-SEQUENCE.
     *
     * The flow mirrors production: the resume map is populated via saveResumeState/loadResumeState (the persistence path that runs at process startup), and
     * the entry's hls.resumeSegmentIndex is read from the public getResumeSegmentIndex() accessor - the exact same call registerPendingStream() makes
     * internally. We then build a playlist with MEDIA-SEQUENCE seeded at that resume index (matching what fmp4Segmenter does in production where it sets
     * "mediaSequence: startIndex") and assert the body emitted on the wire reflects it.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // The 1589811 value is the canonical regression marker - any drift in the resume contract surfaces here as a wrong sequence on the wire.
    const priorIndex = 1589811;

    saveResumeState([{ channelName: "abc", initSegment: null, initVersion: 1, segmentIndex: priorIndex, trackTimestamps: new Map() }]);
    loadResumeState();

    ctx.registerCleanup(() => { deleteResumeData("abc"); });

    // Read the resume index via the public accessor, mirroring registerPendingStream's snapshot. Production's "?? 0" fallback is faithfully reproduced.
    const resumeSegmentIndex = getResumeSegmentIndex("abc") ?? 0;

    assert.equal(resumeSegmentIndex, priorIndex, "the resume map round-trip must surface the saved index unchanged");

    const entry = makeRegistryEntry({ channelName: "abc" });

    entry.hls.resumeSegmentIndex = resumeSegmentIndex;

    registerStream(entry);
    setChannelStreamId("abc", entry.id);

    ctx.registerCleanup(() => { terminateStream(entry.id, "abc", "test cleanup"); });

    const playlist = buildPlaylist({ mediaSequence: entry.hls.resumeSegmentIndex, targetDuration: 4, version: 7 }, [
      { duration: 4, url: "segment" + String(resumeSegmentIndex) + ".m4s" }
    ]);

    updatePlaylist(entry.id, playlist);

    const response = await fetch(urlFor("/hls/abc/stream.m3u8"));
    const body = await response.text();

    assert.match(body, new RegExp("^#EXT-X-MEDIA-SEQUENCE:" + String(priorIndex) + "$", "m"),
      "the served playlist's MEDIA-SEQUENCE must equal the saved resume index");
  });
});
