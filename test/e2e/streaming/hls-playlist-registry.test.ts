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
import type { Request, Response } from "express";
import { bootApp, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { cleanupIdleStreams, handleHLSSegment } from "../../../src/streaming/hls.ts";
import { deleteResumeData, getResumeSegmentIndex, loadResumeState, saveResumeState } from "../../../src/streaming/hlsResume.ts";
import { describe, test } from "node:test";
import { endLoginMode, setBrowserAccessors, startLoginMode } from "../../../src/browser/login.ts";
import { getBrowserInstance, minimizeBrowserWindow } from "../../../src/browser/index.ts";
import { getStream, registerStream } from "../../../src/streaming/registry.ts";
import { setChannelStreamId, terminateStream } from "../../../src/streaming/lifecycle.ts";
import { storeInitSegment, storeSegment, updateAudioPlaylist, updatePlaylist, updateVideoPlaylist } from "../../../src/streaming/hlsSegments.ts";
import type { Browser } from "puppeteer-core";
import { CONFIG } from "../../../src/config/index.ts";
import assert from "node:assert/strict";
import { buildPlaylist } from "../../../src/streaming/playlistBuilder.ts";
import { makeRegistryEntry } from "../../../src/streaming/registry.helpers.ts";

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

describe("HLS segment serving from registry-backed state", () => {

  test("stored init, .m4s, and .ts segments serve with the correct status, Content-Type, and bytes", async () => {

    /* The segment wire contract: handleHLSSegment resolves the channel-to-stream mapping, reads the requested segment from the registry-backed HLSState, and emits
     * it with a codec-appropriate Content-Type. The fMP4 init segment and capture-mode .m4s segments carry video/mp4; native-mode .ts segments carry video/MP2T
     * (an HLS client parses the container from that header). We seed one of each through the same storeInitSegment/storeSegment path the production pipeline uses and
     * assert status, Content-Type prefix, and byte-for-byte body identity - a regression that mangles the buffer or mislabels the container surfaces on all three.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const entry = makeRegistryEntry({ channelName: "seg" });

    registerStream(entry);
    setChannelStreamId("seg", entry.id);

    ctx.registerCleanup(() => { terminateStream(entry.id, "seg", "test cleanup"); });

    // Distinct payloads per segment so a routing regression that returned the wrong buffer would fail the byte-identity assertions rather than passing by coincidence.
    const initData = Buffer.from([ 0x00, 0x00, 0x00, 0x18, 0x66, 0x74, 0x79, 0x70 ]);
    const mediaData = Buffer.from("fmp4-media-segment-bytes");
    const nativeData = Buffer.from("mpegts-native-segment-bytes");

    storeInitSegment(entry.id, initData);
    storeSegment(entry.id, "segment0.m4s", mediaData);
    storeSegment(entry.id, "segment1.ts", nativeData);

    const initResponse = await fetch(urlFor("/hls/seg/init.mp4"));

    assert.equal(initResponse.status, 200, "the stored init segment must serve 200");
    assert.match(initResponse.headers.get("content-type") ?? "", /^video\/mp4/, "the init segment must declare the fMP4 MIME type");
    assert.ok(Buffer.from(await initResponse.arrayBuffer()).equals(initData), "the init segment body must equal the stored bytes");

    const mediaResponse = await fetch(urlFor("/hls/seg/segment0.m4s"));

    assert.equal(mediaResponse.status, 200, "a stored .m4s media segment must serve 200");
    assert.match(mediaResponse.headers.get("content-type") ?? "", /^video\/mp4/, "an fMP4 media segment must declare video/mp4");
    assert.ok(Buffer.from(await mediaResponse.arrayBuffer()).equals(mediaData), "the .m4s segment body must equal the stored bytes");

    const nativeResponse = await fetch(urlFor("/hls/seg/segment1.ts"));

    assert.equal(nativeResponse.status, 200, "a stored .ts native segment must serve 200");
    assert.match(nativeResponse.headers.get("content-type") ?? "", /^video\/MP2T/, "an MPEG-TS segment must declare video/MP2T");
    assert.ok(Buffer.from(await nativeResponse.arrayBuffer()).equals(nativeData), "the .ts segment body must equal the stored bytes");
  });

  test("an unknown segment, missing init, or unknown stream each yield 404", async () => {

    /* The 404 boundary: three distinct not-found conditions must each answer 404. A known stream missing the requested media segment, a known stream that has no
     * init segment stored yet, and a request for a channel with no registered stream all fail the registry lookup. This pins that none of them leak a 200 with an
     * empty or stale body, and that the unknown-stream path (no channel-to-stream mapping) is distinguished from a mapped stream missing the segment.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const entry = makeRegistryEntry({ channelName: "seg404" });

    registerStream(entry);
    setChannelStreamId("seg404", entry.id);

    ctx.registerCleanup(() => { terminateStream(entry.id, "seg404", "test cleanup"); });

    // A mapped stream that never stored the requested media segment.
    const unknownSegment = await fetch(urlFor("/hls/seg404/segment9.m4s"));

    assert.equal(unknownSegment.status, 404, "an unknown segment on a known stream must yield 404");

    // A mapped stream with no init segment stored.
    const missingInit = await fetch(urlFor("/hls/seg404/init.mp4"));

    assert.equal(missingInit.status, 404, "a known stream with no stored init segment must yield 404");

    // A channel with no registered stream at all.
    const unknownStream = await fetch(urlFor("/hls/no-such-channel/segment0.m4s"));

    assert.equal(unknownStream.status, 404, "a request for an unmapped channel must yield 404");
  });

  test("handleHLSSegment answers 400 when a route parameter is empty", () => {

    /* The empty-parameter guard fires before any registry lookup. Express's path-to-regexp never produces an empty :name or :segment over the wire - a URL with an
     * empty path segment simply fails to match the route - so this branch is only reachable by invoking the exported handler directly. We drive it with minimal
     * Request/Response doubles that capture the status and body the guard writes; the cast is confined to the two members the guard touches.
     */
    const captured: { body: unknown; status: number } = { body: undefined, status: 0 };
    const res = {

      send(payload: unknown): void { captured.body = payload; },
      status(code: number): Response {

        captured.status = code;

        return res;
      }
    } as unknown as Response;

    const req = { params: { name: "", segment: "init.mp4" } } as unknown as Request;

    handleHLSSegment(req, res);

    assert.equal(captured.status, 400, "an empty channel name must yield a 400");
    assert.equal(captured.body, "Channel name and segment name are required.", "the 400 body must state the missing-parameter reason");
  });
});

describe("HLS variant playlist serving for separate-audio streams", () => {

  test("video.m3u8 and audio.m3u8 serve their respective stored variant playlists", async () => {

    /* Streams with separate audio renditions store two variant playlists; the route resolves which one to serve by parsing the filename from req.path (there is no
     * :playlist route parameter). We store distinct video and audio variant bodies and assert each URL returns its own body with the HLS MIME type, which pins both
     * that the correct playlist is selected and that the filename-from-path parse routes video.m3u8 and audio.m3u8 independently.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const entry = makeRegistryEntry({ channelName: "var" });

    registerStream(entry);
    setChannelStreamId("var", entry.id);

    ctx.registerCleanup(() => { terminateStream(entry.id, "var", "test cleanup"); });

    updateVideoPlaylist(entry.id, "#EXTM3U\n#EXT-X-VERSION:7\n#VARIANT-VIDEO\n");
    updateAudioPlaylist(entry.id, "#EXTM3U\n#EXT-X-VERSION:7\n#VARIANT-AUDIO\n");

    const videoResponse = await fetch(urlFor("/hls/var/video.m3u8"));
    const videoBody = await videoResponse.text();

    assert.equal(videoResponse.status, 200, "the video variant playlist must serve 200");
    assert.match(videoResponse.headers.get("content-type") ?? "", /^application\/vnd\.apple\.mpegurl(;|$)/, "the video variant must declare the HLS MIME type");
    assert.match(videoBody, /^#VARIANT-VIDEO$/m, "video.m3u8 must serve the stored video variant body");

    const audioResponse = await fetch(urlFor("/hls/var/audio.m3u8"));
    const audioBody = await audioResponse.text();

    assert.equal(audioResponse.status, 200, "the audio variant playlist must serve 200");
    assert.match(audioBody, /^#VARIANT-AUDIO$/m, "audio.m3u8 must serve the stored audio variant body");
  });

  test("a stream without a stored variant playlist, or an unmapped channel, yields 404", async () => {

    /* When a stream has no separate-audio renditions, no variant playlist is stored and the route must answer 404 rather than serving an empty body. The same 404
     * applies when the channel has no registered stream at all. Both branches are pinned so a regression that returned 200 with an empty playlist (which a client
     * would treat as an ended stream) surfaces here.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const entry = makeRegistryEntry({ channelName: "plainvar" });

    registerStream(entry);
    setChannelStreamId("plainvar", entry.id);

    ctx.registerCleanup(() => { terminateStream(entry.id, "plainvar", "test cleanup"); });

    const noVideo = await fetch(urlFor("/hls/plainvar/video.m3u8"));

    assert.equal(noVideo.status, 404, "a stream without a stored video variant must yield 404");

    const noAudio = await fetch(urlFor("/hls/plainvar/audio.m3u8"));

    assert.equal(noAudio.status, 404, "a stream without a stored audio variant must yield 404");

    const unknownStream = await fetch(urlFor("/hls/no-such-channel/video.m3u8"));

    assert.equal(unknownStream.status, 404, "a request for an unmapped channel must yield 404");
  });
});

describe("cleanupIdleStreams idle reclamation", () => {

  test("terminates only idle capture streams, excluding fresh, preTuned, and MPEG-TS-attached streams", async () => {

    /* The idle-reclamation contract, driven directly through the exported cleanupIdleStreams without a browser. A stream is idle when its last playlist request is
     * older than CONFIG.hls.idleTimeout AND it has zero MPEG-TS clients AND it is not preTuned. We seed one stream in each of four states - stale-and-plain,
     * freshly-requested, stale-but-preTuned, and stale-but-MPEG-TS-attached - and assert exactly the stale-and-plain stream is terminated while the other three
     * survive. preTuned streams have no clients by design and the pretune module owns their lifecycle; MPEG-TS clients are connection-tracked, not TTL-tracked, so a
     * positive count keeps the stream alive.
     *
     * Note on ordering: getIdleStreams sorts oldest-lastPlaylistRequest first, but that ordering is only observable through reclaimIdleStream (which picks idle[0]),
     * neither of which is exported; cleanupIdleStreams terminates every idle stream regardless of order, so the ordering invariant is not independently assertable
     * through the exported surface. This test pins the selection and exclusion branches, which are.
     */
    await using ctx = await createIntegrationContext();

    const now = Date.now();

    // Comfortably past the idle threshold so a small clock drift between seeding and the cleanup call cannot flip the branch.
    const staleTs = now - (CONFIG.hls.idleTimeout + 60000);

    const idleEntry = makeRegistryEntry({ channelName: "idle-plain", info: { lastPlaylistRequest: staleTs, storeKey: "idle-plain" } });
    const freshEntry = makeRegistryEntry({ channelName: "idle-fresh", info: { lastPlaylistRequest: now, storeKey: "idle-fresh" } });
    const preTunedEntry = makeRegistryEntry({ channelName: "idle-pretuned", info: { lastPlaylistRequest: staleTs, storeKey: "idle-pretuned" }, preTuned: true });
    const mpegTsEntry = makeRegistryEntry({ channelName: "idle-mpegts", info: { lastPlaylistRequest: staleTs, storeKey: "idle-mpegts" }, mpegTsClientCount: 1 });

    for(const entry of [ idleEntry, freshEntry, preTunedEntry, mpegTsEntry ]) {

      registerStream(entry);
      setChannelStreamId(entry.info.storeKey, entry.id);

      // terminateStream is idempotent, so cleaning up the already-terminated idle stream here is a safe no-op.
      ctx.registerCleanup(() => { terminateStream(entry.id, entry.info.storeKey, "test cleanup"); });
    }

    cleanupIdleStreams();

    assert.equal(getStream(idleEntry.id), undefined, "a stale capture stream with no clients must be terminated");
    assert.notEqual(getStream(freshEntry.id), undefined, "a stream requested within the idle window must survive");
    assert.notEqual(getStream(preTunedEntry.id), undefined, "a preTuned stream is exempt from idle cleanup");
    assert.notEqual(getStream(mpegTsEntry.id), undefined, "a stream with an active MPEG-TS client is exempt from idle cleanup");
  });
});

describe("handlePlayStream request guards", () => {

  test("answers 400 when the url query parameter is missing or blank", async () => {

    /* The ad-hoc /play endpoint requires a non-blank url before it derives the synthetic stream key. A missing parameter and a whitespace-only parameter (which
     * trims to empty) must both be rejected with 400 before any stream setup is attempted. This pins the entry guard so a regression that proceeded to hash an empty
     * URL - producing a shared synthetic key across every blank request - surfaces here.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const missing = await fetch(urlFor("/play"));

    assert.equal(missing.status, 400, "a missing url parameter must yield 400");
    assert.match(await missing.text(), /url query parameter is required/, "the 400 body must name the required parameter");

    const blank = await fetch(urlFor("/play?url=%20%20%20"));

    assert.equal(blank.status, 400, "a whitespace-only url trims to empty and must yield 400");
  });

  test("answers 503 with the login-mode body while login mode is active", async () => {

    /* When login mode is active, new ad-hoc streams must be blocked so the authentication tab is not disrupted. We drive login mode active through the production
     * seam: startLoginMode requires a connected browser and un-minimizes a real tab via CDP, neither of which the integration tier hosts, so we inject a minimal
     * browser double through the same setBrowserAccessors port browser/index.ts wires at startup. The fake page reports itself already closed so unminimizeWindow
     * short-circuits before touching CDP. Cleanup calls endLoginMode (clearing the 15-minute safety timer and resetting the module singleton) and restores the real
     * accessors so no later test observes the double.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const fakePage = {

      goto: async (): Promise<void> => { /* The login page load is irrelevant to the 503 guard under test. */ },
      isClosed: (): boolean => true,
      on: (): void => { /* The close-detection handler is never exercised by this test. */ }
    };

    const fakeBrowser = { connected: true, newPage: async (): Promise<unknown> => fakePage } as unknown as Browser;

    setBrowserAccessors({ getBrowserInstance: (): Browser => fakeBrowser, minimizeBrowserWindow: async (): Promise<void> => { /* No window to minimize in tests. */ } });

    ctx.registerCleanup(async () => {

      await endLoginMode();
      setBrowserAccessors({ getBrowserInstance, minimizeBrowserWindow });
    });

    const started = await startLoginMode("https://example.test/login");

    assert.equal(started.success, true, "login mode must start against the injected browser double");

    const response = await fetch(urlFor("/play?url=" + encodeURIComponent("https://example.test/video")));

    assert.equal(response.status, 503, "an active login session must block new ad-hoc streams");

    const body = await response.json() as { error?: string; message?: string };

    assert.equal(body.error, "Login in progress", "the 503 body must carry the login-mode error label");
    assert.equal(body.message, "Please complete authentication before starting new streams.", "the 503 body must carry the login-mode guidance message");
  });
});
