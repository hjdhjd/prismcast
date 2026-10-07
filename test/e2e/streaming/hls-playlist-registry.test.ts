/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls-playlist-registry.test.ts: HTTP-level integration coverage for the playlist served from registry-backed HLS state. The integration boundary tested
 * here is seeded registry state on one side and the m3u8 body emitted by GET /hls/:name/stream.m3u8 on the other - the wire-facing contract that Channels
 * DVR consumes. This suite asserts these failure classes:
 *
 *   1. Wire-level drift between buildPlaylist's output and what the route actually serves. Anything that would mangle the body in transit (encoding, header
 *      mismatch, premature truncation, accidental rewrite) shows up here as a body assertion miss.
 *   2. Resume-position regressions at the playlist layer. The segment-index decrement that keeps the last completed segment in the resumed playlist lives in
 *      src/app.ts's shutdown handler; hlsResume.ts only persists whatever segmentIndex value it is given. This suite complements hls-resume.test.ts by
 *      asserting that the saved position materializes in the preroll playlist the route regenerates for a resumed stream - its MEDIA-SEQUENCE, its
 *      DISCONTINUITY-SEQUENCE and its opening discontinuity - the operational symptom Channels DVR sees when the resume contract breaks.
 *
 * Why HTTP instead of driving buildPlaylist directly: the suite is hls-playlist-registry.test.ts (describe block "HLS playlist served from
 * registry-backed state") and the architectural integration point is the route handler reading registry state and emitting bytes. Calling buildPlaylist
 * directly would prove only the formatter's pure-function behavior - which is unit-tier coverage. Driving fmp4Segmenter directly would test the segmenter,
 * not the registry-to-route path. The registry-to-route entry point used here (register a stream entry with seeded HLSState, set the channel-to-stream
 * index, GET) is the same entry point every production caller traverses; it bypasses only the browser/ffmpeg setup the integration tier deliberately does
 * not host.
 *
 * Note on the resume test: the production code that reads the resume position into a stream entry lives inside registerPendingStream(), and its consumer is
 * the preroll playlist the route regenerates on every poll until real content arrives. Driving registerPendingStream() directly would require a full Express
 * Request and the deferred-timer machinery, which is browser/FFmpeg territory. Instead, the test readies a preroll variant through generatePreroll's encoder
 * port with the synthetic encoder, reads the position through the public getResumePosition() accessor the registration calls, and seeds an entry the way a
 * pending registration leaves it: no real playlist, the preroll fields, and that position. What the route then serves is generatePrerollPlaylist's own
 * output, so the assertions read the production consumer rather than a playlist the test built.
 */
import type { Request, Response } from "express";
import { bootApp, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { cleanupIdleStreams, handleHLSSegment } from "../../../src/streaming/hls.ts";
import { deleteResumeData, getResumePosition, loadResumeState, saveResumeState } from "../../../src/streaming/hlsResume.ts";
import { describe, test } from "node:test";
import { endLoginMode, setLoginDeps, startLoginMode } from "../../../src/browser/login.ts";
import { generatePreroll, isPrerollReady } from "../../../src/streaming/preroll.ts";
import { getBrowserInstance, syncWindowVisibility } from "../../../src/browser/index.ts";
import { getStream, registerStream } from "../../../src/streaming/registry.ts";
import { makeSyntheticFmp4, makeSyntheticPrerollEncoder } from "../../../src/streaming/preroll.helpers.ts";
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

    // Window advances by three: all three earlier segments shift off and three new ones are appended, so the sequence advances to 3.
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

  test("a saved resume position materializes in the preroll playlist's MEDIA-SEQUENCE, DISCONTINUITY-SEQUENCE and opening discontinuity", async () => {

    /* The resume contract at the wire layer: after a restart, the next playlist served for a previously-streamed channel must start at the saved
     * sequence so Channels DVR's recording continues from where it left off, and its discontinuity sequence must continue from the saved count so it never
     * falls below what a client last read. The preroll that plays while the tune starts opens a new timeline, so its first entry carries a discontinuity. The
     * persistence side (save/load round-trip) is covered in hls-resume.test.ts; this test asserts the consumption side.
     *
     * The flow mirrors production: the resume map is populated through saveResumeState and loadResumeState (the save that runs at shutdown and the load that
     * runs at the next startup), the entry's hls.resumePosition is read from the public getResumePosition() accessor - the exact same call
     * registerPendingStream() makes - and the playlist the route serves is the one it regenerates through generatePrerollPlaylist for an entry with no real
     * playlist yet. A position dropped by the save, the load, the peek, the copy or the regeneration reads 0 here.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // A preroll variant readied in this process through the encoder port, so the route has a real variant to regenerate the playlist from.
    await generatePreroll(makeSyntheticPrerollEncoder({ output: makeSyntheticFmp4([ 2, 2, 2, 2, 2, 2 ]) }));

    assert.ok(isPrerollReady("h264"), "precondition: the synthetic encode readied the baseline variant");

    // An arbitrary large, non-zero index, chosen so an off-by-one or a zeroed value surfaces here as a wrong sequence on the wire.
    const priorIndex = 1589811;
    const priorCount = 5;

    saveResumeState([{ channelName: "abc", discontinuityCount: priorCount, initSegment: null, initVersion: 1, segmentIndex: priorIndex, trackTimestamps: new Map() }],
      Date.now());
    loadResumeState(Date.now());

    ctx.registerCleanup(() => { deleteResumeData("abc"); });

    // Read the resume position via the public accessor, the read registerPendingStream makes.
    const resumePosition = getResumePosition("abc", Date.now());

    assert.deepEqual(resumePosition, { discontinuityCount: priorCount, segmentIndex: priorIndex }, "the resume map round-trip must surface the saved position");

    // The entry as a pending registration leaves it once the deferred preroll timer has fired: the preroll fields and the timer's seeded playlist are set, and no
    // real playlist has arrived, so the route regenerates the preroll playlist on the poll. The seeded text stands in for the timer's and is never served.
    const baseUrl = "http://preroll.test:5589";
    const entry = makeRegistryEntry({ channelName: "abc" });

    entry.hls.playlist = "#EXTM3U\n";
    entry.hls.prerollBaseUrl = baseUrl;
    entry.hls.prerollCodec = "h264";
    entry.hls.prerollStartTime = Date.now();
    entry.hls.resumePosition = resumePosition;

    registerStream(entry);
    setChannelStreamId("abc", entry.id);

    ctx.registerCleanup(() => { terminateStream(entry.id, "abc", "test cleanup"); });

    const response = await fetch(urlFor("/hls/abc/stream.m3u8"));
    const body = await response.text();
    const lines = body.split("\n");
    const firstEntryIndex = lines.findIndex((line) => line.startsWith("#EXTINF:"));

    assert.equal(response.status, 200, "the preroll playlist serves 200; body: " + body.slice(0, 200));
    assert.match(body, new RegExp("^#EXT-X-MEDIA-SEQUENCE:" + String(priorIndex) + "$", "m"),
      "the served playlist's MEDIA-SEQUENCE must equal the saved resume index");
    assert.match(body, new RegExp("^#EXT-X-DISCONTINUITY-SEQUENCE:" + String(priorCount) + "$", "m"),
      "the served playlist's DISCONTINUITY-SEQUENCE must equal the saved discontinuity count");
    assert.ok(firstEntryIndex > 0, "the served playlist lists preroll entries");
    assert.equal(lines[firstEntryIndex - 1], "#EXT-X-DISCONTINUITY", "the first preroll entry opens the resumed stream's new timeline with a discontinuity");
    assert.equal(lines[firstEntryIndex + 1], baseUrl + "/preroll/h264/segment0.m4s", "and it is the first preroll segment");
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

    /* The 404 boundary: each not-found condition must answer 404. A known stream missing the requested media segment and a known stream that has no init
     * segment stored yet pass the channel-to-stream lookup and fail the segment-store read; a request for a channel with no registered stream fails the
     * channel-to-stream lookup itself. This asserts that each answers 404 and none of them leaks a 200 with an empty or stale body.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const entry = makeRegistryEntry({ channelName: "seg404" });

    registerStream(entry);
    setChannelStreamId("seg404", entry.id);

    ctx.registerCleanup(() => { terminateStream(entry.id, "seg404", "test cleanup"); });

    // A mapped stream that never stored the requested media segment. The body tells the not-found branches apart where the shared status cannot.
    const unknownSegment = await fetch(urlFor("/hls/seg404/segment9.m4s"));

    assert.equal(unknownSegment.status, 404, "an unknown segment on a known stream must yield 404");
    assert.equal(await unknownSegment.text(), "Segment not found.", "and its body names the missing segment");

    // A mapped stream with no init segment stored.
    const missingInit = await fetch(urlFor("/hls/seg404/init.mp4"));

    assert.equal(missingInit.status, 404, "a known stream with no stored init segment must yield 404");
    assert.equal(await missingInit.text(), "Init segment not found.", "and its body names the missing init segment");

    // A channel with no registered stream at all.
    const unknownStream = await fetch(urlFor("/hls/no-such-channel/segment0.m4s"));

    assert.equal(unknownStream.status, 404, "a request for an unmapped channel must yield 404");
    assert.equal(await unknownStream.text(), "Stream not found.", "and its body names the missing stream");
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
     * :playlist route parameter). We store distinct video and audio variant bodies and assert each URL returns its own body with the HLS MIME type, which asserts both
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
     * applies when the channel has no registered stream at all. Both branches are asserted so a regression that returned 200 with an empty playlist (which a client
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
    assert.equal(await noVideo.text(), "Playlist not found.", "and its body names the missing playlist");

    const noAudio = await fetch(urlFor("/hls/plainvar/audio.m3u8"));

    assert.equal(noAudio.status, 404, "a stream without a stored audio variant must yield 404");
    assert.equal(await noAudio.text(), "Playlist not found.", "and its body names the missing playlist");

    const unknownStream = await fetch(urlFor("/hls/no-such-channel/video.m3u8"));

    assert.equal(unknownStream.status, 404, "a request for an unmapped channel must yield 404");
    assert.equal(await unknownStream.text(), "Stream not found.", "and its body names the missing stream");
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
     * neither of which is exported; cleanupIdleStreams terminates every idle stream regardless of order, so the ordering rule is not independently assertable
     * through the exported surface. This test asserts the selection and exclusion branches, which are.
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

      // terminateStream is safe to call more than once, so cleaning up the already-terminated idle stream here is a safe no-op.
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
     * trims to empty) must both be rejected with 400 before any stream setup is attempted. This asserts the entry guard so a regression that proceeded to hash an empty
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
     * accessor: startLoginMode requires a connected browser and drives the window through the injected sync, neither of which the integration tier hosts, so we
     * inject a minimal browser double through the same setLoginDeps port browser/index.ts wires at startup. Cleanup calls endLoginMode (clearing the
     * 15-minute safety timer and resetting the module singleton) and restores the real accessors so no later test observes the double.
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

    setLoginDeps({ getBrowserInstance: (): Browser => fakeBrowser, syncWindowVisibility: async (): Promise<void> => { /* No window to drive in tests. */ } });

    ctx.registerCleanup(async () => {

      await endLoginMode();
      setLoginDeps({ getBrowserInstance, syncWindowVisibility });
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
