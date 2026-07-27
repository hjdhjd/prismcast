/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * playlistBuilder.test.ts: Unit tests for the shared HLS playlist builder. buildPlaylist is the documented SSOT for HLS m3u8 generation across the capture-mode
 * fMP4 segmenter, the native HLS proxy, and the standalone preroll playlist. Because every byte of its output ends up in playlists served to Channels DVR clients,
 * every per-segment tag and the computed TARGETDURATION rounding all earn explicit boundary coverage. The function is pure - no globals, no I/O - so the entire
 * test surface is input/output equality.
 */
import { describe, test } from "node:test";
import type { PlaylistSegmentEntry } from "./playlistBuilder.ts";
import assert from "node:assert/strict";
import { buildPlaylist } from "./playlistBuilder.ts";

describe("buildPlaylist", () => {

  test("emits the basic four-line header with version, target duration, and media sequence", () => {

    // Happy path: a single segment with no optional tags. Locks the canonical four-tag header order.
    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, [{ duration: 2, url: "segment0.m4s" }]);

    assert.equal(result, [
      "#EXTM3U",
      "#EXT-X-VERSION:7",
      "#EXT-X-TARGETDURATION:2",
      "#EXT-X-MEDIA-SEQUENCE:0",
      "#EXTINF:2.000,",
      "segment0.m4s",
      ""
    ].join("\n"));
  });

  test("formats EXTINF durations to exactly three decimal places", () => {

    // Boundary: the toFixed(3) literal shapes every emitted duration. Locks the exact rendering for half-integer and irrational inputs.
    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, [
      { duration: 1.5, url: "a.m4s" },
      { duration: 2, url: "b.m4s" },
      { duration: 1.99999, url: "c.m4s" }
    ]);

    assert.match(result, /#EXTINF:1\.500,/);
    assert.match(result, /#EXTINF:2\.000,/);
    assert.match(result, /#EXTINF:2\.000,/);
  });

  test("computes TARGETDURATION as the ceiling of the maximum entry duration", () => {

    // The spec mandates TARGETDURATION as an integer that meets or exceeds the longest segment. A 2.5s segment must produce TARGETDURATION:3, not 2.
    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, [
      { duration: 2.5, url: "a.m4s" },
      { duration: 1.0, url: "b.m4s" }
    ]);

    assert.match(result, /^#EXT-X-TARGETDURATION:3$/m, "ceil(2.5) -> 3");
  });

  test("uses the targetDuration option as a floor when entries are shorter", () => {

    // Capture path passes CONFIG.hls.segmentDuration as the floor to avoid under-declaring TARGETDURATION when all real segments are short.
    const result = buildPlaylist({ mediaSequence: 0, targetDuration: 4, version: 7 }, [
      { duration: 1, url: "a.m4s" },
      { duration: 1.5, url: "b.m4s" }
    ]);

    assert.match(result, /^#EXT-X-TARGETDURATION:4$/m, "floor (4) wins over max entry duration (1.5)");
  });

  test("targetDuration option does not override a longer entry duration (max-takes-all)", () => {

    // The floor is a minimum, not a cap. When the longest entry exceeds the floor, the entry duration drives TARGETDURATION.
    const result = buildPlaylist({ mediaSequence: 0, targetDuration: 2, version: 7 }, [{ duration: 5.1, url: "a.m4s" }]);

    assert.match(result, /^#EXT-X-TARGETDURATION:6$/m, "ceil(5.1) > floor (2)");
  });

  test("computes TARGETDURATION:0 when no entries are present and no floor is provided", () => {

    // Boundary: empty entries with no targetDuration option. Math.ceil(0) is 0 - locks the empty-playlist shape.
    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, []);

    assert.match(result, /^#EXT-X-TARGETDURATION:0$/m);
    assert.match(result, /^#EXT-X-MEDIA-SEQUENCE:0$/m);
  });

  test("respects the floor when entries are empty", () => {

    // Boundary: with no entries, the floor controls TARGETDURATION outright.
    const result = buildPlaylist({ mediaSequence: 7, targetDuration: 4, version: 7 }, []);

    assert.match(result, /^#EXT-X-TARGETDURATION:4$/m);
    assert.match(result, /^#EXT-X-MEDIA-SEQUENCE:7$/m);
  });

  test("emits EXT-X-DISCONTINUITY-SEQUENCE when the option is provided", () => {

    const result = buildPlaylist({ discontinuitySequence: 3, mediaSequence: 0, version: 7 }, [{ duration: 2, url: "a.m4s" }]);

    assert.match(result, /^#EXT-X-DISCONTINUITY-SEQUENCE:3$/m);
  });

  test("emits EXT-X-DISCONTINUITY-SEQUENCE:0 when explicitly provided as zero", () => {

    // Boundary: the implementation distinguishes undefined (omit tag entirely) from 0 (emit with value 0). Both branches are spec-meaningful.
    const result = buildPlaylist({ discontinuitySequence: 0, mediaSequence: 0, version: 7 }, [{ duration: 2, url: "a.m4s" }]);

    assert.match(result, /^#EXT-X-DISCONTINUITY-SEQUENCE:0$/m);
  });

  test("omits EXT-X-DISCONTINUITY-SEQUENCE entirely when option is undefined", () => {

    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, [{ duration: 2, url: "a.m4s" }]);

    assert.doesNotMatch(result, /DISCONTINUITY-SEQUENCE/, "tag must not appear when option is undefined");
  });

  test("emits the initial EXT-X-MAP with a quoted URI when initialMapUri is provided", () => {

    const result = buildPlaylist({ initialMapUri: "init.mp4?v=2", mediaSequence: 0, version: 7 }, [{ duration: 2, url: "a.m4s" }]);

    assert.match(result, /^#EXT-X-MAP:URI="init\.mp4\?v=2"$/m);
  });

  test("omits EXT-X-MAP entirely for MPEG-TS playlists (no initialMapUri)", () => {

    // MPEG-TS streams carry codec config in every segment - they must NOT reference an init segment.
    const result = buildPlaylist({ mediaSequence: 0, version: 3 }, [{ duration: 2, url: "a.ts" }]);

    assert.doesNotMatch(result, /EXT-X-MAP/, "EXT-X-MAP must not appear for MPEG-TS playlists");
  });

  test("emits per-segment EXT-X-DISCONTINUITY before the segment URL", () => {

    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, [
      { duration: 2, url: "a.m4s" },
      { discontinuity: true, duration: 2, url: "b.m4s" }
    ]);

    // The discontinuity tag must precede its segment's EXTINF, not follow it.
    const lines = result.trim().split("\n");
    const discIdx = lines.indexOf("#EXT-X-DISCONTINUITY");
    const bIdx = lines.indexOf("b.m4s");

    assert.notEqual(discIdx, -1, "discontinuity tag must be present");
    assert.ok(discIdx < bIdx, "#EXT-X-DISCONTINUITY must precede the segment URL it applies to");
  });

  test("emits per-segment EXT-X-MAP at discontinuity boundaries", () => {

    // At a discontinuity, fMP4 streams re-emit the init reference with a versioned URI so the client reinitializes its decoder.
    const result = buildPlaylist({ initialMapUri: "init.mp4?v=1", mediaSequence: 0, version: 7 }, [
      { duration: 2, url: "a.m4s" },
      { discontinuity: true, duration: 2, mapUri: "init.mp4?v=2", url: "b.m4s" }
    ]);

    assert.match(result, /^#EXT-X-MAP:URI="init\.mp4\?v=1"$/m, "initial MAP appears before any segment");
    assert.match(result, /^#EXT-X-MAP:URI="init\.mp4\?v=2"$/m, "per-segment MAP appears at discontinuity");
  });

  test("emits EXT-X-PROGRAM-DATE-TIME for entries that include it", () => {

    const pdt = "2026-01-15T20:00:00.000Z";
    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, [{ duration: 2, programDateTime: pdt, url: "a.m4s" }]);

    assert.match(result, new RegExp("^#EXT-X-PROGRAM-DATE-TIME:" + pdt + "$", "m"));
  });

  test("emits SCTE-35 cue tags in the canonical order before EXTINF", () => {

    // Per-segment tag order: DISCONTINUITY, MAP, PROGRAM-DATE-TIME, CUE-IN, CUE-OUT, CUE-OUT-CONT, EXTINF, URL. Locks every cue tag's emission with its expected
    // output shape.
    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, [{


      cueIn: true,
      cueOut: "30.000",
      cueOutCont: "10.000/30.000",
      duration: 2,
      url: "a.m4s"
    }]);

    const lines = result.trim().split("\n");

    assert.ok(lines.includes("#EXT-X-CUE-IN"), "CUE-IN with no value");
    assert.ok(lines.includes("#EXT-X-CUE-OUT:30.000"), "CUE-OUT with duration");
    assert.ok(lines.includes("#EXT-X-CUE-OUT-CONT:10.000/30.000"), "CUE-OUT-CONT with elapsed/total");

    const cueInIdx = lines.indexOf("#EXT-X-CUE-IN");
    const cueOutIdx = lines.indexOf("#EXT-X-CUE-OUT:30.000");
    const cueContIdx = lines.indexOf("#EXT-X-CUE-OUT-CONT:10.000/30.000");
    const extInfIdx = lines.findIndex((l) => l.startsWith("#EXTINF:"));

    assert.ok(cueInIdx < cueOutIdx, "CUE-IN precedes CUE-OUT");
    assert.ok(cueOutIdx < cueContIdx, "CUE-OUT precedes CUE-OUT-CONT");
    assert.ok(cueContIdx < extInfIdx, "all cue tags precede EXTINF");
  });

  test("emits bare EXT-X-CUE-OUT (no duration) when cueOut is the empty string", () => {

    // Boundary: the implementation branches on cueOut.length > 0. An empty string emits the bare tag without a colon-suffix.
    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, [{ cueOut: "", duration: 2, url: "a.m4s" }]);

    assert.match(result, /^#EXT-X-CUE-OUT$/m, "empty-string cueOut emits bare tag");
    assert.doesNotMatch(result, /CUE-OUT:[^\n]/, "no colon variant should appear for empty cueOut");
  });

  test("does not emit cue tags when their fields are undefined", () => {

    // Negative test: a segment without any cueIn / cueOut / cueOutCont fields must produce zero cue tag lines.
    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, [{ duration: 2, url: "a.m4s" }]);

    assert.doesNotMatch(result, /CUE-IN/, "no CUE-IN");
    assert.doesNotMatch(result, /CUE-OUT/, "no CUE-OUT");
    assert.doesNotMatch(result, /CUE-OUT-CONT/, "no CUE-OUT-CONT");
  });

  test("preserves entry order across the playlist body", () => {

    // The builder must not reorder entries - URL order in the output must equal URL order in the input.
    const entries: PlaylistSegmentEntry[] = [
      { duration: 2, url: "first.m4s" },
      { duration: 2, url: "second.m4s" },
      { duration: 2, url: "third.m4s" }
    ];

    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, entries);
    const lines = result.trim().split("\n");
    const urlLines = lines.filter((l) => l.endsWith(".m4s"));

    assert.deepEqual(urlLines, [ "first.m4s", "second.m4s", "third.m4s" ]);
  });

  test("output ends with a single trailing newline", () => {

    // Every HLS client expects the playlist to terminate with a newline. The trailing-newline contract is locked here.
    const result = buildPlaylist({ mediaSequence: 0, version: 7 }, [{ duration: 2, url: "a.m4s" }]);

    assert.equal(result.endsWith("\n"), true, "must end with a newline");
    assert.equal(result.endsWith("\n\n"), false, "must not double-newline");
  });

  test("supports MPEG-TS playlists with version 3", () => {

    // Version 3 is the legacy MPEG-TS variant - no EXT-X-MAP support. Locks that buildPlaylist accepts version 3 cleanly.
    const result = buildPlaylist({ mediaSequence: 100, version: 3 }, [
      { duration: 2, url: "seg100.ts" },
      { duration: 2, url: "seg101.ts" }
    ]);

    assert.match(result, /^#EXT-X-VERSION:3$/m);
    assert.match(result, /^#EXT-X-MEDIA-SEQUENCE:100$/m);
    assert.match(result, /seg100\.ts/);
    assert.match(result, /seg101\.ts/);
    assert.doesNotMatch(result, /EXT-X-MAP/, "no EXT-X-MAP for version 3");
  });

  test("integration: capture-style fMP4 playlist with init MAP, PDT, and three segments", () => {

    // Realistic shape for the fMP4 segmenter: version 7, init MAP, three sequential segments, and a PDT on each. Acts as a regression lock for the integration of all
    // the per-segment branches in their typical combination.
    const result = buildPlaylist({ initialMapUri: "init.mp4?v=1", mediaSequence: 5, targetDuration: 2, version: 7 }, [
      { duration: 2.000, programDateTime: "2026-01-15T20:00:00.000Z", url: "segment5.m4s" },
      { duration: 2.000, programDateTime: "2026-01-15T20:00:02.000Z", url: "segment6.m4s" },
      { duration: 2.000, programDateTime: "2026-01-15T20:00:04.000Z", url: "segment7.m4s" }
    ]);

    assert.match(result, /^#EXTM3U$/m);
    assert.match(result, /^#EXT-X-VERSION:7$/m);
    assert.match(result, /^#EXT-X-TARGETDURATION:2$/m);
    assert.match(result, /^#EXT-X-MEDIA-SEQUENCE:5$/m);
    assert.match(result, /^#EXT-X-MAP:URI="init\.mp4\?v=1"$/m);
    assert.equal((result.match(/#EXT-X-PROGRAM-DATE-TIME/g) ?? []).length, 3, "three PDT lines");
    assert.equal((result.match(/segment\d+\.m4s/g) ?? []).length, 3, "three segment URLs");
  });
});
