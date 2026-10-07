/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsResume.test.ts: Unit tests for HLS sequence resume across PrismCast restarts. hlsResume.ts persists final media-sequence numbers and per-track timestamps to
 * disk during shutdown, then loads them at the next startup so HLS playlists continue advancing forward instead of resetting to 0. The TTL guard discards entries
 * older than 90 seconds so stale resume state does not poison a fresh recording. The tests exercise the file round-trip, TTL discard, peek/delete consume contract,
 * the merge-with-active-streams path used by saveResumeState, and the resume line's figure, measured from persisted counters over a persisted init segment.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { deleteResumeData, getResumeSegmentIndex, loadResumeState, logStreamResume, peekResumeData, saveResumeState } from "./hlsResume.ts";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { readFileSync, writeFileSync } from "node:fs";
import { LOG } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import type { ResumeData } from "./hlsResume.ts";
import type { TestContext } from "node:test";
import assert from "node:assert/strict";
import { format } from "node:util";
import { initializeDataDir } from "../config/paths.ts";
import os from "node:os";
import path from "node:path";

// The reference instant every row in this file counts from, so a row's expected timestamps read as offsets rather than absolute epochs.
const BASE_TIME_MS = 1700000000000;

/* makeBox builds a minimal MP4 box: 4-byte size + 4-byte type + payload. The size includes the 8-byte header. A file-local copy of the same minimal builder
 * used by mp4Parser.moov.test.ts, per the convention that each mp4Parser.*.test.ts (and this file) defines its own copy rather than sharing one.
 */
function makeBox(type: string, payload: Buffer = Buffer.alloc(0)): Buffer {

  const size = 8 + payload.length;
  const buf = Buffer.alloc(size);

  buf.writeUInt32BE(size, 0);
  buf.write(type, 4, 4, "ascii");
  payload.copy(buf, 8);

  return buf;
}

/* makeTrak builds one complete version-0 track: a tkhd carrying the track_ID at byte 20, and an mdia holding an mdhd with the timescale at byte 20 and an hdlr
 * with the handler type at byte 16 - the offsets parseMoovTrackInfo reads.
 */
function makeTrak(options: { handlerType: string; timescale: number; trackId: number }): Buffer {

  const tkhd = Buffer.alloc(16);
  const mdhd = Buffer.alloc(16);
  const hdlr = Buffer.alloc(12);

  tkhd.writeUInt32BE(options.trackId, 12);
  mdhd.writeUInt32BE(options.timescale, 12);
  hdlr.write(options.handlerType, 8, 4, "ascii");

  return makeBox("trak", Buffer.concat([ makeBox("tkhd", tkhd), makeBox("mdia", Buffer.concat([ makeBox("mdhd", mdhd), makeBox("hdlr", hdlr) ])) ]));
}

// The tracks every figure row declares: a video track at 90000 and an audio track at 48000, the timescales a capture's moov carries.
const VIDEO_TRAK = makeTrak({ handlerType: "vide", timescale: 90000, trackId: 1 });
const AUDIO_TRAK = makeTrak({ handlerType: "soun", timescale: 48000, trackId: 2 });

/* An init segment, the ftyp followed by the moov the given tracks make up. The counters the figure rows pair with it put the video track at 120.5 seconds and
 * the audio track at 126.5, deliberately fractional: their mean, 123.5, rounds to 2m 4s, and a conversion that truncates a track's seconds or floors the mean
 * reads 2m 3s, while the largest track, the smallest, the largest raw counter, one timescale for every track, the sum, and any track alone each read another
 * figure.
 */
function makeInitSegment(...traks: Buffer[]): Buffer {

  return Buffer.concat([ makeBox("ftyp", Buffer.from("isom")), makeBox("moov", Buffer.concat(traks)) ]);
}

/**
 * Builds the resume data a figure row hands logStreamResume.
 * @param initSegment - The persisted init segment, or null.
 * @param trackTimestamps - The persisted counters, in the order the row inserts them.
 * @returns The resume data.
 */
function makeResumeData(initSegment: Nullable<Buffer>, trackTimestamps: [ number, bigint ][]): ResumeData {

  return { initSegment, initVersion: 1, segmentIndex: 10, trackTimestamps: new Map(trackTimestamps) };
}

/**
 * Logs a resume for the stream named Channel and returns every info line it produced, formatted as the logger renders it.
 * @param t - The test context the info logger is mocked through.
 * @param resumeData - The resume data to log.
 * @returns The rendered info lines.
 */
function captureResumeLines(t: TestContext, resumeData: ResumeData): string[] {

  const lines: string[] = [];

  t.mock.method(LOG, "info", (message: string, ...args: unknown[]): void => {

    lines.push(format(message, ...args));
  });

  logStreamResume({ displayName: "Channel", resumeData });

  return lines;
}

/**
 * Shape of a single channel's persisted resume entry. Mirrors ResumeEntryJSON in the production module; we keep a local copy so tests do not import a private
 * type.
 */
interface SerializedResumeEntry {

  initVersion: number;
  segmentIndex: number;
  timestamp: number;
  trackTimestamps: Record<string, string>;
}

/* makeResumeFile writes a synthetic resume JSON file at the path getResumeFilePath() resolves to. The shape mirrors ResumeEntryJSON exactly so loadResumeState
 * deserializes it without needing the test to reach into private types.
 */
async function makeResumeFile(dir: string, channels: Record<string, SerializedResumeEntry>): Promise<void> {

  const filePath = path.join(dir, "hls-resume.json");
  const content: Record<string, unknown> = {};

  for(const [ name, entry ] of Object.entries(channels)) {

    content[name] = {


      initSegment: null,
      ...entry
    };
  }

  await writeFile(filePath, JSON.stringify(content), "utf-8");
}

describe("loadResumeState", () => {

  let tempDir: string;

  beforeEach(async () => {

    tempDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-resume-test-"));
    initializeDataDir(tempDir);
  });

  afterEach(async () => {

    await rm(tempDir, { force: true, recursive: true });
  });

  test("populates the resume map from a valid file and deletes the file afterward", async () => {

    // The implementation reads then unlinks the file synchronously. Locks the read-then-delete contract that prevents stale resume data on the next start.
    const filePath = path.join(tempDir, "hls-resume.json");

    await makeResumeFile(tempDir, {


      cnn: { initVersion: 5, segmentIndex: 1234, timestamp: BASE_TIME_MS, trackTimestamps: { 1: "9000000" } }
    });

    loadResumeState(BASE_TIME_MS);

    let postLoadFileExists = true;

    try {

      readFileSync(filePath, "utf-8");
    } catch {

      postLoadFileExists = false;
    }

    assertEqual(postLoadFileExists, false, "file deleted after read");
    assertEqual(getResumeSegmentIndex("cnn", BASE_TIME_MS), 1234, "entry available in memory after load");
  });

  test("discards an entry past the TTL at load time and keeps the fresh one", async (t: TestContext) => {

    /* The load-time discard is otherwise invisible: every read re-checks the TTL, so an implementation that kept expired entries at load would still answer null
     * to every read. The count the load reports is the one observation that separates the two, so the row reads it off the info line.
     */
    const infos: string[] = [];

    await makeResumeFile(tempDir, {


      expired: { initVersion: 0, segmentIndex: 7, timestamp: BASE_TIME_MS - 91000, trackTimestamps: {} },
      fresh: { initVersion: 0, segmentIndex: 11, timestamp: BASE_TIME_MS, trackTimestamps: {} }
    });

    t.mock.method(LOG, "info", (message: string, ...args: unknown[]): void => {

      infos.push(message + "|" + args.map((arg) => String(arg)).join(","));
    });

    loadResumeState(BASE_TIME_MS);

    const loadLine = infos.find((entry) => entry.startsWith("Loaded HLS resume state"));

    assert(loadLine, "the load reported a count");
    assertEqual(loadLine.split("|")[1], "1,", "one channel loaded - the expired entry was discarded at load, not deferred to the read-time check");
    assertEqual(getResumeSegmentIndex("fresh", BASE_TIME_MS), 11, "the fresh entry reads back");
  });

  test("loads a recent entry and exposes it via getResumeSegmentIndex", async () => {

    await makeResumeFile(tempDir, {


      espn: { initVersion: 0, segmentIndex: 42, timestamp: BASE_TIME_MS, trackTimestamps: {} }
    });

    loadResumeState(BASE_TIME_MS);

    assert(typeof getResumeSegmentIndex !== "undefined");
    assertEqual(getResumeSegmentIndex("espn", BASE_TIME_MS), 42, "loaded segment index for espn");
  });

  test("discards entries older than the 90-second TTL", async () => {

    // Boundary: an entry with timestamp older than (now - 90000) is dropped at load time.
    const expiredTs = BASE_TIME_MS - 91000;

    await makeResumeFile(tempDir, {


      old: { initVersion: 0, segmentIndex: 999, timestamp: expiredTs, trackTimestamps: {} }
    });

    loadResumeState(BASE_TIME_MS);

    assertEqual(getResumeSegmentIndex("old", BASE_TIME_MS), null, "expired entry not loaded");
  });

  test("loads entries inside the TTL window even at the boundary", async () => {

    // The TTL check uses '> RESUME_TTL', so an entry exactly 90000ms old is still inside the window.
    const boundaryTs = BASE_TIME_MS - 90000;

    await makeResumeFile(tempDir, {


      boundary: { initVersion: 0, segmentIndex: 7, timestamp: boundaryTs, trackTimestamps: {} }
    });

    loadResumeState(BASE_TIME_MS);

    assertEqual(getResumeSegmentIndex("boundary", BASE_TIME_MS), 7, "TTL boundary inclusive");
  });

  test("is a no-op when the resume file does not exist (clean start)", () => {

    // Negative test: missing file is the normal first-startup case. loadResumeState must not throw and must leave the map empty.
    let threw = false;

    try {

      loadResumeState(BASE_TIME_MS);
    } catch {

      threw = true;
    }

    assertEqual(threw, false, "missing file did not throw");
    assertEqual(getResumeSegmentIndex("anything", BASE_TIME_MS), null, "empty map after no-file load");
  });

  test("discards corrupt JSON and continues with an empty map", async () => {

    // Negative test: a malformed file is silently discarded with a warning. The map stays empty.
    const filePath = path.join(tempDir, "hls-resume.json");

    await writeFile(filePath, "{ not valid json", "utf-8");

    let threw = false;

    try {

      loadResumeState(BASE_TIME_MS);
    } catch {

      threw = true;
    }

    assertEqual(threw, false, "corrupt JSON did not throw");
    assertEqual(getResumeSegmentIndex("anything", BASE_TIME_MS), null, "empty map after corrupt-file load");
  });
});

describe("peekResumeData", () => {

  let tempDir: string;

  beforeEach(async () => {

    tempDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-resume-test-"));
    initializeDataDir(tempDir);
  });

  afterEach(async () => {

    await rm(tempDir, { force: true, recursive: true });
  });

  test("returns the resume data with initVersion incremented and the same segment index", async () => {

    // The peek path increments initVersion by 1 - the new segmenter must produce a different init URI than the prior session so HLS clients re-fetch.
    await makeResumeFile(tempDir, {


      foo: { initVersion: 3, segmentIndex: 100, timestamp: BASE_TIME_MS, trackTimestamps: { 1: "1000" } }
    });

    loadResumeState(BASE_TIME_MS);

    const data = peekResumeData("foo", BASE_TIME_MS);

    assert(data, "peek returned data");
    assertEqual(data.segmentIndex, 100, "segment index preserved");
    assertEqual(data.initVersion, 4, "init version incremented");
    assertEqual(data.trackTimestamps.get(1), 1000n, "track timestamps deserialized to bigint");
  });

  test("returns the data of an entry loaded inside the TTL and logs no Resuming line, the announcement being the caller's", async (t: TestContext) => {

    const infos: string[] = [];

    await makeResumeFile(tempDir, {

      quiet: { initVersion: 2, segmentIndex: 30, timestamp: BASE_TIME_MS, trackTimestamps: { 1: "10845000" } }
    });

    loadResumeState(BASE_TIME_MS);

    t.mock.method(LOG, "info", (message: string): void => {

      infos.push(message);
    });

    const data = peekResumeData("quiet", BASE_TIME_MS);

    assert(data, "precondition: the peek returned the entry, so a line it logged would have fired");
    assertEqual(data.segmentIndex, 30);
    assert.deepEqual(infos.filter((message) => message.startsWith("Resuming")), [], "the peek logs no Resuming line");
  });

  test("returns null for unknown channels", () => {

    assertEqual(peekResumeData("unknown-channel", BASE_TIME_MS), null);
  });

  test("returns null and removes the entry when its TTL has expired", async () => {

    // The peek path includes a defensive TTL recheck. Stale entries are evicted lazily.
    await makeResumeFile(tempDir, {


      stale: { initVersion: 0, segmentIndex: 1, timestamp: BASE_TIME_MS, trackTimestamps: {} }
    });

    loadResumeState(BASE_TIME_MS);

    // Read past the TTL by supplying an instant beyond the window.
    assertEqual(peekResumeData("stale", BASE_TIME_MS + 91000), null, "expired entry returns null");
    // After eviction, the segment index lookup must also fail.
    assertEqual(getResumeSegmentIndex("stale", BASE_TIME_MS + 91000), null);
  });

  test("does NOT consume the entry on read - same data returned on a second peek", async () => {

    // The two-step pattern: peek then deleteResumeData. Locks the contract that peek alone preserves the entry.
    await makeResumeFile(tempDir, {


      bar: { initVersion: 1, segmentIndex: 50, timestamp: BASE_TIME_MS, trackTimestamps: {} }
    });

    loadResumeState(BASE_TIME_MS);

    const first = peekResumeData("bar", BASE_TIME_MS);
    const second = peekResumeData("bar", BASE_TIME_MS);

    assert(first);
    assert(second);
    assertEqual(first.segmentIndex, second.segmentIndex, "same segment index across peeks");
  });
});

describe("logStreamResume", () => {

  // The line a resume whose counters and init segment measure a figure logs: the mean of the video track's 120.5 seconds and the audio track's 126.5.
  const FIGURE_LINE = "Resuming stream for Channel from previous session (2m 4s of prior content).";

  // The line a resume whose data measures nothing logs: the same sentence without the parenthetical.
  const BARE_LINE = "Resuming stream for Channel from previous session.";

  test("logs the position the persisted counters reached over the persisted init segment's timescales, once, at info", (t: TestContext) => {

    const lines = captureResumeLines(t, makeResumeData(makeInitSegment(VIDEO_TRAK, AUDIO_TRAK), [ [ 1, 10845000n ], [ 2, 6072000n ] ]));

    assert.deepEqual(lines, [FIGURE_LINE]);
  });

  test("logs the same figure with the counters inserted in the other order", (t: TestContext) => {

    const lines = captureResumeLines(t, makeResumeData(makeInitSegment(VIDEO_TRAK, AUDIO_TRAK), [ [ 2, 6072000n ], [ 1, 10845000n ] ]));

    assert.deepEqual(lines, [FIGURE_LINE]);
  });

  test("logs the bare line for a null init segment", (t: TestContext) => {

    const lines = captureResumeLines(t, makeResumeData(null, [ [ 1, 10845000n ], [ 2, 6072000n ] ]));

    assert.deepEqual(lines, [BARE_LINE]);
  });

  test("logs the bare line for counters whose track ids the moov does not declare", (t: TestContext) => {

    const lines = captureResumeLines(t, makeResumeData(makeInitSegment(VIDEO_TRAK, AUDIO_TRAK), [ [ 3, 10845000n ], [ 4, 6072000n ] ]));

    assert.deepEqual(lines, [BARE_LINE]);
  });

  test("logs the bare line for empty counters", (t: TestContext) => {

    const lines = captureResumeLines(t, makeResumeData(makeInitSegment(VIDEO_TRAK, AUDIO_TRAK), []));

    assert.deepEqual(lines, [BARE_LINE]);
  });

  test("logs the bare line for an init segment that carries no moov", (t: TestContext) => {

    const lines = captureResumeLines(t, makeResumeData(makeBox("ftyp", Buffer.from("isom")), [ [ 1, 10845000n ], [ 2, 6072000n ] ]));

    assert.deepEqual(lines, [BARE_LINE]);
  });

  test("logs the bare line for a moov rebuilt around a first track cut ten bytes short, rather than throwing", (t: TestContext) => {

    /* The moov's declared size matches the bytes it holds, so the box parser emits it and the walk reaches the truncated track, whose own declared size runs
     * past its buffer. An init segment cut short inside the moov instead never emits the box at all, because the parser holds an incomplete box and discards
     * it at flush - the no-moov row's case, not this one.
     */
    const lines = captureResumeLines(t, makeResumeData(makeInitSegment(VIDEO_TRAK.subarray(0, VIDEO_TRAK.length - 10)), [ [ 1, 10845000n ], [ 2, 6072000n ] ]));

    assert.deepEqual(lines, [BARE_LINE]);
  });
});

describe("deleteResumeData", () => {

  let tempDir: string;

  beforeEach(async () => {

    tempDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-resume-test-"));
    initializeDataDir(tempDir);
  });

  afterEach(async () => {

    await rm(tempDir, { force: true, recursive: true });
  });

  test("removes the entry so subsequent peek returns null", async () => {

    await makeResumeFile(tempDir, {


      gone: { initVersion: 0, segmentIndex: 99, timestamp: BASE_TIME_MS, trackTimestamps: {} }
    });

    loadResumeState(BASE_TIME_MS);

    deleteResumeData("gone");
    assertEqual(peekResumeData("gone", BASE_TIME_MS), null, "post-delete peek returns null");
    assertEqual(getResumeSegmentIndex("gone", BASE_TIME_MS), null);
  });

  test("is a no-op for unknown channels", () => {

    let threw = false;

    try {

      deleteResumeData("never-existed");
    } catch {

      threw = true;
    }

    assertEqual(threw, false, "delete on unknown channel did not throw");
  });
});

describe("getResumeSegmentIndex", () => {

  let tempDir: string;

  beforeEach(async () => {

    tempDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-resume-test-"));
    initializeDataDir(tempDir);
  });

  afterEach(async () => {

    await rm(tempDir, { force: true, recursive: true });
  });

  test("returns null for an unknown channel", () => {

    assertEqual(getResumeSegmentIndex("nope", BASE_TIME_MS), null);
  });

  test("returns the segment index for a recent entry", async () => {

    await makeResumeFile(tempDir, {


      ok: { initVersion: 0, segmentIndex: 17, timestamp: BASE_TIME_MS, trackTimestamps: {} }
    });

    loadResumeState(BASE_TIME_MS);

    assertEqual(getResumeSegmentIndex("ok", BASE_TIME_MS), 17);
  });

  test("returns null when the TTL check fails (read-time staleness check)", async () => {

    // The function double-checks TTL on each read so callers don't need to. Even if loadResumeState accepted the entry, a later read past TTL must reject.
    await makeResumeFile(tempDir, {


      maybe: { initVersion: 0, segmentIndex: 5, timestamp: BASE_TIME_MS, trackTimestamps: {} }
    });

    loadResumeState(BASE_TIME_MS);

    assertEqual(getResumeSegmentIndex("maybe", BASE_TIME_MS + 91000), null, "stale entry filtered at read time");
  });
});

describe("saveResumeState", () => {

  let tempDir: string;

  beforeEach(async () => {

    tempDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-resume-test-"));
    initializeDataDir(tempDir);
  });

  afterEach(async () => {

    await rm(tempDir, { force: true, recursive: true });
  });

  test("writes active stream entries to disk in JSON form", () => {

    // Round-trip via saveResumeState then loadResumeState. The on-disk shape is opaque - we only check that load can read it back.
    saveResumeState([{


      channelName: "alpha",
      initSegment: null,
      initVersion: 2,
      segmentIndex: 200,
      trackTimestamps: new Map([[ 1, 9000000n ]])
    }], BASE_TIME_MS);

    loadResumeState(BASE_TIME_MS);

    assertEqual(getResumeSegmentIndex("alpha", BASE_TIME_MS), 200, "saved entry recovered after load");

    const data = peekResumeData("alpha", BASE_TIME_MS);

    assert(data);
    assertEqual(data.initVersion, 3, "init version incremented on peek");
    assertEqual(data.trackTimestamps.get(1), 9000000n);
  });

  test("does NOT create the file when there are no active entries and no carry-forward state", () => {

    // Boundary: empty input AND empty in-memory map -> no file. We first clear any in-memory state that prior tests in the suite may have left behind by issuing
    // deleteResumeData for every channel name those tests touched. Sibling tests cover the merge-with-carryforward path in isolation; this case is specifically
    // about the "nothing to save" branch.
    for(const key of [ "alpha", "bar", "boundary", "cnn", "espn", "expired", "foo", "fresh", "gone", "maybe", "ok", "quiet", "same", "stale" ]) {

      deleteResumeData(key);
    }

    saveResumeState([], BASE_TIME_MS);

    let threw = false;

    try {

      readFileSync(path.join(tempDir, "hls-resume.json"), "utf-8");
    } catch {

      threw = true;
    }

    assertEqual(threw, true, "no file created for empty save");
  });

  test("active stream data takes precedence over carried-forward entries with the same channel name", async () => {

    // Locks the merge ordering. If both an in-memory carry-forward entry and an active stream exist for the same channel, the active stream wins.
    await makeResumeFile(tempDir, {


      same: { initVersion: 1, segmentIndex: 1, timestamp: BASE_TIME_MS, trackTimestamps: {} }
    });

    loadResumeState(BASE_TIME_MS);

    saveResumeState([{


      channelName: "same",
      initSegment: null,
      initVersion: 9,
      segmentIndex: 999,
      trackTimestamps: new Map()
    }], BASE_TIME_MS);

    // Reload to read what we just saved.
    loadResumeState(BASE_TIME_MS);

    assertEqual(getResumeSegmentIndex("same", BASE_TIME_MS), 999, "active stream value won the merge");
  });

  test("does not throw when the resume file path is unwritable (fs.writeFileSync fails)", () => {

    /* The save path wraps fs.writeFileSync in try/catch and emits a warning rather than throwing - shutdown must remain robust to a momentarily unwritable
     * data directory (read-only filesystem, permission flip, parent removed by an external process). This test exercises that catch block. We trigger the
     * branch by pointing initializeDataDir at a path that contains a non-directory component as its parent, so writeFileSync raises ENOTDIR / ENOENT depending
     * on the platform. The function must swallow the error and return cleanly.
     */
    const unwritablePath = path.join(tempDir, "this-is-a-file");

    writeFileSync(unwritablePath, "not a directory", "utf-8");

    // Now point the data dir at a child path BENEATH that file. fs.writeFileSync inside saveResumeState will fail because "this-is-a-file" is not a directory.
    initializeDataDir(path.join(unwritablePath, "child"));

    let threw = false;

    try {

      saveResumeState([{


        channelName: "alpha",
        initSegment: null,
        initVersion: 0,
        segmentIndex: 1,
        trackTimestamps: new Map()
      }], BASE_TIME_MS);
    } catch {

      threw = true;
    }

    assertEqual(threw, false, "writeFileSync failure swallowed by saveResumeState's try/catch");

    // Restore the data dir for any subsequent setup; the afterEach hook removes the temp tree regardless.
    initializeDataDir(tempDir);
  });

  test("round-trips a non-null initSegment Buffer through save -> load -> peek with bytewise equality", () => {

    /* This test asserts the base64 encode/decode path with a non-null initSegment. A regression in the encode side, the decode side, or the Map-key
     * stringification could silently corrupt the segment without affecting any other test. We seed a 256-byte Buffer with distinguishable content (sequential
     * byte values mod 256) so any byte slip surfaces as a mismatch.
     */
    const original = Buffer.alloc(256);

    for(let i = 0; i < 256; i++) {

      original[i] = i;
    }

    saveResumeState([{


      channelName: "bytes",
      initSegment: original,
      initVersion: 1,
      segmentIndex: 100,
      trackTimestamps: new Map([[ 1, 12345n ]])
    }], BASE_TIME_MS);

    loadResumeState(BASE_TIME_MS);

    const peeked = peekResumeData("bytes", BASE_TIME_MS);

    assert(peeked, "entry recovered after save -> load");
    assert(peeked.initSegment, "initSegment present after the base64 round-trip");
    assertEqual(peeked.initSegment.equals(original), true, "initSegment bytes match the saved buffer exactly");
  });

  test("does not carry forward in-memory entries whose timestamp has aged past the TTL", async () => {

    /* The carry-forward branch in saveResumeState filters in-memory entries by `(now - entry.timestamp) <= RESUME_TTL` so a multi-restart scenario does not
     * resurrect entries that have been stale for more than 90 seconds. The "active stream wins" test exercises the merge with a fresh-timestamp carryforward;
     * this case asserts the negative branch where the carryforward is older than TTL.
     */
    await makeResumeFile(tempDir, {


      stale: { initVersion: 0, segmentIndex: 50, timestamp: BASE_TIME_MS, trackTimestamps: {} }
    });

    loadResumeState(BASE_TIME_MS);

    // Save at an instant past the 90-second TTL boundary, so the in-memory entry classifies as stale to the carry-forward filter.
    // Save with NO active streams. The carry-forward filter should drop the stale entry, producing zero entries; saveResumeState's "Nothing to save" branch
    // skips the write entirely so the file does not exist on disk afterward.
    saveResumeState([], BASE_TIME_MS + 91000);

    let fileExists = true;

    try {

      readFileSync(path.join(tempDir, "hls-resume.json"), "utf-8");
    } catch {

      fileExists = false;
    }

    assertEqual(fileExists, false, "stale carryforward dropped, save produces no file");
  });
});

/* assertEqual is a thin wrapper over assert.equal that gives the test bodies above a familiar Vitest-style call shape. We keep it inline rather than promoting to
 * testing.helpers.ts because no other test file currently needs this shorthand.
 */
function assertEqual<T>(actual: T, expected: T, message?: string): void {

  if(message === undefined) {

    assert.equal(actual, expected);

    return;
  }

  assert.equal(actual, expected, message);
}
