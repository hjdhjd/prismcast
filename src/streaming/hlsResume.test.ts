/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsResume.test.ts: Unit tests for HLS sequence resume across PrismCast restarts. hlsResume.ts persists final media-sequence numbers and per-track timestamps to
 * disk during shutdown, then loads them at the next startup so HLS playlists continue advancing forward instead of resetting to 0. The TTL guard discards entries
 * older than 90 seconds so stale resume state does not poison a fresh recording. The tests exercise the file round-trip, TTL discard, peek/delete consume contract,
 * and the merge-with-active-streams path used by saveResumeState.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { deleteResumeData, getResumeSegmentIndex, loadResumeState, peekResumeData, saveResumeState } from "./hlsResume.ts";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { readFileSync, writeFileSync } from "node:fs";
import assert from "node:assert/strict";
import { initializeDataDir } from "../config/paths.ts";
import os from "node:os";
import path from "node:path";

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
    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
  });

  afterEach(async () => {

    mock.timers.reset();
    await rm(tempDir, { force: true, recursive: true });
  });

  test("populates the resume map from a valid file and deletes the file afterward", async () => {

    // The implementation reads then unlinks the file synchronously. Locks the read-then-delete contract that prevents stale resume data on the next start.
    const filePath = path.join(tempDir, "hls-resume.json");

    await makeResumeFile(tempDir, {


      cnn: { initVersion: 5, segmentIndex: 1234, timestamp: 1_700_000_000_000, trackTimestamps: { 1: "9000000" } }
    });

    loadResumeState();

    let postLoadFileExists = true;

    try {

      readFileSync(filePath, "utf-8");
    } catch {

      postLoadFileExists = false;
    }

    assertEqual(postLoadFileExists, false, "file deleted after read");
    assertEqual(getResumeSegmentIndex("cnn"), 1234, "entry available in memory after load");
  });

  test("loads a recent entry and exposes it via getResumeSegmentIndex", async () => {

    await makeResumeFile(tempDir, {


      espn: { initVersion: 0, segmentIndex: 42, timestamp: 1_700_000_000_000, trackTimestamps: {} }
    });

    loadResumeState();

    assert(typeof getResumeSegmentIndex !== "undefined");
    assertEqual(getResumeSegmentIndex("espn"), 42, "loaded segment index for espn");
  });

  test("discards entries older than the 90-second TTL", async () => {

    // Boundary: an entry with timestamp older than (now - 90000) is dropped at load time.
    const expiredTs = 1_700_000_000_000 - 91_000;

    await makeResumeFile(tempDir, {


      old: { initVersion: 0, segmentIndex: 999, timestamp: expiredTs, trackTimestamps: {} }
    });

    loadResumeState();

    assertEqual(getResumeSegmentIndex("old"), null, "expired entry not loaded");
  });

  test("loads entries inside the TTL window even at the boundary", async () => {

    // The TTL check uses '> RESUME_TTL', so an entry exactly 90000ms old is still inside the window.
    const boundaryTs = 1_700_000_000_000 - 90_000;

    await makeResumeFile(tempDir, {


      boundary: { initVersion: 0, segmentIndex: 7, timestamp: boundaryTs, trackTimestamps: {} }
    });

    loadResumeState();

    assertEqual(getResumeSegmentIndex("boundary"), 7, "TTL boundary inclusive");
  });

  test("is a no-op when the resume file does not exist (clean start)", () => {

    // Negative test: missing file is the normal first-startup case. loadResumeState must not throw and must leave the map empty.
    let threw = false;

    try {

      loadResumeState();
    } catch {

      threw = true;
    }

    assertEqual(threw, false, "missing file did not throw");
    assertEqual(getResumeSegmentIndex("anything"), null, "empty map after no-file load");
  });

  test("discards corrupt JSON and continues with an empty map", async () => {

    // Negative test: a malformed file is silently discarded with a warning. The map stays empty.
    const filePath = path.join(tempDir, "hls-resume.json");

    await writeFile(filePath, "{ not valid json", "utf-8");

    let threw = false;

    try {

      loadResumeState();
    } catch {

      threw = true;
    }

    assertEqual(threw, false, "corrupt JSON did not throw");
    assertEqual(getResumeSegmentIndex("anything"), null, "empty map after corrupt-file load");
  });
});

describe("peekResumeData", () => {

  let tempDir: string;

  beforeEach(async () => {

    tempDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-resume-test-"));
    initializeDataDir(tempDir);
    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
  });

  afterEach(async () => {

    mock.timers.reset();
    await rm(tempDir, { force: true, recursive: true });
  });

  test("returns the resume data with initVersion incremented and the same segment index", async () => {

    // The peek path increments initVersion by 1 - the new segmenter must produce a different init URI than the prior session so HLS clients re-fetch.
    await makeResumeFile(tempDir, {


      foo: { initVersion: 3, segmentIndex: 100, timestamp: 1_700_000_000_000, trackTimestamps: { 1: "1000" } }
    });

    loadResumeState();

    const data = peekResumeData("foo");

    assert(data, "peek returned data");
    assertEqual(data.segmentIndex, 100, "segment index preserved");
    assertEqual(data.initVersion, 4, "init version incremented");
    assertEqual(data.trackTimestamps.get(1), 1000n, "track timestamps deserialized to bigint");
  });

  test("returns null for unknown channels", () => {

    assertEqual(peekResumeData("unknown-channel"), null);
  });

  test("returns null and removes the entry when its TTL has expired", async () => {

    // The peek path includes a defensive TTL recheck. Stale entries are evicted lazily.
    await makeResumeFile(tempDir, {


      stale: { initVersion: 0, segmentIndex: 1, timestamp: 1_700_000_000_000, trackTimestamps: {} }
    });

    loadResumeState();

    // Advance clock past TTL.
    mock.timers.tick(91_000);

    assertEqual(peekResumeData("stale"), null, "expired entry returns null");
    // After eviction, the segment index lookup must also fail.
    assertEqual(getResumeSegmentIndex("stale"), null);
  });

  test("does NOT consume the entry on read - same data returned on a second peek", async () => {

    // The two-step pattern: peek then deleteResumeData. Locks the contract that peek alone preserves the entry.
    await makeResumeFile(tempDir, {


      bar: { initVersion: 1, segmentIndex: 50, timestamp: 1_700_000_000_000, trackTimestamps: {} }
    });

    loadResumeState();

    const first = peekResumeData("bar");
    const second = peekResumeData("bar");

    assert(first);
    assert(second);
    assertEqual(first.segmentIndex, second.segmentIndex, "same segment index across peeks");
  });
});

describe("deleteResumeData", () => {

  let tempDir: string;

  beforeEach(async () => {

    tempDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-resume-test-"));
    initializeDataDir(tempDir);
    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
  });

  afterEach(async () => {

    mock.timers.reset();
    await rm(tempDir, { force: true, recursive: true });
  });

  test("removes the entry so subsequent peek returns null", async () => {

    await makeResumeFile(tempDir, {


      gone: { initVersion: 0, segmentIndex: 99, timestamp: 1_700_000_000_000, trackTimestamps: {} }
    });

    loadResumeState();

    deleteResumeData("gone");
    assertEqual(peekResumeData("gone"), null, "post-delete peek returns null");
    assertEqual(getResumeSegmentIndex("gone"), null);
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
    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
  });

  afterEach(async () => {

    mock.timers.reset();
    await rm(tempDir, { force: true, recursive: true });
  });

  test("returns null for an unknown channel", () => {

    assertEqual(getResumeSegmentIndex("nope"), null);
  });

  test("returns the segment index for a recent entry", async () => {

    await makeResumeFile(tempDir, {


      ok: { initVersion: 0, segmentIndex: 17, timestamp: 1_700_000_000_000, trackTimestamps: {} }
    });

    loadResumeState();

    assertEqual(getResumeSegmentIndex("ok"), 17);
  });

  test("returns null when the TTL check fails (read-time staleness check)", async () => {

    // The function double-checks TTL on each read so callers don't need to. Even if loadResumeState accepted the entry, a later read past TTL must reject.
    await makeResumeFile(tempDir, {


      maybe: { initVersion: 0, segmentIndex: 5, timestamp: 1_700_000_000_000, trackTimestamps: {} }
    });

    loadResumeState();

    mock.timers.tick(91_000);

    assertEqual(getResumeSegmentIndex("maybe"), null, "stale entry filtered at read time");
  });
});

describe("saveResumeState", () => {

  let tempDir: string;

  beforeEach(async () => {

    tempDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-resume-test-"));
    initializeDataDir(tempDir);
    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
  });

  afterEach(async () => {

    mock.timers.reset();
    await rm(tempDir, { force: true, recursive: true });
  });

  test("writes active stream entries to disk in JSON form", () => {

    // Round-trip via saveResumeState then loadResumeState. The on-disk shape is opaque - we only check that load can read it back.
    saveResumeState([{


      channelName: "alpha",
      initSegment: null,
      initVersion: 2,
      segmentIndex: 200,
      trackTimestamps: new Map([[ 1, 9_000_000n ]])
    }]);

    loadResumeState();

    assertEqual(getResumeSegmentIndex("alpha"), 200, "saved entry recovered after load");

    const data = peekResumeData("alpha");

    assert(data);
    assertEqual(data.initVersion, 3, "init version incremented on peek");
    assertEqual(data.trackTimestamps.get(1), 9_000_000n);
  });

  test("does NOT create the file when there are no active entries and no carry-forward state", () => {

    // Boundary: empty input AND empty in-memory map -> no file. We first clear any in-memory state that prior tests in the suite may have left behind by issuing
    // deleteResumeData for every channel name those tests touched. Sibling tests cover the merge-with-carryforward path in isolation; this case is specifically
    // about the "nothing to save" branch.
    for(const key of [ "alpha", "bar", "boundary", "cnn", "espn", "foo", "gone", "maybe", "ok", "same", "stale" ]) {

      deleteResumeData(key);
    }

    saveResumeState([]);

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


      same: { initVersion: 1, segmentIndex: 1, timestamp: 1_700_000_000_000, trackTimestamps: {} }
    });

    loadResumeState();

    saveResumeState([{


      channelName: "same",
      initSegment: null,
      initVersion: 9,
      segmentIndex: 999,
      trackTimestamps: new Map()
    }]);

    // Reload to read what we just saved.
    loadResumeState();

    assertEqual(getResumeSegmentIndex("same"), 999, "active stream value won the merge");
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
      }]);
    } catch {

      threw = true;
    }

    assertEqual(threw, false, "writeFileSync failure swallowed by saveResumeState's try/catch");

    // Restore the data dir for any subsequent setup; the afterEach hook removes the temp tree regardless.
    initializeDataDir(tempDir);
  });

  test("round-trips a non-null initSegment Buffer through save -> load -> peek with bytewise equality", () => {

    /* This test pins the base64 encode/decode path with a non-null initSegment. A regression in the encode side, the decode side, or the Map-key
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
    }]);

    loadResumeState();

    const peeked = peekResumeData("bytes");

    assert(peeked, "entry recovered after save -> load");
    assert(peeked.initSegment, "initSegment present after the base64 round-trip");
    assertEqual(peeked.initSegment.equals(original), true, "initSegment bytes match the saved buffer exactly");
  });

  test("does not carry forward in-memory entries whose timestamp has aged past the TTL", async () => {

    /* The carry-forward branch in saveResumeState filters in-memory entries by `(now - entry.timestamp) <= RESUME_TTL` so a multi-restart scenario does not
     * resurrect entries that have been stale for more than 90 seconds. The "active stream wins" test exercises the merge with a fresh-timestamp carryforward;
     * this case pins the negative branch where the carryforward is older than TTL.
     */
    await makeResumeFile(tempDir, {


      stale: { initVersion: 0, segmentIndex: 50, timestamp: 1_700_000_000_000, trackTimestamps: {} }
    });

    loadResumeState();

    // Advance virtual time past the 90-second TTL boundary so the in-memory entry now classifies as stale. The mock.timers harness above is enabled by
    // beforeEach and reset in afterEach, so the advance survives until this test completes.
    mock.timers.tick(91_000);

    // Save with NO active streams. The carry-forward filter should drop the stale entry, producing zero entries; saveResumeState's "Nothing to save" branch
    // skips the write entirely so the file does not exist on disk afterward.
    saveResumeState([]);

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
