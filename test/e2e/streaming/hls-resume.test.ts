/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls-resume.test.ts: Integration coverage for the HLS resume mechanism. The unit tier covers individual functions; this suite verifies the round-trip
 * persistence: save resume state at shutdown, load it at the next startup, retrieve via the public accessors. The saved segmentIndex is the canonical bug
 * class: shutdown saves the segmenter's index less one, so the resumed playlist still includes the last completed segment and Channels DVR does not drop
 * it, and a regression in that value would silently lose the last segment of every recording.
 */
import { createIntegrationContext, initializePersistence, pathInDataDir } from "../../helpers/integration.helpers.ts";
import { deleteResumeData, getResumePosition, loadResumeState, peekResumeData, saveResumeState } from "../../../src/streaming/hlsResume.ts";
import { describe, test } from "node:test";
import { access } from "node:fs/promises";
import assert from "node:assert/strict";

describe("HLS resume state round-trip", () => {

  test("save -> load round-trips the segment index for each channel", async () => {

    /* Save state for two channels with distinct segmentIndex values. After load, both should be retrievable via getResumePosition with the same values.
     * The save -> load boundary is the production restart path; this test exercises it without actually restarting the process.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    saveResumeState([
      { channelName: "abc", discontinuityCount: 0, initSegment: null, initVersion: 1, segmentIndex: 42, trackTimestamps: new Map() },
      { channelName: "nbc", discontinuityCount: 0, initSegment: null, initVersion: 1, segmentIndex: 7, trackTimestamps: new Map() }
    ], Date.now());

    // Now load - simulating the next startup. loadResumeState reads from disk and populates the in-memory map.
    loadResumeState(Date.now());

    assert.equal(getResumePosition("abc", Date.now())?.segmentIndex ?? null, 42, "abc segmentIndex round-trips");
    assert.equal(getResumePosition("nbc", Date.now())?.segmentIndex ?? null, 7, "nbc segmentIndex round-trips");

    // Cleanup so this test does not leak resume state into subsequent tests.
    deleteResumeData("abc");
    deleteResumeData("nbc");
  });

  test("loadResumeState consumes the file (deletes after read) so the same state is not loaded twice", async () => {

    /* The resume file has a one-shot consumption contract: it exists between shutdown and the next startup, and is removed during load. This prevents the
     * same resume state from being applied across multiple restarts when no new save happened in between.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    saveResumeState([{ channelName: "abc", discontinuityCount: 0, initSegment: null, initVersion: 1, segmentIndex: 42, trackTimestamps: new Map() }], Date.now());

    // File exists post-save.
    await assert.doesNotReject(() => access(pathInDataDir(ctx, "hls-resume.json")), "resume file should exist after save");

    loadResumeState(Date.now());

    // File should be gone post-load.
    await assert.rejects(() => access(pathInDataDir(ctx, "hls-resume.json")), /ENOENT/, "resume file should be deleted by loadResumeState");

    deleteResumeData("abc");
  });

  test("peekResumeData returns the seeding payload without consuming it (deleteResumeData required to remove)", async () => {

    /* The two-phase consumption pattern: peek returns the data, and only deleteResumeData removes it. This means a failed segmenter creation can retry with
     * the same resume state on the next attempt instead of starting from zero (the segment index was already used by the prior session, so starting at zero
     * would replay the same segment numbers and confuse Channels DVR).
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    saveResumeState([{ channelName: "abc", discontinuityCount: 0, initSegment: null, initVersion: 1, segmentIndex: 42, trackTimestamps: new Map() }], Date.now());
    loadResumeState(Date.now());

    // First peek returns the data.
    const first = peekResumeData("abc", Date.now());

    assert.ok(first, "first peek returns the resume data");
    assert.equal(first.segmentIndex, 42, "segmentIndex is what we saved");

    // Second peek (without delete) returns the same data.
    const second = peekResumeData("abc", Date.now());

    assert.ok(second, "second peek returns the same data (no consumption)");
    assert.equal(second.segmentIndex, 42, "segmentIndex unchanged");

    // After explicit delete, peek returns null.
    deleteResumeData("abc");

    assert.equal(peekResumeData("abc", Date.now()), null, "peek after delete returns null");
  });

  test("a channel with no saved resume data returns null from getResumePosition", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    assert.equal(getResumePosition("never-saved", Date.now())?.segmentIndex ?? null, null, "no resume data -> null");
  });

  test("an empty save list produces a file that loads without error and has no resume entries", async () => {

    /* Boundary: shutdown with no active streams writes no resume file at all. saveResumeState returns early when there is nothing to merge (merged.size === 0),
     * so no file is produced. The next load then takes the missing-file path, which must not throw and must yield no entries to retrieve.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    saveResumeState([], Date.now());

    assert.doesNotThrow(() => { loadResumeState(Date.now()); }, "empty resume file loads cleanly");
    assert.equal(getResumePosition("any-channel", Date.now())?.segmentIndex ?? null, null, "no entries to retrieve");
  });
});
