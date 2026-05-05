/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * playlistHint.test.ts: Unit tests for the playlist-reload hint helpers. The module is the SSOT for which fields surface in the M3U playlist and for the prose
 * appended to user-facing messages when those fields change. Tests pin the M3U_FIELDS membership, the PLAYLIST_HINT literal, and the three predicate helpers
 * (stored, delta, change). Locking these behaviors prevents an accidental field-list drift from silently suppressing the playlist-reload nudge after a real
 * mutation.
 */
import type { ChannelDelta, StoredChannel } from "../../../../types/index.ts";
import { M3U_FIELDS, PLAYLIST_HINT, playlistHintForChange, playlistHintForDelta, playlistHintForStored } from "./playlistHint.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { makeChannel } from "../../../../config/userChannels.helpers.ts";

describe("M3U_FIELDS", () => {

  test("declares the six expected playlist-affecting fields", () => {

    // The set is the canonical declaration of which fields surface in the generated M3U. Locking the membership prevents a silent expansion or contraction that
    // would change the playlist-reload nudge policy across the codebase.
    assert.deepEqual(
      [...M3U_FIELDS].toSorted(),
      [ "channelNumber", "guideTitle", "logoUrl", "name", "stationId", "tvgShift" ]
    );
  });
});

describe("PLAYLIST_HINT", () => {

  test("starts with a leading space so callers can concatenate directly", () => {

    // Boundary: the leading space lets callers do `message + PLAYLIST_HINT` without inserting a separator. Locking the leading whitespace catches an accidental
    // trim that would produce "...successfully.Reload..." in user-facing toasts.
    assert.equal(PLAYLIST_HINT[0], " ", "first character must be a space");
  });

  test("ends with a period (full sentence)", () => {

    assert.equal(PLAYLIST_HINT.at(-1), ".");
  });

  test("mentions reloading the playlist in Channels DVR", () => {

    assert.match(PLAYLIST_HINT, /Reload the playlist in Channels DVR/);
  });
});

describe("playlistHintForStored", () => {

  test("returns PLAYLIST_HINT when at least one M3U-affecting field is present", () => {

    const stored: StoredChannel = { name: "ABC Custom" };

    assert.equal(playlistHintForStored(stored), PLAYLIST_HINT);
  });

  test("returns PLAYLIST_HINT for each individual M3U-affecting field", () => {

    // We exercise every field in the SSOT list to lock the "any one is enough" semantics.
    for(const field of M3U_FIELDS) {

      const stored: Record<string, unknown> = { [field]: "value" };

      assert.equal(playlistHintForStored(stored as StoredChannel), PLAYLIST_HINT, "field " + field + " should trigger the hint");
    }
  });

  test("returns an empty string when no M3U-affecting fields are present", () => {

    // Channel-binding fields like url/channelSelector are not playlist-visible, so a stored entry containing only those should not trigger the hint.
    const stored: StoredChannel = { channelSelector: "ABC", url: "https://example.com" };

    assert.equal(playlistHintForStored(stored), "");
  });

  test("returns an empty string for undefined input (boundary)", () => {

    assert.equal(playlistHintForStored(undefined), "");
  });

  test("returns an empty string for an empty object", () => {

    assert.equal(playlistHintForStored({}), "");
  });

  test("returns the hint when a field is present even with a null value (clear semantics)", () => {

    // The presence check uses `field in stored`, not value-truthiness, so an explicit clear (null) still counts as a playlist-visible change.
    const stored: StoredChannel = { channelNumber: null };

    assert.equal(playlistHintForStored(stored), PLAYLIST_HINT, "presence-based check honors null-for-clear semantics");
  });
});

describe("playlistHintForDelta", () => {

  test("returns PLAYLIST_HINT when the delta touches at least one M3U-affecting field", () => {

    const delta: ChannelDelta = { name: "Renamed" };

    assert.equal(playlistHintForDelta(delta), PLAYLIST_HINT);
  });

  test("returns the hint for each individual M3U-affecting field in turn", () => {

    for(const field of M3U_FIELDS) {

      const delta: Record<string, unknown> = { [field]: "value" };

      assert.equal(playlistHintForDelta(delta as ChannelDelta), PLAYLIST_HINT, "field " + field + " should trigger the hint");
    }
  });

  test("returns an empty string when the delta touches only non-M3U fields", () => {

    const delta: ChannelDelta = { channelSelector: "ABC", url: "https://example.com" };

    assert.equal(playlistHintForDelta(delta), "");
  });

  test("returns an empty string for an empty delta", () => {

    assert.equal(playlistHintForDelta({}), "");
  });
});

describe("playlistHintForChange", () => {


  test("returns PLAYLIST_HINT for an add (no previous channel)", () => {

    // Boundary: a new channel is inherently a new M3U entry, so an add always triggers the hint regardless of which fields are populated.
    const next = makeChannel();

    assert.equal(playlistHintForChange(undefined, next), PLAYLIST_HINT);
  });

  test("returns PLAYLIST_HINT when name changes", () => {

    const previous = makeChannel({ name: "ABC" });
    const next = makeChannel({ name: "ABC Custom" });

    assert.equal(playlistHintForChange(previous, next), PLAYLIST_HINT);
  });

  test("returns the hint when channelNumber changes", () => {

    const previous = makeChannel({ channelNumber: 4 });
    const next = makeChannel({ channelNumber: 7 });

    assert.equal(playlistHintForChange(previous, next), PLAYLIST_HINT);
  });

  test("returns the hint when stationId changes", () => {

    const previous = makeChannel({ stationId: "12345" });
    const next = makeChannel({ stationId: "57342" });

    assert.equal(playlistHintForChange(previous, next), PLAYLIST_HINT);
  });

  test("returns the hint for each individual M3U-affecting field in turn", () => {

    // Lock the "any field difference is enough" semantics across the SSOT. We toggle one field at a time so a regression where the iteration shrinks would
    // surface as a per-field failure.
    for(const field of M3U_FIELDS) {

      const previous = makeChannel({ [field]: "before" });
      const next = makeChannel({ [field]: "after" });

      assert.equal(playlistHintForChange(previous, next), PLAYLIST_HINT, "field " + field + " difference should trigger the hint");
    }
  });

  test("returns an empty string when no M3U-affecting field differs", () => {

    // Both snapshots share identity; only a non-M3U field (url) differs. The helper must report no playlist-visible change.
    const previous = makeChannel({ name: "ABC", url: "https://old.example.com" });
    const next = makeChannel({ name: "ABC", url: "https://new.example.com" });

    assert.equal(playlistHintForChange(previous, next), "");
  });

  test("returns an empty string when previous and next are identical (boundary)", () => {

    const channel = makeChannel({ name: "ABC", stationId: "12345" });

    assert.equal(playlistHintForChange(channel, channel), "");
  });

  test("uses strict !== so type-coerced equality is treated as a difference", () => {

    // The helper uses `previous[f] !== next[f]`, not loose comparison. A channelNumber stored as 4 versus "4" would be reported as different - lock that
    // contract since storage normalizes types and an accidental relaxation would mask a real channelNumber mismatch.
    const previous = makeChannel({ channelNumber: 4 });
    const next = makeChannel({ channelNumber: "4" as unknown as number });

    assert.equal(playlistHintForChange(previous, next), PLAYLIST_HINT, "type difference must be treated as a real change");
  });
});
