/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * playlistHint.ts: Single source of truth for the Channels DVR playlist reload hint appended to response messages.
 *
 * The hint is appended to user-facing messages whenever a mutation changes a field that appears in the generated M3U playlist. Channels DVR caches the playlist,
 * so the user needs to reload it to see downstream changes. Centralizing the field list and the hint text here ensures every call site agrees on what "relevant"
 * means and what prose to append.
 */
import type { Channel, ChannelDelta, StoredChannel } from "../../../../types/index.js";

/* Fields that appear in the generated M3U playlist and affect Channels DVR's view of the channel. When any of these change, we append PLAYLIST_HINT so the user
 * knows to reload the playlist in Channels DVR for the change to take effect. The `satisfies` constraint ensures every entry is a real Channel property name, so
 * renaming a Channel field flags this list at compile time instead of silently orphaning the check.
 */
export const M3U_FIELDS = [ "channelNumber", "guideTitle", "logoUrl", "name", "stationId", "tvgShift" ] as const satisfies readonly (keyof Channel)[];

// Appended to success messages when an M3U-affecting field changed. Begins with a leading space so callers can concatenate directly onto their message.
export const PLAYLIST_HINT = " Reload the playlist in Channels DVR to see this change.";

/**
 * Returns PLAYLIST_HINT when a stored channel entry contains any M3U-affecting fields, otherwise an empty string. Used when reverting or removing an override to
 * decide whether the response should include the reload hint.
 * @param stored - The stored channel data (may be a delta or full definition).
 * @returns The hint string, or an empty string when no M3U-affecting fields are present.
 */
export function playlistHintForStored(stored: StoredChannel | undefined): string {

  return stored && M3U_FIELDS.some((f) => f in stored) ? PLAYLIST_HINT : "";
}

/**
 * Returns PLAYLIST_HINT when a delta contains any M3U-affecting field changes, otherwise an empty string. Used after storing a delta override to decide whether
 * the response should include the reload hint.
 * @param delta - The channel delta that was stored.
 * @returns The hint string, or an empty string when no M3U-affecting fields changed.
 */
export function playlistHintForDelta(delta: ChannelDelta): string {

  return M3U_FIELDS.some((f) => f in delta) ? PLAYLIST_HINT : "";
}

/**
 * Returns PLAYLIST_HINT when any M3U-affecting field differs between two channel snapshots, otherwise an empty string. Used during an edit of a user channel
 * (non-delta) where the entire channel record is rewritten - we compare before and after to decide whether the change is playlist-visible.
 * @param previous - The channel record before the mutation, or undefined when adding a new channel.
 * @param next - The channel record after the mutation.
 * @returns The hint string, or an empty string when no M3U-affecting fields changed.
 */
export function playlistHintForChange(previous: Channel | undefined, next: Channel): string {

  // Adds always change the playlist - a new channel is inherently a new M3U entry.
  if(!previous) {

    return PLAYLIST_HINT;
  }

  return M3U_FIELDS.some((f) => previous[f] !== next[f]) ? PLAYLIST_HINT : "";
}
