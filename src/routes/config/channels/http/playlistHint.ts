/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * playlistHint.ts: Single source of truth for the Channels DVR playlist reload hint appended to response messages.
 *
 * The hint is appended to user-facing messages whenever a mutation changes a field that appears in the generated M3U playlist. Channels DVR caches the playlist,
 * so the user needs to reload it to see downstream changes. Centralizing the field list and the hint text here ensures every call site agrees on what "relevant"
 * means and what prose to append.
 */
import type { ChannelDelta, ResolvedChannel, StoredChannel } from "../../../../types/index.ts";
import { getChannelEffectiveTags } from "../../../../config/userChannels.ts";
import { tagsEqual } from "../../../../config/channelForm.ts";

/* Fields that appear in the generated M3U playlist and affect Channels DVR's view of the channel. When any of these change, we append PLAYLIST_HINT so the
 * user knows to reload the playlist in Channels DVR for the change to take effect. The `satisfies` constraint ensures every entry is a real ResolvedChannel
 * property name, so renaming a Channel identity field flags this list at compile time instead of silently orphaning the check.
 *
 * The list is field-level, and that is an approximation for two of the three helpers: playlistHintForStored and playlistHintForDelta report only that a listed
 * field is present, and rendering can shadow a listed field (guideTitle wins over name) or filter its value (tags are intersected with the active vocabulary),
 * so those two can report a change the playlist does not actually show. playlistHintForChange holds both full channel records, so it compares the tags field the
 * way the playlist renders it rather than by presence alone.
 */
export const M3U_FIELDS = [ "channelNumber", "guideTitle", "logoUrl", "name", "stationId", "tags", "tvgShift" ] as const satisfies readonly (keyof ResolvedChannel)[];

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
export function playlistHintForChange(previous: ResolvedChannel | undefined, next: ResolvedChannel): string {

  // Adds always change the playlist - a new channel is inherently a new M3U entry.
  if(!previous) {

    return PLAYLIST_HINT;
  }

  /* tags is the one array-valued field in the list, so identity comparison would flag every rebuilt-but-equal array. It is also the one field the playlist
   * renders through a filter - the M3U shows effective tags, the intersection with the active vocabulary - so both sides are filtered before the
   * order-independent, case-sensitive content comparison. A change confined to tag strings outside the vocabulary is therefore not playlist-visible and
   * produces no hint. getChannelEffectiveTags returns [] for a channel with no tags, so the absent case needs no defaulting here.
   */
  return M3U_FIELDS.some((f) => (f === "tags") ?
    !tagsEqual(getChannelEffectiveTags(previous), getChannelEffectiveTags(next)) :
    (previous[f] !== next[f])) ? PLAYLIST_HINT : "";
}
