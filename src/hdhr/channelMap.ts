/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelMap.ts: Channel key to channel number mapping for HDHomeRun emulation.
 */
import { getAllChannels } from "../config/userChannels.js";
import { resolveProfile } from "../config/profiles.js";

/*
 * CHANNEL NUMBER MAPPING
 *
 * HDHomeRun devices use numeric channel numbers (GuideNumber) for each channel. Plex requires these for EPG (electronic program guide) matching. PrismCast uses
 * string keys (e.g., "cnn", "nbc") for channels, so we need a mapping layer.
 *
 * Channels can optionally specify an explicit channelNumber for precise EPG alignment. Channels without an explicit number get auto-assigned a number starting
 * from 1000. Auto-assigned numbers are deterministic (alphabetically sorted keys get sequential numbers) but not stable across channel additions or removals.
 * Users who need stable numbers for EPG matching should set explicit channelNumber values.
 *
 * The mapping is rebuilt on each lineup request rather than cached. This ensures immediate consistency when channels are added, removed, or modified through the
 * web UI without requiring restart or cache invalidation.
 */

// Auto-assigned channel numbers start at 1000 to avoid conflicts with low user-assigned numbers.
const AUTO_ASSIGN_START = 1000;

/**
 * Represents a single entry in the channel number mapping.
 */
export interface ChannelMapEntry {

  // The channel key (e.g., "cnn").
  key: string;

  // The channel display name.
  name: string;

  // The numeric channel number (explicit or auto-assigned).
  number: number;

  // Gracenote station ID for guide matching, if available.
  stationId?: string;
}

/**
 * Builds the channel number mapping from the current channel configuration. Channels with explicit channelNumber values are assigned first, then remaining
 * channels are auto-assigned sequential numbers starting from AUTO_ASSIGN_START, skipping any numbers already claimed by explicit assignments.
 * @returns Array of channel map entries sorted by channel number, excluding noVideo and disabled channels.
 */
export function buildChannelMap(): ChannelMapEntry[] {

  const channels = getAllChannels();
  const entries: ChannelMapEntry[] = [];

  // Collect all explicit channel numbers first so auto-assignment can skip them.
  const explicitNumbers = new Set<number>();

  for(const [ key, channel ] of Object.entries(channels)) {

    // Skip non-video channels (static pages like EPGs).
    const profile = resolveProfile(channel.profile);

    if(profile.noVideo) {

      continue;
    }

    if((channel.channelNumber !== undefined) && (channel.channelNumber > 0)) {

      explicitNumbers.add(channel.channelNumber);

      entries.push({

        key,
        name: channel.name,
        number: channel.channelNumber,
        stationId: channel.stationId
      });
    }
  }

  // Auto-assign numbers to channels without explicit channelNumber. Sort keys alphabetically for deterministic assignment.
  const unassignedKeys = Object.keys(channels)
    .filter((key) => {

      const channel = channels[key];
      const profile = resolveProfile(channel.profile);

      return !profile.noVideo && ((channel.channelNumber === undefined) || (channel.channelNumber <= 0));
    })
    .sort();

  let nextNumber = AUTO_ASSIGN_START;

  for(const key of unassignedKeys) {

    const channel = channels[key];

    // Skip numbers already claimed by explicit assignments.
    while(explicitNumbers.has(nextNumber)) {

      nextNumber++;
    }

    entries.push({

      key,
      name: channel.name,
      number: nextNumber,
      stationId: channel.stationId
    });

    nextNumber++;
  }

  // Sort by channel number for consistent lineup order.
  entries.sort((a, b) => a.number - b.number);

  return entries;
}

/**
 * Looks up a channel key by its channel number.
 * @param channelNumber - The numeric channel number to look up.
 * @returns The channel key, or undefined if no channel has this number.
 */
export function getChannelKeyByNumber(channelNumber: number): string | undefined {

  const map = buildChannelMap();
  const entry = map.find((e) => e.number === channelNumber);

  return entry?.key;
}
