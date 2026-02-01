/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.ts: User channel file management for PrismCast.
 */
import type { Channel, ChannelListingEntry, ChannelMap } from "../types/index.js";
import { CONFIG } from "./index.js";
import { LOG } from "../utils/index.js";
import { PREDEFINED_CHANNELS } from "../channels/index.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const { promises: fsPromises } = fs;

/*
 * USER CHANNELS FILE
 *
 * PrismCast allows users to define custom channels in ~/.prismcast/channels.json. These user channels are merged with the predefined channels from the source code,
 * with user channels taking precedence when there are key conflicts. This allows users to:
 *
 * 1. Add new channels not included in the default set
 * 2. Override predefined channels with custom URLs or profiles
 * 3. Customize channel metadata like display names or station IDs
 *
 * The channels file is separate from the config file to keep channel definitions independent of server settings. Changes made through the web UI take effect
 * immediately for new stream requests.
 */

/*
 * USER CHANNEL TYPES
 *
 * User channels have the same structure as predefined channels. The UserChannel type is equivalent to Channel but defined here for clarity in the context of user
 * configuration.
 */

/**
 * User-defined channel with all channel properties.
 */
export type UserChannel = Channel;

/**
 * Map of channel keys to user-defined channel configurations.
 */
export type UserChannelMap = ChannelMap;

/**
 * Result of loading user channels from the file.
 */
export interface UserChannelsLoadResult {

  // The loaded user channels (empty object if file doesn't exist or parse error).
  channels: UserChannelMap;

  // True if the file exists but contains invalid JSON.
  parseError: boolean;

  // Error message if parseError is true.
  parseErrorMessage?: string;
}

/*
 * CHANNELS FILE PATH
 *
 * The channels file is stored in the same data directory as the config file (~/.prismcast).
 */

const dataDir = path.join(os.homedir(), ".prismcast");
const channelsFilePath = path.join(dataDir, "channels.json");

/**
 * Returns the path to the user channels file.
 * @returns The absolute path to ~/.prismcast/channels.json.
 */
export function getUserChannelsFilePath(): string {

  return channelsFilePath;
}

/*
 * CHANNELS FILE OPERATIONS
 *
 * These functions handle reading and writing the channels file. All operations are async and handle errors gracefully.
 */

// Module-level storage for loaded user channels. This is populated at startup and used by getAllChannels().
let loadedUserChannels: UserChannelMap = {};
let userChannelsParseError = false;
let userChannelsParseErrorMessage: string | undefined;

/**
 * Returns whether the user channels file had a parse error.
 * @returns True if the channels file exists but contains invalid JSON.
 */
export function hasChannelsParseError(): boolean {

  return userChannelsParseError;
}

/**
 * Returns the parse error message if the channels file had a parse error.
 * @returns The error message or undefined.
 */
export function getChannelsParseErrorMessage(): string | undefined {

  return userChannelsParseErrorMessage;
}

/**
 * Loads user channels from the channels file. Returns an empty map if the file doesn't exist, and sets parseError if the file exists but contains invalid JSON.
 * @returns The loaded channels with parse status.
 */
export async function loadUserChannels(): Promise<UserChannelsLoadResult> {

  try {

    const content = await fsPromises.readFile(channelsFilePath, "utf-8");

    try {

      const channels = JSON.parse(content) as UserChannelMap;

      return { channels, parseError: false };
    } catch(parseError) {

      const message = (parseError instanceof Error) ? parseError.message : String(parseError);

      LOG.warn("Invalid JSON in channels file %s: %s. Using predefined channels only.", channelsFilePath, message);

      return { channels: {}, parseError: true, parseErrorMessage: message };
    }
  } catch(error) {

    // File doesn't exist - this is normal, use predefined channels only.
    if((error as NodeJS.ErrnoException).code === "ENOENT") {

      return { channels: {}, parseError: false };
    }

    // Other read errors - log and use predefined channels.
    LOG.warn("Failed to read channels file %s: %s. Using predefined channels only.", channelsFilePath, (error instanceof Error) ? error.message : String(error));

    return { channels: {}, parseError: false };
  }
}

/**
 * Saves user channels to the channels file and updates the in-memory cache. Changes take effect immediately for new stream requests without requiring a server
 * restart. Creates the data directory if it doesn't exist.
 * @param channels - The channels to save.
 * @throws If the file cannot be written.
 */
export async function saveUserChannels(channels: UserChannelMap): Promise<void> {

  // Ensure data directory exists.
  await fsPromises.mkdir(dataDir, { recursive: true });

  // Sort channels by key for consistent output.
  const sortedChannels: UserChannelMap = {};
  const sortedKeys = Object.keys(channels).sort();

  for(const key of sortedKeys) {

    sortedChannels[key] = channels[key];
  }

  // Write channels with pretty formatting for readability.
  const content = JSON.stringify(sortedChannels, null, 2);

  await fsPromises.writeFile(channelsFilePath, content + "\n", "utf-8");

  // Update in-memory cache so changes take effect immediately for new stream requests.
  loadedUserChannels = { ...sortedChannels };

  // Clear any previous parse error since we're writing valid data.
  userChannelsParseError = false;
  userChannelsParseErrorMessage = undefined;
}

/**
 * Deletes a user channel by key.
 * @param key - The channel key to delete.
 * @throws If the file cannot be read or written.
 */
export async function deleteUserChannel(key: string): Promise<void> {

  const result = await loadUserChannels();

  // If parse error, we can't modify - just log a warning.
  if(result.parseError) {

    throw new Error("Cannot delete channel: channels file contains invalid JSON.");
  }

  // Remove the channel.
  delete result.channels[key];

  // Save the modified channels.
  await saveUserChannels(result.channels);

  LOG.info("User channel '%s' deleted.", key);
}

/**
 * Resets all user channels by deleting the channels file.
 * @throws If the file exists but cannot be deleted.
 */
export async function resetUserChannels(): Promise<void> {

  try {

    await fsPromises.unlink(channelsFilePath);

    LOG.info("Channels file deleted, using predefined channels only.");
  } catch(error) {

    // File doesn't exist - already using predefined channels.
    if((error as NodeJS.ErrnoException).code === "ENOENT") {

      LOG.info("Channels file does not exist, already using predefined channels only.");

      return;
    }

    throw error;
  }
}

/*
 * CHANNEL INITIALIZATION
 *
 * User channels are loaded at server startup and stored in module-level state. This avoids repeated file reads during request handling.
 */

/**
 * Initializes user channels by loading them from the file. This should be called once at server startup.
 */
export async function initializeUserChannels(): Promise<void> {

  const result = await loadUserChannels();

  loadedUserChannels = result.channels;
  userChannelsParseError = result.parseError;
  userChannelsParseErrorMessage = result.parseErrorMessage;

  const userCount = Object.keys(loadedUserChannels).length;
  const predefinedCount = Object.keys(PREDEFINED_CHANNELS).length;
  const totalCount = userCount + predefinedCount;

  if(userCount > 0) {

    LOG.info("Loaded %d channels (%d user, %d predefined).", totalCount, userCount, predefinedCount);
  } else {

    LOG.info("Loaded %d channels.", totalCount);
  }
}

/*
 * CHANNEL LISTING AND MERGING
 *
 * The getChannelListing() function is the single source of truth for merging predefined channels with user channels. It returns enriched entries with source
 * classification and enabled status. All other channel retrieval functions that need merged data build on top of it.
 */

/**
 * Returns the full channel listing with source classification and enabled status. This is the authoritative merge point for predefined and user channels — all
 * code that needs a merged view of channels should use this function (or getAllChannels() which delegates to it).
 *
 * For each channel key, the source is classified as:
 * - "predefined": exists only in predefined channels
 * - "user": exists only in user channels
 * - "override": exists in both (user channel data takes precedence)
 *
 * The enabled field reflects whether the channel is available for streaming. Predefined-only channels can be disabled via configuration; user and override
 * channels are always enabled.
 * @returns Sorted array of channel listing entries.
 */
export function getChannelListing(): ChannelListingEntry[] {

  const allKeys = new Set([ ...Object.keys(PREDEFINED_CHANNELS), ...Object.keys(loadedUserChannels) ]);
  const listing: ChannelListingEntry[] = [];

  for(const key of allKeys) {

    const isPredefined = key in PREDEFINED_CHANNELS;
    const isUser = key in loadedUserChannels;

    // Determine source classification. User channel data takes precedence on key conflicts.
    let source: "override" | "predefined" | "user";

    if(isPredefined && isUser) {

      source = "override";
    } else if(isUser) {

      source = "user";
    } else {

      source = "predefined";
    }

    listing.push({

      channel: isUser ? loadedUserChannels[key] : PREDEFINED_CHANNELS[key],
      enabled: !isPredefinedChannelDisabled(key),
      key,
      source
    });
  }

  // Sort alphabetically by key for consistent ordering across all callers.
  listing.sort((a, b) => a.key.localeCompare(b.key));

  return listing;
}

/**
 * Returns all available channels (predefined + user), with user channels taking precedence on key conflicts. Disabled predefined channels are excluded unless they
 * have a user override. Built on top of getChannelListing() to ensure a single merging code path.
 * @returns The merged channel map with disabled predefined channels filtered out.
 */
export function getAllChannels(): ChannelMap {

  const result: ChannelMap = {};

  for(const entry of getChannelListing()) {

    if(entry.enabled) {

      result[entry.key] = entry.channel;
    }
  }

  return result;
}

/**
 * Returns the loaded user channels (without predefined channels).
 * @returns The user channel map.
 */
export function getUserChannels(): UserChannelMap {

  return { ...loadedUserChannels };
}

/**
 * Checks if a channel key exists in the predefined channels.
 * @param key - The channel key to check.
 * @returns True if the channel is predefined.
 */
export function isPredefinedChannel(key: string): boolean {

  return key in PREDEFINED_CHANNELS;
}

/**
 * Checks if a channel key exists in the user channels.
 * @param key - The channel key to check.
 * @returns True if the channel is user-defined.
 */
export function isUserChannel(key: string): boolean {

  return key in loadedUserChannels;
}

/**
 * Checks if a user channel overrides a predefined channel (same key exists in both).
 * @param key - The channel key to check.
 * @returns True if the user channel overrides a predefined channel.
 */
export function isOverrideChannel(key: string): boolean {

  return isPredefinedChannel(key) && isUserChannel(key);
}

/*
 * DISABLED PREDEFINED CHANNELS
 *
 * Users can disable predefined channels to exclude them from the playlist and block streaming. Disabled channels appear grayed out in the UI with an option to
 * re-enable. This is useful for users who don't want certain predefined channels cluttering their channel list.
 */

/**
 * Checks if a predefined channel is disabled.
 * @param key - The channel key to check.
 * @returns True if the channel is predefined and disabled.
 */
export function isPredefinedChannelDisabled(key: string): boolean {

  // Only predefined channels can be disabled via this mechanism.
  if(!isPredefinedChannel(key)) {

    return false;
  }

  // If a user channel overrides this predefined channel, the predefined channel's disabled state is irrelevant.
  if(isUserChannel(key)) {

    return false;
  }

  return CONFIG.channels.disabledPredefined.includes(key);
}

/**
 * Returns the list of disabled predefined channel keys.
 * @returns Array of disabled channel keys.
 */
export function getDisabledPredefinedChannels(): string[] {

  return [...CONFIG.channels.disabledPredefined];
}

/**
 * Returns all predefined channels regardless of disabled state. Used by the UI to show all predefined channels including disabled ones.
 * @returns The predefined channel map.
 */
export function getPredefinedChannels(): ChannelMap {

  return { ...PREDEFINED_CHANNELS };
}

/**
 * Checks if a channel is available for streaming. A channel is available if it exists in the merged channel map returned by getAllChannels(), which already
 * excludes disabled predefined channels (unless overridden by a user channel).
 * @param key - The channel key to check.
 * @returns True if the channel can be streamed.
 */
export function isChannelAvailable(key: string): boolean {

  return key in getAllChannels();
}

/*
 * CHANNEL VALIDATION
 *
 * These functions validate channel data before saving.
 */

/**
 * Validates a channel key for format and uniqueness.
 * @param key - The channel key to validate.
 * @param isNew - True if this is a new channel (checks for duplicates among user channels).
 * @returns Error message if invalid, undefined if valid.
 */
export function validateChannelKey(key: string, isNew: boolean): string | undefined {

  // Check for empty key.
  if(!key || (key.trim() === "")) {

    return "Channel key is required.";
  }

  // Check format: lowercase alphanumeric and hyphens only.
  if(!/^[a-z0-9-]+$/.test(key)) {

    return "Channel key must contain only lowercase letters, numbers, and hyphens.";
  }

  // Check length.
  if(key.length > 50) {

    return "Channel key must be 50 characters or less.";
  }

  // Check for duplicates when adding new channel.
  if(isNew && isUserChannel(key)) {

    return "A user channel with this key already exists.";
  }

  return undefined;
}

/**
 * Validates a channel URL.
 * @param url - The URL to validate.
 * @returns Error message if invalid, undefined if valid.
 */
export function validateChannelUrl(url: string): string | undefined {

  // Check for empty URL.
  if(!url || (url.trim() === "")) {

    return "URL is required.";
  }

  // Check URL format.
  try {

    const parsed = new URL(url);

    // Only allow http and https protocols.
    if((parsed.protocol !== "http:") && (parsed.protocol !== "https:")) {

      return "URL must use http or https protocol.";
    }
  } catch {

    return "Invalid URL format.";
  }

  return undefined;
}

/**
 * Validates a channel name.
 * @param name - The name to validate.
 * @returns Error message if invalid, undefined if valid.
 */
export function validateChannelName(name: string): string | undefined {

  // Check for empty name.
  if(!name || (name.trim() === "")) {

    return "Channel name is required.";
  }

  // Check length.
  if(name.length > 100) {

    return "Channel name must be 100 characters or less.";
  }

  return undefined;
}

/**
 * Validates a profile name.
 * @param profile - The profile name to validate (can be empty for autodetect).
 * @param validProfiles - Array of valid profile names.
 * @returns Error message if invalid, undefined if valid.
 */
export function validateChannelProfile(profile: string | undefined, validProfiles: string[]): string | undefined {

  // Empty profile means autodetect, which is valid.
  if(!profile || (profile.trim() === "")) {

    return undefined;
  }

  // Check if profile exists.
  if(!validProfiles.includes(profile)) {

    return [ "Unknown profile: ", profile, ". Valid profiles: ", validProfiles.join(", "), "." ].join("");
  }

  return undefined;
}

/**
 * Result of validating imported channels.
 */
export interface ChannelsValidationResult {

  // The validated channels if valid.
  channels: UserChannelMap;

  // Validation error messages.
  errors: string[];

  // True if validation passed.
  valid: boolean;
}

/**
 * Validates an imported channels object for structure and content.
 * @param data - The raw imported data to validate.
 * @param validProfiles - Array of valid profile names.
 * @returns Validation result with errors if invalid.
 */
export function validateImportedChannels(data: unknown, validProfiles: string[]): ChannelsValidationResult {

  const errors: string[] = [];

  // Check that input is an object.
  if((typeof data !== "object") || (data === null) || Array.isArray(data)) {

    return { channels: {}, errors: ["Invalid format: expected an object with channel definitions."], valid: false };
  }

  const channels: UserChannelMap = {};
  const entries = Object.entries(data as Record<string, unknown>);

  for(const [ key, value ] of entries) {

    // Validate key format.
    const keyError = validateChannelKey(key, false);

    if(keyError) {

      errors.push("Channel '" + key + "': " + keyError);

      continue;
    }

    // Check that value is an object.
    if((typeof value !== "object") || (value === null) || Array.isArray(value)) {

      errors.push("Channel '" + key + "': expected an object with channel properties.");

      continue;
    }

    const channelData = value as Record<string, unknown>;

    // Validate required name field.
    if((typeof channelData.name !== "string") || (channelData.name.trim() === "")) {

      errors.push("Channel '" + key + "': name is required.");

      continue;
    }

    const nameError = validateChannelName(channelData.name);

    if(nameError) {

      errors.push("Channel '" + key + "': " + nameError);

      continue;
    }

    // Validate required url field.
    if((typeof channelData.url !== "string") || (channelData.url.trim() === "")) {

      errors.push("Channel '" + key + "': url is required.");

      continue;
    }

    const urlError = validateChannelUrl(channelData.url);

    if(urlError) {

      errors.push("Channel '" + key + "': " + urlError);

      continue;
    }

    // Validate optional profile field.
    const profile = (typeof channelData.profile === "string") ? channelData.profile : undefined;
    const profileError = validateChannelProfile(profile, validProfiles);

    if(profileError) {

      errors.push("Channel '" + key + "': " + profileError);

      continue;
    }

    // Build validated channel.
    const channel: UserChannel = {

      name: channelData.name,
      url: channelData.url
    };

    if(profile) {

      channel.profile = profile;
    }

    // Include optional fields if present.
    if(typeof channelData.stationId === "string") {

      channel.stationId = channelData.stationId;
    }

    if(typeof channelData.channelSelector === "string") {

      channel.channelSelector = channelData.channelSelector;
    }

    // Validate optional channelNumber field (range and type).
    if(channelData.channelNumber !== undefined) {

      const num = Number(channelData.channelNumber);

      if(!Number.isInteger(num) || (num < 1) || (num > 99999)) {

        errors.push("Channel '" + key + "': channelNumber must be an integer between 1 and 99999.");

        continue;
      }

      channel.channelNumber = num;
    }

    channels[key] = channel;
  }

  // Validate channelNumber uniqueness across all imported channels. We check after building the full map so that all duplicates are reported.
  const numberToKey = new Map<number, string>();

  for(const [ key, channel ] of Object.entries(channels)) {

    if(channel.channelNumber === undefined) {

      continue;
    }

    const existing = numberToKey.get(channel.channelNumber);

    if(existing) {

      errors.push("Channel '" + key + "': channelNumber " + String(channel.channelNumber) + " is already used by '" + existing + "'.");
    } else {

      numberToKey.set(channel.channelNumber, key);
    }
  }

  return { channels, errors, valid: errors.length === 0 };
}
