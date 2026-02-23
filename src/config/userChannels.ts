/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.ts: User channel file management for PrismCast.
 */
import type { Channel, ChannelListingEntry, ChannelMap, StoredChannel, StoredChannelMap } from "../types/index.js";
import { buildProviderGroups, getAllProviderTags, getProviderSelections, isChannelAvailableByProvider, isProviderVariant, setEnabledProviders,
  setProviderSelections } from "./providers.js";
import { getChannelsFilePath, getDataDir } from "./paths.js";
import { CONFIG } from "./index.js";
import { LOG } from "../utils/index.js";
import { PREDEFINED_CHANNELS } from "../channels/index.js";
import fs from "node:fs";

const { promises: fsPromises } = fs;

/* PrismCast allows users to define custom channels in channels.json inside the data directory. These user channels are merged with the predefined channels,
 * with user channels taking precedence when there are key conflicts. This allows users to:
 *
 * 1. Add new channels not included in the default set
 * 2. Override predefined channels with custom URLs or profiles
 * 3. Customize channel metadata like display names or station IDs
 *
 * The channels file is separate from the config file to keep channel definitions independent of server settings. Changes made through the web UI take effect
 * immediately for new stream requests.
 */

/* User channels have the same structure as predefined channels. The UserChannel type is equivalent to Channel but defined here for clarity in the context of user
 * configuration.
 */

/**
 * User-defined channel with all channel properties.
 */
export type UserChannel = Channel;

/**
 * Map of channel keys to stored channel data (full definitions or deltas). This is the raw file format for channels.json.
 */
export type UserChannelMap = StoredChannelMap;

/**
 * Result of loading user channels from the file.
 */
export interface UserChannelsLoadResult {

  // The loaded user channels (empty object if file doesn't exist or parse error).
  channels: StoredChannelMap;

  // True if the file exists but contains invalid JSON.
  parseError: boolean;

  // Error message if parseError is true.
  parseErrorMessage?: string;

  // Provider selections loaded from the file (canonical key → provider key).
  providerSelections: Record<string, string>;
}

/* The channels file path is resolved via the centralized paths module (config/paths.ts). The data directory is initialized at startup before channel loading.
 */

/**
 * Returns the path to the user channels file.
 * @returns The absolute path to channels.json inside the data directory.
 */
export function getUserChannelsFilePath(): string {

  return getChannelsFilePath();
}

/* These functions handle reading and writing the channels file. All operations are async and handle errors gracefully.
 */

// Module-level storage for loaded user channels. This is populated at startup and used by getAllChannels(). Entries can be full Channel definitions or
// ChannelDelta overrides for predefined channels.
let loadedUserChannels: StoredChannelMap = {};
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
 * The file can contain a special `providerSelections` key with user's provider preferences, which is extracted separately from channels.
 * @returns The loaded channels with parse status and provider selections.
 */
export async function loadUserChannels(): Promise<UserChannelsLoadResult> {

  try {

    const content = await fsPromises.readFile(getChannelsFilePath(), "utf-8");

    try {

      const parsed = JSON.parse(content) as Record<string, unknown>;

      // Extract providerSelections if present — it's not a channel, it's metadata.
      const providerSelections: Record<string, string> = {};
      const channels: StoredChannelMap = {};

      for(const [ key, value ] of Object.entries(parsed)) {

        if(key === "providerSelections") {

          // Copy provider selections if it's an object.
          if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

            for(const [ selKey, selValue ] of Object.entries(value)) {

              if(typeof selValue === "string") {

                providerSelections[selKey] = selValue;
              }
            }
          }
        } else if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

          // It's a channel definition or delta override.
          channels[key] = value as StoredChannel;
        }
      }

      return { channels, parseError: false, providerSelections };
    } catch(parseError) {

      const message = (parseError instanceof Error) ? parseError.message : String(parseError);

      LOG.warn("Invalid JSON in channels file %s: %s. Using predefined channels only.", getChannelsFilePath(), message);

      return { channels: {}, parseError: true, parseErrorMessage: message, providerSelections: {} };
    }
  } catch(error) {

    // File doesn't exist - this is normal, use predefined channels only.
    if((error as NodeJS.ErrnoException).code === "ENOENT") {

      return { channels: {}, parseError: false, providerSelections: {} };
    }

    // Other read errors - log and use predefined channels.
    LOG.warn("Failed to read channels file %s: %s. Using predefined channels only.", getChannelsFilePath(), (error instanceof Error) ? error.message : String(error));

    return { channels: {}, parseError: false, providerSelections: {} };
  }
}

/**
 * Saves user channels to the channels file and updates the in-memory cache. Changes take effect immediately for new stream requests without requiring a server
 * restart. Creates the data directory if it doesn't exist. Provider selections are also saved if any exist. Empty deltas (no overridden fields) for predefined
 * channel keys are stripped before saving to avoid storing no-op entries.
 * @param channels - The channels to save (full definitions or delta overrides).
 * @throws If the file cannot be written.
 */
export async function saveUserChannels(channels: StoredChannelMap): Promise<void> {

  // Ensure data directory exists.
  await fsPromises.mkdir(getDataDir(), { recursive: true });

  // Strip empty deltas for predefined keys — an empty delta means no changes were made.
  const filtered: StoredChannelMap = {};

  for(const [ key, stored ] of Object.entries(channels)) {

    if((key in PREDEFINED_CHANNELS) && (Object.keys(stored).length === 0)) {

      continue;
    }

    filtered[key] = stored;
  }

  // Sort channels by key for consistent output.
  const sortedChannels: Record<string, Record<string, string> | StoredChannel> = {};
  const sortedKeys = Object.keys(filtered).sort();

  for(const key of sortedKeys) {

    sortedChannels[key] = filtered[key];
  }

  // Include provider selections if any exist.
  const selections = getProviderSelections();

  if(Object.keys(selections).length > 0) {

    // Sort provider selections for consistent output.
    const sortedSelections: Record<string, string> = {};
    const selectionKeys = Object.keys(selections).sort();

    for(const key of selectionKeys) {

      sortedSelections[key] = selections[key];
    }

    sortedChannels.providerSelections = sortedSelections;
  }

  // Write channels with pretty formatting for readability.
  const content = JSON.stringify(sortedChannels, null, 2);

  await fsPromises.writeFile(getChannelsFilePath(), content + "\n", "utf-8");

  // Update in-memory cache so changes take effect immediately for new stream requests.
  loadedUserChannels = { ...filtered };

  // Refresh provider groups so channelsRef reflects the new channel data. This ensures getResolvedChannel() returns correct data after modifications.
  buildProviderGroups(getMergedChannelMap());

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
  Reflect.deleteProperty(result.channels, key);

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

    await fsPromises.unlink(getChannelsFilePath());

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

/* User channels are loaded at server startup and stored in module-level state. This avoids repeated file reads during request handling.
 */

/**
 * Initializes user channels by loading them from the file. This should be called once at server startup. Also builds provider groups and loads provider selections.
 */
export async function initializeUserChannels(): Promise<void> {

  const result = await loadUserChannels();

  loadedUserChannels = result.channels;
  userChannelsParseError = result.parseError;
  userChannelsParseErrorMessage = result.parseErrorMessage;

  // Load provider selections from the file.
  setProviderSelections(result.providerSelections);

  // Load enabled providers from the configuration, validating that each tag is recognized. Invalid tags (e.g., from hand-edited config.json typos) are stripped
  // silently after logging a warning. Validation must happen after buildProviderGroups() because getAllProviderTags() depends on the groups being built.
  const configuredProviders = CONFIG.channels.enabledProviders;

  // Build the merged channels map and then build provider groups.
  const mergedChannels = getMergedChannelMap();

  buildProviderGroups(mergedChannels);

  // Now that provider groups are built, validate the configured provider tags. Strip any unrecognized tags and warn.
  if(configuredProviders.length > 0) {

    const knownTags = new Set(getAllProviderTags().map((t) => t.tag));
    const validTags = configuredProviders.filter((tag) => knownTags.has(tag));
    const invalidTags = configuredProviders.filter((tag) => !knownTags.has(tag));

    if(invalidTags.length > 0) {

      LOG.warn("Ignoring unrecognized provider tags in configuration: %s.", invalidTags.join(", "));
    }

    setEnabledProviders(validTags);
    CONFIG.channels.enabledProviders = validTags;
  } else {

    setEnabledProviders(configuredProviders);
  }

  const userCount = Object.keys(loadedUserChannels).length;
  const predefinedCount = Object.keys(PREDEFINED_CHANNELS).length;
  const totalCount = userCount + predefinedCount;

  if(userCount > 0) {

    LOG.info("Loaded %d channels (%d user, %d predefined).", totalCount, userCount, predefinedCount);
  } else {

    LOG.info("Loaded %d channels.", totalCount);
  }
}

// Fields that users are allowed to override via delta. This allowlist prevents hand-edited channels.json from overriding fields like provider that are
// intentionally not user-editable. Matches the fields in the ChannelDelta interface.
const DELTA_ALLOWED_FIELDS = new Set([ "channelNumber", "channelSelector", "name", "profile", "stationId", "tvgShift", "url" ]);

/**
 * Resolves a stored channel entry (full definition or delta) into a fully resolved Channel. For user-defined channels with no predefined equivalent, the stored
 * entry is returned as-is (it must be a full Channel). For overrides of predefined channels, the predefined definition is used as a base and only allowlisted
 * delta fields are overlaid. Fields set to null in the delta are removed from the result. Fields not in the allowlist are silently ignored.
 * @param key - The channel key.
 * @param stored - The stored channel data (full definition or delta).
 * @returns A fully resolved Channel with all fields populated.
 */
export function resolveStoredChannel(key: string, stored: StoredChannel): Channel {

  const predefined = PREDEFINED_CHANNELS[key];

  // No predefined equivalent — this is a user-defined channel, return as-is. The caller is responsible for ensuring it has a url field.
  // Runtime check needed — TypeScript thinks Record indexing always returns a value, but the key may not exist.
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!predefined) {

    return stored as Channel;
  }

  // Start with a copy of the predefined definition, then overlay allowlisted non-null delta fields.
  const resolved: Channel = { ...predefined };

  for(const [ field, value ] of Object.entries(stored)) {

    if(!DELTA_ALLOWED_FIELDS.has(field)) {

      continue;
    }

    if(value === null) {

      // Explicit null means "clear this field" — delete it from the resolved object.
      Reflect.deleteProperty(resolved, field);
    } else if(value !== undefined) {

      // Non-null, non-undefined — override the predefined value.
      (resolved as unknown as Record<string, unknown>)[field] = value;
    }
  }

  return resolved;
}

/**
 * Returns the merged channel map (predefined + user) without filtering by enabled status or provider variants. Used internally for building provider groups.
 * Resolves any delta overrides into full Channel objects so the result contains only complete definitions.
 * @returns The complete merged channel map.
 */
function getMergedChannelMap(): ChannelMap {

  const result: ChannelMap = { ...PREDEFINED_CHANNELS };

  for(const [ key, stored ] of Object.entries(loadedUserChannels)) {

    result[key] = resolveStoredChannel(key, stored);
  }

  return result;
}

/* The getChannelListing() function is the single source of truth for merging predefined channels with user channels. It returns enriched entries with source
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
 *
 * Provider variants (non-canonical keys in provider groups) are filtered out from this listing — they are accessed via the provider selection mechanism instead.
 *
 * Override entries produce a new resolved Channel object (via resolveStoredChannel()), which is a different reference from PREDEFINED_CHANNELS[key]. The provider
 * system (providers.ts) relies on this reference difference to detect user overrides via isUserOverride(). Predefined-only entries preserve the original reference.
 * @returns Sorted array of channel listing entries.
 */
export function getChannelListing(): ChannelListingEntry[] {

  const allKeys = new Set([ ...Object.keys(PREDEFINED_CHANNELS), ...Object.keys(loadedUserChannels) ]);
  const listing: ChannelListingEntry[] = [];

  for(const key of allKeys) {

    // Skip provider variants — they're accessed via provider selection, not as separate channels.
    if(isProviderVariant(key)) {

      continue;
    }

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

    // For user entries (including overrides), resolve the stored delta/definition into a full Channel. The resolved object is a new reference, which preserves
    // the isUserOverride() contract in providers.ts (reference comparison against PREDEFINED_CHANNELS[key]).
    const channel: Channel = isUser ? resolveStoredChannel(key, loadedUserChannels[key]) : PREDEFINED_CHANNELS[key];

    listing.push({

      availableByProvider: isChannelAvailableByProvider(key),
      channel,
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

    if(entry.enabled && entry.availableByProvider) {

      result[entry.key] = entry.channel;
    }
  }

  return result;
}

/**
 * Returns the raw stored channel data (without predefined channels). Entries may be full Channel definitions or ChannelDelta overrides.
 * @returns The stored channel map.
 */
export function getUserChannels(): StoredChannelMap {

  return { ...loadedUserChannels };
}

/**
 * Returns the predefined channel definition for a key.
 * @param key - The channel key to look up.
 * @returns The predefined channel, or undefined if the key is not predefined.
 */
export function getPredefinedChannel(key: string): Channel | undefined {

  return PREDEFINED_CHANNELS[key];
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

/* Users can disable predefined channels to exclude them from the playlist and block streaming. Disabled channels appear grayed out in the UI with an option to
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

/* These functions validate channel data before saving.
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

  // The validated channels if valid. Import always produces full Channel definitions, not deltas.
  channels: ChannelMap;

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

  const channels: ChannelMap = {};
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

    // Validate optional tvgShift field (must be a finite number). Negative values are valid (e.g., a West Coast feed viewed from the East).
    if(channelData.tvgShift !== undefined) {

      const shift = Number(channelData.tvgShift);

      if(!Number.isFinite(shift)) {

        errors.push("Channel '" + key + "': tvgShift must be a finite number.");

        continue;
      }

      channel.tvgShift = shift;
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

/* Provider selections are stored in the channels.json file alongside user channels. When a selection changes, we save the entire file (channels + selections)
 * to persist the change.
 */

/**
 * Saves the current provider selections to the channels file. This triggers a full file save including all user channels.
 * @throws If the file cannot be written.
 */
export async function saveProviderSelections(): Promise<void> {

  // Simply save the user channels — the saveUserChannels function includes provider selections automatically.
  await saveUserChannels(loadedUserChannels);
}
