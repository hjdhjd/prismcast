/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * providerPacks.ts: Provider pack import/export logic for PrismCast.
 */
import type { ChannelMap, DomainConfig, ProfilesValidationResult, ProviderPack, SiteProfile } from "../types/index.js";
import { getChannelListing, loadUserChannels, saveUserChannels } from "./userChannels.js";
import { getUserDomains, getUserProfiles, saveUserProfiles, validateImportedProfiles } from "./userProfiles.js";
import { sanitizeString } from "../utils/index.js";

/* Provider packs bundle a profile, domain mapping(s), and optionally channels into a single JSON file for distribution. On import, the contents are validated
 * and split into profiles.json and channels.json. On export, a user profile and its associated domain mappings and channels are packaged for sharing.
 */

// Current provider pack format version.
const CURRENT_VERSION = 1;

/**
 * Result of parsing a provider pack.
 */
export interface ParseResult {

  // Validation errors if any.
  errors: string[];

  // The validated pack contents if parsing succeeded.
  pack?: ProviderPack;
}

/**
 * Summary of what was imported from a provider pack. The primary import (profiles and domains) is tracked separately from the secondary channel import so that
 * callers can report partial success accurately — profiles may save while channels fail.
 */
export interface ImportSummary {

  // Number of channels added (0 if channel import failed or no channels in pack).
  channelsAdded: number;

  // Number of domain mappings added.
  domainsAdded: number;

  // Non-fatal warnings from secondary operations (e.g., channel import failure). Empty when everything succeeds.
  errors: string[];

  // Number of profiles added.
  profilesAdded: number;

  // True if the primary import (profiles and domains) succeeded. Channel import warnings do not affect this flag.
  success: boolean;
}

/**
 * Parses and validates a raw provider pack import. Checks the version field, validates profiles and domains via the userProfiles validation functions.
 * @param data - The raw JSON data to parse.
 * @returns Parse result with validated pack or errors.
 */
export function parseProviderPack(data: unknown): ParseResult {

  const errors: string[] = [];

  if((typeof data !== "object") || (data === null) || Array.isArray(data)) {

    return { errors: ["Invalid format: expected a JSON object."] };
  }

  const raw = data as Record<string, unknown>;

  // Validate required fields. Sanitize the pack name after the type check.
  if((typeof raw.name !== "string") || (raw.name.trim() === "")) {

    errors.push("Missing or empty 'name' field.");
  } else {

    raw.name = sanitizeString(raw.name);
  }

  if((typeof raw.version !== "number") || !Number.isInteger(raw.version)) {

    errors.push("Missing or invalid 'version' field (must be an integer).");
  } else if(raw.version > CURRENT_VERSION) {

    errors.push("Unsupported version " + String(raw.version) + ". This version of PrismCast supports up to version " + String(CURRENT_VERSION) + ".");
  }

  if(!raw.profiles || (typeof raw.profiles !== "object") || Array.isArray(raw.profiles) || (Object.keys(raw.profiles as Record<string, unknown>).length === 0)) {

    errors.push("Missing or empty 'profiles' field (at least one profile is required).");
  }

  // If basic structure checks failed, return early.
  if(errors.length > 0) {

    return { errors };
  }

  // Validate the profiles and domains using the shared validation pipeline.
  const validationResult: ProfilesValidationResult = validateImportedProfiles({

    domains: raw.domains,
    profiles: raw.profiles
  });

  if(!validationResult.valid) {

    return { errors: validationResult.errors };
  }

  // Validate channels if present using basic checks (full channel validation happens on import).
  const channels: ChannelMap = {};

  if(raw.channels) {

    if((typeof raw.channels !== "object") || Array.isArray(raw.channels)) {

      errors.push("Invalid 'channels' field: expected an object.");
    } else {

      // Basic channel structure validation — detailed validation happens during importProviderPack.
      for(const [ key, value ] of Object.entries(raw.channels as Record<string, unknown>)) {

        if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

          const ch = value as Record<string, unknown>;

          if((typeof ch.url === "string") && (typeof ch.name === "string")) {

            // Sanitize channel string fields to strip non-printable characters before storing.
            ch.name = sanitizeString(ch.name);
            ch.url = sanitizeString(ch.url);

            if(typeof ch.channelSelector === "string") {

              ch.channelSelector = sanitizeString(ch.channelSelector);
            }

            if(typeof ch.stationId === "string") {

              ch.stationId = sanitizeString(ch.stationId);
            }

            if(typeof ch.profile === "string") {

              ch.profile = sanitizeString(ch.profile);
            }

            channels[key] = value as ChannelMap[string];
          } else {

            errors.push("Channel '" + key + "': requires 'name' and 'url' fields.");
          }
        }
      }
    }
  }

  if(errors.length > 0) {

    return { errors };
  }

  const pack: ProviderPack = {

    name: raw.name as string,
    profiles: validationResult.profiles,
    version: raw.version as number
  };

  if(Object.keys(validationResult.domains).length > 0) {

    pack.domains = validationResult.domains;
  }

  if(Object.keys(channels).length > 0) {

    pack.channels = channels;
  }

  return { errors: [], pack };
}

/**
 * Imports a validated provider pack by writing its contents to profiles.json and optionally channels.json. Returns a summary of what was added.
 * @param pack - The validated provider pack to import.
 * @param options - Import options. Set skipChannels to true to skip importing channels even if the pack contains them.
 * @returns Summary of what was imported.
 */
export async function importProviderPack(pack: ProviderPack, options: { skipChannels?: boolean } = {}): Promise<ImportSummary> {

  const errors: string[] = [];

  // Merge profiles: add pack profiles to existing user profiles.
  const existingProfiles = getUserProfiles();
  const existingDomains = getUserDomains();
  const mergedProfiles = { ...existingProfiles, ...pack.profiles };
  const mergedDomains = { ...existingDomains, ...(pack.domains ?? {}) };

  const profilesAdded = Object.keys(pack.profiles).length;
  const domainsAdded = Object.keys(pack.domains ?? {}).length;

  try {

    await saveUserProfiles(mergedProfiles, mergedDomains);
  } catch(error) {

    errors.push("Failed to save profiles: " + ((error instanceof Error) ? error.message : String(error)));

    return { channelsAdded: 0, domainsAdded: 0, errors, profilesAdded: 0, success: false };
  }

  // Import channels if present and not skipped. Channel import is secondary — failures here produce warnings but do not mark the overall import as failed since
  // profiles and domains were already saved successfully above.
  let channelsAdded = 0;

  if(!options.skipChannels && pack.channels && (Object.keys(pack.channels).length > 0)) {

    try {

      const result = await loadUserChannels();

      if(result.parseError) {

        errors.push("Cannot import channels: channels file contains invalid JSON.");
      } else {

        const mergedChannels = { ...result.channels, ...pack.channels };

        await saveUserChannels(mergedChannels);
        channelsAdded = Object.keys(pack.channels).length;
      }
    } catch(error) {

      errors.push("Failed to save channels: " + ((error instanceof Error) ? error.message : String(error)));
    }
  }

  return {

    channelsAdded,
    domainsAdded,
    errors,
    profilesAdded,
    success: true
  };
}

/**
 * Exports one or more user profiles and their associated domain mappings as a provider pack JSON object. Optionally includes channels that reference the
 * selected profiles.
 * @param profileKeys - Array of user profile keys to export.
 * @param options - Export options controlling what to include.
 * @returns The provider pack object, or null if none of the requested profiles exist.
 */
export function exportProviderPack(
  profileKeys: string[],
  options: { includeChannels?: boolean; includeDomains?: boolean; name?: string } = {}
): ProviderPack | null {

  const userProfiles = getUserProfiles();
  const matchedProfiles: Record<string, SiteProfile> = {};

  for(const key of profileKeys) {

    const profile = userProfiles[key] as SiteProfile | undefined;

    if(profile) {

      matchedProfiles[key] = profile;
    }
  }

  // Return null if none of the requested profiles exist.
  if(Object.keys(matchedProfiles).length === 0) {

    return null;
  }

  const keySet = new Set(Object.keys(matchedProfiles));

  const pack: ProviderPack = {

    name: options.name ?? profileKeys.join(", "),
    profiles: matchedProfiles,
    version: CURRENT_VERSION
  };

  // Include domain mappings that reference any of the selected profiles.
  if(options.includeDomains !== false) {

    const userDomains = getUserDomains();
    const matchingDomains: Record<string, DomainConfig> = {};

    for(const [ domain, config ] of Object.entries(userDomains)) {

      if(config.profile && keySet.has(config.profile)) {

        matchingDomains[domain] = config;
      }
    }

    if(Object.keys(matchingDomains).length > 0) {

      pack.domains = matchingDomains;
    }
  }

  // Include channels that reference any of the selected profiles.
  if(options.includeChannels) {

    const listing = getChannelListing();
    const channels: ChannelMap = {};

    for(const entry of listing) {

      if(entry.channel.profile && keySet.has(entry.channel.profile)) {

        channels[entry.key] = entry.channel;
      }
    }

    if(Object.keys(channels).length > 0) {

      pack.channels = channels;
    }
  }

  return pack;
}
