/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userProfiles.ts: User profile and domain mapping persistence for PrismCast.
 */
import { DOMAIN_CONFIG, SITE_PROFILES } from "./sites.js";
import type { DomainConfig, ProfilesValidationResult, SiteProfile, UserProfilesFile, UserProfilesLoadResult } from "../types/index.js";
import { LOG, containsNonPrintable } from "../utils/index.js";
import { getDataDir, getProfilesFilePath } from "./paths.js";
import { extractDomain } from "../utils/format.js";
import fs from "node:fs";

const { promises: fsPromises } = fs;

/* PrismCast allows users to define custom site profiles and domain mappings in profiles.json inside the data directory. User profiles extend built-in profiles and
 * are merged at runtime, with user domain mappings taking precedence over built-in mappings for domain conflicts. This module handles persistence, validation, and
 * cache management for user-defined profiles and domains.
 *
 * The profiles.json file contains two top-level keys:
 *   - "profiles": Custom site profile definitions (each must extend a built-in profile)
 *   - "domains": Domain-to-profile mappings (can reference built-in or user profiles)
 *
 * User profiles cannot extend other user profiles — only built-in profiles. This prevents cascading breakage when a referenced user profile is deleted.
 */

// Valid SiteProfile behavior flag names that users can set. Metadata fields (category, description, extends, summary) are handled separately.
const VALID_PROFILE_FLAGS = new Set([
  "channelSelector", "clickSelector", "clickToPlay", "fullscreenKey", "fullscreenSelector", "hideSelector", "lockVolumeProperties",
  "needsIframeHandling", "noVideo", "selectReadyVideo", "useRequestFullscreen", "waitForNetworkIdle"
]);

// Generic strategies available for user profiles. Provider-specific strategies are built-in implementations and cannot be used by user profiles.
const GENERIC_STRATEGIES = new Set([ "none", "thumbnailRow", "tileClick" ]);

// All recognized strategy names (generic + provider-specific). Used for validation error messages.
const ALL_STRATEGIES = new Set([ "foxGrid", "guideGrid", "hboGrid", "none", "slingGrid", "thumbnailRow", "tileClick", "youtubeGrid" ]);

// Strategies that require a matchSelector to identify channel elements.
const STRATEGIES_REQUIRING_MATCH_SELECTOR = new Set([ "thumbnailRow", "tileClick" ]);

// Site-specific profiles that cannot be used as a base for user-defined profiles. These profiles have strategies, selectors, or flags tightly coupled to a specific
// streaming service's DOM structure. Users targeting these services should use the predefined channels directly, not create custom profiles extending them.
export const EXCLUDED_PROFILES = new Set([ "disneyNow", "disneyPlus", "foxLive", "hboMax", "huluLive", "slingLive", "youtubeTV" ]);

// Module-level storage for loaded user profiles and domains. Populated at startup and updated on save.
let loadedUserProfiles: Record<string, SiteProfile> = {};
let loadedUserDomains: Record<string, DomainConfig> = {};
let userProfilesParseError = false;
let userProfilesParseErrorMessage: string | undefined;

/**
 * Returns whether the user profiles file had a parse error.
 * @returns True if the profiles file exists but contains invalid JSON.
 */
export function hasProfilesParseError(): boolean {

  return userProfilesParseError;
}

/**
 * Returns the parse error message if the profiles file had a parse error.
 * @returns The error message or undefined.
 */
export function getProfilesParseErrorMessage(): string | undefined {

  return userProfilesParseErrorMessage;
}

/**
 * Returns a copy of the loaded user profiles.
 * @returns Record of profile names to SiteProfile definitions.
 */
export function getUserProfiles(): Record<string, SiteProfile> {

  return { ...loadedUserProfiles };
}

/**
 * Returns a copy of the loaded user domain mappings.
 * @returns Record of domain hostnames to DomainConfig entries.
 */
export function getUserDomains(): Record<string, DomainConfig> {

  return { ...loadedUserDomains };
}

/**
 * Loads user profiles and domain mappings from the profiles file. Returns empty objects if the file doesn't exist, and sets parseError if the file exists but
 * contains invalid JSON.
 * @returns The loaded profiles and domains with parse status.
 */
export async function loadUserProfiles(): Promise<UserProfilesLoadResult> {

  try {

    const content = await fsPromises.readFile(getProfilesFilePath(), "utf-8");

    try {

      // Parse as an untyped record to manually validate structure before assigning to typed interfaces.
      const parsed = JSON.parse(content) as Record<string, unknown>;

      const profiles: Record<string, SiteProfile> = {};
      const domains: Record<string, DomainConfig> = {};

      // Extract profiles if present and valid.
      const rawProfiles = parsed.profiles;

      if((typeof rawProfiles === "object") && (rawProfiles !== null) && !Array.isArray(rawProfiles)) {

        for(const [ key, value ] of Object.entries(rawProfiles as Record<string, unknown>)) {

          if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

            profiles[key] = value as SiteProfile;
          }
        }
      }

      // Extract domains if present and valid.
      const rawDomains = parsed.domains;

      if((typeof rawDomains === "object") && (rawDomains !== null) && !Array.isArray(rawDomains)) {

        for(const [ key, value ] of Object.entries(rawDomains as Record<string, unknown>)) {

          if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

            domains[key] = value as DomainConfig;
          }
        }
      }

      return { domains, parseError: false, profiles };
    } catch(parseError) {

      const message = (parseError instanceof Error) ? parseError.message : String(parseError);

      LOG.warn("Invalid JSON in profiles file %s: %s. Skipping user profiles.", getProfilesFilePath(), message);

      return { domains: {}, parseError: true, parseErrorMessage: message, profiles: {} };
    }
  } catch(error) {

    // File doesn't exist — this is normal, no user profiles defined.
    if((error as NodeJS.ErrnoException).code === "ENOENT") {

      return { domains: {}, parseError: false, profiles: {} };
    }

    // Other read errors — log and skip user profiles.
    LOG.warn("Failed to read profiles file %s: %s. Skipping user profiles.", getProfilesFilePath(), (error instanceof Error) ? error.message : String(error));

    return { domains: {}, parseError: false, profiles: {} };
  }
}

/**
 * Saves user profiles and domain mappings to the profiles file and updates the in-memory cache. Creates the data directory if it doesn't exist. Keys are sorted
 * for consistent output.
 * @param profiles - The profiles to save.
 * @param domains - The domain mappings to save.
 * @throws If the file cannot be written.
 */
export async function saveUserProfiles(profiles: Record<string, SiteProfile>, domains: Record<string, DomainConfig>): Promise<void> {

  // Ensure data directory exists.
  await fsPromises.mkdir(getDataDir(), { recursive: true });

  // Build the file contents with sorted keys for consistent output.
  const file: UserProfilesFile = {};

  // Only include sections that have entries.
  const sortedDomainKeys = Object.keys(domains).sort();
  const sortedProfileKeys = Object.keys(profiles).sort();

  if(sortedDomainKeys.length > 0) {

    const sortedDomains: Record<string, DomainConfig> = {};

    for(const key of sortedDomainKeys) {

      sortedDomains[key] = domains[key];
    }

    file.domains = sortedDomains;
  }

  if(sortedProfileKeys.length > 0) {

    const sortedProfiles: Record<string, SiteProfile> = {};

    for(const key of sortedProfileKeys) {

      sortedProfiles[key] = profiles[key];
    }

    file.profiles = sortedProfiles;
  }

  // Write with pretty formatting for readability.
  const content = JSON.stringify(file, null, 2);

  await fsPromises.writeFile(getProfilesFilePath(), content + "\n", "utf-8");

  // Update in-memory cache.
  loadedUserProfiles = { ...profiles };
  loadedUserDomains = { ...domains };

  // Clear any previous parse error.
  userProfilesParseError = false;
  userProfilesParseErrorMessage = undefined;
}

/**
 * Deletes a user profile by key and removes any domain mappings that reference it. Reloads the file from disk before modifying to avoid data loss.
 * @param key - The profile key to delete.
 * @throws If the file cannot be read or written.
 */
export async function deleteUserProfile(key: string): Promise<void> {

  const result = await loadUserProfiles();

  if(result.parseError) {

    throw new Error("Cannot delete profile: profiles file contains invalid JSON.");
  }

  // Remove the profile.
  Reflect.deleteProperty(result.profiles, key);

  // Remove any domain mappings that reference this profile.
  for(const [ domain, config ] of Object.entries(result.domains)) {

    if(config.profile === key) {

      Reflect.deleteProperty(result.domains, domain);
    }
  }

  await saveUserProfiles(result.profiles, result.domains);

  LOG.info("User profile '%s' deleted.", key);
}

/**
 * Deletes a single user domain mapping. Reloads the file from disk before modifying to avoid data loss.
 * @param domain - The domain hostname to remove.
 * @throws If the file cannot be read or written.
 */
export async function deleteUserDomain(domain: string): Promise<void> {

  const result = await loadUserProfiles();

  if(result.parseError) {

    throw new Error("Cannot delete domain: profiles file contains invalid JSON.");
  }

  Reflect.deleteProperty(result.domains, domain);

  await saveUserProfiles(result.profiles, result.domains);

  LOG.info("User domain mapping '%s' deleted.", domain);
}

/**
 * Initializes user profiles by loading them from the file. Called once at server startup before profile validation and channel loading.
 */
export async function initializeUserProfiles(): Promise<void> {

  const result = await loadUserProfiles();

  loadedUserProfiles = result.profiles;
  loadedUserDomains = result.domains;
  userProfilesParseError = result.parseError;
  userProfilesParseErrorMessage = result.parseErrorMessage;

  // Check for non-printable characters in loaded profile and domain string values. These warnings are informational — loaded data is not modified.
  for(const [ profileKey, profile ] of Object.entries(loadedUserProfiles)) {

    for(const [ field, value ] of Object.entries(profile)) {

      if((typeof value === "string") && containsNonPrintable(value)) {

        LOG.warn("User profile '%s' field '%s' contains non-printable characters. Re-save the profile to clean it.", profileKey, field);
      }
    }
  }

  for(const [ domain, config ] of Object.entries(loadedUserDomains)) {

    for(const [ field, value ] of Object.entries(config)) {

      if((typeof value === "string") && containsNonPrintable(value)) {

        LOG.warn("User domain '%s' field '%s' contains non-printable characters. Re-save the domain mapping to clean it.", domain, field);
      }
    }
  }

  const profileCount = Object.keys(loadedUserProfiles).length;
  const domainCount = Object.keys(loadedUserDomains).length;

  if((profileCount > 0) || (domainCount > 0)) {

    LOG.info("Loaded %d user profile(s) and %d domain mapping(s).", profileCount, domainCount);
  }
}

// Validation Functions.

/**
 * Validates a profile key for format, length, and uniqueness against built-in profiles.
 * @param key - The profile key to validate.
 * @param isNew - True if this is a new profile (checks for duplicates among user profiles).
 * @returns Error message if invalid, undefined if valid.
 */
export function validateProfileKey(key: string, isNew: boolean): string | undefined {

  if(!key || (key.trim() === "")) {

    return "Profile key is required.";
  }

  // Check format: must start with a letter, then letters, digits, and hyphens. Must not end with a hyphen. Profile keys follow camelCase convention but we also
  // allow hyphens for user profiles.
  if(!/^[a-zA-Z]([a-zA-Z0-9-]*[a-zA-Z0-9])?$/.test(key)) {

    return "Profile key must start with a letter, contain only letters, numbers, and hyphens, and not end with a hyphen.";
  }

  if(key.length > 50) {

    return "Profile key must be 50 characters or less.";
  }

  // Built-in profile keys are reserved.
  if(key in SITE_PROFILES) {

    return "Profile key '" + key + "' conflicts with a built-in profile. Choose a different name.";
  }

  // Check for duplicates when adding a new profile.
  if(isNew && (key in loadedUserProfiles)) {

    return "A user profile with this key already exists.";
  }

  return undefined;
}

/**
 * Validates a user-defined site profile. Checks that extends references a built-in profile, strategy is recognized and generic, matchSelector is present when
 * required, and all flag names are valid SiteProfile fields.
 * @param key - The profile key (for error messages).
 * @param profile - The profile definition to validate.
 * @returns Array of error messages (empty if valid).
 */
export function validateProfile(key: string, profile: SiteProfile): string[] {

  const errors: string[] = [];

  // The extends field is required for user profiles — they must build on a built-in profile.
  if(!profile.extends) {

    errors.push("Profile '" + key + "': extends is required. User profiles must extend a built-in profile.");

    return errors;
  }

  // extends must reference a built-in profile (not another user profile).
  if(!(profile.extends in SITE_PROFILES)) {

    errors.push("Profile '" + key + "': extends references non-existent built-in profile '" + profile.extends + "'.");
  } else if(EXCLUDED_PROFILES.has(profile.extends)) {

    // Site-specific profiles are tightly coupled to a streaming service's DOM structure and cannot be meaningfully extended by user profiles.
    errors.push("Profile '" + key + "': '" + profile.extends + "' is a site-specific profile and cannot be extended. " +
      "Use the predefined channels for this service instead.");
  }

  // Validate channel selection configuration if present.
  if(profile.channelSelection) {

    const strategy = profile.channelSelection.strategy;

    if(!ALL_STRATEGIES.has(strategy)) {

      errors.push("Profile '" + key + "': unrecognized channel selection strategy '" + strategy + "'.");
    } else if(!GENERIC_STRATEGIES.has(strategy)) {

      errors.push("Profile '" + key + "': strategy '" + strategy + "' is a built-in provider strategy and cannot be used by user profiles. " +
        "Use 'tileClick', 'thumbnailRow', or 'none'.");
    }

    // matchSelector is required for tileClick and thumbnailRow strategies.
    if(STRATEGIES_REQUIRING_MATCH_SELECTOR.has(strategy) && !profile.channelSelection.matchSelector) {

      errors.push("Profile '" + key + "': matchSelector is required when using the '" + strategy + "' strategy.");
    }
  }

  // Validate that all remaining top-level fields are recognized SiteProfile behavior flags. Fields validated separately above (channelSelection) and metadata-only
  // fields (category, description, extends, summary) are excluded from this check.
  const handledFields = new Set([ "category", "channelSelection", "description", "extends", "summary" ]);

  for(const field of Object.keys(profile)) {

    if(!handledFields.has(field) && !VALID_PROFILE_FLAGS.has(field)) {

      errors.push("Profile '" + key + "': unrecognized flag '" + field + "'.");
    }
  }

  return errors;
}

/**
 * Validates a domain mapping. Checks hostname format, profile references, provider/providerTag strings, loginUrl format, and maxContinuousPlayback type and range.
 * @param domain - The domain hostname.
 * @param config - The domain configuration.
 * @param availableProfiles - Set of available profile names (built-in + user, including profiles in the same import batch).
 * @returns Array of error messages (empty if valid).
 */
export function validateDomain(domain: string, config: DomainConfig, availableProfiles: Set<string>): string[] {

  const errors: string[] = [];

  // Validate domain is a plausible hostname.
  if(!domain || (domain.trim() === "")) {

    errors.push("Domain is required.");

    return errors;
  }

  // Basic hostname format check: must have at least one dot and contain only valid characters.
  if(!/^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}$/.test(domain)) {

    errors.push("Domain '" + domain + "': invalid hostname format.");
  }

  // Reject domains that collide with built-in domain mappings. User domains that shadow built-in domains cause the built-in provider to disappear from the system,
  // affecting all channels on that domain. Users should set the profile field on individual channels to use a custom profile on a built-in domain instead.
  const conciseDomain = extractDomain("https://" + domain);
  const collidesWithBuiltin = (DOMAIN_CONFIG[domain] as DomainConfig | undefined) ?? (DOMAIN_CONFIG[conciseDomain] as DomainConfig | undefined);

  if(collidesWithBuiltin) {

    const builtinProvider = collidesWithBuiltin.provider ?? conciseDomain;

    errors.push("Domain '" + domain + "' is already mapped to built-in provider '" + builtinProvider +
      "'. Set the profile field on individual channels to use your custom profile instead.");
  }

  // profile must reference an existing profile if specified.
  if(config.profile && !availableProfiles.has(config.profile)) {

    errors.push("Domain '" + domain + "': references non-existent profile '" + config.profile + "'.");
  }

  // provider must be non-empty if specified.
  if((config.provider !== undefined) && ((typeof config.provider !== "string") || (config.provider.trim() === ""))) {

    errors.push("Domain '" + domain + "': provider must be a non-empty string.");
  }

  // providerTag must be non-empty if specified.
  if((config.providerTag !== undefined) && ((typeof config.providerTag !== "string") || (config.providerTag.trim() === ""))) {

    errors.push("Domain '" + domain + "': providerTag must be a non-empty string.");
  }

  // loginUrl must be a valid http/https URL if specified.
  if(config.loginUrl !== undefined) {

    if((typeof config.loginUrl !== "string") || (config.loginUrl.trim() === "")) {

      errors.push("Domain '" + domain + "': loginUrl must be a non-empty string.");
    } else {

      try {

        const parsed = new URL(config.loginUrl);

        if((parsed.protocol !== "http:") && (parsed.protocol !== "https:")) {

          errors.push("Domain '" + domain + "': loginUrl must use http or https protocol.");
        }
      } catch {

        errors.push("Domain '" + domain + "': loginUrl is not a valid URL.");
      }
    }
  }

  // maxContinuousPlayback must be a positive number if specified.
  if(config.maxContinuousPlayback !== undefined) {

    if((typeof config.maxContinuousPlayback !== "number") || !Number.isFinite(config.maxContinuousPlayback) ||
      (config.maxContinuousPlayback <= 0)) {

      errors.push("Domain '" + domain + "': maxContinuousPlayback must be a positive number.");
    }
  }

  return errors;
}

/**
 * Validates an entire import batch of profiles and domains. Returns the validated entries and any errors found. Used by both file import and provider pack import.
 * @param data - The raw data to validate (profiles and/or domains).
 * @returns Validation result with validated entries and errors.
 */
export function validateImportedProfiles(data: unknown): ProfilesValidationResult {

  const errors: string[] = [];
  const validProfiles: Record<string, SiteProfile> = {};
  const validDomains: Record<string, DomainConfig> = {};

  if((typeof data !== "object") || (data === null) || Array.isArray(data)) {

    return { domains: {}, errors: ["Invalid format: expected an object with profiles and/or domains."], profiles: {}, valid: false };
  }

  const parsed = data as UserProfilesFile;

  // Validate profiles.
  if(parsed.profiles) {

    if((typeof parsed.profiles !== "object") || Array.isArray(parsed.profiles)) {

      errors.push("Invalid profiles field: expected an object.");
    } else {

      for(const [ key, profile ] of Object.entries(parsed.profiles)) {

        // Check key format.
        const keyError = validateProfileKey(key, false);

        if(keyError) {

          errors.push("Profile '" + key + "': " + keyError);

          continue;
        }

        // Validate profile content.
        const profileErrors = validateProfile(key, profile);

        if(profileErrors.length > 0) {

          errors.push(...profileErrors);

          continue;
        }

        validProfiles[key] = profile;
      }
    }
  }

  // Build the set of available profile names for domain validation: built-in profiles + successfully validated user profiles from this import + existing user
  // profiles.
  const availableProfiles = new Set([
    ...Object.keys(SITE_PROFILES),
    ...Object.keys(validProfiles),
    ...Object.keys(loadedUserProfiles)
  ]);

  // Validate domains.
  if(parsed.domains) {

    if((typeof parsed.domains !== "object") || Array.isArray(parsed.domains)) {

      errors.push("Invalid domains field: expected an object.");
    } else {

      for(const [ domain, config ] of Object.entries(parsed.domains)) {

        const domainErrors = validateDomain(domain, config, availableProfiles);

        if(domainErrors.length > 0) {

          errors.push(...domainErrors);

          continue;
        }

        validDomains[domain] = config;
      }
    }
  }

  return { domains: validDomains, errors, profiles: validProfiles, valid: errors.length === 0 };
}
