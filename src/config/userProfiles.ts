/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userProfiles.ts: User profile and domain mapping persistence for PrismCast.
 */
import { DOMAIN_CONFIG, SITE_PROFILES, getBuiltinProfile, isProviderProfile } from "./sites.js";
import type { DomainConfig, ProfilesValidationResult, SiteProfile, UserProfilesFile, UserProfilesLoadResult } from "../types/index.js";
import { LOG, containsNonPrintable } from "../utils/index.js";
import { createFileStore } from "./persistence.js";
import { extractDomain } from "../utils/format.js";
import { getProfilesFilePath } from "./paths.js";

/* PrismCast allows users to define custom site profiles and domain mappings in profiles.json inside the data directory. User profiles extend built-in profiles and
 * are merged at runtime, with user domain mappings taking precedence over built-in mappings for domain conflicts. This module handles persistence, validation, and
 * cache management for user-defined profiles and domains.
 *
 * The profiles.json file contains two top-level keys:
 *   - "profiles": Custom site profile definitions (each must extend a built-in profile)
 *   - "domains": Domain-to-profile mappings (can reference built-in or user profiles)
 *
 * User profiles cannot extend other user profiles - only built-in profiles. This prevents cascading breakage when a referenced user profile is deleted.
 */

// Legacy profile flag names that have been renamed. Keys are the old names, values are the current names. Used by initializeUserProfiles() and service pack import to
// silently normalize persisted and imported profiles at the boundary where external data enters the system.
const LEGACY_PROFILE_FLAGS: Record<string, string> = { noVideo: "staticCapture" };

// Valid SiteProfile behavior flag names that users can set. Metadata fields (category, description, extends, summary) are handled separately.
const VALID_PROFILE_FLAGS = new Set([
  "channelSelector", "clickSelector", "clickToPlay", "dismissSelector", "fullscreenKey", "fullscreenSelector", "hideSelector", "lockVolumeProperties",
  "needsIframeHandling", "selectReadyVideo", "staticCapture", "useRequestFullscreen", "waitForNetworkIdle"
]);

// Generic strategies available for user profiles. Service-specific strategies are built-in implementations and cannot be used by user profiles.
const GENERIC_STRATEGIES = new Set([ "none", "thumbnailRow", "tileClick" ]);

// All recognized strategy names (generic + service-specific). Used for validation error messages.
const ALL_STRATEGIES = new Set([ "foxGrid", "guideGrid", "hboGrid", "none", "slingGrid", "thumbnailRow", "tileClick", "youtubeGrid" ]);

// Strategies that require a matchSelector to identify channel elements.
const STRATEGIES_REQUIRING_MATCH_SELECTOR = new Set([ "thumbnailRow", "tileClick" ]);


/**
 * Extracts a typed record from an unknown parsed value. Validates that the value is a plain object (not null, not an array) and that each entry is also a plain
 * object. Non-object entries are silently skipped.
 * @param raw - The raw parsed value to extract from.
 * @returns A record of valid object entries.
 */
function extractObjectMap<T>(raw: unknown): Record<string, T> {

  const result: Record<string, T> = {};

  if((typeof raw === "object") && (raw !== null) && !Array.isArray(raw)) {

    for(const [ key, value ] of Object.entries(raw as Record<string, unknown>)) {

      if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

        result[key] = value as T;
      }
    }
  }

  return result;
}

/**
 * Normalizes legacy field names in a set of profiles. Renames any fields listed in LEGACY_PROFILE_FLAGS to their current names in-place. When a profile already
 * contains the current field name, the current value is preserved and the legacy field is deleted without overwriting.
 * @param profiles - The profile records to normalize.
 * @returns True if any field was renamed or removed.
 */
export function normalizeLegacyProfileFlags(profiles: Record<string, SiteProfile>): boolean {

  let changed = false;

  for(const profile of Object.values(profiles)) {

    for(const [ oldName, newName ] of Object.entries(LEGACY_PROFILE_FLAGS)) {

      if(oldName in profile) {

        // Only copy the legacy value if the current field name is not already present. If both exist (e.g., hand-edited JSON), the current name takes precedence.
        if(!(newName in profile)) {

          (profile as Record<string, unknown>)[newName] = (profile as Record<string, unknown>)[oldName];
        }

        Reflect.deleteProperty(profile, oldName);
        changed = true;
      }
    }
  }

  return changed;
}

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

/* Transactional store for profiles.json. The store uses a compound type that carries both profiles and domain mappings. The parse function uses extractObjectMap()
 * to validate sub-objects. The beforeWrite hook conditionally includes/excludes top-level keys based on emptiness.
 */

/**
 * Compound data type for the profiles file store. Carries both profile definitions and domain mappings.
 */
interface ProfilesFileData {

  domains: Record<string, DomainConfig>;
  profiles: Record<string, SiteProfile>;
}

/**
 * Parses raw profiles.json content into the compound data type.
 * @param raw - The raw JSON string from the file.
 * @returns The parsed compound data.
 */
function parseProfilesFile(raw: string): ProfilesFileData {

  const parsed = JSON.parse(raw) as Record<string, unknown>;

  return {

    domains: extractObjectMap<DomainConfig>(parsed.domains),
    profiles: extractObjectMap<SiteProfile>(parsed.profiles)
  };
}

/**
 * Prepares profiles data for writing to disk. Conditionally includes top-level keys only when they have entries.
 * @param data - The compound profiles data.
 * @returns The serializable output.
 */
function prepareProfilesForWrite(data: ProfilesFileData): unknown {

  const file: UserProfilesFile = {};

  if(Object.keys(data.domains).length > 0) {

    file.domains = data.domains;
  }

  if(Object.keys(data.profiles).length > 0) {

    file.profiles = data.profiles;
  }

  return file;
}

// Transactional store instance for profiles.json.
const profilesStore = createFileStore<ProfilesFileData>({

  beforeWrite: prepareProfilesForWrite,
  defaultValue: (): ProfilesFileData => ({ domains: {}, profiles: {} }),
  label: "profiles",
  parse: parseProfilesFile,
  path: getProfilesFilePath
});

/**
 * Reads the current profiles from disk without acquiring the serialization lock. Returns the parsed profiles and domains with parse status. Use this for
 * read-only access and startup initialization. For modifications, use mutateProfiles() instead.
 * @returns The loaded profiles and domains with parse status.
 */
export async function readProfiles(): Promise<UserProfilesLoadResult> {

  const result = await profilesStore.read();

  return {

    domains: result.data.domains,
    parseError: result.parseError,
    parseErrorMessage: result.parseErrorMessage,
    profiles: result.data.profiles
  };
}

/**
 * Serialized read-modify-write operation on profiles.json. The mutation function receives the current profiles and domains as a compound object and can modify
 * them in place. The store handles atomicity, serialization, corruption guard, and backup. After a successful write, the in-memory cache is updated.
 * @param fn - Mutation function. Receives current `{ profiles, domains }`. Modify in place (void return).
 * @throws FileStoreParseError if profiles.json contains invalid JSON.
 */
export async function mutateProfiles(fn: (data: ProfilesFileData) => void): Promise<void> {

  let finalData: ProfilesFileData | undefined;

  await profilesStore.mutate((data: ProfilesFileData) => {

    fn(data);
    finalData = data;
  });

  // Side effects after successful write.
  if(finalData) {

    loadedUserProfiles = { ...finalData.profiles };
    loadedUserDomains = { ...finalData.domains };
    userProfilesParseError = false;
    userProfilesParseErrorMessage = undefined;
  }
}

/**
 * Deletes a user profile by key and removes any domain mappings that reference it.
 * @param key - The profile key to delete.
 * @throws FileStoreParseError if the profiles file contains invalid JSON.
 * @throws If the file cannot be written.
 */
export async function deleteUserProfile(key: string): Promise<void> {

  await mutateProfiles((data) => {

    Reflect.deleteProperty(data.profiles, key);

    // Remove any domain mappings that reference this profile.
    for(const [ domain, config ] of Object.entries(data.domains)) {

      if(config.profile === key) {

        Reflect.deleteProperty(data.domains, domain);
      }
    }
  });

  LOG.info("User profile '%s' deleted.", key);
}

/**
 * Deletes a single user domain mapping.
 * @param domain - The domain hostname to remove.
 * @throws FileStoreParseError if the profiles file contains invalid JSON.
 * @throws If the file cannot be written.
 */
export async function deleteUserDomain(domain: string): Promise<void> {

  await mutateProfiles((data) => {

    Reflect.deleteProperty(data.domains, domain);
  });

  LOG.info("User domain mapping '%s' deleted.", domain);
}

/**
 * Initializes user profiles by loading them from the file. Called once at server startup before profile validation and channel loading.
 */
export async function initializeUserProfiles(): Promise<void> {

  const result = await readProfiles();

  loadedUserProfiles = result.profiles;
  loadedUserDomains = result.domains;
  userProfilesParseError = result.parseError;
  userProfilesParseErrorMessage = result.parseErrorMessage;

  // Normalize legacy profile field names at the persistence boundary. If any fields were renamed, persist the updated file so the migration only runs once.
  // The save is wrapped in its own try/catch so a write failure (disk full, permission error, parse error) is logged as a migration warning rather than
  // propagated to startup. The normalized in-memory profiles are used regardless...the migration is best-effort persistence.
  if(normalizeLegacyProfileFlags(loadedUserProfiles)) {

    try {

      await mutateProfiles((data) => {

        normalizeLegacyProfileFlags(data.profiles);
      });

      LOG.info("Migrated legacy profile flags in user profiles file.");
    } catch(saveError) {

      LOG.warn("Failed to persist migrated profile flags: %s. Migration will retry on next startup.",
        (saveError instanceof Error) ? saveError.message : String(saveError));
    }
  }

  // Check for non-printable characters in loaded profile and domain string values. These warnings are informational - loaded data is not modified.
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

  // The extends field is required for user profiles - they must build on a built-in profile.
  if(!profile.extends) {

    errors.push("Profile '" + key + "': extends is required. User profiles must extend a built-in profile.");

    return errors;
  }

  // extends must reference a built-in general profile (not another user profile or a service-specific profile). Service-specific profiles are tightly coupled to
  // a streaming service's DOM structure and cannot be meaningfully extended by user profiles.
  if(!getBuiltinProfile(profile.extends)) {

    errors.push("Profile '" + key + "': extends references non-existent built-in profile '" + profile.extends + "'.");
  } else if(isProviderProfile(profile.extends)) {

    errors.push("Profile '" + key + "': '" + profile.extends + "' is a service-specific profile and cannot be extended. " +
      "Use the predefined channels for this service instead.");
  }

  // Validate channel selection configuration if present.
  if(profile.channelSelection) {

    const strategy = profile.channelSelection.strategy;

    if(!ALL_STRATEGIES.has(strategy)) {

      errors.push("Profile '" + key + "': unrecognized channel selection strategy '" + strategy + "'.");
    } else if(!GENERIC_STRATEGIES.has(strategy)) {

      errors.push("Profile '" + key + "': strategy '" + strategy + "' is a built-in service strategy and cannot be used by user profiles. " +
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
 * Validates a domain mapping. Checks hostname format, profile references, service/serviceTag strings, loginUrl format, and maxContinuousPlayback type and range.
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

  // Reject domains that collide with built-in domain mappings. User domains that shadow built-in domains cause the built-in service to disappear from the system,
  // affecting all channels on that domain. Users should set the profile field on individual channels to use a custom profile on a built-in domain instead.
  const conciseDomain = extractDomain("https://" + domain);
  const collidesWithBuiltin = (DOMAIN_CONFIG[domain] as DomainConfig | undefined) ?? (DOMAIN_CONFIG[conciseDomain] as DomainConfig | undefined);

  if(collidesWithBuiltin) {

    const builtinService = collidesWithBuiltin.service ?? conciseDomain;

    errors.push("Domain '" + domain + "' is already mapped to built-in service '" + builtinService +
      "'. Set the profile field on individual channels to use your custom profile instead.");
  }

  // dismissSelector must be a non-empty string if specified.
  if((config.dismissSelector !== undefined) && ((typeof config.dismissSelector !== "string") || (config.dismissSelector.trim() === ""))) {

    errors.push("Domain '" + domain + "': dismissSelector must be a non-empty string.");
  }

  // profile must reference an existing profile if specified.
  if(config.profile && !availableProfiles.has(config.profile)) {

    errors.push("Domain '" + domain + "': references non-existent profile '" + config.profile + "'.");
  }

  // service must be non-empty if specified.
  if((config.service !== undefined) && ((typeof config.service !== "string") || (config.service.trim() === ""))) {

    errors.push("Domain '" + domain + "': service must be a non-empty string.");
  }

  // serviceTag must be non-empty if specified.
  if((config.serviceTag !== undefined) && ((typeof config.serviceTag !== "string") || (config.serviceTag.trim() === ""))) {

    errors.push("Domain '" + domain + "': serviceTag must be a non-empty string.");
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

  // videoTimeout must be a positive integer if specified.
  if(config.videoTimeout !== undefined) {

    if((typeof config.videoTimeout !== "number") || !Number.isFinite(config.videoTimeout) || !Number.isInteger(config.videoTimeout) ||
      (config.videoTimeout <= 0)) {

      errors.push("Domain '" + domain + "': videoTimeout must be a positive integer.");
    }
  }

  return errors;
}

/**
 * Validates an entire import batch of profiles and domains. Returns the validated entries and any errors found. Used by both file import and service pack import.
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
