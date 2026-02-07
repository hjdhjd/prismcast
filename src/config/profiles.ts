/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * profiles.ts: Site profile resolution and validation for PrismCast.
 */
import { DEFAULT_SITE_PROFILE, DOMAIN_CONFIG, SITE_PROFILES } from "./sites.js";
import type { ProfileCategory, ProfileResolutionResult, ResolvedSiteProfile, SiteProfile } from "../types/index.js";
import { CHANNELS } from "../channels/index.js";
import type { DomainConfig } from "./sites.js";
import { extractDomain } from "../utils/index.js";

// Re-export site data so existing consumers can import from either module.
export { DEFAULT_SITE_PROFILE, DOMAIN_CONFIG, SITE_PROFILES };
export type { DomainConfig };

/*
 * Profile resolution is the process of determining which behavior flags to use for a given stream. The resolution process handles inheritance, merging parent and
 * child profile properties, and falling back to defaults for unspecified flags.
 *
 * Resolution order (highest to lowest priority):
 *
 * 1. Channel's explicit profile property (if specified)
 * 2. URL-based detection via DOMAIN_CONFIG
 * 3. DEFAULT_SITE_PROFILE
 *
 * Within a profile, inheritance works as follows:
 *
 * 1. Start with DEFAULT_SITE_PROFILE for base values
 * 2. Apply parent profile properties (if extends is set)
 * 3. Apply current profile properties (overriding parent)
 *
 * This allows profiles like "embeddedDynamicMultiVideo" to inherit iframe handling from "embeddedPlayer" which inherits API fullscreen from "fullscreenApi",
 * building up the complete set of behavior flags through the inheritance chain.
 */

/**
 * Resolves a named profile from SITE_PROFILES, handling inheritance and merging with the default profile to ensure all required properties are present. Inheritance
 * is resolved recursively, with child profile properties overriding parent properties.
 *
 * The resolution process:
 *
 * 1. Start with a copy of DEFAULT_SITE_PROFILE
 * 2. If the profile extends another, recursively resolve the parent and merge its properties
 * 3. Merge the current profile's properties, overriding any inherited values
 * 4. Return the fully-resolved profile with all flags set
 *
 * Metadata properties (description, extends) are stripped during resolution - they exist only for documentation and inheritance specification.
 *
 * @param profileName - The name of the profile to resolve.
 * @returns The merged site profile containing all behavior flags.
 */
export function resolveProfile(profileName: string | undefined): ResolvedSiteProfile {

  // No profile specified - return the default.
  if(!profileName) {

    return { ...DEFAULT_SITE_PROFILE };
  }

  // The conditional below guards against runtime scenarios where profileName is not in SITE_PROFILES, even though TypeScript's Record type doesn't capture this.
  // This can happen if a channel references a typo'd profile name or if profiles are modified at runtime.
  const profile = SITE_PROFILES[profileName] as SiteProfile | undefined;

  if(!profile) {

    return { ...DEFAULT_SITE_PROFILE };
  }

  // Start with default values. This ensures all flags have a value even if not specified in the profile.
  let resolved: ResolvedSiteProfile = { ...DEFAULT_SITE_PROFILE };

  // Apply parent profile first if inheritance exists. This allows child profiles to override parent properties while inheriting the rest.
  if(profile.extends) {

    const parent = resolveProfile(profile.extends);

    resolved = { ...resolved, ...parent };
  }

  // Apply current profile properties, excluding metadata fields that should not be in the resolved profile. The category, description, extends, and summary
  // properties are for UI categorization, documentation, inheritance specification, and UI display only.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { category: _category, description: _description, extends: _extends, summary: _summary, ...profileFlags } = profile;

  resolved = { ...resolved, ...profileFlags };

  return resolved;
}

/**
 * Resolves the site profile for a given URL by extracting the concise domain and looking it up in DOMAIN_CONFIG. Uses the same extractDomain() function as
 * getProviderDisplayName() to ensure both profile resolution and display name resolution match on the same key. Falls back to the default profile if no matching
 * domain is found or the matching domain has no profile configured.
 * @param url - The URL to resolve a profile for.
 * @returns The site profile containing behavior flags.
 */
export function getProfileForUrl(url: string | undefined): ProfileResolutionResult {

  // No URL provided - return the default.
  if(!url) {

    return { profile: { ...DEFAULT_SITE_PROFILE }, profileName: "default" };
  }

  // Extract the concise domain (last two hostname parts) and look up the configuration.
  const config = DOMAIN_CONFIG[extractDomain(url)] as DomainConfig | undefined;

  if(config?.profile) {

    return { profile: resolveProfile(config.profile), profileName: config.profile };
  }

  // No matching domain found (or domain has no profile) - use the default profile.
  return { profile: { ...DEFAULT_SITE_PROFILE }, profileName: "default" };
}

/**
 * Resolves the site profile for a channel. Channels can explicitly declare their profile by name, which takes precedence over URL-based detection. This is useful
 * when:
 * - A channel's URL domain doesn't match the expected behavior pattern
 * - The same domain serves multiple channel types needing different handling
 * - A channel needs a custom combination of flags not covered by existing profiles
 *
 * The special value "auto" triggers URL-based domain detection, equivalent to omitting the profile property. This allows channels to explicitly opt into domain
 * detection rather than relying on the implicit behavior of an absent property.
 *
 * Channel-specific properties like channelSelector are merged into the resolved profile, allowing channels to extend profiles with additional configuration.
 *
 * @param channel - The channel object with url and optional profile properties.
 * @returns The site profile containing behavior flags.
 */
export function getProfileForChannel(channel: { channelSelector?: string; profile?: string; url?: string } | undefined): ProfileResolutionResult {

  // No channel provided - return the default.
  if(!channel) {

    return { profile: { ...DEFAULT_SITE_PROFILE }, profileName: "default" };
  }

  let profile: ResolvedSiteProfile;
  let profileName: string;

  // If the channel specifies an explicit profile name, use it directly. This takes precedence over URL-based detection. The value "auto" is treated as unset,
  // falling through to URL-based detection below.
  if(channel.profile && (channel.profile !== "auto")) {

    profile = resolveProfile(channel.profile);
    profileName = channel.profile;
  } else if(channel.url) {

    // Fall back to URL-based profile detection.
    const result = getProfileForUrl(channel.url);

    profile = result.profile;
    profileName = result.profileName;
  } else {

    profile = { ...DEFAULT_SITE_PROFILE };
    profileName = "default";
  }

  // Merge channel-specific properties into the profile. Currently only channelSelector is supported, which specifies a CSS selector for the channel button in
  // multi-channel player pages. The channelSelector on the channel overrides any channelSelector from the profile.
  if(channel.channelSelector) {

    profile = { ...profile, channelSelector: channel.channelSelector };
  }

  return { profile, profileName };
}

/*
 * Before starting the server, we validate all profile configurations to catch errors early. Invalid configurations would cause runtime failures that are difficult
 * to diagnose:
 *
 * - Invalid profile references (typos in extends or profile properties)
 * - Circular inheritance chains (A extends B extends A)
 * - Domain mappings pointing to non-existent profiles
 * - Channel definitions referencing non-existent profiles
 *
 * By validating upfront at startup, we provide clear error messages and prevent the server from starting in a misconfigured state. This is especially important
 * because profile errors might not surface until a specific channel is streamed, which could be hours after startup.
 */

/**
 * Validates all profile configurations including inheritance chains, domain mappings, and channel references. Throws an error if any validation fails. This
 * function runs at startup before the server begins accepting connections.
 *
 * Validation checks:
 *
 * 1. Circular inheritance detection - walks the extends chain for each profile to detect cycles
 * 2. Invalid extends references - ensures all extends targets exist
 * 3. Domain mapping validation - ensures all domain profile references exist
 * 4. Channel profile validation - ensures all channel profile references exist
 *
 * @throws If any profile configuration is invalid.
 */
export function validateProfiles(): void {

  const errors: string[] = [];

  // Check for circular inheritance and invalid extends references in profiles. We walk the extends chain for each profile, tracking visited profiles to detect
  // cycles.
  for(const profileName of Object.keys(SITE_PROFILES)) {

    const visited = new Set<string>();
    let current: string | undefined = profileName;

    while(current) {

      // If we've seen this profile before, we have a circular reference.
      if(visited.has(current)) {

        errors.push([ "Circular inheritance detected in profile: ", profileName ].join(""));

        break;
      }

      visited.add(current);

      // TypeScript's Record type doesn't capture that the key may not exist at runtime.
      const profile = SITE_PROFILES[current] as SiteProfile | undefined;

      current = profile?.extends;

      // Check if the extends target exists. This catches typos and references to deleted profiles.
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      if((current !== undefined) && !SITE_PROFILES[current]) {

        errors.push([ "Profile ", profileName, " extends non-existent profile: ", current ].join(""));

        break;
      }
    }
  }

  // Validate all domain-to-profile mappings reference existing profiles. This catches typos in DOMAIN_CONFIG profile entries.
  for(const [ domain, config ] of Object.entries(DOMAIN_CONFIG)) {

    if(config.profile) {

      const domainProfile = SITE_PROFILES[config.profile] as SiteProfile | undefined;

      if(!domainProfile) {

        errors.push([ "Domain ", domain, " references non-existent profile: ", config.profile ].join(""));
      }
    }
  }

  // Validate all channel profile references point to existing profiles. This catches typos in channel definitions.
  for(const [ channelName, channel ] of Object.entries(CHANNELS)) {

    const channelProfile = channel.profile;

    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if((channelProfile !== undefined) && (channelProfile !== "auto") && !SITE_PROFILES[channelProfile]) {

      errors.push([ "Channel ", channelName, " references non-existent profile: ", channelProfile ].join(""));
    }
  }

  // If any errors were found, throw with all error messages for comprehensive feedback.
  if(errors.length > 0) {

    throw new Error([ "Profile validation failed:\n  ", errors.join("\n  ") ].join(""));
  }
}

/*
 * Helper functions for listing available profiles in the UI.
 */

/**
 * Profile information for UI display, including name, description, category, and summary.
 */
export interface ProfileInfo {

  // UI category for grouping in dropdowns and reference documentation.
  category: ProfileCategory;

  // Human-readable description of the profile's purpose.
  description: string;

  // Profile name (the key in SITE_PROFILES).
  name: string;

  // Short summary for dropdown display (max ~40 chars).
  summary: string;
}

/**
 * Returns all profiles with their descriptions, categories, and summaries, sorted alphabetically by name. Used by the channel configuration UI to populate the
 * profile dropdown with tooltips and the profile reference section.
 * @returns Array of profile info objects.
 */
export function getProfiles(): ProfileInfo[] {

  return Object.keys(SITE_PROFILES).sort().map((name) => {

    const profile = SITE_PROFILES[name];

    return {

      category: profile.category ?? "special",
      description: profile.description ?? "",
      name,
      summary: profile.summary ?? profile.description ?? ""
    };
  });
}
