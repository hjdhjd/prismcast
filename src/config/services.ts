/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.ts: Service group management for multi-service channels.
 */
import type { Channel, ChannelMap, ChannelSortField, ServiceGroup, SortDirection } from "../types/index.js";
import { DOMAIN_CONFIG, getDomainConfig } from "./sites.js";
import { LOG, extractDomain } from "../utils/index.js";
import { CONFIG } from "./index.js";
import { PREDEFINED_CHANNELS } from "../channels/index.js";
import { getChannelEffectiveTags } from "./userChannels.js";
import { getProfileForChannel } from "./profiles.js";
import { getUserDomains } from "./userProfiles.js";
import { mutateConfig } from "./userConfig.js";

/* Service groups allow multiple streaming services to offer the same content. For example, ESPN can be watched via ESPN.com (native) or Disney+.
 *
 * All variant relationships - predefined and user-defined - are expressed via the canonicalKey field on Channel. The flattener sets canonicalKey on predefined
 * variant entries, the browse modal sets it on user variant entries, and the schema-version migration stamps it on entries that lack it. buildServiceGroups
 * scans all channels once and groups by canonicalKey. One field, one mechanism, one code path.
 *
 * User overrides: When a user defines a channel with the same key as a predefined channel, both versions appear in the service dropdown. The user's custom version
 * is shown first (labeled "Custom") and is the default. The original predefined version uses a special key suffix (PREDEFINED_SUFFIX) to distinguish it from the
 * user's version. This allows users to switch between their custom definition and the original at any time.
 *
 * User selections are stored in channels.json (in the data directory) under the `serviceSelections` key and persist across restarts.
 */

// Suffix appended to channel keys to reference the original predefined channel when a user has overridden it. For example, "espn:predefined" references the original
// predefined ESPN channel when the user has created a custom "espn" entry. Exported so other modules (e.g., channelForm) can detect synthetic entries via the
// canonical constant instead of stringly-typed substring checks.
export const PREDEFINED_SUFFIX = ":predefined";

/**
 * Strips the :predefined suffix from a channel key if present, returning the base key. Synthetic keys like "pbs:predefined" are created when a user overrides a
 * predefined channel - the original predefined entry gets this suffix to coexist with the user's custom version in the service dropdown. Functions that look up
 * channel data by key must strip the suffix to find the actual channel entry.
 * @param key - The channel key, possibly with :predefined suffix.
 * @returns The base key without the suffix.
 */
function stripPredefinedSuffix(key: string): string {

  return key.endsWith(PREDEFINED_SUFFIX) ? key.slice(0, -PREDEFINED_SUFFIX.length) : key;
}

// Module-level storage for service groups, keyed by canonical channel key.
const serviceGroups = new Map<string, ServiceGroup>();

// Reference to the channels map for inheritance resolution.
let channelsRef: ChannelMap = {};

// User's service selections, keyed by canonical channel key. Values are the selected service key (e.g., "espn-disneyplus").
let serviceSelections = new Map<string, string>();

// Service Tag System.

// Module-level state for the service filter. Empty array means "no filter" (all services shown). Non-empty means only these tags are active.
let enabledServices: string[] = [];

/**
 * Derives the service tag for a channel from its URL domain, falling back to "direct" if no service tag is configured. Checks the channel's explicit profile
 * first (for user-defined profiles with custom serviceTag), then the URL domain via getDomainConfig().
 * @param channel - The channel to derive a tag for.
 * @returns The service tag string.
 */
function resolveServiceTag(channel: Channel): string {

  // If the channel specifies a user-defined profile, use that profile's serviceTag rather than deriving from the URL. This ensures channels with explicit profile
  // assignments are grouped under the correct service filter even when their URL domain has a different built-in serviceTag.
  if(channel.profile) {

    const profileService = resolveUserProfileService(channel.profile);

    if(profileService?.serviceTag) {

      return profileService.serviceTag;
    }
  }

  const config = getDomainConfig(channel.url);

  return config?.serviceTag ?? "direct";
}

/**
 * Gets the service tag for a channel key. For channels in a service group, reads the pre-computed tag from the group variant entry (computed at group-building
 * time by buildServiceGroups). For standalone channels not in any group, derives the tag from the channel's URL domain. This function should not be called with
 * :predefined synthetic keys - those only exist inside service groups and their tags are available via the group's variant entries.
 * @param key - The channel key.
 * @returns The service tag string.
 */
export function getServiceTagForChannel(key: string): string {

  const effectiveKey = stripPredefinedSuffix(key);
  const group = serviceGroups.get(effectiveKey);

  // For channels in a group, read the pre-computed tag from the variant entry.
  if(group) {

    const variant = group.variants.find((v) => (v.key === key) || (v.key === effectiveKey));

    if(variant) {

      return variant.tag;
    }
  }

  // For standalone channels not in any group, derive from the channel's URL domain.
  const channel = channelsRef[effectiveKey] ?? PREDEFINED_CHANNELS[effectiveKey];


  if(!channel) {

    return "direct";
  }

  return resolveServiceTag(channel);
}

/**
 * Returns the auth domain for a channel key. Domain is the natural auth boundary - browser cookies and sessions scope to it. Multi-channel services work correctly
 * because all their channels share one domain, and canonical channels work correctly because each has its own domain.
 * @param key - The channel key.
 * @returns The extracted domain from the channel's URL, or empty string if the channel or URL cannot be resolved.
 */
export function getAuthDomainForChannel(key: string): string {

  const effectiveKey = stripPredefinedSuffix(key);
  const channel = channelsRef[effectiveKey] ?? PREDEFINED_CHANNELS[effectiveKey];


  if(!channel?.url) {

    return "";
  }

  return extractDomain(channel.url);
}

/**
 * Returns all service tags for a channel (canonical tag + all variant suffix tags). Used to determine which services offer this channel.
 * @param canonicalKey - The canonical channel key.
 * @returns Array of service tag strings.
 */
export function getChannelServiceTags(canonicalKey: string): string[] {

  const group = serviceGroups.get(canonicalKey);

  // For grouped channels, collect tags directly from the pre-computed variant entries.
  if(group) {

    const tags = new Set<string>();

    for(const variant of group.variants) {

      // Skip predefined suffix variants - they share the canonical's tag (but derived from the predefined URL, which may differ from the user override).
      if(variant.key.endsWith(PREDEFINED_SUFFIX)) {

        continue;
      }

      tags.add(variant.tag);
    }

    return [...tags];
  }

  // Standalone channel - derive tag from the channel directly.
  return [getServiceTagForChannel(canonicalKey)];
}

/**
 * Scans all service groups and collects unique service tags with display names. Display names are derived from the service field in DOMAIN_CONFIG entries that
 * have a serviceTag.
 * @returns Array of { displayName, domain, iconUrl, tag } objects sorted alphabetically by display name, with "direct" always first.
 */
export function getAllServiceTags(): { displayName: string; domain?: string; iconUrl?: string; tag: string }[] {

  const tags = new Set<string>();

  // Scan all channels (not just grouped ones) to find all service tags.
  const allKeys = new Set([ ...Object.keys(channelsRef), ...Object.keys(PREDEFINED_CHANNELS) ]);

  for(const key of allKeys) {

    // Skip variant keys - they are covered by getChannelServiceTags() on the canonical.
    const group = serviceGroups.get(key);

    if(group && (group.canonicalKey !== key)) {

      continue;
    }

    const channelTags = getChannelServiceTags(key);

    for(const tag of channelTags) {

      tags.add(tag);
    }
  }

  // Scan user domain mappings for service tags that may not appear in any channel yet (e.g., newly created profiles with no channels assigned).
  const userDomains = getUserDomains();

  for(const config of Object.values(userDomains)) {

    if(config.serviceTag) {

      tags.add(config.serviceTag);
    }
  }

  // Build tag metadata maps from DOMAIN_CONFIG entries. Collects display name, domain, and icon URL for each service tag. First match wins for each tag.
  const tagMeta = new Map<string, { displayName: string; domain?: string; iconUrl?: string }>();

  tagMeta.set("direct", { displayName: "Channel Website" });

  for(const [ domain, config ] of Object.entries(DOMAIN_CONFIG)) {

    if(config.serviceTag && config.service && !tagMeta.has(config.serviceTag)) {

      tagMeta.set(config.serviceTag, { displayName: config.service, domain, iconUrl: config.iconUrl });
    }
  }

  // Scan user domain mappings for metadata not covered by built-in DOMAIN_CONFIG.
  for(const [ domain, config ] of Object.entries(userDomains)) {

    if(config.serviceTag && config.service && !tagMeta.has(config.serviceTag)) {

      tagMeta.set(config.serviceTag, { displayName: config.service, domain, iconUrl: config.iconUrl });
    }
  }

  // Build result with metadata.
  const result: { displayName: string; domain?: string; iconUrl?: string; tag: string }[] = [];

  for(const tag of tags) {

    const meta = tagMeta.get(tag);

    result.push({ displayName: meta?.displayName ?? tag, domain: meta?.domain, iconUrl: meta?.iconUrl, tag });
  }

  // Sort alphabetically by display name, but keep "direct" first.
  result.sort((a, b) => {

    if(a.tag === "direct") {

      return -1;
    }

    if(b.tag === "direct") {

      return 1;
    }

    return a.displayName.localeCompare(b.displayName);
  });

  return result;
}

/**
 * Gets the current enabled service tags.
 * @returns Copy of the enabled services array. Empty means no filter (all shown).
 */
export function getEnabledServices(): string[] {

  return [...enabledServices];
}

/**
 * Sets the enabled service tags in memory (module state + runtime CONFIG). Does NOT persist to config.json - callers that want to persist the change must
 * follow up with saveEnabledServices(). This "set then save" split matches the codebase's convention for other mutable shared state (setServiceSelection /
 * saveServiceSelections, setTagRegistry / saveTagRegistry). Empty array means "no filter" (all services shown).
 * @param tags - The service tags to enable.
 */
export function setEnabledServices(tags: readonly string[]): void {

  enabledServices = [...tags];
  CONFIG.channels.enabledServices = [...tags];
}

/**
 * Persists the current enabledServices state to config.json. Reads module state (written by setEnabledServices) and writes it into the config file. Separate
 * from setEnabledServices so callers that load values from disk don't trigger a spurious write-back.
 */
export async function saveEnabledServices(): Promise<void> {

  await mutateConfig((config) => {

    config.channels ??= {};
    config.channels.enabledServices = [...enabledServices];
  });
}

/**
 * Checks if a service tag is currently enabled. Returns true if the tag is enabled, if no filter is active (empty set), or if the tag is "direct".
 * @param tag - The service tag to check.
 * @returns True if the service is available.
 */
export function isServiceTagEnabled(tag: string): boolean {

  // No filter active - all services are enabled.
  if(enabledServices.length === 0) {

    return true;
  }

  // "direct" is always enabled.
  if(tag === "direct") {

    return true;
  }

  return enabledServices.includes(tag);
}

/**
 * Centralized availability check for the service filter. Returns true if the channel has at least one variant whose service tag is enabled.
 * @param canonicalKey - The canonical channel key.
 * @returns True if the channel passes the service filter.
 */
export function isChannelAvailableByService(canonicalKey: string): boolean {

  // No filter active - all channels are available.
  if(enabledServices.length === 0) {

    return true;
  }

  const tags = getChannelServiceTags(canonicalKey);

  return tags.some((tag) => isServiceTagEnabled(tag));
}

/**
 * Checks if a channel in the merged map is a user override of a predefined channel. This uses object reference comparison - getAllChannels() spreads
 * PREDEFINED_CHANNELS directly into the result, so if the reference differs, a user channel has replaced the predefined one.
 * @param key - The channel key to check.
 * @param channels - The merged channel map.
 * @returns True if the channel is a user override of a predefined channel.
 */
function isUserOverride(key: string, channels: ChannelMap): boolean {

  const predefined = PREDEFINED_CHANNELS[key];

  // A channel is an override if: (1) a predefined version exists, and (2) the merged map has a different object reference.
  return Boolean(predefined) && (channels[key] !== predefined);
}

/**
 * Builds service groups by scanning all channels and grouping by canonicalKey. The flattener sets canonicalKey on predefined variants, the browse modal sets it
 * on user variants, and the schema-version migration stamps it on entries that lack it. One field, one mechanism, one pass.
 *
 * User overrides of predefined channels (same key, different object reference) produce a two-entry group with "Custom" and the original predefined version,
 * even for single-service channels that don't have canonicalKey-based variants.
 *
 * After grouping, every stored service selection is validated against the rebuilt variant structure. Selections that no longer correspond to a real variant are
 * reverted to the canonical default. This is the single resolution boundary for stale selections; read-side resolvers stay pure.
 * @param channels - The merged channel map (predefined + user channels).
 * @returns Canonical keys whose service selections were stale and reverted. Empty array if all selections are valid. The caller decides whether to persist.
 */
export function buildServiceGroups(channels: ChannelMap): string[] {

  channelsRef = channels;
  serviceGroups.clear();

  // Pass 1: Collect variant keys grouped by their canonical key. Entries without canonicalKey are canonicals or standalone channels.
  const variantsByCanonical = new Map<string, string[]>();

  for(const [ key, channel ] of Object.entries(channels)) {

    if(!channel.canonicalKey) {

      continue;
    }

    const existing = variantsByCanonical.get(channel.canonicalKey);

    if(existing) {

      existing.push(key);
    } else {

      variantsByCanonical.set(channel.canonicalKey, [key]);
    }
  }

  // Pass 2: Build groups from the collected variants.
  for(const [ canonicalKey, variantKeys ] of variantsByCanonical) {

    const canonical = channels[canonicalKey];


    if(!canonical) {

      continue;
    }

    const variants: ServiceGroup["variants"] = [];

    // Handle user override of the canonical entry. Two scenarios: (A) the user customized properties (station ID, tags, etc.) but the URL still matches a known
    // service domain - no "Custom" variant needed, just the normal service label with a visual override indicator in the table; (B) the user set a genuinely
    // non-standard URL - "Custom (domain)" is a real service variant and the :predefined entry gives access to the original service URL.
    // isUserOverride returned true means predefined exists for canonicalKey (it's defined as `Boolean(predefined) && (channels[key] !== predefined)`). Look it
    // up here and narrow away the undefined so the scenarios below can rely on the predefined reference.
    const predefined = isUserOverride(canonicalKey, channels) ? PREDEFINED_CHANNELS[canonicalKey] : undefined;

    if(predefined) {

      const userDomain = extractDomain(canonical.url);
      const knownDomains = new Set([extractDomain(predefined.url)]);

      for(const variantKey of variantKeys) {

        const variantChannel = channels[variantKey];

        if(variantChannel) {

          knownDomains.add(extractDomain(variantChannel.url));
        }
      }

      if(knownDomains.has(userDomain)) {

        // Scenario A: property override on a known service. The canonical gets the same label as if it weren't overridden. The modified-dot indicator in the
        // table renderer handles the visual distinction.
        variants.push({ key: canonicalKey, label: getChannelServiceLabel(canonical), tag: resolveServiceTag(canonical) });
      } else {

        // Scenario B: genuinely custom URL. "Custom (domain)" is a real service variant. The :predefined entry gives the user a path back to the original
        // predefined service URL without permanently reverting their other customizations.
        variants.push({ key: canonicalKey, label: "Custom (" + extractDomain(canonical.url) + ")", tag: resolveServiceTag(canonical) });
        variants.push({ key: canonicalKey + PREDEFINED_SUFFIX, label: predefined.service ?? getServiceDisplayName(predefined.url),
          tag: resolveServiceTag(predefined) });
      }
    } else {

      variants.push({ key: canonicalKey, label: getChannelServiceLabel(canonical), tag: resolveServiceTag(canonical) });
    }

    variantKeys.sort();

    for(const variantKey of variantKeys) {

      const variant = channels[variantKey];

      // Defensive: every variantKey in variantKeys came from Pass 1's scan of this same channels map, so the lookup succeeds. The narrowing keeps the types honest.
      if(variant) {

        variants.push({ key: variantKey, label: getChannelServiceLabel(variant), tag: resolveServiceTag(variant) });
      }
    }

    const group: ServiceGroup = { canonicalKey, variants };

    // Map canonical and all variant keys to this group for easy lookup.
    serviceGroups.set(canonicalKey, group);

    for(const variantKey of variantKeys) {

      serviceGroups.set(variantKey, group);
    }

    LOG.debug("config:general", "Service group '%s': variants=%s.", canonicalKey, variants.map((v) => v.key).join(", "));
  }

  // Pass 3: Create groups for user overrides of single-service predefined channels. Only Scenario B (genuinely custom URL) gets a service group - the user needs
  // a dropdown to switch between their custom URL and the predefined service. Scenario A (property override on the same domain) skips group creation entirely
  // because there is only one service and no dropdown is needed; the modified-dot indicator in the table renderer signals the override.
  for(const key of Object.keys(channels)) {

    if(serviceGroups.has(key)) {

      continue;
    }

    if(!isUserOverride(key, channels)) {

      continue;
    }

    const userChannel = channels[key];
    const predefined = PREDEFINED_CHANNELS[key];

    // Defensive: isUserOverride returning true means both predefined and the user entry exist. The narrowing here satisfies the type-checker and guards
    // against an impossible edge case.
    if(!userChannel || !predefined) {

      continue;
    }

    // Scenario A: URL domain matches the predefined service. No service group needed - renders as a single-service channel with a modified-dot indicator.
    if(extractDomain(userChannel.url) === extractDomain(predefined.url)) {

      continue;
    }

    // Scenario B: genuinely custom URL. Create a 2-entry group so the user can switch between their custom URL and the predefined service.
    const variants: ServiceGroup["variants"] = [
      { key, label: "Custom (" + extractDomain(userChannel.url) + ")", tag: resolveServiceTag(userChannel) },
      { key: key + PREDEFINED_SUFFIX, label: predefined.service ?? getServiceDisplayName(predefined.url), tag: resolveServiceTag(predefined) }
    ];

    const group: ServiceGroup = { canonicalKey: key, variants };

    serviceGroups.set(key, group);
    LOG.debug("config:general", "Service group '%s' (override): variants=%s.", key, variants.map((v) => v.key).join(", "));
  }

  // Build the domain-to-predefined-channel reverse index. This enables the manual add form to show an inline hint when the entered URL matches a predefined
  // channel. Scans only canonical entries in PREDEFINED_CHANNELS (variants share the canonical's URL domain and would produce duplicate results).
  predefinedByDomain.clear();

  for(const [ key, channel ] of Object.entries(PREDEFINED_CHANNELS)) {

    if(channel.canonicalKey) {

      continue;
    }

    const domain = extractDomain(channel.url);
    const group = serviceGroups.get(key);
    const entry = { canonicalKey: key, name: channel.name ?? key, serviceCount: group?.variants.length ?? 1 };
    const existing = predefinedByDomain.get(domain);

    if(existing) {

      existing.push(entry);
    } else {

      predefinedByDomain.set(domain, [entry]);
    }
  }

  // Validate stored service selections against the rebuilt groups. Any selection whose variant key is not present in the group is stale and reverted to the
  // canonical default. The caller decides whether to persist based on whether any keys were cleaned.
  const staleKeys: string[] = [];

  for(const [ canonicalKey, selection ] of serviceSelections) {

    const group = serviceGroups.get(canonicalKey);

    if(!group?.variants.some((v) => v.key === selection)) {

      LOG.warn("Service selection '%s' for channel '%s' is no longer valid. Reverting to default.", selection, canonicalKey);
      serviceSelections.delete(canonicalKey);
      staleKeys.push(canonicalKey);
    }
  }

  return staleKeys;
}

// Summary of a predefined channel for the domain-to-channel reverse index. Used by the inline hint in the manual add form and by the embedded client-side data.
interface PredefinedChannelSummary {

  canonicalKey: string;
  name: string;
  serviceCount: number;
}

// Reverse index mapping concise domains to predefined channel summaries. Built by buildServiceGroups() and queried by findPredefinedByDomain() and
// getPredefinedDomainMap().
const predefinedByDomain = new Map<string, PredefinedChannelSummary[]>();

/**
 * Returns predefined channels whose canonical URL domain matches the given URL's domain. Used by the manual add form to show an inline hint when the user
 * enters a URL that has predefined channels available. Returns an empty array when no predefined channels match.
 * @param url - The URL to match against predefined channel domains.
 * @returns Array of matching predefined channel summaries with canonical key, display name, and available service count.
 */
export function findPredefinedByDomain(url: string): PredefinedChannelSummary[] {

  try {

    const domain = extractDomain(url);

    return predefinedByDomain.get(domain) ?? [];
  } catch {

    return [];
  }
}

/**
 * Returns the full domain-to-predefined-channel index for client-side embedding. Used by the channels panel to embed predefined match data so the manual add
 * form can show inline hints without a server round-trip. The returned object is keyed by concise domain with arrays of channel summaries as values.
 * @returns Record mapping domains to predefined channel summaries.
 */
export function getPredefinedDomainMap(): Record<string, PredefinedChannelSummary[]> {

  return Object.fromEntries(predefinedByDomain);
}

/**
 * Resolves a URL to a friendly service display name. Checks built-in DOMAIN_CONFIG first for a stable, well-known service name, then falls back to
 * getDomainConfig() which includes user domain mappings. This ordering prevents user domain overrides from corrupting display labels for predefined channel
 * variants - a user mapping a built-in domain to a custom profile should not rename every service dropdown entry that uses that domain.
 * @param url - The URL to resolve a service display name for.
 * @returns The service display name, or the concise domain if no service name is configured.
 */
export function getServiceDisplayName(url: string): string {

  // Prefer built-in DOMAIN_CONFIG service names for stable display. Check by full hostname first (for subdomain-specific entries like tv.youtube.com), then by
  // concise domain (e.g., disneyplus.com).
  try {

    const hostname = new URL(url).hostname;
    const builtinFull = DOMAIN_CONFIG[hostname];


    if(builtinFull?.service) {

      return builtinFull.service;
    }

    const concise = extractDomain(url);
    const builtinConcise = DOMAIN_CONFIG[concise];


    if(builtinConcise?.service) {

      return builtinConcise.service;
    }
  } catch {

    // Invalid URL - fall through to getDomainConfig.
  }

  // For domains not in DOMAIN_CONFIG, fall back to getDomainConfig() which includes user domain mappings.
  const config = getDomainConfig(url);

  return config?.service ?? extractDomain(url);
}

/**
 * Resolves service identity (tag and display name) for a user-defined profile by scanning its domain mappings. Returns the first matching domain config's
 * serviceTag and service name. This is the single source of truth for "profile key -> service identity" resolution, used by both tag and label lookups to avoid
 * duplicating the domain scan logic.
 * @param profileKey - The user profile key to resolve.
 * @returns The service identity from the profile's domain mappings, or undefined if no matching domain mapping exists.
 */
function resolveUserProfileService(profileKey: string): { service?: string; serviceTag?: string } | undefined {

  const userDomains = getUserDomains();

  for(const config of Object.values(userDomains)) {

    if(config.profile === profileKey) {

      return { service: config.service, serviceTag: config.serviceTag };
    }
  }

  return undefined;
}

/**
 * Resolves the service display label for a channel. Checks in order: explicit `service` field on the channel, the channel's explicit profile resolved via
 * user domain mappings, then URL-based built-in display name. This ensures channels assigned to user-defined profiles show the profile's service name rather
 * than the built-in name for the URL domain.
 * @param channel - The channel to resolve a label for.
 * @returns The service display label.
 */
export function getChannelServiceLabel(channel: Channel): string {

  if(channel.service) {

    return channel.service;
  }

  // If the channel specifies a user-defined profile, use that profile's service name from domain mappings.
  if(channel.profile) {

    const profileService = resolveUserProfileService(channel.profile);

    if(profileService?.service) {

      return profileService.service;
    }
  }

  return getServiceDisplayName(channel.url);
}

// Valid sort field values for the channels table. Exported as the single source of truth for sort field validation, shared by the config POST handler and the
// playlist endpoint's query parameter validation.
export const VALID_SORT_FIELDS = new Set<ChannelSortField>(
  [ "channelNumber", "channelSelector", "hdhrEnabled", "key", "name", "profile", "service", "stationId", "tags" ]
);

/**
 * Extracts a sortable string value from a channel for the specified sort field. Channel numbers are zero-padded to 6 digits for correct numeric ordering within a
 * string comparison. Service values use the display label for human-meaningful sort order. This is the single source of truth for channel sort key extraction,
 * shared by both the server-side table renderer and the M3U playlist generator.
 * @param channel - Fallback channel definition, used only when the selected service variant cannot be resolved (e.g., key not in the merged channel map).
 * @param key - The canonical channel key. Used for key-based sorting and to resolve the selected service variant internally.
 * @param field - The sort field to extract.
 * @returns A lowercase string suitable for comparison-based sorting.
 */
export function getChannelSortKey(channel: Channel, key: string, field: ChannelSortField): string {

  // Resolve the selected service variant so all sort keys reflect the user's service selection. For URL-dependent fields (profile, service), this is essential -
  // a canonical's URL may differ from the selected variant's (e.g., bbcnews canonical uses cox but the user selected the directv variant). For identity fields
  // (name, stationId, channelNumber), the flattener eagerly sets these on all entries, so the resolved channel has identical values regardless of variant.
  const effective = getResolvedChannel(resolveServiceKey(key)) ?? channel;

  switch(field) {

    case "channelNumber": {

      const num = effective.channelNumber;

      return num ? String(num).padStart(6, "0") : "zzzzzz";
    }

    case "channelSelector": {

      return (effective.channelSelector ?? "").toLowerCase();
    }

    case "hdhrEnabled": {

      // Sort enabled channels before disabled. "0" (enabled/absent) sorts before "1" (disabled).
      return (effective.hdhrEnabled === false) ? "1" : "0";
    }

    case "key": {

      return key.toLowerCase();
    }

    case "name": {

      return (effective.name ?? key).toLowerCase();
    }

    case "profile": {

      // Explicit profile: sort by its name.
      if(effective.profile) {

        return effective.profile.toLowerCase();
      }

      // Auto-detected: check whether the profile resolves to a real service or falls back to default. Only apply the ! prefix for non-default auto profiles so
      // they sort between explicit profiles and empty profiles.
      const resolved = getProfileForChannel(effective);

      if(resolved.profileName === "default") {

        return "";
      }

      const label = getChannelServiceLabel(effective);

      return label ? ("!" + label.toLowerCase()) : "";
    }

    case "service": {

      return getChannelServiceLabel(effective).toLowerCase();
    }

    case "stationId": {

      const id = effective.stationId;

      return id ? id.padStart(6, "0") : "zzzzzz";
    }

    case "tags": {

      const effectiveTags = getChannelEffectiveTags(effective);

      return (effectiveTags.length > 0) ? effectiveTags.join(",") : "zz";
    }

    default: {

      return key.toLowerCase();
    }
  }
}

/**
 * Compares two channels for sorting by the specified field and direction with a built-in channel name tiebreaker. The tiebreaker is always ascending so that rows
 * within each group maintain a consistent alphabetical order regardless of the primary sort direction. This is the single comparator for all sort sites - server HTML
 * render, client re-sort, and M3U playlist - to prevent ordering divergence.
 * @param channelA - First channel definition.
 * @param keyA - First channel key.
 * @param channelB - Second channel definition.
 * @param keyB - Second channel key.
 * @param field - The sort field to compare.
 * @param direction - Sort direction for the primary field.
 * @returns A negative, zero, or positive number for sort ordering.
 */
export function compareChannelSort(
  channelA: Channel, keyA: string, channelB: Channel, keyB: string, field: ChannelSortField, direction: SortDirection
): number {

  const valA = getChannelSortKey(channelA, keyA, field);
  const valB = getChannelSortKey(channelB, keyB, field);
  const cmp = (direction === "asc") ? valA.localeCompare(valB) : valB.localeCompare(valA);

  if(cmp !== 0) {

    return cmp;
  }

  // Tiebreaker: channel name ascending regardless of primary direction.
  const nameA = (channelA.name ?? keyA).toLowerCase();
  const nameB = (channelB.name ?? keyB).toLowerCase();

  return nameA.localeCompare(nameB);
}

/**
 * Gets the service group for a channel key. Works with both canonical and variant keys.
 * @param key - Any channel key in the group.
 * @returns The service group if the channel is part of a multi-service group, undefined otherwise.
 */
export function getServiceGroup(key: string): ServiceGroup | undefined {

  return serviceGroups.get(key);
}

/**
 * Checks if a channel key is a non-canonical service variant. Used to filter variants from channel listings.
 * @param key - The channel key to check.
 * @returns True if the key is a variant (not canonical) in a service group.
 */
export function isServiceVariant(key: string): boolean {

  const group = serviceGroups.get(key);

  return (group !== undefined) && (group.canonicalKey !== key);
}

/**
 * Checks if a channel has multiple service options. Used to determine whether to show a service dropdown in the UI.
 * @param key - The channel key to check.
 * @returns True if the channel has more than one service variant.
 */
export function hasMultipleServices(key: string): boolean {

  const group = serviceGroups.get(key);

  return (group !== undefined) && (group.variants.length > 1);
}

/**
 * Gets the canonical key for any channel key. For variant keys, returns the canonical key. For non-grouped or canonical keys, returns the input unchanged.
 * Handles the PREDEFINED_SUFFIX used when a user has overridden a predefined channel.
 * @param key - Any channel key.
 * @returns The canonical key for the channel's service group, or the input key if not part of a group.
 */
export function getCanonicalKey(key: string): string {

  // Strip predefined suffix if present before looking up the group.
  const baseKey = key.endsWith(PREDEFINED_SUFFIX) ? key.slice(0, -PREDEFINED_SUFFIX.length) : key;
  const group = serviceGroups.get(baseKey);

  return group?.canonicalKey ?? baseKey;
}

/**
 * Sets the user's service selections. Called when loading from channels.json.
 * @param selections - Service selections keyed by canonical channel key.
 */
export function setServiceSelections(selections: Record<string, string>): void {

  serviceSelections = new Map(Object.entries(selections));
}

/**
 * Gets all service selections.
 * @returns Copy of the service selections object.
 */
export function getServiceSelections(): Record<string, string> {

  return Object.fromEntries(serviceSelections);
}

/**
 * Gets the service selection for a specific channel.
 * @param canonicalKey - The canonical channel key.
 * @returns The selected service key, or undefined if using the default.
 */
export function getServiceSelection(canonicalKey: string): string | undefined {

  return serviceSelections.get(canonicalKey);
}

/**
 * Sets the service selection for a channel.
 * @param canonicalKey - The canonical channel key.
 * @param serviceKey - The selected service key.
 */
export function setServiceSelection(canonicalKey: string, serviceKey: string): void {

  // If selecting the canonical (default), remove the selection instead of storing it.
  if(serviceKey === canonicalKey) {

    serviceSelections.delete(canonicalKey);
  } else {

    serviceSelections.set(canonicalKey, serviceKey);
  }
}

/**
 * Resolves a canonical channel key to the actual channel key based on user selection. If the user has selected a specific service for this channel, returns that
 * service's key. Otherwise returns the canonical key (default service). When the service filter is active, falls back to the first enabled variant if the stored
 * selection's service is filtered out.
 *
 * Pure resolver with no side effects. Stale selection cleanup belongs to buildServiceGroups(), which validates all stored selections against the rebuilt variant
 * structure on startup and after runtime channel mutations. Reads are safe to call from any context, including inside other mutations.
 * @param canonicalKey - The canonical channel key.
 * @returns The resolved service key to use for streaming.
 */
export function resolveServiceKey(canonicalKey: string): string {

  const selection = serviceSelections.get(canonicalKey);

  // No selection stored - use the canonical key (default service). If the canonical's service tag is filtered out, fall back to the first enabled variant.
  if(!selection) {

    if((enabledServices.length > 0) && !isServiceTagEnabled(getServiceTagForChannel(canonicalKey))) {

      return findFirstEnabledVariant(canonicalKey) ?? canonicalKey;
    }

    return canonicalKey;
  }

  // Valid selection - if its service tag is filtered out, fall back to the first enabled variant.
  if((enabledServices.length > 0) && !isServiceTagEnabled(getServiceTagForChannel(selection))) {

    return findFirstEnabledVariant(canonicalKey) ?? selection;
  }

  return selection;
}

/**
 * Finds the first enabled variant for a channel when the current selection's service is filtered out. Iterates the group's variants and returns the first whose
 * service tag is enabled.
 * @param canonicalKey - The canonical channel key.
 * @returns The first enabled variant key, or undefined if none are enabled.
 */
function findFirstEnabledVariant(canonicalKey: string): string | undefined {

  const group = serviceGroups.get(canonicalKey);

  if(!group) {

    return undefined;
  }

  for(const variant of group.variants) {

    if(variant.key.endsWith(PREDEFINED_SUFFIX)) {

      continue;
    }

    if(isServiceTagEnabled(variant.tag)) {

      return variant.key;
    }
  }

  return undefined;
}

/**
 * Gets a channel with inheritance applied. Variant inheritance is resolved at load time by resolveStoredChannel in userChannels.ts - entries in channelsRef
 * are already fully merged with their canonical (variant values win when set, canonical fills in the rest). This function is a thin accessor that also
 * handles the synthetic :predefined suffix used when a user overrides a canonical but the service dropdown references the original predefined variant.
 * @param key - The channel key (canonical, variant, or :predefined suffix).
 * @returns The complete channel, or undefined if the channel doesn't exist.
 */
export function getResolvedChannel(key: string): Channel | undefined {

  // Handle the :predefined suffix - return the original predefined channel when the user has overridden the canonical but selects the predefined service.
  if(key.endsWith(PREDEFINED_SUFFIX)) {

    return PREDEFINED_CHANNELS[key.slice(0, -PREDEFINED_SUFFIX.length)];
  }

  return channelsRef[key];
}

/**
 * Resolves a variant channel key against pure predefined data (ignoring user overrides). Used for revert detection: when an edit's values match a variant's
 * predefined definition, the custom override can be dropped and the service selection switched to that variant. Predefined variant entries carry only
 * service-specific fields and canonicalKey; identity inherits from the canonical. This resolver layers the variant's service fields onto the canonical so
 * findMatchingVariant can compare form values against a fully-populated variant view.
 * @param key - The channel key (canonical or variant).
 * @returns The resolved predefined channel, or undefined when the key has no predefined definition.
 */
export function resolvePredefinedVariant(key: string): Channel | undefined {

  const entry = PREDEFINED_CHANNELS[key];


  if(!entry) {

    return undefined;
  }

  // Canonical entries have full identity already; return as-is.
  if(!entry.canonicalKey || (entry.canonicalKey === key)) {

    return entry;
  }

  // Predefined variant: identity inherits from the canonical. Build a fresh Channel that merges canonical identity with the variant's service-specific fields.
  const canonical = PREDEFINED_CHANNELS[entry.canonicalKey];


  if(!canonical) {

    return entry;
  }

  // Start from the canonical (identity source), then overlay the variant's own fields. Since the variant is itself a plain Channel object (no nulls, no
  // deltas), a direct spread gives the correct result: variant fields win, canonical fills in the rest. Defensive copy of tags breaks shared array references.
  const resolved: Channel = { ...canonical, ...entry };

  resolved.tags &&= resolved.tags.slice();

  return resolved;
}
