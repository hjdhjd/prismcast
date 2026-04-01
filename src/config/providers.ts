/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * providers.ts: Provider group management for multi-provider channels.
 */
import { CHANNEL_IDENTITY_FIELDS, PREDEFINED_CHANNELS } from "../channels/index.js";
import type { Channel, ChannelMap, ChannelSortField, ProviderGroup, SortDirection } from "../types/index.js";
import { DOMAIN_CONFIG, getDomainConfig } from "./sites.js";
import { LOG, extractDomain } from "../utils/index.js";
import { getChannelEffectiveTags } from "./userChannels.js";
import { getProfileForChannel } from "./profiles.js";
import { getUserDomains } from "./userProfiles.js";

/* Provider groups allow multiple streaming providers to offer the same content. For example, ESPN can be watched via ESPN.com (native) or Disney+.
 *
 * All variant relationships — predefined and user-defined — are expressed via the canonicalKey field on Channel. The flattener sets canonicalKey on predefined
 * variant entries, the browse modal sets it on user variant entries, and the one-time migration stamps it on pre-existing user entries. buildProviderGroups
 * scans all channels once and groups by canonicalKey. One field, one mechanism, one code path.
 *
 * User overrides: When a user defines a channel with the same key as a predefined channel, both versions appear in the provider dropdown. The user's custom version
 * is shown first (labeled "Custom") and is the default. The original predefined version uses a special key suffix (PREDEFINED_SUFFIX) to distinguish it from the
 * user's version. This allows users to switch between their custom definition and the original at any time.
 *
 * User selections are stored in channels.json (in the data directory) under the `providerSelections` key and persist across restarts.
 */

// Suffix appended to channel keys to reference the original predefined channel when a user has overridden it. For example, "espn:predefined" references the original
// predefined ESPN channel when the user has created a custom "espn" entry.
const PREDEFINED_SUFFIX = ":predefined";

/**
 * Strips the :predefined suffix from a channel key if present, returning the base key. Synthetic keys like "pbs:predefined" are created when a user overrides a
 * predefined channel — the original predefined entry gets this suffix to coexist with the user's custom version in the provider dropdown. Functions that look up
 * channel data by key must strip the suffix to find the actual channel entry.
 * @param key - The channel key, possibly with :predefined suffix.
 * @returns The base key without the suffix.
 */
function stripPredefinedSuffix(key: string): string {

  return key.endsWith(PREDEFINED_SUFFIX) ? key.slice(0, -PREDEFINED_SUFFIX.length) : key;
}

// Module-level storage for provider groups, keyed by canonical channel key.
const providerGroups = new Map<string, ProviderGroup>();

// Reference to the channels map for inheritance resolution.
let channelsRef: ChannelMap = {};

// User's provider selections, keyed by canonical channel key. Values are the selected provider key (e.g., "espn-disneyplus").
let providerSelections = new Map<string, string>();

// Provider Tag System.

// Module-level state for the provider filter. Empty array means "no filter" (all providers shown). Non-empty means only these tags are active.
let enabledProviders: string[] = [];

/**
 * Derives the provider tag for a channel from its URL domain, falling back to "direct" if no provider tag is configured. Checks the channel's explicit profile
 * first (for user-defined profiles with custom providerTag), then the URL domain via getDomainConfig().
 * @param channel - The channel to derive a tag for.
 * @returns The provider tag string.
 */
function resolveProviderTag(channel: Channel): string {

  // If the channel specifies a user-defined profile, use that profile's providerTag rather than deriving from the URL. This ensures channels with explicit profile
  // assignments are grouped under the correct provider filter even when their URL domain has a different built-in providerTag.
  if(channel.profile) {

    const profileProvider = resolveUserProfileProvider(channel.profile);

    if(profileProvider?.providerTag) {

      return profileProvider.providerTag;
    }
  }

  const config = getDomainConfig(channel.url);

  return config?.providerTag ?? "direct";
}

/**
 * Gets the provider tag for a channel key. For channels in a provider group, reads the pre-computed tag from the group variant entry (computed at group-building
 * time by buildProviderGroups). For standalone channels not in any group, derives the tag from the channel's URL domain. This function should not be called with
 * :predefined synthetic keys — those only exist inside provider groups and their tags are available via the group's variant entries.
 * @param key - The channel key.
 * @returns The provider tag string.
 */
export function getProviderTagForChannel(key: string): string {

  const effectiveKey = stripPredefinedSuffix(key);
  const group = providerGroups.get(effectiveKey);

  // For channels in a group, read the pre-computed tag from the variant entry.
  if(group) {

    const variant = group.variants.find((v) => (v.key === key) || (v.key === effectiveKey));

    if(variant) {

      return variant.tag;
    }
  }

  // For standalone channels not in any group, derive from the channel's URL domain.
  const channel = channelsRef[effectiveKey] ?? PREDEFINED_CHANNELS[effectiveKey];

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!channel) {

    return "direct";
  }

  return resolveProviderTag(channel);
}

/**
 * Returns the auth domain for a channel key. Domain is the natural auth boundary — browser cookies and sessions scope to it. Multi-channel providers work correctly
 * because all their channels share one domain, and canonical channels work correctly because each has its own domain.
 * @param key - The channel key.
 * @returns The extracted domain from the channel's URL, or empty string if the channel or URL cannot be resolved.
 */
export function getAuthDomainForChannel(key: string): string {

  const effectiveKey = stripPredefinedSuffix(key);
  const channel = channelsRef[effectiveKey] ?? PREDEFINED_CHANNELS[effectiveKey];

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!channel?.url) {

    return "";
  }

  return extractDomain(channel.url);
}

/**
 * Returns all provider tags for a channel (canonical tag + all variant suffix tags). Used to determine which providers offer this channel.
 * @param canonicalKey - The canonical channel key.
 * @returns Array of provider tag strings.
 */
export function getChannelProviderTags(canonicalKey: string): string[] {

  const group = providerGroups.get(canonicalKey);

  // For grouped channels, collect tags directly from the pre-computed variant entries.
  if(group) {

    const tags = new Set<string>();

    for(const variant of group.variants) {

      // Skip predefined suffix variants — they share the canonical's tag (but derived from the predefined URL, which may differ from the user override).
      if(variant.key.endsWith(PREDEFINED_SUFFIX)) {

        continue;
      }

      tags.add(variant.tag);
    }

    return [...tags];
  }

  // Standalone channel — derive tag from the channel directly.
  return [getProviderTagForChannel(canonicalKey)];
}

/**
 * Scans all provider groups and collects unique provider tags with display names. Display names are derived from the provider field in DOMAIN_CONFIG entries that
 * have a providerTag.
 * @returns Array of { displayName, domain, iconUrl, tag } objects sorted alphabetically by display name, with "direct" always first.
 */
export function getAllProviderTags(): { displayName: string; domain?: string; iconUrl?: string; tag: string }[] {

  const tags = new Set<string>();

  // Scan all channels (not just grouped ones) to find all provider tags.
  const allKeys = new Set([ ...Object.keys(channelsRef), ...Object.keys(PREDEFINED_CHANNELS) ]);

  for(const key of allKeys) {

    // Skip variant keys — they are covered by getChannelProviderTags() on the canonical.
    const group = providerGroups.get(key);

    if(group && (group.canonicalKey !== key)) {

      continue;
    }

    const channelTags = getChannelProviderTags(key);

    for(const tag of channelTags) {

      tags.add(tag);
    }
  }

  // Scan user domain mappings for provider tags that may not appear in any channel yet (e.g., newly created profiles with no channels assigned).
  const userDomains = getUserDomains();

  for(const config of Object.values(userDomains)) {

    if(config.providerTag) {

      tags.add(config.providerTag);
    }
  }

  // Build tag metadata maps from DOMAIN_CONFIG entries. Collects display name, domain, and icon URL for each provider tag. First match wins for each tag.
  const tagMeta = new Map<string, { displayName: string; domain?: string; iconUrl?: string }>();

  tagMeta.set("direct", { displayName: "Channel Website" });

  for(const [ domain, config ] of Object.entries(DOMAIN_CONFIG)) {

    if(config.providerTag && config.provider && !tagMeta.has(config.providerTag)) {

      tagMeta.set(config.providerTag, { displayName: config.provider, domain, iconUrl: config.iconUrl });
    }
  }

  // Scan user domain mappings for metadata not covered by built-in DOMAIN_CONFIG.
  for(const [ domain, config ] of Object.entries(userDomains)) {

    if(config.providerTag && config.provider && !tagMeta.has(config.providerTag)) {

      tagMeta.set(config.providerTag, { displayName: config.provider, domain, iconUrl: config.iconUrl });
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
 * Gets the current enabled provider tags.
 * @returns Copy of the enabled providers array. Empty means no filter (all shown).
 */
export function getEnabledProviders(): string[] {

  return [...enabledProviders];
}

/**
 * Sets the enabled provider tags. Empty array means "no filter" (all providers shown).
 * @param tags - The provider tags to enable.
 */
export function setEnabledProviders(tags: string[]): void {

  enabledProviders = [...tags];
}

/**
 * Checks if a provider tag is currently enabled. Returns true if the tag is enabled, if no filter is active (empty set), or if the tag is "direct".
 * @param tag - The provider tag to check.
 * @returns True if the provider is available.
 */
export function isProviderTagEnabled(tag: string): boolean {

  // No filter active — all providers are enabled.
  if(enabledProviders.length === 0) {

    return true;
  }

  // "direct" is always enabled.
  if(tag === "direct") {

    return true;
  }

  return enabledProviders.includes(tag);
}

/**
 * Centralized availability check for the provider filter. Returns true if the channel has at least one variant whose provider tag is enabled.
 * @param canonicalKey - The canonical channel key.
 * @returns True if the channel passes the provider filter.
 */
export function isChannelAvailableByProvider(canonicalKey: string): boolean {

  // No filter active — all channels are available.
  if(enabledProviders.length === 0) {

    return true;
  }

  const tags = getChannelProviderTags(canonicalKey);

  return tags.some((tag) => isProviderTagEnabled(tag));
}

/**
 * Checks if a channel in the merged map is a user override of a predefined channel. This uses object reference comparison — getAllChannels() spreads
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
 * Builds provider groups by scanning all channels and grouping by canonicalKey. Variant entries declare their canonical via the canonicalKey field (set by the
 * flattener for predefined channels, by the browse modal for user channels, and by the one-time migration for pre-existing user entries). This is a single
 * pass over the merged channel map — one field, one mechanism for both predefined and user-defined variant relationships.
 *
 * User overrides of predefined channels (same key, different object reference) produce a two-entry group with "Custom" and the original predefined version,
 * even for single-provider channels that don't have canonicalKey-based variants.
 * @param channels - The merged channel map (predefined + user channels).
 */
export function buildProviderGroups(channels: ChannelMap): void {

  channelsRef = channels;
  providerGroups.clear();

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

    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(!canonical) {

      continue;
    }

    const variants: ProviderGroup["variants"] = [];

    // Handle user override of the canonical entry. The :predefined variant's tag is derived from the predefined channel (not the user override) so that provider
    // filtering correctly reflects the predefined channel's provider, not the user's custom URL.
    if(isUserOverride(canonicalKey, channels)) {

      const predefined = PREDEFINED_CHANNELS[canonicalKey];

      variants.push({ key: canonicalKey, label: "Custom (" + extractDomain(canonical.url) + ")", tag: resolveProviderTag(canonical) });
      variants.push({ key: canonicalKey + PREDEFINED_SUFFIX, label: predefined.provider ?? getProviderDisplayName(predefined.url),
        tag: resolveProviderTag(predefined) });
    } else {

      variants.push({ key: canonicalKey, label: getChannelProviderLabel(canonical), tag: resolveProviderTag(canonical) });
    }

    variantKeys.sort();

    for(const variantKey of variantKeys) {

      const variant = channels[variantKey];

      variants.push({ key: variantKey, label: getChannelProviderLabel(variant), tag: resolveProviderTag(variant) });
    }

    const group: ProviderGroup = { canonicalKey, variants };

    // Map canonical and all variant keys to this group for easy lookup.
    providerGroups.set(canonicalKey, group);

    for(const variantKey of variantKeys) {

      providerGroups.set(variantKey, group);
    }

    LOG.debug("config:general", "Provider group '%s': variants=%s.", canonicalKey, variants.map((v) => v.key).join(", "));
  }

  // Pass 3: Create groups for user overrides of single-provider predefined channels. These don't have canonicalKey-based variants but the user's custom version
  // should be toggleable against the predefined original.
  for(const key of Object.keys(channels)) {

    if(providerGroups.has(key)) {

      continue;
    }

    if(!isUserOverride(key, channels)) {

      continue;
    }

    const userChannel = channels[key];
    const predefined = PREDEFINED_CHANNELS[key];
    const variants: ProviderGroup["variants"] = [
      { key, label: "Custom (" + extractDomain(userChannel.url) + ")", tag: resolveProviderTag(userChannel) },
      { key: key + PREDEFINED_SUFFIX, label: predefined.provider ?? getProviderDisplayName(predefined.url), tag: resolveProviderTag(predefined) }
    ];

    const group: ProviderGroup = { canonicalKey: key, variants };

    providerGroups.set(key, group);
    LOG.debug("config:general", "Provider group '%s' (override): variants=%s.", key, variants.map((v) => v.key).join(", "));
  }
}

/**
 * Resolves a URL to a friendly provider display name. Checks built-in DOMAIN_CONFIG first for a stable, well-known provider name, then falls back to
 * getDomainConfig() which includes user domain mappings. This ordering prevents user domain overrides from corrupting display labels for predefined channel
 * variants — a user mapping a built-in domain to a custom profile should not rename every provider dropdown entry that uses that domain.
 * @param url - The URL to resolve a provider display name for.
 * @returns The provider display name, or the concise domain if no provider name is configured.
 */
export function getProviderDisplayName(url: string): string {

  // Prefer built-in DOMAIN_CONFIG provider names for stable display. Check by full hostname first (for subdomain-specific entries like tv.youtube.com), then by
  // concise domain (e.g., disneyplus.com).
  try {

    const hostname = new URL(url).hostname;
    const builtinFull = DOMAIN_CONFIG[hostname];

    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(builtinFull?.provider) {

      return builtinFull.provider;
    }

    const concise = extractDomain(url);
    const builtinConcise = DOMAIN_CONFIG[concise];

    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(builtinConcise?.provider) {

      return builtinConcise.provider;
    }
  } catch {

    // Invalid URL — fall through to getDomainConfig.
  }

  // For domains not in DOMAIN_CONFIG, fall back to getDomainConfig() which includes user domain mappings.
  const config = getDomainConfig(url);

  return config?.provider ?? extractDomain(url);
}

/**
 * Resolves provider identity (tag and display name) for a user-defined profile by scanning its domain mappings. Returns the first matching domain config's
 * providerTag and provider name. This is the single source of truth for "profile key → provider identity" resolution, used by both tag and label lookups to avoid
 * duplicating the domain scan logic.
 * @param profileKey - The user profile key to resolve.
 * @returns The provider identity from the profile's domain mappings, or undefined if no matching domain mapping exists.
 */
function resolveUserProfileProvider(profileKey: string): { provider?: string; providerTag?: string } | undefined {

  const userDomains = getUserDomains();

  for(const config of Object.values(userDomains)) {

    if(config.profile === profileKey) {

      return { provider: config.provider, providerTag: config.providerTag };
    }
  }

  return undefined;
}

/**
 * Resolves the provider display label for a channel. Checks in order: explicit `provider` field on the channel, the channel's explicit profile resolved via
 * user domain mappings, then URL-based built-in display name. This ensures channels assigned to user-defined profiles show the profile's provider name rather
 * than the built-in name for the URL domain.
 * @param channel - The channel to resolve a label for.
 * @returns The provider display label.
 */
export function getChannelProviderLabel(channel: Channel): string {

  if(channel.provider) {

    return channel.provider;
  }

  // If the channel specifies a user-defined profile, use that profile's provider name from domain mappings.
  if(channel.profile) {

    const profileProvider = resolveUserProfileProvider(channel.profile);

    if(profileProvider?.provider) {

      return profileProvider.provider;
    }
  }

  return getProviderDisplayName(channel.url);
}

// Valid sort field values for the channels table. Exported as the single source of truth for sort field validation, shared by the config POST handler and the
// playlist endpoint's query parameter validation.
export const VALID_SORT_FIELDS = new Set<ChannelSortField>(
  [ "channelNumber", "channelSelector", "hdhrEnabled", "key", "name", "profile", "provider", "stationId", "tags" ]
);

/**
 * Extracts a sortable string value from a channel for the specified sort field. Channel numbers are zero-padded to 6 digits for correct numeric ordering within a
 * string comparison. Provider values use the display label for human-meaningful sort order. This is the single source of truth for channel sort key extraction,
 * shared by both the server-side table renderer and the M3U playlist generator.
 * @param channel - Fallback channel definition, used only when the selected provider variant cannot be resolved (e.g., key not in the merged channel map).
 * @param key - The canonical channel key. Used for key-based sorting and to resolve the selected provider variant internally.
 * @param field - The sort field to extract.
 * @returns A lowercase string suitable for comparison-based sorting.
 */
export function getChannelSortKey(channel: Channel, key: string, field: ChannelSortField): string {

  // Resolve the selected provider variant so all sort keys reflect the user's provider selection. For URL-dependent fields (profile, provider), this is essential —
  // a canonical's URL may differ from the selected variant's (e.g., bbcnews canonical uses cox but the user selected the directv variant). For identity fields
  // (name, stationId, channelNumber), the flattener eagerly sets these on all entries, so the resolved channel has identical values regardless of variant.
  const effective = getResolvedChannel(resolveProviderKey(key)) ?? channel;

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

      // Auto-detected: check whether the profile resolves to a real provider or falls back to default. Only apply the ! prefix for non-default auto profiles so
      // they sort between explicit profiles and empty profiles.
      const resolved = getProfileForChannel(effective);

      if(resolved.profileName === "default") {

        return "";
      }

      const label = getChannelProviderLabel(effective);

      return label ? ("!" + label.toLowerCase()) : "";
    }

    case "provider": {

      return getChannelProviderLabel(effective).toLowerCase();
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
 * within each group maintain a consistent alphabetical order regardless of the primary sort direction. This is the single comparator for all sort sites — server HTML
 * render, client re-sort, and M3U playlist — to prevent ordering divergence.
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
 * Gets the provider group for a channel key. Works with both canonical and variant keys.
 * @param key - Any channel key in the group.
 * @returns The provider group if the channel is part of a multi-provider group, undefined otherwise.
 */
export function getProviderGroup(key: string): ProviderGroup | undefined {

  return providerGroups.get(key);
}

/**
 * Checks if a channel key is a non-canonical provider variant. Used to filter variants from channel listings.
 * @param key - The channel key to check.
 * @returns True if the key is a variant (not canonical) in a provider group.
 */
export function isProviderVariant(key: string): boolean {

  const group = providerGroups.get(key);

  return (group !== undefined) && (group.canonicalKey !== key);
}

/**
 * Checks if a channel has multiple provider options. Used to determine whether to show a provider dropdown in the UI.
 * @param key - The channel key to check.
 * @returns True if the channel has more than one provider variant.
 */
export function hasMultipleProviders(key: string): boolean {

  const group = providerGroups.get(key);

  return (group !== undefined) && (group.variants.length > 1);
}

/**
 * Gets the canonical key for any channel key. For variant keys, returns the canonical key. For non-grouped or canonical keys, returns the input unchanged.
 * Handles the PREDEFINED_SUFFIX used when a user has overridden a predefined channel.
 * @param key - Any channel key.
 * @returns The canonical key for the channel's provider group, or the input key if not part of a group.
 */
export function getCanonicalKey(key: string): string {

  // Strip predefined suffix if present before looking up the group.
  const baseKey = key.endsWith(PREDEFINED_SUFFIX) ? key.slice(0, -PREDEFINED_SUFFIX.length) : key;
  const group = providerGroups.get(baseKey);

  return group?.canonicalKey ?? baseKey;
}

/**
 * Sets the user's provider selections. Called when loading from channels.json.
 * @param selections - Provider selections keyed by canonical channel key.
 */
export function setProviderSelections(selections: Record<string, string>): void {

  providerSelections = new Map(Object.entries(selections));
}

/**
 * Gets all provider selections.
 * @returns Copy of the provider selections object.
 */
export function getProviderSelections(): Record<string, string> {

  return Object.fromEntries(providerSelections);
}

/**
 * Gets the provider selection for a specific channel.
 * @param canonicalKey - The canonical channel key.
 * @returns The selected provider key, or undefined if using the default.
 */
export function getProviderSelection(canonicalKey: string): string | undefined {

  return providerSelections.get(canonicalKey);
}

/**
 * Sets the provider selection for a channel.
 * @param canonicalKey - The canonical channel key.
 * @param providerKey - The selected provider key.
 */
export function setProviderSelection(canonicalKey: string, providerKey: string): void {

  // If selecting the canonical (default), remove the selection instead of storing it.
  if(providerKey === canonicalKey) {

    providerSelections.delete(canonicalKey);
  } else {

    providerSelections.set(canonicalKey, providerKey);
  }
}

/**
 * Resolves a canonical channel key to the actual channel key based on user selection. If the user has selected a specific provider for this channel, returns that
 * provider's key. Otherwise returns the canonical key (default provider). When the provider filter is active, falls back to the first enabled variant if the stored
 * selection's provider is filtered out.
 * @param canonicalKey - The canonical channel key.
 * @returns The resolved provider key to use for streaming.
 */
export function resolveProviderKey(canonicalKey: string): string {

  const selection = providerSelections.get(canonicalKey);

  // No selection stored — use the canonical key (default provider).
  if(!selection) {

    // If the canonical's provider tag is filtered out, find the first enabled variant.
    if((enabledProviders.length > 0) && !isProviderTagEnabled(getProviderTagForChannel(canonicalKey))) {

      return findFirstEnabledVariant(canonicalKey) ?? canonicalKey;
    }

    return canonicalKey;
  }

  // Handle :predefined suffix — validate that the base key exists in PREDEFINED_CHANNELS.
  if(selection.endsWith(PREDEFINED_SUFFIX)) {

    const baseKey = selection.slice(0, -PREDEFINED_SUFFIX.length);

    // Runtime check needed — TypeScript thinks Record indexing always returns a value, but the key may not exist.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(PREDEFINED_CHANNELS[baseKey]) {

      return selection;
    }

    // Predefined channel was removed. Fall through to the invalid selection warning.

    // Runtime check needed — TypeScript thinks Record indexing always returns a value, but the key may not exist.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  } else if(channelsRef[selection]) {

    // Normal selection — validate it exists in the merged channels. If its provider tag is filtered out, find the first enabled variant instead.
    if((enabledProviders.length > 0) && !isProviderTagEnabled(getProviderTagForChannel(selection))) {

      return findFirstEnabledVariant(canonicalKey) ?? selection;
    }

    return selection;
  }

  // Selection is invalid (provider removed). Clear it and log a warning.
  LOG.warn("Provider selection '%s' for channel '%s' no longer exists. Using default.", selection, canonicalKey);

  providerSelections.delete(canonicalKey);

  return canonicalKey;
}

/**
 * Finds the first enabled variant for a channel when the current selection's provider is filtered out. Iterates the group's variants and returns the first whose
 * provider tag is enabled.
 * @param canonicalKey - The canonical channel key.
 * @returns The first enabled variant key, or undefined if none are enabled.
 */
function findFirstEnabledVariant(canonicalKey: string): string | undefined {

  const group = providerGroups.get(canonicalKey);

  if(!group) {

    return undefined;
  }

  for(const variant of group.variants) {

    if(variant.key.endsWith(PREDEFINED_SUFFIX)) {

      continue;
    }

    if(isProviderTagEnabled(variant.tag)) {

      return variant.key;
    }
  }

  return undefined;
}

/**
 * Applies variant inheritance: the variant contributes provider-specific fields (url, channelSelector, profile, etc.) while identity fields always come from the
 * canonical base. Identity fields describe what the channel IS — name, station ID, tags, channel number — and are independent of which provider serves it. The
 * spread brings in all variant fields, then identity fields are unconditionally overwritten from the canonical. This ensures user overrides on the canonical
 * (e.g., renaming a channel or adding tags) propagate to all provider variants, rather than being masked by stale flattener-copied values on the variant.
 * The field list comes from CHANNEL_IDENTITY_FIELDS — the single source of truth for the identity/provider-specific separation.
 * @param variant - The variant channel definition (provider-specific fields).
 * @param base - The canonical (base) channel with user overrides applied (identity fields).
 * @returns A new Channel with identity fields from the canonical and provider-specific fields from the variant.
 */
function applyVariantInheritance(variant: Channel, base: Channel): Channel {

  const result: Channel = { ...variant };

  for(const field of CHANNEL_IDENTITY_FIELDS) {

    copyField(result, base, field);
  }

  return result;
}

// Type-safe field copy using a generic to preserve the field-value relationship. TypeScript can't prove that result[field] and base[field] have compatible types
// when field is a union of literal keys (each key maps to a different type). The generic K narrows to a single key per call, making the assignment type-safe.
// eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters -- K is necessary to correlate the key type between target and source assignments.
function copyField<K extends keyof Channel>(target: Channel, source: Channel, key: K): void {

  target[key] = source[key];
}

/**
 * Gets a channel with inheritance applied. For provider variants, this merges the variant's properties with inherited properties from the canonical entry
 * using the live channel data (which includes user overrides). Use `resolvePredefinedVariant()` when you need resolution against pure predefined data.
 * @param key - The channel key (canonical or variant).
 * @returns The complete channel with inheritance applied, or undefined if the channel doesn't exist.
 */
export function getResolvedChannel(key: string): Channel | undefined {

  // Handle predefined suffix — return the original predefined channel when user has overridden the canonical but selects the predefined provider.
  if(key.endsWith(PREDEFINED_SUFFIX)) {

    const baseKey = key.slice(0, -PREDEFINED_SUFFIX.length);

    return PREDEFINED_CHANNELS[baseKey];
  }

  const channel = channelsRef[key];

  // Runtime check needed even though TypeScript thinks channel is always defined (Record indexing quirk).
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!channel) {

    return undefined;
  }

  const group = providerGroups.get(key);

  // If not part of a group or is the canonical entry, return as-is.
  if(!group || (group.canonicalKey === key)) {

    return channel;
  }

  // This is a variant — merge with canonical entry.
  const canonical = channelsRef[group.canonicalKey];

  // Runtime check — canonical entry should exist if the group exists, but we check defensively.
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!canonical) {

    // Canonical entry missing (shouldn't happen), return variant as-is.
    return channel;
  }

  return applyVariantInheritance(channel, canonical);
}

/**
 * Resolves a variant channel key against pure predefined data (ignoring user overrides). This is used for revert detection — when the user's edits match a
 * variant's predefined definition, the custom override can be removed and the provider selection switched to that variant. For canonical keys, returns the raw
 * predefined channel. For variant keys, applies the same inheritance rules as `getResolvedChannel()` but against `PREDEFINED_CHANNELS` instead of `channelsRef`.
 * @param key - The channel key (canonical or variant).
 * @returns The channel with inheritance applied against predefined data, or undefined if the key has no predefined definition.
 */
export function resolvePredefinedVariant(key: string): Channel | undefined {

  const channel = PREDEFINED_CHANNELS[key];

  // Runtime check — the key may not exist in PREDEFINED_CHANNELS.
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!channel) {

    return undefined;
  }

  const group = providerGroups.get(key);

  // If not part of a group or is the canonical entry, return the predefined channel as-is.
  if(!group || (group.canonicalKey === key)) {

    return channel;
  }

  const canonical = PREDEFINED_CHANNELS[group.canonicalKey];

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!canonical) {

    return channel;
  }

  return applyVariantInheritance(channel, canonical);
}
