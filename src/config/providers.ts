/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * providers.ts: Provider group management for multi-provider channels.
 */
import type { Channel, ChannelMap, ProviderGroup } from "../types/index.js";
import { LOG, extractDomain } from "../utils/index.js";
import { DOMAIN_CONFIG } from "./sites.js";
import { PREDEFINED_CHANNELS } from "../channels/index.js";

/* Provider groups allow multiple streaming providers to offer the same content. For example, ESPN can be watched via ESPN.com (native) or Disney+.
 *
 * Grouping convention: Channels are grouped by key pattern. A key like "espn-disneyplus" is a variant of "espn" because it starts with "espn-" and "espn" exists as a
 * channel. The canonical key (the base key without suffix) is the default provider.
 *
 * IMPORTANT: When adding channels, avoid hyphenated keys that would unintentionally match an existing channel. For example, if "cnn" exists, don't add
 * "cnn-international" as a separate channel — it would become a CNN variant. Use a non-hyphenated key like "cnni" instead.
 *
 * Inheritance: Provider variants inherit `name` and `stationId` from the canonical entry, so variants only need to specify what differs (url, profile, provider).
 *
 * User overrides: When a user defines a channel with the same key as a predefined channel, both versions appear in the provider dropdown. The user's custom version
 * is shown first (labeled "Custom") and is the default. The original predefined version uses a special key suffix (PREDEFINED_SUFFIX) to distinguish it from the
 * user's version. This allows users to switch between their custom definition and the original at any time.
 *
 * User selections are stored in ~/.prismcast/channels.json under the `providerSelections` key and persist across restarts.
 */

// Suffix appended to channel keys to reference the original predefined channel when a user has overridden it. For example, "espn:predefined" references the original
// predefined ESPN channel when the user has created a custom "espn" entry.
const PREDEFINED_SUFFIX = ":predefined";

// Module-level storage for provider groups, keyed by canonical channel key.
const providerGroups: Map<string, ProviderGroup> = new Map();

// Reference to the channels map for inheritance resolution.
let channelsRef: ChannelMap = {};

// User's provider selections, keyed by canonical channel key. Values are the selected provider key (e.g., "espn-disneyplus").
let providerSelections: Record<string, string> = {};

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
 * Builds provider groups by scanning all channels and grouping them by key patterns. A key like "espn-disneyplus" is a variant of "espn" because it starts with
 * "espn-". Should be called at startup after channels are loaded.
 * @param channels - The merged channel map (predefined + user channels).
 */
export function buildProviderGroups(channels: ChannelMap): void {

  channelsRef = channels;
  providerGroups.clear();

  // Build a set of all channel keys for quick lookup.
  const allKeys = new Set(Object.keys(channels));

  // Group variant keys by their canonical key (prefix before first hyphen).
  const variantsByCanonical = new Map<string, string[]>();

  for(const key of allKeys) {

    const hyphenIndex = key.indexOf("-");

    // Keys without hyphens are potential canonicals, not variants.
    if(hyphenIndex === -1) {

      continue;
    }

    const potentialCanonical = key.slice(0, hyphenIndex);

    // Only group if the canonical key exists as a channel.
    if(!allKeys.has(potentialCanonical)) {

      continue;
    }

    // This key is a variant of potentialCanonical.
    const existing = variantsByCanonical.get(potentialCanonical);

    if(existing) {

      existing.push(key);
    } else {

      variantsByCanonical.set(potentialCanonical, [key]);
    }
  }

  // Build provider groups from the grouped keys.
  for(const [ canonicalKey, variantKeys ] of variantsByCanonical) {

    const canonical = channels[canonicalKey];
    const variants: ProviderGroup["variants"] = [];

    if(isUserOverride(canonicalKey, channels)) {

      // User has overridden the canonical channel. Show their custom version first with "Custom" label, then the original predefined version.
      const predefined = PREDEFINED_CHANNELS[canonicalKey];

      variants.push({ key: canonicalKey, label: "Custom (" + extractDomain(canonical.url) + ")" });
      variants.push({ key: canonicalKey + PREDEFINED_SUFFIX, label: predefined.provider ?? getProviderDisplayName(predefined.url) });
    } else {

      // Normal case: canonical is the predefined version (or a new user-defined channel with no predefined equivalent).
      variants.push({ key: canonicalKey, label: canonical.provider ?? getProviderDisplayName(canonical.url) });
    }

    variantKeys.sort();

    for(const variantKey of variantKeys) {

      const variant = channels[variantKey];

      variants.push({ key: variantKey, label: variant.provider ?? getProviderDisplayName(variant.url) });
    }

    const group: ProviderGroup = { canonicalKey, variants };

    // Map canonical and all variants to this group for easy lookup.
    providerGroups.set(canonicalKey, group);

    for(const variantKey of variantKeys) {

      providerGroups.set(variantKey, group);
    }

    LOG.debug("Provider group '%s': variants=%s.", canonicalKey, variants.map((v) => v.key).join(", "));
  }

  // Second pass: Create groups for user overrides that don't have predefined variants. This allows users who override a single-provider channel (like nbc) to still
  // switch between their custom definition and the original predefined version.
  for(const key of allKeys) {

    // Skip if already in a group (handled in first pass).
    if(providerGroups.has(key)) {

      continue;
    }

    // Skip variant keys (keys with hyphens where the prefix exists as a channel).
    const hyphenIndex = key.indexOf("-");

    if((hyphenIndex !== -1) && allKeys.has(key.slice(0, hyphenIndex))) {

      continue;
    }

    // Check if this is a user override of a predefined channel.
    if(!isUserOverride(key, channels)) {

      continue;
    }

    // This is a user override without variants. Create a group with custom and predefined options.
    const userChannel = channels[key];
    const predefined = PREDEFINED_CHANNELS[key];
    const variants: ProviderGroup["variants"] = [
      { key, label: "Custom (" + extractDomain(userChannel.url) + ")" },
      { key: key + PREDEFINED_SUFFIX, label: predefined.provider ?? getProviderDisplayName(predefined.url) }
    ];

    const group: ProviderGroup = { canonicalKey: key, variants };

    providerGroups.set(key, group);
    LOG.debug("Provider group '%s' (override): variants=%s.", key, variants.map((v) => v.key).join(", "));
  }
}

/**
 * Resolves a URL to a friendly provider display name. Checks DOMAIN_CONFIG for a provider name matching the URL's concise domain, falling back to the raw domain
 * string if no provider name is configured. Use this when displaying provider sources in the UI (source column, provider labels) where a friendly name like "Hulu"
 * is preferred over a raw domain like "hulu.com".
 * @param url - The URL to resolve a provider display name for.
 * @returns The provider display name, or the concise domain if no provider name is configured.
 */
export function getProviderDisplayName(url: string): string {

  const domain = extractDomain(url);

  // Check DOMAIN_CONFIG for a friendly provider name. The eslint disable is needed because TypeScript's Record indexing doesn't capture that the key may not
  // exist at runtime.
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  const provider = DOMAIN_CONFIG[domain]?.provider;

  return provider ?? domain;
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

  providerSelections = { ...selections };
}

/**
 * Gets all provider selections.
 * @returns Copy of the provider selections object.
 */
export function getProviderSelections(): Record<string, string> {

  return { ...providerSelections };
}

/**
 * Gets the provider selection for a specific channel.
 * @param canonicalKey - The canonical channel key.
 * @returns The selected provider key, or undefined if using the default.
 */
export function getProviderSelection(canonicalKey: string): string | undefined {

  return providerSelections[canonicalKey];
}

/**
 * Sets the provider selection for a channel.
 * @param canonicalKey - The canonical channel key.
 * @param providerKey - The selected provider key.
 */
export function setProviderSelection(canonicalKey: string, providerKey: string): void {

  // If selecting the canonical (default), remove the selection instead of storing it.
  if(providerKey === canonicalKey) {

    delete providerSelections[canonicalKey];
  } else {

    providerSelections[canonicalKey] = providerKey;
  }
}

/**
 * Resolves a canonical channel key to the actual channel key based on user selection. If the user has selected a specific provider for this channel, returns that
 * provider's key. Otherwise returns the canonical key (default provider).
 * @param canonicalKey - The canonical channel key.
 * @returns The resolved provider key to use for streaming.
 */
export function resolveProviderKey(canonicalKey: string): string {

  const selection = providerSelections[canonicalKey];

  // No selection stored — use the canonical key (default provider).
  if(!selection) {

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

    // Normal selection — validate it exists in the merged channels.
    return selection;
  }

  // Selection is invalid (provider removed). Clear it and log a warning.
  LOG.warn("Provider selection '%s' for channel '%s' no longer exists. Using default.", selection, canonicalKey);

  delete providerSelections[canonicalKey];

  return canonicalKey;
}

/**
 * Gets a channel with inheritance applied. For provider variants, this merges the variant's properties with inherited properties from the canonical entry.
 * Inherited properties: `name`, `stationId`. Explicit properties on the variant take precedence.
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

  // Build the merged channel. Variant properties override canonical, but inherit name and stationId if not set.
  return {

    ...channel,
    name: channel.name ?? canonical.name,
    stationId: channel.stationId ?? canonical.stationId
  };
}
