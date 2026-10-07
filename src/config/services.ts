/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.ts: Service group management for multi-service channels.
 */
import type { ChangeRejection, ConfigChange } from "./reactivity.ts";
import type { Channel, ChannelMap, Config, ResolvedChannel, ServiceGroup } from "../types/index.ts";
import { LOG, extractDomain } from "../utils/index.ts";
import { DOMAIN_CONFIG } from "./sites.ts";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import { getDomainConfig } from "./profiles.ts";
import { getUserDomains } from "./userProfiles.ts";
import { pickIdentity } from "./channelIdentity.ts";
import { registerConfigChangeHandler } from "./reactivity.ts";
import { writeProcessFields } from "./index.ts";

/* Service groups allow multiple streaming services to offer the same content. For example, ESPN can be watched via ESPN.com (native) or Disney+.
 *
 * All variant relationships - predefined and user-defined - are expressed via the canonicalKey field on Channel. The flattener sets canonicalKey on predefined
 * variant entries, the browse modal sets it on user variant entries, and the schema-version migration stamps it on entries that lack it. buildServiceGroups
 * scans all channels once and groups by canonicalKey. One field, one mechanism, one code path.
 *
 * User overrides: When a user defines a channel with the same key as a predefined channel, the override's URL domain decides what the service dropdown shows. A
 * same-domain property override (its URL on the predefined channel's domain or one of its variants') keeps the service's own label and gets no :predefined
 * entry, and a single-service channel overridden this way gets no service group at all. An override on a foreign domain is shown first, labeled "Custom
 * (domain)", and is the default; the original predefined version follows under a special key suffix (PREDEFINED_SUFFIX) that distinguishes it from the user's
 * version, so the user can switch between their custom definition and the original at any time.
 *
 * User selections are stored in channels.json (in the data directory) under the `serviceSelections` key and persist across restarts.
 */

// Suffix appended to channel keys to reference the original predefined channel when a user has overridden it. For example, "espn:predefined" references the original
// predefined ESPN channel when the user has created a custom "espn" entry. hasPredefinedSuffix is the one test for the suffix and stripPredefinedSuffix the one
// strip, and the constant is exported for building a synthetic key.
export const PREDEFINED_SUFFIX = ":predefined";

/**
 * Reports whether a channel key carries the :predefined suffix, the synthetic key of the entry that points back to the original predefined channel in a user
 * override's service group.
 * @param key - The channel key.
 * @returns True when the key carries the :predefined suffix.
 */
export function hasPredefinedSuffix(key: string): boolean {

  return key.endsWith(PREDEFINED_SUFFIX);
}

/**
 * Strips the :predefined suffix from a channel key if present, returning the base key. Synthetic keys like "pbs:predefined" are created when a user overrides a
 * predefined channel - the original predefined entry gets this suffix to coexist with the user's custom version in the service dropdown. The base key finds the
 * service group a synthetic key belongs to, but the channel stored under the base key is the user's custom version, so a lookup that needs the original
 * predefined channel resolves the synthetic key through getResolvedChannel instead.
 * @param key - The channel key, possibly with :predefined suffix.
 * @returns The base key without the suffix.
 */
function stripPredefinedSuffix(key: string): string {

  return hasPredefinedSuffix(key) ? key.slice(0, -PREDEFINED_SUFFIX.length) : key;
}

// Module-level storage for service groups, keyed by canonical channel key.
const serviceGroups = new Map<string, ServiceGroup>();

// Reference to the resolved channels map (post canonical+variant inheritance). All consumer reads go through here, so it stores ResolvedChannel values where
// every entry has both identity and binding populated.
let channelsRef: Record<string, ResolvedChannel> = {};

// User's service selections, keyed by canonical channel key. Values are the selected service key (e.g., "espn-disneyplus").
let serviceSelections = new Map<string, string>();

// Service Tag System.

// The running service filter, the one copy every filter reader consults: the persisted list in CONFIG.channels.enabledServices, restricted to the tags the loaded
// channels and user domains know. Empty means no filter (every service shown); non-empty means only these tags are active.
let enabledServices: string[] = [];

/**
 * Derives the service tag for a channel from its URL domain, falling back to "direct" if no service tag is configured. Checks the channel's explicit profile
 * first (for user-defined profiles with custom serviceTag), then the URL domain via getDomainConfig().
 * @param channel - The channel to derive a tag for.
 * @returns The service tag string.
 */
function resolveServiceTag(channel: Channel): string {

  // If the channel specifies a user-defined profile, use that profile's serviceTag rather than deriving from the URL. This ensures channels with explicit profile
  // assignments are grouped under the correct service filter even when their URL domain has a different builtin serviceTag.
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
 * Resolves a channel key to the channel a lookup by key reads: getResolvedChannel's answer, which for a :predefined key is the original predefined channel a
 * selection of that key tunes, else the predefined catalog's entry for a plain key the resolved map lacks, such as a lookup made before the first service-group
 * build.
 * @param key - The channel key, possibly with the :predefined suffix.
 * @returns The channel, or undefined when the key resolves to none.
 */
function resolveLookupChannel(key: string): ResolvedChannel | Channel | undefined {

  return getResolvedChannel(key) ?? PREDEFINED_CHANNELS[key];
}

/**
 * Gets the service tag for a channel key. A key in a service group reads the tag buildServiceGroups computed for its own entry, which for a :predefined key is the
 * original predefined service's. Any other key reads the tag resolveServiceTag derives for the channel resolveLookupChannel resolves, so a :predefined key whose
 * group carries no entry of its own reads the original predefined channel, and a key that resolves to no channel reads "direct".
 * @param key - The channel key.
 * @returns The service tag string.
 */
export function getServiceTagForChannel(key: string): string {

  // The group is found by the base key, but only the exact key matches: a user override's group lists the custom canonical ahead of its :predefined entry, so
  // the base key's entry would answer a :predefined key with the custom URL's tag.
  const variant = serviceGroups.get(stripPredefinedSuffix(key))?.variants.find((v) => (v.key === key));

  if(variant) {

    return variant.tag;
  }

  const channel = resolveLookupChannel(key);

  if(!channel) {

    return "direct";
  }

  return resolveServiceTag(channel);
}

/**
 * Returns the auth domain for a channel key. Domain is the natural auth boundary - browser cookies and sessions scope to it. Multi-channel services work correctly
 * because all their channels share one domain, and canonical channels work correctly because each has its own domain. The key resolves through
 * resolveLookupChannel, as the service tag's does, so a :predefined key reads the original predefined channel a selection of that key tunes rather than the
 * user's custom version.
 * @param key - The channel key.
 * @returns The extracted domain from the channel's URL, or empty string if the channel or URL cannot be resolved.
 */
export function getAuthDomainForChannel(key: string): string {

  const channel = resolveLookupChannel(key);

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

      // Skip predefined suffix variants - the :predefined variant represents the original service being reverted to, not an independently offered
      // service, so its tag is excluded even when it differs from the canonical's current tag.
      if(hasPredefinedSuffix(variant.key)) {

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
 * Collects the unique service tags of every channel (the resolved channels plus the predefined ones) and of every user domain mapping, with display names. A
 * tag's display name comes from the builtin DOMAIN_CONFIG first, then from the user domain mappings, and "direct" is labeled Channel Website.
 * @returns Array of { displayName, domain, iconUrl, tag } objects sorted alphabetically by display name, with "direct" always first.
 */
export function getAllServiceTags(): { displayName: string; domain?: string; iconUrl?: string; tag: string }[] {

  const tags = new Set<string>();

  // Scan all channels (not just grouped ones) to find all service tags.
  const allKeys = new Set(Object.keys(channelsRef)).union(new Set(Object.keys(PREDEFINED_CHANNELS)));

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

  // Scan user domain mappings for metadata not covered by builtin DOMAIN_CONFIG.
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
 * Sets the running service filter, the module cache every filter reader consults, and writes nothing else. The cache is the running filter and
 * CONFIG.channels.enabledServices is the persisted list: applyServiceFilter derives the filter from the list, at boot and in the handler a save or a process
 * write of the list dispatches.
 * @param tags - The service tags the filter enables. Empty array means "no filter" (all services shown).
 */
export function setEnabledServices(tags: readonly string[]): void {

  enabledServices = [...tags];
}

/**
 * Makes a persisted service list the running filter, restricted to the tags the loaded channels and user domains know, with one warning naming any tag it
 * ignores. This is the one statement of the rule that an unknown tag never reaches the running filter: the boot applies it to the persisted list once the
 * service groups are built, and the handler applies it to the written list whenever a save changes the list or a process write writes it. The file keeps the
 * user's list rather than the restricted one, because a partial store load can shrink the known set, and persisting the restriction would then delete tags
 * that are legitimate once every store loads. A list whose every tag is unknown restricts to the empty filter, which shows every service.
 * @param tags - The persisted service list.
 */
export function applyServiceFilter(tags: readonly string[]): void {

  // An empty list is no filter, so there is nothing to restrict and no reason to collect the known tags.
  if(tags.length === 0) {

    setEnabledServices([]);

    return;
  }

  const knownTags = new Set(getAllServiceTags().map((tag) => tag.tag));
  const configured = [...new Set(tags)];
  const ignored = configured.filter((tag) => !knownTags.has(tag));

  if(ignored.length > 0) {

    LOG.warn("Ignoring unrecognized service tags in configuration: %s.", ignored.join(", "));
  }

  setEnabledServices(configured.filter((tag) => knownTags.has(tag)));
}

/**
 * Writes a new enabled-services list through one process write, so the file and CONFIG hold the list as given before this resolves. The write dispatches the
 * registered handler, which makes the list the running filter restricted to the tags known at that moment, as the boot and an import restrict it, so the route's
 * same-request counts read the new filter; it dispatches the handler even when the list equals the persisted one, which re-derives a running filter left
 * narrower than the list. Empty array means "no filter" (all services shown).
 * @param tags - The new enabled service tags.
 */
export async function mutateEnabledServices(tags: readonly string[]): Promise<void> {

  const list = [...tags];

  await writeProcessFields(() => ({ "channels.enabledServices": list }));
}

/**
 * Realizes a change to the persisted service list, a save's or a process write's: the candidate's list becomes the running filter through the same restriction
 * the boot applies, and the dispatch commits the list itself to CONFIG as it was written. Setting the cache cannot fail, so the handler refuses nothing.
 * @param _changes - The changes under the handler's prefix; the candidate carries the whole list, so the handler reads that instead.
 * @param next - The candidate running configuration.
 * @returns No rejections.
 */
async function applyServiceFilterChange(_changes: readonly ConfigChange[], next: Readonly<Config>): Promise<readonly ChangeRejection[]> {

  applyServiceFilter(next.channels.enabledServices);

  return [];
}

// Module-load side effect: register the handler once per process, as every config-change handler registers, so it is in place before the first save can reach
// the reconcile.
registerConfigChangeHandler("channels.enabledServices", applyServiceFilterChange);

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
 * Checks if a channel in the merged map is a user override of a predefined channel. This uses object reference comparison: the merged map that
 * getMergedChannelMap() builds holds the PREDEFINED_CHANNELS entry itself for an unoverridden canonical, and overlayDelta() produces a new object for an
 * override, so a reference that differs means a user entry replaced the predefined one.
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
 * User overrides of predefined channels (same key, different object reference) split by URL domain. A same-domain property override keeps the service's own
 * label and gets no :predefined entry, and for a single-service channel with no canonicalKey-based variants it gets no group at all. An override on a foreign
 * domain gets a "Custom (domain)" entry plus the :predefined path back to the original service, forming a two-entry group even for a single-service channel.
 *
 * After grouping, every stored service selection is validated against the rebuilt variant structure. Selections that no longer correspond to a real variant are
 * reverted to the canonical default. This is the single resolution boundary for stale selections; read-side resolvers stay pure.
 * @param channels - The merged channel map (predefined + user channels).
 * @returns Canonical keys whose service selections were stale and reverted. Empty array if all selections are valid. The caller decides whether to persist.
 */
export function buildServiceGroups(channels: Record<string, ResolvedChannel>): string[] {

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

      // Defensive: every variantKey in variantKeys came from Pass 1's scan of this same channels map, so the lookup succeeds. The narrowing keeps the types exact.
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
  // channel. Scans only canonical entries in PREDEFINED_CHANNELS: each service variant maps the same channel onto a different service's domain, so including
  // them would flood a shared-service domain (e.g., hulu.com) with many unrelated channels instead of the single, meaningful hint the index is meant to give.
  predefinedByDomain.clear();

  for(const [ key, channel ] of Object.entries(PREDEFINED_CHANNELS)) {

    if(channel.canonicalKey !== undefined) {

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
 * Resolves a URL to a friendly service display name. Checks builtin DOMAIN_CONFIG first for a stable, well-known service name, then falls back to
 * getDomainConfig() which includes user domain mappings. This ordering prevents user domain overrides from corrupting display labels for predefined channel
 * variants - a user mapping a builtin domain to a custom profile should not rename every service dropdown entry that uses that domain.
 * @param url - The URL to resolve a service display name for.
 * @returns The service display name, or the concise domain if no service name is configured.
 */
export function getServiceDisplayName(url: string): string {

  // Prefer builtin DOMAIN_CONFIG service names for stable display. Check by full hostname first (for subdomain-specific entries like tv.youtube.com), then by
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
 * user domain mappings, then URL-based builtin display name. This ensures channels assigned to user-defined profiles show the profile's service name rather
 * than the builtin name for the URL domain.
 * @param channel - The channel to resolve a label for.
 * @returns The service display label.
 */
export function getChannelServiceLabel(channel: ResolvedChannel): string {

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

  const baseKey = stripPredefinedSuffix(key);
  const group = serviceGroups.get(baseKey);

  return group?.canonicalKey ?? baseKey;
}

/**
 * Hydrates the in-memory selections cache from a serialized record. Called by mutateChannels' post-write hook (with the freshly-written disk state) and by
 * initializeUserChannels at startup. Never called by route code directly - mutations go through setServiceSelection (single) or mutateServiceSelections (bulk).
 * @param selections - Service selections keyed by canonical channel key.
 */
export function setServiceSelections(selections: Record<string, string>): void {

  serviceSelections = new Map(Object.entries(selections));
}

/**
 * Gets all service selections from the in-memory cache. The cache is hydrated from the written data on every successful mutate, and buildServiceGroups may
 * then revert stale selections in the cache ahead of the file. Startup persists that cleanup.
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

/* The default selection lookup consults the module-level serviceSelections cache, which mirrors the last committed configuration. Injecting it as a default
 * parameter of resolveServiceKey keeps every read-only caller (the tuning, playlist, and table-rendering hot paths) unchanged, while letting a caller resolve a
 * channel under a selection the cache does not hold. It is a stable module const so the default carries no per-call allocation on the hot path.
 */
const readModuleServiceSelection = (canonicalKey: string): string | undefined => serviceSelections.get(canonicalKey);

// The selection lookup that finds no selection for any channel, so resolveServiceKey resolves a channel the way it resolves once its selection is cleared.
const readNoServiceSelection = (): undefined => undefined;

/**
 * Resolves a canonical channel key to the actual channel key based on the current service selection. If a specific service is selected for this channel, returns
 * that service's key; otherwise the canonical key (default service). When the service filter is active, falls back to the first enabled variant if the selected
 * service is filtered out.
 *
 * The selection source is injected via getSelection, defaulting to the committed module cache. Stale-selection cleanup belongs to buildServiceGroups(), which
 * validates stored selections against the rebuilt variant structure on startup and after runtime mutations. A caller that needs the resolution under a
 * selection the cache does not hold passes its own lookup: hasAlternativeService passes readNoServiceSelection to resolve a channel as it stands once its
 * selection is cleared, which is how the browse remove decides inside the mutation that clears the selection, where the committed cache would still resolve
 * to the just-removed variant.
 * @param canonicalKey - The canonical channel key.
 * @param getSelection - Looks up the stored selection for a canonical key. Defaults to the committed module cache; pass another lookup to resolve under a
 *   selection the cache does not hold.
 * @returns The resolved service key to use for streaming.
 */
export function resolveServiceKey(canonicalKey: string, getSelection: (canonicalKey: string) => string | undefined = readModuleServiceSelection): string {

  const selection = getSelection(canonicalKey);

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
 * Reports whether a channel resolves to a service other than the given one once its service selection is cleared: to the first enabled variant other than a
 * :predefined entry when the filter excludes the canonical's service and such a variant exists, and to the canonical default otherwise. The browse modal's
 * remove action clears the selection and disables or deletes the channel when this is false, and the browse lineup reports the same answer to the client as the
 * channel's alternatives, so the modal's preview of an uncheck and the remove read one decision.
 * @param canonicalKey - The canonical channel key.
 * @param serviceTag - The service tag being removed.
 * @returns True when the channel keeps a service other than serviceTag with no selection stored.
 */
export function hasAlternativeService(canonicalKey: string, serviceTag: string): boolean {

  return getServiceTagForChannel(resolveServiceKey(canonicalKey, readNoServiceSelection)) !== serviceTag;
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

    // The :predefined entry is the path back to the original service rather than a service the channel offers, so the filter fallback never lands on it.
    if(hasPredefinedSuffix(variant.key)) {

      continue;
    }

    if(isServiceTagEnabled(variant.tag)) {

      return variant.key;
    }
  }

  return undefined;
}

/**
 * Gets a channel with inheritance applied. Variant inheritance is resolved at load time by getMergedChannelMap in userChannels.ts (through resolveVariant) -
 * entries in channelsRef are already fully merged with their canonical (variant values win when set, canonical fills in the rest). This function is a thin
 * accessor that also handles the synthetic :predefined suffix used when a user overrides a canonical but the service dropdown references the original
 * predefined variant.
 * @param key - The channel key (canonical, variant, or :predefined suffix).
 * @returns The complete channel, or undefined if the channel doesn't exist.
 */
export function getResolvedChannel(key: string): ResolvedChannel | undefined {

  // Handle the :predefined suffix - return the original predefined channel when the user has overridden the canonical but selects the predefined service. The
  // suffix is only meaningful for canonical keys, so the lookup target is structurally a CanonicalChannel (which is a valid ResolvedChannel).
  if(hasPredefinedSuffix(key)) {

    const predefined = PREDEFINED_CHANNELS[stripPredefinedSuffix(key)];

    return (predefined && (predefined.canonicalKey === undefined)) ? predefined : undefined;
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
export function resolvePredefinedVariant(key: string): ResolvedChannel | undefined {

  const entry = PREDEFINED_CHANNELS[key];

  if(!entry) {

    return undefined;
  }

  // Canonical entries carry full identity already. Narrow on the canonicalKey field to confirm: undefined canonicalKey means CanonicalChannel.
  if(entry.canonicalKey === undefined) {

    return entry;
  }

  // Predefined variant: identity inherits from the canonical, binding comes from the variant. We start with canonical identity ONLY (via pickIdentity) so the
  // canonical service's binding does not leak into a different service's variant. The variant's own binding fields then populate via spread.
  const canonical = PREDEFINED_CHANNELS[entry.canonicalKey];

  if(!canonical || (canonical.canonicalKey !== undefined)) {

    return undefined;
  }

  return { ...pickIdentity(canonical), ...entry };
}
