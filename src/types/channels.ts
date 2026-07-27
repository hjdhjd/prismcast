/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channels.ts: Channel type definitions and the identity/service-binding partition for PrismCast.
 */
import type { Nullable } from "./shared.ts";

/* The channel data model is partitioned into two independent concerns:
 *
 * - Identity: properties of the channel itself (name, station ID, channel number, EPG metadata). Independent of which service streams it.
 * - Service binding: how to reach the channel on a particular service (URL, channelSelector, profile, scroll/dismiss DOM hints).
 *
 * A canonical channel carries both - it is the "what this channel is" plus the canonical service's "how to reach it." A variant channel carries only the
 * service binding plus a reference to its canonical; identity inherits from the canonical at resolution time. This shape makes the canonical->variant inheritance
 * relationship correct by construction: variants structurally cannot carry identity, so they cannot leak the canonical service's identity into a different
 * service's variant.
 *
 * The discriminated union Channel = CanonicalChannel | VariantChannel is the shape of raw stored and predefined-catalog entries. ResolvedChannel is the
 * post-resolution shape consumers see, where a variant's identity has been merged in from its canonical. Most of the codebase consumes ResolvedChannel; only
 * the flatten, resolve, and normalize internals work with the discriminated raw shape.
 *
 * CHANNEL_IDENTITY_KEYS and CHANNEL_BINDING_KEYS are the single source of truth for the partition. The compile-time exhaustiveness check at the bottom of this
 * file ensures every Channel field is classified - adding a new field anywhere in the type without classifying it is a build error, not a latent bug.
 */

/**
 * Identity fields - properties of the channel itself, independent of any specific service. These inherit from canonical to variant during resolution. A user
 * may override identity fields on a canonical entry (renaming, retagging, custom logo, etc.); the override applies to the canonical and propagates to all
 * variants through resolution.
 */
export interface ChannelIdentity {

  // Numeric channel number for guide matching. When set, this number is used as the channel-number in the M3U playlist for Channels DVR and as the GuideNumber
  // in the HDHomeRun lineup for Plex. When omitted, no channel number is included in the M3U playlist and a number is auto-assigned for HDHomeRun. This field
  // is canonical-only: VariantChannel carries no channelNumber field, so every variant inherits the canonical's value unchanged at resolution time.
  channelNumber?: number;

  // Human-readable title for electronic program guide display. When set, this value is emitted as the tvg-name attribute in the M3U playlist instead of the
  // channel name. Useful for channels without EPG data (e.g., static page channels) where the channel name alone doesn't describe the content.
  guideTitle?: string;

  // Whether this channel is included in the HDHomeRun lineup for Plex. When absent or true, the channel appears in the HDHR lineup and is available for Plex
  // DVR tuning. When false, the channel is excluded from the HDHR lineup but remains available in the M3U playlist for Channels DVR. Only stored in user
  // config when explicitly set to false (sparse storage).
  hdhrEnabled?: boolean;

  // Custom logo URL for this channel. When set, this value is emitted as the tvg-logo attribute in the M3U playlist, overriding any logo derived from Channels
  // DVR. Useful for channels without EPG data or when the user prefers a specific logo.
  logoUrl?: string;

  // Human-readable channel name displayed in the M3U playlist. This is what users see in their channel guide. Set eagerly by the flattener on canonical
  // entries from the parent ChannelDefinition's name field. For user-defined channels, set explicitly at creation time.
  name?: string;

  // Gracenote station ID for the Pacific timezone feed. When present on a canonical entry, the Pacific generator auto-creates a sibling ChannelDefinition with
  // this station ID, inheriting services (filtered for East/West-specific channelSelectors). See generatePacificDefinitions() in channels/index.ts.
  pacificStationId?: string;

  // Gracenote station ID for electronic program guide integration. When set, this ID is included in the M3U playlist as the tvc-guide-stationid attribute,
  // allowing Channels DVR to fetch program guide data for the channel.
  stationId?: string;

  // Organizational tags for playlist filtering. Tags are freeform strings with preserved casing, compared case-insensitively via tagsMatch(). Used by the
  // ?tag= playlist query parameter to generate filtered playlists.
  tags?: string[];

  // EPG time shift in hours. When set, this value is included in the M3U playlist as the tvg-shift attribute, telling Channels DVR to offset the guide data
  // by this many hours. Useful for time-delayed feeds that share a station ID with the primary feed (e.g., Pacific feeds that air 3 hours after the East feed).
  tvgShift?: number;
}

/**
 * Service binding fields - how to reach the channel on a particular service's platform. These do NOT inherit from canonical to variant, because each service
 * has its own URL, its own selector vocabulary, its own player DOM hooks. A variant must declare its own binding fields (or have them populated by discovery);
 * the canonical service's binding is meaningful only for the canonical service.
 */
export interface ChannelServiceBinding {

  // Service-specific channel identifier for multi-channel players. For sites where the base URL is shared but different channels require clicking different
  // UI elements (e.g., Hulu, Sling, YouTube TV, Fox.com guide grid).
  channelSelector?: string;

  // CSS selector for an intermittent modal or overlay to dismiss after page load. Service-specific because dismissal patterns differ per provider.
  dismissSelector?: string;

  // Profile name to use for this binding, overriding URL-based profile detection. Use when a service's behavior doesn't match what would be inferred from its
  // domain, or when a specific channel needs different handling than other channels on the same service.
  profile?: string;

  // CSS selector to narrow the DOM search when scrollTarget is set. Service-specific lazy-load handling.
  scrollSelector?: string;

  // Text content to match when scrolling a lazy-loaded section into view before channel selection. Service-specific lazy-load handling.
  scrollTarget?: string;

  // Whether to scroll to the bottom of the page before channel selection. Service-specific lazy-load handling.
  scrollToBottom?: boolean;

  // Display name override for the service selection dropdown on this binding. Normally auto-derived from the URL domain via DOMAIN_CONFIG in config/profiles.ts.
  // Only needed when a binding's display name should differ from the domain-level default.
  service?: string;

  // URL of the streaming page to capture. This should be the direct URL to the live stream player, not a landing page or show page. Authentication cookies
  // from the Chrome profile are used, so the URL can be to authenticated content.
  url: string;
}

/**
 * Canonical or standalone channel entry. Carries identity (the "what") plus the canonical service's binding (the "how to reach it on this service"). The
 * canonicalKey field is structurally absent (typed as never) so that the discriminated union below distinguishes canonicals from variants by shape: a value
 * with a string canonicalKey is a VariantChannel, anything else is a CanonicalChannel.
 *
 * Predefined canonicals are produced by the flattener; standalone user channels (no predefined parent) take this same shape.
 *
 * Sibling-variant non-overlap rule (a storage-layer rule that always holds): a canonical override's binding fields exist to customize the canonical
 * service's binding - NOT to express "I'd rather default this channel to a sibling service." When the user wants a sibling service to be the default
 * for a channel, that intent is expressed via serviceSelections (config/services.ts), not by overriding the canonical URL. The storage layer enforces
 * this: any canonical override whose binding URL extracts to a sibling variant's domain is normalized at write time by inferTargetVariant +
 * normalizeChannelDeltas in config/userChannels.ts. Binding fields are stripped from the canonical override, propagated to the matching variant entry
 * as a binding-only override (when they diverge from the variant's predefined defaults), and serviceSelections[canonicalKey] is set to the matching
 * variant key. The producer (the PUT handler in routes/config/channels/endpoints/crud.ts), the startup heal in initializeUserChannels, and the
 * normalizer all share one inferTargetVariant helper, so the rule lives in exactly one place.
 */
export interface CanonicalChannel extends ChannelIdentity, ChannelServiceBinding {

  // Always absent on canonicals and standalones. Typed as `never` so the type system rejects any attempt to set it - that would convert this entry into a variant.
  canonicalKey?: never;
}

/**
 * Variant channel entry. Carries the binding (tuning) for one non-canonical service, plus a canonicalKey reference to its parent. Variants are pure tuning
 * data: how to reach the channel via this service. Identity (the channel's name, station ID, channel number, hdhrEnabled, tags, etc.) lives on the canonical
 * entry alone and inherits to variants at resolution time. Per-affiliate identity (e.g., a local Chicago Fox affiliate that needs its own station ID) is
 * modeled as a separate canonical channel rather than as a variant carrying override identity.
 *
 * Enforcement layers:
 *
 *   1. Type system (this declaration): VariantChannel extends only ChannelServiceBinding. The type structurally refuses to admit identity fields on
 *      variants in code; identity always inherits from the canonical at resolution time.
 *   2. Catalog source (ServiceVariant in this file): carries binding fields only. The catalog flattener cannot produce identity-bearing variants.
 *   3. Resolver (overlayVariantBinding in userChannels.ts): variant overlays apply CHANNEL_BINDING_KEYS only. Identity fields encountered in a variant entry
 *      (legacy data, hand-edited files, future migrations) are silently dropped during resolution - canonical's identity always wins.
 *   4. PUT handler routing (handlePredefinedEdit in routes/config/channels/endpoints/crud.ts): identity-field edits route to the canonical entry, binding-
 *      field edits route to the active variant entry. Users cannot create identity-on-variant state through the UI.
 *
 * The structural rule the type DOES enforce is the tag: canonicalKey is required (and structurally absent on CanonicalChannel via never), so a value with
 * a string canonicalKey is unambiguously a VariantChannel.
 */
export interface VariantChannel extends ChannelServiceBinding {

  // Required - the tag that marks this entry as a variant of another channel.
  canonicalKey: string;
}

/* Resolution boundary rule (codified for future contributors).
 *
 * Raw stored or catalog values are typed `Channel` (`CanonicalChannel | VariantChannel`) or `StoredChannel` (the storage-only union including `ChannelDelta`).
 * Values downstream of any resolver - `resolveStoredChannel`, `getResolvedChannel`, `overlayDelta`, `overlayVariantBinding`, `pickIdentity` - are typed
 * `ResolvedChannel`. Crossing this boundary by downcasting is a defect: variants do not structurally carry identity, and a `Channel`-typed post-resolution
 * value lies about which fields are reachable on the variant case.
 *
 * The single test for whether a value is `ResolvedChannel`: was it produced by, or returned through, a function whose body ran resolution? If yes, type it
 * `ResolvedChannel`. If no, type it `Channel` or `StoredChannel`. The compiler will not catch a misuse - reviewers must.
 */

/**
 * The shape of a raw stored or predefined-catalog channel entry. Used by the flattener output (PREDEFINED_CHANNELS), by the on-disk channels.json store, and
 * by internal flatten/resolve/normalize logic. Consumers of resolved data use ResolvedChannel instead.
 */
export type Channel = CanonicalChannel | VariantChannel;

/**
 * Map of channel keys to raw stored or predefined-catalog channel entries. Channel keys are URL-safe slugs (lowercase letters, numbers, hyphens) since they
 * appear in stream request URLs.
 */
export type ChannelMap = Record<string, Channel>;

/**
 * Post-resolution channel shape. Variants have had their identity merged in from the canonical, so this carries both identity and binding plus an optional
 * canonicalKey indicating the variant relationship (kept for downstream consumers that need to distinguish a resolved variant from a resolved canonical).
 *
 * This is what most of the codebase consumes - getMergedChannelMap, getChannelListing, the playlist generator, the HDHR lineup, route handlers, and so on
 * all return or handle ResolvedChannel data.
 */
export interface ResolvedChannel extends ChannelIdentity, ChannelServiceBinding {

  // Optional. Present when this resolved entry corresponds to a variant; absent on canonical and standalone resolutions. Consumers that need to know whether
  // an entry is a variant inspect this field.
  canonicalKey?: string;
}

/**
 * Map of channel keys to resolved channels. The output type of getMergedChannelMap and the consumption type for the rest of the codebase.
 */
export type ResolvedChannelMap = Record<string, ResolvedChannel>;

/**
 * Single source of truth for which Channel fields are identity. Used by resolveVariant and normalizeChannelDeltas to extract the inheritable subset of a
 * canonical, by the M3U/HDHR output layers to know which fields to read for guide metadata, and by the user-edit form's allowlist for predefined-channel
 * overrides. Adding a field to ChannelIdentity without listing it here is a compile error via the exhaustiveness check below.
 */
export const CHANNEL_IDENTITY_KEYS = [ "channelNumber", "guideTitle", "hdhrEnabled", "logoUrl", "name", "pacificStationId", "stationId", "tags", "tvgShift" ] as const;

/**
 * Single source of truth for which Channel fields are service-bindings. Used by the variant builder, the resolver (which excludes these from canonical->variant
 * inheritance), the user-edit form's binding-override allowlist, and any code that needs to distinguish "how to reach the channel" from "what the channel is."
 * Adding a field to ChannelServiceBinding without listing it here is a compile error via the exhaustiveness check below.
 */
export const CHANNEL_BINDING_KEYS = [ "channelSelector", "dismissSelector", "profile", "scrollSelector", "scrollTarget", "scrollToBottom", "service", "url" ] as const;

/* Compile-time partition completeness check. Every key on CanonicalChannel and VariantChannel must be classified as identity, binding, or the explicit
 * "neither" set ("canonicalKey"). If a new field is added without classifying it, the _ChannelKeyExhaustiveness type will not collapse to `never` and the
 * `_partitionCompleteness` assignment will fail to type-check.
 *
 * The intent is that this is the only place anyone needs to look to confirm "every Channel field has been considered" - the compiler enforces it.
 */
type _ClassifiedChannelKey = typeof CHANNEL_IDENTITY_KEYS[number] | typeof CHANNEL_BINDING_KEYS[number] | "canonicalKey";
type _ChannelKeyExhaustiveness = Exclude<keyof CanonicalChannel | keyof VariantChannel, _ClassifiedChannelKey>;

interface _PartitionError {

  error: "Add field to CHANNEL_IDENTITY_KEYS, CHANNEL_BINDING_KEYS, or the canonicalKey carve-out";
  field: _ChannelKeyExhaustiveness;
}

const _partitionCompleteness: [_ChannelKeyExhaustiveness] extends [never] ? true : _PartitionError = true;

/* Mark the assertion as intentionally unused - its only purpose is the compile-time check above. */
void _partitionCompleteness;

/**
 * Identity fields that participate in the user-facing delta surface (form input, JSON import, channels.json hand edit). Subset of CHANNEL_IDENTITY_KEYS that
 * excludes catalog-driven structural fields like pacificStationId, which trigger Pacific auto-generation in the flattener and are not user-overridable through
 * any path. Symmetric to DELTA_ELIGIBLE_BINDING_KEYS - both arrays describe "delta-eligible," the universe of fields that may legitimately appear in a
 * ChannelDelta or a stored override.
 *
 * The `satisfies` constraint guarantees every entry is in CHANNEL_IDENTITY_KEYS - this array can never include a field that isn't structurally identity.
 */
export const DELTA_ELIGIBLE_IDENTITY_KEYS = [
  "channelNumber", "guideTitle", "hdhrEnabled", "logoUrl", "name", "stationId", "tags", "tvgShift"
] as const satisfies readonly typeof CHANNEL_IDENTITY_KEYS[number][];

/**
 * Binding fields that participate in the user-facing delta surface. Subset of CHANNEL_BINDING_KEYS that excludes internal DOM-hook fields (dismissSelector,
 * scrollSelector, scrollTarget, scrollToBottom, service) that are set by site profiles and ServiceVariant catalog entries, never by user input. Symmetric to
 * DELTA_ELIGIBLE_IDENTITY_KEYS.
 *
 * The `satisfies` constraint guarantees every entry is in CHANNEL_BINDING_KEYS.
 */
export const DELTA_ELIGIBLE_BINDING_KEYS = [
  "channelSelector", "profile", "url"
] as const satisfies readonly typeof CHANNEL_BINDING_KEYS[number][];

/**
 * The universe of fields that may legitimately appear in a stored override (ChannelDelta) or be customized by the user. Used to type the customization
 * accessor's Map keys, drive the runtime DELTA_ALLOWED_FIELDS Set in userChannels.ts, and underpin every "is this field user-overridable?" question across the
 * codebase. Equal to DELTA_ELIGIBLE_IDENTITY_KEYS ∪ DELTA_ELIGIBLE_BINDING_KEYS.
 */
export type CustomizableField = typeof DELTA_ELIGIBLE_IDENTITY_KEYS[number] | typeof DELTA_ELIGIBLE_BINDING_KEYS[number];

/* Compile-time delta-shape completeness check. Mirrors the _partitionCompleteness pattern above. ChannelDelta's keys must exactly equal CustomizableField - no
 * more, no less. Adding a field to ChannelDelta without listing it in DELTA_ELIGIBLE_IDENTITY_KEYS or DELTA_ELIGIBLE_BINDING_KEYS, or vice versa, fails to
 * type-check.
 *
 * The check has two halves: extras (keys in ChannelDelta but not in the partition) and missing (keys in the partition but not in ChannelDelta). Either failure
 * produces a build error with a descriptive message.
 */
type _DeltaExtraKeys = Exclude<keyof ChannelDelta, CustomizableField>;
type _DeltaMissingKeys = Exclude<CustomizableField, keyof ChannelDelta>;

interface _DeltaShapeError {

  error: "ChannelDelta keys must exactly equal DELTA_ELIGIBLE_IDENTITY_KEYS ∪ DELTA_ELIGIBLE_BINDING_KEYS. Update ChannelDelta or the partition arrays.";
  extra: _DeltaExtraKeys;
  missing: _DeltaMissingKeys;
}

const _deltaCompleteness: [_DeltaExtraKeys, _DeltaMissingKeys] extends [never, never] ? true : _DeltaShapeError = true;

/* Mark the assertion as intentionally unused - its only purpose is the compile-time check above. */
void _deltaCompleteness;

/**
 * Nested channel definition - the AUTHORING shape for predefined channels in channels/index.ts. Already correctly partitioned: identity at the top level,
 * services map below. The flattener compiles these into the runtime Channel discriminated union.
 */
export interface ChannelDefinition {

  // Numeric channel number for guide matching. Inherited unchanged by all service variants - ServiceVariant carries no channelNumber field, so it cannot
  // be overridden on a per-service basis.
  channelNumber?: number;

  // Human-readable channel name displayed in the M3U playlist and channel guide. Required for all channel definitions.
  name: string;

  // Gracenote station ID for the Pacific timezone feed. When present, the Pacific generator auto-creates a sibling ChannelDefinition with this station ID,
  // inheriting the services map (filtered for East/West-specific channelSelectors).
  pacificStationId?: string;

  // Service variants keyed by service slug. The "site" key represents the channel's own website and always wins as canonical when present. When "site" is
  // absent, the service whose key sorts first alphabetically becomes the canonical. Service keys are sorted by computation, not source ordering.
  services: Record<string, ServiceVariant>;

  // Gracenote station ID for electronic program guide integration. Inherited by all service variants.
  stationId?: string;

  // Organizational tags for playlist filtering. Inherited by all service variants. Tags are freeform strings with preserved casing (e.g., "Sports", "HBO"),
  // compared case-insensitively throughout the system via tagsMatch(). Managed via the tag registry. Used by the ?tag= playlist query parameter for custom
  // playlist generation.
  tags?: string[];

  // EPG time shift in hours. Inherited by all service variants.
  tvgShift?: number;
}

/**
 * A single service's streaming configuration within a ChannelDefinition. Each service variant specifies how to reach the channel on that service's platform -
 * exclusively binding (tuning) data. Identity fields (channelNumber, hdhrEnabled, tags, etc.) are user preferences for the channel as a whole and live on the
 * ChannelDefinition (canonical) rather than on individual service variants. Adding a field here that conceptually represents user preference rather than
 * tuning is a category error.
 */
export interface ServiceVariant {

  // Service-specific channel identifier for multi-channel players. This is always service-specific (e.g., Fox uses station codes "FOXD2C" while Sling uses
  // guide names "FOX") and is never inherited from the parent ChannelDefinition.
  channelSelector?: string;

  // CSS selector for an intermittent modal or overlay to dismiss on this service.
  dismissSelector?: string;

  // Profile name override for this service. Overrides URL-based auto-detection.
  profile?: string;

  // Display name override for the service selection dropdown on this specific variant.
  service?: string;

  // CSS selector to narrow the DOM search when scrollTarget is set. Service-specific override.
  scrollSelector?: string;

  // Text content to match when scrolling a lazy-loaded section into view. Service-specific override.
  scrollTarget?: string;

  // Whether to scroll to the bottom of the page before channel selection. Service-specific override.
  scrollToBottom?: boolean;

  // URL of the streaming page for this service. Required for every service variant.
  url: string;
}

/**
 * Enriched channel entry returned by getChannelListing(). Wraps a resolved Channel with source classification and enabled status metadata, providing the
 * single source of truth for merged channel data across the codebase.
 */
export interface ChannelListingEntry {

  // Whether the channel has at least one service variant available given the current service filter. When false, the channel is hidden from the playlist and guide.
  availableByService: boolean;

  // The resolved channel with all properties (name, url, profile, etc.) populated from canonical+variant inheritance.
  channel: ResolvedChannel;

  // Whether the channel is enabled for streaming and playlist inclusion. Disabled predefined channels (without user overrides) have this set to false.
  enabled: boolean;

  // The channel key (URL-safe slug used in stream URLs).
  key: string;

  // Where this channel comes from: "predefined" (builtin), "user" (user-defined), or "override" (user channel replacing a predefined one).
  source: "override" | "predefined" | "user";
}

/**
 * A delta override for a predefined channel. All fields are optional because only fields that differ from the predefined definition are stored. String and
 * number fields use Nullable<T> to distinguish "user cleared this field" (null) from "inherit from predefined" (absent). When a field is null, the predefined
 * value is removed in the resolved channel. When a field is absent, the predefined value is inherited.
 *
 * The set of fields here is the union of DELTA_ELIGIBLE_IDENTITY_KEYS (delta-eligible identity overrides) and DELTA_ELIGIBLE_BINDING_KEYS (delta-eligible
 * binding overrides). Catalog-driven identity (pacificStationId) and internal binding (DOM hooks) are not user-overridable and intentionally absent. The
 * _deltaCompleteness compile-time check (above) enforces that this interface and the two partition arrays stay in agreement.
 */
export interface ChannelDelta {

  // Override for channel number, or null to clear the predefined value.
  channelNumber?: Nullable<number>;

  // Override for channel selector, or null to clear the predefined value.
  channelSelector?: Nullable<string>;

  // Override for guide title, or null to clear the value.
  guideTitle?: Nullable<string>;

  // Override for HDHomeRun lineup inclusion, or null to clear (revert to default included). When false, the channel is excluded from the HDHR lineup.
  hdhrEnabled?: Nullable<boolean>;

  // Override for custom logo URL, or null to clear the value.
  logoUrl?: Nullable<string>;

  // Override for display name, or null to clear the predefined value.
  name?: Nullable<string>;

  // Override for profile, or null to clear the predefined value.
  profile?: Nullable<string>;

  // Override for station ID, or null to clear the predefined value.
  stationId?: Nullable<string>;

  // Override for organizational tags, or null to clear the predefined tags. When set, replaces the predefined tags entirely (full replacement, not additive).
  tags?: Nullable<string[]>;

  // Override for EPG time shift, or null to clear the predefined value.
  tvgShift?: Nullable<number>;

  // Override for URL, or null to clear the predefined value. When absent, the predefined URL is inherited.
  url?: Nullable<string>;
}

/**
 * What gets stored in channels.json per key. For user-defined channels (no predefined equivalent), this is a full Channel with a required url. For overrides
 * of predefined channels, this can be a ChannelDelta with only the differing fields. Legacy full-override entries (from before the delta model) are also valid -
 * they are just deltas that happen to override every field.
 */
export type StoredChannel = Channel | ChannelDelta;

/**
 * Map of channel keys to stored channel data (full definitions or deltas). This is the raw type for the channels.json file contents.
 */
export type StoredChannelMap = Record<string, StoredChannel>;

/* Service groups allow multiple streaming services to offer the same content (e.g., ESPN via ESPN.com or Disney+). All variant relationships are expressed via
 * the canonicalKey field on Channel. The flattener sets it on predefined variants, the browse modal sets it on user variants, and buildServiceGroups groups
 * channels by scanning canonicalKey uniformly. The canonical key identifies the channel content; each variant provides that content through a different service.
 */

/**
 * Represents a group of service variants for the same content. Used by the UI to display service selection dropdowns for multi-service channels.
 */
export interface ServiceGroup {

  // The canonical channel key (without suffix), which is the default service. Example: "espn" for the ESPN channel group.
  canonicalKey: string;

  // List of all service variants including the canonical entry. Each variant has a key, display label, and service tag computed at group-building time.
  variants: {

    // Channel key for this service variant. Example: "espn" or "espn-disneyplus".
    key: string;

    // UI display label derived from channel.service (if set) or auto-resolved from the URL domain via getServiceDisplayName(). Example: "ESPN.com" or "Disney+".
    label: string;

    // Service tag for this variant, computed at group-building time. Used by the service filter to show/hide variants and by getChannelServiceTags to collect
    // tags without re-deriving them from the channel key. For :predefined variants, this is derived from the predefined channel's URL (not the user override).
    tag: string;
  }[];
}
