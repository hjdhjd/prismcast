/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * selection.ts: Channel selection, provider module, and tuning type definitions for PrismCast.
 */
import type { ChannelSelectionStrategy, ResolvedSiteProfile, SiteProfile } from "./profiles.ts";
import type { Frame, Page } from "puppeteer-core";
import type { Nullable } from "./shared.ts";
import type { PersistedLineupChannel } from "../config/providerLineups.ts";

/**
 * Narrowed profile type for strategy functions. When selectChannel() validates that channelSelector is non-null, it narrows the profile to this type so
 * strategy functions receive a guaranteed non-null channelSelector without needing non-null assertions.
 */
export interface ChannelSelectionProfile extends ResolvedSiteProfile {

  channelSelector: string;
}

/**
 * Type guard that proves channelSelector is a non-empty string, rejecting both null and empty string. Used by the coordinator before dispatching to strategy
 * functions.
 */
export function isChannelSelectionProfile(profile: ResolvedSiteProfile): profile is ChannelSelectionProfile {

  return (profile.channelSelector !== null) && (profile.channelSelector.length > 0);
}

/**
 * Result of attempting to select a channel from a multi-channel player UI.
 */
export interface ChannelSelectorResult {

  // True when the tune succeeded via API interception rather than DOM interaction.
  directTune?: boolean;

  // Human-readable explanation of why selection failed, present only when success is false.
  reason?: string;

  // Whether the channel was successfully selected.
  success: boolean;
}

/**
 * The strategy function signature. All strategies take the Puppeteer page and a narrowed profile with guaranteed non-null channelSelector.
 */
export type ChannelStrategyHandler = (page: Page, profile: ChannelSelectionProfile) => Promise<ChannelSelectorResult>;

/**
 * The complete contract for a channel selection strategy. Each provider file exports a single object implementing this interface. The coordinator accesses all
 * provider behavior through these hooks - no strategy-specific imports or hardcoded strategy name checks outside the registry.
 */
export interface ChannelStrategyEntry {

  /**
   * Resets all module-level caches (row positions, discovered URLs, watch URLs). Called on browser restart when cached state may be stale.
   */
  clearCache?: () => void;

  /**
   * Selects the target channel in the provider's guide UI. Receives a Puppeteer page and a profile with a guaranteed non-null channelSelector. Must handle its
   * own retry logic (e.g., overlay dismiss) and return a result indicating success or failure with a diagnostic reason.
   */
  execute: ChannelStrategyHandler;

  /**
   * Removes a cached watch URL after it failed to produce a working stream. Called by the coordinator when a cached direct navigation fails.
   */
  invalidateDirectUrl?: (channelSelector: string) => void;

  /**
   * Returns a watch URL for direct navigation, bypassing guide page loading. Implementations may perform async work such as fetching current asset IDs from
   * provider APIs. The page parameter allows setting up response interception or accessing browser context when needed (e.g., cold cache setup).
   */
  resolveDirectUrl?: (channelSelector: string, page: Page) => Promise<Nullable<string>>;
}

/**
 * Standardized output shape for a channel discovered from a provider's guide. Produced by each provider's discoverChannels implementation and returned as
 * a JSON array from the GET /services/:slug/channels endpoint. Mirrors the shape of channel definitions in channels/index.ts so discovery output can be
 * used directly to populate new entries.
 */
export interface DiscoveredChannel {

  // Parent network name when the channel is a local affiliate. Present for Hulu affiliates, YTTV local affiliates, Fox FOXD2C entries, Spectrum affiliates, and
  // Xfinity/Cox (comcastPolymer) affiliates. Omitted when not applicable.
  affiliate?: string;

  // Category-selector membership. When the discovered channel belongs to a category that the provider declares in ProviderModule.categoryResolution.selectors, this field
  // names which category selector value the channel belongs to (one of the strings in that array) - e.g., "FOXD2C" for every Fox-owned local affiliate Fox.com
  // surfaces in the user's market. The membership names one of the strings in the provider's categoryResolution.selectors catalog, making the relationship explicit:
  // the catalog lives on the provider, the membership lives on each discovered channel. Consumed by the resolver (to find the user's specific instance of a
  // category) and the verifier (to confirm a captured URL's call sign belongs to the expected category set). Omitted for entries that are not category members.
  categorySelector?: string;

  // The value to use as channelSelector in channels/index.ts for tuning to this channel. Always present. For most channels this equals name. For Hulu affiliates
  // this is the network name. For YTTV affiliates this is the network name. For Fox FOXD2C entries this is the internal call sign.
  channelSelector: string;

  // Human-readable display name as the provider shows it in their guide grid.
  name: string;

  // Gracenote station ID extracted from the provider's guide data, when available. Used by Channels DVR for automatic guide data matching via the
  // tvc-guide-stationid M3U attribute. Currently populated by Spectrum (tmsid from channel logo URLs).
  stationId?: string;

  // Channel tier: "paid" for subscription channels, "free" for free ad-supported channels, or "addon" for channels requiring TV provider authentication. Present
  // for Sling, where the paid/free distinction matters (Freestream channels are free), and for Fox, where locked channels are tagged "addon". Omitted for
  // providers where every channel shares the same tier.
  tier?: string;
}

/**
 * Provider-declared identifiers for the provider's authentication wall, consumed by the blocked-page classifier when a discovery walk returns zero channels.
 * Host patterns match the landed URL's hostname (exact, or any subdomain of the pattern); selectors match against the page DOM. Either match classifies the page
 * as an auth wall ahead of the generic sign-in shape probe. Declared per provider only when the generic probe cannot recognize that provider's wall.
 */
export interface AuthWallIndicators {

  // Hostname patterns that identify the provider's authentication wall (e.g., "auth.example.com", or "example.com" to match any of its subdomains).
  hosts?: string[];

  // CSS selectors that identify the provider's authentication wall DOM.
  selectors?: string[];
}

/**
 * Unified provider contract that bundles identity metadata, tuning strategy, and channel discovery into a single registry entry. Each provider tuning file
 * exports one ProviderModule. The coordinator builds its strategy dispatch lookup from provider modules at evaluation time. Generic strategies (thumbnailRow,
 * tileClick) remain bare ChannelStrategyEntry objects - they are not providers.
 */
export interface ProviderModule {

  // Identifiers for this provider's authentication wall, checked by the blocked-page classifier ahead of its generic sign-in shape probe. Omitted when the
  // generic probe recognizes the provider's wall on its own - declare indicators only when it cannot.
  authWallIndicators?: AuthWallIndicators;

  /**
   * Discovers all available channels from the provider's guide. The route handler navigates to guideUrl before calling this function unless handlesOwnNavigation
   * is set. Returns a standardized DiscoveredChannel array.
   */
  discoverChannels: (page: Page) => Promise<DiscoveredChannel[]>;

  /**
   * Returns the provider's own statement of which lineup facts outlive the browser session that discovered them, for the persisted lineup store. A provider whose
   * direct-tune address is a stable per-channel URL (or a stable identifier the URL is built from) exports it here alongside the channel identity; a provider that
   * tunes by interacting with its guide has no such address and omits the hook entirely, so the store never holds a watch URL that cannot be navigated to.
   * Returns null when the provider's cache is cold and there is nothing to state.
   */
  exportDurableLineup?: () => Nullable<PersistedLineupChannel[]>;

  /**
   * Returns cached discovered channels if the provider has already fully enumerated its lineup from a previous tune or discovery call, or null if no enumeration
   * has occurred. When non-null, the route handler can skip browser page creation entirely and return the cached result immediately.
   */
  getCachedChannels: () => Nullable<DiscoveredChannel[]>;

  // The provider's live guide page URL. The route handler navigates here before calling discoverChannels (unless handlesOwnNavigation is set).
  guideUrl: string;

  // When true, the provider's discoverChannels function handles its own navigation instead of relying on the route to navigate to guideUrl first. Used by
  // providers that need to set up response interception before navigation (e.g., Hulu, Sling).
  handlesOwnNavigation?: boolean;

  // Human-readable display name (e.g., "YouTube TV", "Hulu").
  label: string;

  // When true, this provider does not benefit from cached direct-URL tuning optimization. Each tune requires a full page load of the provider's player SPA.
  // Used by the UI to exclude these providers from the "subsequent tunes skip guide navigation" description and to generate slow-initialization warnings.
  noDirectTuneOptimization?: boolean;

  // The site profile definition for this provider. Contains behavior flags, channel selection strategy configuration, and other provider-specific settings.
  // When defined, this profile is registered in the provider profile namespace and can be referenced by DOMAIN_CONFIG entries and channel definitions.
  profile?: SiteProfile;

  // The profile name used to register this provider's profile (e.g., "huluLive", "slingLive"). Must match the profile name referenced in DOMAIN_CONFIG entries
  // for this provider's domain. Required when profile is defined.
  profileName?: string;

  // Service identifier used for API endpoints and service filter matching (e.g., "yttv", "hulu", "foxone"). Matches the serviceTag values in DOMAIN_CONFIG so that
  // slug-based lookups and service filter comparisons use the same identifier space.
  slug: string;

  // The channel-selection strategy contract this provider implements; generic (non-provider) strategies register the same ChannelStrategyEntry shape directly.
  strategy: ChannelStrategyEntry;

  // Number of consecutive tiny segments (below the size threshold) required before triggering tab replacement recovery. Defaults to 10 (~20 seconds at 2-second
  // segments) when undefined. Providers whose normal operation includes extended periods of static or low-motion content (e.g., Comcast Polymer SPA commercial
  // placeholder images) set a higher value to avoid false positive tab replacements while still detecting genuinely frozen video over longer windows.
  // Dead capture pipelines (segments with no video trafs, hasVideo=false) always use the default count of 10 regardless of this setting, ensuring fast
  // detection of audio-only failures.
  tinySegmentThreshold?: number;

  // Must equal the channelSelection.strategy value on this provider's registered profile. The coordinator's tune dispatcher uses this field to route tune calls
  // to the correct provider module - if this drifts from the profile, the dispatcher will not find the provider and channels will fall through to generic handling.
  strategyName: ChannelSelectionStrategy;

  // Optional validator called after a successful precache to determine whether the results prove the provider is authenticated. When defined, precaching calls
  // this with the discovered channels and only marks the provider as authenticated if it returns true. When omitted, any non-empty precache result proves auth.
  // Used by providers like Sling that return a guide lineup even without authentication - free-tier channels appear regardless of login state, so a non-empty
  // result alone does not prove the user has a paid subscription.
  validatePrecache?: (channels: DiscoveredChannel[]) => boolean;

  // Optional validator called after a successful tune to determine whether the channel proves the provider is authenticated. When defined, the tune success
  // handler calls this with the channel selector and only marks provider auth if it returns true. Channel health is always recorded regardless. When omitted,
  // any successful tune proves auth. Used by Sling where free-tier (Freestream) channels succeed without a paid subscription.
  validateTune?: (channelSelector: string) => boolean;

  // Optional failsafe called after a manifest interception finalizes, in both contexts that establish a channel: a tune's interception and a token-refresh
  // re-establishment's, each only for master-kind selections. Inspects the captured master manifest URL to confirm it belongs to the channel identified by
  // channelSelector. Returns null when the URL is acceptable (either it matches the selector, or its shape is unrecognizable - we fail open in that case so a
  // CDN-side path change does not break tuning). Returns a human-readable failure reason when the URL clearly belongs to a different channel - which is the
  // signature of a click that did not switch the player. Currently implemented by foxProvider; other providers can opt in if they have similar risk.
  verifyManifestForChannel?: (url: string, channelSelector: string) => Nullable<string>;

  // Category resolution configuration. Present when this provider exposes selector values that represent a category of channels needing per-user resolution to a
  // concrete identifier (e.g., Fox's "FOXD2C", which resolves to a per-market call sign like "WFLD"). Omitted for providers whose selectors are always concrete
  // (Hulu, Sling, etc.). When present, the type system guarantees the entire feature is configured - selectors, resolver, and strict-resolution flag travel
  // together; declaring the list without a resolver is structurally impossible. See CategoryResolutionConfig for the per-field semantics.
  categoryResolution?: CategoryResolutionConfig;
}

/**
 * Successful outcome of resolving a category selector. Carries the concrete per-user channel selector that the strategy will use for matching and the verifier
 * will use for URL comparison (e.g., "WFLD" for a Chicago-market user resolving the "FOXD2C" category). The value is always a concrete identifier, never another
 * category value - the resolver guarantees the resolution is terminal.
 */
export interface CategoryResolutionSuccess {

  // The resolved per-user channel selector.
  callSign: string;
}

/**
 * Failed outcome of resolving a category selector. Carries a provider-authored, user-facing explanation that the framework relays verbatim - to debug logs on
 * permissive paths, to user-facing errors on strict paths. The resolver writes a complete sentence including any selector- or provider-specific context the user
 * needs to understand the failure and remediate it; the framework adds nothing.
 */
export interface CategoryResolutionFailure {

  // Provider-authored, user-facing explanation of why the selector could not be resolved.
  reason: string;
}

/**
 * Outcome of resolving a category selector. Discriminated union of CategoryResolutionSuccess and CategoryResolutionFailure. Resolvers must always return one of
 * these two shapes - there is no null. This forces every resolver to articulate its outcome explicitly, which guarantees diagnostic detail on every failure and
 * removes ambiguity between "could not resolve" and "did not attempt to resolve."
 *
 * Consumers tell the two variants apart by the `callSign` and `reason` field names, using TypeScript's `"callSign" in result` narrowing without needing a tagged
 * enum. A future evolution that needs additional outcomes (e.g., resolution to a list of candidates for user disambiguation) can add a new variant here as a new
 * named interface without touching the existing two.
 */
export type CategoryResolution = CategoryResolutionSuccess | CategoryResolutionFailure;

/**
 * Cohesive configuration for a provider that exposes one or more category selectors - selector values that represent a category of channels needing per-user
 * resolution to a concrete identifier rather than naming a specific channel directly. Grouping the three related fields into one sub-object makes the contract
 * atomic in the type system: a provider either has the entire configuration or has none of it. There is no way to declare a category list without a resolver, no
 * way to set a resolver without category values, and no way to set the strict-resolution flag without the rest of the machinery being present.
 *
 * Fox is the canonical example: its category selectors include "FOXD2C" (a title shared by every Fox-owned local affiliate Fox.com surfaces in the user's market),
 * and its resolver converts that to a concrete per-market call sign like "WFLD" or "WPWRDT". Providers with no category structure (e.g., Hulu, Sling, where every
 * selector names a specific channel) omit this configuration entirely.
 */
export interface CategoryResolutionConfig {

  // Resolver that converts a category selector to a concrete per-user channel identifier. Receives the page so the resolver can read DOM state or run discovery
  // in-line when needed. Returns CategoryResolutionSuccess on success or CategoryResolutionFailure with a provider-authored, user-facing reason on failure - the
  // framework relays the reason verbatim. Resolvers must always return one of these shapes; throwing is reserved for internal contract violations (i.e., bugs)
  // and propagates through the standard unexpected-error path.
  resolve: (selector: string, page: Page) => Promise<CategoryResolution>;

  // When true (strict), an unresolved category selector aborts the tune with the resolver-authored failure reason. When false or omitted (permissive, the
  // default), the strategy proceeds with the original category selector and the verifier fails open for that case - appropriate for providers like Fox where the
  // strategy can still find a reasonable container by best-effort match.
  requireResolution?: boolean;

  // The selector values that this provider treats as categories. Selectors in this list are routed through resolve() before strategy dispatch; selectors outside
  // it bypass the resolution layer entirely. Read by the resolution layer in selectChannel() to decide whether to invoke the resolver, and by provider
  // implementations as the single source of truth for which values are categories (so the verifier and other internal logic can consult the same list).
  selectors: readonly string[];
}

/**
 * Coordinates for a click target, used when clicking channel selector elements.
 */
export interface ClickTarget {

  // X coordinate relative to the viewport.
  x: number;

  // Y coordinate relative to the viewport.
  y: number;
}

/**
 * Result of tuning to a channel, containing the video context needed for monitoring.
 */
export interface TuneResult {

  // The frame or page containing the video element, used for subsequent monitoring and recovery.
  context: Frame | Page;

  // Propagated from ChannelSelectorResult - true when the tune succeeded via API interception rather than DOM interaction.
  directTune?: boolean;
}

/* Chrome DevTools Protocol operations for window management. We use CDP to resize and minimize browser windows to match viewport dimensions and reduce GPU usage
 * when the visual output isn't needed.
 */

/**
 * Browser chrome dimensions (toolbars, borders) calculated by comparing window.outerHeight/Width to window.innerHeight/Width. Used to set window size such that
 * the viewport (content area) matches our target dimensions.
 */
export interface UiSize {

  // Height of browser chrome in pixels (title bar, toolbar, etc.).
  height: number;

  // Width of browser chrome in pixels (window borders, scrollbars if visible).
  width: number;
}
