/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * selection.ts: Channel selection, provider module, and tuning type definitions for PrismCast.
 */
import type { ChannelSelectionStrategy, ResolvedSiteProfile } from "./profiles.js";
import type { Frame, Page } from "puppeteer-core";
import type { Nullable } from "./shared.js";

// Narrowed profile type for strategy functions. When selectChannel() validates that channelSelector is non-null, it narrows the profile to this type so
// strategy functions receive a guaranteed non-null channelSelector without needing non-null assertions.
export interface ChannelSelectionProfile extends ResolvedSiteProfile {

  channelSelector: string;
}

// Type guard that proves channelSelector is a non-empty string. Matches the original !channelSelector truthiness check, which rejects both null and empty string.
// Used by the coordinator before dispatching to strategy functions.
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
 * provider behavior through these hooks — no strategy-specific imports or hardcoded strategy name checks outside the registry.
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
 * a JSON array from the GET /providers/:slug/channels endpoint. Mirrors the shape of channel definitions in channels/index.ts so discovery output can be
 * used directly to populate new entries.
 */
export interface DiscoveredChannel {

  // Parent network name when the channel is a local affiliate. Present for Hulu affiliates, YTTV local affiliates, and Fox FOXD2C entries. Omitted when not
  // applicable.
  affiliate?: string;

  // The value to use as channelSelector in channels/index.ts for tuning to this channel. Always present. For most channels this equals name. For Hulu affiliates
  // this is the network name. For YTTV affiliates this is the network name. For Fox FOXD2C entries this is the internal call sign.
  channelSelector: string;

  // Human-readable display name as the provider shows it in their guide grid.
  name: string;

  // Gracenote station ID extracted from the provider's guide data, when available. Used by Channels DVR for automatic guide data matching via the
  // tvc-guide-stationid M3U attribute. Currently populated by Spectrum (tmsid from channel logo URLs).
  stationId?: string;

  // Channel tier: "paid" for subscription channels, "free" for free ad-supported channels. Present for Sling where the distinction matters (Freestream channels
  // are free). Omitted for providers where all channels are paid.
  tier?: string;
}

/**
 * Unified provider contract that bundles identity metadata, tuning strategy, and channel discovery into a single registry entry. Each provider tuning file
 * exports one ProviderModule. The coordinator builds its strategy dispatch lookup from provider modules at evaluation time. Generic strategies (thumbnailRow,
 * tileClick) remain bare ChannelStrategyEntry objects — they are not providers.
 */
export interface ProviderModule {

  /**
   * Discovers all available channels from the provider's guide. The route handler navigates to guideUrl before calling this function unless handlesOwnNavigation
   * is set. Returns a standardized DiscoveredChannel array.
   */
  discoverChannels: (page: Page) => Promise<DiscoveredChannel[]>;

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

  // Provider identifier used for API endpoints and provider filter matching (e.g., "yttv", "hulu", "foxcom"). Matches the providerTag values in DOMAIN_CONFIG so that
  // slug-based lookups and provider filter comparisons use the same identifier space.
  slug: string;

  // The existing tuning strategy contract, unchanged from the flat registry pattern.
  strategy: ChannelStrategyEntry;

  // Links back to the site profile strategy name for derived strategy lookup.
  strategyName: ChannelSelectionStrategy;

  // Optional validator called after a successful precache to determine whether the results prove the provider is authenticated. When defined, precaching calls
  // this with the discovered channels and only marks the provider as authenticated if it returns true. When omitted, any non-empty precache result proves auth.
  // Used by providers like Sling that return a guide lineup even without authentication — free-tier channels appear regardless of login state, so a non-empty
  // result alone does not prove the user has a paid subscription.
  validatePrecache?: (channels: DiscoveredChannel[]) => boolean;

  // Optional validator called after a successful tune to determine whether the channel proves the provider is authenticated. When defined, the tune success
  // handler calls this with the channel selector and only marks provider auth if it returns true. Channel health is always recorded regardless. When omitted,
  // any successful tune proves auth. Used by Sling where free-tier (Freestream) channels succeed without a paid subscription.
  validateTune?: (channelSelector: string) => boolean;
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

  // Propagated from ChannelSelectorResult — true when the tune succeeded via API interception rather than DOM interaction.
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
