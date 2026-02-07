/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * profiles.ts: Site profiles and domain mappings for PrismCast.
 */
import type { ProfileCategory, ProfileResolutionResult, ResolvedSiteProfile, SiteProfile } from "../types/index.js";
import { CHANNELS } from "../channels/index.js";

/*
 * Streaming sites implement their video players in wildly different ways. Some use standard HTML5 video with keyboard shortcuts, others embed players in iframes,
 * and many have unique quirks like auto-muting or requiring specific fullscreen methods. Rather than scattering site-specific conditionals throughout the streaming
 * code, we define "site profiles" that describe each site's behavior in a declarative way.
 *
 * The profile system has three components:
 *
 * 1. SITE_PROFILES: Named behavior configurations describing how to handle different player implementations. Profiles can inherit from other profiles using the
 *    "extends" property, allowing us to define base profiles for common patterns (like "keyboardFullscreen" for sites using the f key) and then extend them with
 *    site-specific variations.
 *
 * 2. DOMAIN_TO_PROFILE: A mapping from domain patterns to profile names. When streaming a URL, we check if it matches any known domain and use the corresponding
 *    profile. This is the primary mechanism for automatically selecting the right behavior.
 *
 * 3. Channel-level profile hints: Individual channel definitions can specify an explicit profile name, overriding URL-based detection. This is useful when a
 *    channel's URL doesn't match the expected domain pattern, or when the same domain serves multiple channel types that need different handling.
 *
 * Profile resolution happens at stream startup and the resolved profile is passed through the entire streaming pipeline. The profile flags control:
 * - How fullscreen is triggered (keyboard shortcut vs JavaScript API)
 * - Whether to search for video elements in iframes
 * - Which video element to select when multiple exist
 * - Whether to wait for network activity to settle before playback
 * - Whether to lock volume properties to prevent auto-muting
 * - Whether the page is static content (no video element expected)
 *
 * When adding support for a new streaming site, first check if an existing profile matches its behavior. Only create a new profile if the site requires unique
 * handling not covered by existing profiles.
 */

/*
 * Each profile defines a set of behavior flags that control how we interact with the video player. Profiles are organized in an inheritance hierarchy based on
 * behavior patterns rather than site ownership. This makes it easier to identify the right profile when adding new channels.
 *
 * Base profiles (no extends):
 * - keyboardFullscreen: Sites using the f key for fullscreen toggle
 * - fullscreenApi: Sites requiring the JavaScript requestFullscreen() API
 * - staticPage: Non-video pages captured as static visual content
 *
 * Derived profiles (extends a base):
 * - keyboardDynamic: Keyboard fullscreen + network idle wait (extends keyboardFullscreen)
 * - keyboardMultiVideo: Keyboard fullscreen + multi-video selection (extends keyboardFullscreen)
 * - keyboardIframe: Keyboard fullscreen + iframe handling (extends keyboardFullscreen)
 * - keyboardDynamicMultiVideo: Keyboard + network idle + multi-video selection (extends keyboardDynamic)
 * - clickToPlayKeyboard: Click to start playback + keyboard fullscreen (extends keyboardFullscreen)
 * - brightcove: Brightcove players using API fullscreen + network idle wait (extends fullscreenApi)
 * - clickToPlayApi: Click to start playback + API fullscreen (extends fullscreenApi)
 * - disneyNow: DisneyNOW player with play button overlay + multi-video (extends clickToPlayApi)
 * - embeddedPlayer: Iframe-based players using fullscreen API (extends fullscreenApi)
 * - apiMultiVideo: API fullscreen + multi-video + tile-based channel selection (extends fullscreenApi)
 * - huluLive: Hulu Live TV with guide grid channel selection + fullscreen button (extends fullscreenApi)
 * - embeddedDynamicMultiVideo: Embedded + network idle + multi-video selection (extends embeddedPlayer)
 * - embeddedVolumeLock: Embedded + volume property locking (extends embeddedPlayer)
 *
 * Each profile includes a description field documenting its purpose. This is metadata only - it's stripped during profile resolution and exists purely for
 * documentation.
 */
export const SITE_PROFILES: Record<string, SiteProfile> = {

  // Profile for multi-channel live TV pages that present a grid or shelf of live channel tiles requiring tile-based selection followed by a play button click. Uses
  // the fullscreen API and multi-video selection to find the actively playing stream after channel selection. Does not use iframe handling or network idle wait
  // because these sites serve video directly in the main page and have persistent connections that prevent network idle.
  apiMultiVideo: {

    category: "multiChannel",
    channelSelection: { strategy: "tileClick" },
    description: "Multi-channel sites with tile-based channel grid. Requires Channel Selector set to the CSS selector for the channel tile.",
    extends: "fullscreenApi",
    selectReadyVideo: true,
    summary: "Multi-channel (tile selection, needs selector)"
  },

  // Profile for sites using the Brightcove player platform. Brightcove players require waiting for network activity to settle before the video player is fully
  // initialized. The player dynamically loads its configuration and stream manifest, so waitForNetworkIdle ensures we don't try to interact with the player before
  // it's ready. Uses the JavaScript fullscreen API rather than keyboard shortcuts because Brightcove intercepts keyboard events.
  brightcove: {

    category: "api",
    description: "Brightcove player sites requiring network idle wait and API fullscreen.",
    extends: "fullscreenApi",
    summary: "Brightcove players (network wait)",
    waitForNetworkIdle: true
  },

  // Profile for sites that require clicking to start playback. Some players don't autoplay and need user interaction to begin. Uses the JavaScript fullscreen API.
  // Set clickSelector in the profile or channel definition to specify a play button element; otherwise clicks the video element directly.
  clickToPlayApi: {

    category: "api",
    clickToPlay: true,
    description: "Sites requiring a click to start playback, using the JavaScript fullscreen API. Use clickSelector for play button overlays.",
    extends: "fullscreenApi",
    summary: "Click-to-play (API fullscreen)"
  },

  // Profile for sites that require clicking to start playback, using keyboard 'f' for fullscreen. Use this when clickToPlayApi doesn't work for fullscreen but the
  // site responds to the 'f' key. Set clickSelector in the profile or channel definition to specify a play button element.
  clickToPlayKeyboard: {

    category: "keyboard",
    clickToPlay: true,
    description: "Sites requiring a click to start playback, using the 'f' key for fullscreen. Use clickSelector for play button overlays.",
    extends: "keyboardFullscreen",
    summary: "Click-to-play ('f' key fullscreen)"
  },

  // Profile for DisneyNOW (disneynow.com) which has a play button overlay that must be clicked to start playback and multiple video elements on the page.
  disneyNow: {

    category: "api",
    clickSelector: ".overlay__button button",
    description: "DisneyNOW player with play button overlay and multiple video elements.",
    extends: "clickToPlayApi",
    selectReadyVideo: true,
    summary: "DisneyNOW player"
  },

  // Profile for iframe-embedded players that also have multiple video elements (ads, placeholders, main content) and need network activity to settle. The
  // selectReadyVideo flag ensures we find the video with actual content rather than an ad placeholder. Combines iframe handling with API-based fullscreen.
  embeddedDynamicMultiVideo: {

    category: "api",
    description: "Iframe-embedded players with multiple video elements requiring network idle wait.",
    extends: "embeddedPlayer",
    selectReadyVideo: true,
    summary: "Embedded multi-video (network wait)",
    waitForNetworkIdle: true
  },

  // Intermediate profile for sites that both embed their player in an iframe AND require the JavaScript fullscreen API. Many modern players use this architecture
  // to isolate ad content and use programmatic fullscreen rather than keyboard shortcuts. This profile combines iframe handling with API-based fullscreen.
  embeddedPlayer: {

    category: "api",
    description: "Intermediate base profile for iframe-embedded players using fullscreen API.",
    extends: "fullscreenApi",
    needsIframeHandling: true,
    summary: "Embedded iframe players"
  },

  // Profile for iframe-embedded players that aggressively mute audio after page load - likely to comply with autoplay policies or for accessibility reasons. Some
  // sites set video.muted = true even after we unmute it. The lockVolumeProperties flag uses Object.defineProperty to override the muted and volume getters/setters,
  // preventing the site from re-muting the video.
  embeddedVolumeLock: {

    category: "api",
    description: "Iframe-embedded players that aggressively mute audio after page load.",
    extends: "embeddedPlayer",
    lockVolumeProperties: true,
    summary: "Embedded players that auto-mute"
  },

  // Base profile for sites that require the JavaScript fullscreen API (element.requestFullscreen()) instead of keyboard shortcuts. Many modern players intercept
  // keyboard events for their own controls, making the f key unreliable. Calling requestFullscreen() directly on the video element bypasses the player's keyboard
  // handling and reliably enters fullscreen mode.
  fullscreenApi: {

    category: "api",
    description: "Base profile for sites requiring the JavaScript fullscreen API.",
    summary: "Sites needing JavaScript fullscreen",
    useRequestFullscreen: true
  },

  // Profile for Hulu Live TV which presents a guide grid of live channels. The channel list is revealed by clicking a tab (listSelector), then the desired channel
  // is found by matching img.alt text. Uses the fullscreen API (inherited from fullscreenApi) plus a dedicated fullscreen button selector for the player's native
  // maximize control. Requires selectReadyVideo because the page may have multiple video elements (ads, previews, main content). Uses waitForNetworkIdle because
  // Hulu's SPA has heavy async initialization that often prevents the load event from firing within the retryOperation timeout; the graceful networkidle2 fallback
  // in navigateToPage() allows execution to continue to channel selection even when background requests are still pending.
  huluLive: {

    category: "multiChannel",
    channelSelection: { listSelector: "#CHANNELS", playSelector: "[data-testid=\"generic-tile-thumbnail\"]", strategy: "guideGrid" },
    description: "Hulu Live TV with guide grid channel selection. Requires Channel Selector set to the channel name matching the guide grid image alt text.",
    extends: "fullscreenApi",
    fullscreenSelector: "[aria-label=\"Maximize\"]",
    selectReadyVideo: true,
    summary: "Hulu Live TV (guide grid, needs selector)",
    waitForNetworkIdle: true
  },

  // Profile for sites that use keyboard fullscreen and also need time for network activity to settle before the player is fully initialized. These sites dynamically
  // load their player and content. The waitForNetworkIdle flag ensures we don't try to interact with the player until all initial network requests have completed.
  keyboardDynamic: {

    category: "keyboard",
    description: "Keyboard fullscreen sites requiring network idle wait for dynamic content loading.",
    extends: "keyboardFullscreen",
    summary: "Dynamic sites ('f' key fullscreen)",
    waitForNetworkIdle: true
  },

  // Profile for multi-channel player pages that use keyboard fullscreen and need both network idle wait and multi-video selection. These pages present multiple
  // channels to choose from, and the channelSelector property in the channel definition specifies which one to select. Extends keyboardDynamic to inherit network
  // idle wait behavior. Uses thumbnailRow strategy for channel selection (find channel by thumbnail image URL, click adjacent show entry).
  keyboardDynamicMultiVideo: {

    category: "multiChannel",
    channelSelection: { strategy: "thumbnailRow" },
    description: "Multi-channel sites with thumbnail row layout. Requires Channel Selector set to the channel's thumbnail image URL.",
    extends: "keyboardDynamic",
    selectReadyVideo: true,
    summary: "Multi-channel (thumbnail row, needs selector)"
  },

  // Base profile for sites that respond to the f key for fullscreen toggle. This is the most common fullscreen mechanism, following YouTube-style keyboard
  // shortcuts. The f key is sent as a keyboard event to the page, triggering the player's built-in fullscreen toggle. This works with most standard video players.
  keyboardFullscreen: {

    category: "keyboard",
    description: "Base profile for sites that respond to the f key for fullscreen toggle.",
    fullscreenKey: "f",
    summary: "Standard 'f' key fullscreen"
  },

  // Profile for sites using keyboard fullscreen with video players embedded in iframes. The video element is not directly in the main page DOM, so we need to search
  // through all frames to find it. Once found, the player responds to the standard f key for fullscreen.
  keyboardIframe: {

    category: "keyboard",
    description: "Keyboard fullscreen sites with video embedded in iframes.",
    extends: "keyboardFullscreen",
    needsIframeHandling: true,
    summary: "Iframe players ('f' key fullscreen)"
  },

  // Profile for sites using keyboard fullscreen that load multiple video elements simultaneously - placeholder videos, ad videos, and the main content. We must find
  // the video element that has actually loaded playable data (readyState >= 3) rather than just taking the first video element.
  keyboardMultiVideo: {

    category: "keyboard",
    description: "Keyboard fullscreen sites with multiple video elements requiring ready-state selection.",
    extends: "keyboardFullscreen",
    selectReadyVideo: true,
    summary: "Multi-video sites ('f' key fullscreen)"
  },

  // Profile for non-video pages that should be captured as static visual content. Examples include weather displays (weatherscan.net), maps (windy.com), and
  // diagnostic pages. The noVideo flag tells the streaming code not to wait for a video element or set up playback monitoring - just capture whatever is displayed.
  staticPage: {

    category: "special",
    description: "Base profile for non-video pages captured as static visual content.",
    noVideo: true,
    summary: "Static pages (no video)"
  }
};

/*
 * This mapping associates domain patterns with profile names for automatic profile detection. When resolving a profile for a URL, we check if the URL contains any
 * of these domain strings. Using string containment rather than exact hostname matching allows us to handle:
 *
 * - Subdomains: "www.nbc.com" matches "nbc.com"
 * - Path-based matching: "example.com/live" would match "example.com"
 * - Regional variations: "nbc.com.au" would match "nbc.com"
 *
 * The mapping is checked in Object.keys() order, so more specific domains should be listed if there's potential overlap. For example, if "news.site.com" and
 * "site.com" both existed, the first matching entry would be used.
 *
 * Domains not listed here will use DEFAULT_SITE_PROFILE, which works for most standard video players. Only add entries here when a site requires specific handling.
 */
export const DOMAIN_TO_PROFILE: Record<string, string> = {

  "abc.com": "keyboardMultiVideo",
  "c-span.org": "brightcove",
  "cbs.com": "keyboardIframe",
  "cnbc.com": "fullscreenApi",
  "cnn.com": "fullscreenApi",
  "disneynow.com": "disneyNow",
  "disneyplus.com": "apiMultiVideo",
  "espn.com": "keyboardMultiVideo",
  "foodnetwork.com": "fullscreenApi",
  "foxbusiness.com": "embeddedDynamicMultiVideo",
  "foxnews.com": "embeddedDynamicMultiVideo",
  "foxsports.com": "fullscreenApi",
  "france24.com": "embeddedVolumeLock",
  "hbomax.com": "fullscreenApi",
  "hulu.com": "huluLive",
  "ms.now": "keyboardDynamic",
  "nationalgeographic.com": "keyboardDynamicMultiVideo",
  "nbc.com": "keyboardDynamic",
  "paramountplus.com": "fullscreenApi",
  "sling.com": "embeddedVolumeLock",
  "tbs.com": "fullscreenApi",
  "tntdrama.com": "fullscreenApi",
  "usanetwork.com": "keyboardDynamicMultiVideo",
  "vh1.com": "fullscreenApi",
  "weatherscan.net": "staticPage",
  "windy.com": "staticPage",
  "wttw.com": "fullscreenApi",
  "youtube.com": "keyboardDynamic"
};

/* The default profile provides baseline behavior for sites not explicitly listed in the domain mapping or channel definitions. These settings work for most
 * standard HTML5 video players that follow common conventions. Each flag is explicitly set to its default value for documentation purposes and to ensure
 * predictable behavior - we don't rely on implicit defaults.
 *
 * Sites matching the default profile:
 * - Use standard HTML5 video without iframe embedding
 * - Have a single video element on the page
 * - Don't require clicking to start playback
 * - Don't auto-mute aggressively
 * - Don't require waiting for network activity
 * - Have video content (not static pages)
 *
 * Neither keyboard fullscreen nor API fullscreen is enabled by default because many sites work fine without explicit fullscreen triggering - the video is already
 * displayed at full size in the viewport. Fullscreen is only needed when the player has visible controls or surrounding content that we want to hide.
 */

export const DEFAULT_SITE_PROFILE: ResolvedSiteProfile = {

  // No channel selection - single-channel sites don't need it.
  channelSelection: { strategy: "none" },

  // No channel selector - this is only used for multi-channel player pages.
  channelSelector: null,

  // No click selector - when clickToPlay is true, click the video element by default.
  clickSelector: null,

  // Don't click to play - most sites start automatically or via other mechanisms.
  clickToPlay: false,

  // No fullscreen key - many players work without explicit fullscreen.
  fullscreenKey: null,

  // No fullscreen button selector - most sites don't have a dedicated fullscreen button to click.
  fullscreenSelector: null,

  // Don't lock volume properties - most sites don't aggressively mute.
  lockVolumeProperties: false,

  // Don't search iframes - assume video is in main page DOM.
  needsIframeHandling: false,

  // Expect video content - wait for video element.
  noVideo: false,

  // Use first video element - assume only one video exists.
  selectReadyVideo: false,

  // Don't use requestFullscreen() API.
  useRequestFullscreen: false,

  // Don't wait for network idle - assume player is ready on page load.
  waitForNetworkIdle: false
};

/*
 * Profile resolution is the process of determining which behavior flags to use for a given stream. The resolution process handles inheritance, merging parent and
 * child profile properties, and falling back to defaults for unspecified flags.
 *
 * Resolution order (highest to lowest priority):
 *
 * 1. Channel's explicit profile property (if specified)
 * 2. URL-based detection via DOMAIN_TO_PROFILE
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
 * Resolves the site profile for a given URL by checking the domain against DOMAIN_TO_PROFILE. This function handles partial domain matching to support subdomains
 * and path variations. Falls back to the default profile if no matching domain is found.
 *
 * The matching is done by checking if the URL contains the domain string anywhere. This is simpler than hostname extraction and handles edge cases like URLs with
 * unusual formatting.
 *
 * @param url - The URL to resolve a profile for.
 * @returns The site profile containing behavior flags.
 */
export function getProfileForUrl(url: string | undefined): ProfileResolutionResult {

  // No URL provided - return the default.
  if(!url) {

    return { profile: { ...DEFAULT_SITE_PROFILE }, profileName: "default" };
  }

  // Check each known domain pattern to see if the URL contains it. The first match wins, so more specific domains should be listed first in DOMAIN_TO_PROFILE
  // if there's potential overlap.
  for(const domain of Object.keys(DOMAIN_TO_PROFILE)) {

    if(url.indexOf(domain) !== -1) {

      const profileName = DOMAIN_TO_PROFILE[domain];

      return { profile: resolveProfile(profileName), profileName };
    }
  }

  // No matching domain found - use the default profile.
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

  // Validate all domain-to-profile mappings reference existing profiles. This catches typos in DOMAIN_TO_PROFILE.
  for(const [ domain, domainProfileName ] of Object.entries(DOMAIN_TO_PROFILE)) {

    const domainProfile = SITE_PROFILES[domainProfileName] as SiteProfile | undefined;

    if(!domainProfile) {

      errors.push([ "Domain ", domain, " references non-existent profile: ", domainProfileName ].join(""));
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
