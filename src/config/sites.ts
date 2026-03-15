/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * sites.ts: Site profile definitions and domain-to-profile mappings for PrismCast.
 */
import type { DomainConfig, ResolvedSiteProfile, SiteProfile } from "../types/index.js";
import { extractDomain } from "../utils/index.js";
import { getUserDomains } from "./userProfiles.js";

/* Streaming sites implement their video players in wildly different ways. Some use standard HTML5 video with keyboard shortcuts, others embed players in iframes,
 * and many have unique quirks like auto-muting or requiring specific fullscreen methods. Rather than scattering site-specific conditionals throughout the streaming
 * code, we define "site profiles" that describe each site's behavior in a declarative way.
 *
 * The profile system has four components:
 *
 * 1. SITE_PROFILES: General-purpose behavior configurations that users can select for custom channels. These describe common player implementation patterns and
 *    are shown in UI dropdowns and the provider wizard. Profiles can inherit from other profiles using the "extends" property.
 *
 * 1b. PROVIDER_PROFILES: Internal profiles tied to specific provider modules (Hulu, YouTube TV, Sling, etc.). These have channel selection strategies and
 *     selectors tightly coupled to a streaming service's DOM structure. They are never shown in user-facing profile lists — users targeting these services should
 *     use the predefined channels directly. Provider profiles can extend general profiles (e.g., fullscreenApi) and profile resolution checks both tables.
 *
 * 2. DOMAIN_CONFIG: A mapping from domain patterns to site profiles and provider display names. When streaming a URL, we check if it matches any known domain and
 *    use the corresponding profile. Provider display names give friendly labels (e.g., "Hulu" instead of "hulu.com") for the UI source column and provider
 *    dropdowns. This is the primary mechanism for automatically selecting the right behavior and generating friendly display names.
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

/* Each profile defines a set of behavior flags that control how we interact with the video player. Profiles are organized in an inheritance hierarchy based on
 * behavior patterns rather than site ownership. This makes it easier to identify the right profile when adding new channels.
 *
 * Base profiles (no extends):
 * - keyboardFullscreen: Sites using the f key for fullscreen toggle
 * - fullscreenApi: Sites requiring the JavaScript requestFullscreen() API
 * - staticPage: Non-video pages captured as static visual content
 *
 * General derived profiles (extends a base, user-selectable):
 * - keyboardDynamic: Keyboard fullscreen + network idle wait (extends keyboardFullscreen)
 * - keyboardMultiVideo: Keyboard fullscreen + multi-video selection (extends keyboardFullscreen)
 * - keyboardIframe: Keyboard fullscreen + iframe handling (extends keyboardFullscreen)
 * - keyboardDynamicMultiVideo: Keyboard + network idle + multi-video selection via matchSelector (extends keyboardDynamic)
 * - clickToPlayKeyboard: Click to start playback + keyboard fullscreen (extends keyboardFullscreen)
 * - brightcove: Brightcove players using API fullscreen + network idle wait (extends fullscreenApi)
 * - clickToPlayApi: Click to start playback + API fullscreen (extends fullscreenApi)
 * - embeddedPlayer: Iframe-based players using fullscreen API (extends fullscreenApi)
 * - apiMultiVideo: API fullscreen + multi-video + auto-play tile channel selection via matchSelector (extends fullscreenApi)
 * - embeddedDynamicMultiVideo: Embedded + network idle + multi-video selection (extends embeddedPlayer)
 * - embeddedVolumeLock: Embedded + volume property locking (extends embeddedPlayer)
 *
 * Provider profiles (internal, not user-selectable — in PROVIDER_PROFILES table or registered by provider modules):
 * - directvStream, disneyNow, disneyPlus, foxLive, hboMax, huluLive, slingLive, spectrum, xfinityStream, youtubeTV
 *
 * Each profile includes a description field documenting its purpose. This is metadata only - it's stripped during profile resolution and exists purely for
 * documentation.
 */
export const SITE_PROFILES: Record<string, SiteProfile> = {

  // Profile for multi-channel live TV pages that present a shelf of live channel tiles where clicking a tile auto-plays the selected channel. Uses the fullscreen
  // API and multi-video selection to find the actively playing stream after channel selection. Does not use iframe handling or network idle wait because these sites
  // serve video directly in the main page and have persistent connections that prevent network idle. No playSelector — tile click is the final action.
  apiMultiVideo: {

    category: "multiChannel",
    channelSelection: { matchSelector: "img[src*=\"{channel}\" i]", strategy: "tileClick" },
    description: "Multi-channel sites with auto-play tile selection. Channel Selector is interpolated into the matchSelector CSS template to find the channel element.",
    extends: "fullscreenApi",
    selectReadyVideo: true,
    summary: "Multi-channel (auto-play tiles, needs selector)"
  },

  // Profile for sites using the Brightcove player platform. Brightcove players require waiting for network activity to settle before the video player is fully
  // initialized. The player dynamically loads its configuration and stream manifest, so waitForNetworkIdle ensures we don't try to interact with the player before
  // it's ready. Uses the JavaScript fullscreen API rather than keyboard shortcuts because Brightcove intercepts keyboard events. Uses selectReadyVideo because
  // pages may have multiple video elements (pre-roll ads alongside the main player), and the ad video reaches readyState >= 3 before the main player.
  brightcove: {

    category: "api",
    description: "Brightcove player sites requiring network idle wait and API fullscreen.",
    extends: "fullscreenApi",
    selectReadyVideo: true,
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
  // idle wait behavior. Uses thumbnailRow strategy for channel selection (find channel element via matchSelector, click adjacent show entry).
  keyboardDynamicMultiVideo: {

    category: "multiChannel",
    channelSelection: { matchSelector: "img[src*=\"{channel}\" i]", strategy: "thumbnailRow" },
    description: "Multi-channel sites with thumbnail row layout. Channel Selector is interpolated into the matchSelector CSS template to find the channel element.",
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

/* Provider profiles are internal profiles tied to specific provider modules. They have channel selection strategies, selectors, and flags tightly coupled to a
 * specific streaming service's DOM structure and are not user-selectable. They live in a separate table from SITE_PROFILES so that UI-facing code (profile
 * dropdowns, provider wizard, profile validation) can show only general-purpose profiles without maintaining an exclusion list. The extends mechanism works across
 * both tables — a provider profile can extend a general profile (e.g., fullscreenApi) and profile resolution checks both tables transparently.
 */
export const PROVIDER_PROFILES: Record<string, SiteProfile> = {

  // Profile for DisneyNOW (disneynow.com) which has a play button overlay that must be clicked to start playback and multiple video elements on the page.
  disneyNow: {

    category: "api",
    clickSelector: ".overlay__button button",
    description: "DisneyNOW player with play button overlay and multiple video elements.",
    extends: "clickToPlayApi",
    selectReadyVideo: true,
    summary: "DisneyNOW player"
  },

  // Profile for Disney+ live channels. The live channel shelf displays tiles with network logos. Clicking a tile opens an entity modal with a "WATCH LIVE" button
  // (playSelector) that must be clicked to start the stream. Extends fullscreenApi for requestFullscreen() behavior. The player uses Web Components with Shadow DOM
  // for its controls — the native <toggle-fullscreen> button cannot be clicked by Puppeteer (Shadow DOM boundary), so fullscreen is handled entirely by the inherited
  // requestFullscreen() API. The controls toolbar is hidden via hideSelector to prevent it from appearing in the captured stream. Uses selectReadyVideo because the
  // page has multiple video elements (previews, ads, main content).
  disneyPlus: {

    category: "multiChannel",
    channelSelection: {

      matchSelector: "img[src*=\"{channel}\" i]", playSelector: "[data-testid=\"live-modal-watch-live-action-button\"]",
      scrollToBottom: true, strategy: "tileClick"
    },
    description: "Disney+ live channels with tile selection and play button modal. Channel Selector is interpolated into matchSelector to find the element.",
    extends: "fullscreenApi",
    hideSelector: ".controls__footer__wrapper",
    selectReadyVideo: true,
    summary: "Disney+ (tile + play button, needs selector)"
  }
};


/* This mapping associates domain keys with site profiles, provider display names, and provider filter tags. Most keys are concise second-level domains
 * (e.g., "nbc.com", "foodnetwork.com") matching the output of extractDomain(). Keys can also be full hostnames (e.g., "tv.youtube.com") for subdomain-specific
 * overrides — getDomainConfig() tries the full hostname first, then falls back to the concise domain, so "tv.youtube.com" takes precedence over "youtube.com"
 * when the URL matches.
 *
 * Domains without a profile entry will use DEFAULT_SITE_PROFILE, which works for most standard video players. Domains without a provider entry will display the
 * concise domain string (e.g., "hulu.com") in the UI. Entries with a providerTag participate in the provider filter system — channels whose canonical URL maps to
 * a tagged domain are identified as belonging to that subscription service rather than being tagged as "direct" (free network sites).
 */
export const DOMAIN_CONFIG: Record<string, DomainConfig> = {

  "abc.com": { profile: "keyboardMultiVideo", provider: "ABC.com" },
  "aetv.com": { profile: "fullscreenApi", provider: "A&E" },
  "bet.com": { profile: "fullscreenApi", provider: "BET.com" },
  "c-span.org": { dismissSelector: ".videoAdUiSkipButtonExperimentalText", profile: "brightcove", provider: "C-SPAN.org" },
  "cbs.com": { dismissSelector: "#mvpd__getstarted", profile: "keyboardIframe", provider: "CBS.com" },
  "cnbc.com": { profile: "fullscreenApi", provider: "CNBC.com" },
  "cnn.com": { profile: "fullscreenApi", provider: "CNN.com" },
  "disneynow.com": { profile: "disneyNow", provider: "DisneyNOW" },
  "disneyplus.com": { profile: "disneyPlus", provider: "Disney+", providerTag: "disneyplus" },
  "espn.com": { profile: "keyboardMultiVideo", provider: "ESPN.com" },
  "foodnetwork.com": { profile: "fullscreenApi", provider: "Food Network" },
  "fox.com": { loginUrl: "https://www.fox.com", profile: "foxLive", provider: "Fox One", providerTag: "foxcom" },
  "foxbusiness.com": { profile: "embeddedDynamicMultiVideo", provider: "Fox Business" },
  "foxnews.com": { profile: "embeddedDynamicMultiVideo", provider: "Fox News" },
  "foxsports.com": { profile: "fullscreenApi", provider: "Fox Sports" },
  "france24.com": { profile: "embeddedVolumeLock", provider: "France 24" },
  "freeform.com": { profile: "fullscreenApi", provider: "Freeform" },
  "fyi.tv": { profile: "fullscreenApi", provider: "FYI" },
  "golfchannel.com": { profile: "fullscreenApi", provider: "Golf Channel" },
  "hbomax.com": { profile: "hboMax", provider: "HBO Max", providerTag: "hbomax" },
  "history.com": { profile: "fullscreenApi", provider: "History.com" },
  "hulu.com": { profile: "huluLive", provider: "Hulu", providerTag: "hulu" },
  "lakeshorepbs.org": { profile: "embeddedPlayer", provider: "Lakeshore PBS" },
  "ms.now": { profile: "keyboardDynamic", provider: "MS NOW" },
  "mylifetime.com": { profile: "fullscreenApi", provider: "Lifetime" },
  "nationalgeographic.com": { profile: "keyboardDynamicMultiVideo", provider: "Nat Geo" },
  "nba.com": { profile: "fullscreenApi", provider: "NBA.com" },
  "nbc.com": { maxContinuousPlayback: 4, profile: "keyboardDynamic", provider: "NBC.com" },
  "paramountplus.com": { dismissSelector: ".ppp-watch", profile: "fullscreenApi", provider: "Paramount+", providerTag: "paramountplus" },
  "sling.com": { profile: "embeddedVolumeLock", provider: "Sling TV" },
  "starz.com": { profile: "fullscreenApi", provider: "Starz" },
  "stream.directv.com": { loginUrl: "https://stream.directv.com", profile: "directvStream", provider: "DirecTV Stream", providerTag: "directv" },
  "tbs.com": { profile: "fullscreenApi", provider: "TBS.com" },
  "tntdrama.com": { profile: "fullscreenApi", provider: "TNT" },
  "trutv.com": { profile: "fullscreenApi", provider: "truTV" },
  "tv.youtube.com": { profile: "youtubeTV", provider: "YouTube TV", providerTag: "yttv" },
  "usanetwork.com": { profile: "keyboardDynamicMultiVideo", provider: "USA Network", providerTag: "usa" },
  "vh1.com": { profile: "fullscreenApi", provider: "VH1.com" },
  "watch.sling.com": { profile: "slingLive", provider: "Sling TV", providerTag: "sling" },
  "watch.spectrum.net": { profile: "spectrum", provider: "Spectrum TV", providerTag: "spectrum" },
  "watchhallmarktv.com": { profile: "fullscreenApi", provider: "Hallmark" },
  "weatherscan.net": { profile: "staticPage", provider: "Weatherscan" },
  "windy.com": { profile: "staticPage", provider: "Windy" },
  "wttw.com": { profile: "fullscreenApi", provider: "WTTW" },
  "xfinity.com": { profile: "xfinityStream", provider: "Xfinity Stream", providerTag: "xfinity" },
  "youtube.com": { profile: "keyboardDynamic", provider: "YouTube" }
};

/**
 * Resolves a URL to its DOMAIN_CONFIG entry by trying the full hostname first for subdomain-specific overrides, then falling back to the concise domain (last two
 * hostname parts). This allows entries like "tv.youtube.com" to override the base "youtube.com" entry when the URL matches the more-specific subdomain.
 * @param url - The URL to resolve a domain configuration for.
 * @returns The matching DomainConfig entry, or undefined if no match is found.
 */
export function getDomainConfig(url: string): DomainConfig | undefined {

  // User domain mappings take precedence over built-in mappings. The lookup order is: user full hostname → built-in full hostname → user concise domain →
  // built-in concise domain. This allows users to override specific subdomain mappings or base domain mappings independently.
  const userDomains = getUserDomains();

  try {

    const hostname = new URL(url).hostname;

    // Try user domains for the full hostname first.
    const userHostnameMatch = userDomains[hostname] as DomainConfig | undefined;

    if(userHostnameMatch) {

      return userHostnameMatch;
    }

    // Try the built-in full hostname for subdomain-specific overrides (e.g., "tv.youtube.com" before "youtube.com").
    const hostnameMatch = DOMAIN_CONFIG[hostname] as DomainConfig | undefined;

    if(hostnameMatch) {

      return hostnameMatch;
    }
  } catch {

    // Invalid URL — fall through to concise domain lookup.
  }

  const conciseDomain = extractDomain(url);

  // Try user domains for the concise domain.
  const userConciseMatch = userDomains[conciseDomain] as DomainConfig | undefined;

  if(userConciseMatch) {

    return userConciseMatch;
  }

  return DOMAIN_CONFIG[conciseDomain] as DomainConfig | undefined;
}

// Provider module profiles registered at import time via registerProviderModuleProfile(). Provider modules define their profiles alongside their tuning code and
// register them here so the profile resolution system can find them without importing from browser/channelSelection.ts (which would create circular dependencies).
const providerModuleProfiles = new Map<string, SiteProfile>();

/**
 * Registers a provider module's profile. Called by the coordinator in channelSelection.ts at module evaluation time to make provider profiles available to the
 * profile resolution system. This avoids circular dependencies — sites.ts doesn't need to import from browser/ modules.
 * @param name - The profile name (e.g., "huluLive", "slingLive").
 * @param profile - The SiteProfile definition.
 */
export function registerProviderModuleProfile(name: string, profile: SiteProfile): void {

  if((name in SITE_PROFILES) || (name in PROVIDER_PROFILES)) {

    throw new Error("Provider module profile '" + name + "' collides with an existing static profile. Use a unique profileName.");
  }

  providerModuleProfiles.set(name, profile);
}

/**
 * Looks up a built-in profile by name, checking the general SITE_PROFILES table, the static PROVIDER_PROFILES table, and dynamically registered provider module
 * profiles. This is the single lookup function for all built-in profile resolution — callers should use this instead of accessing any table directly.
 * @param name - The profile name to look up.
 * @returns The matching SiteProfile, or undefined if not found in any source.
 */
export function getBuiltinProfile(name: string): SiteProfile | undefined {

  return (SITE_PROFILES[name] as SiteProfile | undefined) ?? (PROVIDER_PROFILES[name] as SiteProfile | undefined) ?? providerModuleProfiles.get(name);
}

/**
 * Returns true if the given profile name is a provider-specific profile (either in the static PROVIDER_PROFILES table or registered by a provider module). Used
 * by user profile validation to prevent users from extending provider-specific profiles that are tightly coupled to a streaming service's DOM structure.
 * @param name - The profile name to check.
 * @returns True if the profile is a provider profile.
 */
export function isProviderProfile(name: string): boolean {

  return (name in PROVIDER_PROFILES) || providerModuleProfiles.has(name);
}

/**
 * Returns all registered provider module profiles as an iterable of [name, profile] pairs. Used by the validation system to include dynamically registered
 * profiles in inheritance chain checks.
 * @returns Iterable of [name, SiteProfile] pairs.
 */
export function getRegisteredProviderModuleProfiles(): IterableIterator<[string, SiteProfile]> {

  return providerModuleProfiles.entries();
}

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

  // No dismiss selector - most sites don't show intermittent modals.
  dismissSelector: null,

  // No fullscreen key - many players work without explicit fullscreen.
  fullscreenKey: null,

  // No fullscreen button selector - most sites don't have a dedicated fullscreen button we need to click.
  fullscreenSelector: null,

  // No overlay hiding - most sites don't have persistent overlays during fullscreen.
  hideSelector: null,

  // Don't lock volume properties - most sites don't aggressively mute.
  lockVolumeProperties: false,

  // No continuous playback limit - most sites allow indefinite streaming.
  maxContinuousPlayback: null,

  // Don't search iframes - assume video is in main page DOM.
  needsIframeHandling: false,

  // Expect video content - wait for video element.
  noVideo: false,

  // Use first video element - assume only one video exists.
  selectReadyVideo: false,

  // Don't use requestFullscreen() API.
  useRequestFullscreen: false,

  // No per-domain video timeout override - use the global default.
  videoTimeout: null,

  // Don't wait for network idle - assume player is ready on page load.
  waitForNetworkIdle: false
};
