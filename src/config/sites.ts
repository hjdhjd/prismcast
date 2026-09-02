/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * sites.ts: Site profile definitions and domain-to-profile mappings for PrismCast.
 */
import type { DomainConfig, ResolvedSiteProfile, SiteProfile } from "../types/index.ts";
import { extractDomain } from "../utils/index.ts";
import { getUserDomains } from "./userProfiles.ts";

/* Streaming sites implement their video players in wildly different ways. Some use standard HTML5 video with keyboard shortcuts, others embed players in iframes,
 * and many have unique quirks like auto-muting or requiring specific fullscreen methods. Rather than scattering site-specific conditionals throughout the streaming
 * code, we define "site profiles" that describe each site's behavior in a declarative way.
 *
 * The profile system has four components:
 *
 * 1. SITE_PROFILES: General-purpose behavior configurations that users can select for custom channels. These describe common player implementation patterns and
 *    are shown in UI dropdowns and the service wizard. Profiles can inherit from other profiles using the "extends" property.
 *
 * 2. PROVIDER_PROFILES: Internal profiles tied to specific provider modules (Hulu, YouTube TV, Sling, etc.). These have channel selection strategies and
 *    selectors tightly coupled to a streaming service's DOM structure. They are never shown in user-facing profile lists - users targeting these services should
 *    use the predefined channels directly. Provider profiles can extend general profiles (e.g., fullscreenApi) and profile resolution checks both tables.
 *
 * 3. DOMAIN_CONFIG: A mapping from domain patterns to site profiles and service display names. When streaming a URL, we check if it matches any known domain and
 *    use the corresponding profile. Service display names give friendly labels (e.g., "Hulu" instead of "hulu.com") for the UI source column and service
 *    dropdowns. This is the primary mechanism for automatically selecting the right behavior and generating friendly display names.
 *
 * 4. Channel-level profile hints: Individual channel definitions can specify an explicit profile name, overriding URL-based detection. This is useful when a
 *    channel's URL doesn't match the expected domain pattern, or when the same domain serves multiple channel types that need different handling.
 *
 * Profile resolution happens at stream startup and the resolved profile is passed through the entire streaming pipeline. The profile flags control:
 * - Which native fullscreen mechanism a profile may invoke (a keyboard shortcut, the player's own control, or the JavaScript API)
 * - Whether to search for video elements in iframes
 * - Which video element to select when multiple exist
 * - Whether to wait for network activity to settle before playback
 * - Whether to lock volume properties to prevent auto-muting
 * - Whether the page is static content (no video element expected)
 *
 * Fullscreen is a family name here rather than an action. Each family names the native mechanism the site's player supports, and whether a profile actually
 * invokes that mechanism depends on whose layout owns the player's box. Wherever the video sits in a page we can style, PrismCast fills the capture by styling
 * the video to the viewport (applyVideoStyles in browser/video.ts), which works from a background tab and leaves whatever application the user has in front of
 * them where it is. The recording's dimensions come from the quality preset's emulated capture surface, not from the browser window, so the styling costs the
 * picture nothing there. The iframe families are the exception: styling applied inside the frame fills the frame, but the iframe element's box on the top page
 * belongs to the site's own layout and nothing done from within reaches it, so those profiles invoke the native mechanism their players expose. A provider
 * profile opts into the native path where its player requires one, and a custom profile turns a native trigger on for a single site; every one of them does it
 * through the option that carries the mechanism - fullscreenKey for the keypress, fullscreenSelector for the player's own control, useRequestFullscreen for the
 * JavaScript API.
 *
 * When adding support for a new streaming site, first check if an existing profile matches its behavior. Only create a new profile if the site requires unique
 * handling not covered by existing profiles.
 */

/* Each profile defines a set of behavior flags that control how we interact with the video player. Profiles are organized in an inheritance hierarchy based on
 * behavior patterns rather than site ownership. This makes it easier to identify the right profile when adding new channels.
 *
 * Base profiles (no extends):
 * - keyboardFullscreen: Sites whose player toggles fullscreen on the f key
 * - fullscreenApi: Sites whose player exposes the JavaScript requestFullscreen() API
 * - staticPage: Non-video pages captured as static visual content
 *
 * General derived profiles (extends a base, user-selectable):
 * - keyboardDynamic: Keyboard-fullscreen players + network idle wait (extends keyboardFullscreen)
 * - keyboardMultiVideo: Keyboard-fullscreen players + multi-video selection (extends keyboardFullscreen)
 * - keyboardIframe: Keyboard-fullscreen players + iframe handling (extends keyboardFullscreen)
 * - keyboardDynamicMultiVideo: Keyboard + network idle + multi-video selection via matchSelector (extends keyboardDynamic)
 * - clickToPlayKeyboard: Click to start playback, keyboard-fullscreen player (extends keyboardFullscreen)
 * - brightcove: Brightcove players + network idle wait (extends fullscreenApi)
 * - clickToPlayApi: Click to start playback, API-fullscreen player (extends fullscreenApi)
 * - embeddedPlayer: Iframe-based API-fullscreen players (extends fullscreenApi)
 * - apiMultiVideo: API-fullscreen player + multi-video + auto-play tile channel selection via matchSelector (extends fullscreenApi)
 * - embeddedDynamicMultiVideo: Embedded + network idle + multi-video selection (extends embeddedPlayer)
 * - embeddedVolumeLock: Embedded + volume property locking (extends embeddedPlayer)
 *
 * Provider profiles (internal, not user-selectable - in PROVIDER_PROFILES table or registered by provider modules):
 * - coxStream, directvStream, disneyNow, disneyPlus, foxLive, hboMax, huluLive, slingLive, spectrum, xfinityStream, youtubeTV
 *
 * Each profile includes a description field documenting its purpose. This is metadata only - it's stripped during profile resolution and exists purely for
 * documentation.
 */
export const SITE_PROFILES: Record<string, SiteProfile> = {

  // Profile for multi-channel live TV pages that present a shelf of live channel tiles where clicking a tile auto-plays the selected channel. Multi-video
  // selection finds the actively playing stream after channel selection. Does not use iframe handling or network idle wait because these sites serve video
  // directly in the main page and have persistent connections that prevent network idle. No playSelector - tile click is the final action.
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
  // it's ready. The player intercepts keyboard events for its own controls, so its native fullscreen is the JavaScript API. Uses selectReadyVideo because
  // pages may have multiple video elements (preroll ads alongside the main player), and the ad video reaches readyState >= 3 before the main player.
  brightcove: {

    category: "api",
    description: "Brightcove player sites requiring a network idle wait before the player is ready.",
    extends: "fullscreenApi",
    selectReadyVideo: true,
    summary: "Brightcove players (network wait)",
    waitForNetworkIdle: true
  },

  // Profile for sites that require clicking to start playback, on players whose native fullscreen is the JavaScript API. Some players don't autoplay and need
  // user interaction to begin. Set clickSelector in the profile or channel definition to specify a play button element; otherwise clicks the video element
  // directly.
  clickToPlayApi: {

    category: "api",
    clickToPlay: true,
    description: "Sites requiring a click to start playback, on players that expose the JavaScript fullscreen API. Use clickSelector for play button overlays.",
    extends: "fullscreenApi",
    summary: "Click-to-play (fullscreen API player)"
  },

  // Profile for sites that require clicking to start playback, on players that toggle fullscreen with the 'f' key. Use this rather than clickToPlayApi when the
  // site's player offers no requestFullscreen() support. Set clickSelector in the profile or channel definition to specify a play button element.
  clickToPlayKeyboard: {

    category: "keyboard",
    clickToPlay: true,
    description: "Sites requiring a click to start playback, on players that toggle fullscreen with the 'f' key. Use clickSelector for play button overlays.",
    extends: "keyboardFullscreen",
    summary: "Click-to-play ('f' key player)"
  },

  // Profile for iframe-embedded players that also have multiple video elements (ads, placeholders, main content) and need network activity to settle. The
  // selectReadyVideo flag ensures we find the video with actual content rather than an ad placeholder. Extends embeddedPlayer, so it sits in the API family.
  embeddedDynamicMultiVideo: {

    category: "api",
    description: "Iframe-embedded players with multiple video elements requiring network idle wait.",
    extends: "embeddedPlayer",
    selectReadyVideo: true,
    summary: "Embedded multi-video (network wait)",
    waitForNetworkIdle: true
  },

  /* Intermediate profile for sites that embed their player in an iframe and whose player exposes the JavaScript fullscreen API. Many modern players use this
   * architecture to isolate ad content, and they answer to programmatic fullscreen rather than to keyboard shortcuts.
   *
   * This family calls the API rather than relying on the capture styling alone. The styling reaches the video inside the frame and fills it, but the iframe
   * element's own box on the top page is whatever the site sized it to - a centered player a fraction of the capture surface on many of these sites - and no
   * styling applied from inside the frame can grow it. The API call is what takes the picture past that box, and every profile extending this one inherits it.
   */
  embeddedPlayer: {

    category: "api",
    description: "Intermediate base profile for iframe-embedded players that expose the fullscreen API.",
    extends: "fullscreenApi",
    needsIframeHandling: true,
    summary: "Embedded iframe players",
    useRequestFullscreen: true
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

  // Base profile for sites whose player exposes the JavaScript fullscreen API (element.requestFullscreen()) rather than a keyboard shortcut. Many modern players
  // intercept keyboard events for their own controls, so the f key does nothing on them and the API is the mechanism a custom profile would reach for here.
  fullscreenApi: {

    category: "api",
    description: "Base profile for sites whose player exposes the JavaScript fullscreen API.",
    summary: "Player exposes the fullscreen API"
  },

  // Profile for sites that use keyboard fullscreen and also need time for network activity to settle before the player is fully initialized. These sites dynamically
  // load their player and content. The waitForNetworkIdle flag ensures we don't try to interact with the player until all initial network requests have completed.
  keyboardDynamic: {

    category: "keyboard",
    description: "Sites whose player toggles fullscreen with the 'f' key, requiring a network idle wait for dynamic content loading.",
    extends: "keyboardFullscreen",
    summary: "Dynamic sites ('f' key player)",
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

  // Base profile for sites whose player toggles fullscreen on the f key, following YouTube-style keyboard shortcuts. This is the most common native mechanism
  // and covers most standard video players, so it is the family a site lands in when its player offers no other fullscreen affordance.
  keyboardFullscreen: {

    category: "keyboard",
    description: "Base profile for sites whose player toggles fullscreen on the f key.",
    summary: "Player toggles fullscreen on 'f'"
  },

  /* Profile for iframe-embedded video players whose native fullscreen is the f key. The video element is not directly in the main page DOM, so we need to search
   * through all frames to find it.
   *
   * The keypress is this family's answer to the same geometry the API answers for embeddedPlayer: the capture styling fills the frame with the video, but the
   * iframe element's box on the top page is the site's to size and no styling from within the frame reaches it. Sending the key to the player is what takes the
   * picture past that box and onto the full capture surface.
   */
  keyboardIframe: {

    category: "keyboard",
    description: "Sites with video embedded in iframes, on players that toggle fullscreen with the 'f' key.",
    extends: "keyboardFullscreen",
    fullscreenKey: "f",
    needsIframeHandling: true,
    summary: "Iframe sites ('f' key player)"
  },

  // Profile for sites using keyboard fullscreen that load multiple video elements simultaneously - placeholder videos, ad videos, and the main content. We must find
  // the video element that has actually loaded playable data (readyState >= 3) rather than just taking the first video element.
  keyboardMultiVideo: {

    category: "keyboard",
    description: "Sites with multiple video elements requiring ready-state selection, on players that toggle fullscreen with the 'f' key.",
    extends: "keyboardFullscreen",
    selectReadyVideo: true,
    summary: "Multi-video sites ('f' key player)"
  },

  // Profile for non-video pages captured as static visual content. Examples include weather displays (weatherscan.net), maps (windy.com), and diagnostic pages.
  // The staticCapture flag tells the streaming code to navigate directly and skip video element detection, playback initialization, and health monitoring.
  staticPage: {

    category: "special",
    description: "Base profile for non-video pages captured as static visual content.",
    staticCapture: true,
    summary: "Static pages (no video)"
  }
};

/* Provider profiles are internal profiles tied to specific provider modules. They have channel selection strategies, selectors, and flags tightly coupled to a
 * specific streaming service's DOM structure and are not user-selectable. They live in a separate table from SITE_PROFILES so that UI-facing code (profile
 * dropdowns, service wizard, profile validation) can show only general-purpose profiles without maintaining an exclusion list. The extends mechanism works across
 * both tables - a provider profile can extend a general profile (e.g., fullscreenApi) and profile resolution checks both tables transparently.
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
  // (playSelector) that must be clicked to start the stream. The player uses Web Components with Shadow DOM for its controls, and its native <toggle-fullscreen>
  // button sits behind that Shadow DOM boundary where Puppeteer cannot click it, which is why the profile carries no fullscreenSelector. The controls toolbar is
  // hidden via hideSelector to prevent it from appearing in the captured stream. Uses selectReadyVideo because the page has multiple video elements (previews,
  // ads, main content).
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


/* This mapping associates domain keys with site profiles, service display names, and service filter tags. Most keys are concise second-level domains
 * (e.g., "nbc.com", "foodnetwork.com") matching the output of extractDomain(). Keys can also be full hostnames (e.g., "tv.youtube.com") for subdomain-specific
 * overrides - getDomainConfig() tries the full hostname first, then falls back to the concise domain, so "tv.youtube.com" takes precedence over "youtube.com"
 * when the URL matches.
 *
 * Domains without a profile entry will use DEFAULT_SITE_PROFILE, which works for most standard video players. Domains with no service field configured, or with
 * no entry in this map at all, will display the concise domain string (e.g., a hypothetical "example.com") in the UI. Entries with a serviceTag participate in
 * the service filter system - channels whose canonical URL maps to a tagged domain are identified as belonging to that subscription service rather than being
 * tagged as "direct" (free network sites).
 */
export const DOMAIN_CONFIG: Record<string, DomainConfig> = {

  "abc.com": { profile: "keyboardMultiVideo", service: "ABC.com" },
  "aetv.com": { profile: "fullscreenApi", service: "A&E" },
  "bet.com": { profile: "fullscreenApi", service: "BET.com" },
  "c-span.org": { dismissSelector: ".videoAdUiSkipButtonExperimentalText", profile: "brightcove", service: "C-SPAN.org" },
  "cbs.com": { dismissSelector: "#mvpd__getstarted", profile: "keyboardIframe", service: "CBS.com" },
  "cnbc.com": { profile: "fullscreenApi", service: "CNBC.com" },
  "cnn.com": { profile: "fullscreenApi", service: "CNN.com" },
  "disneynow.com": { profile: "disneyNow", service: "DisneyNOW" },
  "disneyplus.com": { iconUrl: "https://static-assets.bamgrid.com/product/disneyplus/favicons/apple-touch-icon-aurora.d3af81fe0571b495a3c80ff8c3d0c8e7.png",
    profile: "disneyPlus", service: "Disney+", serviceTag: "disneyplus" },
  "espn.com": { profile: "keyboardMultiVideo", service: "ESPN.com" },
  "foodnetwork.com": { profile: "fullscreenApi", service: "Food Network" },
  "fox.com": { loginUrl: "https://www.fox.com", profile: "foxLive", service: "Fox One", serviceTag: "foxone" },
  "foxbusiness.com": { profile: "embeddedDynamicMultiVideo", service: "Fox Business" },
  "foxnews.com": { profile: "embeddedDynamicMultiVideo", service: "Fox News" },
  "foxsports.com": { profile: "fullscreenApi", service: "Fox Sports" },
  "france24.com": { profile: "embeddedVolumeLock", service: "France 24" },
  "freeform.com": { profile: "fullscreenApi", service: "Freeform" },
  "fyi.tv": { profile: "fullscreenApi", service: "FYI" },
  "golfchannel.com": { profile: "fullscreenApi", service: "Golf Channel" },
  "hbomax.com": { profile: "hboMax", service: "HBO Max", serviceTag: "hbomax" },
  "history.com": { profile: "fullscreenApi", service: "History.com" },
  "hulu.com": { iconUrl: "https://www.hulu.com/static/icons/apple-touch-icon.png", profile: "huluLive", service: "Hulu", serviceTag: "hulu" },
  "lakeshorepbs.org": { profile: "embeddedPlayer", service: "Lakeshore PBS" },
  "ms.now": { profile: "keyboardDynamic", service: "MS NOW" },
  "mylifetime.com": { profile: "fullscreenApi", service: "Lifetime" },
  "nationalgeographic.com": { profile: "keyboardDynamicMultiVideo", service: "Nat Geo" },
  "nba.com": { profile: "fullscreenApi", service: "NBA.com" },
  // NBC.com enforces a session limit that cuts the stream after roughly four continuous hours of playback, so maxContinuousPlayback (measured in hours) drives a
  // proactive page reload just before that cap is reached.
  "nbc.com": { maxContinuousPlayback: 4, profile: "keyboardDynamic", service: "NBC.com" },
  "paramountplus.com": { dismissSelector: ".ppp-watch", iconUrl: "https://www.paramountplus.com/assets/images/pplus_App_Icon-Blue-144x144.png",
    profile: "fullscreenApi", service: "Paramount+", serviceTag: "paramountplus" },
  "sling.com": { profile: "embeddedVolumeLock", service: "Sling TV" },
  "starz.com": { profile: "fullscreenApi", service: "Starz" },
  "stream.directv.com": { loginUrl: "https://stream.directv.com", profile: "directvStream", service: "DirecTV Stream", serviceTag: "directv" },
  "tbs.com": { profile: "fullscreenApi", service: "TBS.com" },
  "tntdrama.com": { profile: "fullscreenApi", service: "TNT" },
  "trutv.com": { profile: "fullscreenApi", service: "truTV" },
  "tv.youtube.com": { iconUrl: "https://www.youtube.com/yts/img/favicon_144-vfliLAfaB.png", profile: "youtubeTV", service: "YouTube TV", serviceTag: "yttv" },
  "usanetwork.com": { iconUrl: "https://usanetwork.asset.viewlift.com/images/brand/2025/09/30/usa_favicon-1759230411162.ico",
    profile: "keyboardDynamicMultiVideo", service: "USA Network", serviceTag: "usa" },
  "vh1.com": { profile: "fullscreenApi", service: "VH1.com" },
  "watch.sling.com": { profile: "slingLive", service: "Sling TV", serviceTag: "sling" },
  "watch.spectrum.net": { iconUrl: "https://watch.spectrum.net/assets/17.28.0/images/apple-touch-icon.png", profile: "spectrum", service: "Spectrum TV",
    serviceTag: "spectrum" },
  "watchhallmarktv.com": { profile: "fullscreenApi", service: "Hallmark" },
  "watchtv.cox.com": { iconUrl: "https://watchtv.cox.com/partners/cox/images/favicon-128x128.png", profile: "coxStream", service: "Cox Contour TV",
    serviceTag: "cox" },
  "weatherscan.net": { profile: "staticPage", service: "Weatherscan" },
  "windy.com": { profile: "staticPage", service: "Windy" },
  "wttw.com": { profile: "fullscreenApi", service: "WTTW" },
  "xfinity.com": { profile: "xfinityStream", service: "Xfinity Stream", serviceTag: "xfinity" },
  "youtube.com": { profile: "keyboardDynamic", service: "YouTube" }
};

/**
 * Resolves a URL to its domain configuration entry by precedence: user full hostname, then builtin full hostname (for subdomain-specific overrides), then user
 * concise domain, then builtin concise domain (last two hostname parts). User mappings override builtins, and full-hostname entries like "tv.youtube.com" override
 * the base "youtube.com" entry.
 * @param url - The URL to resolve a domain configuration for.
 * @returns The matching DomainConfig entry, or undefined if no match is found.
 */
export function getDomainConfig(url: string): DomainConfig | undefined {

  // User domain mappings take precedence over builtin mappings. The lookup order is: user full hostname -> builtin full hostname -> user concise domain ->
  // builtin concise domain. This allows users to override specific subdomain mappings or base domain mappings independently.
  const userDomains = getUserDomains();

  try {

    const hostname = new URL(url).hostname;

    // Try user domains for the full hostname first.
    const userHostnameMatch = userDomains[hostname];

    if(userHostnameMatch) {

      return userHostnameMatch;
    }

    // Try the builtin full hostname for subdomain-specific overrides (e.g., "tv.youtube.com" before "youtube.com").
    const hostnameMatch = DOMAIN_CONFIG[hostname];

    if(hostnameMatch) {

      return hostnameMatch;
    }
  } catch {

    // Invalid URL - fall through to concise domain lookup.
  }

  const conciseDomain = extractDomain(url);

  // Try user domains for the concise domain.
  const userConciseMatch = userDomains[conciseDomain];

  if(userConciseMatch) {

    return userConciseMatch;
  }

  return DOMAIN_CONFIG[conciseDomain];
}

// Provider module profiles registered at import time via registerProviderModuleProfile(). Provider modules define their profiles alongside their tuning code and
// register them here so the profile resolution system can find them without importing from browser/channelSelection.ts (which would create circular dependencies).
const providerModuleProfiles = new Map<string, SiteProfile>();

/**
 * Registers a provider module's profile. Called by the coordinator in channelSelection.ts at module evaluation time to make provider profiles available to the
 * profile resolution system. This avoids circular dependencies - sites.ts doesn't need to import from browser/ modules.
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
 * Looks up a builtin profile by name, checking the general SITE_PROFILES table, the static PROVIDER_PROFILES table, and dynamically registered provider module
 * profiles. This is the single lookup function for all builtin profile resolution - callers should use this instead of accessing any table directly.
 * @param name - The profile name to look up.
 * @returns The matching SiteProfile, or undefined if not found in any source.
 */
export function getBuiltinProfile(name: string): SiteProfile | undefined {

  return (SITE_PROFILES[name]) ?? (PROVIDER_PROFILES[name]) ?? providerModuleProfiles.get(name);
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
 * All three native fullscreen triggers are off here, and that is the default every builtin profile inherits: the capture is filled by CSS styling, and a custom
 * profile is where a single site turns a trigger on. The module walkthrough above states the rule in full.
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

  // No fullscreen key - CSS styling fills the capture, so no keypress is sent.
  fullscreenKey: null,

  // No fullscreen button selector - a player's own fullscreen control is clicked only where a profile names one.
  fullscreenSelector: null,

  // No overlay hiding - most sites don't have persistent overlays during fullscreen.
  hideSelector: null,

  // Don't lock volume properties - most sites don't aggressively mute.
  lockVolumeProperties: false,

  // No continuous playback limit - most sites allow indefinite streaming.
  maxContinuousPlayback: null,

  // Don't search iframes - assume video is in main page DOM.
  needsIframeHandling: false,

  // Use first video element - assume only one video exists.
  selectReadyVideo: false,

  // Not a static page capture - wait for video element and monitor playback.
  staticCapture: false,

  // No requestFullscreen() call - CSS styling fills the capture.
  useRequestFullscreen: false,

  // No per-domain video timeout override - use the global default.
  videoTimeout: null,

  // Don't wait for network idle - assume player is ready on page load.
  waitForNetworkIdle: false
};
