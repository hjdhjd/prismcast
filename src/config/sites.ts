/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * sites.ts: Site profile definitions and domain-to-profile mappings for PrismCast.
 */
import type { ResolvedSiteProfile, SiteProfile } from "../types/index.js";

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

/**
 * Domain-level configuration associating domain patterns with site profiles and provider display names. Each entry can specify a site profile for behavior
 * configuration and/or a provider display name for friendly UI labels.
 */
export interface DomainConfig {

  // Site profile name for automatic profile detection. When a URL matches this domain, the specified profile is used to configure site-specific behavior
  // (fullscreen method, iframe handling, etc.). Omit for domains that only need a display name.
  profile?: string;

  // Friendly provider name shown in the UI source column and provider labels. When set, this name is used instead of the raw domain string (e.g., "Hulu" instead
  // of "hulu.com"). Omit to fall back to the concise domain extracted from the URL.
  provider?: string;
}

/* This mapping associates concise domain keys with site profiles and provider display names. Keys are second-level domains (e.g., "nbc.com", "foodnetwork.com") that
 * match the output of extractDomain() — the last two hostname parts of a URL. Both getProfileForUrl() and getProviderDisplayName() use extractDomain() to derive
 * the lookup key, ensuring consistent matching semantics across profile resolution and display name resolution.
 *
 * Domains without a profile entry will use DEFAULT_SITE_PROFILE, which works for most standard video players. Domains without a provider entry will display the
 * concise domain string (e.g., "hulu.com") in the UI.
 */
export const DOMAIN_CONFIG: Record<string, DomainConfig> = {

  "abc.com": { profile: "keyboardMultiVideo", provider: "ABC.com" },
  "aetv.com": { provider: "A&E" },
  "bet.com": { profile: "fullscreenApi", provider: "BET.com" },
  "c-span.org": { profile: "brightcove", provider: "C-SPAN.org" },
  "cbs.com": { profile: "keyboardIframe", provider: "CBS.com" },
  "cnbc.com": { profile: "fullscreenApi", provider: "CNBC.com" },
  "cnn.com": { profile: "fullscreenApi", provider: "CNN.com" },
  "disneynow.com": { profile: "disneyNow", provider: "DisneyNOW" },
  "disneyplus.com": { profile: "apiMultiVideo", provider: "Disney+" },
  "espn.com": { profile: "keyboardMultiVideo", provider: "ESPN.com" },
  "foodnetwork.com": { profile: "fullscreenApi", provider: "Food Network" },
  "foxbusiness.com": { profile: "embeddedDynamicMultiVideo", provider: "Fox Business" },
  "foxnews.com": { profile: "embeddedDynamicMultiVideo", provider: "Fox News" },
  "foxsports.com": { profile: "fullscreenApi", provider: "Fox Sports" },
  "france24.com": { profile: "embeddedVolumeLock", provider: "France 24" },
  "fyi.tv": { provider: "FYI" },
  "golfchannel.com": { profile: "fullscreenApi", provider: "Golf Channel" },
  "hbomax.com": { profile: "fullscreenApi", provider: "HBO Max" },
  "history.com": { provider: "History.com" },
  "hulu.com": { profile: "huluLive", provider: "Hulu" },
  "lakeshorepbs.org": { profile: "embeddedPlayer", provider: "Lakeshore PBS" },
  "ms.now": { profile: "keyboardDynamic", provider: "MSNOW" },
  "mylifetime.com": { provider: "Lifetime" },
  "nationalgeographic.com": { profile: "keyboardDynamicMultiVideo", provider: "Nat Geo" },
  "nbc.com": { profile: "keyboardDynamic", provider: "NBC.com" },
  "paramountplus.com": { profile: "fullscreenApi", provider: "Paramount+" },
  "sling.com": { profile: "embeddedVolumeLock", provider: "Sling TV" },
  "tbs.com": { profile: "fullscreenApi", provider: "TBS.com" },
  "tntdrama.com": { profile: "fullscreenApi", provider: "TNT" },
  "trutv.com": { profile: "fullscreenApi", provider: "truTV" },
  "usanetwork.com": { profile: "keyboardDynamicMultiVideo", provider: "USA Network" },
  "vh1.com": { profile: "fullscreenApi", provider: "VH1.com" },
  "weatherscan.net": { profile: "staticPage", provider: "Weatherscan" },
  "windy.com": { profile: "staticPage", provider: "Windy" },
  "wttw.com": { profile: "fullscreenApi", provider: "WTTW" },
  "youtube.com": { profile: "keyboardDynamic", provider: "YouTube" }
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

  // No fullscreen button selector - most sites don't have a dedicated fullscreen button we need to click.
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
