/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * profiles.ts: Site profile, channel selection config, and provider pack type definitions for PrismCast.
 */
import type { ChannelMap } from "./channels.js";
import type { Nullable } from "./shared.js";

/* For multi-channel streaming sites (like USA Network), we need to interact with the site's channel selector UI to switch to the desired channel. The channel
 * selection system uses a strategy pattern to support different site implementations. Each strategy encapsulates the logic for finding and clicking the correct
 * channel element. The strategy type and configuration are defined here alongside site profiles because SiteProfile embeds ChannelSelectionConfig directly.
 */

/**
 * Available channel selection strategies. Each strategy implements a different approach to finding and selecting channels in a multi-channel player UI.
 *
 * - "directvGrid": Tune via webpack injection — captures __webpack_require__, extracts the Redux store from the React fiber tree, matches by channel name, and
 *   dispatches playConsumable. Falls back to logo aria-label click when the interceptor fails. Used by DirecTV Stream.
 * - "foxGrid": Find channel by station code in a non-virtualized guide grid, click the channel logo button via DOM .click(). Used by Fox.com.
 * - "guideGrid": Find channel by exact-matching image alt text, click nearest clickable ancestor. Optionally clicks a tab to reveal the list first. Used by Hulu
 *   Live TV.
 * - "hboGrid": Discover the HBO tab page URL from the homepage menu bar, scrape the live channel tile rail for a matching channel name, and navigate to the watch
 *   URL. Caches the tab URL across tunes with stale-cache fallback. Used by HBO Max.
 * - "none": No channel selection needed (single-channel sites). This is the default.
 * - "slingGrid": Find channel by data-testid in a virtualized A-Z guide grid, scroll via binary search on .guide-cell scrollTop, click the on-now program
 *   cell. Used by Sling TV.
 * - "spectrumGrid": Find channel by callsign or display name in a non-virtualized AngularJS guide grid, extract the Gracenote tmsid from channel logo URLs,
 *   and navigate directly to the watch URL. Caches all ~442 streamable channels on first tune. Used by Spectrum TV.
 * - "thumbnailRow": Find channel element using the profile's matchSelector (defaults to image URL matching), click adjacent element on the same row. Used by
 *   USA Network.
 * - "tileClick": Find channel element using the profile's matchSelector (defaults to image URL matching), click tile, then optionally click play button if
 *   playSelector is configured. Used by Disney+.
 * - "youtubeGrid": Find channel by aria-label in a non-virtualized EPG grid, extract the watch URL, and navigate directly. Used by YouTube TV.
 */
export type ChannelSelectionStrategy =
  "directvGrid" | "foxGrid" | "guideGrid" | "hboGrid" | "none" | "slingGrid" | "spectrumGrid" | "thumbnailRow" | "tileClick" | "youtubeGrid";

/**
 * Configuration for channel selection behavior within a site profile.
 */
export interface ChannelSelectionConfig {

  // CSS selector for a tab or button to click to reveal the channel list before selection. Some sites hide the channel list behind a tab (e.g., a "Channels" tab
  // in a guide grid). When set, this element is clicked before searching for channel images. Only used by the guideGrid strategy.
  listSelector?: string;

  // CSS selector template for finding the channel element on the page. The placeholder {channel} is replaced with the channel's channelSelector value at runtime
  // (e.g., "img[src*=\"{channel}\"]" becomes "img[src*=\"espn\"]"). Supports any valid CSS selector — match by image src, aria-label, data-testid, title, or any
  // other attribute. Used by the tileClick and thumbnailRow strategies. When absent, these strategies default to image URL matching (img[src*="..."]).
  matchSelector?: string;

  // CSS selector for a play button that must be clicked after selecting a channel entry. Some sites show a playback action overlay after channel selection instead
  // of immediately starting playback. When set, this element is waited for and clicked after the channel entry click. Used by the guideGrid and tileClick strategies.
  playSelector?: string;

  // CSS selector to narrow the DOM search when scrollTarget is set. Combined with scrollTarget, the scroll phase queries elements matching this selector and then
  // filters to the one whose textContent matches scrollTarget. Example: "h4" to search only heading elements. Can be overridden per channel.
  scrollSelector?: string;

  // Text content to match when scrolling a lazy-loaded section into view before channel selection. Some sites (e.g., Disney+) lazy-load channel shelves that only
  // render when scrolled into the viewport. When set, the scroll phase finds an element whose textContent matches this string, scrolls it into view, and waits
  // briefly for lazy content to render. Must be used together with scrollSelector. Can be overridden per channel.
  scrollTarget?: string;

  // Whether to scroll to the bottom of the page before channel selection. Some sites (e.g., Disney+) lazy-load entire page sections that only appear in the DOM
  // after scrolling. When true, the page is scrolled to the bottom to force all lazy content to render before the matchSelector poll begins. Can be overridden
  // per channel.
  scrollToBottom?: boolean;

  // The strategy to use for finding and clicking channel elements.
  strategy: ChannelSelectionStrategy;
}

/* Site profiles define behavior patterns for different streaming site implementations. Television network streaming sites vary widely in their player
 * implementations: some use keyboard shortcuts for fullscreen, others require the JavaScript Fullscreen API; some embed video in iframes, others place it directly
 * in the page; some auto-mute videos and fight attempts to unmute them. The profile system captures these behavioral differences as configuration rather than
 * code, making it easy to add support for new sites by defining their characteristics.
 *
 * Profiles support inheritance via the "extends" field, allowing common patterns to be defined once and reused. For example, many NBC Universal properties share
 * the same player implementation, so they extend a common "nbcUniversal" base profile.
 */

/**
 * UI category for profile grouping in dropdowns and reference documentation. Profiles are grouped by their fullscreen mechanism and special characteristics.
 * - "api": Profiles using the JavaScript fullscreen API (including embedded iframe and click-to-play variants).
 * - "custom": User-defined profiles created via the profile builder wizard or imported from provider packs.
 * - "keyboard": Profiles using keyboard shortcuts (typically the 'f' key) for fullscreen.
 * - "multiChannel": Multi-channel profiles requiring a channel selector for tile or thumbnail-based channel selection.
 * - "special": Special-purpose profiles like static page capture.
 */
export type ProfileCategory = "api" | "custom" | "keyboard" | "multiChannel" | "special";

/**
 * Site profile definition with optional flags. All flags are optional because profiles can inherit from other profiles, and only the flags that differ from the
 * parent need to be specified. The DEFAULT_SITE_PROFILE provides baseline values for any flags not set through inheritance.
 */
export interface SiteProfile {

  // UI category for grouping this profile in dropdowns and reference documentation. This is metadata only - it's stripped during profile resolution.
  category?: ProfileCategory;

  // Configuration for channel selection behavior on multi-channel sites. When set, determines how to find and click the desired channel in the site's UI. The
  // strategy property specifies the algorithm used to locate the channel element.
  channelSelection?: ChannelSelectionConfig;

  // The channel slug to match when selecting a channel from a multi-channel player. This is the literal string to find in thumbnail image URLs or other channel
  // identifiers. The value typically comes from the channel definition rather than the profile itself.
  channelSelector?: Nullable<string>;

  // CSS selector for the element to click when clickToPlay is true. Some sites have a play button overlay rather than a clickable video element. When set, this
  // selector is clicked instead of the video element. When not set, the video element itself is clicked.
  clickSelector?: Nullable<string>;

  // Whether the video player requires a click to start playback. Brightcove-based players commonly require this. When true, the stream handler clicks the video
  // element (or clickSelector target) after page load and before waiting for the video to become ready. This simulates user interaction to satisfy autoplay policies.
  clickToPlay?: boolean;

  // Human-readable description of the profile for documentation purposes. This field is stripped during profile resolution and not included in the resolved
  // profile passed to stream handling code.
  description?: string;

  // Name of another profile to inherit from. The parent profile's flags are applied first, then this profile's flags override them. Inheritance chains can be
  // multiple levels deep. Circular inheritance is detected and prevented during profile resolution.
  extends?: string;

  // Short summary of the profile for dropdown display (max ~40 characters). Used in the UI to provide a brief description alongside the profile name. Falls back
  // to description if not provided.
  summary?: string;

  // Keyboard key to press for fullscreen mode. Most video players use "f" for fullscreen. When set, the stream handler sends this keypress to the video element
  // after playback begins. Set to null to disable keyboard fullscreen and rely on CSS-based fullscreen styling instead.
  fullscreenKey?: Nullable<string>;

  // CSS selector for a fullscreen button element to click. When set, this button is clicked before attempting keyboard or API fullscreen methods. This is useful
  // for sites that have a native fullscreen button in their player UI (e.g., a "MAXIMIZE" button). The element is verified to exist before clicking, so toggle
  // buttons that disappear after activation are handled gracefully.
  fullscreenSelector?: Nullable<string>;

  // CSS selector for site-specific overlay elements to hide during capture. When set, a persistent stylesheet is injected into the page that applies
  // "display: none !important" to the matched elements. This is used for player controls, toolbars, or other overlays that remain visible during fullscreen and
  // would otherwise appear in the captured stream. The selector supports standard CSS selector lists (comma-separated for multiple targets).
  hideSelector?: Nullable<string>;

  // Whether to override the video element's volume properties to prevent auto-muting. Some sites (like France 24) aggressively mute videos and fight attempts to
  // unmute them by resetting volume on every state change. When true, we use Object.defineProperty to intercept volume property access and force the video to
  // remain unmuted. This is a heavyweight intervention used only when necessary.
  lockVolumeProperties?: boolean;

  // Whether the video element is embedded in an iframe. When true, the stream handler searches all frames in the page for the video element rather than only the
  // main document. An iframe initialization delay is applied before searching to allow the iframe content to load.
  needsIframeHandling?: boolean;

  // Whether this is a static page without video content. When true, the stream handler skips video element detection and playback monitoring. This is used for
  // pages like electronic program guides or information displays that should be captured as-is without expecting video playback.
  noVideo?: boolean;

  // Whether to select the video element by readyState rather than DOM position. Some pages have multiple video elements (ads, previews, the main content). When
  // true, we find the video with readyState >= 3 (HAVE_FUTURE_DATA) rather than just taking the first video in the DOM. This typically selects the actively
  // playing main content rather than preloaded ad content.
  selectReadyVideo?: boolean;

  // Whether to use the JavaScript Fullscreen API instead of keyboard shortcuts. When true, we call video.requestFullscreen() or use the webkit-prefixed variant.
  // This is more reliable than keyboard shortcuts on some sites but may trigger browser permission prompts or be blocked by site CSP policies.
  useRequestFullscreen?: boolean;

  // Whether to wait for network idle during page navigation. When true, page.goto() waits for the network to be idle (no requests for 500ms) before returning.
  // This ensures all JavaScript has finished loading and executing. Disable for sites that have persistent connections or polling that prevents network idle.
  waitForNetworkIdle?: boolean;
}

/**
 * Fully-resolved site profile with all flags having concrete values. After resolving inheritance chains and applying defaults, every flag has a definite boolean
 * or string value. This interface is used by stream handling code that needs to check profile flags without worrying about undefined values.
 */
export interface ResolvedSiteProfile {

  // Configuration for channel selection behavior, with strategy defaulting to "none".
  channelSelection: ChannelSelectionConfig;

  // The channel slug to match when selecting a channel, or null if not applicable.
  channelSelector: Nullable<string>;

  // CSS selector for the element to click when clickToPlay is true, or null to click the video element.
  clickSelector: Nullable<string>;

  // Whether to click the video element to initiate playback.
  clickToPlay: boolean;

  // Keyboard key for fullscreen, or null to use CSS-based fullscreen.
  fullscreenKey: Nullable<string>;

  // CSS selector for a fullscreen button to click, or null if not applicable.
  fullscreenSelector: Nullable<string>;

  // CSS selector for overlay elements to hide during capture, or null if not applicable.
  hideSelector: Nullable<string>;

  // Whether to override volume properties to prevent auto-muting.
  lockVolumeProperties: boolean;

  // Maximum continuous playback duration in hours before the site enforces a stream cutoff, or null if the site allows indefinite playback. When set, the monitor
  // proactively reloads the page before this limit expires to maintain uninterrupted streaming. Only full page navigations reset the timer — source reloads do not.
  // This value is sourced from DOMAIN_CONFIG rather than site profiles because it represents a site policy, not a player behavior characteristic.
  maxContinuousPlayback: Nullable<number>;

  // Whether to search iframes for the video element.
  needsIframeHandling: boolean;

  // Whether this is a static page without video.
  noVideo: boolean;

  // Whether to select video by readyState rather than DOM position.
  selectReadyVideo: boolean;

  // Whether to use the JavaScript Fullscreen API.
  useRequestFullscreen: boolean;

  // Whether to wait for network idle during navigation.
  waitForNetworkIdle: boolean;
}

/**
 * Result of resolving a site profile. Includes both the resolved profile configuration and the name of the profile that was matched. The name indicates whether the
 * profile came from a channel hint, domain-based autodetection, or the default fallback.
 */
export interface ProfileResolutionResult {

  // The fully-resolved site profile with all flags having concrete values.
  profile: ResolvedSiteProfile;

  // The name of the matched profile (e.g., "keyboardDynamic", "fullscreenApi", "default").
  profileName: string;
}

/* Provider packs allow users to define custom site profiles and domain mappings without modifying source code. These types support the profiles.json storage format,
 * the provider pack distribution format for sharing configurations, and the validation pipeline for imports.
 */

/**
 * Domain-level configuration associating domain patterns with site profiles and provider display names. Each entry can specify a site profile for behavior
 * configuration and/or a provider display name for friendly UI labels. Used by both built-in domain mappings in sites.ts and user-defined mappings in profiles.json.
 */
export interface DomainConfig {

  // URL to navigate to for authentication. Some sites show different login options on their homepage vs their player page. When set, the auth route navigates to
  // this URL instead of the channel's streaming URL. Omit for sites where the streaming URL is also the correct login page.
  loginUrl?: string;

  // Maximum continuous playback duration in hours before the site enforces a stream cutoff. When set, the playback monitor proactively reloads the page before this
  // limit expires to maintain uninterrupted streaming. Fractional values are supported (e.g., 1.5 for 90 minutes). Omit for sites that allow indefinite playback.
  maxContinuousPlayback?: number;

  // Site profile name for automatic profile detection. When a URL matches this domain, the specified profile is used to configure site-specific behavior
  // (fullscreen method, iframe handling, etc.). Omit for domains that only need a display name.
  profile?: string;

  // Friendly provider name shown in the UI source column, provider dropdowns, and labels. When set, this name is used instead of the raw domain string (e.g.,
  // "Hulu" instead of "hulu.com"). Omit to fall back to the concise domain extracted from the URL.
  provider?: string;

  // Provider filter tag for subscription services. Channels whose canonical URL matches a domain with this field are identified as belonging to this subscription
  // service for filtering purposes. Domains that share a tag (e.g., "watch.sling.com" and a hypothetical "sling.com" variant) are treated as the same provider.
  // Omit for network-owned sites (abc.com, nbc.com, espn.com, etc.) — they are implicitly tagged "direct".
  providerTag?: string;
}

/**
 * Storage format for profiles.json. Contains user-defined site profiles and domain mappings that extend or override the built-in configurations.
 */
export interface UserProfilesFile {

  // User-defined domain-to-profile mappings. Each key is a hostname (e.g., "watch.sling.com") and the value configures which profile and provider name to use.
  domains?: Record<string, DomainConfig>;

  // User-defined site profiles. Each key is a profile name (e.g., "huluLive") and the value is a SiteProfile definition with an extends property referencing
  // a built-in profile.
  profiles?: Record<string, SiteProfile>;
}

/**
 * Result of loading user profiles from the profiles.json file.
 */
export interface UserProfilesLoadResult {

  // User-defined domain mappings (empty object if file doesn't exist or parse error).
  domains: Record<string, DomainConfig>;

  // True if the file exists but contains invalid JSON.
  parseError: boolean;

  // Error message if parseError is true.
  parseErrorMessage?: string;

  // User-defined site profiles (empty object if file doesn't exist or parse error).
  profiles: Record<string, SiteProfile>;
}

/**
 * Provider pack distribution format. Bundles a profile, domain mapping(s), and optionally channels for a streaming provider into a single JSON file. On import,
 * its contents are split and written to profiles.json and channels.json.
 */
export interface ProviderPack {

  // Channel definitions to add alongside the profile. Optional — users may want to configure their own channel list.
  channels?: ChannelMap;

  // Domain-to-profile mappings. Optional — users may want to import just a profile to reference from their own domain mappings.
  domains?: Record<string, DomainConfig>;

  // Human-readable provider name for display during import.
  name: string;

  // One or more profile definitions. At least one is required.
  profiles: Record<string, SiteProfile>;

  // Format version for future compatibility.
  version: number;
}

/**
 * Validation result for profile and domain imports.
 */
export interface ProfilesValidationResult {

  // Validated domain mappings that passed all checks.
  domains: Record<string, DomainConfig>;

  // Validation error messages describing each issue found.
  errors: string[];

  // Validated profiles that passed all checks.
  profiles: Record<string, SiteProfile>;

  // True if validation passed with no errors.
  valid: boolean;
}
