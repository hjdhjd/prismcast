/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Type definitions for PrismCast.
 */
import type { Frame, Page } from "puppeteer-core";
import type { RecoveryMetrics } from "../streaming/monitor.js";

/**
 * A utility type that represents a value that can be null.
 * @typeParam T - The type that can be nullable.
 */
export type Nullable<T> = T | null;

/*
 * CONFIGURATION TYPES
 *
 * These interfaces define the structure of the application configuration. The Config interface is the root configuration object, with nested interfaces for each
 * functional area. All configuration values are loaded from environment variables with sensible defaults. The configuration is validated at startup to catch
 * misconfigurations before the server begins accepting connections.
 */

/**
 * Browser-related configuration controlling Chrome launch behavior. Viewport dimensions are derived from the quality preset via getViewport() and are not stored in
 * this configuration object.
 */
export interface BrowserConfig {

  // Path to the Chrome executable. When null, the application searches common installation paths across macOS, Linux, and Windows. Setting this explicitly is
  // useful in containerized environments or when multiple browser versions are installed. Environment variable: CHROME_BIN.
  executablePath: Nullable<string>;

  // Time in milliseconds to wait after browser launch for the puppeteer-stream extension to initialize. The extension injects recording APIs into the browser
  // context, and attempting to capture streams before initialization completes causes silent failures. Increase this value if streams start with blank frames.
  // Environment variable: BROWSER_INIT_TIMEOUT. Default: 1000ms.
  initTimeout: number;
}

/**
 * Filesystem paths for Chrome profile data and extension files.
 */
export interface PathsConfig {

  // Directory name for Chrome's user data (profile, cookies, cache). This directory persists across restarts, allowing sites to remember login state. The directory
  // is created inside the application's data directory. Chrome locks this directory while running, so we kill stale processes on startup.
  chromeProfileName: string;

  // Directory name for extracted puppeteer-stream extension files. When running as a packaged executable, extension files must be extracted to the filesystem
  // because Chrome cannot load extensions from within the executable archive.
  extensionDirName: string;
}

/**
 * Playback monitoring and recovery timing configuration. These values control how quickly the system detects playback problems and how aggressively it attempts
 * recovery. The defaults balance responsiveness against false positives from temporary buffering.
 */
export interface PlaybackConfig {

  // Time in milliseconds to allow buffering before declaring a stall. Live streams occasionally buffer due to network conditions, and triggering recovery too
  // quickly causes unnecessary disruption. This grace period prevents false positives while still catching genuine stalls. Environment variable:
  // BUFFERING_GRACE_PERIOD. Default: 5000ms (5 seconds).
  bufferingGracePeriod: number;

  // Time in milliseconds to wait after clicking a channel selector before checking for video. Some multi-channel players have animated transitions or need time to
  // load the new channel's stream. Environment variable: CHANNEL_SELECTOR_DELAY. Default: 3000ms.
  channelSelectorDelay: number;

  // Time in milliseconds to wait after a channel switch completes for the stream to stabilize. This delay allows the player to finish any post-switch
  // initialization before we begin monitoring playback health. Environment variable: CHANNEL_SWITCH_DELAY. Default: 4000ms.
  channelSwitchDelay: number;

  // Time in milliseconds to wait after clicking the video element to initiate playback. Some players (particularly Brightcove-based) require a click to start and
  // need time to transition from the click handler to actual playback. Environment variable: CLICK_TO_PLAY_DELAY. Default: 1000ms.
  clickToPlayDelay: number;

  // Time in milliseconds to wait for iframe content to initialize before searching for video elements. When video is embedded in an iframe, the iframe document
  // loads asynchronously after the parent page. Searching too early returns no results. Environment variable: IFRAME_INIT_DELAY. Default: 1500ms.
  iframeInitDelay: number;

  // Maximum number of full page navigations allowed within the pageReloadWindow time period. Full page reloads are the most disruptive recovery action, so we limit
  // their frequency to prevent reload loops on fundamentally broken streams. When the limit is reached, recovery falls back to less disruptive source reloads.
  // Environment variable: MAX_PAGE_RELOADS. Default: 3.
  maxPageReloads: number;

  // Interval in milliseconds between playback health checks. Each check evaluates video state (currentTime, paused, ended, error, readyState) and triggers recovery
  // if problems are detected. Shorter intervals detect problems faster but increase CPU usage. Environment variable: MONITOR_INTERVAL. Default: 2000ms.
  monitorInterval: number;

  // Time window in milliseconds for tracking page reload frequency. Page reloads within this window count toward the maxPageReloads limit. After the window
  // expires, the reload counter resets. Environment variable: PAGE_RELOAD_WINDOW. Default: 900000ms (15 minutes).
  pageReloadWindow: number;

  // Time in milliseconds to wait after reloading the video source before resuming playback monitoring. Source reloads (resetting video.src and calling load())
  // require time for the player to reinitialize its internal state. Environment variable: SOURCE_RELOAD_DELAY. Default: 2000ms.
  sourceReloadDelay: number;

  // Number of consecutive stalled checks before triggering recovery. A single stalled check might be a temporary glitch, so we require multiple consecutive
  // failures before acting. With a 2-second monitor interval and threshold of 2, recovery triggers after 4-6 seconds of no progress. Environment variable:
  // STALL_COUNT_THRESHOLD. Default: 2.
  stallCountThreshold: number;

  // Minimum change in video.currentTime (in seconds) between checks to consider playback progressing. Values below this threshold are considered stalled. This
  // accounts for timing precision issues and very slow playback rates. Environment variable: STALL_THRESHOLD. Default: 0.1 seconds.
  stallThreshold: number;

  // Time in milliseconds of continuous healthy playback required before resetting the escalation level. After recovery succeeds, we keep the escalation level
  // elevated briefly in case the fix was temporary. Only after sustained healthy playback do we reset to level 0. This prevents "stutter loops" where playback
  // works briefly then fails again. Environment variable: SUSTAINED_PLAYBACK_REQUIRED. Default: 60000ms (1 minute).
  sustainedPlaybackRequired: number;
}

/**
 * Recovery behavior configuration controlling retry logic, backoff timing, and circuit breaker thresholds. These settings determine how the system handles
 * failures and prevents runaway resource consumption from broken streams.
 */
export interface RecoveryConfig {

  // Maximum random jitter in milliseconds added to retry backoff delays. Jitter prevents "thundering herd" problems where multiple failed operations retry at
  // exactly the same time, overwhelming the target service. The actual jitter for each retry is a random value between 0 and this maximum. Environment variable:
  // BACKOFF_JITTER. Default: 1000ms.
  backoffJitter: number;

  // Number of failures within the circuitBreakerWindow that triggers stream termination. The circuit breaker prevents endless recovery attempts on fundamentally
  // broken streams (wrong URL, geo-blocked content, expired authentication). When tripped, the stream is terminated and the client connection closed. Environment
  // variable: CIRCUIT_BREAKER_THRESHOLD. Default: 10 failures.
  circuitBreakerThreshold: number;

  // Time window in milliseconds for counting failures toward the circuit breaker threshold. Failures outside this window don't count. This allows occasional
  // failures without triggering termination, while catching streams that fail repeatedly in a short period. Environment variable: CIRCUIT_BREAKER_WINDOW. Default:
  // 300000ms (5 minutes).
  circuitBreakerWindow: number;

  // Maximum delay in milliseconds between retry attempts. Exponential backoff doubles the delay after each failure, but this cap prevents excessively long waits.
  // The actual delay is: min(1000 * 2^attempt, maxBackoffDelay) + random(0, backoffJitter). Environment variable: MAX_BACKOFF_DELAY. Default: 3000ms.
  maxBackoffDelay: number;

  // Interval in milliseconds between stale page cleanup runs. Browser pages can accumulate if cleanup fails during stream termination. This periodic cleanup
  // identifies and closes pages not associated with active streams, preventing memory exhaustion. Environment variable: STALE_PAGE_CLEANUP_INTERVAL. Default:
  // 60000ms (1 minute).
  stalePageCleanupInterval: number;

  // Grace period in milliseconds before a page is considered stale. When a page is not associated with any active stream, we wait this duration before closing it.
  // This prevents race conditions where a page is briefly untracked during stream initialization or cleanup. Environment variable: STALE_PAGE_GRACE_PERIOD.
  // Default: 30000ms (30 seconds).
  stalePageGracePeriod: number;
}

/**
 * HLS streaming configuration controlling segment generation and lifecycle.
 */
export interface HLSConfig {

  // Time in milliseconds before an HLS stream is terminated due to inactivity. If no segment or playlist requests are received within this window, the stream is
  // considered abandoned and resources are released. Environment variable: HLS_IDLE_TIMEOUT. Default: 30000ms (30 seconds).
  idleTimeout: number;

  // Maximum number of segments to keep in memory per stream. Older segments are discarded as new ones arrive. This controls memory usage and determines how far
  // back a client can seek. With 2-second segments, 10 segments = 20 seconds of buffer. Environment variable: HLS_MAX_SEGMENTS. Default: 10.
  maxSegments: number;

  // Target duration for each HLS segment in seconds. Shorter segments reduce latency but increase overhead. 2 seconds provides good latency for live TV. This
  // value is passed to FFmpeg's -hls_time parameter. Environment variable: HLS_SEGMENT_DURATION. Default: 2.
  segmentDuration: number;
}

/**
 * Channels configuration controlling which predefined channels are enabled.
 */
export interface ChannelsConfig {

  // List of predefined channel keys that are disabled. Disabled channels are excluded from the playlist and cannot be streamed.
  disabledPredefined: string[];
}

/**
 * HDHomeRun emulation configuration. When enabled, PrismCast runs a separate HTTP server that emulates the HDHomeRun API, allowing Plex to discover and use
 * PrismCast as a virtual tuner for live TV and DVR recording. The emulated device appears in Plex's tuner setup and serves PrismCast's HLS streams directly.
 */
export interface HdhrConfig {

  // Device ID for HDHomeRun identification on the network. Auto-generated on first startup using the HDHomeRun checksum algorithm and stored in the config file
  // for persistence across restarts. Must be exactly 8 hex characters with a valid check digit.
  deviceId: string;

  // Whether HDHomeRun emulation is enabled. When enabled, a second HTTP server listens on the configured port and responds to HDHomeRun API requests from Plex.
  // When disabled, no additional server is started and no resources are consumed. Environment variable: HDHR_ENABLED. Default: true.
  enabled: boolean;

  // Friendly name displayed in Plex when it discovers this tuner. This helps users identify PrismCast among multiple tuners in their Plex setup. Environment
  // variable: HDHR_FRIENDLY_NAME. Default: "PrismCast".
  friendlyName: string;

  // TCP port for the HDHomeRun emulation server. HDHomeRun devices traditionally use port 5004, and Plex expects this port when auto-discovering tuners via UDP.
  // If another HDHomeRun device or emulator is already using this port, PrismCast logs a warning and continues without HDHR emulation. Environment variable:
  // HDHR_PORT. Default: 5004. Valid range: 1-65535.
  port: number;
}

/**
 * Logging configuration controlling file-based logging behavior.
 */
export interface LoggingConfig {

  // Controls HTTP request logging level. "none" disables HTTP request logging, "errors" logs only 4xx and 5xx responses, "filtered" logs important requests
  // while skipping high-frequency endpoints like /logs and /health, "all" logs all requests. Environment variable: HTTP_LOG_LEVEL. Default: "errors".
  httpLogLevel: "all" | "errors" | "filtered" | "none";

  // Maximum size of the log file in bytes. When the file exceeds this size, it is trimmed to half the size keeping only complete lines. The most recent logs are
  // preserved. Environment variable: LOG_MAX_SIZE. Default: 1048576 (1MB). Valid range: 10240-104857600.
  maxSize: number;
}

/**
 * HTTP server configuration controlling network binding.
 */
export interface ServerConfig {

  // IP address or hostname to bind the HTTP server. Use "0.0.0.0" to accept connections on all network interfaces, or "127.0.0.1" to accept only local
  // connections. In containerized deployments, "0.0.0.0" is typically required for the container's port mapping to work. Environment variable: HOST. Default:
  // "0.0.0.0".
  host: string;

  // TCP port number for the HTTP server. Channels DVR and other clients connect to this port to request streams and playlists. Choose a port that doesn't conflict
  // with other services and is accessible through any firewalls. Environment variable: PORT. Default: 5589. Valid range: 1-65535.
  port: number;
}

/**
 * Capture mode for media recording. Determines how video/audio is captured from the browser and processed for HLS output.
 * - "ffmpeg": Captures WebM (H264+Opus) and uses FFmpeg to transcode audio to AAC. More stable for long recordings.
 * - "native": Captures fMP4 (H264+AAC) directly from Chrome. No dependencies but may be unstable with long recordings.
 */
export type CaptureMode = "ffmpeg" | "native";

/**
 * Media streaming configuration controlling video capture quality, timeouts, and concurrency limits.
 */
export interface StreamingConfig {

  // Audio bitrate in bits per second for the captured stream. Higher values improve audio quality but increase bandwidth requirements. 256kbps provides high-quality
  // stereo audio; lower values (128kbps) work for speech-heavy content. Environment variable: AUDIO_BITRATE. Default: 256000. Valid range: 32000-512000.
  audioBitsPerSecond: number;

  // Capture mode determining how video/audio is captured and processed. "ffmpeg" captures WebM (H264+Opus) and uses FFmpeg to transcode audio to AAC - more stable
  // for long recordings but requires FFmpeg. "native" captures fMP4 (H264+AAC) directly from Chrome - no dependencies but may be unstable with long recordings.
  // Environment variable: CAPTURE_MODE. Default: "ffmpeg".
  captureMode: CaptureMode;

  // Target frame rate for video capture. Higher frame rates produce smoother video but require more CPU and bandwidth. 60fps is ideal for sports content; 30fps
  // is sufficient for most television content. The browser may deliver fewer frames if the source content has a lower frame rate. Environment variable:
  // FRAME_RATE. Default: 60.
  frameRate: number;

  // Maximum number of simultaneous streaming sessions. Each stream consumes a browser tab, memory, and CPU resources. Setting this too high can exhaust system
  // resources and degrade all streams. Setting too low prevents legitimate concurrent viewing. Environment variable: MAX_CONCURRENT_STREAMS. Default: 10. Valid
  // range: 1-100.
  maxConcurrentStreams: number;

  // Maximum number of page navigation retry attempts before giving up. Navigation failures can occur due to network issues, slow page loads, or site problems.
  // Retries use exponential backoff to avoid overwhelming struggling sites. Environment variable: MAX_NAV_RETRIES. Default: 4.
  maxNavigationRetries: number;

  // Timeout in milliseconds for page navigation operations. This applies to page.goto() calls and determines how long to wait for the page to load before
  // declaring failure. Increase for slow networks or sites with heavy JavaScript initialization. Environment variable: NAV_TIMEOUT. Default: 10000ms. Valid
  // range: 1000-600000.
  navigationTimeout: number;

  // Video quality preset that determines capture resolution. The preset controls the browser viewport dimensions used for video capture. Valid values: "480p",
  // "720p", "1080p", "1080p-high", "4k". Bitrate and frame rate can be customized independently. Environment variable: QUALITY_PRESET. Default: "720p".
  qualityPreset: string;

  // Video bitrate in bits per second for browser capture. This controls the quality of the stream captured by puppeteer-stream. For HLS output, FFmpeg copies
  // the video stream directly without re-encoding, preserving this quality. 8Mbps is suitable for 720p content; 15-20Mbps is recommended for 1080p. The actual
  // bitrate may vary based on content complexity. Environment variable: VIDEO_BITRATE. Default: 8000000. Valid range: 100000-50000000.
  videoBitsPerSecond: number;

  // Timeout in milliseconds for waiting for a video element to become ready. After navigating to a page, we wait for a video element with sufficient readyState.
  // Increase for sites with slow-loading video players or heavy pre-roll content. Environment variable: VIDEO_TIMEOUT. Default: 10000ms. Valid range:
  // 1000-600000.
  videoTimeout: number;
}

/**
 * Root configuration object containing all application settings organized by functional area.
 */
export interface Config {

  // Browser launch and viewport configuration.
  browser: BrowserConfig;

  // Channel enable/disable configuration.
  channels: ChannelsConfig;

  // HDHomeRun emulation configuration for Plex integration.
  hdhr: HdhrConfig;

  // HLS streaming configuration.
  hls: HLSConfig;

  // Logging configuration.
  logging: LoggingConfig;

  // Filesystem paths for persistent data.
  paths: PathsConfig;

  // Playback monitoring and recovery timing.
  playback: PlaybackConfig;

  // Retry logic and circuit breaker settings.
  recovery: RecoveryConfig;

  // HTTP server binding configuration.
  server: ServerConfig;

  // Media capture quality and timeout settings.
  streaming: StreamingConfig;
}

/*
 * SITE PROFILE TYPES
 *
 * Site profiles define behavior patterns for different streaming site implementations. Television network streaming sites vary widely in their player
 * implementations: some use keyboard shortcuts for fullscreen, others require the JavaScript Fullscreen API; some embed video in iframes, others place it directly
 * in the page; some auto-mute videos and fight attempts to unmute them. The profile system captures these behavioral differences as configuration rather than
 * code, making it easy to add support for new sites by defining their characteristics.
 *
 * Profiles support inheritance via the "extends" field, allowing common patterns to be defined once and reused. For example, many NBC Universal properties share
 * the same player implementation, so they extend a common "nbcUniversal" base profile.
 */

/**
 * Site profile definition with optional flags. All flags are optional because profiles can inherit from other profiles, and only the flags that differ from the
 * parent need to be specified. The DEFAULT_SITE_PROFILE provides baseline values for any flags not set through inheritance.
 */
export interface SiteProfile {

  // Configuration for channel selection behavior on multi-channel sites. When set, determines how to find and click the desired channel in the site's UI. The
  // strategy property specifies the algorithm used to locate the channel element.
  channelSelection?: ChannelSelectionConfig;

  // The channel slug to match when selecting a channel from a multi-channel player. This is the literal string to find in thumbnail image URLs or other channel
  // identifiers. The value typically comes from the channel definition rather than the profile itself.
  channelSelector?: Nullable<string>;

  // Whether the video player requires a click to start playback. Brightcove-based players commonly require this. When true, the stream handler clicks the video
  // element after page load and before waiting for the video to become ready. This simulates user interaction to satisfy autoplay policies.
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

  // Whether to click the video element to initiate playback.
  clickToPlay: boolean;

  // Keyboard key for fullscreen, or null to use CSS-based fullscreen.
  fullscreenKey: Nullable<string>;

  // Whether to override volume properties to prevent auto-muting.
  lockVolumeProperties: boolean;

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

/*
 * CHANNEL TYPES
 *
 * Channels map short URL-friendly names to streaming site URLs with optional metadata. The channel name appears in stream URLs (e.g., /stream/nbc) and must be
 * URL-safe. Channel definitions can override profile settings for specific channels and provide metadata for M3U playlist generation.
 */

/**
 * Channel definition mapping a short name to a streaming URL with optional configuration overrides.
 */
export interface Channel {

  // Numeric channel number for HDHomeRun/Plex guide matching. When set, this number is used as the GuideNumber in the HDHomeRun lineup, enabling Plex to match
  // the channel with electronic program guide data. When omitted, a number is auto-assigned. Only relevant when HDHomeRun emulation is enabled.
  channelNumber?: number;

  // CSS selector for channel selection within a multi-channel player. This overrides any channelSelector in the profile. Used for sites like Pluto TV where the
  // base URL is the same but different channels require clicking different UI elements.
  channelSelector?: string;

  // Human-readable channel name displayed in the M3U playlist. This is what users see in their channel guide. Use proper capitalization and include network
  // suffixes like "HD" or regional identifiers like "(Pacific)" where appropriate.
  name: string;

  // Profile name to use for this channel, overriding URL-based profile detection. Use this when a site's behavior doesn't match what would be inferred from its
  // domain, or when a specific channel needs different handling than others on the same site.
  profile?: string;

  // Gracenote station ID for electronic program guide integration. When set, this ID is included in the M3U playlist as the tvc-guide-stationid attribute,
  // allowing Channels DVR to fetch program guide data for the channel.
  stationId?: string;

  // URL of the streaming page to capture. This should be the direct URL to the live stream player, not a landing page or show page. Authentication cookies from
  // the Chrome profile are used, so the URL can be to authenticated content.
  url: string;
}

/**
 * Enriched channel entry returned by getChannelListing(). Wraps a Channel definition with source classification and enabled status metadata, providing the
 * single source of truth for merged channel data across the codebase.
 */
export interface ChannelListingEntry {

  // The channel definition with all properties (name, url, profile, etc.).
  channel: Channel;

  // Whether the channel is enabled for streaming and playlist inclusion. Disabled predefined channels (without user overrides) have this set to false.
  enabled: boolean;

  // The channel key (URL-safe slug used in stream URLs).
  key: string;

  // Where this channel comes from: "predefined" (built-in), "user" (user-defined), or "override" (user channel replacing a predefined one).
  source: "override" | "predefined" | "user";
}

/**
 * Map of channel short names to channel definitions. Channel names must be URL-safe strings (lowercase letters, numbers, hyphens) since they appear in stream
 * request URLs.
 */
export type ChannelMap = Record<string, Channel>;

/*
 * STREAM TYPES
 *
 * These types track active streaming sessions throughout their lifecycle. When a stream request arrives, we create a StreamInfo object to track the session's
 * state. This allows the /streams endpoint to list active streams, the graceful shutdown handler to close streams cleanly, and the stream handler to coordinate
 * cleanup when streams end.
 */

/**
 * Information about an active streaming session. Created when a stream request is received and deleted when the stream ends.
 */
export interface StreamInfo {

  // Channel name if streaming a named channel from the CHANNELS configuration, or null if streaming an arbitrary URL via the url query parameter.
  channelName: Nullable<string>;

  // Unique numeric identifier for this stream session. Used by the /streams/:id endpoint for stream management and in log messages for correlation.
  id: number;

  // Puppeteer Page object for the browser tab running this stream. Used for cleanup when the stream ends and for the graceful shutdown handler to close all
  // streams.
  page: Page;

  // Timestamp when the stream was initiated. Used to calculate stream duration for logging and the /streams endpoint.
  startTime: Date;

  // Function to stop the playback health monitor for this stream, or null if monitoring hasn't started yet. Called during cleanup to stop the monitoring
  // interval and prevent the monitor from trying to recover a stream that's being terminated. Returns recovery metrics for the termination summary.
  stopMonitor: Nullable<() => RecoveryMetrics>;

  // URL being streamed. Logged for debugging and displayed in the /streams endpoint.
  url: string;
}

/*
 * VIDEO STATE TYPES
 *
 * These types represent the state of HTML5 video elements as reported by the browser. The playback health monitor periodically evaluates video state to detect
 * problems and trigger recovery. Understanding these values is essential for diagnosing playback issues.
 */

/**
 * Snapshot of a video element's playback state. Collected by the playback health monitor to detect stalls, errors, and other problems.
 */
export interface VideoState {

  // Current playback position in seconds. Compared between monitor checks to detect stalls. If this value doesn't change between checks (accounting for the
  // stallThreshold), the video is considered stalled.
  currentTime: number;

  // Whether the video has reached its end. For live streams, this typically indicates an error condition since live streams don't have a natural end.
  ended: boolean;

  // Whether the video element has an error (video.error !== null). This indicates a media error like a decode failure or network error that prevents playback.
  error: boolean;

  // Whether the video is muted. Some sites auto-mute videos; the health monitor enforces unmuted state on each check.
  muted: boolean;

  // The video's networkState property indicating network activity: 0 (EMPTY), 1 (IDLE), 2 (LOADING), 3 (NO_SOURCE). Value 2 (LOADING) combined with low
  // readyState indicates active buffering.
  networkState: number;

  // Whether the video is paused. Paused videos don't progress and may indicate that autoplay was blocked or the user paused playback.
  paused: boolean;

  // The video's readyState property indicating how much data is buffered: 0 (HAVE_NOTHING), 1 (HAVE_METADATA), 2 (HAVE_CURRENT_DATA), 3 (HAVE_FUTURE_DATA), 4
  // (HAVE_ENOUGH_DATA). We consider readyState >= 3 as "ready" because live streams may never reach 4 due to continuous data arrival.
  readyState: number;

  // Alias for currentTime. Some code uses "time" for brevity.
  time: number;

  // Current volume level from 0.0 (silent) to 1.0 (full volume). The health monitor enforces volume = 1.0 on each check to counter sites that lower volume.
  volume: number;
}

/**
 * Strategy for selecting a video element when multiple are present. "selectFirstVideo" takes the first video in DOM order; "selectReadyVideo" finds the video
 * with readyState >= 3, which typically identifies the actively playing main content rather than preloaded ads.
 */
export type VideoSelectorType = "selectFirstVideo" | "selectReadyVideo";

/*
 * URL VALIDATION TYPES
 *
 * Before navigating to user-provided URLs, we validate them to prevent security issues (like file:// access) and provide clear error messages for malformed URLs.
 * Validation runs before any browser interaction to fail fast with helpful feedback.
 */

/**
 * Result of URL validation indicating whether the URL is safe to navigate to.
 */
export interface UrlValidationResult {

  // Human-readable explanation of why validation failed, present only when valid is false.
  reason?: string;

  // Whether the URL passed validation and is safe to navigate to.
  valid: boolean;
}

/**
 * Alias for UrlValidationResult maintained for backward compatibility with existing code.
 */
export type UrlValidation = UrlValidationResult;

/*
 * HEALTH CHECK TYPES
 *
 * The /health endpoint returns detailed status information for monitoring and debugging. This includes browser connection state, memory usage, stream counts, and
 * configuration summary. External monitoring systems can poll this endpoint to detect problems.
 */

/**
 * Health check response structure returned by the /health endpoint.
 */
export interface HealthStatus {

  // Browser connection information.
  browser: {

    // Whether the Puppeteer browser instance is currently connected. False indicates the browser crashed or was closed.
    connected: boolean;

    // Number of open browser pages/tabs. Includes both stream pages and any stale pages pending cleanup.
    pageCount: number;
  };

  // Media capture mode currently configured ("ffmpeg" or "native").
  captureMode: string;

  // Chrome browser version string (e.g., "Chrome/144.0.7559.110"), or null if the browser is not connected.
  chrome: Nullable<string>;

  // Aggregate client information across all active streams.
  clients: {

    // Per-type breakdown sorted alphabetically by type name.
    byType: { count: number; type: string }[];

    // Total number of clients across all streams.
    total: number;
  };

  // Whether FFmpeg is available on the system. Only relevant when captureMode is "ffmpeg".
  ffmpegAvailable: boolean;

  // Node.js memory usage statistics in bytes.
  memory: {

    // Total heap memory allocated by V8.
    heapTotal: number;

    // Heap memory currently in use by V8.
    heapUsed: number;

    // Resident set size - total memory allocated for the process.
    rss: number;

    // Total memory used by HLS segment buffers across all active streams.
    segmentBuffers: number;
  };

  // Human-readable status message, present when status is not "healthy".
  message?: string;

  // Overall health status: "healthy" when everything is working, "degraded" when approaching capacity, "unhealthy" when browser is disconnected.
  status: "degraded" | "healthy" | "unhealthy";

  // Active stream information.
  streams: {

    // Number of currently active streams.
    active: number;

    // Maximum concurrent streams allowed.
    limit: number;
  };

  // ISO 8601 timestamp when the health check was performed.
  timestamp: string;

  // Server uptime in seconds since the process started.
  uptime: number;

  // PrismCast server version from package.json.
  version: string;
}

/*
 * STREAM LIST TYPES
 *
 * The /streams endpoint returns information about all active streams, allowing operators to monitor what's currently streaming and terminate specific streams if
 * needed.
 */

/**
 * Information about a single active stream as returned by the /streams endpoint.
 */
export interface StreamListItem {

  // Channel name if streaming a named channel, or null for arbitrary URLs.
  channel: Nullable<string>;

  // Stream duration in seconds since it started.
  duration: number;

  // Unique numeric identifier for the stream, usable with DELETE /streams/:id.
  id: number;

  // ISO 8601 timestamp when the stream started.
  startTime: string;

  // URL being streamed.
  url: string;
}

/**
 * Response structure for the /streams endpoint.
 */
export interface StreamListResponse {

  // Number of currently active streams.
  count: number;

  // Maximum concurrent streams allowed.
  limit: number;

  // Array of active stream information.
  streams: StreamListItem[];
}

/*
 * CHANNEL SELECTION TYPES
 *
 * For multi-channel streaming sites (like USA Network), we need to interact with the site's channel selector UI to switch to the desired channel. The channel
 * selection system uses a strategy pattern to support different site implementations. Each strategy encapsulates the logic for finding and clicking the correct
 * channel element.
 */

/**
 * Available channel selection strategies. Each strategy implements a different approach to finding and selecting channels in a multi-channel player UI.
 *
 * - "none": No channel selection needed (single-channel sites). This is the default.
 * - "thumbnailRow": Find channel by matching image URL slug, click adjacent element on the same row. Used by USA Network.
 * - "tileClick": Find channel tile by matching image URL slug, click tile, then click play button on modal. Used by Disney+ live channels.
 * - "tileClickDirect": Find channel tile by matching image URL slug, click tile and expect immediate video playback. Used by Kayo Sports.
 */
export type ChannelSelectionStrategy = "none" | "tileClick" | "tileClickDirect" | "thumbnailRow";

/**
 * Configuration for channel selection behavior within a site profile.
 */
export interface ChannelSelectionConfig {

  // The strategy to use for finding and clicking channel elements.
  strategy: ChannelSelectionStrategy;
}

/**
 * Result of attempting to select a channel from a multi-channel player UI.
 */
export interface ChannelSelectorResult {

  // Human-readable explanation of why selection failed, present only when success is false.
  reason?: string;

  // Whether the channel was successfully selected.
  success: boolean;
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
}

/*
 * CDP WINDOW TYPES
 *
 * Chrome DevTools Protocol operations for window management. We use CDP to resize and minimize browser windows to match viewport dimensions and reduce GPU usage
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
