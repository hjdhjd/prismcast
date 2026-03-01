/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * config.ts: Application configuration type definitions for PrismCast.
 */
import type { ChannelSortField, Nullable, SortDirection } from "./shared.js";

/* These interfaces define the structure of the application configuration. The Config interface is the root configuration object, with nested interfaces for each
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

  // Absolute path override for Chrome's user data directory (profile, cookies, cache), or null to use the default location inside the data directory. When null,
  // the directory is built as <dataDir>/<chromeProfileName>. Setting this allows storing Chrome data on a different volume or sharing a profile across instances.
  chromeDataDir: Nullable<string>;

  // Directory name for Chrome's user data within the data directory. Only used when chromeDataDir is null. Chrome locks this directory while running, so we kill
  // stale processes on startup.
  chromeProfileName: string;

  // Directory name for extracted puppeteer-stream extension files. When running as a packaged executable, extension files must be extracted to the filesystem
  // because Chrome cannot load extensions from within the executable archive.
  extensionDirName: string;

  // Absolute path override for the log file, or null to use the default location (<dataDir>/prismcast.log). Setting this allows writing logs to a different
  // volume or a centralized log directory.
  logFile: Nullable<string>;
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

  // Sort direction for the channels table. Default: "asc".
  channelSortDirection: SortDirection;

  // Sort field for the channels table. Default: "name".
  channelSortField: ChannelSortField;

  // List of predefined channel keys that are disabled. Disabled channels are excluded from the playlist and cannot be streamed.
  disabledPredefined: string[];

  // Provider tags that are enabled for filtering. Empty array means no filter (all providers shown). Non-empty means only channels with at least one matching
  // provider variant are included in the playlist and guide.
  enabledProviders: string[];

  // Provider slugs selected for precaching at startup. Empty array means no precaching (default). When non-empty, the listed providers have their channel lineups
  // discovered at startup so that even the first tune benefits from cached lineup data.
  precacheProviders: string[];

  // Optional column field names that are currently visible in the channels table. Empty array means only required columns are shown.
  visibleColumns: string[];
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

  // Active debug filter pattern persisted from the /debug UI. When non-empty at startup and no higher-priority source (PRISMCAST_DEBUG env var or --debug CLI
  // flag) is active, this pattern is applied via initDebugFilter(). Managed by the /debug endpoint, not shown in the Settings/Advanced config UI.
  debugFilter: string;

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
