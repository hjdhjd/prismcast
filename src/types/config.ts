/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * config.ts: Application configuration type definitions for PrismCast.
 */
import type { ChannelSortField, Nullable, SortDirection } from "./shared.ts";

/* These interfaces define the structure of the application configuration. The Config interface is the root configuration object, with nested interfaces for each
 * functional area. Most values can be supplied through a layered priority (CLI flags override environment variables, which override the config file, which overrides
 * builtin defaults), while a few are managed internally - the Channels DVR host is auto-discovered, the HDHomeRun device ID is auto-generated, and the debug filter
 * is owned by the /debug UI. The configuration is validated at startup to catch misconfigurations before the server begins accepting connections.
 */

/**
 * Browser-related configuration controlling Chrome launch behavior. Viewport dimensions are derived from the quality preset via getPresetViewport() and are not
 * stored in this configuration object.
 */
export interface BrowserConfig {

  // Path to the Chrome executable. When null, the application searches common installation paths across macOS, Linux, and Windows. Setting this explicitly is
  // useful in containerized environments or when multiple browser versions are installed. Environment variable: CHROME_BIN.
  executablePath: Nullable<string>;

  // Maximum time in milliseconds to wait after browser launch for the puppeteer-stream extension to initialize. The extension injects recording APIs into the
  // browser context, and attempting to capture streams before initialization completes causes silent failures. The system polls for readiness and proceeds early
  // once the extension is ready, so this is a ceiling rather than a fixed delay. Increase this value if streams start with blank frames. Environment variable:
  // BROWSER_INIT_TIMEOUT. Default: 3000ms.
  initTimeout: number;
}

/**
 * Filesystem path overrides for Chrome profile data and the log file.
 */
export interface PathsConfig {

  // Absolute path override for Chrome's user data directory (profile, cookies, cache), or null to use the default location inside the data directory. When null,
  // the directory is <dataDir>/chromedata. Setting this allows storing Chrome data on a different volume, or reusing one profile across installs that never run
  // at the same time.
  chromeDataDir: Nullable<string>;

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
  // BUFFERING_GRACE_PERIOD. Default: 10000ms (10 seconds).
  bufferingGracePeriod: number;

  // Time in milliseconds to wait after clicking a channel selector before checking for video. Some multi-channel players have animated transitions or need time to
  // load the new channel's stream. Environment variable: CHANNEL_SELECTOR_DELAY. Default: 5000ms.
  channelSelectorDelay: number;

  // Time in milliseconds to wait after a channel switch completes for the stream to stabilize. This delay allows the player to finish any post-switch
  // initialization before we begin monitoring playback health. Environment variable: CHANNEL_SWITCH_DELAY. Default: 4000ms.
  channelSwitchDelay: number;

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
  // The actual delay is: min(1000 * 2^(attempt-1), maxBackoffDelay) + random(0, backoffJitter). Environment variable: MAX_BACKOFF_DELAY. Default: 3000ms.
  maxBackoffDelay: number;

  // Number of failed browser relaunches within relaunchFailureWindow that trips the browser relaunch governor into a cooldown. Below this, the first relaunch
  // failures retry immediately (no penalty for the common transient); at it, the governor backs off along an escalating cooldown so a persistently-broken browser
  // stops thrashing Chrome. Biased eager-for-the-first-failure. Environment variable: RELAUNCH_FAILURE_THRESHOLD. Default: 3 failures.
  relaunchFailureThreshold: number;

  // Time window in milliseconds for counting failed browser relaunches toward relaunchFailureThreshold. Failures outside this window do not count, so isolated
  // failures over a long period never trip the governor while a rapid burst does. Environment variable: RELAUNCH_FAILURE_WINDOW. Default: 300000ms (5 minutes).
  relaunchFailureWindow: number;

  // Continuous capture-readiness in milliseconds required before the browser relaunch governor resets to its normal state. The reset is health-gated rather than
  // success-gated: only sustained readiness clears the accrued failures and the cooldown escalation, so a flapping browser (briefly ready, then dead) still accrues
  // toward a trip. Environment variable: RELAUNCH_HEALTH_HOLD. Default: 120000ms (2 minutes).
  relaunchHealthHold: number;

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

  // Target duration for each HLS segment in seconds. Shorter segments reduce latency but increase overhead. 2 seconds provides good latency for live TV. The
  // segmenter cuts a segment at the first fragment boundary once this much wall-clock time has elapsed since the segment began, and the playlist advertises it
  // as the target duration. Environment variable: HLS_SEGMENT_DURATION. Default: 2.
  segmentDuration: number;
}

/**
 * Channels configuration: channel table preferences, the service filter, precaching, the setup flow's completion, and which predefined channels are disabled.
 */
export interface ChannelsConfig {

  // Sort direction for the channels table. Default: "asc".
  channelSortDirection: SortDirection;

  // Sort field for the channels table. Default: "name".
  channelSortField: ChannelSortField;

  // List of predefined channel keys that are disabled. Disabled channels are excluded from the playlist and cannot be streamed.
  disabledPredefined: string[];

  // Service tags that are enabled for filtering. Empty array means no filter (all services shown). Non-empty means only channels with at least one matching
  // service variant are included in the playlist and guide.
  enabledServices: string[];

  // Service slugs selected for precaching. Empty array means no precaching (default). When non-empty, the listed services have their channel lineups discovered
  // each time the browser launches, and a newly selected service's lineup once a save adds it, so that even the first tune benefits from cached lineup data.
  precacheServices: string[];

  // Whether the user has completed the initial Service Setup flow. When false, the setup wizard auto-presents on the first visit to the channels tab.
  // Set to true on completion or explicit skip - never on browser close or navigation away.
  setupCompleted: boolean;

  // Optional column field names that are currently visible in the channels table. Empty array means only required columns are shown.
  visibleColumns: string[];
}

/**
 * Connection settings for the user's external Channels DVR server. PrismCast talks to Channels DVR over HTTP for show-info polling, device-mapping discovery,
 * and pretune scheduling.
 *
 * Host is auto-discovered at runtime from client request IPs by `showInfo.ts`. It is intentionally NOT in `CONFIG_METADATA` and is not exposed as a settings
 * UI field - users do not configure it directly. The host rule: host-only, never `host:port`. Port lives at `channelsDvr.port` exclusively.
 *
 * Port is user-configurable via `CONFIG_METADATA` because Channels DVR's port is user-configurable per their docs and a non-default port would otherwise be
 * unreachable from PrismCast.
 *
 * All DVR-targeted code reads this one config location.
 */
export interface ChannelsDvrConfig {

  // Auto-discovered Channels DVR hostname or IP. Empty string means "not yet discovered." Populated by `showInfo.setDvrHost()` when a matching M3U device is
  // found on a candidate host. Host-only - never includes a port. The process owns this value through auto-discovery, so it stays out of `CONFIG_METADATA`.
  host: string;

  // TCP port for the user's Channels DVR API. Default 8089 (the canonical Channels DVR port). Override when the user has changed the DVR's listen port from
  // its default.
  port: number;
}

/**
 * HDHomeRun emulation configuration. When enabled, PrismCast runs a separate HTTP server that emulates the HDHomeRun API and (when discoveryEnabled is also
 * true) a UDP discovery responder on port 65001 that lets Plex find PrismCast on the LAN automatically. The emulated device appears in Plex's tuner setup and
 * serves PrismCast's MPEG-TS streams directly. Channels DVR also auto-discovers but its discovery assumes the standard HDHomeRun port 80, which most
 * installations cannot bind; Channels DVR users typically add PrismCast manually as a Custom Channels source. Other HDHR-aware clients work to the extent that
 * they honor the BaseURL advertised in the protocol; their compatibility is incidental rather than supported.
 */
export interface HdhrConfig {

  // Device ID for HDHomeRun identification on the network, stored in the config file for persistence across restarts. While the emulation is enabled, a missing
  // id or one that fails its checksum takes the running id when that one passes and otherwise one generated with the HDHomeRun checksum algorithm, where the boot
  // loads the file or a settings save writes it, and a disabled instance's file is never written for it. Must be exactly 8 hex characters with a valid check digit.
  deviceId: string;

  // Whether LAN discovery is enabled. When true and HDHR emulation is enabled, PrismCast binds a UDP responder on the standard HDHomeRun discovery port (65001)
  // so Plex can auto-detect PrismCast on the local network without the operator entering an IP and port manually. Channels DVR will also discover PrismCast on
  // the LAN, but its auto-discovery assumes port 80 for the HTTP control plane and so cannot fetch the lineup unless hdhr.port is set to 80 (which requires
  // elevated privileges to bind). Independent of hdhr.enabled so an operator who wants HTTP HDHR but not LAN announcement (multi-tenant boxes, environments
  // with an existing real HDHR) can disable just the discovery surface. Environment variable: HDHR_DISCOVERY_ENABLED. Default: true.
  discoveryEnabled: boolean;

  // Whether HDHomeRun emulation is enabled. When enabled, a second HTTP server listens on the configured port and responds to HDHomeRun API requests from Plex.
  // When disabled, no additional server is started and no resources are consumed. Environment variable: HDHR_ENABLED. Default: true.
  enabled: boolean;

  // Friendly name displayed in HDHR-aware clients when they discover this tuner. This helps users identify PrismCast among multiple tuners in their setup.
  // Environment variable: HDHR_FRIENDLY_NAME. Default: "PrismCast".
  friendlyName: string;

  // TCP port for the HDHomeRun emulation server. HDHomeRun devices traditionally use port 5004; this is the port a manual setup paste in Plex or Channels DVR
  // expects. If another HDHomeRun device or emulator is already using this port, PrismCast logs a warning and continues without HDHR emulation. Environment
  // variable: HDHR_PORT. Default: 5004. Valid range: 1-65535.
  port: number;
}

/**
 * The HTTP request log levels, in the order the settings form lists them. This array is the single definition from which the httpLogLevel type and the setting's
 * valid values derive.
 */
export const HTTP_LOG_LEVELS = [ "none", "errors", "filtered", "all" ] as const;

/**
 * Logging configuration controlling file-based logging behavior.
 */
export interface LoggingConfig {

  // Persisted debug filter pattern. The boot applies it via initDebugFilter(), and any save that changes it - from the /debug page or a config import -
  // applies it live, in both cases unless a higher-priority source (PRISMCAST_DEBUG env var or --debug CLI flag) owns the filter. An empty filter clears the
  // runtime filter. Not shown in the Settings/Advanced config UI.
  debugFilter: string;

  // Controls HTTP request logging level. "none" disables HTTP request logging, "errors" logs only 4xx and 5xx responses, "filtered" logs important requests
  // while skipping high-frequency endpoints like /logs and /health, "all" logs all requests. Environment variable: HTTP_LOG_LEVEL. Default: "errors".
  httpLogLevel: typeof HTTP_LOG_LEVELS[number];

  // Maximum size of the log file in bytes. When the file exceeds this size, it is trimmed to at most half the size, keeping the most recent complete lines.
  // Environment variable: LOG_MAX_SIZE. Default: 1048576 (1MB). Valid range: 524288-104857600.
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
 * The capture mode setting. Every stream captures through FFmpeg, so the setting names the capture mode in effect and no capture path branches on it.
 * - "ffmpeg": The capture mode every stream uses, which captures Matroska (the effective capture codec plus Opus) and uses FFmpeg to transcode audio to AAC.
 * - "native": Names Chrome's direct fMP4 (H264+AAC) recording, which no capture path implements and which every configuration corrects to "ffmpeg".
 *
 * Chrome's native fMP4 MediaRecorder produces corrupt output after 20-30 minutes of recording, so every configuration the server builds corrects "native" to
 * "ffmpeg", the warning is logged at startup and by a save whose write lands, and every write of the configuration file stores the corrected value.
 */
export type CaptureMode = "ffmpeg" | "native";

/**
 * Media streaming configuration controlling video capture quality, timeouts, and concurrency limits.
 */
export interface StreamingConfig {

  // Audio bitrate in bits per second for the captured stream. Higher values improve audio quality but increase bandwidth requirements. 256kbps provides high-quality
  // stereo audio; lower values (128kbps) work for speech-heavy content. Environment variable: AUDIO_BITRATE. Default: 256000. Valid range: 32000-512000.
  audioBitsPerSecond: number;

  // Codecs allowed for browser capture. H.264 is always available as the universal baseline. HEVC provides better compression at the same bitrate when GPU hardware
  // encoding is available. The system selects the highest-priority allowed codec that the GPU supports. Environment variable: CAPTURE_CODECS. Default: ["h264", "hevc"].
  captureCodecs: string[];

  // The capture mode in effect. "ffmpeg", the capture mode every stream uses, captures Matroska (the effective capture codec plus Opus) and uses FFmpeg to
  // transcode audio to AAC. "native" names Chrome's direct fMP4 (H264+AAC) recording, which no capture path implements.
  // Environment variable: CAPTURE_MODE. Default: "ffmpeg". Every configuration built corrects any other value to "ffmpeg", and the warning is logged at startup
  // and by a save whose write lands, because Chrome's native fMP4 MediaRecorder corrupts output after 20-30 minutes of recording.
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
  // "720p", "720p-high", "1080p", "1080p-high", "4k". Bitrate and frame rate can be customized independently. Environment variable: QUALITY_PRESET. Default: "720p-high".
  qualityPreset: string;

  // Video bitrate in bits per second for browser capture. This controls the quality of the stream captured by puppeteer-stream. For HLS output, FFmpeg copies
  // the video stream directly without re-encoding, preserving this quality. 8Mbps is suitable for 720p content; 15-20Mbps is recommended for 1080p. The actual
  // bitrate may vary based on content complexity. Environment variable: VIDEO_BITRATE. Default: 12000000. Valid range: 100000-50000000.
  videoBitsPerSecond: number;

  // Timeout in milliseconds for waiting for a video element to become ready. After navigating to a page, we wait for a video element with sufficient readyState.
  // Increase for sites with slow-loading video players or heavy preroll content. Environment variable: VIDEO_TIMEOUT. Default: 11000ms. Valid range:
  // 1000-600000.
  videoTimeout: number;
}

/**
 * Root configuration object containing all application settings organized by functional area.
 */
export interface Config {

  // Chrome executable and launch timing.
  browser: BrowserConfig;

  // Channel table preferences, the service filter, precaching, setup completion, and which predefined channels are disabled.
  channels: ChannelsConfig;

  // Connection settings for the user's external Channels DVR server.
  channelsDvr: ChannelsDvrConfig;

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

/**
 * How a saved configuration value reaches the running process. Every leaf of the configuration carries exactly one class: a setting declares its own through
 * SettingMetadata.reactivity, which states the rule each class carries, and a field the process writes declares its own in its PROCESS_FIELDS entry.
 */
export type ReactivityClass = "live" | "next-stream" | "restart";

/**
 * The classes a field the process writes can carry, the classes PROCESS_FIELDS states. Those fields are state a subsystem or a separate endpoint writes, and none
 * is a per-stream tunable, so next-stream is not among them.
 */
export type ProcessFieldReactivity = Exclude<ReactivityClass, "next-stream">;
