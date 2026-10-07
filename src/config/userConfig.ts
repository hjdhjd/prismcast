/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userConfig.ts: User configuration file management for PrismCast.
 */
import type { Config, Nullable, ProcessFieldReactivity, ReactivityClass } from "../types/index.ts";
import { LOG, assertNever, sanitizeString } from "../utils/index.ts";
import type { CliOverrides } from "./index.ts";
import type { Migration } from "./persistence.ts";
import { createFileStore } from "./persistence.ts";
import { getConfigFilePath } from "./paths.ts";
import { getValidPresetIds } from "./presets.ts";
import { isDeepStrictEqual } from "node:util";

/* PrismCast stores user configuration in config.json inside the data directory (default: ~/.prismcast). This file allows users to customize settings without using
 * environment variables or CLI flags. The configuration system uses a layered approach with the following priority (highest to lowest):
 *
 * 1. CLI flags (--port, --chrome-data-dir, --log-file)
 * 2. Environment variables (SCREAMING_SNAKE_CASE naming)
 * 3. User config file (config.json in data directory)
 * 4. Hard-coded defaults (defined in DEFAULTS)
 *
 * This design follows the standard convention where CLI flags override everything. Docker deployments can use environment variables, standalone installations can
 * use the config file via the web UI at /config, and operators can always override any setting with a CLI flag.
 */

/* The millisecond spans this file states more than once: the upper bounds several bounded settings share, and the rolling windows DEFAULTS opens. Each is named
 * for the duration it expresses, so a bound and a window that happen to agree on a number still read as the separate durations they are.
 */
const FIVE_MINUTES_MS = 300000;
const TEN_MINUTES_MS = 600000;
const ONE_HOUR_MS = 3600000;

/* Each configurable setting has metadata describing its type, valid range, environment variable name, and human-readable description. This metadata is used by the
 * /config web UI to render appropriate form fields and validation, and by the validation system to check values before saving.
 */

/**
 * Metadata describing a single configuration setting. Note: Default values are not stored here to avoid duplication. Use getNestedValue(DEFAULTS, setting.path) to get
 * the default value for a setting.
 */
export interface SettingMetadata {

  // Path to a boolean setting that must be enabled for this setting to be active. When the referenced setting is false, this field is visually greyed out in the
  // UI. The field values are still submitted during save to avoid losing custom values when the parent toggle is temporarily disabled.
  dependsOn?: string;

  // Human-readable description shown in the UI.
  description: string;

  // When set, the field is disabled in the UI and this message is shown as a warning explaining why. The setting's value is forced to its default and cannot be
  // changed by the user. Used for temporarily disabling options due to upstream issues (e.g., Chrome bugs).
  disabledReason?: string;

  // Divisor for converting stored value to display value (e.g., 1000 to convert ms to seconds). When set, the UI displays value/displayDivisor and stores
  // submittedValue*displayDivisor.
  displayDivisor?: number;

  // Number of decimal places for display when using displayDivisor. Defaults to 0 for integers, 2 for floats.
  displayPrecision?: number;

  // Human-friendly unit for display when displayDivisor is set (e.g., "seconds" instead of "ms"). Overrides unit for display purposes.
  displayUnit?: string;

  // Environment variable that can override this setting, or null if not overridable.
  envVar: Nullable<string>;

  // Human-readable label for form fields.
  label: string;

  // Key identifying which list item provider to use when rendering a checkboxList. The provider is looked up in the LIST_ITEM_PROVIDERS registry in the settings
  // renderer. This keeps the config layer free of browser/runtime dependencies - the routes layer owns the registry and can safely import browser capabilities.
  listItemsKey?: string;

  // Maximum allowed value for numeric settings.
  max?: number;

  // Minimum allowed value for numeric settings.
  min?: number;

  // Dot-separated path to the setting (e.g., "browser.initTimeout").
  path: string;

  /* How a saved value reaches the running process. A live setting is read at its point of use or refreshed by a config-change handler, so a save commits it to
   * the running configuration once its handler realizes it. A next-stream setting is read once when a stream starts, so a save commits it for the streams that
   * start afterward. A restart setting is read once at boot or at a Chrome launch, so a save writes it to the file and holds it out of the running
   * configuration until a restart reads it. The class describes the readers as they stand: a setting whose reader changes declares the class that reader earns.
   */
  readonly reactivity: ReactivityClass;

  // Data type for validation and form field rendering.
  type: "boolean" | "checkboxList" | "float" | "host" | "integer" | "path" | "port" | "string";

  // Unit of measurement displayed in the UI (e.g., "ms", "bps").
  unit?: string;

  // Valid values for string type settings.
  validValues?: string[];
}

/**
 * Metadata for all configurable settings, organized by category.
 */
export const CONFIG_METADATA: Record<string, SettingMetadata[]> = {

  browser: [
    {

      description: "Path to Chrome executable. Leave empty to autodetect.",
      envVar: "CHROME_BIN",
      label: "Chrome Executable Path",
      path: "browser.executablePath",
      reactivity: "restart",
      type: "path"
    },
    {

      description: "Maximum wait after browser launch for the puppeteer-stream extension to initialize. The system polls for readiness and proceeds " +
        "early when ready. Increase if streams start with blank frames.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "BROWSER_INIT_TIMEOUT",
      label: "Browser Init Timeout",
      max: 30000,
      min: 100,
      path: "browser.initTimeout",
      reactivity: "restart",
      type: "integer",
      unit: "ms"
    }
  ],

  channels: [
    {

      description: "Speed up your first channel tune by loading service lineups when PrismCast starts. Normally, each service's lineup is loaded on your " +
        "first tune, which may add a few extra seconds. Enabling precaching fetches lineups at startup so even your very first tune is fast. Services not " +
        "enabled in the Channels tab service filter are skipped at startup. Ensure you've logged into each service before enabling.",
      envVar: null,
      label: "Channel Lineup Precaching",
      listItemsKey: "providerModules",
      path: "channels.precacheServices",
      reactivity: "restart",
      type: "checkboxList"
    }
  ],

  channelsDvr: [
    {

      description: "TCP port for the user's Channels DVR API. PrismCast polls Channels DVR over HTTP for show-info, device-mapping discovery, and pretune " +
        "scheduling. Defaults to 8089, which is the canonical Channels DVR port. Override only when the user has changed the DVR's listen port from its default.",
      envVar: "CHANNELS_DVR_PORT",
      label: "Channels DVR Port",
      max: 65535,
      min: 1,
      path: "channelsDvr.port",
      reactivity: "live",
      type: "port"
    }
  ],

  hdhr: [
    {

      description: "Enable HDHomeRun emulation. When enabled, PrismCast runs a second HTTP server that emulates an HDHomeRun tuner so Plex can use PrismCast " +
        "as a live TV source. With LAN discovery also enabled (below) Plex auto-detects PrismCast on the network; otherwise enter the address manually in " +
        "the form IP:port (e.g., 192.168.1.100:5004) via Plex's DVR setup screen.",
      envVar: "HDHR_ENABLED",
      label: "Enable HDHomeRun Emulation",
      path: "hdhr.enabled",
      reactivity: "live",
      type: "boolean"
    },
    {

      dependsOn: "hdhr.enabled",
      description: "Enable LAN discovery. When on, PrismCast responds to HDHomeRun discovery broadcasts on UDP port 65001 so Plex finds PrismCast on the " +
        "local network automatically. Channels DVR also auto-discovers but its discovery flow assumes the standard HDHomeRun port 80, which most " +
        "installations cannot bind - Channels DVR users should add PrismCast manually as a Custom Channels source instead of relying on auto-discovery. " +
        "Disable LAN discovery entirely in multi-tenant environments or when another real HDHomeRun device is present on the same network.",
      envVar: "HDHR_DISCOVERY_ENABLED",
      label: "Enable LAN Discovery",
      path: "hdhr.discoveryEnabled",
      reactivity: "live",
      type: "boolean"
    },
    {

      dependsOn: "hdhr.enabled",
      description: "TCP port for the HDHomeRun emulation server. This is the port you enter when manually adding the tuner in Plex (e.g., " +
        "192.168.1.100:5004). Setting this to 80 also enables Channels DVR's HDHomeRun auto-discovery, but ports below 1024 require elevated privileges to " +
        "bind on most platforms.",
      envVar: "HDHR_PORT",
      label: "HDHomeRun Port",
      max: 65535,
      min: 1,
      path: "hdhr.port",
      reactivity: "live",
      type: "port"
    },
    {

      dependsOn: "hdhr.enabled",
      description: "Display name shown in clients for this tuner. Helps identify PrismCast when you have multiple HDHomeRun devices.",
      envVar: "HDHR_FRIENDLY_NAME",
      label: "Friendly Name",
      path: "hdhr.friendlyName",
      reactivity: "live",
      type: "string"
    }
  ],

  hls: [
    {

      description: "Target duration for each HLS segment. Shorter segments reduce latency but increase overhead.",
      envVar: "HLS_SEGMENT_DURATION",
      label: "Segment Duration",
      max: 10,
      min: 1,
      path: "hls.segmentDuration",
      reactivity: "next-stream",
      type: "integer",
      unit: "seconds"
    },
    {

      description: "Maximum segments to keep in memory per stream. Controls buffer depth and memory usage.",
      envVar: "HLS_MAX_SEGMENTS",
      label: "Max Segments",
      max: 60,
      min: 3,
      path: "hls.maxSegments",
      reactivity: "live",
      type: "integer"
    },
    {

      description: "Time before an idle HLS stream is terminated. Applies when no segment requests are received.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "HLS_IDLE_TIMEOUT",
      label: "Idle Timeout",
      max: FIVE_MINUTES_MS,
      min: 10000,
      path: "hls.idleTimeout",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    }
  ],

  logging: [
    {

      description: "HTTP request logging level. \"none\" disables logging, \"errors\" logs only 4xx/5xx responses, \"filtered\" logs important requests while " +
        "skipping high-frequency endpoints, \"all\" logs everything.",
      envVar: "HTTP_LOG_LEVEL",
      label: "HTTP Log Level",
      path: "logging.httpLogLevel",
      reactivity: "restart",
      type: "string",
      validValues: [ "none", "errors", "filtered", "all" ]
    },
    {

      description: "Maximum log file size in bytes. When exceeded, the file is trimmed to half this size keeping the most recent logs.",
      displayDivisor: 1048576,
      displayPrecision: 1,
      displayUnit: "MB",
      envVar: "LOG_MAX_SIZE",
      label: "Max Log Size",
      max: 104857600,
      min: 524288,
      path: "logging.maxSize",
      reactivity: "restart",
      type: "integer",
      unit: "bytes"
    }
  ],

  paths: [
    {

      description: "Absolute path override for Chrome's user data directory. When set, Chrome profile data is stored at this path instead of the default " +
        "location inside the data directory. Useful for placing Chrome data on a different volume.",
      envVar: "PRISMCAST_CHROME_DATA_DIR",
      label: "Chrome Data Directory",
      path: "paths.chromeDataDir",
      reactivity: "restart",
      type: "path"
    },
    {

      description: "Absolute path override for the log file. When set, logs are written to this path instead of the default location inside the data directory.",
      envVar: "PRISMCAST_LOG_FILE",
      label: "Log File Path",
      path: "paths.logFile",
      reactivity: "restart",
      type: "path"
    }
  ],

  playback: [
    {

      description: "Grace period for buffering before declaring a stall. Prevents false positives from brief network hiccups.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "BUFFERING_GRACE_PERIOD",
      label: "Buffering Grace Period",
      max: 60000,
      min: 1000,
      path: "playback.bufferingGracePeriod",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Delay after clicking a channel selector before checking for video.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "CHANNEL_SELECTOR_DELAY",
      label: "Channel Selector Delay",
      max: 30000,
      min: 500,
      path: "playback.channelSelectorDelay",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Delay after channel switch for stream to stabilize before health monitoring begins.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "CHANNEL_SWITCH_DELAY",
      label: "Channel Switch Delay",
      max: 30000,
      min: 500,
      path: "playback.channelSwitchDelay",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Delay for iframe content to initialize before searching for video elements.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "IFRAME_INIT_DELAY",
      label: "Iframe Init Delay",
      max: 30000,
      min: 500,
      path: "playback.iframeInitDelay",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Maximum full page navigations allowed within the reload window. Prevents reload loops on broken streams.",
      envVar: "MAX_PAGE_RELOADS",
      label: "Max Page Reloads",
      max: 20,
      min: 1,
      path: "playback.maxPageReloads",
      reactivity: "live",
      type: "integer"
    },
    {

      description: "Interval between playback health checks. Shorter intervals detect problems faster but use more CPU.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "MONITOR_INTERVAL",
      label: "Monitor Interval",
      max: 30000,
      min: 500,
      path: "playback.monitorInterval",
      reactivity: "next-stream",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Time window for tracking page reload frequency. After this period, the reload counter resets.",
      displayDivisor: 60000,
      displayUnit: "minutes",
      envVar: "PAGE_RELOAD_WINDOW",
      label: "Page Reload Window",
      max: ONE_HOUR_MS,
      min: 60000,
      path: "playback.pageReloadWindow",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Delay after reloading video source before resuming monitoring.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "SOURCE_RELOAD_DELAY",
      label: "Source Reload Delay",
      max: 30000,
      min: 500,
      path: "playback.sourceReloadDelay",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Consecutive stalled checks before triggering recovery.",
      envVar: "STALL_COUNT_THRESHOLD",
      label: "Stall Count Threshold",
      max: 10,
      min: 1,
      path: "playback.stallCountThreshold",
      reactivity: "live",
      type: "integer"
    },
    {

      description: "Minimum change in video.currentTime (seconds) to consider playback progressing.",
      envVar: "STALL_THRESHOLD",
      label: "Stall Threshold",
      max: 5,
      min: 0.01,
      path: "playback.stallThreshold",
      reactivity: "live",
      type: "float",
      unit: "seconds"
    },
    {

      description: "Duration of healthy playback required before resetting escalation level. Prevents stutter loops.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "SUSTAINED_PLAYBACK_REQUIRED",
      label: "Sustained Playback Required",
      max: FIVE_MINUTES_MS,
      min: 10000,
      path: "playback.sustainedPlaybackRequired",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    }
  ],

  recovery: [
    {

      description: "Random jitter added to retry delays. Prevents thundering herd on retries.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "BACKOFF_JITTER",
      label: "Backoff Jitter",
      max: 10000,
      min: 0,
      path: "recovery.backoffJitter",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Failures within circuit breaker window that trigger stream termination.",
      envVar: "CIRCUIT_BREAKER_THRESHOLD",
      label: "Circuit Breaker Threshold",
      max: 100,
      min: 1,
      path: "recovery.circuitBreakerThreshold",
      reactivity: "live",
      type: "integer"
    },
    {

      description: "Time window for counting failures toward circuit breaker.",
      displayDivisor: 60000,
      displayUnit: "minutes",
      envVar: "CIRCUIT_BREAKER_WINDOW",
      label: "Circuit Breaker Window",
      max: ONE_HOUR_MS,
      min: 60000,
      path: "recovery.circuitBreakerWindow",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Maximum delay between retry attempts. Exponential backoff is capped at this value.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "MAX_BACKOFF_DELAY",
      label: "Max Backoff Delay",
      max: 60000,
      min: 1000,
      path: "recovery.maxBackoffDelay",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Failed browser relaunches within the window that trip the relaunch governor into a cooldown. The first failures relaunch immediately.",
      envVar: "RELAUNCH_FAILURE_THRESHOLD",
      label: "Relaunch Failure Threshold",
      max: 20,
      min: 1,
      path: "recovery.relaunchFailureThreshold",
      reactivity: "live",
      type: "integer"
    },
    {

      description: "Time window for counting failed browser relaunches toward the governor trip.",
      displayDivisor: 60000,
      displayUnit: "minutes",
      envVar: "RELAUNCH_FAILURE_WINDOW",
      label: "Relaunch Failure Window",
      max: ONE_HOUR_MS,
      min: 60000,
      path: "recovery.relaunchFailureWindow",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Continuous capture-readiness required before the relaunch governor resets to its normal state.",
      displayDivisor: 60000,
      displayUnit: "minutes",
      envVar: "RELAUNCH_HEALTH_HOLD",
      label: "Relaunch Health Hold",
      max: TEN_MINUTES_MS,
      min: 60000,
      path: "recovery.relaunchHealthHold",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Interval between stale page cleanup runs. Identifies and closes orphaned browser pages.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "STALE_PAGE_CLEANUP_INTERVAL",
      label: "Stale Page Cleanup Interval",
      max: TEN_MINUTES_MS,
      min: 10000,
      path: "recovery.stalePageCleanupInterval",
      reactivity: "restart",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Grace period before closing a page that appears stale. Prevents race conditions during initialization.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "STALE_PAGE_GRACE_PERIOD",
      label: "Stale Page Grace Period",
      max: 120000,
      min: 5000,
      path: "recovery.stalePageGracePeriod",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    }
  ],

  server: [
    {

      description: "IP address to bind the HTTP server. Use 0.0.0.0 for all interfaces, 127.0.0.1 for local only.",
      envVar: "HOST",
      label: "Host",
      path: "server.host",
      reactivity: "restart",
      type: "host"
    },
    {

      description: "TCP port for the HTTP server. Channels DVR and other clients connect here.",
      envVar: "PORT",
      label: "Port",
      max: 65535,
      min: 1,
      path: "server.port",
      reactivity: "restart",
      type: "port"
    }
  ],

  streaming: [
    {

      description: "FFmpeg (recommended) provides reliable capture for long recordings. Native mode captures directly from Chrome without an external " +
        "process, but may require stream recovery after 20-30 minutes of continuous use.",
      disabledReason: "Native capture mode is temporarily disabled due to a Chrome bug that causes fMP4 MediaRecorder to produce corrupt output after " +
        "20-30 minutes of continuous recording. FFmpeg mode is required until a future Chrome release resolves this issue.",
      envVar: "CAPTURE_MODE",
      label: "Capture Mode",
      path: "streaming.captureMode",
      reactivity: "restart",
      type: "string",
      validValues: [ "ffmpeg", "native" ]
    },
    {

      description: "Video codecs allowed for browser capture. H.264 is always available as the universal baseline. Additional codecs require GPU hardware " +
        "encoding support - codecs without hardware support are shown as disabled.",
      envVar: "CAPTURE_CODECS",
      label: "Capture Codecs",
      listItemsKey: "captureCodecs",
      path: "streaming.captureCodecs",
      reactivity: "restart",
      type: "checkboxList"
    },
    {

      description: "Video quality preset. Determines capture resolution. Bitrate and frame rate can be further customized.",
      envVar: "QUALITY_PRESET",
      label: "Quality Preset",
      path: "streaming.qualityPreset",
      reactivity: "restart",
      type: "string",
      validValues: getValidPresetIds()
    },
    {

      description: "Audio bitrate for browser capture. HLS copies this stream directly (no re-encoding). 256kbps provides high-quality stereo audio.",
      displayDivisor: 1000,
      displayUnit: "kbps",
      envVar: "AUDIO_BITRATE",
      label: "Audio Bitrate",
      max: 512000,
      min: 32000,
      path: "streaming.audioBitsPerSecond",
      reactivity: "next-stream",
      type: "integer",
      unit: "bps"
    },
    {

      description: "Target frame rate. 60fps is ideal for sports; 30fps works for most TV content.",
      envVar: "FRAME_RATE",
      label: "Frame Rate",
      max: 60,
      min: 30,
      path: "streaming.frameRate",
      reactivity: "next-stream",
      type: "integer",
      unit: "fps"
    },
    {

      description: "Maximum simultaneous streams. Each stream uses a browser tab and resources.",
      envVar: "MAX_CONCURRENT_STREAMS",
      label: "Max Concurrent Streams",
      max: 100,
      min: 1,
      path: "streaming.maxConcurrentStreams",
      reactivity: "live",
      type: "integer"
    },
    {

      description: "Maximum navigation retry attempts before giving up.",
      envVar: "MAX_NAV_RETRIES",
      label: "Max Navigation Retries",
      max: 50,
      min: 1,
      path: "streaming.maxNavigationRetries",
      reactivity: "live",
      type: "integer"
    },
    {

      description: "Timeout for page navigation. Increase for slow networks or heavy pages.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "NAV_TIMEOUT",
      label: "Navigation Timeout",
      max: TEN_MINUTES_MS,
      min: 1000,
      path: "streaming.navigationTimeout",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    },
    {

      description: "Video bitrate for browser capture. HLS copies this stream directly (no re-encoding). 8Mbps suits 720p; 15-20Mbps for 1080p.",
      displayDivisor: 1000000,
      displayUnit: "Mbps",
      envVar: "VIDEO_BITRATE",
      label: "Video Bitrate",
      max: 50000000,
      min: 100000,
      path: "streaming.videoBitsPerSecond",
      reactivity: "next-stream",
      type: "integer",
      unit: "bps"
    },
    {

      description: "Timeout for video element to become ready after navigation.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "VIDEO_TIMEOUT",
      label: "Video Timeout",
      max: TEN_MINUTES_MS,
      min: 1000,
      path: "streaming.videoTimeout",
      reactivity: "live",
      type: "integer",
      unit: "ms"
    }
  ]
};

/* The user config file stores partial configuration - only the settings that differ from defaults. All fields are optional because missing fields use defaults.
 */

/**
 * Partial browser configuration for user config file.
 */
export interface UserBrowserConfig {

  executablePath?: Nullable<string>;
  initTimeout?: number;
}

/**
 * Partial HLS configuration for user config file.
 */
export interface UserHLSConfig {

  idleTimeout?: number;
  maxSegments?: number;
  segmentDuration?: number;
}

/**
 * Partial logging configuration for user config file.
 */
export interface UserLoggingConfig {

  debugFilter?: string;
  maxSize?: number;
}

/**
 * Partial playback configuration for user config file.
 */
export interface UserPlaybackConfig {

  bufferingGracePeriod?: number;
  channelSelectorDelay?: number;
  channelSwitchDelay?: number;
  iframeInitDelay?: number;
  maxPageReloads?: number;
  monitorInterval?: number;
  pageReloadWindow?: number;
  sourceReloadDelay?: number;
  stallCountThreshold?: number;
  stallThreshold?: number;
  sustainedPlaybackRequired?: number;
}

/**
 * Partial recovery configuration for user config file.
 */
export interface UserRecoveryConfig {

  backoffJitter?: number;
  circuitBreakerThreshold?: number;
  circuitBreakerWindow?: number;
  maxBackoffDelay?: number;
  relaunchFailureThreshold?: number;
  relaunchFailureWindow?: number;
  relaunchHealthHold?: number;
  stalePageCleanupInterval?: number;
  stalePageGracePeriod?: number;
}

/**
 * Partial server configuration for user config file.
 */
export interface UserServerConfig {

  host?: string;
  port?: number;
}

/**
 * Partial streaming configuration for user config file.
 */
export interface UserStreamingConfig {

  audioBitsPerSecond?: number;
  captureCodecs?: string[];
  captureMode?: string;
  frameRate?: number;
  maxConcurrentStreams?: number;
  maxNavigationRetries?: number;
  navigationTimeout?: number;
  qualityPreset?: string;
  videoBitsPerSecond?: number;
  videoTimeout?: number;
}

/**
 * Partial channels configuration for user config file.
 */
export interface UserChannelsConfig {

  // Sort direction for the channels table.
  channelSortDirection?: string;

  // Sort field for the channels table.
  channelSortField?: string;

  // List of predefined channel keys that are disabled.
  disabledPredefined?: string[];

  // Service tags that are enabled for filtering. Empty means no filter.
  enabledServices?: string[];

  // Service slugs selected for precaching at startup. Empty means no precaching.
  precacheServices?: string[];

  // Whether the Service Setup flow has been completed or skipped.
  setupCompleted?: boolean;

  // Optional column field names currently visible in the channels table.
  visibleColumns?: string[];
}

/**
 * Partial HDHomeRun configuration for user config file.
 */
export interface UserHdhrConfig {

  deviceId?: string;
  discoveryEnabled?: boolean;
  enabled?: boolean;
  friendlyName?: string;
  port?: number;
}

/**
 * Partial Channels DVR connection configuration for user config file.
 */
export interface UserChannelsDvrConfig {

  host?: string;
  port?: number;
}

/**
 * Partial paths configuration for user config file.
 */
export interface UserPathsConfig {

  chromeDataDir?: Nullable<string>;
  logFile?: Nullable<string>;
}

/**
 * User configuration with all fields optional. This is the structure of the config.json file. The schemaVersion and migrationsApplied fields are managed by
 * the file store framework's migration runner; consumers should treat them as opaque metadata.
 */
export interface UserConfig {

  browser?: UserBrowserConfig;
  channels?: UserChannelsConfig;
  channelsDvr?: UserChannelsDvrConfig;
  hdhr?: UserHdhrConfig;
  hls?: UserHLSConfig;
  logging?: UserLoggingConfig;

  // Audit trail of schema migrations applied to this file, in order. Managed by the file store framework's migration runner.
  migrationsApplied?: string[];

  paths?: UserPathsConfig;
  playback?: UserPlaybackConfig;
  recovery?: UserRecoveryConfig;

  // Schema version. Managed by the file store framework's migration runner. Files predating this field are treated as version 1.
  schemaVersion?: number;

  server?: UserServerConfig;
  streaming?: UserStreamingConfig;
}

/**
 * Result of loading user config, includes parse error flag for UI display.
 */
export interface UserConfigLoadResult {

  // The loaded configuration (empty object if file missing or parse error).
  config: UserConfig;

  // True if the config file exists but contains invalid JSON.
  parseError: boolean;

  // Error message if parseError is true.
  parseErrorMessage?: string;

  // True if the config file exists but could not be read for a reason other than its absence (a permission or I/O failure). The config is then the defaults,
  // which describe nothing about the file, so the store refuses to write over it.
  readError: boolean;
}

/* The config file path is resolved via the centralized paths module (config/paths.ts). The data directory is initialized at startup before config loading.
 * Configuration persistence uses a transactional file store that provides atomic writes, serialized mutations, corruption protection, and backup rotation.
 * All config modifications go through mutateConfig(), which prevents the class of bugs where a corrupt file gets silently overwritten with nearly-empty data.
 */

/* Current schema version for config.json. Migrations are declared in configMigrations below; the framework runs them in order from the file's stored version
 * up to this constant, stamps the new version after each, and records the audit trail in migrationsApplied.
 *
 * Version history:
 *   1 - Original. Provider-themed channel field names ("enabledProviders", "precacheProviders") and "foxcom" service tag still present.
 *   2 - Service-themed naming. Renames "enabledProviders" -> "enabledServices", "precacheProviders" -> "precacheServices", and "foxcom" -> "foxone" inside
 *       the channels.enabledServices array. Companion to channels.json v3 which renames foxcom in channel keys and selections.
 *   3 - DVR connection namespace. Moves the top-level `dvrHost` field into `channelsDvr.host`, splitting any legacy `host:port` value so the host portion
 *       lands at `channelsDvr.host` (host-only) and the port portion lands at `channelsDvr.port` only when the user has not already customized the port.
 */
const CURRENT_CONFIG_SCHEMA_VERSION = 3;

/* Declarative schema migrations. The file store framework runs these in order from the file's stored schemaVersion up to CURRENT_CONFIG_SCHEMA_VERSION,
 * stamping the new version and recording the description in migrationsApplied after each application. Apply functions mutate the data in place.
 */
const configMigrations: Record<number, Migration<UserConfig>> = {

  2: {

    apply: applyChannelsProviderRenameMigration,
    description: "Rename legacy provider-themed channel field names and foxcom service tag to foxone"
  },

  3: {

    apply: applyDvrHostNamespaceMigration,
    description: "Move dvrHost into channelsDvr.host (split legacy host:port format)"
  }
};

/**
 * Renames the legacy provider-themed channel fields to their current service-themed names: `enabledProviders` -> `enabledServices`,
 * `precacheProviders` -> `precacheServices`, and the `"foxcom"` tag -> `"foxone"` inside `enabledServices`. When both legacy and current names coexist on disk
 * (a rare hand-edited collision), the current name wins and the legacy key is deleted - operator intent on the new name takes precedence over the auto-rename.
 *
 * Behavioral contract: this is a pure transformation on the on-disk shape. Configs without a `channels` block are left untouched (early return); configs that
 * already have the v2 shape (no legacy keys present) are unchanged at the field level (repeat runs make no further change), though the foxcom-to-foxone map
 * runs unconditionally over the present `enabledServices` array because that operation is itself a no-op for already-migrated tags.
 *
 * Exported for unit-test coverage of the rename cases (each rename, collision wins, foxcom remap, no-channels early return). Production callers reach this
 * only through the schema migration runner, never directly.
 *
 * @param data - The pre-migration UserConfig data, mutated in place.
 */
export function applyChannelsProviderRenameMigration(data: UserConfig): void {

  // Cast to a permissive shape because the legacy provider-themed keys are not declared on UserChannelsConfig - they only exist on older on-disk files.
  const channels = data.channels as Record<string, unknown> | undefined;

  if(!channels) {

    return;
  }

  // Rename enabledProviders -> enabledServices. If both are present (rare hand-edited case) the current name wins.
  if(Array.isArray(channels["enabledProviders"])) {

    if(!Array.isArray(channels["enabledServices"])) {

      channels["enabledServices"] = channels["enabledProviders"];
    }

    delete channels["enabledProviders"];
  }

  // Rename precacheProviders -> precacheServices.
  if(Array.isArray(channels["precacheProviders"])) {

    if(!Array.isArray(channels["precacheServices"])) {

      channels["precacheServices"] = channels["precacheProviders"];
    }

    delete channels["precacheProviders"];
  }

  // Rename "foxcom" -> "foxone" inside the enabledServices service-tag filter.
  if(Array.isArray(channels["enabledServices"])) {

    channels["enabledServices"] = (channels["enabledServices"] as string[]).map((tag) => (tag === "foxcom") ? "foxone" : tag);
  }
}

/**
 * Splits a legacy `dvrHost` value into the new `channelsDvr.host` / `channelsDvr.port` namespace. The host portion is always host-only, never host:port;
 * legacy values carrying an embedded port (e.g., `192.168.1.5:8089`) are split so the host lands at `channelsDvr.host` and the port lands at
 * `channelsDvr.port` IFF the user has not already customized the port. When both an embedded port and an explicit user-set port exist, the explicit port
 * wins (a warning is logged) and the embedded port is discarded - the user's deliberate choice takes precedence over implicit legacy data.
 *
 * Splits at the LAST colon so bracket-wrapped IPv6 forms like `[::1]:8089` survive (the production setDvrHost function always rejects bare-colon values,
 * but disk files may carry hand-edited content the framework cannot vet). When the trailing portion does not parse as a valid port number (1..65535), the
 * entire input is treated as a host-only value rather than fabricating a bogus port.
 *
 * Exported for unit-test coverage of the splitting cases (host-only, host+default-port, host+non-default-port collision). Production callers reach this only
 * through the schema migration runner, never directly.
 *
 * @param data - The pre-migration UserConfig data, mutated in place.
 */
export function applyDvrHostNamespaceMigration(data: UserConfig): void {

  // The legacy field is not declared on the current UserConfig; we read it through a permissive cast to access the on-disk pre-migration shape. After migration
  // the field is deleted so it cannot leak into the post-migration write path.
  const legacy = data as { dvrHost?: unknown };
  const dvrHost = legacy.dvrHost;

  if(typeof dvrHost !== "string") {

    return;
  }

  let host = dvrHost;
  let embeddedPort: number | undefined;

  // Split at the last colon so bracket-wrapped IPv6 (e.g., [::1]:8089) survives. If the trailing portion does not parse as a valid TCP port, treat the whole
  // input as host-only - avoid fabricating a port from a malformed string.
  const colonIdx = dvrHost.lastIndexOf(":");

  if(colonIdx >= 0) {

    const tail = dvrHost.slice(colonIdx + 1);
    const parsed = Number(tail);

    if(Number.isInteger(parsed) && (parsed >= 1) && (parsed <= 65535)) {

      host = dvrHost.slice(0, colonIdx);
      embeddedPort = parsed;
    }
  }

  data.channelsDvr ??= {};
  data.channelsDvr.host = host;

  // Migrate the embedded port only when the user has not customized the port already. A user-set port reflects an explicit choice that overrides whatever the
  // legacy host:port string encoded - prefer the explicit value and log the conflict so it is auditable.
  if(embeddedPort !== undefined) {

    const userPort = data.channelsDvr.port;

    if(userPort === undefined) {

      data.channelsDvr.port = embeddedPort;
    } else if(userPort !== embeddedPort) {

      LOG.warn("Schema migration v3: legacy dvrHost \"%s\" carried embedded port %d but channelsDvr.port is already set to %d. " +
        "Keeping the explicit port; embedded port discarded.", dvrHost, embeddedPort, userPort);
    }
  }

  delete legacy.dvrHost;
}

// Transactional store instance for config.json. The beforeWrite hook is the single chokepoint where the persisted shape is normalized: filterDefaults() runs on
// every save so the file on disk contains only non-default values, regardless of which call site initiated the write. Schema migrations run automatically via
// the file store framework's migration runner before the data reaches mergeConfiguration.
const configStore = createFileStore<UserConfig>({

  beforeWrite: (data: UserConfig): UserConfig => filterDefaults(data),
  currentSchemaVersion: CURRENT_CONFIG_SCHEMA_VERSION,
  defaultValue: (): UserConfig => ({ schemaVersion: CURRENT_CONFIG_SCHEMA_VERSION }),
  getSchemaVersion: (data: UserConfig): number => data.schemaVersion ?? 1,
  label: "configuration",
  migrations: configMigrations,
  parse: (raw: string): UserConfig => JSON.parse(raw) as UserConfig,
  path: getConfigFilePath,
  recordMigration: (data: UserConfig, description: string): void => {

    data.migrationsApplied ??= [];
    data.migrationsApplied.push(description);
  },
  setSchemaVersion: (data: UserConfig, version: number): void => { data.schemaVersion = version; }
});

/**
 * Reads the current configuration from disk without acquiring the serialization lock. Returns the parsed (and migrated) config with parse status. Use this for
 * read-only access (export endpoints, startup initialization). For modifications, use mutateConfig() instead.
 * @returns The loaded configuration with parse status.
 */
export async function readConfig(): Promise<UserConfigLoadResult> {

  const result = await configStore.read();

  return {

    config: result.data,
    parseError: result.parseError,
    parseErrorMessage: result.parseErrorMessage,
    readError: result.readError
  };
}

/**
 * Serialized read-modify-write operation on config.json. The mutation function receives the current config (already migrated to the latest schema version) and
 * modifies it in place. The store handles atomicity, serialization, corruption guard, backup, schema migration, and filterDefaults via the framework.
 *
 * This writes the file and nothing else. It is the write path for the leaves the process owns, the discovered DVR host and the generated DeviceID among them,
 * and for the boot's capture-coercion write-back, and each of those callers keeps the running configuration in step with its own write. A write to the
 * settings surface goes through saveConfiguration() in config/index.ts instead, which runs its mutation through this store, refuses an invalid result before
 * anything reaches disk, and reconciles the running configuration against the file it wrote.
 * @param fn - Mutation function. Receives current config. Modify in place; return value is ignored. A throw inside it writes nothing.
 * @throws FileStoreParseError if config.json contains invalid JSON and no usable backup exists, and an Error if config.json could not be read.
 */
export async function mutateConfig(fn: (current: UserConfig) => void): Promise<void> {

  await configStore.mutate(fn);
}

/* These functions detect which settings are overridden by environment variables, so the UI can disable those fields and show appropriate warnings.
 */

/**
 * Returns a map of setting paths to the display form of the environment override the merge applied, for every setting the environment overrides. The value is
 * derived from the parsed override rather than the raw variable text, so the map describes what the configuration actually holds: a variable whose text does
 * not parse for its setting's type is absent, because the merge does not apply it either, and a host, path, or free-string value appears sanitized because
 * that is the text the merge stored. A boolean or a number renders through String(), a comma-separated list renders joined on commas, and the null that a
 * cleared path setting resolves to renders as the empty string. The UI relies on both halves: presence decides whether a field is disabled, and the value is
 * what the override badge shows beside it.
 * @returns Map of path -> the applied override's display form, for the settings the environment overrides.
 */
export function getEnvOverrides(): Map<string, string> {

  const overrides = new Map<string, string>();

  for(const settings of Object.values(CONFIG_METADATA)) {

    for(const setting of settings) {

      const override = resolveEnvOverride(setting);

      /* Every member of EnvOverride has an arm, and the default arm's assertNever is what keeps that true: a member added to the union without an arm here
       * reaches the default as something other than never, which the compiler refuses, so the omission is a build failure rather than a silent no-op.
       */
      switch(override.kind) {

        case "absent":
        case "unparseable": {

          // A variable nobody set and a value the merge refused to apply are both the same thing here: not an override. The field stays editable, and there is
          // no badge, because there is no applied value to describe.
          break;
        }

        case "value": {

          const { value } = override;

          // An array joins on the separator parseEnvValue split it with, so the badge shows the text an operator could paste back into the variable, and the
          // null a cleared path setting resolves to has no text to show at all.
          overrides.set(setting.path, Array.isArray(value) ? value.join(",") : ((value === null) ? "" : String(value)));

          break;
        }

        default: {

          assertNever(override);
        }
      }
    }
  }

  return overrides;
}

/* These functions merge defaults, user config, and environment overrides into the final CONFIG object.
 */

/**
 * Hard-coded default configuration values. These are the baseline values used when neither user config nor environment variables provide a value.
 */
export const DEFAULTS: Config = {

  browser: {

    executablePath: null,
    initTimeout: 3000
  },

  channels: {

    channelSortDirection: "asc",
    channelSortField: "name",
    disabledPredefined: [],
    enabledServices: [],
    precacheServices: [],
    setupCompleted: false,
    visibleColumns: []
  },

  channelsDvr: {

    host: "",
    port: 8089
  },

  hdhr: {

    deviceId: "",
    discoveryEnabled: true,
    enabled: true,
    friendlyName: "PrismCast",
    port: 5004
  },

  hls: {

    idleTimeout: 30000,
    maxSegments: 10,
    segmentDuration: 2
  },

  logging: {

    debugFilter: "",
    httpLogLevel: "errors",
    maxSize: 1048576
  },

  paths: {

    chromeDataDir: null,
    logFile: null
  },

  playback: {

    bufferingGracePeriod: 10000,
    channelSelectorDelay: 5000,
    channelSwitchDelay: 4000,
    iframeInitDelay: 1500,
    maxPageReloads: 3,
    monitorInterval: 2000,
    pageReloadWindow: 900000,
    sourceReloadDelay: 2000,
    stallCountThreshold: 2,
    stallThreshold: 0.1,
    sustainedPlaybackRequired: 60000
  },

  recovery: {

    backoffJitter: 1000,
    circuitBreakerThreshold: 10,
    circuitBreakerWindow: FIVE_MINUTES_MS,
    maxBackoffDelay: 3000,
    relaunchFailureThreshold: 3,
    relaunchFailureWindow: FIVE_MINUTES_MS,
    relaunchHealthHold: 120000,
    stalePageCleanupInterval: 60000,
    stalePageGracePeriod: 30000
  },

  server: {

    host: "0.0.0.0",
    port: 5589
  },

  streaming: {

    audioBitsPerSecond: 256000,
    captureCodecs: [ "h264", "hevc" ],
    captureMode: "ffmpeg",
    frameRate: 60,
    maxConcurrentStreams: 10,
    maxNavigationRetries: 4,
    navigationTimeout: 10000,
    qualityPreset: "720p-high",
    videoBitsPerSecond: 12000000,
    videoTimeout: 11000
  }
};

/**
 * Parses an environment variable value according to the setting type.
 * @param value - The raw environment variable value.
 * @param type - The expected type of the setting.
 * @returns The parsed value, or undefined if parsing fails.
 */
function parseEnvValue(value: string, type: SettingMetadata["type"]): Nullable<boolean | number | string | string[]> | undefined {

  /* The host, path, and free-string arms below run their value through the shared data-collection sanitizer, which is the treatment the settings form and the
   * config import already give those same types. The environment deserves it for the same reason they do - a value carrying padding or a non-printable
   * character is almost never what the operator meant - and it deserves it more, because this is the one ingress with no validation behind it: an environment
   * value is written straight into the configuration, so anything unusual in it lands there silently instead of being refused with a message.
   */
  switch(type) {

    case "boolean": {

      // Accept common truthy values for environment variables.
      const lower = value.toLowerCase();

      return (lower === "true") || (lower === "1") || (lower === "yes");
    }

    case "float": {

      const num = parseFloat(value);

      return Number.isNaN(num) ? undefined : num;
    }

    case "integer":
    case "port": {

      const num = parseInt(value, 10);

      return Number.isNaN(num) ? undefined : num;
    }

    case "host": {

      return sanitizeString(value);
    }

    case "checkboxList": {

      // Accept comma-separated values (e.g., "h264,hevc").
      return value.split(",").map((v) => v.trim()).filter((v) => v.length > 0);
    }

    case "path": {

      /* An empty path env var means "use default" - return null so the downstream code sees the same sentinel as an unset config field. The sanitized value is
       * computed once and used for both the emptiness test and the return, so the sentinel decision and the stored value are made from the same text.
       */
      const sanitized = sanitizeString(value);

      return (sanitized === "") ? null : sanitized;
    }

    case "string": {

      // A free-form string setting takes the environment variable's value, cleaned of padding and non-printable characters.
      return sanitizeString(value);
    }

    default: {

      throw new Error("Unsupported setting type: " + String(type) + ".");
    }
  }
}

/* Every reader of the environment layer asks the same question - what does the environment contribute to this setting - and the answer is computed in exactly
 * one place. The resolver reports; the callers act. The configuration merge acts by storing the value and by reporting a variable it had to discard, because
 * the merge is where configuration is assembled and where discarding something is an event. The settings form acts by deciding which fields the operator may
 * not edit and what text their override badges carry, and it reports nothing, because reading the environment to render a page is not an event. Both receive
 * the same answer, which is what makes the badge describe the configuration it sits beside rather than the two of them agreeing to read process.env alike.
 */

/**
 * What the environment contributes to one setting, as a report each caller acts on for itself. `absent` means the variable is unset. `unparseable` means the
 * variable is set but its text is not a value of the setting's type, and carries that text along so a caller can name it. `value` carries what the text
 * parsed to.
 */
type EnvOverride = { kind: "absent" } | { kind: "unparseable"; text: string } | { kind: "value"; value: Nullable<boolean | number | string | string[]> };

/**
 * Resolves what a setting's environment variable contributes to the configuration, without acting on it - no logging and no state, so the same call answers
 * the merge and the settings form identically however many times either one asks.
 * @param setting - The setting whose environment variable is consulted.
 * @returns The resolution: absent, unparseable with the offending text, or the parsed value.
 */
function resolveEnvOverride(setting: SettingMetadata): EnvOverride {

  const envVar = setting.envVar;

  if(!envVar) {

    return { kind: "absent" };
  }

  const envValue = process.env[envVar];

  if(envValue === undefined) {

    return { kind: "absent" };
  }

  const parsed = parseEnvValue(envValue, setting.type);

  // The variable is set but its text is not a value of the setting's type, so it is not an override and the configuration layer below still owns the setting.
  if(parsed === undefined) {

    return { kind: "unparseable", text: envValue };
  }

  return { kind: "value", value: parsed };
}

/**
 * Returns what the environment contributes to one setting, for a caller that wants the value and nothing else. An unset variable, text that is not a value of
 * the setting's type, and a path that names no setting all yield undefined. Reading an unparseable value here is not an event to report: the configuration
 * merge has already named it.
 * @param settingPath - The dot-separated path of the setting whose environment variable is consulted.
 * @returns The parsed value the environment contributes, or undefined when it contributes none.
 */
export function getEnvOverrideValue(settingPath: string): Nullable<boolean | number | string | string[]> | undefined {

  const setting = getSettingByPath(settingPath);

  if(!setting) {

    return undefined;
  }

  const override = resolveEnvOverride(setting);

  return (override.kind === "value") ? override.value : undefined;
}

/**
 * Gets a value from a nested object using a dot-separated path.
 * @param obj - The object to read from.
 * @param settingPath - Dot-separated path (e.g., "browser.viewport.width").
 * @returns The value at the path, or undefined if not found.
 */
export function getNestedValue(obj: unknown, settingPath: string): unknown {

  const parts = settingPath.split(".");
  let current: unknown = obj;

  for(const part of parts) {

    if((current === null) || (current === undefined) || (typeof current !== "object")) {

      return undefined;
    }

    current = (current as Record<string, unknown>)[part];
  }

  return current;
}

/**
 * Sets a value in a nested object using a dot-separated path, creating intermediate objects as needed.
 * @param obj - The object to modify.
 * @param settingPath - Dot-separated path (e.g., "browser.viewport.width").
 * @param value - The value to set.
 */
export function setNestedValue(obj: Record<string, unknown>, settingPath: string, value: unknown): void {

  const parts = settingPath.split(".");
  let current = obj;

  for(const part of parts.slice(0, -1)) {

    current[part] ??= {};

    current = current[part] as Record<string, unknown>;
  }

  current[parts.at(-1) ?? ""] = value;
}

/**
 * Merges user configuration with defaults, environment overrides, and CLI overrides to produce the final configuration.
 * Priority (highest to lowest): CLI overrides > env vars > user config > defaults.
 * @param userConfig - User configuration from the config file.
 * @param cliOverrides - Optional CLI flag overrides, applied at the highest priority level.
 * @returns The merged configuration.
 */
export function mergeConfiguration(userConfig: UserConfig, cliOverrides?: CliOverrides): Config {

  // Start with a deep copy of defaults.
  const config = structuredClone(DEFAULTS);

  // Apply user config values.
  for(const settings of Object.values(CONFIG_METADATA)) {

    for(const setting of settings) {

      const userValue = getNestedValue(userConfig, setting.path);

      if(userValue === undefined) {

        continue;
      }

      const defaultValue = getNestedValue(DEFAULTS, setting.path);

      if(!Array.isArray(defaultValue)) {

        setNestedValue(config as unknown as Record<string, unknown>, setting.path, userValue);

        continue;
      }

      /* A list setting takes the shape rule isEqualToDefault() applies. A stored value that is not an array counts as absent, so the default stays. An array
       * merges as a clone, and as the default's own clone when it holds the default's members in any order, so the running configuration never shares a
       * value with the parsed file, and it holds what the file round-trips to: a list equal to the default is dropped from the file and read back as the
       * default, in the default's order.
       */
      if(hasDefaultShape(userValue, defaultValue)) {

        setNestedValue(config as unknown as Record<string, unknown>, setting.path, structuredClone(isEqualToDefault(userValue, defaultValue) ? defaultValue : userValue));
      }
    }
  }

  /* Bring back the state fields the process writes (auto-discovery results like channelsDvr.host, separately-managed lists like channels.disabledPredefined,
   * the persisted debug filter pattern), each by the rule PROCESS_FIELDS states: a stored value with its default's shape that is not the empty string, written
   * as a clone so the running configuration never shares a value with the parsed file. A schema field exists only in the file and is never brought back. This
   * runs after the metadata loop and before the environment and CLI layers, which reach metadata paths only, so those layers still win wherever they apply.
   */
  for(const [ fieldPath, field ] of Object.entries(PROCESS_FIELDS)) {

    switch(field.kind) {

      case "schema": {

        break;
      }

      case "state": {

        const userValue = getNestedValue(userConfig, fieldPath);

        if(hasDefaultShape(userValue, getNestedValue(DEFAULTS, fieldPath)) && (userValue !== "")) {

          setNestedValue(config as unknown as Record<string, unknown>, fieldPath, structuredClone(userValue));
        }

        break;
      }

      default: {

        assertNever(field);
      }
    }
  }

  /* Apply environment variable overrides. This loop is the one place that acts on what the resolver reports, because this is where configuration is assembled -
   * which is also why the warning below belongs here and nowhere else. Every member of EnvOverride has an arm, and the default arm's assertNever is what keeps
   * that true: a member added to the union without an arm here reaches the default as something other than never, which the compiler refuses, so the omission
   * is a build failure rather than a silent no-op.
   */
  for(const settings of Object.values(CONFIG_METADATA)) {

    for(const setting of settings) {

      const override = resolveEnvOverride(setting);

      switch(override.kind) {

        case "absent": {

          // The environment says nothing about this setting, so whatever the user file or the defaults supplied stands.
          break;
        }

        case "unparseable": {

          /* An operator who took the trouble to set the variable deserves to hear that it was discarded. Once per merge is the right cardinality: a merge is a
           * boot or a save to the settings, each of them an operator's own action, so the line arrives when they would look for it and never on its own.
           */
          LOG.warn("Ignoring the %s environment variable: \"%s\" is not a valid %s value for %s.", setting.envVar, override.text, setting.type, setting.path);

          break;
        }

        case "value": {

          setNestedValue(config as unknown as Record<string, unknown>, setting.path, override.value);

          break;
        }

        default: {

          assertNever(override);
        }
      }
    }
  }

  // Apply CLI overrides (highest priority). These are already parsed values keyed by CONFIG_METADATA paths.
  if(cliOverrides) {

    for(const [ overridePath, value ] of Object.entries(cliOverrides)) {

      if(value !== undefined) {

        setNestedValue(config as unknown as Record<string, unknown>, overridePath, value);
      }
    }
  }

  return config;
}

/* The configuration UI uses a simplified two-tab structure: Settings (common options) and Advanced (expert tuning). Rather than annotating every setting with UI
 * placement, we explicitly list the Settings tab contents and derive everything else.
 *
 * Architecture:
 * - CONFIG_METADATA is the single source of truth for what settings exist
 * - SETTINGS_TAB_SECTIONS defines the Settings tab with explicit sections and paths (non-collapsible visual groupings)
 * - Advanced tab contains everything NOT in SETTINGS_TAB_SECTIONS, grouped by storage category (collapsible sections)
 * - ADVANCED_SECTION_META defines Advanced section display names and order
 *
 * Common tasks:
 * - Add a new setting: Add to CONFIG_METADATA under the appropriate category. It automatically appears in the Advanced tab under the matching section.
 * - Promote a setting to Settings tab: Add its path to the appropriate section in SETTINGS_TAB_SECTIONS. It moves from Advanced to Settings.
 * - Reorder Settings sections: Reorder entries in SETTINGS_TAB_SECTIONS.
 * - Reorder Advanced sections: Reorder entries in ADVANCED_SECTION_META.
 * - Add a new Advanced section: Add a new category to CONFIG_METADATA and a corresponding entry to ADVANCED_SECTION_META.
 *
 * Edge cases:
 * - Orphaned path in SETTINGS_TAB_SECTIONS (setting removed from CONFIG_METADATA): Silently filtered out during derivation.
 * - Category not in ADVANCED_SECTION_META: Settings in that category are excluded from the Advanced tab.
 */

/**
 * Metadata for a UI tab in the configuration interface.
 */
export interface UITab {

  // Brief description shown at the top of the tab.
  description: string;

  // Human-readable tab name.
  displayName: string;

  // Tab identifier used in URLs and DOM.
  id: string;

  // Settings to display in this tab.
  settings: SettingMetadata[];
}

/**
 * Metadata for a collapsible section within the Advanced tab.
 */
export interface AdvancedSection {

  // Human-readable section name.
  displayName: string;

  // Section identifier.
  id: string;

  // Settings to display in this section.
  settings: SettingMetadata[];
}

/**
 * Metadata for a non-collapsible section within the Settings tab. Uses the same structure as AdvancedSection for consistency.
 */
export type SettingsSection = AdvancedSection;

/* The settings "promoted" to the main Settings tab, organized into visual sections. These are the options most users might actually change. Everything else goes to
 * the Advanced tab, grouped by storage category. Sections are displayed in array order.
 */
const SETTINGS_TAB_SECTIONS: { displayName: string; id: string; paths: string[] }[] = [

  {

    displayName: "Server",
    id: "server",
    paths: [ "server.port", "server.host" ]
  },
  {

    displayName: "Browser",
    id: "browser",
    paths: [ "browser.executablePath", "browser.initTimeout" ]
  },
  {

    displayName: "Startup",
    id: "startup",
    paths: ["channels.precacheServices"]
  },
  {

    displayName: "Capture",
    id: "capture",
    paths: [ "streaming.captureMode", "streaming.captureCodecs", "streaming.qualityPreset", "streaming.videoBitsPerSecond", "streaming.audioBitsPerSecond",
      "streaming.frameRate" ]
  },
  {

    displayName: "HDHomeRun / Plex",
    id: "hdhr",
    paths: [ "hdhr.enabled", "hdhr.discoveryEnabled", "hdhr.port", "hdhr.friendlyName" ]
  }
];

/* Display metadata for Advanced tab sections. The category field must match a key in CONFIG_METADATA. Entries are sorted alphabetically by category.
 */
const ADVANCED_SECTION_META: { category: string; displayName: string }[] = [

  { category: "channelsDvr", displayName: "Channels DVR" },
  { category: "hls", displayName: "HLS" },
  { category: "logging", displayName: "Logging" },
  { category: "paths", displayName: "Paths" },
  { category: "playback", displayName: "Playback" },
  { category: "recovery", displayName: "Recovery" },
  { category: "streaming", displayName: "Streaming" }
];

/**
 * Returns all setting paths from CONFIG_METADATA.
 * @returns Array of all setting paths.
 */
function getAllSettingPaths(): string[] {

  return Object.values(CONFIG_METADATA).flat().map((s) => s.path);
}

/**
 * Looks up a setting by its path.
 * @param settingPath - The dot-separated path (e.g., "streaming.videoBitsPerSecond").
 * @returns The setting metadata, or undefined if not found.
 */
export function getSettingByPath(settingPath: string): SettingMetadata | undefined {

  for(const settings of Object.values(CONFIG_METADATA)) {

    const found = settings.find((s) => s.path === settingPath);

    if(found) {

      return found;
    }
  }

  return undefined;
}

/**
 * Returns the sections for the Settings tab with resolved setting metadata.
 * @returns Array of section definitions.
 */
export function getSettingsTabSections(): SettingsSection[] {

  return SETTINGS_TAB_SECTIONS.map((section) => ({

    displayName: section.displayName,
    id: section.id,
    settings: section.paths.map((p) => getSettingByPath(p)).filter((s): s is SettingMetadata => s !== undefined)
  }));
}

/**
 * Returns the UI tabs for the configuration interface. The Settings tab contains commonly-used options; the Advanced tab contains everything else.
 * @returns Array of UI tab definitions.
 */
export function getUITabs(): UITab[] {

  // Derive settings tab paths from sections.
  const settingsTabPaths = SETTINGS_TAB_SECTIONS.flatMap((s) => s.paths);

  // Build Settings tab from sections.
  const settingsTabSettings = settingsTabPaths.map((p) => getSettingByPath(p)).filter((s): s is SettingMetadata => s !== undefined);

  // Build Advanced tab from everything not in Settings.
  const advancedPaths = getAllSettingPaths().filter((p) => !settingsTabPaths.includes(p));
  const advancedSettings = advancedPaths.map((p) => getSettingByPath(p)).filter((s): s is SettingMetadata => s !== undefined);

  return [
    {

      description: "Configure common server and streaming options.",
      displayName: "Settings",
      id: "settings",
      settings: settingsTabSettings
    },
    {

      description: "Expert tuning options. The defaults work well for most setups.",
      displayName: "Advanced",
      id: "advanced",
      settings: advancedSettings
    }
  ];
}

/**
 * Returns the collapsible sections for the Advanced tab. Each section groups settings by their storage category.
 * @returns Array of section definitions.
 */
export function getAdvancedSections(): AdvancedSection[] {

  // Derive settings tab paths from sections.
  const settingsTabPaths = SETTINGS_TAB_SECTIONS.flatMap((s) => s.paths);

  // Get all paths that belong in Advanced (not in Settings).
  const advancedPaths = getAllSettingPaths().filter((p) => !settingsTabPaths.includes(p));

  // Pair every advanced path with its resolved setting and category prefix, then drop entries whose category is empty or whose setting lookup failed. The
  // surviving pairs feed Map.groupBy below for the actual grouping.
  const settingsByPath = advancedPaths
    .map((path) => ({ category: path.split(".")[0] ?? "", setting: getSettingByPath(path) }))
    .filter((entry): entry is { category: string; setting: SettingMetadata } => Boolean(entry.category && entry.setting));

  // Group by category (first path segment) using the standard ESNext.Collection grouper.
  const byCategory = Map.groupBy(settingsByPath, (entry) => entry.category);

  // Return sections in the defined order with display names.
  return ADVANCED_SECTION_META
    .filter((meta) => byCategory.has(meta.category))
    .map((meta) => ({

      displayName: meta.displayName,
      id: meta.category,
      settings: byCategory.get(meta.category)?.map((entry) => entry.setting) ?? []
    }));
}

/* When saving user configuration, we only want to persist values that differ from defaults. This keeps the config file clean and makes it easy to see what the user has
 * actually customized. It also ensures that when defaults change in a new version, users automatically get the new defaults for settings they haven't explicitly set.
 */

/**
 * Recursively removes empty objects from a nested object structure. An object is considered empty if it has no own enumerable properties, or if all its properties are
 * themselves empty objects.
 * @param obj - The object to clean.
 * @returns A new object with empty nested objects removed.
 */
function removeEmptyObjects(obj: Record<string, unknown>): Record<string, unknown> {

  const result: Record<string, unknown> = {};

  for(const key of Object.keys(obj)) {

    const value = obj[key];

    // Recursively clean nested objects.
    if((value !== null) && (typeof value === "object") && !Array.isArray(value)) {

      const cleaned = removeEmptyObjects(value as Record<string, unknown>);

      // Only include if the cleaned object is not empty.
      if(Object.keys(cleaned).length > 0) {

        result[key] = cleaned;
      }
    } else {

      // Include non-object values as-is.
      result[key] = value;
    }
  }

  return result;
}

/**
 * Answers whether a value has its default's shape: an array for an array default, and a value of the default's own type for any other. It is the one statement
 * of the shape rule in this module, and a caller that applies it treats a value without its default's shape as absent.
 * @param value - The value to check.
 * @param defaultValue - The default the value stands in for.
 * @returns True when the value has the default's shape.
 */
function hasDefaultShape(value: unknown, defaultValue: unknown): boolean {

  return Array.isArray(defaultValue) ? Array.isArray(value) : (typeof value === typeof defaultValue);
}

/**
 * Checks whether a value equals its default for the purpose of default comparison. An array default is decided first: a value without the default's shape,
 * null and undefined among them, counts as absent and so equals the default, and an array equals it when it holds the default's members in any order. Order
 * does not count because a list setting is a set of choices, and every array this rule reaches is read as a set. Any other value compares through String()
 * coercion, with null and undefined equal to each other and to nothing else.
 * @param value - The value to check.
 * @param defaultValue - The default value to compare against.
 * @returns True if the values are considered equal.
 */
export function isEqualToDefault(value: unknown, defaultValue: unknown): boolean {

  if(Array.isArray(defaultValue)) {

    return !hasDefaultShape(value, defaultValue) || isDeepStrictEqual((value as unknown[]).toSorted(), defaultValue.toSorted());
  }

  // Handle null/undefined cases.
  if((value === null) || (value === undefined)) {

    return (defaultValue === null) || (defaultValue === undefined);
  }

  if((defaultValue === null) || (defaultValue === undefined)) {

    return false;
  }

  const primitive: string | number | boolean = value as string | number | boolean;
  const defaultPrimitive: string | number | boolean = defaultValue as string | number | boolean;

  return String(primitive) === String(defaultPrimitive);
}

// The fields the process writes.

/**
 * The declaration of one configuration field the process writes. The process writes every field in PROCESS_FIELDS, and the settings form writes none of them.
 * The kind tag is the field's whole declaration, and each kind carries one rule:
 *
 * - A state field is process-owned state the running configuration holds. It is kept on disk when its value has its default's shape and differs from the
 *   default, is brought back at boot when its value has its default's shape and is not the empty string, and is applied as its reactivity class says. Its
 *   default, which DEFAULTS already owns, decides its save rule and its restore rule, so the entry states nothing but its class. An empty string at a state
 *   field means no value, so the boot leaves the default standing.
 * - A schema field belongs to the file-store framework's migration runner. It exists only in the file, never in the running configuration, and it is kept
 *   when its predicate holds.
 *
 * One entry carries one kind, and the table refuses a second declaration of a field at compile time, because an object literal cannot repeat a key.
 */
export type ProcessField =
  { readonly kind: "schema"; readonly preserve: (value: unknown) => boolean } |
  { readonly kind: "state"; readonly reactivity: ProcessFieldReactivity };

/**
 * Every configuration field the process writes, keyed by dot path in ASCII order. The drift tests in userConfig.test.ts hold the state keys equal to exactly
 * the leaves DEFAULTS defines outside CONFIG_METADATA, so a leaf added to the configuration without an entry here, or an entry left behind for a leaf that
 * moved into the metadata, fails at test time rather than throwing inside a save. A state field's class follows the same rule a setting's declared class does,
 * read off the field's own readers. Suite 17 in test/e2e/routes/settings-preservation.test.ts iterates the table directly, so a new entry is covered by its
 * preservation sweep once the entry's seed value joins that file's SEED_VALUES table.
 */
export const PROCESS_FIELDS: Readonly<Record<string, ProcessField>> = {

  "channels.channelSortDirection": { kind: "state", reactivity: "live" },
  "channels.channelSortField": { kind: "state", reactivity: "live" },
  "channels.disabledPredefined": { kind: "state", reactivity: "live" },
  "channels.enabledServices": { kind: "state", reactivity: "live" },
  "channels.setupCompleted": { kind: "state", reactivity: "live" },
  "channels.visibleColumns": { kind: "state", reactivity: "live" },
  "channelsDvr.host": { kind: "state", reactivity: "live" },
  "hdhr.deviceId": { kind: "state", reactivity: "live" },
  "logging.debugFilter": { kind: "state", reactivity: "live" },

  // The audit trail of the migrations that have run, kept once it names one.
  "migrationsApplied": { kind: "schema", preserve: (value: unknown): boolean => Array.isArray(value) && (value.length > 0) },

  // The version the migration runner reads to decide which migrations still run, kept whenever it is a number.
  "schemaVersion": { kind: "schema", preserve: (value: unknown): boolean => typeof value === "number" }
};

/**
 * Answers the table's entry for a path, through Object.hasOwn, so an inherited key such as toString is never read as a field.
 * @param fieldPath - The dot-separated configuration path.
 * @returns The entry, or undefined when the table declares no field at the path.
 */
function getProcessField(fieldPath: string): ProcessField | undefined {

  return Object.hasOwn(PROCESS_FIELDS, fieldPath) ? PROCESS_FIELDS[fieldPath] : undefined;
}

/**
 * Resolves the reactivity class of any configuration leaf: the class its settings metadata declares, or the class its PROCESS_FIELDS state entry states. A
 * schema field exists only in the file, never in the running configuration, so it carries no class, and an unclassified path is a coding error rather than an
 * operator's, so each throws rather than guessing a class.
 * @param settingPath - The dot-separated configuration path (e.g., "hdhr.port").
 * @returns The leaf's reactivity class.
 * @throws When the path is neither a setting nor a state field.
 */
export function getReactivityClass(settingPath: string): ReactivityClass {

  const setting = getSettingByPath(settingPath);

  if(setting) {

    return setting.reactivity;
  }

  const field = getProcessField(settingPath);

  if(field) {

    switch(field.kind) {

      case "schema": {

        // A schema field is no leaf of the running configuration, so it falls through to the refusal below.
        break;
      }

      case "state": {

        return field.reactivity;
      }

      default: {

        assertNever(field);
      }
    }
  }

  throw new Error("The configuration path " + settingPath + " carries no reactivity class.");
}

/**
 * Filters a user configuration object to remove values that match the defaults. This produces a minimal config file containing only the settings the user has actually
 * customized. Empty nested objects are also removed.
 * @param config - The user configuration to filter.
 * @returns A new configuration object containing only non-default values.
 */
export function filterDefaults(config: UserConfig): UserConfig {

  const filtered: Record<string, unknown> = {};

  // Iterate over all known settings and check if the value differs from the default. A list setting is decided here too, by its members in any order.
  for(const settings of Object.values(CONFIG_METADATA)) {

    for(const setting of settings) {

      const value = getNestedValue(config, setting.path);

      // Skip undefined values (setting not present in config).
      if(value === undefined) {

        continue;
      }

      const defaultValue = getNestedValue(DEFAULTS, setting.path);

      // Only include if the value differs from the default.
      if(!isEqualToDefault(value, defaultValue)) {

        setNestedValue(filtered, setting.path, value);
      }
    }
  }

  /* Keep the fields the process writes, whose paths lie outside the metadata, so the loop above never wrote them. Each is kept by the rule its PROCESS_FIELDS
   * kind states: a state field when its value has its default's shape and differs from the default, by the same comparison the loop above applies, and a
   * schema field when its predicate holds. The order of this loop and the metadata loop does not matter, because the table and the metadata never share a
   * path, which the drift tests hold.
   */
  for(const [ fieldPath, field ] of Object.entries(PROCESS_FIELDS)) {

    const value = getNestedValue(config, fieldPath);

    switch(field.kind) {

      case "schema": {

        if(field.preserve(value)) {

          setNestedValue(filtered, fieldPath, value);
        }

        break;
      }

      case "state": {

        const defaultValue = getNestedValue(DEFAULTS, fieldPath);

        if(hasDefaultShape(value, defaultValue) && !isEqualToDefault(value, defaultValue)) {

          setNestedValue(filtered, fieldPath, value);
        }

        break;
      }

      default: {

        assertNever(field);
      }
    }
  }

  // Remove any empty nested objects that resulted from filtering.
  return removeEmptyObjects(filtered);
}
