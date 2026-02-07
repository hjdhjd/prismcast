/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userConfig.ts: User configuration file management for PrismCast.
 */
import type { Config, Nullable } from "../types/index.js";
import { LOG } from "../utils/index.js";
import fs from "node:fs";
import { getValidPresetIds } from "./presets.js";
import os from "node:os";
import path from "node:path";

const { promises: fsPromises } = fs;

/* PrismCast stores user configuration in ~/.prismcast/config.json. This file allows users to customize settings without using environment variables. The
 * configuration system uses a layered approach:
 *
 * 1. Hard-coded defaults (defined in DEFAULTS)
 * 2. User config file (~/.prismcast/config.json)
 * 3. Environment variables (highest priority)
 *
 * This design allows Docker deployments to use environment variables to override user settings, while standalone installations can use the config file for
 * convenience. The web UI at /config provides a user-friendly interface for editing the config file.
 */

/* Each configurable setting has metadata describing its type, valid range, environment variable name, and human-readable description. This metadata is used by the
 * /config web UI to render appropriate form fields and validation, and by the validation system to check values before saving.
 */

/**
 * Metadata describing a single configuration setting. Note: Default values are not stored here to avoid duplication. Use getNestedValue(DEFAULTS, setting.path) to get
 * the default value for a setting.
 */
export interface SettingMetadata {

  // Human-readable description shown in the UI.
  description: string;

  // Path to a boolean setting that must be enabled for this setting to be active. When the referenced setting is false, this field is visually greyed out in the
  // UI. The field values are still submitted during save to avoid losing custom values when the parent toggle is temporarily disabled.
  dependsOn?: string;

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
  envVar: string | null;

  // Human-readable label for form fields.
  label: string;

  // Maximum allowed value for numeric settings.
  max?: number;

  // Minimum allowed value for numeric settings.
  min?: number;

  // Dot-separated path to the setting (e.g., "browser.initTimeout").
  path: string;

  // Data type for validation and form field rendering.
  type: "boolean" | "float" | "host" | "integer" | "path" | "port" | "string";

  // Valid values for string type settings.
  validValues?: string[];

  // Unit of measurement displayed in the UI (e.g., "ms", "bps").
  unit?: string;
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
      type: "integer",
      unit: "ms"
    }
  ],

  hdhr: [
    {

      description: "Enable HDHomeRun emulation for Plex integration. When enabled, PrismCast runs a second HTTP server that emulates an HDHomeRun tuner, " +
        "allowing Plex to use PrismCast as a live TV source. In Plex, go to Settings > Live TV & DVR > Set Up Plex DVR and enter this server's address " +
        "manually as IP:port (e.g., 192.168.1.100:5004).",
      envVar: "HDHR_ENABLED",
      label: "Enable HDHomeRun Emulation",
      path: "hdhr.enabled",
      type: "boolean"
    },
    {

      dependsOn: "hdhr.enabled",
      description: "TCP port for the HDHomeRun emulation server. This is the port you enter in Plex when manually adding the tuner (e.g., 192.168.1.100:5004).",
      envVar: "HDHR_PORT",
      label: "HDHomeRun Port",
      max: 65535,
      min: 1,
      path: "hdhr.port",
      type: "port"
    },
    {

      dependsOn: "hdhr.enabled",
      description: "Display name shown in Plex for this tuner. Helps identify PrismCast when you have multiple HDHomeRun devices.",
      envVar: "HDHR_FRIENDLY_NAME",
      label: "Friendly Name",
      path: "hdhr.friendlyName",
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
      type: "integer"
    },
    {

      description: "Time before an idle HLS stream is terminated. Applies when no segment requests are received.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "HLS_IDLE_TIMEOUT",
      label: "Idle Timeout",
      max: 300000,
      min: 10000,
      path: "hls.idleTimeout",
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
      min: 10240,
      path: "logging.maxSize",
      type: "integer",
      unit: "bytes"
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
      type: "integer",
      unit: "ms"
    },
    {

      description: "Delay after clicking video element to initiate playback on Brightcove-based players. Currently unused — waitForVideoReady() " +
        "handles the wait automatically.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "CLICK_TO_PLAY_DELAY",
      label: "Click to Play Delay",
      max: 10000,
      min: 100,
      path: "playback.clickToPlayDelay",
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
      type: "integer",
      unit: "ms"
    },
    {

      description: "Time window for tracking page reload frequency. After this period, the reload counter resets.",
      displayDivisor: 60000,
      displayUnit: "minutes",
      envVar: "PAGE_RELOAD_WINDOW",
      label: "Page Reload Window",
      max: 3600000,
      min: 60000,
      path: "playback.pageReloadWindow",
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
      type: "integer"
    },
    {

      description: "Minimum change in video.currentTime (seconds) to consider playback progressing.",
      envVar: "STALL_THRESHOLD",
      label: "Stall Threshold",
      max: 5,
      min: 0.01,
      path: "playback.stallThreshold",
      type: "float",
      unit: "seconds"
    },
    {

      description: "Duration of healthy playback required before resetting escalation level. Prevents stutter loops.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "SUSTAINED_PLAYBACK_REQUIRED",
      label: "Sustained Playback Required",
      max: 300000,
      min: 10000,
      path: "playback.sustainedPlaybackRequired",
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
      type: "integer"
    },
    {

      description: "Time window for counting failures toward circuit breaker.",
      displayDivisor: 60000,
      displayUnit: "minutes",
      envVar: "CIRCUIT_BREAKER_WINDOW",
      label: "Circuit Breaker Window",
      max: 3600000,
      min: 60000,
      path: "recovery.circuitBreakerWindow",
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
      type: "integer",
      unit: "ms"
    },
    {

      description: "Interval between stale page cleanup runs. Identifies and closes orphaned browser pages.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "STALE_PAGE_CLEANUP_INTERVAL",
      label: "Stale Page Cleanup Interval",
      max: 600000,
      min: 10000,
      path: "recovery.stalePageCleanupInterval",
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
      type: "host"
    },
    {

      description: "TCP port for the HTTP server. Channels DVR and other clients connect here.",
      envVar: "PORT",
      label: "Port",
      max: 65535,
      min: 1,
      path: "server.port",
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
      type: "string",
      validValues: [ "ffmpeg", "native" ]
    },
    {

      description: "Video quality preset. Determines capture resolution. Bitrate and frame rate can be further customized.",
      envVar: "QUALITY_PRESET",
      label: "Quality Preset",
      path: "streaming.qualityPreset",
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
      type: "integer",
      unit: "bps"
    },
    {

      description: "Target frame rate. 60fps is ideal for sports; 30fps works for most TV content.",
      envVar: "FRAME_RATE",
      label: "Frame Rate",
      max: 120,
      min: 15,
      path: "streaming.frameRate",
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
      type: "integer"
    },
    {

      description: "Maximum navigation retry attempts before giving up.",
      envVar: "MAX_NAV_RETRIES",
      label: "Max Navigation Retries",
      max: 50,
      min: 1,
      path: "streaming.maxNavigationRetries",
      type: "integer"
    },
    {

      description: "Timeout for page navigation. Increase for slow networks or heavy pages.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "NAV_TIMEOUT",
      label: "Navigation Timeout",
      max: 600000,
      min: 1000,
      path: "streaming.navigationTimeout",
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
      type: "integer",
      unit: "bps"
    },
    {

      description: "Timeout for video element to become ready after navigation.",
      displayDivisor: 1000,
      displayUnit: "seconds",
      envVar: "VIDEO_TIMEOUT",
      label: "Video Timeout",
      max: 600000,
      min: 1000,
      path: "streaming.videoTimeout",
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

  maxSize?: number;
}

/**
 * Partial playback configuration for user config file.
 */
export interface UserPlaybackConfig {

  bufferingGracePeriod?: number;
  channelSelectorDelay?: number;
  channelSwitchDelay?: number;
  clickToPlayDelay?: number;
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

  // List of predefined channel keys that are disabled.
  disabledPredefined?: string[];
}

/**
 * Partial HDHomeRun configuration for user config file.
 */
export interface UserHdhrConfig {

  deviceId?: string;
  enabled?: boolean;
  friendlyName?: string;
  port?: number;
}

/**
 * User configuration with all fields optional. This is the structure of the config.json file.
 */
export interface UserConfig {

  browser?: UserBrowserConfig;
  channels?: UserChannelsConfig;
  hdhr?: UserHdhrConfig;
  hls?: UserHLSConfig;
  logging?: UserLoggingConfig;
  playback?: UserPlaybackConfig;
  recovery?: UserRecoveryConfig;
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
}

/* The config file is stored in the same data directory as the Chrome profile (~/.prismcast).
 */

const dataDir = path.join(os.homedir(), ".prismcast");
const configFilePath = path.join(dataDir, "config.json");

/**
 * Returns the path to the user configuration file.
 * @returns The absolute path to ~/.prismcast/config.json.
 */
export function getConfigFilePath(): string {

  return configFilePath;
}

/* These functions handle reading and writing the config file. All operations are async and handle errors gracefully.
 */

/**
 * Loads user configuration from the config file. Returns an empty config if the file doesn't exist, and sets parseError if the file exists but contains invalid
 * JSON.
 * @returns The loaded configuration with parse status.
 */
export async function loadUserConfig(): Promise<UserConfigLoadResult> {

  try {

    const content = await fsPromises.readFile(configFilePath, "utf-8");

    try {

      const config = JSON.parse(content) as UserConfig;

      return { config, parseError: false };
    } catch(parseError) {

      const message = (parseError instanceof Error) ? parseError.message : String(parseError);

      LOG.warn("Invalid JSON in configuration file %s: %s. Using defaults.", configFilePath, message);

      return { config: {}, parseError: true, parseErrorMessage: message };
    }
  } catch(error) {

    // File doesn't exist - this is normal, use defaults.
    if((error as NodeJS.ErrnoException).code === "ENOENT") {

      return { config: {}, parseError: false };
    }

    // Other read errors - log and use defaults.
    LOG.warn("Failed to read configuration file %s: %s. Using defaults.", configFilePath, (error instanceof Error) ? error.message : String(error));

    return { config: {}, parseError: false };
  }
}

/**
 * Saves user configuration to the config file. Creates the data directory if it doesn't exist.
 * @param config - The configuration to save.
 * @throws If the file cannot be written.
 */
export async function saveUserConfig(config: UserConfig): Promise<void> {

  // Ensure data directory exists.
  await fsPromises.mkdir(dataDir, { recursive: true });

  // Write config with pretty formatting for readability.
  const content = JSON.stringify(config, null, 2);

  await fsPromises.writeFile(configFilePath, content + "\n", "utf-8");

  LOG.info("Configuration saved to %s.", configFilePath);
}

/* These functions detect which settings are overridden by environment variables, so the UI can disable those fields and show appropriate warnings.
 */

/**
 * Returns a map of setting paths to their environment variable values for settings that are overridden by environment variables.
 * @returns Map of path -> env var value for overridden settings.
 */
export function getEnvOverrides(): Map<string, string> {

  const overrides = new Map<string, string>();

  for(const settings of Object.values(CONFIG_METADATA)) {

    for(const setting of settings) {

      const envValue = setting.envVar ? process.env[setting.envVar] : undefined;

      if(envValue !== undefined) {

        overrides.set(setting.path, envValue);
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
    initTimeout: 1000
  },

  channels: {

    disabledPredefined: []
  },

  hdhr: {

    deviceId: "",
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

    httpLogLevel: "errors",
    maxSize: 1048576
  },

  paths: {


    chromeProfileName: "chromedata",
    extensionDirName: "extension"
  },

  playback: {


    bufferingGracePeriod: 10000,
    channelSelectorDelay: 5000,
    channelSwitchDelay: 4000,
    clickToPlayDelay: 1000,
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
    circuitBreakerWindow: 300000,
    maxBackoffDelay: 3000,
    stalePageCleanupInterval: 60000,
    stalePageGracePeriod: 30000
  },

  server: {


    host: "0.0.0.0",
    port: 5589
  },

  streaming: {

    audioBitsPerSecond: 256000,
    captureMode: "ffmpeg",
    frameRate: 60,
    maxConcurrentStreams: 10,
    maxNavigationRetries: 4,
    navigationTimeout: 10000,
    qualityPreset: "720p-high",
    videoBitsPerSecond: 12000000,
    videoTimeout: 10000
  }
};

/**
 * Parses an environment variable value according to the setting type.
 * @param value - The raw environment variable value.
 * @param type - The expected type of the setting.
 * @returns The parsed value, or undefined if parsing fails.
 */
function parseEnvValue(value: string, type: SettingMetadata["type"]): Nullable<boolean | number | string> | undefined {

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

    case "host":
    case "path": {

      return value;
    }

    default: {

      return value;
    }
  }
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

  for(let i = 0; i < (parts.length - 1); i++) {

    const part = parts[i];

    if(current[part] === undefined) {

      current[part] = {};
    }

    current = current[part] as Record<string, unknown>;
  }

  current[parts[parts.length - 1]] = value;
}

/**
 * Merges user configuration with defaults and environment overrides to produce the final configuration. Priority: env vars > user config > defaults.
 * @param userConfig - User configuration from the config file.
 * @returns The merged configuration.
 */
export function mergeConfiguration(userConfig: UserConfig): Config {

  // Start with a deep copy of defaults.
  const config = JSON.parse(JSON.stringify(DEFAULTS)) as Config;

  // Apply user config values.
  for(const settings of Object.values(CONFIG_METADATA)) {

    for(const setting of settings) {

      const userValue = getNestedValue(userConfig, setting.path);

      if(userValue !== undefined) {

        setNestedValue(config as unknown as Record<string, unknown>, setting.path, userValue);
      }
    }
  }

  /* These fields are stored in the user config file but are not part of CONFIG_METADATA because they are complex types (arrays, auto-generated strings) that don't
   * fit the standard scalar setting model. When adding a new field here, you must also add corresponding preservation logic in filterDefaults() below.
   */
  if(Array.isArray(userConfig.channels?.disabledPredefined)) {

    config.channels.disabledPredefined = [...userConfig.channels.disabledPredefined];
  }

  if((typeof userConfig.hdhr?.deviceId === "string") && (userConfig.hdhr.deviceId.length > 0)) {

    config.hdhr.deviceId = userConfig.hdhr.deviceId;
  }

  // Apply environment variable overrides (highest priority).
  for(const settings of Object.values(CONFIG_METADATA)) {

    for(const setting of settings) {

      const envValue = setting.envVar ? process.env[setting.envVar] : undefined;

      if(envValue !== undefined) {

        const parsedValue = parseEnvValue(envValue, setting.type);

        if(parsedValue !== undefined) {

          setNestedValue(config as unknown as Record<string, unknown>, setting.path, parsedValue);
        }
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

/*
 * The settings "promoted" to the main Settings tab, organized into visual sections. These are the options most users might actually change. Everything else goes to
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

    displayName: "Capture",
    id: "capture",
    paths: [ "streaming.captureMode", "streaming.qualityPreset", "streaming.videoBitsPerSecond", "streaming.audioBitsPerSecond", "streaming.frameRate" ]
  },
  {

    displayName: "HDHomeRun / Plex",
    id: "hdhr",
    paths: [ "hdhr.enabled", "hdhr.port", "hdhr.friendlyName" ]
  }
];

/*
 * Display metadata for Advanced tab sections. The category field must match a key in CONFIG_METADATA. Entries are sorted alphabetically by category.
 */
const ADVANCED_SECTION_META: { category: string; displayName: string }[] = [

  { category: "hls", displayName: "HLS" },
  { category: "logging", displayName: "Logging" },
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
    settings: section.paths
      .map((p) => getSettingByPath(p))
      .filter((s): s is SettingMetadata => s !== undefined)
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
  const settingsTabSettings = settingsTabPaths
    .map((p) => getSettingByPath(p))
    .filter((s): s is SettingMetadata => s !== undefined);

  // Build Advanced tab from everything not in Settings.
  const advancedPaths = getAllSettingPaths().filter((p) => !settingsTabPaths.includes(p));
  const advancedSettings = advancedPaths
    .map((p) => getSettingByPath(p))
    .filter((s): s is SettingMetadata => s !== undefined);

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

  // Group by category (first path segment).
  const byCategory = new Map<string, SettingMetadata[]>();

  for(const path of advancedPaths) {

    const category = path.split(".")[0];
    const setting = getSettingByPath(path);

    if(setting) {

      if(!byCategory.has(category)) {

        byCategory.set(category, []);
      }

      byCategory.get(category)?.push(setting);
    }
  }

  // Return sections in the defined order with display names.
  return ADVANCED_SECTION_META
    .filter((meta) => byCategory.has(meta.category))
    .map((meta) => ({

      displayName: meta.displayName,
      id: meta.category,
      settings: byCategory.get(meta.category) ?? []
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
 * Checks if two values are equal for the purpose of default comparison. Handles null, undefined, and type coercion consistently.
 * @param value - The value to check.
 * @param defaultValue - The default value to compare against.
 * @returns True if the values are considered equal.
 */
export function isEqualToDefault(value: unknown, defaultValue: unknown): boolean {

  // Handle null/undefined cases.
  if((value === null) || (value === undefined)) {

    return (defaultValue === null) || (defaultValue === undefined);
  }

  if((defaultValue === null) || (defaultValue === undefined)) {

    return false;
  }

  // Compare as strings for consistent comparison across types (handles number/string coercion).
  return String(value) === String(defaultValue);
}

/**
 * Filters a user configuration object to remove values that match the defaults. This produces a minimal config file containing only the settings the user has actually
 * customized. Empty nested objects are also removed.
 * @param config - The user configuration to filter.
 * @returns A new configuration object containing only non-default values.
 */
export function filterDefaults(config: UserConfig): UserConfig {

  const filtered: Record<string, unknown> = {};

  // Iterate over all known settings and check if the value differs from the default.
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

  /* Counterpart to the non-CONFIG_METADATA handling in mergeConfiguration() above. When adding a new complex field there, you must also add corresponding
   * preservation logic here, otherwise the field will be lost when saving configuration.
   */
  const configChannelsDisabled = getNestedValue(config, "channels.disabledPredefined") as string[] | undefined;

  if(Array.isArray(configChannelsDisabled) && (configChannelsDisabled.length > 0)) {

    setNestedValue(filtered, "channels.disabledPredefined", configChannelsDisabled);
  }

  const configDeviceId = getNestedValue(config, "hdhr.deviceId") as string | undefined;

  if((typeof configDeviceId === "string") && (configDeviceId.length > 0)) {

    setNestedValue(filtered, "hdhr.deviceId", configDeviceId);
  }

  // Remove any empty nested objects that resulted from filtering.
  return removeEmptyObjects(filtered) as UserConfig;
}
