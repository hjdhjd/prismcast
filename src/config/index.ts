/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Configuration management for PrismCast.
 */
import type { Config, Nullable } from "../types/index.ts";
import { DEFAULTS, mergeConfiguration, readConfig } from "./userConfig.ts";
import { LOG, getCurrentPattern, getPackageVersion, initDebugFilter, isAnyDebugEnabled } from "../utils/index.ts";
import { formatPresetStatus, getEffectivePreset, getValidPresetIds } from "./presets.ts";
import { getChromeDataDir, getConfigFilePath } from "./paths.ts";
import { RECOGNIZED_CODECS } from "../types/index.ts";
import path from "node:path";

/* The CONFIG object centralizes all tunable parameters for the application. Configuration uses a layered approach with the following priority (highest to lowest):
 *
 * 1. CLI flags (--port, --chrome-data-dir, --log-file)
 * 2. Environment variables (SCREAMING_SNAKE_CASE naming)
 * 3. User config file (config.json in the data directory)
 * 4. Hard-coded defaults (defined in userConfig.ts)
 *
 * This design follows the standard convention where CLI flags override everything. Docker deployments can use environment variables, standalone installations can
 * use the web UI at /config, and operators can always override any setting with a CLI flag.
 *
 * The settings are organized by functional area:
 *
 * - server: Network binding for the HTTP server (port, host)
 * - browser: Chrome launch settings (executable path, init timeout)
 * - streaming: Media capture quality (preset, bitrates, frame rate) and timeout limits
 * - playback: Health monitoring intervals and recovery timing thresholds
 * - recovery: Retry backoff parameters and circuit breaker configuration
 * - paths: Filesystem locations for Chrome profile and extension data
 *
 * Configuration is initialized at startup via initializeConfiguration(), which loads the user config file, merges with defaults, applies environment overrides and
 * CLI overrides, and validates all values. If validation fails, the process exits with a descriptive error message.
 */

/**
 * CLI override map. Keys are dot-separated CONFIG_METADATA paths (e.g., "server.port", "paths.chromeDataDir"). Values are the parsed CLI flag values. Applied as the
 * highest-priority merge pass in mergeConfiguration().
 */
export type CliOverrides = Record<string, unknown>;

// The CONFIG object is initialized during startup. It starts as a copy of DEFAULTS and is replaced by the merged configuration.
export let CONFIG: Config = structuredClone(DEFAULTS);

/**
 * Indicates whether a user config file parse error occurred during initialization. The web UI displays a warning when this is true.
 */
export let configParseError = false;

/**
 * The parse error message if configParseError is true.
 */
export let configParseErrorMessage: string | undefined;

/**
 * Initializes the configuration by loading the user config file, merging with defaults, applying environment variable overrides, and applying CLI overrides. This
 * must be called at startup before any code accesses CONFIG. After initialization, the CONFIG object contains the final merged values.
 * @param cliOverrides - Optional CLI flag overrides, applied at the highest priority level.
 */
export async function initializeConfiguration(cliOverrides?: CliOverrides): Promise<void> {

  // Load user configuration from file. Schema migrations (legacy provider field renames, foxcom -> foxone in enabledServices) run automatically inside the
  // file store framework via the declarative configMigrations registry; ensureMigrated (called by the release boot coordinator at startup) persists any
  // upgrades to disk before this function runs. The data returned here is always at CURRENT_CONFIG_SCHEMA_VERSION.
  const result = await readConfig();

  configParseError = result.parseError;
  configParseErrorMessage = result.parseErrorMessage;

  if(result.recoveredFromBackup) {

    LOG.info("Configuration was recovered from backup after a corrupt main file.");
  }

  // Merge defaults, user config, environment variables, and CLI overrides.
  CONFIG = mergeConfiguration(result.config, cliOverrides);

  // Apply persisted debug filter from config.json if no higher-priority source is active. The PRISMCAST_DEBUG env var and --debug CLI flag both call
  // initDebugFilter() before startServer() calls initializeConfiguration(), so isAnyDebugEnabled() is already true when either is set.
  if(!isAnyDebugEnabled() && (CONFIG.logging.debugFilter.length > 0)) {

    initDebugFilter(CONFIG.logging.debugFilter);

    // Normalize the in-memory value to the canonical form. The stored value may have extra whitespace around commas (e.g., "tuning:hulu, recovery") which
    // initDebugFilter strips during parsing. Keeping CONFIG in sync with getCurrentPattern() ensures comparisons elsewhere are reliable.
    CONFIG.logging.debugFilter = getCurrentPattern();
  }

  // Validate quality preset. Viewport is derived on-demand via getViewport() rather than stored in CONFIG.
  const validPresets = getValidPresetIds();

  if(!validPresets.includes(CONFIG.streaming.qualityPreset)) {

    LOG.warn("Invalid quality preset '%s'. Using default '%s'.", CONFIG.streaming.qualityPreset, DEFAULTS.streaming.qualityPreset);

    CONFIG.streaming.qualityPreset = DEFAULTS.streaming.qualityPreset;
  }

  LOG.info("Configuration initialized from defaults, user config, environment variables, and CLI overrides.");
}

/**
 * Returns a deep copy of the default configuration. Used by the web UI to display default values and handle reset operations.
 * @returns A copy of the default configuration.
 */
export function getDefaults(): Config {

  return structuredClone(DEFAULTS);
}

/* Before starting the server, we validate all configuration values to catch errors early. Invalid configurations like negative timeouts or out-of-range bitrates
 * would cause subtle runtime failures that are difficult to diagnose. By validating upfront, we provide clear error messages and prevent the server from starting
 * in a misconfigured state.
 *
 * Validation runs at startup after configuration initialization. If validation fails, the process exits with a non-zero code and a descriptive error message listing
 * all invalid values.
 */

/**
 * Validates that a configuration value is a positive integer within an optional range. This helper performs the common validation pattern of checking for valid
 * integers and enforcing minimum/maximum bounds. It returns an error message if validation fails, allowing the caller to collect all errors before reporting them.
 * @param name - The configuration name for error messages, typically the environment variable name.
 * @param value - The value to validate, typically parsed from an environment variable.
 * @param min - Optional minimum allowed value (inclusive).
 * @param max - Optional maximum allowed value (inclusive).
 * @returns Error message if invalid, null if valid.
 */
export function validatePositiveInt(name: string, value: number, min?: number, max?: number): Nullable<string> {

  // Check for NaN (from parseInt of invalid input) and non-positive values.
  if(!Number.isInteger(value) || (value < 1)) {

    return [ name, " must be a positive integer, got: ", String(value) ].join("");
  }

  // Check minimum bound if specified.
  if((min !== undefined) && (value < min)) {

    return [ name, " must be at least ", String(min), ", got: ", String(value) ].join("");
  }

  // Check maximum bound if specified.
  if((max !== undefined) && (value > max)) {

    return [ name, " must be at most ", String(max), ", got: ", String(value) ].join("");
  }

  return null;
}

/**
 * Validates that a configuration value is a positive number (including floats) within an optional range.
 * @param name - The configuration name for error messages.
 * @param value - The value to validate.
 * @param min - Optional minimum allowed value (inclusive).
 * @param max - Optional maximum allowed value (inclusive).
 * @returns Error message if invalid, null if valid.
 */
export function validatePositiveNumber(name: string, value: number, min?: number, max?: number): Nullable<string> {

  // Check for NaN and non-positive values.
  if(Number.isNaN(value) || (value <= 0)) {

    return [ name, " must be a positive number, got: ", String(value) ].join("");
  }

  // Check minimum bound if specified.
  if((min !== undefined) && (value < min)) {

    return [ name, " must be at least ", String(min), ", got: ", String(value) ].join("");
  }

  // Check maximum bound if specified.
  if((max !== undefined) && (value > max)) {

    return [ name, " must be at most ", String(max), ", got: ", String(value) ].join("");
  }

  return null;
}

/**
 * Validates all configuration values and throws an error if any are invalid. This function runs at startup after configuration initialization. We collect all
 * validation errors before throwing to provide complete feedback rather than failing on the first error and requiring multiple restart cycles to find all problems.
 * @throws If any configuration value is invalid. The error message lists all invalid values.
 */
export function validateConfiguration(): void {

  // Collect all validation errors before throwing so the operator sees every problem at once. The check helper pushes non-null results into the errors array,
  // reducing each validation to a single line.
  const errors: string[] = [];
  const check = (result: string | null): void => { if(result) { errors.push(result); } };

  // Server configuration.
  check(validatePositiveInt("PORT", CONFIG.server.port, 1, 65535));

  // Streaming bitrates. Video: 100kbps-50Mbps. Audio: 32-512kbps.
  check(validatePositiveInt("VIDEO_BITRATE", CONFIG.streaming.videoBitsPerSecond, 100000, 50000000));
  check(validatePositiveInt("AUDIO_BITRATE", CONFIG.streaming.audioBitsPerSecond, 32000, 512000));

  // Timeouts. 1 second minimum prevents premature failures. 10 minutes maximum prevents indefinite hangs.
  check(validatePositiveInt("NAV_TIMEOUT", CONFIG.streaming.navigationTimeout, 1000, 600000));
  check(validatePositiveInt("VIDEO_TIMEOUT", CONFIG.streaming.videoTimeout, 1000, 600000));

  // Concurrent stream limit.
  check(validatePositiveInt("MAX_CONCURRENT_STREAMS", CONFIG.streaming.maxConcurrentStreams, 1, 100));

  // Circuit breaker threshold.
  check(validatePositiveInt("CIRCUIT_BREAKER_THRESHOLD", CONFIG.recovery.circuitBreakerThreshold, 1, 100));

  // Stall threshold (float).
  check(validatePositiveNumber("STALL_THRESHOLD", CONFIG.playback.stallThreshold, 0.01, 5));

  // Logging. 10KB minimum ensures meaningful content. 100MB maximum prevents excessive disk usage.
  check(validatePositiveInt("LOG_MAX_SIZE", CONFIG.logging.maxSize, 10240, 104857600));

  // HLS configuration.
  check(validatePositiveInt("HLS_SEGMENT_DURATION", CONFIG.hls.segmentDuration, 1, 10));
  check(validatePositiveInt("HLS_MAX_SEGMENTS", CONFIG.hls.maxSegments, 3, 60));
  check(validatePositiveInt("HLS_IDLE_TIMEOUT", CONFIG.hls.idleTimeout, 10000, 300000));

  // Validate and normalize captureCodecs. Ensure h264 is always present (universal baseline) and filter out unrecognized codec identifiers. The recognized set
  // is derived from RECOGNIZED_CODECS in types/streaming.ts - the single definition for all capture codec identifiers.
  const recognizedCodecSet = new Set<string>(RECOGNIZED_CODECS);

  CONFIG.streaming.captureCodecs = CONFIG.streaming.captureCodecs.filter((c) => recognizedCodecSet.has(c));

  if(!CONFIG.streaming.captureCodecs.includes("h264")) {

    CONFIG.streaming.captureCodecs.unshift("h264");
  }

  // Force FFmpeg capture mode. Chrome's native fMP4 MediaRecorder produces corrupt output after 20-30 minutes of continuous recording. Until a future Chrome
  // release resolves this, native capture mode is disabled entirely.
  if(CONFIG.streaming.captureMode !== "ffmpeg") {

    LOG.warn("Native capture mode is disabled due to a Chrome fMP4 MediaRecorder bug. Forcing FFmpeg capture mode.");

    CONFIG.streaming.captureMode = "ffmpeg";
  }

  // Validate path overrides. When set, both chromeDataDir and logFile must be absolute paths to prevent ambiguity.
  if((CONFIG.paths.chromeDataDir !== null) && !path.isAbsolute(CONFIG.paths.chromeDataDir)) {

    errors.push("paths.chromeDataDir must be an absolute path, got: " + CONFIG.paths.chromeDataDir);
  }

  if((CONFIG.paths.logFile !== null) && !path.isAbsolute(CONFIG.paths.logFile)) {

    errors.push("paths.logFile must be an absolute path, got: " + CONFIG.paths.logFile);
  }

  // Validate HDHomeRun configuration when enabled.
  if(CONFIG.hdhr.enabled) {

    // HDHR requires FFmpeg for MPEG-TS remuxing. In native mode, FFmpeg is not guaranteed to be available. Disable HDHR and warn the operator. The string cast
    // suppresses TS2367 because captureMode is currently forced to "ffmpeg" above - this guard will become reachable again when native mode is re-enabled.
    if((CONFIG.streaming.captureMode as string) === "native") {

      CONFIG.hdhr.enabled = false;

      LOG.warn("HDHomeRun emulation requires FFmpeg mode. Disabling HDHR because capture mode is set to native.");
    } else {

      check(validatePositiveInt("HDHR_PORT", CONFIG.hdhr.port, 1, 65535));

      // Warn if HDHR port conflicts with the main server port (same host).
      if((CONFIG.hdhr.port === CONFIG.server.port) && ((CONFIG.server.host === "0.0.0.0") || (CONFIG.server.host === "::"))) {

        errors.push("HDHR_PORT (" + String(CONFIG.hdhr.port) + ") conflicts with the main server port.");
      }
    }
  }

  // If any validation errors occurred, throw with complete list for operator to fix all issues at once.
  if(errors.length > 0) {

    throw new Error([ "Configuration validation failed:\n  ", errors.join("\n  ") ].join(""));
  }
}

/**
 * Displays the active configuration at startup. This helps operators verify their settings and diagnose connection issues. We log only the most commonly adjusted
 * values to keep output concise while providing useful debugging information.
 *
 * This function also checks for preset degradation and logs a warning if the configured preset exceeds display capabilities. The warning helps users understand why
 * their stream resolution may be lower than configured.
 */
export function displayConfiguration(): void {

  const presetResult = getEffectivePreset(CONFIG);
  const presetStatus = formatPresetStatus(presetResult);

  LOG.info("Starting PrismCast v%s with configuration:", getPackageVersion());
  LOG.info("  Configuration file: %s", getConfigFilePath());
  LOG.info("  Chrome profile: %s", getChromeDataDir(CONFIG));
  LOG.info("  Server port: %s", CONFIG.server.port);
  LOG.info("  Quality preset: %s", presetStatus);
  LOG.info("  Capture codecs: %s", CONFIG.streaming.captureCodecs.join(", "));
  LOG.info("  Video bitrate: %s", CONFIG.streaming.videoBitsPerSecond);
  LOG.info("  Max retries: %s", CONFIG.streaming.maxNavigationRetries);
  LOG.info("  Max concurrent streams: %s", CONFIG.streaming.maxConcurrentStreams);
  LOG.info("  Circuit breaker threshold: %s failures in %s minutes",
    CONFIG.recovery.circuitBreakerThreshold, Math.round(CONFIG.recovery.circuitBreakerWindow / 60000));
  LOG.info("  Chrome executable: %s", CONFIG.browser.executablePath ?? "autodetect");
  LOG.info("  HLS segment duration: %ss, max segments: %s", CONFIG.hls.segmentDuration, CONFIG.hls.maxSegments);
  LOG.info("  HDHomeRun emulation: %s", CONFIG.hdhr.enabled ? "enabled (port " + String(CONFIG.hdhr.port) + ")" : "disabled");

  // Log a prominent warning if preset was degraded due to display limitations.
  if(presetResult.degraded && presetResult.maxViewport) {

    LOG.warn("Display supports maximum %s\u00d7%s. Configured %s preset will use %s instead.",
      presetResult.maxViewport.width, presetResult.maxViewport.height,
      presetResult.configuredPreset.id, presetResult.effectivePreset.id);
  }

}
