/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Configuration management for PrismCast.
 */
import type { Config, Nullable } from "../types/index.js";
import { DEFAULTS, loadUserConfig, mergeConfiguration } from "./userConfig.js";
import { formatPresetStatus, getEffectivePreset, getValidPresetIds } from "./presets.js";
import { LOG } from "../utils/index.js";

/* The CONFIG object centralizes all tunable parameters for the application. Configuration uses a layered approach with the following priority (highest to lowest):
 *
 * 1. Environment variables (SCREAMING_SNAKE_CASE naming)
 * 2. User config file (~/.prismcast/config.json)
 * 3. Hard-coded defaults (defined in userConfig.ts)
 *
 * This design allows Docker deployments to use environment variables for overrides while standalone installations can use the web UI at /config for convenience.
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
 * Configuration is initialized at startup via initializeConfiguration(), which loads the user config file, merges with defaults, applies environment overrides, and
 * validates all values. If validation fails, the process exits with a descriptive error message.
 */

// The CONFIG object is initialized during startup. It starts as a copy of DEFAULTS and is replaced by the merged configuration.
export let CONFIG: Config = JSON.parse(JSON.stringify(DEFAULTS)) as Config;

/**
 * Indicates whether a user config file parse error occurred during initialization. The web UI displays a warning when this is true.
 */
export let configParseError = false;

/**
 * The parse error message if configParseError is true.
 */
export let configParseErrorMessage: string | undefined;

/**
 * Initializes the configuration by loading the user config file, merging with defaults, and applying environment variable overrides. This must be called at startup
 * before any code accesses CONFIG. After initialization, the CONFIG object contains the final merged values.
 */
export async function initializeConfiguration(): Promise<void> {

  // Load user configuration from file.
  const result = await loadUserConfig();

  configParseError = result.parseError;
  configParseErrorMessage = result.parseErrorMessage;

  // Merge defaults, user config, and environment variables.
  CONFIG = mergeConfiguration(result.config);

  // Validate quality preset. Viewport is derived on-demand via getViewport() rather than stored in CONFIG.
  const validPresets = getValidPresetIds();

  if(!validPresets.includes(CONFIG.streaming.qualityPreset)) {

    LOG.warn("Invalid quality preset '%s'. Using default '%s'.", CONFIG.streaming.qualityPreset, DEFAULTS.streaming.qualityPreset);

    CONFIG.streaming.qualityPreset = DEFAULTS.streaming.qualityPreset;
  }

  LOG.info("Configuration initialized from defaults, user config, and environment variables.");
}

/**
 * Returns a deep copy of the default configuration. Used by the web UI to display default values and handle reset operations.
 * @returns A copy of the default configuration.
 */
export function getDefaults(): Config {

  return JSON.parse(JSON.stringify(DEFAULTS)) as Config;
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

  const errors: string[] = [];

  // Validate server configuration. Port must be within the valid TCP port range (1-65535). Port 0 is reserved.
  const portError = validatePositiveInt("PORT", CONFIG.server.port, 1, 65535);

  if(portError) {

    errors.push(portError);
  }

  // Validate streaming bitrates. Minimum video bitrate (100kbps) ensures basic video quality. Maximum (50Mbps) prevents unreasonable resource consumption.
  // Audio range (32-512kbps) covers all common audio quality levels.
  const videoBitrateError = validatePositiveInt("VIDEO_BITRATE", CONFIG.streaming.videoBitsPerSecond, 100000, 50000000);

  if(videoBitrateError) {

    errors.push(videoBitrateError);
  }

  const audioBitrateError = validatePositiveInt("AUDIO_BITRATE", CONFIG.streaming.audioBitsPerSecond, 32000, 512000);

  if(audioBitrateError) {

    errors.push(audioBitrateError);
  }

  // Validate timeouts. Minimum (1 second) prevents premature failures. Maximum (10 minutes) prevents indefinite hangs while allowing for very slow networks.
  const navTimeoutError = validatePositiveInt("NAV_TIMEOUT", CONFIG.streaming.navigationTimeout, 1000, 600000);

  if(navTimeoutError) {

    errors.push(navTimeoutError);
  }

  const videoTimeoutError = validatePositiveInt("VIDEO_TIMEOUT", CONFIG.streaming.videoTimeout, 1000, 600000);

  if(videoTimeoutError) {

    errors.push(videoTimeoutError);
  }

  // Validate concurrent stream limit. At least 1 stream must be allowed. Maximum of 100 prevents resource exhaustion on most systems.
  const concurrentError = validatePositiveInt("MAX_CONCURRENT_STREAMS", CONFIG.streaming.maxConcurrentStreams, 1, 100);

  if(concurrentError) {

    errors.push(concurrentError);
  }

  // Validate circuit breaker. At least 1 failure required to trip. Maximum of 100 prevents the breaker from never tripping.
  const circuitBreakerError = validatePositiveInt("CIRCUIT_BREAKER_THRESHOLD", CONFIG.recovery.circuitBreakerThreshold, 1, 100);

  if(circuitBreakerError) {

    errors.push(circuitBreakerError);
  }

  // Validate stall threshold (float).
  const stallThresholdError = validatePositiveNumber("STALL_THRESHOLD", CONFIG.playback.stallThreshold, 0.01, 5);

  if(stallThresholdError) {

    errors.push(stallThresholdError);
  }

  // Validate logging configuration. Minimum size (10KB) ensures meaningful log content. Maximum (100MB) prevents excessive disk usage.
  const logMaxSizeError = validatePositiveInt("LOG_MAX_SIZE", CONFIG.logging.maxSize, 10240, 104857600);

  if(logMaxSizeError) {

    errors.push(logMaxSizeError);
  }

  // Validate HLS configuration. Segment duration and max segments have sensible ranges.
  const hlsSegmentDurationError = validatePositiveInt("HLS_SEGMENT_DURATION", CONFIG.hls.segmentDuration, 1, 10);

  if(hlsSegmentDurationError) {

    errors.push(hlsSegmentDurationError);
  }

  const hlsMaxSegmentsError = validatePositiveInt("HLS_MAX_SEGMENTS", CONFIG.hls.maxSegments, 3, 60);

  if(hlsMaxSegmentsError) {

    errors.push(hlsMaxSegmentsError);
  }

  const hlsIdleTimeoutError = validatePositiveInt("HLS_IDLE_TIMEOUT", CONFIG.hls.idleTimeout, 10000, 300000);

  if(hlsIdleTimeoutError) {

    errors.push(hlsIdleTimeoutError);
  }

  // Force FFmpeg capture mode. Chrome's native fMP4 MediaRecorder produces corrupt output after 20-30 minutes of continuous recording. Until a future Chrome
  // release resolves this, native capture mode is disabled entirely.
  if(CONFIG.streaming.captureMode !== "ffmpeg") {

    LOG.warn("Native capture mode is disabled due to a Chrome fMP4 MediaRecorder bug. Forcing FFmpeg capture mode.");

    CONFIG.streaming.captureMode = "ffmpeg";
  }

  // Validate HDHomeRun configuration when enabled.
  if(CONFIG.hdhr.enabled) {

    // HDHR requires FFmpeg for MPEG-TS remuxing. In native mode, FFmpeg is not guaranteed to be available. Disable HDHR and warn the operator. The string cast
    // suppresses TS2367 because captureMode is currently forced to "ffmpeg" above — this guard will become reachable again when native mode is re-enabled.
    if((CONFIG.streaming.captureMode as string) === "native") {

      CONFIG.hdhr.enabled = false;

      LOG.warn("HDHomeRun emulation requires FFmpeg mode. Disabling HDHR because capture mode is set to native.");
    } else {

      const hdhrPortError = validatePositiveInt("HDHR_PORT", CONFIG.hdhr.port, 1, 65535);

      if(hdhrPortError) {

        errors.push(hdhrPortError);
      }

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

  LOG.info("Starting PrismCast with configuration:");
  LOG.info("  Server port: %s", CONFIG.server.port);
  LOG.info("  Quality preset: %s", presetStatus);
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
