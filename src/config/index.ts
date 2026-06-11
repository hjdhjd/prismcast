/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Configuration management for PrismCast.
 */
import type { ApplyResult, ChangeOutcome, ConfigChange } from "./reactivity.ts";
import type { Config, Nullable } from "../types/index.ts";
import { DEFAULTS, mergeConfiguration, mutateConfig, readConfig } from "./userConfig.ts";
import { LOG, canonicalizeDebugPattern, displayLine, formatError, getCurrentPattern, getPackageVersion, initDebugFilter, isAnyDebugEnabled } from "../utils/index.ts";
import { applyConfigChanges, computeConfigDiff, registerConfigChangeHandler } from "./reactivity.ts";
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

// Stashed CLI overrides from the most recent initializeConfiguration call. reloadConfiguration re-applies them so the priority chain (CLI > env > user > defaults)
// stays consistent across reloads. CLI overrides are a startup-only concern in practice; capturing them once and replaying them avoids losing the binding when a
// live reload runs in a process where the operator originally passed --port or --data-dir.
let stashedCliOverrides: CliOverrides | undefined;

// Whether a higher-priority debug source (the PRISMCAST_DEBUG env var or the --debug CLI flag) established the active debug filter before the persisted config
// filter was first applied. Captured once in initializeConfiguration, ahead of the persisted filter, so a later reloadConfiguration can re-apply a changed
// persisted filter live without ever clobbering an env/CLI override - that override must win for the entire process lifetime.
let envOrCliDebugOverride = false;

// Whether validateConfiguration coerced a capture setting (forced FFmpeg mode, normalized captureCodecs) on the live CONFIG at startup. persistCoercedConfig
// reads this to decide whether to write the coerced values back to disk so the on-disk state matches the live binding. Without the write-back a config file
// holding an unsupported capture value (native mode) stays divergent from the coerced CONFIG forever, and the reload-validation path would then reject every
// later save on the resulting phantom capture diff.
let captureConfigCoercedAtStartup = false;

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
  stashedCliOverrides = cliOverrides;

  if(result.recoveredFromBackup) {

    LOG.info("Configuration was recovered from backup after a corrupt main file.");
  }

  // Capture whether a higher-priority debug source (PRISMCAST_DEBUG / --debug) already owns the active filter before we apply the persisted config filter, so a
  // later reload can re-apply a changed persisted filter live without overriding env/CLI. Measured here, ahead of normalizeConfig/commitDebugFilter, it reflects
  // env/CLI alone rather than the persisted filter applying to itself.
  envOrCliDebugOverride = isAnyDebugEnabled();

  // Merge defaults, user config, environment variables, and CLI overrides, then run the same post-merge normalization reload replays, and commit the persisted
  // debug filter to the runtime. The two steps are kept separate so reload can normalize a candidate config without applying its filter until it is committed.
  CONFIG = mergeConfiguration(result.config, cliOverrides);
  normalizeConfig(CONFIG);
  commitDebugFilter();

  LOG.info("Configuration initialized from defaults, user config, environment variables, and CLI overrides.");
}

/**
 * Re-reads the user config file, re-merges with the original CLI overrides, applies post-merge normalizations, commits the result to the in-memory CONFIG
 * binding, and dispatches the resulting diff to registered config-change handlers. This is the single entry point for "config changed - tell subsystems"; both
 * the /config save handler and /config/import handler call it after writing to disk. The returned ApplyResult partitions the diff into changes that were
 * applied live, deferred (requiring restart), or rejected (refused by a handler). Callers use the deferred count to decide whether to schedule a restart.
 *
 * Atomicity contract: failures during the read, merge, or normalize steps leave CONFIG completely untouched - the function builds the new shape in isolation
 * and only reassigns the live binding after all of those steps succeed. Failures DURING handler dispatch, however, occur after the CONFIG reassignment: CONFIG
 * already reflects the new on-disk state, but the handler chain may have run partway. The conservative answer is that handlers must not throw; they should
 * report outcomes (including "rejected") instead. The atomicity-on-read invariant is exercised by index.reload.test.ts.
 *
 * Validation contract: before committing, the merged shape is re-checked against the same hard-error and capture-coercion invariants the startup path enforces.
 * A configuration that carries a hard error (out-of-range numeric, non-absolute path, conflicting HDHR port) or would need a capture coercion (native mode, a
 * captureCodecs list missing the h264 baseline) is rejected rather than silently coerced: CONFIG stays on the previous valid state and every diffed change is
 * reported as rejected so the operator sees why nothing took effect. This closes the window where a live save could commit an un-normalized capture
 * configuration into the live binding without passing through validateConfiguration. The reject-on-invalid path is exercised by index.reload.test.ts.
 * @returns The aggregate result of dispatching the diff.
 */
export async function reloadConfiguration(): Promise<ApplyResult> {

  // Re-read the on-disk config snapshot that mutateConfig just wrote, then build the new in-memory shape in isolation so normalizations do not mutate the
  // previous snapshot before the diff is computed.
  const result = await readConfig();

  configParseError = result.parseError;
  configParseErrorMessage = result.parseErrorMessage;

  const nextConfig = mergeConfiguration(result.config, stashedCliOverrides);

  // Normalize the candidate in place WITHOUT any global side effects (no runtime debug-filter change). The live debug filter is applied by commitDebugFilter
  // only after this reload is known to be committed, so a rejected reload below never touches the running filter.
  normalizeConfig(nextConfig);

  // Compute the diff between the prior CONFIG and the freshly merged one before any reassignment. Reassignment is safe because ESM exports are live bindings -
  // every import { CONFIG } reads through the binding at access time, so there is no stale-reference window.
  const diff = computeConfigDiff(CONFIG, nextConfig);

  // Re-validate the merged shape against the startup invariants. When it cannot be committed safely, leave CONFIG untouched and report every diffed change as
  // rejected with the joined reason - the save was persisted to disk by the caller, but the live binding stays on the previous valid state until the operator
  // corrects the source. The diff.length guard avoids fabricating rejected entries when there is nothing to apply (an empty diff cannot carry a regression).
  const rejection = collectReloadRejection(nextConfig);

  if((diff.length > 0) && (rejection !== null)) {

    const applyResult: ApplyResult = { applied: [], deferred: [], rejected: diff.map((change) => ({ change, reason: rejection })) };

    LOG.warn("Configuration reload rejected; the saved configuration violates an invariant and was not applied live.", { reason: rejection });
    logReloadOutcome(diff, applyResult);

    return applyResult;
  }

  CONFIG = nextConfig;

  // Now that the new configuration is committed, apply its persisted debug filter to the live runtime. Kept out of normalizeConfig so the rejected path above
  // can never change the running filter.
  commitDebugFilter();

  const applyResult = await applyConfigChanges(diff);

  logReloadOutcome(diff, applyResult);

  return applyResult;
}

/**
 * Normalizes a configuration in place WITHOUT any global side effects: clamps an out-of-vocabulary quality preset to the default and rewrites the persisted
 * debug-filter string to its canonical form. Pure with respect to process state - it touches only the passed config - so it is safe to run on a candidate
 * nextConfig before reload decides whether to commit it. Shared by initializeConfiguration and reloadConfiguration so the two paths cannot drift. The live
 * runtime debug filter is applied separately by commitDebugFilter, which runs only once a configuration is committed, so a rejected reload never changes the
 * running filter.
 * @param config - The freshly merged configuration to normalize in place.
 */
function normalizeConfig(config: Config): void {

  // Canonicalize the persisted debug-filter string (trim whitespace around commas, collapse duplicates) unless a higher-priority env/CLI source owns the filter.
  // This keeps the committed CONFIG and the computed diff working with canonical values and avoids a phantom whitespace-only diff; it does NOT touch the runtime
  // filter - commitDebugFilter performs that side effect after a configuration is committed.
  if(!envOrCliDebugOverride) {

    config.logging.debugFilter = canonicalizeDebugPattern(config.logging.debugFilter);
  }

  // Validate quality preset. Viewport is derived on-demand via getViewport() rather than stored in CONFIG.
  const validPresets = getValidPresetIds();

  if(!validPresets.includes(config.streaming.qualityPreset)) {

    LOG.warn("Invalid quality preset '%s'. Using default '%s'.", config.streaming.qualityPreset, DEFAULTS.streaming.qualityPreset);

    config.streaming.qualityPreset = DEFAULTS.streaming.qualityPreset;
  }
}

/**
 * Applies the committed CONFIG's persisted debug filter to the live runtime filter, when no higher-priority env/CLI source owns it and the canonical value
 * differs from what is currently active. This is the global side effect split out of normalizeConfig: it runs only after a configuration is committed (at
 * startup, and on a reload that passed validation), so a rejected reload leaves the running filter untouched. Re-applying a changed persisted filter here is
 * what lets a debug-filter change delivered via /config/import take effect live instead of waiting for a restart; an emptied persisted filter clears the runtime
 * filter the same way (initDebugFilter("") disables it).
 */
function commitDebugFilter(): void {

  if(!envOrCliDebugOverride && (CONFIG.logging.debugFilter !== getCurrentPattern())) {

    initDebugFilter(CONFIG.logging.debugFilter);
  }
}

/**
 * Live-applies the "logging." subset of a config diff. The debug filter is applied live by commitDebugFilter during reloadConfiguration - the runtime filter is
 * updated right after CONFIG is committed and before the diff dispatches - so it reports applied with no restart, which is what lets a debug-filter change take
 * effect immediately instead of waiting for one. The remaining logging fields (httpLogLevel, maxSize) are consumed when the logger and HTTP middleware are wired
 * at startup, so they defer to a restart.
 *
 * Exported so tests can invoke the dispatch directly with a synthetic diff. Registered with the reactivity primitive at module load (side effect below).
 * @param changes - The subset of the diff whose path begins with "logging.".
 * @returns Per-change outcomes.
 */
// The ConfigChangeHandler contract is async (handlers that do real work, like the HDHomeRun handler, await it), but this handler's classification is purely
// synchronous - there is nothing to await. We keep the async signature so every handler reads the same way and conforms to the type directly.
// eslint-disable-next-line @typescript-eslint/require-await
export async function applyLoggingConfigChanges(changes: readonly ConfigChange[]): Promise<readonly ChangeOutcome[]> {

  return changes.map((change) => {

    if(change.path === "logging.debugFilter") {

      return { kind: "applied", path: change.path };
    }

    return { kind: "deferred", path: change.path, reason: "this logging setting takes effect on the next restart" };
  });
}

// Module-load side effect: register the logging live-apply handler once per process so a debug-filter change that commitDebugFilter already applied live
// does not also trigger a redundant restart. ESM modules load at most once per process, so this runs deterministically at boot before any settings save can fire.
// Tests that reset the reactivity registry can re-register by calling registerConfigChangeHandler("logging.", applyLoggingConfigChanges); the symbol is exported.
registerConfigChangeHandler("logging.", applyLoggingConfigChanges);

/**
 * Logs a one-line summary of a reload outcome so operators can see which changes landed live versus which need a restart. Suppressed for empty diffs - the
 * /config save handler can be called with no metadata fields changed (e.g., only system-state fields touched).
 * @param diff - The diff that was dispatched.
 * @param result - The aggregate apply result.
 */
function logReloadOutcome(diff: readonly ConfigChange[], result: ApplyResult): void {

  if(diff.length === 0) {

    return;
  }

  LOG.info("Configuration reloaded.", {

    applied: result.applied.length,
    deferred: result.deferred.length,
    rejected: result.rejected.length,
    total: diff.length
  });
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

    return name + " must be a positive integer, got: " + String(value);
  }

  return checkBounds(name, value, min, max);
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

    return name + " must be a positive number, got: " + String(value);
  }

  return checkBounds(name, value, min, max);
}

/**
 * Shared min/max bound check used by validatePositiveInt and validatePositiveNumber after their first-guard predicate has accepted the value. Both validators
 * share identical bound-error message shape, so the check lives in one place to prevent drift.
 * @param name - The configuration name for error messages.
 * @param value - The value to bound-check.
 * @param min - Optional minimum allowed value (inclusive).
 * @param max - Optional maximum allowed value (inclusive).
 * @returns Error message if out of range, null if within bounds.
 */
function checkBounds(name: string, value: number, min: number | undefined, max: number | undefined): Nullable<string> {

  if((min !== undefined) && (value < min)) {

    return name + " must be at least " + String(min) + ", got: " + String(value);
  }

  if((max !== undefined) && (value > max)) {

    return name + " must be at most " + String(max) + ", got: " + String(value);
  }

  return null;
}

/**
 * The capture-related coercions a configuration needs to satisfy the streaming invariants the startup path enforces. collectCoercions describes them without
 * mutating; applyCoercions applies them (startup); reloadConfiguration treats a non-empty set as grounds to reject a live save rather than coerce silently. The
 * preset and debug-filter normalizations are intentionally NOT modeled here - those are benign canonicalizations handled by normalizeConfig on both
 * the startup and reload paths, whereas these capture coercions guard safety-critical invariants (native mode corrupts output and disables HDHR; the h264
 * baseline is universal) and so must surface to the operator on reload rather than be silently rewritten.
 */
interface ConfigCoercions {

  // The normalized captureCodecs list (unrecognized identifiers removed, h264 baseline ensured), present only when it differs from the input list.
  readonly captureCodecs: Nullable<readonly string[]>;

  // True when captureMode is not "ffmpeg" and must be forced. Chrome's native fMP4 MediaRecorder produces corrupt output after 20-30 minutes of recording.
  readonly forceFfmpegMode: boolean;
}

/**
 * Describes the capture coercions a configuration would need, without mutating it. Pure so both the startup path (which then applies them) and the reload path
 * (which rejects when any are present) can ask the same question against any config snapshot.
 * @param config - The configuration to inspect.
 * @returns The set of needed coercions; captureCodecs is null and forceFfmpegMode is false when none apply.
 */
function collectCoercions(config: Config): ConfigCoercions {

  // Filter captureCodecs to recognized identifiers and guarantee the h264 universal baseline. RECOGNIZED_CODECS in types/streaming.ts is the single definition
  // for all capture codec identifiers.
  const recognizedCodecs = new Set<string>(RECOGNIZED_CODECS);
  const normalizedCodecs = config.streaming.captureCodecs.filter((codec) => recognizedCodecs.has(codec));

  if(!normalizedCodecs.includes("h264")) {

    normalizedCodecs.unshift("h264");
  }

  // Only report a captureCodecs coercion when the normalized list actually differs from the input - identical contents must not trip the reload rejection.
  const captureCodecsChanged = (normalizedCodecs.length !== config.streaming.captureCodecs.length) ||
    normalizedCodecs.some((codec, index) => (codec !== config.streaming.captureCodecs[index]));

  return {

    captureCodecs: captureCodecsChanged ? normalizedCodecs : null,
    forceFfmpegMode: config.streaming.captureMode !== "ffmpeg"
  };
}

/**
 * Predicate: does this coercion set require any change? Used to record whether a startup write-back is needed.
 * @param coercions - The coercions to test.
 * @returns True when at least one coercion would change the configuration.
 */
function hasCoercions(coercions: ConfigCoercions): boolean {

  return (coercions.captureCodecs !== null) || coercions.forceFfmpegMode;
}

/**
 * Applies the described coercions to a configuration in place, emitting the same operator-visible warning the startup path has always logged when forcing
 * FFmpeg mode. Used only by the startup path; the reload path rejects rather than coerces.
 * @param config - The configuration to mutate.
 * @param coercions - The coercions to apply, as computed by collectCoercions.
 */
function applyCoercions(config: Config, coercions: ConfigCoercions): void {

  if(coercions.captureCodecs !== null) {

    config.streaming.captureCodecs = Array.from(coercions.captureCodecs);
  }

  if(coercions.forceFfmpegMode) {

    LOG.warn("Native capture mode is disabled due to a Chrome fMP4 MediaRecorder bug. Forcing FFmpeg capture mode.");

    config.streaming.captureMode = "ffmpeg";
  }
}

/**
 * Collects every hard configuration error - an always-fatal value that no coercion can repair - for the given configuration. Pure: it never mutates and never
 * throws, so both validateConfiguration (which throws on a non-empty result at startup) and reloadConfiguration (which rejects the save) can reuse it.
 * @param config - The configuration to validate.
 * @returns The list of error messages; empty when the configuration has no hard errors.
 */
function collectHardErrors(config: Config): string[] {

  // Collect every hard error before returning so the operator sees all problems at once rather than fixing them one restart at a time. The check helper pushes
  // non-null validator results, reducing each validation to a single line.
  const errors: string[] = [];
  const check = (result: Nullable<string>): void => { if(result) { errors.push(result); } };

  // Server configuration.
  check(validatePositiveInt("PORT", config.server.port, 1, 65535));

  // Streaming bitrates. Video: 100kbps-50Mbps. Audio: 32-512kbps.
  check(validatePositiveInt("VIDEO_BITRATE", config.streaming.videoBitsPerSecond, 100000, 50000000));
  check(validatePositiveInt("AUDIO_BITRATE", config.streaming.audioBitsPerSecond, 32000, 512000));

  // Timeouts. 1 second minimum prevents premature failures. 10 minutes maximum prevents indefinite hangs.
  check(validatePositiveInt("NAV_TIMEOUT", config.streaming.navigationTimeout, 1000, 600000));
  check(validatePositiveInt("VIDEO_TIMEOUT", config.streaming.videoTimeout, 1000, 600000));

  // Concurrent stream limit.
  check(validatePositiveInt("MAX_CONCURRENT_STREAMS", config.streaming.maxConcurrentStreams, 1, 100));

  // Circuit breaker threshold.
  check(validatePositiveInt("CIRCUIT_BREAKER_THRESHOLD", config.recovery.circuitBreakerThreshold, 1, 100));

  // Browser relaunch governor bounds.
  check(validatePositiveInt("RELAUNCH_FAILURE_THRESHOLD", config.recovery.relaunchFailureThreshold, 1, 20));
  check(validatePositiveInt("RELAUNCH_FAILURE_WINDOW", config.recovery.relaunchFailureWindow, 60000, 3600000));
  check(validatePositiveInt("RELAUNCH_HEALTH_HOLD", config.recovery.relaunchHealthHold, 60000, 600000));

  // Stall threshold (float).
  check(validatePositiveNumber("STALL_THRESHOLD", config.playback.stallThreshold, 0.01, 5));

  // Logging. 10KB minimum ensures meaningful content. 100MB maximum prevents excessive disk usage.
  check(validatePositiveInt("LOG_MAX_SIZE", config.logging.maxSize, 10240, 104857600));

  // HLS configuration.
  check(validatePositiveInt("HLS_SEGMENT_DURATION", config.hls.segmentDuration, 1, 10));
  check(validatePositiveInt("HLS_MAX_SEGMENTS", config.hls.maxSegments, 3, 60));
  check(validatePositiveInt("HLS_IDLE_TIMEOUT", config.hls.idleTimeout, 10000, 300000));

  // Validate path overrides. When set, both chromeDataDir and logFile must be absolute paths to prevent ambiguity.
  if((config.paths.chromeDataDir !== null) && !path.isAbsolute(config.paths.chromeDataDir)) {

    errors.push("paths.chromeDataDir must be an absolute path, got: " + config.paths.chromeDataDir);
  }

  if((config.paths.logFile !== null) && !path.isAbsolute(config.paths.logFile)) {

    errors.push("paths.logFile must be an absolute path, got: " + config.paths.logFile);
  }

  // Validate the HDHomeRun port only when HDHR is enabled and the effective capture mode is FFmpeg. Native capture mode disables HDHR (and is itself a coercion
  // surfaced by collectCoercions), so the HDHR port is never bound in that state and a port check against it would be moot. At startup applyCoercions has
  // already forced FFmpeg mode by the time this runs, so the guard reduces to "HDHR enabled"; on reload a native-mode config is rejected before reaching here.
  if(config.hdhr.enabled && (config.streaming.captureMode === "ffmpeg")) {

    check(validatePositiveInt("HDHR_PORT", config.hdhr.port, 1, 65535));

    // Warn if HDHR port conflicts with the main server port (same host).
    if((config.hdhr.port === config.server.port) && ((config.server.host === "0.0.0.0") || (config.server.host === "::"))) {

      errors.push("HDHR_PORT (" + String(config.hdhr.port) + ") conflicts with the main server port.");
    }
  }

  return errors;
}

/**
 * Validates all configuration values and throws an error if any are invalid. This function runs at startup after configuration initialization. It first applies
 * the capture coercions in place (filtering captureCodecs, forcing FFmpeg mode, with the same operator-visible warning as before), then collects every hard
 * error against the coerced CONFIG and throws once with the complete list. Splitting the pure collectors (collectCoercions, collectHardErrors) from the in-place
 * application lets reloadConfiguration reuse the very same invariants without silently coercing a live save.
 * @throws If any configuration value is invalid. The error message lists all invalid values.
 */
export function validateConfiguration(): void {

  const coercions = collectCoercions(CONFIG);

  // Record whether a capture coercion was applied so persistCoercedConfig can write the corrected values back to disk and keep the on-disk state honest.
  captureConfigCoercedAtStartup = hasCoercions(coercions);

  applyCoercions(CONFIG, coercions);

  const errors = collectHardErrors(CONFIG);

  // If any validation errors occurred, throw with the complete list so the operator can fix every issue at once.
  if(errors.length > 0) {

    throw new Error("Configuration validation failed:\n  " + errors.join("\n  "));
  }
}

/**
 * Persists the capture configuration that validateConfiguration coerced at startup back to disk so the on-disk state matches the live CONFIG. A no-op unless a
 * coercion actually occurred. Without this, a config file holding an unsupported capture value (native mode, or a captureCodecs list missing the h264 baseline)
 * stays divergent from the coerced live CONFIG forever, and the reload-validation path would then reject every later save on the resulting phantom capture diff.
 * filterDefaults strips any value equal to its default on write, so a config coerced back to the FFmpeg/h264 defaults leaves a clean file with no capture override.
 * Failures degrade gracefully: the live CONFIG is already coerced, so a write failure only means the divergence persists until the next successful save or boot.
 */
export async function persistCoercedConfig(): Promise<void> {

  if(!captureConfigCoercedAtStartup) {

    return;
  }

  try {

    await mutateConfig((config) => {

      config.streaming ??= {};
      config.streaming.captureCodecs = Array.from(CONFIG.streaming.captureCodecs);
      config.streaming.captureMode = CONFIG.streaming.captureMode;
    });

    LOG.info("Normalized capture configuration written to disk after a startup coercion.");
  } catch(error) {

    LOG.warn("Failed to persist the normalized capture configuration to disk: %s.", formatError(error));
  }
}

/**
 * Determines whether a freshly merged configuration must be rejected on reload rather than committed. Returns a single human-readable reason when the
 * configuration carries a hard error (an always-fatal value) or would require a capture coercion the reload path refuses to apply silently, or null when the
 * configuration is safe to commit live. The reasons are complete sentences because they are surfaced verbatim to the operator in the settings-save response.
 * @param config - The merged, normalized configuration about to be committed.
 * @returns The joined rejection reason, or null when the configuration may be committed.
 */
function collectReloadRejection(config: Config): Nullable<string> {

  const reasons: string[] = collectHardErrors(config);
  const coercions = collectCoercions(config);

  if(coercions.forceFfmpegMode) {

    reasons.push("Native capture mode is disabled and cannot be applied live; set capture mode to FFmpeg.");
  }

  if(coercions.captureCodecs !== null) {

    reasons.push("Capture codecs must include the h264 baseline and contain only recognized identifiers.");
  }

  if(reasons.length === 0) {

    return null;
  }

  return reasons.join(" ");
}

/**
 * Emits a single indented configuration row in the "label: value" format used by the startup display. Routes through displayLine so the line stays free of the
 * sentence-normalization contract that LOG.info applies - a config dump is tabular display, not a sentence, and forcing a trailing period on every row would
 * degrade readability of the block as a whole. Keeps the call-site authoring shape minimal so future additions are a one-liner.
 * @param label - The metric label, displayed left of the colon.
 * @param value - The value to display. Coerced via String() so callers can pass numbers, booleans, or any value without per-call ceremony.
 */
function printConfigRow(label: string, value: unknown): void {

  displayLine("  " + label + ": " + String(value));
}

/**
 * Displays the active configuration at startup. This helps operators verify their settings and diagnose connection issues. We log only the most commonly adjusted
 * values to keep output concise while providing useful debugging information.
 *
 * The block is emitted through displayLine / printConfigRow rather than LOG.info because it is structured display output (a header plus indented label/value
 * rows), not prose log messages - the logger's sentence-normalization contract is intentionally bypassed for this block so the rows render as tabular data, not
 * sentences.
 *
 * This function also checks for preset degradation and logs a warning if the configured preset exceeds display capabilities. The warning helps users understand
 * why their stream resolution may be lower than configured.
 */
export function displayConfiguration(): void {

  const presetResult = getEffectivePreset(CONFIG);
  const presetStatus = formatPresetStatus(presetResult);

  displayLine("Starting PrismCast v%s with configuration:", getPackageVersion());
  printConfigRow("Configuration file", getConfigFilePath());
  printConfigRow("Chrome profile", getChromeDataDir(CONFIG));
  printConfigRow("Server port", CONFIG.server.port);
  printConfigRow("Quality preset", presetStatus);
  printConfigRow("Capture codecs", CONFIG.streaming.captureCodecs.join(", "));
  printConfigRow("Video bitrate", CONFIG.streaming.videoBitsPerSecond);
  printConfigRow("Max retries", CONFIG.streaming.maxNavigationRetries);
  printConfigRow("Max concurrent streams", CONFIG.streaming.maxConcurrentStreams);
  displayLine("  Circuit breaker threshold: %s failures in %s minutes",
    CONFIG.recovery.circuitBreakerThreshold, Math.round(CONFIG.recovery.circuitBreakerWindow / 60000));
  displayLine("  Browser relaunch governor: trips at %s failures in %s minutes, %s minute health hold",
    CONFIG.recovery.relaunchFailureThreshold, Math.round(CONFIG.recovery.relaunchFailureWindow / 60000), Math.round(CONFIG.recovery.relaunchHealthHold / 60000));
  printConfigRow("Chrome executable", CONFIG.browser.executablePath ?? "autodetect");
  displayLine("  HLS segment duration: %ss, max segments: %s", CONFIG.hls.segmentDuration, CONFIG.hls.maxSegments);
  printConfigRow("HDHomeRun emulation", CONFIG.hdhr.enabled ? "enabled (port " + String(CONFIG.hdhr.port) + ")" : "disabled");

  // Log a prominent warning if preset was degraded due to display limitations.
  if(presetResult.degraded && presetResult.maxViewport) {

    LOG.warn("Display supports maximum %s\u00d7%s. Configured %s preset will use %s instead.",
      presetResult.maxViewport.width, presetResult.maxViewport.height,
      presetResult.configuredPreset.id, presetResult.effectivePreset.id);
  }

}
