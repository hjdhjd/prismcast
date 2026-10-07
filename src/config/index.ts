/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Configuration management for PrismCast.
 */
import type { ApplyResult, ConfigChangePartition } from "./reactivity.ts";
import { CONFIG_METADATA, DEFAULTS, getNestedValue, getReactivityClass, getSettingByPath, mergeConfiguration, mutateConfig, readConfig,
  setNestedValue } from "./userConfig.ts";
import type { Config, Nullable } from "../types/index.ts";
import { LOG, canonicalizeDebugPattern, displayLine, formatError, getCurrentPattern, getPackageVersion, initDebugFilter, isAnyDebugEnabled } from "../utils/index.ts";
import { applyConfigChanges, computeConfigDiff, partitionConfigChanges } from "./reactivity.ts";
import { getChromeDataDir, getConfigFilePath } from "./paths.ts";
import { getPresetViewport, getValidPresetIds } from "./presets.ts";
import { RECOGNIZED_CODECS } from "../types/index.ts";
import type { UserConfig } from "./userConfig.ts";
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
 * - hls: HLS output tuning (segment duration, buffer depth, idle timeout)
 * - playback: Health monitoring intervals and recovery timing thresholds
 * - recovery: Retry backoff parameters and circuit breaker configuration
 * - channels: Predefined channel enable/disable state, table sort, and service filter
 * - channelsDvr: Connection settings for the user's external Channels DVR server
 * - hdhr: HDHomeRun emulation settings (port, device ID, LAN discovery)
 * - logging: File-based logging behavior (debug filter, HTTP request log level)
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

/* ConfigStore is the disk-persistence boundary config/index.ts composes on: the read-load and write-back operations backed by the file store. It is injected as a
 * default parameter on the persistence-facing functions so a test can substitute an in-memory store in the same place - no loader mock - while production uses
 * the real defaultConfigStore. mergeConfiguration and the normalization helpers stay direct because they are pure and touch no disk. This mirrors the library's
 * Clock port: a typed interface plus a module-const default, consumed through a defaulted parameter.
 */
export interface ConfigStore {

  readonly mutateConfig: typeof mutateConfig;
  readonly readConfig: typeof readConfig;
}

const defaultConfigStore: ConfigStore = { mutateConfig, readConfig };

// The CONFIG object is the running configuration. It starts as a copy of DEFAULTS, is replaced by the merged configuration at startup, and from then on moves
// one realized leaf at a time as saves reconcile it, so a restart-class value a save holds out never reaches it.
export let CONFIG: Config = structuredClone(DEFAULTS);

/* The loaded snapshot: the configuration file as last loaded, merged and normalized exactly as the next boot would read it. It is recorded at startup, again
 * after the startup coercions, and at the end of every completed reconcile, once that reconcile has committed what its handlers realized, and nowhere else.
 * The running configuration and this snapshot differ exactly where the process does not yet reflect the file - a restart-class value waiting for the
 * restart, a live value a handler refused, or a leaf the process wrote ahead of the file - and every save reconciles that gap.
 */
let loadedConfig: Config = structuredClone(DEFAULTS);

/**
 * The error a save throws when the configuration it would write fails validation. The store writes nothing when its mutation throws, so the file, the running
 * configuration and the loaded snapshot all stay as they were. Its message is the validation reason alone, complete sentences an operator can act on, so a
 * route answers it as the validation error it is.
 */
export class ConfigurationRejectedError extends Error {

  override name = "ConfigurationRejectedError";
}

/**
 * Indicates whether a user config file parse error occurred during initialization. The web UI displays a warning when this is true.
 */
export let configParseError = false;

/**
 * The parse error message if configParseError is true.
 */
export let configParseErrorMessage: string | undefined;

// Stashed CLI overrides from the most recent initializeConfiguration call. Every candidate a save builds re-applies them so the priority chain (CLI > env >
// user > defaults) holds for the saved configuration exactly as it did at boot. CLI overrides are a startup-only concern in practice; capturing them once and
// replaying them avoids losing the binding when a save runs in a process where the operator originally passed --port or --chrome-data-dir. The --data-dir
// flag is resolved separately, before configuration loads, and never flows through this stash.
let stashedCliOverrides: CliOverrides | undefined;

// Whether a higher-priority debug source (the PRISMCAST_DEBUG env var or the --debug CLI flag) established the active debug filter before the persisted config
// filter was first applied. Captured once in initializeConfiguration, ahead of the persisted filter, so a later save can re-apply a changed persisted filter
// live without ever clobbering an env/CLI override - that override must win for the entire process lifetime.
let envOrCliDebugOverride = false;

/* Serialization queue for the reconcile. Each save's reconcile chains onto the previous one, so overlapping saves commit in the order their writes landed.
 * Without it, a reconcile waiting on a slow handler could commit its realized changes after a later save's reconcile had already committed newer values,
 * leaving the running configuration on a state the user already replaced.
 */
let reconcileQueue: Promise<unknown> = Promise.resolve();

// Whether validateConfiguration coerced a capture setting (forced FFmpeg mode, normalized captureCodecs) on the live CONFIG at startup. persistCoercedConfig
// reads this to decide whether to write the coerced values back to disk so the on-disk state matches the live binding. Without the write-back a config file
// holding an unsupported capture value (native mode) stays divergent from the coerced CONFIG forever, and the save's validation would then refuse every later
// save, because each candidate is built from that file.
let captureConfigCoercedAtStartup = false;

/**
 * Initializes the configuration by loading the user config file, merging with defaults, applying environment variable overrides, and applying CLI overrides. This
 * must be called at startup before any code accesses CONFIG. After initialization, the CONFIG object contains the final merged values.
 * @param cliOverrides - Optional CLI flag overrides, applied at the highest priority level.
 */
export async function initializeConfiguration(cliOverrides?: CliOverrides, io: ConfigStore = defaultConfigStore): Promise<void> {

  // Load user configuration from file. Schema migrations (legacy provider field renames, foxcom -> foxone in enabledServices) run automatically inside the
  // file store framework via the declarative configMigrations registry; ensureMigrated (called by the release boot coordinator at startup) persists any
  // upgrades to disk before this function runs. The data returned here is always at CURRENT_CONFIG_SCHEMA_VERSION.
  const result = await io.readConfig();

  configParseError = result.parseError;
  configParseErrorMessage = result.parseErrorMessage;
  stashedCliOverrides = cliOverrides;

  // The store answers a file it could not read and a file with no parseable copy with the defaults, and the boot starts from them rather than refusing to start.
  // One warning names which failure it was, because the store refuses every write until the file is readable again.
  if(result.readError) {

    LOG.warn("The configuration file could not be read, so the configuration starts from the defaults and saves are refused until it can be.");
  } else if(result.parseError) {

    LOG.warn("The configuration file is not valid JSON and has no usable backup, so the configuration starts from the defaults and saves are refused until it is.",
      { reason: result.parseErrorMessage });
  }

  // Capture whether a higher-priority debug source (PRISMCAST_DEBUG / --debug) already owns the active filter before we apply the persisted config filter, so a
  // later save can re-apply a changed persisted filter live without overriding env/CLI. Measured here, ahead of normalizeConfig/commitDebugFilter, it reflects
  // env/CLI alone rather than the persisted filter applying to itself.
  envOrCliDebugOverride = isAnyDebugEnabled();

  // Build the running configuration exactly as every save builds its candidate, then commit the persisted debug filter to the runtime. The two steps are kept
  // separate so a save can build and validate a candidate without applying its filter until a reconcile commits it.
  CONFIG = buildCandidate(result.config);
  commitDebugFilter();
  recordLoadedConfiguration();

  LOG.info("Configuration initialized from defaults, user config, environment variables, and CLI overrides.");
}

/**
 * Returns the loaded snapshot: the configuration file as last loaded, merged and normalized exactly as the next boot would read it. Where it differs from CONFIG,
 * the running process does not yet reflect the file. The settings form renders from it, so a page loaded after a save shows what is saved rather than what is
 * running, and submitting that page cannot post the running value back over a change waiting for a restart.
 * @returns The loaded snapshot. Callers read it and never mutate it.
 */
export function getLoadedConfiguration(): Readonly<Config> {

  return loadedConfig;
}

/**
 * Answers which saved settings the running process does not yet reflect: the gap between CONFIG and the loaded snapshot, partitioned by reactivity class. Its
 * held members are the settings pending a restart, and its live and next-stream members are the settings a handler could not realize. The view covers the
 * settings surface alone, the paths CONFIG_METADATA declares, because a leaf the process writes and the loaded snapshot does not yet hold is the process ahead
 * of the file rather than a save the user is waiting on. Derived on every call from CONFIG and the loaded snapshot, so it holds no state of its own. A
 * reconcile publishes the snapshot only once it has committed what its handlers realized, so a caller outside the reconcile queue reads CONFIG and the snapshot
 * as the last completed reconcile left them, and a change a save introduces is never listed as unrealized while its handlers are still running.
 * @returns The settings-surface gap, partitioned into held, live, and next-stream changes, each carrying the running value as previous and the saved value as
 *   current.
 */
export function getConfigurationGap(): ConfigChangePartition {

  const settingsSurface = computeConfigDiff(CONFIG, loadedConfig).filter((change) => getSettingByPath(change.path) !== undefined);

  return partitionConfigChanges(settingsSurface, getReactivityClass);
}

/**
 * The one entry for a write to the settings surface - the settings save, the import, and the debug page all go through it. Inside the store's own mutation it
 * applies the caller's mutator to the current file, builds the candidate exactly as the next boot would read that file, and refuses an invalid candidate by
 * throwing ConfigurationRejectedError with the validation reason, so the store writes nothing. A file the store cannot parse or cannot read refuses the
 * mutation inside the store the same way. Once the file is written, the save enqueues the reconcile of that candidate and answers with its result.
 *
 * The store's queue serializes the read-modify-validate-write of concurrent saves, and the reconcile queue serializes their commits in the order their writes
 * landed. A config-change handler never saves, because the reconcile it runs inside holds the queue that save's reconcile would wait on.
 * @param mutator - Applies the caller's change to the current file in place. A throw inside it writes nothing.
 * @param io - The config store the save writes through.
 * @returns What the reconcile realized, the restart-class changes this save holds for a restart, and the changes of this save a handler refused.
 * @throws ConfigurationRejectedError when the candidate fails validation, and the store's own error when the file cannot be parsed, read, or written.
 */
export async function saveConfiguration(mutator: (current: UserConfig) => void, io: ConfigStore = defaultConfigStore): Promise<ApplyResult> {

  // The callback runs under the store's queue against the file the store just read, so the candidate is built from exactly the configuration this save writes.
  // The store runs it before resolving, which is what lets the reconcile below start from it without reading the file a second time.
  const saved: { candidate?: Config } = {};

  await io.mutateConfig((current) => {

    mutator(current);

    // The candidate is built on a clone so normalization never reaches the file object the store is about to write.
    const candidate = buildCandidate(structuredClone(current));
    const rejection = collectCandidateRejection(candidate);

    if(rejection !== null) {

      throw new ConfigurationRejectedError(rejection);
    }

    saved.candidate = candidate;
  });

  const { candidate } = saved;

  if(candidate === undefined) {

    throw new Error("The configuration store completed the save without running it, so there is nothing to reconcile.");
  }

  // The store read and parsed the file to run the callback, so a parse failure the boot recorded does not describe the file this save wrote.
  configParseError = false;
  configParseErrorMessage = undefined;

  const operation = reconcileQueue.then(async () => reconcileConfiguration(candidate));

  // Swallow errors on the chain reference so future reconciles can proceed. The error still propagates to the caller via the returned promise.
  // eslint-disable-next-line @typescript-eslint/no-empty-function -- Intentional no-op: errors are propagated to the caller via the returned promise.
  reconcileQueue = operation.catch(() => {});

  return operation;
}

/**
 * Reconciles the running configuration against a candidate a save has just written. The gap between CONFIG and the candidate is partitioned by class: its
 * restart-class changes are held out of CONFIG, and its live and next-stream changes go to their handlers together with the candidate running configuration -
 * CONFIG with those changes applied - so a handler realizes the state it is handed. Only the changes no handler refused are committed, one path at a time, so a
 * process write that lands while the handlers run survives, and nothing is rolled back: a refused change stays in the gap, and every later save retries it.
 *
 * The candidate becomes the loaded snapshot only once the realized changes are committed. A reader outside the reconcile queue - the settings form, the gap
 * accessor - therefore sees CONFIG and the loaded snapshot only as a completed reconcile left them, and a change this save introduces never reads as
 * unrealized while its handlers are still running. A reconcile that throws before that point leaves the snapshot where it was: the next save's gap retries
 * whatever this one did not commit, and its delta, read against that earlier snapshot, reports this save's changes again.
 *
 * The result reports everything the reconcile realized. Its deferred and rejected lists answer for this save alone, through the delta between the previous
 * loaded snapshot and this one: a restart-class change is deferred when this save introduced it and it still differs from the running value, so a save that
 * writes the running value back schedules no restart, and a refusal is reported when this save asked for the refused change. Called only through the reconcile
 * queue in saveConfiguration.
 * @param candidate - The configuration the save wrote, merged, normalized, and validated.
 * @returns The save's outcome.
 */
async function reconcileConfiguration(candidate: Config): Promise<ApplyResult> {

  const previousLoaded = loadedConfig;
  const delta = partitionConfigChanges(computeConfigDiff(previousLoaded, candidate), getReactivityClass);
  const gap = partitionConfigChanges(computeConfigDiff(CONFIG, candidate), getReactivityClass);
  const next = structuredClone(CONFIG);

  for(const change of [ ...gap.live, ...gap.nextStream ]) {

    setNestedValue(next as unknown as Record<string, unknown>, change.path, structuredClone(change.current));
  }

  const dispatch = await applyConfigChanges(gap, next);

  // Commit exactly what the handlers realized, leaf by leaf, rather than assigning whole categories: a process writer may have moved another leaf of CONFIG while
  // the handlers ran, and a per-path commit leaves that write in place. Each value is cloned so CONFIG never shares an array with the loaded snapshot.
  for(const change of dispatch.realized) {

    setNestedValue(CONFIG as unknown as Record<string, unknown>, change.path, structuredClone(change.current));
  }

  // The snapshot moves here, after what the handlers realized is committed, so CONFIG and the loaded snapshot change together for every reader outside the
  // reconcile queue.
  loadedConfig = candidate;

  // The debug filter's one side effect: a committed change to the persisted filter reaches the runtime filter here, unless an env or CLI source owns it.
  commitDebugFilter();

  const livePaths = new Set(gap.live.map((change) => change.path));
  const heldPaths = new Set(gap.held.map((change) => change.path));
  const requestedPaths = new Set([ ...delta.live, ...delta.nextStream ].map((change) => change.path));
  const result: ApplyResult = {

    applied: dispatch.realized.filter((change) => livePaths.has(change.path)),
    deferred: delta.held.filter((change) => heldPaths.has(change.path)),
    nextStream: dispatch.realized.filter((change) => !livePaths.has(change.path)),
    rejected: dispatch.rejected.filter((refusal) => requestedPaths.has(refusal.change.path))
  };

  logReconcileOutcome(result);

  return result;
}

/**
 * Builds a configuration from a user configuration file exactly as the boot does: defaults, the file, the environment, and the stashed CLI overrides merged in
 * priority order, then normalized. The boot and every save share it, so a saved candidate is the configuration the next boot would read.
 * @param userConfig - The user configuration file. A save passes a clone of the file it is writing, so nothing the build does reaches that object.
 * @returns The merged, normalized configuration.
 */
function buildCandidate(userConfig: UserConfig): Config {

  const config = mergeConfiguration(userConfig, stashedCliOverrides);

  normalizeConfig(config);

  return config;
}

/**
 * Records the running configuration as the loaded snapshot. It runs at the end of initializeConfiguration, and inside validateConfiguration after the startup
 * coercions and before the hard-error check, which a boot that fails exits on. Those are the startup steps that build CONFIG from the file, so the snapshot
 * starts equal to the running configuration and a save's gap holds only what the process has not realized.
 */
function recordLoadedConfiguration(): void {

  loadedConfig = structuredClone(CONFIG);
}

/**
 * Normalizes a configuration in place WITHOUT any global side effects: clamps an out-of-vocabulary quality preset to the default, clamps an out-of-range frame
 * rate to the nearer bound its metadata declares, and rewrites the persisted debug-filter string to its canonical form. Pure with respect to process state - it
 * touches only the passed config - so it is safe to run on a candidate a save may still refuse. Every configuration is built through buildCandidate, so the
 * boot and the save cannot drift. The live runtime debug filter is applied separately by commitDebugFilter, which runs only once a configuration is
 * committed, so a refused save never changes the running filter.
 * @param config - The freshly merged configuration to normalize in place.
 */
function normalizeConfig(config: Config): void {

  // Canonicalize the persisted debug-filter string (trim whitespace around commas, collapse duplicates) unless a higher-priority env/CLI source owns the filter.
  // This keeps the committed CONFIG and the computed diff working with canonical values and avoids a phantom whitespace-only diff; it does NOT touch the runtime
  // filter - commitDebugFilter performs that side effect after a configuration is committed.
  if(!envOrCliDebugOverride) {

    config.logging.debugFilter = canonicalizeDebugPattern(config.logging.debugFilter);
  }

  // Validate quality preset. Viewport is derived on-demand via getPresetViewport() rather than stored in CONFIG.
  const validPresets = getValidPresetIds();

  if(!validPresets.includes(config.streaming.qualityPreset)) {

    LOG.warn("The configured quality preset is not one the server recognizes, so the default preset is in use.",
      { configured: config.streaming.qualityPreset, using: DEFAULTS.streaming.qualityPreset });

    config.streaming.qualityPreset = DEFAULTS.streaming.qualityPreset;
  }

  // Hold the frame rate inside the range its metadata declares, the same bounds the settings form validates a save against. The capture constraint holds the
  // track to this rate on both bounds, so a rate outside the range is clamped to the nearer bound with a warning rather than handed to tab capture or refused at
  // startup. A bound the metadata leaves undeclared constrains nothing.
  const { max = Infinity, min = -Infinity } = getSettingByPath("streaming.frameRate") ?? {};
  const clampedFrameRate = Math.min(Math.max(config.streaming.frameRate, min), max);

  if(clampedFrameRate !== config.streaming.frameRate) {

    LOG.warn("The configured frame rate is outside the supported range, so the nearer bound is in use.",
      { applied: clampedFrameRate, configured: config.streaming.frameRate, max, min });

    config.streaming.frameRate = clampedFrameRate;
  }
}

/**
 * Applies the committed CONFIG's persisted debug filter to the live runtime filter, when no higher-priority env/CLI source owns it and the canonical value
 * differs from what is currently active. This is the global side effect split out of normalizeConfig: it runs only after a configuration is committed (at
 * startup, and at the end of every reconcile), so a refused save leaves the running filter untouched. Re-applying a changed persisted filter here is what lets
 * a debug-filter change delivered via /config/import take effect live instead of waiting for a restart; an emptied persisted filter clears the runtime filter
 * the same way (initDebugFilter("") disables it).
 */
function commitDebugFilter(): void {

  if(!envOrCliDebugOverride && (CONFIG.logging.debugFilter !== getCurrentPattern())) {

    initDebugFilter(CONFIG.logging.debugFilter);
  }
}

/**
 * Logs a one-line summary of a save's outcome so operators can see which changes took effect and which wait for a restart. Silent when every bucket is empty,
 * because a save that changed nothing the process reads - a form submitted unchanged - has nothing to report.
 * @param result - The save's outcome.
 */
function logReconcileOutcome(result: ApplyResult): void {

  const counts = { applied: result.applied.length, deferred: result.deferred.length, nextStream: result.nextStream.length, rejected: result.rejected.length };

  if(Object.values(counts).every((count) => count === 0)) {

    return;
  }

  LOG.info("Configuration saved and reconciled with the running process.", counts);
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
 * Validates that a configuration value is an integer at or above its floor and at or below an optional ceiling. The floor is the minimum the setting's metadata
 * declares, and 1 when none is declared, so a setting whose metadata allows zero accepts it and an integer setting with no declared minimum stays positive. It
 * returns an error message rather than throwing, so the caller can collect every error before reporting them. Every message is a complete sentence naming the
 * value it refused, and a value below the floor draws a message naming the floor it applied, because a save joins the messages into the reason it answers with.
 * @param name - The configuration name for error messages, typically the environment variable name or the setting's label.
 * @param value - The value to validate.
 * @param min - The declared minimum (inclusive). When it is omitted, the floor is 1.
 * @param max - Optional maximum allowed value (inclusive).
 * @returns Error message if invalid, null if valid.
 */
export function validateInteger(name: string, value: number, min?: number, max?: number): Nullable<string> {

  // A non-integer, including the NaN an unparseable input yields, is refused before the bounds are read, because no bound makes it valid.
  if(!Number.isInteger(value)) {

    return name + " must be an integer, but it is " + String(value) + ".";
  }

  return checkBounds(name, value, min ?? 1, max);
}

/**
 * Validates that a configuration value is a number, fractional or whole, at or above its floor and at or below an optional ceiling. The floor is the minimum the
 * setting's metadata declares, and when none is declared the value must be positive, so zero is accepted only where the metadata says it is. Every message is a
 * complete sentence naming the value it refused and, for a value below the floor, the floor it applied.
 * @param name - The configuration name for error messages.
 * @param value - The value to validate.
 * @param min - The declared minimum (inclusive). When it is omitted, the value must be greater than zero.
 * @param max - Optional maximum allowed value (inclusive).
 * @returns Error message if invalid, null if valid.
 */
export function validateNumber(name: string, value: number, min?: number, max?: number): Nullable<string> {

  // NaN compares false against every bound, so it is refused by name before the bounds are read.
  if(Number.isNaN(value)) {

    return name + " must be a number, but it is " + String(value) + ".";
  }

  // With no declared minimum the floor excludes zero itself, which an inclusive minimum cannot state, so this validator applies it rather than checkBounds.
  if((min === undefined) && (value <= 0)) {

    return name + " must be a positive number, but it is " + String(value) + ".";
  }

  return checkBounds(name, value, min, max);
}

/**
 * Shared bound check for validateInteger and validateNumber, run once a value has passed its validator's type check. The floor and the ceiling are inclusive,
 * and the floor is the one the validator applies: the declared minimum, or 1 for an integer with none declared. The validators answer a value out of range in
 * the same words, so the check lives in one place to keep them from drifting.
 * @param name - The configuration name for error messages.
 * @param value - The value to bound-check.
 * @param min - The floor the validator applies (inclusive), or undefined when it applies none here.
 * @param max - Optional maximum allowed value (inclusive).
 * @returns Error message if out of range, null if within bounds.
 */
function checkBounds(name: string, value: number, min: number | undefined, max: number | undefined): Nullable<string> {

  if((min !== undefined) && (value < min)) {

    return name + " must be at least " + String(min) + ", but it is " + String(value) + ".";
  }

  if((max !== undefined) && (value > max)) {

    return name + " must be at most " + String(max) + ", but it is " + String(value) + ".";
  }

  return null;
}

/**
 * The capture-related coercions a configuration needs to satisfy the streaming requirements the startup path enforces. collectCoercions describes them without
 * mutating; applyCoercions applies them (startup); a save treats a non-empty set as grounds to refuse its candidate rather than coerce silently. The
 * preset, frame-rate, and debug-filter normalizations are intentionally NOT modeled here - those are benign corrections handled by normalizeConfig on both
 * the startup and save paths, whereas these capture coercions guard safety-critical requirements (native capture mode corrupts output after 20-30 minutes of
 * recording; the h264 baseline is universal) and so must surface to the operator on a save rather than be silently rewritten.
 */
interface ConfigCoercions {

  // The normalized captureCodecs list (unrecognized identifiers removed, h264 baseline ensured), present only when it differs from the input list.
  readonly captureCodecs: Nullable<readonly string[]>;

  // True when captureMode is not "ffmpeg" and must be forced. Chrome's native fMP4 MediaRecorder produces corrupt output after 20-30 minutes of recording.
  readonly forceFfmpegMode: boolean;
}

/**
 * Describes the capture coercions a configuration would need, without mutating it. Pure so both the startup path (which then applies them) and the save path
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

  // Only report a captureCodecs coercion when the normalized list actually differs from the input - identical contents must not trip the save's refusal.
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
 * FFmpeg mode. Used only by the startup path; a save refuses rather than coerces.
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

/** The settings whose value the server refuses to start with out of range. Each entry is a CONFIG_METADATA path, and the floor, the ceiling, and the name the
 * error reports all come from that path's metadata - the same metadata the settings form validates a save against. One declaration of a bound means a value
 * the form accepts is a value the boot accepts, and there is no second copy to fall out of step with the first.
 *
 * The list is narrower than the metadata's full set of bounded numeric settings, and deliberately so: these are the values the server cannot run with, while
 * the settings form validates the rest when they are saved. Widening boot-time validation to every bounded setting is a separate decision, because each
 * addition is a new way for a configuration that boots today to stop booting.
 *
 * hdhr.port is bounded and startup-validated too, but it is not here: its check is conditional on HDHomeRun emulation being enabled, so it lives inside that
 * guard in collectHardErrors and calls the same helper for its bounds.
 */
export const STARTUP_BOUNDED_SETTINGS: readonly string[] = [

  // Server configuration.
  "server.port",

  // Streaming bitrates.
  "streaming.videoBitsPerSecond",
  "streaming.audioBitsPerSecond",

  // Timeouts. The floor prevents premature failures and the ceiling prevents indefinite hangs.
  "streaming.navigationTimeout",
  "streaming.videoTimeout",

  // Concurrent stream limit.
  "streaming.maxConcurrentStreams",

  // Circuit breaker threshold.
  "recovery.circuitBreakerThreshold",

  // Browser relaunch governor bounds.
  "recovery.relaunchFailureThreshold",
  "recovery.relaunchFailureWindow",
  "recovery.relaunchHealthHold",

  // Stall threshold, the one float among these.
  "playback.stallThreshold",

  // Logging.
  "logging.maxSize",

  // HLS configuration.
  "hls.segmentDuration",
  "hls.maxSegments",
  "hls.idleTimeout"
];

/**
 * Validates one bounded setting against the floor and ceiling its CONFIG_METADATA entry declares, naming the error by that entry's environment variable so the
 * message points at the knob an operator would turn. The metadata type picks the validator: an integer or port setting must be a whole number, while a float
 * setting may be fractional.
 * @param config - The configuration to read the value from.
 * @param settingPath - The dot-separated CONFIG_METADATA path of the setting to validate.
 * @returns Error message if the value is invalid, null if it is within bounds.
 */
function validateBoundedSetting(config: Config, settingPath: string): Nullable<string> {

  const setting = getSettingByPath(settingPath);
  const errorName = setting?.envVar;

  /* A path that does not name a setting with an environment variable answers as a failure rather than passing silently, because the server cannot vouch for a
   * value whose bounds it was unable to find. The drift-guard test in config/index.test.ts asserts every path here resolves, so this reports a coding error
   * rather than an operator's.
   */
  if(!setting || !errorName) {

    return settingPath + " has no configuration metadata naming an environment variable, so its bounds cannot be validated.";
  }

  const value = getNestedValue(config, settingPath) as number;

  switch(setting.type) {

    case "float": {

      return validateNumber(errorName, value, setting.min, setting.max);
    }

    case "integer":
    case "port": {

      return validateInteger(errorName, value, setting.min, setting.max);
    }

    default: {

      return settingPath + " is a " + setting.type + " setting, which carries no numeric bounds to validate.";
    }
  }
}

/**
 * Collects every hard configuration error - an always-fatal value that no coercion can repair - for the given configuration. Pure: it never mutates and never
 * throws, so both validateConfiguration (which throws on a non-empty result at startup) and a save (which refuses the candidate) can reuse it.
 * @param config - The configuration to validate.
 * @returns The list of error messages; empty when the configuration has no hard errors.
 */
function collectHardErrors(config: Config): string[] {

  // Collect every hard error before returning so the operator sees all problems at once rather than fixing them one restart at a time. The check helper pushes
  // non-null validator results, reducing each validation to a single line.
  const errors: string[] = [];
  const check = (result: Nullable<string>): void => { if(result) { errors.push(result); } };

  // Bounded settings, each validated against the floor and ceiling its own metadata declares. The list's group comments record what each group is for.
  for(const settingPath of STARTUP_BOUNDED_SETTINGS) {

    check(validateBoundedSetting(config, settingPath));
  }

  // Validate path overrides. When set, both chromeDataDir and logFile must be absolute paths to prevent ambiguity.
  if((config.paths.chromeDataDir !== null) && !path.isAbsolute(config.paths.chromeDataDir)) {

    errors.push("paths.chromeDataDir must be an absolute path, but it is " + config.paths.chromeDataDir + ".");
  }

  if((config.paths.logFile !== null) && !path.isAbsolute(config.paths.logFile)) {

    errors.push("paths.logFile must be an absolute path, but it is " + config.paths.logFile + ".");
  }

  // Validate the HDHomeRun port only when HDHR is enabled and the effective capture mode is FFmpeg. At startup applyCoercions has already forced FFmpeg mode
  // before this runs, so the captureMode check is a defensive no-op here and the guard reduces to just "HDHR enabled". On a save, an un-coerced native-mode
  // candidate skips this specific port check, but collectCandidateRejection separately refuses it via the forceFfmpegMode reason before it could ever be
  // written with native mode active.
  if(config.hdhr.enabled && (config.streaming.captureMode === "ffmpeg")) {

    check(validateBoundedSetting(config, "hdhr.port"));

    // Reject if the HDHR port conflicts with the main server port (same host).
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
 * application lets a save reuse the same hard-error and capture-coercion checks without silently coercing a live save.
 * @throws If any configuration value is invalid. The error message lists all invalid values.
 */
export function validateConfiguration(): void {

  const coercions = collectCoercions(CONFIG);

  // Record whether a capture coercion was applied so persistCoercedConfig can write the corrected values back to disk and keep the on-disk state accurate.
  captureConfigCoercedAtStartup = hasCoercions(coercions);

  applyCoercions(CONFIG, coercions);

  // The coercions moved CONFIG away from the file it was built from, and persistCoercedConfig writes them back next, so the loaded snapshot follows CONFIG here
  // rather than reporting the coerced values as a gap a later save would have to close.
  recordLoadedConfiguration();

  const errors = collectHardErrors(CONFIG);

  // If any validation errors occurred, throw with the complete list so the operator can fix every issue at once.
  if(errors.length > 0) {

    throw new Error("Configuration validation failed:\n  " + errors.join("\n  "));
  }
}

/**
 * Persists the capture configuration that validateConfiguration coerced at startup back to disk so the on-disk state matches the live CONFIG. A no-op unless a
 * coercion actually occurred. Without this, a config file holding an unsupported capture value (native mode, or a captureCodecs list missing the h264 baseline)
 * stays divergent from the coerced live CONFIG forever, and the save's validation would then refuse every later save, because each candidate is built from that file.
 * filterDefaults strips any value equal to its default on write, so a config coerced back to the FFmpeg/h264 defaults leaves a clean file with no capture override.
 * Failures degrade gracefully: the live CONFIG is already coerced, so a write failure only means the divergence persists until the next successful save or boot.
 */
export async function persistCoercedConfig(io: ConfigStore = defaultConfigStore): Promise<void> {

  if(!captureConfigCoercedAtStartup) {

    return;
  }

  try {

    await io.mutateConfig((config) => {

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
 * Determines whether a candidate a save built must be refused rather than written. Returns the reason when the candidate holds an object or a list at a setting
 * that takes a single value, carries a hard error (an always-fatal value), or would require a capture coercion a save refuses to apply silently, and null when
 * the candidate may be written. Every collected reason is a complete sentence, and the reasons are joined with a single space, so the combined text reads as
 * consecutive sentences when it is surfaced verbatim to the operator in the save response.
 * @param config - The merged, normalized candidate a save is about to write.
 * @returns The joined refusal reason, or null when the candidate may be written.
 */
function collectCandidateRejection(config: Config): Nullable<string> {

  // The shape check runs alone: the checks after it read every setting as the type its metadata declares, so they have nothing meaningful to say about a
  // candidate that breaks that assumption.
  const shapeErrors = collectShapeErrors(config);

  if(shapeErrors.length > 0) {

    return shapeErrors.join(" ");
  }

  const reasons: string[] = collectHardErrors(config);
  const coercions = collectCoercions(config);

  if(coercions.forceFfmpegMode) {

    reasons.push("Native capture mode is disabled and cannot be saved; set capture mode to FFmpeg.");
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
 * Collects a reason for every setting whose value has the wrong shape: an object at any setting, or a list at a setting that takes a single value. A file
 * edited by hand can carry such a value, and the merge copies whatever the file holds at a setting's path. A diff walks into a plain object as if its keys were
 * settings, so a nested path would reach the class resolver, which classes only the paths the configuration defines, after the file had been written.
 * @param config - The candidate to inspect.
 * @returns One complete sentence per misshapen setting; empty when every setting holds a value of its declared shape.
 */
function collectShapeErrors(config: Config): string[] {

  const errors: string[] = [];

  for(const settings of Object.values(CONFIG_METADATA)) {

    for(const setting of settings) {

      const value = getNestedValue(config, setting.path);

      if((typeof value !== "object") || (value === null) || (Array.isArray(value) && (setting.type === "checkboxList"))) {

        continue;
      }

      errors.push(setting.path + " holds " + (Array.isArray(value) ? "a list" : "an object") + ", which this setting does not accept.");
    }
  }

  return errors;
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
 */
export function displayConfiguration(): void {

  const viewport = getPresetViewport(CONFIG);
  const presetStatus = CONFIG.streaming.qualityPreset + " (" + String(viewport.width) + "\u00d7" + String(viewport.height) + ")";

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
}
