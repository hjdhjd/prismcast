/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Configuration management for PrismCast.
 */
import type { ApplyResult, ChangeRejection, ConfigChange, ConfigChangePartition } from "./reactivity.ts";
import { CONFIG_METADATA, DEFAULTS, collectStoredCaptureCorrections, correctCaptureValues, correctStoredDeviceId, getNestedValue, getReactivityClass,
  getSettingByPath, mergeConfiguration, mutateConfigThen, readConfig, setNestedValue } from "./userConfig.ts";
import type { CaptureCorrection, DeviceIdCorrection, ProcessFieldPath, UserConfig } from "./userConfig.ts";
import type { Config, Nullable } from "../types/index.ts";
import { LOG, assertNever, canonicalizeDebugPattern, displayLine, formatError, getCurrentPattern, getPackageVersion, initDebugFilter, isAnyDebugEnabled,
  isPlainObject } from "../utils/index.ts";
import { applyConfigChanges, computeConfigDiff, partitionConfigChanges, registerConfigChangeHandler } from "./reactivity.ts";
import { getChromeDataDir, getConfigFilePath } from "./paths.ts";
import { getPresetViewport, getValidPresetIds } from "./presets.ts";
import { HTTP_LOG_LEVELS } from "../types/index.ts";
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
 * Clock port: a typed interface plus a module-const default, consumed through a defaulted parameter. Its one write member holds the store's queue through a
 * follow-up, so every write the configuration layer makes finishes what follows it before the next write reads the file.
 */
export interface ConfigStore {

  readonly mutateConfigThen: typeof mutateConfigThen;
  readonly readConfig: typeof readConfig;
}

const defaultConfigStore: ConfigStore = { mutateConfigThen, readConfig };

// The CONFIG object is the running configuration. It starts as a copy of DEFAULTS, is replaced by the merged configuration at startup, and from then on moves
// one leaf at a time, a realized leaf as saves reconcile it and a written leaf as process writes commit it, so a restart-class value a save holds out never
// reaches it. Only this module writes it.
export let CONFIG: Config = structuredClone(DEFAULTS);

/* The loaded snapshot: the configuration file as last loaded, merged and normalized exactly as the next boot would read it. It is recorded at the end of the
 * boot's configuration load and at the end of every completed reconcile, once that reconcile has committed what its handlers realized, and a landed process
 * write sets its own leaves on it once it has committed them. The running configuration and this snapshot differ exactly where the process does not yet
 * reflect the file - a restart-class value waiting for the restart, a live value a handler refused, or a leaf the process wrote ahead of the file, which only
 * a process write the file refused leaves - and every save reconciles that gap.
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

/**
 * Initializes the configuration by loading the user config file, merging with defaults, applying environment variable overrides, and applying CLI overrides. This
 * must be called at startup before any code accesses CONFIG. After initialization, the CONFIG object contains the final merged values. A file whose stored
 * capture values need a correction, or whose HDHomeRun DeviceID the boot corrected, is written once through the store, the one write storing either correction
 * or both, unless the store could not read the file or the configuration carries a hard error.
 * @param cliOverrides - Optional CLI flag overrides, applied at the highest priority level.
 * @param io - The config store the boot reads from and makes its one correcting write through, which stores a capture correction, a DeviceID correction, or both.
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
  // later save can re-apply a changed persisted filter live without overriding env/CLI. Measured here, ahead of normalizeConfig and applyPersistedDebugFilter, it
  // reflects env/CLI alone rather than the persisted filter applying to itself.
  envOrCliDebugOverride = isAnyDebugEnabled();

  // Build the running configuration exactly as every save builds its candidate, then apply its persisted debug filter to the runtime through
  // applyPersistedDebugFilter. The steps are kept separate because the build is pure: a save builds and validates a candidate without touching the runtime filter,
  // and applies a changed filter only through its registered handler, once the save's write has landed and its reconcile dispatches the change.
  const built = buildCandidate(result.config);

  CONFIG = built.config;
  applyPersistedDebugFilter(CONFIG.logging.debugFilter);

  // The boot has no running DeviceID to keep, so while the emulation is enabled a stored id that is missing or fails its checksum is replaced by a generated
  // one, set on CONFIG and on the file the boot read together.
  const deviceIdCorrection = correctStoredDeviceId({ candidate: CONFIG, file: result.config, running: "" });
  const corrections = (deviceIdCorrection === null) ? built.corrections : [ ...built.corrections, deviceIdCorrection ];

  // The boot announces every correction its configuration needed, the way a save announces its own once its write has landed.
  for(const correction of corrections) {

    logConfigurationCorrection(correction);
  }

  /* One write stores what the file itself needs and changes nothing else. The store corrects the stored capture values on every write, so a file whose own
   * capture values need a correction gets a write that lets the store's hook store and log it, and a DeviceID the boot corrected is set on the file that write
   * reads, so the one write stores either correction or both. A value the environment supplies asks for no write, because it is never stored. The write runs
   * only when the store read the file itself, because the boot has already said the store refuses writes on a file it could not read or parse, and only when
   * the configuration has no hard error, because the boot never writes a configuration it is about to refuse.
   */
  const fileNeedsWrite = (collectStoredCaptureCorrections(result.config).length > 0) || (deviceIdCorrection !== null);

  if(fileNeedsWrite && !result.readError && !result.parseError && (collectHardErrors(CONFIG).length === 0)) {

    try {

      await io.mutateConfigThen((current) => {

        // The store's write hook makes the capture correction, and the corrected DeviceID is set here, so the one write stores either correction or both.
        if(deviceIdCorrection !== null) {

          if(!isPlainObject(current.hdhr)) {

            current.hdhr = {};
          }

          current.hdhr.deviceId = CONFIG.hdhr.deviceId;
        }

        // Nothing follows the boot's write, so its follow-up resolves at once and releases the store's queue.
        return async (): Promise<void> => Promise.resolve();
      });
    } catch(error) {

      // The running configuration keeps a corrected DeviceID, and the next settings save writes it onto the file.
      LOG.warn("The corrected configuration could not be written, so the file keeps its stored values until the next write.", { error: formatError(error) });
    }
  }

  loadedConfig = structuredClone(CONFIG);

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
 * settings surface alone, the paths CONFIG_METADATA declares, because a leaf the process wrote and the loaded snapshot does not hold, which only a process
 * write the file refused leaves, is the process ahead of the file rather than a save the user is waiting on. Derived on every call from CONFIG and the loaded
 * snapshot, so it holds no state of its own. A reconcile publishes the snapshot only once it has committed what its handlers realized, so a caller that does
 * not wait on the store's queue reads CONFIG and the snapshot as the last completed reconcile left them, and a change a save introduces is never listed as
 * unrealized while its handlers are still running.
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
 * mutation inside the store the same way. Once the file is written, the save reconciles that candidate as the write's follow-up and answers with its result.
 *
 * One queue: the store's chain serializes each write's read-modify-write together with its follow-up, a save's reconcile or a process write's commit, which
 * runs while the store's queue is held, so no write reads the file while another write's reconcile or commit is running, and overlapping saves commit in the
 * order their writes landed. A config-change handler never writes the configuration, because the reconcile or the process write's commit it runs inside holds
 * the store's queue, and a write it awaited would wait on that queue and never settle.
 * @param mutator - Applies the caller's change to the current file in place. A throw inside it writes nothing.
 * @param io - The config store the save writes through.
 * @returns What the reconcile realized, the restart-class changes this save holds for a restart, and the changes of this save a handler refused.
 * @throws ConfigurationRejectedError when the candidate fails validation, and the store's own error when the file cannot be parsed, read, or written.
 */
export async function saveConfiguration(mutator: (current: UserConfig) => void, io: ConfigStore = defaultConfigStore): Promise<ApplyResult> {

  // The callback runs under the store's queue against the file the store just read, so the candidate is built from exactly the configuration this save writes,
  // and the follow-up it returns reconciles that candidate once the write has landed, before the store lets the next write read the file.
  return io.mutateConfigThen((current) => {

    mutator(current);

    // The candidate is built on a clone so normalization never reaches the file object the store is about to write.
    const built = buildCandidate(structuredClone(current));

    // The DeviceID is corrected once the build has settled the enabled flag and the stored id the correction reads, and before the validation, so the
    // candidate that is validated and reconciled carries the same id the correction sets on the file object the store writes.
    const deviceIdCorrection = correctStoredDeviceId({ candidate: built.config, file: current, running: CONFIG.hdhr.deviceId });
    const candidate = (deviceIdCorrection === null) ? built : { config: built.config, corrections: [ ...built.corrections, deviceIdCorrection ] };
    const rejection = collectCandidateRejection(candidate.config);

    if(rejection !== null) {

      throw new ConfigurationRejectedError(rejection);
    }

    return async (): Promise<ApplyResult> => {

      // The write has landed, so the corrections the candidate needed are announced here, and a save the validation or the store refused announces none of them.
      for(const correction of candidate.corrections) {

        logConfigurationCorrection(correction);
      }

      // The store read and parsed the file to run the callback, so a parse failure the boot recorded does not describe the file this save wrote.
      configParseError = false;
      configParseErrorMessage = undefined;

      return reconcileConfiguration(candidate.config);
    };
  });
}

/**
 * The leaves a process write answers, keyed by the dot path of a field the process owns, so a path PROCESS_FIELDS does not declare as a state field is a
 * compile error.
 */
export type ProcessFieldValues = Readonly<Partial<Record<ProcessFieldPath, unknown>>>;

/**
 * The operation for every field the process owns, the leaves the settings form never writes: the setup flag, the disabled predefined channels, the display
 * preferences, the service list, and the discovered DVR host. Inside the store's own mutation it answers the leaves from the file the store just read and sets
 * each on that file, and once the write has landed its follow-up commits exactly those leaves: their handlers are dispatched whatever CONFIG held before, and
 * each leaf is set on CONFIG and then on the loaded snapshot. It builds no candidate and validates nothing beyond the store's own write normalization, because
 * the process already holds the values it writes, and every other difference between CONFIG and the file, a hand edit or a refused setting among them, stays
 * for the next settings save to reconcile and report.
 *
 * When the store refuses the write - it could not read or parse the file, or the callback, the write, or its readback failed - the leaves are answered again
 * from the running configuration and committed to CONFIG alone with one warning naming them, and the operation resolves, because the process owns the value
 * and the file is only its record. A readback that fails is such a refusal though the file may hold the leaves, and the loaded snapshot takes them at the
 * next settings save that lands. A rejection that arrives once the follow-up has begun is rethrown instead, because that write has landed: committing and
 * dispatching the leaves a second time would log a refusal the file never made.
 *
 * A caller's fields answers from its argument alone and changes nothing it reaches, because it may run twice, once on the file the store read and once on the
 * clone of CONFIG the refused path hands it, and Readonly holds only its argument's top level. A handler for a process field never writes the configuration,
 * because the commit it runs inside holds the store's queue; a handler that refuses a process leaf leaves it committed regardless.
 * @param fields - Answers the leaves to write, keyed by path, from the stored configuration it is handed.
 * @param io - The config store the write goes through.
 * @throws Whatever a follow-up that has begun rejects with, such as the class resolver's error for a leaf no setting or state entry classifies.
 */
export async function writeProcessFields(fields: (stored: Readonly<UserConfig>) => ProcessFieldValues, io: ConfigStore = defaultConfigStore): Promise<void> {

  // Set by the follow-up's first statement and local to this call, so the catch below tells a write the store refused, which committed nothing, from a landed
  // write whose commit rejected. The flag is a field rather than a bare local because a closure sets it, which the compiler's narrowing of a local cannot see.
  const followUp = { begun: false };

  try {

    await io.mutateConfigThen((current) => {

      const values = fields(current);

      for(const [ leafPath, value ] of Object.entries(values)) {

        setFileLeaf(current, leafPath, structuredClone(value));
      }

      return async (): Promise<void> => {

        followUp.begun = true;

        // The store read and parsed the file to run the callback, so a parse failure the boot recorded does not describe the file this write landed on.
        configParseError = false;
        configParseErrorMessage = undefined;

        await commitProcessFields(values);

        // The loaded snapshot takes the leaves once they are committed, the order a landed save keeps, so a commit that rejects leaves it as it was.
        for(const [ leafPath, value ] of Object.entries(values)) {

          setNestedValue(loadedConfig as unknown as Record<string, unknown>, leafPath, structuredClone(value));
        }
      };
    });
  } catch(error) {

    if(followUp.begun) {

      throw error;
    }

    const values = fields(structuredClone(CONFIG));

    await commitProcessFields(values);

    LOG.warn("The configuration file refused a process write, so the new value applies to the running configuration alone.",
      { error: formatError(error), paths: Object.keys(values) });
  }
}

/**
 * Commits the leaves a process write answered, the step its landed and refused paths share: each leaf becomes a change against CONFIG, the changes are partitioned by
 * class and dispatched to their handlers with a clone of CONFIG carrying every leaf, whatever CONFIG held before, so a handler re-derives its state from the
 * value written, and then each leaf is set on CONFIG. A handler's refusal is logged and commits the leaf regardless, because the process owns it.
 * @param values - The leaves the write answered, keyed by path.
 */
async function commitProcessFields(values: ProcessFieldValues): Promise<void> {

  const changes = Object.entries(values).map(([ leafPath, current ]) => ({ current, path: leafPath, previous: getNestedValue(CONFIG, leafPath) }));
  const partition = partitionConfigChanges(changes, getReactivityClass);
  const next = structuredClone(CONFIG);

  for(const change of changes) {

    setNestedValue(next as unknown as Record<string, unknown>, change.path, structuredClone(change.current));
  }

  const dispatch = await applyConfigChanges(partition, next);

  for(const refusal of dispatch.rejected) {

    LOG.warn("A configuration handler refused a value the process owns, so the running configuration keeps it regardless.",
      { path: refusal.change.path, reason: refusal.reason });
  }

  // Each value is cloned so CONFIG never shares an array with the loaded snapshot or with the file the store wrote.
  for(const change of changes) {

    setNestedValue(CONFIG as unknown as Record<string, unknown>, change.path, structuredClone(change.current));
  }
}

/**
 * Sets one leaf on a configuration file the store is about to write, first replacing each intermediate on the leaf's path that is not a plain object with an
 * empty one, so a hand-edited value standing where a category belongs takes the write rather than refusing every later one.
 * @param file - The configuration file.
 * @param leafPath - The dot-separated path of the leaf.
 * @param value - The value to set.
 */
function setFileLeaf(file: UserConfig, leafPath: string, value: unknown): void {

  const root = file as unknown as Record<string, unknown>;
  let node = root;

  for(const segment of leafPath.split(".").slice(0, -1)) {

    const child = node[segment];

    if(!isPlainObject(child)) {

      node[segment] = {};
    }

    node = node[segment] as Record<string, unknown>;
  }

  setNestedValue(root, leafPath, value);
}

/**
 * Reconciles the running configuration against a candidate a save has just written. The gap between CONFIG and the candidate is partitioned by class: its
 * restart-class changes are held out of CONFIG, and its live and next-stream changes go to their handlers together with the candidate running configuration -
 * CONFIG with those changes applied - so a handler realizes the state it is handed. Only the changes no handler refused are committed, one path at a time, so
 * a refused change and a held restart-class change stay out of CONFIG while the realized changes beside them in the same category are committed, and nothing
 * is rolled back: a refused change stays in the gap, and every later save retries it.
 *
 * The candidate becomes the loaded snapshot only once the realized changes are committed. A reader that does not wait on the store's queue - the settings
 * form, the gap accessor - therefore sees CONFIG and the loaded snapshot only as a completed reconcile left them, and a change this save introduces never reads
 * as unrealized while its handlers are still running. A reconcile that throws before that point leaves the snapshot where it was: the next save's gap retries
 * whatever this one did not commit, and its delta, read against that earlier snapshot, reports this save's changes again.
 *
 * The result reports everything the reconcile realized. Its deferred and rejected lists answer for this save alone, through the delta between the previous
 * loaded snapshot and this one: a restart-class change is deferred when this save introduced it and it still differs from the running value, so a save that
 * writes the running value back schedules no restart, and a refusal is reported when this save asked for the refused change. It runs only as the follow-up
 * of a save's write, while the store's queue is held.
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

  // Commit exactly what the handlers realized, leaf by leaf, rather than assigning whole categories from the candidate: a change a handler refused and a
  // restart-class change the save holds stay out of CONFIG, beside realized changes in the same category. Each value is cloned so CONFIG never shares an array
  // with the loaded snapshot.
  for(const change of dispatch.realized) {

    setNestedValue(CONFIG as unknown as Record<string, unknown>, change.path, structuredClone(change.current));
  }

  // The snapshot moves here, after what the handlers realized is committed, so CONFIG and the loaded snapshot change together for every reader that does not
  // wait on the store's queue.
  loadedConfig = candidate;

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
 * One correction a configuration needs before the server runs with it, tagged by the correction it makes. The capture corrections are the ones
 * correctCaptureValues() answers, the DeviceID correction is the one correctStoredDeviceId() answers, and each other member carries the values its warning
 * names.
 *
 * - frameRate: a frame rate outside the range its metadata declares is clamped to the nearer bound.
 * - httpLogLevel: an HTTP log level the request logger does not know becomes the level that logs every request.
 * - qualityPreset: a quality preset the server does not know becomes the default preset.
 */
type ConfigurationCorrection = CaptureCorrection | DeviceIdCorrection |
  { readonly applied: number; readonly configured: number; readonly kind: "frameRate"; readonly max: number; readonly min: number } |
  { readonly configured: string; readonly kind: "httpLogLevel"; readonly using: "all" } |
  { readonly configured: string; readonly kind: "qualityPreset"; readonly using: string };

/**
 * What buildCandidate() answers: the merged, normalized configuration and the corrections its normalization made, in the order it made them.
 */
interface BuiltCandidate {

  readonly config: Config;
  readonly corrections: readonly ConfigurationCorrection[];
}

/**
 * Builds a configuration from a user configuration file exactly as the boot does: defaults, the file, the environment, and the stashed CLI overrides merged in
 * priority order, then normalized. The boot and every save share it, so a saved candidate is the configuration the next boot would read.
 * @param userConfig - The user configuration file. A save passes a clone of the file it is writing, so nothing the build does reaches that object.
 * @returns The merged, normalized configuration as config, and the corrections its normalization made as corrections, which the caller announces.
 */
function buildCandidate(userConfig: UserConfig): BuiltCandidate {

  const config = mergeConfiguration(userConfig, stashedCliOverrides);
  const corrections = normalizeConfig(config);

  return { config, corrections };
}

/**
 * Normalizes a configuration in place WITHOUT any global side effects: clamps an out-of-vocabulary quality preset to the default, clamps an out-of-range frame
 * rate to the nearer bound its metadata declares, corrects an unavailable capture mode or codec list to values the server can capture with, corrects an
 * unrecognized HTTP log level to the level that logs every request, and rewrites the persisted debug-filter string to its canonical form. It logs nothing and
 * answers each correction it made, in order, because a configuration's corrections are announced by the boot and by a save whose write has landed, never while
 * a candidate a save may still refuse is built. Pure with respect to process state - it touches only the passed config - so it is safe to run on a candidate a
 * save may still refuse. Every configuration is built through buildCandidate, so the boot and the save cannot drift. The live runtime debug filter is applied
 * separately, by the boot and by the registered debug-filter handler a landed save dispatches, so a refused save never changes the running filter.
 * @param config - The freshly merged configuration to normalize in place.
 * @returns The corrections made, in the order they were made; empty when the configuration needed none.
 */
function normalizeConfig(config: Config): readonly ConfigurationCorrection[] {

  const corrections: ConfigurationCorrection[] = [];

  // Canonicalize the persisted debug-filter string (trim whitespace around commas, collapse duplicates) unless a higher-priority env/CLI source owns the filter.
  // This keeps the committed CONFIG and the computed diff working with canonical values and avoids a phantom whitespace-only diff; it does NOT touch the runtime
  // filter - applyPersistedDebugFilter performs that side effect, at the boot and from the handler a landed save dispatches.
  if(!envOrCliDebugOverride) {

    config.logging.debugFilter = canonicalizeDebugPattern(config.logging.debugFilter);
  }

  // Hold the HTTP log level to the levels the request logger knows. The logger decides each request through an exhaustive switch over the level, so a level
  // outside them, from the environment or a hand-edited file, is corrected here to the level that logs every request rather than reaching that switch.
  if(!HTTP_LOG_LEVELS.includes(config.logging.httpLogLevel)) {

    corrections.push({ configured: config.logging.httpLogLevel, kind: "httpLogLevel", using: "all" });

    config.logging.httpLogLevel = "all";
  }

  // Validate quality preset. Viewport is derived on-demand via getPresetViewport() rather than stored in CONFIG.
  const validPresets = getValidPresetIds();

  if(!validPresets.includes(config.streaming.qualityPreset)) {

    corrections.push({ configured: config.streaming.qualityPreset, kind: "qualityPreset", using: DEFAULTS.streaming.qualityPreset });

    config.streaming.qualityPreset = DEFAULTS.streaming.qualityPreset;
  }

  // Hold the frame rate inside the range its metadata declares, the same bounds the settings form validates a save against. The capture constraint holds the
  // track to this rate on both bounds, so a rate outside the range is clamped to the nearer bound with a warning rather than handed to tab capture or refused at
  // startup. A bound the metadata leaves undeclared constrains nothing.
  const { max = Infinity, min = -Infinity } = getSettingByPath("streaming.frameRate") ?? {};
  const clampedFrameRate = Math.min(Math.max(config.streaming.frameRate, min), max);

  if(clampedFrameRate !== config.streaming.frameRate) {

    corrections.push({ applied: clampedFrameRate, configured: config.streaming.frameRate, kind: "frameRate", max, min });

    config.streaming.frameRate = clampedFrameRate;
  }

  /* Correct the capture values to ones the server can capture with. Every candidate passes here, the boot's and every save's, so a capture value from the file or
   * the environment is corrected by construction. The environment is re-applied to every candidate and never written, so a value it supplies warns at the boot
   * and at every landed save, while the file's own values are corrected once, by the store's write hook.
   */
  const capture = correctCaptureValues(config.streaming);

  corrections.push(...capture.corrections);
  config.streaming = { ...config.streaming, ...capture.values };

  return corrections;
}

/**
 * Logs one correction a configuration needed, as one line carrying the values it names: a warning for each correction, and an info line for a DeviceID
 * generated where none was stored. A configuration's corrections are announced by the boot and by a save whose write has landed, never while a candidate a save
 * may still refuse is built, so a save the validation or the store refuses announces none of them.
 * @param correction - The correction to announce.
 */
function logConfigurationCorrection(correction: ConfigurationCorrection): void {

  switch(correction.kind) {

    case "baseline": {

      LOG.warn("The configured capture codecs omit the H.264 baseline, so it is restored.", { configured: correction.configured, using: correction.using });

      break;
    }

    case "deviceId": {

      // Every id is upper-cased, as the HDHomeRun surface prints it. A stored id that was empty and took the running one changes nothing an operator sees, so
      // it is not announced.
      if(correction.configured !== "") {

        LOG.warn("The configured HDHomeRun DeviceID fails its checksum, so a valid one takes its place.",
          { configured: correction.configured.toUpperCase(), using: correction.using.toUpperCase() });
      } else if(correction.generated) {

        LOG.info("An HDHomeRun DeviceID was generated.", { deviceId: correction.using.toUpperCase() });
      }

      break;
    }

    case "frameRate": {

      LOG.warn("The configured frame rate is outside the supported range, so the nearer bound is in use.",
        { applied: correction.applied, configured: correction.configured, max: correction.max, min: correction.min });

      break;
    }

    case "httpLogLevel": {

      LOG.warn("The configured HTTP log level is not one the server recognizes, so every request is logged.",
        { configured: correction.configured, using: correction.using });

      break;
    }

    case "mode": {

      LOG.warn("Native capture mode is unavailable because of a Chrome fMP4 MediaRecorder defect, so FFmpeg capture is in use.",
        { configured: correction.configured, using: correction.using });

      break;
    }

    case "qualityPreset": {

      LOG.warn("The configured quality preset is not one the server recognizes, so the default preset is in use.",
        { configured: correction.configured, using: correction.using });

      break;
    }

    case "unrecognizedCodecs": {

      LOG.warn("The configured capture codecs include identifiers the server does not recognize, so they are ignored.",
        { configured: correction.configured, ignored: correction.ignored, using: correction.using });

      break;
    }

    default: {

      assertNever(correction);
    }
  }
}

/**
 * Applies a persisted debug filter to the live runtime filter, when no higher-priority env/CLI source owns it and the filter differs from what is currently
 * active. This is the global side effect split out of normalizeConfig. The boot applies the filter it read, and a save applies one only through
 * applyDebugFilterChange, its registered handler, which the reconcile dispatches only when the save's gap holds the filter. A save that leaves the filter alone
 * therefore leaves the runtime filter as it stands, which matters because the debug page applies its filter to the runtime before its save is queued: a save
 * queued ahead of that one, or the page's own save refused, must not put the persisted filter back over it. A changed filter delivered through /config/import
 * takes effect live the same way, and an emptied persisted filter clears the runtime filter (initDebugFilter("") disables it).
 * @param filter - The persisted filter, canonical as the build left it.
 */
function applyPersistedDebugFilter(filter: string): void {

  if(!envOrCliDebugOverride && (filter !== getCurrentPattern())) {

    initDebugFilter(filter);
  }
}

/**
 * Realizes a change to the persisted debug filter: the candidate's filter becomes the runtime filter unless an env or CLI source owns it. Applying a filter
 * cannot fail, so the handler refuses nothing. It is exported, as every module-registered handler is, so a suite that resets the registry re-registers it.
 * @param _changes - The change under the handler's path; the candidate carries the filter, so the handler reads it there instead.
 * @param next - The candidate running configuration.
 * @returns No rejections.
 */
export async function applyDebugFilterChange(_changes: readonly ConfigChange[], next: Readonly<Config>): Promise<readonly ChangeRejection[]> {

  applyPersistedDebugFilter(next.logging.debugFilter);

  return [];
}

// Module-load side effect: register the handler once per process, as every config-change handler registers, so it is in place before the first save can reach
// the reconcile.
registerConfigChangeHandler("logging.debugFilter", applyDebugFilterChange);

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

  // Timer intervals. The floor keeps a timer from polling too tightly, and the ceiling keeps a stall or a stale page from going unattended longer than recovery
  // is designed to wait.
  "playback.monitorInterval",
  "recovery.stalePageCleanupInterval",

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
 * Collects every hard configuration error - an always-fatal value the server refuses to run with rather than correcting - for the given configuration. Pure: it
 * never mutates and never throws, so validateConfiguration (which throws on a non-empty result at startup), a save (which refuses the candidate), and the
 * boot's correcting write (which never stores a configuration the boot will refuse) all read the same answer.
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

  // The HDHomeRun port is checked whenever HDHomeRun emulation is on, and only then, so a port setting that nothing binds never refuses a boot or a save.
  if(config.hdhr.enabled) {

    check(validateBoundedSetting(config, "hdhr.port"));

    // Reject if the HDHR port conflicts with the main server port (same host).
    if((config.hdhr.port === config.server.port) && ((config.server.host === "0.0.0.0") || (config.server.host === "::"))) {

      errors.push("HDHR_PORT (" + String(config.hdhr.port) + ") conflicts with the main server port.");
    }
  }

  return errors;
}

/**
 * Validates the running configuration and throws if it carries any hard error. It runs at startup once the configuration is initialized, collects every hard
 * error, and throws once with the complete list, so the operator can fix every issue in one pass. A correctable value never reaches here uncorrected, because
 * the configuration is corrected as it is built.
 * @throws If any configuration value is invalid. The error message lists all invalid values.
 */
export function validateConfiguration(): void {

  const errors = collectHardErrors(CONFIG);

  // If any validation errors occurred, throw with the complete list so the operator can fix every issue at once.
  if(errors.length > 0) {

    throw new Error("Configuration validation failed:\n  " + errors.join("\n  "));
  }
}

/**
 * Determines whether a candidate a save built must be refused rather than written. Returns the reason when the candidate holds an object or a list at a setting
 * that takes a single value or carries a hard error (an always-fatal value), and null when the candidate may be written. A capture value the server cannot
 * capture with is no reason to refuse, because the candidate was corrected as it was built and the store corrects the file it writes. Every collected reason
 * is a complete sentence, and the reasons are joined with a single space, so the combined text reads as consecutive sentences when it is surfaced verbatim to
 * the operator in the save response.
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

  const reasons = collectHardErrors(config);

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
