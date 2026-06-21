/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.ts: Transactional file store framework for PrismCast configuration persistence.
 *
 * This module is the single source of truth for every persistence concern in PrismCast:
 *
 *   - Atomic writes (temp file + rename)
 *   - Serialized mutations (in-process queue)
 *   - Auto-recovery from .bak when the main file fails to parse
 *   - Versioned snapshots at release boundaries (subdirectory + bounded retention)
 *   - Declarative schema migrations with an audit trail
 *   - Pluggable pre-write integrity validators
 *   - Centralized release-boot snapshot coordination across all registered stores
 *
 * Each persisted file (channels, config, profiles, health) declares its data shape, default value, parser, current schema version, ordered migration list, and
 * (optionally) an integrity validator. The framework wires the rest. Adding a new store is a one-line registration in createFileStore; adding a new migration
 * is a one-line entry in the store's migration map.
 *
 * The framework is backend-agnostic by construction: every operation it performs (atomic temp+rename, backup copy, snapshot copy, post-write readback) is
 * expressible against any durable store that offers stat/read/write/copy/rename/unlink with a path namespace. The abstraction surface is StorageBackend below;
 * the default fs-backed adapter lives in persistence.context.ts. Production stores get the default backend implicitly via the createFileStore default parameter.
 */
import { LOG, stringifySorted } from "../utils/index.ts";
import { createDefaultStorageBackend } from "./persistence.context.ts";
import { getDataDir } from "./paths.ts";
import path from "node:path";

// Maximum number of snapshots retained per file. Snapshots are pruned by mtime - the SNAPSHOT_RETENTION most recently created are kept and the rest are
// deleted. The retention window balances forensic value (older snapshots become schema-incompatible with current code) against disk and directory clutter.
const SNAPSHOT_RETENTION = 5;

/**
 * Storage backend abstraction. The framework operates on any durable store that exposes the operations below over a path namespace - the default backend lives
 * in persistence.context.ts and wires real-filesystem I/O via node:fs/promises, but the surface is intentionally narrow so alternative backends (for example, an
 * in-memory backend used during tests, or a future object-store backend) can plug in without modifying the framework.
 *
 * Error contract: when a path is missing, throw a NodeJS.ErrnoException with `code: "ENOENT"` so the framework's existing branch detection (the read() ENOENT
 * fast path, the doMutate ENOENT-tolerant backup copy, the snapshot ENOENT-tolerant source) continues to behave identically. All other failures (permission
 * denied, I/O error, etc.) propagate as Error or NodeJS.ErrnoException with their native code so the framework's error logging surfaces accurate diagnostics.
 *
 * Encoding: every text operation is UTF-8 by contract. Backends do not accept other encodings - JSON is what the framework writes, and JSON is text.
 */
export interface StorageBackend {

  // Throws when the path does not exist. Used by snapshot() to detect idempotent no-ops.
  readonly access: (path: string) => Promise<void>;

  // Copies source to destination. Overwrites destination when it exists. ENOENT thrown when source is missing.
  readonly copyFile: (source: string, destination: string) => Promise<void>;

  // Creates a directory at the given path, recursively creating parent directories. Idempotent on existing directories. The framework only ever calls this with
  // recursive semantics, so the surface bakes recursive in rather than exposing an options bag.
  readonly mkdir: (path: string) => Promise<void>;

  // Lists the names (basename, not full path) of every entry in the given directory. ENOENT when the directory does not exist.
  readonly readdir: (path: string) => Promise<string[]>;

  // Returns the file's content as a UTF-8 string. ENOENT when missing.
  readonly readFile: (path: string) => Promise<string>;

  // Atomically renames source to destination. The framework relies on filesystem-level atomicity (POSIX rename, NTFS MoveFileEx) - alternative backends must
  // provide an equivalent guarantee or risk partial-write windows the framework's integrity check will catch but cannot prevent.
  readonly rename: (source: string, destination: string) => Promise<void>;

  // Returns the file's modification time in millisecond resolution. The framework only uses mtimeMs (for snapshot pruning by recency) so the surface is narrow.
  readonly stat: (path: string) => Promise<{ mtimeMs: number }>;

  // Removes the file at the given path. ENOENT when missing.
  readonly unlink: (path: string) => Promise<void>;

  // Writes UTF-8 content to the file, replacing any prior content. Atomicity is achieved by writing to a .tmp companion and then renaming - the framework owns
  // the choreography rather than relying on writeFile to be atomic.
  readonly writeFile: (path: string, content: string) => Promise<void>;
}

// Shared default backend. Stateless wrappers around node:fs/promises - safe to share across every store in the process. Tests pass options.backend explicitly to
// substitute alternative backends (e.g., an in-memory backend with failure injection); production paths use this default implicitly.
const defaultStorageBackend = createDefaultStorageBackend();

// Types.

/**
 * Error thrown by `mutate()` when the backing file contains invalid JSON and recovery from .bak also failed. Route handlers can catch this specifically to
 * return 400 instead of 500. Background code paths that swallow errors via try/catch will handle it like any other Error.
 */
export class FileStoreParseError extends Error {

  override name = "FileStoreParseError";

  constructor(label: string, filePath: string, parseMessage: string) {

    super("Cannot modify " + label + " (" + filePath + "): file contains invalid JSON. " + parseMessage);
  }
}

/**
 * A single declarative schema migration. Migrations are keyed by their target schema version inside FileStoreOptions.migrations - the framework runs them in
 * order from the file's current version up to the store's currentSchemaVersion, mutating the in-memory data in place and stamping the new version after each
 * application.
 */
export interface Migration<T> {

  // Human-readable description; logged when applied and recorded in the migration audit trail on the file.
  description: string;

  // Mutates the data in place to upgrade it to this migration's target schema version. Must be idempotent across boots when paired with the version stamp -
  // once the target version is recorded, the framework will not invoke this migration again.
  apply: (data: T) => void;
}

/**
 * The result of running migrations during a read operation.
 */
export interface MigrationResult {

  // Descriptions of every migration applied during this run, in order. Empty when the file was already at the current version.
  applied: string[];

  // The schema version the file was at before migrations ran.
  fromVersion: number;

  // The schema version the file is at after migrations ran (equal to fromVersion when nothing was applied).
  toVersion: number;
}

/**
 * A single integrity issue detected by a store's validator. Surfaced via logging; the write proceeds regardless of severity.
 */
export interface ValidationIssue {

  // Stable category identifier for log grouping and future filtering. Per-store conventions; e.g., "identity-field-loss" for channels.
  category: string;

  // Human-readable description of what went wrong.
  description: string;

  // "warning" issues are logged but do not block the write. "error" issues are logged at error level but also do not block the write - the integrity check
  // is non-blocking by design.
  severity: "warning" | "error";
}

/**
 * Result from a read-only file store access. Callers that only need data (export endpoints, API responses) use this without risking a save-back.
 */
export interface FileStoreReadResult<T> {

  data: T;

  // Outcome of the migration runner during this read. Always present; empty applied[] means the file was already at currentSchemaVersion (or the store has no
  // migrations declared).
  migrationResult: MigrationResult;

  parseError: boolean;
  parseErrorMessage?: string;

  // True when the main file failed to parse and a usable copy was successfully recovered from the .bak rotation. Callers can surface this in the UI as a
  // recovery banner; the persistence layer also logs it loudly.
  recoveredFromBackup: boolean;
}

/**
 * Options for creating a file store instance. The four required fields (defaultValue, label, parse, path) are the minimum surface required for any store. The
 * remaining fields opt in to schema versioning, migrations, and integrity validation - any store can adopt them as its data shape grows.
 * @template T - The in-memory data type that callers mutate.
 */
export interface FileStoreOptions<T> {

  // Storage backend abstraction. When omitted, the framework uses the shared default backend that wires node:fs/promises - the production wiring. Alternative
  // backends (in-memory, fault-injecting, future object-store) can be supplied per-store for tests or specialized stores without changing the framework code.
  backend?: StorageBackend;

  /* Transform applied before serialization. Returns the serializable form, which may differ from T (e.g., channels inject metadata keys not in StoredChannelMap).
   * If omitted, the data is serialized as-is.
   */
  beforeWrite?: (data: T) => unknown;

  // Target schema version for new files and the upper bound the migration runner upgrades to. When omitted (or 1 with no migrations), the store is unversioned.
  currentSchemaVersion?: number;

  // Factory producing an empty default value for T. Used when the file does not exist (first run) or cannot be read. Explicit factory avoids overloading the
  // parse function with a hidden dual-purpose contract.
  defaultValue: () => T;

  // Reads the file's current schema version from the parsed data. Required when migrations is provided. Returning a value greater than currentSchemaVersion
  // indicates the file was written by a newer version of PrismCast; the framework logs and proceeds without applying any migrations (forward-compatible read).
  getSchemaVersion?: (data: T) => number;

  // Human-readable label for log messages and error text (e.g., "configuration", "channels").
  label: string;

  // Ordered schema migrations keyed by target schema version. The runner applies migrations whose target is greater than the file's current version, in
  // ascending order, until the file reaches currentSchemaVersion. Each migration must be paired with an entry in setSchemaVersion's contract.
  migrations?: Record<number, Migration<T>>;

  // Parse raw file content into the in-memory type. Called by read() after a successful file read. Should be permissive about reading older shapes - migrations
  // handle semantic transformations.
  parse: (raw: string) => T;

  // Deferred path resolver. Called on each read/write to support late initialization of the data directory.
  path: () => string;

  // Optional callback to record a successful migration in an audit trail field on the data. When provided, the framework calls this after each migration
  // applies. Typically appends to a `migrationsApplied: string[]` field on the persisted shape so operators can see which migrations have run.
  recordMigration?: (data: T, description: string) => void;

  // Writes the file's schema version into the data. Required when migrations is provided. Called after each migration applies and after the runner finishes
  // upgrading.
  setSchemaVersion?: (data: T, version: number) => void;

  // Pre-write integrity validator. Receives the parsed pre-mutation state (deep-cloned snapshot) and the post-mutation, post-beforeWrite state. Returns issues
  // for the framework to surface. Per-store contracts: each store knows its own identity fields, foreign-key-style references, and consistency rules.
  validate?: (prev: T, next: T) => ValidationIssue[];
}

/**
 * A transactional file store that provides atomic writes, serialized mutations, corruption recovery, snapshots, declarative migrations, and integrity
 * validation. Callers interact with data through `mutate()` (serialized read-modify-write) and `read()` (read-only access). Direct file I/O is never exposed.
 * @template T - The in-memory data type that callers mutate.
 */
export interface FileStore<T> {

  /**
   * Verifies migrations are up-to-date and persists the upgrade if any were applied. Idempotent - when the file is already at the current schema version this
   * is a single read with no write. Called once per store at startup by the release boot coordinator after snapshots have been captured.
   * @returns The migration result so callers can log per-store outcomes.
   */
  ensureMigrated(): Promise<MigrationResult>;

  /**
   * Serialized read-modify-write operation. The mutation function receives the current data (already migrated to the latest schema version) and modifies it in
   * place. The store handles atomicity, serialization, corruption guard, backup, validation, and beforeWrite transforms.
   * @param fn - Mutation function. Receives current data. Modify in place; return value is ignored.
   * @throws FileStoreParseError if the file contains invalid JSON and no usable backup is available (corruption guard).
   */
  mutate(fn: (current: T) => void): Promise<void>;

  /**
   * Read-only access to the current file contents. Returns the parsed data with parse status, recovery status, and migration result. Does not acquire the
   * serialization lock - safe to call concurrently. Migrations run in-memory but are not persisted by read() alone; callers who need to persist an upgrade
   * should use ensureMigrated() (or do so implicitly via mutate()).
   */
  read(): Promise<FileStoreReadResult<T>>;

  /**
   * Creates a labeled snapshot copy of the current file inside a `snapshots/` subdirectory next to the source file (named `<file>.<label>`). Idempotent on
   * the label - if a snapshot with the same label already exists, this is a no-op. After a successful create, prunes older snapshots for the same file so at
   * most SNAPSHOT_RETENTION remain (by mtime). Used to preserve a guaranteed restore point at release boundaries before any migrations can mutate the file.
   * @param label - The filename suffix appended after the source file's basename and a dot. Typically a version string like `pre-v1.10.0`.
   */
  snapshot(label: string): Promise<void>;
}

// Store registry: every createFileStore call appends to this list so the release boot coordinator can iterate every persistence-managed file uniformly. Stores
// must be created at module load time (not lazily) so the registry is fully populated by the time snapshotAllForRelease runs.
const registeredStores: FileStore<unknown>[] = [];

/**
 * Snapshots every registered store under the given label. Used at startup to capture a guaranteed restore point at release boundaries before any migrations
 * can run. The label is typically `pre-v<package-version>` so each release boot leaves a snapshot named for the version that was running.
 *
 * Snapshots run in parallel since they are independent file copies. Failures within individual snapshots are logged by the snapshot helper itself and do not
 * propagate; the coordinator always returns successfully so a single store's snapshot failure does not block startup.
 * @param label - The snapshot label suffix.
 */
export async function snapshotAllForRelease(label: string): Promise<void> {

  await Promise.all(registeredStores.map(async (store) => store.snapshot(label)));
}

/**
 * Runs ensureMigrated() on every registered store. Called once at startup after snapshotAllForRelease so that any pending migrations execute against a file
 * that already has its pre-version snapshot captured. Idempotent within a release - subsequent boots see no pending migrations and skip the upgrade write.
 */
export async function ensureAllMigrated(): Promise<void> {

  await Promise.all(registeredStores.map(async (store) => store.ensureMigrated()));
}

// Store factory.

/**
 * Creates a transactional file store instance. Each instance owns a single JSON file and provides serialized, atomic access to it.
 *
 * Safety guarantees:
 * - **Atomic writes:** data is written to a `.tmp` file and renamed over the original. `rename()` is atomic on POSIX and NTFS.
 * - **Serialization:** a promise chain ensures only one `mutate()` runs at a time. Concurrent callers queue behind the active operation.
 * - **Corruption guard:** `mutate()` throws `FileStoreParseError` if both the main file and its `.bak` are unparseable, preventing save-over-corrupt cascades.
 * - **Backup rotation:** before each write, the current file is copied to `.bak`. One-deep rotation provides a recovery path for the previous good version.
 * - **Auto-recovery:** when the main file fails to parse, `read()` transparently restores from `.bak` (atomic temp+rename) and surfaces `recoveredFromBackup`
 *   on the result so callers can banner the event. Only when both files are unparseable does the result fall back to defaults with `parseError: true`.
 * - **Versioned snapshots:** `snapshot(label)` writes a copy of the current file into a `snapshots/` subdirectory, named `<file>.<label>`, idempotent on the
 *   label. After each successful create the directory is pruned to at most SNAPSHOT_RETENTION entries per file (by mtime).
 * - **Declarative migrations:** when `migrations` and `currentSchemaVersion` are provided, `read()` runs any pending migrations in memory and returns the
 *   upgraded data. `ensureMigrated()` persists the upgrade if any were applied. Migrations are version-keyed and idempotent across boots.
 * - **Pre-write validation:** when `validate` is provided, `mutate()` invokes it with the pre-mutation snapshot and post-mutation state, surfacing integrity
 *   issues via the log.
 * - **Post-write integrity check:** after every successful rename, the file is read back and byte-compared against what was written. On mismatch, the .bak is
 *   restored and the call throws - catches encoder bugs, partial writes, and any filesystem-level corruption that happened during the write.
 *
 * @template T - The in-memory data type that callers mutate.
 * @param options - Store configuration.
 * @returns A FileStore instance. Auto-registers in the global store registry so the release boot coordinator can snapshot it.
 */
export function createFileStore<T>(options: FileStoreOptions<T>): FileStore<T> {

  // Serialization queue. Each mutate() call chains onto this promise so operations execute one at a time.
  let queue: Promise<void> = Promise.resolve();

  // Lazy data directory creation. The directory is created once on the first write, then skipped for subsequent writes.
  let dataDirEnsured = false;

  // Resolve the backend: the caller may inject an alternative implementation; production stores get the shared default.
  const backend: StorageBackend = options.backend ?? defaultStorageBackend;

  // Validate the migration configuration at construction time so misconfigurations surface immediately rather than during the first migration attempt.
  if(options.migrations) {

    if((options.currentSchemaVersion === undefined) || !options.getSchemaVersion || !options.setSchemaVersion) {

      throw new Error("Store '" + options.label + "' declares migrations but is missing currentSchemaVersion, getSchemaVersion, or setSchemaVersion.");
    }
  }

  /**
   * Runs every pending migration on the in-memory data, mutating it in place. Returns the result so callers can decide whether to persist. Throws when a
   * version gap appears in the migration map (indicates a programmer error - every intermediate version must have a migration).
   */
  function runMigrations(data: T): MigrationResult {

    if(!options.migrations || (options.currentSchemaVersion === undefined) || !options.getSchemaVersion || !options.setSchemaVersion) {

      return { applied: [], fromVersion: 0, toVersion: 0 };
    }

    let currentVersion = options.getSchemaVersion(data);
    const fromVersion = currentVersion;
    const applied: string[] = [];

    // Forward-compatible read: a file written by a newer PrismCast version reports a higher schemaVersion. We do not attempt to downgrade - just log and let
    // the consumer code work with whatever it can. This keeps the file safe from a downgrade scenario where an old binary would otherwise mutate it.
    if(currentVersion > options.currentSchemaVersion) {

      LOG.warn("%s schema version %d is newer than this build supports (%d). Reading without migration.",
        options.label, currentVersion, options.currentSchemaVersion);

      return { applied: [], fromVersion, toVersion: fromVersion };
    }

    while(currentVersion < options.currentSchemaVersion) {

      const targetVersion = currentVersion + 1;
      const migration = options.migrations[targetVersion];

      if(!migration) {

        throw new Error("Store '" + options.label + "' is missing a migration to schema version " + String(targetVersion) + ".");
      }

      migration.apply(data);
      options.setSchemaVersion(data, targetVersion);

      if(options.recordMigration) {

        options.recordMigration(data, migration.description);
      }

      applied.push(migration.description);
      currentVersion = targetVersion;
    }

    return { applied, fromVersion, toVersion: currentVersion };
  }

  /**
   * Routes ValidationIssues to the log. Warnings log at warn level; errors log at error level. Both are logged without throwing.
   */
  function handleValidationIssues(issues: ValidationIssue[]): void {

    if(issues.length === 0) {

      return;
    }

    for(const issue of issues) {

      const message = "Pre-write integrity check (" + options.label + ", " + issue.category + "): " + issue.description;

      if(issue.severity === "error") {

        LOG.error(message);
      } else {

        LOG.warn(message);
      }
    }
  }

  /**
   * Attempts to recover from .bak when the main file fails to parse. Reads .bak, parses it, and on success restores the main file from .bak via atomic temp+
   * rename. Returns the parsed recovered data, or null if .bak is missing or also unparseable.
   *
   * The restore-write is best-effort - the in-memory recovered data is what callers act on, and a failed restore leaves the main file corrupt on disk without
   * losing the good copy. The actual protection against rotating a corrupt main into .bak lives in doMutate, which skips the main->bak rotation whenever read()
   * reports recoveredFromBackup; this function's restore is the convenience that lets a subsequent clean read see a non-corrupt main once the write succeeds.
   *
   * The restore uses a recovery-specific temp suffix (.recover.tmp) distinct from doMutate's .tmp. Because read() runs outside the mutate serialization lock,
   * a read-triggered recovery can overlap an in-flight mutate; distinct suffixes guarantee the recovery never clobbers the mutate's temp before its rename.
   */
  async function tryRecoverFromBackup(filePath: string): Promise<T | null> {

    const bakPath = filePath + ".bak";

    let bakContent: string;

    try {

      bakContent = await backend.readFile(bakPath);
    } catch {

      return null;
    }

    let data: T;

    try {

      data = options.parse(bakContent);
    } catch {

      return null;
    }

    // Restore main from .bak via atomic temp+rename. Best-effort: if the restore write fails, we still return the recovered data so the caller can proceed
    // with valid in-memory state. A subsequent read would attempt recovery again and produce the same result.
    //
    // The restore uses a recovery-specific temp suffix (.recover.tmp) rather than the .tmp suffix doMutate uses. read() does NOT acquire the mutate
    // serialization lock, so a read-triggered recovery can run concurrently with an in-flight mutate. Sharing the .tmp path would let a recovery's write or
    // rename clobber a mutate's in-flight temp file before the mutate renames it into place. Distinct suffixes keep the two write paths from ever colliding.
    const tmpPath = filePath + ".recover.tmp";

    try {

      await backend.writeFile(tmpPath, bakContent);
      await backend.rename(tmpPath, filePath);
    } catch(restoreError) {

      LOG.warn("Recovered %s data from backup but failed to restore the main file: %s.", options.label,
        (restoreError instanceof Error) ? restoreError.message : String(restoreError));

      try {

        await backend.unlink(tmpPath);
      } catch {

        // Cleanup is best-effort.
      }
    }

    return data;
  }

  /**
   * Reads the file from disk, parses it, runs any pending migrations in memory, and returns the result. On a parse failure of the main file, attempts to
   * recover from .bak before falling back to defaults. Migrations applied here are not persisted by this method alone - callers who need to persist an upgrade
   * should use ensureMigrated().
   */
  async function read(): Promise<FileStoreReadResult<T>> {

    const filePath = options.path();

    let parsed: { data: T; recoveredFromBackup: boolean } | null = null;
    let parseError: { message: string } | null = null;

    try {

      const content = await backend.readFile(filePath);

      try {

        parsed = { data: options.parse(content), recoveredFromBackup: false };
      } catch(error) {

        const message = (error instanceof Error) ? error.message : String(error);

        // The main file is corrupt. Try to recover from the .bak rotation before falling back to defaults.
        const recovered = await tryRecoverFromBackup(filePath);

        if(recovered !== null) {

          LOG.warn("Recovered %s from backup; main file was corrupt: %s.", options.label, message);

          parsed = { data: recovered, recoveredFromBackup: true };
        } else {

          parseError = { message };
        }
      }
    } catch(error) {

      // File doesn't exist - this is normal on first run, use defaults. We deliberately do NOT auto-recover from .bak here: a missing main file is the
      // first-run signal, and recovering from a stale .bak from a prior installation would be surprising. Operators wanting to restore a deleted main file
      // can rename .bak manually.
      if((error as NodeJS.ErrnoException).code === "ENOENT") {

        parsed = { data: options.defaultValue(), recoveredFromBackup: false };
      } else {

        // Other read errors - log and use defaults.
        LOG.warn("Failed to read %s file %s: %s. Using defaults.", options.label, filePath, (error instanceof Error) ? error.message : String(error));

        parsed = { data: options.defaultValue(), recoveredFromBackup: false };
      }
    }

    if(parsed === null) {

      // Both main and .bak failed to parse.
      LOG.warn("Invalid JSON in %s file %s and no usable backup available: %s. Using defaults.", options.label, filePath, parseError?.message ?? "");

      return {

        data: options.defaultValue(),
        migrationResult: { applied: [], fromVersion: 0, toVersion: 0 },
        parseError: true,
        parseErrorMessage: parseError?.message,
        recoveredFromBackup: false
      };
    }

    // Run migrations in-memory. The data returned is always at currentSchemaVersion (or higher in the forward-compat case).
    const migrationResult = runMigrations(parsed.data);

    if(migrationResult.applied.length > 0) {

      LOG.info("Migrated %s schema from v%d to v%d (%d migration(s) applied: %s).",
        options.label, migrationResult.fromVersion, migrationResult.toVersion, migrationResult.applied.length, migrationResult.applied.join(", "));
    }

    return { data: parsed.data, migrationResult, parseError: false, recoveredFromBackup: parsed.recoveredFromBackup };
  }

  /**
   * Prunes old snapshots for a given file, keeping only the SNAPSHOT_RETENTION most recently created. Snapshots are matched by filename prefix so each file's
   * snapshots are pruned independently. Failures (missing dir, unreadable entries, unlink errors) are best-effort - logged but never propagated, since pruning
   * is a hygiene operation and must not block the create that triggered it.
   */
  async function pruneSnapshots(snapshotDir: string, baseName: string): Promise<void> {

    let entries: string[];

    try {

      entries = await backend.readdir(snapshotDir);
    } catch {

      // Directory does not exist or is unreadable - nothing to prune.
      return;
    }

    // Match snapshots that belong to this file. Naming convention is `<baseName>.<label>` inside the snapshots dir, so prefix-matching is sufficient.
    const prefix = baseName + ".";
    const matching = entries.filter((entry) => entry.startsWith(prefix));

    if(matching.length <= SNAPSHOT_RETENTION) {

      return;
    }

    // Stat each candidate. Entries that fail to stat are dropped from the candidate list rather than blocking pruning of the rest.
    const stats = await Promise.all(matching.map(async (name) => {

      const fullPath = path.join(snapshotDir, name);

      try {

        const stat = await backend.stat(fullPath);

        return { mtime: stat.mtimeMs, path: fullPath };
      } catch {

        return null;
      }
    }));

    // Sort by mtime descending so the most recent are first; everything past the retention window is deleted. Unlinks run in parallel since they are
    // independent; a failure on one entry does not block the rest.
    const sorted = stats.filter((entry): entry is { mtime: number; path: string } => (entry !== null)).sort((a, b) => (b.mtime - a.mtime));

    await Promise.all(sorted.slice(SNAPSHOT_RETENTION).map(async (old) => {

      try {

        await backend.unlink(old.path);

        LOG.info("Pruned old snapshot %s.", old.path);
      } catch(error) {

        LOG.warn("Failed to prune snapshot %s: %s.", old.path, (error instanceof Error) ? error.message : String(error));
      }
    }));
  }

  /**
   * Creates a labeled snapshot copy of the current file inside a `snapshots/` subdirectory next to the source file. Idempotent on the label so repeated calls
   * within the same release are no-ops after the first one. After a successful create, prunes old snapshots for this file so at most SNAPSHOT_RETENTION are
   * retained (by mtime). Snapshots are version-keyed safety nets that survive normal .bak rotation, intended to be the restore-of-last-resort when a release
   * introduces a data-shape regression that escapes the primary safeguards.
   */
  async function snapshot(label: string): Promise<void> {

    const filePath = options.path();
    const baseName = path.basename(filePath);
    const snapshotDir = path.join(path.dirname(filePath), "snapshots");
    const snapshotPath = path.join(snapshotDir, baseName + "." + label);

    // Idempotent: skip if a snapshot with this label already exists. The first boot under a given release captures the snapshot; subsequent boots see it and
    // skip, so the snapshot reflects the file's state at first-boot of that release.
    try {

      await backend.access(snapshotPath);

      return;
    } catch {

      // Snapshot does not exist yet; proceed to create it.
    }

    // Ensure the snapshots subdirectory exists. Best-effort: if mkdir fails the copyFile below surfaces the real error.
    try {

      await backend.mkdir(snapshotDir);
    } catch {

      // Best-effort; the copyFile call below will surface the underlying problem.
    }

    try {

      await backend.copyFile(filePath, snapshotPath);

      LOG.info("Created snapshot of %s at %s.", options.label, snapshotPath);
    } catch(error) {

      // Source file does not exist - first run, nothing to snapshot. Not an error.
      if((error as NodeJS.ErrnoException).code === "ENOENT") {

        return;
      }

      LOG.warn("Failed to create snapshot of %s at %s: %s.", options.label, snapshotPath,
        (error instanceof Error) ? error.message : String(error));

      return;
    }

    // Hygiene: keep only the most recent SNAPSHOT_RETENTION snapshots for this file.
    await pruneSnapshots(snapshotDir, baseName);
  }

  /**
   * Executes a single mutation: read (with migrations), guard, snapshot pre-state for validation, mutate, validate, backup, atomic write. Called under the
   * serialization queue.
   */
  async function doMutate(fn: (current: T) => void): Promise<void> {

    const filePath = options.path();

    // Read the current file state. read() will transparently recover from .bak when the main file is corrupt and run any pending migrations; only when both
    // main and .bak fail to parse does parseError surface here.
    const result = await read();

    // Corruption guard: refuse to modify a file that cannot be parsed and could not be recovered from backup. Prevents the cascade where a corrupt file gets
    // overwritten with nearly-empty data.
    if(result.parseError) {

      throw new FileStoreParseError(options.label, filePath, result.parseErrorMessage ?? "Unknown parse error.");
    }

    // Capture pre-mutation state for the validator. Skip the clone when no validator is configured to avoid the overhead.
    const prevState = options.validate ? structuredClone(result.data) : null;

    // Apply the caller's mutation. Callbacks modify data in place.
    fn(result.data);

    // Run the validator if one is configured. Issues are logged without aborting the write.
    if(options.validate && prevState) {

      handleValidationIssues(options.validate(prevState, result.data));
    }

    // Apply the beforeWrite transform to produce the serializable form.
    const serializable = options.beforeWrite ? options.beforeWrite(result.data) : result.data;
    const content = stringifySorted(serializable) + "\n";

    // Ensure the data directory exists on the first write.
    if(!dataDirEnsured) {

      await backend.mkdir(getDataDir());
      dataDirEnsured = true;
    }

    // Backup: copy the current file to .bak before overwriting. Swallow ENOENT (file does not exist yet on first write).
    //
    // We skip the rotation entirely when read() recovered the in-memory state from .bak. recoveredFromBackup being true means the main file on disk was
    // unparseable and the good copy is the existing .bak - so .bak already holds the prior good state. Had tryRecoverFromBackup's restore-write to main failed,
    // the main file on disk is still corrupt; copying it over .bak would destroy the only good copy. Had the restore succeeded, main now equals .bak and the
    // upcoming atomic write overwrites it with the mutated good data while .bak keeps the prior good state. Either way, rotating is at best redundant and at
    // worst destructive, so we leave the known-good .bak untouched and let the atomic write below replace the (recovered) main.
    const bakPath = filePath + ".bak";

    if(!result.recoveredFromBackup) {

      try {

        await backend.copyFile(filePath, bakPath);
      } catch(backupError) {

        if((backupError as NodeJS.ErrnoException).code !== "ENOENT") {

          LOG.warn("Failed to back up %s: %s.", filePath, (backupError instanceof Error) ? backupError.message : String(backupError));
        }
      }
    }

    // Atomic write: write to a temp file, then rename over the original. rename() is atomic on POSIX and NTFS filesystems.
    const tmpPath = filePath + ".tmp";

    try {

      await backend.writeFile(tmpPath, content);
      await backend.rename(tmpPath, filePath);
    } catch(writeError) {

      // Attempt cleanup of the temp file on failure.
      try {

        await backend.unlink(tmpPath);
      } catch {

        // Cleanup is best-effort.
      }

      throw writeError;
    }

    // Post-write integrity check: read the file we just wrote and verify it byte-matches what we intended to write. Catches encoder bugs, partial writes, and
    // any disk- or filesystem-level corruption that happened between writeFile and now. On a verification failure the main file is structurally suspect, so
    // restore from .bak (which still holds the prior good state) and throw - the caller's expectation that the write succeeded was wrong.
    //
    // The readback may hit page cache rather than physical disk on most platforms, so this check does NOT prove durability - that requires fsync. It does
    // prove integrity: what we asked the OS to write equals what the OS now reports as the file's contents.
    let written: string;

    try {

      written = await backend.readFile(filePath);
    } catch(readbackError) {

      LOG.error("Post-write readback failed for %s: %s.", filePath, (readbackError instanceof Error) ? readbackError.message : String(readbackError));

      throw readbackError;
    }

    if(written !== content) {

      LOG.error("Post-write integrity check failed for %s: %d bytes on disk, expected %d bytes. Restoring from backup.",
        filePath, written.length, content.length);

      // Restore the prior good state. tryRecoverFromBackup handles the atomic rename of .bak -> main.
      const recovered = await tryRecoverFromBackup(filePath);

      if(recovered === null) {

        LOG.error("Backup recovery also failed for %s. The file is in an inconsistent state and requires operator action.", filePath);
      }

      throw new Error("Post-write integrity check failed for " + options.label + " (" + filePath + "): on-disk content does not match the intended write.");
    }

    LOG.debug("persistence:write", "Saved %s to %s.", options.label, filePath);
  }

  /**
   * Enqueues a mutation onto the serialization queue. The promise chain ensures only one mutation runs at a time. Errors propagate to the caller but do not
   * break the chain for subsequent callers.
   */
  async function mutate(fn: (current: T) => void): Promise<void> {

    const operation = queue.then(async () => doMutate(fn));

    // Swallow errors on the chain reference so future operations can proceed. The error still propagates to the caller via the returned promise.
    // eslint-disable-next-line @typescript-eslint/no-empty-function -- Intentional no-op: errors are propagated to the caller via the returned promise.
    queue = operation.catch(() => {});

    return operation;
  }

  /**
   * Verifies the file is at the current schema version, persisting any required upgrade. Idempotent - when the file is already current this is a single read
   * with no write. The release boot coordinator calls this on every store after snapshots have been captured.
   */
  async function ensureMigrated(): Promise<MigrationResult> {

    // Peek to determine whether migrations are needed. read() runs migrations in memory but does not persist.
    const peek = await read();

    if(peek.migrationResult.applied.length === 0) {

      return peek.migrationResult;
    }

    // Migrations were applied in memory - persist via a no-op mutate. mutate's internal read re-runs the migrations so the persisted state matches what we
    // observed. The cost is one extra read, but only on boots where migrations were actually needed.
    await mutate(() => { /* no-op: the write exists to persist the schema upgrade. */ });

    return peek.migrationResult;
  }

  const store: FileStore<T> = { ensureMigrated, mutate, read, snapshot };

  registeredStores.push(store);

  return store;
}
