/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.ts: Transactional file store for PrismCast configuration persistence.
 */
import { LOG, stringifySorted } from "../utils/index.js";
import fs from "node:fs";
import { getDataDir } from "./paths.js";

const { promises: fsPromises } = fs;

/**
 * Error thrown by `mutate()` when the backing file contains invalid JSON. Route handlers can catch this specifically to return 400 instead of 500. Background code
 * paths that swallow errors via try/catch will handle it like any other Error.
 */
export class FileStoreParseError extends Error {

  override name = "FileStoreParseError";

  constructor(label: string, filePath: string, parseMessage: string) {

    super("Cannot modify " + label + " (" + filePath + "): file contains invalid JSON. " + parseMessage);
  }
}

// Types.

/**
 * Result from a read-only file store access. Callers that only need data (export endpoints, API responses) use this without risking a save-back.
 */
export interface FileStoreReadResult<T> {

  data: T;
  parseError: boolean;
  parseErrorMessage?: string;
}

/**
 * Options for creating a file store instance.
 * @template T - The in-memory data type that callers mutate.
 */
export interface FileStoreOptions<T> {

  /* Transform applied before serialization. Returns the serializable form, which may differ from T (e.g., channels inject metadata keys not in StoredChannelMap).
   * If omitted, the data is serialized as-is.
   */
  beforeWrite?: (data: T) => unknown;

  // Factory producing an empty default value for T. Used when the file doesn't exist (first run) or can't be read. Explicit factory avoids overloading the
  // parse function with a hidden dual-purpose contract.
  defaultValue: () => T;

  // Human-readable label for log messages and error text (e.g., "configuration", "channels").
  label: string;

  // Parse raw file content into the in-memory type. Called by read() after a successful file read.
  parse: (raw: string) => T;

  // Deferred path resolver. Called on each read/write to support late initialization of the data directory.
  path: () => string;
}

/**
 * A transactional file store that provides atomic writes, serialized mutations, corruption protection, and backup rotation. Callers interact with data through
 * `mutate()` (serialized read-modify-write) and `read()` (read-only access). Direct file I/O is never exposed.
 * @template T - The in-memory data type that callers mutate.
 */
export interface FileStore<T> {

  /**
   * Serialized read-modify-write operation. The mutation function receives the current data and modifies it in place. The store handles atomicity,
   * serialization, corruption guard, backup, and beforeWrite transforms.
   * @param fn - Mutation function. Receives current data. Modify in place; return value is ignored.
   * @throws FileStoreParseError if the file contains invalid JSON (corruption guard).
   */
  mutate(fn: (current: T) => void): Promise<void>;

  /**
   * Read-only access to the current file contents. Returns the parsed data with parse status. Does not acquire the serialization lock...safe to call concurrently.
   */
  read(): Promise<FileStoreReadResult<T>>;
}

// Store factory.

/**
 * Creates a transactional file store instance. Each instance owns a single JSON file and provides serialized, atomic access to it.
 *
 * Safety guarantees:
 * - **Atomic writes:** data is written to a `.tmp` file and renamed over the original. `rename()` is atomic on POSIX and NTFS.
 * - **Serialization:** a promise chain ensures only one `mutate()` runs at a time. Concurrent callers queue behind the active operation.
 * - **Corruption guard:** `mutate()` throws `FileStoreParseError` if the file contains invalid JSON, preventing save-over-corrupt cascades.
 * - **Backup rotation:** before each write, the current file is copied to `.bak`. One-deep rotation provides a recovery path.
 *
 * @template T - The in-memory data type that callers mutate.
 * @param options - Store configuration.
 * @returns A FileStore instance with `read()` and `mutate()` methods.
 */
export function createFileStore<T>(options: FileStoreOptions<T>): FileStore<T> {

  // Serialization queue. Each mutate() call chains onto this promise so operations execute one at a time.
  let queue: Promise<void> = Promise.resolve();

  // Lazy data directory creation. The directory is created once on the first write, then skipped for subsequent writes.
  let dataDirEnsured = false;

  /**
   * Reads the file from disk and parses it. Handles missing files (normal on first run), parse errors, and other read failures.
   */
  async function read(): Promise<FileStoreReadResult<T>> {

    const filePath = options.path();

    try {

      const content = await fsPromises.readFile(filePath, "utf-8");

      try {

        const data = options.parse(content);

        return { data, parseError: false };
      } catch(parseError) {

        const message = (parseError instanceof Error) ? parseError.message : String(parseError);

        LOG.warn("Invalid JSON in %s file %s: %s. Using defaults.", options.label, filePath, message);

        return { data: options.defaultValue(), parseError: true, parseErrorMessage: message };
      }
    } catch(error) {

      // File doesn't exist...this is normal on first run, use defaults.
      if((error as NodeJS.ErrnoException).code === "ENOENT") {

        return { data: options.defaultValue(), parseError: false };
      }

      // Other read errors...log and use defaults.
      LOG.warn("Failed to read %s file %s: %s. Using defaults.", options.label, filePath, (error instanceof Error) ? error.message : String(error));

      return { data: options.defaultValue(), parseError: false };
    }
  }

  /**
   * Executes a single mutation: read, guard, mutate, backup, atomic write. Called under the serialization queue.
   */
  async function doMutate(fn: (current: T) => void): Promise<void> {

    const filePath = options.path();

    // Read the current file state.
    const result = await read();

    // Corruption guard: refuse to modify a file that can't be parsed. This prevents the cascade where a corrupt file gets overwritten with nearly-empty data.
    if(result.parseError) {

      throw new FileStoreParseError(options.label, filePath, result.parseErrorMessage ?? "Unknown parse error.");
    }

    // Apply the caller's mutation. Callbacks modify data in place.
    fn(result.data);

    // Apply the beforeWrite transform to produce the serializable form.
    const serializable = options.beforeWrite ? options.beforeWrite(result.data) : result.data;
    const content = stringifySorted(serializable) + "\n";

    // Ensure the data directory exists on the first write.
    if(!dataDirEnsured) {

      await fsPromises.mkdir(getDataDir(), { recursive: true });
      dataDirEnsured = true;
    }

    // Backup: copy the current file to .bak before overwriting. Swallow ENOENT (file doesn't exist yet on first write).
    const bakPath = filePath + ".bak";

    try {

      await fsPromises.copyFile(filePath, bakPath);
    } catch(backupError) {

      if((backupError as NodeJS.ErrnoException).code !== "ENOENT") {

        LOG.warn("Failed to back up %s: %s.", filePath, (backupError instanceof Error) ? backupError.message : String(backupError));
      }
    }

    // Atomic write: write to a temp file, then rename over the original. rename() is atomic on POSIX and NTFS filesystems.
    const tmpPath = filePath + ".tmp";

    try {

      await fsPromises.writeFile(tmpPath, content, "utf-8");
      await fsPromises.rename(tmpPath, filePath);
    } catch(writeError) {

      // Attempt cleanup of the temp file on failure.
      try {

        await fsPromises.unlink(tmpPath);
      } catch {

        // Cleanup is best-effort.
      }

      throw writeError;
    }

    LOG.info("Saved %s to %s.", options.label, filePath);
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

  return { mutate, read };
}
