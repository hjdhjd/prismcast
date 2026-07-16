/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.helpers.ts: Test-only helpers for the persistence framework. The persistence module is built on a backend-agnostic StorageBackend port; this file
 * exposes an in-memory backend that satisfies the same contract as the production node:fs/promises adapter without touching the real filesystem.
 *
 * The in-memory backend serves two roles in tests:
 *
 *   1. **Faster paths.** Tests that exercise the framework's correctness logic (atomic write sequencing, queue serialization, post-write integrity checks)
 *      benefit from a backend with no filesystem I/O - faster, no temp dirs, no after-test cleanup.
 *
 *   2. **Failure injection.** Override hooks let tests force any single backend operation to throw or return arbitrary content. This is how unreached safety
 *      paths (snapshot pruning's per-entry stat/unlink errors, post-write integrity check's readback mismatch, tryRecoverFromBackup's restore-write failure,
 *      doMutate's non-ENOENT backup failure) are pinned without resorting to fragile real-fs trickery (chmod, EISDIR via directory-as-file).
 *
 * Helper-location convention: this is a domain-specific factory tied to one production module (persistence.ts), so it co-locates with its owner. Cross-cutting
 * test primitives live in src/testing/; domain-specific factories sit next to the module they test.
 */
import type { FileStore, Migration, StorageBackend, ValidationIssue } from "./persistence.ts";
import { createFileStore } from "./persistence.ts";
import { initializeDataDir } from "./paths.ts";
import path from "node:path";

/**
 * Override hooks for makeMemoryStorageBackend. Any hook that is supplied REPLACES the default in-memory implementation for that operation; absent hooks keep
 * the default behavior. Tests typically override one or two operations to inject failure (e.g., readdir rejects, writeFile lies about what it wrote) while
 * leaving the rest functional so the backend is still usable end-to-end.
 *
 * Each hook is invoked exactly when the framework calls the corresponding StorageBackend method. The default implementation of every other operation continues
 * to flow through the underlying Map<string, string> store, so override-and-default operations interleave naturally - a writeFile override can record the call
 * AND let the default behavior store the bytes by mutating the exposed files map directly.
 */
export type MemoryStorageBackendOverrides = Partial<StorageBackend>;

/**
 * The shape returned by makeMemoryStorageBackend. Extends the StorageBackend surface with direct access to the underlying state maps so tests can seed initial
 * content, assert on post-test contents, or inspect mtime ordering without going through the backend operations.
 *
 * The backend operations are typed as MUTABLE (not readonly like the production StorageBackend) so tests can swap individual operations after construction -
 * the canonical pattern for late-binding failure injection where the override needs a captured reference to the original (`const realCopyFile = backend.
 * copyFile; backend.copyFile = (...) => { ... realCopyFile(...) ... };`). The mutability is scoped to test code; production never receives a MemoryStorageBackend.
 */
export interface MemoryStorageBackend {

  // Mutable variants of the StorageBackend surface. Tests reassign individual operations to inject failures while leaving siblings on their default behavior.
  access: StorageBackend["access"];
  copyFile: StorageBackend["copyFile"];
  mkdir: StorageBackend["mkdir"];
  readdir: StorageBackend["readdir"];
  readFile: StorageBackend["readFile"];
  rename: StorageBackend["rename"];
  stat: StorageBackend["stat"];
  unlink: StorageBackend["unlink"];
  writeFile: StorageBackend["writeFile"];

  // The underlying file content map. Keys are absolute paths; values are UTF-8 strings. Mutate directly to seed initial state for a test.
  readonly files: Map<string, string>;

  // The underlying mtime map, keyed by absolute path. Values are monotonically-increasing per-write counters (not real Date.now() values) so tests can reason
  // about relative ordering without dealing with same-millisecond collisions.
  readonly mtimes: Map<string, number>;
}

/**
 * Constructs a NodeJS.ErrnoException with the requested error code. The framework's branch detection relies on `code === "ENOENT"` (and only that code), so the
 * memory backend's missing-path errors must carry the same shape as the real node:fs/promises errors.
 * @param code - The error code (e.g., "ENOENT", "EACCES").
 * @param path - The path that triggered the error. Embedded in the message for diagnostic parity with native fs errors.
 * @returns A NodeJS.ErrnoException with the supplied code.
 */
function makeErrnoError(code: string, path: string): NodeJS.ErrnoException {

  const err = new Error(code + ": memory backend error at '" + path + "'") as NodeJS.ErrnoException;

  err.code = code;

  return err;
}

/**
 * Lists immediate children of a directory by examining the keys of the file map. Subdirectory entries surface as their next-segment basename; sibling files
 * surface as their basename. The result mirrors what fs.readdir returns: a flat array of basenames, no full paths, no trailing separators.
 * @param files - The backend's file content map.
 * @param dirPath - Absolute directory path (no trailing separator).
 * @returns Array of unique basenames inside the directory. Empty when nothing matches.
 */
function listDirectory(files: Map<string, string>, dirPath: string): string[] {

  const prefix = dirPath.endsWith("/") ? dirPath : (dirPath + "/");
  const entries = new Set<string>();

  for(const filePath of files.keys()) {

    if(!filePath.startsWith(prefix)) {

      continue;
    }

    const remainder = filePath.slice(prefix.length);
    const slashIndex = remainder.indexOf("/");

    entries.add((slashIndex === -1) ? remainder : remainder.slice(0, slashIndex));
  }

  return [...entries];
}

/**
 * Builds an in-memory StorageBackend for tests. The default behavior matches a flat-namespace filesystem closely enough for the framework's needs - mkdir is a
 * no-op (directories are implicit in path strings), readdir computes children from the keys of the file map, mtimes are monotonic per-write counters.
 *
 * Override hooks replace specific operations entirely; the operations that are not overridden continue to use the default in-memory implementations against the
 * shared maps. This means a test can override (say) writeFile to inject a corruption between write and readback, while letting every other operation behave
 * normally - the framework's atomic temp+rename, backup copy, and snapshot copy all still execute against the same in-memory state.
 *
 * @param overrides - Optional per-operation overrides. Each absent hook keeps the default in-memory implementation.
 * @returns An in-memory StorageBackend with direct access to the file and mtime maps.
 */
export function makeMemoryStorageBackend(overrides: MemoryStorageBackendOverrides = {}): MemoryStorageBackend {

  const files = new Map<string, string>();
  const mtimes = new Map<string, number>();
  let writeCounter = 0;

  // Bumps the monotonic write counter and returns the new value. Using a counter rather than Date.now() avoids same-millisecond collisions when the test issues
  // bursts of writes - the framework's snapshot pruning sorts by mtimeMs and would otherwise see ties.
  function nextMtime(): number {

    writeCounter += 1;

    return writeCounter;
  }

  // Default in-memory implementations of every StorageBackend operation. Each is declared as an async arrow function so the body can use `throw` and `return`
  // naturally even when no I/O is awaited; the eslint-disable directives suppress require-await on the bodies that don't need an actual await call (require-
  // await would force ceremonial Promise.resolve() shapes that obscure the contract).
  const defaults: StorageBackend = {

    access: async (path: string): Promise<void> => {

      // access succeeds only when the path is a known file. Directories are not tracked, so we infer presence by whether any file has the directory as a prefix.
      if(files.has(path)) {

        return;
      }

      // Directory check: any file under this path means the directory exists. Mirrors the parent-of-children semantics of a real filesystem closely enough for
      // the framework's idempotent-snapshot use case.
      const prefix = path.endsWith("/") ? path : (path + "/");

      for(const filePath of files.keys()) {

        if(filePath.startsWith(prefix)) {

          return;
        }
      }

      throw makeErrnoError("ENOENT", path);
    },

    copyFile: async (source: string, destination: string): Promise<void> => {

      const content = files.get(source);

      if(content === undefined) {

        throw makeErrnoError("ENOENT", source);
      }

      files.set(destination, content);
      mtimes.set(destination, nextMtime());
    },

    // mkdir is a no-op: directories are implicit in path strings. The framework's only mkdir contract is "ensure the parent directory exists for a subsequent
    // write" - in memory, that is automatic.
    mkdir: async (): Promise<void> => Promise.resolve(),

    readFile: async (path: string): Promise<string> => {

      const content = files.get(path);

      if(content === undefined) {

        throw makeErrnoError("ENOENT", path);
      }

      return content;
    },

    readdir: async (path: string): Promise<string[]> => Promise.resolve(listDirectory(files, path)),

    rename: async (source: string, destination: string): Promise<void> => {

      const content = files.get(source);

      if(content === undefined) {

        throw makeErrnoError("ENOENT", source);
      }

      const sourceMtime = mtimes.get(source) ?? nextMtime();

      files.delete(source);
      mtimes.delete(source);
      files.set(destination, content);
      mtimes.set(destination, sourceMtime);
    },

    stat: async (path: string): Promise<{ mtimeMs: number }> => {

      const mtime = mtimes.get(path);

      if(mtime === undefined) {

        throw makeErrnoError("ENOENT", path);
      }

      return { mtimeMs: mtime };
    },

    unlink: async (path: string): Promise<void> => {

      if(!files.has(path)) {

        throw makeErrnoError("ENOENT", path);
      }

      files.delete(path);
      mtimes.delete(path);
    },

    writeFile: async (path: string, content: string): Promise<void> => {

      files.set(path, content);
      mtimes.set(path, nextMtime());
    }
  };

  // Compose: every operation is the override if supplied, otherwise the default. The StorageBackend-typed `defaults` object above is what enforces drift - any
  // future addition to the interface flags a typecheck error at that declaration (it would be missing the new method), forcing the new operation to be considered
  // here as well.
  return {

    access: overrides.access ?? defaults.access,
    copyFile: overrides.copyFile ?? defaults.copyFile,
    files,
    mkdir: overrides.mkdir ?? defaults.mkdir,
    mtimes,
    readFile: overrides.readFile ?? defaults.readFile,
    readdir: overrides.readdir ?? defaults.readdir,
    rename: overrides.rename ?? defaults.rename,
    stat: overrides.stat ?? defaults.stat,
    unlink: overrides.unlink ?? defaults.unlink,
    writeFile: overrides.writeFile ?? defaults.writeFile
  };
}

/**
 * Options accepted by the makeStore / makeMemoryStore factories. Mirrors the FileStoreOptions surface tests typically configure - migrations, validation,
 * schema version, custom parse/defaults - while letting the factories provide the boilerplate (label, path, getSchemaVersion/setSchemaVersion wiring).
 */
export interface MakeStoreOptions<T> {

  // Override for FileStoreOptions.currentSchemaVersion. When set, the factory wires the matching getSchemaVersion / setSchemaVersion pair so tests do not have
  // to repeat that boilerplate.
  currentSchemaVersion?: number;

  // Override for FileStoreOptions.defaultValue. Defaults to producing an empty object cast to T.
  defaultValue?: () => T;

  // Override for FileStoreOptions.migrations. When set, the factory automatically wires the schema-version helpers above so the migration runner has the
  // everything-or-nothing surface FileStoreOptions requires.
  migrations?: Record<number, Migration<T>>;

  // Override for FileStoreOptions.parse. Defaults to JSON.parse cast to T.
  parse?: (raw: string) => T;

  // Override for FileStoreOptions.validate.
  validate?: (prev: T, next: T) => ValidationIssue[];
}

/**
 * Builds a FileStore against a real-filesystem temp directory. Tests pass a unique filename per case so concurrent stores do not collide on the framework's
 * global registry. The factory hides the boilerplate that every test would otherwise duplicate (label derivation, path resolver, schema-version wiring).
 *
 * Use this when the test exercises the full real-fs I/O pipeline (atomic writes, real backup rotation, real snapshot-directory operations). For tests that
 * inject filesystem failures or want a faster path, use makeMemoryStore below.
 *
 * @param dir - The temp directory the store writes into. Caller is responsible for creating and cleaning it (typically via withTempDir).
 * @param filename - The on-disk filename for this store. Must be unique within the test process.
 * @param options - Per-test FileStoreOptions overrides.
 * @returns A FileStore wired to the supplied temp directory.
 */
export function makeStore<T>(dir: string, filename: string, options: MakeStoreOptions<T> = {}): FileStore<T> {

  // Point the production paths resolver at the temp dir so the framework's first-write mkdir hits the right place.
  initializeDataDir(dir);

  return createFileStore<T>({

    currentSchemaVersion: options.currentSchemaVersion,
    defaultValue: options.defaultValue ?? ((): T => ({} as T)),
    getSchemaVersion: options.currentSchemaVersion ? ((data: T): number => (data as { schemaVersion?: number }).schemaVersion ?? 1) : undefined,
    label: "test-" + filename,
    migrations: options.migrations,
    parse: options.parse ?? ((raw: string): T => JSON.parse(raw) as T),
    path: (): string => path.join(dir, filename),
    setSchemaVersion: options.currentSchemaVersion ?
      ((data: T, version: number): void => { (data as { schemaVersion?: number }).schemaVersion = version; }) :
      undefined,
    validate: options.validate
  });
}

/**
 * Like makeStore but injects an in-memory StorageBackend instead of the default node:fs/promises adapter. The caller passes the backend explicitly so the same
 * backend can be inspected after the test (the file map, mtime ledger), or so a single test can build the backend with failure-injection overrides and pass
 * it through unchanged.
 *
 * Tests that need the framework's real on-disk-atomicity guarantees should use makeStore. Tests that exercise the framework's correctness logic over a
 * controlled storage layer (or that need failure injection) should use this.
 *
 * @param backend - The StorageBackend to wire into the store. Typically built via makeMemoryStorageBackend.
 * @param filePath - The path the store writes to. Memory backends use the path as a Map key, so any string distinct from other test paths works.
 * @param options - Per-test FileStoreOptions overrides.
 * @returns A FileStore that issues all I/O through the supplied backend.
 */
export function makeMemoryStore<T>(backend: StorageBackend, filePath: string, options: MakeStoreOptions<T> = {}): FileStore<T> {

  // Point the production paths resolver at a synthetic data dir derived from filePath. The framework calls getDataDir() during the first write to mkdir the
  // data directory; with the memory backend that mkdir is a no-op, but the resolver still has to return a non-null path.
  initializeDataDir(path.dirname(filePath));

  return createFileStore<T>({

    backend,
    currentSchemaVersion: options.currentSchemaVersion,
    defaultValue: options.defaultValue ?? ((): T => ({} as T)),
    getSchemaVersion: options.currentSchemaVersion ? ((data: T): number => (data as { schemaVersion?: number }).schemaVersion ?? 1) : undefined,
    label: "test-mem-" + path.basename(filePath),
    migrations: options.migrations,
    parse: options.parse ?? ((raw: string): T => JSON.parse(raw) as T),
    path: (): string => filePath,
    setSchemaVersion: options.currentSchemaVersion ?
      ((data: T, version: number): void => { (data as { schemaVersion?: number }).schemaVersion = version; }) :
      undefined,
    validate: options.validate
  });
}
