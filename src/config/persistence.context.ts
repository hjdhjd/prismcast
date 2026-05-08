/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.context.ts: The default storage backend for the persistence framework. The framework in persistence.ts is backend-agnostic - every operation it
 * performs (atomic temp+rename, backup copy, snapshot copy, post-write readback) is expressible against any durable store that exposes stat/read/write/copy/
 * rename/unlink with a path namespace. This file is the only place in the persistence module that talks to node:fs/promises, isolating the real-filesystem
 * implementation behind a thin adapter so the framework's public surface (createFileStore) remains expressible against any conforming backend.
 *
 * Production stores get this backend implicitly via the createFileStore default parameter. Alternative backends (an in-memory backend used during tests, a
 * future object-store backend) are passed explicitly via FileStoreOptions.backend. The shape of the wired functions is intentionally identical to the
 * StorageBackend interface in persistence.ts - any drift between the two is a typecheck error at construction time.
 */
import type { StorageBackend } from "./persistence.ts";
import fs from "node:fs";

const { promises: fsPromises } = fs;

/**
 * Builds the default StorageBackend wired to node:fs/promises. The returned adapter is stateless - every call delegates to fsPromises with the framework's
 * documented contract (UTF-8 encoding for text I/O, recursive mkdir, native error propagation including the ENOENT code that the framework's branch detection
 * relies on). Sharing one default instance across every store in the process is safe and cheap; persistence.ts caches a single one at module load.
 * @returns A StorageBackend implementation backed by the real filesystem via node:fs/promises.
 */
export function createDefaultStorageBackend(): StorageBackend {

  return {

    access: async (path: string): Promise<void> => fsPromises.access(path),

    copyFile: async (source: string, destination: string): Promise<void> => fsPromises.copyFile(source, destination),

    mkdir: async (path: string): Promise<void> => {

      // The framework only ever uses recursive mkdir, so the adapter bakes that in rather than exposing the options bag through the StorageBackend surface.
      await fsPromises.mkdir(path, { recursive: true });
    },

    readFile: async (path: string): Promise<string> => fsPromises.readFile(path, "utf-8"),

    readdir: async (path: string): Promise<string[]> => fsPromises.readdir(path),

    rename: async (source: string, destination: string): Promise<void> => fsPromises.rename(source, destination),

    stat: async (path: string): Promise<{ mtimeMs: number }> => {

      // Project Stats down to the field the framework actually uses (mtimeMs for snapshot pruning). Hides the wider Stats surface so backend implementers do not
      // have to fabricate fields that the framework would never read.
      const result = await fsPromises.stat(path);

      return { mtimeMs: result.mtimeMs };
    },

    unlink: async (path: string): Promise<void> => fsPromises.unlink(path),

    writeFile: async (path: string, content: string): Promise<void> => fsPromises.writeFile(path, content, "utf-8")
  };
}
