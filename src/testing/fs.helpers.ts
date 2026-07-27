/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fs.helpers.ts: Filesystem-scoped test helpers. Exposes withTempDir, the canonical scope for tests that need a temporary directory with guaranteed cleanup on
 * failure or success. Equivalent to a `using` block for filesystem state.
 */
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

/**
 * Creates a fresh temporary directory under os.tmpdir(), runs the callback with the path, and removes the directory when the callback resolves or rejects. Use
 * this in tests that touch the filesystem - it guarantees cleanup even if the test fails. Equivalent to a `using` block for filesystem state.
 * @param fn - Callback receiving the absolute path of the temp directory.
 * @returns The callback's return value.
 */
export async function withTempDir<T>(fn: (dir: string) => Promise<T>): Promise<T> {

  const dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-test-"));

  try {

    return await fn(dir);
  } finally {

    await rm(dir, { force: true, recursive: true });
  }
}
