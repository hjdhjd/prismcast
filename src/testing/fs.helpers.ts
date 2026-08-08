/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fs.helpers.ts: Filesystem-scoped test helpers. Exposes withTempDir, the canonical scope for tests that need a temporary directory with guaranteed cleanup on
 * failure or success. Equivalent to a `using` block for filesystem state.
 *
 * Customization: TMPDIR_PREFIX is the only project-specific value. Replace "test-" with "<your-project>-test-" so orphaned temp dirs in os.tmpdir() are
 * trivially identifiable.
 */
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

/**
 * Prefix for temp directory names. Customize once per project; tests import this constant rather than hardcoding the literal so a rename does not require
 * touching every assertion.
 */
export const TMPDIR_PREFIX = "prismcast-test-";

/**
 * Creates a fresh temporary directory under os.tmpdir(), runs the callback with the path, and removes the directory when the callback resolves or rejects. Use
 * this in tests that touch the filesystem - it guarantees cleanup even if the test fails. Equivalent to a `using` block for filesystem state.
 * @param fn - Callback receiving the absolute path of the temp directory.
 * @returns The callback's return value.
 */
export async function withTempDir<T>(fn: (dir: string) => Promise<T>): Promise<T> {

  const dir = await mkdtemp(path.join(os.tmpdir(), TMPDIR_PREFIX));

  try {

    return await fn(dir);
  } finally {

    await rm(dir, { force: true, recursive: true });
  }
}
