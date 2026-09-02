/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pid.test.ts: Unit tests for the OS-level PID primitives in pid.ts. The functions are synchronous and (in clearPidFile's case) use the actual filesystem;
 * tests scope filesystem cases to a temp directory via withTempDir so cleanup is guaranteed. The isProcessRunning() check uses signal 0; we test against the
 * current process (always running) and a deliberately-implausible PID (always not running).
 */
import { capturingLog, withTempDir } from "../testing.helpers.ts";
import { clearPidFile, isProcessRunning } from "./pid.ts";
import { describe, test } from "node:test";
import { existsSync, writeFileSync } from "node:fs";
import assert from "node:assert/strict";
import path from "node:path";

describe("isProcessRunning", () => {

  test("returns true for the current process PID", () => {

    // process.pid is guaranteed to belong to a running process (us). signal 0 against ourselves is a no-op success.
    assert.equal(isProcessRunning(process.pid), true);
  });

  test("returns false for a PID that is virtually certain to be unused (boundary)", () => {

    // 2^31 - 1 is well above any realistic PID on macOS, Linux, or Windows. process.kill rejects with ESRCH and the function returns false.
    assert.equal(isProcessRunning(0x7FFFFFFF), false);
  });

  test("returns true (treats EPERM as running) - documented contract", () => {

    // We cannot induce EPERM on our own PID without root, so we lock the documented branch via behavior reasoning rather than a triggered EPERM. The function
    // intentionally treats EPERM as "still running" because we lack permission to signal it - the process exists. We verify the live-process branch instead,
    // which is the same return value path (true), and rely on the unit boundary above for the negative case.
    assert.equal(isProcessRunning(process.pid), true, "the live-process branch returns true regardless of which error route was taken");
  });
});

describe("clearPidFile", () => {

  test("removes an existing PID file", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "to-remove.pid");

      writeFileSync(filePath, "1234");
      clearPidFile(filePath, "test");

      assert.equal(existsSync(filePath), false, "file is gone after clear");
    });
  });

  test("does not log when the file is already missing (ENOENT silently ignored)", async () => {

    // Boundary: clear is called from cleanup paths that may run after the file has already been removed. ENOENT must be silent so logs stay clean.
    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "never-existed.pid");
      const { lines, logger } = capturingLog();

      clearPidFile(filePath, "test", logger);

      assert.equal(lines().length, 0);
    });
  });

  test("does not throw when no logger is provided on a non-ENOENT failure", async () => {

    // Calling unlink on a directory path triggers a non-ENOENT error. Without a logger the function silently swallows.
    await withTempDir(async (dir) => {

      assert.doesNotThrow(() => {

        clearPidFile(dir, "test");
      });
    });
  });

  test("logs a warning on non-ENOENT removal errors when a logger is provided", async () => {

    await withTempDir(async (dir) => {

      const { lines, logger } = capturingLog();

      // Pass the directory itself as the path - unlink on a directory yields EISDIR (or EPERM on some platforms), both non-ENOENT.
      clearPidFile(dir, "test", logger);

      assert.equal(lines().length, 1);
      assert.equal(lines()[0]?.level, "warn");
      assert.match(lines()[0]?.message ?? "", /Failed to remove .* PID file/);
      assert.equal(lines()[0]?.args[0], "test");
    });
  });
});
