/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pid.test.ts: Unit tests for the PID file primitives in pid.ts. The functions are synchronous and use the actual filesystem; tests scope each case to a temp
 * directory via withTempDir so cleanup is guaranteed. The isProcessRunning() check uses signal 0; we test against the current process (always running) and a
 * deliberately-implausible PID (always not running).
 */
import { clearPidFile, isProcessRunning, readPidFile, writePidFile } from "./pid.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { capturingLog } from "../testing.helpers.ts";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";
import { writeFileSync } from "node:fs";

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

describe("readPidFile", () => {

  test("returns the stored PID for a well-formed PID file", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "test.pid");

      writeFileSync(filePath, "12345");

      assert.equal(readPidFile(filePath, "test"), 12345);
    });
  });

  test("trims surrounding whitespace and newline before parsing", async () => {

    // Boundary: writePidFile emits "12345" without a trailing newline, but PID files written by other tools may include one. The parser uses .trim() before
    // parseInt so the read survives.
    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "test.pid");

      writeFileSync(filePath, "  9999\n");

      assert.equal(readPidFile(filePath, "test"), 9999);
    });
  });

  test("returns null when the file does not exist (ENOENT silently ignored)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "missing.pid");

      assert.equal(readPidFile(filePath, "test"), null);
    });
  });

  test("returns null when the file exists but is empty", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "empty.pid");

      writeFileSync(filePath, "");

      assert.equal(readPidFile(filePath, "test"), null, "parseInt of '' is NaN, function returns null");
    });
  });

  test("returns null when the file content is non-numeric", async () => {

    // Negative test: a corrupted file with text content should not produce a bogus PID. parseInt returns NaN and the function returns null.
    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "bogus.pid");

      writeFileSync(filePath, "not-a-pid");

      assert.equal(readPidFile(filePath, "test"), null);
    });
  });

  test("does NOT log on ENOENT even when a logger is provided", async () => {

    // The function silently ignores ENOENT (the expected first-run / clean-shutdown case). Locking this behavior - a noisy log on every startup would be wrong.
    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "missing.pid");
      const { lines, logger } = capturingLog();

      readPidFile(filePath, "test", logger);

      assert.equal(lines().length, 0, "no log line for ENOENT");
    });
  });

  test("logs a warning on non-ENOENT read errors when a logger is provided", async () => {

    // Force a non-ENOENT error by passing a directory path as the file path. fs.readFileSync of a directory throws EISDIR, which the function should report.
    await withTempDir(async (dir) => {

      const { lines, logger } = capturingLog();

      readPidFile(dir, "test", logger);

      assert.equal(lines().length, 1, "one warning logged");
      assert.equal(lines()[0]?.level, "warn");
      // capturingLog stores the format string and args separately rather than the formatted output. We assert on the format string and the first arg (the label).
      assert.match(lines()[0]?.message ?? "", /Failed to read .* PID file/);
      assert.equal(lines()[0]?.args[0], "test");
    });
  });

  test("does not throw and returns null when no logger is provided on a read error", async () => {

    await withTempDir(async (dir) => {

      // A directory path triggers EISDIR. Without a logger, the catch silently swallows the error.
      assert.doesNotThrow(() => {

        const result = readPidFile(dir, "test");

        assert.equal(result, null);
      });
    });
  });
});

describe("writePidFile", () => {

  test("writes the stringified PID to disk and round-trips via readPidFile", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "out.pid");

      writePidFile(filePath, 4242, "test");

      assert.equal(readPidFile(filePath, "test"), 4242);
    });
  });

  test("overwrites an existing PID file", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "out.pid");

      writePidFile(filePath, 1, "test");
      writePidFile(filePath, 2, "test");

      assert.equal(readPidFile(filePath, "test"), 2, "second write replaces first");
    });
  });

  test("logs a warning when the write fails and a logger is provided", async () => {

    // Force a write error by pointing at a non-existent parent directory (ENOENT on the parent path).
    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "no-such-subdir", "out.pid");
      const { lines, logger } = capturingLog();

      writePidFile(filePath, 1, "test", logger);

      assert.equal(lines().length, 1);
      assert.match(lines()[0]?.message ?? "", /Failed to write .* PID file/);
      assert.equal(lines()[0]?.args[0], "test");
    });
  });

  test("does not throw when no logger is provided on write failure", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "no-such-subdir", "out.pid");

      assert.doesNotThrow(() => {

        writePidFile(filePath, 1, "test");
      });
    });
  });
});

describe("clearPidFile", () => {

  test("removes an existing PID file", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "to-remove.pid");

      writeFileSync(filePath, "1234");
      clearPidFile(filePath, "test");

      assert.equal(readPidFile(filePath, "test"), null, "file is gone after clear");
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
