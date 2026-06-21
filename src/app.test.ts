/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * app.test.ts: Unit tests for the Express application builder module. Almost everything in app.ts is wired into a process-level lifecycle - the HTTP server,
 * the Chrome browser, the file logger, the SIGINT/SIGTERM handlers, the polling intervals - so the surface that can be exercised in isolation is small. The
 * module exports only two symbols: releaseInstanceSlot and startServer. startServer cannot be invoked safely from a unit test (it spawns Chrome, binds the
 * port, registers signal handlers, and calls process.exit on failure), so it is deferred to e2e coverage. releaseInstanceSlot is exercised here against the
 * critical-correctness path: a process that does NOT own the identity file must leave it alone. The ownership check is structural (release() reads the file
 * record and refuses to remove a file whose PID does not match this process), a self-evident invariant that survives across module loads.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { closePuppeteerStreamWssOnIdle, withTempDir } from "./testing.helpers.ts";
import { existsSync, writeFileSync } from "node:fs";
import { getServerPidFilePath, initializeDataDir } from "./config/paths.ts";
import assert from "node:assert/strict";
import { releaseInstanceSlot } from "./app.ts";
import { serializeRecord } from "./utils/index.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* The data-dir state and the PRISMCAST_DATA_DIR env var are module-level. We capture and restore the surrounding values so the suite leaves the global state
 * exactly as it found it. Each test scopes its own data directory via withTempDir + initializeDataDir.
 */
const ORIGINAL_ENV = process.env["PRISMCAST_DATA_DIR"];

beforeEach(() => {

  delete process.env["PRISMCAST_DATA_DIR"];
});

afterEach(() => {

  if(ORIGINAL_ENV === undefined) {

    delete process.env["PRISMCAST_DATA_DIR"];
  } else {

    process.env["PRISMCAST_DATA_DIR"] = ORIGINAL_ENV;
  }
});

describe("releaseInstanceSlot", () => {

  test("is exported as a callable function", () => {

    assert.equal(typeof releaseInstanceSlot, "function", "releaseInstanceSlot should be exported");
  });

  test("does not throw when there is no identity file on disk", () => {

    // A never-claimed process running its exit handler must early-return cleanly. The state machine inside release() classifies a missing file as "free" and
    // skips the unlink entirely.
    assert.doesNotThrow(() => {

      releaseInstanceSlot();
    }, "releaseInstanceSlot should be a safe no-op when no identity file exists");
  });

  test("is idempotent on repeated calls", () => {

    // Repeated invocation is the realistic scenario: the process exit handler may run after a graceful shutdown that already called releaseInstanceSlot, and
    // the function must not throw or otherwise misbehave on the second pass.
    assert.doesNotThrow(() => {

      releaseInstanceSlot();
      releaseInstanceSlot();
      releaseInstanceSlot();
    }, "three back-to-back calls should all be silent no-ops");
  });

  test("does NOT delete a pre-existing identity file owned by another live process (rejected-duplicate safety)", async () => {

    // Sentinel test: the ownership check exists specifically so that a duplicate-instance rejection cannot delete the running instance's identity file via its
    // exit handler. We write a well-formed record at the server identity path whose PID is NOT this process - simulating the legitimate holder's record - then
    // call releaseInstanceSlot from this process. release() inspects the file, finds the PID mismatch, and leaves the file untouched.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      const pidPath = getServerPidFilePath();
      const otherPid = process.pid === 99999 ? 99998 : 99999;

      // The bootId here does not matter for the test - whether the state classifies as held-live, stale-different-boot, or stale-dead-pid, the structural
      // ownership check (pid match) is the gate that protects the file. Using process.pid + 1 ensures we are definitely not the holder.
      writeFileSync(pidPath, serializeRecord({ bootId: "any-boot", pid: otherPid, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }), "utf-8");

      assert.equal(existsSync(pidPath), true, "sentinel record exists before the call");

      releaseInstanceSlot();

      assert.equal(existsSync(pidPath), true, "sentinel record must still exist after releaseInstanceSlot (ownership check held)");

      return Promise.resolve();
    });
  });

  test("does NOT throw when invoked before initializeDataDir has been called for this test", () => {

    // release() reads the file before checking ownership, so it must tolerate the absence of a configured data directory. ENOENT (or any path error) from the
    // unconfigured state resolves to kind: "free" or kind: "stale-malformed", both of which short-circuit before the unlink path.
    assert.doesNotThrow(() => {

      releaseInstanceSlot();
    }, "the call must not depend on initializeDataDir being called first");
  });
});

/* startServer is intentionally not tested here. It launches Chrome via puppeteer-core, binds the configured port, registers process-level signal handlers,
 * spawns ffmpeg children, and may call process.exit on failure - any of which is incompatible with a unit-test context. The integration tier covers it via the
 * test/e2e/ harness.
 */
