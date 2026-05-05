/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * app.test.ts: Unit tests for the Express application builder module. Almost everything in app.ts is wired into a process-level lifecycle - the HTTP server,
 * the Chrome browser, the file logger, the SIGINT/SIGTERM handlers, the polling intervals - so the surface that can be exercised in isolation is small. The
 * module exports only two symbols: clearServerPid and startServer. startServer cannot be invoked safely from a unit test (it spawns Chrome, binds the port,
 * registers signal handlers, and calls process.exit on failure), so it is deferred to e2e coverage. clearServerPid is exercised here against the only
 * externally-observable starting state - ownership flag false - which is the path traversed when a duplicate-instance rejection or a never-started process
 * runs through its exit handler.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { closePuppeteerStreamWssOnIdle, withTempDir } from "./testing.helpers.ts";
import { existsSync, writeFileSync } from "node:fs";
import { getServerPidFilePath, initializeDataDir } from "./config/paths.ts";
import assert from "node:assert/strict";
import { clearServerPid } from "./app.ts";

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

describe("clearServerPid", () => {

  test("is exported as a callable function", () => {

    assert.equal(typeof clearServerPid, "function", "clearServerPid should be exported");
  });

  test("does not throw when invoked without a prior saveServerPid (ownership flag is false)", () => {

    // The internal ownsServerPid flag starts at false in a fresh module load. saveServerPid is the only path that flips it to true, and saveServerPid is not
    // exported - so from any external caller the only observable starting state is "not owning". The function must early-return cleanly in that state.
    assert.doesNotThrow(() => {

      clearServerPid();
    }, "clearServerPid should be a safe no-op when ownership is false");
  });

  test("is idempotent on repeated calls when not owning the PID file", () => {

    // Repeated invocation is the realistic scenario: the process exit handler may run after a graceful shutdown that already called clearServerPid, and the
    // function must not throw or otherwise misbehave on the second pass.
    assert.doesNotThrow(() => {

      clearServerPid();
      clearServerPid();
      clearServerPid();
    }, "three back-to-back calls should all be silent no-ops");
  });

  test("does NOT delete a pre-existing PID file on disk when ownership flag is false", async () => {

    // Sentinel test: the ownership guard exists specifically so that a duplicate-instance rejection cannot delete the running instance's PID file via its exit
    // handler. We simulate a "running instance owns the file" scenario by writing a sentinel at the server PID path, then call clearServerPid from this process
    // (which has never called saveServerPid and therefore must not own anything). The sentinel must survive untouched.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      const pidPath = getServerPidFilePath();

      writeFileSync(pidPath, "12345", "utf-8");

      assert.equal(existsSync(pidPath), true, "sentinel PID file exists before the call");

      clearServerPid();

      assert.equal(existsSync(pidPath), true, "sentinel PID file must still exist after clearServerPid (ownership guard held)");

      return Promise.resolve();
    });
  });

  test("does NOT throw when invoked before initializeDataDir has been called for this test", () => {

    // The ownership guard short-circuits before any call to getServerPidFilePath(), so the absence of a resolved data directory is irrelevant when the flag is
    // false. Locking this contract documents that the guard runs first and the path resolution is reached only on the owning path.
    assert.doesNotThrow(() => {

      clearServerPid();
    }, "the early ownership return must not depend on initializeDataDir being called first");
  });
});

/* startServer is intentionally not tested here. It launches Chrome via puppeteer-core, binds the configured port, registers process-level signal handlers,
 * starts seven recurring intervals (idle cleanup, browser restart checking, stale page cleanup, pretune polling, show info polling, update checking, HDHR
 * server), runs file-based persistence migrations, and on most failure paths invokes process.exit(1). None of those are safe to invoke from a unit test
 * runner. The function's behavior is exercised by the e2e suite, which spawns the full process and asserts on the listening port and the served endpoints.
 */
