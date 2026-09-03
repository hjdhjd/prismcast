/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * app.test.ts: Unit tests for the Express application builder module. Almost everything in app.ts is wired into a process-level lifecycle - the HTTP server,
 * the Chrome browser, the file logger, the SIGINT/SIGTERM handlers, the polling intervals - so the surface that can be exercised in isolation is small. The
 * module exports only two symbols: releaseInstanceSlot and startServer. startServer cannot be invoked safely from a unit test (it spawns Chrome, binds the
 * port, registers signal handlers, and calls process.exit on failure), so it is deferred to e2e coverage. releaseInstanceSlot is exercised here against the
 * critical-correctness path: a process that does NOT own the identity file must leave it alone. The ownership check is structural (release() reads the file
 * record and refuses to remove a file whose PID does not match this process), and that guarantee holds no matter how the module graph was loaded.
 *
 * The HTTP request-logging rules are the second surface tested here. The two skip predicates and the elapsed-time renderer are pure of the Express plumbing - they
 * take a plain record or a request object and return a decision - so the whole filtered/errors rule set is exercised without binding a port or booting the server.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { closePuppeteerStreamWssOnIdle, withTempDir } from "./testing.helpers.ts";
import { elapsedMillis, releaseInstanceSlot, skipInErrorsMode, skipInFilteredMode, stampRequestStart } from "./app.ts";
import { existsSync, writeFileSync } from "node:fs";
import { getServerPidFilePath, initializeDataDir } from "./config/paths.ts";
import type { IncomingMessage } from "node:http";
import assert from "node:assert/strict";
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

    // This is the first call to releaseInstanceSlot() in the process, before initializeDataDir() has ever run, so getServerPidFilePath() throws while
    // resolving the data directory. releaseInstanceSlot()'s own try/catch in app.ts, not release()'s internal file-state handling, swallows that throw and
    // lets the exit handler return cleanly.
    assert.doesNotThrow(() => {

      releaseInstanceSlot();
    }, "releaseInstanceSlot should be a safe no-op when no identity file exists");
  });

  test("is a no-op on repeated calls", () => {

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
    // exit handler. We write a well-formed record at the server identity path that does not identify this process - simulating the legitimate holder's
    // record - then call releaseInstanceSlot from this process. release() classifies the record as not ours (its boot session or PID does not match) and
    // leaves the file untouched.
    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      const pidPath = getServerPidFilePath();
      const otherPid = process.pid === 99999 ? 99998 : 99999;

      // The fixed bootId "any-boot" never equals this process's real boot session id, so release() classifies the record as not ours - a boot-session or PID
      // mismatch - and leaves the file untouched regardless of which branch of the state machine the record lands in. The fixed sentinel PID (99999, or
      // 99998 when this process happens to be 99999) keeps the record unambiguously not-this-process on the PID axis as well.
      writeFileSync(pidPath, serializeRecord({ bootId: "any-boot", pid: otherPid, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }), "utf-8");

      assert.equal(existsSync(pidPath), true, "sentinel record exists before the call");

      releaseInstanceSlot();

      assert.equal(existsSync(pidPath), true, "sentinel record must still exist after releaseInstanceSlot (ownership check held)");

      return Promise.resolve();
    });
  });

  test("does NOT throw when invoked before initializeDataDir has been called for this test", () => {

    // By this point in the file, the prior test's initializeDataDir() call already set config/paths.ts's resolved data directory; withTempDir removed that
    // temp directory afterward but left the module-level resolution in place, so the path this test resolves to no longer exists on disk. release() reads
    // that missing file and inspect() treats the resulting ENOENT as kind: "free", short-circuiting before the unlink path the same way a genuinely
    // unconfigured data directory would.
    assert.doesNotThrow(() => {

      releaseInstanceSlot();
    }, "the call must not depend on initializeDataDir being called first");
  });
});

/* startServer is intentionally not tested here. It launches Chrome via puppeteer-core, binds the configured port, registers process-level signal handlers,
 * spawns ffmpeg children, and may call process.exit on failure - any of which is incompatible with a unit-test context. The integration tier covers it via the
 * test/e2e/ harness.
 */

describe("skipInErrorsMode", () => {

  test("skips every successful response", () => {

    // Errors mode logs 4xx and 5xx only, so a 200 on an endpoint the other mode would always log is still skipped here.
    assert.equal(skipInErrorsMode({ hasRetryAfter: false, statusCode: 200, url: "/stream/nbc" }), true);
  });

  test("skips a 404 for a browser-initiated asset request", () => {

    // Browsers request these on their own; a 404 for one is noise rather than a fault worth a log line.
    assert.equal(skipInErrorsMode({ hasRetryAfter: false, statusCode: 404, url: "/favicon.ico" }), true);
  });

  test("logs a 404 for a path the browser did not ask for on its own", () => {

    // The asset patterns are prefixes, so a 404 outside them is a genuine miss and is logged.
    assert.equal(skipInErrorsMode({ hasRetryAfter: false, statusCode: 404, url: "/hls/nbc/stream.m3u8" }), false);
  });

  test("skips a 503 that carries Retry-After", () => {

    // A 503 with Retry-After announces an unavailability the server chose - a stream still starting up - rather than a fault.
    assert.equal(skipInErrorsMode({ hasRetryAfter: true, statusCode: 503, url: "/hls/nbc/stream.m3u8" }), true);
  });

  test("logs a 503 with no Retry-After", () => {

    // Without the header the 503 is an unexplained failure, so it stays in the log.
    assert.equal(skipInErrorsMode({ hasRetryAfter: false, statusCode: 503, url: "/hls/nbc/stream.m3u8" }), false);
  });
});

describe("skipInFilteredMode", () => {

  test("logs a slow request even on an endpoint the filter otherwise skips", () => {

    /* The detector for the elapsed rule. /logs is a high-frequency polling endpoint the filter suppresses when it is fast, so a slow one is logged only because
     * the elapsed reading outranks the skip pattern. Removing the elapsed rule reds exactly this row.
     */
    assert.equal(skipInFilteredMode({ elapsedMs: 1500, statusCode: 200, url: "/logs" }), false);
  });

  test("skips a fast request on a high-frequency polling endpoint", () => {

    // The complement of the row above: the same endpoint, under the threshold, is suppressed.
    assert.equal(skipInFilteredMode({ elapsedMs: 5, statusCode: 200, url: "/logs" }), true);
  });

  test("logs an error whatever its path and whatever its elapsed time", () => {

    // The error rule runs before the elapsed rule, so a fast 500 on a suppressed endpoint is still logged.
    assert.equal(skipInFilteredMode({ elapsedMs: 5, statusCode: 500, url: "/logs" }), false);
  });

  test("logs a fast request to a streaming or management endpoint", () => {

    // These endpoints mark what the server is doing, so they are logged regardless of speed.
    assert.equal(skipInFilteredMode({ elapsedMs: 2, statusCode: 200, url: "/config" }), false);
  });

  test("skips a fast successful request to the root landing page", () => {

    // The landing page is exact-matched, not prefixed, so only "/" itself is suppressed.
    assert.equal(skipInFilteredMode({ elapsedMs: 2, statusCode: 200, url: "/" }), true);
  });

  test("logs anything that matches no rule", () => {

    // The default is to log: a path outside every pattern list is reported.
    assert.equal(skipInFilteredMode({ elapsedMs: 2, statusCode: 200, url: "/some/other/path" }), false);
  });

  test("treats a request with no elapsed reading as not slow and lets the remaining rules decide", () => {

    // A request that reached the logger without a stamp has no time to compare, so the skip-pattern rule decides it rather than an assumed zero or infinity.
    assert.equal(skipInFilteredMode({ elapsedMs: null, statusCode: 200, url: "/logs" }), true);
  });

  test("does not treat a request exactly at the threshold as slow", () => {

    // The comparison is strictly greater than, so a request landing exactly on the threshold falls through to the pattern rules.
    assert.equal(skipInFilteredMode({ elapsedMs: 1000, statusCode: 200, url: "/logs" }), true);
  });
});

describe("elapsedMillis", () => {

  test("renders an empty string for a request that carries no start stamp", () => {

    // A request that never passed the stamping middleware has no time to report, and an empty rendering leaves the log line's shape intact.
    assert.equal(elapsedMillis({} as IncomingMessage), "");
  });

  test("renders three decimal places for a stamped request, matching morgan's own timing shape", () => {

    /* The stamp and the rendering are asserted together because they are one mechanism: stampRequestStart writes the monotonic start and elapsedMillis reads it.
     * The value itself is timing-dependent, so the assertions are on the shape and on it being a real non-negative reading rather than on a fixed number.
     */
    const request = {} as IncomingMessage;

    let nextCalls = 0;

    stampRequestStart(request, null, () => { nextCalls++; });

    const rendered = elapsedMillis(request);

    assert.equal(nextCalls, 1, "the stamping middleware passes the request along exactly once");
    assert.match(rendered, /^\d+\.\d{3}$/, "the rendering carries three decimals, as morgan's own timing token does");
    assert.ok(Number(rendered) >= 0, "a monotonic source cannot produce a negative elapsed reading");
  });
});
