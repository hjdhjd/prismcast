/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * app.test.ts: Unit tests for the Express application builder module. Almost everything in app.ts is wired into a process-level lifecycle - the HTTP server,
 * the Chrome browser, the file logger, the SIGINT/SIGTERM handlers, the polling intervals - so the surface that can be exercised in isolation is small. The
 * module exports only two symbols: releaseInstanceSlot and startServer. startServer cannot be invoked safely from a unit test (it spawns Chrome, binds the
 * port, registers signal handlers, and calls process.exit on failure), so it is deferred to e2e coverage. releaseInstanceSlot is exercised here against the
 * critical-correctness path: a process that does NOT own the identity file must leave it alone. The ownership check is structural (release() reads the file
 * record and refuses to remove a file whose PID does not match this process), and that guarantee holds no matter how the module graph was loaded.
 *
 * The HTTP request-logging rules are the second surface tested here. The skip predicates, the per-level decision and the elapsed-time renderer are pure of the
 * Express plumbing - they take a plain record or a request object and return a decision - so every level's rule set is exercised without booting the server.
 * The request logger itself runs on a bare Express app bound to a loopback port with a stub stream, so a level change is observed request by request.
 *
 * The log size handler is the third surface. Its rows open the file logger on a file of their own and hand the handler the candidate a save would, so the
 * trim it starts and the save it never holds up are each observed on disk.
 */
import type { IncomingMessage, Server } from "node:http";
import type { PathLike, Stats } from "node:fs";
import { TestClock, settle, waitUntil } from "homebridge-plugin-utils/testing";
import { after, afterEach, before, beforeEach, describe, test } from "node:test";
import { applyLogSizeChanges, createRequestLogger, elapsedMillis, releaseInstanceSlot, skipInErrorsMode, skipInFilteredMode, skipRequestLog,
  stampRequestStart } from "./app.ts";
import { closePuppeteerStreamWssOnIdle, withTempDir } from "./testing.helpers.ts";
import { existsSync, promises, statSync, writeFileSync } from "node:fs";
import { getServerPidFilePath, initializeDataDir } from "./config/paths.ts";
import { initializeFileLogger, shutdownFileLogger } from "./utils/fileLogger.ts";
import type { AddressInfo } from "node:net";
import { CONFIG } from "./config/index.ts";
import type { Config } from "./types/index.ts";
import type { ConfigChange } from "./config/reactivity.ts";
import { HTTP_LOG_LEVELS } from "./types/index.ts";
import assert from "node:assert/strict";
import express from "express";
import { initDebugFilter } from "./utils/debugFilter.ts";
import { join } from "node:path";
import { registerConfigChangeHandler } from "./config/reactivity.ts";
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

describe("skipRequestLog", () => {

  /* Every pair of levels decides at least one of these requests differently, so a level routed to another level's rule fails a row. Each expected answer is a
   * literal read from the mode rules above, never computed by calling them, and the table is keyed by every level, so a level added later cannot compile until
   * its answers are written here.
   */
  const REQUESTS: readonly { readonly expected: Readonly<Record<typeof HTTP_LOG_LEVELS[number], boolean>>; readonly name: string;
    readonly request: { readonly elapsedMs: number; readonly hasRetryAfter: boolean; readonly statusCode: number; readonly url: string }; }[] = [

    {

      expected: { all: false, errors: true, filtered: false, none: true },
      name: "a fast success on a management endpoint",
      request: { elapsedMs: 2, hasRetryAfter: false, statusCode: 200, url: "/config" }
    },
    {

      expected: { all: false, errors: false, filtered: false, none: true },
      name: "a fast error on a polling endpoint",
      request: { elapsedMs: 2, hasRetryAfter: false, statusCode: 500, url: "/logs" }
    },
    {

      expected: { all: false, errors: true, filtered: true, none: true },
      name: "a fast success on a polling endpoint",
      request: { elapsedMs: 2, hasRetryAfter: false, statusCode: 200, url: "/logs" }
    },
    {

      expected: { all: false, errors: true, filtered: false, none: true },
      name: "a 404 for a browser-initiated asset",
      request: { elapsedMs: 2, hasRetryAfter: false, statusCode: 404, url: "/favicon.ico" }
    }
  ];

  for(const level of HTTP_LOG_LEVELS) {

    test("the " + level + " level decides each request by its own rule", () => {

      for(const { expected, name, request } of REQUESTS) {

        assert.equal(skipRequestLog({ ...request, level }), expected[level], name + " at the " + level + " level");
      }
    });
  }
});

/* The request logger on a bare Express app with a stub stream. Each route resolves the finish signal from a listener it registers after morgan's, and morgan makes
 * its skip decision synchronously as the response finishes, so once a row holds that signal the logger has written its line or decided not to.
 */
describe("createRequestLogger", () => {

  const ORIGINAL_LEVEL = CONFIG.logging.httpLogLevel;
  const lines: string[] = [];
  let baseUrl = "";
  let entered = Promise.withResolvers<null>();
  let finished = Promise.withResolvers<null>();
  let release = Promise.withResolvers<null>();
  let server: Server;

  /**
   * Requests a path and waits until its response has finished on the server, so the logger has made its decision.
   * @param path - The path to request.
   */
  async function requestAndFinish(path: string): Promise<void> {

    finished = Promise.withResolvers<null>();

    const response = await fetch(baseUrl + path);

    await response.text();
    await finished.promise;
  }

  before(async () => {

    const app = express();
    const listening = Promise.withResolvers<null>();

    app.use(createRequestLogger({ write: (line: string): void => { lines.push(line); } }));

    app.get("/ping", (_req, res) => {

      res.once("finish", () => { finished.resolve(null); });
      res.send("ok");
    });

    // The held route answers only once the row releases it, so a row can change the level between a request's arrival and its finish.
    app.get("/hold", (_req, res) => {

      res.once("finish", () => { finished.resolve(null); });
      entered.resolve(null);
      void release.promise.then(() => { res.send("ok"); });
    });

    server = app.listen(0, "127.0.0.1", () => { listening.resolve(null); });
    await listening.promise;
    baseUrl = "http://127.0.0.1:" + String((server.address() as AddressInfo).port);
  });

  beforeEach(() => {

    lines.length = 0;
    entered = Promise.withResolvers<null>();
    release = Promise.withResolvers<null>();
  });

  afterEach(() => {

    CONFIG.logging.httpLogLevel = ORIGINAL_LEVEL;
  });

  after(async () => {

    const closed = Promise.withResolvers<null>();

    server.closeAllConnections();
    server.close(() => { closed.resolve(null); });
    await closed.promise;
  });

  test("a request under none writes no line", async () => {

    CONFIG.logging.httpLogLevel = "none";

    await requestAndFinish("/ping");

    assert.deepEqual(lines, []);
  });

  test("after the level changes to all, the next request writes one line", async () => {

    CONFIG.logging.httpLogLevel = "none";

    await requestAndFinish("/ping");

    assert.equal(lines.length, 0, "precondition: the request under none wrote nothing");

    CONFIG.logging.httpLogLevel = "all";

    await requestAndFinish("/ping");

    assert.equal(lines.length, 1, "the next request wrote one line");
    assert.match(lines[0] ?? "", /^GET \/ping from \S+ responded 200 in \d+\.\d{3} ms\.\n$/, "the line carries the elapsed time the arrival stamp recorded");
  });

  test("a request that arrives under all and finishes after the level changes to none writes no line", async () => {

    CONFIG.logging.httpLogLevel = "all";
    finished = Promise.withResolvers<null>();

    const response = fetch(baseUrl + "/hold");

    await entered.promise;

    CONFIG.logging.httpLogLevel = "none";
    release.resolve(null);

    await (await response).text();
    await finished.promise;

    assert.deepEqual(lines, []);
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

/* The log size handler at the composition root. This suite opens no file logger, and a closed logger returns before its stat, so each row opens one on a file
 * of its own, seeds that file above the saved limit with the debug filter cleared, and shuts the logger down before its directory goes. CONFIG holds the running
 * limit and the candidate the saved one, so a handler that read CONFIG would trim nothing.
 */
describe("applyLogSizeChanges", () => {

  const ORIGINAL_LIMIT = CONFIG.logging.maxSize;
  const RUNNING_LIMIT = 1048576;
  const SAVED_LIMIT = 524288;
  const SEED_CONTENT = "[2026/01/01 12:00:00.000 PM] A seeded history line.\n".repeat(12000);
  const CHANGE: ConfigChange = { current: SAVED_LIMIT, path: "logging.maxSize", previous: RUNNING_LIMIT };

  // The candidate a save hands the handler: the running configuration with the saved limit applied, while CONFIG still holds the running one.
  const makeNext = (): Config => {

    const next = structuredClone(CONFIG);

    next.logging.maxSize = SAVED_LIMIT;

    return next;
  };

  beforeEach(() => {

    CONFIG.logging.maxSize = RUNNING_LIMIT;
    initDebugFilter("");
  });

  afterEach(() => {

    CONFIG.logging.maxSize = ORIGINAL_LIMIT;
  });

  test("registering a second handler for the log size limit throws, because the module registered its own at load", () => {

    assert.throws(() => { registerConfigChangeHandler("logging.maxSize", applyLogSizeChanges); },
      { message: "A config change handler is already registered for prefix \"logging.maxSize\"." });
  });

  test("a smaller limit in the candidate trims the open file to at most half of it", async () => {

    await withTempDir(async (dir) => {

      const logPath = join(dir, "handler.log");

      await initializeFileLogger(logPath, RUNNING_LIMIT, new TestClock());

      try {

        writeFileSync(logPath, SEED_CONTENT);

        assert.deepEqual(await applyLogSizeChanges([CHANGE], makeNext()), [], "the handler refuses nothing");

        await waitUntil(() => statSync(logPath).size <= (SAVED_LIMIT / 2),
          { description: "the trim the handler started to bring the file to at most half the saved limit", timeoutMs: 5000 });
      } finally {

        await shutdownFileLogger();
      }
    });
  });

  test("the handler settles while the size check it started is still inside its stat", async (t) => {

    await withTempDir(async (dir) => {

      const logPath = join(dir, "unawaited.log");
      const realStat = promises.stat;
      const releaseStat = Promise.withResolvers<true>();

      let handlerSettled = false;
      let statReached = false;

      await initializeFileLogger(logPath, RUNNING_LIMIT, new TestClock());

      try {

        writeFileSync(logPath, SEED_CONTENT);

        // The stat reads the real file and then parks until the row releases it, so the size check holds inside its stat while the row looks at the handler.
        t.mock.method(promises, "stat", async (file: PathLike): Promise<Stats> => {

          const stats = await realStat(file);

          statReached = true;

          await releaseStat.promise;

          return stats;
        });

        const handling = applyLogSizeChanges([CHANGE], makeNext()).then((rejections) => {

          handlerSettled = true;

          return rejections;
        });

        try {

          // A check that never reaches its stat fails here on the wait's own deadline rather than letting the row pass on a handler that had nothing to wait for.
          await waitUntil(() => statReached, { description: "the size check the handler started to reach its stat" });
          await settle();

          assert.equal(handlerSettled, true, "the handler settled while its size check was still inside its stat");
        } finally {

          releaseStat.resolve(true);
        }

        assert.deepEqual(await handling, [], "the handler refuses nothing");

        await waitUntil(() => statSync(logPath).size <= (SAVED_LIMIT / 2),
          { description: "the released size check's trim to bring the file to at most half the saved limit", timeoutMs: 5000 });
      } finally {

        await shutdownFileLogger();
      }
    });
  });
});
