/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * app.test.ts: Unit tests for the Express application builder module. Almost everything in app.ts is wired into a process-level lifecycle - the HTTP server,
 * the Chrome browser, the file logger, the signal handlers, the polling intervals - so the lifecycle is the first surface, driven only where a unit row can
 * reach it. startServer itself cannot be invoked safely from a unit test (it spawns Chrome, binds the port, registers signal handlers, and calls process.exit
 * on failure), and no automated suite exercises it, because it needs a live Chrome; the signal handlers are registered by a private function inside it, so no
 * row reaches them. releaseInstanceSlot is exercised on its ownership path, the critical-correctness case: a process that does NOT own the identity file must
 * leave it alone. The ownership check is structural (release() reads the file record and refuses to remove a file whose PID does not match this process), and
 * that guarantee holds no matter how the module graph was loaded. startBootServices, the boot's tail, is driven through its injected steps, so each of its
 * shutdown checks is observed at its own boundary with recording stubs standing in for the services, the listener and HDHomeRun, and closeMainServer,
 * shutdown's step for the listener, is driven after such a boot, so the server the boot binds is the one shutdown closes, with its bound crossed on a test
 * clock. handleStartupFailure, the entry point's catch, is driven through its injected shutdown reader and exit.
 *
 * The HTTP request-logging rules are the second surface tested here. The skip predicates, the per-level decision and the elapsed-time renderer are pure of the
 * Express plumbing - they take a plain record or a request object and return a decision - so every level's rule set is exercised without booting the server.
 * The request logger itself runs on a bare Express app bound to a loopback port with a stub stream, so a level change is observed request by request.
 *
 * The log size handler is the third surface. Its rows open the file logger on a file of their own and hand the handler the candidate a save would, so the
 * trim it starts and the save it never holds up are each observed on disk.
 */
import type { IncomingMessage, Server } from "node:http";
import { LOG, serializeRecord } from "./utils/index.ts";
import type { PathLike, Stats } from "node:fs";
import { TestClock, settle, waitUntil } from "homebridge-plugin-utils/testing";
import { after, afterEach, before, beforeEach, describe, test } from "node:test";
import { applyLogSizeChanges, closeMainServer, createRequestLogger, defaultBootServicesDeps, defaultStartupFailureDeps, elapsedMillis, handleStartupFailure,
  releaseInstanceSlot, skipInErrorsMode, skipInFilteredMode, skipRequestLog, stampRequestStart, startBootServices } from "./app.ts";
import { closePuppeteerStreamWssOnIdle, withTempDir } from "./testing.helpers.ts";
import { existsSync, promises, statSync, writeFileSync } from "node:fs";
import { getServerPidFilePath, initializeDataDir } from "./config/paths.ts";
import { initializeFileLogger, shutdownFileLogger } from "./utils/fileLogger.ts";
import { isGracefulShutdown, setGracefulShutdown } from "./browser/index.ts";
import type { AddressInfo } from "node:net";
import type { BootServicesDeps } from "./app.ts";
import { CONFIG } from "./config/index.ts";
import type { Config } from "./types/index.ts";
import type { ConfigChange } from "./config/reactivity.ts";
import type { Express } from "express";
import { HTTP_LOG_LEVELS } from "./types/index.ts";
import assert from "node:assert/strict";
import { attachCdpUpgradeHandler } from "./routes/cdp.ts";
import { createServer } from "node:http";
import express from "express";
import { initDebugFilter } from "./utils/debugFilter.ts";
import { join } from "node:path";
import { registerConfigChangeHandler } from "./config/reactivity.ts";
import { startHdhrServer } from "./hdhr/index.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* The hooks clear the PRISMCAST_DATA_DIR env var before each test and restore the surrounding value after it, so the suite leaves the variable exactly as it
 * found it. The data directory config/paths.ts resolves is module-level and deliberately persists across rows: the row after the ownership row relies on the
 * resolution that row left behind. Only the ownership row scopes its own data directory, via withTempDir + initializeDataDir.
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
      writeFileSync(pidPath, serializeRecord({ bootId: "any-boot", commandHash: null, pid: otherPid, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }), "utf-8");

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

/* startServer is not run here. It launches Chrome via puppeteer-core, binds the configured port, registers process-level signal handlers, spawns ffmpeg
 * children, and may call process.exit on failure - any of which is incompatible with a unit-test context - and no automated suite exercises it, because it
 * needs a live Chrome. Its tail, startBootServices, takes its steps as injected dependencies, and the rows below drive the boot's tail through them: each step is a
 * recording stub, the shutdown check reads the browser module's own state, which a row sets through setGracefulShutdown, and the listen stub returns an
 * http.Server that never listens, so no row binds a port, starts a background service or reaches the CDP module's process-wide upgrade server.
 */
describe("startBootServices", () => {

  const SKIP_LINE = "Shutdown began during startup, so the remaining startup steps are skipped.";

  /**
   * A row's boot: the steps it hands startBootServices, the app the build step returns, the server the listen step returns, the error a rejecting step throws, and
   * each step the stubs recorded, in the order the boot took them, with the argument the step received.
   */
  interface RecordedBoot {

    readonly app: Express;
    readonly deps: BootServicesDeps;
    readonly failure: Error;
    readonly httpServer: Server;
    readonly steps: { readonly name: string; readonly received: unknown }[];
  }

  /**
   * Builds a row's recording steps over the browser module's own shutdown reader. The step a row names sets the graceful-shutdown state, or fails, before it
   * returns, which is how a signal or a failure lands during that step. Only the bind can meet a signal, because the build is synchronous.
   * @param options - Which step sets the shutdown state, and which step fails.
   * @param options.rejectAt - The step that fails with the boot's failure in place of returning.
   * @param options.shutdownAt - The step that sets the graceful-shutdown state before it returns.
   * @returns The row's boot.
   */
  function recordBoot({ rejectAt, shutdownAt }: { rejectAt?: "build" | "listen"; shutdownAt?: "listen" } = {}): RecordedBoot {

    const app = express();
    const failure = new Error("The startup step failed.");
    const httpServer = createServer();
    const steps: { name: string; received: unknown }[] = [];

    const settleStep = (name: "build" | "listen"): void => {

      if(shutdownAt === name) {

        setGracefulShutdown(true);
      }

      if(rejectAt === name) {

        throw failure;
      }
    };

    const deps: BootServicesDeps = {

      attachCdpUpgradeHandler: (listener: Server): void => {

        steps.push({ name: "attach", received: listener });
      },
      buildApp: (): Express => {

        steps.push({ name: "build", received: null });
        settleStep("build");

        return app;
      },
      isGracefulShutdown,
      listenMainServer: async (built: Express): Promise<Server> => {

        steps.push({ name: "listen", received: built });
        settleStep("listen");

        return httpServer;
      },
      startBackgroundServices: (): void => {

        steps.push({ name: "background", received: null });
      },
      startHdhrServer: async (): Promise<void> => {

        steps.push({ name: "hdhr", received: null });
      }
    };

    return { app, deps, failure, httpServer, steps };
  }

  // The names of the steps a row's boot took, in order.
  const stepNames = (boot: RecordedBoot): string[] => boot.steps.map((step) => step.name);

  // How many skip lines a row's LOG.info mock recorded.
  const skipLines = (calls: readonly { arguments: readonly unknown[] }[]): number => calls.filter((call) => call.arguments[0] === SKIP_LINE).length;

  afterEach(() => {

    setGracefulShutdown(false);
  });

  test("with no shutdown, every step runs once in the boot's order, each receiving what the step before it returned", async (t) => {

    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });
    const boot = recordBoot();

    await startBootServices(boot.deps);

    assert.deepEqual(stepNames(boot), [ "background", "build", "listen", "attach", "hdhr" ], "every step ran once, in order");
    assert.equal(boot.steps.find((step) => step.name === "listen")?.received, boot.app, "the listen received the app the build returned");
    assert.equal(boot.steps.find((step) => step.name === "attach")?.received, boot.httpServer, "the attach received the server the listen returned");
    assert.equal(skipLines(info.mock.calls), 0, "the skip line never logged");
  });

  test("a shutdown that began before the boot's tail arms nothing", async (t) => {

    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });
    const boot = recordBoot();

    setGracefulShutdown(true);

    await startBootServices(boot.deps);

    assert.deepEqual(stepNames(boot), [], "no step ran, the background services among them");
    assert.equal(skipLines(info.mock.calls), 1, "the skip line logged once");
  });

  test("a shutdown that begins during the bind stops the boot before HDHomeRun starts", async (t) => {

    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });
    const boot = recordBoot({ shutdownAt: "listen" });

    await startBootServices(boot.deps);

    assert.deepEqual(stepNames(boot), [ "background", "build", "listen", "attach" ], "every step but HDHomeRun ran");
    assert.equal(skipLines(info.mock.calls), 1, "the skip line logged once");
  });

  test("a build that throws rejects the boot's tail with its error, and nothing after the build runs", async (t) => {

    t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });

    const boot = recordBoot({ rejectAt: "build" });

    await assert.rejects(startBootServices(boot.deps), (error: unknown): boolean => error === boot.failure, "the boot's tail rejected with the build's error");

    assert.deepEqual(stepNames(boot), [ "background", "build" ], "neither the listen, the attach nor HDHomeRun ran");
  });

  test("a bind that rejects rejects the boot's tail with its error, and nothing after the bind runs", async (t) => {

    t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });

    const boot = recordBoot({ rejectAt: "listen" });

    await assert.rejects(startBootServices(boot.deps), (error: unknown): boolean => error === boot.failure, "the boot's tail rejected with the bind's error");

    assert.deepEqual(stepNames(boot), [ "background", "build", "listen" ], "neither the attach nor HDHomeRun ran");
  });

  test("the default steps' shutdown reader, CDP attach and HDHomeRun start are the functions their own modules export", () => {

    assert.equal(defaultBootServicesDeps.isGracefulShutdown, isGracefulShutdown, "the shutdown check reads the browser module's own state");
    assert.equal(defaultBootServicesDeps.attachCdpUpgradeHandler, attachCdpUpgradeHandler, "the attach is the CDP module's own");
    assert.equal(defaultBootServicesDeps.startHdhrServer, startHdhrServer, "the HDHomeRun start is the HDHomeRun module's own");
  });

  test("the server the boot's tail binds is the one shutdown's close step closes", async (t) => {

    t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

    const boot = recordBoot();

    // The listen step's server never listens, so the row mocks its close with one that answers its callback with no error and returns the server, as a close
    // with no open connection does.
    const close = t.mock.method(boot.httpServer, "close", (callback?: (error?: Error) => void): Server => {

      callback?.();

      return boot.httpServer;
    });

    await startBootServices(boot.deps);
    await closeMainServer();

    assert.equal(close.mock.callCount(), 1, "shutdown's close step closed the server the listen step returned");
  });

  /* The close step's own branches. Each row boots through the recording steps, so the module holds the listen step's server, which never listens, and mocks
   * that server's close with the outcome the row reads: a reported error, a close that never reports, a close that throws. The close step clears the server
   * before it closes it, so every row leaves no server behind for the next one.
   */

  test("shutdown's close step logs the error a close reports, and resolves", async (t) => {

    t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

    const error = t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });
    const boot = recordBoot();

    t.mock.method(boot.httpServer, "close", (callback?: (failure?: Error) => void): Server => {

      callback?.(new Error("The close failed."));

      return boot.httpServer;
    });

    await startBootServices(boot.deps);
    await closeMainServer();

    assert.deepEqual(error.mock.calls.map((call) => call.arguments),
      [[ "The HTTP server reported an error while closing during shutdown.", { error: "The close failed" } ]],
      "the close's error was logged at error level with the error in the context object");
  });

  test("a close that never reports lets the shutdown continue once its bound lapses on the clock the close step is handed", async (t) => {

    t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const boot = recordBoot();
    const clock = new TestClock();

    let closed = false;

    t.mock.method(boot.httpServer, "close", (): Server => boot.httpServer);

    await startBootServices(boot.deps);

    const closing = closeMainServer(clock).then(() => {

      closed = true;
    });

    await settle();

    assert.equal(closed, false, "precondition: the close step waits while the close has not reported");
    assert.equal(clock.advanceToNext(), true, "the close step armed its bound on the clock it was handed");

    await settle();

    assert.equal(closed, true, "the close step resolved once its bound lapsed, with no real time spent");
    assert.deepEqual(warn.mock.calls.map((call) => call.arguments[0]), ["The HTTP server did not close within its bound, so the shutdown continues."],
      "the lapse was logged once at warn level");

    await closing;
  });

  test("a close step with no server bound closes nothing and logs nothing, so a repeated shutdown close is a no-op", async (t) => {

    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });
    const boot = recordBoot();
    const close = t.mock.method(boot.httpServer, "close", (callback?: (failure?: Error) => void): Server => {

      callback?.();

      return boot.httpServer;
    });

    await startBootServices(boot.deps);
    await closeMainServer();

    assert.equal(close.mock.callCount(), 1, "precondition: the first close step closed the server");

    const logged = info.mock.callCount();
    const error = t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });

    await closeMainServer();

    assert.equal(close.mock.callCount(), 1, "the second close step found no server and closed nothing");
    assert.equal(info.mock.callCount() + error.mock.callCount() + warn.mock.callCount(), logged, "the second close step logged nothing");
  });

  test("the close step destroys the open connections once, before it starts its bound", async (t) => {

    t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

    const boot = recordBoot();
    const clock = new TestClock();
    const pendingAtDestroy: number[] = [];

    t.mock.method(boot.httpServer, "close", (callback?: (failure?: Error) => void): Server => {

      callback?.();

      return boot.httpServer;
    });

    const closeAll = t.mock.method(boot.httpServer, "closeAllConnections", (): void => {

      pendingAtDestroy.push(clock.pending);
    });

    await startBootServices(boot.deps);
    await closeMainServer(clock);

    assert.equal(closeAll.mock.callCount(), 1, "every open connection was destroyed once, so the close waits on no heartbeat stream");
    assert.deepEqual(pendingAtDestroy, [0], "the connections were destroyed before the bound was armed");
  });

  test("a close that throws is caught and logged, and the close step still resolves", async (t) => {

    const error = t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });
    const boot = recordBoot();

    t.mock.method(boot.httpServer, "close", (): Server => {

      throw new Error("The close threw.");
    });

    await startBootServices(boot.deps);
    await assert.doesNotReject(closeMainServer(), "a throwing close never rejects the close step, so the shutdown reaches its exit");

    assert.deepEqual(error.mock.calls.map((call) => call.arguments), [[ "The HTTP server could not be closed during shutdown.", { error: "The close threw" } ]],
      "the throw was logged at error level with the error in the context object");
  });
});

/* The entry point's catch for a failed startup. Each row hands the handler recording stubs for the shutdown reader and the exit, so neither branch reads the
 * browser module's state or ends the test process.
 */
describe("handleStartupFailure", () => {

  test("a failure once shutdown has begun logs the superseded startup and leaves the exit to shutdown", (t) => {

    const error = t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });
    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });
    const exit = t.mock.fn((_code: number): void => { /* Captured via the mock. */ });

    handleStartupFailure(new Error("The launch was superseded."), { exit, isGracefulShutdown: () => true });

    assert.equal(exit.mock.callCount(), 0, "the handler left the exit to shutdown");
    assert.deepEqual(info.mock.calls.map((call) => call.arguments), [["Startup was superseded by shutdown, so the process exits through shutdown."]],
      "the superseded startup was logged once");
    assert.equal(error.mock.callCount(), 0, "nothing was logged as fatal");
  });

  test("any other failure is fatal: the handler logs the error and exits with a failure code", (t) => {

    const error = t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });
    const exit = t.mock.fn((_code: number): void => { /* Captured via the mock. */ });

    handleStartupFailure(new Error("The boot failed."), { exit, isGracefulShutdown: () => false });

    assert.deepEqual(error.mock.calls.map((call) => call.arguments), [[ "Fatal startup error occurred.", { error: "The boot failed" } ]],
      "the fatal error was logged at error level with the error in the context object");
    assert.deepEqual(exit.mock.calls.map((call) => call.arguments), [[1]], "the process exited once with a failure code");
  });

  test("the default shutdown reader is the browser module's own", () => {

    assert.equal(defaultStartupFailureDeps.isGracefulShutdown, isGracefulShutdown, "the handler reads the state the signal handler sets");
  });
});

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

    // The status route answers with the status its path names, and with a Retry-After header when its query asks for one, so a row varies the status or the
    // header alone.
    app.get("/status/:code", (req, res) => {

      res.once("finish", () => { finished.resolve(null); });

      if(req.query["retryAfter"] !== undefined) {

        res.setHeader("Retry-After", "5");
      }

      res.sendStatus(Number(req.params.code));
    });

    // A polling endpoint, which the filtered level skips when it succeeds fast.
    app.get("/logs", (_req, res) => {

      res.once("finish", () => { finished.resolve(null); });
      res.send("ok");
    });

    // A polling endpoint held as /hold is, so a row can move the elapsed reading between the request's arrival and its finish.
    app.get("/health", (_req, res) => {

      res.once("finish", () => { finished.resolve(null); });
      entered.resolve(null);
      void release.promise.then(() => { res.send("ok"); });
    });

    // Every other path answers 404, so a row varies the URL of a not-found response alone.
    app.use((_req, res) => {

      res.once("finish", () => { finished.resolve(null); });
      res.sendStatus(404);
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

  test("a request that arrives under none and finishes after the level changes to all writes no line", async () => {

    CONFIG.logging.httpLogLevel = "none";
    finished = Promise.withResolvers<null>();

    const response = fetch(baseUrl + "/hold");

    await entered.promise;

    CONFIG.logging.httpLogLevel = "all";
    release.resolve(null);

    await (await response).text();
    await finished.promise;

    assert.deepEqual(lines, []);
  });

  test("under errors, a 503 that carries Retry-After writes no line and one without it writes one", async () => {

    CONFIG.logging.httpLogLevel = "errors";

    await requestAndFinish("/status/503?retryAfter=1");
    await requestAndFinish("/status/503");

    assert.equal(lines.length, 1, "one of the requests wrote a line");
    assert.match(lines[0] ?? "", /^GET \/status\/503 from /, "the line is the request without Retry-After");
  });

  test("under errors, a success writes no line and a server error writes one", async () => {

    CONFIG.logging.httpLogLevel = "errors";

    await requestAndFinish("/status/200");
    await requestAndFinish("/status/500");

    assert.equal(lines.length, 1, "one of the requests wrote a line");
    assert.match(lines[0] ?? "", /^GET \/status\/500 from /, "the line is the server error");
  });

  test("under errors, a 404 for a browser asset writes no line and a 404 for any other path writes one", async () => {

    CONFIG.logging.httpLogLevel = "errors";

    await requestAndFinish("/favicon.ico");
    await requestAndFinish("/missing");

    assert.equal(lines.length, 1, "one of the requests wrote a line");
    assert.match(lines[0] ?? "", /^GET \/missing from /, "the line is the 404 for the path the browser did not ask for on its own");
  });

  test("under filtered, a fast success on a polling endpoint writes no line and a slow one writes one", async (t) => {

    // The arrival stamp and the finish reading each come from process.hrtime.bigint(), so the row holds that reading, through its own context so no other row
    // reads the held clock, and moves it past the slow threshold between the held request's arrival and its finish.
    let reading = 0n;

    t.mock.method(process.hrtime, "bigint", (): bigint => reading);
    CONFIG.logging.httpLogLevel = "filtered";

    await requestAndFinish("/logs");

    finished = Promise.withResolvers<null>();

    const response = fetch(baseUrl + "/health");

    await entered.promise;

    reading += 1001000000n;
    release.resolve(null);

    await (await response).text();
    await finished.promise;

    assert.equal(lines.length, 1, "one of the requests wrote a line");
    assert.match(lines[0] ?? "", /^GET \/health from \S+ responded 200 in 1001\.000 ms\.\n$/, "the line is the slow request, timed by the held reading");
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
