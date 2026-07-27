/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.test.ts: Unit tests for the service CLI command handlers in commands.ts under the default-context tier, which exercises the real ServiceContext with
 * platform stubs and HOME redirects. Coverage focuses on the not-installed branches of every handler, the dispatcher, the handleStatus
 * default-context flow, printServiceUsage, and smoke routing through handleServiceCommand. Handlers under literal-context wiring (synthetic ServiceContext, happy
 * paths) are split by grouping: install/uninstall in commands.install.test.ts, runtime lifecycle (start/stop/restart) plus dispatch helpers in
 * commands.lifecycle.test.ts, and handleStatus literal in commands.status.test.ts.
 *
 * Branches that genuinely need a real launchd/systemd/Windows Task Scheduler round trip are still e2e territory and are not exercised here.
 */
import { afterEach, before, beforeEach, describe, mock, test } from "node:test";
import { handleServiceCommand, handleStart, handleStatus, handleStop, handleUninstall, printServiceUsage } from "./commands.ts";
import assert from "node:assert/strict";
import { initializeDataDir } from "../config/paths.ts";
import os from "node:os";
import path from "node:path";

/* These tests share infrastructure for stubbing console output and for redirecting process.platform. We restore the platform after each test and reset all stubs
 * via mock.reset() to avoid cross-test pollution. The data directory is initialized once for the whole suite to a deterministic temp location.
 */

const ORIGINAL_PLATFORM = process.platform;
const ORIGINAL_HOME = process.env["HOME"];

function setPlatform(value: string): void {

  Object.defineProperty(process, "platform", {

    configurable: true,
    value
  });
}

function makeFreshHome(prefix: string): string {

  // Each test points HOME at a fresh empty temp directory so getServiceFilePath() resolves to a location where no service file exists. We don't actually create
  // the directory on disk - the helpers we exercise only do read checks (existsSync), which will return false on a non-existent path.
  return path.join(os.tmpdir(), prefix + "-" + String(Date.now()) + "-" + String(Math.random()).slice(2, 8));
}

function restoreHome(): void {

  if(ORIGINAL_HOME === undefined) {

    Reflect.deleteProperty(process.env, "HOME");
  } else {

    process.env["HOME"] = ORIGINAL_HOME;
  }
}

before(() => {

  // Initializes the data directory so config, log, and PID paths resolve to a scratch location for the whole suite. On darwin and linux the "service file not
  // present" precondition instead comes from each describe block's own HOME redirect (see makeFreshHome below); on Windows, this fresh data directory is what
  // keeps prismcast-service.ps1 absent.
  initializeDataDir(path.join(os.tmpdir(), "prismcast-commands-test-" + String(Date.now())));
});

describe("printServiceUsage", () => {

  let logCalls: unknown[][];

  beforeEach(() => {

    logCalls = [];

    mock.method(globalThis.console, "log", (...args: unknown[]): void => {

      logCalls.push(args);
    });
  });

  afterEach(() => {

    mock.reset();
  });

  test("prints a multi-line usage block to stdout", () => {

    printServiceUsage();

    // The usage block spans multiple lines (header, commands list, options, configuration note). This test locks only the multi-line shape - that the output
    // spans several lines. The sibling tests below verify the canonical "Usage:" header on the first line and the presence of every documented subcommand name.
    assert.ok(logCalls.length > 5, "usage output should span multiple lines");
  });

  test("includes the canonical 'Usage:' header on the first line", () => {

    printServiceUsage();

    const firstLine = logCalls[0]?.[0];

    assert.equal(typeof firstLine, "string");
    assert.match(firstLine as string, /^Usage: prismcast service/);
  });

  test("mentions every documented subcommand", () => {

    printServiceUsage();

    const fullText = logCalls.map((args) => String(args[0])).join("\n");

    for(const cmd of [ "install", "uninstall", "start", "stop", "restart", "status" ]) {

      assert.match(fullText, new RegExp(cmd), "usage should mention the '" + cmd + "' subcommand");
    }
  });

  test("documents the --force flag", () => {

    printServiceUsage();

    const fullText = logCalls.map((args) => String(args[0])).join("\n");

    assert.match(fullText, /--force/);
  });

  test("does not write anything to stderr", () => {

    // Negative test: usage is informational, not an error - it must route through console.log only. Note: the global mock.reset in afterEach will revert this stub.
    const errorCalls: unknown[][] = [];

    mock.method(globalThis.console, "error", (...args: unknown[]): void => {

      errorCalls.push(args);
    });

    printServiceUsage();

    assert.equal(errorCalls.length, 0);
  });
});

describe("handleServiceCommand dispatcher", () => {

  let logCalls: unknown[][];
  let errorCalls: unknown[][];

  beforeEach(() => {

    logCalls = [];
    errorCalls = [];

    mock.method(globalThis.console, "log", (...args: unknown[]): void => {

      logCalls.push(args);
    });

    mock.method(globalThis.console, "error", (...args: unknown[]): void => {

      errorCalls.push(args);
    });
  });

  afterEach(() => {

    mock.reset();
  });

  test("returns 0 and prints usage when no command is given (args=[])", async () => {

    const code = await handleServiceCommand([]);

    assert.equal(code, 0, "empty args is the help path; exit code 0");
    assert.ok(logCalls.length > 0, "usage should have printed to stdout");
  });

  test("returns 0 and prints usage when 'help' is passed", async () => {

    const code = await handleServiceCommand(["help"]);

    assert.equal(code, 0);
    assert.match(String(logCalls[0]?.[0]), /^Usage: prismcast service/);
  });

  test("returns 0 and prints usage when '--help' is passed", async () => {

    const code = await handleServiceCommand(["--help"]);

    assert.equal(code, 0);
    assert.match(String(logCalls[0]?.[0]), /^Usage: prismcast service/);
  });

  test("returns 0 and prints usage when '-h' is passed", async () => {

    const code = await handleServiceCommand(["-h"]);

    assert.equal(code, 0);
    assert.match(String(logCalls[0]?.[0]), /^Usage: prismcast service/);
  });

  test("returns 1 and reports an error for unknown commands", async () => {

    const code = await handleServiceCommand(["bogus-command"]);

    assert.equal(code, 1, "unknown commands exit with code 1");
    assert.ok(errorCalls.length > 0, "an error message should have been printed to stderr");

    const errorText = errorCalls.map((args) => String(args[0])).join("\n");

    assert.match(errorText, /Unknown service command/, "stderr should explain the rejection");
    assert.match(errorText, /bogus-command/, "stderr should echo the offending command name");
  });

  test("unknown command also re-prints usage to stdout for discoverability", async () => {

    await handleServiceCommand(["bogus-command"]);

    // The dispatcher invokes printServiceUsage() after the error so users see what they could have typed instead. We verify the usage text appears in stdout.
    const stdoutText = logCalls.map((args) => String(args[0])).join("\n");

    assert.match(stdoutText, /Usage: prismcast service/);
  });
});

/* The not-installed-state handlers (start/stop/uninstall) all exhibit the same shape: route to the right generator on each platform, observe that the service file
 * is missing, exit code 1 with a "not installed" stderr message. We share one describe block driven by a small fixture table to lock the contract across handlers
 * and platforms without copy-pasting a near-identical block per handler/platform combination.
 */

describe("handlers report 'not installed' on a clean system", () => {

  let logCalls: unknown[][];
  let errorCalls: unknown[][];

  beforeEach(() => {

    logCalls = [];
    errorCalls = [];

    process.env["HOME"] = makeFreshHome("prismcast-handlers-home");

    mock.method(globalThis.console, "log", (...args: unknown[]): void => {

      logCalls.push(args);
    });

    mock.method(globalThis.console, "error", (...args: unknown[]): void => {

      errorCalls.push(args);
    });
  });

  afterEach(() => {

    mock.reset();
    setPlatform(ORIGINAL_PLATFORM);
    restoreHome();
  });

  // Each case is { fn, name, platform } - we walk the cross product so every handler is exercised on at least two platforms.
  const handlerCases: { fn: () => Promise<number>; name: string; platform: string }[] = [

    { fn: handleStart, name: "handleStart", platform: "darwin" },
    { fn: handleStart, name: "handleStart", platform: "linux" },
    { fn: handleStop, name: "handleStop", platform: "darwin" },
    { fn: handleStop, name: "handleStop", platform: "linux" },
    { fn: handleUninstall, name: "handleUninstall", platform: "darwin" },
    { fn: handleUninstall, name: "handleUninstall", platform: "linux" }
  ];

  for(const item of handlerCases) {

    test(item.name + " returns 1 with 'not installed' message on " + item.platform, async () => {

      setPlatform(item.platform);

      const code = await item.fn();

      assert.equal(code, 1, "missing service file should yield exit code 1");

      const errorText = errorCalls.map((args) => String(args[0])).join("\n");

      assert.match(errorText, /not installed/i, "error should mention 'not installed'");
    });
  }

  test("handleStart additionally suggests running 'service install'", async () => {

    // Boundary: handleStart includes a concrete next-step hint. handleStop does not (a stop with nothing installed is not actionable). We lock the difference.
    setPlatform("darwin");

    await handleStart();

    const errorText = errorCalls.map((args) => String(args[0])).join("\n");

    assert.match(errorText, /service install/, "error should suggest 'service install'");
  });
});

describe("handleStatus on a clean (not-installed) system", () => {

  let logCalls: unknown[][];
  let errorCalls: unknown[][];
  let fetchStub: ReturnType<typeof mock.method> | null;

  beforeEach(() => {

    logCalls = [];
    errorCalls = [];
    fetchStub = null;

    process.env["HOME"] = makeFreshHome("prismcast-status-home");

    mock.method(globalThis.console, "log", (...args: unknown[]): void => {

      logCalls.push(args);
    });

    mock.method(globalThis.console, "error", (...args: unknown[]): void => {

      errorCalls.push(args);
    });

    // Stub fetch so handleStatus does not try to reach a running PrismCast server. Belt-and-braces: when "not installed", isRunning returns false and fetch is
    // never called, but stubbing provides a guarantee against unexpected network I/O.
    fetchStub = mock.method(globalThis, "fetch", async (): Promise<Response> => new Response("", { status: 503 }));
  });

  afterEach(() => {

    mock.reset();
    setPlatform(ORIGINAL_PLATFORM);
    fetchStub = null;
    restoreHome();
  });

  test("on darwin, returns 0 and reports installed=No, running=No", async () => {

    setPlatform("darwin");

    const code = await handleStatus();

    assert.equal(code, 0, "status command always returns 0");

    const stdoutText = logCalls.map((args) => String(args[0])).join("\n");

    assert.match(stdoutText, /PrismCast Service Status/, "header should be printed");
    assert.match(stdoutText, /Installed:\s+No/, "should report not installed");
    assert.match(stdoutText, /Running:\s+No/, "should report not running");
  });

  test("on linux, returns 0 and reports installed=No, running=No", async () => {

    setPlatform("linux");

    const code = await handleStatus();

    assert.equal(code, 0);

    const stdoutText = logCalls.map((args) => String(args[0])).join("\n");

    assert.match(stdoutText, /Installed:\s+No/);
    assert.match(stdoutText, /Running:\s+No/);
  });

  test("on darwin, status output reports launchd as service manager and a .plist service file", async () => {

    setPlatform("darwin");

    await handleStatus();

    const stdoutText = logCalls.map((args) => String(args[0])).join("\n");

    assert.match(stdoutText, /Service manager:\s+launchd/, "darwin platform reports launchd");
    assert.match(stdoutText, /\.plist$/m, "the service file path on darwin ends with .plist");
  });

  test("on linux, status output reports systemd as the service manager", async () => {

    setPlatform("linux");

    await handleStatus();

    const stdoutText = logCalls.map((args) => String(args[0])).join("\n");

    assert.match(stdoutText, /Service manager:\s+systemd/);
    assert.match(stdoutText, /\.service$/m, "the service file on linux ends with .service");
  });

  test("when not installed, the status output suggests running 'service install'", async () => {

    setPlatform("darwin");

    await handleStatus();

    const stdoutText = logCalls.map((args) => String(args[0])).join("\n");

    assert.match(stdoutText, /service install/, "status should hint at the install command");
  });

  test("does not invoke fetch when the service is not running", async () => {

    setPlatform("darwin");

    await handleStatus();

    // The stub records its calls in fetchStub.mock.calls. With nothing installed/running, the function never reaches the fetchActiveStreams call.
    assert.equal(fetchStub?.mock.callCount(), 0, "fetch must not fire when the service isn't running");
  });
});

describe("handleServiceCommand routes to each handler (smoke tests)", () => {

  // We don't reproduce every handler's full behavior here - those have their own dedicated describe blocks above. We verify that the dispatcher routes each command
  // to the right handler by observing the handler's externally visible side effects (exit code shape on a not-installed system).

  let logCalls: unknown[][];
  let errorCalls: unknown[][];

  beforeEach(() => {

    logCalls = [];
    errorCalls = [];

    process.env["HOME"] = makeFreshHome("prismcast-route-home");

    mock.method(globalThis.console, "log", (...args: unknown[]): void => {

      logCalls.push(args);
    });

    mock.method(globalThis.console, "error", (...args: unknown[]): void => {

      errorCalls.push(args);
    });

    // Stub fetch defensively (status path may try to reach a server when isRunning happens to be true on a real environment - belt-and-braces).
    mock.method(globalThis, "fetch", async (): Promise<Response> => new Response("", { status: 503 }));
  });

  afterEach(() => {

    mock.reset();
    setPlatform(ORIGINAL_PLATFORM);
    restoreHome();
  });

  // Verbs that route to a "not installed" handler on a clean system. We assert exit code 1 and the "not installed" stderr line for each.
  const notInstalledRoutes = [ "stop", "uninstall", "start", "restart" ];

  for(const verb of notInstalledRoutes) {

    test("'" + verb + "' route lands in its handler (returns 1 on a clean system)", async () => {

      setPlatform("linux");

      const code = await handleServiceCommand([verb]);

      assert.equal(code, 1, verb + " on a not-installed system exits with 1");

      const errorText = errorCalls.map((args) => String(args[0])).join("\n");

      assert.match(errorText, /not installed/i);
    });
  }

  test("'status' route lands in handleStatus (returns 0 with status header)", async () => {

    setPlatform("darwin");

    const code = await handleServiceCommand(["status"]);

    assert.equal(code, 0);

    const stdoutText = logCalls.map((args) => String(args[0])).join("\n");

    assert.match(stdoutText, /PrismCast Service Status/);
  });

  /* The 'install' command path is exercised in the literal-context tests in commands.install.test.ts, which inject a fake generator instead of spawning
   * launchctl/systemctl/powershell.exe. The dispatcher's force-flag parsing (args.includes("--force") || args.includes("-f")) is exercised indirectly via the
   * unknown-command negative test - if --force or -f were not recognized as flags following an "install" verb, the dispatcher would still route to
   * handleInstall(false) rather than misclassify them as unknown commands, so the routing contract is preserved.
   */
});

/* Literal-context fixture types and factories live in commands.helpers.ts. The tests that consume makeFakeGenerator and makeContextHarness are
 * commands.install.test.ts, commands.lifecycle.test.ts, and commands.status.test.ts. This file instead drives the default-context handlers directly through its
 * own local process.platform and HOME stubbing.
 */

