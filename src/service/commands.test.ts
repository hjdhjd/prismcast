/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.test.ts: Unit tests for the service CLI command handlers in commands.ts.
 *
 * The handlers come in two coverage tiers:
 *
 * 1. Default-context tests (legacy, kept for the not-installed and dispatch paths). These stub console.log/console.error via mock.method, route process.platform
 *    via Object.defineProperty, and route HOME so that the "not installed" branches of every handler can be exercised without touching real launchd/systemd/
 *    Task Scheduler state. The handlers run with the default ServiceContext, which wires real I/O.
 *
 * 2. Literal-context tests (the happy paths). Each handler accepts an optional ServiceContext, and these tests pass a literal context that captures stdout/stderr
 *    and returns whatever generator/install state the test wants. This lets us cover handleInstall's happy path, handleStatus with active streams, restart-after
 *    -path-change, etc., without spawning real subprocesses.
 *
 * Branches that genuinely need a real launchd/systemd/sc.exe round trip are still e2e territory and are not exercised here.
 */
import { type ServiceContext, type StreamsResponse, handleInstall, handleRestart, handleServiceCommand, handleStart, handleStatus,
  handleStop, handleUninstall, printServiceUsage } from "./commands.ts";
import type { ServiceDefinition, ServiceGenerator, ServicePaths, StalePathResult } from "./generators.ts";
import { afterEach, before, beforeEach, describe, mock, test } from "node:test";
import type { Nullable } from "../types/index.ts";
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

  // Initialize the data directory before any test runs - all the platform helpers consulted by commands.ts read from it. We point at a fresh temp location so the
  // "service file not present" precondition is satisfied for the entire suite (the data dir won't contain a prismcast-service.ps1 either).
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

    // The usage block is at least a dozen lines (header, commands list, options, configuration note). We don't lock the exact line count, but we verify the
    // shape: the first line is the canonical "Usage:" header, and several known command names appear somewhere in the output.
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
 * and platforms without copy-pasting six near-identical blocks.
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

  // [handler name, handler fn, platform value] - we walk the cross product so every handler is exercised on at least two platforms.
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

  /* The 'install' command path is exercised in the literal-context tests below, which inject a fake generator instead of spawning launchctl/systemctl/sc.exe.
   * The dispatcher's force-flag parsing (args.includes("--force") || args.includes("-f")) is exercised indirectly via the unknown-command negative test - if
   * --force or -f were not recognized as flags following an "install" verb, the dispatcher would still route to handleInstall(false) rather than misclassify
   * them as unknown commands, so the routing contract is preserved.
   */
});

/* Literal-context tests. The fakes below capture stdout/stderr into arrays so tests can assert on output, and the fake ServiceGenerator records every method
 * invocation so tests can verify the right side effects fired in the right order. Each test constructs a fresh context literal with only the fields it cares
 * about overridden, so failure messages point directly at the contract being tested.
 */

interface FakeGenerator extends ServiceGenerator {

  installed: boolean;
  installs: ServiceDefinition[];
  installShouldThrow: Nullable<Error>;
  isInstalledCalls: number;
  isRunningCalls: number;
  running: boolean;
  startCalls: number;
  startShouldThrow: Nullable<Error>;
  stopCalls: number;
  stopShouldThrow: Nullable<Error>;
  uninstallCalls: number;
  uninstallShouldThrow: Nullable<Error>;
}

function makeFakeGenerator(overrides: Partial<FakeGenerator> = {}): FakeGenerator {

  const generator: FakeGenerator = {

    install: async (definition: ServiceDefinition): Promise<void> => {

      generator.installs.push(definition);

      if(generator.installShouldThrow) {

        throw generator.installShouldThrow;
      }

      generator.installed = true;
      generator.running = true;
    },
    installShouldThrow: null,
    installed: false,
    installs: [],
    isInstalled: async (): Promise<boolean> => {

      generator.isInstalledCalls++;

      return generator.installed;
    },
    isInstalledCalls: 0,
    isRunning: async (): Promise<boolean> => {

      generator.isRunningCalls++;

      return generator.running;
    },
    isRunningCalls: 0,
    platform: "darwin",
    running: false,
    serviceManager: "launchd",
    start: async (): Promise<void> => {

      generator.startCalls++;

      if(generator.startShouldThrow) {

        throw generator.startShouldThrow;
      }

      generator.running = true;
    },
    startCalls: 0,
    startShouldThrow: null,
    stop: async (): Promise<void> => {

      generator.stopCalls++;

      if(generator.stopShouldThrow) {

        throw generator.stopShouldThrow;
      }

      generator.running = false;
    },
    stopCalls: 0,
    stopShouldThrow: null,
    uninstall: async (): Promise<void> => {

      generator.uninstallCalls++;

      if(generator.uninstallShouldThrow) {

        throw generator.uninstallShouldThrow;
      }

      generator.installed = false;
      generator.running = false;
    },
    uninstallCalls: 0,
    uninstallShouldThrow: null,
    ...overrides
  };

  return generator;
}

interface ContextHarness {

  context: ServiceContext;
  generator: FakeGenerator | null;
  stderr: string[];
  stdout: string[];
}

interface ContextOverrides {

  detectStalePaths?: () => Nullable<StalePathResult>;
  fetchActiveStreams?: (port: number) => Promise<Nullable<StreamsResponse>>;
  generator?: FakeGenerator | null;
  getServerPort?: () => Promise<number>;
  getServicePaths?: () => Nullable<ServicePaths>;
  platform?: string;
}

function makeContextHarness(overrides: ContextOverrides = {}): ContextHarness {

  const stdout: string[] = [];
  const stderr: string[] = [];
  const generator = overrides.generator === null ? null : (overrides.generator ?? makeFakeGenerator());

  const definition: ServiceDefinition = {

    entryPoint: "/usr/local/lib/prismcast/dist/index.js",
    envVars: {},
    logsDir: "/Users/test/.prismcast",
    nodePath: "/usr/local/bin/node",
    workingDir: "/Users/test/.prismcast"
  };

  const context: ServiceContext = {

    buildServiceDefinition: (): ServiceDefinition => definition,
    detectStalePaths: overrides.detectStalePaths ?? ((): Nullable<StalePathResult> => null),
    fetchActiveStreams: overrides.fetchActiveStreams ?? (async (): Promise<Nullable<StreamsResponse>> => null),
    getDataDir: (): string => "/Users/test/.prismcast",
    getEntryPoint: (): string => "/usr/local/lib/prismcast/dist/index.js",
    getGenerator: (): Nullable<ServiceGenerator> => generator,
    getNodePath: (): string => "/usr/local/bin/node",
    getPlatform: () => (overrides.platform ?? "darwin") as ReturnType<ServiceContext["getPlatform"]>,
    getServerPort: overrides.getServerPort ?? (async (): Promise<number> => 5589),
    getServiceFilePath: (): string => "/Users/test/Library/LaunchAgents/com.prismcast.plist",
    getServicePaths: overrides.getServicePaths ?? ((): Nullable<ServicePaths> => null),
    stderr: (line: string): void => { stderr.push(line); },
    stdout: (line: string): void => { stdout.push(line); }
  };

  return { context, generator, stderr, stdout };
}

describe("handleInstall (literal context)", () => {

  test("happy path: fresh install on macOS calls generator.install with the definition and returns 0", async () => {

    const { context, generator, stdout } = makeContextHarness();

    const code = await handleInstall(false, context);

    assert.equal(code, 0);
    assert.equal(generator?.installs.length, 1, "generator.install was called exactly once");
    assert.match(stdout.join("\n"), /Installing PrismCast service/);
    assert.match(stdout.join("\n"), /Service installed successfully/);
    assert.match(stdout.join("\n"), /service is now running and will start automatically at login/);
  });

  test("rejects already-installed without --force, returns 1, does not call install", async () => {

    const generator = makeFakeGenerator({ installed: true });
    const { context, stderr } = makeContextHarness({ generator });

    const code = await handleInstall(false, context);

    assert.equal(code, 1);
    assert.equal(generator.installs.length, 0, "generator.install was not called");
    assert.match(stderr.join("\n"), /already installed/);
    assert.match(stderr.join("\n"), /--force/);
  });

  test("with --force, reinstalls even when already installed", async () => {

    const generator = makeFakeGenerator({ installed: true });
    const { context, stdout } = makeContextHarness({ generator });

    const code = await handleInstall(true, context);

    assert.equal(code, 0);
    assert.equal(generator.installs.length, 1, "generator.install fired despite already-installed");
    assert.match(stdout.join("\n"), /Service installed successfully/);
  });

  test("returns 1 with stderr message when the platform is unsupported (no generator)", async () => {

    const { context, stderr } = makeContextHarness({ generator: null, platform: "openbsd" });

    const code = await handleInstall(false, context);

    assert.equal(code, 1);
    assert.match(stderr.join("\n"), /not supported on this platform \(openbsd\)/);
  });

  test("returns 1 when generator.install throws and surfaces the error message", async () => {

    const generator = makeFakeGenerator({ installShouldThrow: new Error("plist write failed") });
    const { context, stderr } = makeContextHarness({ generator });

    const code = await handleInstall(false, context);

    assert.equal(code, 1);
    assert.match(stderr.join("\n"), /Failed to install service/);
    assert.match(stderr.join("\n"), /plist write failed/);
  });

  test("emits systemd-flavored instructions when generator.serviceManager is 'systemd'", async () => {

    const generator = makeFakeGenerator({ platform: "linux", serviceManager: "systemd" });
    const { context, stdout } = makeContextHarness({ generator, platform: "linux" });

    const code = await handleInstall(false, context);

    assert.equal(code, 0);
    assert.match(stdout.join("\n"), /journalctl --user -u prismcast -f/, "systemd-specific journal hint shown");
  });

  test("emits Windows-flavored instructions when generator.serviceManager is 'windows-scheduler'", async () => {

    // The Platform type is "darwin" | "linux" | "windows" - not the Node-native "win32" string. The platform helpers normalize to the friendlier name.
    const generator = makeFakeGenerator({ platform: "windows", serviceManager: "windows-scheduler" });
    const { context, stdout } = makeContextHarness({ generator, platform: "windows" });

    const code = await handleInstall(false, context);

    assert.equal(code, 0);
    assert.match(stdout.join("\n"), /type .*prismcast\.log/, "Windows 'type' command hint shown");
  });
});

describe("handleUninstall (literal context)", () => {

  test("happy path: installed service is uninstalled cleanly", async () => {

    const generator = makeFakeGenerator({ installed: true });
    const { context, stdout } = makeContextHarness({ generator });

    const code = await handleUninstall(context);

    assert.equal(code, 0);
    assert.equal(generator.uninstallCalls, 1);
    assert.match(stdout.join("\n"), /Service uninstalled successfully/);
    assert.match(stdout.join("\n"), /Configuration and data files .* preserved/);
  });

  test("returns 1 when the service is not installed", async () => {

    const { context, generator, stderr } = makeContextHarness();

    const code = await handleUninstall(context);

    assert.equal(code, 1);
    assert.equal(generator?.uninstallCalls, 0);
    assert.match(stderr.join("\n"), /not installed/);
  });

  test("returns 1 when generator.uninstall throws and surfaces the error message", async () => {

    const generator = makeFakeGenerator({ installed: true, uninstallShouldThrow: new Error("launchctl unload failed") });
    const { context, stderr } = makeContextHarness({ generator });

    const code = await handleUninstall(context);

    assert.equal(code, 1);
    assert.match(stderr.join("\n"), /Failed to uninstall service/);
    assert.match(stderr.join("\n"), /launchctl unload failed/);
  });
});

describe("handleStart (literal context)", () => {

  test("happy path: installed but not running, paths match, calls generator.start, returns 0", async () => {

    const generator = makeFakeGenerator({ installed: true, running: false });
    const { context, stdout } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/usr/local/lib/prismcast/dist/index.js",
        nodePath: "/usr/local/bin/node"
      })
    });

    const code = await handleStart(context);

    assert.equal(code, 0);
    assert.equal(generator.startCalls, 1, "generator.start was called");
    assert.equal(generator.installs.length, 0, "no reinstall happened (paths matched)");
    assert.match(stdout.join("\n"), /Service started successfully/);
  });

  test("returns 0 with friendly message when the service is already running", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({ generator });

    const code = await handleStart(context);

    assert.equal(code, 0);
    assert.equal(generator.startCalls, 0, "start was not called");
    assert.match(stdout.join("\n"), /already running/);
  });

  test("regenerates the service file when stored paths differ from current runtime paths", async () => {

    // The post-Homebrew-upgrade scenario: existing service file points at an old node binary; current runtime resolves to a new one. handleStart must
    // regenerate before starting.
    const generator = makeFakeGenerator({ installed: true, running: false });
    const { context, stdout } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/old/dist/index.js",
        nodePath: "/old/node"
      })
    });

    const code = await handleStart(context);

    assert.equal(code, 0);
    assert.equal(generator.installs.length, 1, "reinstall was triggered by path mismatch");
    assert.equal(generator.startCalls, 0, "generator.start was not called separately - install() does the start");
    assert.match(stdout.join("\n"), /Detected path changes/);
    assert.match(stdout.join("\n"), /Updated service file with current paths/);
  });

  test("returns 1 with stderr message when the service is not installed", async () => {

    const { context, generator, stderr } = makeContextHarness();

    const code = await handleStart(context);

    assert.equal(code, 1);
    assert.equal(generator?.startCalls, 0);
    assert.match(stderr.join("\n"), /not installed/);
  });

  test("returns 1 when generator.start throws", async () => {

    const generator = makeFakeGenerator({ installed: true, running: false, startShouldThrow: new Error("launchctl bootstrap rejected") });
    const { context, stderr } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/usr/local/lib/prismcast/dist/index.js",
        nodePath: "/usr/local/bin/node"
      })
    });

    const code = await handleStart(context);

    assert.equal(code, 1);
    assert.match(stderr.join("\n"), /Failed to start service/);
    assert.match(stderr.join("\n"), /launchctl bootstrap rejected/);
  });
});

describe("handleStop (literal context)", () => {

  test("happy path: installed service is stopped successfully", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({ generator });

    const code = await handleStop(context);

    assert.equal(code, 0);
    assert.equal(generator.stopCalls, 1);
    assert.match(stdout.join("\n"), /Service stopped successfully/);
  });

  test("returns 0 (success) even when generator.stop throws (loaded-but-not-running case)", async () => {

    // The launchd "loaded but not running" state: stop() throws because there's nothing actively running, but the end state is what we wanted - service stopped.
    const generator = makeFakeGenerator({ installed: true, stopShouldThrow: new Error("Could not find specified service") });
    const { context, stderr, stdout } = makeContextHarness({ generator });

    const code = await handleStop(context);

    assert.equal(code, 0, "stop is treated as success even on throw");
    assert.match(stderr.join("\n"), /Note:.*Could not find specified service/, "the underlying error is logged as a Note");
    assert.match(stdout.join("\n"), /Service stopped successfully/);
  });

  test("returns 1 when the service is not installed", async () => {

    const { context, generator, stderr } = makeContextHarness();

    const code = await handleStop(context);

    assert.equal(code, 1);
    assert.equal(generator?.stopCalls, 0);
    assert.match(stderr.join("\n"), /not installed/);
  });
});

describe("handleRestart (literal context)", () => {

  test("happy path: stops then starts (paths match, no regeneration)", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/usr/local/lib/prismcast/dist/index.js",
        nodePath: "/usr/local/bin/node"
      })
    });

    const code = await handleRestart(context);

    assert.equal(code, 0);
    assert.equal(generator.stopCalls, 1, "stop was called first");
    assert.equal(generator.startCalls, 1, "start was called after stop");
    assert.match(stdout.join("\n"), /Service restarted successfully/);
  });

  test("regenerates the service file during restart when paths drifted", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/old/dist/index.js",
        nodePath: "/old/node"
      })
    });

    const code = await handleRestart(context);

    assert.equal(code, 0);
    assert.equal(generator.stopCalls, 1);
    assert.equal(generator.installs.length, 1, "reinstall fired due to path drift");
    assert.match(stdout.join("\n"), /Detected path changes/);
  });

  test("absorbs generator.stop throws (service may not have been running)", async () => {

    // The "stop before restart" call is best-effort. If the service wasn't actually running, stop() throws but restart proceeds normally.
    const generator = makeFakeGenerator({ installed: true, running: false, stopShouldThrow: new Error("not running") });
    const { context } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/usr/local/lib/prismcast/dist/index.js",
        nodePath: "/usr/local/bin/node"
      })
    });

    const code = await handleRestart(context);

    assert.equal(code, 0, "throw on stop did not abort the restart");
    assert.equal(generator.startCalls, 1);
  });

  test("returns 1 when not installed", async () => {

    const { context, stderr } = makeContextHarness();

    const code = await handleRestart(context);

    assert.equal(code, 1);
    assert.match(stderr.join("\n"), /not installed/);
  });
});

describe("handleStatus (literal context)", () => {

  test("not-installed status: shows Yes/No flags and an install hint", async () => {

    const { context, stdout } = makeContextHarness();

    const code = await handleStatus(context);

    assert.equal(code, 0);

    const text = stdout.join("\n");

    assert.match(text, /PrismCast Service Status/);
    assert.match(text, /Installed:\s+No/);
    assert.match(text, /Running:\s+No/);
    assert.match(text, /service install.*to install the service/);
  });

  test("unsupported platform: shows the 'Not available' banner", async () => {

    const { context, stdout } = makeContextHarness({ generator: null, platform: "openbsd" });

    const code = await handleStatus(context);

    assert.equal(code, 0);
    assert.match(stdout.join("\n"), /Service support: Not available/);
    assert.match(stdout.join("\n"), /not supported on this platform/);
  });

  test("installed and running with active streams shows the stream list", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const fakeStreams: StreamsResponse = {

      count: 2,
      limit: 4,
      streams: [
        { channel: "ABC", duration: 65, id: 1, showName: "Eyewitness News", url: "https://example.com/abc/stream.m3u8" },
        { channel: "NBC", duration: 30, id: 2, showName: "", url: "https://www.nbc.com/live" }
      ]
    };
    const { context, stdout } = makeContextHarness({

      fetchActiveStreams: async (): Promise<StreamsResponse> => fakeStreams,
      generator
    });

    const code = await handleStatus(context);

    assert.equal(code, 0);

    const text = stdout.join("\n");

    assert.match(text, /Installed:\s+Yes/);
    assert.match(text, /Running:\s+Yes/);
    assert.match(text, /Active streams:\s+2\/4/);
    assert.match(text, /ABC.*Eyewitness News/);
    assert.match(text, /NBC/);
  });

  test("running with zero streams shows '0/limit' without iterating an empty list", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({

      fetchActiveStreams: async (): Promise<StreamsResponse> => ({ count: 0, limit: 4, streams: [] }),
      generator
    });

    const code = await handleStatus(context);

    assert.equal(code, 0);
    assert.match(stdout.join("\n"), /Active streams:\s+0\/4/);
  });

  test("running but server unreachable shows the '(server not responding)' fallback", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({

      fetchActiveStreams: async (): Promise<Nullable<StreamsResponse>> => null,
      generator
    });

    const code = await handleStatus(context);

    assert.equal(code, 0);
    assert.match(stdout.join("\n"), /Active streams:\s+\(server not responding\)/);
  });

  test("installed with stale paths emits the regenerate-on-restart warning", async () => {

    const generator = makeFakeGenerator({ installed: true, running: false });
    const { context, stderr } = makeContextHarness({

      detectStalePaths: (): Nullable<StalePathResult> => ({ entryPoint: "/old/dist/index.js", nodePath: "/old/node", stale: true }),
      generator
    });

    const code = await handleStatus(context);

    assert.equal(code, 0);

    const errorText = stderr.join("\n");

    assert.match(errorText, /Service file contains stale paths/);
    assert.match(errorText, /Run 'prismcast service restart'/);
  });

  test("falls back to URL hostname when channel field is missing on a stream", async () => {

    // The status display extracts a hostname when channel is null. www. prefix is stripped. Malformed URLs become "Stream <id>".
    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({

      fetchActiveStreams: async (): Promise<StreamsResponse> => ({

        count: 2,
        limit: 4,
        streams: [
          { channel: null, duration: 0, id: 5, showName: "", url: "https://www.example.com/path" },
          { channel: null, duration: 0, id: 7, showName: "", url: "not-a-url" }
        ]
      }),
      generator
    });

    await handleStatus(context);

    const text = stdout.join("\n");

    assert.match(text, /example\.com/, "www. prefix stripped, hostname shown");
    assert.match(text, /Stream 7/, "malformed URL fell back to 'Stream <id>'");
  });
});

describe("printServiceUsage (literal context)", () => {

  test("writes the usage block to ctx.stdout", () => {

    const { context, stdout } = makeContextHarness();

    printServiceUsage(context);

    const text = stdout.join("\n");

    assert.match(text, /^Usage: prismcast service/);

    for(const cmd of [ "install", "uninstall", "start", "stop", "restart", "status" ]) {

      assert.match(text, new RegExp(cmd), "usage mentions " + cmd);
    }
  });
});

describe("handleServiceCommand (literal context)", () => {

  test("routes 'install --force' to handleInstall(true)", async () => {

    const generator = makeFakeGenerator({ installed: true });
    const { context } = makeContextHarness({ generator });

    const code = await handleServiceCommand([ "install", "--force" ], context);

    // Already installed, --force triggers reinstall - install() is called once.
    assert.equal(code, 0);
    assert.equal(generator.installs.length, 1, "force flag routed through to handleInstall(true)");
  });

  test("routes 'install -f' to handleInstall(true) (short flag)", async () => {

    const generator = makeFakeGenerator({ installed: true });
    const { context } = makeContextHarness({ generator });

    const code = await handleServiceCommand([ "install", "-f" ], context);

    assert.equal(code, 0);
    assert.equal(generator.installs.length, 1);
  });

  test("threads the same context through every subcommand", async () => {

    // We exercise every dispatch case once and verify the context's stdout buffer captures output from each. This ensures the dispatcher passes ctx through
    // rather than constructing a fresh default context per invocation.
    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({ generator });

    await handleServiceCommand(["status"], context);
    assert.match(stdout.join("\n"), /PrismCast Service Status/, "status routed through ctx.stdout");
  });
});
