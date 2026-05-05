/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.test.ts: Unit tests for the platform-specific service file generators in generators.ts. The pure surface (collectServiceEnvironment,
 * buildServiceDefinition, getServicePaths, detectStalePaths, getServiceGenerator) is exercised across all three platform branches by stubbing process.platform via
 * Object.defineProperty - the canonical pattern used by platform.test.ts. The install/start/stop/uninstall paths on each generator spawn external subprocesses
 * (launchctl, systemctl, powershell.exe); those branches are intentionally NOT tested here per the task contract - they require real OS state and belong in e2e
 * coverage. We do verify that the file generators produce well-formed output by routing the service file path into a temp dir and reading back the artifact.
 */
import { type FakeExecFile, execFileAlwaysSucceeds, execFileFromMap, firstOf, makeExecFileError, nthOf } from "../testing.helpers.ts";
import type { GeneratorIO, ServiceDefinition } from "./generators.ts";
import { afterEach, before, beforeEach, describe, mock, test } from "node:test";
import { buildServiceDefinition, collectServiceEnvironment, detectStalePaths, getServiceGenerator, getServicePaths } from "./generators.ts";
import type { Platform } from "../utils/platform.ts";
import assert from "node:assert/strict";
import fs from "node:fs";
import { initializeDataDir } from "../config/paths.ts";
import os from "node:os";
import path from "node:path";

/* The platform helpers (getServiceFilePath, getDataDir, getNodeExecutablePath, getPrismCastEntryPoint, etc.) are not parameterized - they read process.platform and
 * the data directory. We stub process.platform via Object.defineProperty per the platform.test.ts pattern, and we initialize the data directory once for the whole
 * suite to a deterministic temp location.
 */

const ORIGINAL_PLATFORM = process.platform;

// We snapshot the pre-test state of every env var that the module reads so that afterEach can restore them. The exact list is derived from CONFIG_METADATA at module
// load time, but the key ones we manipulate in tests are PATH and the bootstrap variables; we capture and restore those by name.
const TRACKED_ENV_KEYS = [ "PATH", "PRISMCAST_DATA_DIR", "PRISMCAST_DEBUG", "PRISMCAST_SERVICE", "HDHR_PORT", "HLS_SEGMENT_DURATION" ];

function snapshotEnv(): Record<string, string | undefined> {

  const snap: Record<string, string | undefined> = {};

  for(const key of TRACKED_ENV_KEYS) {

    snap[key] = process.env[key];
  }

  return snap;
}

function restoreEnv(snap: Record<string, string | undefined>): void {

  for(const key of TRACKED_ENV_KEYS) {

    const value = snap[key];

    if(value === undefined) {

      // Reflect.deleteProperty avoids the dynamic-delete lint rule; the behavior matches `delete process.env[key]` for ordinary string-keyed env entries.
      Reflect.deleteProperty(process.env, key);
    } else {

      process.env[key] = value;
    }
  }
}

function setPlatform(value: string): void {

  Object.defineProperty(process, "platform", {

    configurable: true,
    value
  });
}

before(() => {

  // Initialize the data directory so getDataDir() does not throw. The Windows getServiceFilePath() reads from this; tests that need a writable location route
  // the file path into a per-test temp dir below.
  initializeDataDir(path.join(os.tmpdir(), "prismcast-generators-test"));
});

describe("collectServiceEnvironment", () => {

  let envSnap: Record<string, string | undefined>;

  beforeEach(() => {

    envSnap = snapshotEnv();
  });

  afterEach(() => {

    restoreEnv(envSnap);
  });

  test("includes PATH when process.env.PATH is set", () => {

    process.env["PATH"] = "/usr/local/bin:/usr/bin";

    const result = collectServiceEnvironment();

    assert.equal(result["PATH"], "/usr/local/bin:/usr/bin", "PATH should be propagated verbatim");
  });

  test("omits PATH when process.env.PATH is unset", () => {

    delete process.env["PATH"];

    const result = collectServiceEnvironment();

    assert.equal("PATH" in result, false, "PATH must not be present when unset upstream");
  });

  test("captures PRISMCAST_DATA_DIR (a bootstrap env var)", () => {

    process.env["PRISMCAST_DATA_DIR"] = "/some/data/dir";

    const result = collectServiceEnvironment();

    assert.equal(result["PRISMCAST_DATA_DIR"], "/some/data/dir", "bootstrap env var should be captured when set");
  });

  test("captures PRISMCAST_DEBUG (a bootstrap env var)", () => {

    process.env["PRISMCAST_DEBUG"] = "tuning:hulu";

    const result = collectServiceEnvironment();

    assert.equal(result["PRISMCAST_DEBUG"], "tuning:hulu");
  });

  test("captures CONFIG_METADATA-declared env vars when set (HDHR_PORT)", () => {

    // HDHR_PORT is declared in CONFIG_METADATA as the env var for hdhr.port. Setting it in process.env should cause it to flow through.
    process.env["HDHR_PORT"] = "5004";

    const result = collectServiceEnvironment();

    assert.equal(result["HDHR_PORT"], "5004", "config-metadata env var should be captured when set");
  });

  test("does not include CONFIG_METADATA env vars that are unset upstream", () => {

    // Negative test: an unset env var must not produce an empty-string entry. The implementation guards on `value !== undefined`.
    delete process.env["HLS_SEGMENT_DURATION"];

    const result = collectServiceEnvironment();

    assert.equal("HLS_SEGMENT_DURATION" in result, false);
  });

  test("does not stamp PRISMCAST_SERVICE itself (that happens in buildServiceDefinition)", () => {

    // The contract is that collectServiceEnvironment captures user/runtime env, while buildServiceDefinition adds the PRISMCAST_SERVICE=1 marker. A bare call
    // here must not add it on its own.
    delete process.env["PRISMCAST_SERVICE"];

    const result = collectServiceEnvironment();

    assert.equal("PRISMCAST_SERVICE" in result, false);
  });
});

describe("buildServiceDefinition", () => {

  let envSnap: Record<string, string | undefined>;

  beforeEach(() => {

    envSnap = snapshotEnv();
  });

  afterEach(() => {

    restoreEnv(envSnap);
    setPlatform(ORIGINAL_PLATFORM);
  });

  test("always sets PRISMCAST_SERVICE=1 in envVars", () => {

    delete process.env["PRISMCAST_SERVICE"];

    const def = buildServiceDefinition();

    assert.equal(def.envVars["PRISMCAST_SERVICE"], "1", "service marker is always stamped");
  });

  test("populates entryPoint, nodePath, workingDir, and logsDir as absolute paths", () => {

    const def = buildServiceDefinition();

    assert.ok(path.isAbsolute(def.entryPoint), "entryPoint should be absolute");
    assert.ok(path.isAbsolute(def.nodePath), "nodePath should be absolute");
    assert.ok(path.isAbsolute(def.workingDir), "workingDir should be absolute");
    assert.ok(path.isAbsolute(def.logsDir), "logsDir should be absolute");
  });

  test("merges collected environment alongside the service marker", () => {

    process.env["PATH"] = "/sentinel/path";
    process.env["PRISMCAST_DATA_DIR"] = "/sentinel/data";

    const def = buildServiceDefinition();

    assert.equal(def.envVars["PRISMCAST_SERVICE"], "1");
    assert.equal(def.envVars["PATH"], "/sentinel/path", "PATH should propagate from process.env");
    assert.equal(def.envVars["PRISMCAST_DATA_DIR"], "/sentinel/data", "bootstrap var should propagate");
  });

  test("entry point ends with index.js", () => {

    const def = buildServiceDefinition();

    assert.match(def.entryPoint, /index\.js$/);
  });
});

describe("getServiceGenerator", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  // [process.platform value, expected platform field, expected serviceManager field]
  const platformCases: { manager: "launchd" | "systemd" | "windows-scheduler"; platform: "darwin" | "linux" | "windows"; processValue: string }[] = [

    { manager: "launchd", platform: "darwin", processValue: "darwin" },
    { manager: "systemd", platform: "linux", processValue: "linux" },
    { manager: "windows-scheduler", platform: "windows", processValue: "win32" }
  ];

  for(const item of platformCases) {

    test("returns a " + item.manager + " generator on " + item.processValue, () => {

      setPlatform(item.processValue);

      const gen = getServiceGenerator();

      assert.ok(gen, "generator should be defined on " + item.processValue);
      assert.equal(gen.platform, item.platform);
      assert.equal(gen.serviceManager, item.manager);
    });
  }

  test("returns a (linux/systemd) generator on unknown platforms via getPlatform's default branch", () => {

    // getPlatform() maps unknown values to "linux", so getServiceGenerator() returns the systemd generator for any non-darwin/non-win32 input. This locks the
    // documented default-platform behavior shared with platform.test.ts.
    setPlatform("freebsd");

    const gen = getServiceGenerator();

    assert.ok(gen, "even unknown platforms produce a generator (mapped through linux)");
    assert.equal(gen.serviceManager, "systemd", "unknown platforms fall through to the linux/systemd branch");
  });
});

/* getServicePaths parser tests. We exercise the per-platform parser branch by stubbing fs.existsSync (always true to clear the bail-out check) and fs.readFileSync
 * (returning a forged service file body for the platform under test). The real external tools (launchctl/systemctl/powershell) are NOT invoked - those need real
 * OS state and live in e2e. The withParserStubs helper centralizes the stub setup and restoration so each test focuses on the parser's input-output contract.
 */

function withParserStubs<T>(content: string | Error | null, run: () => T): T {

  // null content means existsSync should report false (file missing). An Error means readFileSync should throw it. A string is the file body.
  const exists = (content !== null);
  const existsStub = mock.method(fs, "existsSync", () => exists);
  let readStub: ReturnType<typeof mock.method> | null = null;

  if(exists) {

    readStub = mock.method(fs, "readFileSync", () => {

      if(content instanceof Error) {

        throw content;
      }

      return content;
    });
  }

  try {

    return run();
  } finally {

    existsStub.mock.restore();
    readStub?.mock.restore();
  }
}

const FAKE_PLIST = [
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
  "<plist version=\"1.0\">",
  "<dict>",
  "  <key>Label</key>",
  "  <string>com.github.hjdhjd.prismcast</string>",
  "  <key>ProgramArguments</key>",
  "  <array>",
  "    <string>/opt/homebrew/bin/node</string>",
  "    <string>/opt/homebrew/lib/node_modules/prismcast/dist/index.js</string>",
  "  </array>",
  "</dict>",
  "</plist>"
].join("\n");

const FAKE_UNIT = [
  "[Unit]",
  "Description=PrismCast Streaming Server",
  "[Service]",
  "Type=simple",
  "ExecStart=/usr/bin/node /opt/prismcast/dist/index.js",
  "WorkingDirectory=/opt/prismcast",
  ""
].join("\n");

const FAKE_LAUNCHER = [
  "﻿# PrismCast service launcher.",
  "# node: C:\\Program Files\\nodejs\\node.exe",
  "# entry: C:\\Users\\test\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\index.js",
  "",
  "$env:PRISMCAST_SERVICE = '1'",
  "Start-Process node.exe"
].join("\r\n");

describe("getServicePaths happy paths (per platform)", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("darwin: extracts the first two ProgramArguments strings as node and entry point", () => {

    setPlatform("darwin");

    const result = withParserStubs(FAKE_PLIST, () => getServicePaths());

    assert.ok(result, "parser should produce a non-null result");
    assert.equal(result.nodePath, "/opt/homebrew/bin/node");
    assert.equal(result.entryPoint, "/opt/homebrew/lib/node_modules/prismcast/dist/index.js");
  });

  test("linux: extracts node and entry-point paths from ExecStart", () => {

    setPlatform("linux");

    const result = withParserStubs(FAKE_UNIT, () => getServicePaths());

    assert.ok(result, "parser should produce a non-null result");
    assert.equal(result.nodePath, "/usr/bin/node");
    assert.equal(result.entryPoint, "/opt/prismcast/dist/index.js");
  });

  test("windows: extracts node and entry from the # node: and # entry: comment metadata", () => {

    setPlatform("win32");

    const result = withParserStubs(FAKE_LAUNCHER, () => getServicePaths());

    assert.ok(result, "parser should produce a non-null result");
    assert.equal(result.nodePath, "C:\\Program Files\\nodejs\\node.exe");
    assert.equal(result.entryPoint, "C:\\Users\\test\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\index.js");
  });
});

describe("getServicePaths null/error branches (per platform)", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("darwin: returns null when ProgramArguments key is absent", () => {

    setPlatform("darwin");
    assert.equal(withParserStubs("<?xml version=\"1.0\"?><plist><dict></dict></plist>", () => getServicePaths()), null);
  });

  test("linux: returns null when ExecStart is absent (malformed unit)", () => {

    setPlatform("linux");
    assert.equal(withParserStubs("[Unit]\nDescription=Broken\n", () => getServicePaths()), null);
  });

  test("windows: returns null when both metadata comments are absent", () => {

    setPlatform("win32");
    assert.equal(withParserStubs("# Some other comment\nStart-Process node.exe", () => getServicePaths()), null);
  });

  test("windows: returns null when only one of the two metadata lines is present", () => {

    // Boundary: the parser requires both # node: and # entry: lines - either alone is a malformed launcher.
    setPlatform("win32");
    assert.equal(withParserStubs("# node: C:\\node.exe\nStart-Process node.exe", () => getServicePaths()), null);
  });

  test("returns null when the service file does not exist (existsSync=false)", () => {

    setPlatform("linux");
    assert.equal(withParserStubs(null, () => getServicePaths()), null);
  });

  test("returns null when readFileSync throws (e.g., permission denied)", () => {

    setPlatform("linux");
    assert.equal(withParserStubs(new Error("EACCES: permission denied"), () => getServicePaths()), null, "read failure must produce null, not throw");
  });
});

/* withStaleStubs orchestrates the multi-call existsSync sequence detectStalePaths needs. The first existsSync call inside getServicePaths checks the service file
 * itself; the next two calls check the nodePath and entryPoint extracted from it. We script the three boolean returns so each test can assert a specific stale
 * topology (both stale, neither stale, only entry stale, etc.).
 */

function withStaleStubs<T>(unit: string, existsResults: boolean[], run: () => T): T {

  let callCount = 0;
  const existsStub = mock.method(fs, "existsSync", () => existsResults[callCount++] ?? false);
  const readStub = mock.method(fs, "readFileSync", () => unit);

  try {

    return run();
  } finally {

    existsStub.mock.restore();
    readStub.mock.restore();
  }
}

describe("detectStalePaths", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("returns null when the service file does not exist", () => {

    setPlatform("linux");

    const existsStub = mock.method(fs, "existsSync", () => false);

    try {

      assert.equal(detectStalePaths(), null);
    } finally {

      existsStub.mock.restore();
    }
  });

  test("reports stale=true when both node and entry are missing on disk", () => {

    setPlatform("linux");

    // existsSync calls: [service file=true, nodePath=false, entryPoint=false].
    const result = withStaleStubs("ExecStart=/missing/node /missing/index.js\n", [ true, false, false ], () => detectStalePaths());

    assert.ok(result, "stale detection should produce a non-null result");
    assert.equal(result.stale, true);
    assert.equal(result.nodePath, "/missing/node");
    assert.equal(result.entryPoint, "/missing/index.js");
  });

  test("reports stale=false when both paths exist on disk", () => {

    setPlatform("linux");

    const result = withStaleStubs("ExecStart=/present/node /present/index.js\n", [ true, true, true ], () => detectStalePaths());

    assert.ok(result, "stale detection should produce a non-null result");
    assert.equal(result.stale, false);
    assert.equal(result.nodePath, undefined, "nodePath only present when stale");
    assert.equal(result.entryPoint, undefined, "entryPoint only present when stale");
  });

  test("reports only the entryPoint as stale when node exists but entry does not", () => {

    setPlatform("linux");

    // existsSync calls: [service file=true, nodePath=true, entryPoint=false].
    const result = withStaleStubs("ExecStart=/present/node /missing/index.js\n", [ true, true, false ], () => detectStalePaths());

    assert.ok(result, "stale detection should produce a non-null result");
    assert.equal(result.stale, true);
    assert.equal(result.nodePath, undefined, "node was present, so nodePath field is undefined");
    assert.equal(result.entryPoint, "/missing/index.js", "entry was missing, so it surfaces");
  });
});

describe("ServiceDefinition shape contract", () => {

  test("ServiceDefinition keys are exactly the documented set", () => {

    // Locks the public shape so a future field addition forces a code review of the consumers (each generator).
    const def: ServiceDefinition = buildServiceDefinition();
    const keys = Object.keys(def).toSorted();

    assert.deepEqual(keys, [ "entryPoint", "envVars", "logsDir", "nodePath", "workingDir" ]);
  });
});

/* The platform-specific generators (launchd, systemd, windows-scheduler) are pure orchestrators over GeneratorIO. The fakes below capture every IO call into
 * arrays so tests can assert on the exact subprocess invocations and file-write content each generator emits. Each test constructs a fresh fake IO so call
 * histories don't leak across cases.
 */

interface FakeIOCall {

  args: unknown[];
  method: string;
}

/* The execFile fake-shape and helpers (execFileFromMap, execFileAlwaysSucceeds, makeExecFileError, bufferOrStringToString, the FakeExecFile type, and the
 * FakeExecFileResult shape) are cross-cutting test infrastructure imported from testing.helpers.ts. Any test in the codebase that injects an execFile-shaped
 * function into production code uses the same vocabulary.
 */

interface FakeIOOptions {

  // The execFile implementation injected into the fake IO. Defaults to execFileFromMap({}) - strictly throws "no result configured" on every call so test setups
  // can't silently miss commands. Tests typically pass execFileFromMap({...}) for keyed responses or execFileAlwaysSucceeds() for wildcard responses.
  execFile?: FakeExecFile;

  // Map of file paths to whether they "exist" for access() and existsSync().
  existing?: Record<string, boolean>;

  // Map of paths to file contents readFileSync() should return.
  files?: Record<string, string>;

  platform?: Platform;
  serviceFileDirectory?: string;
  serviceFilePath?: string;
}

interface FakeIOHarness {

  calls: FakeIOCall[];
  io: GeneratorIO;
  writes: { content: string; path: string }[];
}

function makeFakeIO(options: FakeIOOptions = {}): FakeIOHarness {

  const calls: FakeIOCall[] = [];
  const writes: { content: string; path: string }[] = [];
  const existing = options.existing ?? {};
  const files = options.files ?? {};
  const userExecFile = options.execFile ?? execFileFromMap({});

  const io: GeneratorIO = {

    access: async (filePath: string): Promise<void> => {

      calls.push({ args: [filePath], method: "access" });

      if(!existing[filePath]) {

        throw new Error("ENOENT: no such file or directory, access " + filePath);
      }
    },
    execFile: async (file: string, args: string[]): Promise<{ stderr: string; stdout: string }> => {

      // Record the invocation before delegating to the user's impl so calls are observable on every code path - including the failure path where userExecFile
      // throws. This keeps assertions on the call sequence stable regardless of whether commands succeed or fail.
      calls.push({ args: [ file, ...args ], method: "execFile" });

      return userExecFile(file, args);
    },
    existsSync: (filePath: string): boolean => {

      calls.push({ args: [filePath], method: "existsSync" });

      return existing[filePath] ?? false;
    },
    getPlatform: (): Platform => options.platform ?? "darwin",
    getServiceFileDirectory: (): string => options.serviceFileDirectory ?? "/Users/test/Library/LaunchAgents",
    getServiceFilePath: (): string => options.serviceFilePath ?? "/Users/test/Library/LaunchAgents/com.prismcast.plist",
    mkdir: async (filePath: string, opts: { recursive: boolean }): Promise<void> => {

      calls.push({ args: [ filePath, opts ], method: "mkdir" });
    },
    readFileSync: (filePath: string): string => {

      calls.push({ args: [filePath], method: "readFileSync" });

      const content = files[filePath];

      if(content === undefined) {

        throw new Error("ENOENT: no such file or directory, open " + filePath);
      }

      return content;
    },
    rm: async (filePath: string, opts: { force: boolean }): Promise<void> => {

      calls.push({ args: [ filePath, opts ], method: "rm" });
    },
    writeFile: async (filePath: string, content: string): Promise<void> => {

      calls.push({ args: [ filePath, content ], method: "writeFile" });
      writes.push({ content, path: filePath });
    }
  };

  return { calls, io, writes };
}

function definitionFixture(overrides: Partial<ServiceDefinition> = {}): ServiceDefinition {

  return {

    entryPoint: "/usr/local/lib/prismcast/dist/index.js",
    envVars: { PATH: "/usr/local/bin:/usr/bin", PRISMCAST_SERVICE: "1" },
    logsDir: "/Users/test/.prismcast",
    nodePath: "/usr/local/bin/node",
    workingDir: "/Users/test/.prismcast",
    ...overrides
  };
}

describe("createLaunchdGenerator (via getServiceGenerator on darwin)", () => {

  test("install: ensures directories, writes a plist, and loads via launchctl", async () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const installDir = "/Users/test/Library/LaunchAgents";
    const { calls, io, writes } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl load -w /Users/test/Library/LaunchAgents/com.prismcast.plist": { stdout: "" }
      }),
      platform: "darwin",
      serviceFileDirectory: installDir,
      serviceFilePath: installPath
    });
    const generator = getServiceGenerator(io);

    assert.notEqual(generator, null);

    await generator?.install(definitionFixture());

    // Verify mkdir for both the install dir and the logs dir.
    const mkdirCalls = calls.filter((c) => c.method === "mkdir").map((c) => c.args[0]);

    assert.ok(mkdirCalls.includes(installDir), "install dir was created");
    assert.ok(mkdirCalls.includes("/Users/test/.prismcast"), "logs dir was created");

    // Exactly one writeFile to the install path; content should be a valid-looking plist.
    assert.equal(writes.length, 1);

    const written = firstOf(writes, "plist write");

    assert.equal(written.path, installPath);
    assert.match(written.content, /<\?xml version="1\.0" encoding="UTF-8"\?>/);
    assert.match(written.content, /<key>Label<\/key>/);
    assert.match(written.content, /<string>com\.github\.hjdhjd\.prismcast<\/string>/, "Label value is the SERVICE_ID");
    assert.match(written.content, /<string>\/usr\/local\/bin\/node<\/string>/, "node path embedded in plist");
    assert.match(written.content, /<key>RunAtLoad<\/key>\s*<true\/>/, "RunAtLoad set");
    assert.match(written.content, /<key>KeepAlive<\/key>\s*<true\/>/, "KeepAlive set");

    // launchctl load -w was invoked.
    const execCalls = calls.filter((c) => c.method === "execFile");
    const firstExec = firstOf(execCalls, "execFile call");

    assert.deepEqual(firstExec.args, [ "launchctl", "load", "-w", installPath ]);
  });

  test("install: falls back to unload+load on initial load failure (reinstall scenario)", async () => {

    // The first load throws (already loaded). The generator unloads and retries, which succeeds.
    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const { calls, io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl load -w /Users/test/Library/LaunchAgents/com.prismcast.plist": { shouldThrow: true },
        "launchctl unload /Users/test/Library/LaunchAgents/com.prismcast.plist": { stdout: "" }
      }),
      platform: "darwin",
      serviceFilePath: installPath
    });
    const generator = getServiceGenerator(io);

    /* The current implementation calls runAndSurfaceStderr in the retry path, which expects a real Error with stderr (Buffer) on the cause. Our fake throws a
     * plain Error so the second load also throws; we expect this to propagate.
     */
    await assert.rejects(() => generator?.install(definitionFixture()) ?? Promise.resolve(), /launchctl load failed/);

    // Verify the unload happened between the two load attempts.
    const execCalls = calls.filter((c) => c.method === "execFile");
    const sequence = execCalls.map((c) => c.args.slice(0, 2).join(" "));

    assert.deepEqual(sequence, [
      "launchctl load",
      "launchctl unload",
      "launchctl load"
    ]);
  });

  test("isInstalled: returns true when the service file exists and false otherwise", async () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const installed = makeFakeIO({ existing: { [installPath]: true }, platform: "darwin", serviceFilePath: installPath });
    const notInstalled = makeFakeIO({ existing: {}, platform: "darwin", serviceFilePath: installPath });

    assert.equal(await getServiceGenerator(installed.io)?.isInstalled(), true);
    assert.equal(await getServiceGenerator(notInstalled.io)?.isInstalled(), false);
  });

  test("isRunning: parses launchctl list output and returns true when the PID column is numeric", async () => {

    // launchctl list emits "PID\tStatus\tLabel" rows. The generator finds the row containing the SERVICE_ID and parses the PID.
    const { io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl list": { stdout: "PID\tStatus\tLabel\n12345\t0\tcom.github.hjdhjd.prismcast\nother-row\n" }
      }),
      platform: "darwin"
    });

    assert.equal(await getServiceGenerator(io)?.isRunning(), true);
  });

  test("isRunning: returns false when the row's PID column is '-' (loaded but not running)", async () => {

    const { io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl list": { stdout: "PID\tStatus\tLabel\n-\t0\tcom.github.hjdhjd.prismcast\n" }
      }),
      platform: "darwin"
    });

    assert.equal(await getServiceGenerator(io)?.isRunning(), false);
  });

  test("isRunning: returns false when the SERVICE_ID is not present in the list output", async () => {

    const { io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl list": { stdout: "PID\tStatus\tLabel\n12345\t0\tcom.other.service\n" }
      }),
      platform: "darwin"
    });

    assert.equal(await getServiceGenerator(io)?.isRunning(), false);
  });

  test("uninstall: unloads then removes the plist", async () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const { calls, io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl unload /Users/test/Library/LaunchAgents/com.prismcast.plist": { stdout: "" }
      }),
      platform: "darwin",
      serviceFilePath: installPath
    });

    await getServiceGenerator(io)?.uninstall();

    const sequence = calls.filter((c) => (c.method === "execFile") || (c.method === "rm")).map((c) => ({ args: c.args, method: c.method }));
    const unload = nthOf(sequence, 0, "uninstall step");
    const removePlist = nthOf(sequence, 1, "uninstall step");

    assert.equal(unload.method, "execFile", "first call is launchctl unload");
    assert.deepEqual(unload.args, [ "launchctl", "unload", installPath ]);
    assert.equal(removePlist.method, "rm", "second call removes the plist file");
    assert.deepEqual(removePlist.args, [ installPath, { force: true } ]);
  });

  test("uninstall: ignores errors from launchctl unload (service may not be loaded)", async () => {

    // The unload throws (e.g., service was never loaded). The generator silently absorbs it and continues to the rm step.
    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const { calls, io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl unload /Users/test/Library/LaunchAgents/com.prismcast.plist": { shouldThrow: true }
      }),
      platform: "darwin",
      serviceFilePath: installPath
    });

    await assert.doesNotReject(() => getServiceGenerator(io)?.uninstall() ?? Promise.resolve());

    // The rm call still happened.
    assert.ok(calls.some((c) => (c.method === "rm") && (c.args[0] === installPath)));
  });
});

describe("createSystemdGenerator (via getServiceGenerator on linux)", () => {

  test("install: writes the systemd unit and runs daemon-reload + enable + start", async () => {

    const installPath = "/Users/test/.config/systemd/user/prismcast.service";
    const { calls, io, writes } = makeFakeIO({

      execFile: execFileFromMap({


        "systemctl --user daemon-reload": { stdout: "" },
        "systemctl --user enable prismcast.service": { stdout: "" },
        "systemctl --user start prismcast.service": { stdout: "" }
      }),
      platform: "linux",
      serviceFileDirectory: "/Users/test/.config/systemd/user",
      serviceFilePath: installPath
    });

    await getServiceGenerator(io)?.install(definitionFixture());

    assert.equal(writes.length, 1);

    const unitWrite = firstOf(writes, "unit write");

    assert.equal(unitWrite.path, installPath);
    assert.match(unitWrite.content, /^\[Unit\]/m);
    assert.match(unitWrite.content, /^ExecStart=\/usr\/local\/bin\/node \/usr\/local\/lib\/prismcast\/dist\/index\.js$/m);
    assert.match(unitWrite.content, /^Restart=always$/m);
    assert.match(unitWrite.content, /^Environment="PRISMCAST_SERVICE=1"$/m);
    assert.match(unitWrite.content, /^WantedBy=default\.target$/m);

    const execSequence = calls.filter((c) => c.method === "execFile").map((c) => c.args.slice(1).join(" "));

    assert.deepEqual(execSequence, [
      "--user daemon-reload",
      "--user enable prismcast.service",
      "--user start prismcast.service"
    ]);
  });

  test("isRunning: returns true when systemctl is-active reports 'active'", async () => {

    const { io } = makeFakeIO({

      execFile: execFileFromMap({ "systemctl --user is-active prismcast.service": { stdout: "active\n" } }),
      platform: "linux"
    });

    assert.equal(await getServiceGenerator(io)?.isRunning(), true);
  });

  test("isRunning: returns false when systemctl is-active throws (inactive returns non-zero)", async () => {

    const { io } = makeFakeIO({

      execFile: execFileFromMap({ "systemctl --user is-active prismcast.service": { shouldThrow: true, stdout: "inactive" } }),
      platform: "linux"
    });

    assert.equal(await getServiceGenerator(io)?.isRunning(), false);
  });

  test("uninstall: stops, disables, removes the unit, and reloads the daemon", async () => {

    const installPath = "/Users/test/.config/systemd/user/prismcast.service";
    const { calls, io } = makeFakeIO({

      execFile: execFileFromMap({


        "systemctl --user daemon-reload": { stdout: "" },
        "systemctl --user disable prismcast.service": { stdout: "" },
        "systemctl --user stop prismcast.service": { stdout: "" }
      }),
      platform: "linux",
      serviceFilePath: installPath
    });

    await getServiceGenerator(io)?.uninstall();

    const sequence = calls.filter((c) => (c.method === "execFile") || (c.method === "rm")).map((c) => ({ args: c.args, method: c.method }));
    const stop = nthOf(sequence, 0, "uninstall step");
    const disable = nthOf(sequence, 1, "uninstall step");
    const removeUnit = nthOf(sequence, 2, "uninstall step");
    const reload = nthOf(sequence, 3, "uninstall step");

    assert.equal(stop.method, "execFile");
    assert.deepEqual(stop.args.slice(1), [ "--user", "stop", "prismcast.service" ]);
    assert.deepEqual(disable.args.slice(1), [ "--user", "disable", "prismcast.service" ]);
    assert.equal(removeUnit.method, "rm");
    assert.deepEqual(removeUnit.args, [ installPath, { force: true } ]);
    assert.deepEqual(reload.args.slice(1), [ "--user", "daemon-reload" ]);
  });
});

/* runAndSurfaceStderr is the diagnostic-enrichment wrapper that all execFile-throwing methods route through (launchctl, systemctl, powershell.exe). When a child
 * exits non-zero, runAndSurfaceStderr reads .stderr off the thrown Error - real promisified execFile produces it as either a utf8 string (default) or a Buffer
 * (encoding: "buffer") - trims it, and folds it into a new Error whose message starts with the supplied description. The original Error becomes the new
 * Error's .cause so programmatic consumers retain access to the structured failure details (.code, .signal, .stdout, .stderr).
 *
 * The tests below pin every branch of that contract: string stderr surfaced, Buffer stderr surfaced via .toString("utf8"), empty stderr falling back to the
 * error.message, and the .cause chain preserved. We exercise this through systemd start/stop which are thin wrappers over runAndSurfaceStderr-execFile pairs.
 */
describe("runAndSurfaceStderr - error message enrichment", () => {

  test("surfaces a string stderr verbatim into the rejected Error's message", async () => {

    // Real execFile with default utf8 encoding produces stderr as a string. The wrapper reads it directly and folds it into the surfaced message.
    const { io } = makeFakeIO({

      execFile: async () => {

        throw makeExecFileError("Command failed", "Failed to start prismcast.service: Unit not found.", "");
      },
      platform: "linux"
    });

    await assert.rejects(
      () => getServiceGenerator(io)?.start() ?? Promise.resolve(),
      (error: Error): boolean => {

        assert.match(error.message, /^systemctl start failed: /);
        assert.match(error.message, /Failed to start prismcast\.service: Unit not found\./);

        return true;
      }
    );
  });

  test("surfaces a Buffer stderr via toString('utf8')", async () => {

    // The encoding: "buffer" execFile config produces Buffer-shaped stderr. runAndSurfaceStderr's Buffer.isBuffer branch decodes it to utf8 before folding.
    const { io } = makeFakeIO({

      execFile: async () => {

        throw makeExecFileError("Command failed", Buffer.from("Failed to stop prismcast.service: Process not running.", "utf8"), Buffer.alloc(0));
      },
      platform: "linux"
    });

    await assert.rejects(
      () => getServiceGenerator(io)?.stop() ?? Promise.resolve(),
      (error: Error): boolean => {

        assert.match(error.message, /^systemctl stop failed: /);
        assert.match(error.message, /Failed to stop prismcast\.service: Process not running\./);

        return true;
      }
    );
  });

  test("falls back to error.message when stderr is empty", async () => {

    // When the child exited non-zero but wrote nothing to stderr, the wrapper falls back to the original Error.message so the surfaced error still carries
    // some context. Without this fallback, users would see "launchctl unload failed: " with no further detail.
    const { io } = makeFakeIO({

      execFile: async () => {

        throw makeExecFileError("Underlying execFile message", "", "");
      },
      platform: "linux"
    });

    await assert.rejects(
      () => getServiceGenerator(io)?.start() ?? Promise.resolve(),
      (error: Error): boolean => {

        assert.match(error.message, /^systemctl start failed: Underlying execFile message$/);

        return true;
      }
    );
  });

  test("trims surrounding whitespace from stderr before folding it into the message", async () => {

    // Real systemctl/launchctl frequently emit a trailing newline. The wrapper trims it so the surfaced message is clean.
    const { io } = makeFakeIO({

      execFile: async () => {

        throw makeExecFileError("Command failed", "  Failed to start prismcast.service.\n\n", "");
      },
      platform: "linux"
    });

    await assert.rejects(
      () => getServiceGenerator(io)?.start() ?? Promise.resolve(),
      (error: Error): boolean => {

        // Note the absence of leading whitespace and trailing newline.
        assert.match(error.message, /^systemctl start failed: Failed to start prismcast\.service\.$/);

        return true;
      }
    );
  });

  test("preserves the original Error as the surfaced Error's .cause for programmatic access", async () => {

    // Programmatic consumers sometimes need to inspect the structured failure (.code, .signal). The wrapper attaches the original via Error.cause so that data
    // is reachable through the cause chain even after the surfacing wrap.
    const original = makeExecFileError("Original execFile failure", "stderr text", "stdout text");
    const { io } = makeFakeIO({

      execFile: async () => {

        throw original;
      },
      platform: "linux"
    });

    await assert.rejects(
      () => getServiceGenerator(io)?.start() ?? Promise.resolve(),
      (error: Error): boolean => {

        assert.equal(error.cause, original, "cause chain points back at the original execFile error");

        return true;
      }
    );
  });
});

describe("createWindowsSchedulerGenerator (via getServiceGenerator on windows)", () => {

  test("install: writes the launcher .ps1 with BOM, registers the task, and starts it", async () => {

    const launcherPath = "C:\\ProgramData\\PrismCast\\prismcast-service.ps1";
    const { calls, io, writes } = makeFakeIO({

      // PowerShell invocations during install carry full script bodies whose exact text depends on runtime data; keying them in a record would be brittle. The
      // wildcard execFile lets every powershell.exe call succeed uniformly while makeFakeIO records the call for sequence assertions below.
      execFile: execFileAlwaysSucceeds(),
      platform: "windows",
      serviceFileDirectory: "C:\\ProgramData\\PrismCast",
      serviceFilePath: launcherPath
    });

    await getServiceGenerator(io)?.install(definitionFixture());

    // Verify the launcher was written with a BOM and contains the metadata comments + Start-Process invocation.
    assert.equal(writes.length, 1);

    const launcherWrite = firstOf(writes, "launcher write");

    assert.equal(launcherWrite.path, launcherPath);
    assert.equal(launcherWrite.content.charCodeAt(0), 0xFEFF, "launcher starts with UTF-8 BOM");
    assert.match(launcherWrite.content, /^# node: \/usr\/local\/bin\/node$/m);
    assert.match(launcherWrite.content, /^# entry: \/usr\/local\/lib\/prismcast\/dist\/index\.js$/m);
    assert.match(launcherWrite.content, /Start-Process @startArgs/);

    // Verify two PowerShell invocations: register, then start. (The legacy artifact removal happens via rm, not execFile.)
    const psCalls = calls.filter((c) => (c.method === "execFile") && (c.args[0] === "powershell.exe"));

    assert.equal(psCalls.length, 2);

    const registerCall = nthOf(psCalls, 0, "PowerShell call");
    const startCall = nthOf(psCalls, 1, "PowerShell call");

    // The full command string is the last argv element; it should reference Register-ScheduledTask in the first call.
    assert.match(String(registerCall.args.at(-1)), /Register-ScheduledTask/);
    assert.match(String(startCall.args.at(-1)), /Start-ScheduledTask/);
  });

  test("isInstalled: returns true when the IS_INSTALLED PowerShell script exits 0", async () => {

    const { io } = makeFakeIO({ execFile: execFileAlwaysSucceeds(), platform: "windows" });

    assert.equal(await getServiceGenerator(io)?.isInstalled(), true);
  });

  test("isInstalled: returns false when the IS_INSTALLED PowerShell script throws (non-zero exit)", async () => {

    // The wildcard execFile here always throws an Error in the shape promisified execFile produces on a non-zero exit (utf8 strings for stdout/stderr).
    const { io } = makeFakeIO({

      execFile: async (): Promise<{ stderr: string; stdout: string }> => {

        throw makeExecFileError("fake-throw", "", "");
      },
      platform: "windows"
    });

    assert.equal(await getServiceGenerator(io)?.isInstalled(), false);
  });

  test("isRunning: returns true when the IS_RUNNING script exits 0", async () => {

    const { io } = makeFakeIO({ execFile: execFileAlwaysSucceeds(), platform: "windows" });

    assert.equal(await getServiceGenerator(io)?.isRunning(), true);
  });
});

describe("getServiceGenerator - platform dispatch", () => {

  test("returns null for unsupported platforms", () => {

    const { io } = makeFakeIO({ platform: "openbsd" as Platform });

    assert.equal(getServiceGenerator(io), null);
  });

  test("returns the launchd generator on darwin", () => {

    const { io } = makeFakeIO({ platform: "darwin" });

    assert.equal(getServiceGenerator(io)?.serviceManager, "launchd");
    assert.equal(getServiceGenerator(io)?.platform, "darwin");
  });

  test("returns the systemd generator on linux", () => {

    const { io } = makeFakeIO({ platform: "linux" });

    assert.equal(getServiceGenerator(io)?.serviceManager, "systemd");
    assert.equal(getServiceGenerator(io)?.platform, "linux");
  });

  test("returns the windows-scheduler generator on windows", () => {

    const { io } = makeFakeIO({ platform: "windows" });

    assert.equal(getServiceGenerator(io)?.serviceManager, "windows-scheduler");
    assert.equal(getServiceGenerator(io)?.platform, "windows");
  });
});

describe("getServicePaths - with injected IO", () => {

  test("returns null when the service file does not exist", () => {

    const { io } = makeFakeIO({ existing: {}, platform: "darwin" });

    assert.equal(getServicePaths(io), null);
  });

  test("parses the launchd plist and returns nodePath + entryPoint", () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const plist = [
      "<?xml version=\"1.0\"?>",
      "<plist>",
      "<dict>",
      "  <key>ProgramArguments</key>",
      "  <array>",
      "    <string>/opt/homebrew/bin/node</string>",
      "    <string>/opt/homebrew/lib/prismcast/dist/index.js</string>",
      "  </array>",
      "</dict>",
      "</plist>"
    ].join("\n");
    const { io } = makeFakeIO({

      existing: { [installPath]: true },
      files: { [installPath]: plist },
      platform: "darwin",
      serviceFilePath: installPath
    });

    const result = getServicePaths(io);

    assert.deepEqual(result, {

      entryPoint: "/opt/homebrew/lib/prismcast/dist/index.js",
      nodePath: "/opt/homebrew/bin/node"
    });
  });

  test("returns null when a launchd plist lacks the ProgramArguments block", () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const { io } = makeFakeIO({

      existing: { [installPath]: true },
      files: { [installPath]: "<?xml version=\"1.0\"?><plist><dict></dict></plist>" },
      platform: "darwin",
      serviceFilePath: installPath
    });

    assert.equal(getServicePaths(io), null);
  });

  test("parses the systemd unit's ExecStart line", () => {

    const installPath = "/Users/test/.config/systemd/user/prismcast.service";
    const unit = [
      "[Unit]",
      "Description=PrismCast",
      "[Service]",
      "ExecStart=/usr/local/bin/node /usr/local/lib/prismcast/dist/index.js",
      "Restart=always"
    ].join("\n");
    const { io } = makeFakeIO({

      existing: { [installPath]: true },
      files: { [installPath]: unit },
      platform: "linux",
      serviceFilePath: installPath
    });

    const result = getServicePaths(io);

    assert.deepEqual(result, {

      entryPoint: "/usr/local/lib/prismcast/dist/index.js",
      nodePath: "/usr/local/bin/node"
    });
  });

  test("parses the Windows launcher's '# node:' and '# entry:' metadata comments", () => {

    const launcherPath = "C:\\ProgramData\\PrismCast\\prismcast-service.ps1";
    const launcher = [
      "# PrismCast service launcher.",
      "# node: C:\\Program Files\\nodejs\\node.exe",
      "# entry: C:\\Users\\test\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\index.js",
      "$env:PRISMCAST_SERVICE = '1'",
      "$startArgs = @{}",
      "Start-Process @startArgs"
    ].join("\r\n");
    const { io } = makeFakeIO({

      existing: { [launcherPath]: true },
      files: { [launcherPath]: launcher },
      platform: "windows",
      serviceFilePath: launcherPath
    });

    const result = getServicePaths(io);

    assert.deepEqual(result, {

      entryPoint: "C:\\Users\\test\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\index.js",
      nodePath: "C:\\Program Files\\nodejs\\node.exe"
    });
  });
});

describe("detectStalePaths - with injected IO", () => {

  test("returns null when there is no service file", () => {

    const { io } = makeFakeIO({ existing: {}, platform: "darwin" });

    assert.equal(detectStalePaths(io), null);
  });

  test("returns stale: false when both paths still exist", () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const nodePath = "/usr/local/bin/node";
    const entryPoint = "/usr/local/lib/prismcast/dist/index.js";
    const plist = "<key>ProgramArguments</key><array><string>" + nodePath + "</string><string>" + entryPoint + "</string></array>";
    const { io } = makeFakeIO({

      existing: { [entryPoint]: true, [installPath]: true, [nodePath]: true },
      files: { [installPath]: plist },
      platform: "darwin",
      serviceFilePath: installPath
    });

    const result = detectStalePaths(io);

    assert.ok(result, "detectStalePaths returned a result");
    assert.equal(result.stale, false);
    assert.equal(result.entryPoint, undefined, "no stale entryPoint surfaced when path exists");
    assert.equal(result.nodePath, undefined, "no stale nodePath surfaced when path exists");
  });

  test("returns stale: true with the nodePath surfaced when the node binary is gone", () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const nodePath = "/old/node";
    const entryPoint = "/usr/local/lib/prismcast/dist/index.js";
    const plist = "<key>ProgramArguments</key><array><string>" + nodePath + "</string><string>" + entryPoint + "</string></array>";
    const { io } = makeFakeIO({

      existing: { [entryPoint]: true, [installPath]: true },
      files: { [installPath]: plist },
      platform: "darwin",
      serviceFilePath: installPath
    });

    const result = detectStalePaths(io);

    assert.ok(result);
    assert.equal(result.stale, true);
    assert.equal(result.nodePath, nodePath, "stale node path is reported");
    assert.equal(result.entryPoint, undefined, "live entry point is not flagged");
  });
});
