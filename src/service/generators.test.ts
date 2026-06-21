/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.test.ts: Unit tests for the pure-leaf surface in generators.ts - collectServiceEnvironment, buildServiceDefinition, the getServiceGenerator dispatch,
 * the ServiceDefinition shape contract, and the getServiceGenerator platform-dispatch tests. Platform-specific generator factories are split per platform:
 * createLaunchdGenerator (macOS) lives in generators.launchAgent.test.ts; createSystemdGenerator + runAndSurfaceStderr live in generators.systemd.test.ts;
 * createWindowsSchedulerGenerator lives in generators.windowsTask.test.ts. The path-resolution helpers (getServicePaths, detectStalePaths) and their injected-IO
 * variants live in generators.paths.test.ts.
 *
 * The install/start/stop/uninstall paths on each generator spawn external subprocesses (launchctl, systemctl, powershell.exe); those branches are intentionally
 * NOT tested in this tier - they require real OS state and belong in e2e coverage.
 */
import { afterEach, before, beforeEach, describe, test } from "node:test";
import { buildServiceDefinition, collectServiceEnvironment, getServiceGenerator } from "./generators.ts";
import type { Platform } from "../utils/platform.ts";
import type { ServiceDefinition } from "./generators.ts";
import assert from "node:assert/strict";
import { initializeDataDir } from "../config/paths.ts";
import { makeFakeIO } from "./generators.helpers.ts";
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

  // Each case: processValue is the raw process.platform string we stub in; platform and manager are the expected ServiceGenerator fields the dispatch should return.
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
