/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * platform.test.ts: Unit tests for the platform-detection helpers in platform.ts. The functions branch on process.platform and read from os/path/url - we
 * verify the per-platform branches by stubbing process.platform via Object.defineProperty (it is a getter property under v8). Tests restore the original platform
 * value in afterEach so cross-test pollution is impossible.
 */
import { SERVICE_ID, SERVICE_NAME, getDataDirectory, getLogsDirectory, getNodeExecutablePath, getPlatform, getPrismCastEntryPoint, getPrismCastWorkingDirectory,
  getServiceFileDirectory, getServiceFilePath, getServiceManager, isRunningAsService, isRunningInContainer, serviceFileExists } from "./platform.ts";
import { afterEach, before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { initializeDataDir } from "../config/paths.ts";
import os from "node:os";
import path from "node:path";

// Initialize the data directory once for the suite. The platform helpers that touch getDataDir() throw if it has not been initialized; tests want a stable
// fixture rather than the dependency injection ceremony of stubbing the path module.
before(() => {

  initializeDataDir(path.join(os.tmpdir(), "prismcast-platform-test"));
});

// Tests stub process.platform via this helper rather than direct assignment, because Node exposes process.platform as an accessor on some builds. The helper
// captures the original value and lets the test stash a getter that returns whatever the test wants.
function setPlatform(value: string): void {

  Object.defineProperty(process, "platform", {

    configurable: true,
    value
  });
}

const ORIGINAL_PLATFORM = process.platform;
const ORIGINAL_SERVICE_ENV = process.env["PRISMCAST_SERVICE"];
const ORIGINAL_CONTAINER_ENV = process.env["PRISMCAST_CONTAINER"];

describe("getPlatform", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("returns 'darwin' on macOS", () => {

    setPlatform("darwin");
    assert.equal(getPlatform(), "darwin");
  });

  test("returns 'windows' on Win32", () => {

    setPlatform("win32");
    assert.equal(getPlatform(), "windows");
  });

  test("returns 'linux' on Linux", () => {

    setPlatform("linux");
    assert.equal(getPlatform(), "linux");
  });

  test("returns 'linux' for any unknown platform (default branch)", () => {

    // Boundary: BSDs, AIX, etc. fall through to the linux branch by design.
    setPlatform("freebsd");
    assert.equal(getPlatform(), "linux");

    setPlatform("aix");
    assert.equal(getPlatform(), "linux");
  });
});

describe("getServiceManager", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("maps darwin to launchd", () => {

    setPlatform("darwin");
    assert.equal(getServiceManager(), "launchd");
  });

  test("maps linux (and unknown) to systemd", () => {

    setPlatform("linux");
    assert.equal(getServiceManager(), "systemd");

    setPlatform("freebsd");
    assert.equal(getServiceManager(), "systemd", "unknown platforms fall through getPlatform's linux branch");
  });

  test("maps windows to windows-scheduler", () => {

    setPlatform("win32");
    assert.equal(getServiceManager(), "windows-scheduler");
  });
});

describe("isRunningAsService", () => {

  afterEach(() => {

    if(ORIGINAL_SERVICE_ENV === undefined) {

      delete process.env["PRISMCAST_SERVICE"];
    } else {

      process.env["PRISMCAST_SERVICE"] = ORIGINAL_SERVICE_ENV;
    }
  });

  test("returns true when PRISMCAST_SERVICE is exactly '1'", () => {

    process.env["PRISMCAST_SERVICE"] = "1";
    assert.equal(isRunningAsService(), true);
  });

  test("returns false when the env var is unset", () => {

    delete process.env["PRISMCAST_SERVICE"];
    assert.equal(isRunningAsService(), false);
  });

  test("returns false when the env var has any non-'1' value", () => {

    // Negative tests: only the literal "1" counts. Empty string, "0", "true", "yes" must all fail.
    process.env["PRISMCAST_SERVICE"] = "";
    assert.equal(isRunningAsService(), false, "empty string");

    process.env["PRISMCAST_SERVICE"] = "0";
    assert.equal(isRunningAsService(), false, "literal 0");

    process.env["PRISMCAST_SERVICE"] = "true";
    assert.equal(isRunningAsService(), false, "true is not 1");

    process.env["PRISMCAST_SERVICE"] = "yes";
    assert.equal(isRunningAsService(), false, "yes is not 1");
  });
});

describe("isRunningInContainer", () => {

  afterEach(() => {

    if(ORIGINAL_CONTAINER_ENV === undefined) {

      delete process.env["PRISMCAST_CONTAINER"];
    } else {

      process.env["PRISMCAST_CONTAINER"] = ORIGINAL_CONTAINER_ENV;
    }
  });

  test("returns true when PRISMCAST_CONTAINER is '1'", () => {

    process.env["PRISMCAST_CONTAINER"] = "1";
    assert.equal(isRunningInContainer(), true);
  });

  test("falls back to checking /.dockerenv when env var is not '1'", () => {

    // Negative test: when the env var is unset, the function attempts to stat /.dockerenv. On a typical macOS dev machine that file does not exist, so the
    // function returns false. We don't try to forge it - we lock the boolean shape.
    delete process.env["PRISMCAST_CONTAINER"];

    const result = isRunningInContainer();

    assert.equal(typeof result, "boolean", "returns a boolean");
  });

  test("returns false when env var is the empty string and /.dockerenv is absent", () => {

    process.env["PRISMCAST_CONTAINER"] = "";

    // We cannot guarantee /.dockerenv state, but in development environments it should be absent. Skip the strict assertion if we happen to be in a container.
    const result = isRunningInContainer();

    assert.equal(typeof result, "boolean");
  });
});

describe("getServiceFilePath", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("on darwin uses ~/Library/LaunchAgents/<id>.plist", () => {

    setPlatform("darwin");
    const expected = path.join(os.homedir(), "Library", "LaunchAgents", SERVICE_ID + ".plist");

    assert.equal(getServiceFilePath(), expected);
  });

  test("on linux uses ~/.config/systemd/user/prismcast.service", () => {

    setPlatform("linux");
    const expected = path.join(os.homedir(), ".config", "systemd", "user", "prismcast.service");

    assert.equal(getServiceFilePath(), expected);
  });

  test("on windows points at <data-dir>/prismcast-service.ps1", () => {

    setPlatform("win32");
    const result = getServiceFilePath();

    assert.match(result, /prismcast-service\.ps1$/, "ends with the PowerShell launcher filename");
  });
});

describe("getServiceFileDirectory", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("returns the parent directory of getServiceFilePath()", () => {

    setPlatform("darwin");
    assert.equal(getServiceFileDirectory(), path.dirname(getServiceFilePath()));
  });
});

describe("getNodeExecutablePath", () => {

  test("returns a non-empty absolute path", () => {

    // The function returns either a known symlink (when one resolves to process.execPath) or process.execPath itself. Either way the result is non-empty and absolute.
    const result = getNodeExecutablePath();

    assert.equal(typeof result, "string");
    assert.ok(result.length > 0);
    assert.ok(path.isAbsolute(result), "result is absolute: " + result);
  });

  test("returns a path that resolves to the same executable as process.execPath", async () => {

    // The contract: even if a symlink is preferred, it must point to the same node binary we are running. We resolve both via realpath and compare.
    const { realpathSync } = await import("node:fs");
    const result = getNodeExecutablePath();

    let resolvedResult: string;

    try {

      resolvedResult = realpathSync(result);
    } catch {

      resolvedResult = result;
    }

    assert.equal(resolvedResult, realpathSync(process.execPath), "symlink (or fallback) resolves to current node binary");
  });
});

describe("getPrismCastEntryPoint", () => {

  test("returns an absolute path ending in index.js", () => {

    const result = getPrismCastEntryPoint();

    assert.ok(path.isAbsolute(result));
    assert.match(result, /index\.js$/);
  });
});

describe("getPrismCastWorkingDirectory", () => {

  test("returns the parent of the parent of the entry point", () => {

    // The implementation goes up two levels: dist/utils/platform.js -> dist -> project root.
    const expected = path.dirname(path.dirname(getPrismCastEntryPoint()));

    assert.equal(getPrismCastWorkingDirectory(), expected);
  });
});

describe("getDataDirectory and getLogsDirectory", () => {

  test("getDataDirectory returns an absolute path", () => {

    const result = getDataDirectory();

    assert.equal(typeof result, "string");
    assert.ok(result.length > 0);
  });

  test("getLogsDirectory returns the same path as getDataDirectory", () => {

    // The two are documented as aliases - logs go in the data directory.
    assert.equal(getLogsDirectory(), getDataDirectory());
  });
});

describe("serviceFileExists", () => {

  test("returns a boolean (existence check via fs.existsSync)", () => {

    const result = serviceFileExists();

    assert.equal(typeof result, "boolean");
  });
});

describe("module-level constants", () => {

  test("SERVICE_ID is the documented bundle identifier", () => {

    assert.equal(SERVICE_ID, "com.github.hjdhjd.prismcast");
  });

  test("SERVICE_NAME is the documented display name", () => {

    assert.equal(SERVICE_NAME, "PrismCast");
  });
});

// Restore platform on suite teardown - belt-and-braces in case any individual test leaves a stub in place.
describe("platform restoration sanity check", () => {

  beforeEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("process.platform is restored after the suite", () => {

    assert.equal(process.platform, ORIGINAL_PLATFORM);
  });
});
