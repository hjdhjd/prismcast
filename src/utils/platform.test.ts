/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * platform.test.ts: Unit tests for the platform-detection helpers in platform.ts. The functions branch on process.platform and read from fs/path/url - we
 * verify the per-platform branches by stubbing process.platform via Object.defineProperty (it is a getter property under v8). Tests restore the original platform
 * value in afterEach so cross-test pollution is impossible.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { getNodeExecutablePath, getPlatform, getPrismCastEntryPoint, getPrismCastWorkingDirectory, getServiceManager, isRunningAsService,
  isRunningInContainer } from "./platform.ts";
import assert from "node:assert/strict";
import path from "node:path";

// Tests stub process.platform via this helper rather than direct assignment, because Node exposes process.platform as an accessor on some builds. The helper
// installs a configurable data property that overrides process.platform with the requested value; the original is captured once at module scope as
// ORIGINAL_PLATFORM and restored in each stubbing describe's afterEach.
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

  test("returns true via the /.dockerenv fallback when the env var is unset and the marker file exists", async () => {

    // The fallback branch calls fs.existsSync("/.dockerenv"). We can't (and shouldn't) create that file on the host - it's a Docker convention - but the
    // platform module captures fs via `import fs from "node:fs"`, so mock.method on the fs default-export's existsSync property substitutes the probe at
    // runtime. The mock is reverted in this test's own finally block via mock.reset().
    delete process.env["PRISMCAST_CONTAINER"];

    const fs = await import("node:fs");
    const existsCalls: string[] = [];

    mock.method(fs.default, "existsSync", (p: string): boolean => {

      existsCalls.push(p);

      return p === "/.dockerenv";
    });

    try {

      assert.equal(isRunningInContainer(), true, "/.dockerenv fallback returns true");
      assert.ok(existsCalls.includes("/.dockerenv"), "the marker path was probed via existsSync");
    } finally {

      mock.reset();
    }
  });

  test("returns false when /.dockerenv probe throws (catch path absorbs filesystem errors)", async () => {

    // Boundary: the try/catch around fs.existsSync absorbs any error from the probe (permission denied, EIO on a degraded filesystem, etc.) and treats it as
    // "not in a container."
    delete process.env["PRISMCAST_CONTAINER"];

    const fs = await import("node:fs");

    mock.method(fs.default, "existsSync", (): boolean => { throw new Error("synthetic fs failure"); });

    try {

      assert.equal(isRunningInContainer(), false, "throwing existsSync -> caller sees false");
    } finally {

      mock.reset();
    }
  });

  test("returns false when the env var is unset and /.dockerenv is absent (default-host case)", () => {

    // Without the mock, on a typical dev host, /.dockerenv does not exist. The function returns false. This asserts the behavior that survives even when no
    // mocking is in play - and complements the mocked tests above which lock the explicit branches.
    delete process.env["PRISMCAST_CONTAINER"];

    const result = isRunningInContainer();

    // On a CI host that happens to be containerized, the result is true; on dev machines it is false. We can't assert the exact value without knowing the host,
    // so we lock the structural contract: returns a boolean and does not throw.
    assert.equal(typeof result, "boolean", "returns a boolean even on hosts without /.dockerenv");
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

    // The implementation goes up two levels from the entry point: dist/index.js -> dist -> project root.
    const expected = path.dirname(path.dirname(getPrismCastEntryPoint()));

    assert.equal(getPrismCastWorkingDirectory(), expected);
  });
});

// A final belt-and-braces restore, run as a beforeEach ahead of the sanity-check assertion, in case any earlier test left a platform stub in place.
describe("platform restoration sanity check", () => {

  beforeEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("process.platform is restored after the suite", () => {

    assert.equal(process.platform, ORIGINAL_PLATFORM);
  });
});
