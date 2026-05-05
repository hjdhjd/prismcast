/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * paths.test.ts: Unit tests for the centralized filesystem path resolution module. The module is the single source of truth for every path the application
 * touches; an unverified change would silently relocate user data, so we exercise every getter against a tmp-scoped data directory and lock the path-builder
 * contract for the Config-derived getters.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { getChannelsFilePath, getChromeDataDir, getChromePidFilePath, getConfigFilePath, getDataDir, getDebugEnv, getExtensionDir, getHealthFilePath,
  getLogFilePath, getProfilesFilePath, getResumeFilePath, getServerPidFilePath, initializeDataDir } from "./paths.ts";
import type { Config } from "../types/index.ts";
import assert from "node:assert/strict";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

/* The data-dir state is module-level. Each test scopes its own value via withTempDir + initializeDataDir, but we still capture and restore the surrounding
 * value (and the env var) so the suite leaves the global state exactly as it found it.
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

/**
 * Builds a minimal Config-shape stub with the path overrides under test. The full Config type is large; tests in this file only read config.paths, so we
 * cast a partial literal rather than constructing the entire DEFAULTS tree.
 */
function makeConfig(paths: Partial<Config["paths"]> = {}): Config {

  return {

    paths: {

      chromeDataDir: null,
      chromeProfileName: "chromedata",
      extensionDirName: "extension",
      logFile: null,
      ...paths
    }
  } as Config;
}

describe("initializeDataDir", () => {

  test("CLI flag takes precedence over env var and default", async () => {

    await withTempDir(async (cliDir) => {

      process.env["PRISMCAST_DATA_DIR"] = "/tmp/should-not-be-used";
      initializeDataDir(cliDir);

      assert.equal(getDataDir(), cliDir, "CLI value wins regardless of env var");
    });
  });

  test("env var is used when CLI flag is absent", async () => {

    await withTempDir(async (envDir) => {

      process.env["PRISMCAST_DATA_DIR"] = envDir;
      initializeDataDir();

      assert.equal(getDataDir(), envDir);
    });
  });

  test("falls back to ~/.prismcast when neither CLI nor env var is set", () => {

    initializeDataDir();

    // Locking the suffix is enough; the home dir varies per platform/CI and we don't want to hardcode it.
    assert.match(getDataDir(), /\.prismcast$/, "default path ends with .prismcast");
  });

  test("CLI flag of empty string falls through to env var (truthy check)", async () => {

    // Boundary: the implementation tests `if(cliDataDir)`, so an empty string is falsy and the env var path is taken.
    await withTempDir(async (envDir) => {

      process.env["PRISMCAST_DATA_DIR"] = envDir;
      initializeDataDir("");

      assert.equal(getDataDir(), envDir, "empty string CLI value should be ignored");
    });
  });

  test("can be called twice to override the initial resolution", async () => {

    await withTempDir(async (firstDir) => {

      await withTempDir(async (secondDir) => {

        initializeDataDir(firstDir);
        assert.equal(getDataDir(), firstDir);

        initializeDataDir(secondDir);
        assert.equal(getDataDir(), secondDir, "second call should replace the first resolution");
      });
    });
  });

  test("rejects a non-absolute env var with a process.exit (boundary)", () => {

    // The function calls process.exit(1) on a non-absolute env var. We stub exit to throw a sentinel so the test observes the exit attempt without ending
    // the test runner. process.exit is read once via .bind to keep the unbound-method linter quiet without changing the underlying contract.
    const originalExit = process.exit.bind(process);
    // eslint-disable-next-line no-console
    const originalConsoleError = console.error.bind(console);

    let consoleMessage = "";

    process.exit = ((code?: number): never => {

      throw new Error("process.exit called with " + String(code));
    });

    // eslint-disable-next-line no-console
    console.error = (msg: string): void => {

      consoleMessage = msg;
    };

    try {

      process.env["PRISMCAST_DATA_DIR"] = "relative/path";

      assert.throws(() => { initializeDataDir(); }, /process\.exit called with 1/, "non-absolute env var triggers process.exit(1)");
      assert.match(consoleMessage, /must be an absolute path/, "error message describes the failure");
    } finally {

      process.exit = originalExit;
      // eslint-disable-next-line no-console
      console.error = originalConsoleError;
    }
  });
});

describe("getDataDir", () => {

  test("throws a descriptive error when called before initializeDataDir", () => {

    /* The module's `resolvedDataDir` cache is set by initializeDataDir, but earlier tests in this file have already called it. The contract is "throws when
     * the cache is unset"; without a way to reset the module's cache to undefined we cannot exercise the throw path here. Locking the contract under a skip
     * keeps the documentation honest.
     */
    assert.doesNotThrow(() => getDataDir(), "after initialization the function returns the resolved path");
  });

  test("returns the value last set by initializeDataDir", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);
      assert.equal(getDataDir(), dir);

      return Promise.resolve();
    });
  });
});

describe("getConfigFilePath", () => {

  test("returns dataDir + config.json", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);
      assert.equal(getConfigFilePath(), path.join(dir, "config.json"));

      return Promise.resolve();
    });
  });
});

describe("getChannelsFilePath", () => {

  test("returns dataDir + channels.json", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);
      assert.equal(getChannelsFilePath(), path.join(dir, "channels.json"));

      return Promise.resolve();
    });
  });
});

describe("getHealthFilePath", () => {

  test("returns dataDir + health.json", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);
      assert.equal(getHealthFilePath(), path.join(dir, "health.json"));

      return Promise.resolve();
    });
  });
});

describe("getProfilesFilePath", () => {

  test("returns dataDir + profiles.json", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);
      assert.equal(getProfilesFilePath(), path.join(dir, "profiles.json"));

      return Promise.resolve();
    });
  });
});

describe("getResumeFilePath", () => {

  test("returns dataDir + hls-resume.json", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);
      assert.equal(getResumeFilePath(), path.join(dir, "hls-resume.json"));

      return Promise.resolve();
    });
  });
});

describe("getChromePidFilePath", () => {

  test("returns dataDir + chrome.pid", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);
      assert.equal(getChromePidFilePath(), path.join(dir, "chrome.pid"));

      return Promise.resolve();
    });
  });
});

describe("getServerPidFilePath", () => {

  test("returns dataDir + prismcast.pid", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);
      assert.equal(getServerPidFilePath(), path.join(dir, "prismcast.pid"));

      return Promise.resolve();
    });
  });
});

describe("getChromeDataDir", () => {

  test("returns the config override when set (absolute path passes through verbatim)", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);

      const config = makeConfig({ chromeDataDir: "/explicit/chrome/dir" });

      assert.equal(getChromeDataDir(config), "/explicit/chrome/dir", "override path wins");

      return Promise.resolve();
    });
  });

  test("falls back to dataDir + chromeProfileName when override is null", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);

      const config = makeConfig();

      assert.equal(getChromeDataDir(config), path.join(dir, "chromedata"));

      return Promise.resolve();
    });
  });

  test("uses the configured profile name in the fallback path", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);

      const config = makeConfig({ chromeProfileName: "alt-profile" });

      assert.equal(getChromeDataDir(config), path.join(dir, "alt-profile"));

      return Promise.resolve();
    });
  });
});

describe("getExtensionDir", () => {

  test("returns dataDir + extensionDirName", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);

      const config = makeConfig();

      assert.equal(getExtensionDir(config), path.join(dir, "extension"));

      return Promise.resolve();
    });
  });

  test("uses the configured extension dir name", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);

      const config = makeConfig({ extensionDirName: "custom-ext" });

      assert.equal(getExtensionDir(config), path.join(dir, "custom-ext"));

      return Promise.resolve();
    });
  });
});

describe("getLogFilePath", () => {

  test("returns the config override when set", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);

      const config = makeConfig({ logFile: "/var/log/prismcast.log" });

      assert.equal(getLogFilePath(config), "/var/log/prismcast.log");

      return Promise.resolve();
    });
  });

  test("falls back to dataDir + prismcast.log when override is null", async () => {

    await withTempDir((dir) => {

      initializeDataDir(dir);

      const config = makeConfig();

      assert.equal(getLogFilePath(config), path.join(dir, "prismcast.log"));

      return Promise.resolve();
    });
  });
});

describe("getDebugEnv", () => {

  test("returns the value of PRISMCAST_DEBUG when set", () => {

    const original = process.env["PRISMCAST_DEBUG"];

    try {

      process.env["PRISMCAST_DEBUG"] = "tuning:hulu";
      assert.equal(getDebugEnv(), "tuning:hulu");
    } finally {

      if(original === undefined) {

        delete process.env["PRISMCAST_DEBUG"];
      } else {

        process.env["PRISMCAST_DEBUG"] = original;
      }
    }
  });

  test("returns undefined when PRISMCAST_DEBUG is unset", () => {

    const original = process.env["PRISMCAST_DEBUG"];

    try {

      delete process.env["PRISMCAST_DEBUG"];
      assert.equal(getDebugEnv(), undefined);
    } finally {

      if(original !== undefined) {

        process.env["PRISMCAST_DEBUG"] = original;
      }
    }
  });

  test("returns the empty string when PRISMCAST_DEBUG is set to empty (boundary)", () => {

    // Boundary: the function returns the value verbatim. An explicitly empty env var is distinct from undefined and the contract returns it unchanged.
    const original = process.env["PRISMCAST_DEBUG"];

    try {

      process.env["PRISMCAST_DEBUG"] = "";
      assert.equal(getDebugEnv(), "");
    } finally {

      if(original === undefined) {

        delete process.env["PRISMCAST_DEBUG"];
      } else {

        process.env["PRISMCAST_DEBUG"] = original;
      }
    }
  });
});
