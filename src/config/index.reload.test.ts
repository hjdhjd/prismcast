/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.reload.test.ts: Atomicity and validation tests for reloadConfiguration. Two contracts are exercised here, both requiring the mock.module + dynamic-
 * import dance so a stubbed readConfig binds before config/index.ts resolves its static import:
 *
 *   1. Atomicity on read failure: a thrown readConfig mid-reload must leave the in-memory CONFIG unchanged - the operator sees an error from the settings
 *      handler and the running CONFIG continues to reflect the previous valid state.
 *
 *   2. Reject-on-invalid: a merged-from-disk configuration that carries a hard error or would need a capture coercion is rejected (CONFIG untouched, every
 *      diffed change reported rejected) rather than silently coerced, while a valid change still commits and dispatches normally.
 *
 * We drive (2) by overriding readConfig to return a synthetic on-disk shape, so reloadConfiguration's real merge + validation path runs against a controlled
 * config without touching the actual config file. The companion happy-path dispatch coverage lives in reactivity.test.ts and hdhr/index.test.ts.
 */
import type * as IndexModule from "./index.ts";
import type * as UserConfigModule from "./userConfig.ts";
import { before, beforeEach, describe, mock, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { getCurrentPattern } from "../utils/index.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// Mock state. When true, the next readConfig invocation throws the captured error; tests flip the flag to arm the failure for one call.
let armReadConfigFailure = false;
const readConfigFailureMessage = "synthetic readConfig failure for atomicity test";

// When set, readConfig returns this synthetic load result instead of reading disk. Tests use it to drive reloadConfiguration's merge + validation path against
// a controlled on-disk shape (an invalid or coercion-needing config) without touching the real config file. Reset in each test's finally block by the validation suite.
let readConfigOverride: UserConfigModule.UserConfigLoadResult | null = null;

// Captures each mutateConfig mutation as a probe object so the persistCoercedConfig write-back can be asserted without touching the real config file. The mock
// invokes the caller's mutator against a fresh empty UserConfig and records the result.
let mutateConfigProbes: UserConfigModule.UserConfig[] = [];

let indexModule: typeof IndexModule;
let CONFIG: typeof IndexModule.CONFIG;
let reloadConfiguration: typeof IndexModule.reloadConfiguration;

before(async () => {

  // Capture the real exports so any name we do not override passes through unchanged. mergeConfiguration is statically imported by config/index.ts; without
  // spreading the real exports the dynamic import would resolve mergeConfiguration to undefined and fail before our throw path runs.
  const realUserConfig = await import("./userConfig.ts");

  const userConfigUrl = new URL("./userConfig.ts", import.meta.url).href;

  // The Node 22 type definitions surface the option as namedExports; the runtime renamed it to exports in a later minor and emits a deprecation warning. We
  // keep namedExports until @types/node catches up.
  mock.module(userConfigUrl, {

    namedExports: {

      ...realUserConfig,
      mutateConfig: (async (fn: (current: UserConfigModule.UserConfig) => void): Promise<void> => {

        const probe: UserConfigModule.UserConfig = {};

        fn(probe);
        mutateConfigProbes.push(probe);
      }) as typeof UserConfigModule.mutateConfig,
      readConfig: ((): ReturnType<typeof UserConfigModule.readConfig> => {

        if(armReadConfigFailure) {

          throw new Error(readConfigFailureMessage);
        }

        if(readConfigOverride) {

          return Promise.resolve(readConfigOverride);
        }

        return realUserConfig.readConfig();
      }) as typeof UserConfigModule.readConfig
    }
  });

  // Dynamic-import config/index.ts AFTER the mock is installed so its static `import { readConfig } from "./userConfig.ts"` resolves through the mock layer. A
  // static import at the top of this file would bind the real export before mock.module had a chance to register.
  indexModule = await import("./index.ts");

  CONFIG = indexModule.CONFIG;
  reloadConfiguration = indexModule.reloadConfiguration;
});

describe("reloadConfiguration - atomicity on read failure", () => {

  test("a thrown readConfig leaves CONFIG reference-identical to its pre-call value", async () => {

    armReadConfigFailure = true;
    const before = CONFIG;

    try {

      await assert.rejects(() => reloadConfiguration(), { message: readConfigFailureMessage });

      // The binding may have been reassigned by anything that imports config/index.ts and mutates CONFIG separately; the load-bearing claim is that the failed
      // reload did not commit any reassignment of its own. Comparing reference identity locks the "we did not reach the CONFIG = nextConfig line" invariant.
      assert.equal(CONFIG, before, "CONFIG binding was not reassigned by the failed reload");
    } finally {

      armReadConfigFailure = false;
    }
  });
});

describe("reloadConfiguration - rejects an invalid or coercion-needing save", () => {

  // Each test arms a synthetic on-disk shape; clear it afterward so a later test (or the atomicity suite, if reordered) reads the real config again. The reject
  // tests never commit, so CONFIG stays pristine for them; the commit test runs last and is the only one that reassigns the live binding.
  let snapshot: typeof IndexModule.CONFIG;

  before(() => {

    snapshot = indexModule.CONFIG;
  });

  test("rejects a native capture mode without committing CONFIG", async () => {

    readConfigOverride = { config: { streaming: { captureMode: "native" } }, parseError: false, recoveredFromBackup: false };

    try {

      const result = await reloadConfiguration();

      // The live binding stays on the previous valid state - native mode never becomes live - and the operator is told why.
      assert.equal(indexModule.CONFIG, snapshot, "CONFIG binding was not reassigned");
      assert.equal(indexModule.CONFIG.streaming.captureMode, "ffmpeg", "live capture mode stays ffmpeg");
      assert.equal(result.applied.length, 0);
      assert.equal(result.deferred.length, 0);
      assert.deepEqual(result.rejected.map((r) => r.change.path), ["streaming.captureMode"]);
      assert.ok(result.rejected.every((r) => r.reason.includes("capture mode")), "the rejection reason names the capture mode");
    } finally {

      readConfigOverride = null;
    }
  });

  test("a rejected reload does not apply a bundled debug-filter change to the runtime filter", async () => {

    // The save combines a reject-triggering native captureMode with a debugFilter edit. Because the whole save is rejected, the runtime debug filter - a global
    // side effect - must stay untouched: the live-apply (commitDebugFilter) runs only after a reload commits, never on the reject path.
    const before = indexModule.CONFIG;

    readConfigOverride = { config: { logging: { debugFilter: "tuning:hulu" }, streaming: { captureMode: "native" } }, parseError: false, recoveredFromBackup: false };

    try {

      const result = await reloadConfiguration();

      assert.equal(indexModule.CONFIG, before, "CONFIG binding was not reassigned");
      assert.equal(result.applied.length, 0, "nothing applied on the reject path");
      assert.equal(result.deferred.length, 0, "nothing deferred on the reject path");
      assert.equal(getCurrentPattern(), "", "the runtime debug filter was not changed by the rejected reload");
    } finally {

      readConfigOverride = null;
    }
  });

  test("rejects an out-of-range port and surfaces the hard-error reason", async () => {

    readConfigOverride = { config: { server: { port: 0 } }, parseError: false, recoveredFromBackup: false };

    try {

      const result = await reloadConfiguration();

      assert.equal(indexModule.CONFIG, snapshot, "CONFIG binding was not reassigned");
      assert.deepEqual(result.rejected.map((r) => r.change.path), ["server.port"]);
      assert.ok(result.rejected.every((r) => r.reason.includes("PORT")), "the rejection reason names the PORT hard error");
    } finally {

      readConfigOverride = null;
    }
  });

  test("commits a valid change and dispatches it (deferred with no handler registered)", async () => {

    readConfigOverride = { config: { server: { port: 6000 } }, parseError: false, recoveredFromBackup: false };

    try {

      const result = await reloadConfiguration();

      // A valid save is not blocked by the reject path: CONFIG commits and the change dispatches. With no "server." handler registered in this process it lands
      // in the deferred bucket, which is exactly the legacy "restart to apply" behavior for an un-opted-in subsystem.
      assert.equal(result.rejected.length, 0);
      assert.equal(indexModule.CONFIG.server.port, 6000, "the valid change was committed to the live binding");
      assert.deepEqual(result.deferred.map((d) => d.change.path), ["server.port"]);
    } finally {

      readConfigOverride = null;
    }
  });
});

describe("persistCoercedConfig - startup capture write-back", () => {

  beforeEach(() => {

    mutateConfigProbes = [];
  });

  test("writes the coerced capture settings to disk when validateConfiguration coerced a value", async () => {

    indexModule.CONFIG.streaming.captureMode = "native";

    // validateConfiguration coerces native -> ffmpeg in place and records that a coercion happened; persistCoercedConfig then writes the corrected value back so
    // the on-disk state matches the live CONFIG and no phantom capture diff can arise on a later reload.
    indexModule.validateConfiguration();
    assert.equal(indexModule.CONFIG.streaming.captureMode, "ffmpeg");

    await indexModule.persistCoercedConfig();

    assert.equal(mutateConfigProbes.length, 1, "the coercion is written back to disk");
    assert.ok(mutateConfigProbes.every((p) => (p.streaming?.captureMode === "ffmpeg")), "the persisted capture mode is the coerced ffmpeg value");
  });

  test("does not write to disk when no coercion was needed", async () => {

    indexModule.CONFIG.streaming.captureMode = "ffmpeg";

    indexModule.validateConfiguration();
    await indexModule.persistCoercedConfig();

    assert.equal(mutateConfigProbes.length, 0, "no write-back without a coercion");
  });
});
