/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.reload.test.ts: Atomicity and validation tests for reloadConfiguration, plus the persistCoercedConfig write-back. Two reload contracts are exercised:
 *
 *   1. Atomicity on read failure: a thrown readConfig mid-reload must leave the in-memory CONFIG unchanged - the operator sees an error from the settings
 *      handler and the running CONFIG continues to reflect the previous valid state.
 *
 *   2. Reject-on-invalid: a merged-from-disk configuration that carries a hard error or would need a capture coercion is rejected (CONFIG untouched, every
 *      diffed change reported rejected) rather than silently coerced, while a valid change still commits and dispatches normally.
 *
 * config/index.ts composes its disk-persistence I/O behind the injectable ConfigStore port (readConfig/mutateConfig, defaulting to the real file store). We pass
 * an in-memory store at that boundary: readConfig returns a synthetic on-disk shape (or throws) so reloadConfiguration's real merge + validation path runs against a
 * controlled config without touching the real file, and mutateConfig records each write-back as a probe. The companion happy-path dispatch coverage lives in
 * reactivity.test.ts and hdhr/index.test.ts.
 */
import * as indexModule from "./index.ts";
import type { UserConfig, UserConfigLoadResult } from "./userConfig.ts";
import { before, beforeEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { getCurrentPattern } from "../utils/index.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// Store-double state. When armReadConfigFailure is true, the next readConfig throws the captured error; when readConfigOverride is set, readConfig returns that
// synthetic load result instead of reading disk; mutateConfigProbes captures each persistCoercedConfig write-back. Declared as separate `let` bindings so per-
// field mutability is explicit at the declaration site.
let armReadConfigFailure = false;
const readConfigFailureMessage = "synthetic readConfig failure for atomicity test";
let readConfigOverride: UserConfigLoadResult | null = null;
let mutateConfigProbes: UserConfig[] = [];

/* Indexed pool of pending reads used by the serialization suite. When armed, readConfig hands out pool entries in invocation order, so a test settles them BY
 * INDEX rather than by call timing. Every entry exists before either reload is issued, which is what lets a test settle the second read first and still settle
 * the first afterward - a pool built lazily inside readConfig could not express that order, because the second entry would not exist until the first read had
 * already been served.
 */
let readConfigPool: PromiseWithResolvers<UserConfigLoadResult>[] | null = null;
let readConfigInvocations = 0;

/**
 * Arms the indexed read pool with the requested number of pending entries and hands them back so the test can settle each one directly.
 * @param size - How many reads to pre-allocate.
 * @returns The pool entries, in the order readConfig will serve them.
 */
function armReadConfigPool(size: number): PromiseWithResolvers<UserConfigLoadResult>[] {

  readConfigInvocations = 0;
  readConfigPool = Array.from({ length: size }, () => Promise.withResolvers<UserConfigLoadResult>());

  return readConfigPool;
}

/* The injected config store: substitutes config/index.ts's disk-persistence boundary so reloadConfiguration's merge + validation and persistCoercedConfig's write-
 * back run against controlled state without touching the real config file. Typed as the production ConfigStore port so the double cannot drift from it. Every
 * reload test arms a failure or an override before driving reloadConfiguration; readConfig throws on the un-armed path rather than falling through to a real disk
 * read, so a test that forgets to arm one fails loudly instead of silently reading production config.
 */
const io: indexModule.ConfigStore = {

  mutateConfig: async (fn) => {

    const probe: UserConfig = {};

    fn(probe);
    mutateConfigProbes.push(probe);
  },
  readConfig: () => {

    // An armed pool takes precedence: each call is served the next pre-allocated entry so the test owns the settlement order.
    if(readConfigPool) {

      const queued = readConfigPool[readConfigInvocations++];

      if(!queued) {

        throw new Error("readConfig was called more times than the armed pool has entries.");
      }

      return queued.promise;
    }

    if(armReadConfigFailure) {

      throw new Error(readConfigFailureMessage);
    }

    if(readConfigOverride) {

      return Promise.resolve(readConfigOverride);
    }

    throw new Error("readConfig was called without an armed failure or override - the test must set one before driving reloadConfiguration.");
  }
};

describe("reloadConfiguration - atomicity on read failure", () => {

  test("a thrown readConfig leaves CONFIG reference-identical to its pre-call value", async () => {

    // Read the live CONFIG binding before and after so the assertion actually observes a reassignment: reloadConfiguration builds the next shape in isolation and
    // only reassigns the live binding after read + merge + normalize + validate succeed, so a thrown readConfig must leave CONFIG referentially unchanged. A local
    // snapshot copy would not track the binding and could not fail.
    armReadConfigFailure = true;
    const before = indexModule.CONFIG;

    try {

      await assert.rejects(() => indexModule.reloadConfiguration(io), { message: readConfigFailureMessage });

      assert.equal(indexModule.CONFIG, before, "CONFIG binding was not reassigned by the failed reload");
    } finally {

      armReadConfigFailure = false;
    }
  });
});

describe("reloadConfiguration - rejects an invalid or coercion-needing save", () => {

  // Each test arms a synthetic on-disk shape; clear it afterward so a later test (or the atomicity suite, if reordered) does not accidentally reuse this
  // override - an unarmed readConfig call throws loudly instead. The reject tests never commit, so CONFIG stays pristine for them; the commit test runs last
  // and is the only one that reassigns the live binding.
  let snapshot: typeof indexModule.CONFIG;

  before(() => {

    snapshot = indexModule.CONFIG;
  });

  test("rejects a native capture mode without committing CONFIG", async () => {

    readConfigOverride = { config: { streaming: { captureMode: "native" } }, parseError: false };

    try {

      const result = await indexModule.reloadConfiguration(io);

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

    readConfigOverride = { config: { logging: { debugFilter: "tuning:hulu" }, streaming: { captureMode: "native" } }, parseError: false };

    try {

      const result = await indexModule.reloadConfiguration(io);

      assert.equal(indexModule.CONFIG, before, "CONFIG binding was not reassigned");
      assert.equal(result.applied.length, 0, "nothing applied on the reject path");
      assert.equal(result.deferred.length, 0, "nothing deferred on the reject path");
      assert.equal(getCurrentPattern(), "", "the runtime debug filter was not changed by the rejected reload");
    } finally {

      readConfigOverride = null;
    }
  });

  test("rejects an out-of-range port and surfaces the hard-error reason", async () => {

    readConfigOverride = { config: { server: { port: 0 } }, parseError: false };

    try {

      const result = await indexModule.reloadConfiguration(io);

      assert.equal(indexModule.CONFIG, snapshot, "CONFIG binding was not reassigned");
      assert.deepEqual(result.rejected.map((r) => r.change.path), ["server.port"]);
      assert.ok(result.rejected.every((r) => r.reason.includes("PORT")), "the rejection reason names the PORT hard error");
    } finally {

      readConfigOverride = null;
    }
  });

  test("commits a valid change and dispatches it (deferred with no handler registered)", async () => {

    readConfigOverride = { config: { server: { port: 6000 } }, parseError: false };

    try {

      const result = await indexModule.reloadConfiguration(io);

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

    await indexModule.persistCoercedConfig(io);

    assert.equal(mutateConfigProbes.length, 1, "the coercion is written back to disk");
    assert.ok(mutateConfigProbes.every((p) => (p.streaming?.captureMode === "ffmpeg")), "the persisted capture mode is the coerced ffmpeg value");
  });

  test("does not write to disk when no coercion was needed", async () => {

    indexModule.CONFIG.streaming.captureMode = "ffmpeg";

    indexModule.validateConfiguration();
    await indexModule.persistCoercedConfig(io);

    assert.equal(mutateConfigProbes.length, 0, "no write-back without a coercion");
  });
});

/* Reloads are serialized on a module-level queue, so two overlapping saves cannot interleave into a state where the reload holding the older disk snapshot
 * commits last. Both assertions drive two reloads whose reads settle out of call order, which is the shape a pair of concurrent save requests produces.
 */
describe("reloadConfiguration - serialized reloads", () => {

  const queuedReadFailureMessage = "synthetic readConfig failure for the queued reload";

  test("overlapping reloads commit in call order, leaving CONFIG on the newest snapshot", async () => {

    const [ firstRead, secondRead ] = armReadConfigPool(2);

    assert.ok(firstRead && secondRead, "both reads are pre-allocated before either reload is issued");

    try {

      /* Both reloads are issued without awaiting, then their reads are settled in reverse order: the second caller's newer snapshot settles first, the first
       * caller's older snapshot afterward. Serialized, the second reload does not even request its read until the first has committed, so the newer value is
       * the one left standing.
       */
      const older = indexModule.reloadConfiguration(io);
      const newer = indexModule.reloadConfiguration(io);

      secondRead.resolve({ config: { server: { port: 6200 } }, parseError: false });
      firstRead.resolve({ config: { server: { port: 6100 } }, parseError: false });

      await older;
      await newer;

      assert.equal(indexModule.CONFIG.server.port, 6200, "the live binding ends on the snapshot from the later call");
    } finally {

      readConfigPool = null;
    }
  });

  test("a failed reload rejects its own caller and leaves the chain usable for the next one", async () => {

    const [ firstRead, secondRead ] = armReadConfigPool(2);

    assert.ok(firstRead && secondRead, "both reads are pre-allocated before either reload is issued");

    try {

      // The first reload's read fails. Its rejection must reach that caller alone: the queue swallows it on the chain reference so the reload behind it still
      // runs and commits.
      const failed = indexModule.reloadConfiguration(io);
      const succeeded = indexModule.reloadConfiguration(io);

      firstRead.reject(new Error(queuedReadFailureMessage));
      secondRead.resolve({ config: { server: { port: 6300 } }, parseError: false });

      await assert.rejects(() => failed, { message: queuedReadFailureMessage });
      await succeeded;

      assert.equal(indexModule.CONFIG.server.port, 6300, "the reload behind the failed one commits normally");
    } finally {

      readConfigPool = null;
    }
  });
});
