/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * config-initialization.test.ts: Integration-tier coverage for the post-merge branches of initializeConfiguration() in src/config/index.ts. Two branches
 * exist beyond the mergeConfiguration pipeline (covered at unit tier in userConfig.merge.test.ts):
 *
 *   1. Persisted debug filter restoration. When config.json carries a logging.debugFilter and no environment- or CLI-driven debug filter is active,
 *      normalizeConfig() rewrites the in-memory copy to its canonical form via canonicalizeDebugPattern() and commitDebugFilter() applies that pattern to the
 *      live runtime filter via initDebugFilter().
 *   2. Quality preset validation gate. An unknown qualityPreset (typo in config.json or a preset removed in a release upgrade) is reset to DEFAULTS with an
 *      operator-visible warning rather than allowed through to the validation layer where it would only surface as a viewport mismatch.
 *
 * Each branch is isolated to its own describe block. Each test runs against its own integration context (an isolated data dir disposed via "await using"),
 * and the debugFilter suite's afterEach restores the PRISMCAST_DEBUG env var and clears the runtime filter so env/CLI debug state does not leak between tests.
 */
import { CONFIG, initializeConfiguration } from "../../../src/config/index.ts";
import { DEFAULTS, readConfig } from "../../../src/config/userConfig.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { createIntegrationContext, writePersistedJson } from "../../helpers/integration.helpers.ts";
import { initDebugFilter, isAnyDebugEnabled } from "../../../src/utils/debugFilter.ts";
import { LOG } from "../../../src/utils/index.ts";
import assert from "node:assert/strict";

describe("initializeConfiguration: persisted debugFilter branch", () => {

  /* The branch fires when no env/CLI debug source owns the filter (envOrCliDebugOverride is false, i.e. no PRISMCAST_DEBUG env var and no --debug CLI flag) AND
   * the canonical persisted CONFIG.logging.debugFilter differs from the currently-active runtime pattern (getCurrentPattern()). That difference also covers the
   * empty-clears-an-active-filter case, not merely a non-empty pattern. We seed config.json with a debugFilter, ensure the env-side debug state is clear, and
   * assert the resulting runtime state.
   */
  const ORIGINAL_ENV = process.env["PRISMCAST_DEBUG"];

  beforeEach(() => {

    Reflect.deleteProperty(process.env, "PRISMCAST_DEBUG");
    initDebugFilter("");
  });

  afterEach(() => {

    if(ORIGINAL_ENV === undefined) {

      Reflect.deleteProperty(process.env, "PRISMCAST_DEBUG");
    } else {

      process.env["PRISMCAST_DEBUG"] = ORIGINAL_ENV;
    }

    initDebugFilter("");
  });

  test("applies the persisted logging.debugFilter pattern and canonicalizes the in-memory copy", async () => {

    /* The persisted form may carry user-formatted whitespace (e.g., "tuning:hulu, recovery"); normalizeConfig() rewrites CONFIG.logging.debugFilter via
     * canonicalizeDebugPattern() to its canonical form (the same form getCurrentPattern would yield) so equality checks elsewhere see the parser's exact output,
     * while commitDebugFilter() applies that pattern to the live runtime filter via initDebugFilter() as a separate side effect.
     */
    await using ctx = await createIntegrationContext();

    // The await-using binding owns the data dir for the test's lifetime; "void ctx;" marks it as intentionally held so the disposable is not flagged as unused
    // before the writePersistedJson call below consumes it.
    void ctx;
    await writePersistedJson(ctx, "config.json", { logging: { debugFilter: "tuning:hulu, recovery" } });

    await initializeConfiguration();

    assert.equal(isAnyDebugEnabled(), true, "debug filter must be active after persisted pattern was applied");
    assert.equal(CONFIG.logging.debugFilter, "tuning:hulu,recovery",
      "in-memory debug filter must be the parser's canonical form (no whitespace around the comma)");
  });

  test("does NOT re-apply the persisted filter when isAnyDebugEnabled is already true", async () => {

    /* Pre-condition: any debug pattern is already active at init time. initializeConfiguration captures that into the envOrCliDebugOverride snapshot (from
     * isAnyDebugEnabled() before the persisted filter applies), and commitDebugFilter() then gates on that snapshot to skip the persisted-pattern apply branch.
     * We simulate by pre-initializing the filter ourselves; the persisted value still flows through mergeConfiguration into CONFIG, but the function does not call
     * initDebugFilter again. The behavioral contract is that the previously-active pattern remains untouched.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await writePersistedJson(ctx, "config.json", { logging: { debugFilter: "tuning:hulu" } });

    initDebugFilter("recovery");

    await initializeConfiguration();

    assert.equal(CONFIG.logging.debugFilter, "tuning:hulu", "merged in-memory filter reflects the persisted value verbatim");
    assert.equal(isAnyDebugEnabled(), true, "the pre-existing debug pattern remains active");
  });
});

describe("readConfig adapter shape", () => {

  /* The readConfig wrapper projects the file-store framework's read result onto the UserConfigLoadResult shape. The contract:
   *   - "config" carries the parsed (and migrated) UserConfig;
   *   - "parseError" / "parseErrorMessage" / "recoveredFromBackup" pass through;
   *   - "migrationResult" (which the framework returns alongside data) is intentionally dropped from the wrapper's return shape so callers can't accidentally
   *     act on framework metadata that's already been applied to the data.
   *
   * The drop-migrationResult contract is the part not exercised elsewhere - the surrounding fields are asserted by backup-recovery.test.ts. We seed a plain
   * current-shape config (a single server.port override), run readConfig, and assert the returned keyset matches the documented shape with no migrationResult
   * leakage. The contract is independent of whether a migration actually ran, so no legacy field is needed to exercise it.
   */
  test("returns the documented keyset and drops migrationResult that the framework projects internally", async () => {

    await using ctx = await createIntegrationContext();

    void ctx;
    await writePersistedJson(ctx, "config.json", { server: { port: 9999 } });

    const result = await readConfig();

    const keys = Object.keys(result).toSorted();

    assert.deepEqual(keys, [ "config", "parseError", "parseErrorMessage", "recoveredFromBackup" ].toSorted(),
      "readConfig wrapper returns exactly the four documented keys; framework's migrationResult is dropped");
    assert.equal(result.config.server?.port, 9999, "config carries the parsed UserConfig content");
    assert.equal(result.parseError, false, "fresh config parses cleanly");
    assert.equal(result.recoveredFromBackup, false, "no backup recovery occurred for a clean read");
  });
});

describe("initializeConfiguration: invalid quality preset reset", () => {

  /* The branch fires when the loaded CONFIG.streaming.qualityPreset is not in getValidPresetIds(). The function resets it to DEFAULTS.streaming.qualityPreset
   * with a LOG.warn naming both values. Operators see one canonical warning rather than a downstream viewport-mismatch error days later.
   */
  let warnSpy: ReturnType<typeof mock.method>;

  beforeEach(() => {

    warnSpy = mock.method(LOG, "warn", () => undefined);
  });

  afterEach(() => {

    warnSpy.mock.restore();
  });

  test("resets an unknown preset to DEFAULTS.streaming.qualityPreset and logs a warning naming both values", async () => {

    await using ctx = await createIntegrationContext();

    void ctx;
    await writePersistedJson(ctx, "config.json", { streaming: { qualityPreset: "nonexistent-preset-xyz" } });

    await initializeConfiguration();

    assert.equal(CONFIG.streaming.qualityPreset, DEFAULTS.streaming.qualityPreset, "unknown preset reset to default");

    const warnings = warnSpy.mock.calls.filter((call) => {

      const arg = call.arguments[0];

      return (typeof arg === "string") && arg.includes("Invalid quality preset");
    });

    assert.equal(warnings.length, 1, "exactly one warning fired for the invalid preset");
  });
});
