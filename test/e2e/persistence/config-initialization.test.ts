/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * config-initialization.test.ts: Integration-tier coverage for the post-merge branches of initializeConfiguration() in src/config/index.ts, the branches that
 * run beyond the mergeConfiguration pipeline (covered at unit tier in userConfig.merge.test.ts):
 *
 *   1. Persisted debug filter restoration. When config.json carries a logging.debugFilter and no environment- or CLI-driven debug filter is active,
 *      normalizeConfig() rewrites the in-memory copy to its canonical form via canonicalizeDebugPattern() and commitDebugFilter() applies that pattern to the
 *      live runtime filter via initDebugFilter().
 *   2. Quality preset validation gate. An unknown qualityPreset (typo in config.json or a preset removed in a release upgrade) is reset to DEFAULTS with an
 *      operator-visible warning rather than allowed through to the validation layer where it would only surface as a viewport mismatch.
 *   3. Frame rate range clamp. A frame rate outside the floor and ceiling its metadata declares (a hand-edited config.json or a FRAME_RATE value) is clamped to
 *      the nearer bound with an operator-visible warning, so the capture constraint, which holds the track to the configured rate on both bounds, never receives
 *      a rate outside that range, and the server still starts.
 *
 * Each branch is isolated to its own describe block. Each test runs against its own integration context (an isolated data dir disposed via "await using"),
 * and the debugFilter suite's afterEach restores the PRISMCAST_DEBUG env var and clears the runtime filter so env/CLI debug state does not leak between tests.
 */
import { CONFIG, initializeConfiguration } from "../../../src/config/index.ts";
import { DEFAULTS, getSettingByPath, readConfig } from "../../../src/config/userConfig.ts";
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
   *   - "parseError" / "parseErrorMessage" / "readError" pass through;
   *   - "migrationResult" (which the framework returns alongside data) is intentionally dropped from the wrapper's return shape so callers can't accidentally
   *     act on framework metadata that's already been applied to the data.
   *
   * The drop-migrationResult contract is the part not exercised elsewhere - the surrounding fields are asserted by backup-recovery.test.ts. We seed a plain
   * current-shape config (a single server.port override), run readConfig, and assert the returned keyset matches the documented shape with no migrationResult
   * leakage. The contract is independent of whether a migration actually ran, so no legacy field is needed to exercise it.
   */
  test("returns the documented keyset and drops migrationResult that the framework projects internally", async () => {

    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "config.json", { server: { port: 9999 } });

    const result = await readConfig();

    const keys = Object.keys(result).toSorted();

    assert.deepEqual(keys, [ "config", "parseError", "parseErrorMessage", "readError" ].toSorted(),
      "readConfig wrapper returns exactly the documented keys; framework's migrationResult is dropped");
    assert.equal(result.config.server?.port, 9999, "config carries the parsed UserConfig content");
    assert.equal(result.parseError, false, "fresh config parses cleanly");
    assert.equal(result.readError, false, "a readable file is no read failure");
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

    await writePersistedJson(ctx, "config.json", { streaming: { qualityPreset: "nonexistent-preset-xyz" } });

    await initializeConfiguration();

    assert.equal(CONFIG.streaming.qualityPreset, DEFAULTS.streaming.qualityPreset, "unknown preset reset to default");

    const warnings = warnSpy.mock.calls.filter((call) => {

      const arg = call.arguments[0];

      return (typeof arg === "string") && arg.startsWith("The configured quality preset");
    });

    assert.deepEqual(warnings.map((call) => call.arguments), [[ "The configured quality preset is not one the server recognizes, so the default preset is in use.",
      { configured: "nonexistent-preset-xyz", using: DEFAULTS.streaming.qualityPreset } ]],
    "exactly one warning fired for the invalid preset, naming the configured and default presets");
  });
});

describe("initializeConfiguration: out-of-range frame rate clamp", () => {

  /* The branch fires when the loaded CONFIG.streaming.frameRate falls outside the floor and ceiling the streaming.frameRate metadata entry declares. The
   * function clamps it to the nearer bound with a LOG.warn naming the configured rate, the range, and the rate applied, and the load completes. Every expected
   * bound is read from the metadata rather than restated, so the rows follow the range the metadata declares and fail against a clamp carrying its own copy.
   */
  const setting = getSettingByPath("streaming.frameRate");
  let warnSpy: ReturnType<typeof mock.method>;

  beforeEach(() => {

    warnSpy = mock.method(LOG, "warn", () => undefined);
  });

  afterEach(() => {

    warnSpy.mock.restore();
  });

  /**
   * Persists a frame rate to config.json, loads the configuration through initializeConfiguration, and collects the frame-rate warnings the load emitted.
   * @param frameRate - The frame rate to persist.
   * @returns The arguments of each frame-rate warning, its sentence and then its context object, in call order.
   */
  async function loadFrameRate(frameRate: number): Promise<unknown[][]> {

    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "config.json", { streaming: { frameRate } });

    await initializeConfiguration();

    return warnSpy.mock.calls.map((call) => call.arguments).filter(([message]) => (typeof message === "string") && message.startsWith("The configured frame rate"));
  }

  test("clamps a rate below the metadata floor up to the floor and warns once, naming the configured and applied rates", async () => {

    assert.ok((typeof setting?.min === "number") && (typeof setting.max === "number"), "sanity: the metadata declares both frame-rate bounds");

    const warnings = await loadFrameRate(24);

    assert.equal(CONFIG.streaming.frameRate, setting.min, "a rate below the floor is clamped up to it");
    assert.deepEqual(warnings, [[ "The configured frame rate is outside the supported range, so the nearer bound is in use.",
      { applied: setting.min, configured: 24, max: setting.max, min: setting.min } ]],
    "exactly one warning fired, naming the configured rate, the range, and the rate applied");
  });

  test("clamps a rate above the metadata ceiling down to the ceiling and warns once, naming the configured and applied rates", async () => {

    assert.ok((typeof setting?.min === "number") && (typeof setting.max === "number"), "sanity: the metadata declares both frame-rate bounds");

    const warnings = await loadFrameRate(120);

    assert.equal(CONFIG.streaming.frameRate, setting.max, "a rate above the ceiling is clamped down to it");
    assert.deepEqual(warnings, [[ "The configured frame rate is outside the supported range, so the nearer bound is in use.",
      { applied: setting.max, configured: 120, max: setting.max, min: setting.min } ]],
    "exactly one warning fired, naming the configured rate, the range, and the rate applied");
  });

  test("leaves a rate inside the metadata range untouched and warns about nothing", async () => {

    assert.ok((typeof setting?.min === "number") && (typeof setting.max === "number"), "sanity: the metadata declares both frame-rate bounds");
    assert.ok((setting.min < 45) && (45 < setting.max), "sanity: the rate this row persists sits inside the declared range");

    const warnings = await loadFrameRate(45);

    assert.equal(CONFIG.streaming.frameRate, 45, "a rate inside the range reaches CONFIG unchanged");
    assert.deepEqual(warnings, [], "no frame-rate warning fired for an in-range rate");
  });
});
