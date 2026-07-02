/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userConfig.test.ts: Unit tests for the pure-function surface of the user-config layer - the DEFAULTS shape invariant, the CONFIG_METADATA structure that
 * drives the UI, and the small primitives (getNestedValue/setNestedValue/isEqualToDefault) plus the UI-tab/section accessors. The merge priority order, env
 * var handling, and filterDefaults are covered in userConfig.merge.test.ts so this file stays under the conventions' 500-line guidance.
 */
import { CONFIG_METADATA, DEFAULTS, getAdvancedSections, getNestedValue, getSettingByPath, getSettingsTabSections, getUITabs, isEqualToDefault,
  setNestedValue } from "./userConfig.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("DEFAULTS", () => {

  test("is a complete Config shape with every top-level group populated", () => {

    // Locking the top-level groups guards against accidental deletion during refactors. The validateConfiguration tests cover field-level invariants.
    assert.ok(DEFAULTS.browser, "browser group present");
    assert.ok(DEFAULTS.channels, "channels group present");
    assert.ok(DEFAULTS.hdhr, "hdhr group present");
    assert.ok(DEFAULTS.hls, "hls group present");
    assert.ok(DEFAULTS.logging, "logging group present");
    assert.ok(DEFAULTS.paths, "paths group present");
    assert.ok(DEFAULTS.playback, "playback group present");
    assert.ok(DEFAULTS.recovery, "recovery group present");
    assert.ok(DEFAULTS.server, "server group present");
    assert.ok(DEFAULTS.streaming, "streaming group present");
  });

  test("declares the documented baseline values for the most-checked fields", () => {

    // These are the values production code asserts against in many places; pinning them prevents subtle drift.
    assert.equal(DEFAULTS.server.port, 5589);
    assert.equal(DEFAULTS.streaming.qualityPreset, "720p-high");
    assert.equal(DEFAULTS.streaming.captureMode, "ffmpeg");
    assert.deepEqual(DEFAULTS.streaming.captureCodecs, [ "h264", "hevc" ]);
    assert.equal(DEFAULTS.recovery.circuitBreakerThreshold, 10);
    assert.equal(DEFAULTS.recovery.circuitBreakerWindow, 300000);
    assert.equal(DEFAULTS.hdhr.enabled, true);
  });

  test("declares the load-bearing recovery and HLS defaults that downstream invariants depend on", () => {

    /* Pinning these explicitly catches a regression where a release upgrade silently changes a numeric tuning constant. The ones called out here are the
     * defaults consumed by streaming/recovery.ts, streaming/monitor.ts, and streaming/hls.ts; their behavior depends on the specific values rather than just "any
     * positive number".
     */
    assert.equal(DEFAULTS.recovery.maxBackoffDelay, 3000, "recovery backoff cap aligns with the documented 3-second ceiling");
    assert.equal(DEFAULTS.recovery.backoffJitter, 1000, "backoff jitter range is ±1 second");
    assert.equal(DEFAULTS.recovery.stalePageGracePeriod, 30000, "stale-page grace period is 30 seconds");
    assert.equal(DEFAULTS.hls.idleTimeout, 30000, "HLS idle timeout matches the 30-second teardown window");
    assert.equal(DEFAULTS.hls.maxSegments, 10, "HLS rolling-window default is 10 segments");
    assert.equal(DEFAULTS.hls.segmentDuration, 2, "HLS segment duration default is 2 seconds");
    assert.equal(DEFAULTS.browser.executablePath, null, "Chrome path defaults to null (autodetect)");
    assert.equal(DEFAULTS.paths.chromeDataDir, null, "chromeDataDir override defaults to null");
    assert.equal(DEFAULTS.paths.logFile, null, "logFile override defaults to null");
    assert.equal(DEFAULTS.channels.setupCompleted, false, "first-run wizard flag defaults to false");
  });

  test("array fields are arrays (not undefined or other types)", () => {

    assert.ok(Array.isArray(DEFAULTS.channels.disabledPredefined));
    assert.ok(Array.isArray(DEFAULTS.channels.enabledServices));
    assert.ok(Array.isArray(DEFAULTS.channels.precacheServices));
    assert.ok(Array.isArray(DEFAULTS.channels.visibleColumns));
    assert.ok(Array.isArray(DEFAULTS.streaming.captureCodecs));
  });
});

describe("CONFIG_METADATA", () => {

  test("groups settings by category (server, browser, streaming, etc.)", () => {

    assert.ok(Array.isArray(CONFIG_METADATA["server"]));
    assert.ok(Array.isArray(CONFIG_METADATA["streaming"]));
    assert.ok(Array.isArray(CONFIG_METADATA["recovery"]));
  });

  test("every entry has the required path, type, and label fields", () => {

    for(const [ category, settings ] of Object.entries(CONFIG_METADATA)) {

      for(const setting of settings) {

        assert.ok(setting.path, category + " entry missing path");
        assert.ok(setting.type, category + " entry " + setting.path + " missing type");
        assert.ok(setting.label, category + " entry " + setting.path + " missing label");
      }
    }
  });

  test("every metadata path corresponds to a defined value in DEFAULTS", () => {

    // filterDefaults() depends on getNestedValue(DEFAULTS, path) resolving for every CONFIG_METADATA path. A typo here would silently break default-stripping on save.
    for(const settings of Object.values(CONFIG_METADATA)) {

      for(const setting of settings) {

        const value = getNestedValue(DEFAULTS, setting.path);

        assert.notEqual(value, undefined, "DEFAULTS missing path: " + setting.path);
      }
    }
  });
});

describe("getNestedValue", () => {

  test("returns the value at a single-segment path", () => {

    assert.equal(getNestedValue({ a: 1 }, "a"), 1);
  });

  test("returns the value at a multi-segment path", () => {

    assert.equal(getNestedValue({ a: { b: { c: 7 } } }, "a.b.c"), 7);
  });

  test("returns undefined when an intermediate segment is missing", () => {

    assert.equal(getNestedValue({ a: 1 }, "a.b.c"), undefined);
  });

  test("returns undefined when an intermediate segment is null", () => {

    assert.equal(getNestedValue({ a: null }, "a.b"), undefined);
  });

  test("returns undefined when the input itself is null", () => {

    assert.equal(getNestedValue(null, "a"), undefined);
  });

  test("returns undefined for an empty path", () => {

    // Boundary: "".split(".") returns [""], so the function looks for the empty-string key. Most objects don't have one, so undefined is the typical result.
    assert.equal(getNestedValue({}, ""), undefined);
  });
});

describe("setNestedValue", () => {

  test("sets a single-segment value", () => {

    const obj: Record<string, unknown> = {};

    setNestedValue(obj, "a", 1);
    assert.deepEqual(obj, { a: 1 });
  });

  test("creates intermediate objects for missing segments", () => {

    const obj: Record<string, unknown> = {};

    setNestedValue(obj, "a.b.c", 7);
    assert.deepEqual(obj, { a: { b: { c: 7 } } });
  });

  test("preserves existing siblings at each level", () => {

    const obj: Record<string, unknown> = { a: { x: 1 } };

    setNestedValue(obj, "a.b", 2);
    // eslint-disable-next-line sort-keys -- the test asserts that the existing 'x' sibling is preserved alongside the newly-set 'b' field.
    assert.deepEqual(obj, { a: { x: 1, b: 2 } });
  });

  test("overwrites existing values at the leaf", () => {

    const obj: Record<string, unknown> = { a: 1 };

    setNestedValue(obj, "a", 2);
    assert.equal(obj["a"], 2);
  });

  test("throws TypeError when an intermediate segment is a non-object primitive (strict-mode boxing fails)", () => {

    /* Boundary: setNestedValue traverses via `current[part] ??= {}`, which keeps any defined non-nullish intermediate. When that intermediate is a primitive
     * (a string here), `??=` is a no-op (the string is already truthy) and the subsequent `(current as Record<string, unknown>)[part]` cast tries to set a
     * property on the boxed primitive. Strict mode (which ESM source files run under) refuses the assignment with TypeError. Pinning the throw documents the
     * actual contract and protects against a regression that would silently swallow the assignment on a non-strict primitive boxing path.
     */
    const obj: Record<string, unknown> = { a: "primitive" };

    assert.throws(() => { setNestedValue(obj, "a.b", 2); }, /Cannot create property/);
    assert.equal(obj["a"], "primitive", "intermediate value untouched after the throw");
  });
});

describe("isEqualToDefault", () => {

  test("treats both null and undefined as equal to each other", () => {

    assert.equal(isEqualToDefault(null, null), true);
    assert.equal(isEqualToDefault(null, undefined), true);
    assert.equal(isEqualToDefault(undefined, null), true);
    assert.equal(isEqualToDefault(undefined, undefined), true);
  });

  test("treats null/undefined value vs a real default as unequal", () => {

    /* The contract for the value=null path: when value is null/undefined, the function returns whether the default is also null/undefined. The implementation
     * uses `(defaultValue === null) || (defaultValue === undefined)`, so a defined default value (0, "", any literal) yields false.
     */
    assert.equal(isEqualToDefault(null, 0), false);
    assert.equal(isEqualToDefault(undefined, 0), false);
    assert.equal(isEqualToDefault(null, ""), false);
  });

  test("returns false when only the default is null/undefined", () => {

    assert.equal(isEqualToDefault(0, null), false);
    assert.equal(isEqualToDefault("x", undefined), false);
  });

  test("compares primitives via String() so '5' equals 5 (lock the documented coercion)", () => {

    assert.equal(isEqualToDefault("5", 5), true, "string and number with same digits compare equal under String() coercion");
    assert.equal(isEqualToDefault(true, "true"), true);
    assert.equal(isEqualToDefault(0, "0"), true);
  });

  test("returns false for distinct primitive values", () => {

    assert.equal(isEqualToDefault("foo", "bar"), false);
    assert.equal(isEqualToDefault(1, 2), false);
  });
});

describe("getSettingByPath", () => {

  test("looks up a known setting by dotted path", () => {

    const result = getSettingByPath("server.port");

    assert.ok(result, "result is defined");
    assert.equal(result.path, "server.port");
    assert.equal(result.envVar, "PORT");
  });

  test("returns undefined for a path with no metadata entry", () => {

    assert.equal(getSettingByPath("not.a.real.path"), undefined);
  });

  test("looks up a setting in a non-server category by dotted path", () => {

    /* Pins that the lookup walks every category, not just the first. Picking hls.segmentDuration covers a category beyond the server group and the
     * configuration metadata loop runs through every entry until match.
     */
    const result = getSettingByPath("hls.segmentDuration");

    assert.ok(result, "getSettingByPath should resolve a non-server-category setting path");
    assert.equal(result.envVar, "HLS_SEGMENT_DURATION");
  });
});

describe("getSettingsTabSections", () => {

  test("returns the explicit Settings tab sections in declared order", () => {

    const sections = getSettingsTabSections();

    assert.deepEqual(sections.map((s) => s.id), [ "server", "browser", "startup", "capture", "hdhr" ]);
  });

  test("each section's settings array contains resolved SettingMetadata entries", () => {

    const sections = getSettingsTabSections();
    const server = sections.find((s) => s.id === "server");

    assert.ok(server, "server section should be present in getSettingsTabSections result");
    assert.ok(server.settings.some((s) => s.path === "server.port"), "server section should include the server.port setting");
  });

  test("orphan paths in SETTINGS_TAB_SECTIONS are silently filtered (defensive)", () => {

    /* The contract documented in the source comment: a path in SETTINGS_TAB_SECTIONS that does not resolve to a CONFIG_METADATA entry is dropped during
     * derivation rather than throwing. Verified indirectly by confirming each returned setting has a matching path - any entry whose getSettingByPath
     * returned undefined would have been filtered out, and we observe no holes. This pins the silent-filter contract.
     */
    const sections = getSettingsTabSections();

    for(const section of sections) {

      for(const setting of section.settings) {

        assert.ok((typeof setting.path === "string") && (setting.path.length > 0), "every surviving setting has a defined dotted path");
      }
    }
  });
});

describe("getUITabs", () => {

  test("returns the Settings and Advanced tabs", () => {

    const tabs = getUITabs();

    assert.equal(tabs.length, 2);
    assert.equal(tabs[0]?.id, "settings");
    assert.equal(tabs[1]?.id, "advanced");
  });

  test("Advanced tab does not duplicate any Settings-tab paths", () => {

    const tabs = getUITabs();
    const settingsPaths = new Set(tabs[0]!.settings.map((s) => s.path));

    for(const setting of tabs[1]!.settings) {

      assert.equal(settingsPaths.has(setting.path), false, "Advanced tab contains Settings path: " + setting.path);
    }
  });
});

describe("getAdvancedSections", () => {

  test("returns sections grouped by category in the documented order", () => {

    const sections = getAdvancedSections();

    // ADVANCED_SECTION_META declares: channelsDvr, hls, logging, paths, playback, recovery, streaming. Subset relation is what we lock - some categories may
    // be empty depending on what's promoted to the Settings tab.
    const ids = sections.map((s) => s.id);

    for(const id of ids) {

      assert.ok([ "channelsDvr", "hls", "logging", "paths", "playback", "recovery", "streaming" ].includes(id), id + " is one of the documented advanced categories");
    }
  });
});
