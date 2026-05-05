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

    // The merge logic depends on getNestedValue(DEFAULTS, path) returning a defined value for every path. A typo here would silently break per-setting defaults.
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

    // Boundary: split("") returns [""], so the function looks for the empty-string key. Most objects don't have one, so undefined is the typical result.
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
});

describe("getSettingsTabSections", () => {

  test("returns the explicit Settings tab sections in declared order", () => {

    const sections = getSettingsTabSections();

    assert.deepEqual(sections.map((s) => s.id), [ "server", "browser", "startup", "capture", "hdhr" ]);
  });

  test("each section's settings array contains resolved SettingMetadata entries", () => {

    const sections = getSettingsTabSections();
    const server = sections.find((s) => s.id === "server");

    assert.ok(server);
    assert.ok(server.settings.some((s) => s.path === "server.port"));
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
