/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userConfig.test.ts: Unit tests for the pure-function surface of the user-config layer - the DEFAULTS shape, the CONFIG_METADATA structure that
 * drives the UI, and the small primitives (getNestedValue/setNestedValue/isEqualToDefault) plus the capture corrections, the store's write normalization, and
 * the UI-tab/section accessors - and for one write through the configuration store itself, which shows the store's write hook storing corrected capture
 * values. The merge priority order, env var handling, and filterDefaults are covered in userConfig.merge.test.ts.
 */
import { CONFIG_METADATA, DEFAULTS, PROCESS_FIELDS, collectStoredCaptureCorrections, correctCaptureValues, correctStoredDeviceId, getAdvancedSections,
  getNestedValue, getReactivityClass, getSettingByPath, getSettingsTabSections, getUITabs, isEqualToDefault, mergeConfiguration, mutateConfig,
  normalizeStoredConfig, readConfig, setNestedValue } from "./userConfig.ts";
import type { Config, ReactivityClass } from "../types/index.ts";
import { LOG, validateDeviceId } from "../utils/index.ts";
import { describe, test } from "node:test";
import { listConfigLeafPaths, withTempDir } from "../testing.helpers.ts";
import { CONFIG } from "./index.ts";
import { SEEDED_DEVICE_ID } from "./index.helpers.ts";
import type { UserConfig } from "./userConfig.ts";
import assert from "node:assert/strict";
import { initializeDataDir } from "./paths.ts";
import os from "node:os";

describe("DEFAULTS", () => {

  test("is a complete Config shape with every top-level group populated", () => {

    // Locking the top-level groups guards against accidental deletion during refactors. The validateConfiguration tests cover field-level rules.
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

    // These are the values production code asserts against in many places; asserting them prevents subtle drift.
    assert.equal(DEFAULTS.server.port, 5589);
    assert.equal(DEFAULTS.streaming.qualityPreset, "720p-high");
    assert.equal(DEFAULTS.streaming.captureMode, "ffmpeg");
    assert.deepEqual(DEFAULTS.streaming.captureCodecs, [ "h264", "hevc" ]);
    assert.equal(DEFAULTS.recovery.circuitBreakerThreshold, 10);
    assert.equal(DEFAULTS.recovery.circuitBreakerWindow, 300000);
    assert.equal(DEFAULTS.hdhr.enabled, true);
  });

  test("declares the recovery and HLS defaults whose specific values downstream behavior depends on", () => {

    /* Asserting these explicitly catches a regression where a release upgrade silently changes a numeric tuning constant. The ones called out here are the
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
     * property on the boxed primitive. Strict mode (which ESM source files run under) refuses the assignment with TypeError. Asserting the throw documents the
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

  test("a list compares with the capture codec default by its members in any order", () => {

    // The default written out, so each answer is a known answer for the comparison rather than for whatever DEFAULTS holds.
    const codecDefault = [ "h264", "hevc" ];

    assert.equal(isEqualToDefault([ "hevc", "h264" ], codecDefault), true, "a reordered list holds the default's members");
    assert.equal(isEqualToDefault(["h264"], codecDefault), false, "a subset of the default's members is another set");
    assert.equal(isEqualToDefault([ "h264", "hevc", "av1" ], codecDefault), false, "a superset of the default's members is another set");
  });

  test("a list compares with the empty precache default by its members", () => {

    assert.equal(isEqualToDefault(["hulu"], []), false, "a list with a member differs from the empty default");
    assert.equal(isEqualToDefault([], []), true, "an empty list equals the empty default");
  });

  test("a value without an array default's shape counts as absent, so it equals the default", () => {

    assert.equal(isEqualToDefault("h264", [ "h264", "hevc" ]), true, "a string at an array default");
    assert.equal(isEqualToDefault(null, []), true, "null at an array default");
  });
});

/* The capture corrections and the store's write normalization. Every expected answer is written out as a literal, so a row reads what the correction produces
 * rather than what a second call of the same code produces.
 */
describe("capture corrections", () => {

  const MODE_CORRECTION = { configured: "native", kind: "mode", using: "ffmpeg" } as const;
  const CORRECTION_LINE = "The configuration file write carries corrected capture values.";

  test("a mode other than FFmpeg is corrected to FFmpeg, carrying the configured mode", () => {

    assert.deepEqual(correctCaptureValues({ captureMode: "native" }), { corrections: [MODE_CORRECTION], values: { captureMode: "ffmpeg" } });
  });

  test("a codec list drops each identifier the server does not recognize, carrying the ignored identifiers and the list in effect", () => {

    assert.deepEqual(correctCaptureValues({ captureCodecs: [ "h264", "av1" ] }),
      { corrections: [{ configured: [ "h264", "av1" ], ignored: ["av1"], kind: "unrecognizedCodecs", using: ["h264"] }], values: { captureCodecs: ["h264"] } });
  });

  test("a codec list without the H.264 baseline gains it first", () => {

    assert.deepEqual(correctCaptureValues({ captureCodecs: ["hevc"] }),
      { corrections: [{ configured: ["hevc"], kind: "baseline", using: [ "h264", "hevc" ] }], values: { captureCodecs: [ "h264", "hevc" ] } });
  });

  test("every correction a layer needs is made in order, the mode first and the baseline last, each naming the list in effect", () => {

    assert.deepEqual(correctCaptureValues({ captureCodecs: ["av1"], captureMode: "native" }), {

      corrections: [ MODE_CORRECTION, { configured: ["av1"], ignored: ["av1"], kind: "unrecognizedCodecs", using: ["h264"] },
        { configured: ["av1"], kind: "baseline", using: ["h264"] } ],
      values: { captureCodecs: ["h264"], captureMode: "ffmpeg" }
    });
  });

  test("values that need no correction pass unchanged, and a layer that defines no capture value gains none", () => {

    assert.deepEqual(correctCaptureValues({ captureCodecs: [ "hevc", "h264" ], captureMode: "ffmpeg" }),
      { corrections: [], values: { captureCodecs: [ "hevc", "h264" ], captureMode: "ffmpeg" } });
    assert.deepEqual(correctCaptureValues({}), { corrections: [], values: {} });
  });

  test("a stored native mode normalizes to a file with no capture mode", (t) => {

    t.mock.method(LOG, "info", () => undefined);

    assert.deepEqual(normalizeStoredConfig({ streaming: { captureMode: "native" } }), {});
  });

  test("a stored list with an unrecognized codec normalizes to the list in effect, which differs from the default and is kept", (t) => {

    t.mock.method(LOG, "info", () => undefined);

    assert.deepEqual(normalizeStoredConfig({ streaming: { captureCodecs: [ "h264", "av1" ] } }), { streaming: { captureCodecs: ["h264"] } });
  });

  test("a stored list without the baseline normalizes to a file with no codec list, because its correction holds the default's members", (t) => {

    t.mock.method(LOG, "info", () => undefined);

    assert.deepEqual(normalizeStoredConfig({ streaming: { captureCodecs: ["hevc"] } }), {});
  });

  test("normalizing logs one info line naming the corrections a file needed, and nothing for a file that needs none", (t) => {

    const info = t.mock.method(LOG, "info", () => undefined);
    const native = { streaming: { captureMode: "native" } };
    const clean = { server: { port: 6000 } };

    assert.deepEqual(collectStoredCaptureCorrections(native), [MODE_CORRECTION]);
    assert.deepEqual(collectStoredCaptureCorrections(clean), []);

    normalizeStoredConfig(native);

    assert.deepEqual(info.mock.calls.map((call) => call.arguments), [[ CORRECTION_LINE, { corrections: [MODE_CORRECTION] } ]]);

    info.mock.resetCalls();
    normalizeStoredConfig(clean);

    assert.equal(info.mock.calls.length, 0, "a file that needs no correction logs nothing");
  });

  test("a file with no capture values normalizes with none while the environment and the running configuration hold capture values", () => {

    /* The codec list chosen here needs a correction whose result, ["h264"], differs from the default, so a normalization that read it from the environment or
     * the running configuration would store that result in the file it returns, where this row would see it.
     */
    const originalEnv = { ...process.env };
    const originalStreaming = structuredClone(CONFIG.streaming);

    try {

      process.env["CAPTURE_CODECS"] = "h264,av1";
      process.env["CAPTURE_MODE"] = "native";
      CONFIG.streaming.captureCodecs = [ "h264", "av1" ];
      CONFIG.streaming.captureMode = "native";

      assert.deepEqual(normalizeStoredConfig({ server: { port: 6000 } }), { server: { port: 6000 } });
    } finally {

      for(const key of Object.keys(process.env)) {

        Reflect.deleteProperty(process.env, key);
      }

      Object.assign(process.env, originalEnv);
      Object.assign(CONFIG.streaming, originalStreaming);
    }
  });

  test("a write through the configuration store stores corrected capture values", async (t) => {

    const info = t.mock.method(LOG, "info", () => undefined);

    // The row points the data directory at a temporary directory withTempDir removes, so once the row ends the resolver names os.tmpdir() instead, a directory
    // that exists, as the services suite's store row leaves it.
    t.after(() => {

      initializeDataDir(os.tmpdir());
    });

    await withTempDir(async (dir) => {

      initializeDataDir(dir);
      await mutateConfig((current) => { current.streaming = { captureCodecs: [ "h264", "av1" ] }; });

      assert.deepEqual((await readConfig()).config.streaming?.captureCodecs, ["h264"], "the file holds the list in effect");
    });

    assert.deepEqual(info.mock.calls.filter((call) => call.arguments[0] === CORRECTION_LINE).map((call) => [...call.arguments]),
      [[ CORRECTION_LINE, { corrections: [{ configured: [ "h264", "av1" ], ignored: ["av1"], kind: "unrecognizedCodecs", using: ["h264"] }] } ]],
      "the write logged its correction once");
  });
});

/* The DeviceID correction reads the candidate the merge builds from a file, so each row builds its candidate from the file it names through mergeConfiguration,
 * the way the boot and a save build theirs, and reads the candidate and the file afterward. No row sets an environment variable, so the merge leaves HDHomeRun
 * enabled by its default unless the file turns it off.
 */
describe("correctStoredDeviceId", () => {

  /**
   * Builds the candidate a file yields and runs the correction on the candidate and the file.
   * @param file - The configuration file, which the correction may write.
   * @param running - The running DeviceID handed to the correction.
   * @returns The correction's answer and the candidate.
   */
  function correct(file: UserConfig, running: string): { candidate: Config; result: ReturnType<typeof correctStoredDeviceId> } {

    const candidate = mergeConfiguration(file);

    return { candidate, result: correctStoredDeviceId({ candidate, file, running }) };
  }

  test("a valid stored id answers null and leaves the candidate and the file unchanged", () => {

    const file: UserConfig = { hdhr: { deviceId: SEEDED_DEVICE_ID } };
    const { candidate, result } = correct(file, "");

    assert.equal(result, null);
    assert.equal(candidate.hdhr.deviceId, SEEDED_DEVICE_ID);
    assert.deepEqual(file, { hdhr: { deviceId: SEEDED_DEVICE_ID } });
  });

  test("a file with no hdhr category gains a DeviceID equal to the candidate's, which passes its checksum", () => {

    const file: UserConfig = {};
    const { candidate, result } = correct(file, "");

    assert.ok(validateDeviceId(candidate.hdhr.deviceId), "the candidate's new id passes its checksum");
    assert.equal(file.hdhr?.deviceId, candidate.hdhr.deviceId, "the file holds the candidate's id");
    assert.deepEqual(result, { configured: "", generated: true, kind: "deviceId", using: candidate.hdhr.deviceId });
  });

  test("a file whose hdhr category is not an object gains an object holding the DeviceID", () => {

    const file = { hdhr: false } as unknown as UserConfig;
    const { candidate } = correct(file, "");

    assert.deepEqual(file.hdhr, { deviceId: candidate.hdhr.deviceId }, "the category is replaced by an object holding the id");
    assert.ok(validateDeviceId(candidate.hdhr.deviceId));
  });

  test("a stored id that fails its checksum takes a valid running id rather than a generated one", () => {

    const file: UserConfig = { hdhr: { deviceId: "10000000" } };
    const { candidate, result } = correct(file, SEEDED_DEVICE_ID);

    assert.deepEqual(result, { configured: "10000000", generated: false, kind: "deviceId", using: SEEDED_DEVICE_ID });
    assert.equal(candidate.hdhr.deviceId, SEEDED_DEVICE_ID);
    assert.equal(file.hdhr?.deviceId, SEEDED_DEVICE_ID);
  });

  test("a stored id that fails its checksum beside a running id that fails too takes a generated id", () => {

    const file: UserConfig = { hdhr: { deviceId: "10000000" } };
    const { candidate, result } = correct(file, "10000000");

    assert.ok(result !== null, "the stored id needs a correction");
    assert.equal(result.generated, true);
    assert.equal(result.configured, "10000000");
    assert.ok(validateDeviceId(result.using), "the generated id passes its checksum");
    assert.equal(candidate.hdhr.deviceId, result.using);
    assert.equal(file.hdhr?.deviceId, result.using);
  });

  test("a disabled emulation answers null and leaves a stored id that fails its checksum as stored", () => {

    const file: UserConfig = { hdhr: { deviceId: "10000000", enabled: false } };
    const { candidate, result } = correct(file, "");

    assert.equal(result, null);
    assert.equal(candidate.hdhr.deviceId, "10000000", "the candidate keeps the stored id");
    assert.equal(file.hdhr?.deviceId, "10000000", "the file keeps the stored id");
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

    /* Asserts that the lookup walks every category, not just the first. Picking hls.segmentDuration covers a category beyond the server group and the
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

  test("the section holding the precache list carries the label the page renders, Precaching", () => {

    // generateSettingsTabContent writes a section's display name into its header unchanged, so the label read here is the one the page shows.
    const sections = getSettingsTabSections();

    assert.equal(sections.find((s) => s.id === "startup")?.displayName, "Precaching", "the section whose id is startup is labelled Precaching");
    assert.ok(!sections.some((s) => s.displayName === "Startup"), "no section is labelled Startup");
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
     * returned undefined would have been filtered out, and we observe no holes. This asserts the silent-filter contract.
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

/* The reactivity classification is total: every leaf the defaults define resolves to exactly one class, from its metadata when it is a setting and from its
 * PROCESS_FIELDS state entry otherwise. The known-answer rows restate the classification table setting by setting, grouped as that table groups them, so a class
 * that drifts from the readers it was read off fails here by name rather than in a save.
 */
describe("reactivity classification", () => {

  const KNOWN_SETTING_CLASSES: readonly (readonly [ ReactivityClass, readonly string[] ])[] = [

    // Read at each Chrome launch; a relaunch is unscheduled, so restart is the contract a user can act on.
    [ "restart", ["browser.executablePath"] ],

    // Read at each launch's extension handshake, which a running browser has finished.
    [ "live", ["browser.initTimeout"] ],

    // The precache module's handler walks the services a save adds.
    [ "live", ["channels.precacheServices"] ],

    // Read per DVR request.
    [ "live", ["channelsDvr.port"] ],

    // The HDHomeRun handler reconciles them, and the name is read per request.
    [ "live", [ "hdhr.discoveryEnabled", "hdhr.enabled", "hdhr.friendlyName", "hdhr.port" ] ],

    // Read per stored segment, per playlist, and per idle sweep.
    [ "live", [ "hls.idleTimeout", "hls.maxSegments" ] ],

    // Copied into each stream's settings when the stream registers.
    [ "next-stream", ["hls.segmentDuration"] ],

    // Read per request.
    [ "live", ["logging.httpLogLevel"] ],

    // The composition root's handler resizes the open logger.
    [ "live", ["logging.maxSize"] ],

    // Read at launch, teardown, and exit, which must agree, and the logger opens its file at boot.
    [ "restart", [ "paths.chromeDataDir", "paths.logFile" ] ],

    // Read per monitor tick, per recovery decision, or per tune step.
    [ "live", [ "playback.bufferingGracePeriod", "playback.channelSelectorDelay", "playback.channelSwitchDelay", "playback.iframeInitDelay",
      "playback.maxPageReloads", "playback.pageReloadWindow", "playback.sourceReloadDelay", "playback.stallCountThreshold", "playback.stallThreshold",
      "playback.sustainedPlaybackRequired" ] ],

    // Copied into each stream's settings when the stream registers.
    [ "next-stream", ["playback.monitorInterval"] ],

    // The browser module's handler re-arms the running stale-page sweep.
    [ "live", ["recovery.stalePageCleanupInterval"] ],

    // Read per failure, per governor decision, per sweep, or per tune.
    [ "live", [ "recovery.backoffJitter", "recovery.circuitBreakerThreshold", "recovery.circuitBreakerWindow", "recovery.maxBackoffDelay",
      "recovery.relaunchFailureThreshold", "recovery.relaunchFailureWindow", "recovery.relaunchHealthHold", "recovery.stalePageGracePeriod" ] ],

    // The listener binds once.
    [ "restart", [ "server.host", "server.port" ] ],

    // Copied into each stream's settings when the stream registers.
    [ "next-stream", [ "streaming.audioBitsPerSecond", "streaming.frameRate", "streaming.videoBitsPerSecond" ] ],

    // The preroll is encoded once at boot from them.
    [ "restart", [ "streaming.captureCodecs", "streaming.captureMode", "streaming.qualityPreset" ] ],

    // Read per admission or per use.
    [ "live", [ "streaming.maxConcurrentStreams", "streaming.maxNavigationRetries", "streaming.navigationTimeout", "streaming.videoTimeout" ] ]
  ];

  const KNOWN_PROCESS_FIELD_CLASSES: readonly (readonly [ string, ReactivityClass ])[] = [

    [ "channels.channelSortDirection", "live" ],
    [ "channels.channelSortField", "live" ],
    [ "channels.disabledPredefined", "live" ],
    [ "channels.enabledServices", "live" ],
    [ "channels.setupCompleted", "live" ],
    [ "channels.visibleColumns", "live" ],
    [ "channelsDvr.host", "live" ],
    [ "hdhr.deviceId", "live" ],
    [ "logging.debugFilter", "live" ]
  ];

  const metadataPaths = new Set(Object.values(CONFIG_METADATA).flat().map((setting) => setting.path));

  test("every metadata entry declares a class of the union", () => {

    const classes = new Set<string>([ "live", "next-stream", "restart" ]);

    for(const setting of Object.values(CONFIG_METADATA).flat()) {

      assert.ok(classes.has(setting.reactivity), setting.path + " declares a class of the union");
    }
  });

  test("every setting the classification table names carries the class the table states, and the table names every setting", () => {

    const named = KNOWN_SETTING_CLASSES.flatMap(([ , settingPaths ]) => settingPaths);

    assert.deepEqual(named.toSorted(), [...metadataPaths].toSorted(), "the known-answer rows cover the settings metadata exactly");

    for(const [ reactivity, settingPaths ] of KNOWN_SETTING_CLASSES) {

      for(const settingPath of settingPaths) {

        assert.equal(getSettingByPath(settingPath)?.reactivity, reactivity, settingPath + " declares " + reactivity);
      }
    }
  });

  // The table's state entries, in the table's own key order, each with the class it states.
  const stateEntries = Object.entries(PROCESS_FIELDS).flatMap(([ fieldPath, field ]) => ((field.kind === "state") ? [[ fieldPath, field.reactivity ] as const] : []));

  test("the table's state entries are exactly the leaves the defaults define outside the metadata, each with the class the known answers state", () => {

    const outside = listConfigLeafPaths().filter((leaf) => !metadataPaths.has(leaf));

    assert.ok(outside.length > 0, "precondition: the defaults define leaves outside the metadata");
    assert.deepEqual(stateEntries.map(([fieldPath]) => fieldPath), outside, "the table's state keys are those leaves, in path order");
    assert.deepEqual(stateEntries, KNOWN_PROCESS_FIELD_CLASSES);
  });

  test("no table key is a metadata path, and every schema key is absent from the leaves the defaults define", () => {

    const leaves = new Set(listConfigLeafPaths());
    const schemaKeys = Object.entries(PROCESS_FIELDS).filter(([ , field ]) => field.kind === "schema").map(([fieldPath]) => fieldPath);

    assert.ok(schemaKeys.length > 0, "precondition: the table declares schema fields");
    assert.deepEqual(Object.keys(PROCESS_FIELDS).filter((fieldPath) => metadataPaths.has(fieldPath)), [], "the table holds no setting");
    assert.deepEqual(schemaKeys.filter((fieldPath) => leaves.has(fieldPath)), [], "a schema field exists only in the file, never in the running configuration");
  });

  test("every leaf the defaults define resolves through the resolver to the class the known-answer tables state, so a wrong answer fails the row", () => {

    // The expected class comes from the known-answer tables above rather than from the metadata or the PROCESS_FIELDS table the resolver itself reads, so a
    // resolver that consults the wrong source or answers a fixed class disagrees with an independent statement of the classification.
    const settingEntries = KNOWN_SETTING_CLASSES.flatMap(([ reactivity, settingPaths ]) => settingPaths.map((settingPath) => [ settingPath, reactivity ] as const));
    const known = new Map<string, ReactivityClass>([ ...settingEntries, ...KNOWN_PROCESS_FIELD_CLASSES ]);
    const leaves = listConfigLeafPaths();

    assert.deepEqual(leaves.toSorted(), [...known.keys()].toSorted(), "precondition: the known-answer tables state a class for every leaf the defaults define");

    for(const leaf of leaves) {

      assert.equal(getReactivityClass(leaf), known.get(leaf), leaf + " resolves to the class the known-answer tables state");
    }
  });

  test("a path neither the metadata nor the PROCESS_FIELDS table carries throws, naming the path", () => {

    assert.throws(() => getReactivityClass("hdhr.friendlyName.nested"), { message: "The configuration path hdhr.friendlyName.nested carries no reactivity class." });
  });

  test("a schema field carries no class, because it exists only in the file", () => {

    assert.throws(() => getReactivityClass("schemaVersion"), { message: "The configuration path schemaVersion carries no reactivity class." });
  });

  test("an inherited key is never read as a field, so it carries no class", () => {

    assert.throws(() => getReactivityClass("toString"), { message: "The configuration path toString carries no reactivity class." });
  });
});
