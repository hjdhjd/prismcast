/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userConfig.merge.test.ts: Unit tests for the env- and CLI-aware portions of the user-config layer - mergeConfiguration, getEnvOverrides, getEnvOverrideValue,
 * filterDefaults, and the save and restore rules of the fields the process writes (the PROCESS_FIELDS table, and channelsDvr.host hydration into runtime
 * CONFIG). These are the tests that mutate process.env, kept apart from the pure-function tests in userConfig.test.ts.
 */
import { CONFIG_METADATA, DEFAULTS, PROCESS_FIELDS, filterDefaults, getEnvOverrideValue, getEnvOverrides, getNestedValue, mergeConfiguration,
  setNestedValue } from "./userConfig.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { LOG } from "../utils/index.ts";
import type { UserConfig } from "./userConfig.ts";
import assert from "node:assert/strict";

describe("mergeConfiguration", () => {

  /* Each test resets process.env to its starting value. mergeConfiguration reads env vars during the merge; without isolation, a single env-var leak from one
   * test can poison every subsequent test's defaults.
   */
  const ORIGINAL_ENV = { ...process.env };

  beforeEach(() => {

    // Clear env vars that mergeConfiguration consults so per-test overrides are deterministic.
    for(const settings of Object.values(CONFIG_METADATA)) {

      for(const setting of settings) {

        if(setting.envVar) {

          Reflect.deleteProperty(process.env, setting.envVar);
        }
      }
    }
  });

  afterEach(() => {

    // Restore the full environment so unrelated tests in other suites are not affected.
    for(const key of Object.keys(process.env)) {

      Reflect.deleteProperty(process.env, key);
    }

    Object.assign(process.env, ORIGINAL_ENV);
  });

  test("returns a fresh DEFAULTS clone when given empty input", () => {

    const result = mergeConfiguration({});

    assert.deepEqual(result, DEFAULTS);
    assert.notEqual(result, DEFAULTS, "result is a structural copy, not the DEFAULTS reference");
  });

  test("user config overrides defaults", () => {

    const userConfig: UserConfig = { server: { port: 9999 } };
    const result = mergeConfiguration(userConfig);

    assert.equal(result.server.port, 9999, "user config override applied");
    assert.equal(result.server.host, DEFAULTS.server.host, "non-overridden field still default");
  });

  test("environment variables override user config", () => {

    process.env["PORT"] = "12345";

    const userConfig: UserConfig = { server: { port: 9999 } };
    const result = mergeConfiguration(userConfig);

    assert.equal(result.server.port, 12345, "env var wins over user config");
  });

  test("CLI overrides take the highest priority over env and user config", () => {

    process.env["PORT"] = "12345";

    const userConfig: UserConfig = { server: { port: 9999 } };
    const result = mergeConfiguration(userConfig, { "server.port": 7777 });

    assert.equal(result.server.port, 7777, "CLI wins");
  });

  test("array fields are spread-copied so mutating the result does not leak into the user config", () => {

    const userConfig: UserConfig = { channels: { disabledPredefined: ["abc"] } };
    const result = mergeConfiguration(userConfig);

    result.channels.disabledPredefined.push("nbc");

    assert.deepEqual(userConfig.channels?.disabledPredefined, ["abc"], "user config array unchanged");
  });

  test("a list setting merges as a clone, so mutating the merged precache list leaves the parsed file's list untouched", () => {

    const userConfig: UserConfig = { channels: { precacheServices: ["hulu"] } };
    const result = mergeConfiguration(userConfig);

    result.channels.precacheServices.push("sling");

    assert.deepEqual(userConfig.channels?.precacheServices, ["hulu"], "the parsed file's list is unchanged");
  });

  test("a stored list holding the default's members in another order merges in the default's order", () => {

    const result = mergeConfiguration({ streaming: { captureCodecs: [ "hevc", "h264" ] } });

    assert.deepEqual(result.streaming.captureCodecs, [ "h264", "hevc" ]);
  });

  test("a stored value that is not a list at a list setting merges as the default", () => {

    const result = mergeConfiguration({ channels: { precacheServices: "hulu" as unknown as string[] } });

    assert.deepEqual(result.channels.precacheServices, []);
  });

  test("user-supplied enabledServices array survives the merge", () => {

    const userConfig: UserConfig = { channels: { enabledServices: [ "hulu", "yttv" ] } };
    const result = mergeConfiguration(userConfig);

    assert.deepEqual(result.channels.enabledServices, [ "hulu", "yttv" ]);
  });

  test("non-string sortField values are ignored (defensive)", () => {

    // channelSortField is brought back by its PROCESS_FIELDS state rule, which takes only a value with its default's shape that is not the empty string. A
    // non-string value fails the rule and the default is preserved.
    const userConfig = { channels: { channelSortField: 123 as unknown as string } } as UserConfig;
    const result = mergeConfiguration(userConfig);

    assert.equal(result.channels.channelSortField, DEFAULTS.channels.channelSortField);
  });

  test("undefined CLI override values are ignored", () => {

    const result = mergeConfiguration({}, { "server.port": undefined });

    assert.equal(result.server.port, DEFAULTS.server.port);
  });

  test("env var with invalid integer parses as undefined and falls through", () => {

    process.env["PORT"] = "not-a-number";

    const result = mergeConfiguration({});

    assert.equal(result.server.port, DEFAULTS.server.port, "invalid env var ignored, default preserved");
  });

  test("boolean env vars accept 'yes'/'1' as true and 'false' as false", () => {

    process.env["HDHR_ENABLED"] = "yes";
    assert.equal(mergeConfiguration({}).hdhr.enabled, true);

    process.env["HDHR_ENABLED"] = "1";
    assert.equal(mergeConfiguration({}).hdhr.enabled, true);

    process.env["HDHR_ENABLED"] = "false";
    assert.equal(mergeConfiguration({}).hdhr.enabled, false);
  });

  test("checkboxList env var splits on commas and trims whitespace", () => {

    process.env["CAPTURE_CODECS"] = " h264 , hevc ";

    const result = mergeConfiguration({});

    assert.deepEqual(result.streaming.captureCodecs, [ "h264", "hevc" ]);
  });

  test("path env var with empty string normalizes to null", () => {

    process.env["PRISMCAST_LOG_FILE"] = "";

    const result = mergeConfiguration({});

    assert.equal(result.paths.logFile, null, "empty path env var means use default (null)");
  });

  test("text env vars are sanitized at the ingress across all three text types", () => {

    /* The host, path, and string arms all clean their value the way the settings form and the config import already clean these same types. Each vehicle below
     * carries surrounding padding, and the string vehicle also carries an embedded non-printable character that no amount of trimming would remove. The host and
     * path vehicles embed a null byte, but process.env truncates a value at a null byte, so those rows exercise the trimming only. Reaching CONFIG uncleaned
     * matters because a host or free-string value has no validator behind it - a host with a trailing newline would be used as written.
     */
    process.env["HOST"] = "  192.168.1.50\u0000  ";
    process.env["HDHR_FRIENDLY_NAME"] = " Living\u200bRoom Tuner ";
    process.env["PRISMCAST_LOG_FILE"] = "  /var/log/prismcast.log\u0000 ";

    const result = mergeConfiguration({});

    assert.equal(result.server.host, "192.168.1.50", "the host env var is trimmed and stripped of non-printables");
    assert.equal(result.hdhr.friendlyName, "LivingRoom Tuner", "the string env var is trimmed and stripped of non-printables");
    assert.equal(result.paths.logFile, "/var/log/prismcast.log", "the path env var is trimmed and stripped of non-printables");
  });

  test("a path env var yields the null sentinel when it holds nothing visible", () => {

    /* These boundaries share one arm. A whitespace-only value collapses to the sentinel because trimming empties it. A value made entirely of non-printable
     * characters does not - trimming leaves it intact - so it is the case that separates cleaning from trimming. Both must land on null, because a path holding
     * nothing visible means "use the default", and for paths.logFile the alternative is a value that fails the absolute-path check and takes startup down with it.
     */
    process.env["PRISMCAST_LOG_FILE"] = "   ";

    assert.equal(mergeConfiguration({}).paths.logFile, null, "a whitespace-only path env var means use the default");

    process.env["PRISMCAST_LOG_FILE"] = "\u0001";

    assert.equal(mergeConfiguration({}).paths.logFile, null, "a path env var holding only a control character means use the default");
  });

  test("the non-text arms are left alone - a padded boolean env var is still not truthy", () => {

    /* Scope assertion. Sanitization covers exactly the text types named by TEXT_SETTING_TYPES, which is the codebase's own definition of a text setting, and nothing
     * beyond them. The boolean arm compares the raw lowercased value against its accepted words, so a padded "true" matches none of them and the arm returns
     * false. Note what that means here: the default for this setting is true, so the override still applies and turns it off - the padded value is not ignored,
     * it is read as a negative. Widening sanitization to this arm would make the padded value match and flip the result back to true, which is why the
     * assertion is written against the padded form.
     */
    process.env["HDHR_ENABLED"] = " true ";

    const result = mergeConfiguration({});

    assert.equal(result.hdhr.enabled, false, "the boolean arm does not trim, so a padded value does not read as true");
  });

  test("integer env var with invalid value falls through for non-PORT settings (e.g., VIDEO_BITRATE)", () => {

    /* The merge has integer-parsing fall-through for every integer field, not just PORT. We assert VIDEO_BITRATE to lock that the per-type branch fires
     * uniformly across CONFIG_METADATA entries; a regression that bypassed parseEnvValue's NaN guard for non-PORT integer settings would surface here.
     */
    process.env["VIDEO_BITRATE"] = "not-a-number";

    const result = mergeConfiguration({});

    assert.equal(result.streaming.videoBitsPerSecond, DEFAULTS.streaming.videoBitsPerSecond, "invalid VIDEO_BITRATE env var ignored, default preserved");
  });

  test("env var that parses as zero is honored (no truthiness gate on parsed values)", () => {

    /* Boundary: the merge writes through any parsed value because resolveEnvOverride's guard is its `parsed === undefined` check. This row asserts that a single
     * non-empty checkboxList override reaches CONFIG unchanged, with no per-element filter applied to the array contents. Its value is truthy, so the row does not
     * tell the undefined check apart from a truthiness gate.
     */
    process.env["CAPTURE_CODECS"] = "h264";

    const result = mergeConfiguration({});

    assert.deepEqual(result.streaming.captureCodecs, ["h264"], "single-codec env override applied without falsy filtering");
  });

  test("CLI override with a non-undefined object value is written through", () => {

    /* The CLI overrides loop writes any non-undefined value into CONFIG via setNestedValue. Here we exercise a string-valued override and assert that the loop
     * accepts it unchanged; setNestedValue places it at the dotted path so the value reaches runtime CONFIG.
     */
    const result = mergeConfiguration({}, { "paths.chromeDataDir": "/tmp/explicit/chrome-data-override" });

    assert.equal(result.paths.chromeDataDir, "/tmp/explicit/chrome-data-override", "CLI override value reaches runtime CONFIG");
  });

  test("checkboxList env var that is empty parses to an empty array (no codec override)", () => {

    /* Boundary: parseEnvValue's checkboxList branch splits on commas and filters empty strings. An entirely-empty env var produces an empty array, which
     * mergeConfiguration writes through. buildCandidate's normalization (correctCaptureValues) restores the H.264 baseline afterward; the merge layer's contract
     * is just "produce the user's literal".
     */
    process.env["CAPTURE_CODECS"] = "";

    const result = mergeConfiguration({});

    assert.deepEqual(result.streaming.captureCodecs, [], "empty CAPTURE_CODECS env var produces an empty list at the merge layer");
  });

  test("float env var is parsed via parseFloat and applied (STALL_THRESHOLD)", () => {

    /* This row covers parseEnvValue's float branch. STALL_THRESHOLD is a documented float setting; asserting the parse here locks the type-specific branch.
     */
    process.env["STALL_THRESHOLD"] = "0.42";

    const result = mergeConfiguration({});

    assert.equal(result.playback.stallThreshold, 0.42, "float env var parsed and applied");
  });
});

describe("getEnvOverrides", () => {

  const ORIGINAL_ENV = { ...process.env };

  afterEach(() => {

    for(const key of Object.keys(process.env)) {

      Reflect.deleteProperty(process.env, key);
    }

    Object.assign(process.env, ORIGINAL_ENV);
  });

  test("returns an empty map when no env vars are set", () => {

    for(const settings of Object.values(CONFIG_METADATA)) {

      for(const setting of settings) {

        if(setting.envVar) {

          Reflect.deleteProperty(process.env, setting.envVar);
        }
      }
    }

    const overrides = getEnvOverrides();

    assert.equal(overrides.size, 0);
  });

  test("returns a map keyed by setting path when env vars are set", () => {

    process.env["PORT"] = "9000";
    process.env["VIDEO_BITRATE"] = "10000000";

    const overrides = getEnvOverrides();

    assert.equal(overrides.get("server.port"), "9000");
    assert.equal(overrides.get("streaming.videoBitsPerSecond"), "10000000");
  });

  /* The map is what the settings form disables fields from and what the override badge beside each disabled field displays, so every row below compares the
   * map's value against the value mergeConfiguration actually stored for the same variable. The pairing is the point: a badge that reports the raw variable
   * text while the configuration holds a parsed and sanitized value tells the operator something untrue about their own server.
   */

  test("a free-string setting's padded, non-printable env value appears in the map as the sanitized text the merge stored", () => {

    /* A zero-width space rather than a null byte: process.env values cross into the OS environment as NUL-terminated strings, so a NUL would be truncated by
     * the assignment itself before sanitizeString ever saw it. The zero-width space is the same class of invisible corruption an operator pastes in.
     */
    process.env["HDHR_FRIENDLY_NAME"] = "  Living\u200B Room  ";

    const overrides = getEnvOverrides();
    const merged = mergeConfiguration({});

    assert.equal(overrides.get("hdhr.friendlyName"), "Living Room", "the map holds the sanitized text, not the raw variable");
    assert.equal(merged.hdhr.friendlyName, "Living Room", "and that text is exactly what the merge stored");
  });

  test("a host setting's padded, non-printable env value appears in the map as the sanitized text the merge stored", () => {

    process.env["HOST"] = "\uFEFF 10.0.0.5 ";

    const overrides = getEnvOverrides();
    const merged = mergeConfiguration({});

    assert.equal(overrides.get("server.host"), "10.0.0.5", "the map holds the sanitized host");
    assert.equal(merged.server.host, "10.0.0.5", "and that host is exactly what the merge stored");
  });

  test("a boolean setting's \"yes\" env value appears in the map as \"true\" - the display form of the boolean the merge stored", () => {

    process.env["HDHR_ENABLED"] = "yes";

    const overrides = getEnvOverrides();
    const merged = mergeConfiguration({});

    assert.equal(overrides.get("hdhr.enabled"), "true", "the map holds the applied boolean's display form");
    assert.equal(merged.hdhr.enabled, true, "and the merge stored the boolean itself");
  });

  test("a comma-separated list env value appears in the map joined on the separator it was split with", () => {

    process.env["CAPTURE_CODECS"] = " h264 , hevc ";

    const overrides = getEnvOverrides();
    const merged = mergeConfiguration({});

    assert.equal(overrides.get("streaming.captureCodecs"), "h264,hevc", "the map holds the list an operator could paste back into the variable");
    assert.deepEqual(merged.streaming.captureCodecs, [ "h264", "hevc" ], "and the merge stored the parsed list");
  });

  test("a path setting cleared to the empty string appears in the map with no text, matching the null the merge stored", () => {

    process.env["PRISMCAST_LOG_FILE"] = "";

    const overrides = getEnvOverrides();
    const merged = mergeConfiguration({});

    assert.equal(overrides.has("paths.logFile"), true, "an empty path variable is still an override - the field stays disabled");
    assert.equal(overrides.get("paths.logFile"), "", "the null the merge stored has no text to display");
    assert.equal(merged.paths.logFile, null, "and the merge stored the cleared-path sentinel");
  });

  test("an unparseable numeric env value is absent from the map, exactly as the merge declines to apply it", () => {

    process.env["VIDEO_BITRATE"] = "not-a-number";

    const overrides = getEnvOverrides();
    const merged = mergeConfiguration({});

    assert.equal(overrides.has("streaming.videoBitsPerSecond"), false, "an unparseable value is not an override, so the field stays editable");
    assert.equal(merged.streaming.videoBitsPerSecond, DEFAULTS.streaming.videoBitsPerSecond, "and the merge left the layer below in place");
  });

  test("a discarded environment variable is reported by the merge and never by the badge reader", (t) => {

    /* Which caller reports is the whole design, so the row asserts both halves against one variable. The settings page reads the environment on every page load,
     * so a reader that reported would log a line per render rather than per operator action; the merge runs on a boot and on each save to the settings, each an
     * operator's own action, so a line per merge arrives when they would look for it. Spying on LOG.warn rather than swapping the logger keeps the assertion
     * narrow, and reading the substitution arguments rather than a formatted string keeps it independent of the format.
     */
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });

    process.env["VIDEO_BITRATE"] = "not-a-number";

    getEnvOverrides();

    assert.equal(warn.mock.calls.length, 0, "resolving the environment to render a badge reports nothing at all");

    mergeConfiguration({});

    const reported = warn.mock.calls.filter((entry) => entry.arguments[1] === "VIDEO_BITRATE");

    assert.equal(reported.length, 1, "the merge reports the discarded variable exactly once");

    const [call] = reported;

    assert.ok(call, "sanity: the one reported call is in hand");
    assert.match(String(call.arguments[0]), /is not a valid/, "and the message says the text was not a valid value for the setting's type");
    assert.equal(call.arguments[2], "not-a-number", "quoting the text that was discarded");
    assert.equal(call.arguments[4], "streaming.videoBitsPerSecond", "and naming the setting it belonged to");
  });
});

describe("getEnvOverrideValue", () => {

  /* The single-setting accessor over the environment resolver, for callers that want the value and have nothing to report about its absence. It is what
   * lets a caller outside the configuration layer - the process exit handler asking where the log file would be - read the environment through the same
   * parser the merge uses rather than reaching for process.env itself.
   */

  const ORIGINAL_ENV = { ...process.env };

  afterEach(() => {

    for(const key of Object.keys(process.env)) {

      Reflect.deleteProperty(process.env, key);
    }

    Object.assign(process.env, ORIGINAL_ENV);
  });

  test("returns the parsed value for a variable that is set", () => {

    process.env["PRISMCAST_LOG_FILE"] = "/env/prismcast.log";

    assert.equal(getEnvOverrideValue("paths.logFile"), "/env/prismcast.log");
  });

  test("returns undefined for a variable that is unset", () => {

    Reflect.deleteProperty(process.env, "PRISMCAST_LOG_FILE");

    assert.equal(getEnvOverrideValue("paths.logFile"), undefined);
  });

  test("returns undefined for a path that names no setting", () => {

    assert.equal(getEnvOverrideValue("paths.nothingIsHere"), undefined);
  });
});

describe("filterDefaults", () => {

  test("drops fields that match defaults", () => {

    const filtered = filterDefaults({ server: { port: DEFAULTS.server.port } });

    assert.equal(getNestedValue(filtered, "server.port"), undefined, "default value should be dropped");
  });

  test("preserves fields that differ from defaults", () => {

    const filtered = filterDefaults({ server: { port: 9999 } });

    assert.equal(getNestedValue(filtered, "server.port"), 9999);
  });

  test("removes empty parent objects after filtering", () => {

    const filtered = filterDefaults({ server: { host: DEFAULTS.server.host, port: DEFAULTS.server.port } });

    assert.equal("server" in filtered, false, "empty server group is removed");
  });

  test("preserves non-empty array fields even when all entries match defaults set elsewhere", () => {

    // disabledPredefined is not in CONFIG_METADATA, so the metadata loop never compares it; it is kept by its PROCESS_FIELDS state rule, which keeps a
    // non-empty user list while letting an empty list (identical to the empty-array default) collapse.
    const filtered = filterDefaults({ channels: { disabledPredefined: ["nbc"] } });

    assert.deepEqual(getNestedValue(filtered, "channels.disabledPredefined"), ["nbc"]);
  });

  test("drops empty arrays when they equal the empty-array default", () => {

    const filtered = filterDefaults({ channels: { disabledPredefined: [] } });

    assert.equal("channels" in filtered, false, "empty disabledPredefined collapses with empty parent");
  });

  test("preserves channelsDvr.host auto-discovery field", () => {

    /* The rule enforced after the v3 schema migration: channelsDvr.host is host-only, never host:port. The fixture reflects that rule. The migration
     * itself (which splits any legacy host:port into host + port) is covered in userConfig.migrations.test.ts; this test is about preservation through
     * filterDefaults's PROCESS_FIELDS loop - the auto-discovered host is not in CONFIG_METADATA so the standard metadata loop would drop it without that loop.
     */
    const filtered = filterDefaults({ channelsDvr: { host: "192.168.1.5" } });

    assert.equal((filtered as { channelsDvr?: { host?: string } }).channelsDvr?.host, "192.168.1.5");
  });

  test("preserves schemaVersion and migrationsApplied metadata", () => {

    const filtered = filterDefaults({ migrationsApplied: ["x"], schemaVersion: 2 });

    assert.equal((filtered as { schemaVersion?: number }).schemaVersion, 2);
    assert.deepEqual((filtered as { migrationsApplied?: string[] }).migrationsApplied, ["x"]);
  });

  test("captureCodecs is preserved when it differs from the default", () => {

    /* DEFAULTS.streaming.captureCodecs = [h264, hevc]. A user list with only h264 differs and must be preserved through the filter so the user's choice
     * survives a save/reload cycle.
     */
    const filteredDiff = filterDefaults({ streaming: { captureCodecs: ["h264"] } });

    assert.deepEqual(getNestedValue(filteredDiff, "streaming.captureCodecs"), ["h264"]);
  });

  test("captureCodecs equal to default in same order is dropped", () => {

    // captureCodecs is a metadata setting, so the metadata loop decides it through isEqualToDefault, which compares a list by its members. A user list
    // identical to the default (same elements, same order) holds the default's members and is therefore not preserved.
    const filtered = filterDefaults({ streaming: { captureCodecs: [...DEFAULTS.streaming.captureCodecs] } });

    assert.equal(getNestedValue(filtered, "streaming.captureCodecs"), undefined, "default-equal captureCodecs is dropped");
  });

  test("captureCodecs reordered relative to default is treated as default-equal by its members", () => {

    /* A reordered codec list (e.g., [hevc, h264] vs default [h264, hevc]) is the same set of choices, so it must be stripped from the persisted shape and the
     * on-disk file does not capture a meaningless reorder. The metadata loop's isEqualToDefault compares an array against an array default by its members in
     * any order, so it classifies the reordered list as default-equal and writes nothing.
     */
    const reordered = [...DEFAULTS.streaming.captureCodecs].toReversed();
    const filtered = filterDefaults({ streaming: { captureCodecs: reordered } });

    assert.equal(getNestedValue(filtered, "streaming.captureCodecs"), undefined,
      "reordered captureCodecs treated as default-equal by its members");
  });

  test("captureCodecs with a different content set is preserved (not just reorder)", () => {

    /* Boundary on the members comparison: a user list missing one of the default codecs is a real customization; it must survive the filter even when the
     * survivor codec appears in the default. isEqualToDefault compares the value and the default as sorted sequences, so any difference in the element
     * multiset is preserved.
     */
    const filtered = filterDefaults({ streaming: { captureCodecs: ["h264"] } });

    assert.deepEqual(getNestedValue(filtered, "streaming.captureCodecs"), ["h264"], "single-codec list is a customization and survives");
  });

  test("captureCodecs holding more than the default's members is preserved", () => {

    const filtered = filterDefaults({ streaming: { captureCodecs: [ "h264", "hevc", "av1" ] } });

    assert.deepEqual(getNestedValue(filtered, "streaming.captureCodecs"), [ "h264", "hevc", "av1" ], "a superset of the default is a customization and survives");
  });

  test("the precache list is kept when it names a service, and dropped when empty or not a list", () => {

    assert.deepEqual(getNestedValue(filterDefaults({ channels: { precacheServices: ["hulu"] } }), "channels.precacheServices"), ["hulu"], "a listed service is kept");
    assert.equal(getNestedValue(filterDefaults({ channels: { precacheServices: [] } }), "channels.precacheServices"), undefined, "the empty default is dropped");
    assert.equal(getNestedValue(filterDefaults({ channels: { precacheServices: null as unknown as string[] } }), "channels.precacheServices"), undefined,
      "a null list counts as absent and is dropped");
  });

  test("recursive removeEmptyObjects walks nested mixed levels (some children empty, some populated)", () => {

    /* The recursive cleanup is exercised end-to-end via filterDefaults whenever a nested group has both default-equal and non-default fields. We construct a
     * fixture with two sibling groups - one whose every field equals the default (so it collapses) and one with a real customization - and assert the empty
     * sibling vanishes while the populated one survives.
     */
    const filtered = filterDefaults({

      hls: { segmentDuration: 7 },
      server: { host: DEFAULTS.server.host, port: DEFAULTS.server.port }
    });

    assert.equal("server" in filtered, false, "fully-default server group collapses");
    assert.deepEqual((filtered as { hls?: { segmentDuration?: number } }).hls, { segmentDuration: 7 },
      "populated hls group survives the recursive cleanup");
  });

  test("filterDefaults preserves a non-empty array preserved field while still stripping default-equal sibling fields in the same nested group", () => {

    /* Asserts that one nested group keeps what its PROCESS_FIELDS rules keep and nothing else: a single channels group can contain both a non-empty array
     * the rules keep (channels.disabledPredefined) and a default-equal scalar (channels.channelSortField). The output must keep the array and drop the scalar;
     * the parent group survives because the array kept it non-empty.
     */
    const filtered = filterDefaults({

      channels: {

        channelSortField: DEFAULTS.channels.channelSortField,
        disabledPredefined: ["nbc"]
      }
    });

    const channels = (filtered as { channels?: { channelSortField?: string; disabledPredefined?: string[] } }).channels;

    assert.ok(channels, "channels group survives because the preserved array kept it non-empty");
    assert.deepEqual(channels.disabledPredefined, ["nbc"], "preserved array kept");
    assert.equal(channels.channelSortField, undefined, "default-equal sibling stripped");
  });
});

describe("process field hydration", () => {

  /* Every field the process writes is declared once in PROCESS_FIELDS, so the rule that keeps it on disk and the rule that brings it back at boot derive from
   * the same entry: a state field's rules from its default, and a schema field, which has no runtime counterpart, from its predicate alone. The rows below hold
   * the restore half for the auto-discovered host and the setup flag, and the known answers further down hold the save and the restore half for every field.
   */
  test("no PROCESS_FIELDS key is a metadata path, so the metadata loop alone decides every setting", () => {

    const metadataPaths = new Set(Object.values(CONFIG_METADATA).flat().map((setting) => setting.path));

    assert.deepEqual(Object.keys(PROCESS_FIELDS).filter((fieldPath) => metadataPaths.has(fieldPath)), [], "the table holds only paths outside the metadata");
  });

  test("hydrates channelsDvr.host from persisted UserConfig into runtime CONFIG", () => {

    /* channelsDvr.host is discovered by the show-info module, which writes it to the file and to CONFIG through one process write, and its PROCESS_FIELDS
     * state entry keeps it on disk. This test asserts that the same entry brings it back into runtime CONFIG on boot, which is where the
     * show-info module and pretune read the host, so it is known from the first poll rather than after the next discovery.
     */
    const userConfig: UserConfig = { channelsDvr: { host: "192.168.1.50" } };
    const result = mergeConfiguration(userConfig);

    assert.equal(result.channelsDvr.host, "192.168.1.50", "persisted host must hydrate into runtime CONFIG");
    assert.equal(result.channelsDvr.port, DEFAULTS.channelsDvr.port, "untouched fields fall through to defaults");
  });

  test("channels.setupCompleted survives a save and hydrates back into runtime CONFIG, and only its true state is written or restored", () => {

    /* The setup flag is a one-way fact with a false default, so the file carries it only once it is true, and a stored value that is not a boolean leaves the
     * running configuration at the default. One seeded document drives the save rule, which keeps it, and the restore rule, which brings it back.
     */
    const userConfig: UserConfig = { channels: { setupCompleted: true } };

    assert.equal(filterDefaults(userConfig).channels?.setupCompleted, true, "a true flag survives the default filter on save");
    assert.equal(mergeConfiguration(userConfig).channels.setupCompleted, true, "a true flag on disk hydrates into runtime CONFIG");
    assert.equal(filterDefaults({ channels: { setupCompleted: false } }).channels?.setupCompleted, undefined, "the default false is stripped on save");
    assert.equal(mergeConfiguration({ channels: { setupCompleted: "true" as unknown as boolean } }).channels.setupCompleted, false,
      "a value that is not the boolean true leaves runtime CONFIG at the default");
  });

  test("hydration leaves runtime CONFIG at defaults when the disk value fails the restore rule", () => {

    /* Empty strings, undefined values, and other "not meaningful enough" cases must not overwrite the default. This covers the edge where a corrupted or
     * legacy file carries channelsDvr.host: "" - the runtime CONFIG should stay at the default empty string (which downstream callers already treat as "not
     * yet discovered") rather than bringing back the same empty value, because a state field's restore rule never takes the empty string.
     */
    const result = mergeConfiguration({ channelsDvr: { host: "" } });

    assert.equal(result.channelsDvr.host, DEFAULTS.channelsDvr.host, "empty string disk value does not overwrite the default");
  });
});

/* The save and restore rules of every field the process writes, as known answers. For each field and each input, the first answer is whether filterDefaults
 * keeps the key on disk, and the second is the value mergeConfiguration produces at the path, written as a literal, so an input equal to the default and an
 * input that is not brought back are both observable. The answers state the rules as behavior, so they hold however the rules are declared.
 */
describe("the fields the process writes are kept and restored by their known answers", () => {

  const INPUTS: readonly (readonly [ string, unknown ])[] = [

    [ "undefined", undefined ],
    [ "the empty string", "" ],
    [ "\"x\"", "x" ],
    [ "\"name\"", "name" ],
    [ "an empty array", [] ],
    [ "[\"a\"]", ["a"] ],
    [ "true", true ],
    [ "false", false ],
    [ "zero", 0 ]
  ];

  // Each row lists [ kept, merged ] per input, in the order INPUTS lists them.
  const EMPTY_STRING_DEFAULT: readonly (readonly [ boolean, unknown ])[] = [
    [ false, "" ], [ false, "" ], [ true, "x" ], [ true, "name" ], [ false, "" ], [ false, "" ], [ false, "" ], [ false, "" ], [ false, "" ]
  ];

  const EMPTY_ARRAY_DEFAULT: readonly (readonly [ boolean, unknown ])[] = [
    [ false, [] ], [ false, [] ], [ false, [] ], [ false, [] ], [ false, [] ], [ true, ["a"] ], [ false, [] ], [ false, [] ], [ false, [] ]
  ];

  const KNOWN_ANSWERS: Readonly<Record<string, readonly (readonly [ boolean, unknown ])[]>> = {

    "channels.channelSortDirection": [
      [ false, "asc" ], [ true, "asc" ], [ true, "x" ], [ true, "name" ], [ false, "asc" ], [ false, "asc" ], [ false, "asc" ], [ false, "asc" ], [ false, "asc" ]
    ],
    "channels.channelSortField": [
      [ false, "name" ], [ true, "name" ], [ true, "x" ], [ false, "name" ], [ false, "name" ], [ false, "name" ], [ false, "name" ], [ false, "name" ],
      [ false, "name" ]
    ],
    "channels.disabledPredefined": EMPTY_ARRAY_DEFAULT,
    "channels.enabledServices": EMPTY_ARRAY_DEFAULT,
    "channels.setupCompleted": [
      [ false, false ], [ false, false ], [ false, false ], [ false, false ], [ false, false ], [ false, false ], [ true, true ], [ false, false ], [ false, false ]
    ],
    "channels.visibleColumns": EMPTY_ARRAY_DEFAULT,
    "channelsDvr.host": EMPTY_STRING_DEFAULT,
    "hdhr.deviceId": EMPTY_STRING_DEFAULT,
    "logging.debugFilter": EMPTY_STRING_DEFAULT
  };

  // Whether the filtered document holds the key itself, so a kept value is told apart from a key filterDefaults dropped.
  const holdsKey = (document: UserConfig, fieldPath: string): boolean => {

    const parts = fieldPath.split(".");
    const parent = (parts.length === 1) ? document : getNestedValue(document, parts.slice(0, -1).join("."));

    return (typeof parent === "object") && (parent !== null) && Object.hasOwn(parent, parts.at(-1) ?? "");
  };

  for(const [ fieldPath, answers ] of Object.entries(KNOWN_ANSWERS)) {

    test(fieldPath + " is kept on disk and restored at boot as its known answers state", () => {

      assert.equal(answers.length, INPUTS.length, "precondition: " + fieldPath + " states an answer for every input");

      for(const [ index, [ label, value ] ] of INPUTS.entries()) {

        const [ kept, merged ] = answers[index] ?? assert.fail(fieldPath + " states no answer for " + label);
        const input = structuredClone(value);
        const document: UserConfig = {};

        setNestedValue(document as Record<string, unknown>, fieldPath, input);

        const filtered = filterDefaults(document);
        const result = getNestedValue(mergeConfiguration(document), fieldPath);

        assert.equal(holdsKey(filtered, fieldPath), kept, fieldPath + " given " + label + " is " + (kept ? "kept" : "dropped") + " on save");

        if(kept) {

          assert.deepEqual(getNestedValue(filtered, fieldPath), input, fieldPath + " given " + label + " is kept as written");
        }

        assert.deepEqual(result, merged, fieldPath + " given " + label + " merges to its known answer");

        if(Array.isArray(input) && Array.isArray(result)) {

          assert.notEqual(result, input, fieldPath + " given " + label + " merges as a copy, never the parsed file's own array");
        }
      }
    });
  }

  test("schemaVersion is kept as a number and dropped otherwise", () => {

    assert.equal(holdsKey(filterDefaults({ schemaVersion: 0 }), "schemaVersion"), true, "zero is kept");
    assert.equal(holdsKey(filterDefaults({ schemaVersion: 3 }), "schemaVersion"), true, "three is kept");
    assert.equal(holdsKey(filterDefaults({ schemaVersion: "1" as unknown as number }), "schemaVersion"), false, "a numeric string is dropped");
  });

  test("migrationsApplied is kept as a non-empty list and dropped otherwise", () => {

    assert.equal(holdsKey(filterDefaults({ migrationsApplied: ["m"] }), "migrationsApplied"), true, "a non-empty list is kept");
    assert.equal(holdsKey(filterDefaults({ migrationsApplied: [] }), "migrationsApplied"), false, "an empty list is dropped");
    assert.equal(holdsKey(filterDefaults({ migrationsApplied: "m" as unknown as string[] }), "migrationsApplied"), false, "a string is dropped");
  });
});
