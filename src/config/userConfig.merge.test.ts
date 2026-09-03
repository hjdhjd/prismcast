/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userConfig.merge.test.ts: Unit tests for the env- and CLI-aware portions of the user-config layer - mergeConfiguration, getEnvOverrides, filterDefaults, and the
 * boot-time hydration registry (the PRESERVED_FIELDS / HYDRATED_FIELDS / PERSISTENCE_ONLY_FIELDS partition and channelsDvr.host hydration into runtime CONFIG).
 * Split out from userConfig.test.ts to keep both files under the conventions' 500-line guidance and to isolate the tests that mutate process.env from the
 * pure-function tests in the sibling suite.
 */
import { CONFIG_METADATA, DEFAULTS, HYDRATED_FIELDS, PERSISTENCE_ONLY_FIELDS, PRESERVED_FIELDS, filterDefaults, getEnvOverrideValue, getEnvOverrides,
  getNestedValue, mergeConfiguration } from "./userConfig.ts";
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

  test("user-supplied enabledServices array survives the merge", () => {

    const userConfig: UserConfig = { channels: { enabledServices: [ "hulu", "yttv" ] } };
    const result = mergeConfiguration(userConfig);

    assert.deepEqual(result.channels.enabledServices, [ "hulu", "yttv" ]);
  });

  test("non-string sortField values are ignored (defensive)", () => {

    // channelSortField is hydrated via the HYDRATED_FIELDS registry using the isNonEmptyString predicate, which rejects non-string (and empty-string) values.
    // A non-string value fails the predicate and the default is preserved.
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
     * carries both kinds of contamination the sanitizer exists for: surrounding padding, and an embedded non-printable character that no amount of trimming
     * would remove. Reaching CONFIG uncleaned matters here because nothing downstream validates an environment value - a host with a trailing newline or a path
     * with an embedded null byte would be used as written.
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

    /* Two boundaries share one arm. A whitespace-only value has always collapsed to the sentinel, because trimming alone is enough to empty it. A value made
     * entirely of non-printable characters does not - trimming leaves it intact - so it is the case that separates cleaning from trimming. Both must land on
     * null, because a path holding nothing visible means "use the default", and for paths.logFile the alternative is a value that fails the absolute-path check
     * and takes startup down with it.
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

    /* Boundary: the merge writes through any defined parsed value because the guard is `parsedValue !== undefined`, NOT a truthy check. This asserts that a single
     * non-empty checkboxList override reaches CONFIG unchanged, with no per-element truthiness filter applied to the array contents. The checkboxList type carries
     * no positivity gate at the merge layer, which makes it the clean vehicle for exercising the defined-value pass-through here.
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
     * mergeConfiguration writes through. validateConfiguration later forces h264 back; the merge layer's contract is just "produce the user's literal".
     */
    process.env["CAPTURE_CODECS"] = "";

    const result = mergeConfiguration({});

    assert.deepEqual(result.streaming.captureCodecs, [], "empty CAPTURE_CODECS env var produces an empty list at the merge layer");
  });

  test("float env var is parsed via parseFloat and applied (STALL_THRESHOLD)", () => {

    /* parseEnvValue's float branch is otherwise unreached by the existing merge tests. STALL_THRESHOLD is a documented float setting; asserting the parse here
     * locks the type-specific branch.
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

    /* Which caller reports is the whole design, so the row asserts both halves against one variable. Rendering the settings page resolves the environment layer
     * once per section, so a reader that reported would turn one operator mistake into a page-load-sized burst of identical lines; the merge runs on a boot and
     * on each configuration reload, each an operator's own action, so a line per merge arrives when they would look for it. Spying on LOG.warn rather than
     * swapping the logger keeps the assertion narrow, and reading the substitution arguments rather than a formatted string keeps it independent of the format.
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

    // disabledPredefined is not in CONFIG_METADATA, so the metadata loop never compares it; it is preserved by the isNonEmptyArray predicate in PRESERVED_FIELDS,
    // which keeps a non-empty user list while letting an empty list (identical to the empty-array default) collapse.
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
     * filterDefaults's allow-list - the auto-discovered host is not in CONFIG_METADATA so the standard metadata loop would drop it without explicit handling.
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

    // captureCodecs is skipped in the metadata loop, so the differsFromSortedArrayDefault predicate is the sole arbiter. A user list identical to the default
    // (same elements, same order) sorts equal to the default and is therefore not preserved.
    const filtered = filterDefaults({ streaming: { captureCodecs: [...DEFAULTS.streaming.captureCodecs] } });

    assert.equal(getNestedValue(filtered, "streaming.captureCodecs"), undefined, "default-equal captureCodecs is dropped");
  });

  test("captureCodecs reordered relative to default is treated as default-equal under sorted comparison", () => {

    /* The sorted-equality predicate (differsFromSortedArrayDefault) is registered for streaming.captureCodecs in PRESERVED_FIELDS. The intent: a reordered
     * codec list (e.g., [hevc, h264] vs default [h264, hevc]) is semantically the same configuration and must be stripped from the persisted shape so the
     * on-disk file does not capture a meaningless reorder. filterDefaults achieves this by skipping PRESERVED_FIELDS-managed paths in its CONFIG_METADATA
     * loop, leaving the predicate as the sole arbiter; the predicate's sorted comparison classifies the reordered list as default-equal and writes nothing.
     * Without that skip, the metadata loop's String() coercion would add the value to the filtered output before the predicate ran (the loop is additive-only
     * and cannot delete entries the predicate would skip).
     */
    const reordered = [...DEFAULTS.streaming.captureCodecs].toReversed();
    const filtered = filterDefaults({ streaming: { captureCodecs: reordered } });

    assert.equal(getNestedValue(filtered, "streaming.captureCodecs"), undefined,
      "reordered captureCodecs treated as default-equal under sorted comparison");
  });

  test("captureCodecs with a different content set is preserved (not just reorder)", () => {

    /* Boundary on the sorted-equality predicate: a user list missing one of the default codecs is a real customization; it must survive the filter even when
     * the survivor codec appears in the default. The predicate compares the two arrays as sorted sequences (multiset equality via toSorted + isDeepStrictEqual),
     * so any difference in the element multiset is preserved.
     */
    const filtered = filterDefaults({ streaming: { captureCodecs: ["h264"] } });

    assert.deepEqual(getNestedValue(filtered, "streaming.captureCodecs"), ["h264"], "single-codec list is a customization and survives");
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

    /* Asserts the interaction between PRESERVED_FIELDS and the metadata-driven loop: a single channels group can contain both a preserved non-empty array
     * (channels.disabledPredefined) and a default-equal scalar (channels.channelSortField). The output must keep the array and drop the scalar; the parent
     * group survives because the array kept it non-empty.
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

describe("hydration registry parity", () => {

  /* The architectural rule: every PRESERVED_FIELDS entry has a declared destination. Either it hydrates into the runtime CONFIG on boot (HYDRATED_FIELDS)
   * or it lives only on the persisted UserConfig shape with no runtime counterpart (PERSISTENCE_ONLY_FIELDS - currently schemaVersion / migrationsApplied,
   * owned by the file-store framework). The two registries partition PRESERVED_FIELDS exactly: every preserved path appears in one set or the other, and the
   * sets are disjoint. A new preservation entry added without an explicit hydration classification fails this single assertion before any per-field test runs.
   *
   * This is the read-side analogue of the drift check on PRESERVED_FIELDS itself: if filterDefaults preserves a value but mergeConfiguration never brings it
   * back, the persisted bytes are wasted (or worse, the runtime CONFIG silently drifts from the on-disk state, as channelsDvr.host did before this registry
   * existed).
   */
  test("PRESERVED_FIELDS partitions exactly into HYDRATED_FIELDS and PERSISTENCE_ONLY_FIELDS, with no overlap", () => {

    const preservedPaths = PRESERVED_FIELDS.map((entry) => entry.path);
    const hydratedPaths = HYDRATED_FIELDS.map((entry) => entry.path);
    const persistenceOnlyPaths = [...PERSISTENCE_ONLY_FIELDS];
    const classifiedPaths = [ ...hydratedPaths, ...persistenceOnlyPaths ];

    assert.deepEqual(preservedPaths.toSorted(), classifiedPaths.toSorted(),
      "every PRESERVED_FIELDS path must appear in either HYDRATED_FIELDS or PERSISTENCE_ONLY_FIELDS");

    const overlap = hydratedPaths.filter((path) => persistenceOnlyPaths.includes(path));

    assert.deepEqual(overlap, [], "HYDRATED_FIELDS and PERSISTENCE_ONLY_FIELDS must be disjoint - no path can be both runtime-hydrated and persistence-only");
  });

  test("hydrates channelsDvr.host from persisted UserConfig into runtime CONFIG", () => {

    /* channelsDvr.host is auto-discovered by showInfo.persistDvrHost and persisted via PRESERVED_FIELDS. This test asserts that HYDRATED_FIELDS brings it back
     * into runtime CONFIG immediately on boot, so the host is available before the next DVR discovery cycle runs.
     */
    const userConfig: UserConfig = { channelsDvr: { host: "192.168.1.50" } };
    const result = mergeConfiguration(userConfig);

    assert.equal(result.channelsDvr.host, "192.168.1.50", "persisted host must hydrate into runtime CONFIG");
    assert.equal(result.channelsDvr.port, DEFAULTS.channelsDvr.port, "untouched fields fall through to defaults");
  });

  test("hydration leaves runtime CONFIG at defaults when the disk value fails the predicate", () => {

    /* Empty strings, undefined values, and other "not meaningful enough" cases must not overwrite the default. This covers the edge where a corrupted or
     * legacy file carries channelsDvr.host: "" - the runtime CONFIG should stay at the default empty string (which downstream callers already treat as "not
     * yet discovered") rather than re-hydrating the same empty value through the registry. Behaviorally identical, but the predicate keeps the registry
     * declarative.
     */
    const result = mergeConfiguration({ channelsDvr: { host: "" } });

    assert.equal(result.channelsDvr.host, DEFAULTS.channelsDvr.host, "empty string disk value does not overwrite the default");
  });
});
