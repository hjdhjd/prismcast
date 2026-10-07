/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the CONFIG validation layer. The merge layer (mergeConfiguration) is exercised in userConfig.merge.test.ts; here we focus on
 * the validation gate (validateInteger, validateNumber, validateConfiguration), the boot's capture and HTTP log level corrections through initializeConfiguration
 * on an in-memory store, the per-CONFIG-clone behavior of getDefaults, the parse-error accessor surface, the displayConfiguration startup block, and the debug
 * filter handler the module registers at load, which a save here reaches because this suite resets no handler registry. Tests that mutate CONFIG save and
 * restore the prior state in afterEach so they remain independent of any other suite that touches CONFIG.
 */
import { CONFIG, STARTUP_BOUNDED_SETTINGS, configParseError, configParseErrorMessage, displayConfiguration, getDefaults, initializeConfiguration,
  saveConfiguration, validateConfiguration, validateInteger, validateNumber } from "./index.ts";
import { CONFIG_METADATA, DEFAULTS, getNestedValue, getSettingByPath } from "./userConfig.ts";
import { LOG, getCurrentPattern, initDebugFilter, validateDeviceId } from "../utils/index.ts";
import { SEEDED_DEVICE_ID, makeMemoryConfigStore } from "./index.helpers.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import type { Config } from "../types/index.ts";
import type { LogEntry } from "../utils/logEmitter.ts";
import type { MemoryConfigStore } from "./index.helpers.ts";
import assert from "node:assert/strict";
import { getPresetViewport } from "./presets.ts";
import { initializeDataDir } from "./paths.ts";
import os from "node:os";
import { subscribeToLogs } from "../utils/logEmitter.ts";

describe("validateInteger", () => {

  test("returns null for a valid integer with no bounds", () => {

    assert.equal(validateInteger("X", 5), null);
  });

  test("with no declared minimum the floor is 1, so zero is refused by a message naming that floor", () => {

    assert.equal(validateInteger("X", 0), "X must be at least 1, but it is 0.");
  });

  test("returns an error for a negative integer", () => {

    const err = validateInteger("X", -1);

    assert.match(err ?? "", /must be at least 1/);
  });

  test("returns an error for a non-integer (float)", () => {

    const err = validateInteger("X", 1.5);

    assert.match(err ?? "", /must be an integer/);
  });

  test("returns an error for NaN", () => {

    const err = validateInteger("X", Number.NaN);

    assert.match(err ?? "", /must be an integer/);
  });

  test("the declared minimum is the floor: zero is accepted under a minimum of zero and refused under a minimum of one", () => {

    assert.equal(validateInteger("X", 0, 0), null, "zero is valid where the metadata declares a floor of zero");
    assert.equal(validateInteger("X", 0, 0, 10000), null, "zero is valid under a floor of zero with a ceiling, the shape of recovery.backoffJitter");
    assert.equal(validateInteger("X", 0, 1), "X must be at least 1, but it is 0.", "zero is refused under a floor of one, by a message naming that floor");
    assert.equal(validateInteger("X", -1, 0), "X must be at least 0, but it is -1.", "a value below a floor of zero is refused by a message naming that floor");
  });

  test("a non-integer is refused whatever the declared floor", () => {

    assert.equal(validateInteger("X", 0.5, 0), "X must be an integer, but it is 0.5.");
  });

  test("enforces the minimum bound (inclusive)", () => {

    assert.equal(validateInteger("X", 5, 5), null, "value equal to min is valid");
    assert.match(validateInteger("X", 4, 5) ?? "", /at least 5/);
  });

  test("enforces the maximum bound (inclusive)", () => {

    assert.equal(validateInteger("X", 100, 1, 100), null, "value equal to max is valid");
    assert.match(validateInteger("X", 101, 1, 100) ?? "", /at most 100/);
  });

  test("max-only bound (min undefined) accepts a value within range and rejects values above max", () => {

    /* Asserts that a declared ceiling still bounds a value when no minimum is declared: the floor falls back to 1 and the ceiling is the active gate. A call with
     * an explicit undefined minimum and a numeric maximum reaches that path through the public surface, so checkBounds stays private without test-only hooks.
     */
    assert.equal(validateInteger("X", 5, undefined, 10), null, "value below max-only bound is valid");
    assert.match(validateInteger("X", 11, undefined, 10) ?? "", /at most 10/, "value above max-only bound is rejected");
  });

  test("with no declared bounds, any integer at or above the fallback floor is accepted", () => {

    /* Asserts the unbounded case through the public surface: with neither bound supplied, the only floor is the fallback of 1 and no ceiling applies, so the
     * boundary value and a very large value pass.
     */
    assert.equal(validateInteger("X", 1), null, "valid integer with no bounds returns null (boundary value 1)");
    assert.equal(validateInteger("X", Number.MAX_SAFE_INTEGER), null, "valid integer with no bounds returns null (large value)");
  });

  test("error message includes the invalid value", () => {

    const err = validateInteger("PORT", -7);

    assert.match(err ?? "", /-7/);
  });
});

describe("validateNumber", () => {

  test("accepts a positive float with no bounds", () => {

    assert.equal(validateNumber("X", 0.5), null);
  });

  test("rejects zero when no minimum is declared (must be > 0)", () => {

    assert.match(validateNumber("X", 0) ?? "", /must be a positive number/);
  });

  test("rejects negative values when no minimum is declared", () => {

    assert.match(validateNumber("X", -0.1) ?? "", /must be a positive number/);
  });

  test("rejects NaN whatever the declared floor", () => {

    assert.match(validateNumber("X", Number.NaN) ?? "", /must be a number/);
    assert.match(validateNumber("X", Number.NaN, 0, 5) ?? "", /must be a number/, "NaN is refused even though it compares false against every bound");
  });

  test("the declared minimum is the floor: zero is accepted under a minimum of zero and refused under a minimum of one", () => {

    assert.equal(validateNumber("X", 0, 0), null, "zero is valid where the metadata declares a floor of zero");
    assert.equal(validateNumber("X", 0, 1), "X must be at least 1, but it is 0.", "zero is refused under a floor of one, by a message naming that floor");
  });

  test("enforces minimum bound (inclusive)", () => {

    assert.equal(validateNumber("X", 0.01, 0.01, 5), null);
    assert.match(validateNumber("X", 0.005, 0.01) ?? "", /at least/);
  });

  test("enforces maximum bound (inclusive)", () => {

    assert.equal(validateNumber("X", 5, 0.01, 5), null);
    assert.match(validateNumber("X", 5.1, 0.01, 5) ?? "", /at most/);
  });

  test("max-only bound rejects values above max even when min is undefined", () => {

    /* Mirror of the validateInteger max-only test - the same ceiling check, exercised through the float-tolerant validator instead of the integer one.
     */
    assert.equal(validateNumber("X", 0.5, undefined, 1), null, "value below max-only bound is valid");
    assert.match(validateNumber("X", 1.5, undefined, 1) ?? "", /at most 1/, "value above max-only bound is rejected");
  });
});

describe("getDefaults", () => {

  test("returns a deep-cloned copy of DEFAULTS (so mutations do not affect the singleton)", () => {

    const a = getDefaults();
    const b = getDefaults();

    assert.notEqual(a, b, "two calls produce distinct references");
    assert.notEqual(a.server, b.server, "nested objects are also cloned");
    assert.deepEqual(a, b, "content is identical");
  });

  test("matches DEFAULTS by value", () => {

    assert.deepEqual(getDefaults(), DEFAULTS);
  });
});

describe("validateConfiguration", () => {

  /* Snapshot the entire CONFIG object before each test so any mutation a test makes is rolled back. The suite uses structuredClone to avoid shared references
   * on nested objects.
   */
  let snapshot: Config;

  beforeEach(() => {

    snapshot = structuredClone(CONFIG);
  });

  afterEach(() => {

    /* Restore by reassigning every top-level group on the live CONFIG. We cannot reassign CONFIG itself here because it's a named import, and ES module named
     * and namespace imports are read-only bindings - this file has no way to assign to the imported name at all, only to mutate the object it points to.
     */
    Object.assign(CONFIG.browser, snapshot.browser);
    Object.assign(CONFIG.channels, snapshot.channels);
    Object.assign(CONFIG.hdhr, snapshot.hdhr);
    Object.assign(CONFIG.hls, snapshot.hls);
    Object.assign(CONFIG.logging, snapshot.logging);
    Object.assign(CONFIG.paths, snapshot.paths);
    Object.assign(CONFIG.playback, snapshot.playback);
    Object.assign(CONFIG.recovery, snapshot.recovery);
    Object.assign(CONFIG.server, snapshot.server);
    Object.assign(CONFIG.streaming, snapshot.streaming);
  });

  test("passes for an unmodified default CONFIG", () => {

    assert.doesNotThrow(() => { validateConfiguration(); });
  });

  test("collects multiple errors and reports them all in one throw", () => {

    CONFIG.server.port = 0;
    CONFIG.streaming.videoBitsPerSecond = 99;

    try {

      validateConfiguration();
      assert.fail("validateConfiguration should have thrown");
    } catch(err) {

      assert.ok(err instanceof Error);
      assert.match(err.message, /PORT/);
      assert.match(err.message, /VIDEO_BITRATE/);
    }
  });

  test("throws when port is out of range", () => {

    CONFIG.server.port = 70000;
    assert.throws(() => { validateConfiguration(); }, /PORT/);
  });

  test("throws when stallThreshold is out of range (float validation)", () => {

    CONFIG.playback.stallThreshold = 0.001;
    assert.throws(() => { validateConfiguration(); }, /STALL_THRESHOLD/);
  });

  test("rejects non-absolute chromeDataDir override", () => {

    CONFIG.paths.chromeDataDir = "relative/path";
    assert.throws(() => { validateConfiguration(); }, /chromeDataDir must be an absolute path/);
  });

  test("rejects non-absolute logFile override", () => {

    CONFIG.paths.logFile = "relative/path";
    assert.throws(() => { validateConfiguration(); }, /logFile must be an absolute path/);
  });

  test("error from chromeDataDir validation flags the absolute path requirement", () => {

    CONFIG.paths.chromeDataDir = "./relative";

    try {

      validateConfiguration();
      assert.fail("validateConfiguration should have thrown");
    } catch(err) {

      assert.ok(err instanceof Error);
      assert.match(err.message, /must be an absolute path/);
    }
  });

  test("HDHR port conflict with main server is reported when host is 0.0.0.0", () => {

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.port = CONFIG.server.port;
    CONFIG.server.host = "0.0.0.0";

    assert.throws(() => { validateConfiguration(); }, /conflicts with the main server port/);
  });

  test("HDHR port conflict is not reported when host is loopback (different bind)", () => {

    // Boundary: host !== 0.0.0.0 means the two ports could coexist on different bind addresses, so the conflict guard does not fire.
    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.port = CONFIG.server.port;
    CONFIG.server.host = "127.0.0.1";

    assert.doesNotThrow(() => { validateConfiguration(); });
  });

  test("a bounded setting below its metadata floor is refused, and the error quotes the floor the metadata declares", () => {

    /* The boot must read logging.maxSize's floor from its metadata - a value one below that floor is refused with the floor named - so a startup path with its
     * own smaller number would boot a log too small to be useful while the form refused the same value.
     */
    const floor = getSettingByPath("logging.maxSize")?.min;

    assert.ok(typeof floor === "number", "sanity: the metadata declares a floor for logging.maxSize");

    CONFIG.logging.maxSize = floor - 1;

    assert.throws(() => { validateConfiguration(); }, new RegExp("LOG_MAX_SIZE must be at least " + String(floor)));
  });

  test("an out-of-range hdhr.port is refused only when HDHomeRun emulation is enabled", () => {

    /* The HDHR port check sits inside its own guard rather than in the startup list, so a configuration with HDHomeRun off boots with any hdhr.port value at
     * all - including one no socket could ever bind. Both directions are asserted together because the guard is the whole contract: moving the check into the
     * list would turn a dormant setting into a boot refusal for every operator who left it alone.
     */
    CONFIG.hdhr.enabled = false;
    CONFIG.hdhr.port = 999999;

    assert.doesNotThrow(() => { validateConfiguration(); }, "an out-of-range port is not read while HDHomeRun emulation is off");

    CONFIG.hdhr.enabled = true;

    assert.throws(() => { validateConfiguration(); }, /HDHR_PORT must be at most 65535/, "with emulation on, the same value is refused by name and bound");
  });
});

/* The boot's capture and DeviceID corrections, driven through initializeConfiguration on an in-memory store. Each row boots from the stored file it names on
 * the store double of index.helpers.ts, which normalizes its held file after each mutation as the real store's write hook does, so a row reads the file a write
 * stored. HDHomeRun is enabled by default, so a row that must show no write, or no DeviceID line, seeds a valid id; the other capture rows let the boot's one
 * write carry the generated id as well. Each row starts with no environment variable the merge consults, and the suite restores the environment and boots from
 * an empty file after each row, so CONFIG holds the defaults again for the suites below.
 */
describe("initializeConfiguration - the boot's capture correction", () => {

  const MODE_WARNING = "Native capture mode is unavailable because of a Chrome fMP4 MediaRecorder defect, so FFmpeg capture is in use.";
  const UNRECOGNIZED_WARNING = "The configured capture codecs include identifiers the server does not recognize, so they are ignored.";
  const BASELINE_WARNING = "The configured capture codecs omit the H.264 baseline, so it is restored.";
  const CAPTURE_WARNINGS = new Set<unknown>([ BASELINE_WARNING, MODE_WARNING, UNRECOGNIZED_WARNING ]);
  const CORRECTION_LINE = "The configuration file write carries corrected capture values.";
  const CORRECTION_LINES = new Set<unknown>([CORRECTION_LINE]);
  const DEVICE_ID_GENERATED = "An HDHomeRun DeviceID was generated.";
  const DEVICE_ID_REPLACED = "The configured HDHomeRun DeviceID fails its checksum, so a valid one takes its place.";
  const DEVICE_ID_LINES = new Set<unknown>([ DEVICE_ID_GENERATED, DEVICE_ID_REPLACED ]);
  const WRITE_FAILURE_LINE = "The corrected configuration could not be written, so the file keeps its stored values until the next write.";
  const ORIGINAL_ENV = { ...process.env };

  let store: MemoryConfigStore = makeMemoryConfigStore();

  /**
   * Answers the arguments of every recorded call whose message is one of the given lines, in the order they were logged, so a row compares the whole list and
   * a repeated, missing, or misattributed line fails it.
   * @param calls - The mock's recorded calls.
   * @param messages - The messages to keep.
   * @returns Each kept call's arguments.
   */
  function callsNaming(calls: readonly { readonly arguments: readonly unknown[] }[], messages: ReadonlySet<unknown>): unknown[][] {

    return calls.filter((call) => messages.has(call.arguments[0])).map((call) => [...call.arguments]);
  }

  beforeEach(() => {

    store = makeMemoryConfigStore();

    for(const setting of Object.values(CONFIG_METADATA).flat()) {

      if(setting.envVar) {

        Reflect.deleteProperty(process.env, setting.envVar);
      }
    }
  });

  afterEach(async () => {

    for(const key of Object.keys(process.env)) {

      Reflect.deleteProperty(process.env, key);
    }

    Object.assign(process.env, ORIGINAL_ENV);
    store.file = {};

    await initializeConfiguration(undefined, store);
  });

  test("a stored native mode boots with FFmpeg capture and one warning, and one write stores no capture mode", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { streaming: { captureMode: "native" } };
    await initializeConfiguration(undefined, store);

    assert.equal(CONFIG.streaming.captureMode, "ffmpeg");
    assert.deepEqual(callsNaming(warn.mock.calls, CAPTURE_WARNINGS), [[ MODE_WARNING, { configured: "native", using: "ffmpeg" } ]]);
    assert.equal(store.writes, 1, "the boot wrote the file once");
    assert.equal(store.file.streaming?.captureMode, undefined, "the held file stores no capture mode");
  });

  test("a stored list with an unrecognized codec boots without it and one warning, and one write stores the list in effect", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { streaming: { captureCodecs: [ "h264", "av1" ] } };
    await initializeConfiguration(undefined, store);

    assert.deepEqual(CONFIG.streaming.captureCodecs, ["h264"]);
    assert.deepEqual(callsNaming(warn.mock.calls, CAPTURE_WARNINGS), [[ UNRECOGNIZED_WARNING, { configured: [ "h264", "av1" ], ignored: ["av1"], using: ["h264"] } ]]);
    assert.equal(store.writes, 1, "the boot wrote the file once");
    assert.deepEqual(store.file.streaming?.captureCodecs, ["h264"], "the held file stores the list in effect, which differs from the default");
  });

  test("a stored list without the baseline boots with it restored and one warning, and one write stores no codec list", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { streaming: { captureCodecs: ["hevc"] } };
    await initializeConfiguration(undefined, store);

    assert.deepEqual(CONFIG.streaming.captureCodecs, [ "h264", "hevc" ]);
    assert.deepEqual(callsNaming(warn.mock.calls, CAPTURE_WARNINGS), [[ BASELINE_WARNING, { configured: ["hevc"], using: [ "h264", "hevc" ] } ]]);
    assert.equal(store.writes, 1, "the boot wrote the file once");
    assert.equal(store.file.streaming?.captureCodecs, undefined, "the held file stores no codec list, because the list in effect holds the default's members");
  });

  test("a file a write before the boot already corrected, as a startup migration's write does, boots with no capture warning, info line or write",
    async (t) => {

      const warn = t.mock.method(LOG, "warn", () => undefined);
      const info = t.mock.method(LOG, "info", () => undefined);

      // The write a startup migration makes before the boot reads the file passes the store's write hook like every other write.
      store.file = { hdhr: { deviceId: SEEDED_DEVICE_ID }, streaming: { captureMode: "native" } };
      await store.mutateConfigThen(() => {

        // A migration's write, which changes nothing the capture correction reads, and whose follow-up resolves at once.
        return async (): Promise<void> => Promise.resolve();
      });

      assert.deepEqual(callsNaming(info.mock.calls, CORRECTION_LINES), [[ CORRECTION_LINE, { corrections: [{ configured: "native", kind: "mode", using: "ffmpeg" }] } ]],
        "precondition: the write before the boot stored the correction and logged it");
      assert.deepEqual(store.file, { hdhr: { deviceId: SEEDED_DEVICE_ID } }, "precondition: the held file needs no further correction");

      info.mock.resetCalls();
      store.writes = 0;

      await initializeConfiguration(undefined, store);

      assert.deepEqual(callsNaming(warn.mock.calls, CAPTURE_WARNINGS), [], "no capture warning");
      assert.deepEqual(callsNaming(info.mock.calls, CORRECTION_LINES), [], "no info line");
      assert.equal(store.writes, 0, "no boot write");
    });

  test("a stored string at the codec list boots with the default list, no warning, no throw, and no write", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { hdhr: { deviceId: SEEDED_DEVICE_ID }, streaming: { captureCodecs: "h264" as unknown as string[] } };

    await assert.doesNotReject(initializeConfiguration(undefined, store));
    assert.deepEqual(CONFIG.streaming.captureCodecs, [ "h264", "hevc" ]);
    assert.deepEqual(callsNaming(warn.mock.calls, CAPTURE_WARNINGS), []);
    assert.equal(store.writes, 0);
  });

  test("a codec list from the environment is corrected in the candidate with its warning, and the store receives no write", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { hdhr: { deviceId: SEEDED_DEVICE_ID } };
    process.env["CAPTURE_CODECS"] = "hevc";
    await initializeConfiguration(undefined, store);

    assert.deepEqual(CONFIG.streaming.captureCodecs, [ "h264", "hevc" ]);
    assert.deepEqual(callsNaming(warn.mock.calls, CAPTURE_WARNINGS), [[ BASELINE_WARNING, { configured: ["hevc"], using: [ "h264", "hevc" ] } ]]);
    assert.equal(store.writes, 0, "a value the environment supplies is never written");
  });

  test("an environment codec list beside a stored native mode leaves one write that stores neither, and the info line names the mode alone", async (t) => {

    t.mock.method(LOG, "warn", () => undefined);

    const info = t.mock.method(LOG, "info", () => undefined);

    process.env["CAPTURE_CODECS"] = "hevc";
    store.file = { streaming: { captureMode: "native" } };
    await initializeConfiguration(undefined, store);

    assert.equal(store.writes, 1, "the stored mode asks for one write");
    assert.equal(store.file.streaming?.captureMode, undefined, "the held file stores no capture mode");
    assert.equal(store.file.streaming?.captureCodecs, undefined, "the held file stores no codec list, because the environment's is never written");
    assert.deepEqual(callsNaming(info.mock.calls, CORRECTION_LINES), [[ CORRECTION_LINE, { corrections: [{ configured: "native", kind: "mode", using: "ffmpeg" }] } ]],
      "the info line names the stored correction and nothing the environment supplied");
  });

  test("a file storing capture values the server can capture with causes no write", async () => {

    store.file = { hdhr: { deviceId: SEEDED_DEVICE_ID }, streaming: { captureCodecs: ["h264"], captureMode: "ffmpeg" } };
    await initializeConfiguration(undefined, store);

    assert.equal(store.writes, 0);
    assert.deepEqual(store.file, { hdhr: { deviceId: SEEDED_DEVICE_ID }, streaming: { captureCodecs: ["h264"], captureMode: "ffmpeg" } }, "the held file is as stored");
  });

  test("a stored native mode beside a hard error causes no write, because the boot never writes a configuration it will refuse", async (t) => {

    t.mock.method(LOG, "warn", () => undefined);

    store.file = { paths: { logFile: "relative/prismcast.log" }, streaming: { captureMode: "native" } };
    await initializeConfiguration(undefined, store);

    assert.throws(() => { validateConfiguration(); }, /paths\.logFile must be an absolute path/, "precondition: the configuration carries a hard error");
    assert.equal(store.writes, 0, "nothing was written");
    assert.equal(store.file.streaming?.captureMode, "native", "the stored mode waits for the next write");
  });

  test("a stored DeviceID that fails its checksum boots with a valid id in its place, one write storing it, and the warning once", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { hdhr: { deviceId: "10000000" } };
    await initializeConfiguration(undefined, store);

    assert.ok(validateDeviceId(CONFIG.hdhr.deviceId), "the running DeviceID passes its checksum");
    assert.notEqual(CONFIG.hdhr.deviceId, "10000000", "the stored id is replaced");
    assert.equal(store.writes, 1, "the boot wrote the file once");
    assert.equal(store.file.hdhr?.deviceId, CONFIG.hdhr.deviceId, "the held file carries the running DeviceID");
    assert.deepEqual(callsNaming(warn.mock.calls, DEVICE_ID_LINES), [[ DEVICE_ID_REPLACED, { configured: "10000000", using: CONFIG.hdhr.deviceId.toUpperCase() } ]]);
  });

  test("a stored lower-case DeviceID that fails its checksum is logged upper-cased, as configured and as in use", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);

    store.file = { hdhr: { deviceId: "abcdef12" } };
    await initializeConfiguration(undefined, store);

    assert.deepEqual(callsNaming(warn.mock.calls, DEVICE_ID_LINES), [[ DEVICE_ID_REPLACED, { configured: "ABCDEF12", using: CONFIG.hdhr.deviceId.toUpperCase() } ]]);
  });

  test("a file with no hdhr category boots with a generated DeviceID, one write storing it, and the info line once", async (t) => {

    const info = t.mock.method(LOG, "info", () => undefined);

    await initializeConfiguration(undefined, store);

    assert.ok(validateDeviceId(CONFIG.hdhr.deviceId), "the generated DeviceID passes its checksum");
    assert.equal(store.writes, 1, "the boot wrote the file once");
    assert.equal(store.file.hdhr?.deviceId, CONFIG.hdhr.deviceId, "the held file carries the running DeviceID");
    assert.deepEqual(callsNaming(info.mock.calls, DEVICE_ID_LINES), [[ DEVICE_ID_GENERATED, { deviceId: CONFIG.hdhr.deviceId.toUpperCase() } ]]);
  });

  test("a stored DeviceID that passes its checksum boots with no write and no DeviceID line", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);
    const info = t.mock.method(LOG, "info", () => undefined);

    store.file = { hdhr: { deviceId: SEEDED_DEVICE_ID } };
    await initializeConfiguration(undefined, store);

    assert.equal(CONFIG.hdhr.deviceId, SEEDED_DEVICE_ID);
    assert.equal(store.writes, 0, "no boot write");
    assert.deepEqual(callsNaming([ ...warn.mock.calls, ...info.mock.calls ], DEVICE_ID_LINES), [], "no DeviceID line");
  });

  test("a stored native mode beside a stored DeviceID that fails its checksum leaves one write carrying the capture and the DeviceID corrections", async (t) => {

    t.mock.method(LOG, "warn", () => undefined);
    t.mock.method(LOG, "info", () => undefined);

    store.file = { hdhr: { deviceId: "10000000" }, streaming: { captureMode: "native" } };
    await initializeConfiguration(undefined, store);

    assert.equal(store.writes, 1, "one write");
    assert.equal(store.file.streaming?.captureMode, undefined, "the held file stores no capture mode");
    assert.ok(validateDeviceId(CONFIG.hdhr.deviceId), "the running DeviceID passes its checksum");
    assert.equal(store.file.hdhr?.deviceId, CONFIG.hdhr.deviceId, "the held file carries the running DeviceID");
  });

  test("a stored native mode beside a DeviceID that passes its checksum leaves one write, which stores no capture mode and keeps the stored id", async (t) => {

    // The emulation is enabled by default, so a file holding no valid id asks for the boot write through its DeviceID correction as well. The stored id here
    // passes its checksum, so the stored capture mode alone asks for the write.
    const warn = t.mock.method(LOG, "warn", () => undefined);
    const info = t.mock.method(LOG, "info", () => undefined);

    store.file = { hdhr: { deviceId: SEEDED_DEVICE_ID }, streaming: { captureMode: "native" } };
    await initializeConfiguration(undefined, store);

    assert.deepEqual(callsNaming([ ...warn.mock.calls, ...info.mock.calls ], DEVICE_ID_LINES), [], "precondition: the stored DeviceID needs no correction");
    assert.equal(store.writes, 1, "the boot wrote the file once");
    assert.equal(store.file.streaming?.captureMode, undefined, "the held file stores no capture mode");
    assert.equal(store.file.hdhr?.deviceId, SEEDED_DEVICE_ID, "the held file keeps the stored DeviceID");
  });

  test("a stored DeviceID that fails its checksum beside a hard error causes no write and runs a valid id", async (t) => {

    t.mock.method(LOG, "warn", () => undefined);

    store.file = { hdhr: { deviceId: "10000000" }, paths: { logFile: "relative/prismcast.log" } };
    await initializeConfiguration(undefined, store);

    assert.throws(() => { validateConfiguration(); }, /paths\.logFile must be an absolute path/, "precondition: the configuration carries a hard error");
    assert.equal(store.writes, 0, "nothing was written");
    assert.equal(store.file.hdhr?.deviceId, "10000000", "the stored id waits for the next write");
    assert.ok(validateDeviceId(CONFIG.hdhr.deviceId), "the running DeviceID passes its checksum");
  });

  test("a disabled emulation boots with no write for the DeviceID and leaves the id as stored, whether none or one that fails its checksum", async (t) => {

    t.mock.method(LOG, "warn", () => undefined);

    store.file = { hdhr: { enabled: false } };
    await initializeConfiguration(undefined, store);

    assert.equal(store.writes, 0, "no write for a missing DeviceID");
    assert.equal(store.file.hdhr?.deviceId, undefined, "the file stores no DeviceID");
    assert.equal(CONFIG.hdhr.deviceId, "", "the running configuration holds none");

    store.file = { hdhr: { deviceId: "10000000", enabled: false } };
    await initializeConfiguration(undefined, store);

    assert.equal(store.writes, 0, "no write for a DeviceID that fails its checksum");
    assert.equal(store.file.hdhr?.deviceId, "10000000", "the file keeps the stored id");
    assert.equal(CONFIG.hdhr.deviceId, "10000000", "the running configuration holds the stored id");
  });

  test("a boot on a file the store could not read attempts no write, logs no could-not-be-written line, and runs a valid DeviceID", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);
    const mutate = t.mock.method(store, "mutateConfigThen");

    store.armedFailure = "read";
    await initializeConfiguration(undefined, store);

    assert.equal(mutate.mock.callCount(), 0, "the boot attempted no write");
    assert.equal(warn.mock.calls.filter((call) => call.arguments[0] === WRITE_FAILURE_LINE).length, 0, "no could-not-be-written line");
    assert.ok(validateDeviceId(CONFIG.hdhr.deviceId), "the running DeviceID passes its checksum");
  });

  test("a boot on a file the store could not parse attempts no write, logs no could-not-be-written line, and runs a valid DeviceID", async (t) => {

    // The double counts no write on an armed failure, so the row counts the calls the boot makes to the store's write member instead.
    const warn = t.mock.method(LOG, "warn", () => undefined);
    const mutate = t.mock.method(store, "mutateConfigThen");

    store.armedFailure = "parse";
    await initializeConfiguration(undefined, store);

    assert.equal(mutate.mock.callCount(), 0, "the boot attempted no write");
    assert.equal(warn.mock.calls.filter((call) => call.arguments[0] === WRITE_FAILURE_LINE).length, 0, "no could-not-be-written line");
    assert.ok(validateDeviceId(CONFIG.hdhr.deviceId), "the running DeviceID passes its checksum");
  });

  test("a boot write the store refuses leaves the generated DeviceID running and logs the could-not-be-written line once", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => undefined);
    const mutate = t.mock.method(store, "mutateConfigThen");

    t.mock.method(LOG, "info", () => undefined);
    store.armedFailure = "write";
    await initializeConfiguration(undefined, store);

    assert.equal(mutate.mock.callCount(), 1, "the boot attempted its one write");
    assert.equal(store.writes, 0, "the store refused the write");
    assert.equal(store.file.hdhr, undefined, "the held file keeps nothing of the refused write");
    assert.ok(validateDeviceId(CONFIG.hdhr.deviceId), "the generated DeviceID keeps running");
    assert.equal(warn.mock.calls.filter((call) => call.arguments[0] === WRITE_FAILURE_LINE).length, 1, "the could-not-be-written line, once");
  });

  test("HDHR_ENABLED turning the emulation on over a file that turns it off generates a DeviceID, written once", async (t) => {

    t.mock.method(LOG, "info", () => undefined);

    process.env["HDHR_ENABLED"] = "true";
    store.file = { hdhr: { enabled: false } };
    await initializeConfiguration(undefined, store);

    assert.ok(validateDeviceId(CONFIG.hdhr.deviceId), "the generated DeviceID passes its checksum");
    assert.equal(store.writes, 1, "the boot wrote the file once");
    assert.equal(store.file.hdhr?.deviceId, CONFIG.hdhr.deviceId, "the held file carries the running DeviceID");
  });

  test("HDHR_ENABLED turning the emulation off over a file with no hdhr category causes no write", async () => {

    process.env["HDHR_ENABLED"] = "false";
    await initializeConfiguration(undefined, store);

    assert.equal(store.writes, 0, "no boot write");
    assert.equal(CONFIG.hdhr.deviceId, "", "the running configuration holds no DeviceID");
  });

  test("a monitor interval below its floor refuses the boot with a hard error naming MONITOR_INTERVAL", async () => {

    process.env["MONITOR_INTERVAL"] = "499";
    await initializeConfiguration(undefined, store);

    assert.throws(() => { validateConfiguration(); }, /MONITOR_INTERVAL must be at least 500, but it is 499\./);
  });

  test("a stale page cleanup interval below its floor refuses the boot with a hard error naming STALE_PAGE_CLEANUP_INTERVAL", async () => {

    process.env["STALE_PAGE_CLEANUP_INTERVAL"] = "9999";
    await initializeConfiguration(undefined, store);

    assert.throws(() => { validateConfiguration(); }, /STALE_PAGE_CLEANUP_INTERVAL must be at least 10000, but it is 9999\./);
  });
});

describe("initializeConfiguration - the HTTP log level correction", () => {

  test("an HTTP log level from the environment the server does not recognize boots as all, with one warning", async (t) => {

    const LEVEL_WARNING = "The configured HTTP log level is not one the server recognizes, so every request is logged.";
    const originalLevel = process.env["HTTP_LOG_LEVEL"];
    const warn = t.mock.method(LOG, "warn", () => undefined);

    // The row restores the variable and boots from an empty file afterward, so CONFIG holds the defaults again for the suites below.
    t.after(async () => {

      if(originalLevel === undefined) {

        Reflect.deleteProperty(process.env, "HTTP_LOG_LEVEL");
      } else {

        process.env["HTTP_LOG_LEVEL"] = originalLevel;
      }

      await initializeConfiguration(undefined, makeMemoryConfigStore());
    });

    process.env["HTTP_LOG_LEVEL"] = "verbose";
    await initializeConfiguration(undefined, makeMemoryConfigStore());

    assert.equal(CONFIG.logging.httpLogLevel, "all");
    assert.deepEqual(warn.mock.calls.filter((call) => call.arguments[0] === LEVEL_WARNING).map((call) => [...call.arguments]),
      [[ LEVEL_WARNING, { configured: "verbose", using: "all" } ]]);
  });
});

describe("STARTUP_BOUNDED_SETTINGS", () => {

  test("every startup-validated path resolves to a metadata entry carrying a floor, a ceiling, and an environment variable name", () => {

    /* The drift guard. The startup list names paths; CONFIG_METADATA supplies the bounds and the name each error reports. A path renamed or a bound removed on
     * the metadata side would leave the boot silently unable to validate a value it is supposed to refuse, so every entry is checked for each piece the helper
     * reads. hdhr.port is checked alongside the list because its conditional check reads the same pieces through the same helper.
     */
    for(const settingPath of [ ...STARTUP_BOUNDED_SETTINGS, "hdhr.port" ]) {

      const setting = getSettingByPath(settingPath);

      assert.ok(setting, settingPath + " resolves to a CONFIG_METADATA entry");
      assert.equal(typeof setting.min, "number", settingPath + " declares a minimum");
      assert.equal(typeof setting.max, "number", settingPath + " declares a maximum");
      assert.equal(typeof setting.envVar, "string", settingPath + " declares an environment variable name to report errors by");
    }
  });

  test("every startup-validated setting's default value sits inside the bounds its metadata declares", () => {

    // A default outside its own bounds would refuse to boot an unconfigured server, so every default is held inside its metadata bounds.
    for(const settingPath of [ ...STARTUP_BOUNDED_SETTINGS, "hdhr.port" ]) {

      const setting = getSettingByPath(settingPath);
      const value = getNestedValue(DEFAULTS, settingPath);

      assert.ok((typeof setting?.min === "number") && (typeof setting.max === "number"), settingPath + " declares both bounds");
      assert.ok(typeof value === "number", settingPath + " has a numeric default");
      assert.ok(value >= setting.min, settingPath + " default is at or above its metadata floor");
      assert.ok(value <= setting.max, settingPath + " default is at or below its metadata ceiling");
    }
  });
});

describe("validator messages", () => {

  test("every refusal the validators produce is a complete sentence ending in a period, so a save can join them into one reason", () => {

    const messages = [ validateInteger("PORT", 1.5), validateInteger("PORT", Number.NaN), validateInteger("PORT", 0), validateInteger("PORT", 4, 5),
      validateInteger("PORT", -1, 0), validateInteger("PORT", 101, 1, 100), validateNumber("X", Number.NaN), validateNumber("X", 0), validateNumber("X", 0.005, 0.01),
      validateNumber("X", 5.1, 0.01, 5) ];

    assert.equal(messages.length, 10, "precondition: every validator branch is represented");

    for(const message of messages) {

      assert.match(message ?? "", /^[A-Z_]+ must be .+, but it is (-?[\d.]+|NaN)\.$/, "each refusal names the value it refused and ends in a period");
    }
  });
});

describe("displayConfiguration", () => {

  /* The function emits a startup block through displayLine / printConfigRow (the structured-display escape hatch). That path routes through the same SSE emitter
   * every log line does, so we capture every emitted entry via subscribeToLogs and assert against the emission stream - that decouples the test from which
   * internal API the function uses (LOG.info vs displayLine) and asserts the actual observable output instead. The LOG.warn spy stays in place to assert the block
   * is purely informational: the configuration it reports is the configuration that will be used, so there is nothing for it to warn about.
   */
  let captured: LogEntry[];
  let unsubscribe: () => void;
  let warnSpy: ReturnType<typeof mock.method>;

  beforeEach(() => {

    /* displayConfiguration calls getConfigFilePath() and getChromeDataDir() which both require initializeDataDir() to have been called. We point at an
     * os.tmpdir() so we never accidentally read or write the real ~/.prismcast directory; the function is read-only against the data dir (it only formats
     * paths into log strings) so no cleanup is needed.
     */
    initializeDataDir(os.tmpdir());

    captured = [];
    unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });

    warnSpy = mock.method(LOG, "warn", () => undefined);
  });

  afterEach(() => {

    unsubscribe();
    warnSpy.mock.restore();
  });

  test("emits informational lines covering port, preset, capture, and HDHR state, and warns about none of them", () => {

    /* We do not lock specific message strings (they are operator formatting) but we do verify the function emits the documented information categories - any
     * future refactor that drops a line will fail this test. The warn count is part of the contract: every row states a setting the run will actually use, so
     * none of them is a condition to raise.
     */
    displayConfiguration();

    const messages = captured.map((entry) => entry.message);

    assert.ok(messages.some((m) => m.includes("Server port")), "server port line must be emitted");
    assert.ok(messages.some((m) => m.includes("Quality preset")), "quality preset line must be emitted");
    assert.ok(messages.some((m) => m.includes("Capture codecs")), "capture codecs line must be emitted");
    assert.ok(messages.some((m) => m.includes("HDHomeRun emulation")), "HDHR line must be emitted");
    assert.equal(warnSpy.mock.calls.length, 0, "the startup block raises no warnings");
  });

  test("states the configured preset with the dimensions every page will render at", () => {

    /* The preset row is the operator's confirmation of the capture surface, so it carries the dimensions rather than the id alone. Those dimensions come from
     * the same getter the browser launches with, which is what makes the row a true statement about what capture will produce rather than a second opinion.
     */
    const viewport = getPresetViewport(CONFIG);
    const expected = "Quality preset: " + CONFIG.streaming.qualityPreset + " (" + String(viewport.width) + "\u00d7" + String(viewport.height) + ")";

    displayConfiguration();

    const presetRow = captured.map((entry) => entry.message).find((m) => m.includes("Quality preset"));

    assert.equal(presetRow, "  " + expected, "the preset row names the preset and its dimensions");
    assert.equal(presetRow.includes("limited to"), false, "no display-driven qualifier appears in the row");
  });

  test("startup block lines are emitted without trailing periods (tabular display, not sentences)", () => {

    /* The block goes through displayLine which deliberately bypasses the logger's sentence-normalization contract. This locks the no-trailing-period behavior
     * so a regression that routes the rows through LOG.info, adding trailing periods, fails here.
     */
    displayConfiguration();

    const rowMessages = captured.map((entry) => entry.message).filter((m) => m.startsWith("  "));

    assert.ok(rowMessages.length >= 8, "the startup block emits at least eight indented rows");

    for(const row of rowMessages) {

      assert.equal(row.endsWith("."), false, "tabular row should NOT end with a period: " + row);
    }
  });

});

describe("configParseError exported state", () => {

  /* The two `let` exports (configParseError, configParseErrorMessage) are reassigned by initializeConfiguration on every load and cleared by every save.
   * Tests that reach either function would leak into this assertion, so we only assert the type contract here - the values themselves are produced by the
   * persistence layer and covered through the integration tier where load failures are exercised end-to-end.
   */
  test("module exports the parse-error pair with the documented types", () => {

    assert.equal(typeof configParseError, "boolean", "configParseError is a boolean (default false)");

    /* configParseErrorMessage is exported as string | undefined; we assert the runtime shape via typeof. The compile-time check is sufficient on its own, but
     * runtime assertion documents the public contract for readers of this test.
     */
    const messageType = typeof configParseErrorMessage;

    assert.equal((messageType === "string") || (messageType === "undefined"), true, "configParseErrorMessage is string or undefined at runtime");
  });
});

describe("saveConfiguration - the debug filter handler the module registers at load", () => {

  let store: MemoryConfigStore = makeMemoryConfigStore();

  // The runtime filter is emptied before the boot and again after the row, so no environment or command-line source owns it when the boot measures that.
  beforeEach(() => {

    initDebugFilter("");
    store = makeMemoryConfigStore({ hdhr: { deviceId: SEEDED_DEVICE_ID } });
  });

  afterEach(async () => {

    initDebugFilter("");
    store.file = { hdhr: { deviceId: SEEDED_DEVICE_ID } };

    await initializeConfiguration(undefined, store);
  });

  test("a save that changes the debug filter applies it to the runtime through the handler the module registered at load", async () => {

    await initializeConfiguration(undefined, store);
    await saveConfiguration((current) => { current.logging = { debugFilter: "tuning:hulu,recovery" }; }, store);

    assert.equal(getCurrentPattern(), "tuning:hulu,recovery", "the runtime filter follows the saved filter");
  });
});
