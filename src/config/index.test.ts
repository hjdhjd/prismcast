/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the CONFIG validation layer. The merge layer (mergeConfiguration) is exercised in userConfig.merge.test.ts; here we focus on
 * the validation gate (validatePositiveInt, validatePositiveNumber, validateConfiguration), the per-CONFIG-clone behavior of getDefaults, the parse-error
 * accessor surface, and the displayConfiguration startup block. Tests that mutate CONFIG save and restore the prior state in afterEach so they remain
 * independent of any other suite that touches CONFIG.
 */
import { CONFIG, applyLoggingConfigChanges, configParseError, configParseErrorMessage, displayConfiguration, getDefaults, validateConfiguration,
  validatePositiveInt, validatePositiveNumber } from "./index.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import type { Config } from "../types/index.ts";
import { DEFAULTS } from "./userConfig.ts";
import { LOG } from "../utils/index.ts";
import type { LogEntry } from "../utils/logEmitter.ts";
import assert from "node:assert/strict";
import { getPresetViewport } from "./presets.ts";
import { initializeDataDir } from "./paths.ts";
import os from "node:os";
import { subscribeToLogs } from "../utils/logEmitter.ts";

describe("validatePositiveInt", () => {

  test("returns null for a valid integer with no bounds", () => {

    assert.equal(validatePositiveInt("X", 5), null);
  });

  test("returns an error for zero (must be at least 1)", () => {

    const err = validatePositiveInt("X", 0);

    assert.match(err ?? "", /must be a positive integer/);
  });

  test("returns an error for a negative integer", () => {

    const err = validatePositiveInt("X", -1);

    assert.match(err ?? "", /must be a positive integer/);
  });

  test("returns an error for a non-integer (float)", () => {

    const err = validatePositiveInt("X", 1.5);

    assert.match(err ?? "", /must be a positive integer/);
  });

  test("returns an error for NaN", () => {

    const err = validatePositiveInt("X", Number.NaN);

    assert.match(err ?? "", /must be a positive integer/);
  });

  test("enforces the minimum bound (inclusive)", () => {

    assert.equal(validatePositiveInt("X", 5, 5), null, "value equal to min is valid");
    assert.match(validatePositiveInt("X", 4, 5) ?? "", /at least 5/);
  });

  test("enforces the maximum bound (inclusive)", () => {

    assert.equal(validatePositiveInt("X", 100, 1, 100), null, "value equal to max is valid");
    assert.match(validatePositiveInt("X", 101, 1, 100) ?? "", /at most 100/);
  });

  test("max-only bound (min undefined) accepts a value within range and rejects values above max", () => {

    /* Asserts the asymmetric checkBounds path where only the max bound is supplied. The first guard (min === undefined) takes the early-return branch; the second
     * guard (max defined) is the active gate. validatePositiveInt with explicit undefined min and a numeric max is the canonical way to reach this path through
     * the public surface, so checkBounds stays private without test-only hooks.
     */
    assert.equal(validatePositiveInt("X", 5, undefined, 10), null, "value below max-only bound is valid");
    assert.match(validatePositiveInt("X", 11, undefined, 10) ?? "", /at most 10/, "value above max-only bound is rejected");
  });

  test("both bounds undefined returns null after the positive-integer gate (no bound check fires)", () => {

    /* Asserts the both-undefined branch of checkBounds: when neither min nor max is supplied, the helper's two guards both fall through and it returns null. The
     * public-surface call validatePositiveInt("X", value) reaches this branch only after the positive-integer gate accepts the value, so we pass a valid value
     * to isolate the bound-check behavior from the gate's behavior.
     */
    assert.equal(validatePositiveInt("X", 1), null, "valid integer with no bounds returns null (boundary value 1)");
    assert.equal(validatePositiveInt("X", Number.MAX_SAFE_INTEGER), null, "valid integer with no bounds returns null (large value)");
  });

  test("error message includes the invalid value", () => {

    const err = validatePositiveInt("PORT", -7);

    assert.match(err ?? "", /-7/);
  });
});

describe("validatePositiveNumber", () => {

  test("accepts a positive float with no bounds", () => {

    assert.equal(validatePositiveNumber("X", 0.5), null);
  });

  test("rejects zero (must be > 0)", () => {

    assert.match(validatePositiveNumber("X", 0) ?? "", /must be a positive number/);
  });

  test("rejects negative values", () => {

    assert.match(validatePositiveNumber("X", -0.1) ?? "", /must be a positive number/);
  });

  test("rejects NaN", () => {

    assert.match(validatePositiveNumber("X", Number.NaN) ?? "", /must be a positive number/);
  });

  test("enforces minimum bound (inclusive)", () => {

    assert.equal(validatePositiveNumber("X", 0.01, 0.01, 5), null);
    assert.match(validatePositiveNumber("X", 0.005, 0.01) ?? "", /at least/);
  });

  test("enforces maximum bound (inclusive)", () => {

    assert.equal(validatePositiveNumber("X", 5, 0.01, 5), null);
    assert.match(validatePositiveNumber("X", 5.1, 0.01, 5) ?? "", /at most/);
  });

  test("max-only bound rejects values above max even when min is undefined", () => {

    /* Mirror of the validatePositiveInt max-only test - same checkBounds path, exercised through the float-tolerant validator instead of the integer one.
     */
    assert.equal(validatePositiveNumber("X", 0.5, undefined, 1), null, "value below max-only bound is valid");
    assert.match(validatePositiveNumber("X", 1.5, undefined, 1) ?? "", /at most 1/, "value above max-only bound is rejected");
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

  /* Snapshot the entire CONFIG object before each test so any mutation made by a test (or by validateConfiguration's own normalization) is rolled back. The
   * suite uses structuredClone to avoid shared references on nested objects.
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

  test("normalizes captureCodecs to always include h264", () => {

    CONFIG.streaming.captureCodecs = ["hevc"];
    validateConfiguration();
    assert.equal(CONFIG.streaming.captureCodecs.includes("h264"), true, "h264 is auto-prepended");
  });

  test("strips unrecognized codec identifiers from captureCodecs", () => {

    CONFIG.streaming.captureCodecs = [ "h264", "wonky-codec", "hevc" ];
    validateConfiguration();
    assert.deepEqual(CONFIG.streaming.captureCodecs, [ "h264", "hevc" ], "wonky-codec stripped, recognized survive");
  });

  test("forces captureMode to ffmpeg even when set to native (Chrome bug guard)", () => {

    /* The contract has two halves: the value mutation AND the operator-visible warning. A regression that silently swapped the value without logging would
     * leave operators wondering why their explicit "native" choice was ignored, so we assert both sides. Spying on LOG.warn rather than capturing every log line
     * keeps the assertion narrow - only the captureMode warning needs to fire here, not the unrelated DEFAULTS warnings other validation branches might emit.
     */
    const warn = mock.method(LOG, "warn", () => undefined);

    try {

      CONFIG.streaming.captureMode = "native";
      validateConfiguration();
      assert.equal(CONFIG.streaming.captureMode, "ffmpeg");

      const captureModeWarning = warn.mock.calls.some((call) => {

        const message = call.arguments[0];

        return (typeof message === "string") && message.includes("Forcing FFmpeg capture mode");
      });

      assert.equal(captureModeWarning, true, "validateConfiguration must LOG.warn when forcing the captureMode mutation");
    } finally {

      warn.mock.restore();
    }
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
});

describe("applyLoggingConfigChanges", () => {

  test("reports debugFilter as applied (live, no restart) and other logging fields as deferred", async () => {

    // commitDebugFilter applies the debug filter live during reload, so the handler reports it applied and triggers no restart. httpLogLevel and maxSize
    // are wired at startup, so they defer to a restart.
    const outcomes = await applyLoggingConfigChanges([
      { current: "tuning:hulu", path: "logging.debugFilter", previous: "" },
      { current: "all", path: "logging.httpLogLevel", previous: "errors" },
      { current: 2097152, path: "logging.maxSize", previous: 1048576 }
    ]);

    assert.deepEqual(outcomes, [
      { kind: "applied", path: "logging.debugFilter" },
      { kind: "deferred", path: "logging.httpLogLevel", reason: "this logging setting takes effect on the next restart" },
      { kind: "deferred", path: "logging.maxSize", reason: "this logging setting takes effect on the next restart" }
    ]);
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
     * so a future regression that routed the rows back through LOG.info (and re-introduced trailing periods on every tabular row) would surface immediately.
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

  /* The two `let` exports (configParseError, configParseErrorMessage) are reassigned by both initializeConfiguration and reloadConfiguration on every load.
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
