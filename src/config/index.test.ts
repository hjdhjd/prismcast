/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the CONFIG validation layer. The merge layer (mergeConfiguration) is exercised in userConfig.test.ts; here we focus on the
 * validation gate (validatePositiveInt, validatePositiveNumber, validateConfiguration) and the per-CONFIG-clone behavior of getDefaults. Tests that mutate
 * CONFIG save and restore the prior state in afterEach so they remain independent of any other suite that touches CONFIG.
 */
import { CONFIG, getDefaults, validateConfiguration, validatePositiveInt, validatePositiveNumber } from "./index.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import type { Config } from "../types/index.ts";
import { DEFAULTS } from "./userConfig.ts";
import assert from "node:assert/strict";

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

    /* Restore by reassigning every top-level group on the live CONFIG. We cannot reassign CONFIG itself because it's an exported `let` consumed by reference
     * across many modules - replacing the reference would leave them seeing the old value.
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

    CONFIG.streaming.captureMode = "native";
    validateConfiguration();
    assert.equal(CONFIG.streaming.captureMode, "ffmpeg");
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
