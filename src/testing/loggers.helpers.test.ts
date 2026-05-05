/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * loggers.helpers.test.ts: Tests for silentLog and capturingLog. These are the LOG doubles every other test relies on, so a bug here cascades into misleading
 * results across the entire suite. Coverage pins: silent contract (drop-the-call), capture shape (level, message, args, category, streamId), bound-logger
 * delegation, snapshot semantics on lines(), and clear() resetting the buffer.
 */
import { capturingLog, silentLog } from "./loggers.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { firstOf } from "./narrowing.helpers.ts";

describe("silentLog", () => {

  test("level methods are present and callable without throwing", () => {

    // The TestLogger interface declares each method as returning void, so the type system already guarantees no return value. We only verify here that the
    // methods exist on the returned object and complete without throwing - the silent contract is "drop the call quietly."
    const log = silentLog();

    assert.doesNotThrow(() => {

      log.debug("any-category", "msg");
    }, "debug should be callable");

    assert.doesNotThrow(() => {

      log.error("msg");
    }, "error should be callable");

    assert.doesNotThrow(() => {

      log.info("msg");
    }, "info should be callable");

    assert.doesNotThrow(() => {

      log.warn("msg");
    }, "warn should be callable");
  });

  test("accepts the same call shapes as production LOG without throwing", () => {

    const log = silentLog();

    // Production code calls these with 0+ format args following the message; the helper must accept all of them silently.
    assert.doesNotThrow(() => {

      log.info("plain");
    }, "info(message) should not throw");

    assert.doesNotThrow(() => {

      log.info("with %s", "value");
    }, "info(message, arg) should not throw");

    assert.doesNotThrow(() => {

      log.info("with %s %d %j", "a", 1, { b: 2 });
    }, "info(message, ...args) should not throw");

    assert.doesNotThrow(() => {

      log.debug("cat:sub", "msg with %s", "x");
    }, "debug should accept a category plus format args");
  });

  test("withStreamId returns a bound logger that also no-ops on every level", () => {

    const bound = silentLog().withStreamId("stream-001");

    assert.doesNotThrow(() => {

      bound.debug("any-category", "msg");
    }, "bound debug should be callable");

    assert.doesNotThrow(() => {

      bound.error("msg");
    }, "bound error should be callable");

    assert.doesNotThrow(() => {

      bound.info("msg");
    }, "bound info should be callable");

    assert.doesNotThrow(() => {

      bound.warn("msg");
    }, "bound warn should be callable");
  });

  test("withStreamId can be called repeatedly with different stream IDs", () => {

    // The implementation is free to share a single bound object across stream IDs; what matters here is that two callers each get back a working bound logger.
    const log = silentLog();

    const bound1 = log.withStreamId("a");
    const bound2 = log.withStreamId("b");

    assert.doesNotThrow(() => {

      bound1.info("x");
    }, "first bound info should no-op");

    assert.doesNotThrow(() => {

      bound2.warn("y");
    }, "second bound warn should no-op");
  });
});

describe("capturingLog", () => {

  test("records info calls with their message and args", () => {

    const cap = capturingLog();

    cap.logger.info("hello %s", "world");

    assert.deepEqual(cap.lines(), [{ args: ["world"], level: "info", message: "hello %s" }], "info call should be recorded with literal message and args");
  });

  test("records error and warn calls under their respective levels", () => {

    const cap = capturingLog();

    cap.logger.error("boom");
    cap.logger.warn("careful");

    const lines = cap.lines();

    assert.equal(lines.length, 2, "should have two captured lines");
    assert.equal(lines[0]!.level, "error", "first should be error");
    assert.equal(lines[1]!.level, "warn", "second should be warn");
  });

  test("records debug calls with the category extracted from the first argument", () => {

    const cap = capturingLog();

    cap.logger.debug("tuning:hulu", "tuned %s", "ABC");

    const line = firstOf(cap.lines(), "captured log line");

    assert.equal(line.level, "debug", "level should be debug");
    assert.equal(line.category, "tuning:hulu", "category should be the first arg");
    assert.equal(line.message, "tuned %s", "message should be the second arg");
    assert.deepEqual(line.args, ["ABC"] as unknown[], "remaining args should be after the message");
  });

  test("records bound logger calls with the stream ID attached", () => {

    const cap = capturingLog();

    cap.logger.withStreamId("s-42").info("started");

    const line = firstOf(cap.lines(), "captured log line");

    assert.equal(line.streamId, "s-42", "stream ID should be recorded on the captured line");
    assert.equal(line.level, "info", "level should be info");
    assert.equal(line.message, "started", "message should match");
  });

  test("records bound debug calls with both stream ID and category", () => {

    const cap = capturingLog();

    cap.logger.withStreamId("s-9").debug("recovery:tab", "replacing");

    const line = firstOf(cap.lines(), "captured log line");

    assert.equal(line.streamId, "s-9", "stream ID should be present");
    assert.equal(line.category, "recovery:tab", "category should be present");
    assert.equal(line.message, "replacing", "message should match");
  });

  test("clear() empties the captured buffer in place", () => {

    const cap = capturingLog();

    cap.logger.info("a");
    cap.logger.info("b");

    assert.equal(cap.lines().length, 2, "should have two lines before clear");

    cap.clear();

    assert.equal(cap.lines().length, 0, "should have zero lines after clear");
  });

  test("lines() returns a snapshot - mutating it does not affect future captures", () => {

    const cap = capturingLog();

    cap.logger.info("first");

    const snapshot = cap.lines();

    snapshot.push({ args: [], level: "info", message: "injected" });

    cap.logger.info("second");

    const fresh = cap.lines();

    assert.equal(fresh.length, 2, "fresh snapshot should reflect only real captures");
    assert.equal(fresh[1]!.message, "second", "fresh snapshot's last entry should be the second real call");
  });

  test("starts with no captured lines on a fresh instance", () => {

    assert.deepEqual(capturingLog().lines(), [], "fresh capturingLog should have empty lines");
  });
});
