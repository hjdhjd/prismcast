/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logger.test.ts: Unit tests for the LOG primitives in logger.ts. The logger has multiple side-effect routes - SSE emission via logEmitter, file writes via
 * fileLogger, console writes when console mode is on. The fileLogger is not initialized in the test environment so its writeLogEntry path is a no-op; we
 * exercise console-mode and SSE emission instead by stubbing console methods and subscribing to the log emitter. Tests reset console-logging mode and the debug
 * filter between cases to avoid state leakage.
 */
import { LOG, isConsoleLogging, isDebugLogging, setConsoleLogging, setDebugLogging } from "./logger.ts";
import { type LogEntry, subscribeToLogs } from "./logEmitter.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import assert from "node:assert/strict";
import { initDebugFilter } from "./debugFilter.ts";
import { runWithStreamContext } from "./streamContext.ts";

describe("setConsoleLogging and isConsoleLogging", () => {

  afterEach(() => {

    setConsoleLogging(false);
  });

  test("isConsoleLogging defaults to false", () => {

    setConsoleLogging(false);

    assert.equal(isConsoleLogging(), false);
  });

  test("setConsoleLogging(true) flips the flag and the getter reads true", () => {

    setConsoleLogging(true);

    assert.equal(isConsoleLogging(), true);
  });

  test("setConsoleLogging(false) restores file-mode after enabling console", () => {

    setConsoleLogging(true);
    setConsoleLogging(false);

    assert.equal(isConsoleLogging(), false);
  });
});

describe("setDebugLogging and isDebugLogging", () => {

  afterEach(() => {

    initDebugFilter("");
  });

  test("isDebugLogging defaults to false (no debug categories enabled)", () => {

    initDebugFilter("");

    assert.equal(isDebugLogging(), false);
  });

  test("setDebugLogging(true) enables wildcard category and the getter reports true", () => {

    setDebugLogging(true);

    assert.equal(isDebugLogging(), true);
  });

  test("setDebugLogging(false) clears the filter and the getter reports false", () => {

    setDebugLogging(true);
    setDebugLogging(false);

    assert.equal(isDebugLogging(), false);
  });
});

describe("LOG.info / warn / error (via SSE emission)", () => {

  // We capture every emitted LogEntry so we can verify level routing and message shape without depending on the file or console output.
  let captured: LogEntry[];
  let unsubscribe: () => void;

  beforeEach(() => {

    captured = [];
    unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
    setConsoleLogging(false);
  });

  afterEach(() => {

    unsubscribe();
    setConsoleLogging(false);
    initDebugFilter("");
  });

  test("LOG.info emits one entry with level 'info' and the formatted message", () => {

    LOG.info("hello %s", "world");

    assert.equal(captured.length, 1);

    const entry = captured[0]!;

    assert.equal(entry.level, "info");
    assert.equal(entry.message, "hello world", "format args interpolated via util.format");
  });

  test("LOG.warn emits one entry with level 'warn'", () => {

    LOG.warn("careful");

    assert.equal(captured.length, 1);

    const entry = captured[0]!;

    assert.equal(entry.level, "warn");
    assert.equal(entry.message, "careful");
  });

  test("LOG.error emits one entry with level 'error'", () => {

    LOG.error("oh no");

    assert.equal(captured.length, 1);
    assert.equal(captured[0]?.level, "error");
  });

  test("messages without format args are emitted verbatim", () => {

    // Boundary: zero format args - the implementation skips util.format() entirely on this path.
    LOG.info("plain message");

    assert.equal(captured[0]?.message, "plain message");
  });

  test("each call produces exactly one entry (no duplication)", () => {

    LOG.info("a");
    LOG.warn("b");
    LOG.error("c");

    assert.equal(captured.length, 3);
  });

  test("includes the timestamp field on every entry", () => {

    LOG.info("timestamped");

    assert.match(captured[0]?.timestamp ?? "", /\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3}/, "timestamp matches yyyy/mm/dd hh:mm:ss.mmm");
  });
});

describe("LOG.debug category gating", () => {

  let captured: LogEntry[];
  let unsubscribe: () => void;

  beforeEach(() => {

    captured = [];
    unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
  });

  afterEach(() => {

    unsubscribe();
    initDebugFilter("");
  });

  test("does not emit when no debug categories are enabled", () => {

    initDebugFilter("");
    LOG.debug("tuning:hulu", "should be dropped");

    assert.equal(captured.length, 0);
  });

  test("emits when the specific category is enabled", () => {

    initDebugFilter("tuning:hulu");
    LOG.debug("tuning:hulu", "binary search converged");

    assert.equal(captured.length, 1);

    const entry = captured[0]!;

    assert.equal(entry.level, "debug");
    assert.equal(entry.categoryTag, "tuning:hulu", "category tag attached to the entry");
  });

  test("emits when wildcard is enabled (matches any category)", () => {

    initDebugFilter("*");
    LOG.debug("anything:goes", "yes");

    assert.equal(captured.length, 1);
  });

  test("does not emit when category is excluded under wildcard", () => {

    // Negative test: even with wildcard, an explicit exclude must suppress.
    initDebugFilter("*,-tuning:hulu");
    LOG.debug("tuning:hulu", "blocked");

    assert.equal(captured.length, 0);
  });

  test("does not emit when a different category is enabled (no cross-routing)", () => {

    initDebugFilter("recovery:tab");
    LOG.debug("tuning:hulu", "different category");

    assert.equal(captured.length, 0);
  });
});

describe("LOG stream context prefixing", () => {

  let captured: LogEntry[];
  let unsubscribe: () => void;

  beforeEach(() => {

    captured = [];
    unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
  });

  afterEach(() => {

    unsubscribe();
    initDebugFilter("");
  });

  test("prefixes messages with [streamId] when called inside runWithStreamContext", async () => {

    await runWithStreamContext({ streamId: "cnn-abc" }, async () => {

      LOG.info("in context");
    });

    assert.equal(captured.length, 1);
    assert.equal(captured[0]?.message, "[cnn-abc] in context");
  });

  test("appends the show name when the resolver returns one", async () => {

    await runWithStreamContext({ showNameResolver: () => "Today Show", streamId: "nbc-xyz" }, async () => {

      LOG.info("with show");
    });

    assert.equal(captured[0]?.message, "[nbc-xyz] [Today Show] with show");
  });

  test("omits the show name suffix when the resolver returns an empty string", async () => {

    await runWithStreamContext({ showNameResolver: () => "", streamId: "abc-def" }, async () => {

      LOG.info("no show");
    });

    assert.equal(captured[0]?.message, "[abc-def] no show", "no double-bracket section when show name is empty");
  });

  test("does NOT prefix when called outside a stream context", () => {

    LOG.info("uncontexted");

    assert.equal(captured[0]?.message, "uncontexted");
  });
});

describe("LOG.withStreamId bound logger", () => {

  let captured: LogEntry[];
  let unsubscribe: () => void;

  beforeEach(() => {

    captured = [];
    unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
  });

  afterEach(() => {

    unsubscribe();
    initDebugFilter("");
  });

  test("prefixes every emitted message with the bound streamId", () => {

    const bound = LOG.withStreamId("bound-123");

    bound.info("info-msg");
    bound.warn("warn-msg");
    bound.error("error-msg");

    assert.equal(captured.length, 3);
    assert.equal(captured[0]?.message, "[bound-123] info-msg");
    assert.equal(captured[1]?.message, "[bound-123] warn-msg");
    assert.equal(captured[2]?.message, "[bound-123] error-msg");
  });

  test("preserves the level on each routed message", () => {

    const bound = LOG.withStreamId("bound-456");

    bound.info("i");
    bound.warn("w");
    bound.error("e");

    assert.equal(captured[0]?.level, "info");
    assert.equal(captured[1]?.level, "warn");
    assert.equal(captured[2]?.level, "error");
  });

  test("debug method respects category gating", () => {

    initDebugFilter("recovery:tab");
    const bound = LOG.withStreamId("bound-789");

    bound.debug("recovery:tab", "should appear");
    bound.debug("recovery:nav", "should NOT appear");

    assert.equal(captured.length, 1);
    assert.equal(captured[0]?.message, "[bound-789] should appear");
  });

  test("does NOT include the show name (only stream id)", async () => {

    // The bound logger uses an explicit streamId path that bypasses the show-name resolver. This is the documented contract: bound loggers are for outside-of-context
    // logging where no show is in scope.
    const bound = LOG.withStreamId("bound-abc");

    await runWithStreamContext({ showNameResolver: () => "Some Show", streamId: "ignored" }, async () => {

      bound.info("explicit");
    });

    assert.equal(captured[0]?.message, "[bound-abc] explicit", "show name suffix not applied on bound logger");
  });
});

describe("LOG console mode output", () => {

  let logCalls: unknown[][];
  let warnCalls: unknown[][];
  let errorCalls: unknown[][];

  beforeEach(() => {

    logCalls = [];
    warnCalls = [];
    errorCalls = [];

    mock.method(globalThis.console, "log", (...args: unknown[]): void => { logCalls.push(args); });
    mock.method(globalThis.console, "warn", (...args: unknown[]): void => { warnCalls.push(args); });
    mock.method(globalThis.console, "error", (...args: unknown[]): void => { errorCalls.push(args); });

    setConsoleLogging(true);
  });

  afterEach(() => {

    setConsoleLogging(false);
    mock.reset();
  });

  test("LOG.info routes to console.log when console mode is on", () => {

    LOG.info("hello");

    assert.equal(logCalls.length, 1);
    assert.equal(warnCalls.length, 0);
    assert.equal(errorCalls.length, 0);
  });

  test("LOG.warn routes to console.warn (yellow path)", () => {

    LOG.warn("caution");

    assert.equal(warnCalls.length, 1);
    assert.equal(logCalls.length, 0);
  });

  test("LOG.error routes to console.error (red path)", () => {

    LOG.error("boom");

    assert.equal(errorCalls.length, 1);
    assert.equal(logCalls.length, 0);
  });
});
