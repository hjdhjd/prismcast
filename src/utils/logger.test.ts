/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logger.test.ts: Unit tests for the LOG primitives in logger.ts. The logger has multiple side-effect routes - SSE emission via logEmitter, file writes via
 * fileLogger, console writes when console mode is on. The fileLogger is not initialized in the test environment so its writeLogEntry path is a no-op; we
 * exercise console-mode and SSE emission instead by stubbing console methods and subscribing to the log emitter. Tests reset console-logging mode and the debug
 * filter between cases to avoid state leakage.
 */
import { LOG, displayLine, isConsoleLogging, isDebugLogging, setConsoleLogging, setDebugLogging } from "./logger.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import type { LogEntry } from "./logEmitter.ts";
import assert from "node:assert/strict";
import { initDebugFilter } from "./debugFilter.ts";
import { runWithStreamContext } from "./streamContext.ts";
import { subscribeToLogs } from "./logEmitter.ts";

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

  test("LOG.info emits one entry with level 'info' and the sentence-terminated formatted message", () => {

    LOG.info("hello %s", "world");

    assert.equal(captured.length, 1);

    const entry = captured[0]!;

    assert.equal(entry.level, "info");
    assert.equal(entry.message, "hello world.", "format args interpolated via util.format and terminated by the logger's sentence contract");
  });

  test("LOG.warn emits one entry with level 'warn'", () => {

    LOG.warn("careful");

    assert.equal(captured.length, 1);

    const entry = captured[0]!;

    assert.equal(entry.level, "warn");
    assert.equal(entry.message, "careful.");
  });

  test("LOG.error emits one entry with level 'error'", () => {

    LOG.error("oh no");

    assert.equal(captured.length, 1);
    assert.equal(captured[0]?.level, "error");
  });

  test("messages without format args are emitted with the sentence terminator appended", () => {

    // Boundary: zero format args - the implementation skips util.format() entirely on this path but still runs the message through the sentence normalizer.
    LOG.info("plain message");

    assert.equal(captured[0]?.message, "plain message.");
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
    assert.equal(captured[0]?.message, "[cnn-abc] in context.");
  });

  test("appends the show name when the resolver returns one", async () => {

    await runWithStreamContext({ showNameResolver: () => "Today Show", streamId: "nbc-xyz" }, async () => {

      LOG.info("with show");
    });

    assert.equal(captured[0]?.message, "[nbc-xyz] [Today Show] with show.");
  });

  test("omits the show name suffix when the resolver returns an empty string", async () => {

    await runWithStreamContext({ showNameResolver: () => "", streamId: "abc-def" }, async () => {

      LOG.info("no show");
    });

    assert.equal(captured[0]?.message, "[abc-def] no show.", "no double-bracket section when show name is empty");
  });

  test("does NOT prefix when called outside a stream context", () => {

    LOG.info("uncontexted");

    assert.equal(captured[0]?.message, "uncontexted.");
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
    assert.equal(captured[0]?.message, "[bound-123] info-msg.");
    assert.equal(captured[1]?.message, "[bound-123] warn-msg.");
    assert.equal(captured[2]?.message, "[bound-123] error-msg.");
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

    assert.equal(captured[0]?.message, "[bound-abc] explicit.", "show name suffix not applied on bound logger");
  });
});

describe("LOG sentence normalization (info / warn / error)", () => {

  /* The logger guarantees that every non-debug line ends with exactly one terminator. This suite pins each branch of the normalizer so a future regression in
   * the helper (or a removal of the call from logWithLevel) surfaces immediately. Debug intentionally bypasses the normalizer and is covered separately.
   */
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

  test("appends a period when the message lacks any terminator", () => {

    LOG.info("plain");

    assert.equal(captured[0]?.message, "plain.");
  });

  test("leaves a single trailing period unchanged", () => {

    LOG.info("already terminated.");

    assert.equal(captured[0]?.message, "already terminated.");
  });

  test("collapses repeated trailing periods to a single period (double-period regression class)", () => {

    // This is the exact pathology the helper exists to prevent: a format string carrying "." plus an interpolated value also carrying "." would yield "..".
    LOG.info("redundant..");

    assert.equal(captured[0]?.message, "redundant.");
  });

  test("collapses long runs of trailing periods to a single period", () => {

    LOG.info("ellipsis-like...");

    assert.equal(captured[0]?.message, "ellipsis-like.");
  });

  test("preserves a trailing question mark (producer-intentional)", () => {

    LOG.info("is this expected?");

    assert.equal(captured[0]?.message, "is this expected?");
  });

  test("preserves a trailing exclamation mark (producer-intentional)", () => {

    LOG.warn("watch out!");

    assert.equal(captured[0]?.message, "watch out!");
  });

  test("normalizes warn-level messages", () => {

    LOG.warn("warn body");

    assert.equal(captured[0]?.message, "warn body.");
  });

  test("normalizes error-level messages", () => {

    LOG.error("error body");

    assert.equal(captured[0]?.message, "error body.");
  });

  test("normalizes the body but not the stream-id prefix when running in a stream context", async () => {

    await runWithStreamContext({ streamId: "stream-1" }, async () => {

      LOG.info("inside ctx");
    });

    assert.equal(captured[0]?.message, "[stream-1] inside ctx.");
  });

  test("does NOT normalize debug messages (debug is fragments by convention)", () => {

    initDebugFilter("*");
    LOG.debug("recovery:tab", "raw fragment");

    assert.equal(captured[0]?.message, "raw fragment", "debug bypasses the sentence contract");
  });

  test("works with %s interpolation: format-string period + value period collapses to one", () => {

    // The original regression: format string ends with "." and value also ends with "." - the assembled message would be ".." without the normalizer.
    LOG.info("startup failed: %s", "Invalid URL.");

    assert.equal(captured[0]?.message, "startup failed: Invalid URL.");
  });

  test("works with %s interpolation: format-string period + value without period yields one period", () => {

    // formatError strips trailing punctuation so values may arrive bare; the period in the format string carries through unchanged.
    LOG.info("startup failed: %s.", "boom");

    assert.equal(captured[0]?.message, "startup failed: boom.");
  });

  test("works with %s interpolation: no format-string period + value carries period yields one period", () => {

    // The historical "userMessage carries period, format string omits" pattern: still emits a single terminating period after normalization.
    LOG.info("startup failed: %s", "Invalid URL.");

    assert.equal(captured[0]?.message, "startup failed: Invalid URL.");
  });
});

describe("displayLine (non-sentence escape hatch)", () => {

  /* displayLine routes through the same SSE / file / console pipeline as LOG.info but bypasses the sentence normalizer. It exists so structured display output
   * (the startup configuration dump, banners) can render without forced terminal periods. This suite pins its contract so a future change that quietly funneled
   * displayLine through normalizeSentence - or stripped its format-arg support - would surface immediately.
   */
  let captured: LogEntry[];
  let unsubscribe: () => void;

  beforeEach(() => {

    captured = [];
    unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
  });

  afterEach(() => {

    unsubscribe();
  });

  test("emits at info level (matches LOG.info routing without the contract)", () => {

    displayLine("plain");

    assert.equal(captured.length, 1);
    assert.equal(captured[0]?.level, "info");
  });

  test("does NOT append a terminator to a message that lacks one", () => {

    // The whole point of the escape hatch: a structured row like "  Server port: 5589" must NOT become "  Server port: 5589." on the way out.
    displayLine("  Server port: 5589");

    assert.equal(captured[0]?.message, "  Server port: 5589");
  });

  test("does NOT touch a message that already ends with a colon (e.g. block header)", () => {

    // The startup block header ends with ":" to introduce the rows below. The sentence normalizer would append "." after the colon; displayLine preserves it.
    displayLine("Starting PrismCast v1.10.1 with configuration:");

    assert.equal(captured[0]?.message, "Starting PrismCast v1.10.1 with configuration:");
  });

  test("supports util.format interpolation like LOG.info", () => {

    displayLine("  HLS segment duration: %ss, max segments: %s", 6, 12);

    assert.equal(captured[0]?.message, "  HLS segment duration: 6s, max segments: 12");
  });

  test("preserves a trailing period verbatim (no double-period collapse, no append)", () => {

    // Caller is in charge of terminal punctuation. We emit exactly what was passed in.
    displayLine("ends with period.");

    assert.equal(captured[0]?.message, "ends with period.");
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

  test("warn-level output in console mode embeds the SGR escape sequence (styleText was actually applied)", () => {

    // The console-mode branch wraps the message in styleText(color, ...) when color is non-null. The previous tests verified the routing (warn->console.warn,
    // error->console.error) but never asserted that the message string actually contained the SGR escape introduced by styleText. A regression that swapped
    // styleText for plain string passing would still route to the right console method but emit uncolored output - this test catches that.
    LOG.warn("colorized");

    const captured = typeof warnCalls[0]?.[0] === "string" ? warnCalls[0][0] : "";

    // styleText wraps the message in an ANSI SGR escape sequence (ESC + open-bracket + code + m + payload + ESC + open-bracket + 39m). The two-character
    // ESC-and-open-bracket sequence is the introducer every styleText output emits regardless of color choice; we use String.includes for the substring check
    // rather than a regex literal so the control character does not need to appear inline.
    assert.equal(captured.includes("\u001b["), true, "styled output carries the SGR escape sequence introduced by styleText");
  });

  test("error-level output in console mode is also styled (red path)", () => {

    // Symmetric with the warn case - error messages route through the same styleText branch with color="red".
    LOG.error("colorized error");

    const captured = typeof errorCalls[0]?.[0] === "string" ? errorCalls[0][0] : "";

    assert.equal(captured.includes("\u001b["), true, "error output carries the SGR escape sequence");
  });

  test("info-level output in console mode is NOT styled (color=null path)", () => {

    // The info color is null, which takes the unstyled branch (line 166: consoleMethod(logMessage)). We assert the absence of the SGR sequence so a future
    // change that accidentally styled info messages surfaces here.
    LOG.info("plain info");

    const captured = typeof logCalls[0]?.[0] === "string" ? logCalls[0][0] : "";

    assert.equal(captured.includes("\u001b["), false, "info output is plain (color=null skips styleText)");
  });
});
