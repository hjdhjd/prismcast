/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logs.test.ts: HTTP-level integration coverage for the log-viewing route (src/routes/logs.ts). GET /logs reads the on-disk log file at getLogFilePath(CONFIG),
 * which - with the default (null) config.paths.logFile - resolves to <dataDir>/prismcast.log, the very path the harness exposes as pathInDataDir(ctx,
 * "prismcast.log"). We seed a raw log fixture at that path and assert the parse contract: an info line (no level bracket) maps to level "info", a [WARN]/[ERROR]
 * line maps to the matching level, a [DEBUG:category] line maps to level "debug" and surfaces the category as categoryTag, and a line that does not match
 * LOG_LINE_PATTERN is dropped. total counts only the parsed (matching) lines.
 *
 * The file logger (src/utils/fileLogger.ts) is initialized only by app.ts's startup path via initializeFileLogger(); bootApp deliberately does not call it, so
 * isInitialized stays false and no runtime log lines are appended to the seeded file during the test. That makes the parsed set exactly the fixture, so the
 * total / filter / slice assertions can be exact rather than lower-bounds. We still assert setConsoleLogging(false) at the top so console mode (which would short-
 * circuit readLogEntries to mode:"console" with no file read) cannot leak in from another suite sharing the module singleton and flip the response shape.
 *
 * The 500 branch is fault-injected without touching production: mkdir'ing a directory at the log path makes readFile throw EISDIR (not the ENOENT the handler
 * treats as an empty file), which propagates out of readLogEntries and lands in the route's catch. That catch ships the rich error payload through
 * sendErrorResponse's numeric-status overload, so the wire shape is the fixed { entries, error, filtered, mode, total } plus the canonical success:false marker
 * at HTTP 500. Sibling suites (streams.test.ts, settings-preservation.test.ts) seed the registry / config; this suite seeds the log file on disk instead.
 */
import { bootApp, createIntegrationContext, initializePersistence, pathInDataDir } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { mkdir, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import { setConsoleLogging } from "../../../src/utils/logger.ts";

interface LogEntry {

  categoryTag?: string;
  level: "debug" | "error" | "info" | "warn";
  message: string;
  timestamp: string;
}

interface LogsResponse {

  entries: LogEntry[];
  error?: string;
  filtered: number;
  mode: "console" | "file";
  success?: boolean;
  total: number;
}

/* A fixture with one line per parse outcome: an info line (no level bracket), a [WARN] line, an [ERROR] line, a [DEBUG:tuning:hulu] line whose category must be
 * extracted, and a trailing junk line that does not match LOG_LINE_PATTERN and must be dropped. The leading lines are the entries GET /logs must parse.
 * Timestamps use the file logger's YYYY/MM/DD HH:MM:SS.mmm shape so LOG_LINE_PATTERN accepts them.
 */
const INFO_LINE = "[2026/07/04 10:15:30.100] An informational startup line.";
const WARN_LINE = "[2026/07/04 10:15:31.200] [WARN] A cautionary warning line.";
const ERROR_LINE = "[2026/07/04 10:15:32.300] [ERROR] A fatal error line.";
const DEBUG_LINE = "[2026/07/04 10:15:33.400] [DEBUG:tuning:hulu] A hulu tuning debug line.";
const JUNK_LINE = "this line is not a valid log entry and must be dropped";

/**
 * Composes the seed fixture as a newline-joined block. Kept as a helper so every test in the suite seeds byte-identical content.
 * @returns The raw log-file content for the fixture.
 */
function fixtureContent(): string {

  return [ INFO_LINE, WARN_LINE, ERROR_LINE, DEBUG_LINE, JUNK_LINE ].join("\n") + "\n";
}

describe("GET /logs - file-mode parse contract", () => {

  test("parses the fixture, maps levels and category, drops the junk line, and counts only parsed lines", async () => {

    await using ctx = await createIntegrationContext();

    // Console mode would short-circuit readLogEntries to mode:"console" and never read the file; assert file mode so the fixture is what the handler parses.
    setConsoleLogging(false);

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Seed the raw log file at the fallback path getLogFilePath(CONFIG) resolves to (config.paths.logFile defaults to null -> <dataDir>/prismcast.log).
    await writeFile(pathInDataDir(ctx, "prismcast.log"), fixtureContent(), "utf8");

    const response = await fetch(urlFor("/logs"));

    assert.equal(response.status, 200, "the logs listing responds 200");

    const body = await response.json() as LogsResponse;

    assert.equal(body.mode, "file", "the handler read the file rather than reporting console mode");

    // The junk line does not match LOG_LINE_PATTERN, so it is excluded from the parsed set. total counts the parsed entries.
    assert.equal(body.total, 4, "total counts the four parsable lines and excludes the junk line");
    assert.equal(body.entries.length, 4, "all four parsed entries are returned under the default 100-line cap");

    const [ info, warn, error, debug ] = body.entries;

    // A missing level bracket maps to info; the message is the text after the timestamp.
    assert.ok(info, "the info entry is present");
    assert.equal(info.level, "info", "a line with no level bracket maps to info");
    assert.equal(info.message, "An informational startup line.", "the info message is the text after the timestamp");
    assert.equal(info.timestamp, "2026/07/04 10:15:30.100", "the timestamp capture excludes the surrounding brackets");
    assert.equal(info.categoryTag, undefined, "an info line carries no categoryTag");

    // [WARN] maps to warn.
    assert.ok(warn, "the warn entry is present");
    assert.equal(warn.level, "warn", "a [WARN] line maps to warn");
    assert.equal(warn.message, "A cautionary warning line.", "the warn message excludes the level bracket");

    // [ERROR] maps to error.
    assert.ok(error, "the error entry is present");
    assert.equal(error.level, "error", "an [ERROR] line maps to error");
    assert.equal(error.message, "A fatal error line.", "the error message excludes the level bracket");

    // [DEBUG:tuning:hulu] maps to debug and extracts the category suffix after the first colon as categoryTag.
    assert.ok(debug, "the debug entry is present");
    assert.equal(debug.level, "debug", "a [DEBUG:...] line maps to debug");
    assert.equal(debug.categoryTag, "tuning:hulu", "the category suffix after the first colon becomes categoryTag");
    assert.equal(debug.message, "A hulu tuning debug line.", "the debug message excludes the level-and-category bracket");
  });
});

describe("GET /logs?level=error - level filter", () => {

  test("returns only error-level entries and reports filtered separately from total", async () => {

    await using ctx = await createIntegrationContext();

    setConsoleLogging(false);

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await writeFile(pathInDataDir(ctx, "prismcast.log"), fixtureContent(), "utf8");

    const response = await fetch(urlFor("/logs?level=error"));

    assert.equal(response.status, 200, "the filtered logs listing responds 200");

    const body = await response.json() as LogsResponse;

    // The filter keeps only entries whose level equals the requested value; the fixture has exactly one error line.
    assert.equal(body.filtered, 1, "exactly one fixture entry survives the error filter");
    assert.equal(body.total, 4, "total still counts every parsed entry, unaffected by the filter");
    assert.equal(body.entries.length, 1, "only the surviving entry is returned");

    const [only] = body.entries;

    assert.ok(only, "the single filtered entry is present");
    assert.equal(only.level, "error", "the returned entry is the error-level one");
    assert.equal(only.message, "A fatal error line.", "the returned entry is the seeded error line");
  });
});

describe("GET /logs?lines=1 - last-N slice", () => {

  test("returns the last parsed entry via slice(-1), dropping the earlier parsed entries", async () => {

    await using ctx = await createIntegrationContext();

    setConsoleLogging(false);

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await writeFile(pathInDataDir(ctx, "prismcast.log"), fixtureContent(), "utf8");

    const response = await fetch(urlFor("/logs?lines=1"));

    assert.equal(response.status, 200, "the sliced logs listing responds 200");

    const body = await response.json() as LogsResponse;

    // slice(-1) keeps only the final parsed entry. The junk line never becomes an entry, so the last entry is the DEBUG line, the last fixture line that
    // parses, not the trailing junk. total is unaffected by the slice.
    assert.equal(body.total, 4, "total counts every parsed entry, independent of the line cap");
    assert.equal(body.entries.length, 1, "the line cap returns exactly one entry");

    const [last] = body.entries;

    assert.ok(last, "the single sliced entry is present");
    assert.equal(last.level, "debug", "slice(-1) returns the last parsed entry, which is the debug line");
    assert.equal(last.categoryTag, "tuning:hulu", "the sliced entry retains its extracted categoryTag");
    assert.equal(last.message, "A hulu tuning debug line.", "the sliced entry is the debug line, not the dropped junk line");
  });
});

describe("GET /logs - read-failure 500 branch", () => {

  test("ships the fixed error payload with the success marker at 500 when the log path is unreadable", async () => {

    await using ctx = await createIntegrationContext();

    setConsoleLogging(false);

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Fault-inject a non-ENOENT read failure: a directory at the log path makes readFile throw EISDIR, which readLogEntries rethrows (only ENOENT is swallowed as
    // an empty file) and the route's catch converts into the 500 error response.
    await mkdir(pathInDataDir(ctx, "prismcast.log"), { recursive: true });

    const response = await fetch(urlFor("/logs"));

    assert.equal(response.status, 500, "an unreadable log path yields HTTP 500");

    const body = await response.json() as LogsResponse;

    // The catch ships the rich payload verbatim through sendErrorResponse's numeric-status overload, which appends success:false. The empty-state fields let the
    // log viewer render without branching on response shape.
    assert.deepEqual(body.entries, [], "the failure payload carries an empty entries array");
    assert.equal(body.error, "Failed to read log file.", "the failure payload carries the documented error copy");
    assert.equal(body.filtered, 0, "the failure payload reports zero filtered entries");
    assert.equal(body.mode, "file", "the failure payload keeps the file mode marker");
    assert.equal(body.total, 0, "the failure payload reports zero total entries");
    assert.equal(body.success, false, "the failure payload carries the canonical envelope marker");
  });
});
