/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the entry point module. index.ts is a top-level script: importing it runs the unhandled-rejection / uncaught-exception process
 * handlers, calls initializeDataDir, branches on the first argv token, and (on the default branch) calls startServer which spawns Chrome and binds the port. The
 * helpers it keeps - parseArgs and requireAbsolutePath - are not exported, so they cannot be reached from a unit test without importing the module and
 * triggering its side effects. The only exported surface that is safe to import via `import type` (which is fully erased) is the ParsedArgs interface, and this
 * file exercises that surface. The behavior the entry point delegates is covered where it lives, and the notes at the end of this file name each place.
 */
import { describe, test } from "node:test";
import type { ParsedArgs } from "./index.ts";
import assert from "node:assert/strict";

/* The ParsedArgs interface is the contract between parseArgs (the private CLI parser) and startServer (its consumer). Locking the interface shape catches
 * accidental field renames or visibility changes that would silently break the merge order CLI > env > config.json > defaults. A type-only import is fully
 * erased at compile time, so importing it does NOT execute the entry-point script - the module never runs initializeDataDir or startServer.
 */

describe("ParsedArgs", () => {

  test("accepts a literal with only the two required boolean fields populated", () => {

    // The required boolean fields are the ones the entry point defaults to false before parsing; the remaining fields are optional path/port flags that
    // remain undefined when the corresponding CLI flag is not passed. Locking this shape ensures parseArgs and startServer stay in sync about which fields
    // are guaranteed to be present.
    const minimal: ParsedArgs = {

      consoleLogging: false,
      debugLogging: false
    };

    assert.equal(minimal.consoleLogging, false, "consoleLogging defaults to false at the type level");
    assert.equal(minimal.debugLogging, false, "debugLogging defaults to false at the type level");
    assert.equal(minimal.chromeDataDir, undefined, "chromeDataDir is optional and starts undefined");
    assert.equal(minimal.dataDir, undefined, "dataDir is optional and starts undefined");
    assert.equal(minimal.logFile, undefined, "logFile is optional and starts undefined");
    assert.equal(minimal.port, undefined, "port is optional and starts undefined");
  });

  test("accepts a literal with every optional path-and-port field populated", () => {

    // The optional fields cover every path/port flag the CLI accepts. Locking this shape means a future addition (e.g., --extension-dir) must extend the
    // interface rather than smuggle a new field through ad hoc.
    const full: ParsedArgs = {

      chromeDataDir: "/var/lib/prismcast/chromedata",
      consoleLogging: true,
      dataDir: "/var/lib/prismcast",
      debugLogging: true,
      logFile: "/var/log/prismcast.log",
      port: 5589
    };

    assert.equal(full.chromeDataDir, "/var/lib/prismcast/chromedata");
    assert.equal(full.consoleLogging, true);
    assert.equal(full.dataDir, "/var/lib/prismcast");
    assert.equal(full.debugLogging, true);
    assert.equal(full.logFile, "/var/log/prismcast.log");
    assert.equal(full.port, 5589);
  });

  test("port is typed as number (locks against accidental string typing)", () => {

    // Boundary: parseArgs runs parseInt on the raw CLI argument and only assigns when it isn't NaN. The interface enforces that the resulting field is a number,
    // which keeps downstream code (the cliOverrides assembly in startServer, the server.listen call) free of string-to-number coercions.
    const args: ParsedArgs = { consoleLogging: false, debugLogging: false, port: 8080 };

    assert.equal(typeof args.port, "number", "port must be a number when present");
  });
});

/* Where the rest of index.ts is covered. No suite spawns the entry point as a subprocess, because the default branch boots the server, which launches Chrome and
 * binds the port, so what the entry point delegates lives in modules a unit row can import, and what it keeps for itself no suite reaches:
 *
 * - The usage text (-h / --help) and the environment listing (--list-env) render in cliHelp.ts, and src/cliHelp.test.ts covers the text and the listing, every
 *   printed default and every category included.
 *
 * - The startup failure catch is handleStartupFailure in app.ts, and src/app.test.ts covers its superseded and fatal branches.
 *
 * - parseArgs() and requireAbsolutePath(): not exported, read process.argv directly and call process.exit on -h, -v and a relative path. No suite covers them.
 *
 * - The unhandledRejection / uncaughtException handlers: registered at module load via process.on. Mutating process state from a unit test would leak across
 *   the rest of the test run, and the test runner has its own unhandled-rejection guard that would compete. No suite covers them.
 *
 * - The 'exit' handler that calls flushLogBufferSync / releaseInstanceSlot / killStaleChrome: registered only on the default branch (server startup), which no
 *   suite reaches.
 *
 * - The dispatch to handleServiceCommand / handleUpgradeCommand / the environment listing / startServer: top-level promise chains that branch on the first argv
 *   token, which no suite reaches.
 */
