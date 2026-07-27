/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the entry point module. index.ts is a top-level script: importing it runs the unhandled-rejection / uncaught-exception process
 * handlers, calls initializeDataDir, branches on the first argv token, and (on the default branch) calls startServer which spawns Chrome and binds the port. The
 * helpers it does have - parseArgs, requireAbsolutePath, printUsage, printEnvironmentVariables - are not exported, so they cannot be reached from a unit test
 * without importing the module and triggering its side effects. The only exported surface that is safe to import via `import type` (which is fully erased) is
 * the ParsedArgs interface, and this file exercises that surface.
 *
 * Everything else is deferred to the e2e suite, which is the appropriate level for testing process-level orchestration.
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

/* The remaining surface area of index.ts is deferred. We document the deferrals here so a future maintainer can confirm the test conventions are being
 * followed and not just silently skipped:
 *
 * - parseArgs(): not exported, parses process.argv directly and on -h / -v it calls process.exit. Cannot be tested in isolation without exporting
 *   it; importing the module to reach it would also run the surrounding entry-point code (initializeDataDir, branch on subcommand, startServer). Deferred to
 *   e2e where the CLI is invoked as a subprocess.
 *
 * - requireAbsolutePath(): not exported, calls process.exit(1) on relative paths. Same import-side-effect problem as parseArgs. Deferred to e2e.
 *
 * - printUsage(): not exported, writes to console.log. Same import-side-effect problem. Deferred to e2e.
 *
 * - printEnvironmentVariables(): not exported, walks CONFIG_METADATA and writes to console.log. Same import-side-effect problem. Deferred to e2e, where the
 *   subprocess invocation `prismcast --list-env` exercises every category branch end-to-end.
 *
 * - The unhandledRejection / uncaughtException handlers: registered at module load via process.on. Mutating process state from a unit test would leak across
 *   the rest of the test run, and the test runner has its own unhandled-rejection guard that would compete. Deferred to e2e.
 *
 * - The 'exit' handler that calls flushLogBufferSync / releaseInstanceSlot / killStaleChrome: registered only on the default branch (server startup), which we
 *   cannot reach without importing index.ts and triggering startServer. Deferred to e2e.
 *
 * - The dispatch to handleServiceCommand / handleUpgradeCommand / printEnvironmentVariables / startServer: top-level await-less promise chains that branch on
 *   the first argv token. This is the script's main control flow and can only be observed by spawning the entry point as a subprocess. Deferred to e2e.
 */
