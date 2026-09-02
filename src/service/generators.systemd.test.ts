/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.systemd.test.ts: Unit tests for createSystemdGenerator (the Linux systemd generator) plus runAndSurfaceStderr (the helper that enriches command
 * failures with the underlying tool's stderr text). Pure leaves live in generators.test.ts; getServicePaths and detectStalePaths variants live in
 * generators.paths.test.ts; macOS LaunchAgent lives in generators.launchAgent.test.ts; Windows scheduler lives in generators.windowsTask.test.ts.
 */
import { definitionFixture, makeFakeIO } from "./generators.helpers.ts";
import { describe, test } from "node:test";
import { execFileFromMap, firstOf, makeExecFileError, nthOf } from "../testing.helpers.ts";
import { getServiceGenerator, getServicePaths } from "./generators.ts";
import assert from "node:assert/strict";

describe("createSystemdGenerator (via getServiceGenerator on linux)", () => {

  test("install: writes the systemd unit and runs daemon-reload + enable + restart", async () => {

    // The final verb is restart, not start, so a --force reinstall over a running unit replaces the process against the freshly reloaded unit definition. Both
    // the fixture key and the asserted sequence below carry that expectation. Against source that still issues start, the failure surfaces at the exec rather
    // than at the assertion: the map holds no entry for that command, so execFileFromMap throws "no result configured for systemctl --user start
    // prismcast.service". That names the stale verb outright, which is a more direct read than spotting the difference between two arrays would be.
    const installPath = "/Users/test/.config/systemd/user/prismcast.service";
    const { calls, io, writes } = makeFakeIO({

      execFile: execFileFromMap({


        "systemctl --user daemon-reload": { stdout: "" },
        "systemctl --user enable prismcast.service": { stdout: "" },
        "systemctl --user restart prismcast.service": { stdout: "" }
      }),
      platform: "linux",
      serviceFileDirectory: "/Users/test/.config/systemd/user",
      serviceFilePath: installPath
    });

    await getServiceGenerator(io)?.install(definitionFixture());

    assert.equal(writes.length, 1);

    const unitWrite = firstOf(writes, "unit write");

    assert.equal(unitWrite.path, installPath);
    assert.match(unitWrite.content, /^\[Unit\]/m);
    assert.match(unitWrite.content, /^ExecStart="\/usr\/local\/bin\/node" "\/usr\/local\/lib\/prismcast\/dist\/index\.js"$/m);
    assert.match(unitWrite.content, /^Restart=always$/m);
    assert.match(unitWrite.content, /^Environment="PRISMCAST_SERVICE=1"$/m);
    assert.match(unitWrite.content, /^WantedBy=default\.target$/m);

    const execSequence = calls.filter((c) => c.method === "execFile").map((c) => c.args.slice(1).join(" "));

    assert.deepEqual(execSequence, [
      "--user daemon-reload",
      "--user enable prismcast.service",
      "--user restart prismcast.service"
    ]);
  });

  test("install: percent specifiers and dollar variables are doubled in the emitted ExecStart line", async () => {

    /* The doubling is invisible to a write-then-parse round trip: the reader collapses %% and $$ back to single characters and passes a lone % or $ through
     * unchanged, so identity holds whether or not the writer doubles at all. Only systemd itself observes the difference, expanding %n as a specifier and
     * $PATH as a variable inside a quoted field. That makes the emitted text the only place this half of the grammar can be asserted.
     */
    const installPath = "/Users/test/.config/systemd/user/prismcast.service";
    const { io, writes } = makeFakeIO({

      execFile: execFileFromMap({

        "systemctl --user daemon-reload": { stdout: "" },
        "systemctl --user enable prismcast.service": { stdout: "" },
        "systemctl --user restart prismcast.service": { stdout: "" }
      }),
      platform: "linux",
      serviceFileDirectory: "/Users/test/.config/systemd/user",
      serviceFilePath: installPath
    });

    await getServiceGenerator(io)?.install(definitionFixture({ entryPoint: "/opt/$HOME/100%/index.js", nodePath: "/opt/%n/$PATH/node" }));

    assert.match(firstOf(writes, "unit write").content, /^ExecStart="\/opt\/%%n\/\$\$PATH\/node" "\/opt\/\$\$HOME\/100%%\/index\.js"$/m,
      "both specifiers and both variables are doubled inside the quoted fields");
  });

  /* The unit writer and the stale-path reader are inverses, so every case below must come back out of getServicePaths exactly as it went in. The corpus covers
   * each character the quoting grammar treats specially: the space that motivates quoting at all, the backslash and the double quote that escape with a
   * backslash, and the percent specifier and dollar variable that escape by doubling. The last case is the corner where a path ends in a backslash - a reader
   * that unescaped after scanning rather than during it would mistake the closing quote for an escaped one and swallow the rest of the line. One test per case
   * so a grammar hole names the character class it belongs to instead of hiding behind whichever fixture happened to run first.
   */
  const roundTripCases = [
    { label: "a space in both paths", paths: { entryPoint: "/opt/My Apps/prismcast/dist/index.js", nodePath: "/opt/My Apps/node" } },
    { label: "backslash separators", paths: { entryPoint: "C:\\prismcast\\dist\\index.js", nodePath: "C:\\Program Files\\nodejs\\node.exe" } },
    { label: "an embedded double quote", paths: { entryPoint: "/opt/pris\"mcast/dist/index.js", nodePath: "/opt/no\"de/node" } },
    { label: "a percent specifier", paths: { entryPoint: "/opt/100%/dist/index.js", nodePath: "/opt/%n/node" } },
    { label: "a dollar variable", paths: { entryPoint: "/opt/$HOME/dist/index.js", nodePath: "/opt/$PATH/node" } },
    { label: "a trailing backslash against the closing quote", paths: { entryPoint: "/opt/prismcast/dist/index.js", nodePath: "/opt/ends in a backslash\\" } },
    { label: "a backslash immediately followed by a quote", paths: { entryPoint: "/opt/prismcast/dist/index.js", nodePath: "/opt/a\\\"b/node" } }
  ];

  for(const item of roundTripCases) {

    test("install: the ExecStart line round-trips through getServicePaths with " + item.label, async () => {

      const installPath = "/Users/test/.config/systemd/user/prismcast.service";
      const { io, writes } = makeFakeIO({

        execFile: execFileFromMap({

          "systemctl --user daemon-reload": { stdout: "" },
          "systemctl --user enable prismcast.service": { stdout: "" },
          "systemctl --user restart prismcast.service": { stdout: "" }
        }),
        platform: "linux",
        serviceFileDirectory: "/Users/test/.config/systemd/user",
        serviceFilePath: installPath
      });

      await getServiceGenerator(io)?.install(definitionFixture(item.paths));

      const { io: readerIo } = makeFakeIO({

        existing: { [installPath]: true },
        files: { [installPath]: firstOf(writes, "unit write").content },
        platform: "linux",
        serviceFilePath: installPath
      });

      assert.deepEqual(getServicePaths(readerIo), item.paths, "write-then-parse identity");
    });
  }

  test("isRunning: returns true when systemctl is-active reports 'active'", async () => {

    const { io } = makeFakeIO({

      execFile: execFileFromMap({ "systemctl --user is-active prismcast.service": { stdout: "active\n" } }),
      platform: "linux"
    });

    assert.equal(await getServiceGenerator(io)?.isRunning(), true);
  });

  test("isRunning: returns false when systemctl is-active throws (inactive returns non-zero)", async () => {

    const { io } = makeFakeIO({

      execFile: execFileFromMap({ "systemctl --user is-active prismcast.service": { shouldThrow: true, stdout: "inactive" } }),
      platform: "linux"
    });

    assert.equal(await getServiceGenerator(io)?.isRunning(), false);
  });

  test("uninstall: stops, disables, removes the unit, and reloads the daemon", async () => {

    const installPath = "/Users/test/.config/systemd/user/prismcast.service";
    const { calls, io } = makeFakeIO({

      execFile: execFileFromMap({


        "systemctl --user daemon-reload": { stdout: "" },
        "systemctl --user disable prismcast.service": { stdout: "" },
        "systemctl --user stop prismcast.service": { stdout: "" }
      }),
      platform: "linux",
      serviceFilePath: installPath
    });

    await getServiceGenerator(io)?.uninstall();

    const sequence = calls.filter((c) => (c.method === "execFile") || (c.method === "rm")).map((c) => ({ args: c.args, method: c.method }));
    const stop = nthOf(sequence, 0, "uninstall step");
    const disable = nthOf(sequence, 1, "uninstall step");
    const removeUnit = nthOf(sequence, 2, "uninstall step");
    const reload = nthOf(sequence, 3, "uninstall step");

    assert.equal(stop.method, "execFile");
    assert.deepEqual(stop.args.slice(1), [ "--user", "stop", "prismcast.service" ]);
    assert.deepEqual(disable.args.slice(1), [ "--user", "disable", "prismcast.service" ]);
    assert.equal(removeUnit.method, "rm");
    assert.deepEqual(removeUnit.args, [ installPath, { force: true } ]);
    assert.deepEqual(reload.args.slice(1), [ "--user", "daemon-reload" ]);
  });
});

/* runAndSurfaceStderr is the diagnostic-enrichment wrapper that all execFile-throwing methods route through (launchctl, systemctl, powershell.exe). When a child
 * exits non-zero, runAndSurfaceStderr reads .stderr off the thrown Error - real promisified execFile produces it as either a utf8 string (default) or a Buffer
 * (encoding: "buffer") - trims it, and folds it into a new Error whose message starts with the supplied description. The original Error becomes the new
 * Error's .cause so programmatic consumers retain access to the structured failure details (.code, .signal, .stdout, .stderr).
 *
 * The tests below assert every branch of that contract: string stderr surfaced, Buffer stderr surfaced via .toString("utf8"), empty stderr falling back to the
 * error.message, and the .cause chain preserved. We exercise this through systemd start/stop which are thin wrappers over runAndSurfaceStderr-execFile pairs.
 */
describe("runAndSurfaceStderr - error message enrichment", () => {

  test("surfaces a string stderr verbatim into the rejected Error's message", async () => {

    // Real execFile with default utf8 encoding produces stderr as a string. The wrapper reads it directly and folds it into the surfaced message.
    const { io } = makeFakeIO({

      execFile: async () => {

        throw makeExecFileError("Command failed", "Failed to start prismcast.service: Unit not found.", "");
      },
      platform: "linux"
    });

    await assert.rejects(
      () => getServiceGenerator(io)?.start() ?? Promise.resolve(),
      (error: Error): boolean => {

        assert.match(error.message, /^systemctl start failed: /);
        assert.match(error.message, /Failed to start prismcast\.service: Unit not found\./);

        return true;
      }
    );
  });

  test("surfaces a Buffer stderr via toString('utf8')", async () => {

    // The encoding: "buffer" execFile config produces Buffer-shaped stderr. runAndSurfaceStderr's Buffer.isBuffer branch decodes it to utf8 before folding.
    const { io } = makeFakeIO({

      execFile: async () => {

        throw makeExecFileError("Command failed", Buffer.from("Failed to stop prismcast.service: Process not running.", "utf8"), Buffer.alloc(0));
      },
      platform: "linux"
    });

    await assert.rejects(
      () => getServiceGenerator(io)?.stop() ?? Promise.resolve(),
      (error: Error): boolean => {

        assert.match(error.message, /^systemctl stop failed: /);
        assert.match(error.message, /Failed to stop prismcast\.service: Process not running\./);

        return true;
      }
    );
  });

  test("falls back to error.message when stderr is empty", async () => {

    // When the child exited non-zero but wrote nothing to stderr, the wrapper falls back to the original Error.message so the surfaced error still carries
    // some context. Without this fallback, users would see "launchctl unload failed: " with no further detail.
    const { io } = makeFakeIO({

      execFile: async () => {

        throw makeExecFileError("Underlying execFile message", "", "");
      },
      platform: "linux"
    });

    await assert.rejects(
      () => getServiceGenerator(io)?.start() ?? Promise.resolve(),
      (error: Error): boolean => {

        assert.match(error.message, /^systemctl start failed: Underlying execFile message$/);

        return true;
      }
    );
  });

  test("trims surrounding whitespace from stderr before folding it into the message", async () => {

    // Real systemctl/launchctl frequently emit a trailing newline. The wrapper trims it so the surfaced message is clean.
    const { io } = makeFakeIO({

      execFile: async () => {

        throw makeExecFileError("Command failed", "  Failed to start prismcast.service.\n\n", "");
      },
      platform: "linux"
    });

    await assert.rejects(
      () => getServiceGenerator(io)?.start() ?? Promise.resolve(),
      (error: Error): boolean => {

        // Note the absence of leading whitespace and trailing newline.
        assert.match(error.message, /^systemctl start failed: Failed to start prismcast\.service\.$/);

        return true;
      }
    );
  });

  test("preserves the original Error as the surfaced Error's .cause for programmatic access", async () => {

    // Programmatic consumers sometimes need to inspect the structured failure (.code, .signal). The wrapper attaches the original via Error.cause so that data
    // is reachable through the cause chain even after the surfacing wrap.
    const original = makeExecFileError("Original execFile failure", "stderr text", "stdout text");
    const { io } = makeFakeIO({

      execFile: async () => {

        throw original;
      },
      platform: "linux"
    });

    await assert.rejects(
      () => getServiceGenerator(io)?.start() ?? Promise.resolve(),
      (error: Error): boolean => {

        assert.equal(error.cause, original, "cause chain points back at the original execFile error");

        return true;
      }
    );
  });
});

