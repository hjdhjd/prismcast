/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.launchAgent.test.ts: Unit tests for createLaunchdGenerator (the macOS LaunchAgent generator) under the GeneratorIO adapter pattern. Pure leaves
 * (collectServiceEnvironment, buildServiceDefinition, getServiceGenerator dispatch, ServiceDefinition shape contract) live in generators.test.ts; getServicePaths
 * and detectStalePaths variants live in generators.paths.test.ts; Linux-systemd lives in generators.systemd.test.ts; Windows scheduler lives in
 * generators.windowsTask.test.ts.
 */
import { definitionFixture, makeFakeIO } from "./generators.helpers.ts";
import { describe, test } from "node:test";
import { execFileFromMap, firstOf, nthOf } from "../testing.helpers.ts";
import assert from "node:assert/strict";
import { getServiceGenerator } from "./generators.ts";

describe("createLaunchdGenerator (via getServiceGenerator on darwin)", () => {

  test("install: ensures directories, writes a plist, and loads via launchctl", async () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const installDir = "/Users/test/Library/LaunchAgents";
    const { calls, io, writes } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl load -w /Users/test/Library/LaunchAgents/com.prismcast.plist": { stdout: "" }
      }),
      platform: "darwin",
      serviceFileDirectory: installDir,
      serviceFilePath: installPath
    });
    const generator = getServiceGenerator(io);

    assert.notEqual(generator, null);

    await generator?.install(definitionFixture());

    // Verify mkdir for both the install dir and the logs dir.
    const mkdirCalls = calls.filter((c) => c.method === "mkdir").map((c) => c.args[0]);

    assert.ok(mkdirCalls.includes(installDir), "install dir was created");
    assert.ok(mkdirCalls.includes("/Users/test/.prismcast"), "logs dir was created");

    // Exactly one writeFile to the install path; content should be a valid-looking plist.
    assert.equal(writes.length, 1);

    const written = firstOf(writes, "plist write");

    assert.equal(written.path, installPath);
    assert.match(written.content, /<\?xml version="1\.0" encoding="UTF-8"\?>/);
    assert.match(written.content, /<key>Label<\/key>/);
    assert.match(written.content, /<string>com\.github\.hjdhjd\.prismcast<\/string>/, "Label value is the SERVICE_ID");
    assert.match(written.content, /<string>\/usr\/local\/bin\/node<\/string>/, "node path embedded in plist");
    assert.match(written.content, /<key>RunAtLoad<\/key>\s*<true\/>/, "RunAtLoad set");
    assert.match(written.content, /<key>KeepAlive<\/key>\s*<true\/>/, "KeepAlive set");

    // launchctl load -w was invoked.
    const execCalls = calls.filter((c) => c.method === "execFile");
    const firstExec = firstOf(execCalls, "execFile call");

    assert.deepEqual(firstExec.args, [ "launchctl", "load", "-w", installPath ]);
  });

  test("install: falls back to unload+load on initial load failure (reinstall scenario)", async () => {

    // The first load throws (already loaded), so the generator unloads and retries. Here the retry is configured to fail as well, exercising the
    // runAndSurfaceStderr error path so the install rejects with "launchctl load failed".
    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const { calls, io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl load -w /Users/test/Library/LaunchAgents/com.prismcast.plist": { shouldThrow: true },
        "launchctl unload /Users/test/Library/LaunchAgents/com.prismcast.plist": { stdout: "" }
      }),
      platform: "darwin",
      serviceFilePath: installPath
    });
    const generator = getServiceGenerator(io);

    /* The retry path wraps the second load in runAndSurfaceStderr, which reads .stderr off the thrown Error itself (falling back to error.message when absent) and
     * attaches the original Error as the rethrown error's .cause. The retried load rejects because the fake is keyed shouldThrow, so runAndSurfaceStderr surfaces it
     * as "launchctl load failed: <message>", which the assertion below matches.
     */
    await assert.rejects(() => generator?.install(definitionFixture()) ?? Promise.resolve(), /launchctl load failed/);

    // Verify the unload happened between the two load attempts.
    const execCalls = calls.filter((c) => c.method === "execFile");
    const sequence = execCalls.map((c) => c.args.slice(0, 2).join(" "));

    assert.deepEqual(sequence, [
      "launchctl load",
      "launchctl unload",
      "launchctl load"
    ]);
  });

  test("isInstalled: returns true when the service file exists and false otherwise", async () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const installed = makeFakeIO({ existing: { [installPath]: true }, platform: "darwin", serviceFilePath: installPath });
    const notInstalled = makeFakeIO({ existing: {}, platform: "darwin", serviceFilePath: installPath });

    assert.equal(await getServiceGenerator(installed.io)?.isInstalled(), true);
    assert.equal(await getServiceGenerator(notInstalled.io)?.isInstalled(), false);
  });

  test("isRunning: parses launchctl list output and returns true when the PID column is numeric", async () => {

    // launchctl list emits "PID\tStatus\tLabel" rows. The generator finds the row containing the SERVICE_ID and parses the PID.
    const { io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl list": { stdout: "PID\tStatus\tLabel\n12345\t0\tcom.github.hjdhjd.prismcast\nother-row\n" }
      }),
      platform: "darwin"
    });

    assert.equal(await getServiceGenerator(io)?.isRunning(), true);
  });

  test("isRunning: returns false when the row's PID column is '-' (loaded but not running)", async () => {

    const { io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl list": { stdout: "PID\tStatus\tLabel\n-\t0\tcom.github.hjdhjd.prismcast\n" }
      }),
      platform: "darwin"
    });

    assert.equal(await getServiceGenerator(io)?.isRunning(), false);
  });

  test("isRunning: returns false when the SERVICE_ID is not present in the list output", async () => {

    const { io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl list": { stdout: "PID\tStatus\tLabel\n12345\t0\tcom.other.service\n" }
      }),
      platform: "darwin"
    });

    assert.equal(await getServiceGenerator(io)?.isRunning(), false);
  });

  test("uninstall: unloads then removes the plist", async () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const { calls, io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl unload /Users/test/Library/LaunchAgents/com.prismcast.plist": { stdout: "" }
      }),
      platform: "darwin",
      serviceFilePath: installPath
    });

    await getServiceGenerator(io)?.uninstall();

    const sequence = calls.filter((c) => (c.method === "execFile") || (c.method === "rm")).map((c) => ({ args: c.args, method: c.method }));
    const unload = nthOf(sequence, 0, "uninstall step");
    const removePlist = nthOf(sequence, 1, "uninstall step");

    assert.equal(unload.method, "execFile", "first call is launchctl unload");
    assert.deepEqual(unload.args, [ "launchctl", "unload", installPath ]);
    assert.equal(removePlist.method, "rm", "second call removes the plist file");
    assert.deepEqual(removePlist.args, [ installPath, { force: true } ]);
  });

  test("uninstall: ignores errors from launchctl unload (service may not be loaded)", async () => {

    // The unload throws (e.g., service was never loaded). The generator silently absorbs it and continues to the rm step.
    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const { calls, io } = makeFakeIO({

      execFile: execFileFromMap({


        "launchctl unload /Users/test/Library/LaunchAgents/com.prismcast.plist": { shouldThrow: true }
      }),
      platform: "darwin",
      serviceFilePath: installPath
    });

    await assert.doesNotReject(() => getServiceGenerator(io)?.uninstall() ?? Promise.resolve());

    // The rm call still happened.
    assert.ok(calls.some((c) => (c.method === "rm") && (c.args[0] === installPath)));
  });
});

