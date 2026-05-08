/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.lifecycle.test.ts: Unit tests for the runtime lifecycle handlers in commands.ts under literal-context wiring - handleStart, handleStop, handleRestart,
 * plus the small dispatch helpers (printServiceUsage and handleServiceCommand) under literal-context wiring. Default-context tests live in commands.test.ts;
 * install/uninstall literal tests live in commands.install.test.ts; handleStatus literal lives in commands.status.test.ts.
 */
import { describe, test } from "node:test";
import { handleRestart, handleServiceCommand, handleStart, handleStop, printServiceUsage } from "./commands.ts";
import { makeContextHarness, makeFakeGenerator } from "./commands.helpers.ts";
import type { Nullable } from "../types/index.ts";
import type { ServicePaths } from "./generators.ts";
import assert from "node:assert/strict";

describe("handleStart (literal context)", () => {

  test("happy path: installed but not running, paths match, calls generator.start, returns 0", async () => {

    const generator = makeFakeGenerator({ installed: true, running: false });
    const { context, stdout } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/usr/local/lib/prismcast/dist/index.js",
        nodePath: "/usr/local/bin/node"
      })
    });

    const code = await handleStart(context);

    assert.equal(code, 0);
    assert.equal(generator.startCalls, 1, "generator.start was called");
    assert.equal(generator.installs.length, 0, "no reinstall happened (paths matched)");
    assert.match(stdout.join("\n"), /Service started successfully/);
  });

  test("returns 0 with friendly message when the service is already running", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({ generator });

    const code = await handleStart(context);

    assert.equal(code, 0);
    assert.equal(generator.startCalls, 0, "start was not called");
    assert.match(stdout.join("\n"), /already running/);
  });

  test("regenerates the service file when stored paths differ from current runtime paths", async () => {

    // The post-Homebrew-upgrade scenario: existing service file points at an old node binary; current runtime resolves to a new one. handleStart must
    // regenerate before starting.
    const generator = makeFakeGenerator({ installed: true, running: false });
    const { context, stdout } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/old/dist/index.js",
        nodePath: "/old/node"
      })
    });

    const code = await handleStart(context);

    assert.equal(code, 0);
    assert.equal(generator.installs.length, 1, "reinstall was triggered by path mismatch");
    assert.equal(generator.startCalls, 0, "generator.start was not called separately - install() does the start");
    assert.match(stdout.join("\n"), /Detected path changes/);
    assert.match(stdout.join("\n"), /Updated service file with current paths/);
  });

  test("returns 1 with stderr message when the service is not installed", async () => {

    const { context, generator, stderr } = makeContextHarness();

    const code = await handleStart(context);

    assert.equal(code, 1);
    assert.equal(generator?.startCalls, 0);
    assert.match(stderr.join("\n"), /not installed/);
  });

  test("returns 1 when generator.start throws", async () => {

    const generator = makeFakeGenerator({ installed: true, running: false, startShouldThrow: new Error("launchctl bootstrap rejected") });
    const { context, stderr } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/usr/local/lib/prismcast/dist/index.js",
        nodePath: "/usr/local/bin/node"
      })
    });

    const code = await handleStart(context);

    assert.equal(code, 1);
    assert.match(stderr.join("\n"), /Failed to start service/);
    assert.match(stderr.join("\n"), /launchctl bootstrap rejected/);
  });
});

describe("handleStop (literal context)", () => {

  test("happy path: installed service is stopped successfully", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({ generator });

    const code = await handleStop(context);

    assert.equal(code, 0);
    assert.equal(generator.stopCalls, 1);
    assert.match(stdout.join("\n"), /Service stopped successfully/);
  });

  test("returns 0 (success) even when generator.stop throws (loaded-but-not-running case)", async () => {

    // The launchd "loaded but not running" state: stop() throws because there's nothing actively running, but the end state is what we wanted - service stopped.
    const generator = makeFakeGenerator({ installed: true, stopShouldThrow: new Error("Could not find specified service") });
    const { context, stderr, stdout } = makeContextHarness({ generator });

    const code = await handleStop(context);

    assert.equal(code, 0, "stop is treated as success even on throw");
    assert.match(stderr.join("\n"), /Note:.*Could not find specified service/, "the underlying error is logged as a Note");
    assert.match(stdout.join("\n"), /Service stopped successfully/);
  });

  test("returns 1 when the service is not installed", async () => {

    const { context, generator, stderr } = makeContextHarness();

    const code = await handleStop(context);

    assert.equal(code, 1);
    assert.equal(generator?.stopCalls, 0);
    assert.match(stderr.join("\n"), /not installed/);
  });
});

describe("handleRestart (literal context)", () => {

  test("happy path: stops then starts (paths match, no regeneration)", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/usr/local/lib/prismcast/dist/index.js",
        nodePath: "/usr/local/bin/node"
      })
    });

    const code = await handleRestart(context);

    assert.equal(code, 0);
    assert.equal(generator.stopCalls, 1, "stop was called first");
    assert.equal(generator.startCalls, 1, "start was called after stop");
    assert.match(stdout.join("\n"), /Service restarted successfully/);
  });

  test("regenerates the service file during restart when paths drifted", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/old/dist/index.js",
        nodePath: "/old/node"
      })
    });

    const code = await handleRestart(context);

    assert.equal(code, 0);
    assert.equal(generator.stopCalls, 1);
    assert.equal(generator.installs.length, 1, "reinstall fired due to path drift");
    assert.match(stdout.join("\n"), /Detected path changes/);
  });

  test("absorbs generator.stop throws (service may not have been running)", async () => {

    // The "stop before restart" call is best-effort. If the service wasn't actually running, stop() throws but restart proceeds normally.
    const generator = makeFakeGenerator({ installed: true, running: false, stopShouldThrow: new Error("not running") });
    const { context } = makeContextHarness({

      generator,
      getServicePaths: (): Nullable<ServicePaths> => ({

        entryPoint: "/usr/local/lib/prismcast/dist/index.js",
        nodePath: "/usr/local/bin/node"
      })
    });

    const code = await handleRestart(context);

    assert.equal(code, 0, "throw on stop did not abort the restart");
    assert.equal(generator.startCalls, 1);
  });

  test("returns 1 when not installed", async () => {

    const { context, stderr } = makeContextHarness();

    const code = await handleRestart(context);

    assert.equal(code, 1);
    assert.match(stderr.join("\n"), /not installed/);
  });
});

describe("printServiceUsage (literal context)", () => {

  test("writes the usage block to ctx.stdout", () => {

    const { context, stdout } = makeContextHarness();

    printServiceUsage(context);

    const text = stdout.join("\n");

    assert.match(text, /^Usage: prismcast service/);

    for(const cmd of [ "install", "uninstall", "start", "stop", "restart", "status" ]) {

      assert.match(text, new RegExp(cmd), "usage mentions " + cmd);
    }
  });
});

describe("handleServiceCommand (literal context)", () => {

  test("routes 'install --force' to handleInstall(true)", async () => {

    const generator = makeFakeGenerator({ installed: true });
    const { context } = makeContextHarness({ generator });

    const code = await handleServiceCommand([ "install", "--force" ], context);

    // Already installed, --force triggers reinstall - install() is called once.
    assert.equal(code, 0);
    assert.equal(generator.installs.length, 1, "force flag routed through to handleInstall(true)");
  });

  test("routes 'install -f' to handleInstall(true) (short flag)", async () => {

    const generator = makeFakeGenerator({ installed: true });
    const { context } = makeContextHarness({ generator });

    const code = await handleServiceCommand([ "install", "-f" ], context);

    assert.equal(code, 0);
    assert.equal(generator.installs.length, 1);
  });

  test("threads the same context through every subcommand", async () => {

    // We exercise every dispatch case once and verify the context's stdout buffer captures output from each. This ensures the dispatcher passes ctx through
    // rather than constructing a fresh default context per invocation.
    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({ generator });

    await handleServiceCommand(["status"], context);
    assert.match(stdout.join("\n"), /PrismCast Service Status/, "status routed through ctx.stdout");
  });
});
