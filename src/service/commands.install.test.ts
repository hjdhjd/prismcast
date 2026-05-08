/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.install.test.ts: Unit tests for the install and uninstall handlers in commands.ts under literal-context wiring (synthetic ServiceContext that captures
 * stdout/stderr and returns whatever generator state the test wants). Default-context tests for the dispatcher, not-installed branches, and smoke routing live
 * in commands.test.ts; lifecycle handlers (start/stop/restart) plus printServiceUsage literal and handleServiceCommand literal live in commands.lifecycle.test.ts;
 * handleStatus literal lives in commands.status.test.ts.
 */
import { describe, test } from "node:test";
import { handleInstall, handleUninstall } from "./commands.ts";
import { makeContextHarness, makeFakeGenerator } from "./commands.helpers.ts";
import assert from "node:assert/strict";

describe("handleInstall (literal context)", () => {

  test("happy path: fresh install on macOS calls generator.install with the definition and returns 0", async () => {

    const { context, generator, stdout } = makeContextHarness();

    const code = await handleInstall(false, context);

    assert.equal(code, 0);
    assert.equal(generator?.installs.length, 1, "generator.install was called exactly once");
    assert.match(stdout.join("\n"), /Installing PrismCast service/);
    assert.match(stdout.join("\n"), /Service installed successfully/);
    assert.match(stdout.join("\n"), /service is now running and will start automatically at login/);
  });

  test("rejects already-installed without --force, returns 1, does not call install", async () => {

    const generator = makeFakeGenerator({ installed: true });
    const { context, stderr } = makeContextHarness({ generator });

    const code = await handleInstall(false, context);

    assert.equal(code, 1);
    assert.equal(generator.installs.length, 0, "generator.install was not called");
    assert.match(stderr.join("\n"), /already installed/);
    assert.match(stderr.join("\n"), /--force/);
  });

  test("with --force, reinstalls even when already installed", async () => {

    const generator = makeFakeGenerator({ installed: true });
    const { context, stdout } = makeContextHarness({ generator });

    const code = await handleInstall(true, context);

    assert.equal(code, 0);
    assert.equal(generator.installs.length, 1, "generator.install fired despite already-installed");
    assert.match(stdout.join("\n"), /Service installed successfully/);
  });

  test("returns 1 with stderr message when the platform is unsupported (no generator)", async () => {

    const { context, stderr } = makeContextHarness({ generator: null, platform: "openbsd" });

    const code = await handleInstall(false, context);

    assert.equal(code, 1);
    assert.match(stderr.join("\n"), /not supported on this platform \(openbsd\)/);
  });

  test("returns 1 when generator.install throws and surfaces the error message", async () => {

    const generator = makeFakeGenerator({ installShouldThrow: new Error("plist write failed") });
    const { context, stderr } = makeContextHarness({ generator });

    const code = await handleInstall(false, context);

    assert.equal(code, 1);
    assert.match(stderr.join("\n"), /Failed to install service/);
    assert.match(stderr.join("\n"), /plist write failed/);
  });

  test("emits systemd-flavored instructions when generator.serviceManager is 'systemd'", async () => {

    const generator = makeFakeGenerator({ platform: "linux", serviceManager: "systemd" });
    const { context, stdout } = makeContextHarness({ generator, platform: "linux" });

    const code = await handleInstall(false, context);

    assert.equal(code, 0);
    assert.match(stdout.join("\n"), /journalctl --user -u prismcast -f/, "systemd-specific journal hint shown");
  });

  test("emits Windows-flavored instructions when generator.serviceManager is 'windows-scheduler'", async () => {

    // The Platform type is "darwin" | "linux" | "windows" - not the Node-native "win32" string. The platform helpers normalize to the friendlier name.
    const generator = makeFakeGenerator({ platform: "windows", serviceManager: "windows-scheduler" });
    const { context, stdout } = makeContextHarness({ generator, platform: "windows" });

    const code = await handleInstall(false, context);

    assert.equal(code, 0);
    assert.match(stdout.join("\n"), /type .*prismcast\.log/, "Windows 'type' command hint shown");
  });
});

describe("handleUninstall (literal context)", () => {

  test("happy path: installed service is uninstalled cleanly", async () => {

    const generator = makeFakeGenerator({ installed: true });
    const { context, stdout } = makeContextHarness({ generator });

    const code = await handleUninstall(context);

    assert.equal(code, 0);
    assert.equal(generator.uninstallCalls, 1);
    assert.match(stdout.join("\n"), /Service uninstalled successfully/);
    assert.match(stdout.join("\n"), /Configuration and data files .* preserved/);
  });

  test("returns 1 when the service is not installed", async () => {

    const { context, generator, stderr } = makeContextHarness();

    const code = await handleUninstall(context);

    assert.equal(code, 1);
    assert.equal(generator?.uninstallCalls, 0);
    assert.match(stderr.join("\n"), /not installed/);
  });

  test("returns 1 when generator.uninstall throws and surfaces the error message", async () => {

    const generator = makeFakeGenerator({ installed: true, uninstallShouldThrow: new Error("launchctl unload failed") });
    const { context, stderr } = makeContextHarness({ generator });

    const code = await handleUninstall(context);

    assert.equal(code, 1);
    assert.match(stderr.join("\n"), /Failed to uninstall service/);
    assert.match(stderr.join("\n"), /launchctl unload failed/);
  });
});

