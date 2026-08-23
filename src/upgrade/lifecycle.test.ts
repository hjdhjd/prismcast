/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lifecycle.test.ts: Unit tests for the upgrade-lifecycle strategy registry, the platform dispatcher, and the per-strategy behavior. The strategies are pure
 * functions over an UpgradeLifecycleContext that resolve with an UpgradeStep, so tests inject inline contexts that capture every side-effect (runCommand
 * invocations, spawnDetached calls) and assert against the awaited outcome. The Windows handoff strategy's PowerShell command string is asserted to contain the
 * structural elements the helper needs (positional parameters quoted correctly, the script body, etc.) without locking the exact whitespace - we want
 * regressions in argument order to fail, not stylistic rewrites of the helper.
 */
import { UPGRADE_LIFECYCLES, performUpgrade, selectLifecycle } from "./lifecycle.ts";
import type { UpgradeLifecycleContext, UpgradeRunResult } from "./lifecycle.ts";
import { describe, test } from "node:test";
import type { InstallInfo } from "./detection.ts";
import assert from "node:assert/strict";

interface CapturedLifecycle {

  context: UpgradeLifecycleContext;
  runCalls: { cmd: string; cwd: string | undefined; timeoutMs: number | undefined }[];
  spawnCalls: { command: string; args: readonly string[] }[];
}

interface ContextOverrides {

  readonly commandTimeoutMs?: number;
  readonly parentPid?: number;
  readonly platform?: NodeJS.Platform;
  readonly runCommand?: (cmd: string, options: { readonly cwd?: string; readonly timeoutMs?: number }) => UpgradeRunResult;
  readonly serviceTaskName?: string;
  readonly upgradeLogPath?: string;
}

/* makeLifecycleContext builds an UpgradeLifecycleContext literal with sensible defaults. Every field is captured for inspection by tests; the runner returns
 * success by default and spawnDetached pushes its arguments into an array rather than actually launching a child process.
 */
function makeLifecycleContext(overrides: ContextOverrides = {}): CapturedLifecycle {

  const runCalls: { cmd: string; cwd: string | undefined; timeoutMs: number | undefined }[] = [];
  const spawnCalls: { command: string; args: readonly string[] }[] = [];
  const runCommand = overrides.runCommand ?? ((): UpgradeRunResult => ({ success: true }));

  const context: UpgradeLifecycleContext = {

    commandTimeoutMs: overrides.commandTimeoutMs,
    parentPid: overrides.parentPid ?? 4242,
    platform: overrides.platform ?? "linux",
    runCommand: async (cmd: string, options: { readonly cwd?: string; readonly timeoutMs?: number }): Promise<UpgradeRunResult> => {

      runCalls.push({ cmd, cwd: options.cwd, timeoutMs: options.timeoutMs });

      return runCommand(cmd, options);
    },
    serviceTaskName: overrides.serviceTaskName ?? "PrismCast",
    spawnDetached: (command: string, args: readonly string[]): void => {

      spawnCalls.push({ args, command });
    },
    upgradeLogPath: overrides.upgradeLogPath ?? "/var/log/prismcast/upgrade.log"
  };

  return { context, runCalls, spawnCalls };
}

function makeInstallInfo(overrides: Partial<InstallInfo> = {}): InstallInfo {

  return {

    displayName: "npm (global)",
    method: "npm-global",
    upgradeCommand: "npm install -g prismcast@latest",
    upgradeable: true,
    ...overrides
  } as InstallInfo;
}

describe("UPGRADE_LIFECYCLES registry", () => {

  test("contains exactly the two expected strategies in priority order", () => {

    // Order encodes priority. POSIX serves more platforms (darwin + linux) and acts as the implicit fallback for unknown platforms; Windows is the
    // narrow-platform specialist. Locking the order ensures a future contributor adding a third strategy thinks about where in the dispatcher's walk it should
    // sit.
    const ids = UPGRADE_LIFECYCLES.map((s) => s.id);

    assert.deepEqual(ids, [ "posix-in-process", "windows-handoff" ]);
  });

  test("every strategy declares id, platforms (non-empty), and perform (function)", () => {

    // Locks the UpgradeLifecycleStrategy contract: a future strategy that forgets one of the required fields fails this test before any caller notices.
    for(const strategy of UPGRADE_LIFECYCLES) {

      assert.equal(typeof strategy.id, "string", "strategy must have a string id");
      assert.ok(strategy.id.length > 0, "strategy id must be non-empty");
      assert.ok(Array.isArray(strategy.platforms), "strategy must declare a platforms array");
      assert.ok(strategy.platforms.length > 0, "strategy '" + strategy.id + "' must declare at least one platform");
      assert.equal(typeof strategy.perform, "function", "strategy '" + strategy.id + "' must declare a perform function");
    }
  });

  test("platforms across strategies do not overlap", () => {

    // The dispatcher walks the registry in order and picks the FIRST match. Overlap would make the registry's iteration order silently determine which
    // strategy wins, in a way the strategy authors might not expect. Locking non-overlap catches a future strategy that accidentally claims a platform another
    // strategy already serves.
    const seen = new Set<NodeJS.Platform>();

    for(const strategy of UPGRADE_LIFECYCLES) {

      for(const platform of strategy.platforms) {

        assert.equal(seen.has(platform), false, "platform '" + platform + "' is claimed by more than one strategy");
        seen.add(platform);
      }
    }
  });
});

describe("selectLifecycle", () => {

  test("returns the POSIX in-process strategy on darwin", () => {

    assert.equal(selectLifecycle("darwin").id, "posix-in-process");
  });

  test("returns the POSIX in-process strategy on linux", () => {

    assert.equal(selectLifecycle("linux").id, "posix-in-process");
  });

  test("returns the Windows handoff strategy on win32", () => {

    assert.equal(selectLifecycle("win32").id, "windows-handoff");
  });

  test("falls back to the POSIX in-process strategy for unknown platforms", () => {

    // freebsd, sunos, aix, openbsd, etc. - any platform PrismCast has not specifically engineered for. The dispatcher falls back to the POSIX strategy because
    // it serves the broadest platform set; the dispatcher being total (never throwing on a one-off platform name) is the property we lock.
    assert.equal(selectLifecycle("freebsd").id, "posix-in-process");
    assert.equal(selectLifecycle("sunos").id, "posix-in-process");
    assert.equal(selectLifecycle("openbsd").id, "posix-in-process");
  });
});

describe("performUpgrade with the POSIX in-process strategy", () => {

  test("calls runCommand with the install info's upgradeCommand and forwards packageDir as cwd", async () => {

    // Locks the POSIX runner's wire-up: the lifecycle is a thin pass-through over runCommand, so the runner sees exactly the InstallInfo's upgradeCommand and
    // the packageDir (if any) as cwd. A future change that started rewriting the command (e.g., wrapping it in `sudo`) would fail this test deliberately.
    const cap = makeLifecycleContext({ platform: "darwin" });
    const info = makeInstallInfo({

      method: "npm-local",
      packageDir: "/Users/me/my-app",
      upgradeCommand: "npm install prismcast@latest"
    });

    const step = await performUpgrade(cap.context, info);

    assert.deepEqual(step, { kind: "ran", success: true });
    assert.equal(cap.runCalls.length, 1);

    const call = cap.runCalls[0];

    assert.ok(call);
    assert.equal(call.cmd, "npm install prismcast@latest");
    assert.equal(call.cwd, "/Users/me/my-app");
  });

  test("propagates the runner's failure outcome through the UpgradeStep", async () => {

    const cap = makeLifecycleContext({

      platform: "linux",
      runCommand: () => ({ success: false })
    });

    const step = await performUpgrade(cap.context, makeInstallInfo());

    assert.deepEqual(step, { kind: "ran", success: false });
  });

  test("forwards the context's command deadline to the runner", async () => {

    // The deadline is caller policy, not a property of the platform: the web UI bounds the command so a stalled install cannot hold an HTTP request open, while
    // the CLI runs unbounded. The strategy is the pass-through that carries the caller's choice to the runner, so the wire-up is pinned here.
    const cap = makeLifecycleContext({ commandTimeoutMs: 120000, platform: "darwin" });

    await performUpgrade(cap.context, makeInstallInfo());

    assert.equal(cap.runCalls[0]?.timeoutMs, 120000);
  });

  test("forwards no deadline when the context declares none", async () => {

    // The CLI's context omits the deadline entirely. Passing undefined through rather than substituting a default keeps "unbounded" expressible, which is what
    // an interactive upgrade with a user watching the terminal wants.
    const cap = makeLifecycleContext({ platform: "linux" });

    await performUpgrade(cap.context, makeInstallInfo());

    assert.equal(cap.runCalls[0]?.timeoutMs, undefined);
  });

  test("resolves rather than returning the outcome directly", async () => {

    // The port is asynchronous so a caller sharing its event loop with the HTTP server keeps serving requests for the length of a package install. A regression
    // to a synchronous return would still satisfy the awaited assertions above (await on a non-promise is a no-op), so the promise itself is pinned here.
    const cap = makeLifecycleContext({ platform: "darwin" });
    const pending = performUpgrade(cap.context, makeInstallInfo());

    assert.equal(typeof (pending as { then?: unknown }).then, "function", "performUpgrade hands back a thenable");
    assert.deepEqual(await pending, { kind: "ran", success: true });
  });

  test("does not invoke spawnDetached on POSIX", async () => {

    // Negative: the POSIX strategy is in-process by design. A regression that wired spawnDetached into the POSIX path would silently change semantics; lock
    // that the helper-spawn primitive is only touched by the Windows strategy.
    const cap = makeLifecycleContext({ platform: "darwin" });

    await performUpgrade(cap.context, makeInstallInfo());

    assert.equal(cap.spawnCalls.length, 0);
  });
});

describe("performUpgrade with the Windows handoff strategy", () => {

  test("returns a handed-off UpgradeStep with the context's log path", async () => {

    const cap = makeLifecycleContext({

      platform: "win32",
      upgradeLogPath: "C:\\Users\\jp\\.prismcast\\upgrade.log"
    });

    const step = await performUpgrade(cap.context, makeInstallInfo());

    assert.deepEqual(step, { kind: "handed-off", logPath: "C:\\Users\\jp\\.prismcast\\upgrade.log" });
  });

  test("spawns powershell.exe exactly once with the documented arguments and a single -Command string", async () => {

    // The launch arguments are the masterclass-stable contract: -NoProfile and -NonInteractive for a clean shell, -ExecutionPolicy Bypass for installer-style
    // scripted scenarios, -WindowStyle Hidden so no console flashes, then -Command with the composed helper script. Locking the argument list (and the
    // command name "powershell.exe") protects the entire spawn surface from accidental drift.
    const cap = makeLifecycleContext({ platform: "win32" });

    await performUpgrade(cap.context, makeInstallInfo());

    assert.equal(cap.spawnCalls.length, 1, "Windows handoff must spawn exactly one detached child");
    assert.equal(cap.spawnCalls[0]?.command, "powershell.exe");

    const args = cap.spawnCalls[0].args;

    assert.deepEqual(args.slice(0, 7), [

      "-NoProfile",
      "-NonInteractive",
      "-ExecutionPolicy", "Bypass",
      "-WindowStyle", "Hidden",
      "-Command"
    ], "the first seven argv entries are fixed flags");
    assert.equal(args.length, 8, "exactly the seven fixed flags plus the single -Command payload");
    assert.equal(typeof args[7], "string");
  });

  test("the -Command payload embeds the parent PID, the service task name, the upgrade command, the working directory, and the log path", async () => {

    // The helper script reads five positional parameters: parent PID, task name, upgrade command, working dir, log path. The composed -Command string must
    // expose all five so the helper can do its job. We assert each one appears in the payload via its single-quote-escaped literal form.
    const cap = makeLifecycleContext({

      parentPid: 12345,
      platform: "win32",
      serviceTaskName: "PrismCast",
      upgradeLogPath: "C:\\Users\\jp\\.prismcast\\upgrade.log"
    });

    await performUpgrade(cap.context, makeInstallInfo({

      method: "npm-local",
      packageDir: "C:\\Users\\jp\\my-app",
      upgradeCommand: "npm install prismcast@latest"
    }));

    const spawnCall = cap.spawnCalls[0];

    assert.ok(spawnCall, "Windows handoff must have spawned a child");

    const payload = spawnCall.args[7];

    assert.ok(payload, "the -Command payload must be present as args[7]");

    assert.match(payload, /'12345'/, "parent PID must appear as a single-quoted literal");
    assert.match(payload, /'PrismCast'/, "service task name must appear as a single-quoted literal");
    assert.match(payload, /'npm install prismcast@latest'/, "upgrade command must appear as a single-quoted literal");
    assert.match(payload, /'C:\\Users\\jp\\my-app'/, "working dir must appear as a single-quoted literal");
    assert.match(payload, /'C:\\Users\\jp\\\.prismcast\\upgrade\.log'/, "log path must appear as a single-quoted literal");
  });

  test("encodes an empty working directory as an empty single-quoted literal when the install method has no packageDir", async () => {

    // npm-global on Windows has no packageDir. The handoff helper expects every positional parameter to be present (PowerShell positional binding is
    // order-sensitive), so we serialize the absent value as the empty string rather than dropping the argument. Locking this prevents a future refactor that
    // "saved an argument" from breaking parameter binding in the helper.
    const cap = makeLifecycleContext({ platform: "win32" });

    await performUpgrade(cap.context, makeInstallInfo({

      method: "npm-global",
      packageDir: undefined,
      upgradeCommand: "npm install -g prismcast@latest"
    }));

    const spawnCall = cap.spawnCalls[0];

    assert.ok(spawnCall, "Windows handoff must have spawned a child");

    const payload = spawnCall.args[7];

    assert.ok(payload, "the -Command payload must be present as args[7]");

    assert.match(payload, /''/, "an empty working dir must serialize to an empty single-quoted literal, not be omitted");
  });

  test("escapes embedded single quotes in the upgrade command via doubled single quotes", async () => {

    // PowerShell single-quoted strings are literal except for the embedded-quote rule: a single quote inside a literal is written as two consecutive single
    // quotes. The helper's argument-quoting layer is the only escape surface in the entire handoff path, and this is the rule it must follow.
    const cap = makeLifecycleContext({ platform: "win32" });

    await performUpgrade(cap.context, makeInstallInfo({ upgradeCommand: "echo 'hello' && exit 0" }));

    const spawnCall = cap.spawnCalls[0];

    assert.ok(spawnCall, "Windows handoff must have spawned a child");

    const payload = spawnCall.args[7];

    assert.ok(payload, "the -Command payload must be present as args[7]");

    assert.match(payload, /'echo ''hello'' && exit 0'/, "embedded single quotes must be doubled");
  });

  test("does not invoke runCommand on Windows", async () => {

    // Negative: the Windows strategy hands off; it never runs the upgrade in-process. A regression that called runCommand here would reintroduce the
    // EBUSY-prone in-process path on Windows, which is exactly what this strategy is designed to avoid.
    const cap = makeLifecycleContext({ platform: "win32" });

    await performUpgrade(cap.context, makeInstallInfo());

    assert.equal(cap.runCalls.length, 0);
  });
});
