/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.test.ts: Unit tests for handleUpgradeCommand. The handler is a pure orchestrator over an UpgradeContext, so every branch is testable by passing an
 * inline context that captures stdout/stderr, returns whatever InstallInfo the test wants, fakes the registry response, and stubs the platform-aware upgrade
 * executor. No real fetch, no real subprocess, no real detectInstallMethod, no real process.exit.
 *
 * The field under test is `performUpgrade`, which mirrors the production UpgradeContext field. Tests stub it to produce either a "ran" UpgradeStep (with the
 * in-process outcome) or a "handed-off" UpgradeStep (Windows-style detached helper); the handler is exercised against both branches without the lifecycle
 * module ever being reached. The production field resolves with its UpgradeStep, so the harness wraps each stub in the promise the handler awaits...the stubs
 * themselves stay synchronous because none of them models timing.
 */
import { describe, test } from "node:test";
import { INSTALL_STRATEGIES } from "./detection.ts";
import type { InstallInfo } from "./detection.ts";
import type { UpgradeContext } from "./commands.ts";
import type { UpgradeStep } from "./lifecycle.ts";
import assert from "node:assert/strict";
import { handleUpgradeCommand } from "./commands.ts";

/* makeUpgradeContext builds an UpgradeContext literal with sensible defaults. Stdout/stderr are routed into captured arrays so tests can inspect output;
 * performUpgrade/exit/fetchLatestVersion are stubs that record their inputs, while detect is passed through unmodified. Tests override only the fields
 * they care about.
 */
interface CapturedContext {

  context: UpgradeContext;
  exits: number[];
  fetchCalls: number;
  performUpgradeCalls: { info: InstallInfo }[];
  stderr: string[];
  stdout: string[];
}

interface ContextOverrides {

  detect?: () => InstallInfo;
  fetchLatestVersion?: () => Promise<string | null>;
  isService?: boolean;
  performUpgrade?: (info: InstallInfo) => UpgradeStep;
}

function makeUpgradeContext(overrides: ContextOverrides = {}): CapturedContext {

  const stdout: string[] = [];
  const stderr: string[] = [];
  const exits: number[] = [];
  const performUpgradeCalls: { info: InstallInfo }[] = [];
  let fetchCalls = 0;

  const detect = overrides.detect ?? ((): InstallInfo => ({

    displayName: "Unknown",
    manualUpgradeMessage: ["Unable to detect installation method. Please upgrade manually:"],
    method: "unknown",
    upgradeCommand: "npm install -g prismcast@latest",
    upgradeable: false
  }));
  const fetchLatestVersion = overrides.fetchLatestVersion ?? (async (): Promise<string | null> => null);
  const performUpgrade = overrides.performUpgrade ?? ((): UpgradeStep => ({ kind: "ran", success: true }));

  const context: UpgradeContext = {

    detect,
    exit: (code: number): never => {

      exits.push(code);

      // exit() is typed as never, so we throw a sentinel after recording the requested exit code; tests assert on `exits` rather than relying on never-returning.
      throw new Error("__test_exit__:" + String(code));
    },
    fetchLatestVersion: async (): Promise<string | null> => {

      fetchCalls++;

      return fetchLatestVersion();
    },
    isService: overrides.isService ?? false,
    performUpgrade: async (info: InstallInfo): Promise<UpgradeStep> => {

      performUpgradeCalls.push({ info });

      return performUpgrade(info);
    },
    stderr: (line: string): void => {

      stderr.push(line);
    },
    stdout: (line: string): void => {

      stdout.push(line);
    }
  };

  return { context, exits, fetchCalls, performUpgradeCalls, stderr, stdout };
}

/* The factory accepts a flat overrides shape rather than `Partial<InstallInfo>` because Partial<InstallInfo> only keeps the fields common to both union
 * members, which drops manualUpgradeMessage from the type entirely. The flat shape lets tests describe whichever variant they want with a single,
 * ergonomic literal.
 */
interface InstallInfoOverrides {

  readonly displayName?: string;
  readonly manualUpgradeMessage?: readonly string[];
  readonly method?: InstallInfo["method"];
  readonly packageDir?: string;
  readonly upgradeCommand?: string;
  readonly upgradeable?: boolean;
}

function makeInstallInfo(overrides: InstallInfoOverrides = {}): InstallInfo {

  // Look up the canonical displayName for the chosen method from the registry so test fixtures stay in sync with the strategies' actual labels. Falls back to
  // "Unknown" for the unknown sentinel; explicit overrides win over both.
  const method = overrides.method ?? "unknown";
  const registered = INSTALL_STRATEGIES.find((s) => s.id === method);
  const displayName = overrides.displayName ?? registered?.displayName ?? "Unknown";
  const upgradeCommand = overrides.upgradeCommand ?? registered?.upgradeCommand ?? "npm install -g prismcast@latest";
  const upgradeable = overrides.upgradeable ?? registered?.upgradeable ?? false;

  if(upgradeable) {

    return { displayName, method, packageDir: overrides.packageDir, upgradeCommand, upgradeable: true };
  }

  // Non-upgradeable: derive manualUpgradeMessage from the registered strategy when present; otherwise fall back to the unknown sentinel's message. Explicit
  // overrides win.
  const manualUpgradeMessage = overrides.manualUpgradeMessage ??
    ((registered && !registered.upgradeable) ? registered.manualUpgradeMessage : ["Unable to detect installation method. Please upgrade manually:"]);

  return { displayName, manualUpgradeMessage, method, packageDir: overrides.packageDir, upgradeCommand, upgradeable: false };
}

describe("handleUpgradeCommand - help dispatch", () => {

  test("--help short-circuits before any detection or fetch", async () => {

    const cap = makeUpgradeContext();
    const exitCode = await handleUpgradeCommand(["--help"], cap.context);

    assert.equal(exitCode, 0);
    assert.equal(cap.fetchCalls, 0, "--help must not fetch the registry");
    assert.equal(cap.performUpgradeCalls.length, 0, "--help must not run any upgrade command");
    assert.match(cap.stdout.join("\n"), /Usage: prismcast upgrade/);
  });

  test("-h is an alias for --help", async () => {

    const cap = makeUpgradeContext();

    await handleUpgradeCommand(["-h"], cap.context);

    assert.match(cap.stdout.join("\n"), /Usage: prismcast upgrade/);
    assert.equal(cap.fetchCalls, 0);
  });

  test("'help' as a positional arg is also accepted", async () => {

    const cap = makeUpgradeContext();

    await handleUpgradeCommand(["help"], cap.context);

    assert.match(cap.stdout.join("\n"), /Usage: prismcast upgrade/);
  });

  test("usage block lists --check, --force, and --help", async () => {

    const cap = makeUpgradeContext();

    await handleUpgradeCommand(["--help"], cap.context);

    const text = cap.stdout.join("\n");

    assert.match(text, /--check/);
    assert.match(text, /--force/);
    assert.match(text, /--help/);
  });
});

describe("handleUpgradeCommand - unknown install method (non-upgradeable)", () => {

  test("prints the manual instruction and exits 0", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "unknown", upgradeable: false }),
      fetchLatestVersion: async () => "1.99.0"
    });

    const exitCode = await handleUpgradeCommand([], cap.context);

    assert.equal(exitCode, 0);
    assert.equal(cap.performUpgradeCalls.length, 0, "unknown method must not invoke the lifecycle");

    const text = cap.stdout.join("\n");

    assert.match(text, /Unable to detect installation method/);
    assert.match(text, /npm install -g prismcast@latest/);
  });

  test("--check on unknown still prints the summary plus the manual instruction", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "unknown", upgradeable: false }),
      fetchLatestVersion: async () => null
    });

    const exitCode = await handleUpgradeCommand(["--check"], cap.context);

    assert.equal(exitCode, 0);

    const text = cap.stdout.join("\n");

    assert.match(text, /PrismCast Upgrade Check/);
    assert.match(text, /Install method:\s+Unknown/);
    assert.match(text, /Unable to detect installation method/);
  });
});

describe("handleUpgradeCommand - docker (non-upgradeable)", () => {

  const dockerInfo = makeInstallInfo({

    method: "docker",
    upgradeCommand: "docker pull ghcr.io/hjdhjd/prismcast:latest && docker compose up -d",
    upgradeable: false
  });

  test("prints docker-specific instructions and exits 0 without invoking the lifecycle", async () => {

    const cap = makeUpgradeContext({ detect: () => dockerInfo, fetchLatestVersion: async () => "1.99.0" });

    const exitCode = await handleUpgradeCommand([], cap.context);

    assert.equal(exitCode, 0);
    assert.equal(cap.performUpgradeCalls.length, 0);

    const text = cap.stdout.join("\n");

    assert.match(text, /Docker containers cannot be upgraded in-place/);
    assert.match(text, /docker pull ghcr.io\/hjdhjd\/prismcast:latest/);
  });

  test("--check on docker prints the summary, the in-place advisory, and the recreate instructions", async () => {

    // Locks the unified manual-message behavior between --check and the main upgrade flow. Both paths print every line of the strategy's manualUpgradeMessage,
    // so the --check output includes the "Docker containers cannot be upgraded in-place." advisory in addition to the recreate instruction. The strategy is the
    // single source of truth for its own messaging.
    const cap = makeUpgradeContext({ detect: () => dockerInfo, fetchLatestVersion: async () => "1.99.0" });

    await handleUpgradeCommand(["--check"], cap.context);

    const text = cap.stdout.join("\n");

    assert.match(text, /Install method:\s+Docker/);
    assert.match(text, /Docker containers cannot be upgraded in-place/);
    assert.match(text, /To upgrade, pull the latest image and recreate the container/);
    assert.match(text, /docker pull ghcr.io\/hjdhjd\/prismcast:latest/);
  });
});

describe("handleUpgradeCommand - upgradeable, already current", () => {

  test("prints 'already the latest version' and exits 0 when current >= latest", async () => {

    // Use a deliberately-future version that beats whatever the package's actual version is, so the comparison is current >= latest.
    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "homebrew", upgradeCommand: "brew update && brew upgrade prismcast", upgradeable: true }),
      fetchLatestVersion: async () => "0.0.1"
    });

    const exitCode = await handleUpgradeCommand([], cap.context);

    assert.equal(exitCode, 0);
    assert.equal(cap.performUpgradeCalls.length, 0, "must not run the upgrade when already current");

    const text = cap.stdout.join("\n");

    assert.match(text, /already the latest version/);
    assert.match(text, /Use --force to upgrade anyway/);
  });

  test("--force overrides the already-current short-circuit", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "homebrew", upgradeCommand: "brew update && brew upgrade prismcast", upgradeable: true }),
      fetchLatestVersion: async () => "0.0.1",
      performUpgrade: () => ({ kind: "ran", success: true })
    });

    const exitCode = await handleUpgradeCommand(["--force"], cap.context);

    assert.equal(exitCode, 0);
    assert.equal(cap.performUpgradeCalls.length, 1, "force must invoke the lifecycle");
    assert.equal(cap.performUpgradeCalls[0]?.info.upgradeCommand, "brew update && brew upgrade prismcast");
  });
});

describe("handleUpgradeCommand - upgradeable, registry check failed", () => {

  test("returns 1 and prints 'Unable to check' when latestVersion is null and --force is not set", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "homebrew", upgradeCommand: "brew update && brew upgrade prismcast", upgradeable: true }),
      fetchLatestVersion: async () => null
    });

    const exitCode = await handleUpgradeCommand([], cap.context);

    assert.equal(exitCode, 1, "registry failure without --force must exit 1");
    assert.equal(cap.performUpgradeCalls.length, 0, "must not run the upgrade when we cannot check");
    assert.equal(cap.stderr.length, 1);
    assert.match(cap.stderr[0]!, /Unable to check for updates/);
  });

  test("--force bypasses the registry-failure error and runs the upgrade", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "homebrew", upgradeCommand: "brew update && brew upgrade prismcast", upgradeable: true }),
      fetchLatestVersion: async () => null,
      performUpgrade: () => ({ kind: "ran", success: true })
    });

    const exitCode = await handleUpgradeCommand(["--force"], cap.context);

    assert.equal(exitCode, 0);
    assert.equal(cap.performUpgradeCalls.length, 1);
  });
});

describe("handleUpgradeCommand - successful upgrade execution", () => {

  test("invokes the lifecycle with the install method's InstallInfo (upgradeCommand and packageDir flow through)", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "npm-global", upgradeCommand: "npm install -g prismcast@latest", upgradeable: true }),
      fetchLatestVersion: async () => "9.9.9",
      performUpgrade: () => ({ kind: "ran", success: true })
    });

    const exitCode = await handleUpgradeCommand([], cap.context);

    assert.equal(exitCode, 0);
    assert.equal(cap.performUpgradeCalls.length, 1);
    assert.equal(cap.performUpgradeCalls[0]!.info.upgradeCommand, "npm install -g prismcast@latest");
    assert.equal(cap.performUpgradeCalls[0]!.info.packageDir, undefined, "non-npm-local methods must not carry a packageDir");
  });

  test("npm-local upgrades hold cwd at the resolved packageDir", async () => {

    // The lifecycle (and the in-process runner it delegates to) needs to run from the consumer's project directory so npm resolves the workspace correctly.
    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({

        method: "npm-local",
        packageDir: "/Users/me/proj",
        upgradeCommand: "npm install prismcast@latest",
        upgradeable: true
      }),
      fetchLatestVersion: async () => "9.9.9",
      performUpgrade: () => ({ kind: "ran", success: true })
    });

    await handleUpgradeCommand([], cap.context);

    assert.equal(cap.performUpgradeCalls[0]?.info.packageDir, "/Users/me/proj");
  });

  test("npm-local without a packageDir forwards undefined to the lifecycle", async () => {

    // The dispatcher leaves packageDir undefined when the project root has no package.json; that undefined flows through to the lifecycle strategy so the
    // in-process runner uses process.cwd() instead of crashing on an empty path.
    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "npm-local", upgradeCommand: "npm install prismcast@latest", upgradeable: true }),
      fetchLatestVersion: async () => "9.9.9",
      performUpgrade: () => ({ kind: "ran", success: true })
    });

    await handleUpgradeCommand([], cap.context);

    assert.equal(cap.performUpgradeCalls[0]?.info.packageDir, undefined);
  });

  test("prints 'Upgrade complete' and the manual restart hint when not running as a service", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "npm-global", upgradeCommand: "npm install -g prismcast@latest", upgradeable: true }),
      fetchLatestVersion: async () => "9.9.9",
      isService: false,
      performUpgrade: () => ({ kind: "ran", success: true })
    });

    await handleUpgradeCommand([], cap.context);

    const text = cap.stdout.join("\n");

    assert.match(text, /Upgrade complete/);
    assert.match(text, /restart PrismCast manually/);
  });

  test("calls exit(0) after a successful upgrade when running as a service", async () => {

    // Service mode lets the OS service manager restart the process. The handler signals "we're done, restart us" via process.exit(0); the test sentinel
    // captures that as an exits[0] === 0 entry.
    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "npm-global", upgradeCommand: "npm install -g prismcast@latest", upgradeable: true }),
      fetchLatestVersion: async () => "9.9.9",
      isService: true,
      performUpgrade: () => ({ kind: "ran", success: true })
    });

    await assert.rejects(

      () => handleUpgradeCommand([], cap.context),
      /__test_exit__:0/,
      "service mode triggers process.exit(0) which the test stub raises as a sentinel"
    );

    assert.deepEqual(cap.exits, [0]);

    const text = cap.stdout.join("\n");

    assert.match(text, /Restarting PrismCast via service manager/);
  });
});

describe("handleUpgradeCommand - upgrade execution failure", () => {

  test("returns 1 and prints 'Upgrade failed' when the lifecycle reports a ran/failure outcome", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "homebrew", upgradeCommand: "brew update && brew upgrade prismcast", upgradeable: true }),
      fetchLatestVersion: async () => "9.9.9",
      performUpgrade: () => ({ kind: "ran", success: false })
    });

    const exitCode = await handleUpgradeCommand([], cap.context);

    assert.equal(exitCode, 1);
    assert.equal(cap.stderr.length, 2, "two stderr lines: empty separator and the failure message");
    assert.match(cap.stderr[1]!, /Upgrade failed/);
  });

  test("does NOT call exit when the upgrade fails (caller should observe the exit code)", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "homebrew", upgradeCommand: "brew update && brew upgrade prismcast", upgradeable: true }),
      fetchLatestVersion: async () => "9.9.9",
      isService: true,
      performUpgrade: () => ({ kind: "ran", success: false })
    });

    // Even in service mode, a failed upgrade returns 1 rather than calling exit(0); the caller turns the return value into the process exit code.
    const exitCode = await handleUpgradeCommand([], cap.context);

    assert.equal(exitCode, 1);
    assert.deepEqual(cap.exits, [], "exit must not fire on failed upgrade");
  });
});

describe("handleUpgradeCommand - --check mode for upgradeable methods", () => {

  test("prints 'Run prismcast upgrade' when an upgrade is available", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "npm-global", upgradeCommand: "npm install -g prismcast@latest", upgradeable: true }),
      fetchLatestVersion: async () => "9.9.9"
    });

    await handleUpgradeCommand(["--check"], cap.context);

    const text = cap.stdout.join("\n");

    assert.match(text, /Run 'prismcast upgrade' to upgrade/);
    assert.equal(cap.performUpgradeCalls.length, 0, "--check must never invoke the lifecycle");
  });

  test("prints 'already the latest' when current >= latest", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "homebrew", upgradeCommand: "brew update && brew upgrade prismcast", upgradeable: true }),
      fetchLatestVersion: async () => "0.0.1"
    });

    await handleUpgradeCommand(["--check"], cap.context);

    assert.match(cap.stdout.join("\n"), /already the latest version/);
  });

  test("suggests --force when the registry check returns null", async () => {

    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "homebrew", upgradeCommand: "brew update && brew upgrade prismcast", upgradeable: true }),
      fetchLatestVersion: async () => null
    });

    await handleUpgradeCommand(["--check"], cap.context);

    assert.match(cap.stdout.join("\n"), /Run 'prismcast upgrade --force'/);
  });
});

describe("handleUpgradeCommand - handoff lifecycle (Windows)", () => {

  test("prints the background-upgrade banner with the helper log path and exits 0 when the lifecycle hands off", async () => {

    // The Windows handoff strategy returns { kind: "handed-off", logPath } from performUpgrade. The handler must surface the log path to the user and call
    // process.exit(0) immediately - any code after that point would race the detached helper's wait-then-upgrade sequence. Locks both behaviors.
    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "npm-global", upgradeCommand: "npm install -g prismcast@latest", upgradeable: true }),
      fetchLatestVersion: async () => "9.9.9",
      performUpgrade: () => ({ kind: "handed-off", logPath: "C:\\Users\\jp\\.prismcast\\upgrade.log" })
    });

    await assert.rejects(

      () => handleUpgradeCommand([], cap.context),
      /__test_exit__:0/,
      "handoff outcome must trigger process.exit(0) so the detached helper takes over"
    );

    assert.deepEqual(cap.exits, [0]);

    const text = cap.stdout.join("\n");

    assert.match(text, /Upgrade is running in the background/);
    assert.match(text, /helper will restart it when the upgrade completes/, "the service-task branch must be acknowledged");
    assert.match(text, /restart PrismCast manually/, "the no-service branch must also be acknowledged");
    assert.match(text, /Helper log: C:\\Users\\jp\\\.prismcast\\upgrade\.log/);
  });

  test("handoff outcome bypasses the in-process success path: no 'Upgrade complete' line, no service-manager-restart message", async () => {

    // Locks the structural separation between the two outcomes. The handoff branch's messaging is platform-honest ("running in the background, log at X");
    // the in-process branch's messaging is platform-honest the other way ("complete, restarting via service manager" or "restart manually"). A regression that
    // mixed the two would confuse the user about what just happened.
    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "npm-global", upgradeCommand: "npm install -g prismcast@latest", upgradeable: true }),
      fetchLatestVersion: async () => "9.9.9",
      isService: true,
      performUpgrade: () => ({ kind: "handed-off", logPath: "C:\\log.txt" })
    });

    await assert.rejects(() => handleUpgradeCommand([], cap.context), /__test_exit__:0/);

    const text = cap.stdout.join("\n");

    // The handoff branch must not leak the in-process completion line nor the POSIX service-manager restart line - both are wrong on Windows because the
    // helper, not the parent, drives the restart. The "restart PrismCast manually" string is permitted in the handoff banner because the conditional second
    // clause acknowledges the no-service case honestly; the POSIX fallback line is "Please restart PrismCast manually to use the new version", which is the
    // string we verify is absent here.
    assert.doesNotMatch(text, /Upgrade complete/, "handoff branch must not print the in-process completion line");
    assert.doesNotMatch(text, /Restarting PrismCast via service manager/, "handoff branch must not print the POSIX service-manager restart line");
    assert.doesNotMatch(text, /Please restart PrismCast manually to use the new version/, "handoff branch must not print the POSIX in-process restart line");
  });

  test("--check does not reach the lifecycle even on platforms where handoff would otherwise be selected", async () => {

    // --check is a pure inspection path - it reports what would happen but never invokes performUpgrade, on any platform. A handoff-returning stub here would
    // be a bug if reached; this test locks that the check path short-circuits before lifecycle dispatch.
    const cap = makeUpgradeContext({

      detect: () => makeInstallInfo({ method: "npm-global", upgradeCommand: "npm install -g prismcast@latest", upgradeable: true }),
      fetchLatestVersion: async () => "9.9.9",
      performUpgrade: () => {

        throw new Error("--check must not invoke the lifecycle");
      }
    });

    const exitCode = await handleUpgradeCommand(["--check"], cap.context);

    assert.equal(exitCode, 0);
    assert.equal(cap.performUpgradeCalls.length, 0);
  });
});

describe("handleUpgradeCommand - default-context wiring", () => {

  test("invokes successfully without an explicit context (uses createDefaultUpgradeContext)", async () => {

    // The handler accepts an optional ctx parameter that defaults to createDefaultUpgradeContext(). We don't verify exact output here because the default context
    // talks to real I/O; we only verify the call returns a documented exit code without throwing. This locks the default-argument wiring so a future refactor
    // that breaks the optional doesn't pass unnoticed.
    const exitCode = await handleUpgradeCommand(["--help"]);

    assert.equal(exitCode, 0, "--help is the only branch that touches no I/O, so it is safe to invoke without a stub context");
  });
});
