/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.ts: Upgrade command handler for the PrismCast CLI.
 *
 * handleUpgradeCommand is a pure orchestrator over an UpgradeContext. The context bundles install detection, registry version lookup, the platform-aware
 * upgrade executor, stdout/stderr writers, process exit, and the service-mode probe; production wires all of them through createDefaultUpgradeContext (in
 * commands.context.ts), tests pass a context literal. The decision logic - help dispatch, --check vs --force vs default flow, "already up to date" gating,
 * post-upgrade restart, handoff-on-Windows messaging - is fully testable without touching the real network, real subprocesses, or process state.
 *
 * The HOW of running an upgrade lives in lifecycle.ts (a platform-strategy port that returns an UpgradeStep discriminated union). This file is responsible for
 * the WHEN: ordering the detection-and-version-check phase, deciding whether to run anything at all, and choosing what to print and whether to exit based on
 * the lifecycle's outcome. The two responsibilities are isolated so neither layer needs to know about the other's edge cases.
 */
import type { InstallInfo, NonUpgradeableInstallInfo } from "./detection.ts";
import { getPackageVersion, isVersionLessThan, normalizeVersion } from "../utils/version.ts";
import type { Nullable } from "../types/index.ts";
import type { UpgradeStep } from "./lifecycle.ts";
import { createDefaultUpgradeContext } from "./commands.context.ts";

/**
 * The runtime context handleUpgradeCommand consumes. Each field models one capability the command needs - detection, version lookup, platform-aware upgrade
 * execution, stdout/stderr output, process termination, and a service-mode probe. Decision logic is a pure function of this shape; production wires it through
 * createDefaultUpgradeContext (in commands.context.ts), tests pass a context literal.
 */
export interface UpgradeContext {

  // Detects the install method. Defaults to detectInstallMethod() with the default DetectionContext.
  readonly detect: () => InstallInfo;

  // Process termination. Used by the post-upgrade restart path. Typed as never because process.exit does not return.
  readonly exit: (code: number) => never;

  // Fetches the latest published version from the npm registry. Returns null on network failure or registry error.
  readonly fetchLatestVersion: () => Promise<string | null>;

  // Whether the process is running under a service manager (launchd, systemd, Windows Task Scheduler). Affects the in-process success branch: service mode
  // exits cleanly so launchd KeepAlive or systemd Restart= brings PrismCast back up; manual mode prints "please restart" instructions instead. Unused by the
  // handoff branch because the helper handles the service restart itself.
  readonly isService: boolean;

  // Performs the upgrade for one InstallInfo by dispatching to the platform-appropriate lifecycle strategy. Resolves with a discriminated UpgradeStep - either
  // "ran" (the strategy executed the command in this process) or "handed-off" (the strategy spawned a detached helper). Callers narrow on `kind` to choose the
  // right messaging and exit behavior.
  readonly performUpgrade: (info: InstallInfo) => Promise<UpgradeStep>;

  // Writes a line to stderr. This is the sole error-output surface handleUpgradeCommand uses - it never calls console.error directly - so tests can capture
  // output without touching the real stderr stream.
  readonly stderr: (line: string) => void;

  // Writes a line to stdout. This is the sole output surface handleUpgradeCommand uses - it never calls console.log directly - so tests can capture output
  // without touching the real stdout stream.
  readonly stdout: (line: string) => void;
}

/* The upgrade subcommand handles `prismcast upgrade [--check] [--force] [-h|--help]`. Detection picks the install method, the registry lookup checks for a
 * newer version, and the runner executes the per-method upgrade command (or prints manual instructions for non-upgradeable methods).
 */

function printUpgradeUsage(ctx: UpgradeContext): void {

  ctx.stdout("Usage: prismcast upgrade [options]");
  ctx.stdout("");
  ctx.stdout("Upgrade PrismCast to the latest version.");
  ctx.stdout("");
  ctx.stdout("Options:");
  ctx.stdout("  --check             Show upgrade information without upgrading");
  ctx.stdout("  --force             Upgrade even if already up to date");
  ctx.stdout("  -h, --help          Show this help message");
}

// Shared between --check mode and the non-upgradeable summary so both surfaces print identical version/install-method/upgrade-command rows.
function printUpgradeInfo(ctx: UpgradeContext, info: InstallInfo, currentVersion: string, latestVersion: Nullable<string>): void {

  ctx.stdout("PrismCast Upgrade Check");
  ctx.stdout("─".repeat(40));
  ctx.stdout("Current version: v" + currentVersion);

  if(latestVersion) {

    ctx.stdout("Latest version:  v" + latestVersion);
  } else {

    ctx.stdout("Latest version:  (unable to check)");
  }

  ctx.stdout("Install method:  " + info.displayName);

  if(info.upgradeable) {

    ctx.stdout("Upgrade command: " + info.upgradeCommand);
  }
}

/**
 * Prints the full report for a non-upgradeable install method - the summary table, a blank line, the strategy's manual upgrade message, and the indented
 * command. Single source of truth for "this is what the user sees when we cannot upgrade in-place," shared between the --check flow and the main upgrade flow.
 * The parameter type is the narrow NonUpgradeableInstallInfo variant; callers narrow via the discriminated union before invoking, so manualUpgradeMessage is
 * always in scope.
 * @param ctx - The upgrade context (used for stdout output).
 * @param info - The detected non-upgradeable installation info.
 * @param currentVersion - The currently running version.
 * @param latestVersion - The latest available version, or null if unknown.
 */
function printNonUpgradeableSummary(ctx: UpgradeContext, info: NonUpgradeableInstallInfo, currentVersion: string, latestVersion: Nullable<string>): void {

  printUpgradeInfo(ctx, info, currentVersion, latestVersion);
  ctx.stdout("");

  for(const line of info.manualUpgradeMessage) {

    ctx.stdout(line);
  }

  ctx.stdout("  " + info.upgradeCommand);
}

function handleCheck(ctx: UpgradeContext, info: InstallInfo, currentVersion: string, latestVersion: Nullable<string>): number {

  if(!info.upgradeable) {

    printNonUpgradeableSummary(ctx, info, currentVersion, latestVersion);

    return 0;
  }

  printUpgradeInfo(ctx, info, currentVersion, latestVersion);
  ctx.stdout("");

  if(!latestVersion) {

    ctx.stdout("Run 'prismcast upgrade --force' to upgrade without a version check.");
  } else if(!isVersionLessThan(currentVersion, latestVersion)) {

    ctx.stdout("PrismCast v" + currentVersion + " is already the latest version.");
  } else {

    ctx.stdout("Run 'prismcast upgrade' to upgrade.");
  }

  return 0;
}

/**
 * Main handler for the `upgrade` subcommand. Parses arguments and orchestrates the appropriate upgrade flow through the UpgradeContext. Pure orchestrator over
 * UpgradeContext - decision logic depends only on context methods plus the synchronous getPackageVersion() lookup, never on direct subprocess, registry, or
 * process-state I/O.
 * @param args - Arguments after 'upgrade' (e.g., ['--check', '--force']).
 * @param ctx - The upgrade context. Defaults to createDefaultUpgradeContext() which wires real runtime I/O.
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleUpgradeCommand(args: readonly string[], ctx: UpgradeContext = createDefaultUpgradeContext()): Promise<number> {

  const showHelp = args.includes("--help") || args.includes("-h") || args.includes("help");
  const checkOnly = args.includes("--check");
  const force = args.includes("--force");

  if(showHelp) {

    printUpgradeUsage(ctx);

    return 0;
  }

  const info = ctx.detect();
  const currentVersion = normalizeVersion(getPackageVersion());
  const latestVersion = await ctx.fetchLatestVersion();

  if(checkOnly) {

    return handleCheck(ctx, info, currentVersion, latestVersion);
  }

  // Non-upgradeable methods (docker, unknown): delegate to the shared summary helper, which is the SSOT for the version-table-plus-manual-instructions report.
  // The InstallInfo union narrows on `!info.upgradeable` to expose manualUpgradeMessage; the consumer never switches on info.method.
  if(!info.upgradeable) {

    printNonUpgradeableSummary(ctx, info, currentVersion, latestVersion);

    return 0;
  }

  // Already-current short-circuit: skip when --force is set or when we could not check the registry. Without --force, an indeterminate version check is fatal
  // because we should not blindly run npm install when we cannot see whether it would do anything.
  if(!force && latestVersion && !isVersionLessThan(currentVersion, latestVersion)) {

    ctx.stdout("PrismCast v" + currentVersion + " is already the latest version.");
    ctx.stdout("Use --force to upgrade anyway.");

    return 0;
  }

  if(!force && !latestVersion) {

    ctx.stderr("Unable to check for updates. Run with --force to upgrade anyway.");

    return 1;
  }

  // Pre-upgrade announcement so the user knows what is about to run.
  ctx.stdout("Upgrading PrismCast...");
  ctx.stdout("Install method: " + info.displayName);
  ctx.stdout("Running: " + info.upgradeCommand);
  ctx.stdout("");

  // Dispatch to the platform-aware lifecycle. The resolved UpgradeStep tells us whether the upgrade ran in this process (POSIX path) or was handed off to a
  // detached helper (Windows path); we narrow on `kind` and handle each variant's messaging and exit behavior. packageDir flows through to the lifecycle as
  // part of InstallInfo (only npm-local declares a resolver that sets it).
  const step = await ctx.performUpgrade(info);

  if(step.kind === "handed-off") {

    // The Windows handoff strategy has spawned a detached helper that is waiting for the current process to exit so file locks on the prismcast directory
    // release before npm install runs. We must exit immediately; the helper handles the upgrade and (conditionally) the service-task restart on its own. The
    // log path is surfaced so the user has somewhere to look while the upgrade runs.
    //
    // The restart message is deliberately phrased as a conditional rather than an unconditional promise. The helper only restarts the scheduled task when one
    // is registered; a user who invokes `prismcast upgrade` against a non-service install will need to restart PrismCast manually after the helper finishes.
    // We cannot know which branch will apply without probing Task Scheduler from our process, and that would add a synchronous PowerShell call to every
    // upgrade. The honest two-clause message keeps both audiences correctly informed without that cost.
    ctx.stdout("Upgrade is running in the background.");
    ctx.stdout("If PrismCast is registered as a Windows service, the helper will restart it when the upgrade completes; otherwise, restart PrismCast manually.");
    ctx.stdout("Helper log: " + step.logPath);

    ctx.exit(0);
  }

  // In-process path: the upgrade ran to completion here and the result is the runner's outcome.
  if(!step.success) {

    ctx.stderr("");
    ctx.stderr("Upgrade failed. Check the output above for details.");

    return 1;
  }

  ctx.stdout("");
  ctx.stdout("Upgrade complete.");

  // Service-managed processes restart automatically when we exit; manual installs need a user-driven restart. The handoff branch above does not reach this
  // code because its helper owns the restart; only the in-process branch falls through here.
  if(ctx.isService) {

    ctx.stdout("Restarting PrismCast via service manager...");

    ctx.exit(0);
  }

  ctx.stdout("Please restart PrismCast manually to use the new version.");

  return 0;
}

