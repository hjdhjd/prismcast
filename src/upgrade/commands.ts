/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.ts: Upgrade command handler for the PrismCast CLI.
 *
 * handleUpgradeCommand is a pure orchestrator over an UpgradeContext. The context bundles install detection, registry version lookup, the subprocess runner,
 * stdout/stderr writers, process exit, and the service-mode probe; production wires all of them through createDefaultUpgradeContext (in commands.context.ts),
 * tests pass a context literal. The decision logic - help dispatch, --check vs --force vs default flow, "already up to date" gating, post-upgrade restart - is
 * fully testable without touching the real network, real subprocesses, or process state.
 */
import type { InstallInfo, NonUpgradeableInstallInfo } from "./detection.ts";
import { getPackageVersion, isVersionLessThan, normalizeVersion } from "../utils/version.ts";
import type { Nullable } from "../types/index.ts";
import { createDefaultUpgradeContext } from "./commands.context.ts";

/**
 * Result of running an upgrade command. The success flag distinguishes ran-without-error from threw-or-exited-non-zero; the runner does not surface the actual
 * error because execSync's stderr is inherited to the user's terminal directly.
 */
export interface UpgradeResult {

  // True when the command exited 0; false on any non-zero exit, throw, or timeout.
  readonly success: boolean;
}

/**
 * The runtime context handleUpgradeCommand consumes. Each field models one capability the command needs - detection, version lookup, subprocess execution,
 * stdout/stderr output, process termination, and a service-mode probe. Decision logic is a pure function of this shape; production wires it through
 * createDefaultUpgradeContext (in commands.context.ts), tests pass a context literal.
 */
export interface UpgradeContext {

  // Detects the install method. Defaults to detectInstallMethod() with the default DetectionContext.
  readonly detect: () => InstallInfo;

  // Process termination. Used by the post-upgrade restart path. Typed as never because process.exit does not return.
  readonly exit: (code: number) => never;

  // Fetches the latest published version from the npm registry. Returns null on network failure or registry error.
  readonly fetchLatestVersion: () => Promise<string | null>;

  // Whether the process is running under a service manager (launchd, systemd, Windows service). Affects the post-upgrade flow - service mode exits cleanly so
  // the manager restarts the process; manual mode prints "please restart" instructions instead.
  readonly isService: boolean;

  // Subprocess runner that executes the upgrade command. The implementation inherits the user's terminal so they see npm/brew output live; the result captures
  // only the outcome (success or failure), not stdout/stderr.
  readonly runUpgradeCommand: (cmd: string, options: { cwd?: string }) => UpgradeResult;

  // stderr writer.
  readonly stderr: (line: string) => void;

  // stdout writer.
  readonly stdout: (line: string) => void;
}

/* The upgrade subcommand handles `prismcast upgrade [--check] [--force] [-h|--help]`. Detection picks the install method, the registry lookup checks for a
 * newer version, and the runner executes the per-method upgrade command (or prints manual instructions for non-upgradeable methods).
 */

/**
 * Prints usage information for the upgrade subcommand.
 * @param ctx - The upgrade context (used for stdout output).
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

/**
 * Prints the upgrade summary table (shared between --check mode and the pre-upgrade display).
 * @param ctx - The upgrade context (used for stdout output).
 * @param info - The detected installation info.
 * @param currentVersion - The currently running version.
 * @param latestVersion - The latest available version, or null if unknown.
 */
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

/**
 * Handles the --check flag: prints upgrade information and exits with 0.
 * @param ctx - The upgrade context.
 * @param info - The detected installation info.
 * @param currentVersion - The currently running version.
 * @param latestVersion - The latest available version, or null if unknown.
 * @returns Exit code (0).
 */
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
 * Main handler for the `upgrade` subcommand. Parses arguments and orchestrates the appropriate upgrade flow through the UpgradeContext. Pure function of
 * UpgradeContext modulo the help/usage paths that just write to stdout - no detection, registry, subprocess, or process state is touched outside the context's
 * methods.
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
  // because we should not blindly run npm install when we can not see whether it would do anything.
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

  // Execute the upgrade. packageDir is set only by the npm-local strategy (it is the lone field in ResolvableFields and only npm-local declares a resolver),
  // so reading it directly is sufficient - the runner uses it as cwd when present and falls back to process.cwd() when absent.
  const result = ctx.runUpgradeCommand(info.upgradeCommand, { cwd: info.packageDir });

  if(!result.success) {

    ctx.stderr("");
    ctx.stderr("Upgrade failed. Check the output above for details.");

    return 1;
  }

  ctx.stdout("");
  ctx.stdout("Upgrade complete.");

  // Service-managed processes restart automatically when we exit; manual installs need a user-driven restart.
  if(ctx.isService) {

    ctx.stdout("Restarting PrismCast via service manager...");

    ctx.exit(0);
  }

  ctx.stdout("Please restart PrismCast manually to use the new version.");

  return 0;
}

