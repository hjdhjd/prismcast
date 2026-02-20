/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.ts: Upgrade command handlers for PrismCast CLI.
 */
import { fetchLatestVersion, getPackageVersion, isVersionLessThan, normalizeVersion } from "../utils/version.js";
import type { InstallInfo } from "./detection.js";
import type { Nullable } from "../types/index.js";
import { detectInstallMethod } from "./detection.js";
import { execSync } from "node:child_process";
import { isRunningAsService } from "../utils/platform.js";

/* These handlers implement the `prismcast upgrade` subcommand for detecting the installation method, checking for updates, and executing the appropriate upgrade
 * command. The pattern mirrors the service subcommand handlers in service/commands.ts.
 */

/**
 * Prints a message to stdout.
 * @param message - The message to print.
 */
function print(message: string): void {

  // eslint-disable-next-line no-console
  console.log(message);
}

/**
 * Prints an error message to stderr.
 * @param message - The error message to print.
 */
function printError(message: string): void {

  // eslint-disable-next-line no-console
  console.error(message);
}

/**
 * Formats the display name for an installation method.
 * @param info - The installation info to format.
 * @returns A human-readable name for the install method.
 */
function formatMethodName(info: InstallInfo): string {

  switch(info.method) {

    case "docker": {

      return "Docker";
    }

    case "homebrew": {

      return "Homebrew";
    }

    case "npm-global": {

      return "npm (global)";
    }

    case "npm-local": {

      return "npm (local)";
    }

    default: {

      return "Unknown";
    }
  }
}

/**
 * Prints usage information for the upgrade subcommand.
 */
function printUpgradeUsage(): void {

  print("Usage: prismcast upgrade [options]");
  print("");
  print("Upgrade PrismCast to the latest version.");
  print("");
  print("Options:");
  print("  --check             Show upgrade information without upgrading");
  print("  --force             Upgrade even if already up to date");
  print("  -h, --help          Show this help message");
}

/**
 * Prints the upgrade check summary (shared between --check mode and the pre-upgrade display).
 * @param info - The detected installation info.
 * @param currentVersion - The currently running version.
 * @param latestVersion - The latest available version, or null if unknown.
 */
function printUpgradeInfo(info: InstallInfo, currentVersion: string, latestVersion: Nullable<string>): void {

  print("PrismCast Upgrade Check");
  print("\u2500".repeat(40));
  print("Current version: v" + currentVersion);

  if(latestVersion) {

    print("Latest version:  v" + latestVersion);
  } else {

    print("Latest version:  (unable to check)");
  }

  print("Install method:  " + formatMethodName(info));

  if(info.upgradeable) {

    print("Upgrade command: " + info.upgradeCommand);
  }
}

/**
 * Handles the --check flag: prints upgrade information and exits.
 * @param info - The detected installation info.
 * @param currentVersion - The currently running version.
 * @param latestVersion - The latest available version, or null if unknown.
 * @returns Exit code (0 for success).
 */
function handleCheck(info: InstallInfo, currentVersion: string, latestVersion: Nullable<string>): number {

  printUpgradeInfo(info, currentVersion, latestVersion);
  print("");

  if(!info.upgradeable) {

    // Docker or unknown: show manual instructions using the command from detection.
    if(info.method === "docker") {

      print("To upgrade, pull the latest image and recreate the container:");
      print("  " + info.upgradeCommand);
    } else {

      print("Unable to detect installation method. Please upgrade manually:");
      print("  " + info.upgradeCommand);
    }

    return 0;
  }

  if(!latestVersion) {

    print("Run 'prismcast upgrade --force' to upgrade without a version check.");
  } else if(!isVersionLessThan(currentVersion, latestVersion)) {

    print("PrismCast v" + currentVersion + " is already the latest version.");
  } else {

    print("Run 'prismcast upgrade' to upgrade.");
  }

  return 0;
}

/**
 * Executes the upgrade command for the detected installation method.
 * @param info - The detected installation info.
 * @returns True if the upgrade command succeeded, false otherwise.
 */
function executeUpgrade(info: InstallInfo): boolean {

  try {

    // For npm-local, run the install from the project directory that contains the dependency.
    const options: { cwd?: string; encoding: BufferEncoding; stdio: "inherit" } = { encoding: "utf-8", stdio: "inherit" };

    if((info.method === "npm-local") && info.packageDir) {

      options.cwd = info.packageDir;
    }

    execSync(info.upgradeCommand, options);

    return true;
  } catch {

    return false;
  }
}

/**
 * Main handler for the `upgrade` subcommand. Parses arguments and executes the appropriate upgrade logic.
 * @param args - Arguments after 'upgrade' (e.g., ['--check', '--force']).
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleUpgradeCommand(args: string[]): Promise<number> {

  // Parse flags.
  const showHelp = args.includes("--help") || args.includes("-h") || args.includes("help");
  const checkOnly = args.includes("--check");
  const force = args.includes("--force");

  if(showHelp) {

    printUpgradeUsage();

    return 0;
  }

  // Detect installation method.
  const info = detectInstallMethod();
  const currentVersion = normalizeVersion(getPackageVersion());

  // Fetch the latest version from npm.
  const latestVersion = await fetchLatestVersion();

  // Handle --check mode.
  if(checkOnly) {

    return handleCheck(info, currentVersion, latestVersion);
  }

  // If the installation is not upgradeable (Docker or unknown), print instructions and exit.
  if(!info.upgradeable) {

    printUpgradeInfo(info, currentVersion, latestVersion);
    print("");

    if(info.method === "docker") {

      print("Docker containers cannot be upgraded in-place.");
      print("To upgrade, pull the latest image and recreate the container:");
      print("  " + info.upgradeCommand);
    } else {

      print("Unable to detect installation method. Please upgrade manually:");
      print("  " + info.upgradeCommand);
    }

    return 0;
  }

  // Check if already up to date (skip with --force or if version check failed).
  if(!force && latestVersion && !isVersionLessThan(currentVersion, latestVersion)) {

    print("PrismCast v" + currentVersion + " is already the latest version.");
    print("Use --force to upgrade anyway.");

    return 0;
  }

  // If we couldn't check the latest version and --force wasn't specified, warn the user.
  if(!force && !latestVersion) {

    printError("Unable to check for updates. Run with --force to upgrade anyway.");

    return 1;
  }

  // Show what we're about to do.
  print("Upgrading PrismCast...");
  print("Install method: " + formatMethodName(info));
  print("Running: " + info.upgradeCommand);
  print("");

  // Execute the upgrade.
  if(!executeUpgrade(info)) {

    printError("");
    printError("Upgrade failed. Check the output above for details.");

    return 1;
  }

  print("");
  print("Upgrade complete.");

  // Handle restart: if running as a service, the service manager will restart PrismCast when we exit.
  if(isRunningAsService()) {

    print("Restarting PrismCast via service manager...");

    process.exit(0);
  } else {

    print("Please restart PrismCast manually to use the new version.");
  }

  return 0;
}
