/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * paths.ts: Centralized filesystem path resolution for PrismCast.
 */
import type { Config } from "../types/index.ts";
import { SERVICE_ID } from "../identity.ts";
import fs from "node:fs";
import { getPlatform } from "../utils/platform.ts";
import os from "node:os";
import path from "node:path";

/* This module is the single source of truth for all filesystem paths used by PrismCast. All other modules import path getters from here instead of computing paths
 * independently. The data directory is resolved once at startup via initializeDataDir(), before config.json is loaded - this is necessary because the data directory
 * determines where config.json lives, creating a chicken-and-egg dependency that cannot be resolved through config.json itself.
 *
 * Resolution priority for the data directory (highest to lowest):
 *   1. CLI flag (--data-dir)
 *   2. Environment variable (PRISMCAST_DATA_DIR)
 *   3. Default (~/.prismcast)
 *
 * Chrome data directory and log file paths are stored in Config (settable via config.json, env var, or CLI flag) and resolved after config loading.
 */

// The resolved data directory, initialized once at startup. All path getters depend on this value.
let resolvedDataDir: string | undefined;

/**
 * Initializes the data directory from the CLI flag, environment variable, or default. Must be called at startup before any config loading or path resolution. May
 * be called a second time with a CLI flag to override the initial resolution.
 * @param cliDataDir - Optional data directory from the --data-dir CLI flag.
 */
export function initializeDataDir(cliDataDir?: string): void {

  const envDataDir = process.env["PRISMCAST_DATA_DIR"];

  if(cliDataDir) {

    // CLI flag is already validated by requireAbsolutePath() in index.ts.
    resolvedDataDir = cliDataDir;
  } else if(envDataDir) {

    if(!path.isAbsolute(envDataDir)) {

      // eslint-disable-next-line no-console
      console.error("Error: PRISMCAST_DATA_DIR must be an absolute path, got: " + envDataDir);

      process.exit(1);
    }

    resolvedDataDir = envDataDir;
  } else {

    resolvedDataDir = path.join(os.homedir(), ".prismcast");
  }
}

/**
 * Returns the resolved data directory. Throws if called before initializeDataDir().
 * @returns The absolute path to the data directory.
 */
export function getDataDir(): string {

  if(!resolvedDataDir) {

    throw new Error("Data directory not initialized. Call initializeDataDir() first.");
  }

  return resolvedDataDir;
}

/**
 * Returns the path to the user configuration file.
 * @returns The absolute path to config.json inside the data directory.
 */
export function getConfigFilePath(): string {

  return path.join(getDataDir(), "config.json");
}

/**
 * Returns the path to the user channels file.
 * @returns The absolute path to channels.json inside the data directory.
 */
export function getChannelsFilePath(): string {

  return path.join(getDataDir(), "channels.json");
}

/**
 * Returns the path to the health state file.
 * @returns The absolute path to health.json inside the data directory.
 */
export function getHealthFilePath(): string {

  return path.join(getDataDir(), "health.json");
}

/**
 * Returns the path to the user profiles file.
 * @returns The absolute path to profiles.json inside the data directory.
 */
export function getProfilesFilePath(): string {

  return path.join(getDataDir(), "profiles.json");
}

/**
 * Returns the path to the HLS resume state file.
 * @returns The absolute path to hls-resume.json inside the data directory.
 */
export function getResumeFilePath(): string {

  return path.join(getDataDir(), "hls-resume.json");
}

/**
 * Returns the path to the server identity file. Used to detect duplicate PrismCast instances - the file is written immediately after the instance-guard claim
 * succeeds and is checked at the start of the next launch. Orphaned Chrome processes are discovered via the OS process table (utils/processInspector) using a
 * command-line signature, so no separate Chrome PID file is needed.
 * @returns The absolute path to prismcast.pid inside the data directory.
 */
export function getServerPidFilePath(): string {

  return path.join(getDataDir(), "prismcast.pid");
}

/**
 * Returns the directory path for service stdout/stderr output. This is the same as the data directory today; the named alias documents intent at the call site
 * (a service generator asking for "where do my logs go?" rather than reaching directly for the data dir) and leaves room to diverge if a future platform wants
 * its service logs elsewhere.
 * @returns The absolute path to the service logs directory.
 */
export function getLogsDirectory(): string {

  return getDataDir();
}

/**
 * Returns the platform-specific path where the service file should be installed. macOS and Linux use well-known per-user locations (Library/LaunchAgents,
 * ~/.config/systemd/user); on Windows we co-locate the PowerShell launcher script with the rest of PrismCast's runtime state inside the data directory, since
 * Windows has no equivalent per-user service registry path. The launcher carries the runtime configuration (working dir, env vars, node and entry-point paths)
 * and acts as the single source of truth for stale-path detection in service/commands.ts.
 * @returns The absolute path to the service file location, or "" on unsupported platforms.
 */
export function getServiceFilePath(): string {

  const homeDir = os.homedir();

  switch(getPlatform()) {

    case "darwin": {

      return path.join(homeDir, "Library", "LaunchAgents", SERVICE_ID + ".plist");
    }

    case "linux": {

      return path.join(homeDir, ".config", "systemd", "user", "prismcast.service");
    }

    case "windows": {

      return path.join(getDataDir(), "prismcast-service.ps1");
    }

    default: {

      return "";
    }
  }
}

/**
 * Returns the directory containing the service file for the current platform. May need to be created before writing the service file.
 * @returns The absolute path to the service file directory.
 */
export function getServiceFileDirectory(): string {

  return path.dirname(getServiceFilePath());
}

/**
 * Checks whether a service file exists at the expected platform-specific location. Centralized here alongside the path resolver it depends on so a single module
 * owns both the "where" and "does it exist" questions for the service file.
 * @returns True if the service file exists.
 */
export function serviceFileExists(): boolean {

  return fs.existsSync(getServiceFilePath());
}

/**
 * Returns the Chrome user data directory. When config.paths.chromeDataDir is set, that absolute path is used directly. Otherwise, the directory is built from the
 * data directory and the configured profile name.
 * @param config - The application configuration.
 * @returns The absolute path to the Chrome data directory.
 */
export function getChromeDataDir(config: Config): string {

  return config.paths.chromeDataDir ?? path.join(getDataDir(), config.paths.chromeProfileName);
}

/**
 * Returns the extension directory path, built from the data directory and the configured extension directory name.
 * @param config - The application configuration.
 * @returns The absolute path to the extension directory.
 */
export function getExtensionDir(config: Config): string {

  return path.join(getDataDir(), config.paths.extensionDirName);
}

/**
 * Returns the log file path. When config.paths.logFile is set, that absolute path is used directly. Otherwise, the default location inside the data directory is used.
 * @param config - The application configuration.
 * @returns The absolute path to the log file.
 */
export function getLogFilePath(config: Config): string {

  return config.paths.logFile ?? path.join(getDataDir(), "prismcast.log");
}

/**
 * Returns the value of the PRISMCAST_DEBUG environment variable, if set. Centralizing this read keeps the index-signature access on process.env in one place and
 * gives tests a single place to stub.
 * @returns The PRISMCAST_DEBUG value, or undefined when unset.
 */
export function getDebugEnv(): string | undefined {

  return process.env["PRISMCAST_DEBUG"];
}
