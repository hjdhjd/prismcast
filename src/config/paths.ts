/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * paths.ts: Centralized filesystem path resolution for PrismCast.
 */
import type { Config } from "../types/index.js";
import os from "node:os";
import path from "node:path";

/* This module is the single source of truth for all filesystem paths used by PrismCast. All other modules import path getters from here instead of computing paths
 * independently. The data directory is resolved once at startup via initializeDataDir(), before config.json is loaded — this is necessary because the data directory
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

  const envDataDir = process.env.PRISMCAST_DATA_DIR;

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
