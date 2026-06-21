/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * platform.ts: Platform detection and service-related utilities for PrismCast.
 */
import type { Nullable } from "../types/index.ts";
import fs from "node:fs";
import path from "node:path";
import url from "node:url";

/* These utilities provide platform detection and service-related functionality. The isRunningAsService() function checks for an environment variable set by the
 * service definition, allowing the application to adapt its restart behavior based on whether it's managed by a service manager or running standalone.
 */

// The union is intentionally three-valued. getPlatform() maps process.platform "darwin" and "win32" explicitly, and collapses everything else (Linux, the BSDs,
// and any other Unix) into "linux", so there is no separate BSD or "other" branch to represent here.
export type Platform = "darwin" | "linux" | "windows";

export type ServiceManager = "launchd" | "systemd" | "windows-scheduler";

// Environment variable name used to detect container mode.
const CONTAINER_ENV_VAR = "PRISMCAST_CONTAINER";

// Environment variable name used to detect service mode.
const SERVICE_ENV_VAR = "PRISMCAST_SERVICE";

/**
 * Returns the current platform as a normalized string.
 * @returns The platform: "darwin" for macOS, "linux" for Linux, "windows" for Windows.
 */
export function getPlatform(): Platform {

  switch(process.platform) {

    case "darwin": {

      return "darwin";
    }

    case "win32": {

      return "windows";
    }

    default: {

      return "linux";
    }
  }
}

/**
 * Returns the appropriate service manager for the current platform.
 * @returns The service manager type, or null if the platform is not supported for service installation.
 */
export function getServiceManager(): Nullable<ServiceManager> {

  switch(getPlatform()) {

    case "darwin": {

      return "launchd";
    }

    case "linux": {

      return "systemd";
    }

    case "windows": {

      return "windows-scheduler";
    }

    default: {

      return null;
    }
  }
}

/**
 * Checks whether PrismCast is running as a managed service. This is determined by the presence of the PRISMCAST_SERVICE environment variable, which is set in the
 * service definition file. This allows the application to adapt its restart behavior - when running as a service, exiting will trigger an automatic restart by the
 * service manager, but when running standalone, the user must restart manually.
 * @returns True if running as a managed service, false otherwise.
 */
export function isRunningAsService(): boolean {

  return process.env[SERVICE_ENV_VAR] === "1";
}

/**
 * Checks whether PrismCast is running inside a Docker container. Two-tier detection: the explicit PRISMCAST_CONTAINER environment variable set in our Dockerfile is
 * the primary signal; the /.dockerenv marker file (created by Docker in every container) is the backup for custom images that omit the environment variable.
 * @returns True if running inside a container, false otherwise.
 */
export function isRunningInContainer(): boolean {

  if(process.env[CONTAINER_ENV_VAR] === "1") {

    return true;
  }

  // Backup: Docker creates /.dockerenv in every container. This catches custom images that don't set PRISMCAST_CONTAINER.
  try {

    return fs.existsSync("/.dockerenv");
  } catch {

    return false;
  }
}

/**
 * Returns the full path to the Node.js executable. This is needed for service files which require absolute paths since the service environment may not have PATH set.
 * We prefer symlink paths (e.g., /opt/homebrew/bin/node) over resolved paths (e.g., /opt/homebrew/Cellar/node/25.4.0/bin/node) so that Homebrew and similar package
 * managers can upgrade Node without breaking the service.
 * @returns The absolute path to the node binary, preferring symlinks when available.
 */
export function getNodeExecutablePath(): string {

  const resolvedPath = process.execPath;

  // Common symlink locations for Node.js on various platforms.
  const symlinkPaths = [
    "/opt/homebrew/bin/node",
    "/usr/local/bin/node",
    "/usr/bin/node",
    "/home/linuxbrew/.linuxbrew/bin/node"
  ];

  // Check if any of the common symlink paths resolve to the same executable.
  for(const symlinkPath of symlinkPaths) {

    try {

      const resolved = fs.realpathSync(symlinkPath);

      if(resolved === resolvedPath) {

        return symlinkPath;
      }
    } catch {

      // Symlink doesn't exist or can't be resolved; try next.
    }
  }

  // No symlink found; fall back to the resolved path.
  return resolvedPath;
}

/**
 * Returns the full path to PrismCast's entry point (dist/index.js). This is needed for service files which require absolute paths.
 * @returns The absolute path to the PrismCast entry point.
 */
export function getPrismCastEntryPoint(): string {

  // Use import.meta.url to get the current file's path, then resolve to the entry point. This works regardless of how PrismCast was installed (npx, global, local).
  // At runtime, this file is dist/utils/platform.js, so we go up two levels to dist/, then add index.js.
  const currentFile = url.fileURLToPath(import.meta.url);
  const utilsDir = path.dirname(currentFile);
  const distDir = path.dirname(utilsDir);

  return path.join(distDir, "index.js");
}

/**
 * Returns the working directory for PrismCast. This is the parent of the dist directory.
 * @returns The absolute path to the PrismCast working directory.
 */
export function getPrismCastWorkingDirectory(): string {

  const entryPoint = getPrismCastEntryPoint();

  return path.dirname(path.dirname(entryPoint));
}

