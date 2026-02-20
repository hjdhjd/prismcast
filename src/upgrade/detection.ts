/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * detection.ts: Installation method detection for PrismCast upgrade.
 */
import type { Nullable } from "../types/index.js";
import { execSync } from "node:child_process";
import { existsSync } from "node:fs";
import { isRunningInContainer } from "../utils/platform.js";
import path from "node:path";
import url from "node:url";

// Installation method types. Docker and unknown are not upgradeable in-place.
export type InstallMethod = "docker" | "homebrew" | "npm-global" | "npm-local" | "unknown";

/**
 * Information about the detected installation method and how to upgrade.
 */
export interface InstallInfo {

  method: InstallMethod;
  packageDir?: string;
  upgradeCommand: string;
  upgradeable: boolean;
}

// Docker image name for display in upgrade instructions.
const DOCKER_IMAGE = "ghcr.io/hjdhjd/prismcast";

/**
 * Resolves the npm global prefix directory via `npm prefix -g` with a short timeout.
 * @returns The global prefix path, or null if the command fails or npm is not available.
 */
function resolveNpmGlobalPrefix(): Nullable<string> {

  try {

    return execSync("npm prefix -g", { encoding: "utf-8", timeout: 5000 }).trim();
  } catch {

    return null;
  }
}

/**
 * Detects how PrismCast was installed by examining the path of the running code. Detection order: Docker container, Homebrew, npm global, npm local, unknown.
 * @returns Installation information including method, whether it is upgradeable, and the appropriate upgrade command.
 */
export function detectInstallMethod(): InstallInfo {

  // Docker detection comes first — inside a container, path-based detection is irrelevant.
  if(isRunningInContainer()) {

    return {

      method: "docker",
      upgradeCommand: "docker pull " + DOCKER_IMAGE + ":latest && docker compose up -d",
      upgradeable: false
    };
  }

  // Resolve the path of the currently running code via import.meta.url. At runtime this file is dist/upgrade/detection.js.
  const currentFile = url.fileURLToPath(import.meta.url);

  // Homebrew detection: Homebrew formula installs live under /Cellar/prismcast/. We check specifically for /Cellar/prismcast/ rather than a broad /homebrew/ match
  // because npm global installs on Homebrew-managed Node also live under /opt/homebrew/ (e.g., /opt/homebrew/lib/node_modules/prismcast/).
  if(currentFile.includes("/Cellar/prismcast/")) {

    return {

      method: "homebrew",
      upgradeCommand: "brew update && brew upgrade prismcast",
      upgradeable: true
    };
  }

  // npm global detection: the code path starts with the global prefix (e.g., /usr/local/lib/node_modules/prismcast/...).
  const globalPrefix = resolveNpmGlobalPrefix();

  if(globalPrefix && currentFile.startsWith(globalPrefix)) {

    return {

      method: "npm-global",
      upgradeCommand: "npm install -g prismcast@latest",
      upgradeable: true
    };
  }

  // npm local detection: the path contains /node_modules/prismcast/ indicating a local project install.
  const localMarker = "/node_modules/prismcast/";
  const localIndex = currentFile.indexOf(localMarker);

  if(localIndex !== -1) {

    // Extract the package directory (the project root containing package.json) from the path before node_modules.
    const projectRoot = currentFile.slice(0, localIndex);

    // Verify the project root actually has a package.json.
    const packageDir = existsSync(path.join(projectRoot, "package.json")) ? projectRoot : undefined;

    return {

      method: "npm-local",
      packageDir,
      upgradeCommand: "npm install prismcast@latest",
      upgradeable: true
    };
  }

  // Unknown: development build, npx, or an unrecognized installation layout.
  return {

    method: "unknown",
    upgradeCommand: "npm install -g prismcast@latest",
    upgradeable: false
  };
}
