/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * version.ts: Version checking and update notification utilities.
 */
import { LOG } from "./logger.js";
import type { Nullable } from "../types/index.js";
import { fileURLToPath } from "url";
import { formatError } from "./errors.js";
import { readFileSync } from "fs";
import { resolve } from "path";

// Package name for npm registry lookups.
const NPM_PACKAGE_NAME = "prismcast";

// Cached package version.
let cachedPackageVersion: Nullable<string> = null;

/**
 * Gets the current package version from package.json.
 * @returns The current version string (e.g., "1.0.7").
 */
export function getPackageVersion(): string {

  if(cachedPackageVersion) {

    return cachedPackageVersion;
  }

  try {

    // Resolve the path to package.json relative to this file. This file is in src/utils/ or dist/utils/, and package.json is in the project root.
    const currentDir = fileURLToPath(new URL(".", import.meta.url));
    const packagePath = resolve(currentDir, "../../package.json");
    const packageJson = JSON.parse(readFileSync(packagePath, "utf-8")) as { version: string };

    cachedPackageVersion = packageJson.version;

    return cachedPackageVersion;
  } catch {

    return "0.0.0";
  }
}

// GitHub raw URL for fetching changelog.
const CHANGELOG_URL = "https://raw.githubusercontent.com/hjdhjd/prismcast/main/Changelog.md";

// How often to check for updates (2 hours in milliseconds).
const UPDATE_CHECK_INTERVAL = 2 * 60 * 60 * 1000;

// Cached version information.
let cachedLatestVersion: Nullable<string> = null;
let cachedChangelog: Nullable<string> = null;
let lastCheckTime = 0;
let updateCheckInterval: Nullable<ReturnType<typeof setInterval>> = null;

/**
 * Normalizes a version string by stripping the leading 'v' prefix if present.
 * @param version - Version string (e.g., "v1.0.7" or "1.0.7").
 * @returns Normalized version without 'v' prefix (e.g., "1.0.7").
 */
function normalizeVersion(version: string): string {

  return version.replace(/^v/, "");
}

/**
 * Compares two semver version strings. Returns true if version a is less than version b. Assumes versions are already normalized (no 'v' prefix).
 * @param a - First version string (e.g., "1.0.7").
 * @param b - Second version string (e.g., "1.0.8").
 * @returns True if a < b.
 */
function isVersionLessThan(a: string, b: string): boolean {

  const partsA = a.split(".").map(Number);
  const partsB = b.split(".").map(Number);

  for(let i = 0; i < Math.max(partsA.length, partsB.length); i++) {

    const numA = partsA[i] ?? 0;
    const numB = partsB[i] ?? 0;

    if(numA < numB) {

      return true;
    }

    if(numA > numB) {

      return false;
    }
  }

  return false;
}

/**
 * Fetches the latest version from the npm registry.
 * @returns The latest version string, or null if the fetch failed.
 */
async function fetchLatestVersion(): Promise<Nullable<string>> {

  try {

    const response = await fetch("https://registry.npmjs.org/" + NPM_PACKAGE_NAME);

    if(!response.ok) {

      return null;
    }

    const data = await response.json() as { "dist-tags"?: { latest?: string } };
    const latest = data["dist-tags"]?.latest;

    return latest ? normalizeVersion(latest) : null;
  } catch(error) {

    LOG.debug("config", "Failed to fetch latest version from npm: %s.", formatError(error));

    return null;
  }
}

/**
 * Fetches the changelog from GitHub.
 * @returns The full changelog content, or null if the fetch failed.
 */
async function fetchChangelogContent(): Promise<Nullable<string>> {

  try {

    const response = await fetch(CHANGELOG_URL);

    if(!response.ok) {

      return null;
    }

    return await response.text();
  } catch(error) {

    LOG.debug("config", "Failed to fetch changelog from GitHub: %s.", formatError(error));

    return null;
  }
}

/**
 * Extracts the changelog entry for a specific version. Assumes version is already normalized (no 'v' prefix).
 * @param changelog - The full changelog content.
 * @param version - The version to extract (e.g., "1.0.8").
 * @returns The changelog entry for the version, or null if not found.
 */
function extractVersionChangelog(changelog: string, version: string): Nullable<string> {

  // Match the version header and capture everything until the next version header or end of file. The changelog format is: ## 1.0.8 (date)
  // Note: We use (?![^]) instead of $ for end-of-string because the m flag makes $ match end-of-line, which would stop the non-greedy *? at the first line.
  const pattern = new RegExp("^## " + version.replace(/\./g, "\\.") + "\\s+\\([^)]+\\)\\s*\\n([\\s\\S]*?)(?=^## \\d|(?![^]))", "m");
  const match = changelog.match(pattern);

  if(!match) {

    return null;
  }

  // Clean up the entry: trim whitespace and remove leading/trailing blank lines.
  return match[1].trim();
}

/**
 * Checks for updates and caches the results.
 * @param currentVersion - The currently running version.
 * @param force - If true, bypasses the debounce check.
 */
export async function checkForUpdates(currentVersion: string, force = false): Promise<void> {

  const now = Date.now();

  // Skip if we checked recently (within 1 minute) to avoid duplicate checks on startup, unless forced.
  if(!force && ((now - lastCheckTime) < 60000)) {

    return;
  }

  lastCheckTime = now;

  const current = normalizeVersion(currentVersion);
  const latest = await fetchLatestVersion();

  if(!latest) {

    return;
  }

  const previousLatest = cachedLatestVersion;

  cachedLatestVersion = latest;

  // Determine if this is a newly detected update (version changed since last check).
  const isNewUpdate = (previousLatest !== latest) && isVersionLessThan(current, latest);

  // Log if there's a newer version available.
  if(isNewUpdate) {

    LOG.info("Update available: v%s (current: v%s).", latest, current);
  }

  // Fetch changelog if we haven't yet, or refresh it when a new update is detected to ensure the new version's changelog is available.
  if(!cachedChangelog || isNewUpdate) {

    const changelog = await fetchChangelogContent();

    if(changelog) {

      cachedChangelog = changelog;
    }
  }
}

/**
 * Gets the cached latest version information.
 * @param currentVersion - The currently running version.
 * @returns Object with latest version and whether an update is available.
 */
export function getVersionInfo(currentVersion: string): { latestVersion: Nullable<string>; updateAvailable: boolean } {

  const current = normalizeVersion(currentVersion);
  const latest = cachedLatestVersion;

  return {

    latestVersion: latest,
    updateAvailable: (latest !== null) && isVersionLessThan(current, latest)
  };
}

/**
 * Parses a changelog entry into an array of items.
 * @param entry - The raw changelog entry text.
 * @returns Array of changelog items (bullet points), or null if no items found.
 */
function parseChangelogItems(entry: string): Nullable<string[]> {

  const lines = entry.split("\n").filter((line) => line.trim().startsWith("*"));
  const items = lines.map((line) => line.replace(/^\s*\*\s*/, ""));

  return items.length > 0 ? items : null;
}

/**
 * Gets the changelog items for a specific version as an array of strings. Uses a two-phase lookup: first checks the cache, then fetches fresh data if the
 * version isn't found. This handles both cold cache (no data yet) and stale cache (new version not in cached data) scenarios.
 * @param version - The version to get changelog items for.
 * @returns Array of changelog items (bullet points), or null if unavailable.
 */
export async function getChangelogItems(version: string): Promise<Nullable<string[]>> {

  const normalized = normalizeVersion(version);

  // Phase 1: Try to find version in cached changelog (fast path).
  if(cachedChangelog) {

    const entry = extractVersionChangelog(cachedChangelog, normalized);

    if(entry) {

      return parseChangelogItems(entry);
    }
  }

  // Phase 2: Version not in cache (or no cache). Fetch fresh changelog and try again.
  const changelog = await fetchChangelogContent();

  if(!changelog) {

    return null;
  }

  cachedChangelog = changelog;

  const entry = extractVersionChangelog(cachedChangelog, normalized);

  if(!entry) {

    return null;
  }

  return parseChangelogItems(entry);
}

/**
 * Starts periodic update checking.
 * @param currentVersion - The currently running version.
 */
export function startUpdateChecking(currentVersion: string): void {

  const current = normalizeVersion(currentVersion);

  // Do an initial check.
  void checkForUpdates(current);

  // Set up periodic checking.
  updateCheckInterval ??= setInterval(() => void checkForUpdates(current), UPDATE_CHECK_INTERVAL);
}

/**
 * Stops periodic update checking.
 */
export function stopUpdateChecking(): void {

  if(updateCheckInterval) {

    clearInterval(updateCheckInterval);
    updateCheckInterval = null;
  }
}
