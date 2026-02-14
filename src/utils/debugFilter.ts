/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * debugFilter.ts: Category-based debug log filtering for PrismCast.
 */

/* The debug filter provides category-based control over debug log output, inspired by the `debug` npm package. Categories use colon-separated namespaces
 * (e.g., "tuning:hulu", "recovery:tab") and the PRISMCAST_DEBUG environment variable accepts comma-separated patterns with wildcard and exclusion support.
 *
 * Pattern syntax:
 *   - "*" enables all categories.
 *   - "category" enables an exact category or any sub-category (prefix match).
 *   - "-category" excludes a category or its sub-categories, even when wildcard is active.
 *
 * Examples:
 *   PRISMCAST_DEBUG=tuning:hulu          Only Hulu tuning messages.
 *   PRISMCAST_DEBUG=recovery             All recovery sub-categories (recovery:tab, recovery:nav, etc.).
 *   PRISMCAST_DEBUG=*,-streaming:ffmpeg,-streaming:segmenter  Everything except FFmpeg and segmenter messages.
 */

// Whether any debug output is configured at all. Fast-path check avoids category string work when debug is off.
let anyEnabled = false;

// Whether wildcard (*) was specified — all categories pass unless explicitly excluded.
let wildcardEnabled = false;

// Categories to include (exact or prefix match).
const includeSet = new Set<string>();

// Categories to exclude (exact or prefix match). Takes priority over includes and wildcard.
const excludeSet = new Set<string>();

/**
 * Checks whether a category matches any pattern in the given set. A pattern matches if it equals the category exactly or if the category starts with the pattern
 * followed by a colon (prefix match for sub-categories).
 * @param category - The category to check.
 * @param patterns - The set of patterns to match against.
 * @returns True if the category matches any pattern.
 */
function matchesAny(category: string, patterns: Set<string>): boolean {

  if(patterns.has(category)) {

    return true;
  }

  for(const pattern of patterns) {

    if(category.startsWith(pattern + ":")) {

      return true;
    }
  }

  return false;
}

/**
 * Parses a comma-separated pattern string and configures the debug filter. Calling this function replaces any previous filter configuration.
 * @param pattern - Comma-separated list of category patterns (e.g., "tuning:hulu,recovery,-streaming:ffmpeg").
 */
export function initDebugFilter(pattern: string): void {

  // Reset state.
  includeSet.clear();
  excludeSet.clear();
  wildcardEnabled = false;
  anyEnabled = false;

  const parts = pattern.split(",").map((p) => p.trim()).filter((p) => p.length > 0);

  if(parts.length === 0) {

    return;
  }

  for(const part of parts) {

    if(part === "*") {

      wildcardEnabled = true;
    } else if(part.startsWith("-")) {

      excludeSet.add(part.substring(1));
    } else {

      includeSet.add(part);
    }
  }

  anyEnabled = true;
}

/**
 * Checks whether a specific debug category is enabled under the current filter configuration.
 * @param category - The category to check (e.g., "tuning:hulu", "recovery:tab").
 * @returns True if debug output should be produced for this category.
 */
export function isCategoryEnabled(category: string): boolean {

  if(!anyEnabled) {

    return false;
  }

  // Excludes always win, even over wildcard.
  if(matchesAny(category, excludeSet)) {

    return false;
  }

  if(wildcardEnabled) {

    return true;
  }

  return matchesAny(category, includeSet);
}

/**
 * Fast-path check for whether any debug categories are configured. When this returns false, callers can skip category string construction entirely.
 * @returns True if at least one debug category is enabled.
 */
export function isAnyDebugEnabled(): boolean {

  return anyEnabled;
}

/**
 * Reconstructs the current filter pattern string from internal state. Returns an empty string when no debug output is configured.
 * @returns The current pattern string (e.g., "*,-streaming:ffmpeg" or "tuning:hulu,recovery").
 */
export function getCurrentPattern(): string {

  if(!anyEnabled) {

    return "";
  }

  const parts: string[] = [];

  if(wildcardEnabled) {

    parts.push("*");
  }

  // Exclude entries are prefixed with "-".
  for(const entry of excludeSet) {

    parts.push("-" + entry);
  }

  // Include entries are bare category names.
  for(const entry of includeSet) {

    parts.push(entry);
  }

  return parts.join(",");
}

// Debug Category Registry.

/**
 * Metadata for a known debug category. Used by the /debug UI to display available categories with descriptions.
 */
export interface DebugCategory {

  readonly category: string;
  readonly description: string;
}

/**
 * Static registry of all known debug categories with descriptions. Sorted alphabetically by category. The /debug endpoint uses this to render hierarchical checkboxes.
 * Parent groups (e.g., "streaming", "timing", "tuning") are derived by the UI from the colon-separated namespaces — only leaf categories are declared here.
 */
export const DEBUG_CATEGORIES: readonly DebugCategory[] = [

  { category: "browser", description: "Browser lifecycle: launch, close, stale page cleanup, restart." },
  { category: "browser:video", description: "Video context, fullscreen, volume locking, playback." },
  { category: "config", description: "Provider groups, version checking." },
  { category: "recovery", description: "General recovery: browser re-minimize, monitor abort." },
  { category: "recovery:context", description: "Video context: frame detachment, re-search." },
  { category: "recovery:nav", description: "Page navigation recovery: new tab detection, URL validation." },
  { category: "recovery:segments", description: "Segment production: self-heal detection." },
  { category: "recovery:tab", description: "Tab replacement: old tab cleanup, new tab creation, retries." },
  { category: "retry", description: "Retry attempts, page-closed aborts." },
  { category: "streaming:ffmpeg", description: "FFmpeg stderr output, pipe errors." },
  { category: "streaming:hls", description: "HLS segment storage, page close errors." },
  { category: "streaming:mpegts", description: "MPEG-TS remuxer errors, client connect/disconnect." },
  { category: "streaming:segmenter", description: "fMP4 parsing: keyframes, init segments, duration clamping." },
  { category: "streaming:setup", description: "Stream setup: redirect resolution, profile override, capture init." },
  { category: "streaming:showinfo", description: "Channels DVR show name lookups, device mapping." },
  { category: "timing:browser", description: "Browser launch: process spawn, extension init, display detection." },
  { category: "timing:hls", description: "HLS playlist delivery time." },
  { category: "timing:recovery", description: "Recovery totals: navigation recovery, tab replacement." },
  { category: "timing:startup", description: "Stream startup: init segment, first playlist, capture ready." },
  { category: "timing:tab", description: "Tab replacement: old tab cleanup, new page creation." },
  { category: "timing:tune", description: "Tune waterfall: navigation, channel selection, video ready." },
  { category: "tuning:hbo", description: "HBO Max: tab URL discovery, channel rail, navigation." },
  { category: "tuning:hulu", description: "Hulu Live guide grid: binary search, cache, click retries." },
  { category: "tuning:sling", description: "Sling TV guide grid: binary search, cache, click retries." },
  { category: "tuning:yttv", description: "YouTube TV EPG grid navigation." }
];

/**
 * Creates a lightweight elapsed-time closure using performance.now(). Call the returned function to get the elapsed milliseconds since creation.
 * @returns A closure that returns elapsed milliseconds as a number.
 */
export function startTimer(): () => number {

  const start = performance.now();

  return (): number => Math.round(performance.now() - start);
}
