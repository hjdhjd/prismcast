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

// Whether wildcard (*) was specified - all categories pass unless explicitly excluded.
let wildcardEnabled = false;

// Categories to include (exact or prefix match). Reassigned wholesale by initDebugFilter from a freshly parsed pattern, so it is a let rather than a const.
let includeSet = new Set<string>();

// Categories to exclude (exact or prefix match). Takes priority over includes and wildcard. Reassigned wholesale by initDebugFilter.
let excludeSet = new Set<string>();

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
 * The structured form of a parsed filter pattern: the wildcard flag plus the include and exclude category sets. Shared by the runtime filter (initDebugFilter
 * stores it as module state) and the pure canonicalizer (canonicalizeDebugPattern), so the parse and the re-emission live in one place each.
 */
interface ParsedPattern {

  readonly excludes: Set<string>;
  readonly includes: Set<string>;
  readonly wildcard: boolean;
}

/**
 * Parses a comma-separated pattern string into its structured form. Pure: it allocates fresh sets and never touches module state, so it is safe to call for
 * canonicalization independently of applying the filter. Whitespace around commas is trimmed and empty tokens are dropped; duplicate tokens collapse via the sets.
 * @param pattern - Comma-separated list of category patterns (e.g., "tuning:hulu,recovery,-streaming:ffmpeg").
 * @returns The parsed wildcard flag and include/exclude sets.
 */
function parsePattern(pattern: string): ParsedPattern {

  const excludes = new Set<string>();
  const includes = new Set<string>();
  let wildcard = false;

  for(const part of pattern.split(",").map((p) => p.trim()).filter((p) => p.length > 0)) {

    if(part === "*") {

      wildcard = true;
    } else if(part.startsWith("-")) {

      excludes.add(part.substring(1));
    } else {

      includes.add(part);
    }
  }

  return { excludes, includes, wildcard };
}

/**
 * Re-emits a parsed pattern as its canonical string form: wildcard first, then excludes (prefixed with "-"), then includes, comma-joined. Pure inverse of
 * parsePattern, shared by getCurrentPattern (formatting module state) and canonicalizeDebugPattern (formatting a freshly parsed pattern).
 * @param parsed - The parsed pattern to format.
 * @returns The canonical comma-separated pattern string ("" when nothing is configured).
 */
function formatPattern(parsed: ParsedPattern): string {

  const parts: string[] = [];

  if(parsed.wildcard) {

    parts.push("*");
  }

  for(const entry of parsed.excludes) {

    parts.push("-" + entry);
  }

  for(const entry of parsed.includes) {

    parts.push(entry);
  }

  return parts.join(",");
}

/**
 * Parses a comma-separated pattern string and configures the runtime debug filter. Calling this function replaces any previous filter configuration.
 * @param pattern - Comma-separated list of category patterns (e.g., "tuning:hulu,recovery,-streaming:ffmpeg").
 */
export function initDebugFilter(pattern: string): void {

  const parsed = parsePattern(pattern);

  includeSet = parsed.includes;
  excludeSet = parsed.excludes;
  wildcardEnabled = parsed.wildcard;
  anyEnabled = parsed.wildcard || (parsed.includes.size > 0) || (parsed.excludes.size > 0);
}

/**
 * Re-emits a pattern string in the canonical form getCurrentPattern produces - whitespace trimmed, duplicates collapsed, wildcard then excludes then includes -
 * WITHOUT touching the active runtime filter. Used to normalize a persisted pattern for storage and diffing independently of applying it live.
 * @param pattern - The raw pattern string to canonicalize.
 * @returns The canonical pattern string.
 */
export function canonicalizeDebugPattern(pattern: string): string {

  return formatPattern(parsePattern(pattern));
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

  return formatPattern({ excludes: excludeSet, includes: includeSet, wildcard: wildcardEnabled });
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
 * Parent groups (e.g., "streaming", "timing", "tuning") are derived by the UI from the colon-separated namespaces - only leaf categories are declared here.
 */
export const DEBUG_CATEGORIES: readonly DebugCategory[] = [

  { category: "browser:lifecycle", description: "Browser lifecycle: launch, close, stale page cleanup, restart." },
  { category: "browser:video", description: "Video context, fullscreen, volume locking, playback." },
  { category: "cdp", description: "Enables the Chrome DevTools Protocol proxy at /cdp. Feature gate, not a log filter - observable via its HTTP/WS surface." },
  { category: "config:general", description: "Service groups, version checking." },
  { category: "config:reactivity", description: "Config-change reactivity dispatch: outcomes ignored for paths a handler was not given." },
  { category: "hdhr", description: "HDHomeRun UDP responder: per-packet dispatch traces (Discover, Get, Set, malformed drops), reply send failures." },
  { category: "native:coordinator", description: "Native streaming decisions: interception result, probe result, capture teardown, proxy start." },
  { category: "native:decrypt", description: "AES-128 decryption: key fetch, IV source (explicit vs. sequence), segment sizes." },
  { category: "native:intercept", description: "CDP manifest interception: listener installed, .m3u8 URLs observed, master identified, timeout." },
  { category: "native:manifest", description: "Raw variant manifest body from service - verbose, use for diagnosing DAI/ad stitching issues." },
  { category: "native:monitor", description: "Native health: segment staleness, error counts, L2/L3 recovery actions, capture fallback." },
  { category: "native:probe", description: "DRM probe: variant count, bandwidths, encryption classification, key accessibility, cache hit/miss." },
  { category: "native:proxy", description: "Manifest polling, segment fetch/store, playlist generation, segment rotation, key rotation." },
  { category: "native:token", description: "Token refresh: expiry parsed, timer scheduled, refresh triggered, manifest acquired." },
  { category: "persistence:write", description: "File store writes: per-mutation save to disk after successful atomic temp+rename and integrity check." },
  { category: "precache", description: "Channel lineup precaching: deferred runs, service filter skips." },
  { category: "recovery:context", description: "Video context: frame detachment, re-search." },
  { category: "recovery:general", description: "General recovery: browser re-minimize, monitor abort." },
  { category: "recovery:nav", description: "Page navigation recovery: new tab detection, URL validation." },
  { category: "recovery:resolution", description: "Video resolution monitoring: ABR degradation detection, grace periods, recovery escalation." },
  { category: "recovery:segments", description: "Segment production: self-heal detection." },
  { category: "recovery:tab", description: "Tab replacement: old tab cleanup, new tab creation, retries." },
  { category: "recovery:tracks", description: "Track composition: video traf presence in below-threshold segments." },
  { category: "retry", description: "Retry attempts, page-closed aborts." },
  { category: "streaming:ffmpeg", description: "FFmpeg stderr output, pipe errors." },
  { category: "streaming:hls", description: "HLS segment storage, page close errors." },
  { category: "streaming:logos", description: "Channel logo cache: DVR device extraction, TMS station lookups, cache population." },
  { category: "streaming:mpegts", description: "MPEG-TS client connections, FFmpeg remuxer spawn and errors." },
  { category: "streaming:preroll", description: "Preroll lifecycle: fMP4 generation, preroll playlist delivery, transition to live content." },
  { category: "streaming:pretune", description: "Predictive channel pretuning: DVR job polling, timer scheduling, tune triggers." },
  { category: "streaming:segmenter", description: "fMP4 parsing: keyframes, init segments, duration clamping." },
  { category: "streaming:setup", description: "Stream setup: redirect resolution, profile override, capture init." },
  { category: "streaming:showinfo", description: "Channels DVR show name lookups, device mapping." },
  { category: "timing:browser", description: "Browser launch: process spawn, extension init, display detection." },
  { category: "timing:hls", description: "HLS playlist delivery time." },
  { category: "timing:native", description: "Native streaming: interception latency, probe latency, first segment, refresh duration." },
  { category: "timing:recovery", description: "Recovery totals: navigation recovery, tab replacement." },
  { category: "timing:startup", description: "Stream startup: init segment, first playlist, capture ready." },
  { category: "timing:tab", description: "Tab replacement: old tab cleanup, new page creation." },
  { category: "timing:tune", description: "Tune waterfall: navigation, channel selection, video ready." },
  { category: "tuning:cox", description: "Cox Contour TV: channelmap API interception, SPA channel switching, Watch Now modal dismissal." },
  { category: "tuning:directv", description: "DirecTV Stream: interceptor tuning, cache, logo click fallback." },
  { category: "tuning:fox", description: "Fox.com guide grid: console bridge, page errors, request failures, channel-switch diagnostics." },
  { category: "tuning:hbo", description: "HBO Max: tab URL discovery, channel rail, navigation." },
  { category: "tuning:hulu", description: "Hulu Live guide grid: binary search, cache, click retries." },
  { category: "tuning:sling", description: "Sling TV guide grid: binary search, cache, click retries." },
  { category: "tuning:spectrum", description: "Spectrum TV guide grid: channel discovery, cache, direct URL navigation." },
  { category: "tuning:tileClick", description: "Tile click strategy: scroll phase, matchSelector poll, play button retries, modal dismiss." },
  { category: "tuning:xfinity", description: "Xfinity Stream: channelmap API interception, SPA channel switching, Watch Now modal dismissal." },
  { category: "tuning:yttv", description: "YouTube TV EPG grid navigation." }
];

