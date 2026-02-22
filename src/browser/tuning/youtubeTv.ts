/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * youtubeTv.ts: YouTube TV EPG grid channel selection strategy.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ChannelStrategyEntry, Nullable } from "../../types/index.js";
import { LOG, evaluateWithAbort, formatError } from "../../utils/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { logAvailableChannels } from "../channelSelection.js";

// Base URL for YouTube TV watch page navigation.
const YOUTUBE_TV_BASE_URL = "https://tv.youtube.com";

// Module-level cache for watch URLs discovered during channel selection. On the first tune to any YTTV channel, the strategy discovers all ~256 channels from the
// non-virtualized EPG grid, populating this cache with every channel's watch URL keyed by its lowercased guide name (e.g., "cnn", "nbc 5", "espn"). Subsequent
// tunes to any YTTV channel resolve via findWatchUrl() in resolveDirectUrl, skipping guide navigation entirely. Cleared on browser disconnect via clearYttvCache().
const yttvWatchUrlCache = new Map<string, string>();

// Tracks consecutive guide page loads that discover zero channels. When this reaches the recovery threshold, the strategy clears YTTV site data (service workers
// and cache storage) via CDP and reloads the guide to break out of a degraded state where the guide grid container renders but channel entries are not populated.
// Reset to zero on any successful discovery (> 0 channels found) or on browser restart (via clearYttvCache).
let consecutiveEmptyDiscoveries = 0;

// Number of consecutive empty discoveries before attempting site data recovery via CDP.
const EMPTY_DISCOVERY_RECOVERY_THRESHOLD = 3;

// Known alternate channel names for affiliates that vary by market. CW appears as "WGN" in some markets. PBS affiliates appear under local call letters (e.g.,
// WTTW, KQED) or branded names (e.g., "Cascade PBS", "Lakeshore PBS") rather than "PBS", so we list the major market call letters and branded names to cover most
// users. Each alternate is tried after the primary name fails both exact and prefix+digit matching. Users in smaller markets override via custom channel entries with
// their local call letters as the channelSelector.
const CHANNEL_ALTERNATES: Record<string, string[]> = {

  "cw": ["WGN"],
  "pbs": [
    "Cascade PBS", "GBH", "KAET", "KBTC", "KCET", "KCTS", "KERA", "KLCS", "KOCE", "KPBS", "KQED", "KRMA", "KUHT", "KVIE", "Lakeshore PBS", "MPT", "NJ PBS",
    "THIRTEEN", "TPT", "WETA", "WGBH", "WHYY", "WLIW", "WNED", "WNET", "WNIT", "WPBA", "WPBT", "WTTW", "WTVS", "WXEL"
  ]
};

/**
 * Looks up a watch URL in the cache using the same three-tier matching logic as the guide grid DOM query. The tiers are tried in order for each name in the
 * candidate list (primary channelSelector first, then any CHANNEL_ALTERNATES):
 *
 * 1. Exact match: cache key equals the lowercased name (e.g., "cnn" matches "cnn").
 * 2. Prefix+digit: cache key starts with the name followed by a space and a digit. Catches local affiliates displayed as "{Network} {Number}" (e.g., "nbc 5",
 *    "abc 7") while excluding unrelated channels that share the prefix (e.g., "nbc sports").
 * 3. Parenthetical suffix: cache key starts with the name followed by " (". Catches timezone/region variants like "magnolia network (pacific)".
 *
 * When a non-exact match succeeds, the result is also cached under the primary channelSelector key for O(1) lookup on subsequent calls. This function doubles as
 * the resolveDirectUrl hook — after the first tune populates the cache via channel discovery, every subsequent YTTV tune resolves here without loading the guide
 * page.
 * @param channelName - The channelSelector value (e.g., "CNN", "NBC", "CW").
 * @returns The full watch URL or null if no match is found.
 */
function findWatchUrl(channelName: string): Nullable<string> {

  const lower = channelName.toLowerCase();

  // Build the candidate list: primary name first, then any known alternates for markets where the affiliate uses a different name. The eslint disable is needed
  // because TypeScript's Record indexing doesn't capture that the key may not exist at runtime.
  const alternates = CHANNEL_ALTERNATES[lower];

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  const namesToTry = alternates ? [ lower, ...alternates.map((a) => a.toLowerCase()) ] : [lower];

  for(const name of namesToTry) {

    // Tier 1: Exact match.
    const exact = yttvWatchUrlCache.get(name);

    if(exact) {

      // Cache under the primary channelSelector key if we matched via an alternate name, so subsequent lookups are O(1).
      if(name !== lower) {

        yttvWatchUrlCache.set(lower, exact);
      }

      return exact;
    }

    // Tier 2: Prefix+digit match for local affiliates. Iterate all cache entries to find one whose key starts with "{name} " followed by a digit, matching the
    // "{Network} {Number}" pattern (e.g., "nbc 5", "abc 7") while excluding unrelated channels that share the prefix (e.g., "nbc sports").
    for(const [ key, url ] of yttvWatchUrlCache) {

      if(key.startsWith(name + " ") && (key.length > name.length + 1) && (key.charCodeAt(name.length + 1) >= 48) && (key.charCodeAt(name.length + 1) <= 57)) {

        yttvWatchUrlCache.set(lower, url);

        return url;
      }
    }

    // Tier 3: Parenthetical suffix match for timezone/region variants. Find a cache entry whose key starts with "{name} (" to catch channels like
    // "magnolia network (pacific)" or "the filipino channel (pacific)".
    for(const [ key, url ] of yttvWatchUrlCache) {

      if(key.startsWith(name + " (")) {

        yttvWatchUrlCache.set(lower, url);

        return url;
      }
    }
  }

  return null;
}

/**
 * Invalidates the cached YouTube TV watch URL for the given channel selector. Called when a cached URL fails to produce a working stream. Deletes the
 * channelSelector key — the guide-name entries from channel discovery are left intact and will be refreshed on the next strategy run when the guide page is
 * reloaded.
 * @param channelSelector - The channel selector string to invalidate.
 */
function invalidateYttvDirectUrl(channelSelector: string): void {

  yttvWatchUrlCache.delete(channelSelector.toLowerCase());
}

/**
 * Clears all cached YouTube TV watch URLs and resets the empty discovery counter. Called by clearChannelSelectionCaches() in the coordinator when the browser
 * restarts, since a fresh browser session resolves the degraded guide state that the counter tracks.
 */
function clearYttvCache(): void {

  yttvWatchUrlCache.clear();
  consecutiveEmptyDiscoveries = 0;
}

/**
 * Discovers all channels from the YouTube TV EPG grid in a single evaluate round-trip. For each thumbnail endpoint with a valid watch/ href, extracts the channel
 * name (from the aria-label, stripping the "watch " prefix) and the watch path. Channels with "live" or "browse/" hrefs are premium add-ons or info pages and are
 * excluded. Returns an empty array if no channels are found (e.g., guide in a degraded state) or if the evaluate is aborted.
 * @param page - The Puppeteer page object positioned on the YouTube TV live guide.
 * @returns Array of discovered channel names and watch paths.
 */
async function discoverGuideChannels(page: Page): Promise<{ name: string; watchPath: string }[]> {

  return await evaluateWithAbort(page, (): { name: string; watchPath: string }[] => {

    const results: { name: string; watchPath: string }[] = [];

    for(const thumb of Array.from(document.querySelectorAll("ytu-endpoint.tenx-thumb[aria-label]"))) {

      const label = thumb.getAttribute("aria-label") ?? "";

      if(!label.startsWith("watch ")) {

        continue;
      }

      const anchor = thumb.querySelector("a");
      const href = anchor?.getAttribute("href") ?? "";

      // Only include channels with streamable watch URLs. Channels with "live" or "browse/" hrefs are premium add-ons or info pages.
      if(href.startsWith("watch/")) {

        results.push({ name: label.slice(6), watchPath: href });
      }
    }

    return results;
  }, []);
}

/**
 * Attempts to recover from a degraded YouTube TV guide state by clearing cached site data (service workers and cache storage) via CDP and reloading the guide page.
 * This targets the specific failure mode where the guide grid container renders but channel entries are not populated, typically caused by stale browser session
 * state after a Chrome update. Cookies and login session are preserved — only caching layers are cleared.
 * @param page - The Puppeteer page object.
 * @returns Discovered channels after recovery, or an empty array if recovery failed.
 */
async function attemptGuideRecovery(page: Page): Promise<{ name: string; watchPath: string }[]> {

  LOG.warn("Clearing YouTube TV cached site data to recover from empty guide.");

  // Clear service workers and cache storage for the YouTube TV origin. These are caching layers that will be repopulated on reload — cookies and login session
  // state are deliberately preserved to avoid forcing re-authentication.
  try {

    const client = await page.createCDPSession();

    await client.send("Storage.clearDataForOrigin", { origin: YOUTUBE_TV_BASE_URL, storageTypes: "cache_storage,service_workers" });
    await client.detach();
  } catch(error) {

    LOG.warn("Failed to clear YouTube TV site data: %s.", formatError(error));

    return [];
  }

  // Reload the guide page with fresh state.
  try {

    await page.goto(YOUTUBE_TV_BASE_URL + "/live", { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
  } catch(error) {

    LOG.warn("Failed to reload YouTube TV guide after clearing site data: %s.", formatError(error));

    return [];
  }

  // Wait for the EPG grid to render after reload.
  try {

    await page.waitForSelector("ytu-epg-row", { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    LOG.warn("YouTube TV guide grid did not load after clearing site data.");

    return [];
  }

  // Re-attempt channel discovery on the reloaded page.
  const channels = await discoverGuideChannels(page);

  if(channels.length > 0) {

    LOG.info("YouTube TV guide recovery succeeded — discovered %s channels after clearing site data.", channels.length);
  } else {

    LOG.warn("YouTube TV guide still empty after clearing site data.");
  }

  return channels;
}

/**
 * YouTube TV grid strategy: discovers all watch URLs from the non-virtualized EPG grid at tv.youtube.com/live in a single pass, populating the module-level cache
 * so that subsequent tunes to any YTTV channel resolve via findWatchUrl() without loading the guide page. All ~256 channel rows are present in the DOM
 * simultaneously, so one querySelectorAll captures every channel's name and watch URL.
 *
 * The selection process:
 * 1. Wait for ytu-epg-row elements to confirm the guide grid has loaded.
 * 2. Discover all channels: extract aria-label names and watch/ hrefs from every thumbnail endpoint.
 * 3. If no channels are discovered (degraded guide state), attempt recovery by clearing cached site data via CDP and reloading.
 * 4. Populate the watch URL cache with all discovered channels.
 * 5. Look up the target channel using tiered matching (exact, prefix+digit, parenthetical, alternates) against the cache.
 * 6. Navigate to the matched watch URL via page.goto().
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector (channel name, e.g., "CNN", "ESPN", "NBC").
 * @returns Result object with success status and optional failure reason.
 */
async function youtubeGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelName = profile.channelSelector;

  // Wait for the EPG grid to render. All ~256 rows load simultaneously (no virtualization), so once any row exists, all channels are queryable.
  try {

    await page.waitForSelector("ytu-epg-row", { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return { reason: "YouTube TV guide grid did not load.", success: false };
  }

  // Discover all channels from the guide grid.
  let allChannels = await discoverGuideChannels(page);

  // If the guide loaded but no channels were discovered, the guide is in a degraded state — the grid container rendered but channel entries were not populated.
  // This can happen when stale browser session state (service workers, cached SPA code) becomes inconsistent after a Chrome update. Track consecutive occurrences
  // and attempt recovery by clearing cached site data once the threshold is reached.
  if(allChannels.length === 0) {

    consecutiveEmptyDiscoveries++;

    LOG.warn("YouTube TV guide loaded but no channels were discovered (%s consecutive). The guide may be in a degraded state.",
      consecutiveEmptyDiscoveries);

    if(consecutiveEmptyDiscoveries >= EMPTY_DISCOVERY_RECOVERY_THRESHOLD) {

      allChannels = await attemptGuideRecovery(page);
    }
  }

  // If we still have no channels after the initial discovery and any recovery attempt, there is nothing to search or cache. This error message is deliberately
  // distinct from the name-mismatch "not found" message below so users can immediately tell the guide itself is broken rather than suspecting a wrong channel name.
  if(allChannels.length === 0) {

    return { reason: "YouTube TV guide is empty — no channels were discovered.", success: false };
  }

  // Successful discovery — reset the consecutive empty counter.
  consecutiveEmptyDiscoveries = 0;

  // Populate the watch URL cache with all discovered channels. This makes every subsequent YTTV tune a cache hit via resolveDirectUrl, skipping guide navigation
  // entirely. Cache keys are lowercased guide names (e.g., "cnn", "nbc 5", "espn"). The tiered matching in findWatchUrl() handles channelSelector-to-guide-name
  // resolution (e.g., "NBC" finds "nbc 5" via prefix+digit matching, "CW" finds the local affiliate via CHANNEL_ALTERNATES).
  for(const ch of allChannels) {

    yttvWatchUrlCache.set(ch.name.toLowerCase(), YOUTUBE_TV_BASE_URL + "/" + ch.watchPath);
  }

  LOG.debug("tuning:yttv", "Discovered %s YouTube TV channels.", allChannels.length);

  // Look up the target channel using tiered matching against the populated cache.
  const watchUrl = findWatchUrl(channelName);

  if(!watchUrl) {

    // Channel not found. Log available channels as a diagnostic to help users identify their market's channel names and create user-defined channels with the
    // correct channelSelector value. Build additional known names from CHANNEL_ALTERNATES values so they are also filtered out of the diagnostic list.
    const additionalKnownNames: string[] = [];

    for(const alts of Object.values(CHANNEL_ALTERNATES)) {

      for(const alt of alts) {

        additionalKnownNames.push(alt);
      }
    }

    logAvailableChannels({

      additionalKnownNames,
      availableChannels: allChannels.map((ch) => ch.name).sort(),
      channelName,
      guideUrl: "https://tv.youtube.com/live",
      presetSuffix: "-yttv",
      providerName: "YouTube TV"
    });

    return { reason: "Channel \"" + channelName + "\" not found in YouTube TV guide.", success: false };
  }

  LOG.debug("tuning:yttv", "Navigating to YouTube TV watch URL for %s.", channelName);

  try {

    await page.goto(watchUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
  } catch(error) {

    return { reason: "Failed to navigate to YouTube TV watch page: " + formatError(error) + ".", success: false };
  }

  return { success: true };
}

/**
 * Async wrapper around findWatchUrl for the ChannelStrategyEntry.resolveDirectUrl contract. The page parameter is unused because YTTV watch URLs are resolved
 * purely from the in-memory cache populated during the initial guide page discovery.
 * @param channelSelector - The channel selector string (e.g., "CNN", "ESPN", "NBC").
 * @param _page - Unused. Present to satisfy the async resolveDirectUrl signature.
 * @returns The cached watch URL or null.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars, @typescript-eslint/require-await
async function resolveYttvDirectUrl(channelSelector: string, _page: Page): Promise<Nullable<string>> {

  return findWatchUrl(channelSelector);
}

export const yttvStrategy: ChannelStrategyEntry = {

  clearCache: clearYttvCache,
  execute: youtubeGridStrategy,
  invalidateDirectUrl: invalidateYttvDirectUrl,
  resolveDirectUrl: resolveYttvDirectUrl
};
