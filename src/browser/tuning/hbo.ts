/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hbo.ts: HBO Max channel selection strategy with tab URL caching and channel rail reading.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, DiscoveredChannel, Nullable, ProviderModule } from "../../types/index.js";
import { LOG, evaluateWithAbort, formatError } from "../../utils/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { logAvailableChannels } from "../channelSelection.js";

// Base URL for HBO Max watch page navigation and tab URL construction.
const HBO_MAX_BASE_URL = "https://play.hbomax.com";

// Internal cache entry combining discovery metadata and tuning data. The discovered field provides the API-facing DiscoveredChannel (name, channelSelector), and
// the watchUrl provides the direct navigation target for tuning. Both are populated from the same readHboChannelRail() result, ensuring a single source of truth
// for "what channels exist" and "how to tune to them."
interface HboChannelEntry {

  discovered: DiscoveredChannel;
  watchUrl: string;
}

// Unified channel cache for HBO Max. Maps lowercased channel names (e.g., "hbo", "hbo hits") to their combined discovery and tuning data. Populated during the
// first tune (when the strategy reads all channels from the channel rail) or the first discovery call. Both tuning (via resolveHboDirectUrl) and discovery (via
// getCachedChannels / discoverHboChannels) read from this single cache. Cleared on browser disconnect via clearHboCache().
const hboChannelCache = new Map<string, HboChannelEntry>();

// Module-level cache for the HBO tab page URL discovered from the homepage menu bar. Cleared on browser disconnect (via clearHboCache) and inline when the
// cached URL turns out to be stale (the channel rail is not found at the cached URL). Separate from the channel cache because it's a navigation target, not
// channel data.
let hboTabUrl: Nullable<string> = null;

/**
 * Returns a cached HBO Max watch URL for the given channel selector, or null if no cached URL exists.
 * @param channelSelector - The channel selector string (e.g., "HBO", "HBO Hits").
 * @returns The cached watch URL or null.
 */
function resolveHboDirectUrl(channelSelector: string): Nullable<string> {

  const entry = hboChannelCache.get(channelSelector.toLowerCase());

  if(entry) {

    LOG.debug("tuning:hbo", "HBO cache hit for %s: %s.", channelSelector, entry.watchUrl);

    return entry.watchUrl;
  }

  return null;
}

/**
 * Invalidates the cached HBO Max entry for the given channel selector. Called when a cached URL fails to produce a working stream.
 * @param channelSelector - The channel selector string to invalidate.
 */
function invalidateHboDirectUrl(channelSelector: string): void {

  hboChannelCache.delete(channelSelector.toLowerCase());
}

/**
 * Clears all HBO caches: the unified channel cache and the tab URL. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts, since
 * cached state may be stale in a new browser session.
 */
function clearHboCache(): void {

  hboChannelCache.clear();
  hboTabUrl = null;
}

/**
 * Derives a DiscoveredChannel array from the unified channel cache. HBO does not create alias entries (resolveHboDirectUrl does exact-match only, no
 * prefix/alternate fallback), so deduplication is not needed — every cache value is unique. Sorts by name before returning.
 * @returns Sorted array of discovered channels.
 */
function buildHboDiscoveredChannels(): DiscoveredChannel[] {

  return Array.from(hboChannelCache.values()).map((entry) => entry.discovered).sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * Populates the unified channel cache from raw channel rail data. For each channel, builds a DiscoveredChannel and pairs it with the full watch URL.
 * Shared by hboGridStrategy (tuning-time population) and discoverHboChannels (discovery endpoint).
 * @param rawChannels - Array of channel names and watch paths from readHboChannelRail().
 */
function populateHboChannelCache(rawChannels: { name: string; watchPath: string }[]): void {

  for(const ch of rawChannels) {

    const watchUrl = HBO_MAX_BASE_URL + ch.watchPath;

    hboChannelCache.set(ch.name.toLowerCase(), { discovered: { channelSelector: ch.name, name: ch.name }, watchUrl });

    LOG.debug("tuning:hbo", "Cached HBO Max watch URL for %s: %s.", ch.name, watchUrl);
  }
}

/**
 * Reads the HBO tab URL from the homepage menu bar. The HBO brand page is linked via an `a[aria-label="H B O"]` element in the top navigation. The href attribute
 * contains a relative path like `/channel/c0d1f27a-...` which we combine with the base URL to form the full page URL.
 * @param page - The Puppeteer page object, expected to be on the HBO Max homepage.
 * @returns The full HBO tab page URL, or null if the tab link was not found.
 */
async function readHboTabUrl(page: Page): Promise<Nullable<string>> {

  // Wait for the HBO tab link to appear in the menu bar. The homepage is a single-page application that renders the navigation dynamically after the initial HTML
  // shell loads. Without this wait, the evaluate call below would run against an incomplete DOM and fail to find the tab link.
  const HBO_TAB_SELECTOR = "a[aria-label=\"H B O\"]";

  try {

    await page.waitForSelector(HBO_TAB_SELECTOR, { timeout: 5000 });
  } catch {

    return null;
  }

  const href = await evaluateWithAbort(page, (selector: string): Nullable<string> => {

    const tab = document.querySelector(selector) as Nullable<HTMLAnchorElement>;

    if(!tab) {

      return null;
    }

    return tab.getAttribute("href");
  }, [HBO_TAB_SELECTOR]);

  if(!href) {

    return null;
  }

  return HBO_MAX_BASE_URL + href;
}

// Result of reading the HBO channel rail on the tab page. Distinguishes between the rail not being found (stale URL, wrong page) and the rail being found with
// its discovered channels.
interface HboRailResult {

  channels: { name: string; watchPath: string }[];
  railFound: boolean;
}

/**
 * Reads all channels from the HBO Channels tile rail on the HBO tab page. The rail section contains tiles for each live channel, each with a backup text
 * `<p aria-hidden="true">` element containing the channel name and an `<a>` with href pointing to the watch page. Returns all discovered channels so the caller
 * can populate the cache in bulk.
 * @param page - The Puppeteer page object, expected to be on the HBO tab page.
 * @returns Object with `railFound` indicating whether the rail section was present, and `channels` containing all discovered channel names and watch paths.
 */
async function readHboChannelRail(page: Page): Promise<HboRailResult> {

  const HBO_RAIL_SELECTOR = "section[data-testid=\"hbo-page-rail-distribution-channels-us_rail\"]";

  // Wait for the distribution channels rail section to appear. If it doesn't appear, the tab URL may be stale or the page structure changed.
  try {

    await page.waitForSelector(HBO_RAIL_SELECTOR, { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return { channels: [], railFound: false };
  }

  // The rail uses lazy loading via IntersectionObserver — tile content only populates when the rail is visible in the viewport. The rail section element appears
  // immediately with skeleton PhantomTile placeholders, but the actual channel tiles (with names and watch URLs) are fetched asynchronously after the rail scrolls
  // into view. We scroll the rail into view and then wait for anchor elements to appear, indicating the tiles have loaded.
  await page.evaluate((selector: string): void => {

    document.querySelector(selector)?.scrollIntoView({ behavior: "instant", block: "center" });
  }, HBO_RAIL_SELECTOR);

  try {

    await page.waitForSelector(HBO_RAIL_SELECTOR + " a", { timeout: 5000 });
  } catch {

    return { channels: [], railFound: false };
  }

  // Read all channels from the rail. Each tile contains an anchor with the watch URL and a backup text paragraph with the channel name.
  const channels = await evaluateWithAbort(page, (selector: string): { name: string; watchPath: string }[] => {

    const rail = document.querySelector(selector);

    if(!rail) {

      return [];
    }

    const results: { name: string; watchPath: string }[] = [];

    for(const anchor of Array.from(rail.querySelectorAll("a"))) {

      const nameEl = anchor.querySelector("p[aria-hidden=\"true\"]");

      if(!nameEl) {

        continue;
      }

      const name = (nameEl.textContent || "").trim();

      if(name.length === 0) {

        continue;
      }

      const href = anchor.getAttribute("href");

      // Validate that the href points to a live channel watch page. Watch URLs follow the pattern /channel/watch/{channelUUID}/{programUUID}.
      if(href?.includes("/channel/watch/")) {

        results.push({ name, watchPath: href });
      }
    }

    return results;
  }, [HBO_RAIL_SELECTOR]);

  return { channels, railFound: true };
}

/**
 * HBO grid strategy: discovers the HBO channels tab URL from the homepage menu bar, navigates to the tab page, reads the live channel rail for all channel
 * watch URLs, and navigates to the target channel's URL. All discovered channels are cached so that subsequent tunes to any HBO channel resolve via resolveDirectUrl
 * without loading the tab page.
 *
 * The strategy handles three navigations per tune:
 * 1. Homepage (already loaded by navigateToPage) → read menu bar for tab URL (or use cache)
 * 2. Tab page → read channel rail for all watch URLs
 * 3. Watch page → video playback begins
 *
 * When the cached tab URL is stale (rail section not found), the strategy clears the cache, navigates back to the homepage, rediscovers the tab URL, and retries.
 * This fallback triggers at most once per tune attempt.
 * @param page - The Puppeteer page object, expected to be on the HBO Max homepage.
 * @param profile - The resolved site profile with a non-null channelSelector (channel name, e.g., "HBO", "HBO Hits").
 * @returns Result object with success status and optional failure reason.
 */
async function hboGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelName = profile.channelSelector;
  let usedCache = false;

  // Phase 1: Navigate to the HBO tab page. Use cached URL if available, otherwise discover it from the homepage menu bar.
  if(hboTabUrl) {

    usedCache = true;

    LOG.debug("tuning:hbo", "Using cached HBO tab URL: %s.", hboTabUrl);
  } else {

    const discovered = await readHboTabUrl(page);

    if(!discovered) {

      return { reason: "HBO tab not found in homepage menu bar. HBO Max subscription may not be active.", success: false };
    }

    hboTabUrl = discovered;

    LOG.debug("tuning:hbo", "Discovered HBO tab URL: %s.", hboTabUrl);
  }

  try {

    await page.goto(hboTabUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
  } catch(error) {

    return { reason: "Failed to navigate to HBO tab page: " + formatError(error) + ".", success: false };
  }

  // Phase 2: Read the channel rail for all channel watch URLs.
  let railResult = await readHboChannelRail(page);

  // Fallback: if the rail was not found and we used a cached URL, the cache may be stale. Clear it, navigate back to the homepage, rediscover the tab URL, and retry.
  if(!railResult.railFound && usedCache) {

    LOG.debug("tuning:hbo", "HBO channel rail not found at cached URL. Rediscovering tab URL from homepage.");

    hboTabUrl = null;

    try {

      await page.goto(HBO_MAX_BASE_URL, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
    } catch(error) {

      return { reason: "Failed to navigate back to HBO Max homepage: " + formatError(error) + ".", success: false };
    }

    const rediscovered = await readHboTabUrl(page);

    if(!rediscovered) {

      return { reason: "HBO tab not found in homepage menu bar after cache invalidation.", success: false };
    }

    hboTabUrl = rediscovered;

    LOG.debug("tuning:hbo", "Rediscovered HBO tab URL: %s.", hboTabUrl);

    try {

      await page.goto(hboTabUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
    } catch(error) {

      return { reason: "Failed to navigate to rediscovered HBO tab page: " + formatError(error) + ".", success: false };
    }

    railResult = await readHboChannelRail(page);

    if(!railResult.railFound) {

      return { reason: "HBO channel rail not found at rediscovered URL. Site structure may have changed.", success: false };
    }
  }

  if(!railResult.railFound) {

    return { reason: "HBO channel rail not found on tab page.", success: false };
  }

  // Populate the unified channel cache with all discovered channels. Always repopulate rather than skipping when the cache has entries, because invalidated
  // entries (deleted by invalidateHboDirectUrl) need to be restored with fresh watch URLs from the rail.
  populateHboChannelCache(railResult.channels);

  // Look up the target channel from the populated cache.
  const watchUrl = resolveHboDirectUrl(channelName);

  if(!watchUrl) {

    // Channel not found. Log available channels from the rail data to help users identify valid channelSelector values.
    logAvailableChannels({

      availableChannels: railResult.channels.map((ch) => ch.name).sort(),
      channelName,
      guideUrl: "https://play.hbomax.com",
      providerName: "HBO Max"
    });

    return { reason: "Channel " + channelName + " not found in HBO channel rail.", success: false };
  }

  // Phase 3: Navigate to the watch URL to start playback.
  LOG.debug("tuning:hbo", "Navigating to HBO Max watch URL for %s.", channelName);

  try {

    await page.goto(watchUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
  } catch(error) {

    return { reason: "Failed to navigate to HBO Max watch page: " + formatError(error) + ".", success: false };
  }

  return { success: true };
}

/**
 * Async wrapper around resolveHboDirectUrl for the ChannelStrategyEntry.resolveDirectUrl contract. The page parameter is unused because HBO watch URLs are
 * resolved purely from the in-memory cache populated during the first channel rail read.
 * @param channelSelector - The channel selector string (e.g., "HBO", "HBO Hits").
 * @param _page - Unused. Present to satisfy the async resolveDirectUrl signature.
 * @returns The cached watch URL or null.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars, @typescript-eslint/require-await
async function resolveHboDirectUrlAsync(channelSelector: string, _page: Page): Promise<Nullable<string>> {

  return resolveHboDirectUrl(channelSelector);
}

/**
 * Discovers all channels from the HBO Max channel rail. Returns cached results if the unified channel cache is populated from a prior tune or discovery call.
 * Otherwise, extracts the HBO tab URL from the homepage menu bar, navigates to the tab page, reads all channels from the distribution channels rail, and
 * populates the cache (unless empty, to allow retries on transient failures or missing HBO subscription).
 * @param page - The Puppeteer page object, already on the HBO Max homepage (navigated by the route handler).
 * @returns Array of discovered channels.
 */
async function discoverHboChannels(page: Page): Promise<DiscoveredChannel[]> {

  // Return from the unified cache if already populated.
  if(hboChannelCache.size > 0) {

    return buildHboDiscoveredChannels();
  }

  // Discover the HBO tab URL from the homepage menu bar.
  const tabUrl = await readHboTabUrl(page);

  if(!tabUrl) {

    return [];
  }

  // Navigate to the HBO tab page.
  try {

    await page.goto(tabUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
  } catch {

    return [];
  }

  // Read the channel rail.
  const railResult = await readHboChannelRail(page);

  if(!railResult.railFound || (railResult.channels.length === 0)) {

    return [];
  }

  populateHboChannelCache(railResult.channels);

  return buildHboDiscoveredChannels();
}

/**
 * Returns cached discovered channels from the unified channel cache, or null if the cache is empty (no prior tune or discovery call has read the channel rail).
 * @returns Sorted array of discovered channels or null.
 */
function getHboCachedChannels(): Nullable<DiscoveredChannel[]> {

  if(hboChannelCache.size === 0) {

    return null;
  }

  return buildHboDiscoveredChannels();
}

export const hboProvider: ProviderModule = {

  discoverChannels: discoverHboChannels,
  getCachedChannels: getHboCachedChannels,
  guideUrl: "https://play.hbomax.com",
  label: "HBO Max",
  slug: "hbo",
  strategy: {

    clearCache: clearHboCache,
    execute: hboGridStrategy,
    invalidateDirectUrl: invalidateHboDirectUrl,
    resolveDirectUrl: resolveHboDirectUrlAsync
  },
  strategyName: "hboGrid"
};
