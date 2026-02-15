/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hbo.ts: HBO Max channel selection strategy with tab URL caching and channel rail scraping.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ChannelStrategyEntry, Nullable } from "../../types/index.js";
import { LOG, evaluateWithAbort, formatError } from "../../utils/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { logAvailableChannels } from "../channelSelection.js";

// Base URL for HBO Max watch page navigation and tab URL construction.
const HBO_MAX_BASE_URL = "https://play.hbomax.com";

// Module-level cache for the HBO tab page URL discovered from the homepage menu bar. Cleared on browser disconnect (via clearHboCache) and inline when the
// cached URL turns out to be stale (the channel rail is not found at the cached URL).
let hboTabUrl: Nullable<string> = null;

// Module-level cache for HBO watch URLs discovered during channel rail scraping. On the first tune to any HBO channel, the strategy performs a bulk scrape of all
// channels from the HBO Channels tile rail, populating this cache with every channel's watch URL keyed by its lowercased name (e.g., "hbo", "hbo 2", "hbo hits").
// Subsequent tunes resolve via resolveHboDirectUrl without loading the tab page. Cleared on browser disconnect via clearHboCache().
const hboWatchUrlCache = new Map<string, string>();

/**
 * Returns a cached HBO Max watch URL for the given channel selector, or null if no cached URL exists.
 * @param channelSelector - The channel selector string (e.g., "HBO", "HBO Hits").
 * @returns The cached watch URL or null.
 */
function resolveHboDirectUrl(channelSelector: string): Nullable<string> {

  const url = hboWatchUrlCache.get(channelSelector.toLowerCase()) ?? null;

  if(url) {

    LOG.debug("tuning:hbo", "HBO cache hit for %s: %s.", channelSelector, url);
  }

  return url;
}

/**
 * Invalidates the cached HBO Max watch URL for the given channel selector. Called when a cached URL fails to produce a working stream.
 * @param channelSelector - The channel selector string to invalidate.
 */
function invalidateHboDirectUrl(channelSelector: string): void {

  hboWatchUrlCache.delete(channelSelector.toLowerCase());
}

/**
 * Clears the HBO tab URL and watch URL caches. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts.
 */
function clearHboCache(): void {

  hboTabUrl = null;
  hboWatchUrlCache.clear();
}

/**
 * Scrapes the HBO tab URL from the homepage menu bar. The HBO brand page is linked via an `a[aria-label="H B O"]` element in the top navigation. The href attribute
 * contains a relative path like `/channel/c0d1f27a-...` which we combine with the base URL to form the full page URL.
 * @param page - The Puppeteer page object, expected to be on the HBO Max homepage.
 * @returns The full HBO tab page URL, or null if the tab link was not found.
 */
async function scrapeHboTabUrl(page: Page): Promise<Nullable<string>> {

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

// Result of scraping the HBO channel rail on the tab page. Distinguishes between the rail not being found (stale URL, wrong page) and the rail being found with
// its discovered channels.
interface HboRailResult {

  channels: { name: string; watchPath: string }[];
  railFound: boolean;
}

/**
 * Scrapes all channels from the HBO Channels tile rail on the HBO tab page. The rail section contains tiles for each live channel, each with a backup text
 * `<p aria-hidden="true">` element containing the channel name and an `<a>` with href pointing to the watch page. Returns all discovered channels so the caller
 * can populate the cache in bulk.
 * @param page - The Puppeteer page object, expected to be on the HBO tab page.
 * @returns Object with `railFound` indicating whether the rail section was present, and `channels` containing all discovered channel names and watch paths.
 */
async function scrapeHboChannelRail(page: Page): Promise<HboRailResult> {

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

  // Bulk scrape all channels from the rail. Each tile contains an anchor with the watch URL and a backup text paragraph with the channel name.
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
 * HBO grid strategy: discovers the HBO channels tab URL from the homepage menu bar, navigates to the tab page, bulk scrapes the live channel rail for all channel
 * watch URLs, and navigates to the target channel's URL. All discovered channels are cached so that subsequent tunes to any HBO channel resolve via resolveDirectUrl
 * without loading the tab page.
 *
 * The strategy handles three navigations per tune:
 * 1. Homepage (already loaded by navigateToPage) → scrape menu bar for tab URL (or use cache)
 * 2. Tab page → bulk scrape channel rail for all watch URLs
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

    const discovered = await scrapeHboTabUrl(page);

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

  // Phase 2: Bulk scrape the channel rail for all channel watch URLs.
  let railResult = await scrapeHboChannelRail(page);

  // Fallback: if the rail was not found and we used a cached URL, the cache may be stale. Clear it, navigate back to the homepage, rediscover the tab URL, and retry.
  if(!railResult.railFound && usedCache) {

    LOG.debug("tuning:hbo", "HBO channel rail not found at cached URL. Rediscovering tab URL from homepage.");

    hboTabUrl = null;

    try {

      await page.goto(HBO_MAX_BASE_URL, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
    } catch(error) {

      return { reason: "Failed to navigate back to HBO Max homepage: " + formatError(error) + ".", success: false };
    }

    const rediscovered = await scrapeHboTabUrl(page);

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

    railResult = await scrapeHboChannelRail(page);

    if(!railResult.railFound) {

      return { reason: "HBO channel rail not found at rediscovered URL. Site structure may have changed.", success: false };
    }
  }

  if(!railResult.railFound) {

    return { reason: "HBO channel rail not found on tab page.", success: false };
  }

  // Populate the watch URL cache with all discovered channels. This makes every subsequent HBO tune a cache hit via resolveDirectUrl, skipping tab page navigation
  // entirely.
  for(const ch of railResult.channels) {

    const watchUrl = HBO_MAX_BASE_URL + ch.watchPath;

    hboWatchUrlCache.set(ch.name.toLowerCase(), watchUrl);

    LOG.debug("tuning:hbo", "Cached HBO Max watch URL for %s: %s.", ch.name, watchUrl);
  }

  // Look up the target channel from the populated cache.
  const watchUrl = hboWatchUrlCache.get(channelName.toLowerCase());

  if(!watchUrl) {

    // Channel not found. Log available channels from the scraped rail data to help users identify valid channelSelector values.
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
 * resolved purely from the in-memory cache populated during the first channel rail scrape.
 * @param channelSelector - The channel selector string (e.g., "HBO", "HBO Hits").
 * @param _page - Unused. Present to satisfy the async resolveDirectUrl signature.
 * @returns The cached watch URL or null.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars, @typescript-eslint/require-await
async function resolveHboDirectUrlAsync(channelSelector: string, _page: Page): Promise<Nullable<string>> {

  return resolveHboDirectUrl(channelSelector);
}

export const hboStrategy: ChannelStrategyEntry = {

  clearCache: clearHboCache,
  execute: hboGridStrategy,
  invalidateDirectUrl: invalidateHboDirectUrl,
  resolveDirectUrl: resolveHboDirectUrlAsync
};
