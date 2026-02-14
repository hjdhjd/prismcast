/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hbo.ts: HBO Max channel selection strategy with tab URL caching and channel rail scraping.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, Nullable } from "../../types/index.js";
import { LOG, evaluateWithAbort, formatError } from "../../utils/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { logAvailableChannels } from "../channelSelection.js";

// Base URL for HBO Max watch page navigation and tab URL construction.
const HBO_MAX_BASE_URL = "https://play.hbomax.com";

// Module-level cache for the HBO tab page URL discovered from the homepage menu bar. Cleared on browser disconnect (via clearHboCache) and inline when the
// cached URL turns out to be stale (the channel rail is not found at the cached URL).
let hboTabUrl: Nullable<string> = null;

/**
 * Clears the HBO tab URL cache. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts.
 */
export function clearHboCache(): void {

  hboTabUrl = null;
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

// Result of scraping the HBO channel rail on the tab page. Distinguishes between the rail not being found (stale URL, wrong page) and the rail being found but the
// target channel not existing within it.
interface HboRailResult {

  railFound: boolean;
  watchPath: Nullable<string>;
}

/**
 * Scrapes the HBO Channels tile rail on the HBO tab page for a watch URL matching the given channel name. The rail section contains tiles for each live channel,
 * each with a backup text `<p aria-hidden="true">` element containing the channel name and an `<a>` with href pointing to the watch page.
 * @param page - The Puppeteer page object, expected to be on the HBO tab page.
 * @param channelName - The channel name to match (e.g., "HBO", "HBO Hits"). Case-insensitive.
 * @returns Object with `railFound` indicating whether the rail section was present, and `watchPath` containing the relative watch URL if the channel was found.
 */
async function scrapeHboChannelRail(page: Page, channelName: string): Promise<HboRailResult> {

  const HBO_RAIL_SELECTOR = "section[data-testid=\"hbo-page-rail-distribution-channels-us_rail\"]";

  // Wait for the distribution channels rail section to appear. If it doesn't appear, the tab URL may be stale or the page structure changed.
  try {

    await page.waitForSelector(HBO_RAIL_SELECTOR, { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return { railFound: false, watchPath: null };
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

    return { railFound: false, watchPath: null };
  }

  // Scrape the rail for the target channel's watch URL. Each tile in the rail contains an anchor with the watch URL and a backup text paragraph with the channel name.
  const watchPath = await evaluateWithAbort(page, (selector: string, target: string): Nullable<string> => {

    const rail = document.querySelector(selector);

    if(!rail) {

      return null;
    }

    const targetLower = target.toLowerCase();
    const anchors = rail.querySelectorAll("a");

    for(const anchor of Array.from(anchors)) {

      // The channel name appears in a <p aria-hidden="true"> element within the tile. This is the backup text that displays the channel name when the tile
      // image fails to load.
      const nameEl = anchor.querySelector("p[aria-hidden=\"true\"]");

      if(!nameEl) {

        continue;
      }

      const name = nameEl.textContent.trim().toLowerCase();

      if(name !== targetLower) {

        continue;
      }

      const href = anchor.getAttribute("href");

      // Validate that the href points to a live channel watch page. Watch URLs follow the pattern /channel/watch/{channelUUID}/{programUUID}.
      if(href?.includes("/channel/watch/")) {

        return href;
      }
    }

    return null;
  }, [ HBO_RAIL_SELECTOR, channelName ]);

  return { railFound: true, watchPath };
}

/**
 * HBO grid strategy: discovers the HBO channels tab URL from the homepage menu bar, navigates to the tab page, scrapes the live channel rail for the target channel's
 * watch URL, and navigates to it. Caches the tab URL across tunes and falls back to rediscovery if the cached URL is stale (rail not found at the cached location).
 *
 * The strategy handles three navigations per tune:
 * 1. Homepage (already loaded by navigateToPage) → scrape menu bar for tab URL (or use cache)
 * 2. Tab page → scrape channel rail for watch URL
 * 3. Watch page → video playback begins
 *
 * When the cached tab URL is stale (rail section not found), the strategy clears the cache, navigates back to the homepage, rediscovers the tab URL, and retries.
 * This fallback triggers at most once per tune attempt.
 * @param page - The Puppeteer page object, expected to be on the HBO Max homepage.
 * @param profile - The resolved site profile with a non-null channelSelector (channel name, e.g., "HBO", "HBO Hits").
 * @returns Result object with success status and optional failure reason.
 */
export async function hboGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

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

  // Phase 2: Scrape the channel rail for the target channel's watch URL.
  let railResult = await scrapeHboChannelRail(page, channelName);

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

    railResult = await scrapeHboChannelRail(page, channelName);

    if(!railResult.railFound) {

      return { reason: "HBO channel rail not found at rediscovered URL. Site structure may have changed.", success: false };
    }
  }

  if(!railResult.railFound) {

    return { reason: "HBO channel rail not found on tab page.", success: false };
  }

  if(!railResult.watchPath) {

    // Best-effort diagnostic: collect all channel names from the rail so the user can see what values are valid for channelSelector.
    try {

      const availableChannels = await evaluateWithAbort(page, (): string[] => {

        const rail = document.querySelector("section[data-testid=\"hbo-page-rail-distribution-channels-us_rail\"]");

        if(!rail) {

          return [];
        }

        return Array.from(rail.querySelectorAll("p[aria-hidden=\"true\"]"))
          .map((p) => (p.textContent || "").trim())
          .filter((n) => n.length > 0)
          .sort();
      }, []);

      if(availableChannels.length > 0) {

        logAvailableChannels({

          availableChannels,
          channelName,
          guideUrl: "https://play.hbomax.com",
          providerName: "HBO Max"
        });
      }
    } catch {

      // Diagnostic dump is best-effort.
    }

    return { reason: "Channel " + channelName + " not found in HBO channel rail.", success: false };
  }

  // Phase 3: Navigate to the watch URL to start playback.
  const watchUrl = HBO_MAX_BASE_URL + railResult.watchPath;

  LOG.debug("tuning:hbo", "Navigating to HBO Max watch URL for %s.", channelName);

  try {

    await page.goto(watchUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
  } catch(error) {

    return { reason: "Failed to navigate to HBO Max watch page: " + formatError(error) + ".", success: false };
  }

  return { success: true };
}
