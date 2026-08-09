/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hbo.ts: HBO Max channel selection strategy. Lands on the /channels hub and reads the "Everything You Love From HBO" rail for live linear channel watch URLs.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, DiscoveredChannel, Nullable, ProviderModule } from "../../types/index.ts";
import { LOG, delay, evaluateWithAbort, formatError } from "../../utils/index.ts";
import { CONFIG } from "../../config/index.ts";
import type { Page } from "puppeteer-core";
import { createProviderChannelCache } from "./cache.ts";
import { logAvailableChannels } from "./shared.ts";

// Base URL for HBO Max watch page navigation. Used to build full watch URLs by concatenating with the relative /channel/watch/<uuid>/<uuid> path read from the
// rail. Kept as a separate constant from HBO_CHANNELS_URL so the two roles (path-concatenation base vs. landing-URL constant) stay distinct.
const HBO_MAX_BASE_URL = "https://play.hbomax.com";

// The /channels hub URL where HBO Max surfaces all live linear channels. PrismCast lands here directly via provider.guideUrl; this is the single landing URL for
// both tuning and discovery.
const HBO_CHANNELS_URL = "https://play.hbomax.com/channels";

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
const hboCache = createProviderChannelCache<HboChannelEntry>((entry) => entry.discovered);

/**
 * Returns a cached HBO Max watch URL for the given channel selector, or null if no cached URL exists.
 * @param channelSelector - The channel selector string (e.g., "HBO", "HBO Hits").
 * @returns The cached watch URL or null.
 */
function resolveHboDirectUrl(channelSelector: string): Nullable<string> {

  const entry = hboCache.map.get(channelSelector.toLowerCase());

  if(entry) {

    LOG.debug("tuning:hbo", "HBO cache hit for %s: %s.", channelSelector, entry.watchUrl);

    return entry.watchUrl;
  }

  return null;
}

/**
 * Clears the unified HBO channel cache. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts, since cached watch URLs may be
 * stale in a new browser session.
 */
function clearHboCache(): void {

  hboCache.clear();
}

/**
 * Populates the unified channel cache from raw channel rail data. For each channel, builds a DiscoveredChannel and pairs it with the full watch URL. A non-empty
 * read replaces the cached lineup rather than merging into it, so the cache mirrors the rail the read saw instead of accumulating every rail ever read.
 * Shared by hboGridStrategy (tuning-time population) and discoverHboChannels (discovery endpoint).
 * @param rawChannels - Array of channel names and watch paths from readHboChannelRail().
 */
function populateHboChannelCache(rawChannels: { name: string; watchPath: string }[]): void {

  // An empty read is a no-op rather than a wipe: the only way to reach here with zero channels is a found rail whose extraction matched nothing, and discarding a
  // working cache on that remote failure would break every subsequent tune the stale-but-live entries still serve.
  if(rawChannels.length === 0) {

    return;
  }

  // Replace rather than merge: the cache is a picture of the current rail, and carrying entries the rail no longer lists would keep serving dead watch URLs for
  // removed or renamed channels until the browser restarts.
  hboCache.clear();

  for(const ch of rawChannels) {

    const watchUrl = HBO_MAX_BASE_URL + ch.watchPath;

    hboCache.map.set(ch.name.toLowerCase(), { discovered: { channelSelector: ch.name, name: ch.name }, watchUrl });

    LOG.debug("tuning:hbo", "Cached HBO Max watch URL for %s: %s.", ch.name, watchUrl);
  }

  // The rail read this pass was built from lists every live linear channel, so what sits in the cache at this point is the complete lineup and warm reads can
  // be served from it.
  hboCache.markComplete();
}

// Result of reading the HBO channel rail. Distinguishes between the rail not being found (page structure changed, navigation went sideways) and the rail being
// found with its discovered channels.
interface HboRailResult {

  channels: { name: string; watchPath: string }[];
  railFound: boolean;
}

/**
 * Reads all channels from the "Everything You Love From HBO" rail on the /channels hub. The rail section contains tiles for each live linear channel, each with
 * a `<p aria-hidden="true">` element containing the channel name and an `<a>` whose href points to the watch page. Returns all discovered channels so the caller
 * can populate the cache in bulk.
 * @param page - The Puppeteer page object, expected to be on https://play.hbomax.com/channels.
 * @returns Object with `railFound` indicating whether the rail section was present, and `channels` containing all discovered channel names and watch paths.
 */
async function readHboChannelRail(page: Page): Promise<HboRailResult> {

  const HBO_RAIL_SELECTOR = "section[data-testid=\"channels-hub-page-everything-you-love-hbo-rail-us_rail\"]";

  // Wait for the HBO channel rail section to appear. If it doesn't appear, the page structure may have changed or the user's HBO Max subscription may not be active.
  try {

    await page.waitForSelector(HBO_RAIL_SELECTOR, { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return { channels: [], railFound: false };
  }

  // The rail uses lazy loading via IntersectionObserver - tile content only populates when the rail is visible in the viewport. The rail section element appears
  // immediately with skeleton PhantomTile placeholders, but the actual channel tiles (with names and watch URLs) are fetched asynchronously after the rail scrolls
  // into view. We scroll the rail into view and then wait for anchor elements to appear, indicating the tiles have loaded.
  await page.evaluate((selector: string): void => {

    document.querySelector(selector)?.scrollIntoView({ behavior: "instant", block: "center" });
  }, HBO_RAIL_SELECTOR);

  try {

    // This second wait only needs to catch the lazy-loaded tile content populating after the scroll above; the rail section itself already exists, so a
    // short, fixed five-second budget is sufficient here, unlike CONFIG.streaming.videoTimeout used above for the section's initial appearance.
    await page.waitForSelector(HBO_RAIL_SELECTOR + " a", { timeout: 5000 });
  } catch {

    return { channels: [], railFound: false };
  }

  /* Wait for the anchor count to settle before extracting. The wait above resolves the instant the FIRST anchor appears, while the IntersectionObserver-driven
   * lazy load is still filling in tiles, so an extraction taken at that moment can be non-empty yet partial - and the cache population downstream trusts this
   * read to BE the current rail, so a partial read would shrink the cache on ordinary tuning traffic. Two consecutive equal, nonzero counts mean the tiles have
   * stopped arriving. The count reads through evaluateWithAbort, the same abort-aware primitive the extraction below uses, so a stream termination mid-wait
   * short-circuits the loop rather than riding Puppeteer's default protocol timeout. At the iteration cap we extract whatever is present, which is what an
   * immediate extraction would have returned anyway, only later. The budget sits inside its outer bound: a tune races the whole playback initialization against
   * setup.ts's 45-second deadline, and this read's worst case - the section wait, the five-second anchor wait, and this loop's roughly three seconds - leaves
   * that deadline ample room.
   */
  let previousAnchorCount = 0;

  for(let attempt = 0; attempt < 10; attempt++) {

    // eslint-disable-next-line no-await-in-loop
    const anchorCount = await evaluateWithAbort(page, (selector: string): number => document.querySelectorAll(selector + " a").length, [HBO_RAIL_SELECTOR]);

    if((anchorCount > 0) && (anchorCount === previousAnchorCount)) {

      break;
    }

    previousAnchorCount = anchorCount;

    // eslint-disable-next-line no-await-in-loop
    await delay(300);
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
 * HBO grid strategy: reads the live channel rail on the /channels hub for all channel watch URLs, then navigates to the target channel's URL. All discovered
 * channels are cached so subsequent tunes resolve via resolveDirectUrl without re-reading the rail.
 *
 * The strategy handles two phases per tune:
 *   1. Channel rail (page is already on /channels, navigated by the coordinator via provider.guideUrl) -> read all watch URLs.
 *   2. Watch page -> video playback begins.
 *
 * @param page - The Puppeteer page object, expected to be on https://play.hbomax.com/channels.
 * @param profile - The resolved site profile with a non-null channelSelector (channel name, e.g., "HBO", "HBO Hits").
 * @returns Result object with success status and optional failure reason.
 */
async function hboGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelName = profile.channelSelector;

  // Phase 1: Read the channel rail. The coordinator has already navigated the page to provider.guideUrl (the /channels hub), so we read the rail in place.
  const railResult = await readHboChannelRail(page);

  if(!railResult.railFound) {

    return { reason: "HBO channel rail not found on /channels. HBO Max may have restructured the page, or the subscription may not be active.", success: false };
  }

  // Populate the unified channel cache with all discovered channels. Always repopulate rather than skipping when the cache has entries, because an entry the
  // invalidate hook dropped needs restoring with a fresh watch URL from the rail.
  populateHboChannelCache(railResult.channels);

  // Look up the target channel from the populated cache.
  const watchUrl = resolveHboDirectUrl(channelName);

  if(!watchUrl) {

    // Channel not found. Log available channels from the rail data to help users identify valid channelSelector values.
    logAvailableChannels({

      availableChannels: railResult.channels.map((ch) => ch.name).sort(),
      channelName,
      guideUrl: HBO_CHANNELS_URL,
      providerName: "HBO Max"
    });

    return { reason: "Channel " + channelName + " not found in HBO channel rail.", success: false };
  }

  // Phase 2: Navigate to the watch URL to start playback.
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
async function resolveHboDirectUrlAsync(channelSelector: string, _page: Page): Promise<Nullable<string>> {

  return resolveHboDirectUrl(channelSelector);
}

/**
 * Discovers all channels from the HBO Max channel rail. Returns cached results if the unified channel cache is populated from a prior tune or discovery call.
 * Otherwise reads the rail in place - the route handler has already navigated the page to provider.guideUrl (the /channels hub).
 * @param page - The Puppeteer page object, already on https://play.hbomax.com/channels (navigated by the route handler).
 * @returns Array of discovered channels.
 */
async function discoverHboChannels(page: Page): Promise<DiscoveredChannel[]> {

  // Return from the unified cache if a prior tune or discovery call already read the rail.
  const cached = hboCache.cached();

  if(cached) {

    return cached;
  }

  // Read the channel rail directly. The discovery route handler has already landed the page on /channels.
  const railResult = await readHboChannelRail(page);

  if(!railResult.railFound || (railResult.channels.length === 0)) {

    return [];
  }

  populateHboChannelCache(railResult.channels);

  return hboCache.discovered();
}

export const hboProvider: ProviderModule = {

  discoverChannels: discoverHboChannels,
  getCachedChannels: hboCache.cached,
  guideUrl: HBO_CHANNELS_URL,
  label: "HBO Max",

  // Profile for HBO Max live channels (play.hbomax.com/channels). The /channels hub contains an "Everything You Love From HBO" rail showing the current live linear
  // channels (e.g., HBO, HBO Hits) as tiles. The hboGrid strategy reads the rail for the watch URL matching the channelSelector name and navigates to it. Extends
  // fullscreenApi for requestFullscreen() behavior inherited by the watch page.
  profile: {

    category: "multiChannel",
    channelSelection: { strategy: "hboGrid" },
    description: "HBO Max with live channel rail selection. Set Channel Selector to the channel name (e.g., HBO, HBO Hits).",
    extends: "fullscreenApi",
    summary: "HBO Max (live channels, needs selector)"
  },
  profileName: "hboMax",
  slug: "hbomax",
  strategy: {

    clearCache: clearHboCache,
    execute: hboGridStrategy,
    invalidateDirectUrl: hboCache.invalidate,
    resolveDirectUrl: resolveHboDirectUrlAsync
  },
  strategyName: "hboGrid"
};
