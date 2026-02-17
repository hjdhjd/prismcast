/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * sling.ts: Sling TV guide grid channel selection strategy with binary search and row caching.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ChannelStrategyEntry, ClickTarget, Nullable } from "../../types/index.js";
import { LOG, delay, formatError } from "../../utils/index.js";
import { logAvailableChannels, normalizeChannelName } from "../channelSelection.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";

// Sling TV guide grid row index cache. Maps normalized channel names (from data-testid="channel-{NAME}" attributes) to their row indices extracted from the
// parent .guide-row-container CSS class (gridGuideRow-{N}). Separate from the Hulu guideRowCache because Sling uses a different row index system (CSS class-based)
// and different scroll mechanics (.guide-cell scrollTop vs document.documentElement.scrollTop).
const slingRowCache = new Map<string, number>();

// Channel GUID cache: maps normalized channel names to stable Sling channel GUIDs. Populated by intercepting guide grid API responses during guide page load.
// Channel GUIDs are permanent infrastructure identifiers that never change — only the per-program asset_id rotates.
const slingChannelGuidCache = new Map<string, string>();

// Playback info URL template captured from the grid API response. The CDN hostname (e.g., cbd46b77.cdn.cms.movetv.com) may change, so we derive it at runtime
// from the first PLAY_CONTENT tile's playback_info.url rather than hardcoding it. The template stores everything up to and including "/channels" — the caller
// appends "/{channel_guid}/schedule/now/playback_info.qvt".
let slingPlaybackInfoBase: Nullable<string> = null;

// Tracks which pages have response interception listeners registered to avoid duplicate registrations.
const pagesWithListeners = new WeakSet<Page>();

// Base URL for constructing direct player URLs. This is the user-facing Sling domain, not a CDN edge — stable.
const SLING_PLAYER_BASE = "https://watch.sling.com/1/asset";

// Polling interval for the frontier-based cache wait. 300ms balances responsiveness (detecting newly arrived API pages quickly) against CPU overhead from cache
// scans. The API delivers pages in bursts, so shorter intervals provide diminishing returns.
const FRONTIER_POLL_INTERVAL = 300;

// Maximum time to wait for the target channel's GUID to appear in the cache. The full Sling channel alphabet takes 3-5 seconds to deliver via the paginated
// grid API. Five seconds ensures even the latest-alphabet channels are captured while bounding worst-case wait time for channels that genuinely aren't in the
// lineup (where the frontier never passes the target because it IS the last channel alphabetically).
const FRONTIER_MAX_WAIT = 5000;

/**
 * Clears all Sling TV caches: row index cache, channel GUID cache, and playback info base URL. Called by clearChannelSelectionCaches() in the coordinator when
 * the browser restarts, since cached state may be stale in a new browser session.
 */
function clearSlingCache(): void {

  slingRowCache.clear();
  slingChannelGuidCache.clear();
  slingPlaybackInfoBase = null;
}

// Rendered channel entry from the Sling TV guide grid. Captures the normalized name from data-testid="channel-{NAME}" and the row index from the parent
// .guide-row-container CSS class (gridGuideRow-{N}).
interface SlingRenderedChannel {

  name: string;
  rowIndex: number;
}

// Combined result from readSlingChannelsAndLocate(). Contains all rendered channels for binary search direction and cache population, plus click coordinates for
// the target channel's on-now cell if it was found among the rendered channels. The matchedName field captures the actual channel name that matched (which may
// differ from the target when a local affiliate prefix match fires).
interface SlingReadResult {

  channels: Nullable<SlingRenderedChannel[]>;
  clickTarget: Nullable<ClickTarget>;
  matchedName: Nullable<string>;
}

/**
 * Reads all rendered channel entries from the Sling TV guide grid in a single browser evaluate call. Extracts names from data-testid="channel-{NAME}" attributes
 * and row indices from the parent .guide-row-container CSS class containing gridGuideRow-{N}. When the target channel is found, also locates the on-now program
 * cell, scrolls it into view, and returns its center coordinates — eliminating a second evaluate round-trip. Populates the slingRowCache as a side effect. For
 * local affiliates (ABC, FOX, NBC), also matches channels whose name starts with the target followed by " (" to handle the "network (callsign)" format.
 * @param page - The Puppeteer page object.
 * @param targetName - The normalized (lowercased, trimmed) channel name to match, or null to skip click target resolution.
 * @returns Object with all rendered channels, optional click coordinates for the target's on-now cell, and the actual matched name (which may differ from
 *   targetName for local affiliates).
 */
async function readSlingChannelsAndLocate(page: Page, targetName: Nullable<string>): Promise<SlingReadResult> {

  const raw = await page.evaluate((target: Nullable<string>): Nullable<{
    channels: { name: string; rowIndex: number }[];
    clickTarget: Nullable<{ x: number; y: number }>;
    matchedName: Nullable<string>;
  }> => {

    const containers = document.querySelectorAll("[data-testid^=\"channel-\"]");

    if(containers.length === 0) {

      return null;
    }

    const prefix = "channel-";
    const channels: { name: string; rowIndex: number }[] = [];
    let clickTarget: Nullable<{ x: number; y: number }> = null;
    let matchedName: Nullable<string> = null;

    for(const el of Array.from(containers)) {

      const testid = el.getAttribute("data-testid") ?? "";
      const name = testid.slice(prefix.length).trim().replace(/\s+/g, " ").toLowerCase();

      // Extract row index from the parent .guide-row-container CSS class. The class name follows the pattern "gridGuideRow-{N}".
      let rowIndex = -1;
      const rowContainer = el.closest(".guide-row-container");

      if(rowContainer) {

        const classMatch = /gridGuideRow-(\d+)/.exec(rowContainer.className);

        if(classMatch) {

          rowIndex = parseInt(classMatch[1], 10);
        }
      }

      channels.push({ name, rowIndex });

      // When the target is found, locate the on-now program cell in the same pass. This avoids a second querySelectorAll + normalize loop in a separate evaluate.
      // Sling local affiliates use the format "network (callsign)" so we also check for a prefix match where the channel name starts with the target followed by
      // " (". This handles market-specific call signs without hardcoding them.
      if(target && !clickTarget && ((name === target) || name.startsWith(target + " ("))) {

        matchedName = name;

        if(rowContainer) {

          const onNow = rowContainer.querySelector(".grid-program-cell-container.active") as Nullable<HTMLElement>;

          if(onNow) {

            onNow.scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

            const rect = onNow.getBoundingClientRect();

            if((rect.width > 0) && (rect.height > 0)) {

              clickTarget = { x: rect.x + (rect.width / 2), y: rect.y + (rect.height / 2) };
            }
          }
        }
      }
    }

    return { channels, clickTarget, matchedName };
  }, targetName);

  if(!raw) {

    return { channels: null, clickTarget: null, matchedName: null };
  }

  // Populate the row index cache with discovered mappings.
  const rendered: SlingRenderedChannel[] = [];

  for(const ch of raw.channels) {

    rendered.push({ name: ch.name, rowIndex: ch.rowIndex });

    if(ch.rowIndex >= 0) {

      slingRowCache.set(ch.name, ch.rowIndex);
    }
  }

  return { channels: rendered, clickTarget: raw.clickTarget, matchedName: raw.matchedName };
}

// Shorter timeout for non-final click attempts. Successful navigations typically complete in 1-2 seconds, so 5 seconds is generous while still failing fast enough
// to allow meaningful retries within the overall time budget.
const CLICK_RETRY_TIMEOUT = 5000;

// The guide page URL contains this path segment. Used to detect whether the page has navigated away from the guide after a click attempt.
const GUIDE_URL_MARKER = "grid_guide";

// Maximum number of click attempts before giving up. Three attempts with 5-second timeouts on the first two gives a worst-case wall-clock time of ~15 seconds —
// comparable to a single 10-second timeout plus the overhead of the full retry cycle, but with a much higher success rate for transient failures.
const MAX_CLICK_ATTEMPTS = 3;

/**
 * Clicks the on-now program cell and waits for Sling to navigate to the player page. The click triggers a full page navigation to a /1/asset/{assetId}/watch URL,
 * so we use Promise.all with page.waitForNavigation() to ensure the player page's DOM is ready before returning. Without this wait, initializePlayback() could run
 * against a page that is mid-transition — either finding nothing or grabbing a stale element from the guide page. Uses domcontentloaded rather than load because
 * the player page only needs to render a <video> element — waiting for all subresources (images, fonts) would add unnecessary latency since startVideoPlayback()
 * independently waits for the video element. No settle delay before the click because readSlingChannelsAndLocate() already called scrollIntoView and read
 * getBoundingClientRect, confirming the element is positioned, and any mispositioned click is caught by the navigation timeout.
 * @param page - The Puppeteer page object.
 * @param target - The x/y coordinates of the on-now cell to click.
 * @param timeout - Navigation timeout in milliseconds. Defaults to CONFIG.streaming.navigationTimeout.
 * @returns Result object with success status and optional failure reason.
 */
async function clickSlingOnNowAndWaitForNavigation(
  page: Page, target: ClickTarget, timeout = CONFIG.streaming.navigationTimeout
): Promise<ChannelSelectorResult> {

  try {

    // Register the navigation wait before the click fires to avoid a race where the navigation completes before waitForNavigation starts listening.
    await Promise.all([
      page.waitForNavigation({ timeout, waitUntil: "domcontentloaded" }),
      page.mouse.click(target.x, target.y)
    ]);

    return { success: true };
  } catch(error) {

    return { reason: "Navigation to Sling TV player page failed: " + formatError(error) + ".", success: false };
  }
}

/**
 * Attempts to click the on-now program cell up to MAX_CLICK_ATTEMPTS times with in-place retry. On each non-final attempt, uses a shorter timeout (CLICK_RETRY_TIMEOUT)
 * to fail fast and allow a retry within the same time budget as a single full-timeout attempt plus the expensive full-retry cycle. Between retries, checks whether the
 * page has already navigated away from the guide (indicating the click triggered navigation but the player page was slow to load) and re-reads on-now cell coordinates
 * from the guide page to handle any virtualizer layout shifts that occurred during the timeout.
 * @param page - The Puppeteer page object.
 * @param initialTarget - The x/y coordinates from the initial readSlingChannelsAndLocate() call.
 * @param normalizedName - The normalized channel name for re-reading coordinates on retry.
 * @param channelName - The original channel name for log messages.
 * @returns Result object with success status and optional failure reason from the last attempt.
 */
async function clickWithRetry(
  page: Page, initialTarget: ClickTarget, normalizedName: string, channelName: string
): Promise<ChannelSelectorResult> {

  let target = initialTarget;
  let lastResult: ChannelSelectorResult = { reason: "No click attempts made.", success: false };

  for(let attempt = 0; attempt < MAX_CLICK_ATTEMPTS; attempt++) {

    // On retry attempts, re-read on-now cell coordinates. The virtualizer may have shifted layout during the timeout, making the original coordinates stale.
    if(attempt > 0) {

      // If the page has navigated away from the guide, the click did trigger navigation — it was just slow. Return success and let initializePlayback's
      // waitForVideoReady handle the rest rather than re-clicking a page that is already mid-navigation.
      if(!page.url().includes(GUIDE_URL_MARKER)) {

        LOG.debug("tuning:sling", "Sling page navigated away from guide after click attempt %s for %s. Treating as success.", attempt, channelName);

        return { success: true };
      }

      LOG.debug("tuning:sling", "Sling click attempt %s of %s for %s.", attempt + 1, MAX_CLICK_ATTEMPTS, channelName);

      // Re-read coordinates from the still-loaded guide page. Wrapped in try/catch because the page might commit a pending navigation between the URL check above
      // and this evaluate call, destroying the execution context. In that case, fall through with the previous coordinates — the next URL check will detect the
      // navigation and return success.
      try {

        // eslint-disable-next-line no-await-in-loop
        const retryResult = await readSlingChannelsAndLocate(page, normalizedName);

        if(retryResult.clickTarget) {

          target = retryResult.clickTarget;
        }
      } catch {

        // Page navigated away during re-read. The next iteration's URL check will catch this.
      }
    }

    // Use a shorter timeout on non-final attempts to fail fast. The final attempt gets the full navigationTimeout as a last-ditch effort.
    const timeout = (attempt < (MAX_CLICK_ATTEMPTS - 1)) ? CLICK_RETRY_TIMEOUT : CONFIG.streaming.navigationTimeout;

    // eslint-disable-next-line no-await-in-loop
    lastResult = await clickSlingOnNowAndWaitForNavigation(page, target, timeout);

    if(lastResult.success) {

      return lastResult;
    }
  }

  return lastResult;
}

/**
 * Looks up a channel GUID from the cache by normalized name. Falls back to local affiliate prefix matching where the cache key starts with the target name
 * followed by " (" (e.g., "abc" matches "abc (wabc)"). On a prefix match, caches the GUID under the primary name for O(1) on subsequent lookups.
 * @param normalizedName - The normalized (lowercased, trimmed) channel name.
 * @returns The channel GUID or null if not cached.
 */
function findChannelGuid(normalizedName: string): Nullable<string> {

  let channelGuid = slingChannelGuidCache.get(normalizedName) ?? null;

  if(!channelGuid) {

    for(const [ key, guid ] of slingChannelGuidCache) {

      if(key.startsWith(normalizedName + " (")) {

        channelGuid = guid;

        // Cache under the primary name for O(1) on next lookup.
        slingChannelGuidCache.set(normalizedName, guid);

        break;
      }
    }
  }

  return channelGuid;
}

/**
 * Returns the alphabetically latest key in the GUID cache. Used by the polling loop to detect when the API has delivered pages past the target's position —
 * if the frontier sorts after the target name, the API page covering the target's range has already been processed and the channel is not in the lineup.
 * @returns The alphabetically latest cache key, or null if the cache is empty.
 */
function getCacheFrontier(): Nullable<string> {

  let max: Nullable<string> = null;

  for(const key of slingChannelGuidCache.keys()) {

    if(!max || (key > max)) {

      max = key;
    }
  }

  return max;
}

/**
 * Fetches the current asset_id for a Sling channel from the public playback info endpoint. Returns null if the playback info base URL has not been captured yet
 * (cold cache), the endpoint returns a non-200 response, or the response does not contain an asset GUID. Callers should catch network and JSON parse errors.
 * @param channelGuid - The stable Sling channel GUID.
 * @returns The current asset_id string or null.
 */
async function fetchSlingAssetId(channelGuid: string): Promise<Nullable<string>> {

  if(!slingPlaybackInfoBase) {

    return null;
  }

  const response = await fetch(slingPlaybackInfoBase + "/" + channelGuid + "/schedule/now/playback_info.qvt");

  if(!response.ok) {

    return null;
  }

  const data = await response.json() as { playback_info?: { asset?: { guid?: string } } };

  return data.playback_info?.asset?.guid ?? null;
}

/**
 * Resolves a direct player URL from the channel GUID cache. Looks up the channel GUID by normalized name (with local affiliate prefix fallback), fetches the
 * current asset_id from the public playback info endpoint, and returns the full player URL. Returns null if the channel is not in the GUID cache, the playback
 * info base URL is unknown, or the asset_id fetch fails. Shared by both resolveSlingDirectUrl (pre-navigation) and the slingGridStrategy fast path
 * (post-guide-load).
 * @param normalizedName - The normalized (lowercased, trimmed) channel name.
 * @returns The direct player URL (e.g., "https://watch.sling.com/1/asset/{id}/watch") or null.
 */
async function resolvePlayerUrl(normalizedName: string): Promise<Nullable<string>> {

  const channelGuid = findChannelGuid(normalizedName);

  if(!channelGuid) {

    return null;
  }

  const assetId = await fetchSlingAssetId(channelGuid);

  if(!assetId) {

    return null;
  }

  return SLING_PLAYER_BASE + "/" + assetId + "/watch";
}

// Partial type for Sling's grid guide API response. Only the fields we need are typed — the response contains many more fields per ribbon and tile.
interface SlingGridApiRibbon {

  stitch_id?: string;
  tiles?: {
    actions?: {
      DETAIL_VIEW?: { adobe?: { ChannelName?: string } };
      PLAY_CONTENT?: { playback_info?: { url?: string } };
    };
  }[];
}

interface SlingGridApiResponse {

  ribbons?: SlingGridApiRibbon[];
}

/**
 * Processes a single Sling grid guide API response, extracting channel GUIDs and the playback info base URL. Called by the response interception listener for
 * each paginated grid_guide_a_z response. Populates the slingChannelGuidCache and, on the first tile with a PLAY_CONTENT action, captures slingPlaybackInfoBase.
 * @param data - The parsed JSON response from the grid guide API.
 */
function processGridApiResponse(data: SlingGridApiResponse): void {

  if(!data.ribbons) {

    return;
  }

  for(const ribbon of data.ribbons) {

    const tile = ribbon.tiles?.[0];
    const channelName = tile?.actions?.DETAIL_VIEW?.adobe?.ChannelName;
    const channelGuid = ribbon.stitch_id;
    const isLive = !!tile?.actions?.PLAY_CONTENT;

    // Only cache ribbons that have a PLAY_CONTENT action. AVOD/VOD variant channels (e.g., "truTV Sneak Peak", Freestream browse entries) share the same
    // ChannelName as the live channel but have a different GUID that doesn't resolve via playback_info. Live channels always have PLAY_CONTENT; variants
    // only have DETAIL_VIEW.
    if(channelName && channelGuid && isLive) {

      slingChannelGuidCache.set(normalizeChannelName(channelName), channelGuid);
    }

    // Extract the playback info base URL from the first tile that has one. The CDN hostname varies (e.g., cbd46b77.cdn.cms.movetv.com), so we derive it
    // from the actual API response rather than hardcoding. We take everything up to and including "/channels" from the tile's playback_info.url.
    if(!slingPlaybackInfoBase) {

      const playbackUrl = tile?.actions?.PLAY_CONTENT?.playback_info?.url;

      if((typeof playbackUrl === "string") && playbackUrl.includes("/channels/")) {

        slingPlaybackInfoBase = playbackUrl.substring(0, playbackUrl.indexOf("/channels/") + "/channels".length);
      }
    }
  }

}

/**
 * Sets up response interception on the page to capture Sling's grid guide API responses. As the guide page loads, Sling fetches paginated channel data from the
 * grid_guide_a_z API. Each response contains ribbons with channel GUIDs (stitch_id) and channel names (DETAIL_VIEW.adobe.ChannelName). We intercept these to
 * populate the slingChannelGuidCache, enabling the API fast path on subsequent tunes. Also extracts the playback info base URL from the first PLAY_CONTENT tile
 * to avoid hardcoding the CDN hostname. Uses a WeakSet to prevent duplicate listener registration on the same page.
 * @param page - The Puppeteer page object.
 */
function setupGridResponseInterception(page: Page): void {

  if(pagesWithListeners.has(page)) {

    return;
  }

  pagesWithListeners.add(page);

  page.on("response", (response) => {

    const url = response.url();

    if(!url.includes("pres/grid_guide_a_z") || (response.status() !== 200)) {

      return;
    }

    void response.json().then((data: SlingGridApiResponse) => {

      processGridApiResponse(data);
    }).catch(() => {

      // Response parsing failed — binary search fallback handles it.
    });
  });
}

/**
 * Resolves a direct player URL for a Sling channel. When the channel GUID cache is warm (populated by grid API response interception), fetches the current
 * asset_id from the public playback_info endpoint and returns a direct player URL. When the cache is cold, sets up response interception on the page so that
 * the next guide page navigation will populate the cache for future tunes.
 * @param channelSelector - The channel selector string (e.g., "ESPN", "ABC").
 * @param page - The Puppeteer page for setting up response interception on cold cache.
 * @returns The direct player URL or null.
 */
async function resolveSlingDirectUrl(channelSelector: string, page: Page): Promise<Nullable<string>> {

  const normalizedName = normalizeChannelName(channelSelector);

  // Warm cache: resolve the player URL from the channel GUID cache and the public playback info endpoint.
  try {

    const playerUrl = await resolvePlayerUrl(normalizedName);

    if(playerUrl) {

      LOG.debug("tuning:sling", "Sling direct URL resolved for %s: %s.", channelSelector, playerUrl);

      return playerUrl;
    }
  } catch(error) {

    LOG.debug("tuning:sling", "Sling playback info fetch failed for %s: %s.", channelSelector, formatError(error));

    return null;
  }

  // Cold cache: set up response interception for the upcoming guide page navigation. The listener will populate slingChannelGuidCache as the guide loads,
  // making the channel_guid available for the execute fast path and subsequent resolveDirectUrl calls.
  setupGridResponseInterception(page);

  return null;
}

/**
 * Invalidates the cached channel GUID for the given channel selector. Called when a cached direct URL fails to produce a working stream.
 * @param channelSelector - The channel selector string to invalidate.
 */
function invalidateSlingDirectUrl(channelSelector: string): void {

  slingChannelGuidCache.delete(normalizeChannelName(channelSelector));
}

/**
 * Sling TV grid strategy: finds a channel in the virtualized, alphabetically sorted guide grid at watch.sling.com/dashboard/grid_guide/grid_guide_a_z using binary
 * search on the .guide-cell scroll container, then clicks the on-now program cell to navigate to the player page. The guide renders ~8-10 of ~638 rows at a time,
 * each 120px tall, sorted A-Z by channel name. Channel identification uses data-testid="channel-{NAME}" attributes.
 *
 * The selection process:
 * 1. Wait for channel entries to appear in the DOM (confirms guide grid has loaded and API data is flowing)
 * 2. API fast path with frontier polling: poll the GUID cache until the target channel appears, the cache frontier passes the target's position, or 5 seconds
 *    elapse — then fetch the current asset_id and navigate directly, skipping all steps below
 * 3. Read grid metadata: locate the .guide-cell scroll host and compute total rows from scrollHeight / 120
 * 4. Check the slingRowCache for a direct-scroll shortcut from a previous tune
 * 5. Binary search: scroll .guide-cell to the midpoint row, read rendered channels, compare alphabetically to adjust bounds
 * 6. Click the on-now program cell via clickWithRetry() — retries up to 3 times on navigation timeout before giving up
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector (channel name).
 * @returns Result object with success status and optional failure reason.
 */
async function slingGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelName = profile.channelSelector;
  const normalizedName = normalizeChannelName(channelName);

  // Phase 1: Wait for the guide grid to render. Channel entries appear as data-testid="channel-{NAME}" elements within the virtualized list.
  try {

    await page.waitForSelector("[data-testid^=\"channel-\"]", { timeout: 5000, visible: true });
  } catch {

    return { reason: "Sling TV guide grid did not load.", success: false };
  }

  // API fast path with frontier-based polling. The GUID cache populates progressively as paginated grid API responses arrive in alphabetical order during page
  // load. Rather than checking the cache once (missing late-alphabet channels whose API page hasn't arrived yet), poll until one of three conditions is met:
  // (1) the target channel's GUID appears in the cache, (2) the cache frontier (alphabetically latest entry) passes the target's position (confirming the
  // channel is not in the lineup), or (3) the maximum wait time is exceeded. This ensures the cache is fully populated for all subsequent tunes — a one-time
  // cost that eliminates binary search for the rest of the session.
  const pollStart = Date.now();

  while((Date.now() - pollStart) < FRONTIER_MAX_WAIT) {

    if(findChannelGuid(normalizedName)) {

      break;
    }

    // Check if the cache frontier has passed the target's alphabetical position. If the latest cache key sorts after the target, the API page covering
    // the target's range has already been processed — the channel is not in the lineup and further polling won't help.
    const frontier = getCacheFrontier();

    if(frontier && (frontier > normalizedName)) {

      LOG.debug("tuning:sling", "Cache frontier \"%s\" passed target \"%s\". Channel not in Sling lineup.", frontier, normalizedName);

      break;
    }

    // eslint-disable-next-line no-await-in-loop
    await delay(FRONTIER_POLL_INTERVAL);
  }

  // After polling, attempt the full resolve pipeline: GUID lookup → asset_id fetch → player URL construction.
  const playerUrl = await resolvePlayerUrl(normalizedName).catch(() => null);

  if(playerUrl) {

    LOG.debug("tuning:sling", "Sling API fast path for %s (%sms): %s.", channelName, Date.now() - pollStart, playerUrl);

    try {

      await page.goto(playerUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "domcontentloaded" });

      return { success: true };
    } catch(error) {

      return { reason: "Sling TV API fast path navigation failed: " + formatError(error) + ".", success: false };
    }
  }

  LOG.debug("tuning:sling", "Sling binary search fallback for %s after %sms polling (GUID cache: %s, base URL: %s).",
    channelName, Date.now() - pollStart, slingChannelGuidCache.size, slingPlaybackInfoBase ? "yes" : "no");

  // Phase 2: Read grid metadata. The .guide-cell element is the scroll host for the virtualized channel list. Each row is 120px tall with a 30px offset for the
  // time header row at the top of the grid.
  const ROW_HEIGHT = 120;
  const ROW_OFFSET = 30;

  const gridMeta = await page.evaluate((rowHeight: number): Nullable<{ totalRows: number }> => {

    const guideCell = document.querySelector(".guide-cell");

    if(!guideCell) {

      return null;
    }

    const totalRows = Math.round(guideCell.scrollHeight / rowHeight);

    if(totalRows <= 0) {

      return null;
    }

    return { totalRows };
  }, ROW_HEIGHT);

  if(!gridMeta) {

    return { reason: "Could not locate Sling TV guide grid scroll container.", success: false };
  }

  const { totalRows } = gridMeta;

  // Helper: scroll the .guide-cell container to a specific row index and wait for the virtualizer to render.
  const scrollToRow = async (rowIndex: number): Promise<void> => {

    await page.evaluate((scrollTo: number): void => {

      const guideCell = document.querySelector(".guide-cell");

      if(guideCell) {

        guideCell.scrollTop = scrollTo;
      }
    }, ROW_OFFSET + (rowIndex * ROW_HEIGHT));

    await delay(200);
  };

  // Phase 3: Check the cache for a direct-scroll shortcut. If we've tuned to this channel before, skip binary search and scroll directly.
  const cachedRow = slingRowCache.get(normalizedName);

  if(cachedRow !== undefined) {

    LOG.debug("tuning:sling", "Sling row cache hit for %s at row %s.", channelName, cachedRow);

    await scrollToRow(cachedRow);

    const { channels, clickTarget } = await readSlingChannelsAndLocate(page, normalizedName);

    if(channels && clickTarget) {

      return await clickWithRetry(page, clickTarget, normalizedName, channelName);
    }

    // Cache hit but channel not found at expected position. Clear this entry and fall through to binary search.
    LOG.debug("tuning:sling", "Sling cache miss for %s. Falling back to binary search.", channelName);

    slingRowCache.delete(normalizedName);
  }

  // Phase 4: Binary search through the virtualized channel list. On each iteration we scroll to the midpoint, read rendered channels (with click target resolution
  // for the target name), and either click immediately on match or compare alphabetically to adjust bounds. The combined readSlingChannelsAndLocate call returns
  // both the channel list (for direction) and the on-now cell coordinates (for clicking) in a single browser round-trip.
  let low = 0;
  let high = totalRows - 1;
  const maxIterations = 12;
  let foundClickTarget: Nullable<ClickTarget> = null;
  let foundMatchedName: Nullable<string> = null;

  for(let iteration = 0; iteration < maxIterations; iteration++) {

    if(low > high) {

      break;
    }

    const mid = Math.floor((low + high) / 2);

    // eslint-disable-next-line no-await-in-loop
    await scrollToRow(mid);

    // eslint-disable-next-line no-await-in-loop
    const { channels, clickTarget, matchedName } = await readSlingChannelsAndLocate(page, normalizedName);

    if(!channels || (channels.length === 0)) {

      continue;
    }

    // If the target was found, the click coordinates are already resolved. No second evaluate needed.
    if(clickTarget) {

      foundClickTarget = clickTarget;
      foundMatchedName = matchedName;

      break;
    }

    // Sort by name so first/last reflect alphabetical extremes. querySelectorAll returns DOM insertion order, which may not match visual order in a virtualizer
    // that recycles elements by appending new rows rather than inserting in visual position.
    channels.sort((a, b) => a.name.localeCompare(b.name));

    // Determine binary search direction by comparing against the first and last rendered channel names.
    const first = channels[0].name;
    const last = channels[channels.length - 1].name;

    if(normalizedName.localeCompare(first) < 0) {

      // Target sorts before the first rendered channel. Scroll up.
      high = mid - 1;

      continue;
    }

    if(normalizedName.localeCompare(last) > 0) {

      // Target sorts after the last rendered channel. Scroll down.
      low = mid + 1;

      continue;
    }

    // Target is between the first and last rendered channels but no exact match. The channel may not exist in the guide.
    break;
  }

  if(!foundClickTarget) {

    // Log available channels from the row cache to help users identify the correct channelSelector value. The cache contains channels seen during binary search
    // iterations and any prior tune attempts in this session — a partial but often actionable subset of Sling's ~636 channel catalog.
    const availableChannels = Array.from(slingRowCache.keys()).sort();

    if(availableChannels.length > 0) {

      logAvailableChannels({

        availableChannels,
        channelName,
        guideUrl: "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z",
        providerName: "Sling TV"
      });
    }

    return { reason: "Could not find channel " + channelName + " in Sling TV guide grid.", success: false };
  }

  // When a local affiliate was matched via prefix, cache the network name as an alias so subsequent tunes skip binary search and scroll directly to the
  // affiliate's row.
  if(foundMatchedName && (foundMatchedName !== normalizedName)) {

    const affiliateRow = slingRowCache.get(foundMatchedName);

    if(affiliateRow !== undefined) {

      slingRowCache.set(normalizedName, affiliateRow);
    }
  }

  // Phase 5: Click the on-now program cell and wait for Sling to navigate to the player page. Uses the retry loop to handle transient click or navigation failures
  // without tearing down the entire attempt and reloading the guide page.
  return await clickWithRetry(page, foundClickTarget, normalizedName, channelName);
}

export const slingStrategy: ChannelStrategyEntry = {

  clearCache: clearSlingCache,
  execute: slingGridStrategy,
  invalidateDirectUrl: invalidateSlingDirectUrl,
  resolveDirectUrl: resolveSlingDirectUrl
};
