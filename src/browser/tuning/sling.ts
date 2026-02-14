/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * sling.ts: Sling TV guide grid channel selection strategy with binary search and row caching.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ClickTarget, Nullable } from "../../types/index.js";
import { LOG, delay, formatError } from "../../utils/index.js";
import { logAvailableChannels, normalizeChannelName } from "../channelSelection.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";

// Sling TV guide grid row index cache. Maps normalized channel names (from data-testid="channel-{NAME}" attributes) to their row indices extracted from the
// parent .guide-row-container CSS class (gridGuideRow-{N}). Separate from the Hulu guideRowCache because Sling uses a different row index system (CSS class-based)
// and different scroll mechanics (.guide-cell scrollTop vs document.documentElement.scrollTop).
const slingRowCache = new Map<string, number>();

/**
 * Clears the Sling TV row cache. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts.
 */
export function clearSlingCache(): void {

  slingRowCache.clear();
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
 * Sling TV grid strategy: finds a channel in the virtualized, alphabetically sorted guide grid at watch.sling.com/dashboard/grid_guide/grid_guide_a_z using binary
 * search on the .guide-cell scroll container, then clicks the on-now program cell to navigate to the player page. The guide renders ~8-10 of ~638 rows at a time,
 * each 120px tall, sorted A-Z by channel name. Channel identification uses data-testid="channel-{NAME}" attributes.
 *
 * The selection process:
 * 1. Wait for channel entries to appear in the DOM (confirms guide grid has loaded)
 * 2. Read grid metadata: locate the .guide-cell scroll host and compute total rows from scrollHeight / 120
 * 3. Check the slingRowCache for a direct-scroll shortcut from a previous tune
 * 4. Binary search: scroll .guide-cell to the midpoint row, read rendered channels, compare alphabetically to adjust bounds
 * 5. Click the on-now program cell via clickWithRetry() — retries up to 3 times on navigation timeout before giving up
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector (channel name).
 * @returns Result object with success status and optional failure reason.
 */
export async function slingGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelName = profile.channelSelector;

  // Phase 1: Wait for the guide grid to render. Channel entries appear as data-testid="channel-{NAME}" elements within the virtualized list.
  try {

    await page.waitForSelector("[data-testid^=\"channel-\"]", { timeout: 5000, visible: true });
  } catch {

    return { reason: "Sling TV guide grid did not load.", success: false };
  }

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
  const normalizedName = normalizeChannelName(channelName);

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

    LOG.debug("tuning:sling", "Sling cache hit for %s at row %s.", channelName, cachedRow);

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
