/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hulu.ts: Hulu Live TV channel selection with fetch interception for direct tuning, guide grid fallback with binary search, position-based inference, and row caching.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ChannelStrategyEntry, ClickTarget, Nullable } from "../../types/index.js";
import { LOG, delay, evaluateWithAbort, formatError } from "../../utils/index.js";
import { logAvailableChannels, normalizeChannelName, scrollAndClick } from "../channelSelection.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";

// Guide grid row number cache. Maps lowercased, trimmed channel names from data-testid attributes to their row numbers (from sr-only text). Populated passively
// during binary search iterations and used for direct-scroll optimization on subsequent tunes. Session-scoped — cleared when the browser restarts.
const guideRowCache = new Map<string, number>();

// Hulu channel UUID cache. Maps normalized channel names (from guide.hulu.com/guide/details API responses) to channel UUIDs used in the playlist API's channel_id
// field. Populated server-side by intercepting details API responses during page load. Session-scoped — cleared when the browser restarts via clearHuluCache().
const huluUuidCache = new Map<string, string>();

// Hulu channel EAB cache. Maps channel UUIDs to their program schedules (from guide.hulu.com/guide/listing API responses). Each entry is an array of programs with
// EAB IDs and airing times. Used to supply the correct content_eab_id when swapping channel_id in the playlist API request — the server requires a valid EAB for
// the target channel. Session-scoped — cleared when the browser restarts via clearHuluCache().
const huluEabCache = new Map<string, HuluListingProgram[]>();

// Tracks pages with details API response listeners to avoid duplicate registration. Mirrors the pagesWithListeners pattern in sling.ts.
const huluPagesWithListeners = new WeakSet<Page>();

// The Hulu live page URL. The evaluateOnNewDocument fetch interceptor swaps channel_id and content_eab_id in the playlist API request, making the app play the
// target channel through its own pipeline without guide grid interaction. On warm cache, UUID and EAB are injected at install time. On cold cache (non-affiliate),
// the interceptor captures them from listing and details API responses during page load, holding the playlist request until both are resolved.
const HULU_LIVE_URL = "https://www.hulu.com/live";

// Partial type for Hulu's guide details API response. Each item represents a scheduled program and includes channel_info with the channel's UUID and display name.
// Multiple items may share the same channel_info (different programs on the same channel).
interface HuluDetailsItem {

  channel_info?: {

    id?: string;
    name?: string;
  };
}

interface HuluDetailsResponse {

  items?: HuluDetailsItem[];
}

// Partial type for a single program in Hulu's guide listing API response. Each program has an EAB ID and an airing window used to determine which program is
// currently live on a given channel.
interface HuluListingProgram {

  airingEnd: string;
  airingStart: string;
  eab: string;
}

// Partial type for a channel entry in Hulu's guide listing API response. Each channel has a UUID and an array of scheduled programs.
interface HuluListingChannel {

  id?: string;
  programs?: HuluListingProgram[];
}

interface HuluListingResponse {

  channels?: HuluListingChannel[];
}

/**
 * Clears all Hulu caches: guide row cache, channel UUID cache, and EAB program cache. Called by clearChannelSelectionCaches() in the coordinator when the
 * browser restarts, since cached state may be stale in a new browser session.
 */
function clearHuluCache(): void {

  guideRowCache.clear();
  huluEabCache.clear();
  huluUuidCache.clear();
}

// Rendered channel entry from the guide grid. Captures the lowercased trimmed name from data-testid and the DOM position index (order of appearance in the DOM,
// which reflects the guide's network-name sort order).
interface RenderedChannel {

  domIndex: number;
  name: string;
  rowNumber: number;
}

/**
 * Reads all rendered channel containers from the guide grid, extracting their names from data-testid attributes and row numbers from sr-only text. Populates the
 * row number cache as a side effect.
 * @param page - The Puppeteer page object.
 * @returns Array of rendered channels in DOM order, or null if no channels are rendered.
 */
async function readRenderedChannels(page: Page): Promise<Nullable<RenderedChannel[]>> {

  const channels = await page.evaluate((): Nullable<{ name: string; rowNumber: number }[]> => {

    const containers = document.querySelectorAll("[data-testid^=\"live-guide-channel-kyber-\"]");

    if(containers.length === 0) {

      return null;
    }

    const prefix = "live-guide-channel-kyber-";
    const results: { name: string; rowNumber: number }[] = [];

    for(const el of Array.from(containers)) {

      const testid = el.getAttribute("data-testid") ?? "";
      const name = testid.slice(prefix.length).trim().replace(/\s+/g, " ").toLowerCase();

      // Extract row number from sr-only text. Format: "{Name} Details, row {N} of {Total}. ..."
      let rowNumber = -1;
      const btn = el.querySelector("[data-testid=\"live-guide-channel-button\"]");

      if(btn) {

        const srOnly = btn.querySelector(".sr-only, [class*=\"sr-only\"]");

        if(srOnly) {

          const match = /row (\d+) of/.exec(srOnly.textContent);

          if(match) {

            // Row numbers in sr-only text are 1-based. Convert to 0-based for scroll offset calculation.
            rowNumber = parseInt(match[1], 10) - 1;
          }
        }
      }

      results.push({ name, rowNumber });
    }

    return results;
  });

  if(!channels) {

    return null;
  }

  // Assign DOM indices and populate the row number cache.
  const rendered: RenderedChannel[] = [];

  for(let i = 0; i < channels.length; i++) {

    const ch = channels[i];

    rendered.push({ domIndex: i, name: ch.name, rowNumber: ch.rowNumber });

    // Cache the row number for future direct-scroll lookups.
    if(ch.rowNumber >= 0) {

      guideRowCache.set(ch.name, ch.rowNumber);
    }
  }

  return rendered;
}

/**
 * Locates the on-now program cell for the channel at the given data-testid name (lowercased, trimmed), scrolls it into view, and returns its center coordinates
 * for a subsequent page.mouse.click(). We return coordinates rather than clicking inside the evaluate because page.mouse.click() generates the full pointer event
 * chain (pointerdown → mousedown → pointerup → mouseup → click) that React's event delegation requires, whereas a bare DOM .click() dispatches only a synthetic
 * click event that may not be processed reliably in a Puppeteer automation context.
 * @param page - The Puppeteer page object.
 * @param targetName - The lowercased, trimmed channel name to match against data-testid.
 * @returns Center coordinates of the on-now cell, or null if not found.
 */
async function locateOnNowCell(page: Page, targetName: string): Promise<Nullable<ClickTarget>> {

  return evaluateWithAbort(page, (target: string): Nullable<{ x: number; y: number }> => {

    const prefix = "live-guide-channel-kyber-";
    const containers = document.querySelectorAll("[data-testid^=\"" + prefix + "\"]");

    for(const el of Array.from(containers)) {

      const testid = el.getAttribute("data-testid") ?? "";
      const name = testid.slice(prefix.length).trim().replace(/\s+/g, " ").toLowerCase();

      if(name === target) {

        const row = el.closest("[data-testid=\"live-guide-row\"]");

        if(!row) {

          return null;
        }

        const onNow = row.querySelector(".LiveGuideProgram--first") as Nullable<HTMLElement>;

        if(!onNow) {

          return null;
        }

        onNow.scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

        const rect = onNow.getBoundingClientRect();

        if((rect.width > 0) && (rect.height > 0)) {

          return { x: rect.x + (rect.width / 2), y: rect.y + (rect.height / 2) };
        }

        return null;
      }
    }

    return null;
  }, [targetName]);
}

// US broadcast call sign pattern. Local affiliate stations have 3-4 uppercase letter call signs starting with W (east of the Mississippi) or K (west). This
// pattern is used to identify call signs in the guide grid so position-based inference can find local affiliates that sort by their hidden network name.
const CALL_SIGN_PATTERN = /^[WK][A-Z]{2,3}$/i;

// Network names that map to local affiliate call signs in Hulu's guide. The details API returns channel_info.name as the local call sign rather than the
// network name, so these names won't match any details response entry. Cold direct tuning is skipped for these — the guide grid handles them via
// position-based inference. On the first guide grid tune, the in-page interceptor captures the affiliate's channel_id from the playlist request and caches
// it under the channelSelector key (e.g., "abc" → uuid). Subsequent tunes resolve via warm direct tuning.
const NETWORK_NAMES_WITH_AFFILIATES = new Set([ "abc", "cbs", "cw", "fox", "nbc", "pbs" ]);

/**
 * Position-based inference for local affiliates. When binary search returns "missing" (target name sorts between rendered channels but no exact match), this
 * function identifies the local affiliate at the target's alphabetical insertion point.
 *
 * The guide sorts most local affiliates by their network name (ABC, CBS, etc.), but displays call signs in data-testid. The binary search converges to the
 * correct scroll position because the target network name sorts correctly, but the name match fails because the data-testid contains the call sign. The
 * affiliate occupies the DOM position where the network name would be if it existed.
 *
 * Algorithm:
 * 1. Filter rendered channels to non-call-sign names (these sort correctly by their displayed name)
 * 2. Find where the target would insert alphabetically among the non-call-sign neighbors
 * 3. The channel at the DOM position between those two neighbors is the local affiliate
 * @param rendered - The rendered channels in DOM order.
 * @param targetName - The lowercased target channel name.
 * @returns The name of the inferred local affiliate channel, or null if inference fails.
 */
function inferLocalAffiliate(rendered: RenderedChannel[], targetName: string): Nullable<string> {

  // Build a list of non-call-sign channels with their DOM indices. These channels sort alphabetically by their displayed name and serve as position anchors.
  const anchors: RenderedChannel[] = [];

  for(const ch of rendered) {

    if(!CALL_SIGN_PATTERN.test(ch.name)) {

      anchors.push(ch);
    }
  }

  // If no non-call-sign channels are rendered, we have no position anchors and cannot infer the affiliate.
  if(anchors.length === 0) {

    return null;
  }

  // Find the insertion point: the first anchor whose name sorts after the target.
  let insertBeforeIndex = -1;

  for(let i = 0; i < anchors.length; i++) {

    if(targetName.localeCompare(anchors[i].name) < 0) {

      insertBeforeIndex = i;

      break;
    }
  }

  // Determine the DOM index range between the two surrounding anchor channels.
  let lowerDomIndex: number;
  let upperDomIndex: number;

  if(insertBeforeIndex === 0) {

    // Target sorts before all anchors. Look for call signs before the first anchor.
    lowerDomIndex = -1;
    upperDomIndex = anchors[0].domIndex;
  } else if(insertBeforeIndex === -1) {

    // Target sorts after all anchors. Look for call signs after the last anchor.
    lowerDomIndex = anchors[anchors.length - 1].domIndex;
    upperDomIndex = rendered.length;
  } else {

    // Target sorts between two anchors.
    lowerDomIndex = anchors[insertBeforeIndex - 1].domIndex;
    upperDomIndex = anchors[insertBeforeIndex].domIndex;
  }

  // Find call sign channels in the DOM range between the two anchors.
  for(const ch of rendered) {

    if((ch.domIndex > lowerDomIndex) && (ch.domIndex < upperDomIndex) && CALL_SIGN_PATTERN.test(ch.name)) {

      return ch.name;
    }
  }

  return null;
}

/**
 * Waits for the play button to appear and clicks it using coordinate-based mouse events. Called by clickOnNowCellAndPlay after each on-now cell click attempt.
 * @param page - The Puppeteer page object.
 * @param playSelector - The CSS selector for the play button, or undefined if no play button is needed.
 * @param timeout - Optional timeout in milliseconds for the play button to appear. Defaults to CONFIG.streaming.videoTimeout.
 * @returns Result object with success status and optional failure reason.
 */
async function waitForPlayButton(page: Page, playSelector: Nullable<string> | undefined, timeout?: number): Promise<ChannelSelectorResult> {

  if(!playSelector) {

    return { success: true };
  }

  try {

    await page.waitForSelector(playSelector, { timeout: timeout ?? CONFIG.streaming.videoTimeout, visible: true });

    // Wait two animation frames for React to flush pending state updates. The play button may be visible in the DOM before React's concurrent mode has committed
    // the channel selection state to the component's event handlers. Without this, clicking immediately can trigger playback of the previously-selected channel
    // rather than the one we just chose. The double-rAF pattern synchronizes with the browser's rendering pipeline rather than using a fixed delay.
    await page.evaluate(async () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => { resolve(); }))));

    // Get the play button's coordinates for a real mouse click. Like the on-now cell click, we use page.mouse.click() to generate the full pointer event chain
    // rather than a bare DOM .click().
    const playTarget = await evaluateWithAbort(page, (selector: string): Nullable<{ x: number; y: number }> => {

      const el = document.querySelector(selector) as Nullable<HTMLElement>;

      if(!el) {

        return null;
      }

      el.scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

      const rect = el.getBoundingClientRect();

      if((rect.width > 0) && (rect.height > 0)) {

        return { x: rect.x + (rect.width / 2), y: rect.y + (rect.height / 2) };
      }

      return null;
    }, [playSelector]);

    if(!playTarget) {

      return { reason: "Play selector " + playSelector + " found but has no dimensions.", success: false };
    }

    await scrollAndClick(page, playTarget);

    return { success: true };
  } catch(error) {

    return { reason: "Could not click play selector " + playSelector + ": " + formatError(error) + ".", success: false };
  }
}

/**
 * Clicks the on-now program cell and waits for the play button, retrying on failure. Handles two distinct failure modes: (1) the on-now cell click doesn't
 * register because React hasn't fully hydrated event handlers — the play button never appears; (2) the play button appears and is clicked but the click is
 * silently swallowed — playback doesn't start. Both failures are detected by waitForPlayButton and trigger a retry of the full on-now cell and play button
 * sequence after a brief delay.
 * @param page - The Puppeteer page object.
 * @param clickTarget - The lowercased, trimmed channel name to locate in the guide grid.
 * @param playSelector - The CSS selector for the play button, or undefined if no play button is needed.
 * @param channelName - The original channel name for logging.
 * @returns Result object with success status and optional failure reason.
 */
async function clickOnNowCellAndPlay(page: Page, clickTarget: string, playSelector: Nullable<string> | undefined, channelName: string): Promise<ChannelSelectorResult> {

  // Maximum number of on-now cell click attempts. The first click may not register if React hasn't finished hydrating the guide's event handlers.
  const MAX_CLICK_ATTEMPTS = 3;

  // Shorter timeout for the play button on non-final attempts. When a click registers, the play button appears in under 10ms — this timeout only determines how
  // quickly we detect a missed click and retry. Keeping it low saves ~2s per failed attempt compared to the previous 3000ms value.
  const RETRY_PLAY_TIMEOUT = 1000;

  // Delay between click retries. Gives React additional time to finish hydrating event handlers.
  const CLICK_RETRY_DELAY = 1500;

  // Track the last click coordinates for diagnostic logging on failure.
  let lastClickCoords: Nullable<ClickTarget> = null;

  for(let attempt = 0; attempt < MAX_CLICK_ATTEMPTS; attempt++) {

    // eslint-disable-next-line no-await-in-loop
    const onNowTarget = await locateOnNowCell(page, clickTarget);

    if(!onNowTarget) {

      return { reason: "Found channel " + channelName + " but could not locate on-now program cell.", success: false };
    }

    lastClickCoords = onNowTarget;

    // eslint-disable-next-line no-await-in-loop
    await scrollAndClick(page, onNowTarget);

    // Use a shorter timeout on non-final attempts to enable quick retries. The final attempt uses the full default timeout as a last chance.
    const playTimeout = (attempt < MAX_CLICK_ATTEMPTS - 1) ? RETRY_PLAY_TIMEOUT : undefined;

    // eslint-disable-next-line no-await-in-loop
    const result = await waitForPlayButton(page, playSelector, playTimeout);

    if(result.success) {

      return result;
    }

    // The play button either didn't appear (on-now cell click missed) or appeared and was clicked but playback didn't start (play button click swallowed).
    // Retry the full sequence after a brief delay to allow further React hydration.
    if(attempt < MAX_CLICK_ATTEMPTS - 1) {

      LOG.debug("tuning:hulu", "Channel selection attempt failed for %s: %s. Retrying (attempt %s of %s).", channelName, result.reason, attempt + 2, MAX_CLICK_ATTEMPTS);

      // eslint-disable-next-line no-await-in-loop
      await delay(CLICK_RETRY_DELAY);
    }
  }

  // All click attempts exhausted. Run diagnostics to capture the page state at the moment of failure. This information is critical for debugging intermittent
  // guide interaction failures where the on-now cell is found but clicking it never produces the play button modal.
  if(lastClickCoords) {

    try {

      const diagnostics = await evaluateWithAbort(page, (x: number, y: number, target: string): {
        elementStack: string[];
        hydrated: boolean;
        onNowFound: boolean;
        pageAge: number;
      } => {

        // What elements are at the click coordinates, from topmost to bottommost? If something other than the on-now cell is intercepting clicks, it will
        // appear first in this list.
        const elements = document.elementsFromPoint(x, y);
        const elementStack = elements.slice(0, 5).map((el) => {

          const tag = el.tagName.toLowerCase();
          const id = el.id ? ("#" + el.id) : "";
          const cls = el.className && (typeof el.className === "string") ? ("." + el.className.trim().split(/\s+/).slice(0, 2).join(".")) : "";
          const testId = el.getAttribute("data-testid") ?? "";
          const testIdStr = testId ? ("[data-testid=\"" + testId + "\"]") : "";

          return tag + id + cls + testIdStr;
        });

        // Check whether the on-now cell has React internal properties, which indicates React has hydrated the element and attached event handlers. React
        // attaches __reactFiber$ and __reactProps$ properties to hydrated elements. If these are absent, the element is rendered but not interactive.
        let hydrated = false;
        let onNowFound = false;
        const prefix = "live-guide-channel-kyber-";
        const containers = document.querySelectorAll("[data-testid^=\"" + prefix + "\"]");

        for(const el of Array.from(containers)) {

          const testid = el.getAttribute("data-testid") ?? "";
          const name = testid.slice(prefix.length).trim().replace(/\s+/g, " ").toLowerCase();

          if(name === target) {

            const row = el.closest("[data-testid=\"live-guide-row\"]");

            if(row) {

              const onNow = row.querySelector(".LiveGuideProgram--first");

              if(onNow) {

                onNowFound = true;
                hydrated = Object.keys(onNow).some((k) => k.startsWith("__reactFiber") || k.startsWith("__reactProps"));
              }
            }

            break;
          }
        }

        return { elementStack, hydrated, onNowFound, pageAge: Math.round(performance.now()) };
      }, [ lastClickCoords.x, lastClickCoords.y, clickTarget ]);

      LOG.warn("Guide click diagnostics for %s: pageAge=%sms, onNowCell=%s, reactHydrated=%s, elementsAtClick=[%s].",
        channelName, diagnostics.pageAge, diagnostics.onNowFound ? "present" : "missing",
        diagnostics.hydrated ? "yes" : "no", diagnostics.elementStack.join(" > "));
    } catch(error) {

      LOG.warn("Could not collect guide click diagnostics for %s: %s.", channelName, formatError(error));
    }
  }

  return { reason: "Play button did not appear after " + String(MAX_CLICK_ATTEMPTS) + " on-now cell click attempts for " + channelName + ".", success: false };
}

/**
 * Attempts to tune to a channel via the fast path: injects UUID and EAB into the in-page fetch interceptor, which swaps the held playlist request to the target
 * channel. Called after binary search identifies a local affiliate whose UUID and EAB are available in the server-side caches. On success, dismisses the guide
 * overlay so the video player is visible for capture, then returns true. On failure (interceptor's directTunePromise already resolved due to timeout), returns
 * false so the caller can fall through to the click-based path.
 * @param page - The Puppeteer page object.
 * @param channelUuid - The channel UUID to inject into the interceptor.
 * @param currentEab - The currently-airing EAB to inject into the interceptor.
 * @param channelName - The original channel name for logging.
 * @returns True if the injection succeeded, false if the interceptor's Promise had already resolved.
 */
async function tryFastPathTune(page: Page, channelUuid: string, currentEab: string, channelName: string): Promise<boolean> {

  const injected = await page.evaluate((u: string, e: string): boolean => {

    const resolver = (window as unknown as Record<string, unknown>).__prismcastResolveDirectTune;

    if(typeof resolver === "function") {

      return (resolver as (uuid: string, eab: string) => boolean)(u, e);
    }

    return false;
  }, channelUuid, currentEab);

  if(!injected) {

    LOG.debug("tuning:hulu", "Fast-path injection failed for %s (interceptor Promise already resolved).", channelName);

    return false;
  }

  // Dismiss the guide overlay so the video player is visible for capture. The guide was opened for binary search and is still covering the player.
  try {

    await page.keyboard.press("Escape");
    await delay(300);
  } catch(error) {

    LOG.debug("tuning:hulu", "Could not dismiss guide after fast-path tune: %s.", formatError(error));
  }

  LOG.debug("tuning:hulu", "Direct tune via fetch interception for inferred affiliate %s (uuid=%s).", channelName, channelUuid);

  return true;
}

/**
 * Releases the held playlist request in the in-page fetch interceptor, reverting to the click-based flow. Called when the fast-path injection can't proceed
 * (UUID or EAB not in server-side caches) or wasn't attempted. Sets holdActive to false in the interceptor so subsequent playlist requests from the play button
 * click follow the affiliate capture path ([HULU-CACHE]).
 * @param page - The Puppeteer page object.
 */
async function releaseHeldPlaylist(page: Page): Promise<void> {

  await page.evaluate((): void => {

    const release = (window as unknown as Record<string, unknown>).__prismcastReleasePlaylist;

    if(typeof release === "function") {

      (release as () => void)();
    }
  });
}

/**
 * Guide grid strategy: finds a channel in a virtualized, alphabetically sorted channel grid by scrolling the page to the target row using binary search, then
 * clicking the on-now program cell to open the playback overlay. This strategy works for sites like Hulu Live TV where the channel guide is rendered as a
 * virtualized list — only ~13 of ~124 rows exist in the DOM at any time, positioned absolutely within a tall spacer div. The virtualizer renders rows based on
 * the page scroll position (`document.documentElement.scrollTop`), so we scroll to bring the target channel into the DOM, then interact with it directly.
 *
 * Three mechanisms handle different channel types:
 * 1. Binary search with passive row number caching — primary mechanism for most channels (~800ms first time, ~200ms on cache hit)
 * 2. Position-based inference — handles local affiliates when searching by network name (e.g., "ABC" finds the local call sign at the right sort position)
 * 3. Linear scan fallback — safety net for raw call sign searches or any channel the binary search cannot find (~2.4 seconds)
 *
 * The selection process:
 * 1. If listSelector is provided, click the tab/button to reveal the channel list (e.g., a "Channels" tab)
 * 2. Wait for the channel grid rows to render in the DOM
 * 3. Check the row number cache for a direct-scroll shortcut
 * 4. Binary search: scroll to the midpoint row, read rendered channels (caching row numbers), check for exact match or infer local affiliate
 * 5. If binary search fails, linear scan from top to bottom as a universal fallback
 * 6. Click the on-now program cell (`.LiveGuideProgram--first`) in the target channel's row to open the playback overlay
 * 7. If playSelector is provided, wait for and click the play button to start live playback
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector (channel name) and channelSelection config.
 * @returns Result object with success status and optional failure reason.
 */
async function guideGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const { channelSelection, channelSelector: channelName } = profile;
  const { listSelector, playSelector } = channelSelection;

  // Ensure the guide is open and on the correct tab. We wait for the tab button to become VISIBLE (not just present in the DOM) because the guide overlay may exist
  // in the DOM structure while still hidden during page initialization or animation. Clicking a hidden button dispatches a DOM event but has no visual effect — the
  // guide remains hidden and the virtualizer never populates rows. We use $eval for the click because overlapping elements (spinners, overlays) can intercept
  // Puppeteer's coordinate-based mouse events.
  if(listSelector) {

    try {

      await page.waitForSelector(listSelector, { timeout: CONFIG.streaming.videoTimeout, visible: true });
      await page.$eval(listSelector, (el) => { (el as HTMLElement).click(); });

      // Brief delay for the tab switch animation and virtualizer initialization.
      await delay(300);
    } catch(error) {

      LOG.warn("Could not click channel list selector %s: %s.", listSelector, formatError(error));
    }
  }

  // Wait for channel grid rows to become visible. If rows don't appear within a short initial window, retry the tab click once — the first click may have fired
  // during a transitional state before the guide was fully interactive, or the guide may have been animating open.
  let rowsVisible = false;

  for(let guideAttempt = 0; guideAttempt < 2; guideAttempt++) {

    try {

      const rowTimeout = (guideAttempt === 0) ? 5000 : CONFIG.streaming.videoTimeout;

      // eslint-disable-next-line no-await-in-loop
      await page.waitForSelector("[data-testid=\"live-guide-row\"]", { timeout: rowTimeout, visible: true });
      rowsVisible = true;

      break;
    } catch {

      // Rows not visible yet. On first failure, retry the tab click in case the guide wasn't fully interactive.
      if((guideAttempt === 0) && listSelector) {

        LOG.debug("tuning:hulu", "Guide rows not visible after initial wait. Retrying tab click for %s.", listSelector);

        try {

          // eslint-disable-next-line no-await-in-loop
          await page.$eval(listSelector, (el) => { (el as HTMLElement).click(); });

          // eslint-disable-next-line no-await-in-loop
          await delay(500);
        } catch {

          // Retry click failed. Fall through to final wait attempt.
        }
      }
    }
  }

  if(!rowsVisible) {

    return { reason: "Channel grid rows did not render.", success: false };
  }

  // Each row in the virtualized grid is exactly 112px tall. The total number of channels is derived from the spacer div's height.
  const ROW_HEIGHT = 112;

  // Normalize the channel name to lowercase for case-insensitive matching against data-testid suffixes.
  const normalizedName = normalizeChannelName(channelName);

  // Read grid metadata by walking up from a rendered row to find the spacer and viewport divs. The spacer div is the direct parent of all absolutely-positioned
  // rows, and its height equals totalRows * ROW_HEIGHT. The viewport div is the spacer's parent (overflow: hidden). We calculate gridDocTop as the viewport's
  // document-level offset, so that scrolling to gridDocTop + (rowIndex * ROW_HEIGHT) places that row at the top of the browser viewport.
  const gridMeta = await page.evaluate((rowHeight: number): Nullable<{ gridDocTop: number; totalRows: number }> => {

    const row = document.querySelector("[data-testid=\"live-guide-row\"]");

    if(!row) {

      return null;
    }

    // The spacer div is the parent of all row elements.
    const spacer = row.parentElement;

    if(!spacer) {

      return null;
    }

    const spacerHeight = spacer.offsetHeight;

    if(spacerHeight < rowHeight) {

      return null;
    }

    // The viewport div is the spacer's parent. Its position relative to the document determines our scroll offset.
    const viewport = spacer.parentElement;

    if(!viewport) {

      return null;
    }

    const gridDocTop = viewport.getBoundingClientRect().top + document.documentElement.scrollTop;

    return { gridDocTop, totalRows: Math.round(spacerHeight / rowHeight) };
  }, ROW_HEIGHT);

  if(!gridMeta) {

    return { reason: "Could not locate channel grid spacer element.", success: false };
  }

  const { gridDocTop, totalRows } = gridMeta;

  // Helper: scroll to a specific row index and wait for the virtualizer to render.
  const scrollToRow = async (rowIndex: number): Promise<void> => {

    await page.evaluate((scrollTo: number): void => {

      document.documentElement.scrollTop = scrollTo;
    }, gridDocTop + (rowIndex * ROW_HEIGHT));

    await delay(200);
  };

  // The name of the channel to click. This starts as the normalized target name but may be replaced by a local affiliate call sign via position inference.
  let clickTarget = normalizedName;

  // Check the row number cache for a direct-scroll shortcut. If we've seen this channel before, we can skip binary search entirely and scroll directly to it.
  const cachedRow = guideRowCache.get(normalizedName);

  if(cachedRow !== undefined) {

    LOG.debug("tuning:hulu", "Guide cache hit for %s at row %s.", channelName, cachedRow);

    await scrollToRow(cachedRow);

    // Read rendered channels to update the cache and confirm the channel is present.
    const rendered = await readRenderedChannels(page);

    if(rendered) {

      const match = rendered.find((ch) => ch.name === normalizedName);

      if(match) {

        return await clickOnNowCellAndPlay(page, normalizedName, playSelector, channelName);
      }
    }

    // Cache hit but channel not found at expected position. The guide may have changed. Clear this entry and fall through to binary search.
    LOG.debug("tuning:hulu", "Guide cache miss for %s. Falling back to binary search.", channelName);

    guideRowCache.delete(normalizedName);
  }

  // Binary search through the virtualized channel list. On each iteration we scroll to the midpoint of the current range, wait for the virtualizer to render,
  // then check if the target channel is among the ~13 rendered rows. If not, we compare the target name alphabetically against the first and last rendered
  // channel names to narrow the range. The search converges in ~3-4 iterations because the 13-row render window covers a large fraction of the remaining range.
  let low = 0;
  let high = totalRows - 1;
  const maxIterations = 10;
  let found = false;

  for(let iteration = 0; iteration < maxIterations; iteration++) {

    if(low > high) {

      break;
    }

    const mid = Math.floor((low + high) / 2);

    // eslint-disable-next-line no-await-in-loop
    await scrollToRow(mid);

    // Read all rendered channels, populating the row number cache as a side effect.
    // eslint-disable-next-line no-await-in-loop
    const rendered = await readRenderedChannels(page);

    if(!rendered || (rendered.length === 0)) {

      continue;
    }

    // Check for an exact match first.
    const exactMatch = rendered.find((ch) => ch.name === normalizedName);

    if(exactMatch) {

      found = true;

      break;
    }

    // Determine binary search direction by comparing the target against the first and last rendered non-call-sign channel names. Call sign channels (W*/K*
    // local affiliates) are excluded from direction comparison because they sort by hidden network name, not by their displayed call sign — using them for
    // localeCompare would send the search the wrong way.
    const nonCallSigns = rendered.filter((ch) => !CALL_SIGN_PATTERN.test(ch.name));

    if(nonCallSigns.length === 0) {

      // All rendered channels are call signs. Cannot determine direction. Move down and hope for better data.
      low = mid + 1;

      continue;
    }

    const first = nonCallSigns[0].name;
    const last = nonCallSigns[nonCallSigns.length - 1].name;

    if(normalizedName.localeCompare(first) < 0) {

      // Target sorts before the first visible non-call-sign channel. Scroll up (toward lower row indices).
      high = mid - 1;

      continue;
    }

    if(normalizedName.localeCompare(last) > 0) {

      // Target sorts after the last visible non-call-sign channel. Scroll down (toward higher row indices).
      low = mid + 1;

      continue;
    }

    // The target is alphabetically between the first and last rendered channels but was not found by exact data-testid match. This is the "missing" case — the
    // channel may be a local affiliate whose call sign doesn't match the network name we're searching for. Try position-based inference.
    const inferred = inferLocalAffiliate(rendered, normalizedName);

    if(inferred) {

      LOG.debug("tuning:hulu", "Inferred local affiliate %s for network name %s.", inferred, channelName);

      clickTarget = inferred;
      found = true;

      // Cache the network name → affiliate's row number so subsequent tunes for the same network name become direct scrolls.
      const inferredRow = guideRowCache.get(inferred);

      if(inferredRow !== undefined) {

        guideRowCache.set(normalizedName, inferredRow);
      }

      // Cross-reference the UUID cache so the network name can resolve to a warm-cache direct tune on subsequent requests. The details API returns the local
      // call sign as channel_info.name, so the UUID cache is keyed by call sign. The user's channelSelector uses the network name (e.g., "ABC"). Without this
      // cross-reference, local affiliates would always fall through to cold-cache guide grid tunes because the names never match.
      const inferredUuid = huluUuidCache.get(inferred);

      if(inferredUuid) {

        huluUuidCache.set(normalizedName, inferredUuid);

        LOG.debug("tuning:hulu", "Cross-referenced UUID cache: %s -> %s (from inferred affiliate %s).", channelName, inferredUuid, inferred);

        // Fast path: inject UUID+EAB into the in-page interceptor to tune via fetch interception instead of clicking. The server-side caches are populated
        // from details and listing API responses during page load, which typically complete before binary search finishes (~600ms). If both UUID and EAB are
        // available, the interceptor swaps the held playlist request to the target channel — no on-now cell click or play button interaction needed.
        const currentEab = findCurrentEab(inferredUuid);

        if(currentEab) {

          // eslint-disable-next-line no-await-in-loop
          const fastPathSuccess = await tryFastPathTune(page, inferredUuid, currentEab, channelName);

          if(fastPathSuccess) {

            return { success: true };
          }
        }
      }
    }

    break;
  }

  // If binary search did not find the channel (and position inference didn't identify a local affiliate), fall back to a linear scan through all channels. This
  // handles edge cases like raw call sign searches where localeCompare gives the wrong direction, or channels like "Lakeshore PBS" that sort by hidden network
  // name but don't match the W/K call sign pattern.
  if(!found) {

    LOG.debug("tuning:hulu", "Binary search did not find %s. Starting linear scan fallback.", channelName);

    for(let row = 0; row < totalRows; row += 10) {

      // eslint-disable-next-line no-await-in-loop
      await scrollToRow(row);

      // eslint-disable-next-line no-await-in-loop
      const rendered = await readRenderedChannels(page);

      if(!rendered) {

        continue;
      }

      const match = rendered.find((ch) => ch.name === normalizedName);

      if(match) {

        found = true;

        break;
      }
    }
  }

  if(!found) {

    // Log available channels from the guide row cache to help users identify the correct channelSelector value. The cache accumulates all channel names encountered
    // during binary search and linear scan, so it contains most or all channels even though the virtualized grid only renders ~13 at a time.
    const availableChannels = Array.from(guideRowCache.keys()).sort();

    if(availableChannels.length > 0) {

      logAvailableChannels({

        availableChannels,
        channelName,
        guideUrl: "https://www.hulu.com/live-tv",
        presetSuffix: "-hulu",
        providerName: "Hulu"
      });
    }

    return { reason: "Could not find channel " + channelName + " in guide grid.", success: false };
  }

  // Release the held playlist before falling through to the click path. This is a safety net — the inference block releases the hold in all its sub-paths, but
  // exact-match and linear-scan paths may reach here without having released. The release is a no-op if the hold was already released or was never active.
  await releaseHeldPlaylist(page);

  // Click the on-now program cell and wait for the play button, with click retries to handle React hydration timing.
  return await clickOnNowCellAndPlay(page, clickTarget, playSelector, channelName);
}

/**
 * Wraps guideGridStrategy with a single retry after dismissing any stale overlay that may be covering the guide grid. After a failed click attempt on the on-now
 * cell, the playback overlay or entity modal can remain open, obscuring the guide and preventing subsequent channel selection attempts from locating guide rows.
 * Pressing Escape closes most modal overlays in React-based SPAs.
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector (channel name) and channelSelection config.
 * @returns Result object with success status and optional failure reason.
 */
async function guideGridWithRetry(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  let result = await guideGridStrategy(page, profile);

  if(!result.success) {

    LOG.warn("Guide grid channel selection failed: %s. Dismissing overlay and retrying.", result.reason ?? "Unknown reason");

    try {

      await page.keyboard.press("Escape");
      await delay(500);
    } catch(error) {

      LOG.debug("tuning:hulu", "Could not dismiss guide overlay: %s.", formatError(error));
    }

    result = await guideGridStrategy(page, profile);
  }

  return result;
}

/**
 * Sets up server-side response interception on the page to capture channel UUID mappings from Hulu's guide details API and EAB schedules from the listing API.
 * As the live page loads, Hulu fetches program details from guide.hulu.com/guide/details in batches. Each response item includes channel_info with the
 * channel's UUID and display name. We intercept these responses to populate the huluUuidCache, enabling instant UUID resolution on subsequent tunes. The
 * in-page interceptor in resolveHuluDirectUrl() expands details request bodies with additional EABs so these responses cover all ~123 channels. Also bridges
 * in-page console signals (HULU-DIAG and HULU-CACHE) to the Node.js LOG. Uses a WeakSet to prevent duplicate listener registration.
 * @param page - The Puppeteer page object.
 */
function setupDetailsResponseInterception(page: Page): void {

  if(huluPagesWithListeners.has(page)) {

    return;
  }

  huluPagesWithListeners.add(page);

  // Bridge in-page console.log("[HULU-DIAG]", ...) messages from the evaluateOnNewDocument interceptor to our LOG system. This is the only way to get
  // diagnostic output from code running in the browser context back to the Node.js process.
  page.on("console", (msg) => {

    const text = msg.text();

    if(text.startsWith("[HULU-DIAG]")) {

      LOG.debug("tuning:hulu", text);
    }

    // Affiliate UUID capture: the in-page interceptor emits "[HULU-CACHE] targetName=channelUuid" when it observes a playlist passthrough for an affiliate guide
    // grid tune. Cache the UUID under the channelSelector key so subsequent tunes resolve via warm direct tuning instead of the guide grid.
    if(text.startsWith("[HULU-CACHE] ")) {

      const payload = text.slice("[HULU-CACHE] ".length);
      const eqIdx = payload.indexOf("=");

      if(eqIdx > 0) {

        const name = payload.slice(0, eqIdx);
        const channelUuid = payload.slice(eqIdx + 1);

        huluUuidCache.set(name, channelUuid);

        LOG.debug("tuning:hulu", "Cached affiliate UUID from playlist: %s -> %s. UUID cache size: %s.", name, channelUuid, huluUuidCache.size);
      }
    }
  });

  page.on("response", (response) => {

    const url = response.url();

    if(response.status() !== 200) {

      return;
    }

    // Details API: capture channel UUID mappings for the persistent cache.
    if(url.includes("guide.hulu.com/guide/details")) {

      void response.json().then((data: HuluDetailsResponse) => {

        if(!Array.isArray(data.items)) {

          return;
        }

        const channelsSeen = new Set<string>();

        for(const item of data.items) {

          const info = item.channel_info;

          if(info?.name && info.id) {

            huluUuidCache.set(normalizeChannelName(info.name), info.id);
            channelsSeen.add(info.name);
          }
        }

        LOG.debug("tuning:hulu", "Details API: %s items, %s unique channels. UUID cache size: %s.",
          data.items.length, channelsSeen.size, huluUuidCache.size);
      }).catch(() => {

        // CORS preflight responses (OPTIONS) return empty bodies that fail JSON parsing. This is expected and harmless.
      });

      return;
    }

    // Listing API: capture program schedules (EABs with airing times) for each channel. The app fires listing requests on every page load covering all ~130
    // channels with ~8 hours of programs. We cache the full schedule per channel UUID so resolveHuluDirectUrl can find the currently-airing EAB at tune time.
    if(url.includes("guide.hulu.com/guide/listing")) {

      void response.json().then((data: HuluListingResponse) => {

        if(!Array.isArray(data.channels)) {

          return;
        }

        let programCount = 0;

        for(const channel of data.channels) {

          if(channel.id && Array.isArray(channel.programs) && (channel.programs.length > 0)) {

            huluEabCache.set(channel.id, channel.programs);
            programCount += channel.programs.length;
          }
        }

        LOG.debug("tuning:hulu", "Listing API: %s channels, %s programs. EAB cache size: %s.", data.channels.length, programCount, huluEabCache.size);
      }).catch(() => {

        // CORS preflight responses (OPTIONS) return empty bodies that fail JSON parsing. This is expected and harmless.
      });
    }
  });
}

/**
 * Finds the currently-airing EAB for a channel from the EAB cache. Searches the cached program schedule for the given channel UUID and returns the EAB of the
 * program whose airing window brackets the current time. Returns null if the channel has no cached programs or if no program is currently airing (stale cache
 * or program boundary gap).
 * @param channelUuid - The channel UUID to look up in the EAB cache.
 * @returns The currently-airing EAB string, or null if no match.
 */
function findCurrentEab(channelUuid: string): Nullable<string> {

  const programs = huluEabCache.get(channelUuid);

  if(!programs) {

    return null;
  }

  const now = Date.now();

  for(const program of programs) {

    if((now >= new Date(program.airingStart).getTime()) && (now < new Date(program.airingEnd).getTime())) {

      return program.eab;
    }
  }

  return null;
}

/**
 * Resolves a direct URL for Hulu channel tuning and installs a fetch interceptor that handles both warm and cold tunes. On warm cache (UUID and EAB known from
 * previous API responses), the interceptor has both values at install time and swaps the first playlist request immediately. On cold cache, the interceptor
 * captures the target channel's UUID from the details API response and EAB from the listing API response, holding the playlist request until both are resolved
 * (with an 8-second timeout). On all tunes, the interceptor also expands listing and details API requests to populate the full UUID cache (~123 channels) for
 * future warm tunes. Returns null only for known local affiliate network names on cold cache (ABC, CBS, NBC, Fox, PBS, CW) — their channel_info.name in the
 * details API uses call signs that don't match the channelSelector, so the guide grid handles them via position-based inference.
 * @param channelSelector - The channel selector string (e.g., "Fox", "CNN", "ESPN").
 * @param page - The Puppeteer page for evaluateOnNewDocument installation and response interception setup.
 * @returns The Hulu live URL for direct tuning, or null for local affiliate networks on cold cache or interceptor installation failure.
 */
async function resolveHuluDirectUrl(channelSelector: string, page: Page): Promise<Nullable<string>> {

  const normalizedName = normalizeChannelName(channelSelector);
  const cachedUuid = huluUuidCache.get(normalizedName) ?? null;

  // Set up server-side response listeners to populate the UUID and EAB caches. Must be set up before navigation so we capture details and listing API responses
  // during both the guide grid flow (cold cache) and the intercepted page load (warm cache).
  setupDetailsResponseInterception(page);

  // Look up the currently-airing EAB for the target channel (if UUID is known). On warm cache (both UUID and EAB available), the interceptor has both at install
  // time and swaps immediately. On cold cache or stale EAB, the interceptor captures the values from API responses during page load, holding the playlist until
  // both are resolved. The guide grid only runs for local affiliate network names (NETWORK_NAMES_WITH_AFFILIATES) whose names don't match the details API.
  const cachedEab = cachedUuid ? findCurrentEab(cachedUuid) : null;
  const isWarmCache = Boolean(cachedUuid && cachedEab);

  if(isWarmCache) {

    LOG.debug("tuning:hulu", "resolveHuluDirectUrl: warm cache for %s (uuid=%s, eab=%s).", channelSelector, cachedUuid, cachedEab);
  } else if(!cachedUuid) {

    // On cold cache, local affiliate network names fall through to the guide grid because their channel_info.name uses call signs that the interceptor
    // can't match. All other channels attempt cold direct tuning — the interceptor captures UUID+EAB from API responses during page load.
    if(NETWORK_NAMES_WITH_AFFILIATES.has(normalizedName)) {

      LOG.debug("tuning:hulu", "resolveHuluDirectUrl: cold cache for %s (local affiliate). Falling through to guide grid.", channelSelector);
    } else {

      LOG.debug("tuning:hulu", "resolveHuluDirectUrl: cold cache for %s. Attempting cold direct tune via API interception.", channelSelector);
    }
  } else {

    LOG.debug("tuning:hulu", "resolveHuluDirectUrl: UUID cached for %s but no current EAB. Attempting direct tune via API interception.", channelSelector);
  }

  // Collect all unique UUIDs from the cache for listing API request expansion. On warm tunes, this keeps EAB schedules fresh for all known channels. On cold
  // tunes after the first, this expands the listing request beyond the mini-guide's ~10 UUIDs. Empty on the very first cold tune (no cached data yet).
  const allCachedUuids = [...new Set(huluUuidCache.values())];

  // Collect one current EAB per cached channel for details API request expansion. On warm tunes, this fills the UUID cache completely. On cold tunes after the
  // first, this supplements the in-page listing-derived EABs. Empty on the very first cold tune — the interceptor builds EABs dynamically from the listing API
  // response instead.
  const allCurrentEabs: string[] = [];

  for(const channelUuid of huluEabCache.keys()) {

    const currentEab = findCurrentEab(channelUuid);

    if(currentEab) {

      allCurrentEabs.push(currentEab);
    }
  }

  /* Install the fetch interceptor before navigation on both warm and cold tunes. On warm tunes, it swaps channel_id and content_eab_id in playlist requests
   * immediately. On cold non-affiliate tunes, it holds the playlist request and swaps after capturing both values from listing and details API responses. On all
   * tunes, it captures listing API responses to build an in-page EAB map and expands subsequent details API requests, populating the UUID cache to ~123 channels
   * on a single page load. The script runs via evaluateOnNewDocument — it executes before any page JavaScript, patching window.fetch so Hulu's module-scoped
   * fetch reference captures the interceptor. Each stream gets its own page via createPageWithCapture(), so there's no persistence concern.
   */
  try {

    // Always hold the playlist request. On warm cache, the interceptor swaps immediately. On cold cache (including affiliates), the guide grid strategy will either
    // inject UUID+EAB via the fast path (resolving the held playlist) or release the hold before falling through to the click path.
    const attemptDirectTune = true;

    await page.evaluateOnNewDocument((
      initialUuid: string, initialEab: string, cachedUuids: string[], cachedEabs: string[],
      targetName: string, holdPlaylist: boolean
    ): void => {

      // evaluateOnNewDocument runs in every frame, including ad iframes. We only want to intercept fetches in the main frame — iframes don't make Hulu API
      // calls. The try/catch handles cross-origin iframes where accessing window.top throws a SecurityError.
      try {

        if(window.self !== window.top) {

          return;
        }
      } catch {

        return;
      }

      /* Save original fetch before any page script runs. Using bind(window) ensures correct this-context when called outside window's property access chain,
       * since Hulu's modules capture window.fetch in closures and invoke it as a plain function call.
       */
      const originalFetch = window.fetch.bind(window);

      // Mutable copies of the warm-cache values. On warm cache, these are set at install time. On cold cache, they start empty and are populated by
      // captureDetailsData (uuid) and captureListingData (eab) as API responses arrive during page load.
      let uuid = initialUuid;
      let eab = initialEab;

      // Mutable copy of holdPlaylist. The guide grid strategy sets this to false via __prismcastReleasePlaylist when the fast-path injection fails, reverting to
      // the click-based flow where the playlist should pass through immediately and capture the affiliate UUID via [HULU-CACHE].
      let holdActive = holdPlaylist;

      // Tracks whether the initial page-load playlist request has been seen. When holdActive is false (cold affiliate guide grid click fallback), the first live
      // playlist request carries the previously-playing channel's UUID — not the target's. We skip that one and only capture from subsequent requests, which
      // are triggered by the guide grid's play button click and carry the correct affiliate UUID.
      let initialPlaylistSeen = false;

      // eslint-disable-next-line no-console
      console.log("[HULU-DIAG] Fetch interceptor installed (" + ((uuid && eab) ? "warm" : "cold") + ")." + (uuid ? " uuid=" + uuid + " eab=" + (eab || "pending") : ""));

      // Promise that resolves when both UUID and EAB are available for the playlist swap. On warm cache, resolves immediately (both injected at install time).
      // On cold cache, resolves when the in-page listing and details API response parsers have captured both values. The playlist handler awaits this Promise
      // to hold the request until the target channel's data is ready.
      let directTuneResolve: (() => void) | null = null;

      const directTunePromise = (uuid && eab) ?
        Promise.resolve() :
        new Promise<void>((resolve) => { directTuneResolve = resolve; });

      // Checks whether both UUID and EAB are now known and resolves directTunePromise if so. Called after each successful capture from listing or details API
      // responses. Order-independent — handles both "listing first, details second" and "details first, listing second" sequences.
      function tryResolveDirectTune(): void {

        if(uuid && eab && directTuneResolve) {

          directTuneResolve();
          directTuneResolve = null;
        }
      }

      // Injection endpoint for the guide grid strategy's fast-path affiliate tune. After binary search identifies the local affiliate and the server-side caches
      // provide the UUID and EAB, the strategy calls this function via page.evaluate to feed both values into the interceptor. The held playlist request then
      // resumes with the swapped channel_id and content_eab_id. Returns true if the injection was accepted (directTunePromise not yet resolved), false if the
      // Promise already resolved (8s timeout expired or a previous injection).
      (window as unknown as Record<string, unknown>).__prismcastResolveDirectTune = (u: string, e: string): boolean => {

        if(!directTuneResolve) {

          return false;
        }

        uuid = u;
        eab = e;
        tryResolveDirectTune();

        return true;
      };

      // Release endpoint for the guide grid strategy's click-path fallback. When the fast-path injection can't proceed (UUID or EAB not in server-side caches),
      // the strategy calls this to unblock the held playlist request and revert to the click-based flow. Sets holdActive to false so the playlist handler follows
      // the affiliate capture path ([HULU-CACHE]) on subsequent requests from the play button click.
      (window as unknown as Record<string, unknown>).__prismcastReleasePlaylist = (): void => {

        holdActive = false;

        if(directTuneResolve) {

          directTuneResolve();
          directTuneResolve = null;
        }
      };

      // In-page EAB map built dynamically from listing API responses. On the first cold tune, cachedEabs is empty (no pre-computed data from Node.js), so this
      // map is the sole source of expansion data for details API requests. Populated asynchronously when the first listing response arrives.
      const capturedCurrentEabs = new Map<string, string>();

      // Deferred Promise that resolves when captureListingData finishes parsing the first listing response. Details API requests await this Promise (with a 2s
      // timeout) before expanding, so even the very first details request gets expanded with listing-derived EABs. The listing response typically arrives
      // ~200-600ms after the request fires, adding minimal latency to the details response — and the details API is not on the critical path for the channel
      // grid that binary search needs (it only provides program info for the mini-guide overlay).
      let listingCapturedResolve: (() => void) | null = null;

      const listingCapturedPromise = new Promise<void>((resolve) => {

        listingCapturedResolve = resolve;
      });

      // Fire-and-forget: parses a listing API response to build the in-page EAB map. For each channel, finds the currently-airing program by comparing airing
      // times against the current time, mirroring the server-side findCurrentEab() logic. Called on all listing return paths (expanded and passthrough).
      function captureListingData(response: Response): void {

        try {

          void response.clone().json().then((data: Record<string, unknown>) => {

            const channels = data.channels;

            if(!Array.isArray(channels)) {

              return;
            }

            const now = Date.now();
            let captured = 0;

            for(const channel of channels as Record<string, unknown>[]) {

              if((typeof channel.id !== "string") || !Array.isArray(channel.programs)) {

                continue;
              }

              for(const program of channel.programs as Record<string, unknown>[]) {

                if((typeof program.eab === "string") && (typeof program.airingStart === "string") && (typeof program.airingEnd === "string")) {

                  if((now >= new Date(program.airingStart).getTime()) && (now < new Date(program.airingEnd).getTime())) {

                    capturedCurrentEabs.set(channel.id, program.eab);
                    captured++;

                    break;
                  }
                }
              }
            }

            if(captured > 0) {

              // eslint-disable-next-line no-console
              console.log("[HULU-DIAG] Listing response captured: " + String(captured) + " current EABs for details expansion.");
            }

            // If the target UUID is already known (from a details response that arrived first), look up its EAB in the freshly captured listing data. This
            // handles the "details first, listing second" ordering — the details parser set uuid but couldn't find the EAB yet.
            if(uuid && !eab) {

              const capturedEab = capturedCurrentEabs.get(uuid);

              if(capturedEab) {

                eab = capturedEab;
                tryResolveDirectTune();
              }
            }

            // Signal that listing data is available. Any details request awaiting listingCapturedPromise will now proceed with expansion.
            if(listingCapturedResolve) {

              listingCapturedResolve();
              listingCapturedResolve = null;
            }
          // eslint-disable-next-line @typescript-eslint/no-empty-function
          }).catch(() => {});
        } catch {

          // Response doesn't support clone() or json() — silently skip capture.
        }
      }

      // Fire-and-forget: parses a details API response to extract the target channel's UUID for cold direct tune. On warm cache (uuid already set at install
      // time), this is a no-op — the UUID is already available. On cold cache, this is the primary mechanism for discovering the target's UUID: the expanded
      // details response contains channel_info for all ~123 channels, and we match by normalized name. After finding the UUID, looks up the EAB in the captured
      // listing data and calls tryResolveDirectTune to release the held playlist request.
      function captureDetailsData(response: Response): void {

        if(uuid) {

          return;
        }

        try {

          void response.clone().json().then((data: Record<string, unknown>) => {

            const items = data.items;

            if(!Array.isArray(items)) {

              return;
            }

            for(const item of items as Record<string, unknown>[]) {

              const info = item.channel_info as Record<string, unknown> | undefined;

              if(info && (typeof info.name === "string") && (typeof info.id === "string")) {

                const name = info.name.trim().toLowerCase().replace(/\s+/g, " ");

                if(name === targetName) {

                  uuid = info.id;

                  // eslint-disable-next-line no-console
                  console.log("[HULU-DIAG] Details response: found UUID " + uuid + " for " + targetName + ".");

                  // Look up the EAB for this UUID in already-captured listing data. If the listing response arrived before this details response, the EAB is
                  // already in capturedCurrentEabs and we can resolve directTunePromise immediately.
                  const capturedEab = capturedCurrentEabs.get(uuid);

                  if(capturedEab) {

                    eab = capturedEab;
                  }

                  tryResolveDirectTune();

                  break;
                }
              }
            }
          // eslint-disable-next-line @typescript-eslint/no-empty-function
          }).catch(() => {});
        } catch {

          // Response doesn't support clone() or json() — silently skip capture.
        }
      }

      // Extracts the request body from either the init options or a cloned Request object. Returns null if the body is not a string and the input is not a
      // Request. Used by all three API handlers to normalize body extraction across the two fetch call patterns Hulu uses.
      async function getBodyText(input: RequestInfo | URL, init?: RequestInit): Promise<string | null> {

        if(init && (typeof init.body === "string")) {

          return init.body;
        }

        if(input instanceof Request) {

          return await input.clone().text();
        }

        return null;
      }

      // Sends a fetch request with a modified body, reconstructing the request from a URL string when the input is a Request object. Using input.url as a
      // string sidesteps the Fetch spec's body-lock check — getBodyText() locked the ReadableStream via input.clone().text(), and the spec checks lock status
      // on the first argument BEFORE applying init overrides, causing a TypeError even though we provide a replacement body.
      async function fetchWithBody(input: RequestInfo | URL, init: RequestInit | undefined, body: string): Promise<Response> {

        if(input instanceof Request) {

          return await originalFetch(input.url, {

            body,
            credentials: input.credentials,
            headers: input.headers,
            method: input.method,
            mode: input.mode
          });
        }

        return await originalFetch(input, Object.assign({}, init ?? {}, { body }));
      }

      // Marked async to satisfy @typescript-eslint/promise-function-async since the function returns Promise<Response>.
      window.fetch = async function(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {

        const url = (typeof input === "string") ? input : (input instanceof URL) ? input.href : input.url;

        // Expand listing API requests to include all cached UUIDs. The mini-guide listing only sends ~10 channel UUIDs, but we have UUIDs from previous
        // tunes. Injecting them into the request keeps the EAB cache fresh for all known channels, so subsequent warm tunes can resolve a current EAB without
        // falling back to the guide grid. The full guide already sends all ~130 UUIDs, so the API handles larger arrays without issue.
        if(url.includes("guide.hulu.com") && url.includes("/guide/listing")) {

          const listingBody = await getBodyText(input, init);

          if(listingBody) {

            try {

              const listingObj = JSON.parse(listingBody) as Record<string, unknown>;

              if(Array.isArray(listingObj.channels)) {

                const existing = new Set(listingObj.channels as string[]);

                for(const id of cachedUuids) {

                  existing.add(id);
                }

                listingObj.channels = [...existing];

                const listingResponse = await fetchWithBody(input, init, JSON.stringify(listingObj));

                captureListingData(listingResponse);

                return listingResponse;
              }
            } catch {

              // Body parse error — pass through unmodified.
            }
          }

          const listingPassthrough = await originalFetch(input, init);

          captureListingData(listingPassthrough);

          return listingPassthrough;
        }

        // Expand details API requests to include EABs for all known channels. The mini-guide details request only covers ~7 visible channels (~19 EABs),
        // but we have EABs from two sources: pre-injected cachedEabs from Node.js (available on warm tunes and subsequent cold tunes), and dynamically
        // captured capturedCurrentEabs from the listing API response (the primary source on the first cold tune). Merging both ensures the details response
        // returns channel_info (name→UUID mappings) for all ~123 channels on any tune, even the very first cold tune after a restart.
        if(url.includes("guide.hulu.com") && url.includes("/guide/details")) {

          // If the in-page EAB map is empty and no pre-injected EABs are available, wait for the listing response to be parsed before proceeding. This holds
          // the first details request for ~200-600ms until captureListingData resolves the Promise. The 2s timeout is a safety net — if the listing response
          // never arrives, the details request proceeds without expansion (same behavior as before).
          if((capturedCurrentEabs.size === 0) && (cachedEabs.length === 0)) {

            await Promise.race([ listingCapturedPromise, new Promise<void>((resolve) => { setTimeout(resolve, 2000); }) ]);
          }

          const detailsBody = await getBodyText(input, init);

          if(detailsBody) {

            try {

              const detailsObj = JSON.parse(detailsBody) as Record<string, unknown>;

              if(Array.isArray(detailsObj.eabs)) {

                const existing = new Set(detailsObj.eabs as string[]);

                // Add pre-injected EABs from Node.js cache (available on warm tunes and subsequent cold tunes with partial cache data).
                for(const id of cachedEabs) {

                  existing.add(id);
                }

                // Add dynamically captured EABs from in-page listing response (primary source on the first cold tune when cachedEabs is empty).
                for(const capturedEab of capturedCurrentEabs.values()) {

                  existing.add(capturedEab);
                }

                detailsObj.eabs = [...existing];

                const expandedResponse = await fetchWithBody(input, init, JSON.stringify(detailsObj));

                captureDetailsData(expandedResponse);

                return expandedResponse;
              }
            } catch {

              // Body parse error — pass through unmodified.
            }
          }

          const detailsPassthrough = await originalFetch(input, init);

          captureDetailsData(detailsPassthrough);

          return detailsPassthrough;
        }

        // Intercept every live playlist request and swap channel_id and content_eab_id to the target channel. The 204 CORS preflight precedes the real POST,
        // so we keep swapping rather than one-shot to ensure the real request gets the modified body. Each stream gets its own page, so persistence is safe.
        if(url.includes("play.hulu.com") && url.includes("playlist")) {

          // Wait for UUID and EAB to be available. On warm cache, directTunePromise resolved immediately at creation time. On cold cache with holdActive
          // enabled, this holds the playlist request until either: (1) the guide grid strategy injects UUID+EAB via __prismcastResolveDirectTune (fast path), or
          // (2) the in-page listing/details parsers capture both values (cold non-affiliate), or (3) the 8-second timeout expires (safety net). When holdActive
          // is false (set by __prismcastReleasePlaylist after fast-path failure), the playlist passes through with affiliate UUID capture for the click fallback.
          if(holdActive) {

            await Promise.race([ directTunePromise, new Promise<void>((resolve) => { setTimeout(resolve, 8000); }) ]);
          }

          if(!uuid || !eab) {

            // Affiliate guide grid click fallback: holdActive was set to false by __prismcastReleasePlaylist, or was never enabled. The guide grid handles channel
            // selection via clicking, and Hulu's app fires this playlist request with the affiliate's channel_id. Capture it and emit a console signal so the
            // server-side listener can cache it under the channelSelector key for warm direct tuning on subsequent tunes.
            if(!holdActive) {

              const affiliateBody = await getBodyText(input, init);

              if(affiliateBody) {

                try {

                  const bodyObj = JSON.parse(affiliateBody) as Record<string, unknown>;

                  if((bodyObj.play_intent === "live") && (typeof bodyObj.channel_id === "string")) {

                    if(initialPlaylistSeen) {

                      // eslint-disable-next-line no-console
                      console.log("[HULU-CACHE] " + targetName + "=" + bodyObj.channel_id);
                    }

                    initialPlaylistSeen = true;
                  }
                } catch {

                  // Body parse error — skip capture.
                }

                // Must use fetchWithBody — getBodyText may have locked the Request's ReadableStream via clone().
                return fetchWithBody(input, init, affiliateBody);
              }

              return originalFetch(input, init);
            }

            // eslint-disable-next-line no-console
            console.log("[HULU-DIAG] Playlist passthrough: no uuid/eab resolved for " + targetName + " (uuid=" + uuid + ", eab=" + (eab ? "yes" : "no") + ").");

            return originalFetch(input, init);
          }

          const bodyText = await getBodyText(input, init);

          if(bodyText) {

            try {

              const bodyObj = JSON.parse(bodyText) as Record<string, unknown>;

              if(bodyObj.play_intent === "live") {

                const originalChannelId = String(bodyObj.channel_id);

                // eslint-disable-next-line camelcase
                bodyObj.channel_id = uuid;

                // eslint-disable-next-line camelcase
                bodyObj.content_eab_id = eab;

                // eslint-disable-next-line no-console
                console.log("[HULU-DIAG] Playlist swapped: channel_id " + originalChannelId + " -> " + uuid + ", eab -> " + eab);

                return await fetchWithBody(input, init, JSON.stringify(bodyObj));
              }
            } catch {

              // Body parse error — pass through unmodified.
            }
          }
        }

        return originalFetch(input, init);
      };
    }, cachedUuid ?? "", cachedEab ?? "", allCachedUuids, allCurrentEabs, normalizedName, attemptDirectTune);
  } catch(error) {

    LOG.debug("tuning:hulu", "Failed to install Hulu fetch interceptor: %s.", formatError(error));

    return null;
  }

  // For known local affiliate network names on cold cache, skip cold direct tuning — the channel_info.name in the details API uses the local call sign
  // rather than the network name, so the in-page interceptor can't match by name. The guide grid handles these via position-based inference and
  // cross-references the UUID for warm tunes on subsequent requests.
  if(!cachedUuid && NETWORK_NAMES_WITH_AFFILIATES.has(normalizedName)) {

    return null;
  }

  LOG.debug("tuning:hulu", "Fetch interceptor installed. Returning URL: %s.", HULU_LIVE_URL);

  return HULU_LIVE_URL;
}

/**
 * Invalidates the cached channel UUID for the given channel selector. Called when a cached direct URL fails to produce a working stream, so the next tune
 * attempts the cold cache path (details API extraction) or falls back to the guide grid.
 * @param channelSelector - The channel selector string to invalidate.
 */
function invalidateHuluDirectUrl(channelSelector: string): void {

  huluUuidCache.delete(normalizeChannelName(channelSelector));
}

export const huluStrategy: ChannelStrategyEntry = {

  clearCache: clearHuluCache,
  execute: guideGridWithRetry,
  invalidateDirectUrl: invalidateHuluDirectUrl,
  resolveDirectUrl: resolveHuluDirectUrl
};
