/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hulu.ts: Hulu Live TV guide grid channel selection strategy with binary search, position-based inference, and row caching.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ClickTarget, Nullable } from "../../types/index.js";
import { LOG, delay, evaluateWithAbort, formatError } from "../../utils/index.js";
import { logAvailableChannels, normalizeChannelName, scrollAndClick } from "../channelSelection.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";

// Guide grid row number cache. Maps lowercased, trimmed channel names from data-testid attributes to their row numbers (from sr-only text). Populated passively
// during binary search iterations and used for direct-scroll optimization on subsequent tunes. Session-scoped — cleared when the browser restarts.
const guideRowCache = new Map<string, number>();

/**
 * Clears the Hulu guide row cache. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts.
 */
export function clearHuluCache(): void {

  guideRowCache.clear();
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

/**
 * Position-based inference for local affiliates. When binary search returns "missing" (target name sorts between rendered channels but no exact match), this
 * function identifies the local affiliate at the target's alphabetical insertion point.
 *
 * The guide sorts local affiliates by their network name (ABC, CBS, NBC, etc.), but displays call signs (WLS, WBBM, WMAQ) in data-testid. The binary search
 * converges to the correct scroll position because the target network name sorts correctly, but the name match fails because the data-testid contains the call
 * sign. The affiliate occupies the DOM position where the network name would be if it existed.
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
 * Clicks the on-now program cell and waits for the play button with navigation verification, retrying on failure. Handles two distinct failure modes: (1) the
 * on-now cell click doesn't register because React hasn't fully hydrated event handlers — the play button never appears; (2) the play button appears and is
 * clicked but the click is silently swallowed — navigation to the player page doesn't occur. Both failures are detected by waitForPlayButton and trigger a
 * retry of the full on-now cell and play button sequence after a brief delay.
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

    // The play button either didn't appear (on-now cell click missed) or appeared and was clicked but navigation didn't occur (play button click swallowed).
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
 * Guide grid strategy: finds a channel in a virtualized, alphabetically sorted channel grid by scrolling the page to the target row using binary search, then
 * clicking the on-now program cell to open the playback overlay. This strategy works for sites like Hulu Live TV where the channel guide is rendered as a
 * virtualized list — only ~13 of ~124 rows exist in the DOM at any time, positioned absolutely within a tall spacer div. The virtualizer renders rows based on
 * the page scroll position (`document.documentElement.scrollTop`), so we scroll to bring the target channel into the DOM, then interact with it directly.
 *
 * Three mechanisms handle different channel types:
 * 1. Binary search with passive row number caching — primary mechanism for most channels (~800ms first time, ~200ms on cache hit)
 * 2. Position-based inference — handles local affiliates when searching by network name (e.g., "ABC" finds WLS at the right sort position)
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
export async function guideGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

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
    }

    break;
  }

  // If binary search did not find the channel (and position inference didn't identify a local affiliate), fall back to a linear scan through all channels. This
  // handles edge cases like raw call sign searches (e.g., "WLS") where localeCompare gives the wrong direction, or channels like "Lakeshore PBS" that sort by
  // hidden network name but don't match the W/K call sign pattern.
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

  // Click the on-now program cell and wait for the play button, with click retries to handle React hydration timing.
  return await clickOnNowCellAndPlay(page, clickTarget, playSelector, channelName);
}
