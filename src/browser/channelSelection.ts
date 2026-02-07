/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelSelection.ts: Channel selection strategies for multi-channel streaming sites.
 */
import type { ChannelSelectionConfig, ChannelSelectorResult, ClickTarget, ResolvedSiteProfile } from "../types/index.js";
import { LOG, delay, evaluateWithAbort, formatError } from "../utils/index.js";
import { CONFIG } from "../config/index.js";
import type { Page } from "puppeteer-core";

/* Multi-channel streaming sites (like USA Network) present multiple channels on a single page, with a program guide for each channel. Users must select which
 * channel they want to watch by clicking on a show in the guide. This module provides a strategy-based system for automating that channel selection.
 *
 * The strategy pattern allows different sites to have different selection mechanisms:
 * - guideGrid: Scroll a virtualized channel grid to the target channel via binary search on document.documentElement.scrollTop, then click the on-now program
 *   cell and play button (Hulu Live). Supports position-based inference for local affiliate call signs and a linear scan fallback.
 * - thumbnailRow: Find channel by matching image URL slug, click adjacent show entry on the same row (USA Network)
 * - tileClick: Find channel tile by matching image URL slug, click tile, then click play button on modal (Disney+ live)
 *
 * Each strategy is a self-contained function that takes the page and channel identifier, and returns a success/failure result. The main selectChannel() function
 * delegates to the appropriate strategy based on the profile configuration.
 */

// Guide grid row number cache. Maps lowercased, trimmed channel names from data-testid attributes to their row numbers (from sr-only text). Populated passively
// during binary search iterations and used for direct-scroll optimization on subsequent tunes. Session-scoped — cleared when the browser restarts.
const guideRowCache = new Map<string, number>();

/**
 * Clears the guide grid row number cache. Called by handleBrowserDisconnect() in browser/index.ts when the browser restarts, since the guide layout may change
 * between sessions.
 */
export function clearGuideCache(): void {

  guideRowCache.clear();
}

/* These utilities are shared across channel selection strategies. They handle common operations like finding elements, scrolling, and clicking.
 */

/**
 * Clicks at the specified coordinates after a brief settle delay. The delay allows scroll animations and lazy-loaded content to finish before the click fires.
 * Callers are responsible for scrolling the target element into view (typically via scrollIntoView inside a page.evaluate call) before invoking this function.
 * @param page - The Puppeteer page object.
 * @param target - The x/y coordinates to click.
 * @returns True if the click was executed.
 */
async function scrollAndClick(page: Page, target: ClickTarget): Promise<boolean> {

  // Brief delay after scrolling for any animations or lazy-loaded content to settle.
  await delay(200);

  // Click the target coordinates to switch to the channel.
  await page.mouse.click(target.x, target.y);

  return true;
}

/* Each strategy implements a different approach to finding and selecting channels. Strategies are self-contained functions that can be tested independently.
 */

/**
 * Thumbnail row strategy: finds a channel by matching the slug in thumbnail image URLs, then clicks an adjacent clickable element on the same row. This strategy
 * works for sites like USA Network where channels are displayed as rows with a thumbnail on the left and program entries to the right.
 *
 * The selection process:
 * 1. Search all images on the page for one whose src URL contains the channel slug
 * 2. Verify the image has dimensions (is rendered and visible)
 * 3. Walk up the DOM to find a container wide enough to hold both thumbnail and guide entries
 * 4. Search for clickable elements (links, buttons, cards) to the right of the thumbnail on the same row
 * 5. Fall back to divs with cursor:pointer if no semantic clickables found
 * 6. Click the found element to switch to the channel
 * @param page - The Puppeteer page object.
 * @param channelSlug - The literal string to match in thumbnail image URLs.
 * @returns Result object with success status and optional failure reason.
 */
async function thumbnailRowStrategy(page: Page, channelSlug: string): Promise<ChannelSelectorResult> {

  // Find clickable element by evaluating DOM. The logic walks through the page looking for channel thumbnail images, then finds clickable show entries on the
  // same row.
  const clickTarget = await evaluateWithAbort(page, (slug: string): ClickTarget | null => {

    const images = document.querySelectorAll("img");

    for(const img of Array.from(images)) {

      // Channel thumbnails have URLs containing the channel slug pattern. Match against the src URL.
      if(img.src && img.src.includes(slug)) {

        const imgRect = img.getBoundingClientRect();

        // Verify the image has dimensions (is actually rendered and visible).
        if((imgRect.width > 0) && (imgRect.height > 0)) {

          // Found the channel thumbnail. Now walk up the DOM tree to find a container that holds both the thumbnail and the guide entries for this row.
          let rowContainer: HTMLElement | null = img.parentElement;

          while(rowContainer && (rowContainer !== document.body)) {

            const containerRect = rowContainer.getBoundingClientRect();

            // Look for a container significantly wider than the thumbnail (indicating it contains more than just the image). The factor of 2 is a heuristic
            // that works for typical channel guide layouts.
            if(containerRect.width > (imgRect.width * 2)) {

              // This container is wide enough to contain guide entries. Search for clickable elements (show cards) to the right of the thumbnail.
              const clickables = rowContainer.querySelectorAll(
                "a, button, [role=\"button\"], [onclick], [class*=\"card\"], [class*=\"program\"], [class*=\"show\"], [class*=\"episode\"]"
              );

              const imgCenterY = imgRect.y + (imgRect.height / 2);

              for(const clickable of Array.from(clickables)) {

                const clickRect = clickable.getBoundingClientRect();
                const clickCenterY = clickRect.y + (clickRect.height / 2);

                // The guide entry must meet these criteria:
                // - To the right of the thumbnail (with small tolerance for overlapping borders)
                // - Has dimensions (is visible)
                // - On the same row (vertical center within thumbnail height)
                const isRightOfThumbnail = clickRect.x > (imgRect.x + imgRect.width - 10);
                const hasDimensions = (clickRect.width > 0) && (clickRect.height > 0);
                const isSameRow = Math.abs(clickCenterY - imgCenterY) < imgRect.height;

                if(isRightOfThumbnail && hasDimensions && isSameRow) {

                  // Found a suitable click target. Scroll it into view and return its center coordinates.
                  (clickable as HTMLElement).scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

                  const newRect = clickable.getBoundingClientRect();

                  return { x: newRect.x + (newRect.width / 2), y: newRect.y + (newRect.height / 2) };
                }
              }

              // Fallback: if no semantically clickable elements found, look for divs with cursor: pointer styling. These are often custom-styled click
              // handlers.
              const allDivs = rowContainer.querySelectorAll("div");

              for(const div of Array.from(allDivs)) {

                const divRect = div.getBoundingClientRect();
                const divCenterY = divRect.y + (divRect.height / 2);
                const style = window.getComputedStyle(div);

                const isRightOfThumbnail = divRect.x > (imgRect.x + imgRect.width - 10);
                const hasDimensions = (divRect.width > 20) && (divRect.height > 20);
                const isClickable = style.cursor === "pointer";
                const isSameRow = Math.abs(divCenterY - imgCenterY) < imgRect.height;

                if(isRightOfThumbnail && hasDimensions && isClickable && isSameRow) {

                  div.scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

                  const newRect = div.getBoundingClientRect();

                  return { x: newRect.x + (newRect.width / 2), y: newRect.y + (newRect.height / 2) };
                }
              }
            }

            rowContainer = rowContainer.parentElement;
          }

          // Ultimate fallback: click a fixed offset to the right of the thumbnail. This is a last resort if the guide structure doesn't match our
          // expectations.
          img.scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

          const newImgRect = img.getBoundingClientRect();

          return { x: newImgRect.x + newImgRect.width + 50, y: newImgRect.y + (newImgRect.height / 2) };
        }
      }
    }

    // Channel thumbnail not found in any images.
    return null;
  }, [channelSlug]);

  if(clickTarget) {

    await scrollAndClick(page, clickTarget);

    // Poll for the video readyState to drop below 3, indicating the channel switch has started loading new content. This replaces a fixed post-click delay with
    // early exit. If no video exists yet or readyState never drops (channel already selected), the timeout expires harmlessly and waitForVideoReady() handles the
    // rest.
    try {

      await page.waitForFunction(
        (): boolean => {

          const v = document.querySelector("video");

          return !v || (v.readyState < 3);
        },
        { timeout: CONFIG.playback.channelSwitchDelay }
      );
    } catch {

      // Timeout — readyState never dropped. Proceed normally.
    }

    return { success: true };
  }

  return { reason: "Channel thumbnail not found in page images.", success: false };
}

/**
 * Tile click strategy: finds a channel by matching the slug in tile image URLs, clicks the tile to open an entity modal, then clicks a "watch live" play button on
 * the modal. This strategy works for sites like Disney+ where live channels are displayed as tiles in a horizontal shelf, and selecting one opens a modal with a
 * play button to start the live stream.
 *
 * The selection process:
 * 1. Search all images on the page for one whose src URL contains the channel slug
 * 2. Walk up the DOM to find the nearest clickable ancestor (the tile container)
 * 3. Scroll the tile into view and click it
 * 4. Wait for the play button to appear on the resulting modal
 * 5. Click the play button to start live playback
 * @param page - The Puppeteer page object.
 * @param channelSlug - The literal string to match in tile image URLs.
 * @returns Result object with success status and optional failure reason.
 */
async function tileClickStrategy(page: Page, channelSlug: string): Promise<ChannelSelectorResult> {

  // Step 1: Find the channel tile by matching the slug in a descendant image's src URL. Live channels are displayed as tiles in a horizontal shelf, each containing
  // an image with the network name in the URL label parameter (e.g., "poster_linear_espn_none"). We match the image, then walk up the DOM to find the nearest
  // clickable ancestor that represents the entire tile.
  const tileTarget = await evaluateWithAbort(page, (slug: string): ClickTarget | null => {

    const images = document.querySelectorAll("img");

    for(const img of Array.from(images)) {

      if(img.src && img.src.includes(slug)) {

        const imgRect = img.getBoundingClientRect();

        // Verify the image has dimensions (is actually rendered and visible). This matches the pattern in thumbnailRowStrategy and provides defense-in-depth if the
        // wait phase timed out before the image fully loaded.
        if((imgRect.width > 0) && (imgRect.height > 0)) {

          // Walk up the DOM to find the nearest clickable ancestor wrapping the tile. Check for semantic clickable elements (<a>, <button>, role="button") and
          // elements with explicit click handlers first. Track cursor:pointer elements as a fallback for sites using custom click handlers without semantic markup.
          let ancestor: HTMLElement | null = img.parentElement;
          let pointerFallback: HTMLElement | null = null;

          while(ancestor && (ancestor !== document.body)) {

            const tag = ancestor.tagName;

            // Semantic clickable elements are the most reliable indicators of an interactive tile container.
            if((tag === "A") || (tag === "BUTTON") || (ancestor.getAttribute("role") === "button") || ancestor.hasAttribute("onclick")) {

              ancestor.scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

              const rect = ancestor.getBoundingClientRect();

              if((rect.width > 0) && (rect.height > 0)) {

                return { x: rect.x + (rect.width / 2), y: rect.y + (rect.height / 2) };
              }
            }

            // Track the nearest cursor:pointer ancestor with reasonable dimensions as a fallback.
            if(!pointerFallback) {

              const rect = ancestor.getBoundingClientRect();

              if((rect.width > 20) && (rect.height > 20) && (window.getComputedStyle(ancestor).cursor === "pointer")) {

                pointerFallback = ancestor;
              }
            }

            ancestor = ancestor.parentElement;
          }

          // Fallback: use cursor:pointer ancestor if no semantic clickable was found above.
          if(pointerFallback) {

            pointerFallback.scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

            const rect = pointerFallback.getBoundingClientRect();

            if((rect.width > 0) && (rect.height > 0)) {

              return { x: rect.x + (rect.width / 2), y: rect.y + (rect.height / 2) };
            }
          }
        }
      }
    }

    return null;
  }, [channelSlug]);

  if(!tileTarget) {

    return { reason: "Channel tile not found in page images.", success: false };
  }

  // Click the channel tile to open the entity modal.
  await scrollAndClick(page, tileTarget);

  // Step 2: Wait for the "WATCH LIVE" button to appear on the entity modal. The button is an <a> element with a specific data-testid attribute. After clicking the
  // tile, the site performs a SPA navigation that renders a modal with playback options.
  const playButtonSelector = "[data-testid=\"live-modal-watch-live-action-button\"]";

  try {

    await page.waitForSelector(playButtonSelector, { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return { reason: "Play button did not appear after clicking channel tile.", success: false };
  }

  // Get the play button coordinates for clicking.
  const playTarget = await evaluateWithAbort(page, (selector: string): ClickTarget | null => {

    const button = document.querySelector(selector);

    if(!button) {

      return null;
    }

    (button as HTMLElement).scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

    const rect = button.getBoundingClientRect();

    if((rect.width > 0) && (rect.height > 0)) {

      return { x: rect.x + (rect.width / 2), y: rect.y + (rect.height / 2) };
    }

    return null;
  }, [playButtonSelector]);

  if(!playTarget) {

    return { reason: "Play button found but has no dimensions.", success: false };
  }

  // Click the play button to start live playback.
  await scrollAndClick(page, playTarget);

  return { success: true };
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
async function readRenderedChannels(page: Page): Promise<RenderedChannel[] | null> {

  const channels = await page.evaluate((): Array<{ name: string; rowNumber: number }> | null => {

    const containers = document.querySelectorAll("[data-testid^=\"live-guide-channel-kyber-\"]");

    if(containers.length === 0) {

      return null;
    }

    const prefix = "live-guide-channel-kyber-";
    const results: Array<{ name: string; rowNumber: number }> = [];

    for(const el of Array.from(containers)) {

      const testid = el.getAttribute("data-testid") ?? "";
      const name = testid.slice(prefix.length).trim().replace(/\s+/g, " ").toLowerCase();

      // Extract row number from sr-only text. Format: "{Name} Details, row {N} of {Total}. ..."
      let rowNumber = -1;
      const btn = el.querySelector("[data-testid=\"live-guide-channel-button\"]");

      if(btn) {

        const srOnly = btn.querySelector(".sr-only, [class*=\"sr-only\"]");

        if(srOnly) {

          const match = srOnly.textContent.match(/row (\d+) of/);

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
async function locateOnNowCell(page: Page, targetName: string): Promise<ClickTarget | null> {

  return evaluateWithAbort(page, (target: string): { x: number; y: number } | null => {

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

        const onNow = row.querySelector(".LiveGuideProgram--first") as HTMLElement | null;

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

// Normalizes a channel name for case-insensitive, whitespace-tolerant comparison. Trims leading and trailing whitespace, collapses internal whitespace sequences
// (including non-breaking spaces, tabs, and other Unicode whitespace matched by \s) into a single regular space, and lowercases. This handles data-testid values
// with trailing spaces (e.g., "WLS "), double spaces, or non-breaking space characters that would otherwise cause exact match failures.
function normalizeChannelName(name: string): string {

  return name.trim().replace(/\s+/g, " ").toLowerCase();
}

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
function inferLocalAffiliate(rendered: RenderedChannel[], targetName: string): string | null {

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
 * @param channelName - The channel name to match against data-testid attributes (case-insensitive).
 * @param channelSelection - The channel selection configuration containing strategy, listSelector, and playSelector.
 * @returns Result object with success status and optional failure reason.
 */
async function guideGridStrategy(page: Page, channelName: string, channelSelection: ChannelSelectionConfig): Promise<ChannelSelectorResult> {

  const { listSelector, playSelector } = channelSelection;

  // Ensure the guide is open and on the correct tab. We wait for the tab button to become VISIBLE (not just present in the DOM) because the guide overlay may exist
  // in the DOM structure while still hidden during page initialization or animation. Clicking a hidden button dispatches a DOM event but has no visual effect — the
  // guide remains hidden and the virtualizer never populates rows. We use $eval for the click because overlapping elements (spinners, overlays) can intercept
  // Puppeteer's coordinate-based mouse events.
  if(listSelector) {

    try {

      await page.waitForSelector(listSelector, { timeout: CONFIG.streaming.videoTimeout, visible: true });
      await page.$eval(listSelector, (el) => (el as HTMLElement).click());

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

        LOG.debug("Guide rows not visible after initial wait. Retrying tab click for %s.", listSelector);

        try {

          // eslint-disable-next-line no-await-in-loop
          await page.$eval(listSelector, (el) => (el as HTMLElement).click());

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
  const gridMeta = await page.evaluate((rowHeight: number): { gridDocTop: number; totalRows: number } | null => {

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

    LOG.debug("Guide cache hit for %s at row %s.", channelName, cachedRow);

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
    LOG.debug("Guide cache miss for %s. Falling back to binary search.", channelName);

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

      LOG.info("Inferred local affiliate %s for network name %s.", inferred, channelName);

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

    LOG.debug("Binary search did not find %s. Starting linear scan fallback.", channelName);

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

    return { reason: "Could not find channel " + channelName + " in guide grid.", success: false };
  }

  // Click the on-now program cell and wait for the play button, with click retries to handle React hydration timing.
  return await clickOnNowCellAndPlay(page, clickTarget, playSelector, channelName);
}

/**
 * Waits for the play button to appear and clicks it using coordinate-based mouse events. Called by clickOnNowCellAndPlay after each on-now cell click attempt.
 * @param page - The Puppeteer page object.
 * @param playSelector - The CSS selector for the play button, or undefined if no play button is needed.
 * @param timeout - Optional timeout in milliseconds for the play button to appear. Defaults to CONFIG.streaming.videoTimeout.
 * @returns Result object with success status and optional failure reason.
 */
async function waitForPlayButton(page: Page, playSelector: string | undefined, timeout?: number): Promise<ChannelSelectorResult> {

  if(!playSelector) {

    return { success: true };
  }

  try {

    await page.waitForSelector(playSelector, { timeout: timeout ?? CONFIG.streaming.videoTimeout, visible: true });

    // Wait two animation frames for React to flush pending state updates. The play button may be visible in the DOM before React's concurrent mode has committed
    // the channel selection state to the component's event handlers. Without this, clicking immediately can trigger playback of the previously-selected channel
    // rather than the one we just chose. The double-rAF pattern synchronizes with the browser's rendering pipeline rather than using a fixed delay.
    await page.evaluate(async () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))));

    // Get the play button's coordinates for a real mouse click. Like the on-now cell click, we use page.mouse.click() to generate the full pointer event chain
    // rather than a bare DOM .click().
    const playTarget = await evaluateWithAbort(page, (selector: string): { x: number; y: number } | null => {

      const el = document.querySelector(selector) as HTMLElement | null;

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
 * Clicks the on-now program cell and waits for the play button, retrying the click if the play button doesn't appear. This handles a timing issue where the
 * guide grid's DOM elements render (so binary search finds the channel) before React has fully hydrated the event handlers (so the first mouse click on the
 * on-now cell may not trigger the playback overlay). Retrying the click after a brief delay allows hydration to complete.
 * @param page - The Puppeteer page object.
 * @param clickTarget - The lowercased, trimmed channel name to locate in the guide grid.
 * @param playSelector - The CSS selector for the play button, or undefined if no play button is needed.
 * @param channelName - The original channel name for logging.
 * @returns Result object with success status and optional failure reason.
 */
async function clickOnNowCellAndPlay(page: Page, clickTarget: string, playSelector: string | undefined, channelName: string): Promise<ChannelSelectorResult> {

  // Maximum number of on-now cell click attempts. The first click may not register if React hasn't finished hydrating the guide's event handlers.
  const MAX_CLICK_ATTEMPTS = 3;

  // Shorter timeout for the play button on non-final attempts. Long enough for the overlay to appear if the click registered, but short enough to retry quickly
  // rather than waiting the full videoTimeout on each attempt.
  const RETRY_PLAY_TIMEOUT = 3000;

  // Delay between click retries. Gives React additional time to finish hydrating event handlers.
  const CLICK_RETRY_DELAY = 1500;

  for(let attempt = 0; attempt < MAX_CLICK_ATTEMPTS; attempt++) {

    // eslint-disable-next-line no-await-in-loop
    const onNowTarget = await locateOnNowCell(page, clickTarget);

    if(!onNowTarget) {

      return { reason: "Found channel " + channelName + " but could not locate on-now program cell.", success: false };
    }

    // eslint-disable-next-line no-await-in-loop
    await scrollAndClick(page, onNowTarget);

    // Use a shorter timeout on non-final attempts to enable quick retries. The final attempt uses the full default timeout as a last chance.
    const playTimeout = (attempt < MAX_CLICK_ATTEMPTS - 1) ? RETRY_PLAY_TIMEOUT : undefined;

    // eslint-disable-next-line no-await-in-loop
    const result = await waitForPlayButton(page, playSelector, playTimeout);

    if(result.success) {

      return result;
    }

    // Play button didn't appear — the click likely didn't register. Retry after a brief delay.
    if(attempt < MAX_CLICK_ATTEMPTS - 1) {

      LOG.debug("Play button did not appear for %s. Retrying on-now cell click (attempt %s of %s).", channelName, attempt + 2, MAX_CLICK_ATTEMPTS);

      // eslint-disable-next-line no-await-in-loop
      await delay(CLICK_RETRY_DELAY);
    }
  }

  return { reason: "Play button did not appear after " + MAX_CLICK_ATTEMPTS + " on-now cell click attempts for " + channelName + ".", success: false };
}

/* The selectChannel() function is the public API for channel selection. It delegates to the appropriate strategy based on the profile configuration.
 */

/**
 * Selects a channel from a multi-channel player UI using the strategy specified in the profile. This is the main entry point for channel selection, called by
 * tuneToChannel() after page navigation.
 *
 * The function handles:
 * - Polling for channel slug image readiness before strategy dispatch
 * - Strategy dispatch based on profile.channelSelection.strategy
 * - No-op for single-channel sites (strategy "none" or no channelSelector)
 * - Logging of selection attempts and results
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile containing channelSelection config and channelSelector slug.
 * @returns Result object with success status and optional failure reason.
 */
export async function selectChannel(page: Page, profile: ResolvedSiteProfile): Promise<ChannelSelectorResult> {

  const { channelSelection, channelSelector } = profile;

  // No channel selection needed if strategy is "none" or no channelSelector is specified.
  if((channelSelection.strategy === "none") || !channelSelector) {

    return { success: true };
  }

  // Poll for the channel slug image to appear and fully load. We check both src match and load completion (img.complete + naturalWidth) to ensure the image is
  // actually rendered before proceeding. This prevents race conditions where the img element exists with the correct src but the browser hasn't finished fetching
  // and rendering it, which can cause layout instability and click failures. We skip this polling for guideGrid because the channel list images are hidden behind a
  // tab that the strategy clicks — polling here would always time out, wasting channelSelectorDelay before the strategy even begins.
  if(channelSelection.strategy !== "guideGrid") {

    try {

      await page.waitForFunction(
        (slug: string): boolean => {

          return Array.from(document.querySelectorAll("img")).some((img) => img.src && img.src.includes(slug) && img.complete && (img.naturalWidth > 0));
        },
        { timeout: CONFIG.playback.channelSelectorDelay },
        channelSelector
      );
    } catch {

      // Timeout — the image hasn't loaded yet. Proceed anyway and let the strategy evaluate and report not-found naturally.
    }
  }

  // Dispatch to the appropriate strategy.
  let result: ChannelSelectorResult;

  switch(channelSelection.strategy) {

    case "guideGrid": {

      result = await guideGridStrategy(page, channelSelector, channelSelection);

      break;
    }

    case "thumbnailRow": {

      result = await thumbnailRowStrategy(page, channelSelector);

      break;
    }

    case "tileClick": {

      result = await tileClickStrategy(page, channelSelector);

      break;
    }

    default: {

      // Unknown strategy - this shouldn't happen if profiles are validated, but handle gracefully.
      LOG.warn("Unknown channel selection strategy: %s.", channelSelection.strategy);

      return { reason: "Unknown channel selection strategy.", success: false };
    }
  }

  if(!result.success) {

    LOG.warn("Failed to select %s from channel guide: %s", channelSelector, result.reason ?? "Unknown reason.");
  }

  return result;
}
