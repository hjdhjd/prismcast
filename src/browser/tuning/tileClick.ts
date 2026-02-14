/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tileClick.ts: Tile click channel selection strategy (Disney+ live channels).
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ClickTarget, Nullable } from "../../types/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { evaluateWithAbort } from "../../utils/index.js";
import { scrollAndClick } from "../channelSelection.js";

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
 * @param profile - The resolved site profile with a non-null channelSelector (image URL slug).
 * @returns Result object with success status and optional failure reason.
 */
export async function tileClickStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelSlug = profile.channelSelector;

  // Step 1: Find the channel tile by matching the slug in a descendant image's src URL. Live channels are displayed as tiles in a horizontal shelf, each containing
  // an image with the network name in the URL label parameter (e.g., "poster_linear_espn_none"). We match the image, then walk up the DOM to find the nearest
  // clickable ancestor that represents the entire tile.
  const tileTarget = await evaluateWithAbort(page, (slug: string): Nullable<ClickTarget> => {

    const images = document.querySelectorAll("img");

    for(const img of Array.from(images)) {

      if(img.src.includes(slug)) {

        const imgRect = img.getBoundingClientRect();

        // Verify the image has dimensions (is actually rendered and visible). This matches the pattern in thumbnailRowStrategy and provides defense-in-depth if the
        // wait phase timed out before the image fully loaded.
        if((imgRect.width > 0) && (imgRect.height > 0)) {

          // Walk up the DOM to find the nearest clickable ancestor wrapping the tile. Check for semantic clickable elements (<a>, <button>, role="button") and
          // elements with explicit click handlers first. Track cursor:pointer elements as a fallback for sites using custom click handlers without semantic markup.
          let ancestor: Nullable<HTMLElement> = img.parentElement;
          let pointerFallback: Nullable<HTMLElement> = null;

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
  const playTarget = await evaluateWithAbort(page, (selector: string): Nullable<ClickTarget> => {

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
