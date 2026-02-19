/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tileClick.ts: Tile click channel selection strategy for multi-channel live TV sites.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ChannelStrategyEntry, ClickTarget, Nullable } from "../../types/index.js";
import { LOG, evaluateWithAbort } from "../../utils/index.js";
import { resolveMatchSelector, scrollAndClick } from "../channelSelection.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";

// Maximum number of play button click attempts before giving up. The first click sometimes misses due to coordinate shifts from SPA animations or overlay
// interference — retrying with fresh coordinates resolves most transient failures.
const MAX_PLAY_CLICK_ATTEMPTS = 3;

// Timeout in milliseconds to wait for the play button to disappear after clicking it. This is the verification signal that the SPA transitioned to the player view.
// Kept short so retries happen quickly rather than burning the full videoTimeout on a silent miss.
const PLAY_CLICK_VERIFY_TIMEOUT = 3000;

/**
 * Tile click strategy: finds a channel element using the profile's matchSelector (defaults to image URL matching), clicks the tile, and optionally clicks a play
 * button if the profile specifies one. This strategy works for multi-channel live TV sites that display channels as tiles in a horizontal shelf. When
 * `channelSelection.playSelector` is set (e.g., Disney+), clicking the tile opens a modal and the play button is clicked to start the stream. When `playSelector`
 * is absent, clicking the tile is the final action and the site auto-plays the selected channel.
 *
 * The selection process:
 * 1. Find channel elements matching the resolved matchSelector CSS selector
 * 2. Walk up the DOM from the first visible match to find the nearest clickable ancestor (the tile container)
 * 3. Scroll the tile into view and click it
 * 4. If `playSelector` is configured: wait for the play button, click it, and verify the modal dismissed — retrying up to 3 times on silent failures
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector.
 * @returns Result object with success status and optional failure reason.
 */
async function tileClickStrategyFn(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const selector = resolveMatchSelector(profile);

  // Step 1: Find the channel tile by querying for elements matching the resolved matchSelector. The selector is interpolated from the profile's matchSelector
  // template (or defaults to img[src*="channelSelector"]). We find the first visible match, then walk up the DOM to find the nearest clickable ancestor that
  // represents the entire tile.
  const tileTarget = await evaluateWithAbort(page, (sel: string): Nullable<ClickTarget> => {

    const elements = document.querySelectorAll(sel);

    for(const el of Array.from(elements)) {

      const htmlEl = el as HTMLElement;
      const elRect = htmlEl.getBoundingClientRect();

      // Verify the element has dimensions (is actually rendered and visible). This provides defense-in-depth if the coordinator's wait phase timed out before the
      // element fully loaded.
      if((elRect.width > 0) && (elRect.height > 0)) {

        // Walk up the DOM to find the nearest clickable ancestor wrapping the tile. Check for semantic clickable elements (<a>, <button>, role="button") and
        // elements with explicit click handlers first. Track cursor:pointer elements as a fallback for sites using custom click handlers without semantic markup.
        let ancestor: Nullable<HTMLElement> = htmlEl.parentElement;
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

    return null;
  }, [selector]);

  if(!tileTarget) {

    return { reason: "Channel element not found for selector.", success: false };
  }

  // Click the channel tile. For sites with a play button (playSelector configured), this opens a modal. For auto-play sites, this is the final action.
  await scrollAndClick(page, tileTarget);

  // Step 2 (conditional): If the profile specifies a play button selector, wait for it and click it. Sites like Disney+ show a modal with a "WATCH LIVE" button
  // after clicking the tile. Sites without playSelector auto-play on tile click and skip this phase entirely.
  const playButtonSelector = profile.channelSelection.playSelector;

  if(playButtonSelector) {

    try {

      await page.waitForSelector(playButtonSelector, { timeout: CONFIG.streaming.videoTimeout });
    } catch {

      return { reason: "Play button did not appear after clicking channel tile.", success: false };
    }

    // Click the play button with retry. Coordinate-based clicks can silently miss due to SPA animations, overlay interference, or stale coordinates. We verify each
    // click by checking whether the play button disappears (indicating the modal dismissed and the SPA transitioned to the player). On failure, we re-read
    // coordinates and retry — the same pattern used by Sling's clickWithRetry.
    for(let attempt = 0; attempt < MAX_PLAY_CLICK_ATTEMPTS; attempt++) {

      // Get the play button coordinates. Re-read on every attempt because SPA animations or layout shifts may have moved the button since the last read.
      // eslint-disable-next-line no-await-in-loop
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

        // Play button disappeared between attempts — the previous click likely worked and the SPA transitioned. This can happen when the verification timeout races
        // against a slow modal dismissal animation.
        if(attempt > 0) {

          LOG.debug("tuning:tileClick", "Play button disappeared before attempt %s. Previous click likely succeeded.", attempt + 1);

          return { success: true };
        }

        return { reason: "Play button found but has no dimensions.", success: false };
      }

      // Click the play button to start live playback.
      // eslint-disable-next-line no-await-in-loop
      await scrollAndClick(page, playTarget);

      // Verify the click worked by waiting for the play button to disappear. When the SPA transitions to the player view, the modal (and its play button) is removed
      // from the DOM. If the button is still present after the timeout, the click missed and we retry with fresh coordinates.
      try {

        // eslint-disable-next-line no-await-in-loop
        await page.waitForSelector(playButtonSelector, { hidden: true, timeout: PLAY_CLICK_VERIFY_TIMEOUT });

        return { success: true };
      } catch {

        // Play button still visible — the click had no effect. Log and retry.
        if(attempt < (MAX_PLAY_CLICK_ATTEMPTS - 1)) {

          LOG.info("Play button click attempt %s of %s did not dismiss the modal. Retrying with fresh coordinates.",
            attempt + 1, MAX_PLAY_CLICK_ATTEMPTS);
        }
      }
    }

    return { reason: "Play button click did not dismiss the modal after " + String(MAX_PLAY_CLICK_ATTEMPTS) + " attempts.", success: false };
  }

  return { success: true };
}

export const tileClickStrategy: ChannelStrategyEntry = { execute: tileClickStrategyFn };
