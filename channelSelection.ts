/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelSelection.ts: Channel selection strategies for multi-channel streaming sites.
 */
import type { ChannelSelectorResult, ClickTarget, ResolvedSiteProfile } from "../types/index.js";
import { LOG, delay, evaluateWithAbort } from "../utils/index.js";
import { CONFIG } from "../config/index.js";
import type { Page } from "puppeteer-core";

/*
 * CHANNEL SELECTION SYSTEM
 *
 * Multi-channel streaming sites (like USA Network) present multiple channels on a single page, with a program guide for each channel. Users must select which
 * channel they want to watch by clicking on a show in the guide. This module provides a strategy-based system for automating that channel selection.
 *
 * The strategy pattern allows different sites to have different selection mechanisms:
 * - thumbnailRow: Find channel by matching image URL slug, click adjacent show entry on the same row (USA Network)
 * - tileClick: Find channel tile by matching image URL slug, click tile, then click play button on modal (Disney+ live)
 *
 * Each strategy is a self-contained function that takes the page and channel slug, and returns a success/failure result. The main selectChannel() function
 * delegates to the appropriate strategy based on the profile configuration.
 */

/*
 * HELPER FUNCTIONS
 *
 * These utilities are shared across channel selection strategies. They handle common operations like finding elements, scrolling, and clicking.
 */

/**
 * Scrolls an element into view and clicks it at the specified coordinates. Includes delays to allow for animations and content loading.
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

/*
 * CHANNEL SELECTION STRATEGIES
 *
 * Each strategy implements a different approach to finding and selecting channels. Strategies are self-contained functions that can be tested independently.
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

/**
 * Tile click direct strategy: finds a channel by matching the slug in tile image URLs and clicks the tile to start playback immediately.
 * This strategy is identical to tileClickStrategy but skips waiting for and clicking the play button, as the site automatically begins
 * playback when the tile is clicked. Works for sites where channel tile selection directly initiates live streaming.
 *
 * The selection process:
 * 1. Search all images on the page for one whose src URL contains the channel slug
 * 2. Walk up the DOM to find the nearest clickable ancestor (the tile container)
 * 3. Scroll the tile into view and click it
 * 4. Return immediately - playback starts automatically
 * @param page - The Puppeteer page object.
 * @param channelSlug - The literal string to match in tile image URLs.
 * @returns Result object with success status and optional failure reason.
 */
async function tileClickDirectStrategy(page: Page, channelSlug: string): Promise<ChannelSelectorResult> {

  // Step 1: Find the channel tile by matching the slug in a descendant image's src URL. This logic is identical to tileClickStrategy.
  const tileTarget = await evaluateWithAbort(page, (slug: string): ClickTarget | null => {

    const images = document.querySelectorAll("img");

    for(const img of Array.from(images)) {

      if(img.src && img.src.includes(slug)) {

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

    return null;
  }, [channelSlug]);

  if(!tileTarget) {

    return { reason: "Channel tile not found in page images.", success: false };
  }

  // Click the channel tile to start playback immediately. Unlike tileClickStrategy, we don't wait for or click a play button.
  await scrollAndClick(page, tileTarget);

  return { success: true };
}

/*
 * MAIN ENTRY POINT
 *
 * The selectChannel() function is the public API for channel selection. It delegates to the appropriate strategy based on the profile configuration.
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

  // Poll for the channel slug image to appear in the DOM. This replaces a fixed delay with an early-exit poll — if the guide renders quickly, we proceed
  // immediately instead of waiting the full configured duration. The configured delay serves as the timeout ceiling.
  try {

    await page.waitForFunction(
      (slug: string): boolean => {

        return Array.from(document.querySelectorAll("img")).some((img) => img.src && img.src.includes(slug));
      },
      { timeout: CONFIG.playback.channelSelectorDelay },
      channelSelector
    );
  } catch {

    // Timeout — the image hasn't appeared yet. Proceed anyway and let the strategy evaluate and report not-found naturally.
  }

  // Dispatch to the appropriate strategy.
  let result: ChannelSelectorResult;

  switch(channelSelection.strategy) {

    case "thumbnailRow": {

      result = await thumbnailRowStrategy(page, channelSelector);

      break;
    }

    case "tileClick": {

      result = await tileClickStrategy(page, channelSelector);

      break;
    }

    case "tileClickDirect": {

      result = await tileClickDirectStrategy(page, channelSelector);

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
