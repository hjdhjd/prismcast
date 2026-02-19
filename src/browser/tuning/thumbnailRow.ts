/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * thumbnailRow.ts: Thumbnail row channel selection strategy.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ChannelStrategyEntry, ClickTarget, Nullable } from "../../types/index.js";
import { resolveMatchSelector, scrollAndClick } from "../channelSelection.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { evaluateWithAbort } from "../../utils/index.js";

/**
 * Thumbnail row strategy: finds a channel element using the profile's matchSelector (defaults to image URL matching), then clicks an adjacent clickable element on
 * the same row. This strategy works for sites like USA Network where channels are displayed as rows with a thumbnail on the left and program entries to the right.
 *
 * The selection process:
 * 1. Find channel elements matching the resolved matchSelector CSS selector
 * 2. Verify the first visible match has dimensions (is rendered and visible)
 * 3. Walk up the DOM to find a container wide enough to hold both the channel element and guide entries
 * 4. Search for clickable elements (links, buttons, cards) to the right of the channel element on the same row
 * 5. Fall back to divs with cursor:pointer if no semantic clickables found
 * 6. Click the found element to switch to the channel
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector.
 * @returns Result object with success status and optional failure reason.
 */
async function thumbnailRowStrategyFn(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const selector = resolveMatchSelector(profile);

  // Find clickable element by evaluating DOM. The logic queries for elements matching the resolved matchSelector, then finds clickable show entries on the same
  // row.
  const clickTarget = await evaluateWithAbort(page, (sel: string): Nullable<ClickTarget> => {

    const elements = document.querySelectorAll(sel);

    for(const el of Array.from(elements)) {

      const htmlEl = el as HTMLElement;
      const elRect = htmlEl.getBoundingClientRect();

      // Verify the element has dimensions (is actually rendered and visible).
      if((elRect.width > 0) && (elRect.height > 0)) {

        // Found the channel element. Now walk up the DOM tree to find a container that holds both the element and the guide entries for this row.
        let rowContainer: Nullable<HTMLElement> = htmlEl.parentElement;

        while(rowContainer && (rowContainer !== document.body)) {

          const containerRect = rowContainer.getBoundingClientRect();

          // Look for a container significantly wider than the channel element (indicating it contains more than just the element). The factor of 2 is a
          // heuristic that works for typical channel guide layouts.
          if(containerRect.width > (elRect.width * 2)) {

            // This container is wide enough to contain guide entries. Search for clickable elements (show cards) to the right of the channel element.
            const clickables = rowContainer.querySelectorAll(
              "a, button, [role=\"button\"], [onclick], [class*=\"card\"], [class*=\"program\"], [class*=\"show\"], [class*=\"episode\"]"
            );

            const elCenterY = elRect.y + (elRect.height / 2);

            for(const clickable of Array.from(clickables)) {

              const clickRect = clickable.getBoundingClientRect();
              const clickCenterY = clickRect.y + (clickRect.height / 2);

              // The guide entry must meet these criteria:
              // - To the right of the channel element (with small tolerance for overlapping borders)
              // - Has dimensions (is visible)
              // - On the same row (vertical center within channel element height)
              const isRightOfElement = clickRect.x > (elRect.x + elRect.width - 10);
              const hasDimensions = (clickRect.width > 0) && (clickRect.height > 0);
              const isSameRow = Math.abs(clickCenterY - elCenterY) < elRect.height;

              if(isRightOfElement && hasDimensions && isSameRow) {

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

              const isRightOfElement = divRect.x > (elRect.x + elRect.width - 10);
              const hasDimensions = (divRect.width > 20) && (divRect.height > 20);
              const isClickable = style.cursor === "pointer";
              const isSameRow = Math.abs(divCenterY - elCenterY) < elRect.height;

              if(isRightOfElement && hasDimensions && isClickable && isSameRow) {

                div.scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

                const newRect = div.getBoundingClientRect();

                return { x: newRect.x + (newRect.width / 2), y: newRect.y + (newRect.height / 2) };
              }
            }
          }

          rowContainer = rowContainer.parentElement;
        }

        // Ultimate fallback: click a fixed offset to the right of the channel element. This is a last resort if the guide structure doesn't match our
        // expectations.
        htmlEl.scrollIntoView({ behavior: "instant", block: "center", inline: "center" });

        const newElRect = htmlEl.getBoundingClientRect();

        return { x: newElRect.x + newElRect.width + 50, y: newElRect.y + (newElRect.height / 2) };
      }
    }

    // Channel element not found.
    return null;
  }, [selector]);

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

  return { reason: "Channel element not found for selector.", success: false };
}

export const thumbnailRowStrategy: ChannelStrategyEntry = { execute: thumbnailRowStrategyFn };
