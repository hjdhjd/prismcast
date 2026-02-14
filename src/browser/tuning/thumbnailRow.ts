/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * thumbnailRow.ts: Thumbnail row channel selection strategy.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ClickTarget, Nullable } from "../../types/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { evaluateWithAbort } from "../../utils/index.js";
import { scrollAndClick } from "../channelSelection.js";

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
 * @param profile - The resolved site profile with a non-null channelSelector (image URL slug).
 * @returns Result object with success status and optional failure reason.
 */
export async function thumbnailRowStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelSlug = profile.channelSelector;

  // Find clickable element by evaluating DOM. The logic walks through the page looking for channel thumbnail images, then finds clickable show entries on the
  // same row.
  const clickTarget = await evaluateWithAbort(page, (slug: string): Nullable<ClickTarget> => {

    const images = document.querySelectorAll("img");

    for(const img of Array.from(images)) {

      // Channel thumbnails have URLs containing the channel slug pattern. Match against the src URL.
      if(img.src.includes(slug)) {

        const imgRect = img.getBoundingClientRect();

        // Verify the image has dimensions (is actually rendered and visible).
        if((imgRect.width > 0) && (imgRect.height > 0)) {

          // Found the channel thumbnail. Now walk up the DOM tree to find a container that holds both the thumbnail and the guide entries for this row.
          let rowContainer: Nullable<HTMLElement> = img.parentElement;

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
