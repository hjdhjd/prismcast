/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fox.ts: Fox.com guide grid channel selection strategy.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, Nullable } from "../../types/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { evaluateWithAbort } from "../../utils/index.js";
import { logAvailableChannels } from "../channelSelection.js";

/**
 * Fox.com grid strategy: finds a channel in the non-virtualized guide grid at fox.com/live/channels by matching the station code (button title attribute) on
 * GuideChannelLogo buttons. All ~15 channels are present in the DOM simultaneously once the grid renders dynamically (~4.5s after page load). Clicking the
 * logo button is an SPA state transition — the Bitmovin player at the top of the page switches channels without navigation, destroying and recreating its
 * video element with a new blob src.
 *
 * The selection process:
 * 1. Poll via waitForFunction until the target station code's GuideChannelContainer appears (handles progressive grid rendering).
 * 2. Scan all channel rows, reading the button title attribute from each GuideChannelLogo element.
 * 3. Case-insensitive match against the channelSelector station code (e.g., FOXD2C, FNC, FS1).
 * 4. On match, call logoButton.click() directly via DOM — coordinate-based clicking is not possible because the GuideProgramHero (sticky, z-40) overlays the
 *    guide grid and intercepts all mouse events at the element's coordinates.
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector (station code, e.g., "FOXD2C", "FNC", "FS1").
 * @returns Result object with success status and optional failure reason.
 */
export async function foxGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const stationCode = profile.channelSelector;

  // Wait for the target station code to appear in the guide grid. The grid loads dynamically after the initial page load and renders progressively — the first
  // GuideChannelContainer may appear before all ~15 rows are in the DOM. We poll for the specific station code rather than waiting for any container, ensuring the
  // target channel's row is actually present before attempting the click.
  try {

    await page.waitForFunction(
      (code: string): boolean => {

        const containers = document.querySelectorAll("[data-testid=\"GuideChannelContainer\"]");

        return Array.from(containers).some((c) => {

          const btn = c.querySelector("[data-testid=\"GuideChannelLogo\"] button");

          return (btn?.getAttribute("title") ?? "").toLowerCase() === code.toLowerCase();
        });
      },
      { timeout: CONFIG.streaming.videoTimeout },
      stationCode
    );
  } catch {

    // Best-effort diagnostic: collect all available station codes from the guide grid so the user can see what values are valid for channelSelector.
    try {

      const availableChannels = await evaluateWithAbort(page, (): string[] => {

        return Array.from(document.querySelectorAll("[data-testid=\"GuideChannelLogo\"] button"))
          .map((btn) => btn.getAttribute("title") ?? "")
          .filter((t) => t.length > 0)
          .sort();
      }, []);

      if(availableChannels.length > 0) {

        logAvailableChannels({

          availableChannels,
          channelName: stationCode,
          guideUrl: "https://www.fox.com/live/channels",
          providerName: "Fox"
        });
      }
    } catch {

      // Diagnostic dump is best-effort.
    }

    return { reason: "Station code " + stationCode + " not found in Fox.com guide grid.", success: false };
  }

  // Click the channel logo button to tune the player. We use DOM element.click() rather than coordinate-based page.mouse.click() because the GuideProgramHero
  // section (sticky top-[64px], z-40) overlays the guide grid and intercepts all coordinate-based mouse events. DOM .click() dispatches the event directly to the
  // element, bypassing the sticky hero's hit-testing. The logo button click is the SPA state change that tunes the Bitmovin player — it destroys and recreates
  // its video element with a new blob src for the selected channel.
  const clicked = await evaluateWithAbort(page, (code: string): boolean => {

    const codeLower = code.toLowerCase();
    const containers = document.querySelectorAll("[data-testid=\"GuideChannelContainer\"]");

    for(const container of Array.from(containers)) {

      const logoButton = container.querySelector("[data-testid=\"GuideChannelLogo\"] button") as Nullable<HTMLElement>;

      if(!logoButton) {

        continue;
      }

      if((logoButton.getAttribute("title") ?? "").toLowerCase() !== codeLower) {

        continue;
      }

      logoButton.click();

      return true;
    }

    return false;
  }, [stationCode]);

  if(!clicked) {

    return { reason: "Station code " + stationCode + " not found in Fox.com guide grid.", success: false };
  }

  return { success: true };
}
