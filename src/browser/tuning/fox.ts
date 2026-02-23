/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fox.ts: Fox.com guide grid channel selection strategy.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, DiscoveredChannel, Nullable, ProviderModule } from "../../types/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { evaluateWithAbort } from "../../utils/index.js";
import { logAvailableChannels } from "../channelSelection.js";

// Raw channel info extracted from each GuideChannelContainer during discovery. The stationCode comes from the button title (e.g., "FOXD2C", "FNC"), the internalCode
// from the first data-content-impression-id prefix (e.g., the local affiliate call sign or internal station identifier), and locked from the presence of a lock-icon
// overlay on the first program thumbnail.
interface FoxChannelInfo {

  internalCode: string;
  locked: boolean;
  stationCode: string;
}

// Cached discovery results. Populated by the first discoverFoxChannels call (discovery endpoint). Fox tuning is stateless (no tuning cache exists), so only
// the discovery endpoint populates this cache. Cleared on browser disconnect via clearFoxCache().
let cachedDiscoveredChannels: Nullable<DiscoveredChannel[]> = null;

/**
 * Clears the Fox discovery cache. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts.
 */
function clearFoxCache(): void {

  cachedDiscoveredChannels = null;
}

/**
 * Fox.com grid strategy: finds a channel in the non-virtualized guide grid at fox.com/live/channels by matching the station code (button title attribute) on
 * GuideChannelLogo buttons, with fallback to internal station codes from data-content-impression-id attributes. All ~15 channels are present in the DOM
 * simultaneously once the grid renders dynamically (~4.5s after page load). Clicking the logo button is an SPA state transition — the Bitmovin player at the
 * top of the page switches channels without navigation, destroying and recreating its video element with a new blob src.
 *
 * The selection process:
 * 1. Poll via waitForFunction until the target appears as either a button title (e.g., FOXD2C, FNC) or an impression ID prefix (local affiliate call sign).
 * 2. Scan all channel rows, checking button title first, then impression ID prefix for fallback matching.
 * 3. Case-insensitive match against the channelSelector.
 * 4. On match, call logoButton.click() directly via DOM — coordinate-based clicking is not possible because the GuideProgramHero (sticky, z-40) overlays the
 *    guide grid and intercepts all mouse events at the element's coordinates.
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector (station code like "FOXD2C" or a local affiliate call sign).
 * @returns Result object with success status and optional failure reason.
 */
async function foxGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const stationCode = profile.channelSelector;

  // Wait for the target to appear in the guide grid. Checks both button title (primary) and data-content-impression-id prefix (fallback for call-sign-based
  // selectors that target a specific local affiliate by call sign).
  try {

    await page.waitForFunction(
      (code: string): boolean => {

        const codeLower = code.toLowerCase();
        const containers = document.querySelectorAll("[data-testid=\"GuideChannelContainer\"]");

        return Array.from(containers).some((c) => {

          const btn = c.querySelector("[data-testid=\"GuideChannelLogo\"] button");

          if((btn?.getAttribute("title") ?? "").toLowerCase() === codeLower) {

            return true;
          }

          // Fallback: check the first data-content-impression-id prefix. The format is "{PREFIX}-program-..." where PREFIX is the internal station code.
          const impressionDiv = c.querySelector("[data-content-impression-id]");
          const impressionId = impressionDiv?.getAttribute("data-content-impression-id") ?? "";
          const prefix = impressionId.split("-program-")[0].toLowerCase();

          return (prefix.length > 0) && (prefix === codeLower);
        });
      },
      { timeout: CONFIG.streaming.videoTimeout },
      stationCode
    );
  } catch {

    // Best-effort diagnostic: collect all available station codes and internal codes from the guide grid.
    try {

      const availableChannels = await evaluateWithAbort(page, (): string[] => {

        const codes = new Set<string>();

        for(const container of Array.from(document.querySelectorAll("[data-testid=\"GuideChannelContainer\"]"))) {

          const btn = container.querySelector("[data-testid=\"GuideChannelLogo\"] button");
          const title = (btn?.getAttribute("title") ?? "").trim();

          if(title.length > 0) {

            codes.add(title);
          }

          const impressionDiv = container.querySelector("[data-content-impression-id]");
          const impressionId = (impressionDiv?.getAttribute("data-content-impression-id") ?? "");
          const prefix = impressionId.split("-program-")[0].trim();

          if((prefix.length > 0) && (prefix !== title)) {

            codes.add(prefix);
          }
        }

        return Array.from(codes).sort();
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
  // element, bypassing the sticky hero's hit-testing.
  //
  // Matching order: button title first (handles "FOXD2C", "FNC", etc.), then impression ID prefix (handles local affiliate call signs for specific affiliate
  // selection).
  const clicked = await evaluateWithAbort(page, (code: string): boolean => {

    const codeLower = code.toLowerCase();
    const containers = document.querySelectorAll("[data-testid=\"GuideChannelContainer\"]");

    for(const container of Array.from(containers)) {

      const logoButton = container.querySelector("[data-testid=\"GuideChannelLogo\"] button") as Nullable<HTMLElement>;

      if(!logoButton) {

        continue;
      }

      // Primary match: button title (e.g., "FOXD2C", "FNC").
      if((logoButton.getAttribute("title") ?? "").toLowerCase() === codeLower) {

        logoButton.click();

        return true;
      }
    }

    // Fallback pass: match against impression ID prefix for call-sign-based selectors.
    for(const container of Array.from(containers)) {

      const logoButton = container.querySelector("[data-testid=\"GuideChannelLogo\"] button") as Nullable<HTMLElement>;

      if(!logoButton) {

        continue;
      }

      const impressionDiv = container.querySelector("[data-content-impression-id]");
      const impressionId = impressionDiv?.getAttribute("data-content-impression-id") ?? "";
      const prefix = impressionId.split("-program-")[0].toLowerCase();

      if((prefix.length > 0) && (prefix === codeLower)) {

        logoButton.click();

        return true;
      }
    }

    return false;
  }, [stationCode]);

  if(!clicked) {

    return { reason: "Station code " + stationCode + " not found in Fox.com guide grid.", success: false };
  }

  return { success: true };
}

/**
 * Reads all channel info from the Fox guide grid. For each GuideChannelContainer, extracts the display station code (button title), the internal station code
 * (from the first data-content-impression-id prefix), and whether the channel is locked (lock-icon present on the first program thumbnail).
 * @param page - The Puppeteer page object, expected to be on the Fox live channels page with at least one GuideChannelContainer rendered.
 * @returns Array of channel info objects in DOM order.
 */
async function readFoxChannels(page: Page): Promise<FoxChannelInfo[]> {

  return await evaluateWithAbort(page, (): FoxChannelInfo[] => {

    const results: FoxChannelInfo[] = [];

    for(const container of Array.from(document.querySelectorAll("[data-testid=\"GuideChannelContainer\"]"))) {

      const logoButton = container.querySelector("[data-testid=\"GuideChannelLogo\"] button");
      const stationCode = (logoButton?.getAttribute("title") ?? "").trim();

      if(stationCode.length === 0) {

        continue;
      }

      // Extract the internal station code from the first data-content-impression-id. Format: "{PREFIX}-program-..." where PREFIX is the internal code.
      const impressionDiv = container.querySelector("[data-content-impression-id]");
      const impressionId = (impressionDiv?.getAttribute("data-content-impression-id") ?? "");
      const internalCode = impressionId.split("-program-")[0].trim();

      // Locked channels have a lock-icon SVG overlaid on the first program thumbnail. These require TV provider authentication (add-on tier).
      const locked = container.querySelector("[data-testid=\"lock-icon\"]") !== null;

      results.push({ internalCode: internalCode.length > 0 ? internalCode : stationCode, locked, stationCode });
    }

    return results;
  }, []);
}

/**
 * Discovers all channels from the Fox guide grid. Waits for the first grid container to confirm the guide has rendered (the route handler's networkidle2
 * navigation ensures all API data has arrived before this function is called), then reads station codes, internal codes, and lock status from each
 * GuideChannelContainer. For FOXD2C entries (local affiliates), the channelSelector and affiliate are set to the internal call sign to enable precise affiliate
 * selection. For all other channels, the channelSelector and name are the display station code. Locked channels (requiring TV provider authentication) are
 * tagged with tier "addon".
 * @param page - The Puppeteer page object, expected to be on the Fox live channels page.
 * @returns Array of discovered channels with station codes, affiliate tagging, and tier information.
 */
async function discoverFoxChannels(page: Page): Promise<DiscoveredChannel[]> {

  if(cachedDiscoveredChannels) {

    return cachedDiscoveredChannels;
  }

  // Wait for at least one GuideChannelContainer to confirm the guide grid has rendered. The route handler navigates with networkidle2, which ensures all API data
  // has arrived before this function is called — no additional network idle wait is needed here.
  try {

    await page.waitForSelector("[data-testid=\"GuideChannelContainer\"]", { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return [];
  }

  const foxChannels = await readFoxChannels(page);

  // Do not cache empty results — leave null so subsequent calls retry the full walk. Empty results can indicate no TV provider login.
  if(foxChannels.length === 0) {

    return [];
  }

  cachedDiscoveredChannels = foxChannels.map((ch) => {

    const entry: DiscoveredChannel = { channelSelector: ch.stationCode, name: ch.stationCode };

    // FOXD2C entries are local affiliates distinguished by their internal call sign. Use the call sign as channelSelector for unambiguous
    // tuning and tag the affiliate field so the relationship is visible in discovery output.
    if(ch.stationCode === "FOXD2C") {

      entry.affiliate = ch.internalCode;
      entry.channelSelector = ch.internalCode;
    }

    if(ch.locked) {

      entry.tier = "addon";
    }

    return entry;
  });

  cachedDiscoveredChannels.sort((a, b) => a.name.localeCompare(b.name));

  return cachedDiscoveredChannels;
}

export const foxProvider: ProviderModule = {

  discoverChannels: discoverFoxChannels,
  getCachedChannels: (): Nullable<DiscoveredChannel[]> => cachedDiscoveredChannels,
  guideUrl: "https://www.fox.com/live/channels",
  label: "Fox",
  slug: "fox",
  strategy: { clearCache: clearFoxCache, execute: foxGridStrategy },
  strategyName: "foxGrid"
};
