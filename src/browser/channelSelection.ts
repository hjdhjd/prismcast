/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelSelection.ts: Channel selection coordinator for multi-channel streaming sites.
 */
import { type ChannelSelectionProfile, type ChannelSelectorResult, type ClickTarget, type ResolvedSiteProfile, isChannelSelectionProfile } from "../types/index.js";
import { LOG, delay } from "../utils/index.js";
import { clearHboCache, hboGridStrategy } from "./tuning/hbo.js";
import { clearHuluCache, guideGridStrategy } from "./tuning/hulu.js";
import { clearSlingCache, slingGridStrategy } from "./tuning/sling.js";
import { CONFIG } from "../config/index.js";
import type { Page } from "puppeteer-core";
import { thumbnailRowStrategy } from "./tuning/thumbnailRow.js";
import { tileClickStrategy } from "./tuning/tileClick.js";
import { youtubeGridStrategy } from "./tuning/youtubeTv.js";

/* Multi-channel streaming sites (like USA Network) present multiple channels on a single page, with a program guide for each channel. Users must select which
 * channel they want to watch by clicking on a show in the guide. This module coordinates the dispatch to per-provider strategy functions in the tuning/ directory.
 *
 * Each strategy is a self-contained file under tuning/ that implements the ChannelStrategyFn signature. The coordinator handles pre-dispatch concerns (image
 * polling, no-op checks) and post-dispatch logging. Strategy files may import scrollAndClick() and normalizeChannelName() from this coordinator — the circular
 * import is safe because all cross-module calls happen inside async functions long after module evaluation completes.
 */

// Strategy function signature. All strategies take the Puppeteer page and a narrowed profile with guaranteed non-null channelSelector.
type ChannelStrategyFn = (page: Page, profile: ChannelSelectionProfile) => Promise<ChannelSelectorResult>;

// Strategy dispatch registry. Maps strategy names from ChannelSelectionStrategy to their implementation functions.
const strategies: Record<string, ChannelStrategyFn> = {

  guideGrid: guideGridStrategy,
  hboGrid: hboGridStrategy,
  slingGrid: slingGridStrategy,
  thumbnailRow: thumbnailRowStrategy,
  tileClick: tileClickStrategy,
  youtubeGrid: youtubeGridStrategy
};

/**
 * Clears all channel selection caches. Called by handleBrowserDisconnect() in browser/index.ts when the browser restarts, since cached state (guide row positions,
 * discovered page URLs) may be stale in a new browser session.
 */
export function clearChannelSelectionCaches(): void {

  clearHboCache();
  clearHuluCache();
  clearSlingCache();
}

/**
 * Clicks at the specified coordinates after a brief settle delay. The delay allows scroll animations and lazy-loaded content to finish before the click fires.
 * Callers are responsible for scrolling the target element into view (typically via scrollIntoView inside a page.evaluate call) before invoking this function.
 * Exported for use by tuning strategy files (thumbnailRow, tileClick, hulu).
 * @param page - The Puppeteer page object.
 * @param target - The x/y coordinates to click.
 * @returns True if the click was executed.
 */
export async function scrollAndClick(page: Page, target: ClickTarget): Promise<boolean> {

  // Brief delay after scrolling for any animations or lazy-loaded content to settle.
  await delay(200);

  // Click the target coordinates to switch to the channel.
  await page.mouse.click(target.x, target.y);

  return true;
}

// Normalizes a channel name for case-insensitive, whitespace-tolerant comparison. Trims leading and trailing whitespace, collapses internal whitespace sequences
// (including non-breaking spaces, tabs, and other Unicode whitespace matched by \s) into a single regular space, and lowercases. This handles data-testid values
// with trailing spaces (e.g., "WLS "), double spaces, or non-breaking space characters that would otherwise cause exact match failures.
// Exported for use by tuning strategy files (hulu, sling).
export function normalizeChannelName(name: string): string {

  return name.trim().replace(/\s+/g, " ").toLowerCase();
}

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

  const { channelSelection } = profile;

  // No channel selection needed if strategy is "none" or no channelSelector is specified.
  if((channelSelection.strategy === "none") || !isChannelSelectionProfile(profile)) {

    return { success: true };
  }

  // Poll for the channel slug image to appear and fully load. We check both src match and load completion (img.complete + naturalWidth) to ensure the image is
  // actually rendered before proceeding. This prevents race conditions where the img element exists with the correct src but the browser hasn't finished fetching
  // and rendering it, which can cause layout instability and click failures. We skip this polling for guideGrid (channel list images are hidden behind a tab),
  // hboGrid (channelSelector is a channel name, not an image URL slug), slingGrid (same reason as hboGrid), and youtubeGrid (same reason as hboGrid).
  const skipImagePolling = [ "guideGrid", "hboGrid", "slingGrid", "youtubeGrid" ];

  if(!skipImagePolling.includes(channelSelection.strategy)) {

    try {

      await page.waitForFunction(
        (slug: string): boolean => {

          return Array.from(document.querySelectorAll("img")).some((img) => img.src && img.src.includes(slug) && img.complete && (img.naturalWidth > 0));
        },
        { timeout: CONFIG.playback.channelSelectorDelay },
        profile.channelSelector
      );
    } catch {

      // Timeout — the image hasn't loaded yet. Proceed anyway and let the strategy evaluate and report not-found naturally.
    }
  }

  // Dispatch to the appropriate strategy via the registry.
  const strategyFn = strategies[channelSelection.strategy];

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!strategyFn) {

    LOG.warn("Unknown channel selection strategy: %s.", channelSelection.strategy);

    return { reason: "Unknown channel selection strategy.", success: false };
  }

  const result = await strategyFn(page, profile);

  if(!result.success) {

    LOG.warn("Failed to select %s from channel guide: %s", profile.channelSelector, result.reason ?? "Unknown reason.");
  }

  return result;
}
