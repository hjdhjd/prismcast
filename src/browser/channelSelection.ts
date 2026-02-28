/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelSelection.ts: Channel selection coordinator for multi-channel streaming sites.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ChannelStrategyEntry, ClickTarget, Nullable, ProviderModule, ResolvedSiteProfile } from "../types/index.js";
import { LOG, delay, evaluateWithAbort } from "../utils/index.js";
import { CHANNELS } from "../channels/index.js";
import { CONFIG } from "../config/index.js";
import type { Page } from "puppeteer-core";
import { directvProvider } from "./tuning/directv.js";
import { foxProvider } from "./tuning/fox.js";
import { hboProvider } from "./tuning/hbo.js";
import { huluProvider } from "./tuning/hulu.js";
import { isChannelSelectionProfile } from "../types/index.js";
import { slingProvider } from "./tuning/sling.js";
import { thumbnailRowStrategy } from "./tuning/thumbnailRow.js";
import { tileClickStrategy } from "./tuning/tileClick.js";
import { yttvProvider } from "./tuning/youtubeTv.js";

/* Multi-channel streaming sites (like USA Network) present multiple channels on a single page, with a program guide for each channel. Users must select which
 * channel they want to watch by clicking on a show in the guide. This module coordinates the dispatch to per-provider strategy functions in the tuning/ directory.
 *
 * Each provider tuning file exports a single ProviderModule object that bundles identity metadata (slug, label, guideUrl), the tuning strategy, and a
 * discoverChannels implementation. The coordinator builds its strategy dispatch lookup from provider modules at evaluation time. Generic strategies
 * (thumbnailRow, tileClick) remain bare ChannelStrategyEntry objects — they are site-specific interaction patterns, not provider-level registrations.
 *
 * Strategy files may import scrollAndClick(), normalizeChannelName(), and resolveMatchSelector() from this coordinator — the circular import is safe because
 * all cross-module calls happen inside async functions long after module evaluation completes.
 */

/* Adding a new channel selection provider:
 *
 * 1. Create a new file in tuning/ implementing the strategy function with the ChannelStrategyHandler signature.
 * 2. Export a single ProviderModule object from the file. Set the required fields:
 *    - slug, label, guideUrl: Identity metadata for API endpoints and logging.
 *    - strategyName: The ChannelSelectionStrategy union value that site profiles reference.
 *    - strategy: A ChannelStrategyEntry with at minimum an execute hook. Also set clearCache, resolveDirectUrl, and invalidateDirectUrl as needed.
 *    - discoverChannels: Reads the provider's guide for all available channels, returning DiscoveredChannel[].
 * 3. Import the provider here and add it to the providerModules array.
 * 4. Add the strategy name to the ChannelSelectionStrategy union type in types/index.ts.
 * 5. Add a site profile entry in config/sites.ts that references the new strategy name.
 *
 * The coordinator handles all cross-cutting concerns (dispatch, cache clearing, direct URL resolution, matchSelector polling) through the ChannelStrategyEntry
 * interface. Strategy files may import scrollAndClick(), normalizeChannelName(), resolveMatchSelector(), and logAvailableChannels() from this module for shared
 * utilities.
 */

// Provider module registry. The primary registry for all provider-level operations. Each entry bundles identity metadata, tuning strategy, and channel discovery.
// Future capabilities become additional methods on ProviderModule — no new registries needed.
const providerModules: readonly ProviderModule[] = [ directvProvider, foxProvider, hboProvider, huluProvider, slingProvider, yttvProvider ];

// Strategy dispatch registry. Derived from provider modules (keyed by strategyName) plus generic strategies that are not provider-level registrations.
const strategies: Record<string, ChannelStrategyEntry> = Object.fromEntries([
  ...providerModules.map((p) => [ p.strategyName, p.strategy ]),
  [ "thumbnailRow", thumbnailRowStrategy ],
  [ "tileClick", tileClickStrategy ]
]) as Record<string, ChannelStrategyEntry>;

/**
 * Returns a direct watch URL for the channel specified in the profile, if one can be resolved. Looks up the strategy entry's resolveDirectUrl hook and calls it
 * with the channelSelector and page. Returns null if the strategy has no resolver, the profile has no channelSelector, or the resolver returns null.
 * @param profile - The resolved site profile.
 * @param page - The Puppeteer page object, passed through to the strategy's resolver for response interception setup or API calls.
 * @returns The direct watch URL or null.
 */
export async function resolveDirectUrl(profile: ResolvedSiteProfile, page: Page): Promise<Nullable<string>> {

  const { channelSelection, channelSelector } = profile;

  if(!channelSelector) {

    return null;
  }

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  return await strategies[channelSelection.strategy]?.resolveDirectUrl?.(channelSelector, page) ?? null;
}

/**
 * Invalidates the cached direct watch URL for the channel specified in the profile. Looks up the strategy entry's invalidateDirectUrl hook and calls it with
 * the channelSelector. No-op if the strategy has no invalidator or the profile has no channelSelector.
 * @param profile - The resolved site profile.
 */
export function invalidateDirectUrl(profile: ResolvedSiteProfile): void {

  const { channelSelection, channelSelector } = profile;

  if(!channelSelector) {

    return;
  }

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  strategies[channelSelection.strategy]?.invalidateDirectUrl?.(channelSelector);
}

/**
 * Clears all channel selection caches. Called by handleBrowserDisconnect() in browser/index.ts when the browser restarts, since cached state (guide row positions,
 * discovered page URLs, watch URLs) may be stale in a new browser session.
 */
export function clearChannelSelectionCaches(): void {

  for(const entry of Object.values(strategies)) {

    entry.clearCache?.();
  }
}

/**
 * Looks up a provider module by its URL slug. Returns undefined if no provider matches.
 * @param slug - The provider slug (e.g., "yttv", "hulu", "sling").
 * @returns The matching provider module or undefined.
 */
export function getProviderBySlug(slug: string): ProviderModule | undefined {

  return providerModules.find((p) => p.slug === slug);
}

/**
 * Returns all registered provider module slugs. Used for validation in the checkboxList setting for precache providers.
 * @returns Array of provider slugs.
 */
export function getProviderSlugs(): string[] {

  return providerModules.map((p) => p.slug);
}

/**
 * Returns slug and label pairs for all registered provider modules. Used by the checkboxList setting to render precache provider checkbox labels.
 * @returns Array of objects with label and slug properties.
 */
export function getProviderModuleInfo(): { label: string; slug: string }[] {

  return providerModules.map((p) => ({ label: p.label, slug: p.slug }));
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
// with trailing spaces, double spaces, or non-breaking space characters that would otherwise cause exact match failures.
// Exported for use by tuning strategy files (hulu, sling).
export function normalizeChannelName(name: string): string {

  return name.trim().replace(/\s+/g, " ").toLowerCase();
}

// Resolves the CSS selector for finding a channel element. Interpolates the {channel} placeholder in the profile's matchSelector template with the channelSelector
// value. When matchSelector is not configured, falls back to image URL matching for backward compatibility.
// Exported for use by tuning strategy files (tileClick, thumbnailRow).
export function resolveMatchSelector(profile: ChannelSelectionProfile): string {

  const template = profile.channelSelection.matchSelector;

  if(template) {

    return template.replaceAll("{channel}", profile.channelSelector);
  }

  // Default to image URL slug matching for backward compatibility with profiles that don't specify matchSelector.
  return "img[src*=\"" + profile.channelSelector + "\"]";
}

/**
 * Logs available channel names from a provider's guide grid when channel selection fails. Produces an actionable log message listing channel names that users can
 * use as `channelSelector` values in user-defined channels. When `presetSuffix` is provided, channels already covered by built-in preset definitions are filtered
 * out so users see only channels that require manual configuration. When omitted (small channel sets like Fox or HBO), all channels are logged unfiltered.
 * @param options - Diagnostic dump configuration.
 * @param options.additionalKnownNames - Extra names to exclude from the filtered list (e.g., CHANNEL_ALTERNATES values for YTTV).
 * @param options.availableChannels - Sorted list of channel names discovered in the guide grid.
 * @param options.channelName - The channelSelector value that failed to match, for the log message.
 * @param options.guideUrl - The URL of the provider's guide page, included in the log message so users know what to set as the channel URL.
 * @param options.presetSuffix - Key suffix to filter preset channels (e.g., "-yttv", "-hulu"). Omit for small unfiltered channel sets.
 * @param options.providerName - Human-readable provider name for the log message (e.g., "YouTube TV", "Hulu").
 */
export function logAvailableChannels(options: {
  additionalKnownNames?: string[];
  availableChannels: string[];
  channelName: string;
  guideUrl: string;
  presetSuffix?: string;
  providerName: string;
}): void {

  const { additionalKnownNames, availableChannels, channelName, guideUrl, presetSuffix, providerName } = options;

  if(availableChannels.length === 0) {

    return;
  }

  let filteredChannels: string[];
  let countLabel: string;

  if(presetSuffix) {

    // Collect all channelSelector values from preset channels with this suffix, lowercased for case-insensitive comparison.
    const knownSelectors: string[] = Object.entries(CHANNELS)
      .filter(([key]) => key.endsWith(presetSuffix))
      .map(([ , ch ]) => (ch.channelSelector ?? "").toLowerCase())
      .filter((s) => s.length > 0);

    // Include additional known names (e.g., CHANNEL_ALTERNATES values for YTTV) so those are also filtered out.
    if(additionalKnownNames) {

      for(const name of additionalKnownNames) {

        knownSelectors.push(name.toLowerCase());
      }
    }

    // Filter to channels not matched by any known selector. A channel is "covered" if a preset would find it via exact match (with parenthetical suffix stripped)
    // or prefix+digit match. This mirrors the strategy's own matching tiers so users see only channels that genuinely need manual configuration.
    filteredChannels = availableChannels.filter((name) => {

      const lower = name.toLowerCase();
      const stripped = lower.replace(/ \(.*\)$/, "");

      return !knownSelectors.some((sel) => {

        return (stripped === sel) ||
          (lower.startsWith(sel + " ") && (lower.length > sel.length + 1) && (lower.charCodeAt(sel.length + 1) >= 48) && (lower.charCodeAt(sel.length + 1) <= 57));
      });
    });

    countLabel = "uncovered (" + String(filteredChannels.length) + " of " + String(availableChannels.length) + ")";
  } else {

    // No preset suffix — log all available channels unfiltered. Used for small channel sets (Fox, HBO) where the full list is actionable without filtering.
    filteredChannels = availableChannels;
    countLabel = String(filteredChannels.length);
  }

  if(filteredChannels.length === 0) {

    return;
  }

  LOG.warn("Channel \"%s\" not found in %s guide. Create a user-defined channel with one of the names below as the Channel Selector and %s as the URL. " +
    "Available channels (%s): %s.", channelName, providerName, guideUrl, countLabel, filteredChannels.join(", "));
}

/**
 * Selects a channel from a multi-channel player UI using the strategy specified in the profile. This is the main entry point for channel selection, called by
 * tuneToChannel() after page navigation.
 *
 * The function handles:
 * - Pre-selection scroll phase to force lazy-loaded content into the DOM (when scrollToBottom or scrollSelector+scrollTarget is set)
 * - Polling for channel element readiness before strategy dispatch (when profile.channelSelection.matchSelector is set)
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

  const entry = strategies[channelSelection.strategy];

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  if(!entry) {

    LOG.warn("Unknown channel selection strategy: %s.", channelSelection.strategy);

    return { reason: "Unknown channel selection strategy.", success: false };
  }

  // Pre-selection scroll phase. Some sites (e.g., Disney+) lazy-load entire page sections — headings, tiles, and images only appear in the DOM after scrolling
  // them into the viewport. Two scroll modes are supported: scrollToBottom scrolls the page to the bottom to force all lazy content into the DOM, and
  // scrollSelector+scrollTarget progressively scrolls until a specific element with matching text content is found and scrolled into view. Both modes gate on a
  // readiness signal before scrolling — scrollToBottom waits for the page to become scrollable (scrollHeight > innerHeight), while scrollSelector waits for the
  // first matching DOM element — since SPAs typically fire the load event before React/framework rendering completes.
  if(channelSelection.scrollToBottom) {

    // Wait for the SPA to render enough content to make the page scrollable. SPAs fire the load event before the framework renders page sections, so scrollHeight
    // equals innerHeight immediately after navigation. We poll until scrollHeight exceeds innerHeight, indicating content has been rendered and there is somewhere
    // to scroll.
    try {

      await page.waitForFunction((): boolean => document.body.scrollHeight > window.innerHeight, { timeout: CONFIG.streaming.videoTimeout });
    } catch {

      LOG.debug("tuning:tileClick", "Page did not become scrollable within %sms (scrollHeight: %s, innerHeight: %s). Proceeding anyway.",
        CONFIG.streaming.videoTimeout, await page.evaluate(() => document.body.scrollHeight), await page.evaluate(() => window.innerHeight));
    }

    // Press End to scroll to the bottom of the page, forcing lazy-loaded sections to render as they enter the viewport.
    await page.keyboard.press("End");

    LOG.debug("tuning:tileClick", "Pressed End to scroll to page bottom (scrollHeight: %s).",
      await page.evaluate(() => document.body.scrollHeight));
  } else if(channelSelection.scrollSelector && channelSelection.scrollTarget) {

    // Targeted scroll: find a specific element matching scrollSelector whose text content equals scrollTarget, then scroll it into view. This is used when only a
    // particular section needs to be visible rather than the entire page. Progressively scrolls in viewport-sized increments, checking after each step whether the
    // target element has appeared — necessary because sites with IntersectionObserver-based lazy loading only add sections to the DOM as they enter the viewport.
    let found = false;

    // Wait for at least one element matching the selector to appear so the SPA has started rendering content.
    try {

      await page.waitForSelector(channelSelection.scrollSelector, { timeout: CONFIG.streaming.videoTimeout });
    } catch {

      LOG.debug("tuning:tileClick", "No \"%s\" elements appeared within %sms. Page may not have rendered.",
        channelSelection.scrollSelector, CONFIG.streaming.videoTimeout);
    }

    // Set the scroll deadline after the readiness gate so the progressive scroll loop gets its own full time budget rather than sharing it with the waitForSelector.
    const scrollDeadline = Date.now() + CONFIG.streaming.videoTimeout;

    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- found is mutated inside the loop body.
    while(!found && (Date.now() < scrollDeadline)) {

      // eslint-disable-next-line no-await-in-loop
      found = await evaluateWithAbort(page, (selector: string, target: string): boolean => {

        for(const el of Array.from(document.querySelectorAll(selector))) {

          if(el.textContent.trim() === target) {

            (el as HTMLElement).scrollIntoView({ behavior: "instant", block: "center" });

            return true;
          }
        }

        return false;
      }, [ channelSelection.scrollSelector, channelSelection.scrollTarget ]);

      if(found) {

        break;
      }

      // Scroll down by one viewport height to trigger the next batch of lazy-loaded sections.
      // eslint-disable-next-line no-await-in-loop
      await page.evaluate(() => { window.scrollBy(0, window.innerHeight); });

      // eslint-disable-next-line no-await-in-loop
      await delay(300);
    }

    if(found) {

      LOG.debug("tuning:tileClick", "Scroll target \"%s\" via \"%s\": found and scrolled into view.",
        channelSelection.scrollTarget, channelSelection.scrollSelector);

      // Brief settle delay for lazy content near the target to finish rendering.
      await delay(500);
    } else {

      // Log what headings exist to help diagnose text mismatches.
      const headings = await evaluateWithAbort(page, (selector: string): string[] => {

        return Array.from(document.querySelectorAll(selector)).map((el) => el.textContent.trim());
      }, [channelSelection.scrollSelector]);

      LOG.debug("tuning:tileClick", "Scroll target \"%s\" via \"%s\": not found after %sms. Found headings: %s.",
        channelSelection.scrollTarget, channelSelection.scrollSelector, CONFIG.streaming.videoTimeout, JSON.stringify(headings));
    }
  }

  // Poll for the channel element to appear and become visible. Only run when matchSelector is explicitly configured — the default fallback in
  // resolveMatchSelector() is for strategy-internal use, and guide-based strategies that don't set matchSelector skip this wait entirely. For <img> elements, we
  // also verify load completion (img.complete + naturalWidth) to prevent race conditions where the element exists with the correct src but hasn't finished
  // rendering, which can cause layout instability and click failures.
  if(channelSelection.matchSelector) {

    const selector = resolveMatchSelector(profile);

    LOG.debug("tuning:tileClick", "Polling for matchSelector: %s (timeout: %sms).", selector, CONFIG.playback.channelSelectorDelay);

    try {

      await page.waitForFunction(
        (sel: string): boolean => {

          const el = document.querySelector(sel);

          if(!el) {

            return false;
          }

          const rect = el.getBoundingClientRect();

          if(!((rect.width > 0) && (rect.height > 0))) {

            return false;
          }

          // For <img> elements, also verify the image has fully loaded.
          if(el instanceof HTMLImageElement) {

            return el.complete && (el.naturalWidth > 0);
          }

          return true;
        },
        { timeout: CONFIG.playback.channelSelectorDelay },
        selector
      );

      LOG.debug("tuning:tileClick", "matchSelector poll succeeded: element found and visible.");
    } catch {

      // Timeout — the element hasn't appeared or loaded yet. Proceed anyway and let the strategy evaluate and report not-found naturally.
      LOG.debug("tuning:tileClick", "matchSelector poll timed out after %sms. Element not found or not visible.", CONFIG.playback.channelSelectorDelay);
    }
  }

  // Dispatch to the appropriate strategy via the registry.
  const result = await entry.execute(page, profile);

  return result;
}
