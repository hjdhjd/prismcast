/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * shared.ts: Shared utilities for channel selection tuning strategies.
 */
import type { ChannelSelectionProfile, ClickTarget } from "../../types/index.ts";
import { LOG, delay } from "../../utils/index.ts";
import { CHANNELS } from "../../channels/index.ts";
import type { Page } from "puppeteer-core";

/* These utility functions are used by multiple tuning strategy files (hulu, sling, fox, etc.) and the channel selection coordinator. They live in this shared module
 * to avoid circular imports - the coordinator imports tuning strategy modules, and those modules need these utilities. Placing the utilities in the coordinator would
 * create a circular dependency (coordinator -> tuning -> coordinator).
 */

/**
 * Scrolls a target element into view and clicks it using coordinate-based mouse click. The 200ms settle delay after scrolling allows lazy-loaded content and
 * animations to complete before the click is dispatched. Coordinate-based clicking generates the full pointer event chain (pointerdown -> mousedown -> pointerup ->
 * mouseup -> click), which is more reliable for React/SPA-based sites than synthetic DOM click events.
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

/**
 * Normalizes a channel name for case-insensitive, whitespace-tolerant comparison. Trims leading and trailing whitespace, collapses internal whitespace sequences
 * (including non-breaking spaces, tabs, and other Unicode whitespace matched by \s) into a single regular space, and lowercases. This handles data-testid values
 * with trailing spaces, double spaces, or non-breaking space characters that would otherwise cause exact match failures.
 * @param name - The raw channel name to normalize.
 * @returns The normalized channel name.
 */
export function normalizeChannelName(name: string): string {

  return name.trim().replace(/\s+/g, " ").toLowerCase();
}

/**
 * Resolves the CSS selector for finding a channel element. Interpolates the {channel} placeholder in the profile's matchSelector template with the channelSelector
 * value. When matchSelector is not configured, falls back to image URL matching for backward compatibility.
 * @param profile - The resolved profile with channel selection configuration.
 * @returns The CSS selector string.
 */
export function resolveMatchSelector(profile: ChannelSelectionProfile): string {

  const template = profile.channelSelection.matchSelector;

  if(template) {

    return template.replaceAll("{channel}", profile.channelSelector);
  }

  // Default to image URL slug matching for backward compatibility with profiles that don't specify matchSelector.
  return "img[src*=\"" + profile.channelSelector + "\" i]";
}

/**
 * Logs available channel names from a provider's guide grid when channel selection fails. Produces an actionable log message listing channel names that users can
 * use as `channelSelector` values in user-defined channels. When `presetSuffix` is provided, channels already covered by builtin preset definitions are filtered
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

    // No preset suffix - log all available channels unfiltered. Used for small channel sets (Fox, HBO) where the full list is actionable without filtering.
    filteredChannels = availableChannels;
    countLabel = String(filteredChannels.length);
  }

  if(filteredChannels.length === 0) {

    return;
  }

  LOG.warn("Channel \"%s\" not found in %s guide. Create a user-defined channel with one of the names below as the Channel Selector and %s as the URL. " +
    "Available channels (%s): %s.", channelName, providerName, guideUrl, countLabel, filteredChannels.join(", "));
}
