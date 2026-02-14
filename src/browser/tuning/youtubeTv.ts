/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * youtubeTv.ts: YouTube TV EPG grid channel selection strategy.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, Nullable } from "../../types/index.js";
import { LOG, evaluateWithAbort, formatError } from "../../utils/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { logAvailableChannels } from "../channelSelection.js";

// Base URL for YouTube TV watch page navigation.
const YOUTUBE_TV_BASE_URL = "https://tv.youtube.com";

// Known alternate channel names for affiliates that vary by market. CW appears as "WGN" in some markets. PBS affiliates appear under local call letters (e.g.,
// WTTW, KQED) or branded names (e.g., "Cascade PBS", "Lakeshore PBS") rather than "PBS", so we list the major market call letters and branded names to cover most
// users. Each alternate is tried after the primary name fails both exact and prefix+digit matching. Users in smaller markets override via custom channel entries with
// their local call letters as the channelSelector.
const CHANNEL_ALTERNATES: Record<string, string[]> = {

  "cw": ["WGN"],
  "pbs": [
    "Cascade PBS", "GBH", "KAET", "KBTC", "KCET", "KCTS", "KERA", "KLCS", "KOCE", "KPBS", "KQED", "KRMA", "KUHT", "KVIE", "Lakeshore PBS", "MPT", "NJ PBS",
    "THIRTEEN", "TPT", "WETA", "WGBH", "WHYY", "WLIW", "WNED", "WNET", "WNIT", "WPBA", "WPBT", "WTTW", "WTVS", "WXEL"
  ]
};

/**
 * YouTube TV grid strategy: finds a channel in the non-virtualized EPG grid at tv.youtube.com/live by querying the aria-label attribute on thumbnail endpoint
 * elements. All ~256 channel rows are present in the DOM simultaneously, so a single querySelector locates the target channel. The strategy extracts the watch
 * URL from the matching anchor element and navigates directly — no scrolling, clicking, or timing workarounds needed.
 *
 * The selection process:
 * 1. Wait for ytu-epg-row elements to confirm the guide grid has loaded.
 * 2. Find the target channel using a case-insensitive aria-label CSS selector.
 * 3. Extract the href attribute and validate it starts with "watch/" (not "live" or "browse/").
 * 4. Navigate to the full watch URL via page.goto().
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector (channel name, e.g., "CNN", "ESPN", "NBC").
 * @returns Result object with success status and optional failure reason.
 */
export async function youtubeGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelName = profile.channelSelector;

  // Wait for the EPG grid to render. All ~256 rows load simultaneously (no virtualization), so once any row exists, all channels are queryable.
  try {

    await page.waitForSelector("ytu-epg-row", { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return { reason: "YouTube TV guide grid did not load.", success: false };
  }

  // Build the list of names to try: the primary name first, then any known alternates for markets where the affiliate uses a different name. The eslint disable is
  // needed because TypeScript's Record indexing doesn't capture that the key may not exist at runtime.
  const alternates = CHANNEL_ALTERNATES[channelName.toLowerCase()];

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  const namesToTry = alternates ? [ channelName, ...alternates ] : [channelName];

  // Find the watch URL for the target channel. For each name in the list, we try three matching tiers:
  //
  // 1. Exact match: aria-label="watch {Name}" (case-insensitive).
  // 2. Prefix+digit: aria-label starts with "watch {Name} " followed by a digit. This catches local affiliates displayed as "{Network} {Number}" (e.g., "NBC 5",
  //    "ABC 7", "FOX 32") while excluding unrelated channels (e.g., "NBC Sports Chicago").
  // 3. Parenthetical suffix: aria-label starts with "watch {Name} (" to match timezone/region variants like "Magnolia Network (Pacific)" or
  //    "The Filipino Channel (Pacific)".
  const watchPath = await evaluateWithAbort(page, (names: string[]): Nullable<string> => {

    // Helper to extract and validate a watch URL from an anchor element. Returns the href if it points to a streamable watch page, null otherwise.
    const extractWatchHref = (anchor: Nullable<HTMLAnchorElement>): Nullable<string> => {

      if(!anchor) {

        return null;
      }

      const href = anchor.getAttribute("href");

      // Validate the href points to a streamable watch page. Channels with "live" or "browse/" hrefs are premium add-ons or info pages that cannot be streamed.
      if(!href?.startsWith("watch/")) {

        return null;
      }

      return href;
    };

    // Try each name in order. The primary channel name is tried first, followed by any known alternates.
    for(const name of names) {

      // Tier 1: Exact match. The CSS "i" flag enables case-insensitive matching to handle variations in capitalization between the channel selector and the guide.
      const exactSelector = "ytu-endpoint.tenx-thumb[aria-label=\"watch " + name + "\" i] a";
      const exactResult = extractWatchHref(document.querySelector(exactSelector) as Nullable<HTMLAnchorElement>);

      if(exactResult) {

        return exactResult;
      }

      // Tier 2: Prefix + digit match for local affiliates. Find all thumbnails whose aria-label starts with "watch {Name} " and filter to those where the next
      // character is a digit, matching the "{Network} {Number}" pattern (e.g., "NBC 5", "ABC 7") while excluding unrelated channels (e.g., "NBC Sports Chicago").
      const prefixSelector = "ytu-endpoint.tenx-thumb[aria-label^=\"watch " + name + " \" i] a";
      const candidates = document.querySelectorAll(prefixSelector);
      const prefix = "watch " + name + " ";

      for(const candidate of Array.from(candidates)) {

        const parent = candidate.closest("ytu-endpoint.tenx-thumb");
        const label = parent?.getAttribute("aria-label") ?? "";
        const suffix = label.slice(prefix.length);

        // Accept only if the remainder starts with a digit — this is the local affiliate channel number.
        if((suffix.length > 0) && (suffix.charCodeAt(0) >= 48) && (suffix.charCodeAt(0) <= 57)) {

          return extractWatchHref(candidate as HTMLAnchorElement);
        }
      }

      // Tier 3: Parenthetical suffix match for timezone/region variants. Channels like "Magnolia Network (Pacific)" or "The Filipino Channel (Pacific)" have the
      // base name followed by a space and a parenthetical. The CSS selector matches aria-labels starting with "watch {Name} (" to catch these variants.
      const parenSelector = "ytu-endpoint.tenx-thumb[aria-label^=\"watch " + name + " (\" i] a";
      const parenResult = extractWatchHref(document.querySelector(parenSelector) as Nullable<HTMLAnchorElement>);

      if(parenResult) {

        return parenResult;
      }
    }

    return null;
  }, [namesToTry]);

  if(!watchPath) {

    // Channel not found. Query all available channel names from the guide grid and log them as a diagnostic to help users identify their market's channel names
    // and create user-defined channels with the correct channelSelector value.
    try {

      const availableChannels = await evaluateWithAbort(page, (): string[] => {

        return Array.from(document.querySelectorAll("ytu-endpoint.tenx-thumb[aria-label]"))
          .map((el) => {

            const label = el.getAttribute("aria-label") ?? "";

            return label.startsWith("watch ") ? label.slice(6) : "";
          })
          .filter((name) => name.length > 0)
          .sort();
      }, []);

      // Build additional known names from CHANNEL_ALTERNATES values so they are also filtered out of the diagnostic list.
      const additionalKnownNames: string[] = [];

      for(const alts of Object.values(CHANNEL_ALTERNATES)) {

        for(const alt of alts) {

          additionalKnownNames.push(alt);
        }
      }

      logAvailableChannels({

        additionalKnownNames,
        availableChannels,
        channelName,
        guideUrl: "https://tv.youtube.com/live",
        presetSuffix: "-yttv",
        providerName: "YouTube TV"
      });
    } catch {

      // Diagnostic dump is best-effort. Don't let it mask the real channel selection failure.
    }

    return { reason: "Channel \"" + channelName + "\" not found in YouTube TV guide.", success: false };
  }

  // Navigate directly to the watch URL. This auto-starts playback without any click interaction needed.
  const watchUrl = YOUTUBE_TV_BASE_URL + "/" + watchPath;

  LOG.debug("tuning:yttv", "Navigating to YouTube TV watch URL for %s.", channelName);

  try {

    await page.goto(watchUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
  } catch(error) {

    return { reason: "Failed to navigate to YouTube TV watch page: " + formatError(error) + ".", success: false };
  }

  return { success: true };
}

