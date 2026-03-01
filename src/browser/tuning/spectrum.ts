/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * spectrum.ts: Spectrum TV guide grid channel selection strategy.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, DiscoveredChannel, Nullable, ProviderModule } from "../../types/index.js";
import { LOG, evaluateWithAbort, formatError } from "../../utils/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { logAvailableChannels } from "../channelSelection.js";

// Base URL for Spectrum TV watch page navigation.
const SPECTRUM_BASE_URL = "https://watch.spectrum.net";

// Internal cache entry combining discovery metadata and tuning data. The discovered field provides the API-facing DiscoveredChannel (name, channelSelector,
// affiliate, stationId), and the tmsid provides the Gracenote station ID for direct URL construction. Both are populated from the same discoverGuideChannels()
// result, ensuring a single source of truth for "what channels exist" and "how to tune to them."
interface SpectrumChannelEntry {

  discovered: DiscoveredChannel;
  tmsid: string;
}

// Unified channel cache for Spectrum TV. Maps lowercased lookup keys (callsigns like "espnhd", stripped display names like "espn", and network names like "nbc")
// to their combined discovery and tuning data. Multiple keys may reference the same entry. Populated during the first tune (when the strategy enumerates all
// streamable channels from the non-virtualized guide grid) or the first discovery call. Both tuning (via findSpectrumChannel) and discovery (via
// getCachedChannels / discoverSpectrumChannels) read from this single cache. Cleared on browser disconnect via clearSpectrumCache().
const spectrumChannelCache = new Map<string, SpectrumChannelEntry>();

// Tracks consecutive guide page loads that discover zero channels. When this reaches the recovery threshold, the strategy clears Spectrum site data (cache
// storage) via CDP and reloads the guide to break out of a degraded state where the guide grid container renders but channel entries are not populated. Reset
// to zero on any successful discovery (> 0 channels found) or on browser restart (via clearSpectrumCache).
let consecutiveEmptyDiscoveries = 0;

// Number of consecutive empty discoveries before attempting site data recovery via CDP.
const EMPTY_DISCOVERY_RECOVERY_THRESHOLD = 3;

// Regex pattern for detecting local affiliates and subchannels. Matches "{Name} ({CallSign})" with optional trailing " HD"/" DT" suffix and optional
// "East"/"West" direction.
const PARENTHETICAL_PATTERN = /^(.+?) \(([^)]+)\)(?: (?:HD|DT)(?: (?:East|West))?)?$/;

// Regex pattern for stripping trailing technology and direction suffixes from display names. Handles " HD", " DT", " HD East", " HD West", " DT East", " DT West".
const SUFFIX_PATTERN = / (?:HD|DT)(?: (?:East|West))?$/;

// Broadcast network names used for affiliate detection. When a channel's rowheader matches the parenthetical pattern and the name before "(" is one of these
// networks, the channel is tagged as an affiliate with the network name as channelSelector. Other parenthetical channels (subchannels like "Buzzr (WTVT)") use
// the pre-parenthetical name as channelSelector instead.
const BROADCAST_NETWORKS = new Set([
  "abc", "cbs", "cw", "fox", "nbc", "pbs"
]);

/**
 * Raw channel data extracted from the Spectrum guide DOM in a single evaluate pass. Each entry pairs channel header data (callsign, number, tmsid) with the
 * human-readable rowheader display name, joined by channel-index.
 */
interface RawSpectrumChannel {

  callsign: string;
  channelNumber: string;
  displayName: string;
  tmsid: string;
}

/**
 * Extracts all streamable channels from the Spectrum guide DOM in a single evaluate round-trip. Reads channel headers for callsigns, channel numbers, and
 * Gracenote station IDs (from logo image URLs), then reads rowheaders for human-readable display names, joining by channel-index. Deduplicates by tmsid
 * (first occurrence wins) to eliminate legacy mirror ranges (1000+/1200+). Filters out non-streamable channels via the :not(.nonstreamable) selector.
 * @param page - The Puppeteer page object positioned on the Spectrum guide.
 * @returns Array of raw channel data with callsign, channel number, display name, and tmsid.
 */
async function discoverGuideChannels(page: Page): Promise<RawSpectrumChannel[]> {

  return await evaluateWithAbort(page, (): RawSpectrumChannel[] => {

    const results: RawSpectrumChannel[] = [];
    const seenTmsids = new Set<string>();

    // Build a channel-index to rowheader display name map from the program grid section. Rowheaders use the format "Channel {number} {display name}" — we
    // extract just the display name portion by stripping the "Channel {number} " prefix.
    const rowheaderMap = new Map<string, string>();

    for(const span of Array.from(document.querySelectorAll("span[id^=\"rowheader-\"]"))) {

      const id = span.getAttribute("id") ?? "";
      const index = id.replace("rowheader-", "");
      const text = span.textContent.trim();

      // Strip the "Channel {number} " prefix to get the display name. The prefix always follows this format.
      const prefixMatch = /^Channel \d+ (.+)$/.exec(text);

      if(prefixMatch) {

        rowheaderMap.set(index, prefixMatch[1]);
      }
    }

    // Read all streamable channel headers. Non-streamable channels are excluded by the :not(.nonstreamable) CSS selector.
    for(const li of Array.from(document.querySelectorAll("li.channel-header-row:not(.nonstreamable)"))) {

      const callsignEl = li.querySelector("p.callsign");
      const numberEl = li.querySelector("p.channel-number");
      const logoImg = li.querySelector("img.channel-logo");
      const channelIndex = li.getAttribute("channel-index") ?? "";

      if(!callsignEl || !numberEl || !logoImg) {

        continue;
      }

      const callsign = callsignEl.textContent.trim();
      const channelNumber = numberEl.textContent.trim();

      // Extract the Gracenote station ID from the logo image URL. The format is: /guide/{tmsid}?width=50&sourceType=colorhybrid.
      const src = logoImg.getAttribute("src") ?? "";
      const tmsidMatch = /\/guide\/(\d+)\?/.exec(src);

      if(!tmsidMatch) {

        continue;
      }

      const tmsid = tmsidMatch[1];

      // Deduplicate by tmsid — first occurrence wins (lowest channel number). This eliminates legacy mirror ranges (1000+/1200+) that share the same tmsid
      // with primary entries but have no program listing data.
      if(seenTmsids.has(tmsid)) {

        continue;
      }

      seenTmsids.add(tmsid);

      // Look up the human-readable display name from the rowheader map. Fall back to the callsign if no rowheader exists (unlikely but defensive).
      const displayName = rowheaderMap.get(channelIndex) ?? callsign;

      // Filter out audio-only Music Choice channels (callsigns MC01-MC50, display names like "~MC01:") and Spectrum-internal overflow channels (callsigns
      // SPCTRM1-SPCTRM20, display names like "Spectrum1"). These are streamable in the guide but are not useful for video capture or channel discovery.
      if(displayName.startsWith("~") || /^Spectrum\d/.test(displayName)) {

        continue;
      }

      results.push({ callsign, channelNumber, displayName, tmsid });
    }

    return results;
  }, []);
}

/**
 * Attempts to recover from a degraded Spectrum guide state by clearing cached site data via CDP and reloading the guide page. This targets a failure mode where
 * the guide grid container renders but channel entries are not populated, which can be caused by stale AngularJS template or API response caches. Cookies and
 * login session state are preserved — only caching layers are cleared.
 * @param page - The Puppeteer page object.
 * @returns Discovered channels after recovery, or an empty array if recovery failed.
 */
async function attemptGuideRecovery(page: Page): Promise<RawSpectrumChannel[]> {

  LOG.warn("Clearing Spectrum TV cached site data to recover from empty guide.");

  // Clear cache storage for the Spectrum origin. Cookies and login session state are deliberately preserved to avoid forcing re-authentication.
  try {

    const client = await page.createCDPSession();

    await client.send("Storage.clearDataForOrigin", { origin: SPECTRUM_BASE_URL, storageTypes: "cache_storage" });
    await client.detach();
  } catch(error) {

    LOG.warn("Failed to clear Spectrum TV site data: %s.", formatError(error));

    return [];
  }

  // Reload the guide page with fresh state.
  try {

    await page.goto(SPECTRUM_BASE_URL + "/guide", { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
  } catch(error) {

    LOG.warn("Failed to reload Spectrum TV guide after clearing site data: %s.", formatError(error));

    return [];
  }

  // Wait for the guide grid to render after reload.
  try {

    await page.waitForSelector("li.channel-header-row", { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    LOG.warn("Spectrum TV guide grid did not load after clearing site data.");

    return [];
  }

  // Re-attempt channel discovery on the reloaded page.
  const channels = await discoverGuideChannels(page);

  if(channels.length > 0) {

    LOG.info("Spectrum TV guide recovery succeeded — discovered %s channels after clearing site data.", channels.length);
  } else {

    LOG.warn("Spectrum TV guide still empty after clearing site data.");
  }

  return channels;
}

/**
 * Populates the unified channel cache from raw guide channel data. For each channel, builds a DiscoveredChannel with affiliate detection and pairs it with the
 * tmsid for watch URL construction. Creates multiple cache keys per channel: (a) lowercased callsign, (b) lowercased display name stripped of " HD"/" DT"
 * suffix, and (c) lowercased network name for affiliates (unless the key is already taken by a cable channel). When a channel's rowheader matches the "{Name}
 * ({CallSign})" pattern and the name is a known broadcast network, it is tagged as an affiliate with the network name as channelSelector. Other parenthetical
 * channels (subchannels) use the pre-parenthetical name as channelSelector.
 * @param rawChannels - Array of raw channel data from discoverGuideChannels().
 */
function populateSpectrumChannelCache(rawChannels: RawSpectrumChannel[]): void {

  spectrumChannelCache.clear();

  // First pass: build all entries and store under callsign key. Track network name keys separately to avoid overwriting cable channels in the second pass.
  const entries: { entry: SpectrumChannelEntry; networkKey?: string; strippedKey: string }[] = [];

  for(const ch of rawChannels) {

    const stripped = ch.displayName.replace(SUFFIX_PATTERN, "");
    const parenthetical = PARENTHETICAL_PATTERN.exec(ch.displayName);
    let channelSelector: string;
    let affiliate: string | undefined;
    let networkKey: string | undefined;

    if(parenthetical) {

      const preName = parenthetical[1];

      // Check if this is a broadcast affiliate (e.g., "NBC (WFLA) HD") or a subchannel (e.g., "Buzzr (WTVT)").
      if(BROADCAST_NETWORKS.has(preName.toLowerCase())) {

        channelSelector = preName;
        affiliate = preName;
        networkKey = preName.toLowerCase();
      } else {

        // Subchannel or non-broadcast parenthetical — use the pre-parenthetical name as channelSelector.
        channelSelector = preName;
      }
    } else {

      // Cable channel — use the stripped display name as channelSelector.
      channelSelector = stripped;
    }

    const discovered: DiscoveredChannel = {

      ...(affiliate ? { affiliate } : {}),
      channelSelector,
      name: stripped,
      stationId: ch.tmsid
    };

    const spectrumEntry: SpectrumChannelEntry = { discovered, tmsid: ch.tmsid };

    // Store under lowercased callsign (primary key).
    spectrumChannelCache.set(ch.callsign.toLowerCase(), spectrumEntry);

    entries.push({ entry: spectrumEntry, networkKey, strippedKey: stripped.toLowerCase() });
  }

  // Second pass: add stripped display name keys and network name keys. Cable channel names take precedence over affiliate network names when keys collide (e.g.,
  // a cable channel named "Fox" would keep its key over a "FOX (WTVT)" affiliate entry).
  for(const { entry, networkKey, strippedKey } of entries) {

    // Add stripped display name key if not already taken. First-write wins — if two channels strip to the same name, the first (lower channel number) keeps it.
    if(!spectrumChannelCache.has(strippedKey)) {

      spectrumChannelCache.set(strippedKey, entry);
    }

    // Add network name key for affiliates if not already taken by a cable channel or another affiliate.
    if(networkKey && !spectrumChannelCache.has(networkKey)) {

      spectrumChannelCache.set(networkKey, entry);
    }
  }
}

/**
 * Looks up a channel in the unified cache using tiered matching logic:
 *
 * 1. Exact match: cache key equals the lowercased input (matches callsigns like "espnhd" and stripped names like "espn" and network names like "nbc").
 * 2. HD/DT suffix tolerance: input + "hd" or input + "dt" matches a cache key (e.g., "espn" → "espnhd", "wfla" → "wfladt").
 * 3. Display name iteration: iterate all cache entries, check if discovered.name (lowercased) equals input. Catches long display names like "Discovery Channel"
 *    when only the callsign-derived key was cached.
 *
 * When a non-exact match succeeds, the result is cached under the input key for O(1) lookup on subsequent calls. This function doubles as the resolveDirectUrl
 * hook — after the first tune populates the cache via channel discovery, every subsequent Spectrum tune resolves here without loading the guide page.
 * @param channelName - The channelSelector value (e.g., "ESPN", "NBC", "Discovery Channel").
 * @returns The matching cache entry or null if no match is found.
 */
function findSpectrumChannel(channelName: string): Nullable<SpectrumChannelEntry> {

  const lower = channelName.toLowerCase();

  // Tier 1: Exact match on any cache key (callsigns, stripped names, network names).
  const exact = spectrumChannelCache.get(lower);

  if(exact) {

    return exact;
  }

  // Tier 2: HD/DT suffix tolerance. Try appending common technology suffixes.
  const hdMatch = spectrumChannelCache.get(lower + "hd");

  if(hdMatch) {

    spectrumChannelCache.set(lower, hdMatch);

    return hdMatch;
  }

  const dtMatch = spectrumChannelCache.get(lower + "dt");

  if(dtMatch) {

    spectrumChannelCache.set(lower, dtMatch);

    return dtMatch;
  }

  // Tier 3: Display name iteration. Check if any entry's discovered.name matches the input. This catches channels with long display names (e.g.,
  // "Discovery Channel") that may not have a matching cache key because the callsign was "DSCHD" and the stripped key "discovery channel" was already checked
  // in tier 1. This tier is the fallback for any names not covered by the first two tiers.
  const seen = new Set<SpectrumChannelEntry>();

  for(const entry of spectrumChannelCache.values()) {

    if(seen.has(entry)) {

      continue;
    }

    seen.add(entry);

    if(entry.discovered.name.toLowerCase() === lower) {

      spectrumChannelCache.set(lower, entry);

      return entry;
    }
  }

  return null;
}

/**
 * Invalidates the cached Spectrum channel entry for the given channel selector. Called when a cached URL fails to produce a working stream. Deletes the
 * channelSelector key — the original keys from channel discovery are left intact and will be refreshed on the next strategy run when the guide page is reloaded.
 * @param channelSelector - The channel selector string to invalidate.
 */
function invalidateSpectrumDirectUrl(channelSelector: string): void {

  spectrumChannelCache.delete(channelSelector.toLowerCase());
}

/**
 * Clears the Spectrum channel cache. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts, since a fresh browser session
 * may have different channel availability.
 */
function clearSpectrumCache(): void {

  spectrumChannelCache.clear();
  consecutiveEmptyDiscoveries = 0;
}

/**
 * Derives a DiscoveredChannel array from the unified channel cache, deduplicating alias entries via Set reference equality. Multiple cache keys may point to
 * the same SpectrumChannelEntry (callsign key, stripped name key, network name key, and any alias keys created by findSpectrumChannel). Without deduplication,
 * iterating cache values would produce duplicate entries. Sorts by name before returning.
 * @returns Sorted, deduplicated array of discovered channels.
 */
function buildSpectrumDiscoveredChannels(): DiscoveredChannel[] {

  const channels: DiscoveredChannel[] = [];
  const seen = new Set<SpectrumChannelEntry>();

  for(const entry of spectrumChannelCache.values()) {

    if(seen.has(entry)) {

      continue;
    }

    seen.add(entry);
    channels.push(entry.discovered);
  }

  channels.sort((a, b) => a.name.localeCompare(b.name));

  return channels;
}

/**
 * Spectrum TV grid strategy: discovers all streamable channels from the non-virtualized guide at watch.spectrum.net/guide in a single evaluate pass, populating
 * the unified channel cache so that subsequent tunes to any Spectrum channel resolve via findSpectrumChannel() without loading the guide page. All ~442 streamable
 * channel rows are present in the DOM simultaneously (AngularJS, non-virtualized), so one querySelectorAll captures every channel's callsign, display name, and
 * Gracenote station ID.
 *
 * The selection process:
 * 1. Wait for li.channel-header-row elements to confirm the guide grid has loaded (AngularJS rendering gate).
 * 2. Discover all channels: extract callsigns, channel numbers, tmsids, and display names from channel headers and rowheaders.
 * 3. If no channels are discovered (degraded guide state), attempt recovery by clearing cached site data via CDP and reloading.
 * 4. Populate the unified channel cache with all discovered channels.
 * 5. Look up the target channel using tiered matching (exact, suffix tolerance, display name) against the cache.
 * 6. Navigate to the matched watch URL via page.goto() — direct URL navigation with no clicking.
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector (channel name, e.g., "ESPN", "CNN", "NBC").
 * @returns Result object with success status and optional failure reason.
 */
async function spectrumGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelName = profile.channelSelector;

  // Wait for the guide grid to render. All streamable channels load simultaneously (non-virtualized AngularJS), so once any row exists, all channels are
  // queryable.
  try {

    await page.waitForSelector("li.channel-header-row", { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return { reason: "Spectrum guide grid did not load.", success: false };
  }

  // Discover all channels from the guide grid.
  let allChannels = await discoverGuideChannels(page);

  // If the guide loaded but no channels were discovered, the guide is in a degraded state — the grid container rendered but channel entries were not populated.
  // This can happen when stale AngularJS template or API response caches become inconsistent. Track consecutive occurrences and attempt recovery by clearing
  // cached site data once the threshold is reached.
  if(allChannels.length === 0) {

    consecutiveEmptyDiscoveries++;

    LOG.warn("Spectrum TV guide loaded but no channels were discovered (%s consecutive). The guide may be in a degraded state.",
      consecutiveEmptyDiscoveries);

    if(consecutiveEmptyDiscoveries >= EMPTY_DISCOVERY_RECOVERY_THRESHOLD) {

      allChannels = await attemptGuideRecovery(page);
    }
  }

  // If we still have no channels after the initial discovery and any recovery attempt, there is nothing to search or cache.
  if(allChannels.length === 0) {

    return { reason: "Spectrum guide is empty — no channels were discovered.", success: false };
  }

  // Successful discovery — reset the consecutive empty counter and repopulate the unified channel cache. Always repopulate rather than skipping when the cache
  // has entries, because invalidated entries need to be restored with fresh data from the guide.
  consecutiveEmptyDiscoveries = 0;
  populateSpectrumChannelCache(allChannels);

  LOG.debug("tuning:spectrum", "Discovered %s Spectrum channels.", allChannels.length);

  // Look up the target channel using tiered matching against the populated cache.
  const entry = findSpectrumChannel(channelName);

  if(!entry) {

    // Channel not found. Log available channels as a diagnostic to help users identify their market's channel names.
    logAvailableChannels({

      availableChannels: buildSpectrumDiscoveredChannels().map((ch) => ch.name).sort(),
      channelName,
      guideUrl: "https://watch.spectrum.net/guide",
      presetSuffix: "-spectrum",
      providerName: "Spectrum TV"
    });

    return { reason: "Channel \"" + channelName + "\" not found in Spectrum guide.", success: false };
  }

  LOG.debug("tuning:spectrum", "Navigating to Spectrum watch URL for %s (tmsid: %s).", channelName, entry.tmsid);

  try {

    await page.goto(SPECTRUM_BASE_URL + "/livetv?tmsid=" + entry.tmsid, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
  } catch(error) {

    return { reason: "Failed to navigate to Spectrum watch page: " + formatError(error) + ".", success: false };
  }

  return { success: true };
}

/**
 * Async wrapper around findSpectrumChannel for the ChannelStrategyEntry.resolveDirectUrl contract. The page parameter is unused because Spectrum watch URLs are
 * resolved purely from the in-memory cache populated during the initial guide page discovery.
 * @param channelSelector - The channel selector string (e.g., "ESPN", "CNN", "NBC").
 * @param _page - Unused. Present to satisfy the async resolveDirectUrl signature.
 * @returns The cached watch URL or null.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars, @typescript-eslint/require-await
async function resolveSpectrumDirectUrl(channelSelector: string, _page: Page): Promise<Nullable<string>> {

  const entry = findSpectrumChannel(channelSelector);

  if(!entry) {

    return null;
  }

  return SPECTRUM_BASE_URL + "/livetv?tmsid=" + entry.tmsid;
}

/**
 * Discovers all channels from the Spectrum TV guide. Returns cached results if the unified channel cache is populated from a prior tune or discovery call.
 * Otherwise, waits for the first channel header row to confirm the guide has rendered, then extracts all streamable channels via discoverGuideChannels() and
 * populates the cache (unless empty, to allow retries on transient failures).
 * @param page - The Puppeteer page object.
 * @returns Array of discovered channels with affiliate tagging and Gracenote station IDs.
 */
async function discoverSpectrumChannels(page: Page): Promise<DiscoveredChannel[]> {

  // Return from the unified cache if already populated.
  if(spectrumChannelCache.size > 0) {

    return buildSpectrumDiscoveredChannels();
  }

  // Wait for at least one channel header row to confirm the guide grid has rendered.
  try {

    await page.waitForSelector("li.channel-header-row", { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return [];
  }

  const allChannels = await discoverGuideChannels(page);

  // Do not cache empty results — leave the cache empty so subsequent calls retry the full walk. Empty results can indicate no subscription or transient failures.
  if(allChannels.length === 0) {

    return [];
  }

  populateSpectrumChannelCache(allChannels);

  return buildSpectrumDiscoveredChannels();
}

/**
 * Returns cached discovered channels from the unified channel cache, or null if the cache is empty (no prior tune or discovery call has enumerated the lineup).
 * @returns Sorted array of discovered channels or null.
 */
function getSpectrumCachedChannels(): Nullable<DiscoveredChannel[]> {

  if(spectrumChannelCache.size === 0) {

    return null;
  }

  return buildSpectrumDiscoveredChannels();
}

export const spectrumProvider: ProviderModule = {

  discoverChannels: discoverSpectrumChannels,
  getCachedChannels: getSpectrumCachedChannels,
  guideUrl: "https://watch.spectrum.net/guide",
  label: "Spectrum TV",
  slug: "spectrum",
  strategy: {

    clearCache: clearSpectrumCache,
    execute: spectrumGridStrategy,
    invalidateDirectUrl: invalidateSpectrumDirectUrl,
    resolveDirectUrl: resolveSpectrumDirectUrl
  },
  strategyName: "spectrumGrid"
};
