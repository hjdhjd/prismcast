/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * spectrum.ts: Spectrum TV guide grid channel selection strategy.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, DiscoveredChannel, Nullable, ProviderModule } from "../../types/index.ts";
import { LOG, evaluateWithAbort, formatError } from "../../utils/index.ts";
import { attemptGuideRecovery, createEmptyDiscoveryGuard, logAvailableChannels } from "./shared.ts";
import { createProviderChannelCache, dedupeCacheEntries } from "./cache.ts";
import { CONFIG } from "../../config/index.ts";
import type { Page } from "puppeteer-core";
import type { PersistedLineupChannel } from "../../config/providerLineups.ts";

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
const spectrumCache = createProviderChannelCache<SpectrumChannelEntry>((entry) => entry.discovered);

// Recovery guard for the degraded guide state where the grid container renders but channel entries are not populated, which stale AngularJS template or API
// response caches produce. The guard counts the empty loads and signals when clearing Spectrum site data (cache storage) via CDP is warranted. Reset on any
// successful discovery (> 0 channels found) and on browser restart (via clearSpectrumCache).
const emptyDiscoveryGuard = createEmptyDiscoveryGuard("Spectrum TV");

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

    // Build a channel-index to rowheader display name map from the program grid section. Rowheaders use the format "Channel {number} {display name}" - we
    // extract just the display name portion by stripping the "Channel {number} " prefix.
    const rowheaderMap = new Map<string, string>();

    for(const span of Array.from(document.querySelectorAll("span[id^=\"rowheader-\"]"))) {

      const id = span.getAttribute("id") ?? "";
      const index = id.replace("rowheader-", "");
      const text = span.textContent.trim();

      // Strip the "Channel {number} " prefix to get the display name. The prefix always follows this format.
      const prefixMatch = /^Channel \d+ (.+)$/.exec(text);

      if(prefixMatch?.[1]) {

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
      const tmsid = /\/guide\/(\d+)\?/.exec(src)?.[1];

      if(!tmsid) {

        continue;
      }

      // Deduplicate by tmsid - first occurrence in DOM order wins. The guide renders rows in ascending channel-number order, so this keeps the primary
      // (lowest-numbered) entry and eliminates legacy mirror ranges (1000+/1200+) that share the same tmsid with primary entries but have no program listing data.
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
 * Populates the unified channel cache from raw guide channel data. For each channel, builds a DiscoveredChannel with affiliate detection and pairs it with the
 * tmsid for watch URL construction. Creates multiple cache keys per channel: (a) lowercased callsign, (b) lowercased display name stripped of " HD"/" DT"
 * suffix, and (c) lowercased network name for affiliates (unless the key is already taken by a cable channel). When a channel's rowheader matches the "{Name}
 * ({CallSign})" pattern and the name is a known broadcast network, it is tagged as an affiliate with the network name as channelSelector. Other parenthetical
 * channels (subchannels) use the pre-parenthetical name as channelSelector.
 * @param rawChannels - Array of raw channel data from discoverGuideChannels().
 */
function populateSpectrumChannelCache(rawChannels: RawSpectrumChannel[]): void {

  spectrumCache.clear();

  // First pass: build all entries and store under callsign key. Track network name keys separately to avoid overwriting cable channels in the second pass.
  const entries: { entry: SpectrumChannelEntry; networkKey?: string; strippedKey: string }[] = [];

  for(const ch of rawChannels) {

    const stripped = ch.displayName.replace(SUFFIX_PATTERN, "");
    const parenthetical = PARENTHETICAL_PATTERN.exec(ch.displayName);
    let channelSelector: string;
    let affiliate: string | undefined;
    let networkKey: string | undefined;

    const preName = parenthetical?.[1];

    if(preName) {

      // Check if this is a broadcast affiliate (e.g., "NBC (WFLA) HD") or a subchannel (e.g., "Buzzr (WTVT)").
      if(BROADCAST_NETWORKS.has(preName.toLowerCase())) {

        channelSelector = preName;
        affiliate = preName;
        networkKey = preName.toLowerCase();
      } else {

        // Subchannel or non-broadcast parenthetical - use the pre-parenthetical name as channelSelector.
        channelSelector = preName;
      }
    } else {

      // Cable channel - use the stripped display name as channelSelector.
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
    spectrumCache.map.set(ch.callsign.toLowerCase(), spectrumEntry);

    entries.push({ entry: spectrumEntry, networkKey, strippedKey: stripped.toLowerCase() });
  }

  // Second pass: add stripped display name keys and network name keys. Cable channel names take precedence over affiliate network names when keys collide (e.g.,
  // a cable channel named "Fox" would keep its key over a "FOX (WTVT)" affiliate entry).
  for(const { entry, networkKey, strippedKey } of entries) {

    // Add stripped display name key if not already taken. First-write wins - if two channels strip to the same name, the first (lower channel number) keeps it.
    if(!spectrumCache.map.has(strippedKey)) {

      spectrumCache.map.set(strippedKey, entry);
    }

    // Add network name key for affiliates if not already taken by a cable channel or another affiliate.
    if(networkKey && !spectrumCache.map.has(networkKey)) {

      spectrumCache.map.set(networkKey, entry);
    }
  }

  // The guide pass this was built from covered every streamable row, so what sits in the cache at this point is the complete lineup and warm reads can be
  // served from it.
  spectrumCache.markComplete();
}

/**
 * Builds the Spectrum watch address for a channel's Gracenote station ID. This is the one place the /livetv?tmsid= form is written: the strategy's direct
 * navigation, the cached-URL resolver, and the durable-lineup export all read it here, so a change to Spectrum's watch-page shape is a one-line edit rather than
 * a hunt for every place the string was assembled.
 * @param tmsid - The channel's Gracenote station ID.
 * @returns The full Spectrum watch URL.
 */
function spectrumWatchUrl(tmsid: string): string {

  return SPECTRUM_BASE_URL + "/livetv?tmsid=" + tmsid;
}

/**
 * Returns Spectrum TV's durable lineup for the persisted lineup store: each cached channel's identity paired with the watch URL built from its Gracenote station
 * ID. The station ID is a stable channel identifier rather than session state, so the address it builds survives a restart and lets a boot whose guide read failed
 * still tune directly. The tiered matching below files one entry object under several alias keys, so the entries are reduced to one occurrence per object before
 * projection.
 * @returns The durable lineup rows, or null when the cache is cold.
 */
function exportSpectrumLineup(): Nullable<PersistedLineupChannel[]> {

  if(spectrumCache.map.size === 0) {

    return null;
  }

  return dedupeCacheEntries(spectrumCache.map.values()).map((entry) => ({

    channelSelector: entry.discovered.channelSelector,
    name: entry.discovered.name,
    watchUrl: spectrumWatchUrl(entry.tmsid)
  }));
}

/**
 * Looks up a channel in the unified cache using tiered matching logic:
 *
 * 1. Exact match: cache key equals the lowercased input (matches callsigns like "espnhd" and stripped names like "espn" and network names like "nbc").
 * 2. HD/DT suffix tolerance: input + "hd" or input + "dt" matches a cache key (e.g., "espn" -> "espnhd", "wfla" -> "wfladt").
 * 3. Display name iteration: iterate all cache entries, check if discovered.name (lowercased) equals input. Catches long display names like "Discovery Channel"
 *    when only the callsign-derived key was cached.
 *
 * When a non-exact match succeeds, the result is cached under the input key for O(1) lookup on subsequent calls. This function is the cache-lookup core behind
 * the resolveDirectUrl hook (resolveSpectrumDirectUrl) - after the first tune populates the cache, every subsequent Spectrum tune resolves through here without
 * loading the guide page.
 * @param channelName - The channelSelector value (e.g., "ESPN", "NBC", "Discovery Channel").
 * @returns The matching cache entry or null if no match is found.
 */
function findSpectrumChannel(channelName: string): Nullable<SpectrumChannelEntry> {

  const lower = channelName.toLowerCase();

  // Tier 1: Exact match on any cache key (callsigns, stripped names, network names).
  const exact = spectrumCache.map.get(lower);

  if(exact) {

    return exact;
  }

  // Tier 2: HD/DT suffix tolerance. Try appending common technology suffixes.
  const hdMatch = spectrumCache.map.get(lower + "hd");

  if(hdMatch) {

    spectrumCache.map.set(lower, hdMatch);

    return hdMatch;
  }

  const dtMatch = spectrumCache.map.get(lower + "dt");

  if(dtMatch) {

    spectrumCache.map.set(lower, dtMatch);

    return dtMatch;
  }

  // Tier 3: Display name iteration. Check if any entry's discovered.name matches the input. This catches channels with long display names (e.g.,
  // "Discovery Channel") that may not have a matching cache key because the callsign was "DSCHD" and the stripped key "discovery channel" was already checked
  // in tier 1. This tier is the fallback for any names not covered by the first two tiers.
  const seen = new Set<SpectrumChannelEntry>();

  for(const entry of spectrumCache.map.values()) {

    if(seen.has(entry)) {

      continue;
    }

    seen.add(entry);

    if(entry.discovered.name.toLowerCase() === lower) {

      spectrumCache.map.set(lower, entry);

      return entry;
    }
  }

  return null;
}

/**
 * Clears the Spectrum channel cache. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts, since a fresh browser session
 * may have different channel availability.
 */
function clearSpectrumCache(): void {

  spectrumCache.clear();
  emptyDiscoveryGuard.reset();
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
 * 6. Navigate to the matched watch URL via page.goto() - direct URL navigation with no clicking.
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

    return { guideUnavailable: true, reason: "Spectrum guide grid did not load.", success: false };
  }

  // Discover all channels from the guide grid.
  let allChannels = await discoverGuideChannels(page);

  // If the guide loaded but no channels were discovered, the guide is in a degraded state - the grid container rendered but channel entries were not populated.
  // This can happen when stale AngularJS template or API response caches become inconsistent. Track consecutive occurrences and attempt recovery by clearing
  // cached site data once the threshold is reached.
  if(allChannels.length === 0) {

    if(emptyDiscoveryGuard.recordEmpty()) {

      allChannels = await attemptGuideRecovery(page, {

        discover: discoverGuideChannels,
        origin: SPECTRUM_BASE_URL,
        providerName: "Spectrum TV",
        reloadUrl: SPECTRUM_BASE_URL + "/guide",
        storageTypes: "cache_storage",
        waitSelector: "li.channel-header-row"
      });
    }
  }

  // If we still have no channels after the initial discovery and any recovery attempt, there is nothing to search or cache.
  if(allChannels.length === 0) {

    return { reason: "Spectrum guide is empty - no channels were discovered.", success: false };
  }

  // Successful discovery - reset the consecutive empty counter and repopulate the unified channel cache. Always repopulate rather than skipping when the cache
  // has entries, because invalidated entries need to be restored with fresh data from the guide.
  emptyDiscoveryGuard.reset();
  populateSpectrumChannelCache(allChannels);

  LOG.debug("tuning:spectrum", "Discovered %s Spectrum channels.", allChannels.length);

  // Look up the target channel using tiered matching against the populated cache.
  const entry = findSpectrumChannel(channelName);

  if(!entry) {

    // Channel not found. Log available channels as a diagnostic to help users identify their market's channel names.
    logAvailableChannels({

      availableChannels: spectrumCache.discovered().map((ch) => ch.name).sort(),
      channelName,
      guideUrl: "https://watch.spectrum.net/guide",
      presetSuffix: "-spectrum",
      providerName: "Spectrum TV"
    });

    return { reason: "Channel \"" + channelName + "\" not found in Spectrum guide.", success: false };
  }

  LOG.debug("tuning:spectrum", "Navigating to Spectrum watch URL for %s (tmsid: %s).", channelName, entry.tmsid);

  try {

    await page.goto(spectrumWatchUrl(entry.tmsid), { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
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
async function resolveSpectrumDirectUrl(channelSelector: string, _page: Page): Promise<Nullable<string>> {

  const entry = findSpectrumChannel(channelSelector);

  if(!entry) {

    return null;
  }

  return spectrumWatchUrl(entry.tmsid);
}

/**
 * Discovers all channels from the Spectrum TV guide. Returns cached results if the unified channel cache is populated from a prior tune or discovery call.
 * Otherwise, waits for the first channel header row to confirm the guide has rendered, then extracts all streamable channels via discoverGuideChannels() and
 * populates the cache (unless empty, to allow retries on transient failures).
 * @param page - The Puppeteer page object.
 * @returns Array of discovered channels with affiliate tagging and Gracenote station IDs.
 */
async function discoverSpectrumChannels(page: Page): Promise<DiscoveredChannel[]> {

  // Return from the unified cache if a prior tune or discovery call already enumerated the lineup.
  const cached = spectrumCache.cached();

  if(cached) {

    return cached;
  }

  // Wait for at least one channel header row to confirm the guide grid has rendered.
  try {

    await page.waitForSelector("li.channel-header-row", { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return [];
  }

  const allChannels = await discoverGuideChannels(page);

  // Do not cache empty results - leave the cache empty so subsequent calls retry the full walk. Empty results can indicate no subscription or transient failures.
  if(allChannels.length === 0) {

    return [];
  }

  populateSpectrumChannelCache(allChannels);

  return spectrumCache.discovered();
}

export const spectrumProvider: ProviderModule = {

  discoverChannels: discoverSpectrumChannels,
  exportDurableLineup: exportSpectrumLineup,
  getCachedChannels: spectrumCache.cached,
  guideUrl: "https://watch.spectrum.net/guide",
  label: "Spectrum TV",

  // Profile for Spectrum TV (watch.spectrum.net) live guide grid. The guide page at /guide presents all ~442 streamable channels in a non-virtualized AngularJS
  // DOM. Channel headers provide callsigns, channel numbers, and Gracenote station IDs (tmsid from logo image URLs). The spectrumGrid strategy reads all channels
  // in a single evaluate pass, caches them, and navigates directly to /livetv?tmsid={stationId} - no clicking, no SPA state changes, no overlays. The channelSelector
  // matches against clean channel names (e.g., "ESPN", "CNN", "NBC") with callsign suffix tolerance and affiliate network name resolution.
  profile: {

    category: "multiChannel",
    channelSelection: { strategy: "spectrumGrid" },
    description: "Spectrum TV with guide grid channel selection. Set Channel Selector to the channel name (e.g., ESPN, CNN, NBC).",
    extends: "fullscreenApi",
    summary: "Spectrum TV (guide grid, needs selector)"
  },
  profileName: "spectrum",
  slug: "spectrum",
  strategy: {

    clearCache: clearSpectrumCache,
    execute: spectrumGridStrategy,

    // Only the failing selector's own key is dropped. The keys channel discovery wrote stay put, and the next strategy run reloads the guide and rebuilds them.
    invalidateDirectUrl: spectrumCache.invalidate,
    resolveDirectUrl: resolveSpectrumDirectUrl
  },
  strategyName: "spectrumGrid"
};
