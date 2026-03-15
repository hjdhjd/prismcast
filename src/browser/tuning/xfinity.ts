/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * xfinity.ts: Xfinity Stream in-page SPA channel switching strategy.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, DiscoveredChannel, Nullable, ProviderModule } from "../../types/index.js";
import { LOG, delay, formatError } from "../../utils/index.js";
import { CONFIG } from "../../config/index.js";
import type { Page } from "puppeteer-core";
import { logAvailableChannels } from "../channelSelection.js";

/* Xfinity Stream uses a Polymer SPA (`TV-APP`) that manages channel playback via an internal `channelMap` object. The `channelMap.channels` property is populated
 * from the channelmap API during page load and contains the complete channel lineup (~1389 entries). Calling `_watchChannelEventHandler(null, { channel })` on the
 * `TV-APP` element switches channels in-page in ~2-3 seconds — matching native browser performance. This avoids the ~15-second cost of full page navigation.
 *
 * The channelmap API at xtvapi.cloudtv.comcast.net returns the complete channel lineup in a single response during guide page load. We intercept this response to
 * populate a Node-side channel cache for name matching and channel discovery. At tune time, the strategy waits for the SPA's `channelMap.channels` to populate,
 * looks up the target channel by callSign, and invokes `_watchChannelEventHandler` via `page.evaluate()`.
 *
 * Some entertainment channels (e.g., Discovery) display a "Watch Now" modal button after the SPA switches channels. A fire-and-forget poll detects and clicks this
 * button concurrently with `waitForVideoReady`. News channels (e.g., CNN) auto-play without any modal.
 *
 * Tuning flow:
 * 1. resolveDirectUrl: sets up channelmap API interception and returns null. On warm tunes, also enables CDP request interception to serve the cached channelmap
 *    response instantly, eliminating the 3-5s network round-trip.
 * 2. The caller (tuneToChannel in video.ts) navigates to the guide URL, loading the Polymer SPA.
 * 3. xfinityDirectStrategy: waits for `TV-APP.channelMap.channels` to populate, finds the target channel by callSign, and calls `_watchChannelEventHandler`.
 * 4. A fire-and-forget poll watches for a visible "Watch Now" modal button and clicks it if it appears.
 * 5. initializePlayback continues with waitForVideoReady, fullscreen, etc.
 */

// Guide page URL. The channelmap API fires automatically when this page loads.
const XFINITY_GUIDE_URL = "https://www.xfinity.com/stream/listings";

// URL pattern for the channelmap API response. Matched against response URLs to identify channelmap data.
const CHANNELMAP_API_PATTERN = "xtvapi.cloudtv.comcast.net/channelmap";

// Polling interval for cold-cache wait. 200ms balances responsiveness against CPU overhead.
const CACHE_POLL_INTERVAL = 200;

// Broadcast network verbose names from callSignVoiceOverHint, mapped to standard abbreviations. Used to identify local affiliates and set the channelSelector to the
// network abbreviation rather than the verbose name or callSign.
const BROADCAST_HINTS: Record<string, string> = {

  "american broadcasting company": "ABC",
  "columbia broadcasting system": "CBS",
  "fox broadcasting company": "Fox",
  "national broadcasting company": "NBC",
  "public broadcasting service": "PBS"
};

// Audio-only channel brand prefixes to skip during channelmap processing. These channels produce static screens with music and are not useful for video capture.
// Matched against the lowercased branchOf/company/callSign field using prefix comparison to catch genre variants (e.g., "Music Choice Hip-Hop").
const AUDIO_ONLY_PREFIXES = [ "music choice", "stingray" ];

// Common Xfinity callSign suffixes to strip when building secondary cache keys. Stripped in order — the first match is used. Only strip when the result is at least
// 2 characters to avoid degenerate cases. Intentionally excludes "d" and "h" — those appear as meaningful parts of callSigns (ESPND, BETHH) rather than pure
// technology suffixes, causing cache key collisions with unrelated channels.
const CALLSIGN_SUFFIXES = [ "str", "hd" ];

// Duration in milliseconds to poll for the Watch Now modal button. Matches the dismiss poll constants in video.ts.
const WATCH_NOW_POLL_DURATION = 5000;

// Interval in milliseconds between Watch Now modal poll checks. Matches the dismiss poll constants in video.ts.
const WATCH_NOW_POLL_INTERVAL = 500;

// Unified channel cache entry combining discovery metadata and tuning data.
interface XfinityChannelEntry {

  // CallSign for SPA channel lookup (e.g., "CNNHD", "ESPND").
  callSign: string;

  // Channel number from the channelmap API. Used to resolve ties between multiple local broadcast affiliates sharing the same network (e.g., KCTSD ch 109 vs
  // KBTCD ch 108 for PBS) — lowest channel number wins.
  channelNumber: number;

  // Discovery output for the channels API.
  discovered: DiscoveredChannel;
}

/* Raw channel structure from the channelmap API response. Only the fields we consume are typed here. The API returns many more fields per channel — we omit them
 * rather than maintaining a complete schema for an external API that may evolve.
 */
interface XfinityApiChannel {

  "branchOf/company/callSign"?: string;
  callSign?: string;
  callSignVoiceOverHint?: string;
  entitled?: boolean;
  isHD?: boolean;
  isTve?: boolean;
  number?: number;
}

// Channelmap API response structure.
interface XfinityChannelmapResponse {

  _embedded?: {

    channels?: XfinityApiChannel[];
  };
}

// Unified channel cache. Maps lowercased lookup keys to combined discovery and tuning data. Multiple keys may reference the same entry (callSign, stripped callSign,
// branchOf name). Populated during channelmap API interception or discovery. Cleared on browser disconnect via clearXfinityCache().
const xfinityChannelCache = new Map<string, XfinityChannelEntry>();

// Tracks whether the channel cache has been fully enumerated from a complete channelmap API response. Guards getCachedChannels to avoid returning partial data.
let xfinityFullyEnumerated = false;

// Cached channelmap API response for CDP request interception on warm tunes. Stored on the first successful API response. The exact URL and headers are replayed so
// that only the correct request is intercepted (the SPA makes multiple requests to URLs matching the channelmap pattern, but only one is the actual channel lineup).
let cachedChannelmapBody: Nullable<string> = null;
let cachedChannelmapUrl: Nullable<string> = null;
let cachedChannelmapHeaders: Record<string, string> = {};

// Tracks which pages have response interception listeners registered to avoid duplicate registrations.
const pagesWithListeners = new WeakSet<Page>();

/**
 * Strips common Xfinity callSign suffixes to produce a normalized lookup key. Strips the first matching suffix from the end of the callSign, but only when the
 * remaining string is at least 2 characters. Examples: CNNHD→CNN, AESTR→AE.
 * @param callSign - The raw callSign from the channelmap API.
 * @returns The stripped callSign, or the original if no suffix matched or stripping would produce a string shorter than 2 characters.
 */
function stripCallSignSuffix(callSign: string): string {

  const lower = callSign.toLowerCase();

  for(const suffix of CALLSIGN_SUFFIXES) {

    if(lower.endsWith(suffix) && ((lower.length - suffix.length) >= 2)) {

      return lower.slice(0, -suffix.length);
    }
  }

  return lower;
}

/**
 * Caches a single channel entry with multi-key lookups. Creates up to four cache keys per channel: (a) lowercased callSign, (b) stripped callSign with common
 * suffixes removed, (c) lowercased branchOf/company/callSign, and (d) broadcast network abbreviation for local affiliates. All keys use first-write-wins except
 * broadcast network keys, which are unconditionally written so local affiliates override national feeds.
 * @param callSign - The raw callSign string (already validated non-null by caller).
 * @param branchOf - The branchOf/company/callSign value from the API.
 * @param entry - The unified cache entry to store.
 * @param affiliate - The broadcast network abbreviation (e.g., "NBC") or undefined for non-broadcast channels.
 * @returns True if the entry was cached (at least one key written), false if all keys were already occupied.
 */
function cacheChannelEntry(callSign: string, branchOf: string, entry: XfinityChannelEntry, affiliate: string | undefined): boolean {

  const callSignLower = callSign.toLowerCase();
  let keysWritten = 0;

  // Primary key: lowercased callSign (e.g., "cnnhd").
  if(!xfinityChannelCache.has(callSignLower)) {

    xfinityChannelCache.set(callSignLower, entry);
    keysWritten++;
  }

  // Secondary key: stripped callSign with common suffixes removed (e.g., "cnn" from "cnnhd").
  const strippedKey = stripCallSignSuffix(callSign);

  if((strippedKey !== callSignLower) && !xfinityChannelCache.has(strippedKey)) {

    xfinityChannelCache.set(strippedKey, entry);
    keysWritten++;
  }

  // Tertiary key: lowercased branchOf/company/callSign (e.g., "cnn", "animal planet").
  if(branchOf) {

    const branchKey = branchOf.toLowerCase();

    if((branchKey !== callSignLower) && (branchKey !== strippedKey) && !xfinityChannelCache.has(branchKey)) {

      xfinityChannelCache.set(branchKey, entry);
      keysWritten++;
    }
  }

  // Broadcast network key (e.g., "nbc", "abc"). Unconditional write ensures local affiliates override any previously-cached national entry for the same network.
  if(affiliate) {

    xfinityChannelCache.set(affiliate.toLowerCase(), entry);
    keysWritten++;
  }

  return keysWritten > 0;
}

/**
 * Processes a channelmap API response and populates the unified channel cache using three-pass tiered filtering. Channels are prioritized: non-TVE HD entitled
 * first (cable HD — optimal for a home-network server), then TVE entitled (fallback for channels without a cable HD entry), then non-TVE SD entitled (safety net
 * for SD-only channels). All passes share a single seenCallSigns set for exact callSign deduplication. Cross-tier dedup is handled naturally by
 * cacheChannelEntry's first-write-wins guards — lower-tier entries cannot overwrite cache keys already occupied by higher-tier entries.
 *
 * Local affiliates are detected via callSignVoiceOverHint matching against BROADCAST_HINTS and preferred over national feeds (e.g., local KOMOD over national
 * WLSD1 for ABC). Local identification uses a pre-scan of non-TVE entries — stations present in the non-TVE lineup are local to the subscriber's market.
 * Sub-feeds (DATLN, TODAY) sharing the same voiceOverHint as the primary affiliate are deduplicated by the broadcast network key guard.
 * @param data - The parsed channelmap API response.
 */
function processChannelmapResponse(data: XfinityChannelmapResponse): void {

  const channels = data._embedded?.channels;

  if(!Array.isArray(channels)) {

    return;
  }

  // Pre-scan for non-TVE local broadcast callSigns. These identify which entries are local affiliates vs national feeds. Non-TVE entries are always local to the
  // subscriber's market, so an entry whose callSign also appears in the non-TVE lineup is the local affiliate for that broadcast network.
  const localBroadcastCallSigns = new Set<string>();

  for(const channel of channels) {

    if(channel.isTve || !channel.callSign || !channel.callSignVoiceOverHint) {

      continue;
    }

    const hint = channel.callSignVoiceOverHint.toLowerCase();

    if(BROADCAST_HINTS[hint]) {

      localBroadcastCallSigns.add(channel.callSign.toLowerCase());
    }
  }

  // Track callSigns we have already processed to enforce first-write-wins deduplication across all passes.
  const seenCallSigns = new Set<string>();
  let count = 0;

  // Three-pass tiered filtering. Each pass processes channels matching specific criteria. All passes share seenCallSigns for exact callSign dedup and use the
  // same entry creation and caching logic. Cross-tier dedup is handled by cacheChannelEntry's first-write-wins guards on stripped callSign and branchOf keys.
  const passes: { filter: (ch: XfinityApiChannel) => boolean }[] = [

    // Pass 1: Non-TVE HD entitled. Cable HD channels — the primary version for a home-network server. Includes channels like PBS (KCTSD) and CW (KSTWD) that
    // have no TVE entries at all, plus HD cable variants of channels that also have TVE entries.
    { filter: (ch): boolean => !ch.isTve && (ch.isHD === true) && (ch.entitled === true) },

    // Pass 2: TVE entitled. TV Everywhere channels that work from any network. Serves as fallback for channels without a cable HD entry.
    { filter: (ch): boolean => (ch.isTve === true) && (ch.entitled === true) },

    // Pass 3: Non-TVE SD entitled. Safety net for SD-only channels with no HD or TVE variant (e.g., PBS World, local digital subchannels).
    { filter: (ch): boolean => !ch.isTve && !ch.isHD && (ch.entitled === true) }
  ];

  for(const pass of passes) {

    for(const channel of channels) {

      if(!channel.callSign || !pass.filter(channel)) {

        continue;
      }

      const callSignLower = channel.callSign.toLowerCase();

      // Deduplicate by callSign — first occurrence wins (typically the lowest channel number).
      if(seenCallSigns.has(callSignLower)) {

        continue;
      }

      seenCallSigns.add(callSignLower);

      // Skip audio-only channels (Music Choice, Stingray) — they produce static screens with music and are not useful for video capture.
      const branchOf = channel["branchOf/company/callSign"] ?? "";
      const branchLower = branchOf.toLowerCase();

      if(AUDIO_ONLY_PREFIXES.some((prefix) => branchLower.startsWith(prefix))) {

        continue;
      }

      // Determine the display name. For broadcast affiliates identified by callSignVoiceOverHint, use the network abbreviation. For cable channels, use the
      // branchOf/company/callSign value (the network's display name). The channelSelector always uses the raw callSign for guaranteed uniqueness — branchOf
      // names like "Cinemax" or "BET" are shared across multiple distinct channels (MAXHD, MXPLD, MXACD, etc.).
      const voiceOverHint = (channel.callSignVoiceOverHint ?? "").toLowerCase();
      let displayName = (branchOf.length > 0) ? branchOf : (channel.callSignVoiceOverHint ?? channel.callSign);
      let affiliate: string | undefined;

      const broadcastNetwork = BROADCAST_HINTS[voiceOverHint];

      if(broadcastNetwork) {

        // Broadcast affiliate handling: prefer local affiliates over national feeds. The channelmap API may include both a national feed (e.g., WLSD1 for
        // ABC) and the subscriber's local affiliate (e.g., KOMOD for ABC). We identify local affiliates by checking whether their callSign appears in the
        // non-TVE entries, which are always local to the subscriber's market.
        const networkKey = broadcastNetwork.toLowerCase();
        const isLocal = localBroadcastCallSigns.has(callSignLower);
        const existingEntry = xfinityChannelCache.get(networkKey);

        if(existingEntry) {

          // Network already cached. Override only when this entry is a better match: local replacing national, or lower channel number when both are local
          // (e.g., KBTCD ch 108 over KCTSD ch 109 for PBS).
          const existingIsLocal = localBroadcastCallSigns.has(existingEntry.callSign.toLowerCase());
          const newNumber = channel.number ?? Infinity;

          if(!isLocal || (existingIsLocal && (newNumber >= existingEntry.channelNumber))) {

            continue;
          }

          // Better affiliate found: remove the old entry's cache keys so it doesn't appear as a duplicate in channel discovery.
          const oldCallSign = existingEntry.callSign.toLowerCase();

          xfinityChannelCache.delete(oldCallSign);

          const oldStripped = stripCallSignSuffix(existingEntry.callSign);

          if(oldStripped !== oldCallSign) {

            xfinityChannelCache.delete(oldStripped);
          }

          count--;
        }

        displayName = broadcastNetwork;
        affiliate = broadcastNetwork;
      }

      const discovered: DiscoveredChannel = {

        ...(affiliate ? { affiliate } : {}),
        channelSelector: channel.callSign,
        name: displayName
      };

      const entry: XfinityChannelEntry = {

        callSign: channel.callSign,
        channelNumber: channel.number ?? Infinity,
        discovered
      };

      if(cacheChannelEntry(channel.callSign, branchOf, entry, affiliate)) {

        count++;
      }
    }
  }

  if(count > 0) {

    xfinityFullyEnumerated = true;

    LOG.debug("tuning:xfinity", "Channelmap API: cached %s channels (%s cache keys).", count, xfinityChannelCache.size);
  }
}

/**
 * Sets up response interception on the page to capture Xfinity's channelmap API response. The API fires automatically on guide page load and returns the complete
 * channel lineup in a single response. Uses a WeakSet to prevent duplicate listener registration on the same page.
 * @param page - The Puppeteer page object.
 */
function setupChannelmapInterception(page: Page): void {

  if(pagesWithListeners.has(page)) {

    return;
  }

  pagesWithListeners.add(page);

  page.on("response", (response) => {

    const url = response.url();

    if(!url.includes(CHANNELMAP_API_PATTERN) || (response.status() !== 200)) {

      return;
    }

    void response.text().then((text: string) => {

      // Cache the exact URL, headers, and body for CDP request interception on warm tunes. The exact URL ensures we only intercept the correct channelmap request —
      // the SPA makes multiple requests to URLs matching the pattern, but only this one returns the channel lineup. We strip content-encoding and content-length
      // because response.text() returns the decoded (decompressed) body — replaying encoding headers with decoded content would cause double-decompression.
      cachedChannelmapUrl = url;
      cachedChannelmapBody = text;

      const headers = response.headers();

      delete headers["content-encoding"];
      delete headers["content-length"];

      cachedChannelmapHeaders = headers;

      const data = JSON.parse(text) as XfinityChannelmapResponse;

      processChannelmapResponse(data);
    }).catch(() => {

      // Response parsing failed — cold cache fallback handles it.
    });
  });
}

/**
 * Looks up a channel in the unified cache using three-tier matching:
 *
 * 1. Exact match: cache key equals the lowercased input (matches callSigns, stripped callSigns, and branchOf names).
 * 2. Suffix-tolerant: input + "hd" or input + "d" matches a cache key (e.g., "cnn" → "cnnhd", "espn" → "espnd").
 * 3. Display name iteration: iterate all cache entries, check if discovered.name (lowercased) equals input. Catches verbose display names not covered by cache keys.
 *
 * When a non-exact match succeeds, the result is cached under the input key for O(1) lookup on subsequent calls.
 * @param channelName - The channelSelector value (e.g., "CNN", "CNNHD", "Animal Planet").
 * @returns The matching cache entry or null if no match is found.
 */
function findXfinityChannel(channelName: string): Nullable<XfinityChannelEntry> {

  const lower = channelName.toLowerCase();

  // Tier 1: Exact match on any cache key (callSigns, stripped names, branchOf names).
  const exact = xfinityChannelCache.get(lower);

  if(exact) {

    return exact;
  }

  // Tier 2: Suffix-tolerant. Try appending common technology suffixes.
  const hdMatch = xfinityChannelCache.get(lower + "hd");

  if(hdMatch) {

    xfinityChannelCache.set(lower, hdMatch);

    return hdMatch;
  }

  const dMatch = xfinityChannelCache.get(lower + "d");

  if(dMatch) {

    xfinityChannelCache.set(lower, dMatch);

    return dMatch;
  }

  // Tier 3: Display name iteration. Check if any entry's discovered.name matches the input. This catches verbose display names (e.g., "Cable News Network") that
  // may not have a matching cache key.
  const seen = new Set<XfinityChannelEntry>();

  for(const entry of xfinityChannelCache.values()) {

    if(seen.has(entry)) {

      continue;
    }

    seen.add(entry);

    if(entry.discovered.name.toLowerCase() === lower) {

      xfinityChannelCache.set(lower, entry);

      return entry;
    }
  }

  return null;
}

/**
 * Sets up channelmap API interception for the upcoming page navigation and, on warm tunes, enables CDP request interception to serve the cached channelmap response
 * instantly. Like DirecTV's resolveDirectUrl, this always returns null — all tuning happens in the strategy's execute function via `_watchChannelEventHandler`.
 * @param _channelSelector - The channel selector string (unused — channel lookup happens in the strategy).
 * @param page - The Puppeteer page for response interception setup.
 * @returns Always null — no direct URL navigation.
 */
async function resolveXfinityDirectUrl(_channelSelector: string, page: Page): Promise<Nullable<string>> {

  // Set up response interception so the channelmap API response is captured during guide page navigation.
  setupChannelmapInterception(page);

  // On warm cache, intercept the channelmap API request and serve the cached response immediately. This eliminates the 3-5s network round-trip on warm tunes. We
  // match on the exact URL captured during the cold tune — the SPA makes multiple requests to URLs matching the channelmap pattern, but only one is the channel lineup.
  if(cachedChannelmapBody && cachedChannelmapUrl) {

    const targetUrl = cachedChannelmapUrl;
    const body = cachedChannelmapBody;
    const headers = cachedChannelmapHeaders;

    await page.setRequestInterception(true);

    page.on("request", (request) => {

      if(request.url() === targetUrl) {

        LOG.debug("tuning:xfinity", "Channelmap API: served from cache.");

        void request.respond({

          body,
          headers,
          status: 200
        }).catch(() => {

          // Serve failed — fall back to the network so the cold-cache path takes over.
          void request.continue().catch(() => {

            // Page closed — ignore.
          });
        });

        return;
      }

      void request.continue().catch(() => {

        // Page closed — ignore.
      });
    });
  }

  return null;
}

/**
 * Polls for a visible "Watch Now" modal button and clicks it if found. Some entertainment channels (e.g., Discovery) display this modal after
 * `_watchChannelEventHandler` switches channels. News channels (e.g., CNN) auto-play without any modal. Runs as a fire-and-forget background task alongside
 * `waitForVideoReady` — channels without modals resolve via `waitForVideoReady` alone, while channels with modals get unblocked by the click.
 *
 * The button requires JS-based detection because multiple `button.style-scope.tv-button` elements exist on the page with zero dimensions. We filter by text
 * content ("Watch Now") and non-zero bounding rect to find the visible one.
 * @param page - The Puppeteer page object.
 */
async function watchNowModalPoll(page: Page): Promise<void> {

  const checks = Math.ceil(WATCH_NOW_POLL_DURATION / WATCH_NOW_POLL_INTERVAL);

  for(let i = 0; i < checks; i++) {

    // Delay between checks, but not before the first one — the first check is immediate.
    if(i > 0) {

      // eslint-disable-next-line no-await-in-loop
      await delay(WATCH_NOW_POLL_INTERVAL);
    }

    try {

      // eslint-disable-next-line no-await-in-loop
      const clicked = await page.evaluate((): boolean => {

        const buttons = Array.from(document.querySelectorAll("button.style-scope.tv-button"));

        for(const button of buttons) {

          const text = button.textContent.trim();
          const rect = button.getBoundingClientRect();

          if((text === "Watch Now") && (rect.width > 0)) {

            (button as HTMLElement).click();

            return true;
          }
        }

        return false;
      });

      if(clicked) {

        LOG.debug("tuning:xfinity", "Watch Now modal dismissed.");

        return;
      }
    } catch {

      // Page closed or navigated away — exit silently.
      return;
    }
  }
}

/**
 * In-page SPA tuning strategy for Xfinity Stream. Waits for the Polymer SPA's `TV-APP` element and its `channelMap.channels` to populate, then invokes
 * `_watchChannelEventHandler(null, { channel })` to switch channels in-page. This is the execute path for both warm and cold cache scenarios — the guide URL
 * navigation loads the SPA, and this strategy performs the channel switch within it.
 * @param page - The Puppeteer page object (on the guide page after SPA load).
 * @param profile - The resolved site profile with a non-null channelSelector.
 * @returns Result object with success status and optional failure reason.
 */
async function xfinityDirectStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const channelName = profile.channelSelector;
  const timeout = CONFIG.streaming.videoTimeout;

  // Wait for the TV-APP Polymer element to be present in the DOM.
  try {

    await page.waitForSelector("tv-app", { timeout });
  } catch {

    return { reason: "Xfinity Stream guide page did not load within timeout.", success: false };
  }

  // Wait for the SPA's channelMap.channels to populate. The channelmap API fires during page load and the SPA stores the response in this property.
  try {

    /* eslint-disable @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument,
       @typescript-eslint/no-unsafe-return, @typescript-eslint/no-explicit-any */
    await page.waitForFunction((): boolean => {

      const tvApp = document.querySelector("tv-app");

      if(!tvApp) {

        return false;
      }

      const channels = (tvApp as any).channelMap?.channels;

      return channels && (typeof channels === "object") && (Object.keys(channels).length > 0);
    }, { timeout });
    /* eslint-enable @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument,
       @typescript-eslint/no-unsafe-return, @typescript-eslint/no-explicit-any */
  } catch {

    // If we served a cached response and the SPA couldn't process it, the cached data is likely stale or corrupt. Clear it so the next tune falls through to a
    // fresh network fetch.
    invalidateCachedResponse();

    return { reason: "Xfinity Stream channel lineup did not load within timeout.", success: false };
  }

  // Poll for Node-side cache population. The channelmap API response is intercepted by setupChannelmapInterception (called by resolveXfinityDirectUrl before we
  // got here). We need the Node-side cache for findXfinityChannel's three-tier matching.
  const deadline = Date.now() + timeout;

  while((xfinityChannelCache.size === 0) && (Date.now() < deadline)) {

    // eslint-disable-next-line no-await-in-loop
    await delay(CACHE_POLL_INTERVAL);
  }

  if(xfinityChannelCache.size === 0) {

    return { reason: "Xfinity Stream channel lineup API did not respond within timeout.", success: false };
  }

  // Look up the target channel in the Node-side cache.
  const entry = findXfinityChannel(channelName);

  if(!entry) {

    logAvailableChannels({

      availableChannels: buildXfinityDiscoveredChannels().map((ch) => ch.name + " (" + ch.channelSelector + ")").sort(),
      channelName,
      guideUrl: XFINITY_GUIDE_URL,
      presetSuffix: "-xfinity",
      providerName: "Xfinity Stream"
    });

    return { reason: "Channel " + channelName + " was not found in the Xfinity Stream channel lineup.", success: false };
  }

  // Call _watchChannelEventHandler on the TV-APP element to switch channels in-page. The SPA's channelMap.channels is keyed by internal ID tags, not callSigns,
  // so we iterate the values to find the matching callSign.
  const targetCallSign = entry.callSign;

  try {

    /* eslint-disable @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument,
       @typescript-eslint/no-unsafe-call, @typescript-eslint/no-explicit-any */
    const found = await page.evaluate((callSign: string): boolean => {

      const tvApp = document.querySelector("tv-app");

      if(!tvApp) {

        return false;
      }

      const channelMap = (tvApp as any).channelMap?.channels;

      if(!channelMap) {

        return false;
      }

      // Find the channel object by matching callSign. The channelMap is keyed by internal ID tags, so we iterate values.
      for(const channel of Object.values(channelMap)) {

        if((channel as any)?.callSign === callSign) {

          (tvApp as any)._watchChannelEventHandler(null, { channel });

          return true;
        }
      }

      return false;
    }, targetCallSign);
    /* eslint-enable @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument,
       @typescript-eslint/no-unsafe-call, @typescript-eslint/no-explicit-any */

    if(!found) {

      // The Node-side cache had this callSign but the SPA's channelMap doesn't — the cached response is stale. Clear it so the next tune fetches fresh data.
      invalidateCachedResponse();

      return { reason: "Channel " + targetCallSign + " is no longer available in the Xfinity Stream channel lineup.", success: false };
    }
  } catch(error) {

    return { reason: "Failed to switch Xfinity Stream channel: " + formatError(error) + ".", success: false };
  }

  LOG.debug("tuning:xfinity", "Invoked _watchChannelEventHandler for %s (callSign: %s).", channelName, targetCallSign);

  // Launch fire-and-forget Watch Now modal poll. Some entertainment channels show a modal after channel switch that must be clicked to start playback.
  void watchNowModalPoll(page);

  return { success: true };
}

/**
 * Derives a deduplicated DiscoveredChannel array from the unified channel cache. Multiple cache keys may point to the same XfinityChannelEntry, so we deduplicate
 * via Set reference equality. Sorts by name before returning.
 * @returns Sorted, deduplicated array of discovered channels.
 */
function buildXfinityDiscoveredChannels(): DiscoveredChannel[] {

  const channels: DiscoveredChannel[] = [];
  const seen = new Set<XfinityChannelEntry>();

  for(const entry of xfinityChannelCache.values()) {

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
 * Discovers all channels from Xfinity Stream by navigating to the guide page and intercepting the channelmap API response. Returns cached results if the cache is
 * already fully populated. Sets handlesOwnNavigation so the route handler does not navigate before calling this function.
 * @param page - The Puppeteer page object (fresh page, not yet navigated).
 * @returns Array of discovered channels.
 */
async function discoverXfinityChannels(page: Page): Promise<DiscoveredChannel[]> {

  // Return from cache if already fully enumerated.
  if(xfinityFullyEnumerated && (xfinityChannelCache.size > 0)) {

    return buildXfinityDiscoveredChannels();
  }

  // Set up response interception before navigation so we capture the channelmap API response during page load.
  setupChannelmapInterception(page);

  try {

    await page.goto(XFINITY_GUIDE_URL, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
  } catch(error) {

    LOG.warn("Failed to load Xfinity Stream guide page: %s.", formatError(error));

    return [];
  }

  // Poll for cache population.
  const deadline = Date.now() + CONFIG.streaming.videoTimeout;

  while((xfinityChannelCache.size === 0) && (Date.now() < deadline)) {

    // eslint-disable-next-line no-await-in-loop
    await delay(CACHE_POLL_INTERVAL);
  }

  if(xfinityChannelCache.size === 0) {

    LOG.warn("Xfinity Stream channel lineup did not return any channels.");

    return [];
  }

  return buildXfinityDiscoveredChannels();
}

/**
 * Returns cached discovered channels from the unified channel cache, or null if the cache is empty or has not been fully enumerated.
 * @returns Sorted array of discovered channels or null.
 */
function getXfinityCachedChannels(): Nullable<DiscoveredChannel[]> {

  if(!xfinityFullyEnumerated || (xfinityChannelCache.size === 0)) {

    return null;
  }

  return buildXfinityDiscoveredChannels();
}

/**
 * Clears the cached channelmap API response so the next tune fetches fresh data from the network. Called when the strategy detects that the cached response produced
 * a failure — either the SPA couldn't populate channelMap.channels, or a callSign present in the Node-side cache was missing from the SPA's channelMap. The current
 * tune still fails (the channelmap request already fired), but the next tune self-heals via the cold-cache path.
 */
function invalidateCachedResponse(): void {

  if(!cachedChannelmapBody) {

    return;
  }

  cachedChannelmapBody = null;
  cachedChannelmapHeaders = {};
  cachedChannelmapUrl = null;

  LOG.debug("tuning:xfinity", "Channelmap API: invalidated cached response — next tune will fetch from network.");
}

/**
 * Clears the Xfinity channel cache. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts, since a fresh browser session may have
 * different channel availability.
 */
function clearXfinityCache(): void {

  cachedChannelmapBody = null;
  cachedChannelmapHeaders = {};
  cachedChannelmapUrl = null;
  xfinityChannelCache.clear();
  xfinityFullyEnumerated = false;
}

/**
 * Invalidates the cached channel entry for the given channel selector. Called when a stream fails to start after tuning, allowing the next tune attempt to
 * re-resolve via fresh channelmap data.
 * @param channelSelector - The channel selector string to invalidate.
 */
function invalidateXfinityDirectUrl(channelSelector: string): void {

  xfinityChannelCache.delete(channelSelector.toLowerCase());
}

export const xfinityProvider: ProviderModule = {

  discoverChannels: discoverXfinityChannels,
  getCachedChannels: getXfinityCachedChannels,
  guideUrl: XFINITY_GUIDE_URL,
  handlesOwnNavigation: true,
  label: "Xfinity Stream",
  slug: "xfinity",
  strategy: {

    clearCache: clearXfinityCache,
    execute: xfinityDirectStrategy,
    invalidateDirectUrl: invalidateXfinityDirectUrl,
    resolveDirectUrl: resolveXfinityDirectUrl
  },
  strategyName: "xfinityDirect",
  tinySegmentThreshold: 150
};
