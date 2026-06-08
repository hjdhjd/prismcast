/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * comcastPolymer.ts: Shared Comcast Polymer SPA factory for providers built on the TV-APP platform (Xfinity Stream, Cox Contour TV).
 */
import type { ChannelSelectionProfile, ChannelSelectionStrategy, ChannelSelectorResult, DiscoveredChannel, Nullable, ProviderModule } from "../../types/index.ts";
import { LOG, delay, formatError } from "../../utils/index.ts";
import { installOncePerPage, logAvailableChannels } from "./shared.ts";
import { CONFIG } from "../../config/index.ts";
import type { Page } from "puppeteer-core";

/* Comcast's Polymer SPA (`TV-APP`) manages channel playback via an internal `channelMap` object. The `channelMap.channels` property is populated from the channelmap
 * API during page load and contains the complete channel lineup. Calling `_watchChannelEventHandler(null, { channel })` on the `TV-APP` element switches channels
 * in-page in ~2-3 seconds - matching native browser performance. This avoids the ~15-second cost of full page navigation.
 *
 * The channelmap API at xtvapi.cloudtv.comcast.net returns the complete channel lineup in a single response during guide page load. We intercept this response to
 * populate a Node-side channel cache for name matching and channel discovery. At tune time, the strategy waits for the SPA's `channelMap.channels` to populate,
 * looks up the target channel by callSign, and invokes `_watchChannelEventHandler` via `page.evaluate()`.
 *
 * Some entertainment channels (e.g., Discovery) display a "Watch Now" modal button after the SPA switches channels. A fire-and-forget poll detects and clicks this
 * button concurrently with `waitForVideoReady`. News channels (e.g., CNN) auto-play without any modal.
 *
 * Multiple providers (Xfinity Stream, Cox Contour TV) share this Polymer SPA platform. The `createComcastPolymerProvider` factory creates isolated provider instances
 * with separate channel caches and channelmap response caches, parameterized by guide URL, branding, and strategy name.
 *
 * Tuning flow:
 * 1. resolveDirectUrl: sets up channelmap API interception and returns null. On warm tunes, also enables CDP request interception to serve the cached channelmap
 *    response instantly, eliminating the 3-5s network round-trip.
 * 2. The caller (tuneToChannel in video.ts) navigates to the guide URL, loading the Polymer SPA.
 * 3. directStrategy: waits for `TV-APP.channelMap.channels` to populate, finds the target channel by callSign, and calls `_watchChannelEventHandler`.
 * 4. A fire-and-forget poll watches for a visible "Watch Now" modal button and clicks it if it appears.
 * 5. initializePlayback continues with waitForVideoReady, fullscreen, etc.
 */

// Narrow type for the Comcast TV-APP Polymer Web Component. The actual element has a much broader API, but we only access these properties. Used in page.evaluate()
// calls to provide type-safe access to the Polymer element's internal properties. The channels property is typed as nullable because the Polymer SPA sets it to null
// during initialization before the channelmap API response populates it...the runtime value passes through undefined -> null -> empty object -> populated object.
interface TvApp {

  channelMap?: { channels?: Record<string, { callSign?: string } | null> | null };
  _watchChannelEventHandler: (event: null, data: { channel: unknown }) => void;
}

// URL pattern for the channelmap API response. Matched against response URLs to identify channelmap data. Shared across all Comcast Polymer providers - the same
// backend serves both Xfinity and Cox subscribers, returning subscriber-specific entitled channel lineups based on authentication.
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

// Base technology suffixes for Comcast callSigns. Each base suffix may have a Pacific timezone variant with "p" appended (e.g., "hd" -> "hdp", "h" -> "hp").
// Single-character base suffixes also support a reverse convention where "p" is prepended (e.g., "h" -> "ph") - observed in STARZ Encore channels (STZEAPH).
// The reverse convention only applies to single-character suffixes; multi-character suffixes always use the forward convention (e.g., "hdp" not "phd").
// CALLSIGN_SUFFIXES is derived from this array with all Pacific variants and base suffixes sorted by length descending to ensure longest-match-first stripping
// (e.g., CNNHDP must strip "hdp" to CNN, not "hd" to CNNP). Intentionally excludes "d" as a base suffix - it appears as a meaningful part of callSigns (ESPND)
// rather than a pure technology suffix, causing cache key collisions with unrelated channels.
const BASE_SUFFIXES = [ "str", "hd", "h", "dt" ];

// Full suffix list for stripping, derived from BASE_SUFFIXES. Forward Pacific variants (base + "p") are generated for all base suffixes. Reverse Pacific
// variants ("p" + base) are generated only for single-character base suffixes where the convention is observed. Sorted by length descending to ensure
// longest-match-first ordering, with alphabetical tiebreaking within the same length.
const CALLSIGN_SUFFIXES =
  [...new Set([
    ...BASE_SUFFIXES.map((s) => s + "p"),
    ...BASE_SUFFIXES.filter((s) => s.length === 1).map((s) => "p" + s),
    ...BASE_SUFFIXES
  ])].sort((a, b) => (b.length - a.length) || a.localeCompare(b));

// Duration in milliseconds to poll for the Watch Now modal button. Matches the dismiss poll constants in video.ts.
const WATCH_NOW_POLL_DURATION = 5000;

// Interval in milliseconds between Watch Now modal poll checks. Matches the dismiss poll constants in video.ts.
const WATCH_NOW_POLL_INTERVAL = 500;

// Unified channel cache entry combining discovery metadata and tuning data.
interface ChannelEntry {

  // CallSign for SPA channel lookup (e.g., "CNNHD", "ESPND", "CNNHDP").
  callSign: string;

  // Channel number from the channelmap API. Used to resolve ties between multiple local broadcast affiliates sharing the same network (e.g., KCTSD ch 109 vs
  // KBTCD ch 108 for PBS) - lowest channel number wins.
  channelNumber: number;

  // Discovery output for the channels API.
  discovered: DiscoveredChannel;

  // True for Pacific timezone variant entries (callSign ends with a Pacific suffix like "hdp", "hp", or "ph"). Used by cacheChannelEntry to determine priority
  // for shared cache keys - non-Pacific entries override Pacific entries on stripped and branchOf keys, ensuring that display-name-based lookups (e.g., "A&E")
  // resolve to the primary East feed rather than the Pacific variant.
  pacific: boolean;
}

/* Raw channel structure from the channelmap API response. Only the fields we consume are typed here. The API returns many more fields per channel - we omit them
 * rather than maintaining a complete schema for an external API that may evolve.
 */
interface ApiChannel {

  "branchOf/company/callSign"?: string;
  callSign?: string;
  callSignVoiceOverHint?: string;
  entitled?: boolean;
  isHD?: boolean;
  isTve?: boolean;
  number?: number;
}

// Channelmap API response structure.
interface ChannelmapResponse {

  _embedded?: {

    channels?: ApiChannel[];
  };
}

/**
 * Configuration for creating a Comcast Polymer SPA provider instance. Each field parameterizes a provider-specific value that differs between Xfinity Stream
 * and Cox Contour TV while sharing all underlying Polymer SPA logic.
 */
export interface ComcastPolymerProviderConfig {

  // Debug log category (e.g., "tuning:xfinity", "tuning:cox"). Must have a corresponding entry in DEBUG_CATEGORIES in debugFilter.ts.
  debugCategory: string;

  // Guide page URL where the channelmap API fires automatically on page load (e.g., "https://www.xfinity.com/stream/listings").
  guideUrl: string;

  // Human-readable provider display name (e.g., "Xfinity Stream", "Cox Contour TV").
  label: string;

  // Channel preset suffix for logAvailableChannels (e.g., "-xfinity", "-cox").
  presetSuffix: string;

  // Profile description text shown in the provider setup UI.
  profileDescription: string;

  // Profile name for DOMAIN_CONFIG registration (e.g., "xfinityStream", "coxStream").
  profileName: string;

  // Profile summary text shown in profile selection.
  profileSummary: string;

  // Provider slug for API endpoints and provider filter matching (e.g., "xfinity", "cox").
  slug: string;

  // Channel selection strategy name for the site profile (e.g., "xfinityDirect", "coxDirect").
  strategyName: ChannelSelectionStrategy;
}

/**
 * Strips common Comcast callSign suffixes to produce a normalized lookup key. Strips the first matching suffix from the end of the callSign, but only when the
 * remaining string is at least 2 characters. Examples: CNNHD->CNN, AESTR->AE, CNNHDP->CNN, BRAVOHP->BRAVO, STZEAPH->STZEA, KNXVDTP->KNXV.
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
 * Determines whether a callSign represents a Pacific timezone variant. A callSign is Pacific if it ends with "p" adjacent to a recognized base technology
 * suffix: forward (base + "p", e.g., "hdp", "hp") for all base suffixes, or reverse ("p" + base, e.g., "ph") for single-character base suffixes only. Both
 * conventions appear in the Comcast channelmap - AETVHDP and BRAVOHP use the forward convention, while STZEAPH uses the reverse convention. Derived from
 * BASE_SUFFIXES so adding a new technology suffix automatically enables Pacific detection.
 * @param callSign - The raw callSign from the channelmap API.
 * @returns True if the callSign ends with a recognized Pacific suffix (e.g., "hdp", "hp", "ph").
 */
function isPacificCallSign(callSign: string): boolean {

  const lower = callSign.toLowerCase();

  return BASE_SUFFIXES.some((base) => {

    const forward = base + "p";

    if(lower.endsWith(forward) && ((lower.length - forward.length) >= 2)) {

      return true;
    }

    // Reverse convention (e.g., "ph") only applies to single-character base suffixes. Multi-character reverse suffixes like "phd" would falsely match
    // callSigns whose base happens to end in "p" (e.g., a hypothetical ALPHD matching "p" + "hd").
    if(base.length === 1) {

      const reverse = "p" + base;

      return lower.endsWith(reverse) && ((lower.length - reverse.length) >= 2);
    }

    return false;
  });
}

/**
 * Creates a complete ProviderModule for a Comcast Polymer SPA provider. Each invocation creates isolated per-provider state (channel cache, channelmap response
 * cache, enumeration flag) so multiple providers sharing the same platform do not interfere with each other.
 * @param config - Provider-specific configuration values.
 * @returns A fully configured ProviderModule ready for registration in the coordinator.
 */
export function createComcastPolymerProvider(config: ComcastPolymerProviderConfig): ProviderModule {

  // Per-provider mutable state. Each provider instance gets its own channel cache and channelmap response cache, isolated via closure scope.

  // Unified channel cache. Maps lowercased lookup keys to combined discovery and tuning data. Multiple keys may reference the same entry (callSign, stripped
  // callSign, branchOf name). Populated during channelmap API interception or discovery. Cleared on browser disconnect via clearCache().
  const channelCache = new Map<string, ChannelEntry>();

  // Tracks whether the channel cache has been fully enumerated from a complete channelmap API response. Guards getCachedChannels to avoid returning partial data.
  let fullyEnumerated = false;

  // Cached channelmap API response for CDP request interception on warm tunes. Stored on the first successful API response. The exact URL and headers are replayed
  // so that only the correct request is intercepted (the SPA makes multiple requests to URLs matching the channelmap pattern, but only one is the actual channel
  // lineup).
  let cachedBody: Nullable<string> = null;
  let cachedUrl: Nullable<string> = null;
  let cachedHeaders: Record<string, string> = {};

  // Tracks which pages have response interception listeners registered to avoid duplicate registrations.
  const pagesWithListeners = new WeakSet<Page>();

  /**
   * Determines whether a non-Pacific entry should override an existing Pacific entry on a shared cache key. Shared keys (stripped callSign, branchOf) should
   * resolve to the primary East feed, not the Pacific variant. When a non-Pacific entry encounters a shared key held by a Pacific entry, the non-Pacific entry
   * takes priority. This mirrors the broadcast affiliate pattern where local entries override national entries.
   * @param existingEntry - The entry currently holding the cache key, or undefined if the key is unclaimed.
   * @param newEntry - The entry attempting to claim the cache key.
   * @returns True if the new entry should claim the key (key is unclaimed, or existing entry is Pacific and new entry is not).
   */
  function shouldClaimSharedKey(existingEntry: ChannelEntry | undefined, newEntry: ChannelEntry): boolean {

    if(!existingEntry) {

      return true;
    }

    return existingEntry.pacific && !newEntry.pacific;
  }

  /**
   * Caches a single channel entry with multi-key lookups. Creates up to four cache keys per channel: (a) lowercased callSign, (b) stripped callSign with common
   * suffixes removed, (c) lowercased branchOf/company/callSign, and (d) broadcast network abbreviation for local affiliates.
   *
   * Priority rules for shared keys (stripped, branchOf): non-Pacific entries override Pacific entries, otherwise first-write-wins. This ensures that
   * display-name-based lookups (e.g., channelSelector "A&E") resolve to the primary East feed regardless of processing order. Primary callSign keys always use
   * first-write-wins since each callSign is unique. Broadcast network keys use unconditional write so local affiliates override national feeds.
   * @param callSign - The raw callSign string (already validated non-null by caller).
   * @param branchOf - The branchOf/company/callSign value from the API.
   * @param entry - The unified cache entry to store.
   * @param affiliate - The broadcast network abbreviation (e.g., "NBC") or undefined for non-broadcast channels.
   * @returns True if the entry was cached (at least one key written), false if all keys were already occupied.
   */
  function cacheChannelEntry(callSign: string, branchOf: string, entry: ChannelEntry, affiliate: string | undefined): boolean {

    const callSignLower = callSign.toLowerCase();
    let keysWritten = 0;

    // Primary key: lowercased callSign (e.g., "cnnhd"). Always first-write-wins since each callSign is unique.
    if(!channelCache.has(callSignLower)) {

      channelCache.set(callSignLower, entry);
      keysWritten++;
    }

    // Secondary key: stripped callSign with common suffixes removed (e.g., "cnn" from "cnnhd" or "cnnhdp"). Non-Pacific overrides Pacific.
    const strippedKey = stripCallSignSuffix(callSign);

    if((strippedKey !== callSignLower) && shouldClaimSharedKey(channelCache.get(strippedKey), entry)) {

      channelCache.set(strippedKey, entry);
      keysWritten++;
    }

    // Tertiary key: lowercased branchOf/company/callSign (e.g., "cnn", "animal planet"). Non-Pacific overrides Pacific.
    if(branchOf) {

      const branchKey = branchOf.toLowerCase();

      if((branchKey !== callSignLower) && (branchKey !== strippedKey) && shouldClaimSharedKey(channelCache.get(branchKey), entry)) {

        channelCache.set(branchKey, entry);
        keysWritten++;
      }
    }

    // Broadcast network key (e.g., "nbc", "abc"). Unconditional write ensures local affiliates override any previously-cached national entry for the same network.
    if(affiliate) {

      channelCache.set(affiliate.toLowerCase(), entry);
      keysWritten++;
    }

    return keysWritten > 0;
  }

  /**
   * Processes a channelmap API response and populates the unified channel cache using three-pass tiered filtering. Channels are prioritized: non-TVE HD entitled
   * first (cable HD - optimal for a home-network server), then TVE entitled (fallback for channels without a cable HD entry), then non-TVE SD entitled (safety net
   * for SD-only channels). All passes share a single seenCallSigns set for exact callSign deduplication. Cross-tier dedup is handled naturally by
   * cacheChannelEntry's first-write-wins guards - lower-tier entries cannot overwrite cache keys already occupied by higher-tier entries.
   *
   * Local affiliates are detected via callSignVoiceOverHint matching against BROADCAST_HINTS and preferred over national feeds (e.g., local KOMOD over national
   * WLSD1 for ABC). Local identification uses a pre-scan of non-TVE entries - stations present in the non-TVE lineup are local to the subscriber's market.
   * Sub-feeds (DATLN, TODAY) sharing the same voiceOverHint as the primary affiliate are deduplicated by the broadcast network key guard.
   *
   * Pacific timezone variants (callSigns ending with a Pacific suffix like "hdp", "hp", or "ph") are detected via isPacificCallSign and labeled with " (Pacific)"
   * in the display name, matching the convention from generatePacificDefinitions() in the predefined channel system. Non-Pacific entries take priority over Pacific
   * entries for shared cache keys (stripped callSign, branchOf) via shouldClaimSharedKey, ensuring that display-name-based lookups resolve to the East feed.
   * @param data - The parsed channelmap API response.
   */
  function processChannelmapResponse(data: ChannelmapResponse): void {

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
    const passes: { filter: (ch: ApiChannel) => boolean }[] = [

      // Pass 1: Non-TVE HD entitled. Cable HD channels - the primary version for a home-network server. Includes channels like PBS (KCTSD) and CW (KSTWD) that
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

        // Deduplicate by callSign - first occurrence wins (typically the lowest channel number).
        if(seenCallSigns.has(callSignLower)) {

          continue;
        }

        seenCallSigns.add(callSignLower);

        // Skip audio-only channels (Music Choice, Stingray) - they produce static screens with music and are not useful for video capture.
        const branchOf = channel["branchOf/company/callSign"] ?? "";
        const branchLower = branchOf.toLowerCase();

        if(AUDIO_ONLY_PREFIXES.some((prefix) => branchLower.startsWith(prefix))) {

          continue;
        }

        // Determine the display name. For broadcast affiliates identified by callSignVoiceOverHint, use the network abbreviation. For cable channels, use the
        // branchOf/company/callSign value (the network's display name). The channelSelector always uses the raw callSign for guaranteed uniqueness - branchOf
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
          const existingEntry = channelCache.get(networkKey);

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

            channelCache.delete(oldCallSign);

            const oldStripped = stripCallSignSuffix(existingEntry.callSign);

            if(oldStripped !== oldCallSign) {

              channelCache.delete(oldStripped);
            }

            count--;
          }

          displayName = broadcastNetwork;
          affiliate = broadcastNetwork;
        }

        // Detect Pacific timezone variants by callSign suffix. Append " (Pacific)" to the display name to distinguish them in channel discovery, matching the
        // naming convention established by generatePacificDefinitions() in the predefined channel system.
        const pacific = isPacificCallSign(channel.callSign);

        if(pacific) {

          displayName += " (Pacific)";
        }

        const discovered: DiscoveredChannel = {

          ...(affiliate ? { affiliate } : {}),
          channelSelector: channel.callSign,
          name: displayName
        };

        const entry: ChannelEntry = {

          callSign: channel.callSign,
          channelNumber: channel.number ?? Infinity,
          discovered,
          pacific
        };

        if(cacheChannelEntry(channel.callSign, branchOf, entry, affiliate)) {

          count++;
        }
      }
    }

    if(count > 0) {

      fullyEnumerated = true;

      LOG.debug(config.debugCategory, "Channelmap API: cached %s channels (%s cache keys).", count, channelCache.size);
    }
  }

  /**
   * Sets up response interception on the page to capture the channelmap API response. The API fires automatically on guide page load and returns the complete
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

        // Cache the exact URL, headers, and body for CDP request interception on warm tunes. The exact URL ensures we only intercept the correct channelmap
        // request - the SPA makes multiple requests to URLs matching the pattern, but only this one returns the channel lineup. We strip content-encoding and
        // content-length because response.text() returns the decoded (decompressed) body - replaying encoding headers with decoded content would cause
        // double-decompression.
        cachedUrl = url;
        cachedBody = text;

        const headers = response.headers();

        delete headers["content-encoding"];
        delete headers["content-length"];

        cachedHeaders = headers;

        const data = JSON.parse(text) as ChannelmapResponse;

        processChannelmapResponse(data);
      }).catch(() => {

        // Response parsing failed - cold cache fallback handles it.
      });
    });
  }

  /**
   * Looks up a channel in the unified cache using three-tier matching:
   *
   * 1. Exact match: cache key equals the lowercased input (matches callSigns, stripped callSigns, and branchOf names).
   * 2. Suffix-tolerant: input + "hd" or input + "d" matches a cache key (e.g., "cnn" -> "cnnhd", "espn" -> "espnd").
   * 3. Display name iteration: iterate all cache entries, check if discovered.name (lowercased) equals input. Catches verbose display names not covered by cache keys.
   *
   * When a non-exact match succeeds, the result is cached under the input key for O(1) lookup on subsequent calls.
   * @param channelName - The channelSelector value (e.g., "CNN", "CNNHD", "Animal Planet").
   * @returns The matching cache entry or null if no match is found.
   */
  function findChannel(channelName: string): Nullable<ChannelEntry> {

    const lower = channelName.toLowerCase();

    // Tier 1: Exact match on any cache key (callSigns, stripped names, branchOf names).
    const exact = channelCache.get(lower);

    if(exact) {

      return exact;
    }

    // Tier 2: Suffix-tolerant. Try appending common technology suffixes.
    const hdMatch = channelCache.get(lower + "hd");

    if(hdMatch) {

      channelCache.set(lower, hdMatch);

      return hdMatch;
    }

    const dMatch = channelCache.get(lower + "d");

    if(dMatch) {

      channelCache.set(lower, dMatch);

      return dMatch;
    }

    // Tier 3: Display name iteration. Check if any entry's discovered.name matches the input. This catches verbose display names (e.g., "Cable News Network") that
    // may not have a matching cache key.
    const seen = new Set<ChannelEntry>();

    for(const entry of channelCache.values()) {

      if(seen.has(entry)) {

        continue;
      }

      seen.add(entry);

      if(entry.discovered.name.toLowerCase() === lower) {

        channelCache.set(lower, entry);

        return entry;
      }
    }

    return null;
  }

  /**
   * Sets up channelmap API interception for the upcoming page navigation and, on warm tunes, enables CDP request interception to serve the cached channelmap
   * response instantly. Like DirecTV's resolveDirectUrl, this always returns null - all tuning happens in the strategy's execute function via
   * `_watchChannelEventHandler`.
   * @param _channelSelector - The channel selector string (unused - channel lookup happens in the strategy).
   * @param page - The Puppeteer page for response interception setup.
   * @returns Always null - no direct URL navigation.
   */
  async function resolveDirectUrl(_channelSelector: string, page: Page): Promise<Nullable<string>> {

    // Set up response interception so the channelmap API response is captured during guide page navigation.
    setupChannelmapInterception(page);

    // Install the request-interception listener that serves the cached channelmap response on warm tunes, eliminating the 3-5s network round-trip. The install is
    // gated through installOncePerPage so a recovery re-tune on the same page does not re-enable request interception or stack a second `page.on("request")`
    // listener - tuneToChannel is the single source of truth for both initial setup and recovery, so resolveDirectUrl runs again on the same page during recovery.
    // The listener reads the cache through the live closure cells (cachedUrl/cachedBody/cachedHeaders) on every request rather than capturing them by value at
    // install time. This matters because the install runs once: a copy taken here would pin the channelmap snapshot from the first tune and invalidateCachedResponse
    // could never stop the listener from replaying stale data. Reading the live cells means a null cachedBody simply falls through to the network, so an invalidated
    // cache self-heals on the next navigation, and a warm cache is served until it is invalidated.
    await installOncePerPage(page, "channelmap-request-intercept", async () => {

      await page.setRequestInterception(true);

      page.on("request", (request) => {

        // Read the cache through the live closure cells so invalidateCachedResponse() is observed by this long-lived listener. When the cache is cold or has been
        // invalidated, cachedBody/cachedUrl are null and we fall through to request.continue() below.
        if(cachedBody && cachedUrl && (request.url() === cachedUrl)) {

          LOG.debug(config.debugCategory, "Channelmap API: served from cache.");

          void request.respond({

            body: cachedBody,
            headers: cachedHeaders,
            status: 200
          }).catch(() => {

            // Serve failed - fall back to the network so the cold-cache path takes over.
            void request.continue().catch(() => {

              // Page closed - ignore.
            });
          });

          return;
        }

        void request.continue().catch(() => {

          // Page closed - ignore.
        });
      });
    });

    return null;
  }

  /**
   * Polls for a visible "Watch Now" modal button and clicks it if found. Some entertainment channels (e.g., Discovery) display this modal after
   * `_watchChannelEventHandler` switches channels. News channels (e.g., CNN) auto-play without any modal. Runs as a fire-and-forget background task alongside
   * `waitForVideoReady` - channels without modals resolve via `waitForVideoReady` alone, while channels with modals get unblocked by the click.
   *
   * The button requires JS-based detection because multiple `button.style-scope.tv-button` elements exist on the page with zero dimensions. We filter by text
   * content ("Watch Now") and non-zero bounding rect to find the visible one.
   * @param page - The Puppeteer page object.
   */
  async function watchNowModalPoll(page: Page): Promise<void> {

    const checks = Math.ceil(WATCH_NOW_POLL_DURATION / WATCH_NOW_POLL_INTERVAL);

    for(let i = 0; i < checks; i++) {

      // Delay between checks, but not before the first one - the first check is immediate.
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

          LOG.debug(config.debugCategory, "Watch Now modal dismissed.");

          return;
        }
      } catch {

        // Page closed or navigated away - exit silently.
        return;
      }
    }
  }

  /**
   * Derives a deduplicated DiscoveredChannel array from the unified channel cache. Multiple cache keys may point to the same ChannelEntry, so we deduplicate
   * via Set reference equality. Sorts by name before returning.
   * @returns Sorted, deduplicated array of discovered channels.
   */
  function buildDiscoveredChannels(): DiscoveredChannel[] {

    const channels: DiscoveredChannel[] = [];
    const seen = new Set<ChannelEntry>();

    for(const entry of channelCache.values()) {

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
   * In-page SPA tuning strategy. Waits for the Polymer SPA's `TV-APP` element and its `channelMap.channels` to populate, then invokes
   * `_watchChannelEventHandler(null, { channel })` to switch channels in-page. This is the execute path for both warm and cold cache scenarios - the guide URL
   * navigation loads the SPA, and this strategy performs the channel switch within it.
   * @param page - The Puppeteer page object (on the guide page after SPA load).
   * @param profile - The resolved site profile with a non-null channelSelector.
   * @returns Result object with success status and optional failure reason.
   */
  async function directStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

    const channelName = profile.channelSelector;
    const timeout = CONFIG.streaming.videoTimeout;

    // Wait for the TV-APP Polymer element to be present in the DOM.
    try {

      await page.waitForSelector("tv-app", { timeout });
    } catch {

      return { reason: config.label + " guide page did not load within timeout.", success: false };
    }

    // Wait for the SPA's channelMap.channels to populate. The channelmap API fires during page load and the SPA stores the response in this property.
    try {

      await page.waitForFunction((): boolean => {

        const tvApp = document.querySelector("tv-app") as unknown as TvApp | null;

        if(!tvApp) {

          return false;
        }

        const channels = tvApp.channelMap?.channels;

        return (channels !== null) && (channels !== undefined) && (typeof channels === "object") && (Object.keys(channels).length > 0);
      }, { timeout });
    } catch {

      // If we served a cached response and the SPA couldn't process it, the cached data is likely stale or corrupt. Clear it so the next tune falls through to
      // a fresh network fetch.
      invalidateCachedResponse();

      return { reason: config.label + " channel lineup did not load within timeout.", success: false };
    }

    // Poll for Node-side cache population. The channelmap API response is intercepted by setupChannelmapInterception (called by resolveDirectUrl before we got
    // here). We need the Node-side cache for findChannel's three-tier matching.
    const deadline = Date.now() + timeout;

    while((channelCache.size === 0) && (Date.now() < deadline)) {

      // eslint-disable-next-line no-await-in-loop
      await delay(CACHE_POLL_INTERVAL);
    }

    if(channelCache.size === 0) {

      return { reason: config.label + " channel lineup API did not respond within timeout.", success: false };
    }

    // Look up the target channel in the Node-side cache.
    const entry = findChannel(channelName);

    if(!entry) {

      logAvailableChannels({

        availableChannels: buildDiscoveredChannels().map((ch) => ch.name + " (" + ch.channelSelector + ")").sort(),
        channelName,
        guideUrl: config.guideUrl,
        presetSuffix: config.presetSuffix,
        providerName: config.label
      });

      return { reason: "Channel " + channelName + " was not found in the " + config.label + " channel lineup.", success: false };
    }

    // Call _watchChannelEventHandler on the TV-APP element to switch channels in-page. The SPA's channelMap.channels is keyed by internal ID tags, not callSigns,
    // so we iterate the values to find the matching callSign.
    const targetCallSign = entry.callSign;

    try {

      const found = await page.evaluate((callSign: string): boolean => {

        const tvApp = document.querySelector("tv-app") as unknown as TvApp | null;

        if(!tvApp) {

          return false;
        }

        const channelMap = tvApp.channelMap?.channels;

        if(!channelMap) {

          return false;
        }

        // Find the channel object by matching callSign. The channelMap is keyed by internal ID tags, so we iterate values.
        for(const channel of Object.values(channelMap)) {

          if(channel?.callSign === callSign) {

            tvApp._watchChannelEventHandler(null, { channel });

            return true;
          }
        }

        return false;
      }, targetCallSign);

      if(!found) {

        // The Node-side cache had this callSign but the SPA's channelMap doesn't - the cached response is stale. Clear it so the next tune fetches fresh data.
        invalidateCachedResponse();

        return { reason: "Channel " + targetCallSign + " is no longer available in the " + config.label + " channel lineup.", success: false };
      }
    } catch(error) {

      return { reason: "Failed to switch " + config.label + " channel: " + formatError(error) + ".", success: false };
    }

    LOG.debug(config.debugCategory, "Invoked _watchChannelEventHandler for %s (callSign: %s).", channelName, targetCallSign);

    // Launch fire-and-forget Watch Now modal poll. Some entertainment channels show a modal after channel switch that must be clicked to start playback.
    void watchNowModalPoll(page);

    return { success: true };
  }

  /**
   * Discovers all channels by navigating to the guide page and intercepting the channelmap API response. Returns cached results if the cache is already fully
   * populated. Sets handlesOwnNavigation so the route handler does not navigate before calling this function.
   * @param page - The Puppeteer page object (fresh page, not yet navigated).
   * @returns Array of discovered channels.
   */
  async function discoverChannels(page: Page): Promise<DiscoveredChannel[]> {

    // Return from cache if already fully enumerated.
    if(fullyEnumerated && (channelCache.size > 0)) {

      return buildDiscoveredChannels();
    }

    // Set up response interception before navigation so we capture the channelmap API response during page load.
    setupChannelmapInterception(page);

    try {

      await page.goto(config.guideUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
    } catch(error) {

      LOG.warn("Failed to load %s guide page: %s.", config.label, formatError(error));

      return [];
    }

    // Poll for cache population.
    const deadline = Date.now() + CONFIG.streaming.videoTimeout;

    while((channelCache.size === 0) && (Date.now() < deadline)) {

      // eslint-disable-next-line no-await-in-loop
      await delay(CACHE_POLL_INTERVAL);
    }

    if(channelCache.size === 0) {

      LOG.warn("%s channel lineup did not return any channels.", config.label);

      return [];
    }

    return buildDiscoveredChannels();
  }

  /**
   * Returns cached discovered channels from the unified channel cache, or null if the cache is empty or has not been fully enumerated.
   * @returns Sorted array of discovered channels or null.
   */
  function getCachedChannels(): Nullable<DiscoveredChannel[]> {

    if(!fullyEnumerated || (channelCache.size === 0)) {

      return null;
    }

    return buildDiscoveredChannels();
  }

  /**
   * Clears the cached channelmap API response so the next tune fetches fresh data from the network. Called when the strategy detects that the cached response
   * produced a failure - either the SPA couldn't populate channelMap.channels, or a callSign present in the Node-side cache was missing from the SPA's channelMap.
   * The current tune still fails (the channelmap request already fired), but the next tune self-heals via the cold-cache path.
   */
  function invalidateCachedResponse(): void {

    if(!cachedBody) {

      return;
    }

    cachedBody = null;
    cachedHeaders = {};
    cachedUrl = null;

    LOG.debug(config.debugCategory, "Channelmap API: invalidated cached response - next tune will fetch from network.");
  }

  /**
   * Clears the channel cache. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts, since a fresh browser session may have
   * different channel availability.
   */
  function clearCache(): void {

    cachedBody = null;
    cachedHeaders = {};
    cachedUrl = null;
    channelCache.clear();
    fullyEnumerated = false;
  }

  /**
   * Invalidates the cached channel entry for the given channel selector. Called when a stream fails to start after tuning, allowing the next tune attempt to
   * re-resolve via fresh channelmap data.
   * @param channelSelector - The channel selector string to invalidate.
   */
  function invalidateDirectUrl(channelSelector: string): void {

    channelCache.delete(channelSelector.toLowerCase());
  }

  return {

    discoverChannels,
    getCachedChannels,
    guideUrl: config.guideUrl,
    handlesOwnNavigation: true,
    label: config.label,
    noDirectTuneOptimization: true,

    // Profile for Comcast Polymer SPA live channels. The channelmap API at xtvapi.cloudtv.comcast.net returns the complete channel lineup. Tuning uses in-page SPA
    // channel switching: after the guide page loads the Polymer SPA, the strategy calls `_watchChannelEventHandler(null, { channel })` on the `TV-APP` element to
    // switch channels in ~2-3 seconds without page navigation. Extends fullscreenApi for requestFullscreen() behavior on the player page.
    profile: {

      category: "multiChannel",
      channelSelection: { strategy: config.strategyName },
      description: config.profileDescription,
      extends: "fullscreenApi",
      summary: config.profileSummary
    },
    profileName: config.profileName,
    slug: config.slug,
    strategy: {

      clearCache,
      execute: directStrategy,
      invalidateDirectUrl,
      resolveDirectUrl
    },
    strategyName: config.strategyName,
    tinySegmentThreshold: 150
  };
}
