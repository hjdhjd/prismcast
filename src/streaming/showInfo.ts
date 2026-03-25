/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * showInfo.ts: Channels DVR API integration for show name and channel logo lookup.
 */
import { LOG, formatError } from "../utils/index.js";
import { clearChannelLogos, getAllChannels, getChannelListing, getChannelLogo, getChannelStationId, setChannelLogo,
  setChannelLogos } from "../config/userChannels.js";
import { loadUserConfig, saveUserConfig } from "../config/userConfig.js";
import type { Nullable } from "../types/index.js";
import { getAllStreams } from "./registry.js";

/* This module integrates with the Channels DVR API for two purposes: show name lookup and channel logo population.
 *
 * Show names are determined from two sources:
 *
 * 1. Active recordings (/dvr/jobs) - if Channels DVR is recording a channel, we get the show name from the recording job.
 * 2. Program guide (/devices/{id}/guide/now) - for live viewing without recording, we fall back to the current program from the guide.
 *
 * The show name polling mechanism uses a two-phase approach - discovery and lookup:
 *
 * 1. Discovery: Every 30 seconds, try each unique client address as a potential Channels DVR server by calling getDeviceMappings(). If a host has matching M3U
 *    devices, cache it as the last known DVR host. Device mappings are cached for 5 minutes, so non-DVR hosts (e.g., Plex IPs) only incur a network timeout on the
 *    first attempt and every 5 minutes thereafter.
 *
 * 2. Lookup: Use the cached DVR host for show name lookups across ALL active streams, regardless of which client initiated them. This enables Plex-initiated
 *    streams to display show names from the Channels DVR guide, as long as any Channels DVR connection has been seen at some point.
 *
 * Channel logos are populated in two tiers when a DVR host is known:
 *
 * 1. Tier 1 (/devices): Logo URLs are extracted from the matched M3U device's channel list during getDeviceMappings(). This covers all channels in the M3U
 *    playlist with authoritative, current logo URLs from the DVR.
 *
 * 2. Tier 2 (/tms/stations/{name}): For channels with station IDs not covered by tier 1 (disabled channels, channels without an enabled provider), a TMS station
 *    name search finds the best matching logo. Results are matched by station ID when possible, otherwise the first result with a valid logo is used.
 *
 * Logo population runs on DVR host discovery (startup or first stream connection) and refreshes every 24 hours. Individual channel add/edit operations trigger a
 * single-entry TMS search for immediate logo availability.
 *
 * Caching strategy:
 * - Last known DVR host: Persists across poll cycles, resets on shutdown
 * - Device channel mappings: Cached for 5 minutes per host (rarely change)
 * - Recording jobs and guide data: Fetched fresh each poll cycle (30 seconds)
 * - Channel logos: Cached by station ID in userChannels.ts, refreshed every 24 hours
 *
 * Failure handling is graceful: if the API is unreachable or returns no match, show names and logos simply stay empty. This never affects streaming functionality.
 */

// Constants.

// Default Channels DVR port.
const CHANNELS_DVR_PORT = 8089;

// How often to poll for show info (30 seconds).
const POLL_INTERVAL_MS = 30000;

// How often to refresh device channel mappings (5 minutes).
const MAPPINGS_REFRESH_INTERVAL_MS = 300000;

// Timeout for API requests (5 seconds).
const API_TIMEOUT_MS = 5000;

// Debounce delay for triggered updates (2 seconds).
const TRIGGER_DEBOUNCE_MS = 2000;

// Logo refresh interval (24 hours). Logos change when networks rebrand, which is a handful of times per year.
const LOGO_REFRESH_INTERVAL_MS = 86400000;

// Logo image height in pixels for URL normalization (2x for retina displays). Applied to CDN URLs to request smaller images for UI rendering.
const LOGO_HEIGHT_PX = 48;

// Types.

/**
 * Channel entry from a Channels DVR device.
 */
interface ChannelsDvrChannel {

  // Guide number (e.g., "7008").
  GuideNumber: string;

  // Channel ID from the M3U playlist - this is our channel key (e.g., "cnn").
  ID: string;

  // Logo URL for the channel (absolute URL from the TMS image CDN, may include query parameters for sizing).
  Logo?: string;
}

/**
 * Device entry from Channels DVR /devices endpoint.
 */
interface ChannelsDvrDevice {

  // Channel list for this device.
  Channels?: ChannelsDvrChannel[];

  // Device identifier (e.g., "M3U-Prism").
  DeviceID: string;

  // Provider type (e.g., "m3u" for M3U sources).
  Provider: string;
}

/**
 * Job entry from Channels DVR /dvr/jobs endpoint.
 */
interface ChannelsDvrJob {

  // Guide number of the channel being recorded (e.g., "7008").
  Channel: string;

  // Device identifier (e.g., "M3U-Prism").
  DeviceID: string;

  // Program title (e.g., "Erin Burnett OutFront").
  Name: string;
}

/**
 * Cached device channel mappings for a single DVR host.
 */
interface DeviceMappingsCache {

  // List of M3U device IDs for guide lookups.
  deviceIds: string[];

  // Last refresh timestamp.
  lastRefresh: number;

  // Map of DeviceID → (Map of GuideNumber → channel ID).
  mappings: Map<string, Map<string, string>>;
}

/**
 * Airing entry from the guide/now endpoint.
 */
interface ChannelsDvrAiring {

  // Program title (e.g., "Anderson Cooper 360").
  Title: string;
}

/**
 * Channel info from the guide/now endpoint.
 */
interface ChannelsDvrGuideChannel {

  // Channel ID - this is our channel key (e.g., "cnn").
  ChannelID: string;
}

/**
 * Guide entry from Channels DVR /devices/{id}/guide/now endpoint.
 */
interface ChannelsDvrGuideEntry {

  // Currently airing programs (usually just one).
  Airings?: ChannelsDvrAiring[];

  // Channel information.
  Channel: ChannelsDvrGuideChannel;
}

/**
 * Station entry from the Channels DVR /tms/stations/{search} endpoint. Returns stations matching the search string with their Gracenote metadata and logo URLs.
 */
interface TmsStationResult {

  // Preferred image for the station (landscape logo). The uri field contains the CDN URL.
  preferredImage?: { uri?: string };

  // Gracenote station ID.
  stationId?: string;
}

// State.

// Interval handle for periodic polling.
let pollInterval: Nullable<ReturnType<typeof setInterval>> = null;

// Cache of show names by stream ID.
const showNameCache = new Map<number, string>();

// Cache of device channel mappings by DVR host.
const deviceMappingsByHost = new Map<string, DeviceMappingsCache>();

// Interval handle for 24-hour logo refresh.
let logoRefreshInterval: Nullable<ReturnType<typeof setInterval>> = null;

// Last known Channels DVR host that had matching M3U devices. Used to look up show names for all streams, including those initiated by non-DVR clients
// (e.g., Plex). Persists across poll cycles but resets on shutdown. Updated whenever getDeviceMappings() finds matching devices on a host.
let lastKnownDvrHost: Nullable<string> = null;

// Pending debounced trigger for immediate show name updates.
let pendingTrigger: Nullable<ReturnType<typeof setTimeout>> = null;

// Public API.

/**
 * Returns the last known Channels DVR host address. Used by the pretune module to poll for upcoming scheduled recordings.
 * @returns The DVR host address, or null if no DVR host has been discovered.
 */
export function getDvrHost(): Nullable<string> {

  return lastKnownDvrHost;
}

/**
 * Sets the DVR host address and persists it to the config file if it changed. Called by the discovery loop when a matching M3U device is found on a host.
 * @param host - The DVR server hostname or IP address.
 */
export function setDvrHost(host: string): void {

  if(lastKnownDvrHost === host) {

    return;
  }

  lastKnownDvrHost = host;

  void persistDvrHost(host);

  // Populate logos whenever the DVR host changes. The early return above (host === lastKnownDvrHost) prevents redundant calls during show name polling's
  // repeated confirmations of the same host. A genuine host change (e.g., DVR migration) triggers a full re-population with the new host's data.
  void populateChannelLogos();
}

/**
 * Starts the show info polling interval. Should be called on server startup.
 */
export function startShowInfoPolling(): void {

  if(pollInterval) {

    return;
  }

  // Load the persisted DVR host from the config file so the pretune module can begin polling immediately on startup.
  void loadPersistedDvrHost();

  // Run immediately on startup, then every 30 seconds.
  void updateShowNames();

  pollInterval = setInterval(() => {

    void updateShowNames();
  }, POLL_INTERVAL_MS);

  // Start the 24-hour logo refresh. The initial population is triggered by loadPersistedDvrHost() or setDvrHost() when a DVR host becomes known.
  logoRefreshInterval = setInterval(() => {

    void populateChannelLogos();
  }, LOGO_REFRESH_INTERVAL_MS);
}

/**
 * Stops the show info polling interval. Should be called on server shutdown.
 */
export function stopShowInfoPolling(): void {

  if(pollInterval) {

    clearInterval(pollInterval);
    pollInterval = null;
  }

  // Clear the logo refresh interval.
  if(logoRefreshInterval) {

    clearInterval(logoRefreshInterval);
    logoRefreshInterval = null;
  }

  // Clear pending trigger.
  if(pendingTrigger) {

    clearTimeout(pendingTrigger);
    pendingTrigger = null;
  }

  // Clear caches and cached DVR host on shutdown.
  showNameCache.clear();
  deviceMappingsByHost.clear();
  clearChannelLogos();
  lastKnownDvrHost = null;
}

/**
 * Triggers an immediate show name update. Uses debouncing to prevent excessive API calls when multiple streams start in quick succession. The update runs after a
 * short delay, collapsing multiple triggers into a single poll.
 */
export function triggerShowNameUpdate(): void {

  // Clear any pending trigger to reset the debounce timer.
  if(pendingTrigger) {

    clearTimeout(pendingTrigger);
  }

  // Schedule the update after the debounce delay.
  pendingTrigger = setTimeout(() => {

    pendingTrigger = null;

    void updateShowNames();
  }, TRIGGER_DEBOUNCE_MS);
}

/**
 * Gets the cached show name for a stream.
 * @param streamId - The stream ID to look up.
 * @returns The show name if known, empty string otherwise.
 */
export function getShowName(streamId: number): string {

  return showNameCache.get(streamId) ?? "";
}

/**
 * Clears the cached show name for a stream. Should be called when a stream terminates.
 * @param streamId - The stream ID to clear.
 */
export function clearShowName(streamId: number): void {

  showNameCache.delete(streamId);
}

// Internal Functions.

/**
 * Updates show names for all active streams. Discovery phase tries each unique client address as a potential Channels DVR server. Lookup phase uses the cached DVR
 * host for all streams, enabling non-DVR clients (e.g., Plex) to display show names.
 */
async function updateShowNames(): Promise<void> {

  const streams = getAllStreams();

  if(streams.length === 0) {

    return;
  }

  // Collect all stream entries for lookup and unique client addresses for DVR host discovery.
  const allStreamEntries: { channelKey: string; id: number }[] = [];
  const discoveryHosts = new Set<string>();

  for(const stream of streams) {

    allStreamEntries.push({ channelKey: stream.info.storeKey, id: stream.id });

    if(stream.clientAddress) {

      // Normalize IPv6-mapped IPv4 addresses (::ffff:192.168.1.1 → 192.168.1.1).
      const host = stream.clientAddress.startsWith("::ffff:") ? stream.clientAddress.slice(7) : stream.clientAddress;

      discoveryHosts.add(host);
    }
  }

  // Discovery phase: try each unique client address to find or confirm a DVR host. Device mappings are cached with a 5-minute TTL, so non-DVR hosts only incur a
  // network timeout on the first attempt and every 5 minutes thereafter.
  await Promise.all(
    Array.from(discoveryHosts).map(async (host) => {

      const mappings = await getDeviceMappings(host);

      if(mappings.size > 0) {

        setDvrHost(host);
      }
    })
  );

  // Lookup phase: use the cached DVR host for show name lookups across all streams.
  if(lastKnownDvrHost) {

    await updateShowNamesForHost(lastKnownDvrHost, allStreamEntries);

    return;
  }

  // No DVR host available — clear show names for all streams.
  for(const stream of allStreamEntries) {

    showNameCache.delete(stream.id);
  }
}

/**
 * Updates show names for streams from a single DVR host.
 * @param host - The DVR server hostname or IP address.
 * @param hostStreams - Array of streams from this host with their channel keys.
 */
async function updateShowNamesForHost(host: string, hostStreams: { channelKey: string; id: number }[]): Promise<void> {

  // Ensure we have fresh device mappings.
  const mappings = await getDeviceMappings(host);

  if(mappings.size === 0) {

    // No M3U devices found or API unreachable - clear show names for these streams.
    for(const stream of hostStreams) {

      showNameCache.delete(stream.id);
    }

    return;
  }

  // Fetch active jobs.
  const jobs = await fetchFromDvr<ChannelsDvrJob>(host, "/dvr/jobs");

  // Build a map of channel key → show name from active recordings.
  const recordingShowNames = new Map<string, string>();

  for(const job of jobs) {

    // Look up the device mappings for this job's device.
    const deviceMappings = mappings.get(job.DeviceID);

    if(!deviceMappings) {

      continue;
    }

    // Look up the channel key for this job's guide number.
    const channelKey = deviceMappings.get(job.Channel);

    if(channelKey) {

      recordingShowNames.set(channelKey, job.Name);
    }
  }

  // For channels without recording data, get show names from the guide.
  const guideShowNames = await getGuideShowNames(host);

  // Update show name cache for each stream.
  for(const stream of hostStreams) {

    // Prefer recording name over guide name (recording is more accurate for what's actually being captured).
    const showName = recordingShowNames.get(stream.channelKey) ?? guideShowNames.get(stream.channelKey);

    if(showName) {

      const previousName = showNameCache.get(stream.id);

      if(previousName !== showName) {

        showNameCache.set(stream.id, showName);

        LOG.debug("streaming:showinfo", "Show name for stream %d (%s): %s.", stream.id, stream.channelKey, showName);
      }
    } else {

      // No matching recording or guide entry found - clear any stale show name.
      if(showNameCache.has(stream.id)) {

        showNameCache.delete(stream.id);

        LOG.debug("streaming:showinfo", "Cleared show name for stream %d (%s): no matching program.", stream.id, stream.channelKey);
      }
    }
  }
}

/**
 * Gets device channel mappings for a DVR host, refreshing the cache if needed.
 * @param host - The DVR server hostname or IP address.
 * @returns Map of DeviceID → (Map of GuideNumber → channel ID).
 */
export async function getDeviceMappings(host: string): Promise<Map<string, Map<string, string>>> {

  const cached = deviceMappingsByHost.get(host);
  const now = Date.now();

  // Return cached mappings if they're fresh enough.
  if(cached && ((now - cached.lastRefresh) < MAPPINGS_REFRESH_INTERVAL_MS)) {

    return cached.mappings;
  }

  // Fetch fresh device data.
  const devices = await fetchFromDvr<ChannelsDvrDevice>(host, "/devices");

  // Get PrismCast's channel keys to identify which M3U device is ours.
  const prismcastChannelKeys = new Set(Object.keys(getAllChannels()));

  // Build mappings for M3U devices that match PrismCast's channel list.
  const mappings = new Map<string, Map<string, string>>();
  const deviceIds: string[] = [];
  let totalChannels = 0;

  for(const device of devices) {

    // Only process M3U sources.
    if(device.Provider !== "m3u") {

      continue;
    }

    if(!device.Channels || (device.Channels.length === 0)) {

      continue;
    }

    // Identify PrismCast's M3U source by measuring channel ID overlap. We use overlap-based matching rather than exact matching because the channel set can drift:
    // the user may disable channels after Channels DVR imports the playlist, or add new channels that haven't been refreshed yet in the DVR. We count how many of
    // PrismCast's channel keys appear in the device, and accept the device if overlap exceeds 80% of the larger set.
    const deviceChannelIds = new Set(device.Channels.map((ch) => ch.ID));
    let overlapCount = 0;

    for(const key of prismcastChannelKeys) {

      if(deviceChannelIds.has(key)) {

        overlapCount++;
      }
    }

    const maxSize = Math.max(deviceChannelIds.size, prismcastChannelKeys.size);
    const overlapRatio = overlapCount / maxSize;

    if(overlapRatio < 0.8) {

      LOG.debug("streaming:showinfo", "Skipping M3U device %s: low channel overlap (%d/%d = %d%%).",
        device.DeviceID, overlapCount, maxSize, Math.round(overlapRatio * 100));

      continue;
    }

    LOG.debug("streaming:showinfo", "Matched M3U device %s as PrismCast source (%d channels, %d%% overlap).",
      device.DeviceID, device.Channels.length, Math.round(overlapRatio * 100));

    // Build guide number → channel ID map for this device and extract logo URLs. Logo URLs are cached by station ID (resolved from the channel key via
    // getChannelStationId) so that Pacific channels and provider variants all share the same logo entry.
    const guideToChannelId = new Map<string, string>();
    const logos = new Map<string, string>();

    for(const channel of device.Channels) {

      guideToChannelId.set(channel.GuideNumber, channel.ID);

      if(channel.Logo) {

        const stationId = getChannelStationId(channel.ID);

        if(stationId) {

          logos.set(stationId, normalizeLogoUrl(channel.Logo));
        }
      }
    }

    // Populate the logo cache with URLs from this device (tier 1).
    if(logos.size > 0) {

      setChannelLogos(logos);
    }

    mappings.set(device.DeviceID, guideToChannelId);
    deviceIds.push(device.DeviceID);
    totalChannels += device.Channels.length;
  }

  // Cache the mappings and device IDs.
  deviceMappingsByHost.set(host, { deviceIds, lastRefresh: now, mappings });

  LOG.debug("streaming:showinfo", "Refreshed device mappings from %s: %d M3U device(s), %d channel(s).", host, mappings.size, totalChannels);

  return mappings;
}

/**
 * Gets guide show names for a DVR host by fetching current program data.
 * @param host - The DVR server hostname or IP address.
 * @returns Map of channel ID → show name for currently airing programs.
 */
async function getGuideShowNames(host: string): Promise<Map<string, string>> {

  // Get device IDs from the mappings cache (should already be populated).
  const deviceCache = deviceMappingsByHost.get(host);

  if(!deviceCache || (deviceCache.deviceIds.length === 0)) {

    return new Map();
  }

  // Fetch guide data from all M3U devices in parallel.
  const guideResults = await Promise.all(
    deviceCache.deviceIds.map(async (deviceId) => fetchFromDvr<ChannelsDvrGuideEntry>(host, "/devices/" + deviceId + "/guide/now"))
  );

  // Build channel ID → show name map from all devices.
  const showNames = new Map<string, string>();

  for(const guideEntries of guideResults) {

    for(const entry of guideEntries) {

      const channelId = entry.Channel.ChannelID;
      const showName = entry.Airings?.[0]?.Title;

      if(channelId && showName) {

        showNames.set(channelId, showName);
      }
    }
  }

  LOG.debug("streaming:showinfo", "Fetched guide data from %s: %d channel(s) with current programs.", host, showNames.size);

  return showNames;
}

/**
 * Fetches JSON data from a Channels DVR API endpoint.
 * @param host - The DVR server hostname or IP address.
 * @param path - The API path (e.g., "/devices" or "/dvr/jobs").
 * @returns Array of results, empty array on any error.
 */
export async function fetchFromDvr<T>(host: string, path: string): Promise<T[]> {

  const url = "http://" + host + ":" + String(CHANNELS_DVR_PORT) + path;

  try {

    const controller = new AbortController();
    const timeoutId = setTimeout(() => { controller.abort(); }, API_TIMEOUT_MS);

    const response = await fetch(url, {

      headers: { "Accept": "application/json" },
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    if(!response.ok) {

      return [];
    }

    return await response.json() as T[];
  } catch(error) {

    if(error instanceof Error) {

      // Don't log abort errors (timeouts) - they're expected for unreachable servers.
      if(error.name !== "AbortError") {

        LOG.debug("streaming:showinfo", "Failed to fetch %s from %s: %s.", path, host, formatError(error));
      }
    }

    return [];
  }
}

/**
 * Loads the persisted DVR host from the config file. Called on startup so the pretune module can begin polling immediately.
 */
async function loadPersistedDvrHost(): Promise<void> {

  try {

    const result = await loadUserConfig();

    if(!result.parseError && result.config.dvrHost) {

      lastKnownDvrHost = result.config.dvrHost;

      LOG.debug("streaming:showinfo", "Loaded persisted DVR host: %s.", lastKnownDvrHost);

      // Populate logos immediately from the persisted host so they're available on the first page load.
      void populateChannelLogos();
    }
  } catch {

    // Ignore errors - the host will be discovered from the first stream's client address.
  }
}

/**
 * Persists the DVR host to the config file so it survives restarts.
 * @param host - The DVR server hostname or IP address.
 */
async function persistDvrHost(host: string): Promise<void> {

  try {

    const result = await loadUserConfig();

    if(!result.parseError) {

      result.config.dvrHost = host;

      await saveUserConfig(result.config);
    }
  } catch(error) {

    LOG.debug("streaming:showinfo", "Failed to persist DVR host: %s.", formatError(error));
  }
}

// Logo population.

/**
 * Populates the channel logo cache in two tiers. Tier 1 fetches logo URLs from the DVR's /devices endpoint (covers all channels in the M3U playlist). Tier 2 runs
 * TMS station name searches for any remaining channels with station IDs not covered by tier 1. Called on DVR host discovery (startup or first stream) and every 24
 * hours to pick up network rebrands.
 */
async function populateChannelLogos(): Promise<void> {

  if(!lastKnownDvrHost) {

    return;
  }

  // Tier 1: fetch device data and extract logos. getDeviceMappings() handles the /devices fetch, device matching, and logo extraction into the cache as a side
  // effect. The 5-minute mapping cache means we don't re-fetch if show name polling already called this recently.
  await getDeviceMappings(lastKnownDvrHost);

  // Tier 2: search TMS by channel name for channels with station IDs not covered by tier 1. This covers disabled channels, channels without an enabled provider,
  // and any channels the DVR device didn't include logos for.
  const listing = getChannelListing();
  let cachedCount = 0;
  let tier2Count = 0;

  for(const entry of listing) {

    // Skip channels that already have a logo in the cache (from tier 1 or a previous population cycle).
    if(getChannelLogo(entry.key)) {

      cachedCount++;

      continue;
    }

    const stationId = getChannelStationId(entry.key);

    if(!stationId) {

      continue;
    }

    const channelName = entry.channel.name ?? entry.key;

    // eslint-disable-next-line no-await-in-loop -- Intentional: sequential to avoid flooding the DVR's TMS proxy with concurrent requests.
    const found = await searchTmsStationLogo(lastKnownDvrHost, channelName, stationId);

    if(found) {

      tier2Count++;
    }
  }

  LOG.debug("streaming:logos", "Logo population complete: %d cached, %d from TMS search, %d total.", cachedCount, tier2Count, cachedCount + tier2Count);
}

/**
 * Searches the TMS stations endpoint for a channel's logo by name. If a result matches the target station ID, uses that logo. Otherwise uses the first result
 * with a valid logo URL (same brand, different regional feed). Caches the result if found.
 * @param host - The DVR server hostname or IP address.
 * @param channelName - The channel display name to search for (e.g., "AMC", "Animal Planet").
 * @param targetStationId - The Gracenote station ID to match against results.
 * @returns True if a logo was found and cached, false otherwise.
 */
async function searchTmsStationLogo(host: string, channelName: string, targetStationId: string): Promise<boolean> {

  const results = await fetchFromDvr<TmsStationResult>(host, "/tms/stations/" + encodeURIComponent(channelName));

  if(results.length === 0) {

    return false;
  }

  // Prefer an exact station ID match. If none, use the first result with a valid logo URL (same brand, different regional variant).
  let logoUrl: string | undefined;

  for(const result of results) {

    const uri = result.preferredImage?.uri;

    if(!uri) {

      continue;
    }

    if(result.stationId === targetStationId) {

      logoUrl = uri;

      break;
    }

    logoUrl ??= uri;
  }

  if(!logoUrl) {

    return false;
  }

  setChannelLogo(targetStationId, normalizeLogoUrl(logoUrl));

  LOG.debug("streaming:logos", "TMS search for '%s': found logo for station %s.", channelName, targetStationId);

  return true;
}

/**
 * Triggers a single-channel logo lookup via TMS station name search. Called when a channel is added or edited with a station ID. Fire-and-forget - the logo
 * appears on the next page load after the search completes.
 * @param channelName - The channel display name to search for.
 * @param stationId - The Gracenote station ID for the channel.
 */
export function updateChannelLogo(channelName: string, stationId: string): void {

  if(!lastKnownDvrHost) {

    return;
  }

  void searchTmsStationLogo(lastKnownDvrHost, channelName, stationId);
}

/**
 * Normalizes a logo URL by replacing query parameters with a height-only sizing parameter. The TMS CDN accepts ?h= for image height. This reduces bandwidth by
 * requesting smaller images for UI rendering (48px height for retina) instead of the default 360x270 that the DVR API returns.
 * @param url - The original logo URL from the DVR or TMS API.
 * @returns The URL with query parameters replaced by the height sizing parameter.
 */
function normalizeLogoUrl(url: string): string {

  try {

    const parsed = new URL(url);

    parsed.search = "?h=" + String(LOGO_HEIGHT_PX);

    return parsed.toString();
  } catch {

    return url;
  }
}
