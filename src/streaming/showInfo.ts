/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * showInfo.ts: Channels DVR API integration for show name lookup.
 */
import { LOG, formatError } from "../utils/index.js";
import type { Nullable } from "../types/index.js";
import { getAllChannels } from "../config/userChannels.js";
import { getAllStreams } from "./registry.js";

/*
 * SHOW INFO POLLING
 *
 * This module integrates with the Channels DVR API to retrieve the names of shows for active streams. Show names are determined from two sources:
 *
 * 1. Active recordings (/dvr/jobs) - If Channels DVR is recording a channel, we get the show name from the recording job
 * 2. Program guide (/devices/{id}/guide/now) - For live viewing without recording, we fall back to the current program from the guide
 *
 * The polling mechanism uses a two-phase approach — discovery and lookup:
 *
 * 1. DISCOVERY: Every 30 seconds, try each unique client address as a potential Channels DVR server by calling getDeviceMappings(). If a host has matching M3U
 *    devices, cache it as the last known DVR host. Device mappings are cached for 5 minutes, so non-DVR hosts (e.g., Plex IPs) only incur a network timeout on the
 *    first attempt and every 5 minutes thereafter.
 *
 * 2. LOOKUP: Use the cached DVR host for show name and logo lookups across ALL active streams, regardless of which client initiated them. This enables Plex-initiated
 *    streams to display show names and logos from the Channels DVR guide, as long as any Channels DVR connection has been seen at some point.
 *
 * Caching strategy:
 * - Last known DVR host: Persists across poll cycles, resets on shutdown
 * - Device channel mappings: Cached for 5 minutes per host (rarely change)
 * - Recording jobs and guide data: Fetched fresh each poll cycle (30 seconds)
 *
 * Failure handling is graceful: if the API is unreachable or returns no match, the show name simply stays empty. This never affects streaming functionality.
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

// Logo height in pixels for URL sizing parameter (2x for retina displays).
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

  // Logo URL for the channel (absolute URL, may include query parameters).
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

// State.

// Interval handle for periodic polling.
let pollInterval: Nullable<ReturnType<typeof setInterval>> = null;

// Cache of show names by stream ID.
const showNameCache = new Map<number, string>();

// Cache of device channel mappings by DVR host.
const deviceMappingsByHost = new Map<string, DeviceMappingsCache>();

// Cache of channel logos by channel key (e.g., "cnn" → "http://...").
const channelLogoCache = new Map<string, string>();

// Last known Channels DVR host that had matching M3U devices. Used to look up show names and logos for all streams, including those initiated by non-DVR clients
// (e.g., Plex). Persists across poll cycles but resets on shutdown. Updated whenever getDeviceMappings() finds matching devices on a host.
let lastKnownDvrHost: Nullable<string> = null;

// Pending debounced trigger for immediate show name updates.
let pendingTrigger: Nullable<ReturnType<typeof setTimeout>> = null;

// Public API.

/**
 * Starts the show info polling interval. Should be called on server startup.
 */
export function startShowInfoPolling(): void {

  if(pollInterval) {

    return;
  }

  // Run immediately on startup, then every 30 seconds.
  void updateShowNames();

  pollInterval = setInterval(() => {

    void updateShowNames();
  }, POLL_INTERVAL_MS);
}

/**
 * Stops the show info polling interval. Should be called on server shutdown.
 */
export function stopShowInfoPolling(): void {

  if(pollInterval) {

    clearInterval(pollInterval);
    pollInterval = null;
  }

  // Clear pending trigger.
  if(pendingTrigger) {

    clearTimeout(pendingTrigger);
    pendingTrigger = null;
  }

  // Clear caches and cached DVR host on shutdown.
  showNameCache.clear();
  deviceMappingsByHost.clear();
  channelLogoCache.clear();
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

/**
 * Gets the cached logo URL for a channel.
 * @param channelKey - The channel key to look up (e.g., "cnn").
 * @returns The logo URL if known, undefined otherwise.
 */
export function getChannelLogo(channelKey: string): string | undefined {

  return channelLogoCache.get(channelKey);
}

// Internal Functions.

/**
 * Updates show names for all active streams. Discovery phase tries each unique client address as a potential Channels DVR server. Lookup phase uses the cached DVR
 * host for all streams, enabling non-DVR clients (e.g., Plex) to display show names and logos.
 */
async function updateShowNames(): Promise<void> {

  const streams = getAllStreams();

  if(streams.length === 0) {

    return;
  }

  // Collect all stream entries for lookup and unique client addresses for DVR host discovery.
  const allStreamEntries: Array<{ channelKey: string; id: number }> = [];
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

        lastKnownDvrHost = host;
      }
    })
  );

  // Lookup phase: use the cached DVR host for show name and logo lookups across all streams.
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
async function updateShowNamesForHost(host: string, hostStreams: Array<{ channelKey: string; id: number }>): Promise<void> {

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

        LOG.debug("Show name for stream %d (%s): %s.", stream.id, stream.channelKey, showName);
      }
    } else {

      // No matching recording or guide entry found - clear any stale show name.
      if(showNameCache.has(stream.id)) {

        showNameCache.delete(stream.id);

        LOG.debug("Cleared show name for stream %d (%s): no matching program.", stream.id, stream.channelKey);
      }
    }
  }
}

/**
 * Gets device channel mappings for a DVR host, refreshing the cache if needed.
 * @param host - The DVR server hostname or IP address.
 * @returns Map of DeviceID → (Map of GuideNumber → channel ID).
 */
async function getDeviceMappings(host: string): Promise<Map<string, Map<string, string>>> {

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

    // Check if this device's channel IDs exactly match PrismCast's channels. This identifies our M3U source and avoids misidentification when the user has multiple
    // M3U sources in Channels DVR.
    const deviceChannelIds = new Set(device.Channels.map((ch) => ch.ID));

    if(deviceChannelIds.size !== prismcastChannelKeys.size) {

      LOG.debug("Skipping M3U device %s: channel count mismatch (%d vs %d).", device.DeviceID, deviceChannelIds.size, prismcastChannelKeys.size);

      continue;
    }

    let allMatch = true;

    for(const channelId of deviceChannelIds) {

      if(!prismcastChannelKeys.has(channelId)) {

        allMatch = false;

        break;
      }
    }

    if(!allMatch) {

      LOG.debug("Skipping M3U device %s: channel IDs do not match PrismCast channels.", device.DeviceID);

      continue;
    }

    LOG.debug("Matched M3U device %s as PrismCast source (%d channels).", device.DeviceID, device.Channels.length);

    // Build guide number → channel ID map for this device and cache logo URLs.
    const guideToChannelId = new Map<string, string>();

    for(const channel of device.Channels) {

      guideToChannelId.set(channel.GuideNumber, channel.ID);

      // Cache the logo URL if available.
      if(channel.Logo) {

        const normalizedLogo = normalizeLogoUrl(channel.Logo);

        if(normalizedLogo) {

          channelLogoCache.set(channel.ID, normalizedLogo);
        }
      }
    }

    mappings.set(device.DeviceID, guideToChannelId);
    deviceIds.push(device.DeviceID);
    totalChannels += device.Channels.length;
  }

  // Cache the mappings and device IDs.
  deviceMappingsByHost.set(host, { deviceIds, lastRefresh: now, mappings });

  LOG.debug("Refreshed device mappings from %s: %d M3U device(s), %d channel(s).", host, mappings.size, totalChannels);

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

  LOG.debug("Fetched guide data from %s: %d channel(s) with current programs.", host, showNames.size);

  return showNames;
}

/**
 * Fetches JSON data from a Channels DVR API endpoint.
 * @param host - The DVR server hostname or IP address.
 * @param path - The API path (e.g., "/devices" or "/dvr/jobs").
 * @returns Array of results, empty array on any error.
 */
async function fetchFromDvr<T>(host: string, path: string): Promise<T[]> {

  const url = "http://" + host + ":" + String(CHANNELS_DVR_PORT) + path;

  try {

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), API_TIMEOUT_MS);

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

        LOG.debug("Failed to fetch %s from %s: %s.", path, host, formatError(error));
      }
    }

    return [];
  }
}

/**
 * Normalizes a logo URL by stripping existing query parameters and adding our sizing parameter.
 * @param url - The original logo URL from Channels DVR.
 * @returns The normalized URL with height parameter, or undefined if the URL is invalid.
 */
function normalizeLogoUrl(url: string): string | undefined {

  try {

    const parsed = new URL(url);

    // Strip existing query parameters and add our sizing.
    parsed.search = "?h=" + String(LOGO_HEIGHT_PX);

    return parsed.toString();
  } catch {

    // Invalid URL - return undefined to skip caching.
    return undefined;
  }
}
