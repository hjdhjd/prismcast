/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * showInfo.ts: Channels DVR API integration for show name and channel logo lookup.
 */
import type { ChangeRejection, ConfigChange } from "../config/reactivity.ts";
import type { Config, Nullable } from "../types/index.ts";
import { LOG, formatError, normalizeClientAddress, timeoutSignal } from "../utils/index.ts";
import { TimerRegistry, systemClock } from "homebridge-plugin-utils";
import { clearChannelLogos, getAllChannels, getChannelListing, getChannelLogo, getChannelStationId, setChannelLogo,
  setChannelLogos } from "../config/userChannels.ts";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "homebridge-plugin-utils";
import { emitChannelUpdate } from "./statusEmitter.ts";
import { getAllStreams } from "./registry.ts";
import { mutateConfig } from "../config/userConfig.ts";
import { registerConfigChangeHandler } from "../config/reactivity.ts";

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
 *    devices, it becomes the DVR host. Device mappings are cached for 5 minutes, so non-DVR hosts (e.g., Plex IPs) only incur a network timeout on the first
 *    attempt and every 5 minutes thereafter.
 *
 * 2. Lookup: Use the DVR host for show name lookups across ALL active streams, regardless of which client initiated them. This enables Plex-initiated
 *    streams to display show names from the Channels DVR guide, as long as any Channels DVR connection has been seen at some point.
 *
 * Channel logos are populated in two tiers when a DVR host is known:
 *
 * 1. Tier 1 (/devices): Logo URLs are extracted from the matched M3U device's channel list during getDeviceMappings(). This covers all channels in the M3U
 *    playlist with authoritative, current logo URLs from the DVR.
 *
 * 2. Tier 2 (/tms/stations/{name}): For channels with station IDs not covered by tier 1 (disabled channels, channels without an enabled service), a TMS station
 *    name search finds the best matching logo. Results are matched by station ID when possible, otherwise the first result with a valid logo is used.
 *
 * Logo population runs when the poller starts with a DVR host known, when discovery finds a new host, and when a save changes the host or the port, and it
 * refreshes every 24 hours. Individual channel add/edit operations trigger a single-entry TMS search for immediate logo availability.
 *
 * State and caching strategy:
 * - DVR host: The running configuration's channelsDvr.host, the one copy every reader consults. The boot reads it from the file, discovery writes it and
 *   persists it, and a save that changes it takes effect live, so it outlives the poller and survives restarts
 * - Device channel mappings: Cached for 5 minutes per host, aged on the poller's clock (rarely change)
 * - Recording jobs and guide data: Fetched fresh each poll cycle (30 seconds)
 * - Channel logos: Cached by station ID in userChannels.ts, refreshed every 24 hours
 *
 * Failure handling is graceful: if the API is unreachable or returns no match, show names and logos simply stay empty. This never affects streaming functionality.
 */

// Constants.

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

  // Map of DeviceID -> (Map of GuideNumber -> channel ID).
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

/* The poller's timers, on the library's lifetime-bound registry: the show-name poll under "poll", the 24-hour logo refresh under "logos", and the debounced
 * trigger under "trigger". Null while the poller is stopped, so the binding is also the statement of whether it is running.
 */
let timers: Nullable<TimerRegistry> = null;

// The clock the poller's own reads take their instant from. Set at start and reset at stop, so an operation that begins under one poller never mixes two clocks.
let pollingClock: Clock = systemClock;

// Cache of show names by stream ID.
const showNameCache = new Map<number, string>();

// Cache of device channel mappings by DVR host.
const deviceMappingsByHost = new Map<string, DeviceMappingsCache>();

// Public API.

/**
 * Returns the Channels DVR host the running configuration holds. It is used to look up show names for every stream, including those a non-DVR client such as
 * Plex started, and by the pretune module to poll for upcoming scheduled recordings.
 * @returns The DVR host address, or null when no DVR host is known.
 */
export function getDvrHost(): Nullable<string> {

  return (CONFIG.channelsDvr.host.length > 0) ? CONFIG.channelsDvr.host : null;
}

/**
 * Makes a host the running DVR host and persists it to the config file if it changed. Called by the discovery loop when a matching M3U device is found on a
 * host.
 *
 * The host must be host-only - never `host:port`. The port lives at `CONFIG.channelsDvr.port` exclusively. Inputs containing a colon are rejected
 * with a debug log rather than silently stripped, because a colon-bearing host indicates a caller-side bug (auto-discovery should be feeding IPs, not
 * `IP:port` strings) and silent stripping would mask it. The schema migration to `channelsDvr.host` splits any legacy `host:port` value at read time, so this
 * function only sees host-only inputs in the steady state.
 *
 * @param host - The DVR server hostname or IP address. Must NOT include a port.
 */
export function setDvrHost(host: string): void {

  if(host.includes(":")) {

    LOG.debug("streaming:showinfo", "setDvrHost rejected colon-bearing input %s; host must be host-only, port lives at CONFIG.channelsDvr.port.", host);

    return;
  }

  if(CONFIG.channelsDvr.host === host) {

    return;
  }

  // The running configuration takes the host first, so every reader sees it at once, and the file follows, so the next save finds the two equal and reports
  // nothing for it.
  CONFIG.channelsDvr.host = host;

  void persistDvrHost(host);

  // Populate logos whenever the DVR host changes. The early return above prevents redundant calls during show name polling's repeated confirmations of the same
  // host. A genuine host change (e.g., DVR migration) triggers a full re-population with the new host's data.
  populateRunningChannelLogos();
}

/**
 * Starts the show info polling interval. Should be called on server startup.
 * @param clock - The clock the poll cadences, the debounce, and the mapping cache's refresh instant read; defaults to the system clock.
 */
export function startShowInfoPolling(clock: Clock = systemClock): void {

  if(timers) {

    return;
  }

  pollingClock = clock;
  timers = new TimerRegistry({ clock });

  // The DVR host is the running configuration's, which the boot read from the file, so the logos populate from it at once when one is known.
  populateRunningChannelLogos();

  // Run immediately on startup, then every 30 seconds.
  void updateShowNames();

  timers.setInterval("poll", () => {

    void updateShowNames();
  }, POLL_INTERVAL_MS);

  // Start the 24-hour logo refresh. A start with no DVR host known populates nothing above; setDvrHost or a save populates once a host becomes known.
  timers.setInterval("logos", () => {

    populateRunningChannelLogos();
  }, LOGO_REFRESH_INTERVAL_MS);
}

/**
 * Stops the show info polling interval. Should be called on server shutdown.
 */
export function stopShowInfoPolling(): void {

  // Disposing the registry drains the two intervals and any pending trigger together, and makes a later arm on it inert.
  timers?.dispose();
  timers = null;
  pollingClock = systemClock;

  // Clear the caches on shutdown. The DVR host belongs to the running configuration rather than to the poller, so it stays.
  showNameCache.clear();
  deviceMappingsByHost.clear();
  clearChannelLogos();
}

/**
 * Triggers an immediate show name update. Uses debouncing to prevent excessive API calls when multiple streams start in quick succession. The update runs after a
 * short delay, collapsing multiple triggers into a single poll.
 */
export function triggerShowNameUpdate(): void {

  // Registering under the same key replaces whatever the key held, which is the debounce: the latest trigger wins. A call made before the poller has started, or
  // after it has stopped, arms nothing.
  timers?.setTimeout("trigger", () => {

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

  // One instant and one DVR port for the whole operation, read at its start, so a stop landing mid-operation cannot split this update's cache reads across two
  // clocks and a save landing mid-operation cannot split its requests across two ports.
  const now = pollingClock.now();
  const port = CONFIG.channelsDvr.port;
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

      discoveryHosts.add(normalizeClientAddress(stream.clientAddress));
    }
  }

  // Discovery phase: try each unique client address to find or confirm a DVR host. Device mappings are cached with a 5-minute TTL, so non-DVR hosts only incur a
  // network timeout on the first attempt and every 5 minutes thereafter.
  await Promise.all(
    Array.from(discoveryHosts).map(async (host) => {

      const mappings = await getDeviceMappings(host, port, now);

      if(mappings.size > 0) {

        setDvrHost(host);
      }
    })
  );

  // Lookup phase: use the DVR host for show name lookups across all streams.
  const dvrHost = getDvrHost();

  if(dvrHost) {

    await updateShowNamesForHost(dvrHost, port, allStreamEntries, now);

    return;
  }

  // No DVR host available - clear show names for all streams.
  for(const stream of allStreamEntries) {

    showNameCache.delete(stream.id);
  }
}

/**
 * Updates show names for streams from a single DVR host.
 * @param host - The DVR server hostname or IP address.
 * @param port - The DVR's API port.
 * @param hostStreams - Array of streams from this host with their channel keys.
 * @param now - The instant the calling update read at its start, threaded so the whole operation ages the cache against one reading.
 */
async function updateShowNamesForHost(host: string, port: number, hostStreams: { channelKey: string; id: number }[], now: number): Promise<void> {

  // Ensure we have fresh device mappings.
  const mappings = await getDeviceMappings(host, port, now);

  if(mappings.size === 0) {

    // No M3U devices found or API unreachable - clear show names for these streams.
    for(const stream of hostStreams) {

      showNameCache.delete(stream.id);
    }

    return;
  }

  // Fetch active jobs.
  const jobs = await fetchFromDvr<ChannelsDvrJob>(host, port, "/dvr/jobs");

  // Build a map of channel key -> show name from active recordings.
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
  const guideShowNames = await getGuideShowNames(host, port);

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
 * Result of measuring channel ID overlap between a Channels DVR M3U device and PrismCast's own channel set.
 */
export interface DeviceOverlap {

  // Whether the overlap ratio clears the acceptance threshold. Computed as `!(overlapRatio < 0.8)` rather than `overlapRatio >= 0.8` - the two forms agree for
  // finite ratios but diverge when both sets are empty, where overlapRatio is NaN and only the negated form accepts.
  matches: boolean;

  // Size of the larger of the two sets - the denominator for overlapRatio.
  maxSize: number;

  // Count of prismcastChannelKeys entries also present in deviceChannelIds (the intersection size).
  overlapCount: number;

  // overlapCount divided by maxSize. NaN when both sets are empty.
  overlapRatio: number;
}

/**
 * Measures channel ID overlap between a Channels DVR M3U device's channel set and PrismCast's own channel keys, and reports whether the overlap clears the
 * acceptance threshold used to identify which M3U device in Channels DVR is PrismCast's own source. We use overlap-based matching rather than exact matching
 * because the channel set can drift: the user may disable channels after Channels DVR imports the playlist, or add new channels that haven't been refreshed
 * yet in the DVR. Pure and side-effect free.
 * @param deviceChannelIds - Channel IDs reported by the Channels DVR M3U device.
 * @param prismcastChannelKeys - PrismCast's own channel keys.
 * @returns The overlap counts, ratio, and accept/reject decision.
 */
export function matchesM3uDevice(deviceChannelIds: Set<string>, prismcastChannelKeys: Set<string>): DeviceOverlap {

  let overlapCount = 0;

  for(const key of prismcastChannelKeys) {

    if(deviceChannelIds.has(key)) {

      overlapCount++;
    }
  }

  const maxSize = Math.max(deviceChannelIds.size, prismcastChannelKeys.size);
  const overlapRatio = overlapCount / maxSize;

  // Written as a negated less-than so that the both-empty case (overlapRatio = NaN) accepts: `!(NaN < 0.8)` is true, while `NaN >= 0.8` is false. In practice
  // getDeviceMappings only reaches this function for devices with a non-empty Channels list, so the NaN branch fires only when prismcastChannelKeys is also
  // empty.
  return { matches: !(overlapRatio < 0.8), maxSize, overlapCount, overlapRatio };
}

/**
 * Gets device channel mappings for a DVR host, refreshing the cache if needed. The cache is keyed by host alone, so the channelsDvr. handler clears it when a
 * save changes the host or the port.
 * @param host - The DVR server hostname or IP address.
 * @param port - The DVR's API port.
 * @param now - The instant the cache's freshness is measured against.
 * @returns Map of DeviceID -> (Map of GuideNumber -> channel ID).
 */
export async function getDeviceMappings(host: string, port: number, now: number): Promise<Map<string, Map<string, string>>> {

  const cached = deviceMappingsByHost.get(host);

  // Return cached mappings if they're fresh enough.
  if(cached && ((now - cached.lastRefresh) < MAPPINGS_REFRESH_INTERVAL_MS)) {

    return cached.mappings;
  }

  // Fetch fresh device data.
  const devices = await fetchFromDvr<ChannelsDvrDevice>(host, port, "/devices");

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

    // Identify PrismCast's M3U source by measuring channel ID overlap via matchesM3uDevice() - see that function for why overlap-based matching is used instead
    // of exact matching, and why the accept threshold is written as a negated less-than.
    const deviceChannelIds = new Set(device.Channels.map((ch) => ch.ID));
    const overlap = matchesM3uDevice(deviceChannelIds, prismcastChannelKeys);

    if(!overlap.matches) {

      LOG.debug("streaming:showinfo", "Skipping M3U device %s: low channel overlap (%d/%d = %d%%).",
        device.DeviceID, overlap.overlapCount, overlap.maxSize, Math.round(overlap.overlapRatio * 100));

      continue;
    }

    LOG.debug("streaming:showinfo", "Matched M3U device %s as PrismCast source (%d channels, %d%% overlap).",
      device.DeviceID, device.Channels.length, Math.round(overlap.overlapRatio * 100));

    // Build guide number -> channel ID map for this device and extract logo URLs. Logo URLs are cached by station ID (resolved from the channel key via
    // getChannelStationId) so that Pacific channels and service variants all share the same logo entry.
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
 * @param port - The DVR's API port.
 * @returns Map of channel ID -> show name for currently airing programs.
 */
async function getGuideShowNames(host: string, port: number): Promise<Map<string, string>> {

  // Get device IDs from the mappings cache (should already be populated).
  const deviceCache = deviceMappingsByHost.get(host);

  if(!deviceCache || (deviceCache.deviceIds.length === 0)) {

    return new Map();
  }

  // Fetch guide data from all M3U devices in parallel.
  const guideResults = await Promise.all(
    deviceCache.deviceIds.map(async (deviceId) => fetchFromDvr<ChannelsDvrGuideEntry>(host, port, "/devices/" + deviceId + "/guide/now"))
  );

  // Build channel ID -> show name map from all devices.
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
 * Fetches JSON data from a Channels DVR API endpoint. The caller hands it the host and the port, so a request made while a save is being reconciled can reach
 * the DVR the candidate configuration names rather than the one the running configuration still holds.
 * @param host - The DVR server hostname or IP address.
 * @param port - The DVR's API port.
 * @param path - The API path (e.g., "/devices" or "/dvr/jobs").
 * @param clock - The clock this request's bound arms on. Defaults to the poller's own clock, read at call time, so a request the poller makes and a request the
 *                pretune scheduler makes each land on the timeline its caller is driving.
 * @returns Array of results, empty array on any error.
 */
export async function fetchFromDvr<T>(host: string, port: number, path: string, clock: Clock = pollingClock): Promise<T[]> {

  const url = "http://" + host + ":" + String(port) + path;

  // The bound carries this error as its abort reason, which is what lets the catch below tell our own lapse apart from every other failure by reference rather
  // than by parsing a name the platform chooses.
  const lapse = new Error("Channels DVR request to " + host + " timed out after " + String(API_TIMEOUT_MS) + "ms.");
  const bound = timeoutSignal(API_TIMEOUT_MS, { clock, reason: lapse });

  try {

    const response = await fetch(url, {

      headers: { "Accept": "application/json" },
      signal: bound.signal
    });

    if(!response.ok) {

      return [];
    }

    return await response.json() as T[];
  } catch(error) {

    if(error instanceof Error) {

      // Our own lapse is the ordinary outcome for an unreachable server, so it stays silent; anything else is a failure worth a line.
      if(error !== lapse) {

        LOG.debug("streaming:showinfo", "Failed to fetch %s from %s: %s.", path, host, formatError(error));
      }
    }

    return [];
  } finally {

    bound.cancel();
  }
}

/**
 * Persists the DVR host to the config file so it survives restarts. setDvrHost has already made it the running host, and a write that fails leaves the file
 * behind the running configuration until the next save reconciles the two from the file.
 * @param host - The DVR server hostname or IP address.
 */
async function persistDvrHost(host: string): Promise<void> {

  try {

    await mutateConfig((config) => {

      config.channelsDvr ??= {};
      config.channelsDvr.host = host;
    });
  } catch(error) {

    LOG.debug("streaming:showinfo", "Failed to persist DVR host: %s.", formatError(error));
  }
}

// Logo Population.

/**
 * Populates the logos from the running DVR host and port when a host is known, the population the poller's start, its daily refresh, and a discovered host
 * run.
 */
function populateRunningChannelLogos(): void {

  const host = getDvrHost();

  if(host) {

    void populateChannelLogos(host, CONFIG.channelsDvr.port);
  }
}

/**
 * Populates the channel logo cache in two tiers. Tier 1 fetches logo URLs from the DVR's /devices endpoint (covers all channels in the M3U playlist). Tier 2 runs
 * TMS station name searches for any remaining channels with station IDs not covered by tier 1. Runs when a DVR host becomes known or changes, when a save changes
 * the port, and every 24 hours to pick up network rebrands. The host and the port are arguments, so the reconcile's handler can hand it the candidate's values
 * while the running configuration still holds the previous ones.
 * @param host - The DVR server hostname or IP address.
 * @param port - The DVR's API port.
 */
async function populateChannelLogos(host: string, port: number): Promise<void> {

  // One instant for the whole operation, as the show-name update reads one for its own.
  const now = pollingClock.now();

  // Tier 1: fetch device data and extract logos. getDeviceMappings() handles the /devices fetch, device matching, and logo extraction into the cache as a side
  // effect. The 5-minute mapping cache means we don't re-fetch if show name polling already called this recently.
  await getDeviceMappings(host, port, now);

  // Tier 2: search TMS by channel name for channels with station IDs not covered by tier 1. This covers disabled channels, channels without an enabled service,
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
    const found = await searchTmsStationLogo(host, port, channelName, stationId);

    if(found) {

      tier2Count++;
    }
  }

  LOG.debug("streaming:logos", "Logo population complete: %d cached, %d from TMS search, %d total.", cachedCount, tier2Count, cachedCount + tier2Count);

  // Emit a channel-update SSE event with all resolved logos so any open channels tab can populate logo images without a page reload.
  const logos: Record<string, string> = {};

  for(const entry of listing) {

    const logoUrl = getChannelLogo(entry.key);

    if(logoUrl) {

      logos[entry.key] = logoUrl;
    }
  }

  if(Object.keys(logos).length > 0) {

    emitChannelUpdate({ logos });
  }
}

/**
 * Searches the TMS stations endpoint for a channel's logo by name. If a result matches the target station ID, uses that logo. Otherwise uses the first result
 * with a valid logo URL (same brand, different regional feed). Caches the result if found.
 * @param host - The DVR server hostname or IP address.
 * @param port - The DVR's API port.
 * @param channelName - The channel display name to search for (e.g., "AMC", "Animal Planet").
 * @param targetStationId - The Gracenote station ID to match against results.
 * @returns True if a logo was found and cached, false otherwise.
 */
async function searchTmsStationLogo(host: string, port: number, channelName: string, targetStationId: string): Promise<boolean> {

  const results = await fetchFromDvr<TmsStationResult>(host, port, "/tms/stations/" + encodeURIComponent(channelName));

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

  const host = getDvrHost();

  if(!host) {

    return;
  }

  void searchTmsStationLogo(host, CONFIG.channelsDvr.port, channelName, stationId);
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

/**
 * Realizes a saved change to the DVR host or port. The device mappings were fetched from the DVR the running configuration names, so the cache is cleared, and
 * the logos repopulate from the candidate's host and port, which the running configuration takes only once the reconcile commits. The population runs without
 * being awaited, so the save answers without waiting on the DVR, and a host the save clears populates nothing. Nothing here can fail, so the handler refuses
 * nothing.
 * @param _changes - The changes under the handler's prefix; the candidate carries the host and the port, so the handler reads them there instead.
 * @param next - The candidate running configuration.
 * @returns No rejections.
 */
async function applyDvrConfigChanges(_changes: readonly ConfigChange[], next: Readonly<Config>): Promise<readonly ChangeRejection[]> {

  deviceMappingsByHost.clear();

  if(next.channelsDvr.host.length > 0) {

    void populateChannelLogos(next.channelsDvr.host, next.channelsDvr.port);
  }

  return [];
}

// Module-load side effect: register the handler once per process, as every config-change handler registers, so it is in place before the first save can reach
// the reconcile.
registerConfigChangeHandler("channelsDvr.", applyDvrConfigChanges);
