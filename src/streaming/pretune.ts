/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pretune.ts: Predictive channel pretuning from Channels DVR schedule.
 */
import { LOG, delay, formatError } from "../utils/index.js";
import { fetchFromDvr, getDeviceMappings, getDvrHost } from "./showInfo.js";
import { getChannelStreamId, terminateStream } from "./lifecycle.js";
import { getStream, getStreamCount } from "./registry.js";
import { initializeStream, validateChannel } from "./hls.js";
import type { Nullable } from "../types/index.js";
import { emitCurrentSystemStatus } from "../browser/index.js";

/* This module polls the Channels DVR schedule API to discover upcoming recordings and pretunes channels 30 seconds before they start. When the DVR requests the
 * stream, it's already live with buffered segments — achieving near-instant tuning instead of 3-7 second cold starts.
 *
 * The polling loop runs every 60 seconds, checking for jobs starting within a 5-minute scheduling horizon. For each eligible job (PrismCast channel as the DVR's
 * preferred source), a precise setTimeout is scheduled for 30 seconds before the recording start time. When the timer fires, the module validates the channel,
 * checks for conflicts, and calls initializeStream() with the preTuned flag. A safety timeout tears down unclaimed streams 90 seconds after the scheduled start.
 *
 * Key design decisions:
 * - Only pretune when a PrismCast guide number is the FIRST entry in the job's channels array (DVR's preferred source).
 * - Yield to active streams — don't disrupt an existing stream for a speculative tune.
 * - Retry up to 5 times within the pretune window on failure.
 * - Safety timeout at start_time + 90s handles cancelled jobs or missed client connections.
 */

// Constants.

// How often to poll for upcoming jobs (60 seconds).
const POLL_INTERVAL_MS = 60000;

// How far ahead to look for upcoming jobs (5 minutes).
const SCHEDULING_HORIZON_MS = 300000;

// How far before the recording start to begin pretuning (30 seconds).
const PRETUNE_LEAD_MS = 30000;

// Safety timeout for unclaimed pretuned streams (90 seconds past scheduled start).
const SAFETY_TIMEOUT_MS = 90000;

// Maximum retry attempts for failed pretune.
const MAX_RETRIES = 5;

// Delay between retry attempts (5 seconds).
const RETRY_DELAY_MS = 5000;

// Types.

/**
 * Scheduled recording job from the Channels DVR /api/v1/jobs endpoint.
 */
interface ScheduledJob {

  // Guide numbers ordered by DVR preference. First entry is the preferred source.
  channels: string[];

  // Unique job identifier (e.g., "1772944140-7").
  id: string;

  // Job metadata containing lifecycle flags.
  item: {

    // Whether the recording pass has been cancelled.
    cancelled?: boolean;

    // Whether the recording has completed.
    completed?: boolean;
  };

  // Program title (e.g., "Saturday Night Live").
  name: string;

  // Whether the job has been skipped by the user.
  skipped?: boolean;

  // Recording start time as Unix timestamp in seconds.
  start_time: number;
}

// State.

// Interval handle for periodic job polling.
let pollInterval: Nullable<ReturnType<typeof setInterval>> = null;

// Active pretune timers keyed by job ID. Used to avoid duplicate scheduling and to clear timers when jobs disappear.
const activeTimers = new Map<string, ReturnType<typeof setTimeout>>();

// Safety timers keyed by stream ID. Used to tear down unclaimed pretuned streams after the scheduled start time.
const safetyTimers = new Map<number, ReturnType<typeof setTimeout>>();

// Public API.

/**
 * Starts the pretune polling loop. Should be called on server startup after show info polling is initialized.
 */
export function startPretunePolling(): void {

  if(pollInterval) {

    return;
  }

  // Run the first poll after a short delay to allow the persisted DVR host to load.
  setTimeout(() => {

    void pollForUpcomingJobs();
  }, 5000);

  pollInterval = setInterval(() => {

    void pollForUpcomingJobs();
  }, POLL_INTERVAL_MS);
}

/**
 * Stops the pretune polling loop and clears all pending timers. Should be called on server shutdown.
 */
export function stopPretunePolling(): void {

  if(pollInterval) {

    clearInterval(pollInterval);
    pollInterval = null;
  }

  // Clear all pending pretune timers.
  for(const timer of activeTimers.values()) {

    clearTimeout(timer);
  }

  activeTimers.clear();

  // Clear all safety timers.
  for(const timer of safetyTimers.values()) {

    clearTimeout(timer);
  }

  safetyTimers.clear();
}

// Internal Functions.

/**
 * Polls the Channels DVR API for upcoming scheduled recordings and schedules pretune timers for eligible jobs.
 */
async function pollForUpcomingJobs(): Promise<void> {

  const host = getDvrHost();

  if(!host) {

    return;
  }

  const jobs = await fetchFromDvr<ScheduledJob>(host, "/api/v1/jobs");

  if(jobs.length === 0) {

    return;
  }

  const now = Date.now();
  const horizon = now + SCHEDULING_HORIZON_MS;

  // Track which job IDs we see this poll cycle so we can clear timers for removed jobs.
  const seenJobIds = new Set<string>();

  // Get the device mappings once for resolving guide numbers. The cache ensures this is fast on repeated calls.
  const mappings = await getDeviceMappings(host);

  if(mappings.size === 0) {

    return;
  }

  for(const job of jobs) {

    // Skip cancelled or skipped jobs.
    if(job.item.cancelled || job.skipped) {

      continue;
    }

    const startMs = job.start_time * 1000;

    // Skip jobs outside our scheduling horizon or already started.
    if((startMs > horizon) || (startMs <= now)) {

      continue;
    }

    // Only consider jobs where the first (preferred) channel is a PrismCast channel.
    if(job.channels.length === 0) {

      continue;
    }

    const guideNumber = job.channels[0];
    const channelId = resolveGuideNumber(mappings, guideNumber);

    if(!channelId) {

      continue;
    }

    seenJobIds.add(job.id);

    // Skip if a timer is already scheduled for this job.
    if(activeTimers.has(job.id)) {

      continue;
    }

    // Calculate when to pretune. If the pretune time has already passed but the start time hasn't, pretune immediately.
    const pretuneTime = startMs - PRETUNE_LEAD_MS;
    const effectiveDelay = Math.max(0, pretuneTime - now);

    LOG.debug("streaming:pretune", "Scheduling pretune for '%s' (%s) in %ds.", job.name, channelId, Math.round(effectiveDelay / 1000));

    const timer = setTimeout(() => {

      activeTimers.delete(job.id);

      void pretuneChannel(channelId, job.name, startMs);
    }, effectiveDelay);

    activeTimers.set(job.id, timer);
  }

  // Clear timers for jobs that disappeared from the API (cancelled, rescheduled, or moved outside the horizon).
  for(const [ jobId, timer ] of activeTimers) {

    if(!seenJobIds.has(jobId)) {

      clearTimeout(timer);
      activeTimers.delete(jobId);

      LOG.debug("streaming:pretune", "Cleared timer for removed job %s.", jobId);
    }
  }
}

/**
 * Resolves a DVR guide number to a PrismCast channel ID by checking all device mappings.
 * @param mappings - Map of DeviceID to (Map of GuideNumber to channel ID).
 * @param guideNumber - The guide number to resolve (e.g., "7220").
 * @returns The PrismCast channel ID if the guide number maps to a PrismCast channel, undefined otherwise.
 */
function resolveGuideNumber(mappings: Map<string, Map<string, string>>, guideNumber: string): string | undefined {

  for(const deviceMappings of mappings.values()) {

    const channelId = deviceMappings.get(guideNumber);

    if(channelId) {

      return channelId;
    }
  }

  return undefined;
}

/**
 * Pretunes a channel ahead of a scheduled recording. Validates the channel, checks for conflicts, initializes the stream with the preTuned flag, and sets up a
 * safety timeout for unclaimed streams. Retries on failure up to MAX_RETRIES times within the pretune window.
 * @param channelId - The PrismCast channel key (e.g., "cnn").
 * @param jobName - The program title for logging (e.g., "Anderson Cooper 360").
 * @param startTimeMs - The recording start time in milliseconds.
 */
async function pretuneChannel(channelId: string, jobName: string, startTimeMs: number): Promise<void> {

  // Check if the channel is already streaming.
  const existingStreamId = getChannelStreamId(channelId);

  if((existingStreamId !== undefined) && (existingStreamId !== -1)) {

    return;
  }

  // Check if another stream is active. Pretuning should not disrupt existing streams.
  if(getStreamCount() > 0) {

    LOG.debug("streaming:pretune", "Another stream is active. Yielding pretune for %s ('%s').", channelId, jobName);

    return;
  }

  // Validate the channel.
  const validation = validateChannel(channelId);

  if(!validation.valid) {

    LOG.debug("streaming:pretune", "Pretune validation failed for %s: %s.", channelId,
      typeof validation.body === "string" ? validation.body : "validation error");

    return;
  }

  const displayName = validation.channel.name ?? channelId;

  let attempts = 0;

  while(attempts < MAX_RETRIES) {

    attempts++;

    // Re-check for concurrent streams before each attempt. Another stream may have started since the last check or retry.
    if((attempts > 1) && (getStreamCount() > 0)) {

      LOG.debug("streaming:pretune", "Another stream started during pretune retry. Yielding for %s.", channelId);

      return;
    }

    try {

      // eslint-disable-next-line no-await-in-loop
      const streamId = await initializeStream({

        channel: validation.channel,
        channelName: channelId,
        clientAddress: null,
        preTuned: true,
        url: validation.channel.url
      });

      if(streamId !== null) {

        const leadSeconds = Math.round((startTimeMs - Date.now()) / 1000);

        LOG.info("Pretuned %s for %s (starts in %ds).", displayName, jobName, leadSeconds);

        // Set a safety timeout to tear down the stream if no real client connects within 90 seconds of the scheduled start.
        const safetyDelay = Math.max(0, (startTimeMs + SAFETY_TIMEOUT_MS) - Date.now());
        const safetyTimer = setTimeout(() => {

          safetyTimers.delete(streamId);

          const stream = getStream(streamId);

          if(stream?.preTuned) {

            LOG.info("No client connected for pretuned %s. Ending stream.", displayName);

            terminateStream(streamId, channelId, "pretune safety timeout");
            void emitCurrentSystemStatus();
          }
        }, safetyDelay);

        safetyTimers.set(streamId, safetyTimer);

        return;
      }
    } catch(error) {

      LOG.warn("Pretune attempt %d/%d failed for %s: %s.", attempts, MAX_RETRIES, displayName, formatError(error));
    }

    // Stop retrying if we're past the scheduled start time.
    if(Date.now() >= startTimeMs) {

      LOG.debug("streaming:pretune", "Past start time for %s. Stopping pretune attempts.", channelId);

      return;
    }

    // Brief delay before retry.
    if(attempts < MAX_RETRIES) {

      // eslint-disable-next-line no-await-in-loop
      await delay(RETRY_DELAY_MS);
    }
  }

  LOG.warn("All %d pretune attempts failed for %s.", MAX_RETRIES, displayName);
}
