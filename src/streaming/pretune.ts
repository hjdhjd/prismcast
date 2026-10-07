/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pretune.ts: Predictive channel pretuning from Channels DVR schedule.
 */
import { LOG, formatError } from "../utils/index.ts";
import { TimerRegistry, systemClock } from "homebridge-plugin-utils";
import { clearAllPretuneSafetyTimers, setPretuneSafetyTimer, startPretuneSafetyTimers } from "./pretuneTimers.ts";
import { fetchFromDvr, getDeviceMappings, getDvrHost } from "./showInfo.ts";
import { getChannelStreamId, terminateStream } from "./lifecycle.ts";
import { initializeStream, validateChannel } from "./hls.ts";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "homebridge-plugin-utils";
import type { Nullable } from "../types/index.ts";
import { emitCurrentSystemStatus } from "../browser/index.ts";
import { getStream } from "./registry.ts";

/* This module polls the Channels DVR schedule API to discover upcoming recordings and pretunes channels 30 seconds before they start. When the DVR requests the
 * stream, it's already live with buffered segments - achieving near-instant tuning instead of 3-7 second cold starts.
 *
 * The polling loop runs every 60 seconds, checking for jobs starting within a 5-minute scheduling horizon. For each eligible job (PrismCast channel as the DVR's
 * preferred source), a per-job timer is armed on the scheduler's clock for 30 seconds before the recording start time. When the timer fires, the module checks
 * for conflicts, validates the channel, and calls initializeStream() with the preTuned flag. A safety timeout tears down unclaimed streams 90 seconds after the
 * scheduled start.
 *
 * Key design decisions:
 * - Only pretune when a PrismCast guide number is the FIRST entry in the job's channels array (DVR's preferred source).
 * - Skip channels that are already streaming - no duplicate streams for the same channel.
 * - Pretune alongside existing streams freely; capacity enforcement is handled by initializeStream().
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
export interface ScheduledJob {

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

/* PretuneDeps is the external-I/O surface the pretune decision logic composes on: the DVR data-acquisition calls (getDvrHost, fetchFromDvr, getDeviceMappings) and the
 * expensive go-action (initializeStream). It is injected as a default parameter threaded from startPretunePolling so a test can substitute in-memory stubs at the same
 * injection boundary - no loader mock - while production uses the real defaultPretuneDeps. validateChannel, the registry, lifecycle, and the safety-timer registry stay
 * direct imports because they are not the substituted boundary. fetchFromDvr is narrowed to the ScheduledJob rows pretune actually reads. The deps carry the
 * library's Clock as one of their members.
 */
export interface PretuneDeps {

  // The time source every read, the poll cadence, the per-job timers, and the safety timers run on. Injected as a Clock (rather than direct platform calls) so a
  // test drives the whole schedule on one virtual timeline and asserts it instead of waiting it out.
  readonly clock: Clock;
  readonly fetchFromDvr: (host: string, port: number, path: string, clock?: Clock) => Promise<ScheduledJob[]>;
  readonly getDeviceMappings: typeof getDeviceMappings;
  readonly getDvrHost: typeof getDvrHost;
  readonly initializeStream: typeof initializeStream;
}

const defaultPretuneDeps: PretuneDeps = { clock: systemClock, fetchFromDvr, getDeviceMappings, getDvrHost, initializeStream };

// State.

/**
 * The pair of timer registries a running scheduler owns: the poll cadence and its first poll, and one keyed one-shot per scheduled job.
 */
interface SchedulerRegistries {

  // One keyed one-shot per scheduled job, keyed by job ID. Used to avoid duplicate scheduling and to clear timers when jobs disappear.
  readonly jobs: TimerRegistry;

  // The repeating poll and the deferred first poll.
  readonly polls: TimerRegistry;
}

/* The running scheduler, or null when it is stopped. Both registries are built on the scheduler's clock at start and disposed at stop, and this one binding is the
 * single statement of whether the scheduler is running, so the two registries can never disagree about it.
 */
let scheduler: Nullable<SchedulerRegistries> = null;

// Public API.

/**
 * Starts the pretune polling loop. Should be called on server startup.
 */
export function startPretunePolling(deps: PretuneDeps = defaultPretuneDeps): void {

  if(scheduler) {

    return;
  }

  const registries = { jobs: new TimerRegistry({ clock: deps.clock }), polls: new TimerRegistry({ clock: deps.clock }) };

  scheduler = registries;

  startPretuneSafetyTimers(deps.clock);

  /* The first poll runs a few seconds after the start and the cadence follows it. Each poll reads the DVR host and port from the running configuration when it
   * runs, so a host the boot read from the file or a save changed is the one it polls. Each poll is handed the registry it must arm on, by identity, so a poll
   * still running across a stop arms on the disposed registry it started with rather than on whatever the module binding holds when its awaits resume.
   */
  registries.polls.schedule(() => {

    void pollForUpcomingJobs(deps, registries.jobs);
  }, 5000);

  registries.polls.setInterval("poll", () => {

    void pollForUpcomingJobs(deps, registries.jobs);
  }, POLL_INTERVAL_MS);
}

/**
 * Stops the pretune polling loop and clears all pending timers. Should be called on server shutdown.
 */
export function stopPretunePolling(): void {

  // Disposing both registries drains every pending poll and per-job timer and makes any later arm on them inert.
  scheduler?.jobs.dispose();
  scheduler?.polls.dispose();
  scheduler = null;

  // Clear all safety timers via their owning registry.
  clearAllPretuneSafetyTimers();
}

// Internal Functions.

/**
 * Polls the Channels DVR API for upcoming scheduled recordings and arms pretune timers for eligible jobs.
 * @param deps - The injected external-I/O surface and clock.
 * @param jobRegistry - The per-job registry this poll arms on, handed in by the timer that fired it. Named for the registry rather than for the jobs because the
 *                      DVR job list is already a body-level binding here.
 */
async function pollForUpcomingJobs(deps: PretuneDeps, jobRegistry: TimerRegistry): Promise<void> {

  const host = deps.getDvrHost();

  if(!host) {

    return;
  }

  // The port is read with the host, so one poll reaches one DVR address throughout.
  const port = CONFIG.channelsDvr.port;
  const jobs = await deps.fetchFromDvr(host, port, "/api/v1/jobs", deps.clock);

  if(jobs.length === 0) {

    return;
  }

  const now = deps.clock.now();
  const horizon = now + SCHEDULING_HORIZON_MS;

  // Track which job IDs we see this poll cycle so we can clear timers for removed jobs.
  const seenJobIds = new Set<string>();

  // Get the device mappings once for resolving guide numbers. The cache ensures this is fast on repeated calls.
  const mappings = await deps.getDeviceMappings(host, port, now);

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
    const guideNumber = job.channels[0];

    if(!guideNumber) {

      continue;
    }

    const channelId = resolveGuideNumber(mappings, guideNumber);

    if(!channelId) {

      continue;
    }

    seenJobIds.add(job.id);

    // Skip if a timer is already scheduled for this job.
    if(jobRegistry.has(job.id)) {

      continue;
    }

    // Calculate when to pretune. If the pretune time has already passed but the start time hasn't, pretune immediately.
    const pretuneTime = startMs - PRETUNE_LEAD_MS;
    const effectiveDelay = Math.max(0, pretuneTime - now);

    LOG.debug("streaming:pretune", "Scheduling pretune for '%s' (%s) in %ds.", job.name, channelId, Math.round(effectiveDelay / 1000));

    // The registry removes a keyed one-shot's entry before running its callback, so the fired job's key is already gone by the time the pretune begins.
    jobRegistry.setTimeout(job.id, () => {

      void pretuneChannel(channelId, job.name, startMs, deps);
    }, effectiveDelay);
  }

  // Clear timers for jobs that disappeared from the API (cancelled, rescheduled, or moved outside the horizon). The registry answers its own key iterator, so
  // clearing the key the walk is standing on is well-defined.
  for(const jobId of jobRegistry.keys()) {

    if(!seenJobIds.has(jobId)) {

      jobRegistry.clear(jobId);

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
 * Pretunes a channel ahead of a scheduled recording. Checks for conflicts, validates the channel, initializes the stream with the preTuned flag, and sets up a
 * safety timeout for unclaimed streams. Retries on failure up to MAX_RETRIES times within the pretune window.
 * @param channelId - The PrismCast channel key (e.g., "cnn").
 * @param jobName - The program title for logging (e.g., "Anderson Cooper 360").
 * @param startTimeMs - The recording start time in milliseconds.
 */
async function pretuneChannel(channelId: string, jobName: string, startTimeMs: number, deps: PretuneDeps): Promise<void> {

  // Check if the channel is already streaming.
  const existingStreamId = getChannelStreamId(channelId);

  if(existingStreamId !== undefined) {

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

    try {

      // eslint-disable-next-line no-await-in-loop
      const streamId = await deps.initializeStream({

        channel: validation.channel,
        channelName: channelId,
        clientAddress: null,
        preTuned: true,
        url: validation.channel.url
      });

      if(streamId !== null) {

        const leadSeconds = Math.round((startTimeMs - deps.clock.now()) / 1000);

        LOG.info("Pretuned %s for %s (starts in %ds).", displayName, jobName, leadSeconds);

        // Set a safety timeout to tear down the stream if no real client connects within 90 seconds of the scheduled start.
        const safetyDelay = Math.max(0, (startTimeMs + SAFETY_TIMEOUT_MS) - deps.clock.now());

        setPretuneSafetyTimer(streamId, () => {

          const stream = getStream(streamId);

          if(stream?.preTuned) {

            LOG.info("No client connected for pretuned %s. Ending stream.", displayName);

            terminateStream(streamId, channelId, "pretune safety timeout");
            void emitCurrentSystemStatus();
          }
        }, safetyDelay);

        return;
      }
    } catch(error) {

      LOG.warn("Pretune attempt %d/%d failed for %s: %s.", attempts, MAX_RETRIES, displayName, formatError(error));
    }

    // Stop retrying if we're past the scheduled start time.
    if(deps.clock.now() >= startTimeMs) {

      LOG.debug("streaming:pretune", "Past start time for %s. Stopping pretune attempts.", channelId);

      return;
    }

    // Brief delay before retry.
    if(attempts < MAX_RETRIES) {

      // eslint-disable-next-line no-await-in-loop
      await deps.clock.delay(RETRY_DELAY_MS);
    }
  }

  LOG.warn("All %d pretune attempts failed for %s.", MAX_RETRIES, displayName);
}
