/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsResume.ts: HLS sequence number persistence across restarts.
 */
import { LOG, formatDuration, formatError, stringifySorted } from "../utils/index.ts";
import { computeTimelinePosition, parseInitSegmentTrackInfo } from "./mp4Parser.ts";
import type { Nullable } from "../types/index.ts";
import fs from "node:fs";
import { getResumeFilePath } from "../config/paths.ts";

/* When PrismCast restarts mid-recording, HLS media sequences reset to 0. Channels DVR detects "Playlist reset to a lower sequence" and produces unpredictable
 * timestamps in the recording. This module persists final sequence numbers at shutdown and seeds from them on restart so sequences always move forward.
 *
 * The data is only relevant for ~90 seconds after shutdown - just long enough for Channels DVR to reconnect. The file is written once at shutdown and deleted
 * immediately after loading at the next startup.
 */

// TTL for resume entries. Entries older than this are discarded on load.
const RESUME_TTL = 90000;

/** Serialized format for JSON persistence. BigInt values are stored as strings; Buffer as base64. */
interface ResumeEntryJSON {

  discontinuityCount?: number;
  initSegment: Nullable<string>;
  initVersion: number;
  segmentIndex: number;
  timestamp: number;
  trackTimestamps: Record<string, string>;
}

/**
 * In-memory resume entry with deserialized types.
 */
interface ResumeEntry {

  discontinuityCount: number;
  initSegment: Nullable<Buffer>;
  initVersion: number;
  segmentIndex: number;
  timestamp: number;
  trackTimestamps: Map<number, bigint>;
}

/**
 * Data collected from an active stream at shutdown, passed in by the shutdown handler.
 */
export interface ResumeStreamData {

  channelName: string;
  discontinuityCount: number;
  initSegment: Nullable<Buffer>;
  initVersion: number;
  segmentIndex: number;
  trackTimestamps: Map<number, bigint>;
}

/**
 * Where a resumed stream's media sequence and discontinuity sequence continue from.
 */
export interface ResumePosition {

  // How many discontinuity markers the previous session's playlists had emitted, which the resumed discontinuity sequence continues from.
  readonly discontinuityCount: number;

  // The segment index the resumed media sequence continues from, the last segment the previous session completed.
  readonly segmentIndex: number;
}

/**
 * Resume data returned to the caller for seeding a new segmenter, its position included.
 */
export interface ResumeData extends ResumePosition {

  initSegment: Nullable<Buffer>;
  initVersion: number;
  trackTimestamps: Map<number, bigint>;
}

// In-memory map of channel name to resume entry. Populated at startup, consumed as streams reconnect.
const resumeMap = new Map<string, ResumeEntry>();

/**
 * Loads resume state from disk into memory. Called once at startup after config loading. The file is deleted immediately after reading - it only needs to exist
 * between shutdown and the next startup. If the file is missing or corrupt, the map stays empty and all streams start at 0 (today's behavior).
 * @param now - The instant the TTL is measured against.
 */
export function loadResumeState(now: number): void {

  const filePath = getResumeFilePath();

  let raw: string;

  try {

    raw = fs.readFileSync(filePath, "utf-8");
  } catch {

    // File does not exist - clean start.
    return;
  }

  // Delete the file immediately. It has served its purpose.
  try {

    fs.unlinkSync(filePath);
  } catch {

    // Non-fatal - the file will be overwritten on next shutdown.
  }

  let parsed: Record<string, ResumeEntryJSON>;

  try {

    parsed = JSON.parse(raw) as Record<string, ResumeEntryJSON>;
  } catch {

    LOG.warn("Corrupt hls-resume.json discarded.");

    return;
  }

  let loaded = 0;

  for(const [ channel, entry ] of Object.entries(parsed)) {

    // Discard entries that have exceeded the TTL.
    if((now - entry.timestamp) > RESUME_TTL) {

      continue;
    }

    // Deserialize trackTimestamps from Record<string, string> to Map<number, bigint>.
    const trackTimestamps = new Map<number, bigint>();

    for(const [ key, value ] of Object.entries(entry.trackTimestamps)) {

      trackTimestamps.set(Number(key), BigInt(value));
    }

    // Deserialize initSegment from base64 string to Buffer.
    const initSegment = entry.initSegment ? Buffer.from(entry.initSegment, "base64") : null;

    // A file written before the discontinuity count was persisted can still be read within the TTL after an upgrade, so an entry without one loads at zero.
    resumeMap.set(channel, {

      discontinuityCount: entry.discontinuityCount ?? 0,
      initSegment,
      initVersion: entry.initVersion,
      segmentIndex: entry.segmentIndex,
      timestamp: entry.timestamp,
      trackTimestamps
    });

    loaded++;
  }

  if(loaded > 0) {

    LOG.info("Loaded HLS resume state for %d channel%s.", loaded, loaded === 1 ? "" : "s");
  }
}

/**
 * Reads resume data for a channel without removing it from the map, and without logging: announcing the resume is the caller's, through logStreamResume. Returns
 * the seeding parameters if the entry exists and is within TTL, or null if no resume data is available. Its readers are getResumePosition, which never consumes
 * the entry, and the capture segmenter's creation, which calls deleteResumeData() once its segmenter is attached. Keeping the read apart from the delete means
 * resume data survives if segmenter creation fails - the next stream start can retry with the same resume state instead of starting from scratch.
 * @param channelName - The channel key to look up.
 * @param now - The instant the TTL is measured against.
 * @returns The resume data, which seeds the capture segmenter and carries the position the registration reads, or null.
 */
export function peekResumeData(channelName: string, now: number): Nullable<ResumeData> {

  const entry = resumeMap.get(channelName);

  if(!entry) {

    return null;
  }

  // Check TTL in case time has passed since loadResumeState(). Expired entries are cleaned up by deleteResumeData() or the next loadResumeState().
  if((now - entry.timestamp) > RESUME_TTL) {

    resumeMap.delete(channelName);

    return null;
  }

  return {

    discontinuityCount: entry.discontinuityCount,
    initSegment: entry.initSegment,
    initVersion: entry.initVersion + 1,
    segmentIndex: entry.segmentIndex,
    trackTimestamps: entry.trackTimestamps
  };
}

/**
 * Returns the position a channel's resume entry continues from. This is the registration's one read: the standalone preroll playlist and the segmenter that
 * replaces it continue from the position it returns, so an entry whose TTL expires mid-tune cannot leave them disagreeing. It consumes nothing, and it copies the
 * position out of the peeked data rather than returning that data, so a stream's state never holds the persisted init segment or counters.
 * @param channelName - The channel key to look up.
 * @param now - The instant the TTL is measured against.
 * @returns The resume position, or null if no valid resume data exists.
 */
export function getResumePosition(channelName: string, now: number): Nullable<ResumePosition> {

  const resumeData = peekResumeData(channelName, now);

  return resumeData ? { discontinuityCount: resumeData.discontinuityCount, segmentIndex: resumeData.segmentIndex } : null;
}

/**
 * Options for logging a resumed stream.
 */
export interface StreamResumeLogOptions {

  // The name the line shows for the stream: its channel's display name when it has one, its key otherwise.
  displayName: string;

  // The resume data the stream continues from, as peekResumeData returned it.
  resumeData: ResumeData;
}

/**
 * Logs that a stream resumes a previous session. The line carries the prior content the previous session's output reached when the resume data measures it,
 * and is the same sentence without that figure when it does not, because a figure is reported only when it is measured.
 * @param options - The display name and the resume data.
 */
export function logStreamResume(options: StreamResumeLogOptions): void {

  const { displayName, resumeData } = options;
  const priorContentMs = measurePriorContent(resumeData);

  if(priorContentMs === null) {

    LOG.info("Resuming stream for %s from previous session.", displayName);

    return;
  }

  LOG.info("Resuming stream for %s from previous session (%s of prior content).", displayName, formatDuration(priorContentMs));
}

/* The position the previous session's output timeline reached, in milliseconds: the persisted decode-time counters over the persisted init segment's
 * timescales, converted exactly as a continuing segmenter converts the counters it is handed. The timeline includes a preroll's duration when a preroll
 * opened it, and the sessions it resumed when none did. A shutdown between a tab replacement's swap and the successor's moov persists no init segment, so
 * the line carries no figure; one between the successor's moov and a track's first moof persists that track's counter from the predecessor, and the
 * conversion assumes, as the segmenter's own does, that Chrome keeps a track's timescale across captures. Null when the resume data carries no init
 * segment or no track carries a counter together with a timescale, which is also what a malformed moov yields, because the walk finds no track in it
 * rather than throwing.
 */
function measurePriorContent(resumeData: ResumeData): Nullable<number> {

  if(!resumeData.initSegment) {

    return null;
  }

  const timescales = new Map(Array.from(parseInitSegmentTrackInfo(resumeData.initSegment), ([ trackId, info ]): [ number, number ] => [ trackId, info.timescale ]));
  const positionSec = computeTimelinePosition({ timescales, trackTimestamps: resumeData.trackTimestamps });

  return (positionSec === null) ? null : (positionSec * 1000);
}

/**
 * Removes resume data for a channel from the in-memory map. Called after the segmenter has been successfully created and piped, confirming the resume data was used.
 * @param channelName - The channel key to remove.
 */
export function deleteResumeData(channelName: string): void {

  resumeMap.delete(channelName);
}

/**
 * Serializes a resume entry for JSON persistence. Converts Map<number, bigint> to Record<string, string> and Buffer to base64.
 * @param entry - The entry to serialize.
 * @returns The entry's JSON form.
 */
function serializeEntry(entry: ResumeEntry): ResumeEntryJSON {

  const serializedTimestamps: Record<string, string> = {};

  for(const [ key, value ] of entry.trackTimestamps) {

    serializedTimestamps[String(key)] = String(value);
  }

  return {

    discontinuityCount: entry.discontinuityCount,
    initSegment: entry.initSegment ? entry.initSegment.toString("base64") : null,
    initVersion: entry.initVersion,
    segmentIndex: entry.segmentIndex,
    timestamp: entry.timestamp,
    trackTimestamps: serializedTimestamps
  };
}

/**
 * Saves resume state to disk. Merges active stream data (always takes precedence for a given channel) with unconsumed in-memory entries still within TTL. This
 * supports the rapid-restart scenario where a channel that never reconnected carries forward through multiple restart cycles.
 *
 * Called during graceful shutdown with data collected from active streams by the shutdown handler. The caller passes pre-collected stream data to avoid circular
 * dependencies with the registry module.
 * @param entries - Stream data collected from active streams at shutdown.
 * @param now - The instant the TTL is measured against, and the timestamp the active entries are written with.
 */
export function saveResumeState(entries: ResumeStreamData[], now: number): void {

  const merged = new Map<string, ResumeEntryJSON>();

  // Carry forward unconsumed entries that are still within TTL.
  for(const [ channel, entry ] of resumeMap) {

    if((now - entry.timestamp) <= RESUME_TTL) {

      merged.set(channel, serializeEntry(entry));
    }
  }

  // Active stream data takes precedence over carried-forward entries.
  for(const stream of entries) {

    merged.set(stream.channelName, serializeEntry({ ...stream, timestamp: now }));
  }

  // Nothing to save.
  if(merged.size === 0) {

    return;
  }

  // Convert the Map to a plain object for JSON serialization.
  const obj: Record<string, ResumeEntryJSON> = {};

  for(const [ channel, entry ] of merged) {

    obj[channel] = entry;
  }

  try {

    fs.writeFileSync(getResumeFilePath(), stringifySorted(obj) + "\n", "utf-8");

    LOG.info("Saved HLS resume state for %d channel%s.", merged.size, merged.size === 1 ? "" : "s");
  } catch(error) {

    LOG.warn("Failed to save HLS resume state: %s.", formatError(error));
  }
}
