/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsResume.ts: HLS sequence number persistence across restarts.
 */
import { LOG, formatError } from "../utils/index.js";
import type { Nullable } from "../types/index.js";
import fs from "node:fs";
import { getResumeFilePath } from "../config/paths.js";

/* When PrismCast restarts mid-recording, HLS media sequences reset to 0. Channels DVR detects "Playlist reset to a lower sequence" and produces unpredictable
 * timestamps in the recording. This module persists final sequence numbers at shutdown and seeds from them on restart so sequences always move forward.
 *
 * The data is only relevant for ~90 seconds after shutdown — just long enough for Channels DVR to reconnect. The file is written once at shutdown and deleted
 * immediately after loading at the next startup.
 */

// TTL for resume entries. Entries older than this are discarded on load.
const RESUME_TTL = 90_000;

/** Serialized format for JSON persistence. BigInt values are stored as strings; Buffer as base64. */
interface ResumeEntryJSON {

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
  initSegment: Nullable<Buffer>;
  initVersion: number;
  segmentIndex: number;
  trackTimestamps: Map<number, bigint>;
}

/**
 * Resume data returned to the caller for seeding a new segmenter.
 */
export interface ResumeData {

  initSegment: Nullable<Buffer>;
  initVersion: number;
  segmentIndex: number;
  trackTimestamps: Map<number, bigint>;
}

// In-memory map of channel name to resume entry. Populated at startup, consumed as streams reconnect.
const resumeMap = new Map<string, ResumeEntry>();

/**
 * Loads resume state from disk into memory. Called once at startup after config loading. The file is deleted immediately after reading — it only needs to exist
 * between shutdown and the next startup. If the file is missing or corrupt, the map stays empty and all streams start at 0 (today's behavior).
 */
export function loadResumeState(): void {

  const filePath = getResumeFilePath();

  let raw: string;

  try {

    raw = fs.readFileSync(filePath, "utf-8");
  } catch {

    // File does not exist — clean start.
    return;
  }

  // Delete the file immediately. It has served its purpose.
  try {

    fs.unlinkSync(filePath);
  } catch {

    // Non-fatal — the file will be overwritten on next shutdown.
  }

  let parsed: Record<string, ResumeEntryJSON>;

  try {

    parsed = JSON.parse(raw) as Record<string, ResumeEntryJSON>;
  } catch {

    LOG.warn("Corrupt hls-resume.json discarded.");

    return;
  }

  const now = Date.now();
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

    resumeMap.set(channel, {

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
 * Consumes resume data for a channel. Returns the seeding parameters if the entry exists and is within TTL, or null if no resume data is available. The entry is
 * removed from the in-memory map after consumption (each channel resumes at most once).
 * @param channelName - The channel key to look up.
 * @returns Resume data for seeding the segmenter, or null.
 */
export function consumeResumeData(channelName: string): Nullable<ResumeData> {

  const entry = resumeMap.get(channelName);

  if(!entry) {

    return null;
  }

  // Remove the entry regardless of TTL check — expired entries should not linger.
  resumeMap.delete(channelName);

  // Check TTL again in case time has passed since loadResumeState().
  if((Date.now() - entry.timestamp) > RESUME_TTL) {

    return null;
  }

  LOG.info("Resuming HLS sequence for '%s' from segment %d.", channelName, entry.segmentIndex);

  return {

    initSegment: entry.initSegment,
    initVersion: entry.initVersion + 1,
    segmentIndex: entry.segmentIndex,
    trackTimestamps: entry.trackTimestamps
  };
}

/**
 * Serializes a resume entry for JSON persistence. Converts Map<number, bigint> to Record<string, string> and Buffer to base64.
 */
function serializeEntry(
  initSegment: Nullable<Buffer>, initVersion: number, segmentIndex: number, timestamp: number, trackTimestamps: Map<number, bigint>
): ResumeEntryJSON {

  const serializedTimestamps: Record<string, string> = {};

  for(const [ key, value ] of trackTimestamps) {

    serializedTimestamps[String(key)] = String(value);
  }

  return {

    initSegment: initSegment ? initSegment.toString("base64") : null,
    initVersion,
    segmentIndex,
    timestamp,
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
 */
export function saveResumeState(entries: ResumeStreamData[]): void {

  const now = Date.now();
  const merged = new Map<string, ResumeEntryJSON>();

  // Carry forward unconsumed entries that are still within TTL.
  for(const [ channel, entry ] of resumeMap) {

    if((now - entry.timestamp) <= RESUME_TTL) {

      merged.set(channel, serializeEntry(entry.initSegment, entry.initVersion, entry.segmentIndex, entry.timestamp, entry.trackTimestamps));
    }
  }

  // Active stream data takes precedence over carried-forward entries.
  for(const stream of entries) {

    merged.set(stream.channelName, serializeEntry(stream.initSegment, stream.initVersion, stream.segmentIndex, now, stream.trackTimestamps));
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

    fs.writeFileSync(getResumeFilePath(), JSON.stringify(obj), "utf-8");

    LOG.info("Saved HLS resume state for %d channel%s.", merged.size, merged.size === 1 ? "" : "s");
  } catch(error) {

    LOG.warn("Failed to save HLS resume state: %s.", formatError(error));
  }
}
