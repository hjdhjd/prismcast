/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsSegments.ts: HLS segment storage functions for PrismCast.
 */
import { CONFIG } from "../config/index.js";
import { LOG } from "../utils/index.js";
import type { StreamRegistryEntry } from "./registry.js";
import { getStream } from "./registry.js";

/* This module provides functions for storing and retrieving HLS segments, playlists, and init segments. All data is stored in the stream registry's HLSState, which is
 * the single source of truth for stream data. Key responsibilities:
 *
 * 1. Store init segment, media segments, and playlists per stream
 * 2. Enforce segment count limits to control memory usage
 * 3. Provide access to segments and playlists for HTTP handlers
 *
 * Data types:
 * - Init segment (init.mp4): Contains codec configuration, sent once at stream start, retained for stream lifetime
 * - Media segments (.m4s or .ts): Contain audio/video data, rotated based on maxSegments config
 * - Playlist (.m3u8): Updated as new segments are produced
 *
 * Note: Stream lifecycle (creation, cleanup) is managed by the registry. This module focuses solely on segment storage operations.
 */

// Segment Management.

/**
 * Stores a media segment. Enforces the segment count limit by removing the oldest segment when necessary. Handles both capture-mode .m4s segments from the fMP4
 * segmenter and native-mode .ts segments from the HLS proxy.
 * @param streamId - The numeric stream ID.
 * @param filename - The segment filename (e.g., "segment0.m4s").
 * @param data - The segment binary data.
 */
export function storeSegment(streamId: number, filename: string, data: Buffer): void {

  const stream = getStream(streamId);

  if(!stream) {

    LOG.debug("streaming:hls", "Attempted to store segment for unknown stream %s.", streamId);

    return;
  }

  stream.hls.segments.set(filename, data);

  // Notify MPEG-TS consumers that a new segment is available. Emit before rotation so the data is guaranteed accessible in the Map.
  stream.hls.segmentEmitter.emit("segment", filename, data);

  // Enforce segment limit by removing oldest segments. JavaScript Maps maintain insertion order, so the first key is always the oldest segment.
  while(stream.hls.segments.size > CONFIG.hls.maxSegments) {

    const oldestKey = stream.hls.segments.keys().next().value;

    if(oldestKey === undefined) {

      break;
    }

    stream.hls.segments.delete(oldestKey);
  }
}

/**
 * Gets a media segment by filename.
 * @param streamId - The numeric stream ID.
 * @param filename - The segment filename.
 * @returns The segment data, or undefined if not found.
 */
export function getSegment(streamId: number, filename: string): Buffer | undefined {

  const stream = getStream(streamId);

  if(!stream) {

    return undefined;
  }

  return stream.hls.segments.get(filename);
}

/**
 * Returns the number of media segments currently stored for a stream. Used by the segmenter to clamp the playlist window to segments that actually exist in storage.
 * @param streamId - The numeric stream ID.
 * @returns The number of stored segments, or 0 if the stream is not found.
 */
export function getSegmentCount(streamId: number): number {

  return getStream(streamId)?.hls.segments.size ?? 0;
}

// Audio Segment Management.

/**
 * Stores an audio segment for streams with separate audio renditions. Enforces the same segment count limit as video segments by removing the oldest audio segment
 * when necessary.
 * @param streamId - The numeric stream ID.
 * @param filename - The audio segment filename (e.g., "audio0.ts").
 * @param data - The segment binary data.
 */
export function storeAudioSegment(streamId: number, filename: string, data: Buffer): void {

  const stream = getStream(streamId);

  if(!stream) {

    LOG.debug("streaming:hls", "Attempted to store audio segment for unknown stream %s.", streamId);

    return;
  }

  stream.hls.audioSegments.set(filename, data);

  // Notify consumers that a new audio segment is available.
  stream.hls.segmentEmitter.emit("audioSegment", filename, data);

  // Enforce segment limit by removing oldest audio segments.
  while(stream.hls.audioSegments.size > CONFIG.hls.maxSegments) {

    const oldestKey = stream.hls.audioSegments.keys().next().value;

    if(oldestKey === undefined) {

      break;
    }

    stream.hls.audioSegments.delete(oldestKey);
  }
}

/**
 * Gets an audio segment by filename.
 * @param streamId - The numeric stream ID.
 * @param filename - The audio segment filename.
 * @returns The segment data, or undefined if not found.
 */
export function getAudioSegment(streamId: number, filename: string): Buffer | undefined {

  const stream = getStream(streamId);

  if(!stream) {

    return undefined;
  }

  return stream.hls.audioSegments.get(filename);
}

// Audio Playlist Management.

/**
 * Updates the audio variant playlist content for a stream with separate audio renditions.
 * @param streamId - The numeric stream ID.
 * @param content - The audio m3u8 playlist content.
 */
export function updateAudioPlaylist(streamId: number, content: string): void {

  const stream = getStream(streamId);

  if(!stream) {

    LOG.debug("streaming:hls", "Attempted to update audio playlist for unknown stream %s.", streamId);

    return;
  }

  stream.hls.audioPlaylist = content;
}

/**
 * Gets the current audio variant playlist for a stream.
 * @param streamId - The numeric stream ID.
 * @returns The audio playlist content, or undefined if not found.
 */
export function getAudioPlaylist(streamId: number): string | undefined {

  // eslint-disable-next-line @typescript-eslint/prefer-nullish-coalescing -- intentional: convert empty string to undefined.
  return getStream(streamId)?.hls.audioPlaylist || undefined;
}

// Video Playlist Management (Separate Audio Streams).

/**
 * Updates the video variant playlist content for a stream with separate audio renditions. When hasAudio is true, the main playlist becomes the master playlist, and
 * the video variant playlist is stored separately.
 * @param streamId - The numeric stream ID.
 * @param content - The video m3u8 playlist content.
 */
export function updateVideoPlaylist(streamId: number, content: string): void {

  const stream = getStream(streamId);

  if(!stream) {

    LOG.debug("streaming:hls", "Attempted to update video playlist for unknown stream %s.", streamId);

    return;
  }

  stream.hls.videoPlaylist = content;
}

/**
 * Gets the current video variant playlist for a stream with separate audio renditions.
 * @param streamId - The numeric stream ID.
 * @returns The video playlist content, or undefined if not found.
 */
export function getVideoPlaylist(streamId: number): string | undefined {

  // eslint-disable-next-line @typescript-eslint/prefer-nullish-coalescing -- intentional: convert empty string to undefined.
  return getStream(streamId)?.hls.videoPlaylist || undefined;
}

// Init Segment Management.

/**
 * Stores the fMP4 initialization segment for a stream. The init segment contains codec configuration and is sent once at stream start. Unlike media segments, it is
 * retained for the entire stream lifetime (not subject to rotation).
 * @param streamId - The numeric stream ID.
 * @param data - The init segment binary data.
 */
export function storeInitSegment(streamId: number, data: Buffer): void {

  const stream = getStream(streamId);

  if(!stream) {

    LOG.debug("streaming:hls", "Attempted to store init segment for unknown stream %s.", streamId);

    return;
  }

  const isFirstInit = stream.hls.initSegment === null;

  stream.hls.initSegment = data;

  // Notify MPEG-TS consumers that the init segment is available.
  stream.hls.segmentEmitter.emit("initSegment", data);

  if(isFirstInit) {

    stream.hls.signalInitSegmentReady();

    const elapsed = ((Date.now() - stream.startTime.getTime()) / 1000).toFixed(3);

    LOG.debug("timing:startup", "Init segment ready in %ss.", elapsed);
  }
}

/**
 * Gets the fMP4 initialization segment for a stream.
 * @param streamId - The numeric stream ID.
 * @returns The init segment data, or undefined if not found or not yet received.
 */
export function getInitSegment(streamId: number): Buffer | undefined {

  const stream = getStream(streamId);

  if(!stream) {

    return undefined;
  }

  return stream.hls.initSegment ?? undefined;
}

// Playlist Management.

/**
 * Updates the playlist content for a stream. If this is the first playlist, signals that the stream is ready.
 * @param streamId - The numeric stream ID.
 * @param content - The m3u8 playlist content.
 */
export function updatePlaylist(streamId: number, content: string): void {

  const stream = getStream(streamId);

  if(!stream) {

    LOG.debug("streaming:hls", "Attempted to update playlist for unknown stream %s.", streamId);

    return;
  }

  const isFirstRealPlaylist = !stream.hls.hasRealPlaylist;

  stream.hls.playlist = content;

  if(isFirstRealPlaylist) {

    stream.hls.hasRealPlaylist = true;
    stream.hls.signalPlaylistReady();

    // Cancel the deferred preroll timer now that real content is available. The timer may still be running for native streams where browser setup completed quickly
    // but the proxy's first poll cycle took longer than the preroll delay (PREROLL_DELAY_MS). Without this, the timer would fire after real content is already
    // flowing, uselessly seeding preroll state. For streams where the timer already fired (preroll is active), this is a no-op — the timer handle is already null.
    if(stream.hls.prerollTimer) {

      clearTimeout(stream.hls.prerollTimer);
      stream.hls.prerollTimer = null;
    }

    const elapsed = ((Date.now() - stream.startTime.getTime()) / 1000).toFixed(3);

    LOG.debug("streaming:preroll", "Live playlist ready for stream %d.", streamId);
    LOG.debug("timing:startup", "First playlist ready in %ss.", elapsed);
  }
}

/**
 * Gets the current playlist for a stream.
 * @param streamId - The numeric stream ID.
 * @returns The playlist content, or undefined if not found.
 */
export function getPlaylist(streamId: number): string | undefined {

  return getStream(streamId)?.hls.playlist;
}

/**
 * Waits for the first playlist to be available for a stream.
 * @param streamId - The numeric stream ID.
 * @param timeout - Maximum time to wait in milliseconds.
 * @returns True if playlist is ready, false if timeout or stream not found.
 */
export async function waitForPlaylist(streamId: number, timeout: number): Promise<boolean> {

  return waitForReady(streamId, async (stream) => stream.hls.playlistReady, timeout);
}

/**
 * Waits for the first init segment to be available for a stream. Used by MPEG-TS consumers to wait for codec configuration before starting their FFmpeg remuxer.
 * @param streamId - The numeric stream ID.
 * @param timeout - Maximum time to wait in milliseconds.
 * @returns True if init segment is ready, false if timeout or stream not found.
 */
export async function waitForInitSegment(streamId: number, timeout: number): Promise<boolean> {

  return waitForReady(streamId, async (stream) => stream.hls.initSegmentReady, timeout);
}

// Internal Helpers.

/**
 * Races a readiness promise from a stream's HLS state against a timeout. Returns true if the promise resolves before the timeout, false if the timeout fires first or
 * the stream doesn't exist.
 * @param streamId - The numeric stream ID.
 * @param getPromise - Accessor that extracts the readiness promise from the stream's HLS state.
 * @param timeout - Maximum time to wait in milliseconds.
 * @returns True if ready before timeout, false otherwise.
 */
async function waitForReady(streamId: number, getPromise: (stream: StreamRegistryEntry) => Promise<void>, timeout: number): Promise<boolean> {

  const stream = getStream(streamId);

  if(!stream) {

    return false;
  }

  let timer: ReturnType<typeof setTimeout> | undefined;

  const timeoutPromise = new Promise<boolean>((resolve) => {

    timer = setTimeout(() => { resolve(false); }, timeout);
  });

  const readyPromise = getPromise(stream).then(() => true);
  const result = await Promise.race([ readyPromise, timeoutPromise ]);

  // Clear the timeout timer to prevent an orphaned timer from firing after the result is already determined. No-op if the timeout already fired.
  if(timer) {

    clearTimeout(timer);
  }

  return result;
}
