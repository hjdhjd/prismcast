/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsSegments.ts: HLS segment storage functions for PrismCast.
 */
import { CONFIG } from "../config/index.js";
import { LOG } from "../utils/index.js";
import type { StreamRegistryEntry } from "./registry.js";
import { getStream } from "./registry.js";

/*
 * HLS SEGMENT STORAGE
 *
 * This module provides functions for storing and retrieving HLS segments, playlists, and init segments. All data is stored in the stream registry's HLSState, which is
 * the single source of truth for stream data. Key responsibilities:
 *
 * 1. Store init segment, media segments, and playlists per stream
 * 2. Enforce segment count limits to control memory usage
 * 3. Provide access to segments and playlists for HTTP handlers
 *
 * For fMP4 HLS, there are three types of data:
 * - Init segment (init.mp4): Contains codec configuration, sent once at stream start, retained for stream lifetime
 * - Media segments (.m4s): Contain audio/video data, rotated based on maxSegments config
 * - Playlist (.m3u8): Updated as new segments are produced
 *
 * Note: Stream lifecycle (creation, cleanup) is managed by the registry. This module focuses solely on segment storage operations.
 */

// Segment Management.

/**
 * Stores a media segment received from the segmenter. Enforces the segment count limit by removing the oldest segment when necessary. This is for .m4s media segments,
 * not the init segment.
 * @param streamId - The numeric stream ID.
 * @param filename - The segment filename (e.g., "segment0.m4s").
 * @param data - The segment binary data.
 */
export function storeSegment(streamId: number, filename: string, data: Buffer): void {

  const stream = getStream(streamId);

  if(!stream) {

    LOG.warn("Attempted to store segment for unknown stream %s.", streamId);

    return;
  }

  stream.hls.segments.set(filename, data);

  // Notify MPEG-TS consumers that a new segment is available. Emit before rotation so the data is guaranteed accessible in the Map.
  stream.hls.segmentEmitter.emit("segment", filename, data);

  // Enforce segment limit by removing oldest segments. JavaScript Maps maintain insertion order, so the first key is always the oldest segment.
  while(stream.hls.segments.size > CONFIG.hls.maxSegments) {

    const oldestKey = stream.hls.segments.keys().next().value as string;

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

    LOG.warn("Attempted to store init segment for unknown stream %s.", streamId);

    return;
  }

  const isFirstInit = stream.hls.initSegment === null;

  stream.hls.initSegment = data;

  // Notify MPEG-TS consumers that the init segment is available.
  stream.hls.segmentEmitter.emit("initSegment", data);

  if(isFirstInit) {

    stream.hls.signalInitSegmentReady();
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

    LOG.warn("Attempted to update playlist for unknown stream %s.", streamId);

    return;
  }

  const isFirstPlaylist = stream.hls.playlist === "";

  stream.hls.playlist = content;

  if(isFirstPlaylist) {

    stream.hls.signalPlaylistReady();
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

  const timeoutPromise = new Promise<boolean>((resolve) => {

    setTimeout(() => resolve(false), timeout);
  });

  const readyPromise = getPromise(stream).then(() => true);

  return Promise.race([ readyPromise, timeoutPromise ]);
}
