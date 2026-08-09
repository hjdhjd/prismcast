/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsSegments.ts: HLS segment storage functions for PrismCast. The wait-for-readiness helpers (waitForPlaylist, waitForInitSegment) route their timeout race
 * through the Clock port (see utils/clock.ts) so tests can deterministically simulate "promise resolves before timeout" vs "timeout fires before promise"
 * without depending on real timers - the race is exactly the pattern Node's synchronous mock.timers.tick cannot drive reliably.
 */
import type { InitSegmentTrack, StreamRegistryEntry } from "./registry.ts";
import { LOG, realClock } from "../utils/index.ts";
import { cancelPrerollTimer, getStream } from "./registry.ts";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "../utils/index.ts";

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

// Internal Segment Helpers.

/**
 * Stores a segment into the specified Map, emits an event, and enforces the segment count limit by removing the oldest entry. JavaScript Maps maintain insertion
 * order, so the first key is always the oldest segment. This is the shared implementation for both video and audio segment storage.
 * @param stream - The stream registry entry.
 * @param segments - The target segment Map (video or audio).
 * @param eventName - The event name to emit on the segment emitter.
 * @param filename - The segment filename.
 * @param data - The segment binary data.
 */
function storeSegmentToMap(stream: StreamRegistryEntry, segments: Map<string, Buffer>, eventName: "audioSegment" | "segment", filename: string, data: Buffer): void {

  // Determine which running byte counter to update based on the event name.
  const isAudio = eventName === "audioSegment";

  segments.set(filename, data);

  // Increment the running byte counter for O(1) memory reporting.
  if(isAudio) {

    stream.hls.audioSegmentBytes += data.length;
  } else {

    stream.hls.segmentBytes += data.length;
  }

  // Notify consumers that a new segment is available. Emit before rotation so the data is guaranteed accessible in the Map.
  stream.hls.segmentEmitter.emit(eventName, filename, data);

  // Enforce segment limit by removing oldest segments. Decrement the running byte counter for each removed segment.
  while(segments.size > CONFIG.hls.maxSegments) {

    const oldestKey = segments.keys().next().value;

    if(oldestKey === undefined) {

      break;
    }

    const removed = segments.get(oldestKey);

    if(removed) {

      if(isAudio) {

        stream.hls.audioSegmentBytes -= removed.length;
      } else {

        stream.hls.segmentBytes -= removed.length;
      }
    }

    segments.delete(oldestKey);
  }
}

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

  storeSegmentToMap(stream, stream.hls.segments, "segment", filename, data);
}

/**
 * Gets a media segment by filename.
 * @param streamId - The numeric stream ID.
 * @param filename - The segment filename.
 * @returns The segment data, or undefined if not found.
 */
export function getSegment(streamId: number, filename: string): Buffer | undefined {

  return getStream(streamId)?.hls.segments.get(filename);
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
 * Stores an audio segment for streams with separate audio renditions. Enforces the same segment count limit as video segments.
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

  storeSegmentToMap(stream, stream.hls.audioSegments, "audioSegment", filename, data);
}

/**
 * Gets an audio segment by filename.
 * @param streamId - The numeric stream ID.
 * @param filename - The audio segment filename.
 * @returns The segment data, or undefined if not found.
 */
export function getAudioSegment(streamId: number, filename: string): Buffer | undefined {

  return getStream(streamId)?.hls.audioSegments.get(filename);
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

    signalFirstInit(stream);
  }
}

/**
 * Resolves a stream's init-segment readiness promise and records the startup timing. Both init stores share this so the readiness contract - and the timing log
 * that measures it - has one implementation regardless of which storage shape produced the first init.
 *
 * @param stream - The stream registry entry whose readiness to signal.
 */
function signalFirstInit(stream: StreamRegistryEntry): void {

  stream.hls.signalInitSegmentReady();

  const elapsed = ((Date.now() - stream.startTime.getTime()) / 1000).toFixed(3);

  LOG.debug("timing:startup", "Init segment ready in %ss.", elapsed);
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

// Named Init Segment Management.

/* The functions below own the native relay's per-track initialization segments - the ones fetched from upstream #EXT-X-MAP references and served under names the
 * relay mints. They are deliberately separate from the single-slot storeInitSegment above: capture owns its encoder and overwrites one init in place, while a
 * relayed fMP4 source can change its init mid-stream and must keep the outgoing one fetchable until the last segment referencing it leaves the window.
 *
 * The "initSegment" emitter event is deliberately not emitted here. Its only subscribers are the capture path's MPEG-TS consumers, which read the single slot;
 * a native fMP4 client resolves its init through resolveMpegTsInitSource instead, so emitting would notify no one.
 */

/**
 * Stores a named initialization segment for one track and updates that track's current served name. Readiness is signaled from the VIDEO track's first init
 * only: the consumer that waits on it is the MPEG-TS remux path, which resolves the video init, so signaling on an audio-first arrival would release a client
 * before the buffer it needs exists.
 *
 * The byte counter moves by the delta against whatever was already stored under this filename, so re-storing a name whose bytes the relay is reusing is
 * byte-neutral and safe to call more than once. That differs from storeSegmentToMap's always-add shape, which is correct there because its callers only ever
 * mint fresh names.
 *
 * @param streamId - The numeric stream ID.
 * @param track - The track this init belongs to.
 * @param filename - The name the init is served under.
 * @param data - The init segment binary data.
 */
export function storeNamedInitSegment(streamId: number, track: InitSegmentTrack, filename: string, data: Buffer): void {

  const stream = getStream(streamId);

  if(!stream) {

    LOG.debug("streaming:hls", "Attempted to store a named init segment for unknown stream %s.", streamId);

    return;
  }

  const initSegments = stream.hls.initSegments[track];
  const existing = initSegments.get(filename);
  const isFirstVideoInit = (track === "video") && (stream.hls.initSegments.video.size === 0);

  initSegments.set(filename, data);
  stream.hls.initSegmentBytes += data.length - (existing?.length ?? 0);
  stream.hls.currentInitNames[track] = filename;

  if(isFirstVideoInit) {

    signalFirstInit(stream);
  }
}

/**
 * Gets a named initialization segment by the name it is served under, searching both tracks. The names the relay mints are distinct per track, so a single
 * lookup key is unambiguous and the HTTP route needs no track parameter.
 *
 * @param streamId - The numeric stream ID.
 * @param filename - The served init segment name.
 * @returns The init segment data, or undefined when the stream or the name is unknown.
 */
export function getNamedInitSegment(streamId: number, filename: string): Buffer | undefined {

  const stream = getStream(streamId);

  if(!stream) {

    return undefined;
  }

  return stream.hls.initSegments.video.get(filename) ?? stream.hls.initSegments.audio.get(filename);
}

/**
 * Finds a track's already-stored init segment whose bytes match the supplied data. This is what makes init identity content-based rather than URL-based: a
 * service that rotates a token through its #EXT-X-MAP URI re-serves the same bytes, and an ad break that alternates between two initializations returns to one
 * already stored. Comparing against the track's whole stored set - not just its current init - is what lets that alternation reuse names instead of minting a
 * duplicate for bytes already being served. The search lives with the storage it scans so callers never reach into the Maps.
 *
 * @param streamId - The numeric stream ID.
 * @param track - The track whose stored inits to search.
 * @param data - The candidate init segment bytes.
 * @returns The served name of the matching init, or undefined when no stored init has these bytes.
 */
export function findNamedInitSegment(streamId: number, track: InitSegmentTrack, data: Buffer): string | undefined {

  const stream = getStream(streamId);

  if(!stream) {

    return undefined;
  }

  for(const [ filename, stored ] of stream.hls.initSegments[track]) {

    if(stored.equals(data)) {

      return filename;
    }
  }

  return undefined;
}

/**
 * Evicts a track's init segments that the retain set no longer names, decrementing the byte counter by each eviction so the counter tracks the live set. The
 * caller derives the retain set from the segments actually in that track's playlist window plus the track's current init, so an init survives exactly as long as
 * something can still reference it. Operating on one track's Map makes the isolation structural - a video prune cannot evict an audio init.
 *
 * @param streamId - The numeric stream ID.
 * @param track - The track to prune.
 * @param retain - The served init names that must survive.
 */
export function pruneNamedInitSegments(streamId: number, track: InitSegmentTrack, retain: Set<string>): void {

  const stream = getStream(streamId);

  if(!stream) {

    return;
  }

  const initSegments = stream.hls.initSegments[track];

  for(const [ filename, stored ] of initSegments) {

    if(!retain.has(filename)) {

      stream.hls.initSegmentBytes -= stored.length;
      initSegments.delete(filename);
    }
  }
}

/**
 * Releases every piece of native init state for a stream. This is the single cleanup owner the recovery path calls when a stream leaves native mode: the init
 * Maps outlive the proxy that filled them, and getStreamMemoryUsage reads the byte counter with no mode gate, so state left behind here would misreport memory
 * for the rest of the stream's life.
 *
 * @param streamId - The numeric stream ID.
 */
export function clearNativeInitState(streamId: number): void {

  const stream = getStream(streamId);

  if(!stream) {

    return;
  }

  stream.hls.initSegments.audio.clear();
  stream.hls.initSegments.video.clear();
  stream.hls.initSegmentBytes = 0;
  stream.hls.currentInitNames.audio = null;
  stream.hls.currentInitNames.video = null;
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
    // flowing, uselessly seeding preroll state. For streams where the timer already fired (preroll is active), this is a no-op - the timer handle is already null.
    cancelPrerollTimer(stream.hls);

    const elapsed = ((Date.now() - stream.startTime.getTime()) / 1000).toFixed(3);

    LOG.debug("streaming:preroll", "Live playlist ready for stream %d.", streamId);
    LOG.debug("timing:startup", "First playlist ready in %ss.", elapsed);
  }
}

/**
 * Gets the current playlist for a stream. Unlike getAudioPlaylist and getVideoPlaylist, this returns the raw stored string as-is, including an empty string, so
 * callers can distinguish a stream that exists but has no playlist yet ("") from a stream that was not found (undefined).
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
 * @param clock - Clock used for the timeout race. Defaults to realClock; tests inject a fake to drive the race deterministically.
 * @returns True if playlist is ready, false if timeout or stream not found.
 */
export async function waitForPlaylist(streamId: number, timeout: number, clock: Clock = realClock): Promise<boolean> {

  return waitForReady(streamId, async (stream) => stream.hls.playlistReady, timeout, clock);
}

/**
 * Waits for the first init segment to be available for a stream. Used by MPEG-TS consumers to wait for codec configuration before starting their FFmpeg remuxer.
 * @param streamId - The numeric stream ID.
 * @param timeout - Maximum time to wait in milliseconds.
 * @param clock - Clock used for the timeout race. Defaults to realClock; tests inject a fake to drive the race deterministically.
 * @returns True if init segment is ready, false if timeout or stream not found.
 */
export async function waitForInitSegment(streamId: number, timeout: number, clock: Clock = realClock): Promise<boolean> {

  return waitForReady(streamId, async (stream) => stream.hls.initSegmentReady, timeout, clock);
}

// Internal Helpers.

/**
 * Races a readiness promise from a stream's HLS state against a timeout. Returns true if the promise resolves before the timeout, false if the timeout fires first or
 * the stream doesn't exist.
 * @param streamId - The numeric stream ID.
 * @param getPromise - Accessor that extracts the readiness promise from the stream's HLS state.
 * @param timeout - Maximum time to wait in milliseconds.
 * @param clock - Clock providing the timeout race; the public wrappers thread their own clock through.
 * @returns True if ready before timeout, false otherwise.
 */
async function waitForReady(streamId: number, getPromise: (stream: StreamRegistryEntry) => Promise<void>, timeout: number, clock: Clock): Promise<boolean> {

  const stream = getStream(streamId);

  if(!stream) {

    return false;
  }

  // Third race arm: a one-shot "terminated" listener resolving false. Without it, a terminateStream landing mid-wait would leave the caller hanging the full timeout,
  // because termination never resolves the readiness promise (its registry contract stays pure - only real content resolves it). The listener is attached
  // synchronously here, right after the getStream read, so no emit can be missed: terminateStream runs synchronously from its "terminated" emit through unregisterStream
  // with no suspension point in between. It is removed in the finally, whichever arm settles first.
  const emitter = stream.hls.segmentEmitter;
  const { promise: terminated, resolve: resolveTerminated } = Promise.withResolvers<boolean>();
  const onTerminated = (): void => resolveTerminated(false);

  emitter.once("terminated", onTerminated);

  try {

    return await clock.waitWithTimeout(Promise.race([ getPromise(stream).then(() => true), terminated ]), timeout).catch(() => false);
  } finally {

    emitter.off("terminated", onTerminated);
  }
}
