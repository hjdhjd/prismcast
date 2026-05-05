/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * registry.ts: Stream tracking for PrismCast.
 */
import type { Nullable, ResolvedSiteProfile, StreamingMode } from "../types/index.ts";
import type { CaptureCodec } from "./codec.ts";
import { EventEmitter } from "node:events";
import type { FFmpegProcess } from "../utils/index.ts";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.ts";
import type { NativeProxy } from "../native/proxy.ts";
import type { Page } from "puppeteer-core";
import type { Readable } from "node:stream";
import type { RecoveryMetrics } from "./recovery.ts";

/* The stream registry is the single source of truth for all active streaming sessions. Each stream is tracked in a single StreamRegistryEntry containing browser
 * state, HLS segment storage, and the segmenter reference. This consolidation prevents data desync issues that could occur with separate Maps for each concern. The
 * registry enables the /streams endpoint to list all active streams, the /health endpoint to report stream counts, graceful shutdown to terminate all streams,
 * browser disconnect handling to clean up orphaned streams, and concurrent stream limit enforcement.
 */

// Types.

/**
 * Typed event map for segment notifications. MPEG-TS consumers and lifecycle handlers subscribe to these events. Using a typed interface ensures event names and
 * argument types are checked at compile time, preventing misspelled event names or incorrect payloads.
 */
interface SegmentEmitterEventMap {

  audioSegment: [filename: string, data: Buffer];
  initSegment: [data: Buffer];
  segment: [filename: string, data: Buffer];
  terminated: [];
}

/**
 * Typed EventEmitter for segment notifications. Narrows Node's untyped EventEmitter to only accept the events defined in SegmentEmitterEventMap.
 */
export interface SegmentEmitter extends EventEmitter {

  emit<K extends keyof SegmentEmitterEventMap>(event: K, ...args: SegmentEmitterEventMap[K]): boolean;
  off<K extends keyof SegmentEmitterEventMap>(event: K, listener: (...args: SegmentEmitterEventMap[K]) => void): this;
  on<K extends keyof SegmentEmitterEventMap>(event: K, listener: (...args: SegmentEmitterEventMap[K]) => void): this;
}

/**
 * HLS segment and playlist storage for a stream. This includes the fMP4 initialization segment (codec configuration), media segments (.m4s files), and the current
 * playlist content. The playlistReady promise allows callers to wait for the first playlist to be generated.
 *
 * Note: HLSState is co-located with the registry because it is part of StreamRegistryEntry. Moving it to hlsSegments.ts would create a circular dependency since
 * hlsSegments.ts imports getStream from registry.ts.
 */
export interface HLSState {

  // Playlist content.

  // The current m3u8 playlist content. For streams without separate audio, this is the variant playlist. For streams with separate audio, this is the master
  // playlist referencing video.m3u8 and audio.m3u8.
  playlist: string;

  // The video variant playlist content for streams with separate audio renditions. Empty string when not applicable.
  videoPlaylist: string;

  // The audio variant playlist content for streams with separate audio renditions. Empty string when not applicable.
  audioPlaylist: string;

  // Whether this stream has separate audio renditions. When true, stream.m3u8 serves a master playlist referencing video.m3u8 and audio.m3u8. When false (the
  // common case), stream.m3u8 serves the variant playlist directly.
  hasAudio: boolean;

  // Whether the segmenter has produced a real playlist (as opposed to a preroll-seeded playlist). Used by updatePlaylist to detect the first real playlist for timing
  // logs. Without this flag, the empty-string check would fail when preroll content pre-seeds the playlist field.
  hasRealPlaylist: boolean;

  // Segment storage.

  // The fMP4 initialization segment containing codec configuration. Sent once at stream start and retained for the stream's lifetime. Clients must fetch this before
  // any media segments.
  initSegment: Nullable<Buffer>;

  // Map of media segment filenames to their binary data.
  segments: Map<string, Buffer>;

  // Running total of all video segment buffer sizes in bytes. Updated by storeSegmentToMap on add and rotate. Eliminates O(n) iteration in getStreamMemoryUsage().
  segmentBytes: number;

  // Map of audio segment filenames to their binary data. Used only for streams with separate audio renditions (e.g., Google DAI on BET/VH1).
  audioSegments: Map<string, Buffer>;

  // Running total of all audio segment buffer sizes in bytes. Updated by storeSegmentToMap on add and rotate. Must be reset to zero when audioSegments is cleared
  // directly (e.g., native-to-capture fallback in monitor.ts).
  audioSegmentBytes: number;

  // Typed emitter for segment notifications. MPEG-TS consumers subscribe to these events to receive segment data in real time.
  segmentEmitter: SegmentEmitter;

  // Readiness signals.

  // Promise that resolves when the first init segment is stored. Used by MPEG-TS consumers to wait for codec configuration before starting their FFmpeg remuxer.
  initSegmentReady: Promise<void>;

  // Function to signal that the init segment is ready.
  signalInitSegmentReady: () => void;

  // Promise that resolves when the first playlist is available.
  playlistReady: Promise<void>;

  // Function to signal that the playlist is ready.
  signalPlaylistReady: () => void;

  // Preroll management.

  // The base URL for constructing absolute preroll segment URIs in the composite playlist (e.g., "http://192.168.1.100:5589"). Null when no preroll is active.
  prerollBaseUrl: Nullable<string>;

  // The preroll codec variant for this stream. Determines which preroll variant is served and referenced in playlists. Null when no preroll is active.
  prerollCodec: Nullable<CaptureCodec>;

  // Number of preroll segments that precede real content. Zero when no preroll is active. The segmenter uses this to know which indices in the playlist sliding
  // window are preroll entries vs real entries.
  prerollSegmentCount: number;

  // Wall-clock time when the preroll timer fired and the progressive preroll playlist began. Used to compute elapsed time on each playlist poll so the progressive
  // window advances in real time, simulating a live stream. Null before the preroll timer fires.
  prerollStartTime: Nullable<Date>;

  // Timer handle for deferred preroll seeding. The timer fires after PREROLL_DELAY_MS; if real content hasn't arrived yet, preroll is seeded and playlistReady is
  // signaled. Deliberately NOT cancelled in completeStreamSetup() - the timer must survive setup completion for native streams where the proxy's first poll cycle
  // takes 10-15+ seconds. Cancelled in two places: updatePlaylist() in hlsSegments.ts (when the first real playlist arrives) and terminateStream() in lifecycle.ts
  // (on stream teardown).
  prerollTimer: Nullable<ReturnType<typeof setTimeout>>;

  // Resume continuity.

  // Snapshotted resume segment index from the prior session. Read once at stream registration and stored here so both the preroll timer callback and the segmenter
  // creation in completeStreamSetup() use the same value - eliminating the TTL race that would occur if each read the resume map independently.
  resumeSegmentIndex: number;
}

/**
 * Stream-specific information for idle detection.
 */
export interface StreamInfo {

  // Timestamp of the last playlist request, used for idle timeout detection.
  lastPlaylistRequest: number;

  // Key used to look up this stream in the channel-to-stream mapping.
  storeKey: string;
}

/**
 * Registry entry for an active stream. This is the single source of truth for all stream data, including browser state, HLS segments, and the segmenter reference.
 */
export interface StreamRegistryEntry {

  // Video codec label for this stream (e.g., "H264", "HEVC"). For capture mode, determined by GPU capabilities. For native mode, extracted from the service's
  // HLS manifest. Null before stream setup completes.
  captureCodec: Nullable<string>;

  // Channel name if streaming a named channel, or null for arbitrary URLs.
  channelName: Nullable<string>;

  // IP address of the client that initiated this stream. Used to identify the Channels DVR server for show info lookup.
  clientAddress: Nullable<string>;

  // The FFmpeg process for Matroska-to-fMP4 transcoding, or null if using native fMP4 capture.
  ffmpegProcess: Nullable<FFmpegProcess>;

  // Whether this stream is using hardware-accelerated video encoding on the local GPU. True when Chrome's MediaRecorder is using hardware encoding in capture mode.
  // False for native HLS (pass-through, no local encoding) and software-only capture.
  hardwareAccelerated: boolean;

  // HLS segment storage including init segment, media segments, and playlist.
  hls: HLSState;

  // Unique numeric identifier for this stream.
  id: number;

  // Count of active MPEG-TS client connections consuming this stream. Incremented when a client connects, decremented on disconnect. Used by idle timeout logic to
  // keep the stream alive while MPEG-TS clients are connected.
  mpegTsClientCount: number;

  // Declared bandwidth from the service's HLS manifest in bits per second. Zero for capture-mode streams and when the BANDWIDTH attribute is absent.
  nativeBandwidth: number;

  // The native HLS proxy for streams that bypass screen capture. Null for capture-mode streams.
  nativeProxy: Nullable<NativeProxy>;

  // Video resolution from the service's HLS manifest (e.g., "1920x1080"). Null for capture-mode streams and when absent from the manifest.
  nativeResolution: Nullable<string>;

  // Stream-specific info for idle detection.
  info: StreamInfo;

  // The browser page for this stream. Null for pending stream entries that have been registered but whose async setup has not yet completed.
  page: Nullable<Page>;

  // Whether this stream was started by the pretune module ahead of a scheduled recording. Pretuned streams are exempt from idle timeout until a real client
  // connects, at which point this flag is cleared and the stream follows normal idle timeout behavior.
  preTuned: boolean;

  // The resolved site profile used for this stream. Needed for tab replacement recovery to recreate the capture with the same profile. Null for pending stream entries
  // that have been registered but whose async setup has not yet completed.
  profile: Nullable<ResolvedSiteProfile>;

  // The raw capture stream from puppeteer-stream. In FFmpeg mode, this is the Matroska stream piped to FFmpeg's stdin. In native mode, this is the same as the segmenter
  // input. Must be destroyed before closing the page to ensure chrome.tabCapture releases the capture and prevents "Cannot capture a tab with an active stream" errors
  // on subsequent stream requests.
  rawCaptureStream: Nullable<Readable>;

  // The fMP4 segmenter that processes the capture stream, or null if not yet created.
  segmenter: Nullable<FMP4SegmenterResult>;

  // Timestamp when the stream started.
  startTime: Date;

  // Function to stop the health monitor, or null if monitoring hasn't started. Returns recovery metrics for the termination summary.
  stopMonitor: Nullable<() => RecoveryMetrics>;

  // String identifier for logging (e.g., "cnn-5jecl6").
  streamIdStr: string;

  // Streaming mode: "capture" for screen capture via puppeteer-stream, "native" for direct HLS consumption.
  streamingMode: StreamingMode;

  // URL being streamed.
  url: string;
}

// State.

// The unified stream registry. Maps numeric stream IDs to stream entries.
const streamRegistry = new Map<number, StreamRegistryEntry>();

// Counter for generating unique stream IDs. Incremented for each new stream.
let streamIdCounter = 0;

// Public API.

/**
 * Gets the next unique stream ID by incrementing the counter. Each call returns a new, higher ID that has never been used before in this process lifetime.
 * @returns The next unique stream ID.
 */
export function getNextStreamId(): number {

  return ++streamIdCounter;
}

/**
 * Registers a stream in the registry. This should be called after stream setup is complete and the stream is ready to serve data.
 * @param entry - The stream registry entry to add.
 */
export function registerStream(entry: StreamRegistryEntry): void {

  streamRegistry.set(entry.id, entry);
}

/**
 * Unregisters a stream from the registry. This should be called during stream cleanup to remove the stream from tracking.
 * @param id - The numeric stream ID to remove.
 */
export function unregisterStream(id: number): void {

  streamRegistry.delete(id);
}

/**
 * Gets a stream entry by its ID.
 * @param id - The numeric stream ID to look up.
 * @returns The stream entry if found, undefined otherwise.
 */
export function getStream(id: number): StreamRegistryEntry | undefined {

  return streamRegistry.get(id);
}

/**
 * Gets all stream entries in the registry.
 * @returns Array of all stream registry entries.
 */
export function getAllStreams(): StreamRegistryEntry[] {

  return Array.from(streamRegistry.values());
}

/**
 * Gets the total number of streams in the registry.
 * @returns The number of active streams.
 */
export function getStreamCount(): number {

  return streamRegistry.size;
}

/**
 * Updates the last playlist request timestamp for a stream. This should be called whenever a playlist or segment is requested to keep the idle timeout accurate.
 * @param id - The numeric stream ID.
 */
export function updateLastAccess(id: number): void {

  const entry = streamRegistry.get(id);

  if(entry) {

    entry.info.lastPlaylistRequest = Date.now();
  }
}

/**
 * Creates the initial HLS state for a new stream. This sets up empty segment storage and the playlist readiness signaling mechanism.
 * @returns A new HLSState object ready to receive segments.
 */
export function createHLSState(): HLSState {

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise: initSegmentReady, resolve: signalInitSegmentReady } = Promise.withResolvers<void>();

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise: playlistReady, resolve: signalPlaylistReady } = Promise.withResolvers<void>();

  const segmentEmitter = new EventEmitter() as SegmentEmitter;

  // Allow up to 20 listeners per event to support multiple concurrent MPEG-TS clients consuming the same stream.
  segmentEmitter.setMaxListeners(20);

  return {

    audioPlaylist: "",
    audioSegmentBytes: 0,
    audioSegments: new Map(),
    hasAudio: false,
    hasRealPlaylist: false,
    initSegment: null,
    initSegmentReady,
    playlist: "",
    playlistReady,
    prerollBaseUrl: null,
    prerollCodec: null,
    prerollSegmentCount: 0,
    prerollStartTime: null,
    prerollTimer: null,
    resumeSegmentIndex: 0,
    segmentBytes: 0,
    segmentEmitter,
    segments: new Map(),
    signalInitSegmentReady,
    signalPlaylistReady,
    videoPlaylist: ""
  };
}

// Memory Usage.

/**
 * Memory usage breakdown for a stream's HLS segment storage.
 */
export interface StreamMemoryUsage {

  // Size of the fMP4 initialization segment in bytes.
  initSegment: number;

  // Total size of all media segments in bytes.
  segments: number;

  // Total memory usage (initSegment + segments) in bytes.
  total: number;
}

/**
 * Returns the memory usage for a single stream's HLS segment storage. Uses the running byte counters maintained by storeSegmentToMap() for O(1) reads instead of
 * iterating all segment buffers. The init segment is a single buffer read (also O(1)).
 * @param entry - The stream registry entry to measure.
 * @returns Memory usage breakdown in bytes.
 */
export function getStreamMemoryUsage(entry: StreamRegistryEntry): StreamMemoryUsage {

  const initSegmentSize = entry.hls.initSegment?.length ?? 0;
  const segmentsSize = entry.hls.segmentBytes + entry.hls.audioSegmentBytes;

  return {

    initSegment: initSegmentSize,
    segments: segmentsSize,
    total: initSegmentSize + segmentsSize
  };
}

/**
 * Calculates the total segment memory usage across all active streams. This is useful for monitoring overall memory consumption by HLS buffers.
 * @returns Total memory usage in bytes across all streams.
 */
export function getTotalSegmentMemory(): number {

  let total = 0;

  for(const entry of streamRegistry.values()) {

    total += getStreamMemoryUsage(entry).total;
  }

  return total;
}

// Segment Health.

/**
 * Returns whether the last segment contained video traf boxes. Used by the monitor to distinguish dead capture pipelines (audio-only, no video trafs) from legitimate
 * small segments produced by static content (video trafs present but low-bitrate). Returns null if the segmenter does not exist or the video trackId is unknown.
 * @param entry - The stream registry entry to query.
 * @returns True if video trafs were present, false if absent, null if unknown.
 */
export function getLastSegmentHasVideo(entry: StreamRegistryEntry): Nullable<boolean> {

  return entry.segmenter?.getLastSegmentHasVideo() ?? null;
}

/**
 * Gets the size in bytes of the last segment stored for a stream. Used by the monitor to detect dead capture pipelines that produce empty segments (18 bytes observed)
 * while the video element appears healthy.
 * @param entry - The stream registry entry to query.
 * @returns Segment size in bytes, or null if no segmenter exists.
 */
export function getLastSegmentSize(entry: StreamRegistryEntry): Nullable<number> {

  return entry.segmenter?.getLastSegmentSize() ?? null;
}
