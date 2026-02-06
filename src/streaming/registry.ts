/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * registry.ts: Stream tracking for PrismCast.
 */
import type { Nullable, ResolvedSiteProfile } from "../types/index.js";
import { EventEmitter } from "node:events";
import type { FFmpegProcess } from "../utils/index.js";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.js";
import type { Page } from "puppeteer-core";
import type { Readable } from "node:stream";
import type { RecoveryMetrics } from "./monitor.js";

/* The stream registry is the single source of truth for all active streaming sessions. Each stream is tracked in a single StreamRegistryEntry containing browser
 * state, HLS segment storage, and the segmenter reference. This consolidation prevents data desync issues that could occur with separate Maps for each concern. The
 * registry enables the /streams endpoint to list all active streams, the /health endpoint to report stream counts, graceful shutdown to terminate all streams,
 * browser disconnect handling to clean up orphaned streams, and concurrent stream limit enforcement.
 */

// Types.

/**
 * HLS segment and playlist storage for a stream. This includes the fMP4 initialization segment (codec configuration), media segments (.m4s files), and the current
 * playlist content. The playlistReady promise allows callers to wait for the first playlist to be generated.
 *
 * Note: HLSState is co-located with the registry because it is part of StreamRegistryEntry. Moving it to hlsSegments.ts would create a circular dependency since
 * hlsSegments.ts imports getStream from registry.ts.
 */
export interface HLSState {

  // The fMP4 initialization segment containing codec configuration. Sent once at stream start and retained for the stream's lifetime. Clients must fetch this before
  // any media segments.
  initSegment: Nullable<Buffer>;

  // Promise that resolves when the first init segment is stored. Used by MPEG-TS consumers to wait for codec configuration before starting their FFmpeg remuxer.
  initSegmentReady: Promise<void>;

  // The current m3u8 playlist content.
  playlist: string;

  // Promise that resolves when the first playlist is available.
  playlistReady: Promise<void>;

  // EventEmitter for segment notifications. MPEG-TS consumers subscribe to these events to receive segment data in real time. Events:
  //   "initSegment" (data: Buffer) — fired when an init segment is stored
  //   "segment" (filename: string, data: Buffer) — fired when a media segment is stored
  //   "terminated" () — fired when the stream is being terminated
  segmentEmitter: EventEmitter;

  // Map of media segment filenames to their binary data.
  segments: Map<string, Buffer>;

  // Function to signal that the init segment is ready.
  signalInitSegmentReady: () => void;

  // Function to signal that the playlist is ready.
  signalPlaylistReady: () => void;
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

  // Channel name if streaming a named channel, or null for arbitrary URLs.
  channelName: Nullable<string>;

  // IP address of the client that initiated this stream. Used to identify the Channels DVR server for show info lookup.
  clientAddress: Nullable<string>;

  // The FFmpeg process for WebM-to-fMP4 transcoding, or null if using native fMP4 capture.
  ffmpegProcess: Nullable<FFmpegProcess>;

  // HLS segment storage including init segment, media segments, and playlist.
  hls: HLSState;

  // Unique numeric identifier for this stream.
  id: number;

  // Count of active MPEG-TS client connections consuming this stream. Incremented when a client connects, decremented on disconnect. Used by idle timeout logic to
  // keep the stream alive while MPEG-TS clients are connected.
  mpegTsClientCount: number;

  // Stream-specific info for idle detection.
  info: StreamInfo;

  // The browser page for this stream.
  page: Page;

  // The resolved site profile used for this stream. Needed for tab replacement recovery to recreate the capture with the same profile.
  profile: ResolvedSiteProfile;

  // The raw capture stream from puppeteer-stream. In FFmpeg mode, this is the WebM stream piped to FFmpeg's stdin. In native mode, this is the same as the segmenter
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

  let signalInitSegmentReady: () => void = () => {};
  let signalPlaylistReady: () => void = () => {};

  const initSegmentReady = new Promise<void>((resolve) => {

    signalInitSegmentReady = resolve;
  });

  const playlistReady = new Promise<void>((resolve) => {

    signalPlaylistReady = resolve;
  });

  const segmentEmitter = new EventEmitter();

  // Allow up to 20 listeners per event to support multiple concurrent MPEG-TS clients consuming the same stream.
  segmentEmitter.setMaxListeners(20);

  return {

    initSegment: null,
    initSegmentReady,
    playlist: "",
    playlistReady,
    segmentEmitter,
    segments: new Map(),
    signalInitSegmentReady,
    signalPlaylistReady
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
 * Calculates the memory usage for a single stream's HLS segment storage. This measures the Buffer sizes of the init segment and all media segments currently retained
 * in memory.
 * @param entry - The stream registry entry to measure.
 * @returns Memory usage breakdown in bytes.
 */
export function getStreamMemoryUsage(entry: StreamRegistryEntry): StreamMemoryUsage {

  const initSegmentSize = entry.hls.initSegment?.length ?? 0;

  let segmentsSize = 0;

  for(const segment of entry.hls.segments.values()) {

    segmentsSize += segment.length;
  }

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
 * Gets the size in bytes of the last segment stored for a stream. Used by the monitor to detect dead capture pipelines that produce empty segments (18 bytes observed)
 * while the video element appears healthy.
 * @param entry - The stream registry entry to query.
 * @returns Segment size in bytes, or null if no segmenter exists.
 */
export function getLastSegmentSize(entry: StreamRegistryEntry): number | null {

  return entry.segmenter?.getLastSegmentSize() ?? null;
}
