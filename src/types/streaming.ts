/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streaming.ts: Stream session and playback state type definitions for PrismCast.
 */
import type { Nullable } from "./shared.ts";

/**
 * All recognized capture codec identifiers. H.264 is the universal baseline; additional codecs require GPU hardware encoding. This array is the single definition
 * from which the CaptureCodec type, MIME type lookup, and config validation all derive.
 */
export const RECOGNIZED_CODECS = [ "h264", "hevc" ] as const;

/**
 * Video codec identifier for browser capture, derived from RECOGNIZED_CODECS.
 */
export type CaptureCodec = typeof RECOGNIZED_CODECS[number];

/**
 * Chrome's rejection text when it cannot open a capture source for the tab. Two layers speak this string and neither may spell it independently: the capture
 * module tests a refusal against it to decide whether the start is worth one more attempt, and the recovery module carries it in the signature list that
 * classifies a failure as capture infrastructure. It lives here, in a module with no runtime dependencies at all, so the classifier can read the protocol's own
 * wording without taking on the browser stack that speaks the protocol.
 */
export const CAPTURE_SOURCE_UNAVAILABLE_MESSAGE = "Could not start video source";

/* These types represent the state of HTML5 video elements as reported by the browser. The playback health monitor periodically evaluates video state to detect
 * problems and trigger recovery. Understanding these values is essential for diagnosing playback issues.
 */

/**
 * Snapshot of a video element's playback state. Collected by the playback health monitor to detect stalls, errors, and other problems.
 */
export interface VideoState {

  // Current playback position in seconds. Compared between monitor checks to detect stalls. If this value doesn't change between checks (accounting for the
  // stallThreshold), the video is considered stalled.
  currentTime: number;

  // Whether the video has reached its end. For live streams, this typically indicates an error condition since live streams don't have a natural end.
  ended: boolean;

  // Whether the video element has an error (video.error !== null). This indicates a media error like a decode failure or network error that prevents playback.
  error: boolean;

  // Whether the video is muted. Some sites auto-mute videos; the health monitor enforces unmuted state on each check.
  muted: boolean;

  // The video's networkState property indicating network activity: 0 (EMPTY), 1 (IDLE), 2 (LOADING), 3 (NO_SOURCE). Value 2 (LOADING) combined with low
  // readyState indicates active buffering.
  networkState: number;

  // Whether the video is paused. Paused videos don't progress and may indicate that autoplay was blocked or the user paused playback.
  paused: boolean;

  // The video's readyState property indicating how much data is buffered: 0 (HAVE_NOTHING), 1 (HAVE_METADATA), 2 (HAVE_CURRENT_DATA), 3 (HAVE_FUTURE_DATA), 4
  // (HAVE_ENOUGH_DATA). We consider readyState >= 3 as "ready" because live streams may never reach 4 due to continuous data arrival.
  readyState: number;

  // Alias for currentTime. Some code uses "time" for brevity.
  time: number;

  // Intrinsic height of the video source in pixels. Zero when no video is loaded.
  videoHeight: number;

  // Intrinsic width of the video source in pixels. Zero when no video is loaded.
  videoWidth: number;

  // Current volume level from 0.0 (silent) to 1.0 (full volume). The health monitor enforces volume = 1.0 on each check to counter sites that lower volume.
  volume: number;
}

/**
 * Strategy for selecting a video element when multiple are present. "selectFirstVideo" takes the first video in DOM order; "selectReadyVideo" finds the video
 * with readyState >= 3, which typically identifies the actively playing main content rather than preloaded ads.
 */
export type VideoSelectorType = "selectFirstVideo" | "selectReadyVideo";

/* Before navigating to user-provided URLs, we validate them to prevent security issues (like file:// access) and provide clear error messages for malformed URLs.
 * Validation runs before any browser interaction to fail fast with helpful feedback.
 */

/**
 * Result of URL validation indicating whether the URL is safe to navigate to.
 */
export interface UrlValidationResult {

  // Human-readable explanation of why validation failed, present only when valid is false.
  reason?: string;

  // Whether the URL passed validation and is safe to navigate to.
  valid: boolean;
}

/**
 * Streaming mode for an active stream. "capture" uses Chrome screen capture via puppeteer-stream. "native" intercepts the service's HLS stream and consumes it
 * directly in Node, bypassing screen capture entirely.
 */
export type StreamingMode = "capture" | "native";

/**
 * Media container format of an upstream HLS source. "fmp4" sources (CMAF) carry their codec configuration in a separate initialization segment referenced by
 * #EXT-X-MAP, so the relay must fetch, store, and re-reference that init for the fragments to be playable. "ts" sources are self-describing - every MPEG-TS
 * segment carries its own PAT/PMT - so they need no init at all. Consumers select the fMP4 behavior on an exact "fmp4" match; every other value takes the
 * self-describing path.
 */
export type MediaContainer = "fmp4" | "ts";

/* The /health endpoint returns detailed status information for monitoring and debugging. This includes browser connection state, memory usage, stream counts, and
 * configuration summary. External monitoring systems can poll this endpoint to detect problems.
 */

/**
 * Health check response structure returned by the /health endpoint.
 */
export interface HealthStatus {

  // Browser connection information.
  browser: {

    // Whether the browser is waiting to relaunch because it can no longer start captures. Its running captures continue and new stream requests are refused with a
    // 503 back-off until nothing depends on it and the relaunch runs.
    captureImpaired: boolean;

    // Whether the Puppeteer browser instance is currently connected. False indicates the browser crashed or was closed.
    connected: boolean;

    // Number of open browser pages/tabs. Includes both stream pages and any stale pages pending cleanup.
    pageCount: number;
  };

  // Media capture mode currently configured ("ffmpeg" or "native").
  captureMode: string;

  // Chrome browser version string (e.g., "Chrome/144.0.7559.110"), or null if the browser is not connected.
  chrome: Nullable<string>;

  // Aggregate client information across all active streams.
  clients: {

    // Per-type breakdown sorted alphabetically by type name.
    byType: { count: number; type: string }[];

    // Total number of clients across all streams.
    total: number;
  };

  // Whether FFmpeg is available on the system. Only relevant when captureMode is "ffmpeg".
  ffmpegAvailable: boolean;

  // Node.js memory usage statistics in bytes.
  memory: {

    // Total heap memory allocated by V8.
    heapTotal: number;

    // Heap memory currently in use by V8.
    heapUsed: number;

    // Resident set size - total memory allocated for the process.
    rss: number;

    // Total memory used by HLS segment buffers across all active streams.
    segmentBuffers: number;
  };

  // Human-readable status message, present when status is not "healthy".
  message?: string;

  // Overall health status: "healthy" when everything is working, "degraded" when approaching capacity, "unhealthy" when browser is disconnected.
  status: "degraded" | "healthy" | "unhealthy";

  // Active stream information.
  streams: {

    // Number of currently active streams.
    active: number;

    // Maximum concurrent streams allowed.
    limit: number;
  };

  // ISO 8601 timestamp when the health check was performed.
  timestamp: string;

  // Server uptime in seconds since the process started.
  uptime: number;

  // PrismCast server version from package.json.
  version: string;
}

/* The /streams endpoint returns information about all active streams, allowing operators to monitor what's currently streaming and terminate specific streams if
 * needed.
 */

/**
 * Information about a single active stream as returned by the /streams endpoint.
 */
export interface StreamListItem {

  // Channel name if streaming a named channel, or null for arbitrary URLs.
  channel: Nullable<string>;

  // Stream duration in seconds since it started.
  duration: number;

  // Unique numeric identifier for the stream, usable with DELETE /streams/:id.
  id: number;

  // ISO 8601 timestamp when the stream started.
  startTime: string;

  // URL being streamed.
  url: string;
}

/**
 * Response structure for the /streams endpoint.
 */
export interface StreamListResponse {

  // Number of currently active streams.
  count: number;

  // Maximum concurrent streams allowed.
  limit: number;

  // Array of active stream information.
  streams: StreamListItem[];
}
