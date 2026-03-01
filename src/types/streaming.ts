/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streaming.ts: Stream session and playback state type definitions for PrismCast.
 */
import type { Nullable } from "./shared.js";
import type { Page } from "puppeteer-core";
import type { RecoveryMetrics } from "../streaming/recovery.js";

/* These types track active streaming sessions throughout their lifecycle. When a stream request arrives, we create a StreamInfo object to track the session's
 * state. This allows the /streams endpoint to list active streams, the graceful shutdown handler to close streams cleanly, and the stream handler to coordinate
 * cleanup when streams end.
 */

/**
 * Information about an active streaming session. Created when a stream request is received and deleted when the stream ends.
 */
export interface StreamInfo {

  // Channel name if streaming a named channel from the CHANNELS configuration, or null if streaming an arbitrary URL via the url query parameter.
  channelName: Nullable<string>;

  // Unique numeric identifier for this stream session. Used by the /streams/:id endpoint for stream management and in log messages for correlation.
  id: number;

  // Puppeteer Page object for the browser tab running this stream. Used for cleanup when the stream ends and for the graceful shutdown handler to close all
  // streams.
  page: Page;

  // Timestamp when the stream was initiated. Used to calculate stream duration for logging and the /streams endpoint.
  startTime: Date;

  // Function to stop the playback health monitor for this stream, or null if monitoring hasn't started yet. Called during cleanup to stop the monitoring
  // interval and prevent the monitor from trying to recover a stream that's being terminated. Returns recovery metrics for the termination summary.
  stopMonitor: Nullable<() => RecoveryMetrics>;

  // URL being streamed. Logged for debugging and displayed in the /streams endpoint.
  url: string;
}

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
 * Alias for UrlValidationResult maintained for backward compatibility with existing code.
 */
export type UrlValidation = UrlValidationResult;

/* The /health endpoint returns detailed status information for monitoring and debugging. This includes browser connection state, memory usage, stream counts, and
 * configuration summary. External monitoring systems can poll this endpoint to detect problems.
 */

/**
 * Health check response structure returned by the /health endpoint.
 */
export interface HealthStatus {

  // Browser connection information.
  browser: {

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
