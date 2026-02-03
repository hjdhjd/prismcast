/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lifecycle.ts: Stream lifecycle management for PrismCast.
 */
import { LOG, formatDuration, formatError, getAbortController, unregisterAbortController } from "../utils/index.js";
import { formatRecoveryMetricsSummary, getTotalRecoveryAttempts } from "./monitor.js";
import { getStream, unregisterStream } from "./registry.js";
import type { KeyframeStats } from "./fmp4Segmenter.js";
import type { Nullable } from "../types/index.js";
import type { Readable } from "node:stream";
import type { RecoveryMetrics } from "./monitor.js";
import { clearClients } from "./clients.js";
import { clearShowName } from "./showInfo.js";
import { emitStreamRemoved } from "./statusEmitter.js";
import { formatKeyframeStatsSummary } from "./fmp4Segmenter.js";
import { isGracefulShutdown } from "../browser/index.js";

/*
 * STREAM LIFECYCLE
 *
 * This module provides the authoritative stream termination logic. All code paths that need to terminate a stream should call terminateStream() from this module. This
 * ensures consistent cleanup behavior including:
 *
 * - Stopping the segmenter
 * - Removing channel-to-stream mapping
 * - Stopping the health monitor
 * - Closing the browser page
 * - Unregistering from the stream registry
 * - Clearing client tracking data
 * - Emitting SSE events
 *
 * Callers are responsible for calling emitCurrentSystemStatus() after termination if they need to update the SSE system status. This is not done automatically to
 * avoid circular dependencies with the browser module.
 */

// State.

/**
 * Map of channel names to their active HLS stream IDs. Used to share streams between multiple clients requesting the same channel. This is kept separate from the
 * stream registry because it's a lookup index for deduplication, not stream state.
 */
const channelToStreamId = new Map<string, number>();

/**
 * Set of stream IDs for which termination has been initiated. Used to suppress warnings from segmenter callbacks during cleanup.
 */
const terminationInitiated = new Set<number>();

// Channel Mapping Functions.

/**
 * Gets the stream ID for a channel, if one exists.
 * @param channelName - The channel name to look up.
 * @returns The stream ID if found, undefined otherwise.
 */
export function getChannelStreamId(channelName: string): number | undefined {

  return channelToStreamId.get(channelName);
}

/**
 * Associates a channel name with a stream ID.
 * @param channelName - The channel name.
 * @param streamId - The stream ID to associate.
 */
export function setChannelStreamId(channelName: string, streamId: number): void {

  channelToStreamId.set(channelName, streamId);
}

/**
 * Removes the channel-to-stream mapping for a channel.
 * @param channelName - The channel name to remove.
 */
export function deleteChannelStreamId(channelName: string): void {

  channelToStreamId.delete(channelName);
}

// Termination State Functions.

/**
 * Checks if termination has been initiated for a stream.
 * @param streamId - The stream ID to check.
 * @returns True if termination has been initiated, false otherwise.
 */
export function isTerminationInitiated(streamId: number): boolean {

  return terminationInitiated.has(streamId);
}

// Capture Stream Cleanup.

/**
 * Destroys the raw capture stream to ensure chrome.tabCapture releases the capture. This MUST be called before closing the page to prevent capture state corruption.
 * When the stream is destroyed, puppeteer-stream's close handler fires synchronously (while the browser is still connected), which calls STOP_RECORDING in the
 * extension. Without this, the extension may think a capture is still active, causing subsequent getStream() calls to hang with "Cannot capture a tab with an active
 * stream" errors.
 * @param rawCaptureStream - The raw capture stream from puppeteer-stream, or null if not available.
 */
function destroyCaptureStream(rawCaptureStream: Nullable<Readable>): void {

  if(!rawCaptureStream) {

    return;
  }

  if(!rawCaptureStream.destroyed) {

    rawCaptureStream.destroy();
  }
}

// Stream Termination.

/**
 * Terminates a stream, cleaning up all resources. This is the authoritative termination function that all code paths should use for consistent cleanup.
 *
 * Note: This function does NOT call emitCurrentSystemStatus() to avoid circular dependencies with the browser module. Callers should call emitCurrentSystemStatus()
 * after termination if they need to update the SSE system status.
 * @param streamId - The numeric stream ID.
 * @param channelName - The channel name for channel mapping cleanup.
 * @param reason - The reason for termination (e.g., "idle timeout", "circuit breaker").
 */
export function terminateStream(streamId: number, channelName: string, reason: string): void {

  // Mark termination as initiated to suppress spurious warnings from segmenter callbacks.
  if(terminationInitiated.has(streamId)) {

    // Termination already in progress.
    return;
  }

  terminationInitiated.add(streamId);

  // Get stream info early to calculate duration and access resources.
  const streamInfo = getStream(streamId);
  const durationMs = streamInfo ? (Date.now() - streamInfo.startTime.getTime()) : 0;

  // Abort pending evaluate calls for this stream. This immediately rejects any pending page.evaluate() calls, preventing them from hanging for up to 180 seconds
  // (Puppeteer's protocolTimeout). We do this first so pending operations fail fast before we start cleaning up resources.
  if(streamInfo?.streamIdStr) {

    const controller = getAbortController(streamInfo.streamIdStr);

    if(controller) {

      controller.abort();
    }

    unregisterAbortController(streamInfo.streamIdStr);
  }

  // Destroy the raw capture stream BEFORE killing FFmpeg or closing the page. This triggers puppeteer-stream's close handler while the browser is still connected,
  // ensuring STOP_RECORDING is called and chrome.tabCapture releases the capture. Without this, subsequent getStream() calls may hang with "active stream" errors.
  if(streamInfo) {

    destroyCaptureStream(streamInfo.rawCaptureStream);
  }

  // Kill the FFmpeg process if using FFmpeg mode. This sets FFmpeg's internal shuttingDown flag, so when the segmenter stops (which closes the capture stream and
  // FFmpeg's stdin), FFmpeg won't report spurious errors about truncated input. The order matters: kill() must be called before segmenter.stop() to set the flag
  // before stdin closes.
  if(streamInfo?.ffmpegProcess) {

    streamInfo.ffmpegProcess.kill();
  }

  // Capture keyframe statistics before stopping the segmenter. The stats are a snapshot of the accumulated state, so they remain valid after stop() is called.
  let keyframeStats: Nullable<KeyframeStats> = null;

  if(streamInfo?.segmenter) {

    keyframeStats = streamInfo.segmenter.getKeyframeStats();
    streamInfo.segmenter.stop();
  }

  // Remove channel mapping.
  if(channelToStreamId.get(channelName) === streamId) {

    channelToStreamId.delete(channelName);
  }

  // Clean up stream resources and capture recovery metrics.
  let recoveryMetrics: Nullable<RecoveryMetrics> = null;

  if(streamInfo) {

    // Stop the health monitor and get recovery metrics.
    if(streamInfo.stopMonitor) {

      recoveryMetrics = streamInfo.stopMonitor();
    }

    // Close the browser page. Skip during graceful shutdown since closeBrowser() will close all pages and we'd get spurious "Target closed" errors.
    if(!isGracefulShutdown() && !streamInfo.page.isClosed()) {

      streamInfo.page.close().catch((error) => {

        LOG.warn("Error closing page for stream %s: %s.", streamId, formatError(error));
      });
    }
  }

  // Notify MPEG-TS clients that this stream is ending. This must happen before unregisterStream() which destroys the HLSState. Removing all listeners prevents
  // memory leaks from orphaned event handlers.
  if(streamInfo) {

    streamInfo.hls.segmentEmitter.emit("terminated");
    streamInfo.hls.segmentEmitter.removeAllListeners();
  }

  // Unregister from registry. This also clears the HLS segment storage.
  unregisterStream(streamId);

  // Clear client tracking data for this stream.
  clearClients(streamId);

  // Clear cached show name.
  clearShowName(streamId);

  // Emit stream removed event.
  emitStreamRemoved(streamId);

  // Clean up termination tracking.
  terminationInitiated.delete(streamId);

  // Log termination with stream ID prefix since we're outside the stream context. Include recovery summary if there were any recoveries.
  const streamIdStr = streamInfo?.streamIdStr ?? ("s" + String(streamId).padStart(4, "0"));
  const reasonSuffix = (reason === "no active clients") ? "" : " (" + reason + ")";
  const streamLog = LOG.withStreamId(streamIdStr);

  // Include recovery summary and keyframe statistics in the termination log when available.
  const keyframeSummary = keyframeStats ? formatKeyframeStatsSummary(keyframeStats) : "";

  if(recoveryMetrics && (getTotalRecoveryAttempts(recoveryMetrics) > 0) && keyframeSummary) {

    streamLog.info("Stream ended after %s%s. %s %s", formatDuration(durationMs), reasonSuffix, formatRecoveryMetricsSummary(recoveryMetrics), keyframeSummary);
  } else if(recoveryMetrics && (getTotalRecoveryAttempts(recoveryMetrics) > 0)) {

    streamLog.info("Stream ended after %s%s. %s", formatDuration(durationMs), reasonSuffix, formatRecoveryMetricsSummary(recoveryMetrics));
  } else if(keyframeSummary) {

    streamLog.info("Stream ended after %s%s. %s", formatDuration(durationMs), reasonSuffix, keyframeSummary);
  } else {

    streamLog.info("Stream ended after %s%s.", formatDuration(durationMs), reasonSuffix);
  }
}
