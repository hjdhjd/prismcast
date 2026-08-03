/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lifecycle.ts: Stream lifecycle management for PrismCast.
 */
import type { KeyframeStats, SessionStats } from "./fmp4Segmenter.ts";
import { LOG, formatDuration, formatError, getAbortController, unregisterAbortController } from "../utils/index.ts";
import { cancelPrerollTimer, getStream, unregisterStream } from "./registry.ts";
import { formatKeyframeStatsSummary, formatSessionStatsSummary } from "./fmp4Segmenter.ts";
import { formatRecoveryMetricsSummary, getTotalRecoveryAttempts } from "./recovery.ts";
import { isGracefulShutdown, unregisterManagedPage } from "../browser/index.ts";
import type { Nullable } from "../types/index.ts";
import type { RecoveryMetrics } from "./recovery.ts";
import type { StreamRegistryEntry } from "./registry.ts";
import { clearClients } from "./clients.ts";
import { clearPretuneSafetyTimer } from "./pretuneTimers.ts";
import { clearShowName } from "./showInfo.ts";
import { emitStreamRemoved } from "./statusEmitter.ts";

/* This module provides the authoritative stream termination logic. All code paths that need to terminate a stream should call terminateStream() from this module. This
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
 * Set of stream IDs whose termination is in flight. Serves as the idempotency guard that makes redundant terminateStream() calls a no-op (callers may issue them
 * freely), and lets in-flight segmenter and monitor callbacks suppress spurious warnings during cleanup.
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

// Stream Termination.

/**
 * Disposes a stream's owned resources in the correct teardown order. The stream is the root of an ownership tree whose leaves are self-disposing nodes - the health
 * monitor, the native proxy, and the capture session - and this function composes them, inlining the simple owned operations (aborting the per-stream
 * AbortController, cancelling the deferred preroll timer, closing the page).
 *
 * It lives here, in the lifecycle owner, rather than as a [Symbol.dispose] on the registry entry, because the stream's teardown is not self-contained: it reads the
 * per-stream AbortController from a side registry and the graceful-shutdown flag from the browser module. (The entry's index/membership cleanup - registry, client
 * tracking, show-name cache, SSE - stays in terminateStream for the same reason: those cross-module registries reference the stream, and the orchestrator removes
 * them rather than the entry disposing itself from its own containers.) The genuinely self-contained resources are the nodes this composes; the stream composes them.
 *
 * The order matters: abort first so pending page.evaluate() calls reject immediately instead of hanging on Puppeteer's 180s protocolTimeout while teardown
 * proceeds; stop the monitor so it no longer reacts to the stream; cancel the preroll timer; dispose the active pipeline (capture and native modes are mutually
 * exclusive - the capture session kills FFmpeg first, then destroys its capture stream, firing STOP_RECORDING while the browser is still connected, then stops
 * the segmenter); and close the page last, after that destroy.
 * @param entry - The registry entry whose owned resources to dispose.
 */
function disposeStreamResources(entry: StreamRegistryEntry): void {

  // Abort any pending page.evaluate() calls so they reject immediately rather than hanging on Puppeteer's protocolTimeout, then unregister the controller.
  getAbortController(entry.streamIdStr)?.abort();
  unregisterAbortController(entry.streamIdStr);

  // Stop the health monitor; it no longer needs to react to the stream. Its metrics were already read in the termination prologue.
  entry.monitor?.dispose();

  // Cancel the deferred preroll timer so it cannot fire after teardown and write to the soon-to-be-unregistered HLS state.
  cancelPrerollTimer(entry.hls);

  // Dispose the active pipeline. The CaptureSession kills its FFmpeg child, then destroys its capture stream, then stops its segmenter; the native proxy stops
  // its polling loop and token-refresh timer. Only one is present for a given stream.
  entry.nativeProxy?.stop();
  entry.captureSession?.dispose();

  // Release the page from managed tracking ahead of the close, the unregister-then-close order every other teardown site follows. This is bookkeeping against an
  // in-process set with no Puppeteer call behind it, so it runs whenever there is a page at all - including the cases below where the close itself is skipped,
  // which would otherwise leave the id tracked for a page nobody owns.
  if(entry.page) {

    unregisterManagedPage(entry.page);
  }

  // Close the browser page last - after the capture stream's destroy fired STOP_RECORDING while the browser was still connected. Skip during graceful shutdown
  // (closeBrowser() closes every page; double-closing yields "Target closed" errors). The page is null for a pending entry whose async setup never created one.
  if(entry.page && !isGracefulShutdown() && !entry.page.isClosed()) {

    entry.page.close().catch((error: unknown) => {

      LOG.debug("streaming:hls", "Error closing page for stream %s: %s.", entry.id, formatError(error));
    });
  }
}

/**
 * Terminates a stream, cleaning up all resources. This is the authoritative termination function that all code paths should use for consistent cleanup. It collapses
 * to three readable phases: a prologue that snapshots the summary statistics while the resources are still live, disposeStreamResources() to tear down the stream's
 * owned resources, and the index/membership cleanup plus the termination log.
 *
 * Note: This function does NOT call emitCurrentSystemStatus() to avoid circular dependencies with the browser module. Callers should call emitCurrentSystemStatus()
 * after termination if they need to update the SSE system status.
 * @param streamId - The numeric stream ID.
 * @param channelName - The channel name for channel mapping cleanup.
 * @param reason - The reason for termination (e.g., "idle timeout", "circuit breaker").
 */
export function terminateStream(streamId: number, channelName: string, reason: string): void {

  // The guard makes redundant terminate calls a no-op (callers can issue them freely) and suppresses spurious warnings from in-flight segmenter/monitor callbacks.
  if(terminationInitiated.has(streamId)) {

    return;
  }

  terminationInitiated.add(streamId);

  const streamInfo = getStream(streamId);
  const durationMs = streamInfo ? (Date.now() - streamInfo.startTime.getTime()) : 0;

  // Prologue: snapshot every statistic the termination summary needs while the resources are still live. Each capture-mode resource is a node exposing a read
  // alongside its dispose - the segmenter (via the capture session), the native proxy, and the health monitor - read here, disposed below. The counters remain valid
  // after disposal, but reading up front keeps the disposal purely side-effecting. Capture and native modes are mutually exclusive, so at most one set is present.
  const segmenter = streamInfo?.captureSession?.segmenter;
  const keyframeStats: Nullable<KeyframeStats> = segmenter?.getKeyframeStats() ?? null;
  const segmentCount = segmenter?.getSegmentIndex() ?? 0;
  const sessionStats: Nullable<SessionStats> = segmenter?.getSessionStats() ?? null;
  const nativeProxyStats = streamInfo?.nativeProxy?.getStats();
  const recoveryMetrics: Nullable<RecoveryMetrics> = streamInfo?.monitor?.getMetrics() ?? null;

  // Dispose the stream's owned resources (abort, monitor, preroll timer, pipeline, page) in teardown order.
  if(streamInfo) {

    disposeStreamResources(streamInfo);
  }

  // Index/membership cleanup. The channel mapping, registry entry, client tracking, show-name cache, and SSE consumers are owned by other modules and reference this
  // stream; the orchestrator removes them here. The "terminated" event must fire before unregisterStream() destroys the HLS state, and removing all listeners
  // prevents orphaned-handler leaks.
  if(channelToStreamId.get(channelName) === streamId) {

    channelToStreamId.delete(channelName);
  }

  if(streamInfo) {

    streamInfo.hls.segmentEmitter.emit("terminated");
    streamInfo.hls.segmentEmitter.removeAllListeners();
  }

  unregisterStream(streamId);
  clearClients(streamId);
  clearPretuneSafetyTimer(streamId);
  clearShowName(streamId);
  emitStreamRemoved(streamId);
  terminationInitiated.delete(streamId);

  // Epilogue: compose and emit the termination summary from the prologue snapshot. Logged with the stream-ID prefix since we are outside the stream context.
  const streamIdStr = streamInfo?.streamIdStr ?? ("s" + String(streamId).padStart(4, "0"));

  // "No active clients" is the routine, expected way a stream ends and is omitted from the summary; every other reason is exceptional and worth calling out.
  const reasonSuffix = (reason === "no active clients") ? "" : " (" + reason + ")";
  const streamLog = LOG.withStreamId(streamIdStr);

  // Build termination log message from optional summary components. Each component is appended only when it has meaningful content, avoiding combinatorial branching.
  const summaryParts = ["Stream ended after " + formatDuration(durationMs) + reasonSuffix + "."];

  if(recoveryMetrics && (getTotalRecoveryAttempts(recoveryMetrics) > 0)) {

    summaryParts.push(formatRecoveryMetricsSummary(recoveryMetrics));
  }

  const keyframeSummary = keyframeStats ? formatKeyframeStatsSummary(keyframeStats) : "";

  if(keyframeSummary) {

    summaryParts.push(keyframeSummary);
  }

  streamLog.info(summaryParts.join(" "));

  // Log native proxy statistics at debug level when in native mode. This provides segment delivery and token refresh metrics.
  if(nativeProxyStats) {

    streamLog.debug("native:proxy", "Native proxy stopped for %s. Segments served: %s, fetch errors: %s, token refreshes: %s.",
      channelName, nativeProxyStats.segmentsFetched, nativeProxyStats.fetchErrors, nativeProxyStats.tokenRefreshes);
  }

  // Log session-level segmenter statistics at debug level. This provides A-V sync, tab replacement, and data integrity metrics for diagnosing timestamp issues.
  if(sessionStats) {

    const sessionSummary = formatSessionStatsSummary(sessionStats, segmentCount);

    if(sessionSummary) {

      streamLog.debug("streaming:segmenter", "%s", sessionSummary);
    }
  }
}
