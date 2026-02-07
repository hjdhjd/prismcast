/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * monitor.ts: Playback health monitoring for PrismCast.
 */
import { EvaluateTimeoutError, LOG, formatError, getAbortSignal, isSessionClosedError, runWithStreamContext } from "../utils/index.js";
import type { Frame, Page } from "puppeteer-core";
import type { Nullable, ResolvedSiteProfile, VideoState } from "../types/index.js";
import type { StreamHealthStatus, StreamStatus } from "./statusEmitter.js";
import {
  buildVideoSelectorType, checkVideoPresence, enforceVideoVolume, ensurePlayback, findVideoContext, getVideoState, tuneToChannel, validateVideoElement
} from "../browser/video.js";
import { getChannelLogo, getShowName } from "./showInfo.js";
import { getLastSegmentSize, getStream, getStreamMemoryUsage } from "./registry.js";
import { CONFIG } from "../config/index.js";
import { emitStreamHealthChanged } from "./statusEmitter.js";
import { getClientSummary } from "./clients.js";
import { resizeAndMinimizeWindow } from "../browser/cdp.js";

/* Live video streams can fail in many ways: the network can drop, the player can stall, the site can auto-pause, or ads can break playback. The health monitor
 * watches for these failures and attempts recovery. This is essential for unattended DVR recording where the user cannot manually intervene.
 *
 * The monitor runs on a configurable interval (default: 2 seconds) and performs these checks:
 *
 * 1. Video progression: Compares currentTime to previous check. If currentTime has not advanced by at least STALL_THRESHOLD (0.1 seconds), the video is considered
 *    stalled. However, a single stall is not enough to trigger recovery - we require STALL_COUNT_THRESHOLD (2) consecutive stalls to avoid reacting to momentary
 *    hiccups.
 *
 * 2. Buffering detection: Checks readyState and networkState to detect active buffering. Live streams occasionally buffer due to network conditions, so we allow a
 *    BUFFERING_GRACE_PERIOD (default 10 seconds) before declaring a stall. This prevents unnecessary recovery during normal buffering.
 *
 * 3. Volume enforcement: Some sites aggressively mute videos (e.g., France24). The monitor checks and restores volume on every interval to ensure audio capture.
 *
 * 4. Issue-aware recovery: When issues are detected, recovery is tailored to the issue type:
 *    - Paused issues: Try Level 1 (play/unmute) first, then escalate to Level 2 if that fails.
 *    - Buffering issues: Skip Level 1 (ineffective for buffering) and go directly to Level 2.
 *    - If Level 2 has already been attempted, skip to Level 3 (second L2 attempts always fail).
 *
 *    Recovery levels:
 *    - Level 1: Basic play/unmute and fullscreen (only for paused issues).
 *    - Level 2: Reload video source (first attempt has ~58% success rate).
 *    - Level 3: Full page navigation (always succeeds).
 *
 * 5. Circuit breaker: If too many failures occur within a time window (default: 10 failures in 5 minutes), the stream is considered fundamentally broken and the
 *    circuit breaker trips, terminating the stream. This prevents endless recovery attempts that consume resources.
 *
 * 6. Escalation reset: After SUSTAINED_PLAYBACK_REQUIRED (60 seconds) of healthy playback, the escalation level resets to 0, the source reload tracking clears,
 *    and the circuit breaker resets. This allows a stream that recovered to start fresh, rather than immediately escalating to aggressive recovery on the next issue.
 *
 * 7. Window re-minimize: Recovery actions (especially fullscreen) can cause the browser window to un-minimize. After the recovery grace period passes and the first
 *    healthy check occurs, the window is re-minimized to reduce GPU usage. This happens sooner than the escalation reset (~5-10 seconds vs 60 seconds) because we
 *    don't need to wait for sustained playback to determine the window can be minimized.
 *
 * The monitor is designed to be resilient to page navigations and context changes. When a page navigation recovery is performed, the monitor updates its context
 * reference to the new video context.
 */

/* The monitor tracks recovery statistics throughout the stream's lifetime. These metrics are returned when the monitor is stopped and included in the stream
 * termination log for analytics and troubleshooting.
 */

/**
 * Recovery metrics tracked throughout the stream's lifetime. Returned when the monitor stops for inclusion in termination logs.
 */
export interface RecoveryMetrics {

  // Timestamp when current recovery started, or null if not recovering. Used to calculate recovery duration.
  currentRecoveryStartTime: Nullable<number>;

  // The recovery method currently in progress, for logging success. Null if not recovering.
  currentRecoveryMethod: Nullable<string>;

  // Page navigation recovery statistics.
  pageNavigationAttempts: number;
  pageNavigationSuccesses: number;

  // Play/unmute recovery statistics.
  playUnmuteAttempts: number;
  playUnmuteSuccesses: number;

  // Source reload recovery statistics.
  sourceReloadAttempts: number;
  sourceReloadSuccesses: number;

  // Tab replacement recovery statistics.
  tabReplacementAttempts: number;
  tabReplacementSuccesses: number;

  // Total recovery time in milliseconds, for calculating average.
  totalRecoveryTimeMs: number;
}

// Recovery method names used in logging. Centralized to ensure consistency across start, success, and failure messages.
const RECOVERY_METHODS = {

  pageNavigation: "page navigation",
  playUnmute: "play/unmute",
  sourceReload: "source reload",
  tabReplacement: "tab replacement"
} as const;

// Type for recovery method values.
type RecoveryMethodValue = typeof RECOVERY_METHODS[keyof typeof RECOVERY_METHODS];

/* These mappings connect recovery method names to their corresponding counter fields in RecoveryMetrics. Using a mapping pattern instead of if/else chains reduces
 * code duplication, makes adding new recovery methods trivial (add one entry to each map), ensures consistency between attempt and success counting, and provides
 * type safety via the RecoveryMetrics interface.
 */

// Maps recovery method names to their attempt counter field names.
const ATTEMPT_FIELDS: Record<RecoveryMethodValue, keyof RecoveryMetrics> = {

  [RECOVERY_METHODS.pageNavigation]: "pageNavigationAttempts",
  [RECOVERY_METHODS.playUnmute]: "playUnmuteAttempts",
  [RECOVERY_METHODS.sourceReload]: "sourceReloadAttempts",
  [RECOVERY_METHODS.tabReplacement]: "tabReplacementAttempts"
};

// Maps recovery method names to their success counter field names.
const SUCCESS_FIELDS: Record<RecoveryMethodValue, keyof RecoveryMetrics> = {

  [RECOVERY_METHODS.pageNavigation]: "pageNavigationSuccesses",
  [RECOVERY_METHODS.playUnmute]: "playUnmuteSuccesses",
  [RECOVERY_METHODS.sourceReload]: "sourceReloadSuccesses",
  [RECOVERY_METHODS.tabReplacement]: "tabReplacementSuccesses"
};

/**
 * Creates a new RecoveryMetrics object with all counters initialized to zero.
 * @returns A fresh RecoveryMetrics object.
 */
function createRecoveryMetrics(): RecoveryMetrics {

  return {

    currentRecoveryMethod: null,
    currentRecoveryStartTime: null,
    pageNavigationAttempts: 0,
    pageNavigationSuccesses: 0,
    playUnmuteAttempts: 0,
    playUnmuteSuccesses: 0,
    sourceReloadAttempts: 0,
    sourceReloadSuccesses: 0,
    tabReplacementAttempts: 0,
    tabReplacementSuccesses: 0,
    totalRecoveryTimeMs: 0
  };
}

/**
 * Gets the total number of recovery attempts across all methods. Iterates over ATTEMPT_FIELDS to sum all attempt counters, ensuring new recovery methods are
 * automatically included without code changes.
 * @param metrics - The recovery metrics object.
 * @returns Total recovery attempts.
 */
export function getTotalRecoveryAttempts(metrics: RecoveryMetrics): number {

  let total = 0;

  for(const fieldName of Object.values(ATTEMPT_FIELDS)) {

    total += metrics[fieldName] as number;
  }

  return total;
}

/**
 * Gets the total number of successful recoveries across all methods. Iterates over SUCCESS_FIELDS to sum all success counters, ensuring new recovery methods
 * are automatically included without code changes.
 * @param metrics - The recovery metrics object.
 * @returns Total successful recoveries.
 */
function getTotalRecoverySuccesses(metrics: RecoveryMetrics): number {

  let total = 0;

  for(const fieldName of Object.values(SUCCESS_FIELDS)) {

    total += metrics[fieldName] as number;
  }

  return total;
}

/**
 * Formats recovery duration from start time to now.
 * @param startTime - The timestamp when recovery started.
 * @returns Formatted duration string like "2.1s".
 */
function formatRecoveryDuration(startTime: number): string {

  const durationMs = Date.now() - startTime;

  return (durationMs / 1000).toFixed(1) + "s";
}

/**
 * Maps issue category to user-friendly description for logging.
 * @param category - The issue category from getIssueCategory().
 * @returns User-friendly description.
 */
function getIssueDescription(category: "paused" | "buffering" | "other"): string {

  if(category === "paused") {

    return "paused";
  }

  if(category === "buffering") {

    return "buffering";
  }

  return "stalled";
}

/**
 * Maps recovery level to method name.
 * @param level - The recovery level (1, 2, or 3).
 * @returns The recovery method name.
 */
function getRecoveryMethod(level: number): string {

  if(level === 1) {

    return RECOVERY_METHODS.playUnmute;
  }

  if(level === 2) {

    return RECOVERY_METHODS.sourceReload;
  }

  return RECOVERY_METHODS.pageNavigation;
}

/**
 * Records a recovery attempt in the metrics. Uses the ATTEMPT_FIELDS mapping to find the correct counter field, eliminating the need for if/else chains. This
 * makes adding new recovery methods trivial - just add an entry to ATTEMPT_FIELDS.
 * @param metrics - The metrics object to update.
 * @param method - The recovery method being attempted.
 */
function recordRecoveryAttempt(metrics: RecoveryMetrics, method: string): void {

  // Cast to the specific field type to handle potential unknown methods at runtime. The mapping ensures valid methods resolve to counter field names.
  const field = ATTEMPT_FIELDS[method as RecoveryMethodValue] as keyof RecoveryMetrics | undefined;

  if(field !== undefined) {

    (metrics[field] as number)++;
  }

  metrics.currentRecoveryStartTime = Date.now();
  metrics.currentRecoveryMethod = method;
}

/**
 * Records a successful recovery in the metrics and clears the pending recovery state. Uses the SUCCESS_FIELDS mapping to find the correct counter field,
 * eliminating the need for if/else chains. This makes adding new recovery methods trivial - just add an entry to SUCCESS_FIELDS.
 * @param metrics - The metrics object to update.
 * @param method - The recovery method that succeeded.
 */
function recordRecoverySuccess(metrics: RecoveryMetrics, method: string): void {

  // Cast to the specific field type to handle potential unknown methods at runtime. The mapping ensures valid methods resolve to counter field names.
  const field = SUCCESS_FIELDS[method as RecoveryMethodValue] as keyof RecoveryMetrics | undefined;

  if(field !== undefined) {

    (metrics[field] as number)++;
  }

  if(metrics.currentRecoveryStartTime !== null) {

    metrics.totalRecoveryTimeMs += Date.now() - metrics.currentRecoveryStartTime;
  }

  metrics.currentRecoveryStartTime = null;
  metrics.currentRecoveryMethod = null;
}

/**
 * Capitalizes the first letter of a string.
 * @param str - The string to capitalize.
 * @returns The string with the first letter capitalized.
 */
function capitalize(str: string): string {

  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Formats the recovery metrics summary for the termination log. Uses the SUCCESS_FIELDS mapping to iterate over all recovery methods, eliminating hardcoded
 * checks for each method type. This ensures new recovery methods are automatically included in the summary.
 * @param metrics - The recovery metrics object.
 * @returns Formatted summary string, or empty string if no recoveries occurred.
 */
export function formatRecoveryMetricsSummary(metrics: RecoveryMetrics): string {

  const totalAttempts = getTotalRecoveryAttempts(metrics);

  if(totalAttempts === 0) {

    return "No recoveries needed.";
  }

  const totalSuccesses = getTotalRecoverySuccesses(metrics);

  // Build the breakdown of recovery methods used by iterating over all methods in SUCCESS_FIELDS. This automatically includes any new recovery methods added to
  // the mapping without requiring code changes here.
  const parts: string[] = [];

  for(const [ methodName, fieldName ] of Object.entries(SUCCESS_FIELDS)) {

    const count = metrics[fieldName] as number;

    if(count > 0) {

      parts.push(String(count) + "× " + methodName);
    }
  }

  // Calculate average recovery time.
  const avgTimeMs = totalSuccesses > 0 ? metrics.totalRecoveryTimeMs / totalSuccesses : 0;
  const avgTimeStr = (avgTimeMs / 1000).toFixed(1) + "s";

  // Format: "Recoveries: 8 (5× source reload, 3× page navigation), avg 4.2s."
  if(parts.length > 0) {

    return "Recoveries: " + String(totalSuccesses) + " (" + parts.join(", ") + "), avg " + avgTimeStr + ".";
  }

  // Edge case: attempts but no successes (stream terminated before recovery completed).
  return "Recoveries: " + String(totalAttempts) + " attempted, 0 succeeded.";
}

/**
 * Circuit breaker state for tracking failures within a time window. The circuit breaker prevents endless recovery attempts on fundamentally broken streams by
 * terminating after a threshold of failures within a configured window.
 */
interface CircuitBreakerState {

  // Timestamp of the first failure in the current window. Used to determine if failures are within the window.
  firstFailureTime: Nullable<number>;

  // Total number of failures within the circuit breaker window.
  totalFailureCount: number;
}

/**
 * Result from checking circuit breaker state.
 */
interface CircuitBreakerResult {

  // Whether the circuit breaker should trip (terminate the stream).
  shouldTrip: boolean;

  // Total count of failures recorded.
  totalCount: number;

  // Whether we're within the time window from the first failure.
  withinWindow: boolean;
}

/**
 * Records a failure and checks whether the circuit breaker should trip. This centralizes the circuit breaker logic that was previously duplicated in multiple
 * recovery paths. The function updates the state in place and returns whether the breaker should trip.
 * @param state - The circuit breaker state to update.
 * @param now - The current timestamp.
 * @returns Result indicating whether the circuit breaker should trip and diagnostic info.
 */
function checkCircuitBreaker(state: CircuitBreakerState, now: number): CircuitBreakerResult {

  // Record this failure.
  state.totalFailureCount++;
  state.firstFailureTime ??= now;

  // Check if we're within the failure window.
  const withinWindow = (now - state.firstFailureTime) < CONFIG.recovery.circuitBreakerWindow;

  // Determine if we should trip.
  const shouldTrip = withinWindow && (state.totalFailureCount >= CONFIG.recovery.circuitBreakerThreshold);

  // Reset the window if we're outside it (start fresh count).
  if(!withinWindow) {

    state.totalFailureCount = 1;
    state.firstFailureTime = now;
  }

  return { shouldTrip, totalCount: state.totalFailureCount, withinWindow };
}

/**
 * Resets the circuit breaker state. Called when sustained healthy playback is achieved.
 * @param state - The circuit breaker state to reset.
 */
function resetCircuitBreaker(state: CircuitBreakerState): void {

  state.firstFailureTime = null;
  state.totalFailureCount = 0;
}

/**
 * Result from attempting page navigation recovery.
 */
interface PageNavigationRecoveryResult {

  // The new video context if recovery succeeded, undefined otherwise.
  newContext?: Frame | Page;

  // Whether recovery succeeded (video element found and validated).
  success: boolean;
}

/**
 * Result from tab replacement recovery. When a browser tab becomes unresponsive (consecutive evaluate timeouts), the recovery handler closes the old tab, creates a
 * new one with fresh capture, and returns the new page and context. The monitor then updates its internal references to continue monitoring the new tab.
 */
export interface TabReplacementResult {

  // The video context (page or frame containing the video element).
  context: Frame | Page;

  // The new browser page.
  page: Page;
}

/**
 * Formats the issue type for diagnostic logging. Returns a human-readable string describing what triggered the recovery. Multiple issues can occur simultaneously
 * (e.g., "paused, stalled"), so we collect all applicable issues into a comma-separated list.
 * @param state - The video state object containing paused, ended, hasError, etc.
 * @param isStalled - Whether the video is stalled (not progressing).
 * @param isBuffering - Whether the video is actively buffering.
 * @returns A description of the issue.
 */
function formatIssueType(state: VideoState, isStalled: boolean, isBuffering: boolean): string {

  const issues: string[] = [];

  if(state.paused) {

    issues.push("paused");
  }

  if(state.ended) {

    issues.push("ended");
  }

  if(state.error) {

    issues.push("error");
  }

  // Distinguish between buffering (temporary, network-related) and stalled (stopped for unknown reason). Both result in no progression, but buffering indicates the
  // player is actively trying to get more data.
  if(isStalled && isBuffering) {

    issues.push("buffering");
  }

  if(isStalled && !isBuffering) {

    issues.push("stalled");
  }

  return issues.length > 0 ? issues.join(", ") : "unknown";
}

/**
 * Determines the issue category for recovery path selection. This is separate from formatIssueType (which is for logging) because recovery decisions need a single
 * category, not a list of all issues. The categories are:
 * - "paused": Video is paused but not buffering. L1 (play/unmute) may help.
 * - "buffering": Video is buffering or stalled with low readyState. Skip L1, go to L2 (source reload).
 * - "other": Error, ended, or unknown state. Skip L1, go to L2 (source reload).
 * @param state - The video state object.
 * @param isStalled - Whether the video is stalled (not progressing).
 * @param isBuffering - Whether the video is actively buffering.
 * @returns The issue category for recovery path selection.
 */
function getIssueCategory(state: VideoState, isStalled: boolean, isBuffering: boolean): "paused" | "buffering" | "other" {

  // Error and ended states take priority - these need aggressive recovery.
  if(state.error || state.ended) {

    return "other";
  }

  // Buffering (readyState < 3 with active network) needs source reload, not play/unmute.
  if(isBuffering) {

    return "buffering";
  }

  // Stalled with low readyState is effectively buffering.
  if(isStalled && (state.readyState < 3)) {

    return "buffering";
  }

  // Paused state (without buffering) may respond to play/unmute.
  if(state.paused) {

    return "paused";
  }

  // Stalled without low readyState - unknown cause, treat as buffering.
  if(isStalled) {

    return "buffering";
  }

  return "other";
}

/**
 * Monitors video playback health and attempts escalating recovery when issues are detected. This function runs on an interval, checking video state and triggering
 * increasingly aggressive recovery actions when playback stalls, pauses, or errors occur.
 *
 * The monitor includes a circuit breaker that terminates the stream after a configurable number of consecutive failures within a time window. This prevents endless
 * recovery attempts on fundamentally broken streams.
 *
 * Tab replacement recovery: When the browser tab becomes unresponsive (3+ consecutive evaluate timeouts), the monitor triggers tab replacement via the
 * onTabReplacement callback. This closes the hung tab, creates a fresh one with new capture, and continues the stream. This is more reliable than page.goto-based
 * recovery because a hung tab may not respond to navigation commands.
 *
 * The returned cleanup function should be called when the stream ends to stop monitoring and release resources.
 *
 * @param page - The Puppeteer page object.
 * @param context - The frame or page containing the video element.
 * @param profile - The site profile for behavior configuration.
 * @param url - The URL of the stream, needed for page reload recovery.
 * @param streamId - A concise identifier for the stream, used in log messages.
 * @param streamInfo - Stream metadata for status updates.
 * @param onCircuitBreak - Callback function called when circuit breaker trips.
 * @param onTabReplacement - Optional callback for tab replacement recovery. When provided and 3+ consecutive timeouts occur, this is called to replace the hung tab.
 *                           If null/undefined, tab replacement is not available and timeouts will eventually trip the circuit breaker.
 * @returns A cleanup function to stop the monitor.
 */
/**
 * Stream info passed to the monitor for status updates.
 */
export interface MonitorStreamInfo {

  channelName: Nullable<string>;
  numericStreamId: number;
  providerName: string;
  startTime: Date;
}

export function monitorPlaybackHealth(
  page: Page,
  context: Frame | Page,
  profile: ResolvedSiteProfile,
  url: string,
  streamId: string,
  streamInfo: MonitorStreamInfo,
  onCircuitBreak: () => void,
  onTabReplacement?: () => Promise<TabReplacementResult | null>
): () => RecoveryMetrics {

  /*
   * Monitor state variables. These track the video's behavior over time and control recovery decisions.
   */

  // The current page reference. This can change after tab replacement recovery, when the old tab is closed and a new one is created. We use a mutable variable so we
  // can update the reference after replacement.
  let currentPage = page;

  // The video's currentTime from the previous check. Used to detect whether the video is progressing. Null on first check since we have no previous value.
  let lastTime: Nullable<number> = null;

  // Number of consecutive checks where currentTime did not advance. We require multiple consecutive stalls before triggering recovery to avoid reacting to momentary
  // hiccups. Reset to 0 when progression is detected.
  let stallCount = 0;

  // Current escalation level (0-4). Level 0 means no recovery needed. Each time recovery is triggered, this increments to try increasingly aggressive actions.
  // Resets to 0 after sustained healthy playback.
  let escalationLevel = 0;

  // Timestamp of the last recovery attempt. Used to calculate healthy playback duration for escalation reset.
  let lastRecoveryTime = 0;

  // Timestamp when buffering started, or null if not currently buffering. Used to apply the buffering grace period - we don't trigger recovery for buffering until
  // it exceeds BUFFERING_GRACE_PERIOD.
  let bufferingStartTime: Nullable<number> = null;

  // Timestamps of recent page reloads within the PAGE_RELOAD_WINDOW. Used to enforce MAX_PAGE_RELOADS limit. Old entries are pruned on each check.
  let pageReloadTimestamps: number[] = [];

  // Counter for consecutive page navigation failures. If navigation fails twice in a row, we fall back to source reload (level 2) instead. This prevents getting
  // stuck in a loop when navigation itself is the problem.
  let consecutiveNavigationFailures = 0;

  // Track whether source reload (L2) has been attempted in the current page session. Log analysis shows the first source reload often works (~58%), but the second
  // always fails and leaves the video at readyState=0. When this flag is true and recovery is needed, we skip L2 and go directly to L3 (page reload).
  let sourceReloadAttempted = false;

  // Segment production monitoring state. After L2/L3 recovery, we track whether segments are actually being produced. If recovery reports success but the capture
  // pipeline is dead (MediaRecorder stopped producing data), we need to escalate to tab replacement.
  let preRecoverySegmentIndex: Nullable<number> = null;      // Segment index when recovery started, used to detect if new segments are produced.
  let segmentWaitStartTime: Nullable<number> = null;         // Timestamp when we started waiting for segment production after recovery grace period.
  let segmentProductionStalled = false;                       // Flag indicating segment production has stalled after recovery.

  // Continuous segment size monitoring state. Detects spontaneous capture pipeline death (no preceding recovery event) by checking segment sizes. Dead pipelines
  // produce tiny segments (18 bytes observed) while the video element appears healthy. This complements post-recovery index monitoring.
  let lastCheckedSegmentIndex = 0;                            // Last segment index we inspected (to detect new segments).
  let consecutiveTinySegments = 0;                            // Count of consecutive tiny segments.
  let wasInTinySegmentState = false;                          // For detecting spontaneous recovery (tiny→valid transition without explicit recovery).

  // Track whether the browser window needs to be re-minimized after recovery. Recovery actions (especially ensureFullscreen) can cause the window to un-minimize.
  // We set this flag when recovery is triggered and clear it after the recovery grace period passes and we see a healthy check.
  let pendingReMinimize = false;

  // Flag indicating the cleanup function was called. When true, the next interval check will clear itself.
  let intervalCleared = false;

  // Flag indicating a recovery operation is in progress. We skip health checks during recovery to avoid triggering additional recovery while one is running.
  let recoveryInProgress = false;

  // The current video context (page or frame). This can change after a page navigation recovery, when we need to find the new video context.
  let currentContext: Frame | Page = context;

  // Circuit breaker state. Tracks total failures within a time window and trips (terminates the stream) when too many failures occur.
  const circuitBreaker: CircuitBreakerState = { firstFailureTime: null, totalFailureCount: 0 };

  // Counter for consecutive "video not found" occurrences. We apply a grace period before triggering recovery to handle momentary context invalidation or readyState
  // fluctuations. Reset to 0 when video is found.
  let videoNotFoundCount = 0;

  // Counter for consecutive evaluate timeouts. When the browser tab becomes unresponsive, evaluate() calls will timeout instead of returning data. After 3
  // consecutive timeouts, we trigger tab replacement recovery (if the callback is provided). Reset to 0 on successful getVideoState().
  let consecutiveTimeouts = 0;

  // Total recovery attempts for status reporting.
  let totalRecoveryAttempts = 0;

  // Last known video state for status reporting.
  let lastVideoState: Nullable<VideoState> = null;

  // Recovery metrics tracked throughout the stream's lifetime.
  const metrics = createRecoveryMetrics();

  // Last issue tracking for UI display. Stores what triggered recovery and when, so users can see stream history.
  let lastIssueType: Nullable<string> = null;
  let lastIssueTime: Nullable<number> = null;

  // Recovery grace period. After a recovery action, we wait before checking for new issues to give the action time to take effect. L1 (play/unmute) is a quick
  // action. L2 (source reload) and L3 (page reload) need more time for rebuffering/navigation.
  const recoveryGracePeriods = [ 0, 3000, 10000, 10000 ];  // L0, L1, L2, L3 in milliseconds.

  // Segment stall timeout. After L2/L3 recovery completes, if no new segments are produced within this timeout, the capture pipeline is considered dead and we
  // escalate directly to tab replacement. This catches the case where recovery reports success but the MediaRecorder/FFmpeg pipeline has silently died.
  const SEGMENT_STALL_TIMEOUT = 10000;  // 10 seconds.

  // Tiny segment detection thresholds. Used for continuous segment size monitoring to detect dead capture pipelines. When video capture dies but audio continues,
  // segments contain only audio data (~400KB for 3-second segments at 130Kbps AAC). Valid segments with video are 5-10MB. The 2MB threshold catches both completely
  // dead captures (18 bytes) and audio-only captures (~400KB) while providing a 2.5x safety margin against false positives on valid segments.
  const TINY_SEGMENT_THRESHOLD = 2097152; // 2MB - segments below this indicate dead or degraded capture.
  const TINY_SEGMENT_COUNT_TRIGGER = 10;  // Trigger recovery after 10 consecutive tiny segments (~20 seconds with 2-second segments).

  let recoveryGraceUntil = 0;

  // Pre-compute the selector type string for video element selection. This is passed to evaluate() calls.
  const selectorType = buildVideoSelectorType(profile);

  // Capture stream context for re-establishing on each interval tick. AsyncLocalStorage context is lost when entering setInterval callbacks.
  const streamContext = { channelName: streamInfo.channelName ?? undefined, streamId, url };

  // Helper to mark a discontinuity in the HLS playlist after recovery events that disrupt the video source. The segmenter flushes its current fragment buffer and sets
  // a pending discontinuity flag so the next segment boundary includes an #EXT-X-DISCONTINUITY tag. This tells HLS clients to flush their decoder state.
  const markStreamDiscontinuity = (): void => {

    getStream(streamInfo.numericStreamId)?.segmenter?.markDiscontinuity();
  };

  /**
   * Computes the health status classification based on current monitor state.
   * @returns The health status classification.
   */
  function computeHealthStatus(): StreamHealthStatus {

    // Error state takes precedence.
    if(lastVideoState?.error) {

      return "error";
    }

    // If we're at escalation level 3 (page reload), we're in serious trouble.
    if(escalationLevel >= 3) {

      return "error";
    }

    // If we're actively recovering (levels 1-2).
    if(escalationLevel > 0) {

      return "recovering";
    }

    // If we're buffering (within grace period).
    if(bufferingStartTime !== null) {

      return "buffering";
    }

    // If we have consecutive stalls but not yet triggering recovery.
    if(stallCount > 0) {

      return "stalled";
    }

    return "healthy";
  }

  /**
   * Emits a status update for this stream.
   */
  function emitStatusUpdate(): void {

    const now = Date.now();

    // Get current memory usage from the stream's HLS segment buffers.
    const entry = getStream(streamInfo.numericStreamId);
    const memoryBytes = entry ? getStreamMemoryUsage(entry).total : 0;

    // Get the channel key from the registry entry for logo lookup.
    const channelKey = entry?.info.storeKey ?? "";

    // Get current client counts and type breakdown for this stream.
    const clientSummary = getClientSummary(streamInfo.numericStreamId);

    const status: StreamStatus = {

      bufferingDuration: bufferingStartTime ? Math.round((now - bufferingStartTime) / 1000) : null,
      channel: streamInfo.channelName,
      clientCount: clientSummary.total,
      clients: clientSummary.clients,
      currentTime: lastVideoState?.time ?? 0,
      duration: Math.round((now - streamInfo.startTime.getTime()) / 1000),
      escalationLevel,
      health: computeHealthStatus(),
      id: streamInfo.numericStreamId,
      lastIssueTime,
      lastIssueType,
      lastRecoveryTime: lastRecoveryTime > 0 ? lastRecoveryTime : null,
      logoUrl: channelKey ? (getChannelLogo(channelKey) ?? "") : "",
      memoryBytes,
      networkState: lastVideoState?.networkState ?? 0,
      pageReloadsInWindow: pageReloadTimestamps.length,
      providerName: streamInfo.providerName,
      readyState: lastVideoState?.readyState ?? 0,
      recoveryAttempts: totalRecoveryAttempts,
      showName: getShowName(streamInfo.numericStreamId),
      startTime: streamInfo.startTime.toISOString(),
      url
    };

    emitStreamHealthChanged(status);
  }

  /**
   * Finalizes tab replacement by clearing the recovery flag and emitting status. This helper ensures consistent cleanup across all tab replacement exit paths (success,
   * failure, and error). The flag must be reset before emitting status to prevent the monitor from getting stuck if emitStatusUpdate() throws.
   */
  function finalizeTabReplacement(): void {

    recoveryInProgress = false;

    emitStatusUpdate();
  }

  /**
   * Resets all segment monitoring state variables. Called after successful recovery or sustained healthy playback to clear tracking for both post-recovery index
   * monitoring and continuous size monitoring.
   */
  function resetSegmentMonitoringState(): void {

    preRecoverySegmentIndex = null;
    segmentWaitStartTime = null;
    segmentProductionStalled = false;
    consecutiveTinySegments = 0;
    wasInTinySegmentState = false;
    lastCheckedSegmentIndex = getStream(streamInfo.numericStreamId)?.segmenter?.getSegmentIndex() ?? 0;
  }

  /**
   * Resets all failure/retry counters to zero. Called after successful tab replacement or page navigation to give the stream a fresh start.
   */
  function resetRecoveryCounters(): void {

    consecutiveTimeouts = 0;
    consecutiveNavigationFailures = 0;
    videoNotFoundCount = 0;
    stallCount = 0;
  }

  /**
   * Resets escalation level and related flags. Called after successful recovery to allow the stream to start from level 0 on future issues.
   */
  function resetEscalationState(): void {

    escalationLevel = 0;
    sourceReloadAttempted = false;
  }

  /**
   * Sets the recovery grace period and re-minimize flag after a recovery action. The grace period prevents the monitor from immediately detecting new issues while the
   * recovery action takes effect.
   * @param level - The recovery level (1-3) to determine grace period duration.
   */
  function setRecoveryGracePeriod(level: number): void {

    pendingReMinimize = true;
    recoveryGraceUntil = Date.now() + recoveryGracePeriods[level];
  }

  /**
   * Tab replacement result type. Indicates whether the replacement succeeded, failed (but stream continues), or terminated (circuit breaker tripped).
   */
  type TabReplacementOutcome = { outcome: "success" } | { outcome: "failed" } | { outcome: "terminated" };

  /**
   * Handles tab replacement failure by checking the circuit breaker. If the breaker trips, terminates the stream. Returns the appropriate outcome for the caller.
   * @param context - Description of the failure for logging.
   * @returns The tab replacement outcome (failed or terminated).
   */
  function handleTabReplacementFailure(context: string): TabReplacementOutcome {

    const cbResult = checkCircuitBreaker(circuitBreaker, Date.now());

    if(cbResult.shouldTrip) {

      LOG.error("Circuit breaker tripped after %s. Stream appears fundamentally broken.", context);

      clearInterval(interval);
      onCircuitBreak();

      return { outcome: "terminated" };
    }

    return { outcome: "failed" };
  }

  /**
   * Executes tab replacement recovery with full error handling. This unified helper handles all tab replacement triggers (tiny segments, stalled capture, unresponsive
   * tab) consistently, including metrics recording, success/failure logging, circuit breaker checks, and state resets.
   * @param issueType - Description of what triggered the replacement (for logging and UI display).
   * @returns The tab replacement outcome.
   */
  async function executeTabReplacement(issueType: string): Promise<TabReplacementOutcome> {

    // Guard: caller should ensure onTabReplacement exists, but TypeScript needs explicit narrowing.
    if(!onTabReplacement) {

      return { outcome: "failed" };
    }

    recoveryInProgress = true;
    totalRecoveryAttempts++;
    lastRecoveryTime = Date.now();
    lastIssueType = issueType;
    lastIssueTime = Date.now();

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.tabReplacement);

    try {

      const result = await onTabReplacement();

      if(result) {

        // Success: update references and reset state.
        currentPage = result.page;
        currentContext = result.context;

        const duration = formatRecoveryDuration(metrics.currentRecoveryStartTime ?? Date.now());

        LOG.info("Recovered in %s via %s.", duration, RECOVERY_METHODS.tabReplacement);

        recordRecoverySuccess(metrics, RECOVERY_METHODS.tabReplacement);

        // Full state reset for fresh tab.
        resetRecoveryCounters();
        resetEscalationState();
        resetSegmentMonitoringState();
        setRecoveryGracePeriod(3);
        resetCircuitBreaker(circuitBreaker);

        return { outcome: "success" };
      }

      // Failure: null result from callback.
      LOG.warn("Tab replacement unsuccessful.");

      return handleTabReplacementFailure("tab replacement unsuccessful");
    } catch(error) {

      // Error: exception during replacement.
      LOG.warn("Tab replacement failed: %s.", formatError(error));

      return handleTabReplacementFailure("tab replacement error");
    } finally {

      finalizeTabReplacement();
    }
  }

  /**
   * Performs page navigation recovery with validation. This is the single recovery function used by both the "video not found" and "escalation level 4" code paths,
   * ensuring consistent behavior. The function:
   * 1. Calls tuneToChannel to reinitialize playback
   * 2. Checks for unexpected new tabs
   * 3. Validates the page URL
   * 4. Validates the video element exists and is accessible
   * 5. Only returns success if all validations pass
   * @returns Recovery result with the new context if successful.
   */
  async function performPageNavigationRecovery(): Promise<PageNavigationRecoveryResult> {

    // Track page count before navigation to detect unexpected new tabs (popups, ads).
    const browser = currentPage.browser();
    const pageCountBefore = (await browser.pages()).length;

    try {

      // Use tuneToChannel to reinitialize playback. This is the single source of truth for channel initialization, ensuring recovery uses the exact same sequence
      // as initial setup (navigation, channel selection, video detection, click-to-play, playback).
      const { context: newContext } = await tuneToChannel(currentPage, url, profile);

      // Check for unexpected new tabs created during tuning.
      const pageCountAfter = (await browser.pages()).length;

      if(pageCountAfter > pageCountBefore) {

        LOG.warn("Detected %s new tab(s) created during navigation.", pageCountAfter - pageCountBefore);
      }

      // Validate that we're on the expected page.
      const currentUrl = currentPage.url();
      const expectedHostname = new URL(url).hostname;

      if(currentUrl.indexOf(expectedHostname) === -1) {

        LOG.warn("Page URL after navigation (%s) does not match expected hostname.", currentUrl);
      }

      // Validate that the video element is accessible and has reasonable state.
      const validationState = await validateVideoElement(newContext, selectorType);

      if(validationState.found) {

        return { newContext, success: true };
      }

      LOG.warn("Page navigation completed but video element not found in new context.");

      return { success: false };
    } catch(error) {

      LOG.warn("Failed to reinitialize video after page navigation: %s.", formatError(error));

      return { success: false };
    }
  }

  /*
   * Main monitoring interval. This runs every MONITOR_INTERVAL milliseconds to check video state and trigger recovery when needed.
   *
   * IMPORTANT: Early returns must call emitStatusUpdate() before returning (except when the stream is terminating, e.g., page closed or circuit breaker tripped). This
   * ensures SSE clients always have current status data (duration, memory, health) even during recovery, buffering, or video search periods. Without this, the
   * streamStatuses map becomes stale and new SSE connections receive outdated snapshots.
   *
   * CHECK ORDER MATTERS: The recoveryInProgress check must come BEFORE the currentPage.isClosed() check. During tab replacement, the old page is intentionally closed
   * while the handler creates a new page. If we check isClosed() first, we would terminate the interval while recovery is still in progress, causing status updates to
   * stop permanently. The sequence is: (1) intervalCleared for explicit cleanup, (2) recoveryInProgress to continue during recovery, (3) isClosed() for unexpected
   * page termination outside of recovery.
   */
  const interval = setInterval((): void => {

    // Stop monitoring if cleanup was requested.
    if(intervalCleared) {

      clearInterval(interval);

      return;
    }

    // Skip health checks if a recovery operation is in progress. During tab replacement, the old page will be closed but we must keep the interval running until the
    // new page is assigned. Emit status so SSE clients see current state (health, duration, memory) even during recovery.
    if(recoveryInProgress) {

      emitStatusUpdate();

      return;
    }

    // Stop monitoring if the page was closed outside of recovery. This handles cases like browser disconnect or explicit stream termination.
    if(currentPage.isClosed()) {

      clearInterval(interval);

      return;
    }

    // Re-establish stream context for this interval tick. AsyncLocalStorage context is lost when entering setInterval callbacks.
    runWithStreamContext(streamContext, async () => {

      try {

        // Early exit if the stream's abort signal has been triggered. This prevents wasted work when the stream is being terminated.
        const abortSignal = getAbortSignal(streamId);

        if(abortSignal?.aborted) {

          clearInterval(interval);

          return;
        }

        // Capture current timestamp for all timing calculations in this check cycle.
        const now = Date.now();

        // Gather current video state for analysis. The getVideoState helper encapsulates video element selection and returns all properties needed for health analysis.
        // We catch frame detachment errors specifically to handle context invalidation differently from normal "video not found" cases.
        let stateInfo = null;
        let contextInvalidated = false;

        try {

          stateInfo = await getVideoState(currentContext, selectorType);
        } catch(stateError) {

          // Check for execution context destroyed errors, which indicate the frame was detached.
          const errorMessage = formatError(stateError);
          const isContextDestroyed = [ "context", "destroyed", "detached", "target closed" ].some((term) => errorMessage.toLowerCase().includes(term));

          if(isContextDestroyed) {

            LOG.warn("Video context was invalidated (frame detached). Will re-search for video.");
            contextInvalidated = true;
          } else {

            // Other errors should be propagated.
            throw stateError;
          }
        }

        // Map to the VideoState type used by the monitor (includes 'time' alias for currentTime).
        const state: Nullable<VideoState> = stateInfo ? { ...stateInfo, time: stateInfo.currentTime } : null;

        // If context was invalidated (frame detached), immediately try to find the video in a new context.
        if(contextInvalidated) {

          LOG.info("Re-searching for video after context invalidation.");

          try {

            const newContext = await findVideoContext(currentPage, profile);
            const validationState = await validateVideoElement(newContext, selectorType);

            if(validationState.found) {

              LOG.info("Video found in new context after detachment. readyState=%s.", validationState.readyState);

              currentContext = newContext;
              videoNotFoundCount = 0;

              // Emit status so SSE clients stay current even when returning early after context re-search.
              emitStatusUpdate();

              return;
            }
          } catch(searchError) {

            LOG.warn("Context re-search after detachment failed: %s.", formatError(searchError));
          }

        // If re-search failed, let the normal "video not found" logic handle it.
        }

        /*
       * If no video element found, apply a grace period before triggering recovery. The video may be temporarily unavailable due to:
       * - readyState fluctuations (selectReadyVideo finds no video with readyState >= 3)
       * - Frame detachment/reattachment during page updates
       * - Momentary DOM changes during ad transitions
       *
       * We distinguish between "no video element exists" and "video exists but not ready". The latter is treated as buffering and given more time.
       */
        if(!state) {

          // Determine context type for diagnostic logging.
          const contextType = currentContext === currentPage ? "main page" : "iframe";
          const frameCount = currentPage.frames().length;

          // Check video presence to distinguish between "no video" and "video exists but not ready".
          let presence: Nullable<Awaited<ReturnType<typeof checkVideoPresence>>> = null;

          try {

            presence = await checkVideoPresence(currentContext, selectorType);
          } catch(_error) {

          // If presence check fails (context destroyed), treat as no video.
          }

          if(presence?.anyVideoExists && !presence.readyVideoFound) {

            // Video element exists but doesn't meet readyState criteria. This is a buffering condition, not a missing video condition.
            // Apply the normal buffering grace period instead of escalating to navigation.
            LOG.info("Video element exists but not ready (count=%s, maxReadyState=%s). Treating as buffering.", presence.videoCount, presence.maxReadyState);

            // Reset video not found counter since video actually exists.
            videoNotFoundCount = 0;

            // Emit status so SSE clients see current state even during this buffering condition.
            emitStatusUpdate();

            // Let the normal buffering detection handle this on subsequent checks.
            return;
          }

          videoNotFoundCount++;

          LOG.warn("Video element not found (attempt %s/3). Context: %s, frames: %s, videoCount: %s.",
            videoNotFoundCount, contextType, frameCount, presence?.videoCount ?? 0);

          // Grace period: Wait for 2 consecutive failures before attempting context re-search, 3 before full navigation.
          if(videoNotFoundCount < 2) {

            // First failure - just log and wait for next check. Emit status so SSE clients stay current.
            emitStatusUpdate();

            return;
          }

          // After 2+ failures, try re-searching frames to see if video moved to a different context.
          if(videoNotFoundCount === 2) {

            LOG.info("Re-searching frames for video element.");

            try {

              const newContext = await findVideoContext(currentPage, profile);
              const validationState = await validateVideoElement(newContext, selectorType);

              if(validationState.found) {

                LOG.info("Video found in different context after re-search. readyState=%s.", validationState.readyState);

                currentContext = newContext;
                videoNotFoundCount = 0;

                // Emit status so SSE clients see current state.
                emitStatusUpdate();

                return;
              }

              LOG.warn("Re-search did not find video in any frame.");
            } catch(error) {

              LOG.warn("Frame re-search failed: %s.", formatError(error));
            }

            // Emit status so SSE clients stay current during video search.
            emitStatusUpdate();

            return;
          }

          // After 3+ consecutive failures, escalate to full page navigation recovery.
          LOG.warn("Video element not found. Attempting %s...", RECOVERY_METHODS.pageNavigation);

          // Check circuit breaker for too many failures.
          const cbResult = checkCircuitBreaker(circuitBreaker, now);

          if(cbResult.shouldTrip) {

            LOG.error("Circuit breaker tripped after %s failures. Stream appears fundamentally broken.", cbResult.totalCount);

            clearInterval(interval);
            onCircuitBreak();

            return;
          }

          // Set escalation to level 3 to trigger page navigation. We skip lower levels since they require a video element.
          // Note: Keep state updates in sync with the main recovery path in the needsRecovery block below.
          escalationLevel = 3;
          lastRecoveryTime = now;
          totalRecoveryAttempts++;
          pendingReMinimize = true;
          recoveryInProgress = true;

          recordRecoveryAttempt(metrics, RECOVERY_METHODS.pageNavigation);

          // Check page reload limit before attempting recovery.
          const reloadWindow = now - CONFIG.playback.pageReloadWindow;

          pageReloadTimestamps = pageReloadTimestamps.filter((ts) => ts > reloadWindow);

          if(pageReloadTimestamps.length >= CONFIG.playback.maxPageReloads) {

            LOG.error("Exceeded maximum page navigations (%s in %s minutes). Cannot recover without video element.",
              CONFIG.playback.maxPageReloads, Math.round(CONFIG.playback.pageReloadWindow / 60000));

            clearInterval(interval);
            onCircuitBreak();

            return;
          }

          pageReloadTimestamps.push(now);

          // Use the unified recovery function with validation.
          const recoveryResult = await performPageNavigationRecovery();

          // Page navigation disrupted the video stream. Mark a discontinuity regardless of navigation success so HLS clients resynchronize their decoders.
          markStreamDiscontinuity();

          // Set grace period to give page navigation time to take effect (L3 = 10 seconds).
          recoveryGraceUntil = now + recoveryGracePeriods[3];

          if(recoveryResult.success && recoveryResult.newContext) {

            // Update the context reference for subsequent monitor checks (only after validation succeeds).
            currentContext = recoveryResult.newContext;

            // Log success with timing.
            const duration = formatRecoveryDuration(metrics.currentRecoveryStartTime ?? now);

            LOG.info("Recovered in %s via %s.", duration, RECOVERY_METHODS.pageNavigation);

            recordRecoverySuccess(metrics, RECOVERY_METHODS.pageNavigation);

            // Reset state after successful page navigation recovery.
            resetRecoveryCounters();
            resetEscalationState();
            resetSegmentMonitoringState();
          } else {

            consecutiveNavigationFailures++;

            LOG.warn("Page navigation unsuccessful.");
          }

          recoveryInProgress = false;

          // Emit status so SSE clients see the recovery result.
          emitStatusUpdate();

          return;
        }

        // Video was found - reset the not found counter, timeout counter, and save state for status reporting.
        videoNotFoundCount = 0;
        consecutiveTimeouts = 0;
        lastVideoState = state;

        /*
       * Volume enforcement. Some sites aggressively mute videos (e.g., France24 mutes on page visibility change, some sites mute for ads). We restore volume on
       * every check to ensure audio is captured.
       */
        if(state.muted || (state.volume < 1)) {

          await enforceVideoVolume(currentContext, selectorType);
        }

        /*
       * Stall detection. We compare currentTime to the previous check to determine if the video is progressing. The STALL_THRESHOLD (0.1 seconds) allows for minor
       * timing variations while still detecting genuinely stalled videos.
       */

        // Video is progressing if: this is the first check (no previous time), OR currentTime has advanced by at least STALL_THRESHOLD since last check.
        const isProgressing = (lastTime === null) || (Math.abs(state.time - lastTime) >= CONFIG.playback.stallThreshold);

        /*
       * Buffering detection. True buffering occurs when the player needs more data (readyState < 3) AND is actively fetching it (networkState === 2). We use AND
       * rather than OR because networkState === 2 is normal for live streams - data continuously arrives. Only when combined with insufficient data does it indicate
       * actual buffering.
       */
        const isBuffering = (state.readyState < 3) && (state.networkState === 2);

        /*
       * Buffering grace period tracking. When buffering starts, we record the timestamp. We only trigger recovery if buffering exceeds BUFFERING_GRACE_PERIOD. This
       * allows normal network buffering to resolve without intervention.
       */
        if(isBuffering && !bufferingStartTime) {

          bufferingStartTime = now;
        } else if(!isBuffering) {

          bufferingStartTime = null;
        }

        // Check if we're within the buffering grace period (recently started buffering and haven't exceeded the threshold).
        const withinBufferingGrace = isBuffering && bufferingStartTime && ((now - bufferingStartTime) < CONFIG.playback.bufferingGracePeriod);

        // Check if we're within the recovery grace period (recently performed a recovery action and waiting for it to take effect).
        const withinRecoveryGrace = now < recoveryGraceUntil;

        /*
         * Segment production monitoring. After L2/L3 recovery completes (grace period ends), we verify that segments are actually being produced. If recovery reported
         * success but the capture pipeline is dead (MediaRecorder stopped producing data, FFmpeg stdin idle), segments will stop flowing while the video element
         * appears healthy. This catches the 20+ minute freeze bug where PrismCast reports "Recovered" but Channels DVR receives no data.
         */
        if((preRecoverySegmentIndex !== null) && !withinRecoveryGrace) {

          // Start the segment wait timer when recovery grace period ends.
          segmentWaitStartTime ??= now;

          // Check if segments are flowing by comparing current index to pre-recovery index.
          const entry = getStream(streamInfo.numericStreamId);
          const currentSegmentIndex = entry?.segmenter?.getSegmentIndex() ?? null;

          if((currentSegmentIndex !== null) && (currentSegmentIndex > preRecoverySegmentIndex)) {

            // Segments are flowing - recovery actually succeeded. Clear tracking state.
            preRecoverySegmentIndex = null;
            segmentWaitStartTime = null;
            segmentProductionStalled = false;
          } else if((now - segmentWaitStartTime) > SEGMENT_STALL_TIMEOUT) {

            // No new segments for SEGMENT_STALL_TIMEOUT after recovery grace period. The capture pipeline is dead.
            LOG.warn("No segments produced for %ss after recovery. Capture pipeline appears dead.", SEGMENT_STALL_TIMEOUT / 1000);

            segmentProductionStalled = true;
          }
        }

        /*
         * Continuous segment size monitoring. Runs on every healthy interval to detect spontaneous capture pipeline death (no preceding recovery event). Dead pipelines
         * produce tiny segments (18 bytes observed) while the video element appears healthy. This catches failures that post-recovery index monitoring misses because
         * there's no recovery to trigger monitoring.
         */
        const sizeCheckEntry = getStream(streamInfo.numericStreamId);
        const currentSegmentIndex = sizeCheckEntry?.segmenter?.getSegmentIndex() ?? 0;

        if((currentSegmentIndex > lastCheckedSegmentIndex) && sizeCheckEntry) {

          // A new segment was produced. Check its size.
          const segmentSize = getLastSegmentSize(sizeCheckEntry) ?? 0;

          if(segmentSize < TINY_SEGMENT_THRESHOLD) {

            consecutiveTinySegments++;
            wasInTinySegmentState = true;

            if(consecutiveTinySegments >= TINY_SEGMENT_COUNT_TRIGGER) {

              LOG.warn("Detected %d consecutive tiny segments (%d bytes). Capture pipeline appears dead.", consecutiveTinySegments, segmentSize);

              // Trigger tab replacement if available, otherwise let circuit breaker handle it via segmentProductionStalled.
              if(onTabReplacement && !recoveryInProgress) {

                const tabResult = await executeTabReplacement("tiny segments");

                if(tabResult.outcome === "terminated") {

                  return;
                }
              } else if(!onTabReplacement) {

                // No tab replacement callback - set stalled flag for circuit breaker.
                segmentProductionStalled = true;
              }
            }
          } else {

            // Valid segment size. Check for spontaneous recovery from tiny segment state. We don't mark discontinuity here - only tab replacement marks discontinuity.
            // Self-healing may be transient and not require decoder reset.
            if(wasInTinySegmentState) {

              LOG.debug("Segment production self-healed (%d bytes).", segmentSize);
            }

            // Reset tiny segment tracking.
            consecutiveTinySegments = 0;
            wasInTinySegmentState = false;
          }

          lastCheckedSegmentIndex = currentSegmentIndex;
        }

        /*
       * Re-minimize check. After recovery, the browser window may have been un-minimized by fullscreen actions. As soon as the stream is healthy (progressing without
       * issues), we re-minimize to reduce GPU usage.
       */
        if(pendingReMinimize && isProgressing && !state.paused && !state.error && !state.ended) {

          LOG.debug("Re-minimizing browser window after successful recovery.");

          pendingReMinimize = false;

          await resizeAndMinimizeWindow(currentPage, true);
        }

        /*
       * Stall counter management. We increment stallCount when the video is not progressing and not within buffering grace. We reset to 0 when progression resumes.
       * This hysteresis prevents reacting to single-frame hiccups.
       */
        if(!isProgressing && !withinBufferingGrace) {

          stallCount++;
        } else if(isProgressing) {

          stallCount = 0;
        }

        /*
       * Recovery decision. We trigger recovery when any of these conditions are met AND we're not within the recovery grace period:
       * - Video has an error state
       * - Video ended (live streams shouldn't end)
       * - Video is paused and not just buffering
       * - Video is stalled for too long (stallCount exceeds threshold and not in buffering grace)
       * - Segment production has stalled after recovery (capture pipeline dead)
       */
        const needsRecovery = !withinRecoveryGrace && (state.error || state.ended || (state.paused && !withinBufferingGrace) ||
                            (!isProgressing && !withinBufferingGrace && (stallCount > CONFIG.playback.stallCountThreshold)) ||
                            segmentProductionStalled);

        /*
       * Escalation reset. After sustained healthy playback (SUSTAINED_PLAYBACK_REQUIRED, default 60 seconds), we reset the escalation level and circuit breaker.
       * This allows a stream that recovered to start fresh, rather than immediately escalating to aggressive recovery on the next issue.
       */
        if(isProgressing && !state.paused && !state.ended && !state.error) {

          // If a recovery was pending confirmation (L1/L2), log success now that we have healthy playback.
          if((metrics.currentRecoveryStartTime !== null) && (metrics.currentRecoveryMethod !== null)) {

            const duration = formatRecoveryDuration(metrics.currentRecoveryStartTime);

            LOG.info("Recovered in %s via %s.", duration, metrics.currentRecoveryMethod);

            recordRecoverySuccess(metrics, metrics.currentRecoveryMethod);
          }

          const healthyDuration = now - lastRecoveryTime;

          if((escalationLevel > 0) && (healthyDuration > CONFIG.playback.sustainedPlaybackRequired)) {

            // Clear buffering state. The bufferingStartTime may persist through recovery cycles due to networkState === 2 (NETWORK_LOADING) being true for live streams
            // even during healthy playback. Since we have confirmed 60 seconds of progression, the stream is definitively not buffering.
            bufferingStartTime = null;

            // Reset escalation, segment tracking, and circuit breaker. Sustained healthy playback confirms the stream works.
            resetEscalationState();
            resetSegmentMonitoringState();
            resetCircuitBreaker(circuitBreaker);
          }
        }

        /*
       * Recovery execution. When recovery is needed, we update circuit breaker state, determine the appropriate recovery level based on issue type and history, and
       * execute the recovery action. The recovery system is issue-aware:
       * - Paused issues try L1 (play/unmute) first since it works ~50% of the time for paused state
       * - Buffering issues skip L1 and go directly to L2 (source reload) since L1 never helps buffering
       * - If L2 has already been attempted, skip to L3 (page reload) since a second L2 always fails
       */
        if(needsRecovery) {

          /*
           * Segment production stall handling. When we detect that segments stopped flowing after L2/L3 recovery, the capture pipeline is dead and normal recovery
           * won't help. We skip the escalation ladder and go directly to tab replacement if available.
           */
          if(segmentProductionStalled && onTabReplacement) {

            LOG.warn("Capture pipeline stalled after recovery. Escalating directly to %s...", RECOVERY_METHODS.tabReplacement);

            await executeTabReplacement("capture pipeline stalled");

            return;
          }

          // Check circuit breaker for too many failures. The helper handles incrementing, window checks, and resetting if outside the window.
          const cbResult = checkCircuitBreaker(circuitBreaker, now);

          if(cbResult.shouldTrip) {

            const elapsedSeconds = circuitBreaker.firstFailureTime ? Math.round((now - circuitBreaker.firstFailureTime) / 1000) : 0;

            LOG.error("Circuit breaker tripped after %s failures in %ss. Stream appears fundamentally broken.", cbResult.totalCount, elapsedSeconds);

            clearInterval(interval);
            onCircuitBreak();

            return;
          }

          /*
         * Issue-aware escalation. Instead of blindly incrementing the level, we determine the appropriate level based on:
         * 1. The type of issue (paused vs buffering vs other)
         * 2. Whether source reload (L2) has already been attempted in this page session
         * 3. The current escalation level
         *
         * Levels:
         * - Level 1: Basic play/unmute - only for paused issues
         * - Level 2: Reload video source - for buffering/other issues, or when L1 fails
         * - Level 3: Page navigation - when L2 fails or has already been attempted
         */
          const issueCategory = getIssueCategory(state, !isProgressing, isBuffering);
          let nextLevel: number;

          if((issueCategory === "paused") && (escalationLevel === 0)) {

            // Paused issues: try L1 first (play/unmute works ~50% for paused).
            nextLevel = 1;
          } else if(!sourceReloadAttempted) {

            // First recovery attempt for buffering/other, or L1 didn't fix paused: try L2 (source reload).
            nextLevel = 2;
          } else {

            // Source reload already attempted: go to L3 (page reload).
            nextLevel = 3;
          }

          // Note: Keep state updates in sync with the video-not-found recovery path above.
          escalationLevel = nextLevel;
          lastRecoveryTime = now;
          totalRecoveryAttempts++;
          pendingReMinimize = true;

          // Get recovery method name for logging and metrics.
          const recoveryMethod = getRecoveryMethod(escalationLevel);

          // Store issue type and time for UI display.
          const issueType = formatIssueType(state, !isProgressing, isBuffering);

          lastIssueType = issueType;
          lastIssueTime = now;

          // If a previous recovery was pending (L1 or L2 that didn't result in healthy playback), log that it was unsuccessful before starting the new attempt.
          if(metrics.currentRecoveryMethod !== null) {

            LOG.warn("%s unsuccessful. Attempting %s...", capitalize(metrics.currentRecoveryMethod), recoveryMethod);
          } else {

            // First recovery attempt - log with issue description.
            const issueDesc = getIssueDescription(issueCategory);

            LOG.warn("Playback %s. Attempting %s...", issueDesc, recoveryMethod);
          }

          // Record this recovery attempt in metrics.
          recordRecoveryAttempt(metrics, recoveryMethod);

          // For L2/L3 recovery, record the current segment index so we can verify segments are flowing after recovery completes.
          if(escalationLevel >= 2) {

            const entry = getStream(streamInfo.numericStreamId);

            preRecoverySegmentIndex = entry?.segmenter?.getSegmentIndex() ?? null;
            segmentWaitStartTime = null;  // Will be set after recovery grace period ends.
            segmentProductionStalled = false;
          }

          // Mark recovery in progress to prevent overlapping recovery attempts.
          recoveryInProgress = true;

          try {

            /*
           * Levels 1-2: In-page recovery. These levels are handled by ensurePlayback() which performs recovery actions without navigating the page.
           */
            if(escalationLevel <= 2) {

              await ensurePlayback(currentPage, currentContext, profile, escalationLevel);

              // Track that source reload was attempted so we skip directly to L3 next time.
              if(escalationLevel === 2) {

                sourceReloadAttempted = true;

                // The source reload disrupted the video stream. Mark a discontinuity so HLS clients resynchronize their decoders.
                markStreamDiscontinuity();
              }

              // Set grace period to give this recovery level time to take effect before the next check.
              recoveryGraceUntil = now + recoveryGracePeriods[escalationLevel];
            } else {

              /*
             * Level 3: Page navigation recovery. This is the most aggressive recovery - we navigate to the URL again and reinitialize everything.
             */

              // Safety check: If page navigation has failed twice consecutively, fall back to source reload. This prevents getting stuck in a loop when navigation
              // itself is broken (e.g., network issues, site blocking).
              if(consecutiveNavigationFailures >= 2) {

                LOG.error("Page navigation has failed %s consecutive times. Falling back to source reload recovery.",
                  consecutiveNavigationFailures);

                escalationLevel = 2;
                consecutiveNavigationFailures = 0;

                // Reset source reload tracking so the fallback L2 gets a fair chance. Without this, the next recovery cycle would skip L2 and try L3 again.
                sourceReloadAttempted = false;
              } else {

                // Check page reload limit to prevent excessive navigations. We allow MAX_PAGE_RELOADS within PAGE_RELOAD_WINDOW.
                const reloadWindow = now - CONFIG.playback.pageReloadWindow;

                // Prune old timestamps outside the window.
                pageReloadTimestamps = pageReloadTimestamps.filter((ts) => {

                  return ts > reloadWindow;
                });

                if(pageReloadTimestamps.length >= CONFIG.playback.maxPageReloads) {

                  LOG.error("Exceeded maximum page navigations (%s in %s minutes). Falling back to source reload.",
                    CONFIG.playback.maxPageReloads, Math.round(CONFIG.playback.pageReloadWindow / 60000));

                  escalationLevel = 2;

                  // Reset source reload tracking so the fallback L2 gets a fair chance.
                  sourceReloadAttempted = false;
                } else {

                  pageReloadTimestamps.push(now);

                  // Use the unified recovery function with validation.
                  const recoveryResult = await performPageNavigationRecovery();

                  // Page navigation disrupted the video stream. Mark a discontinuity regardless of navigation success so HLS clients resynchronize their decoders.
                  markStreamDiscontinuity();

                  // Set grace period to give page navigation time to take effect (L3 = 10 seconds).
                  recoveryGraceUntil = now + recoveryGracePeriods[3];

                  if(recoveryResult.success && recoveryResult.newContext) {

                    // Update the context reference to the new context (only after validation succeeds).
                    currentContext = recoveryResult.newContext;

                    // Log success with timing.
                    const duration = formatRecoveryDuration(metrics.currentRecoveryStartTime ?? now);

                    LOG.info("Recovered in %s via %s.", duration, RECOVERY_METHODS.pageNavigation);

                    recordRecoverySuccess(metrics, RECOVERY_METHODS.pageNavigation);

                    // Reset state after successful page navigation recovery.
                    resetRecoveryCounters();
                    resetEscalationState();
                    resetSegmentMonitoringState();
                  } else {

                    consecutiveNavigationFailures++;

                    LOG.warn("Page navigation unsuccessful (attempt %s/2).", consecutiveNavigationFailures);
                  }
                }
              }
            }
          } catch(error) {

            LOG.warn("Recovery via %s failed: %s.", getRecoveryMethod(escalationLevel), formatError(error));
          }

          recoveryInProgress = false;
        }

        // Update lastTime for the next stall check.
        lastTime = state.time;

        // Emit status update for SSE subscribers.
        emitStatusUpdate();
      } catch(error) {

        recoveryInProgress = false;

        // If the session or page was closed, stop monitoring gracefully.
        if(isSessionClosedError(error) || currentPage.isClosed()) {

          clearInterval(interval);

          return;
        }

        // Check for evaluate timeout errors, which indicate the browser tab may be unresponsive.
        if(error instanceof EvaluateTimeoutError) {

          consecutiveTimeouts++;

          LOG.warn("Monitor check timed out (%s consecutive). Tab may be unresponsive.", consecutiveTimeouts);

          // Update issue state so SSE clients can show the degraded state.
          lastIssueType = "tab timing out";
          lastIssueTime = Date.now();

          // After 3 consecutive timeouts, attempt tab replacement if the callback is available.
          if((consecutiveTimeouts >= 3) && onTabReplacement) {

            LOG.warn("Tab unresponsive. Attempting %s...", RECOVERY_METHODS.tabReplacement);

            await executeTabReplacement("tab unresponsive");

            return;
          }

          // Emit status so SSE clients see current duration/memory even during timeout degradation (when consecutiveTimeouts < 3).
          emitStatusUpdate();

          return;
        }

        // Log abort errors at debug level since they're expected during stream termination. Log other errors at error level.
        const errorMessage = formatError(error);

        if(errorMessage.includes("aborted")) {

          LOG.debug("Monitor check aborted: %s.", errorMessage);
        } else {

          LOG.error("Monitor check failed: %s.", errorMessage);
        }

        // Emit status for non-abort errors so SSE clients stay current. Abort errors don't need this because termination is already in progress and the next
        // tick's abort check will clean up.
        if(!errorMessage.includes("aborted")) {

          emitStatusUpdate();
        }
      }
    }).catch((outerError) => {

      // Log errors that escape the inner try/catch. In normal operation we should not reach here - if we do, there's a bug to investigate.
      LOG.warn("Monitor tick error escaped inner try/catch: %s.", formatError(outerError));
    });
  }, CONFIG.playback.monitorInterval);

  /*
   * Return the cleanup function. The caller (stream handler) should call this when the stream ends to stop monitoring. Returns the recovery metrics for the
   * termination summary log.
   */
  return function(): RecoveryMetrics {

    intervalCleared = true;

    clearInterval(interval);

    return metrics;
  };
}
