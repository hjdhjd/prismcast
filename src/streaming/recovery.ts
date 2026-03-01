/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * recovery.ts: Recovery types, constants, metrics tracking, circuit breaker, and issue classification for PrismCast.
 */
import type { Frame, Page } from "puppeteer-core";
import type { Nullable, VideoState } from "../types/index.js";
import { CONFIG } from "../config/index.js";

/* Recovery metrics are tracked throughout each stream's lifetime. The playback health monitor accumulates these counters during recovery attempts, and the
 * termination handler includes them in the stream-end log for analytics and troubleshooting.
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
export const RECOVERY_METHODS = {

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
export function createRecoveryMetrics(): RecoveryMetrics {

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
export function formatRecoveryDuration(startTime: number): string {

  const durationMs = Date.now() - startTime;

  return (durationMs / 1000).toFixed(1) + "s";
}

/**
 * Maps issue category to user-friendly description for logging.
 * @param category - The issue category from getIssueCategory().
 * @returns User-friendly description.
 */
export function getIssueDescription(category: "paused" | "buffering" | "other"): string {

  switch(category) {

    case "paused": {

      return "paused";
    }

    case "buffering": {

      return "buffering";
    }

    default: {

      return "stalled";
    }
  }
}

/**
 * Maps recovery level to method name.
 * @param level - The recovery level (1, 2, or 3).
 * @returns The recovery method name.
 */
export function getRecoveryMethod(level: number): string {

  switch(level) {

    case 1: {

      return RECOVERY_METHODS.playUnmute;
    }

    case 2: {

      return RECOVERY_METHODS.sourceReload;
    }

    default: {

      return RECOVERY_METHODS.pageNavigation;
    }
  }
}

/**
 * Records a recovery attempt in the metrics. Uses the ATTEMPT_FIELDS mapping to find the correct counter field, eliminating the need for if/else chains. This
 * makes adding new recovery methods trivial - just add an entry to ATTEMPT_FIELDS.
 *
 * Note: Tab replacement calls this once per logical attempt even though it may internally retry the onTabReplacement callback. The retry is an implementation
 * detail of executeTabReplacement, not a separate recovery attempt from the monitor's perspective. The circuit breaker likewise records one failure per logical
 * attempt, not per callback invocation.
 * @param metrics - The metrics object to update.
 * @param method - The recovery method being attempted.
 */
export function recordRecoveryAttempt(metrics: RecoveryMetrics, method: string): void {

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
export function recordRecoverySuccess(metrics: RecoveryMetrics, method: string): void {

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
export function capitalize(str: string): string {

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
export interface CircuitBreakerState {

  // Timestamp of the first failure in the current window. Used to determine if failures are within the window.
  firstFailureTime: Nullable<number>;

  // Total number of failures within the circuit breaker window.
  totalFailureCount: number;
}

/**
 * Result from checking circuit breaker state.
 */
export interface CircuitBreakerResult {

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
export function checkCircuitBreaker(state: CircuitBreakerState, now: number): CircuitBreakerResult {

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
export function resetCircuitBreaker(state: CircuitBreakerState): void {

  state.firstFailureTime = null;
  state.totalFailureCount = 0;
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
export function formatIssueType(state: VideoState, isStalled: boolean, isBuffering: boolean): string {

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
export function getIssueCategory(state: VideoState, isStalled: boolean, isBuffering: boolean): "paused" | "buffering" | "other" {

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
