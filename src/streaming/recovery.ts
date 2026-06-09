/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * recovery.ts: Recovery types, constants, metrics tracking, circuit breaker, and issue classification for PrismCast.
 */
import type { Frame, Page } from "puppeteer-core";
import type { Nullable, VideoState } from "../types/index.ts";
import { CONFIG } from "../config/index.ts";

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

/**
 * Handle returned by monitorPlaybackHealth. The playback monitor is a self-contained resource node: it owns a polling interval and exposes its accumulated recovery
 * metrics for reading, separately from stopping. The metrics counters are live throughout the monitor's lifetime and remain valid after disposal (the interval is
 * cleared but the counter object is retained), so the termination prologue reads getMetrics() while the stream is still live, then dispose() stops the interval -
 * the same read-then-dispose shape as the capture session's segmenter stats and the native proxy's getStats(). Implements both dispose() (project convention) and
 * TC39 Symbol.dispose so the monitor composes uniformly with the other capture-mode resources.
 */
export interface MonitorHandle extends Disposable {

  // Stops the health-monitor interval. Idempotent: a second call is a harmless clearInterval on an already-cleared handle, and the monitor's internal guard
  // short-circuits any in-flight async tick. Aliased to [Symbol.dispose].
  readonly dispose: () => void;

  // Returns the live recovery metrics accumulated over the monitor's lifetime. Safe to read at any time, including after disposal. Read in the termination prologue
  // for the stream-end summary log.
  readonly getMetrics: () => RecoveryMetrics;

  // TC39 explicit resource management hook. Aliases dispose() so the monitor can be consumed with "using" or composed alongside the other self-disposing nodes.
  readonly [Symbol.dispose]: () => void;
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

      parts.push(String(count) + "x " + methodName);
    }
  }

  // Calculate average recovery time.
  const avgTimeMs = totalSuccesses > 0 ? metrics.totalRecoveryTimeMs / totalSuccesses : 0;
  const avgTimeStr = (avgTimeMs / 1000).toFixed(1) + "s";

  // Format: "Recoveries: 8 (5x source reload, 3x page navigation), avg 4.2s."
  if(parts.length > 0) {

    return "Recoveries: " + String(totalSuccesses) + " (" + parts.join(", ") + "), avg " + avgTimeStr + ".";
  }

  // Edge case: attempts but no successes (stream terminated before recovery completed).
  return "Recoveries: " + String(totalAttempts) + " attempted, 0 succeeded.";
}

/**
 * State for failure accrual within a sliding time window. This is the single failure-counting primitive in PrismCast: the per-stream circuit breaker below and the
 * browser supervisor (browser/index.ts) both judge "have there been too many failures recently?" through it, differing only in the policy bounds they pass and what
 * they do when the window trips. The state is a plain record the caller owns and persists across calls; the bounds are supplied per call so one primitive serves
 * consumers with different tolerances without hard-coding any single config value.
 */
export interface FailureWindowState {

  // Timestamp of the first failure in the current window, or null when no failure has been recorded since the last reset. Anchors the window.
  firstFailureTime: Nullable<number>;

  // Number of failures recorded within the current window.
  totalFailureCount: number;
}

/**
 * Policy bounds for a failure window: how long the window lasts and how many failures within it constitute a trip.
 */
export interface FailureWindowOptions {

  // Number of failures within windowMs that constitutes a trip.
  readonly threshold: number;

  // Sliding window duration in milliseconds.
  readonly windowMs: number;
}

/**
 * Result of recording one failure against a window.
 */
export interface FailureWindowResult {

  // Total failures now recorded in the current window.
  readonly totalCount: number;

  // Whether the window tripped - the threshold was reached within the window. What a consumer does on a trip is its own concern: the stream breaker terminates the
  // stream; the browser supervisor opens its breaker.
  readonly tripped: boolean;

  // Whether this failure fell within the window anchored by the first failure. False means the window had lapsed and was restarted from this failure.
  readonly withinWindow: boolean;
}

/**
 * Records a failure against a sliding window and reports whether the window has tripped. Pure over (state, now, options): it mutates the supplied state in place and
 * returns the decision plus diagnostics. When a failure arrives after the window has lapsed, the window restarts from it (count resets to 1) so accrual reflects
 * only the recent window. This is the single source of truth for failure accrual; consumers differ only in the bounds they pass and what a trip means to them.
 * @param state - The failure-window state to update.
 * @param now - The current timestamp in milliseconds.
 * @param options - The window duration and trip threshold.
 * @returns The trip decision and diagnostics.
 */
export function recordFailure(state: FailureWindowState, now: number, options: FailureWindowOptions): FailureWindowResult {

  state.totalFailureCount++;
  state.firstFailureTime ??= now;

  const withinWindow = (now - state.firstFailureTime) < options.windowMs;
  const tripped = withinWindow && (state.totalFailureCount >= options.threshold);

  // When the window has lapsed, restart it from this failure so accrual reflects only the recent window.
  if(!withinWindow) {

    state.totalFailureCount = 1;
    state.firstFailureTime = now;
  }

  return { totalCount: state.totalFailureCount, tripped, withinWindow };
}

/**
 * Resets a failure window to its empty state. Called when sustained health is achieved (the stream recovered; the browser is durably ready).
 * @param state - The failure-window state to reset.
 */
export function resetFailureWindow(state: FailureWindowState): void {

  state.firstFailureTime = null;
  state.totalFailureCount = 0;
}

/**
 * The per-stream circuit breaker's state is a failure window. The alias keeps the breaker's vocabulary at its call sites while there is exactly one underlying state
 * type and one accrual implementation.
 */
export type CircuitBreakerState = FailureWindowState;

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
 * Records a failure and checks whether the per-stream circuit breaker should trip. This is the single entry point for circuit breaker decisions across every
 * recovery path - any code that wants to count a stream failure goes through this function. It is the stream-scoped policy binding of recordFailure: the window and
 * threshold come from CONFIG.recovery, and a trip means "terminate the stream."
 * @param state - The circuit breaker state to update.
 * @param now - The current timestamp.
 * @returns Result indicating whether the circuit breaker should trip and diagnostic info.
 */
export function checkCircuitBreaker(state: CircuitBreakerState, now: number): CircuitBreakerResult {

  const result = recordFailure(state, now, { threshold: CONFIG.recovery.circuitBreakerThreshold, windowMs: CONFIG.recovery.circuitBreakerWindow });

  return { shouldTrip: result.tripped, totalCount: result.totalCount, withinWindow: result.withinWindow };
}

/**
 * Resets the circuit breaker state. Called when sustained healthy playback is achieved.
 * @param state - The circuit breaker state to reset.
 */
export function resetCircuitBreaker(state: CircuitBreakerState): void {

  resetFailureWindow(state);
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

/* Capture-infrastructure error signatures. These indicate a fault in Chrome's capture pipeline itself - the puppeteer-stream tabCapture extension, the serialized
 * capture queue, or stream initialization - rather than a site- or stream-specific problem. They are the faults that warrant backing a client off (HTTP 503) and,
 * for the browser supervisor, treating a setup failure as evidence the browser itself may no longer be capture-ready.
 */
const CAPTURE_INFRASTRUCTURE_PATTERNS = [ "Cannot capture", "Capture queue", "timed out" ] as const;

/**
 * Classifies whether an error originates in Chrome's capture infrastructure (the extension, the capture queue, or stream initialization) rather than in a specific
 * site or stream. This is the single source of truth for that judgment: the stream-setup path uses it to decide a 503 back-off, and the browser supervisor uses it
 * to decide whether a setup failure is evidence the browser may no longer be capture-ready. It is layered with, not exclusive of, the narrower "Cannot capture a tab
 * with an active stream" stale-mutex predicate handled at its own call sites: a stale-mutex error is also a capture-infrastructure error (so this matches it too),
 * but the process-exit decision that the stale-mutex case alone warrants stays a distinct check.
 * @param error - The error or message to classify.
 * @returns True when the message carries a capture-infrastructure signature.
 */
export function isCaptureInfrastructureError(error: unknown): boolean {

  const message = (error instanceof Error) ? error.message : String(error);

  return CAPTURE_INFRASTRUCTURE_PATTERNS.some((pattern) => message.includes(pattern));
}
