/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * recovery.ts: Recovery types, constants, metrics tracking, circuit breaker, and issue classification for PrismCast.
 */
import type { Frame, Page } from "puppeteer-core";
import type { Nullable, VideoState } from "../types/index.ts";
import { CAPTURE_SOURCE_UNAVAILABLE_MESSAGE } from "../types/index.ts";
import { CONFIG } from "../config/index.ts";
import type { StreamHealthStatus } from "./statusEmitter.ts";

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

  // Stops the health-monitor interval. Safe to call more than once: a second call is a harmless clearInterval on an already-cleared handle, and the monitor's
  // internal guard short-circuits any in-flight async tick. Aliased to [Symbol.dispose].
  readonly dispose: () => void;

  // Returns the live recovery metrics accumulated over the monitor's lifetime. Safe to read at any time, including after disposal. Read in the termination prologue
  // for the stream-end summary log.
  readonly getMetrics: () => RecoveryMetrics;

  // TC39 explicit resource management hook. Aliases dispose() so the monitor can be consumed with "using" or composed alongside the other self-disposing nodes.
  readonly [Symbol.dispose]: () => void;
}

// Recovery method names. These are the single source of truth for recovery methods: they serve as the log labels in start, success, and failure messages, as the
// computed keys into the ATTEMPT_FIELDS and SUCCESS_FIELDS metrics-counter maps below, and as the return values of getRecoveryMethod. Changing a value here changes
// all three in lockstep, so they are not free to edit for log cosmetics - editing one silently re-routes (or breaks) the corresponding metrics counter.
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
 * browser relaunch governor (browser/launchGovernor.ts) both judge "have there been too many failures recently?" through it, differing only in the policy bounds they
 * pass and what they do when the window trips. The state is a plain record the caller owns and persists across calls; the bounds are supplied per call so one primitive
 * serves consumers with different tolerances without hard-coding any single config value.
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
 * Records a failure against a sliding window and reports whether the window has tripped. Deterministic over (state, now, options): given the same inputs it always
 * mutates the supplied state the same way and returns the same decision plus diagnostics, with no reliance on the wall clock or any module-level state. When a
 * failure arrives after the window has lapsed, the window restarts from it (count resets to 1) so accrual reflects only the recent window. This is the single
 * source of truth for failure accrual; consumers differ only in the bounds they pass and what a trip means to them.
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

/* Capture-infrastructure error signatures. These indicate a fault in Chrome's capture pipeline itself - the tabCapture extension, the capture lock's turn wait,
 * or stream initialization - rather than a site- or stream-specific problem. The acquisition's own terminal messages sit here alongside the collision and the
 * timeouts, because a capture Chrome refuses on both attempts, a browser with no loaded extension, and a browser reporting no active tab are all faults of the
 * capture system rather than of the site being tuned. They are the faults that warrant backing a client off (HTTP 503) and,
 * for the browser supervisor, treating a setup failure as evidence the browser itself may no longer be capture-ready.
 *
 * The "timed out" entry is deliberately a broad substring rather than one literal string per message: it covers every timeout raised within
 * createPageWithCapture's own pipeline - the capture lock's turn wait (whose message keeps the "Capture queue" wording, matched by that pattern too), stream
 * initialization, the playback-initialization safety net, and the capability probe - without hard-coding each message here. Every reader of this list sits on that
 * pipeline's failure paths, so the substring only ever sees errors surfaced from it. It stays safe there because navigateToPage and reloadPage swallow Puppeteer's
 * own navigation timeouts as warnings instead of throwing, and the evaluate-call timeout path (EvaluateTimeoutError) belongs to the health monitor's own call chain,
 * never this one.
 *
 * The refusal Chrome answers a capture start with is referenced from the module that speaks that protocol rather than re-typed here, so a wording change in the
 * extension's answer cannot leave the two spellings disagreeing.
 */
const CAPTURE_INFRASTRUCTURE_PATTERNS = [ "Cannot capture", "Capture queue", CAPTURE_SOURCE_UNAVAILABLE_MESSAGE, "No active tab", "capture extension",
  "timed out" ] as const;

/**
 * Classifies whether an error originates in Chrome's capture infrastructure (the extension, the capture lock, or stream initialization) rather than in a specific
 * site or stream. This is the single source of truth for that judgment, and it has two readers, both on the establishment's failure paths: the acquisition
 * chokepoint uses it to decide whether a failure is evidence the browser may no longer be capture-ready, and the stream-setup path uses it to decide the
 * client-facing 503 back-off. The judgment is shared; the side effect fires in exactly one of them.
 * @param error - The error or message to classify.
 * @returns True when the message carries a capture-infrastructure signature.
 */
export function isCaptureInfrastructureError(error: unknown): boolean {

  const message = (error instanceof Error) ? error.message : String(error);

  return CAPTURE_INFRASTRUCTURE_PATTERNS.some((pattern) => message.includes(pattern));
}

// Recovery Decisions.

/**
 * Derives the reported health status for a capture stream from its recovery and playback state. This is the single source of truth for the health precedence
 * ladder the monitor emits over SSE: an error state - or a page-reload-level escalation (>= 3), which is equally severe - reports "error"; an active escalation
 * (levels 1-2) reports "recovering"; buffering within the grace window reports "buffering"; consecutive stalls that have not yet crossed the recovery trigger
 * report "stalled"; otherwise "healthy". Deterministic and side-effect free over its inputs, so the precedence lives in exactly one place and is directly testable.
 * @param inputs - The recovery escalation level, error flag, buffering flag, and consecutive stall count read from the monitor's state.
 * @returns The stream health status to report.
 */
export function deriveStreamHealth(inputs: { escalationLevel: number; hasError: boolean; isBuffering: boolean; stallCount: number }): StreamHealthStatus {

  // An error state and a page-reload-level escalation (>= 3) are equally severe - both report as an error.
  if(inputs.hasError || (inputs.escalationLevel >= 3)) {

    return "error";
  }

  // Escalation levels 1-2 mean recovery is actively in progress.
  if(inputs.escalationLevel > 0) {

    return "recovering";
  }

  // Buffering within the grace period.
  if(inputs.isBuffering) {

    return "buffering";
  }

  // Consecutive stalls that have not yet crossed the recovery trigger.
  if(inputs.stallCount > 0) {

    return "stalled";
  }

  return "healthy";
}

/**
 * Decides whether the monitor should trigger a recovery action this tick. This is the single source of truth for the recovery-trigger condition: recovery never
 * fires inside the post-recovery grace window; otherwise it fires on an error, an ended stream, a persistent pause (paused past the stall-count hysteresis while
 * outside the buffering grace window), a persistent stall (not progressing past the same hysteresis while outside the buffering grace window), or a stalled
 * capture pipeline. The buffering grace window filters transient rebuffer pauses so they do not escalate. Deterministic and side-effect free over its inputs.
 * @param inputs - The grace-window flags, playback state flags, pause/stall counts, the stall-count threshold, and the production-stalled flag from the monitor.
 * @returns True when a recovery action is warranted.
 */
export function shouldTriggerRecovery(inputs: {
  hasEnded: boolean;
  hasError: boolean;
  isPaused: boolean;
  isProgressing: boolean;
  pauseCount: number;
  productionStalled: boolean;
  stallCount: number;
  stallCountThreshold: number;
  withinBufferingGrace: boolean;
  withinRecoveryGrace: boolean;
}): boolean {

  // Recovery never fires inside the post-recovery grace window - a freshly recovered stream is given time to stabilize before another attempt.
  if(inputs.withinRecoveryGrace) {

    return false;
  }

  // A persistent pause: paused past the stall-count hysteresis while outside the buffering grace window that filters transient rebuffer pauses.
  const persistentPause = inputs.isPaused && !inputs.withinBufferingGrace && (inputs.pauseCount > inputs.stallCountThreshold);

  // A persistent stall: not progressing past the same hysteresis while outside the buffering grace window.
  const persistentStall = !inputs.isProgressing && !inputs.withinBufferingGrace && (inputs.stallCount > inputs.stallCountThreshold);

  return inputs.hasError || inputs.hasEnded || persistentPause || persistentStall || inputs.productionStalled;
}

/**
 * Computes the recovery escalation level to enter for a fresh recovery episode. This is the single source of truth for the issue-aware escalation ladder: a
 * paused stream from a clean slate tries L1 (play/unmute) first, since it resolves paused playback roughly half the time; a first attempt for a buffering or
 * other issue - or a pause that L1 did not fix - escalates to L2 (source reload); and once a source reload has already been attempted, it escalates to L3 (page
 * navigation). The distinct L3-to-L2 fallback under a navigation rate limit is a separate concern owned by the recovery executor, not this ladder.
 * @param inputs - The current escalation level, the classified issue category, and whether a source reload has already been attempted this episode.
 * @returns The escalation level (1, 2, or 3) to enter.
 */
export function computeNextRecoveryLevel(inputs: {
  currentEscalationLevel: number;
  issueCategory: "buffering" | "other" | "paused";
  sourceReloadAttempted: boolean;
}): number {

  // A paused stream from a clean slate tries L1 (play/unmute) first - it resolves paused playback roughly half the time.
  if((inputs.issueCategory === "paused") && (inputs.currentEscalationLevel === 0)) {

    return 1;
  }

  // First attempt for a buffering or other issue, or L1 did not fix the pause: escalate to L2 (source reload).
  if(!inputs.sourceReloadAttempted) {

    return 2;
  }

  // Source reload already attempted: escalate to L3 (page navigation).
  return 3;
}

/**
 * The health classification and recovery escalation warranted by a native stream's segment-delivery metrics.
 */
export interface NativeSegmentHealthDecision {

  // The recovery escalation warranted this tick: none, an L2 page reload for fresh tokens, or an L3 capture fallback.
  readonly action: "l2" | "l3" | "none";

  // The health status to report for the native stream.
  readonly health: StreamHealthStatus;

  // The cause this tick names, or null when the stream is healthy.
  readonly issueType: Nullable<string>;
}

/**
 * A native stream's recorded issue: the cause the classifier last named and the moment that cause began. The pair travels together because the time is only
 * meaningful as the start of the labelled cause - a label replaced without its time would report a stall that has run since the fetch errors started.
 */
export interface NativeIssueRecord {

  // When the recorded cause began, or null when no cause is recorded.
  readonly issueTime: Nullable<number>;

  // The recorded cause, or null when none is recorded.
  readonly issueType: Nullable<string>;
}

/**
 * Classifies a native (proxied HLS) stream's health from its segment-delivery metrics and decides the recovery escalation, if any. This is the single source of
 * truth for the native staleness ladder: active fetch errors report "recovering" without escalating from here (the proxy retries internally); otherwise, once at
 * least one segment has been produced, staleness beyond 2x the target duration reports "stalled", with escalation to an L2 page reload at 4x on the first attempt
 * and, once that reload has not resolved the stall, an L3 capture fallback. Below the staleness threshold the stream is healthy. Pure and total; the
 * caller owns the segment-advance bookkeeping, issue-timestamp recording, logging, and firing of the returned action.
 * @param inputs - Consecutive fetch errors, the last-segment timestamp, prior recovery attempts, staleness in milliseconds, and the target duration in milliseconds.
 * @returns The health status, the warranted escalation action, and the issue label to record.
 */
export function classifyNativeSegmentHealth(inputs: {
  consecutiveErrors: number;
  lastSegmentTime: number;
  recoveryAttempts: number;
  stalenessMs: number;
  targetDurationMs: number;
}): NativeSegmentHealthDecision {

  // Active fetch errors take precedence: the proxy is retrying internally, so report recovering without escalating from here.
  if(inputs.consecutiveErrors > 0) {

    return { action: "none", health: "recovering", issueType: "fetch errors" };
  }

  // Staleness is only meaningful once at least one segment has been produced.
  const isStale = (inputs.stalenessMs > (inputs.targetDurationMs * 2)) && (inputs.lastSegmentTime > 0);

  if(!isStale) {

    return { action: "none", health: "healthy", issueType: null };
  }

  // Stalled past the escalation threshold (4x target duration). The first attempt is an L2 page reload for fresh tokens; once that reload has been attempted and the
  // stall persists, the next stalled tick escalates to an L3 capture fallback. Escalation is driven by whether the reload resolved the stall (recoveryAttempts), not
  // by a further staleness tier - a stream still stalled one tick after its reload is exactly the "the reload did not work" signal.
  const pastEscalationThreshold = inputs.stalenessMs > (inputs.targetDurationMs * 4);

  if(pastEscalationThreshold && (inputs.recoveryAttempts === 0)) {

    return { action: "l2", health: "stalled", issueType: "segment stall" };
  }

  if(pastEscalationThreshold && (inputs.recoveryAttempts > 0)) {

    return { action: "l3", health: "stalled", issueType: "segment stall" };
  }

  return { action: "none", health: "stalled", issueType: "segment stall" };
}

/**
 * Advances a native stream's issue record for one classification tick. Placed beside classifyNativeSegmentHealth because it completes that classifier's contract:
 * the classifier names the cause each tick, and this decides what the record carries as a result.
 *
 * A null decision leaves the record alone, so a stream that recovers keeps whatever the caller's own recovery bookkeeping cleared or kept. A cause equal to the
 * recorded one leaves the record alone too, and that rule is why the record cannot simply be overwritten: the start time has to survive across every tick the cause
 * persists, so the display reports how long the stream has been in trouble rather than how long ago the last tick ran. A different cause replaces both fields, so a
 * stream that degrades from fetch errors into a segment stall reports the stall and the moment the stall began.
 * @param current - The record as it stands.
 * @param issueType - The cause this tick named, or null when the stream is healthy.
 * @param now - The current time in milliseconds, stamped when the cause changes.
 * @returns The record for the next tick, which is the same object when nothing changed.
 */
export function nextNativeIssueRecord(current: NativeIssueRecord, issueType: Nullable<string>, now: number): NativeIssueRecord {

  if((issueType === null) || (issueType === current.issueType)) {

    return current;
  }

  return { issueTime: now, issueType };
}

/**
 * The largest-area intrinsic reading a stream has delivered, and whether the resolution recovery ladder has already run to acceptance at that size. The two facts
 * live on one record so an acceptance can never outlive the peak it was granted at: a larger reading replaces the record whole, accepted included.
 */
export interface ResolutionPeak {

  // Whether the ladder has already run to acceptance at this size, which is what keeps it from running again inside the same degraded episode.
  readonly accepted: boolean;

  // The height in pixels of the peak reading.
  readonly height: number;

  // The width in pixels of the peak reading.
  readonly width: number;
}

/**
 * The pixel area of a size. Every comparison below runs through it so "bigger" means one thing across the resolution helpers.
 * @param size - The size to measure.
 * @returns The area in pixels.
 */
function resolutionArea(size: { height: number; width: number }): number {

  return size.width * size.height;
}

/**
 * Folds a fresh intrinsic reading into the peak record, returning the record the stream should carry from here. This is the single source of truth for what "the
 * best this stream has delivered" means: the first reading establishes the peak, a larger-area reading replaces it whole - clearing any acceptance, because the
 * source has just proved it can do better than the level the ladder settled for - and anything smaller leaves the record alone. Area rather than per-dimension,
 * because a rendition ladder changes both dimensions together and a wider-but-shorter reading must not read as growth.
 * @param inputs - The current peak record, or null before the first reading, and the reading to fold in.
 * @returns The peak record to carry forward.
 */
export function updateResolutionPeak(inputs: { peak: Nullable<ResolutionPeak>; reading: { height: number; width: number } }): ResolutionPeak {

  if(!inputs.peak || (resolutionArea(inputs.reading) > resolutionArea(inputs.peak))) {

    return { accepted: false, height: inputs.reading.height, width: inputs.reading.width };
  }

  return inputs.peak;
}

/**
 * The reading's share of the peak by pixel area, where 1 means the picture is back at its best and 0.25 means a quarter of it. Every caller that reports or
 * judges a degradation reads the ratio from here rather than recomputing it, so the number in a log line and the number a threshold is tested against agree.
 * @param inputs - The peak record and the reading to measure against it.
 * @returns The reading's area over the peak's area.
 */
export function resolutionAreaRatio(inputs: { peak: ResolutionPeak; reading: { height: number; width: number } }): number {

  return resolutionArea(inputs.reading) / resolutionArea(inputs.peak);
}

/**
 * Whether a reading counts as degraded against the stream's own peak. Judging by area rather than by either dimension is what makes the test catch the case it
 * exists for: a 416x234 rendition against an 800x450 peak is 27 percent of the picture but 52 percent of either dimension, so a per-dimension test at the same
 * threshold would call the field's stuck renditions healthy.
 * @param inputs - The peak record, the reading to judge, and the area fraction below which a reading is degraded.
 * @returns True when the reading's area is below the threshold share of the peak's.
 */
export function isResolutionDegraded(inputs: { peak: ResolutionPeak; reading: { height: number; width: number }; threshold: number }): boolean {

  return resolutionAreaRatio(inputs) < inputs.threshold;
}

/**
 * The verb for a reading that has climbed back above the degradation threshold: "restored" once it is back at the peak's area, "improved" while it is short of
 * the best the stream has shown. This is what lets one message tell the whole story of an episode ending.
 * @param inputs - The peak record and the reading that ended the episode.
 * @returns The verb describing the outcome.
 */
export function describeResolutionOutcome(inputs: { peak: ResolutionPeak; reading: { height: number; width: number } }): "improved" | "restored" {

  return (resolutionArea(inputs.reading) >= resolutionArea(inputs.peak)) ? "restored" : "improved";
}
