/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * monitor.ts: Playback health monitoring for PrismCast.
 */
import type { CircuitBreakerState, MonitorHandle, RecoveryMetrics, TabReplacementResult } from "./recovery.ts";
import { EvaluateTimeoutError, LOG, capitalize, formatError, getAbortSignal, isSessionClosedError, runWithStreamContext, startTimer } from "../utils/index.ts";
import type { Frame, Page } from "puppeteer-core";
import type { Nullable, ResolvedSiteProfile, VideoState } from "../types/index.ts";
import { RECOVERY_METHODS, checkCircuitBreaker, classifyNativeSegmentHealth, computeNextRecoveryLevel, createRecoveryMetrics, deriveStreamHealth, formatIssueType,
  formatRecoveryDuration, getIssueCategory, getIssueDescription, getRecoveryMethod, recordRecoveryAttempt, recordRecoverySuccess,
  resetCircuitBreaker, shouldTriggerRecovery } from "./recovery.ts";
import type { StreamHealthStatus, StreamStatus } from "./statusEmitter.ts";
import { applyVideoStyles, buildVideoSelectorType, checkVideoPresence, enforceVideoVolume, ensurePlayback, findVideoContext, getVideoState, tuneToChannel,
  validateVideoElement, verifyFullscreen } from "../browser/video.ts";
import { getLastSegmentHasVideo, getLastSegmentSize, getStream, getStreamMemoryUsage } from "./registry.ts";
import { CONFIG } from "../config/index.ts";
import type { StreamRegistryEntry } from "./registry.ts";
import { clearProbeCache } from "../native/probe.ts";
import { emitStreamHealthChanged } from "./statusEmitter.ts";
import { getChannelLogo } from "../config/userChannels.ts";
import { getClientSummary } from "./clients.ts";
import { getEffectiveViewport } from "../config/presets.ts";
import { getProviderBySlug } from "../browser/channelSelection.ts";
import { getShowName } from "./showInfo.ts";
import { refreshNativeManifest } from "../native/index.ts";
import { resizeAndMinimizeWindow } from "../browser/cdp.ts";

/* Live video streams can fail in many ways: the network can drop, the player can stall, the site can auto-pause, or ads can break playback. The health monitor
 * watches for these failures and attempts recovery. This is essential for unattended DVR recording where the user cannot manually intervene.
 *
 * The monitor runs on a configurable interval (default: 2 seconds) and performs these checks:
 *
 * 1. Video progression: Compares currentTime to previous check. If currentTime has not advanced by at least STALL_THRESHOLD (0.1 seconds), the video is considered
 *    stalled. However, a single stall is not enough to trigger recovery - we require more than STALL_COUNT_THRESHOLD (default 2, so 3+) consecutive stalls to avoid
 *    reacting to momentary hiccups. Pause detection uses the same threshold - the video.paused property must be true for more than STALL_COUNT_THRESHOLD consecutive
 *    checks before triggering L1 recovery. This filters out transient rebuffer pauses where the player briefly pauses to refill its buffer and resumes on its own.
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
 * 6. Escalation reset: After SUSTAINED_PLAYBACK_REQUIRED (60 seconds) of healthy playback, the escalation level resets to 0, the source reload tracking clears, every
 *    failure counter (including the consecutive navigation-failure tally) clears, and the circuit breaker resets. This allows a stream that recovered to start fresh,
 *    rather than immediately escalating to aggressive recovery on the next issue.
 *
 * 7. Window re-minimize: Recovery actions (especially fullscreen) can cause the browser window to un-minimize. After the recovery grace period passes and the first
 *    healthy check occurs, the window is re-minimized to reduce GPU usage. This happens sooner than the escalation reset (~5-10 seconds vs 60 seconds) because we
 *    don't need to wait for sustained playback to determine the window can be minimized.
 *
 * The monitor is designed to be resilient to page navigations and context changes. When a page navigation recovery is performed, the monitor updates its context
 * reference to the new video context.
 */

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
 * Stream info passed to the monitor for status updates.
 */
export interface MonitorStreamInfo {

  channelName: Nullable<string>;
  numericStreamId: number;
  serviceName: string;

  // Service filter tag from the domain config (e.g., "xfinity", "hulu"). Used to look up the ProviderModule for service-specific monitoring flags.
  serviceTag?: string;

  startTime: Date;
}

// Monitor state interfaces.

/**
 * Native stream health tracking state. Used when the stream is in native mode to monitor segment delivery health.
 */
interface NativeHealthState {

  issueTime: Nullable<number>;
  issueType: Nullable<string>;
  lastCheckedSegmentIndex: number;
  lastSegmentAdvanceTime: number;
  recoveryAttempts: number;
}

/**
 * Recovery lifecycle state. Tracks escalation level, failure counters, and recovery flags that control which recovery method to use and when.
 */
interface RecoveryState {

  escalationLevel: number;
  graceUntil: number;
  inProgress: boolean;
  lastRecoveryTime: number;
  pauseCount: number;
  sourceReloadAttempted: boolean;
  stallCount: number;
  totalAttempts: number;
}

/**
 * Resolution degradation monitoring state. Tracks ABR quality relative to the configured viewport.
 */
interface ResolutionState {

  consecutiveDegradedReadings: number;
  graceEnd: number;
  recoveryAttempt: number;
}

/**
 * Segment production tracking state. Monitors both post-recovery segment verification and continuous segment size health.
 */
interface SegmentState {

  consecutiveTinySegments: number;
  lastCheckedIndex: number;
  lastSegmentAdvanceTime: number;
  preRecoveryIndex: Nullable<number>;
  productionStalled: boolean;
  waitStartTime: Nullable<number>;
  wasInTinyState: boolean;
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
 * @returns A MonitorHandle exposing the live recovery metrics (getMetrics) and a self-contained dispose that stops the monitor's polling interval.
 */
export function monitorPlaybackHealth(
  page: Page,
  context: Frame | Page,
  profile: ResolvedSiteProfile,
  url: string,
  streamId: string,
  streamInfo: MonitorStreamInfo,
  onCircuitBreak: () => void,
  onTabReplacement?: () => Promise<Nullable<TabReplacementResult>>
): MonitorHandle {

  /* Monitor state. These track the video's behavior over time and control recovery decisions. Mutable variables are organized into typed state objects by subsystem
   * (recovery, segments, native health, resolution) to clarify ownership and interaction boundaries. Variables that don't belong to a specific subsystem remain as
   * standalone declarations.
   */

  // The current page reference. This can change after tab replacement recovery, when the old tab is closed and a new one is created. We use a mutable variable so we
  // can update the reference after replacement.
  let currentPage = page;

  // The video's currentTime from the previous check. Used to detect whether the video is progressing. Null on first check since we have no previous value.
  let lastTime: Nullable<number> = null;

  // Timestamp when buffering started, or null if not currently buffering. Used to apply the buffering grace period - we don't trigger recovery for buffering until
  // it exceeds BUFFERING_GRACE_PERIOD.
  let bufferingStartTime: Nullable<number> = null;

  // Timestamps of recent page reloads within the PAGE_RELOAD_WINDOW. Used to enforce MAX_PAGE_RELOADS limit. Old entries are pruned on each check.
  let pageReloadTimestamps: number[] = [];

  // Counter for consecutive page navigation failures. If navigation fails twice in a row, we fall back to source reload (level 2) instead. This prevents getting
  // stuck in a loop when navigation itself is the problem.
  let consecutiveNavigationFailures = 0;

  // Track whether the browser window needs to be re-minimized after recovery. Recovery actions (especially ensureFullscreen) can cause the window to un-minimize.
  // We set this flag when recovery is triggered and clear it after the recovery grace period passes and we see a healthy check.
  let pendingReMinimize = false;

  // Graduated fullscreen reinforcement counter. Counts consecutive ticks where verifyFullscreen() returns false. On tick 1 we apply basic CSS styles (sufficient
  // for well-behaved sites like Hulu). On tick 2+ we escalate to !important priority to override sites that actively fight style changes. Reset to 0 when the
  // video fills the viewport again.
  let fullscreenReapplyCount = 0;

  // Flag indicating the cleanup function was called. When true, the next interval check will clear itself.
  let intervalCleared = false;

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

  // Last known video state for status reporting.
  let lastVideoState: Nullable<VideoState> = null;

  // Recovery metrics tracked throughout the stream's lifetime.
  const metrics = createRecoveryMetrics();

  // Last issue tracking for UI display. Stores what triggered recovery and when, so users can see stream history.
  let lastIssueType: Nullable<string> = null;
  let lastIssueTime: Nullable<number> = null;

  // Recovery grace periods in milliseconds, indexed by recovery level (L0 = no recovery, L1 = play/unmute, L2 = source reload, L3 = page reload). After a recovery
  // action we wait this long before checking for new issues to give the action time to take effect. L1 is a quick action. L2 and L3 need more time for
  // rebuffering or navigation to complete.
  const recoveryGracePeriods: readonly [number, number, number, number] = [ 0, 3_000, 10_000, 10_000 ];

  // Segment stall timeout (10 seconds). After L2/L3 recovery completes, if no new segments are produced within this window, the capture pipeline is considered
  // dead and we escalate directly to tab replacement. This catches the case where recovery reports success but the MediaRecorder/FFmpeg pipeline has silently died.
  const SEGMENT_STALL_TIMEOUT = 10_000;

  // Tiny segment detection thresholds. Used for continuous segment size monitoring to detect dead capture pipelines. When video capture dies but audio continues,
  // segments contain only audio data. Audio is transcoded at a controlled bitrate (max 512Kbps), so audio-only segments are at most ~128KB for 2-second segments (the
  // default hls.segmentDuration). The 500KB threshold catches both dead captures (18 bytes) and audio-only captures while staying well below the smallest video preset
  // (480p/3Mbps ~ 750KB/segment, also a 2-second basis).
  // The default count trigger (10) requires roughly 20 seconds of consecutive tiny segments before action is taken, balancing responsiveness against false positives.
  const TINY_SEGMENT_THRESHOLD = 512_000;
  const TINY_SEGMENT_COUNT_TRIGGER = 10;

  // Resolve the service-specific tiny segment count threshold once at monitor startup. Services with extended static content (e.g., Xfinity commercial
  // placeholders) set a higher value to tolerate longer periods of small segments without false positive tab replacements. Dead capture pipelines (segments with
  // no video trafs) always use TINY_SEGMENT_COUNT_TRIGGER regardless of this setting.
  const providerModule = streamInfo.serviceTag ? getProviderBySlug(streamInfo.serviceTag) : undefined;
  const providerTinySegmentThreshold = providerModule?.tinySegmentThreshold ?? TINY_SEGMENT_COUNT_TRIGGER;

  // Segment staleness timeout. When no new segments have been produced for this duration, the capture pipeline is considered dead even though the video element may
  // appear healthy. This catches the case where Chrome's MediaRecorder silently stops emitting data without raising an error - the input stream stays "open" but no
  // data events fire. The 20-second threshold is 4x the maximum expected moof delivery interval (5 seconds) to avoid false positives during normal bursty delivery.
  const SEGMENT_STALENESS_TIMEOUT = 20_000;

  // Resolution degradation detection. When the video element's intrinsic resolution is significantly below the configured viewport, the service's ABR is delivering
  // low-quality content. The threshold is expressed as a ratio - if either dimension is below this fraction of the viewport, the resolution is considered degraded.
  // 50% catches clear ABR degradation (768x432 on 1080p = 40%) while allowing legitimate 720p content on 1080p (67% > 50%).
  const RESOLUTION_RATIO_THRESHOLD = 0.5;

  // Grace period in milliseconds after stream start and after each recovery action. Gives ABR time to ramp up before flagging degradation.
  const RESOLUTION_GRACE_PERIOD = 30000;

  // Number of consecutive degraded readings required before triggering recovery. At ~2 seconds per monitor tick, 15 readings = ~30 seconds of sustained
  // degradation. This lets transient ABR dips (commercial breaks, ad transitions) self-heal without unnecessary page reloads.
  const RESOLUTION_DEGRADED_COUNT_THRESHOLD = 15;

  // Fixed margin in milliseconds before the maxContinuousPlayback limit at which a proactive reload is triggered. Two minutes provides enough time for page
  // navigation and video reinitialization to complete before the site enforces its cutoff.
  const PROACTIVE_RELOAD_MARGIN_MS = 120000;

  // Timestamp of the most recent full page navigation. Used to calculate elapsed continuous playback for proactive reload when maxContinuousPlayback is configured.
  // Initialized to Date.now() because the monitor starts immediately after tuneToChannel() succeeds in stream setup, meaning a page load just completed. Reset
  // after any successful page navigation recovery or tab replacement, but NOT after source reloads (L2) which preserve the page's JavaScript context.
  let lastPageNavigationTime = Date.now();

  // Pre-compute the selector type string for video element selection. This is passed to evaluate() calls.
  const selectorType = buildVideoSelectorType(profile);

  // Capture stream context for re-establishing on each interval tick. AsyncLocalStorage context is lost when entering setInterval callbacks. The show name
  // resolver is lazy - it reads from the live show name cache at log time, so messages always reflect the current program even as shows change mid-stream.
  const streamContext = {

    channelName: streamInfo.channelName ?? undefined,
    showNameResolver: (): string => getShowName(streamInfo.numericStreamId),
    streamId,
    url
  };

  // Recovery state. Tracks escalation level, failure counters, and recovery lifecycle flags that control the decision to trigger recovery and which method to use.
  const recoveryState: RecoveryState = {

    escalationLevel: 0,
    graceUntil: 0,
    inProgress: false,
    lastRecoveryTime: 0,
    pauseCount: 0,
    sourceReloadAttempted: false,
    stallCount: 0,
    totalAttempts: 0
  };

  // Segment production state. Tracks both post-recovery segment verification and continuous segment size monitoring. After L2/L3 recovery, we verify segments are
  // actually being produced. Independently, we monitor segment sizes on every tick to detect spontaneous capture pipeline death (dead pipelines produce tiny segments
  // while the video element appears healthy).
  const segmentState: SegmentState = {

    consecutiveTinySegments: 0,
    lastCheckedIndex: 0,
    lastSegmentAdvanceTime: Date.now(),
    preRecoveryIndex: null,
    productionStalled: false,
    waitStartTime: null,
    wasInTinyState: false
  };

  // Native stream health state. Only used when the stream is in native mode. Tracks segment delivery health to detect stalled streams where the service's manifest
  // stops advancing or segments stop arriving. Native recovery uses recoveryState.inProgress rather than a separate flag, since the interval callback already checks
  // that flag before dispatching to either the native or capture-mode health check path.
  const nativeHealthState: NativeHealthState = {

    issueTime: null,
    issueType: null,
    lastCheckedSegmentIndex: 0,
    lastSegmentAdvanceTime: Date.now(),
    recoveryAttempts: 0
  };

  // Resolution degradation monitoring. Separate from the recovery escalation (L1-L3 plus tab replacement) which handles broken playback. Resolution degradation is a
  // quality issue - the stream works but at lower-than-expected resolution. Uses its own tracking and two-step escalation: page reload, then tab replacement.
  const resolutionState: ResolutionState = {

    consecutiveDegradedReadings: 0,
    graceEnd: Date.now() + RESOLUTION_GRACE_PERIOD,
    recoveryAttempt: 0
  };

  /**
   * Checks segment delivery health for native streams. Detects stalled streams by comparing the proxy's segment index and last segment timestamp against thresholds.
   * Recovery follows three escalation levels per the plan:
   *
   * - L1: Re-fetch manifest (handled by the proxy's internal retry loop - consecutive failures up to the threshold)
   * - L2: Reload page for fresh tokens (same mechanism as proactive token refresh, but triggered by segment staleness)
   * - L3: Fall back to capture mode via tab replacement (stops native proxy, creates fresh page with capture pipeline)
   *
   * Note: The `recoveryState.inProgress` guard at the top of the interval callback prevents re-entry during async L2/L3 recovery. The native path does not need its
   * own guard - it reuses the shared flag.
   *
   * @param entry - The stream registry entry for the native stream.
   */
  function checkNativeStreamHealth(entry: StreamRegistryEntry): void {

    const proxy = entry.nativeProxy;

    if(!proxy) {

      emitStatusUpdate();

      return;
    }

    const now = Date.now();
    const currentSegmentIndex = proxy.getSegmentIndex();
    const lastSegmentTime = proxy.getLastSegmentTime();
    const targetDuration = proxy.getTargetDuration();
    const consecutiveErrors = proxy.getConsecutiveErrors();
    const storeKey = entry.info.storeKey;

    // Fast path: if the proxy hit its error threshold and stopped itself, trigger L3 fallback immediately. This avoids waiting for the staleness threshold when hard
    // errors (HTTP 403, network failures) have already been detected by the proxy's internal retry loop.
    if(proxy.hasErrored()) {

      LOG.debug("native:monitor", "Native proxy errored for %s. Initiating capture fallback.", storeKey);

      nativeHealthState.issueType = "proxy error";
      nativeHealthState.issueTime = now;

      // The caller establishes the stream context for this interval tick, so the fire-and-forget recovery promise inherits it across its async continuations.
      void executeNativeL3Fallback(entry);

      return;
    }

    // Check if new segments have been produced since the last tick.
    if(currentSegmentIndex > nativeHealthState.lastCheckedSegmentIndex) {

      nativeHealthState.lastCheckedSegmentIndex = currentSegmentIndex;
      nativeHealthState.lastSegmentAdvanceTime = now;

      // Clear any previous issue tracking when segments are flowing.
      if(nativeHealthState.issueType) {

        nativeHealthState.issueType = null;
        nativeHealthState.issueTime = null;
        nativeHealthState.recoveryAttempts = 0;

        LOG.debug("native:monitor", "Native stream healthy for %s. Segments advancing (index %s).", storeKey, currentSegmentIndex);
      }
    }

    // Calculate staleness: time since the last new segment was produced.
    const stalenessMs = now - nativeHealthState.lastSegmentAdvanceTime;

    // Classify health and the warranted escalation via the pure classifyNativeSegmentHealth core in recovery.ts. The shell below owns the side effects the core
    // leaves out: issue-timestamp recording, diagnostic logging, and firing the returned recovery action.
    const decision = classifyNativeSegmentHealth({

      consecutiveErrors,
      lastSegmentTime,
      recoveryAttempts: nativeHealthState.recoveryAttempts,
      stalenessMs,
      targetDurationMs: targetDuration * 1000
    });

    // Record the issue label the decision named when none is already tracked, so an earlier issue keeps its original timestamp across ticks.
    if(decision.issueType && !nativeHealthState.issueType) {

      nativeHealthState.issueType = decision.issueType;
      nativeHealthState.issueTime = now;
    }

    const staleSec = Math.round(stalenessMs / 1000);

    // Diagnostic logging mirrors the classification. The staleness threshold reported is 2x the target duration in seconds.
    if(decision.health === "recovering") {

      LOG.debug("native:monitor", "Native stream recovering for %s. Consecutive errors: %s.", storeKey, consecutiveErrors);
    } else if(decision.health === "stalled") {

      LOG.debug("native:monitor", "Native stream stalled for %s. No new segments in %ss (threshold: %ss).", storeKey, staleSec, Math.round(targetDuration * 2));
    }

    // L2: at 4x target duration on the first attempt, reload the page for fresh tokens - recovering from auth expiry where the manifest URL is still valid but
    // segment URLs are rejected. The proxy continues serving cached segments during the reload.
    if(decision.action === "l2") {

      LOG.warn("Native stream stalled for %s. No new segments in %ss. Attempting recovery.", storeKey, staleSec);

      nativeHealthState.recoveryAttempts++;

      // The caller establishes the stream context for this interval tick, so the fire-and-forget recovery promise inherits it across its async continuations.
      void executeNativeL2Recovery(entry);

      return;
    }

    // L3: at 6x target duration, or at 4x once an L2 attempt has already been made, fall back to capture mode via tab replacement.
    if(decision.action === "l3") {

      LOG.warn("Falling back to capture mode for %s: native streaming stalled after recovery attempt.", storeKey);

      // The caller establishes the stream context for this interval tick, so the fire-and-forget recovery promise inherits it across its async continuations.
      void executeNativeL3Fallback(entry);

      return;
    }

    emitNativeStatus(entry, decision.health);
  }

  /**
   * Emits a status update with native-specific health classification. Populates meaningful fields (health, issue tracking, memory, clients) and zeroes video-specific
   * fields that are not applicable to native streams.
   *
   * @param entry - The stream registry entry.
   * @param health - The health status to report.
   */
  function emitNativeStatus(entry: StreamRegistryEntry, health: StreamHealthStatus): void {

    if(intervalCleared) {

      return;
    }

    const now = Date.now();
    const memoryBytes = getStreamMemoryUsage(entry).total;
    const channelKey = entry.info.storeKey;
    const clientSummary = getClientSummary(streamInfo.numericStreamId);

    // Native streams have no recovery ladder, so we reuse escalationLevel purely as a UI severity encoding rather than as a recovery-level index: healthy maps to 0,
    // stalled to 1, and recovering to 2. The intended ordering is stalled < recovering because an actively-recovering stream warrants a stronger visual signal than one
    // that has merely stalled.
    const escalation = (health === "stalled") ? 1 : ((health === "recovering") ? 2 : 0);

    const status: StreamStatus = {

      bufferingDuration: null,
      captureCodec: entry.captureCodec,
      channel: streamInfo.channelName,
      clientCount: clientSummary.total,
      clients: clientSummary.clients,
      currentTime: 0,
      duration: Math.round((now - streamInfo.startTime.getTime()) / 1000),
      escalationLevel: escalation,
      hardwareAccelerated: entry.hardwareAccelerated,
      health,
      id: streamInfo.numericStreamId,
      lastIssueTime: nativeHealthState.issueTime,
      lastIssueType: nativeHealthState.issueType,
      lastRecoveryTime: null,
      logoUrl: channelKey ? (getChannelLogo(channelKey) ?? "") : "",
      memoryBytes,
      nativeBandwidth: entry.nativeBandwidth,
      nativeResolution: entry.nativeResolution,
      networkState: 0,
      pageReloadsInWindow: 0,
      readyState: 0,
      recoveryAttempts: nativeHealthState.recoveryAttempts,
      serviceName: streamInfo.serviceName,
      showName: getShowName(streamInfo.numericStreamId),
      startTime: streamInfo.startTime.toISOString(),
      streamingMode: entry.streamingMode,
      url
    };

    emitStreamHealthChanged(status);
  }

  /**
   * L2 recovery for native streams: reloads the page to get fresh authentication tokens and re-intercepts the manifest. Delegates to the shared refreshNativeManifest
   * helper in the coordinator module, which handles interceptor installation, navigation, probing, isStopped() guards, and proxy updates.
   *
   * @param entry - The stream registry entry.
   */
  async function executeNativeL2Recovery(entry: StreamRegistryEntry): Promise<void> {

    const proxy = entry.nativeProxy;

    if(!proxy || proxy.isStopped()) {

      return;
    }

    recoveryState.inProgress = true;

    LOG.debug("native:monitor", "Starting L2 recovery (page reload) for %s.", entry.info.storeKey);

    try {

      const success = await refreshNativeManifest({

        channelName: entry.info.storeKey,
        page: currentPage,
        proxy,
        streamIdStr: streamId,
        url
      });

      if(success) {

        // Reset staleness tracking so the monitor gives the refreshed stream time to produce segments.
        nativeHealthState.lastSegmentAdvanceTime = Date.now();
      }
    } finally {

      recoveryState.inProgress = false;
    }
  }

  /**
   * L3 recovery for native streams: falls back to capture mode via tab replacement. Stops the native proxy, creates a fresh page with capture pipeline, and switches
   * the stream to capture mode. The existing tab replacement infrastructure handles page creation, capture initialization, segmenter creation, and registry updates.
   *
   * If the proxy's onError fires concurrently (from the poll loop hitting its failure threshold), terminateStream runs before this async function gets a chance to
   * execute. By the time L3 runs, the stream is already terminated and executeTabReplacement returns null, which we handle as a failed outcome.
   *
   * @param entry - The stream registry entry.
   */
  async function executeNativeL3Fallback(entry: StreamRegistryEntry): Promise<void> {

    if(!onTabReplacement) {

      LOG.warn("Capture fallback not available for %s: no tab replacement handler.", entry.info.storeKey);
      onCircuitBreak();

      return;
    }

    LOG.debug("native:monitor", "Starting L3 fallback (capture mode) for %s.", entry.info.storeKey);

    // Stop the native proxy before tab replacement closes the page. The proxy may still be polling and would encounter errors when the page navigates away.
    if(entry.nativeProxy) {

      entry.nativeProxy.stop();
      entry.nativeProxy = null;
    }

    // Use the existing tab replacement infrastructure. It sets recoveryState.inProgress = true internally and clears it in finalizeTabReplacement. It creates a new
    // page with capture, navigates, sets up playback, creates a segmenter, and updates the registry entry (the page and the new capture session, with the segmenter
    // attached to it).
    const outcome = await executeTabReplacement("native fallback to capture");

    if(outcome.outcome === "success") {

      // Tab replacement succeeded. Update the registry to reflect capture mode. The tab replacement handler already set the page and the new capture session (with
      // its segmenter attached) on the registry entry. We just need to update the streaming mode and clear audio state.
      entry.streamingMode = "capture";

      // Clear separate audio state from the native proxy. Without this, hasAudio remains true and the HLS handler continues serving the master playlist (referencing
      // video.m3u8 and audio.m3u8) instead of the capture segmenter's variant playlist. Clients that cached the master playlist structure would request stale audio
      // and video variant playlists pointing to segments that are no longer being updated.
      entry.hls.hasAudio = false;
      entry.hls.audioPlaylist = "";
      entry.hls.audioSegments.clear();
      entry.hls.audioSegmentBytes = 0;
      entry.hls.videoPlaylist = "";

      // Clear the probe cache so subsequent tunes to this channel don't re-attempt native streaming.
      clearProbeCache(entry.info.storeKey);

      LOG.info("Switched to capture mode for %s: native streaming failed.", entry.info.storeKey);

      // The monitor's next tick will see streamingMode === "capture" and run the normal video element monitoring path. The state reset from
      // applyTabReplacementSuccess (called by executeTabReplacement) already initialized all capture-mode monitor variables.
    } else if(outcome.outcome === "terminated") {

      // Circuit breaker tripped during tab replacement. Stream is being terminated.
      LOG.warn("Capture fallback failed for %s: circuit breaker tripped.", entry.info.storeKey);
    } else {

      // Tab replacement failed but stream wasn't terminated. The circuit breaker will handle it on the next failure.
      LOG.warn("Capture fallback failed for %s: tab replacement unsuccessful.", entry.info.storeKey);
    }
  }

  // Helper to mark a discontinuity in the HLS playlist after recovery events that disrupt the video source. The segmenter flushes its current fragment buffer and sets
  // a pending discontinuity flag so the next segment boundary includes an #EXT-X-DISCONTINUITY tag. This tells HLS clients to flush their decoder state.
  const markStreamDiscontinuity = (): void => {

    getStream(streamInfo.numericStreamId)?.captureSession?.segmenter?.markDiscontinuity();
  };

  /**
   * Computes the health status classification based on current monitor state.
   * @returns The health status classification.
   */
  function computeHealthStatus(): StreamHealthStatus {

    // The health precedence ladder is the pure deriveStreamHealth core in recovery.ts; the monitor only supplies its current recovery and playback state.
    return deriveStreamHealth({

      escalationLevel: recoveryState.escalationLevel,
      hasError: lastVideoState?.error ?? false,
      isBuffering: bufferingStartTime !== null,
      stallCount: recoveryState.stallCount
    });
  }

  /**
   * Emits a status update for this stream.
   */
  function emitStatusUpdate(): void {

    // Do not emit after the monitor has been stopped. An in-flight async tick can resume from an await after terminateStream() has disposed the monitor handle
    // (clearing this interval) and called emitStreamRemoved(). Without this guard, the emitStreamHealthChanged() call below would re-add the dead stream to the
    // streamStatuses Map, creating a zombie entry that persists in SSE snapshots indefinitely.
    if(intervalCleared) {

      return;
    }

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
      captureCodec: entry?.captureCodec ?? null,
      channel: streamInfo.channelName,
      clientCount: clientSummary.total,
      clients: clientSummary.clients,
      currentTime: lastVideoState?.time ?? 0,
      duration: Math.round((now - streamInfo.startTime.getTime()) / 1000),
      escalationLevel: recoveryState.escalationLevel,
      hardwareAccelerated: entry?.hardwareAccelerated ?? false,
      health: computeHealthStatus(),
      id: streamInfo.numericStreamId,
      lastIssueTime,
      lastIssueType,
      lastRecoveryTime: recoveryState.lastRecoveryTime > 0 ? recoveryState.lastRecoveryTime : null,
      logoUrl: channelKey ? (getChannelLogo(channelKey) ?? "") : "",
      memoryBytes,
      nativeBandwidth: entry?.nativeBandwidth ?? 0,
      nativeResolution: entry?.nativeResolution ?? null,
      networkState: lastVideoState?.networkState ?? 0,
      pageReloadsInWindow: pageReloadTimestamps.length,
      readyState: lastVideoState?.readyState ?? 0,
      recoveryAttempts: recoveryState.totalAttempts,
      serviceName: streamInfo.serviceName,
      showName: getShowName(streamInfo.numericStreamId),
      startTime: streamInfo.startTime.toISOString(),
      streamingMode: entry?.streamingMode ?? "capture",
      url
    };

    emitStreamHealthChanged(status);
  }

  /**
   * Finalizes tab replacement by clearing the recovery flag and emitting status. This helper ensures consistent cleanup across all tab replacement exit paths (success,
   * failure, and error). The flag must be reset before emitting status to prevent the monitor from getting stuck if emitStatusUpdate() throws.
   */
  function finalizeTabReplacement(): void {

    recoveryState.inProgress = false;

    emitStatusUpdate();
  }

  /**
   * Resets all segment monitoring state variables. Called after successful recovery or sustained healthy playback to clear tracking for both post-recovery index
   * monitoring and continuous size monitoring.
   */
  function resetSegmentMonitoringState(): void {

    segmentState.preRecoveryIndex = null;
    segmentState.waitStartTime = null;
    segmentState.productionStalled = false;
    segmentState.consecutiveTinySegments = 0;
    segmentState.wasInTinyState = false;
    segmentState.lastCheckedIndex = getStream(streamInfo.numericStreamId)?.captureSession?.segmenter?.getSegmentIndex() ?? 0;
    segmentState.lastSegmentAdvanceTime = Date.now();
  }

  /**
   * Resets all failure/retry counters to zero. Called after successful tab replacement or page navigation to give the stream a fresh start.
   */
  function resetRecoveryCounters(): void {

    consecutiveTimeouts = 0;
    consecutiveNavigationFailures = 0;
    fullscreenReapplyCount = 0;
    recoveryState.pauseCount = 0;
    recoveryState.stallCount = 0;
    videoNotFoundCount = 0;
  }

  /**
   * Resets resolution monitoring state. Called when resolution reaches expected levels or after any recovery action that restarts ABR negotiation.
   */
  function resetResolutionState(): void {

    resolutionState.consecutiveDegradedReadings = 0;
    resolutionState.graceEnd = Date.now() + RESOLUTION_GRACE_PERIOD;
    resolutionState.recoveryAttempt = 0;
  }

  /**
   * Checks the page reload rate limit. Prunes expired timestamps, checks if the limit has been reached, and records the current timestamp if allowed. Callers are
   * responsible for logging and handling the rate-limited case - consequences differ by context (circuit break, deferral, fallback to L2).
   * @returns True if a page reload is allowed and the timestamp has been recorded, false if the rate limit has been reached.
   */
  function isPageReloadAllowed(): boolean {

    const reloadWindow = Date.now() - CONFIG.playback.pageReloadWindow;

    pageReloadTimestamps = pageReloadTimestamps.filter((ts) => ts > reloadWindow);

    if(pageReloadTimestamps.length >= CONFIG.playback.maxPageReloads) {

      return false;
    }

    pageReloadTimestamps.push(Date.now());

    return true;
  }

  /**
   * Resets escalation level and related flags. Called after successful recovery to allow the stream to start from level 0 on future issues.
   */
  function resetEscalationState(): void {

    recoveryState.escalationLevel = 0;
    recoveryState.sourceReloadAttempted = false;
  }

  /**
   * Sets the recovery grace period and re-minimize flag after a recovery action. The grace period prevents the monitor from immediately detecting new issues while the
   * recovery action takes effect.
   * @param level - The recovery level (1-3) to determine grace period duration.
   */
  function setRecoveryGracePeriod(level: number): void {

    pendingReMinimize = true;
    recoveryState.graceUntil = Date.now() + (recoveryGracePeriods[level] ?? 0);
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

      LOG.error("Recovery exhausted (%s) - terminating stream.", context);

      clearInterval(interval);
      onCircuitBreak();

      return { outcome: "terminated" };
    }

    return { outcome: "failed" };
  }

  /**
   * Applies successful tab replacement state. Updates page and context references, logs recovery duration, records metrics, and resets all failure/escalation state
   * for the fresh tab. Consolidated here so the try and catch paths in executeTabReplacement share a single implementation.
   * @param result - The successful tab replacement result containing the new page and context.
   */
  function applyTabReplacementSuccess(result: TabReplacementResult): void {

    currentPage = result.page;
    currentContext = result.context;

    const duration = formatRecoveryDuration(metrics.currentRecoveryStartTime ?? Date.now());

    LOG.info("Recovered in %s via %s.", duration, RECOVERY_METHODS.tabReplacement);

    recordRecoverySuccess(metrics, RECOVERY_METHODS.tabReplacement);

    // Full state reset for fresh tab.
    lastPageNavigationTime = Date.now();
    resetRecoveryCounters();
    resetEscalationState();
    resetSegmentMonitoringState();
    resetResolutionState();
    setRecoveryGracePeriod(3);
    resetCircuitBreaker(circuitBreaker);
  }

  /**
   * Handles tab replacement failure after all retry attempts are exhausted. Clears stale recovery metrics (preventing ghost "Recovered" logs from the
   * deferred-success check), runs the circuit breaker, and detects zombie streams where the old page was destroyed but no replacement was created.
   * @param context - Description of the failure for circuit breaker logging.
   * @returns The tab replacement outcome (failed or terminated).
   */
  function handleExhaustedTabReplacement(context: string): TabReplacementOutcome {

    // Clear stale recovery metrics so the deferred-success check does not falsely log "Recovered" from leftover state set by recordRecoveryAttempt.
    metrics.currentRecoveryStartTime = null;
    metrics.currentRecoveryMethod = null;

    LOG.warn("Tab replacement unsuccessful after retry - stream will be terminated.");

    const failureOutcome = handleTabReplacementFailure(context);

    // If the circuit breaker did not trip but the old page is gone (handler destroyed it before createPageWithCapture failed), the stream is unrecoverable. The
    // next monitor tick would silently clear the interval via currentPage.isClosed() with no termination log, no status emission, and no cleanup - creating a
    // zombie entry in the registry. Terminate explicitly instead.
    if((failureOutcome.outcome === "failed") && currentPage.isClosed()) {

      LOG.error("Tab replacement failed and the original page is no longer available - terminating stream.");

      clearInterval(interval);
      onCircuitBreak();

      return { outcome: "terminated" };
    }

    return failureOutcome;
  }

  /**
   * Executes tab replacement recovery with full error handling. This unified helper handles all tab replacement triggers (tiny segments, stalled capture, unresponsive
   * tab) consistently, including metrics recording, success/failure logging, circuit breaker checks, and state resets.
   *
   * On failure, retries onTabReplacement once before giving up. The handler destroys old resources (capture, segmenter, FFmpeg, page) before calling
   * createPageWithCapture, so a retry is the only chance to save the stream when the first attempt fails. All handler cleanup steps are idempotent on retry:
   * rawCaptureStream.destroyed guard, segmenter stop() checks state.stopped, FFmpeg kill() checks ffmpeg.killed, page close checks !oldPage.isClosed(), and
   * unregisterManagedPage is idempotent.
   * @param issueType - Description of what triggered the replacement (for logging and UI display).
   * @returns The tab replacement outcome.
   */
  async function executeTabReplacement(issueType: string): Promise<TabReplacementOutcome> {

    // Guard: caller should ensure onTabReplacement exists, but TypeScript needs explicit narrowing.
    if(!onTabReplacement) {

      return { outcome: "failed" };
    }

    recoveryState.inProgress = true;
    recoveryState.totalAttempts++;
    recoveryState.lastRecoveryTime = Date.now();
    lastIssueType = issueType;
    lastIssueTime = Date.now();

    const tabRecoveryElapsed = startTimer();

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.tabReplacement);

    try {

      let result = await onTabReplacement();

      // First attempt failed - retry once. See idempotency notes in the JSDoc above.
      if(!result) {

        LOG.debug("recovery:tab", "Tab replacement attempt 1/2 failed. Retrying...");

        try {

          result = await onTabReplacement();
        } catch(retryError) {

          LOG.debug("recovery:tab", "Tab replacement attempt 2/2 failed: %s.", formatError(retryError));
        }
      }

      if(result) {

        applyTabReplacementSuccess(result);

        return { outcome: "success" };
      }

      return handleExhaustedTabReplacement("tab replacement unsuccessful");
    } catch(error) {

      // Unexpected error (not from onTabReplacement - those are caught internally by the handler in hls.ts and return null). Guard against registry corruption,
      // getStream failures, or other unexpected errors.
      LOG.debug("recovery:tab", "Tab replacement attempt 1/2 failed: %s. Retrying...", formatError(error));

      try {

        const retryResult = await onTabReplacement();

        if(retryResult) {

          applyTabReplacementSuccess(retryResult);

          return { outcome: "success" };
        }
      } catch(retryError) {

        LOG.debug("recovery:tab", "Tab replacement attempt 2/2 failed: %s.", formatError(retryError));
      }

      return handleExhaustedTabReplacement("tab replacement error");
    } finally {

      LOG.debug("timing:recovery", "Tab replacement completed. Total: %sms.", tabRecoveryElapsed());

      finalizeTabReplacement();
    }
  }

  /**
   * Performs page navigation recovery with validation. This is the single recovery function used by both the "video not found" and "escalation level 3" (page
   * navigation) code paths, ensuring consistent behavior. The function:
   * 1. Calls tuneToChannel to reinitialize playback
   * 2. Checks for unexpected new tabs
   * 3. Validates the page URL
   * 4. Validates the video element exists and is accessible
   * 5. Only returns success if all validations pass
   * @returns Recovery result with the new context if successful.
   */
  async function performPageNavigationRecovery(): Promise<PageNavigationRecoveryResult> {

    const navRecoveryElapsed = startTimer();

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

        LOG.debug("recovery:nav", "Detected %s new tab(s) created during navigation.", pageCountAfter - pageCountBefore);
      }

      // Validate that we're on the expected page.
      const currentUrl = currentPage.url();
      const expectedHostname = new URL(url).hostname;

      if(!currentUrl.includes(expectedHostname)) {

        LOG.debug("recovery:nav", "Page URL after navigation (%s) does not match expected hostname.", currentUrl);
      }

      // Validate that the video element is accessible and has reasonable state.
      const validationState = await validateVideoElement(newContext, selectorType);

      if(validationState.found) {

        LOG.debug("timing:recovery", "Page navigation recovery succeeded. Total: %sms.", navRecoveryElapsed());

        return { newContext, success: true };
      }

      LOG.warn("Page navigation completed but video element not found in new context.");

      LOG.debug("timing:recovery", "Page navigation recovery failed (no video). Total: %sms.", navRecoveryElapsed());

      return { success: false };
    } catch(error) {

      LOG.warn("Failed to reinitialize video after page navigation: %s.", formatError(error));

      LOG.debug("timing:recovery", "Page navigation recovery failed (error). Total: %sms.", navRecoveryElapsed());

      return { success: false };
    }
  }

  // Tick phase functions. Each implements one phase of the monitoring interval, closing over monitor state and taking per-tick computed values as parameters. Functions
  // that can exit the tick return a boolean: true means the caller should return immediately, false means continue.

  /**
   * Handles the case where no video element was found in the current context. Applies a grace period before triggering recovery: first failure waits, second failure
   * attempts frame re-search, third failure escalates to full page navigation. Distinguishes "no video element" from "video exists but not ready" (buffering).
   *
   * Always exits the tick - no path falls through to subsequent health checks.
   * @param now - Current timestamp for timing calculations.
   */
  async function handleVideoNotFound(now: number): Promise<void> {

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

      // Video element exists but doesn't meet readyState criteria. This is a buffering condition, not a missing video condition. Apply the normal buffering grace
      // period instead of escalating to navigation.
      LOG.debug("recovery:general", "Video is buffering (readyState=%s, elements=%s).", presence.maxReadyState, presence.videoCount);

      // Reset video not found counter since video actually exists.
      videoNotFoundCount = 0;

      emitStatusUpdate();

      return;
    }

    videoNotFoundCount++;

    LOG.warn("Video element not found (attempt %s/3). Context: %s, frames: %s, videoCount: %s.",
      videoNotFoundCount, contextType, frameCount, presence?.videoCount ?? 0);

    // Grace period: Wait for 2 consecutive failures before attempting context re-search, 3 before full navigation.
    if(videoNotFoundCount < 2) {

      emitStatusUpdate();

      return;
    }

    // After 2+ failures, try re-searching frames to see if video moved to a different context.
    if(videoNotFoundCount === 2) {

      LOG.debug("recovery:context", "Re-searching frames for video element.");

      try {

        const newContext = await findVideoContext(currentPage, profile);
        const validationState = await validateVideoElement(newContext, selectorType);

        if(validationState.found) {

          LOG.info("Video found in different context after re-search. readyState=%s.", validationState.readyState);

          currentContext = newContext;
          videoNotFoundCount = 0;

          emitStatusUpdate();

          return;
        }

        LOG.warn("Re-search did not find video in any frame.");
      } catch(error) {

        LOG.warn("Frame re-search failed: %s.", formatError(error));
      }

      emitStatusUpdate();

      return;
    }

    // After 3+ consecutive failures, escalate to full page navigation recovery.
    LOG.warn("Video element not found - recovering via %s.", RECOVERY_METHODS.pageNavigation);

    // Check circuit breaker for too many failures.
    const cbResult = checkCircuitBreaker(circuitBreaker, now);

    if(cbResult.shouldTrip) {

      LOG.error("Recovery failed after %s attempts - terminating stream.", cbResult.totalCount);

      clearInterval(interval);
      onCircuitBreak();

      return;
    }

    // Set escalation to level 3 to trigger page navigation. We skip lower levels since they require a video element.
    // Note: Keep state updates in sync with the main recovery path in executeRecoveryAction below.
    recoveryState.escalationLevel = 3;
    recoveryState.lastRecoveryTime = now;
    recoveryState.totalAttempts++;
    pendingReMinimize = true;
    recoveryState.inProgress = true;

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.pageNavigation);

    // Check page reload limit before attempting recovery.
    if(!isPageReloadAllowed()) {

      LOG.error("Page navigation rate limit reached (%s in %s minutes) - cannot recover without video element.",
        CONFIG.playback.maxPageReloads, Math.round(CONFIG.playback.pageReloadWindow / 60000));

      clearInterval(interval);
      onCircuitBreak();

      return;
    }

    const recoveryResult = await performPageNavigationRecovery();

    // Page navigation disrupted the video stream. Mark a discontinuity regardless of navigation success so HLS clients resynchronize their decoders.
    markStreamDiscontinuity();

    // Set grace period to give page navigation time to take effect (L3 = 10 seconds).
    recoveryState.graceUntil = now + recoveryGracePeriods[3];

    if(recoveryResult.success && recoveryResult.newContext) {

      currentContext = recoveryResult.newContext;

      const duration = formatRecoveryDuration(metrics.currentRecoveryStartTime ?? now);

      LOG.info("Recovered in %s via %s.", duration, RECOVERY_METHODS.pageNavigation);

      recordRecoverySuccess(metrics, RECOVERY_METHODS.pageNavigation);

      lastPageNavigationTime = Date.now();
      resetRecoveryCounters();
      resetEscalationState();
      resetSegmentMonitoringState();
      resetResolutionState();
    } else {

      consecutiveNavigationFailures++;

      LOG.warn("Page navigation did not restore playback.");
    }

    recoveryState.inProgress = false;

    emitStatusUpdate();
  }

  /**
   * Monitors segment production health. Performs two independent checks:
   *
   * 1. Post-recovery verification: After L2/L3 recovery completes and the grace period ends, verifies that segments are actually being produced. This catches the case
   *    where recovery reports success but the MediaRecorder/FFmpeg pipeline has silently died.
   *
   * 2. Continuous size monitoring: Watches segment sizes on every tick to detect spontaneous capture pipeline death. Dead pipelines produce tiny segments (18 bytes
   *    observed) while the video element appears healthy. Also detects segment staleness when no new segments have been produced for SEGMENT_STALENESS_TIMEOUT.
   *
   * @param now - Current timestamp for timing calculations.
   * @param withinRecoveryGrace - Whether the monitor is within the post-recovery grace period.
   * @returns True if the tick should exit (after tab replacement trigger), false to continue.
   */
  async function monitorSegmentHealth(now: number, withinRecoveryGrace: boolean): Promise<boolean> {

    // Post-recovery segment verification. After recovery grace period ends, check that segments are flowing.
    if((segmentState.preRecoveryIndex !== null) && !withinRecoveryGrace) {

      // Start the segment wait timer when recovery grace period ends.
      segmentState.waitStartTime ??= now;

      // Check if segments are flowing by comparing current index to pre-recovery index.
      const entry = getStream(streamInfo.numericStreamId);
      const currentIndex = entry?.captureSession?.segmenter?.getSegmentIndex() ?? null;

      if((currentIndex !== null) && (currentIndex > segmentState.preRecoveryIndex)) {

        // Segments are flowing - recovery actually succeeded. Clear tracking state.
        segmentState.preRecoveryIndex = null;
        segmentState.waitStartTime = null;
        segmentState.productionStalled = false;
      } else if((now - segmentState.waitStartTime) > SEGMENT_STALL_TIMEOUT) {

        // No new segments for SEGMENT_STALL_TIMEOUT after recovery grace period. The capture pipeline is dead.
        LOG.warn("No segments produced for %ss after recovery - capture pipeline may have stalled.", SEGMENT_STALL_TIMEOUT / 1000);

        segmentState.productionStalled = true;
      }
    }

    // Continuous segment size monitoring. Runs on every healthy interval to detect spontaneous capture pipeline death.
    const sizeCheckEntry = getStream(streamInfo.numericStreamId);
    const currentSegmentIndex = sizeCheckEntry?.captureSession?.segmenter?.getSegmentIndex() ?? 0;

    if((currentSegmentIndex > segmentState.lastCheckedIndex) && sizeCheckEntry) {

      // A new segment was produced. Update the staleness tracker and check its size.
      segmentState.lastSegmentAdvanceTime = now;

      const segmentSize = getLastSegmentSize(sizeCheckEntry) ?? 0;

      if(segmentSize < TINY_SEGMENT_THRESHOLD) {

        segmentState.consecutiveTinySegments++;
        segmentState.wasInTinyState = true;

        // Check track composition to determine the effective threshold. Dead capture pipelines produce audio-only segments (hasVideo=false) and always use the
        // default count for fast detection. Segments with video trafs present use the service-specific threshold, which may be higher for services with extended
        // static content (e.g., Xfinity commercial placeholders lasting several minutes).
        const hasVideo = getLastSegmentHasVideo(sizeCheckEntry);
        const effectiveThreshold = (hasVideo === false) ? TINY_SEGMENT_COUNT_TRIGGER : providerTinySegmentThreshold;

        LOG.debug("recovery:tracks", "Below-threshold segment: %d bytes, hasVideo=%s, consecutive=%d, threshold=%d.",
          segmentSize, String(hasVideo), segmentState.consecutiveTinySegments, effectiveThreshold);

        if(segmentState.consecutiveTinySegments >= effectiveThreshold) {

          LOG.warn("Detected %d consecutive undersized segments (%dKB) - capture pipeline may have stalled.",
            segmentState.consecutiveTinySegments, Math.round(segmentSize / 1024));

          // Trigger tab replacement if available, otherwise let circuit breaker handle it via segmentState.productionStalled.
          if(onTabReplacement && !recoveryState.inProgress) {

            await executeTabReplacement("tiny segments");

            return true;
          } else if(!onTabReplacement) {

            segmentState.productionStalled = true;
          }
        }
      } else {

        // Valid segment size. Check for spontaneous recovery from tiny segment state.
        if(segmentState.wasInTinyState) {

          const hasVideo = getLastSegmentHasVideo(sizeCheckEntry);

          LOG.debug("recovery:segments", "Segment production self-healed (%d bytes, hasVideo=%s).", segmentSize, String(hasVideo));
        }

        segmentState.consecutiveTinySegments = 0;
        segmentState.wasInTinyState = false;
      }

      segmentState.lastCheckedIndex = currentSegmentIndex;
    } else if(sizeCheckEntry && (segmentState.lastCheckedIndex > 0) && !withinRecoveryGrace &&
      ((now - segmentState.lastSegmentAdvanceTime) > SEGMENT_STALENESS_TIMEOUT)) {

      /* Segment staleness detection. The segment index has not advanced for longer than SEGMENT_STALENESS_TIMEOUT. This catches the case where Chrome's
       * MediaRecorder silently stops emitting data - the input Readable stream stays "open" (no end/error events) but no data events fire. The segmenter
       * receives nothing, produces no new segments, and the playlist freezes at the last known sequence number. The video element on the page continues
       * playing normally (currentTime advances, no errors), so all video health checks pass. Without this check, the stale playlist persists indefinitely.
       *
       * The sizeCheckEntry guard prevents firing on a stream that was terminated mid-tick. The segmentState.lastCheckedIndex > 0 guard ensures we don't trigger
       * during stream startup before the first segment has been produced. The recovery grace guard prevents false triggering during legitimate pauses (e.g.,
       * after tab replacement while the new capture pipeline is initializing).
       */
      LOG.warn("No new segments produced for %ss - capture pipeline may have stalled.", SEGMENT_STALENESS_TIMEOUT / 1000);

      if(onTabReplacement && !recoveryState.inProgress) {

        await executeTabReplacement("segment staleness");

        return true;
      } else if(!onTabReplacement) {

        segmentState.productionStalled = true;
      }
    }

    return false;
  }

  /**
   * Monitors video resolution against the configured viewport and triggers recovery for sustained ABR degradation. Uses a two-step escalation: page reload (forces ABR
   * restart), then tab replacement (fresh page with new network connections). Accepts the resolution after both attempts to avoid infinite loops on legitimately
   * low-resolution content. Also detects and logs resolution restoration after successful recovery.
   *
   * @param now - Current timestamp for timing calculations.
   * @param state - Current video state with intrinsic resolution.
   * @param isProgressing - Whether the video is advancing.
   * @param withinRecoveryGrace - Whether the monitor is within the post-recovery grace period.
   * @returns True if the tick should exit (after recovery action), false to continue.
   */
  async function monitorResolutionDegradation(now: number, state: VideoState, isProgressing: boolean, withinRecoveryGrace: boolean): Promise<boolean> {

    // Only check when playback is healthy, outside recovery grace, and the video has non-zero intrinsic dimensions.
    if(!isProgressing || state.paused || state.error || state.ended || withinRecoveryGrace || (state.videoWidth === 0) || (state.videoHeight === 0)) {

      return false;
    }

    const viewport = getEffectiveViewport(CONFIG);
    const widthRatio = state.videoWidth / viewport.width;
    const heightRatio = state.videoHeight / viewport.height;

    const isDegraded = (widthRatio < RESOLUTION_RATIO_THRESHOLD) || (heightRatio < RESOLUTION_RATIO_THRESHOLD);

    if(isDegraded && (now >= resolutionState.graceEnd)) {

      resolutionState.consecutiveDegradedReadings++;

      LOG.debug("recovery:resolution", "Video resolution: %s\u00d7%s (viewport: %s\u00d7%s, ratio: %s%%\u00d7%s%%, consecutive: %s/%s).",
        String(state.videoWidth), String(state.videoHeight), String(viewport.width), String(viewport.height),
        String(Math.round(widthRatio * 100)), String(Math.round(heightRatio * 100)),
        String(resolutionState.consecutiveDegradedReadings), String(RESOLUTION_DEGRADED_COUNT_THRESHOLD));
    } else {

      resolutionState.consecutiveDegradedReadings = 0;
    }

    // Escalation step 1: page reload. Forces the service's ABR to restart quality negotiation. Only triggers after sustained degradation
    // (RESOLUTION_DEGRADED_COUNT_THRESHOLD consecutive readings) to let transient ABR dips self-heal.
    if((resolutionState.consecutiveDegradedReadings >= RESOLUTION_DEGRADED_COUNT_THRESHOLD) && (resolutionState.recoveryAttempt === 0)) {

      const degradedDuration = resolutionState.consecutiveDegradedReadings * 2;

      LOG.warn("Video resolution has been degraded for %ss (%s\u00d7%s in %s\u00d7%s viewport). Attempting recovery via %s.",
        String(degradedDuration), String(state.videoWidth), String(state.videoHeight),
        String(viewport.width), String(viewport.height), RECOVERY_METHODS.pageNavigation);

      recoveryState.inProgress = true;

      if(!isPageReloadAllowed()) {

        LOG.warn("Resolution recovery deferred - page navigation rate limit reached (%s in %s minutes).",
          CONFIG.playback.maxPageReloads, Math.round(CONFIG.playback.pageReloadWindow / 60000));

        // Defer by pushing grace end forward to avoid re-triggering every 2 seconds.
        resolutionState.graceEnd = now + RESOLUTION_GRACE_PERIOD;
        recoveryState.inProgress = false;

        emitStatusUpdate();

        return true;
      }

      pendingReMinimize = true;

      const recoveryResult = await performPageNavigationRecovery();

      markStreamDiscontinuity();

      resolutionState.consecutiveDegradedReadings = 0;
      resolutionState.recoveryAttempt = 1;
      resolutionState.graceEnd = now + RESOLUTION_GRACE_PERIOD;

      if(recoveryResult.success && recoveryResult.newContext) {

        currentContext = recoveryResult.newContext;
        lastPageNavigationTime = Date.now();
      } else {

        LOG.warn("Resolution recovery via page reload unsuccessful.");
      }

      recoveryState.inProgress = false;

      emitStatusUpdate();

      return true;
    }

    // Escalation step 2: tab replacement. Creates a fresh page with new capture pipeline and network connections.
    if((resolutionState.consecutiveDegradedReadings >= RESOLUTION_DEGRADED_COUNT_THRESHOLD) && (resolutionState.recoveryAttempt === 1)) {

      if(onTabReplacement) {

        const degradedDuration = resolutionState.consecutiveDegradedReadings * 2;

        LOG.warn("Video resolution is still degraded after %ss (%s\u00d7%s). Attempting recovery via %s.",
          String(degradedDuration), String(state.videoWidth), String(state.videoHeight), RECOVERY_METHODS.tabReplacement);

        await executeTabReplacement("resolution degraded");

        resolutionState.consecutiveDegradedReadings = 0;
        resolutionState.recoveryAttempt = 2;
        resolutionState.graceEnd = now + RESOLUTION_GRACE_PERIOD;

        return true;
      }

      // Tab replacement not available - skip directly to acceptance.
      resolutionState.recoveryAttempt = 2;
    }

    // Acceptance: resolution still degraded after both recovery attempts. Log once and stop retrying.
    if((resolutionState.consecutiveDegradedReadings >= RESOLUTION_DEGRADED_COUNT_THRESHOLD) && (resolutionState.recoveryAttempt === 2)) {

      LOG.warn("Video resolution remains degraded (%s\u00d7%s in %s\u00d7%s viewport) after recovery attempts. The stream will continue at reduced quality.",
        String(state.videoWidth), String(state.videoHeight), String(viewport.width), String(viewport.height));

      resolutionState.recoveryAttempt = 3;
    }

    // Resolution is good: clear tracking state so future degradation starts fresh. Use "restored" when resolution matches the viewport, "improved" when it's
    // above the degradation threshold but below the viewport. Include the recovery method so this single message tells the complete story.
    if(!isDegraded && (resolutionState.recoveryAttempt > 0)) {

      const isFullQuality = (state.videoWidth >= viewport.width) && (state.videoHeight >= viewport.height);
      const verb = isFullQuality ? "restored" : "improved";
      const method = (resolutionState.recoveryAttempt === 1) ? "page reload" : "tab replacement";

      LOG.info("Video resolution %s to %s\u00d7%s after %s.", verb, String(state.videoWidth), String(state.videoHeight), method);

      resolutionState.consecutiveDegradedReadings = 0;
      resolutionState.recoveryAttempt = 0;
      resolutionState.graceEnd = 0;
    }

    return false;
  }

  /**
   * Handles proactive page reload for sites with maxContinuousPlayback limits. Some streaming sites enforce a maximum continuous playback duration (e.g., NBC.com cuts
   * streams after 4 hours). This function proactively reloads the page before the site's limit expires to maintain uninterrupted streaming. The reload triggers
   * PROACTIVE_RELOAD_MARGIN_MS (2 minutes) before the configured limit.
   *
   * Only runs when playback is healthy (escalationLevel === 0), not within a recovery grace period, and progressing normally.
   *
   * @param now - Current timestamp for timing calculations.
   * @param state - Current video state.
   * @param isProgressing - Whether the video is advancing.
   * @param withinRecoveryGrace - Whether the monitor is within the post-recovery grace period.
   * @returns True if the tick should exit (after reload attempt or deferral), false to continue.
   */
  async function handleProactiveReload(now: number, state: VideoState, isProgressing: boolean, withinRecoveryGrace: boolean): Promise<boolean> {

    if((profile.maxContinuousPlayback === null) || (recoveryState.escalationLevel !== 0) || withinRecoveryGrace || !isProgressing || state.paused || state.error ||
      state.ended) {

      return false;
    }

    const maxPlaybackMs = profile.maxContinuousPlayback * 3600000;
    const elapsedMs = now - lastPageNavigationTime;

    if(elapsedMs < (maxPlaybackMs - PROACTIVE_RELOAD_MARGIN_MS)) {

      return false;
    }

    const elapsedHours = (elapsedMs / 3600000).toFixed(1);

    LOG.info("Proactive reload after %sh of continuous playback (site limit: %sh). Reloading page to prevent stream cutoff.",
      elapsedHours, String(profile.maxContinuousPlayback));

    recoveryState.inProgress = true;

    // Check page reload rate limit. Proactive reload is best-effort maintenance - if the reload budget is exhausted from recent error recoveries, we gracefully
    // yield. If the site eventually cuts the stream, normal error recovery handles it.
    if(!isPageReloadAllowed()) {

      LOG.warn("Proactive reload deferred - page navigation rate limit reached (%s in %s minutes).",
        CONFIG.playback.maxPageReloads, Math.round(CONFIG.playback.pageReloadWindow / 60000));

      // Set a grace period to prevent this deferral from re-triggering every 2 seconds while the rate limit remains in effect. We set recoveryState.graceUntil
      // directly rather than calling setRecoveryGracePeriod() because no recovery action was performed - the window state is unchanged and pendingReMinimize
      // should not be set.
      recoveryState.graceUntil = now + recoveryGracePeriods[3];
      recoveryState.inProgress = false;

      emitStatusUpdate();

      return true;
    }

    const recoveryResult = await performPageNavigationRecovery();

    markStreamDiscontinuity();
    setRecoveryGracePeriod(3);

    if(recoveryResult.success && recoveryResult.newContext) {

      currentContext = recoveryResult.newContext;
      lastPageNavigationTime = Date.now();

      LOG.info("Proactive reload completed successfully.");

      resetRecoveryCounters();
      resetSegmentMonitoringState();
      resetResolutionState();
    } else {

      LOG.warn("Proactive reload unsuccessful. Will retry after recovery grace period.");
    }

    recoveryState.inProgress = false;

    emitStatusUpdate();

    return true;
  }

  /**
   * Executes recovery when the monitor has determined that recovery is needed. Handles issue-aware escalation:
   * - Paused issues try L1 (play/unmute) first since it works ~50% of the time for paused state
   * - Buffering issues skip L1 and go directly to L2 (source reload) since L1 never helps buffering
   * - If L2 has already been attempted, skip to L3 (page reload) since a second L2 always fails
   *
   * Also handles segment production stalls (direct escalation to tab replacement) and circuit breaker checks. Levels 1-2 use ensurePlayback() for in-page recovery,
   * level 3 uses performPageNavigationRecovery() for full page navigation.
   *
   * @param now - Current timestamp for timing calculations.
   * @param state - Current video state.
   * @param isProgressing - Whether the video is advancing.
   * @param isBuffering - Whether the video is actively buffering.
   * @returns True if the tick should exit (circuit breaker tripped or tab replacement triggered), false to continue.
   */
  async function executeRecoveryAction(now: number, state: VideoState, isProgressing: boolean, isBuffering: boolean): Promise<boolean> {

    // Segment production stall handling. When segments stopped flowing after L2/L3 recovery, the capture pipeline is dead and normal recovery won't help. Skip the
    // escalation ladder and go directly to tab replacement if available.
    if(segmentState.productionStalled && onTabReplacement) {

      LOG.warn("Capture pipeline still stalled - escalating to %s.", RECOVERY_METHODS.tabReplacement);

      await executeTabReplacement("capture pipeline stalled");

      return true;
    }

    // Check circuit breaker for too many failures.
    const cbResult = checkCircuitBreaker(circuitBreaker, now);

    if(cbResult.shouldTrip) {

      const elapsedSeconds = circuitBreaker.firstFailureTime ? Math.round((now - circuitBreaker.firstFailureTime) / 1000) : 0;

      LOG.error("Recovery failed after %s attempts in %ss - terminating stream.", cbResult.totalCount, elapsedSeconds);

      clearInterval(interval);
      onCircuitBreak();

      return true;
    }

    // Issue-aware escalation. The escalation ladder is the pure computeNextRecoveryLevel core in recovery.ts; the later L3-to-L2 fallback under a navigation rate
    // limit stays here in the executor because it is entangled with the async navigation attempt.
    const issueCategory = getIssueCategory(state, !isProgressing, isBuffering);
    const nextLevel = computeNextRecoveryLevel({

      currentEscalationLevel: recoveryState.escalationLevel,
      issueCategory,
      sourceReloadAttempted: recoveryState.sourceReloadAttempted
    });

    // Note: Keep state updates in sync with the video-not-found recovery path in handleVideoNotFound above.
    recoveryState.escalationLevel = nextLevel;
    recoveryState.lastRecoveryTime = now;
    recoveryState.totalAttempts++;
    pendingReMinimize = true;

    // Get recovery method name for logging and metrics.
    const recoveryMethod = getRecoveryMethod(recoveryState.escalationLevel);

    // Store issue type and time for UI display.
    const issueType = formatIssueType(state, !isProgressing, isBuffering);

    lastIssueType = issueType;
    lastIssueTime = now;

    // If a previous recovery was pending (L1 or L2 that didn't result in healthy playback), log that it was unsuccessful before starting the new attempt.
    if(metrics.currentRecoveryMethod !== null) {

      LOG.warn("%s did not resolve the issue - escalating to %s.", capitalize(metrics.currentRecoveryMethod), recoveryMethod);
    } else {

      const issueDesc = getIssueDescription(issueCategory);

      LOG.warn("Playback %s - recovering via %s.", issueDesc, recoveryMethod);
    }

    recordRecoveryAttempt(metrics, recoveryMethod);

    // For L2/L3 recovery, record the current segment index so we can verify segments are flowing after recovery completes.
    if(recoveryState.escalationLevel >= 2) {

      const entry = getStream(streamInfo.numericStreamId);

      segmentState.preRecoveryIndex = entry?.captureSession?.segmenter?.getSegmentIndex() ?? null;
      segmentState.waitStartTime = null;
      segmentState.productionStalled = false;
    }

    recoveryState.inProgress = true;

    try {

      // Levels 1-2: In-page recovery via ensurePlayback().
      if(recoveryState.escalationLevel <= 2) {

        await ensurePlayback(currentPage, currentContext, profile, { recoveryLevel: recoveryState.escalationLevel, skipNativeFullscreen: true });

        if(recoveryState.escalationLevel === 2) {

          recoveryState.sourceReloadAttempted = true;

          markStreamDiscontinuity();
        }

        recoveryState.graceUntil = now + (recoveryGracePeriods[recoveryState.escalationLevel] ?? 0);
        resetResolutionState();
      } else {

        // Level 3: Page navigation recovery.

        // Safety check: If page navigation has failed twice consecutively, fall back to source reload.
        if(consecutiveNavigationFailures >= 2) {

          LOG.warn("Page navigation has failed %s consecutive times - falling back to source reload.",
            consecutiveNavigationFailures);

          recoveryState.escalationLevel = 2;
          consecutiveNavigationFailures = 0;

          recoveryState.sourceReloadAttempted = false;
        } else {

          if(!isPageReloadAllowed()) {

            LOG.warn("Page navigation rate limit reached (%s in %s minutes) - falling back to source reload.",
              CONFIG.playback.maxPageReloads, Math.round(CONFIG.playback.pageReloadWindow / 60000));

            recoveryState.escalationLevel = 2;

            recoveryState.sourceReloadAttempted = false;
          } else {

            const recoveryResult = await performPageNavigationRecovery();

            markStreamDiscontinuity();

            recoveryState.graceUntil = now + recoveryGracePeriods[3];

            if(recoveryResult.success && recoveryResult.newContext) {

              currentContext = recoveryResult.newContext;

              const duration = formatRecoveryDuration(metrics.currentRecoveryStartTime ?? now);

              LOG.info("Recovered in %s via %s.", duration, RECOVERY_METHODS.pageNavigation);

              recordRecoverySuccess(metrics, RECOVERY_METHODS.pageNavigation);

              lastPageNavigationTime = Date.now();
              resetRecoveryCounters();
              resetEscalationState();
              resetSegmentMonitoringState();
              resetResolutionState();
            } else {

              consecutiveNavigationFailures++;

              LOG.warn("Page navigation did not restore playback (attempt %s/2).", consecutiveNavigationFailures);
            }
          }
        }
      }
    } catch(error) {

      LOG.warn("Recovery via %s failed: %s.", getRecoveryMethod(recoveryState.escalationLevel), formatError(error));
    }

    recoveryState.inProgress = false;

    return false;
  }

  /* Main monitoring interval. This runs every MONITOR_INTERVAL milliseconds to check video state and trigger recovery when needed.
   *
   * Every early return must call emitStatusUpdate() before returning (except when the stream is terminating, e.g., page closed or circuit breaker tripped). This
   * ensures SSE clients always have current status data (duration, memory, health) even during recovery, buffering, or video search periods. Without this, the
   * streamStatuses map becomes stale and new SSE connections receive outdated snapshots.
   *
   * Check ordering: the recoveryState.inProgress check must come before the currentPage.isClosed() check. During tab replacement, the old page is intentionally
   * closed while the handler creates a new page; if we checked isClosed() first, we would terminate the interval while recovery is still in progress, causing
   * status updates to stop permanently. The required sequence is: (1) intervalCleared for explicit cleanup, (2) recoveryState.inProgress to continue during
   * recovery, (3) isClosed() for unexpected page termination outside of recovery.
   */
  const interval = setInterval((): void => {

    // Stop monitoring if cleanup was requested.
    if(intervalCleared) {

      clearInterval(interval);

      return;
    }

    // Skip health checks if a recovery operation is in progress. During tab replacement, the old page will be closed but we must keep the interval running until the
    // new page is assigned. Emit status so SSE clients see current state (health, duration, memory) even during recovery.
    if(recoveryState.inProgress) {

      emitStatusUpdate();

      return;
    }

    // Stop monitoring if the page was closed outside of recovery. This handles cases like browser disconnect or explicit stream termination.
    if(currentPage.isClosed()) {

      clearInterval(interval);

      return;
    }

    // For static capture profiles (e.g., staticPage), there is no video element to monitor. Skip all video health checks and just emit a status update.
    if(profile.staticCapture) {

      emitStatusUpdate();

      return;
    }

    // For native streaming mode, monitor segment delivery health instead of video element state. We check the registry on each tick rather than caching the mode at
    // startup because the streaming mode is set after the monitor starts (native streaming is attempted after setupStream returns).
    const nativeEntry = getStream(streamInfo.numericStreamId);

    if(nativeEntry?.streamingMode === "native") {

      // Re-establish stream context for this interval tick before running the native health check. AsyncLocalStorage context is lost when entering setInterval
      // callbacks, so without this wrapper the native path's non-debug warnings would emit without the stream-ID prefix. This mirrors the capture-mode branch below.
      // eslint-disable-next-line @typescript-eslint/require-await -- runWithStreamContext requires a promise-returning callback; checkNativeStreamHealth is synchronous.
      void runWithStreamContext(streamContext, async () => {

        checkNativeStreamHealth(nativeEntry);
      });

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

            LOG.debug("recovery:context", "Video context was invalidated (frame detached). Will re-search for video.");
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

          LOG.debug("recovery:context", "Re-searching for video after context invalidation.");

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

        // Video not found: apply grace period, attempt frame re-search, and escalate to page navigation recovery. Always exits the tick.
        if(!state) {

          await handleVideoNotFound(now);

          return;
        }

        // Video was found - reset the not found counter, timeout counter, and save state for status reporting.
        videoNotFoundCount = 0;
        consecutiveTimeouts = 0;
        lastVideoState = state;

        /* Volume enforcement. Some sites aggressively mute videos (e.g., France24 mutes on page visibility change, some sites mute for ads). We restore volume on
         * every check to ensure audio is captured.
         */
        if(state.muted || (state.volume < 1)) {

          await enforceVideoVolume(currentContext, selectorType);
        }

        /* Stall detection. We compare currentTime to the previous check to determine if the video is progressing. The STALL_THRESHOLD (0.1 seconds) allows for minor
         * timing variations while still detecting genuinely stalled videos.
         */

        // Video is progressing if: this is the first check (no previous time), OR currentTime has advanced by at least STALL_THRESHOLD since last check.
        const isProgressing = (lastTime === null) || (Math.abs(state.time - lastTime) >= CONFIG.playback.stallThreshold);

        /* Buffering detection. True buffering occurs when the player needs more data (readyState < 3) AND is actively fetching it (networkState === 2). We use AND
         * rather than OR because networkState === 2 is normal for live streams - data continuously arrives. Only when combined with insufficient data does it indicate
         * actual buffering.
         */
        const isBuffering = (state.readyState < 3) && (state.networkState === 2);

        /* Buffering grace period tracking. When buffering starts, we record the timestamp. We only trigger recovery if buffering exceeds BUFFERING_GRACE_PERIOD. This
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
        const withinRecoveryGrace = now < recoveryState.graceUntil;

        // Segment production monitoring: post-recovery verification and continuous size/staleness checks.
        if(await monitorSegmentHealth(now, withinRecoveryGrace)) {

          return;
        }

        /* Re-minimize check. After recovery, the browser window may have been un-minimized by fullscreen actions. As soon as the stream is healthy (progressing without
         * issues), we re-minimize to reduce GPU usage.
         */
        if(pendingReMinimize && isProgressing && !state.paused && !state.error && !state.ended) {

          LOG.debug("recovery:general", "Re-minimizing browser window after successful recovery.");

          pendingReMinimize = false;

          await resizeAndMinimizeWindow(currentPage);
        }

        /* Fullscreen reinforcement. Some streaming sites (notably Hulu) revert the video to a mini-player or PiP layout in response to browser state changes such as
         * window minimization or visibility events. Because the video continues playing normally in the smaller frame, no existing recovery condition is triggered - the
         * health monitor sees healthy, progressing playback while the captured frame shows a small video in the corner of the viewport. We verify that the video fills
         * the viewport on every healthy tick and re-apply CSS fullscreen styling when it shrinks. The response is graduated: basic CSS first (sufficient for
         * well-behaved sites like Hulu), escalating to !important priority only if basic styles don't hold by the next tick. The readyState guard prevents false
         * positives during momentary readyState dips where verifyFullscreen() cannot find a ready video even though the video layout has not changed. A null return
         * from verifyFullscreen() indicates the check was inconclusive (e.g. context destroyed) and is ignored.
         */
        if(isProgressing && !state.paused && !state.error && !state.ended && !withinRecoveryGrace && (state.readyState >= 3)) {

          const isFullscreen = await verifyFullscreen(currentContext, selectorType);

          if(isFullscreen === false) {

            fullscreenReapplyCount++;

            // Graduated escalation: first attempt uses basic CSS (sufficient for well-behaved sites like Hulu that only need a nudge). If the basic styles
            // didn't hold by the next tick, escalate to !important priority to override sites that actively fight style changes.
            const useImportant = fullscreenReapplyCount > 1;

            if(fullscreenReapplyCount === 1) {

              LOG.info("Video no longer fills viewport. Re-applying fullscreen styling.");
            } else if(fullscreenReapplyCount === 2) {

              LOG.info("Basic fullscreen styling did not hold. Escalating to !important priority.");
            }

            await applyVideoStyles(currentContext, selectorType, useImportant);
          } else if(isFullscreen && (fullscreenReapplyCount > 0)) {

            LOG.info("Video fullscreen restored.");

            fullscreenReapplyCount = 0;
          }
        }

        // Resolution degradation detection: two-step recovery (page reload, tab replacement) for sustained ABR degradation.
        if(await monitorResolutionDegradation(now, state, isProgressing, withinRecoveryGrace)) {

          return;
        }

        /* Stall counter management. We increment recoveryState.stallCount when the video is not progressing and not within buffering grace. We reset to 0 when
         * progression resumes. This hysteresis prevents reacting to single-frame hiccups.
         */
        if(!isProgressing && !withinBufferingGrace) {

          recoveryState.stallCount++;
        } else if(isProgressing) {

          recoveryState.stallCount = 0;
        }

        /* Pause counter management. We increment recoveryState.pauseCount when video.paused is true and reset when it clears. This provides the same hysteresis as
         * stall detection, filtering out transient rebuffer pauses (where the player briefly pauses to refill its buffer) while still catching genuine persistent
         * pauses.
         */
        if(state.paused) {

          recoveryState.pauseCount++;
        } else {

          recoveryState.pauseCount = 0;
        }

        /* Recovery decision. We trigger recovery when any of these conditions are met AND we're not within the recovery grace period:
         * - Video has an error state
         * - Video ended (live streams shouldn't end)
         * - Video is paused persistently (pauseCount exceeds threshold and not just buffering)
         * - Video is stalled for too long (stallCount exceeds threshold and not in buffering grace)
         * - Segment production has stalled after recovery (capture pipeline dead)
         */
        const needsRecovery = shouldTriggerRecovery({

          hasEnded: state.ended,
          hasError: state.error,
          isPaused: state.paused,
          isProgressing,
          pauseCount: recoveryState.pauseCount,
          productionStalled: segmentState.productionStalled,
          stallCount: recoveryState.stallCount,
          stallCountThreshold: CONFIG.playback.stallCountThreshold,
          withinBufferingGrace: Boolean(withinBufferingGrace),
          withinRecoveryGrace
        });

        /* Escalation reset. After sustained healthy playback (SUSTAINED_PLAYBACK_REQUIRED, default 60 seconds), we reset the escalation level and circuit breaker.
         * This allows a stream that recovered to start fresh, rather than immediately escalating to aggressive recovery on the next issue.
         */
        if(isProgressing && !state.paused && !state.ended && !state.error) {

          // If a recovery was pending confirmation (L1/L2), log success now that we have healthy playback.
          if((metrics.currentRecoveryStartTime !== null) && (metrics.currentRecoveryMethod !== null)) {

            const duration = formatRecoveryDuration(metrics.currentRecoveryStartTime);

            LOG.info("Recovered in %s via %s.", duration, metrics.currentRecoveryMethod);

            recordRecoverySuccess(metrics, metrics.currentRecoveryMethod);
          }

          const healthyDuration = now - recoveryState.lastRecoveryTime;

          if((recoveryState.escalationLevel > 0) && (healthyDuration > CONFIG.playback.sustainedPlaybackRequired)) {

            // Clear buffering state. The bufferingStartTime may persist through recovery cycles due to networkState === 2 (NETWORK_LOADING) being true for live streams
            // even during healthy playback. Since we have confirmed 60 seconds of progression, the stream is definitively not buffering.
            bufferingStartTime = null;

            // Reset escalation, failure counters, segment tracking, and circuit breaker. Sustained healthy playback confirms the stream works, so we clear every
            // failure tally for a true fresh start. Without resetting the counters here, a stale consecutiveNavigationFailures count could carry across the healthy
            // window and prematurely escalate a future L3 episode, defeating the purpose of the 60-second reset.
            resetEscalationState();
            resetRecoveryCounters();
            resetSegmentMonitoringState();
            resetCircuitBreaker(circuitBreaker);
          }
        }

        // Proactive page reload for sites with maxContinuousPlayback limits.
        if(await handleProactiveReload(now, state, isProgressing, withinRecoveryGrace)) {

          return;
        }

        // Recovery execution: issue-aware escalation (L1 play/unmute, L2 source reload, L3 page navigation) with circuit breaker.
        if(needsRecovery) {

          if(await executeRecoveryAction(now, state, isProgressing, isBuffering)) {

            return;
          }
        }

        // Update lastTime for the next stall check.
        lastTime = state.time;

        // Emit status update for SSE subscribers.
        emitStatusUpdate();
      } catch(error) {

        recoveryState.inProgress = false;

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

            LOG.warn("Tab unresponsive - recovering via %s.", RECOVERY_METHODS.tabReplacement);

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

          LOG.debug("recovery:general", "Monitor check aborted: %s.", errorMessage);
        } else {

          LOG.error("Monitor check failed: %s.", errorMessage);
        }

        // Emit status for non-abort errors so SSE clients stay current. Abort errors don't need this because termination is already in progress and the next
        // tick's abort check will clean up.
        if(!errorMessage.includes("aborted")) {

          emitStatusUpdate();
        }
      }
    }).catch((outerError: unknown) => {

      // Log errors that escape the inner try/catch. In normal operation we should not reach here - if we do, there's a bug to investigate.
      LOG.warn("Monitor tick error escaped inner try/catch: %s.", formatError(outerError));
    });
  }, CONFIG.playback.monitorInterval);

  /* The monitor's teardown: mark the interval cleared (so any in-flight async tick short-circuits) and clear it. Self-contained - it owns only the interval. Defined
   * as a const so it is exposed as both dispose() and [Symbol.dispose].
   */
  const dispose = (): void => {

    intervalCleared = true;
    clearInterval(interval);
  };

  /* Return the monitor handle. getMetrics() exposes the live recovery metrics (read in the termination prologue for the summary log); dispose() / [Symbol.dispose]
   * stops the monitor. The metrics object remains valid after disposal because it is a retained counter snapshot, not derived at stop time.
   */
  return {

    dispose,
    getMetrics: (): RecoveryMetrics => metrics,
    [Symbol.dispose]: dispose
  };
}
