/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * monitor.ts: Playback health monitoring for PrismCast.
 */
import type { CircuitBreakerState, MonitorHandle, RecoveryMetrics, ResolutionPeak, TabReplacementResult } from "./recovery.ts";
import { EvaluateTimeoutError, LOG, capitalize, formatError, formatResolution, getAbortSignal, isPageDeathError, isSessionClosedError,
  runWithStreamContext, startTimer } from "../utils/index.ts";
import type { Frame, Page } from "puppeteer-core";
import type { NativeStreamIdentity, StreamRegistryEntry } from "./registry.ts";
import type { Nullable, ResolvedSiteProfile, VideoState } from "../types/index.ts";
import { RECOVERY_METHODS, checkCircuitBreaker, classifyNativeSegmentHealth, computeNextRecoveryLevel, createRecoveryMetrics, deriveStreamHealth,
  describeResolutionOutcome, formatIssueType, formatRecoveryDuration, getIssueCategory, getIssueDescription, getRecoveryMethod, isResolutionDegraded,
  nextNativeIssueRecord, recordRecoveryAttempt, recordRecoverySuccess, resetCircuitBreaker, resolutionAreaRatio, shouldTriggerRecovery,
  updateResolutionPeak } from "./recovery.ts";
import type { StreamHealthStatus, StreamStatus } from "./statusEmitter.ts";
import { applyNativeQualityRefresh, getLastSegmentHasVideo, getLastSegmentSize, getStream, getStreamMemoryUsage, getStreamSegmenter, isCaptureIdentity,
  isHardwareAccelerated, makePendingCaptureIdentity } from "./registry.ts";
import { applyVideoStyles, buildVideoSelectorType, checkVideoPresence, enforceVideoVolume, ensurePlayback, findVideoContext, getVideoState, tuneToChannel,
  validateVideoElement, verifyFullscreen } from "../browser/video.ts";
import { getCaptureImpairment, syncWindowVisibility } from "../browser/index.ts";
import { getEffectiveCaptureCodec, isCaptureHardwareAccelerated } from "./codec.ts";
import { CONFIG } from "../config/index.ts";
import { clearNativeInitState } from "./hlsSegments.ts";
import { clearProbeCache } from "../native/probe.ts";
import { emitStreamHealthChanged } from "./statusEmitter.ts";
import { getChannelLogo } from "../config/userChannels.ts";
import { getClientSummary } from "./clients.ts";
import { getPresetViewport } from "../config/presets.ts";
import { getProviderBySlug } from "../browser/channelSelection.ts";
import { getShowName } from "./showInfo.ts";
import { reaffirmCaptureSurface } from "../browser/cdp.ts";
import { refreshNativeManifest } from "../native/index.ts";

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
 *    - Level 3: Full page navigation (the most disruptive option; two consecutive failures fall back to source reload, and repeated failures can trip the
 *      circuit breaker or hit the page-reload rate limit).
 *
 * 5. Circuit breaker: If too many failures occur within a time window (default: 10 failures in 5 minutes), the stream is considered fundamentally broken and the
 *    circuit breaker trips, terminating the stream. This prevents endless recovery attempts that consume resources.
 *
 * 6. Escalation reset: After SUSTAINED_PLAYBACK_REQUIRED (60 seconds) of healthy playback, the escalation level resets to 0, the source reload tracking clears, every
 *    failure counter (including the consecutive navigation-failure tally) clears, and the circuit breaker resets. This allows a stream that recovered to start fresh,
 *    rather than immediately escalating to aggressive recovery on the next issue.
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
 * Resolution degradation monitoring state. Tracks ABR quality relative to the best resolution the stream has delivered.
 */
interface ResolutionState {

  consecutiveDegradedReadings: number;
  graceEnd: number;

  /* The largest-area intrinsic reading seen while playback was progressing, null until the first such reading and growing only, because the page's rendition ladder
   * is fixed for the stream and a drop below half of it is what "degraded" means. The record's accepted flag says the ladder already ran to acceptance at this
   * size; it clears when the picture returns to the peak (the episode is over) or when a larger reading replaces the record (the source proved more), and never
   * through the recovery resets, so an unrelated recovery cannot re-run the ladder inside one degraded episode.
   */
  peak: Nullable<ResolutionPeak>;

  // The ladder step in flight: none, page reload issued, or tab replacement issued.
  recoveryAttempt: 0 | 1 | 2;
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

/* MonitorDeps is the cross-module collaborator set the monitor reaches other layers through: the capture-impairment read the recovery ladder consults before
 * offering tab replacement, the window-visibility sync a mode revert settles the window with, and the two capture-codec reads a completed native fallback
 * re-derives its identity from. It is injected as a default parameter, mirroring CreatePageWithCaptureDeps in streaming/setup.ts and VideoTuneDeps in
 * browser/video.ts, so a test can substitute stubs at the same collaborator-injection boundary - no loader mock - while production uses the real
 * defaultMonitorDeps built from the functions this module already imports. The impairment read belongs here for a reason of its own: a replacement starts a fresh
 * capture, so whether one can succeed is a fact about the browser, and injecting the read is what lets a test drive the ladder's availability decision without a
 * live Chrome. The codec reads belong here for the mirror-image reason: their real answers depend on the host's GPU, so only an injected pair lets a test prove
 * the fallback actually consults them rather than passing on values that happen to agree. This is the collaborator-injection form of the Clock port
 * (utils/clock.ts).
 */
export interface MonitorDeps {

  readonly getCaptureImpairment: typeof getCaptureImpairment;
  readonly getEffectiveCaptureCodec: typeof getEffectiveCaptureCodec;
  readonly isCaptureHardwareAccelerated: typeof isCaptureHardwareAccelerated;
  readonly syncWindowVisibility: typeof syncWindowVisibility;
}

const defaultMonitorDeps: MonitorDeps = { getCaptureImpairment, getEffectiveCaptureCodec, isCaptureHardwareAccelerated, syncWindowVisibility };

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
 *                           If null/undefined, tab replacement is not available; sustained evaluate timeouts are only surfaced via status (lastIssueType) with no
 *                           automatic recovery escalation.
 * @param deps - The injected browser-boundary collaborators; defaults to defaultMonitorDeps. Threaded so a test drives the ladder's availability decision and the
 *               window sync without a live Chrome.
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
  onTabReplacement?: () => Promise<Nullable<TabReplacementResult>>,
  deps: MonitorDeps = defaultMonitorDeps
): MonitorHandle {

  /* Monitor state. These track the video's behavior over time and control recovery decisions. Mutable variables are organized into typed state objects by subsystem
   * (recovery, segments, native health, resolution) to clarify ownership and interaction boundaries. Variables that don't belong to a specific subsystem remain as
   * standalone declarations.
   */

  // The current page reference. This can change after tab replacement recovery, when the old tab is closed and a new one is created. We use a mutable variable so we
  // can update the reference after replacement.
  let currentPage = page;

  /* How many ticks pass between capture-surface re-affirmations, and the count of ticks since this stream's monitor started. Thirty ticks is about a minute at the
   * default cadence, which is the resolution the catch-all is aiming for: fast enough that no disturbance survives long in a recording, slow enough that the
   * command costs nothing measurable next to the health reads the same tick performs.
   */
  const SURFACE_REAFFIRM_TICK_INTERVAL = 30;

  let reaffirmTickCounter = 0;

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

  // Graduated fullscreen reinforcement counter. Counts consecutive ticks where verifyFullscreen() returns false. On tick 1 we apply basic CSS styles (sufficient
  // for well-behaved sites like Hulu). On tick 2+ we escalate to !important priority to override sites that actively fight style changes. Reset to 0 when the
  // video fills the viewport again.
  let fullscreenReapplyCount = 0;

  // Flag indicating the cleanup function was called. When true, the next interval check will clear itself.
  let intervalCleared = false;

  /* Serialization flag for the tick body. The interval fires on its schedule whether or not the previous tick's async body has settled, so without this a single
   * slow evaluate - bounded only by the evaluate timeout, seven and a half intervals long at the default cadence - stacks concurrent bodies that each mutate the
   * shared counters and can each independently reach a recovery decision. With it, one dispatched tick body is the most that can be in flight.
   *
   * The scope is exactly that: the dispatched body. A native-mode recovery is launched fire-and-forget from inside a tick and outlives the body that started it,
   * and recoveryState.inProgress is the guard covering that window - which is why every native recovery entry point raises that flag synchronously before its
   * first await, and why a future native recovery path must do the same or it runs under neither guard.
   */
  let tickInProgress = false;

  // The current video context (page or frame). This can change after a page navigation recovery, when we need to find the new video context.
  let currentContext: Frame | Page = context;

  // Circuit breaker state. Tracks total failures within a time window and trips (terminates the stream) when too many failures occur.
  const circuitBreaker: CircuitBreakerState = { firstFailureTime: null, totalFailureCount: 0 };

  // Counter for consecutive "video not found" occurrences. We apply a grace period before triggering recovery to handle momentary context invalidation or readyState
  // fluctuations. Reset to 0 when video is found.
  let videoNotFoundCount = 0;

  /* Counter for consecutive evaluate timeouts. When the browser tab becomes unresponsive, evaluate() calls time out instead of returning data. The counter drives
   * two things: the three-strike threshold that triggers tab replacement recovery (when the callback is provided), and the timeout the next state read is issued
   * with - any non-zero value arms the short confirmation probe below. Reset to 0 on a successful getVideoState() and on every other piece of direct evidence
   * that the tab answers evaluates.
   */
  let consecutiveTimeouts = 0;

  /* Evaluate bound for a state read issued while a timeout streak is open. One full-length timeout is what detects a hung tab; the reads that follow only have
   * to tell a still-hung tab from a responsive one, and a tab that answers at all answers well inside this. At the default cadence three strikes then accumulate
   * in roughly twenty seconds - one full timeout to detect, two short probes to confirm - rather than three full timeouts.
   *
   * The bound is fixed rather than derived from the operator's monitor interval: how quickly a tab must answer to count as alive is a property of the tab, not of
   * how often we poll it, and the wall-clock to replacement scales with the chosen interval either way.
   */
  const UNRESPONSIVE_PROBE_TIMEOUT = 2000;

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
  const recoveryGracePeriods: readonly [number, number, number, number] = [ 0, 3000, 10000, 10000 ];

  // Segment stall timeout (10 seconds). After L2/L3 recovery completes, if no new segments are produced within this window, the capture pipeline is considered
  // dead and we escalate directly to tab replacement. This catches the case where recovery reports success but the MediaRecorder/FFmpeg pipeline has silently died.
  const SEGMENT_STALL_TIMEOUT = 10000;

  // Tiny segment detection thresholds. Used for continuous segment size monitoring to detect dead capture pipelines. When video capture dies but audio continues,
  // segments contain only audio data. Audio is transcoded at a controlled bitrate (max 512Kbps), so audio-only segments are at most ~128KB for 2-second segments (the
  // default hls.segmentDuration). The 500KB threshold catches both dead captures (18 bytes) and audio-only captures while staying well below the smallest video preset
  // (480p/3Mbps ~ 750KB/segment, also a 2-second basis).
  // The default count trigger (10) requires roughly 20 seconds of consecutive tiny segments before action is taken, balancing responsiveness against false positives.
  const TINY_SEGMENT_THRESHOLD = 512000;
  const TINY_SEGMENT_COUNT_TRIGGER = 10;

  // Resolve the service-specific tiny segment count threshold once at monitor startup. Services with extended static content (e.g., Xfinity commercial
  // placeholders) set a higher value to tolerate longer periods of small segments without false positive tab replacements. Dead capture pipelines (segments with
  // no video trafs) always use TINY_SEGMENT_COUNT_TRIGGER regardless of this setting.
  const providerModule = streamInfo.serviceTag ? getProviderBySlug(streamInfo.serviceTag) : undefined;
  const providerTinySegmentThreshold = providerModule?.tinySegmentThreshold ?? TINY_SEGMENT_COUNT_TRIGGER;

  // Segment staleness timeout. When no new segments have been produced for this duration, the capture pipeline is considered dead even though the video element may
  // appear healthy. This catches the case where Chrome's MediaRecorder silently stops emitting data without raising an error - the input stream stays "open" but no
  // data events fire. The 20-second threshold is 4x the maximum expected moof delivery interval (5 seconds) to avoid false positives during normal bursty delivery.
  const SEGMENT_STALENESS_TIMEOUT = 20000;

  // The capture surface: the size capture encodes at, reported beside the source's own size in the status the monitor emits. Read once for the monitor's lifetime,
  // because the quality preset is restart-gated and cannot change while a stream is running, so re-deriving it on every two-second tick would be work that can only
  // ever produce the same answer.
  const presetViewport = getPresetViewport(CONFIG);

  // Resolution degradation detection. When the video element's intrinsic resolution falls well below the best the stream has delivered, the service's ABR is stuck
  // on a low rendition. The threshold is a fraction of that best reading by pixel area. The field's stuck renditions read 16 to 27 percent of their peaks, while
  // an ordinary one-rung adaptive downshift - 1600x900 to 1024x576 on CNN, 41 percent by area - is a service pacing its own bitrate and heals on its own. A third
  // sits between the two: every stuck rendition on record fails it, a one-rung dip passes it, and a source whose best is 720p is never judged against a surface
  // it cannot fill. Each recovery this detector drives is a capture restart, so the line is drawn where the picture has actually collapsed, not merely shrunk.
  const RESOLUTION_RATIO_THRESHOLD = 1 / 3;

  // Grace period in milliseconds after stream start and after each recovery action. Gives ABR time to ramp up before flagging degradation.
  const RESOLUTION_GRACE_PERIOD = 30000;

  // Number of consecutive degraded readings required before triggering recovery. At ~2 seconds per monitor tick, 15 readings = ~30 seconds of sustained
  // degradation. This lets transient ABR dips (commercial breaks, ad transitions) self-heal without unnecessary page reloads.
  const RESOLUTION_DEGRADED_COUNT_THRESHOLD = 15;

  // Fixed margin in milliseconds before the maxContinuousPlayback limit at which a proactive reload is triggered. Two minutes provides enough time for page
  // navigation and video reinitialization to complete before the site enforces its cutoff.
  const PROACTIVE_RELOAD_MARGIN_MS = 120000;

  // Timestamp of the most recent full page navigation. Used to calculate elapsed continuous playback for proactive reload when maxContinuousPlayback is configured.
  // Initialized to Date.now() because the monitor starts immediately after stream setup establishes playback, meaning a page load just completed. Reset
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

  /* Whether the ladder can offer tab replacement at this moment. A replacement starts a fresh capture, which a browser marked as unable to start captures refuses,
   * and the handler disposes the working pipeline before it asks - so on a marked browser the ladder treats replacement as unavailable and every decline site keeps
   * the behavior it already has for a stream that was given no handler. The mark is read on each decision rather than captured once, because it can land at any
   * point in a stream's life.
   */
  const canReplaceTab = (): boolean => (onTabReplacement !== undefined) && (deps.getCaptureImpairment() === null);

  // The reason a replacement cannot proceed, put into words in one place so every message that has to explain a decline says the same thing.
  const tabReplacementUnavailableReason = (): string => (onTabReplacement ? "the browser can no longer start captures" : "no tab replacement handler");

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
  // quality issue - the stream works but at lower than the best it has delivered. Uses its own tracking and two-step escalation: page reload, then tab replacement.
  const resolutionState: ResolutionState = {

    consecutiveDegradedReadings: 0,
    graceEnd: Date.now() + RESOLUTION_GRACE_PERIOD,
    peak: null,
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

    // The narrow happens here rather than being inherited from the tick's dispatch: a narrowing does not cross a function call, and the entry's identity can be
    // replaced by a fallback that ran between the dispatch and this frame. A stream that is no longer native has nothing here to check.
    const identity = entry.identity;

    if(identity.mode !== "native") {

      emitStatusUpdate();

      return;
    }

    const proxy = identity.nativeProxy;
    const now = Date.now();
    const currentSegmentIndex = proxy.getSegmentIndex();
    const lastSegmentTime = proxy.getLastSegmentTime();
    const targetDuration = proxy.getTargetDuration();
    const consecutiveErrors = proxy.getConsecutiveErrors();
    const storeKey = entry.info.storeKey;

    // Fast path: if the proxy hit its error threshold and stopped itself, trigger L3 fallback immediately. This avoids waiting for the staleness threshold when hard
    // errors (HTTP 403, network failures) have already been detected by the proxy's internal retry loop.
    if(proxy.hasErrored()) {

      nativeHealthState.issueType = "proxy error";
      nativeHealthState.issueTime = now;

      if(deferFallbackInsideGrace(entry, identity)) {

        return;
      }

      LOG.debug("native:monitor", "Native proxy errored for %s. Initiating capture fallback.", storeKey);

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

    /* Advance the recorded issue through the pure rule in recovery.ts: an unchanged cause keeps its original start time across ticks, and a changed cause takes
     * both the new label and a fresh stamp. A stream that degrades from fetch errors into a segment stall therefore reports the stall it is actually in.
     */
    const issueRecord = nextNativeIssueRecord(nativeHealthState, decision.issueType, now);

    nativeHealthState.issueTime = issueRecord.issueTime;
    nativeHealthState.issueType = issueRecord.issueType;

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

    // L3: at 4x target duration once an L2 attempt has already been made and the stall persists, fall back to capture mode via tab replacement.
    if(decision.action === "l3") {

      if(deferFallbackInsideGrace(entry, identity)) {

        return;
      }

      LOG.warn("Falling back to capture mode for %s: native streaming stalled after recovery attempt.", storeKey);

      // The caller establishes the stream context for this interval tick, so the fire-and-forget recovery promise inherits it across its async continuations.
      void executeNativeL3Fallback(entry);

      return;
    }

    emitNativeStatus(entry, identity, decision.health);
  }

  /**
   * Holds a native capture fallback at the decision site while the recovery grace window is open, reporting the stream as recovering instead of entering the
   * fallback cycle. Every native trigger - a relay that stopped itself on its error threshold, a stall the ladder has escalated to its last rung - describes a
   * condition that holds on every tick once it is true. Without this read the whole cycle - the mode pre-flip, the attempt, the revert, the window sync, the
   * warning - would run twice a second inside a window every other trigger is respecting. Nothing is lost by waiting, since neither condition is curing itself in
   * the meantime, and the status still goes out so the display keeps advancing. The replacement primitive's own entry gate stays the guarantee beneath this: a
   * gate here can be forgotten, that one cannot be bypassed.
   *
   * The status is recovering for every trigger, because a fallback being held is a recovery in progress from the display's point of view.
   *
   * @param entry - The stream registry entry.
   * @param identity - The stream's native identity, narrowed by the caller.
   * @returns True when the fallback was held, which is the caller's signal to return without entering it.
   */
  function deferFallbackInsideGrace(entry: StreamRegistryEntry, identity: NativeStreamIdentity): boolean {

    if(!isWithinRecoveryGrace()) {

      return false;
    }

    LOG.debug("native:monitor", "Capture fallback for %s waits for the recovery grace window to close.", entry.info.storeKey);

    emitNativeStatus(entry, identity, "recovering");

    return true;
  }

  /**
   * Emits a status update with native-specific health classification. Populates meaningful fields (health, issue tracking, memory, clients) and zeroes video-specific
   * fields that are not applicable to native streams.
   *
   * @param entry - The stream registry entry.
   * @param identity - The stream's native identity, narrowed by the caller so the manifest quality members are read without a second mode test.
   * @param health - The health status to report.
   */
  function emitNativeStatus(entry: StreamRegistryEntry, identity: NativeStreamIdentity, health: StreamHealthStatus): void {

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
      captureCodec: identity.captureCodec,
      captureResolution: null,
      channel: streamInfo.channelName,
      clientCount: clientSummary.total,
      clients: clientSummary.clients,
      currentTime: 0,
      duration: Math.round((now - streamInfo.startTime.getTime()) / 1000),
      escalationLevel: escalation,
      hardwareAccelerated: isHardwareAccelerated(entry),
      health,
      id: streamInfo.numericStreamId,
      lastIssueTime: nativeHealthState.issueTime,
      lastIssueType: nativeHealthState.issueType,
      lastRecoveryTime: null,
      logoUrl: channelKey ? (getChannelLogo(channelKey) ?? "") : "",
      memoryBytes,
      nativeBandwidth: identity.nativeBandwidth,
      nativeResolution: identity.nativeResolution,
      networkState: 0,
      pageReloadsInWindow: 0,
      readyState: 0,
      recoveryAttempts: nativeHealthState.recoveryAttempts,
      serviceName: streamInfo.serviceName,
      showName: getShowName(streamInfo.numericStreamId),
      sourceResolution: null,
      startTime: streamInfo.startTime.toISOString(),
      streamingMode: identity.mode,
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

    // The narrow is taken fresh here, not inherited from the caller: this runs as a fire-and-forget continuation, and a stream that has since fallen back to
    // capture has no manifest left to refresh.
    const identity = entry.identity;

    if((identity.mode !== "native") || identity.nativeProxy.isStopped()) {

      return;
    }

    const proxy = identity.nativeProxy;

    /* The refresh probes under the identity the tune established, which the entry holds. It is null only for a pending entry whose setup has not filled it in,
     * and a stream with a running native proxy is past that point - so this answers a window that recovery does not reach rather than papering over one.
     * Declining is the answer either way: an identity assembled here would stamp a binding this frame cannot see, and the stall escalates to L3.
     */
    const probeIdentity = entry.probeIdentity;

    if(!probeIdentity) {

      LOG.debug("native:monitor", "Skipping L2 recovery for %s: the stream has no probe-cache identity yet.", entry.info.storeKey);

      return;
    }

    // The refresh's page-reload strategy re-establishes the channel through the capability the stream's setup built. Every write that produces a native identity
    // supplies it, which the type states, so this frame reads it rather than testing for it.
    const reestablishManifest = identity.reestablishManifest;

    recoveryState.inProgress = true;

    LOG.debug("native:monitor", "Starting L2 recovery (page reload) for %s.", entry.info.storeKey);

    try {

      const success = await refreshNativeManifest({

        channelName: entry.info.storeKey,
        onFeedApplied: (metadata) => {

          // The registry write belongs to this layer, not to the native one: recovery already holds the entry, so a refresh that binds a different rung of the
          // service's ladder is recorded here as the stream's current quality.
          applyNativeQualityRefresh(entry, metadata);
        },
        page: currentPage,
        probeIdentity,
        proxy,
        reestablishManifest,
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
   * The relay keeps producing throughout the attempt and is stopped only on the exits that hand the stream to something else, so a fallback that cannot establish
   * its capture leaves the stream exactly as it found it rather than stranded between two modes.
   *
   * If the proxy's onError fires concurrently (from the poll loop hitting its failure threshold), terminateStream runs before this async function gets a chance to
   * execute. By the time L3 runs, the stream is already terminated and executeTabReplacement reports the stop.
   *
   * @param entry - The stream registry entry.
   */
  async function executeNativeL3Fallback(entry: StreamRegistryEntry): Promise<void> {

    // The whole identity this fallback is leaving, held in one local. The narrow is taken fresh here because a narrowing does not cross a call, and the failed
    // arm restores this same object rather than reconstructing it member by member.
    const previousIdentity = entry.identity;

    if(previousIdentity.mode !== "native") {

      return;
    }

    if(!canReplaceTab()) {

      LOG.warn("Capture fallback not available for %s: %s.", entry.info.storeKey, tabReplacementUnavailableReason());
      onCircuitBreak();

      return;
    }

    LOG.debug("native:monitor", "Starting L3 fallback (capture mode) for %s.", entry.info.storeKey);

    /* Declare capture mode before the replacement rather than after it. The window-visibility policy reads the registry, and createPageWithCapture syncs the window
     * at the top of establishment - so an entry still marked native there would let the policy minimize the window under a capture that is being acquired. Every
     * outcome below either keeps this or reverts it, so the entry never rests on a mode it is not in.
     *
     * The proxy is deliberately left running behind that declaration, right through the attempt. The replacement builds its capture on a new page and does not
     * touch the old one until the swap, so there is nothing for a polling proxy to trip over, and a fallback that fails hands back a stream that never stopped
     * relaying. Its token-refresh timer keeps running too: a refresh landing mid-window is ordinary native operation, and the registry's quality write skips it
     * because the entry is holding a capture identity for the duration.
     */
    entry.identity = makePendingCaptureIdentity();

    // Use the existing tab replacement infrastructure. It sets recoveryState.inProgress = true internally and clears it in finalizeTabReplacement. It creates a new
    // page with capture, navigates, sets up playback, creates a segmenter, and swaps the registry entry over to the new page and capture session.
    const outcome = await executeTabReplacement("native fallback to capture");

    /* An exhaustive switch rather than a chain, so a future outcome cannot be absorbed silently into a catch-all. No rejection handling wraps the await above:
     * executeTabReplacement converts every throw into an outcome in its own catch, so there is no rejection to arm for.
     */
    switch(outcome.outcome) {

      case "success": {

        /* The handler set the page and the new capture session (with its segmenter attached) on the entry, and capture mode was declared before the replacement,
         * so what remains is the state the native path left behind.
         *
         * Clear separate audio state from the native proxy. Without this, hasAudio remains true and the HLS handler continues serving the master playlist
         * (referencing video.m3u8 and audio.m3u8) instead of the capture segmenter's variant playlist. Clients that cached the master playlist structure would
         * request stale audio and video variant playlists pointing to segments that are no longer being updated.
         */
        entry.hls.hasAudio = false;
        entry.hls.audioPlaylist = "";
        entry.hls.audioSegments.clear();
        entry.hls.audioSegmentBytes = 0;
        entry.hls.videoPlaylist = "";

        // Release the relay's initialization segments. They belong to a source this stream no longer consumes, and the memory report reads their byte counter with
        // no mode gate, so state left here would be counted for the rest of the stream's life. The nativeContainer label needs no matching reset: it lives on the
        // native identity this entry no longer holds.
        clearNativeInitState(entry.id);

        // Clear the probe cache so subsequent tunes to this channel don't re-attempt native streaming.
        clearProbeCache(entry.info.storeKey);

        /* Re-derive the codec facts from the capture decision. The identity the handler wrote carried forward what the pre-swap identity held, which on this path
         * is the pending shape the pre-flip installed - and before that, the label read off the service's manifest, which describes a feed this stream is no
         * longer consuming. The status display would otherwise report that stale label for the rest of the stream's life. This is the one sanctioned within-variant
         * spread: the capture session and the page the handler installed are left exactly as they are.
         */
        if(isCaptureIdentity(entry)) {

          entry.identity = { ...entry.identity, captureCodec: deps.getEffectiveCaptureCodec().toUpperCase(), hardwareAccelerated: deps.isCaptureHardwareAccelerated() };
        }

        LOG.info("Switched to capture mode for %s: native streaming failed.", entry.info.storeKey);

        // The monitor's next tick sees a capture identity and runs the normal video element monitoring path. The state reset from applyTabReplacementSuccess
        // (called by executeTabReplacement) already initialized all capture-mode monitor variables.
        break;
      }

      case "failed": {

        /* The replacement did not take, but the stream continues un-terminated - and because nothing was disposed before the swap, what it continues on is the
         * native relay it has been running all along. Restoring the held identity whole hands back the same live proxy, and the window sync lets the presentation
         * settle now that no capture is being attempted.
         *
         * The restore is correct in both entry conditions. A proxy that is still healthy simply resumes. A proxy that had already stopped itself on its error
         * threshold re-triggers this fallback on a later tick - deliberately: the grace window throttles each cycle and the circuit breaker bounds the count, so a
         * native stream whose relay died and whose capture fallback keeps failing escalates to termination rather than resting in a silent limbo.
         */
        entry.identity = previousIdentity;

        void deps.syncWindowVisibility();

        LOG.warn("Capture fallback failed for %s: tab replacement unsuccessful.", entry.info.storeKey);

        break;
      }

      case "deferred": {

        /* The replacement was declined before it started because the previous recovery's window is still open. Nothing was attempted, so the revert is the same one
         * the failed arm performs and the narration is a breadcrumb rather than a warning.
         *
         * No live native path reaches this arm: every native trigger holds its fallback at the decision site while the window is open, and nothing runs between
         * that read and the primitive's own that could open a window. The arm stays because the outcome union is shared with the capture triggers and the switch
         * is exhaustive by design.
         */
        entry.identity = previousIdentity;

        void deps.syncWindowVisibility();

        LOG.debug("native:monitor", "Capture fallback for %s deferred inside the recovery grace window.", entry.info.storeKey);

        break;
      }

      case "stopped": {

        // The monitor stopped while the replacement was in flight, which means the stream terminated. Termination disposed what the registry held - the pending
        // capture identity, which holds nothing - so the proxy is this frame's to release.
        break;
      }

      case "terminated": {

        // The circuit breaker tripped during the replacement and terminated the stream synchronously, again while the registry held the pending identity, so
        // nothing else can reach the proxy.
        LOG.warn("Capture fallback failed for %s: circuit breaker tripped.", entry.info.storeKey);

        break;
      }
    }

    /* One stop site owns the whole leak matrix. The two arms that put the held identity back are the two that hand the proxy on to a stream that is still using
     * it; every other exit is the end of this proxy's life - the capture pipeline has taken over, or the stream is gone and took nothing with it - and an
     * unstopped proxy would go on polling and refreshing forever.
     */
    if((outcome.outcome !== "failed") && (outcome.outcome !== "deferred")) {

      previousIdentity.nativeProxy.stop();
    }
  }

  // Helper to mark a discontinuity in the HLS playlist after recovery events that disrupt the video source. The segmenter flushes its current fragment buffer and sets
  // a pending discontinuity flag so the next segment boundary includes an #EXT-X-DISCONTINUITY tag. This tells HLS clients to flush their decoder state.
  const markStreamDiscontinuity = (): void => {

    getStreamSegmenter(getStream(streamInfo.numericStreamId))?.markDiscontinuity();
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

    // An entry that is gone projects as the capture defaults, which is the same shape the flat wire carries for a stream whose setup has not filled it in yet.
    const identity = entry?.identity;
    const nativeIdentity = (identity?.mode === "native") ? identity : null;

    // The two capture-only sizes are null for a native entry: the entry's mode is the contract, not the builder.
    const captureStream = identity?.mode !== "native";
    const sourceResolution = (captureStream && lastVideoState && (lastVideoState.videoWidth > 0) && (lastVideoState.videoHeight > 0)) ?
      formatResolution(lastVideoState.videoWidth, lastVideoState.videoHeight) : null;
    const captureResolution = captureStream ? formatResolution(presetViewport.width, presetViewport.height) : null;

    const status: StreamStatus = {

      bufferingDuration: bufferingStartTime ? Math.round((now - bufferingStartTime) / 1000) : null,
      captureCodec: identity?.captureCodec ?? null,
      captureResolution,
      channel: streamInfo.channelName,
      clientCount: clientSummary.total,
      clients: clientSummary.clients,
      currentTime: lastVideoState?.time ?? 0,
      duration: Math.round((now - streamInfo.startTime.getTime()) / 1000),
      escalationLevel: recoveryState.escalationLevel,
      hardwareAccelerated: entry ? isHardwareAccelerated(entry) : false,
      health: computeHealthStatus(),
      id: streamInfo.numericStreamId,
      lastIssueTime,
      lastIssueType,
      lastRecoveryTime: recoveryState.lastRecoveryTime > 0 ? recoveryState.lastRecoveryTime : null,
      logoUrl: channelKey ? (getChannelLogo(channelKey) ?? "") : "",
      memoryBytes,
      nativeBandwidth: nativeIdentity?.nativeBandwidth ?? 0,
      nativeResolution: nativeIdentity?.nativeResolution ?? null,
      networkState: lastVideoState?.networkState ?? 0,
      pageReloadsInWindow: pageReloadTimestamps.length,
      readyState: lastVideoState?.readyState ?? 0,
      recoveryAttempts: recoveryState.totalAttempts,
      serviceName: streamInfo.serviceName,
      showName: getShowName(streamInfo.numericStreamId),
      sourceResolution,
      startTime: streamInfo.startTime.toISOString(),
      streamingMode: identity?.mode ?? "capture",
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
    segmentState.lastCheckedIndex = getStreamSegmenter(getStream(streamInfo.numericStreamId))?.getSegmentIndex() ?? 0;
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
   * Resets the resolution monitoring a recovery action makes stale: the degraded-reading count, the grace window, and the ladder step. Deliberately not the peak
   * record - the source's rendition ladder does not change with a reload or a tab replacement, and the record's accepted flag is what keeps an unrelated recovery
   * from re-running the resolution ladder inside one degraded episode.
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
   * Sets the recovery grace period after a recovery action. The grace period prevents the monitor from immediately detecting new issues while the recovery action
   * takes effect.
   * @param level - The recovery level (1-3) to determine grace period duration.
   */
  function setRecoveryGracePeriod(level: number): void {

    recoveryState.graceUntil = Date.now() + (recoveryGracePeriods[level] ?? 0);
  }

  /**
   * Reports whether the monitor is inside the post-recovery grace window. One state, read here by every consumer that has to honour it: the tick computes the
   * value it threads into the health checks from this, and the triggers that sit outside that thread - the tiny-segment gate, the unresponsive-tab gate, the
   * native fallback triggers through deferFallbackInsideGrace, and the replacement primitive's own entry gate - call it directly. A second way of asking the
   * same question is how a trigger ends up escalating inside a window every other trigger is respecting.
   * @returns True while the grace window from the last recovery action is still open.
   */
  function isWithinRecoveryGrace(): boolean {

    return Date.now() < recoveryState.graceUntil;
  }

  /**
   * Stops the monitoring interval. Pairs the two operations that must always happen together: setting intervalCleared so any in-flight async tick short-circuits at
   * its next stop check, and clearing the interval so no further ticks fire. Every path that stops the monitor - the tick's own guards, the circuit-breaker
   * terminations, and dispose() - routes through here, so a stopped monitor can never look un-stopped to a resuming await, which would go on to act on a stream
   * that has already been terminated.
   */
  function stopMonitoring(): void {

    intervalCleared = true;
    clearInterval(interval);
  }

  /**
   * Reports whether the monitor stopped while a recovery action was awaiting, and releases the recovery flag when it did. A recovery await can resume after
   * terminateStream has already disposed this monitor; the resumption must then apply nothing at all - no grace window, no discontinuity mark, no context
   * adoption, no counter reset, no navigation-failure tally, no status emission - because each of those would describe a stream that is already gone.
   *
   * Releasing recoveryState.inProgress is this helper's own job for exactly that reason: all five callers raise the flag before their await and lower it on the
   * way out, and the resumption they skip is where that lowering would otherwise have happened. executeTabReplacement is not one of them - its finally already
   * owns the release.
   * @returns True if the monitor stopped and the caller must apply nothing.
   */
  const abandonRecoveryIfStopped = (): boolean => {

    if(!intervalCleared) {

      return false;
    }

    recoveryState.inProgress = false;

    return true;
  };

  /**
   * Tab replacement result type. Indicates whether the replacement succeeded, failed (but stream continues), terminated (circuit breaker tripped), was abandoned
   * because the monitor stopped while the replacement was in flight (stopped - nothing was applied and the caller must not act on it), or was declined before it
   * started because the previous attempt's grace window is still open (deferred - nothing was attempted, no attempt was counted, and the trigger will be back).
   *
   * Deferred is its own arm rather than a shade of failed because the difference matters to every consumer's narration: a failure is evidence about the stream and
   * warrants a warning and a breaker count, while a deferral is the throttle working as designed and warrants neither.
   */
  type TabReplacementOutcome = { outcome: "success" } | { outcome: "failed" } | { outcome: "terminated" } | { outcome: "stopped" } | { outcome: "deferred" };

  /**
   * Handles tab replacement failure by checking the circuit breaker. If the breaker trips, terminates the stream. Returns the appropriate outcome for the caller.
   * @param context - Description of the failure for logging.
   * @returns The tab replacement outcome (failed or terminated).
   */
  function handleTabReplacementFailure(context: string): TabReplacementOutcome {

    // Entry stop check, mirroring the tick's intervalCleared-first ordering (see the main interval's check-order note): a settlement that resumed after the monitor
    // stopped must return the stopped outcome and trip nothing, because the breaker accounting, the logs, and the termination would all describe a stream that
    // has already ended.
    if(intervalCleared) {

      return { outcome: "stopped" };
    }

    const cbResult = checkCircuitBreaker(circuitBreaker, Date.now());

    if(cbResult.shouldTrip) {

      LOG.error("Recovery exhausted (%s) - terminating stream.", context);

      stopMonitoring();
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
   * deferred-success check), arms the grace window that throttles the next attempt while one can still start, runs the circuit breaker, and catches the one state
   * that still strands a stream.
   * @param context - Description of the failure for circuit breaker logging.
   * @returns The tab replacement outcome (failed or terminated).
   */
  function handleExhaustedTabReplacement(context: string): TabReplacementOutcome {

    // Entry stop check, mirroring the tick's intervalCleared-first ordering (see the main interval's check-order note): a settlement that resumed after the monitor
    // stopped must return the stopped outcome without clearing metrics or tripping the breaker, because both would be bookkeeping for a stream that has already
    // ended.
    if(intervalCleared) {

      return { outcome: "stopped" };
    }

    // Clear stale recovery metrics so the deferred-success check does not falsely log "Recovered" from leftover state set by recordRecoveryAttempt.
    metrics.currentRecoveryStartTime = null;
    metrics.currentRecoveryMethod = null;

    LOG.warn("Tab replacement was unsuccessful. The stream continues on its existing capture.");

    /* Arm the grace window on the way out, the same window a success arms. A failed replacement leaves the stream running on its old pipeline, still exhibiting
     * whatever condition triggered the attempt - so without this the very next tick re-satisfies that condition and fires again, and a persistently degraded
     * stream spends its life in a replacement loop. With it, the cadence is one real attempt per window (ten seconds at this level, from the monitor's grace
     * table), each true failure counted by the circuit breaker, ending in termination when the breaker's threshold trips. Bounded escalation rather than a tight
     * loop, and no log line per suppressed tick.
     *
     * The arming reads whether a replacement can still start, because on a browser that can start none there is no replacement loop here to throttle: every
     * trigger reaches its no-replacement arm rather than the handler. Holding the arms that terminate for a window holds every other recording's re-tune behind
     * this stream, because the relaunch that cures the browser waits on the last stream's end. A failure that itself marked the browser lands the mark before the
     * exhaustion settles, so the read here sees it.
     */
    if(canReplaceTab()) {

      setRecoveryGracePeriod(3);
    }

    const failureOutcome = handleTabReplacementFailure(context);

    /* The safety net for the one state that still strands a stream: termination raced the exhaustion and took the page while this settled. A replacement that
     * simply failed leaves the old page open and serving, so this does not fire for it - that stream continues on its existing capture with the breaker counting,
     * which is the designed outcome. When the page really is gone, the next tick would silently clear the interval via currentPage.isClosed() with no termination
     * log, no status emission, and no cleanup, leaving a zombie entry in the registry. Terminate explicitly instead.
     */
    if((failureOutcome.outcome === "failed") && currentPage.isClosed()) {

      LOG.error("Tab replacement failed and the original page is no longer available - terminating stream.");

      stopMonitoring();
      onCircuitBreak();

      return { outcome: "terminated" };
    }

    return failureOutcome;
  }

  /**
   * Executes tab replacement recovery with full error handling. This unified helper handles all tab replacement triggers (tiny segments, stalled capture, unresponsive
   * tab, resolution degradation, native fallback) consistently, including metrics recording, success/failure logging, circuit breaker checks, and state resets.
   *
   * The handler builds the replacement capture before disposing anything: it creates a fresh page, acquires its capture, and tunes it while the existing pipeline
   * keeps producing, and only then swaps in one synchronous frame. So a failed attempt costs the stream nothing - the old capture is still running and still
   * serving the recording - and the single retry below is a second chance rather than the last one. The retry re-reads continuity from the still-live old
   * segmenter, and its own fresh resources are discarded on failure the same way the first attempt's were.
   *
   * Every entry runs through the grace check below, which is what keeps a persistently degraded stream from spending its life re-attempting.
   * @param issueType - Description of what triggered the replacement (for logging and UI display).
   * @returns The tab replacement outcome.
   */
  async function executeTabReplacement(issueType: string): Promise<TabReplacementOutcome> {

    // Guard: the caller has already consulted canReplaceTab, but TypeScript needs the explicit narrowing for the awaited calls below.
    if(!onTabReplacement || !canReplaceTab()) {

      return { outcome: "failed" };
    }

    /* The throttle, enforced at the one place every trigger passes through. Individual triggers consult the grace window in their own gates so their warnings and
     * side effects stay quiet too, but a gate can be forgotten and a new trigger can be added, whereas this cannot be bypassed by construction. It sits ahead of
     * every piece of bookkeeping - before the attempt metric, before the recovery flags, before the handler runs - so a deferral costs nothing and is
     * indistinguishable from the attempt never having been asked for.
     */
    if(isWithinRecoveryGrace()) {

      LOG.debug("recovery:tab", "Deferring the %s tab replacement: the previous recovery's grace window is still open.", issueType);

      return { outcome: "deferred" };
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

      // First attempt failed - retry once. See the safe-to-retry notes in the JSDoc above.
      if(!result) {

        LOG.debug("recovery:tab", "Tab replacement attempt 1/2 failed. Retrying...");

        try {

          result = await onTabReplacement();
        } catch(retryError) {

          LOG.debug("recovery:tab", "Tab replacement attempt 2/2 failed: %s.", formatError(retryError));
        }
      }

      if(result) {

        // The stream can terminate during the awaited onTabReplacement above; a settlement that resumed after the monitor stopped must not adopt the fresh capture
        // resources or reset recovery state. Report the stop and apply nothing - the fresh page and capture session were already torn down at the source by the
        // hls.ts handler's own post-await re-check, so there is nothing to clean up here.
        if(intervalCleared) {

          return { outcome: "stopped" };
        }

        applyTabReplacementSuccess(result);

        return { outcome: "success" };
      }

      return handleExhaustedTabReplacement("tab replacement unsuccessful");
    } catch(error) {

      // Unexpected error (not from onTabReplacement - those are caught internally by the handler in hls.ts and return null). Guard against registry corruption,
      // capture-acquisition failures, or other unexpected errors.
      LOG.debug("recovery:tab", "Tab replacement attempt 1/2 failed: %s. Retrying...", formatError(error));

      try {

        const retryResult = await onTabReplacement();

        if(retryResult) {

          // Same post-await stop check as the try path: a settlement that resumed after the monitor stopped applies nothing and reports the stop.
          if(intervalCleared) {

            return { outcome: "stopped" };
          }

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

      // Use tuneToChannel to reinitialize playback. It is the tune this recovery path owns, and it runs the phases stream setup ran - navigation, channel
      // selection, video detection, click-to-play, playback - so a recovered stream comes up the way a fresh one did.
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

      // The presence check answering is direct evidence the tab runs evaluates, and the timeout streak both drives the tab-replacement threshold and arms the
      // short confirmation probe, so it must not outlive that evidence.
      consecutiveTimeouts = 0;

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

          // A validated video means the tab answered the re-search evaluates, and the timeout streak both drives the tab-replacement threshold and arms the
          // short confirmation probe, so it must not outlive that evidence.
          consecutiveTimeouts = 0;

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

    // Post-await stop check guarding both breaker branches below. The only await on the path to here is checkVideoPresence above; a tick that resumed after the
    // monitor stopped must neither trip the breaker nor navigate, because the stream both would act on has already been terminated. No await separates the two
    // breaker branches, so this single check makes both inert.
    if(intervalCleared) {

      return;
    }

    // Check circuit breaker for too many failures.
    const cbResult = checkCircuitBreaker(circuitBreaker, now);

    if(cbResult.shouldTrip) {

      LOG.error("Recovery failed after %s attempts - terminating stream.", cbResult.totalCount);

      stopMonitoring();
      onCircuitBreak();

      return;
    }

    // Set escalation to level 3 to trigger page navigation. We skip lower levels since they require a video element.
    // Note: Keep state updates in sync with the main recovery path in executeRecoveryAction below.
    recoveryState.escalationLevel = 3;
    recoveryState.lastRecoveryTime = now;
    recoveryState.totalAttempts++;
    recoveryState.inProgress = true;

    recordRecoveryAttempt(metrics, RECOVERY_METHODS.pageNavigation);

    // Check page reload limit before attempting recovery.
    if(!isPageReloadAllowed()) {

      LOG.error("Page navigation rate limit reached (%s in %s minutes) - cannot recover without video element.",
        CONFIG.playback.maxPageReloads, Math.round(CONFIG.playback.pageReloadWindow / 60000));

      stopMonitoring();
      onCircuitBreak();

      return;
    }

    const recoveryResult = await performPageNavigationRecovery();

    // The stream can terminate while the navigation above runs. A resumption that lands after the monitor stopped applies nothing - the discontinuity mark, the
    // grace window, the context adoption, and the failure tally below would all describe a stream that was torn down at the source.
    if(abandonRecoveryIfStopped()) {

      return;
    }

    // Page navigation disrupted the video stream. Mark a discontinuity regardless of navigation success so HLS clients resynchronize their decoders.
    markStreamDiscontinuity();

    // Open the grace window that gives page navigation time to take effect (L3 = 10 seconds). The helper reads the clock here, so the window is measured from
    // the completed recovery action rather than from the tick that began it.
    setRecoveryGracePeriod(3);

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
      const currentIndex = getStreamSegmenter(entry)?.getSegmentIndex() ?? null;

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
    const currentSegmentIndex = getStreamSegmenter(sizeCheckEntry)?.getSegmentIndex() ?? 0;

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

        /* Act on sustained undersized segments, but not inside the post-recovery grace window. The grace term is what keeps a stream that a replacement did not
         * cure from re-escalating every two seconds: a fresh capture needs its window to start producing real segments, and a stream still emitting tiny ones
         * when the window closes will satisfy this again immediately. Suppressing the whole branch - the warning and the circuit-breaker fallback alongside the
         * replacement - is deliberate, and mirrors what the staleness branch below has always done with its own grace term. The counter keeps advancing
         * meanwhile, so the evidence is not lost, only the reaction is deferred.
         */
        if((segmentState.consecutiveTinySegments >= effectiveThreshold) && !withinRecoveryGrace) {

          LOG.warn("Detected %d consecutive undersized segments (%dKB) - capture pipeline may have stalled.",
            segmentState.consecutiveTinySegments, Math.round(segmentSize / 1024));

          // Replace the tab when a replacement can start, and otherwise raise the stalled-production flag. The recovery action reads that flag on this same tick
          // and terminates the stream, because no replacement can be started and the in-page ladder cannot revive a dead capture.
          if(canReplaceTab() && !recoveryState.inProgress) {

            await executeTabReplacement("tiny segments");

            return true;
          } else if(!canReplaceTab()) {

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

      if(canReplaceTab() && !recoveryState.inProgress) {

        await executeTabReplacement("segment staleness");

        return true;
      } else if(!canReplaceTab()) {

        segmentState.productionStalled = true;
      }
    }

    return false;
  }

  /**
   * Monitors video resolution against the best resolution the stream has delivered and triggers recovery for sustained ABR degradation. Uses a two-step escalation:
   * page reload (forces ABR restart), then tab replacement (fresh page with new network connections). Acceptance after both attempts is recorded on the peak record
   * and ends with the episode, so the ladder runs at most once per level the source has demonstrated rather than looping on content that is simply low-resolution.
   * Also detects and logs resolution restoration after successful recovery.
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

    const reading = { height: state.videoHeight, width: state.videoWidth };
    const peak = updateResolutionPeak({ peak: resolutionState.peak, reading });

    resolutionState.peak = peak;

    const isDegraded = isResolutionDegraded({ peak, reading, threshold: RESOLUTION_RATIO_THRESHOLD });

    /* The picture is back at or near its peak: the episode is over, whatever the ladder had reached. Say so once, then clear every trace of the episode - the
     * accepted flag included - so a later drop starts a fresh ladder. The method the message names is read from the ladder step, which an unrelated recovery may
     * have zeroed while the acceptance stood, so the sentence stays truthful in every case.
     */
    if(!isDegraded) {

      if(peak.accepted || (resolutionState.recoveryAttempt > 0)) {

        const method = (resolutionState.recoveryAttempt === 1) ? "page reload" :
          ((resolutionState.recoveryAttempt === 2) ? "tab replacement" : "an unrelated recovery");
        const verb = describeResolutionOutcome({ peak, reading });

        LOG.info("Video resolution %s to %s\u00d7%s after %s.", verb, String(state.videoWidth), String(state.videoHeight), method);

        resolutionState.consecutiveDegradedReadings = 0;
        resolutionState.graceEnd = 0;
        resolutionState.peak = { ...peak, accepted: false };
        resolutionState.recoveryAttempt = 0;
      }

      return false;
    }

    // A ladder that already ran to acceptance at this peak does not run again inside the same episode: the source has shown nothing better since.
    if(peak.accepted) {

      return false;
    }

    if(now >= resolutionState.graceEnd) {

      resolutionState.consecutiveDegradedReadings++;

      LOG.debug("recovery:resolution", "Video resolution: %s\u00d7%s (peak: %s\u00d7%s, area: %s%%, consecutive: %s/%s).",
        String(state.videoWidth), String(state.videoHeight), String(peak.width), String(peak.height),
        String(Math.round(100 * resolutionAreaRatio({ peak, reading }))),
        String(resolutionState.consecutiveDegradedReadings), String(RESOLUTION_DEGRADED_COUNT_THRESHOLD));
    } else {

      resolutionState.consecutiveDegradedReadings = 0;
    }

    // Escalation step 1: page reload. Forces the service's ABR to restart quality negotiation. Only triggers after sustained degradation
    // (RESOLUTION_DEGRADED_COUNT_THRESHOLD consecutive readings) to let transient ABR dips self-heal.
    if((resolutionState.consecutiveDegradedReadings >= RESOLUTION_DEGRADED_COUNT_THRESHOLD) && (resolutionState.recoveryAttempt === 0)) {

      const degradedDuration = resolutionState.consecutiveDegradedReadings * 2;

      LOG.warn("Video resolution has been degraded for %ss (%s\u00d7%s against a %s\u00d7%s peak). Attempting recovery via %s.",
        String(degradedDuration), String(state.videoWidth), String(state.videoHeight),
        String(peak.width), String(peak.height), RECOVERY_METHODS.pageNavigation);

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

      const recoveryResult = await performPageNavigationRecovery();

      // Same post-await stop check as the other navigation sites: a resumption after the monitor stopped applies neither the grace windows nor the ratchet step
      // below, since both would be bookkeeping for a stream that is gone.
      if(abandonRecoveryIfStopped()) {

        return true;
      }

      markStreamDiscontinuity();

      // Arm the shared recovery grace window unconditionally, mirroring the proactive-reload sibling. This is the one navigation path that would otherwise leave it
      // unarmed, letting the post-reload settle-in feed the general stall ladder and fire a colliding second recovery a couple of ticks later. Unconditional because
      // even a failed reload has a settle window that must not read as a fresh stall. This is the shared recoveryState.graceUntil, distinct from the
      // resolutionState.graceEnd cooldown set just below.
      setRecoveryGracePeriod(3);

      resolutionState.consecutiveDegradedReadings = 0;
      resolutionState.recoveryAttempt = 1;
      resolutionState.graceEnd = now + RESOLUTION_GRACE_PERIOD;

      if(recoveryResult.success && recoveryResult.newContext) {

        currentContext = recoveryResult.newContext;
        lastPageNavigationTime = Date.now();

        // Reset the general-recovery state a successful reload made stale, composed for this site. Deliberately NOT resetResolutionState() (which the proactive-reload
        // sibling calls): that zeroes the recoveryAttempt ratchet set to 1 just above, collapsing the two-step resolution ladder into an endless step-1 loop. And
        // deliberately WITH resetEscalationState() (which the sibling omits): the sibling's caller guarantees a zero escalation level, but this path can run mid-cooldown
        // with a stale general-ladder level, and every ungated navigation sibling clears it.
        resetRecoveryCounters();
        resetSegmentMonitoringState();
        resetEscalationState();
      } else {

        LOG.warn("Resolution recovery via page reload unsuccessful.");
      }

      recoveryState.inProgress = false;

      emitStatusUpdate();

      return true;
    }

    // Escalation step 2: tab replacement. Creates a fresh page with new capture pipeline and network connections.
    if((resolutionState.consecutiveDegradedReadings >= RESOLUTION_DEGRADED_COUNT_THRESHOLD) && (resolutionState.recoveryAttempt === 1)) {

      if(canReplaceTab()) {

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

    /* Acceptance: still degraded after both recovery attempts. Log once and record the acceptance on the peak record, which is what holds the ladder for the rest
     * of this episode. The ladder step stays at 2 so the eventual restoration names the tab replacement, and the reading count is zeroed so no further step fires.
     */
    if((resolutionState.consecutiveDegradedReadings >= RESOLUTION_DEGRADED_COUNT_THRESHOLD) && (resolutionState.recoveryAttempt === 2)) {

      LOG.warn("Video resolution remains degraded (%s\u00d7%s against a %s\u00d7%s peak) after recovery attempts. The stream will continue at reduced quality.",
        String(state.videoWidth), String(state.videoHeight), String(peak.width), String(peak.height));

      resolutionState.consecutiveDegradedReadings = 0;
      resolutionState.peak = { ...peak, accepted: true };
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

      // Set a grace period so this deferral does not re-trigger every 2 seconds while the rate limit remains in effect. Level 3 is the bound a page navigation
      // would have taken, which is the action being deferred.
      setRecoveryGracePeriod(3);

      recoveryState.inProgress = false;

      emitStatusUpdate();

      return true;
    }

    const recoveryResult = await performPageNavigationRecovery();

    // Same post-await stop check as the other navigation sites: a resumption after the monitor stopped applies nothing, and the reload it was maintaining is
    // moot now that the stream is gone.
    if(abandonRecoveryIfStopped()) {

      return true;
    }

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

    // Post-await stop check, covering every exit below it. The tick awaited getVideoState before calling this; a tick that resumed after the monitor stopped must
    // trip nothing and attempt nothing, because the accounting, the log, the replacement, and the termination would all describe a stream that has already ended.
    // Return terminal so the tick exits without further work.
    if(intervalCleared) {

      return true;
    }

    /* Segment production stall handling. The capture pipeline is dead and the in-page ladder cannot revive a capture, so a replacement is the one cure. When none
     * can start, the stream is unrecoverable: every rung the ladder would run spends an attempt and part of the breaker's window on a stream it cannot save, while
     * holding open the relaunch that would cure a marked browser. So the stream terminates through the breaker at once, which is the judgment the unresponsive-tab
     * branch already makes.
     */
    if(segmentState.productionStalled) {

      if(canReplaceTab()) {

        LOG.warn("Capture pipeline still stalled - escalating to %s.", RECOVERY_METHODS.tabReplacement);

        await executeTabReplacement("capture pipeline stalled");

        return true;
      }

      LOG.error("Capture pipeline stalled and tab replacement is unavailable (%s) - terminating stream.", tabReplacementUnavailableReason());

      stopMonitoring();
      onCircuitBreak();

      return true;
    }

    // Check circuit breaker for too many failures.
    const cbResult = checkCircuitBreaker(circuitBreaker, now);

    if(cbResult.shouldTrip) {

      const elapsedSeconds = circuitBreaker.firstFailureTime ? Math.round((now - circuitBreaker.firstFailureTime) / 1000) : 0;

      LOG.error("Recovery failed after %s attempts in %ss - terminating stream.", cbResult.totalCount, elapsedSeconds);

      stopMonitoring();
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

      segmentState.preRecoveryIndex = getStreamSegmenter(entry)?.getSegmentIndex() ?? null;
      segmentState.waitStartTime = null;
    }

    recoveryState.inProgress = true;

    try {

      // Levels 1-2: In-page recovery via ensurePlayback().
      if(recoveryState.escalationLevel <= 2) {

        await ensurePlayback(currentPage, currentContext, profile, { recoveryLevel: recoveryState.escalationLevel, skipNativeFullscreen: true });

        // The stream can terminate while ensurePlayback runs. A resumption after the monitor stopped applies none of the settlement below - the source-reload
        // mark, the discontinuity, the grace window, the resolution reset. Returning true is what makes the caller exit the tick immediately.
        if(abandonRecoveryIfStopped()) {

          return true;
        }

        if(recoveryState.escalationLevel === 2) {

          recoveryState.sourceReloadAttempted = true;

          markStreamDiscontinuity();
        }

        setRecoveryGracePeriod(recoveryState.escalationLevel);
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

            // Same post-await stop check as the other navigation sites. The exit value here is true, not the false this function returns on normal completion:
            // false means "continue the tick", and a tick running against a terminated stream has nothing left to do.
            if(abandonRecoveryIfStopped()) {

              return true;
            }

            markStreamDiscontinuity();

            setRecoveryGracePeriod(3);

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

  /**
   * Dispatches one tick body and holds the serialization flag for that body's whole lifetime. This is the only writer of tickInProgress: the flag is raised here
   * before the body starts and released when its promise settles, so no other site can leave it stuck raised and starve the monitor.
   *
   * The catch is the backstop for both dispatch branches. The capture branch's body ends in its own escape handler, but the native branch's does not, so a throw
   * out of the native health check would otherwise surface as an unhandled rejection. Both branches hand back the promise of an async call, which is what
   * guarantees the settlement handlers attach at all - a body that could throw synchronously would skip them and wedge the flag.
   * @param body - The tick body to run. Its promise settling is what releases the flag.
   */
  const dispatchSerializedTick = (body: () => Promise<unknown>): void => {

    tickInProgress = true;

    void body().catch((dispatchError: unknown) => {

      LOG.warn("Monitor tick dispatch failed: %s.", formatError(dispatchError));
    }).finally(() => {

      tickInProgress = false;
    });
  };

  /* Main monitoring interval. This runs every MONITOR_INTERVAL milliseconds to check video state and trigger recovery when needed.
   *
   * Every early return must call emitStatusUpdate() before returning (except when the stream is terminating, e.g., page closed or circuit breaker tripped). This
   * ensures SSE clients always have current status data (duration, memory, health) even during recovery, buffering, or video search periods. Without this, the
   * streamStatuses map becomes stale and new SSE connections receive outdated snapshots.
   *
   * Check ordering, in the sequence the callback applies them:
   *
   * 1. intervalCleared, for explicit cleanup.
   * 2. tickInProgress, so a firing that lands while the previous body is still running skips instead of stacking a second body on the same counters.
   * 3. recoveryState.inProgress. On the capture path this covers the window a recovery await holds open inside a tick body; on the native path it is the only
   *    guard there is, since a native recovery is launched fire-and-forget and outlives the tick that started it.
   * 4. currentPage.isClosed(), for page termination outside of recovery. It must come after the recovery check: the page reference this monitor holds is the one
   *    a replacement is about to retire, and the swap closes it, so a tick landing between the close and applyTabReplacementSuccess's re-point would read the
   *    closed page and clear the interval mid-recovery, stopping status updates permanently.
   */
  const interval = setInterval((): void => {

    // Stop monitoring if cleanup was requested.
    if(intervalCleared) {

      stopMonitoring();

      return;
    }

    // Skip this firing if the previous tick body has not settled. Emit status so SSE clients keep seeing current duration, memory, and health while a long read
    // is outstanding.
    if(tickInProgress) {

      emitStatusUpdate();

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

      stopMonitoring();

      return;
    }

    /* The catch-all for capture composition, on the one path every live capture stream's tick passes through - static captures included, which is why it precedes
     * the static-capture return below. Chrome composes the capture of a selected tab from the window's fitted presentation, and re-issuing the page's own declared
     * metrics moves the composition back to the emulated surface; firing it on a cadence means any disturbance, from any cause known or unknown, is corrected
     * within about a minute rather than lasting for the recording. The registry is read again here rather than reusing the native check below, which sits on the
     * far side of that return. The command uses currentPage, the reference tab replacement updates, and the stream context is re-established because an interval
     * callback does not inherit it - so a rejection logs under this stream's own prefix.
     */
    reaffirmTickCounter++;

    if(((reaffirmTickCounter % SURFACE_REAFFIRM_TICK_INTERVAL) === 0) && (getStream(streamInfo.numericStreamId)?.identity.mode !== "native")) {

      void runWithStreamContext(streamContext, async (): Promise<void> => {

        try {

          await reaffirmCaptureSurface(currentPage);
        } catch(error) {

          LOG.debug("browser:lifecycle", "Could not re-affirm the capture surface: %s.", formatError(error));
        }
      });
    }

    // For static capture profiles (e.g., staticPage), there is no video element to monitor. Skip all video health checks and just emit a status update.
    if(profile.staticCapture) {

      emitStatusUpdate();

      return;
    }

    // For native streaming mode, monitor segment delivery health instead of video element state. We check the registry on each tick rather than caching the mode at
    // startup because the streaming mode is set after the monitor starts (native streaming is attempted after setupStream returns).
    const nativeEntry = getStream(streamInfo.numericStreamId);

    if(nativeEntry?.identity.mode === "native") {

      // Re-establish stream context for this interval tick before running the native health check. AsyncLocalStorage context is lost when entering setInterval
      // callbacks, so without this wrapper the native path's non-debug warnings would emit without the stream-ID prefix. This mirrors the capture-mode branch below.
      dispatchSerializedTick(() => runWithStreamContext(streamContext, async () => {

        checkNativeStreamHealth(nativeEntry);
      }));

      return;
    }

    // Re-establish stream context for this interval tick. AsyncLocalStorage context is lost when entering setInterval callbacks.
    dispatchSerializedTick(() => runWithStreamContext(streamContext, async () => {

      try {

        // Early exit if the stream's abort signal has been triggered. This prevents wasted work when the stream is being terminated.
        const abortSignal = getAbortSignal(streamId);

        if(abortSignal?.aborted) {

          stopMonitoring();

          return;
        }

        // Capture current timestamp for all timing calculations in this check cycle.
        const now = Date.now();

        // Gather current video state for analysis. The getVideoState helper encapsulates video element selection and returns all properties needed for health analysis.
        // We catch frame detachment errors specifically to handle context invalidation differently from normal "video not found" cases.
        let stateInfo = null;
        let contextInvalidated = false;

        try {

          // A read issued while a timeout streak is open is a confirmation probe, so it carries the short bound. With no streak open the read takes the evaluate
          // wrapper's default, which is the bound that detects a hung tab in the first place.
          stateInfo = await getVideoState(currentContext, selectorType, (consecutiveTimeouts > 0) ? UNRESPONSIVE_PROBE_TIMEOUT : undefined);
        } catch(stateError) {

          // Classify through the shared page-death predicate: the world the read ran in is gone, so the video may simply live in a context we no longer hold.
          if(isPageDeathError(stateError)) {

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

              // A validated video means the tab answered the re-search evaluates, and the timeout streak both drives the tab-replacement threshold and arms the
              // short confirmation probe, so it must not outlive that evidence.
              consecutiveTimeouts = 0;

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
        const withinRecoveryGrace = isWithinRecoveryGrace();

        // Segment production monitoring: post-recovery verification and continuous size/staleness checks.
        if(await monitorSegmentHealth(now, withinRecoveryGrace)) {

          return;
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

          stopMonitoring();

          return;
        }

        // Check for evaluate timeout errors, which indicate the browser tab may be unresponsive.
        if(error instanceof EvaluateTimeoutError) {

          consecutiveTimeouts++;

          LOG.warn("Monitor check timed out (%s consecutive). Tab may be unresponsive.", consecutiveTimeouts);

          // Update issue state so SSE clients can show the degraded state.
          lastIssueType = "tab timing out";
          lastIssueTime = Date.now();

          /* After 3 consecutive timeouts the tab has stopped answering and only a replacement can save the stream. When one cannot be started, the stream is
           * unrecoverable: the ladder has nothing left to try, and leaving it in the registry would hold open the very relaunch that would cure a marked browser.
           * So the stream terminates through the breaker, exactly as the zombie case in handleExhaustedTabReplacement does.
           *
           * None of that runs inside the post-recovery grace window. A tab that has just been replaced is entitled to its settling time, and evaluate calls
           * against a page mid-establishment time out for reasons that are not a wedged tab - so escalating here would spend an attempt, or terminate a stream,
           * on evidence the window exists to discount. The timeout tally keeps climbing, so a genuinely wedged tab is acted on the moment the window closes.
           */
          if((consecutiveTimeouts >= 3) && !isWithinRecoveryGrace()) {

            if(canReplaceTab()) {

              LOG.warn("Tab unresponsive - recovering via %s.", RECOVERY_METHODS.tabReplacement);

              await executeTabReplacement("tab unresponsive");

              return;
            }

            LOG.error("Tab unresponsive and tab replacement is unavailable (%s) - terminating stream.", tabReplacementUnavailableReason());

            stopMonitoring();
            onCircuitBreak();

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
    }));
  }, CONFIG.playback.monitorInterval);

  /* The monitor's teardown: mark the interval cleared (so any in-flight async tick short-circuits) and clear it. Self-contained - it owns only the interval. Defined
   * as a const so it is exposed as both dispose() and [Symbol.dispose].
   */
  const dispose = (): void => {

    stopMonitoring();
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
