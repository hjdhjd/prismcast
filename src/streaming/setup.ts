/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.ts: Common stream setup logic for PrismCast.
 */
import type { Browser, Frame, Page } from "puppeteer-core";
import { BrowserCaptureImpairedError, BrowserSupersededError, BrowserUnavailableError, acquireCaptureStream, emulateCaptureSurface, emulateLayoutSurface,
  getBrowserInstance, getCaptureImpairment, getCurrentBrowser, installCaptureFocusHook, noteBrowserCaptureImpaired, registerManagedPage, setCaptureProbe,
  syncWindowVisibility, unregisterManagedPage } from "../browser/index.ts";
import { CaptureAbandonedError, CaptureTurnTimeoutError, createCaptureLock } from "./captureLock.ts";
import type { CaptureStream, CaptureStreamOptions } from "../browser/index.ts";
import type { Clock, FFmpegProcess } from "../utils/index.ts";
import { FINALIZE_SETTLE_DELAY, installManifestInterceptor } from "../browser/manifestInterceptor.ts";
import { LOG, delay, extractDomain, formatError, getStreamContext, maxRetryDuration, realClock, registerAbortController,
  resolveFFmpegPath, retryOperation, runWithStreamContext, spawnFFmpeg, startTimer, waitWithTimeout } from "../utils/index.ts";
import type { ManifestInterceptionResult, ManifestInterceptorHandle } from "../browser/manifestInterceptor.ts";
import type { MonitorHandle, TabReplacementResult } from "./recovery.ts";
import type { Nullable, ResolvedChannel, ResolvedSiteProfile, TuneResult, UrlValidationResult } from "../types/index.ts";
import { getAuthDomainForChannel, getServiceDisplayName, resolveServiceKey } from "../config/services.ts";
import { getBuiltinProfile, getProfileForChannel, getProfileForUrl, resolveProfile } from "../config/profiles.ts";
import { getProviderByStrategy, invalidateDirectUrl, resolveDirectUrl } from "../browser/channelSelection.ts";
import { initializePlayback, injectVideoSelector, muteExistingVideos, navigateToPage } from "../browser/video.ts";
import { CONFIG } from "../config/index.ts";
import type { CaptureSession } from "./captureSession.ts";
import type { InitializePlaybackOptions } from "../browser/video.ts";
import type { MonitorStreamInfo } from "./monitor.ts";
import type { ProbeCacheIdentity } from "../native/probe.ts";
import { chromeFetch } from "../utils/index.ts";
import { createCaptureSession } from "./captureSession.ts";
import { getCachedEncryption } from "../native/probe.ts";
import { getCaptureMimeType } from "./codec.ts";
import { getDomainAuthState } from "../config/health.ts";
import { getDomainConfig } from "../config/sites.ts";
import { getNextStreamId } from "./registry.ts";
import { getUserProfiles } from "../config/userProfiles.ts";
import { isCaptureInfrastructureError } from "./recovery.ts";
import { isChannelSelectionProfile } from "../types/index.ts";
import { monitorPlaybackHealth } from "./monitor.ts";
import { mutateChannels } from "../config/userChannels.ts";
import { pipeline } from "node:stream/promises";
import { reaffirmCaptureSurface } from "../browser/cdp.ts";
import { startOverlayHandling } from "../browser/consent.ts";

/* This module contains the common stream setup logic for HLS streaming. The core logic is split into two functions:
 *
 * 1. createPageWithCapture(): Creates a browser page, starts media capture, navigates to the URL, and sets up video playback. This is the reusable core that both
 *    initial stream setup and tab replacement recovery use.
 *
 * 2. setupStream(): Orchestrates stream creation by calling createPageWithCapture(), then starting the health monitor. This is the
 *    entry point for new stream requests.
 *
 * The separation allows tab replacement recovery (in monitor.ts) to reuse the capture setup logic without duplicating code. When a browser tab becomes unresponsive,
 * the recovery handler can close the old tab, call createPageWithCapture() to create a fresh one, and continue with the same stream ID.
 *
 * createPageWithCapture() handles:
 * - Browser page creation with CSP bypass
 * - Media stream initialization (native fMP4 or Matroska+FFmpeg)
 * - Navigation with retry
 * - Video element detection and playback setup
 *
 * setupStream() additionally handles:
 * - Request validation (URL format only)
 * - Health monitor startup
 * - Cleanup function creation
 */

// Native fMP4 capture uses MP4/AAC for direct HLS segmentation without transcoding.
const NATIVE_FMP4_MIME_TYPE = "video/mp4;codecs=avc1,mp4a.40.2";

// Capture initialization is serialized through a task-scoped lock. Chrome's tabCapture extension can only initialize one capture at a time - a second start while
// one is in flight fails with "Cannot capture a tab with an active stream" - so every acquisition runs as a task on this one process-wide lock, which holds the
// turn until the task's promise settles. Once a capture is established it runs concurrently with other captures without issue.

// The wedge-derivation policy for the capture lock. A task held past max(CAPTURE_WEDGE_FLOOR_MS, deadline + CAPTURE_WEDGE_MARGIN_MS) without settling invokes its
// onWedge callback so the call site can route it into browser recovery. The floor is the wedge bound at the normal navigation timeout; the margin keeps the wedge
// strictly later than any larger caller deadline.
const CAPTURE_WEDGE_FLOOR_MS = 30000;
const CAPTURE_WEDGE_MARGIN_MS = 5000;

// The one production capture lock. It takes no config-derived values: setup.ts's module body runs before initializeConfiguration(), and streaming.* saves mutate the
// live CONFIG binding mid-process, so every timing bound is read per call at the call sites instead.
const captureLock = createCaptureLock({ clock: realClock, wedgeFloorMs: CAPTURE_WEDGE_FLOOR_MS, wedgeMarginMs: CAPTURE_WEDGE_MARGIN_MS });

// The bound on how long the capture extension may take to confirm a recording stopped, after a raw capture stream is destroyed and before the owning page is
// closed. Destroying the stream sends the stop request; the extension answers by closing its socket once the recorder has stopped, its tracks have stopped, and
// its pending sends have drained, and that answer is what CaptureStream.stopped carries. The stop has to land while the browser is still connected, or Chrome's
// tabCapture state lingers and the next acquisition draws "Cannot capture a tab with an active stream". The ceiling bounds the case where the answer can never
// arrive - a browser that is already gone - so a page close is never held hostage to it.
const STOP_RECORDING_CEILING_MS = 3000;
const STOP_RECORDING_CEILING_MESSAGE = "The capture extension did not confirm the recording stopped in time.";

// The slack the probe's teardown gets beyond the stop confirmation itself: the managed-page unregister and the page close that follow it.
const PAGE_CLOSE_MARGIN_MS = 2000;

// The teardown allowance the mid-life probe's outer lock deadline adds over its acquisition criterion bound. The pass/fail criterion times the acquisition alone;
// this allowance lets the destroy-and-confirm teardown complete inside the turn without counting against that criterion. It is derived from the stop ceiling it
// has to outlast rather than written as a second number beside it, so the two cannot drift apart. A teardown that hangs past the allowance trips the outer
// deadline and is reported as a capture-infrastructure failure.
const PROBE_TEARDOWN_ALLOWANCE_MS = STOP_RECORDING_CEILING_MS + PAGE_CLOSE_MARGIN_MS;

// The playback-initialization safety-net timeout. Channel selection plus video setup runs after navigation with no outer timeout racing its internal click
// retries; for guideGrid strategies a selection failure triggers an overlay dismiss and retry, which doubles the channel-selection budget. 45 seconds accommodates
// that retry while still preventing a pathological hang if multiple internal timeouts chain sequentially. Consumed by both the phase-2 race and the interception
// budget below.
const PLAYBACK_INIT_TIMEOUT = 45000;

// Margin folded into the interception budget beyond the phases it explicitly sizes: the small span between phase-2 finishing and finalize firing (the window
// resize and minimize, setupStream's pre-verification work) plus true slack. The interception window is a leak bound for a tune that dies without unwinding, not a
// latency bound - no healthy path waits on it - so its generosity costs nothing.
const INTERCEPTION_BUDGET_MARGIN_MS = 5000;

// The caller-visible capture-timeout messages, each defined once so the lock's deadline errors and any pin test read the exact text. Both match
// isCaptureInfrastructureError via its "timed out" substring, so they are exported for the classification pin that locks that contract.
export const STREAM_INIT_TIMEOUT_MESSAGE = "Stream initialization timed out.";
export const CAPTURE_PROBE_TIMEOUT_MESSAGE = "Capture probe timed out.";

/**
 * Retires a raw capture stream: destroys it so the stop request goes out while the browser is still connected, then waits for the extension to confirm the
 * recording stopped before the caller closes the page. This is the destroy-and-confirm core shared by the mid-life probe's success teardown and every path that
 * must retire a capture stream produced after its caller had already abandoned the turn. It is the async sibling of captureSession.ts's disposer, which tears the
 * composed FFmpeg pipeline down synchronously: this helper awaits the confirmation because a page close follows it directly, and unlike the composed disposer it
 * is used on abandoned paths where the page may already be closed, so the stop side is then best-effort.
 *
 * It never throws. A confirmation that does not arrive inside the ceiling is warned about and then stepped past, because holding a page open for a browser that
 * has stopped answering helps nobody.
 * @param stream - The raw capture stream to retire.
 * @param clock - Clock bounding the stop confirmation. Defaults to realClock; tests inject a fake.
 */
export async function retireRawStream(stream: CaptureStream, clock: Clock = realClock): Promise<void> {

  stream.destroy();

  try {

    await clock.waitWithTimeout(stream.stopped, STOP_RECORDING_CEILING_MS, new Error(STOP_RECORDING_CEILING_MESSAGE));
  } catch {

    LOG.warn("The capture extension did not confirm the recording stopped within %dms; closing the page regardless.", STOP_RECORDING_CEILING_MS);
  }
}

/**
 * Thrown by the capture-lock task when it finds the page already closed at the instant its turn is granted (a browser crash during the turn-wait). createPageWithCapture
 * catches it to drive the closed-page recursion outside the lock, so the turn releases at once rather than the recursion running while the turn is held. Module-private:
 * it never crosses the function boundary.
 */
class PageClosedDuringTurnError extends Error {

  public constructor() {

    super("Page closed while waiting for its capture turn.");
    this.name = "PageClosedDuringTurnError";
  }
}

/**
 * Thrown by createPageWithCapture when an establishment that navigated to a resolved direct watch URL failed on evidence against that URL - the coordinator's own
 * verdict, read from invalidateDirectUrl, never re-derived here. It says one thing to the caller: the hint has already been evicted, and a fresh attempt down the
 * guide path is worth making. setupStream is the one caller that acts on it; every other caller sees an ordinary establishment failure carrying the original
 * rejection as its cause.
 */
export class DirectUrlEstablishmentError extends Error {

  public constructor(cause: unknown) {

    // The underlying failure travels in the message as well as in cause, because the callers that do not act on the type - tab replacement above all - log the
    // message alone, and a wrapper that swallowed the reason would make this path harder to diagnose than an untyped throw.
    super("Direct watch URL establishment failed: " + formatError(cause) + ".", { cause });
    this.name = "DirectUrlEstablishmentError";
  }
}

// Maximum number of times createPageWithCapture() will retry when it detects that the page was closed while waiting for its turn on the capture lock (e.g., due to a
// browser crash). An explicit guard prevents unbounded recursion.
const MAX_PAGE_CLOSED_RETRIES = 3;

// Maximum time in milliseconds to wait for a single capture probe's acquisition to respond. Shared by the launch-gate verification (verifyCaptureSystem) and the
// mid-life re-verification, so both tiers exercise the capture path with the same bound.
const CAPTURE_PROBE_TIMEOUT_MS = 5000;

// Wire the capture-readiness probe into the browser launch gate. setup.ts owns the capture lock and the readiness probe; browser/index.ts owns the launch
// lifecycle. Injecting verifyCaptureSystem here (setup.ts already depends on browser/index.ts) keeps the dependency one-directional and breaks the
// cycle, mirroring the browserAccessors boundary between login.ts and index.ts. It runs once at module load, before any browser launch.
setCaptureProbe(verifyCaptureSystem);

// Types.

/**
 * Factory function type for creating tab replacement handlers. Called by setupStream after generating stream IDs and resolving the profile, allowing the caller to
 * create a handler with access to all necessary context.
 */
export type TabReplacementHandlerFactory = (
  numericStreamId: number,
  streamId: string,
  profile: ResolvedSiteProfile,
  metadataComment: string | undefined
) => () => Promise<Nullable<TabReplacementResult>>;

/**
 * Options for setting up a stream.
 */
export interface StreamSetupOptions {

  // The resolved channel definition (with canonical-to-variant identity inheritance applied) if streaming a named channel.
  channel?: ResolvedChannel;

  // The channel name (key) if streaming a named channel.
  channelName?: string;

  // Channel selector for multi-channel sites. Only used for ad-hoc streams (no channel definition). For predefined channels, the selector comes from
  // channel.channelSelector via getProfileForChannel.
  channelSelector?: string;

  // Click selector for play button overlays. Only used for ad-hoc streams. For predefined channels, the selector comes from the profile definition.
  clickSelector?: string;

  // Whether to click an element to start playback. Only used for ad-hoc streams. For predefined channels, this comes from the profile definition.
  clickToPlay?: boolean;

  // Pre-allocated numeric stream ID from a pending registry entry. When provided, setupStream uses this instead of allocating a new ID. This ensures the abort
  // controller, health monitor, and other internal state reference the same ID as the pending entry in the stream registry.
  numericStreamId?: number;

  // Factory function to create a tab replacement handler. Called after stream IDs are generated so the handler has access to them. If not provided, tab replacement
  // recovery is disabled.
  onTabReplacementFactory?: TabReplacementHandlerFactory;

  // The probe-cache identity this stream resolves under, built by completeStreamSetup - the one caller holding both the true per-stream key and the binding it
  // is stamped from on every entry path. Required, because a stream without one could only look the cache up under a partial identity.
  probeIdentity: ProbeCacheIdentity;

  // Override the autodetected profile with a specific profile name.
  profileOverride?: string;

  // Whether to treat this as a static page capture (no video element detection or playback monitoring).
  staticCapture?: boolean;

  // Pre-allocated string stream ID from a pending registry entry. When provided, setupStream uses this instead of generating a new one. Must be provided together
  // with numericStreamId to maintain ID consistency.
  streamId?: string;

  // The URL to stream. Required.
  url: string;
}

/**
 * Result from setting up a stream.
 */
export interface StreamSetupResult {

  // The capture-pipeline composite owning the raw capture stream and (in FFmpeg mode) the FFmpeg child. The caller attaches the fMP4 segmenter via
  // captureSession.attachSegmenter() once it is created, installs the session on the registry entry, and owns its disposal thereafter.
  captureSession: CaptureSession;

  // The channel display name if streaming a named channel.
  channelName: Nullable<string>;

  // Whether the channel was tuned via a direct mechanism (a cached URL, API interception, or a profile with no DOM-based channel-selection step at all)
  // rather than DOM interaction; the last case has nothing for the manifest interceptor to wait through.
  directTune: boolean;

  // Cleanup function to release all resources. Safe to call multiple times.
  cleanup: () => Promise<void>;

  // Manifest interceptor handle from the CDP listener installed before navigation. Contains the interception promise and a finalize() function that the caller
  // invokes after channel selection is complete. Null if the interceptor was not installed (tab replacement or DRM-cached channel).
  manifestInterception: Nullable<ManifestInterceptorHandle>;

  // Unique numeric ID for this stream.
  numericStreamId: number;

  // The browser page for this stream.
  page: Page;

  // The probe-cache identity this stream resolves under, echoed back so the native chain and the registry entry read the same value the setup path looked the
  // cache up with.
  probeIdentity: ProbeCacheIdentity;

  // The resolved site profile.
  profile: ResolvedSiteProfile;

  // The name of the resolved profile (e.g., "keyboardDynamic", "fullscreenApi", "default").
  profileName: string;

  // Friendly service display name derived from the URL domain via DOMAIN_CONFIG (e.g., "Hulu" for hulu.com). Used for SSE status display.
  serviceName: string;

  // Timestamp when the stream started.
  startTime: Date;

  // The playback health monitor handle. Exposes the live recovery metrics (read in the termination prologue) and a self-contained dispose that stops the monitor.
  monitor: MonitorHandle;

  // Unique string ID for log correlation (e.g., "nbc-abc123").
  streamId: string;

  // The URL being streamed.
  url: string;
}

/**
 * Error thrown when stream setup fails. Includes HTTP status code and user-friendly message for the response.
 */
export class StreamSetupError extends Error {

  public readonly statusCode: number;
  public readonly userMessage: string;

  constructor(message: string, statusCode: number, userMessage: string, options?: ErrorOptions) {

    super(message, options);

    this.name = "StreamSetupError";
    this.statusCode = statusCode;
    this.userMessage = userMessage;
  }
}

/**
 * Prepends sign-in guidance to a user-facing failure message when the failing channel's service domain is currently marked needs-sign-in. A confirmed
 * authentication wall on the service is the most likely cause of a failed tune there, and the channel table's login icon is the remedy - the underlying error
 * stays in the message so the original failure remains identifiable. Returns the message unchanged for ad-hoc URL streams without a channel identity, for
 * channels whose domain cannot be resolved, and for domains not marked needs-sign-in.
 *
 * Exported for unit-test coverage of the guidance composition. Production callers reach this only through setupStream's failure paths, never directly.
 * @param userMessage - The user-facing failure message being composed.
 * @param channelKey - The failing channel's key, or null/undefined for ad-hoc URL streams.
 * @param serviceName - The service's display name, used to name who needs the sign-in.
 * @returns The user message, led by sign-in guidance when the channel's domain is marked needs-sign-in.
 */
export function withSignInGuidance(userMessage: string, channelKey: Nullable<string> | undefined, serviceName: string): string {

  if(!channelKey) {

    return userMessage;
  }

  const domain = getAuthDomainForChannel(resolveServiceKey(channelKey));

  if(!domain || (getDomainAuthState(domain)?.status !== "needsLogin")) {

    return userMessage;
  }

  return serviceName + " needs sign-in. Open PrismCast's channel table and click this channel's login icon to sign in. " + userMessage;
}

/**
 * Options for creating a page with capture.
 */
export interface CreatePageWithCaptureOptions {

  // Comment to embed in FFmpeg output metadata (channel name or domain).
  comment?: string;

  // Callback invoked on FFmpeg process errors (only used in ffmpeg capture mode).
  onFFmpegError?: (error: Error) => void;

  // Forwarded to initializePlayback() and ultimately selectChannel(). Persists a category-selector resolution (e.g., Fox "FOXD2C" -> "WFLD") to the user's channel
  // store so subsequent tunes start with the concrete selector. Construction belongs to the streaming setup or recovery layer where the channel key and service tag
  // are in scope; createPageWithCapture forwards it without inspection. Omit for ad-hoc URL streams that have no stable channel record to update.
  persistResolution?: (resolvedSelector: string) => Promise<void>;

  // The resolved site profile for video handling.
  profile: ResolvedSiteProfile;

  // When true, the direct watch URL is not resolved at all and the establishment navigates to the guide URL. Set by setupStream's guide fallback, whose whole
  // purpose is to take the path a resolved URL would have bypassed. Skipping the resolver rather than discarding its answer is what makes a second
  // DirectUrlEstablishmentError structurally impossible on the fallback attempt: with no direct URL in play, the branch that throws it is never entered.
  skipDirectUrl?: boolean;

  // When true, skips CDP manifest interception. Set when the probe cache already has a "drm" result for this channel, avoiding 15 seconds of wasted CDP overhead.
  skipManifestInterception?: boolean;

  // The stream ID string for logging (e.g., "cnn-5jecl6").
  streamId: string;

  // When true, this establishment is replacing the page of a stream that is already running rather than starting a new one. It gates manifest interception: a
  // replacement has nothing to adjudicate, because the channel was verified on the tune that first established the stream.
  tabReplacement?: boolean;

  // The URL to navigate to and capture.
  url: string;

  // Internal retry counter for the page-closed-during-turn recovery. Callers should not set this - it is incremented automatically when createPageWithCapture()
  // retries after detecting a dead page from a browser crash that occurred while it waited for its turn on the capture lock.
  _pageClosedRetries?: number;
}

/**
 * Result from creating a page with capture. Contains everything needed to create a segmenter and continue with stream setup.
 */
export interface CreatePageWithCaptureResult {

  // The capture-pipeline composite owning the raw puppeteer-stream capture and (in FFmpeg mode) the FFmpeg child. The caller attaches the fMP4 segmenter via
  // captureSession.attachSegmenter() and owns its disposal thereafter.
  captureSession: CaptureSession;

  // The video context (page or frame containing the video element).
  context: Frame | Page;

  // Whether the channel was tuned via a direct mechanism (a cached URL, API interception, or a profile with no DOM-based channel-selection step at all)
  // rather than DOM interaction; the last case has nothing for the manifest interceptor to wait through.
  directTune: boolean;

  // Manifest interceptor handle for native streaming, or null if interception was not installed.
  manifestInterception: Nullable<ManifestInterceptorHandle>;

  // The browser page for this capture.
  page: Page;
}

// Request ID Generation.

/**
 * Generates a short alphanumeric request ID for log correlation. The ID is 6 characters to keep log messages readable while providing enough uniqueness for
 * practical debugging. With 36 possible characters (a-z, 0-9), there are 2.1 billion possible IDs, making collisions unlikely during any debugging session.
 * @returns A 6-character alphanumeric string.
 */
function generateRequestId(): string {

  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  const bytes = new Uint8Array(6);

  crypto.getRandomValues(bytes);

  let result = "";

  for(const byte of bytes) {

    result += chars.charAt(byte % chars.length);
  }

  return result;
}

/**
 * Generates a concise stream identifier for logging purposes. The identifier combines the channel name or hostname with a unique request ID, making it easy to
 * trace related log messages. We prefer the channel name when available because it's more meaningful than a hostname.
 * @param channelName - The channel name if streaming a named channel.
 * @param url - The URL being streamed.
 * @returns A concise stream identifier.
 */
export function generateStreamId(channelName: string | undefined, url: string | undefined): string {

  const requestId = generateRequestId();

  // If we have a channel name, use it as the prefix. Channel names are short and meaningful (e.g., "nbc", "espn").
  if(channelName) {

    return channelName + "-" + requestId;
  }

  // For direct URL requests, use the concise domain as the prefix.
  if(url) {

    return extractDomain(url) + "-" + requestId;
  }

  // Fallback when neither channel name nor URL is available. This shouldn't happen in normal operation but provides a valid ID for edge cases.
  return "unknown-" + requestId;
}

// URL Validation.

/**
 * Validates a URL before attempting to navigate to it. This function checks for supported protocols, prevents local file access, and ensures the URL is properly
 * formatted. Validating URLs before navigation prevents security issues and provides clear error messages.
 * @param url - The URL to validate.
 * @returns Validation result with optional reason for failure.
 */
export function validateStreamUrl(url: string | undefined): UrlValidationResult {

  // A URL is required. This catches both undefined and empty string.
  if(!url) {

    return { reason: "URL is required.", valid: false };
  }

  try {

    const parsed = new URL(url);

    // We support HTTP, HTTPS, and chrome: protocols. HTTP and HTTPS are standard web protocols, while chrome: URLs are used for internal pages like
    // chrome://gpu for diagnostics. Other protocols (javascript:, data:, blob:) are not supported.
    const allowedProtocols = [ "chrome:", "http:", "https:" ];

    if(!allowedProtocols.includes(parsed.protocol)) {

      return { reason: "Unsupported protocol: " + parsed.protocol, valid: false };
    }

    return { valid: true };
  } catch(_error) {

    // The URL constructor throws for invalid URLs. We catch this and return a clear error message.
    return { reason: "Invalid URL format.", valid: false };
  }
}

// Page and Capture Creation.

/**
 * Disposes a browser page acquired during stream setup: unregisters it from managed-page tracking and closes it if still open. The close is fire-and-forget with a
 * debug log on error, matching the long-standing setup-failure cleanup behavior. Used as the DisposableStack disposer for pages in createPageWithCapture and
 * setupStream, and by the setup-result cleanup closure, so every setup-phase page teardown flows through one definition.
 * @param page - The page to dispose.
 */
function disposePage(page: Page): void {

  unregisterManagedPage(page);

  if(!page.isClosed()) {

    page.close().catch((error: unknown) => {

      LOG.debug("streaming:setup", "Page close error during setup cleanup: %s.", formatError(error));
    });
  }
}

/* CreatePageWithCaptureDeps is the cross-module collaborator set createPageWithCapture composes on at the browser boundary: the shared-browser accessor, the
 * capture acquisition, and the overlay-handling poll the static-capture branch fires. It is injected as a default parameter, mirroring VideoTuneDeps in
 * browser/video.ts and PrecachingDeps in browser/precaching.ts, so a test can substitute stubs at the same collaborator-injection boundary - no loader mock -
 * while production uses the real
 * defaultCreatePageWithCaptureDeps built from the functions this module already imports. syncWindowVisibility belongs here for a reason of its own: the window has
 * to be on screen before capture acquires the compositor, and injecting the sync is what lets a test observe that ordering without a real window.
 * emulateCaptureSurface is injected for the same reason: the emulated surface has to be declared on the page before capture acquires it, and the ordering pin
 * has to observe it landing there. So are the two surface re-affirmation steps - the focus hook installed before acquisition and the re-issue that closes the
 * establishment - which reach the page through the same boundary rather than being called on it directly, so a test's hand-built page double stays as small as the
 * pipeline it drives. The remaining browser calls (registerManagedPage, unregisterManagedPage) stay direct imports: they mutate an in-process page
 * set, so they need no substitution. This is the collaborator-injection form of the Clock port (utils/clock.ts).
 */
export interface CreatePageWithCaptureDeps {

  readonly acquireCaptureStream: typeof acquireCaptureStream;
  readonly emulateCaptureSurface: typeof emulateCaptureSurface;
  readonly getCurrentBrowser: typeof getCurrentBrowser;
  readonly installCaptureFocusHook: typeof installCaptureFocusHook;
  readonly reaffirmCaptureSurface: typeof reaffirmCaptureSurface;
  readonly startOverlayHandling: typeof startOverlayHandling;
  readonly syncWindowVisibility: typeof syncWindowVisibility;
}

const defaultCreatePageWithCaptureDeps: CreatePageWithCaptureDeps = { acquireCaptureStream, emulateCaptureSurface, getCurrentBrowser, installCaptureFocusHook,
  reaffirmCaptureSurface, startOverlayHandling, syncWindowVisibility };

/**
 * Creates a browser page with media capture and navigates to the URL. This is the reusable core function used by both initial stream setup and tab replacement
 * recovery. It handles:
 * - Creating a new browser page with CSP bypass
 * - Initializing media capture (native fMP4 or Matroska+FFmpeg)
 * - Navigating to the URL with retry
 * - Setting up video playback via navigateToPage() + initializePlayback()
 *
 * The caller is responsible for:
 * - Creating the segmenter and attaching it to the capture session via captureSession.attachSegmenter()
 * - Registering/updating the stream in the registry
 * - Starting/updating the health monitor
 * - Handling cleanup on failure
 *
 * @param options - Options for page and capture creation.
 * @param deps - The injected browser and overlay-poll collaborators; defaults to defaultCreatePageWithCaptureDeps. Threaded so a test drives this function without a
 * live Chrome by substituting the shared-browser accessor, the capture acquisition, and the static-capture overlay poll.
 * @returns The page, context, and capture session (which owns the raw capture stream and any FFmpeg child).
 * @throws Error if page creation, capture initialization, or navigation fails.
 */
export async function createPageWithCapture(options: CreatePageWithCaptureOptions,
  deps: CreatePageWithCaptureDeps = defaultCreatePageWithCaptureDeps): Promise<CreatePageWithCaptureResult> {

  const captureElapsed = startTimer();
  const { comment, onFFmpegError, profile, streamId, url } = options;

  /* Bring the window on screen before anything else. Capture reads the compositor's output for the shared window, and that output is only composed correctly for a
   * window the desktop is presenting, so the sync has to land ahead of capture acquisition rather than alongside it. The pending registry entry is already in
   * capture mode by the time any caller reaches here, on both request paths, so the policy reads capture-active and this resolves to a window on screen.
   */
  await deps.syncWindowVisibility();

  // Acquire every resource on a DisposableStack so that any throw - in capture initialization, navigation, or playback setup - disposes them structurally as the
  // function unwinds, in last-acquired-first order (capture session before page, so the capture stream is destroyed and STOP_RECORDING fires while the browser is
  // still connected, before the page closes). On success we move() the stack to disarm it and transfer ownership to the caller. This centralizes teardown that
  // would otherwise be repeated in each failure path, and closes the navigation-path leak of the manifest interceptor.
  using resources = new DisposableStack();

  /* Create browser page. The establishment goes on to start a capture, so a browser that can no longer start one refuses here, before a page exists. The page
   * opens behind whatever the window is showing: its capture start selects it for the length of that start and hands the selection back, so a user working in
   * another tab is never moved out of it.
   */
  const browser = await deps.getCurrentBrowser("capture");
  const page = await browser.newPage({ background: true });

  // Register in-flight: the registry does not record this page against the stream until setup finishes, so the mark is what keeps stale page cleanup from closing
  // it mid-tune.
  registerManagedPage(page, { inFlightSetup: true });
  resources.adopt(page, disposePage);

  // Emulate the capture surface before anything else touches the page and well before capture acquires it: the dimensions the encoder is held to and the pixel
  // density the compositor rasters at are both settled here, on a page nothing has overridden yet, and a failure unwinds through the DisposableStack exactly as
  // every later step does.
  const surface = await deps.emulateCaptureSurface(page);

  await page.setBypassCSP(true);

  // Inject the shared video selector helper into the browser context. This must happen before navigation so the helper is available when evaluate calls run during
  // initializePlayback (startVideoPlayback, applyVideoStyles, verifyFullscreen, lockVolumeProperties) and subsequent health monitoring (getVideoState).
  await injectVideoSelector(page);

  // Install the tab-activation heal alongside it, before capture is acquired, so the page carries the listener from its first document onward: a user who selects
  // this tab at any point in its life gets the capture's composition moved back to the emulated surface about a second later.
  await deps.installCaptureFocusHook(page);

  // Select MIME type based on capture mode. FFmpeg mode is more stable for long recordings because Chrome's native fMP4 MediaRecorder can become unstable. The
  // codec decision (H.264 vs HEVC) is delegated to the codec module, which considers the user's allowlist and GPU hardware capabilities.
  const useFFmpeg = CONFIG.streaming.captureMode === "ffmpeg";
  const captureMimeType = useFFmpeg ? getCaptureMimeType() : NATIVE_FMP4_MIME_TYPE;

  // Resolve the FFmpeg binary path up front, before the raw capture stream is acquired below. resolveFFmpegPath is a memoized resolver that can sticky-reject; if its
  // await sat between acquiring the raw capture stream and wrapping it in a CaptureSession, a rejection would strand the stream undestroyed (STOP_RECORDING never
  // fires, leaving chrome.tabCapture active). Resolving it here keeps any rejection on a path with no capture resource yet acquired, so the CaptureSession remains
  // the single owner from the instant the stream exists. Falls back to "ffmpeg" so spawn() defers to PATH lookup; only meaningful in FFmpeg mode.
  const ffmpegBin = useFFmpeg ? ((await resolveFFmpegPath()) ?? "ffmpeg") : "ffmpeg";

  // The capture-pipeline composite, assigned once the raw capture stream and optional FFmpeg child exist and registered on the DisposableStack the moment it is built.
  let captureSession: CaptureSession;

  // Initialize media stream capture. The whole capture-init phase runs inside one try so a browser crash detected at turn grant drives the closed-page recursion,
  // while every other failure unwinds through the DisposableStack.
  try {

    const streamOptions: CaptureStreamOptions = {

      audio: true,
      audioBitsPerSecond: CONFIG.streaming.audioBitsPerSecond,
      mimeType: captureMimeType,
      video: true,
      videoBitsPerSecond: CONFIG.streaming.videoBitsPerSecond,

      /* The dimension bounds pin the track to exactly the surface emulateCaptureSurface declared on this page, rather than to a second read of the configured
       * preset, so a preset saved mid-establishment cannot leave the encoder constrained to dimensions the page was never emulated at.
       *
       * The frame rate is constrained to a 30-60 fps band: 60 is the live-TV ceiling, and a 30 floor keeps motion smooth even when the user configures a lower
       * rate. The ceiling is fixed at 60 while the floor follows the user's configured rate (clamped into the band), so the encoder favours the requested rate but
       * never drops below 30. The readiness probe (attemptCaptureProbe) instead pins both bounds to a flat 30 because its acquisition fails or succeeds at the
       * tabCapture API level before encoding matters, so a representative-but-minimal constraint set suffices there.
       */
      videoConstraints: {

        mandatory: {

          maxFrameRate: 60,
          maxHeight: surface.height,
          maxWidth: surface.width,
          minFrameRate: Math.max(30, Math.min(60, CONFIG.streaming.frameRate)),
          minHeight: surface.height,
          minWidth: surface.width
        }
      }
    };

    // Acquire the raw capture stream through the capture lock. The lock serializes acquisition against every other capture init process-wide and holds the turn
    // until this task's promise settles, so no two inits collide. Every timing bound is read here, at the call, because the module-scope lock captured nothing
    // from CONFIG.
    const stream = await captureLock.run(async (signal: AbortSignal): Promise<CaptureStream> => {

      // If Chrome crashed while this task waited for its turn, the page is dead. Throw a typed error so the turn releases at once and the closed-page recursion runs
      // OUTSIDE the lock, on a fresh page.
      if(page.isClosed()) {

        throw new PageClosedDuringTurnError();
      }

      // Initialize capture. The acquisition carries this task's own abort signal, so the one retry it may make can never begin after the caller's deadline has
      // fired and the caller has stopped waiting for the result.
      const raw = await deps.acquireCaptureStream(page, streamOptions, { signal });

      // The caller deadline fired while the acquisition was still running: retire the stream this task just produced - destroy plus the stop confirmation - inside
      // the turn, then reject, so no path strands a live capture on a closing page or mistakes a retired stream for a usable one.
      if(signal.aborted) {

        await retireRawStream(raw, realClock);

        throw new CaptureAbandonedError();
      }

      return raw;
    }, {

      deadlineMessage: STREAM_INIT_TIMEOUT_MESSAGE,
      deadlineMs: CONFIG.streaming.navigationTimeout,

      /* A capture initialization that holds the lock this long is evidence about the browser rather than about the stream: the wedge fires only after this task's
       * own deadline has already failed the tune, so the tune is lost either way and what is left to decide is what the browser can still be trusted with.
       * Recording it marks the instance this task ran against, which refuses later capture starts at once and relaunches the browser when nothing depends on it -
       * the same response the probe's verdict takes, reached from the other direction.
       */
      onWedge: (): void => {

        noteBrowserCaptureImpaired(browser, "capture initialization wedged past the recovery bound");
      },
      turnWaitMs: CONFIG.streaming.navigationTimeout
    });

    // For FFmpeg mode, spawn FFmpeg to transcode the Matroska stream to fMP4. FFmpeg copies the H264 video and transcodes Opus audio to AAC. The binary path was
    // resolved up front (above) so no throwable await sits between acquiring the raw capture stream and wrapping it in the CaptureSession that owns it. The spawn and
    // pipeline wiring below are synchronous, so the stream is owned the instant it exists.
    let ffmpegProcess: Nullable<FFmpegProcess> = null;

    if(useFFmpeg) {

      const ffmpeg = spawnFFmpeg(ffmpegBin, CONFIG.streaming.audioBitsPerSecond, (error) => {

        LOG.error("FFmpeg process error: %s.", formatError(error));

        if(onFFmpegError) {

          onFFmpegError(error);
        }
      }, streamId, comment);

      ffmpegProcess = ffmpeg;

      // Handle pipe errors on stdout. Stdin errors are handled by pipeline() below.
      ffmpeg.stdout.on("error", (error) => {

        const errorMessage = formatError(error);

        if(errorMessage.includes("EPIPE")) {

          LOG.debug("streaming:ffmpeg", "FFmpeg stdout pipe closed: %s.", errorMessage);
        } else {

          LOG.error("FFmpeg stdout pipe error: %s.", errorMessage);
          ffmpeg.kill();

          if(onFFmpegError) {

            onFFmpegError(error);
          }
        }
      });

      // Pipe the Matroska capture stream to FFmpeg's stdin using pipeline() for proper cleanup. When FFmpeg is killed during tab replacement, pipeline() automatically
      // destroys the source stream, preventing "write after end" errors that would occur with .pipe().
      pipeline(stream, ffmpeg.stdin).catch((error: unknown) => {

        const errorMessage = formatError(error);

        // EPIPE, "write after end", and "Premature close" errors are expected during cleanup when FFmpeg is killed or the capture stream is destroyed.
        if(errorMessage.includes("EPIPE") || errorMessage.includes("write after end") || errorMessage.includes("Premature close")) {

          return;
        }

        // Unexpected pipeline errors require cleanup.
        LOG.error("Capture pipeline error: %s.", errorMessage);
        ffmpeg.kill();

        if(onFFmpegError) {

          onFFmpegError(error instanceof Error ? error : new Error(String(error)));
        }
      });
    }

    // Wrap the raw capture stream and optional FFmpeg child as one self-disposing pipeline unit, and register it for structural teardown. The session derives the
    // segmenter input internally (FFmpeg's stdout in FFmpeg mode, the raw stream in native-fMP4 mode); the caller attaches the segmenter once it is created.
    captureSession = createCaptureSession({ ffmpegProcess, rawCaptureStream: stream });
    resources.use(captureSession);
  } catch(error) {

    // A browser crash detected at turn grant surfaces as PageClosedDuringTurnError: the page is dead, so unregister it, bump the retry counter, and start over on a
    // fresh page on the new browser. The recursion runs here, outside the lock, so the turn was already released the instant the task threw. The retry cap prevents
    // unbounded recursion during a crash loop.
    if(error instanceof PageClosedDuringTurnError) {

      unregisterManagedPage(page);

      const retryCount = options._pageClosedRetries ?? 0;

      if(retryCount >= MAX_PAGE_CLOSED_RETRIES) {

        throw new Error("Browser crashed too many times during capture initialization.");
      }

      return await createPageWithCapture({ ...options, _pageClosedRetries: retryCount + 1 }, deps);
    }

    // Every other rejection - a caller-deadline CaptureDeadlineError, or any other capture-init failure - just
    // unwinds. Resource teardown (page, interceptor, and the capture session once built) is handled by the DisposableStack as this throw unwinds the function scope.
    throw error;
  }

  // Navigate and set up playback. For static capture profiles, just navigate without video setup.
  let context: Frame | Page;
  let strategyDirectTune = false;
  let usedDirectUrl = false;

  // Install the CDP manifest interceptor immediately before the navigate-and-tune fork, so its observation window opens after the capture-lock and acquisition
  // (which the observer would otherwise idle through) and spans exactly the phases that produce manifests: navigation with retry, channel selection, and video
  // setup. The install decision is gated on tab replacement and the skip flag only - static-capture profiles install too, since they stay native-HLS eligible
  // through the interception's presence, so the guard sits upstream of the fork and both branches inherit it. The navigation allowance handed to the budget is
  // this path's worst-case retry duration, backoff sleeps included; establishmentBudgetMs owns why the window has to outlive that allowance and every phase
  // after it, direct tunes included.
  const interceptionBudgetMs = establishmentBudgetMs(maxRetryDuration({

    backoffJitter: CONFIG.recovery.backoffJitter,
    maxAttempts: CONFIG.streaming.maxNavigationRetries,
    maxBackoffDelay: CONFIG.recovery.maxBackoffDelay,
    timeoutMs: CONFIG.streaming.navigationTimeout
  }));

  const manifestInterception = (!options.tabReplacement && !options.skipManifestInterception) ? await installManifestInterceptor(page, interceptionBudgetMs) : null;

  // Register the interception on the resource stack after the capture session, so on an unwind it disposes first. Its disposal is a CDP observer detach with no
  // ordering dependency on the capture session or the page; the only ordered teardown pair is capture-session-before-page (STOP_RECORDING while the browser is
  // still connected), which both this stack and the later owned stack preserve.
  if(manifestInterception) {

    resources.use(manifestInterception);
  }

  try {

    if(!profile.staticCapture) {

      // Check for a direct watch URL, unless the caller has asked for the guide path outright. When one is available, navigate directly to it and skip channel
      // selection, avoiding guide page navigation entirely. On a failure the coordinator blames the URL for, the catch block below evicts the hint and leaves this
      // function typed, and setupStream re-invokes once with the resolution skipped so the tune still gets its guide attempt.
      const directUrl = options.skipDirectUrl ? null : await resolveDirectUrl(profile, page);

      usedDirectUrl = !!directUrl;

      const navigationUrl = directUrl ?? url;

      /* Phase 1 (navigation) and phase 2 (channel selection + video setup) run through the shared establishment composition, so the step order this path uses -
       * navigate, stamp the observation epoch, initialize playback under the module-scope PLAYBACK_INIT_TIMEOUT safety net - is the same order the
       * re-establishment path runs. When navigating to a cached direct URL, channel selection is skipped because the URL already targets the correct channel.
       * The composition places no outer timeout around channel selection: each sub-step (selectChannel, waitForVideoReady, etc.) carries its own internal
       * timeout via videoTimeout and the click retry constants.
       */
      const tuneResult = await establishChannelPlayback(page, profile, manifestInterception, {

        initOptions: { persistResolution: options.persistResolution, requestedUrl: navigationUrl, skipChannelSelection: usedDirectUrl },

        // Navigate with retry. The 10-second navigationTimeout is appropriate for page loads, and retryOperation correctly reloads the page on genuine navigation
        // failures. The retry ladder is this path's own policy, kept separate from channel selection so its timeout does not race the internal click retry loops
        // in channel selection strategies (guideGrid can take 15-20 seconds for binary search + click retries).
        navigate: async (): Promise<void> => {

          await retryOperation({

            backoffJitter: CONFIG.recovery.backoffJitter,
            description: "page navigation for " + navigationUrl,
            maxAttempts: CONFIG.streaming.maxNavigationRetries,
            maxBackoffDelay: CONFIG.recovery.maxBackoffDelay,
            operation: async (): Promise<void> => {

              await navigateToPage(page, navigationUrl, profile);
            },
            shouldAbort: () => page.isClosed(),
            timeoutMs: CONFIG.streaming.navigationTimeout
          });
        }
      });

      strategyDirectTune = tuneResult.directTune ?? false;
      context = tuneResult.context;
    } else {

      await page.goto(url);

      // A static capture navigates once and takes the page as-is, with no channel selection or video wait, so it never reaches the tune path's overlay poll. Launch
      // a bounded staticCapture poll so a cookie banner or per-site modal is dismissed on the captured page. There is no controller: the phase's window bounds it and
      // a closed page stops it via the tick-error taxonomy. Any dismissal click lands in the captured pixels, which is exactly the intent for a static capture.
      void deps.startOverlayHandling(page, profile, { phase: "staticCapture" });

      context = page;
    }
  } catch(error) {

    // If a direct watch URL was used, offer the failure to the cache coordinator, which evicts the live and persisted hints when the failure is evidence against
    // the URL and reports that verdict back. The retention policy lives there and is read from there - this file never re-derives which failures count.
    const urlEvidence = usedDirectUrl && invalidateDirectUrl(profile, error);

    // Window presentation is deliberately left alone here. The failing stream's registry entry is still registered in capture mode at this point, so the policy
    // would read capture-active and hold the window on screen anyway; the terminate path that tears the entry down is the moment that genuinely settles it.
    // Resource teardown (capture session, interceptor, page) is handled by the DisposableStack as this throw unwinds the function scope; the capture session
    // disposes first, destroying the capture stream before the page closes, so STOP_RECORDING ordering is preserved.

    /* A failure the coordinator blamed on the URL is worth one more attempt down the guide path, so it leaves this function typed for setupStream to act on. An
     * establishment timeout qualifies: the retry is a whole fresh invocation, so there is no abandoned first attempt still driving a page, no one-shot manifest
     * interception handle to reuse, and no direct-tune marker to reset - the retry gets its own page, handle, and markers, which is exactly what a fresh tune
     * would get. Page-death and abort failures, the ones the retention policy keeps the URL for, rethrow raw: nothing about them says the URL is wrong.
     */
    if(urlEvidence) {

      throw new DirectUrlEstablishmentError(error);
    }

    throw error;
  }

  /* Both establishment branches join here, which is why the re-affirmation sits at this point: capture acquisition selects this tab for the length of the start
   * and hands the selection back, and a tune of a fullscreen-activating profile selects it again for its fullscreen sequence and hands it back once more.
   * Re-issuing the page's own declared metrics is what leaves the capture composing the emulated surface rather than the window's fitted view of it, whatever the
   * establishment did with the selection.
   */
  await deps.reaffirmCaptureSurface(page);

  // Re-settle the window now that the page is established, handing the sync the page it should use for the CDP session. Navigation activates the window on macOS,
  // so a pass here settles it against the policy after the tune, as every other transition does.
  await deps.syncWindowVisibility(page);

  LOG.debug("timing:startup", "Page with capture ready. Total: %sms.", captureElapsed());

  // Success: transfer ownership of the page, interceptor, and capture session out of the scope guard. move() empties the stack so its scope-exit disposal is a no-op
  // and the caller becomes responsible for disposing what it now holds.
  resources.move();

  return {

    captureSession,
    context,

    // The kind the interception is finalized against, derived by the shared formula from this path's tune facts and the profile.
    directTune: computeDirectTuneKind({ profile, strategyDirectTune, usedDirectUrl }),

    manifestInterception,
    page
  };
}

// URL Redirect Resolution.

/**
 * Resolves a URL's final destination by following HTTP redirects. This is used for profile detection when a channel's URL belongs to an indirection service (e.g.,
 * FruitDeepLinks) whose domain has no profile mapping. By following redirects, we discover the actual streaming site's domain and can resolve the correct profile.
 *
 * Uses a HEAD request to avoid downloading response bodies. The 3-second timeout ensures stream startup isn't blocked by slow or unreachable indirection services.
 *
 * @param url - The URL to resolve.
 * @returns The final URL after following all redirects, or null on any error.
 */
async function resolveRedirectUrl(url: string): Promise<Nullable<string>> {

  try {

    const response = await chromeFetch(url, { method: "HEAD", signal: AbortSignal.timeout(3000) });

    return response.url;
  } catch {

    return null;
  }
}

// Stream Setup.

/**
 * Constructs the persistResolution callback for a tune. When the resolution layer in selectChannel() converts a category selector to a concrete per-user call sign,
 * this callback writes the result to the user's channel store as a per-service-variant override - the same delta shape produced by manual edits in the web UI. The
 * variant key is the canonical channel key plus the service tag, so this closure must be built at the streaming setup layer where both are in scope. Returns a
 * Promise that resolves once the write is committed; resolution-layer errors thrown from the underlying file store are surfaced for caller-side logging but do not
 * abort the tune (selectChannel attaches a .catch on the returned promise).
 *
 * Safe to call more than once at the storage layer: writing the same selector twice is a no-op (the file store deduplicates identical deltas via normalization).
 * The closure does not pre-check whether the value differs - the underlying store handles that.
 * @param canonicalKey - The canonical channel key (e.g., "fox").
 * @param serviceTag - The active service tag (e.g., "foxone").
 * @returns Async callback that persists the resolved selector to the channel store.
 */
function buildPersistResolutionCallback(canonicalKey: string, serviceTag: string): (resolvedSelector: string) => Promise<void> {

  const variantKey = canonicalKey + "-" + serviceTag;

  return async (resolvedSelector: string): Promise<void> => {

    await mutateChannels((data) => {

      const existing = data.channels[variantKey] ?? {};

      data.channels[variantKey] = { ...existing, channelSelector: resolvedSelector };
    });

    LOG.debug("tuning", "Persisted resolved selector \"%s\" to channel store as \"%s\".", resolvedSelector, variantKey);
  };
}

/* Decides whether the manifest a tune's interception selected belongs to the channel the profile selects, using the provider's own verifier when one exists. The
 * provider and channelSelector gates run before the interception promise is awaited, so a stream with nothing to verify never waits on the interception
 * here...the promise's settle time belongs only to the paths that consume it. Verification is gated to master-kind selections because a provider verifier like
 * Fox's reads the channel call sign from a fixed segment of the master CDN URL, and a media (chunklist) URL has a different path shape the verifier was never
 * calibrated for - a false tune-failure on a correct stream. Providers without a verifier, profiles without a channelSelector, non-master selections, and a null
 * interception all verify vacuously: the gates upstream are the only identity signal we have there, so the honest answer is no objection rather than a guess.
 *
 * @param interceptionPromise - The interception promise from the handle the tune finalized, awaited only once the gates above admit a verifier.
 * @param profile - The resolved site profile whose strategy names the provider and whose channelSelector names the expected channel.
 * @returns A human-readable failure reason when the manifest belongs to a different channel, or null when it verifies (including every vacuous case).
 */
export async function verifyManifestSelection(interceptionPromise: Promise<Nullable<ManifestInterceptionResult>>,
  profile: ResolvedSiteProfile): Promise<Nullable<string>> {

  const provider = getProviderByStrategy(profile.channelSelection.strategy);

  if(!provider?.verifyManifestForChannel || !profile.channelSelector) {

    return null;
  }

  const interception = await interceptionPromise;

  if(interception?.selectedKind !== "master") {

    return null;
  }

  return provider.verifyManifestForChannel(interception.manifestUrl, profile.channelSelector);
}

// Channel Establishment.

/**
 * Sizes the interception observation window for one establishment run. The observer has to outlive every phase that can feed it: the caller's own navigation
 * allowance (retry-aware on the tune path, a single attempt on the refresh path), the bounded playback initialization, the settle the interceptor waits out
 * before it resolves, and a margin covering the small span between those phases. A window that expired mid-establishment would resolve with whatever the page
 * load captured rather than the channel the selection landed on, which is the outcome this budget forecloses. Because no healthy path waits on the timer - it is
 * a leak bound for an establishment that dies without unwinding - its generosity costs nothing.
 * @param navigationAllowanceMs - The caller's worst-case navigation allowance, including any retry backoff its own policy performs.
 * @returns The observation window, in milliseconds.
 */
export function establishmentBudgetMs(navigationAllowanceMs: number): number {

  return navigationAllowanceMs + PLAYBACK_INIT_TIMEOUT + FINALIZE_SETTLE_DELAY + INTERCEPTION_BUDGET_MARGIN_MS;
}

/**
 * Derives the direct-tune kind the interception is finalized against. A tune is direct when the route was a cached direct watch URL, when the strategy itself
 * resolved a direct tune through API interception, or when the profile has no DOM-based channel-selection step to run at all. The refresh path always navigates
 * the configured channel url with full selection, so it never has a cached direct URL to report and omits that term entirely.
 * @param options - The resolved profile plus the tune facts the kind reads: whether a cached direct URL was taken (absent on paths that cannot take one) and
 *   whether the strategy resolved a direct tune of its own.
 * @returns True when the interception should adjudicate as a direct tune.
 */
export function computeDirectTuneKind(options: { profile: ResolvedSiteProfile; strategyDirectTune: boolean; usedDirectUrl?: boolean }): boolean {

  return (options.usedDirectUrl ?? false) || options.strategyDirectTune || !isChannelSelectionProfile(options.profile);
}

/* The browser-boundary collaborator establishChannelPlayback composes on. Playback initialization is the one step that drives a live page, so injecting it lets
 * the composition's choreography be driven without Chrome while production runs the real function this module already imports. This is the
 * collaborator-injection form of the Clock port (utils/clock.ts), the same shape CreatePageWithCaptureDeps uses.
 */
export interface EstablishChannelPlaybackDeps {

  readonly initializePlayback: typeof initializePlayback;
}

const defaultEstablishChannelPlaybackDeps: EstablishChannelPlaybackDeps = { initializePlayback };

/* The per-path policy establishChannelPlayback carries. Everything that legitimately differs between the tune and the refresh arrives through here, so the step
 * order stays the composition's while each caller keeps the decisions that are genuinely its own.
 */
export interface EstablishChannelPlaybackOptions {

  // Options forwarded to playback initialization; each path supplies its own (the tune threads persistResolution and skipChannelSelection, the refresh supplies
  // only requestedUrl).
  initOptions: InitializePlaybackOptions;

  // Performs the caller's navigation policy in full - the tune wraps its retry ladder here, the refresh navigates single-attempt. The composition awaits this
  // before stamping the epoch, so the policy stays the caller's while the ordering stays the composition's.
  navigate: () => Promise<void>;

  // Invoked once when playback initialization settles - success, failure, or a late completion after the bounded wait lapsed. The caller closes over whatever
  // context the hook needs; the refresh path uses it to restore its mute.
  onInitSettled?: () => void;
}

/**
 * Establishes a channel on a page: navigate under the caller's own policy, stamp the interception's observation epoch, then run playback initialization under
 * the module's safety-net bound. The tune path and the native refresh capability both run this, so the sequence and its ordering are written exactly once and
 * only per-path policy differs.
 * @param page - The page being established.
 * @param profile - The resolved site profile whose strategy drives navigation and channel selection.
 * @param handle - The interceptor observing this establishment, or null when nothing is observing it (a tab replacement, or an install that failed).
 * @param options - The caller's navigation policy, its initialization options, and an optional settlement hook. See EstablishChannelPlaybackOptions.
 * @param deps - Injected browser-boundary collaborator; defaults to the real playback initializer in production.
 * @returns The result playback initialization produced: the video context for subsequent monitoring, and the strategy's direct-tune flag where it resolved one.
 * @throws The caller's navigation failure, the initialization's own failure, or the bound's timeout error when the bound lapses first.
 */
export async function establishChannelPlayback(page: Page, profile: ResolvedSiteProfile, handle: Nullable<ManifestInterceptorHandle>,
  options: EstablishChannelPlaybackOptions, deps: EstablishChannelPlaybackDeps = defaultEstablishChannelPlaybackDeps): Promise<TuneResult> {

  const { initOptions, navigate, onInitSettled } = options;

  await navigate();

  // Channel selection begins here, so the observation epoch is stamped now - manifests seen earlier belong to page load (a guide page's auto-played default
  // channel), and guide-tune selection prefers a master observed after this point. A null handle means nothing is observing, so there is no epoch to stamp.
  handle?.markChannelSelectionStart();

  const initPromise = deps.initializePlayback(page, profile, initOptions);

  /* The settlement hook rides the initialization's own settlement rather than a fixed step after the wait below, because only the promise knows which of
   * success, failure, or a late completion actually happened - and a late completion is reachable, since the wait abandons rather than cancels: a lapsed
   * initialization is not stopped, it winds down on its own bounded internal phases and can finish afterward. The trailing catch is the whole safety net for
   * the hook itself, absorbing a hook that throws as well as a structurally-permitted async hook that rejects, so this chain can never become a rejection
   * source of its own.
   */
  if(onInitSettled) {

    void initPromise.finally(() => onInitSettled()).catch(() => { /* The bounded wait below owns the initialization's failure. */ });
  }

  return waitWithTimeout(initPromise, PLAYBACK_INIT_TIMEOUT, new Error("Playback initialization timed out after " + String(PLAYBACK_INIT_TIMEOUT) + "ms."));
}

/**
 * Adjudicates what an establishment's interception selected: finalize it against the honest direct-tune kind, then confirm the manifest belongs to the channel
 * the profile names. The tune path and the native refresh capability both run this, so finalize-then-verify is written exactly once.
 *
 * The stage hands on the interception promise rather than a resolved result, which is what keeps verifyManifestSelection's latency rule holding - its provider
 * and selector gates run before the promise is awaited, so a stream with nothing to verify never waits on the interception here.
 * @param handle - The interceptor this establishment installed, still observing.
 * @param profile - The resolved site profile whose strategy names the provider and whose channelSelector names the expected channel.
 * @param directTune - The direct-tune kind to finalize against.
 * @returns A human-readable failure reason when the manifest belongs to a different channel, or null when it verifies (including every vacuous case).
 */
export async function adjudicateChannelSelection(handle: ManifestInterceptorHandle, profile: ResolvedSiteProfile,
  directTune: boolean): Promise<Nullable<string>> {

  handle.finalize(directTune);

  return verifyManifestSelection(handle.promise, profile);
}

/**
 * Sets up a stream: validates input, creates browser page, initializes capture, navigates to URL, and starts health monitoring.
 *
 * This function handles all common stream setup logic. The caller is responsible for:
 * - Creating the segmenter and attaching it to the returned capture session (via captureSession.attachSegmenter), or upgrading to native streaming
 * - Registering the stream in the registry
 * - Triggering cleanup when the stream ends
 *
 * @param options - Stream configuration options.
 * @param onCircuitBreak - Callback invoked when the circuit breaker trips (stream unrecoverable).
 * @param deps - The injected browser and overlay-poll collaborators, forwarded to every establishment attempt this function makes; defaults to
 * defaultCreatePageWithCaptureDeps. Threaded for the same reason createPageWithCapture takes them: a test drives the establishment sequence - including its guide
 * fallback - without a live Chrome by substituting the shared-browser accessor, the capture launcher, and the overlay poll.
 * @returns Setup result with capture session, cleanup function, and metadata.
 * @throws StreamSetupError if setup fails with appropriate status code and message.
 */
export async function setupStream(options: StreamSetupOptions, onCircuitBreak: () => void,
  deps: CreatePageWithCaptureDeps = defaultCreatePageWithCaptureDeps): Promise<StreamSetupResult> {

  const { channel, channelName, channelSelector, clickSelector, clickToPlay, onTabReplacementFactory, probeIdentity, profileOverride, staticCapture,
    url } = options;

  // Use pre-allocated IDs from a pending registry entry when available, or generate new ones. Pre-allocated IDs ensure the abort controller, health monitor, and
  // tab replacement handler all reference the same stream identity as the pending entry in the registry.
  const streamId = options.streamId ?? generateStreamId(channelName, url);
  const numericStreamId = options.numericStreamId ?? getNextStreamId();
  const startTime = new Date();

  // Create and register the AbortController for this stream. This allows pending evaluate calls to be cancelled immediately when the stream is terminated.
  const abortController = new AbortController();

  registerAbortController(streamId, abortController);

  // Resolve the profile for this stream. If the original URL's domain has no mapping (profileName === "default"), try following HTTP redirects to discover the
  // actual destination domain. This supports indirection services like FruitDeepLinks that use redirect URLs to route to the actual streaming site.
  let profileResult = channel ? getProfileForChannel(channel) : getProfileForUrl(url);

  if(profileResult.profileName === "default") {

    const urlToResolve = channel?.url ?? url;

    if(urlToResolve) {

      const resolvedUrl = await resolveRedirectUrl(urlToResolve);

      if(resolvedUrl && (resolvedUrl !== urlToResolve)) {

        const redirectResult = getProfileForUrl(resolvedUrl);

        if(redirectResult.profileName !== "default") {

          profileResult = redirectResult;

          LOG.debug("streaming:setup", "Resolved redirect for profile detection: %s -> %s (%s).", urlToResolve, resolvedUrl, redirectResult.profileName);
        }
      }
    }
  }

  let profile = profileResult.profile;
  let profileName = profileResult.profileName;

  // Wrap the setup in stream context for log correlation.
  return runWithStreamContext({ channelName: channel?.name, streamId, url }, async () => {

    // Apply profile override if specified.
    if(profileOverride) {

      // An override may name any profile that exists: a builtin from any source through the single lookup, or one of the user's own. The UI profile catalog is
      // not the oracle here - it omits the provider profiles, which a direct override may legitimately ask for.
      if(Boolean(getBuiltinProfile(profileOverride)) || (profileOverride in getUserProfiles())) {

        profile = resolveProfile(profileOverride);
        profileName = profileOverride;

        LOG.debug("streaming:setup", "Profile overridden to '%s' via query parameter.", profileOverride);
      } else {

        LOG.warn("Unknown profile override '%s', using resolved profile.", profileOverride);
      }
    }

    /* A channel that named a profile and did not get it should hear about it once, at the moment it matters. Resolution reports the substitution on its return
     * rather than logging it, because the playlist render and the channel table call the same resolver and would repeat the message on every fetch and every
     * draw. Comparing profileName against the resolver's keeps a query-parameter override quiet: when ?profile= replaces the resolution wholesale, the
     * substitution the resolver reported is not what tunes.
     */
    if(profileResult.overriddenProfile && (profileName === profileResult.profileName)) {

      LOG.warn("Channel %s specifies the %s profile, which requires a channel selector the channel does not define; tuning with the %s profile instead.",
        channel?.name ?? channelName ?? url, profileResult.overriddenProfile, profileName);
    }

    // Apply static capture override if specified.
    if(staticCapture) {

      profile = { ...profile, staticCapture: true };
    }

    // Merge the ad-hoc channel selector into the profile if provided. This must happen after the profile override block above, which replaces the profile object
    // wholesale and would discard an earlier merge. For predefined channels, getProfileForChannel already handles the merge from channel.channelSelector.
    if(channelSelector) {

      profile = { ...profile, channelSelector };
    }

    // Merge the ad-hoc clickToPlay and clickSelector options into the profile. clickSelector implies clickToPlay. For ad-hoc streams, these enable clicking an
    // element to start playback - either the video element (clickToPlay alone) or a play button overlay (clickToPlay + clickSelector).
    if(clickToPlay || clickSelector) {

      profile = { ...profile, clickToPlay: true, ...(clickSelector ? { clickSelector } : {}) };
    }

    // Compute the metadata comment for FFmpeg. Prefer the friendly channel name, fall back to the channel key, or extract the domain from the URL.
    const metadataComment = channel?.name ?? channelName ?? extractDomain(url);

    // Compute the friendly service display name once for use in both the monitor and the setup result.
    const serviceName = getServiceDisplayName(url);

    // Create the tab replacement handler if a factory was provided. This is done after profile resolution so the handler has access to the final profile.
    const onTabReplacement = onTabReplacementFactory ? onTabReplacementFactory(numericStreamId, streamId, profile, metadataComment) : undefined;

    // Validate URL.
    const validation = validateStreamUrl(url);

    if(!validation.valid) {

      LOG.error("Invalid URL requested: %s - %s.", url, validation.reason ?? "Unknown error");

      throw new StreamSetupError(
        "Invalid URL: " + (validation.reason ?? "Unknown error"),
        400,
        validation.reason ?? "Invalid URL."
      );
    }

    // Concurrent-stream capacity is reserved upstream at the registration site (reserveStreamSlot in hls.ts) before this stream's pending entry is registered, so
    // the new stream is excluded from its own check. We deliberately do NOT re-check here: by the time setupStream runs, getStreamCount() already includes this
    // stream's pending entry, so a count-based check would double-count it against its own slot and reject at the legitimate boundary - after the client has
    // already received a preroll playlist. reserveStreamSlot is the single source of truth for the capacity decision; setupStream's sole caller (completeStreamSetup)
    // always reserves before reaching here.

    // Create page and start capture using the shared function. This handles browser page creation, capture initialization, FFmpeg spawning, and navigation with retry.
    let captureResult: CreatePageWithCaptureResult;

    try {

      /* Skip CDP manifest interception when the channel is pinned to screen capture, or when the probe cache already knows this stream's binding resolves to
       * DRM. The per-channel override short-circuits first, so a forced channel never installs the interceptor: nothing intercepts a manifest, no native
       * attempt runs, no probe fires, and the encryption cache stays untouched by that stream. The cache half avoids creating a CDP session that sits idle for
       * 15 seconds before the interceptor timeout cleans it up; every stream carries an identity, ad-hoc URLs included, so that lookup needs no guard.
       */
      const skipInterception = (channel?.forceCapture === true) || (getCachedEncryption(probeIdentity) === "drm");

      // Build the persistResolution closure for the active channel. When the resolution layer in selectChannel() converts a category selector to a concrete call
      // sign, this closure writes the result to the user's channel store as a per-service-variant override - the same shape produced when a user manually edits
      // the selector via the web UI. Omitted for ad-hoc URL streams (no channel record to update).
      const serviceTag = getDomainConfig(url)?.serviceTag;
      const persistResolution = (channelName && serviceTag) ?
        buildPersistResolutionCallback(channelName, serviceTag) :
        undefined;

      const attemptOptions: CreatePageWithCaptureOptions = {

        comment: metadataComment,
        onFFmpegError: onCircuitBreak,
        persistResolution,
        profile,
        skipManifestInterception: skipInterception,
        streamId,
        url
      };

      /* The establishment, with its one guide fallback. A failure the coordinator blamed on a direct watch URL arrives here typed, with the hint already evicted;
       * one more invocation with the resolution skipped is exactly what the next tune would do, and doing it now spares the client a whole failed request. The
       * fallback runs after the first attempt's DisposableStack has unwound, so it gets a fresh page, a fresh manifest-interception handle, and usedDirectUrl
       * naturally false - the direct-tune marker and the manifest finalizer are computed per invocation and need no reset. Only the typed error is caught here;
       * every other failure, the fallback's own included, falls to setupStream's catch below so one classification block serves both attempts identically.
       *
       * The worst case it can produce: a stale hint plus a genuinely broken guide costs two establishments before the classified failure. On the preroll-fed HLS
       * path the client stays fed throughout; on the blocking callers - pretune, MPEG-TS, an ad-hoc play - the alternative is the same second establishment run by
       * the caller's own retry, one request later.
       */
      const establish = async (): Promise<CreatePageWithCaptureResult> => {

        try {

          return await createPageWithCapture(attemptOptions, deps);
        } catch(error) {

          if(!(error instanceof DirectUrlEstablishmentError)) {

            throw error;
          }

          LOG.warn("The direct watch URL for %s did not establish playback. Retrying once through the provider's guide.", metadataComment);

          return await createPageWithCapture({ ...attemptOptions, skipDirectUrl: true }, deps);
        }
      };

      captureResult = await establish();
    } catch(error) {

      // The browser supervisor's acquire() rejects with these while the capture system is recovering: BrowserUnavailableError when the relaunch governor is cooling
      // (degraded), BrowserSupersededError when an in-flight launch was abandoned by a readiness-loss. Both are transient "retry me" conditions, so they map to a 503
      // back-off (Channels DVR honors the Retry-After the route attaches to a 503). We handle them first and WITHOUT an error log: the supervisor raises the loud
      // degraded alarm once on the transition, so the per-request 503s during the cooldown must stay quiet rather than spam an error on every Channels DVR retry.
      // Their messages carry no capture-infrastructure signature, so without this explicit branch the classifier below would make them a 500 the client never backs
      // off from. This must precede isCaptureInfrastructureError.
      if((error instanceof BrowserUnavailableError) || (error instanceof BrowserSupersededError)) {

        throw new StreamSetupError("Browser temporarily unavailable.", 503, "The capture system is recovering. Please retry shortly.", { cause: error });
      }

      // The browser is alive and still serving the captures it started, but it can no longer start another, so this tune is refused before a page is even opened.
      // Quiet like its sibling above, because the alarm fired once when the mark was recorded. The message differs because the wait ends differently: the client is
      // waiting on the browser's own streams to end rather than on a cooldown to elapse.
      if(error instanceof BrowserCaptureImpairedError) {

        throw new StreamSetupError("Browser temporarily unavailable.", 503, "The browser can no longer start captures and will relaunch once its current streams " +
          "end. Please retry shortly.", { cause: error });
      }

      // createPageWithCapture handles its own cleanup on failure (closes page, kills FFmpeg).
      const errorMessage = formatError(error);
      const lowerMessage = errorMessage.toLowerCase();
      const benignPatterns = [ "abort", "session closed" ];
      const isBenign = benignPatterns.some((pattern) => lowerMessage.includes(pattern));

      if(!isBenign) {

        LOG.error("Stream setup failed for %s: %s.", url, errorMessage);
      }

      // Capture infrastructure errors should return 503 to signal Channels DVR to back off. These include Chrome capture state issues, capture-lock turn-wait
      // timeouts, and stream initialization failures. Using 503 with Retry-After prevents retry storms when there's a systemic issue. isCaptureInfrastructureError
      // (recovery.ts) is the single source of truth for this classification, shared with the browser supervisor's readiness detection.
      const isCaptureError = isCaptureInfrastructureError(errorMessage);

      // A capture-infrastructure failure may mean the browser, though still connected, can no longer start captures. Hand it to the passive mid-life detector, which
      // (guarded and single-flight, in the background) re-verifies capture readiness and marks the browser impaired if confirmed - its running captures continue, new
      // capture starts are refused at acquire(), and the adapter relaunches it once no stream depends on it. Fire-and-forget: it must never delay this response.
      if(isCaptureError) {

        noteCaptureInfrastructureFailure();
      }

      // A failed tune on a service currently marked needs-sign-in most likely failed AT the auth wall, so the user-facing message leads with the remedy.
      throw new StreamSetupError("Stream error.", isCaptureError ? 503 : 500, withSignInGuidance("Failed to start stream.", channelName, serviceName), { cause: error });
    }

    const { captureSession, context, directTune, manifestInterception, page } = captureResult;

    // Hold the page, capture session, and interceptor on a scope guard so a tune-verification failure below disposes them structurally rather than repeating the
    // teardown inline. Push order mirrors the capture-setup resource stack (page, capture session, interception): the interception is registered last so it disposes
    // first, but its disposal is a CDP observer detach with no ordering dependency on either peer; the pair that must stay ordered is capture-session-before-page,
    // so the capture stream is destroyed and STOP_RECORDING fires while the browser is still connected, ahead of the page close. On success we move() the guard and
    // hand ownership to the cleanup closure (and, once the session is installed on the registry entry, to terminateStream).
    using owned = new DisposableStack();

    owned.adopt(page, disposePage);
    owned.use(captureSession);

    if(manifestInterception) {

      owned.use(manifestInterception);
    }

    // Tune verification. The shared adjudication stage finalizes the manifest interceptor and confirms the captured master manifest URL belongs to the channel
    // that was just tuned. This step makes setupStream "verified by construction" - every consumer of StreamSetupResult (HLS preroll, HLS blocking, MPEG-TS,
    // native proxy, capture mode) receives a stream guaranteed to be on the requested channel without having to opt in or coordinate. Streams with no manifest
    // interception at all (DRM-cached channels, tab replacements) have nothing to adjudicate and skip the step entirely; verifyManifestSelection owns which of
    // the remaining ones it can speak to. A failure reason throws StreamSetupError so the existing failure path marks channel health, terminates the pending
    // registry entry, and surfaces a clear error - never silently delivers the wrong channel. The scope guard disposes the capture session, interceptor, and page
    // as the throw unwinds.
    if(manifestInterception) {

      const verifyError = await adjudicateChannelSelection(manifestInterception, profile, directTune);

      if(verifyError) {

        const failureLabel = channel?.name ?? channelName ?? url;

        throw new StreamSetupError("Tune verification failed: " + verifyError, 502,
          withSignInGuidance("Tune verification failed for " + failureLabel + ". " + verifyError, channelName, serviceName));
      }
    }

    // Monitor stream info for status updates. The serviceTag enables service-specific monitoring flags (e.g., tinySegmentThreshold).
    const monitorStreamInfo: MonitorStreamInfo = {

      channelName: channel?.name ?? null,
      numericStreamId,
      serviceName,
      serviceTag: getDomainConfig(url)?.serviceTag,
      startTime
    };

    // Start the health monitor for this stream.
    const monitor = monitorPlaybackHealth(page, context, profile, url, streamId, monitorStreamInfo, onCircuitBreak, onTabReplacement);

    // Cleanup function. Releases all resources associated with the stream. Safe to call more than once. completeStreamSetup uses it as the fallback teardown
    // when the pending entry is terminated before the capture session is installed on it; once installed, terminateStream disposes the same session (safe to
    // call disposal more than once). Disposes the capture session (kill -> destroy -> stop) before closing the page so STOP_RECORDING fires while the browser is
    // connected.
    let cleanupCompleted = false;

    const cleanup = async (): Promise<void> => {

      if(cleanupCompleted) {

        return;
      }

      cleanupCompleted = true;

      monitor.dispose();
      captureSession.dispose();
      disposePage(page);

      // Settle the window against the policy now that this stream's resources are gone.
      await deps.syncWindowVisibility();
    };

    // Success: transfer ownership out of the scope guard. The cleanup closure and, once the session is installed on the registry entry, terminateStream become
    // responsible for disposing the page and capture session; the interceptor continues into tune verification and native streaming.
    owned.move();

    // Return the setup result.
    return {

      captureSession,
      channelName: channel?.name ?? null,
      cleanup,
      directTune,
      manifestInterception,
      monitor,
      numericStreamId,
      page,
      probeIdentity,
      profile,
      profileName,
      serviceName,
      startTime,
      streamId,
      url
    };
  });
}

// Channel Re-establishment.

/**
 * Options for re-establishing a stream's channel on its page.
 */
export interface ReestablishChannelManifestOptions {

  // The channel display name, used only to enrich the log context; null for an ad-hoc URL stream.
  channelName: Nullable<string>;

  // The live browser page to re-establish on. Supplied by the caller rather than captured, so a caller holding a page this stream has moved to drives the
  // re-establishment against the page it actually owns.
  page: Page;

  // The stream's resolved site profile, which decides the navigation wait strategy, the overlay policy, and the channel-selection strategy.
  profile: ResolvedSiteProfile;

  // The stream's string ID, which establishes the log context when the caller supplies none.
  streamIdStr: string;

  // The stream's configured channel URL. The re-establishment always navigates here and always runs channel selection.
  url: string;
}

/* Re-establishes a stream's channel on its page and returns the fresh manifest interception, for the native token-refresh path. It runs the same establishment
 * composition the tune path runs - navigate under this path's own policy, stamp the observation epoch, initialize playback under the shared bound, then adjudicate
 * what the interception selected - so there is exactly one establishment sequence in this system. The deliberate divergences from the tune path are each owned here.
 * Navigation is single-attempt: both callers of the refresh chain bring their own retry ladders (the proactive timer re-arms failures with bounded backoff, and the
 * monitor escalates a persisting stall to capture fallback), so retrying inside would stack ladders and stretch a recovery the project wants fast. The route is
 * always the configured channel url with full channel selection: a cached direct URL and a skipped selection belong to the tune's own optimization, and the
 * re-establishment takes the click-verified route instead, so the kind it finalizes against carries no cached-direct-URL term. And a category selector's
 * re-resolved call sign is not persisted here: persistence belongs to the tune lifecycle, and the recovery paths already re-tune without it. Playback
 * initialization is bounded by the same race the tune uses, with the same abandon-on-timeout semantics; the remnant is bounded and act-limited - its phases expire
 * on their own internal timeouts, its consent poll can only reject cookie banners and dismiss per-site modals within its fixed window, and a later navigation on
 * the page force-settles whatever remains - so a subsequent refresh attempt starting on this page meets at worst a dying consent poll, never a competing channel
 * click. Failures normalize to null with a warning rather than throwing - the caller is a background refresh cycle whose ladder already owns the endgame. The body
 * runs under the stream's log context so the composed primitives' own lines carry the stream prefix: the ambient context is kept when a caller already established
 * one (the monitor's recovery path carries a richer context, show-name resolution included, that a nested run would replace rather than merge), and is supplied
 * only on the proactive timer's path, which has none. The page's audio is re-muted on the initialization's own settlement rather than at any fixed step: playback
 * establishment unmutes by direct property write, so an already-playing element stays audible until re-muted, and attaching the re-mute to the initialization
 * promise covers every outcome - success, failure, and a timed-out attempt whose establishment completes late - while the play-override the native upgrade
 * registered keeps future play() calls muted without re-registration.
 *
 * @param options - The stream facts the re-establishment closes over. See ReestablishChannelManifestOptions.
 * @returns The verified manifest interception, or null when the channel could not be re-established.
 */
export async function reestablishChannelManifest(options: ReestablishChannelManifestOptions): Promise<Nullable<ManifestInterceptionResult>> {

  const { channelName, page, profile, streamIdStr, url } = options;

  const establish = async (): Promise<Nullable<ManifestInterceptionResult>> => {

    try {

      // The navigation allowance handed to the budget is a single attempt's timeout, since this path navigates once by design; establishmentBudgetMs owns why
      // the observer has to outlive that allowance and every step after it.
      const budgetMs = establishmentBudgetMs(CONFIG.streaming.navigationTimeout);

      // Scope-bind the interceptor with "using" so its CDP observer is disposed on every exit from this function, including the early returns below.
      using handle = await installManifestInterceptor(page, budgetMs);

      if(!handle) {

        LOG.warn("The channel for %s could not be re-established: the manifest interceptor did not install.", channelName ?? url);

        return null;
      }

      // Establish the channel through the shared composition: navigate, stamp the observation epoch - which fences anything the reloaded page auto-played so it
      // cannot win adjudication over the channel the selection lands on - then run playback initialization under the same bound the tune path uses.
      const tuneResult = await establishChannelPlayback(page, profile, handle, {

        initOptions: { requestedUrl: url },

        // Navigate through the profile's own wait strategy, single-attempt by design. A navigation failure throws to the catch below and normalizes to null.
        navigate: async (): Promise<void> => {

          await navigateToPage(page, url, profile);
        },

        // Restore the page's mute when the initialization settles. Playback establishment unmutes by direct property write, so an element that is already
        // playing stays audible until it is re-muted, and attaching the restore to the settlement is what covers every outcome rather than only the happy one.
        onInitSettled: (): void => {

          void muteExistingVideos(page);
        }
      });

      // Adjudicate honestly: the kind formula gets no cached-direct-URL term here, because the route above never takes one.
      const verifyError = await adjudicateChannelSelection(handle, profile, computeDirectTuneKind({ profile, strategyDirectTune: tuneResult.directTune ?? false }));

      // On a verifier-bearing master path the adjudication has already awaited this promise; on the vacuous path this await is the one that waits out the
      // finalize settle. Reading it before the verification result is contract rather than preference: an establishment that intercepted nothing has to report
      // exactly that, and the verification gates raise no objection for a null interception anyway, so the check order is what keeps each warning accurate to
      // its own outcome.
      const interception = await handle.promise;

      if(!interception) {

        LOG.warn("The channel for %s could not be re-established: no manifest was intercepted.", channelName ?? url);

        return null;
      }

      if(verifyError) {

        LOG.warn("The re-established channel for %s did not verify: %s", channelName ?? url, verifyError);

        return null;
      }

      return interception;
    } catch(error) {

      LOG.warn("The channel for %s could not be re-established: %s.", channelName ?? url, formatError(error));

      return null;
    }
  };

  // Keep a caller's own context rather than nesting a thinner one inside it - the monitor's recovery context carries show-name resolution this frame cannot
  // rebuild - and establish one only where none exists, which is the proactive refresh timer's bare callback.
  if(getStreamContext()) {

    return establish();
  }

  return runWithStreamContext({ channelName: channelName ?? undefined, streamId: streamIdStr, url }, establish);
}

// Capture Readiness Verification.

/**
 * Verifies that a freshly-launched Chrome instance can actually capture, by running a real capture acquisition against a throwaway page on it. This is the
 * capability tier of the browser launch gate: browser/index.ts injects it via setCaptureProbe and runs it inside launchReadyBrowser, so a browser is published as
 * ready only after its capture capability is verified - at boot AND at every relaunch, not just startup. It exercises the exact acquisition path that fails when
 * the capture extension is unregistered, so a dead extension is detected immediately rather than causing every subsequent stream request to fail.
 *
 * Each attempt creates a temporary page, attempts a short capture, and tears down both cleanly. The teardown waits for the extension to confirm the recording
 * stopped, bounded by a ceiling, before closing the page, so Chrome's tabCapture state is released rather than lingering into the first real request.
 *
 * After a system reboot or a fresh relaunch, Chrome's display stack or capture extension may not be ready immediately. The probe retries up to PROBE_MAX_ATTEMPTS
 * times with a delay between attempts, giving the system time to settle before giving up. At boot this prevents a rapid restart storm where the service manager
 * relaunches PrismCast repeatedly, each attempt orphaning a Chrome process; at relaunch it provides in-launch settling before the supervisor counts a launch failure.
 * @param browser - The Chrome instance to verify (the local instance being launched, passed in rather than re-acquired to avoid re-entering the launch in flight).
 */
export async function verifyCaptureSystem(browser: Browser): Promise<void> {

  const PROBE_MAX_ATTEMPTS = 3;
  const PROBE_RETRY_DELAY = 5000;

  for(let attempt = 1; attempt <= PROBE_MAX_ATTEMPTS; attempt++) {

    // The launch gate runs its capture probe OFF the capture lock, deliberately: it fires pre-publish, when supervisor.current() is null, so a wedged gate task would
    // have no recovery target and would jam the shared lock. GATE mode keeps the acquisition bounded internally instead.
    // eslint-disable-next-line no-await-in-loop -- Sequential retries are intentional; each probe must complete before deciding whether to retry.
    const result = await attemptCaptureProbe(browser, { boundMs: CAPTURE_PROBE_TIMEOUT_MS, kind: "gate" });

    // Probe succeeded.
    if(result === null) {

      return;
    }

    // If we have retries remaining, log a warning and wait before the next attempt.
    if(attempt < PROBE_MAX_ATTEMPTS) {

      LOG.warn("Capture probe attempt %d of %d failed: %s. Retrying in %ds.", attempt, PROBE_MAX_ATTEMPTS, result, PROBE_RETRY_DELAY / 1000);

      // eslint-disable-next-line no-await-in-loop -- Deliberate delay between sequential retry attempts.
      await delay(PROBE_RETRY_DELAY);
    } else {

      /* On Windows the probe's own error rarely names the cause, and two conditions account for nearly every failure there: a virtualization layer sitting between
       * Chrome and the display, and a profile that will not load an unpacked extension. Naming both in the thrown message saves the user a support round trip.
       * The message is unchanged on every other platform, and the hint is appended after the probe's own text so the "timed out" substring the
       * capture-infrastructure classifier reads is still present.
       */
      const windowsHint = (process.platform === "win32") ? " On Windows, Hyper-V or WSL can interfere with Chrome's capture pipeline, and Chrome refuses to " +
        "load the capture extension unless the profile has extension developer mode enabled." : "";

      // The probe's message is a diagnostic fragment that may or may not end in a period, so we terminate it before appending a sentence of our own.
      const detail = ((windowsHint.length > 0) && !result.endsWith(".")) ? (result + ".") : result;

      throw new Error("Capture system verification failed after " + String(PROBE_MAX_ATTEMPTS) + " attempts: " + detail + windowsHint);
    }
  }
}

/**
 * The capture probe's operating mode. The launch gate bounds the acquisition with an internal race (a pre-publish bypass off the capture lock); the mid-life path
 * runs on the capture lock, which owns the outer bounds, so its acquisition is awaited raw and SELF-TIMED against the criterion: the pass/fail judgment counts the
 * acquisition's own latency alone, never the teardown that follows it. That span covers the acquisition whole, retry included - a browser whose capture needs a
 * retry to start is exactly a browser worth reverifying. boundMs is the criterion in both arms; the mid-life arm also carries the lock's AbortSignal so a probe
 * abandoned at the outer deadline retires the stream it produced.
 */
type CaptureProbeMode = { boundMs: number; kind: "gate" } | { boundMs: number; kind: "midlife"; signal: AbortSignal };

/**
 * Executes a single capture probe attempt. Creates a temporary page on the given browser, tries to start a capture stream, and tears everything down cleanly. It
 * NEVER throws in either mode: it returns null on success or an error-message string on failure, so callers branch on the string. The two modes differ only in how
 * the acquisition is bounded - see CaptureProbeMode.
 * @param browser - The Chrome instance to probe.
 * @param mode - The operating mode: gate (internal acquisition race) or midlife (self-timed acquisition on the lock).
 * @param clock - Clock used for the mid-life self-timing and the teardown confirmation. Defaults to realClock.
 * @returns Null on success, or an error message string on failure.
 */
async function attemptCaptureProbe(browser: Browser, mode: CaptureProbeMode, clock: Clock = realClock): Promise<Nullable<string>> {

  // The probe page opens behind whatever the window is showing; it needs its tab selected only for its own capture start, which takes the selection itself.
  const page = await browser.newPage({ background: true });

  registerManagedPage(page);

  // The probe page carries the preset-sized layout at the display's density, the surface a capture page starts from before its own declaration, so the probe's
  // acquisition keeps the shape the field validated.
  const surface = await emulateLayoutSurface(page);

  // Tears the probe page down cleanly: retire the raw capture stream (destroy plus the stop confirmation) while the browser is still connected, unregister the
  // managed page, then close it. Shared by every success and self-timed-failure path in both modes.
  const teardown = async (stream: CaptureStream): Promise<void> => {

    await retireRawStream(stream, clock);
    unregisterManagedPage(page);

    if(!page.isClosed()) {

      await page.close();
    }
  };

  try {

    // Use the same capture MIME type and surface as the runtime. The stale state error occurs at the tabCapture API level before encoding matters, so matching
    // those runtime constraints ensures the probe exercises a representative acquisition. The constraints are pinned to the dimensions the declaration above
    // returned, so the probe holds its track to the surface the page actually carries rather than to a second read of the preset.
    const useFFmpeg = CONFIG.streaming.captureMode === "ffmpeg";
    const captureMimeType = useFFmpeg ? getCaptureMimeType() : NATIVE_FMP4_MIME_TYPE;

    const streamOptions: CaptureStreamOptions = {

      audio: true,
      mimeType: captureMimeType,
      video: true,
      videoConstraints: {

        mandatory: {

          maxFrameRate: 30,
          maxHeight: surface.height,
          maxWidth: surface.width,
          minFrameRate: 30,
          minHeight: surface.height,
          minWidth: surface.width
        }
      }
    };

    // GATE mode: bound the acquisition with an internal timeout. On a lapse the acquisition is still running, so attach a both-callback handler that retires a
    // late-arriving stream (best-effort; the page may already be closing) and consumes a late rejection. The bounded wait already observes the promise's rejection,
    // so a fulfillment-only handler would create unhandled-rejection noise. This cleans up the orphan without serializing successive gate attempts against one another.
    if(mode.kind === "gate") {

      const streamPromise = acquireCaptureStream(page, streamOptions);
      const timeoutError = new Error(CAPTURE_PROBE_TIMEOUT_MESSAGE);

      let stream: CaptureStream;

      try {

        stream = await waitWithTimeout(streamPromise, mode.boundMs, timeoutError);
      } catch(error) {

        // Only the internal timeout leaves the acquisition running; an in-time rejection produced no stream to clean up and is already observed by the bounded wait.
        if(error === timeoutError) {

          void streamPromise.then((late) => {

            void retireRawStream(late, clock);
          }, (reason: unknown) => {

            LOG.debug("streaming:setup", "A late gate capture stream rejected after the probe timeout: %s.", formatError(reason));
          });
        }

        throw error;
      }

      await teardown(stream);

      LOG.info("Capture system verified successfully.");

      return null;
    }

    // MID-LIFE mode: await the acquisition raw and self-time it. The turn (owned by the lock) spans the whole task, but the pass/fail CRITERION is the
    // acquisition's own latency against boundMs, measured without racing or abandoning anything, so the teardown that follows never counts against it.
    const startedAt = clock.now();
    const stream = await acquireCaptureStream(page, streamOptions, { signal: mode.signal });
    const elapsed = clock.now() - startedAt;

    await teardown(stream);

    // Report failure when the caller abandoned this probe at the lock's outer deadline (unobservable in practice - the lock already rejected the caller - but it keeps
    // the never-throw contract and prevents stranding a capture), or when the acquisition's own latency exceeded the criterion bound.
    if(mode.signal.aborted || (elapsed > mode.boundMs)) {

      return CAPTURE_PROBE_TIMEOUT_MESSAGE;
    }

    LOG.info("Capture system verified successfully.");

    return null;
  } catch(error) {

    const errorMessage = formatError(error);

    // Clean up the test page.
    unregisterManagedPage(page);

    if(!page.isClosed()) {

      page.close().catch(() => { /* Fire-and-forget during error cleanup. */ });
    }

    return errorMessage;
  }
}

// Passive Mid-Life Capture-Death Detection.

/* A browser can be capture-ready at launch and lose its capture capability later - the extension wedges, tabCapture stalls, a display reconfiguration leaves the
 * process unable to start another capture while the captures already running continue - without ever firing a "disconnected" event, so neither the launch gate nor
 * the disconnect handler would catch it. This detector rides a signal that is already happening: a stream-setup failure carrying a capture-infrastructure
 * signature. The probe is the arbiter, serialized through the capture lock so it can never race a real stream's capture acquisition, and it runs in the background,
 * single-flight, so it never delays a response or stacks up. A probe that never obtained a turn is no verdict at all, because a busy lock is evidence about load
 * rather than about the browser. On a confirmed failure the one recovery action runs: mark the browser, which leaves its running captures alone, refuses new
 * capture starts at acquire(), and leaves the adapter to decide when the relaunch is safe.
 */

// At most one mid-life re-verification runs at a time across the process, so a burst of capture-infrastructure failures triggers a single probe, not a storm.
let captureReverificationInProgress = false;

/**
 * What one mid-life capture probe established about the browser. A `captured` outcome is the browser proving it can still start a capture; `failed` is the verdict
 * the mark rests on; `inconclusive` is the probe never having run, which is evidence of neither.
 */
type CaptureProbeOutcome =
  { readonly kind: "captured" } |
  { readonly kind: "failed"; readonly reason: string } |
  { readonly kind: "inconclusive"; readonly reason: string };

/**
 * Classifies a throw from the probe's run on the capture lock into a verdict or the absence of one. A turn-wait timeout means the probe never got to run: with
 * other streams tuning at the same moment it queued behind their legitimate acquisitions, and a lock that is busy says something about load, not about the browser
 * - a holder that is genuinely hung marks the browser through its own wedge. Every other value, the outer deadline above all, means the probe held its turn and its
 * own acquisition did not settle, which is exactly the failure the mark exists for. Pure and total, so the distinction the detector rests on is pinnable without a
 * browser.
 * @param error - The value the lock run threw.
 * @returns The outcome the detector acts on.
 */
export function classifyCaptureProbeFailure(error: unknown): CaptureProbeOutcome {

  if(error instanceof CaptureTurnTimeoutError) {

    return { kind: "inconclusive", reason: formatError(error) };
  }

  return { kind: "failed", reason: formatError(error) };
}

/**
 * Runs one capture probe as a task on the capture lock, so it cannot race a concurrent capture acquisition (which would draw a spurious "Cannot capture a tab
 * with an active stream"). The lock holds the turn across the probe's full task - the acquisition plus the stop confirmation inside attemptCaptureProbe - so the
 * next initialization does not start until the probe's capture is fully released. The mid-life probe self-times its acquisition against `timeout`; the lock's
 * outer deadline adds a teardown allowance over it as a safety net.
 * @param browser - The Chrome instance to probe.
 * @param timeout - Maximum time in milliseconds to wait for the probe's acquisition to respond (the self-timed criterion).
 * @returns Captured when the browser started a capture, failed when it could not, inconclusive when the probe never obtained its turn.
 */
async function probeCaptureSerialized(browser: Browser, timeout: number): Promise<CaptureProbeOutcome> {

  try {

    const failure = await captureLock.run((signal: AbortSignal): Promise<Nullable<string>> => attemptCaptureProbe(browser, { boundMs: timeout, kind: "midlife",
      signal }), {

      deadlineMessage: CAPTURE_PROBE_TIMEOUT_MESSAGE,
      deadlineMs: timeout + PROBE_TEARDOWN_ALLOWANCE_MS,

      // The probe's wedge is a loud warning only, never a second verdict: by wedge time the detector has already marked this browser from the returned failure
      // (the outer deadline fires roughly 22s earlier), so a mark raised here would land on an instance that already carries one.
      onWedge: (): void => {

        LOG.warn("A mid-life capture probe has wedged past the recovery bound; the browser was already marked when the probe's deadline fired, so this wedge " +
          "is informational.");
      },
      turnWaitMs: CONFIG.streaming.navigationTimeout
    });

    return (failure === null) ? { kind: "captured" } : { kind: "failed", reason: failure };
  } catch(error) {

    return classifyCaptureProbeFailure(error);
  }
}

/**
 * Passive mid-life capture-death detection, called from the stream-setup failure path when the failure carries a capture-infrastructure signature. It re-verifies
 * the browser's capture capability with a lock-serialized probe in the background and, on a confirmed failure, marks the browser: its running captures continue,
 * new capture starts are refused at acquire(), and the adapter relaunches it once nothing depends on it. Any refused start is worth a probe - a browser that is
 * demonstrably capturing for someone else can still be unable to start another - while a browser that already carries a mark needs no second verdict.
 * Fire-and-forget so the failing request's response is not delayed; single-flight so a burst of failures triggers at most one probe.
 */
function noteCaptureInfrastructureFailure(): void {

  // Single-flight: a re-verification is already deciding the browser's fate; do not stack another. Read first so a re-verify already in flight short-circuits
  // before the browser lookup below is even performed.
  if(captureReverificationInProgress) {

    return;
  }

  // A readiness probe needs a connected browser to exercise. If none is published, a disconnect already handled the readiness loss.
  const browser = getBrowserInstance();

  if(!browser) {

    return;
  }

  // A refused start against a browser that is already marked belongs to a tune that acquired it an instant before the mark landed. The verdict is recorded and the
  // relaunch is scheduled, so a probe here would spend the capture lock re-establishing what is already known.
  if(getCaptureImpairment() !== null) {

    return;
  }

  captureReverificationInProgress = true;

  // Run in the background so the failing request's 503 response is not delayed by the probe, which can take up to the probe timeout.
  void (async (): Promise<void> => {

    try {

      const outcome = await probeCaptureSerialized(browser, CAPTURE_PROBE_TIMEOUT_MS);

      switch(outcome.kind) {

        case "failed": {

          // The probe confirmed the browser cannot start a capture though it is still connected. We pass the exact instance we probed, so a disconnect and relaunch
          // during the probe leaves the fresh browser unmarked.
          noteBrowserCaptureImpaired(browser, outcome.reason);

          break;
        }

        case "inconclusive": {

          LOG.debug("streaming:setup", "Mid-life capture re-verification could not obtain a turn on the capture lock (%s); no verdict.", outcome.reason);

          break;
        }

        case "captured": {

          // The browser started a capture, so it is healthy and the setup failure that brought us here belonged to the stream rather than to the browser.
          break;
        }
      }
    } finally {

      captureReverificationInProgress = false;
    }
  })();
}
