/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.ts: Common stream setup logic for PrismCast.
 */
import type { Browser, Frame, Page } from "puppeteer-core";
import { BrowserSupersededError, BrowserUnavailableError, getBrowserInstance, getCurrentBrowser, getStream, invalidateBrowser, minimizeBrowserWindow,
  registerManagedPage, setCaptureProbe, unregisterManagedPage } from "../browser/index.ts";
import { LOG, delay, extractDomain, formatError, raceWithTimeout, registerAbortController, resolveFFmpegPath, retryOperation, runWithStreamContext,
  spawnFFmpeg, startTimer } from "../utils/index.ts";
import type { MonitorHandle, TabReplacementResult } from "./recovery.ts";
import type { Nullable, ResolvedChannel, ResolvedSiteProfile, UrlValidationResult } from "../types/index.ts";
import { getAllStreams, getNextStreamId } from "./registry.ts";
import { getAuthDomainForChannel, getServiceDisplayName, resolveServiceKey } from "../config/services.ts";
import { getProfileForChannel, getProfileForUrl, getProfiles, resolveProfile } from "../config/profiles.ts";
import { getProviderByStrategy, invalidateDirectUrl, resolveDirectUrl } from "../browser/channelSelection.ts";
import { initializePlayback, injectVideoSelector, navigateToPage } from "../browser/video.ts";
import { CONFIG } from "../config/index.ts";
import type { CaptureSession } from "./captureSession.ts";
import type { FFmpegProcess } from "../utils/index.ts";
import type { ManifestInterceptorHandle } from "../browser/manifestInterceptor.ts";
import type { MonitorStreamInfo } from "./monitor.ts";
import type { Readable } from "node:stream";
import { chromeFetch } from "../utils/index.ts";
import { createCaptureSession } from "./captureSession.ts";
import { getCachedEncryption } from "../native/probe.ts";
import { getCaptureMimeType } from "./codec.ts";
import { getDomainAuthState } from "../config/health.ts";
import { getDomainConfig } from "../config/sites.ts";
import { getEffectiveViewport } from "../config/presets.ts";
import { installManifestInterceptor } from "../browser/manifestInterceptor.ts";
import { isCaptureInfrastructureError } from "./recovery.ts";
import { isChannelSelectionProfile } from "../types/index.ts";
import { monitorPlaybackHealth } from "./monitor.ts";
import { mutateChannels } from "../config/userChannels.ts";
import { pipeline } from "node:stream/promises";
import { resizeAndMinimizeWindow } from "../browser/cdp.ts";

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

// Capture initialization queue. Chrome's tabCapture extension can only initialize one capture at a time - concurrent getStream() calls fail with "Cannot capture a
// tab with an active stream." We serialize capture initialization using a promise chain so requests execute sequentially. Once a capture is established, it runs
// concurrently with other captures without issue.
let captureQueue: Promise<void> = Promise.resolve();
let captureQueueDepth = 0;

// Threshold for logging a warning when the capture queue depth is unusually high. Under normal operation, the queue depth is 0-2. Higher values indicate
// many simultaneous stream requests competing for Chrome's single-threaded capture initialization.
const CAPTURE_QUEUE_DEPTH_WARNING = 5;

/**
 * A reserved position in the capture-initialization queue. The caller awaits `ready` (the predecessor has cleared), runs its getStream init, then calls `release`
 * so the next waiter may proceed. The caller chooses WHEN to release, which is the whole point: a real stream releases as soon as getStream settles, so the next
 * init overlaps its post-init setup; the readiness probe releases only after its full teardown, so STOP_RECORDING settles before the next init starts.
 */
interface CaptureQueueSlot {

  // Resolves once the predecessor capture has cleared, bounded by navigationTimeout so a single wedged getStream cannot block the queue forever. Rejects with
  // "Capture queue wait timed out." on that timeout, having already released this slot so a timed-out waiter never wedges the queue for the next caller.
  readonly ready: Promise<void>;

  // Releases the slot for the next waiter. Idempotent (release-once), so the success path, the catch, and the timeout self-release can all call it safely.
  readonly release: () => void;
}

/**
 * Reserves the next position in the capture-initialization queue. This is the single place that advances `captureQueue`: every getStream initialization - real
 * streams in createPageWithCapture and the mid-life readiness probe alike - serializes through it, because Chrome's tabCapture extension rejects concurrent
 * getStream() initialization with "Cannot capture a tab with an active stream." The slot owns the queue mechanics (advance, depth accounting, bounded predecessor
 * wait, release-once); the caller owns only the release timing.
 * @returns The reserved slot.
 */
function acquireCaptureQueueSlot(): CaptureQueueSlot {

  const previous = captureQueue;

  captureQueueDepth++;

  if(captureQueueDepth >= CAPTURE_QUEUE_DEPTH_WARNING) {

    LOG.warn("Capture queue depth is %d. Multiple stream requests are competing for Chrome's capture initialization.", captureQueueDepth);
  }

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise, resolve } = Promise.withResolvers<void>();

  captureQueue = promise;

  let released = false;

  const release = (): void => {

    if(!released) {

      released = true;
      captureQueueDepth--;
      resolve();
    }
  };

  // Bound the predecessor wait so a single stuck getStream cannot block all future captures indefinitely; on timeout we release this slot before surfacing the error
  // so the next waiter is not blocked by our failure.
  const ready = raceWithTimeout(previous, CONFIG.streaming.navigationTimeout, new Error("Capture queue wait timed out.")).catch((error: unknown) => {

    release();

    throw error;
  });

  return { ready, release };
}

// Maximum number of times createPageWithCapture() will retry when it detects that the page was closed while waiting in the capture queue (e.g., due to a browser
// crash). An explicit guard prevents unbounded recursion.
const MAX_PAGE_CLOSED_RETRIES = 3;

// Maximum time in milliseconds to wait for a single capture probe's getStream() to respond. Shared by the launch-gate verification (verifyCaptureSystem) and the
// mid-life re-verification, so both tiers exercise the capture path with the same bound.
const CAPTURE_PROBE_TIMEOUT_MS = 5000;

// Wire the capture-readiness probe into the browser launch gate. setup.ts owns getStream and the unrecoverable stale-mutex process.exit decision; browser/index.ts
// owns the launch lifecycle. Injecting verifyCaptureSystem here (setup.ts already depends on browser/index.ts) keeps the dependency one-directional and breaks the
// cycle, mirroring the browserAccessors seam between login.ts and index.ts. It runs once at module load, before any browser launch.
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

  // Whether the channel was tuned via a direct mechanism (cached URL or API interception) rather than DOM interaction.
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

  // When true, skips CDP manifest interception. Set when the probe cache already has a "drm" result for this channel, avoiding 15 seconds of wasted CDP overhead.
  skipManifestInterception?: boolean;

  // The stream ID string for logging (e.g., "cnn-5jecl6").
  streamId: string;

  // When true, adds a settling delay between fullscreen setup and window minimize during tab replacement. Chrome's compositor may not fully stabilize the video
  // surface after fullscreen is established, and minimizing too quickly can cause the captured content to appear zoomed into the top-left corner.
  tabReplacement?: boolean;

  // The URL to navigate to and capture.
  url: string;

  // Internal retry counter for page-closed-during-queue recovery. Callers should not set this - it is incremented automatically when createPageWithCapture()
  // retries after detecting a dead page from a browser crash that occurred while waiting in the capture queue.
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

  // Whether the channel was tuned via a direct mechanism (cached URL or API interception) rather than DOM interaction.
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
 * @returns The page, context, and capture session (which owns the raw capture stream and any FFmpeg child).
 * @throws Error if page creation, capture initialization, or navigation fails.
 */
export async function createPageWithCapture(options: CreatePageWithCaptureOptions): Promise<CreatePageWithCaptureResult> {

  const captureElapsed = startTimer();
  const { comment, onFFmpegError, profile, streamId, url } = options;

  // Acquire every resource on a DisposableStack so that any throw - in capture initialization, navigation, or playback setup - disposes them structurally as the
  // function unwinds, in last-acquired-first order (capture session before page, so the capture stream is destroyed and STOP_RECORDING fires while the browser is
  // still connected, before the page closes). On success we move() the stack to disarm it and transfer ownership to the caller. This centralizes teardown that
  // would otherwise be repeated in each failure path, and closes the navigation-path leak of the manifest interceptor.
  using resources = new DisposableStack();

  // Create browser page.
  const browser = await getCurrentBrowser();
  const page = await browser.newPage();

  registerManagedPage(page);
  resources.adopt(page, disposePage);

  await page.setBypassCSP(true);

  // Inject the shared video selector helper into the browser context. This must happen before navigation so the helper is available when evaluate calls run during
  // initializePlayback (startVideoPlayback, applyVideoStyles, verifyFullscreen, lockVolumeProperties) and subsequent health monitoring (getVideoState).
  await injectVideoSelector(page);

  // Install CDP manifest interceptor before navigation. This listener captures .m3u8 URLs from the browser's network requests, enabling native HLS streaming for
  // services that use clear or AES-128 encrypted streams. Skipped for tab replacements (native proxy is independent of capture) and for channels already known to
  // use DRM (avoids 15 seconds of wasted CDP overhead per tune). The await ensures the CDP session and Network domain are ready before navigation begins.
  const manifestInterception = (!options.tabReplacement && !options.skipManifestInterception) ? await installManifestInterceptor(page) : null;

  if(manifestInterception) {

    resources.use(manifestInterception);
  }

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

  // Reserve our turn in the capture-initialization queue up front, so the catch can release it on any failure. The slot owns the queue mechanics; we choose when to
  // release - here, as soon as getStream settles (below), so the next caller's init can overlap this stream's remaining setup.
  const slot = acquireCaptureQueueSlot();

  // Initialize media stream capture.
  try {

    const streamOptions = {

      audio: true,
      audioBitsPerSecond: CONFIG.streaming.audioBitsPerSecond,
      mimeType: captureMimeType,
      video: true,
      videoBitsPerSecond: CONFIG.streaming.videoBitsPerSecond,

      // Constrain capture frame rate to a 30-60 fps band: 60 is the live-TV ceiling, and a 30 floor keeps motion smooth even when the user configures a lower rate.
      // The ceiling is fixed at 60 while the floor follows the user's configured rate (clamped into the band), so the encoder favours the requested rate but never
      // drops below 30. The readiness probe (attemptCaptureProbe) instead pins both bounds to a flat 30 because its getStream() fails or succeeds at the tabCapture
      // API level before encoding matters, so a representative-but-minimal constraint set suffices there.
      videoConstraints: {

        mandatory: {

          maxFrameRate: 60,
          maxHeight: getEffectiveViewport(CONFIG).height,
          maxWidth: getEffectiveViewport(CONFIG).width,
          minFrameRate: Math.max(30, Math.min(60, CONFIG.streaming.frameRate)),
          minHeight: getEffectiveViewport(CONFIG).height,
          minWidth: getEffectiveViewport(CONFIG).width
        }
      }
    } as unknown as Parameters<typeof getStream>[1];

    // Serialize capture initialization: wait for our turn before calling getStream(), because Chrome's tabCapture extension rejects concurrent initialization. The
    // slot bounds this wait and self-releases on a wedged-predecessor timeout, so the resulting "Capture queue wait timed out." flows straight to the catch below.
    await slot.ready;

    // After the queue wait, verify our page is still connected. If Chrome crashed while we were waiting, our page is dead and we need to start over with a
    // fresh page on the new browser. Release our queue position first so subsequent callers aren't blocked.
    if(page.isClosed()) {

      slot.release();
      unregisterManagedPage(page);

      const retryCount = options._pageClosedRetries ?? 0;

      if(retryCount >= MAX_PAGE_CLOSED_RETRIES) {

        throw new Error("Browser crashed too many times during capture initialization.");
      }

      return await createPageWithCapture({ ...options, _pageClosedRetries: retryCount + 1 });
    }

    const streamPromise = getStream(page, streamOptions);

    // Release the slot as soon as getStream settles successfully, so the next caller's init can overlap this stream's remaining setup. On failure the catch block
    // releases instead; the rejection handler is a no-op to suppress the unhandled-rejection warning, since the actual error handling happens in the catch below.
    void streamPromise.then(() => { slot.release(); }, () => { /* Suppress unhandled rejection; actual error handling is in the catch block below. */ });

    const stream = await raceWithTimeout(streamPromise, CONFIG.streaming.navigationTimeout, new Error("Stream initialization timed out."));

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
      pipeline(stream as unknown as Readable, ffmpeg.stdin).catch((error: unknown) => {

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

    const errorMessage = formatError(error);

    // Stale capture state is unrecoverable. The "Cannot capture a tab with an active stream" error occurs inside puppeteer-stream's second lock section, which
    // has no try/finally. The internal mutex is permanently leaked - all subsequent getStream() calls will hang on it. Chrome restart cannot fix module-level
    // state, so the only recourse is a full process restart. Release the capture queue so other callers aren't left hanging, then exit. Resource teardown (page,
    // interceptor) is handled by the DisposableStack as this throw unwinds the function scope.
    if(errorMessage.includes("Cannot capture a tab with an active stream")) {

      LOG.error("Stale capture state detected. puppeteer-stream's internal capture mutex is now permanently locked. The capture system is unrecoverable. " +
        "Exiting so the service manager can restart with a clean module state.");

      slot.release();

      // Defer the exit briefly so the error log above has time to flush to disk before the process dies; an immediate process.exit() can truncate the buffered
      // file write and lose the diagnostic that explains why the service restarted.
      setTimeout(() => process.exit(1), 100);

      throw error;
    }

    // For non-stale errors, release our queue slot so subsequent callers can proceed. Resource teardown is handled by the DisposableStack on unwind.
    slot.release();

    throw error;
  }

  // Navigate and set up playback. For static capture profiles, just navigate without video setup.
  let context: Frame | Page;
  let strategyDirectTune = false;
  let usedDirectUrl = false;

  try {

    if(!profile.staticCapture) {

      // Check for a direct watch URL. If available, navigate directly to it and skip channel selection, avoiding guide page navigation entirely. On failure,
      // the cache entry is invalidated in the catch block so the outer retry loop (in streaming/hls.ts) re-invokes with the guide URL.
      const directUrl = await resolveDirectUrl(profile, page);

      usedDirectUrl = !!directUrl;

      const navigationUrl = directUrl ?? url;

      // Phase 1: Navigate to the page with retry. The 10-second navigationTimeout is appropriate for page loads, and retryOperation correctly reloads the page on
      // genuine navigation failures. Navigation is wrapped in retryOperation separately from channel selection so the timeout does not race with the internal click
      // retry loops in channel selection strategies (guideGrid can take 15-20 seconds for binary search + click retries).
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

      // Phase 2: Channel selection + video setup. When navigating to a cached direct URL, skip channel selection since the URL already targets the correct
      // channel. Runs after navigation succeeds with no outer timeout racing against internal click retries. Each sub-step (selectChannel, waitForVideoReady,
      // etc.) has its own internal timeout via videoTimeout and click retry constants. For guideGrid strategies, a channel selection failure triggers an overlay
      // dismiss and retry, which doubles the channel selection time budget. The 45-second safety-net timeout accommodates this retry while still preventing
      // pathological hangs if multiple internal timeouts chain sequentially.
      const PLAYBACK_INIT_TIMEOUT = 45000;

      const tuneResult = await raceWithTimeout(
        initializePlayback(page, profile, { persistResolution: options.persistResolution, skipChannelSelection: usedDirectUrl }),
        PLAYBACK_INIT_TIMEOUT,
        new Error("Playback initialization timed out after " + String(PLAYBACK_INIT_TIMEOUT) + "ms.")
      );

      strategyDirectTune = tuneResult.directTune ?? false;
      context = tuneResult.context;
    } else {

      await page.goto(url);
      context = page;
    }
  } catch(error) {

    // If a cached direct URL was used, invalidate it so the next attempt falls through to guide navigation.
    if(usedDirectUrl) {

      invalidateDirectUrl(profile);
    }

    // Re-minimize the browser window. Navigation may have un-minimized it (new tab activation on macOS), and without this the window stays visible after the failed
    // attempt. Fire-and-forget since we're about to throw. Resource teardown (capture session, interceptor, page) is handled by the DisposableStack as this throw
    // unwinds the function scope; the capture session disposes first, destroying the capture stream before the page closes, so STOP_RECORDING ordering is preserved.
    minimizeBrowserWindow().catch(() => { /* Fire-and-forget; we're about to throw. */ });

    throw error;
  }

  // During tab replacement, allow Chrome's compositor to fully stabilize the fullscreen video surface before minimizing. Without this delay, the compositor may
  // snapshot an incorrect scaling state during the minimize transition, causing the captured content to appear zoomed into the top-left corner.
  if(options.tabReplacement && !profile.staticCapture) {

    await delay(500);
  }

  // Resize and minimize window.
  await resizeAndMinimizeWindow(page);

  LOG.debug("timing:startup", "Page with capture ready. Total: %sms.", captureElapsed());

  // Success: transfer ownership of the page, interceptor, and capture session out of the scope guard. move() empties the stack so its scope-exit disposal is a no-op
  // and the caller becomes responsible for disposing what it now holds.
  resources.move();

  return {

    captureSession,
    context,
    directTune: usedDirectUrl || strategyDirectTune || !isChannelSelectionProfile(profile),
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
 * Idempotent at the storage layer: writing the same selector twice is a no-op (the file store deduplicates identical deltas via normalization). The closure does
 * not pre-check whether the value differs - the underlying store handles that.
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
 * @returns Setup result with capture session, cleanup function, and metadata.
 * @throws StreamSetupError if setup fails with appropriate status code and message.
 */
export async function setupStream(options: StreamSetupOptions, onCircuitBreak: () => void): Promise<StreamSetupResult> {

  const { channel, channelName, channelSelector, clickSelector, clickToPlay, onTabReplacementFactory, profileOverride, staticCapture, url } = options;

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

      const validProfiles = getProfiles().map((p) => p.name);

      if(validProfiles.includes(profileOverride)) {

        profile = resolveProfile(profileOverride);
        profileName = profileOverride;

        LOG.debug("streaming:setup", "Profile overridden to '%s' via query parameter.", profileOverride);
      } else {

        LOG.warn("Unknown profile override '%s', using resolved profile.", profileOverride);
      }
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

      // Skip CDP manifest interception if the probe cache already knows this channel uses DRM. This avoids creating a CDP session that sits idle for 15 seconds
      // before the interceptor timeout cleans it up.
      const skipInterception = channelName ? (getCachedEncryption(channelName) === "drm") : false;

      // Build the persistResolution closure for the active channel. When the resolution layer in selectChannel() converts a category selector to a concrete call
      // sign, this closure writes the result to the user's channel store as a per-service-variant override - the same shape produced when a user manually edits
      // the selector via the web UI. Omitted for ad-hoc URL streams (no channel record to update).
      const serviceTag = getDomainConfig(url)?.serviceTag;
      const persistResolution = (channelName && serviceTag) ?
        buildPersistResolutionCallback(channelName, serviceTag) :
        undefined;

      captureResult = await createPageWithCapture({

        comment: metadataComment,
        onFFmpegError: onCircuitBreak,
        persistResolution,
        profile,
        skipManifestInterception: skipInterception,
        streamId,
        url
      });
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

      // createPageWithCapture handles its own cleanup on failure (closes page, kills FFmpeg).
      const errorMessage = formatError(error);
      const lowerMessage = errorMessage.toLowerCase();
      const benignPatterns = [ "abort", "session closed" ];
      const isBenign = benignPatterns.some((pattern) => lowerMessage.includes(pattern));

      if(!isBenign) {

        LOG.error("Stream setup failed for %s: %s.", url, errorMessage);
      }

      // Capture infrastructure errors should return 503 to signal Channels DVR to back off. These include Chrome capture state issues, queue timeouts, and stream
      // initialization failures. Using 503 with Retry-After prevents retry storms when there's a systemic issue. isCaptureInfrastructureError (recovery.ts) is the
      // single source of truth for this classification, shared with the browser supervisor's readiness detection.
      const isCaptureError = isCaptureInfrastructureError(errorMessage);

      // A capture-infrastructure failure may mean the browser, though still connected, can no longer capture. Hand it to the passive mid-life detector, which (guarded
      // and single-flight, in the background) re-verifies capture readiness and invalidates the browser for a governed relaunch if confirmed. Fire-and-forget: it must
      // never delay this response.
      if(isCaptureError) {

        noteCaptureInfrastructureFailure(numericStreamId);
      }

      // A failed tune on a service currently marked needs-sign-in most likely failed AT the auth wall, so the user-facing message leads with the remedy.
      throw new StreamSetupError("Stream error.", isCaptureError ? 503 : 500, withSignInGuidance("Failed to start stream.", channelName, serviceName), { cause: error });
    }

    const { captureSession, context, directTune, manifestInterception, page } = captureResult;

    // Hold the page, interceptor, and capture session on a scope guard so a tune-verification failure below disposes them structurally rather than repeating the
    // teardown inline. Push order is load-bearing: the capture session is registered last so it disposes first, destroying the capture stream and firing
    // STOP_RECORDING while the browser is still connected, ahead of the page close. On success we move() the guard and hand ownership to the cleanup closure (and,
    // once the session is installed on the registry entry, to terminateStream).
    using owned = new DisposableStack();

    owned.adopt(page, disposePage);

    if(manifestInterception) {

      owned.use(manifestInterception);
    }

    owned.use(captureSession);

    // Tune verification. Finalize the manifest interceptor and confirm the captured master manifest URL belongs to the channel that was just tuned. This step
    // makes setupStream "verified by construction" - every consumer of StreamSetupResult (HLS preroll, HLS blocking, MPEG-TS, native proxy, capture mode) receives
    // a stream guaranteed to be on the requested channel without having to opt in or coordinate.
    //
    // The verifier is a per-provider hook on ProviderModule.verifyManifestForChannel. Today only foxProvider implements it (Fox's CDN URL encodes the channel call
    // sign in the path). Verification is opportunistic: providers without a verifier and streams without a manifest interception (e.g., DRM-cached channels, tab
    // replacements) skip the check. When a verifier returns a failure reason, we throw StreamSetupError so the existing failure path marks channel health, terminates
    // the pending registry entry, and surfaces a clear error - never silently delivers the wrong channel. The scope guard disposes the capture session, interceptor,
    // and page as the throw unwinds.
    if(manifestInterception) {

      manifestInterception.finalize(directTune);

      const provider = getProviderByStrategy(profile.channelSelection.strategy);

      if(provider?.verifyManifestForChannel && profile.channelSelector) {

        const interception = await manifestInterception.promise;

        if(interception) {

          const verifyError = provider.verifyManifestForChannel(interception.masterManifestUrl, profile.channelSelector);

          if(verifyError) {

            await minimizeBrowserWindow();

            const failureLabel = channel?.name ?? channelName ?? url;

            throw new StreamSetupError("Tune verification failed: " + verifyError, 502,
              withSignInGuidance("Tune verification failed for " + failureLabel + ". " + verifyError, channelName, serviceName));
          }
        }
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

    // Cleanup function. Releases all resources associated with the stream. Idempotent - safe to call multiple times. completeStreamSetup uses it as the fallback
    // teardown when the pending entry is terminated before the capture session is installed on it; once installed, terminateStream disposes the same session
    // (disposal is idempotent). Disposes the capture session (kill -> destroy -> stop) before closing the page so STOP_RECORDING fires while the browser is connected.
    let cleanupCompleted = false;

    const cleanup = async (): Promise<void> => {

      if(cleanupCompleted) {

        return;
      }

      cleanupCompleted = true;

      monitor.dispose();
      captureSession.dispose();
      disposePage(page);

      // Re-minimize the browser window.
      await minimizeBrowserWindow();
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
      profile,
      profileName,
      serviceName,
      startTime,
      streamId,
      url
    };
  });
}

// Capture Readiness Verification.

/**
 * Verifies that a freshly-launched Chrome instance can actually capture, by running a real getStream against a throwaway page on it. This is the capability tier of
 * the browser launch gate: browser/index.ts injects it via setCaptureProbe and runs it inside launchReadyBrowser, so a browser is published as ready only after its
 * capture capability is verified - at boot AND at every relaunch, not just startup. It exercises the exact getStream path that hangs when the puppeteer-stream
 * extension is unregistered, so it would have detected the original outage's dead extension immediately.
 *
 * Each attempt creates a temporary page, attempts a short capture, and tears down both cleanly. A 500ms delay after destroying the capture stream allows
 * puppeteer-stream's fire-and-forget STOP_RECORDING chain to complete before closing the page, preventing a stale capture cascade on the first real request.
 *
 * After a system reboot or a fresh relaunch, Chrome's display stack or capture extension may not be ready immediately. The probe retries up to PROBE_MAX_ATTEMPTS
 * times with a delay between attempts, giving the system time to settle before giving up. At boot this prevents a rapid restart storm where the service manager
 * relaunches PrismCast repeatedly, each attempt orphaning a Chrome process; at relaunch it provides in-launch settling before the supervisor counts a launch failure.
 *
 * If stale capture state is detected, the process exits immediately - a Chrome restart cannot fix the leaked module-level mutex, only a fresh process can.
 * @param browser - The Chrome instance to verify (the local instance being launched, passed in rather than re-acquired to avoid re-entering the launch in flight).
 */
export async function verifyCaptureSystem(browser: Browser): Promise<void> {

  const PROBE_MAX_ATTEMPTS = 3;
  const PROBE_RETRY_DELAY = 5000;

  for(let attempt = 1; attempt <= PROBE_MAX_ATTEMPTS; attempt++) {

    // eslint-disable-next-line no-await-in-loop -- Sequential retries are intentional; each probe must complete before deciding whether to retry.
    const result = await attemptCaptureProbe(browser, CAPTURE_PROBE_TIMEOUT_MS);

    // Probe succeeded.
    if(result === null) {

      return;
    }

    // Stale capture state is unrecoverable. The error occurs inside puppeteer-stream's second lock section, which has no try/finally - the internal mutex is
    // permanently leaked. All subsequent getStream() calls will hang on it. Chrome restart cannot fix module-level state, so exit and let the service manager
    // restart with a clean process.
    if(result.includes("Cannot capture a tab with an active stream")) {

      LOG.error("The capture probe detected stale capture state. puppeteer-stream's internal capture mutex is now permanently locked. Exiting so the service " +
        "manager can restart with a clean module state.");

      process.exit(1);
    }

    // If we have retries remaining, log a warning and wait before the next attempt.
    if(attempt < PROBE_MAX_ATTEMPTS) {

      LOG.warn("Capture probe attempt %d of %d failed: %s. Retrying in %ds.", attempt, PROBE_MAX_ATTEMPTS, result, PROBE_RETRY_DELAY / 1000);

      // eslint-disable-next-line no-await-in-loop -- Deliberate delay between sequential retry attempts.
      await delay(PROBE_RETRY_DELAY);
    } else {

      throw new Error("Capture system verification failed after " + String(PROBE_MAX_ATTEMPTS) + " attempts: " + result);
    }
  }
}

/**
 * Executes a single capture probe attempt. Creates a temporary page on the given browser, tries to start a capture stream, and tears everything down cleanly.
 * @param browser - The Chrome instance to probe.
 * @param timeout - Maximum time in milliseconds to wait for getStream() to respond.
 * @returns Null on success, or an error message string on failure.
 */
async function attemptCaptureProbe(browser: Browser, timeout: number): Promise<Nullable<string>> {

  const page = await browser.newPage();

  registerManagedPage(page);

  try {

    // Use the same capture MIME type and viewport (height/width) as the runtime. The stale state error occurs at the tabCapture API level before encoding matters,
    // so matching those runtime constraints ensures the probe exercises a representative getStream() call.
    const useFFmpeg = CONFIG.streaming.captureMode === "ffmpeg";
    const captureMimeType = useFFmpeg ? getCaptureMimeType() : NATIVE_FMP4_MIME_TYPE;

    const streamOptions = {

      audio: true,
      mimeType: captureMimeType,
      video: true,
      videoConstraints: {

        mandatory: {

          maxFrameRate: 30,
          maxHeight: getEffectiveViewport(CONFIG).height,
          maxWidth: getEffectiveViewport(CONFIG).width,
          minFrameRate: 30,
          minHeight: getEffectiveViewport(CONFIG).height,
          minWidth: getEffectiveViewport(CONFIG).width
        }
      }
    } as unknown as Parameters<typeof getStream>[1];

    const stream = await raceWithTimeout(getStream(page, streamOptions), timeout, new Error("Capture probe timed out."));

    // Capture succeeded - the system is functional. Destroy the stream before closing the page to ensure chrome.tabCapture releases the capture cleanly.
    const readable = stream as unknown as Readable;

    readable.destroy();

    // Wait for puppeteer-stream's capture cleanup chain to complete. readable.destroy() triggers STOP_RECORDING via the close handler, but the call is
    // fire-and-forget. The async chain (STOP_RECORDING -> recorder.stop() -> onstop -> track.stop()) must finish before closing the page, or Chrome's tabCapture
    // state may linger and cause "Cannot capture a tab with an active stream" errors on the first real stream request.
    await delay(500);

    unregisterManagedPage(page);

    if(!page.isClosed()) {

      await page.close();
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

/* A browser can be capture-ready at launch and lose its capture capability later - the extension wedges, tabCapture stalls - without ever firing a "disconnected"
 * event, so neither the launch gate nor the disconnect handler would catch it. This detector rides a signal that is already happening: a stream-setup failure
 * carrying a capture-infrastructure signature. It is deliberately conservative. The guard (the failing stream is the only active stream) preserves per-stream
 * isolation - if any other stream is active, the browser is either demonstrably capturing or those streams will trip their own circuit breakers and drain. The probe
 * is the authoritative arbiter, serialized through the capture queue so it can never race a real stream's getStream init, and it runs in the background, single-flight,
 * so it never delays a response or stacks up. On a confirmed failure, the one recovery action runs: invalidate the browser for a governed relaunch.
 */

// At most one mid-life re-verification runs at a time across the process, so a burst of capture-infrastructure failures triggers a single probe, not a storm.
let captureReverificationInProgress = false;

/**
 * Runs one capture probe serialized through the capture queue, so it cannot race a concurrent getStream initialization (which would draw a spurious "Cannot capture
 * a tab with an active stream"). It holds the queue slot across the probe's full teardown - getStream plus the STOP_RECORDING settle inside attemptCaptureProbe - so
 * the next initialization does not start until the probe's capture is fully released.
 * @param browser - The Chrome instance to probe.
 * @param timeout - Maximum time in milliseconds to wait for the probe's getStream() to respond.
 * @returns Null when the browser captured successfully, or an error message when it could not (including a wedged-queue timeout).
 */
async function probeCaptureSerialized(browser: Browser, timeout: number): Promise<Nullable<string>> {

  const slot = acquireCaptureQueueSlot();

  try {

    await slot.ready;

    return await attemptCaptureProbe(browser, timeout);
  } catch(error) {

    // The only throw here is a wedged-predecessor queue-wait timeout; attemptCaptureProbe returns its own failures as strings. A jammed capture queue is itself
    // evidence the browser cannot capture, so surface it as a probe failure.
    return formatError(error);
  } finally {

    slot.release();
  }
}

/**
 * Passive mid-life capture-death detection, called from the stream-setup failure path when the failure carries a capture-infrastructure signature. If the failing
 * stream is the only active stream, it re-verifies the browser's capture capability with a queue-serialized probe in the background and, on confirmed failure,
 * invalidates the browser for a governed relaunch - catching a browser that is still connected (no "disconnected" event) but can no longer capture. Fire-and-forget
 * so the failing request's response is not delayed; single-flight so a burst of failures triggers at most one probe.
 * @param failingStreamId - The numeric id of the stream whose setup just failed, excluded from the active-stream guard.
 */
function noteCaptureInfrastructureFailure(failingStreamId: number): void {

  // Single-flight: a re-verification is already deciding the browser's fate; do not stack another.
  if(captureReverificationInProgress) {

    return;
  }

  // Isolation guard: only re-verify when the failing stream is the only active stream. Any other active stream means either the browser is demonstrably capturing
  // (so this failure is stream-specific - never invalidate) or those streams will trip their own circuit breakers and drain, after which a later failure reaches
  // this zero-other case. We never tear down a browser other streams are using.
  if(getAllStreams().some((entry) => entry.id !== failingStreamId)) {

    return;
  }

  // A readiness probe needs a connected browser to exercise. If none is published, a disconnect already handled the readiness loss.
  const browser = getBrowserInstance();

  if(!browser) {

    return;
  }

  captureReverificationInProgress = true;

  // Run in the background so the failing request's 503 response is not delayed by the probe, which can take up to the probe timeout.
  void (async (): Promise<void> => {

    try {

      const failure = await probeCaptureSerialized(browser, CAPTURE_PROBE_TIMEOUT_MS);

      if(failure !== null) {

        // The probe confirmed the browser cannot capture though it is still connected. invalidateBrowser is the single recovery action - relinquish readiness,
        // terminate the now-doomed streams, and close Chrome - so the next request relaunches a fresh, gate-verified browser. We pass the exact instance we probed:
        // invalidateBrowser no-ops if it was already superseded by a disconnect-and-relaunch during the probe, so we never tear down a healthy replacement. A
        // genuinely leaked module mutex, if that was the cause, surfaces again at the relaunch's gate probe and exits there; a merely-slow getStream that finally
        // settles instead lets the relaunch recover.
        await invalidateBrowser(browser, "a capture probe failed after a stream setup failure with no other active streams");
      }
    } catch(error) {

      LOG.debug("streaming:setup", "Mid-life capture re-verification aborted: %s.", formatError(error));
    } finally {

      captureReverificationInProgress = false;
    }
  })();
}
