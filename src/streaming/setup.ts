/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.ts: Common stream setup logic for PrismCast.
 */
import type { Frame, Page } from "puppeteer-core";
import { LOG, delay, extractDomain, formatError, raceWithTimeout, registerAbortController, resolveFFmpegPath, retryOperation, runWithStreamContext,
  spawnFFmpeg, startTimer } from "../utils/index.ts";
import type { Nullable, ResolvedChannel, ResolvedSiteProfile, UrlValidationResult } from "../types/index.ts";
import type { RecoveryMetrics, TabReplacementResult } from "./recovery.ts";
import { getCurrentBrowser, getStream, minimizeBrowserWindow, registerManagedPage, unregisterManagedPage } from "../browser/index.ts";
import { getNextStreamId, getStreamCount } from "./registry.ts";
import { getProfileForChannel, getProfileForUrl, getProfiles, resolveProfile } from "../config/profiles.ts";
import { getProviderByStrategy, invalidateDirectUrl, resolveDirectUrl } from "../browser/channelSelection.ts";
import { initializePlayback, injectVideoSelector, navigateToPage } from "../browser/video.ts";
import { CONFIG } from "../config/index.ts";
import type { FFmpegProcess } from "../utils/index.ts";
import type { ManifestInterceptorHandle } from "../browser/manifestInterceptor.ts";
import type { MonitorStreamInfo } from "./monitor.ts";
import type { Readable } from "node:stream";
import { chromeFetch } from "../utils/index.ts";
import { getCachedEncryption } from "../native/probe.ts";
import { getCaptureMimeType } from "./codec.ts";
import { getDomainConfig } from "../config/sites.ts";
import { getEffectiveViewport } from "../config/presets.ts";
import { getServiceDisplayName } from "../config/services.ts";
import { installManifestInterceptor } from "../browser/manifestInterceptor.ts";
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
 * 2. setupStream(): Orchestrates stream creation by calling createPageWithCapture(), then registering the stream and starting the health monitor. This is the
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
 * - Request validation (URL format, concurrent stream limit)
 * - Stream registration
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

// Maximum number of times createPageWithCapture() will retry when it detects that the page was closed while waiting in the capture queue (e.g., due to a browser
// crash). An explicit guard prevents unbounded recursion.
const MAX_PAGE_CLOSED_RETRIES = 3;

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

  // The puppeteer-stream capture output. For native fMP4 mode, this is the raw MP4/AAC stream. For Matroska+FFmpeg mode, this is FFmpeg's fMP4 output.
  captureStream: Readable;

  // The channel display name if streaming a named channel.
  channelName: Nullable<string>;

  // Whether the channel was tuned via a direct mechanism (cached URL or API interception) rather than DOM interaction.
  directTune: boolean;

  // Cleanup function to release all resources. Safe to call multiple times.
  cleanup: () => Promise<void>;

  // The FFmpeg process for Matroska-to-fMP4 transcoding, or null if using native fMP4 mode.
  ffmpegProcess: Nullable<FFmpegProcess>;

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

  // The raw capture stream from puppeteer-stream. Must be destroyed before closing the page.
  rawCaptureStream: Readable;

  // Timestamp when the stream started.
  startTime: Date;

  // Function to stop the health monitor. Returns recovery metrics for the termination summary.
  stopMonitor: () => RecoveryMetrics;

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

  // The output stream for the segmenter. For native fMP4 mode, this is the raw capture. For Matroska+FFmpeg mode, this is FFmpeg's fMP4 output.
  captureStream: Readable;

  // The video context (page or frame containing the video element).
  context: Frame | Page;

  // Whether the channel was tuned via a direct mechanism (cached URL or API interception) rather than DOM interaction.
  directTune: boolean;

  // The FFmpeg process if using Matroska+FFmpeg mode, null otherwise.
  ffmpegProcess: Nullable<FFmpegProcess>;

  // Manifest interceptor handle for native streaming, or null if interception was not installed.
  manifestInterception: Nullable<ManifestInterceptorHandle>;

  // The browser page for this capture.
  page: Page;

  // The raw capture stream from puppeteer-stream (before FFmpeg processing). Must be destroyed before closing the page to ensure chrome.tabCapture releases the
  // capture. In native mode, this is the same object as captureStream.
  rawCaptureStream: Readable;
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
 * Creates a browser page with media capture and navigates to the URL. This is the reusable core function used by both initial stream setup and tab replacement
 * recovery. It handles:
 * - Creating a new browser page with CSP bypass
 * - Initializing media capture (native fMP4 or Matroska+FFmpeg)
 * - Navigating to the URL with retry
 * - Setting up video playback via navigateToPage() + initializePlayback()
 *
 * The caller is responsible for:
 * - Creating the segmenter and piping captureStream to it
 * - Registering/updating the stream in the registry
 * - Starting/updating the health monitor
 * - Handling cleanup on failure
 *
 * @param options - Options for page and capture creation.
 * @returns The page, context, capture stream, and FFmpeg process (if any).
 * @throws Error if page creation, capture initialization, or navigation fails.
 */
export async function createPageWithCapture(options: CreatePageWithCaptureOptions): Promise<CreatePageWithCaptureResult> {

  const captureElapsed = startTimer();
  const { comment, onFFmpegError, profile, streamId, url } = options;

  // Create browser page.
  const browser = await getCurrentBrowser();
  const page = await browser.newPage();

  registerManagedPage(page);

  await page.setBypassCSP(true);

  // Inject the shared video selector helper into the browser context. This must happen before navigation so the helper is available when evaluate calls run during
  // initializePlayback (startVideoPlayback, applyVideoStyles, verifyFullscreen, lockVolumeProperties) and subsequent health monitoring (getVideoState).
  await injectVideoSelector(page);

  // Install CDP manifest interceptor before navigation. This listener captures .m3u8 URLs from the browser's network requests, enabling native HLS streaming for
  // services that use clear or AES-128 encrypted streams. Skipped for tab replacements (native proxy is independent of capture) and for channels already known to
  // use DRM (avoids 15 seconds of wasted CDP overhead per tune). The await ensures the CDP session and Network domain are ready before navigation begins.
  const manifestInterception = (!options.tabReplacement && !options.skipManifestInterception) ? await installManifestInterceptor(page) : null;

  // Select MIME type based on capture mode. FFmpeg mode is more stable for long recordings because Chrome's native fMP4 MediaRecorder can become unstable. The
  // codec decision (H.264 vs HEVC) is delegated to the codec module, which considers the user's allowlist and GPU hardware capabilities.
  const useFFmpeg = CONFIG.streaming.captureMode === "ffmpeg";
  const captureMimeType = useFFmpeg ? getCaptureMimeType() : NATIVE_FMP4_MIME_TYPE;

  // Track the output stream that will be sent to the segmenter and FFmpeg process if used. Also track the raw capture stream separately - it must be destroyed
  // before closing the page to ensure chrome.tabCapture releases the capture.
  let outputStream: Readable;
  let rawCaptureStream: Nullable<Readable> = null;
  let ffmpegProcess: Nullable<FFmpegProcess> = null;

  // Capture queue release function, hoisted here so both the try and catch blocks can access it. The promise and resolver are created together via
  // withResolvers when the queue entry is registered below. The once-guard prevents double-releasing from multiple code paths (success, catch, timeout).
  let captureQueueReleased = false;
  let releaseCaptureQueue: () => void;

  const releaseCaptureOnce = (): void => {

    if(!captureQueueReleased) {

      captureQueueReleased = true;
      captureQueueDepth--;
      releaseCaptureQueue();
    }
  };

  // Initialize media stream capture.
  try {

    const streamOptions = {

      audio: true,
      audioBitsPerSecond: CONFIG.streaming.audioBitsPerSecond,
      mimeType: captureMimeType,
      video: true,
      videoBitsPerSecond: CONFIG.streaming.videoBitsPerSecond,
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

    // Serialize capture initialization. Wait for any previous capture to finish before calling getStream(), because Chrome's tabCapture extension rejects
    // concurrent initialization attempts. On success, the lock is released immediately so the next caller can proceed. On failure, the lock is held until the
    // catch block decides what to do - the catch block releases the lock after handling the error.
    const previousCapture = captureQueue;

    captureQueueDepth++;

    if(captureQueueDepth >= CAPTURE_QUEUE_DEPTH_WARNING) {

      LOG.warn("Capture queue depth is %d. Multiple stream requests are competing for Chrome's capture initialization.", captureQueueDepth);
    }

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const queued = Promise.withResolvers<void>();

    captureQueue = queued.promise;
    releaseCaptureQueue = queued.resolve;

    // Guard against a permanently hung predecessor. If the previous capture doesn't complete within the navigation timeout, release our queue position and let the
    // caller's error handling deal with it. This prevents a single stuck getStream() from blocking all future captures indefinitely.
    try {

      await raceWithTimeout(previousCapture, CONFIG.streaming.navigationTimeout, new Error("Capture queue wait timed out."));
    } catch(error) {

      // Release our queue position so subsequent captures aren't blocked by our failure.
      releaseCaptureOnce();

      throw error;
    }

    // After the queue wait, verify our page is still connected. If Chrome crashed while we were waiting, our page is dead and we need to start over with a
    // fresh page on the new browser. Release our queue position first so subsequent callers aren't blocked.
    if(page.isClosed()) {

      releaseCaptureOnce();
      unregisterManagedPage(page);

      const retryCount = options._pageClosedRetries ?? 0;

      if(retryCount >= MAX_PAGE_CLOSED_RETRIES) {

        throw new Error("Browser crashed too many times during capture initialization.");
      }

      return await createPageWithCapture({ ...options, _pageClosedRetries: retryCount + 1 });
    }

    const streamPromise = getStream(page, streamOptions);

    // Release the queue on success only. On failure, the catch block handles the release. The rejection handler is a no-op to suppress unhandled rejection
    // warnings; the actual error handling happens in the catch block below.
    void streamPromise.then(() => { releaseCaptureOnce(); }, () => { /* Suppress unhandled rejection; actual error handling is in the catch block below. */ });

    const stream = await raceWithTimeout(streamPromise, CONFIG.streaming.navigationTimeout, new Error("Stream initialization timed out."));

    // Store the raw capture stream. This must be destroyed before closing the page.
    rawCaptureStream = stream;

    // For FFmpeg mode, spawn FFmpeg to transcode the Matroska stream to fMP4. FFmpeg copies the H264 video and transcodes Opus audio to AAC. The resolved binary
    // path comes from the production-cached resolver - first call probes; subsequent calls return the memoized result. Falls back to "ffmpeg" so spawn() defers
    // to PATH lookup if the resolver couldn't find a path (matching the previous behavior; the spawn will then fail with ENOENT if PATH is also empty).
    if(useFFmpeg) {

      const ffmpegBin = (await resolveFFmpegPath()) ?? "ffmpeg";

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

      // Use FFmpeg's stdout (fMP4 output) as the output stream for segmentation.
      outputStream = ffmpeg.stdout;
    } else {

      // Native fMP4 mode: Use the raw capture stream directly. In this mode, rawCaptureStream and outputStream are the same object.
      outputStream = rawCaptureStream;
    }
  } catch(error) {

    // Clean up on capture initialization failure. Destroy the raw capture stream first to ensure chrome.tabCapture releases the capture.
    if(rawCaptureStream && !rawCaptureStream.destroyed) {

      rawCaptureStream.destroy();
    }

    unregisterManagedPage(page);

    if(!page.isClosed()) {

      page.close().catch(() => { /* Fire-and-forget during error cleanup. */ });
    }

    const errorMessage = formatError(error);

    // Stale capture state is unrecoverable. The "Cannot capture a tab with an active stream" error occurs inside puppeteer-stream's second lock section, which
    // has no try/finally. The internal mutex is permanently leaked - all subsequent getStream() calls will hang on it. Chrome restart cannot fix module-level
    // state, so the only recourse is a full process restart. Release the capture queue so other callers aren't left hanging, then exit.
    if(errorMessage.includes("Cannot capture a tab with an active stream")) {

      LOG.error("Stale capture state detected. puppeteer-stream's internal capture mutex is now permanently locked. The capture system is unrecoverable. " +
        "Exiting so the service manager can restart with a clean module state.");

      releaseCaptureOnce();

      setTimeout(() => process.exit(1), 100);

      throw error;
    }

    // For non-stale errors, release the capture queue so subsequent callers can proceed.
    releaseCaptureOnce();

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

    // Clean up on navigation or playback initialization failure. Destroy the raw capture stream first to ensure chrome.tabCapture releases the capture.
    if(!rawCaptureStream.destroyed) {

      rawCaptureStream.destroy();
    }

    if(ffmpegProcess) {

      ffmpegProcess.kill();
    }

    unregisterManagedPage(page);

    if(!page.isClosed()) {

      page.close().catch(() => { /* Fire-and-forget during error cleanup. */ });
    }

    // Re-minimize the browser window. Navigation may have un-minimized it (new tab activation on macOS), and without this the window stays visible after the
    // failed attempt. Fire-and-forget since we're about to throw.
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

  return {

    captureStream: outputStream,
    context,
    directTune: usedDirectUrl || strategyDirectTune || !isChannelSelectionProfile(profile),
    ffmpegProcess,
    manifestInterception,
    page,
    rawCaptureStream
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
 * - Connecting the returned captureStream to the appropriate output (HTTP response, FFmpeg, etc.)
 * - Registering the stream in the registry
 * - Triggering cleanup when the stream ends
 *
 * @param options - Stream configuration options.
 * @param onCircuitBreak - Callback invoked when the circuit breaker trips (stream unrecoverable).
 * @returns Setup result with capture stream, cleanup function, and metadata.
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

    // Check concurrent stream limit.
    if(getStreamCount() >= CONFIG.streaming.maxConcurrentStreams) {

      LOG.warn("Concurrent stream limit reached (%s/%s). Rejecting request.", getStreamCount(), CONFIG.streaming.maxConcurrentStreams);

      throw new StreamSetupError(
        "Concurrent stream limit reached.",
        503,
        "Maximum concurrent streams (" + String(CONFIG.streaming.maxConcurrentStreams) + ") reached. Try again later."
      );
    }

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

      // createPageWithCapture handles its own cleanup on failure (closes page, kills FFmpeg).
      const errorMessage = formatError(error);
      const lowerMessage = errorMessage.toLowerCase();
      const benignPatterns = [ "abort", "session closed" ];
      const isBenign = benignPatterns.some((pattern) => lowerMessage.includes(pattern));

      if(!isBenign) {

        LOG.error("Stream setup failed for %s: %s.", url, errorMessage);
      }

      // Capture infrastructure errors should return 503 to signal Channels DVR to back off. These include Chrome capture state issues, queue timeouts, and stream
      // initialization failures. Using 503 with Retry-After prevents retry storms when there's a systemic issue.
      const captureErrorPatterns = [ "Cannot capture", "timed out", "Capture queue" ];
      const isCaptureError = captureErrorPatterns.some((pattern) => errorMessage.includes(pattern));

      throw new StreamSetupError("Stream error.", isCaptureError ? 503 : 500, "Failed to start stream.", { cause: error });
    }

    const { captureStream, context, directTune, ffmpegProcess, manifestInterception, page, rawCaptureStream } = captureResult;

    // Tune verification. Finalize the manifest interceptor and confirm the captured master manifest URL belongs to the channel that was just tuned. This step
    // makes setupStream "verified by construction" - every consumer of StreamSetupResult (HLS preroll, HLS blocking, MPEG-TS, native proxy, capture mode) receives
    // a stream guaranteed to be on the requested channel without having to opt in or coordinate.
    //
    // The verifier is a per-provider hook on ProviderModule.verifyManifestForChannel. Today only foxProvider implements it (Fox's CDN URL encodes the channel call
    // sign in the path). Verification is opportunistic: providers without a verifier and streams without a manifest interception (e.g., DRM-cached channels, tab
    // replacements) skip the check. When a verifier returns a failure reason, we tear down the capture and throw StreamSetupError so the existing failure path
    // marks channel health, terminates the pending registry entry, and surfaces a clear error - never silently delivers the wrong channel.
    if(manifestInterception) {

      manifestInterception.finalize(directTune);

      const provider = getProviderByStrategy(profile.channelSelection.strategy);

      if(provider?.verifyManifestForChannel && profile.channelSelector) {

        const interception = await manifestInterception.promise;

        if(interception) {

          const verifyError = provider.verifyManifestForChannel(interception.masterManifestUrl, profile.channelSelector);

          if(verifyError) {

            // Verification failed. Tear down the capture before throwing so we do not leak the page, FFmpeg process, or browser-side capture session. We replicate
            // the relevant cleanup steps inline because the cleanup() closure has not been constructed yet at this point in the function.
            if(!rawCaptureStream.destroyed) {

              rawCaptureStream.destroy();
            }

            if(ffmpegProcess) {

              ffmpegProcess.kill();
            }

            unregisterManagedPage(page);

            if(!page.isClosed()) {

              page.close().catch((closeError: unknown) => {

                LOG.debug("streaming:setup", "Page close error during verification failure cleanup: %s.", formatError(closeError));
              });
            }

            await minimizeBrowserWindow();

            const failureLabel = channel?.name ?? channelName ?? url;

            throw new StreamSetupError("Tune verification failed: " + verifyError, 502, "Tune verification failed for " + failureLabel + ". " + verifyError);
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
    const stopMonitor = monitorPlaybackHealth(page, context, profile, url, streamId, monitorStreamInfo, onCircuitBreak, onTabReplacement);

    // Cleanup function. Releases all resources associated with the stream. Idempotent - safe to call multiple times.
    let cleanupCompleted = false;

    const cleanup = async (): Promise<void> => {

      if(cleanupCompleted) {

        return;
      }

      cleanupCompleted = true;

      // Stop the health monitor first.
      stopMonitor();

      // Destroy the raw capture stream BEFORE closing the page. This triggers puppeteer-stream's close handler while the browser is still connected, ensuring
      // STOP_RECORDING is called and chrome.tabCapture releases the capture. Without this, subsequent getStream() calls may hang with "active stream" errors.
      if(!rawCaptureStream.destroyed) {

        rawCaptureStream.destroy();
      }

      // Kill the FFmpeg process if using Matroska+FFmpeg mode.
      if(ffmpegProcess) {

        ffmpegProcess.kill();
      }

      // Unregister from managed pages.
      unregisterManagedPage(page);

      // Close the browser page (fire-and-forget to avoid blocking on stuck pages).
      if(!page.isClosed()) {

        page.close().catch((error: unknown) => {

          LOG.debug("streaming:setup", "Page close error during cleanup: %s.", formatError(error));
        });
      }

      // Re-minimize the browser window.
      await minimizeBrowserWindow();
    };

    // Return the setup result.
    return {

      captureStream,
      channelName: channel?.name ?? null,
      cleanup,
      directTune,
      ffmpegProcess,
      manifestInterception,
      numericStreamId,
      page,
      profile,
      profileName,
      rawCaptureStream,
      serviceName,
      startTime,
      stopMonitor,
      streamId,
      url
    };
  });
}

// Startup Capture Verification.

/**
 * Verifies that Chrome's capture system is functional before the server starts accepting requests. This detects stale tabCapture state left over from a previous
 * Chrome process - common during quick service restarts where the old process hasn't fully exited before the new one launches. Without this probe, the first stream
 * request would trigger the runtime stale capture handler, which exits the process because the puppeteer-stream mutex is permanently leaked.
 *
 * The probe creates a temporary page, attempts a short capture, and tears down both cleanly. A 500ms delay after destroying the capture stream allows
 * puppeteer-stream's fire-and-forget STOP_RECORDING chain to complete before closing the page, preventing the stale capture cascade on the first real request.
 *
 * After a system reboot, Chrome's display stack or capture extension may not be ready when the service manager starts PrismCast. The probe retries up to
 * PROBE_MAX_ATTEMPTS times with a delay between attempts, giving the system time to settle before giving up. This prevents a rapid restart storm where the service
 * manager relaunches PrismCast repeatedly, each attempt orphaning a Chrome process and degrading the environment further.
 *
 * If stale capture state is detected, the process exits immediately - Chrome restart cannot fix the leaked mutex, only a fresh process can.
 */
export async function verifyCaptureSystem(): Promise<void> {

  const PROBE_MAX_ATTEMPTS = 3;
  const PROBE_RETRY_DELAY = 5000;
  const PROBE_TIMEOUT = 5000;

  for(let attempt = 1; attempt <= PROBE_MAX_ATTEMPTS; attempt++) {

    // eslint-disable-next-line no-await-in-loop -- Sequential retries are intentional; each probe must complete before deciding whether to retry.
    const result = await attemptCaptureProbe(PROBE_TIMEOUT);

    // Probe succeeded.
    if(result === null) {

      return;
    }

    // Stale capture state is unrecoverable. The error occurs inside puppeteer-stream's second lock section, which has no try/finally - the internal mutex is
    // permanently leaked. All subsequent getStream() calls will hang on it. Chrome restart cannot fix module-level state, so exit and let the service manager
    // restart with a clean process.
    if(result.includes("Cannot capture a tab with an active stream")) {

      LOG.error("Startup probe detected stale capture state. puppeteer-stream's internal capture mutex is now permanently locked. Exiting so the service " +
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
 * Executes a single capture probe attempt. Creates a temporary page, tries to start a capture stream, and tears everything down cleanly.
 * @param timeout - Maximum time in milliseconds to wait for getStream() to respond.
 * @returns Null on success, or an error message string on failure.
 */
async function attemptCaptureProbe(timeout: number): Promise<Nullable<string>> {

  const browser = await getCurrentBrowser();
  const page = await browser.newPage();

  registerManagedPage(page);

  try {

    // Use the same capture MIME type and viewport constraints as the runtime. The stale state error occurs at the tabCapture API level before encoding matters,
    // but matching the runtime configuration ensures the probe exercises the exact same getStream() parameters.
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
