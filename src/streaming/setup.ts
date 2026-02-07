/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.ts: Common stream setup logic for PrismCast.
 */
import type { Channel, Nullable, ResolvedSiteProfile, UrlValidation } from "../types/index.js";
import type { Frame, Page } from "puppeteer-core";
import { LOG, delay, formatError, registerAbortController, retryOperation, runWithStreamContext, spawnFFmpeg } from "../utils/index.js";
import type { MonitorStreamInfo, RecoveryMetrics, TabReplacementResult } from "./monitor.js";
import { closeBrowser, getCurrentBrowser, getStream, minimizeBrowserWindow, registerManagedPage, unregisterManagedPage } from "../browser/index.js";
import { getNextStreamId, getStreamCount } from "./registry.js";
import { getProfileForChannel, getProfileForUrl, getProfiles, resolveProfile } from "../config/profiles.js";
import { CONFIG } from "../config/index.js";
import type { FFmpegProcess } from "../utils/index.js";
import type { Readable } from "node:stream";
import { getEffectiveViewport } from "../config/presets.js";
import { monitorPlaybackHealth } from "./monitor.js";
import { pipeline } from "node:stream/promises";
import { resizeAndMinimizeWindow } from "../browser/cdp.js";
import { initializePlayback, navigateToPage } from "../browser/video.js";

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
 * - Media stream initialization (native fMP4 or WebM+FFmpeg)
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

// WebM+FFmpeg capture uses WebM/H264+Opus, which requires FFmpeg to transcode audio to AAC. This mode is more stable for long recordings because Chrome's native fMP4
// MediaRecorder can become unstable after 20-30 minutes.
const WEBM_FFMPEG_MIME_TYPE = "video/webm;codecs=h264,opus";

// Capture initialization queue. Chrome's tabCapture extension can only initialize one capture at a time — concurrent getStream() calls fail with "Cannot capture a
// tab with an active stream." We serialize capture initialization using a promise chain so requests execute sequentially. Once a capture is established, it runs
// concurrently with other captures without issue.
let captureQueue: Promise<void> = Promise.resolve();

// Stale capture recovery. Chrome's tabCapture can retain stale state after a crash, causing "Cannot capture a tab with an active stream" errors on fresh launches.
// When detected, we restart Chrome entirely to clear the state. Limited to 3 attempts per server lifetime to prevent infinite loops.
const MAX_STALE_CAPTURE_RECOVERY_ATTEMPTS = 3;
let staleCaptureRecoveryAttempts = 0;

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
) => () => Promise<TabReplacementResult | null>;

/**
 * Options for setting up a stream.
 */
export interface StreamSetupOptions {

  // The channel definition if streaming a named channel.
  channel?: Channel;

  // The channel name (key) if streaming a named channel.
  channelName?: string;

  // Channel selector for multi-channel sites. Only used for ad-hoc streams (no channel definition). For predefined channels, the selector comes from
  // channel.channelSelector via getProfileForChannel.
  channelSelector?: string;

  // Click selector for play button overlays. Only used for ad-hoc streams. For predefined channels, the selector comes from the profile definition.
  clickSelector?: string;

  // Whether to click an element to start playback. Only used for ad-hoc streams. For predefined channels, this comes from the profile definition.
  clickToPlay?: boolean;

  // Whether to treat this as a static page without video.
  noVideo?: boolean;

  // Factory function to create a tab replacement handler. Called after stream IDs are generated so the handler has access to them. If not provided, tab replacement
  // recovery is disabled.
  onTabReplacementFactory?: TabReplacementHandlerFactory;

  // Override the autodetected profile with a specific profile name.
  profileOverride?: string;

  // The URL to stream. Required.
  url: string;
}

/**
 * Result from setting up a stream.
 */
export interface StreamSetupResult {

  // The puppeteer-stream capture output. For native fMP4 mode, this is the raw MP4/AAC stream. For WebM+FFmpeg mode, this is FFmpeg's fMP4 output.
  captureStream: Readable;

  // The channel display name if streaming a named channel.
  channelName: Nullable<string>;

  // Cleanup function to release all resources. Safe to call multiple times.
  cleanup: () => Promise<void>;

  // The FFmpeg process for WebM-to-fMP4 transcoding, or null if using native fMP4 mode.
  ffmpegProcess: Nullable<FFmpegProcess>;

  // Unique numeric ID for this stream.
  numericStreamId: number;

  // The browser page for this stream.
  page: Page;

  // The resolved site profile.
  profile: ResolvedSiteProfile;

  // The name of the resolved profile (e.g., "keyboardDynamic", "fullscreenApi", "default").
  profileName: string;

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

  constructor(message: string, statusCode: number, userMessage: string) {

    super(message);

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

  // The resolved site profile for video handling.
  profile: ResolvedSiteProfile;

  // The stream ID string for logging (e.g., "cnn-5jecl6").
  streamId: string;

  // The URL to navigate to and capture.
  url: string;
}

/**
 * Result from creating a page with capture. Contains everything needed to create a segmenter and continue with stream setup.
 */
export interface CreatePageWithCaptureResult {

  // The output stream for the segmenter. For native fMP4 mode, this is the raw capture. For WebM+FFmpeg mode, this is FFmpeg's fMP4 output.
  captureStream: Readable;

  // The video context (page or frame containing the video element).
  context: Frame | Page;

  // The FFmpeg process if using WebM+FFmpeg mode, null otherwise.
  ffmpegProcess: Nullable<FFmpegProcess>;

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

  let result = "";

  for(let i = 0; i < 6; i++) {

    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }

  return result;
}

/**
 * Extracts the domain from a URL, removing the www. prefix for cleaner display. Returns undefined if the URL cannot be parsed.
 * @param url - The URL to extract the domain from.
 * @returns The domain without www. prefix, or undefined if parsing fails.
 */
function extractDomain(url: string): string | undefined {

  try {

    return new URL(url).hostname.replace(/^www\./, "");
  } catch {

    return undefined;
  }
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

    return [ channelName, "-", requestId ].join("");
  }

  // For direct URL requests, extract the hostname for the prefix.
  if(url) {

    const domain = extractDomain(url);

    if(domain) {

      return [ domain, "-", requestId ].join("");
    }

    // If URL parsing fails, use a truncated version of the URL. This handles malformed URLs gracefully.
    const truncated = url.length > 20 ? [ url.substring(0, 20), "..." ].join("") : url;

    return [ truncated, "-", requestId ].join("");
  }

  // Fallback when neither channel name nor URL is available. This shouldn't happen in normal operation but provides a valid ID for edge cases.
  return [ "unknown-", requestId ].join("");
}

// URL Validation.

/**
 * Validates a URL before attempting to navigate to it. This function checks for supported protocols, prevents local file access, and ensures the URL is properly
 * formatted. Validating URLs before navigation prevents security issues and provides clear error messages.
 * @param url - The URL to validate.
 * @returns Validation result with optional reason for failure.
 */
export function validateStreamUrl(url: string | undefined): UrlValidation {

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

      return { reason: [ "Unsupported protocol: ", parsed.protocol ].join(""), valid: false };
    }

    // Local file access is explicitly blocked for security reasons. While the URL constructor wouldn't typically parse a file: URL through the protocol check
    // above, we check explicitly for defense in depth.
    if(parsed.protocol === "file:") {

      return { reason: "Local file access is not permitted.", valid: false };
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
 * - Initializing media capture (native fMP4 or WebM+FFmpeg)
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

  const { comment, onFFmpegError, profile, streamId, url } = options;

  // Create browser page.
  const browser = await getCurrentBrowser();
  const page = await browser.newPage();

  registerManagedPage(page);

  await page.setBypassCSP(true);

  // Select MIME type based on capture mode. FFmpeg mode is more stable for long recordings because Chrome's native fMP4 MediaRecorder can become unstable.
  const useFFmpeg = CONFIG.streaming.captureMode === "ffmpeg";
  const captureMimeType = useFFmpeg ? WEBM_FFMPEG_MIME_TYPE : NATIVE_FMP4_MIME_TYPE;

  // Track the output stream that will be sent to the segmenter and FFmpeg process if used. Also track the raw capture stream separately - it must be destroyed
  // before closing the page to ensure chrome.tabCapture releases the capture.
  let outputStream: Readable;
  let rawCaptureStream: Nullable<Readable> = null;
  let ffmpegProcess: Nullable<FFmpegProcess> = null;

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
          minFrameRate: CONFIG.streaming.frameRate,
          minHeight: getEffectiveViewport(CONFIG).height,
          minWidth: getEffectiveViewport(CONFIG).width
        }
      }
    } as unknown as Parameters<typeof getStream>[1];

    // Serialize capture initialization. Wait for any previous capture to finish before calling getStream(), because Chrome's tabCapture extension rejects
    // concurrent initialization attempts. The lock is released when getStream() resolves or rejects (not when our timeout fires), because Chrome's extension state
    // is what matters. If our timeout fires while getStream() is still in-flight, the catch block closes the page, which causes the pending getStream() to reject,
    // which releases the lock and allows the next queued capture to proceed.
    let releaseCaptureQueue: () => void = () => {};
    const previousCapture = captureQueue;

    captureQueue = new Promise<void>((resolve) => {

      releaseCaptureQueue = resolve;
    });

    // Guard against a permanently hung predecessor. If the previous capture doesn't complete within the navigation timeout, release our queue position and let the
    // caller's error handling deal with it. This prevents a single stuck getStream() from blocking all future captures indefinitely.
    try {

      await Promise.race([
        previousCapture,
        new Promise<never>((_, reject) => {

          setTimeout(() => {

            reject(new Error("Capture queue wait timed out."));
          }, CONFIG.streaming.navigationTimeout);
        })
      ]);
    } catch(error) {

      // Release our queue position so subsequent captures aren't blocked by our failure.
      releaseCaptureQueue();

      throw error;
    }

    const streamPromise = getStream(page, streamOptions);

    // Release the queue when getStream() completes, regardless of success or failure. This is independent of our application timeout below.
    void streamPromise.then(() => releaseCaptureQueue(), () => releaseCaptureQueue());

    const timeoutPromise = new Promise<never>((_, reject) => {

      setTimeout(() => {

        reject(new Error("Stream initialization timed out."));
      }, CONFIG.streaming.navigationTimeout);
    });

    const stream = await Promise.race([ streamPromise, timeoutPromise ]);

    // Store the raw capture stream. This must be destroyed before closing the page.
    rawCaptureStream = stream as unknown as Readable;

    // For FFmpeg mode, spawn FFmpeg to transcode the WebM stream to fMP4. FFmpeg copies the H264 video and transcodes Opus audio to AAC.
    if(useFFmpeg) {

      const ffmpeg = spawnFFmpeg(CONFIG.streaming.audioBitsPerSecond, (error) => {

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

          LOG.debug("FFmpeg stdout pipe closed: %s.", errorMessage);
        } else {

          LOG.error("FFmpeg stdout pipe error: %s.", errorMessage);
          ffmpeg.kill();

          if(onFFmpegError) {

            onFFmpegError(error);
          }
        }
      });

      // Pipe the WebM capture stream to FFmpeg's stdin using pipeline() for proper cleanup. When FFmpeg is killed during tab replacement, pipeline() automatically
      // destroys the source stream, preventing "write after end" errors that would occur with .pipe().
      pipeline(stream as unknown as Readable, ffmpeg.stdin).catch((error) => {

        const errorMessage = formatError(error);

        // EPIPE, "write after end", and "Premature close" errors are expected during cleanup when FFmpeg is killed or the capture stream is destroyed.
        if(errorMessage.includes("EPIPE") || errorMessage.includes("write after end") || errorMessage.includes("Premature close")) {

          return;
        }

        // Unexpected pipeline errors require cleanup.
        LOG.error("Capture pipeline error: %s.", errorMessage);
        ffmpeg.kill();

        if(onFFmpegError) {

          onFFmpegError(error);
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

      page.close().catch(() => {});
    }

    // Check for stale capture state error. This occurs after a crash when Chrome's tabCapture retains state from the previous process. Restarting Chrome clears
    // the stale state. We limit recovery attempts to prevent infinite loops if the issue persists.
    const errorMessage = formatError(error);

    if(errorMessage.includes("Cannot capture a tab with an active stream") && (staleCaptureRecoveryAttempts < MAX_STALE_CAPTURE_RECOVERY_ATTEMPTS)) {

      staleCaptureRecoveryAttempts++;

      LOG.warn("Stale capture state detected (attempt %d/%d). Restarting Chrome to clear state.",
        staleCaptureRecoveryAttempts, MAX_STALE_CAPTURE_RECOVERY_ATTEMPTS);

      // Kill Chrome entirely to clear all tabCapture state.
      await closeBrowser();

      // Wait for Chrome to fully exit before relaunching.
      await delay(1500);

      // Relaunch Chrome. getCurrentBrowser() creates a fresh instance since closeBrowser() cleared the reference.
      await getCurrentBrowser();

      // Retry the capture with a fresh browser.
      return createPageWithCapture(options);
    }

    throw error;
  }

  // Navigate and set up playback. For noVideo profiles, just navigate without video setup.
  let context: Frame | Page;

  try {

    if(profile.noVideo === false) {

      // Phase 1: Navigate to the page with retry. The 10-second navigationTimeout is appropriate for page loads, and retryOperation correctly reloads the page on
      // genuine navigation failures. Navigation is wrapped in retryOperation separately from channel selection so the timeout does not race with the internal click
      // retry loops in channel selection strategies (guideGrid can take 15-20 seconds for binary search + click retries).
      await retryOperation(
        async (): Promise<void> => {

          await navigateToPage(page, url, profile);
        },
        CONFIG.streaming.maxNavigationRetries,
        CONFIG.streaming.navigationTimeout,
        "page navigation for " + url,
        undefined,
        () => page.isClosed()
      );

      // Phase 2: Channel selection + video setup. Runs after navigation succeeds with no outer timeout racing against internal click retries. Each sub-step
      // (selectChannel, waitForVideoReady, etc.) has its own internal timeout via videoTimeout and click retry constants. A 30-second safety-net timeout prevents
      // pathological hangs if multiple internal timeouts chain sequentially.
      const PLAYBACK_INIT_TIMEOUT = 30000;

      const tuneResult = await Promise.race([
        initializePlayback(page, profile),
        new Promise<never>((_, reject) => {

          setTimeout(() => {

            reject(new Error("Playback initialization timed out after " + String(PLAYBACK_INIT_TIMEOUT) + "ms."));
          }, PLAYBACK_INIT_TIMEOUT);
        })
      ]);

      context = tuneResult.context;
    } else {

      await page.goto(url);
      context = page;
    }
  } catch(error) {

    // Clean up on navigation or playback initialization failure. Destroy the raw capture stream first to ensure chrome.tabCapture releases the capture.
    if(!rawCaptureStream.destroyed) {

      rawCaptureStream.destroy();
    }

    if(ffmpegProcess) {

      ffmpegProcess.kill();
    }

    unregisterManagedPage(page);

    if(!page.isClosed()) {

      page.close().catch(() => {});
    }

    throw error;
  }

  // Resize and minimize window.
  await resizeAndMinimizeWindow(page, !profile.noVideo);

  return {

    captureStream: outputStream,
    context,
    ffmpegProcess,
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
async function resolveRedirectUrl(url: string): Promise<string | null> {

  try {

    const response = await fetch(url, { method: "HEAD", signal: AbortSignal.timeout(3000) });

    return response.url;
  } catch {

    return null;
  }
}

// Stream Setup.

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

  const { channel, channelName, channelSelector, clickSelector, clickToPlay, noVideo, onTabReplacementFactory, profileOverride, url } = options;

  // Generate stream identifiers early so all log messages include them.
  const streamId = generateStreamId(channelName, url);
  const numericStreamId = getNextStreamId();
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

          LOG.info("Resolved redirect for profile detection: %s → %s (%s).", urlToResolve, resolvedUrl, redirectResult.profileName);
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

        LOG.info("Profile overridden to '%s' via query parameter.", profileOverride);
      } else {

        LOG.warn("Unknown profile override '%s', using resolved profile.", profileOverride);
      }
    }

    // Apply noVideo override if specified.
    if(noVideo) {

      profile = { ...profile, noVideo: true };
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

    // Create the tab replacement handler if a factory was provided. This is done after profile resolution so the handler has access to the final profile.
    const onTabReplacement = onTabReplacementFactory ? onTabReplacementFactory(numericStreamId, streamId, profile, metadataComment) : undefined;

    // Validate URL.
    const validation = validateStreamUrl(url);

    if(!validation.valid) {

      LOG.error("Invalid URL requested: %s - %s.", url, validation.reason ?? "Unknown error");

      throw new StreamSetupError(
        [ "Invalid URL: ", validation.reason ?? "Unknown error" ].join(""),
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
        [ "Maximum concurrent streams (", String(CONFIG.streaming.maxConcurrentStreams), ") reached. Try again later." ].join("")
      );
    }

    // Create page and start capture using the shared function. This handles browser page creation, capture initialization, FFmpeg spawning, and navigation with retry.
    let captureResult: CreatePageWithCaptureResult;

    try {

      captureResult = await createPageWithCapture({

        comment: metadataComment,
        onFFmpegError: onCircuitBreak,
        profile,
        streamId,
        url
      });
    } catch(error) {

      // createPageWithCapture handles its own cleanup on failure (closes page, kills FFmpeg).
      const errorMessage = formatError(error);
      const isBenign = errorMessage.toLowerCase().includes("abort") || errorMessage.toLowerCase().includes("session closed");

      if(!isBenign) {

        LOG.error("Stream setup failed for %s: %s.", url, errorMessage);
      }

      // Capture infrastructure errors should return 503 to signal Channels DVR to back off. These include Chrome capture state issues, queue timeouts, and stream
      // initialization failures. Using 503 with Retry-After prevents retry storms when there's a systemic issue.
      const captureErrorPatterns = [ "Cannot capture", "timed out", "Capture queue" ];
      const isCaptureError = captureErrorPatterns.some((pattern) => errorMessage.includes(pattern));

      throw new StreamSetupError("Stream error.", isCaptureError ? 503 : 500, "Failed to start stream.");
    }

    const { captureStream, context, ffmpegProcess, page, rawCaptureStream } = captureResult;

    // Monitor stream info for status updates.
    const monitorStreamInfo: MonitorStreamInfo = {

      channelName: channel?.name ?? null,
      numericStreamId,
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

      // Kill the FFmpeg process if using WebM+FFmpeg mode.
      if(ffmpegProcess) {

        ffmpegProcess.kill();
      }

      // Unregister from managed pages.
      unregisterManagedPage(page);

      // Close the browser page (fire-and-forget to avoid blocking on stuck pages).
      if(!page.isClosed()) {

        page.close().catch((error) => {

          LOG.warn("Page close error during cleanup: %s.", formatError(error));
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
      ffmpegProcess,
      numericStreamId,
      page,
      profile,
      profileName,
      rawCaptureStream,
      startTime,
      stopMonitor,
      streamId,
      url
    };
  });
}
