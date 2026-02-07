/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * video.ts: Video context and playback handling for PrismCast.
 */
import { EvaluateAbortError, LOG, delay, evaluateWithAbort, formatError } from "../utils/index.js";
import type { Frame, Page } from "puppeteer-core";
import type { ResolvedSiteProfile, TuneResult, VideoSelectorType } from "../types/index.js";
import { CONFIG } from "../config/index.js";
import { selectChannel } from "./channelSelection.js";

/* These functions manage the video element lifecycle for streaming capture. The key challenges we solve:
 *
 * 1. Video context resolution: Video elements may be in the main page or nested inside iframes. Some streaming sites (like those using Brightcove or JW Player
 *    embedded via iframe) require searching through frames to find the video. We detect this based on the site profile's needsIframeHandling flag.
 *
 * 2. Video selection: Pages may have multiple video elements (ads, previews, main content). The selectReadyVideo strategy finds the video with readyState >= 3,
 *    which typically identifies the actively playing main content. The selectFirstVideo strategy simply takes the first video in DOM order.
 *
 * 3. Ready state detection: We wait for readyState >= 3 (HAVE_FUTURE_DATA) rather than readyState === 4 (HAVE_ENOUGH_DATA) because live streams continuously
 *    receive data and may never reach readyState 4. The >= 3 threshold indicates enough data is buffered to begin playback.
 *
 * 4. Fullscreen styling: To maximize capture quality, we apply CSS styles that make the video fill the entire viewport. This CSS-based approach works regardless
 *    of the site's native fullscreen mechanism.
 *
 * 5. Volume enforcement: Some sites aggressively mute videos or lower volume. We enforce volume=1 and muted=false, and for particularly aggressive sites, we use
 *    Object.defineProperty to intercept and ignore attempts to change these values.
 *
 * 6. Recovery escalation: When playback stalls, we use increasingly aggressive recovery techniques:
 *    - Level 1: Basic play/unmute - just call play() and ensure audio is on.
 *    - Level 2: Seek to live edge - jump to the end of the seekable range for live streams.
 *    - Level 3: Reload source - reset video.src and call load() to reinitialize the player.
 *    - Level 4: Full page navigation (handled in monitor.ts, not here).
 *
 * The video selector system uses a string type identifier ("selectReadyVideo" or "selectFirstVideo") that's passed to page.evaluate() and interpreted in the
 * browser context. This avoids using eval() while still allowing dynamic video selection behavior.
 */

/**
 * Builds a selector type identifier for the video element based on the site profile. This returns a string that browser context code interprets to select the
 * appropriate video element. Using a string identifier instead of passing functions avoids serialization issues with page.evaluate() and is more secure than
 * eval()-based approaches.
 * @param profile - The site profile indicating video selection strategy.
 * @returns A selector type identifier: "selectReadyVideo" for sites with multiple videos, "selectFirstVideo" for standard sites.
 */
export function buildVideoSelectorType(profile: ResolvedSiteProfile): VideoSelectorType {

  // Sites with multiple video elements (ads, previews, main content) need to select by readyState to find the actively playing main content. Standard sites
  // with a single video element can just take the first one.
  return profile.selectReadyVideo ? "selectReadyVideo" : "selectFirstVideo";
}

/* These helper functions encapsulate common video element operations that are used in both initial setup and health monitoring. By centralizing these operations,
 * we ensure consistent behavior and reduce code duplication across the codebase.
 *
 * Many functions below contain identical video selection logic. This duplication is intentional and unavoidable due to Puppeteer's architecture. When we call
 * evaluateWithAbort(), the function body is serialized as a string and executed in Chrome's browser context. We cannot import shared modules (browser context has
 * no access to Node modules), pass function references (only serializable data crosses the boundary), or use closures over Node-side variables (they don't exist
 * in the browser). Each browser-context function must be self-contained. The duplication is the cost of operating across the Node/browser boundary. To maintain
 * consistency, all video selector blocks must use the same pattern, and when modifying the selection logic, update all functions that use this pattern.
 * - The selectorType parameter MUST be passed as the first argument to evaluateWithAbort
 */

/**
 * Video state information returned by getVideoState(). Contains all properties needed to assess playback health.
 */
export interface VideoStateInfo {

  // Current playback position in seconds.
  currentTime: number;

  // Whether the video has ended.
  ended: boolean;

  // Whether the video has a media error.
  error: boolean;

  // Whether the video is muted.
  muted: boolean;

  // Network state (0=EMPTY, 1=IDLE, 2=LOADING, 3=NO_SOURCE).
  networkState: number;

  // Whether the video is paused.
  paused: boolean;

  // Ready state (0=NOTHING, 1=METADATA, 2=CURRENT_DATA, 3=FUTURE_DATA, 4=ENOUGH_DATA).
  readyState: number;

  // Current volume level (0-1).
  volume: number;
}

/**
 * Gets the current state of the video element for health monitoring. Returns null if no video element is found.
 * @param context - The frame or page containing the video element.
 * @param selectorType - The video selector type for finding the element.
 * @returns The video state or null if no video found.
 */
export async function getVideoState(context: Frame | Page, selectorType: VideoSelectorType): Promise<VideoStateInfo | null> {

  return evaluateWithAbort(context, (type: string): VideoStateInfo | null => {

    let video: HTMLVideoElement | null | undefined;

    if(type === "selectReadyVideo") {

      video = Array.from(document.querySelectorAll("video")).find((v) => {

        return v.readyState >= 3;
      });
    } else {

      video = document.querySelector("video");
    }

    if(!video) {

      return null;
    }

    return {

      currentTime: video.currentTime,
      ended: video.ended,
      error: video.error !== null,
      muted: video.muted,
      networkState: video.networkState,
      paused: video.paused,
      readyState: video.readyState,
      volume: video.volume
    };
  }, [selectorType]);
}

/**
 * Enforces volume settings on the video element. Sets muted to false and volume to 1. This is called periodically during health monitoring to counter sites that
 * aggressively mute videos.
 * @param context - The frame or page containing the video element.
 * @param selectorType - The video selector type for finding the element.
 */
export async function enforceVideoVolume(context: Frame | Page, selectorType: VideoSelectorType): Promise<void> {

  await evaluateWithAbort(context, (type: string): void => {

    let video: HTMLVideoElement | null | undefined;

    if(type === "selectReadyVideo") {

      video = Array.from(document.querySelectorAll("video")).find((v) => {

        return v.readyState >= 3;
      });
    } else {

      video = document.querySelector("video");
    }

    if(video) {

      video.muted = false;
      video.volume = 1;
    }
  }, [selectorType]);
}

/**
 * Validation result for checking if a video element exists and is accessible.
 */
export interface VideoValidationResult {

  // Whether a video element was found.
  found: boolean;

  // The video's readyState if found.
  readyState?: number;
}

/**
 * Validates that a video element exists and returns its ready state. Used after page navigation to verify recovery succeeded.
 * @param context - The frame or page containing the video element.
 * @param selectorType - The video selector type for finding the element.
 * @returns Validation result indicating if video was found and its readyState.
 */
export async function validateVideoElement(context: Frame | Page, selectorType: VideoSelectorType): Promise<VideoValidationResult> {

  return evaluateWithAbort(context, (type: string): VideoValidationResult => {

    let video: HTMLVideoElement | null | undefined;

    if(type === "selectReadyVideo") {

      video = Array.from(document.querySelectorAll("video")).find((v) => {

        return v.readyState >= 3;
      });
    } else {

      video = document.querySelector("video");
    }

    return video ? { found: true, readyState: video.readyState } : { found: false };
  }, [selectorType]);
}

/**
 * Result from checking video presence, distinguishing between "no video exists" and "video exists but not ready".
 */
export interface VideoPresenceResult {

  // Whether any video element exists in the DOM (regardless of readyState).
  anyVideoExists: boolean;

  // Maximum readyState among all video elements (or undefined if no videos exist).
  maxReadyState?: number;

  // Whether a video matching the selector criteria (readyState >= 3 for selectReadyVideo) was found.
  readyVideoFound: boolean;

  // Total number of video elements in the DOM.
  videoCount: number;
}

/**
 * Checks video presence in the context, returning detailed information about what videos exist and their states. This helps distinguish between:
 * - No video element exists at all (DOM issue, wrong context)
 * - Video elements exist but none are ready (buffering, still loading)
 * - Ready video exists (normal operation)
 *
 * This is useful when getVideoState returns null to determine if we should wait (video buffering) or escalate (no video at all).
 * @param context - The frame or page containing the video element.
 * @param selectorType - The video selector type for finding the element.
 * @returns Detailed presence information.
 */
export async function checkVideoPresence(context: Frame | Page, selectorType: VideoSelectorType): Promise<VideoPresenceResult> {

  return evaluateWithAbort(context, (type: string): VideoPresenceResult => {

    const videos = Array.from(document.querySelectorAll("video"));
    const videoCount = videos.length;

    if(videoCount === 0) {

      return { anyVideoExists: false, readyVideoFound: false, videoCount: 0 };
    }

    // Find the maximum readyState among all videos.
    const maxReadyState = Math.max(...videos.map((v) => v.readyState));

    // Check if a video matching the selector criteria exists.
    let readyVideoFound = false;

    if(type === "selectReadyVideo") {

      readyVideoFound = videos.some((v) => v.readyState >= 3);
    } else {

      // For selectFirstVideo, any video counts as ready.
      readyVideoFound = true;
    }

    return { anyVideoExists: true, maxReadyState, readyVideoFound, videoCount };
  }, [selectorType]);
}

/**
 * Reloads the video source to force the player to reinitialize. This clears the src attribute, calls load() to reset the player state, restores the original src,
 * and calls load() again. This is more disruptive than seeking but can fix players stuck in error states or with corrupted internal state.
 * @param context - The frame or page containing the video element.
 * @param selectorType - The video selector type for finding the element.
 */
export async function reloadVideoSource(context: Frame | Page, selectorType: VideoSelectorType): Promise<void> {

  await evaluateWithAbort(context, (type: string): void => {

    let video: HTMLVideoElement | null | undefined;

    if(type === "selectReadyVideo") {

      video = Array.from(document.querySelectorAll("video")).find((v) => {

        return v.readyState >= 3;
      });
    } else {

      video = document.querySelector("video");
    }

    if(video) {

      const currentSrc = video.src;

      video.src = "";
      video.load();
      video.src = currentSrc;
      video.load();
    }
  }, [selectorType]);
}

/**
 * Starts video playback by ensuring the video is unmuted, at full volume, and playing. This combines volume enforcement with play() initiation for efficient single
 * round-trip execution in the browser context.
 * @param context - The frame or page containing the video element.
 * @param selectorType - The video selector type for finding the element.
 */
export async function startVideoPlayback(context: Frame | Page, selectorType: VideoSelectorType): Promise<void> {

  await evaluateWithAbort(context, (type: string): void => {

    let video: HTMLVideoElement | null | undefined;

    if(type === "selectReadyVideo") {

      video = Array.from(document.querySelectorAll("video")).find((v) => {

        return v.readyState >= 3;
      });
    } else {

      video = document.querySelector("video");
    }

    if(video) {

      // Ensure audio is enabled. Some sites mute videos by default or in response to various events.
      video.muted = false;
      video.volume = 1;

      // Call play() if the video is paused. The catch handles cases where autoplay is blocked (though our Chrome flags should prevent this).
      if(video.paused) {

        video.play().catch((): void => {

          // Ignore play errors - the monitor will retry if playback doesn't resume.
        });
      }
    }
  }, [selectorType]);
}

/**
 * Navigates a browser page to the specified URL with site-appropriate wait conditions. The navigation strategy depends on the site's player implementation:
 *
 * - waitForNetworkIdle=true: Wait for network activity to settle (no requests for 500ms). This ensures all JavaScript has loaded and the player is fully
 *   initialized. Used for sites with complex async initialization.
 *
 * - waitForNetworkIdle=false: Return as soon as the page fires load event. Used for sites that have persistent connections or polling that would prevent
 *   networkidle from ever completing.
 *
 * Navigation timeouts are handled gracefully - we log a warning but don't throw, since the video may have loaded successfully even if networkidle never
 * completed.
 * @param page - The Puppeteer page object.
 * @param url - The URL to navigate to.
 * @param profile - The site profile containing navigation preferences.
 */
export async function navigateToPage(page: Page, url: string, profile: ResolvedSiteProfile): Promise<void> {

  if(profile.waitForNetworkIdle) {

    try {

      // Wait for network idle (no requests for 500ms). This ensures complex JavaScript players have fully initialized. The networkidle2 strategy allows up
      // to 2 concurrent requests, which handles sites with persistent connections for analytics.
      await page.goto(url, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
    } catch(error) {

      // Timeout errors during navigation are common and often non-fatal - the video may have loaded successfully even if some background requests never
      // completed. We log a warning and continue rather than throwing.
      if(error && ((error as Error).name === "TimeoutError")) {

        LOG.warn("Page navigation timed out after %sms for %s.", CONFIG.streaming.navigationTimeout, url);
      } else {

        // Non-timeout errors (network failure, invalid URL, etc.) should be propagated for retry handling.
        throw error;
      }
    }
  } else {

    // Simple navigation without waiting for network idle. Returns after the load event fires. Used for sites that would never reach networkidle due to
    // persistent connections, streaming data, or continuous polling.
    await page.goto(url);
  }
}

/**
 * Finds the appropriate context (frame or page) containing the video element. Some streaming sites embed their video player in an iframe, which creates a
 * separate document context. We need to find this iframe and operate within it to access the video element.
 *
 * The search process:
 * 1. If the profile doesn't need iframe handling, return the main page directly
 * 2. Wait for an iframe element to appear in the DOM
 * 3. Allow time for the iframe content to initialize (embedded players often load additional resources)
 * 4. Search through all frames to find one containing a video element
 * 5. Fall back to the main page if no iframe contains a video
 * @param page - The Puppeteer page object.
 * @param profile - The site profile indicating whether iframe handling is needed.
 * @returns The frame or page containing the video element.
 */
export async function findVideoContext(page: Page, profile: ResolvedSiteProfile): Promise<Frame | Page> {

  // For sites that don't use iframes (most common case), the video is directly in the main page document. Skip the iframe search.
  if(!profile.needsIframeHandling) {

    return page;
  }

  // Wait for an iframe element to appear in the page DOM. This ensures the site has created the embedded player container.
  await page.waitForSelector("iframe", { timeout: CONFIG.streaming.videoTimeout });

  // Poll for a video element to appear in any iframe. Complex embedded players (Brightcove, JW Player, etc.) load additional resources and scripts after the
  // iframe element appears, so the video may not be immediately available. We retry the search with brief pauses, using the configured delay as the overall
  // timeout ceiling. This replaces a fixed delay with early exit — if the video appears quickly, we proceed immediately.
  const deadline = Date.now() + CONFIG.playback.iframeInitDelay;

  let iframeSearchComplete = false;
  let lastFrameCount = 0;

  while(!iframeSearchComplete && (Date.now() < deadline)) {

    const pageFrames = page.frames();

    lastFrameCount = pageFrames.length;

    for(const frame of pageFrames) {

      // Skip the main frame since we're looking for video in iframes, not the main page.
      if(frame === page.mainFrame()) {

        continue;
      }

      try {

        // Check if this frame contains a video element. We use a short timeout (2 seconds) to prevent a single hanging frame from consuming the polling budget.
        // eslint-disable-next-line no-await-in-loop
        const hasVideo = await evaluateWithAbort(frame, (): boolean => {

          return !!document.querySelector("video");
        }, undefined, 2000);

        if(hasVideo) {

          return frame;
        }
      } catch(error) {

        // AbortError means stream was terminated — stop polling immediately.
        if(error instanceof EvaluateAbortError) {

          iframeSearchComplete = true;

          break;
        }

        // Other errors (cross-origin, detached frame) — skip this frame and continue searching.
      }
    }

    // Brief pause before re-checking. 200ms intervals provide responsive polling without excessive CDP overhead.
    if(!iframeSearchComplete && (Date.now() < deadline)) {

      // eslint-disable-next-line no-await-in-loop
      await delay(200);
    }
  }

  // If no iframe contains a video, fall back to the main page. This is a potential issue for iframe-handling profiles since we expected the video to be in an
  // iframe. Log a warning and verify the main page actually has a video element.
  LOG.warn("No iframe contained video element. Falling back to main page context (searched %s frames).", Math.max(0, lastFrameCount - 1));

  // Check if the main page actually contains a video element.
  try {

    const mainPageHasVideo = await evaluateWithAbort(page, (): boolean => {

      return !!document.querySelector("video");
    });

    if(!mainPageHasVideo) {

      LOG.warn("Main page fallback: no video element found in main page either.");
    }
  } catch(_error) {

    // Ignore evaluation errors - we'll return the page anyway and let the caller handle missing video. Also handles AbortError if stream is terminated.
  }

  return page;
}

/**
 * Waits for the video element to reach a ready state indicating it has loaded enough data to begin playback. We use readyState >= 3 (HAVE_FUTURE_DATA) as the
 * threshold because:
 *
 * - readyState 0 (HAVE_NOTHING): No data available
 * - readyState 1 (HAVE_METADATA): Duration and dimensions known, but no media data
 * - readyState 2 (HAVE_CURRENT_DATA): Data for current position available, but not enough for playback
 * - readyState 3 (HAVE_FUTURE_DATA): Enough data for current position plus at least a little ahead
 * - readyState 4 (HAVE_ENOUGH_DATA): Enough data to play through without buffering (for known-length media)
 *
 * Live streams continuously receive data and may never reach readyState 4, so we use >= 3 as the threshold. The health monitor handles any subsequent buffering
 * or playback issues.
 * @param context - The frame or page containing the video element.
 * @param profile - The site profile with video selection preferences.
 */
export async function waitForVideoReady(context: Frame | Page, profile: ResolvedSiteProfile): Promise<void> {

  // First, wait for any video element to appear in the DOM. This catches cases where the video element is created dynamically by JavaScript.
  await context.waitForSelector("video", { timeout: CONFIG.streaming.videoTimeout });

  if(profile.selectReadyVideo) {

    // For sites with multiple video elements, wait for at least one to reach readyState >= 3. This typically identifies the main content video rather than
    // preloaded ad videos or preview thumbnails.
    await context.waitForFunction(
      (): boolean => {

        const videos = document.querySelectorAll("video");

        return Array.from(videos).some((v) => {

          return v.readyState >= 3;
        });
      },
      { timeout: CONFIG.streaming.videoTimeout }
    );
  } else {

    // For standard sites with a single video, wait for that specific video to reach readyState >= 3.
    await context.waitForFunction(
      (): boolean => {

        const video = document.querySelector("video");

        return !!video && (video.readyState >= 3);
      },
      { timeout: CONFIG.streaming.videoTimeout }
    );
  }
}

/**
 * Applies fullscreen styling to the video element using CSS to maximize the capture area. This CSS-based approach works for all sites regardless of their native
 * fullscreen mechanism (keyboard shortcuts, JavaScript API, etc.).
 *
 * The styling:
 * - position: fixed - Removes the video from document flow and positions relative to viewport
 * - top: 0; left: 0; width: 100%; height: 100% - Fills the entire viewport
 * - zIndex: 999000 - Ensures the video appears above all other page content
 * - objectFit: contain - Maintains aspect ratio while fitting within the viewport
 * - background: black - Fills any letterbox/pillarbox areas with black
 * - cursor: none - Hides the mouse cursor for cleaner capture
 * @param context - The frame or page containing the video element.
 * @param selectorType - The video selector type for finding the element.
 */
export async function applyVideoStyles(context: Frame | Page, selectorType: VideoSelectorType): Promise<void> {

  await evaluateWithAbort(context, (type: string): void => {

    // Find the video element using the appropriate selection strategy.
    let video: HTMLVideoElement | null | undefined;

    if(type === "selectReadyVideo") {

      video = Array.from(document.querySelectorAll("video")).find((v) => {

        return v.readyState >= 3;
      });
    } else {

      video = document.querySelector("video");
    }

    if(!video) {

      return;
    }

    // Apply fullscreen-like styling via CSS. This is more reliable than the native fullscreen API because it doesn't require user gesture and can't be
    // blocked by the site's CSP.
    video.style.background = "black";
    video.style.cursor = "none";
    video.style.height = "100%";
    video.style.left = "0";
    video.style.objectFit = "contain";
    video.style.position = "fixed";
    video.style.top = "0";
    video.style.width = "100%";
    video.style.zIndex = "999000";
  }, [selectorType]);
}

/**
 * Locks the volume properties on the video element to prevent the site's JavaScript from muting our stream. Some sites (like France24) aggressively mute videos
 * or lower volume in response to various events. They may reset volume on play, on focus, on visibility change, or on a timer.
 *
 * This function uses Object.defineProperty to intercept property access, making it impossible for site JavaScript to change muted or volume values. The property
 * descriptors are set to configurable: true so the browser can still access the underlying values for playback.
 *
 * The function is idempotent - a __volumeLocked flag on the video element prevents applying the lock multiple times.
 * @param context - The frame or page containing the video element.
 * @param selectorType - The video selector type for finding the element.
 */
export async function lockVolumeProperties(context: Frame | Page, selectorType: VideoSelectorType): Promise<void> {

  try {

    await evaluateWithAbort(context, (type: string): void => {

      // Find the video element.
      let video: HTMLVideoElement | null | undefined;

      if(type === "selectReadyVideo") {

        video = Array.from(document.querySelectorAll("video")).find((v) => {

          return v.readyState >= 3;
        });
      } else {

        video = document.querySelector("video");
      }

      // Skip if no video found or already locked. The __volumeLocked flag prevents applying the lock multiple times, which would cause issues with the
      // property descriptors.
      if(!video || (video as HTMLVideoElement & { __volumeLocked?: boolean }).__volumeLocked) {

        return;
      }

      // Override the muted property to always return false and ignore attempts to set it. This prevents site JavaScript from muting the video.
      Object.defineProperty(video, "muted", {

        configurable: true,
        get: function(): boolean {

          return false;
        },
        set: function(): void {

          // Ignore attempts to mute. The setter does nothing, so any code setting video.muted = true has no effect.
        }
      });

      // Override the volume property to always return 1 (full volume) and ignore attempts to change it.
      Object.defineProperty(video, "volume", {

        configurable: true,
        get: function(): number {

          return 1;
        },
        set: function(): void {

          // Ignore attempts to change volume. The setter does nothing, so any code setting video.volume = 0.5 has no effect.
        }
      });

      // Mark the video as locked to prevent re-applying the lock.
      (video as HTMLVideoElement & { __volumeLocked?: boolean }).__volumeLocked = true;
    }, [selectorType]);

    LOG.info("Volume properties locked successfully.");
  } catch(error) {

    // Volume locking is not critical to stream function - log a warning but don't fail the operation. Also handles AbortError if stream is terminated.
    LOG.warn("Could not lock volume properties: %s.", formatError(error));
  }
}

/**
 * Triggers fullscreen mode using the appropriate method for the site. Different sites have different fullscreen implementations:
 *
 * - Keyboard shortcuts (fullscreenKey): Many players use "f" as a keyboard shortcut for fullscreen. We send this keypress to activate the player's native
 *   fullscreen mode.
 *
 * - JavaScript Fullscreen API (useRequestFullscreen): Some players require calling video.requestFullscreen() directly. This may trigger browser permission
 *   prompts or be blocked by CSP, but works on many sites.
 *
 * Note that we also apply CSS-based fullscreen styling separately (in applyVideoStyles), which provides a reliable fallback when native fullscreen methods fail.
 * @param page - The Puppeteer page object for keyboard input.
 * @param context - The frame or page containing the video element.
 * @param profile - The site profile indicating fullscreen method.
 * @param selectorType - The video selector type for finding the element.
 */
export async function triggerFullscreen(
  page: Page,
  context: Frame | Page,
  profile: ResolvedSiteProfile,
  selectorType: VideoSelectorType
): Promise<void> {

  // Try clicking a fullscreen button if configured. This fires before keyboard and API methods because clicking the site's own fullscreen control is the most
  // reliable approach — it uses the site's native mechanism. The element existence check guards against toggle buttons that have changed state or disappeared
  // (e.g., after the player is already maximized). Keyboard and API methods serve as fallbacks below.
  if(profile.fullscreenSelector) {

    try {

      const buttonExists = await page.$(profile.fullscreenSelector);

      if(buttonExists) {

        await page.click(profile.fullscreenSelector);

        // Brief delay for the site's fullscreen animation to complete before subsequent checks.
        await delay(300);
      }
    } catch(error) {

      LOG.warn("Could not click fullscreen button %s: %s.", profile.fullscreenSelector, formatError(error));
    }
  }

  // Try keyboard shortcut if configured. The fullscreenKey is typically "f" for most video players.
  if(profile.fullscreenKey) {

    await page.keyboard.type(profile.fullscreenKey);
  }

  // Try JavaScript Fullscreen API if configured. This calls video.requestFullscreen() which may trigger browser fullscreen mode.
  if(profile.useRequestFullscreen) {

    try {

      await evaluateWithAbort(context, (type: string): void => {

        // Find the video element.
        let video: HTMLVideoElement | null | undefined;

        if(type === "selectReadyVideo") {

          video = Array.from(document.querySelectorAll("video")).find((v) => {

            return v.readyState >= 3;
          });
        } else {

          video = document.querySelector("video");
        }

        // Request fullscreen if the API is available. The catch handles cases where fullscreen is blocked by browser policy or CSP.
        if(video?.requestFullscreen) {

          video.requestFullscreen().catch((): void => {

            // Ignore fullscreen errors - the CSS-based styling provides a fallback.
          });
        }
      }, [selectorType]);
    } catch(error) {

      LOG.warn("Could not trigger fullscreen: %s.", formatError(error));
    }
  }
}

/**
 * Verifies that the video element is filling the viewport, indicating that fullscreen styling was successfully applied. This function checks the video element's
 * bounding rectangle against the viewport dimensions to determine if the video appears fullscreen.
 *
 * The verification allows for some tolerance because:
 * - The video may have letterboxing/pillarboxing due to aspect ratio differences
 * - Some browsers report slightly smaller dimensions due to scrollbars or UI chrome
 * - CSS rounding may cause small discrepancies
 *
 * We require the video to fill at least 85% of the viewport in at least one dimension (the constraining dimension for aspect ratio) and at least 50% in the
 * other dimension to catch obviously broken cases.
 * @param context - The frame or page containing the video element.
 * @param selectorType - The video selector type for finding the element.
 * @returns True if the video appears to be fullscreen, false otherwise.
 */
async function verifyFullscreen(context: Frame | Page, selectorType: VideoSelectorType): Promise<boolean> {

  try {

    return await evaluateWithAbort(context, (type: string): boolean => {

      // Find the video element using the appropriate selection strategy.
      let video: HTMLVideoElement | null | undefined;

      if(type === "selectReadyVideo") {

        video = Array.from(document.querySelectorAll("video")).find((v) => {

          return v.readyState >= 3;
        });
      } else {

        video = document.querySelector("video");
      }

      if(!video) {

        return false;
      }

      const rect = video.getBoundingClientRect();
      const viewportWidth = window.innerWidth;
      const viewportHeight = window.innerHeight;

      // Calculate how much of the viewport the video fills in each dimension.
      const widthRatio = rect.width / viewportWidth;
      const heightRatio = rect.height / viewportHeight;

      // The video should fill at least 85% in at least one dimension (accounting for aspect ratio letterboxing) and at least 50% in the other dimension (to
      // catch obviously broken cases where the video is tiny or off-screen).
      const fillsWidth = widthRatio >= 0.85;
      const fillsHeight = heightRatio >= 0.85;
      const minimumCoverage = (widthRatio >= 0.5) && (heightRatio >= 0.5);

      return (fillsWidth || fillsHeight) && minimumCoverage;
    }, [selectorType]);
  } catch(_error) {

    // If we can't evaluate (page closed, frame detached), assume fullscreen failed.
    return false;
  }
}

/**
 * Applies aggressive fullscreen styling when standard styling fails. This function uses multiple techniques to force the video to fill the viewport:
 *
 * 1. CSS !important flags: Overrides any site CSS that might be constraining the video element.
 *
 * 2. Hide sibling elements: Sets display: none on sibling elements in the video's parent container, removing player controls, overlays, and other UI that might
 *    be obscuring the video.
 *
 * 3. Expand parent containers: Walks up the DOM tree and applies fullscreen styling to parent elements, breaking out of any constrained containers.
 *
 * This is more invasive than standard styling and may break site functionality, but ensures the video fills the viewport for capture.
 * @param context - The frame or page containing the video element.
 * @param selectorType - The video selector type for finding the element.
 */
async function applyAggressiveFullscreen(context: Frame | Page, selectorType: VideoSelectorType): Promise<void> {

  await evaluateWithAbort(context, (type: string): void => {

    // Find the video element using the appropriate selection strategy.
    let video: HTMLVideoElement | null | undefined;

    if(type === "selectReadyVideo") {

      video = Array.from(document.querySelectorAll("video")).find((v) => {

        return v.readyState >= 3;
      });
    } else {

      video = document.querySelector("video");
    }

    if(!video) {

      return;
    }

    // Apply fullscreen styling with !important flags to override any site CSS. Using cssText replaces all existing inline styles, ensuring a clean slate.
    video.style.cssText = [
      "background: black !important",
      "cursor: none !important",
      "height: 100% !important",
      "left: 0 !important",
      "object-fit: contain !important",
      "position: fixed !important",
      "top: 0 !important",
      "width: 100% !important",
      "z-index: 999999 !important"
    ].join("; ");

    // Hide sibling elements that might be overlaying the video (player controls, progress bars, channel logos, etc.).
    const parent = video.parentElement;

    if(parent) {

      for(const sibling of Array.from(parent.children)) {

        if((sibling !== video) && (sibling instanceof HTMLElement)) {

          sibling.style.setProperty("display", "none", "important");
        }
      }
    }

    // Expand parent containers up the DOM tree. Sites often wrap videos in multiple container divs with constrained dimensions. We need to break out of these.
    let container = video.parentElement;

    while(container && (container !== document.body)) {

      container.style.cssText = [
        "height: 100% !important",
        "left: 0 !important",
        "position: fixed !important",
        "top: 0 !important",
        "width: 100% !important",
        "z-index: 999998 !important"
      ].join("; ");

      container = container.parentElement;
    }
  }, [selectorType]);
}

/**
 * Ensures the video is displayed fullscreen with verification and retry logic. This function orchestrates the fullscreen process:
 *
 * 1. Initial attempt: Apply CSS styles and trigger fullscreen API
 * 2. Verify: Check if video is filling the viewport
 * 3. Simple retry: If verification fails, wait and retry the same approach (timing issues are common)
 * 4. Escalate: If simple retries fail, apply aggressive fullscreen techniques
 *
 * The retry approach handles the common case where fullscreen fails due to timing - the page may still be initializing when we first attempt. Escalation to
 * aggressive techniques is a last resort that may break site functionality but ensures video fills the viewport.
 * @param page - The Puppeteer page object for keyboard input.
 * @param context - The frame or page containing the video element.
 * @param profile - The site profile indicating fullscreen method.
 * @param selectorType - The video selector type for finding the element.
 */
export async function ensureFullscreen(
  page: Page,
  context: Frame | Page,
  profile: ResolvedSiteProfile,
  selectorType: VideoSelectorType
): Promise<void> {

  // Configuration for retry behavior. These values are tuned for typical page load timing.
  const maxSimpleRetries = 3;
  const retryDelay = 500;
  const verifyDelay = 200;

  for(let attempt = 1; attempt <= maxSimpleRetries; attempt++) {

    // Apply CSS styles to make the video fill the viewport.
    // eslint-disable-next-line no-await-in-loop
    await applyVideoStyles(context, selectorType);

    // Trigger native fullscreen using the site's preferred method (keyboard shortcut or JavaScript API).
    // eslint-disable-next-line no-await-in-loop
    await triggerFullscreen(page, context, profile, selectorType);

    // Wait a moment for fullscreen to take effect. The browser needs time to process the style changes and any fullscreen API calls.
    // eslint-disable-next-line no-await-in-loop
    await delay(verifyDelay);

    // Verify that fullscreen succeeded by checking video dimensions.
    // eslint-disable-next-line no-await-in-loop
    const isFullscreen = await verifyFullscreen(context, selectorType);

    if(isFullscreen) {

      if(attempt > 1) {

        LOG.info("Fullscreen succeeded on attempt %s.", attempt);
      }

      return;
    }

    // Fullscreen verification failed. If we have retries remaining, wait and try again.
    if(attempt < maxSimpleRetries) {

      LOG.warn("Fullscreen verification failed (attempt %s/%s). Retrying after %sms.", attempt, maxSimpleRetries, retryDelay);

      // eslint-disable-next-line no-await-in-loop
      await delay(retryDelay);
    }
  }

  // All simple retries exhausted. Escalate to aggressive fullscreen techniques.
  LOG.warn("Fullscreen failed after %s attempts. Escalating to aggressive fullscreen.", maxSimpleRetries);

  await applyAggressiveFullscreen(context, selectorType);

  // Also try keyboard "f" as a last resort if the profile doesn't already use it. Many players respond to the "f" key for fullscreen.
  if(!profile.fullscreenKey) {

    await page.keyboard.type("f");
  }

  // Final verification after aggressive techniques.
  await delay(verifyDelay);

  const finalCheck = await verifyFullscreen(context, selectorType);

  if(finalCheck) {

    LOG.info("Fullscreen succeeded after aggressive techniques.");
  } else {

    LOG.warn("Fullscreen could not be verified even after aggressive techniques. Video may not fill viewport.");
  }
}

/**
 * Ensures the video is playing with proper audio settings. This is the core playback function that handles both initial setup and recovery from stalls. It is
 * designed to be idempotent - safe to call multiple times without adverse effects.
 *
 * Recovery escalation levels (higher levels include all lower-level actions):
 *
 * LEVEL 1 - Basic recovery (default):
 * - Set muted=false and volume=1
 * - Call play() if video is paused
 * - Ensure fullscreen with verification and retry (applies CSS, triggers fullscreen API/keyboard, verifies video fills viewport, escalates to aggressive
 *   techniques if needed)
 * - Lock volume properties if profile requires it
 *
 * LEVEL 2 - Reload video source:
 * - All level 1 actions, plus:
 * - Reset video.src to empty, call load()
 * - Restore original src, call load() again
 * - Wait for source to reinitialize
 * - This forces the player to completely reinitialize, fixing stuck players
 *
 * Level 3 (full page navigation) is handled by the playback monitor, not this function.
 * @param page - The Puppeteer page object.
 * @param context - The frame or page containing the video element.
 * @param profile - The site profile containing all behavior flags.
 * @param recoveryLevel - The escalation level (1-2). Defaults to 1.
 */
export async function ensurePlayback(
  page: Page,
  context: Frame | Page,
  profile: ResolvedSiteProfile,
  recoveryLevel?: number
): Promise<void> {

  const selectorType = buildVideoSelectorType(profile);
  const level = recoveryLevel ?? 1;

  // LEVEL 2: Reload video source. This forces the player to completely reinitialize by clearing and restoring the src attribute. This can fix players stuck in
  // error states or with corrupted internal state.
  if(level >= 2) {

    try {

      await reloadVideoSource(context, selectorType);

      // Wait for the source to reload. The player needs time to parse the manifest, establish connections, and buffer initial data.
      await delay(CONFIG.playback.sourceReloadDelay);
    } catch(_error) {

      // Source reload errors are non-fatal - we continue with basic recovery actions.
    }
  }

  // LEVEL 1: Basic play/unmute recovery. This is the minimum recovery action - ensure the video is playing with audio enabled. We do this before fullscreen so
  // the video is playing when we verify dimensions.
  try {

    await startVideoPlayback(context, selectorType);
  } catch(_error) {

    // Basic recovery errors are non-fatal - we continue with other actions.
  }

  // Ensure fullscreen with verification and retry. This applies CSS styling, triggers native fullscreen, verifies the video fills the viewport, and retries
  // with escalating techniques if needed.
  await ensureFullscreen(page, context, profile, selectorType);

  // Apply volume locking if the profile requires it. This prevents the site from muting the video after we've set volume.
  if(profile.lockVolumeProperties) {

    await lockVolumeProperties(context, selectorType);
  }
}

/**
 * Dismisses any stale overlay or modal that may be covering the guide grid. After a failed click attempt on the on-now cell, the playback overlay or entity modal
 * can remain open, obscuring the guide and preventing subsequent channel selection attempts from locating guide rows. Pressing Escape closes most modal overlays
 * in React-based SPAs.
 * @param page - The Puppeteer page object.
 */
async function dismissGuideOverlay(page: Page): Promise<void> {

  try {

    await page.keyboard.press("Escape");

    // Brief delay for the overlay dismiss animation to complete and the guide grid to re-render.
    await delay(500);
  } catch(error) {

    // Overlay dismissal is best-effort. The overlay may not exist, or the page may be in a state where keyboard input is ignored.
    LOG.debug("Could not dismiss guide overlay: %s.", formatError(error));
  }
}

/**
 * Performs all post-navigation channel initialization: selects the channel, finds the video context, clicks to play if needed, waits for video readiness, and
 * ensures playback with fullscreen styling. This function is separated from navigateToPage() so that retryOperation() in setup.ts can wrap only navigation with a
 * timeout, while channel selection and video setup run with their own internal time budgets (click retry loops, videoTimeout, etc.) without being killed by the
 * navigation timeout.
 *
 * For guideGrid channel selection failures, the function attempts a single retry after dismissing any stale overlay that may be covering the guide grid. This
 * handles the case where a failed click attempt left an overlay open, causing subsequent locateOnNowCell calls to fail.
 *
 * @param page - The Puppeteer page object.
 * @param profile - The site profile containing all behavior flags.
 * @returns The video context (frame or page) for subsequent monitoring.
 */
export async function initializePlayback(page: Page, profile: ResolvedSiteProfile): Promise<TuneResult> {

  // For multi-channel players (like usanetwork.com/live with multiple channels), select the desired channel from the UI. The selectChannel function checks the
  // profile's channelSelection strategy and channelSelector to determine if/how to select a channel.
  let channelResult = await selectChannel(page, profile);

  if(!channelResult.success) {

    // For guideGrid strategy, a stale overlay from a previous failed click attempt may be covering the guide. Dismiss it and retry channel selection once.
    if(profile.channelSelection.strategy === "guideGrid") {

      LOG.warn("Guide grid channel selection failed: %s. Dismissing overlay and retrying.", channelResult.reason ?? "Unknown reason");

      await dismissGuideOverlay(page);

      channelResult = await selectChannel(page, profile);
    }

    if(!channelResult.success) {

      LOG.warn("Channel selection may have failed: %s.", channelResult.reason ?? "Unknown reason");
    }
  }

  // Find the video context, which may be an iframe for embedded players. Some streaming sites embed their video player in an iframe, requiring us to search
  // through frames to find the one containing the video element.
  const context = await findVideoContext(page, profile);

  // For clickToPlay sites, we need to click an element to start playback. These players require user interaction to begin playing, even with autoplay enabled. If
  // clickSelector is set, we click that element (typically a play button overlay); otherwise we click the video element directly.
  if(profile.clickToPlay) {

    const clickTarget = profile.clickSelector ?? "video";

    try {

      // Wait for the click target to appear in the DOM. Play button overlays may be rendered after initial page load.
      await context.waitForSelector(clickTarget, { timeout: CONFIG.streaming.videoTimeout });
      await context.click(clickTarget);
    } catch(clickError) {

      LOG.warn("Could not click %s to initiate playback: %s.", clickTarget, formatError(clickError));
    }
  }

  // Wait for video to be ready (readyState >= 3). This ensures enough data is buffered for playback to begin smoothly.
  await waitForVideoReady(context, profile);

  // Ensure playback is started, unmuted, and fullscreen. This applies CSS styling, triggers native fullscreen, and enforces volume settings.
  await ensurePlayback(page, context, profile, 1);

  return { context };
}

/**
 * Tunes to a channel by navigating to the URL and initializing video playback. This is the single source of truth for channel initialization, used by both initial
 * stream setup and recovery. Having one authoritative function ensures consistent behavior and prevents code divergence between setup and recovery paths.
 *
 * The tuning process:
 * 1. Navigate: Load the target URL using site-appropriate wait conditions
 * 2. Select channel: For multi-channel players, click the desired channel in the UI
 * 3. Find video: Locate the video element (which may be in an iframe)
 * 4. Click to play: For Brightcove-style players, click the video to start playback
 * 5. Wait for ready: Ensure the video has buffered enough data to play
 * 6. Ensure playback: Start playback, unmute, and apply fullscreen styling
 *
 * Note: Stream context for logging is automatically retrieved from AsyncLocalStorage. Callers should wrap their stream handling code in runWithStreamContext() to
 * ensure log messages include the stream ID prefix.
 *
 * @param page - The Puppeteer page object.
 * @param url - The URL to navigate to.
 * @param profile - The site profile containing all behavior flags.
 * @returns The video context (frame or page) for subsequent monitoring.
 */
export async function tuneToChannel(page: Page, url: string, profile: ResolvedSiteProfile): Promise<TuneResult> {

  // Navigate to the target URL. This handles timeout and network errors, returning when the page is loaded. The navigation strategy (networkidle vs load event)
  // is determined by the profile's waitForNetworkIdle flag.
  await navigateToPage(page, url, profile);

  // Perform all post-navigation initialization: channel selection, video context resolution, click to play, video readiness, and fullscreen.
  return initializePlayback(page, profile);
}
