/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * intercept.ts: CDP Network domain listener for intercepting HLS manifest URLs.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { LOG, startTimer } from "../utils/index.js";
import type { Nullable } from "../types/index.js";

/* This module installs a Chrome DevTools Protocol (CDP) listener on the Network domain to capture HLS manifest URLs as the browser's video player fetches them. Unlike
 * the per-call CDP sessions created by withCDPSession() in cdp.ts, we create a long-lived session here because the listener must remain active across multiple network
 * requests until channel selection is complete.
 *
 * The listener filters for URLs ending in .m3u8 and uses Network.getResponseBody to inspect the response content. Master manifests contain #EXT-X-STREAM-INF directives
 * (variant playlist references), which distinguishes them from variant/media playlists.
 *
 * For multi-channel sites, the video player may load manifests for channels other than the one requested. The interceptor tracks both the first and latest master
 * manifest URLs to handle two distinct scenarios:
 *
 * - Direct-navigation sites (Fox Sports): The first manifest is the correct one — the player loaded it for the navigated URL. Background prefetches for other channels
 *   may arrive later but should be ignored. finalize(directTune=true) selects the first manifest.
 *
 * - Guide-based sites (Fox.com, Hulu, Sling, YouTube TV): The player loads a default channel's manifest during page navigation, then the guide grid strategy clicks the
 *   correct entry, triggering a new manifest fetch. finalize(directTune=false) selects the latest manifest.
 */

// Default timeout for manifest interception after installation.
const INTERCEPTION_TIMEOUT = 15000;

// Brief delay after finalize() is called to allow any in-flight manifest response to arrive. Applied only for guide-based tunes (directTune=false) where the
// channel switch may trigger a manifest fetch that arrives milliseconds after the click handler returns. Direct tunes skip this delay and resolve immediately.
const FINALIZE_SETTLE_DELAY = 1500;

/**
 * Result of manifest interception containing the CDP session for reuse during token refresh and the master manifest URL.
 */
export interface ManifestInterceptionResult {

  // The CDP session used for interception. Passed to the native proxy for lifecycle management — the proxy cleans it up on stop() and hands it off during token refresh.
  cdpSession: CDPSession;

  // The master manifest URL intercepted from the browser's network requests. For direct tunes, the first manifest captured; for guide tunes, the most recent.
  masterManifestUrl: string;
}

/**
 * Handle returned by installManifestInterceptor. Provides the interception promise and a finalize function to signal that channel selection is complete.
 */
export interface ManifestInterceptorHandle {

  // Signals that channel selection is complete. When directTune is true (single-channel site navigated by URL), resolves immediately with the first captured manifest —
  // the one loaded for the navigated URL. When false (guide-based multi-channel site), applies a brief settle delay and resolves with the latest manifest — the one
  // from the channel switch click.
  finalize: (directTune: boolean) => void;

  // Promise that resolves with the interception result after finalize() is called (or after the timeout expires, whichever comes first).
  promise: Promise<Nullable<ManifestInterceptionResult>>;
}

/**
 * Installs a CDP Network.responseReceived listener on the given page to capture master HLS manifest URLs. The listener tracks both the first and latest master
 * manifest URLs as they arrive. The returned handle provides a promise that resolves when finalize() is called — the caller invokes finalize() after channel
 * selection is complete, passing directTune to control which manifest is selected.
 *
 * For direct-navigation sites (directTune=true), the first master manifest is the correct one — it was loaded for the navigated URL. Background prefetches for
 * other channels are ignored. For guide-based sites (directTune=false), the latest manifest is selected — it corresponds to the channel the user clicked.
 *
 * @param page - The Puppeteer page to monitor.
 * @param timeout - Maximum time in milliseconds to wait for a manifest (default: 15 seconds). Acts as a safety net if finalize() is never called.
 * @returns The interceptor handle, or null if the CDP session could not be created.
 */
export async function installManifestInterceptor(page: Page, timeout: number = INTERCEPTION_TIMEOUT): Promise<Nullable<ManifestInterceptorHandle>> {

  if(page.isClosed()) {

    return null;
  }

  const elapsed = startTimer();

  let cdpSession: CDPSession;

  try {

    cdpSession = await page.createCDPSession();
  } catch(error) {

    LOG.debug("native:intercept", "Failed to create CDP session: %s.", String(error));

    return null;
  }

  // Enable the Network domain to receive request and response events.
  try {

    await cdpSession.send("Network.enable");
  } catch(error) {

    LOG.debug("native:intercept", "Failed to enable Network domain: %s.", String(error));

    return null;
  }

  LOG.debug("native:intercept", "CDP manifest interceptor installed.");

  // Track both the first and most recently observed master manifest URLs. For direct-navigation sites (directTune=true), the first manifest is the correct one — the
  // player loaded it for the navigated URL. Background prefetches for other channels may arrive later and must not overwrite the selection. For guide-based sites
  // (directTune=false), the last manifest is correct — the guide click triggers a new manifest fetch that replaces whatever the player initially loaded.
  let firstManifestUrl: Nullable<string> = null;
  let latestManifestUrl: Nullable<string> = null;
  let resolved = false;
  let manifestCount = 0;

  // Finalize function, assigned inside the promise constructor and exposed on the returned handle.
  let finalize: (directTune: boolean) => void = () => { /* No-op until promise constructor assigns the real function. */ };

  const promise = new Promise<Nullable<ManifestInterceptionResult>>((resolve) => {

    // Timeout guard. If finalize() is never called (defensive), resolve with whatever we have after the timeout.
    const timer = setTimeout(() => {

      if(!resolved) {

        resolved = true;

        cdpSession.off("Network.responseReceived", wrappedHandler);

        if(latestManifestUrl) {

          LOG.debug("native:intercept", "Manifest interception timed out after %sms. Resolving with latest URL (%s captured).", elapsed(), manifestCount);

          resolve({ cdpSession, masterManifestUrl: latestManifestUrl });
        } else {

          LOG.debug("native:intercept", "Manifest interception timed out after %sms. No master manifest captured.", elapsed());
          removeManifestInterceptor(cdpSession);
          resolve(null);
        }
      }
    }, timeout);

    // Listen for completed responses. We use Network.responseReceived to identify .m3u8 responses, then fetch the body to inspect content.
    const onResponseReceived = async (params: { requestId: string; response: { url: string } }): Promise<void> => {

      if(resolved) {

        return;
      }

      const url = params.response.url;

      // Filter for .m3u8 URLs. Strip query parameters before checking the extension.
      const urlPath = url.split("?")[0];

      if(!urlPath.endsWith(".m3u8")) {

        return;
      }

      LOG.debug("native:intercept", "Observed .m3u8 response: %s.", url.slice(0, 120));

      // Fetch the response body to inspect whether this is a master manifest.
      try {

        const bodyResult = await cdpSession.send("Network.getResponseBody", { requestId: params.requestId }) as { base64Encoded: boolean; body: string };

        const body = bodyResult.base64Encoded ? Buffer.from(bodyResult.body, "base64").toString("utf-8") : bodyResult.body;

        // Master manifests contain #EXT-X-STREAM-INF directives that reference variant playlists. Media/variant playlists contain #EXTINF or #EXT-X-TARGETDURATION
        // but not #EXT-X-STREAM-INF.
        if(body.includes("#EXT-X-STREAM-INF")) {

          manifestCount++;
          firstManifestUrl ??= url;
          latestManifestUrl = url;

          LOG.debug("native:intercept", "Master manifest captured (#%s) in %sms: %s.", manifestCount, elapsed(), url.slice(0, 120));
        } else {

          LOG.debug("native:intercept", "Skipping non-master .m3u8 (no #EXT-X-STREAM-INF).");
        }
      } catch(error) {

        // Response body may be unavailable if the request was redirected or the response was a preflight. This is expected and not an error.
        LOG.debug("native:intercept", "Could not read .m3u8 response body: %s.", String(error));
      }
    };

    // Wrap the async handler to avoid no-misused-promises — EventEmitter.on() does not handle returned promises.
    const wrappedHandler = (...args: [{ requestId: string; response: { url: string } }]): void => { void onResponseReceived(...args); };

    cdpSession.on("Network.responseReceived", wrappedHandler);

    // Assign the finalize function. Called by the stream setup code after channel selection is complete. The resolution strategy depends on two factors: whether a
    // manifest has already been captured, and whether the tune is direct or guide-based.
    //
    // - Manifest captured + direct tune: resolve immediately (A&E, most TVE sites — manifest arrived during page load).
    // - Manifest captured + guide tune: wait FINALIZE_SETTLE_DELAY (Fox guide, Hulu — a newer manifest from the channel switch may still arrive).
    // - No manifest captured + either: wait FINALIZE_SETTLE_DELAY (Fox Sports — manifest fetch starts after video element appears, hasn't arrived yet).
    finalize = (directTune: boolean): void => {

      if(resolved) {

        return;
      }

      // Helper that resolves the promise with the current state. For direct tunes, the first manifest is the correct one (loaded for the navigated URL — background
      // prefetches for other channels may have overwritten latestManifestUrl). For guide tunes, the last manifest is correct (from the channel switch click).
      const resolveNow = (): void => {

        if(resolved) {

          return;
        }

        resolved = true;
        clearTimeout(timer);

        cdpSession.off("Network.responseReceived", wrappedHandler);

        const selectedUrl = directTune ? firstManifestUrl : latestManifestUrl;

        if(selectedUrl) {

          LOG.debug("native:intercept", "Interception finalized in %sms with %s manifest(s). Using %s: %s.", elapsed(), manifestCount,
            directTune ? "first" : "latest", selectedUrl.slice(0, 120));

          resolve({ cdpSession, masterManifestUrl: selectedUrl });
        } else {

          LOG.debug("native:intercept", "Interception finalized in %sms but no master manifest was captured.", elapsed());
          removeManifestInterceptor(cdpSession);
          resolve(null);
        }
      };

      if(directTune && firstManifestUrl) {

        // Direct tune with manifest already captured: resolve immediately with zero delay. The first manifest arrived during page load and is the correct one.
        resolveNow();
      } else {

        // Either no manifest captured yet (some providers fetch the manifest after the video element appears) or a guide-based tune where the channel switch
        // may produce a newer manifest. Wait briefly for in-flight responses.
        setTimeout(resolveNow, FINALIZE_SETTLE_DELAY);
      }
    };
  });

  return { finalize, promise };
}

/**
 * Removes the Network domain listener and detaches the CDP session. Safe to call multiple times or on an already-detached session.
 *
 * @param cdpSession - The CDP session to clean up.
 */
export function removeManifestInterceptor(cdpSession: CDPSession): void {

  try {

    cdpSession.removeAllListeners("Network.responseReceived");
    void cdpSession.send("Network.disable").catch(() => { /* Session may already be detached. */ });
    void cdpSession.detach().catch(() => { /* Session may already be detached. */ });
  } catch(_error) {

    // Ignore errors during cleanup — the session may already be detached.
  }

  LOG.debug("native:intercept", "CDP manifest interceptor removed.");
}
