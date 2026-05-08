/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * manifestInterceptor.ts: CDP Network domain listener for intercepting HLS manifest URLs.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { LOG, chromeFetch, startTimer } from "../utils/index.ts";
import type { HlsPlaylistKind } from "../native/probe.ts";
import type { Nullable } from "../types/index.ts";
import { classifyHlsPlaylist } from "../native/probe.ts";

/* This module installs a Chrome DevTools Protocol (CDP) listener on the Network domain to capture HLS manifest URLs as the browser's video player fetches them. The
 * listener is shared by two consumers:
 *
 * - Native HLS streaming: captures the playlist URL during stream startup and hands the CDP session off to the proxy for subsequent token-refresh tunes. See
 *   installManifestInterceptor() and the consumers in src/native/. This consumer accepts both master and media playlists; the probe normalizes either kind into a
 *   MediaFeed so downstream code does not branch on which kind arrived.
 * - Tune verification: confirms that the channel a strategy clicked actually loaded by matching the resulting manifest URL against an expected predicate (e.g.,
 *   call sign in the path). See awaitMatchingManifest() and the consumer in src/browser/tuning/fox.ts. This consumer expects master playlists only because tune
 *   verification is a multi-channel concept that does not apply to direct-tune media-only sources.
 *
 * Both APIs share the underlying CDP observer that filters .m3u8 URLs, fetches the body, and asks classifyHlsPlaylist() for the playlist kind; the kind is
 * forwarded to consumers so each can apply its own kind-specific filter without re-reading the body. Classification lives in exactly one place
 * (src/native/probe.ts) so the master/media decision cannot drift between the interceptor's transport-layer gate and the probe's resolution-layer dispatch.
 * We use a direct fetch rather than CDP's Network.getResponseBody because Chrome's network cache can evict response bodies before we read them, causing
 * spurious "No data found for resource" failures.
 *
 * For multi-channel sites, the video player may load manifests for channels other than the one requested. installManifestInterceptor tracks both the first and
 * latest master manifest URLs to handle two distinct tuning patterns. The pattern is selected by the directTune flag passed to finalize():
 *
 * - Direct tunes (directTune=true): the navigated URL itself selects the channel, so the first manifest captured is the correct one - the player loaded it for
 *   the navigated URL. Background prefetches for other channels may arrive later but should be ignored. The first manifest wins.
 * - Guide tunes (directTune=false): page navigation loads a default channel's manifest, then a tuning strategy (guide-grid click, JS bridge call, webpack
 *   injection, etc.) triggers a new manifest fetch for the requested channel. The latest manifest wins.
 *
 * awaitMatchingManifest takes a different approach: it resolves with the first master manifest whose URL satisfies the caller-supplied predicate. The CDP session
 * is torn down automatically on resolution. This is the verification path - the caller has already triggered the tune and now wants confirmation that the
 * resulting manifest belongs to the expected channel.
 */

// Default timeout for the long-lived interception used by native HLS.
const INTERCEPTION_TIMEOUT = 15000;

// Brief delay after finalize() is called to allow any in-flight manifest response to arrive. Applied only for guide-based tunes (directTune=false) where the
// channel switch may trigger a manifest fetch that arrives milliseconds after the click handler returns. Direct tunes skip this delay and resolve immediately.
const FINALIZE_SETTLE_DELAY = 1500;

// Default timeout for awaitMatchingManifest. Tune verification is a short-lived check after a click, so the budget is tighter than the native interception path.
const VERIFICATION_TIMEOUT = 8000;

// Timeout for the per-response manifest body fetch. The body is fed to classifyHlsPlaylist() to determine whether it is a master playlist; if Chrome's network
// cache evicts the body or the CDN serves a slow response, we abandon and treat the response as non-master.
const MANIFEST_BODY_FETCH_TIMEOUT = 5000;

/**
 * Captured-URL state used by selectInterceptedManifest() to choose the playlist URL appropriate for a given resolution mode. Modeled as a plain record so the
 * selection logic is testable in isolation from the CDP listener that maintains the underlying state.
 */
export interface InterceptedManifestState {

  // True when the resolution should pick the first URL captured (direct tune); false when it should pick the latest URL captured (guide tune).
  directTune: boolean;

  // The first observed master playlist URL, or null when no master playlist arrived during the interception window.
  firstMasterUrl: Nullable<string>;

  // The first observed media playlist URL, or null when no media playlist arrived during the interception window.
  firstMediaUrl: Nullable<string>;

  // The most recently observed master playlist URL, or null when no master playlist arrived during the interception window.
  latestMasterUrl: Nullable<string>;

  // The most recently observed media playlist URL, or null when no media playlist arrived during the interception window.
  latestMediaUrl: Nullable<string>;
}

/**
 * Selects the playlist URL appropriate for a given resolution mode from the per-kind first/latest state captured by the manifest observer. Master URLs always
 * outrank media URLs when both kinds were observed because masters declare richer metadata (variant bandwidth, resolution, separate audio renditions). Within
 * the chosen kind, direct tunes pick the first URL and guide tunes pick the latest URL. Returns null when no qualifying URL is available.
 *
 * Pure function exported for unit testing; the closure inside installManifestInterceptor() consumes it through the same shape.
 *
 * @param state - The captured-URL state at resolution time.
 * @returns The selected URL, or null when no URL matches the resolution mode.
 */
export function selectInterceptedManifest(state: InterceptedManifestState): Nullable<string> {

  if(state.directTune) {

    return state.firstMasterUrl ?? state.firstMediaUrl;
  }

  return state.latestMasterUrl ?? state.latestMediaUrl;
}

/**
 * Result of manifest interception containing the CDP session for reuse during token refresh and the playlist URL the probe will consume. The URL may be either
 * a master or a media playlist; the probe normalizes both kinds into a MediaFeed so downstream code does not branch.
 */
export interface ManifestInterceptionResult {

  // The CDP session used for interception. Passed to the native proxy for lifecycle management - the proxy cleans it up on stop() and hands it off during token refresh.
  cdpSession: CDPSession;

  // The HLS playlist URL intercepted from the browser's network requests. For direct tunes, the first qualifying URL captured; for guide tunes, the most recent.
  // Master playlists take precedence over media playlists when both have arrived during the interception window because master playlists declare additional
  // metadata (variant bandwidth, resolution, separate audio renditions) that improves the resulting MediaFeed.
  masterManifestUrl: string;
}

/**
 * Handle returned by installManifestInterceptor. Provides the interception promise and a finalize function to signal that channel selection is complete.
 */
export interface ManifestInterceptorHandle {

  // Signals that channel selection is complete. When directTune is true (single-channel site navigated by URL), resolves immediately with the first captured manifest -
  // the one loaded for the navigated URL. When false (guide-based multi-channel site), applies a brief settle delay and resolves with the latest manifest - the one
  // from the channel switch click.
  finalize: (directTune: boolean) => void;

  // Promise that resolves with the interception result after finalize() is called (or after the timeout expires, whichever comes first).
  promise: Promise<Nullable<ManifestInterceptionResult>>;
}

/**
 * Internal handle returned by startManifestObserver. Encapsulates the CDP session, the per-playlist callback registration, and the cleanup function. Both public
 * APIs (installManifestInterceptor and awaitMatchingManifest) build on top of this.
 */
interface ManifestObserver {

  // The CDP session running the listener.
  cdpSession: CDPSession;

  // Disposes the observer: removes the Network listener, optionally detaches the session. Pass keepSession=true when the caller intends to reuse the session
  // (native HLS does this so the proxy can perform token-refresh fetches on the same session).
  dispose: (keepSession: boolean) => void;
}

/**
 * Installs a CDP Network.responseReceived listener that calls onPlaylist() for every recognized HLS playlist URL observed. The listener filters to .m3u8 URLs,
 * fetches the body, and asks classifyHlsPlaylist() for the playlist kind; recognized kinds (master, media) fire the callback with the kind label, while
 * unrecognized bodies are skipped. Consumers apply their own kind-specific filtering on top of the callback - installManifestInterceptor accepts both kinds
 * with master priority, while awaitMatchingManifest accepts master only.
 *
 * Returns null when the page is closed or the CDP session cannot be created. The caller is responsible for calling dispose() to clean up.
 * @param page - The Puppeteer page to monitor.
 * @param onPlaylist - Callback invoked for each verified HLS playlist URL together with its kind.
 * @param logCategory - Debug log category for this observer's lifecycle messages.
 * @returns The observer handle, or null if installation failed.
 */
async function startManifestObserver(page: Page, onPlaylist: (url: string, kind: HlsPlaylistKind) => void,
  logCategory: string): Promise<Nullable<ManifestObserver>> {

  if(page.isClosed()) {

    return null;
  }

  let cdpSession: CDPSession;

  try {

    cdpSession = await page.createCDPSession();
  } catch(error) {

    LOG.debug(logCategory, "Failed to create CDP session: %s.", String(error));

    return null;
  }

  try {

    await cdpSession.send("Network.enable");
  } catch(error) {

    LOG.debug(logCategory, "Failed to enable Network domain: %s.", String(error));

    return null;
  }

  LOG.debug(logCategory, "CDP manifest observer installed.");

  let disposed = false;

  // Listen for completed responses. We use Network.responseReceived to capture .m3u8 URLs, then fetch the manifest body directly from Node.js to verify it is
  // a master manifest. This avoids CDP's Network.getResponseBody which is unreliable - Chrome's network cache can evict response bodies before we read them.
  const onResponseReceived = async (params: { response: { url: string } }): Promise<void> => {

    if(disposed) {

      return;
    }

    const url = params.response.url;

    // Filter for .m3u8 URLs. Strip query parameters before checking the extension.
    const urlPath = url.split("?")[0] ?? "";

    if(!urlPath.endsWith(".m3u8")) {

      return;
    }

    LOG.debug(logCategory, "Observed .m3u8 response: %s.", url.slice(0, 120));

    // Fetch the manifest body directly to inspect whether this is a master manifest. The URL contains embedded CDN auth tokens, so no cookies or special
    // headers are needed beyond the Chrome User-Agent injected by chromeFetch().
    try {

      const response = await chromeFetch(url, { signal: AbortSignal.timeout(MANIFEST_BODY_FETCH_TIMEOUT) });

      if(!response.ok) {

        LOG.debug(logCategory, "Manifest fetch returned HTTP %s for %s.", response.status, url.slice(0, 120));

        return;
      }

      const body = await response.text();

      // Delegate the master/media decision to the canonical classifier. Recognized kinds (master, media) are forwarded to the callback together with the kind
      // label; consumers apply their own filter. Both consumers of onPlaylist maintain their own resolved-flag guard, so a callback that arrives after dispose
      // is harmless - the downstream resolve is idempotent.
      const kind = classifyHlsPlaylist(body);

      if(kind === "unknown") {

        LOG.debug(logCategory, "Skipping .m3u8 classified as unknown (no recognizable HLS directives).");

        return;
      }

      onPlaylist(url, kind);
    } catch(error) {

      LOG.debug(logCategory, "Could not fetch .m3u8 body: %s.", String(error));
    }
  };

  // Wrap the async handler to avoid no-misused-promises - EventEmitter.on() does not handle returned promises.
  const wrappedHandler = (...args: [{ response: { url: string } }]): void => { void onResponseReceived(...args); };

  cdpSession.on("Network.responseReceived", wrappedHandler);

  const dispose = (keepSession: boolean): void => {

    if(disposed) {

      return;
    }

    disposed = true;
    cdpSession.off("Network.responseReceived", wrappedHandler);

    if(!keepSession) {

      removeManifestInterceptor(cdpSession);
    }
  };

  return { cdpSession, dispose };
}

/**
 * Installs a long-lived CDP listener that tracks observed HLS playlist URLs on the given page. Both master and media playlists are accepted; the listener tracks
 * first/latest URLs separately per kind so master-based and media-only sites both work without separate code paths. Master playlists take precedence over media
 * playlists when both have arrived during the interception window because masters carry richer metadata (variant bandwidth, resolution, separate audio
 * renditions) that improves the resulting MediaFeed. The returned handle provides a finalize(directTune) callback - when called, the interceptor resolves with
 * whichever URL is appropriate for the tune type:
 *
 * - directTune=true: resolves immediately (or after the settle delay if no manifest has arrived yet) with the first manifest captured. Used by sites where the
 *   navigated URL itself selects the channel and the player loads its manifest before the click handler returns. Master-first URL preferred over media-first.
 * - directTune=false: resolves after the settle delay with the latest manifest captured. Used by guide-based sites where the channel-switch click triggers a new
 *   manifest fetch that may arrive milliseconds after the click handler returns. Master-latest URL preferred over media-latest.
 *
 * On timeout (finalize never called), the listener resolves with the latest captured URL using the same master-priority rule, or null if none arrived. The CDP
 * session is preserved in the result and ownership transfers to the caller; callers must call removeManifestInterceptor() when done.
 * @param page - The Puppeteer page to monitor.
 * @param timeout - Maximum time in milliseconds to wait for a manifest. Acts as a safety net if finalize() is never called.
 * @returns The interceptor handle, or null if the CDP session could not be created.
 */
export async function installManifestInterceptor(page: Page, timeout: number = INTERCEPTION_TIMEOUT): Promise<Nullable<ManifestInterceptorHandle>> {

  const elapsed = startTimer();

  // Track first/latest URLs separately per playlist kind. Master URLs take precedence at selection time because masters carry richer metadata. For direct
  // tunes, the first URL of the higher-priority kind wins; for guide tunes, the latest URL of the higher-priority kind wins. Without separate per-kind
  // tracking, a master-based site whose player happens to also load a media playlist for a different channel would risk picking the wrong one.
  let firstMasterUrl: Nullable<string> = null;
  let latestMasterUrl: Nullable<string> = null;
  let firstMediaUrl: Nullable<string> = null;
  let latestMediaUrl: Nullable<string> = null;
  let resolved = false;
  let manifestCount = 0;

  const { promise, resolve } = Promise.withResolvers<Nullable<ManifestInterceptionResult>>();

  const observer = await startManifestObserver(page, (url: string, kind: HlsPlaylistKind): void => {

    manifestCount++;

    if(kind === "master") {

      firstMasterUrl ??= url;
      latestMasterUrl = url;
    } else if(kind === "media") {

      firstMediaUrl ??= url;
      latestMediaUrl = url;
    }

    LOG.debug("native:intercept", "%s playlist captured (#%s) in %sms: %s.", kind, manifestCount, elapsed(), url.slice(0, 120));
  }, "native:intercept");

  if(!observer) {

    return null;
  }

  // Selects the URL appropriate for the resolution mode by delegating to the pure selectInterceptedManifest() helper. Closure variables are passed by value so
  // the helper does not depend on shared mutable state.
  const selectUrl = (directTune: boolean): Nullable<string> => selectInterceptedManifest({

    directTune,
    firstMasterUrl,
    firstMediaUrl,
    latestMasterUrl,
    latestMediaUrl
  });

  // Timeout guard. If finalize() is never called (defensive), resolve with whatever we have after the timeout.
  const timer = setTimeout(() => {

    if(resolved) {

      return;
    }

    resolved = true;

    // The timeout path mirrors the latest-URL semantics of a guide tune; if no finalize() ever arrived we err on the side of the most recent capture.
    const selected = selectUrl(false);

    if(selected) {

      LOG.debug("native:intercept", "Manifest interception timed out after %sms. Resolving with latest URL (%s captured).", elapsed(), manifestCount);
      observer.dispose(true);
      resolve({ cdpSession: observer.cdpSession, masterManifestUrl: selected });
    } else {

      LOG.debug("native:intercept", "Manifest interception timed out after %sms. No HLS playlist captured.", elapsed());
      observer.dispose(false);
      resolve(null);
    }
  }, timeout);

  // Finalize function exposed on the returned handle. Called by the stream setup code after channel selection is complete. The resolution strategy depends on
  // two factors: whether a qualifying manifest has already been captured, and whether the tune is direct or guide-based.
  //
  // - Manifest captured + direct tune: resolve immediately (A&E, most TVE sites - manifest arrived during page load).
  // - Manifest captured + guide tune: wait FINALIZE_SETTLE_DELAY (Fox guide, Hulu - a newer manifest from the channel switch may still arrive).
  // - No manifest captured + either: wait FINALIZE_SETTLE_DELAY (Fox Sports - manifest fetch starts after video element appears, hasn't arrived yet).
  const finalize = (directTune: boolean): void => {

    if(resolved) {

      return;
    }

    // Helper that resolves the promise with the current state. The selectUrl helper applies the master-first priority and the directTune first/latest semantics.
    const resolveNow = (): void => {

      if(resolved) {

        return;
      }

      resolved = true;
      clearTimeout(timer);

      const selectedUrl = selectUrl(directTune);

      if(selectedUrl) {

        LOG.debug("native:intercept", "Interception finalized in %sms with %s playlist capture(s). Using %s: %s.", elapsed(), manifestCount,
          directTune ? "first" : "latest", selectedUrl.slice(0, 120));
        observer.dispose(true);
        resolve({ cdpSession: observer.cdpSession, masterManifestUrl: selectedUrl });
      } else {

        LOG.debug("native:intercept", "Interception finalized in %sms but no HLS playlist was captured.", elapsed());
        observer.dispose(false);
        resolve(null);
      }
    };

    // For direct tunes, resolve immediately if a master URL has already arrived. We do not short-circuit on a media-only first URL because a master may still
    // be in flight - waiting the settle delay gives master priority a chance to take effect. Once the settle elapses we resolve with whichever URL ranks
    // highest under selectUrl.
    if(directTune && firstMasterUrl) {

      resolveNow();
    } else {

      setTimeout(resolveNow, FINALIZE_SETTLE_DELAY);
    }
  };

  return { finalize, promise };
}

/**
 * Awaits the first master HLS manifest URL whose URL satisfies the supplied predicate. Used by tune-verification paths where the caller has just clicked or
 * navigated to a target channel and wants to confirm the resulting stream belongs to that channel rather than to whatever default the page was previously
 * showing.
 *
 * Resolves with the matching URL on success. Resolves with null if no matching manifest arrives within the timeout. The CDP session is torn down automatically
 * in both cases - the verification path does not hand off the session to a downstream consumer.
 * @param page - The Puppeteer page to monitor.
 * @param predicate - Test applied to each verified master manifest URL. Return true to accept the URL and resolve.
 * @param timeout - Maximum time in milliseconds to wait for a matching manifest.
 * @returns The matching URL, or null on timeout.
 */
export async function awaitMatchingManifest(page: Page, predicate: (url: string) => boolean,
  timeout: number = VERIFICATION_TIMEOUT): Promise<Nullable<string>> {

  const elapsed = startTimer();
  let resolved = false;

  const { promise, resolve } = Promise.withResolvers<Nullable<string>>();

  // Tune verification is a multi-channel concept that only applies to master playlists - direct-tune media-only sources do not run a guide-click verification
  // step because the page navigation itself selects the channel. We reject media playlists at the consumer level rather than asking the observer to filter so
  // the observer keeps a single canonical contract (forward all recognized HLS playlists with their kind).
  const observer = await startManifestObserver(page, (url: string, kind: HlsPlaylistKind): void => {

    if(resolved) {

      return;
    }

    if(kind !== "master") {

      LOG.debug("native:intercept", "Tune verification ignoring non-master playlist (%s).", kind);

      return;
    }

    if(predicate(url)) {

      resolved = true;

      LOG.debug("native:intercept", "Matching manifest found in %sms: %s.", elapsed(), url.slice(0, 120));
      observer?.dispose(false);
      clearTimeout(timer);
      resolve(url);
    } else {

      LOG.debug("native:intercept", "Master manifest did not match predicate: %s.", url.slice(0, 120));
    }
  }, "native:intercept");

  if(!observer) {

    return null;
  }

  const timer = setTimeout(() => {

    if(resolved) {

      return;
    }

    resolved = true;

    LOG.debug("native:intercept", "Awaiting matching manifest timed out after %sms.", elapsed());
    observer.dispose(false);
    resolve(null);
  }, timeout);

  return promise;
}

/**
 * Removes the Network domain listener and detaches the CDP session. Safe to call multiple times or on an already-detached session. Public so that native HLS
 * consumers (which receive the CDP session through ManifestInterceptionResult) can clean up after token-refresh handoffs and proxy shutdown.
 * @param cdpSession - The CDP session to clean up.
 */
export function removeManifestInterceptor(cdpSession: CDPSession): void {

  try {

    cdpSession.removeAllListeners("Network.responseReceived");
    void cdpSession.send("Network.disable").catch(() => { /* Session may already be detached. */ });
    void cdpSession.detach().catch(() => { /* Session may already be detached. */ });
  } catch(_error) {

    // Ignore errors during cleanup - the session may already be detached.
  }

  LOG.debug("native:intercept", "CDP manifest interceptor removed.");
}
