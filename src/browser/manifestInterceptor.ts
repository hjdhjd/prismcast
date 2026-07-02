/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * manifestInterceptor.ts: Selection state machines for HLS manifest interception, layered on top of the HLS playlist observer.
 */
import type { HlsPlaylistObserver, ObservedHlsPlaylist } from "./hlsPlaylistObserver.ts";
import { LOG, startTimer } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import { observeHlsPlaylists } from "./hlsPlaylistObserver.ts";

/* This module hosts two consumer-facing state machines built on top of the HLS playlist observer (hlsPlaylistObserver.ts), which in turn rides on the tab-wide
 * network observer (tabNetworkObserver.ts). The layering:
 *
 *   tabNetworkObserver  -> tab-wide CDP transport (OOPIF-aware via auto-attach + flatten)
 *   hlsPlaylistObserver -> HLS filter + body fetch + classify + per-URL dedup
 *   manifestInterceptor -> selection: first-vs-latest, master priority, finalize semantics, predicate match
 *
 * Two state machines live here:
 *
 * - installManifestInterceptor(): the long-lived capture used by native HLS setup. Accepts both master and media playlists, tracks first and latest URLs per
 *   kind, and resolves with the URL appropriate for the tune type when finalize() is called. Master playlists take precedence over media playlists when both
 *   arrive during the interception window because masters carry richer metadata (variant bandwidth, resolution, separate audio renditions).
 * - awaitMatchingManifest(): the short-lived verification used after a guide-click tune. Accepts master playlists only and resolves with the first URL whose
 *   path satisfies a caller-supplied predicate. Media playlists are skipped at the consumer level because tune verification is a multi-channel concept that
 *   does not apply to direct-tune media-only sources.
 *
 * Race safety, URL filtering, body fetching, and classification all live below this module - we receive a stream of recognized HLS playlists with their kind
 * and apply selection policy. Public callers do not hold CDP session references; the underlying HLS observer disposes its tab network observer deterministically
 * when finalize, timeout, predicate match, or explicit dispose fires.
 */

// Default timeout for the long-lived interception used by native HLS.
const INTERCEPTION_TIMEOUT = 15000;

// Brief delay after finalize() is called to allow any in-flight manifest response to arrive. Applied for guide-based tunes (directTune=false), where the channel
// switch may trigger a manifest fetch that arrives milliseconds after the click handler returns, and for direct tunes that have not yet captured a master
// manifest. A direct tune skips this delay and resolves immediately only once a master manifest has already arrived.
const FINALIZE_SETTLE_DELAY = 1500;

// Default timeout for awaitMatchingManifest. Tune verification is a short-lived check after a click, so the budget is tighter than the native interception path.
const VERIFICATION_TIMEOUT = 8000;

/**
 * Captured-URL state used by selectInterceptedManifest() to choose the playlist URL appropriate for a given resolution mode. Modeled as a plain record so the
 * selection logic is testable in isolation from the network plumbing that maintains the underlying state.
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
 * Pure selection policy exported so the rule is testable in isolation from the network plumbing; the closure inside installManifestInterceptor() consumes it
 * through the same shape.
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
 * Result of manifest interception. The URL may be either a master or a media playlist; the probe normalizes both kinds into a MediaFeed so downstream code does
 * not branch on which kind originally arrived. The underlying observer and its CDP sessions are owned and disposed internally - callers do not hold session
 * references.
 */
export interface ManifestInterceptionResult {

  // The HLS playlist URL intercepted from the browser's network requests. For direct tunes, the first qualifying URL captured; for guide tunes, the most recent.
  // Master playlists take precedence over media playlists when both have arrived during the interception window because master playlists declare additional
  // metadata (variant bandwidth, resolution, separate audio renditions) that improves the resulting MediaFeed.
  masterManifestUrl: string;
}

/**
 * Handle returned by installManifestInterceptor. Provides the interception promise, a finalize function to signal that channel selection is complete, and an
 * explicit dispose path. Implements TC39 Symbol.dispose so callers may use the "using" keyword for scope-bound cleanup, including on thrown errors.
 */
export interface ManifestInterceptorHandle extends Disposable {

  // Cancels the interception. Resolves the promise with null if it has not already settled. Safe to call multiple times. Aliased to [Symbol.dispose] so the
  // handle can be used with "using" syntax for scope-bound cleanup, including on thrown errors.
  readonly dispose: () => void;

  // Signals that channel selection is complete. When directTune is true (single-channel site navigated by URL) and a master manifest has already arrived,
  // resolves immediately with the first captured manifest - the one loaded for the navigated URL. When directTune is true but only a media-only manifest, or
  // nothing, has arrived so far, it waits the settle delay so master priority still has a chance to take effect. When false (guide-based multi-channel site),
  // applies a brief settle delay and resolves with the latest manifest - the one from the channel switch click.
  readonly finalize: (directTune: boolean) => void;

  // Promise that resolves with the interception result after finalize() is called (or after the timeout expires, whichever comes first). Resolves with null
  // when no qualifying playlist was captured.
  readonly promise: Promise<Nullable<ManifestInterceptionResult>>;

  // TC39 explicit resource management hook. Aliases dispose() so "using interceptor = ..." produces deterministic teardown at scope exit.
  readonly [Symbol.dispose]: () => void;
}

/**
 * Installs a long-lived HLS playlist observer that tracks observed playlist URLs on the given page. Both master and media playlists are accepted; first/latest
 * URLs are tracked separately per kind so master-based and media-only sites both work without separate code paths. Master playlists take precedence over media
 * playlists when both have arrived during the interception window because masters carry richer metadata (variant bandwidth, resolution, separate audio
 * renditions) that improves the resulting MediaFeed. The returned handle provides a finalize(directTune) callback - when called, the observer resolves with
 * whichever URL is appropriate for the tune type:
 *
 * - directTune=true: resolves immediately if a master manifest has already arrived (or after the settle delay when only a media-only manifest, or nothing, has
 *   arrived so far) with the first manifest captured. Used by sites where the navigated URL itself selects the channel and the player loads its manifest before
 *   the click handler returns. Master-first URL preferred over media-first.
 * - directTune=false: resolves after the settle delay with the latest manifest captured. Used by guide-based sites where the channel-switch click triggers a
 *   new manifest fetch that may arrive milliseconds after the click handler returns. Master-latest URL preferred over media-latest.
 *
 * On timeout (finalize never called), the observer resolves with the latest captured URL using the same master-priority rule, or null if none arrived. The
 * underlying HLS observer is disposed automatically on resolution; callers do not hold CDP session references.
 *
 * @param page - The puppeteer page to monitor.
 * @param timeout - Maximum time in milliseconds to wait for a manifest. Acts as a safety net if finalize() is never called.
 * @returns The interceptor handle, or null if the observer could not be installed.
 */
export async function installManifestInterceptor(page: Page, timeout: number = INTERCEPTION_TIMEOUT): Promise<Nullable<ManifestInterceptorHandle>> {

  const elapsed = startTimer();

  // Track first/latest URLs separately per playlist kind. Master URLs take precedence at selection time because masters carry richer metadata. For direct tunes,
  // the first URL of the higher-priority kind wins; for guide tunes, the latest URL of the higher-priority kind wins. Without separate per-kind tracking, a
  // master-based site whose player happens to also load a media playlist for a different channel would risk picking the wrong one.
  let firstMasterUrl: Nullable<string> = null;
  let latestMasterUrl: Nullable<string> = null;
  let firstMediaUrl: Nullable<string> = null;
  let latestMediaUrl: Nullable<string> = null;
  let resolved = false;
  let manifestCount = 0;

  const { promise, resolve } = Promise.withResolvers<Nullable<ManifestInterceptionResult>>();

  const observer: Nullable<HlsPlaylistObserver> = await observeHlsPlaylists(page, {

    logCategory: "native:intercept",
    onPlaylist: (playlist: ObservedHlsPlaylist): void => {

      manifestCount++;

      if(playlist.kind === "master") {

        firstMasterUrl ??= playlist.url;
        latestMasterUrl = playlist.url;
      } else if(playlist.kind === "media") {

        firstMediaUrl ??= playlist.url;
        latestMediaUrl = playlist.url;
      }

      LOG.debug("native:intercept", "%s playlist captured (#%s) in %sms: %s.", playlist.kind, manifestCount, elapsed(), playlist.url.slice(0, 120));
    }
  });

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

  // Centralized resolution helper. Used by the timeout, finalize, and dispose paths to settle the promise exactly once and tear down the observer regardless of
  // which path won. The selectedUrl computation honors both the master-priority rule and the directTune first/latest semantics via selectInterceptedManifest.
  const settle = (directTune: boolean, reason: string): void => {

    if(resolved) {

      return;
    }

    resolved = true;
    clearTimeout(timer);

    const selectedUrl = selectUrl(directTune);

    if(selectedUrl) {

      LOG.debug("native:intercept", "Interception %s in %sms with %s capture(s). Using %s: %s.", reason, elapsed(), manifestCount,
        directTune ? "first" : "latest", selectedUrl.slice(0, 120));
      observer.dispose();
      resolve({ masterManifestUrl: selectedUrl });
    } else {

      LOG.debug("native:intercept", "Interception %s in %sms but no HLS playlist was captured.", reason, elapsed());
      observer.dispose();
      resolve(null);
    }
  };

  // Timeout guard. If finalize() is never called (defensive), settle with whatever we have after the timeout. The timeout path mirrors the latest-URL semantics
  // of a guide tune; if no finalize() ever arrived we err on the side of the most recent capture.
  const timer = setTimeout((): void => { settle(false, "timed out"); }, timeout);

  // Finalize function exposed on the returned handle. Called by the stream setup code after channel selection is complete. The resolution strategy depends on
  // two factors: whether a qualifying manifest has already been captured, and whether the tune is direct or guide-based.
  //
  // - Master manifest captured + direct tune: settle immediately (A&E, most TVE sites - manifest arrived during page load).
  // - Manifest captured + guide tune: wait FINALIZE_SETTLE_DELAY (Fox guide, Hulu - a newer manifest from the channel switch may still arrive).
  // - No manifest captured + either: wait FINALIZE_SETTLE_DELAY (Fox Sports - manifest fetch starts after video element appears, hasn't arrived yet).
  const finalize = (directTune: boolean): void => {

    if(resolved) {

      return;
    }

    // For direct tunes, settle immediately if a master URL has already arrived. We do not short-circuit on a media-only first URL because a master may still
    // be in flight - waiting the settle delay gives master priority a chance to take effect.
    if(directTune && firstMasterUrl) {

      settle(true, "finalized");
    } else {

      setTimeout((): void => { settle(directTune, "finalized"); }, FINALIZE_SETTLE_DELAY);
    }
  };

  // Explicit dispose path for "using" syntax or caller-driven cancellation. Idempotent. Resolves the promise with null if it has not already settled, so
  // callers awaiting the promise are never left hanging on a disposed observer.
  const dispose = (): void => {

    if(resolved) {

      return;
    }

    resolved = true;
    clearTimeout(timer);

    LOG.debug("native:intercept", "Interception disposed in %sms with %s capture(s) and no resolution selected.", elapsed(), manifestCount);
    observer.dispose();
    resolve(null);
  };

  return { dispose, finalize, promise, [Symbol.dispose]: dispose };
}

/**
 * Awaits the first master HLS manifest whose URL satisfies the supplied predicate. Used by tune-verification paths where the caller has just clicked or
 * navigated to a target channel and wants to confirm the resulting stream belongs to that channel rather than to whatever default the page was previously
 * showing.
 *
 * Resolves with the matching URL on success. Resolves with null if no matching manifest arrives within the timeout. The underlying observer is torn down
 * automatically in both cases - the verification path does not hand off a session to a downstream consumer.
 *
 * @param page - The puppeteer page to monitor.
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
  const observer: Nullable<HlsPlaylistObserver> = await observeHlsPlaylists(page, {

    logCategory: "native:intercept",
    onPlaylist: (playlist: ObservedHlsPlaylist): void => {

      if(resolved) {

        return;
      }

      if(playlist.kind !== "master") {

        LOG.debug("native:intercept", "Tune verification ignoring non-master playlist (%s).", playlist.kind);

        return;
      }

      if(predicate(playlist.url)) {

        resolved = true;

        LOG.debug("native:intercept", "Matching manifest found in %sms: %s.", elapsed(), playlist.url.slice(0, 120));
        observer?.dispose();
        clearTimeout(timer);
        resolve(playlist.url);
      } else {

        LOG.debug("native:intercept", "Master manifest did not match predicate: %s.", playlist.url.slice(0, 120));
      }
    }
  });

  if(!observer) {

    return null;
  }

  const timer = setTimeout((): void => {

    if(resolved) {

      return;
    }

    resolved = true;

    LOG.debug("native:intercept", "Awaiting matching manifest timed out after %sms.", elapsed());
    observer.dispose();
    resolve(null);
  }, timeout);

  return promise;
}
