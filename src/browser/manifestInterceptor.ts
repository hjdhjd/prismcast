/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * manifestInterceptor.ts: Selection state machines for HLS manifest interception, layered on top of the HLS playlist observer.
 */
import type { HlsPlaylistObserver, ObservedHlsPlaylist } from "./hlsPlaylistObserver.ts";
import { LOG, extractPathname, startTimer } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import type { RecognizedHlsPlaylistKind } from "../native/probe.ts";
import { observeHlsPlaylists } from "./hlsPlaylistObserver.ts";

/* This module hosts two consumer-facing state machines built on top of the HLS playlist observer (hlsPlaylistObserver.ts), which in turn rides on the tab-wide
 * network observer (tabNetworkObserver.ts). The layering:
 *
 *   tabNetworkObserver  -> tab-wide CDP transport (OOPIF-aware via auto-attach + flatten)
 *   hlsPlaylistObserver -> HLS filter + body fetch + classify + per-URL dedup + wire-arrival sequencing
 *   manifestInterceptor -> selection: the channel-selection epoch, playlist membership, the liveness override, finalize semantics, predicate match
 *
 * This module hosts the following consumer-facing state machines built on top of the HLS playlist observer:
 *
 * - installManifestInterceptor(): the long-lived capture used by native HLS setup. Accepts both master and media playlists, tracks the first and latest URL per
 *   kind, and resolves with the playlist appropriate for the tune type when finalize() is called. A direct tune (single-channel site navigated by URL) takes the
 *   first qualifying capture, master preferred. A guide tune (multi-channel site) judges the captures through three signals - a channel-selection epoch stamped
 *   when selection begins, playlist membership of a media playlist within the latest master's declared children, and media liveness - so the tuned channel's
 *   playlist is chosen even when a page-load default channel was captured first. See selectInterceptedManifest for the full rule.
 * - awaitMatchingManifest(): the short-lived verification used after a guide-click tune. Accepts master playlists only and resolves with the first URL whose
 *   path satisfies a caller-supplied predicate. Media playlists are skipped at the consumer level because tune verification is a multi-channel concept that
 *   does not apply to direct-tune media-only sources.
 *
 * Race safety, URL filtering, body fetching, classification, and wire-arrival sequencing all live below this module - we receive a stream of recognized HLS
 * playlists with their kind, per-kind facts, and arrival ordinal, and apply selection policy. Public callers do not hold CDP session references; the underlying
 * HLS observer disposes its tab network observer deterministically when finalize, timeout, predicate match, or explicit dispose fires.
 */

// Brief delay after finalize() is called to allow any in-flight manifest response to arrive. Applied for guide-based tunes (directTune=false), where the channel
// switch may trigger a manifest fetch that arrives milliseconds after the click handler returns, and for direct tunes that have not yet captured a master
// manifest. A direct tune skips this delay and resolves immediately only once a master manifest has already arrived. Exported so a caller sizing its own
// interception window (streaming/setup.ts) can include the settle window the window must outlive.
export const FINALIZE_SETTLE_DELAY = 1500;

// Default timeout for awaitMatchingManifest. Tune verification is a short-lived check after a click, so the budget is tighter than the native interception path.
const VERIFICATION_TIMEOUT = 8000;

/**
 * A master playlist observation reduced to the facts selection needs: the declared child playlist URLs (for membership judgment), the wire-arrival ordinal, and
 * the URL. Modeled as a record so a URL can never exist without its ordinal - the null-consistency question separate parallel locals would raise is
 * unrepresentable.
 */
export interface InterceptedMasterFact {

  // The master's declared child playlist URIs resolved to absolute URLs (variant streams and media renditions both), used to judge whether a media playlist is
  // one of this master's own children.
  readonly childUrls: readonly string[];

  // The wire-arrival ordinal of this observation, from the observer's sequence counter.
  readonly ordinal: number;

  // Absolute URL of the master playlist.
  readonly url: string;
}

/**
 * A media playlist observation reduced to the facts selection needs: its liveness, the wire-arrival ordinal, and the URL. Modeled as a record so a URL can never
 * exist without its ordinal.
 */
export interface InterceptedMediaFact {

  // True when the media playlist may still grow (no ENDLIST marker, not VOD-typed). Only a live media playlist may override a master.
  readonly live: boolean;

  // The wire-arrival ordinal of this observation, from the observer's sequence counter.
  readonly ordinal: number;

  // Absolute URL of the media playlist.
  readonly url: string;
}

/**
 * Captured state used by selectInterceptedManifest() to choose the playlist appropriate for a given resolution mode. Modeled as a plain record so the selection
 * logic is testable in isolation from the network plumbing that maintains the underlying state. Every judgment - including the pathname normalization membership
 * relies on - lives in the selection function; this record carries only facts.
 */
export interface InterceptedManifestState {

  // True when the resolution should pick the first URL captured (direct tune); false when it should judge the latest captures through the guide-tune rule.
  readonly directTune: boolean;

  // The first observed master playlist URL, or null when no master playlist arrived during the interception window.
  readonly firstMasterUrl: Nullable<string>;

  // The first observed media playlist URL, or null when no media playlist arrived during the interception window.
  readonly firstMediaUrl: Nullable<string>;

  // The most recently observed master playlist and its facts, or null when no master playlist arrived during the interception window.
  readonly latestMaster: Nullable<InterceptedMasterFact>;

  // The most recently observed media playlist and its facts, or null when no media playlist arrived during the interception window.
  readonly latestMedia: Nullable<InterceptedMediaFact>;

  // The wire-arrival ordinal fenced when channel selection began, or null when no epoch was declared (a direct tune, or the defensive timeout). Distinct from 0,
  // which is a declared epoch stamped before any observation arrived - so this is never defaulted with ??, since null and 0 carry different meanings.
  readonly markOrdinal: Nullable<number>;
}

/**
 * The outcome of selectInterceptedManifest(): the chosen URL and the kind selection decided it is. The kind is reported alongside the URL so no caller re-derives
 * it by comparing URLs, which would be fragile when the same URL could sit in both the master and media records.
 */
export interface SelectedManifest {

  // The kind of the selected playlist, decided by selection rather than re-derived downstream.
  readonly kind: RecognizedHlsPlaylistKind;

  // The selected playlist URL.
  readonly url: string;
}

/**
 * Reports whether a media playlist is a verified NON-member of a master's declared children, comparing by pathname only. Membership is URL-identity policy, not
 * manifest-format knowledge, which is why it lives here rather than in probe. Pathname-only because post-redirect response URLs diverge from a master's declared
 * child URIs at the host far more often than at the path. The judgment is member-conservative: a media URL that cannot be parsed, an empty declared child list, or
 * an empty effective set after dropping unparseable children all read as MEMBER (the master wins, degrading to the categorical rule, never a new failure). Only a
 * parseable media pathname absent from a non-empty set of parseable child pathnames is a verified non-member.
 *
 * @param mediaUrl - The candidate media playlist URL.
 * @param childUrls - The master's declared child playlist URLs.
 * @returns True only when the media pathname is proven absent from a non-empty effective set of child pathnames.
 */
function isVerifiedNonMemberOfMaster(mediaUrl: string, childUrls: readonly string[]): boolean {

  const mediaPathname = extractPathname(mediaUrl);

  // A media URL we cannot parse cannot be proven foreign, so it reads as a member and the master wins.
  if(mediaPathname === null) {

    return false;
  }

  // Normalize the declared children to pathnames, dropping any that cannot be parsed. An empty declared list or a list that parses to nothing yields an empty
  // effective set: with no children to compare against, membership cannot be disproven, so the master wins.
  const childPathnames = childUrls.map((child) => extractPathname(child)).filter((pathname): pathname is string => pathname !== null);

  if(childPathnames.length === 0) {

    return false;
  }

  return !childPathnames.includes(mediaPathname);
}

/**
 * Selects the playlist appropriate for a given resolution mode from the captured state, reporting the URL together with the kind selection decided.
 *
 * Direct tunes take the earliest qualifying capture (firstMasterUrl before firstMediaUrl), master preferred, with the kind tagged by the slot that supplied the
 * URL. The epoch never participates on the direct branch: a direct tune navigates by URL and its first capture answers.
 *
 * Guide tunes apply a three-signal rule over the latest captures:
 *
 *   1. No master captured: the media fallback (latestMedia's URL) or null. Liveness gates only the override of a master, never the sole available feed, so a VOD
 *      media playlist still answers when it is all that arrived.
 *   2. Epoch declared and the master post-epoch (latestMaster.ordinal > markOrdinal): the master, categorically. A master observed after channel selection began
 *      answers the click. An absent epoch (mark never called) also leaves the master the winner, because without a click there is no fresher truth to prefer.
 *   3. Override to the media only when every clause holds: an epoch exists (rule 2 falling through means the master is pre-epoch), latestMedia is live, the media
 *      is post-epoch (latestMedia.ordinal > markOrdinal - which structurally postdates the pre-epoch master, so no separate ordering clause is needed), and the
 *      media is a verified non-member of the master's children. Otherwise the master wins.
 *
 * Membership is member-conservative (see isVerifiedNonMemberOfMaster): anything short of a proven foreign pathname keeps the master, so the override engages only
 * where a media playlist is demonstrably not one of the master's own children. Pure selection policy exported so the rule is testable in isolation from the
 * network plumbing; the closure inside installManifestInterceptor() consumes it through the same shape.
 *
 * @param state - The captured state at resolution time.
 * @returns The selected playlist, or null when no URL matches the resolution mode.
 */
export function selectInterceptedManifest(state: InterceptedManifestState): Nullable<SelectedManifest> {

  if(state.directTune) {

    if(state.firstMasterUrl !== null) {

      return { kind: "master", url: state.firstMasterUrl };
    }

    if(state.firstMediaUrl !== null) {

      return { kind: "media", url: state.firstMediaUrl };
    }

    return null;
  }

  const { latestMaster, latestMedia, markOrdinal } = state;

  // Rule 1: no master captured. The media fallback answers regardless of liveness.
  if(latestMaster === null) {

    return (latestMedia === null) ? null : { kind: "media", url: latestMedia.url };
  }

  // Rule 2: a master observed after channel selection began answers the click categorically; an absent epoch keeps the master too.
  if((markOrdinal !== null) && (latestMaster.ordinal > markOrdinal)) {

    return { kind: "master", url: latestMaster.url };
  }

  // Rule 3: override to a live, post-epoch, verified non-member media; otherwise the master wins.
  if((markOrdinal !== null) && (latestMedia !== null) && latestMedia.live && (latestMedia.ordinal > markOrdinal) &&
    isVerifiedNonMemberOfMaster(latestMedia.url, latestMaster.childUrls)) {

    return { kind: "media", url: latestMedia.url };
  }

  return { kind: "master", url: latestMaster.url };
}

/**
 * Result of manifest interception. The URL may be either a master or a media playlist; selectedKind reports which selection chose, and the probe normalizes both
 * kinds into a MediaFeed so downstream code does not branch on which kind originally arrived. The underlying observer and its CDP sessions are owned and disposed
 * internally - callers do not hold session references.
 */
export interface ManifestInterceptionResult {

  // The HLS playlist URL selected from the browser's network requests. For direct tunes, the first qualifying URL captured; for guide tunes, the playlist chosen
  // by the epoch, membership, and liveness rule (see selectInterceptedManifest).
  readonly manifestUrl: string;

  // The kind selection decided the URL is, so no consumer re-derives it by comparing URLs.
  readonly selectedKind: RecognizedHlsPlaylistKind;
}

/**
 * Handle returned by installManifestInterceptor. Provides the interception promise, a finalize function to signal that channel selection is complete, an epoch
 * stamp marking when channel selection begins, and an explicit dispose path. Implements TC39 Symbol.dispose so callers may use the "using" keyword for
 * scope-bound cleanup, including on thrown errors.
 */
export interface ManifestInterceptorHandle extends Disposable {

  // Cancels the interception. Resolves the promise with null if it has not already settled. Safe to call multiple times. Aliased to [Symbol.dispose] so the
  // handle can be used with "using" syntax for scope-bound cleanup, including on thrown errors.
  readonly dispose: () => void;

  // Signals that channel selection is complete. When directTune is true (single-channel site navigated by URL) and a master manifest has already arrived,
  // resolves immediately with the first captured manifest - the one loaded for the navigated URL. When directTune is true but only a media-only manifest, or
  // nothing, has arrived so far, it waits the settle delay so a still-in-flight master can be preferred. When false (guide-based multi-channel site), applies a
  // brief settle delay and resolves through the guide-tune rule - the epoch, membership, and liveness signals over the latest captures.
  readonly finalize: (directTune: boolean) => void;

  // Stamps the observation epoch at which channel selection begins by fencing the observer's current wire sequence. Observations after the stamp answer the
  // click, and guide-tune selection prefers a master observed after this point. Callable again - the latest stamp wins - and consulted only by guide-tune
  // selection (a direct tune and the defensive timeout resolve without it). A no-op once the interception has resolved.
  readonly markChannelSelectionStart: () => void;

  // Promise that resolves with the interception result after finalize() is called (or after the timeout expires, whichever comes first). Resolves with null
  // when no qualifying playlist was captured.
  readonly promise: Promise<Nullable<ManifestInterceptionResult>>;

  // TC39 explicit resource management hook. Aliases dispose() so "using interceptor = ..." produces deterministic teardown at scope exit.
  readonly [Symbol.dispose]: () => void;
}

/**
 * A first-slot tracker: the earliest-arriving playlist URL of a kind tagged with its wire ordinal, so a late delivery of an even-earlier arrival can correct the
 * slot. Symmetric with the latest-fact records but without the per-kind extras, since the direct branch reads only the URL.
 */
interface FirstObservation {

  readonly ordinal: number;

  readonly url: string;
}

/**
 * Options for the internal settle helper. Grouped into one object because a third positional boolean beside directTune would be ambiguous at the call sites.
 */
interface SettleOptions {

  // True when the resolution should pick the first URL captured (direct tune); false for the guide-tune rule.
  readonly directTune: boolean;

  // When true, resolution forces the epoch to null so the guide-tune rule falls back to its categorical, master-preferred outcome. Only the defensive timeout
  // passes this, because it cannot know the tune kind and the epoch rule must never engage on a path a direct tune can reach. Defaults false.
  readonly epochFree?: boolean;

  // Human-readable reason for the settle, used only in the log line.
  readonly reason: string;
}

/**
 * Installs a long-lived HLS playlist observer that tracks observed playlists on the given page. Both master and media playlists are accepted; the first and latest
 * URL per kind are tracked separately, guarded by wire-arrival order, so master-based and media-only sites both work without separate code paths. The returned
 * handle provides a finalize(directTune) callback and a markChannelSelectionStart() epoch stamp; when finalize is called the observer resolves through
 * selectInterceptedManifest with the playlist appropriate for the tune type:
 *
 * - directTune=true: resolves immediately if a master manifest has already arrived (or after the settle delay when only a media-only manifest, or nothing, has
 *   arrived so far) with the first manifest captured. Used by sites where the navigated URL itself selects the channel and the player loads its manifest before
 *   the click handler returns. Master-first URL preferred over media-first.
 * - directTune=false: resolves after the settle delay through the guide-tune rule (channel-selection epoch, playlist membership, liveness), so the tuned
 *   channel's playlist wins over a page-load default channel captured first. Used by guide-based sites where the channel-switch click triggers a new manifest
 *   fetch that may arrive milliseconds after the click handler returns.
 *
 * On timeout (finalize never called), the observer resolves epoch-free through the same guide-tune rule with no epoch declared, so the categorical
 * master-preferred outcome holds and a direct tune the timeout may be guarding is never exposed to the epoch rule. The underlying HLS observer is disposed
 * automatically on resolution; callers do not hold CDP session references.
 *
 * @param page - The puppeteer page to monitor.
 * @param timeout - Maximum time in milliseconds to wait for a manifest. Required: every caller sizes its interception window consciously to outlive the phases it
 *   observes, so no production caller relies on a shared default. Acts as a safety net if finalize() is never called.
 * @param observeFactory - The HLS playlist observer factory (default observeHlsPlaylists); injectable so the selection state machine can run without a live browser.
 * @returns The interceptor handle, or null if the observer could not be installed.
 */
export async function installManifestInterceptor(page: Page, timeout: number,
  observeFactory: typeof observeHlsPlaylists = observeHlsPlaylists): Promise<Nullable<ManifestInterceptorHandle>> {

  const elapsed = startTimer();

  // Track the first and latest observation per kind, plus the epoch fence. Slot updates are guarded by wire-arrival order, not delivery order: the first slot
  // holds the lowest-ordinal arrival and the latest facts hold the highest, so a body fetch that resolves out of order still records arrival truth. Under
  // in-order delivery this is byte-for-byte the earliest/most-recent behavior. manifestCount stays purely diagnostic for the log lines.
  let firstMaster: Nullable<FirstObservation> = null;
  let firstMedia: Nullable<FirstObservation> = null;
  let latestMaster: Nullable<InterceptedMasterFact> = null;
  let latestMedia: Nullable<InterceptedMediaFact> = null;
  let markOrdinal: Nullable<number> = null;
  let resolved = false;
  let manifestCount = 0;

  const { promise, resolve } = Promise.withResolvers<Nullable<ManifestInterceptionResult>>();

  const observer: Nullable<HlsPlaylistObserver> = await observeFactory(page, {

    logCategory: "native:intercept",
    onPlaylist: (playlist: ObservedHlsPlaylist): void => {

      manifestCount++;

      if(playlist.kind === "master") {

        // First slot: the lowest ordinal wins, so a late delivery of an even-earlier arrival corrects it.
        if((firstMaster === null) || (playlist.sequence < firstMaster.ordinal)) {

          firstMaster = { ordinal: playlist.sequence, url: playlist.url };
        }

        // Latest slot: the highest ordinal wins, so a late delivery of a lower arrival never displaces it.
        if((latestMaster === null) || (playlist.sequence > latestMaster.ordinal)) {

          latestMaster = { childUrls: playlist.childUrls, ordinal: playlist.sequence, url: playlist.url };
        }
      } else {

        if((firstMedia === null) || (playlist.sequence < firstMedia.ordinal)) {

          firstMedia = { ordinal: playlist.sequence, url: playlist.url };
        }

        if((latestMedia === null) || (playlist.sequence > latestMedia.ordinal)) {

          latestMedia = { live: playlist.live, ordinal: playlist.sequence, url: playlist.url };
        }
      }

      LOG.debug("native:intercept", "%s playlist captured (#%s, seq %s) in %sms: %s.", playlist.kind, manifestCount, playlist.sequence, elapsed(),
        playlist.url.slice(0, 120));
    }
  });

  if(!observer) {

    return null;
  }

  // Builds the selection state from the closure locals. epochFree forces markOrdinal to null so the timeout path resolves without the epoch rule, which must
  // never engage on a path a direct tune can reach.
  const buildState = (directTune: boolean, epochFree: boolean): InterceptedManifestState => ({

    directTune,
    firstMasterUrl: firstMaster?.url ?? null,
    firstMediaUrl: firstMedia?.url ?? null,
    latestMaster,
    latestMedia,
    markOrdinal: epochFree ? null : markOrdinal
  });

  // Centralized resolution helper. Used by the timeout, finalize, and dispose paths to settle the promise exactly once and tear down the observer regardless of
  // which path won. The selection honors the direct-vs-guide semantics and, for guide tunes, the epoch/membership/liveness rule via selectInterceptedManifest.
  const settle = (options: SettleOptions): void => {

    const { directTune, epochFree = false, reason } = options;

    if(resolved) {

      return;
    }

    resolved = true;
    clearTimeout(timer);

    const selection = selectInterceptedManifest(buildState(directTune, epochFree));

    if(selection) {

      LOG.debug("native:intercept", "Interception %s in %sms with %s capture(s). Using %s %s: %s.", reason, elapsed(), manifestCount,
        directTune ? "first" : "latest", selection.kind, selection.url.slice(0, 120));
      observer.dispose();
      resolve({ manifestUrl: selection.url, selectedKind: selection.kind });
    } else {

      LOG.debug("native:intercept", "Interception %s in %sms but no HLS playlist was captured.", reason, elapsed());
      observer.dispose();
      resolve(null);
    }
  };

  // Timeout guard. If finalize() is never called (defensive), settle epoch-free after the timeout: the timeout cannot know the tune kind, and the epoch rule must
  // never engage on a path a direct tune can reach, so it resolves with the categorical master-preferred outcome. With the interception budget sized by the
  // caller to outlive the tune (streaming/setup.ts), this path is last-resort defense rather than a routine finish.
  const timer = setTimeout((): void => { settle({ directTune: false, epochFree: true, reason: "timed out" }); }, timeout);

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
    // be in flight - waiting the settle delay gives the master-first preference a chance to take effect.
    if(directTune && (firstMaster !== null)) {

      settle({ directTune: true, reason: "finalized" });
    } else {

      setTimeout((): void => { settle({ directTune, reason: "finalized" }); }, FINALIZE_SETTLE_DELAY);
    }
  };

  // Stamps the observation epoch at which channel selection begins by fencing the observer's current wire sequence. Observations after the stamp answer the
  // click; guide-tune selection prefers a master observed after this point. Callable again - the latest stamp wins - and a no-op once the interception has
  // resolved, matching the finalize and dispose guard idiom.
  const markChannelSelectionStart = (): void => {

    if(resolved) {

      return;
    }

    markOrdinal = observer.currentSequence();

    LOG.debug("native:intercept", "Channel selection epoch stamped at wire sequence %s.", markOrdinal);
  };

  // Explicit dispose path for "using" syntax or caller-driven cancellation. Safe to call more than once. Resolves the promise with null if it has not already
  // settled, so callers awaiting the promise are never left hanging on a disposed observer.
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

  return { dispose, finalize, markChannelSelectionStart, promise, [Symbol.dispose]: dispose };
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
 * @param observeFactory - The HLS playlist observer factory (default observeHlsPlaylists); injectable so the verification state machine can run without a live browser.
 * @returns The matching URL, or null on timeout.
 */
export async function awaitMatchingManifest(page: Page, predicate: (url: string) => boolean,
  timeout: number = VERIFICATION_TIMEOUT, observeFactory: typeof observeHlsPlaylists = observeHlsPlaylists): Promise<Nullable<string>> {

  const elapsed = startTimer();
  let resolved = false;

  const { promise, resolve } = Promise.withResolvers<Nullable<string>>();

  // Tune verification is a multi-channel concept that only applies to master playlists - direct-tune media-only sources do not run a guide-click verification
  // step because the page navigation itself selects the channel. We reject media playlists at the consumer level rather than asking the observer to filter so
  // the observer keeps a single canonical contract (forward all recognized HLS playlists with their kind).
  const observer: Nullable<HlsPlaylistObserver> = await observeFactory(page, {

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
