/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hlsPlaylistObserver.ts: HLS-aware observer layered on top of the tab-wide network observer.
 */
import { LOG, chromeFetch } from "../utils/index.ts";
import type { HlsPlaylistKind } from "../native/probe.ts";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import type { TabNetworkObserver } from "./tabNetworkObserver.ts";
import { classifyHlsPlaylist } from "../native/probe.ts";
import { observeTabResponses } from "./tabNetworkObserver.ts";

/* This module is the HLS-aware layer on top of tabNetworkObserver. Its job is to deliver every recognized HLS playlist (URL + kind) observed anywhere in the tab
 * to a single callback. It encapsulates:
 *
 *   - URL filter: only `.m3u8` URLs (query string ignored) are eligible
 *   - Body fetch: Node-side fetch via chromeFetch (CDP's Network.getResponseBody is unreliable because Chrome's network cache can evict response bodies)
 *   - Classification: dispatch to classifyHlsPlaylist() to decide master vs media vs unknown
 *   - Per-URL deduplication: the same `.m3u8` URL fired multiple times during the observation window (typical: hls.js re-polling the chunklist every ~2s)
 *     produces exactly one body fetch and exactly one callback delivery
 *
 * It does not know about first vs latest URL selection, master priority, finalize semantics, or predicate-based verification - those are consumer concerns and
 * live in manifestInterceptor.ts (the long-lived interception path and the short-lived predicate-match path). This module is the single source of truth for
 * "given a tab, observe its HLS playlists"; any future feature that needs the same primitive (debug logging of every observed playlist, playlist content
 * archiving, etc.) consumes this module rather than re-implementing the filter + fetch + classify pipeline.
 *
 * Dedup rationale. During a typical interception window the hls.js player inside an OOPIF re-polls its chunklist multiple times. Without dedup we would fetch
 * and classify the chunklist body once per poll, wasting CPU and network. Dedup is applied at the URL gate (before fetch is initiated) so an in-flight fetch is
 * not duplicated by a near-simultaneous second observation. URLs are kept in the set across both successful and failed fetches: a manifest URL that failed to
 * fetch is unlikely to succeed on an immediate retry, and the interceptor timeout in the consumer is the safety net for genuinely missed manifests. A consumer
 * that needs different dedup semantics (e.g., refetch every N seconds) constructs its own observer rather than parameterizing this one.
 */

// Timeout for the per-response manifest body fetch. The body is fed to classifyHlsPlaylist() to determine whether it is a master playlist; if Chrome's network
// cache evicts the body or the CDN serves a slow response, we abandon and treat the response as unclassifiable.
const MANIFEST_BODY_FETCH_TIMEOUT = 5000;

/**
 * A recognized HLS playlist observed by the tab. Carries the URL the player loaded and the classification result (master multivariant playlist or media segment
 * playlist). Unknown bodies (no `#EXT-X-STREAM-INF` and no `#EXTINF`/`#EXT-X-TARGETDURATION`) are not delivered.
 */
export interface ObservedHlsPlaylist {

  // Master multivariant playlist (`#EXT-X-STREAM-INF` present) or media segment playlist (`#EXTINF` / `#EXT-X-TARGETDURATION` present).
  readonly kind: HlsPlaylistKind;

  // Absolute URL of the HLS playlist.
  readonly url: string;
}

/**
 * Options accepted by observeHlsPlaylists(). The callback fires once per unique HLS playlist URL observed during the observer's lifetime.
 */
export interface HlsPlaylistObserverOptions {

  // Debug log category for the observer's lifecycle messages. Pass-through to LOG.debug so consumers can route messages through their own scope (e.g.,
  // "native:intercept" for the long-lived interception, "tune:verify" for predicate verification).
  readonly logCategory: string;

  // Invoked once per unique recognized HLS playlist URL observed. The callback is synchronous; the observer awaits no return value.
  readonly onPlaylist: (playlist: ObservedHlsPlaylist) => void;
}

/**
 * Handle returned by observeHlsPlaylists(). Implements both the project's dispose() convention and TC39 Symbol.dispose so callers may use either an explicit
 * dispose() call or the "using" keyword. Disposal is idempotent and tears down the underlying tab network observer.
 */
export interface HlsPlaylistObserver extends Disposable {

  // Releases the underlying tab network observer and stops further classification work. Safe to call multiple times; subsequent calls are no-ops.
  readonly dispose: () => void;

  // TC39 explicit resource management hook. Aliases dispose() so "using observer = ..." produces deterministic teardown at scope exit, including on thrown errors.
  readonly [Symbol.dispose]: () => void;
}

/**
 * Installs an HLS playlist observer on the given page. The callback fires once per unique recognized HLS playlist URL observed across the tab's entire target
 * tree (top frame, iframes including OOPIFs, workers, service workers). The OOPIF-aware transport is provided by tabNetworkObserver underneath; this observer
 * adds the HLS-specific filter, body fetch, classification dispatch, and per-URL dedup.
 *
 * Returns null when the underlying tab network observer could not be installed (page closed, root CDP session creation failed).
 *
 * @param page - The puppeteer page to observe.
 * @param options - Observer options including the playlist callback and the debug log category.
 * @returns The observer handle, or null if installation failed.
 */
export async function observeHlsPlaylists(page: Page, options: HlsPlaylistObserverOptions): Promise<Nullable<HlsPlaylistObserver>> {

  const { logCategory, onPlaylist } = options;

  let disposed = false;

  // Per-URL dedup state. We add a URL to seenUrls the moment we decide to fetch it - before the asynchronous chromeFetch resolves - so a second observation of
  // the same URL arriving while the first fetch is in flight does not initiate a duplicate fetch. URLs remain in the set for the observer's lifetime; the
  // consumer's interception window timeout bounds the memory footprint.
  const seenUrls = new Set<string>();

  // Handles a single response delivered by the tab network observer. Filters to .m3u8 URLs, applies per-URL dedup, fetches the body for classification, and
  // dispatches the kind to the consumer callback. Wrapped in try/catch so a body-fetch failure on one URL does not interfere with subsequent responses arriving
  // on other URLs.
  const handleResponse = async (url: string): Promise<void> => {

    if(disposed) {

      return;
    }

    // Strip query parameters before checking the extension. Many CDNs append cache-busting or tokenization query strings to .m3u8 URLs.
    const urlPath = url.split("?")[0] ?? "";

    if(!urlPath.endsWith(".m3u8")) {

      return;
    }

    // Dedup gate. We use the full URL (with query string) as the dedup key so two different tokenized URLs to the same playlist do not collapse. The chunklist
    // poll case - where hls.js fires the same exact URL repeatedly - is the workload this optimization targets.
    if(seenUrls.has(url)) {

      return;
    }

    seenUrls.add(url);

    LOG.debug(logCategory, "Observed .m3u8 response: %s.", url.slice(0, 120));

    try {

      const response = await chromeFetch(url, { signal: AbortSignal.timeout(MANIFEST_BODY_FETCH_TIMEOUT) });

      if(!response.ok) {

        LOG.debug(logCategory, "Manifest fetch returned HTTP %s for %s.", response.status, url.slice(0, 120));

        return;
      }

      const body = await response.text();

      // Delegate the master/media decision to the canonical classifier. Recognized kinds (master, media) are forwarded to the callback together with the kind
      // label; consumers apply their own filter. Both consumers (installManifestInterceptor and awaitMatchingManifest) maintain their own resolved-flag guards,
      // but we still gate locally on the disposed flag so a callback that arrives after dispose is never invoked even if the consumer guard were absent.
      const kind = classifyHlsPlaylist(body);

      if(kind === "unknown") {

        LOG.debug(logCategory, "Skipping .m3u8 classified as unknown (no recognizable HLS directives).");

        return;
      }

      // The disposed flag may have flipped during the awaits above. Closure-captured mutation across awaits is invisible to ESLint's flow analysis (which is why
      // the next line carries an explicit disable), but it is observable at runtime and matters for race-safety: we never deliver a callback after dispose, full
      // stop. This guard lives here because race safety is the observer's responsibility, not the consumer's - consumers should not have to repeat it.
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      if(disposed) {

        return;
      }

      onPlaylist({ kind, url });
    } catch(error) {

      LOG.debug(logCategory, "Could not fetch .m3u8 body: %s.", String(error));
    }
  };

  const tabObserver: Nullable<TabNetworkObserver> = await observeTabResponses(page, {

    onResponse: (response): void => { void handleResponse(response.url); }
  });

  if(!tabObserver) {

    return null;
  }

  LOG.debug(logCategory, "HLS playlist observer installed.");

  const dispose = (): void => {

    if(disposed) {

      return;
    }

    disposed = true;
    tabObserver.dispose();
    seenUrls.clear();
  };

  return { dispose, [Symbol.dispose]: dispose };
}
