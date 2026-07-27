/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Coordinator for native HLS streaming - manifest interception, DRM probe, and proxy lifecycle.
 */
import { LOG, cancellableTimeout, formatError, startTimer } from "../utils/index.ts";
import { clearProbeCache, probeManifest } from "./probe.ts";
import type { CaptureCodec } from "../streaming/codec.ts";
import type { ManifestInterceptionResult } from "../browser/manifestInterceptor.ts";
import type { MediaFeed } from "./probe.ts";
import type { NativeProxy } from "./proxy.ts";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import { createNativeProxy } from "./proxy.ts";
import { fetchDecryptionKey } from "./decrypt.ts";
import { installManifestInterceptor } from "../browser/manifestInterceptor.ts";
import { parseTokenExpiry } from "./tokenExpiry.ts";

/* This module orchestrates the native streaming decision. After the browser navigates to a channel and video playback begins, we check whether the service's HLS
 * stream can be consumed directly in Node (bypassing screen capture). The decision flow is:
 *
 * 1. Await the manifest interception promise (CDP listener was installed before navigation)
 * 2. If no manifest was intercepted: return null (fall back to capture)
 * 3. Probe the manifest for encryption type
 * 4. If DRM: log the reason, return null
 * 5. If the client is an MPEG-TS consumer and the stream has a separate audio rendition: return null (incompatible PAT/PMT, fall back to capture)
 * 6. If AES-128: pre-fetch the decryption key, returning null if it is inaccessible
 * 7. If clear or AES-128 (with key in hand): create and return the native proxy
 *
 * Token refresh: When the intercepted URL contains expiration tokens, we schedule a SINGLE timer aimed at the next expiry boundary - the earlier of the master URL's
 * and the polled variant URL's expirations, minus a comfortable margin. The refresh first attempts a direct Node.js fetch of the master manifest URL (no browser
 * involvement), falling back to a browser page reload when the master URL itself has expired. Each refresh reschedules from the boundary it is aiming at, not from a
 * shrinking-but-unchanging master expiry, so the schedule never degenerates into a per-cycle re-probe loop in the final minutes before expiry. The proxy
 * continues serving cached segments during the refresh.
 */

// Time in milliseconds before token expiry to trigger a refresh. We refresh 5 minutes early to ensure the new manifest is ready before the old one expires.
const TOKEN_REFRESH_MARGIN = 300000;

// Minimum delay in milliseconds before a token refresh fires. This is the absolute floor for any scheduled refresh: when a boundary lands at or in the past (an
// already-expired or imminently-expiring token), we still wait at least this long so the timer cannot fire back-to-back and thrash. It is NOT the steady-state
// cadence - inside the margin window we schedule a single refresh aimed at the actual expiry boundary, not a repeating MIN_REFRESH_DELAY poll (see scheduleTokenRefresh).
const MIN_REFRESH_DELAY = 30000;

// Minimum remaining token lifetime (in milliseconds) for a direct-fetched variant URL to be considered usable. If the variant URL's token expires sooner than this,
// the direct fetch result is discarded and we fall back to a page reload to get a genuinely fresh token. This prevents handing the proxy a variant URL that expires
// almost immediately. Set low (5s) because the proxy only needs the variant URL to survive one poll cycle (~3s). A higher threshold (e.g., 30s) would cause Fox.com
// channels to reject perfectly usable variant URLs - Fox.com tokens have ~57s total lifetime, leaving ~27s at the 30s refresh point.
const MIN_USABLE_TOKEN_LIFETIME = 5000;

// Timeout for awaiting the manifest interception promise after playback init.
const INTERCEPTION_AWAIT_TIMEOUT = 5000;

/**
 * Options for attempting native streaming.
 */
export interface AttemptNativeStreamingOptions {

  // The channel name for logging and cache keys.
  channelName: string;

  // The manifest interception promise from the CDP listener installed before navigation.
  interceptionPromise: Promise<Nullable<ManifestInterceptionResult>>;

  // When true, the requesting client is an MPEG-TS consumer. Channels with separate audio renditions are not viable for MPEG-TS clients because the independent
  // video and audio MPEG-TS segments have incompatible PAT/PMT tables from ad splicing. These channels fall back to capture mode for MPEG-TS but use native
  // streaming for HLS clients.
  mpegTsClient?: boolean;

  // Callback invoked on native proxy errors for recovery orchestration.
  onError: (error: string) => void;

  // The browser page (kept alive for token refresh).
  page: Page;

  // The preroll codec variant for composite playlist construction.
  prerollCodec?: CaptureCodec;

  // Number of preroll segments preceding real content. When non-zero, the proxy starts segment numbering after the preroll range to reserve the index space. The
  // composite playlist reads the base URL dynamically from the stream's HLS state.
  prerollSegmentCount?: number;

  // Numeric stream ID for segment storage.
  streamId: number;

  // String stream ID for logging.
  streamIdStr: string;

  // The channel URL for page reload during token refresh.
  url: string;
}

/**
 * Result of a successful native streaming attempt.
 */
export interface NativeStreamResult {

  // Declared bandwidth of the selected variant in bits per second. Zero when the BANDWIDTH attribute is absent or unparseable.
  bandwidth: number;

  // Video codec label (e.g., "H264", "HEVC", "AV1"), or null when the CODECS attribute is absent or unrecognized.
  codec: Nullable<string>;

  // Whether the stream has separate audio renditions. Set once at stream creation on HLSState.hasAudio so the HLS handler knows to serve variant playlists.
  hasAudio: boolean;

  // The native proxy that fetches and stores segments. The proxy holds no CDP session references - session ownership lives entirely inside the manifest
  // interceptor's tab network observer, which disposes itself when interception completes (finalize, timeout, predicate match, or explicit dispose).
  proxy: NativeProxy;

  // Video resolution from the master manifest (e.g., "1920x1080"), or null when absent.
  resolution: Nullable<string>;
}

/**
 * Attempts to upgrade a stream from screen capture to native HLS streaming. Returns the native proxy on success, or null if the stream is not viable for native
 * consumption (DRM, interception timeout, or probe failure).
 *
 * @param options - Options for the native streaming attempt.
 * @returns The native stream result on success, or null to fall back to capture.
 */
export async function attemptNativeStreaming(options: AttemptNativeStreamingOptions): Promise<Nullable<NativeStreamResult>> {

  const { channelName, interceptionPromise, mpegTsClient, onError, page, streamId, streamIdStr, url } = options;

  const elapsed = startTimer();

  LOG.debug("native:coordinator", "Attempting native streaming for %s.", channelName);

  // Await the manifest interception with a short timeout. The CDP listener was installed before navigation, so by the time we get here the manifest should
  // already be captured or close to it. cancellableTimeout owns the underlying setTimeout so we clear it in finally when interceptionPromise wins the race;
  // otherwise the ref'd timer would hold the event loop for up to INTERCEPTION_AWAIT_TIMEOUT after a successful tune.
  let interception: Nullable<ManifestInterceptionResult>;
  const timeout = cancellableTimeout(INTERCEPTION_AWAIT_TIMEOUT);

  try {

    const result = await Promise.race([ interceptionPromise, timeout.promise ]);

    interception = (result === false) ? null : result;
  } catch(error) {

    LOG.debug("native:coordinator", "Manifest interception error for %s: %s.", channelName, formatError(error));

    return null;
  } finally {

    timeout.cancel();
  }

  if(!interception) {

    LOG.debug("native:coordinator", "No manifest intercepted for %s in %sms. Falling back to capture.", channelName, elapsed());

    return null;
  }

  LOG.debug("native:coordinator", "Manifest intercepted for %s in %sms.", channelName, elapsed());

  // Probe the intercepted URL and normalize the result to a MediaFeed. The probe handles both master and media playlists transparently; this code path does not
  // need to know which kind arrived.
  const mediaFeed = await probeManifest(interception.masterManifestUrl, channelName);

  if(!mediaFeed) {

    LOG.debug("native:coordinator", "Probe failed for %s. Falling back to capture.", channelName);

    return null;
  }

  if(mediaFeed.encryption === "drm") {

    LOG.debug("native:coordinator", "Native streaming not viable for %s: DRM-protected stream.", channelName);

    return null;
  }

  // Separate audio renditions (e.g., Google DAI on BET/VH1) cannot be served to MPEG-TS clients because the independent video and audio MPEG-TS segments have
  // incompatible PAT/PMT tables and variable packet sizes from ad splicing. HLS clients (Channels DVR) handle separate audio renditions natively via master playlist.
  if(mpegTsClient && mediaFeed.audioVariantUrl) {

    LOG.debug("native:coordinator", "Native streaming not viable for %s: separate audio rendition incompatible with MPEG-TS clients.", channelName);

    return null;
  }

  LOG.debug("native:coordinator", "Native streaming viable for %s (%s, variant: %s).", channelName, mediaFeed.encryption, mediaFeed.bestVariantUrl.slice(0, 80));

  // For AES-128 streams, pre-fetch the decryption key before committing to native mode. This validates key accessibility while the capture pipeline is still intact,
  // allowing a seamless fallback to capture if the key is inaccessible. Without this, the proxy would discover the problem on its first segment fetch - after the
  // capture pipeline has already been torn down and no fallback is possible.
  let prefetchedKey: Nullable<Buffer> = null;

  if((mediaFeed.encryption === "aes128") && mediaFeed.keyUrl) {

    prefetchedKey = await fetchDecryptionKey(mediaFeed.keyUrl);

    if(!prefetchedKey) {

      LOG.debug("native:coordinator", "Native streaming not viable for %s: decryption key inaccessible.", channelName);

      return null;
    }

    LOG.debug("native:coordinator", "AES-128 decryption key pre-fetched for %s.", channelName);
  }

  // Create the native proxy. The manifest interceptor's underlying CDP session is owned and disposed internally by the interceptor - the proxy holds no session
  // references. For AES-128 streams, the pre-fetched key is passed so the proxy does not need to fetch it again on the first segment. The preroll segment count
  // determines the segment index offset (reserving index space for preroll). Streams with separate audio cannot use preroll because the preroll content is muxed
  // video+audio and can't be split into separate renditions.
  const hasSeparateAudio = mediaFeed.audioVariantUrl !== null;
  const proxyPrerollSegmentCount = (!hasSeparateAudio && options.prerollSegmentCount) ? options.prerollSegmentCount : 0;

  const proxy = createNativeProxy({

    audioVariantUrl: mediaFeed.audioVariantUrl,
    channelName,
    encryption: mediaFeed.encryption,
    keyUrl: mediaFeed.keyUrl,
    onError,
    prefetchedKey,
    prerollCodec: options.prerollCodec,
    prerollSegmentCount: proxyPrerollSegmentCount,
    streamId,
    streamIdStr,
    variantUrl: mediaFeed.bestVariantUrl
  });

  // Schedule token refresh if either URL contains expiration tokens. We pass the variant URL the proxy will poll so the refresh boundary is the earlier of the
  // master and variant expirations.
  scheduleTokenRefresh({

    channelName,
    masterUrl: interception.masterManifestUrl,
    page,
    proxy,
    streamIdStr,
    url,
    variantUrl: mediaFeed.bestVariantUrl
  });

  LOG.debug("timing:native", "Native streaming setup completed for %s in %sms.", channelName, elapsed());

  return { bandwidth: mediaFeed.bandwidth, codec: mediaFeed.codec, hasAudio: mediaFeed.audioVariantUrl !== null, proxy, resolution: mediaFeed.resolution };
}

// Token Refresh.

/**
 * Options for scheduling token refresh.
 */
interface TokenRefreshOptions {

  channelName: string;
  masterUrl: string;
  page: Page;
  proxy: NativeProxy;
  streamIdStr: string;
  url: string;

  // The variant URL the proxy is actually polling. Its token rotates independently of the master URL and may expire first, so the refresh boundary is the earlier
  // of the two. When the master URL carries no expiry token, the variant URL still pins the boundary; when neither does, no refresh is scheduled.
  variantUrl: string;
}

/**
 * Computes the absolute moment (milliseconds since the Unix epoch) at which the next token refresh must occur, or null when neither URL carries an expiry token.
 *
 * The boundary is the earlier of the master URL's and the variant URL's expirations. The master URL governs which refresh strategy can run (a direct re-fetch is
 * possible only while the master token is alive; once it expires only a page reload mints a new master), while the variant URL is what the proxy polls for segments.
 * Taking the minimum guarantees the proxy never holds a dead variant URL and the refresh never tries a direct fetch against an expired master.
 *
 * @param masterUrl - The master manifest URL whose token gates the direct-fetch strategy.
 * @param variantUrl - The variant URL the proxy polls for segments.
 * @returns The earliest expiry in milliseconds since the epoch, or null when neither URL carries an expiry token.
 */
function computeRefreshBoundary(masterUrl: string, variantUrl: string): Nullable<number> {

  const masterExpiry = parseTokenExpiry(masterUrl);
  const variantExpiry = parseTokenExpiry(variantUrl);

  if((masterExpiry === null) && (variantExpiry === null)) {

    return null;
  }

  // At least one expiry exists. Default the missing one to positive infinity so Math.min selects the URL that actually constrains the boundary.
  return Math.min(masterExpiry ?? Number.POSITIVE_INFINITY, variantExpiry ?? Number.POSITIVE_INFINITY);
}

/**
 * Schedules a SINGLE token refresh timer aimed at the next expiry boundary - the earlier of the master URL's and the polled variant URL's expirations. There are two
 * regimes:
 *
 * 1. **Comfortable margin** (more than TOKEN_REFRESH_MARGIN remains): schedule the refresh TOKEN_REFRESH_MARGIN before the boundary, so the fresh manifest is ready
 *    well before the old one expires.
 *
 * 2. **Inside the margin window** (TOKEN_REFRESH_MARGIN or less remains): re-fetching the same master URL does not extend its lifetime, so re-probing it on a fixed
 *    cadence is wasted work. We schedule exactly one refresh aimed at the boundary itself (clamped to MIN_REFRESH_DELAY so an already-expired or imminent boundary
 *    cannot fire back-to-back). When that refresh fires the master token is spent, the direct fetch fails, and the page-reload path mints a genuinely new master URL
 *    - once, at the boundary, not every MIN_REFRESH_DELAY for the final minutes before expiry.
 *
 * This is the core of the busy-loop fix: each refresh reschedules from the boundary it aims at, never from a shrinking-but-unchanging master expiry, so the cadence
 * is a single boundary-targeted timer rather than a per-cycle re-probe.
 *
 * @param options - Token refresh options.
 */
function scheduleTokenRefresh(options: TokenRefreshOptions): void {

  const { channelName, masterUrl, page, proxy, streamIdStr, url, variantUrl } = options;

  const boundary = computeRefreshBoundary(masterUrl, variantUrl);

  if(boundary === null) {

    LOG.debug("native:token", "No token expiry found in manifest URLs for %s.", channelName);

    return;
  }

  const timeUntilExpiry = boundary - Date.now();

  // Outside the margin we lead the boundary by TOKEN_REFRESH_MARGIN; inside it we aim straight at the boundary so the direct fetch fails into a page reload exactly
  // once. Either way the result is clamped to MIN_REFRESH_DELAY so a past-due or imminent boundary still yields a single, non-thrashing timer.
  const lead = (timeUntilExpiry > TOKEN_REFRESH_MARGIN) ? TOKEN_REFRESH_MARGIN : 0;
  const refreshIn = Math.max(MIN_REFRESH_DELAY, timeUntilExpiry - lead);

  LOG.debug("native:token", "Token expires in %ss for %s. Refresh scheduled in %ss.",
    Math.round(timeUntilExpiry / 1000), channelName, Math.round(refreshIn / 1000));

  // Store the timer handle on the proxy so it can be cancelled if the proxy is stopped before the timer fires. Pass the master URL so the refresh can attempt a
  // direct fetch before falling back to a page reload.
  const timer = setTimeout(() => {

    void refreshNativeManifest({ channelName, masterUrl, page, proxy, streamIdStr, url });
  }, refreshIn);

  proxy.setTokenRefreshTimer(timer);
}

/**
 * Refreshes a native stream's manifest to obtain fresh auth tokens. Two strategies are tried in order:
 *
 * 1. **Direct fetch** (if masterUrl is provided): Re-fetches the master manifest URL from Node.js without involving the browser. This avoids the visible page reload
 *    that disrupts the browser tab. The CDN returns the manifest with current token values as long as the URL's own auth token hasn't expired. When the master URL
 *    expires, the direct fetch returns 403 and we fall through to strategy 2.
 *
 * 2. **Page reload**: Navigates the browser page back to the channel URL, triggering fresh authentication via cookies. A CDP interceptor captures the new master
 *    manifest URL with fresh tokens. This is the only path that generates genuinely new tokens and is required when the master URL itself has expired.
 *
 * L2 recovery (failure-triggered) from the monitor omits masterUrl, going straight to page reload since the stream is already failing and needs a full refresh.
 *
 * @param options - Refresh options. masterUrl is optional - when provided, direct fetch is attempted first.
 * @returns True if the refresh succeeded (proxy updated with new manifest), false otherwise.
 */
export async function refreshNativeManifest(options: {
  channelName: string;
  masterUrl?: string;
  page: Page;
  proxy: NativeProxy;
  streamIdStr: string;
  url: string;
}): Promise<boolean> {

  const { channelName, masterUrl, page, proxy, streamIdStr, url } = options;
  const streamLog = LOG.withStreamId(streamIdStr);

  if(proxy.isStopped()) {

    return false;
  }

  const refreshElapsed = startTimer();

  streamLog.debug("native:token", "Starting manifest refresh for %s.", channelName);

  // Strategy 1: Direct fetch. Re-fetch the master manifest URL from Node.js to get fresh variant URLs without reloading the browser page. This works as long as the
  // master URL's own CDN auth token hasn't expired. When it does expire, probeManifest returns null (403) and we fall through to the page reload strategy.
  if(masterUrl) {

    const directResult = await tryDirectManifestRefresh(masterUrl, channelName, streamLog);

    if(directResult) {

      // Check if the proxy was stopped during the async probe (e.g., stream terminated while probing).
      if(proxy.isStopped()) {

        return false;
      }

      // Direct fetch succeeded. Update the proxy with the fresh variant URL(s).
      proxy.updateVariantUrl(directResult.bestVariantUrl);

      if(directResult.audioVariantUrl) {

        proxy.updateAudioVariantUrl(directResult.audioVariantUrl);
      }

      streamLog.debug("native:token", "Manifest refresh completed for %s via direct fetch in %sms.", channelName, refreshElapsed());

      // Schedule the next refresh aimed at the new boundary. The master URL is unchanged by a direct fetch and may still be valid for further direct fetches, but
      // the boundary is now driven by the earlier of the (unchanging) master expiry and the freshly-fetched variant expiry. Because scheduleTokenRefresh aims at
      // that boundary - leading it only when there is comfortable margin - the schedule does not degenerate into a per-cycle re-probe of the still-valid master.
      // Once the master URL expires, the next refresh's direct fetch fails and the page-reload fallback generates a new master URL.
      scheduleTokenRefresh({

        channelName,
        masterUrl,
        page,
        proxy,
        streamIdStr,
        url,
        variantUrl: directResult.bestVariantUrl
      });

      return true;
    }

    // Direct fetch failed. Fall through to page reload.
    streamLog.debug("native:token", "Direct manifest fetch failed for %s. Falling back to page reload.", channelName);
  }

  // Strategy 2: Page reload. Navigate the browser page to trigger fresh authentication and intercept a new master manifest via CDP.
  if(page.isClosed()) {

    streamLog.debug("native:token", "Manifest refresh failed for %s: page is closed.", channelName);

    return false;
  }

  try {

    // Install a fresh interceptor on the page, scope-bound with "using" so its CDP observer is disposed on every exit from this block. The proxy.isStopped early
    // return after navigation, and any throw from page.goto, would otherwise leave the observer (and its 20s timeout) running with no consumer until the timeout
    // fires. The handle self-disposes when its promise resolves, after which this scope-bound disposal is a no-op on repeat. For token refresh the page
    // navigates directly to the channel URL (no guide grid), so the first manifest captured is the correct one; we call finalize() after navigation to resolve
    // immediately with whatever was captured.
    using handle = await installManifestInterceptor(page, 20000);

    if(!handle) {

      streamLog.debug("native:token", "Manifest refresh failed for %s: could not install interceptor.", channelName);

      return false;
    }

    // Navigate the page back to the channel URL to trigger fresh authentication. The interceptor captures .m3u8 requests generated by this navigation. Note: this
    // uses a bare page.goto without the service's site profile (no waitForNetworkIdle, scroll options, etc.). This is acceptable because native streaming only
    // activates for services whose video players load the HLS manifest during basic page load. Services requiring profile-aware navigation for manifest delivery
    // would have failed the initial interception and would not be in native mode.
    await page.goto(url, { timeout: 30000, waitUntil: "domcontentloaded" });

    // Check if the proxy was stopped while we were navigating (e.g., stream terminated during the page load).
    if(proxy.isStopped()) {

      return false;
    }

    // Signal finalize and await the interception result. Token refresh always navigates directly (no guide grid), so the first manifest is correct.
    handle.finalize(true);

    const newInterception = await handle.promise;

    if(!newInterception) {

      streamLog.debug("native:token", "Manifest refresh failed for %s: no manifest intercepted after reload.", channelName);

      return false;
    }

    // Check if the proxy was stopped while we were waiting for the interception. The interceptor has already disposed its observer on resolution; no further
    // cleanup is required.
    if(proxy.isStopped()) {

      return false;
    }

    // Probe the new manifest to get the updated variant URL. The interceptor has already released its observer by the time the promise resolves, so a probe
    // failure here only requires giving up on this refresh attempt - no session bookkeeping to unwind.
    const refreshedFeed = await probeManifest(newInterception.masterManifestUrl, channelName);

    if(!refreshedFeed) {

      streamLog.debug("native:token", "Manifest refresh failed for %s: probe failed on new manifest.", channelName);

      return false;
    }

    // Check if the proxy was stopped during the probe.
    if(proxy.isStopped()) {

      return false;
    }

    // Update the proxy with the new variant URL(s).
    proxy.updateVariantUrl(refreshedFeed.bestVariantUrl);

    if(refreshedFeed.audioVariantUrl) {

      proxy.updateAudioVariantUrl(refreshedFeed.audioVariantUrl);
    }

    streamLog.debug("native:token", "Manifest refresh completed for %s via page reload in %sms.", channelName, refreshElapsed());

    // Schedule the next proactive token refresh with the NEW master URL from the fresh interception and the freshly-probed variant URL. Subsequent direct fetches
    // will use the master URL until it expires; the boundary is the earlier of the master and variant expirations.
    scheduleTokenRefresh({

      channelName,
      masterUrl: newInterception.masterManifestUrl,
      page,
      proxy,
      streamIdStr,
      url,
      variantUrl: refreshedFeed.bestVariantUrl
    });

    return true;
  } catch(error) {

    streamLog.debug("native:token", "Manifest refresh failed for %s: %s.", channelName, formatError(error));

    // Clear the probe cache so a subsequent attempt re-probes.
    clearProbeCache(channelName);

    return false;
  }
}

/**
 * Attempts to refresh the manifest by directly fetching the master manifest URL from Node.js. Returns a fresh MediaFeed if the fetch succeeds and the variant URL
 * has sufficient token lifetime remaining, or null if the direct fetch should be abandoned in favor of a page reload.
 *
 * @param masterUrl - The master manifest URL to re-fetch.
 * @param channelName - The channel name for logging and cache keys.
 * @param streamLog - The stream-scoped logger.
 * @returns A MediaFeed with a fresh variant URL, or null on failure.
 */
async function tryDirectManifestRefresh(masterUrl: string, channelName: string,
  streamLog: ReturnType<typeof LOG.withStreamId>): Promise<Nullable<MediaFeed>> {

  const mediaFeed = await probeManifest(masterUrl, channelName);

  if(!mediaFeed || (mediaFeed.encryption === "drm")) {

    return null;
  }

  // Verify the variant URL's token hasn't already expired or is about to expire. Parse the expiry from the variant URL (not the master URL) since that's what the
  // proxy will actually poll. If the token expires within MIN_USABLE_TOKEN_LIFETIME, the direct fetch result is stale - the page reload path will generate a
  // genuinely fresh token.
  const variantExpiry = parseTokenExpiry(mediaFeed.bestVariantUrl);

  if(variantExpiry) {

    const remaining = variantExpiry - Date.now();

    if(remaining < MIN_USABLE_TOKEN_LIFETIME) {

      streamLog.debug("native:token", "Direct fetch returned near-expired variant for %s (%ss remaining). Discarding.", channelName, Math.round(remaining / 1000));

      return null;
    }
  }

  return mediaFeed;
}
