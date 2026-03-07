/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Coordinator for native HLS streaming — manifest interception, DRM probe, and proxy lifecycle.
 */
import { LOG, formatError, startTimer } from "../utils/index.js";
import { clearProbeCache, probeManifest } from "./probe.js";
import { installManifestInterceptor, removeManifestInterceptor } from "./intercept.js";
import type { ManifestInterceptionResult } from "./intercept.js";
import type { NativeProxy } from "./proxy.js";
import type { Nullable } from "../types/index.js";
import type { Page } from "puppeteer-core";
import type { ProbeResult } from "./probe.js";
import { createNativeProxy } from "./proxy.js";
import { fetchDecryptionKey } from "./decrypt.js";

/* This module orchestrates the native streaming decision. After the browser navigates to a channel and video playback begins, we check whether the provider's HLS
 * stream can be consumed directly in Node (bypassing screen capture). The decision flow is:
 *
 * 1. Await the manifest interception promise (CDP listener was installed before navigation)
 * 2. If no manifest was intercepted: return null (fall back to capture)
 * 3. Probe the manifest for encryption type
 * 4. If DRM: log the reason, return null
 * 5. If clear or AES-128: create and return the native proxy
 *
 * Token refresh: When the intercepted URL contains expiration tokens, we schedule a timer to refresh the manifest before the tokens expire. The refresh first
 * attempts a direct Node.js fetch of the master manifest URL (no browser involvement), falling back to a browser page reload when the master URL itself has expired.
 * The proxy continues serving cached segments during the refresh.
 */

// Time in milliseconds before token expiry to trigger a refresh. We refresh 5 minutes early to ensure the new manifest is ready before the old one expires.
const TOKEN_REFRESH_MARGIN = 300000;

// Minimum delay in milliseconds before a token refresh fires. Prevents a refresh loop when the token lifetime is shorter than TOKEN_REFRESH_MARGIN (e.g., Fox Sports
// tokens expire in ~118 seconds). Without this floor, the refresh delay would be Math.max(0, 118000 - 300000) = 0ms, triggering an infinite loop of page reloads.
const MIN_REFRESH_DELAY = 30000;

// Minimum remaining token lifetime (in milliseconds) for a direct-fetched variant URL to be considered usable. If the variant URL's token expires sooner than this,
// the direct fetch result is discarded and we fall back to a page reload to get a genuinely fresh token. This prevents handing the proxy a variant URL that expires
// almost immediately. Set low (5s) because the proxy only needs the variant URL to survive one poll cycle (~3s). A higher threshold (e.g., 30s) would cause Fox.com
// channels to reject perfectly usable variant URLs — Fox.com tokens have ~57s total lifetime, leaving ~27s at the 30s refresh point.
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

  // Whether the stream has separate audio renditions. Set once at stream creation on HLSState.hasAudio so the HLS handler knows to serve variant playlists.
  hasAudio: boolean;

  // The native proxy that fetches and stores segments. The proxy owns the CDP session from interception and cleans it up on stop().
  proxy: NativeProxy;
}

/**
 * Attempts to upgrade a stream from screen capture to native HLS streaming. Returns the native proxy on success, or null if the stream is not viable for native
 * consumption (DRM, interception timeout, or probe failure). The proxy owns the CDP session from interception and manages its lifecycle.
 *
 * @param options - Options for the native streaming attempt.
 * @returns The native stream result on success, or null to fall back to capture.
 */
export async function attemptNativeStreaming(options: AttemptNativeStreamingOptions): Promise<Nullable<NativeStreamResult>> {

  const { channelName, interceptionPromise, mpegTsClient, onError, page, streamId, streamIdStr, url } = options;

  const elapsed = startTimer();

  LOG.debug("native:coordinator", "Attempting native streaming for %s.", channelName);

  // Await the manifest interception with a short timeout. The CDP listener was installed before navigation, so by the time we get here the manifest should
  // already be captured or close to it.
  let interception: Nullable<ManifestInterceptionResult>;

  try {

    interception = await Promise.race([
      interceptionPromise,
      new Promise<null>((resolve) => {

        setTimeout(() => { resolve(null); }, INTERCEPTION_AWAIT_TIMEOUT);
      })
    ]);
  } catch(error) {

    LOG.debug("native:coordinator", "Manifest interception error for %s: %s.", channelName, formatError(error));

    return null;
  }

  if(!interception) {

    LOG.debug("native:coordinator", "No manifest intercepted for %s in %sms. Falling back to capture.", channelName, elapsed());

    return null;
  }

  LOG.debug("native:coordinator", "Manifest intercepted for %s in %sms.", channelName, elapsed());

  // Probe the manifest for encryption type.
  const probeResult = await probeManifest(interception.masterManifestUrl, channelName);

  if(!probeResult) {

    LOG.debug("native:coordinator", "Probe failed for %s. Falling back to capture.", channelName);
    removeManifestInterceptor(interception.cdpSession);

    return null;
  }

  if(probeResult.encryption === "drm") {

    LOG.debug("native:coordinator", "Native streaming not viable for %s: DRM-protected stream.", channelName);
    removeManifestInterceptor(interception.cdpSession);

    return null;
  }

  // Separate audio renditions (e.g., Google DAI on BET/VH1) cannot be served to MPEG-TS clients because the independent video and audio MPEG-TS segments have
  // incompatible PAT/PMT tables and variable packet sizes from ad splicing. HLS clients (Channels DVR) handle separate audio renditions natively via master playlist.
  if(mpegTsClient && probeResult.audioVariantUrl) {

    LOG.debug("native:coordinator", "Native streaming not viable for %s: separate audio rendition incompatible with MPEG-TS clients.", channelName);
    removeManifestInterceptor(interception.cdpSession);

    return null;
  }

  LOG.debug("native:coordinator", "Native streaming viable for %s (%s, variant: %s).", channelName, probeResult.encryption, probeResult.bestVariantUrl.slice(0, 80));

  // For AES-128 streams, pre-fetch the decryption key before committing to native mode. This validates key accessibility while the capture pipeline is still intact,
  // allowing a seamless fallback to capture if the key is inaccessible. Without this, the proxy would discover the problem on its first segment fetch — after the
  // capture pipeline has already been torn down and no fallback is possible.
  let prefetchedKey: Nullable<Buffer> = null;

  if((probeResult.encryption === "aes128") && probeResult.keyUrl) {

    prefetchedKey = await fetchDecryptionKey(probeResult.keyUrl);

    if(!prefetchedKey) {

      LOG.debug("native:coordinator", "Native streaming not viable for %s: decryption key inaccessible.", channelName);
      removeManifestInterceptor(interception.cdpSession);

      return null;
    }

    LOG.debug("native:coordinator", "AES-128 decryption key pre-fetched for %s.", channelName);
  }

  // Create the native proxy. The CDP session is passed so the proxy can clean it up on stop(), preventing session leaks when the stream terminates before a token
  // refresh occurs. For AES-128 streams, the pre-fetched key is passed so the proxy does not need to fetch it again on the first segment.
  const proxy = createNativeProxy({

    audioVariantUrl: probeResult.audioVariantUrl,
    cdpSession: interception.cdpSession,
    channelName,
    encryption: probeResult.encryption,
    keyUrl: probeResult.keyUrl,
    onError,
    prefetchedKey,
    streamId,
    streamIdStr,
    variantUrl: probeResult.bestVariantUrl
  });

  // Schedule token refresh if the URL contains expiration tokens.
  scheduleTokenRefresh({

    channelName,
    masterUrl: interception.masterManifestUrl,
    page,
    proxy,
    streamIdStr,
    url
  });

  LOG.debug("timing:native", "Native streaming setup completed for %s in %sms.", channelName, elapsed());

  return { hasAudio: probeResult.audioVariantUrl !== null, proxy };
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
}

/**
 * Parses token expiration from an HLS manifest or variant URL. Common patterns include `exp=N`, `hdnea=...~exp=N~...`, path-style `/exp=N~`, and `hdnts=exp%3DN`.
 * Returns the expiration timestamp in milliseconds, or null if no expiration is found.
 *
 * @param url - The URL to parse for token expiration.
 * @returns The expiration timestamp in milliseconds, or null.
 */
function parseTokenExpiry(url: string): Nullable<number> {

  // Pattern 1: exp=N (plain query parameter).
  const expMatch = /[?&]exp=(\d{10,13})/.exec(url);

  if(expMatch) {

    const value = Number(expMatch[1]);

    // If the value is 10 digits, it's seconds; if 13, it's milliseconds.
    return value < 1e12 ? value * 1000 : value;
  }

  // Pattern 2: hdnea=...~exp=N~... (Akamai token in query parameter).
  const hdneaExpMatch = /~exp=(\d{10,13})~/.exec(url);

  if(hdneaExpMatch) {

    const value = Number(hdneaExpMatch[1]);

    return value < 1e12 ? value * 1000 : value;
  }

  // Pattern 3: /exp=N~ (Akamai token in URL path, e.g., foxvideo-sports.akamaized.net/exp=N~acl=...).
  const pathExpMatch = /\/exp=(\d{10,13})~/.exec(url);

  if(pathExpMatch) {

    const value = Number(pathExpMatch[1]);

    return value < 1e12 ? value * 1000 : value;
  }

  // Pattern 4: URL-encoded exp%3D (hdnts parameter).
  const encodedExpMatch = /exp%3D(\d{10,13})/.exec(url);

  if(encodedExpMatch) {

    const value = Number(encodedExpMatch[1]);

    return value < 1e12 ? value * 1000 : value;
  }

  return null;
}

/**
 * Schedules a token refresh timer if the manifest URL contains an expiration token. The refresh occurs TOKEN_REFRESH_MARGIN milliseconds before the token expires.
 * On timer fire, the refresh first attempts a direct Node.js fetch of the master manifest URL, falling back to a browser page reload if the direct fetch fails.
 *
 * @param options - Token refresh options.
 */
function scheduleTokenRefresh(options: TokenRefreshOptions): void {

  const { channelName, masterUrl, page, proxy, streamIdStr, url } = options;

  const expiry = parseTokenExpiry(masterUrl);

  if(!expiry) {

    LOG.debug("native:token", "No token expiry found in manifest URL for %s.", channelName);

    return;
  }

  const timeUntilExpiry = expiry - Date.now();
  const refreshIn = Math.max(MIN_REFRESH_DELAY, timeUntilExpiry - TOKEN_REFRESH_MARGIN);

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
 * @param options - Refresh options. masterUrl is optional — when provided, direct fetch is attempted first.
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

      // Schedule the next refresh with the same master URL. It may still be valid for subsequent direct fetches. Once it expires, the direct fetch will fail and
      // the page reload fallback will generate a new master URL.
      scheduleTokenRefresh({

        channelName,
        masterUrl,
        page,
        proxy,
        streamIdStr,
        url
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

    // Install a fresh interceptor on the page. For token refresh, the page navigates directly to the channel URL (no guide grid), so the first manifest captured is
    // the correct one. We call finalize() after navigation to resolve immediately with whatever was captured.
    const handle = await installManifestInterceptor(page, 20000);

    if(!handle) {

      streamLog.debug("native:token", "Manifest refresh failed for %s: could not install interceptor.", channelName);

      return false;
    }

    // Navigate the page back to the channel URL to trigger fresh authentication. The interceptor captures .m3u8 requests generated by this navigation. Note: this
    // uses a bare page.goto without the provider's site profile (no waitForNetworkIdle, scroll options, etc.). This is acceptable because native streaming only
    // activates for providers whose video players load the HLS manifest during basic page load. Providers requiring profile-aware navigation for manifest delivery
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

    // Check if the proxy was stopped while we were waiting for the interception. Clean up the new session since the proxy will not take ownership of it.
    if(proxy.isStopped()) {

      removeManifestInterceptor(newInterception.cdpSession);

      return false;
    }

    // Probe the new manifest to get the updated variant URL. We probe before handing the new CDP session to the proxy so that a probe failure does not leave the
    // proxy holding a reference to a session we are about to clean up.
    const probeResult = await probeManifest(newInterception.masterManifestUrl, channelName);

    if(!probeResult) {

      streamLog.debug("native:token", "Manifest refresh failed for %s: probe failed on new manifest.", channelName);
      removeManifestInterceptor(newInterception.cdpSession);

      return false;
    }

    // Check if the proxy was stopped during the probe. Clean up the new session since the proxy will not take ownership of it.
    if(proxy.isStopped()) {

      removeManifestInterceptor(newInterception.cdpSession);

      return false;
    }

    // Probe succeeded. Hand the new CDP session to the proxy. updateCdpSession cleans up the old session internally before replacing it.
    proxy.updateCdpSession(newInterception.cdpSession);

    // Update the proxy with the new variant URL(s).
    proxy.updateVariantUrl(probeResult.bestVariantUrl);

    if(probeResult.audioVariantUrl) {

      proxy.updateAudioVariantUrl(probeResult.audioVariantUrl);
    }

    streamLog.debug("native:token", "Manifest refresh completed for %s via page reload in %sms.", channelName, refreshElapsed());

    // Schedule the next proactive token refresh with the NEW master URL from the fresh interception. Subsequent direct fetches will use this URL until it expires.
    scheduleTokenRefresh({

      channelName,
      masterUrl: newInterception.masterManifestUrl,
      page,
      proxy,
      streamIdStr,
      url
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
 * Attempts to refresh the manifest by directly fetching the master manifest URL from Node.js. Returns the probe result if the fetch succeeds and the variant URL
 * has sufficient token lifetime remaining, or null if the direct fetch should be abandoned in favor of a page reload.
 *
 * @param masterUrl - The master manifest URL to re-fetch.
 * @param channelName - The channel name for logging and cache keys.
 * @param streamLog - The stream-scoped logger.
 * @returns The probe result with a fresh variant URL, or null on failure.
 */
async function tryDirectManifestRefresh(masterUrl: string, channelName: string,
  streamLog: ReturnType<typeof LOG.withStreamId>): Promise<Nullable<ProbeResult>> {

  const probeResult = await probeManifest(masterUrl, channelName);

  if(!probeResult || (probeResult.encryption === "drm")) {

    return null;
  }

  // Verify the variant URL's token hasn't already expired or is about to expire. Parse the expiry from the variant URL (not the master URL) since that's what the
  // proxy will actually poll. If the token expires within MIN_USABLE_TOKEN_LIFETIME, the direct fetch result is stale — the page reload path will generate a
  // genuinely fresh token.
  const variantExpiry = parseTokenExpiry(probeResult.bestVariantUrl);

  if(variantExpiry) {

    const remaining = variantExpiry - Date.now();

    if(remaining < MIN_USABLE_TOKEN_LIFETIME) {

      streamLog.debug("native:token", "Direct fetch returned near-expired variant for %s (%ss remaining). Discarding.", channelName, Math.round(remaining / 1000));

      return null;
    }
  }

  return probeResult;
}
