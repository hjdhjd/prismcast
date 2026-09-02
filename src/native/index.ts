/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Coordinator for native HLS streaming - manifest interception, DRM probe, and proxy lifecycle.
 */
import { LOG, boundedWait, formatError, startTimer } from "../utils/index.ts";
import type { MediaContainer, Nullable } from "../types/index.ts";
import type { MediaFeed, PipelineShape, ProbeCacheIdentity } from "./probe.ts";
import { clearProbeCache, probeManifest } from "./probe.ts";
import type { CaptureCodec } from "../streaming/codec.ts";
import type { ManifestInterceptionResult } from "../browser/manifestInterceptor.ts";
import type { NativeProxy } from "./proxy.ts";
import type { Page } from "puppeteer-core";
import { createNativeProxy } from "./proxy.ts";
import { fetchDecryptionKey } from "./decrypt.ts";
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
 *
 * A refresh re-runs the same ranked selection a tune runs, narrowed to the candidates the running proxy can absorb, so a stream heals in both directions as the
 * service's ladder changes underneath it. A refreshed manifest offering nothing that pipeline can serve is declined outright rather than half-applied: the
 * refresh reports failure, and the monitor's own escalation owns the one designed transition from relaying back to capture.
 *
 * Two triggers reach the refresh - the boundary timer above and the monitor's failure-triggered recovery - and they share one attempt: whichever arrives second
 * awaits the one in flight and reads its real outcome, because running two against a single browser page races their navigations and the monitor steers on the
 * boolean it reads back. An attempt that fails on a live proxy warns and re-arms itself, doubling the delay per consecutive failure up to the refresh margin and
 * using the proxy's one timer slot, so a stream that stumbles keeps its proactive chain instead of coasting until its tokens die.
 */

// Time in milliseconds before token expiry to trigger a refresh. We refresh 5 minutes early to ensure the new manifest is ready before the old one expires.
const TOKEN_REFRESH_MARGIN = 300000;

// Minimum delay in milliseconds before a token refresh fires. This is the absolute floor for any scheduled refresh: when a boundary lands at or in the past (an
// already-expired or imminently-expiring token), we still wait at least this long so the timer cannot fire back-to-back and thrash. It is NOT the steady-state
// cadence - inside the margin window we schedule a single refresh aimed at the actual expiry boundary, not a repeating MIN_REFRESH_DELAY poll (see scheduleTokenRefresh).
const MIN_REFRESH_DELAY = 30000;

// The longest delay Node's setTimeout accepts: a 32-bit signed millisecond count, roughly 24.8 days. A boundary further out than that is clamped to this ceiling,
// where the refresh fires early, re-probes, and reschedules against the boundary it re-derives - one harmless extra refresh per ceiling interval. An unclamped
// delay would instead overflow the timer, which Node fires immediately, turning a distant expiry into a refresh loop running at the speed of the network.
const MAX_TIMER_DELAY_MS = 2147483647;

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

  // Callback invoked when a token refresh binds a fresh feed, carrying the quality facts that rebind may have changed. Optional because recording them is a
  // streaming-layer concern the native layer only relays: the layer that owns the registry entry supplies the closure that writes it.
  onFeedApplied?: (metadata: RefreshedFeedMetadata) => void;

  // The browser page (kept alive for token refresh).
  page: Page;

  // The preroll codec variant for composite playlist construction.
  prerollCodec?: CaptureCodec;

  // Number of preroll segments preceding real content. When non-zero, the proxy starts segment numbering after the preroll range to reserve the index space. The
  // composite playlist reads the base URL dynamically from the stream's HLS state.
  prerollSegmentCount?: number;

  // The probe-cache identity this stream resolves under, built by the stream setup path and carried through the native chain unchanged. Every probe on this
  // stream - the tune-time one here and each token refresh after it - reads and writes the cache under this one identity.
  probeIdentity: ProbeCacheIdentity;

  // Re-establishes the stream's channel on the supplied page and returns the resulting manifest interception. The streaming layer supplies it, closed over the
  // stream's own tune facts; it re-runs that tune under the stream's log context, so the interception handed back was adjudicated and verified by exactly the
  // semantics the original tune used. Null means the channel could not be re-established, whatever the cause.
  reestablishManifest: (page: Page) => Promise<Nullable<ManifestInterceptionResult>>;

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

  // Container format of the upstream segments, mirroring MediaFeed.container. "fmp4" streams carry a separate initialization segment the relay must fetch and
  // re-reference; "ts" streams are self-describing. Null only on the DRM path, which never reaches a successful result.
  container: Nullable<MediaContainer>;

  // Whether the stream has separate audio renditions. Set once at stream creation on HLSState.hasAudio so the HLS handler knows to serve variant playlists.
  hasAudio: boolean;

  // The native proxy that fetches and stores segments. The proxy holds no CDP session references - session ownership lives entirely inside the manifest
  // interceptor's tab network observer, which disposes itself when interception completes (finalize, timeout, predicate match, or explicit dispose).
  proxy: NativeProxy;

  // Video resolution from the master manifest (e.g., "1920x1080"), or null when absent.
  resolution: Nullable<string>;
}

/**
 * The quality facts a token refresh can change when it binds a fresh feed. Selection is constrained to the running pipeline's shape, which holds the container
 * and the audio topology equal across a rebind, so what a refresh can move is the rung of the ladder it landed on: the bandwidth, the codec, and the resolution.
 */
export interface RefreshedFeedMetadata {

  // Declared bandwidth of the bound variant in bits per second. Zero when the BANDWIDTH attribute is absent or unparseable.
  bandwidth: number;

  // Video codec label (e.g., "H264", "HEVC", "AV1"), or null when the CODECS attribute is absent or unrecognized.
  codec: Nullable<string>;

  // Video resolution of the bound variant (e.g., "1920x1080"), or null when the attribute is absent.
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

  const { channelName, interceptionPromise, mpegTsClient, onError, onFeedApplied, page, probeIdentity, reestablishManifest, streamId, streamIdStr,
    url } = options;

  const elapsed = startTimer();

  LOG.debug("native:coordinator", "Attempting native streaming for %s.", channelName);

  // Await the manifest interception with a short bound. The CDP listener was installed before navigation, so by the time we get here the manifest should
  // already be captured or close to it. A lapse is an ordinary outcome here - it simply means capture serves this tune - so the wait is value-shaped and its
  // null joins the interception's own null on the fallback branch below.
  let interception: Nullable<ManifestInterceptionResult>;

  try {

    interception = await boundedWait(interceptionPromise, INTERCEPTION_AWAIT_TIMEOUT);
  } catch(error) {

    LOG.debug("native:coordinator", "Manifest interception error for %s: %s.", channelName, formatError(error));

    return null;
  }

  if(!interception) {

    LOG.debug("native:coordinator", "No manifest intercepted for %s in %sms. Falling back to capture.", channelName, elapsed());

    return null;
  }

  LOG.debug("native:coordinator", "Manifest intercepted for %s in %sms.", channelName, elapsed());

  /* Probe the intercepted URL and normalize the result to a MediaFeed. The probe handles both master and media playlists transparently; this code path does not
   * need to know which kind arrived. Tune admission refuses a window of at most one segment: the interceptor latches the first master on the wire, and for a
   * service that fronts its player with a per-session bumper that master describes the bumper rather than the channel. Declining lands on the null path below,
   * where capture serves the channel the relay could not.
   */
  const mediaFeed = await probeManifest(interception.manifestUrl, probeIdentity, { rejectStaticPlaylists: true });

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

  /* The proxy is built around the feed's container, so a feed that carries none cannot become a pipeline. Only a DRM classification leaves it null, and that
   * path returned above, so this guard answers a state the flow does not reach rather than inventing a container to satisfy the option's type.
   */
  if(mediaFeed.container === null) {

    LOG.debug("native:coordinator", "Native streaming not viable for %s: the probe returned no container classification.", channelName);

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
    container: mediaFeed.container,
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
    masterUrl: interception.manifestUrl,
    onFeedApplied,
    page,
    probeIdentity,
    proxy,
    reestablishManifest,
    streamIdStr,
    url,
    variantUrl: mediaFeed.bestVariantUrl
  });

  LOG.debug("timing:native", "Native streaming setup completed for %s in %sms.", channelName, elapsed());

  return { bandwidth: mediaFeed.bandwidth, codec: mediaFeed.codec, container: mediaFeed.container, hasAudio: mediaFeed.audioVariantUrl !== null, proxy,
    resolution: mediaFeed.resolution };
}

// Token Refresh.

/**
 * Options for scheduling token refresh.
 */
interface TokenRefreshOptions {

  channelName: string;
  masterUrl: string;

  // Callback the refresh invokes when it binds a fresh feed, carried through every reschedule so a stream's quality reporting survives the whole chain of
  // refreshes rather than only the first.
  onFeedApplied?: (metadata: RefreshedFeedMetadata) => void;

  page: Page;

  // The stream's probe-cache identity, carried so every refresh probes under the identity the tune established rather than one derived from the rotating
  // manifest URL it is refreshing.
  probeIdentity: ProbeCacheIdentity;

  proxy: NativeProxy;

  // Re-establishes the stream's channel on the supplied page and returns the resulting manifest interception. The streaming layer supplies it, closed over the
  // stream's own tune facts; it re-runs that tune under the stream's log context, so the interception handed back was adjudicated and verified by exactly the
  // semantics the original tune used. Null means the channel could not be re-established, whatever the cause.
  reestablishManifest: (page: Page) => Promise<Nullable<ManifestInterceptionResult>>;

  streamIdStr: string;
  url: string;

  // The variant URL the proxy is actually polling. Its token rotates independently of the master URL and may expire first, so the refresh boundary is the earlier
  // of the two. When the master URL carries no expiry token, the variant URL still sets the boundary; when neither does, no refresh is scheduled.
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

  const { channelName, masterUrl, onFeedApplied, page, probeIdentity, proxy, reestablishManifest, streamIdStr, url, variantUrl } = options;

  const boundary = computeRefreshBoundary(masterUrl, variantUrl);

  if(boundary === null) {

    LOG.debug("native:token", "No token expiry found in manifest URLs for %s.", channelName);

    return;
  }

  const timeUntilExpiry = boundary - Date.now();

  // Outside the margin we lead the boundary by TOKEN_REFRESH_MARGIN; inside it we aim straight at the boundary so the direct fetch fails into a page reload exactly
  // once. The floor of MIN_REFRESH_DELAY keeps a past-due or imminent boundary from thrashing, and the ceiling of MAX_TIMER_DELAY_MS keeps a distant one inside
  // the range setTimeout can actually represent.
  const lead = (timeUntilExpiry > TOKEN_REFRESH_MARGIN) ? TOKEN_REFRESH_MARGIN : 0;
  const refreshIn = Math.min(Math.max(MIN_REFRESH_DELAY, timeUntilExpiry - lead), MAX_TIMER_DELAY_MS);

  LOG.debug("native:token", "Token expires in %ss for %s. Refresh scheduled in %ss.",
    Math.round(timeUntilExpiry / 1000), channelName, Math.round(refreshIn / 1000));

  // Store the timer handle on the proxy so it can be cancelled if the proxy is stopped before the timer fires. Pass the master URL so the refresh can attempt a
  // direct fetch before falling back to a page reload.
  const timer = setTimeout(() => {

    void refreshNativeManifest({ channelName, masterUrl, onFeedApplied, page, probeIdentity, proxy, reestablishManifest, streamIdStr, url });
  }, refreshIn);

  proxy.setTokenRefreshTimer(timer);
}

/**
 * Options for refreshing a native stream's manifest.
 */
interface ManifestRefreshOptions {

  channelName: string;

  // The master manifest URL a direct fetch re-fetches. Omitted by the monitor's failure-triggered recovery, which knows no live master and goes straight to the
  // page reload.
  masterUrl?: string;

  // Callback invoked when this refresh binds a fresh feed, carrying the quality facts the rebind may have changed.
  onFeedApplied?: (metadata: RefreshedFeedMetadata) => void;

  page: Page;

  // The stream's probe-cache identity, carried so every refresh probes under the identity the tune established rather than one derived from the rotating
  // manifest URL it is refreshing.
  probeIdentity: ProbeCacheIdentity;

  proxy: NativeProxy;

  // Re-establishes the stream's channel on the supplied page and returns the resulting manifest interception. The streaming layer supplies it, closed over the
  // stream's own tune facts; it re-runs that tune under the stream's log context, so the interception handed back was adjudicated and verified by exactly the
  // semantics the original tune used. Null means the channel could not be re-established, whatever the cause.
  reestablishManifest: (page: Page) => Promise<Nullable<ManifestInterceptionResult>>;

  streamIdStr: string;
  url: string;
}

/**
 * Refreshes a native stream's manifest to obtain fresh auth tokens, serialized across callers and re-armed when an attempt fails.
 *
 * Two triggers reach this function: the proactive timer aimed at the token's expiry boundary, and the monitor's failure-triggered recovery. They can arrive
 * within the same cycle, and two refreshes running against one browser page race each other's navigation, so an attempt already in flight is the attempt every
 * caller gets - the later caller awaits it and reads its real settled outcome, which matters because the monitor steers its escalation on the boolean it reads
 * back here.
 *
 * A failed refresh on a live proxy warns and re-arms itself with a doubling delay bounded by the refresh margin, so one transient failure cannot leave a stream
 * with no proactive refresh for the rest of its life. The retry uses the proxy's single timer slot, the same one the boundary schedule uses.
 *
 * @param options - Refresh options. masterUrl is optional - when provided, direct fetch is attempted first. probeIdentity is the stream's own, so the probe on
 *                  either strategy reads and writes the cache under the identity the tune established.
 * @returns True if the refresh succeeded (proxy updated with new manifest), false otherwise.
 */
export async function refreshNativeManifest(options: ManifestRefreshOptions): Promise<boolean> {

  const { channelName, proxy, streamIdStr } = options;
  const streamLog = LOG.withStreamId(streamIdStr);
  const pending = proxy.getPendingRefresh();

  if(pending) {

    return pending;
  }

  /* What goes into the slot is a promise that never rejects: the attempt's own throw is caught and normalized to false here, before the promise is shared, so a
   * caller awaiting the slot can never be handed a rejection from machinery it did not start. The store below happens before this attempt reaches its first
   * suspension point, which is what makes the slot visible to a caller arriving in the same tick.
   */
  const attempt = (async (): Promise<boolean> => {

    try {

      return await runManifestRefresh(options, streamLog);
    } catch(error) {

      streamLog.debug("native:token", "Manifest refresh failed for %s: %s.", channelName, formatError(error));

      return false;
    }
  })();

  proxy.setPendingRefresh(attempt);

  let succeeded = false;

  try {

    succeeded = await attempt;

    if(succeeded) {

      // The stream is refreshing again, so the backoff retires: whatever fails next starts over at the floor delay.
      proxy.clearRefreshFailures();
    } else if(!proxy.isStopped()) {

      /* A failure on a live proxy is the field's early warning that a stream is heading for token death - every refusal inside the attempt reports at debug, so
       * this is the one line an operator sees - and it re-arms the chain the failure would otherwise have ended. The delay doubles per consecutive failure up to
       * the refresh margin, read from a count captured once so the warn and the timer cannot disagree. The re-arm uses the proxy's single timer slot: arming
       * retires whatever is there, and stop() retires whichever is live, so there is never a second timer to leak. Nothing caps the retries because the stream's
       * own lifecycle already does - a token that stays dead drives segment failures into the proxy's error threshold, and the fallback to capture that follows
       * stops the proxy and its timer with it.
       */
      const failures = proxy.noteRefreshFailure();
      const retryIn = Math.min(MIN_REFRESH_DELAY * (2 ** (failures - 1)), TOKEN_REFRESH_MARGIN);

      streamLog.warn("The token refresh for %s did not complete. Retrying in %s seconds.", channelName, Math.round(retryIn / 1000));

      proxy.setTokenRefreshTimer(setTimeout(() => {

        void refreshNativeManifest(options);
      }, retryIn));
    }
  } catch(error) {

    /* Only the bookkeeping above can throw here, since the attempt settles rather than rejects. Catching it keeps the outcome the attempt reached intact and,
     * more to the point, keeps this promise from rejecting: both production callers void or await it without a catch of their own, so an escape would kill the
     * very re-arm chain this function exists to keep alive.
     */
    streamLog.warn("The token refresh bookkeeping for %s did not complete: %s.", channelName, formatError(error));
  } finally {

    proxy.setPendingRefresh(null);
  }

  return succeeded;
}

/**
 * Runs one manifest refresh attempt. Two strategies are tried in order:
 *
 * 1. **Direct fetch** (if masterUrl is provided): Re-fetches the master manifest URL from Node.js without involving the browser. This avoids the visible page reload
 *    that disrupts the browser tab. The CDN returns the manifest with current token values as long as the URL's own auth token hasn't expired. When the master URL
 *    expires, the direct fetch returns 403 and we fall through to strategy 2.
 *
 * 2. **Channel re-establishment**: Runs the stream's own tune again on the page through the capability the streaming layer supplied, so fresh authentication and a
 *    fresh master manifest arrive by exactly the semantics that established the stream - which is what keeps a guide-tuned stream on its own channel across the
 *    reload. This is the only path that generates genuinely new tokens and is required when the master URL itself has expired.
 *
 * L2 recovery (failure-triggered) from the monitor omits masterUrl, going straight to page reload since the stream is already failing and needs a full refresh.
 *
 * Every point at which this attempt resumes after an await re-checks whether the proxy stopped meanwhile, because a stream can terminate at any of them and a
 * stopped proxy must not be written to.
 *
 * @param options - Refresh options, as handed to refreshNativeManifest.
 * @param streamLog - The stream-scoped logger the caller already holds.
 * @returns True if the refresh succeeded (proxy updated with new manifest), false otherwise.
 */
async function runManifestRefresh(options: ManifestRefreshOptions, streamLog: ReturnType<typeof LOG.withStreamId>): Promise<boolean> {

  const { channelName, masterUrl, page, probeIdentity, proxy, reestablishManifest } = options;

  if(proxy.isStopped()) {

    return false;
  }

  const refreshElapsed = startTimer();

  streamLog.debug("native:token", "Starting manifest refresh for %s.", channelName);

  // Strategy 1: Direct fetch. Re-fetch the master manifest URL from Node.js to get fresh variant URLs without reloading the browser page. This works as long as the
  // master URL's own CDN auth token hasn't expired. When it does expire, probeManifest returns null (403) and we fall through to the page reload strategy.
  if(masterUrl) {

    const directResult = await tryDirectManifestRefresh(masterUrl, proxy.getPipelineShape(), probeIdentity, streamLog);

    if(directResult) {

      // Check if the proxy was stopped during the async probe (e.g., stream terminated while probing).
      if(proxy.isStopped()) {

        return false;
      }

      /* Apply the fresh feed, carrying the master URL forward: a direct fetch does not mint a new master, and the one in hand may still serve further direct
       * fetches. The boundary the next schedule aims at therefore comes from the earlier of that unchanging master expiry and the freshly-fetched variant's,
       * which is what keeps a still-valid master from being re-probed on a cadence of its own.
       */
      const applied = applyRefreshedFeed(directResult, masterUrl, options, streamLog);

      if(applied) {

        streamLog.debug("native:token", "Manifest refresh completed for %s via direct fetch in %sms.", channelName, refreshElapsed());
      }

      return applied;
    }

    /* The direct fetch produced nothing usable - the master's token has expired, the fetch failed, or the manifest offered nothing this pipeline can absorb -
     * so the page reload gets the second opinion. A fresh session can mint a ladder the running pipeline fits again, and a decline that persists through the
     * reload leaves through the failure path below.
     */
    streamLog.debug("native:token", "Direct manifest fetch failed for %s. Falling back to page reload.", channelName);
  }

  // Strategy 2: Channel re-establishment. Reload the page through the stream's own tune to trigger fresh authentication and intercept a new master manifest.
  if(page.isClosed()) {

    streamLog.debug("native:token", "Manifest refresh failed for %s: page is closed.", channelName);

    return false;
  }

  try {

    // Re-establish the channel through the streaming layer's capability: the same tune machinery that established this stream navigates, selects, and
    // adjudicates, so the interception below carries the tuned channel's manifest rather than whatever the reloaded page produced first. A null means the
    // re-establishment failed or was rejected by the provider verifier; the refresh fails with it and the monitor's ladder owns the escalation.
    const newInterception = await reestablishManifest(page);

    if(!newInterception) {

      streamLog.debug("native:token", "Manifest refresh failed for %s: the channel could not be re-established.", channelName);

      return false;
    }

    // Check if the proxy was stopped while we were waiting for the interception. The interceptor has already disposed its observer on resolution; no further
    // cleanup is required.
    if(proxy.isStopped()) {

      return false;
    }

    // Probe the new manifest to get the updated variant URL. The interceptor has already released its observer by the time the promise resolves, so a probe
    // failure here only requires giving up on this refresh attempt - no session bookkeeping to unwind.
    const refreshedFeed = await probeManifest(newInterception.manifestUrl, probeIdentity, { pipelineShape: proxy.getPipelineShape() });

    if(!refreshedFeed) {

      streamLog.debug("native:token", "Manifest refresh failed for %s: probe failed on new manifest.", channelName);

      return false;
    }

    // Check if the proxy was stopped during the probe.
    if(proxy.isStopped()) {

      return false;
    }

    // Apply the fresh feed, carrying the NEW master URL from this interception forward: subsequent direct fetches use it until it expires, and the next boundary
    // is the earlier of its expiry and the freshly-probed variant's.
    const applied = applyRefreshedFeed(refreshedFeed, newInterception.manifestUrl, options, streamLog);

    if(applied) {

      streamLog.debug("native:token", "Manifest refresh completed for %s via page reload in %sms.", channelName, refreshElapsed());
    }

    return applied;
  } catch(error) {

    streamLog.debug("native:token", "Manifest refresh failed for %s: %s.", channelName, formatError(error));

    // Clear the probe cache so a subsequent attempt re-probes. The clear addresses the stream's own identity, which is what every probe on this stream reads and
    // writes under.
    clearProbeCache(probeIdentity.key);

    return false;
  }
}

/**
 * Applies an accepted refresh to the running proxy and schedules the next one. Both strategies converge here, so the sequence a successful refresh performs - the
 * URL swaps, the quality report, the reschedule - has exactly one home, and a future check on whether the refreshed feed still describes the same channel has one
 * place to live.
 *
 * @param feed - The accepted feed, already selected against the proxy's pipeline shape.
 * @param masterUrl - The master URL the next schedule should carry, which is the caller's to name: a direct fetch reuses the master it re-fetched, a page reload
 *                    carries the one its fresh interception produced.
 * @param options - The refresh options this cycle was invoked with, threaded into the reschedule so the chain outlives this cycle.
 * @param streamLog - The stream-scoped logger.
 * @returns True when the feed was applied, false when it was declined.
 */
function applyRefreshedFeed(feed: MediaFeed, masterUrl: string, options: ManifestRefreshOptions,
  streamLog: ReturnType<typeof LOG.withStreamId>): boolean {

  const { channelName, onFeedApplied, page, probeIdentity, proxy, reestablishManifest, streamIdStr, url } = options;

  /* A separate-audio pipeline polls two manifests, so it must be handed both URLs or neither: a video URL swapped without its audio would leave the two tracks
   * on different CDN sessions. Selection admits only candidates whose audio topology matches this pipeline's, so an accepted feed arriving here without a
   * rendition contradicts the selection that produced it - the refresh declines the whole application rather than performing half of it. Declining rather than
   * throwing keeps a background cycle survivable: the false return is the same outcome every other refusal produces, and the caller already handles it.
   */
  const { separateAudio } = proxy.getPipelineShape();
  const audioVariantUrl = separateAudio ? feed.audioVariantUrl : null;

  if(separateAudio && (audioVariantUrl === null)) {

    streamLog.debug("native:token", "Declining the refreshed feed for %s: it carries no audio rendition for a pipeline whose audio is a separate one.",
      channelName);

    return false;
  }

  proxy.updateVariantUrl(feed.bestVariantUrl);

  if(audioVariantUrl !== null) {

    proxy.updateAudioVariantUrl(audioVariantUrl);
  }

  /* Report the fresh quality facts upward. The callback belongs to the streaming layer, which owns the registry entry these facts are written to, and its
   * failure must not undo an application that has already happened: a swap that succeeded is worth more than the status display it feeds, so a throw is warned
   * about and the refresh still counts as the success it was.
   */
  if(onFeedApplied) {

    try {

      onFeedApplied({ bandwidth: feed.bandwidth, codec: feed.codec, resolution: feed.resolution });
    } catch(error) {

      streamLog.warn("The refreshed stream quality could not be recorded for %s: %s.", channelName, formatError(error));
    }
  }

  scheduleTokenRefresh({

    channelName,
    masterUrl,
    onFeedApplied,
    page,
    probeIdentity,
    proxy,
    reestablishManifest,
    streamIdStr,
    url,
    variantUrl: feed.bestVariantUrl
  });

  return true;
}

/**
 * Attempts to refresh the manifest by directly fetching the master manifest URL from Node.js. Returns a fresh MediaFeed if the fetch succeeds and the variant URL
 * has sufficient token lifetime remaining, or null if the direct fetch should be abandoned in favor of a page reload.
 *
 * @param masterUrl - The master manifest URL to re-fetch.
 * @param pipelineShape - The running proxy's compatibility envelope, which the probe selects within.
 * @param probeIdentity - The stream's probe-cache identity. The master URL passing through here carries session tokens that rotate on every refresh, so it is
 *                        never what the cache is keyed or stamped by; the stream's own identity is.
 * @param streamLog - The stream-scoped logger.
 * @returns A MediaFeed with a fresh variant URL, or null on failure.
 */
async function tryDirectManifestRefresh(masterUrl: string, pipelineShape: PipelineShape, probeIdentity: ProbeCacheIdentity,
  streamLog: ReturnType<typeof LOG.withStreamId>): Promise<Nullable<MediaFeed>> {

  const mediaFeed = await probeManifest(masterUrl, probeIdentity, { pipelineShape });

  // A null feed is the only refusal to read here. A constrained probe hands back nothing the running pipeline cannot absorb - a mismatched encryption kind,
  // container, or audio topology comes back as null - so the admission question is answered before this line, in the one place that can answer it completely.
  if(!mediaFeed) {

    return null;
  }

  // Verify the variant URL's token hasn't already expired or is about to expire. Parse the expiry from the variant URL (not the master URL) since that's what the
  // proxy will actually poll. If the token expires within MIN_USABLE_TOKEN_LIFETIME, the direct fetch result is stale - the page reload path will generate a
  // genuinely fresh token.
  const variantExpiry = parseTokenExpiry(mediaFeed.bestVariantUrl);

  if(variantExpiry) {

    const remaining = variantExpiry - Date.now();

    if(remaining < MIN_USABLE_TOKEN_LIFETIME) {

      streamLog.debug("native:token", "Direct fetch returned near-expired variant for %s (%ss remaining). Discarding.", probeIdentity.key,
        Math.round(remaining / 1000));

      return null;
    }
  }

  return mediaFeed;
}
