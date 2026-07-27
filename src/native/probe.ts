/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * probe.ts: HLS manifest probe and media-feed normalizer.
 */
import { LOG, chromeFetch, startTimer } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import { inferMediaCodec } from "./codecInference.ts";

/* This module probes an intercepted HLS playlist URL and produces a fully described MediaFeed - the canonical input to the native proxy. The HLS spec defines
 * exactly two playlist kinds, and this module normalizes both to the same shape so downstream code does not branch on which kind arrived:
 *
 * - Master (multivariant) playlists declare variant streams via #EXT-X-STREAM-INF and reference media playlist URLs. We select the highest-bandwidth variant,
 *   resolve any separate audio rendition declared via #EXT-X-MEDIA:TYPE=AUDIO, fetch the chosen variant body, and classify its encryption.
 * - Media playlists declare segments directly via #EXTINF and #EXT-X-TARGETDURATION. The input URL is itself the media feed; we fetch the body once and
 *   classify its encryption.
 *
 * In both cases the output is a MediaFeed carrying the variant URL the proxy will poll, the encryption classification, the AES-128 key URL when applicable, the
 * optional separate-audio rendition URL, and the codec/resolution/bandwidth metadata that the status display reads. Encryption classification logic is identical
 * across both kinds because it operates on the media body, not the master:
 *
 * - No #EXT-X-KEY or METHOD=NONE -> "clear" (no encryption, direct pass-through)
 * - METHOD=AES-128 with accessible key URL -> "aes128" (Node can decrypt with crypto.createDecipheriv)
 * - METHOD=SAMPLE-AES, SAMPLE-AES-CTR, or any other method -> "drm" (requires CDM, not viable)
 *
 * Playlist-kind detection is centralized in classifyHlsPlaylist() so the manifest interceptor and the probe share one source of truth for the master/media
 * decision.
 */

// Timeout for individual manifest/key fetches.
const FETCH_TIMEOUT = 10000;

/**
 * Encryption classification result from probing an HLS manifest.
 */
export type EncryptionType = "aes128" | "clear" | "drm";

/**
 * Kind of HLS playlist as defined by RFC 8216. A master (multivariant) playlist declares variant streams via #EXT-X-STREAM-INF; a media playlist declares
 * segments directly via #EXTINF and #EXT-X-TARGETDURATION. Bodies that show neither signal classify as "unknown" so consumers can ignore them.
 */
export type HlsPlaylistKind = "master" | "media" | "unknown";

/**
 * Classifies an HLS playlist body by inspecting its directives. Master detection short-circuits on the first #EXT-X-STREAM-INF tag because that directive only
 * appears in master playlists; media detection accumulates positive signals (#EXTINF, #EXT-X-TARGETDURATION) across the body. Bodies with neither signal are
 * not HLS playlists. This is the single source of truth for the master/media decision - both the manifest interceptor (transport-layer "is this HLS?" gate) and
 * the probe orchestrator (resolution-layer master-vs-media branch) consume it so the classification cannot drift between call sites.
 *
 * @param body - The raw HLS playlist body text.
 * @returns The kind of playlist, or "unknown" if the body is not a recognizable HLS playlist.
 */
export function classifyHlsPlaylist(body: string): HlsPlaylistKind {

  let mediaSignal = false;

  for(const rawLine of body.split("\n")) {

    const line = rawLine.trim();

    if(!line.startsWith("#")) {

      continue;
    }

    if(line.startsWith("#EXT-X-STREAM-INF")) {

      return "master";
    }

    if(line.startsWith("#EXTINF") || line.startsWith("#EXT-X-TARGETDURATION")) {

      mediaSignal = true;
    }
  }

  return mediaSignal ? "media" : "unknown";
}

/**
 * Fully described HLS media feed ready for consumption by the native proxy. Every code path that produces this type (master-playlist resolution and media-only
 * passthrough) emerges with the same shape, so downstream code (proxy creation, token refresh, status display) does not branch on which playlist kind originally
 * arrived.
 */
export interface MediaFeed {

  // URL of the audio rendition playlist if the master manifest declares a separate audio track via #EXT-X-MEDIA:TYPE=AUDIO with a URI. Null when audio is muxed into
  // the video variant (no separate audio rendition).
  audioVariantUrl: Nullable<string>;

  // Declared bandwidth of the selected variant in bits per second from the #EXT-X-STREAM-INF BANDWIDTH attribute. Zero when the attribute is absent or unparseable.
  bandwidth: number;

  // URL of the highest-bandwidth variant playlist.
  bestVariantUrl: string;

  // Video codec label (e.g., "H264", "HEVC", "AV1"), or null when the CODECS attribute is absent or the codec is unrecognized.
  codec: Nullable<string>;

  // Classified encryption type.
  encryption: EncryptionType;

  // AES-128 key URL if encryption is "aes128". Null otherwise.
  keyUrl: Nullable<string>;

  // Video resolution from the #EXT-X-STREAM-INF RESOLUTION attribute (e.g., "1920x1080"). Null when the attribute is absent.
  resolution: Nullable<string>;
}

// Cache of encryption types keyed by channel name. Stores the classification (clear/aes128/drm) with a timestamp for TTL expiration. Variant URLs and key URLs
// contain session-bound auth tokens that expire between tunes, so they must never be cached - only the stable encryption type is safe to persist across sessions.
// The DRM skip optimization in setup.ts uses this cache to avoid installing the CDP interceptor for channels known to use DRM.
const probeCache = new Map<string, { encryption: EncryptionType; timestamp: number }>();

// Cache entries older than this are considered stale and re-probed. 24 hours covers the case where a service changes a channel's encryption profile (e.g., free ->
// premium DRM). The DRM short-circuit in probeManifest() still applies within the TTL, so frequently-tuned DRM channels avoid repeated probe overhead.
const PROBE_CACHE_TTL = 24 * 60 * 60 * 1000;

/**
 * Returns the cached encryption type for a channel, or null if the channel has not been probed or the cache entry has expired. Used by the stream setup path to skip
 * CDP interceptor installation for channels already known to use DRM.
 *
 * @param channelName - The channel name to look up.
 * @returns The cached encryption type, or null if not probed or expired.
 */
export function getCachedEncryption(channelName: string): Nullable<EncryptionType> {

  const entry = probeCache.get(channelName);

  if(!entry) {

    return null;
  }

  if((Date.now() - entry.timestamp) > PROBE_CACHE_TTL) {

    probeCache.delete(channelName);

    return null;
  }

  return entry.encryption;
}

/**
 * Clears the probe cache for a specific channel. Called when a native stream fails, forcing a fresh probe on the next attempt.
 *
 * @param channelName - The channel name to clear from the cache.
 */
export function clearProbeCache(channelName: string): void {

  probeCache.delete(channelName);
}

/**
 * Probes an HLS playlist URL and returns a fully described MediaFeed. The input may be either a master playlist or a media playlist; classifyHlsPlaylist()
 * decides at runtime and the resolver dispatches accordingly. The probe cache is checked for DRM channels only - if a previous probe classified the channel as
 * DRM, we return the cached result immediately since the caller will bail out regardless of URLs. For viable channels (clear or aes128), we always run the full
 * probe because the variant URL and key URL contain auth tokens that expire between browser sessions.
 *
 * @param playlistUrl - The HLS playlist URL (master or media; contains auth tokens from the browser's original request).
 * @param channelName - The channel name for cache lookup.
 * @returns The MediaFeed, or null if probing fails.
 */
export async function probeManifest(playlistUrl: string, channelName: string): Promise<Nullable<MediaFeed>> {

  // Short-circuit for DRM channels only. The cached DRM classification is stable within the TTL window (services rarely change DRM type), and the caller returns
  // null immediately on DRM without using any URLs. For clear/aes128 channels, we must re-probe to get fresh variant and key URLs with current auth tokens.
  const cached = getCachedEncryption(channelName);

  if(cached === "drm") {

    LOG.debug("native:probe", "Probe cache hit for %s: drm.", channelName);

    return { audioVariantUrl: null, bandwidth: 0, bestVariantUrl: "", codec: null, encryption: "drm", keyUrl: null, resolution: null };
  }

  const elapsed = startTimer();

  try {

    // Fetch the playlist body once and let classifyHlsPlaylist() decide which branch to take. The interceptor has already done a similar classification at the
    // network-observer layer, but we re-classify here because (a) the body can change between the interceptor's read and ours when the master URL serves a
    // live, mutating playlist, and (b) probeManifest() is also invoked directly by the token-refresh path which has no interceptor classification to inherit.
    const body = await fetchManifestText(playlistUrl);

    if(!body) {

      LOG.debug("native:probe", "Failed to fetch playlist for %s.", channelName);

      return null;
    }

    const kind = classifyHlsPlaylist(body);
    const resolved = (kind === "master") ? await resolveMasterPlaylist(body, playlistUrl) :
      (kind === "media") ? await resolveMediaPlaylist(body, playlistUrl) :
        null;

    if(!resolved) {

      LOG.debug("native:probe", "Could not resolve %s playlist for %s.", kind, channelName);

      return null;
    }

    // Classify encryption from the media body. This branch is identical for master-derived and media-only feeds because #EXT-X-KEY tags live on the media
    // playlist regardless of which playlist kind originally arrived.
    const result = await classifyEncryption(resolved, channelName);

    probeCache.set(channelName, { encryption: result.encryption, timestamp: Date.now() });

    LOG.debug("native:probe", "Probe completed for %s in %sms: %s (%s).", channelName, elapsed(), result.encryption, kind);

    return result;
  } catch(error) {

    LOG.debug("native:probe", "Probe failed for %s: %s.", channelName, String(error));

    return null;
  }
}

/**
 * Fetches a manifest URL and returns the response text. Returns null on failure.
 *
 * @param url - The manifest URL to fetch.
 * @returns The response text, or null on failure.
 */
async function fetchManifestText(url: string): Promise<Nullable<string>> {

  try {

    const response = await chromeFetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT) });

    if(!response.ok) {

      LOG.debug("native:probe", "Manifest fetch returned HTTP %s.", response.status);

      return null;
    }

    return await response.text();
  } catch(error) {

    LOG.debug("native:probe", "Manifest fetch error: %s.", String(error));

    return null;
  }
}

/**
 * Resolved media-feed metadata produced by the master-playlist or media-playlist resolver. This is the single shape that classifyEncryption() consumes - the
 * encryption classifier does not branch on which playlist kind originally arrived, so neither does its input. The resolver is also responsible for filling in
 * codec/resolution/bandwidth from whatever signal its branch can read (master playlist's #EXT-X-STREAM-INF for the master branch, first-segment inference for
 * the media branch).
 */
interface ResolvedMedia {

  // URL of the separate audio rendition playlist when the master declares one via #EXT-X-MEDIA:TYPE=AUDIO. Always null for media-only feeds because audio
  // renditions are a master-playlist-level concept.
  audioVariantUrl: Nullable<string>;

  // Declared bandwidth in bits per second from the master's BANDWIDTH attribute. Zero for media-only feeds because the playlist itself carries no bandwidth
  // declaration.
  bandwidth: number;

  // Human-readable video codec label (e.g., "H264", "HEVC"), or null when neither the master's CODECS attribute nor first-segment inference produced a label.
  codec: Nullable<string>;

  // The media playlist body. classifyEncryption() walks this for #EXT-X-KEY tags; for master-derived feeds this is the chosen variant's body, for media-only
  // feeds this is the input playlist itself.
  mediaBody: string;

  // The media playlist URL. The proxy polls this URL on its segment-fetch cycle.
  mediaUrl: string;

  // Video resolution (e.g., "1920x1080") from the master's RESOLUTION attribute. Always null for media-only feeds because TS PMT does not carry resolution and
  // SPS-level inference is out of scope; recovering it would require parsing the SPS NALU inside a video access unit.
  resolution: Nullable<string>;
}

/**
 * Resolves a master playlist into a ResolvedMedia by selecting the highest-bandwidth variant, finding the optional separate-audio rendition, and fetching the
 * chosen variant body.
 *
 * @param masterBody - The master manifest text.
 * @param masterUrl - The master manifest URL for resolving relative variant URLs.
 * @returns The resolved media feed metadata, or null when the master cannot be resolved.
 */
async function resolveMasterPlaylist(masterBody: string, masterUrl: string): Promise<Nullable<ResolvedMedia>> {

  // Parse variant streams and select the highest bandwidth.
  const bestVariant = selectBestVariant(masterBody, masterUrl);

  if(!bestVariant) {

    LOG.debug("native:probe", "No variant streams found in master manifest.");

    return null;
  }

  LOG.debug("native:probe", "Best variant selected: %s.", bestVariant.url.slice(0, 120));

  // Check for a separate audio rendition in the master manifest.
  const audioVariantUrl = parseAudioRendition(masterBody, masterUrl);

  if(audioVariantUrl) {

    LOG.debug("native:probe", "Separate audio rendition found: %s.", audioVariantUrl.slice(0, 120));
  }

  // Fetch the chosen variant manifest. The variant is what classifyEncryption() will inspect for #EXT-X-KEY tags and what the proxy will poll for segments.
  const variantBody = await fetchManifestText(bestVariant.url);

  if(!variantBody) {

    LOG.debug("native:probe", "Failed to fetch variant manifest for %s.", bestVariant.url);

    return null;
  }

  return {

    audioVariantUrl,
    bandwidth: bestVariant.bandwidth,
    codec: bestVariant.codec,
    mediaBody: variantBody,
    mediaUrl: bestVariant.url,
    resolution: bestVariant.resolution
  };
}

/**
 * Resolves a media playlist into a ResolvedMedia. The input URL is itself the media feed - there is no master to traverse - so the resolver wraps the body and
 * URL verbatim, infers the codec from the first segment via codecInference.ts, and returns. Resolution stays null because TS PMT does not carry resolution and
 * SPS-level inference is out of scope.
 *
 * @param mediaBody - The media playlist text.
 * @param mediaUrl - The media playlist URL (the proxy will poll this).
 * @returns The resolved media feed metadata.
 */
async function resolveMediaPlaylist(mediaBody: string, mediaUrl: string): Promise<ResolvedMedia> {

  // Best-effort codec inference. Returns codec=null on any failure (no segment, fetch error, unrecognized format) so the rest of the pipeline continues unimpaired.
  const inferred = await inferMediaCodec({ baseUrl: mediaUrl, playlistBody: mediaBody });

  return {

    audioVariantUrl: null,
    bandwidth: 0,
    codec: inferred.codec,
    mediaBody,
    mediaUrl,
    resolution: null
  };
}

/**
 * Metadata for the selected variant from the master manifest. Internal to resolveMasterPlaylist().
 */
interface VariantSelection {

  // Declared bandwidth in bits per second from the BANDWIDTH attribute.
  bandwidth: number;

  // Video codec from the CODECS attribute (e.g., "H264", "HEVC", "AV1"), or null when the attribute is absent or the codec is unrecognized.
  codec: Nullable<string>;

  // Video resolution from the RESOLUTION attribute (e.g., "1920x1080"), or null when absent.
  resolution: Nullable<string>;

  // Absolute URL of the selected variant playlist.
  url: string;
}

/**
 * Parses #EXT-X-STREAM-INF lines from a master manifest and returns the highest-bandwidth variant with its metadata.
 *
 * @param masterBody - The master manifest content.
 * @param masterUrl - The master manifest URL for resolving relative variant URLs.
 * @returns The selected variant metadata, or null if no variants are found.
 */
function selectBestVariant(masterBody: string, masterUrl: string): Nullable<VariantSelection> {

  // Map video codec prefixes from CODECS attribute to human-readable labels. Defined outside the variant iteration loop to avoid per-line allocation.
  const codecPrefixes: Record<string, string> = { "av01": "AV1", "avc1": "H264", "avc3": "H264", "hev1": "HEVC", "hvc1": "HEVC", "vp09": "VP9" };

  const lines = masterBody.split("\n");
  let bestBandwidth = 0;
  let bestCodec: Nullable<string> = null;
  let bestResolution: Nullable<string> = null;
  let bestUrl: Nullable<string> = null;
  const bandwidths: number[] = [];

  for(let i = 0; i < lines.length; i++) {

    const line = lines[i]?.trim() ?? "";

    if(!line.startsWith("#EXT-X-STREAM-INF:")) {

      continue;
    }

    // Parse BANDWIDTH attribute.
    const bandwidth = Number(/BANDWIDTH=(\d+)/.exec(line)?.[1] ?? 0);

    // Parse RESOLUTION attribute (e.g., RESOLUTION=1920x1080).
    const resolution: Nullable<string> = /RESOLUTION=(\d+x\d+)/.exec(line)?.[1] ?? null;

    // Parse CODECS attribute and map the video codec prefix to a human-readable label. The CODECS value contains comma-separated codec strings (e.g.,
    // "avc1.640028,mp4a.40.2"). The video codec is identified by its prefix: avc1/avc3 -> H264, hvc1/hev1 -> HEVC, av01 -> AV1, vp09 -> VP9.
    const codecsValue = /CODECS="([^"]+)"/.exec(line)?.[1];
    let codec: Nullable<string> = null;

    if(codecsValue) {

      const prefix = codecsValue.split(",")[0]?.trim().split(".")[0];

      codec = prefix ? (codecPrefixes[prefix] ?? null) : null;
    }

    // The variant URL is on the next line.
    const variantLine = lines[i + 1]?.trim() ?? "";

    if(!variantLine || variantLine.startsWith("#")) {

      continue;
    }

    bandwidths.push(bandwidth);

    if(bandwidth > bestBandwidth) {

      bestBandwidth = bandwidth;
      bestCodec = codec;
      bestResolution = resolution;
      bestUrl = variantLine;
    }
  }

  LOG.debug("native:probe", "Found %s variant(s) with bandwidths: %s.", bandwidths.length, bandwidths.join(", "));

  if(!bestUrl) {

    return null;
  }

  // Resolve relative URLs against the master manifest URL.
  return { bandwidth: bestBandwidth, codec: bestCodec, resolution: bestResolution, url: resolveUrl(bestUrl, masterUrl) };
}

/**
 * Classifies the encryption type of a media playlist by parsing its #EXT-X-KEY tags. Operates uniformly on master-derived and media-only ResolvedMedia inputs
 * because #EXT-X-KEY tags are a media-playlist-level concept regardless of whether a master playlist sat above the media.
 *
 * @param resolved - The resolved media feed metadata.
 * @param channelName - The channel name for logging.
 * @returns The MediaFeed with the classified encryption type and (when applicable) the AES-128 key URL.
 */
async function classifyEncryption(resolved: ResolvedMedia, channelName: string): Promise<MediaFeed> {

  const lines = resolved.mediaBody.split("\n");
  let encryption: EncryptionType = "clear";
  let keyUrl: Nullable<string> = null;

  for(const line of lines) {

    const trimmed = line.trim();

    if(!trimmed.startsWith("#EXT-X-KEY:")) {

      continue;
    }

    // Parse METHOD attribute.
    const method = /METHOD=([A-Za-z0-9-]+)/.exec(trimmed)?.[1]?.toUpperCase() ?? "NONE";

    // A NONE tag does not settle the classification on its own. Some manifests interleave a NONE tag with a later, more
    // restrictive key tag (for example, a clear lead-in segment followed by an AES-128 or DRM-protected segment), so we keep
    // scanning rather than stopping here. Every method below this point breaks out of the loop once found, favoring the
    // strongest encryption signal present in the playlist over the order in which the tags happen to appear.
    if(method === "NONE") {

      continue;
    }

    if(method === "AES-128") {

      // Parse URI attribute for the key URL.
      const uri = /URI="([^"]+)"/.exec(trimmed)?.[1];

      if(!uri) {

        LOG.debug("native:probe", "AES-128 key tag has no URI for %s.", channelName);
        encryption = "drm";

        break;
      }

      const rawKeyUrl = resolveUrl(uri, resolved.mediaUrl);

      // Test that the key is accessible and is exactly 16 bytes.
      // eslint-disable-next-line no-await-in-loop
      const keyAccessible = await testKeyAccessibility(rawKeyUrl);

      if(keyAccessible) {

        encryption = "aes128";
        keyUrl = rawKeyUrl;
      } else {

        LOG.debug("native:probe", "AES-128 key inaccessible or wrong size for %s.", channelName);
        encryption = "drm";
      }

      break;
    }

    // SAMPLE-AES, SAMPLE-AES-CTR, or any other method indicates DRM.
    LOG.debug("native:probe", "Unsupported encryption method '%s' for %s.", method, channelName);
    encryption = "drm";

    break;
  }

  return {

    audioVariantUrl: resolved.audioVariantUrl,
    bandwidth: resolved.bandwidth,
    bestVariantUrl: resolved.mediaUrl,
    codec: resolved.codec,
    encryption,
    keyUrl,
    resolution: resolved.resolution
  };
}

/**
 * Tests whether an AES-128 key URL is accessible and returns a 16-byte key.
 *
 * @param keyUrl - The key URL to test.
 * @returns True if the key is accessible and exactly 16 bytes.
 */
async function testKeyAccessibility(keyUrl: string): Promise<boolean> {

  try {

    const response = await chromeFetch(keyUrl, { signal: AbortSignal.timeout(FETCH_TIMEOUT) });

    if(!response.ok) {

      return false;
    }

    const buffer = await response.arrayBuffer();

    LOG.debug("native:probe", "Key fetch returned %s bytes.", buffer.byteLength);

    return buffer.byteLength === 16;
  } catch(error) {

    LOG.debug("native:probe", "Key accessibility test failed: %s.", String(error));

    return false;
  }
}

/**
 * Parses the first #EXT-X-MEDIA:TYPE=AUDIO rendition with a URI from a master manifest. Returns the absolute audio variant URL, or null if no separate audio
 * rendition is declared (audio is muxed into the video variant).
 *
 * @param masterBody - The master manifest content.
 * @param masterUrl - The master manifest URL for resolving relative URIs.
 * @returns The absolute audio variant URL, or null.
 */
function parseAudioRendition(masterBody: string, masterUrl: string): Nullable<string> {

  for(const line of masterBody.split("\n")) {

    const trimmed = line.trim();

    if(!trimmed.startsWith("#EXT-X-MEDIA:")) {

      continue;
    }

    // Only match AUDIO renditions.
    if(!trimmed.includes("TYPE=AUDIO")) {

      continue;
    }

    // Extract the URI attribute. Not all #EXT-X-MEDIA:TYPE=AUDIO tags have a URI - some are descriptive-only when audio is muxed into the video variant.
    const uri = /URI="([^"]+)"/.exec(trimmed)?.[1];

    if(uri) {

      return resolveUrl(uri, masterUrl);
    }
  }

  return null;
}

/**
 * Resolves a potentially relative URL against a base URL. Handles both absolute and relative URLs. Exported for reuse by the proxy module.
 *
 * @param url - The URL to resolve (may be relative or absolute).
 * @param baseUrl - The base URL for resolving relative references.
 * @returns The resolved absolute URL.
 */
export function resolveUrl(url: string, baseUrl: string): string {

  // If the URL is already absolute, return it directly.
  if(url.startsWith("http://") || url.startsWith("https://")) {

    return url;
  }

  // Use the URL constructor to resolve relative URLs against the base.
  return new URL(url, baseUrl).href;
}
