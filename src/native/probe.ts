/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * probe.ts: DRM probe for HLS manifest encryption classification.
 */
import { LOG, chromeFetch, startTimer } from "../utils/index.js";
import type { Nullable } from "../types/index.js";

/* This module fetches an HLS master manifest, selects the highest-bandwidth variant, and inspects its #EXT-X-KEY tags to classify the encryption type. The result
 * determines whether PrismCast can consume the stream natively (clear or AES-128) or must fall back to screen capture (Widevine, FairPlay, or other DRM).
 *
 * Classification logic:
 * - No #EXT-X-KEY or METHOD=NONE → "clear" (no encryption, direct pass-through)
 * - METHOD=AES-128 with accessible key URL → "aes128" (Node can decrypt with crypto.createDecipheriv)
 * - METHOD=SAMPLE-AES, SAMPLE-AES-CTR, or any other method → "drm" (requires CDM, not viable)
 */

// Timeout for individual manifest/key fetches.
const FETCH_TIMEOUT = 10000;

/**
 * Encryption classification result from probing an HLS manifest.
 */
export type EncryptionType = "aes128" | "clear" | "drm";

/**
 * Result of probing an HLS master manifest for encryption type and best variant.
 */
export interface ProbeResult {

  // URL of the audio rendition playlist if the master manifest declares a separate audio track via #EXT-X-MEDIA:TYPE=AUDIO with a URI. Null when audio is muxed into
  // the video variant (no separate audio rendition).
  audioVariantUrl: Nullable<string>;

  // Declared bandwidth of the selected variant in bits per second from the #EXT-X-STREAM-INF BANDWIDTH attribute. Zero when the attribute is absent or unparseable.
  bandwidth: number;

  // URL of the highest-bandwidth variant playlist.
  bestVariantUrl: string;

  // Classified encryption type.
  encryption: EncryptionType;

  // AES-128 key URL if encryption is "aes128". Null otherwise.
  keyUrl: Nullable<string>;

  // Video resolution from the #EXT-X-STREAM-INF RESOLUTION attribute (e.g., "1920x1080"). Null when the attribute is absent.
  resolution: Nullable<string>;
}

// Cache of encryption types keyed by channel name. Stores only the classification (clear/aes128/drm), not the full ProbeResult with URLs. Variant URLs and key URLs
// contain session-bound auth tokens that expire between tunes, so they must never be cached — only the stable encryption type is safe to persist across sessions.
// The DRM skip optimization in setup.ts uses this cache to avoid installing the CDP interceptor for channels known to use DRM.
const probeCache = new Map<string, EncryptionType>();

/**
 * Returns the cached encryption type for a channel, or null if the channel has not been probed. Used by the stream setup path to skip CDP interceptor installation
 * for channels already known to use DRM.
 *
 * @param channelName - The channel name to look up.
 * @returns The cached encryption type, or null if not probed.
 */
export function getCachedEncryption(channelName: string): Nullable<EncryptionType> {

  return probeCache.get(channelName) ?? null;
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
 * Probes an HLS master manifest to determine encryption type and select the best variant. The probe cache is checked for DRM channels only — if a previous probe
 * classified the channel as DRM, we return the cached result immediately since the caller will bail out regardless of URLs. For viable channels (clear or aes128),
 * we always run the full probe because the variant URL and key URL contain auth tokens that expire between browser sessions.
 *
 * @param masterUrl - The master manifest URL (contains auth tokens from the browser's original request).
 * @param channelName - The channel name for cache lookup.
 * @returns The probe result, or null if probing fails.
 */
export async function probeManifest(masterUrl: string, channelName: string): Promise<Nullable<ProbeResult>> {

  // Short-circuit for DRM channels only. The cached DRM classification is stable (providers don't change DRM type mid-process), and the caller returns null
  // immediately on DRM without using any URLs. For clear/aes128 channels, we must re-probe to get fresh variant and key URLs with current auth tokens.
  const cached = probeCache.get(channelName);

  if(cached === "drm") {

    LOG.debug("native:probe", "Probe cache hit for %s: drm.", channelName);

    return { audioVariantUrl: null, bandwidth: 0, bestVariantUrl: "", encryption: "drm", keyUrl: null, resolution: null };
  }

  const elapsed = startTimer();

  try {

    // Fetch the master manifest.
    const masterBody = await fetchManifestText(masterUrl);

    if(!masterBody) {

      LOG.debug("native:probe", "Failed to fetch master manifest for %s.", channelName);

      return null;
    }

    // Parse variant streams and select the highest bandwidth.
    const bestVariant = selectBestVariant(masterBody, masterUrl);

    if(!bestVariant) {

      LOG.debug("native:probe", "No variant streams found in master manifest for %s.", channelName);

      return null;
    }

    LOG.debug("native:probe", "Best variant selected for %s: %s.", channelName, bestVariant.url.slice(0, 120));

    // Check for a separate audio rendition in the master manifest.
    const audioVariantUrl = parseAudioRendition(masterBody, masterUrl);

    if(audioVariantUrl) {

      LOG.debug("native:probe", "Separate audio rendition found for %s: %s.", channelName, audioVariantUrl.slice(0, 120));
    }

    // Fetch the variant manifest.
    const variantBody = await fetchManifestText(bestVariant.url);

    if(!variantBody) {

      LOG.debug("native:probe", "Failed to fetch variant manifest for %s.", channelName);

      return null;
    }

    // Classify encryption.
    const result = await classifyEncryption(variantBody, bestVariant, audioVariantUrl, channelName);

    probeCache.set(channelName, result.encryption);

    LOG.debug("native:probe", "Probe completed for %s in %sms: %s.", channelName, elapsed(), result.encryption);

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
 * Metadata for the selected variant from the master manifest.
 */
interface VariantSelection {

  // Declared bandwidth in bits per second from the BANDWIDTH attribute.
  bandwidth: number;

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

  const lines = masterBody.split("\n");
  let bestBandwidth = 0;
  let bestResolution: Nullable<string> = null;
  let bestUrl: Nullable<string> = null;
  const bandwidths: number[] = [];

  for(let i = 0; i < lines.length; i++) {

    const line = lines[i].trim();

    if(!line.startsWith("#EXT-X-STREAM-INF:")) {

      continue;
    }

    // Parse BANDWIDTH attribute.
    const bandwidthMatch = /BANDWIDTH=(\d+)/.exec(line);
    const bandwidth = bandwidthMatch ? Number(bandwidthMatch[1]) : 0;

    // Parse RESOLUTION attribute (e.g., RESOLUTION=1920x1080).
    const resolutionMatch = /RESOLUTION=(\d+x\d+)/.exec(line);
    const resolution: Nullable<string> = resolutionMatch ? resolutionMatch[1] : null;

    // The variant URL is on the next line.
    const variantLine = (i + 1 < lines.length) ? lines[i + 1].trim() : "";

    if(!variantLine || variantLine.startsWith("#")) {

      continue;
    }

    bandwidths.push(bandwidth);

    if(bandwidth > bestBandwidth) {

      bestBandwidth = bandwidth;
      bestResolution = resolution;
      bestUrl = variantLine;
    }
  }

  LOG.debug("native:probe", "Found %s variant(s) with bandwidths: %s.", bandwidths.length, bandwidths.join(", "));

  if(!bestUrl) {

    return null;
  }

  // Resolve relative URLs against the master manifest URL.
  return { bandwidth: bestBandwidth, resolution: bestResolution, url: resolveUrl(bestUrl, masterUrl) };
}

/**
 * Classifies the encryption type of a variant manifest by parsing its #EXT-X-KEY tags.
 *
 * @param variantBody - The variant manifest content.
 * @param variant - The selected variant metadata (URL, bandwidth, resolution).
 * @param audioVariantUrl - The audio rendition URL, or null when audio is muxed.
 * @param channelName - The channel name for logging.
 * @returns The probe result with the classified encryption type.
 */
async function classifyEncryption(variantBody: string, variant: VariantSelection, audioVariantUrl: Nullable<string>,
  channelName: string): Promise<ProbeResult> {

  const lines = variantBody.split("\n");
  let encryption: EncryptionType = "clear";
  let keyUrl: Nullable<string> = null;

  for(const line of lines) {

    const trimmed = line.trim();

    if(!trimmed.startsWith("#EXT-X-KEY:")) {

      continue;
    }

    // Parse METHOD attribute.
    const methodMatch = /METHOD=([A-Za-z0-9-]+)/.exec(trimmed);
    const method = methodMatch ? methodMatch[1].toUpperCase() : "NONE";

    if(method === "NONE") {

      continue;
    }

    if(method === "AES-128") {

      // Parse URI attribute for the key URL.
      const uriMatch = /URI="([^"]+)"/.exec(trimmed);

      if(!uriMatch) {

        LOG.debug("native:probe", "AES-128 key tag has no URI for %s.", channelName);
        encryption = "drm";

        break;
      }

      const rawKeyUrl = resolveUrl(uriMatch[1], variant.url);

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

  return { audioVariantUrl, bandwidth: variant.bandwidth, bestVariantUrl: variant.url, encryption, keyUrl, resolution: variant.resolution };
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

    // Extract the URI attribute. Not all #EXT-X-MEDIA:TYPE=AUDIO tags have a URI — some are descriptive-only when audio is muxed into the video variant.
    const uriMatch = /URI="([^"]+)"/.exec(trimmed);

    if(uriMatch) {

      return resolveUrl(uriMatch[1], masterUrl);
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
