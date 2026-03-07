/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * proxy.ts: Native HLS proxy — manifest polling, segment fetching, and playlist generation.
 */
import { LOG, startTimer } from "../utils/index.js";
import { decryptSegment, deriveIvFromSequence, fetchDecryptionKey, parseExplicitIv } from "./decrypt.js";
import { storeAudioSegment, storeSegment, updateAudioPlaylist, updatePlaylist, updateVideoPlaylist } from "../streaming/hlsSegments.js";
import type { CDPSession } from "puppeteer-core";
import type { Nullable } from "../types/index.js";
import { getStream } from "../streaming/registry.js";
import { removeManifestInterceptor } from "./intercept.js";
import { resolveUrl } from "./probe.js";

/* This module implements the native HLS proxy that replaces Chrome screen capture for viable streams. It polls the provider's variant manifest at regular intervals,
 * detects new segments by tracking #EXT-X-MEDIA-SEQUENCE, fetches each segment (optionally decrypting AES-128), stores them in the existing HLS segment system, and
 * generates a clean MPEG-TS HLS playlist.
 *
 * The proxy generates its own playlist rather than rewriting the provider's playlist, which avoids dealing with CDN-relative URLs and provider-specific quirks.
 * Video segments are stored as "segment0.ts", "segment1.ts", etc. For streams with separate audio renditions, audio segments are stored as "audio0.ts",
 * "audio1.ts", etc.
 */

// Timeout for segment fetches.
const SEGMENT_FETCH_TIMEOUT = 10000;

// Maximum consecutive manifest poll failures before reporting an error.
const MAX_MANIFEST_FAILURES = 3;

// Maximum consecutive segment fetch failures before reporting an error.
const MAX_SEGMENT_FAILURES = 5;

/**
 * Options for creating a native HLS proxy.
 */
export interface NativeProxyOptions {

  // URL of the separate audio rendition playlist, or null when audio is muxed into the video variant.
  audioVariantUrl: Nullable<string>;

  // The CDP session from manifest interception. Cleaned up when the proxy stops to prevent session leaks.
  cdpSession: CDPSession;

  // The channel name for logging.
  channelName: string;

  // Encryption type classified by the probe.
  encryption: "aes128" | "clear";

  // AES-128 key URL from the probe result. Required when encryption is "aes128".
  keyUrl: Nullable<string>;

  // Callback invoked on errors for recovery orchestration.
  onError: (error: string) => void;

  // Pre-fetched AES-128 decryption key from the coordinator. When provided, the proxy uses this key directly instead of fetching it on the first segment.
  prefetchedKey: Nullable<Buffer>;

  // Numeric stream ID for segment storage.
  streamId: number;

  // String stream ID for logging.
  streamIdStr: string;

  // The variant manifest URL to poll.
  variantUrl: string;
}

/**
 * Public interface returned by createNativeProxy(). Provides start/stop control and health metrics.
 */
export interface NativeProxy {

  // Returns the number of consecutive segment fetch errors.
  getConsecutiveErrors: () => number;

  // Returns true if the proxy hit its error threshold and stopped itself. The monitor checks this to trigger immediate L3 fallback instead of waiting for the
  // staleness threshold.
  hasErrored: () => boolean;

  // Returns the last segment size in bytes, or null if no segments have been fetched.
  getLastSegmentSize: () => Nullable<number>;

  // Returns the timestamp of the last successfully stored segment.
  getLastSegmentTime: () => number;

  // Returns the current segment index.
  getSegmentIndex: () => number;

  // Returns streaming statistics for the termination summary.
  getStats: () => NativeProxyStats;

  // Returns the target segment duration in seconds from the provider's manifest. Used by the monitor for staleness detection (2× threshold).
  getTargetDuration: () => number;

  // Starts the manifest polling loop.
  start: () => void;

  // Returns true if the proxy has been stopped.
  isStopped: () => boolean;

  // Sets the token refresh timer handle so it can be cancelled on stop. Called by the coordinator after scheduling a refresh.
  setTokenRefreshTimer: (timer: ReturnType<typeof setTimeout>) => void;

  // Stops the proxy and cleans up timers, token refresh timer, and CDP session.
  stop: () => void;

  // Updates the audio variant URL after a token refresh. Only applicable for streams with separate audio renditions.
  updateAudioVariantUrl: (newUrl: string) => void;

  // Updates the active CDP session after a token refresh. The proxy tracks the session so it can clean it up on stop.
  updateCdpSession: (session: CDPSession) => void;

  // Updates the variant URL after a token refresh.
  updateVariantUrl: (newUrl: string) => void;
}

/**
 * Statistics tracked by the native proxy for the termination summary.
 */
export interface NativeProxyStats {

  // Total number of segment fetch errors.
  fetchErrors: number;

  // Total number of segments successfully fetched and stored.
  segmentsFetched: number;

  // Total number of token refresh cycles completed.
  tokenRefreshes: number;
}

// Parsed Manifest Types.

/**
 * A single segment parsed from a variant manifest, with its associated encryption state from the most recent #EXT-X-KEY tag.
 */
interface ParsedSegment {

  // Segment duration from #EXTINF.
  duration: number;

  // Explicit IV hex string from #EXT-X-KEY, or null to derive from sequence number.
  ivHex: Nullable<string>;

  // AES-128 key URL from #EXT-X-KEY, or null for clear segments.
  keyUrl: Nullable<string>;

  // Media sequence number for this segment.
  sequence: number;

  // Absolute segment URL.
  url: string;
}

/**
 * Result of parsing a variant manifest's metadata and segment list.
 */
interface ManifestParseResult {

  // #EXT-X-MEDIA-SEQUENCE value.
  mediaSequence: number;

  // Parsed segment entries with duration, encryption state, and absolute URLs.
  segments: ParsedSegment[];

  // #EXT-X-TARGETDURATION value.
  targetDuration: number;
}

// Manifest Parsing Helpers.

/**
 * Parses a variant manifest into its metadata and segment list. Handles #EXT-X-MEDIA-SEQUENCE, #EXT-X-TARGETDURATION, #EXT-X-KEY (AES-128 with key rotation), and
 * #EXTINF + segment URL pairs. Key/IV state is tracked per-segment so key rotation within a single manifest is handled correctly.
 *
 * @param body - The variant manifest text content.
 * @param baseUrl - Base URL for resolving relative segment and key URLs.
 * @returns Parsed metadata and segment list.
 */
function parseVariantManifest(body: string, baseUrl: string): ManifestParseResult {

  const lines = body.split("\n");

  // Parse #EXT-X-MEDIA-SEQUENCE.
  let mediaSequence = 0;

  for(const line of lines) {

    const trimmed = line.trim();

    if(trimmed.startsWith("#EXT-X-MEDIA-SEQUENCE:")) {

      mediaSequence = Number(trimmed.split(":")[1]);

      break;
    }
  }

  // Parse #EXT-X-TARGETDURATION.
  let targetDuration = 6;

  for(const line of lines) {

    const trimmed = line.trim();

    if(trimmed.startsWith("#EXT-X-TARGETDURATION:")) {

      targetDuration = Number(trimmed.split(":")[1]);

      break;
    }
  }

  // Parse segments: each #EXTINF line is followed by the segment URL. Per the HLS spec, each #EXT-X-KEY applies to all subsequent segments until the next
  // #EXT-X-KEY tag. We track the current key URL and IV per segment so key rotation within a single manifest is handled correctly.
  const segments: ParsedSegment[] = [];
  let currentSequence = mediaSequence;
  let currentManifestKeyUrl: Nullable<string> = null;
  let currentIvHex: Nullable<string> = null;

  for(let i = 0; i < lines.length; i++) {

    const trimmed = lines[i].trim();

    if(trimmed.startsWith("#EXT-X-KEY:")) {

      const methodMatch = /METHOD=([A-Za-z0-9-]+)/.exec(trimmed);
      const method = methodMatch ? methodMatch[1].toUpperCase() : "NONE";

      if(method === "AES-128") {

        const uriMatch = /URI="([^"]+)"/.exec(trimmed);

        if(uriMatch) {

          currentManifestKeyUrl = resolveUrl(uriMatch[1], baseUrl);
        }

        const ivMatch = /IV=([0-9a-fA-Fx]+)/.exec(trimmed);

        currentIvHex = ivMatch ? ivMatch[1] : null;
      } else if(method === "NONE") {

        currentManifestKeyUrl = null;
        currentIvHex = null;
      }

      continue;
    }

    if(!trimmed.startsWith("#EXTINF:")) {

      continue;
    }

    const durationStr = trimmed.slice(8).split(",")[0];
    const duration = Number(durationStr);
    const segUrl = (i + 1 < lines.length) ? lines[i + 1].trim() : "";

    if(!segUrl || segUrl.startsWith("#")) {

      currentSequence++;

      continue;
    }

    segments.push({ duration, ivHex: currentIvHex, keyUrl: currentManifestKeyUrl, sequence: currentSequence, url: resolveUrl(segUrl, baseUrl) });
    currentSequence++;
  }

  return { mediaSequence, segments, targetDuration };
}

/**
 * Builds a variant playlist string from stored segment filenames and their durations. Used for both video and audio variant playlists.
 *
 * @param segmentEntries - Ordered list of segment filenames from the segment Map.
 * @param durations - Map of filename to #EXTINF duration.
 * @param prefix - Filename prefix for extracting the sequence index (e.g., "segment" or "audio").
 * @param targetDuration - #EXT-X-TARGETDURATION value.
 * @returns The formatted m3u8 playlist string.
 */
function buildVariantPlaylist(segmentEntries: string[], durations: Map<string, number>, prefix: string, targetDuration: number): string {

  const playlistLines: string[] = [
    "#EXTM3U",
    "#EXT-X-VERSION:3",
    "#EXT-X-TARGETDURATION:" + String(Math.ceil(targetDuration))
  ];

  if(segmentEntries.length > 0) {

    const firstIndex = Number(segmentEntries[0].replace(prefix, "").replace(".ts", ""));

    playlistLines.push("#EXT-X-MEDIA-SEQUENCE:" + String(firstIndex));
  } else {

    playlistLines.push("#EXT-X-MEDIA-SEQUENCE:0");
  }

  for(const filename of segmentEntries) {

    const duration = durations.get(filename) ?? targetDuration;

    playlistLines.push("#EXTINF:" + duration.toFixed(3) + ",");
    playlistLines.push(filename);
  }

  return playlistLines.join("\n") + "\n";
}

/**
 * Creates a native HLS proxy that polls a variant manifest, fetches segments, and generates playlists.
 *
 * @param options - Proxy configuration.
 * @returns The native proxy interface.
 */
export function createNativeProxy(options: NativeProxyOptions): NativeProxy {

  const { channelName, encryption, keyUrl, onError, streamId } = options;
  const { audioVariantUrl: initialAudioVariantUrl, variantUrl: initialVariantUrl } = options;
  const hasAudio = initialAudioVariantUrl !== null;
  let activeCdpSession: CDPSession = options.cdpSession;
  let audioVariantUrl = initialAudioVariantUrl;
  let variantUrl = initialVariantUrl;

  // State.
  let pollTimer: ReturnType<typeof setTimeout> | null = null;
  let tokenRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;
  let segmentIndex = 0;
  let lastMediaSequence = -1;
  let lastSegmentSize: Nullable<number> = null;
  let lastSegmentTime = 0;
  let consecutiveManifestFailures = 0;
  const videoSegmentTracker = { consecutiveFailures: 0, debugLabel: "Segment", label: "segment" };
  let errorThresholdReached = false;
  let firstPollComplete = false;
  let lastAudioTargetDuration = 6;
  let lastTargetDuration = 6;
  let readinessSignaled = false;

  // Statistics.
  let totalFetchErrors = 0;
  let totalSegmentsFetched = 0;
  let totalTokenRefreshes = 0;

  // AES-128 decryption key cache. Maps key URLs to their fetched 16-byte keys. Each segment in a manifest can reference a different key URL (key rotation), so we
  // cache by URL rather than maintaining a single "current key". The coordinator pre-fetches the initial key, which is seeded into the cache here.
  const keysByUrl = new Map<string, Buffer>();

  if(options.prefetchedKey && keyUrl) {

    keysByUrl.set(keyUrl, options.prefetchedKey);
  }

  // Track fetched media sequence numbers to avoid re-fetching segments.
  const fetchedSequences = new Set<number>();

  // Track per-segment durations from #EXTINF tags. Keyed by segment filename (e.g., "segment0.ts") to enable accurate playlist generation. Without this, the
  // playlist would use the target duration as a uniform approximation, causing Channels DVR to miscalculate stream timing.
  const segmentDurations = new Map<string, number>();

  // Audio-specific state for streams with separate audio renditions.
  let audioSegmentIndex = 0;
  let audioConsecutiveManifestFailures = 0;
  const audioSegmentTracker = { consecutiveFailures: 0, debugLabel: "Audio segment", label: "audio segment" };
  const audioFetchedSequences = new Set<number>();
  const audioSegmentDurations = new Map<string, number>();

  /**
   * Orchestrates the poll cycle: fetches video and audio manifests in parallel, stores segments, and generates playlists after both stores are updated.
   */
  async function pollManifest(): Promise<void> {

    if(stopped) {

      return;
    }

    const pollElapsed = startTimer();

    try {

      const response = await fetch(variantUrl, { signal: AbortSignal.timeout(SEGMENT_FETCH_TIMEOUT) });

      if(!response.ok) {

        consecutiveManifestFailures++;
        totalFetchErrors++;

        LOG.debug("native:proxy", "Manifest poll failed for %s: HTTP %s.", channelName, response.status);

        if(consecutiveManifestFailures >= MAX_MANIFEST_FAILURES) {

          errorThresholdReached = true;
          stopped = true;
          onError("manifest poll failed " + String(consecutiveManifestFailures) + " times");

          return;
        }

        schedulePoll(5000);

        return;
      }

      consecutiveManifestFailures = 0;

      const body = await response.text();

      LOG.debug("native:proxy", "Manifest poll for %s completed in %sms.", channelName, pollElapsed());

      // Fetch and store video and audio segments in parallel so neither delays the other, and generate playlists after both stores are updated. This ensures video
      // and audio playlists are published atomically — no window where a client sees a master playlist referencing audio.m3u8 before audio segments exist.
      let hasNewVideoSegments: boolean;
      let hasNewAudioSegments = false;

      if(hasAudio) {

        [ hasNewVideoSegments, hasNewAudioSegments ] = await Promise.all([ processManifest(body), pollAudioManifest() ]);
      } else {

        hasNewVideoSegments = await processManifest(body);
      }

      if(hasNewVideoSegments || hasNewAudioSegments) {

        generatePlaylist(lastTargetDuration);

        if(hasAudio) {

          generateAudioPlaylist(lastAudioTargetDuration);
        }
      }

      // Set firstPollComplete when segments are available. For streams with separate audio, require both video and audio segments before signaling readiness to
      // prevent publishing a master playlist before both variant playlists have content.
      if(!firstPollComplete && (segmentIndex > 0) && (!hasAudio || (audioSegmentIndex > 0))) {

        firstPollComplete = true;
      }

      // Signal playlist readiness after the first poll completes. Note: initSegmentReady is signaled immediately when the proxy starts (in hls.ts) since native
      // MPEG-TS has no separate init segment.
      if(firstPollComplete && !readinessSignaled) {

        readinessSignaled = true;

        const stream = getStream(streamId);

        if(stream) {

          stream.hls.signalPlaylistReady();
        }

        LOG.debug("native:proxy", "First poll cycle complete for %s. Segment index: %s.", channelName, segmentIndex);
      }
    } catch(error) {

      consecutiveManifestFailures++;
      totalFetchErrors++;

      LOG.debug("native:proxy", "Manifest poll failed for %s: %s.", channelName, String(error));

      if(consecutiveManifestFailures >= MAX_MANIFEST_FAILURES) {

        errorThresholdReached = true;
        stopped = true;
        onError("manifest poll error: " + String(error));

        return;
      }
    }

    // Schedule the next poll at roughly half the target segment duration for timely detection of new segments.
    schedulePoll(3000);
  }

  /**
   * Parses a variant manifest and fetches new segments. Playlist generation is handled by the caller after both video and audio segments are stored.
   *
   * @param body - The variant manifest text content.
   * @returns True if new segments were stored, false if the media sequence hadn't advanced.
   */
  async function processManifest(body: string): Promise<boolean> {

    const { mediaSequence, segments, targetDuration } = parseVariantManifest(body, variantUrl);

    lastTargetDuration = targetDuration;

    // Prune old entries from the fetchedSequences Set and segmentDurations Map. The provider's media sequence window slides forward, so entries below the current
    // base sequence will never be checked again. Without pruning, these structures grow unboundedly over hours of streaming.
    if(fetchedSequences.size > 100) {

      for(const seq of fetchedSequences) {

        if(seq < mediaSequence) {

          fetchedSequences.delete(seq);
        }
      }
    }

    if(segmentDurations.size > 100) {

      const stream = getStream(streamId);
      const activeSegments = stream ? new Set(stream.hls.segments.keys()) : new Set<string>();

      for(const key of segmentDurations.keys()) {

        if(!activeSegments.has(key)) {

          segmentDurations.delete(key);
        }
      }
    }

    // Fetch new segments (those we haven't fetched yet).
    const newSegments = segments.filter((s) => !fetchedSequences.has(s.sequence));

    if(newSegments.length === 0) {

      // No new segments — the media sequence hasn't advanced.
      if(lastMediaSequence === mediaSequence) {

        LOG.debug("native:proxy", "No new segments for %s (sequence still %s).", channelName, mediaSequence);
      }

      lastMediaSequence = mediaSequence;

      return false;
    }

    LOG.debug("native:proxy", "Fetching %s new segment(s) for %s (sequence %s).", newSegments.length, channelName, mediaSequence);

    lastMediaSequence = mediaSequence;

    // Fetch and store each new segment sequentially. Track whether at least one segment was stored so the caller only generates playlists when data actually changed.
    let storedAny = false;

    for(const seg of newSegments) {

      if(stopped) {

        break;
      }

      // eslint-disable-next-line no-await-in-loop
      const segmentData = await fetchTrackedSegment(videoSegmentTracker, seg.url, seg.sequence, seg.ivHex, seg.keyUrl);

      if(!segmentData) {

        continue;
      }

      const filename = "segment" + String(segmentIndex) + ".ts";

      // Check segment count before store to detect rotation (oldest segment evicted to enforce maxSegments limit).
      const stream = getStream(streamId);
      const countBefore = stream?.hls.segments.size ?? 0;

      storeSegment(streamId, filename, segmentData);
      fetchedSequences.add(seg.sequence);
      segmentDurations.set(filename, seg.duration);
      storedAny = true;

      lastSegmentSize = segmentData.length;
      lastSegmentTime = Date.now();
      segmentIndex++;
      totalSegmentsFetched++;

      // Log the first segment fetch latency for timing diagnostics.
      if(totalSegmentsFetched === 1) {

        LOG.debug("timing:native", "First segment fetched for %s (%s bytes).", channelName, segmentData.length);
      }

      // Detect segment rotation — if the count didn't increase, the oldest segment was evicted.
      const countAfter = stream?.hls.segments.size ?? 0;

      if((countAfter <= countBefore) && (countBefore > 0)) {

        LOG.debug("native:proxy", "Segment rotated for %s (oldest removed, %s segments retained).", channelName, countAfter);
      }

      LOG.debug("native:proxy", "Stored %s (%s bytes, seq %s) for %s.", filename, segmentData.length, seg.sequence, channelName);
    }

    return storedAny;
  }

  /**
   * Fetches and optionally decrypts a segment. This is the shared core used by both video and audio fetch paths. It handles the HTTP fetch, AES-128 decryption with
   * key caching, and IV derivation. Error tracking is the caller's responsibility since video and audio have independent failure thresholds.
   *
   * @param url - The segment URL.
   * @param sequence - The media sequence number (for IV derivation).
   * @param ivHex - Explicit IV hex string from the manifest, or null.
   * @param segKeyUrl - The key URL for this specific segment from the manifest's #EXT-X-KEY tag, or null for clear segments.
   * @returns The segment data (decrypted if necessary), or null on failure.
   */
  async function fetchAndDecryptSegment(url: string, sequence: number, ivHex: Nullable<string>, segKeyUrl: Nullable<string>): Promise<Nullable<Buffer>> {

    const response = await fetch(url, { signal: AbortSignal.timeout(SEGMENT_FETCH_TIMEOUT) });

    if(!response.ok) {

      LOG.debug("native:proxy", "Segment fetch failed for %s: HTTP %s.", channelName, response.status);

      return null;
    }

    let data: Buffer = Buffer.from(await response.arrayBuffer());

    // Decrypt if AES-128 and this segment has a key URL. Segments before the first #EXT-X-KEY tag in the manifest have segKeyUrl === null (clear).
    if((encryption === "aes128") && segKeyUrl) {

      // Look up the key in the per-URL cache, or fetch it if not cached.
      let key = keysByUrl.get(segKeyUrl);

      if(!key) {

        key = await fetchDecryptionKey(segKeyUrl) ?? undefined;

        if(!key) {

          LOG.debug("native:proxy", "Decryption key unavailable for %s (key URL: %s).", channelName, segKeyUrl.slice(0, 80));

          return null;
        }

        keysByUrl.set(segKeyUrl, key);

        LOG.debug("native:proxy", "New decryption key cached for %s.", channelName);
      }

      // Determine the IV: use explicit IV from the manifest, or derive from the media sequence number.
      const iv = ivHex ? (parseExplicitIv(ivHex) ?? deriveIvFromSequence(sequence)) : deriveIvFromSequence(sequence);

      LOG.debug("native:decrypt", "IV source for sequence %s: %s.", sequence, ivHex ? "explicit" : "sequence");

      data = decryptSegment(data, key, iv);
    }

    return data;
  }

  /**
   * Fetches a segment with error tracking. Wraps fetchAndDecryptSegment with per-tracker failure counting. Consecutive failures trigger the error threshold which
   * stops the proxy and signals the monitor for recovery. Used for both video and audio segments with independent trackers.
   *
   * @param tracker - Mutable error tracking state with labels for log and error messages.
   * @param url - The segment URL.
   * @param sequence - The media sequence number (for IV derivation).
   * @param ivHex - Explicit IV hex string from the manifest, or null.
   * @param segKeyUrl - The key URL for this specific segment, or null for clear segments.
   * @returns The segment data (decrypted if necessary), or null on failure.
   */
  async function fetchTrackedSegment(tracker: { consecutiveFailures: number; debugLabel: string; label: string }, url: string, sequence: number,
    ivHex: Nullable<string>, segKeyUrl: Nullable<string>): Promise<Nullable<Buffer>> {

    try {

      const data = await fetchAndDecryptSegment(url, sequence, ivHex, segKeyUrl);

      if(!data) {

        tracker.consecutiveFailures++;
        totalFetchErrors++;

        if(tracker.consecutiveFailures >= MAX_SEGMENT_FAILURES) {

          errorThresholdReached = true;
          stopped = true;
          onError(tracker.label + " fetch failed " + String(tracker.consecutiveFailures) + " times");
        }

        return null;
      }

      tracker.consecutiveFailures = 0;

      return data;
    } catch(error) {

      tracker.consecutiveFailures++;
      totalFetchErrors++;

      LOG.debug("native:proxy", "%s fetch failed for %s: %s.", tracker.debugLabel, channelName, String(error));

      if(tracker.consecutiveFailures >= MAX_SEGMENT_FAILURES) {

        errorThresholdReached = true;
        stopped = true;
        onError(tracker.label + " fetch error: " + String(error));
      }

      return null;
    }
  }

  /**
   * Polls the audio variant manifest and fetches new audio segments. Returns true if new segments were stored. Only called for streams with separate audio
   * renditions.
   */
  async function pollAudioManifest(): Promise<boolean> {

    if(stopped || !audioVariantUrl) {

      return false;
    }

    try {

      const response = await fetch(audioVariantUrl, { signal: AbortSignal.timeout(SEGMENT_FETCH_TIMEOUT) });

      if(!response.ok) {

        audioConsecutiveManifestFailures++;
        totalFetchErrors++;

        LOG.debug("native:proxy", "Audio manifest poll failed for %s: HTTP %s.", channelName, response.status);

        if(audioConsecutiveManifestFailures >= MAX_MANIFEST_FAILURES) {

          errorThresholdReached = true;
          stopped = true;
          onError("audio manifest poll failed " + String(audioConsecutiveManifestFailures) + " times");
        }

        return false;
      }

      audioConsecutiveManifestFailures = 0;

      const body = await response.text();

      return await processAudioManifest(body, audioVariantUrl);
    } catch(error) {

      audioConsecutiveManifestFailures++;
      totalFetchErrors++;

      LOG.debug("native:proxy", "Audio manifest poll failed for %s: %s.", channelName, String(error));

      if(audioConsecutiveManifestFailures >= MAX_MANIFEST_FAILURES) {

        errorThresholdReached = true;
        stopped = true;
        onError("audio manifest poll error: " + String(error));
      }

      return false;
    }
  }

  /**
   * Parses an audio variant manifest and fetches new audio segments. Playlist generation is handled by the caller after both video and audio segments are stored.
   *
   * @param body - The audio variant manifest text content.
   * @param baseUrl - The audio variant URL for resolving relative segment URLs.
   * @returns True if new segments were stored, false if the media sequence hadn't advanced.
   */
  async function processAudioManifest(body: string, baseUrl: string): Promise<boolean> {

    const { mediaSequence, segments, targetDuration } = parseVariantManifest(body, baseUrl);

    lastAudioTargetDuration = targetDuration;

    // Prune old entries from audioFetchedSequences.
    if(audioFetchedSequences.size > 100) {

      for(const seq of audioFetchedSequences) {

        if(seq < mediaSequence) {

          audioFetchedSequences.delete(seq);
        }
      }
    }

    if(audioSegmentDurations.size > 100) {

      const stream = getStream(streamId);
      const activeSegments = stream ? new Set(stream.hls.audioSegments.keys()) : new Set<string>();

      for(const key of audioSegmentDurations.keys()) {

        if(!activeSegments.has(key)) {

          audioSegmentDurations.delete(key);
        }
      }
    }

    const newSegments = segments.filter((s) => !audioFetchedSequences.has(s.sequence));

    if(newSegments.length === 0) {

      return false;
    }

    LOG.debug("native:proxy", "Fetching %s new audio segment(s) for %s (sequence %s).", newSegments.length, channelName, mediaSequence);

    let storedAny = false;

    for(const seg of newSegments) {

      if(stopped) {

        break;
      }

      // eslint-disable-next-line no-await-in-loop
      const segmentData = await fetchTrackedSegment(audioSegmentTracker, seg.url, seg.sequence, seg.ivHex, seg.keyUrl);

      if(!segmentData) {

        continue;
      }

      const filename = "audio" + String(audioSegmentIndex) + ".ts";

      storeAudioSegment(streamId, filename, segmentData);
      audioFetchedSequences.add(seg.sequence);
      audioSegmentDurations.set(filename, seg.duration);
      storedAny = true;

      audioSegmentIndex++;
      totalSegmentsFetched++;

      LOG.debug("native:proxy", "Stored %s (%s bytes, seq %s) for %s.", filename, segmentData.length, seg.sequence, channelName);
    }

    return storedAny;
  }

  /**
   * Generates playlists from the stored segments. For streams without separate audio, generates a single variant playlist. For streams with separate audio,
   * generates a master playlist referencing video.m3u8 and audio.m3u8, plus individual variant playlists for each.
   *
   * @param targetDuration - The #EXT-X-TARGETDURATION value from the provider's manifest.
   */
  function generatePlaylist(targetDuration: number): void {

    const stream = getStream(streamId);

    if(!stream) {

      return;
    }

    const segmentEntries = Array.from(stream.hls.segments.keys());

    if(!hasAudio) {

      // Muxed audio — single variant playlist served as stream.m3u8.
      updatePlaylist(streamId, buildVariantPlaylist(segmentEntries, segmentDurations, "segment", targetDuration));
    } else {

      // Separate audio — generate video variant playlist and master playlist.
      updateVideoPlaylist(streamId, buildVariantPlaylist(segmentEntries, segmentDurations, "segment", targetDuration));

      // Estimate bandwidth from stored segment sizes and durations. Sum total bytes and total duration across video segments, then convert to bits per second.
      // Falls back to 5 Mbps when no duration data is available (first segment before durations are populated).
      let bandwidth = 5000000;
      let totalBytes = 0;
      let totalDuration = 0;

      for(const filename of segmentEntries) {

        const size = stream.hls.segments.get(filename)?.length ?? 0;
        const duration = segmentDurations.get(filename);

        if(duration) {

          totalBytes += size;
          totalDuration += duration;
        }
      }

      if(totalDuration > 0) {

        bandwidth = Math.round((totalBytes * 8) / totalDuration);
      }

      // Generate the master playlist referencing video.m3u8 and audio.m3u8.
      const masterPlaylist = "#EXTM3U\n" +
        "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"English\",DEFAULT=YES,AUTOSELECT=YES,URI=\"audio.m3u8\"\n" +
        "#EXT-X-STREAM-INF:BANDWIDTH=" + String(bandwidth) + ",AUDIO=\"audio\"\n" +
        "video.m3u8\n";

      updatePlaylist(streamId, masterPlaylist);
    }

    LOG.debug("native:proxy", "Playlist generated for %s with %s segment(s), target duration %ss.", channelName, segmentEntries.length, targetDuration);
  }

  /**
   * Generates the audio variant playlist from stored audio segments.
   *
   * @param targetDuration - The #EXT-X-TARGETDURATION value from the audio manifest.
   */
  function generateAudioPlaylist(targetDuration: number): void {

    const stream = getStream(streamId);

    if(!stream) {

      return;
    }

    const audioEntries = Array.from(stream.hls.audioSegments.keys());

    updateAudioPlaylist(streamId, buildVariantPlaylist(audioEntries, audioSegmentDurations, "audio", targetDuration));
  }

  /**
   * Schedules the next manifest poll after a delay.
   *
   * @param delayMs - Delay in milliseconds before the next poll.
   */
  function schedulePoll(delayMs: number): void {

    if(stopped) {

      return;
    }

    pollTimer = setTimeout(() => {

      void pollManifest();
    }, delayMs);
  }

  return {

    getConsecutiveErrors: (): number => videoSegmentTracker.consecutiveFailures + consecutiveManifestFailures + audioSegmentTracker.consecutiveFailures +
      audioConsecutiveManifestFailures,

    getLastSegmentSize: (): Nullable<number> => lastSegmentSize,

    getLastSegmentTime: (): number => lastSegmentTime,

    getSegmentIndex: (): number => segmentIndex,

    getStats: (): NativeProxyStats => ({

      fetchErrors: totalFetchErrors,
      segmentsFetched: totalSegmentsFetched,
      tokenRefreshes: totalTokenRefreshes
    }),

    getTargetDuration: (): number => lastTargetDuration,

    hasErrored: (): boolean => errorThresholdReached,

    isStopped: (): boolean => stopped,

    setTokenRefreshTimer: (timer: ReturnType<typeof setTimeout>): void => {

      tokenRefreshTimer = timer;
    },

    start: (): void => {

      LOG.debug("native:proxy", "Starting native proxy for %s (%s).", channelName, encryption);
      void pollManifest();
    },

    stop: (): void => {

      stopped = true;

      if(pollTimer) {

        clearTimeout(pollTimer);
        pollTimer = null;
      }

      // Cancel the pending token refresh timer to prevent fire-after-termination. Without this, the timer fires on a stopped proxy and attempts to navigate a
      // potentially closed or reused page.
      if(tokenRefreshTimer) {

        clearTimeout(tokenRefreshTimer);
        tokenRefreshTimer = null;
      }

      // Clean up the CDP session from manifest interception to prevent session leaks.
      removeManifestInterceptor(activeCdpSession);

      LOG.debug("native:proxy", "Stopped native proxy for %s.", channelName);
    },

    updateAudioVariantUrl: (newUrl: string): void => {

      audioVariantUrl = newUrl;
      audioFetchedSequences.clear();

      LOG.debug("native:proxy", "Audio variant URL updated for %s.", channelName);
    },

    updateCdpSession: (session: CDPSession): void => {

      // Clean up the old CDP session before replacing it. This prevents session leaks when the session is swapped during token refresh or L2 recovery.
      removeManifestInterceptor(activeCdpSession);
      activeCdpSession = session;
    },

    updateVariantUrl: (newUrl: string): void => {

      variantUrl = newUrl;
      totalTokenRefreshes++;

      // Clear segment tracking state. The new variant URL may point to a different CDN session with its own media sequence numbering (e.g., Fox Sports resets to 0
      // on each new session). Without this reset, the proxy's fetchedSequences Set contains sequence numbers from the old session that overlap with the new one,
      // causing all new segments to be incorrectly filtered as "already fetched."
      fetchedSequences.clear();
      lastMediaSequence = -1;

      LOG.debug("native:proxy", "Variant URL updated for %s. Segment tracking reset.", channelName);
    }
  };
}
