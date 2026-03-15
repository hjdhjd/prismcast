/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * proxy.ts: Native HLS proxy — manifest polling, segment fetching, and playlist generation.
 */
import { LOG, startTimer } from "../utils/index.js";
import { buildPrerollEntries, computePrerollWindow } from "../streaming/preroll.js";
import { decryptSegment, deriveIvFromSequence, fetchDecryptionKey, parseExplicitIv } from "./decrypt.js";
import { storeAudioSegment, storeSegment, updateAudioPlaylist, updatePlaylist, updateVideoPlaylist } from "../streaming/hlsSegments.js";
import type { CDPSession } from "puppeteer-core";
import { CONFIG } from "../config/index.js";
import type { Nullable } from "../types/index.js";
import type { PlaylistSegmentEntry } from "../streaming/playlistBuilder.js";
import { buildPlaylist } from "../streaming/playlistBuilder.js";
import { getStream } from "../streaming/registry.js";
import { removeManifestInterceptor } from "./intercept.js";
import { resolveUrl } from "./probe.js";

/* This module implements the native HLS proxy that replaces Chrome screen capture for viable streams. It polls the provider's variant manifest at regular intervals,
 * detects new segments by tracking #EXT-X-MEDIA-SEQUENCE, fetches each segment (optionally decrypting AES-128), stores them in the existing HLS segment system, and
 * generates an HLS playlist that faithfully propagates upstream metadata — discontinuity markers, program timestamps, and SCTE-35 ad signaling (cue-out, cue-in,
 * cue-out-cont).
 *
 * The proxy generates its own playlist rather than rewriting the provider's playlist, which avoids dealing with CDN-relative URLs and provider-specific quirks. However,
 * it preserves all playback-critical tags from the upstream manifest so that downstream consumers (Channels DVR) can correctly handle PTS resets at ad boundaries,
 * synchronize wall-clock time, and detect commercial breaks.
 *
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

  // Number of preroll segments preceding real content. When non-zero, the proxy starts segment numbering at this index to reserve the preroll index range. The
  // composite playlist behavior (including preroll entries) is determined dynamically by checking stream.hls.prerollStartTime at playlist generation time.
  prerollSegmentCount?: number;

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
 * A single segment parsed from a variant manifest, with its associated encryption state, discontinuity flags, and upstream metadata from the most recent tags.
 */
interface ParsedSegment {

  // True if this segment is preceded by an #EXT-X-CUE-IN tag (ad break end).
  cueIn: boolean;

  // #EXT-X-CUE-OUT duration value preceding this segment (ad break start), or null if not an ad boundary.
  cueOut: Nullable<string>;

  // #EXT-X-CUE-OUT-CONT value for this segment (ad break continuation with elapsed time and duration), or null when not inside an ad break.
  cueOutCont: Nullable<string>;

  // True if this segment is preceded by an #EXT-X-DISCONTINUITY tag (PTS reset boundary).
  discontinuity: boolean;

  // Segment duration from #EXTINF.
  duration: number;

  // Explicit IV hex string from #EXT-X-KEY, or null to derive from sequence number.
  ivHex: Nullable<string>;

  // AES-128 key URL from #EXT-X-KEY, or null for clear segments.
  keyUrl: Nullable<string>;

  // Upstream #EXT-X-PROGRAM-DATE-TIME ISO string preceding this segment, or null when the manifest omits it.
  programDateTime: Nullable<string>;

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

  // Parsed segment entries with duration, encryption state, discontinuity flags, and absolute URLs.
  segments: ParsedSegment[];

  // #EXT-X-TARGETDURATION value.
  targetDuration: number;
}

/**
 * Per-segment metadata tracked alongside the stored segment data. These Maps are keyed by local filename (e.g., "segment0.ts") and provide the information needed to
 * generate a faithful playlist with upstream tags propagated.
 */
interface SegmentMetadata {

  // Segments preceded by #EXT-X-CUE-IN in the upstream manifest.
  cueIns: Map<string, boolean>;

  // Segments with #EXT-X-CUE-OUT-CONT in the upstream manifest. Value is the full tag parameter string (elapsed time, duration, SCTE-35 data).
  cueOutConts: Map<string, string>;

  // Segments preceded by #EXT-X-CUE-OUT in the upstream manifest. Value is the duration string from the tag.
  cueOuts: Map<string, string>;

  // Segments preceded by #EXT-X-DISCONTINUITY in the upstream manifest.
  discontinuities: Map<string, boolean>;

  // Segment durations from #EXTINF tags.
  durations: Map<string, number>;

  // Wall-clock timestamps as ISO strings from #EXT-X-PROGRAM-DATE-TIME. Only present for segments where the upstream manifest provides this tag.
  timestamps: Map<string, string>;

  // Total number of discontinuities observed across all segments ever stored. Used with the count of discontinuities in the current playlist window to compute the
  // #EXT-X-DISCONTINUITY-SEQUENCE header value.
  totalDiscontinuities: number;
}

// Manifest Parsing Helpers.

/**
 * Parses a variant manifest into its metadata and segment list. Handles #EXT-X-MEDIA-SEQUENCE, #EXT-X-TARGETDURATION, #EXT-X-KEY (AES-128 with key rotation),
 * #EXT-X-DISCONTINUITY, #EXT-X-PROGRAM-DATE-TIME, #EXT-X-CUE-OUT, #EXT-X-CUE-OUT-CONT, #EXT-X-CUE-IN, and #EXTINF + segment URL pairs. Key/IV state and metadata
 * flags are tracked per-segment so key rotation and discontinuities within a single manifest are handled correctly.
 *
 * @param body - The variant manifest text content.
 * @param baseUrl - Base URL for resolving relative segment and key URLs.
 * @returns Parsed metadata and segment list.
 */
function parseVariantManifest(body: string, baseUrl: string): ManifestParseResult {

  const lines = body.split("\n");

  // Parse header tags: #EXT-X-MEDIA-SEQUENCE and #EXT-X-TARGETDURATION. The upstream #EXT-X-DISCONTINUITY-SEQUENCE is intentionally ignored — the proxy computes its
  // own value from its local sliding window, which differs from the upstream's window size and rotation rate.
  let mediaSequence = 0;
  let targetDuration = 6;

  for(const line of lines) {

    const trimmed = line.trim();

    if(trimmed.startsWith("#EXT-X-MEDIA-SEQUENCE:")) {

      mediaSequence = Number(trimmed.split(":")[1]);
    } else if(trimmed.startsWith("#EXT-X-TARGETDURATION:")) {

      targetDuration = Number(trimmed.split(":")[1]);
    }
  }

  // Parse segments: each #EXTINF line is followed by the segment URL. Per the HLS spec, each #EXT-X-KEY applies to all subsequent segments until the next
  // #EXT-X-KEY tag. We track the current key URL and IV per segment so key rotation within a single manifest is handled correctly. Metadata tags
  // (#EXT-X-DISCONTINUITY, #EXT-X-PROGRAM-DATE-TIME, #EXT-X-CUE-OUT, #EXT-X-CUE-IN, #EXT-X-CUE-OUT-CONT) are accumulated as pending flags and attached to the
  // next segment.
  const segments: ParsedSegment[] = [];
  let currentSequence = mediaSequence;
  let currentManifestKeyUrl: Nullable<string> = null;
  let currentIvHex: Nullable<string> = null;

  // Pending metadata flags — accumulated from tags between segments and attached to the next #EXTINF.
  let pendingCueIn = false;
  let pendingCueOut: Nullable<string> = null;
  let pendingCueOutCont: Nullable<string> = null;
  let pendingDiscontinuity = false;
  let pendingProgramDateTime: Nullable<string> = null;

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

    if(trimmed === "#EXT-X-DISCONTINUITY") {

      pendingDiscontinuity = true;

      continue;
    }

    if(trimmed.startsWith("#EXT-X-PROGRAM-DATE-TIME:")) {

      pendingProgramDateTime = trimmed.slice(25);

      continue;
    }

    if(trimmed.startsWith("#EXT-X-CUE-OUT-CONT:")) {

      pendingCueOutCont = trimmed.slice(20);

      continue;
    }

    if(trimmed.startsWith("#EXT-X-CUE-OUT:")) {

      pendingCueOut = trimmed.slice(15);

      continue;
    }

    if(trimmed === "#EXT-X-CUE-OUT") {

      pendingCueOut = "";

      continue;
    }

    if(trimmed === "#EXT-X-CUE-IN") {

      pendingCueIn = true;

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

    segments.push({

      cueIn: pendingCueIn,
      cueOut: pendingCueOut,
      cueOutCont: pendingCueOutCont,
      discontinuity: pendingDiscontinuity,
      duration,
      ivHex: currentIvHex,
      keyUrl: currentManifestKeyUrl,
      programDateTime: pendingProgramDateTime,
      sequence: currentSequence,
      url: resolveUrl(segUrl, baseUrl)
    });

    // Reset pending flags after attaching them to a segment.
    pendingCueIn = false;
    pendingCueOut = null;
    pendingCueOutCont = null;
    pendingDiscontinuity = false;
    pendingProgramDateTime = null;
    currentSequence++;
  }

  return { mediaSequence, segments, targetDuration };
}

/**
 * Builds a variant playlist string from stored segment filenames and their associated metadata. Emits #EXT-X-DISCONTINUITY, #EXT-X-DISCONTINUITY-SEQUENCE,
 * #EXT-X-PROGRAM-DATE-TIME, #EXT-X-CUE-OUT, #EXT-X-CUE-OUT-CONT, and #EXT-X-CUE-IN tags to faithfully represent upstream ad boundaries and PTS resets.
 *
 * @param segmentEntries - Ordered list of segment filenames from the segment Map.
 * @param metadata - Per-segment metadata (durations, timestamps, discontinuities, cue markers).
 * @param prefix - Filename prefix for extracting the sequence index (e.g., "segment" or "audio").
 * @param targetDuration - #EXT-X-TARGETDURATION value.
 * @returns The formatted m3u8 playlist string.
 */
function buildVariantPlaylist(segmentEntries: string[], metadata: SegmentMetadata, prefix: string, targetDuration: number): string {

  // Compute MEDIA-SEQUENCE from the first filename in the window. The filename encodes the local segment index (e.g., "segment5.ts" → 5).
  let mediaSequence = 0;

  if(segmentEntries.length > 0) {

    mediaSequence = Number(segmentEntries[0].replace(prefix, "").replace(".ts", ""));
  }

  // Compute DISCONTINUITY-SEQUENCE: total discontinuities ever observed minus those still visible in the current window. Only provided when > 0 to keep playlists
  // clean for streams that never have discontinuities.
  let windowDiscontinuities = 0;

  for(const filename of segmentEntries) {

    if(metadata.discontinuities.has(filename)) {

      windowDiscontinuities++;
    }
  }

  const discSeq = metadata.totalDiscontinuities - windowDiscontinuities;
  const discontinuitySequence = (discSeq > 0) ? discSeq : undefined;

  const entries = segmentEntries.map((filename) => buildEntryFromMetadata(filename, metadata, targetDuration));

  return buildPlaylist({ discontinuitySequence, mediaSequence, targetDuration, version: 3 }, entries);
}

/**
 * Creates an empty SegmentMetadata instance with fresh Maps and a zeroed discontinuity counter.
 * @returns A new SegmentMetadata instance.
 */
function createSegmentMetadata(): SegmentMetadata {

  return {

    cueIns: new Map<string, boolean>(),
    cueOutConts: new Map<string, string>(),
    cueOuts: new Map<string, string>(),
    discontinuities: new Map<string, boolean>(),
    durations: new Map<string, number>(),
    timestamps: new Map<string, string>(),
    totalDiscontinuities: 0
  };
}

/**
 * Records per-segment metadata from a parsed upstream segment into a SegmentMetadata instance. Stores duration, upstream program-date-time (when available),
 * discontinuity flag, and SCTE-35 cue markers.
 *
 * @param meta - The metadata instance to update.
 * @param filename - The local segment filename (e.g., "segment0.ts").
 * @param seg - The parsed segment with upstream metadata.
 */
function storeSegmentMetadata(meta: SegmentMetadata, filename: string, seg: ParsedSegment): void {

  meta.durations.set(filename, seg.duration);

  if(seg.programDateTime) {

    meta.timestamps.set(filename, seg.programDateTime);
  }

  if(seg.discontinuity) {

    meta.discontinuities.set(filename, true);
    meta.totalDiscontinuities++;
  }

  if(seg.cueIn) {

    meta.cueIns.set(filename, true);
  }

  if(seg.cueOut !== null) {

    meta.cueOuts.set(filename, seg.cueOut);
  }

  if(seg.cueOutCont !== null) {

    meta.cueOutConts.set(filename, seg.cueOutCont);
  }
}

/**
 * Builds a PlaylistSegmentEntry from a segment's metadata. Translates the per-segment Maps (durations, timestamps, discontinuities, cue markers) into the shared entry
 * type used by the playlist builder. Used by both buildVariantPlaylist() and buildCompositePlaylist() to avoid duplicating the field-by-field metadata lookup.
 *
 * @param filename - The segment filename (e.g., "segmentN.ts" where N starts at prerollSegmentCount).
 * @param metadata - The metadata instance to read from.
 * @param defaultDuration - Fallback duration when the metadata Map has no entry for this filename.
 * @returns A PlaylistSegmentEntry with all applicable metadata fields set.
 */
function buildEntryFromMetadata(filename: string, metadata: SegmentMetadata, defaultDuration: number): PlaylistSegmentEntry {

  const entry: PlaylistSegmentEntry = {

    duration: metadata.durations.get(filename) ?? defaultDuration,
    url: filename
  };

  if(metadata.discontinuities.has(filename)) {

    entry.discontinuity = true;
  }

  const timestamp = metadata.timestamps.get(filename);

  if(timestamp) {

    entry.programDateTime = timestamp;
  }

  if(metadata.cueIns.has(filename)) {

    entry.cueIn = true;
  }

  const cueOut = metadata.cueOuts.get(filename);

  if(cueOut !== undefined) {

    entry.cueOut = cueOut;
  }

  const cueOutCont = metadata.cueOutConts.get(filename);

  if(cueOutCont !== undefined) {

    entry.cueOutCont = cueOutCont;
  }

  return entry;
}

/**
 * Prunes stale entries from a SegmentMetadata instance. Removes entries for segments that are no longer in the active segment set (evicted from the sliding window).
 * This prevents unbounded growth over hours of streaming.
 *
 * @param meta - The metadata instance to prune.
 * @param activeSegments - Set of filenames currently in the segment store.
 */
function pruneMetadata(meta: SegmentMetadata, activeSegments: Set<string>): void {

  for(const key of meta.durations.keys()) {

    if(!activeSegments.has(key)) {

      meta.cueIns.delete(key);
      meta.cueOutConts.delete(key);
      meta.cueOuts.delete(key);
      meta.discontinuities.delete(key);
      meta.durations.delete(key);
      meta.timestamps.delete(key);
    }
  }
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

  // Preroll segment index offset. When preroll is ready (prerollSegmentCount > 0), real segments start numbering after the preroll range (e.g., segmentN.ts where
  // N = prerollSegmentCount). This offset is unconditional — it reserves the index space for preroll regardless of whether the deferred preroll timer fires.
  // The composite playlist behavior (including preroll entries) is determined dynamically by checking stream.hls.prerollStartTime at playlist generation time.
  const prerollSegmentCount = options.prerollSegmentCount ?? 0;

  // State.
  let pollTimer: ReturnType<typeof setTimeout> | null = null;
  let tokenRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;
  let segmentIndex = prerollSegmentCount;
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

  // Composite playlist discontinuity tracking. This is the composite path's independent source of truth for DISCONTINUITY-SEQUENCE computation. The Set records URLs
  // of entries that have had discontinuity=true in any composite playlist (upstream or the synthetic preroll boundary). It grows monotonically — once a discontinuity
  // is observed, it's tracked forever. On the first composite call, the Set is bootstrapped from videoMetadata to capture any upstream discontinuities that occurred
  // before the composite path activated. The offset accounts for historical discontinuities that pruned out of the metadata's Map but are preserved in its counter.
  // After bootstrap, the Set is self-sufficient. DISCONTINUITY-SEQUENCE = (offset + set.size) - windowDiscontinuities.
  const compositeDiscontinuities = new Set<string>();
  let compositeBaseOffset = 0;
  let compositeSeeded = false;

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

  // Per-segment metadata for the video variant. These Maps are keyed by local filename (e.g., "segment0.ts") and track the upstream manifest tags associated with
  // each stored segment. The playlist generator reads these to emit faithful HLS output with discontinuity markers, program timestamps, and ad signaling.
  const videoMetadata = createSegmentMetadata();

  // Audio-specific state for streams with separate audio renditions.
  let audioSegmentIndex = 0;
  let audioConsecutiveManifestFailures = 0;
  const audioSegmentTracker = { consecutiveFailures: 0, debugLabel: "Audio segment", label: "audio segment" };
  const audioFetchedSequences = new Set<number>();
  const audioMetadata = createSegmentMetadata();

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
      LOG.debug("native:manifest", "Variant manifest for %s:\n%s", channelName, body);

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

    // Prune old entries from the fetchedSequences Set and segment metadata. The provider's media sequence window slides forward, so entries below the current base
    // sequence will never be checked again. Without pruning, these structures grow unboundedly over hours of streaming.
    if(fetchedSequences.size > 100) {

      for(const seq of fetchedSequences) {

        if(seq < mediaSequence) {

          fetchedSequences.delete(seq);
        }
      }
    }

    if(videoMetadata.durations.size > 100) {

      const stream = getStream(streamId);
      const activeSegments = stream ? new Set(stream.hls.segments.keys()) : new Set<string>();

      pruneMetadata(videoMetadata, activeSegments);
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
      storeSegmentMetadata(videoMetadata, filename, seg);
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

    // Decrypt if this segment has a key URL. The manifest's #EXT-X-KEY tag is authoritative — DAI streams can switch between clear and AES-128 mid-stream (e.g., ad
    // pods encrypted while main content is clear), so the initial probe classification cannot be relied upon. Segments before the first #EXT-X-KEY tag have
    // segKeyUrl === null (clear).
    if(segKeyUrl) {

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

    if(audioMetadata.durations.size > 100) {

      const stream = getStream(streamId);
      const activeSegments = stream ? new Set(stream.hls.audioSegments.keys()) : new Set<string>();

      pruneMetadata(audioMetadata, activeSegments);
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
      storeSegmentMetadata(audioMetadata, filename, seg);
      storedAny = true;

      audioSegmentIndex++;
      totalSegmentsFetched++;

      LOG.debug("native:proxy", "Stored %s (%s bytes, seq %s) for %s.", filename, segmentData.length, seg.sequence, channelName);
    }

    return storedAny;
  }

  /**
   * Generates playlists from the stored segments. For streams without separate audio, generates a single variant playlist. When preroll is active (muxed audio only),
   * produces a composite playlist with fMP4 preroll entries and MPEG-TS real entries bridged by a DISCONTINUITY tag. The preroll entries use the same fMP4 segments
   * served during the standalone preroll phase, ensuring smooth MEDIA-SEQUENCE progression. For streams with separate audio, generates a master playlist referencing
   * video.m3u8 and audio.m3u8, plus individual variant playlists for each (no preroll — preroll is muxed and can't be split into separate renditions).
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

      if((prerollSegmentCount > 0) && stream.hls.prerollStartTime && stream.hls.prerollBaseUrl) {

        // Composite playlist with fMP4 preroll entries + MPEG-TS real entries. The prerollStartTime check ensures we only include preroll entries when the deferred
        // timer has fired and the client is actually watching preroll. Without this check, fast native streams (where real content arrives before the preroll delay)
        // would include unnecessary preroll entries. The compositor handles the sliding window with the maxPrerollInWindow cap.
        updatePlaylist(streamId, buildCompositePlaylist(segmentEntries, targetDuration, stream.hls.prerollBaseUrl));
      } else {

        // No preroll active — standard variant playlist. The segment index may still be offset (starting at prerollSegmentCount) to reserve the index space, but no
        // preroll entries are included.
        updatePlaylist(streamId, buildVariantPlaylist(segmentEntries, videoMetadata, "segment", targetDuration));
      }
    } else {

      // Separate audio — generate video variant playlist and master playlist. Preroll is not supported for separate audio streams (preroll is muxed).
      updateVideoPlaylist(streamId, buildVariantPlaylist(segmentEntries, videoMetadata, "segment", targetDuration));

      // Estimate bandwidth from stored segment sizes and durations. Sum total bytes and total duration across video segments, then convert to bits per second.
      // Falls back to 5 Mbps when no duration data is available (first segment before durations are populated).
      let bandwidth = 5000000;
      let totalBytes = 0;
      let totalDuration = 0;

      for(const filename of segmentEntries) {

        const size = stream.hls.segments.get(filename)?.length ?? 0;
        const duration = videoMetadata.durations.get(filename);

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
   * Builds a composite playlist with fMP4 preroll entries and MPEG-TS real entries. Uses the same compositor and builder as the capture path's generatePlaylist(),
   * ensuring identical windowing behavior (maxPrerollInWindow cap, progressive falloff). The DISCONTINUITY tag at the preroll-to-real boundary signals the container
   * format change (fMP4 → MPEG-TS), which is spec-compliant per RFC 8216 Section 4.3.3.3. VERSION:7 is used to support EXT-X-MAP for the preroll init segment;
   * after preroll entries fall off the window, VERSION:7 remains but is backward-compatible with the MPEG-TS entries.
   *
   * @param segmentEntries - Ordered list of stored segment filenames from the segment Map.
   * @param targetDuration - The #EXT-X-TARGETDURATION value from the provider's manifest.
   * @param prerollBaseUrl - The base URL for absolute preroll segment URIs, read dynamically from the stream's HLS state.
   * @returns The formatted composite m3u8 playlist string.
   */
  function buildCompositePlaylist(segmentEntries: string[], targetDuration: number, prerollBaseUrl: string): string {

    // Compute the sliding window start index via the compositor. The three-way max prevents negative indices, enforces the sliding window rule, and caps preroll
    // entries at maxPrerollInWindow to force clients past preroll toward the live edge.
    const realSegmentCount = segmentEntries.length;

    const startIndex = computePrerollWindow({

      currentSegmentIndex: segmentIndex,
      maxSegments: CONFIG.hls.maxSegments,
      prerollSegmentCount,
      realSegmentCount
    });

    // Build fMP4 preroll entries for preroll indices still in the window. These reference the global /preroll/ routes with absolute URLs and .m4s extension.
    let prerollEntries: PlaylistSegmentEntry[] = [];

    if(startIndex < prerollSegmentCount) {

      prerollEntries = buildPrerollEntries({ baseUrl: prerollBaseUrl, extension: ".m4s", prerollSegmentCount, startIndex });
    }

    // Build real MPEG-TS entries from the video metadata maps via the shared helper.
    const realEntries = segmentEntries.map((filename) => buildEntryFromMetadata(filename, videoMetadata, targetDuration));

    // Mark the preroll-to-real boundary on the first real entry when preroll entries are present in the window. This is a playlist-level concern (the stitching of
    // preroll before real content), not a segment-level property — so it's applied here in the composite builder rather than injected into videoMetadata at segment
    // storage time. This keeps the metadata clean (upstream discontinuities only) and avoids a stray DISCONTINUITY tag on fast streams where the composite never
    // activates.
    if((prerollEntries.length > 0) && (realEntries.length > 0)) {

      realEntries[0].discontinuity = true;
    }

    // Bootstrap the composite discontinuity tracker on the first call. This captures any upstream discontinuities that occurred before the composite path activated
    // (e.g., the proxy ran in non-composite mode while the preroll timer hadn't fired). The Map has filenames of currently-stored discontinuity segments; the counter
    // includes historical ones that pruned out of the Map. The difference becomes a fixed offset for segments we can never recover by filename.
    if(!compositeSeeded) {

      for(const filename of videoMetadata.discontinuities.keys()) {

        compositeDiscontinuities.add(filename);
      }

      compositeBaseOffset = videoMetadata.totalDiscontinuities - videoMetadata.discontinuities.size;
      compositeSeeded = true;
    }

    // Compute DISCONTINUITY-SEQUENCE using the composite path's independent discontinuity tracker. For each entry with discontinuity=true (whether from upstream
    // metadata or the synthetic preroll boundary), record its URL in the Set. The Set grows monotonically — once a discontinuity is observed, it's tracked forever.
    // DISCONTINUITY-SEQUENCE = total ever observed (offset + Set size) minus those visible in the current window.
    const entries = [ ...prerollEntries, ...realEntries ];

    let windowDiscontinuities = 0;

    for(const entry of entries) {

      if(entry.discontinuity) {

        compositeDiscontinuities.add(entry.url);
        windowDiscontinuities++;
      }
    }

    const discSeq = (compositeBaseOffset + compositeDiscontinuities.size) - windowDiscontinuities;
    const discontinuitySequence = (discSeq > 0) ? discSeq : undefined;

    // Determine the initial MAP URI. When the window starts with preroll entries, the preroll init segment (fMP4) is referenced. When the window has moved past all
    // preroll, no MAP is needed (MPEG-TS segments are self-describing). The DISCONTINUITY tag at the preroll-to-real boundary invalidates the MAP per RFC 8216
    // Section 4.3.3.3, so MPEG-TS entries after the boundary carry their codec config inline.
    const initialMapUri = (prerollEntries.length > 0) ? (prerollBaseUrl + "/preroll/init.mp4") : undefined;

    return buildPlaylist({

      discontinuitySequence,
      initialMapUri,
      mediaSequence: startIndex,
      targetDuration,
      version: 7
    }, entries);
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

    updateAudioPlaylist(streamId, buildVariantPlaylist(audioEntries, audioMetadata, "audio", targetDuration));
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
