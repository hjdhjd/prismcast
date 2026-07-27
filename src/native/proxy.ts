/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * proxy.ts: Native HLS proxy - manifest polling, segment fetching, and playlist generation.
 */
import { LOG, chromeFetch, realClock, startTimer } from "../utils/index.ts";
import { buildPrerollEntries, computePrerollWindow } from "../streaming/preroll.ts";
import { decryptSegment, deriveIvFromSequence, fetchDecryptionKey, parseExplicitIv } from "./decrypt.ts";
import { storeAudioSegment, storeSegment, updateAudioPlaylist, updatePlaylist, updateVideoPlaylist } from "../streaming/hlsSegments.ts";
import { CONFIG } from "../config/index.ts";
import type { CaptureCodec } from "../streaming/codec.ts";
import type { Clock } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import type { PlaylistSegmentEntry } from "../streaming/playlistBuilder.ts";
import { buildPlaylist } from "../streaming/playlistBuilder.ts";
import { getStream } from "../streaming/registry.ts";
import { resolveUrl } from "./probe.ts";

/* This module implements the native HLS proxy that replaces Chrome screen capture for viable streams. It polls the service's variant manifest at regular intervals,
 * detects new segments by tracking #EXT-X-MEDIA-SEQUENCE, fetches each segment (optionally decrypting AES-128), stores them in the existing HLS segment system, and
 * generates an HLS playlist that faithfully propagates upstream metadata - discontinuity markers, program timestamps, and SCTE-35 ad signaling (cue-out, cue-in,
 * cue-out-cont).
 *
 * The proxy generates its own playlist rather than rewriting the service's playlist, which avoids dealing with CDN-relative URLs and service-specific quirks. However,
 * it preserves all playback-critical tags from the upstream manifest so that downstream consumers (Channels DVR) can correctly handle PTS resets at ad boundaries,
 * synchronize wall-clock time, and detect commercial breaks.
 *
 * Video segments are stored as "segment0.ts", "segment1.ts", etc. For streams with separate audio renditions, audio segments are stored as "audio0.ts",
 * "audio1.ts", etc.
 */

// Timeout for segment fetches.
const SEGMENT_FETCH_TIMEOUT = 10000;

// Maximum consecutive manifest poll failures before reporting an error. Client errors (4xx) use this threshold directly. Server errors (5xx) and network timeouts
// use double the threshold to tolerate transient CDN issues that typically self-resolve within a few retry cycles.
const MAX_MANIFEST_FAILURES = 3;

// Maximum consecutive segment fetch failures before reporting an error. Segment fetches are smaller and far more numerous than manifest polls, so isolated
// transient failures (a dropped connection on one segment) are more common here; this higher tolerance than MAX_MANIFEST_FAILURES avoids escalating on
// blips that would otherwise self-resolve on the next fetch.
const MAX_SEGMENT_FAILURES = 5;

// Manifest poll backoff base delay and cap. On success, the poll interval returns to the base delay (a fixed 3000ms, ~half a typical 6s segment). On failure, the delay
// doubles on each consecutive failure up to the cap. Jitter of +/-20% prevents multiple streams from retrying in lockstep after a shared CDN outage.
const MANIFEST_BACKOFF_BASE = 3000;
const MANIFEST_BACKOFF_CAP = 15000;

/**
 * Computes the consecutive-failure threshold for a manifest poll based on the HTTP status of the most recent failure. Client errors (4xx) typically indicate
 * permanent issues (auth expiry, content removed) that won't self-resolve, so they use the base threshold directly. Server errors (5xx), network errors, and
 * timeouts are transient CDN conditions that usually recover within a few retry cycles, so they get double the attempts. A missing status (network error, DNS
 * failure, connection reset, timeout) is treated as transient. This is the single source of truth for the 4xx-versus-else threshold decision across every
 * manifest-poll call site - video and audio, success-path and catch-path failure handlers alike - so audio and video share identical escalation logic.
 *
 * @param status - The HTTP status code of the failed response, or undefined for network/timeout errors that never produced a response.
 * @returns The consecutive-failure threshold to compare against before reporting an error.
 */
export function manifestFailureThreshold(status?: number): number {

  const isClientError = (status !== undefined) && (status >= 400) && (status < 500);

  return isClientError ? MAX_MANIFEST_FAILURES : (MAX_MANIFEST_FAILURES * 2);
}

/**
 * Result of resolving a segment's initialization vector. A discriminated union so the malformed-IV rejection is a typed outcome the caller must branch on rather
 * than an overloaded null that could be confused with a successfully-derived IV. The "ok" variant carries the 16-byte IV to use; the "reject" variant signals that
 * the manifest declared an explicit IV that failed to parse and the segment must not be decrypted.
 */
export type SegmentIvResult = { iv: Buffer; status: "ok" } | { status: "reject" };

/**
 * Resolves the initialization vector for a segment from the manifest's explicit IV (when present) or the media sequence number (when absent), per RFC 8216
 * Section 5.2. When the manifest provides an explicit IV in the #EXT-X-KEY tag, it is authoritative and must parse cleanly: a malformed explicit IV is rejected
 * rather than silently substituted with the sequence-derived IV. Substituting would decrypt the segment with the wrong IV, yielding corrupted CBC output on the
 * first block - plausible-looking but garbage video served silently. Rejecting lets the caller count a fetch failure so recovery can escalate. The genuine absence
 * of an explicit IV (ivHex === null) is the only case that derives the IV from the sequence number.
 *
 * @param ivHex - The explicit IV hex string from the manifest, or null when the manifest declares no explicit IV.
 * @param sequence - The media sequence number used to derive the IV when no explicit IV is provided.
 * @returns An "ok" result carrying the resolved IV, or a "reject" result when an explicit IV was provided but failed to parse.
 */
export function resolveSegmentIv(ivHex: Nullable<string>, sequence: number): SegmentIvResult {

  if(ivHex !== null) {

    const explicitIv = parseExplicitIv(ivHex);

    if(!explicitIv) {

      return { status: "reject" };
    }

    return { iv: explicitIv, status: "ok" };
  }

  return { iv: deriveIvFromSequence(sequence), status: "ok" };
}

/**
 * Options for creating a native HLS proxy.
 */
export interface NativeProxyOptions {

  // URL of the separate audio rendition playlist, or null when audio is muxed into the video variant.
  audioVariantUrl: Nullable<string>;

  // The channel name for logging.
  channelName: string;

  // Optional clock for the polling cadence sleep. Defaults to realClock; tests inject a fake clock so the manifest poll loop's backoff resolves on demand
  // rather than via real timers. This mirrors the same default-arg port pattern used by retry.ts, timing.ts, and hlsSegments.ts - the production code path
  // is unchanged when callers omit it.
  clock?: Clock;

  // Encryption type classified by the probe.
  encryption: "aes128" | "clear";

  // AES-128 key URL from the probe result. Required when encryption is "aes128".
  keyUrl: Nullable<string>;

  // Callback invoked on errors for recovery orchestration.
  onError: (error: string) => void;

  // Pre-fetched AES-128 decryption key from the coordinator. When provided, the proxy uses this key directly instead of fetching it on the first segment.
  prefetchedKey: Nullable<Buffer>;

  // The preroll codec variant for composite playlist construction. Determines which preroll variant URLs and durations are used.
  prerollCodec?: CaptureCodec;

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

  // Returns the target segment duration in seconds from the service's manifest. Used by the monitor for staleness detection (2x threshold).
  getTargetDuration: () => number;

  // Starts the manifest polling loop.
  start: () => void;

  // Returns true if the proxy has been stopped.
  isStopped: () => boolean;

  // Sets the token refresh timer handle so it can be cancelled on stop. Called by the coordinator after scheduling a refresh.
  setTokenRefreshTimer: (timer: ReturnType<typeof setTimeout>) => void;

  // Stops the proxy and cancels the pending token refresh timer.
  stop: () => void;

  // TC39 explicit resource management hook, aliasing stop() so the proxy is a self-disposing node that composes uniformly with the other capture-mode resources
  // (the capture session and the health monitor). Its teardown is self-contained - it owns the polling loop and token-refresh timer and releases exactly those.
  [Symbol.dispose]: () => void;

  // Updates the audio variant URL after a token refresh. Only applicable for streams with separate audio renditions.
  updateAudioVariantUrl: (newUrl: string) => void;

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

// Proxy State Types.

/**
 * Segment fetch error tracker with labels for log and error messages. Used independently for video and audio segment fetch paths with separate failure thresholds.
 */
interface SegmentFetchTracker {

  consecutiveFailures: number;
  debugLabel: string;
  label: string;
}

/**
 * Core proxy lifecycle state. Controls whether the proxy is running, tracks readiness signaling, and manages poll timing.
 */
interface ProxyLifecycleState {

  errorThresholdReached: boolean;
  firstPollComplete: boolean;
  manifestBackoffMs: number;
  readinessSignaled: boolean;
  stopped: boolean;
  tokenRefreshTimer: ReturnType<typeof setTimeout> | null;
}

/**
 * Video segment and manifest tracking state. Tracks media sequence progression, segment storage indices, failure counters, and token refresh coordination for the
 * video variant.
 */
interface VideoTrackingState {

  activeKeyUrls: Set<string>;
  consecutiveManifestFailures: number;
  fetchedSequences: Set<number>;
  highWaterSequence: number;
  lastMediaSequence: number;
  lastSegmentSize: Nullable<number>;
  lastSegmentTime: number;
  lastTargetDuration: number;
  metadata: SegmentMetadata;
  segmentIndex: number;
  segmentTracker: SegmentFetchTracker;
  tokenRefreshPending: boolean;
  variantUrl: string;
}

/**
 * Audio segment and manifest tracking state. Mirrors the video tracking structure for streams with separate audio renditions. Includes the mutable audio variant URL
 * that changes on token refresh.
 */
interface AudioTrackingState {

  activeKeyUrls: Set<string>;
  consecutiveManifestFailures: number;
  fetchedSequences: Set<number>;
  highWaterSequence: number;
  lastTargetDuration: number;
  metadata: SegmentMetadata;
  segmentIndex: number;
  segmentTracker: SegmentFetchTracker;
  tokenRefreshPending: boolean;
  variantUrl: Nullable<string>;
}

/**
 * Composite playlist discontinuity tracking state. Independent source of truth for DISCONTINUITY-SEQUENCE computation in composite playlists that stitch fMP4 preroll
 * entries with MPEG-TS real entries.
 */
interface CompositePlaylistState {

  baseOffset: number;
  discontinuities: Set<string>;
  seeded: boolean;
}

/**
 * Cumulative statistics tracked by the proxy for health reporting and the termination summary.
 */
interface ProxyStatsState {

  totalFetchErrors: number;
  totalSegmentsFetched: number;
  totalTokenRefreshes: number;
}

/**
 * Shared context passed from the closure to extracted module-level functions. Bundles references that don't change over the proxy's lifetime (channelName, streamId,
 * onError callback) alongside shared mutable state objects (lifecycle, stats) that extracted functions need to read and write.
 */
interface ProxyContext {

  channelName: string;
  lifecycle: ProxyLifecycleState;
  onError: (error: string) => void;
  stats: ProxyStatsState;
  streamId: number;
}

/**
 * Callback type for the segment fetch function passed to extracted module-level functions. Wraps the closure-bound fetchTrackedSegment that handles HTTP fetch,
 * AES-128 decryption, and error tracking.
 */
type SegmentFetchFn = (tracker: SegmentFetchTracker, url: string, sequence: number, ivHex: Nullable<string>,
  segKeyUrl: Nullable<string>) => Promise<Nullable<Buffer>>;

/**
 * Options for building a composite playlist with fMP4 preroll entries and MPEG-TS real entries.
 */
interface CompositePlaylistOptions {

  composite: CompositePlaylistState;
  prerollBaseUrl: string;
  prerollCodec: CaptureCodec;
  prerollSegmentCount: number;
  segmentEntries: string[];
  segmentIndex: number;
  targetDuration: number;
  videoMetadata: SegmentMetadata;
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

  // Parse header tags: #EXT-X-MEDIA-SEQUENCE and #EXT-X-TARGETDURATION. The upstream #EXT-X-DISCONTINUITY-SEQUENCE is intentionally ignored - the proxy computes its
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

  // Pending metadata flags - accumulated from tags between segments and attached to the next #EXTINF.
  let pendingCueIn = false;
  let pendingCueOut: Nullable<string> = null;
  let pendingCueOutCont: Nullable<string> = null;
  let pendingDiscontinuity = false;
  let pendingProgramDateTime: Nullable<string> = null;

  for(let i = 0; i < lines.length; i++) {

    const trimmed = lines[i]?.trim() ?? "";

    if(trimmed.startsWith("#EXT-X-KEY:")) {

      const method = /METHOD=([A-Za-z0-9-]+)/.exec(trimmed)?.[1]?.toUpperCase() ?? "NONE";

      if(method === "AES-128") {

        const uri = /URI="([^"]+)"/.exec(trimmed)?.[1];

        if(uri) {

          currentManifestKeyUrl = resolveUrl(uri, baseUrl);
        }

        // A quoted attribute value - most often the URI - may legally contain commas and arbitrary text, including a literal "IV=" sequence, so no attribute regex
        // may scan across one. We blank every quoted span to an empty "" before scanning for the IV attribute, while the URI extraction above still reads the
        // original line. The [:,] boundary ties IV= to the start of an attribute, right after the tag colon or a separating comma, so a coincidental attribute-name
        // suffix such as FOOIV= cannot match, and the \s* tolerates the whitespace-after-comma layout some services emit. The capture takes the raw attribute value
        // and defers every format judgment - the 0x or 0X prefix and the 32-hex length - to parseExplicitIv, the single owner of IV format rules.
        const attributeResidue = trimmed.replace(/"[^"]*"/g, "\"\"");

        currentIvHex = /[:,]\s*IV=([^,\s]+)/.exec(attributeResidue)?.[1] ?? null;
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

    const duration = Number(trimmed.slice(8).split(",")[0] ?? 0);
    const segUrl = lines[i + 1]?.trim() ?? "";

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

  // Compute MEDIA-SEQUENCE from the first filename in the window. The filename encodes the local segment index (e.g., "segment5.ts" -> 5).
  let mediaSequence = 0;

  const firstEntry = segmentEntries[0];

  if(firstEntry) {

    mediaSequence = Number(firstEntry.replace(prefix, "").replace(".ts", ""));
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
 * Replaces the contents of a key-URL Set with the distinct AES-128 key URLs referenced by the segments of a freshly-parsed manifest. The Set is rebuilt from
 * scratch on each poll so it always reflects exactly the keys the current manifest window references - never a stale accumulation. The video and audio paths each
 * own one such Set; their union is the live working set the per-URL key cache prunes against at token-refresh boundaries, which bounds the cache to the keys the
 * stream is actually using rather than every key it has ever rotated through.
 *
 * @param target - The Set to rebuild in place.
 * @param segments - The parsed segments whose key URLs (when present) define the new active set.
 */
function refreshActiveKeyUrls(target: Set<string>, segments: ParsedSegment[]): void {

  target.clear();

  for(const seg of segments) {

    if(seg.keyUrl !== null) {

      target.add(seg.keyUrl);
    }
  }
}

/**
 * Evicts decryption keys whose URL is not in the active working set, mutating the cache in place. The cache is keyed by URL because each token rotation can
 * reference a different key URL; without this prune, the cache accumulates one dead entry per rotation indefinitely. The active set is the union of the URLs the
 * current video and audio manifests reference, so keys still in use survive while keys that rotated out of both manifests are released. Returns the number of
 * entries evicted so the caller can decide whether to log. Extracted as a pure module-level function (no closure capture) so the eviction invariant is unit-testable
 * in isolation from the polling loop.
 *
 * @param keysByUrl - The per-URL key cache to prune in place.
 * @param activeKeyUrls - The union of key URLs referenced by the current video and audio manifests.
 * @returns The number of cache entries evicted.
 */
export function pruneKeyCache(keysByUrl: Map<string, Buffer>, activeKeyUrls: ReadonlySet<string>): number {

  let evicted = 0;

  for(const cachedKeyUrl of keysByUrl.keys()) {

    if(!activeKeyUrls.has(cachedKeyUrl)) {

      keysByUrl.delete(cachedKeyUrl);
      evicted++;
    }
  }

  return evicted;
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

// Audio Stream Handler.

/**
 * Polls the audio variant manifest and fetches new audio segments. Combines the audio manifest poll and segment processing into a single entry point that mirrors the
 * video path. Only called for streams with separate audio renditions.
 *
 * @param ctx - Shared proxy context with lifecycle state, stats, channel name, and error callback.
 * @param audio - Audio-specific tracking state (sequence tracking, metadata, failure counters).
 * @param fetchSegment - Closure-bound segment fetch function that handles HTTP fetch, decryption, and error tracking.
 * @returns True if new audio segments were stored, false otherwise.
 */
async function pollAudioStream(ctx: ProxyContext, audio: AudioTrackingState, fetchSegment: SegmentFetchFn): Promise<boolean> {

  if(ctx.lifecycle.stopped || !audio.variantUrl) {

    return false;
  }

  try {

    const response = await chromeFetch(audio.variantUrl, { signal: AbortSignal.timeout(SEGMENT_FETCH_TIMEOUT) });

    if(!response.ok) {

      audio.consecutiveManifestFailures++;
      ctx.stats.totalFetchErrors++;

      // Classify the failure via the shared threshold helper so the audio path escalates identically to the video path: 4xx uses the base threshold, 5xx and
      // network conditions get double the attempts.
      const effectiveThreshold = manifestFailureThreshold(response.status);

      LOG.debug("native:proxy", "Audio manifest poll failed for %s: HTTP %s (%s/%s).",
        ctx.channelName, response.status, audio.consecutiveManifestFailures, effectiveThreshold);

      if(audio.consecutiveManifestFailures >= effectiveThreshold) {

        ctx.lifecycle.errorThresholdReached = true;
        ctx.lifecycle.stopped = true;
        ctx.onError("audio manifest poll failed " + String(audio.consecutiveManifestFailures) + " times");
      }

      return false;
    }

    audio.consecutiveManifestFailures = 0;

    const body = await response.text();

    return await processAudioStream(ctx, audio, body, audio.variantUrl, fetchSegment);
  } catch(error) {

    // Network errors (timeouts, DNS failures, connection resets) are transient - the threshold helper returns the doubled value for a missing status, matching the
    // video catch path so audio and video escalate identically.
    audio.consecutiveManifestFailures++;
    ctx.stats.totalFetchErrors++;

    const networkThreshold = manifestFailureThreshold();

    LOG.debug("native:proxy", "Audio manifest poll failed for %s: %s (%s/%s).", ctx.channelName, String(error), audio.consecutiveManifestFailures, networkThreshold);

    if(audio.consecutiveManifestFailures >= networkThreshold) {

      ctx.lifecycle.errorThresholdReached = true;
      ctx.lifecycle.stopped = true;
      ctx.onError("audio manifest poll error: " + String(error));
    }

    return false;
  }
}

/**
 * Parses an audio variant manifest and fetches new audio segments. Handles sequence tracking, high-water mark filtering, token refresh detection, and metadata storage.
 * Playlist generation is handled by the caller after both video and audio segments are stored.
 *
 * @param ctx - Shared proxy context.
 * @param audio - Audio-specific tracking state.
 * @param body - The audio variant manifest text content.
 * @param baseUrl - The audio variant URL for resolving relative segment URLs.
 * @param fetchSegment - Closure-bound segment fetch function.
 * @returns True if new segments were stored, false if the media sequence hadn't advanced.
 */
async function processAudioStream(ctx: ProxyContext, audio: AudioTrackingState, body: string, baseUrl: string,
  fetchSegment: SegmentFetchFn): Promise<boolean> {

  const { mediaSequence, segments, targetDuration } = parseVariantManifest(body, baseUrl);

  audio.lastTargetDuration = targetDuration;

  // Record the AES-128 key URLs the current audio manifest references so the per-URL key cache can prune against the live video+audio working set on the next
  // token refresh, rather than retaining a key per rotation indefinitely.
  refreshActiveKeyUrls(audio.activeKeyUrls, segments);

  // Prune old entries from audio fetchedSequences and audio metadata on each poll cycle.
  for(const seq of audio.fetchedSequences) {

    if(seq < mediaSequence) {

      audio.fetchedSequences.delete(seq);
    }
  }

  const audioPruneStream = getStream(ctx.streamId);
  const activeAudioSegments = audioPruneStream ? new Set(audioPruneStream.hls.audioSegments.keys()) : new Set<string>();

  pruneMetadata(audio.metadata, activeAudioSegments);

  // Filter audio segments using the same high-water mark and token refresh detection as the video path.
  let newSegments: ParsedSegment[];

  if(audio.highWaterSequence === -1) {

    newSegments = segments.slice(-CONFIG.hls.maxSegments);
  } else {

    newSegments = segments.filter((s) => (s.sequence > audio.highWaterSequence) && !audio.fetchedSequences.has(s.sequence));

    if((newSegments.length === 0) && audio.tokenRefreshPending && (segments.length > 0)) {

      LOG.debug("native:proxy", "Audio sequence timeline reset detected for %s after token refresh. Resetting high-water mark from %s.",
        ctx.channelName, audio.highWaterSequence);

      audio.highWaterSequence = -1;
      audio.tokenRefreshPending = false;
      newSegments = segments.slice(-CONFIG.hls.maxSegments);
    }
  }

  if((newSegments.length > 0) && audio.tokenRefreshPending) {

    audio.tokenRefreshPending = false;
  }

  if(newSegments.length === 0) {

    return false;
  }

  LOG.debug("native:proxy", "Fetching %s new audio segment(s) for %s (sequence %s).", newSegments.length, ctx.channelName, mediaSequence);

  let storedAny = false;

  for(const seg of newSegments) {

    if(ctx.lifecycle.stopped) {

      break;
    }

    // eslint-disable-next-line no-await-in-loop
    const segmentData = await fetchSegment(audio.segmentTracker, seg.url, seg.sequence, seg.ivHex, seg.keyUrl);

    if(!segmentData) {

      continue;
    }

    const filename = "audio" + String(audio.segmentIndex) + ".ts";

    storeAudioSegment(ctx.streamId, filename, segmentData);
    audio.fetchedSequences.add(seg.sequence);
    audio.highWaterSequence = Math.max(audio.highWaterSequence, seg.sequence);
    storeSegmentMetadata(audio.metadata, filename, seg);
    storedAny = true;

    audio.segmentIndex++;
    ctx.stats.totalSegmentsFetched++;

    LOG.debug("native:proxy", "Stored %s (%s bytes, seq %s) for %s.", filename, segmentData.length, seg.sequence, ctx.channelName);
  }

  return storedAny;
}

// Composite Playlist Builder.

/**
 * Builds a composite playlist with fMP4 preroll entries and MPEG-TS real entries. Uses the same compositor and builder as the capture path's generatePlaylist(),
 * ensuring identical windowing behavior (maxPrerollInWindow cap, progressive falloff). The DISCONTINUITY tag at the preroll-to-real boundary signals the container
 * format change (fMP4 -> MPEG-TS), which is spec-compliant per RFC 8216 Section 4.3.3.3. VERSION:7 is used to support EXT-X-MAP for the preroll init segment;
 * after preroll entries fall off the window, VERSION:7 remains but is backward-compatible with the MPEG-TS entries.
 *
 * @param options - Composite playlist configuration with segment data, preroll settings, and composite tracking state.
 * @returns The formatted composite m3u8 playlist string.
 */
function buildCompositePlaylist(options: CompositePlaylistOptions): string {

  const { composite, prerollBaseUrl, prerollCodec, prerollSegmentCount, segmentEntries, segmentIndex, targetDuration, videoMetadata } = options;

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

    prerollEntries = buildPrerollEntries({ baseUrl: prerollBaseUrl, codec: prerollCodec, extension: ".m4s", prerollSegmentCount, startIndex });
  }

  // Build real MPEG-TS entries from the video metadata maps via the shared helper.
  const realEntries = segmentEntries.map((filename) => buildEntryFromMetadata(filename, videoMetadata, targetDuration));

  // Mark the preroll-to-real boundary on the first real entry when preroll entries are present in the window. This is a playlist-level concern (the stitching of
  // preroll before real content), not a segment-level property - so it's applied here in the composite builder rather than injected into videoMetadata at segment
  // storage time. This keeps the metadata clean (upstream discontinuities only) and avoids a stray DISCONTINUITY tag on fast streams where the composite never
  // activates.
  const firstRealEntry = realEntries[0];

  if((prerollEntries.length > 0) && firstRealEntry) {

    firstRealEntry.discontinuity = true;
  }

  // Bootstrap the composite discontinuity tracker on the first call. This captures any upstream discontinuities that occurred before the composite path activated
  // (e.g., the proxy ran in non-composite mode while the preroll timer hadn't fired). The Map has filenames of currently-stored discontinuity segments; the counter
  // includes historical ones that pruned out of the Map. The difference becomes a fixed offset for segments we can never recover by filename.
  if(!composite.seeded) {

    for(const filename of videoMetadata.discontinuities.keys()) {

      composite.discontinuities.add(filename);
    }

    composite.baseOffset = videoMetadata.totalDiscontinuities - videoMetadata.discontinuities.size;
    composite.seeded = true;
  }

  // Compute DISCONTINUITY-SEQUENCE using the composite path's independent discontinuity tracker. For each entry with discontinuity=true (whether from upstream
  // metadata or the synthetic preroll boundary), record its URL in the Set. The Set grows monotonically - once a discontinuity is observed, it's tracked forever.
  // DISCONTINUITY-SEQUENCE = total ever observed (offset + Set size) minus those visible in the current window.
  const entries = [ ...prerollEntries, ...realEntries ];

  let windowDiscontinuities = 0;

  for(const entry of entries) {

    if(entry.discontinuity) {

      composite.discontinuities.add(entry.url);
      windowDiscontinuities++;
    }
  }

  const discSeq = (composite.baseOffset + composite.discontinuities.size) - windowDiscontinuities;
  const discontinuitySequence = (discSeq > 0) ? discSeq : undefined;

  // Determine the initial MAP URI. When the window starts with preroll entries, the preroll init segment (fMP4) is referenced. When the window has moved past all
  // preroll, no MAP is needed (MPEG-TS segments are self-describing). The DISCONTINUITY tag at the preroll-to-real boundary invalidates the MAP per RFC 8216
  // Section 4.3.3.3, so MPEG-TS entries after the boundary carry their codec config inline.
  const initialMapUri = (prerollEntries.length > 0) ? (prerollBaseUrl + "/preroll/" + prerollCodec + "/init.mp4") : undefined;

  return buildPlaylist({

    discontinuitySequence,
    initialMapUri,
    mediaSequence: startIndex,
    targetDuration,
    version: 7
  }, entries);
}

/**
 * Creates a native HLS proxy that polls a variant manifest, fetches segments, and generates playlists.
 *
 * @param options - Proxy configuration.
 * @returns The native proxy interface.
 */
export function createNativeProxy(options: NativeProxyOptions): NativeProxy {

  const { channelName, clock = realClock, encryption, keyUrl, onError, streamId } = options;
  const hasAudio = options.audioVariantUrl !== null;

  // Preroll segment index offset. When preroll is ready (prerollSegmentCount > 0), real segments start numbering after the preroll range (e.g., segmentN.ts where
  // N = prerollSegmentCount). This offset is unconditional - it reserves the index space for preroll regardless of whether the deferred preroll timer fires.
  // The composite playlist behavior (including preroll entries) is determined dynamically by checking stream.hls.prerollStartTime at playlist generation time.
  const prerollCodec: CaptureCodec = options.prerollCodec ?? "h264";
  const prerollSegmentCount = options.prerollSegmentCount ?? 0;

  /* Proxy state. These track segment storage, manifest polling, error thresholds, and playlist generation across the proxy's lifetime. Mutable variables are organized
   * into typed state objects by subsystem (lifecycle, video tracking, audio tracking, composite playlist, statistics) to clarify ownership and interaction boundaries.
   * The ProxyContext bundles immutable references with shared mutable state for extracted module-level functions.
   */

  // Proxy lifecycle state. Controls whether the proxy is running, tracks readiness signaling, and manages poll timing.
  const lifecycle: ProxyLifecycleState = {

    errorThresholdReached: false,
    firstPollComplete: false,
    manifestBackoffMs: MANIFEST_BACKOFF_BASE,
    readinessSignaled: false,
    stopped: false,
    tokenRefreshTimer: null
  };

  // Video segment and manifest tracking state.
  const video: VideoTrackingState = {

    activeKeyUrls: new Set<string>(),
    consecutiveManifestFailures: 0,
    fetchedSequences: new Set<number>(),
    highWaterSequence: -1,
    lastMediaSequence: -1,
    lastSegmentSize: null,
    lastSegmentTime: 0,
    lastTargetDuration: 6,
    metadata: createSegmentMetadata(),
    segmentIndex: prerollSegmentCount,
    segmentTracker: { consecutiveFailures: 0, debugLabel: "Segment", label: "segment" },
    tokenRefreshPending: false,
    variantUrl: options.variantUrl
  };

  // Audio segment and manifest tracking state for streams with separate audio renditions.
  const audio: AudioTrackingState = {

    activeKeyUrls: new Set<string>(),
    consecutiveManifestFailures: 0,
    fetchedSequences: new Set<number>(),
    highWaterSequence: -1,
    lastTargetDuration: 6,
    metadata: createSegmentMetadata(),
    segmentIndex: 0,
    segmentTracker: { consecutiveFailures: 0, debugLabel: "Audio segment", label: "audio segment" },
    tokenRefreshPending: false,
    variantUrl: options.audioVariantUrl
  };

  // Composite playlist discontinuity tracking. This is the composite path's independent source of truth for DISCONTINUITY-SEQUENCE computation. The Set records URLs
  // of entries that have had discontinuity=true in any composite playlist (upstream or the synthetic preroll boundary). It grows monotonically - once a discontinuity
  // is observed, it's tracked forever. On the first composite call, the Set is bootstrapped from video.metadata to capture any upstream discontinuities that occurred
  // before the composite path activated. The offset accounts for historical discontinuities that pruned out of the metadata's Map but are preserved in its counter.
  // After bootstrap, the Set is self-sufficient. DISCONTINUITY-SEQUENCE = (offset + set.size) - windowDiscontinuities.
  const composite: CompositePlaylistState = {

    baseOffset: 0,
    discontinuities: new Set<string>(),
    seeded: false
  };

  // Cumulative statistics.
  const stats: ProxyStatsState = {

    totalFetchErrors: 0,
    totalSegmentsFetched: 0,
    totalTokenRefreshes: 0
  };

  // Shared context for extracted module-level functions. Bundles immutable references with shared mutable state objects.
  const ctx: ProxyContext = { channelName, lifecycle, onError, stats, streamId };

  // AES-128 decryption key cache. Maps key URLs to their fetched 16-byte keys. Each segment in a manifest can reference a different key URL (key rotation), so we
  // cache by URL rather than maintaining a single "current key". The coordinator pre-fetches the initial key, which is seeded into the cache here.
  const keysByUrl = new Map<string, Buffer>();

  if(options.prefetchedKey && keyUrl) {

    keysByUrl.set(keyUrl, options.prefetchedKey);
  }

  /**
   * Prunes the per-URL decryption key cache against this stream's live working set - the union of the key URLs the most recently polled video and audio manifests
   * reference - and logs the eviction count. Called at each token-refresh boundary, this bounds the cache to the keys the stream is actually using (typically one or
   * two) rather than retaining one dead entry per rotation. Refresh is the natural point at which the prior CDN session's keys become unreachable, so it runs here
   * rather than on every poll. The eviction itself lives in the pure module-level pruneKeyCache so the invariant is unit-testable.
   */
  function pruneStreamKeyCache(): void {

    const activeKeyUrls = new Set<string>([ ...video.activeKeyUrls, ...audio.activeKeyUrls ]);
    const evicted = pruneKeyCache(keysByUrl, activeKeyUrls);

    if(evicted > 0) {

      LOG.debug("native:proxy", "Pruned %s stale decryption key(s) for %s (%s active).", evicted, channelName, keysByUrl.size);
    }
  }

  /**
   * Computes the next retry delay with exponential backoff and jitter. Doubles the current backoff (capped at MANIFEST_BACKOFF_CAP) and applies +/-20% jitter.
   * @returns The jittered delay in milliseconds.
   */
  function nextBackoffDelay(): number {

    lifecycle.manifestBackoffMs = Math.min(lifecycle.manifestBackoffMs * 2, MANIFEST_BACKOFF_CAP);

    const jitter = 0.8 + (Math.random() * 0.4);

    return Math.round(lifecycle.manifestBackoffMs * jitter);
  }

  /**
   * Orchestrates the poll cycle: fetches video and audio manifests in parallel, stores segments, and generates playlists after both stores are updated.
   */
  async function pollManifest(): Promise<void> {

    if(lifecycle.stopped) {

      return;
    }

    const pollElapsed = startTimer();

    try {

      const response = await chromeFetch(video.variantUrl, { signal: AbortSignal.timeout(SEGMENT_FETCH_TIMEOUT) });

      if(!response.ok) {

        video.consecutiveManifestFailures++;
        stats.totalFetchErrors++;

        // Classify the error via the shared threshold helper. Client errors (4xx) use the base threshold; server errors (5xx) and network conditions get double the
        // attempts. This is the same logic the audio poll path uses, so video and audio escalate identically.
        const isClientError = (response.status >= 400) && (response.status < 500);
        const effectiveThreshold = manifestFailureThreshold(response.status);

        LOG.debug("native:proxy", "Manifest poll failed for %s: HTTP %s (%s, %s/%s).",
          channelName, response.status, isClientError ? "client" : "server", video.consecutiveManifestFailures, effectiveThreshold);

        if(video.consecutiveManifestFailures >= effectiveThreshold) {

          lifecycle.errorThresholdReached = true;
          lifecycle.stopped = true;
          onError("manifest poll failed " + String(video.consecutiveManifestFailures) + " times (HTTP " + String(response.status) + ")");

          return;
        }

        schedulePoll(nextBackoffDelay());

        return;
      }

      video.consecutiveManifestFailures = 0;
      lifecycle.manifestBackoffMs = MANIFEST_BACKOFF_BASE;

      const body = await response.text();

      LOG.debug("native:proxy", "Manifest poll for %s completed in %sms.", channelName, pollElapsed());
      LOG.debug("native:manifest", "Variant manifest for %s:\n%s", channelName, body);

      // Fetch and store video and audio segments in parallel so neither delays the other, and generate playlists after both stores are updated. This ensures video
      // and audio playlists are published atomically - no window where a client sees a master playlist referencing audio.m3u8 before audio segments exist.
      let hasNewVideoSegments: boolean;
      let hasNewAudioSegments = false;

      if(hasAudio) {

        [ hasNewVideoSegments, hasNewAudioSegments ] = await Promise.all([ processManifest(body), pollAudioStream(ctx, audio, fetchTrackedSegment) ]);
      } else {

        hasNewVideoSegments = await processManifest(body);
      }

      if(hasNewVideoSegments || hasNewAudioSegments) {

        generatePlaylist(video.lastTargetDuration);

        if(hasAudio) {

          generateAudioPlaylist(audio.lastTargetDuration);
        }
      }

      // Set firstPollComplete when segments are available. For streams with separate audio, require both video and audio segments before signaling readiness to
      // prevent publishing a master playlist before both variant playlists have content.
      if(!lifecycle.firstPollComplete && (video.segmentIndex > 0) && (!hasAudio || (audio.segmentIndex > 0))) {

        lifecycle.firstPollComplete = true;
      }

      // Signal playlist readiness after the first poll completes. Note: initSegmentReady is signaled immediately when the proxy starts (in hls.ts) since native
      // MPEG-TS has no separate init segment.
      if(lifecycle.firstPollComplete && !lifecycle.readinessSignaled) {

        lifecycle.readinessSignaled = true;

        const stream = getStream(streamId);

        if(stream) {

          stream.hls.signalPlaylistReady();
        }

        LOG.debug("native:proxy", "First poll cycle complete for %s. Segment index: %s.", channelName, video.segmentIndex);
      }
    } catch(error) {

      // Network errors (timeouts, DNS failures, connection resets) are transient - use the extended threshold and backoff. The threshold helper returns the doubled
      // value for a missing status, which is exactly the network-error case here.
      video.consecutiveManifestFailures++;
      stats.totalFetchErrors++;

      const networkThreshold = manifestFailureThreshold();

      LOG.debug("native:proxy", "Manifest poll failed for %s: %s (%s/%s).",
        channelName, String(error), video.consecutiveManifestFailures, networkThreshold);

      if(video.consecutiveManifestFailures >= networkThreshold) {

        lifecycle.errorThresholdReached = true;
        lifecycle.stopped = true;
        onError("manifest poll error: " + String(error));

        return;
      }

      schedulePoll(nextBackoffDelay());

      return;
    }

    // Schedule the next poll at MANIFEST_BACKOFF_BASE (3000ms, ~half a typical 6s segment) for timely detection of new segments.
    lifecycle.manifestBackoffMs = MANIFEST_BACKOFF_BASE;

    schedulePoll(MANIFEST_BACKOFF_BASE);
  }

  /**
   * Parses a variant manifest and fetches new segments. Playlist generation is handled by the caller after both video and audio segments are stored.
   *
   * @param body - The variant manifest text content.
   * @returns True if new segments were stored, false if the media sequence hadn't advanced.
   */
  async function processManifest(body: string): Promise<boolean> {

    const { mediaSequence, segments, targetDuration } = parseVariantManifest(body, video.variantUrl);

    video.lastTargetDuration = targetDuration;

    // Record the AES-128 key URLs the current video manifest references so the per-URL key cache can prune against the live video+audio working set on the next
    // token refresh, rather than retaining a key per rotation indefinitely.
    refreshActiveKeyUrls(video.activeKeyUrls, segments);

    // Prune old entries from the fetchedSequences Set and segment metadata on each poll cycle. The service's media sequence window slides forward, so entries below
    // the current base sequence will never be checked again. Without pruning, these structures grow unboundedly over hours of streaming.
    for(const seq of video.fetchedSequences) {

      if(seq < mediaSequence) {

        video.fetchedSequences.delete(seq);
      }
    }

    const pruneStream = getStream(streamId);
    const activeVideoSegments = pruneStream ? new Set(pruneStream.hls.segments.keys()) : new Set<string>();

    pruneMetadata(video.metadata, activeVideoSegments);

    // Filter segments to only those that advance the live edge. On the first poll (highWaterSequence === -1), we have no baseline yet, so we take the last
    // maxSegments entries to fill the initial playlist window. On subsequent polls, we only fetch segments with sequence numbers above the high-water mark. This
    // filters out DAI backfill segments from interleaved session windows - segments that are "new" (not in fetchedSequences) but have lower sequence numbers than
    // what we've already served, causing PTS discontinuities and visible playback glitches.
    let newSegments: ParsedSegment[];

    if(video.highWaterSequence === -1) {

      // First poll - no baseline. Take the tail of the manifest to fill the playlist window.
      newSegments = segments.slice(-CONFIG.hls.maxSegments);
    } else {

      // Subsequent polls - only fetch segments that advance past the high-water mark.
      newSegments = segments.filter((s) => (s.sequence > video.highWaterSequence) && !video.fetchedSequences.has(s.sequence));

      // Detect CDN sequence timeline reset after a token refresh. Some CDNs (e.g., Fox Sports) create a new session with sequence numbers starting at 0 when
      // tokens are refreshed. If the first poll after a refresh produces zero segments above the high-water mark, the timeline has genuinely reset - clear the
      // high-water mark and re-process the same manifest with the tail-fill strategy.
      if((newSegments.length === 0) && video.tokenRefreshPending && (segments.length > 0)) {

        LOG.debug("native:proxy", "Sequence timeline reset detected for %s after token refresh. Resetting high-water mark from %s.",
          channelName, video.highWaterSequence);

        video.highWaterSequence = -1;
        video.tokenRefreshPending = false;
        newSegments = segments.slice(-CONFIG.hls.maxSegments);
      }
    }

    // Clear the token refresh flag on the first poll that produces segments, whether via normal filtering or after a reset detection.
    if((newSegments.length > 0) && video.tokenRefreshPending) {

      video.tokenRefreshPending = false;
    }

    if(newSegments.length === 0) {

      // No new segments - the media sequence hasn't advanced.
      if(video.lastMediaSequence === mediaSequence) {

        LOG.debug("native:proxy", "No new segments for %s (sequence still %s).", channelName, mediaSequence);
      }

      video.lastMediaSequence = mediaSequence;

      return false;
    }

    LOG.debug("native:proxy", "Fetching %s new segment(s) for %s (sequence %s).", newSegments.length, channelName, mediaSequence);

    video.lastMediaSequence = mediaSequence;

    // Fetch and store each new segment sequentially. Track whether at least one segment was stored so the caller only generates playlists when data actually changed.
    let storedAny = false;

    for(const seg of newSegments) {

      if(lifecycle.stopped) {

        break;
      }

      // eslint-disable-next-line no-await-in-loop
      const segmentData = await fetchTrackedSegment(video.segmentTracker, seg.url, seg.sequence, seg.ivHex, seg.keyUrl);

      if(!segmentData) {

        continue;
      }

      const filename = "segment" + String(video.segmentIndex) + ".ts";

      // Check segment count before store to detect rotation (oldest segment evicted to enforce maxSegments limit).
      const stream = getStream(streamId);
      const countBefore = stream?.hls.segments.size ?? 0;

      storeSegment(streamId, filename, segmentData);
      video.fetchedSequences.add(seg.sequence);
      video.highWaterSequence = Math.max(video.highWaterSequence, seg.sequence);
      storeSegmentMetadata(video.metadata, filename, seg);
      storedAny = true;

      video.lastSegmentSize = segmentData.length;
      video.lastSegmentTime = Date.now();
      video.segmentIndex++;
      stats.totalSegmentsFetched++;

      // Log the first segment fetch latency for timing diagnostics.
      if(stats.totalSegmentsFetched === 1) {

        LOG.debug("timing:native", "First segment fetched for %s (%s bytes).", channelName, segmentData.length);
      }

      // Detect segment rotation - if the count didn't increase, the oldest segment was evicted.
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

    const response = await chromeFetch(url, { signal: AbortSignal.timeout(SEGMENT_FETCH_TIMEOUT) });

    if(!response.ok) {

      LOG.debug("native:proxy", "Segment fetch failed for %s: HTTP %s.", channelName, response.status);

      return null;
    }

    let data: Buffer = Buffer.from(await response.arrayBuffer());

    // Decrypt if this segment has a key URL. The manifest's #EXT-X-KEY tag is authoritative - DAI streams can switch between clear and AES-128 mid-stream (e.g., ad
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

      // Determine the IV via the shared resolver. A malformed explicit IV is rejected rather than silently substituted with the sequence-derived IV, which would
      // decrypt the segment with the wrong IV and serve plausible-looking but garbage video. Rejecting returns null here so the fetch tracker counts a failure and
      // recovery can escalate.
      const ivResult = resolveSegmentIv(ivHex, sequence);

      if(ivResult.status === "reject") {

        LOG.warn("Rejecting segment for %s: the manifest provided a malformed explicit IV.", channelName, { ivHex, sequence });

        return null;
      }

      LOG.debug("native:decrypt", "IV source for sequence %s: %s.", sequence, (ivHex !== null) ? "explicit" : "sequence");

      data = decryptSegment(data, key, ivResult.iv);
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
  async function fetchTrackedSegment(tracker: SegmentFetchTracker, url: string, sequence: number,
    ivHex: Nullable<string>, segKeyUrl: Nullable<string>): Promise<Nullable<Buffer>> {

    try {

      const data = await fetchAndDecryptSegment(url, sequence, ivHex, segKeyUrl);

      if(!data) {

        tracker.consecutiveFailures++;
        stats.totalFetchErrors++;

        if(tracker.consecutiveFailures >= MAX_SEGMENT_FAILURES) {

          lifecycle.errorThresholdReached = true;
          lifecycle.stopped = true;
          onError(tracker.label + " fetch failed " + String(tracker.consecutiveFailures) + " times");
        }

        return null;
      }

      tracker.consecutiveFailures = 0;

      return data;
    } catch(error) {

      tracker.consecutiveFailures++;
      stats.totalFetchErrors++;

      LOG.debug("native:proxy", "%s fetch failed for %s: %s.", tracker.debugLabel, channelName, String(error));

      if(tracker.consecutiveFailures >= MAX_SEGMENT_FAILURES) {

        lifecycle.errorThresholdReached = true;
        lifecycle.stopped = true;
        onError(tracker.label + " fetch error: " + String(error));
      }

      return null;
    }
  }

  /**
   * Generates playlists from the stored segments. For streams without separate audio, generates a single variant playlist. When preroll is active (muxed audio only),
   * produces a composite playlist with fMP4 preroll entries and MPEG-TS real entries bridged by a DISCONTINUITY tag. The preroll entries use the same fMP4 segments
   * served during the standalone preroll phase, ensuring smooth MEDIA-SEQUENCE progression. For streams with separate audio, generates the video variant playlist
   * and the master playlist referencing video.m3u8 and audio.m3u8 (no preroll - preroll is muxed and can't be split into separate renditions); the audio variant
   * playlist is produced separately by generateAudioPlaylist(), which the poll loop calls alongside this function.
   *
   * @param targetDuration - The #EXT-X-TARGETDURATION value from the service's manifest.
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
        updatePlaylist(streamId, buildCompositePlaylist({

          composite,
          prerollBaseUrl: stream.hls.prerollBaseUrl,
          prerollCodec,
          prerollSegmentCount,
          segmentEntries,
          segmentIndex: video.segmentIndex,
          targetDuration,
          videoMetadata: video.metadata
        }));
      } else {

        // No preroll active - standard variant playlist. The segment index may still be offset (starting at prerollSegmentCount) to reserve the index space, but no
        // preroll entries are included.
        updatePlaylist(streamId, buildVariantPlaylist(segmentEntries, video.metadata, "segment", targetDuration));
      }
    } else {

      // Separate audio - generate video variant playlist and master playlist. Preroll is not supported for separate audio streams (preroll is muxed).
      updateVideoPlaylist(streamId, buildVariantPlaylist(segmentEntries, video.metadata, "segment", targetDuration));

      // Estimate bandwidth from stored segment sizes and durations. Sum total bytes and total duration across video segments, then convert to bits per second.
      // Falls back to 5 Mbps when no duration data is available (first segment before durations are populated).
      let bandwidth = 5000000;
      let totalBytes = 0;
      let totalDuration = 0;

      for(const filename of segmentEntries) {

        const size = stream.hls.segments.get(filename)?.length ?? 0;
        const duration = video.metadata.durations.get(filename);

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

    updateAudioPlaylist(streamId, buildVariantPlaylist(audioEntries, audio.metadata, "audio", targetDuration));
  }

  /**
   * Schedules the next manifest poll after a delay. The sleep is awaited through the injected Clock port (defaulting to realClock) so tests can virtualize the
   * polling cadence without depending on real timers. Cancellation semantics: stop() flips lifecycle.stopped, and the post-sleep guard catches that flip before
   * issuing the next poll. The in-flight sleep itself is not cancelled - in production a stopped proxy lingers for at most the currently scheduled poll delay,
   * which can be as long as MANIFEST_BACKOFF_CAP once jittered, before the awaiter wakes and exits cleanly. This is the same shape as retryOperation in
   * utils/retry.ts which already adopted the Clock port for the same reason.
   *
   * @param delayMs - Delay in milliseconds before the next poll.
   */
  function schedulePoll(delayMs: number): void {

    if(lifecycle.stopped) {

      return;
    }

    void (async (): Promise<void> => {

      await clock.sleep(delayMs);

      if(lifecycle.stopped) {

        return;
      }

      void pollManifest();
    })();
  }

  // The proxy's teardown: flip the lifecycle flag the polling awaiter checks, and cancel the pending token-refresh timer. Defined as a const so it can be exposed
  // both as stop() (the established domain verb) and as [Symbol.dispose] (the TC39 protocol), making the proxy a self-disposing node without duplicating the body.
  const stop = (): void => {

    lifecycle.stopped = true;

    // The polling cadence sleep is owned by schedulePoll's awaiter and is not cancelled here - the awaiter checks lifecycle.stopped after clock.sleep resolves
    // and exits before issuing the next poll. See schedulePoll's docblock for the cancellation contract.

    // Cancel the pending token refresh timer to prevent fire-after-termination. Without this, the timer fires on a stopped proxy and attempts to navigate a
    // potentially closed or reused page.
    if(lifecycle.tokenRefreshTimer) {

      clearTimeout(lifecycle.tokenRefreshTimer);
      lifecycle.tokenRefreshTimer = null;
    }

    LOG.debug("native:proxy", "Stopped native proxy for %s.", channelName);
  };

  return {

    getConsecutiveErrors: (): number => video.segmentTracker.consecutiveFailures + video.consecutiveManifestFailures +
      audio.segmentTracker.consecutiveFailures + audio.consecutiveManifestFailures,

    getLastSegmentSize: (): Nullable<number> => video.lastSegmentSize,

    getLastSegmentTime: (): number => video.lastSegmentTime,

    getSegmentIndex: (): number => video.segmentIndex,

    getStats: (): NativeProxyStats => ({

      fetchErrors: stats.totalFetchErrors,
      segmentsFetched: stats.totalSegmentsFetched,
      tokenRefreshes: stats.totalTokenRefreshes
    }),

    getTargetDuration: (): number => video.lastTargetDuration,

    hasErrored: (): boolean => lifecycle.errorThresholdReached,

    isStopped: (): boolean => lifecycle.stopped,

    setTokenRefreshTimer: (timer: ReturnType<typeof setTimeout>): void => {

      lifecycle.tokenRefreshTimer = timer;
    },

    start: (): void => {

      LOG.debug("native:proxy", "Starting native proxy for %s (%s).", channelName, encryption);
      void pollManifest();
    },

    stop,

    updateAudioVariantUrl: (newUrl: string): void => {

      audio.variantUrl = newUrl;
      audio.fetchedSequences.clear();
      audio.tokenRefreshPending = true;

      LOG.debug("native:proxy", "Audio variant URL updated for %s.", channelName);
    },

    updateVariantUrl: (newUrl: string): void => {

      video.variantUrl = newUrl;
      stats.totalTokenRefreshes++;

      // Clear the fetched set so sequence numbers from the old CDN session don't cause segments in the new session to be incorrectly filtered as "already fetched."
      // The high-water mark is intentionally preserved - it's the same live stream with the same sequence timeline, just with fresh auth tokens. Resetting it would
      // allow the proxy to re-fetch segments the client already consumed, causing PTS discontinuities and visible playback glitches. The tokenRefreshPending flag
      // enables processManifest to detect genuine sequence timeline resets (e.g., Fox Sports) on the first poll after refresh.
      video.fetchedSequences.clear();
      video.lastMediaSequence = -1;
      video.tokenRefreshPending = true;

      // Evict decryption keys the prior CDN session referenced but the live manifests no longer do. A token rotation typically retires the old session's key URL,
      // so without this the per-URL key cache would grow by one dead entry per refresh over the stream's lifetime.
      pruneStreamKeyCache();

      LOG.debug("native:proxy", "Variant URL updated for %s. Segment tracking reset.", channelName);
    },

    // TC39 explicit resource management hook aliasing stop(), so the proxy is a self-disposing node consumable via the protocol (a DisposableStack or "using"). The
    // explicit teardown in disposeStreamResources calls stop() directly, following the codebase convention that explicit call sites use the readable verb.
    [Symbol.dispose]: stop
  };
}
