/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * registry.ts: Stream tracking for PrismCast.
 */
import type { MediaContainer, Nullable, ResolvedSiteProfile } from "../types/index.ts";
import type { CaptureCodec } from "./codec.ts";
import type { CaptureSession } from "./captureSession.ts";
import { EventEmitter } from "node:events";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.ts";
import type { ManifestInterceptionResult } from "../browser/manifestInterceptor.ts";
import type { MonitorHandle } from "./recovery.ts";
import type { NativeProxy } from "../native/proxy.ts";
import type { Page } from "puppeteer-core";
import type { ProbeCacheIdentity } from "../native/probe.ts";
import type { RefreshedFeedMetadata } from "../native/index.ts";

/* The stream registry is the single source of truth for all active streaming sessions. Each stream is tracked in a single StreamRegistryEntry containing browser
 * state, HLS segment storage, and the segmenter reference. This consolidation prevents data desync issues that could occur with separate Maps for each concern. The
 * registry enables the /streams endpoint to list all active streams, the /health endpoint to report stream counts, graceful shutdown to terminate all streams,
 * browser disconnect handling to clean up orphaned streams, and concurrent stream limit enforcement.
 */

// Types.

/**
 * Typed event map for segment notifications. MPEG-TS consumers and lifecycle handlers subscribe to these events. Using a typed interface ensures event names and
 * argument types are checked at compile time, preventing misspelled event names or incorrect payloads.
 */
interface SegmentEmitterEventMap {

  audioSegment: [filename: string, data: Buffer];
  initSegment: [data: Buffer];
  segment: [filename: string, data: Buffer];
  terminated: [];
}

/**
 * Typed EventEmitter for segment notifications. Narrows Node's untyped EventEmitter to only accept the events defined in SegmentEmitterEventMap.
 */
export interface SegmentEmitter extends EventEmitter {

  emit<K extends keyof SegmentEmitterEventMap>(event: K, ...args: SegmentEmitterEventMap[K]): boolean;
  off<K extends keyof SegmentEmitterEventMap>(event: K, listener: (...args: SegmentEmitterEventMap[K]) => void): this;
  on<K extends keyof SegmentEmitterEventMap>(event: K, listener: (...args: SegmentEmitterEventMap[K]) => void): this;
}

/**
 * The two tracks a native fMP4 relay stores initialization segments for. Declared once here, beside the state that holds those segments, so every consuming
 * signature names the type rather than restating the literal pair.
 */
export type InitSegmentTrack = "audio" | "video";

/**
 * HLS segment and playlist storage for a stream. This includes the fMP4 initialization segment (codec configuration), media segments (.m4s files), and the current
 * playlist content. The playlistReady promise allows callers to wait for the first playlist to be generated.
 *
 * Note: HLSState is co-located with the registry because it is part of StreamRegistryEntry. Moving it to hlsSegments.ts would create a circular dependency since
 * hlsSegments.ts imports getStream from registry.ts.
 */
export interface HLSState {

  // Playlist content.

  // The current m3u8 playlist content. For streams without separate audio, this is the variant playlist. For streams with separate audio, this is the master
  // playlist referencing video.m3u8 and audio.m3u8.
  playlist: string;

  // The video variant playlist content for streams with separate audio renditions. Empty string when not applicable.
  videoPlaylist: string;

  // The audio variant playlist content for streams with separate audio renditions. Empty string when not applicable.
  audioPlaylist: string;

  // Whether this stream has separate audio renditions. When true, stream.m3u8 serves a master playlist referencing video.m3u8 and audio.m3u8. When false (the
  // common case), stream.m3u8 serves the variant playlist directly.
  hasAudio: boolean;

  // Whether the segmenter has produced a real playlist (as opposed to a preroll-seeded playlist). Used by updatePlaylist to detect the first real playlist for timing
  // logs. Without this flag, the empty-string check would fail when preroll content pre-seeds the playlist field.
  hasRealPlaylist: boolean;

  // Segment storage.

  // The fMP4 initialization segment containing codec configuration. Sent once at stream start and retained for the stream's lifetime. Clients must fetch this before
  // any media segments.
  initSegment: Nullable<Buffer>;

  // Map of media segment filenames to their binary data.
  segments: Map<string, Buffer>;

  // Running total of all video segment buffer sizes in bytes. Updated by storeSegmentToMap on add and rotate. Eliminates O(n) iteration in getStreamMemoryUsage().
  segmentBytes: number;

  // Map of audio segment filenames to their binary data. Used only for streams with separate audio renditions (e.g., Google DAI on BET/VH1).
  audioSegments: Map<string, Buffer>;

  // Running total of all audio segment buffer sizes in bytes. Updated by storeSegmentToMap on add and rotate. Must be reset to zero when audioSegments is cleared
  // directly (e.g., native-to-capture fallback in monitor.ts).
  audioSegmentBytes: number;

  /* Initialization segments a native fMP4 relay has fetched from upstream #EXT-X-MAP references, indexed by track and keyed by the name they are served under.
   * The per-track Record is what lets the store operate generically on initSegments[track] where the flat segments/audioSegments pair forces a branch, and it
   * makes cross-track isolation structural rather than a naming convention: pruning one track cannot reach the other's entries.
   *
   * A track holds more than one entry whenever an upstream MAP change is still referenced by segments in the playlist window - the outgoing init must remain
   * fetchable until the last segment that needs it rotates out. This differs from the capture path's single initSegment slot above, which is overwritten in
   * place because capture owns its own encoder and never changes codec configuration mid-stream.
   */
  initSegments: Record<InitSegmentTrack, Map<string, Buffer>>;

  // Running total of the bytes held across both init Maps. Maintained by the named-init store on store and prune so memory reporting stays O(1), the same shape
  // as the segment counters above.
  initSegmentBytes: number;

  // The init each track's freshly-stored segments currently reference, by served name, or null before the track's first init arrives. The store maintains this
  // and the playlist and remux paths read it; it is never derived by parsing filenames.
  currentInitNames: Record<InitSegmentTrack, Nullable<string>>;

  // Typed emitter for segment notifications. MPEG-TS consumers subscribe to these events to receive segment data in real time.
  segmentEmitter: SegmentEmitter;

  // Readiness signals.

  // Promise that resolves when the first init segment is stored. Used by MPEG-TS consumers to wait for codec configuration before starting their FFmpeg remuxer.
  initSegmentReady: Promise<void>;

  // Function to signal that the init segment is ready.
  signalInitSegmentReady: () => void;

  // Promise that resolves when the first playlist is available.
  playlistReady: Promise<void>;

  // Function to signal that the playlist is ready.
  signalPlaylistReady: () => void;

  // Preroll management.

  // The base URL for constructing absolute preroll segment URIs in the composite playlist (e.g., "http://192.168.1.100:5589"). Null when no preroll is active.
  prerollBaseUrl: Nullable<string>;

  // The preroll codec variant for this stream. Determines which preroll variant is served and referenced in playlists. Null when no preroll is active.
  prerollCodec: Nullable<CaptureCodec>;

  // Number of preroll segments that precede real content. Zero when no preroll is active. The segmenter uses this to know which indices in the playlist sliding
  // window are preroll entries vs real entries.
  prerollSegmentCount: number;

  // Wall-clock time when the preroll timer fired and the progressive preroll playlist began. Used to compute elapsed time on each playlist poll so the progressive
  // window advances in real time, simulating a live stream. Null before the preroll timer fires.
  prerollStartTime: Nullable<Date>;

  // Timer handle for deferred preroll seeding. The timer fires after PREROLL_DELAY_MS; if real content hasn't arrived yet, preroll is seeded and playlistReady is
  // signaled. Deliberately NOT cancelled in completeStreamSetup() - the timer must survive setup completion for native streams where the proxy's first poll cycle
  // takes 10-15+ seconds. Every path that invalidates preroll state disarms it through cancelPrerollTimer(), the single disarm point.
  prerollTimer: Nullable<ReturnType<typeof setTimeout>>;

  // Resume continuity.

  // Snapshotted resume segment index from the prior session. Read once at stream registration and stored here so both the preroll timer callback and the segmenter
  // creation in completeStreamSetup() use the same value - eliminating the TTL race that would occur if each read the resume map independently.
  resumeSegmentIndex: number;
}

/**
 * Stream-specific information for idle detection.
 */
export interface StreamInfo {

  // Timestamp of the last playlist request, used for idle timeout detection.
  lastPlaylistRequest: number;

  // Key used to look up this stream in the channel-to-stream mapping.
  storeKey: string;
}

/**
 * The identity of a capture-mode stream: the screen-capture pipeline and the encoder facts that describe what it is producing.
 */
export interface CaptureStreamIdentity {

  // Video codec label for this stream (e.g., "H264", "HEVC"), determined by the GPU's capabilities and the user's allowlist. Null until setup completes.
  readonly captureCodec: Nullable<string>;

  /* The capture-pipeline composite: the raw puppeteer-stream capture, the optional Matroska-to-fMP4 FFmpeg child, and the fMP4 segmenter, owned as one
   * self-disposing unit with the correct kill-then-destroy-then-stop teardown order. The attached segmenter (read via captureSession.segmenter) is itself null
   * until createCaptureSegmenter wires it.
   *
   * Null IS the pending state - an entry registered before its async setup produced a pipeline. Membership in the registry under a capture identity is what
   * hasActiveCaptureStreams answers on, so a pending entry counts as capture-active and the window stays visible across the whole tune.
   */
  readonly captureSession: Nullable<CaptureSession>;

  // Whether this stream is using hardware-accelerated video encoding on the local GPU. True when Chrome's MediaRecorder is using hardware encoding.
  readonly hardwareAccelerated: boolean;

  // The member that marks this identity as capture mode.
  readonly mode: "capture";
}

/**
 * The identity of a native-mode stream: the HLS proxy relaying the service's own segments, and the manifest facts that describe the feed it is bound to.
 */
export interface NativeStreamIdentity {

  // Video codec label extracted from the service's HLS manifest. Null when the CODECS attribute is absent or unrecognized.
  readonly captureCodec: Nullable<string>;

  // The member that marks this identity as native mode.
  readonly mode: "native";

  // Declared bandwidth from the service's HLS manifest in bits per second. Zero when the BANDWIDTH attribute is absent.
  readonly nativeBandwidth: number;

  // Container format of the upstream segments the proxy is relaying. Null on a feed the DRM path abandoned.
  readonly nativeContainer: Nullable<MediaContainer>;

  // The native HLS proxy for streams that bypass screen capture. Never null: a native identity that had lost its proxy would leave health checking a permanent
  // no-op, so the type makes that state unrepresentable rather than guarding against it at every read.
  readonly nativeProxy: NativeProxy;

  // Video resolution from the service's HLS manifest (e.g., "1920x1080"). Null when the attribute is absent.
  readonly nativeResolution: Nullable<string>;

  /* Re-establishes this stream's channel on a page and returns the resulting manifest interception: the streaming layer builds this at native upgrade by closing
   * over the stream's own profile and url, so a token-refresh page reload acquires its manifest through the same tune machinery - navigation, overlay handling,
   * channel selection, adjudication, and verification - that established the stream. Never null: every write that produces a native identity supplies it.
   */
  readonly reestablishManifest: (page: Page) => Promise<Nullable<ManifestInterceptionResult>>;
}

/* A stream is either capturing the browser's compositor or relaying the service's own HLS, and the two carry entirely different state. Modelling that as a
 * discriminated union rather than as flat fields with an exclusivity convention makes the illegal states unrepresentable: a native identity without its proxy,
 * a capture identity carrying manifest quality, a half-completed mode flip.
 *
 * Mutation is by whole-identity replacement - `entry.identity = { ... }` - and every member being readonly is what makes that rule compiler-enforced rather than
 * asserted. A mode-CHANGING write is always a pure object literal, never a spread of the outgoing identity: a cross-variant spread evades excess-property
 * checking and could smuggle the old variant's members into the new one. A refresh that stays WITHIN a variant may spread its own current identity with the
 * changed members, since same-variant-in-same-variant-out leaves nothing to smuggle.
 *
 * The union settles the state, not the reading of it. A consumer narrows freshly on `entry.identity.mode` (or through the published predicates) before touching
 * variant members, and an asynchronous callback narrows AT FIRE TIME, because the identity it was created under may have been replaced by the time it runs.
 */
export type StreamIdentity = CaptureStreamIdentity | NativeStreamIdentity;

/**
 * Registry entry for an active stream. This is the single source of truth for all stream data, including browser state, HLS segments, and the segmenter reference.
 */
export interface StreamRegistryEntry {

  // Channel name if streaming a named channel, or null for arbitrary URLs.
  channelName: Nullable<string>;

  // IP address of the client that initiated this stream. Used to identify the Channels DVR server for show info lookup.
  clientAddress: Nullable<string>;

  // HLS segment storage including init segment, media segments, and playlist.
  hls: HLSState;

  // Numeric identifier assigned by getNextStreamId(); unique for the lifetime of this process, not persisted across restarts.
  id: number;

  // What this stream is and what pipeline is producing it. Replaced whole at every mode transition; never mutated member by member.
  identity: StreamIdentity;

  // Count of active MPEG-TS client connections consuming this stream. Incremented when a client connects, decremented on disconnect. Used by idle timeout logic to
  // keep the stream alive while MPEG-TS clients are connected.
  mpegTsClientCount: number;

  // Stream-specific info for idle detection.
  info: StreamInfo;

  // The browser page for this stream. Null for pending stream entries that have been registered but whose async setup has not yet completed.
  page: Nullable<Page>;

  // Whether this stream was started by the pretune module ahead of a scheduled recording. Pretuned streams are exempt from idle timeout until a real client
  // connects, at which point this flag is cleared and the stream follows normal idle timeout behavior.
  preTuned: boolean;

  // The probe-cache identity this stream resolves under - the channel key plus the stamp of the binding it was tuned from. Native recovery reads it here to
  // re-probe under the same identity the tune used, rather than reassembling one from a recovery frame that does not hold the binding. Null for pending stream
  // entries that have been registered but whose async setup has not yet completed.
  probeIdentity: Nullable<ProbeCacheIdentity>;

  // The resolved site profile used for this stream. Needed for tab replacement recovery to recreate the capture with the same profile. Null for pending stream entries
  // that have been registered but whose async setup has not yet completed.
  profile: Nullable<ResolvedSiteProfile>;

  // Set when the stream entry is created; the basis for uptime and duration calculations reported by status and logging.
  startTime: Date;

  // The playback health monitor handle, or null if monitoring hasn't started. Exposes the live recovery metrics (read in the termination prologue) and a
  // self-contained dispose that stops the monitor's polling interval.
  monitor: Nullable<MonitorHandle>;

  // String identifier for logging (e.g., "cnn-5jecl6").
  streamIdStr: string;

  // The source URL being captured or proxied; either a predefined channel's configured URL or an ad-hoc URL supplied by the caller.
  url: string;
}

// State.

// The unified stream registry. Maps numeric stream IDs to stream entries.
const streamRegistry = new Map<number, StreamRegistryEntry>();

// Counter for generating unique stream IDs. Incremented for each new stream.
let streamIdCounter = 0;

// Public API.

/**
 * Gets the next unique stream ID by incrementing the counter. Each call returns a new, higher ID that has never been used before in this process lifetime.
 * @returns The next unique stream ID.
 */
export function getNextStreamId(): number {

  return ++streamIdCounter;
}

/**
 * Registers a stream in the registry. This should be called after stream setup is complete and the stream is ready to serve data.
 * @param entry - The stream registry entry to add.
 */
export function registerStream(entry: StreamRegistryEntry): void {

  streamRegistry.set(entry.id, entry);
}

/**
 * Unregisters a stream from the registry. This should be called during stream cleanup to remove the stream from tracking.
 * @param id - The numeric stream ID to remove.
 */
export function unregisterStream(id: number): void {

  streamRegistry.delete(id);
}

/**
 * Gets a stream entry by its ID.
 * @param id - The numeric stream ID to look up.
 * @returns The stream entry if found, undefined otherwise.
 */
export function getStream(id: number): StreamRegistryEntry | undefined {

  return streamRegistry.get(id);
}

/**
 * Gets all stream entries in the registry.
 * @returns Array of all stream registry entries.
 */
export function getAllStreams(): StreamRegistryEntry[] {

  return Array.from(streamRegistry.values());
}

/**
 * Gets the total number of streams in the registry.
 * @returns The number of active streams.
 */
export function getStreamCount(): number {

  return streamRegistry.size;
}

/**
 * Reports whether any registered stream is in capture mode. The browser window's visibility policy reads this, so it is evaluated fresh on every call - the answer
 * flips with every stream start and end, and a cached copy would be stale exactly when it matters.
 *
 * Registry presence is the test rather than a live captureSession reference. A pending entry registers in capture mode before physical capture starts, and an entry
 * leaves the registry only after teardown, so membership brackets the entire window during which Chrome is reading the compositor. The capture session attaches
 * partway through establishment, which would leave the predicate reading false across the whole tune - the moment the window most needs to be visible.
 * @returns True when at least one registered stream is in capture mode.
 */
export function hasActiveCaptureStreams(): boolean {

  return getAllStreams().some(isCaptureIdentity);
}

// Identity.

/**
 * Builds the identity a stream is born with: capture mode, with no pipeline and no encoder facts yet. The literal exists here alone so the pending birth and the
 * native fallback's pre-flip declare the same shape rather than hand-copying it.
 * @returns A capture identity in its pending state.
 */
export function makePendingCaptureIdentity(): CaptureStreamIdentity {

  return { captureCodec: null, captureSession: null, hardwareAccelerated: false, mode: "capture" };
}

/**
 * Narrows a stream entry to capture mode. Published alongside the segment-health readers so consumers outside the streaming layer ask the registry what a stream
 * is rather than reaching into its identity themselves.
 * @param entry - The stream registry entry to test.
 * @returns True when the entry carries a capture identity, narrowing it for the caller.
 */
export function isCaptureIdentity(entry: StreamRegistryEntry): entry is StreamRegistryEntry & { identity: CaptureStreamIdentity } {

  return entry.identity.mode === "capture";
}

/**
 * Reports whether a stream is encoding on the local GPU. Hardware acceleration is a capture-mode fact, so native streams answer false: they pass the service's own
 * segments through and encode nothing locally. The status projections read it here rather than each narrowing the identity themselves.
 * @param entry - The stream registry entry to query.
 * @returns True when the stream is capturing with hardware-accelerated encoding.
 */
export function isHardwareAccelerated(entry: StreamRegistryEntry): boolean {

  return (entry.identity.mode === "capture") && entry.identity.hardwareAccelerated;
}

/**
 * Records the quality a token refresh has bound this stream to. The registry owns the mechanics because two layers relay the same refresh - the native upgrade in
 * hls.ts and the monitor's L2 recovery - and a hand-copied guard-then-spread in each would be two chances to get it wrong.
 *
 * The narrow happens here, freshly, because both callers invoke this from an asynchronous callback that can fire long after any earlier check: a refresh that
 * completes while a capture fallback is mid-flight finds a capture identity and is skipped. Skipping is right in every case - quality metadata for a stream
 * midway through a fallback is moot, and a fallback that fails restores the proxy, whose next refresh writes the quality again.
 * @param entry - The stream registry entry to update.
 * @param refreshed - The quality facts of the freshly bound feed.
 */
export function applyNativeQualityRefresh(entry: StreamRegistryEntry, refreshed: RefreshedFeedMetadata): void {

  const identity = entry.identity;

  if(identity.mode !== "native") {

    return;
  }

  entry.identity = { ...identity, captureCodec: refreshed.codec, nativeBandwidth: refreshed.bandwidth, nativeResolution: refreshed.resolution };
}

/**
 * Updates the last playlist request timestamp for a stream. This should be called whenever a playlist or segment is requested to keep the idle timeout accurate.
 * @param id - The numeric stream ID.
 */
export function updateLastAccess(id: number): void {

  const entry = streamRegistry.get(id);

  if(entry) {

    entry.info.lastPlaylistRequest = Date.now();
  }
}

/**
 * Creates the initial HLS state for a new stream. This sets up empty segment storage and the playlist readiness signaling mechanism.
 * @returns A new HLSState object ready to receive segments.
 */
export function createHLSState(): HLSState {

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise: initSegmentReady, resolve: signalInitSegmentReady } = Promise.withResolvers<void>();

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise: playlistReady, resolve: signalPlaylistReady } = Promise.withResolvers<void>();

  const segmentEmitter = new EventEmitter() as SegmentEmitter;

  // Allow up to 20 listeners per event to support multiple concurrent MPEG-TS clients consuming the same stream.
  segmentEmitter.setMaxListeners(20);

  return {

    audioPlaylist: "",
    audioSegmentBytes: 0,
    audioSegments: new Map(),
    currentInitNames: { audio: null, video: null },
    hasAudio: false,
    hasRealPlaylist: false,
    initSegment: null,
    initSegmentBytes: 0,
    initSegmentReady,
    initSegments: { audio: new Map(), video: new Map() },
    playlist: "",
    playlistReady,
    prerollBaseUrl: null,
    prerollCodec: null,
    prerollSegmentCount: 0,
    prerollStartTime: null,
    prerollTimer: null,
    resumeSegmentIndex: 0,
    segmentBytes: 0,
    segmentEmitter,
    segments: new Map(),
    signalInitSegmentReady,
    signalPlaylistReady,
    videoPlaylist: ""
  };
}

/**
 * Disarms a stream's deferred preroll timer and clears the handle. This is the single disarm point for the preroll timer: every path that invalidates preroll state -
 * the first real playlist arriving (updatePlaylist in hlsSegments.ts), a native-mode commit that nulls the preroll fields (startNativeProxy in hls.ts), and stream
 * teardown (terminateStream in lifecycle.ts) - routes through here so the timer can never fire against state that has already moved on. Safe to call when no timer is
 * armed: the handle is already null and the call is a no-op.
 * @param hls - The HLS state whose preroll timer to disarm.
 */
export function cancelPrerollTimer(hls: HLSState): void {

  if(hls.prerollTimer) {

    clearTimeout(hls.prerollTimer);
    hls.prerollTimer = null;
  }
}

// Memory Usage.

/**
 * Memory usage breakdown for a stream's HLS segment storage.
 */
export interface StreamMemoryUsage {

  // Size of the initialization segments in bytes - the capture path's single slot plus every initialization segment a native fMP4 relay is holding.
  initSegment: number;

  // Total size of all media segments in bytes.
  segments: number;

  // Total memory usage (initSegment + segments) in bytes.
  total: number;
}

/**
 * Returns the memory usage for a single stream's HLS segment storage. Uses the running byte counters maintained by storeSegmentToMap() for O(1) reads instead of
 * iterating all segment buffers. The init segment is a single buffer read (also O(1)).
 * @param entry - The stream registry entry to measure.
 * @returns Memory usage breakdown in bytes.
 */
export function getStreamMemoryUsage(entry: StreamRegistryEntry): StreamMemoryUsage {

  // Both init sources fold into one figure: the capture path's single slot and the running total the native relay's per-track init Maps maintain. A stream only
  // ever populates one of the two, but summing keeps the breakdown truthful without a mode branch here.
  const initSegmentSize = (entry.hls.initSegment?.length ?? 0) + entry.hls.initSegmentBytes;
  const segmentsSize = entry.hls.segmentBytes + entry.hls.audioSegmentBytes;

  return {

    initSegment: initSegmentSize,
    segments: segmentsSize,
    total: initSegmentSize + segmentsSize
  };
}

/**
 * Calculates the total segment memory usage across all active streams. This is useful for monitoring overall memory consumption by HLS buffers.
 * @returns Total memory usage in bytes across all streams.
 */
export function getTotalSegmentMemory(): number {

  let total = 0;

  for(const entry of streamRegistry.values()) {

    total += getStreamMemoryUsage(entry).total;
  }

  return total;
}

// Segment Health.

/**
 * Returns the fMP4 segmenter producing a stream's segments, or null when there is none: a native stream relays the service's own segments, and a capture stream
 * has no segmenter until createCaptureSegmenter wires one. This is the single place the capture-identity narrow and the pipeline walk are written, so the several
 * consumers that read segment indices, init segments, and session statistics all ask the same question the same way.
 *
 * An absent entry is accepted because callers routinely look the stream up and read its segmenter in one expression; a stream that is gone has no segmenter.
 * @param entry - The stream registry entry to query, or undefined when the lookup found nothing.
 * @returns The attached segmenter, or null.
 */
export function getStreamSegmenter(entry: StreamRegistryEntry | undefined): Nullable<FMP4SegmenterResult> {

  if(entry?.identity.mode !== "capture") {

    return null;
  }

  return entry.identity.captureSession?.segmenter ?? null;
}

/**
 * Returns whether the last segment contained video traf boxes. Used by the monitor to distinguish dead capture pipelines (audio-only, no video trafs) from legitimate
 * small segments produced by static content (video trafs present but low-bitrate). Returns null if the segmenter does not exist or the video trackId is unknown.
 * @param entry - The stream registry entry to query.
 * @returns True if video trafs were present, false if absent, null if unknown.
 */
export function getLastSegmentHasVideo(entry: StreamRegistryEntry): Nullable<boolean> {

  return getStreamSegmenter(entry)?.getLastSegmentHasVideo() ?? null;
}

/**
 * Gets the size in bytes of the last segment stored for a stream. Used by the monitor to detect dead capture pipelines that produce empty segments (18 bytes observed)
 * while the video element appears healthy.
 * @param entry - The stream registry entry to query.
 * @returns Segment size in bytes, or null if no segmenter exists.
 */
export function getLastSegmentSize(entry: StreamRegistryEntry): Nullable<number> {

  return getStreamSegmenter(entry)?.getLastSegmentSize() ?? null;
}
