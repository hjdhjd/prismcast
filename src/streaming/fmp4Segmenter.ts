/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fmp4Segmenter.ts: fMP4 HLS segmentation for PrismCast.
 */
import { createMP4BoxParser, detectMoofKeyframe, offsetMoofTimestamps, parseMoovTimescales } from "./mp4Parser.js";
import { storeInitSegment, storeSegment, updatePlaylist } from "./hlsSegments.js";
import { CONFIG } from "../config/index.js";
import { LOG } from "../utils/index.js";
import type { MP4Box } from "./mp4Parser.js";
import type { Nullable } from "../types/index.js";
import type { Readable } from "node:stream";

/* This module transforms a puppeteer-stream MP4 capture into HLS fMP4 segments. The overall flow is: (1) receive MP4 data from puppeteer-stream (H.264 + AAC from
 * either native capture or FFmpeg transcoding), (2) parse MP4 box structure to identify ftyp + moov (initialization segment) and moof + mdat pairs (media fragments),
 * (3) store init segment and accumulate media fragments into segments, and (4) generate and update the m3u8 playlist.
 *
 * Keyframe detection is available for diagnostics by setting KEYFRAME_DEBUG to true. When enabled, each moof's traf/trun sample flags are parsed (ISO 14496-12) to
 * determine whether fragments start with sync samples (keyframes). Statistics are logged at stream termination and per-segment warnings are emitted for segments that
 * don't start with a keyframe. When disabled, the moof data is passed through without inspection.
 */

// Set to true to enable keyframe detection and statistics. This parses traf/trun sample flags in each moof to track keyframe frequency and log per-segment warnings.
// Useful for diagnosing frozen screen issues in downstream HLS consumers.
const KEYFRAME_DEBUG = false;

// Types.

/**
 * Options for creating an fMP4 segmenter.
 */
export interface FMP4SegmenterOptions {

  // Initial per-track timestamp counters for continuation after tab replacement. When provided, the segmenter continues writing timestamps from where the previous
  // segmenter left off, ensuring monotonic baseMediaDecodeTime across capture restarts. If not provided, all tracks start at 0.
  initialTrackTimestamps?: Map<number, bigint>;

  // Callback when the segmenter encounters an error.
  onError: (error: Error) => void;

  // Callback when the segmenter stops (stream ended or error).
  onStop: () => void;

  // If true, the first segment from this segmenter should have a discontinuity marker. Used after tab replacement to signal codec/timing change. When
  // previousInitSegment is also provided, the marker is suppressed if the new init segment is byte-identical to the previous one.
  pendingDiscontinuity?: boolean;

  // The init segment (ftyp + moov) from the previous segmenter. When provided alongside pendingDiscontinuity, the new init segment is compared against this buffer.
  // If byte-identical, the discontinuity marker is suppressed because the decoder parameters have not changed.
  previousInitSegment?: Nullable<Buffer>;

  // Prior session stats to merge from the old segmenter during tab replacement. The new segmenter inherits these accumulated stats and increments the tab replacement
  // counter, so the final stats at stream end reflect the entire session across all segmenter instances.
  priorSessionStats?: SessionStats;

  // Starting init version for URI cache busting after tab replacement. Ensures the init URI increments monotonically across segmenter instances so HLS clients
  // re-fetch the init segment when its content changes. If not provided, starts at 0.
  startingInitVersion?: number;

  // Starting segment index for continuation after tab replacement. If not provided, starts at 0.
  startingSegmentIndex?: number;

  // The numeric stream ID for storage.
  streamId: number;
}

/**
 * Keyframe detection statistics tracked across the lifetime of a segmenter. These metrics provide visibility into the actual keyframe frequency in the fMP4 output,
 * which is critical for diagnosing frozen screen issues in downstream consumers like Channels DVR.
 */
export interface KeyframeStats {

  // Average interval between keyframes in milliseconds. Computed from totalKeyframeIntervalMs / (keyframeCount - 1).
  averageKeyframeIntervalMs: number;

  // Total number of moof boxes where keyframe detection returned null (indeterminate).
  indeterminateCount: number;

  // Total number of moof boxes that started with a keyframe.
  keyframeCount: number;

  // Maximum observed interval between consecutive keyframes in milliseconds.
  maxKeyframeIntervalMs: number;

  // Minimum observed interval between consecutive keyframes in milliseconds.
  minKeyframeIntervalMs: number;

  // Total number of moof boxes that did not start with a keyframe.
  nonKeyframeCount: number;

  // Number of segments whose first moof was not a keyframe. This directly correlates with potential frozen frame issues.
  segmentsWithoutLeadingKeyframe: number;
}

/**
 * Session-level statistics accumulated across the lifetime of a stream, including across tab replacements. These metrics provide a summary of segmenter health and A-V
 * synchronization quality when the stream terminates.
 */
export interface SessionStats {

  // Number of moofs that failed timestamp processing (caught by try/catch in the moof handler). Non-zero values indicate malformed fMP4 data from Chrome.
  malformedMoofCount: number;

  // Number of segment-level A-V sync measurements. Each segment boundary contributes one measurement.
  syncSpreadCount: number;

  // Maximum observed inter-track timing spread in milliseconds.
  syncSpreadMaxMs: number;

  // Minimum observed inter-track timing spread in milliseconds.
  syncSpreadMinMs: number;

  // Running sum of inter-track timing spreads for mean calculation.
  syncSpreadSumMs: number;

  // Number of tab replacements that occurred during this stream session.
  tabReplacementCount: number;
}

/**
 * Result of creating an fMP4 segmenter.
 */
export interface FMP4SegmenterResult {

  // Returns the combined init segment (ftyp + moov) buffer, or null if the init segment has not been received yet. Used by tab replacement to pass the previous
  // init segment to the new segmenter for byte comparison.
  getInitSegment: () => Nullable<Buffer>;

  // Returns the current init version counter. Used by tab replacement to continue the version sequence so init URIs remain monotonically increasing.
  getInitVersion: () => number;

  // Returns a snapshot of the current keyframe detection statistics.
  getKeyframeStats: () => KeyframeStats;

  // Get the size in bytes of the last segment stored. Used by the monitor to detect dead capture pipelines producing empty segments.
  getLastSegmentSize: () => number;

  // Get the current segment index. Used by tab replacement to continue numbering from where the old segmenter left off.
  getSegmentIndex: () => number;

  // Returns a snapshot of the accumulated session statistics. Used by tab replacement to carry stats to the new segmenter, and by stream termination to log the summary.
  getSessionStats: () => SessionStats;

  // Returns a copy of the per-track timestamp counters. Used by tab replacement to pass accumulated timestamps to the new segmenter, ensuring monotonic
  // baseMediaDecodeTime across capture restarts.
  getTrackTimestamps: () => Map<number, bigint>;

  // Flush the current fragment buffer as a short segment and mark the next segment with a discontinuity tag. Called after recovery events (source reload, page
  // navigation) that disrupt the video source, so HLS clients know to flush their decoder state and resynchronize.
  markDiscontinuity: () => void;

  // Pipe a readable stream to the segmenter.
  pipe: (stream: Readable) => void;

  // Stop the segmenter and clean up.
  stop: () => void;
}

/**
 * Internal state for tracking segmentation progress.
 */
interface SegmenterState {

  // Segment indices that should have a discontinuity marker before them in the playlist.
  discontinuityIndices: Set<number>;

  // Whether the first segment has been emitted. When false, the moof handler cuts at the first opportunity (one moof+mdat pair) to minimize time-to-first-frame.
  firstSegmentEmitted: boolean;

  // Accumulated fragment data for the current segment.
  fragmentBuffer: Buffer[];

  // Whether we have received the complete init segment.
  hasInit: boolean;

  // Total number of moof boxes where keyframe detection returned null (indeterminate).
  indeterminateCount: number;

  // Boxes collected for the init segment (ftyp + moov).
  initBoxes: Buffer[];

  // The combined init segment buffer (ftyp + moov) after it has been assembled. Null until the moov box is received. Retained for the getInitSegment() getter so
  // tab replacement can pass it to the new segmenter for byte comparison.
  initSegment: Nullable<Buffer>;

  // Monotonic version counter for the init segment URI. Incremented each time the init content changes (new stream startup or different codec parameters after tab
  // replacement). Used in #EXT-X-MAP:URI="init.mp4?v=N" to force HLS clients to re-fetch the init when it changes, preventing timescale mismatches.
  initVersion: number;

  // Total number of moof boxes that started with a keyframe.
  keyframeCount: number;

  // Timestamp of the last detected keyframe moof, for interval calculation. Null until the first keyframe is seen.
  lastKeyframeTime: Nullable<number>;

  // Size in bytes of the last segment stored. Used by the monitor to detect dead capture pipelines producing empty segments.
  lastSegmentSize: number;

  // Maximum observed interval between consecutive keyframes in milliseconds.
  maxKeyframeIntervalMs: number;

  // Minimum observed interval between consecutive keyframes in milliseconds.
  minKeyframeIntervalMs: number;

  // Total number of moof boxes that did not start with a keyframe.
  nonKeyframeCount: number;

  // Whether the next segment should have a discontinuity marker (consumed when first segment is output).
  pendingDiscontinuity: boolean;

  // Actual media-time durations for each segment in seconds, computed from accumulated trun sample durations divided by the track timescale. Falls back to wall-clock
  // time when media-time data is unavailable (e.g., moov timescale parsing failed). Used by generatePlaylist() for accurate #EXTINF values. Pruned to keep only
  // entries within the playlist sliding window.
  segmentDurations: Map<number, number>;

  // Whether the current segment's first moof has been checked for keyframe status. Reset when outputSegment() clears the fragment buffer.
  segmentFirstMoofChecked: boolean;

  // Current media segment index.
  segmentIndex: number;

  // Wall-clock time when the current segment started accumulating. Used for segment cutting decisions (when to start a new segment) and as a fallback for EXTINF
  // duration when media-time data is unavailable.
  segmentStartTime: number;

  // Accumulated per-track trun durations for the current segment, in timescale units. Keyed by track_ID. Reset when a segment is output. Used with trackTimescales
  // to compute media-time EXTINF values that exactly match the fMP4 PTS progression.
  segmentTrackDurations: Map<number, bigint>;

  // Number of segments whose first moof was not a keyframe.
  segmentsWithoutLeadingKeyframe: number;

  // Running session statistics accumulated across the lifetime of this segmenter instance. Initialized from priorSessionStats (if provided during tab replacement) to
  // carry forward stats from previous segmenter instances.
  sessionStats: SessionStats;

  // Whether the segmenter has been stopped.
  stopped: boolean;

  // Running total of keyframe intervals in milliseconds. Used with keyframeCount to compute the average.
  totalKeyframeIntervalMs: number;

  // Per-track constant offsets applied to Chrome's original tfdt values. Zero during normal playback (pure pass-through). Computed lazily on the first moof per track
  // after tab replacement: offset = initialTrackTimestamp - chromeTfdt.
  trackOffsets: Map<number, bigint>;

  // Tracks which track IDs have had their offset computed. Offsets are computed lazily on the first moof per track because Chrome's first tfdt value isn't known
  // until it arrives.
  trackOffsetsInitialized: Set<number>;

  // Per-track timescale values parsed from the moov box. Keyed by track_ID. Populated once when the moov box is received. Converts accumulated trun durations (in
  // timescale units) to seconds for EXTINF: seconds = duration / timescale.
  trackTimescales: Map<number, number>;

  // Per-track timestamp counters, keyed by track_ID. Each value is the next expected baseMediaDecodeTime (originalTfdt + offset + duration), used for tab replacement
  // handoff via getTrackTimestamps(). Audio and video tracks have separate counters because they may use different timescales (e.g., 90000 for video, 48000 for audio).
  trackTimestamps: Map<number, bigint>;
}

// Keyframe Stats Formatting.

/**
 * Formats keyframe statistics into a human-readable summary for the termination log. Returns an empty string if no moof boxes were processed. The format mirrors the
 * recovery metrics summary style used in monitor.ts.
 *
 * Example output:
 * - "Keyframes: 2490 of 2490 moofs (100.0%), interval 1.9-2.1s avg 2.0s."
 * - "Keyframes: 85 of 198 moofs (42.9%), interval 1.8-12.4s avg 3.1s, 5 segments without leading keyframe."
 *
 * @param stats - The keyframe statistics to format.
 * @returns Formatted summary string, or empty string if no data.
 */
export function formatKeyframeStatsSummary(stats: KeyframeStats): string {

  const totalMoofs = stats.keyframeCount + stats.nonKeyframeCount + stats.indeterminateCount;

  // No moof boxes were processed — stream ended before any media fragments arrived.
  if(totalMoofs === 0) {

    return "";
  }

  const percentage = ((stats.keyframeCount / totalMoofs) * 100).toFixed(1);
  const parts: string[] = [ "Keyframes: ", String(stats.keyframeCount), " of ", String(totalMoofs), " moofs (", percentage, "%)" ];

  // Include interval statistics if we have at least two keyframes (needed for a meaningful interval).
  if(stats.keyframeCount >= 2) {

    const minSec = (stats.minKeyframeIntervalMs / 1000).toFixed(1);
    const maxSec = (stats.maxKeyframeIntervalMs / 1000).toFixed(1);
    const avgSec = (stats.averageKeyframeIntervalMs / 1000).toFixed(1);

    parts.push(", interval ", minSec, "-", maxSec, "s avg ", avgSec, "s");
  }

  // Note segments that didn't start with a keyframe — these directly correlate with potential frozen frame issues.
  if(stats.segmentsWithoutLeadingKeyframe > 0) {

    parts.push(", ", String(stats.segmentsWithoutLeadingKeyframe), " segment");

    if(stats.segmentsWithoutLeadingKeyframe !== 1) {

      parts.push("s");
    }

    parts.push(" without leading keyframe");
  }

  parts.push(".");

  return parts.join("");
}

/**
 * Formats session statistics into a human-readable summary for the termination log. Returns an empty string if no sync measurements were recorded. The format provides
 * a concise overview of segmenter health including A-V synchronization, tab replacements, and data integrity.
 *
 * Example output:
 * - "Session: 1725 segments, A-V sync: mean 12.0ms, min 0.7ms, max 25.7ms."
 * - "Session: 485 segments, A-V sync: mean 10.5ms, min 1.7ms, max 24.3ms, 2 tab replacements."
 * - "Session: 100 segments, A-V sync: mean 15.2ms, min 2.0ms, max 30.1ms, 1 tab replacement, 3 malformed moofs."
 *
 * @param stats - The session statistics to format.
 * @param segmentCount - Total number of segments produced across all segmenter instances.
 * @returns Formatted summary string, or empty string if no data.
 */
export function formatSessionStatsSummary(stats: SessionStats, segmentCount: number): string {

  if(stats.syncSpreadCount === 0) {

    return "";
  }

  const meanMs = (stats.syncSpreadSumMs / stats.syncSpreadCount).toFixed(1);

  const parts: string[] = [ "Session: ", String(segmentCount), " segments, A-V sync: mean ", meanMs, "ms, min ", stats.syncSpreadMinMs.toFixed(1), "ms, max ",
    stats.syncSpreadMaxMs.toFixed(1), "ms" ];

  if(stats.tabReplacementCount > 0) {

    parts.push(", ", String(stats.tabReplacementCount), " tab replacement");

    if(stats.tabReplacementCount !== 1) {

      parts.push("s");
    }
  }

  if(stats.malformedMoofCount > 0) {

    parts.push(", ", String(stats.malformedMoofCount), " malformed moof");

    if(stats.malformedMoofCount !== 1) {

      parts.push("s");
    }
  }

  parts.push(".");

  return parts.join("");
}

// Segmenter Implementation.

/**
 * Creates an fMP4 segmenter that transforms MP4 input into HLS segments. The segmenter parses MP4 boxes, extracts the init segment, detects keyframes in each moof
 * fragment, and accumulates media fragments into segments based on the configured duration.
 * @param options - Segmenter options including stream ID and callbacks.
 * @returns The segmenter interface with pipe, stop, and keyframe stats methods.
 */
export function createFMP4Segmenter(options: FMP4SegmenterOptions): FMP4SegmenterResult {

  const { initialTrackTimestamps, onError, onStop, pendingDiscontinuity, previousInitSegment, priorSessionStats, startingInitVersion, startingSegmentIndex,
    streamId } = options;

  // Initialize state.
  const state: SegmenterState = {

    discontinuityIndices: new Set(),
    firstSegmentEmitted: false,
    fragmentBuffer: [],
    hasInit: false,
    indeterminateCount: 0,
    initBoxes: [],
    initSegment: null,
    initVersion: startingInitVersion ?? 0,
    keyframeCount: 0,
    lastKeyframeTime: null,
    lastSegmentSize: 0,
    maxKeyframeIntervalMs: 0,
    minKeyframeIntervalMs: Infinity,
    nonKeyframeCount: 0,
    pendingDiscontinuity: pendingDiscontinuity ?? false,
    segmentDurations: new Map(),
    segmentFirstMoofChecked: false,
    segmentIndex: startingSegmentIndex ?? 0,
    segmentStartTime: Date.now(),
    segmentTrackDurations: new Map(),
    segmentsWithoutLeadingKeyframe: 0,
    sessionStats: priorSessionStats ? { ...priorSessionStats, tabReplacementCount: priorSessionStats.tabReplacementCount + 1 } :
      { malformedMoofCount: 0, syncSpreadCount: 0, syncSpreadMaxMs: 0, syncSpreadMinMs: Infinity, syncSpreadSumMs: 0, tabReplacementCount: 0 },
    stopped: false,
    totalKeyframeIntervalMs: 0,
    trackOffsets: new Map(),
    trackOffsetsInitialized: new Set(),
    trackTimescales: new Map(),
    trackTimestamps: initialTrackTimestamps ? new Map(initialTrackTimestamps) : new Map<number, bigint>()
  };

  // Reference to the input stream for cleanup.
  let inputStream: Nullable<Readable> = null;

  /**
   * Generates the m3u8 playlist content.
   */
  function generatePlaylist(): string {

    // Compute TARGETDURATION from the maximum actual segment duration in the current playlist window. RFC 8216 requires this value to be an integer that is greater
    // than or equal to every #EXTINF duration in the playlist. We floor at the configured segment duration to avoid under-declaring when all segments are short.
    const startIndex = Math.max(0, state.segmentIndex - CONFIG.hls.maxSegments);
    let maxDuration = CONFIG.hls.segmentDuration;

    for(let i = startIndex; i < state.segmentIndex; i++) {

      const duration = state.segmentDurations.get(i) ?? CONFIG.hls.segmentDuration;

      if(duration > maxDuration) {

        maxDuration = duration;
      }
    }

    const lines: string[] = [
      "#EXTM3U",
      "#EXT-X-VERSION:7",
      [ "#EXT-X-TARGETDURATION:", String(Math.ceil(maxDuration)) ].join(""),
      [ "#EXT-X-MEDIA-SEQUENCE:", String(startIndex) ].join(""),
      [ "#EXT-X-MAP:URI=\"init.mp4?v=", String(state.initVersion), "\"" ].join("")
    ];

    // Add segment entries for each segment in the current playlist window.
    for(let i = startIndex; i < state.segmentIndex; i++) {

      // Add discontinuity marker before segments that follow a recovery event. Re-emit the init segment reference so clients explicitly reinitialize the decoder
      // with the current codec parameters.
      if(state.discontinuityIndices.has(i)) {

        lines.push("#EXT-X-DISCONTINUITY");
        lines.push([ "#EXT-X-MAP:URI=\"init.mp4?v=", String(state.initVersion), "\"" ].join(""));
      }

      // Use the actual recorded duration for this segment. Fall back to the configured target duration for segments that predate duration tracking (e.g. after
      // a hot restart with continuation).
      const duration = state.segmentDurations.get(i) ?? CONFIG.hls.segmentDuration;

      lines.push([ "#EXTINF:", duration.toFixed(3), "," ].join(""));
      lines.push([ "segment", String(i), ".m4s" ].join(""));
    }

    lines.push("");

    return lines.join("\n");
  }

  /**
   * Resets per-segment tracking state. Called after outputting a segment. Extracted to avoid duplication of the same four assignments.
   */
  function resetSegmentTracking(): void {

    state.fragmentBuffer = [];
    state.segmentFirstMoofChecked = false;
    state.segmentStartTime = Date.now();
    state.segmentTrackDurations = new Map();
  }

  /**
   * Outputs the current fragment buffer as a segment.
   */
  function outputSegment(): void {

    if(state.fragmentBuffer.length === 0) {

      return;
    }

    // If this segment follows a tab replacement, record its index for discontinuity marking.
    if(state.pendingDiscontinuity) {

      state.discontinuityIndices.add(state.segmentIndex);
      state.pendingDiscontinuity = false;
    }

    // Compute the segment duration from accumulated trun durations (media timeline). Both audio and video tracks should produce nearly identical real-time values. We
    // take the maximum across tracks to handle edge cases where one track has slightly more data at a segment boundary. Falls back to wall-clock time if no media
    // durations were accumulated (e.g., timestamp processing threw for every moof in this segment, or moov timescale parsing failed). Floored at 0.1 seconds to
    // prevent zero-duration entries that would violate HLS expectations.
    let mediaDuration = 0;

    for(const [ trackId, accumulated ] of state.segmentTrackDurations) {

      const timescale = state.trackTimescales.get(trackId);

      if(timescale && (accumulated > 0n)) {

        const seconds = Number(accumulated) / timescale;

        if(seconds > mediaDuration) {

          mediaDuration = seconds;
        }
      }
    }

    const actualDuration = Math.max(0.1, (mediaDuration > 0) ? mediaDuration : ((Date.now() - state.segmentStartTime) / 1000));

    state.segmentDurations.set(state.segmentIndex, actualDuration);

    // Compute inter-track sync spread for session statistics. This measures the timing difference between audio and video tracks at each segment boundary. Only
    // computed when both tracks have known timescales and active timestamp counters.
    if(state.trackTimescales.size >= 2) {

      let minPos = Infinity;
      let maxPos = -Infinity;
      let validCount = 0;

      for(const [ trackId, timestamp ] of state.trackTimestamps) {

        const timescale = state.trackTimescales.get(trackId);

        if(timescale && (timestamp > 0n)) {

          const positionSec = Number(timestamp) / timescale;

          if(positionSec < minPos) {

            minPos = positionSec;
          }

          if(positionSec > maxPos) {

            maxPos = positionSec;
          }

          validCount++;
        }
      }

      if(validCount >= 2) {

        const spreadMs = (maxPos - minPos) * 1000;

        state.sessionStats.syncSpreadCount++;
        state.sessionStats.syncSpreadSumMs += spreadMs;

        if(spreadMs < state.sessionStats.syncSpreadMinMs) {

          state.sessionStats.syncSpreadMinMs = spreadMs;
        }

        if(spreadMs > state.sessionStats.syncSpreadMaxMs) {

          state.sessionStats.syncSpreadMaxMs = spreadMs;
        }
      }
    }

    // Combine all fragment data into a single segment.
    const segmentData = Buffer.concat(state.fragmentBuffer);
    const segmentName = [ "segment", String(state.segmentIndex), ".m4s" ].join("");

    // Store the segment and update size for health monitoring.
    storeSegment(streamId, segmentName, segmentData);
    state.lastSegmentSize = segmentData.length;

    // Increment segment index and mark the first segment as emitted.
    state.segmentIndex++;
    state.firstSegmentEmitted = true;

    // Prune duration entries outside the playlist sliding window to prevent unbounded growth.
    const pruneThreshold = Math.max(0, state.segmentIndex - CONFIG.hls.maxSegments);

    for(const idx of state.segmentDurations.keys()) {

      if(idx < pruneThreshold) {

        state.segmentDurations.delete(idx);
      }
    }

    // Clear the fragment buffer and reset segment-level tracking for the next segment.
    resetSegmentTracking();

    // Update the playlist.
    updatePlaylist(streamId, generatePlaylist());
  }

  /**
   * Processes keyframe detection results for a moof box. Updates running statistics and logs warnings when segments don't start with keyframes.
   */
  function trackKeyframe(isKeyframe: Nullable<boolean>): void {

    const now = Date.now();

    if(isKeyframe === true) {

      state.keyframeCount++;

      // Compute the interval from the previous keyframe. We need at least one prior keyframe for a meaningful interval.
      if(state.lastKeyframeTime !== null) {

        const intervalMs = now - state.lastKeyframeTime;

        state.totalKeyframeIntervalMs += intervalMs;

        if(intervalMs < state.minKeyframeIntervalMs) {

          state.minKeyframeIntervalMs = intervalMs;
        }

        if(intervalMs > state.maxKeyframeIntervalMs) {

          state.maxKeyframeIntervalMs = intervalMs;
        }

        LOG.debug("streaming:segmenter", "Keyframe detected, interval: %dms.", intervalMs);
      }

      state.lastKeyframeTime = now;
    } else if(isKeyframe === false) {

      state.nonKeyframeCount++;
    } else {

      state.indeterminateCount++;
    }

    // Check if this is the first moof in the current segment. A segment that doesn't start with a keyframe may cause frozen frames in downstream consumers.
    if(!state.segmentFirstMoofChecked) {

      state.segmentFirstMoofChecked = true;

      if(isKeyframe !== true) {

        state.segmentsWithoutLeadingKeyframe++;

        LOG.warn("Segment %d does not start with a keyframe.", state.segmentIndex);
      }
    }
  }

  /**
   * Handles a parsed MP4 box.
   */
  function handleBox(box: MP4Box): void {

    if(state.stopped) {

      return;
    }

    // Handle init segment boxes (ftyp, moov).
    if(!state.hasInit) {

      if((box.type === "ftyp") || (box.type === "moov")) {

        state.initBoxes.push(box.data);

        // Check if we have both ftyp and moov.
        if(box.type === "moov") {

          // Output the init segment.
          const initData = Buffer.concat(state.initBoxes);

          storeInitSegment(streamId, initData);

          state.hasInit = true;
          state.initSegment = initData;

          // Check whether the init content changed. Always true for fresh streams (no previousInitSegment). For tab replacement, false when the new capture
          // happens to use the same codec parameters as the old one.
          const initChanged = !previousInitSegment || !initData.equals(previousInitSegment);

          // Version the init URI for HLS cache busting. Incrementing the version makes the #EXT-X-MAP URI different from the previous playlist, forcing clients
          // to re-fetch the init segment. This prevents timescale mismatches when Chrome's MediaRecorder picks a different timescale between capture sessions.
          if(initChanged) {

            state.initVersion++;
          }

          // Extract per-track timescale values from the moov box. These convert trun sample durations (timescale units) to seconds for media-time EXTINF values.
          // Wrapped in try/catch so a malformed moov never prevents stream startup — EXTINF falls back to wall-clock time if parsing fails.
          try {

            state.trackTimescales = parseMoovTimescales(box.data);

            if(state.trackTimescales.size === 0) {

              LOG.debug("streaming:segmenter", "No track timescales found in moov. EXTINF will use wall-clock fallback.");
            }
          } catch {

            LOG.debug("streaming:segmenter", "Failed to parse moov timescales. EXTINF will use wall-clock fallback.");
          }

          // Log init segment details for debugging timescale or codec issues.
          const timescaleEntries: string[] = [];

          for(const [ trackId, timescale ] of state.trackTimescales) {

            timescaleEntries.push("track " + String(trackId) + "=" + String(timescale));
          }

          LOG.debug("streaming:segmenter", "Init segment received: %d bytes, version=%d, timescales=[%s].",
            initData.length, state.initVersion, timescaleEntries.join(", "));

          // Suppress the discontinuity marker when codec parameters are unchanged (byte-identical init). This avoids an unnecessary decoder flush on the client.
          if(!initChanged && state.pendingDiscontinuity) {

            state.pendingDiscontinuity = false;
          }
        }

        return;
      }
    }

    // Handle media fragment boxes (moof, mdat).
    if(box.type === "moof") {

      // Start of a new fragment. Check whether we should cut a segment before adding this moof to the buffer.
      if(state.fragmentBuffer.length > 0) {

        if(!state.firstSegmentEmitted) {

          // Fast path: emit the first segment as soon as we have one complete moof+mdat pair.
          outputSegment();
        } else {

          const elapsedMs = Date.now() - state.segmentStartTime;
          const targetMs = CONFIG.hls.segmentDuration * 1000;

          if(elapsedMs >= targetMs) {

            outputSegment();
          }
        }
      }

      // Rewrite tfdt.baseMediaDecodeTime in each traf by adding a constant per-track offset to Chrome's original values. The offset is 0 during normal playback (pure
      // pass-through) and computed lazily on the first moof per track after tab replacement. This preserves Chrome's wall-clock-based inter-track sync, eliminating the
      // A/V drift that accumulated under the old trun-based timestamp regeneration approach. Wrapped in try/catch so a malformed moof never crashes the segmenter — the
      // segment passes through with Chrome's original timestamps, which is better than dropping it entirely.
      try {

        const trackResults = offsetMoofTimestamps(box.data, state.trackOffsets);

        // Lazy offset initialization: compute offsets for all newly seen tracks before any corrective re-write. This two-pass approach is necessary because
        // offsetMoofTimestamps processes ALL trafs in the moof — if we re-called after each individual track's initialization, already-offset tracks would get
        // their offset applied a second time. By computing all offsets first, a single re-call applies them all atomically. This is safe because on the first
        // moof of a segmenter all tracks are uninitialized, so the first call writes all tracks with offset 0 (no-op) and the re-call applies all offsets to
        // Chrome's original values. Chrome's MediaRecorder declares all tracks in the moov at stream start and tab replacement creates a fresh segmenter, so a
        // new track appearing mid-stream in a multi-track moof alongside already-initialized tracks cannot occur.
        let needsRewrite = false;

        for(const [ trackId, result ] of trackResults) {

          if(!state.trackOffsetsInitialized.has(trackId)) {

            const initialValue = initialTrackTimestamps?.get(trackId);
            const offset = (initialValue !== undefined) ? (initialValue - result.originalTfdt) : 0n;

            state.trackOffsets.set(trackId, offset);
            state.trackOffsetsInitialized.add(trackId);

            if(offset !== 0n) {

              needsRewrite = true;
            }

            LOG.debug("streaming:segmenter", "Initialized offset for track %d: %s (initial=%s, chrome=%s).",
              trackId, String(offset), String(initialValue ?? 0n), String(result.originalTfdt));
          }
        }

        // The first call wrote uninitialized tracks with offset 0 (pure pass-through), so the buffer still contains Chrome's original tfdt values for those
        // tracks. Now that all offsets are stored, a single re-call applies them correctly without double-offsetting any track.
        if(needsRewrite) {

          offsetMoofTimestamps(box.data, state.trackOffsets);
        }

        // Track "next expected" for future tab replacement handoff and accumulate durations for EXTINF.
        for(const [ trackId, result ] of trackResults) {

          const trackOffset = state.trackOffsets.get(trackId) ?? 0n;

          state.trackTimestamps.set(trackId, result.originalTfdt + trackOffset + result.duration);

          // Accumulate duration for media-time EXTINF computation. No sanity check needed — Chrome's timestamps are trusted.
          if(result.duration > 0n) {

            const prev = state.segmentTrackDurations.get(trackId) ?? 0n;

            state.segmentTrackDurations.set(trackId, prev + result.duration);
          }
        }
      } catch {

        state.sessionStats.malformedMoofCount++;

        LOG.debug("streaming:segmenter", "Failed to apply offset to moof timestamps.");
      }

      // When keyframe debugging is enabled, parse traf/trun sample flags to detect whether this moof starts with a keyframe. Wrapped in try/catch for failure
      // isolation — a malformed moof should never crash the segmenter.
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      if(KEYFRAME_DEBUG) {

        try {

          const isKeyframe = detectMoofKeyframe(box.data);

          trackKeyframe(isKeyframe);
        } catch {

          state.indeterminateCount++;
        }
      }

      // Add moof to the fragment buffer.
      state.fragmentBuffer.push(box.data);

      return;
    }

    if(box.type === "mdat") {

      // Add mdat to the fragment buffer.
      state.fragmentBuffer.push(box.data);

      return;
    }

    // Other box types (styp, sidx, etc.) are passed through to the current segment.
    if(state.hasInit) {

      state.fragmentBuffer.push(box.data);
    }
  }

  // Create the MP4 box parser.
  const parser = createMP4BoxParser(handleBox);

  /**
   * Handles data from the input stream.
   */
  function handleData(chunk: Buffer): void {

    if(state.stopped) {

      return;
    }

    try {

      parser.push(chunk);
    } catch(error) {

      onError(error instanceof Error ? error : new Error(String(error)));
    }
  }

  /**
   * Handles the end of the input stream.
   */
  function handleEnd(): void {

    if(state.stopped) {

      return;
    }

    // Output any remaining data as a final segment.
    if(state.fragmentBuffer.length > 0) {

      outputSegment();
    }

    state.stopped = true;
    parser.flush();
    onStop();
  }

  /**
   * Handles input stream errors.
   */
  function handleError(error: Error): void {

    if(state.stopped) {

      return;
    }

    state.stopped = true;
    parser.flush();
    onError(error);
  }

  return {

    getInitSegment: (): Nullable<Buffer> => state.initSegment,

    getInitVersion: (): number => state.initVersion,

    getKeyframeStats: (): KeyframeStats => ({

      averageKeyframeIntervalMs: (state.keyframeCount >= 2) ? (state.totalKeyframeIntervalMs / (state.keyframeCount - 1)) : 0,
      indeterminateCount: state.indeterminateCount,
      keyframeCount: state.keyframeCount,
      maxKeyframeIntervalMs: (state.keyframeCount >= 2) ? state.maxKeyframeIntervalMs : 0,
      minKeyframeIntervalMs: (state.keyframeCount >= 2) ? state.minKeyframeIntervalMs : 0,
      nonKeyframeCount: state.nonKeyframeCount,
      segmentsWithoutLeadingKeyframe: state.segmentsWithoutLeadingKeyframe
    }),

    getLastSegmentSize: (): number => state.lastSegmentSize,

    getSegmentIndex: (): number => state.segmentIndex,

    getSessionStats: (): SessionStats => ({ ...state.sessionStats }),

    getTrackTimestamps: (): Map<number, bigint> => new Map(state.trackTimestamps),

    markDiscontinuity: (): void => {

      if(state.stopped) {

        return;
      }

      // Flush any accumulated fragments as a short segment so pre-recovery and post-recovery content are cleanly separated.
      outputSegment();

      state.pendingDiscontinuity = true;
    },

    pipe: (stream: Readable): void => {

      inputStream = stream;

      stream.on("data", handleData);
      stream.on("end", handleEnd);
      stream.on("error", handleError);
    },

    stop: (): void => {

      if(state.stopped) {

        return;
      }

      state.stopped = true;

      // Remove listeners from input stream.
      if(inputStream) {

        inputStream.removeListener("data", handleData);
        inputStream.removeListener("end", handleEnd);
        inputStream.removeListener("error", handleError);
      }

      // Flush the parser.
      parser.flush();
    }
  };
}
