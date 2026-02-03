/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fmp4Segmenter.ts: fMP4 HLS segmentation for PrismCast.
 */
import { createMP4BoxParser, detectMoofKeyframe } from "./mp4Parser.js";
import { storeInitSegment, storeSegment, updatePlaylist } from "./hlsSegments.js";
import { CONFIG } from "../config/index.js";
import { LOG } from "../utils/index.js";
import type { MP4Box } from "./mp4Parser.js";
import type { Nullable } from "../types/index.js";
import type { Readable } from "node:stream";

/*
 * FMP4 SEGMENTATION
 *
 * This module transforms a puppeteer-stream MP4 capture into HLS fMP4 segments. The overall flow is:
 *
 * 1. Receive MP4 data from puppeteer-stream (H.264 + AAC from either native capture or FFmpeg transcoding)
 * 2. Parse MP4 box structure to identify:
 *    - ftyp + moov: Initialization segment (codec configuration)
 *    - moof + mdat pairs: Media fragments
 * 3. Store init segment and accumulate media fragments into segments
 * 4. Generate and update the m3u8 playlist
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

  // Callback when the segmenter encounters an error.
  onError: (error: Error) => void;

  // Callback when the segmenter stops (stream ended or error).
  onStop: () => void;

  // If true, the first segment from this segmenter should have a discontinuity marker. Used after tab replacement to signal codec/timing change.
  pendingDiscontinuity?: boolean;

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
 * Result of creating an fMP4 segmenter.
 */
export interface FMP4SegmenterResult {

  // Returns a snapshot of the current keyframe detection statistics.
  getKeyframeStats: () => KeyframeStats;

  // Get the current segment index. Used by tab replacement to continue numbering from where the old segmenter left off.
  getSegmentIndex: () => number;

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

  // Total number of moof boxes that started with a keyframe.
  keyframeCount: number;

  // Timestamp of the last detected keyframe moof, for interval calculation. Null until the first keyframe is seen.
  lastKeyframeTime: Nullable<number>;

  // Maximum observed interval between consecutive keyframes in milliseconds.
  maxKeyframeIntervalMs: number;

  // Minimum observed interval between consecutive keyframes in milliseconds.
  minKeyframeIntervalMs: number;

  // Total number of moof boxes that did not start with a keyframe.
  nonKeyframeCount: number;

  // Whether the next segment should have a discontinuity marker (consumed when first segment is output).
  pendingDiscontinuity: boolean;

  // Actual wall-clock durations for each segment in seconds. Used by generatePlaylist() for accurate #EXTINF values. Pruned to keep only entries within the playlist
  // sliding window.
  segmentDurations: Map<number, number>;

  // Whether the current segment's first moof has been checked for keyframe status. Reset when outputSegment() clears the fragment buffer.
  segmentFirstMoofChecked: boolean;

  // Current media segment index.
  segmentIndex: number;

  // Time when current segment started accumulating.
  segmentStartTime: number;

  // Number of segments whose first moof was not a keyframe.
  segmentsWithoutLeadingKeyframe: number;

  // Whether the segmenter has been stopped.
  stopped: boolean;

  // Running total of keyframe intervals in milliseconds. Used with keyframeCount to compute the average.
  totalKeyframeIntervalMs: number;
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

// Segmenter Implementation.

/**
 * Creates an fMP4 segmenter that transforms MP4 input into HLS segments. The segmenter parses MP4 boxes, extracts the init segment, detects keyframes in each moof
 * fragment, and accumulates media fragments into segments based on the configured duration.
 * @param options - Segmenter options including stream ID and callbacks.
 * @returns The segmenter interface with pipe, stop, and keyframe stats methods.
 */
export function createFMP4Segmenter(options: FMP4SegmenterOptions): FMP4SegmenterResult {

  const { onError, onStop, pendingDiscontinuity, startingSegmentIndex, streamId } = options;

  // Initialize state.
  const state: SegmenterState = {

    discontinuityIndices: new Set(),
    firstSegmentEmitted: false,
    fragmentBuffer: [],
    hasInit: false,
    indeterminateCount: 0,
    initBoxes: [],
    keyframeCount: 0,
    lastKeyframeTime: null,
    maxKeyframeIntervalMs: 0,
    minKeyframeIntervalMs: Infinity,
    nonKeyframeCount: 0,
    pendingDiscontinuity: pendingDiscontinuity ?? false,
    segmentDurations: new Map(),
    segmentFirstMoofChecked: false,
    segmentIndex: startingSegmentIndex ?? 0,
    segmentStartTime: Date.now(),
    segmentsWithoutLeadingKeyframe: 0,
    stopped: false,
    totalKeyframeIntervalMs: 0
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
      "#EXT-X-MAP:URI=\"init.mp4\""
    ];

    // Add segment entries for each segment in the current playlist window.
    for(let i = startIndex; i < state.segmentIndex; i++) {

      // Add discontinuity marker before segments that follow a recovery event. Re-emit the init segment reference so clients explicitly reinitialize the decoder
      // with the current codec parameters.
      if(state.discontinuityIndices.has(i)) {

        lines.push("#EXT-X-DISCONTINUITY");
        lines.push("#EXT-X-MAP:URI=\"init.mp4\"");
      }

      // Use the actual recorded duration for this segment. Fall back to the configured target duration for segments that predate duration tracking (e.g. after
      // a hot restart with continuation).
      const duration = state.segmentDurations.get(i) ?? CONFIG.hls.segmentDuration;

      lines.push([ "#EXTINF:", String(duration.toFixed(3)), "," ].join(""));
      lines.push([ "segment", String(i), ".m4s" ].join(""));
    }

    lines.push("");

    return lines.join("\n");
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

    // Record the actual wall-clock duration of this segment. We floor at 0.1 seconds to prevent zero-duration entries that would violate HLS expectations.
    const actualDuration = Math.max(0.1, (Date.now() - state.segmentStartTime) / 1000);

    state.segmentDurations.set(state.segmentIndex, actualDuration);

    // Combine all fragment data into a single segment.
    const segmentData = Buffer.concat(state.fragmentBuffer);
    const segmentName = [ "segment", String(state.segmentIndex), ".m4s" ].join("");

    // Store the segment.
    storeSegment(streamId, segmentName, segmentData);

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

    // Clear the fragment buffer and reset the first-moof keyframe check for the next segment.
    state.fragmentBuffer = [];
    state.segmentFirstMoofChecked = false;
    state.segmentStartTime = Date.now();

    // Update the playlist.
    updatePlaylist(streamId, generatePlaylist());
  }

  /**
   * Processes keyframe detection results for a moof box. Updates running statistics and logs warnings when segments don't start with keyframes.
   */
  function trackKeyframe(isKeyframe: boolean | null): void {

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

        LOG.debug("Keyframe detected, interval: %dms.", intervalMs);
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
        }

        return;
      }
    }

    // Handle media fragment boxes (moof, mdat).
    if(box.type === "moof") {

      // Start of a new fragment. Check whether we should cut a segment before adding this moof to the buffer.
      if(state.fragmentBuffer.length > 0) {

        if(!state.firstSegmentEmitted) {

          // Fast path: emit the first segment as soon as we have one complete moof+mdat pair. This minimizes time-to-first-frame by making the first segment
          // available after just one fragment rather than waiting for the full target duration.
          outputSegment();
        } else {

          const elapsedMs = Date.now() - state.segmentStartTime;
          const targetMs = CONFIG.hls.segmentDuration * 1000;

          if(elapsedMs >= targetMs) {

            outputSegment();
          }
        }
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

    getKeyframeStats: (): KeyframeStats => ({

      averageKeyframeIntervalMs: (state.keyframeCount >= 2) ? (state.totalKeyframeIntervalMs / (state.keyframeCount - 1)) : 0,
      indeterminateCount: state.indeterminateCount,
      keyframeCount: state.keyframeCount,
      maxKeyframeIntervalMs: (state.keyframeCount >= 2) ? state.maxKeyframeIntervalMs : 0,
      minKeyframeIntervalMs: (state.keyframeCount >= 2) ? state.minKeyframeIntervalMs : 0,
      nonKeyframeCount: state.nonKeyframeCount,
      segmentsWithoutLeadingKeyframe: state.segmentsWithoutLeadingKeyframe
    }),

    getSegmentIndex: (): number => state.segmentIndex,

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
