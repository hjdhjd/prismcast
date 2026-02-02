/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fmp4Segmenter.ts: fMP4 HLS segmentation for PrismCast.
 */
import { storeInitSegment, storeSegment, updatePlaylist } from "./hlsSegments.js";
import { CONFIG } from "../config/index.js";
import type { MP4Box } from "./mp4Parser.js";
import type { Nullable } from "../types/index.js";
import type { Readable } from "node:stream";
import { createMP4BoxParser } from "./mp4Parser.js";

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
 * The parser is codec-agnostic—it only examines box types (ftyp, moov, moof, mdat) without parsing codec-specific data. Media passes through unchanged.
 */

// ─────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────

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
 * Result of creating an fMP4 segmenter.
 */
export interface FMP4SegmenterResult {

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

  // Boxes collected for the init segment (ftyp + moov).
  initBoxes: Buffer[];

  // Whether the next segment should have a discontinuity marker (consumed when first segment is output).
  pendingDiscontinuity: boolean;

  // Actual wall-clock durations for each segment in seconds. Used by generatePlaylist() for accurate #EXTINF values. Pruned to keep only entries within the playlist
  // sliding window.
  segmentDurations: Map<number, number>;

  // Current media segment index.
  segmentIndex: number;

  // Time when current segment started accumulating.
  segmentStartTime: number;

  // Whether the segmenter has been stopped.
  stopped: boolean;
}

// ─────────────────────────────────────────────────────────────
// Segmenter Implementation
// ─────────────────────────────────────────────────────────────

/**
 * Creates an fMP4 segmenter that transforms MP4 input into HLS segments. The segmenter parses MP4 boxes, extracts the init segment, and accumulates media fragments
 * into segments based on the configured duration.
 * @param options - Segmenter options including stream ID and callbacks.
 * @returns The segmenter interface with pipe and stop methods.
 */
export function createFMP4Segmenter(options: FMP4SegmenterOptions): FMP4SegmenterResult {

  const { onError, onStop, pendingDiscontinuity, startingSegmentIndex, streamId } = options;

  // Initialize state.
  const state: SegmenterState = {

    discontinuityIndices: new Set(),
    firstSegmentEmitted: false,
    fragmentBuffer: [],
    hasInit: false,
    initBoxes: [],
    pendingDiscontinuity: pendingDiscontinuity ?? false,
    segmentDurations: new Map(),
    segmentIndex: startingSegmentIndex ?? 0,
    segmentStartTime: Date.now(),
    stopped: false
  };

  // Reference to the input stream for cleanup.
  let inputStream: Nullable<Readable> = null;

  /**
   * Generates the m3u8 playlist content.
   */
  function generatePlaylist(): string {

    const lines: string[] = [
      "#EXTM3U",
      "#EXT-X-VERSION:7",
      [ "#EXT-X-TARGETDURATION:", String(Math.ceil(CONFIG.hls.segmentDuration)) ].join(""),
      [ "#EXT-X-MEDIA-SEQUENCE:", String(Math.max(0, state.segmentIndex - CONFIG.hls.maxSegments)) ].join(""),
      "#EXT-X-MAP:URI=\"init.mp4\""
    ];

    // Add segment entries. We only list segments that are currently in storage (based on maxSegments).
    const startIndex = Math.max(0, state.segmentIndex - CONFIG.hls.maxSegments);

    for(let i = startIndex; i < state.segmentIndex; i++) {

      // Add discontinuity marker before segments that follow a tab replacement.
      if(state.discontinuityIndices.has(i)) {

        lines.push("#EXT-X-DISCONTINUITY");
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

    // Clear the fragment buffer.
    state.fragmentBuffer = [];
    state.segmentStartTime = Date.now();

    // Update the playlist.
    updatePlaylist(streamId, generatePlaylist());
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
