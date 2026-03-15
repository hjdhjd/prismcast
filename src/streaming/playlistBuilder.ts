/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * playlistBuilder.ts: Shared HLS playlist builder for PrismCast.
 */

/* This module provides a format-neutral HLS playlist builder used by both the capture-mode fMP4 segmenter and the native HLS proxy. It accepts playlist-level options
 * and an ordered array of segment entries, then produces the m3u8 string. All tag formatting lives here — PROGRAM-DATE-TIME, DISCONTINUITY-SEQUENCE, DISCONTINUITY,
 * SCTE-35, EXT-X-MAP, EXTINF. The builder is stateless and pure: it receives data and returns a string. It knows nothing about preroll, capture mode, native mode,
 * windowing, or where segments come from.
 */

// Types.

/**
 * A single segment entry in the playlist. Each entry carries everything the builder needs to emit that segment's tags. The caller constructs entries from its own data
 * source (fMP4 segmenter state, native proxy metadata, or preroll segment cache) and passes them to buildPlaylist(). Properties are listed alphabetically per project
 * convention.
 */
export interface PlaylistSegmentEntry {

  // Emit #EXT-X-CUE-IN before this segment (SCTE-35 ad break end).
  cueIn?: boolean;

  // Emit #EXT-X-CUE-OUT before this segment (SCTE-35 ad break start). Empty string emits the bare tag without a duration; non-empty string emits with the duration
  // value appended.
  cueOut?: string;

  // Emit #EXT-X-CUE-OUT-CONT: before this segment (SCTE-35 ad break continuation with elapsed time and total duration).
  cueOutCont?: string;

  // Emit #EXT-X-DISCONTINUITY before this segment. Signals a PTS reset boundary to the client.
  discontinuity?: boolean;

  // Segment duration in seconds for #EXTINF. Formatted to three decimal places.
  duration: number;

  // Emit #EXT-X-MAP:URI="..." before this segment. Used at discontinuity boundaries in fMP4 streams to re-emit the init segment reference so clients explicitly
  // reinitialize the decoder with the current codec parameters.
  mapUri?: string;

  // Emit #EXT-X-PROGRAM-DATE-TIME: before this segment. ISO 8601 string providing authoritative wall-clock time for the segment.
  programDateTime?: string;

  // Segment URL. Absolute for preroll segments (served from global /preroll/ routes), relative for real segments (resolved against the playlist request URL).
  url: string;
}

/**
 * Playlist-level options for buildPlaylist(). Properties are listed alphabetically per project convention.
 */
export interface PlaylistOptions {

  // #EXT-X-DISCONTINUITY-SEQUENCE value. When undefined, the tag is omitted entirely — used when no discontinuities exist. When 0, the tag is emitted with value 0
  // — used when discontinuities exist in the playlist but none have scrolled off the beginning of the window.
  discontinuitySequence?: number;

  // #EXT-X-MAP URI for the first segment in the playlist window. Emitted before any segment entries. Used by fMP4 streams for the initialization segment; omitted
  // for MPEG-TS streams which carry codec configuration in every segment.
  initialMapUri?: string;

  // #EXT-X-MEDIA-SEQUENCE starting value for this playlist window.
  mediaSequence: number;

  // Floor value for #EXT-X-TARGETDURATION computation. The builder takes the maximum of this value and the maximum entry duration, then applies Math.ceil to produce
  // the integer required by RFC 8216. When omitted, the builder computes purely from entry durations. The capture path passes CONFIG.hls.segmentDuration as the floor
  // to avoid under-declaring when all segments are short.
  targetDuration?: number;

  // #EXT-X-VERSION value. 7 for fMP4 streams (requires EXT-X-MAP support), 3 for MPEG-TS streams.
  version: number;
}

// Playlist Builder.

/**
 * Builds an HLS playlist string from playlist options and an ordered array of segment entries. This is the single formatting function used by all playlist generators
 * in PrismCast — the capture-mode fMP4 segmenter, the native HLS proxy, and the standalone preroll playlist. The builder is stateless and pure: it iterates the
 * entries, emits the appropriate HLS tags for each, and returns the formatted m3u8 string.
 *
 * Per-segment tag ordering follows the HLS spec and preserves the ordering used by both existing generators: DISCONTINUITY, MAP, PROGRAM-DATE-TIME, CUE-IN,
 * CUE-OUT, CUE-OUT-CONT, EXTINF, then the segment URL.
 *
 * @param options - Playlist-level configuration (version, sequence numbers, init segment, target duration).
 * @param entries - Ordered array of segment entries to format.
 * @returns The complete m3u8 playlist string with a trailing newline.
 */
export function buildPlaylist(options: PlaylistOptions, entries: PlaylistSegmentEntry[]): string {

  // Compute TARGETDURATION as the maximum of the provided floor value and the maximum entry duration, rounded up to the nearest integer per RFC 8216 Section 4.3.3.1.
  let maxDuration = options.targetDuration ?? 0;

  for(const entry of entries) {

    if(entry.duration > maxDuration) {

      maxDuration = entry.duration;
    }
  }

  const lines: string[] = [
    "#EXTM3U",
    "#EXT-X-VERSION:" + String(options.version),
    "#EXT-X-TARGETDURATION:" + String(Math.ceil(maxDuration)),
    "#EXT-X-MEDIA-SEQUENCE:" + String(options.mediaSequence)
  ];

  // Emit DISCONTINUITY-SEQUENCE when the caller provides a value. The distinction between undefined (no discontinuities in the stream's history) and 0
  // (discontinuities exist but none have scrolled off the window) is meaningful for spec compliance — both capture and native paths control this independently.
  if(options.discontinuitySequence !== undefined) {

    lines.push("#EXT-X-DISCONTINUITY-SEQUENCE:" + String(options.discontinuitySequence));
  }

  // Emit the initial EXT-X-MAP for fMP4 streams. This applies to all segments until a per-segment mapUri overrides it at a discontinuity boundary.
  if(options.initialMapUri) {

    lines.push("#EXT-X-MAP:URI=\"" + options.initialMapUri + "\"");
  }

  // Emit per-segment tags and EXTINF for each entry.
  for(const entry of entries) {

    if(entry.discontinuity) {

      lines.push("#EXT-X-DISCONTINUITY");
    }

    if(entry.mapUri) {

      lines.push("#EXT-X-MAP:URI=\"" + entry.mapUri + "\"");
    }

    if(entry.programDateTime) {

      lines.push("#EXT-X-PROGRAM-DATE-TIME:" + entry.programDateTime);
    }

    if(entry.cueIn) {

      lines.push("#EXT-X-CUE-IN");
    }

    if(entry.cueOut !== undefined) {

      lines.push((entry.cueOut.length > 0) ? "#EXT-X-CUE-OUT:" + entry.cueOut : "#EXT-X-CUE-OUT");
    }

    if(entry.cueOutCont !== undefined) {

      lines.push("#EXT-X-CUE-OUT-CONT:" + entry.cueOutCont);
    }

    lines.push("#EXTINF:" + entry.duration.toFixed(3) + ",");
    lines.push(entry.url);
  }

  return lines.join("\n") + "\n";
}
