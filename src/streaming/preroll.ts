/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * preroll.ts: Preroll generation and compositor for immediate HLS response during stream startup.
 */
import type { Express, Request, Response } from "express";
import { LOG, resolveFFmpegPath } from "../utils/index.js";
import { createMP4BoxParser, offsetMoofTimestamps, parseMoovTrackInfo } from "./mp4Parser.js";
import { CONFIG } from "../config/index.js";
import type { Nullable } from "../types/index.js";
import type { PlaylistSegmentEntry } from "./playlistBuilder.js";
import { buildPlaylist } from "./playlistBuilder.js";
import { getEffectiveViewport } from "../config/presets.js";
import { spawn } from "node:child_process";

/* When a client requests an HLS playlist for a channel that is still starting up, PrismCast returns a startup playlist immediately to prevent HTTP timeouts. Channels
 * DVR requires at least one segment in the playlist — an empty playlist is rejected with "Playlist had no segments." This module generates PREROLL_TOTAL_DURATION
 * seconds of black+silence fMP4 at server startup, splits it into individual segments with naturally monotonic PTS, and serves them via global routes so the startup
 * playlist can reference valid media content.
 *
 * The preroll playlist is served progressively — segments are revealed over time based on elapsed wall-clock time, simulating a live stream. The client polls for
 * updates, sees new segments appear, and keeps playing without stalling for the full duration of the tune. When real content arrives, the fMP4 segmenter's composite
 * playlist takes over, including preroll entries in its sliding window during the transition. As real segments accumulate, preroll entries fall off the window and the
 * playlist becomes purely live content.
 */

// Total duration of preroll content in seconds. FFmpeg generates this much continuous black+silence fMP4, which is then split into individual segments at keyframe
// boundaries. The configured duration provides ample coverage for the slowest providers with the progressive playlist delivering content in real time for the full
// tune duration.
const PREROLL_TOTAL_DURATION = 30;

// Number of segments to reveal immediately on the first preroll playlist request. The HLS spec (RFC 8216 Section 6.3.3) says a client should start playback 3 target
// durations from the end of a live playlist, so 4 segments gives the client 3 playable segments plus one buffer.
const PREROLL_INITIAL_WINDOW = 4;

// Cached preroll buffers. Generated once at startup, reused for all streams.
let prerollInitSegment: Nullable<Buffer> = null;

// Individual preroll media segment buffers. Each entry is one moof+mdat pair with naturally monotonic PTS from the continuous FFmpeg output. Unlike the previous
// implementation that served the same buffer N times (causing PTS restarts), each segment has distinct timestamps.
const prerollMediaSegments: Buffer[] = [];

// Per-segment durations in seconds. Derived from trun sample durations and moov timescales during splitPrerollBuffers(). Used for EXTINF values in the playlist.
const prerollSegmentDurations: number[] = [];

// Public Accessors.

/**
 * Returns true when the preroll init segment and at least one media segment have been generated and are ready to serve.
 */
export function isPrerollReady(): boolean {

  return (prerollInitSegment !== null) && (prerollMediaSegments.length > 0);
}

/**
 * Returns the number of preroll segments available. Used by the segmenter to determine the preroll-to-real segment index boundary.
 * @returns The count of preroll media segments.
 */
export function getPrerollSegmentCount(): number {

  return prerollMediaSegments.length;
}

/**
 * Returns the duration of a specific preroll segment in seconds. Used by the segmenter's generatePlaylist() for EXTINF values when preroll entries are in the sliding
 * window.
 * @param index - The zero-based segment index.
 * @returns The segment duration in seconds.
 */
export function getPrerollSegmentDuration(index: number): number {

  return prerollSegmentDurations[index] ?? 2;
}

/**
 * Returns the total duration of all preroll segments in seconds. Used by the fmp4Segmenter to compute PTS offsets that make Chrome's real content timestamps continue
 * from where the preroll ended, eliminating the PTS discontinuity at the preroll-to-live boundary.
 * @returns The sum of all preroll segment durations in seconds.
 */
export function getPrerollTotalDurationSec(): number {

  let total = 0;

  for(const duration of prerollSegmentDurations) {

    total += duration;
  }

  return total;
}

/**
 * Returns the maximum duration across all preroll segments, rounded up to the nearest integer. Used for TARGETDURATION computation in the composite playlist.
 * @returns The ceiling of the maximum preroll segment duration.
 */
export function getPrerollMaxDuration(): number {

  if(prerollSegmentDurations.length === 0) {

    return 2;
  }

  let max = 0;

  for(const duration of prerollSegmentDurations) {

    if(duration > max) {

      max = duration;
    }
  }

  return Math.ceil(max);
}

// Preroll Generation.

/**
 * Generates the preroll fMP4 at startup. Spawns FFmpeg to create PREROLL_TOTAL_DURATION seconds of black frame + silence as fragmented MP4, then splits
 * the output into an init segment (ftyp + moov) and individual media segments (moof + mdat pairs) using the MP4 box parser. Each segment has naturally monotonic PTS
 * because it comes from a continuous FFmpeg encode. If FFmpeg is unavailable or fails, the preroll is left uninitialized and the system degrades gracefully — the
 * blocking stream setup path is used instead.
 */
export async function generatePreroll(): Promise<void> {

  const ffmpegPath = await resolveFFmpegPath();

  if(!ffmpegPath) {

    LOG.warn("FFmpeg is not available. Preroll generation skipped — startup playlists will have no segments.");

    return;
  }

  // Use the effective viewport to match the resolution Chrome MediaRecorder will produce. This prevents a resolution mismatch at the preroll→live discontinuity
  // boundary. Baseline profile and level 3.1 match Chrome's MediaRecorder output (confirmed via parseMoovCodecConfig telemetry). Slow preset and CRF 18 produce
  // higher quality — acceptable for a short duration of simple content generated once at startup.
  const viewport = getEffectiveViewport(CONFIG);
  const size = String(viewport.width) + "x" + String(viewport.height);

  const args = [
    "-hide_banner", "-loglevel", "warning",
    "-f", "lavfi", "-i", "color=black:size=" + size + ":rate=30:duration=" + String(PREROLL_TOTAL_DURATION),
    "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo",
    "-t", String(PREROLL_TOTAL_DURATION),
    "-c:v", "libx264", "-preset", "slow", "-tune", "stillimage", "-profile:v", "baseline", "-level", "3.1", "-pix_fmt", "yuv420p",
    "-crf", "18",
    "-g", "60",
    "-c:a", "aac", "-b:a", "128000",
    "-f", "mp4",
    "-movflags", "frag_keyframe+empty_moov+default_base_moof+skip_sidx+skip_trailer",
    "-flush_packets", "1",
    "pipe:1"
  ];

  try {

    const output = await spawnAndCollect(ffmpegPath, args);

    splitPrerollBuffers(output);

    const init = prerollInitSegment;

    if(init && (prerollMediaSegments.length > 0)) {

      const totalSize = prerollMediaSegments.reduce((sum, seg) => sum + seg.length, 0);

      LOG.debug("streaming:preroll", "Preroll ready: %d segments, init=%d bytes, media=%d bytes total.", prerollMediaSegments.length, init.length, totalSize);
    } else {

      LOG.warn("Preroll generation produced incomplete output — startup playlists will have no segments.");

      return;
    }
  } catch(error) {

    LOG.warn("Preroll generation failed: %s.", error instanceof Error ? error.message : String(error));
  }
}

// Internal Helpers.

/**
 * Spawns FFmpeg with the given arguments and collects all stdout output into a single Buffer. Used for one-shot operations like preroll generation where the entire
 * output fits in memory.
 * @param ffmpegBin - Path to the FFmpeg executable.
 * @param args - FFmpeg command-line arguments.
 * @returns Promise resolving to the complete stdout output as a Buffer.
 */
async function spawnAndCollect(ffmpegBin: string, args: string[]): Promise<Buffer> {

  return new Promise((resolve, reject) => {

    const chunks: Buffer[] = [];
    const ffmpeg = spawn(ffmpegBin, args, { stdio: [ "ignore", "pipe", "pipe" ] });

    ffmpeg.stdout.on("data", (chunk: Buffer) => {

      chunks.push(chunk);
    });

    ffmpeg.stderr.on("data", (data: Buffer) => {

      const message = data.toString().trim();

      if(message.length > 0) {

        LOG.debug("streaming:preroll", "FFmpeg: %s", message);
      }
    });

    ffmpeg.on("error", (error) => {

      reject(error);
    });

    ffmpeg.on("exit", (code) => {

      if(code !== 0) {

        reject(new Error("FFmpeg exited with code " + String(code) + "."));

        return;
      }

      resolve(Buffer.concat(chunks));
    });
  });
}

/**
 * Splits an fMP4 buffer into an init segment (ftyp + moov) and individual media segments (moof + mdat pairs) using the MP4 box parser. The init segment contains codec
 * configuration needed by the HLS client to initialize its decoder. Each moof+mdat pair becomes a distinct segment buffer with naturally monotonic PTS timestamps.
 *
 * Duration extraction: the moov box is parsed with parseMoovTrackInfo() to get per-track timescales. Each moof is then parsed with offsetMoofTimestamps() (with an
 * empty offset map for pure pass-through) to extract per-track trun durations. The maximum duration across tracks (converted to seconds via timescale) becomes the
 * segment's EXTINF duration.
 *
 * @param data - The complete fMP4 output from FFmpeg.
 */
function splitPrerollBuffers(data: Buffer): void {

  const initBoxes: Buffer[] = [];
  let moovBox: Nullable<Buffer> = null;

  // Accumulator for the current moof+mdat pair. A segment is finalized when the mdat following a moof arrives.
  let pendingMoof: Nullable<Buffer> = null;

  const parser = createMP4BoxParser((box) => {

    if((box.type === "ftyp") || (box.type === "moov")) {

      initBoxes.push(box.data);

      if(box.type === "moov") {

        moovBox = box.data;
      }
    } else if(box.type === "moof") {

      // Start a new segment. If there's an orphaned moof without a following mdat (shouldn't happen with well-formed fMP4), it gets overwritten.
      pendingMoof = box.data;
    } else if(box.type === "mdat") {

      // Finalize the current segment by combining the pending moof with this mdat.
      if(pendingMoof) {

        prerollMediaSegments.push(Buffer.concat([ pendingMoof, box.data ]));
        pendingMoof = null;
      }
    }
  });

  parser.push(data);
  parser.flush();

  if(initBoxes.length > 0) {

    prerollInitSegment = Buffer.concat(initBoxes);
  }

  // Extract per-segment durations from the moov timescales and moof trun sample durations. The moov provides the timescale for converting raw sample durations to
  // seconds. Each moof's traf/trun boxes contain the accumulated sample durations for that fragment.
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- moovBox is set inside the parser callback; TS can't track closure mutations.
  if(moovBox && (prerollMediaSegments.length > 0)) {

    const trackInfoMap = parseMoovTrackInfo(moovBox);

    // Build a timescale lookup from trackId to timescale.
    const timescales = new Map<number, number>();

    for(const [ trackId, info ] of trackInfoMap) {

      timescales.set(trackId, info.timescale);
    }

    // Extract duration from each segment's moof box. The offsetMoofTimestamps function with an empty offset map acts as a pure reader — it returns per-track
    // durations without modifying the buffer (it writes back the same tfdt values it read since the offset is 0). A fresh map is created per segment to prevent
    // state accumulation across iterations.
    for(const segment of prerollMediaSegments) {

      // The segment buffer is moof+mdat concatenated. The moof is the first box — parse it to find its size.
      if(segment.length < 8) {

        prerollSegmentDurations.push(2);

        continue;
      }

      const moofSize = segment.readUInt32BE(0);
      const moofData = segment.subarray(0, moofSize);

      // Extract per-track durations from the moof. Take the maximum across tracks and convert to seconds.
      const trackResults = offsetMoofTimestamps(moofData, new Map<number, bigint>());

      let maxDurationSec = 0;

      for(const [ trackId, result ] of trackResults) {

        const timescale = timescales.get(trackId);

        if(timescale && (result.duration > 0n)) {

          const seconds = Number(result.duration) / timescale;

          if(seconds > maxDurationSec) {

            maxDurationSec = seconds;
          }
        }
      }

      // Fall back to 2 seconds if duration extraction failed (shouldn't happen with well-formed FFmpeg output).
      prerollSegmentDurations.push(maxDurationSec > 0 ? maxDurationSec : 2);
    }
  }
}

// Preroll Compositor.

// Maximum number of preroll entries allowed in a composite playlist window. When the composite playlist first appears, the client may be mid-stream in the progressive
// preroll playlist at a low segment index. Limiting preroll entries forces MEDIA-SEQUENCE past the client's current position, causing it to seek forward to near the
// live edge rather than playing through many seconds of remaining preroll before reaching real content.
const MAX_PREROLL_IN_WINDOW = 3;

/**
 * Options for computing the composite playlist sliding window when preroll is active.
 */
export interface PrerollWindowOptions {

  // The next segment index to be written by the segmenter. The playlist window covers indices [startIndex, currentSegmentIndex).
  currentSegmentIndex: number;

  // Maximum number of segments allowed in the playlist window (CONFIG.hls.maxSegments).
  maxSegments: number;

  // Number of preroll segments preceding real content.
  prerollSegmentCount: number;

  // Number of real segments currently stored in the segment Map.
  realSegmentCount: number;
}

/**
 * Computes the sliding window start index for a composite playlist containing both preroll and real segments. The start index is determined by three constraints:
 * (1) never negative, (2) the standard sliding window rule (current index minus window size), and (3) the preroll cap that limits how many preroll entries appear in
 * the window. The third constraint is the key addition — it prevents clients from playing through many seconds of remaining preroll before reaching live content.
 *
 * @param options - Window computation parameters.
 * @returns The start index for the composite playlist window.
 */
export function computePrerollWindow(options: PrerollWindowOptions): number {

  const totalAvailable = options.prerollSegmentCount + options.realSegmentCount;
  const windowSize = Math.min(totalAvailable, options.maxSegments);

  return Math.max(0, options.currentSegmentIndex - windowSize, options.prerollSegmentCount - MAX_PREROLL_IN_WINDOW);
}

/**
 * Options for building preroll segment entries.
 */
export interface PrerollEntryOptions {

  // Base URL for constructing absolute preroll segment URIs (e.g., "http://192.168.1.100:5589").
  baseUrl: string;

  // File extension for preroll segments. ".m4s" for fMP4. Parameterized for future format flexibility.
  extension: string;

  // Total number of preroll segments. Entries are produced for indices in [startIndex, prerollSegmentCount).
  prerollSegmentCount: number;

  // First preroll index to include in the entries. Typically the composite window's start index.
  startIndex: number;
}

/**
 * Builds an array of playlist segment entries for preroll segments within the given index range. Each entry has an absolute URL pointing to the global /preroll/ route
 * and the duration extracted from the preroll segment cache. No per-segment metadata tags (DISCONTINUITY, PROGRAM-DATE-TIME, etc.) are set — preroll is synthetic
 * placeholder content.
 *
 * @param options - Entry construction parameters.
 * @returns Ordered array of preroll segment entries for the builder.
 */
export function buildPrerollEntries(options: PrerollEntryOptions): PlaylistSegmentEntry[] {

  const entries: PlaylistSegmentEntry[] = [];

  for(let i = options.startIndex; i < options.prerollSegmentCount; i++) {

    entries.push({

      duration: getPrerollSegmentDuration(i),
      url: options.baseUrl + "/preroll/segment" + String(i) + options.extension
    });
  }

  return entries;
}

/**
 * Computes how many preroll segments should be visible based on elapsed wall-clock time since the preroll started. The initial window (PREROLL_INITIAL_WINDOW segments)
 * is revealed immediately so the client has enough content to begin playback per the HLS 3-from-end rule. Additional segments are revealed one at a time as wall-clock
 * time passes — each new segment appears when enough time has elapsed for the client to have consumed the prior content beyond the initial window.
 *
 * @param prerollStartTime - Wall-clock time when the preroll began.
 * @returns The number of preroll segments to reveal.
 */
export function computeProgressiveReveal(prerollStartTime: Date): number {

  const totalSegments = prerollMediaSegments.length;
  const elapsedSec = (Date.now() - prerollStartTime.getTime()) / 1000;

  let revealCount = Math.min(PREROLL_INITIAL_WINDOW, totalSegments);

  // Compute the total duration of the initial window. Segments beyond this threshold are revealed progressively — each one becomes visible when elapsed time
  // exceeds the cumulative duration of all segments before it, minus the initial window's duration (since those were available from the start).
  let initialWindowDuration = 0;

  for(let i = 0; i < Math.min(PREROLL_INITIAL_WINDOW, totalSegments); i++) {

    initialWindowDuration += prerollSegmentDurations[i] ?? 2;
  }

  let cumulativeDuration = initialWindowDuration;

  for(let i = PREROLL_INITIAL_WINDOW; i < totalSegments; i++) {

    cumulativeDuration += prerollSegmentDurations[i] ?? 2;

    if(elapsedSec < (cumulativeDuration - initialWindowDuration)) {

      break;
    }

    revealCount = i + 1;
  }

  return revealCount;
}

// Playlist Generation.

/**
 * Generates a progressive HLS playlist referencing the global preroll segments. The playlist simulates a live stream by revealing segments based on elapsed wall-clock
 * time since the preroll started. On each client poll, more segments become visible — the client sees new content appear and keeps playing without stalling.
 *
 * The playlist uses absolute URLs because preroll segments are served at global routes (/preroll/*) while the playlist itself is served under /hls/:name/. Without
 * absolute URLs, the client would request /hls/:name/preroll/segment0.m4s which does not exist. The playlist omits #EXT-X-ENDLIST so it behaves as a live playlist —
 * the client polls for updates and receives the real content playlist when ready.
 *
 * PROGRAM-DATE-TIME is intentionally omitted from the preroll playlist. Preroll is synthetic placeholder content — assigning it wall-clock timestamps would be
 * misleading and would create a backward time jump at the preroll-to-live boundary (preroll PDT would overshoot real content PDT because the preroll covers 30
 * seconds of content but real content typically arrives in ~15 seconds). PDT is emitted only on real segments once the segmenter produces them.
 *
 * @param baseUrl - The server's external URL (e.g., "http://192.168.1.100:5589").
 * @param startingSequence - The MEDIA-SEQUENCE offset. Zero for fresh starts. For resume streams, this is set to the saved segment index so the preroll
 *   playlist continues from the prior session's sequence range rather than restarting at 0.
 * @param prerollStartTime - Wall-clock time when the preroll began. Used to compute elapsed time and determine how many segments to reveal.
 * @returns The complete HLS playlist string, or an empty string if the preroll is not ready.
 */
export function generatePrerollPlaylist(baseUrl: string, startingSequence: number, prerollStartTime: Date): string {

  if(!isPrerollReady()) {

    return "";
  }

  const revealCount = computeProgressiveReveal(prerollStartTime);
  const entries = buildPrerollEntries({ baseUrl, extension: ".m4s", prerollSegmentCount: revealCount, startIndex: 0 });

  return buildPlaylist({

    initialMapUri: baseUrl + "/preroll/init.mp4",
    mediaSequence: startingSequence,
    targetDuration: getPrerollMaxDuration(),
    version: 7
  }, entries);
}

// Route Setup.

/**
 * Registers the preroll segment routes on the Express application. These serve the cached preroll buffers with immutable cache headers since the content never
 * changes after generation.
 * @param app - The Express application.
 */
export function setupPrerollRoutes(app: Express): void {

  app.get("/preroll/init.mp4", (_req: Request, res: Response) => {

    if(!prerollInitSegment) {

      res.status(404).send("Preroll not available.");

      return;
    }

    res.setHeader("Cache-Control", "public, max-age=86400");
    res.setHeader("Content-Type", "video/mp4");
    res.send(prerollInitSegment);
  });

  // Parameterized route serves individual preroll segment buffers. Each segment contains a distinct moof+mdat pair with unique PTS values from the continuous FFmpeg
  // output. The init.mp4 exact route above takes priority over this catch-all for init segment requests.
  app.get("/preroll/:segment", (req: Request, res: Response) => {

    // Extract the segment index from the filename (e.g., "segment5.m4s" → 5).
    const segmentParam = req.params.segment;
    const match = (typeof segmentParam === "string") ? /^segment(\d+)\.m4s$/.exec(segmentParam) : null;

    if(!match) {

      res.status(404).send("Preroll not available.");

      return;
    }

    const index = parseInt(match[1], 10);

    if((index < 0) || (index >= prerollMediaSegments.length)) {

      res.status(404).send("Preroll segment not found.");

      return;
    }

    res.setHeader("Cache-Control", "public, max-age=86400");
    res.setHeader("Content-Type", "video/mp4");
    res.send(prerollMediaSegments[index]);
  });
}
