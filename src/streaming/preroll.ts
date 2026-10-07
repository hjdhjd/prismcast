/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * preroll.ts: Preroll generation and compositor for immediate HLS response during stream startup.
 */
import { CAPTURE_BASELINE_CODEC, RECOGNIZED_CODECS } from "../types/index.ts";
import type { Express, Request, Response } from "express";
import { LOG, formatResolution, resolvePrerollFFmpegPath, timeoutSignal } from "../utils/index.ts";
import { createMP4BoxParser, offsetMoofTimestamps, parseMoovTrackInfo } from "./mp4Parser.ts";
import { CONFIG } from "../config/index.ts";
import type { CaptureCodec } from "./codec.ts";
import type { Clock } from "homebridge-plugin-utils";
import type { Nullable } from "../types/index.ts";
import type { PlaylistSegmentEntry } from "./playlistBuilder.ts";
import type { ResumePosition } from "./hlsResume.ts";
import { buildPlaylist } from "./playlistBuilder.ts";
import { getEffectiveCaptureCodec } from "./codec.ts";
import { getPresetViewport } from "../config/presets.ts";
import { spawn } from "node:child_process";
import { buffer as streamToBuffer } from "node:stream/consumers";

/* When a client requests an HLS playlist for a channel that is still starting up, PrismCast returns a startup playlist immediately to prevent HTTP timeouts. Channels
 * DVR requires at least one segment in the playlist - an empty playlist is rejected with "Playlist had no segments." This module generates PREROLL_TOTAL_DURATION
 * seconds of black+silence fMP4 at server startup, splits it into individual segments with naturally monotonic PTS, and serves them via global routes so the startup
 * playlist can reference valid media content.
 *
 * The baseline codec's variant is always generated, and the effective capture codec's variant as well when it is not the baseline (the user allows it AND the GPU
 * supports its hardware encoding). This ensures the preroll codec matches the capture codec at the preroll-to-live boundary, eliminating a cross-codec discontinuity
 * that would force a decoder reinitialization.
 * Preroll encodes with the bundled ffmpeg-for-homebridge binary when it is usable, because its encoder set is known to include libx265, and with an ffmpeg from
 * the system PATH otherwise. It never uses the Channels DVR FFmpeg, whose minimal encoder set may lack HEVC support.
 *
 * The preroll playlist is served progressively - segments are revealed over time based on elapsed wall-clock time, simulating a live stream. The client polls for
 * updates, sees new segments appear, and keeps playing without stalling for the full duration of the tune. When real content arrives, the fMP4 segmenter's composite
 * playlist takes over, including preroll entries in its sliding window during the transition. As real segments accumulate, preroll entries fall off the window and the
 * playlist becomes purely live content.
 */

// Total duration of preroll content in seconds. FFmpeg generates this much continuous black+silence fMP4, which is then split into individual segments at keyframe
// boundaries. The configured duration provides ample coverage for the slowest services with the progressive playlist delivering content in real time for the full
// tune duration.
const PREROLL_TOTAL_DURATION = 30;

// Number of segments to reveal immediately on the first preroll playlist request. The HLS spec (RFC 8216 Section 6.3.3) says a client should start playback 3 target
// durations from the end of a live playlist, so 4 segments gives the client 3 playable segments plus one buffer.
const PREROLL_INITIAL_WINDOW = 4;

// Deadline for a single preroll variant's encode, bounding this one-shot startup work so a wedged FFmpeg cannot hold the boot before the HTTP listener comes up.
// The slowest legitimate encode (libx265's slow preset on weak hardware) plausibly takes tens of seconds, so the deadline sits well above that: firing early costs
// only the preroll on a machine that was going to struggle anyway, and firing late still bounds a hung boot at a minute per variant. The boot encodes the baseline's
// variant and at most one more, the effective codec's when it is not the baseline, in sequence, so the worst pathological boot delay is twice this deadline before
// the server continues preroll-free.
const PREROLL_GENERATION_TIMEOUT_MS = 60000;

/* The video encoder arguments for each capture codec's preroll variant. The table is keyed by the codec union, so the compiler holds every recognized codec to its
 * own encoder arguments: a codec added to the recognized list does not compile until its preroll variant can be encoded.
 */
const PREROLL_VIDEO_ARGUMENTS: Readonly<Record<CaptureCodec, readonly string[]>> = {

  // Baseline profile and level 3.1 match Chrome's MediaRecorder output (confirmed via parseMoovCodecConfig telemetry). Slow preset and CRF 18 produce higher
  // quality - acceptable for a short duration of simple content generated once at startup.
  h264: [
    "-c:v", "libx264", "-preset", "slow", "-tune", "stillimage", "-profile:v", "baseline", "-level", "3.1", "-pix_fmt", "yuv420p",
    "-crf", "18"
  ],

  // The libx265 encoder does not support the -tune stillimage option (libx264-specific), so we omit it. The -tag:v hvc1 flag ensures the output uses the hvc1 box
  // type (matching Chrome's MediaRecorder HEVC output) rather than libx265's default hev1.
  hevc: [
    "-c:v", "libx265", "-preset", "slow", "-profile:v", "main", "-pix_fmt", "yuv420p",
    "-crf", "18", "-tag:v", "hvc1"
  ]
};

/**
 * Per-codec preroll variant. Each variant contains a complete set of preroll buffers (init segment + media segments) and their durations. The baseline codec's
 * variant is generated at startup, and the effective capture codec's variant as well when it is not the baseline, so the preroll codec matches the capture codec at
 * the preroll-to-live boundary.
 */
interface PrerollVariant {

  durations: number[];
  initSegment: Buffer;
  mediaSegments: Buffer[];
}

// Cached preroll variants keyed by capture codec. Generated once at startup and reused for all streams: the baseline codec's variant always, and the effective
// capture codec's variant when it is not the baseline.
const prerollVariants = new Map<string, PrerollVariant>();

// The file extension of every preroll media segment. The entries build their URLs with it and the segment route accepts exactly it, so a URL a playlist names is
// always one the route serves.
const PREROLL_SEGMENT_EXTENSION = ".m4s";

/**
 * How preroll generation reaches FFmpeg: resolving the binary to encode with, and running one encode to completion. generatePreroll takes it as a defaulted
 * parameter wired to the real resolver and spawnAndCollect, the way the FFmpeg context is injected, so a test drives the resolve, split and store path with a
 * synthetic encoder in place of a real FFmpeg.
 */
export interface PrerollEncoder {

  // Resolves the FFmpeg binary to encode with, or undefined when none is usable.
  readonly resolve: () => Promise<string | undefined>;

  // Runs one encode with the given binary and arguments, resolving with its complete output.
  readonly run: (ffmpegBin: string, args: string[]) => Promise<Buffer>;
}

// Public Accessors.

/**
 * Returns true when the specified preroll variant has been generated and is ready to serve.
 * @param codec - The preroll codec variant to check.
 */
export function isPrerollReady(codec: CaptureCodec): boolean {

  const variant = prerollVariants.get(codec);

  return (variant !== undefined) && (variant.mediaSegments.length > 0);
}

/**
 * Returns the number of preroll segments available for the specified codec. Stream registration records the count on the stream's HLS state, from which the
 * capture segmenter and the native proxy take it as the preroll-to-real segment index boundary.
 * @param codec - The preroll codec variant.
 * @returns The count of preroll media segments.
 */
export function getPrerollSegmentCount(codec: CaptureCodec): number {

  return prerollVariants.get(codec)?.mediaSegments.length ?? 0;
}

/**
 * Returns the duration of a specific preroll segment in seconds. Used by the segmenter's generatePlaylist() for EXTINF values when preroll entries are in the sliding
 * window.
 * @param codec - The preroll codec variant.
 * @param index - The zero-based segment index.
 * @returns The segment duration in seconds.
 */
export function getPrerollSegmentDuration(codec: CaptureCodec, index: number): number {

  return prerollVariants.get(codec)?.durations[index] ?? 2;
}

/**
 * Returns the total duration of all preroll segments in seconds. Used by the fmp4Segmenter to compute PTS offsets that make the remuxed capture's real content
 * timestamps continue from where the preroll ended, eliminating the PTS discontinuity at the preroll-to-live boundary.
 * @param codec - The preroll codec variant.
 * @returns The sum of all preroll segment durations in seconds.
 */
export function getPrerollTotalDurationSec(codec: CaptureCodec): number {

  const variant = prerollVariants.get(codec);

  if(!variant) {

    return 0;
  }

  let total = 0;

  for(const duration of variant.durations) {

    total += duration;
  }

  return total;
}

/**
 * Returns the maximum duration across all preroll segments for the specified codec, rounded up to the nearest integer. Used for TARGETDURATION computation in the
 * standalone preroll playlist built by generatePrerollPlaylist.
 * @param codec - The preroll codec variant.
 * @returns The ceiling of the maximum preroll segment duration.
 */
export function getPrerollMaxDuration(codec: CaptureCodec): number {

  const variant = prerollVariants.get(codec);

  if(!variant || (variant.durations.length === 0)) {

    return 2;
  }

  let max = 0;

  for(const duration of variant.durations) {

    if(duration > max) {

      max = duration;
    }
  }

  return Math.ceil(max);
}

/**
 * Returns the preroll codec that should be used for new streams. The preferred codec matches the effective capture codec (determined by the user's allowlist and GPU
 * capabilities) so the preroll-to-live boundary has no codec discontinuity. If the preferred variant was not generated (e.g., generation failed), falls back to
 * whichever variant is available.
 * @returns The codec to use for preroll, or CAPTURE_BASELINE_CODEC when no variant is ready.
 */
export function getPrerollCodec(): CaptureCodec {

  const preferred: CaptureCodec = getEffectiveCaptureCodec();

  if(isPrerollReady(preferred)) {

    return preferred;
  }

  // Any generated variant is better than none, so the first ready variant in the recognized codecs' order stands in for the preferred one, and the baseline stands
  // in when none is ready.
  return RECOGNIZED_CODECS.find((codec) => isPrerollReady(codec)) ?? CAPTURE_BASELINE_CODEC;
}

// Preroll Generation.

/**
 * Generates preroll fMP4 variants at startup. Prefers the bundled ffmpeg-for-homebridge binary, whose encoder set is known to carry both libx264 and libx265, and
 * falls back to an ffmpeg on the system PATH when the bundled binary is missing or will not run - the postinstall download that fetches it can fail, and a working
 * system FFmpeg is a better answer than no preroll at all. The baseline codec's variant is always generated, and the effective capture codec's variant as well
 * when it is not the baseline (the user allows it AND the GPU supports its hardware encoding), so the preroll codec matches the capture codec at the
 * preroll-to-live boundary.
 *
 * Each variant spawns FFmpeg to create PREROLL_TOTAL_DURATION seconds of black frame + silence as fragmented MP4, then splits the output into an init segment
 * (ftyp + moov) and individual media segments (moof + mdat pairs) using the MP4 box parser. Each segment has naturally monotonic PTS because it comes from a
 * continuous FFmpeg encode. If no FFmpeg is available or a variant fails, the system degrades gracefully - the blocking stream setup path is used instead. That
 * per-variant degradation is also the safety net for a PATH build that turns out to be missing an encoder, which the bundled binary would have had. Each
 * variant's encode is bounded by the generation deadline, so a hung FFmpeg degrades startup to a preroll-free boot instead of blocking it.
 * @param encoder - How the binary is resolved and each encode is run. Defaults to the real resolver and spawnAndCollect.
 */
export async function generatePreroll(encoder: PrerollEncoder = { resolve: resolvePrerollFFmpegPath, run: spawnAndCollect }): Promise<void> {

  const ffmpegBin = await encoder.resolve();

  if(!ffmpegBin) {

    LOG.warn("No FFmpeg is available for preroll generation: neither the bundled binary nor an ffmpeg on the system PATH could be run. Preroll generation is " +
      "skipped, so startup playlists will have no segments.");

    return;
  }

  // Size the preroll from the configured preset, which is the same surface capture renders and encodes at. Matching it prevents a resolution mismatch at the
  // preroll-to-live discontinuity boundary.
  const viewport = getPresetViewport(CONFIG);
  const size = formatResolution(viewport.width, viewport.height);

  // Every variant encodes with the one binary resolved above.
  const encode = async (args: string[]): Promise<Buffer> => encoder.run(ffmpegBin, args);

  // The baseline's variant is always generated, so a preroll stays available even when the effective codec's variant fails to encode.
  await generateVariant(encode, CAPTURE_BASELINE_CODEC, PREROLL_VIDEO_ARGUMENTS[CAPTURE_BASELINE_CODEC], size);

  // The effective capture codec's variant is generated as well when it is not the baseline, so the preroll matches the capture codec at the preroll-to-live boundary.
  const effectiveCodec = getEffectiveCaptureCodec();

  if(effectiveCodec !== CAPTURE_BASELINE_CODEC) {

    await generateVariant(encode, effectiveCodec, PREROLL_VIDEO_ARGUMENTS[effectiveCodec], size);
  }
}

/**
 * Generates one capture codec's preroll variant and stores it in the variant cache. Constructs the FFmpeg argument list from shared parameters (input sources,
 * duration, GOP size, audio codec, fMP4 output flags) combined with the codec-specific video encoder arguments.
 * @param encode - Runs one encode of the given arguments with the resolved FFmpeg binary, resolving with its complete output.
 * @param codec - The codec label for this variant.
 * @param videoArgs - Codec-specific FFmpeg arguments for the video encoder (e.g., "-c:v libx264 -preset slow ...").
 * @param size - The output resolution as "WxH".
 */
async function generateVariant(encode: (args: string[]) => Promise<Buffer>, codec: CaptureCodec, videoArgs: readonly string[], size: string): Promise<void> {

  // A 60-frame GOP at the source's 30 fps puts a keyframe every 2 seconds, and with frag_keyframe that gives 2-second preroll segments, which match the default HLS
  // segment duration and the 2-second fallbacks used in this module.
  const args = [
    "-hide_banner", "-nostats", "-loglevel", "warning",
    "-f", "lavfi", "-i", "color=black:size=" + size + ":rate=30:duration=" + String(PREROLL_TOTAL_DURATION),
    "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo",
    "-t", String(PREROLL_TOTAL_DURATION),
    ...videoArgs,
    "-g", "60",
    "-c:a", "aac", "-b:a", "128000",
    "-f", "mp4",
    "-movflags", "frag_keyframe+empty_moov+default_base_moof+skip_sidx+skip_trailer",
    "-flush_packets", "1",
    "pipe:1"
  ];

  try {

    const output = await encode(args);
    const variant = splitPrerollBuffers(output);

    if(variant && (variant.mediaSegments.length > 0)) {

      prerollVariants.set(codec, variant);

      const totalSize = variant.mediaSegments.reduce((sum, seg) => sum + seg.length, 0);

      LOG.debug("streaming:preroll", "Preroll %s ready: %d segments, init=%d bytes, media=%d bytes total.",
        codec, variant.mediaSegments.length, variant.initSegment.length, totalSize);
    } else {

      LOG.warn("Preroll %s generation produced incomplete output.", codec);
    }
  } catch(error) {

    LOG.warn("Preroll %s generation failed: %s.", codec, error instanceof Error ? error.message : String(error));
  }
}

// Internal Helpers.

/**
 * What a one-shot FFmpeg collection needs beyond the executable and its arguments: how long the child is allowed, and where that deadline's time comes from.
 */
export interface SpawnCollectOptions {

  // The clock the deadline arms on. Defaults to the system clock, so the production caller omits it and a test drives the kill by advancing its own.
  readonly clock?: Clock;

  // Milliseconds the child is allowed before it is killed. Defaults to PREROLL_GENERATION_TIMEOUT_MS.
  readonly timeoutMs?: number;
}

/**
 * Spawns FFmpeg with the given arguments and collects all stdout output into a single Buffer. Used for one-shot operations like preroll generation where the entire
 * output fits in memory. Exported so the deadline and collection semantics can be exercised directly against a Node child process; the production caller omits the
 * options and takes the defaults.
 * @param ffmpegBin - Path to the FFmpeg executable.
 * @param args - FFmpeg command-line arguments.
 * @param options - How long the child is allowed, and the clock that deadline arms on.
 * @returns Promise resolving to the complete stdout output as a Buffer. Rejects when the deadline passes, when the spawn itself fails, when the child exits with a
 *   nonzero code, or when an external signal terminates it.
 */
export async function spawnAndCollect(ffmpegBin: string, args: string[], options: SpawnCollectOptions = {}): Promise<Buffer> {

  const { clock, timeoutMs = PREROLL_GENERATION_TIMEOUT_MS } = options;

  /* The deadline arms on the Clock port and reaches the child through spawn's own signal option, so there is no separate timer to keep correct and no path where
   * the promise settles while the process lives on. SIGKILL rather than SIGTERM because a wedged encoder can ignore SIGTERM, the output is discarded on a timeout
   * anyway, and the encode holds no temp files or child processes needing a graceful teardown.
   */
  const bound = timeoutSignal(timeoutMs, { clock });
  const timeoutMessage = "FFmpeg timed out after " + String(timeoutMs) + "ms and was killed.";

  const ffmpeg = spawn(ffmpegBin, args, { killSignal: "SIGKILL", signal: bound.signal, stdio: [ "ignore", "pipe", "pipe" ] });

  ffmpeg.stderr.on("data", (data: Buffer) => {

    const message = data.toString().trim();

    if(message.length > 0) {

      LOG.debug("streaming:preroll", "FFmpeg: %s", message);
    }
  });

  // Join stdout collection with the process exit: streamToBuffer resolves when stdout closes, the exit listener decides whether the spawn succeeded, and the abort
  // deadline rejects the pair when FFmpeg hangs.
  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise: exitPromise, resolve: signalExit, reject: signalExitFailure } = Promise.withResolvers<void>();

  // A deadline kill reaches us as an AbortError on the "error" event, and the ordering of that event against "exit" is not contractual, so both listeners consult
  // the signal and translate an aborted spawn into the same timeout failure rather than the platform's abort text or a bare signal name.
  ffmpeg.on("error", (error: Error) => {

    signalExitFailure(bound.signal.aborted ? new Error(timeoutMessage) : error);
  });

  ffmpeg.on("exit", (code, signal) => {

    if(code === 0) {

      signalExit();

      return;
    }

    if(bound.signal.aborted) {

      signalExitFailure(new Error(timeoutMessage));

      return;
    }

    // A signal-killed process reports a null code, so name the signal rather than logging the uninformative "exited with code null". Preroll kills its own FFmpeg
    // in exactly one case, the generation deadline handled above, so any signal reaching this line is an external interruption to a one-shot startup encode and
    // stays a failure named by its signal rather than a graceful stop.
    signalExitFailure(new Error(signal ? ("FFmpeg killed by signal " + signal + ".") : ("FFmpeg exited with code " + String(code) + ".")));
  });

  try {

    const [output] = await Promise.all([ streamToBuffer(ffmpeg.stdout), exitPromise ]);

    return output;
  } finally {

    bound.cancel();
  }
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
 * @returns The parsed variant, or null if the output was incomplete.
 */
function splitPrerollBuffers(data: Buffer): Nullable<PrerollVariant> {

  const initBoxes: Buffer[] = [];
  const mediaSegments: Buffer[] = [];
  const durations: number[] = [];
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

        mediaSegments.push(Buffer.concat([ pendingMoof, box.data ]));
        pendingMoof = null;
      }
    }
  });

  parser.push(data);
  parser.flush();

  if(initBoxes.length === 0) {

    return null;
  }

  const initSegment = Buffer.concat(initBoxes);

  // Extract per-segment durations from the moov timescales and moof trun sample durations. The moov provides the timescale for converting raw sample durations to
  // seconds. Each moof's traf/trun boxes contain the accumulated sample durations for that fragment.
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- moovBox is set inside the parser callback; TS can't track closure mutations.
  if(moovBox && (mediaSegments.length > 0)) {

    const trackInfoMap = parseMoovTrackInfo(moovBox);

    // Build a timescale lookup from trackId to timescale.
    const timescales = new Map<number, number>();

    for(const [ trackId, info ] of trackInfoMap) {

      timescales.set(trackId, info.timescale);
    }

    // Extract duration from each segment's moof box. The offsetMoofTimestamps function with an empty offset map acts as a pure reader - it returns per-track
    // durations without modifying the buffer (it writes back the same tfdt values it read since the offset is 0).
    for(const segment of mediaSegments) {

      // The segment buffer is moof+mdat concatenated. The moof is the first box - parse it to find its size.
      if(segment.length < 8) {

        durations.push(2);

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
      durations.push(maxDurationSec > 0 ? maxDurationSec : 2);
    }
  }

  return { durations, initSegment, mediaSegments };
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

  // The index of the first preroll segment: zero for a fresh stream and the resume index for a resumed one, so the window's preroll range is the preroll count's
  // indices from there.
  readonly prerollStartIndex: number;

  // Number of real segments currently stored in the segment Map.
  realSegmentCount: number;
}

/**
 * Computes the sliding window start index for a composite playlist containing both preroll and real segments. The start index is the largest of these
 * constraints: (1) never before the preroll's first index, (2) the standard sliding window rule (current index minus window size), and (3) the preroll cap that
 * limits how many preroll entries appear in the window. The third constraint is the key addition - it prevents clients from playing through many seconds of
 * remaining preroll before reaching live content.
 *
 * @param options - Window computation parameters.
 * @returns The start index for the composite playlist window.
 */
export function computePrerollWindow(options: PrerollWindowOptions): number {

  const totalAvailable = options.prerollSegmentCount + options.realSegmentCount;
  const windowSize = Math.min(totalAvailable, options.maxSegments);

  return Math.max(options.prerollStartIndex, options.currentSegmentIndex - windowSize, options.prerollStartIndex + options.prerollSegmentCount - MAX_PREROLL_IN_WINDOW);
}

/**
 * Options for building preroll segment entries.
 */
export interface PrerollEntryOptions {

  // Base URL for constructing absolute preroll segment URIs (e.g., "http://192.168.1.100:5589").
  baseUrl: string;

  // The preroll codec variant. Determines the URL path segment (e.g., "/preroll/h264/segment0.m4s").
  codec: CaptureCodec;

  // Total number of preroll segments. Entries are produced for indices in [startIndex, prerollSegmentCount).
  prerollSegmentCount: number;

  // The zero-based preroll index to start from: the composite window's start minus the preroll's first index, so a resumed stream's window maps onto the preroll's
  // own segment numbering.
  startIndex: number;
}

/**
 * Builds an array of playlist segment entries for preroll segments within the given index range. Each entry has an absolute URL pointing to the global /preroll/ route
 * and the duration extracted from the preroll segment cache. The builder sets only the URL and duration, and the caller adds any marker its timeline needs (for
 * example, a resumed stream's opening discontinuity). PROGRAM-DATE-TIME is deliberately left out, because preroll is synthetic placeholder content.
 *
 * @param options - Entry construction parameters.
 * @returns Ordered array of preroll segment entries for the builder.
 */
export function buildPrerollEntries(options: PrerollEntryOptions): PlaylistSegmentEntry[] {

  const entries: PlaylistSegmentEntry[] = [];

  for(let i = options.startIndex; i < options.prerollSegmentCount; i++) {

    entries.push({

      duration: getPrerollSegmentDuration(options.codec, i),
      url: options.baseUrl + "/preroll/" + options.codec + "/segment" + String(i) + PREROLL_SEGMENT_EXTENSION
    });
  }

  return entries;
}

/**
 * Builds the URI of a preroll variant's init segment. This is the one spelling of that path: the startup playlist's map entry, the composite playlists the capture
 * segmenter and the native proxy build while preroll entries are in their window, and the init route itself all take it from here.
 * @param baseUrl - The server's external URL (e.g., "http://192.168.1.100:5589"), or the empty string for the route's own path.
 * @param codec - The preroll codec variant, or the route's codec parameter.
 * @returns The init segment's URI.
 */
export function buildPrerollInitUri(baseUrl: string, codec: CaptureCodec | ":codec"): string {

  return baseUrl + "/preroll/" + codec + "/init.mp4";
}

/**
 * Computes how many segments should be revealed given elapsed wall-clock time and a codec's segment durations. This is the pure reveal math, isolated from the
 * prerollVariants cache and from the clock so it can be exercised directly with plain arguments - no seeded variant, no timer mocking required. The initial window
 * (initialWindow segments) is revealed immediately so the client has enough content to begin playback per the HLS 3-from-end rule. Additional segments are revealed
 * one at a time as wall-clock time passes - a segment beyond the initial window appears once elapsed time reaches the summed duration of the segments past the
 * initial window up to and including itself.
 *
 * @param totalSegments - The number of segments available (from variant.mediaSegments.length). Distinct from durations.length - the durations array is tracked
 *   separately and may differ in length.
 * @param durations - Per-segment duration in seconds (from variant.durations). A missing entry at a given index falls back to 2 seconds.
 * @param elapsedSec - Elapsed wall-clock time in seconds since the preroll began. May be negative if the start time is in the future.
 * @param initialWindow - Number of segments revealed immediately, regardless of elapsed time. Defaults to PREROLL_INITIAL_WINDOW.
 * @returns The number of segments to reveal.
 */
export function computeReveal(totalSegments: number, durations: readonly number[], elapsedSec: number, initialWindow = PREROLL_INITIAL_WINDOW): number {

  let revealCount = Math.min(initialWindow, totalSegments);

  // Compute the total duration of the initial window. Segments beyond the initial window are revealed progressively - each one becomes visible once elapsed time
  // reaches the summed duration of the segments past the initial window up to and including itself. The initial window's own duration is excluded from that sum
  // because those segments were available from the start.
  let initialWindowDuration = 0;

  for(let i = 0; i < Math.min(initialWindow, totalSegments); i++) {

    initialWindowDuration += durations[i] ?? 2;
  }

  let cumulativeDuration = initialWindowDuration;

  for(let i = initialWindow; i < totalSegments; i++) {

    cumulativeDuration += durations[i] ?? 2;

    if(elapsedSec < (cumulativeDuration - initialWindowDuration)) {

      break;
    }

    revealCount = i + 1;
  }

  return revealCount;
}

/**
 * The inputs the progressive reveal is computed from. Named members rather than positional arguments because two of them are adjacent epoch instants, which a
 * positional call could transpose without the compiler noticing.
 */
export interface ProgressiveRevealOptions {

  // The preroll codec variant.
  readonly codec: CaptureCodec;

  // The epoch millisecond instant of the poll the reveal is being computed for.
  readonly now: number;

  // The epoch millisecond instant the preroll began.
  readonly prerollStartTime: number;
}

/**
 * Computes how many preroll segments should be visible based on elapsed wall-clock time since the preroll started. Thin adapter over computeReveal: looks up the
 * codec's cached variant, converts the two instants into elapsed seconds, and delegates the reveal math.
 *
 * @param options - See ProgressiveRevealOptions.
 * @returns The number of preroll segments to reveal.
 */
export function computeProgressiveReveal(options: ProgressiveRevealOptions): number {

  const { codec, now, prerollStartTime } = options;
  const variant = prerollVariants.get(codec);

  if(!variant) {

    return 0;
  }

  const elapsedSec = (now - prerollStartTime) / 1000;

  return computeReveal(variant.mediaSegments.length, variant.durations, elapsedSec);
}

// Playlist Generation.

/**
 * The inputs a progressive preroll playlist is built from. Extends the reveal options so the playlist builder hands one object to the reveal math and the two
 * epoch instants travel under their own names all the way down.
 */
export interface PrerollPlaylistOptions extends ProgressiveRevealOptions {

  // The server's external URL (e.g., "http://192.168.1.100:5589").
  readonly baseUrl: string;

  // The position a resumed stream continues from, its media sequence and its discontinuity sequence, or null for a fresh start.
  readonly resumePosition: Nullable<ResumePosition>;
}

/**
 * Generates a progressive HLS playlist referencing the global preroll segments for the specified codec. The playlist simulates a live stream by revealing segments
 * based on elapsed wall-clock time since the preroll started. On each client poll, more segments become visible - the client sees new content appear and keeps playing
 * without stalling.
 *
 * The playlist uses absolute URLs because preroll segments are served at global routes (/preroll/:codec/*) while the playlist itself is served under /hls/:name/.
 * Without absolute URLs, the client would request /hls/:name/preroll/... which does not exist. The playlist omits #EXT-X-ENDLIST so it behaves as a live playlist -
 * the client polls for updates and receives the real content playlist when ready.
 *
 * PROGRAM-DATE-TIME is intentionally omitted from the preroll playlist. Preroll is synthetic placeholder content - assigning it wall-clock timestamps would be
 * misleading and would create a backward time jump at the preroll-to-live boundary (preroll PDT would overshoot real content PDT because the preroll covers 30
 * seconds of content but real content typically arrives in ~15 seconds). PDT is emitted only on real segments once the segmenter produces them.
 *
 * A resumed stream's playlist continues the media sequence and the discontinuity sequence from its resume position, and marks its first entry with a
 * discontinuity, because its preroll opens a new timeline. A fresh stream's playlist starts its media sequence at 0 and carries neither the discontinuity
 * sequence nor the marker.
 *
 * @param options - See PrerollPlaylistOptions.
 * @returns The complete HLS playlist string, or an empty string if the preroll is not ready.
 */
export function generatePrerollPlaylist(options: PrerollPlaylistOptions): string {

  const { baseUrl, codec, resumePosition } = options;

  if(!isPrerollReady(codec)) {

    return "";
  }

  // The wider object satisfies the narrower reveal options, so the two instants reach the reveal math under their own names.
  const revealCount = computeProgressiveReveal(options);
  const entries = buildPrerollEntries({ baseUrl, codec, prerollSegmentCount: revealCount, startIndex: 0 });

  // A resumed stream's preroll opens a new timeline, its own init segment and its timestamps restarting, so its first entry carries the discontinuity marker and the
  // playlist reports the persisted count as its discontinuity sequence, a count of 0 included because the playlist then holds a marker. A fresh stream's playlist
  // carries neither.
  const [first] = entries;

  if(resumePosition && first) {

    first.discontinuity = true;
  }

  return buildPlaylist({

    discontinuitySequence: resumePosition?.discontinuityCount,
    initialMapUri: buildPrerollInitUri(baseUrl, codec),
    mediaSequence: resumePosition?.segmentIndex ?? 0,
    targetDuration: getPrerollMaxDuration(codec),
    version: 7
  }, entries);
}

// Route Setup.

/**
 * Registers the preroll segment routes on the Express application. Routes are codec-prefixed (/preroll/:codec/init.mp4 and /preroll/:codec/segmentN.m4s) so each
 * variant is served at a distinct path. The cached preroll buffers are served with immutable cache headers since the content never changes after generation.
 * @param app - The Express application.
 */
export function setupPrerollRoutes(app: Express): void {

  app.get(buildPrerollInitUri("", ":codec"), (req: Request, res: Response) => {

    // The variant map holds exactly the variants generated, so it decides which codecs a route serves, and any other parameter finds no variant. The type test
    // narrows the request parameter to the single string the map is keyed by.
    const codec = req.params["codec"];
    const variant = (typeof codec === "string") ? prerollVariants.get(codec) : undefined;

    if(!variant) {

      res.status(404).send("Preroll not available.");

      return;
    }

    res.setHeader("Cache-Control", "public, max-age=86400");
    res.setHeader("Content-Type", "video/mp4");
    res.send(variant.initSegment);
  });

  // Parameterized route serves individual preroll segment buffers. Each segment contains a distinct moof+mdat pair with unique PTS values from the continuous FFmpeg
  // output. The init.mp4 exact route above takes priority over this catch-all for init segment requests.
  app.get("/preroll/:codec/:segment", (req: Request, res: Response) => {

    const codec = req.params["codec"];
    const variant = (typeof codec === "string") ? prerollVariants.get(codec) : undefined;

    if(!variant) {

      res.status(404).send("Preroll not available.");

      return;
    }

    // Extract the segment index from the filename (e.g., "segment5.m4s" -> 5). The extension is tested against the constant the entries are built with, so the
    // pattern reads only the index.
    const segmentParam = req.params["segment"];
    const match = ((typeof segmentParam === "string") && segmentParam.endsWith(PREROLL_SEGMENT_EXTENSION)) ?
      /^segment(\d+)$/.exec(segmentParam.slice(0, -PREROLL_SEGMENT_EXTENSION.length)) : null;

    if(!match) {

      res.status(404).send("Preroll not available.");

      return;
    }

    const index = parseInt(match[1] ?? "", 10);

    if(!Number.isFinite(index) || (index < 0) || (index >= variant.mediaSegments.length)) {

      res.status(404).send("Preroll segment not found.");

      return;
    }

    res.setHeader("Cache-Control", "public, max-age=86400");
    res.setHeader("Content-Type", "video/mp4");
    res.send(variant.mediaSegments[index]);
  });
}
