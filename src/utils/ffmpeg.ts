/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ffmpeg.ts: FFmpeg process management for PrismCast.
 */
import type { Readable, Writable } from "node:stream";
import type { ChildProcess } from "node:child_process";
import { LOG } from "./logger.js";
import type { Nullable } from "../types/index.js";
import bundledFFmpegPath from "ffmpeg-for-homebridge";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";

// FFmpeg stderr noise patterns that are suppressed from logging. The -nostats flag suppresses most progress output, but some FFmpeg builds or versions may still
// emit these patterns. The filter acts as a safety net to keep logs clean.
const FFMPEG_NOISE_PATTERNS = [ "Press [q] to stop", "frame=", "size=", "time=", "bitrate=", "speed=" ];

/**
 * Returns the path to the bundled ffmpeg-for-homebridge binary if it exists on disk. This binary ships as an npm dependency and has a full encoder set (libx264,
 * libx265, zoompan, overlay) regardless of what the user has installed. Used by preroll generation to guarantee HEVC encoding availability without depending on
 * the Channels DVR FFmpeg (which has a minimal encoder set) or a system installation.
 * @returns The absolute path to the bundled FFmpeg, or undefined if the package did not resolve or the binary is missing.
 */
export function getBundledFFmpegPath(): string | undefined {

  if(bundledFFmpegPath && existsSync(bundledFFmpegPath)) {

    return bundledFFmpegPath;
  }

  return undefined;
}

// Common input-side flags shared by every FFmpeg spawner. They silence the banner and per-frame stats, restrict logging to warnings and errors, tolerate corrupt
// input frames rather than aborting on read errors, and cap input probing at 16KB so the encoder begins emitting output as soon as the first segment lands.
const FFMPEG_INPUT_FLAGS: readonly string[] = [
  "-hide_banner",
  "-nostats",
  "-loglevel", "warning",
  "-fflags", "+discardcorrupt",
  "-err_detect", "ignore_err",
  "-probesize", "16384"
];

// Cached FFmpeg path after resolution. Null means not yet resolved, undefined means not found.
let cachedFFmpegPath: Nullable<string> | undefined = null;

/**
 * Checks if FFmpeg exists at a specific path by attempting to run it.
 * @param pathToCheck - Full path to the FFmpeg executable.
 * @returns Promise resolving to true if FFmpeg runs successfully at this path.
 */
async function checkFFmpegAtPath(pathToCheck: string): Promise<boolean> {

  return new Promise((resolve) => {

    const ffmpeg = spawn(pathToCheck, ["-version"], {

      stdio: [ "ignore", "ignore", "ignore" ]
    });

    ffmpeg.on("error", () => {

      resolve(false);
    });

    ffmpeg.on("exit", (code) => {

      resolve(code === 0);
    });
  });
}

/**
 * Resolves the FFmpeg executable path. The resolved path is cached after the first successful lookup to avoid repeated filesystem checks.
 *
 * FFmpeg can be located in several places depending on how it was installed. We check in order of preference:
 *
 * 1. Channels DVR bundled FFmpeg:
 *    - macOS: ~/Library/Application Support/ChannelsDVR/latest/ffmpeg
 *    - Windows: C:\ProgramData\channelsdvr\latest\ffmpeg.exe
 *    - Linux: ~/channels-dvr/latest/ffmpeg, /usr/local/channels-dvr/latest/ffmpeg, /opt/channels-dvr/latest/ffmpeg
 * 2. Bundled FFmpeg from the ffmpeg-for-homebridge package.
 * 3. System PATH (standard installation via package manager or manual install).
 *
 * @returns Promise resolving to the FFmpeg path if found, or undefined if not available.
 */
export async function resolveFFmpegPath(): Promise<string | undefined> {

  // Return cached result if already resolved.
  if(cachedFFmpegPath !== null) {

    return cachedFFmpegPath;
  }

  // On macOS, check Channels DVR bundled FFmpeg first. Users of PrismCast with Channels DVR likely have this available.
  if(process.platform === "darwin") {

    const channelsDvrPath = join(homedir(), "Library", "Application Support", "ChannelsDVR", "latest", "ffmpeg");

    if(existsSync(channelsDvrPath) && (await checkFFmpegAtPath(channelsDvrPath))) {

      cachedFFmpegPath = channelsDvrPath;

      return cachedFFmpegPath;
    }
  }

  // On Windows, check Channels DVR bundled FFmpeg. Users of PrismCast with Channels DVR likely have this available.
  if(process.platform === "win32") {

    const channelsDvrPath = join("C:", "ProgramData", "channelsdvr", "latest", "ffmpeg.exe");

    if(existsSync(channelsDvrPath) && (await checkFFmpegAtPath(channelsDvrPath))) {

      cachedFFmpegPath = channelsDvrPath;

      return cachedFFmpegPath;
    }
  }

  // On Linux, check common Channels DVR installation paths. The Channels DVR setup script creates a channels-dvr directory in the current working directory when
  // run. The official recommendation is ~/channels-dvr, but users also install to /usr/local/channels-dvr and /opt/channels-dvr.
  if(process.platform === "linux") {

    const linuxChannelsDvrPaths = [
      join(homedir(), "channels-dvr", "latest", "ffmpeg"),
      join("/usr", "local", "channels-dvr", "latest", "ffmpeg"),
      join("/opt", "channels-dvr", "latest", "ffmpeg")
    ];

    for(const channelsDvrPath of linuxChannelsDvrPaths) {

      // eslint-disable-next-line no-await-in-loop
      if(existsSync(channelsDvrPath) && (await checkFFmpegAtPath(channelsDvrPath))) {

        cachedFFmpegPath = channelsDvrPath;

        return cachedFFmpegPath;
      }
    }
  }

  // Check ffmpeg-for-homebridge bundled FFmpeg. This provides a reliable fallback without requiring manual FFmpeg installation.
  if(bundledFFmpegPath && existsSync(bundledFFmpegPath) && (await checkFFmpegAtPath(bundledFFmpegPath))) {

    cachedFFmpegPath = bundledFFmpegPath;

    return cachedFFmpegPath;
  }

  // Finally, check if ffmpeg is available in the system PATH.
  if(await checkFFmpegAtPath("ffmpeg")) {

    cachedFFmpegPath = "ffmpeg";

    return cachedFFmpegPath;
  }

  // FFmpeg not found anywhere.
  cachedFFmpegPath = undefined;

  return undefined;
}

// FFmpeg Process Types.

/**
 * Result from spawning an FFmpeg process.
 */
export interface FFmpegProcess {

  // Function to gracefully terminate the FFmpeg process.
  kill: () => void;

  // The underlying child process for lifecycle tracking.
  process: ChildProcess;

  // Writable stream for piping input to FFmpeg.
  stdin: Writable;

  // Readable stream for receiving output from FFmpeg.
  stdout: Readable;
}

// MPEG-TS Output Flags.

/* MPEG-TS muxer flags for the fMP4-to-MPEG-TS remuxer. Tuned to produce output resembling a real HDHomeRun CONNECT DUO (HDTC-2US) ATSC transport stream.
 * Plex's transcoder may make assumptions about stream structure based on the reported device model (PID assignments, PAT/PMT frequency). Using ATSC-conventional
 * values avoids "Invalid argument" failures when Plex tries to transcode the live session for remote clients. These are pure container metadata changes - the actual
 * A/V data is untouched by -c copy.
 */
const MPEGTS_OUTPUT_FLAGS = [
  "-f", "mpegts",
  "-mpegts_pmt_start_pid", "0x0020",
  "-mpegts_start_pid", "0x0031",
  "-mpegts_service_type", "digital_tv",
  "-pat_period", "0.1",
  "-pcr_period", "40",
  "-flush_packets", "1"
];

// Internal Helper.

/**
 * Spawns an FFmpeg process with shared lifecycle management. Handles stderr logging with noise filtering, exit and error callbacks, and graceful shutdown for all
 * FFmpeg use cases. Callers provide the FFmpeg arguments and a descriptive label; this helper handles everything else.
 * @param options.args - FFmpeg command-line arguments.
 * @param options.label - Descriptive label for log messages and error strings (e.g., "FFmpeg", "MPEG-TS remuxer").
 * @param options.onError - Callback invoked when FFmpeg exits unexpectedly or encounters an error.
 * @param options.streamId - Optional stream identifier for log prefixing.
 * @returns FFmpeg process wrapper with stdin, stdout, and kill function.
 */
function spawnFFmpegProcess({ args, label, onError, streamId }: {
  args: string[];
  label: string;
  onError: (error: Error) => void;
  streamId?: string;
}): FFmpegProcess {

  const ffmpegBin = cachedFFmpegPath ?? "ffmpeg";

  const ffmpeg = spawn(ffmpegBin, args, { stdio: [ "pipe", "pipe", "pipe" ] });
  const logPrefix = streamId ? "[" + streamId + "] " : "";

  // Track whether graceful shutdown has been initiated. When true, we suppress error callbacks because any exit (whether from SIGTERM or stdin close) is expected.
  let shuttingDown = false;

  // Log FFmpeg stderr output (warnings and errors). Suppress noise patterns that are not actionable.
  ffmpeg.stderr.on("data", (data: Buffer) => {

    if(shuttingDown) {

      return;
    }

    const message = data.toString().trim();

    if(FFMPEG_NOISE_PATTERNS.some((pattern) => message.includes(pattern))) {

      return;
    }

    if(message.length > 0) {

      LOG.debug("streaming:ffmpeg", "%s%s: %s", logPrefix, label, message);
    }
  });

  // Handle FFmpeg process exit.
  ffmpeg.on("exit", (code, signal) => {

    if(shuttingDown) {

      return;
    }

    if(signal === "SIGTERM") {

      // Normal termination via kill() - do not treat as error.
      return;
    }

    if((code !== null) && (code !== 0)) {

      onError(new Error(label + " exited with code " + String(code) + "."));
    } else if(signal) {

      onError(new Error(label + " killed by signal " + signal + "."));
    }
  });

  // Handle spawn errors (e.g., FFmpeg not found).
  ffmpeg.on("error", (error) => {

    if(shuttingDown) {

      return;
    }

    onError(error);
  });

  // Kill function for graceful shutdown. Sets the shuttingDown flag before sending SIGTERM so that any exit is treated as normal termination.
  const kill = (): void => {

    shuttingDown = true;

    if(!ffmpeg.killed) {

      ffmpeg.kill("SIGTERM");
    }
  };

  return {

    kill,
    process: ffmpeg,
    stdin: ffmpeg.stdin,
    stdout: ffmpeg.stdout
  };
}

// Public FFmpeg Spawners.

/**
 * Spawns an FFmpeg process configured to remux Matroska to fMP4. The process reads from stdin and writes to stdout, allowing integration into a Node.js stream
 * pipeline.
 *
 * Chrome's MediaRecorder outputs a Matroska container with video (H264 or HEVC depending on hardware capabilities) and Opus audio. HLS clients need fMP4 with
 * AAC audio. This function configures FFmpeg to bridge the gap: video is copied unchanged (no quality loss, minimal CPU), audio is transcoded from Opus to AAC
 * (lightweight), and the container is rewritten as fragmented MP4 with streaming-friendly flags. The output feeds directly into the fMP4 segmenter.
 *
 * The exact flag set lives inline in the args array below, with grouped comments above each set of related flags. Common input-side flags shared with
 * spawnMpegTsRemuxer come from FFMPEG_INPUT_FLAGS.
 *
 * @param audioBitrate - Audio bitrate in bits per second (e.g., 256000 for 256 kbps).
 * @param onError - Callback invoked when FFmpeg exits unexpectedly or encounters an error.
 * @param streamId - Stream identifier for logging.
 * @param comment - Optional comment metadata (channel name or domain) to embed in the output.
 * @returns FFmpeg process wrapper with stdin, stdout, and kill function.
 */
export function spawnFFmpeg(audioBitrate: number, onError: (error: Error) => void, streamId?: string, comment?: string): FFmpegProcess {

  // Use Apple's AudioToolbox AAC encoder on macOS for better quality and performance. Fall back to FFmpeg's built-in AAC encoder on other platforms.
  const aacEncoder = process.platform === "darwin" ? "aac_at" : "aac";

  const ffmpegArgs = [
    ...FFMPEG_INPUT_FLAGS,
    // Read Matroska from stdin.
    "-i", "pipe:0",
    // Pass video through unchanged (codec copy) and transcode audio to AAC at the requested bitrate. macOS uses Apple's hardware AAC encoder; other platforms fall
    // back to FFmpeg's software encoder.
    "-c:v", "copy",
    "-c:a", aacEncoder,
    "-b:a", String(audioBitrate),
    // Emit fragmented MP4 with a moov-less header so HLS segmenters can consume the stream incrementally. skip_sidx and skip_trailer keep init-segment overhead low.
    "-f", "mp4",
    "-movflags", "frag_keyframe+empty_moov+default_base_moof+skip_sidx+skip_trailer",
    // Flush each packet immediately to minimize the latency between MediaRecorder output and downstream segmentation.
    "-flush_packets", "1"
  ];

  // Add metadata comment if provided. This embeds "PrismCast - <channel>" in the output for identification.
  if(comment) {

    ffmpegArgs.push("-metadata", "comment=PrismCast - " + comment);
  }

  // Write output to stdout. This must come last so any preceding flags (including optional metadata) take effect on the output stream.
  ffmpegArgs.push("pipe:1");

  return spawnFFmpegProcess({

    args: ffmpegArgs,
    label: "FFmpeg",
    onError,
    streamId
  });
}

/**
 * Spawns an FFmpeg process configured to remux fMP4 input to MPEG-TS output with codec copy. The process reads a continuous fMP4 stream (init segment followed by
 * media segments) from stdin and writes MPEG-TS to stdout. No transcoding occurs - both video and audio are copied unchanged - so CPU usage is minimal.
 *
 * The exact flag set lives inline in the args array below. Common input-side flags shared with spawnFFmpeg come from FFMPEG_INPUT_FLAGS; ATSC-conventional output
 * flags come from MPEGTS_OUTPUT_FLAGS.
 *
 * @param onError - Callback invoked when FFmpeg exits unexpectedly or encounters an error.
 * @param streamId - Optional stream identifier for logging.
 * @returns FFmpeg process wrapper with stdin, stdout, and kill function.
 */
export function spawnMpegTsRemuxer(onError: (error: Error) => void, streamId?: string): FFmpegProcess {

  const ffmpegArgs = [
    ...FFMPEG_INPUT_FLAGS,
    // Read fragmented MP4 from stdin and copy both video and audio without transcoding.
    "-f", "mp4",
    "-i", "pipe:0",
    "-c", "copy",
    ...MPEGTS_OUTPUT_FLAGS,
    // Write MPEG-TS to stdout.
    "pipe:1"
  ];

  return spawnFFmpegProcess({

    args: ffmpegArgs,
    label: "MPEG-TS remuxer",
    onError,
    streamId
  });
}

/**
 * Checks if FFmpeg is available on the system. This resolves the FFmpeg path and caches it for use by spawnFFmpeg().
 * @returns Promise resolving to true if FFmpeg is available, false otherwise.
 */
export async function isFFmpegAvailable(): Promise<boolean> {

  const ffmpegPath = await resolveFFmpegPath();

  return ffmpegPath !== undefined;
}
