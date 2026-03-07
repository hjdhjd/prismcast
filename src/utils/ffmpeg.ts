/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ffmpeg.ts: FFmpeg process management for PrismCast.
 */
import type { Readable, Writable } from "node:stream";
import type { ChildProcess } from "node:child_process";
import { LOG } from "./logger.js";
import type { Nullable } from "../types/index.js";
import { existsSync } from "node:fs";
import ffmpegForHomebridge from "ffmpeg-for-homebridge";
import { homedir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";

// The ffmpeg-for-homebridge package has incorrect type definitions (declares named export but JS uses default export). Cast to the correct type.
const ffmpegPath = ffmpegForHomebridge as unknown as string | undefined;

/* When using WebM capture mode, Chrome's MediaRecorder outputs WebM container with H264 video and Opus audio. For HLS compatibility, we need fMP4 container with
 * H264 video and AAC audio. FFmpeg handles this conversion:
 *
 * - Video: Passed through unchanged (copy codec) - no quality loss, minimal CPU
 * - Audio: Transcoded from Opus to AAC - lightweight operation
 * - Container: Converted from WebM to fragmented MP4 with streaming-friendly flags
 *
 * The FFmpeg process runs for the lifetime of the stream, reading WebM from stdin and writing fMP4 to stdout. This output feeds directly into the existing fMP4
 * segmenter.
 */

/* FFmpeg can be located in several places depending on how it was installed. We check in order of preference:
 * 1. Channels DVR bundled FFmpeg:
 *    - macOS: ~/Library/Application Support/ChannelsDVR/latest/ffmpeg
 *    - Windows: C:\ProgramData\channelsdvr\latest\ffmpeg.exe
 *    - Linux: ~/channels-dvr/latest/ffmpeg, /usr/local/channels-dvr/latest/ffmpeg, /opt/channels-dvr/latest/ffmpeg
 * 2. Bundled FFmpeg from ffmpeg-for-homebridge package
 * 3. System PATH (standard installation via package manager or manual install)
 *
 * The resolved path is cached after the first successful lookup to avoid repeated filesystem checks.
 */

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
 * Resolves the FFmpeg executable path. Checks Channels DVR (macOS, Windows, Linux), then the bundled ffmpeg-for-homebridge, then system PATH. The resolved path is
 * cached for subsequent calls.
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
  if(ffmpegPath && existsSync(ffmpegPath) && (await checkFFmpegAtPath(ffmpegPath))) {

    cachedFFmpegPath = ffmpegPath;

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
 * values avoids "Invalid argument" failures when Plex tries to transcode the live session for remote clients. These are pure container metadata changes — the actual
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
    const noisePatterns = [ "Press [q] to stop", "frame=", "size=", "time=", "bitrate=", "speed=" ];

    if(noisePatterns.some((pattern) => message.includes(pattern))) {

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

      // Normal termination via kill() — do not treat as error.
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
 * Spawns an FFmpeg process configured to transcode WebM (H264+Opus) to fMP4 (H264+AAC). The process reads from stdin and writes to stdout, allowing it to be
 * integrated into a Node.js stream pipeline. Video is passed through unchanged; audio is transcoded from Opus to AAC for HLS compatibility.
 *
 * FFmpeg arguments:
 * - `-hide_banner -loglevel warning`: Reduce noise, only show warnings/errors
 * - `-probesize 16384`: Limit input probing to 16KB (Chrome's WebM header fits well under this) to minimize startup delay
 * - `-i pipe:0`: Read input from stdin
 * - `-c:v copy`: Copy video stream without re-encoding (H264 passthrough)
 * - `-c:a aac -b:a <bitrate>`: Transcode audio to AAC at specified bitrate
 * - `-f mp4`: Output MP4 container format
 * - `-movflags frag_keyframe+empty_moov+default_base_moof`: Streaming-friendly fMP4 flags
 * - `-flush_packets 1`: Flush output immediately after each packet to minimize latency
 * - `pipe:1`: Write output to stdout
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
    "-hide_banner",
    "-loglevel", "warning",
    "-probesize", "16384",
    "-i", "pipe:0",
    "-c:v", "copy",
    "-c:a", aacEncoder,
    "-b:a", String(audioBitrate),
    "-f", "mp4",
    "-movflags", "frag_keyframe+empty_moov+default_base_moof+skip_sidx+skip_trailer",
    "-flush_packets", "1"
  ];

  // Add metadata comment if provided. This embeds "PrismCast - <channel>" in the output for identification.
  if(comment) {

    ffmpegArgs.push("-metadata", "comment=PrismCast - " + comment);
  }

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
 * media segments) from stdin and writes MPEG-TS to stdout. No transcoding occurs — both video (H264) and audio (AAC) are copied unchanged — so CPU usage is minimal.
 *
 * FFmpeg arguments:
 * - `-hide_banner -loglevel warning`: Reduce noise, only show warnings/errors
 * - `-probesize 16384`: Limit input probing to 16KB (fMP4 init segment is ~1.3KB) to minimize startup delay
 * - `-f mp4 -i pipe:0`: Read fragmented MP4 from stdin
 * - `-c copy`: Copy both video and audio codecs without transcoding
 * - MPEG-TS output flags: See MPEGTS_OUTPUT_FLAGS for ATSC-conventional settings
 * - `pipe:1`: Write output to stdout
 * @param onError - Callback invoked when FFmpeg exits unexpectedly or encounters an error.
 * @param streamId - Optional stream identifier for logging.
 * @returns FFmpeg process wrapper with stdin, stdout, and kill function.
 */
export function spawnMpegTsRemuxer(onError: (error: Error) => void, streamId?: string): FFmpegProcess {

  const ffmpegArgs = [
    "-hide_banner",
    "-loglevel", "warning",
    "-probesize", "16384",
    "-f", "mp4",
    "-i", "pipe:0",
    "-c", "copy",
    ...MPEGTS_OUTPUT_FLAGS,
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
