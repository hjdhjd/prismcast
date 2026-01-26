/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ffmpeg.ts: FFmpeg process management for WebM to fMP4 transcoding.
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

/*
 * FFMPEG TRANSCODING
 *
 * When using WebM capture mode, Chrome's MediaRecorder outputs WebM container with H264 video and Opus audio. For HLS compatibility, we need fMP4 container with
 * H264 video and AAC audio. FFmpeg handles this conversion:
 *
 * - Video: Passed through unchanged (copy codec) - no quality loss, minimal CPU
 * - Audio: Transcoded from Opus to AAC - lightweight operation
 * - Container: Converted from WebM to fragmented MP4 with streaming-friendly flags
 *
 * The FFmpeg process runs for the lifetime of the stream, reading WebM from stdin and writing fMP4 to stdout. This output feeds directly into the existing fMP4
 * segmenter.
 */

/*
 * FFMPEG PATH RESOLUTION
 *
 * FFmpeg can be located in several places depending on how it was installed. We check in order of preference:
 * 1. Channels DVR bundled FFmpeg on macOS (~/Library/Application Support/ChannelsDVR/latest/ffmpeg)
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
 * Resolves the FFmpeg executable path. Checks Channels DVR (macOS), then the bundled ffmpeg-for-homebridge, then system PATH. The resolved path is cached for
 * subsequent calls.
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

/**
 * Result from spawning an FFmpeg process.
 */
export interface FFmpegProcess {

  // Function to gracefully terminate the FFmpeg process.
  kill: () => void;

  // The underlying child process for lifecycle tracking.
  process: ChildProcess;

  // Writable stream for piping WebM input to FFmpeg.
  stdin: Writable;

  // Readable stream for receiving fMP4 output from FFmpeg.
  stdout: Readable;
}

/**
 * Spawns an FFmpeg process configured to transcode WebM (H264+Opus) to fMP4 (H264+AAC). The process reads from stdin and writes to stdout, allowing it to be
 * integrated into a Node.js stream pipeline. Video is passed through unchanged; audio is transcoded from Opus to AAC for HLS compatibility.
 *
 * FFmpeg arguments:
 * - `-hide_banner -loglevel warning`: Reduce noise, only show warnings/errors
 * - `-i pipe:0`: Read input from stdin
 * - `-c:v copy`: Copy video stream without re-encoding (H264 passthrough)
 * - `-c:a aac -b:a <bitrate>`: Transcode audio to AAC at specified bitrate
 * - `-f mp4`: Output MP4 container format
 * - `-movflags frag_keyframe+empty_moov+default_base_moof`: Streaming-friendly fMP4 flags
 * - `pipe:1`: Write output to stdout
 * @param audioBitrate - Audio bitrate in bits per second (e.g., 256000 for 256 kbps).
 * @param onError - Callback invoked when FFmpeg exits unexpectedly or encounters an error.
 * @param streamId - Stream identifier for logging.
 * @returns FFmpeg process wrapper with stdin, stdout, and kill function.
 */
export function spawnFFmpeg(audioBitrate: number, onError: (error: Error) => void, streamId?: string): FFmpegProcess {

  // Use the cached FFmpeg path from resolveFFmpegPath(). This should always be set because isFFmpegAvailable() is called during startup, which populates the cache.
  // If somehow not set, fall back to "ffmpeg" and let spawn handle the error.
  const ffmpegPath = cachedFFmpegPath ?? "ffmpeg";

  // Use Apple's AudioToolbox AAC encoder on macOS for better quality and performance. Fall back to FFmpeg's built-in AAC encoder on other platforms.
  const aacEncoder = process.platform === "darwin" ? "aac_at" : "aac";

  const ffmpegArgs = [
    "-hide_banner",
    "-loglevel", "warning",
    "-i", "pipe:0",
    "-c:v", "copy",
    "-c:a", aacEncoder,
    "-b:a", String(audioBitrate),
    "-f", "mp4",
    "-movflags", "frag_keyframe+empty_moov+default_base_moof",
    "pipe:1"
  ];

  const ffmpeg = spawn(ffmpegPath, ffmpegArgs, {

    stdio: [ "pipe", "pipe", "pipe" ]
  });

  const logPrefix = streamId ? "[" + streamId + "] " : "";

  // Track whether graceful shutdown has been initiated. When true, we suppress error callbacks because any exit (whether from SIGTERM or stdin close) is expected.
  let shuttingDown = false;

  // Log FFmpeg stderr output (warnings and errors). stderr is guaranteed to be a Readable since we set stdio: ["pipe", "pipe", "pipe"].
  ffmpeg.stderr.on("data", (data: Buffer) => {

    // Suppress warnings during shutdown - truncated input warnings are expected when the capture stream closes.
    if(shuttingDown) {

      return;
    }

    const message = data.toString().trim();

    // Filter out common noise that isn't actionable.
    const noisePatterns = [ "Press [q] to stop", "frame=", "size=", "time=", "bitrate=", "speed=" ];

    if(noisePatterns.some((pattern) => message.includes(pattern))) {

      return;
    }

    if(message.length > 0) {

      LOG.debug("%sFFmpeg: %s", logPrefix, message);
    }
  });

  // Handle FFmpeg process exit.
  ffmpeg.on("exit", (code, signal) => {

    // During graceful shutdown, any exit is expected (whether from SIGTERM or stdin closing).
    if(shuttingDown) {

      return;
    }

    if(signal === "SIGTERM") {

      // Normal termination via kill() - don't treat as error.
      return;
    }

    if((code !== null) && (code !== 0)) {

      onError(new Error("FFmpeg exited with code " + String(code) + "."));
    } else if(signal) {

      onError(new Error("FFmpeg killed by signal " + signal + "."));
    }
  });

  // Handle spawn errors (e.g., FFmpeg not found).
  ffmpeg.on("error", (error) => {

    // During graceful shutdown, suppress errors from stdin pipe closing.
    if(shuttingDown) {

      return;
    }

    onError(error);
  });

  // Kill function for graceful shutdown. Sets the shuttingDown flag before sending SIGTERM so that any exit (whether from SIGTERM or stdin closing due to capture
  // stream ending) is treated as normal termination.
  const kill = (): void => {

    shuttingDown = true;

    if(!ffmpeg.killed) {

      ffmpeg.kill("SIGTERM");
    }
  };

  return {

    kill,
    process: ffmpeg,
    stdin: ffmpeg.stdin as Writable,
    stdout: ffmpeg.stdout as Readable
  };
}

/**
 * Checks if FFmpeg is available on the system. This resolves the FFmpeg path and caches it for use by spawnFFmpeg().
 * @returns Promise resolving to true if FFmpeg is available, false otherwise.
 */
export async function isFFmpegAvailable(): Promise<boolean> {

  const ffmpegPath = await resolveFFmpegPath();

  return ffmpegPath !== undefined;
}
