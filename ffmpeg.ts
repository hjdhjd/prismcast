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
import { CONFIG } from "../config/index.js";

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
 * - `-flush_packets 1`: Flush output immediately after each packet to minimize latency
 * - `pipe:1`: Write output to stdout
 * @param audioBitrate - Audio bitrate in bits per second (e.g., 256000 for 256 kbps).
 * @param onError - Callback invoked when FFmpeg exits unexpectedly or encounters an error.
 * @param streamId - Stream identifier for logging.
 * @param comment - Optional comment metadata (channel name or domain) to embed in the output.
 * @returns FFmpeg process wrapper with stdin, stdout, and kill function.
 */
export function spawnFFmpeg(audioBitrate: number, onError: (error: Error) => void, streamId?: string, comment?: string): FFmpegProcess {

  // Use the cached FFmpeg path from resolveFFmpegPath(). This should always be set because isFFmpegAvailable() is called during startup, which populates the cache.
  // If somehow not set, fall back to "ffmpeg" and let spawn handle the error.
  const ffmpegPath = cachedFFmpegPath ?? "ffmpeg";

  // Use Apple's AudioToolbox AAC encoder on macOS for better quality and performance. Fall back to FFmpeg's built-in AAC encoder on other platforms.
  const aacEncoder = process.platform === "darwin" ? "aac_at" : "aac";

  const ffmpegArgs = [
    "-hide_banner",
    "-loglevel", "warning",
    "-i", "pipe:0"
  ];

  // Add seek parameter if configured (skip X seconds to bypass tuning screens)
  if(CONFIG.streaming.seekSeconds > 0) {
    ffmpegArgs.push("-ss", String(CONFIG.streaming.seekSeconds));
  }

  ffmpegArgs.push(
    "-c:v", "copy",
    "-c:a", aacEncoder,
    "-b:a", String(audioBitrate),
    "-f", "mp4",
    "-movflags", "frag_keyframe+empty_moov+default_base_moof+skip_sidx+skip_trailer",
    "-flush_packets", "1"
  );

  // Add metadata comment if provided. This embeds "PrismCast - <channel>" in the output for identification.
  if(comment) {

    ffmpegArgs.push("-metadata", "comment=PrismCast - " + comment);
  }

  ffmpegArgs.push("pipe:1");

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
 * Spawns an FFmpeg process configured to remux fMP4 input to MPEG-TS output with codec copy. The process reads a continuous fMP4 stream (init segment followed by
 * media segments) from stdin and writes MPEG-TS to stdout. No transcoding occurs — both video (H264) and audio (AAC) are copied unchanged — so CPU usage is minimal.
 *
 * FFmpeg arguments:
 * - `-hide_banner -loglevel warning`: Reduce noise, only show warnings/errors
 * - `-f mp4 -i pipe:0`: Read fragmented MP4 from stdin
 * - `-c copy`: Copy both video and audio codecs without transcoding
 * - `-f mpegts`: Output MPEG-TS container format
 * - `pipe:1`: Write output to stdout
 * @param onError - Callback invoked when FFmpeg exits unexpectedly or encounters an error.
 * @param streamId - Optional stream identifier for logging.
 * @returns FFmpeg process wrapper with stdin, stdout, and kill function.
 */
export function spawnMpegTsRemuxer(onError: (error: Error) => void, streamId?: string): FFmpegProcess {

  const ffmpegBin = cachedFFmpegPath ?? "ffmpeg";

  const ffmpegArgs = [
    "-hide_banner",
    "-loglevel", "warning",
    "-i", "pipe:0"
  ];

  // Add seek parameter if configured (skip X seconds to bypass tuning screens)
  if(CONFIG.streaming.seekSeconds > 0) {
    ffmpegArgs.push("-ss", String(CONFIG.streaming.seekSeconds));
  }

  ffmpegArgs.push(
    "-c", "copy",
    "-f", "mpegts",
    "pipe:1"
  );

  const ffmpeg = spawn(ffmpegBin, ffmpegArgs, {

    stdio: [ "pipe", "pipe", "pipe" ]
  });

  const logPrefix = streamId ? "[" + streamId + "] " : "";

  // Track whether graceful shutdown has been initiated. When true, we suppress error callbacks because any exit is expected.
  let shuttingDown = false;

  // Log FFmpeg stderr output (warnings and errors).
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

      LOG.debug("%sMPEG-TS remuxer: %s", logPrefix, message);
    }
  });

  // Handle FFmpeg process exit.
  ffmpeg.on("exit", (code, signal) => {

    if(shuttingDown) {

      return;
    }

    if(signal === "SIGTERM") {

      return;
    }

    if((code !== null) && (code !== 0)) {

      onError(new Error("MPEG-TS remuxer exited with code " + String(code) + "."));
    } else if(signal) {

      onError(new Error("MPEG-TS remuxer killed by signal " + signal + "."));
    }
  });

  // Handle spawn errors (e.g., FFmpeg not found).
  ffmpeg.on("error", (error) => {

    if(shuttingDown) {

      return;
    }

    onError(error);
  });

  // Kill function for graceful shutdown.
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
