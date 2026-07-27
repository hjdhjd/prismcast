/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ffmpeg.ts: FFmpeg process management for PrismCast. The path-resolution algorithm is a pure function over an FFmpegContext; the spawn-args builders are pure
 * functions over their own explicit parameters (audioBitrate/options, or none). The default I/O wiring for the FFmpegContext lives in ffmpeg.context.ts. Tests
 * construct context literals inline to exercise the resolution algorithm against synthetic filesystem scenarios without spawning real probe processes.
 */
import type { Readable, Writable } from "node:stream";
import type { ChildProcess } from "node:child_process";
import { LOG } from "./logger.ts";
import { createDefaultFFmpegContext } from "./ffmpeg.context.ts";
import { join } from "node:path";
import { memoizeAsync } from "./memo.ts";
import { spawn } from "node:child_process";

// FFmpeg stderr noise patterns that are suppressed from logging. The -nostats flag suppresses most progress output, but some FFmpeg builds or versions may still
// emit these patterns. The filter acts as a safety net to keep logs clean.
const FFMPEG_NOISE_PATTERNS = [ "Press [q] to stop", "frame=", "size=", "time=", "bitrate=", "speed=" ];

/**
 * The runtime context probeFFmpegPath (and getBundledFFmpegPath) consume. Models the I/O boundary used by path resolution: filesystem existence checks, the
 * user's home directory, the detected platform, the spawn-based probe, and the bundled FFmpeg path from the ffmpeg-for-homebridge npm package. Production wires
 * it through createDefaultFFmpegContext (in ffmpeg.context.ts); tests pass a context literal.
 */
export interface FFmpegContext {

  // The bundled FFmpeg path from ffmpeg-for-homebridge, or undefined if the package did not resolve.
  readonly bundledPath: string | undefined;

  // Whether a path exists on disk.
  readonly exists: (path: string) => boolean;

  // The user's home directory.
  readonly homedir: () => string;

  // The detected platform.
  readonly platform: NodeJS.Platform;

  // Probes whether FFmpeg works at the given path by running it with -version. Resolves to true on a clean exit code 0, false on any spawn error or non-zero exit.
  readonly probe: (path: string) => Promise<boolean>;
}

/**
 * Returns the path to the bundled ffmpeg-for-homebridge binary if it exists on disk. This binary ships as an npm dependency and has a full encoder set (libx264,
 * libx265, zoompan, overlay) regardless of what the user has installed. Used by preroll generation to guarantee HEVC encoding availability without depending on
 * the Channels DVR FFmpeg (which has a minimal encoder set) or a system installation.
 * @param ctx - The FFmpeg context. Defaults to createDefaultFFmpegContext() which wires real runtime I/O.
 * @returns The absolute path to the bundled FFmpeg, or undefined if the package did not resolve or the binary is missing.
 */
export function getBundledFFmpegPath(ctx: FFmpegContext = createDefaultFFmpegContext()): string | undefined {

  if(ctx.bundledPath && ctx.exists(ctx.bundledPath)) {

    return ctx.bundledPath;
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

/**
 * Probes the filesystem to find an FFmpeg executable. Pure algorithm over the supplied FFmpegContext: every disk check, every probe spawn, and every platform
 * branch flows through ctx. No internal caching - each call re-probes - so tests can construct independent scenarios. The production-facing resolveFFmpegPath
 * wraps this in a singleton memoizer.
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
 * @param ctx - The FFmpeg context. Defaults to createDefaultFFmpegContext() which wires real runtime I/O.
 * @returns Promise resolving to the FFmpeg path if found, or undefined if not available.
 */
export async function probeFFmpegPath(ctx: FFmpegContext = createDefaultFFmpegContext()): Promise<string | undefined> {

  // On macOS, check Channels DVR bundled FFmpeg first. Users of PrismCast with Channels DVR likely have this available.
  if(ctx.platform === "darwin") {

    const channelsDvrPath = join(ctx.homedir(), "Library", "Application Support", "ChannelsDVR", "latest", "ffmpeg");

    if(ctx.exists(channelsDvrPath) && (await ctx.probe(channelsDvrPath))) {

      return channelsDvrPath;
    }
  }

  // On Windows, check Channels DVR bundled FFmpeg. Users of PrismCast with Channels DVR likely have this available.
  if(ctx.platform === "win32") {

    const channelsDvrPath = join("C:", "ProgramData", "channelsdvr", "latest", "ffmpeg.exe");

    if(ctx.exists(channelsDvrPath) && (await ctx.probe(channelsDvrPath))) {

      return channelsDvrPath;
    }
  }

  // On Linux, check common Channels DVR installation paths. The Channels DVR setup script creates a channels-dvr directory in the current working directory when
  // run. The official recommendation is ~/channels-dvr, but users also install to /usr/local/channels-dvr and /opt/channels-dvr.
  if(ctx.platform === "linux") {

    const linuxChannelsDvrPaths = [
      join(ctx.homedir(), "channels-dvr", "latest", "ffmpeg"),
      join("/usr", "local", "channels-dvr", "latest", "ffmpeg"),
      join("/opt", "channels-dvr", "latest", "ffmpeg")
    ];

    for(const channelsDvrPath of linuxChannelsDvrPaths) {

      // eslint-disable-next-line no-await-in-loop
      if(ctx.exists(channelsDvrPath) && (await ctx.probe(channelsDvrPath))) {

        return channelsDvrPath;
      }
    }
  }

  // Check ffmpeg-for-homebridge bundled FFmpeg. This provides a reliable fallback without requiring manual FFmpeg installation.
  if(ctx.bundledPath && ctx.exists(ctx.bundledPath) && (await ctx.probe(ctx.bundledPath))) {

    return ctx.bundledPath;
  }

  // Finally, check if ffmpeg is available in the system PATH.
  if(await ctx.probe("ffmpeg")) {

    return "ffmpeg";
  }

  // FFmpeg not found anywhere.
  return undefined;
}

/**
 * Resolves the FFmpeg executable path against the production filesystem. Memoized via memoizeAsync: first call probes; subsequent calls return the cached
 * result. Used by production code paths (server startup, health checks, stream setup). Tests should use probeFFmpegPath against synthetic contexts instead -
 * this function intentionally takes no parameters, so tests have no way to substitute its dependencies and the caching contract stays sealed. The
 * memoization correctness (single-shot probe, concurrent-caller dedup, sticky rejection) is exercised by memo.test.ts against the underlying primitive.
 *
 * Sticky-rejection caveat: memoizeAsync caches a probe rejection for the lifetime of the process - all future callers receive the same rejected promise
 * without re-probing. probeFFmpegPath today returns undefined for the "not found" case rather than throwing, so this branch never fires in practice. Future
 * maintainers: do NOT change probeFFmpegPath to throw on transient conditions (permission errors, EAGAIN, etc.) without first reconsidering the caching
 * contract here. A throw-on-transient-failure path would permanently disable FFmpeg detection until restart, defeating the purpose of the cache. If the probe
 * needs to surface real errors, either return them as a tagged value type or compose memoizeAsync with a retry-after-failure layer.
 * @returns Promise resolving to the FFmpeg path if found, or undefined if not available.
 */
export const resolveFFmpegPath = memoizeAsync<string | undefined>(async () => probeFFmpegPath());

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

// FFmpeg Exit Classification.

/**
 * The outcome of classifying an FFmpeg process exit event. A discriminated union on outcome so callers can branch without inspecting an optional message field.
 */
export type FfmpegExitOutcome = { outcome: "error"; message: string } | { outcome: "normal" };

/**
 * Classifies an FFmpeg process exit from its reported code and signal. Pure: the shuttingDown suppression happens in the caller before this function is ever
 * invoked, so this function only ever classifies exits the caller actually wants judged. SIGTERM is graceful termination via kill() and always classifies as
 * normal regardless of the exit code, because a killed process can still report whatever code it happened to be mid-exit with. A non-zero code is checked before
 * signal presence, so a non-zero code accompanied by a signal still yields the code-based message; a truthy signal that is not SIGTERM (e.g. SIGKILL, SIGSEGV)
 * only produces its own message when the code was null.
 * @param code - The process exit code, or null if the process was terminated by a signal.
 * @param signal - The signal that terminated the process, or null if it exited via a normal code.
 * @param label - Descriptive label for the error message (e.g., "FFmpeg", "MPEG-TS remuxer").
 * @returns The classified outcome: normal, or error with a formatted message.
 */
export function classifyFfmpegExit(code: number | null, signal: NodeJS.Signals | null, label: string): FfmpegExitOutcome {

  if(signal === "SIGTERM") {

    // Normal termination via kill() - do not treat as error.
    return { outcome: "normal" };
  }

  if((code !== null) && (code !== 0)) {

    return { message: label + " exited with code " + String(code) + ".", outcome: "error" };
  }

  if(signal) {

    return { message: label + " killed by signal " + signal + ".", outcome: "error" };
  }

  return { outcome: "normal" };
}

// Internal Helper.

/**
 * Spawns an FFmpeg process with shared lifecycle management. Handles stderr logging with noise filtering, exit and error callbacks, and graceful shutdown for all
 * FFmpeg use cases. Callers provide the binary path, the FFmpeg arguments, and a descriptive label; this helper handles everything else.
 * @param options.args - FFmpeg command-line arguments.
 * @param options.ffmpegBin - The absolute path to the FFmpeg binary (or "ffmpeg" to defer to PATH lookup at spawn time).
 * @param options.label - Descriptive label for log messages and error strings (e.g., "FFmpeg", "MPEG-TS remuxer").
 * @param options.onError - Callback invoked when FFmpeg exits unexpectedly or encounters an error.
 * @param options.streamId - Optional stream identifier for log prefixing.
 * @returns FFmpeg process wrapper with stdin, stdout, and kill function.
 */
function spawnFFmpegProcess({ args, ffmpegBin, label, onError, streamId }: {
  args: string[];
  ffmpegBin: string;
  label: string;
  onError: (error: Error) => void;
  streamId?: string;
}): FFmpegProcess {

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

    const result = classifyFfmpegExit(code, signal, label);

    if(result.outcome === "error") {

      onError(new Error(result.message));
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

// Public FFmpeg arg builders. Pure functions whose output is the exact argv array spawnFFmpeg / spawnMpegTsRemuxer pass to spawn(). Extracted so tests can
// verify the FFmpeg command-line shape without launching real subprocesses; spawnFFmpeg / spawnMpegTsRemuxer are thin spawn() wrappers around these builders
// plus the shared lifecycle helper.

/**
 * Options for buildSpawnFFmpegArgs. Keeping the platform as an injected option (defaulting to process.platform) lets tests verify both the macOS and non-macOS
 * branches independently rather than re-asserting whatever branch the host happens to take.
 */
export interface SpawnFFmpegOptions {

  // Optional comment metadata (channel name or domain) to embed in the output's metadata. Omitted -> no -metadata flag emitted.
  readonly comment?: string;

  // The platform to build the args for. Defaults to process.platform. Tests pass an explicit value to exercise both branches of the AAC encoder selection.
  readonly platform?: NodeJS.Platform;
}

/**
 * Builds the argv for an FFmpeg invocation that remuxes Matroska to fMP4. Pure: depends only on the supplied audioBitrate and options. The platform-conditional
 * choice between Apple's AudioToolbox AAC encoder and FFmpeg's software encoder is parameterized via options.platform (defaulting to process.platform), so tests
 * can verify both branches without depending on the host.
 *
 * Chrome's MediaRecorder outputs a Matroska container with video (H264 or HEVC depending on hardware capabilities) and Opus audio. HLS clients need fMP4 with
 * AAC audio. The args copy video unchanged (no quality loss, minimal CPU), transcode audio from Opus to AAC, and rewrite the container as fragmented MP4 with
 * streaming-friendly flags suitable for incremental segmentation.
 *
 * @param audioBitrate - Audio bitrate in bits per second (e.g., 256000 for 256 kbps).
 * @param options - Optional comment metadata and platform override.
 * @returns The FFmpeg argv array (excluding the binary itself).
 */
export function buildSpawnFFmpegArgs(audioBitrate: number, options: SpawnFFmpegOptions = {}): string[] {

  const platform = options.platform ?? process.platform;
  const comment = options.comment;

  // Use Apple's AudioToolbox AAC encoder on macOS for better quality and performance. Fall back to FFmpeg's builtin AAC encoder on other platforms.
  const aacEncoder = platform === "darwin" ? "aac_at" : "aac";

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

  return ffmpegArgs;
}

/**
 * Builds the argv for an FFmpeg invocation that remuxes fMP4 input to MPEG-TS output with codec copy (no transcoding). Pure: takes no inputs and depends on no
 * external state.
 *
 * The output flags emit ATSC-conventional MPEG-TS resembling a real HDHomeRun CONNECT DUO so Plex's transcoder makes the right assumptions about stream structure
 * (PID assignments, PAT/PMT frequency). Container metadata only - the actual A/V data is untouched by -c copy.
 *
 * @returns The FFmpeg argv array (excluding the binary itself).
 */
export function buildMpegTsRemuxerArgs(): string[] {

  return [
    ...FFMPEG_INPUT_FLAGS,
    // Read fragmented MP4 from stdin and copy both video and audio without transcoding.
    "-f", "mp4",
    "-i", "pipe:0",
    "-c", "copy",
    ...MPEGTS_OUTPUT_FLAGS,
    // Write MPEG-TS to stdout.
    "pipe:1"
  ];
}

// Public FFmpeg Spawners.

/**
 * Spawns an FFmpeg process configured to remux Matroska to fMP4. The process reads from stdin and writes to stdout, allowing integration into a Node.js stream
 * pipeline. Callers provide the resolved FFmpeg binary path; resolution is the caller's responsibility (typically via resolveFFmpegPath() at startup or stream
 * setup) so this function stays a thin spawn() wrapper.
 * @param ffmpegBin - The absolute path to the FFmpeg binary (typically the result of resolveFFmpegPath()).
 * @param audioBitrate - Audio bitrate in bits per second (e.g., 256000 for 256 kbps).
 * @param onError - Callback invoked when FFmpeg exits unexpectedly or encounters an error.
 * @param streamId - Stream identifier for logging.
 * @param comment - Optional comment metadata (channel name or domain) to embed in the output.
 * @returns FFmpeg process wrapper with stdin, stdout, and kill function.
 */
export function spawnFFmpeg(ffmpegBin: string, audioBitrate: number, onError: (error: Error) => void, streamId?: string, comment?: string): FFmpegProcess {

  return spawnFFmpegProcess({

    args: buildSpawnFFmpegArgs(audioBitrate, { comment }),
    ffmpegBin,
    label: "FFmpeg",
    onError,
    streamId
  });
}

/**
 * Spawns an FFmpeg process configured to remux fMP4 input to MPEG-TS output with codec copy. The process reads a continuous fMP4 stream (init segment followed by
 * media segments) from stdin and writes MPEG-TS to stdout. No transcoding occurs - both video and audio are copied unchanged - so CPU usage is minimal.
 * @param ffmpegBin - The absolute path to the FFmpeg binary (typically the result of resolveFFmpegPath()).
 * @param onError - Callback invoked when FFmpeg exits unexpectedly or encounters an error.
 * @param streamId - Optional stream identifier for logging.
 * @returns FFmpeg process wrapper with stdin, stdout, and kill function.
 */
export function spawnMpegTsRemuxer(ffmpegBin: string, onError: (error: Error) => void, streamId?: string): FFmpegProcess {

  return spawnFFmpegProcess({

    args: buildMpegTsRemuxerArgs(),
    ffmpegBin,
    label: "MPEG-TS remuxer",
    onError,
    streamId
  });
}

/**
 * Checks if FFmpeg is available on the system. Wraps the production-cached resolveFFmpegPath, so first call probes the filesystem and subsequent calls return
 * the memoized result. Tests should use probeFFmpegPath against synthetic contexts instead - this function delegates to the singleton resolver and offers no
 * way for tests to substitute its dependencies.
 * @returns Promise resolving to true if FFmpeg is available, false otherwise.
 */
export async function isFFmpegAvailable(): Promise<boolean> {

  return (await resolveFFmpegPath()) !== undefined;
}
