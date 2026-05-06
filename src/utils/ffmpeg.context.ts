/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ffmpeg.context.ts: The default adapter that produces an FFmpegContext from real runtime I/O. resolveFFmpegPath in ffmpeg.ts is a pure orchestrator over an
 * FFmpegContext; this file is the only place in the FFmpeg module that reads the filesystem, spawns probe subprocesses, or queries process.platform. Tests
 * bypass this file entirely by constructing FFmpegContext literals inline.
 */
import type { FFmpegContext } from "./ffmpeg.ts";
import bundledFFmpegPath from "ffmpeg-for-homebridge";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { spawn } from "node:child_process";

/**
 * Probes whether FFmpeg works at the given path by running it with -version. Resolves to true when the spawn succeeds and exit code is 0; false on any spawn
 * error (binary not found, permission denied) or non-zero exit. Used by the path-resolution loop to confirm a candidate path actually launches.
 * @param pathToCheck - Full path to the FFmpeg executable.
 * @returns Promise resolving to true if FFmpeg runs successfully at this path.
 */
async function checkFFmpegAtPath(pathToCheck: string): Promise<boolean> {

  const { promise, resolve } = Promise.withResolvers<boolean>();
  const ffmpeg = spawn(pathToCheck, ["-version"], { stdio: [ "ignore", "ignore", "ignore" ] });

  ffmpeg.on("error", () => { resolve(false); });
  ffmpeg.on("exit", (code) => { resolve(code === 0); });

  return promise;
}

/**
 * Builds the default FFmpegContext from real runtime I/O.
 * @returns An FFmpegContext populated from the live process.
 */
export function createDefaultFFmpegContext(): FFmpegContext {

  return {

    bundledPath: bundledFFmpegPath,
    exists: existsSync,
    homedir,
    platform: process.platform,
    probe: checkFFmpegAtPath
  };
}
