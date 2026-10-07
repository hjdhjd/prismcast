/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ffmpeg.helpers.ts: Test-only FFmpeg child double. Co-located with the FFmpeg module, whose spawn wrapper builds the FFmpegProcess this double stands in for.
 *
 * Every suite whose rows reach the FFmpeg spawn takes its child from this one double rather than keeping a per-file variant, so the teardown flag and the streams
 * behave the same wherever a capture pipeline is driven. The streams are real PassThrough instances, so the production pipeline wiring runs unchanged against them.
 */
import type { ChildProcess } from "node:child_process";
import type { FFmpegProcess } from "./ffmpeg.ts";
import { PassThrough } from "node:stream";
import { declareKeysOf } from "../testing.helpers.ts";

/**
 * The FFmpeg child double. It answers the teardown-requested read exactly as the real wrapper does - set unconditionally by kill() - and exposes its streams so a
 * row can raise the events a dying child raises.
 */
export interface FakeFFmpeg extends FFmpegProcess {

  // How many times kill() has been called, so a row can read whether the live branch tore the pipeline down before escalating.
  readonly kills: () => number;
}

/**
 * Compile-time-complete enumeration of every key in FakeFFmpeg, each FFmpegProcess member and the kill counter. Pair with assertSameShape in
 * ffmpeg.helpers.test.ts to catch drift in either direction:
 *
 * - If FakeFFmpeg gains a key, declareKeysOf's completeness check fails to compile - the array must be updated.
 * - Once the array is updated, the self-test's parity row fails when the double does not populate the new key.
 */
export const FAKE_FFMPEG_KEYS = declareKeysOf<FakeFFmpeg>()([

  "isShuttingDown",
  "kill",
  "kills",
  "process",
  "stdin",
  "stdout"
] as const);

/**
 * Builds the FFmpeg double.
 * @returns The double, with real PassThrough streams so the production pipeline wiring runs unchanged.
 */
export function makeFakeFFmpeg(): FakeFFmpeg {

  let kills = 0;
  let shuttingDown = false;

  return {

    isShuttingDown: (): boolean => shuttingDown,
    kill: (): void => {

      kills++;
      shuttingDown = true;
    },
    kills: (): number => kills,
    process: {} as ChildProcess,
    stdin: new PassThrough(),
    stdout: new PassThrough()
  };
}
