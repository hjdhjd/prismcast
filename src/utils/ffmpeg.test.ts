/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ffmpeg.test.ts: Unit tests for the FFmpeg helpers in ffmpeg.ts. Three layers are exercised:
 *
 * 1. probeFFmpegPath - the pure path-resolution algorithm. Tested by passing inline FFmpegContext literals that fake the platform, filesystem, home directory,
 *    probe results, and the bundled FFmpeg path. No real spawn, no real fs, no real platform check.
 *
 * 2. buildSpawnFFmpegArgs / buildMpegTsRemuxerArgs - pure argv builders. Tested by inspecting the returned arrays for the exact flag positions and values
 *    that production callers depend on.
 *
 * 3. getBundledFFmpegPath - pure existence check via context.
 *
 * The two spawner functions (spawnFFmpeg, spawnMpegTsRemuxer) drive real subprocesses and are not exercised here - they are thin wrappers around the spawn()
 * primitive plus the arg builders above. Their integration with the real FFmpeg binary belongs in e2e coverage.
 *
 * resolveFFmpegPath and isFFmpegAvailable are the production-cached singletons that probe the real filesystem; they have no test seam and no parameters because
 * their caching contract is intentionally sealed.
 */
import { buildMpegTsRemuxerArgs, buildSpawnFFmpegArgs, classifyFfmpegExit, getBundledFFmpegPath, probeFFmpegPath } from "./ffmpeg.ts";
import { describe, test } from "node:test";
import type { FFmpegContext } from "./ffmpeg.ts";
import assert from "node:assert/strict";
import { join } from "node:path";

/* makeFFmpegContext builds an FFmpegContext literal with sensible defaults. probe results are driven by an existsSet (paths recorded as existing) and a
 * probeResults map (paths that should report success when probed). Tests override only the fields they care about. The default platform is "linux" with no
 * paths existing so the resolution falls all the way through to the fallback "ffmpeg" probe.
 */
interface ContextOverrides {

  bundledPath?: string | undefined;
  existsSet?: Set<string>;
  homedir?: string;
  platform?: NodeJS.Platform;
  probeResults?: Map<string, boolean>;
}

function makeFFmpegContext(overrides: ContextOverrides = {}): { context: FFmpegContext; existsCalls: string[]; probeCalls: string[] } {

  const existsSet = overrides.existsSet ?? new Set<string>();
  const probeResults = overrides.probeResults ?? new Map<string, boolean>();
  const home = overrides.homedir ?? "/Users/test";
  const existsCalls: string[] = [];
  const probeCalls: string[] = [];

  const context: FFmpegContext = {

    bundledPath: "bundledPath" in overrides ? overrides.bundledPath : "/opt/bundled/ffmpeg",
    exists: (path: string): boolean => {

      existsCalls.push(path);

      return existsSet.has(path);
    },
    homedir: (): string => home,
    platform: overrides.platform ?? "linux",
    probe: async (path: string): Promise<boolean> => {

      probeCalls.push(path);

      return probeResults.get(path) ?? false;
    }
  };

  return { context, existsCalls, probeCalls };
}

describe("getBundledFFmpegPath", () => {

  test("returns the bundled path when it exists on disk", () => {

    const { context } = makeFFmpegContext({

      bundledPath: "/opt/bundled/ffmpeg",
      existsSet: new Set(["/opt/bundled/ffmpeg"])
    });

    assert.equal(getBundledFFmpegPath(context), "/opt/bundled/ffmpeg");
  });

  test("returns undefined when the bundled path does not exist", () => {

    // The package may resolve to a path string at import time, but the binary itself can be missing on disk (npm install --production stripped it, antivirus
    // quarantine, etc.). The function checks both - resolving to a path is necessary but not sufficient.
    const { context } = makeFFmpegContext({

      bundledPath: "/opt/bundled/ffmpeg",
      existsSet: new Set()
    });

    assert.equal(getBundledFFmpegPath(context), undefined);
  });

  test("returns undefined when ctx.bundledPath is undefined (package failed to resolve)", () => {

    // ffmpeg-for-homebridge can return undefined if the package didn't resolve at import time (rare, but possible in some bundler configurations).
    const { context } = makeFFmpegContext({ bundledPath: undefined });

    assert.equal(getBundledFFmpegPath(context), undefined);
  });
});

describe("probeFFmpegPath", () => {

  test("on macOS, returns the Channels DVR path when it exists and probes successfully", async () => {

    const channelsDvrPath = "/Users/test/Library/Application Support/ChannelsDVR/latest/ffmpeg";
    const { context, probeCalls } = makeFFmpegContext({

      existsSet: new Set([channelsDvrPath]),
      platform: "darwin",
      probeResults: new Map([[ channelsDvrPath, true ]])
    });

    assert.equal(await probeFFmpegPath(context), channelsDvrPath, "first-choice macOS path returned");
    assert.deepEqual(probeCalls, [channelsDvrPath], "probed only the first-choice path");
  });

  test("on macOS, falls through when the Channels DVR path exists but probe fails", async () => {

    // The path may exist (e.g., the file is there but the binary is broken or the wrong architecture). The probe is the authoritative check.
    const channelsDvrPath = "/Users/test/Library/Application Support/ChannelsDVR/latest/ffmpeg";
    const bundledPath = "/opt/bundled/ffmpeg";
    const { context } = makeFFmpegContext({

      bundledPath,
      existsSet: new Set([ channelsDvrPath, bundledPath ]),
      platform: "darwin",
      probeResults: new Map([
        [ channelsDvrPath, false ],
        [ bundledPath, true ]
      ])
    });

    assert.equal(await probeFFmpegPath(context), bundledPath, "falls through to the bundled path");
  });

  test("on macOS, falls through when the Channels DVR path does not exist", async () => {

    const bundledPath = "/opt/bundled/ffmpeg";
    const { context } = makeFFmpegContext({

      bundledPath,
      existsSet: new Set([bundledPath]),
      platform: "darwin",
      probeResults: new Map([[ bundledPath, true ]])
    });

    assert.equal(await probeFFmpegPath(context), bundledPath, "falls through to bundled when first-choice missing");
  });

  test("on Windows, returns the ProgramData Channels DVR path when present", async () => {

    // Compose the expected path through node:path/join so the test matches whatever separator convention the production code uses on the host running the test.
    // The production code calls join("C:", "ProgramData", "channelsdvr", "latest", "ffmpeg.exe"); we mirror that here so the comparison is platform-neutral.
    const channelsDvrPath = join("C:", "ProgramData", "channelsdvr", "latest", "ffmpeg.exe");
    const { context } = makeFFmpegContext({

      existsSet: new Set([channelsDvrPath]),
      platform: "win32",
      probeResults: new Map([[ channelsDvrPath, true ]])
    });

    assert.equal(await probeFFmpegPath(context), channelsDvrPath);
  });

  test("on Linux, tries the home-directory path first", async () => {

    const homePath = "/Users/test/channels-dvr/latest/ffmpeg";
    const { context, probeCalls } = makeFFmpegContext({

      existsSet: new Set([homePath]),
      platform: "linux",
      probeResults: new Map([[ homePath, true ]])
    });

    assert.equal(await probeFFmpegPath(context), homePath);
    assert.deepEqual(probeCalls, [homePath], "stopped at the first successful path");
  });

  test("on Linux, falls through to /usr/local then /opt in order", async () => {

    const usrLocalPath = "/usr/local/channels-dvr/latest/ffmpeg";
    const optPath = "/opt/channels-dvr/latest/ffmpeg";
    const { context, probeCalls } = makeFFmpegContext({

      existsSet: new Set([ usrLocalPath, optPath ]),
      platform: "linux",
      probeResults: new Map([
        [ usrLocalPath, true ],
        [ optPath, true ]
      ])
    });

    assert.equal(await probeFFmpegPath(context), usrLocalPath, "/usr/local wins over /opt");
    assert.deepEqual(probeCalls, [usrLocalPath], "stopped at /usr/local without probing /opt");
  });

  test("on Linux, falls all the way through to /opt when home and /usr/local are absent", async () => {

    const optPath = "/opt/channels-dvr/latest/ffmpeg";
    const { context } = makeFFmpegContext({

      existsSet: new Set([optPath]),
      platform: "linux",
      probeResults: new Map([[ optPath, true ]])
    });

    assert.equal(await probeFFmpegPath(context), optPath);
  });

  test("falls through to the bundled FFmpeg when no platform-specific path matches", async () => {

    // Cross-platform fallback: if the Channels DVR paths don't exist or don't probe, the bundled FFmpeg is the next-best option.
    const bundledPath = "/opt/bundled/ffmpeg";
    const { context } = makeFFmpegContext({

      bundledPath,
      existsSet: new Set([bundledPath]),
      platform: "darwin",
      probeResults: new Map([[ bundledPath, true ]])
    });

    assert.equal(await probeFFmpegPath(context), bundledPath);
  });

  test("falls through to bare 'ffmpeg' when bundled path exists on disk but its probe fails", async () => {

    // Priority-fallthrough boundary: the bundled probe is the second-to-last candidate. If it exists but the probe rejects (binary present but corrupt or
    // wrong-arch), the resolver must fall through to the bare "ffmpeg" probe rather than returning the failed bundled path.
    const bundledPath = "/opt/bundled/ffmpeg";
    const { context, probeCalls } = makeFFmpegContext({

      bundledPath,
      existsSet: new Set([bundledPath]),
      platform: "linux",
      probeResults: new Map([
        [ bundledPath, false ],
        [ "ffmpeg", true ]
      ])
    });

    assert.equal(await probeFFmpegPath(context), "ffmpeg", "fell through past failed bundled probe to bare ffmpeg");
    assert.deepEqual(probeCalls, [ bundledPath, "ffmpeg" ], "bundled probed first, then bare ffmpeg");
  });

  test("falls through to the system PATH when nothing else matches", async () => {

    // Last resort: try probing "ffmpeg" as a bare command. If the OS PATH has a usable FFmpeg, we get back the literal string "ffmpeg".
    const { context, probeCalls } = makeFFmpegContext({

      bundledPath: undefined,
      platform: "linux",
      probeResults: new Map([[ "ffmpeg", true ]])
    });

    assert.equal(await probeFFmpegPath(context), "ffmpeg");
    assert.ok(probeCalls.includes("ffmpeg"), "the bare 'ffmpeg' probe was tried");
  });

  test("returns undefined when no path matches anywhere", async () => {

    // Pessimistic case: no path exists, the bundled package is missing, and the system PATH does not have ffmpeg.
    const { context } = makeFFmpegContext({

      bundledPath: undefined,
      platform: "linux",
      probeResults: new Map([[ "ffmpeg", false ]])
    });

    assert.equal(await probeFFmpegPath(context), undefined);
  });

  test("does not check macOS paths on Windows or Linux", async () => {

    // The macOS-specific path should not be probed on other platforms - the platform guard is structural.
    const macPath = "/Users/test/Library/Application Support/ChannelsDVR/latest/ffmpeg";
    const { context, probeCalls } = makeFFmpegContext({

      bundledPath: undefined,
      existsSet: new Set([macPath]),
      platform: "linux",
      probeResults: new Map([[ macPath, true ]])
    });

    await probeFFmpegPath(context);
    assert.equal(probeCalls.includes(macPath), false, "macOS path was not probed on Linux");
  });

  test("re-probes on each call (no internal cache - caching lives in resolveFFmpegPath, not here)", async () => {

    /* probeFFmpegPath is the pure algorithm and intentionally has no cache. Production callers wanting memoization use resolveFFmpegPath, whose cache lives in
     * a sealed closure outside this function's scope. Two probe calls against the same context must each result in a fresh probe of the matching path.
     */
    const homePath = "/Users/test/channels-dvr/latest/ffmpeg";
    const { context, probeCalls } = makeFFmpegContext({

      existsSet: new Set([homePath]),
      platform: "linux",
      probeResults: new Map([[ homePath, true ]])
    });

    await probeFFmpegPath(context);
    await probeFFmpegPath(context);

    assert.equal(probeCalls.length, 2, "each call probed independently");
  });
});

describe("classifyFfmpegExit", () => {

  /* The classifier is pure and receives only (code, signal, label) - the caller's shuttingDown gate is applied before this function is ever invoked, so it is
   * not part of what these tests exercise. Each case is synthetic and pins the exact precedence and message shape a regression could silently break.
   */

  test("SIGTERM outranks a non-zero code (kill() during shutdown reports normal)", () => {

    assert.deepEqual(classifyFfmpegExit(1, "SIGTERM", "FFmpeg"), { outcome: "normal" });
  });

  test("code 0 with no signal classifies as normal", () => {

    assert.deepEqual(classifyFfmpegExit(0, null, "FFmpeg"), { outcome: "normal" });
  });

  test("a non-zero code with no signal classifies as error with the exact message", () => {

    assert.deepEqual(classifyFfmpegExit(1, null, "FFmpeg"), { message: "FFmpeg exited with code 1.", outcome: "error" });
  });

  test("a different non-zero code still classifies as error with the code substituted into the message", () => {

    assert.deepEqual(classifyFfmpegExit(255, null, "MPEG-TS remuxer"), { message: "MPEG-TS remuxer exited with code 255.", outcome: "error" });
  });

  test("a non-SIGTERM signal with a null code classifies as error with the exact signal message", () => {

    assert.deepEqual(classifyFfmpegExit(null, "SIGKILL", "FFmpeg"), { message: "FFmpeg killed by signal SIGKILL.", outcome: "error" });
  });

  test("both code and signal null classifies as normal", () => {

    assert.deepEqual(classifyFfmpegExit(null, null, "FFmpeg"), { outcome: "normal" });
  });

  test("a non-zero code with a non-SIGTERM signal present yields the code message, not the signal message", () => {

    // Precedence: the code branch is checked before the signal branch, so a non-zero code paired with a signal must still produce the code-based message.
    assert.deepEqual(classifyFfmpegExit(1, "SIGKILL", "FFmpeg"), { message: "FFmpeg exited with code 1.", outcome: "error" });
  });
});

describe("buildSpawnFFmpegArgs", () => {

  /* The argv builder is pure. We pin the exact flag positions and values that production FFmpeg invocations depend on; if any of these shift, the tests catch it
   * before the spawn happens at runtime. The aac_at vs aac selection is platform-conditional and is documented separately.
   */

  test("includes the canonical FFmpeg input flags at the start of argv", () => {

    const args = buildSpawnFFmpegArgs(256000);

    // The input flags suppress banners and per-frame stats, restrict logging to warnings and errors, tolerate corrupt input frames, and cap input probing at 16KB.
    assert.ok(args.includes("-hide_banner"));
    assert.ok(args.includes("-nostats"));
    assert.deepEqual(args.slice(args.indexOf("-loglevel"), args.indexOf("-loglevel") + 2), [ "-loglevel", "warning" ]);
    assert.deepEqual(args.slice(args.indexOf("-fflags"), args.indexOf("-fflags") + 2), [ "-fflags", "+discardcorrupt" ]);
    assert.deepEqual(args.slice(args.indexOf("-err_detect"), args.indexOf("-err_detect") + 2), [ "-err_detect", "ignore_err" ]);
    assert.deepEqual(args.slice(args.indexOf("-probesize"), args.indexOf("-probesize") + 2), [ "-probesize", "16384" ]);
  });

  test("reads Matroska from stdin (-i pipe:0)", () => {

    const args = buildSpawnFFmpegArgs(256000);
    const inputIndex = args.indexOf("-i");

    assert.equal(args[inputIndex + 1], "pipe:0");
  });

  test("copies the video stream unchanged (-c:v copy)", () => {

    const args = buildSpawnFFmpegArgs(256000);
    const videoIndex = args.indexOf("-c:v");

    assert.equal(args[videoIndex + 1], "copy");
  });

  test("transcodes audio with the supplied bitrate", () => {

    const args = buildSpawnFFmpegArgs(192000);
    const bitrateIndex = args.indexOf("-b:a");

    assert.equal(args[bitrateIndex + 1], "192000");
  });

  test("uses Apple's AudioToolbox AAC encoder when platform is 'darwin'", () => {

    const args = buildSpawnFFmpegArgs(256000, { platform: "darwin" });
    const encoderIndex = args.indexOf("-c:a");

    assert.equal(args[encoderIndex + 1], "aac_at");
  });

  test("uses FFmpeg's software AAC encoder on Linux", () => {

    const args = buildSpawnFFmpegArgs(256000, { platform: "linux" });
    const encoderIndex = args.indexOf("-c:a");

    assert.equal(args[encoderIndex + 1], "aac");
  });

  test("uses FFmpeg's software AAC encoder on Windows (any non-darwin platform)", () => {

    const args = buildSpawnFFmpegArgs(256000, { platform: "win32" });
    const encoderIndex = args.indexOf("-c:a");

    assert.equal(args[encoderIndex + 1], "aac");
  });

  test("defaults the platform to process.platform when no override is supplied", () => {

    // Without an explicit platform option, the function reads process.platform. This lets production callers stay terse while tests pin both branches.
    const args = buildSpawnFFmpegArgs(256000);
    const encoderIndex = args.indexOf("-c:a");
    const expected = process.platform === "darwin" ? "aac_at" : "aac";

    assert.equal(args[encoderIndex + 1], expected, "default platform matches the host's process.platform");
  });

  test("emits fragmented MP4 with the streaming-friendly movflags", () => {

    const args = buildSpawnFFmpegArgs(256000);
    const formatIndex = args.indexOf("-f");

    assert.equal(args[formatIndex + 1], "mp4");

    const movflagsIndex = args.indexOf("-movflags");

    assert.equal(args[movflagsIndex + 1], "frag_keyframe+empty_moov+default_base_moof+skip_sidx+skip_trailer");
  });

  test("flushes packets immediately to minimize segmentation latency", () => {

    const args = buildSpawnFFmpegArgs(256000);
    const flushIndex = args.indexOf("-flush_packets");

    assert.equal(args[flushIndex + 1], "1");
  });

  test("ends with pipe:1 so output goes to stdout", () => {

    const args = buildSpawnFFmpegArgs(256000);

    assert.equal(args.at(-1), "pipe:1");
  });

  test("omits the metadata flag when no comment is supplied", () => {

    const args = buildSpawnFFmpegArgs(256000);

    assert.equal(args.indexOf("-metadata"), -1);
  });

  test("includes a 'PrismCast - <comment>' metadata flag when a comment is supplied", () => {

    const args = buildSpawnFFmpegArgs(256000, { comment: "ABC East" });
    const metaIndex = args.indexOf("-metadata");

    assert.notEqual(metaIndex, -1, "metadata flag is present");
    assert.equal(args[metaIndex + 1], "comment=PrismCast - ABC East");
  });

  test("places the metadata flag before pipe:1 so the metadata applies to output", () => {

    // Order matters: -metadata before pipe:1 attaches the metadata to the output stream. Pinning this here so a future refactor can't reorder them silently.
    const args = buildSpawnFFmpegArgs(256000, { comment: "test" });

    assert.ok(args.indexOf("-metadata") < args.indexOf("pipe:1"));
  });

  test("comment and platform options compose independently", () => {

    // The two options are orthogonal - exercising both at once verifies neither overrides the other's behavior.
    const args = buildSpawnFFmpegArgs(256000, { comment: "channel", platform: "darwin" });

    assert.equal(args[args.indexOf("-c:a") + 1], "aac_at", "platform option still drives encoder choice");
    assert.equal(args[args.indexOf("-metadata") + 1], "comment=PrismCast - channel", "comment option still emits the metadata flag");
  });
});

describe("buildMpegTsRemuxerArgs", () => {

  /* The MPEG-TS remuxer's argv is fully static; we just verify the canonical flag positions and ATSC-conventional output flags that Plex's transcoder relies on.
   */

  test("reads fragmented MP4 from stdin", () => {

    const args = buildMpegTsRemuxerArgs();

    // The -f mp4 flag declares the input format before the -i flag.
    assert.deepEqual(args.slice(args.indexOf("-f"), args.indexOf("-f") + 2), [ "-f", "mp4" ]);

    const inputIndex = args.indexOf("-i");

    assert.equal(args[inputIndex + 1], "pipe:0");
  });

  test("copies both video and audio without transcoding", () => {

    const args = buildMpegTsRemuxerArgs();
    const copyIndex = args.indexOf("-c");

    assert.equal(args[copyIndex + 1], "copy");
  });

  test("emits MPEG-TS with ATSC-conventional service and PID conventions", () => {

    const args = buildMpegTsRemuxerArgs();

    // Output container.
    assert.equal(args[args.lastIndexOf("-f") + 1], "mpegts");

    // ATSC-conventional PID assignments and service type so Plex's transcoder makes the right assumptions.
    assert.equal(args[args.indexOf("-mpegts_pmt_start_pid") + 1], "0x0020");
    assert.equal(args[args.indexOf("-mpegts_start_pid") + 1], "0x0031");
    assert.equal(args[args.indexOf("-mpegts_service_type") + 1], "digital_tv");
    assert.equal(args[args.indexOf("-pat_period") + 1], "0.1");
    assert.equal(args[args.indexOf("-pcr_period") + 1], "40");
  });

  test("ends with pipe:1 so output goes to stdout", () => {

    const args = buildMpegTsRemuxerArgs();

    assert.equal(args.at(-1), "pipe:1");
  });

  test("returns the same argv on each call (no hidden state)", () => {

    // The function is pure; consecutive calls produce structurally equal arrays. Tests pin this to lock the contract for production callers that may cache the
    // result locally.
    assert.deepEqual(buildMpegTsRemuxerArgs(), buildMpegTsRemuxerArgs());
  });
});
