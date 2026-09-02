/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.ffmpegWiring.test.ts: Tests for the FFmpeg error wiring createPageWithCapture attaches to a capture pipeline's streams.
 *
 * Two listeners live outside the spawn wrapper - the one on the child's stdout and the one on the capture-to-stdin pipeline - and they are the two places a
 * post-teardown event can still reach the caller's callback. That matters because a tab replacement disposes the outgoing pipeline in the same frame it installs
 * the incoming one, so a stray event from the killed child lands against a registry that already holds a healthy new pipeline, where breaking the circuit would
 * terminate the stream the replacement just saved.
 *
 * Both directions are asserted for both listeners, because the two ways to get this wrong are opposites: a gate that silences nothing leaves the hazard open, and
 * a gate that silences everything hides genuine faults on a live pipeline. createPageWithCapture composes on its injected collaborators, so these rows drive the
 * real wiring with a stub browser, a PassThrough capture stream, and an FFmpeg double whose teardown state and stream events the test drives.
 */
import type { Browser, CDPSession, Page } from "puppeteer-core";
import { after, before, beforeEach, describe, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import type { CaptureStream } from "../browser/tabCapture.ts";
import type { ChildProcess } from "node:child_process";
import type { CreatePageWithCaptureDeps } from "./setup.ts";
import type { FFmpegProcess } from "../utils/index.ts";
import { PassThrough } from "node:stream";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { createPageWithCapture } from "./setup.ts";
import { setTimeout as delay } from "node:timers/promises";
import { makeProfile } from "../config/profiles.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/**
 * The FFmpeg child double. It answers the teardown-requested read exactly as the real wrapper does - set unconditionally by kill() - and exposes its streams so a
 * row can raise the events a dying child raises.
 */
interface FakeFFmpeg extends FFmpegProcess {

  // How many times kill() has been called, so a row can read whether the live branch tore the pipeline down before escalating.
  readonly kills: () => number;
}

/**
 * Builds the FFmpeg double.
 * @returns The double, with real PassThrough streams so the production pipeline wiring runs unchanged.
 */
function makeFakeFFmpeg(): FakeFFmpeg {

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

// The FFmpeg double the current row's establishment is handed, and the capture stream feeding it.
let ffmpeg: FakeFFmpeg;
let captureStream: PassThrough;

// Every error the establishment's caller-facing callback received, in order.
let faults: Error[];

// The capture mode the suite found, restored on the way out so the shared CONFIG is left as it was.
let originalCaptureMode: string;

const deps: CreatePageWithCaptureDeps = {

  acquireCaptureStream: async (): Promise<CaptureStream> => {

    captureStream = new PassThrough();

    return captureStream as unknown as CaptureStream;
  },
  emulateCaptureSurface: async (): Promise<{ height: number; width: number }> => ({ height: 1080, width: 1920 }),
  getCurrentBrowser: async (): Promise<Browser> => ({ connected: false } as unknown as Browser),
  installActivationHeal: async (): Promise<void> => { /* Nothing to enrol on a stub page. */ },
  openSharedWindowTab: async (): Promise<Page> => makeStubPage(),
  reaffirmCaptureSurface: async (): Promise<void> => { /* No compositor to re-affirm against. */ },
  spawnFFmpeg: (): FFmpegProcess => ffmpeg,
  startOverlayHandling: async (): Promise<void> => { /* No overlays on a stub page. */ },
  syncWindowVisibility: async (): Promise<void> => { /* No window to settle. */ }
};

/* A minimal Page for the static-capture pipeline: goto and evaluateOnNewDocument are all it is asked for, and the disconnected browser resolves the overlay
 * poll's tick taxonomy to a clean stop rather than leaving a fire-and-forget promise pending past the row.
 */
function makeStubPage(): Page {

  return {

    browser: (): Browser => ({ connected: false } as unknown as Browser),
    createCDPSession: async (): Promise<CDPSession> => ({ send: async (): Promise<unknown> => ({}) } as unknown as CDPSession),
    evaluate: async (): Promise<never> => { throw new Error("The stub page has no live DOM to evaluate against."); },
    evaluateOnNewDocument: async (): Promise<void> => { /* The injected video-selector helper needs no real document on a stub. */ },
    goto: async (): Promise<void> => { /* The static branch navigates once and takes the page as-is. */ },
    isClosed: (): boolean => false,
    setBypassCSP: async (): Promise<void> => { /* Nothing to bypass on a stub. */ }
  } as unknown as Page;
}

/**
 * Establishes a capture through the real createPageWithCapture on the static branch, which is the shortest path that still runs the whole FFmpeg wiring.
 * @returns The established capture session, for the row to dispose.
 */
async function establish(): Promise<{ dispose: () => void }> {

  const result = await createPageWithCapture({

    onFFmpegError: (error: Error): void => { faults.push(error); },
    profile: makeProfile({ staticCapture: true }),
    skipManifestInterception: true,
    streamId: "ffmpeg-wiring-test",
    url: "https://static.example/page"
  }, deps);

  return { dispose: (): void => result.captureSession.dispose() };
}

before(() => {

  originalCaptureMode = CONFIG.streaming.captureMode;

  // FFmpeg mode is what attaches the two listeners under test.
  CONFIG.streaming.captureMode = "ffmpeg";
});

after(() => {

  CONFIG.streaming.captureMode = originalCaptureMode as typeof CONFIG.streaming.captureMode;
});

beforeEach(() => {

  faults = [];
  ffmpeg = makeFakeFFmpeg();
});

describe("createPageWithCapture: a disposed pipeline never fires its error callback", () => {

  test("a stdout error on a killed pipeline is not reported to the caller", async () => {

    // The silenced direction. The message is deliberately not one of the strings teardown is known to produce, because naming those strings is exactly the
    // approach that cannot cover a stray event nobody predicted.
    const capture = await establish();

    ffmpeg.kill();
    ffmpeg.stdout.emit("error", new Error("read ECONNRESET on a pipe nobody is reading any more"));

    assert.deepEqual(faults, [], "a pipeline that was told to tear down reports nothing");

    capture.dispose();
  });

  test("a stdout error on a live pipeline is still reported exactly once", async () => {

    // The not-over-silenced direction. A gate that suppressed unconditionally would leave a genuinely dying capture invisible to the recovery ladder, so this row
    // is what keeps the fix from being a blanket mute.
    const capture = await establish();

    ffmpeg.stdout.emit("error", new Error("read ECONNRESET on a pipe nobody is reading any more"));

    assert.equal(faults.length, 1, "a live pipeline's fault reaches the caller");
    assert.equal(ffmpeg.kills(), 1, "and the pipeline is torn down on the way");

    capture.dispose();
  });

  test("a pipeline error on a killed capture is not reported to the caller", async () => {

    // The second listener, the capture-to-stdin pipeline, on the same two polarities. Its string filters cover the errors a normal teardown produces; this error
    // is not one of them, which is what leaves the teardown read as the thing standing between it and the callback.
    const capture = await establish();

    ffmpeg.kill();
    captureStream.destroy(new Error("the capture source failed in an unanticipated way"));

    await delay(10);

    assert.deepEqual(faults, [], "a pipeline whose FFmpeg was told to tear down reports nothing");

    capture.dispose();
  });

  test("a pipeline error on a live capture is still reported exactly once", async () => {

    const capture = await establish();

    captureStream.destroy(new Error("the capture source failed in an unanticipated way"));

    await delay(10);

    assert.equal(faults.length, 1, "a live capture's pipeline fault reaches the caller");

    capture.dispose();
  });
});
