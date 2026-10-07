/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.ffmpegWiring.test.ts: Tests for the FFmpeg error wiring createPageWithCapture attaches to a capture pipeline's streams.
 *
 * The listeners that live outside the spawn wrapper - the one on the child's stdout and the one on the capture-to-stdin pipeline - are where a post-teardown event can
 * still reach the caller's callback. That matters because a tab replacement disposes the outgoing pipeline in the same frame it installs the incoming one, so a stray
 * event from the killed child lands against a registry that already holds a healthy new pipeline, where breaking the circuit would terminate the stream the replacement
 * just saved.
 *
 * Both directions are asserted for each listener, because the two ways to get this wrong are opposites: a gate that silences nothing leaves the hazard open, and a gate
 * that silences everything hides genuine faults on a live pipeline. createPageWithCapture composes on its injected collaborators, so these rows drive the real wiring
 * with a stub browser, a PassThrough capture stream, and an FFmpeg double whose teardown state and stream events the test drives.
 *
 * The spawn stub records the binary and the audio rate each spawn is handed, which is how a row reads that the FFmpeg encoder runs at the stream's own audio rate,
 * and that the binary is the one the injected resolver answers: its path when it finds one, the bare name the PATH lookup takes when it finds none, and no spawn
 * and no capture at all when the resolution rejects.
 */
import type { Browser, Page } from "puppeteer-core";
import { beforeEach, describe, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import type { CaptureStream } from "../browser/tabCapture.ts";
import type { CreatePageWithCaptureDeps } from "./setup.ts";
import type { FFmpegProcess } from "../utils/index.ts";
import type { FakeFFmpeg } from "../utils/ffmpeg.helpers.ts";
import { PassThrough } from "node:stream";
import type { StreamSettings } from "../config/streamSettings.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { createPageWithCapture } from "./setup.ts";
import { setTimeout as delay } from "node:timers/promises";
import { makeFakeFFmpeg } from "../utils/ffmpeg.helpers.ts";
import { makeProfile } from "../config/profiles.helpers.ts";
import { makeStreamSettings } from "../config/streamSettings.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The binary the resolver each row starts from answers, a path no host carries, so a spawn handed it can only have taken it from the injected resolver.
const RESOLVED_BINARY = "/test/resolved/ffmpeg";

// The FFmpeg double the current row's establishment is handed, and the capture stream feeding it.
let ffmpeg: FakeFFmpeg;
let captureStream: PassThrough;

// How many capture acquisitions the current row's establishment made.
let acquisitions: number;

// Every error the establishment's caller-facing callback received, in order.
let faults: Error[];

// The current row's FFmpeg resolution, which the injected resolver delegates to.
let resolveBinary: () => Promise<string | undefined>;

// The binary and the audio rate each FFmpeg spawn was handed, in order.
let spawnedAudioRates: number[];
let spawnedBinaries: string[];

const deps: CreatePageWithCaptureDeps = {

  acquireCaptureStream: async (): Promise<CaptureStream> => {

    acquisitions++;
    captureStream = new PassThrough();

    return captureStream as unknown as CaptureStream;
  },

  // The acquisition succeeds on every row here, so no verdict is ever asked for; the stub answers with none.
  awaitCaptureVerdict: async (): Promise<null> => null,
  emulateCaptureSurface: async (): Promise<{ height: number; width: number }> => ({ height: 1080, width: 1920 }),
  getCurrentBrowser: async (): Promise<Browser> => ({ connected: false } as unknown as Browser),
  installActivationHeal: async (): Promise<void> => { /* Nothing to enrol on a stub page. */ },
  openSharedWindowTab: async (): Promise<Page> => makeStubPage(),
  reaffirmCaptureSurface: async (): Promise<void> => { /* No compositor to re-affirm against. */ },
  resolveFFmpegPath: async (): Promise<string | undefined> => resolveBinary(),
  spawnFFmpeg: (ffmpegBin: string, audioBitsPerSecond: number): FFmpegProcess => {

    spawnedBinaries.push(ffmpegBin);
    spawnedAudioRates.push(audioBitsPerSecond);

    return ffmpeg;
  },
  startOverlayHandling: async (): Promise<void> => { /* No overlays on a stub page. */ },
  syncWindowVisibility: async (): Promise<void> => { /* No window to settle. */ }
};

/* A minimal Page for the static-capture pipeline. It answers the members that pipeline touches (setBypassCSP, the injected video selector's
 * evaluateOnNewDocument, the capture lock's isClosed check, and goto), plus close, which is what an establishment's unwind asks of it when the establishment
 * rejects.
 */
function makeStubPage(): Page {

  return {

    close: async (): Promise<void> => { /* Nothing to close on a stub. */ },
    evaluateOnNewDocument: async (): Promise<void> => { /* The injected video-selector helper needs no real document on a stub. */ },
    goto: async (): Promise<void> => { /* The static branch navigates once and takes the page as-is. */ },
    isClosed: (): boolean => false,
    setBypassCSP: async (): Promise<void> => { /* Nothing to bypass on a stub. */ }
  } as unknown as Page;
}

/**
 * Establishes a capture through the real createPageWithCapture on the static branch, which is the shortest path that still runs the whole FFmpeg wiring.
 * @param settings - The stream's settings the establishment runs at.
 * @returns The established capture session, for the row to dispose.
 */
async function establish(settings: StreamSettings = makeStreamSettings()): Promise<{ dispose: () => void }> {

  const result = await createPageWithCapture({

    onFFmpegError: (error: Error): void => { faults.push(error); },
    profile: makeProfile({ staticCapture: true }),
    settings,
    skipManifestInterception: true,
    streamId: "ffmpeg-wiring-test",
    url: "https://static.example/page"
  }, deps);

  return { dispose: (): void => result.captureSession.dispose() };
}

beforeEach(() => {

  acquisitions = 0;
  faults = [];
  ffmpeg = makeFakeFFmpeg();
  resolveBinary = async (): Promise<string> => RESOLVED_BINARY;
  spawnedAudioRates = [];
  spawnedBinaries = [];
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
    // is what keeps the gate from being a blanket mute.
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

describe("createPageWithCapture: the FFmpeg encoder runs at the stream's audio rate", () => {

  test("the spawn receives the stream's audio rate, not its video rate or the running configuration's", async () => {

    /* The stream's audio and video rates are distinct from each other and from the running configuration's, which is assigned other values as the negative
     * control, so a spawn handed the video rate or a configuration read receives a rate the stream never started with.
     */
    const settings = makeStreamSettings({ audioBitsPerSecond: 96000, videoBitsPerSecond: 3000000 });
    const configuredAudioRate = CONFIG.streaming.audioBitsPerSecond;
    const configuredVideoRate = CONFIG.streaming.videoBitsPerSecond;

    CONFIG.streaming.audioBitsPerSecond = 192000;
    CONFIG.streaming.videoBitsPerSecond = 6000000;

    try {

      const capture = await establish(settings);

      assert.deepEqual(spawnedAudioRates, [settings.audioBitsPerSecond], "the one spawn ran at the stream's audio rate");

      capture.dispose();
    } finally {

      CONFIG.streaming.audioBitsPerSecond = configuredAudioRate;
      CONFIG.streaming.videoBitsPerSecond = configuredVideoRate;
    }
  });
});

describe("createPageWithCapture: the FFmpeg binary comes from the injected resolver", () => {

  test("the spawn receives the binary the injected resolver returns", async () => {

    // The resolver each row starts from answers a path no host carries, so a spawn handed anything else took its binary from somewhere other than the collaborator.
    const capture = await establish();

    assert.deepEqual(spawnedBinaries, [RESOLVED_BINARY], "the one spawn ran the binary the resolver answered");

    capture.dispose();
  });

  test("a resolver that finds no FFmpeg leaves the spawn the PATH lookup", async () => {

    // The resolver answers undefined when no candidate runs, and the establishment hands the spawn the bare name so spawn() defers to the PATH lookup.
    resolveBinary = async (): Promise<undefined> => undefined;

    const capture = await establish();

    assert.deepEqual(spawnedBinaries, ["ffmpeg"], "the one spawn ran the bare name the PATH lookup resolves");

    capture.dispose();
  });

  test("a resolver rejection rejects the establishment with that error before any capture is acquired", async () => {

    // The resolution runs ahead of the acquisition, so a rejection finds no capture stream to strand and the capture session stays the stream's sole owner from
    // the instant the stream exists.
    const failure = new Error("The FFmpeg resolution failed.");

    resolveBinary = async (): Promise<never> => { throw failure; };

    await assert.rejects(establish(), (error: unknown) => error === failure, "the establishment rejects with the resolver's own error");

    assert.equal(acquisitions, 0, "no capture was acquired");
    assert.deepEqual(spawnedBinaries, [], "no FFmpeg was spawned");
  });
});
