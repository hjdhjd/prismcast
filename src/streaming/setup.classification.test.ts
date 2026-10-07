/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.classification.test.ts: Setup-tier tests for how a capture-infrastructure failure from each of the establishment's failure phases reaches the client.
 *
 * A failure from any phase leaves createPageWithCapture through its catch blocks, and each rethrow point can quietly lose a phase: a rethrow that converts the
 * error takes its signature with it. So every phase is driven here, each with a failure the pattern list recognises, and each is read through setupStream's
 * status mapping - the 503 that tells Channels DVR to back off rather than the 500 it would retry straight into. What is asserted here is that the failure
 * survives the rethrows with its signature intact and reaches that mapping as a 503. The catches' hand-off to the capture verdict answers with none, so no row
 * waits on a probe, and it records each call, so the establishment-phase and site-failure rows assert that the failure was handed off exactly once.
 *
 * A browser whose capture system failed a launch gate reaches the same mapping by its type rather than by its wording, so its row's message carries no signature
 * the pattern list recognises and the 503 can come only from the type.
 *
 * Everything runs through the CreatePageWithCaptureDeps collaborators the sibling setup-tier suites use, so no Chrome and no CDP are involved: the launch gate
 * fails by rejecting the browser acquisition, the acquisition phase by rejecting the capture acquisition, and the establishment phase by rejecting the
 * navigation that follows a successful acquisition.
 */
import type { Browser, Page } from "puppeteer-core";
import { StreamSetupError, setupStream } from "./setup.ts";
import { after, before, beforeEach, describe, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import { CaptureLaunchError } from "../browser/index.ts";
import type { CaptureStream } from "../browser/tabCapture.ts";
import type { CreatePageWithCaptureDeps } from "./setup.ts";
import type { FFmpegProcess } from "../utils/index.ts";
import { LOG } from "../utils/index.ts";
import type { ProbeCacheIdentity } from "../native/probe.ts";
import { Readable } from "node:stream";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { initializeDataDir } from "../config/paths.ts";
import { makeFakeFFmpeg } from "../utils/ffmpeg.helpers.ts";
import { makeStreamSettings } from "../config/streamSettings.helpers.ts";
import { mkdtemp } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// A URL whose domain the profile layer already maps, so profile resolution settles locally instead of following redirects to discover a destination domain.
const STREAM_URL = "https://play.hbomax.com/channels";

// The probe-cache identity every case streams under. A stamp no classification was ever stored against means the cache lookup misses and nothing about encryption
// influences the path under test.
const PROBE_IDENTITY: ProbeCacheIdentity = { key: "classification-case", stamp: "classification-stamp" };

// The stream's start instant, which setup takes from the pending entry. A fixed instant, because no row here completes a tune whose monitor would report it.
const STREAM_START_TIME = 1700000000000;

// What the browser acquisition, the capture acquisition and the navigation do for the current case. A case sets exactly one of them to fail, which is what makes
// the phase the row names the phase the failure actually came from.
let browserFailure: Error | null = null;
let acquisitionFailure: Error | null = null;
let navigationFailure: Error | null = null;

// How many times the case's failure was handed to the capture verdict.
let verdictCalls = 0;

/**
 * Builds a stub page whose navigation raises the case's establishment failure, and which answers the handful of other members the failing path touches.
 * @returns A stub page.
 */
function makeStubPage(): Page {

  return {

    close: async (): Promise<void> => { /* Nothing to close on a stub. */ },
    evaluate: async (): Promise<unknown> => undefined,
    evaluateOnNewDocument: async (): Promise<void> => { /* The injected video-selector helper needs no real document on a stub. */ },
    goto: async (): Promise<void> => {

      if(navigationFailure) {

        throw navigationFailure;
      }
    },
    isClosed: (): boolean => false,
    setBypassCSP: async (): Promise<void> => { /* Nothing to bypass on a stub. */ },
    url: (): string => STREAM_URL
  } as unknown as Page;
}

const deps: CreatePageWithCaptureDeps = {

  acquireCaptureStream: async (): Promise<CaptureStream> => {

    if(acquisitionFailure) {

      throw acquisitionFailure;
    }

    return Object.assign(new Readable({ read: (): void => { /* Nothing is read from the stub capture. */ } }),
      { stop: async (): Promise<void> => undefined, stopped: Promise.resolve() });
  },

  // What these rows read is the status code each phase's failure produces and whether it was handed off, so the verdict counts the call and answers with none,
  // and no case waits on a probe.
  awaitCaptureVerdict: async (): Promise<null> => {

    verdictCalls++;

    return null;
  },
  emulateCaptureSurface: async (): Promise<{ height: number; width: number }> => ({ height: 1080, width: 1920 }),
  getCurrentBrowser: async (): Promise<Browser> => {

    if(browserFailure) {

      throw browserFailure;
    }

    return { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;
  },
  installActivationHeal: async (): Promise<void> => { /* The activation heal is not what this path measures. */ },
  openSharedWindowTab: async (): Promise<Page> => makeStubPage(),
  reaffirmCaptureSurface: async (): Promise<void> => { /* A failing establishment never reaches the re-affirmation. */ },
  resolveFFmpegPath: async (): Promise<string> => "ffmpeg",
  spawnFFmpeg: (): FFmpegProcess => makeFakeFFmpeg(),
  startOverlayHandling: async (): Promise<void> => { /* No overlay poll matters on a failing establishment. */ },
  syncWindowVisibility: async (): Promise<void> => { /* Window presentation is not what this path measures. */ }
};

/**
 * Runs a tune that is expected to fail, and hands back the setup error it produced.
 * @returns The StreamSetupError the tune raised.
 */
async function runFailingTune(): Promise<StreamSetupError> {

  try {

    await setupStream({ numericStreamId: 9401, probeIdentity: PROBE_IDENTITY, settings: makeStreamSettings(), startTime: STREAM_START_TIME, staticCapture: true,
      streamId: "classification-test", url: STREAM_URL }, (): void => { /* No circuit break on these paths. */ }, deps);
  } catch(error) {

    assert.ok(error instanceof StreamSetupError, "the tune failed as a setup error");

    return error;
  }

  throw new Error("The tune was expected to fail and did not.");
}

let originalNavigationRetries: number;
let restoreError: () => void;

before(async () => {

  originalNavigationRetries = CONFIG.streaming.maxNavigationRetries;

  // A single navigation attempt keeps the failure immediate rather than spending the retry ladder's backoff sleeps on a stub that will never succeed.
  CONFIG.streaming.maxNavigationRetries = 1;

  // Every row drives a genuine setup failure, whose error line is expected and not what they measure.
  const original = LOG.error.bind(LOG);

  LOG.error = (): void => { /* The failure line is expected on every path. */ };
  restoreError = (): void => { LOG.error = original; };

  initializeDataDir(await mkdtemp(path.join(os.tmpdir(), "prismcast-classification-")));
});

after(() => {

  CONFIG.streaming.maxNavigationRetries = originalNavigationRetries;
  restoreError();
});

beforeEach(() => {

  browserFailure = null;
  acquisitionFailure = null;
  navigationFailure = null;
  verdictCalls = 0;
});

describe("setupStream - capture-infrastructure classification across each establishment phase", () => {

  test("an acquisition-phase capture failure is classified and reaches the client as a back-off", async () => {

    // The acquisition phase: Chrome refusing the capture start itself. The 503 is what Channels DVR reads as "wait and retry" rather
    // than as a broken channel it should keep hammering.
    acquisitionFailure = new Error("Cannot capture a tab with an active stream.");

    const error = await runFailingTune();

    assert.equal(error.statusCode, 503, "a capture-infrastructure failure backs the client off");
  });

  test("an establishment-phase capture failure is classified too, and reaches the client the same way", async () => {

    /* The phase that is easiest to lose. The playback-initialization safety net and the capability probe both surface here, past acquisition, and the pattern
     * list names both - but this failure travels through each rethrow point before any caller sees it. The row goes red if a rethrow converts the error into
     * something the pattern list does not recognise, so the failure no longer reaches setupStream's status mapping as a 503.
     */
    navigationFailure = new Error("Playback initialization timed out.");

    const error = await runFailingTune();

    assert.equal(error.statusCode, 503, "an establishment-phase capture-infrastructure failure backs the client off the same way");
    assert.equal(verdictCalls, 1, "and the establishment catch handed the failure to the capture verdict once");
  });

  test("a site failure that is not capture infrastructure still reaches the client as a plain error", async () => {

    // The control that keeps the rows above from being satisfied by a path that answers 503 to everything. A site that simply will not load is the channel's
    // problem, not the capture system's, and the client should see it as such. The hand-off is unconditional, because the verdict does its own filtering.
    navigationFailure = new Error("The site returned an unexpected page.");

    const error = await runFailingTune();

    assert.equal(error.statusCode, 500, "a site-specific failure is not a capture-infrastructure back-off");
    assert.equal(verdictCalls, 1, "and the establishment catch still handed it to the capture verdict once");
  });

  test("a launch-gate failure reaches the client as a back-off by its type, with its cause kept", async () => {

    /* The browser launched but its capture probe found no working capture, so the acquisition of a browser for this tune rejects with the gate's error. Its
     * message names a probe detail no pattern recognises, so only the type can earn the 503, and the setup error carries the gate's error as its cause.
     */
    const launchError = new CaptureLaunchError("Capture system verification failed after 3 attempts: The page reported no video dimensions.");

    browserFailure = launchError;

    const error = await runFailingTune();

    assert.equal(error.statusCode, 503, "a launch-gate failure backs the client off");
    assert.equal(error.cause, launchError, "and the setup error keeps the gate's error as its cause");
  });
});
