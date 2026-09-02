/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.classification.test.ts: Setup-tier tests for capture-infrastructure classification across both of the establishment's failure phases.
 *
 * The classification lives at the acquisition chokepoint - inside createPageWithCapture's own two catch blocks - so that every caller of the acquisition gets it.
 * A chokepoint with two rethrow points can quietly lose a phase: a helper wired into one catch and not the other, or a rethrow that converts the error and takes
 * its signature with it. So both phases are driven here, each with a failure the pattern list recognises, and each is read through the one client-facing
 * consequence the judgment drives - the 503 that tells Channels DVR to back off rather than the 500 it would retry straight into.
 *
 * Everything runs through the CreatePageWithCaptureDeps collaborators the sibling setup-tier suites use, so no Chrome and no CDP are involved: the acquisition
 * phase fails by rejecting the capture acquisition, and the establishment phase fails by rejecting the navigation that follows a successful acquisition.
 */
import type { Browser, Page } from "puppeteer-core";
import { StreamSetupError, setupStream } from "./setup.ts";
import { after, before, beforeEach, describe, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import type { CaptureMode } from "../types/index.ts";
import type { CaptureStream } from "../browser/tabCapture.ts";
import type { CreatePageWithCaptureDeps } from "./setup.ts";
import { LOG } from "../utils/index.ts";
import type { ProbeCacheIdentity } from "../native/probe.ts";
import { Readable } from "node:stream";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { initializeDataDir } from "../config/paths.ts";
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

// What the acquisition and the navigation do for the current case. A case sets exactly one of them to fail, which is what makes the phase the row names the phase
// the failure actually came from.
let acquisitionFailure: Error | null = null;
let navigationFailure: Error | null = null;

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
  emulateCaptureSurface: async (): Promise<{ height: number; width: number }> => ({ height: 1080, width: 1920 }),
  getCurrentBrowser: async (): Promise<Browser> => ({ newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser),
  installActivationHeal: async (): Promise<void> => { /* The activation heal is not what this path measures. */ },
  openSharedWindowTab: async (): Promise<Page> => makeStubPage(),
  reaffirmCaptureSurface: async (): Promise<void> => { /* A failing establishment never reaches the re-affirmation. */ },
  spawnFFmpeg: (): never => { throw new Error("These rows run in native-fMP4 capture mode, where no FFmpeg child is spawned."); },
  startOverlayHandling: async (): Promise<void> => { /* No overlay poll matters on a failing establishment. */ },
  syncWindowVisibility: async (): Promise<void> => { /* Window presentation is not what this path measures. */ }
};

/**
 * Runs a tune that is expected to fail, and hands back the setup error it produced.
 * @returns The StreamSetupError the tune raised.
 */
async function runFailingTune(): Promise<StreamSetupError> {

  try {

    await setupStream({ probeIdentity: PROBE_IDENTITY, staticCapture: true, streamId: "classification-test", url: STREAM_URL },
      (): void => { /* No circuit break on these paths. */ }, deps);
  } catch(error) {

    assert.ok(error instanceof StreamSetupError, "the tune failed as a setup error");

    return error;
  }

  throw new Error("The tune was expected to fail and did not.");
}

let originalCaptureMode: CaptureMode;
let originalNavigationRetries: number;
let restoreError: () => void;

before(async () => {

  originalCaptureMode = CONFIG.streaming.captureMode;
  originalNavigationRetries = CONFIG.streaming.maxNavigationRetries;

  // Native capture keeps FFmpeg resolution out of the path ahead of navigation, and a single navigation attempt keeps the failure immediate rather than spending
  // the retry ladder's backoff sleeps on a stub that will never succeed.
  CONFIG.streaming.captureMode = "native";
  CONFIG.streaming.maxNavigationRetries = 1;

  // Both rows drive a genuine setup failure, whose error line is expected and not what they measure.
  const original = LOG.error.bind(LOG);

  LOG.error = (): void => { /* The failure line is expected on both paths. */ };
  restoreError = (): void => { LOG.error = original; };

  initializeDataDir(await mkdtemp(path.join(os.tmpdir(), "prismcast-classification-")));
});

after(() => {

  CONFIG.streaming.captureMode = originalCaptureMode;
  CONFIG.streaming.maxNavigationRetries = originalNavigationRetries;
  restoreError();
});

beforeEach(() => {

  acquisitionFailure = null;
  navigationFailure = null;
});

describe("setupStream - capture-infrastructure classification across both establishment phases", () => {

  test("an acquisition-phase capture failure is classified and reaches the client as a back-off", async () => {

    // The phase the classification has always covered: Chrome refusing the capture start itself. The 503 is what Channels DVR reads as "wait and retry" rather
    // than as a broken channel it should keep hammering.
    acquisitionFailure = new Error("Cannot capture a tab with an active stream.");

    const error = await runFailingTune();

    assert.equal(error.statusCode, 503, "a capture-infrastructure failure backs the client off");
  });

  test("an establishment-phase capture failure is classified too, and reaches the client the same way", async () => {

    /* The phase that is easiest to lose. The playback-initialization safety net and the capability probe both surface here, past acquisition, and the pattern
     * list names both - but this failure travels through two rethrows before any caller sees it. The row goes red if the establishment catch drops its
     * classification, or if a rethrow converts the error into something the pattern list does not recognise.
     */
    navigationFailure = new Error("Playback initialization timed out.");

    const error = await runFailingTune();

    assert.equal(error.statusCode, 503, "an establishment-phase capture-infrastructure failure backs the client off the same way");
  });

  test("a site failure that is not capture infrastructure still reaches the client as a plain error", async () => {

    // The control that keeps the two rows above from being satisfied by a path that answers 503 to everything. A site that simply will not load is the channel's
    // problem, not the capture system's, and the client should see it as such.
    navigationFailure = new Error("The site returned an unexpected page.");

    const error = await runFailingTune();

    assert.equal(error.statusCode, 500, "a site-specific failure is not a capture-infrastructure back-off");
  });
});
