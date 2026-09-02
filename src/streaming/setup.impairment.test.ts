/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.impairment.test.ts: Setup-tier tests for what a tune does when the browser can no longer start captures. Two things are pinned here. The establishment
 * declares the purpose it needs the browser for, so the refusal happens at acquisition rather than after a page has been opened and a capture attempted. And the
 * refusal reaches the client as its own 503: quiet, because the alarm already fired once when the mark was recorded, and carrying a message that says the wait ends
 * with the browser's own streams rather than with a cooldown - which is what distinguishes it from the governor's back-off, pinned here beside it.
 *
 * Everything runs through the CreatePageWithCaptureDeps collaborators the sibling setup.directUrlFallback.test.ts uses: the injected browser accessor rejects, so
 * no Chrome, no CDP, and no page are involved at all.
 */
import type { Browser, Page } from "puppeteer-core";
import { BrowserCaptureImpairedError, BrowserUnavailableError } from "../browser/index.ts";
import { after, before, beforeEach, describe, test } from "node:test";
import type { BrowserPurpose } from "../browser/index.ts";
import type { CaptureStream } from "../browser/tabCapture.ts";
import type { CreatePageWithCaptureDeps } from "./setup.ts";
import { LOG } from "../utils/index.ts";
import type { ProbeCacheIdentity } from "../native/probe.ts";
import { Readable } from "node:stream";
import { StreamSetupError } from "./setup.ts";
import type { TestContext } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { initializeDataDir } from "../config/paths.ts";
import { mkdtemp } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setupStream } from "./setup.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// A URL whose domain the profile layer already maps, so profile resolution settles locally instead of following redirects to discover a destination domain.
const STREAM_URL = "https://play.hbomax.com/channels";

// The probe-cache identity every case streams under. A stamp no classification was ever stored against means the cache lookup misses and nothing about encryption
// influences the path under test.
const PROBE_IDENTITY: ProbeCacheIdentity = { key: "capture-impairment-case", stamp: "capture-impairment-stamp" };

// The impairment the refusing accessor reports, matching the shape a probe's verdict records.
const IMPAIRMENT = { reason: "Could not start video source", since: 0 };

// Every purpose the establishment asked the browser accessor for, in order. The list is what makes the declared purpose an observed fact rather than an assumption:
// a double that ignored its argument would leave this empty and prove nothing.
let purposes: BrowserPurpose[] = [];

/**
 * Builds the injected collaborators for one case, with a browser accessor that records the purpose it was asked for and then rejects with the case's error. The
 * remaining members are the ones createPageWithCapture would reach if the acquisition succeeded; none of them runs, because it does not.
 * @param failure - The rejection the browser accessor raises.
 * @returns The injected collaborators.
 */
function makeDeps(failure: Error): CreatePageWithCaptureDeps {

  return {

    acquireCaptureStream: async (): Promise<CaptureStream> => Object.assign(new Readable({ read: (): void => { /* Nothing is read from the stub capture. */ } }),
      { stop: async (): Promise<void> => undefined, stopped: Promise.resolve() }),
    emulateCaptureSurface: async (): Promise<{ height: number; width: number }> => ({ height: 1080, width: 1920 }),
    getCurrentBrowser: async (purpose: BrowserPurpose): Promise<Browser> => {

      purposes.push(purpose);

      throw failure;
    },
    installActivationHeal: async (): Promise<void> => { /* No page is ever created to heal. */ },

    openSharedWindowTab: async (): Promise<Page> => {

      throw new Error("No page is opened at all when the browser accessor refuses.");
    },
    reaffirmCaptureSurface: async (): Promise<void> => { /* No surface is ever acquired to re-affirm. */ },
    startOverlayHandling: async (): Promise<void> => { /* No page is ever created to poll. */ },
    syncWindowVisibility: async (): Promise<void> => { /* Window presentation is not what this path measures. */ }
  };
}

/**
 * Captures LOG.error calls for the life of a test, returning the counter the assertions read. The refusal must be quiet: the alarm fired once when the mark was
 * recorded, so a per-request error on every retry would be the noise this branch exists to avoid.
 * @param t - The test context whose mock registry restores the method at test end.
 * @returns An object whose count field holds how many error lines were logged.
 */
function captureErrors(t: TestContext): { count: number } {

  const captured = { count: 0 };

  t.mock.method(LOG, "error", (): void => { captured.count++; });

  return captured;
}

before(async () => {

  // Point the data directory at a temp directory so nothing this suite touches writes into a real one.
  initializeDataDir(await mkdtemp(path.join(os.tmpdir(), "prismcast-impairment-")));
});

after(() => {

  purposes = [];
});

beforeEach(() => {

  purposes = [];
});

describe("setupStream - a browser that can no longer start captures", () => {

  test("acquires the browser for a capture and refuses the tune with the impairment message", async (t) => {

    /* The whole refusal path in one case: the establishment names the capture purpose, the supervisor's rejection travels out of createPageWithCapture untouched,
     * and setupStream's catch turns it into the 503 the client backs off on. The message is asserted as a literal rather than against a shared constant, because
     * what is being pinned is exactly the text a client reads.
     */
    const errors = captureErrors(t);

    await assert.rejects(setupStream({ probeIdentity: PROBE_IDENTITY, url: STREAM_URL }, (): void => { /* No circuit break here. */ },
      makeDeps(new BrowserCaptureImpairedError(IMPAIRMENT))), (error: unknown) => (error instanceof StreamSetupError) && (error.statusCode === 503) &&
      (error.userMessage === "The browser can no longer start captures and will relaunch once its current streams end. Please retry shortly."),
    "the refusal reaches the caller as a 503 carrying the impairment message");

    assert.deepEqual(purposes, ["capture"], "the establishment declared the capture purpose, so the refusal landed before a page existed");
    assert.equal(errors.count, 0, "and the refusal logged no error, because the alarm already fired once at the mark");
  });

  test("keeps the recovering-capture-system message for a browser the relaunch governor is cooling", async (t) => {

    // The sibling branch, pinned beside it so the pair is proven distinct. A cooling governor ends on a clock, so its message says to retry shortly and nothing
    // about streams; collapsing the pair into one message would fail here.
    const errors = captureErrors(t);

    await assert.rejects(setupStream({ probeIdentity: PROBE_IDENTITY, url: STREAM_URL }, (): void => { /* No circuit break here. */ },
      makeDeps(new BrowserUnavailableError(0))), (error: unknown) => (error instanceof StreamSetupError) && (error.statusCode === 503) &&
      (error.userMessage === "The capture system is recovering. Please retry shortly."),
    "the governor's back-off keeps its own message");

    assert.deepEqual(purposes, ["capture"], "the establishment declared the capture purpose on this path too");
    assert.equal(errors.count, 0, "and it is quiet for the same reason");
  });
});
