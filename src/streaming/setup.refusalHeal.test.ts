/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.refusalHeal.test.ts: Setup-tier tests for what a tune does when Chrome refuses the capture start. The refusal is browser-wide and a fresh Chrome accepts
 * at once, so the establishment releases the failed attempt's page, waits for the mid-life probe's verdict, and re-enters itself once through
 * getCurrentBrowser("capture") - whose own outcomes are the whole retry policy. What is asserted here is that policy as observable acts: how many capture starts
 * were made, which browser the second one ran against, and the order of the release against the wait.
 *
 * The ordering row is the one that cannot be read any other way. The relaunch a failed verdict triggers waits for every in-flight page to be gone, so a tune still
 * holding its page would be waiting on a relaunch its own page forbids; the release therefore has to precede the wait, and the timeline below is where that is
 * read. The page double records its own close, which is the second half of the disposer whose first half - the managed-page unregister that empties the in-flight
 * set - runs synchronously immediately before it.
 *
 * Everything runs through the CreatePageWithCaptureDeps collaborators the sibling setup-tier suites use, so no Chrome, no CDP, and no capture extension are
 * involved: the acquisition is scripted per attempt, the verdict is scripted per case, and the browser accessor hands back marker objects a row can tell apart.
 * This file is separate from setup.captureLock.test.ts because that file's header scopes it to the lock's closed-page recursion, while these rows are about the
 * refusal recovery that runs outside the lock.
 */
import type { Browser, CDPSession, Page } from "puppeteer-core";
import type { CaptureProbeOutcome, CreatePageWithCaptureDeps } from "./setup.ts";
import { beforeEach, describe, test } from "node:test";
import { BrowserCaptureImpairedError } from "../browser/index.ts";
import { CAPTURE_SOURCE_UNAVAILABLE_MESSAGE } from "../types/index.ts";
import type { CaptureStream } from "../browser/tabCapture.ts";
import type { FFmpegProcess } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import { PassThrough } from "node:stream";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { createPageWithCapture } from "./setup.ts";
import { makeFakeFFmpeg } from "../utils/ffmpeg.helpers.ts";
import { makeProfile } from "../config/profiles.helpers.ts";
import { makeStreamSettings } from "../config/streamSettings.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The two browser instances a row can tell apart: the one the refused attempt ran against, and the one a relaunch would publish in its place.
const REFUSING_BROWSER = { connected: true } as unknown as Browser;
const FRESH_BROWSER = { connected: true } as unknown as Browser;

// The observable acts of an establishment, in the order they happened. Only the two the ordering row compares are recorded, because a longer log would invite
// assertions about steps these rows do not measure.
let timeline: string[] = [];

// What the acquisition does on each successive capture start, indexed by attempt. An entry past the end of the script means the start succeeds.
let acquisitionFailures: (Error | undefined)[] = [];

// What the browser accessor does on each successive call: the failure it raises, or the instance it hands back.
let browserFailures: (Error | undefined)[] = [];
let browsers: Browser[] = [];

// What the verdict answers for the current case. Scripted per row rather than per call, because every row here asks for at most one verdict it acts on.
let verdictAnswer: () => Promise<Nullable<CaptureProbeOutcome>> = async (): Promise<null> => null;

// The counts the rows read: capture starts attempted, browser acquisitions made, verdicts asked for, and the instance each opened page was opened on.
let acquisitions = 0;
let browserCalls = 0;
let verdictCalls = 0;
let openedOn: Browser[] = [];

/**
 * Builds the capture page double for one establishment. It answers the members the static-capture pipeline touches and records its own close, which is how the
 * ordering row reads that the failed attempt's page was released before anything waited on a verdict.
 * @returns The page double.
 */
function makeStubPage(): Page {

  return {

    browser: (): Browser => ({ connected: false } as unknown as Browser),
    close: async (): Promise<void> => { timeline.push("page:close"); },
    createCDPSession: async (): Promise<CDPSession> => ({ send: async (): Promise<unknown> => ({}) } as unknown as CDPSession),
    evaluate: async (): Promise<never> => { throw new Error("The stub page has no live DOM to evaluate against."); },
    evaluateOnNewDocument: async (): Promise<void> => { /* The injected video-selector helper needs no real document on a stub. */ },
    goto: async (): Promise<void> => { /* The static branch's one navigation needs no real destination. */ },
    isClosed: (): boolean => false,
    setBypassCSP: async (): Promise<void> => { /* Nothing to bypass on a stub. */ }
  } as unknown as Page;
}

const deps: CreatePageWithCaptureDeps = {

  acquireCaptureStream: async (): Promise<CaptureStream> => {

    acquisitions++;

    const failure = acquisitionFailures[acquisitions - 1];

    if(failure) {

      throw failure;
    }

    return Object.assign(new PassThrough(), { stop: async (): Promise<void> => undefined, stopped: Promise.resolve() });
  },
  awaitCaptureVerdict: (): Promise<Nullable<CaptureProbeOutcome>> => {

    timeline.push("verdict");
    verdictCalls++;

    return verdictAnswer();
  },
  emulateCaptureSurface: async (): Promise<{ height: number; width: number }> => ({ height: 1080, width: 1920 }),
  getCurrentBrowser: async (): Promise<Browser> => {

    browserCalls++;

    const failure = browserFailures[browserCalls - 1];

    if(failure) {

      throw failure;
    }

    return browsers[browserCalls - 1] ?? REFUSING_BROWSER;
  },
  installActivationHeal: async (): Promise<void> => { /* The activation heal is not what these rows measure. */ },
  openSharedWindowTab: async (browser: Browser): Promise<Page> => {

    openedOn.push(browser);

    return makeStubPage();
  },
  reaffirmCaptureSurface: async (): Promise<void> => { /* No compositor to re-affirm against. */ },
  resolveFFmpegPath: async (): Promise<string> => "ffmpeg",
  spawnFFmpeg: (): FFmpegProcess => makeFakeFFmpeg(),
  startOverlayHandling: async (): Promise<void> => { /* No overlays on a stub page. */ },
  syncWindowVisibility: async (): Promise<void> => { /* Window presentation is not what these rows measure. */ }
};

/**
 * Builds the establishment options every row runs. A static-capture profile with interception skipped leaves the capture acquisition and its catch as the only
 * pipeline the call exercises, which is exactly the region under test.
 * @returns The options for createPageWithCapture.
 */
function makeOptions(): Parameters<typeof createPageWithCapture>[0] {

  return { profile: makeProfile({ staticCapture: true }), settings: makeStreamSettings(), skipManifestInterception: true, streamId: "refusal-test",
    url: "https://static.example/page" };
}

/**
 * Returns the state every row starts from. Called from beforeEach, and again inside the row that drives two establishments of its own.
 */
function resetState(): void {

  acquisitionFailures = [];
  acquisitions = 0;
  browserCalls = 0;
  browserFailures = [];
  browsers = [];
  openedOn = [];
  timeline = [];
  verdictAnswer = async (): Promise<null> => null;
  verdictCalls = 0;
}

beforeEach(() => {

  resetState();
});

describe("createPageWithCapture - the refusal retry", () => {

  test("releases the refused attempt's page before it waits, then retries once and succeeds", async () => {

    /* The whole healed path in one row. The ordering assertion is the reason it exists: the relaunch a failed verdict triggers waits for every in-flight page,
     * so a tune that waited while still holding its own page would be waiting on something its own page forbids. The close recorded here is the second half of
     * the page disposer, whose first half releases the in-flight mark synchronously immediately before it.
     */
    acquisitionFailures = [new Error(CAPTURE_SOURCE_UNAVAILABLE_MESSAGE)];
    verdictAnswer = async (): Promise<CaptureProbeOutcome> => ({ kind: "captured" });

    const result = await createPageWithCapture(makeOptions(), deps);

    // Release the capture session the successful call transferred to us so its PassThrough does not linger past the row.
    result.captureSession.dispose();

    assert.equal(acquisitions, 2, "the refusal is followed by exactly one more capture start");
    assert.equal(verdictCalls, 1, "and exactly one verdict was asked for");
    assert.deepEqual(timeline, [ "page:close", "verdict" ], "the failed attempt's page was released before anything waited on the verdict");
  });

  test("retries on the browser the second acquisition hands back when the verdict is a failure", async () => {

    // A failed verdict settles only once the relaunch it triggered has settled, so by the time the retry re-acquires, the accessor is answering with the fresh
    // instance. The row reads which instance each attempt's page was opened on, which is what a retry that reused the refusing browser would get wrong.
    acquisitionFailures = [new Error(CAPTURE_SOURCE_UNAVAILABLE_MESSAGE)];
    browsers = [ REFUSING_BROWSER, FRESH_BROWSER ];
    verdictAnswer = async (): Promise<CaptureProbeOutcome> => ({ kind: "failed", reason: "the probe could not start a capture" });

    const result = await createPageWithCapture(makeOptions(), deps);

    result.captureSession.dispose();

    assert.equal(acquisitions, 2, "the retry ran");
    assert.equal(openedOn[0], REFUSING_BROWSER, "the refused attempt ran against the browser that refused it");
    assert.equal(openedOn[1], FRESH_BROWSER, "and the retry ran against the instance the relaunch published");
  });

  test("propagates the impaired refusal when the relaunch could not run", async () => {

    /* The third of acquire()'s outcomes, and the one that makes the retry safe to attempt unconditionally: with another stream established or another tune
     * mid-setup, the relaunch a failed verdict wanted cannot run, so the marked browser is still the published one and a capture purpose is refused at the
     * accessor. That refusal is already a quiet 503 upstream, so it travels rather than becoming a second failed establishment.
     */
    acquisitionFailures = [new Error(CAPTURE_SOURCE_UNAVAILABLE_MESSAGE)];
    browserFailures = [ undefined, new BrowserCaptureImpairedError({ reason: "the probe could not start a capture", since: 0 }) ];
    verdictAnswer = async (): Promise<CaptureProbeOutcome> => ({ kind: "failed", reason: "the probe could not start a capture" });

    await assert.rejects(createPageWithCapture(makeOptions(), deps), (error: unknown) => error instanceof BrowserCaptureImpairedError,
      "the supervisor's own refusal is what the caller sees");

    assert.equal(acquisitions, 1, "the retry never reached a capture start, because no browser was handed to it");
  });

  test("fails with the original refusal when the verdict decides nothing", async () => {

    /* Two answers mean the same thing and are asserted together: a probe that never obtained its turn is evidence about the lock's load rather than about the
     * browser, and no published browser means a disconnect already handled the readiness loss. Neither gives a second establishment anything to start from, so
     * the refusal the acquisition raised is the failure the caller sees, unchanged.
     */
    const answers: Nullable<CaptureProbeOutcome>[] = [ { kind: "inconclusive", reason: "Capture queue wait timed out." }, null ];

    for(const answer of answers) {

      resetState();

      const refusal = new Error(CAPTURE_SOURCE_UNAVAILABLE_MESSAGE);

      acquisitionFailures = [refusal];
      verdictAnswer = async (): Promise<Nullable<CaptureProbeOutcome>> => answer;

      // eslint-disable-next-line no-await-in-loop -- The two answers are asserted in sequence against one shared rig, which one establishment at a time requires.
      await assert.rejects(createPageWithCapture(makeOptions(), deps), (error: unknown) => error === refusal,
        "the acquisition's own rejection travels to the caller unchanged");

      assert.equal(acquisitions, 1, "no second establishment follows a verdict that decided nothing");
    }
  });

  test("stops at two establishments when the retry is refused as well", async () => {

    // The bound, read as a count. The marker travels in the options of the re-entry, so the second refusal finds it set and takes the ordinary failure path;
    // without it this row would recurse until something else stopped it.
    const second = new Error(CAPTURE_SOURCE_UNAVAILABLE_MESSAGE);

    acquisitionFailures = [ new Error(CAPTURE_SOURCE_UNAVAILABLE_MESSAGE), second ];
    verdictAnswer = async (): Promise<CaptureProbeOutcome> => ({ kind: "captured" });

    await assert.rejects(createPageWithCapture(makeOptions(), deps), (error: unknown) => error === second,
      "the second refusal is the tune's failure");

    assert.equal(acquisitions, 2, "there is never a third capture start");
  });

  test("hands a non-refusal capture failure to the verdict without waiting on it", async () => {

    /* The negative control for the whole retry, and the both-directions proof that the other branch does not wait. The capture-lock collision is a
     * capture-infrastructure failure the detector is still worth telling about, but it is not Chrome refusing a source, so nothing is retried - and the verdict
     * this row hands back never settles, so a branch that awaited it would hang here rather than fail.
     */
    const held = Promise.withResolvers<Nullable<CaptureProbeOutcome>>();

    acquisitionFailures = [new Error("Cannot capture a tab with an active stream.")];
    verdictAnswer = (): Promise<Nullable<CaptureProbeOutcome>> => held.promise;

    await assert.rejects(createPageWithCapture(makeOptions(), deps),
      (error: unknown) => (error instanceof Error) && error.message.includes("Cannot capture a tab with an active stream."),
      "the collision travels to the caller as itself");

    assert.equal(acquisitions, 1, "no retry follows a failure that is not a refusal");
    assert.equal(verdictCalls, 1, "the detector was told about it exactly once");

    // Settle the held verdict so nothing of this row outlives it.
    held.resolve(null);
  });
});
