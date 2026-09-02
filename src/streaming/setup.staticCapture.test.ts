/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.staticCapture.test.ts: Unit tests pinning two contracts of createPageWithCapture: that it launches a bounded staticCapture overlay poll for static-capture
 * profiles and only for those, and that it brings the browser window on screen, emulates the capture surface, and installs the activation heal before it acquires
 * capture, re-affirming that surface once acquisition has selected the tab. createPageWithCapture
 * composes on the browser boundary through its CreatePageWithCaptureDeps collaborators, so the test drives it with a stub browser (no Chrome launch), a PassThrough
 * capture stream (no puppeteer-stream), a recording overlay poll, a recording window sync, a recording surface emulation, and recording surface re-affirmation
 * steps, while the real pipeline runs everything else. The stub page is shaped so the static
 * branch completes: injectVideoSelector uses only evaluateOnNewDocument (a no-op here), and createCaptureSession merely wraps the injected PassThrough. Native
 * capture mode skips the FFmpeg path and skipManifestInterception avoids the CDP interceptor, leaving the static branch (page.goto then the staticCapture poll) as
 * the only pipeline the call exercises. The remaining browser calls (registerManagedPage, unregisterManagedPage) run real: they mutate an in-process page set, so
 * they are inert against the stub.
 */
import type { Browser, CDPSession, Page } from "puppeteer-core";
import { before, beforeEach, describe, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import type { CaptureStream } from "../browser/tabCapture.ts";
import type { CreatePageWithCaptureDeps } from "./setup.ts";
import { PassThrough } from "node:stream";
import type { StartOverlayHandlingOptions } from "../browser/consent.ts";
import assert from "node:assert/strict";
import { createPageWithCapture } from "./setup.ts";
import { makeProfile } from "../config/profiles.helpers.ts";

// The overlay-handling options recorded by the injected startOverlayHandling, in call order, so the test can assert the phase and the absence of an abort signal for
// the static poll.
let overlayCalls: StartOverlayHandlingOptions[] = [];

// The URLs the stub page navigated to, so a regression that dropped the goto surfaces (the poll only makes sense after the capture page has loaded its content).
let pageGotos: string[] = [];

// The injected collaborator calls the establishment makes, in order, so the ordering pin can read where the window sync lands relative to capture acquisition.
let depsCalls: string[] = [];

// The page argument each window sync received, in call order. The pass at the top of the establishment has no page yet; the pass that closes it hands over the page
// it just built.
let syncPages: (Page | undefined)[] = [];

// The page each capture-surface emulation received, in call order, so the pin can check the density step landed on the very page the establishment went on to
// capture rather than on some other page.
let surfacePages: Page[] = [];

// The page each activation-hook install and each surface re-affirmation received, in call order, so the pin can check both landed on the establishment's own page.
let focusHookPages: Page[] = [];
let reaffirmPages: Page[] = [];

/* A minimal Page for the static-capture pipeline. goto records and resolves. evaluate rejects: injectVideoSelector never calls it (it uses evaluateOnNewDocument),
 * and nothing else on the success path measures the page. For the non-static control, the tune path's channel selection rejects the same way, failing that branch
 * fast so no staticCapture poll is recorded. That path also fires video.ts's own overlay poll through the real consent module (not this file's injected recorder);
 * the poll's tick-error taxonomy reads page.browser().connected, so the stub reports a disconnected browser to resolve the tick to "stop" and let the
 * fire-and-forget poll settle cleanly rather than leaving a rejected promise pending after the test.
 */
function makeStubPage(): Page {

  return {

    browser: (): Browser => ({ connected: false } as unknown as Browser),
    createCDPSession: async (): Promise<CDPSession> => ({ send: async (): Promise<unknown> => ({}) } as unknown as CDPSession),
    evaluate: async (): Promise<never> => { throw new Error("The stub page has no live DOM to evaluate against."); },
    evaluateOnNewDocument: async (): Promise<void> => { /* The injected video-selector helper needs no real document on a stub. */ },
    goto: async (url: string): Promise<void> => { pageGotos.push(url); },
    isClosed: (): boolean => false,
    setBypassCSP: async (): Promise<void> => { /* Nothing to bypass on a stub. */ }
  } as unknown as Page;
}

/* The injected browser-boundary collaborators: getCurrentBrowser hands back a stub browser whose newPage returns the recording stub page (no Chrome),
 * acquireCaptureStream yields a real PassThrough so the real createCaptureSession has a stream to own (no extension protocol), startOverlayHandling records each
 * poll's phase and abort signal in place of a live poll, syncWindowVisibility records the window passes in place of CDP traffic, emulateCaptureSurface records the
 * density step and answers with a fixed surface so the capture constraints it feeds stay total, and installCaptureFocusHook and reaffirmCaptureSurface record the
 * two surface-re-affirmation steps in place of page injection and raw CDP. createPageWithCapture defaults every one of these to the real functions; substituting
 * them here is what keeps the call off a live browser, and recording the acquisition alongside the rest is what makes their order observable.
 */
const deps: CreatePageWithCaptureDeps = {

  // The acquisition hands back a real PassThrough carrying the two capture controls, so createCaptureSession owns a genuine stream and destroys a real one.
  acquireCaptureStream: async (): Promise<CaptureStream> => {

    depsCalls.push("acquireCaptureStream");

    return Object.assign(new PassThrough(), { stop: async (): Promise<void> => undefined, stopped: Promise.resolve() });
  },
  emulateCaptureSurface: async (page: Page): Promise<{ height: number; width: number }> => {

    depsCalls.push("emulateCaptureSurface");
    surfacePages.push(page);

    return { height: 1080, width: 1920 };
  },
  getCurrentBrowser: async (): Promise<Browser> => ({ newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser),
  installCaptureFocusHook: async (page: Page): Promise<void> => {

    depsCalls.push("installCaptureFocusHook");
    focusHookPages.push(page);
  },
  reaffirmCaptureSurface: async (page: Page): Promise<void> => {

    depsCalls.push("reaffirmCaptureSurface");
    reaffirmPages.push(page);
  },
  startOverlayHandling: async (_page: Page, _profile: unknown, options: StartOverlayHandlingOptions): Promise<void> => { overlayCalls.push(options); },
  syncWindowVisibility: async (page?: Page): Promise<void> => {

    depsCalls.push("syncWindowVisibility");
    syncPages.push(page);
  }
};

before(() => {

  // Native capture mode skips the FFmpeg spawn/pipeline path so the static branch is reachable without a real subprocess.
  CONFIG.streaming.captureMode = "native";
});

beforeEach(() => {

  depsCalls = [];
  focusHookPages = [];
  overlayCalls = [];
  pageGotos = [];
  reaffirmPages = [];
  surfacePages = [];
  syncPages = [];
});

describe("createPageWithCapture - static-capture overlay poll", () => {

  test("launches exactly one staticCapture-phase poll with no abort signal for a static-capture profile", async () => {

    /* Traced path: the static branch (profile.staticCapture true) navigates once with page.goto and then fire-and-forgets startOverlayHandling with phase
     * staticCapture. The checks that would fail against a static branch that issued a bare goto with no poll: exactly one recorded call, its phase, and a signal
     * of undefined - the static poll has no abort owner (its bounded window is the terminator), so passing a controller signal would be the wrong shape.
     */
    const profile = makeProfile({ staticCapture: true });

    const result = await createPageWithCapture(
      { numericStreamId: 1, profile, skipManifestInterception: true, streamId: "static-test", url: "https://static.example/page" }, deps);

    // Release the capture session the successful call transferred to us so its PassThrough does not linger past the test.
    result.captureSession.dispose();

    assert.deepEqual(pageGotos, ["https://static.example/page"], "the static capture navigates to the requested URL once");
    assert.equal(overlayCalls.length, 1, "exactly one overlay poll is launched for the static capture");

    const call = overlayCalls[0];

    assert.ok(call, "the overlay poll was recorded");
    assert.equal(call.phase, "staticCapture", "the static capture runs the poll under the staticCapture phase");
    assert.equal(call.signal, undefined, "the static poll carries no abort signal - its bounded window is the terminator");
  });

  test("launches no staticCapture-phase poll for a non-static profile (the discriminator)", async () => {

    // The complementary control: a non-static profile takes the tune path, whose channel selection fails fast here (the stub's evaluate rejects), so no
    // startOverlayHandling call is recorded through the injected collaborators - and specifically none under the staticCapture phase. The tune path's own overlay poll
    // runs through video.ts's real collaborators, not this test's injected collaborators, so it never reaches overlayCalls. If the static poll were launched
    // unconditionally rather than gated on profile.staticCapture, this run would record a staticCapture call regardless.
    const profile = makeProfile({ staticCapture: false });

    await assert.rejects(createPageWithCapture({ numericStreamId: 2, profile, skipManifestInterception: true, streamId: "tune-test", url: "https://tune.example/live" },
      deps), "the tune path fails against the stub page rather than reaching a static poll");

    assert.equal(overlayCalls.filter((call) => call.phase === "staticCapture").length, 0, "no staticCapture poll runs for a non-static profile");
  });
});

describe("createPageWithCapture - window visibility ordering", () => {

  test("brings the window on screen before acquiring capture, and hands the established page to the closing pass", async () => {

    /* Tab capture consumes the compositor's output for the shared window, and that output is only composed for capture to read while the window is presented - so
     * the sync has to land before the acquisition, never alongside or after it. The capture surface is emulated in the same window, between the two: the page has to
     * carry the preset's dimensions and the display's density before capture acquires it, or the track is acquired against a surface nobody declared. The recorded
     * call order is the pin: moving either step below capture acquisition reorders these entries and fails here. The closing entry is the pass that ends the
     * establishment, which carries the page it just built so the executor can use that tab's CDP session rather than hunting for an open page.
     *
     * The two re-affirmation steps bracket capture acquisition for a reason of their own. Acquisition selects the capture's tab - the capture extension targets
     * whichever tab is active - so the composition it starts from is the window's fitted view of the page; the re-issue that follows moves it to the emulated
     * surface. The activation heal is installed before all of that, so the page carries its focus listener from its first document onward. The static branch this
     * test drives reaches both, which is what makes the pin honest here.
     */
    const profile = makeProfile({ staticCapture: true });

    const result = await createPageWithCapture(
      { numericStreamId: 3, profile, skipManifestInterception: true, streamId: "order-test", url: "https://static.example/page" }, deps);

    // Release the capture session the successful call transferred to us so its PassThrough does not linger past the test.
    result.captureSession.dispose();

    assert.deepEqual(depsCalls,
      [ "syncWindowVisibility", "emulateCaptureSurface", "installCaptureFocusHook", "acquireCaptureStream", "reaffirmCaptureSurface", "syncWindowVisibility" ],
      "the window sync leads the establishment and closes it, with the surface emulated, the activation heal installed, capture acquired, and the surface " +
      "re-affirmed in between");
    assert.equal(syncPages[0], undefined, "the leading pass has no page yet - it runs before the capture page exists");
    assert.equal(syncPages[1], result.page, "the closing pass receives the page the establishment built");
    assert.equal(surfacePages[0], result.page, "the surface is emulated on the very page the establishment captured and handed back");
    assert.equal(focusHookPages[0], result.page, "the activation heal is installed on that same page");
    assert.equal(reaffirmPages[0], result.page, "the closing re-affirmation is issued against that same page");
  });
});
