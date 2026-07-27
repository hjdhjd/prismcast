/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.staticCapture.test.ts: Unit test pinning that createPageWithCapture launches a bounded staticCapture overlay poll for static-capture profiles - and only for
 * those. createPageWithCapture composes on the browser boundary through its CreatePageWithCaptureDeps collaborators, so the test drives it with a stub browser (no
 * Chrome launch), a PassThrough capture stream (no puppeteer-stream), and a recording overlay poll, while the real pipeline runs everything else. The stub page is
 * shaped so the static branch completes: injectVideoSelector uses only evaluateOnNewDocument (a no-op here); createCaptureSession merely wraps the injected
 * PassThrough; and resizeAndMinimizeWindow returns silently when its chrome-size probe (page.evaluate) rejects, which the stub arranges. Native capture mode skips the
 * FFmpeg path and skipManifestInterception avoids the CDP interceptor, leaving the static branch (page.goto then the staticCapture poll) as the only pipeline the call
 * exercises. The remaining browser calls (registerManagedPage, unregisterManagedPage, minimizeBrowserWindow) run real: they mutate an in-process page set or
 * early-return without a live browser, so they are inert against the stub.
 */
import type { Browser, Page } from "puppeteer-core";
import { before, beforeEach, describe, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import type { CreatePageWithCaptureDeps } from "./setup.ts";
import { PassThrough } from "node:stream";
import type { PuppeteerStream } from "puppeteer-stream";
import type { StartOverlayHandlingOptions } from "../browser/consent.ts";
import assert from "node:assert/strict";
import { createPageWithCapture } from "./setup.ts";
import { makeProfile } from "../config/profiles.helpers.ts";

// The overlay-handling options recorded by the injected startOverlayHandling, in call order, so the test can assert the phase and the absence of an abort signal for
// the static poll.
let overlayCalls: StartOverlayHandlingOptions[] = [];

// The URLs the stub page navigated to, so a regression that dropped the goto surfaces (the poll only makes sense after the capture page has loaded its content).
let pageGotos: string[] = [];

/* A minimal Page for the static-capture pipeline. goto records and resolves. evaluate rejects: injectVideoSelector never calls it (it uses evaluateOnNewDocument),
 * and resizeAndMinimizeWindow's chrome-size probe swallows the rejection and returns, so the success path completes without a CDP surface. For the non-static control,
 * the tune path's channel selection rejects the same way, failing that branch fast so no staticCapture poll is recorded. That path also fires video.ts's own overlay
 * poll through the real consent module (not this file's injected recorder); the poll's tick-error taxonomy reads page.browser().connected, so the stub reports a
 * disconnected browser to resolve the tick to "stop" and let the fire-and-forget poll settle cleanly rather than leaving a rejected promise pending after the test.
 */
function makeStubPage(): Page {

  return {

    browser: (): Browser => ({ connected: false } as unknown as Browser),
    evaluate: async (): Promise<never> => { throw new Error("The stub page has no live DOM to evaluate against."); },
    evaluateOnNewDocument: async (): Promise<void> => { /* The injected video-selector helper needs no real document on a stub. */ },
    goto: async (url: string): Promise<void> => { pageGotos.push(url); },
    isClosed: (): boolean => false,
    setBypassCSP: async (): Promise<void> => { /* Nothing to bypass on a stub. */ }
  } as unknown as Page;
}

/* The injected browser-boundary collaborators: getCurrentBrowser hands back a stub browser whose newPage returns the recording stub page (no Chrome), getStream yields
 * a real PassThrough so the real createCaptureSession has a stream to own (no puppeteer-stream), and startOverlayHandling records each poll's phase and abort signal in
 * place of a live poll. createPageWithCapture defaults every one of these to the real functions; substituting them here is what keeps the call off a live browser.
 */
const deps: CreatePageWithCaptureDeps = {

  getCurrentBrowser: async (): Promise<Browser> => ({ newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser),
  // getStream returns a real PassThrough augmented with a no-op stop so it satisfies puppeteer-stream's PuppeteerStream type, which requires a stop method, while
  // still handing createCaptureSession a genuine stream to own and destroy.
  getStream: async (): Promise<PuppeteerStream> => Object.assign(new PassThrough(), { stop: (): Promise<void> => Promise.resolve() }),
  startOverlayHandling: async (_page: Page, _profile: unknown, options: StartOverlayHandlingOptions): Promise<void> => { overlayCalls.push(options); }
};

before(() => {

  // Native capture mode skips the FFmpeg spawn/pipeline path so the static branch is reachable without a real subprocess.
  CONFIG.streaming.captureMode = "native";
});

beforeEach(() => {

  overlayCalls = [];
  pageGotos = [];
});

describe("createPageWithCapture - static-capture overlay poll", () => {

  test("launches exactly one staticCapture-phase poll with no abort signal for a static-capture profile", async () => {

    /* Traced path: the static branch (profile.staticCapture true) navigates once with page.goto and then fire-and-forgets startOverlayHandling with phase
     * staticCapture. The checks that would fail against a static branch that issued a bare goto with no poll: exactly one recorded call, its phase, and a signal
     * of undefined - the static poll has no abort owner (its bounded window is the terminator), so passing a controller signal would be the wrong shape.
     */
    const profile = makeProfile({ staticCapture: true });

    const result = await createPageWithCapture({ profile, skipManifestInterception: true, streamId: "static-test", url: "https://static.example/page" }, deps);

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

    await assert.rejects(createPageWithCapture({ profile, skipManifestInterception: true, streamId: "tune-test", url: "https://tune.example/live" }, deps),
      "the tune path fails against the stub page rather than reaching a static poll");

    assert.equal(overlayCalls.filter((call) => call.phase === "staticCapture").length, 0, "no staticCapture poll runs for a non-static profile");
  });
});
