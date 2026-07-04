/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * video.diagnosis.test.ts: Unit tests for the failed-tune blocked-page diagnosis in video.ts (diagnoseBlockedTune, reached through initializePlayback's failure
 * sites). video.test.ts statically value-imports video.ts, so mock.module cannot take effect there; this sibling file follows the precaching.revalidation.test.ts
 * precedent - register the module mocks first, then bind initializePlayback through a dynamic import so its captured imports resolve to the overrides. The
 * channelSelection, blockedPage, and consent modules are mocked at the module boundary; the health module is real, so marking assertions go through
 * getDomainAuthState, and each test uses its own domain to keep the shared in-memory auth state isolated.
 */
import type * as VideoModule from "./video.ts";
import type { BlockedPageClassification, ClassifyBlockedPageOptions } from "./blockedPage.ts";
import type { ChannelSelectorResult, ProviderModule } from "../types/index.ts";
import { afterEach, before, beforeEach, describe, mock, test } from "node:test";
import { LOG } from "../utils/index.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { getDomainAuthState } from "../config/health.ts";
import { setImmediate as immediate } from "node:timers/promises";
import { makeProfile } from "../config/profiles.helpers.ts";

// Mutable state the module mocks read, so each test can shape the provider match, the channel selection outcome, and the classification without re-registering
// mocks. classifyCalls records the options object of every classifier invocation, so tests can assert both the call count and the forwarded indicators.
let classifyCalls: ClassifyBlockedPageOptions[] = [];
let classifyResult: () => Promise<BlockedPageClassification>;
let mockDomainProviders: ProviderModule[] = [];
let mockSelectResult: ChannelSelectorResult = { success: true };

// The function under test, bound after mock.module registration via dynamic import.
let initializePlayback: typeof VideoModule.initializePlayback;

/* Builds a stub Page satisfying the surface the exercised initializePlayback paths touch: evaluate for the best-effort video mute, and waitForSelector for the
 * video-readiness wait (site B) and the iframe context resolution (site C). A test drives a failure site by passing a waitForSelector that rejects; the default
 * resolves so selection-failure tests (site A) never reach it.
 */
function makeStubPage(waitForSelector?: (selector: string, options?: unknown) => Promise<unknown>): Page {

  return {

    evaluate: async (): Promise<unknown> => undefined,
    isClosed: (): boolean => false,
    url: (): string => "https://www.stub-tune.test/landed",
    waitForSelector: waitForSelector ?? (async (): Promise<unknown> => ({}))
  } as unknown as Page;
}

before(async () => {

  /* Like the precaching.revalidation.test.ts precedent, the mocks enumerate their named exports explicitly: the enumerated names are exactly the surface video.ts
   * consumes from each module. The utils and config modules stay real - LOG method mocks and getDomainAuthState assertions must observe the same instances
   * video.ts uses.
   */
  const selectionUrl = new URL("./channelSelection.ts", import.meta.url).href;

  mock.module(selectionUrl, {

    namedExports: {

      getProvidersForDomain: (): ProviderModule[] => mockDomainProviders,
      invalidateDirectUrl: (): void => { /* The cached-URL layer is not under test. */ },
      resolveDirectUrl: async (): Promise<null> => null,
      selectChannel: async (): Promise<ChannelSelectorResult> => mockSelectResult
    }
  });

  const blockedPageUrl = new URL("./blockedPage.ts", import.meta.url).href;

  mock.module(blockedPageUrl, {

    namedExports: {

      classifyBlockedPage: async (page: Page, options: ClassifyBlockedPageOptions): Promise<BlockedPageClassification> => {

        classifyCalls.push(options);

        return classifyResult();
      }
    }
  });

  const consentUrl = new URL("./consent.ts", import.meta.url).href;

  mock.module(consentUrl, {

    namedExports: {

      startOverlayHandling: async (): Promise<void> => { /* The overlay poll is not under test. */ }
    }
  });

  // Dynamic-import video.ts now that the mocks are registered, so its captured imports resolve to the overrides above.
  const videoModule = await import("./video.ts");

  initializePlayback = videoModule.initializePlayback;
});

describe("initializePlayback - failed-tune blocked-page diagnosis", () => {

  beforeEach(() => {

    classifyCalls = [];
    classifyResult = async (): Promise<BlockedPageClassification> => ({ kind: "unknown" });
    mockDomainProviders = [];
    mockSelectResult = { success: true };

    // Suppress the health flush debounce timer so nothing fires against a real data directory, and park raceWithTimeout's classification-budget timer so the
    // timeout test can drive its expiry explicitly.
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("marks the domain, warns, and enriches the error when the wall sits on a registered provider's domain", async (t) => {

    /* Traced path: the authWall provider arm in diagnoseBlockedTune - markDomainAuthRequired(domain), the WARN naming the provider label, the evidence, and the
     * remedy, then the enriched throw - reached from the selection-failure site in initializePlayback. A mutation dropping the mark, the WARN, or any message
     * component fails here.
     */
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const indicators = { hosts: ["auth.stub-a.test"] };

    mockDomainProviders = [{ authWallIndicators: indicators, guideUrl: "https://www.stub-a.test/guide", label: "Stub Provider" } as unknown as ProviderModule];
    mockSelectResult = { reason: "Station code FBN not found.", success: false };
    classifyResult = async (): Promise<BlockedPageClassification> => ({ evidence: "a sign-in form is present at stub-a.test/signin", kind: "authWall" });

    await assert.rejects(initializePlayback(makeStubPage(), makeProfile(), { requestedUrl: "https://www.stub-a.test/guide" }), (error: unknown) => {

      assert.ok(error instanceof Error, "the enriched failure is an Error");
      assert.equal(error.message, "Tune failed: Stub Provider is presenting an authentication wall (a sign-in form is present at stub-a.test/signin). " +
        "Sign in from the channel table's login icon.", "the error names the provider, the evidence, and the remedy");

      return true;
    });

    assert.equal(getDomainAuthState("stub-a.test")?.status, "needsLogin", "the wall sighting marks the provider's domain needs-sign-in");
    assert.equal(classifyCalls.length, 1, "the classifier ran once");
    assert.equal(classifyCalls[0]?.indicators, indicators, "the provider's declared indicators are forwarded to the classifier");

    const warnLine = warn.mock.calls.find((call) => String(call.arguments[0]).includes("authentication wall"));

    assert.ok(warnLine, "the wall is reported at WARN");
    assert.ok(String(warnLine.arguments[0]).includes("Sign in from the channel table's login icon."), "the WARN carries the remedy");
    assert.equal(warnLine.arguments[1], "Stub Provider", "the WARN names the provider label");
    assert.equal(warnLine.arguments[2], "a sign-in form is present at stub-a.test/signin", "the WARN carries the evidence");
  });

  test("reports a wall on an unregistered domain without marking and without the remedy sentence", async () => {

    /* Traced path: the authWall no-provider arm in diagnoseBlockedTune - no markDomainAuthRequired, no remedy sentence, an enriched error naming the domain and
     * the evidence only. An ad-hoc /play or custom-channel domain has no channel-table row to point at, and an unmarked entry would have no clearing path.
     */
    mockSelectResult = { reason: "Station code FBN not found.", success: false };
    classifyResult = async (): Promise<BlockedPageClassification> => ({ evidence: "a sign-in form is present at stub-b.test/login", kind: "authWall" });

    await assert.rejects(initializePlayback(makeStubPage(), makeProfile(), { requestedUrl: "https://www.stub-b.test/play" }), (error: unknown) => {

      assert.ok(error instanceof Error, "the enriched failure is an Error");
      assert.equal(error.message, "Tune failed: stub-b.test is presenting an authentication wall (a sign-in form is present at stub-b.test/login).",
        "the error names the domain and the evidence, with no remedy sentence");

      return true;
    });

    assert.equal(getDomainAuthState("stub-b.test"), null, "no needs-sign-in mark lands without a registered provider");
    assert.equal(classifyCalls[0]?.indicators, undefined, "no indicators exist to forward without a provider match");
  });

  test("surfaces the standing consent guidance text, character-identical, when a consent overlay blocks the page", async () => {

    /* Traced path: the consentOverlay arm in diagnoseBlockedTune. The guidance text is the detect-and-guide sentence that previously lived inline in
     * initializePlayback's video-wait catch; equality (not substring) pins it character-identical.
     */
    mockSelectResult = { reason: "Station code FBN not found.", success: false };
    classifyResult = async (): Promise<BlockedPageClassification> => ({ kind: "consentOverlay" });

    await assert.rejects(initializePlayback(makeStubPage(), makeProfile(), { requestedUrl: "https://www.stub-c.test/guide" }), (error: unknown) => {

      assert.ok(error instanceof Error, "the guidance is an Error");
      assert.equal(error.message, "This site is displaying a consent or cookie prompt that is blocking playback. Open it once in setup or login mode and " +
        "dismiss the prompt - your choice is remembered.", "the consent guidance text is character-identical");

      return true;
    });

    assert.equal(getDomainAuthState("stub-c.test"), null, "a consent overlay never marks the domain");
  });

  test("rethrows the video-wait rejection by reference when the page classifies as unknown", async () => {

    /* Traced path: the default (unknown) arm in diagnoseBlockedTune, reached from the video-wait catch in initializePlayback - the original rejection value must
     * propagate unchanged, asserted by reference identity, with the classifier having run exactly once.
     */
    const sentinel = new Error("Waiting for selector `video` failed: Waiting failed: 30000ms exceeded");

    mockSelectResult = { success: true };

    const page = makeStubPage(async (): Promise<unknown> => {

      throw sentinel;
    });

    await assert.rejects(initializePlayback(page, makeProfile(), { requestedUrl: "https://www.stub-d.test/guide" }), (error: unknown) => {

      assert.equal(error, sentinel, "the raw rejection propagates by reference");

      return true;
    });

    assert.equal(classifyCalls.length, 1, "the failure routed through the classifier before rethrowing");
    assert.equal(getDomainAuthState("stub-d.test"), null, "an unknown page never marks the domain");
  });

  test("treats a classification that outruns its budget as unknown and rethrows the original error", async () => {

    /* Traced path: the raceWithTimeout wrapper around classifyBlockedPage in diagnoseBlockedTune - a classification promise that never settles must be abandoned
     * at the budget and treated as unknown, so the original selection error surfaces byte-identical. The budget timer is driven explicitly through mocked
     * setTimeout; ticking past the four-second classification budget fires it.
     */
    mockSelectResult = { reason: "Station code FBN not found.", success: false };
    classifyResult = (): Promise<BlockedPageClassification> => Promise.withResolvers<BlockedPageClassification>().promise;

    const pending = initializePlayback(makeStubPage(), makeProfile(), { requestedUrl: "https://www.stub-e.test/guide" });

    const rejection = assert.rejects(pending, (error: unknown) => {

      assert.ok(error instanceof Error, "the original failure is an Error");
      assert.equal(error.message, "Channel selection failed: Station code FBN not found.", "the original selection error surfaces byte-identical");

      return true;
    });

    // Let the tune reach the classification race (pure microtasks up to the timer registration), then expire the budget.
    await immediate();
    mock.timers.tick(5000);

    await rejection;

    assert.equal(classifyCalls.length, 1, "the classifier was invoked before its budget expired");
    assert.equal(getDomainAuthState("stub-e.test"), null, "a timed-out classification never marks the domain");
  });

  test("throws today's exact selection error with zero classifier invocation when requestedUrl is absent", async () => {

    /* Traced path: the requestedUrl presence gate at the selection-failure site in initializePlayback. With the option absent, the failure must produce the plain
     * selection error, byte-identical to the pre-diagnosis behavior, and the classifier must never run.
     */
    mockSelectResult = { reason: "Station code FBN not found.", success: false };

    await assert.rejects(initializePlayback(makeStubPage(), makeProfile(), {}), (error: unknown) => {

      assert.ok(error instanceof Error, "the failure is an Error");
      assert.equal(error.message, "Channel selection failed: Station code FBN not found.", "the selection error is byte-identical to the plain throw");

      return true;
    });

    assert.equal(classifyCalls.length, 0, "no classifier pass runs without the option");
  });

  test("routes a video-context resolution failure through the diagnosis and rethrows the raw rejection when unknown", async () => {

    /* Traced path: the findVideoContext try/catch in initializePlayback (an iframe-handling profile whose player iframe never appears rejects with a raw selector
     * timeout) - the rejection must route through diagnoseBlockedTune, proven by the classifier invocation, and the unknown outcome must rethrow the original
     * rejection by reference.
     */
    const sentinel = new Error("Waiting for selector `iframe` failed: Waiting failed: 30000ms exceeded");

    mockSelectResult = { success: true };

    const page = makeStubPage(async (): Promise<unknown> => {

      throw sentinel;
    });

    await assert.rejects(initializePlayback(page, makeProfile({ needsIframeHandling: true }), { requestedUrl: "https://www.stub-f.test/guide" }), (error: unknown) => {

      assert.equal(error, sentinel, "the raw context-resolution rejection propagates by reference");

      return true;
    });

    assert.equal(classifyCalls.length, 1, "the context-resolution failure routed through the classifier");
  });
});
