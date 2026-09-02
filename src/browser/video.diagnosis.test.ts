/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * video.diagnosis.test.ts: Unit tests for the failed-tune blocked-page diagnosis in video.ts (diagnoseBlockedTune, reached through initializePlayback's failure
 * sites). initializePlayback accepts its tune collaborators (selectChannel and the overlay poll on the main path; the provider lookup and blocked-page classifier
 * on the failure path) as an injected VideoTuneDeps parameter, so we pass a deps object of stubs through that interface and never drive a browser. The health
 * module is real, so marking assertions go through getDomainAuthState, and each test uses its own domain to keep the shared in-memory auth state isolated.
 */
import type { BlockedPageClassification, ClassifyBlockedPageOptions } from "./blockedPage.ts";
import type { ChannelSelectorResult, ProviderModule } from "../types/index.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { LOG } from "../utils/index.ts";
import type { OverlayPhase } from "./consent.ts";
import type { Page } from "puppeteer-core";
import type { VideoTuneDeps } from "./video.ts";
import assert from "node:assert/strict";
import { getDomainAuthState } from "../config/health.ts";
import { setImmediate as immediate } from "node:timers/promises";
import { initializePlayback } from "./video.ts";
import { makeProfile } from "../config/profiles.helpers.ts";

// A record of one startOverlayHandling invocation, captured by the deps.startOverlayHandling double. abortedAtCall and priorTuneSetupAborted snapshot abort state AT
// THE MOMENT of the call - the fields that assert the phase-poll lifecycle ordering (a post-hoc read of the signal is useless because every poll's finally
// aborts its signal by the time the tune settles).
interface OverlayCallRecord {

  // Whether this call's own signal was already aborted when the poll was launched.
  abortedAtCall: boolean;

  // The videoWait arm's embed-gate resolver, captured so a test can deterministically make the gate win the video-wait race.
  onEmbedGateAccepted?: () => void;

  // The phase declared for this poll.
  phase: OverlayPhase;

  // Whether the earliest tuneSetup poll's signal was already aborted when this call was launched, or null when no tuneSetup poll has been recorded yet.
  priorTuneSetupAborted: boolean | null;

  // This poll's abort signal, retained so a test can read its final aborted state after the tune settles.
  signal?: AbortSignal;
}

// Mutable state the module mocks read, so each test can shape the provider match, the channel selection outcome, and the classification without re-registering
// mocks. classifyCalls records the options object of every classifier invocation, so tests can assert both the call count and the forwarded indicators.
let classifyCalls: ClassifyBlockedPageOptions[] = [];
let classifyResult: () => Promise<BlockedPageClassification>;
let mockDomainProviders: ProviderModule[] = [];
let mockSelectResult: ChannelSelectorResult = { success: true };

// startOverlayHandling invocations in order, and the tuneSetup poll's aborted state captured at each classifier invocation - the two channels the phase-poll
// lifecycle tests assert over.
let overlayCalls: OverlayCallRecord[] = [];
let classifyTuneSetupAborted: (boolean | null)[] = [];

/* The injected tune collaborators: channel selection and the overlay poll on the main path, plus the provider lookup and blocked-page classifier on the failure
 * path, substituted at video.ts's VideoTuneDeps interface so the diagnosis paths run without a real browser. Each field reads the mutable module state above at call
 * time. classifyBlockedPage records every options object into classifyCalls and snapshots the tuneSetup poll's aborted state into classifyTuneSetupAborted, and
 * startOverlayHandling records every poll's phase, its own aborted-at-call state, and (for videoWait) the embed-gate resolver - the channels the phase-poll
 * lifecycle tests assert over. Typed as the production port so the doubles cannot drift, and contextually typed so options resolves to StartOverlayHandlingOptions
 * without importing it; invalidateDirectUrl/resolveDirectUrl stay real because initializePlayback's tested paths never call them.
 */
const deps: VideoTuneDeps = {

  classifyBlockedPage: async (_page, options) => {

    classifyCalls.push(options);

    // Snapshot the tuneSetup poll's aborted state at the moment of classification. The pre-diagnosis abort() must have already fired, so an in-span failure
    // classifies a poll-quiet page; this is what tells apart the explicit abort from a too-late shared-finally abort.
    const tuneSetupSignal = overlayCalls.find((call) => call.phase === "tuneSetup")?.signal;

    classifyTuneSetupAborted.push(tuneSetupSignal ? tuneSetupSignal.aborted : null);

    return classifyResult();
  },
  getProvidersForDomain: (): ProviderModule[] => mockDomainProviders,
  selectChannel: async (): Promise<ChannelSelectorResult> => mockSelectResult,

  // The real poll is not under test, but its options ARE: the double records each call's phase, its own aborted-at-call state, the tuneSetup poll's aborted state at
  // that moment, and (for videoWait) the embed-gate resolver, so the phase-poll lifecycle tests can assert the poll choreography without a live Chrome.
  startOverlayHandling: async (_page, _profile, options): Promise<void> => {

    const tuneSetupSignal = overlayCalls.find((call) => call.phase === "tuneSetup")?.signal;

    overlayCalls.push({

      abortedAtCall: options.signal?.aborted ?? false,
      onEmbedGateAccepted: (options.phase === "videoWait") ? options.onEmbedGateAccepted : undefined,
      phase: options.phase,
      priorTuneSetupAborted: tuneSetupSignal ? tuneSetupSignal.aborted : null,
      signal: options.signal
    });
  }
};

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

describe("initializePlayback - failed-tune blocked-page diagnosis", () => {

  beforeEach(() => {

    classifyCalls = [];
    classifyResult = async (): Promise<BlockedPageClassification> => ({ kind: "unknown" });
    mockDomainProviders = [];
    mockSelectResult = { success: true };
    overlayCalls = [];
    classifyTuneSetupAborted = [];

    // Suppress the health flush debounce timer so nothing fires against a real data directory during these diagnosis tests.
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

    await assert.rejects(initializePlayback(makeStubPage(), makeProfile(), { requestedUrl: "https://www.stub-a.test/guide" }, deps), (error: unknown) => {

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

    await assert.rejects(initializePlayback(makeStubPage(), makeProfile(), { requestedUrl: "https://www.stub-b.test/play" }, deps), (error: unknown) => {

      assert.ok(error instanceof Error, "the enriched failure is an Error");
      assert.equal(error.message, "Tune failed: stub-b.test is presenting an authentication wall (a sign-in form is present at stub-b.test/login).",
        "the error names the domain and the evidence, with no remedy sentence");

      return true;
    });

    assert.equal(getDomainAuthState("stub-b.test"), null, "no needs-sign-in mark lands without a registered provider");
    assert.equal(classifyCalls[0]?.indicators, undefined, "no indicators exist to forward without a provider match");
  });

  test("surfaces the standing consent guidance text, character-identical, when a consent overlay blocks the page", async () => {

    /* Traced path: the consentOverlay arm in diagnoseBlockedTune. The guidance text is the detect-and-guide sentence diagnoseBlockedTune throws for a
     * consentOverlay classification; equality (not substring) asserts it character-identical.
     */
    mockSelectResult = { reason: "Station code FBN not found.", success: false };
    classifyResult = async (): Promise<BlockedPageClassification> => ({ kind: "consentOverlay" });

    await assert.rejects(initializePlayback(makeStubPage(), makeProfile(), { requestedUrl: "https://www.stub-c.test/guide" }, deps), (error: unknown) => {

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

    await assert.rejects(initializePlayback(page, makeProfile(), { requestedUrl: "https://www.stub-d.test/guide" }, deps), (error: unknown) => {

      assert.equal(error, sentinel, "the raw rejection propagates by reference");

      return true;
    });

    assert.equal(classifyCalls.length, 1, "the failure routed through the classifier before rethrowing");
    assert.equal(getDomainAuthState("stub-d.test"), null, "an unknown page never marks the domain");
  });

  test("throws today's exact selection error with zero classifier invocation when requestedUrl is absent", async () => {

    /* Traced path: the requestedUrl presence gate at the selection-failure site in initializePlayback. With the option absent, the failure must produce the plain
     * selection error, byte-identical to the pre-diagnosis behavior, and the classifier must never run.
     */
    mockSelectResult = { reason: "Station code FBN not found.", success: false };

    await assert.rejects(initializePlayback(makeStubPage(), makeProfile(), {}, deps), (error: unknown) => {

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

    const pending = initializePlayback(page, makeProfile({ needsIframeHandling: true }), { requestedUrl: "https://www.stub-f.test/guide" }, deps);

    await assert.rejects(pending, (error: unknown) => {

      assert.equal(error, sentinel, "the raw context-resolution rejection propagates by reference");

      return true;
    });

    assert.equal(classifyCalls.length, 1, "the context-resolution failure routed through the classifier");
  });
});

describe("initializePlayback - phase-scoped overlay poll lifecycle", () => {

  beforeEach(() => {

    classifyCalls = [];
    classifyResult = async (): Promise<BlockedPageClassification> => ({ kind: "unknown" });
    mockDomainProviders = [];
    mockSelectResult = { success: true };
    overlayCalls = [];
    classifyTuneSetupAborted = [];

    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("launches the tuneSetup poll un-aborted, then the videoWait poll only after the tuneSetup poll is aborted", async (t) => {

    /* Traced path: the tuneSetup withOverlayGuard span wraps channel selection, context resolution, and click-to-play; its finally aborts the tuneSetup poll at the
     * phase boundary before the videoWait poll launches. A video-wait failure makes the tune reject after both polls have been launched, so both are observable. The
     * abortedAtCall/priorTuneSetupAborted snapshots would both read differently if the guard aborted late or the polls overlapped.
     */
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });
    t.mock.method(LOG, "warn", () => { /* Silenced. */ });

    const sentinel = new Error("Waiting for selector `video` failed: Waiting failed: 30000ms exceeded");

    mockSelectResult = { success: true };

    const page = makeStubPage(async (): Promise<unknown> => {

      throw sentinel;
    });

    await assert.rejects(initializePlayback(page, makeProfile(), { requestedUrl: "https://www.stub-order-a.test/guide" }, deps));

    assert.equal(overlayCalls.length, 2, "exactly two polls run for a tune that fails at the video wait - tuneSetup then videoWait");

    const tuneSetupCall = overlayCalls[0];
    const videoWaitCall = overlayCalls[1];

    assert.ok(tuneSetupCall, "the tuneSetup poll was launched");
    assert.ok(videoWaitCall, "the videoWait poll was launched");
    assert.equal(tuneSetupCall.phase, "tuneSetup", "the tuneSetup poll launches first");
    assert.equal(tuneSetupCall.abortedAtCall, false, "the tuneSetup poll starts un-aborted");
    assert.equal(videoWaitCall.phase, "videoWait", "the videoWait poll launches second");
    assert.equal(videoWaitCall.priorTuneSetupAborted, true, "the tuneSetup poll is already aborted by the time the videoWait poll launches - the polls never overlap");
  });

  test("aborts the tuneSetup poll before diagnosing a channel-selection failure", async (t) => {

    /* Traced path: the selection-failure site inside the tuneSetup span calls abortGuard() before diagnoseBlockedTune. The classifier mock records the tuneSetup
     * signal's aborted state at classification time; it must be true, which is exactly what distinguishes the explicit pre-diagnosis abort from a too-late abort in
     * the guard's shared finally (that would abort only after the classifier had already read a still-clicking page).
     */
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });
    t.mock.method(LOG, "warn", () => { /* Silenced. */ });

    mockSelectResult = { reason: "Station code FBN not found.", success: false };

    await assert.rejects(initializePlayback(makeStubPage(), makeProfile(), { requestedUrl: "https://www.stub-order-b.test/guide" }, deps));

    assert.equal(classifyCalls.length, 1, "the selection failure routed through the classifier");
    assert.equal(classifyTuneSetupAborted[0], true, "the tuneSetup poll was aborted before the page was classified");
  });

  test("aborts the tuneSetup poll before diagnosing a video-context resolution failure", async (t) => {

    /* Traced path: the findVideoContext catch inside the tuneSetup span calls abortGuard() before diagnoseBlockedTune. Same pre-diagnosis-abort signal as the
     * selection-failure path, on the other in-span failure site.
     */
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });
    t.mock.method(LOG, "warn", () => { /* Silenced. */ });

    const sentinel = new Error("Waiting for selector `iframe` failed: Waiting failed: 30000ms exceeded");

    mockSelectResult = { success: true };

    const page = makeStubPage(async (): Promise<unknown> => {

      throw sentinel;
    });

    await assert.rejects(initializePlayback(page, makeProfile({ needsIframeHandling: true }), { requestedUrl: "https://www.stub-order-c.test/guide" }, deps));

    assert.equal(classifyCalls.length, 1, "the context-resolution failure routed through the classifier");
    assert.equal(classifyTuneSetupAborted[0], true, "the tuneSetup poll was aborted before the page was classified");
  });

  test("starts a postGateReload poll after an accepted embed gate and aborts it once the second wait settles", async (t) => {

    /* Traced path: the gate branch of the video-wait race. The first video wait pends until it is abandoned; invoking the recorded videoWait resolver makes the gate
     * win the race, which aborts that wait, reloads the page, and runs the postGateReload span (context resolution + a second wait) under its own poll. The second
     * wait rejects here, so the span settles and the guard's finally aborts the postGateReload poll without depending on the fullscreen/ensurePlayback path. The
     * not-aborted-at-call / aborted-after-settle pair ties the abort to the span boundary rather than a premature or missing abort.
     */
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });
    t.mock.method(LOG, "warn", () => { /* Silenced. */ });
    t.mock.method(LOG, "info", () => { /* Silenced. */ });

    mockSelectResult = { success: true };

    let reloadCount = 0;

    const page = {

      evaluate: async (): Promise<unknown> => undefined,
      isClosed: (): boolean => false,
      reload: async (): Promise<unknown> => {

        reloadCount++;

        return undefined;
      },
      url: (): string => "https://www.stub-pg.test/landed",
      waitForSelector: async (_selector: string, options?: { signal?: AbortSignal }): Promise<unknown> => {

        // The first video wait carries the wait-abandonment signal; it pends until the accepted gate aborts it. The post-reload wait carries no signal and rejects,
        // settling the postGateReload span so its poll is aborted.
        if(options?.signal) {

          const abandoned = Promise.withResolvers<never>();

          options.signal.addEventListener("abort", () => { abandoned.reject(new Error("first video wait abandoned")); }, { once: true });

          return abandoned.promise;
        }

        throw new Error("post-reload readiness rejected");
      }
    } as unknown as Page;

    const pending = initializePlayback(page, makeProfile(), {}, deps);

    // Let the tune advance through the tuneSetup span and launch the videoWait poll, then make the gate win the race.
    for(let i = 0; (i < 50) && !overlayCalls.some((call) => call.phase === "videoWait"); i++) {

      // eslint-disable-next-line no-await-in-loop
      await immediate();
    }

    const videoWaitCall = overlayCalls.find((call) => call.phase === "videoWait");

    assert.ok(videoWaitCall?.onEmbedGateAccepted, "the videoWait poll exposed an embed-gate resolver");

    videoWaitCall.onEmbedGateAccepted();

    await assert.rejects(pending);

    assert.equal(reloadCount, 1, "the accepted gate triggered exactly one reload");

    const third = overlayCalls[2];

    assert.ok(third, "a third overlay poll was launched after the reload");
    assert.equal(third.phase, "postGateReload", "the post-reload span runs under the postGateReload phase");
    assert.equal(third.abortedAtCall, false, "the postGateReload poll starts un-aborted");
    assert.equal(third.signal?.aborted, true, "the postGateReload poll is aborted once the second wait settles");
  });
});
