/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.establishment.test.ts: Unit tests for the shared channel-establishment composition - establishmentBudgetMs, computeDirectTuneKind,
 * establishChannelPlayback, and adjudicateChannelSelection - the pieces the tune path and the native token-refresh capability both run. What these rows pin is
 * the choreography rather than any one step's mechanics: which step runs before which, whether the settlement hook fires and how many times, and that the
 * interception is finalized before anything reads its promise. establishChannelPlayback composes on the browser boundary through its EstablishChannelPlaybackDeps
 * collaborator, so playback initialization is a recording stub and the page is a bare reference no stub ever dereferences - no Chrome, no CDP, no real timers.
 *
 * Honest bounds, stated rather than masked. The timeout-lapse choreography is not driven here: the playback bound is a module-private 45-second constant by
 * design, and the bounded-wait primitive owns its own suite under utils, so a row reaching for the lapse would either wait out real time or re-implement the
 * primitive. The bodies that consume the composition (the tune's capture phase and the capability's recovery frame) stay Chrome-entangled and are covered
 * structurally elsewhere, so what is asserted here is the composition's own contract, not their integrated behavior.
 */
import type { ManifestInterceptionResult, ManifestInterceptorHandle } from "../browser/manifestInterceptor.ts";
import type { Nullable, TuneResult } from "../types/index.ts";
import { adjudicateChannelSelection, computeDirectTuneKind, establishChannelPlayback, establishmentBudgetMs } from "./setup.ts";
import { assertNoUnhandledRejections, closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { describe, test } from "node:test";
import type { EstablishChannelPlaybackDeps } from "./setup.ts";
import type { InitializePlaybackOptions } from "../browser/video.ts";
import type { Page } from "puppeteer-core";
import type { ResolvedSiteProfile } from "../types/index.ts";
import assert from "node:assert/strict";
import { makeProfile } from "../config/profiles.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* A Fox master manifest URL in the shape the live provider verifier decodes: the call sign sits in a fixed segment, so this URL reads as "fnc". The verifier-
 * bearing adjudication rows drive both directions off this one fixture by varying only the selector they compare it against.
 */
const FOX_MASTER_URL = "https://cdn.example.test/abc123/prod/fnc-ue2/index.m3u8";

// The tune result the recording initializer resolves with unless a row supplies its own. The context is the only field the composition passes through, and it is
// never dereferenced here.
const TUNE: TuneResult = { context: {} as unknown as Page };

// Yields one macrotask, so a settlement chain that was going to fire has fired before a row reads its recorder, and any unhandledRejection event has been emitted
// before assertNoUnhandledRejections's cleanup reads its capture buffer.
async function flushMacro(): Promise<void> {

  await new Promise<void>((resolve) => {

    setImmediate(resolve);
  });
}

// A bare page reference. The composition hands the page straight to the injected initializer and reads no property off it, so an empty object cast through
// unknown is a faithful stub; each call produces a distinct identity, which is what the settlement hook's closure row needs.
function makePage(): Page {

  return {} as unknown as Page;
}

/* A recording stand-in for the interceptor handle. Every method the composition or the adjudication stage can reach appends to the shared step log, so a row
 * reads the whole sequence out of one array rather than correlating separate counters. then() is instrumented on the promise, which is what lets a row prove
 * whether the interception was consulted at all and, where it was, that the read landed after the finalize fired.
 */
function makeHandle(steps: string[], interception: Nullable<ManifestInterceptionResult> = null): ManifestInterceptorHandle {

  const settled = Promise.resolve(interception);

  const promise = {

    then: (onFulfilled?: ((value: Nullable<ManifestInterceptionResult>) => unknown) | null,
      onRejected?: ((reason: unknown) => unknown) | null): Promise<unknown> => {

      steps.push("promise-read");

      return settled.then(onFulfilled, onRejected);
    }
  } as Promise<Nullable<ManifestInterceptionResult>>;

  const handle: ManifestInterceptorHandle = {

    dispose: (): void => {

      steps.push("dispose");
    },
    finalize: (directTune: boolean): void => {

      steps.push("finalize:" + String(directTune));
    },
    markChannelSelectionStart: (): void => {

      steps.push("epoch");
    },
    promise,
    [Symbol.dispose]: (): void => {

      steps.push("dispose");
    }
  };

  return handle;
}

// What a row asks of the recording playback initializer below.
interface RecordingInitOptions {

  // The error the initializer throws instead of resolving, so a row can drive the failure polarity through the same path the success rows take.
  failure?: Error;

  // Appended with exactly the options object each call received. It admits undefined because the primitive's own third parameter is optional, and recording what
  // actually arrived is what would surface a composition that stopped forwarding the caller's options - a recorder substituting an empty object would absorb it.
  initCalls?: (InitializePlaybackOptions | undefined)[];

  // The shared step log the initializer appends to.
  steps: string[];

  // The result the initializer resolves with when no failure is supplied.
  tune?: TuneResult;
}

/* A recording playback initializer. Each call appends its step, records the options it received, and then settles the way the row asked. Throwing from inside the
 * stub, rather than handing in an already-rejected promise, keeps the rejection unobservable until the composition attaches to it - so the row that pins the
 * absence of an unhandled rejection is testing the composition rather than the fixture's own timing.
 */
function makeDeps(options: RecordingInitOptions): EstablishChannelPlaybackDeps {

  return {

    initializePlayback: async (_page: Page, _profile: ResolvedSiteProfile, initOptions?: InitializePlaybackOptions): Promise<TuneResult> => {

      options.steps.push("initialize");
      options.initCalls?.push(initOptions);

      if(options.failure) {

        throw options.failure;
      }

      return options.tune ?? TUNE;
    }
  };
}

describe("establishChannelPlayback", () => {

  test("navigates, then stamps the observation epoch, then initializes playback", async () => {

    /* Traced path: the composition awaits the caller's navigate before touching the handle, and stamps before it invokes the initializer. The step log is the
     * whole assertion - a regression that stamped ahead of navigation (so a guide page's auto-played default landed inside the epoch) or initialized ahead of the
     * stamp (so the tune's own manifests landed outside it) reorders this array rather than changing any count.
     */
    const steps: string[] = [];
    const initCalls: (InitializePlaybackOptions | undefined)[] = [];
    const result = await establishChannelPlayback(makePage(), makeProfile(), makeHandle(steps), {

      initOptions: { requestedUrl: "https://example.test/watch", skipChannelSelection: true },
      navigate: async (): Promise<void> => {

        steps.push("navigate");
      }
    }, makeDeps({ initCalls, steps }));

    assert.deepEqual(steps, [ "navigate", "epoch", "initialize" ], "the sequence runs navigate, then the epoch stamp, then playback initialization");
    assert.deepEqual(initCalls, [{ requestedUrl: "https://example.test/watch", skipChannelSelection: true }],
      "the caller's initialization options reach the primitive unchanged");
    assert.equal(result, TUNE, "the composition returns the initialization's own result");
  });

  test("skips the epoch stamp for a null handle and still initializes playback", async () => {

    // Traced path: a tab replacement installs no interceptor, so the handle is null. Nothing is observing, so there is no epoch to stamp - but navigation and
    // initialization must still run, and the result must still be the initialization's. This is the arrangement a non-null-safe stamp would throw on.
    const steps: string[] = [];
    const result = await establishChannelPlayback(makePage(), makeProfile(), null, {

      initOptions: {},
      navigate: async (): Promise<void> => {

        steps.push("navigate");
      }
    }, makeDeps({ steps }));

    assert.deepEqual(steps, [ "navigate", "initialize" ], "no epoch is stamped when nothing is observing");
    assert.equal(result, TUNE, "the result is the initialization's, unaffected by the missing interception");
  });

  test("runs the hookless path with no settlement step appended", async () => {

    // Traced path: the tune path supplies no hook, so the settlement chain must not be built. This is the control for the hook rows below - the same arrangement
    // with the hook omitted - and the macrotask yield is what gives a chain that was built anyway the chance to append before the log is read.
    const steps: string[] = [];
    const result = await establishChannelPlayback(makePage(), makeProfile(), makeHandle(steps), {

      initOptions: {},
      navigate: async (): Promise<void> => {

        steps.push("navigate");
      }
    }, makeDeps({ steps }));

    await flushMacro();

    assert.deepEqual(steps, [ "navigate", "epoch", "initialize" ], "nothing appends after the initialization when no hook was supplied");
    assert.equal(result, TUNE, "a resolving initialization still returns its result");
  });

  test("fires the settlement hook exactly once after a resolving initialization", async () => {

    // Traced path: the refresh path's re-mute rides this hook, so it has to fire after the initialization settles and it has to fire once. Recording the hook
    // into the same step log is what pins the ordering; the length check is what would catch a chain that attached twice.
    const steps: string[] = [];
    const result = await establishChannelPlayback(makePage(), makeProfile(), makeHandle(steps), {

      initOptions: {},
      navigate: async (): Promise<void> => {

        steps.push("navigate");
      },
      onInitSettled: (): void => {

        steps.push("settled");
      }
    }, makeDeps({ steps }));

    await flushMacro();

    assert.deepEqual(steps, [ "navigate", "epoch", "initialize", "settled" ], "the hook fires after the initialization, not before it");
    assert.equal(steps.filter((step) => step === "settled").length, 1, "the hook fires exactly once");
    assert.equal(result, TUNE, "the success path's result is untouched by the hook");
  });

  test("fires the settlement hook and rejects with the initialization's error, leaving no unhandled rejection", async () => {

    /* Traced path: the failing polarity of the hook. A failed initialization still needs the hook (the refresh path's mute must be restored on failure too), the
     * caller still needs the original error, and the settlement chain the hook rides must not become a second, unowned rejection - the leak class this rig
     * exists for. The macrotask yield before the restore is what gives Node's unhandledRejection event a chance to have been emitted; a microtask-only tick
     * cannot observe it, so a rig without this yield would pass on a real leak.
     */
    const restore = assertNoUnhandledRejections();
    const steps: string[] = [];
    const failure = new Error("channel selection never completed");
    const settled = establishChannelPlayback(makePage(), makeProfile(), makeHandle(steps), {

      initOptions: {},
      navigate: async (): Promise<void> => {

        steps.push("navigate");
      },
      onInitSettled: (): void => {

        steps.push("settled");
      }
    }, makeDeps({ failure, steps }));

    await assert.rejects(() => settled, (error: unknown): boolean => error === failure);

    await flushMacro();

    assert.deepEqual(steps, [ "navigate", "epoch", "initialize", "settled" ], "the hook fires on the failure path as well");

    restore();
  });

  test("propagates a navigation failure without initializing playback", async () => {

    // Traced path: the failure polarity of the order pin. Navigation is awaited first, so its rejection has to leave the epoch unstamped and the initializer
    // uninvoked - an establishment that initialized on a page that never loaded would tune against whatever was still on screen.
    const steps: string[] = [];
    const failure = new Error("page navigation for https://example.test/watch failed");

    await assert.rejects(() => establishChannelPlayback(makePage(), makeProfile(), makeHandle(steps), {

      initOptions: {},
      navigate: async (): Promise<void> => {

        steps.push("navigate");

        throw failure;
      }
    }, makeDeps({ steps })), (error: unknown): boolean => error === failure);

    assert.deepEqual(steps, ["navigate"], "neither the epoch stamp nor playback initialization runs after a navigation failure");
  });
});

describe("computeDirectTuneKind", () => {

  test("reports direct for a cached direct URL even on a channel-selection profile", () => {

    // Traced path: the tune path's cached-direct-URL route. The profile does have a selection step, so this row fails for any formula that dropped the
    // usedDirectUrl term and inferred the kind from the profile alone.
    assert.equal(computeDirectTuneKind({ profile: makeProfile({ channelSelector: "FNC" }), strategyDirectTune: false, usedDirectUrl: true }), true);
  });

  test("reports direct when the strategy itself resolved a direct tune", () => {

    // Traced path: an API-interception tune on a selection profile - the strategy tuned without a DOM click, so the interception must be finalized as direct even
    // though the route was the guide URL rather than a cached watch URL.
    assert.equal(computeDirectTuneKind({ profile: makeProfile({ channelSelector: "FNC" }), strategyDirectTune: true, usedDirectUrl: false }), true);
  });

  test("reports direct for a profile with no channel-selection step, with usedDirectUrl omitted", () => {

    // Traced path: the refresh path's shape - it never takes a cached direct URL, so it omits the term entirely and the kind rests on the profile. A single-
    // channel site navigated by URL has nothing to select, so its first manifest is by definition the right one.
    assert.equal(computeDirectTuneKind({ profile: makeProfile({ channelSelector: null }), strategyDirectTune: false }), true);
  });

  test("reports not-direct on a selection profile with the omitted term and no strategy direct tune", () => {

    // Traced path: the refresh path's ordinary case - a guide-based site, a click-verified route, no direct term to report. The absent usedDirectUrl must default
    // to false rather than to undefined leaking through as truthy.
    assert.equal(computeDirectTuneKind({ profile: makeProfile({ channelSelector: "FNC" }), strategyDirectTune: false }), false);
  });
});

describe("adjudicateChannelSelection", () => {

  test("finalizes once with the caller's kind and returns null without consulting the interception when nothing can verify", async () => {

    /* Traced path: the vacuous half. guideGrid is a registered provider that implements no verifier, so verification returns before it awaits anything - which
     * makes this row a pin on the finalize and on the absence of an await together: the finalize ran exactly once carrying the boolean it was handed, and the
     * interception promise was never read at all.
     * It deliberately claims nothing about the awaiting path; the verifier-bearing row below covers that.
     */
    const steps: string[] = [];
    const handle = makeHandle(steps, { manifestUrl: FOX_MASTER_URL, selectedKind: "master" });
    const result = await adjudicateChannelSelection(handle, makeProfile({ channelSelection: { strategy: "guideGrid" }, channelSelector: "FNC" }), true);

    assert.equal(result, null, "a stream with nothing to verify raises no objection");
    assert.deepEqual(steps, ["finalize:true"], "the interception is finalized with the caller's kind and never awaited");
  });

  test("finalizes before reading the interception and passes the live Fox verifier's acceptance through", async () => {

    // Traced path: the genuinely-awaiting half. A Fox-strategy profile with a selector admits the verifier, the master-kind interception admits the URL check,
    // and the provider decodes "fnc" from the URL to match the selector. The step order is the claim: the read of the promise lands after the finalize fired.
    const steps: string[] = [];
    const handle = makeHandle(steps, { manifestUrl: FOX_MASTER_URL, selectedKind: "master" });
    const result = await adjudicateChannelSelection(handle, makeProfile({ channelSelection: { strategy: "foxGrid" }, channelSelector: "FNC" }), false);

    assert.equal(result, null, "the verifier accepts a master URL for the requested call sign");
    assert.deepEqual(steps, [ "finalize:false", "promise-read" ], "the interception is finalized first, then read");
  });

  test("returns the live Fox verifier's reason when the master URL belongs to a different call sign", async () => {

    // Traced path: the same fixture against a different selector, so the reason string is the provider's own, returned through both stages unchanged. This is
    // what makes the pair a pass-through proof rather than a second acceptance row.
    const steps: string[] = [];
    const handle = makeHandle(steps, { manifestUrl: FOX_MASTER_URL, selectedKind: "master" });
    const result = await adjudicateChannelSelection(handle, makeProfile({ channelSelection: { strategy: "foxGrid" }, channelSelector: "FS1" }), false);

    assert.equal(result, "Manifest URL is for channel \"fnc\", but \"FS1\" was requested.", "the verifier's reason reaches the caller unchanged");
    assert.deepEqual(steps, [ "finalize:false", "promise-read" ], "a rejecting verdict reads the promise on the same ordering");
  });
});

describe("establishmentBudgetMs", () => {

  test("adds the playback bound, the finalize settle, and the margin to the caller's navigation allowance", () => {

    // A conscious pin of the budget rule: 45000 for the playback bound, 1500 for the settle the interceptor waits out, and 5000 of margin. A term that changed
    // without this row changing with it would silently shrink or stretch every establishment's observation window.
    assert.equal(establishmentBudgetMs(10000), 61500);
    assert.equal(establishmentBudgetMs(0), 51500, "the fixed terms are the floor for a caller with no navigation allowance");
  });
});
