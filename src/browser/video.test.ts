/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * video.test.ts: Unit tests for the testable helpers in video.ts. The module's broader API drives Puppeteer pages through page.click, page.waitForSelector,
 * page.goto, and the fullscreen sequence - those paths are deferred to e2e. The unit tests here cover buildVideoSelectorType (a pure profile->string mapper), the
 * evaluate-backed helpers that route through evaluateWithAbort with no other browser interaction (getVideoState, enforceVideoVolume, validateVideoElement,
 * checkVideoPresence, reloadVideoSource, startVideoPlayback, applyVideoStyles, lockVolumeProperties, verifyFullscreen), and the injection helpers that register their
 * in-page scripts via page.evaluateOnNewDocument (injectVideoSelector, and suppressPageAudio, which also runs an immediate page.evaluate to mute existing videos).
 * Each helper is exercised against a stub context whose evaluate() returns a configurable value or throws, so the helper's argument-passing and result-handling
 * contracts are locked without spinning up Chrome.
 */
import type { Frame, Page } from "puppeteer-core";
import { applyVideoStyles, buildVideoSelectorType, checkVideoPresence, enforceVideoVolume, getVideoState, injectVideoSelector, lockVolumeProperties,
  reloadVideoSource, startVideoPlayback, suppressPageAudio, validateVideoElement, verifyFullscreen } from "./video.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { makeProfile } from "../config/profiles.helpers.ts";

/* makeProfile builds a ResolvedSiteProfile literal with all required fields populated to safe defaults. Tests override only the fields they care about - typically
 * selectReadyVideo to flip the selector type. The object is intentionally minimal because video.ts only reads selectReadyVideo and a few other flags from
 * ResolvedSiteProfile during the tested paths.
 */

/* ContextStub captures the evaluate calls a helper performs so tests can assert on the arguments. The evaluateImpl callback is the test-controlled response - it
 * either returns a value (resolved by Promise.resolve) or throws.
 */
interface ContextStub {

  calls: { args: unknown[]; fn: unknown }[];
  evaluate: (fn: unknown, ...args: unknown[]) => Promise<unknown>;
}

/* makeContextStub returns a Frame/Page-shaped stub where evaluate() proxies to the supplied factory. Tests pass a factory that returns the stubbed value or throws
 * to exercise specific helper branches. The cast through unknown bypasses the broad Frame|Page surface - the helpers only call .evaluate, so nothing else needs to
 * exist.
 */
function makeContextStub(impl: (fn: unknown, ...args: unknown[]) => unknown): { context: Frame | Page; stub: ContextStub } {

  const calls: { args: unknown[]; fn: unknown }[] = [];

  const stub: ContextStub = {

    calls,
    evaluate: async (fn: unknown, ...args: unknown[]): Promise<unknown> => {

      calls.push({ args, fn });

      // The impl may throw synchronously or asynchronously - we await to surface either path as a rejection.
      return await impl(fn, ...args);
    }
  };

  return { context: stub as unknown as Frame | Page, stub };
}

/* makePageStub returns a Page-shaped stub whose evaluateOnNewDocument records its callback. Tests assert on the call count to lock the
 * injection contract without trying to execute the in-page function (which references browser-only globals).
 */
interface PageStub {

  evaluate: (fn: unknown, ...args: unknown[]) => Promise<unknown>;
  evaluateCalls: number;
  evaluateOnNewDocumentCalls: number;
}

function makePageStub(options: { evaluateImpl?: (fn: unknown, ...args: unknown[]) => unknown } = {}): { page: Page; stub: PageStub } {

  const stub: PageStub = {

    evaluate: async (fn: unknown, ...args: unknown[]): Promise<unknown> => {

      stub.evaluateCalls++;

      if(options.evaluateImpl) {

        return await options.evaluateImpl(fn, ...args);
      }

      return undefined;
    },
    evaluateCalls: 0,
    evaluateOnNewDocumentCalls: 0
  };

  const page = {

    evaluate: stub.evaluate,
    evaluateOnNewDocument: async (): Promise<{ identifier: string; type: string }> => {

      stub.evaluateOnNewDocumentCalls++;

      // Puppeteer's evaluateOnNewDocument resolves to a NewDocumentScriptEvaluation object - we return a plausible-shaped object so callers that ignore the result
      // are unaffected.
      return { identifier: "stub", type: "stub" };
    }
  } as unknown as Page;

  return { page, stub };
}

describe("buildVideoSelectorType", () => {

  test("returns 'selectReadyVideo' when the profile has selectReadyVideo true", () => {

    assert.equal(buildVideoSelectorType(makeProfile({ selectReadyVideo: true })), "selectReadyVideo", "true flag -> selectReadyVideo");
  });

  test("returns 'selectFirstVideo' when the profile has selectReadyVideo false", () => {

    assert.equal(buildVideoSelectorType(makeProfile({ selectReadyVideo: false })), "selectFirstVideo", "false flag -> selectFirstVideo");
  });

  test("returns 'selectFirstVideo' on a default profile (the documented baseline)", () => {

    // Boundary: the default profile defaults selectReadyVideo to false, which corresponds to the standard single-video case.
    assert.equal(buildVideoSelectorType(makeProfile()), "selectFirstVideo", "default profile -> selectFirstVideo");
  });
});

describe("getVideoState", () => {

  test("returns the value the in-page function resolved to (passes through unchanged)", async () => {

    const stateValue = {

      currentTime: 12.5,
      ended: false,
      error: false,
      muted: false,
      networkState: 2,
      paused: false,
      readyState: 4,
      videoHeight: 720,
      videoWidth: 1280,
      volume: 1
    };

    const { context, stub } = makeContextStub(() => stateValue);

    const result = await getVideoState(context, "selectFirstVideo");

    assert.deepEqual(result, stateValue, "evaluate result surfaces verbatim");
    assert.equal(stub.calls.length, 1, "evaluate called exactly once");
    assert.deepEqual(stub.calls[0]?.args, ["selectFirstVideo"], "selector type passed through to in-page function");
  });

  test("returns null when the in-page function reports no video element", async () => {

    // Boundary: when the page has no video, the in-page function returns null. The wrapper passes that through.
    const { context } = makeContextStub(() => null);

    assert.equal(await getVideoState(context, "selectReadyVideo"), null, "null surfaces");
  });

  test("forwards the selectReadyVideo selector type as the only argument", async () => {

    const { context, stub } = makeContextStub(() => null);

    await getVideoState(context, "selectReadyVideo");

    assert.deepEqual(stub.calls[0]?.args, ["selectReadyVideo"], "selectReadyVideo argument forwarded verbatim");
  });
});

describe("enforceVideoVolume", () => {

  test("invokes evaluate with the selector type and resolves without a value (void return)", async () => {

    const { context, stub } = makeContextStub(() => undefined);

    await enforceVideoVolume(context, "selectFirstVideo");

    assert.equal(stub.calls.length, 1, "evaluate called exactly once");
    assert.deepEqual(stub.calls[0]?.args, ["selectFirstVideo"], "selector type forwarded");
  });

  test("propagates errors thrown by evaluate (no internal swallow)", async () => {

    // enforceVideoVolume does not wrap evaluate in a try/catch - errors bubble. This locks the contract that callers handle errors at the boundary.
    const { context } = makeContextStub(() => {

      throw new Error("synthetic evaluate failure");
    });

    await assert.rejects(() => enforceVideoVolume(context, "selectReadyVideo"), /synthetic evaluate failure/, "error propagates");
  });
});

describe("validateVideoElement", () => {

  test("returns the validation result the in-page function resolved with (found case)", async () => {

    const { context } = makeContextStub(() => ({ found: true, readyState: 4 }));

    assert.deepEqual(await validateVideoElement(context, "selectFirstVideo"), { found: true, readyState: 4 }, "found result surfaces");
  });

  test("returns { found: false } when no video element exists", async () => {

    const { context } = makeContextStub(() => ({ found: false }));

    assert.deepEqual(await validateVideoElement(context, "selectFirstVideo"), { found: false }, "not-found result surfaces");
  });

  test("forwards the selector type to the in-page function", async () => {

    const { context, stub } = makeContextStub(() => ({ found: true, readyState: 3 }));

    await validateVideoElement(context, "selectReadyVideo");

    assert.deepEqual(stub.calls[0]?.args, ["selectReadyVideo"], "selector type forwarded");
  });
});

describe("checkVideoPresence", () => {

  test("returns the in-page function's full presence result for the no-video case", async () => {

    // Boundary: when the page has no video elements at all, the helper reports a zero-count result. We lock the field shape.
    const { context } = makeContextStub(() => ({ anyVideoExists: false, readyVideoFound: false, videoCount: 0 }));

    const result = await checkVideoPresence(context, "selectFirstVideo");

    assert.deepEqual(result, { anyVideoExists: false, readyVideoFound: false, videoCount: 0 }, "zero-count shape surfaces");
  });

  test("returns the in-page function's full presence result for the ready-video case", async () => {

    const { context } = makeContextStub(() => ({ anyVideoExists: true, maxReadyState: 4, readyVideoFound: true, videoCount: 2 }));

    const result = await checkVideoPresence(context, "selectReadyVideo");

    assert.deepEqual(result, { anyVideoExists: true, maxReadyState: 4, readyVideoFound: true, videoCount: 2 }, "ready-video shape surfaces");
  });

  test("forwards the selector type so the in-page function knows whether to require readyState >= 3", async () => {

    const { context, stub } = makeContextStub(() => ({ anyVideoExists: false, readyVideoFound: false, videoCount: 0 }));

    await checkVideoPresence(context, "selectReadyVideo");

    assert.deepEqual(stub.calls[0]?.args, ["selectReadyVideo"], "selectReadyVideo forwarded");
  });
});

describe("reloadVideoSource", () => {

  test("invokes evaluate with the selector type once", async () => {

    const { context, stub } = makeContextStub(() => undefined);

    await reloadVideoSource(context, "selectFirstVideo");

    assert.equal(stub.calls.length, 1, "evaluate called once");
    assert.deepEqual(stub.calls[0]?.args, ["selectFirstVideo"], "selector type forwarded");
  });

  test("propagates evaluate failures (no internal swallow)", async () => {

    const { context } = makeContextStub(() => {

      throw new Error("synthetic reload failure");
    });

    await assert.rejects(() => reloadVideoSource(context, "selectFirstVideo"), /synthetic reload failure/, "error propagates");
  });
});

describe("startVideoPlayback", () => {

  test("invokes evaluate with the selector type once", async () => {

    const { context, stub } = makeContextStub(() => undefined);

    await startVideoPlayback(context, "selectReadyVideo");

    assert.equal(stub.calls.length, 1, "evaluate called once");
    assert.deepEqual(stub.calls[0]?.args, ["selectReadyVideo"], "selector type forwarded");
  });
});

describe("applyVideoStyles", () => {

  test("forwards both the selector type and the important flag to evaluate (default important=false)", async () => {

    const { context, stub } = makeContextStub(() => undefined);

    await applyVideoStyles(context, "selectFirstVideo");

    assert.deepEqual(stub.calls[0]?.args, [ "selectFirstVideo", false ], "default important is false");
  });

  test("forwards important=true when the caller requests aggressive styling", async () => {

    const { context, stub } = makeContextStub(() => undefined);

    await applyVideoStyles(context, "selectReadyVideo", true);

    assert.deepEqual(stub.calls[0]?.args, [ "selectReadyVideo", true ], "important flag forwarded");
  });
});

describe("lockVolumeProperties", () => {

  test("invokes evaluate once with the selector type on the happy path", async () => {

    const { context, stub } = makeContextStub(() => undefined);

    await lockVolumeProperties(context, "selectFirstVideo");

    assert.equal(stub.calls.length, 1, "evaluate called once");
    assert.deepEqual(stub.calls[0]?.args, ["selectFirstVideo"], "selector type forwarded");
  });

  test("absorbs evaluate errors silently (the warn-and-continue contract)", async () => {

    // Negative test: lockVolumeProperties is non-critical to stream function. Errors are logged at warn level but the helper resolves normally so the caller can
    // continue. We assert no rejection escapes.
    const { context } = makeContextStub(() => {

      throw new Error("synthetic lock failure");
    });

    await assert.doesNotReject(() => lockVolumeProperties(context, "selectFirstVideo"), "errors must not escape");
  });
});

describe("verifyFullscreen", () => {

  test("returns the boolean result reported by evaluate", async () => {

    const { context: trueContext } = makeContextStub(() => true);

    assert.equal(await verifyFullscreen(trueContext, "selectFirstVideo"), true, "true surfaces");

    const { context: falseContext } = makeContextStub(() => false);

    assert.equal(await verifyFullscreen(falseContext, "selectFirstVideo"), false, "false surfaces");
  });

  test("returns null when evaluate throws (the inconclusive-check contract)", async () => {

    // Boundary: the helper returns null rather than rethrowing or returning false when evaluate fails. This lets monitor code distinguish between "fullscreen
    // verified false" and "could not verify" to avoid a false-positive layout-changed signal.
    const { context } = makeContextStub(() => {

      throw new Error("synthetic verify failure");
    });

    assert.equal(await verifyFullscreen(context, "selectReadyVideo"), null, "evaluate failure -> null");
  });

  test("forwards the selector type to evaluate", async () => {

    const { context, stub } = makeContextStub(() => true);

    await verifyFullscreen(context, "selectReadyVideo");

    assert.deepEqual(stub.calls[0]?.args, ["selectReadyVideo"], "selector type forwarded");
  });
});

describe("injectVideoSelector", () => {

  test("registers exactly one evaluateOnNewDocument call (the persistent injection)", async () => {

    const { page, stub } = makePageStub();

    await injectVideoSelector(page);

    assert.equal(stub.evaluateOnNewDocumentCalls, 1, "evaluateOnNewDocument called exactly once");
    assert.equal(stub.evaluateCalls, 0, "no immediate evaluate call - injection runs on the next navigation");
  });

  test("a second call results in a second registration (idempotency is not enforced at this layer)", async () => {

    // Boundary: the helper does not gate on a registered flag - duplicate calls register duplicate handlers, so the injected script runs twice on each new document.
    // Each run merely reassigns the same window.__prismcastSelectVideo global, which is safe to call more than once, so the net effect is a no-op; we lock the
    // surface contract here.
    const { page, stub } = makePageStub();

    await injectVideoSelector(page);
    await injectVideoSelector(page);

    assert.equal(stub.evaluateOnNewDocumentCalls, 2, "second call adds a second registration");
  });
});

describe("suppressPageAudio", () => {

  test("registers an evaluateOnNewDocument hook and runs an immediate evaluate to mute current videos", async () => {

    const { page, stub } = makePageStub();

    await suppressPageAudio(page);

    assert.equal(stub.evaluateOnNewDocumentCalls, 1, "prototype override registered for future plays");
    assert.equal(stub.evaluateCalls, 1, "immediate evaluate runs to mute already-playing videos");
  });

  test("absorbs evaluate errors silently (best-effort current-video mute)", async () => {

    // Negative test: the immediate evaluate is wrapped in try/catch. An error here must not abort the suppression - the prototype override has already been
    // registered and will catch future play() calls. We verify by passing an evaluateImpl that throws.
    const { page, stub } = makePageStub({

      evaluateImpl: (): never => {

        throw new Error("synthetic mute failure");
      }
    });

    await assert.doesNotReject(() => suppressPageAudio(page), "errors must not escape");
    assert.equal(stub.evaluateOnNewDocumentCalls, 1, "registration still completed");
  });
});
