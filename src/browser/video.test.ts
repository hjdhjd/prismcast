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
import { seedVideoSelector, withDocument } from "../testing.helpers.ts";
import type { Window } from "happy-dom";
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

/* The stacking fixture the applyVideoStyles DOM rows run against. A site header ranked high at the root sits outside the video's chain, and the chain itself mixes
 * the positioning cases the ancestor walk has to handle: a positioned container carrying a modest z-index, an absolutely positioned wrapper, and two static
 * elements. The positions are declared inline because happy-dom reports an unspecified position as the empty string rather than "static", and the walk's static
 * test is what the fixture exists to exercise.
 */
const STACKING_FIXTURE = "<header id=\"h\" style=\"position:relative;z-index:99998\"></header>" +
  "<main id=\"m\" style=\"position:static\"><div id=\"c\" style=\"position:relative;z-index:100\">" +
  "<div id=\"w\" style=\"position:absolute\"><div id=\"p\" style=\"position:static\"><video id=\"v\"></video></div></div></div></main>";

/* The box properties the styling neutralizes beyond the margin and transform pair the rows below read by name: the maxes a stylesheet clamps the box with on
 * either axis, and the independent transform properties that move the box exactly as a transform does. The rows read them through getPropertyValue and
 * getPropertyPriority because the CSSOM view happy-dom builds carries no camelCase accessor for rotate, scale, or translate.
 */
const NEUTRALIZED_BOX_PROPERTIES = [ "max-height", "max-width", "rotate", "scale", "translate" ];

/* Reads a fixture element's inline style declaration by id. A missing id is a broken fixture rather than a failed expectation, so it throws with the id named
 * instead of surfacing further down as a property read on null.
 * @param window - The window backing the fixture document.
 * @param id - The fixture element's id.
 * @returns The element's inline style declaration.
 */
function styleOf(window: Window, id: string): CSSStyleDeclaration {

  const element = window.document.getElementById(id);

  if(!element) {

    throw new Error("The fixture has no element with id " + id + ".");
  }

  return (element as unknown as HTMLElement).style;
}

/* The two in-page callbacks the DOM rows execute: the styling pass, which takes the selector type and the priority flag, and the verification, which takes the
 * selector type and answers whether the video fills the viewport.
 */
type StyleCallback = (type: string, useImportant: boolean) => void;
type VerifyCallback = (type: string) => boolean;

/* Runs an evaluate-backed helper against a stub context, then returns the in-page callback it handed to evaluate. The DOM rows below invoke that callback directly
 * against a synthetic document, which is what lets them exercise page-side behavior without a browser. One extractor serves every such helper: the row makes the
 * call it wants and names the callback's signature as the type argument.
 * @param invoke - Calls the helper under test with the stub context, carrying whatever arguments the row cares about.
 * @returns The callback the helper handed to evaluate.
 */
async function captureCallback<T>(invoke: (context: Frame | Page) => Promise<unknown>): Promise<T> {

  const { context, stub } = makeContextStub(() => undefined);

  await invoke(context);

  return stub.calls[0]?.fn as T;
}

/* Runs the verification callback against a video whose box is the given rect, in a 1920x1080 viewport. The viewport is set explicitly because happy-dom's bare
 * window is 1024x768, at which a box shifted half a screen would still measure as filling and the rows would assert nothing. The video is a stand-in rather than a
 * fixture element: the callback calls getBoundingClientRect and nothing else, so scripting that one method is what lets a row state a box no synthetic layout
 * engine would produce.
 * @param callback - The captured verification callback.
 * @param rect - The video's viewport-relative box.
 * @returns What the verification answered for that box.
 */
function verifyWithRect(callback: VerifyCallback, rect: { height: number; left: number; top: number; width: number }): boolean {

  return withDocument("", (window) => {

    window.innerHeight = 1080;
    window.innerWidth = 1920;

    seedVideoSelector(window, { getBoundingClientRect: (): typeof rect => rect });

    return callback("selectFirstVideo");
  });
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

  test("lifts every ancestor up to the body into the video's layer, positioning the static ones", async () => {

    /* The defect this asserts: a z-index ranks an element only inside its nearest stacking context, so the video's own high z-index is capped by whatever its
     * positioned container ranks at, and a site header above that container paints over the capture. Lifting each ancestor to the video's layer within its own
     * context is what makes the styling's promise hold. The header is outside the chain and must be left exactly as authored.
     */
    const callback = await captureCallback<StyleCallback>((context) => applyVideoStyles(context, "selectFirstVideo", false));

    withDocument(STACKING_FIXTURE, (window) => {

      seedVideoSelector(window, window.document.getElementById("v"));

      callback("selectFirstVideo", false);

      assert.equal(styleOf(window, "c").position, "relative", "an already-positioned container keeps its position");
      assert.equal(styleOf(window, "c").zIndex, "999000", "the container is lifted out of its modest rank");
      assert.equal(styleOf(window, "p").position, "relative", "a static parent is positioned so a z-index applies to it");
      assert.equal(styleOf(window, "p").zIndex, "999000", "the static parent is lifted");
      assert.equal(styleOf(window, "w").position, "absolute", "an absolutely positioned wrapper keeps its position");
      assert.equal(styleOf(window, "w").zIndex, "999000", "the wrapper is lifted");
      assert.equal(styleOf(window, "m").position, "relative", "the walk reaches every ancestor, not just the parent");
      assert.equal(styleOf(window, "m").zIndex, "999000", "the outermost ancestor below the body is lifted");
      assert.equal(styleOf(window, "h").zIndex, "99998", "an element outside the video's chain is untouched");
      assert.equal(styleOf(window, "h").position, "relative", "the element outside the chain keeps its position too");
      assert.equal(window.document.body.style.zIndex, "", "the walk stops at the body, where the root context decides the order");
      assert.equal(styleOf(window, "v").zIndex, "999000", "the video carries the same layer as its ancestors");
    });
  });

  test("clears every property that moves or clamps the video's own box, and leaves its ancestors' transforms alone", async () => {

    /* The field failures this catches: a player centers its video with its own translate, which slides the anchored full-viewport box off-center and leaves the
     * capture compositing a corner of the frame, and a stylesheet max holds the box to a fraction of the viewport without ever contesting the width and height
     * the styling sets. Positioning alone answers neither, so the styling neutralizes the video's geometry and motion outright. The ancestors are the boundary -
     * the standard tier deliberately does not reach into their transforms, which is the aggressive tier's business.
     */
    const callback = await captureCallback<StyleCallback>((context) => applyVideoStyles(context, "selectFirstVideo", false));

    withDocument(STACKING_FIXTURE, (window) => {

      seedVideoSelector(window, window.document.getElementById("v"));

      callback("selectFirstVideo", false);

      assert.equal(styleOf(window, "v").transform, "none", "a site's centering translate cannot survive the styling pass");
      assert.equal(styleOf(window, "v").transition, "none", "and no transition is left to animate the clear into the verification window");
      assert.equal(styleOf(window, "v").margin, "0px", "a negative margin cannot shift the anchored box either");

      for(const property of NEUTRALIZED_BOX_PROPERTIES) {

        assert.equal(styleOf(window, "v").getPropertyValue(property), "none", property + " is cleared on the video");
      }

      assert.equal(styleOf(window, "c").transform, "", "the container's transform is left where the page put it");
      assert.equal(styleOf(window, "p").transform, "", "and so is the static parent's");
    });
  });

  test("leaves an ancestor that already ranks at or above the layer alone", async () => {

    /* The aggressive fullscreen path ranks ancestors at 999998, above this layer. A standard styling pass runs after it on every reinforcement tick, and lowering
     * those ancestors would undo the stronger path's work. The static parent in the same fixture shows the walk still lifts everything else.
     */
    const callback = await captureCallback<StyleCallback>((context) => applyVideoStyles(context, "selectFirstVideo", false));

    const fixture = "<div id=\"c\" style=\"position:relative;z-index:999998\"><div id=\"p\" style=\"position:static\"><video id=\"v\"></video></div></div>";

    withDocument(fixture, (window) => {

      seedVideoSelector(window, window.document.getElementById("v"));

      callback("selectFirstVideo", false);

      assert.equal(styleOf(window, "c").zIndex, "999998", "the higher tier survives a later standard pass");
      assert.equal(styleOf(window, "p").zIndex, "999000", "the unranked parent is still lifted");
    });
  });

  test("applies the lifted properties with important priority when the caller requests it", async () => {

    // The flag governs the ancestors exactly as it governs the video: a site that fights style changes wins on the ancestors otherwise, and the video's own
    // important-flagged z-index would then be capped by an ancestor the site re-ranked.
    const callback = await captureCallback<StyleCallback>((context) => applyVideoStyles(context, "selectFirstVideo", true));

    withDocument(STACKING_FIXTURE, (window) => {

      seedVideoSelector(window, window.document.getElementById("v"));

      callback("selectFirstVideo", true);

      assert.equal(styleOf(window, "c").getPropertyPriority("z-index"), "important", "the lifted z-index carries the priority");
      assert.equal(styleOf(window, "p").getPropertyPriority("position"), "important", "the positioning carries the priority");
      assert.equal(styleOf(window, "v").getPropertyPriority("transform"), "important", "and so does the cleared transform, which is what a site's own stylesheet " +
        "rule would otherwise outrank");

      for(const property of NEUTRALIZED_BOX_PROPERTIES) {

        assert.equal(styleOf(window, "v").getPropertyPriority(property), "important", property + " is cleared at the same priority");
      }
    });
  });

  test("applies the lifted properties without priority when the caller does not request it", async () => {

    // The other half of the flag's contract. Hardcoding the priority would override site styling on every ordinary establishment pass, which is the escalation the
    // aggressive path exists to make deliberate.
    const callback = await captureCallback<StyleCallback>((context) => applyVideoStyles(context, "selectFirstVideo", false));

    withDocument(STACKING_FIXTURE, (window) => {

      seedVideoSelector(window, window.document.getElementById("v"));

      callback("selectFirstVideo", false);

      assert.equal(styleOf(window, "c").getPropertyPriority("z-index"), "", "the lifted z-index carries no priority");
      assert.equal(styleOf(window, "p").getPropertyPriority("position"), "", "the positioning carries no priority");
      assert.equal(styleOf(window, "v").getPropertyPriority("transform"), "", "and the cleared transform carries none either");

      for(const property of NEUTRALIZED_BOX_PROPERTIES) {

        assert.equal(styleOf(window, "v").getPropertyPriority(property), "", property + " carries none either");
      }
    });
  });

  test("styles nothing when the selector finds no video", async () => {

    // The early return covers the window between navigation and the player mounting its video. Without it the callback would dereference null and throw inside the
    // page, turning a routine timing gap into an evaluate failure the monitor has to classify.
    const callback = await captureCallback<StyleCallback>((context) => applyVideoStyles(context, "selectFirstVideo", false));

    withDocument(STACKING_FIXTURE, (window) => {

      seedVideoSelector(window, null);

      assert.doesNotThrow(() => callback("selectFirstVideo", false), "the callback returns cleanly with no video");
      assert.equal(styleOf(window, "c").zIndex, "100", "the container keeps its authored rank");
      assert.equal(styleOf(window, "p").position, "static", "the static parent keeps its authored position");
    });
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

  test("passes a video whose box fills the viewport", async () => {

    // The ordinary success: styling applied, box anchored at the origin at the viewport's size. Every row below is measured against this one.
    const callback = await captureCallback<VerifyCallback>((context) => verifyFullscreen(context, "selectFirstVideo"));

    assert.equal(verifyWithRect(callback, { height: 1080, left: 0, top: 0, width: 1920 }), true, "a video filling the viewport verifies");
  });

  test("fails a full-sized video that a transform has shifted off-center", async () => {

    /* The field failure: a player centers its video with translate(-50%, -50%), so a full-viewport box sits at (-960, -540) and only its bottom-right quarter
     * lands on screen. The box still measures 1920x1080, which is why the raw dimensions cannot tell this case from the passing one above - what the capture
     * composites is the quarter, and that is what the verification has to answer for.
     */
    const callback = await captureCallback<VerifyCallback>((context) => verifyFullscreen(context, "selectFirstVideo"));

    assert.equal(verifyWithRect(callback, { height: 1080, left: -960, top: -540, width: 1920 }), false, "a shifted full-sized video does not verify");
  });

  test("fails a box that pillars itself inside the viewport", async () => {

    /* A 1440-wide box centered in a 1920 viewport covers the full height and three quarters of the width. The styling gives the video the whole viewport and
     * lets object-fit pillar the picture inside that box, so a pillared box is the styling defeated rather than a 4:3 feed rendered correctly - the content's
     * own shape never reaches this measurement, which reads the element's box and nothing else.
     */
    const callback = await captureCallback<VerifyCallback>((context) => verifyFullscreen(context, "selectFirstVideo"));

    assert.equal(verifyWithRect(callback, { height: 1080, left: 240, top: 0, width: 1440 }), false, "a pillared box does not verify");
  });

  test("fails a box that letterboxes itself inside the viewport", async () => {

    /* The height axis of the same contract, and the row that states it most directly: a 700-tall band spanning the full width covers 0.648 of the height. A box
     * that letterboxes itself has been clamped by the page, while letterboxed content inside a full box is object-fit's output and never reaches this rule.
     */
    const callback = await captureCallback<VerifyCallback>((context) => verifyFullscreen(context, "selectFirstVideo"));

    assert.equal(verifyWithRect(callback, { height: 700, left: 0, top: 190, width: 1920 }), false, "a banded box does not verify");
  });

  test("fails a box the page has halved along the width", async () => {

    /* Taken from the field: a live player halved the styled video's width, leaving a full-height box covering half the viewport. Nothing else notices - the
     * video plays, the health signals read clean, and the recording carries half a frame - so this measurement is the only place the defeat can surface.
     */
    const callback = await captureCallback<VerifyCallback>((context) => verifyFullscreen(context, "selectFirstVideo"));

    assert.equal(verifyWithRect(callback, { height: 1080, left: 0, top: 0, width: 960 }), false, "a half-width box does not verify");
  });

  test("fails a video that is merely small", async () => {

    // The mini-player reversion the monitor's reinforcement was written for: the video plays on, centered and a quarter of the size, and no other health signal
    // notices. Coverage is what fails it rather than position, so the threshold alone decides this row.
    const callback = await captureCallback<VerifyCallback>((context) => verifyFullscreen(context, "selectFirstVideo"));

    assert.equal(verifyWithRect(callback, { height: 270, left: 720, top: 405, width: 480 }), false, "a quarter-sized video does not verify");
  });

  test("fails a video parked entirely off-screen", async () => {

    // A box pushed one full viewport down and to the right contributes nothing at all. Both extents clamp to zero rather than going negative, so the ratios read
    // 0 and the check fails on a video the raw dimensions would have called fullscreen.
    const callback = await captureCallback<VerifyCallback>((context) => verifyFullscreen(context, "selectFirstVideo"));

    assert.equal(verifyWithRect(callback, { height: 1080, left: 1920, top: 1080, width: 1920 }), false, "an off-screen video does not verify");
  });

  test("fails a video pushed half off along one axis", async () => {

    /* Half the width off screen leaves a full-sized box covering half the viewport horizontally. The measurement is the same one the half-width row above makes
     * of a clamped box: what the capture composites is half a frame either way, and where the missing half went is not something the picture records.
     */
    const callback = await captureCallback<VerifyCallback>((context) => verifyFullscreen(context, "selectFirstVideo"));

    assert.equal(verifyWithRect(callback, { height: 1080, left: 960, top: 0, width: 1920 }), false, "a box half off screen does not verify");
  });
});

describe("injectVideoSelector", () => {

  test("registers exactly one evaluateOnNewDocument call (the persistent injection)", async () => {

    const { page, stub } = makePageStub();

    await injectVideoSelector(page);

    assert.equal(stub.evaluateOnNewDocumentCalls, 1, "evaluateOnNewDocument called exactly once");
    assert.equal(stub.evaluateCalls, 0, "no immediate evaluate call - injection runs on the next navigation");
  });

  test("a second call results in a second registration (repeat calls are not collapsed at this layer)", async () => {

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

  test("absorbs an evaluateOnNewDocument failure (best-effort prototype override)", async () => {

    // Negative test for the other half of the best-effort contract: the prototype-override registration is guarded too, so an injection failure - a page
    // mid-navigation or tearing down as the stream switches to native delivery - must not abort suppression or the switch that called it. makePageStub
    // hardcodes a resolving evaluateOnNewDocument, so we replace it on this instance to drive the rejecting branch.
    const { page, stub } = makePageStub();

    (page as unknown as { evaluateOnNewDocument: () => Promise<never> }).evaluateOnNewDocument = async (): Promise<never> => {

      throw new Error("synthetic injection failure");
    };

    await assert.doesNotReject(() => suppressPageAudio(page), "injection errors must not escape");
    assert.equal(stub.evaluateCalls, 1, "the immediate mute still runs after a failed registration");
  });
});
