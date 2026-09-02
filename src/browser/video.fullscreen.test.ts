/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * video.fullscreen.test.ts: Unit tests for the fullscreen path in video.ts, and specifically for the tab selection it takes. Chrome grants the Fullscreen API to
 * the tab its window has selected, so a native sequence runs inside one hold of the selection primitive - taken once, given back once, whatever the sequence has
 * to do in between.
 *
 * Everything is faked here rather than mocked at the loader, in the shape browser/tuning's page doubles use: a context whose evaluate dispatches on the source
 * text of the callback it is handed, a page recording its clicks and keystrokes, and a selection primitive recording the hold's two boundaries. What each row
 * reads is the resulting timeline, because the question is where the selection sits relative to the sequence's own steps rather than what any one step returned.
 */
import type { Frame, Page } from "puppeteer-core";
import type { ResolvedSiteProfile, VideoSelectorType } from "../types/index.ts";
import { closePuppeteerStreamWssOnIdle, seedVideoSelector, withDocument } from "../testing.helpers.ts";
import { describe, test } from "node:test";
import type { FullscreenDeps } from "./video.ts";
import type { SelectedTab } from "./tabSelection.ts";
import assert from "node:assert/strict";
import { ensureFullscreen } from "./video.ts";

// The module under test reaches puppeteer-stream through its own imports, which spawns a WebSocketServer at load and would otherwise hold the runner open.
closePuppeteerStreamWssOnIdle();

// The video selector type every row drives. Which strategy it names is immaterial here - it is carried through to the doubles unread.
const SELECTOR_TYPE: VideoSelectorType = "selectFirstVideo";

// The escalation's in-page styling callback, which takes the selector type and styles whatever the page-side selector hands it.
type AggressiveCallback = (type: string) => void;

/**
 * Builds the profile a row tunes with.
 * @param overrides - The profile fields the row cares about.
 * @returns The profile.
 */
function makeProfile(overrides: Partial<ResolvedSiteProfile> = {}): ResolvedSiteProfile {

  return { useRequestFullscreen: true, ...overrides } as unknown as ResolvedSiteProfile;
}

/**
 * Builds the context double. Its evaluate dispatches on the source text of the callback it is handed, which is how one double serves the styling, verification,
 * and native-state reads without the production code needing a test-only branch.
 * @param timeline - The shared ordering record.
 * @param options - The scripted answers.
 * @param options.aggressive - Receives the escalation's own styling callback, so a row can run it against a fixture instead of only recording that it ran.
 * @param options.nativeActive - What the native-fullscreen read answers. Defaults to true.
 * @param options.verified - What the dimension check answers. Defaults to true.
 * @returns The context double.
 */
function makeContext(timeline: string[],
  options: { aggressive?: (callback: AggressiveCallback) => void; nativeActive?: boolean; verified?: boolean } = {}): Frame | Page {

  return {

    evaluate: async (target: unknown): Promise<unknown> => {

      const source = String(target);

      if(source.includes("document.fullscreenElement")) {

        timeline.push("native");

        return options.nativeActive ?? true;
      }

      if(source.includes("requestFullscreen")) {

        timeline.push("trigger");

        return undefined;
      }

      if(source.includes("innerWidth")) {

        timeline.push("verify");

        return options.verified ?? true;
      }

      if(source.includes("rect.width / 2")) {

        timeline.push("coords");

        return { x: 10, y: 10 };
      }

      // The two styling passes are told apart by the layer each lifts the video to: the ordinary pass names 999000, the escalation's !important pass 999999.
      if(source.includes("999999")) {

        timeline.push("aggressive");
        options.aggressive?.(target as AggressiveCallback);

        return undefined;
      }

      timeline.push("styles");

      return undefined;
    }
  } as unknown as Frame | Page;
}

/**
 * Builds the page double, recording the traffic the sequence's activation and trigger steps produce.
 * @param timeline - The shared ordering record.
 * @returns The page double.
 */
function makePage(timeline: string[]): Page {

  return {

    $: async (): Promise<null> => null,
    addStyleTag: async (): Promise<void> => { timeline.push("styleTag"); },
    keyboard: { type: async (key: string): Promise<void> => { timeline.push("type:" + key); } },
    mouse: { click: async (): Promise<void> => { timeline.push("click"); } }
  } as unknown as Page;
}

/**
 * Builds the injected selection primitive, recording the hold's two boundaries into the shared timeline.
 * @param timeline - The shared ordering record.
 * @param ceilings - The ceiling each hold was asked for, so a row can read that the fullscreen path passes its own.
 * @returns The deps.
 */
function makeDeps(timeline: string[], ceilings: (number | undefined)[]): FullscreenDeps {

  return {

    withTabSelected: async <T>(_page: Page, body: (tab: SelectedTab) => Promise<T>, context?: { ceilingMs?: number }): Promise<T> => {

      ceilings.push(context?.ceilingMs);
      timeline.push("select");

      const selected: SelectedTab = {

        id: 42,
        reassert: async (): Promise<void> => { timeline.push("reassert"); },
        url: "https://example.test/live",
        windowId: 1
      };

      try {

        return await body(selected);
      } finally {

        timeline.push("release");
      }
    }
  };
}

/**
 * Drives a CSS-only sequence whose verifications never pass, so the escalation runs, and returns the styling callback that pass handed to evaluate. The callback
 * carries both of the escalation's style lists - the video's own and its containers' - which is why the row that uses it executes it against a fixture: the two
 * lists live in one function source, so reading that source cannot tell which element each list reaches.
 * @returns The escalation's styling callback.
 */
async function captureAggressiveCallback(): Promise<AggressiveCallback> {

  const timeline: string[] = [];
  const captured: AggressiveCallback[] = [];

  await ensureFullscreen(makePage(timeline), makeContext(timeline, { aggressive: (callback) => captured.push(callback), verified: false }),
    makeProfile({ useRequestFullscreen: false }), SELECTOR_TYPE, false, makeDeps(timeline, []));

  const [callback] = captured;

  assert.ok(callback, "the sequence escalated and its styling callback was captured");

  return callback;
}

describe("ensureFullscreen", () => {

  test("runs the whole native sequence inside one selection when the first attempt verifies", async () => {

    /* The selection is what makes the Fullscreen API grantable, so it has to be taken before the trigger and given back only once the sequence has confirmed
     * native fullscreen. The recorded order is the assertion: a sequence that selected per attempt, or released before its verification, reorders this log.
     */
    const timeline: string[] = [];
    const ceilings: (number | undefined)[] = [];

    await ensureFullscreen(makePage(timeline), makeContext(timeline), makeProfile(), SELECTOR_TYPE, false, makeDeps(timeline, ceilings));

    assert.deepEqual(timeline, [ "select", "styles", "trigger", "verify", "native", "release" ],
      "the selection brackets the styling, the trigger, and both verifications");
    assert.deepEqual(ceilings, [6000], "the fullscreen path holds under its own ceiling rather than the capture start's default");
  });

  test("takes no selection at all on the recovery path", async () => {

    // Monitor recovery passes skipNativeFullscreen, which is the CSS-only path: no Fullscreen API call, so no reason to move the user's tab selection.
    const timeline: string[] = [];
    const ceilings: (number | undefined)[] = [];

    await ensureFullscreen(makePage(timeline), makeContext(timeline), makeProfile(), SELECTOR_TYPE, true, makeDeps(timeline, ceilings));

    assert.equal(timeline.includes("select"), false, "the recovery path never selects a tab");
    assert.ok(timeline.includes("styles"), "while the CSS styling it does own still ran");
  });

  test("takes no selection for a profile that does not use the Fullscreen API", async () => {

    // The complementary control: the profile field, not the recovery flag, is what decides here. A CSS-only profile has nothing to ask Chrome for.
    const timeline: string[] = [];
    const ceilings: (number | undefined)[] = [];

    await ensureFullscreen(makePage(timeline), makeContext(timeline), makeProfile({ useRequestFullscreen: false }), SELECTOR_TYPE, false,
      makeDeps(timeline, ceilings));

    assert.equal(timeline.includes("select"), false, "a CSS-only profile never selects a tab");
    assert.deepEqual(ceilings, [], "and never asks for a hold");
  });

  test("escalates inside the SAME selection when the attempts never verify", async () => {

    /* The escalation is the longest a sequence can run - three attempts, the activation clicks, the aggressive styling, the re-trigger, and a final pair of
     * reads. All of it is one hold: releasing between the attempts and the escalation would hand the user their tab back and take it again mid-sequence, and
     * would leave the escalation's own Fullscreen API call running against a tab Chrome does not treat as selected.
     */
    const timeline: string[] = [];
    const ceilings: (number | undefined)[] = [];

    await ensureFullscreen(makePage(timeline), makeContext(timeline, { verified: false }), makeProfile(), SELECTOR_TYPE, false, makeDeps(timeline, ceilings));

    assert.equal(timeline.filter((entry) => entry === "select").length, 1, "exactly one selection covers the escalation");
    assert.equal(timeline[0], "select", "taken before the first attempt");
    assert.equal(timeline.at(-1), "release", "and given back only at the very end");

    const aggressive = timeline.indexOf("aggressive");

    assert.ok(aggressive > 0, "the escalation ran");
    assert.ok(timeline.indexOf("click") < aggressive, "the activation click precedes the aggressive styling");
    assert.ok(timeline.lastIndexOf("verify") > aggressive, "and the final verification follows it, still inside the hold");
    assert.deepEqual(timeline.filter((entry) => entry.startsWith("type:")), [], "and the escalation sends no keypress for a profile that carries no key");
  });

  test("the escalation presses no key for a profile without one", async () => {

    /* A profile with no fullscreenKey has keyboard fullscreen turned off, and the escalation honors that rather than typing "f" on its own behalf. The row
     * above is the same reading on the native path; this one is the CSS-only path, where the escalation is all that could send a keystroke.
     */
    const timeline: string[] = [];
    const ceilings: (number | undefined)[] = [];

    await ensureFullscreen(makePage(timeline), makeContext(timeline, { verified: false }), makeProfile({ useRequestFullscreen: false }), SELECTOR_TYPE, false,
      makeDeps(timeline, ceilings));

    assert.ok(timeline.includes("aggressive"), "the sequence escalated");
    assert.deepEqual(timeline.filter((entry) => entry.startsWith("type:")), [], "and no keypress reached the page");
  });

  test("a profile that carries a key is still typed on every attempt, and never by the escalation", async () => {

    // The complement, so the row above reads as a policy the profile sets rather than a keypress the sequence lost: triggerFullscreen sends the key once per
    // simple retry, three times over the three of them, and the escalation adds none.
    const timeline: string[] = [];
    const ceilings: (number | undefined)[] = [];

    await ensureFullscreen(makePage(timeline), makeContext(timeline, { verified: false }), makeProfile({ fullscreenKey: "f", useRequestFullscreen: false }),
      SELECTOR_TYPE, false, makeDeps(timeline, ceilings));

    assert.equal(timeline.filter((entry) => (entry === "type:f")).length, 3, "one keypress per simple retry");
    assert.ok(timeline.lastIndexOf("type:f") < timeline.indexOf("aggressive"), "every keypress precedes the escalation");
  });

  test("the escalation clears the box properties on the video and the containing-block ones on the container above it", async () => {

    /* What the escalation has to answer at its own tier: a player's centering translate slides the anchored video off-center, a stylesheet max holds its box to
     * a fraction of the viewport, and a transformed ancestor is the harder case - it becomes the containing block for the video's fixed positioning, so the
     * video anchors to that ancestor's box instead of to the viewport and no styling on the video alone corrects it. The independent transform properties do
     * that to an ancestor exactly as a transform does, which is why the container list clears them too and the video list does not stop at the maxes. Running
     * the callback against a fixture is what tells its style lists apart, since reading its source would only show that both strings are present somewhere in
     * one function.
     */
    const callback = await captureAggressiveCallback();

    withDocument("<div id=\"c\"><video id=\"v\"></video></div>", (window) => {

      // happy-dom types getElementById as the base Element, which carries no inline style declaration, so the fixture reads go through a narrow view of each one.
      const container = window.document.getElementById("c") as unknown as HTMLElement;
      const video = window.document.getElementById("v") as unknown as HTMLElement;

      seedVideoSelector(window, video);

      callback(SELECTOR_TYPE);

      assert.ok(video.style.cssText.includes("transform: none !important"), "the video's own transform is cleared at the escalation's priority");
      assert.ok(video.style.cssText.includes("transition: none !important"), "and its transition with it, so the clear snaps rather than animating");
      assert.ok(video.style.cssText.includes("margin: 0px !important"), "and its margin, which the CSSOM serializes as a zero length");
      assert.ok(video.style.cssText.includes("max-height: none !important"), "the maxes go with them, since a stylesheet max clamps the box at any priority");
      assert.ok(video.style.cssText.includes("max-width: none !important"), "on the width axis the field failure came from as well");
      assert.ok(video.style.cssText.includes("rotate: none !important"), "and each independent transform property is cleared on its own");
      assert.ok(video.style.cssText.includes("scale: none !important"), "because clearing transform reaches none of them");
      assert.ok(video.style.cssText.includes("translate: none !important"), "and any one of them alone moves the box off the viewport");
      assert.ok(container.style.cssText.includes("transform: none !important"), "the container's transform is cleared as well, so the video anchors to the " +
        "viewport rather than to it");
      assert.ok(container.style.cssText.includes("transition: none !important"), "and the container's transition, for the reason it is cleared on the video");
      assert.ok(container.style.cssText.includes("rotate: none !important"), "the container's rotate goes too, since it makes the same containing block");
      assert.ok(container.style.cssText.includes("scale: none !important"), "and its scale");
      assert.ok(container.style.cssText.includes("translate: none !important"), "and its translate, the one a centering player is most likely to have set");
    });
  });
});
