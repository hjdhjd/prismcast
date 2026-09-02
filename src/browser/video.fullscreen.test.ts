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
import { describe, test } from "node:test";
import type { FullscreenDeps } from "./video.ts";
import type { SelectedTab } from "./tabSelection.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { ensureFullscreen } from "./video.ts";

// The module under test reaches puppeteer-stream through its own imports, which spawns a WebSocketServer at load and would otherwise hold the runner open.
closePuppeteerStreamWssOnIdle();

// The video selector type every row drives. Which strategy it names is immaterial here - it is carried through to the doubles unread.
const SELECTOR_TYPE: VideoSelectorType = "selectFirstVideo";

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
 * @param options.nativeActive - What the native-fullscreen read answers. Defaults to true.
 * @param options.verified - What the dimension check answers. Defaults to true.
 * @returns The context double.
 */
function makeContext(timeline: string[], options: { nativeActive?: boolean; verified?: boolean } = {}): Frame | Page {

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

describe("ensureFullscreen", () => {

  test("runs the whole native sequence inside one selection when the first attempt verifies", async () => {

    /* The selection is what makes the Fullscreen API grantable, so it has to be taken before the trigger and given back only once the sequence has confirmed
     * native fullscreen. The recorded order is the pin: a sequence that selected per attempt, or released before its verification, reorders this log.
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
  });
});
