/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tabSelection.test.ts: Unit tests for the tab-selection primitive in tabSelection.ts. The module's whole world is the capture extension's tabs and windows APIs
 * plus a page it writes a title into, so what is pinned here is that conversation as an ordered timeline: which reads it takes, in what order it takes them, what
 * it updates, and what it hands back.
 *
 * Everything is faked in this file rather than mocked at the loader, in the shape tabCapture.test.ts established: an extension page whose evaluate dispatches on
 * the source text of the callback it is handed, a page double that records the title token it is given and answers with the title it replaced, and a shared
 * timeline every call appends to. A two-window fixture backs the rows that care which window a read was scoped to.
 */
import type { Browser, Page } from "puppeteer-core";
import type { SelectedTab, TabSelectionDeps } from "./tabSelection.ts";
import { TAB_NOT_FOUND_MESSAGE, TAB_NOT_SELECTED_MESSAGE, withTabSelected } from "./tabSelection.ts";
import { beforeEach, describe, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "../utils/index.ts";
import type { LogEntry } from "../utils/logEmitter.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { makeFakeClock } from "../utils/clock.helpers.ts";
import { subscribeToLogs } from "../utils/logEmitter.ts";

// This file imports the module that imports puppeteer-stream, which spawns a WebSocketServer at load and would otherwise hold the runner open.
closePuppeteerStreamWssOnIdle();

// A tab in the fake browser: what the extension would report for it, plus the mutable selected flag the fake's own updates move.
interface FakeTab {

  active: boolean;
  id: number;
  title?: string;
  url: string;
  windowId: number;
}

// The fixture a row drives: the tabs the extension can see, which window Chrome treats as current, and the shared ordering record.
interface Fixture {

  extension: Page;
  lastFocusedWindowId: number;
  page: Page;
  pageEvaluates: string[];
  tabs: FakeTab[];
  timeline: string[];
}

/**
 * Runs a body with every emitted log entry captured, and hands back the warnings among them.
 * @param body - The work to run under capture.
 * @returns The warn-level entries emitted while the body ran.
 */
async function captureWarnings(body: () => Promise<void>): Promise<LogEntry[]> {

  const captured: LogEntry[] = [];
  const unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });

  try {

    await body();
  } finally {

    unsubscribe();
  }

  return captured.filter((entry) => entry.level === "warn");
}

/**
 * Builds the fixture: a page double, an extension double over a mutable tab table, and the timeline both write into.
 *
 * The extension's evaluate dispatches on the source text of the callback it receives, which is how one double serves every call in the protocol without the
 * production code needing a test-only branch. Each call records itself with the argument it carried, so a row can assert both the order and the scoping.
 * @param options - The fixture's starting state.
 * @param options.lastFocusedWindowId - The window Chrome treats as current. Defaults to the window this page's tab is in.
 * @param options.ownTitle - The title the page double reports before the token is written. Defaults to a site-like title.
 * @param options.tabs - The tab table. Defaults to this page's tab alone in window 1, with another tab selected beside it.
 * @returns The fixture.
 */
function makeFixture(options: { lastFocusedWindowId?: number; ownTitle?: string; tabs?: FakeTab[] } = {}): Fixture {

  const timeline: string[] = [];
  const pageEvaluates: string[] = [];

  const fixture: Fixture = {

    extension: null as unknown as Page,
    lastFocusedWindowId: options.lastFocusedWindowId ?? 1,
    page: null as unknown as Page,
    pageEvaluates,

    tabs: options.tabs ?? [

      { active: true, id: 7, url: "https://example.test/other", windowId: 1 },
      { active: false, id: 42, url: "https://example.test/live", windowId: 1 }
    ],
    timeline
  };

  // The page double. Its evaluate is the identification's own channel: the first call writes the token and answers with the title it replaced, the second writes
  // that title back. Both are recorded, so a row can assert that a second selection of the same page identifies nothing at all.
  fixture.page = {

    browser: (): Browser => ({ connected: true } as unknown as Browser),
    evaluate: async (target: unknown, arg?: unknown): Promise<unknown> => {

      const source = String(target);

      if(source.includes("document.title = marker")) {

        pageEvaluates.push("title:set");
        timeline.push("page:title");

        const tab = fixture.tabs.find((candidate) => candidate.id === 42);

        if(tab) {

          tab.title = String(arg);
        }

        return options.ownTitle ?? "Example Live";
      }

      pageEvaluates.push("title:restore");
      timeline.push("page:restore");

      return undefined;
    }
  } as unknown as Page;

  fixture.extension = {

    evaluate: async (target: unknown, arg?: unknown): Promise<unknown> => {

      const source = String(target);

      if(source.includes("chrome.tabs.query({})")) {

        timeline.push("query:all");

        return fixture.tabs.map((tab) => ({ active: tab.active, id: tab.id, title: tab.title, url: tab.url, windowId: tab.windowId }));
      }

      if(source.includes("lastFocusedWindow")) {

        timeline.push("query:lastFocused");

        return fixture.tabs.filter((tab) => tab.active && (tab.windowId === fixture.lastFocusedWindowId))
          .map((tab) => ({ active: tab.active, id: tab.id, url: tab.url, windowId: tab.windowId }));
      }

      if(source.includes("chrome.tabs.query")) {

        timeline.push("query:window:" + String(arg));

        return fixture.tabs.filter((tab) => tab.active && (tab.windowId === arg))
          .map((tab) => ({ active: tab.active, id: tab.id, url: tab.url, windowId: tab.windowId }));
      }

      if(source.includes("chrome.tabs.get")) {

        timeline.push("get:" + String(arg));

        const tab = fixture.tabs.find((candidate) => candidate.id === arg);

        if(!tab) {

          throw new Error("No tab with id " + String(arg) + ".");
        }

        return { active: tab.active, id: tab.id, title: tab.title, url: tab.url, windowId: tab.windowId };
      }

      if(source.includes("chrome.tabs.update")) {

        timeline.push("update:" + String(arg));

        for(const tab of fixture.tabs) {

          if(tab.windowId === fixture.tabs.find((candidate) => candidate.id === arg)?.windowId) {

            tab.active = (tab.id === arg);
          }
        }

        return {};
      }

      if(source.includes("chrome.windows.update")) {

        timeline.push("focus:" + String(arg));
        fixture.lastFocusedWindowId = Number(arg);

        return {};
      }

      throw new Error("Unserved evaluate shape: " + source.slice(0, 160));
    }
  } as unknown as Page;

  return fixture;
}

/**
 * Composes the deps a hold runs against.
 * @param fixture - The fixture whose extension double the hold talks to.
 * @returns The deps.
 */
function makeDeps(fixture: Fixture): TabSelectionDeps {

  return { getExtensionPage: async (): Promise<Page> => fixture.extension };
}

/* Every row runs against the one module-level selection chain, so a row that leaves a hold outstanding would be observed by the next. The rows that lapse a
 * ceiling settle their bodies before they finish for that reason.
 */
describe("withTabSelected", () => {

  let clock: Clock;

  beforeEach(() => {

    clock = makeFakeClock().clock;
  });

  test("selects the tab, runs the body, and hands back the tab that was selected before it, in that order", async () => {

    /* The whole protocol as one ordered log. The fresh read comes first because the window a tab sits in can change between holds; the previous-tab read is
     * scoped to that window; the update and its confirmation follow; the focus read decides whether Chrome already treats this window as current; and only after
     * the body has run does the re-read and the give-back land. An implementation that skipped the confirmation, or gave the tab back before the body, reorders
     * or shortens this log.
     */
    const fixture = makeFixture();
    const seen: SelectedTab[] = [];

    await withTabSelected(fixture.page, async (selected: SelectedTab): Promise<void> => {

      seen.push(selected);
      fixture.timeline.push("body");
    }, { clock, deps: makeDeps(fixture) });

    assert.deepEqual(fixture.timeline,
      [ "page:title", "query:all", "page:restore", "get:42", "query:window:1", "update:42", "get:42", "query:lastFocused", "body", "get:42", "update:7" ],
      "identify, read fresh, read the previous tab in that window, select, confirm, check the focus, run, then give the tab back");
    const selected = seen[0];

    assert.ok(selected, "the body ran exactly once, with a selected tab in hand");
    assert.equal(selected.id, 42, "the body receives the page's own tab id");
    assert.equal(selected.url, "https://example.test/live", "and the url that read reported");
    assert.equal(selected.windowId, 1, "and the window the fresh read named");
  });

  test("identifies a page once, by a title token it puts back", async () => {

    /* The extension cannot see CDP target ids and a capture page is a fresh about:blank when it is first selected, so a token in the page's own title is the only
     * thing both sides can see. Two things make it safe: the title is restored, and the answer is cached - a second hold on the same page writes no title at all,
     * which is what the page's own evaluate log proves.
     */
    const fixture = makeFixture();

    await withTabSelected(fixture.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(fixture) });

    assert.deepEqual(fixture.pageEvaluates, [ "title:set", "title:restore" ], "the token goes in and the page's own title comes back");
    assert.equal(fixture.tabs.find((tab) => tab.id === 42)?.title?.startsWith("prismcast-tab-"), true, "the tab was found wearing the token");

    fixture.pageEvaluates.length = 0;

    await withTabSelected(fixture.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(fixture) });

    assert.deepEqual(fixture.pageEvaluates, [], "a second hold on the same page touches its title not at all");
  });

  test("hands the tab back even when the body throws, and the body's rejection travels unchanged", async () => {

    // The give-back is the user's tab coming back, so it cannot be conditional on the work succeeding. The rejection is the caller's to see, unwrapped.
    const fixture = makeFixture();
    const failure = new Error("The body could not do its work.");

    await assert.rejects(withTabSelected(fixture.page, async (): Promise<void> => { throw failure; }, { clock, deps: makeDeps(fixture) }),
      (error: unknown) => error === failure, "the body's own error object reaches the caller");

    assert.equal(fixture.timeline.at(-1), "update:7", "the previous tab was selected again on the way out");
    assert.equal(fixture.tabs.find((tab) => tab.id === 7)?.active, true, "and it is the selected tab once more");
  });

  test("gives nothing back when the tab that was selected before is this page's own, or when nothing was selected", async () => {

    /* Two shapes with the same answer: re-selecting the tab that is already selected is a round trip that changes nothing, and a window with no selected tab has
     * no previous selection to restore. Both would be harmless updates, which is exactly why an implementation could carry them unnoticed.
     */
    for(const tabs of [

      [{ active: true, id: 42, url: "https://example.test/live", windowId: 1 }],
      [{ active: false, id: 42, url: "https://example.test/live", windowId: 1 }]
    ]) {

      const fixture = makeFixture({ tabs });

      // eslint-disable-next-line no-await-in-loop -- Each shape is its own hold and has to settle before the next is set up.
      await withTabSelected(fixture.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(fixture) });

      assert.equal(fixture.timeline.filter((entry) => entry.startsWith("update:")).length, 1, "exactly one update: the selection itself, and no give-back");
    }
  });

  test("swallows a give-back whose update fails, leaving the body's result intact", async () => {

    // The tab that had the selection may have closed while the hold ran. There is nothing else to hand it to, and the caller's work succeeded either way.
    const fixture = makeFixture();
    const evaluate = fixture.extension.evaluate.bind(fixture.extension);

    let held = false;

    (fixture.extension as unknown as { evaluate: (target: unknown, arg?: unknown) => Promise<unknown> }).evaluate =
      async (target: unknown, arg?: unknown): Promise<unknown> => {

        if(held && String(target).includes("chrome.tabs.update") && (arg === 7)) {

          throw new Error("The tab is gone.");
        }

        return evaluate(target as never, arg as never);
      };

    const result = await withTabSelected(fixture.page, async (): Promise<string> => {

      held = true;

      return "the body's answer";
    }, { clock, deps: makeDeps(fixture) });

    assert.equal(result, "the body's answer", "a failed give-back is not the caller's problem");
  });

  test("rejects with the not-found message when no tab wears the token, or the tab that does carries no id", async () => {

    /* The identification is the one step with nothing to fall back on: without a tab id there is no selection to take. Both shapes have to fail before anything
     * is updated, and the page's title has to come back either way - a page left wearing a marker is a visible defect in the browser the user is looking at.
     */
    for(const tabs of [

      [{ active: true, id: 7, url: "https://example.test/other", windowId: 1 }],

      [

        { active: true, id: 7, url: "https://example.test/other", windowId: 1 },
        { active: false, id: 42, url: "https://example.test/live", windowId: 1 }
      ]
    ]) {

      const fixture = makeFixture({ tabs });

      // The second shape has a tab that will wear the token, but the extension reports it without an id.
      if(tabs.length === 2) {

        const evaluate = fixture.extension.evaluate.bind(fixture.extension);

        (fixture.extension as unknown as { evaluate: (target: unknown, arg?: unknown) => Promise<unknown> }).evaluate =
          async (target: unknown, arg?: unknown): Promise<unknown> => {

            const answer = await evaluate(target as never, arg as never);

            if(String(target).includes("chrome.tabs.query({})")) {

              return (answer as { id?: number }[]).map((tab) => ({ ...tab, id: undefined }));
            }

            return answer;
          };
      }

      // eslint-disable-next-line no-await-in-loop -- Each shape is its own hold and has to settle before the next is set up.
      await assert.rejects(withTabSelected(fixture.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(fixture) }),
        (error: unknown) => (error instanceof Error) && (error.message === TAB_NOT_FOUND_MESSAGE), "an unidentifiable tab fails the hold");

      assert.equal(fixture.timeline.filter((entry) => entry.startsWith("update:")).length, 0, "nothing was selected");
      assert.deepEqual(fixture.pageEvaluates, [ "title:set", "title:restore" ], "and the page's own title came back");
    }
  });

  test("rejects with the not-selected message when Chrome does not honor the update, after the give-back has had its say", async () => {

    /* A selection Chrome did not take is a failure to report rather than one to start a capture against: the recorder would point at whatever tab is selected
     * instead. The give-back still runs on this path, and what it does is the interesting half: it re-reads, sees that this page's tab is not the selected one,
     * and leaves the selection where it is - which is right, because a selection that never moved has nothing to restore. The user's tab is untouched throughout.
     */
    const fixture = makeFixture();
    const evaluate = fixture.extension.evaluate.bind(fixture.extension);

    (fixture.extension as unknown as { evaluate: (target: unknown, arg?: unknown) => Promise<unknown> }).evaluate =
      async (target: unknown, arg?: unknown): Promise<unknown> => {

        // The update is accepted and then quietly undone, which is how a refusal Chrome does not raise would look from here.
        if(String(target).includes("chrome.tabs.update") && (arg === 42)) {

          fixture.timeline.push("update:42");

          return {};
        }

        return evaluate(target as never, arg as never);
      };

    await assert.rejects(withTabSelected(fixture.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(fixture) }),
      (error: unknown) => (error instanceof Error) && (error.message === TAB_NOT_SELECTED_MESSAGE), "an unconfirmed selection fails the hold");

    assert.equal(fixture.timeline.at(-1), "get:42", "the give-back ran, and its re-read is the last thing the hold did");
    assert.equal(fixture.timeline.filter((entry) => entry === "update:7").length, 0, "no redundant update: the tab that had the selection never lost it");
    assert.equal(fixture.tabs.find((tab) => tab.id === 7)?.active, true, "and it is still the selected tab");
  });

  test("leaves the selection alone when something else took it during the hold, and gives it back when the tab is gone", async () => {

    /* Whoever moved the selection during the hold - a user clicking a tab, login mode opening its page - chose more recently, so their choice stands. The
     * complementary case is a tab that cannot be read at all: the page has gone, nothing of ours holds the selection, and the previous tab is where it belongs.
     */
    const moved = makeFixture();

    await withTabSelected(moved.page, async (): Promise<void> => {

      for(const tab of moved.tabs) {

        tab.active = (tab.id === 7);
      }
    }, { clock, deps: makeDeps(moved) });

    assert.equal(moved.timeline.filter((entry) => entry === "update:7").length, 0, "no give-back update was issued over the newer choice");

    const gone = makeFixture();

    await withTabSelected(gone.page, async (): Promise<void> => {

      gone.tabs = gone.tabs.filter((tab) => tab.id !== 42);
    }, { clock, deps: makeDeps(gone) });

    assert.equal(gone.timeline.at(-1), "update:7", "a tab that cannot be read hands the selection back to the tab that had it");
  });

  test("serializes holds: the second begins only after the first has given the tab back, and a failed hold does not stop it", async () => {

    /* A window has one selected tab, so two holders alternating under each other would leave each running against the other's selection. The chain is what makes
     * that unrepresentable, and it has to survive a failure: a rejected hold hands its rejection to its own caller while the chain moves on.
     */
    const first = makeFixture();
    const second = makeFixture();
    const order: string[] = [];

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise, resolve } = Promise.withResolvers<void>();

    const failing = withTabSelected(first.page, async (): Promise<void> => {

      order.push("first:body");

      await promise;

      throw new Error("The first hold's body failed.");
    }, { clock, deps: makeDeps(first) });

    const queued = withTabSelected(second.page, async (): Promise<void> => { order.push("second:body"); }, { clock, deps: makeDeps(second) });

    await Promise.resolve();

    assert.deepEqual(second.timeline, [], "the second hold has not touched the extension while the first is holding");

    resolve();

    await assert.rejects(failing, (error: unknown) => (error instanceof Error) && (error.message === "The first hold's body failed."));
    await queued;

    assert.deepEqual(order, [ "first:body", "second:body" ], "the bodies ran one after the other");
    assert.equal(first.timeline.at(-1), "update:7", "the first hold gave its tab back before the second began");
  });

  test("scopes the previous-tab read and the give-back to the window the fresh read named", async () => {

    /* A user can drag a capture tab into another window between holds, which is why the tab id is the only cached identity and the window is read fresh every
     * time. A hold that read the previous tab without scoping it would find the other window's selected tab and hand this window's selection to a tab that is
     * not even in it.
     */
    const fixture = makeFixture({

      tabs: [

        { active: true, id: 3, url: "https://example.test/elsewhere", windowId: 9 },
        { active: true, id: 7, url: "https://example.test/other", windowId: 2 },
        { active: false, id: 42, url: "https://example.test/live", windowId: 2 }
      ]
    });

    fixture.lastFocusedWindowId = 2;

    await withTabSelected(fixture.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(fixture) });

    assert.ok(fixture.timeline.includes("query:window:2"), "the previous-tab read is scoped to the window the fresh read reported");
    assert.equal(fixture.timeline.at(-1), "update:7", "and the give-back targets that window's tab, not the other window's selected tab");
    assert.equal(fixture.tabs.find((tab) => tab.id === 3)?.active, true, "the other window's selection was never touched");
  });

  test("makes its own window current when another window holds the focus, warning once, before the body runs", async () => {

    /* Chrome records the active tab of the LAST-FOCUSED window, so a tab that is merely active in a window Chrome does not consider current is not selected in
     * the sense a capture start needs - a site popup holding the focus would have its own tab recorded instead. The correction is the one deliberate raise in the
     * module, so it is reported, and it has to land before the body acts rather than alongside it.
     */
    const fixture = makeFixture({

      lastFocusedWindowId: 5,
      tabs: [

        { active: true, id: 3, url: "https://popup.example.test/", windowId: 5 },
        { active: true, id: 7, url: "https://example.test/other", windowId: 1 },
        { active: false, id: 42, url: "https://example.test/live", windowId: 1 }
      ]
    });

    const warnings = await captureWarnings(async () => {

      await withTabSelected(fixture.page, async (): Promise<void> => { fixture.timeline.push("body"); }, { clock, deps: makeDeps(fixture) });
    });

    assert.ok(fixture.timeline.indexOf("focus:1") < fixture.timeline.indexOf("body"), "the window is made current before the body acts");
    assert.equal(warnings.length, 1, "one report per hold");
    assert.match(warnings[0]?.message ?? "", /focusedTab: 'https:\/\/popup.example.test\/'/, "naming the tab that held the focus");
  });

  test("issues no focus and no warning when this page's own window is already the current one, or nothing is", async () => {

    // The negative control for the row above: a single-window install is the ordinary case, and it stays raise-free. A read naming nothing is the same answer.
    for(const lastFocusedWindowId of [ 1, 99 ]) {

      const fixture = makeFixture({ lastFocusedWindowId });

      // eslint-disable-next-line no-await-in-loop -- Each shape is its own hold and has to settle before the next is set up.
      const warnings = await captureWarnings(async () => {

        await withTabSelected(fixture.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(fixture) });
      });

      assert.equal(fixture.timeline.filter((entry) => entry.startsWith("focus:")).length, 0, "no window was raised");
      assert.equal(warnings.length, 0, "and nothing was reported");
    }
  });

  test("gives the tab back at its ceiling and moves the chain on, while the body's own result still reaches its caller", async () => {

    /* A body this module does not own can hang - an extension evaluate has no timeout of its own - and a chain wedged by one body would block every later capture
     * start and fullscreen request for the life of the process, surviving even a browser relaunch. The ceiling ends the HOLD rather than the body: the selection
     * goes back, the next hold begins, and the caller still receives what its own body eventually produced.
     */
    const fixture = makeFixture();
    const queuedFixture = makeFixture();
    const ceilings: number[] = [];

    const lapsingClock: Clock = {

      now: (): number => 0,
      sleep: async (): Promise<void> => undefined,
      waitWithTimeout: async <T>(_promise: Promise<T>, timeoutMs: number, timeoutError?: Error): Promise<T> => {

        ceilings.push(timeoutMs);

        throw timeoutError ?? new Error("The bound lapsed.");
      }
    };

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise, resolve } = Promise.withResolvers<void>();

    const warnings: LogEntry[] = [];
    const unsubscribe = subscribeToLogs((entry) => { warnings.push(entry); });

    const held = withTabSelected(fixture.page, async (): Promise<string> => {

      await promise;

      return "the body's answer";
    }, { clock: lapsingClock, deps: makeDeps(fixture) });

    const queued = withTabSelected(queuedFixture.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(queuedFixture) });

    await queued;

    assert.equal(fixture.timeline.at(-1), "update:7", "the selection went back at the ceiling, to the tab that had it");
    assert.equal(queuedFixture.timeline.at(-1), "update:7", "and the queued hold ran to completion before the lapsed body settled");

    resolve();

    assert.equal(await held, "the body's answer", "the caller still receives what its body produced");

    unsubscribe();

    assert.deepEqual(ceilings, [CONFIG.streaming.navigationTimeout], "a bare call rides the deadline a capture start already runs under");

    const lapseWarnings = warnings.filter((entry) => entry.level === "warn");

    assert.equal(lapseWarnings.length, 1, "the lapse is reported once");
    assert.match(lapseWarnings[0]?.message ?? "", /ceilingMs: 10000/, "naming the ceiling it was held past");
  });

  test("rides the ceiling its caller names", async () => {

    // The fullscreen path holds a selection for a whole activation sequence and passes a ceiling of its own, so the value has to be the caller's rather than a
    // constant of this module's.
    const fixture = makeFixture();
    const ceilings: number[] = [];

    const recordingClock: Clock = {

      now: (): number => 0,
      sleep: async (): Promise<void> => undefined,
      waitWithTimeout: async <T>(promise: Promise<T>, timeoutMs: number): Promise<T> => {

        ceilings.push(timeoutMs);

        return promise;
      }
    };

    await withTabSelected(fixture.page, async (): Promise<void> => undefined, { ceilingMs: 6000, clock: recordingClock, deps: makeDeps(fixture) });

    assert.deepEqual(ceilings, [6000], "the context's ceiling is the one the bound runs on");
  });

  test("re-asserts a selection that was taken away during the hold, and issues nothing when it still holds", async () => {

    /* A login page opening or a user's click can deselect the capture's tab mid-hold, and the next thing the body does would then run against the wrong tab. The
     * re-assert is what a body calls before each round trip it cares about; when nothing moved, it costs one read and no update.
     */
    const fixture = makeFixture();

    await withTabSelected(fixture.page, async (selected: SelectedTab): Promise<void> => {

      await selected.reassert();

      fixture.timeline.push("mark:unchanged");

      for(const tab of fixture.tabs) {

        tab.active = (tab.id === 7);
      }

      await selected.reassert();
    }, { clock, deps: makeDeps(fixture) });

    const marker = fixture.timeline.indexOf("mark:unchanged");
    const before = fixture.timeline.slice(0, marker);
    const after = fixture.timeline.slice(marker);

    assert.equal(before.filter((entry) => entry === "update:42").length, 1, "the re-assert on a selection that still holds issues no update of its own");
    assert.equal(after.filter((entry) => entry === "update:42").length, 1, "the re-assert after the deselection takes the tab back");
  });

  test("re-focuses this page's window when another took the focus mid-hold, and issues nothing when none did", async () => {

    // The same correction the selection step performs, repeated per re-assert: a window that takes the focus between two capture attempts would divert the next
    // attempt's recording to its own tab.
    const fixture = makeFixture({

      tabs: [

        { active: true, id: 3, url: "https://popup.example.test/", windowId: 5 },
        { active: true, id: 7, url: "https://example.test/other", windowId: 1 },
        { active: false, id: 42, url: "https://example.test/live", windowId: 1 }
      ]
    });

    await withTabSelected(fixture.page, async (selected: SelectedTab): Promise<void> => {

      await selected.reassert();

      fixture.timeline.push("mark:focus-taken");
      fixture.lastFocusedWindowId = 5;

      await selected.reassert();
    }, { clock, deps: makeDeps(fixture) });

    const marker = fixture.timeline.indexOf("mark:focus-taken");

    assert.equal(fixture.timeline.slice(0, marker).filter((entry) => entry === "focus:1").length, 0, "nothing is raised while this window is already current");
    assert.equal(fixture.timeline.slice(marker).filter((entry) => entry === "focus:1").length, 1, "and the window is made current again once another took it");
  });

  test("rejects with the not-found message when the fresh read carries no id or no window, before anything is updated", async () => {

    /* SelectedTab's id and windowId are required because the selection cannot be scoped without them, and the narrowing that earns them sits at the read rather
     * than at the point of use. A read missing either has to fail before an update goes out against a window nobody knows.
     */
    for(const missing of [ "id", "windowId" ]) {

      const fixture = makeFixture();
      const evaluate = fixture.extension.evaluate.bind(fixture.extension);

      (fixture.extension as unknown as { evaluate: (target: unknown, arg?: unknown) => Promise<unknown> }).evaluate =
        async (target: unknown, arg?: unknown): Promise<unknown> => {

          const answer = await evaluate(target as never, arg as never);

          if(String(target).includes("chrome.tabs.get")) {

            return { ...(answer as object), [missing]: undefined };
          }

          return answer;
        };

      // eslint-disable-next-line no-await-in-loop -- Each shape is its own hold and has to settle before the next is set up.
      await assert.rejects(withTabSelected(fixture.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(fixture) }),
        (error: unknown) => (error instanceof Error) && (error.message === TAB_NOT_FOUND_MESSAGE), "a read missing " + missing + " cannot be selected against");

      assert.equal(fixture.timeline.filter((entry) => entry.startsWith("update:")).length, 0, "nothing was updated");
    }
  });
});
