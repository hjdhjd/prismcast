/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tabSelection.test.ts: Unit tests for the tab-selection primitive in tabSelection.ts. The module's whole world is the capture extension's tabs and windows APIs
 * plus a page it writes a title into, so what is asserted here is that conversation as an ordered timeline: which reads it takes, in what order it takes them, what
 * it updates, what it announces to its activation subscribers, and what it hands back.
 *
 * Everything is faked in this file rather than mocked at the loader, in the shape tabCapture.test.ts established: an extension page whose evaluate dispatches on
 * the source text of the callback it is handed, a page double that records the title token it is given and answers with the title it replaced, and a shared
 * timeline every call appends to. A two-window fixture backs the rows that care which window a read was scoped to.
 */
import type { Browser, Page, Target } from "puppeteer-core";
import type { SelectedTab, SharedWindowTabDeps, TabSelectionDeps } from "./tabSelection.ts";
import { TAB_NOT_FOUND_MESSAGE, TAB_NOT_SELECTED_MESSAGE, getCachedTabId, onTabActivation, openSharedWindowTab, withTabSelected } from "./tabSelection.ts";
import { beforeEach, describe, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "../utils/index.ts";
import type { LogEntry } from "../utils/logEmitter.ts";
import type { Nullable } from "../types/index.ts";
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

// A tab an open produced: the page behind it, the URL it wears, the tab that was selected before it arrived, and whether it has been closed.
interface OpenedTab {

  closed: boolean;
  id: number;
  page: Page;
  previousActiveId?: number;
  url: string;
}

/* The world an open runs in. The extension double and the tab table are the hold rows' own, so a tab arriving selected and a selection handed back are the same
 * reads those rows make; what is new is a carrier page for the open to be evaluated on, a browser that reports its pages and resolves the target an open
 * produced, and the dials a row turns to make each degradation happen.
 */
interface OpenWorld {

  browser: Browser;

  // The page the resolver hands back, and how many times it was asked. Two resolves is the retry.
  carrier: Nullable<Page>;
  carrierResolves: number;

  // The pages the placement confirmation was handed, in order.
  confirmedPages: Page[];

  // The options each fallback creation was asked for, and the pages it handed back.
  createdOptions: unknown[];
  createdPages: Page[];

  fixture: Fixture;

  // The URL each open was asked for, so a row can read the nonce the tab wears.
  openUrls: string[];

  // The tabs the opens produced, in order.
  opened: OpenedTab[];

  // Whether the carrier's window.open reports a window at all. False is Chrome refusing the open.
  opens: boolean;

  // The answers the placement confirmation gives, taken in order. An exhausted list confirms.
  placements: boolean[];

  // Whether the tab an open produced ever turns up as a target.
  targetAppears: boolean;

  // Whether the target wait hangs until the turn's own ceiling cancels it, which is the only other way this double ends a wait.
  targetHangs: boolean;
}

/**
 * Builds the page behind a tab an open produced: it answers the identification against its own tab, reports the nonce URL a scan looks for, and takes its tab out
 * of the table when it is closed - handing the selection back to whatever had it before the tab arrived, the way Chrome does.
 * @param world - The world the tab belongs to.
 * @param id - The tab id the identification should name.
 * @param url - The URL the tab wears.
 * @returns The page double.
 */
function makeOpenedPage(world: OpenWorld, id: number, url: string): Page {

  const fixture = world.fixture;

  let closed = false;

  return {

    browser: (): Browser => world.browser,

    close: async (): Promise<void> => {

      closed = true;

      fixture.timeline.push("close:" + String(id));
      fixture.tabs = fixture.tabs.filter((tab) => tab.id !== id);

      const entry = world.opened.find((tab) => tab.id === id);

      if(entry) {

        entry.closed = true;
      }

      const restored = fixture.tabs.find((tab) => tab.id === entry?.previousActiveId);

      if(restored) {

        restored.active = true;
      }
    },

    evaluate: async (target: unknown, arg?: unknown): Promise<unknown> => {

      if(!String(target).includes("document.title = marker")) {

        fixture.timeline.push("opened:restore");

        return undefined;
      }

      fixture.timeline.push("opened:title");

      const tab = fixture.tabs.find((candidate) => candidate.id === id);

      if(tab) {

        tab.title = String(arg);
      }

      return "";
    },
    isClosed: (): boolean => closed,
    url: (): string => url
  } as unknown as Page;
}

/**
 * Builds the world an open runs in: the extension double from makeFixture, a carrier page whose identification names tab 42 of window 1, and a browser that
 * reports its pages, resolves the target an open produced, and records the pages the fallback creates. Every dial starts at its healthy setting.
 * @returns The world.
 */
function makeOpenWorld(): OpenWorld {

  const fixture = makeFixture();

  const world: OpenWorld = {

    browser: null as unknown as Browser,
    carrier: null,
    carrierResolves: 0,
    confirmedPages: [],
    createdOptions: [],
    createdPages: [],
    fixture,
    openUrls: [],
    opened: [],
    opens: true,
    placements: [],
    targetAppears: true,
    targetHangs: false
  };

  let nextOpenedTabId = 100;
  let carrierClosed = false;

  /* The carrier. Its identification is the fixture page's own - the token it writes names tab 42, the tab the window's other tab is selected beside - and the one
   * call it serves beyond that is the open, which delivers the tab the way Chrome does: in the carrier's window, and selected.
   */
  world.carrier = {

    browser: (): Browser => world.browser,
    close: async (): Promise<void> => { carrierClosed = true; },

    evaluate: async (target: unknown, arg?: unknown): Promise<unknown> => {

      if(!String(target).includes("window.open")) {

        return fixture.page.evaluate(target as never, arg as never);
      }

      const url = String(arg);

      fixture.timeline.push("open");
      world.openUrls.push(url);

      if(!world.opens) {

        return false;
      }

      const id = nextOpenedTabId++;
      const previousActiveId = fixture.tabs.find((tab) => tab.active && (tab.windowId === 1))?.id;

      // Chrome delivers the tab selected, which is what leaves the window's previous selection needing to be handed back.
      for(const tab of fixture.tabs) {

        if(tab.windowId === 1) {

          tab.active = false;
        }
      }

      fixture.tabs.push({ active: true, id, url, windowId: 1 });
      world.opened.push({ closed: false, id, page: makeOpenedPage(world, id, url), previousActiveId, url });

      return true;
    },
    isClosed: (): boolean => carrierClosed,
    url: (): string => "https://example.test/carrier"
  } as unknown as Page;

  world.browser = {

    newPage: async (options?: unknown): Promise<Page> => {

      const created = { isClosed: (): boolean => false, url: (): string => "about:blank" } as unknown as Page;

      world.createdOptions.push(options);
      world.createdPages.push(created);

      return created;
    },
    pages: async (): Promise<Page[]> => [ ...(world.carrier ? [world.carrier] : []), ...world.opened.filter((tab) => !tab.closed).map((tab) => tab.page) ],

    waitForTarget: async (predicate: (target: Target) => boolean, options?: { signal?: AbortSignal }): Promise<Target> => {

      fixture.timeline.push("waitForTarget");

      // The only way this wait ends without a target is the caller's own ceiling, which is what makes a row that hangs it a test of the signal being threaded.
      if(world.targetHangs) {

        const hung = Promise.withResolvers<Target>();

        options?.signal?.addEventListener("abort", () => { hung.reject(options.signal?.reason as Error); });

        return hung.promise;
      }

      const match = world.opened.find((tab) => !tab.closed && predicate({ url: (): string => tab.url } as unknown as Target));

      if(!match || !world.targetAppears) {

        throw new Error("Waiting for target failed: timeout 3000 ms exceeded.");
      }

      return { page: async (): Promise<Nullable<Page>> => match.page } as unknown as Target;
    }
  } as unknown as Browser;

  return world;
}

/**
 * Composes the deps an open runs against: the extension lookup the hold rows use, and the two topology answers the module cannot resolve for itself.
 * @param world - The world the open runs in.
 * @returns The deps.
 */
function makeOpenDeps(world: OpenWorld): SharedWindowTabDeps {

  return {

    confirmPlacement: async (page: Page): Promise<boolean> => {

      world.fixture.timeline.push("confirm");
      world.confirmedPages.push(page);

      return world.placements.shift() ?? true;
    },
    getExtensionPage: async (): Promise<Page> => world.fixture.extension,

    resolveCarrier: async (): Promise<Nullable<Page>> => {

      world.carrierResolves++;

      return world.carrier;
    }
  };
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

    assert.deepEqual(ceilings, [CONFIG.streaming.navigationTimeout], "a bare call uses the deadline a capture start already runs under");

    const lapseWarnings = warnings.filter((entry) => entry.level === "warn");

    assert.equal(lapseWarnings.length, 1, "the lapse is reported once");
    assert.match(lapseWarnings[0]?.message ?? "", /ceilingMs: 10000/, "naming the ceiling it was held past");
  });

  test("uses the ceiling its caller names", async () => {

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

  /* The four rows below subscribe to the activation report, which lives in a module-level set. Each one unsubscribes in a finally for that reason: a subscription
   * left behind would keep recording into a dead row's array while the rows after it ran, and the failure it produced would name the wrong row.
   */

  test("reports every activation it performs, in the order it performs them, naming the tab each one selected", async () => {

    /* A captured page hears nothing when its tab is activated through the extension, so this report is the only announcement of the fact there is. Both of the
     * module's updates make one, and where each lands in the timeline is the assertion: the selection's report sits behind its update and ahead of the
     * confirmation, because the update is what Chrome acted on; the give-back's sits behind its own update, after the body. The bridge from a reported id back to
     * a page is asserted beside them, since an id no subscriber can resolve to a page is an announcement about nothing.
     */
    const fixture = makeFixture();
    const reported: number[] = [];

    const unsubscribe = onTabActivation((tabId: number): void => {

      reported.push(tabId);
      fixture.timeline.push("report:" + String(tabId));
    });

    try {

      await withTabSelected(fixture.page, async (): Promise<void> => { fixture.timeline.push("body"); }, { clock, deps: makeDeps(fixture) });
    } finally {

      unsubscribe();
    }

    assert.deepEqual(fixture.timeline,

      [ "page:title", "query:all", "page:restore", "get:42", "query:window:1", "update:42", "report:42", "get:42", "query:lastFocused", "body", "get:42",
        "update:7", "report:7" ],
      "each report lands immediately behind the update that earned it, and nowhere else in the protocol");
    assert.deepEqual(reported, [ 42, 7 ], "the selection's activation, then the give-back's");
    assert.equal(getCachedTabId(fixture.page), 42, "and the id the selection reported resolves back to the page it named");
  });

  test("reports nothing for a give-back that yielded, one with nothing to give back, or one whose update was refused", async () => {

    /* A report is a statement that Chrome activated a tab, so every give-back path that issues no update has to be silent. Three of them: one that finds
     * something else selected and leaves the newer choice standing, one whose page already held the selection so there is nothing to restore, and one whose
     * update is refused because the tab it was for has closed. All three still perform the selection's own activation, so the honest reading of each is exactly
     * one report - which is also what tells a silent give-back from a hold that stopped reporting altogether.
     */
    const reported: number[] = [];
    const unsubscribe = onTabActivation((tabId: number): void => { reported.push(tabId); });

    try {

      // Yielded: something else took the selection while the body ran.
      const yielded = makeFixture();

      await withTabSelected(yielded.page, async (): Promise<void> => {

        for(const tab of yielded.tabs) {

          tab.active = (tab.id === 7);
        }
      }, { clock, deps: makeDeps(yielded) });

      // Nothing to give back: the tab that held the selection before the hold is this page's own.
      const own = makeFixture({ tabs: [{ active: true, id: 42, url: "https://example.test/live", windowId: 1 }] });

      await withTabSelected(own.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(own) });

      // Refused: the tab that held the selection has closed, so the give-back's update activates nothing.
      const refused = makeFixture();
      const evaluate = refused.extension.evaluate.bind(refused.extension);

      let held = false;

      (refused.extension as unknown as { evaluate: (target: unknown, arg?: unknown) => Promise<unknown> }).evaluate =
        async (target: unknown, arg?: unknown): Promise<unknown> => {

          if(held && String(target).includes("chrome.tabs.update") && (arg === 7)) {

            throw new Error("The tab is gone.");
          }

          return evaluate(target as never, arg as never);
        };

      await withTabSelected(refused.page, async (): Promise<void> => { held = true; }, { clock, deps: makeDeps(refused) });
    } finally {

      unsubscribe();
    }

    assert.deepEqual(reported, [ 42, 42, 42 ], "three holds, three selection reports, and not one give-back among them");
  });

  test("stops delivering once the subscription is ended, and stands by the reports already taken", async () => {

    // The set outlives every hold, so a subscriber that has gone away has exactly one way to stop being called, and the reports it already took are its own.
    const first = makeFixture();
    const reported: number[] = [];
    const unsubscribe = onTabActivation((tabId: number): void => { reported.push(tabId); });

    try {

      await withTabSelected(first.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(first) });
    } finally {

      unsubscribe();
    }

    assert.deepEqual(reported, [ 42, 7 ], "the hold under the subscription was reported in full");

    const second = makeFixture();

    await withTabSelected(second.page, async (): Promise<void> => undefined, { clock, deps: makeDeps(second) });

    assert.deepEqual(reported, [ 42, 7 ], "and the hold after it added nothing");
    assert.equal(second.timeline.at(-1), "update:7", "though that hold ran its activations all the same");
  });

  test("keeps a failing subscriber away from the hold and from the subscriber beside it", async () => {

    /* A subscriber is somebody else's code, and a hold is the one thing here a caller waits on: a throw reaching the selection step would fail a capture start,
     * and one reaching the give-back would leave the user's tab where it was not. The second subscriber is what proves the swallow is per-listener rather than a
     * bail-out that quietly drops the rest of the walk.
     */
    const fixture = makeFixture();
    const reported: number[] = [];

    const unsubscribeFailing = onTabActivation((): void => {

      throw new Error("The subscriber could not handle the activation.");
    });

    const unsubscribe = onTabActivation((tabId: number): void => { reported.push(tabId); });

    try {

      assert.equal(await withTabSelected(fixture.page, async (): Promise<string> => "the body's answer", { clock, deps: makeDeps(fixture) }),
        "the body's answer", "the hold completed and its body's result reached the caller");
    } finally {

      unsubscribeFailing();
      unsubscribe();
    }

    assert.deepEqual(reported, [ 42, 7 ], "the subscriber behind the failing one received both reports");
    assert.equal(fixture.timeline.at(-1), "update:7", "and the give-back still ran");
  });
});

/* The open rows share the module-level chain with the hold rows above, so each one settles its turn before it finishes. Every degradation ends at the same plain
 * create, which is what makes the recorded creation options the evidence that an anchor was given up rather than never attempted.
 */
describe("openSharedWindowTab", () => {

  test("opens the tab on a page of the shared window, hands the selection straight back, and returns the tab it opened", async () => {

    /* The whole turn as one ordered log. The carrier is identified and its window read first, because the window a page sits in is the only route to the tab that
     * will want the selection back; the open follows; the tab is waited for by its nonce, confirmed where it landed, identified, and only then does the selection
     * go back. An implementation that opened before reading the previous selection, or handed the tab over before giving it back, reorders or shortens this log.
     */
    const world = makeOpenWorld();

    const page = await openSharedWindowTab(world.browser, { deps: makeOpenDeps(world) });

    assert.deepEqual(world.fixture.timeline,

      [ "page:title", "query:all", "page:restore", "get:42", "query:window:1", "open", "waitForTarget", "confirm", "opened:title", "query:all", "opened:restore",
        "get:100", "update:7" ],
      "identify the carrier, read its window, read that window's selected tab, open, wait for the tab, confirm where it landed, identify it, give the selection " +
      "back");
    assert.equal(world.createdOptions.length, 0, "nothing was created the plain way");
    assert.equal(page, world.opened[0]?.page, "the tab the open produced is what comes back");
    assert.match(world.openUrls[0] ?? "", /^about:blank#prismcast-open-\d+$/, "the open carries a nonce fragment, which is what the target wait matches on");
    assert.equal(world.confirmedPages[0], page, "the placement confirmation is asked about the tab that arrived, not about the carrier it came from");
    assert.equal(world.fixture.tabs.find((tab) => tab.id === 7)?.active, true, "and the tab that had the selection has it again");
    assert.equal(getCachedTabId(page), 100, "the opened tab is identified here, so the capture start that follows finds the answer already cached");
  });

  test("queues behind a hold, beginning only once the hold has given the selection back", async () => {

    /* An open takes the selection too - the tab arrives selected - so an open running beside a hold would be two holders alternating under each other, and a
     * capture start whose tab was deselected mid-acquisition records whatever tab replaced it. The chain is what makes that unrepresentable rather than unlikely.
     */
    const held = makeFixture();
    const world = makeOpenWorld();

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise, resolve } = Promise.withResolvers<void>();

    const holding = withTabSelected(held.page, async (): Promise<void> => { await promise; }, { clock: makeFakeClock().clock, deps: makeDeps(held) });
    const opening = openSharedWindowTab(world.browser, { deps: makeOpenDeps(world) });

    await Promise.resolve();

    assert.equal(world.fixture.timeline.length, 0, "the open has not touched the extension while the hold is holding");
    assert.equal(world.carrierResolves, 0, "and it has not so much as resolved a carrier");

    resolve();

    await holding;
    await opening;

    assert.equal(held.timeline.at(-1), "update:7", "the hold gave its tab back before the open began");
    assert.equal(world.fixture.timeline.at(-1), "update:7", "and the open then ran its own turn through to its own give-back");
  });

  test("falls back to a plain background page when the carrier refuses the open", async () => {

    // Chrome answers a blocked open with a null window rather than an error, so the return value is the whole of the evidence. Nothing was opened, so nothing is
    // closed and nothing is reported above debug: a page created the plain way is a working page, just not an anchored one.
    const world = makeOpenWorld();

    world.opens = false;

    const warnings = await captureWarnings(async () => {

      assert.equal(await openSharedWindowTab(world.browser, { deps: makeOpenDeps(world) }), world.createdPages[0], "the plainly created page comes back");
    });

    assert.deepEqual(world.createdOptions, [{ background: true }], "created behind whatever the window is showing, exactly as an unanchored create always was");
    assert.equal(world.opened.length, 0, "no tab was produced to close");
    assert.equal(world.fixture.timeline.includes("waitForTarget"), false, "and no target was waited for");
    assert.equal(warnings.length, 0, "a degradation with nothing left to try is a debug matter");
  });

  test("closes the tab it never found a target for, finding it by the nonce its URL carries, and falls back", async () => {

    /* The one failure path holding no page at all: the target never arrived, so the tab is looked up among the browser's pages by its nonce. A tab left behind
     * here is one nothing registered, which means the staleness sweep never judges it and it sits in the window for the life of the browser.
     */
    const world = makeOpenWorld();

    world.targetAppears = false;

    const page = await openSharedWindowTab(world.browser, { deps: makeOpenDeps(world) });

    assert.equal(world.opened.length, 1, "the open did produce a tab");
    assert.equal(world.opened[0]?.closed, true, "which was closed on the way out");
    assert.equal(world.fixture.timeline.includes("close:100"), true, "through the page the nonce scan found");
    assert.equal(page, world.createdPages[0], "and the caller receives a plainly created page instead");
  });

  test("resolves a carrier again when the one it had could not carry the open, and opens on the second pass", async () => {

    /* A carrier can close between the moment it is resolved and the moment the open is evaluated on it. That is evidence about the carrier rather than about the
     * open, so the answer is to ask for another one - and the resolve count is what tells a second resolve from a bare retry against the same page.
     */
    const world = makeOpenWorld();
    const carrier = world.carrier!;
    const carrierEvaluate = carrier.evaluate.bind(carrier);

    let firstPass = true;

    (carrier as unknown as { evaluate: (target: unknown, arg?: unknown) => Promise<unknown> }).evaluate =
      async (target: unknown, arg?: unknown): Promise<unknown> => {

        if(firstPass) {

          firstPass = false;

          throw new Error("Attempted to use detached Frame.");
        }

        return carrierEvaluate(target as never, arg as never);
      };

    const page = await openSharedWindowTab(world.browser, { deps: makeOpenDeps(world) });

    assert.equal(world.carrierResolves, 2, "the carrier was resolved again rather than reused");
    assert.equal(page, world.opened[0]?.page, "and the second pass opened the tab");
    assert.equal(world.createdOptions.length, 0, "nothing was created the plain way");
  });

  test("closes a tab that landed in the wrong window, retries once, and abandons the anchor with a warning when the second lands wrong too", async () => {

    /* The placement confirmation is the whole of what stands between a capture tab and a discovery window, so a tab it rejects is closed rather than handed over.
     * A wrong window is evidence the carrier was wrong, which earns exactly one more resolve; giving the guarantee up after that is the one abandonment an
     * operator wants to see, so it is the one reported above debug.
     */
    const world = makeOpenWorld();

    world.placements = [ false, false ];

    const warnings = await captureWarnings(async () => {

      assert.equal(await openSharedWindowTab(world.browser, { deps: makeOpenDeps(world) }), world.createdPages[0], "the plainly created page comes back");
    });

    assert.equal(world.opened.length, 2, "two opens, one per pass");
    assert.deepEqual(world.opened.map((tab) => tab.closed), [ true, true ], "and both tabs were closed rather than handed over");
    assert.equal(world.carrierResolves, 2, "the carrier was resolved once per pass");
    assert.equal(warnings.length, 1, "the abandonment is reported once");
    assert.match(warnings[0]?.message ?? "", /Chrome chose the window for it/, "naming what happened in its place");
  });

  test("keeps the tab the second pass placed correctly, and reports nothing", async () => {

    // The other half of the retry: one wrong window is no reason to give the guarantee up, and a pass that lands right hands its tab over with the selection
    // returned exactly as an uncontested open would.
    const world = makeOpenWorld();

    world.placements = [ false, true ];

    const warnings = await captureWarnings(async () => {

      assert.equal(await openSharedWindowTab(world.browser, { deps: makeOpenDeps(world) }), world.opened[1]?.page, "the second pass's tab is what comes back");
    });

    assert.equal(world.opened[0]?.closed, true, "the tab that landed in the wrong window was closed");
    assert.equal(world.opened[1]?.closed, false, "the one that landed right was kept");
    assert.equal(world.fixture.tabs.find((tab) => tab.id === 7)?.active, true, "and the tab that had the selection has it back");
    assert.equal(warnings.length, 0, "nothing was abandoned, so nothing is reported");
  });

  test("falls back at debug when the capture extension cannot be reached at all", async () => {

    // Extension trouble must not become a new way for an establishment to fail: the caller needs a page, and a page created the plain way is still a page. The
    // turn never begins, so nothing is resolved, opened, or reported.
    const world = makeOpenWorld();

    const warnings = await captureWarnings(async () => {

      const deps: SharedWindowTabDeps = {

        ...makeOpenDeps(world),
        getExtensionPage: async (): Promise<Page> => { throw new Error("The capture extension is not loaded."); }
      };

      assert.equal(await openSharedWindowTab(world.browser, { deps }), world.createdPages[0], "the plainly created page comes back");
    });

    assert.equal(world.carrierResolves, 0, "no carrier was resolved");
    assert.equal(world.fixture.timeline.length, 0, "and the extension was never spoken to");
    assert.equal(warnings.length, 0, "an unreachable extension degrades quietly");
  });

  test("stops at its ceiling, closes the tab it opened, and lets the next holder begin", async () => {

    /* A turn that lapsed and carried on could still land its tab, give the selection back, or close a tab after the queue had moved on to the next holder, which
     * is the alternating-holders race the executor exists to prevent - so this ceiling CANCELS the turn rather than detaching it the way a hold's does. The
     * target wait here ends only when the ceiling's signal fires, which is what makes this row a test of that signal reaching the wait at all.
     */
    const world = makeOpenWorld();
    const queuedFixture = makeFixture();

    world.targetHangs = true;

    const warnings = await captureWarnings(async () => {

      const opening = openSharedWindowTab(world.browser, { ceilingMs: 150, deps: makeOpenDeps(world) });
      const queued = withTabSelected(queuedFixture.page, async (): Promise<void> => undefined, { clock: makeFakeClock().clock, deps: makeDeps(queuedFixture) });

      assert.equal(await opening, world.createdPages[0], "the caller receives a plainly created page");

      await queued;
    });

    assert.equal(world.opened[0]?.closed, true, "the tab the lapsed turn opened was closed rather than left in the window");
    assert.equal(queuedFixture.timeline.at(-1), "update:7", "and the hold behind it ran its own turn to completion");
    assert.equal(warnings.length, 0, "a lapse degrades at debug like every other path with nothing left to try");
  });
});
