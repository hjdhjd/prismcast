/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tabSelection.ts: PrismCast's conversation with the capture extension about which tab its window has selected.
 *
 * Two things need a page's tab to be the selected tab of its window for a while. Starting a tab capture is one: chrome.tabCapture.capture records whichever tab is
 * selected, and the tab id the extension receives is only logged. A site's Fullscreen API request is the other: Chrome grants fullscreen to the selected tab. Both
 * needs are momentary, and both end with the user entitled to the tab they had.
 *
 * This module also announces the activations it performs. A page being captured is held visible for the whole of its capture, so a tab this module activates
 * through the extension raises no event at all inside the page it activates - not focus, not visibilitychange, not pageshow, measured 2026-08-30 across a full
 * deselect-reselect cycle. That leaves the actor as the only party able to report the fact, which is what onTabActivation subscribes to. A subscriber receives a
 * tab id and nothing else, so this module still knows about tabs and windows alone, and what a capture surface owes an activation stays the capture layer's.
 *
 * Puppeteer's own way of selecting a tab, page.bringToFront and the CDP Page.bringToFront command behind it, also activates the window at the operating-system
 * level and pulls Chrome over whatever application the user is working in. The capture extension's chrome.tabs.update selects a tab without touching the window's
 * focus, so this module speaks to the extension rather than to the page - which is also why it resolves the extension's own options page through the library's
 * resolver on every hold rather than taking a page as a parameter: video.ts consumes this primitive and must never import the library itself.
 *
 * A window has exactly one selected tab, so this module is also the one place that decides who holds it. Every selection runs through one executor, the shape
 * windowSync.ts gives the window's presentation: holds run one after another, each takes the selection, runs its caller's work, and hands the previous tab back
 * before the next hold begins, so two holders can never alternate the selection under each other. Every hold carries a ceiling, because the work a hold wraps is
 * not this module's: an extension evaluate has no timeout of its own, and one wedged body would otherwise block every later capture start and fullscreen request
 * for the life of the process, a condition no browser relaunch could clear.
 */
import type { Browser, Page, Target } from "puppeteer-core";
import { LOG, evaluateWithAbort, formatError, realClock, timeoutSignal, waitWithTimeout } from "../utils/index.ts";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import { getExtensionPage } from "puppeteer-stream";

/* The extension's options page can reach the chrome.* APIs, and this Node program cannot. Declaring the two namespaces at module scope rather than in the
 * project's global declarations keeps them spellable in exactly the one file whose evaluate callbacks execute inside that page, which is the same reason
 * tabCapture.ts declares the extension's recording entry points at its own module scope.
 */
declare const chrome: {

  tabs: {

    get(tabId: number): Promise<ExtensionTab>;
    query(query: { active?: boolean; lastFocusedWindow?: boolean; windowId?: number }): Promise<ExtensionTab[]>;
    update(tabId: number, properties: { active: boolean }): Promise<ExtensionTab>;
  };
  windows: { update(windowId: number, properties: { focused: boolean }): Promise<unknown> };
};

// Constants.

// The failure when the extension cannot name the tab a page is showing in. Both messages carry the phrase "capture extension" on purpose: streaming/recovery.ts
// classifies a capture-start failure as capture infrastructure by substring, which is what earns a 503 back-off and a re-verification of the browser rather than a
// 500 the client would not retry.
export const TAB_NOT_FOUND_MESSAGE = "The capture extension could not find the page's tab.";

// The failure when Chrome did not honor the selection. Neither a capture start nor a fullscreen request can proceed against a tab that is not the selected one.
export const TAB_NOT_SELECTED_MESSAGE = "The capture extension did not select the page's tab.";

// How long an open waits for the tab it asked for to show up as a target. Chrome has already been told to open it, so this bounds an event arriving rather than
// any work: an open with no target by now is one that produced none.
const OPEN_TARGET_WAIT_MS = 3000;

// The ceiling on a whole open turn, its one retry included. It sits under the deadline a capture start already runs under, so a turn that lapses still leaves the
// caller that is waiting on it time to get a page the plain way.
const OPEN_TURN_CEILING_MS = 8000;

// How many times a turn resolves a carrier and opens against it. The second pass is the retry a wrong-window tab and a carrier that closed under the evaluate
// both earn: each is evidence about the carrier rather than about the open, so resolving again is the only thing worth doing differently.
const OPEN_ATTEMPTS = 2;

// Types.

// A tab as the extension reports it. Every field is optional in Chrome's own API, so a read is narrowed before anything depends on it.
interface ExtensionTab {

  readonly active?: boolean;
  readonly id?: number;
  readonly title?: string;
  readonly url?: string;
  readonly windowId?: number;
}

/**
 * The library value this module talks to, injected so a test can drive a whole hold without a browser.
 */
export interface TabSelectionDeps {

  readonly getExtensionPage: typeof getExtensionPage;
}

/**
 * The collaborators an open runs through: the window topology, and the library lookup this module already talks to.
 *
 * A sibling of TabSelectionDeps rather than an extension of it, because TabSelectionDeps has to stay satisfiable by its own default for the three consumers that
 * take it that way. The two topology functions answer questions about CDP's window table, which this module structurally cannot ask - it speaks to the capture
 * extension and to nothing else - so they carry no default at all and arrive from a call site that can see both modules.
 */
export interface SharedWindowTabDeps {

  // Reports whether a page ended up in the shared window. True when no identity was ever recorded, false when the page's window cannot be read.
  readonly confirmPlacement: (page: Page) => Promise<boolean>;

  // The library's lookup for the extension's options page. Defaults to the real one.
  readonly getExtensionPage?: typeof getExtensionPage;

  // Picks the page the open is evaluated on, or null when the browser has none that qualifies.
  readonly resolveCarrier: (browser: Browser) => Promise<Nullable<Page>>;
}

/**
 * Per-call context for an open: how long the whole turn may take, and the collaborators it runs through.
 */
export interface OpenSharedWindowTabContext {

  // The ceiling on the whole turn. Defaults to this module's own, which sits under the deadline a capture start runs under.
  readonly ceilingMs?: number;

  // The topology collaborators and the library lookup.
  readonly deps: SharedWindowTabDeps;
}

/**
 * The selected tab handed to a hold's body: what it is, and the way to be sure of it again at a moment of the body's own choosing.
 */
export interface SelectedTab {

  readonly id: number;

  // Re-takes the selection: re-selects this page's tab if something deselected it during the hold, and re-focuses its window if another window took the focus. A
  // body whose work spans several round trips calls this before each one rather than trusting a selection taken at the start.
  readonly reassert: () => Promise<void>;

  readonly url: Nullable<string>;
  readonly windowId: number;
}

/**
 * Per-call context for a hold: how long the selection may be held, the clock that bound runs on, and the collaborators it talks through.
 */
export interface WithTabSelectedContext {

  // The ceiling on this hold. Defaults to the deadline a capture start already runs under, so the default hold cannot outlive the caller waiting on it.
  readonly ceilingMs?: number;

  // The time port the ceiling runs on. Defaults to realClock; tests inject a fake.
  readonly clock?: Clock;

  // The library collaborators. Defaults to the real ones.
  readonly deps?: TabSelectionDeps;
}

// The production collaborators.
export const defaultTabSelectionDeps: TabSelectionDeps = { getExtensionPage };

/* The tab id each page was identified as, cached for the page's life. A tab id survives navigation, so one identification per page is enough - while the window
 * that tab sits in can change under us at any time (a user can drag a tab into another window), which is why the window is read fresh at every hold and never
 * cached beside the id.
 */
const tabIds = new WeakMap<Page, number>();

// The counter behind each identification's title token. It only moves forward, so no two identifications in this process can match one another's marker.
let nextTitleToken = 0;

// The counter behind each open's nonce. It only moves forward for the same reason the title token's does: two opens must never be able to wait for one another's
// tab, and the nonce is the whole of what tells them apart.
let nextOpenNonce = 0;

// The tail of the selection chain. Each hold is queued onto it and replaces it with its own release signal, so the next hold begins when the selection was handed
// back rather than when the caller before it finished reading its result.
let selectionQueue: Promise<void> = Promise.resolve();

// The subscribers the activation report walks. Module-level rather than per-hold, because the fact reported is about the browser's tabs rather than about any one
// hold, and the capture layer subscribes once for the life of the process.
const activationListeners = new Set<(tabId: number) => void>();

// Activation reporting.

/**
 * Subscribes to the activation report: every tab this module activates through the extension, announced by id as soon as the update resolves.
 *
 * A captured page cannot hear its own activation, so the module performing the activation is the one place the fact exists to be announced at all. The report
 * carries the id and stops there, which is what keeps this module's vocabulary to tabs and windows no matter what a subscriber goes on to do with one.
 * @param listener - Called with the id of each tab this module activates.
 * @returns The closure that ends the subscription. A subscriber that has gone away without calling it stays in the set for the life of the process.
 */
export function onTabActivation(listener: (tabId: number) => void): () => void {

  activationListeners.add(listener);

  return (): void => {

    activationListeners.delete(listener);
  };
}

/**
 * Announces an activation to every subscriber.
 *
 * Each listener is called inside a swallow of its own, because a subscriber is somebody else's code and a hold's correctness cannot rest on it: a throw would
 * otherwise fail the selection step a capture start is waiting on, or replace the give-back's own reading of what went wrong with a subscriber's.
 * @param tabId - The tab that was activated.
 */
function reportActivation(tabId: number): void {

  for(const listener of activationListeners) {

    try {

      listener(tabId);
    } catch(error) {

      LOG.debug("browser:lifecycle", "A tab-activation subscriber failed to handle the report: %s.", formatError(error));
    }
  }
}

/**
 * The tab id a page was identified as, when it has one.
 *
 * The report names a tab and a subscriber holds pages, so this is the bridge between them. The answer is only ever an id a completed identification cached, which
 * means a page this module has never selected reads undefined - the right answer for it, because no activation of ours can have named it.
 * @param page - The page to look up.
 * @returns The cached tab id, or undefined when this page has not been identified.
 */
export function getCachedTabId(page: Page): number | undefined {

  return tabIds.get(page);
}

// Selection.

/**
 * Reads a tab's current state from the extension.
 * @param extension - The extension's options page.
 * @param tabId - The tab to read.
 * @returns The tab as the extension reports it.
 */
async function readTab(extension: Page, tabId: number): Promise<ExtensionTab> {

  return extension.evaluate((id: number) => chrome.tabs.get(id), tabId);
}

/**
 * Names the tab a page is showing in, caching the answer for the page's life.
 *
 * The extension cannot see CDP target ids, and a capture page is a fresh about:blank when it is first selected - its capture start precedes its navigation - so
 * there is nothing on the page to match against. A unique token written into the page's own title is something both sides can see: the page sets it, the extension
 * finds the tab wearing it, and the title it replaced goes back in a finally so a navigated page is never left carrying the marker.
 * @param page - The page to identify.
 * @param extension - The extension's options page.
 * @returns The page's tab id.
 * @throws When no tab wears the token, or the tab that does carries no id.
 */
async function identifyTab(page: Page, extension: Page): Promise<number> {

  const cached = tabIds.get(page);

  if(cached !== undefined) {

    return cached;
  }

  const token = "prismcast-tab-" + String(++nextTitleToken);

  const replaced = await page.evaluate((marker: string): string => {

    const previousTitle = document.title;

    document.title = marker;

    return previousTitle;
  }, token);

  try {

    const tabs = await extension.evaluate(() => chrome.tabs.query({}));
    const match = tabs.find((tab) => tab.title === token);

    if(match?.id === undefined) {

      throw new Error(TAB_NOT_FOUND_MESSAGE);
    }

    tabIds.set(page, match.id);

    return match.id;
  } finally {

    try {

      await page.evaluate((title: string): void => { document.title = title; }, replaced);
    } catch(error) {

      // Restoring the title is housekeeping, and a page that has gone away mid-identification has taken its title with it. Letting this reach the caller would
      // replace the identification's own verdict with a note about the cleanup.
      LOG.debug("browser:lifecycle", "The page's title could not be restored after identifying its tab: %s.", formatError(error));
    }
  }
}

/**
 * Hands the selection back to the tab that had it before this module took it.
 *
 * Three readings, three answers: the tab this module selected is still the selected one, so the tab that was selected before gets it back; something else is
 * selected, so whoever moved it since - a user clicking a tab, login mode opening its page - chose more recently and their choice stands; or the tab cannot be
 * read at all, which means it has gone and nothing of ours is holding the selection either way. Every path a selection is taken on comes back through here, so
 * the hold and the open give the tab back on identical terms.
 * @param extension - The extension's options page.
 * @param selectedId - The tab this module selected.
 * @param previousId - The tab that was selected before, when the window had one.
 */
async function returnSelection(extension: Page, selectedId: number, previousId: number | undefined): Promise<void> {

  if((previousId === undefined) || (previousId === selectedId)) {

    return;
  }

  let stillSelected = true;

  try {

    stillSelected = (await readTab(extension, selectedId)).active === true;
  } catch(error) {

    LOG.debug("browser:lifecycle", "The capture extension could not re-read the tab before returning the selection: %s.", formatError(error));
  }

  if(!stillSelected) {

    return;
  }

  try {

    await extension.evaluate((target: number) => chrome.tabs.update(target, { active: true }), previousId);

    // The tab handed the selection back can be another stream's capture tab, which is the activation the report matters most for. It sits inside the try so an
    // update that never happened is never announced.
    reportActivation(previousId);
  } catch(error) {

    // The tab that was selected before may have closed while the selection was held, and there is nothing else to return the selection to.
    LOG.debug("browser:lifecycle", "The capture extension could not return the selection to the tab that had it: %s.", formatError(error));
  }
}

/**
 * Runs a body with a page's tab selected in its window, and hands back the tab that was selected before it afterwards.
 *
 * Holds are serialized: this queues onto the chain, and the hold that follows begins when this one has given the selection back - not when this caller has
 * finished reading its result, which is what lets a body that outlives its ceiling keep running without holding the selection or the queue.
 * @param page - The page whose tab the body needs selected.
 * @param body - The work to run while the tab is selected. It receives the selected tab.
 * @param context - The ceiling, clock, and collaborators. Defaults to the capture start's own deadline on the real clock with the production collaborators.
 * @returns Whatever the body resolves with, even when the hold's ceiling lapsed first.
 * @throws The body's own rejection, or a selection failure of this module's own.
 */
export async function withTabSelected<T>(page: Page, body: (tab: SelectedTab) => Promise<T>, context: WithTabSelectedContext = {}): Promise<T> {

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const released = Promise.withResolvers<void>();

  /* The chain advances on the release signal rather than on this call's own promise. Those are different moments whenever a body outlives the ceiling: chaining
   * the queue on the caller's promise would either hold the selection until the body finally settled, or hand the caller a result the body never produced.
   * hold() settles the release in a finally on every path, including the paths where the selection was never taken, so the chain cannot wedge on a failure.
   */
  const run = selectionQueue.then(async () => hold(page, body, context, released));

  selectionQueue = released.promise;

  return run;
}

/**
 * Takes the selection, runs the body under it, and gives the previous tab back.
 * @param page - The page whose tab is selected.
 * @param body - The work to run while it is selected.
 * @param context - The ceiling, clock, and collaborators.
 * @param released - The signal the executor chains the next hold on, settled as soon as the selection has been handed back.
 * @returns Whatever the body resolves with.
 */
async function hold<T>(page: Page, body: (tab: SelectedTab) => Promise<T>, context: WithTabSelectedContext,
  released: PromiseWithResolvers<void>): Promise<T> {

  const { ceilingMs = CONFIG.streaming.navigationTimeout, clock = realClock, deps = defaultTabSelectionDeps } = context;

  try {

    const extension = await deps.getExtensionPage(page.browser());
    const id = await identifyTab(page, extension);
    const tab = await readTab(extension, id);

    // The window is read fresh on every hold, and the fields the selection depends on are narrowed here rather than asserted downstream, so SelectedTab's
    // required fields are earned by a read that actually carried them.
    if((tab.id === undefined) || (tab.windowId === undefined)) {

      throw new Error(TAB_NOT_FOUND_MESSAGE);
    }

    const windowId = tab.windowId;
    const [previous] = await extension.evaluate((target: number) => chrome.tabs.query({ active: true, windowId: target }), windowId);
    const previousId = previous?.id;

    // Whether this hold has already reported another window holding the focus. The correction itself runs as often as it is needed - the selection step and every
    // re-assert perform it - while one report is what an operator needs to see per hold.
    let focusReported = false;

    /* Makes this page's window the one Chrome treats as current, and only then. chrome.tabCapture.capture records the active tab of the LAST-FOCUSED window, so a
     * tab that is merely active in a window Chrome does not consider current is not selected in the sense either caller needs - a site popup holding the focus
     * would have its own tab recorded instead. This is the one deliberate raise in the module, taken only when the read names a tab in another window.
     */
    const focusWindow = async (): Promise<void> => {

      const [focused] = await extension.evaluate(() => chrome.tabs.query({ active: true, lastFocusedWindow: true }));

      if((focused?.id === undefined) || (focused.windowId === undefined) || (focused.windowId === windowId)) {

        return;
      }

      if(!focusReported) {

        focusReported = true;

        LOG.warn("Another browser window held the focus while a tab was selected for capture, so PrismCast brought its own window forward.",
          { focusedTab: focused.url ?? null });
      }

      await extension.evaluate((target: number) => chrome.windows.update(target, { focused: true }), windowId);
    };

    // Selects this page's tab and confirms Chrome honored it. A selection Chrome did not take is a failure to report rather than one to run a capture start or a
    // fullscreen request against.
    const selectTab = async (): Promise<void> => {

      await extension.evaluate((target: number) => chrome.tabs.update(target, { active: true }), id);

      // Reported ahead of the confirmation rather than behind it, because the update is what Chrome acted on: a selection that then fails its confirmation was
      // still an activation, and a heal fired for it is a re-issue against a surface that already carries the right values.
      reportActivation(id);

      const confirmed = await readTab(extension, id);

      if(confirmed.active !== true) {

        throw new Error(TAB_NOT_SELECTED_MESSAGE);
      }
    };

    try {

      await selectTab();
      await focusWindow();

      const selected: SelectedTab = {

        id,
        reassert: async (): Promise<void> => {

          if((await readTab(extension, id)).active !== true) {

            await selectTab();
          }

          await focusWindow();
        },
        url: tab.url ?? null,
        windowId
      };

      /* The ceiling is raced against the body rather than enforced by cancelling it, because the body belongs to the caller and this module has no way to stop it.
       * The lapse error is this hold's own object, so a lapse is told from the body's own rejection by identity rather than by message.
       */
      const lapse = new Error("The tab selection was held longer than " + String(ceilingMs) + " ms.");
      const running = body(selected);

      try {

        return await clock.waitWithTimeout(running, ceilingMs, lapse);
      } catch(error) {

        if(error !== lapse) {

          throw error;
        }

        LOG.warn("A tab selection was held past its ceiling, so the selection was returned while the operation that took it continues.", { ceilingMs });

        /* The body keeps running detached and its result still reaches the caller...what ends here is the hold, so the next capture start or fullscreen request
         * is not queued behind work this module cannot bound. The promise is returned unawaited on purpose: awaiting it here would put the wait INSIDE the try,
         * so the give-back in the finally would sit behind the very body the ceiling just gave up on, and the selection would be held for exactly as long as if
         * there were no ceiling at all.
         */
        // eslint-disable-next-line @typescript-eslint/return-await -- Awaiting here would defer the give-back until the body settles, defeating the ceiling.
        return running;
      }
    } finally {

      await returnSelection(extension, id, previousId);
    }
  } finally {

    released.resolve();
  }
}

// Opening.

/**
 * Opens a page as a tab of the shared browser window, and hands the selection straight back to the tab that had it.
 *
 * The open belongs here because the open IS a selection change. Chrome picks the window for a plain background page and skips a minimized one, so a capture tab
 * created while the shared window rests minimized beside a discovery window takes root in the discovery window instead and keeps that window alive long past the
 * walk it was opened for. The one anchor Chrome honors in every window state is the opener relationship: a window.open evaluated on a page of the shared window
 * opens its tab in that window, minimized or not, and restores the window on the way in (measured 2026-08-31, both window states).
 *
 * Running it as a turn on this module's executor closes two races rather than one. chrome.tabCapture records whichever tab is selected and page creation runs
 * outside the capture lock, so a bare open could land between a concurrent stream's re-assert and its recording start and put a blank tab on that stream's
 * recording; and two overlapping opens on one page silently lose all but the first, because a page carries a single transient activation. Serialized turns make
 * both unrepresentable rather than unlikely.
 *
 * Two id spaces meet at this call and are never compared. The window identity a placement is confirmed against belongs to CDP's table, and this module never sees
 * it - it arrives as an injected answer of true or false. Every id this module reads or writes itself, the tab that was selected and the tab that gets it back,
 * is the capture extension's.
 *
 * Three side effects belong to somebody else. The tab arrives selected, which is why the selection goes back inside this same turn. The open restores a minimized
 * window, which the window-visibility policy settles afterwards. And the give-back's activation report re-affirms whichever tab regains the selection, so when
 * that is another stream's capture page its composition wobbles for about a second until the re-affirm ladder lands - the same self-healing wobble a capture
 * start already causes once, now possible a second time at page creation.
 * @param browser - The browser to open the tab in.
 * @param context - The turn's ceiling and the collaborators it runs through.
 * @returns The opened page, or a page created the plain way when the anchor could not be had.
 */
export async function openSharedWindowTab(browser: Browser, context: OpenSharedWindowTabContext): Promise<Page> {

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const released = Promise.withResolvers<void>();

  // The open queues onto the same chain a hold does, and the chain advances on the release signal for the same reason: the next holder begins when this turn has
  // genuinely finished with the selection.
  const run = selectionQueue.then(async () => openTurn(browser, context, released));

  selectionQueue = released.promise;

  return run;
}

/**
 * Opens the tab under the selection, gives the selection back, and settles - degrading to a plain background page whenever the anchor cannot be had.
 * @param browser - The browser to open the tab in.
 * @param context - The turn's ceiling and collaborators.
 * @param released - The signal the executor chains the next turn on, settled once this turn has genuinely finished.
 * @returns The page, opened as a tab of the shared window or created plainly.
 */
async function openTurn(browser: Browser, context: OpenSharedWindowTabContext, released: PromiseWithResolvers<void>): Promise<Page> {

  const { ceilingMs = OPEN_TURN_CEILING_MS, deps } = context;
  const { confirmPlacement, getExtensionPage: lookup = getExtensionPage, resolveCarrier } = deps;

  // The fragment the opened tab wears, and the whole of what tells it from any other tab. Chrome reports a fragment on the target verbatim, so the target that
  // turns up wearing this one is this turn's and nobody else's.
  const nonce = "prismcast-open-" + String(++nextOpenNonce);
  const nonceUrl = "about:blank#" + nonce;

  /* The turn's one ceiling. It is expressed as a signal because that is the form the target wait takes, and it reaches every other await through the bound below.
   * It CANCELS rather than detaching, which is the opposite of what a hold's ceiling does and deliberately so: a hold's body belongs to its caller and is left to
   * finish, while a detached open could still land its tab, give the selection back, or close a tab after the queue had already moved on to the next holder -
   * the alternating-holders race this executor exists to make unrepresentable.
   */
  const lapse = new Error("The capture extension's window did not take a new tab within " + String(ceilingMs) + " ms.");
  const deadline = realClock.now() + ceilingMs;
  const ceiling = timeoutSignal(ceilingMs, lapse);
  const bounded = async <T>(work: Promise<T>): Promise<T> => waitWithTimeout(work, Math.max(1, deadline - realClock.now()), lapse);

  // The tab this turn opened, for as long as there is one that might still need closing.
  let opened: Nullable<Page> = null;

  /* Closes the tab an abandoned open produced. The target-timeout path holds no page at all, so the tab is looked up among the browser's pages by the nonce its
   * URL still carries: a tab left behind here is one nothing registered, which means the staleness sweep never judges it and it would sit in the window for the
   * life of the browser.
   */
  const closeOpenedTab = async (): Promise<void> => {

    const tab = opened ?? (await browser.pages()).find((page) => page.url().includes(nonce)) ?? null;

    opened = null;

    if(!tab || tab.isClosed()) {

      return;
    }

    try {

      await tab.close();
    } catch(error) {

      LOG.debug("browser:lifecycle", "The tab an abandoned open produced could not be closed: %s.", formatError(error));
    }
  };

  /* Creates the page the plain way, which is where every degradation ends: no carrier, an open Chrome refused, a spent retry, a lapsed turn, or an extension this
   * module could not reach at all. That last one is why the fallback exists rather than a throw - extension trouble must not become a new way for an
   * establishment to fail, because the page still has to exist either way. Giving the guarantee up after the retry was spent is the one case an operator wants to
   * see, so it is the one case reported above debug.
   */
  const fallback = async (reason: string, retriesSpent: boolean): Promise<Page> => {

    if(retriesSpent) {

      LOG.warn("A capture tab could not be opened in PrismCast's own browser window, so Chrome chose the window for it.", { reason });
    } else {

      LOG.debug("browser:lifecycle", "A capture tab was created without the shared window's opener anchor: %s.", reason);
    }

    return browser.newPage({ background: true });
  };

  try {

    const extension = await bounded(lookup(browser));

    /* Why the anchor was given up, when it was given up before both passes were spent. A reason set inside the loop is a degradation with nothing left to try, so
     * it reports at debug; falling out of the loop with none set means the carrier was resolved twice and the tab still did not land in the shared window, which
     * is the one abandonment an operator wants to see.
     */
    let declined: Nullable<string> = null;

    for(let attempt = 1; attempt <= OPEN_ATTEMPTS; attempt++) {

      // eslint-disable-next-line no-await-in-loop -- Each pass is a fresh resolve and open, and the second only runs because the first told us the carrier was wrong.
      const carrier = await bounded(resolveCarrier(browser));

      if(!carrier) {

        declined = "no page of the shared window was available to open from";

        break;
      }

      let previousId: number | undefined;

      try {

        /* The window's own selected tab, read exactly the way a hold reads it: the carrier names its tab, the tab names its window, and the window names the tab
         * that will want the selection back. Every id in this block is the capture extension's, which is the only table this module is ever allowed to reason in.
         */
        // eslint-disable-next-line no-await-in-loop -- The carrier read belongs to the pass that resolved it.
        const carrierTab = await bounded(readTab(extension, await bounded(identifyTab(carrier, extension))));

        if(carrierTab.windowId === undefined) {

          throw new Error(TAB_NOT_FOUND_MESSAGE);
        }

        // eslint-disable-next-line no-await-in-loop -- The previous selection has to be read before this pass's open, not before the loop.
        const [previous] = await bounded(extension.evaluate((target: number) => chrome.tabs.query({ active: true, windowId: target }), carrierTab.windowId));

        previousId = previous?.id;

        // A blocked open answers with a null window rather than throwing, so the return value is read rather than assumed. Chrome runs with popup blocking off,
        // which leaves a null answer as evidence about the carrier's own state.
        // eslint-disable-next-line no-await-in-loop -- The open is the pass.
        if(!(await bounded(evaluateWithAbort(carrier, (target: string): boolean => window.open(target, "_blank") !== null, [nonceUrl])))) {

          declined = "the page the open was evaluated on refused it";

          break;
        }
      } catch(error) {

        if(error === lapse) {

          throw error;
        }

        // A carrier can close between the moment it was resolved and the moment the open is evaluated on it, which is the one failure another resolve can answer.
        LOG.debug("browser:lifecycle", "The page an open was to be evaluated on could not carry it: %s.", formatError(error));

        continue;
      }

      // eslint-disable-next-line no-await-in-loop -- The tab this pass opened is what this pass then confirms.
      const target = await browser.waitForTarget((candidate: Target): boolean => candidate.url() === nonceUrl,
        { signal: ceiling.signal, timeout: OPEN_TARGET_WAIT_MS });

      // eslint-disable-next-line no-await-in-loop -- Same pass, same tab.
      const page = await bounded(target.page());

      if(!page) {

        throw new Error("The capture extension's window produced a tab with no page behind it.");
      }

      opened = page;

      // eslint-disable-next-line no-await-in-loop -- The confirmation is what decides whether this pass was the last one.
      if(!(await bounded(confirmPlacement(page)))) {

        // eslint-disable-next-line no-await-in-loop -- A tab in the wrong window is closed before the next pass opens another.
        await closeOpenedTab();

        continue;
      }

      /* The tab arrived selected, so the selection goes back inside this same turn rather than being left for the capture start that follows. Identifying the tab
       * first is what makes the give-back's three readings exact, and it is work the capture start would otherwise do moments later against this same page.
       */
      // eslint-disable-next-line no-await-in-loop -- The give-back closes the pass that took the selection.
      await bounded(returnSelection(extension, await bounded(identifyTab(page, extension)), previousId));

      opened = null;

      return page;
    }

    await closeOpenedTab();

    return await fallback(declined ?? "the tab did not open in PrismCast's own browser window", declined === null);
  } catch(error) {

    await closeOpenedTab();

    return await fallback((error === lapse) ? lapse.message : formatError(error), false);
  } finally {

    ceiling.cancel();
    released.resolve();
  }
}
