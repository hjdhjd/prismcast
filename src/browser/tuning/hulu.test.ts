/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hulu.test.ts: Unit tests for the Hulu Live TV provider module. The guide is virtualized, so the strategy drives a scroll-and-read loop rather than one big
 * read - which is what makes a page stub worth building here: it serves each evaluate by the source of the function handed to it, and every scroll the strategy
 * asks for is recorded. Those scroll counts are the instrument. A warm tune that scrolls once went straight to the row it remembered; a warm tune that scrolls
 * more than once fell back to searching for it.
 *
 * The describes split by behavior surface rather than by export, following the module's single provider object: the warm-cache shortcut, and the fast-path
 * resolution that can end a cold tune before it ever reaches the click.
 *
 * What the shortcut has to get right is row recognition. It scrolls to a row number it cached, reads what rendered there, and has to decide whether the channel
 * it wants is on screen - and the name it asked for is frequently not the name that row displays. A local affiliate is filed under its network name and renders
 * as its call sign; a channel whose guide name differs from the user's channelSelector by punctuation is filed under the guide's spelling and found through a
 * fuzzy key. Both cases are one cache entry reachable by two names, so entry identity is what recognizes the row, and both are asserted below.
 */
import type { ChannelSelectionProfile, Nullable } from "../../types/index.ts";
import { after, afterEach, before, beforeEach, describe, test } from "node:test";
import { initDebugFilter, subscribeToLogs } from "../../utils/index.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { huluProvider } from "./hulu.ts";
import { makeProfile } from "../../config/profiles.helpers.ts";

// One guide row as readRenderedChannels reads it out of the DOM: the lowercased data-testid name it matches on, the original-cased display name, and the
// zero-based row number recovered from the row's screen-reader text.
interface RenderedRow {

  readonly displayName: string;
  readonly name: string;
  readonly rowNumber: number;
}

interface GuidePage {

  // Every coordinate pair clicked, in order.
  readonly clicks: { x: number; y: number }[];

  // Every channel name the strategy asked to locate an on-now cell for, in order. The last one is the row it actually clicked.
  readonly locates: string[];

  // How many times the held playlist was released, which the click path does and a resolved fast path does not.
  readonly releases: () => number;

  // How many times the strategy asked the in-page interceptor whether it had already resolved the tune itself.
  readonly resolveChecks: () => number;

  // Every scroll offset the strategy asked for, in order. Its length is the probe count that separates a shortcut from a search.
  readonly scrolls: number[];

  // Swaps the rows the guide renders, standing in for a different market's lineup.
  readonly setLineup: (rows: RenderedRow[]) => void;

  // Makes the in-page interceptor report that it resolved the tune on its own, which is the fast path a cold tune can end on.
  readonly setSelfResolved: (resolved: boolean) => void;

  // The Page-shaped stub to hand the provider.
  readonly stub: Page;
}

// A guide tall enough to search but short enough to read: the first probe lands on row 9 and the whole lineup renders in one window there.
const TOTAL_ROWS = 20;

/* An affiliate market. The call sign occupies the DOM position where its network name would sort, which is exactly what position inference reads: "abc" sorts
 * after "aaa" and before "zulu", and the only call-sign-shaped row between those two anchors is the affiliate.
 */
const AFFILIATE_LINEUP: RenderedRow[] = [

  { displayName: "AAA Network", name: "aaa", rowNumber: 4 },
  { displayName: "WABC", name: "wabc", rowNumber: 5 },
  { displayName: "Zulu", name: "zulu", rowNumber: 6 }
];

// A lineup carrying a channel whose guide spelling differs from the channelSelector a user would write for it only by punctuation and spacing.
const PUNCTUATED_LINEUP: RenderedRow[] = [

  { displayName: "AAA Network", name: "aaa", rowNumber: 4 },
  { displayName: "C-SPAN 3", name: "c-span 3", rowNumber: 5 },
  { displayName: "Zulu", name: "zulu", rowNumber: 6 }
];

/* makeGuidePage returns a Page-shaped stub for the surfaces guideGridStrategy touches. Every evaluate is dispatched by the source of the page function it was
 * handed, because the strategy's reads are distinguished by what they look for in the DOM rather than by any argument: a marker unique to each read picks the
 * canned answer for it. An evaluate whose source matches nothing throws rather than returning undefined, so a read this stub does not know about surfaces as a
 * failure naming itself instead of as a mystery further down the strategy.
 */
function makeGuidePage(rows: RenderedRow[] = []): GuidePage {

  const clicks: { x: number; y: number }[] = [];
  const locates: string[] = [];
  const scrolls: number[] = [];

  let lineup = rows;
  let releases = 0;
  let resolveChecks = 0;
  let selfResolved = false;

  const evaluate = async (pageFunction: unknown, ...args: unknown[]): Promise<unknown> => {

    const source = String(pageFunction);

    await Promise.resolve();

    // The profile-selector probe, answered as an account that was never prompted.
    if(source.includes("ProfileSelectorModal")) {

      return { present: false };
    }

    // The grid metadata read. A 100px row height makes every scroll offset the row index times a round number.
    if(source.includes("spacerHeight")) {

      return { gridDocTop: 0, rowHeight: 100, totalRows: TOTAL_ROWS };
    }

    // The scroll that brings a row range into the render window.
    if(source.includes("documentElement.scrollTop")) {

      scrolls.push(args[0] as number);

      return undefined;
    }

    // The rendered-row read. Its screen-reader lookup is what distinguishes it from the other reads over the same containers.
    if(source.includes("sr-only")) {

      return lineup;
    }

    // The fast path's two mechanisms: injecting a captured channel into the held playlist request, and asking whether the interceptor got there first.
    if(source.includes("__prismcastResolveDirectTune")) {

      return false;
    }

    if(source.includes("__prismcastIsDirectTuneResolved")) {

      resolveChecks++;

      return selfResolved;
    }

    if(source.includes("__prismcastReleasePlaylist")) {

      releases++;

      return undefined;
    }

    // The post-failure click diagnostics, which only run once every click attempt has been spent.
    if(source.includes("elementsFromPoint")) {

      return { elementStack: [], hydrated: true, onNowFound: true, pageAge: 0 };
    }

    // The on-now cell lookup, keyed by the name the strategy decided to click.
    if(source.includes("LiveGuideProgram--first")) {

      locates.push(String(args[0]));

      return { x: 10, y: 20 };
    }

    throw new Error("Unserved evaluate shape: " + source.slice(0, 160));
  };

  const stub = {

    $eval: async (): Promise<undefined> => {

      await Promise.resolve();

      return undefined;
    },
    evaluate,
    keyboard: {

      press: async (): Promise<undefined> => {

        await Promise.resolve();

        return undefined;
      }
    },
    mouse: {

      click: async (x: number, y: number): Promise<undefined> => {

        clicks.push({ x, y });

        await Promise.resolve();

        return undefined;
      }
    },
    waitForSelector: async (): Promise<null> => {

      await Promise.resolve();

      return null;
    }
  } as unknown as Page;

  return {

    clicks,
    locates,
    releases: (): number => releases,
    resolveChecks: (): number => resolveChecks,
    scrolls,
    setLineup: (next: RenderedRow[]): void => {

      lineup = next;
    },
    setSelfResolved: (resolved: boolean): void => {

      selfResolved = resolved;
    },
    stub
  };
}

// makeHuluProfile narrows a neutral profile to what the strategy requires. It carries no play selector, so the tune completes on the on-now cell click rather
// than waiting for a play button that this fixture's page would never render.
function makeHuluProfile(channelSelector: string): ChannelSelectionProfile {

  return makeProfile({ channelSelection: { listSelector: "#CHANNELS", strategy: "guideGrid" }, channelSelector }) as ChannelSelectionProfile;
}

// Every log line emitted during a test, captured off the same emitter the web UI's log stream reads.
let captured: string[] = [];
let unsubscribe: Nullable<() => void> = null;

before(() => {

  // The shortcut announces itself only at debug level, so the category has to be on for the branch to be observable at all.
  initDebugFilter("tuning:hulu");
});

after(() => {

  initDebugFilter("");
});

beforeEach(() => {

  captured = [];
  unsubscribe = subscribeToLogs((entry) => { captured.push(entry.message); });
});

afterEach(() => {

  unsubscribe?.();
  unsubscribe = null;

  // The channel cache is module state, so every test starts from the empty cache a browser restart produces.
  huluProvider.strategy.clearCache?.();
});

describe("warm guide cache shortcut", () => {

  test("scrolls straight to a local affiliate's row and clicks its call sign", async () => {

    /* The first tune has to find the affiliate the hard way: the search converges on the window where "abc" would sort, position inference names the call sign
     * sitting there, and the cross-reference files the network name against the affiliate's own cache entry. That alias is what the second tune resolves, and
     * the row it lands on renders as "WABC" rather than as anything named "abc" - so recognizing it is a question about which entry the row belongs to.
     */
    const guide = makeGuidePage(AFFILIATE_LINEUP);

    const cold = await huluProvider.strategy.execute(guide.stub, makeHuluProfile("abc"));

    assert.equal(cold.success, true, "the cold tune finds the affiliate through position inference");
    assert.equal(guide.scrolls.length, 1, "the first tune converges on the affiliate's window in a single probe");

    const coldScrolls = guide.scrolls.length;

    const warm = await huluProvider.strategy.execute(guide.stub, makeHuluProfile("abc"));

    assert.ok(captured.some((message) => message.includes("Guide cache hit")), "the second tune enters the shortcut rather than searching from cold");
    assert.equal(guide.scrolls.length - coldScrolls, 1, "the shortcut scrolls once to the remembered row and stops there");
    assert.equal(warm.success, true, "the warm tune succeeds");
    assert.equal(guide.locates.at(-1), "wabc", "the row that was clicked is the affiliate's call sign, which is what the guide actually renders");
  });

  test("scrolls straight to a row whose guide spelling differs from the channel selector", async () => {

    /* The other way one entry answers to two names: a channelSelector written without the guide's spacing resolves through the fuzzy key lookup. The entry the
     * shortcut trusted is filed under the guide's spelling, so the rendered row never carries the name that was asked for.
     */
    const guide = makeGuidePage(PUNCTUATED_LINEUP);

    const seed = await huluProvider.strategy.execute(guide.stub, makeHuluProfile("C-SPAN 3"));

    assert.equal(seed.success, true, "tuning by the guide's own spelling caches the row");

    const seedScrolls = guide.scrolls.length;

    const warm = await huluProvider.strategy.execute(guide.stub, makeHuluProfile("C-SPAN3"));

    assert.ok(captured.some((message) => message.includes("Guide cache hit")), "the fuzzy key resolves to the cached entry and enters the shortcut");
    assert.equal(guide.scrolls.length - seedScrolls, 1, "the shortcut scrolls once to the remembered row and stops there");
    assert.equal(warm.success, true, "the warm tune succeeds");
    assert.equal(guide.locates.at(-1), "c-span 3", "the row that was clicked is the one the guide renders, not the spelling the request used");
  });

  test("clicks the requested name when the row renders under it", async () => {

    // The ordinary case, where the cached entry's key and the rendered row's name are the same string. Nothing about recognizing an aliased row may change what
    // this one does.
    const guide = makeGuidePage(AFFILIATE_LINEUP);

    await huluProvider.strategy.execute(guide.stub, makeHuluProfile("zulu"));

    const coldScrolls = guide.scrolls.length;

    const warm = await huluProvider.strategy.execute(guide.stub, makeHuluProfile("zulu"));

    assert.equal(warm.success, true, "the warm tune succeeds");
    assert.equal(guide.scrolls.length - coldScrolls, 1, "the shortcut scrolls once to the remembered row");
    assert.equal(guide.locates.at(-1), "zulu", "the row clicked is the one that was asked for");
  });
});

describe("cold tune fast path", () => {

  test("returns as soon as the interceptor resolves the tune, skipping the release and the click", async () => {

    /* When the in-page interceptor has already swapped the playlist request for the inferred affiliate, the tune is done: there is nothing left to release, no
     * second resolution to attempt, and no cell to click. Each of those three is asserted by its absence, because a resolution that fell through to them would
     * still report success while doing the work twice.
     */
    const guide = makeGuidePage(AFFILIATE_LINEUP);

    guide.setSelfResolved(true);

    const result = await huluProvider.strategy.execute(guide.stub, makeHuluProfile("abc"));

    assert.equal(result.success, true, "the resolved fast path is the tune's result");
    assert.equal(guide.resolveChecks(), 1, "the tune stops at the resolution rather than asking again after the search");
    assert.equal(guide.releases(), 0, "a resolved playlist is never released back to the click flow");
    assert.deepEqual(guide.locates, [], "no on-now cell is looked up once the tune is resolved");
    assert.deepEqual(guide.clicks, [], "nothing is clicked once the tune is resolved");
  });
});
