/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hbo.test.ts: Unit tests for the HBO Max provider module. The strategy reads one page, the /channels hub, so what these rows guard is how a page gets there:
 * every navigation and reload of a profile on this strategy goes through the strategy's own navigator, which enters the hub in place of the site's home page and
 * loads every other address as given. The page double records each document load with the arguments it was issued with, so a row reads where the navigator
 * sent the page rather than inferring it from what happened next.
 *
 * The navigator is driven through the strategy object's own navigate member rather than a module-private function, because that member is what the navigation
 * and reload functions in video.ts reach for. The last row runs a cold tune from the home page through the rail read to the watch page, then the warm tune that
 * follows it from the cached watch URL, which is what the navigator exists to make work before any discovery has run.
 */
import type { ChannelSelectionProfile, Nullable } from "../../types/index.ts";
import { after, afterEach, before, beforeEach, describe, test } from "node:test";
import { initDebugFilter, subscribeToLogs } from "../../utils/index.ts";
import type { FakePage } from "../../testing.helpers.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { hboProvider } from "./hbo.ts";
import { makeFakePage } from "../../testing.helpers.ts";
import { makeProfile } from "../../config/profiles.helpers.ts";

// The site's home page, which has no channel rail, and the hub the navigator enters in place of it.
const HOME_URL = "https://play.hbomax.com/";
const HUB_URL = "https://play.hbomax.com/channels";

// The relative watch path the rail fixture carries for HBO, and the full watch URL the strategy builds from it.
const WATCH_PATH = "/channel/watch/aaa/bbb";
const WATCH_URL = "https://play.hbomax.com/channel/watch/aaa/bbb";

// A content page a user names as a channel's address with no selector, which plays where it loads and has to load exactly as given.
const CONTENT_URL = "https://play.hbomax.com/video/watch/ccc/ddd";

// A second content page, one whose path carries no watch segment, because a rule keyed on that segment would still load the first one as given.
const SERIES_URL = "https://play.hbomax.com/series/eee";

// An address with no scheme, which the URL parser refuses, so the navigator has no path to read from it.
const UNPARSEABLE_URL = "play.hbomax.com/channels";

// The debug line the navigator emits when it enters the hub in place of the home page, which is what a debug log shows of that reroute.
const HUB_ENTRY_LINE = "Entering the HBO Max channel hub in place of the home page.";

// The hub's channel rail. Each page read the strategy makes while reading the rail hands this selector to its page function, which is how a row tells what it read.
const RAIL_SELECTOR = "section[data-testid=\"channels-hub-page-everything-you-love-hbo-rail-us_rail\"]";

// The rail read's page reads, answered in the order the strategy issues them: the scroll that brings the rail into view, the tile counts the settle loop reads
// until one repeats, which stops it after one pause, then the extraction carrying the HBO tile.
const RAIL_READ_ANSWERS: readonly unknown[] = [ undefined, 1, 1, [{ name: "HBO", watchPath: WATCH_PATH }] ];

/**
 * Builds a page double whose every document load settles at once with a null response, which the navigator never reads.
 * @returns The page double.
 */
function makeLoadPage(): FakePage {

  return makeFakePage({ onGoto: (call) => { call.resolve(null); } });
}

/**
 * Narrows a neutral profile to what the strategy requires: the HBO Max selection strategy and a non-null channel selector.
 * @param channelSelector - The channel name the strategy looks for on the rail.
 * @returns The narrowed profile.
 */
function makeHboProfile(channelSelector: string): ChannelSelectionProfile {

  return makeProfile({ channelSelection: { strategy: "hboGrid" }, channelSelector }) as ChannelSelectionProfile;
}

/**
 * Puts a page on a URL through the HBO Max strategy's own navigator, asserting on the way that the strategy registers one.
 * @param page - The page double to navigate.
 * @param url - The address to hand the navigator.
 */
async function navigateHbo(page: Page, url: string): Promise<void> {

  const navigate = hboProvider.strategy.navigate;

  assert.ok(navigate, "the HBO Max strategy registers its own navigator");

  await navigate(page, url);
}

/**
 * Reads back the URL of every document load a page double received.
 * @param fake - The page double to read.
 * @returns The URLs, in issue order.
 */
function loadedUrls(fake: FakePage): string[] {

  return fake.navigations.map((call) => String(call.args[0]));
}

afterEach(() => {

  // The channel cache is module state, so every test starts from the empty cache a browser restart produces.
  hboProvider.strategy.clearCache?.();
});

describe("HBO Max navigator", () => {

  test("is registered on the strategy, so every navigation and reload of an HBO Max profile goes through it", () => {

    assert.ok(hboProvider.strategy.navigate, "the HBO Max strategy declares a navigator");
  });

  test("enters the channel hub in place of the home page, with or without the trailing slash, under the plain load wait", async () => {

    // The URL parser normalizes the root path, so the home page's address with no trailing slash takes the same route as the address that carries it.
    const withSlash = makeLoadPage();
    const withoutSlash = makeLoadPage();

    await navigateHbo(withSlash.page, HOME_URL);
    await navigateHbo(withoutSlash.page, "https://play.hbomax.com");

    for(const fake of [ withSlash, withoutSlash ]) {

      const args = fake.navigations[0]?.args ?? [];

      assert.equal(fake.navigations.length, 1, "the home page issued exactly one document load");
      assert.equal(args[0], HUB_URL, "and that load was the hub");
      assert.ok(args.length <= 2, "with at most a wait option beside the URL");
      assert.notEqual((args[1] as { waitUntil?: unknown } | undefined)?.waitUntil, "networkidle2", "and no network-idle wait");
    }
  });

  test("loads a watch URL as given", async () => {

    // The warm path: a cached or persisted watch URL is the channel's own page, and sending it to the hub would cost every warm tune its shortcut.
    const fake = makeLoadPage();

    await navigateHbo(fake.page, WATCH_URL);

    assert.deepEqual(loadedUrls(fake), [WATCH_URL], "one document load, to the watch URL unchanged");
  });

  test("loads the hub as given when handed the hub, which is a reload of a page on the hub", async () => {

    // The reload function hands the navigator the page's own URL, and a page that entered at the hub reloads as the hub.
    const fake = makeLoadPage();

    await navigateHbo(fake.page, HUB_URL);

    assert.deepEqual(loadedUrls(fake), [HUB_URL], "one document load, to the hub unchanged");
  });

  test("loads a content page a user names as given", async () => {

    // A selector-less channel naming a content page plays where it loads, so only the home page is ever rerouted.
    const fake = makeLoadPage();

    await navigateHbo(fake.page, CONTENT_URL);

    assert.deepEqual(loadedUrls(fake), [CONTENT_URL], "one document load, to the content page unchanged");
  });

  test("loads a content page whose path carries no watch segment as given", async () => {

    // The navigator tells the home page apart by its path alone, so a content page with no watch segment loads as given just as one with it does.
    const fake = makeLoadPage();

    await navigateHbo(fake.page, SERIES_URL);

    assert.deepEqual(loadedUrls(fake), [SERIES_URL], "one document load, to the content page unchanged");
  });

  test("loads an address the URL parser refuses as given, rather than throwing before the load", async () => {

    /* The navigator reads a path only from an address the parser accepts, so an address it refuses is not the home page and goes to the document load
     * unchanged, as it would on a strategy with no navigator. The row awaits the navigator, so a parser error thrown ahead of the load fails the row.
     */
    const fake = makeLoadPage();

    await navigateHbo(fake.page, UNPARSEABLE_URL);

    assert.deepEqual(loadedUrls(fake), [UNPARSEABLE_URL], "one document load, to the address unchanged");
  });
});

describe("HBO Max hub entry log line", () => {

  // The debug lines emitted during a row, captured off the same emitter the web UI's log stream reads. The navigator announces the hub entry only at debug level,
  // so the category has to be on for the line to be observable at all.
  let debugLines: string[] = [];
  let unsubscribe: Nullable<() => void> = null;

  before(() => {

    initDebugFilter("tuning:hbo");
  });

  after(() => {

    initDebugFilter("");
  });

  beforeEach(() => {

    debugLines = [];
    unsubscribe = subscribeToLogs((entry) => {

      if(entry.level === "debug") {

        debugLines.push(entry.message);
      }
    });
  });

  afterEach(() => {

    unsubscribe?.();
    unsubscribe = null;
  });

  test("announces the hub entry for the home page and for nothing else", async () => {

    // Every address but the home page loads as given and has nothing to announce, so the one line the home page emits is the reroute itself.
    await navigateHbo(makeLoadPage().page, WATCH_URL);
    await navigateHbo(makeLoadPage().page, CONTENT_URL);
    await navigateHbo(makeLoadPage().page, HUB_URL);

    assert.deepEqual(debugLines, [], "a watch page, a content page and the hub, each loaded as given, announced nothing");

    await navigateHbo(makeLoadPage().page, HOME_URL);

    assert.deepEqual(debugLines, [HUB_ENTRY_LINE], "the home page announced its entry at the hub exactly once");
  });
});

describe("HBO Max tune from a cold cache", () => {

  test("a tune naming the home page enters the hub, reads the rail and loads the watch page, and the next tune loads the cached watch URL", async () => {

    // One page carries the cold tune and the warm one, so the navigations it records are the whole route: the hub, the watch page the cold tune chose, then the
    // warm tune's load.
    const fake = makeFakePage({

      onEvaluate: (call, index) => {

        if(index >= RAIL_READ_ANSWERS.length) {

          call.reject(new Error("The strategy made a page read the rail fixture does not script."));

          return;
        }

        call.resolve(RAIL_READ_ANSWERS[index]);
      },
      onGoto: (call) => { call.resolve(null); },
      onWaitForSelector: (call) => { call.resolve({}); }
    });

    await navigateHbo(fake.page, HOME_URL);

    const result = await hboProvider.strategy.execute(fake.page, makeHboProfile("HBO"));

    assert.deepEqual(result, { success: true }, "the cold tune found HBO on the rail");
    assert.deepEqual(loadedUrls(fake), [ HUB_URL, WATCH_URL ], "the page entered at the hub, then loaded the channel's watch page");
    assert.deepEqual(fake.evaluations.map((call) => call.args[1]), [ RAIL_SELECTOR, RAIL_SELECTOR, RAIL_SELECTOR, RAIL_SELECTOR ],
      "every page read targeted the hub's channel rail: the scroll, the tile counts and the extraction");

    const resolved = await hboProvider.strategy.resolveDirectUrl?.("HBO", fake.page);

    assert.equal(resolved, WATCH_URL, "the rail read cached the channel's watch URL for the next tune");

    await navigateHbo(fake.page, resolved);

    assert.deepEqual(loadedUrls(fake), [ HUB_URL, WATCH_URL, WATCH_URL ], "the warm tune loaded the cached watch URL as given");

    // The strategy's cache clear, which a browser restart runs, empties the cache this tune filled, since watch URLs cached in one browser session may be stale in
    // the next.
    hboProvider.strategy.clearCache?.();

    assert.equal(await hboProvider.strategy.resolveDirectUrl?.("HBO", fake.page), null, "the cache clear empties the cache the cold tune filled");
  });
});
