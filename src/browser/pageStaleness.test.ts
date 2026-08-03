/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pageStaleness.test.ts: Unit tests for the stale page decision core - the pure judgment behind managed page cleanup. evaluateStalePages takes `now` and the whole
 * page, ownership, and clock state as one snapshot, so these tests are fully deterministic with literal timestamps and need no timer mocking or Chrome.
 */
import { describe, test } from "node:test";
import type { StalePageSnapshot } from "./pageStaleness.ts";
import assert from "node:assert/strict";
import { evaluateStalePages } from "./pageStaleness.ts";

// A round grace period and a base timestamp far from zero, so that elapsed and not-yet-elapsed clocks read at a glance in the fixtures below.
const GRACE = 30000;
const NOW = 1000000;

/* Builds a snapshot from the nothing-to-do baseline - no pages, no ownership, no clocks - plus whatever the scenario turns on. Each test states only the fields
 * its assertion depends on, so the input that drives the judgment stays visible rather than buried in boilerplate.
 */
function snapshot(overrides: Partial<StalePageSnapshot> = {}): StalePageSnapshot {

  return { activePageIds: new Set<string>(), gracePeriodMs: GRACE, inFlightSetupPageIds: new Set<string>(), now: NOW, pageIds: [],
    staleFirstSeen: new Map<string, number>(), ...overrides };
}

describe("pageStaleness: active stream pages", () => {

  test("an active page is never a close candidate and its running clock is dropped", () => {

    /* The active page carries a long-elapsed clock and the budget permits one close, so if active ownership stopped being consulted this page would be the one
     * candidate and would close. Both assertions therefore turn on the ownership check: the empty closeIds, and the forgetting of the clock that ownership makes
     * moot.
     */
    const actions = evaluateStalePages(snapshot({

      activePageIds: new Set(["page-1"]),
      pageIds: [ "page-1", "page-2", "page-3" ],
      staleFirstSeen: new Map([[ "page-1", NOW - (2 * GRACE) ]])
    }));

    assert.deepEqual(actions.closeIds, [], "an active page never closes");
    assert.deepEqual(actions.forgetTrackedIds, ["page-1"], "the clock on the now-active page is forgotten");
    assert.deepEqual(actions.startTrackingIds, [ "page-2", "page-3" ], "the unowned pages start clocks and the active page does not");
  });
});

describe("pageStaleness: the unowned page lifecycle", () => {

  test("a page starts a clock on first sight, waits out the grace period, and closes on the call at exactly the boundary", () => {

    // The undefined entry is a page PrismCast did not create. It is here to lift the preserve-one budget above zero, so the closing call below is decided by the
    // grace period rather than by the budget.
    const firstSight = evaluateStalePages(snapshot({ now: NOW, pageIds: [ "page-1", undefined ] }));

    assert.deepEqual(firstSight.startTrackingIds, ["page-1"], "first sight starts the clock");
    assert.deepEqual(firstSight.closeIds, [], "nothing closes on the call that starts the clock");

    const clocks = new Map([[ "page-1", NOW ]]);

    // One millisecond short of the grace period. Every action list must be empty: a page mid-reprieve is not merely spared the close, it is left entirely alone -
    // in particular its clock is not restarted, which would push the close out forever.
    const midReprieve = evaluateStalePages(snapshot({ now: (NOW + GRACE) - 1, pageIds: [ "page-1", undefined ], staleFirstSeen: clocks }));

    assert.deepEqual(midReprieve.closeIds, [], "no close before the grace period elapses");
    assert.deepEqual(midReprieve.startTrackingIds, [], "a running clock is not restarted");
    assert.deepEqual(midReprieve.forgetTrackedIds, [], "a running clock is not forgotten");
    assert.deepEqual(midReprieve.clearInFlightIds, [], "nothing to unmark");

    // Exactly at the boundary the reprieve is over, which is what tells an elapsed-at-the-boundary rule apart from one that waits a further millisecond.
    const atBoundary = evaluateStalePages(snapshot({ now: NOW + GRACE, pageIds: [ "page-1", undefined ], staleFirstSeen: clocks }));

    assert.deepEqual(atBoundary.closeIds, ["page-1"], "the page closes on the call at exactly firstSeen plus the grace period");
  });
});

describe("pageStaleness: the in-flight setup exemption", () => {

  test("a page whose setup is in flight is exempt however long its clock has run", () => {

    // The same snapshot twice, differing only in the in-flight mark. This is the whole reason the exemption exists: a tune slow enough to outlast the grace
    // period would otherwise have its own page closed out from under it.
    const exempt = evaluateStalePages(snapshot({

      inFlightSetupPageIds: new Set(["page-1"]),
      pageIds: [ "page-1", undefined ],
      staleFirstSeen: new Map([[ "page-1", NOW - (10 * GRACE) ]])
    }));

    assert.deepEqual(exempt.closeIds, [], "the in-flight page does not close");
    assert.deepEqual(exempt.forgetTrackedIds, ["page-1"], "the clock the exemption makes moot is forgotten, exactly as it is for an active page");

    const unmarked = evaluateStalePages(snapshot({

      pageIds: [ "page-1", undefined ],
      staleFirstSeen: new Map([[ "page-1", NOW - (10 * GRACE) ]])
    }));

    assert.deepEqual(unmarked.closeIds, ["page-1"], "the very same page closes without the mark, so the exemption is what spares it");
  });

  test("an in-flight page never starts a clock in the first place", () => {

    const actions = evaluateStalePages(snapshot({ inFlightSetupPageIds: new Set(["page-1"]), pageIds: [ "page-1", undefined ] }));

    assert.deepEqual(actions.startTrackingIds, [], "the exemption stops the clock from ever starting, not just the close");
  });
});

describe("pageStaleness: in-flight mark convergence", () => {

  test("a mark clears once the registry records the page against its stream", () => {

    const actions = evaluateStalePages(snapshot({ activePageIds: new Set(["page-1"]), inFlightSetupPageIds: new Set(["page-1"]), pageIds: ["page-1"] }));

    assert.deepEqual(actions.clearInFlightIds, ["page-1"], "ownership has landed, so the mark has done its job");
    assert.deepEqual(actions.forgetTrackedIds, [], "an owned page carrying no clock leaves nothing to forget");
  });

  test("a mark for a page missing from the page list is kept rather than pruned", () => {

    // The id is absent from the page list and from the registry both, which is what a still-running setup looks like on a pass where the browser did not report
    // its page. Pruning here would strip the exemption from that setup, so the mark is deliberately retained until its own teardown or the session end drops it.
    const actions = evaluateStalePages(snapshot({ inFlightSetupPageIds: new Set(["page-1"]), pageIds: [ "page-2", undefined ] }));

    assert.deepEqual(actions.clearInFlightIds, [], "an absent in-flight page keeps its mark");
  });
});

describe("pageStaleness: pages PrismCast did not create", () => {

  test("unmanaged pages are never tracked or closed, and count only toward the budget", () => {

    // Two unmanaged pages and one long-stale managed page. The budget is what the unmanaged pages affect: counting them gives room for the close below, and
    // leaving them out of the page count would give a budget of zero and spare the managed page for the wrong reason.
    const actions = evaluateStalePages(snapshot({

      pageIds: [ undefined, undefined, "page-1" ],
      staleFirstSeen: new Map([[ "page-1", NOW - (2 * GRACE) ]])
    }));

    assert.deepEqual(actions.closeIds, ["page-1"], "only the managed page is closable, and the unmanaged pages give the budget room for it");
    assert.deepEqual(actions.startTrackingIds, [], "unmanaged pages never start clocks");
    assert.deepEqual(actions.forgetTrackedIds, [], "unmanaged pages produce no tracking entries to forget");
  });
});

describe("pageStaleness: the preserve-one budget", () => {

  test("the budget counts pages and active pages only, and spends itself in walk order", () => {

    /* Four pages, one of them active, and three candidates whose clocks started at deliberately scrambled times. The budget leaves two closable, and the two that
     * close are the first two the browser reported - not the two oldest, which is what tells walk order apart from age order.
     */
    const actions = evaluateStalePages(snapshot({

      activePageIds: new Set(["page-1"]),
      pageIds: [ "page-1", "page-2", "page-3", "page-4" ],
      staleFirstSeen: new Map([ [ "page-2", NOW - (2 * GRACE) ], [ "page-3", NOW - (9 * GRACE) ], [ "page-4", NOW - (5 * GRACE) ] ])
    }));

    assert.deepEqual(actions.closeIds, [ "page-2", "page-3" ], "two of the three candidates close, in the order the browser reported them");
  });

  test("in-flight pages do not shrink the budget", () => {

    /* One in-flight page that no stream owns yet, plus two elapsed candidates. The budget is three pages less the preserved one less the zero active pages, so
     * both candidates close and the in-flight page is the one left standing. Subtracting in-flight pages from the budget as well would close only the first
     * candidate, sparing the second for no reason - the exemption already keeps in-flight pages out of the candidate list entirely.
     */
    const actions = evaluateStalePages(snapshot({

      inFlightSetupPageIds: new Set(["page-1"]),
      pageIds: [ "page-1", "page-2", "page-3" ],
      staleFirstSeen: new Map([ [ "page-2", NOW - (2 * GRACE) ], [ "page-3", NOW - (2 * GRACE) ] ])
    }));

    assert.deepEqual(actions.closeIds, [ "page-2", "page-3" ], "both candidates close and the in-flight page survives as the preserved one");
  });

  test("a page that is both active and in-flight is subtracted from the budget exactly once", () => {

    /* The overlap case: a setup whose ownership has just landed is briefly in both sets. It must count once, through the active term. Counting it twice would
     * leave a budget of one and spare a candidate that should close.
     */
    const actions = evaluateStalePages(snapshot({

      activePageIds: new Set(["page-1"]),
      inFlightSetupPageIds: new Set(["page-1"]),
      pageIds: [ "page-1", "page-2", "page-3", "page-4" ],
      staleFirstSeen: new Map([ [ "page-2", NOW - (2 * GRACE) ], [ "page-3", NOW - (2 * GRACE) ], [ "page-4", NOW - (2 * GRACE) ] ])
    }));

    assert.deepEqual(actions.closeIds, [ "page-2", "page-3" ], "the overlapping page costs the budget one, not two");
  });

  test("the budget floors at zero when the active pages outnumber the pages the browser reports", () => {

    /* A reachable state: the registry still holds entries whose pages have gone from the browser, so the active ids outnumber the page list. The arithmetic goes
     * negative, and the floor is what turns that into closing nothing. Without it the negative count reaches the slice as a from-the-end length and closes every
     * candidate but the last - the exact opposite of the restraint a negative budget is asking for.
     */
    const actions = evaluateStalePages(snapshot({

      activePageIds: new Set([ "page-7", "page-8", "page-9" ]),
      pageIds: [ "page-1", "page-2", "page-3" ],
      staleFirstSeen: new Map([ [ "page-1", NOW - (2 * GRACE) ], [ "page-2", NOW - (2 * GRACE) ], [ "page-3", NOW - (2 * GRACE) ] ])
    }));

    assert.deepEqual(actions.closeIds, [], "a negative budget closes nothing");
  });
});

describe("pageStaleness: the dead-entry sweep", () => {

  test("a clock for a page that is gone from the browser is forgotten", () => {

    // Without the sweep these entries accumulate for the life of the browser session, one per page closed by any path other than this cleanup.
    const actions = evaluateStalePages(snapshot({

      pageIds: [ "page-1", undefined ],
      staleFirstSeen: new Map([[ "page-9", NOW - (2 * GRACE) ]])
    }));

    assert.deepEqual(actions.forgetTrackedIds, ["page-9"], "the clock for the vanished page is forgotten");
    assert.deepEqual(actions.startTrackingIds, ["page-1"], "the page that is present starts its own clock");
    assert.deepEqual(actions.closeIds, [], "a vanished page is forgotten, not closed");
  });
});
