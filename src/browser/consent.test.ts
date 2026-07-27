/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consent.test.ts: Unit tests for the Node-side orchestration in consent.ts - the unified auto-dismiss logging (logAutoDismiss), the phase-scoped overlay-handling
 * poll (startOverlayHandling), and the detect-and-guide probe (consentOverlayPresent). The in-page heuristics themselves (the embed-gate selector/keyword matching
 * and the coordinate resolution inside page.evaluate) are pinned against a synthetic happy-dom document in the co-located consent.heuristics.test.ts; here a page
 * stub returns scripted page.evaluate results so the poll's decision flow - phase masking, reject-then-accept ordering, the embed-gate signal, the probe/act split,
 * malformed-selector fault isolation, the tick-error taxonomy, and abort handling - is locked without spinning up Chrome. Time is driven by an injected fake clock
 * so multi-tick behavior is deterministic with no real timers. LOG is spied via the test-context mock so the logging contract is asserted directly on the
 * production LOG object.
 */
import { consentOverlayPresent, logAutoDismiss, startOverlayHandling } from "./consent.ts";
import { describe, test } from "node:test";
import type { Clock } from "../utils/index.ts";
import { LOG } from "../utils/index.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { makeProfile } from "../config/profiles.helpers.ts";

// The Didomi reject selector seeded in the CMP registry. The poll passes it to page.evaluate when probing for a cookie banner, so the stub routes on it to simulate
// "banner present" vs "absent". Kept here as the single literal the test depends on, matching the one seeded entry in consent.ts.
const DIDOMI_REJECT = "#didomi-notice-disagree-button";

// A per-site dismissSelector value used to exercise the modal path through the poll. Any string works; the stub routes on it by value.
const DISMISS_SELECTOR = ".watch-live";

/* PageStub records the coordinate clicks dispatched and the page.evaluate arguments seen, so a test can assert what the poll did. The evaluate router maps each
 * call's argument to a scripted result: the embed-gate probe passes an object carrying a `gate` key (and an `act` flag), the CMP reject probe passes the reject
 * selector string, the CMP-detect probe passes an array of detect selectors, and the per-site modal passes the configured dismissSelector string (whose scripted
 * result is one of "absent" | "clicked" | "invalid-selector"). Routing on the argument shape lets one stub serve every page.evaluate the consent module performs.
 */
interface PageStub {

  clicks: { x: number; y: number }[];
  evaluateArgs: unknown[];
}

/**
 * Options for makePageStub: the browser-connected state and the isClosed result the tick-error taxonomy reads when an evaluate rejects.
 */
interface PageStubOptions {

  connected?: boolean;
  isClosed?: () => boolean;
}

function makePageStub(router: (arg: unknown) => unknown, options: PageStubOptions = {}): { page: Page; stub: PageStub } {

  const { connected = true, isClosed = (): boolean => false } = options;
  const stub: PageStub = { clicks: [], evaluateArgs: [] };

  const page = {

    browser: (): { connected: boolean } => ({ connected }),
    evaluate: async (_fn: unknown, arg?: unknown): Promise<unknown> => {

      stub.evaluateArgs.push(arg);

      return router(arg);
    },
    isClosed,
    mouse: {

      click: async (x: number, y: number): Promise<void> => {

        stub.clicks.push({ x, y });
      }
    }
  } as unknown as Page;

  return { page, stub };
}

/* A deterministic fake Clock: now() advances only when sleep() is called, so the poll's schedule is a pure function of its tick count with no wall-clock dependency.
 * raceWithTimeout is unused by the consent module and simply forwards the promise.
 */
function makeFakeClock(): Clock {

  let now = 0;

  return {

    now: (): number => now,
    raceWithTimeout: async <T>(promise: Promise<T>): Promise<T> => promise,
    sleep: async (ms: number): Promise<void> => { now += ms; }
  };
}

// True when the argument is the embed-gate probe payload (locateEmbedGate passes { accept, act, exclude, gate }).
function isGateProbe(arg: unknown): boolean {

  return (typeof arg === "object") && (arg !== null) && ("gate" in arg);
}

// Reads the `act` flag recorded on an embed-gate probe argument. The flag is the Node-observable proxy for the in-page scrollIntoView: the acting path passes act
// true (scroll + coordinates), the read-only detection probe passes act false (presence only).
function gateProbeAct(arg: unknown): boolean {

  return (arg as { act: boolean }).act;
}

// Returns the arguments recorded for a mock call at the given index, asserting the call exists so the indexed access satisfies noUncheckedIndexedAccess without
// scattering non-null assertions through the test bodies.
function callArgs(calls: readonly { arguments: readonly unknown[] }[], index: number): readonly unknown[] {

  const call = calls[index];

  assert.ok(call, "expected a recorded call at index " + String(index));

  return call.arguments;
}

describe("logAutoDismiss", () => {

  test("cookie-consent emits a vendor-named INFO line plus a browser:consent debug companion", (t) => {

    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });
    const debug = t.mock.method(LOG, "debug", () => { /* Captured via the mock. */ });

    logAutoDismiss("cookie-consent", { selector: DIDOMI_REJECT, vendor: "Didomi" });

    assert.equal(info.mock.calls.length, 1, "exactly one INFO line");
    assert.match(String(callArgs(info.mock.calls, 0)[0]), /rejected the %s cookie-consent prompt/);
    assert.equal(callArgs(info.mock.calls, 0)[1], "Didomi", "vendor is interpolated");
    assert.equal(debug.mock.calls.length, 1, "exactly one DEBUG companion");
    assert.equal(callArgs(debug.mock.calls, 0)[0], "browser:consent", "debug is tagged with the consent category");
  });

  test("embed-gate emits a fixed accept INFO line plus a browser:consent debug companion", (t) => {

    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });
    const debug = t.mock.method(LOG, "debug", () => { /* Captured via the mock. */ });

    logAutoDismiss("embed-gate", { label: "Accept" });

    assert.equal(info.mock.calls.length, 1);
    assert.match(String(callArgs(info.mock.calls, 0)[0]), /accepted an embedded-player consent prompt/);
    assert.equal(callArgs(info.mock.calls, 0).length, 1, "no positional interpolation arg on the fixed message");
    assert.equal(callArgs(debug.mock.calls, 0)[0], "browser:consent", "every kind emits the consent-tagged debug companion");
  });

  test("modal emits the interstitial-dismiss INFO line plus a browser:consent debug companion", (t) => {

    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });
    const debug = t.mock.method(LOG, "debug", () => { /* Captured via the mock. */ });

    logAutoDismiss("modal", { selector: DISMISS_SELECTOR });

    assert.equal(info.mock.calls.length, 1);
    assert.match(String(callArgs(info.mock.calls, 0)[0]), /dismissed an interstitial modal/);
    assert.equal(callArgs(info.mock.calls, 0).length, 1, "no positional interpolation arg on the fixed message");
    assert.equal(callArgs(debug.mock.calls, 0)[0], "browser:consent", "every kind emits the consent-tagged debug companion");
  });

  test("the vendor placeholder falls back to \"site\" when no vendor is supplied", (t) => {

    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

    t.mock.method(LOG, "debug", () => { /* Silenced. */ });

    logAutoDismiss("cookie-consent");

    assert.equal(callArgs(info.mock.calls, 0)[1], "site");
  });
});

describe("startOverlayHandling", () => {

  test("accepts an embed gate: coordinate-clicks it, signals, and stops the poll", async (t) => {

    t.mock.method(LOG, "info", () => { /* Silenced. */ });
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });

    // No cookie banner present (CMP reject probe returns null); an embed gate is located at (10, 20).
    const { page, stub } = makePageStub((arg) => {

      if(isGateProbe(arg)) {

        return { label: "Accept", x: 10, y: 20 };
      }

      return null;
    });

    let gateSignals = 0;

    await startOverlayHandling(page, makeProfile(), { onEmbedGateAccepted: () => { gateSignals++; }, phase: "videoWait" });

    assert.equal(gateSignals, 1, "the embed-gate callback fired exactly once");
    assert.deepEqual(stub.clicks, [{ x: 10, y: 20 }], "the gate's accept control was coordinate-clicked");
  });

  test("rejects a cookie banner before accepting an embed gate in the same tick", async (t) => {

    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

    t.mock.method(LOG, "debug", () => { /* Silenced. */ });

    // Cookie banner present (reject at (5, 6)) and an embed gate present (accept at (10, 20)).
    const { page, stub } = makePageStub((arg) => {

      if(isGateProbe(arg)) {

        return { label: "Accept", x: 10, y: 20 };
      }

      if(arg === DIDOMI_REJECT) {

        return { x: 5, y: 6 };
      }

      return null;
    });

    let gateSignals = 0;

    await startOverlayHandling(page, makeProfile(), { onEmbedGateAccepted: () => { gateSignals++; }, phase: "videoWait" });

    assert.deepEqual(stub.clicks, [ { x: 5, y: 6 }, { x: 10, y: 20 } ], "the cookie reject is clicked before the gate accept");
    assert.equal(gateSignals, 1);

    const messages = info.mock.calls.map((call) => String(call.arguments[0]));

    assert.ok(messages.some((m) => m.includes("cookie-consent prompt")), "the cookie reject was logged");
    assert.ok(messages.some((m) => m.includes("embedded-player consent prompt")), "the gate accept was logged");
  });

  test("returns immediately without touching the page when the signal is already aborted", async (t) => {

    t.mock.method(LOG, "info", () => { /* Silenced. */ });

    const { page, stub } = makePageStub(() => null);
    const controller = new AbortController();

    controller.abort();

    let gateSignals = 0;

    await startOverlayHandling(page, makeProfile(), { onEmbedGateAccepted: () => { gateSignals++; }, phase: "videoWait", signal: controller.signal });

    assert.equal(stub.evaluateArgs.length, 0, "no page evaluation occurred");
    assert.equal(stub.clicks.length, 0, "no click occurred");
    assert.equal(gateSignals, 0, "the gate callback never fired");
  });

  test("aborting inside the CMP reject halts the tick before the embed-gate probe and the modal dismiss run", async (t) => {

    t.mock.method(LOG, "info", () => { /* Silenced. */ });
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });

    const controller = new AbortController();

    // The Didomi banner is present, and clicking its reject aborts the signal mid-tick. The abort check placed right after the CMP-reject group must then return
    // "stop" before the same tick's embed-gate probe and per-site modal dismiss, so neither is ever issued within that tick. This distinguishes the mid-tick check
    // from the between-tick checks in the poll loop: a between-tick check cannot suppress the later action groups of the tick that is already running.
    const { page, stub } = makePageStub((arg) => {

      if(arg === DIDOMI_REJECT) {

        controller.abort();

        return { x: 5, y: 6 };
      }

      return null;
    });

    await startOverlayHandling(page, makeProfile({ dismissSelector: DISMISS_SELECTOR }),
      { clock: makeFakeClock(), onEmbedGateAccepted: () => { /* The gate never fires; the videoWait arm of the union requires the callback. */ }, phase: "videoWait",
        signal: controller.signal });

    assert.deepEqual(stub.clicks, [{ x: 5, y: 6 }], "only the CMP reject dispatched before the abort halted the tick");
    assert.equal(stub.evaluateArgs.filter(isGateProbe).length, 0, "the post-reject abort check skipped the embed-gate probe");
    assert.ok(!stub.evaluateArgs.includes(DISMISS_SELECTOR), "the post-reject abort check skipped the per-site modal dismiss");
  });

  test("dismisses a per-site modal through the poll, then dedups it on later ticks", async (t) => {

    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

    t.mock.method(LOG, "debug", () => { /* Silenced. */ });

    // No cookie banner; the per-site modal is present; an embed gate appears on the second tick only, to terminate the poll.
    let gateProbes = 0;

    const { page, stub } = makePageStub((arg) => {

      if(isGateProbe(arg)) {

        gateProbes++;

        return (gateProbes >= 2) ? { label: "Accept", x: 9, y: 9 } : null;
      }

      return (arg === DISMISS_SELECTOR) ? "clicked" : null;
    });

    await startOverlayHandling(page, makeProfile({ dismissSelector: DISMISS_SELECTOR }),
      { clock: makeFakeClock(), onEmbedGateAccepted: () => { /* Terminates the poll. */ }, phase: "videoWait" });

    const modalProbes = stub.evaluateArgs.filter((arg) => arg === DISMISS_SELECTOR);

    assert.equal(modalProbes.length, 1, "the dismissSelector is probed once on tick one and deduped thereafter");
    assert.ok(info.mock.calls.some((call) => String(call.arguments[0]).includes("interstitial modal")), "the modal dismissal was logged");
  });

  test("an armed dismissSelector whose valid selector matches nothing stays armed and is re-probed on the next tick", async (t) => {

    t.mock.method(LOG, "info", () => { /* Silenced. */ });
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });

    const controller = new AbortController();
    let dismissProbes = 0;

    // No cookie banner and no embed gate; the configured per-site modal is probed but its valid selector matches nothing ("absent"). The absent result must leave
    // the action armed - not mark it done or disabled - so the modal is probed again on the next tick. The abort is tied to the second modal probe, so the poll ends
    // deterministically on the injected clock once two ticks have each run the dismiss action.
    const { page, stub } = makePageStub((arg) => {

      if(isGateProbe(arg)) {

        return null;
      }

      if(arg === DISMISS_SELECTOR) {

        dismissProbes++;

        if(dismissProbes >= 2) {

          controller.abort();
        }

        return "absent";
      }

      return null;
    });

    await startOverlayHandling(page, makeProfile({ dismissSelector: DISMISS_SELECTOR }),
      { clock: makeFakeClock(), onEmbedGateAccepted: () => { /* The gate never fires in this test. */ }, phase: "videoWait", signal: controller.signal });

    const modalProbes = stub.evaluateArgs.filter((arg) => arg === DISMISS_SELECTOR);

    assert.ok(modalProbes.length >= 2, "an absent modal leaves the action armed, so it is re-probed on a subsequent tick");
    assert.equal(stub.clicks.length, 0, "an absent modal never dispatches a click");
  });

  test("rejects a cookie banner once and does not re-probe that vendor on later ticks", async (t) => {

    t.mock.method(LOG, "info", () => { /* Silenced. */ });
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });

    // The Didomi banner is present; an embed gate appears on the second tick to terminate the poll.
    let gateProbes = 0;

    const { page, stub } = makePageStub((arg) => {

      if(isGateProbe(arg)) {

        gateProbes++;

        return (gateProbes >= 2) ? { label: "Accept", x: 1, y: 1 } : null;
      }

      return (arg === DIDOMI_REJECT) ? { x: 2, y: 2 } : null;
    });

    await startOverlayHandling(page, makeProfile(), { clock: makeFakeClock(), onEmbedGateAccepted: () => { /* Terminates the poll. */ }, phase: "videoWait" });

    const rejectProbes = stub.evaluateArgs.filter((arg) => arg === DIDOMI_REJECT);

    assert.equal(rejectProbes.length, 1, "the CMP vendor is rejected once and not re-probed once handled");
  });

  // Every non-videoWait phase masks the embed-gate accept: its policy forbids the gate, so the acting gate probe never runs, while cookie rejection and per-site
  // modal dismissal stay live. The assertion that would fail against an unmasked implementation is the zero gate-probe count.
  for(const phase of [ "discovery", "postGateReload", "staticCapture", "tuneSetup" ] as const) {

    test("the " + phase + " phase rejects a CMP banner and dismisses a modal but never runs the embed-gate probe", async (t) => {

      t.mock.method(LOG, "info", () => { /* Silenced. */ });
      t.mock.method(LOG, "debug", () => { /* Silenced. */ });

      const controller = new AbortController();

      // The CMP banner and the per-site modal are both present. Aborting the instant the modal is dismissed ends the poll after a single tick.
      const { page, stub } = makePageStub((arg) => {

        if(isGateProbe(arg)) {

          return { label: "Accept", x: 9, y: 9 };
        }

        if(arg === DIDOMI_REJECT) {

          return { x: 5, y: 6 };
        }

        if(arg === DISMISS_SELECTOR) {

          controller.abort();

          return "clicked";
        }

        return null;
      });

      await startOverlayHandling(page, makeProfile({ dismissSelector: DISMISS_SELECTOR }), { clock: makeFakeClock(), phase, signal: controller.signal });

      assert.equal(stub.evaluateArgs.filter(isGateProbe).length, 0, "a masked phase never issues the embed-gate probe");
      assert.ok(stub.clicks.some((click) => (click.x === 5) && (click.y === 6)), "the CMP banner is still rejected");
      assert.ok(stub.evaluateArgs.includes(DISMISS_SELECTOR), "the per-site modal is still dismissed");
    });
  }

  test("consentOverlayPresent probes the embed gate read-only while the videoWait tick scrolls and clicks it", async (t) => {

    t.mock.method(LOG, "info", () => { /* Silenced. */ });
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });

    // Detection probe: no CMP banner, an embed gate present. The recorded gate-probe argument must carry act false (presence only, no scrollIntoView).
    const probe = makePageStub((arg) => {

      if(Array.isArray(arg)) {

        return false;
      }

      return isGateProbe(arg) ? { label: "Accept" } : null;
    });

    assert.equal(await consentOverlayPresent(probe.page), true);

    const probeGateArg = probe.stub.evaluateArgs.find(isGateProbe);

    assert.ok(probeGateArg, "the detection probe issued an embed-gate probe");
    assert.equal(gateProbeAct(probeGateArg), false, "the detection probe runs in read-only mode - no scrollIntoView");

    // Acting path: the videoWait tick locates and coordinate-clicks the same gate. Its recorded gate-probe argument must carry act true.
    const act = makePageStub((arg) => (isGateProbe(arg) ? { label: "Accept", x: 3, y: 4 } : null));

    await startOverlayHandling(act.page, makeProfile(), { onEmbedGateAccepted: () => { /* Terminates the poll. */ }, phase: "videoWait" });

    const actGateArg = act.stub.evaluateArgs.find(isGateProbe);

    assert.ok(actGateArg, "the videoWait tick issued an embed-gate probe");
    assert.equal(gateProbeAct(actGateArg), true, "the acting path scrolls the matched control into view");
    assert.deepEqual(act.stub.clicks, [{ x: 3, y: 4 }], "the acting path coordinate-clicks the gate");
  });

  test("a malformed dismissSelector disables only itself, warns once per selector per process, and the poll survives", async (t) => {

    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });

    t.mock.method(LOG, "info", () => { /* Silenced. */ });
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });

    // A selector unique to this test, so the process-wide warned-selectors set is not pre-poisoned by another test using the same value.
    const badSelector = ":::malformed-" + String(Date.now());

    // Builds a router where the CMP banner is present, the gate is absent, and the bad dismissSelector reports "invalid-selector". A gate-probe counter drives the
    // abort so each poll runs a deterministic two ticks - long enough to prove the dismiss action is not re-probed after being disabled.
    const makeRouter = (controller: AbortController): ((arg: unknown) => unknown) => {

      let gateProbes = 0;

      return (arg): unknown => {

        if(isGateProbe(arg)) {

          gateProbes++;

          if(gateProbes >= 2) {

            controller.abort();
          }

          return null;
        }

        if(arg === DIDOMI_REJECT) {

          return { x: 5, y: 6 };
        }

        return (arg === badSelector) ? "invalid-selector" : null;
      };
    };

    const firstController = new AbortController();
    const first = makePageStub(makeRouter(firstController));

    await startOverlayHandling(first.page, makeProfile({ dismissSelector: badSelector }),
      { clock: makeFakeClock(), onEmbedGateAccepted: () => { /* The gate never fires in this test; the videoWait arm of the union requires the callback. */ },
        phase: "videoWait", signal: firstController.signal });

    const firstDismissProbes = first.stub.evaluateArgs.filter((arg) => arg === badSelector);

    assert.equal(firstDismissProbes.length, 1, "the malformed selector is probed once, then disabled - not re-probed on the second tick");
    assert.ok(first.stub.evaluateArgs.filter(isGateProbe).length >= 2, "the poll survived the malformed selector and ran a subsequent tick");
    assert.equal(warn.mock.calls.length, 1, "the malformed selector warns exactly once");
    assert.match(String(warn.mock.calls[0]?.arguments[0]), /not a valid CSS selector/);

    // A second poll instance with the SAME selector re-disables silently: the process-wide warned set already holds it, so no second warning is emitted.
    const secondController = new AbortController();
    const second = makePageStub(makeRouter(secondController));

    await startOverlayHandling(second.page, makeProfile({ dismissSelector: badSelector }),
      { clock: makeFakeClock(), onEmbedGateAccepted: () => { /* The gate never fires in this test; the videoWait arm of the union requires the callback. */ },
        phase: "videoWait", signal: secondController.signal });

    assert.equal(second.stub.evaluateArgs.filter((arg) => arg === badSelector).length, 1, "the second poll still probes and disables the selector");
    assert.equal(warn.mock.calls.length, 1, "the second poll re-disables silently - still exactly one warning across both polls");
  });

  test("a tick error continues the poll on a live page but stops it on a closed page or disconnected browser", async (t) => {

    t.mock.method(LOG, "info", () => { /* Silenced. */ });
    t.mock.method(LOG, "debug", () => { /* Silenced. */ });

    // Live page: the first evaluate throws (a transient in-walk navigation error), but isClosed is false and the browser is connected, so the tick continues and a
    // subsequent evaluate arrives. The gate-probe counter aborts after a couple of ticks so the poll ends deterministically.
    const liveController = new AbortController();
    let liveCalls = 0;

    const live = makePageStub((arg) => {

      liveCalls++;

      if(liveCalls === 1) {

        throw new Error("Execution context was destroyed, most likely because of a navigation.");
      }

      if(isGateProbe(arg) && (liveCalls >= 3)) {

        liveController.abort();
      }

      return null;
    });

    await startOverlayHandling(live.page, makeProfile(), { clock: makeFakeClock(), onEmbedGateAccepted: () => { /* Unused. */ }, phase: "videoWait",
      signal: liveController.signal });

    assert.ok(live.stub.evaluateArgs.length >= 2, "a transient tick error let the poll continue to a subsequent evaluate");

    // Closed page: the first evaluate throws and isClosed reports true, so the tick stops the poll with no further evaluate.
    const closed = makePageStub(() => { throw new Error("Target closed."); }, { isClosed: (): boolean => true });

    await startOverlayHandling(closed.page, makeProfile(), { clock: makeFakeClock(), onEmbedGateAccepted: () => { /* Unused. */ }, phase: "videoWait" });

    assert.equal(closed.stub.evaluateArgs.length, 1, "a closed page stops the poll after the throwing evaluate, with no further probe");

    // Disconnected browser: the first evaluate throws and the browser reports not connected, so the tick stops the poll with no further evaluate.
    const disconnected = makePageStub(() => { throw new Error("Session closed."); }, { connected: false });

    await startOverlayHandling(disconnected.page, makeProfile(), { clock: makeFakeClock(), onEmbedGateAccepted: () => { /* Unused. */ }, phase: "videoWait" });

    assert.equal(disconnected.stub.evaluateArgs.length, 1, "a disconnected browser stops the poll after the throwing evaluate, with no further probe");
  });

  test("polls repeatedly as a no-op and stops the instant the signal aborts, driven by the injected clock", async (t) => {

    t.mock.method(LOG, "info", () => { /* Silenced. */ });

    // Nothing actionable is ever present, so the poll is a pure no-op. The gate-probe counter aborts right after the second tick's gate probe; no real timer is used.
    const controller = new AbortController();
    let gateProbes = 0;

    const { page, stub } = makePageStub((arg) => {

      if(isGateProbe(arg)) {

        gateProbes++;

        if(gateProbes === 2) {

          controller.abort();
        }
      }

      return null;
    });

    let gateSignals = 0;

    await startOverlayHandling(page, makeProfile({ videoTimeout: 10000 }),
      { clock: makeFakeClock(), onEmbedGateAccepted: () => { gateSignals++; }, phase: "videoWait", signal: controller.signal });

    assert.equal(stub.clicks.length, 0, "a no-overlay poll never clicks");
    assert.equal(gateSignals, 0, "a no-overlay poll never signals a gate");
    assert.equal(stub.evaluateArgs.length, 4, "exactly two no-op ticks (CMP + gate probe each) ran before the abort ended the poll");
  });
});

describe("consentOverlayPresent", () => {

  test("returns true when a known CMP banner is detected", async () => {

    // The CMP-detect probe (array argument) reports a banner; the embed-gate probe is never consulted.
    const { page, stub } = makePageStub((arg) => Array.isArray(arg));

    assert.equal(await consentOverlayPresent(page), true);
    assert.equal(stub.evaluateArgs.length, 1, "short-circuits on the CMP-detect probe");
  });

  test("returns true when no CMP banner but an embed gate is located", async () => {

    const { page } = makePageStub((arg) => {

      if(Array.isArray(arg)) {

        return false;
      }

      return isGateProbe(arg) ? { label: "Accept" } : null;
    });

    assert.equal(await consentOverlayPresent(page), true);
  });

  test("returns false when neither a CMP banner nor an embed gate is present", async () => {

    const { page } = makePageStub((arg) => (Array.isArray(arg) ? false : null));

    assert.equal(await consentOverlayPresent(page), false);
  });

  test("returns false when probing throws (page navigated or closed)", async () => {

    const { page } = makePageStub(() => { throw new Error("Target closed."); });

    assert.equal(await consentOverlayPresent(page), false);
  });
});
