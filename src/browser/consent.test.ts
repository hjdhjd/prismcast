/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consent.test.ts: Unit tests for the Node-side orchestration in consent.ts - the unified auto-dismiss logging (logAutoDismiss), the overlay-handling poll
 * (startOverlayHandling), and the detect-and-guide probe (consentOverlayPresent). The in-page heuristics themselves (the embed-gate selector/keyword matching and
 * the coordinate resolution inside page.evaluate) run in a real DOM and are deferred to the browser-e2e tier; here a page stub returns scripted page.evaluate
 * results so the poll's decision flow - reject-then-accept ordering, the embed-gate signal, abort handling, and page-closed handling - is locked without spinning
 * up Chrome. LOG is spied via the test-context mock so the logging contract is asserted directly on the production LOG object.
 */
import { consentOverlayPresent, logAutoDismiss, startOverlayHandling } from "./consent.ts";
import { describe, test } from "node:test";
import { LOG } from "../utils/index.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { makeProfile } from "../config/profiles.helpers.ts";

// The Didomi reject selector seeded in the CMP registry. The poll passes it to page.evaluate when probing for a cookie banner, so the stub routes on it to simulate
// "banner present" vs "absent". Kept here as the single literal the test depends on, matching the one seeded entry in consent.ts.
const DIDOMI_REJECT = "#didomi-notice-disagree-button";

// A per-site dismissSelector value used to exercise the legacy modal path through the poll. Any string works; the stub routes on it by value.
const DISMISS_SELECTOR = ".watch-live";

/* PageStub records the coordinate clicks dispatched and the page.evaluate arguments seen, so a test can assert what the poll did. The evaluate router maps each
 * call's argument to a scripted result: the embed-gate probe passes an object carrying a `gate` key, the CMP reject probe passes the reject selector string, the
 * CMP-detect probe passes an array of detect selectors, and the per-site modal passes the configured dismissSelector string. Routing on the argument shape lets one
 * stub serve every page.evaluate the consent module performs.
 */
interface PageStub {

  clicks: { x: number; y: number }[];
  evaluateArgs: unknown[];
}

function makePageStub(router: (arg: unknown) => unknown): { page: Page; stub: PageStub } {

  const stub: PageStub = { clicks: [], evaluateArgs: [] };

  const page = {

    evaluate: async (_fn: unknown, arg?: unknown): Promise<unknown> => {

      stub.evaluateArgs.push(arg);

      return router(arg);
    },
    mouse: {

      click: async (x: number, y: number): Promise<void> => {

        stub.clicks.push({ x, y });
      }
    }
  } as unknown as Page;

  return { page, stub };
}

// True when the argument is the embed-gate probe payload (locateEmbedGate passes { accept, exclude, gate }).
function isGateProbe(arg: unknown): boolean {

  return (typeof arg === "object") && (arg !== null) && ("gate" in arg);
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

    await startOverlayHandling(page, makeProfile(), { onEmbedGateAccepted: () => { gateSignals++; } });

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

    await startOverlayHandling(page, makeProfile(), { onEmbedGateAccepted: () => { gateSignals++; } });

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

    await startOverlayHandling(page, makeProfile(), { onEmbedGateAccepted: () => { gateSignals++; }, signal: controller.signal });

    assert.equal(stub.evaluateArgs.length, 0, "no page evaluation occurred");
    assert.equal(stub.clicks.length, 0, "no click occurred");
    assert.equal(gateSignals, 0, "the gate callback never fired");
  });

  test("stops silently when the page evaluation throws (page navigated or closed)", async (t) => {

    t.mock.method(LOG, "info", () => { /* Silenced. */ });

    const { page } = makePageStub(() => { throw new Error("Target closed."); });

    let gateSignals = 0;

    // Resolves rather than rejecting - the poll swallows page-gone errors.
    await startOverlayHandling(page, makeProfile(), { onEmbedGateAccepted: () => { gateSignals++; } });

    assert.equal(gateSignals, 0, "no gate was signaled");
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

      return (arg === DISMISS_SELECTOR) ? true : null;
    });

    await startOverlayHandling(page, makeProfile({ dismissSelector: DISMISS_SELECTOR }), { onEmbedGateAccepted: () => { /* Terminates the poll. */ } });

    const modalProbes = stub.evaluateArgs.filter((arg) => arg === DISMISS_SELECTOR);

    assert.equal(modalProbes.length, 1, "the dismissSelector is probed once on tick one and deduped thereafter");
    assert.ok(info.mock.calls.some((call) => String(call.arguments[0]).includes("interstitial modal")), "the modal dismissal was logged");
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

    await startOverlayHandling(page, makeProfile(), { onEmbedGateAccepted: () => { /* Terminates the poll. */ } });

    const rejectProbes = stub.evaluateArgs.filter((arg) => arg === DIDOMI_REJECT);

    assert.equal(rejectProbes.length, 1, "the CMP vendor is rejected once and not re-probed once handled");
  });

  test("polls multiple ticks as a no-op and stops promptly once the signal aborts", async (t) => {

    t.mock.method(LOG, "info", () => { /* Silenced. */ });

    // Nothing actionable is ever present, so the poll is a pure no-op until the signal aborts it mid-window.
    const { page, stub } = makePageStub(() => null);
    const controller = new AbortController();

    let gateSignals = 0;

    setTimeout(() => { controller.abort(); }, 700);

    await startOverlayHandling(page, makeProfile({ videoTimeout: 10000 }), { onEmbedGateAccepted: () => { gateSignals++; }, signal: controller.signal });

    assert.equal(stub.clicks.length, 0, "a no-overlay poll never clicks");
    assert.equal(gateSignals, 0, "a no-overlay poll never signals a gate");
    assert.ok(stub.evaluateArgs.length >= 2, "the poll ran at least one full tick before aborting");
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

      return isGateProbe(arg) ? { label: "Accept", x: 1, y: 2 } : null;
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
