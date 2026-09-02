/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * browserSupervisor.test.ts: Unit tests for the browser capture-readiness supervisor. The supervisor's launch is an injected port and its clock is an injected
 * now(), so the entire lifecycle state machine - the gate, the loop-safe governor wiring, and every transition - is exercised here with a fake launch and a fake
 * clock, without ever spawning Chrome. The headline guarantees asserted below: single-flight launches, and NO launch attempt while the governor is cooling (the
 * loop bound).
 */
import { BrowserCaptureImpairedError, BrowserSupersededError, BrowserUnavailableError, createBrowserSupervisor } from "./browserSupervisor.ts";
import { describe, test } from "node:test";
import type { Browser } from "puppeteer-core";
import type { BrowserSupervisorPorts } from "./browserSupervisor.ts";
import type { LaunchGovernorPolicy } from "./launchGovernor.ts";
import assert from "node:assert/strict";

// Trips on the 2nd failed launch within a 10s window; cooldown ladder 1s -> 5s; 30s of sustained readiness resets to CLOSED.
const POLICY: LaunchGovernorPolicy = { cooldownLadderMs: [ 1000, 5000 ], failureThreshold: 2, failureWindowMs: 10000, healthHoldMs: 30000 };

// The supervisor never calls any Browser method, so an opaque stub suffices as the "launched" instance.
const stubBrowser = {} as unknown as Browser;

// A second, distinct instance. The identity guard on the mark compares by reference, so telling it apart from the published browser needs a different object and
// nothing else.
const otherBrowser = {} as unknown as Browser;

/* Test harness: a controllable launch port and clock. `env.launchImpl` is the per-test launch behavior (resolve a stub, reject, or return a deferred promise for
 * single-flight); `env.launchCalls` counts how many times the port was actually invoked - the assertion that proves the loop bound; `env.clock` is the injected now.
 * The governor policy defaults to the fixture above; a test that needs different bounds passes its own rather than building its own ports.
 */
function makeHarness(policy: LaunchGovernorPolicy = POLICY): {
  env: { clock: number; closeCalls: number; launchCalls: number; launchImpl: () => Promise<Browser> };
  sup: ReturnType<typeof createBrowserSupervisor>;
  transitions: string[];
} {

  const env = { clock: 0, closeCalls: 0, launchCalls: 0, launchImpl: ((): Promise<Browser> => Promise.resolve(stubBrowser)) };
  const transitions: string[] = [];

  const ports: BrowserSupervisorPorts = {

    close: async (): Promise<void> => { env.closeCalls++; },
    launch: async (): Promise<Browser> => {

      env.launchCalls++;

      return env.launchImpl();
    },
    now: (): number => env.clock,
    onStateChange: (next): void => {

      // A marked ready state records as its own marker, so an assertion can count the transition that carries the mark without reading the state back. Every other
      // transition records as its bare kind, which is what the assertions written before the mark existed read.
      transitions.push(((next.kind === "ready") && (next.impairment !== null)) ? "ready:impaired" : next.kind);
    },
    policy: (): LaunchGovernorPolicy => policy
  };

  return { env, sup: createBrowserSupervisor(ports), transitions };
}

const failLaunch = (): Promise<Browser> => Promise.reject(new Error("extension dead"));

describe("browserSupervisor: acquire happy path", () => {

  test("launches lazily from absent and becomes ready", async () => {

    const h = makeHarness();

    assert.equal(h.sup.inspect().kind, "absent");

    const browser = await h.sup.acquire("page");

    assert.equal(browser, stubBrowser);
    assert.equal(h.sup.inspect().kind, "ready");
    assert.equal(h.env.launchCalls, 1);
    assert.equal(h.sup.current(), stubBrowser);
  });

  test("returns the ready browser without relaunching", async () => {

    const h = makeHarness();

    await h.sup.acquire("page");
    const again = await h.sup.acquire("page");

    assert.equal(again, stubBrowser);
    assert.equal(h.env.launchCalls, 1, "a ready browser is reused, not relaunched");
  });

  test("currentLaunchTime reflects the moment the browser became ready", async () => {

    const h = makeHarness();

    h.env.clock = 4242;
    await h.sup.acquire("page");

    assert.equal(h.sup.currentLaunchTime(), 4242);
  });
});

describe("browserSupervisor: single-flight", () => {

  test("concurrent acquire calls join one in-flight launch", async () => {

    const h = makeHarness();
    const { promise, resolve } = Promise.withResolvers<Browser>();

    h.env.launchImpl = (): Promise<Browser> => promise;

    const a = h.sup.acquire("page");
    const b = h.sup.acquire("page");

    assert.equal(h.env.launchCalls, 1, "only one launch is started for concurrent acquirers");
    assert.equal(h.sup.inspect().kind, "launching");

    resolve(stubBrowser);

    assert.equal(await a, stubBrowser);
    assert.equal(await b, stubBrowser);
    assert.equal(h.sup.inspect().kind, "ready");
  });
});

describe("browserSupervisor: failure handling and the loop bound", () => {

  test("a failure below the threshold returns to absent and relaunches on the next request", async () => {

    const h = makeHarness();

    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire("page"), /extension dead/);

    assert.equal(h.sup.inspect().kind, "absent", "below the threshold there is no cooldown");

    // The next request relaunches immediately - the common transient costs no cooldown.
    h.env.launchImpl = (): Promise<Browser> => Promise.resolve(stubBrowser);

    const browser = await h.sup.acquire("page");

    assert.equal(browser, stubBrowser);
    assert.equal(h.env.launchCalls, 2);
  });

  test("reaching the threshold trips to degraded and then rejects WITHOUT spawning Chrome (the loop bound)", async () => {

    const h = makeHarness();

    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire("page"), /extension dead/);
    await assert.rejects(h.sup.acquire("page"), /extension dead/);

    const degraded = h.sup.inspect();

    assert.equal(degraded.kind, "degraded");
    assert.equal(h.env.launchCalls, 2, "two real launch attempts so far");

    // While cooling, acquire must fail fast WITHOUT a third launch - this is what bounds the request-driven relaunch loop.
    await assert.rejects(h.sup.acquire("page"), BrowserUnavailableError);
    await assert.rejects(h.sup.acquire("page"), BrowserUnavailableError);

    assert.equal(h.env.launchCalls, 2, "no Chrome is spawned while the governor is cooling, regardless of request volume");
  });
});

describe("browserSupervisor: HALF-OPEN trial and escalation", () => {

  test("once the cooldown elapses a single trial launches; success becomes ready", async () => {

    const h = makeHarness();

    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire("page"), /extension dead/);
    await assert.rejects(h.sup.acquire("page"), /extension dead/);

    const degraded = h.sup.inspect();

    assert.equal(degraded.kind, "degraded");

    // Advance past the cooldown; the next acquire is the HALF-OPEN trial. assert.equal above narrows the union by its kind tag, so .until is directly accessible.
    h.env.clock = degraded.until;
    h.env.launchImpl = (): Promise<Browser> => Promise.resolve(stubBrowser);

    const browser = await h.sup.acquire("page");

    assert.equal(browser, stubBrowser);
    assert.equal(h.sup.inspect().kind, "ready");
    assert.equal(h.env.launchCalls, 3, "exactly one trial launch after the cooldown");
  });

  test("a failed trial escalates the cooldown along the ladder", async () => {

    const h = makeHarness();

    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire("page"), /extension dead/);
    await assert.rejects(h.sup.acquire("page"), /extension dead/);

    const first = h.sup.inspect();

    assert.equal(first.kind, "degraded");

    const firstUntil = first.until;

    assert.equal(firstUntil, 0 + 1000, "first cooldown is the first ladder rung");

    // Cooldown elapses; the trial also fails -> escalate to the second rung.
    h.env.clock = firstUntil;
    await assert.rejects(h.sup.acquire("page"), /extension dead/);

    const second = h.sup.inspect();

    assert.equal(second.kind, "degraded");
    assert.equal(second.until, firstUntil + 5000, "the failed trial escalates to the next rung");
  });
});

/* A production-ratio fixture: every cooldown rung is at least as long as the failure window, which is the regime production runs in. The trial that follows such a
 * cooldown therefore fails outside the window, where the window's own lapse-restart would otherwise read the trial as a forgiven first failure.
 */
const TRIAL_POLICY: LaunchGovernorPolicy = { cooldownLadderMs: [ 2000, 8000 ], failureThreshold: 2, failureWindowMs: 1000, healthHoldMs: 30000 };

describe("browserSupervisor: HALF-OPEN trial with a cooldown longer than the failure window", () => {

  test("a failed trial re-trips and escalates even though the failure window lapsed during the cooldown", async () => {

    const h = makeHarness(TRIAL_POLICY);

    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire("page"), /extension dead/);

    h.env.clock = 100;
    await assert.rejects(h.sup.acquire("page"), /extension dead/);

    const first = h.sup.inspect();

    assert.equal(first.kind, "degraded");

    const firstUntil = first.until;

    assert.equal(firstUntil, 100 + 2000, "the second failure trips to the first rung");

    // The cooldown outlasts the failure window, so the trial's failure arrives after the window has lapsed - the case the trial rule exists for.
    h.env.clock = firstUntil;
    await assert.rejects(h.sup.acquire("page"), /extension dead/);

    const second = h.sup.inspect();

    assert.equal(second.kind, "degraded", "a failed trial cools down again rather than returning to absent");
    assert.equal(second.until, firstUntil + 8000, "the failed trial escalates to the next rung");
    assert.equal(h.env.launchCalls, 3, "two initial launches plus the one trial");

    // Still cooling: a request arriving before the escalated horizon is rejected without spawning Chrome.
    await assert.rejects(h.sup.acquire("page"), BrowserUnavailableError);
    assert.equal(h.env.launchCalls, 3, "no launch is attempted while the escalated cooldown holds");
  });
});

describe("browserSupervisor: readiness loss and health-gated reset", () => {

  test("noteReadinessLost from ready returns to absent and relaunches on the next request", async () => {

    const h = makeHarness();

    await h.sup.acquire("page");
    assert.equal(h.sup.inspect().kind, "ready");

    h.sup.noteReadinessLost();

    assert.equal(h.sup.inspect().kind, "absent");
    assert.equal(h.sup.current(), null);

    const browser = await h.sup.acquire("page");

    assert.equal(browser, stubBrowser);
    assert.equal(h.env.launchCalls, 2);
  });

  test("a readiness-loss during an in-flight launch supersedes it: the orphan is closed, not published", async () => {

    // The stale-launch race: a disconnect (or intentional close) fires while a launch is suspended at the launch port. The completing launch must NOT clobber the
    // resulting absent state with a ready browser the supervisor was told to abandon - it closes the orphan and signals the caller to retry.
    const h = makeHarness();
    const { promise, resolve } = Promise.withResolvers<Browser>();

    h.env.launchImpl = (): Promise<Browser> => promise;

    const acquired = h.sup.acquire("page");

    assert.equal(h.sup.inspect().kind, "launching");

    // Readiness is lost while the launch is still in flight.
    h.sup.noteReadinessLost();
    assert.equal(h.sup.inspect().kind, "absent");

    // The launch now completes - but it was superseded.
    resolve(stubBrowser);

    await assert.rejects(acquired, BrowserSupersededError, "the superseded launch rejects so the initiating request retries");
    assert.equal(h.sup.inspect().kind, "absent", "the superseded launch did not clobber absent with ready");
    assert.equal(h.sup.current(), null);
    assert.equal(h.env.closeCalls, 1, "the orphaned browser was closed to avoid a leaked Chrome process");
  });

  test("sustained readiness resets the governor, so accrued failures no longer count toward a trip", async () => {

    const h = makeHarness();

    // Trip to degraded, then recover via a trial.
    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire("page"), /extension dead/);
    await assert.rejects(h.sup.acquire("page"), /extension dead/);

    const degraded = h.sup.inspect();

    assert.equal(degraded.kind, "degraded");

    h.env.clock = degraded.until;
    h.env.launchImpl = (): Promise<Browser> => Promise.resolve(stubBrowser);
    await h.sup.acquire("page");
    assert.equal(h.sup.inspect().kind, "ready");

    // Before the hold elapses, the health tick does not reset.
    assert.equal(h.sup.noteSustainedHealth(), false);

    // After the hold, the tick resets the governor to CLOSED.
    h.env.clock += POLICY.healthHoldMs;
    assert.equal(h.sup.noteSustainedHealth(), true, "sustained readiness recovers to CLOSED");

    // Prove the reset: a single subsequent failure no longer trips (fresh window), so we land in absent rather than degraded.
    h.sup.noteReadinessLost();
    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire("page"), /extension dead/);

    assert.equal(h.sup.inspect().kind, "absent", "after the reset a lone failure does not trip the governor");
  });
});

describe("browserSupervisor: the teardown drain", () => {

  test("a teardown holds the lifecycle closed to launches and rejects acquires with the drain's horizon", async () => {

    const h = makeHarness();

    await h.sup.acquire("page");
    assert.equal(h.sup.inspect().kind, "ready");

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: teardown } = Promise.withResolvers<void>();

    // The adapter's order: retire the session, then publish the teardown of the Chrome that is still exiting.
    h.env.clock = 9000;
    h.sup.noteReadinessLost();
    h.sup.noteTeardownBegun(teardown, 7000);

    const closing = h.sup.inspect();

    assert.equal(closing.kind, "closing");
    assert.equal(closing.until, 9000 + 7000, "the horizon is the drain bound measured from the moment the teardown began");

    await assert.rejects(h.sup.acquire("page"), (error: unknown) => {

      assert.ok(error instanceof BrowserUnavailableError, "a request mid-drain gets the retryable rejection, not a second Chrome");
      assert.equal(error.retryAfter, 16000, "and carries the drain's horizon as its retry-after");

      return true;
    });

    assert.equal(h.env.launchCalls, 1, "no launch is attempted while the retiring Chrome still holds the profile lock");
  });

  test("the settled teardown returns the lifecycle to absent so the next request launches fresh", async () => {

    const h = makeHarness();

    await h.sup.acquire("page");

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: teardown, resolve } = Promise.withResolvers<void>();

    h.sup.noteReadinessLost();
    h.sup.noteTeardownBegun(teardown, 7000);
    assert.equal(h.sup.inspect().kind, "closing");

    resolve();
    await teardown;

    assert.equal(h.sup.inspect().kind, "absent", "the drain is over, so the launch window reopens");

    const browser = await h.sup.acquire("page");

    assert.equal(browser, stubBrowser);
    assert.equal(h.env.launchCalls, 2, "the next request launches a fresh browser");
  });

  test("a teardown that fails still ends the session", async () => {

    // A close that could not confirm the process exited leaves the lifecycle in exactly the same place a clean one does: absent, ready to launch. Any Chrome that
    // outlived the close is the stale-process sweep's problem at the next launch, not a reason to hold the launch window shut indefinitely.
    const h = makeHarness();

    await h.sup.acquire("page");

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: teardown, reject } = Promise.withResolvers<void>();

    h.sup.noteReadinessLost();
    h.sup.noteTeardownBegun(teardown, 7000);

    reject(new Error("chrome would not exit"));
    await assert.rejects(teardown, /chrome would not exit/);

    assert.equal(h.sup.inspect().kind, "absent");
    assert.equal(await h.sup.acquire("page"), stubBrowser);
    assert.equal(h.env.launchCalls, 2, "a failed close does not strand the lifecycle in closing");
  });

  test("a teardown begun without a preceding readiness loss still supersedes an in-flight launch", async () => {

    // The adapter always relinquishes readiness first, so this order does not arise in production. The assertion exists because the method advances the launch epoch
    // itself rather than trusting the caller to have done it, and that self-sufficiency is what makes the contract safe to reason about at any call site.
    const h = makeHarness();
    const { promise: launch, resolve: completeLaunch } = Promise.withResolvers<Browser>();

    h.env.launchImpl = (): Promise<Browser> => launch;

    const acquired = h.sup.acquire("page");

    assert.equal(h.sup.inspect().kind, "launching");

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: teardown, resolve: settleTeardown } = Promise.withResolvers<void>();

    h.sup.noteTeardownBegun(teardown, 7000);
    assert.equal(h.sup.inspect().kind, "closing");

    completeLaunch(stubBrowser);

    await assert.rejects(acquired, BrowserSupersededError, "the launch that was in flight does not publish over the teardown");
    assert.equal(h.env.closeCalls, 1, "the orphaned browser was closed rather than leaked");
    assert.equal(h.sup.inspect().kind, "closing", "and the drain is still in progress");

    settleTeardown();
    await teardown;

    assert.equal(h.sup.inspect().kind, "absent");
  });

  test("a later teardown episode owns the state, and the earlier one settling cannot end it", async () => {

    // Each episode arms its own exit handler against the exact state it published, so a drain that settles after the lifecycle has moved on finds a state it does
    // not own and leaves it alone. The adapter never opens a second teardown over an unsettled one; only a direct call at the contract level reaches this.
    const h = makeHarness();

    await h.sup.acquire("page");

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: firstDrain, resolve: settleFirst } = Promise.withResolvers<void>();

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: secondDrain, resolve: settleSecond } = Promise.withResolvers<void>();

    // Distinct bounds so the horizon identifies which episode owns the state.
    h.env.clock = 9000;
    h.sup.noteReadinessLost();
    h.sup.noteTeardownBegun(firstDrain, 7000);
    h.sup.noteTeardownBegun(secondDrain, 3000);

    const second = h.sup.inspect();

    assert.equal(second.kind, "closing");
    assert.equal(second.until, 9000 + 3000, "the later episode owns the state");

    settleFirst();
    await firstDrain;

    const afterFirst = h.sup.inspect();

    assert.equal(afterFirst.kind, "closing", "the earlier episode's settle does not end a drain it no longer owns");
    assert.equal(afterFirst.until, 9000 + 3000, "and leaves the later episode's horizon untouched");

    settleSecond();
    await secondDrain;

    assert.equal(h.sup.inspect().kind, "absent", "the owning episode's settle returns the lifecycle to absent");
  });
});

describe("browserSupervisor: capture impairment", () => {

  test("marks the published browser without disturbing anything else about it", async () => {

    /* The mark is a fact recorded on the instance that earned it, so nothing else about the ready state moves: the same browser is still current, and the launch
     * time still reads the moment the launch completed rather than the moment the mark landed. The clock is advanced between the two so a `since` that echoed the
     * launch time, or a hardcoded zero, would fail the reading.
     */
    const h = makeHarness();

    await h.sup.acquire("page");

    const before = h.transitions.length;

    h.env.clock = 5000;

    assert.equal(h.sup.noteCaptureImpaired(stubBrowser, "Could not start video source"), true, "the first mark on the published browser records");
    assert.deepEqual(h.sup.captureImpairment(), { reason: "Could not start video source", since: 5000 }, "the record carries the reason and the mark's own clock");
    assert.equal(h.sup.current(), stubBrowser, "the browser is still published");
    assert.equal(h.sup.currentLaunchTime(), 0, "and its launch time is untouched by the mark");
    assert.deepEqual(h.transitions.slice(before), ["ready:impaired"], "the observer fired exactly once, for the transition carrying the mark");
  });

  test("declines a mark aimed at an instance that is no longer the published one", async () => {

    // The identity guard: a caller confirms a specific browser, and a disconnect plus relaunch during that confirmation can have replaced it. A verdict about the
    // instance that is gone must never land on the one that took its place.
    const h = makeHarness();

    await h.sup.acquire("page");

    assert.equal(h.sup.noteCaptureImpaired(otherBrowser, "Could not start video source"), false, "a mark aimed elsewhere does not record");
    assert.equal(h.sup.captureImpairment(), null, "and the published browser stays unmarked");
  });

  test("declines a second mark on an already-marked instance and keeps the first record", async () => {

    // First mark wins. The adapter's alarm and its status emit both belong to the transition that records the mark, so a second verdict against the same
    // instance must not fire either of them again - which is what the transition count proves - and the record it would have overwritten stays as it was.
    const h = makeHarness();

    await h.sup.acquire("page");

    h.env.clock = 5000;
    h.sup.noteCaptureImpaired(stubBrowser, "Could not start video source");

    const after = h.transitions.length;

    h.env.clock = 60000;

    assert.equal(h.sup.noteCaptureImpaired(stubBrowser, "a later, different failure"), false, "the second mark does not record");
    assert.deepEqual(h.sup.captureImpairment(), { reason: "Could not start video source", since: 5000 }, "the first reason and its timestamp survive");
    assert.equal(h.transitions.length, after, "and no transition fired");
  });

  test("refuses a capture acquire on a marked browser without launching, and still serves a page acquire", async () => {

    /* The refusal is the loop bound applied to the mark: a second Chrome against the profile lock the still-serving instance holds is never the answer, so the
     * launch port must not be touched. The page acquire proves the refusal is scoped to the operation the browser can no longer perform.
     */
    const h = makeHarness();

    await h.sup.acquire("page");

    h.env.clock = 5000;
    h.sup.noteCaptureImpaired(stubBrowser, "Could not start video source");

    const launches = h.env.launchCalls;

    await assert.rejects(() => h.sup.acquire("capture"), (error: unknown) => (error instanceof BrowserCaptureImpairedError) &&
      (error.impairment.reason === "Could not start video source") && (error.impairment.since === 5000), "a capture acquire rejects with the recorded impairment");

    assert.equal(h.env.launchCalls, launches, "and no launch was attempted");
    assert.equal(await h.sup.acquire("page"), stubBrowser, "a page acquire still returns the marked browser");
    assert.equal(h.env.launchCalls, launches, "and it launched nothing either");
  });

  test("serves a capture acquire on an unmarked ready browser without launching", async () => {

    // The other half of the guard. A guard that dropped the impairment term, or inverted it, would refuse here - and would pass every assertion that only ever asks a
    // marked browser.
    const h = makeHarness();

    await h.sup.acquire("page");

    const launches = h.env.launchCalls;

    assert.equal(await h.sup.acquire("capture"), stubBrowser, "an unmarked ready browser serves a capture acquire");
    assert.equal(h.env.launchCalls, launches, "from the published instance, with no launch");
  });

  test("the mark dies with the instance: a readiness loss and relaunch publishes an unmarked browser", async () => {

    // The member exists only on the ready variant, so there is nowhere for a mark to survive a readiness loss. The relaunch is a real one - the launch count is
    // what proves the fresh ready state came from the port rather than from a state that was never left.
    const h = makeHarness();

    await h.sup.acquire("page");

    h.env.clock = 5000;
    h.sup.noteCaptureImpaired(stubBrowser, "Could not start video source");
    h.sup.noteReadinessLost();

    assert.equal(h.sup.captureImpairment(), null, "the mark is gone the moment readiness is");

    const launches = h.env.launchCalls;

    await h.sup.acquire("page");

    assert.equal(h.env.launchCalls, launches + 1, "the next acquire launched a fresh instance");
    assert.equal(h.sup.inspect().kind, "ready", "which is published ready");
    assert.equal(h.sup.captureImpairment(), null, "carrying no mark");
  });

  test("declines a mark when nothing is published and while a launch is still in flight", async () => {

    /* A verdict needs a published instance to attach to. From absent there is none, and mid-launch the browser the caller would be describing has not been
     * published yet - the state holds the launch's promise, not a browser.
     */
    const h = makeHarness();

    assert.equal(h.sup.noteCaptureImpaired(stubBrowser, "Could not start video source"), false, "nothing published, nothing to mark");

    const { promise: launched, resolve: finishLaunch } = Promise.withResolvers<Browser>();

    h.env.launchImpl = (): Promise<Browser> => launched;

    const acquiring = h.sup.acquire("page");

    assert.equal(h.sup.inspect().kind, "launching", "the launch is in flight");
    assert.equal(h.sup.noteCaptureImpaired(stubBrowser, "Could not start video source"), false, "and a mark against it does not record");

    finishLaunch(stubBrowser);

    await acquiring;

    assert.equal(h.sup.captureImpairment(), null, "the browser the launch published is unmarked");
  });
});
