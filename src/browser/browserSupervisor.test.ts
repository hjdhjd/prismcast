/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * browserSupervisor.test.ts: Unit tests for the browser capture-readiness supervisor. The supervisor's launch is an injected port and its clock is an injected
 * now(), so the entire lifecycle state machine - the gate, the loop-safe governor wiring, and every transition - is exercised here with a fake launch and a fake
 * clock, without ever spawning Chrome. The headline guarantees pinned below: single-flight launches, and NO launch attempt while the governor is cooling (the
 * loop bound).
 */
import { BrowserSupersededError, BrowserUnavailableError, createBrowserSupervisor } from "./browserSupervisor.ts";
import { describe, test } from "node:test";
import type { Browser } from "puppeteer-core";
import type { BrowserSupervisorPorts } from "./browserSupervisor.ts";
import type { LaunchGovernorPolicy } from "./launchGovernor.ts";
import assert from "node:assert/strict";

// Trips on the 2nd failed launch within a 10s window; cooldown ladder 1s -> 5s; 30s of sustained readiness resets to CLOSED.
const POLICY: LaunchGovernorPolicy = { cooldownLadderMs: [ 1000, 5000 ], failureThreshold: 2, failureWindowMs: 10000, healthHoldMs: 30000 };

// The supervisor never calls any Browser method, so an opaque stub suffices as the "launched" instance.
const stubBrowser = {} as unknown as Browser;

/* Test harness: a controllable launch port and clock. `env.launchImpl` is the per-test launch behavior (resolve a stub, reject, or return a deferred promise for
 * single-flight); `env.launchCalls` counts how many times the port was actually invoked - the assertion that proves the loop bound; `env.clock` is the injected now.
 */
function makeHarness(): {
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
    onStateChange: (next): void => { transitions.push(next.kind); },
    policy: (): LaunchGovernorPolicy => POLICY
  };

  return { env, sup: createBrowserSupervisor(ports), transitions };
}

const failLaunch = (): Promise<Browser> => Promise.reject(new Error("extension dead"));

describe("browserSupervisor: acquire happy path", () => {

  test("launches lazily from absent and becomes ready", async () => {

    const h = makeHarness();

    assert.equal(h.sup.inspect().kind, "absent");

    const browser = await h.sup.acquire();

    assert.equal(browser, stubBrowser);
    assert.equal(h.sup.inspect().kind, "ready");
    assert.equal(h.env.launchCalls, 1);
    assert.equal(h.sup.current(), stubBrowser);
  });

  test("returns the ready browser without relaunching", async () => {

    const h = makeHarness();

    await h.sup.acquire();
    const again = await h.sup.acquire();

    assert.equal(again, stubBrowser);
    assert.equal(h.env.launchCalls, 1, "a ready browser is reused, not relaunched");
  });

  test("currentLaunchTime reflects the moment the browser became ready", async () => {

    const h = makeHarness();

    h.env.clock = 4242;
    await h.sup.acquire();

    assert.equal(h.sup.currentLaunchTime(), 4242);
  });
});

describe("browserSupervisor: single-flight", () => {

  test("concurrent acquire calls join one in-flight launch", async () => {

    const h = makeHarness();
    const { promise, resolve } = Promise.withResolvers<Browser>();

    h.env.launchImpl = (): Promise<Browser> => promise;

    const a = h.sup.acquire();
    const b = h.sup.acquire();

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
    await assert.rejects(h.sup.acquire(), /extension dead/);

    assert.equal(h.sup.inspect().kind, "absent", "below the threshold there is no cooldown");

    // The next request relaunches immediately - the common transient costs no cooldown.
    h.env.launchImpl = (): Promise<Browser> => Promise.resolve(stubBrowser);

    const browser = await h.sup.acquire();

    assert.equal(browser, stubBrowser);
    assert.equal(h.env.launchCalls, 2);
  });

  test("reaching the threshold trips to degraded and then rejects WITHOUT spawning Chrome (the loop bound)", async () => {

    const h = makeHarness();

    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire(), /extension dead/);
    await assert.rejects(h.sup.acquire(), /extension dead/);

    const degraded = h.sup.inspect();

    assert.equal(degraded.kind, "degraded");
    assert.equal(h.env.launchCalls, 2, "two real launch attempts so far");

    // While cooling, acquire must fail fast WITHOUT a third launch - this is what bounds the request-driven relaunch loop.
    await assert.rejects(h.sup.acquire(), BrowserUnavailableError);
    await assert.rejects(h.sup.acquire(), BrowserUnavailableError);

    assert.equal(h.env.launchCalls, 2, "no Chrome is spawned while the governor is cooling, regardless of request volume");
  });
});

describe("browserSupervisor: HALF-OPEN trial and escalation", () => {

  test("once the cooldown elapses a single trial launches; success becomes ready", async () => {

    const h = makeHarness();

    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire(), /extension dead/);
    await assert.rejects(h.sup.acquire(), /extension dead/);

    const degraded = h.sup.inspect();

    assert.equal(degraded.kind, "degraded");

    // Advance past the cooldown; the next acquire is the HALF-OPEN trial. assert.equal above narrows the union by its kind tag, so .until is directly accessible.
    h.env.clock = degraded.until;
    h.env.launchImpl = (): Promise<Browser> => Promise.resolve(stubBrowser);

    const browser = await h.sup.acquire();

    assert.equal(browser, stubBrowser);
    assert.equal(h.sup.inspect().kind, "ready");
    assert.equal(h.env.launchCalls, 3, "exactly one trial launch after the cooldown");
  });

  test("a failed trial escalates the cooldown along the ladder", async () => {

    const h = makeHarness();

    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire(), /extension dead/);
    await assert.rejects(h.sup.acquire(), /extension dead/);

    const first = h.sup.inspect();

    assert.equal(first.kind, "degraded");

    const firstUntil = first.until;

    assert.equal(firstUntil, 0 + 1000, "first cooldown is the first ladder rung");

    // Cooldown elapses; the trial also fails -> escalate to the second rung.
    h.env.clock = firstUntil;
    await assert.rejects(h.sup.acquire(), /extension dead/);

    const second = h.sup.inspect();

    assert.equal(second.kind, "degraded");
    assert.equal(second.until, firstUntil + 5000, "the failed trial escalates to the next rung");
  });
});

describe("browserSupervisor: readiness loss and health-gated reset", () => {

  test("noteReadinessLost from ready returns to absent and relaunches on the next request", async () => {

    const h = makeHarness();

    await h.sup.acquire();
    assert.equal(h.sup.inspect().kind, "ready");

    h.sup.noteReadinessLost();

    assert.equal(h.sup.inspect().kind, "absent");
    assert.equal(h.sup.current(), null);

    const browser = await h.sup.acquire();

    assert.equal(browser, stubBrowser);
    assert.equal(h.env.launchCalls, 2);
  });

  test("a readiness-loss during an in-flight launch supersedes it: the orphan is closed, not published", async () => {

    // The stale-launch race: a disconnect (or intentional close) fires while a launch is suspended at the launch port. The completing launch must NOT clobber the
    // resulting absent state with a ready browser the supervisor was told to abandon - it closes the orphan and signals the caller to retry.
    const h = makeHarness();
    const { promise, resolve } = Promise.withResolvers<Browser>();

    h.env.launchImpl = (): Promise<Browser> => promise;

    const acquired = h.sup.acquire();

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
    await assert.rejects(h.sup.acquire(), /extension dead/);
    await assert.rejects(h.sup.acquire(), /extension dead/);

    const degraded = h.sup.inspect();

    assert.equal(degraded.kind, "degraded");

    h.env.clock = degraded.until;
    h.env.launchImpl = (): Promise<Browser> => Promise.resolve(stubBrowser);
    await h.sup.acquire();
    assert.equal(h.sup.inspect().kind, "ready");

    // Before the hold elapses, the health tick does not reset.
    assert.equal(h.sup.noteSustainedHealth(), false);

    // After the hold, the tick resets the governor to CLOSED.
    h.env.clock += POLICY.healthHoldMs;
    assert.equal(h.sup.noteSustainedHealth(), true, "sustained readiness recovers to CLOSED");

    // Prove the reset: a single subsequent failure no longer trips (fresh window), so we land in absent rather than degraded.
    h.sup.noteReadinessLost();
    h.env.launchImpl = failLaunch;
    await assert.rejects(h.sup.acquire(), /extension dead/);

    assert.equal(h.sup.inspect().kind, "absent", "after the reset a lone failure does not trip the governor");
  });
});
