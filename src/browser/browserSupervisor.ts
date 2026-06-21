/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * browserSupervisor.ts: The browser capture-readiness supervisor - the testable lifecycle state machine that owns "a published browser is capture-ready."
 */
import type { LaunchGovernorPolicy, LaunchGovernorState } from "./launchGovernor.ts";
import { canAttemptLaunch, createLaunchGovernorState, noteLaunchFailure, noteLaunchSuccess, noteReadinessLost, noteSustainedHealth } from "./launchGovernor.ts";
import type { Browser } from "puppeteer-core";
import type { Nullable } from "../types/index.ts";

/* The supervisor models the browser as a fallible external dependency whose single invariant is: a published browser is capture-ready. It is deliberately pure of
 * Chrome I/O - the actual launch (spawn Chrome, verify the capture extension, capture display/version) is injected as the `launch` port, and the impure adapter in
 * browser/index.ts provides it. That inversion makes the riskiest logic - the gate, the loop-safe governor, and the lifecycle transitions - fully unit-testable
 * with a fake launch and a fake clock, which is exactly the logic the original outage proved must be tested. The supervisor never reads the wall clock, never logs,
 * and never touches Chrome directly; it threads an injected now() and reports transitions through an optional callback so the adapter can log and alarm.
 *
 * The lifecycle is one explicit discriminated union (no parallel booleans or scattered nullables): the breaker's OPEN/HALF-OPEN are the `degraded`/`trialing`
 * states, so there is exactly one state machine. acquire() is the single entry point that lazily launches and gates: it returns a ready browser, joins an in-flight
 * launch, or - while the governor is cooling - rejects fast WITHOUT launching, which is what decouples relaunch from request arrival and bounds the loop.
 */

/**
 * The browser lifecycle. The Browser reference lives inside the `ready` variant, so "is it ready" cannot desync from "do we have a browser" the way a separate
 * boolean flag could, and illegal states (ready-and-launching) are unrepresentable. `degraded` is the governor's OPEN state (cooling, not serving); `trialing` is
 * its HALF-OPEN state (one launch in flight after the cooldown elapsed).
 */
export type BrowserLifecycle =
  { readonly kind: "absent" } |
  { readonly kind: "launching"; readonly promise: Promise<Browser> } |
  { readonly kind: "ready"; readonly browser: Browser; readonly launchTime: number } |
  { readonly kind: "degraded"; readonly reason: string; readonly until: number } |
  { readonly kind: "trialing"; readonly promise: Promise<Browser> };

/**
 * The injected dependencies. `launch` performs the real, gated launch - it must resolve only with a capture-ready browser and reject otherwise (the adapter runs
 * the readiness gate inside it). `now` is the time source (realClock.now in production, a fake in tests). `policy` bounds the governor; it is a getter, read fresh
 * at each governor decision, so the adapter can source the bounds from live configuration and an operator's change takes effect without reconstructing the
 * supervisor. `onStateChange` is an optional observer the adapter uses to log transitions and raise the loud degraded alarm; the supervisor itself stays log-free.
 */
export interface BrowserSupervisorPorts {

  // Closes a browser. Used only to clean up an orphan: a launch that completed after a readiness-loss superseded it. Normal teardown (scheduled restart) is the
  // adapter's concern.
  readonly close: (browser: Browser) => Promise<void>;

  readonly launch: () => Promise<Browser>;
  readonly now: () => number;
  readonly onStateChange?: (next: BrowserLifecycle, previous: BrowserLifecycle) => void;
  readonly policy: () => LaunchGovernorPolicy;
}

/**
 * The supervisor surface. acquire() is the lazy, gated launcher (the getCurrentBrowser core); current()/currentLaunchTime() are non-launching reads; the note*
 * methods feed lifecycle events; noteSustainedHealth() drives the health-gated governor reset; inspect() exposes the state for the adapter and tests.
 */
export interface BrowserSupervisor {

  // Returns a capture-ready browser, joining any in-flight launch. Launches lazily when absent or when a cooldown has elapsed (a trial). Rejects without launching
  // while the governor is cooling - the loop-safety property - and rejects with the underlying error when a launch fails.
  readonly acquire: () => Promise<Browser>;

  // The ready browser, or null when not ready. Does not launch.
  readonly current: () => Nullable<Browser>;

  // The timestamp at which the current browser became ready, or null when not ready. Consumed by the adapter's age-based scheduled-restart check.
  readonly currentLaunchTime: () => Nullable<number>;

  // The current lifecycle state. For the adapter (to map degraded -> 503 / alarm) and for tests.
  readonly inspect: () => BrowserLifecycle;

  // Records that readiness was lost - an unexpected disconnect or an intentional close (scheduled restart). Transitions to absent and clears the governor's health
  // anchor. Not a launch failure: failures are counted only when a launch attempt fails, never on a disconnect.
  readonly noteReadinessLost: () => void;

  // Periodic health tick. Resets the governor to CLOSED once the browser has been continuously ready for the policy's hold. Returns true when this tick performed
  // the reset, so the adapter can log the recovery.
  readonly noteSustainedHealth: () => boolean;
}

/**
 * Error thrown by acquire() while the governor is cooling (degraded). Carries the timestamp until which launches are blocked so the adapter can surface a
 * Retry-After and a 503-class back-off rather than spawning Chrome. Distinct from a launch failure (which rejects with the underlying launch error).
 */
export class BrowserUnavailableError extends Error {

  public readonly retryAfter: number;

  constructor(retryAfter: number) {

    super("The browser is temporarily unavailable while its capture system recovers.");

    this.name = "BrowserUnavailableError";
    this.retryAfter = retryAfter;
  }
}

/**
 * Thrown by acquire() when a launch in flight was superseded by a readiness-loss (a disconnect or an intentional close) before it completed. The browser it
 * produced has been closed; the caller should retry, which will launch fresh or hit the governor's gate. This keeps an abandoned launch from publishing a browser
 * the supervisor was told to discard.
 */
export class BrowserSupersededError extends Error {

  constructor() {

    super("The browser launch was superseded before it completed.");

    this.name = "BrowserSupersededError";
  }
}

/**
 * Creates a browser supervisor over the injected ports. The returned object owns the lifecycle and governor state for the process lifetime; the adapter holds one
 * instance and routes all browser access through it.
 * @param ports - The injected launch, clock, policy, and optional transition observer.
 * @returns The supervisor surface.
 */
export function createBrowserSupervisor(ports: BrowserSupervisorPorts): BrowserSupervisor {

  const governor: LaunchGovernorState = createLaunchGovernorState();

  let state: BrowserLifecycle = { kind: "absent" };

  // Monotonic launch epoch. Incremented whenever readiness is lost so an in-flight launch can detect it was superseded and refuse to publish a browser the
  // supervisor was told to abandon (the stale-launch race). Each launch captures the epoch at its start and checks it before mutating state.
  let generation = 0;

  // Single transition chokepoint so every state change flows through one place and notifies the observer. The observer is best-effort - it never affects the
  // transition - so a throwing logger cannot corrupt the lifecycle.
  function transition(next: BrowserLifecycle): void {

    const previous = state;

    state = next;

    ports.onStateChange?.(next, previous);
  }

  // Drives one launch attempt to completion, transitioning to ready on success or to degraded/absent on failure, and feeding the governor either way. Returns the
  // browser on success and rejects with the underlying error on failure. The state is moved to launching/trialing by the caller before this resolves.
  async function runLaunch(launchGeneration: number): Promise<Browser> {

    let browser: Browser;

    try {

      browser = await ports.launch();
    } catch(error) {

      // Record the failure and move the state only if this launch is still the current one. A readiness-loss during the launch supersedes it (a newer epoch owns
      // the state), and a superseded launch must not count a governor failure or move the state - the caller's request will retry.
      if(launchGeneration === generation) {

        const outcome = noteLaunchFailure(governor, ports.now(), ports.policy());

        // A trip moves us to degraded (cooling, no launches until the cooldown elapses); below the threshold we return to absent so the next request relaunches
        // immediately - the common transient costs no cooldown.
        if(outcome.tripped && (outcome.cooldownUntil !== null)) {

          transition({ kind: "degraded", reason: "Browser launch failed and the relaunch governor tripped.", until: outcome.cooldownUntil });
        } else {

          transition({ kind: "absent" });
        }
      }

      throw error;
    }

    // The launch succeeded. If a readiness-loss superseded it while it was in flight, this browser was abandoned: close it (best effort) so it does not leak a
    // Chrome process, and signal the caller to retry rather than clobbering the current state with a browser that should never have been published.
    if(launchGeneration !== generation) {

      try {

        await ports.close(browser);
      } catch {

        // Best effort: a failed orphan close is the adapter's close to log; the supervisor still reports supersession.
      }

      throw new BrowserSupersededError();
    }

    noteLaunchSuccess(governor, ports.now());
    transition({ browser, kind: "ready", launchTime: ports.now() });

    return browser;
  }

  /* Begins a launch in the given phase (launching for a normal acquire, trialing for a post-cooldown trial) and publishes the in-flight promise into the state so
   * concurrent acquire() callers join it rather than starting a second Chrome. The body deliberately has no await: it starts runLaunch (which suspends at the
   * launch port) and publishes the launching/trialing state synchronously, before returning, so the single-flight invariant holds. It is async only to satisfy the
   * promise-returning-function convention; the synchronous publish is what matters for correctness.
   */
  async function beginLaunch(kind: "launching" | "trialing"): Promise<Browser> {

    const promise = runLaunch(generation);

    transition({ kind, promise });

    return promise;
  }

  async function acquire(): Promise<Browser> {

    switch(state.kind) {

      case "ready": {

        return state.browser;
      }

      // Join the launch already in flight rather than starting a second one - the single-flight guarantee.
      case "launching":
      case "trialing": {

        return state.promise;
      }

      // Cooling: the cooldown gates the relaunch, decoupled from request arrival. If it has elapsed, run one trial (HALF-OPEN); otherwise reject fast without
      // spawning Chrome (the loop bound).
      case "degraded": {

        if(canAttemptLaunch(governor, ports.now())) {

          return beginLaunch("trialing");
        }

        throw new BrowserUnavailableError(state.until);
      }

      case "absent": {

        // canAttemptLaunch is true from absent (no cooldown is set), but we consult it for symmetry and future-proofing.
        if(canAttemptLaunch(governor, ports.now())) {

          return beginLaunch("launching");
        }

        throw new BrowserUnavailableError(ports.now());
      }

      default: {

        return assertNever(state);
      }
    }
  }

  function current(): Nullable<Browser> {

    return (state.kind === "ready") ? state.browser : null;
  }

  function currentLaunchTime(): Nullable<number> {

    return (state.kind === "ready") ? state.launchTime : null;
  }

  function handleReadinessLost(): void {

    // Advance the epoch first so any launch already in flight is superseded and cannot publish its browser over this readiness-loss.
    generation++;

    noteReadinessLost(governor);

    // Only meaningful from ready/launching/trialing; from absent/degraded it is a harmless no-op that keeps the call site simple.
    if((state.kind === "ready") || (state.kind === "launching") || (state.kind === "trialing")) {

      transition({ kind: "absent" });
    }
  }

  function noteSustainedHealthTick(): boolean {

    if(state.kind !== "ready") {

      return false;
    }

    return noteSustainedHealth(governor, ports.now(), ports.policy());
  }

  return { acquire, current, currentLaunchTime, inspect: () => state, noteReadinessLost: handleReadinessLost, noteSustainedHealth: noteSustainedHealthTick };
}

/**
 * Exhaustiveness guard for the lifecycle switch: referencing a `never` makes the compiler flag any unhandled variant if the union grows.
 * @param value - The unreachable value.
 * @returns Never returns.
 */
function assertNever(value: never): never {

  throw new Error("Unhandled browser lifecycle state: " + JSON.stringify(value));
}
