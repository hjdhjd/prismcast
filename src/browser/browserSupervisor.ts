/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * browserSupervisor.ts: The browser capture-readiness supervisor - the testable lifecycle state machine that owns "a published browser is capture-ready."
 */
import type { LaunchGovernorPolicy, LaunchGovernorState } from "./launchGovernor.ts";
import { canAttemptLaunch, createLaunchGovernorState, noteLaunchFailure, noteLaunchSuccess, noteReadinessLost, noteSustainedHealth } from "./launchGovernor.ts";
import type { Browser } from "puppeteer-core";
import type { Nullable } from "../types/index.ts";

/* The supervisor models the browser as a fallible external dependency whose single guarantee is: a published browser is capture-ready, or carries the record that
 * says it can no longer start one. It is deliberately pure of Chrome I/O - the actual launch (spawn Chrome, verify the capture extension, capture display/version)
 * is injected as the `launch` port, and the impure adapter in browser/index.ts provides it. That inversion makes the riskiest logic - the gate, the loop-safe
 * governor, and the lifecycle transitions - fully unit-testable with a fake launch and a fake clock, which is exactly the logic the original outage proved must be
 * tested. The supervisor never reads the wall clock, never logs, and never touches Chrome directly; it threads an injected now() and reports transitions through an
 * optional callback so the adapter can log and alarm.
 *
 * The lifecycle is one explicit discriminated union (no parallel booleans or scattered nullables): the breaker's OPEN/HALF-OPEN are the `degraded`/`trialing`
 * states, so there is exactly one state machine. acquire() is the single entry point that lazily launches and gates: it returns a ready browser, joins an in-flight
 * launch, or - while the governor is cooling - rejects fast WITHOUT launching, which is what decouples relaunch from request arrival and bounds the loop. It also
 * takes the caller's purpose, so a browser that can no longer start a capture refuses a capture acquire outright while still serving the callers that only need a
 * page to open.
 */

/**
 * The record of a browser that can no longer start a capture while the captures it is already running continue. `reason` is the text of the failure that produced
 * the verdict - the probe's message or the description of the wedge - and `since` is the supervisor's clock at the moment the mark was recorded.
 */
export interface CaptureImpairment {

  readonly reason: string;
  readonly since: number;
}

/**
 * What a caller needs the browser for. A "page" caller opens a tab and drives it - precaching, the startup warm-up, the relaunch that follows a restart - while a
 * "capture" caller goes on to start a tab capture for a stream. Naming the purpose at acquire() is what lets a browser that can no longer start captures keep
 * serving the work it can still do.
 */
export type BrowserPurpose = "capture" | "page";

/**
 * The browser lifecycle. The Browser reference lives inside the `ready` variant, so "is it ready" cannot desync from "do we have a browser" the way a separate
 * boolean flag could, and illegal states (ready-and-launching) are unrepresentable. `degraded` is the governor's OPEN state (cooling, not serving); `trialing` is
 * its HALF-OPEN state (one launch in flight after the cooldown elapsed). `closing` is a teardown of a still-running Chrome draining to completion, and its `until`
 * is the conservative bound that requests rejected mid-drain carry as their retry horizon.
 *
 * The `ready` variant's `impairment` is the guarantee's second half: a published browser is capture-ready, or it carries the record that says it no longer is.
 * Because only `ready` can hold that member, the mark cannot outlive the instance it describes - a relaunch publishes a fresh `ready` whose impairment is null, and
 * no state reached in between can carry one forward.
 */
export type BrowserLifecycle =
  { readonly kind: "absent" } |
  { readonly kind: "launching"; readonly promise: Promise<Browser> } |
  { readonly kind: "ready"; readonly browser: Browser; readonly impairment: Nullable<CaptureImpairment>; readonly launchTime: number } |
  { readonly kind: "degraded"; readonly reason: string; readonly until: number } |
  { readonly kind: "trialing"; readonly promise: Promise<Browser> } |
  { readonly kind: "closing"; readonly teardown: Promise<void>; readonly until: number };

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
  // while the governor is cooling - the loop-safety property - and rejects with the underlying error when a launch fails. On a marked browser a "capture" purpose
  // rejects without launching - a second Chrome against the profile lock is never the answer to a browser that is still serving the captures it started - while a
  // "page" purpose returns it.
  readonly acquire: (purpose: BrowserPurpose) => Promise<Browser>;

  // The impairment recorded on the ready browser, or null when nothing is published or the published browser carries no mark. Does not launch.
  readonly captureImpairment: () => Nullable<CaptureImpairment>;

  // The ready browser, or null when not ready. Does not launch.
  readonly current: () => Nullable<Browser>;

  // The timestamp at which the current browser became ready, or null when not ready. Consumed by the adapter's age-based scheduled-restart check.
  readonly currentLaunchTime: () => Nullable<number>;

  // The current lifecycle state. For the adapter (to map degraded -> 503 / alarm) and for tests.
  readonly inspect: () => BrowserLifecycle;

  // Records that the ready browser can no longer start a capture, against the exact instance the caller verified. Records nothing when that instance is no longer
  // the published one, when nothing is published, or when a mark is already held; returns whether this call is the one that recorded it.
  readonly noteCaptureImpaired: (browser: Browser, reason: string) => boolean;

  // Records that readiness was lost - an unexpected disconnect or an intentional close (scheduled restart). Transitions to absent and clears the governor's health
  // anchor. Not a launch failure: failures are counted only when a launch attempt fails, never on a disconnect.
  readonly noteReadinessLost: () => void;

  // Periodic health tick. Resets the governor to CLOSED once the browser has been continuously ready for the policy's hold. Returns true when this tick performed
  // the reset, so the adapter can log the recovery.
  readonly noteSustainedHealth: () => boolean;

  // Records that the adapter has begun tearing down the retired session's still-running Chrome, after noteReadinessLost. The lifecycle holds in closing - acquire
  // rejects retryable and nothing launches - until the teardown settles, at which point it returns to absent. The drain bound is the caller's worst-case estimate
  // of how long the process can take to exit, and becomes the retry horizon the rejections carry.
  readonly noteTeardownBegun: (teardown: Promise<void>, drainBoundMs: number) => void;
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
 * Thrown by acquire() for a capture purpose on a browser that can no longer start captures. The caller maps it to the same 503 back-off class as
 * BrowserUnavailableError, and it carries no retry horizon because the relaunch that cures it waits on the browser's own streams draining rather than on a clock.
 */
export class BrowserCaptureImpairedError extends Error {

  public readonly impairment: CaptureImpairment;

  constructor(impairment: CaptureImpairment) {

    super("The browser can no longer start captures and is waiting to relaunch.");

    this.impairment = impairment;
    this.name = "BrowserCaptureImpairedError";
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
  // browser on success and rejects with the underlying error on failure. The state is moved to launching/trialing by the caller before this resolves. Whether the
  // attempt is the post-cooldown trial arrives as a parameter rather than being read back from the state at settle time: the lifecycle may have moved on by then,
  // and being a trial is a fact about the attempt itself.
  async function runLaunch(launchGeneration: number, isTrial: boolean): Promise<Browser> {

    let browser: Browser;

    try {

      browser = await ports.launch();
    } catch(error) {

      // Record the failure and move the state only if this launch is still the current one. A readiness-loss during the launch supersedes it (a newer epoch owns
      // the state), and a superseded launch must not count a governor failure or move the state - the caller's request will retry.
      if(launchGeneration === generation) {

        const outcome = noteLaunchFailure(governor, ports.now(), ports.policy(), isTrial);

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

        // Best effort: a failed orphan close is the adapter's concern to log; the supervisor still reports supersession.
      }

      throw new BrowserSupersededError();
    }

    noteLaunchSuccess(governor, ports.now());
    transition({ browser, impairment: null, kind: "ready", launchTime: ports.now() });

    return browser;
  }

  /* Begins a launch in the given phase (launching for a normal acquire, trialing for a post-cooldown trial) and publishes the in-flight promise into the state so
   * concurrent acquire() callers join it rather than starting a second Chrome. The body deliberately has no await: it starts runLaunch (which suspends at the
   * launch port) and publishes the launching/trialing state synchronously, before returning, so single-flight holds. It is async only to satisfy the
   * promise-returning-function convention; the synchronous publish is what matters for correctness. The phase this begins in is also what tells the attempt
   * whether it is the trial, so the governor can judge its failure accordingly.
   */
  async function beginLaunch(kind: "launching" | "trialing"): Promise<Browser> {

    const promise = runLaunch(generation, kind === "trialing");

    transition({ kind, promise });

    return promise;
  }

  async function acquire(purpose: BrowserPurpose): Promise<Browser> {

    switch(state.kind) {

      case "ready": {

        // A marked browser still opens pages, so only a capture purpose is refused. Refusing here rather than launching is what keeps a second Chrome away from the
        // profile lock this instance still holds while it serves the captures it started; the relaunch is the adapter's to schedule once nothing depends on it.
        if((purpose === "capture") && state.impairment) {

          throw new BrowserCaptureImpairedError(state.impairment);
        }

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

      // Draining: the retiring Chrome still holds the profile lock, so launching now would spawn a second one against it. Reject on the same retryable class the
      // cooldown uses, carrying the drain's bound as the retry horizon, and let the caller come back once the process is gone.
      case "closing": {

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

  function captureImpairment(): Nullable<CaptureImpairment> {

    return (state.kind === "ready") ? state.impairment : null;
  }

  /* Records the capture impairment against the published ready browser, and reports whether this call is the one that recorded it.
   *
   * The identity guard is what makes a verdict safe to act on late: the caller confirmed a specific instance, and a disconnect plus relaunch during that
   * confirmation can have replaced it, so a mark aimed at a superseded instance is dropped rather than applied to the healthy browser that took its place. The
   * first mark wins for a reason of its own - the adapter's alarm and its status emit both belong to this transition, so a second verdict against the same
   * instance must not fire either of them again.
   */
  function handleCaptureImpaired(browser: Browser, reason: string): boolean {

    if((state.kind !== "ready") || (state.browser !== browser) || (state.impairment !== null)) {

      return false;
    }

    transition({ ...state, impairment: { reason, since: ports.now() } });

    return true;
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

    // Only meaningful from ready/launching/trialing; from absent/degraded/closing it is a harmless no-op that keeps the call site simple. Excluding closing is what
    // keeps a drain intact: that state ends when its teardown settles, not when a caller reports the readiness it has already given up.
    if((state.kind === "ready") || (state.kind === "launching") || (state.kind === "trialing")) {

      transition({ kind: "absent" });
    }
  }

  /* Enters the closing state for a teardown the adapter has begun on the retired session's still-running Chrome, and holds there for the whole drain: acquire
   * rejects retryable rather than launching a second Chrome against the profile lock the exiting one still holds.
   *
   * The method is self-sufficient rather than order-dependent - it advances the epoch and clears the governor's readiness anchor itself - so a teardown published
   * without a preceding readiness-loss still supersedes an in-flight launch. In the adapter's real order readiness is relinquished first, which makes both
   * operations redundant repeats here; that is harmless, and cheaper than a contract that only holds when the calls arrive in one particular order.
   */
  function handleTeardownBegun(teardown: Promise<void>, drainBoundMs: number): void {

    generation++;

    noteReadinessLost(governor);

    const closing: BrowserLifecycle = { kind: "closing", teardown, until: ports.now() + drainBoundMs };

    transition(closing);

    /* Leave closing when this teardown settles, and only while this exact episode is still the current state. Comparing by identity rather than by kind means a
     * state reached after the drain - including a second closing episode - cannot be knocked back to absent by an earlier drain settling late. A rejected teardown
     * exits the same way: a failed close still ends the session, and any Chrome that survived it is reaped by the stale-process sweep at the next launch.
     */
    const exit = (): void => {

      if(state === closing) {

        transition({ kind: "absent" });
      }
    };

    void teardown.then(exit, exit);
  }

  function noteSustainedHealthTick(): boolean {

    if(state.kind !== "ready") {

      return false;
    }

    return noteSustainedHealth(governor, ports.now(), ports.policy());
  }

  return { acquire, captureImpairment, current, currentLaunchTime, inspect: () => state, noteCaptureImpaired: handleCaptureImpaired,
    noteReadinessLost: handleReadinessLost, noteSustainedHealth: noteSustainedHealthTick, noteTeardownBegun: handleTeardownBegun };
}

/**
 * Exhaustiveness guard for the lifecycle switch: referencing a `never` makes the compiler flag any unhandled variant if the union grows.
 * @param value - The unreachable value.
 * @returns Never returns.
 */
function assertNever(value: never): never {

  throw new Error("Unhandled browser lifecycle state: " + JSON.stringify(value));
}
