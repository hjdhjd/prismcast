/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * captureLock.ts: The task-scoped serializer for Chrome capture initialization.
 *
 * Chrome's tabCapture extension allows only one capture acquisition in flight process-wide; a second concurrent start throws "Cannot capture a tab with an
 * active stream". This primitive serializes capture initialization so that collision is impossible by
 * construction: a caller hands the lock the capture-init work, and the lock owns the turn for the whole lifetime of that work. The turn releases only when the
 * work's promise settles - never on a caller timeout, a caller's catch, or a waiter giving up - so no two tasks ever run at once.
 *
 * The lock is a pure primitive: it takes an injected Clock and per-task callbacks, imports nothing from the browser layer, and captures nothing from configuration.
 * Every timing bound arrives per run() call, because setup.ts (the composition point that constructs the one production lock) has its module body evaluated before
 * configuration is initialized, so a bound captured at construction would be stale. The caller owns what its task does and whether recovery may fire; the lock owns
 * only the turn lifetime and the bound mechanics.
 *
 * Turn ordering is a FIFO by promise chain. Each run() call synchronously captures the current tail as its predecessor and installs its own settlement promise as
 * the new tail, before any await, so concurrent calls establish a deterministic same-tick order. A waiter that gives up on its turn-wait bound forwards its
 * settlement to its predecessor's own settlement rather than resolving it directly, so a give-up never advances the chain past an operation that is still running.
 */
import type { Clock } from "../utils/index.ts";
import { realClock } from "../utils/index.ts";

// Error classes. The messages of the turn-timeout and deadline errors are part of the compatibility contract: isCaptureInfrastructureError (recovery.ts) classifies
// by substring, so callers rely on the exact text. Each class carries its narrative comment above it.

// Raised when a waiter's turn-wait bound elapses before its predecessor settles. The message is fixed here so both the primitive and its pin test read from one
// place; "queue" remains accurate vocabulary because the lock still maintains a FIFO of waiting turns.
export class CaptureTurnTimeoutError extends Error {

  public constructor() {

    super("Capture queue wait timed out.");
    this.name = "CaptureTurnTimeoutError";
  }
}

// Raised when a task holds its turn past the caller-supplied deadline. The message is supplied per call site (CaptureRunOptions.deadlineMessage) because the stream
// path and the probe path surface different, individually-pinned strings, both of which must keep matching the capture-infrastructure classifier.
export class CaptureDeadlineError extends Error {

  public constructor(message: string) {

    super(message);
    this.name = "CaptureDeadlineError";
  }
}

// Thrown by a task after it retires a resource it produced once the caller had already abandoned the turn (the deadline fired). Rejecting rather than resolving with
// the retired resource guarantees no code path can mistake a torn-down capture stream for a usable one. This rejection never reaches the caller - the caller already
// settled on the deadline error - so it is a purely internal settlement signal that releases the turn.
export class CaptureAbandonedError extends Error {

  public constructor() {

    super("Capture stream retired after the caller abandoned its turn.");
    this.name = "CaptureAbandonedError";
  }
}

// Types.

/**
 * Construction options for the capture lock. The clock is injected so tests drive time deterministically; the wedge-derivation policy is fixed for the lock's
 * lifetime because it encodes an operational safety bound rather than a per-task choice.
 */
export interface CaptureLockOptions {

  // The time port used for the turn-wait race, the task deadline race, and the wedge sleep. Defaults to realClock; tests pass a fake clock.
  readonly clock?: Clock;

  // The wedge-derivation policy: a task's wedge bound is max(wedgeFloorMs, deadlineMs + wedgeMarginMs), computed per run() call. Deriving from the task's own
  // deadline keeps the wedge strictly later than the caller-visible bound on every call, so the two-stage ordering cannot be mis-wired.
  readonly wedgeFloorMs: number;
  readonly wedgeMarginMs: number;
}

/**
 * Per-call options for a single run() invocation. Every timing bound is supplied here rather than captured at construction, because the production lock is built
 * before configuration is available and configuration can change mid-process.
 */
export interface CaptureRunOptions {

  // The message the CaptureDeadlineError carries when the task deadline fires. Supplied per site so each call's timeout keeps its own pinned, classifier-matching text.
  readonly deadlineMessage: string;

  // The maximum time the task itself may run once it holds the turn, before the caller is rejected with a CaptureDeadlineError. The turn is not released on this
  // deadline; it releases only when the task's own promise settles.
  readonly deadlineMs: number;

  // Fired once if the task holds the turn past the derived wedge bound without settling. The lock keeps holding - it never releases early; the call site decides what
  // recovery, if any, may fire. A task with no recovery action omits it.
  readonly onWedge?: () => void;

  // The maximum time this call may wait for its predecessor to settle before giving up its turn with a CaptureTurnTimeoutError. A give-up never advances the chain
  // past an unsettled predecessor.
  readonly turnWaitMs: number;
}

/**
 * The capture lock's public surface: a single run() method that serializes the supplied task against every other task on this lock.
 */
export interface CaptureLock {

  // Runs the task while holding an exclusive turn. The task receives an AbortSignal that is aborted if the caller deadline fires, letting a task that produced a
  // resource after abandonment retire it. Resolves with the task's value, or rejects with CaptureTurnTimeoutError (turn-wait elapsed), CaptureDeadlineError (task
  // deadline elapsed), or the task's own rejection.
  readonly run: <T>(task: (signal: AbortSignal) => Promise<T>, options: CaptureRunOptions) => Promise<T>;
}

// Factory.

/**
 * Creates a capture lock. The returned lock serializes every task passed to run(): no task begins while another runs, and the turn releases only when the running
 * task's promise settles. A wedged task (one that holds its turn past the derived wedge bound without settling) invokes its onWedge callback once, so the call site
 * can route it into recovery, but the turn still releases only on true settlement.
 * @param options - The injected clock and the wedge-derivation policy.
 * @returns A capture lock.
 */
export function createCaptureLock(options: CaptureLockOptions): CaptureLock {

  const { clock = realClock, wedgeFloorMs, wedgeMarginMs } = options;

  // The FIFO tail. Each run() call reads this as its predecessor and replaces it with its own settlement promise, synchronously, before any await. The first call
  // sees an already-resolved predecessor and is granted its turn immediately.
  let tail: Promise<void> = Promise.resolve();

  const run = async <T>(task: (signal: AbortSignal) => Promise<T>, runOptions: CaptureRunOptions): Promise<T> => {

    // Derive the wedge bound from this call's own deadline so it is always strictly later than the caller-visible deadline. No separate deadline-versus-wedge
    // validation is possible to mis-wire because the derivation guarantees the ordering.
    const wedgeBoundMs = Math.max(wedgeFloorMs, runOptions.deadlineMs + wedgeMarginMs);

    // Link this node into the FIFO synchronously, before any await, so concurrent run() calls establish a deterministic order: our predecessor is whatever tail was,
    // and our own settlement becomes the new tail for the next caller to wait on.
    const predecessor = tail;

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: settled, resolve: signalSettled } = Promise.withResolvers<void>();

    tail = settled;

    // Turn-wait phase: wait for the predecessor to settle, bounded by the caller's turn-wait. On timeout, forward our settlement to the predecessor's own settlement
    // so a later caller still cannot begin until the running operation actually settles, then reject the waiter. Forwarding (rather than resolving our node directly)
    // is what keeps a give-up from advancing the chain past an unsettled predecessor.
    try {

      await clock.waitWithTimeout(predecessor, runOptions.turnWaitMs, new CaptureTurnTimeoutError());
    } catch(error) {

      void predecessor.then(signalSettled, signalSettled);

      throw error;
    }

    // Turn granted. From here the turn is held until the task's promise settles, whatever the caller does.
    const controller = new AbortController();
    const work = task(controller.signal);

    // Track settlement independently of the caller-facing deadline wait below. This single subscription both releases the turn when the task truly settles and
    // deliberately consumes a late rejection, so an abandoned task (whose caller already moved on) never surfaces an unhandled rejection. Attaching both callbacks is
    // required: the bounded wait in the deadline path already observes the work's rejection, and a fulfillment-only handler here would create unhandled-rejection noise.
    let workSettled = false;

    const markSettled = (): void => {

      workSettled = true;
      signalSettled();
    };

    void work.then(markSettled, markSettled);

    // Arm the wedge as a flag-guarded sleep rather than a cancellable timer: if the task has not settled by the derived bound, invoke onWedge once. A late wakeup
    // after the task already settled is a cheap no-op, so no timer handle or cancellation bookkeeping is needed. The wedge only signals; it never releases the turn.
    void clock.sleep(wedgeBoundMs).then(() => {

      if(!workSettled) {

        runOptions.onWedge?.();
      }
    });

    // Deadline phase: the caller waits up to its task deadline for the work to settle. If the deadline wins, abort the signal - letting a task that later produces a
    // resource retire it - and reject the caller. The turn is NOT released here; the settlement subscription above keeps holding it until the work truly settles.
    try {

      return await clock.waitWithTimeout(work, runOptions.deadlineMs, new CaptureDeadlineError(runOptions.deadlineMessage));
    } catch(error) {

      if(error instanceof CaptureDeadlineError) {

        controller.abort();
      }

      throw error;
    }
  };

  return { run };
}
