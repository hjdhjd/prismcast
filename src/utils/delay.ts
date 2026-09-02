/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * delay.ts: The project's wait policies. homebridge-plugin-utils owns the wait mechanisms - waitWithSignal races a held promise against an interrupt signal,
 * and runWithAbort carries the return-null-on-abort policy - and this file names the policies built over them exactly once: timeoutSignal is the
 * reason-carrying interrupt source, waitWithTimeout is the throw-shaped bounded wait, boundedWait is the value-shaped one, delay is the named sleep, and
 * pollUntil is the read-a-reported-state shape. Any code path that needs "promise with a bound" consumes a policy here rather than re-rolling the timer, race,
 * and cleanup sequence.
 *
 * pollUntil sits beside the two bounded waits rather than among them because it binds a different thing. The bounded waits hold a promise somebody else
 * produced and decide what a lapse means; pollUntil has no promise to hold - the state it is waiting on is one the caller can only ask for - so it asks on a
 * cadence and reports what it last saw. That is why its ceiling is a bound to log rather than a delay the healthy path pays: a signal that is already true
 * costs exactly one read.
 *
 * Choosing between the two bounded waits is a question about semantics, not mechanism. A wait whose interruption is exceptional - the operation was supposed
 * to finish and did not - throws through waitWithTimeout, so the failure travels as an error the caller can distinguish by identity or type. A wait whose
 * lapse is an ordinary branch the caller was already going to take returns null through boundedWait, so the caller reads it as a value rather than unwinding
 * through a catch it did not need.
 *
 * Each policy owns its timer's entire lifecycle: it creates the handle, races through the library, and cancels in a finally. Centralizing that ownership is
 * what keeps a forgotten cleanup from leaking a timer at any call site, and it is what guarantees the handle is disposed at settlement while the event loop is
 * still healthy (see the rationale on timeoutSignal).
 */
import { runWithAbort, waitWithSignal } from "homebridge-plugin-utils";
import type { Clock } from "./clock.ts";
import type { Nullable } from "../types/index.ts";
import { setTimeout as nodeSleep } from "node:timers/promises";
import { realClock } from "./clock.ts";

/**
 * A timeout expressed as an abort signal, paired with the disposal its consumer owes. The signal aborts with the caller's own error object as its reason, which
 * is what the platform's AbortSignal.timeout() cannot express - it always aborts with a generic TimeoutError, forcing every consumer that needs to tell its own
 * lapse apart from anyone else's to translate at the catch site. Carrying the reason instead means a caller can compare the rejection by reference or by type
 * and get an exact answer.
 */
export interface TimeoutSignal {

  cancel: () => void;
  signal: AbortSignal;
}

/**
 * Creates a timeout signal that aborts after the given duration, carrying the supplied error as its abort reason.
 *
 * The consumer owns the returned handle and must cancel() it once its wait settles. That discipline is what keeps a timer from being disposed at process exit
 * instead of at settlement: on Windows, libuv disposing a still-pending timeout handle during natural exit can race pending socket cleanup and trip the
 * UV_HANDLE_CLOSING assertion in libuv's async.c. Cancelling at settlement disposes the handle while the event loop is still healthy, which sidesteps the race
 * regardless of how soon afterwards the process exits. The timer is also unref'd, so during its bounded pre-settlement window it never by itself holds open an
 * otherwise-empty loop.
 *
 * The default error is built inside the timer callback so a wait that finishes in time allocates nothing.
 * @param ms - The timeout duration in milliseconds.
 * @param reason - Optional error to abort with. Defaults to a generic Error naming the duration.
 * @returns A handle exposing the timeout's signal and the cancel function that disposes its timer.
 */
export function timeoutSignal(ms: number, reason?: Error): TimeoutSignal {

  const controller = new AbortController();
  const timer = setTimeout(() => { controller.abort(reason ?? new Error("Operation timed out after " + String(ms) + "ms.")); }, ms);

  timer.unref();

  return { cancel: (): void => { clearTimeout(timer); }, signal: controller.signal };
}

/**
 * Waits for a promise, bounded by a timeout that throws. If the promise settles first its outcome passes through unchanged; if the bound lapses first, the
 * caller's exact error object is thrown, so a call site can identify its own lapse by reference or by instanceof rather than by parsing a message.
 *
 * This is the throw-shaped wait policy: reach for it when a lapse means the operation failed. The timer's full lifecycle lives here - created before the race
 * and cancelled in the finally on every exit path - so no call site can forget the cleanup.
 * @param promise - The promise to wait on.
 * @param timeoutMs - The timeout duration in milliseconds.
 * @param timeoutError - Optional error to throw on timeout. Defaults to a generic Error naming the duration.
 * @returns The resolved value of the promise.
 * @throws The supplied error (or the default) when the bound lapses first, or the promise's own rejection when it settles first.
 */
export async function waitWithTimeout<T>(promise: Promise<T>, timeoutMs: number, timeoutError?: Error): Promise<T> {

  const timeout = timeoutSignal(timeoutMs, timeoutError);

  try {

    return await waitWithSignal(promise, timeout.signal);
  } finally {

    timeout.cancel();
  }
}

/**
 * Waits for a promise, bounded by a timeout that yields null. If the promise settles first its value is returned; if the bound lapses first the result is null,
 * so the caller branches on a value rather than unwinding through a catch.
 *
 * This is the value-shaped wait policy: reach for it when a lapse is an ordinary outcome the caller already handles - a shutdown escalating from SIGTERM to
 * SIGKILL, a tune that did not land, an interception that never arrived. A promise's own rejection still propagates, because a rejection is a failure rather
 * than a lapse.
 *
 * The composition is deliberate. runWithAbort performs no race of its own; it composes the bounds into one signal, hands that signal to the factory, and maps
 * an abort-time rejection to null. So the factory forwards the signal into waitWithSignal, and that forwarding is what makes the bound genuinely bind on a
 * promise this code does not own - a factory that ignored its signal would leave a held promise waiting forever. The bound rides timeoutSignal rather than
 * runWithAbort's own timeout option so every bound in the project flows through the one source that can carry a reason, owns its timer, and is controllable
 * from a fake clock.
 *
 * One constraint belongs to the caller: a promise that can itself resolve null cannot be told apart from a lapse. Callers own that fit.
 * @param promise - The promise to wait on.
 * @param timeoutMs - The timeout duration in milliseconds.
 * @returns The resolved value of the promise, or null if the bound lapsed first.
 */
export async function boundedWait<T>(promise: Promise<T>, timeoutMs: number): Promise<Nullable<T>> {

  const timeout = timeoutSignal(timeoutMs);

  try {

    return await runWithAbort((signal) => waitWithSignal(promise, signal), { signal: timeout.signal });
  } finally {

    timeout.cancel();
  }
}

/**
 * Creates a promise that resolves after the specified delay. Wraps node:timers/promises.setTimeout so the codebase has a single canonical "sleep" name.
 * @param ms - The delay duration in milliseconds.
 * @returns A promise that resolves after the specified delay.
 */
export async function delay(ms: number): Promise<void> {

  await nodeSleep(ms);
}

/**
 * The inputs to one poll: what to read, when to stop reading, how often to ask, and how long to keep asking.
 */
export interface PollUntilOptions<T> {

  // The wait between one read and the next. A cadence, not a settle: nothing is being given time to happen, the state is simply being asked for again.
  readonly cadenceMs: number;

  // The longest the poll keeps asking before it gives up and reports what it last saw. A ceiling of 0 performs exactly one read.
  readonly ceilingMs: number;

  // The time port driving the cadence and the elapsed measurement. Defaults to realClock; tests pass a fake.
  readonly clock?: Clock;

  // Reads the signal once. A rejection propagates to the caller unchanged, because what a failed read means belongs to the caller, not to the poll.
  readonly read: () => Promise<T>;

  // Decides whether a value read is the one the caller was waiting for.
  readonly until: (value: T) => boolean;
}

/**
 * The result of one poll: whether the ceiling lapsed, how many reads it took, and the last value read.
 */
export interface PollOutcome<T> {

  // True when the ceiling elapsed with no read satisfying the predicate. The value below is then the last state observed, not a satisfying one.
  readonly lapsed: boolean;

  // How many times read() was called. A satisfied-on-arrival signal reads once.
  readonly reads: number;

  // The value the last read produced, satisfying or not.
  readonly value: T;
}

/**
 * Reads a signal on a cadence until it satisfies a predicate or a ceiling lapses. This is the shape every "wait for a reported state" call in the project
 * shares: a state that only its owner can report, asked for on a cadence, under a ceiling that exists to be logged rather than to be waited out.
 *
 * The first read runs immediately, with no sleep ahead of it, so a signal that is already true costs one round trip and nothing else - which is what makes this
 * a strictly better answer than a fixed delay even on the paths where the state is usually settled. A read that rejects propagates unchanged: the poll has no
 * opinion on what a failed read means, and swallowing it would hide a fault behind a lapse.
 * @param options - The poll's read, predicate, cadence, ceiling, and clock.
 * @returns The outcome: whether the ceiling lapsed, the number of reads, and the last value read.
 */
export async function pollUntil<T>(options: PollUntilOptions<T>): Promise<PollOutcome<T>> {

  const { cadenceMs, ceilingMs, clock = realClock, read, until } = options;
  const startedAt = clock.now();

  let reads = 0;

  for(;;) {

    // eslint-disable-next-line no-await-in-loop -- A poll is sequential by definition: each read has to settle before the cadence sleep and the next read.
    const value = await read();

    reads++;

    if(until(value)) {

      return { lapsed: false, reads, value };
    }

    // The ceiling is checked only after a read has already happened, so the poll always reports a value and a zero ceiling still asks once.
    if((clock.now() - startedAt) >= ceilingMs) {

      return { lapsed: true, reads, value };
    }

    // eslint-disable-next-line no-await-in-loop -- The cadence is the point: the next read must not start until this wait completes.
    await clock.sleep(cadenceMs);
  }
}
