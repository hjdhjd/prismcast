/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * delay.ts: The project's wait policies. homebridge-plugin-utils owns the wait mechanisms - waitWithSignal races a held promise against an interrupt signal,
 * and runWithAbort carries the return-null-on-abort policy - and this file names the policies built over them exactly once: timeoutSignal is the
 * reason-carrying interrupt source, waitWithTimeout is the throw-shaped bounded wait, boundedWait is the value-shaped one, and delay is the named sleep. Any
 * code path that needs "promise with a bound" consumes a policy here rather than re-rolling the timer, race, and cleanup sequence.
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
import type { Nullable } from "../types/index.ts";
import { setTimeout as nodeSleep } from "node:timers/promises";

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
