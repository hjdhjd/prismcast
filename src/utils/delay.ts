/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * delay.ts: Async delay and timeout utilities. The Clock port (clock.ts) consumes raceWithTimeout and delay from this file as its production implementation;
 * any other code path that needs "promise with timeout" or a cancellable timeout should consume these directly rather than re-rolling the timer/promise/race/
 * cleanup sequence.
 *
 * Why these primitives are canonical: they exercise three patterns every project eventually needs - a cancellable timer with explicit cleanup, a single
 * timeout-race-with-cleanup implementation, and a named "sleep" wrapper around node:timers/promises.setTimeout. Centralizing them prevents the bug pattern
 * where timers are leaked because some call site forgot the .finally(clearTimeout).
 */
import { setTimeout as nodeSleep } from "node:timers/promises";

/**
 * A cancellable timeout that can be used with Promise.race. The promise resolves to false when the timeout fires, and cancel() clears the timer to prevent it
 * from holding an event loop reference after the race is won by another promise.
 */
export interface CancellableTimeout {

  cancel: () => void;
  promise: Promise<false>;
}

/**
 * Creates a cancellable timeout for use with Promise.race. Returns a promise that resolves to false after the specified delay, and a cancel function that
 * clears the timer so it does not hold an event loop reference after the race is won by another promise.
 * @param ms - The timeout duration in milliseconds.
 * @returns An object with the timeout promise and a cancel function.
 */
export function cancellableTimeout(ms: number): CancellableTimeout {

  const { promise, resolve } = Promise.withResolvers<false>();
  const timer = setTimeout(() => { resolve(false); }, ms);

  return { cancel: (): void => { clearTimeout(timer); }, promise };
}

/**
 * Races a promise against a timeout. If the promise resolves before the timeout, its value is returned. If the timeout fires first, the provided error is
 * thrown (or a default Error if none is provided). The timer is always cleaned up via .finally() to prevent orphaned event loop references.
 *
 * This is the single implementation of the timeout-race-with-cleanup pattern. All code paths that need "promise with timeout" should use this rather than
 * manually constructing timer/promise/race/cleanup sequences.
 * @param promise - The promise to race against the timeout.
 * @param timeoutMs - The timeout duration in milliseconds.
 * @param timeoutError - Optional error to throw on timeout. Defaults to a generic Error with the timeout duration.
 * @returns The resolved value of the promise.
 */
export async function raceWithTimeout<T>(promise: Promise<T>, timeoutMs: number, timeoutError?: Error): Promise<T> {

  const { promise: timeoutPromise, reject: signalTimeout } = Promise.withResolvers<never>();
  const timer = setTimeout(() => {

    signalTimeout(timeoutError ?? new Error("Operation timed out after " + String(timeoutMs) + "ms."));
  }, timeoutMs);

  return Promise.race([ promise, timeoutPromise ]).finally(() => { clearTimeout(timer); });
}

/**
 * Creates a promise that resolves after the specified delay. Wraps node:timers/promises.setTimeout so the codebase has a single canonical "sleep" name.
 * @param ms - The delay duration in milliseconds.
 * @returns A promise that resolves after the specified delay.
 */
export async function delay(ms: number): Promise<void> {

  await nodeSleep(ms);
}
