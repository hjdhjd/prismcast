/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * delay.ts: Async delay and timeout utilities for PrismCast.
 */

/**
 * A cancellable timeout that can be used with Promise.race. The promise resolves to false when the timeout fires, and cancel() clears the timer to prevent it
 * from holding an event loop reference after the race is won by another promise.
 */
export interface CancellableTimeout {

  cancel: () => void;
  promise: Promise<false>;
}

/**
 * Creates a cancellable timeout for use with Promise.race. Returns a promise that resolves to false after the specified delay, and a cancel function that clears
 * the timer. This avoids the split declaration-then-assignment pattern that requires definite assignment assertions when managing timer IDs alongside
 * Promise.race.
 * @param ms - The timeout duration in milliseconds.
 * @returns An object with the timeout promise and a cancel function.
 */
export function cancellableTimeout(ms: number): CancellableTimeout {

  let timer: ReturnType<typeof setTimeout>;

  const promise = new Promise<false>((resolve) => {

    timer = setTimeout(() => { resolve(false); }, ms);
  });

  return { cancel: (): void => { clearTimeout(timer); }, promise };
}

/**
 * Creates a promise that resolves after the specified delay. This is a convenience wrapper around setTimeout that allows using async/await syntax for delays.
 * @param ms - The delay duration in milliseconds.
 * @returns A promise that resolves after the specified delay.
 */
export async function delay(ms: number): Promise<void> {

  return new Promise<void>((resolve) => {

    setTimeout(resolve, ms);
  });
}
