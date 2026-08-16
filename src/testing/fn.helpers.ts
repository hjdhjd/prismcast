/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fn.helpers.ts: Function-shaped test fixtures.
 */

/**
 * No-op function returning undefined. Used wherever a test needs a stub callback to satisfy a contract without doing anything observable. Centralizing this as
 * a named export rather than declaring local bare-arrow `() => {}` literals at each test site avoids the @typescript-eslint/no-empty-function rule and gives
 * the codebase a single, grep-able sentinel for "intentionally does nothing" test stubs.
 */
export function noop(): void {

  return undefined;
}

/**
 * Yields to the microtask queue the given number of times. A test driving async production code under mock timers needs this between advancing the clock and
 * asserting: advancing fires the timer callback, but the settlement it causes still has to travel one promise link per microtask turn - through a wrapper's
 * catch, the caller's catch, a finally - and the clock does not move those. The default is deliberately generous, because an extra turn costs nothing and one
 * turn too few is an assertion that passes or fails on chain depth.
 * @param count - How many microtask turns to yield.
 */
export async function flushMicrotasks(count = 20): Promise<void> {

  for(let turn = 0; turn < count; turn++) {

    // Sequential by definition: each await is one microtask turn, and yielding them concurrently would collapse the whole point into a single turn.
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve();
  }
}
