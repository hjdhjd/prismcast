/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clock.ts: The Clock port - the project's abstraction for time-dependent operations. Production code that needs to sleep, race a promise against a timeout, or
 * read the high-resolution clock should consume a Clock when those operations live inside an async chain that tests need to control deterministically. The
 * default realClock delegates to delay() and raceWithTimeout() in delay.ts and to performance.now() in the runtime; tests pass a fake-clock literal that resolves
 * sleeps instantly and chooses raceWithTimeout outcomes explicitly.
 *
 * Why this exists. Node's builtin node:test mock.timers exposes only synchronous tick() and runAll() - it has never shipped a tickAsync/runAllAsync variant on
 * any version. Synchronous tick advances the fake clock but does not drain microtask chains across nested Promise.race / .finally / await delay() patterns; the
 * promises stay pending past the tick. retryOperation is the canonical case: its internal raceWithTimeout -> Promise.race -> .finally -> await delay() chain is
 * exactly the shape mock.timers cannot deterministically resolve. Injecting a Clock sidesteps the runtime gap entirely - tests provide a sleep() that resolves
 * immediately and a raceWithTimeout() that forwards or rejects on demand.
 *
 * When to use a Clock vs. a direct delay()/raceWithTimeout() import. Use the direct imports when the production code's tests can fake time with mock.timers and
 * the call sites are shallow enough for synchronous tick() to drain. Reach for Clock injection only when nested async chains break that pattern, or when the
 * test surface needs to assert on the *schedule* (number and durations of sleeps) rather than just the eventual outcome.
 */
import { delay, raceWithTimeout } from "./delay.ts";

/**
 * The time-dependent capability set: sleep, race-with-timeout, and read-the-clock. Decision logic that consumes a Clock is a pure function of this shape -
 * production wires it from real I/O via realClock, tests pass a fake clock literal.
 */
export interface Clock {

  // Returns the current high-resolution timestamp in milliseconds. Wraps performance.now() in production; fakes return a deterministic value (often 0, or a
  // controlled sequence). Consumed today by timing.ts's startTimer, which reads now() at creation and on each elapsed-time call; retry.ts consumes only sleep()
  // and raceWithTimeout().
  readonly now: () => number;

  // Races a promise against a timeout. Identical contract to raceWithTimeout in delay.ts: resolves with the promise's value on success, throws timeoutError (or
  // a default Error) on timeout, cleans up the timer in either case. Fake clocks typically forward the promise unchanged, or throw the timeout error explicitly
  // when the test wants to exercise the timeout path.
  readonly raceWithTimeout: <T>(promise: Promise<T>, timeoutMs: number, timeoutError?: Error) => Promise<T>;

  // Resolves after the specified delay. Identical contract to delay() in delay.ts. Fake clocks typically resolve immediately and record the requested duration
  // so tests can assert on the backoff schedule.
  readonly sleep: (ms: number) => Promise<void>;
}

/**
 * The default Clock implementation. Delegates to performance.now() for current time, to raceWithTimeout() in delay.ts for promise-vs-timeout races, and to
 * delay() in delay.ts for sleeps. Production callers consume this via the default-arg pattern; tests bypass it by passing a fake-clock literal.
 */
export const realClock: Clock = {

  now: () => performance.now(),
  raceWithTimeout,
  sleep: delay
};
