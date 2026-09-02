/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clock.helpers.ts: Test affordances for the Clock port. The realClock default in clock.ts is the production-side adapter; makeFakeClock here is the test-side
 * counterpart - they form a pair, co-located so any test that consumes a Clock has the factory ready without inventing its own. makeAdvancingClock is the same
 * fake with its sleeps wired to its own now(), for the consumers that measure elapsed time as well as schedule waits. By default the fake clock's
 * sleep() resolves immediately and records the requested duration into a shared array (so tests can assert on the schedule); waitWithTimeout() forwards the
 * inner promise unchanged (so the operation's own resolve/throw drives the outcome); now() returns 0. Tests that need different behavior pass overrides for
 * the specific method they want to control - e.g., a waitWithTimeout that throws synchronously to simulate the bound lapsing.
 *
 * Excluded from the build emit by the *.helpers.ts pattern in tsconfig.build.json.
 */
import type { Clock } from "./clock.ts";

/**
 * The handle returned by makeFakeClock - the clock to inject plus the captured sleep schedule. Tests assert on `sleeps.length` to verify backoff was scheduled
 * the expected number of times, and on the values themselves to verify the exponential-with-jitter computation when needed.
 */
export interface FakeClockHandle {

  readonly clock: Clock;

  // The recorded sleep durations (in milliseconds) the consumer asked for. Populated only when sleep() is left at its default - tests that override sleep()
  // own their own recording.
  readonly sleeps: number[];
}

/**
 * Builds a fake Clock plus a shared sleep-recording array. The default behavior is the right answer for almost every test:
 *
 *   - sleep(ms) resolves immediately and pushes ms into the sleeps array
 *   - waitWithTimeout(promise, ...) forwards the inner promise unchanged
 *   - now() returns 0
 *
 * Tests that need a different shape pass an override for that method only. Common overrides:
 *
 *   - `waitWithTimeout: async (_p, ms, err) => { throw err ?? new Error("timed out after " + String(ms) + "ms."); }` - simulates the bound lapsing first
 *   - `now: () => 12345` - locks a deterministic timestamp for elapsed-time math
 *
 * @param overrides - Partial Clock to override the defaults for specific methods. Defaults to {}.
 * @returns A handle exposing the fake clock and the captured sleep durations.
 */
export function makeFakeClock(overrides: Partial<Clock> = {}): FakeClockHandle {

  const sleeps: number[] = [];

  const clock: Clock = {

    now: overrides.now ?? ((): number => 0),
    sleep: overrides.sleep ?? (async (ms: number): Promise<void> => {

      sleeps.push(ms);

      // Yield once to the microtask queue so the fake matches the async semantics of a real delay() (a real setTimeout(0) yields too); without this, awaiters
      // run synchronously after sleep, which is structurally distinct from the production code path tests are exercising.
      await Promise.resolve();
    }),
    waitWithTimeout: overrides.waitWithTimeout ?? (async <T>(promise: Promise<T>): Promise<T> => promise)
  };

  return { clock, sleeps };
}

/**
 * Builds a fake Clock whose sleeps advance its own now(), plus the shared sleep-recording array. The default fake freezes now() at 0, which is the right answer
 * for a consumer that only schedules waits; a consumer that also MEASURES elapsed time - a poll deciding whether its ceiling has passed, an operation timing
 * itself - needs the two to move together, and a test that hand-rolls that pairing is how two files end up disagreeing about what a fake sleep does.
 *
 * now() and sleep() are this factory's whole point, so neither is overridable here. A test wanting different behavior for either wants makeFakeClock.
 * @returns A handle exposing the advancing fake clock and the captured sleep durations.
 */
export function makeAdvancingClock(): FakeClockHandle {

  const sleeps: number[] = [];

  let elapsed = 0;

  const { clock } = makeFakeClock({

    now: (): number => elapsed,
    sleep: async (ms: number): Promise<void> => {

      sleeps.push(ms);
      elapsed += ms;

      // Yield to the microtask queue exactly as the default fake's sleep does, so a consumer's awaits behave as they do against a real delay.
      await Promise.resolve();
    }
  });

  return { clock, sleeps };
}
