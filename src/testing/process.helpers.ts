/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * process.helpers.ts: Process-level test helpers. Currently exposes assertNoUnhandledRejections and expectAt. Both bridge between the test body and the
 * surrounding Node process surface - one captures unhandled rejections via the process's emitter, the other yields to the microtask queue while waiting for a
 * predicate to become true.
 */
import type { EventEmitter } from "node:events";

/**
 * Registers a per-test handler that fails the test if any unhandled promise rejection occurs during the test body. Use this for tests of code that fires
 * fire-and-forget promises ("void this.refresh()") - without a guard, a rejection in one of those promises would surface as a Node-level warning rather than a
 * test failure. The returned cleanup function must be invoked from t.after() or afterEach() to remove the handler.
 *
 * The emitter parameter exists for testability - tests pass a fresh EventEmitter so synthetic events do not conflict with the test runner's own
 * unhandledRejection guard. Production callers omit it and the helper attaches to process.
 *
 * @param emitter - The emitter to listen on. Defaults to process. Tests pass a controlled emitter to avoid runner conflicts.
 * @returns A cleanup function that detaches the handler and throws if any rejection was captured.
 */
export function assertNoUnhandledRejections(emitter: EventEmitter = process): () => void {

  const captured: { promise: Promise<unknown>; reason: unknown }[] = [];

  const handler = (reason: unknown, promise: Promise<unknown>): void => {

    captured.push({ promise, reason });
  };

  emitter.on("unhandledRejection", handler);

  return (): void => {

    emitter.off("unhandledRejection", handler);

    if(captured.length > 0) {

      // Construct a single error that carries the first rejection's reason as its cause, so the failure stack points at the rejection's origin and not at the
      // assertion frame. Subsequent rejections are summarized in the message.
      const first = captured[0];
      const summary = "Unhandled rejection during test (" + String(captured.length) + " total): " + String(first?.reason);

      throw new Error(summary, { cause: first?.reason });
    }
  };
}

/**
 * Polls a synchronous predicate, yielding to the microtask queue between attempts, until it returns a non-undefined value or the iteration budget is exhausted.
 * Use this when a test needs to wait for a value that becomes available across promise chains (e.g., async event listeners that update shared state) without
 * introducing real-time delays. The default budget is 100 iterations, which is more than enough for any code path that resolves within a finite microtask
 * chain. Tests that need to wait on real timers should use mock.timers.tickAsync(), not this helper.
 * @param predicate - Function returning the awaited value, or undefined when not yet available.
 * @param options - Optional iteration budget override.
 * @returns The first non-undefined value the predicate returns.
 * @throws If the predicate never returns a value within the iteration budget.
 */
export async function expectAt<T>(predicate: () => T | undefined, options: { iterations?: number } = {}): Promise<T> {

  const max = options.iterations ?? 100;

  for(let i = 0; i < max; i++) {

    const value = predicate();

    if(value !== undefined) {

      return value;
    }

    // We yield to the microtask queue so chained promises can resolve before we re-check. Sequential awaits in a polling loop are intentional - parallelization
    // would race the predicate against itself - so the no-await-in-loop rule does not apply here.
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve();
  }

  throw new Error("expectAt: predicate did not yield a value within " + String(max) + " microtask iterations");
}
