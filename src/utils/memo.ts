/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * memo.ts: Async memoization primitives. The single primitive here is memoizeAsync - a one-shot async cache that runs its probe at most once across the
 * lifetime of the returned function and shares the in-flight promise across concurrent first-callers. Used to lazily compute values that are expensive on
 * first call and cheap on subsequent calls (e.g., resolveFFmpegPath probing the filesystem).
 */

/* The internal state of a memoizeAsync closure as a discriminated union:
 *
 * - "unset": probe has never run. The next call kicks it off.
 * - "pending": probe is in flight (or has settled to a rejection). All concurrent callers await the same promise. The state stays "pending" until probe
 *   succeeds; on rejection the closure stays "pending" forever (sticky rejection).
 * - "resolved": probe completed successfully and the value is cached. All future calls take the fast path returning the cached value directly.
 *
 * Modeling state as a discriminated union (rather than parallel boolean + value variables) compiles the rule that the value is only readable when resolved
 * into the type system: every read site must switch on the kind first, and TypeScript narrows .value/.promise to the appropriate branch. A future refactor that
 * tries to read the cached value outside the resolved fast path is a type error.
 */
type MemoState<T> = { kind: "unset" } | { kind: "pending"; promise: Promise<T> } | { kind: "resolved"; value: T };

/**
 * Wraps an async probe in a one-shot memoization closure. The returned function:
 *
 * - On first call: invokes probe(), caches the resolved value, returns it.
 * - On concurrent first-calls (multiple callers arriving before the first probe resolves): all callers share the single in-flight promise, so probe is
 *   invoked exactly once regardless of arrival order.
 * - On subsequent calls after first success: returns the cached value directly without invoking probe.
 * - On probe rejection: the rejection is sticky. Every subsequent call returns the same rejected promise without re-probing. This is by design - retrying
 *   a probe that has demonstrated it can fail risks a retry storm; if the caller wants retry semantics, they should compose memoizeAsync with a retry helper
 *   rather than have memoizeAsync silently retry. (Production-cached resolveFFmpegPath has no rejection path - probeFFmpegPath returns undefined for "not
 *   found" rather than throwing - so this branch never fires in the ffmpeg use case.)
 *
 * The cache and pending state are closure-scoped: there is no way to reach into the closure from a test, no reset method, and no way for callers to
 * invalidate the cache. Callers that need invalidation are doing a different thing (TTL cache, refresh-on-event cache) and should not use this primitive.
 *
 * @param probe - The async function to memoize. Invoked at most once on success, or once per process on rejection.
 * @returns A function that returns the same Promise<T> on every call.
 */
export function memoizeAsync<T>(probe: () => Promise<T>): () => Promise<T> {

  let state: MemoState<T> = { kind: "unset" };

  return async (): Promise<T> => {

    switch(state.kind) {

      case "resolved": {

        // Fast path: return the cached value directly. TypeScript narrows state.value to T on this branch.
        return state.value;
      }

      case "pending": {

        // Concurrent caller (or a follow-up caller during a sticky-rejection state). Share the single in-flight promise.
        return state.promise;
      }

      case "unset": {

        /* First call. Kick off the probe and chain a success handler that flips state to "resolved". The chained promise is what callers receive: when probe
         * succeeds, the .then() callback runs, state becomes "resolved", and the chained promise resolves with the value. When probe rejects, the .then()
         * callback never runs (no onRejected handler) and the chained promise rejects, so state stays "pending" carrying the rejected promise - all future
         * callers receive the same rejection.
         */
        const promise = probe().then((value) => {

          state = { kind: "resolved", value };

          return value;
        });

        state = { kind: "pending", promise };

        return promise;
      }
    }
  };
}
