/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * retry.ts: Retry logic with exponential backoff for PrismCast. Time-dependent operations route through a Clock (see clock.ts) so tests can deterministically
 * control sleeps and timeout races without depending on real-time delays - the function's nested raceWithTimeout/Promise.race/finally/await delay chain is
 * exactly the shape Node's synchronous mock.timers.tick cannot drain.
 */
import { formatError, isSessionClosedError } from "./errors.ts";
import type { Clock } from "./clock.ts";
import { LOG } from "./logger.ts";
import { realClock } from "./clock.ts";

/* The retry system provides resilient operation execution with exponential backoff and jitter. When operations fail due to transient issues like network hiccups or
 * slow page loads, the system automatically retries with increasing delays. The exponential backoff prevents overwhelming struggling services, while jitter prevents
 * multiple clients from synchronizing their retry attempts.
 */

/**
 * Options for retryOperation. Groups all parameters into a single object to avoid positional parameter sprawl and make the function extensible.
 */
export interface RetryOptions<T> {

  // Maximum jitter added to the backoff delay in milliseconds. Prevents synchronized retries across concurrent operations. Default: 1000ms.
  backoffJitter?: number;

  // The clock used for sleeps between attempts and for the per-attempt timeout race. Defaults to realClock which delegates to delay()/raceWithTimeout() from
  // delay.ts. Tests inject a fake clock so backoff sleeps resolve instantly and timeout races have deterministic outcomes - the production code path is
  // unchanged.
  clock?: Clock;

  // Human-readable description for logging purposes.
  description: string;

  // Optional async function called after timeout errors. If it returns a truthy value, the operation is considered successful and retrying stops. Useful for
  // cases where the operation succeeded but took too long (e.g., page loaded and video started playing, but networkidle2 never completed).
  earlySuccessCheck?: () => Promise<boolean>;

  // Maximum number of attempts before giving up.
  maxAttempts: number;

  // Maximum backoff delay in milliseconds between retry attempts. Caps the exponential growth to prevent excessively long waits. Default: 3000ms.
  maxBackoffDelay?: number;

  // An async function to attempt. Should throw on failure.
  operation: () => Promise<T>;

  // Optional function called before each attempt. If it returns true, retries are aborted immediately. Useful for checking if the page was closed during the
  // backoff delay.
  shouldAbort?: () => boolean;

  // Timeout in milliseconds for each individual attempt.
  timeoutMs: number;
}

/**
 * Implements a generic retry mechanism with exponential backoff and jitter. This function attempts an operation multiple times, waiting progressively longer between
 * attempts to avoid overwhelming failing services. The exponential backoff with jitter prevents thundering herd problems where many clients retry simultaneously.
 * @param options - Retry configuration including the operation, attempt limits, timeouts, and optional backoff tuning.
 * @returns The result of the operation if successful.
 * @throws The last error encountered if all attempts fail.
 */
export async function retryOperation<T>(options: RetryOptions<T>): Promise<T | undefined> {

  const { backoffJitter = 1000, clock = realClock, description, earlySuccessCheck, maxAttempts, maxBackoffDelay = 3000, operation, shouldAbort, timeoutMs } = options;

  let lastError: unknown = null;

  for(let attempt = 1; attempt <= maxAttempts; attempt++) {

    // Check if we should abort before starting this attempt. This catches cases where the page was closed during the backoff delay between retries.
    if(shouldAbort?.()) {

      throw new Error("Operation aborted: abort condition met before retry.");
    }

    if(attempt > 1) {

      LOG.debug("retry", "Retrying %s (attempt %s of %s).", description, attempt, maxAttempts);
    }

    try {

      // eslint-disable-next-line no-await-in-loop
      return await clock.raceWithTimeout(operation(), timeoutMs);
    } catch(error) {

      lastError = error;

      // If the page or session was closed, retrying is pointless. Abort immediately without warning since we're not going to retry.
      if(isSessionClosedError(error)) {

        LOG.debug("retry", "Page was closed, aborting retries for %s.", description);

        throw error;
      }

      // For timeout errors, check if the operation actually succeeded despite the timeout. This handles cases where the page loaded and video started playing, but
      // some wait condition like networkidle2 never completed. We check this before logging a warning because if early success passes, there's nothing to warn about.
      if(earlySuccessCheck && formatError(error).includes("timed out")) {

        try {

          // eslint-disable-next-line no-await-in-loop
          const successResult = await earlySuccessCheck();

          if(successResult) {

            return;
          }
        } catch(_checkError) {

          // Early success check failed, continue with retry logic.
        }
      }

      // If we reach here, we're going to retry (or fail after max attempts). Now log the warning since there's an actual issue to report.
      LOG.warn("Attempt %s failed for %s: %s.", attempt, description, formatError(error));

      // Between retry attempts, wait with exponential backoff plus random jitter.
      if(attempt < maxAttempts) {

        const baseDelay = Math.min(1000 * (2 ** (attempt - 1)), maxBackoffDelay);
        const jitter = Math.random() * backoffJitter;

        // eslint-disable-next-line no-await-in-loop
        await clock.sleep(baseDelay + jitter);
      }
    }
  }

  throw lastError;
}
