/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * retry.ts: Retry logic with exponential backoff for PrismCast.
 */
import { formatError, isSessionClosedError } from "./errors.js";
import { CONFIG } from "../config/index.js";
import { LOG } from "./logger.js";

/* The retry system provides resilient operation execution with exponential backoff and jitter. When operations fail due to transient issues like network hiccups or
 * slow page loads, the system automatically retries with increasing delays. The exponential backoff prevents overwhelming struggling services, while jitter prevents
 * multiple clients from synchronizing their retry attempts.
 */

/**
 * Implements a generic retry mechanism with exponential backoff and jitter. This function attempts an operation multiple times, waiting progressively longer between
 * attempts to avoid overwhelming failing services. The exponential backoff with jitter prevents thundering herd problems where many clients retry simultaneously.
 * @param operation - An async function to attempt. Should throw on failure.
 * @param maxAttempts - Maximum number of attempts before giving up.
 * @param timeoutMs - Timeout in milliseconds for each individual attempt.
 * @param description - Human-readable description for logging purposes.
 * @param earlySuccessCheck - Optional async function called after timeout errors. If it returns a truthy value, that value is returned as success instead of
 *                            retrying. Useful for cases where the operation succeeded but took too long.
 * @param shouldAbort - Optional function called before each attempt. If it returns true, retries are aborted immediately. Useful for checking if the page was closed
 *                      during the backoff delay.
 * @returns The result of the operation if successful.
 * @throws The last error encountered if all attempts fail.
 */
export async function retryOperation<T>(
  operation: () => Promise<T>,
  maxAttempts: number,
  timeoutMs: number,
  description: string,
  earlySuccessCheck?: () => Promise<boolean>,
  shouldAbort?: () => boolean
): Promise<T | void> {

  let lastError: unknown = null;

  for(let attempt = 1; attempt <= maxAttempts; attempt++) {

    // Check if we should abort before starting this attempt. This catches cases where the page was closed during the backoff delay between retries.
    if(shouldAbort?.()) {

      throw new Error("Operation aborted: abort condition met before retry.");
    }

    if(attempt > 1) {

      LOG.info("Retrying %s (attempt %s of %s).", description, attempt, maxAttempts);
    }

    try {

      // Race the operation against a timeout to prevent hanging on operations that never complete.
      const timeoutPromise = new Promise<never>((_, reject) => {

        setTimeout(() => {

          reject(new Error([ "Operation timed out after ", String(timeoutMs), "ms." ].join("")));
        }, timeoutMs);
      });

      // eslint-disable-next-line no-await-in-loop
      return await Promise.race([ operation(), timeoutPromise ]);
    } catch(error) {

      lastError = error;

      // If the page or session was closed, retrying is pointless. Abort immediately without warning since we're not going to retry.
      if(isSessionClosedError(error)) {

        LOG.info("Page was closed, aborting retries for %s.", description);

        throw error;
      }

      // For timeout errors, check if the operation actually succeeded despite the timeout. This handles cases where the page loaded and video started playing, but
      // some wait condition like networkidle2 never completed. We check this before logging a warning because if early success passes, there's nothing to warn about.
      if(earlySuccessCheck && (formatError(error).indexOf("timed out") !== -1)) {

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
      LOG.warn("Attempt %s failed for %s: %s", attempt, description, formatError(error));

      // Between retry attempts, wait with exponential backoff plus random jitter.
      if(attempt < maxAttempts) {

        const baseDelay = Math.min(1000 * Math.pow(2, attempt - 1), CONFIG.recovery.maxBackoffDelay);
        const jitter = Math.random() * CONFIG.recovery.backoffJitter;

        // eslint-disable-next-line no-await-in-loop
        await new Promise<void>((resolve) => {

          setTimeout(resolve, baseDelay + jitter);
        });
      }
    }
  }

  throw lastError;
}
