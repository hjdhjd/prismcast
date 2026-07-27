/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * errors.ts: Error formatting and handling utilities for PrismCast.
 */

/* These helpers group the error-handling primitives shared across the application: one normalizes arbitrary errors into log-ready text, the other classifies
 * unrecoverable browser-state errors so retry loops can abort early.
 */

/**
 * Formats an error for logging by extracting the message if available, falling back to string conversion for non-Error objects. Trailing punctuation is stripped
 * to allow callers to add consistent punctuation in their log format strings.
 * @param error - The error to format.
 * @returns A string representation suitable for logging, without trailing punctuation.
 */
export function formatError(error: unknown): string {

  let message: string;

  if(error instanceof Error) {

    message = error.message;
  } else if(error && (typeof (error as { message?: unknown }).message === "string")) {

    message = (error as { message: string }).message;
  } else {

    message = String(error);
  }

  // Strip trailing punctuation to prevent double punctuation when callers add their own.
  return message.replace(/[.!?]+$/, "");
}

/**
 * Checks whether an error indicates that the browser page or session has been closed or is otherwise unrecoverable. These errors should cause immediate abort of
 * any retry loops rather than continuing to retry an operation that will never succeed. Common unrecoverable errors include closed targets, closed sessions, and
 * detached frames (which occur when the page has been closed mid-operation).
 * @param error - The error to check.
 * @returns True if the error indicates a closed or unrecoverable state.
 */
export function isSessionClosedError(error: unknown): boolean {

  const message = formatError(error);

  const unrecoverablePatterns = [ "Target closed", "Session closed", "detached Frame" ];

  return unrecoverablePatterns.some((pattern) => message.includes(pattern));
}

/**
 * Checks whether an error is the stale-capture-mutex signal: Chrome's tabCapture extension rejects a second concurrent getStream() with "Cannot capture a tab with
 * an active stream", and that rejection leaks puppeteer-stream's module-level mutex permanently, so the only recovery is a full process restart. This is the single
 * home for that one literal - every site that must decide the process-exit escalation reads it from here. The predicate is deliberately narrow: session-closed-shaped
 * rejections are common and benign, so widening it would exit the process on ordinary browser crashes. The related, broader capture-infrastructure classifier
 * (isCaptureInfrastructureError in recovery.ts) matches this message too, but the process-exit decision this predicate gates stays its own distinct check.
 * @param error - The error or message to check.
 * @returns True if the error carries the stale-capture-mutex signature.
 */
export function isStaleCaptureMutexError(error: unknown): boolean {

  return formatError(error).includes("Cannot capture a tab with an active stream");
}
