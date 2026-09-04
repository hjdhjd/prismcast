/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * errors.ts: Error formatting and handling utilities for PrismCast.
 */

/* These helpers group the error-handling primitives shared across the application: one normalizes arbitrary errors into log-ready text, and two classify
 * browser-state failures - one so a retry loop aborts as soon as the session it was retrying against is gone, the other so a caller can tell a page that
 * died apart from a subject that was never there.
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
 *
 * Matching ignores case. The same dead state reaches callers with different capitalization depending on which Puppeteer or CDP surface raised it, and a
 * differently-capitalized closed target is the same closed target.
 * @param error - The error to check.
 * @returns True if the error indicates a closed or unrecoverable state.
 */
export function isSessionClosedError(error: unknown): boolean {

  const message = formatError(error).toLowerCase();

  const unrecoverablePatterns = [ "target closed", "session closed", "detached frame" ];

  return unrecoverablePatterns.some((pattern) => message.includes(pattern));
}

/**
 * Checks whether an error means the DOM world an operation ran in ceased to exist. Two failure families union here: the page, target, or session is gone (what
 * isSessionClosedError already covers), and the execution context or frame was torn down by a navigation or a teardown while the page itself may live on.
 *
 * The question this answers is about the world, not the work. An operation that failed this way reached no verdict on its own subject, so a caller deciding
 * whether to discard cached state about that subject learns nothing from it and must keep the state.
 *
 * The context and frame wordings are matched as exact phrases rather than single words. A lone "destroyed" or "detached" turns up in errors that have nothing to
 * do with page death - a DOM node detached from its document, an object destroyed by an unrelated teardown - and matching those would route ordinary failures
 * into page-death handling. The phrases are the CDP error family; each one does not reach every call surface, since the evaluate wrapper rewrites the
 * cannot-find-context wording into the context-destroyed one, but other Puppeteer surfaces still emit it directly.
 *
 * Composing isSessionClosedError formats the message twice on a call that matches neither family. That trade is deliberate: the session-closed patterns stay
 * stated in exactly one place, and this runs only on an error path.
 * @param error - The error to check.
 * @returns True if the error indicates the page, execution context, or frame the operation ran in is gone.
 */
export function isPageDeathError(error: unknown): boolean {

  if(isSessionClosedError(error)) {

    return true;
  }

  const message = formatError(error).toLowerCase();

  const contextDeathPatterns = [ "cannot find context with specified id", "execution context is not available", "execution context was destroyed",
    "frame got detached", "frame was detached" ];

  return contextDeathPatterns.some((pattern) => message.includes(pattern));
}

