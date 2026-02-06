/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * evaluate.ts: Puppeteer evaluate wrapper with abort and timeout support.
 */
import type { Frame, Page } from "puppeteer-core";
import { getStreamId } from "./streamContext.js";

/* This module provides a wrapper around Puppeteer's page.evaluate() and frame.evaluate() that adds two critical safety mechanisms:
 *
 * 1. Abort signal: When a stream is terminated, its AbortController is triggered, immediately rejecting all pending evaluate calls for that stream. This prevents zombie
 *    CDP calls from hanging for 180 seconds (Puppeteer's default protocolTimeout) when the browser becomes unresponsive.
 *
 * 2. Timeout: A configurable timeout (default 15 seconds) provides a safety net for evaluate calls that hang. This catches cases where the browser is unresponsive but
 *    the stream hasn't been explicitly terminated yet.
 *
 * The wrapper automatically retrieves the abort signal from the stream registry using the stream context from AsyncLocalStorage. If no stream context is available
 * (e.g., during browser initialization), it falls back to timeout-only behavior.
 *
 * IMPORTANT: When aborting or timing out, the underlying CDP call is still pending in Puppeteer - we just stop waiting for it locally. We attach a no-op .catch() to
 * the evaluate promise to suppress unhandled rejection warnings when the CDP call eventually completes or times out.
 */

// Default timeout for evaluate calls in milliseconds.
const DEFAULT_EVALUATE_TIMEOUT = 15000;

// Map of stream ID strings to their AbortControllers. Uses string IDs (e.g., "cnn-5jecl6") since that's what the stream context provides via AsyncLocalStorage.
const abortControllers = new Map<string, AbortController>();

/**
 * Registers an AbortController for a stream. Called when a stream is created.
 * @param streamIdStr - The stream ID string (e.g., "cnn-5jecl6").
 * @param controller - The AbortController for this stream.
 */
export function registerAbortController(streamIdStr: string, controller: AbortController): void {

  abortControllers.set(streamIdStr, controller);
}

/**
 * Unregisters an AbortController for a stream. Called when a stream is terminated.
 * @param streamIdStr - The stream ID string.
 */
export function unregisterAbortController(streamIdStr: string): void {

  abortControllers.delete(streamIdStr);
}

/**
 * Gets the AbortSignal for a stream, if one exists.
 * @param streamIdStr - The stream ID string.
 * @returns The AbortSignal if found, undefined otherwise.
 */
export function getAbortSignal(streamIdStr: string): AbortSignal | undefined {

  return abortControllers.get(streamIdStr)?.signal;
}

/**
 * Gets the AbortController for a stream, if one exists.
 * @param streamIdStr - The stream ID string.
 * @returns The AbortController if found, undefined otherwise.
 */
export function getAbortController(streamIdStr: string): AbortController | undefined {

  return abortControllers.get(streamIdStr);
}

/**
 * Custom error class for timeout errors. This allows callers to distinguish timeout errors from other errors.
 */
export class EvaluateTimeoutError extends Error {

  constructor(timeoutMs: number) {

    super("Evaluate timed out after " + String(timeoutMs) + "ms.");

    this.name = "EvaluateTimeoutError";
  }
}

/**
 * Custom error class for abort errors. This allows callers to distinguish abort errors from other errors.
 */
export class EvaluateAbortError extends Error {

  constructor() {

    super("Evaluate aborted due to stream termination.");

    this.name = "EvaluateAbortError";
  }
}

/**
 * Executes a Puppeteer evaluate call with abort and timeout support. This wrapper provides immediate cancellation when a stream is terminated and a safety timeout to
 * prevent hanging on unresponsive browsers.
 *
 * If running within a stream context (via AsyncLocalStorage), the abort signal for that stream is used. If no stream context is available, only the timeout is applied.
 * @param context - The Page or Frame to evaluate in.
 * @param pageFunction - The function to evaluate in the browser context.
 * @param args - Arguments to pass to the function (optional).
 * @param timeoutMs - Timeout in milliseconds (default: 15000).
 * @returns The result of the evaluate call.
 * @throws EvaluateAbortError if the stream was terminated.
 * @throws EvaluateTimeoutError if the timeout was reached.
 * @throws Any error from the underlying evaluate call.
 */
export async function evaluateWithAbort<T, Args extends unknown[]>(
  context: Frame | Page,
  pageFunction: (...args: Args) => T,
  args?: Args,
  timeoutMs?: number
): Promise<T> {

  const timeout = timeoutMs ?? DEFAULT_EVALUATE_TIMEOUT;

  // Get stream context to find the abort signal.
  const streamIdStr = getStreamId();
  const signal = streamIdStr !== undefined ? getAbortSignal(streamIdStr) : undefined;

  // Check if already aborted before starting.
  if(signal?.aborted) {

    throw new EvaluateAbortError();
  }

  // Start the evaluate call. We use 'as unknown as' to bypass TypeScript's strict function signature checking since Puppeteer's evaluate accepts various function
  // signatures that are difficult to type precisely.
  const evaluatePromise = args ?
    context.evaluate(pageFunction as unknown as (...args: unknown[]) => T, ...args) :
    context.evaluate(pageFunction as unknown as () => T);

  // Attach a no-op catch to suppress unhandled rejection warnings. When we abort or timeout, the underlying CDP call is still pending and will eventually resolve or
  // reject. Without this, we'd get unhandled rejection warnings when the CDP call completes after we've moved on.
  evaluatePromise.catch(() => {});

  // Create the timeout promise.
  const timeoutPromise = new Promise<never>((_, reject) => {

    setTimeout(() => {

      reject(new EvaluateTimeoutError(timeout));
    }, timeout);
  });

  // If we have an abort signal, create an abort promise.
  if(signal) {

    const abortPromise = new Promise<never>((_, reject) => {

      // Check if already aborted (race condition protection).
      if(signal.aborted) {

        reject(new EvaluateAbortError());

        return;
      }

      signal.addEventListener("abort", () => {

        reject(new EvaluateAbortError());
      }, { once: true });
    });

    // Race all three: evaluate, timeout, and abort.
    return Promise.race([ evaluatePromise, timeoutPromise, abortPromise ]);
  }

  // No abort signal available, just race evaluate against timeout.
  return Promise.race([ evaluatePromise, timeoutPromise ]);
}

