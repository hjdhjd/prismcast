/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streamContext.ts: AsyncLocalStorage-based stream context for automatic log correlation.
 */
import { AsyncLocalStorage } from "async_hooks";

/* This module provides an AsyncLocalStorage-based context that automatically propagates through async/await chains. When a stream request is handled, we establish a
 * context containing the stream ID and other metadata. All code executed within that async context can retrieve this information without explicit parameter passing.
 *
 * This is particularly useful for logging - log statements anywhere in the call chain automatically include the stream ID prefix without functions needing to accept
 * and pass through a streamId parameter.
 *
 * IMPORTANT: AsyncLocalStorage context is lost when entering a new async context, such as setInterval or setTimeout callbacks. For these cases, you must re-establish
 * the context by calling runWithStreamContext() at the start of the callback.
 */

/**
 * Stream context containing metadata for the current stream operation. This interface is extensible - additional fields can be added as needed without changing
 * function signatures throughout the codebase.
 */
export interface StreamContext {

  // Friendly channel name (e.g., "NBC", "CNN"). Optional because ad-hoc URL streams may not have a channel name.
  channelName?: string;

  // Unique stream identifier used for log correlation.
  streamId: string;

  // The URL being streamed.
  url?: string;
}

// AsyncLocalStorage instance for stream context. This is the core mechanism that allows context to propagate through async calls.
const streamContextStorage = new AsyncLocalStorage<StreamContext>();

/**
 * Runs a function within a stream context. All async operations called within the function will have access to the stream context via getStreamContext() and
 * getStreamId(). This should be called at stream entry points (request handlers) and at the start of timer callbacks (setInterval, setTimeout) to re-establish
 * context.
 * @param context - The stream context containing streamId and optional metadata.
 * @param fn - The async function to run within the context.
 * @returns The result of the function.
 */
export async function runWithStreamContext<T>(context: StreamContext, fn: () => Promise<T>): Promise<T> {

  return streamContextStorage.run(context, fn);
}

/**
 * Retrieves the full stream context for the current async operation.
 * @returns The stream context, or undefined if not running within a stream context.
 */
export function getStreamContext(): StreamContext | undefined {

  return streamContextStorage.getStore();
}

/**
 * Convenience function to retrieve just the stream ID from the current context.
 * @returns The stream ID, or undefined if not running within a stream context.
 */
export function getStreamId(): string | undefined {

  return streamContextStorage.getStore()?.streamId;
}
