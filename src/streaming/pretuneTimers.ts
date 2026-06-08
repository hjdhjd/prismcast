/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pretuneTimers.ts: Ownership of the pretune safety-timer registry.
 *
 * A pretuned stream is started ahead of its scheduled time and reaped by a per-stream safety timer if no real client ever claims it. That timer must be cancelled
 * when the stream IS claimed and terminated through the normal lifecycle, so terminateStream() (lifecycle.ts) needs to clear it. This registry lives in its own
 * leaf module - importing nothing from the streaming graph - so lifecycle.ts can clear a timer without importing pretune.ts. pretune.ts imports lifecycle.ts (for
 * terminateStream) and hls.ts (for initializeStream/validateChannel), so a direct lifecycle->pretune edge would close a hls -> lifecycle -> pretune -> hls import
 * cycle. Keeping the timer state here is the single source of truth for the registry and keeps the dependency edges acyclic.
 */

// Safety timers keyed by stream ID. Used to tear down unclaimed pretuned streams after the scheduled start time. Owned here so both the producer (pretune.ts, which
// schedules them) and the consumer (lifecycle.ts, which clears them on normal termination) reference one registry without a cyclic import.
const safetyTimers = new Map<number, ReturnType<typeof setTimeout>>();

/**
 * Records the pending safety timer for a pretuned stream, replacing any existing entry for that stream ID. Called by pretune.ts when it schedules the reaper.
 * @param streamId - The numeric stream ID the timer guards.
 * @param timer - The timer handle to track.
 */
export function setPretuneSafetyTimer(streamId: number, timer: ReturnType<typeof setTimeout>): void {

  // Cancel any prior reaper for this stream before tracking the new one, so the registry holds at most one live timer per stream and an overwritten handle can never
  // survive to fire against the same stream later. In practice a stream is pretuned once, so this is a no-op; it keeps the registry's one-timer-per-stream invariant
  // robust against any future re-registration.
  clearPretuneSafetyTimer(streamId);

  safetyTimers.set(streamId, timer);
}

/**
 * Forgets a safety timer entry without cancelling it. Called from inside the timer's own callback after it has fired, where clearing the (already-elapsed) timer
 * would be redundant; we only need to drop the bookkeeping entry.
 * @param streamId - The numeric stream ID whose entry to drop.
 */
export function forgetPretuneSafetyTimer(streamId: number): void {

  safetyTimers.delete(streamId);
}

/**
 * Cancels and forgets the pending safety timer for a pretuned stream. Called by terminateStream() when a pretuned stream is claimed and torn down through the normal
 * lifecycle, so the safety timeout - which exists only to reap streams that were never claimed - does not linger in the Map until it fires harmlessly against an
 * already-gone stream. Safe to call for any stream ID; streams without a pending safety timer are a no-op.
 * @param streamId - The numeric stream ID whose safety timer to clear.
 */
export function clearPretuneSafetyTimer(streamId: number): void {

  const timer = safetyTimers.get(streamId);

  if(timer) {

    clearTimeout(timer);
    safetyTimers.delete(streamId);
  }
}

/**
 * Cancels and forgets every pending safety timer. Called by stopPretunePolling() on server shutdown so no reaper survives the polling loop.
 */
export function clearAllPretuneSafetyTimers(): void {

  for(const timer of safetyTimers.values()) {

    clearTimeout(timer);
  }

  safetyTimers.clear();
}
