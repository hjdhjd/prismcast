/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logEmitter.test.ts: Unit tests for the SSE log emitter in logEmitter.ts. The module wraps a singleton EventEmitter; each test that subscribes registers
 * its unsubscribe function with a shared array so a single afterEach hook can guarantee the singleton's listener list is cleared between cases, even when
 * a test throws before reaching its own cleanup.
 */
import { afterEach, describe, test } from "node:test";
import { emitLogEntry, subscribeToLogs } from "./logEmitter.ts";
import type { LogEntry } from "./logEmitter.ts";
import assert from "node:assert/strict";

// makeEntry builds a LogEntry literal with sensible defaults; the multiple cases below each construct a LogEntry, so a small inline factory
// keeps the call sites compact without needing a separate helpers file.
function makeEntry(overrides: Partial<LogEntry> = {}): LogEntry {

  return {

    level: "info",
    message: "default message",
    timestamp: "2026/05/02 09:00:00.000 AM",
    ...overrides
  };
}

describe("emitLogEntry and subscribeToLogs", () => {

  // We track active unsubscribe functions so afterEach can guarantee cleanup even if a test throws.
  const activeUnsubscribes: (() => void)[] = [];

  afterEach(() => {

    while(activeUnsubscribes.length > 0) {

      const unsub = activeUnsubscribes.pop();

      unsub?.();
    }
  });

  test("delivers an emitted entry to a single subscribed callback", () => {

    const received: LogEntry[] = [];

    activeUnsubscribes.push(subscribeToLogs((entry) => { received.push(entry); }));

    const entry = makeEntry({ message: "hello" });

    emitLogEntry(entry);

    assert.equal(received.length, 1, "exactly one delivery");
    assert.equal(received[0], entry, "delivered entry is the same reference (no copy)");
  });

  test("delivers a single emit to every subscribed callback (broadcast)", () => {

    // Three subscribers, one emit -> three deliveries. Locks the broadcast contract that SSE clients rely on.
    const a: LogEntry[] = [];
    const b: LogEntry[] = [];
    const c: LogEntry[] = [];

    activeUnsubscribes.push(subscribeToLogs((entry) => { a.push(entry); }));
    activeUnsubscribes.push(subscribeToLogs((entry) => { b.push(entry); }));
    activeUnsubscribes.push(subscribeToLogs((entry) => { c.push(entry); }));

    emitLogEntry(makeEntry({ message: "broadcast" }));

    assert.equal(a.length, 1);
    assert.equal(b.length, 1);
    assert.equal(c.length, 1);
  });

  test("returns an unsubscribe function that removes the listener", () => {

    const received: LogEntry[] = [];
    const unsubscribe = subscribeToLogs((entry) => { received.push(entry); });

    emitLogEntry(makeEntry());

    assert.equal(received.length, 1, "first emit delivered");

    unsubscribe();

    emitLogEntry(makeEntry());

    assert.equal(received.length, 1, "second emit not delivered after unsubscribe");
  });

  test("unsubscribe is safe to call twice", () => {

    // Negative test: calling unsubscribe a second time must not throw and must not affect other subscribers.
    const received: LogEntry[] = [];
    const unsubscribe = subscribeToLogs((entry) => { received.push(entry); });

    assert.doesNotThrow(() => {

      unsubscribe();
      unsubscribe();
    });

    emitLogEntry(makeEntry());
    assert.equal(received.length, 0, "no delivery after unsubscribe");
  });

  test("preserves all fields on the emitted entry (level, message, timestamp, optional categoryTag)", () => {

    const received: LogEntry[] = [];

    activeUnsubscribes.push(subscribeToLogs((entry) => { received.push(entry); }));

    const entry: LogEntry = {

      categoryTag: "tuning:hulu",
      level: "debug",
      message: "binary search converged",
      timestamp: "2026/05/02 09:00:00.000 AM"
    };

    emitLogEntry(entry);

    const delivered = received[0]!;

    assert.equal(delivered.level, "debug");
    assert.equal(delivered.message, "binary search converged");
    assert.equal(delivered.categoryTag, "tuning:hulu");
    assert.equal(delivered.timestamp, "2026/05/02 09:00:00.000 AM");
  });

  test("emit with no subscribers is a no-op (does not throw)", () => {

    // Boundary: emitting on a fresh emitter with no listeners must succeed silently. The SSE log path fires for every log line; a startup-time emit before any
    // browser is connected hits this code path on every PrismCast launch.
    assert.doesNotThrow(() => {

      emitLogEntry(makeEntry({ message: "no listeners" }));
    });
  });

  test("a subscriber added mid-emit-cycle does not receive the in-progress emit", () => {

    // Negative test: EventEmitter's standard semantics deliver only to listeners present at emit time. A listener registered inside another listener's callback
    // must not fire for the same emit.
    const received: LogEntry[] = [];

    activeUnsubscribes.push(subscribeToLogs(() => {

      activeUnsubscribes.push(subscribeToLogs((entry) => { received.push(entry); }));
    }));

    emitLogEntry(makeEntry({ message: "first" }));

    assert.equal(received.length, 0, "the inner listener was added during the emit but did not receive that emit");

    emitLogEntry(makeEntry({ message: "second" }));

    assert.equal(received.length, 1, "the inner listener does receive the next emit");
  });

  test("emits are delivered synchronously (callback runs before emit returns)", () => {

    // EventEmitter.emit is synchronous - a subscriber sees the entry before control returns to emitLogEntry's caller. SSE infrastructure depends on this.
    let called = false;

    activeUnsubscribes.push(subscribeToLogs(() => { called = true; }));

    emitLogEntry(makeEntry());

    assert.equal(called, true, "subscriber ran synchronously");
  });
});
