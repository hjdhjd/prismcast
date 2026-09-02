/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clients.test.ts: Unit tests for client tracking by protocol. clients.ts owns a per-stream Map of connected HLS / MPEG-TS clients, applies TTL-based expiration to
 * HLS entries during getClientSummary, and exposes register / unregister / clear primitives. The module gates registration through getStream() from registry.ts to
 * prevent orphaned client entries after stream termination - that gate is a critical correctness path that gets explicit coverage here.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { clearClients, getClientSummary, registerClient, unregisterClient } from "./clients.ts";
import { getNextStreamId, registerStream, unregisterStream } from "./registry.ts";
import assert from "node:assert/strict";
import { makeRegistryEntry } from "./registry.helpers.ts";

describe("registerClient", () => {

  let streamId: number;

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
    streamId = getNextStreamId();
    registerStream(makeRegistryEntry({ id: streamId }));
  });

  afterEach(() => {

    clearClients(streamId);
    unregisterStream(streamId);
    mock.timers.reset();
  });

  test("records an HLS client and surfaces it via the summary", () => {

    registerClient(streamId, "192.168.1.50", "hls");

    const summary = getClientSummary(streamId);

    assert.equal(summary.total, 1);
    assert.deepEqual(summary.clients, [{ count: 1, type: "hls" }]);
  });

  test("records an MPEG-TS client distinctly from an HLS client at the same address", () => {

    // The map key is "protocol:address", so the same client IP using both protocols counts twice (one per protocol). This is the design - HLS and MPEG-TS are
    // separate transports that can co-exist for the same household.
    registerClient(streamId, "192.168.1.50", "hls");
    registerClient(streamId, "192.168.1.50", "mpegts");

    const summary = getClientSummary(streamId);

    assert.equal(summary.total, 2);
    assert.deepEqual(summary.clients, [
      { count: 1, type: "hls" },
      { count: 1, type: "mpegts" }
    ]);
  });

  test("normalizes IPv6-mapped IPv4 addresses so '::ffff:X' and 'X' are the same client", () => {

    // The classic Express-on-IPv6 stack reports addresses as '::ffff:192.168.1.50' on some requests and '192.168.1.50' on others. Without normalization, the same
    // client would double-count. Locks the de-duplication contract.
    registerClient(streamId, "::ffff:192.168.1.50", "hls");
    registerClient(streamId, "192.168.1.50", "hls");

    const summary = getClientSummary(streamId);

    assert.equal(summary.total, 1, "two registrations for the same normalized address collapse to one");
  });

  test("re-registering the same client refreshes lastSeen rather than creating a duplicate", () => {

    // The Map-set semantics are how the HLS TTL is implemented: every playlist poll calls registerClient again, refreshing the timestamp. We verify the count stays
    // at 1 and that an outside-TTL gap still keeps the client visible if a refresh occurred in between.
    registerClient(streamId, "192.168.1.50", "hls");

    mock.timers.tick(20_000);
    registerClient(streamId, "192.168.1.50", "hls");

    // Now advance past 30s but only 11s past the second registration. The TTL is computed from lastSeen, so the client must still be visible.
    mock.timers.tick(11_000);

    const summary = getClientSummary(streamId);

    assert.equal(summary.total, 1, "re-register kept the client alive past the original TTL");
  });

  test("does NOT register a client for an unknown stream (stream-must-exist guard)", () => {

    // Negative test: the registerClient guard checks getStream() and silently no-ops if the stream is gone. This is the protection against orphan client entries
    // surviving across stream termination.
    const phantomId = 999_999;

    registerClient(phantomId, "10.0.0.1", "hls");

    const summary = getClientSummary(phantomId);

    assert.equal(summary.total, 0, "guard suppressed the registration for an unknown stream");
    assert.deepEqual(summary.clients, [], "no per-type entries either");
  });
});

describe("unregisterClient", () => {

  let streamId: number;

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
    streamId = getNextStreamId();
    registerStream(makeRegistryEntry({ id: streamId }));
  });

  afterEach(() => {

    clearClients(streamId);
    unregisterStream(streamId);
    mock.timers.reset();
  });

  test("removes an MPEG-TS client previously registered for the same protocol", () => {

    registerClient(streamId, "192.168.1.50", "mpegts");
    assert.equal(getClientSummary(streamId).total, 1);

    unregisterClient(streamId, "192.168.1.50", "mpegts");
    assert.equal(getClientSummary(streamId).total, 0);
  });

  test("normalizes addresses on unregister too (mirrors registerClient)", () => {

    // Locks the symmetry: register/unregister must use the same normalization. Otherwise an unregister with the IPv6-mapped form would fail to find the entry
    // registered with the bare IPv4 form.
    registerClient(streamId, "::ffff:192.168.1.50", "mpegts");

    unregisterClient(streamId, "192.168.1.50", "mpegts");

    assert.equal(getClientSummary(streamId).total, 0, "unregister succeeded across IPv6-mapped/bare boundary");
  });

  test("only removes the matching protocol entry (does not affect other protocols at same address)", () => {

    registerClient(streamId, "192.168.1.50", "hls");
    registerClient(streamId, "192.168.1.50", "mpegts");

    unregisterClient(streamId, "192.168.1.50", "mpegts");

    const summary = getClientSummary(streamId);

    assert.equal(summary.total, 1);
    assert.deepEqual(summary.clients, [{ count: 1, type: "hls" }], "HLS entry preserved");
  });

  test("is a no-op when the stream has no client map yet", () => {

    // Negative test: tear-down code may unregister before any registration occurred. Must not throw.
    assert.doesNotThrow(() => {

      unregisterClient(streamId, "10.0.0.1", "mpegts");
    });
  });

  test("is a no-op for a previously-unknown client (silent ignore)", () => {

    registerClient(streamId, "10.0.0.1", "hls");

    assert.doesNotThrow(() => {

      unregisterClient(streamId, "10.0.0.99", "hls");
    });

    assert.equal(getClientSummary(streamId).total, 1, "the existing client is unaffected");
  });
});

describe("getClientSummary", () => {

  let streamId: number;

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
    streamId = getNextStreamId();
    registerStream(makeRegistryEntry({ id: streamId }));
  });

  afterEach(() => {

    clearClients(streamId);
    unregisterStream(streamId);
    mock.timers.reset();
  });

  test("returns an empty summary for a stream that never had clients", () => {

    const summary = getClientSummary(streamId);

    assert.equal(summary.total, 0);
    assert.deepEqual(summary.clients, []);
  });

  test("expires HLS clients past the 30-second TTL on the next summary call", () => {

    // The TTL is documented as 30000ms. The implementation deletes entries where (now - lastSeen) > TTL, so we step just past the boundary.
    registerClient(streamId, "10.0.0.1", "hls");
    assert.equal(getClientSummary(streamId).total, 1);

    mock.timers.tick(30_001);

    assert.equal(getClientSummary(streamId).total, 0, "HLS client expired past 30s TTL");
  });

  test("does NOT expire HLS clients exactly at the TTL boundary (strict greater-than)", () => {

    // The implementation uses '> TTL' not '>= TTL', so a delta of exactly 30000 keeps the client alive. Locks the off-by-one boundary.
    registerClient(streamId, "10.0.0.1", "hls");

    mock.timers.tick(30_000);

    assert.equal(getClientSummary(streamId).total, 1, "boundary is strict-greater, so 30000ms is still inside TTL");
  });

  test("does NOT expire MPEG-TS clients via TTL (only HLS is TTL-based)", () => {

    // MPEG-TS clients have persistent connections - they are managed by explicit register/unregister, not TTL. Locks the protocol-specific lifecycle contract.
    registerClient(streamId, "10.0.0.1", "mpegts");

    mock.timers.tick(60_000);

    assert.equal(getClientSummary(streamId).total, 1, "MPEG-TS survives well past the HLS TTL");
  });

  test("aggregates per-type counts across multiple clients", () => {

    registerClient(streamId, "10.0.0.1", "hls");
    registerClient(streamId, "10.0.0.2", "hls");
    registerClient(streamId, "10.0.0.3", "hls");
    registerClient(streamId, "10.0.0.4", "mpegts");

    const summary = getClientSummary(streamId);

    assert.equal(summary.total, 4);
    assert.deepEqual(summary.clients, [
      { count: 3, type: "hls" },
      { count: 1, type: "mpegts" }
    ]);
  });

  test("sorts the per-type breakdown alphabetically by type ('hls' before 'mpegts')", () => {

    // The sort uses localeCompare; with two lowercase strings 'hls' and 'mpegts' the order is straightforwardly alphabetic.
    // Locks the consistent ordering that UI rendering depends on.
    registerClient(streamId, "10.0.0.1", "mpegts");
    registerClient(streamId, "10.0.0.2", "hls");

    const summary = getClientSummary(streamId);

    assert.deepEqual(summary.clients.map((c) => c.type), [ "hls", "mpegts" ]);
  });

  test("returns a fresh summary array on each call (no shared reference)", () => {

    // Ensures callers can mutate the returned array without affecting subsequent calls. Tests against a future regression that returns a cached result.
    registerClient(streamId, "10.0.0.1", "hls");

    const a = getClientSummary(streamId);
    const b = getClientSummary(streamId);

    assert.notEqual(a.clients, b.clients, "fresh array per call");
  });

  test("returns total: 0 after every HLS client expires (clean-up path)", () => {

    // Boundary: when expiration empties the map, the implementation deletes the per-stream Map entirely and returns the zero-summary. This locks the cleanup
    // path so getClientSummary is safe to call in a tight loop without leaking empty Maps.
    registerClient(streamId, "10.0.0.1", "hls");
    registerClient(streamId, "10.0.0.2", "hls");

    mock.timers.tick(30_001);

    const summary = getClientSummary(streamId);

    assert.equal(summary.total, 0);
    assert.deepEqual(summary.clients, []);
  });
});

describe("clearClients", () => {

  let streamId: number;

  beforeEach(() => {

    mock.timers.enable({ apis: ["Date"], now: 1_700_000_000_000 });
    streamId = getNextStreamId();
    registerStream(makeRegistryEntry({ id: streamId }));
  });

  afterEach(() => {

    unregisterStream(streamId);
    mock.timers.reset();
  });

  test("removes every client for a stream (HLS and MPEG-TS together)", () => {

    registerClient(streamId, "10.0.0.1", "hls");
    registerClient(streamId, "10.0.0.2", "mpegts");

    clearClients(streamId);

    assert.equal(getClientSummary(streamId).total, 0);
  });

  test("is a no-op for an unknown stream", () => {

    // Negative test: cleanup paths may call clearClients with an ID whose registration was rolled back. Must not throw.
    assert.doesNotThrow(() => {

      clearClients(999_999);
    });
  });

  test("is a no-op on repeated calls", () => {

    registerClient(streamId, "10.0.0.1", "hls");
    clearClients(streamId);

    assert.doesNotThrow(() => {

      clearClients(streamId);
    });

    assert.equal(getClientSummary(streamId).total, 0);
  });
});
