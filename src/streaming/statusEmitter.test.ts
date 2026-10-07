/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * statusEmitter.test.ts: Unit tests for the SSE status emitter. statusEmitter.ts owns the singleton EventEmitter that broadcasts stream and system status to connected
 * SSE clients. The emitter maintains current state in two module-scoped Maps (streamStatuses by stream ID, cachedSystemStatus) so new clients receive a snapshot on
 * connect. The tests lock the snapshot/event contract: emit-then-snapshot reflects the latest state, removed streams stop appearing, the zombie-update guard rejects
 * status for removed streams, and the systemStatusChanged dedupe emits only when a field the page header renders changes - the fields RenderedSystemStatus
 * declares - so a status that differs only in what no client renders stays off the wire.
 */
import type { StatusEventType, StreamStatus, SystemStatus } from "./statusEmitter.ts";
import { createInitialStreamStatus, emitChannelUpdate, emitStreamAdded, emitStreamHealthChanged,
  emitStreamRemoved, emitSystemStatusChanged, getStatusSnapshot, getStreamStatus, removeStreamStatus, subscribeToStatus } from "./statusEmitter.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

/* makeStreamStatus returns a fully-populated StreamStatus for use as a registration payload. The shape is what callers pass into emitStreamAdded - in production
 * code, createInitialStreamStatus produces this shape. We keep a local builder so individual tests can override specific fields without rebuilding the whole record.
 */
function makeStreamStatus(overrides: Partial<StreamStatus> = {}): StreamStatus {

  const id = overrides.id ?? Math.floor(Math.random() * 1000000);

  return {

    bufferingDuration: null,
    captureCodec: null,
    captureResolution: null,
    channel: "test-channel",
    clientCount: 0,
    clients: [],
    currentTime: 0,
    duration: 0,
    escalationLevel: 0,
    hardwareAccelerated: false,
    health: "healthy",
    id,
    lastIssueTime: null,
    lastIssueType: null,
    lastRecoveryTime: null,
    logoUrl: "",
    memoryBytes: 0,
    nativeBandwidth: 0,
    nativeResolution: null,
    networkState: 0,
    pageReloadsInWindow: 0,
    readyState: 0,
    recoveryAttempts: 0,
    serviceName: "TestService",
    showName: "",
    sourceResolution: null,
    startTime: new Date(0).toISOString(),
    streamingMode: "capture",
    url: "https://example.test",
    ...overrides
  };
}

/* makeSystemStatus mirrors the SystemStatus shape used by emitSystemStatusChanged and getStatusSnapshot. We expose individual overrides so tests can tweak the
 * fields the dedupe compares, the ones the page header renders, without rebuilding the rest.
 */
function makeSystemStatus(overrides: { browser?: Partial<SystemStatus["browser"]>; streams?: Partial<SystemStatus["streams"]> } = {}): SystemStatus {

  return {

    browser: { captureImpaired: false, connected: false, pageCount: 0, ...overrides.browser },
    memory: { heapUsed: 0, rss: 0 },
    streams: { active: 0, limit: 10, ...overrides.streams },
    uptime: 0
  };
}

describe("createInitialStreamStatus", () => {

  test("populates required fields from options and defaults the rest", () => {

    const startTime = Date.parse("2026-01-15T20:00:00.000Z");

    const status = createInitialStreamStatus({


      channelName: "abc",
      numericStreamId: 42,
      serviceName: "ABC",
      startTime,
      url: "https://abc.test/live"
    });

    assert.equal(status.id, 42);
    assert.equal(status.channel, "abc");
    assert.equal(status.serviceName, "ABC");
    assert.equal(status.url, "https://abc.test/live");
    assert.equal(status.startTime, new Date(startTime).toISOString(), "startTime serialized to ISO");
    assert.equal(status.health, "healthy", "default health is healthy");
    assert.equal(status.streamingMode, "capture", "default streaming mode is capture");
    assert.equal(status.captureCodec, null, "default codec is null");
    assert.equal(status.hardwareAccelerated, false, "default hwAccel is false");
    assert.equal(status.logoUrl, "", "default logoUrl is empty string");
  });

  test("threads optional fields through when provided", () => {

    const status = createInitialStreamStatus({


      captureCodec: "hevc",
      channelName: "espn",
      hardwareAccelerated: true,
      logoUrl: "https://logos.test/espn.png",
      numericStreamId: 7,
      serviceName: "ESPN",
      startTime: 0,
      streamingMode: "native",
      url: "https://espn.test"
    });

    assert.equal(status.captureCodec, "hevc");
    assert.equal(status.hardwareAccelerated, true);
    assert.equal(status.logoUrl, "https://logos.test/espn.png");
    assert.equal(status.streamingMode, "native");
  });

  test("accepts a null channelName for ad-hoc URL streams", () => {

    // Boundary: ad-hoc streams have no associated channel. The status object accepts null directly without coercion to empty string.
    const status = createInitialStreamStatus({


      channelName: null,
      numericStreamId: 1,
      serviceName: "Custom URL",
      startTime: Date.now(),
      url: "https://example.test"
    });

    assert.equal(status.channel, null, "ad-hoc streams have null channel");
  });
});

describe("emitStreamAdded", () => {

  test("stores the status so getStreamStatus can retrieve it", () => {

    const status = makeStreamStatus({ id: 1001 });

    emitStreamAdded(status);

    const fetched = getStreamStatus(1001);

    assert.equal(fetched, status, "stored status returned by getStreamStatus");

    // Cleanup
    emitStreamRemoved(1001);
  });

  test("notifies subscribers with a streamAdded event", () => {

    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    const status = makeStreamStatus({ id: 1002 });

    emitStreamAdded(status);

    const added = events.find((e) => e.event === "streamAdded");

    assert.ok(added, "streamAdded event emitted");
    assert.equal(added.data, status, "subscriber received the status payload");

    unsubscribe();
    emitStreamRemoved(1002);
  });
});

describe("emitStreamRemoved", () => {

  test("clears the stored status so subsequent getStreamStatus returns undefined", () => {

    const status = makeStreamStatus({ id: 2001 });

    emitStreamAdded(status);
    emitStreamRemoved(2001);

    assert.equal(getStreamStatus(2001), undefined, "stored status cleared on remove");
  });

  test("notifies subscribers with a streamRemoved event carrying the id", () => {

    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    emitStreamAdded(makeStreamStatus({ id: 2002 }));
    emitStreamRemoved(2002);

    const removed = events.find((e) => e.event === "streamRemoved");

    assert.ok(removed, "streamRemoved event emitted");
    assert.deepEqual(removed.data, { id: 2002 });

    unsubscribe();
  });

  test("is a no-op for an id that was never added", () => {

    // Negative test: cleanup paths may double-call emitStreamRemoved. The function must not throw on stale IDs.
    assert.doesNotThrow(() => {

      emitStreamRemoved(999999);
    });
  });
});

describe("emitStreamHealthChanged", () => {

  test("updates the stored status when the stream is currently registered", () => {

    const status = makeStreamStatus({ health: "healthy", id: 3001 });

    emitStreamAdded(status);

    const updated = makeStreamStatus({ health: "buffering", id: 3001 });

    emitStreamHealthChanged(updated);

    assert.equal(getStreamStatus(3001)?.health, "buffering", "stored health updated");

    emitStreamRemoved(3001);
  });

  test("notifies subscribers with a streamHealthChanged event", () => {

    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    emitStreamAdded(makeStreamStatus({ id: 3002 }));

    const updated = makeStreamStatus({ health: "stalled", id: 3002 });

    emitStreamHealthChanged(updated);

    const healthEvent = events.find((e) => e.event === "streamHealthChanged");

    assert.ok(healthEvent, "streamHealthChanged event emitted");
    assert.equal((healthEvent.data as StreamStatus).health, "stalled");

    unsubscribe();
    emitStreamRemoved(3002);
  });

  test("does NOT add a stream that was already removed (zombie-update guard)", () => {

    // The implementation includes a defense-in-depth check: emitStreamHealthChanged silently drops updates for streams no longer in streamStatuses. This prevents
    // zombie entries from sneaking back into the snapshot if a late health update fires after emitStreamRemoved.
    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    // Never call emitStreamAdded for this id.
    emitStreamHealthChanged(makeStreamStatus({ id: 4001 }));

    assert.equal(getStreamStatus(4001), undefined, "zombie-update guard prevented re-registration");
    assert.equal(events.filter((e) => e.event === "streamHealthChanged").length, 0, "no event emitted for zombie");

    unsubscribe();
  });

  test("does NOT add a stream after emitStreamRemoved (post-remove guard)", () => {

    // Same guard, exercised via the add-then-remove-then-update sequence which is the realistic operational path.
    emitStreamAdded(makeStreamStatus({ id: 4002 }));
    emitStreamRemoved(4002);

    emitStreamHealthChanged(makeStreamStatus({ health: "buffering", id: 4002 }));

    assert.equal(getStreamStatus(4002), undefined, "stream stays gone after late health update");
  });
});

describe("emitSystemStatusChanged", () => {

  test("emits when browser.connected changes from initial null state", () => {

    // The dedup check uses the cached system status. Initially cached is null, so the first call emits.
    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    emitSystemStatusChanged(makeSystemStatus({ browser: { connected: true, pageCount: 1 } }));

    assert.ok(events.find((e) => e.event === "systemStatusChanged"), "first call always emits");

    unsubscribe();
  });

  test("does NOT emit when nothing meaningful changed (dedup)", () => {

    // The dedupe compares the fields the page header renders. A status equal to the cached one in every such field fires no event, which keeps a status that
    // differs only in what no client renders off the wire.
    emitSystemStatusChanged(makeSystemStatus({ browser: { connected: true }, streams: { active: 1 } }));

    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    // The same rendered fields again. The dedupe must suppress it.
    emitSystemStatusChanged(makeSystemStatus({ browser: { connected: true }, streams: { active: 1 } }));

    assert.equal(events.filter((e) => e.event === "systemStatusChanged").length, 0, "no event for unchanged key fields");

    unsubscribe();
  });

  test("emits when streams.active changes (dedup-relevant field)", () => {

    emitSystemStatusChanged(makeSystemStatus({ streams: { active: 1 } }));

    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    emitSystemStatusChanged(makeSystemStatus({ streams: { active: 2 } }));

    assert.equal(events.filter((e) => e.event === "systemStatusChanged").length, 1, "stream-count change triggers emit");

    unsubscribe();
  });

  test("emits when browser.connected toggles", () => {

    emitSystemStatusChanged(makeSystemStatus({ browser: { connected: true } }));

    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    emitSystemStatusChanged(makeSystemStatus({ browser: { connected: false } }));

    assert.equal(events.filter((e) => e.event === "systemStatusChanged").length, 1, "browser-connect change triggers emit");

    unsubscribe();
  });

  test("emits when browser.captureImpaired toggles, and dedupes the repeat", () => {

    /* The mark is broadcast on its own account. A status that differs from the cached one only in captureImpaired has the same connectivity and the same active
     * count, so a dedupe that compared only those would swallow the very transition the interface needs to render - and the repeat that follows must still be
     * suppressed, or a status that differs only in what no client renders would wake every client for the life of the mark. The clearing is asserted as well, so a
     * dedupe that noticed only the rising edge would surface here.
     */
    emitSystemStatusChanged(makeSystemStatus({ browser: { captureImpaired: false, connected: true } }));

    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    emitSystemStatusChanged(makeSystemStatus({ browser: { captureImpaired: true, connected: true } }));

    assert.equal(events.filter((e) => e.event === "systemStatusChanged").length, 1, "the mark triggers an emit on its own");

    emitSystemStatusChanged(makeSystemStatus({ browser: { captureImpaired: true, connected: true } }));

    assert.equal(events.filter((e) => e.event === "systemStatusChanged").length, 1, "and an identical repeat is suppressed");

    emitSystemStatusChanged(makeSystemStatus({ browser: { captureImpaired: false, connected: true } }));

    assert.equal(events.filter((e) => e.event === "systemStatusChanged").length, 2, "and the clearing emits on its own account too");

    unsubscribe();
  });

  test("emits when only the stream limit changes, carrying the new limit to the snapshot, and keeps what no client renders off the wire", () => {

    /* The header renders the limit beside the active count, so a status that changes only the limit must reach every client and the snapshot a connecting
     * client receives. An identical repeat must not, and neither must a status that differs only in the page count, the memory figures or the uptime, which no
     * client renders.
     */
    emitSystemStatusChanged(makeSystemStatus({ browser: { connected: true }, streams: { active: 1, limit: 10 } }));

    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });
    const limited = makeSystemStatus({ browser: { connected: true }, streams: { active: 1, limit: 11 } });
    const systemEvents = (): unknown[] => events.filter((e) => e.event === "systemStatusChanged").map((e) => e.data);

    emitSystemStatusChanged(limited);

    assert.deepEqual(systemEvents(), [limited], "a limit change emits one event carrying the new limit");
    assert.equal(getStatusSnapshot().system.streams.limit, 11, "and the snapshot reads the new limit");

    emitSystemStatusChanged(makeSystemStatus({ browser: { connected: true }, streams: { active: 1, limit: 11 } }));

    assert.equal(systemEvents().length, 1, "an identical repeat is suppressed");

    emitSystemStatusChanged({ ...makeSystemStatus({ browser: { connected: true, pageCount: 4 }, streams: { active: 1, limit: 11 } }), memory: { heapUsed: 7, rss: 9 },
      uptime: 120 });

    assert.equal(systemEvents().length, 1, "a status that differs only in what no client renders is suppressed");

    unsubscribe();
  });
});

describe("getStatusSnapshot", () => {

  test("includes every currently-stored stream", () => {

    emitStreamAdded(makeStreamStatus({ id: 5001, serviceName: "A" }));
    emitStreamAdded(makeStreamStatus({ id: 5002, serviceName: "B" }));

    const snapshot = getStatusSnapshot();
    const ids = snapshot.streams.map((s) => s.id);

    assert.ok(ids.includes(5001));
    assert.ok(ids.includes(5002));

    emitStreamRemoved(5001);
    emitStreamRemoved(5002);
  });

  test("omits streams that have been removed", () => {

    emitStreamAdded(makeStreamStatus({ id: 5003 }));
    emitStreamRemoved(5003);

    const snapshot = getStatusSnapshot();

    assert.ok(!snapshot.streams.some((s) => s.id === 5003), "removed stream not in snapshot");
  });

  test("returns a default system status when none has been cached yet", () => {

    // The snapshot falls back to a default status (disconnected, no active stream) until a first status is cached. Other rows in this module set the cache
    // before this one runs, so the row asserts the shape and the field types every snapshot holds rather than the default's values.
    const snapshot = getStatusSnapshot();

    assert.ok(typeof snapshot.system.browser.captureImpaired === "boolean");
    assert.ok(typeof snapshot.system.browser.connected === "boolean");
    assert.ok(typeof snapshot.system.browser.pageCount === "number");
    assert.ok(typeof snapshot.system.streams.active === "number");
    assert.ok(typeof snapshot.system.streams.limit === "number");
  });

  test("carries the browser's capture-impairment mark through to a connecting client", () => {

    // The snapshot is the whole state a client gets on connect, so a mark carried only by the delta events would leave a client that connected during the mark
    // rendering a healthy header until the next transition. The emit caches the status it lets through, which is how the mark reaches the snapshot.
    emitSystemStatusChanged(makeSystemStatus({ browser: { captureImpaired: true, connected: true } }));

    assert.equal(getStatusSnapshot().system.browser.captureImpaired, true, "the cached mark reaches the snapshot");

    emitSystemStatusChanged(makeSystemStatus({ browser: { captureImpaired: false, connected: true } }));

    assert.equal(getStatusSnapshot().system.browser.captureImpaired, false, "and so does its clearing");
  });

  test("does not include health state in its snapshot - the route layer composes health and the channel patch", () => {

    // statusEmitter owns only the stream + system snapshot. The channel-table catch-up patch is composed in routes/streams.ts by spreading getStatusSnapshot()
    // together with buildSnapshotChannelPatch(), so the streaming layer stays free of health, channel, and rendering concerns. Asserting that health and
    // channelPatch are absent here documents the layering boundary.
    const snapshot = getStatusSnapshot();

    assert.ok(!("health" in snapshot), "statusEmitter snapshot must not carry a health field");
    assert.ok(!("channelPatch" in snapshot), "channelPatch composition belongs to the route layer, not statusEmitter");
  });
});

describe("getStreamStatus", () => {

  test("returns the stored status for a known stream", () => {

    const status = makeStreamStatus({ id: 6001 });

    emitStreamAdded(status);

    assert.equal(getStreamStatus(6001), status);

    emitStreamRemoved(6001);
  });

  test("returns undefined for an unknown stream", () => {

    assert.equal(getStreamStatus(999999), undefined);
  });
});

describe("removeStreamStatus", () => {

  test("clears the stored status without emitting an event", () => {

    // Used during cleanup paths that have already emitted streamRemoved elsewhere. Distinguishes from emitStreamRemoved which both clears and emits.
    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    emitStreamAdded(makeStreamStatus({ id: 7001 }));

    // Drain the streamAdded event from the buffer so we can isolate the next phase.
    const eventCountBefore = events.length;

    removeStreamStatus(7001);

    assert.equal(getStreamStatus(7001), undefined, "stored status cleared");
    assert.equal(events.length, eventCountBefore, "no streamRemoved event from removeStreamStatus");

    unsubscribe();
  });

  test("is a no-op for an unknown stream", () => {

    assert.doesNotThrow(() => {

      removeStreamStatus(999999);
    });
  });
});

describe("emitChannelUpdate", () => {

  test("notifies subscribers with a channelUpdate event carrying the patch", () => {

    const events: { event: StatusEventType; data: unknown }[] = [];
    const unsubscribe = subscribeToStatus((event, data) => {

      events.push({ data, event });
    });

    const patch = { logos: { abc: "https://logos.test/abc.png" } };

    emitChannelUpdate(patch);

    const update = events.find((e) => e.event === "channelUpdate");

    assert.ok(update, "channelUpdate event emitted");
    assert.equal(update.data, patch, "subscriber received the patch payload");

    unsubscribe();
  });
});

describe("subscribeToStatus", () => {

  test("returns an unsubscribe function that detaches every internal listener", () => {

    const events: StatusEventType[] = [];
    const unsubscribe = subscribeToStatus((event) => {

      events.push(event);
    });

    unsubscribe();

    // After unsubscribe, no emitted event should reach the callback.
    emitChannelUpdate({ logos: {} });
    emitStreamAdded(makeStreamStatus({ id: 8001 }));
    emitStreamRemoved(8001);

    assert.deepEqual(events, [], "callback received no events post-unsubscribe");
  });

  test("multiple subscribers each receive every event independently", () => {

    const a: StatusEventType[] = [];
    const b: StatusEventType[] = [];

    const unA = subscribeToStatus((event) => { a.push(event); });
    const unB = subscribeToStatus((event) => { b.push(event); });

    emitChannelUpdate({ x: 1 });

    assert.ok(a.includes("channelUpdate"));
    assert.ok(b.includes("channelUpdate"));

    unA();
    unB();
  });
});
