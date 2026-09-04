/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * sse.test.ts: Unit tests for installSseStream in sse.ts. The helper is the single source of truth for Server-Sent Events transport setup across /logs/stream
 * and /streams/status: it owns the response headers (Cache-Control, Connection, Content-Type), the immediate flushHeaders() call that opens the EventSource
 * handshake, the 30-second keep-alive heartbeat, and the close() teardown that clears the heartbeat timer. Every consumer route relies on these wire-byte
 * contracts; tests assert them here so a regression in any branch fails at this tier rather than silently degrading the live status / log streams.
 *
 * The suite uses makeReqRes from ./express.helpers.ts to synthesize an Express Response object with mock.fn-backed setHeader / flushHeaders / write spies, and
 * mock.timers to drive the heartbeat interval deterministically without sleeping.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import assert from "node:assert/strict";
import { installSseStream } from "./sse.ts";
import { makeReqRes } from "./express.helpers.ts";

// The exact heartbeat frame the helper writes every 30 seconds. Asserting this literal locks the keep-alive contract a proxy depends on - any drift here would
// silently change the on-the-wire shape that EventSource clients and intermediaries observe.
const HEARTBEAT_FRAME = "event: heartbeat\ndata: \n\n";

const HEARTBEAT_INTERVAL_MS = 30000;

describe("installSseStream - response headers", () => {

  test("calls setHeader exactly three times in the documented order with correct argument pairs", () => {

    // The helper sets Cache-Control, Connection, and Content-Type in that fixed order. Locking the call shape (count, ordering, exact argument pairs) catches
    // regressions where a consumer accidentally adds, removes, or reorders headers - any of which would break proxy routing or the EventSource handshake.
    const { res, setHeader } = makeReqRes();

    installSseStream(res);

    assert.equal(setHeader.mock.callCount(), 3, "exactly three setHeader calls");
    assert.deepEqual(setHeader.mock.calls[0]?.arguments, [ "Cache-Control", "no-cache" ]);
    assert.deepEqual(setHeader.mock.calls[1]?.arguments, [ "Connection", "keep-alive" ]);
    assert.deepEqual(setHeader.mock.calls[2]?.arguments, [ "Content-Type", "text/event-stream" ]);
  });

  test("calls flushHeaders exactly once before the heartbeat starts", () => {

    // flushHeaders() is the contract that opens the EventSource handshake on the wire - clients block in `connecting` state until the response head arrives.
    // The helper must invoke it exactly once and BEFORE any data writes (the heartbeat's first tick is later, but flush ordering is asserted below by checking
    // that no write happens at install-time).
    const { flushHeaders, res, write } = makeReqRes();

    installSseStream(res);

    assert.equal(flushHeaders.mock.callCount(), 1, "flushHeaders must run exactly once");
    assert.equal(write.mock.callCount(), 0, "no data writes occur during install (heartbeat fires later via the interval)");
  });
});

describe("installSseStream - heartbeat", () => {

  beforeEach(() => {

    // We virtualize setInterval so the heartbeat cadence is deterministic. The Date API is left alone because the helper does not read the wall clock.
    mock.timers.enable({ apis: ["setInterval"] });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("writes the heartbeat frame every 30 seconds with the documented literal bytes", () => {

    const { res, write } = makeReqRes();

    installSseStream(res);

    // No heartbeat at install-time - the first frame only fires after the interval elapses.
    assert.equal(write.mock.callCount(), 0);

    mock.timers.tick(HEARTBEAT_INTERVAL_MS);

    assert.equal(write.mock.callCount(), 1, "heartbeat fires after the 30s interval");
    assert.deepEqual(write.mock.calls[0]?.arguments, [HEARTBEAT_FRAME], "writes the documented event/data literal frame");

    // Cadence: subsequent ticks fire on the same period.
    mock.timers.tick(HEARTBEAT_INTERVAL_MS);

    assert.equal(write.mock.callCount(), 2, "heartbeat fires on every subsequent 30s tick");
    assert.deepEqual(write.mock.calls[1]?.arguments, [HEARTBEAT_FRAME]);
  });

  test("does not fire the heartbeat early (boundary at 29_999ms)", () => {

    // Boundary: the interval contract says "fire AT 30s." Ticking just under 30s must not produce a write. This catches a regression where a consumer changes
    // setInterval to setTimeout or starts the timer with a smaller initial delay.
    const { res, write } = makeReqRes();

    installSseStream(res);

    mock.timers.tick(HEARTBEAT_INTERVAL_MS - 1);

    assert.equal(write.mock.callCount(), 0, "no heartbeat at 29_999ms");
  });
});

describe("installSseStream - close()", () => {

  beforeEach(() => {

    mock.timers.enable({ apis: ["setInterval"] });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("clears the heartbeat interval - subsequent ticks do not produce writes", () => {

    // Regression test for the close() contract: a regression that dropped clearInterval would leak the heartbeat timer past disconnect, wasting cycles and
    // potentially writing into a torn-down socket. We register the heartbeat, advance one full interval to confirm it fires, then close, then advance another
    // interval and assert the write count is unchanged. This mirrors the eventual real-world use - a client disconnects, the route's req.on("close") handler
    // calls sse.close(), and the heartbeat must stop forever.
    const { res, write } = makeReqRes();
    const sse = installSseStream(res);

    mock.timers.tick(HEARTBEAT_INTERVAL_MS);

    assert.equal(write.mock.callCount(), 1, "first heartbeat fired");

    sse.close();

    // Advance well past two more interval boundaries. Without clearInterval, the spy would record additional writes here.
    mock.timers.tick(HEARTBEAT_INTERVAL_MS * 3);

    assert.equal(write.mock.callCount(), 1, "no further heartbeats after close()");
  });

  test("close() can be called more than once - a double-call does not throw", () => {

    // Defensive: req.on("close") may fire multiple times in pathological proxy scenarios, and a future consumer might call sse.close() defensively in addition
    // to the route's own teardown. clearInterval on an already-cleared id is a no-op in Node, so the helper is safe to call more than once for free; this test
    // asserts that contract so a future "track whether we already closed" guard isn't accidentally introduced, breaking callers.
    const { res } = makeReqRes();
    const sse = installSseStream(res);

    assert.doesNotThrow(() => {

      sse.close();
      sse.close();
    });
  });
});

describe("installSseStream - sendEvent", () => {

  test("named eventType writes 'event: <type>\\n' followed by 'data: <json>\\n\\n'", () => {

    // The named-event branch is what /streams/status uses for snapshot, streamAdded, streamRemoved, streamHealthChanged, systemStatusChanged, and channelUpdate.
    // We assert the two-write shape exactly: first the event line, then the data line with the trailing blank line that terminates the SSE frame.
    const { res, write } = makeReqRes();
    const sse = installSseStream(res);

    sse.sendEvent("snapshot", { active: 2 });

    assert.equal(write.mock.callCount(), 2, "named event produces two writes (event line + data line)");
    assert.deepEqual(write.mock.calls[0]?.arguments, ["event: snapshot\n"]);
    assert.deepEqual(write.mock.calls[1]?.arguments, ["data: {\"active\":2}\n\n"]);
  });

  test("null eventType skips the 'event:' line and writes only the data line", () => {

    // The null-eventType branch is what /logs/stream uses to forward log entries as unnamed `data:` events. The contract says: when eventType is null, do NOT
    // write an event: line; only write `data: <json>\n\n`. This is the fingerprint a regression here would leave - either an extra "event: null\n" line or a
    // dropped data line.
    const { res, write } = makeReqRes();
    const sse = installSseStream(res);

    sse.sendEvent(null, { level: "info", message: "hello" });

    assert.equal(write.mock.callCount(), 1, "null event produces a single write (data line only, no event prefix)");
    assert.deepEqual(write.mock.calls[0]?.arguments, ["data: {\"level\":\"info\",\"message\":\"hello\"}\n\n"]);
  });

  test("subsequent sendEvent calls append to the same connection (independent of prior calls)", () => {

    // Each call writes its own event without any cross-call state. We fire one named, one unnamed, and confirm the wire bytes accumulate in order.
    const { res, write } = makeReqRes();
    const sse = installSseStream(res);

    sse.sendEvent("streamAdded", { id: 1 });
    sse.sendEvent(null, { tag: "log" });

    assert.deepEqual(write.mock.calls.map((c) => c.arguments[0]), [
      "event: streamAdded\n",
      "data: {\"id\":1}\n\n",
      "data: {\"tag\":\"log\"}\n\n"
    ]);
  });

  test("serializes representative payload shapes via JSON.stringify (object, array, string, number, null)", () => {

    // The helper passes the payload through JSON.stringify verbatim. We assert the wire bytes for the shapes production actually pushes through SSE so a future
    // refactor that swaps stringification (e.g., adopts a structured-clone-based serializer) doesn't silently change client-observable bytes. We deliberately
    // do NOT cover BigInt or circular references - those throw under JSON.stringify and the production code never passes them; asserting negative behavior would
    // entrench a contract we don't promise.
    const { res, write } = makeReqRes();
    const sse = installSseStream(res);

    sse.sendEvent(null, { bar: [ 2, 3 ], foo: 1 });
    sse.sendEvent(null, [ "a", "b" ]);
    sse.sendEvent(null, "raw-string");
    sse.sendEvent(null, 42);
    sse.sendEvent(null, null);

    assert.deepEqual(write.mock.calls.map((c) => c.arguments[0]), [
      "data: {\"bar\":[2,3],\"foo\":1}\n\n",
      "data: [\"a\",\"b\"]\n\n",
      "data: \"raw-string\"\n\n",
      "data: 42\n\n",
      "data: null\n\n"
    ]);
  });

  test("undefined fields in object payloads are stripped (matches JSON.stringify semantics)", () => {

    // JSON.stringify drops undefined-valued properties from objects. We assert this so a future shape-change (e.g., wrapping the payload in a defensive normalizer)
    // doesn't silently change wire bytes for the common case of optional fields like LogEntry.categoryTag.
    const { res, write } = makeReqRes();
    const sse = installSseStream(res);

    sse.sendEvent(null, { drop: undefined, keep: "yes" });

    assert.deepEqual(write.mock.calls[0]?.arguments, ["data: {\"keep\":\"yes\"}\n\n"]);
  });

  test("undefined as the top-level payload yields 'data: undefined\\n\\n' (JSON.stringify returns undefined which coerces in string concat)", () => {

    // Edge case: JSON.stringify(undefined) returns the JS value `undefined`, which the "data: " + JSON.stringify(...) concatenation coerces to the literal
    // string "undefined". This is unspecified surface but the production code paths could in theory pass undefined; asserting current behavior as a contract
    // means a consumer relying on it knows the wire shape, and any future shift (e.g., deciding to skip the write entirely on undefined) would surface here.
    const { res, write } = makeReqRes();
    const sse = installSseStream(res);

    sse.sendEvent(null, undefined);

    assert.equal(write.mock.callCount(), 1);
    assert.deepEqual(write.mock.calls[0]?.arguments, ["data: undefined\n\n"]);
  });
});
