/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.helpers.test.ts: Unit tests for the shared puppeteer CDP stubs. The fixtures themselves carry no production logic - their value rests on three contracts
 * we lock here: (1) FakeCdpSession records send/detach calls and the listener add/remove operations the observer issues, (2) FakeConnection's register/session
 * roundtrip behaves like puppeteer's flatten-mode session lookup, (3) makeFakeCdpPage returns a Page whose createCDPSession and isClosed methods reflect the
 * constructor arguments. Locking these contracts here prevents drift surfacing as inscrutable failures inside the observer test files that consume the stubs.
 */
import { FakeCdpSession, FakeConnection, makeFakeCdpPage } from "./cdp.helpers.ts";
import { describe, test } from "node:test";
import type { CDPSession } from "puppeteer-core";
import type { CapturedCdpCommand } from "./cdp.helpers.ts";
import assert from "node:assert/strict";

describe("FakeCdpSession", () => {

  test("records every send() call with method and params", async () => {

    // Contract: the sent array is the canonical observation channel for "what commands did the code under test issue?". Every send must surface here in
    // order, with the exact method name and params value the caller passed.
    const session = new FakeCdpSession(null);

    await session.send("Network.enable");
    await session.send("Target.setAutoAttach", { autoAttach: true, flatten: true });

    assert.equal(session.sent.length, 2, "two send calls recorded");

    const first = session.sent[0];
    const second = session.sent[1];

    assert.ok(first, "first call captured");
    assert.ok(second, "second call captured");
    assert.equal(first.method, "Network.enable", "first method recorded verbatim");
    assert.equal(first.params, undefined, "params absent when caller passes nothing");
    assert.equal(second.method, "Target.setAutoAttach", "second method recorded verbatim");
    assert.deepEqual(second.params, { autoAttach: true, flatten: true }, "params object forwarded verbatim");
  });

  test("send() resolves with undefined", async () => {

    // Contract: production observers await send() for completion, not for a return value. The stub matches by resolving undefined; tests that depend on a
    // non-undefined return would be relying on something the production contract does not provide.
    const session = new FakeCdpSession(null);
    const result = await session.send("Browser.getVersion");

    assert.equal(result, undefined, "send resolves with undefined");
  });

  test("detach() increments detachCalls", async () => {

    // Contract: the observer's disposal path calls detach() in fire-and-forget mode. The counter is how tests verify the call happened (and how many times -
    // typically once per session for normal disposal).
    const session = new FakeCdpSession(null);

    assert.equal(session.detachCalls, 0, "starts at zero");

    await session.detach();
    assert.equal(session.detachCalls, 1, "one detach recorded");

    await session.detach();
    assert.equal(session.detachCalls, 2, "second detach recorded (counter is monotonic)");
  });

  test("connection() returns the FakeConnection passed at construction", () => {

    // Contract: connection() mirrors puppeteer's CDPSession.connection() shape. When a FakeConnection is supplied, the stub returns that same instance so the
    // observer's Connection.session(sessionId) lookup works against the same map the test populated.
    const conn = new FakeConnection();
    const session = new FakeCdpSession(conn);

    assert.equal(session.connection(), conn, "connection identity preserved");
  });

  test("connection() returns null when constructed without one (Connection | undefined branch)", () => {

    // Contract: the observer's null-connection branch (puppeteer's CDPSession.connection(): Connection | undefined) is exercised when the stub returns null.
    // Without this branch, tests cannot drive the observer's no-connection cleanup path.
    const session = new FakeCdpSession(null);

    assert.equal(session.connection(), null, "no-connection case returns null");
  });

  test("listenerOps records on() calls with the event name", () => {

    // Contract: the observer installs listeners; tests assert that disposal removes them. The listenerOps log is the observation channel for both halves of
    // that contract. on() must record with op="on" and the event name as a plain string.
    const session = new FakeCdpSession(null);

    session.on("Network.responseReceived", () => undefined);
    session.on("Target.attachedToTarget", () => undefined);

    const onOps = session.listenerOps.filter((op) => op.op === "on");

    assert.equal(onOps.length, 2, "two on() calls recorded");
    assert.deepEqual(onOps.map((op) => op.event), [ "Network.responseReceived", "Target.attachedToTarget" ], "events recorded in registration order");
  });

  test("listenerOps records removeAllListeners() with the event name when one is provided", () => {

    // Contract: the observer's disposal calls removeAllListeners("Network.responseReceived") etc. The log captures the event name so tests can verify each
    // expected event was cleared. The shorthand "*" applies only when called without arguments (tested separately below).
    const session = new FakeCdpSession(null);

    session.removeAllListeners("Network.responseReceived");

    const removeOps = session.listenerOps.filter((op) => op.op === "removeAll");

    assert.equal(removeOps.length, 1, "one removeAll call recorded");
    assert.equal(removeOps[0]?.event, "Network.responseReceived", "event recorded verbatim");
  });

  test("listenerOps records removeAllListeners() with no argument as '*'", () => {

    // Boundary: EventEmitter.removeAllListeners() with no argument removes listeners for every event. The log uses "*" as the sentinel for this all-events
    // form so test assertions can distinguish a per-event clear from a sweep.
    const session = new FakeCdpSession(null);

    session.removeAllListeners();

    const removeOps = session.listenerOps.filter((op) => op.op === "removeAll");

    assert.equal(removeOps[0]?.event, "*", "no-argument call recorded as '*'");
  });

  test("emitResponse() dispatches a Network.responseReceived event with the supplied URL", () => {

    // Contract: emitResponse is the test-friendly helper that wraps the EventEmitter primitive with the exact payload shape the observer expects to parse out
    // of CDP. The URL surfaces inside params.response.url; headers default to an empty object when the test does not pass them.
    const session = new FakeCdpSession(null);
    const captured: { url: string; headers: Record<string, string> }[] = [];

    session.on("Network.responseReceived", (params: { response: { url: string; headers: Record<string, string> } }) => {

      captured.push({ headers: params.response.headers, url: params.response.url });
    });

    session.emitResponse("https://example.test/asset.m3u8");

    assert.equal(captured.length, 1, "one event delivered");

    const first = captured[0];

    assert.ok(first, "event captured");
    assert.equal(first.url, "https://example.test/asset.m3u8", "URL forwarded verbatim");
    assert.deepEqual(first.headers, {}, "headers default to empty record");
  });

  test("emitResponse() forwards the supplied headers when provided", () => {

    // Contract: when headers are explicitly passed, they reach the consumer unchanged. The observer's normalize-missing-headers path is separate; this stub
    // honors whatever the test passes.
    const session = new FakeCdpSession(null);
    const captured: { headers: Record<string, string> }[] = [];

    session.on("Network.responseReceived", (params: { response: { headers: Record<string, string> } }) => {

      captured.push({ headers: params.response.headers });
    });

    session.emitResponse("https://example.test/asset.m3u8", { "content-type": "application/vnd.apple.mpegurl" });

    assert.equal(captured[0]?.headers["content-type"], "application/vnd.apple.mpegurl", "headers forwarded verbatim");
  });

  test("emitAttached() dispatches a Target.attachedToTarget event with the supplied sessionId and type", () => {

    // Contract: emitAttached is the canonical way for tests to drive the observer's child-attach handler. The payload includes a deterministic targetId derived
    // from the sessionId so tests do not need to invent one.
    const session = new FakeCdpSession(null);
    const captured: { sessionId: string; targetInfo: { type: string; targetId: string } }[] = [];

    session.on("Target.attachedToTarget", (params: { sessionId: string; targetInfo: { type: string; targetId: string } }) => {

      captured.push(params);
    });

    session.emitAttached("child-1", "iframe");

    const first = captured[0];

    assert.ok(first, "event captured");
    assert.equal(first.sessionId, "child-1", "sessionId forwarded");
    assert.equal(first.targetInfo.type, "iframe", "target type forwarded");
    assert.equal(first.targetInfo.targetId, "target-child-1", "targetId derived from sessionId");
  });

  test("emitDetached() dispatches a Target.detachedFromTarget event with the supplied sessionId", () => {

    // Contract: emitDetached drives the observer's detach handler. The payload carries only the sessionId; the observer is responsible for resolving it
    // back to a session via the connection.
    const session = new FakeCdpSession(null);
    const captured: { sessionId: string }[] = [];

    session.on("Target.detachedFromTarget", (params: { sessionId: string }) => {

      captured.push(params);
    });

    session.emitDetached("child-1");

    assert.equal(captured[0]?.sessionId, "child-1", "sessionId forwarded");
  });
});

describe("FakeConnection", () => {

  test("session() returns the FakeCdpSession registered under the given sessionId", () => {

    // Contract: the observer calls connection.session(sessionId) to resolve a freshly-attached child target's sessionId into a CDPSession. The stub mirrors
    // puppeteer's flatten-mode lookup behavior.
    const conn = new FakeConnection();
    const child = new FakeCdpSession(conn);

    conn.register("child-1", child as unknown as CDPSession);

    assert.equal(conn.session("child-1"), child as unknown as CDPSession, "registered session resolves by sessionId");
  });

  test("session() returns null for an unregistered sessionId", () => {

    // Contract: puppeteer's Connection.session(sessionId) returns null when the sessionId is not in its multiplex table. The stub matches so the observer's
    // race-condition handling (child detached between attach event and our lookup) is exercised when the test does not register the child.
    const conn = new FakeConnection();

    assert.equal(conn.session("nonexistent"), null, "unregistered sessionId resolves to null");
  });

  test("register() overwrites a prior entry for the same sessionId", () => {

    // Boundary: sessionIds are unique per connection in puppeteer, but tests may want to swap which session a sessionId resolves to. The stub honors the last
    // write, matching map semantics.
    const conn = new FakeConnection();
    const first = new FakeCdpSession(null);
    const second = new FakeCdpSession(null);

    conn.register("child-1", first as unknown as CDPSession);
    conn.register("child-1", second as unknown as CDPSession);

    assert.equal(conn.session("child-1"), second as unknown as CDPSession, "last write wins");
  });
});

describe("makeFakeCdpPage", () => {

  test("createCDPSession() resolves with the supplied root session", async () => {

    // Contract: the observer calls page.createCDPSession() to obtain its root session. The stub returns whatever the test passed in, so observer behavior is
    // driven by the test's session not by a separately-constructed default.
    const root = new FakeCdpSession(null);
    const page = makeFakeCdpPage(root);
    const session = await page.createCDPSession();

    assert.equal(session, root as unknown as CDPSession, "supplied root session returned");
  });

  test("isClosed() returns false by default", () => {

    // Boundary: the closed flag defaults to false, matching the typical fresh-page state. Tests that exercise the closed-page early-exit branch must opt in.
    const root = new FakeCdpSession(null);
    const page = makeFakeCdpPage(root);

    assert.equal(page.isClosed(), false, "default isClosed is false");
  });

  test("isClosed() returns true when the closed flag is set", () => {

    // Contract: tests exercising the observer's closed-page bail-out pass closed=true. The stub honors the flag.
    const root = new FakeCdpSession(null);
    const page = makeFakeCdpPage(root, true);

    assert.equal(page.isClosed(), true, "closed flag honored");
  });
});

describe("CapturedCdpCommand type", () => {

  test("is structurally satisfied by an inline object literal", () => {

    // Type-shape check: external tests construct CapturedCdpCommand-typed values when filtering session.sent. The structural assignability is the contract -
    // not a class, not a brand. This test exists to surface accidental tightening of the type (e.g., adding required fields) at the unit tier.
    const command: CapturedCdpCommand = { method: "Network.enable", params: undefined };

    assert.equal(command.method, "Network.enable");
    assert.equal(command.params, undefined);
  });
});
