/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.test.ts: Unit tests for the Chrome DevTools Protocol proxy. The proxy's contract is "translate inbound CDP wire traffic into Puppeteer CDPSession calls
 * and back" - a regression in command routing, event forwarding, lifecycle cleanup, or the Target-domain synthesis would silently break every external CDP
 * client, so we exercise each branch through the public WS-facing surface.
 *
 * Test seam. The CdpProxySession class is constructed directly with a synthetic WebSocket, Browser, CDPSession, and Connection - none of those interact with the
 * real ws library, Puppeteer, or any I/O. Each fake records what was asked of it so assertions can verify both the wire frames sent to the client and the CDP
 * commands forwarded into Puppeteer.
 */
import type { Browser, CDPSession, Connection } from "puppeteer-core";
import { afterEach, beforeEach, describe, test } from "node:test";
import { CdpProxySession } from "./cdp.ts";
import { EventEmitter } from "node:events";
import type { WebSocket } from "ws";
import assert from "node:assert/strict";
import { initDebugFilter } from "../utils/index.ts";

/* The WebSocket OPEN readyState constant. The ws library sets WebSocket.OPEN = 1; we hardcode the literal here to avoid pulling the real ws into the test harness
 * (the proxy reads readyState via a numeric comparison).
 */
const WS_OPEN = 1;

/**
 * A test double for the ws library's WebSocket. EventEmitter for the `message`/`close`/`error` handlers the proxy registers, plus the `send`/`close`/`readyState`
 * surface the proxy uses to write frames and probe state. Captured frames are appended to `sent` in send order.
 */
class FakeWebSocket extends EventEmitter {

  readyState = WS_OPEN;
  readonly sent: unknown[] = [];

  send(data: string): void {

    this.sent.push(JSON.parse(data));
  }

  close(): void {

    this.readyState = 3;
    this.emit("close");
  }

  // Helper for tests to deliver a wire frame as if the remote client sent it.
  deliver(frame: unknown): void {

    this.emit("message", Buffer.from(JSON.stringify(frame), "utf8"));
  }

  // Returns the most recent frame whose method matches the predicate.
  lastEvent(method: string): unknown {

    for(let i = this.sent.length - 1; i >= 0; i--) {

      const frame = this.sent[i] as { method?: string };

      if(frame.method === method) {

        return frame;
      }
    }

    return undefined;
  }
}

/**
 * A test double for puppeteer-core's CDPSession. EventEmitter for the events the proxy forwards (the proxy monkey-patches `emit` to intercept every CDP event;
 * the fake's `emit` therefore needs to remain swappable). Tracks every `send()` call so tests can assert on the CDP commands the proxy forwards. The `respond`
 * helper returns canned responses by method name.
 */
class FakeCdpSession extends EventEmitter {

  detached = false;
  readonly calls: { method: string; params?: unknown }[] = [];
  readonly responses = new Map<string, unknown>();

  // Pretends to be the underlying connection so the proxy can call createSession on it. Wired automatically by FakeBrowser for the browser-level session and by
  // FakeConnection.createSession for sub-sessions.
  connectionRef: FakeConnection | null = null;

  async send(method: string, params?: unknown): Promise<unknown> {

    this.calls.push({ method, params });

    const canned = this.responses.get(method);

    if(canned instanceof Error) {

      throw canned;
    }

    return canned ?? {};
  }

  connection(): Connection | undefined {

    return (this.connectionRef ?? undefined) as unknown as Connection | undefined;
  }

  async detach(): Promise<void> {

    this.detached = true;
  }
}

/**
 * A test double for puppeteer-core's Connection. `createSession()` returns a fresh FakeCdpSession whose connectionRef points back at this connection so sub-sessions
 * remain wired into the same fake graph. Tracks every createSession invocation so tests can assert how many target sessions were created. The production
 * Connection.createSession takes a TargetInfo argument; the fake ignores it because every test that calls into this path is asserting on count + wire effects,
 * not on which target was passed.
 */
class FakeConnection {

  readonly createdSessions: FakeCdpSession[] = [];

  async createSession(): Promise<CDPSession> {

    const session = new FakeCdpSession();

    session.connectionRef = this;
    this.createdSessions.push(session);

    return session as unknown as CDPSession;
  }
}

/**
 * A test double for puppeteer-core's Browser. Provides a target whose createCDPSession returns the seeded browser-level FakeCdpSession.
 */
class FakeBrowser extends EventEmitter {

  readonly browserSession: FakeCdpSession;
  readonly connection: FakeConnection;

  constructor() {

    super();

    this.connection = new FakeConnection();
    this.browserSession = new FakeCdpSession();
    this.browserSession.connectionRef = this.connection;
  }

  target(): { createCDPSession: () => Promise<CDPSession> } {

    return {

      createCDPSession: async (): Promise<CDPSession> => this.browserSession as unknown as CDPSession
    };
  }
}

/**
 * Helper: constructs a fully-wired proxy session ready for tests to drive. Returns the proxy and the fakes the test will assert against.
 */
async function makeProxy(): Promise<{ browser: FakeBrowser; proxy: CdpProxySession; ws: FakeWebSocket }> {

  const ws = new FakeWebSocket();
  const browser = new FakeBrowser();
  const proxy = new CdpProxySession(ws as unknown as WebSocket, browser as unknown as Browser);

  await proxy.start();

  return { browser, proxy, ws };
}

/* The proxy logs at info level on attach and detach. Tests don't assert on logs but the file logger may complain about a missing data directory; we suppress
 * those side effects by leaving the file logger uninitialized (which routes log calls to a buffer that drains on process exit). Each test resets the debug filter
 * so the proxy's gated forwarding behaves identically regardless of any prior debug state.
 */
beforeEach(() => {

  initDebugFilter("");
});

afterEach(() => {

  initDebugFilter("");
});

describe("CdpProxySession - browser-level command routing", () => {

  test("passes a browser-level command through to the browser session and forwards the response with the same id", async () => {

    const { browser, ws } = await makeProxy();

    // Stage the response Chrome would return for Browser.getVersion.
    browser.browserSession.responses.set("Browser.getVersion", { product: "TestChrome/1.0", protocolVersion: "1.3" });

    ws.deliver({ id: 42, method: "Browser.getVersion", params: {} });

    // Wait for the async dispatch.
    await new Promise<void>((resolve) => setImmediate(resolve));

    const call = browser.browserSession.calls.find((c) => c.method === "Browser.getVersion");

    assert.ok(call, "Browser.getVersion was forwarded to the browser session");

    const response = ws.sent.find((frame) => (frame as { id?: number }).id === 42) as { id: number; result: unknown };

    assert.ok(response, "response was sent back to the client");
    assert.deepEqual(response.result, { product: "TestChrome/1.0", protocolVersion: "1.3" });
  });

  test("forwards CDP errors back to the client as { error } frames", async () => {

    const { browser, ws } = await makeProxy();

    browser.browserSession.responses.set("Browser.getVersion", new Error("boom"));

    ws.deliver({ id: 7, method: "Browser.getVersion", params: {} });

    await new Promise<void>((resolve) => setImmediate(resolve));

    const response = ws.sent.find((frame) => (frame as { id?: number }).id === 7) as { error: { message: string }; id: number };

    assert.ok(response, "error frame was emitted");
    assert.equal(response.error.message, "boom");
  });
});

describe("CdpProxySession - Target domain synthesis", () => {

  test("Target.setDiscoverTargets enumerates existing targets and emits Target.targetCreated for each", async () => {

    const { browser, ws } = await makeProxy();
    const targetInfos = [
      { attached: false, browserContextId: "ctx", canAccessOpener: false, targetId: "t1", title: "Tab A", type: "page", url: "https://a/" },
      { attached: false, browserContextId: "ctx", canAccessOpener: false, targetId: "t2", title: "Tab B", type: "page", url: "https://b/" }
    ];

    browser.browserSession.responses.set("Target.getTargets", { targetInfos });

    ws.deliver({ id: 1, method: "Target.setDiscoverTargets", params: { discover: true } });

    await new Promise<void>((resolve) => setImmediate(resolve));

    const targetCreatedEvents = ws.sent.filter((frame) => (frame as { method?: string }).method === "Target.targetCreated");

    assert.equal(targetCreatedEvents.length, 2, "one Target.targetCreated event per existing target");
  });

  test("Target.* events are NOT forwarded to the client when discoverTargets is off", async () => {

    const { browser, ws } = await makeProxy();

    // Without calling Target.setDiscoverTargets, the client should not receive Target.targetCreated events even when the underlying session emits them.
    browser.browserSession.emit("Target.targetCreated", { targetInfo: { targetId: "t1", type: "page", url: "https://example/" } });

    const targetEvents = ws.sent.filter((frame) => {

      const method = (frame as { method?: string }).method;

      return (typeof method === "string") && method.startsWith("Target.");
    });

    assert.equal(targetEvents.length, 0, "no Target.* events leaked to the client");
  });

  test("Target.setAutoAttach creates a sub-session per existing target and emits Target.attachedToTarget for each", async () => {

    const { browser, ws } = await makeProxy();
    const targetInfos = [
      { attached: false, browserContextId: "ctx", canAccessOpener: false, targetId: "t1", title: "Tab A", type: "page", url: "https://a/" },
      { attached: false, browserContextId: "ctx", canAccessOpener: false, targetId: "t2", title: "Tab B", type: "page", url: "https://b/" }
    ];

    browser.browserSession.responses.set("Target.getTargets", { targetInfos });

    ws.deliver({ id: 1, method: "Target.setAutoAttach", params: { autoAttach: true, flatten: true } });

    await new Promise<void>((resolve) => setImmediate(resolve));

    const attached = ws.sent.filter((frame) => (frame as { method?: string }).method === "Target.attachedToTarget");

    assert.equal(attached.length, 2, "one Target.attachedToTarget per existing target");
    assert.equal(browser.connection.createdSessions.length, 2, "two sub-sessions created via Connection.createSession");

    const sessionIds = attached.map((frame) => (frame as { params: { sessionId: string } }).params.sessionId);

    assert.equal(new Set(sessionIds).size, 2, "each attached event carries a unique sessionId");
  });

  test("Target.attachToTarget routes through getTargetInfo and returns the synthetic sessionId", async () => {

    const { browser, ws } = await makeProxy();
    const targetInfo = { attached: false, browserContextId: "ctx", canAccessOpener: false, targetId: "tX", title: "Page", type: "page", url: "https://x/" };

    browser.browserSession.responses.set("Target.getTargetInfo", { targetInfo });

    ws.deliver({ id: 9, method: "Target.attachToTarget", params: { flatten: true, targetId: "tX" } });

    await new Promise<void>((resolve) => setImmediate(resolve));

    const response = ws.sent.find((frame) => (frame as { id?: number }).id === 9) as { id: number; result: { sessionId: string } };

    assert.ok(response, "response was sent");
    assert.ok(typeof response.result.sessionId === "string", "response contains a sessionId");
    assert.equal(browser.connection.createdSessions.length, 1, "exactly one sub-session was created");
  });
});

describe("CdpProxySession - sub-session routing", () => {

  test("a command with sessionId routes to the right sub-session", async () => {

    const { browser, ws } = await makeProxy();
    const targetInfo = { attached: false, browserContextId: "ctx", canAccessOpener: false, targetId: "tA", title: "Page", type: "page", url: "https://a/" };

    browser.browserSession.responses.set("Target.getTargetInfo", { targetInfo });

    ws.deliver({ id: 1, method: "Target.attachToTarget", params: { flatten: true, targetId: "tA" } });
    await new Promise<void>((resolve) => setImmediate(resolve));

    const attachResponse = ws.sent.find((frame) => (frame as { id?: number }).id === 1) as { result: { sessionId: string } };
    const sessionId = attachResponse.result.sessionId;
    const subSession = browser.connection.createdSessions[0];

    assert.ok(subSession);

    subSession.responses.set("Runtime.evaluate", { result: { type: "string", value: "hello" } });

    ws.deliver({ id: 2, method: "Runtime.evaluate", params: { expression: "1" }, sessionId });
    await new Promise<void>((resolve) => setImmediate(resolve));

    const evalResponse = ws.sent.find((frame) => (frame as { id?: number }).id === 2) as { id: number; result: unknown; sessionId: string };

    assert.equal(evalResponse.sessionId, sessionId, "response echoes the client's sessionId");
    assert.deepEqual(evalResponse.result, { result: { type: "string", value: "hello" } });
    assert.equal(subSession.calls.length, 1, "sub-session received the command");
  });

  test("a command targeting an unknown sessionId returns an error frame", async () => {

    const { ws } = await makeProxy();

    ws.deliver({ id: 99, method: "Runtime.evaluate", params: { expression: "1" }, sessionId: "nonexistent" });
    await new Promise<void>((resolve) => setImmediate(resolve));

    const response = ws.sent.find((frame) => (frame as { id?: number }).id === 99) as { error: { message: string }; id: number };

    assert.ok(response.error, "error frame returned");
    assert.match(response.error.message, /session not attached/);
  });
});

describe("CdpProxySession - event forwarding", () => {

  test("a CDP-domain event emitted by the browser session is forwarded to the client", async () => {

    const { browser, ws } = await makeProxy();

    browser.browserSession.emit("Network.requestWillBeSent", { request: { url: "https://example/" }, requestId: "r1" });

    const event = ws.lastEvent("Network.requestWillBeSent") as { method: string; params: unknown };

    assert.ok(event, "Network.requestWillBeSent was forwarded");
    assert.deepEqual(event.params, { request: { url: "https://example/" }, requestId: "r1" });
  });

  test("an event emitted by a sub-session is forwarded with the corresponding sessionId", async () => {

    const { browser, ws } = await makeProxy();
    const targetInfo = { attached: false, browserContextId: "ctx", canAccessOpener: false, targetId: "tA", title: "Page", type: "page", url: "https://a/" };

    browser.browserSession.responses.set("Target.getTargetInfo", { targetInfo });

    ws.deliver({ id: 1, method: "Target.attachToTarget", params: { flatten: true, targetId: "tA" } });
    await new Promise<void>((resolve) => setImmediate(resolve));

    const attachResponse = ws.sent.find((frame) => (frame as { id?: number }).id === 1) as { result: { sessionId: string } };
    const sessionId = attachResponse.result.sessionId;
    const subSession = browser.connection.createdSessions[0];

    assert.ok(subSession);

    subSession.emit("Page.loadEventFired", { timestamp: 1234 });

    const event = ws.lastEvent("Page.loadEventFired") as { method: string; params: unknown; sessionId: string };

    assert.ok(event, "Page.loadEventFired was forwarded");
    assert.equal(event.sessionId, sessionId, "event carries the right sessionId");
  });

  test("non-CDP events (no dot-named domain) are not forwarded", async () => {

    const { browser, ws } = await makeProxy();

    browser.browserSession.emit("sessiondetached", {});
    browser.browserSession.emit("disconnect", {});

    const forwarded = ws.sent.filter((frame) => {

      const method = (frame as { method?: string }).method;

      return (method === "sessiondetached") || (method === "disconnect");
    });

    assert.equal(forwarded.length, 0, "lifecycle events were not forwarded as CDP events");
  });
});

describe("CdpProxySession - lifecycle", () => {

  test("Target.targetDestroyed detaches matching sub-sessions and emits Target.detachedFromTarget when discoverTargets is on", async () => {

    const { browser, ws } = await makeProxy();
    const targetInfo = { attached: false, browserContextId: "ctx", canAccessOpener: false, targetId: "tA", title: "Page", type: "page", url: "https://a/" };

    browser.browserSession.responses.set("Target.getTargets", { targetInfos: [targetInfo] });

    // Enable discovery so the client will receive Target.* events.
    ws.deliver({ id: 1, method: "Target.setDiscoverTargets", params: { discover: true } });
    await new Promise<void>((resolve) => setImmediate(resolve));

    browser.browserSession.responses.set("Target.getTargetInfo", { targetInfo });

    ws.deliver({ id: 2, method: "Target.attachToTarget", params: { flatten: true, targetId: "tA" } });
    await new Promise<void>((resolve) => setImmediate(resolve));

    const subSession = browser.connection.createdSessions[0];

    assert.ok(subSession);

    browser.browserSession.emit("Target.targetDestroyed", { targetId: "tA" });
    await new Promise<void>((resolve) => setImmediate(resolve));

    const detached = ws.lastEvent("Target.detachedFromTarget") as { method: string; params: { sessionId: string; targetId: string } };

    assert.ok(detached, "Target.detachedFromTarget emitted");
    assert.equal(detached.params.targetId, "tA");
    assert.equal(subSession.detached, true, "sub-session was detached");
  });

  test("WS close detaches every sub-session and the browser session", async () => {

    const { browser, ws } = await makeProxy();
    const targetInfo = { attached: false, browserContextId: "ctx", canAccessOpener: false, targetId: "tA", title: "Page", type: "page", url: "https://a/" };

    browser.browserSession.responses.set("Target.getTargetInfo", { targetInfo });

    ws.deliver({ id: 1, method: "Target.attachToTarget", params: { flatten: true, targetId: "tA" } });
    await new Promise<void>((resolve) => setImmediate(resolve));

    const subSession = browser.connection.createdSessions[0];

    assert.ok(subSession);

    ws.close();

    // Let async cleanup settle.
    await new Promise<void>((resolve) => setImmediate(resolve));

    assert.equal(subSession.detached, true, "sub-session was detached on WS close");
    assert.equal(browser.browserSession.detached, true, "browser session was detached on WS close");
  });

  test("browser 'disconnected' triggers session teardown (per-session lifecycle ownership)", async () => {

    // The session subscribes to its own browser's "disconnected" event in start() and unsubscribes in cleanup(). This pins the contract that a disconnected
    // browser tears down its attached session without any module-level broadcast or shared registry.
    const { browser, ws } = await makeProxy();

    // Sanity: while attached, the WS has not received a close. The fake captures sent frames; close is observed via readyState transition (set by ws.close()).
    assert.notEqual(ws.readyState, 3, "WS open before disconnect");
    assert.equal(browser.listenerCount("disconnected"), 1, "session subscribed exactly one disconnect listener");

    browser.emit("disconnected");

    // Let the WS close handler and async cleanup settle.
    await new Promise<void>((resolve) => setImmediate(resolve));

    assert.equal(ws.readyState, 3, "WS closed when its browser disconnected");
    assert.equal(browser.browserSession.detached, true, "browser-level CDPSession detached during cleanup");
    assert.equal(browser.listenerCount("disconnected"), 0, "session unsubscribed its disconnect listener on cleanup");
  });

  test("subsequent browser 'disconnected' events after cleanup do not throw or re-trigger close", async () => {

    // After cleanup() runs, the session must be fully detached from its browser. A late "disconnected" arrival (e.g., a second emit by Puppeteer during teardown)
    // must be a structural no-op - otherwise we have re-entry into an already-cleaned session, which is the exact class of bug per-session ownership eliminates.
    const { browser, ws } = await makeProxy();

    ws.close();
    await new Promise<void>((resolve) => setImmediate(resolve));

    assert.equal(browser.listenerCount("disconnected"), 0, "no disconnect listener after cleanup");

    // Emitting again must not throw and must not re-close (already closed) or otherwise mutate state.
    assert.doesNotThrow(() => browser.emit("disconnected"));
    assert.equal(ws.readyState, 3, "WS stays closed");
  });
});
