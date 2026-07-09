/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tabNetworkObserver.test.ts: Unit tests for the tab-wide CDP network observer. The observer's value rests on four contracts: (1) it installs Network.enable
 * and Target.setAutoAttach on the root session at construction; (2) when a child target attaches, it installs the same triple (Network.enable, setAutoAttach,
 * response listener) on the child session so OOPIF traffic flows through the same callback; (3) the consumer-supplied targetFilter predicate decides which
 * target types to observe; (4) disposal removes every listener on every tracked session and is idempotent, with both explicit dispose() and TC39 using-syntax
 * paths producing identical teardown. The fixture (FakeCdpSession, FakeConnection, makeFakeCdpPage) lives in src/testing/cdp.helpers.ts so it can be shared
 * across every test that exercises this observer or any module layered on top of it; the actual CDP wire is deferred to e2e coverage and this file isolates
 * the observer's behavior from Chrome.
 */
import type { CDPSession, Page } from "puppeteer-core";
import { FakeCdpSession, FakeConnection, closePuppeteerStreamWssOnIdle, makeFakeCdpPage, noop } from "../testing.helpers.ts";
import { describe, test } from "node:test";
import type { ObservedResponse } from "./tabNetworkObserver.ts";
import assert from "node:assert/strict";
import { observeTabResponses } from "./tabNetworkObserver.ts";

// Schedule background-server cleanup on a 0ms unref'd timer so the runner exits cleanly after the suite resolves.
closePuppeteerStreamWssOnIdle();

// CDP method names hoisted for grep-friendliness and DRY in command-list assertions.
const NETWORK_ENABLE = "Network.enable";
const NETWORK_DISABLE = "Network.disable";
const TARGET_SET_AUTO_ATTACH = "Target.setAutoAttach";

describe("observeTabResponses", () => {

  // Reusable factory for the per-test fixture - the observer always starts from a fresh root session and fresh connection so tests are isolated. Each test
  // destructures locally; tests that do not need the connection skip it.
  function buildFixture(): { connection: FakeConnection; root: FakeCdpSession } {

    const conn = new FakeConnection();
    const root = new FakeCdpSession(conn);

    return { connection: conn, root };
  }

  test("returns null when the page is already closed (no observation possible)", async () => {

    // Boundary: a closed page has no live target to attach to. The observer must bail out cleanly rather than creating a doomed session.
    const { root: rootSession } = buildFixture();

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession, true), { onResponse: noop });

    assert.equal(observer, null, "closed page short-circuits to null");
    assert.equal(rootSession.sent.length, 0, "no CDP commands issued for a closed page");
  });

  test("returns null when the root CDP session reports no connection (Connection | undefined branch)", async () => {

    // Boundary: puppeteer types CDPSession.connection() as Connection | undefined. The undefined case happens when the session is detached or constructed in a
    // state without a transport backing. The observer must bail rather than risking a TypeError on the first child attach attempt.
    const detachedRoot = new FakeCdpSession(null);
    const observer = await observeTabResponses(makeFakeCdpPage(detachedRoot), { onResponse: noop });

    assert.equal(observer, null, "no-connection branch returns null");
    assert.equal(detachedRoot.detachCalls, 1, "the orphaned session is detached during cleanup");
  });

  test("sends Target.setAutoAttach with flatten=true and Network.enable on the root session", async () => {

    // The foundational contract: the observer enables flattened auto-attach and Network observation on the root before returning. Without flatten=true, child
    // session events would not multiplex onto the parent connection and OOPIF observation would be impossible. We assert both commands fired with the expected
    // params; the exact send order is implementation detail and not asserted.
    const { root: rootSession } = buildFixture();

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: noop });

    assert.ok(observer, "observer installs successfully");

    const setAutoAttach = rootSession.sent.find((c) => c.method === TARGET_SET_AUTO_ATTACH);
    const networkEnable = rootSession.sent.find((c) => c.method === NETWORK_ENABLE);

    assert.ok(setAutoAttach, "Target.setAutoAttach sent on the root session");
    assert.ok(networkEnable, "Network.enable sent on the root session");
    assert.deepEqual(setAutoAttach.params, { autoAttach: true, flatten: true, waitForDebuggerOnStart: false }, "flatten=true and non-blocking auto-attach");

    observer.dispose();
  });

  test("forwards Network.responseReceived from the root session to the callback", async () => {

    // Happy path: the central observation contract. A response event on the root session reaches the consumer callback with the URL and headers carried through
    // unmodified, and with sessionId="" (the empty string is CDP's convention for the root session).
    const { root: rootSession } = buildFixture();

    const observed: ObservedResponse[] = [];

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: (r): void => { observed.push(r); } });

    assert.ok(observer, "observer installed");

    rootSession.emitResponse("https://example.test/asset.m3u8", { "content-type": "application/vnd.apple.mpegurl" });

    assert.equal(observed.length, 1, "exactly one response delivered");

    const first = observed[0];

    assert.ok(first, "response captured");
    assert.equal(first.url, "https://example.test/asset.m3u8", "URL forwarded verbatim");
    assert.equal(first.sessionId, "", "root session reports empty sessionId per CDP convention");
    assert.equal(first.headers["content-type"], "application/vnd.apple.mpegurl", "headers forwarded verbatim");

    observer.dispose();
  });

  test("forwards a response with missing headers as an empty record (CDP omits the headers field)", async () => {

    // Boundary: CDP sometimes omits the headers field on a responseReceived event (e.g., when the response is served from a service worker without populating
    // headers). The observer must normalize this to an empty record so consumers see a stable shape.
    const { root: rootSession } = buildFixture();

    const observed: ObservedResponse[] = [];

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: (r): void => { observed.push(r); } });

    assert.ok(observer, "observer installed");

    // Emit a response payload without a headers field. EventEmitter.emit accepts arbitrary args.
    rootSession.emit("Network.responseReceived", { response: { url: "https://example.test/noheaders.m3u8" } });

    assert.equal(observed.length, 1, "response delivered despite missing headers");

    const first = observed[0];

    assert.ok(first, "response captured");
    assert.deepEqual(first.headers, {}, "missing headers normalized to empty record");

    observer.dispose();
  });

  test("installs Network.enable and Target.setAutoAttach on a freshly-attached child session", async () => {

    // The OOPIF contract: when a child target attaches, the observer must enable Network and propagate auto-attach on the child too. Without this, the child's
    // responses and the child's own descendants would be invisible. Tests verify both commands surface in the child session's send log.
    const { connection, root: rootSession } = buildFixture();

    const childSession = new FakeCdpSession(connection);

    connection.register("child-1", childSession as unknown as CDPSession);

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: noop });

    assert.ok(observer, "observer installed");

    rootSession.emitAttached("child-1", "iframe");

    // The attach handler is async; spin the microtask queue so it completes.
    await Promise.resolve();
    await Promise.resolve();

    const childSetAutoAttach = childSession.sent.find((c) => c.method === TARGET_SET_AUTO_ATTACH);
    const childNetworkEnable = childSession.sent.find((c) => c.method === NETWORK_ENABLE);

    assert.ok(childSetAutoAttach, "child session received Target.setAutoAttach (recursive propagation)");
    assert.ok(childNetworkEnable, "child session received Network.enable");

    observer.dispose();
  });

  test("forwards a response from a child session through the same callback", async () => {

    // The whole reason this module exists: a response observed on a child target (e.g., an OOPIF) must reach the consumer without the consumer needing to know
    // about target boundaries. We attach a child session, emit a response on it, and assert the consumer sees it tagged with the child's sessionId.
    const { connection, root: rootSession } = buildFixture();

    const childSession = new FakeCdpSession(connection);

    connection.register("child-oopif", childSession as unknown as CDPSession);

    const observed: ObservedResponse[] = [];

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: (r): void => { observed.push(r); } });

    assert.ok(observer, "observer installed");

    rootSession.emitAttached("child-oopif", "iframe");

    await Promise.resolve();
    await Promise.resolve();

    childSession.emitResponse("https://oopif.test/stream.m3u8");

    assert.equal(observed.length, 1, "response from child session delivered");

    const first = observed[0];

    assert.ok(first, "response captured");
    assert.equal(first.url, "https://oopif.test/stream.m3u8", "URL forwarded");
    assert.equal(first.sessionId, "child-oopif", "child sessionId tags the delivered response");

    observer.dispose();
  });

  test("excludes the browser target type by default (no CDP commands sent on browser-typed children)", async () => {

    // The default target filter exists because the browser-level target never emits Network events relevant to a tab consumer. We emit an attach event with
    // type="browser" and assert no commands were sent to the would-be child session under the default filter.
    const { connection, root: rootSession } = buildFixture();

    const browserSession = new FakeCdpSession(connection);

    connection.register("browser-session", browserSession as unknown as CDPSession);

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: noop });

    assert.ok(observer, "observer installed");

    rootSession.emitAttached("browser-session", "browser");

    await Promise.resolve();
    await Promise.resolve();

    assert.equal(browserSession.sent.length, 0, "no CDP commands issued for an excluded target type under the default filter");

    observer.dispose();
  });

  test("honors a caller-supplied targetFilter predicate (iframe-only example)", async () => {

    // The targetFilter is the consumer's policy lever. Here we pass a predicate that observes only iframe targets, skipping workers. Two children attach (one
    // iframe, one service_worker); only the iframe's session should receive CDP commands from the observer.
    const { connection, root: rootSession } = buildFixture();

    const iframeSession = new FakeCdpSession(connection);
    const workerSession = new FakeCdpSession(connection);

    connection.register("iframe-only", iframeSession as unknown as CDPSession);
    connection.register("worker-skip", workerSession as unknown as CDPSession);

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), {

      onResponse: noop,
      targetFilter: (info): boolean => info.type === "iframe"
    });

    assert.ok(observer, "observer installed");

    rootSession.emitAttached("iframe-only", "iframe");
    rootSession.emitAttached("worker-skip", "service_worker");

    await Promise.resolve();
    await Promise.resolve();

    const iframeEnable = iframeSession.sent.find((c) => c.method === NETWORK_ENABLE);
    const workerEnable = workerSession.sent.find((c) => c.method === NETWORK_ENABLE);

    assert.ok(iframeEnable, "iframe target observed (matches predicate)");
    assert.equal(workerEnable, undefined, "worker target skipped (does not match predicate)");

    observer.dispose();
  });

  test("removes listeners from a child session on Target.detachedFromTarget", async () => {

    // Lifecycle contract: when a child target detaches, the observer drops its listeners. We verify by emitting a response on the detached session after the
    // detach event and asserting the consumer does not see it.
    const { connection, root: rootSession } = buildFixture();

    const childSession = new FakeCdpSession(connection);

    connection.register("child-detach", childSession as unknown as CDPSession);

    const observed: ObservedResponse[] = [];

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: (r): void => { observed.push(r); } });

    assert.ok(observer, "observer installed");

    rootSession.emitAttached("child-detach", "iframe");
    await Promise.resolve();
    await Promise.resolve();

    rootSession.emitDetached("child-detach");

    // Responses emitted after detach must not flow through the callback.
    childSession.emitResponse("https://detached.test/stream.m3u8");

    assert.equal(observed.length, 0, "no responses delivered after detach");

    observer.dispose();
  });

  test("dispose() removes all listeners and disables Network on every tracked session (root and children)", async () => {

    // The disposal contract: every session the observer ever attached to (root plus all children) gets its listeners cleared and Network.disable issued. The
    // session.detach() call is best-effort and unobserved by consumers; we only assert the observable side effects.
    const { connection, root: rootSession } = buildFixture();

    const childSession = new FakeCdpSession(connection);

    connection.register("child-dispose", childSession as unknown as CDPSession);

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: noop });

    assert.ok(observer, "observer installed");

    rootSession.emitAttached("child-dispose", "iframe");
    await Promise.resolve();
    await Promise.resolve();

    observer.dispose();

    const rootDisable = rootSession.sent.find((c) => c.method === NETWORK_DISABLE);
    const childDisable = childSession.sent.find((c) => c.method === NETWORK_DISABLE);

    assert.ok(rootDisable, "Network.disable sent on root session");
    assert.ok(childDisable, "Network.disable sent on child session");

    const rootRemoveAll = rootSession.listenerOps.filter((op) => op.op === "removeAll");
    const childRemoveAll = childSession.listenerOps.filter((op) => op.op === "removeAll");

    assert.ok(rootRemoveAll.length > 0, "removeAllListeners called on root session");
    assert.ok(childRemoveAll.length > 0, "removeAllListeners called on child session");
  });

  test("dispose() is idempotent - a second call is a safe no-op", async () => {

    // Boundary: cleanup paths in PrismCast often call dispose from multiple code paths (success, error, parent disposal). The observer must tolerate that without
    // double-disabling or double-detaching. We invoke dispose twice and verify the command count does not grow.
    const { root: rootSession } = buildFixture();

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: noop });

    assert.ok(observer, "observer installed");

    observer.dispose();
    const sentAfterFirstDispose = rootSession.sent.length;

    observer.dispose();
    const sentAfterSecondDispose = rootSession.sent.length;

    assert.equal(sentAfterFirstDispose, sentAfterSecondDispose, "second dispose issues no additional CDP commands");
  });

  test("the using keyword triggers disposal at scope exit (normal path)", async () => {

    // TC39 explicit resource management end-to-end test. We acquire the observer with the using keyword inside a scoped block; at scope exit, Symbol.dispose
    // must fire automatically and produce the same Network.disable as an explicit dispose() call. This validates that V8/Node's ERM machinery correctly invokes
    // our disposer rather than just confirming the alias is wired up.
    const { root: rootSession } = buildFixture();

    {

      using observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: noop });

      assert.ok(observer, "observer installed inside the using scope");
    }

    // Scope exit fires Symbol.dispose. The observable side effect is Network.disable on the root session.
    const networkDisable = rootSession.sent.find((c) => c.method === NETWORK_DISABLE);

    assert.ok(networkDisable, "using-scope exit triggers Network.disable on the root session");
  });

  test("the using keyword triggers disposal even when the scope exits via thrown exception", async () => {

    // Exception-safety contract: TC39 ERM guarantees disposal even when the scope exits via throw. This is the load-bearing reason to use Symbol.dispose at all
    // (otherwise an explicit dispose() call inside a finally block suffices). The observer must tear down on the exception path the same way it tears down on
    // the normal path.
    const { root: rootSession } = buildFixture();

    await assert.rejects(async () => {

      using observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: noop });

      assert.ok(observer, "observer installed inside the using scope");

      throw new Error("simulated failure inside the using scope");
    }, /simulated failure/);

    const networkDisable = rootSession.sent.find((c) => c.method === NETWORK_DISABLE);

    assert.ok(networkDisable, "throw-path scope exit still triggers Network.disable on the root session");
  });

  test("[Symbol.dispose] is wired and identical to dispose()", async () => {

    // Identity contract: dispose() and Symbol.dispose are the same function reference. This is what lets callers use either explicit dispose() or "using"
    // without behavioral surprises. We compare the references directly rather than just invoking both, locking the alias relationship at the type-shape level.
    const { root: rootSession } = buildFixture();

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: noop });

    assert.ok(observer, "observer installed");
    assert.equal(typeof observer[Symbol.dispose], "function", "Symbol.dispose hook present on the handle");
    assert.equal(observer[Symbol.dispose], observer.dispose, "Symbol.dispose is the same function reference as dispose");

    observer.dispose();
  });

  test("ignores responses delivered after disposal (callback not invoked post-dispose)", async () => {

    // Disposal contract: a synchronous emit() after dispose() must not reach the consumer callback. Under EventEmitter's synchronous emit semantics, the load-
    // bearing protection here is removeAllListeners (called from inside dispose) - by the time we re-emit, the listener has already been detached and the emit
    // is a no-op. The disposed-flag check inside the response handler is belt-and-suspenders for hypothetical out-of-order delivery from a non-EventEmitter
    // transport that might queue events past detachment; it is unreachable under the current fixture, but its presence costs nothing and guards against a future
    // change in the underlying emitter implementation. The HLS observer carries an equivalent disposed flag at a load-bearing location (across the chromeFetch
    // await), tested directly in hlsPlaylistObserver.test.ts.
    const { root: rootSession } = buildFixture();

    const observed: ObservedResponse[] = [];

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: (r): void => { observed.push(r); } });

    assert.ok(observer, "observer installed");

    observer.dispose();

    rootSession.emitResponse("https://ghost.test/late.m3u8");

    assert.equal(observed.length, 0, "no callback delivery after dispose");
  });

  test("returns null when page.createCDPSession() rejects (session-creation failure branch)", async () => {

    // Boundary: the third installation failure mode beyond the already-covered closed-page and no-connection branches. When createCDPSession() rejects (the target
    // vanished mid-construction, a protocol error, a browser tear-down race), the observer must catch it, log at debug, and return null rather than letting the
    // rejection escape to the caller. We build a page whose createCDPSession() rejects; if the observer failed to catch it, the await below would reject and the
    // assertion would never run, failing the test.
    const rejectingPage = {

      createCDPSession: async (): Promise<CDPSession> => {

        await Promise.resolve();

        throw new Error("session creation refused");
      },
      isClosed: (): boolean => false
    } as unknown as Page;

    const observer = await observeTabResponses(rejectingPage, { onResponse: noop });

    assert.equal(observer, null, "createCDPSession rejection short-circuits to null");
  });

  test("ignores an attachedToTarget whose sessionId cannot be resolved (child-detached-before-lookup race)", async () => {

    // Race contract: Target.attachedToTarget can fire for a child that has already detached by the time the observer looks it up via connection.session(). That
    // lookup returns null and the observer must treat the attach as a no-op - no commands issued, no throw, and the observer left fully healthy. We construct a
    // child session but deliberately do NOT register it on the connection, so the sessionId is unresolvable, then assert the orphan received zero commands and a
    // subsequent root response still flows through the callback.
    const { connection, root: rootSession } = buildFixture();

    const orphanSession = new FakeCdpSession(connection);

    const observed: ObservedResponse[] = [];

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: (r): void => { observed.push(r); } });

    assert.ok(observer, "observer installed");

    // Emit an attach for a sessionId the connection cannot resolve. onChildAttached() must bail at the null-session guard before touching any session.
    rootSession.emitAttached("orphan-unresolvable", "iframe");

    await Promise.resolve();
    await Promise.resolve();

    assert.equal(orphanSession.sent.length, 0, "no CDP commands issued for an unresolvable child session");

    // The observer must remain healthy after ignoring the race - a subsequent root response still reaches the callback.
    rootSession.emitResponse("https://root.test/after-orphan.m3u8");

    assert.equal(observed.length, 1, "observer still forwards root responses after ignoring an unresolvable attach");

    observer.dispose();
  });

  test("treats a detachedFromTarget for an untracked session as a clean no-op", async () => {

    // Lifecycle boundary: a Target.detachedFromTarget can arrive for a sessionId the observer never tracked (e.g., a target filtered out at attach time, or a
    // duplicate detach). connection.session() returns null and onChildDetached() must return early rather than calling removeAllListeners on a null session. The
    // emit is synchronous, so assert.doesNotThrow directly pins the guard: were it removed, null.removeAllListeners would throw synchronously through the emit.
    const { root: rootSession } = buildFixture();

    const observed: ObservedResponse[] = [];

    const observer = await observeTabResponses(makeFakeCdpPage(rootSession), { onResponse: (r): void => { observed.push(r); } });

    assert.ok(observer, "observer installed");

    assert.doesNotThrow((): void => { rootSession.emitDetached("never-attached"); }, "detach for an untracked session does not throw");

    // The observer must remain healthy after the no-op detach - a subsequent root response still reaches the callback.
    rootSession.emitResponse("https://root.test/after-untracked-detach.m3u8");

    assert.equal(observed.length, 1, "observer still forwards root responses after an untracked-detach no-op");

    observer.dispose();
  });
});
