/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.helpers.ts: Shared structural stubs for puppeteer's CDP surface used by every test that exercises browser/tabNetworkObserver.ts or any module layered on
 * top of it (browser/hlsPlaylistObserver.ts, browser/manifestInterceptor.ts).
 */
import type { CDPSession, Page } from "puppeteer-core";
import { EventEmitter } from "node:events";

/* Why this module exists. The puppeteer surface our observers depend on - CDPSession.send/detach/on/off/emit/removeAllListeners/connection() and the
 * Connection.session(sessionId) lookup - is the same regardless of which observer is being tested. Before this module, every test file that exercised the
 * observer stack declared its own FakeCdpSession + FakeConnection + makeFakePage trio with near-identical bodies, inviting drift each time puppeteer's
 * surface changed. Centralizing the stubs here is the single source of truth: a future puppeteer-rename or addition is one edit, not N.
 *
 * What's stubbed.
 *
 *   - FakeCdpSession extends EventEmitter and adds: a record of every send(method, params) call (sent), a detach() call counter, a per-event listener-op
 *     spy (listenerOps) that records every on/off/removeAllListeners invocation the observer makes, a connection() method returning the constructor arg
 *     (mirroring puppeteer's nullable Connection | undefined contract), and named emit helpers for the three CDP events the observers consume
 *     (Network.responseReceived, Target.attachedToTarget, Target.detachedFromTarget).
 *   - FakeConnection holds a register/session map so tests can wire up child CDPSessions and have them resolved by sessionId in the way puppeteer's flatten
 *     mode does.
 *   - makeFakeCdpPage returns a Page-shaped object whose createCDPSession resolves with the supplied root session and whose isClosed() reflects the closed
 *     constructor flag - the minimum surface our observers touch on a Page.
 *
 * The classes carry the full feature set (listener-op spying, null-connection support, all three emit helpers); consumers that don't need a given field simply
 * never read it. This keeps the stub as one shape, avoiding the "two near-identical fixtures" pattern that drove the extraction.
 */

/**
 * One captured CDP send() invocation. Tests inspect this list to assert that the observer issued the expected commands (Target.setAutoAttach, Network.enable,
 * Network.disable). The method name is the CDP method string; params is whatever the observer passed.
 */
export interface CapturedCdpCommand {

  // CDP method name (e.g., "Network.enable", "Target.setAutoAttach").
  method: string;

  // Whatever params object the observer passed; opaque to the stub.
  params: unknown;
}

/**
 * One listener-registration operation observed on a FakeCdpSession. Tests assert that disposal removes every listener the observer installed, which the
 * tabNetworkObserver tests verify by inspecting the recorded "removeAll" entries.
 */
export interface CdpSessionListenerOp {

  // Event name passed to on / off / removeAllListeners. The literal "*" is used for the all-events form of removeAllListeners().
  event: string;

  // Which operation: a listener was added (on), removed (off), or all-removed (removeAll).
  op: "on" | "off" | "removeAll";
}

/**
 * Structural stub of puppeteer's CDPSession. Carries the public surface the observers use - send(), detach(), connection(), and the EventEmitter contract
 * inherited from node:events. The constructor accepts a FakeConnection or null so tests can exercise both the connected-session happy path and the puppeteer
 * "no connection (session detached)" branch.
 *
 * The class spies on on/off/removeAllListeners through a wrap-at-construction technique: it captures the EventEmitter's original methods bound to this and
 * replaces the public methods with wrappers that record the operation into listenerOps before delegating. This lets tests assert disposal removed every
 * listener the observer installed without relying on EventEmitter internals.
 */
export class FakeCdpSession extends EventEmitter {

  // Counter incremented on each detach() call. The observer's disposal path calls detach in best-effort fire-and-forget mode; tests assert this counter
  // equals the expected number of detach calls (typically 1 for normal disposal).
  public detachCalls = 0;

  // Append-only listener-operation log. The observer's disposal path is expected to call removeAllListeners on every event it subscribed to; tests inspect
  // this list to confirm those operations happened.
  public listenerOps: CdpSessionListenerOp[] = [];

  // Append-only record of every send() call. Tests inspect this to verify the observer issued the expected CDP commands.
  public readonly sent: CapturedCdpCommand[] = [];

  // Private connection reference. Exposed via the connection() method below, matching puppeteer's instance-method shape rather than a public field.
  readonly #connection: FakeConnection | null;

  public constructor(connection: FakeConnection | null) {

    super();

    this.#connection = connection;

    // Spy on add/remove operations so tests can assert that disposal removes every listener installed by the observer. The wrappers preserve the original
    // EventEmitter behavior - they just record the operation before delegating to the bound originals.
    const originalOn = this.on.bind(this);
    const originalOff = this.off.bind(this);
    const originalRemoveAll = this.removeAllListeners.bind(this);

    this.on = (event: string | symbol, handler: (...args: unknown[]) => void): this => {

      this.listenerOps.push({ event: String(event), op: "on" });

      return originalOn(event, handler);
    };

    this.off = (event: string | symbol, handler: (...args: unknown[]) => void): this => {

      this.listenerOps.push({ event: String(event), op: "off" });

      return originalOff(event, handler);
    };

    this.removeAllListeners = (event?: string | symbol): this => {

      this.listenerOps.push({ event: String(event ?? "*"), op: "removeAll" });

      return originalRemoveAll(event);
    };
  }

  // CDP send() - records the call and returns undefined. Production observers never inspect the resolved value of send() other than for awaitable completion;
  // the stub matches that contract via `async` + `await Promise.resolve()` so the microtask boundary at the call site matches puppeteer's real send() (which
  // awaits a wire roundtrip) rather than collapsing it into a synchronous resolution.
  public async send(method: string, params?: unknown): Promise<unknown> {

    this.sent.push({ method, params });

    await Promise.resolve();

    return undefined;
  }

  // CDP detach() - records the call via detachCalls and resolves. The observer's dispose path treats detach failures as benign, so the stub never rejects.
  // Same async-with-microtask pattern as send() above for parity with puppeteer's real detach().
  public async detach(): Promise<void> {

    this.detachCalls++;

    await Promise.resolve();
  }

  // CDP connection() - returns the FakeConnection passed at construction, or null when constructed without one. Mirrors puppeteer's Connection | undefined
  // contract so the observer's null-branch is exercised when the test constructs the session with null.
  public connection(): FakeConnection | null {

    return this.#connection;
  }

  /* Named emit helpers - tests use these instead of raw emit() so the event payload shape is centralized here, not duplicated at every emit site. The payloads
   * match the structural shapes the observers parse out of the CDP protocol. */

  // Emits a Network.responseReceived event with the supplied URL and optional headers. Headers default to an empty object, matching CDP's behavior when the
  // response carries no headers.
  public emitResponse(url: string, headers?: Record<string, string>): void {

    this.emit("Network.responseReceived", { response: { headers: headers ?? {}, url } });
  }

  // Emits a Target.attachedToTarget event for a child target with the supplied sessionId and CDP target type ("page", "iframe", "service_worker", "worker",
  // "shared_worker", "browser", etc.). The targetId is derived deterministically from the sessionId so tests do not need to construct it manually.
  public emitAttached(sessionId: string, type: string): void {

    this.emit("Target.attachedToTarget", { sessionId, targetInfo: { targetId: "target-" + sessionId, type }, waitingForDebugger: false });
  }

  // Emits a Target.detachedFromTarget event for the supplied sessionId. The observer responds by removing its listeners from the corresponding child session.
  public emitDetached(sessionId: string): void {

    this.emit("Target.detachedFromTarget", { sessionId });
  }
}

/**
 * Structural stub of puppeteer's Connection. Provides the session(sessionId) lookup the observer uses to resolve a freshly-attached child target's sessionId
 * into a CDPSession. Tests register child sessions via register() before emitting the corresponding Target.attachedToTarget event so the lookup succeeds.
 */
export class FakeConnection {

  // Private sessionId -> CDPSession map. Tests populate via register() before driving the observer.
  readonly #sessions = new Map<string, CDPSession>();

  // Registers a child session under the given sessionId. The observer will resolve the child via session() after receiving Target.attachedToTarget.
  public register(sessionId: string, session: CDPSession): void {

    this.#sessions.set(sessionId, session);
  }

  // Returns the CDPSession registered for the supplied sessionId, or null if none is registered. Matches puppeteer's Connection.session(sessionId) shape.
  public session(sessionId: string): CDPSession | null {

    return this.#sessions.get(sessionId) ?? null;
  }
}

/**
 * Returns a Page-shaped object whose createCDPSession() resolves with the supplied root FakeCdpSession and whose isClosed() returns the supplied closed flag.
 * This is the minimum surface the observers touch on a Page; everything else is unused and intentionally absent.
 *
 * @param rootSession - The FakeCdpSession returned by createCDPSession().
 * @param closed - Whether isClosed() should report true. Defaults to false. Tests exercising the closed-page branch pass true.
 * @returns A Page-shaped object suitable for passing to observers under test.
 */
export function makeFakeCdpPage(rootSession: FakeCdpSession, closed = false): Page {

  return {

    // Declared async with an await for parity with puppeteer's real createCDPSession() (which awaits a wire roundtrip). Collapsing to a synchronous Promise
    // would hide a microtask boundary that production code path crosses, which can mask order-of-operations bugs in tests.
    createCDPSession: async (): Promise<CDPSession> => {

      await Promise.resolve();

      return rootSession as unknown as CDPSession;
    },
    isClosed: (): boolean => closed
  } as unknown as Page;
}
