/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cdp.ts: Chrome DevTools Protocol proxy for PrismCast.
 *
 * PrismCast launches Chrome with --remote-debugging-pipe so puppeteer-stream can load the capture extension, which means Chrome's standard --remote-debugging-port
 * CDP surface is not available. This module fills the gap by exposing a CDP-compatible HTTP and WebSocket surface at /cdp that translates external CDP traffic
 * into Puppeteer CDPSession calls. The result: chrome://inspect, puppeteer.connect({ browserURL }), and chrome-remote-interface all attach to the running browser
 * as if Chrome had been launched with a debugging port, without disturbing PrismCast's own pipe-based session.
 *
 * Surfaces exposed (all gated on isCategoryEnabled("cdp") at request time, so they appear and disappear in sync with the cdp toggle in /debug):
 *
 *   GET  /cdp/json/version         Browser metadata + webSocketDebuggerUrl for the browser-level WS.
 *   GET  /cdp/json                 Target list, one entry per attachable target.
 *   GET  /cdp/json/list            Alias of /cdp/json (matches the Chrome convention).
 *   WS   /cdp/devtools/browser/X   Browser-level CDP with flat multiplexing.
 *
 * Multiplexing model. Each WS connection owns one browser-level Puppeteer CDPSession (`browserSession`) and a Map<syntheticSessionId, CDPSession> for the
 * per-target sub-sessions the client attaches. We synthesize the Target domain locally on top of Puppeteer's Browser/Connection API (intercepting
 * Target.setDiscoverTargets, Target.setAutoAttach, Target.attachToTarget, Target.detachFromTarget, Target.getTargets, Target.getTargetInfo) and pass every other
 * command through to the appropriate session. Events from each session are forwarded to the WS with the corresponding sessionId via an emit-monkey-patch
 * (capturing the dot-named CDP events without coupling to any private Puppeteer API). The synthetic sessionIds are opaque to clients - they use whatever we hand
 * back from Target.attachToTarget / Target.attachedToTarget.
 *
 * Lifecycle. WS close detaches every CDPSession we created and restores the patched emits. The CDP `Target.targetDestroyed` event observed on our browser session
 * emits a synthetic `Target.detachedFromTarget` to the client and removes the corresponding entry from the session map. Puppeteer's `disconnected` event closes
 * every active WS with code 1001 (going away) so external clients see a clean teardown when the browser crashes or is closed.
 */
import type { Browser, CDPSession, Connection } from "puppeteer-core";
import type { Express, Request, Response } from "express";
import type { Server as HttpServer, IncomingMessage } from "node:http";
import { LOG, formatError, getPackageVersion, isCategoryEnabled } from "../utils/index.ts";
import { WebSocket, WebSocketServer } from "ws";
import type { Protocol } from "devtools-protocol";
import type { Socket } from "node:net";
import { getBrowserInstance } from "../browser/index.ts";

/* Module-level state. The WebSocketServer is created in `noServer` mode so the HTTP server's upgrade event remains the single dispatch point; we route
 * /cdp/devtools/* upgrades into the WSS and leave any other upgrade requests untouched so future features can claim them.
 *
 * Lifecycle ownership lives one layer down: each CdpProxySession subscribes to its own browser's `disconnected` event in start() and unsubscribes in cleanup().
 * That keeps ownership per-session rather than as a module-level "track the current browser, broadcast over a Set of active sessions" scheme, which would encode
 * a per-instance concern (this WS attached to that browser) as shared state and race when listener registration outran rotation. Discovery endpoints likewise hold no
 * disconnect subscription: the discoverySessions WeakMap is keyed by Browser identity, so rotation invalidates structurally and the old entry GCs with its
 * browser.
 */
let wss: WebSocketServer | null = null;

/* Browser-hosted origins permitted to open a CDP debugging socket. Chrome serves its own DevTools frontend from devtools://devtools, which is the origin the
 * frontend reached through chrome://inspect presents. Nothing else belongs here: the devtoolsFrontendUrl the discovery endpoint hands out is a path relative to
 * PrismCast's own origin that PrismCast serves no route for, matching Chrome's convention of letting the browser supply its own frontend, so no hosted frontend
 * is ever loaded on this proxy's behalf. Admitting another frontend is a one-line addition here.
 */
const CDP_ALLOWED_ORIGINS: ReadonlySet<string> = new Set(["devtools://devtools"]);

/* CDP wire types. We model what the proxy exchanges over the WebSocket; the Protocol namespace from devtools-protocol gives us the per-domain types so the
 * synthesized Target.* messages stay byte-compatible with what Chrome itself would emit.
 */

/**
 * A CDP wire request received from a connected client. `id` is the client's correlation token; we echo it back in the response. `sessionId` is present when the
 * client is addressing a sub-session attached via Target.attachToTarget; absent for browser-level commands.
 */
interface CdpRequest {

  readonly id: number;
  readonly method: string;
  readonly params?: unknown;
  readonly sessionId?: string;
}

/**
 * A CDP wire response sent to the client. Either result or error is populated, never both.
 */
interface CdpResponse {

  readonly error?: { readonly code: number; readonly message: string };
  readonly id: number;
  readonly result?: unknown;
  readonly sessionId?: string;
}

/**
 * A CDP wire event sent to the client. No `id`; sessionId is present for events emitted by a sub-session.
 */
interface CdpEvent {

  readonly method: string;
  readonly params?: unknown;
  readonly sessionId?: string;
}

/**
 * Builds the public-facing webSocketDebuggerUrl for a target. Chrome's wire convention puts the host:port of the debugging port at the front and a path
 * identifying the target underneath /devtools. We mirror that shape so external clients recognize the URL as standard CDP.
 * @param req - The current Express request, used to derive host + protocol so the URL reflects how the client actually reached us.
 * @param suffix - The target-specific path under /devtools (e.g., "browser/<id>", "page/<id>").
 * @returns The ws:// URL.
 */
function makeWsUrl(req: Request, suffix: string): string {

  // Express exposes the Host header at req.headers.host. We fall back to the request hostname plus the accepting socket's local port only if the Host header
  // is absent, which should not happen for HTTP/1.1 clients but is defensive.
  const host = req.headers.host ?? (req.hostname + ":" + String(req.socket.localPort ?? ""));

  // The proxy speaks ws (not wss) because PrismCast's HTTP listener is plain HTTP. If a reverse proxy fronts PrismCast with TLS, the operator is responsible for
  // adjusting tooling that needs wss.
  return "ws://" + host + "/cdp/devtools/" + suffix;
}

/* Per-browser CDPSession cache used by the GET /cdp/json endpoints to query Chrome for the live target list. Keyed by Browser identity so rotation auto-invalidates
 * - the entry for the previous browser is unreferenced (along with the browser itself) and the next discovery call against the new browser is a structural cache
 * miss. Each entry is stored as a pending Promise rather than a resolved value so concurrent callers per browser serialize on the same in-flight creation; storing
 * the resolved value would let two simultaneous requests each create their own session (the slower one overwriting the faster one's reference and leaking the
 * orphan).
 */
const discoverySessions = new WeakMap<Browser, Promise<CDPSession>>();

/**
 * Returns a working browser-level CDPSession for discovery, lazily creating one per Browser identity and re-creating it once a previously created session later
 * reports itself detached (e.g., after a transient CDP error surfaces post-creation). A creation attempt that itself rejects evicts itself from the cache, so the
 * next call attempts a fresh session while every caller already awaiting the failed attempt still receives its failure. Concurrent callers against the same
 * browser share the same in-flight Promise; callers against a different browser get their own.
 * @param browser - The active Puppeteer Browser.
 * @returns A CDPSession attached to the browser target.
 */
async function getDiscoverySession(browser: Browser): Promise<CDPSession> {

  const existing = discoverySessions.get(browser);

  if(existing) {

    const cached = await existing;

    if(!cached.detached) {

      return cached;
    }
  }

  const session = browser.target().createCDPSession();

  discoverySessions.set(browser, session);

  /* Evict a failed creation so the next call gets a fresh attempt instead of replaying a cached rejection forever. The delete is unconditional rather than
   * guarded on the slot still holding this promise, and that is sound rather than careless: this function is the map's only writer (the one get above and the
   * one set on the line above), a browser rotation keys an entirely different WeakMap slot, and this handler is armed at creation so it runs ahead of any
   * later awaiter of the same settlement - nothing can interpose a newer entry between this promise's rejection and its eviction. Observing the rejection here
   * does not swallow it: a caller already awaiting this promise still receives the failure.
   */
  void session.then(undefined, (): void => {

    discoverySessions.delete(browser);
  });

  return session;
}

/* Puppeteer's CDPSession.send is strongly typed via ProtocolMapping so the compiler enforces correctness when callers know the method name at authoring time. The
 * proxy forwards arbitrary method strings from external clients, so we cast to a method-erased signature at this single boundary; runtime behavior is identical
 * and the wire protocol enforces the shape on both sides. Confining this send-forwarding cast to one helper avoids scattering it across every passthrough call
 * site.
 */
type ErasedCdpSend = (method: string, params?: unknown) => Promise<unknown>;

/**
 * Sends a CDP command on a given session. Wrapper around CDPSession.send that erases the method generic so we can forward arbitrary methods received over the
 * wire.
 * @param session - The CDP session.
 * @param method - The CDP method name.
 * @param params - The CDP parameters.
 * @returns The CDP result.
 */
async function sendCdp(session: CDPSession, method: string, params?: unknown): Promise<unknown> {

  return (session.send as unknown as ErasedCdpSend)(method, params);
}

/**
 * Looks up the underlying Connection for a CDPSession. Returns null when the session is detached.
 * @param session - The CDP session.
 * @returns The Connection or null.
 */
function connectionFor(session: CDPSession): Connection | null {

  return session.connection() ?? null;
}

/* End of wire helpers. */

/**
 * Per-WebSocket CDP proxy state. One instance per attached client. Owns the browser-level CDPSession, the per-target sub-session map, and the lifecycle
 * subscriptions to the Browser. Cleans up on WS close, target destruction, or browser disconnect. Exported for unit testing - production callers receive
 * instances via the upgrade handler and do not construct them directly.
 */
export class CdpProxySession {

  /* The active CDPSession attached to the browser target. Handles browser-level commands and serves as the routing channel for events the proxy emits when no
   * sessionId is present.
   */
  private browserSession: CDPSession | null = null;

  /* The underlying Puppeteer Connection that owns every CDPSession we create. Captured for the Connection.createSession path which lets us mint a Puppeteer
   * CDPSession from a CDP TargetInfo (the recommended public API for attaching to an arbitrary target by id).
   */
  private connection: Connection | null = null;

  /* Maps synthetic sessionId values (opaque to the client; we hand them out via Target.attachToTarget responses and Target.attachedToTarget events) to the
   * underlying Puppeteer CDPSession for that target. Subsequent client messages with a sessionId route through this map.
   */
  private readonly sessions = new Map<string, CDPSession>();

  /* Reverse-map sessionId -> CDP targetId, so the targetdestroyed lifecycle hook can emit Target.detachedFromTarget with the right targetId for the synthetic
   * sub-session.
   */
  private readonly sessionToTargetId = new Map<string, string>();

  /* Captured original emit functions so we can restore them on detach. Each entry is the EventEmitter prototype-bound original emit for the corresponding
   * CDPSession.
   */
  private readonly originalEmits = new Map<CDPSession, (event: string | symbol, ...args: unknown[]) => boolean>();

  /* Counter for synthetic sessionIds. Incremented per attach. The string form looks like an opaque token; we prepend a deterministic prefix so the synthetic id
   * is recognizable as ours when reading raw wire traffic during debugging.
   */
  private nextSessionSerial = 1;

  /* Whether the client has called Target.setDiscoverTargets({ discover: true }). When true, we emit Target.targetCreated / Target.targetDestroyed events as
   * browser targets come and go.
   */
  private discoverTargets = false;

  /* Whether the client has called Target.setAutoAttach({ autoAttach: true, flatten: true }). When true, every existing and future target is auto-attached, and
   * Target.attachedToTarget events are emitted with the synthetic sessionId.
   */
  private autoAttach = false;

  /* Whether start() completed successfully and the "CDP client attached." log fired. cleanup() consults this so the paired "detached" log only emits when an
   * "attached" log preceded it - keeping the audit trail honest when setup fails before the proxy is ever usable.
   */
  private attached = false;

  /* The bound listener attached to `this.browser`'s "disconnected" event in start(). Held as a field so cleanup() can unsubscribe by reference - using a fresh
   * arrow function at unsubscribe time would silently fail to remove the original listener because EventEmitter compares listener identity. Cleared back to null
   * on unsubscribe so the field's presence is a faithful indicator of "are we still listening."
   */
  private onBrowserDisconnect: (() => void) | null = null;

  private readonly ws: WebSocket;
  private readonly browser: Browser;

  constructor(ws: WebSocket, browser: Browser) {

    this.ws = ws;
    this.browser = browser;
  }

  /**
   * Initializes the per-connection CDP plumbing: registers the WebSocket lifecycle handlers, subscribes to the browser's disconnect event, opens a browser-level
   * CDPSession, captures the underlying Connection, and subscribes to CDP target events. Any failure during async setup is caught by the WS close handler
   * (registered first), which fires when `closeWith()` closes the socket and routes cleanup through the same path the happy-path teardown takes. Wiring the
   * lifecycle handlers before any async operation that can fail guarantees the session is torn down (including its disconnect listener) regardless of where
   * setup aborts. Handler bodies are null-safe so an early message arriving before browserSession is set produces a clean error frame rather than a crash.
   */
  async start(): Promise<void> {

    /* Wire the WebSocket lifecycle handlers BEFORE any async setup. closeWith() (used by the failure paths below) closes the socket, which fires the close event,
     * which runs cleanup() - the handler-first ordering guarantees cleanup runs from any failure path, including the browser-disconnect subscription registered
     * immediately after this.
     */
    this.ws.on("message", (data) => { void this.handleClientMessage(data); });
    this.ws.on("close", () => { void this.cleanup(); });
    this.ws.on("error", (err: Error) => {

      LOG.warn("CDP proxy WebSocket error: %s.", err.message);
      void this.cleanup();
    });

    /* Subscribe to the browser's "disconnected" event so this session tears down cleanly when the browser PrismCast launched goes away (crash, rotation, normal
     * shutdown). The handler closes our WS with 1001 ("going away"); the WS close handler above then runs cleanup(), which unsubscribes us. Doing this per-session
     * - rather than a module-level broadcast over a Set - means the lifecycle concern lives on the resource it actually depends on, and a stale subscription from
     * a previous browser can never fire against a session attached to a new one.
     */
    this.onBrowserDisconnect = (): void => { this.closeWith(1001, "browser disconnected"); };
    this.browser.on("disconnected", this.onBrowserDisconnect);

    try {

      this.browserSession = await this.browser.target().createCDPSession();
    } catch(error) {

      LOG.warn("CDP proxy could not open a browser session: %s.", formatError(error));
      this.closeWith(1011, "browser session creation failed");

      return;
    }

    const connection = connectionFor(this.browserSession);

    if(!connection) {

      this.closeWith(1011, "no CDP connection");

      return;
    }

    this.connection = connection;

    /* Subscribe to CDP Target.* lifecycle events on the browser session so we can drive internal session cleanup and auto-attach. This is CDP-native: the
     * targetId on these events matches what Chrome reports through Target.getTargets and is the same id clients send back via Target.attachToTarget. Using
     * Puppeteer's higher-level browser.on("targetcreated") instead would force us to map Puppeteer's opaque Target objects to CDP targetIds, which Puppeteer
     * doesn't expose. The listeners are registered before we enable discovery so no event slips past during the round-trip.
     */
    this.browserSession.on("Target.targetCreated", (event) => { void this.handleCdpTargetCreated(event); });
    this.browserSession.on("Target.targetDestroyed", (event) => { void this.handleCdpTargetDestroyed(event); });

    // Capture every event the browser session emits and forward it to the client with no sessionId (it's a browser-level event).
    this.attachEventListener(this.browserSession, null);

    /* Enable target discovery on our browser session so Target.targetCreated / targetDestroyed events fire. This is required for our internal session cleanup
     * regardless of whether the client has called Target.setDiscoverTargets - the wildcard forwarder gates client-facing emission of Target.* events on
     * `this.discoverTargets`, so the internal subscription doesn't leak events to clients that didn't ask for them.
     */
    try {

      await sendCdp(this.browserSession, "Target.setDiscoverTargets", { discover: true });
    } catch(error) {

      LOG.warn("CDP proxy could not enable target discovery: %s.", formatError(error));
    }

    this.attached = true;
    LOG.info("CDP client attached.");
  }

  /**
   * Closes the WS with a code/reason and triggers cleanup. Safe to call when the WS is already closed.
   * @param code - WebSocket close code (1000 normal, 1001 going-away, 1011 internal-error).
   * @param reason - Human-readable reason.
   */
  closeWith(code: number, reason: string): void {

    if(this.ws.readyState === WebSocket.OPEN) {

      try {

        this.ws.close(code, reason);
      } catch {

        // The ws library may throw if the socket is in an intermediate state. The 'close' event still fires from the underlying socket and runs cleanup() via the
        // handler registered synchronously at the top of start() - that handler-first ordering guarantees cleanup runs from any failure path.
      }
    }
  }

  /**
   * Handles a CDP message received over the WebSocket. Parses the JSON, dispatches to the browser-level or target-level handler, and forwards the response.
   * Malformed JSON is silently dropped (the wire protocol has no error frame for that case at the client's level).
   * @param data - The raw WebSocket message data.
   */
  private async handleClientMessage(data: unknown): Promise<void> {

    let raw: string;

    if(typeof data === "string") {

      raw = data;
    } else if(Buffer.isBuffer(data)) {

      raw = data.toString("utf8");
    } else if(Array.isArray(data)) {

      raw = Buffer.concat(data).toString("utf8");
    } else {

      return;
    }

    let msg: CdpRequest;

    try {

      msg = JSON.parse(raw) as CdpRequest;
    } catch {

      return;
    }

    if((typeof msg.id !== "number") || (typeof msg.method !== "string")) {

      return;
    }

    if(msg.sessionId !== undefined) {

      await this.handleTargetMessage(msg);
    } else {

      await this.handleBrowserMessage(msg);
    }
  }

  /**
   * Dispatches a browser-level CDP command. Intercepts the Target domain to synthesize its semantics on top of Puppeteer's Browser API; passes everything else
   * through to the browser session.
   * @param msg - The parsed CDP request.
   */
  private async handleBrowserMessage(msg: CdpRequest): Promise<void> {

    switch(msg.method) {

      case "Target.setDiscoverTargets": {

        await this.handleSetDiscoverTargets(msg);

        return;
      }

      case "Target.setAutoAttach": {

        await this.handleSetAutoAttach(msg);

        return;
      }

      case "Target.attachToTarget": {

        await this.handleAttachToTarget(msg);

        return;
      }

      case "Target.detachFromTarget": {

        await this.handleDetachFromTarget(msg);

        return;
      }

      case "Target.getTargets":
      case "Target.getTargetInfo": {

        // These are pure reads. We pass them through to the browser session which returns Chrome's authoritative answer.
        await this.passthrough(msg, this.browserSession);

        return;
      }

      default: {

        await this.passthrough(msg, this.browserSession);

        return;
      }
    }
  }

  /**
   * Dispatches a message addressed to a specific sub-session. Looks up the CDPSession by synthetic sessionId and forwards. Unknown sessionId yields a CDP error
   * response so the client sees a clean failure rather than silently dropping.
   * @param msg - The parsed CDP request with a sessionId.
   */
  private async handleTargetMessage(msg: CdpRequest): Promise<void> {

    const sessionId = msg.sessionId;

    if(sessionId === undefined) {

      return;
    }

    const session = this.sessions.get(sessionId);

    if(!session) {

      this.sendResponse({ error: { code: -32602, message: "session not attached: " + sessionId }, id: msg.id, sessionId });

      return;
    }

    await this.passthrough(msg, session);
  }

  /**
   * Forwards a CDP command to a session and sends the response (or error) back to the client. The sessionId on the response is the one the client provided so
   * the client can correlate.
   * @param msg - The parsed CDP request.
   * @param session - The CDP session to forward to.
   */
  private async passthrough(msg: CdpRequest, session: CDPSession | null): Promise<void> {

    if(!session) {

      this.sendResponse({ error: { code: -32603, message: "no session" }, id: msg.id, sessionId: msg.sessionId });

      return;
    }

    try {

      const result = await sendCdp(session, msg.method, msg.params);

      this.sendResponse({ id: msg.id, result, sessionId: msg.sessionId });
    } catch(error) {

      this.sendResponse({ error: { code: -32603, message: formatError(error) }, id: msg.id, sessionId: msg.sessionId });
    }
  }

  /**
   * Handles Target.setDiscoverTargets. When enabled, emits Target.targetCreated for every existing target so the client's view of the world matches Chrome's;
   * Target.targetCreated / Target.targetDestroyed events thereafter flow via the browser session's CDP Target.* event subscriptions.
   * @param msg - The parsed CDP request.
   */
  private async handleSetDiscoverTargets(msg: CdpRequest): Promise<void> {

    const params = (msg.params ?? {}) as { discover?: boolean };

    this.discoverTargets = params.discover === true;
    this.sendResponse({ id: msg.id, result: {} });

    if(!this.discoverTargets || !this.browserSession) {

      return;
    }

    // Emit Target.targetCreated for every existing target so the client has the full picture as of attach time.
    try {

      const info = await sendCdp(this.browserSession, "Target.getTargets", {}) as Protocol.Target.GetTargetsResponse;

      for(const targetInfo of info.targetInfos) {

        this.sendEvent({ method: "Target.targetCreated", params: { targetInfo } });
      }
    } catch(error) {

      LOG.warn("CDP proxy could not enumerate targets for discovery: %s.", formatError(error));
    }
  }

  /**
   * Handles Target.setAutoAttach. When enabled, creates a per-target CDPSession for every existing target, generates a synthetic sessionId, and emits
   * Target.attachedToTarget for each. Subsequent new targets attach via the targetcreated lifecycle hook.
   * @param msg - The parsed CDP request.
   */
  private async handleSetAutoAttach(msg: CdpRequest): Promise<void> {

    const params = (msg.params ?? {}) as { autoAttach?: boolean; flatten?: boolean };

    // We only support flatten:true semantics - the modern mode every contemporary client uses. flatten:false is the legacy hierarchical mode where each session
    // has its own WebSocket; we have no way to express that.
    this.autoAttach = (params.autoAttach === true) && (params.flatten !== false);
    this.sendResponse({ id: msg.id, result: {} });

    if(!this.autoAttach || !this.browserSession) {

      return;
    }

    try {

      const info = await sendCdp(this.browserSession, "Target.getTargets", {}) as Protocol.Target.GetTargetsResponse;

      // Sequential attach (not Promise.all) so synthetic sessionIds are assigned and emitted in target order. Parallelism would interleave the
      // Target.attachedToTarget events and complicate any client that asserts on the attachment sequence.
      for(const targetInfo of info.targetInfos) {

        // eslint-disable-next-line no-await-in-loop
        await this.attachToTargetInfo(targetInfo);
      }
    } catch(error) {

      LOG.warn("CDP proxy could not enumerate targets for auto-attach: %s.", formatError(error));
    }
  }

  /**
   * Handles Target.attachToTarget. Looks up the target's info via the browser session, creates a Puppeteer CDPSession via Connection.createSession (the public
   * API for attaching to a target by id), generates a synthetic sessionId, and returns it. The client uses that sessionId for subsequent messages.
   * @param msg - The parsed CDP request.
   */
  private async handleAttachToTarget(msg: CdpRequest): Promise<void> {

    const params = (msg.params ?? {}) as { flatten?: boolean; targetId?: string };

    if(typeof params.targetId !== "string") {

      this.sendResponse({ error: { code: -32602, message: "targetId required" }, id: msg.id });

      return;
    }

    if(!this.browserSession || !this.connection) {

      this.sendResponse({ error: { code: -32603, message: "browser session not ready" }, id: msg.id });

      return;
    }

    try {

      const info = await sendCdp(this.browserSession, "Target.getTargetInfo", { targetId: params.targetId }) as Protocol.Target.GetTargetInfoResponse;
      const sessionId = await this.attachToTargetInfo(info.targetInfo, { emitEvent: false });

      this.sendResponse({ id: msg.id, result: { sessionId } });
    } catch(error) {

      this.sendResponse({ error: { code: -32603, message: formatError(error) }, id: msg.id });
    }
  }

  /**
   * Attaches to a target given its TargetInfo: creates a Puppeteer CDPSession via Connection.createSession, generates a synthetic sessionId, wires event
   * forwarding, and optionally emits Target.attachedToTarget for the client. Returns the sessionId so callers (manual attach via Target.attachToTarget) can
   * include it in their response.
   * @param targetInfo - The CDP TargetInfo identifying the target.
   * @param options - emitEvent: whether to emit Target.attachedToTarget after attach (defaults to true; manual attach sets false and returns the sessionId via
   *                  the original response).
   * @returns The synthetic sessionId for the newly-attached session.
   */
  private async attachToTargetInfo(targetInfo: Protocol.Target.TargetInfo, options: { emitEvent?: boolean } = {}): Promise<string> {

    if(!this.connection) {

      throw new Error("connection unavailable");
    }

    const session = await this.connection.createSession(targetInfo);
    const sessionId = "cdp-proxy-" + String(this.nextSessionSerial++);

    this.sessions.set(sessionId, session);
    this.sessionToTargetId.set(sessionId, targetInfo.targetId);
    this.attachEventListener(session, sessionId);

    if(options.emitEvent !== false) {

      this.sendEvent({

        method: "Target.attachedToTarget",
        params: { sessionId, targetInfo, waitingForDebugger: false }
      });
    }

    return sessionId;
  }

  /**
   * Handles Target.detachFromTarget. Looks up the session by sessionId, detaches it, emits Target.detachedFromTarget, and removes from the map.
   * @param msg - The parsed CDP request.
   */
  private async handleDetachFromTarget(msg: CdpRequest): Promise<void> {

    const params = (msg.params ?? {}) as { sessionId?: string };

    if(typeof params.sessionId !== "string") {

      this.sendResponse({ error: { code: -32602, message: "sessionId required" }, id: msg.id });

      return;
    }

    await this.detachSessionById(params.sessionId);
    this.sendResponse({ id: msg.id, result: {} });
  }

  /**
   * Detaches a sub-session by its synthetic sessionId, restoring the patched emit, calling CDPSession.detach() (best-effort - the session may already be gone if
   * the target closed), and emitting Target.detachedFromTarget so the client's view is consistent.
   * @param sessionId - The synthetic sessionId.
   */
  private async detachSessionById(sessionId: string): Promise<void> {

    const session = this.sessions.get(sessionId);
    const targetId = this.sessionToTargetId.get(sessionId);

    if(session) {

      this.detachEventListener(session);

      try {

        await session.detach();
      } catch {

        // Detach is best-effort. A session whose target already closed will throw; the resulting state (session gone) is what we want anyway.
      }
    }

    this.sessions.delete(sessionId);
    this.sessionToTargetId.delete(sessionId);

    if(targetId) {

      this.sendEvent({ method: "Target.detachedFromTarget", params: { sessionId, targetId } });
    }
  }

  /**
   * Handles the CDP `Target.targetCreated` event emitted by the browser session. When autoAttach is on, mints a Puppeteer CDPSession for the new target and emits
   * a synthetic Target.attachedToTarget to the client. Whether the client also sees the Target.targetCreated event is decided by the wildcard forwarder (which
   * gates Target.* events on this.discoverTargets).
   * @param event - The CDP event payload.
   */
  private async handleCdpTargetCreated(event: Protocol.Target.TargetCreatedEvent): Promise<void> {

    if(!this.autoAttach) {

      return;
    }

    try {

      await this.attachToTargetInfo(event.targetInfo);
    } catch(error) {

      LOG.warn("CDP proxy auto-attach failed: %s.", formatError(error));
    }
  }

  /**
   * Handles the CDP `Target.targetDestroyed` event emitted by the browser session. Detaches every sub-session attached to this targetId and emits the
   * corresponding Target.detachedFromTarget events. The targetId on the CDP event matches exactly what we stored in sessionToTargetId at attach time, so there is
   * no fuzzy matching - this is the lookup the Puppeteer-level targetdestroyed event could not give us.
   * @param event - The CDP event payload.
   */
  private async handleCdpTargetDestroyed(event: Protocol.Target.TargetDestroyedEvent): Promise<void> {

    const matching: string[] = [];

    for(const [ sessionId, targetId ] of this.sessionToTargetId) {

      if(targetId === event.targetId) {

        matching.push(sessionId);
      }
    }

    // Sequential detach so the synthesized Target.detachedFromTarget events emit in deterministic order to the client.
    for(const sessionId of matching) {

      // eslint-disable-next-line no-await-in-loop
      await this.detachSessionById(sessionId);
    }
  }

  /**
   * Attaches a wildcard event listener to a CDPSession. Each CDP-domain event (matched by the "Domain.event" naming pattern - uppercase first letter, dot,
   * lowercase next letter) is forwarded to the WS client after the session's own listeners have run, so any internal state mutation (e.g. cleanup driven by
   * Target.targetDestroyed) is settled before the client sees the event. Target.* events are gated on this.discoverTargets so a client that has not subscribed
   * to the Target domain does not receive its events even though our internal subscription stays on.
   *
   * Implementation note. CDPSession exposes no public "subscribe to every event" hook, so we monkey-patch the EventEmitter.emit method. The patch saves the
   * original so detachEventListener can restore it cleanly. Order of operations inside the patch is: dispatch through original first (specific listeners fire,
   * including the internal Target.* handlers we registered), then forward to the client. This ordering avoids the race where the client receives an event tied
   * to state we haven't yet cleaned up.
   *
   * @param session - The CDP session to monitor.
   * @param sessionId - The synthetic sessionId to include in the forwarded events (null for browser-level events).
   */
  private attachEventListener(session: CDPSession, sessionId: string | null): void {

    const original = session.emit.bind(session) as (event: string | symbol, ...args: unknown[]) => boolean;

    this.originalEmits.set(session, original);

    // We alias `this` to a closure-captured reference and use a function expression, matching the shape of the original CDPSession.emit method we are replacing -
    // a plain method whose own `this` is rebound to whatever it is invoked on (here, the session). Referencing `proxy` instead of `this` inside the body keeps
    // that distinction explicit at every call site below.
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const proxy = this;
    const patched = function(event: string | symbol, ...args: unknown[]): boolean {

      // Dispatch to user-registered listeners first so internal handlers run before the client sees the event.
      const result = original(event, ...args);

      if((typeof event === "string") && (/^[A-Z][a-zA-Z0-9]+\.[a-z]/).test(event)) {

        // Target.* events are gated: clients only see them when they have opted in via Target.setDiscoverTargets / setAutoAttach. Our internal subscription stays
        // on regardless so we can drive session cleanup.
        const isTargetEvent = event.startsWith("Target.");
        const shouldForward = !isTargetEvent || proxy.discoverTargets;

        if(shouldForward) {

          proxy.sendEvent({ method: event, params: args[0], ...(sessionId === null ? {} : { sessionId }) });
        }
      }

      return result;
    };

    (session as unknown as { emit: typeof patched }).emit = patched;
  }

  /**
   * Restores the original emit on a session, undoing attachEventListener. Called on detach / cleanup.
   * @param session - The CDP session whose emit to restore.
   */
  private detachEventListener(session: CDPSession): void {

    const original = this.originalEmits.get(session);

    if(original) {

      (session as unknown as { emit: typeof original }).emit = original;
      this.originalEmits.delete(session);
    }
  }

  /**
   * Sends a CDP wire response back to the client.
   * @param response - The response object.
   */
  private sendResponse(response: CdpResponse): void {

    this.sendRaw(response);
  }

  /**
   * Sends a CDP wire event back to the client.
   * @param event - The event object.
   */
  private sendEvent(event: CdpEvent): void {

    this.sendRaw(event);
  }

  /**
   * Serializes a frame and writes it to the WS, swallowing errors that arise from a closed socket. We do not retry; if the WS is closed, the proxy session is
   * already on its way to cleanup.
   * @param frame - The frame to send.
   */
  private sendRaw(frame: CdpResponse | CdpEvent): void {

    if(this.ws.readyState !== WebSocket.OPEN) {

      return;
    }

    try {

      this.ws.send(JSON.stringify(frame));
    } catch(error) {

      LOG.warn("CDP proxy WebSocket write failed: %s.", formatError(error));
    }
  }

  /**
   * Detaches every sub-session, restores patched emits, and unsubscribes from the browser-disconnect event. Safe to call multiple times: detach of an
   * already-detached CDPSession is a no-op, and the disconnect-listener field is cleared after the first removal so a repeat call is a no-op too. Triggered by
   * WS close, WS error, or closeWith().
   */
  async cleanup(): Promise<void> {

    // Unsubscribe from browser disconnect first so a disconnect arriving mid-cleanup cannot drive a second closeWith() against an already-closing WS. The
    // sequencing is conservative - closeWith is safe to call again on an already-closing socket - but unsubscribing here keeps the lifecycle ledger clean.
    if(this.onBrowserDisconnect) {

      this.browser.off("disconnected", this.onBrowserDisconnect);
      this.onBrowserDisconnect = null;
    }

    // Sequential detach to keep the final wave of Target.detachedFromTarget events ordered.
    for(const sessionId of Array.from(this.sessions.keys())) {

      // eslint-disable-next-line no-await-in-loop
      await this.detachSessionById(sessionId);
    }

    if(this.browserSession) {

      this.detachEventListener(this.browserSession);

      try {

        await this.browserSession.detach();
      } catch {

        // Best-effort.
      }

      this.browserSession = null;
    }

    if(this.attached) {

      this.attached = false;
      LOG.info("CDP client detached.");
    }
  }
}

/* End of CdpProxySession. */

/**
 * Configures the /cdp/json/version, /cdp/json, and /cdp/json/list discovery endpoints on the Express app. Each handler checks isCategoryEnabled("cdp") at request
 * time and returns 404 when CDP is disabled, so the surface appears and disappears in sync with the toggle in /debug without requiring a restart.
 * @param app - The Express application.
 */
export function setupCdpEndpoint(app: Express): void {

  app.get("/cdp/json/version", async (req: Request, res: Response): Promise<void> => {

    if(!isCategoryEnabled("cdp")) {

      res.status(404).send("Not Found");

      return;
    }

    const browser = getBrowserInstance();

    if(!browser) {

      res.status(503).send("Browser not running");

      return;
    }

    try {

      const session = await getDiscoverySession(browser);
      const version = await sendCdp(session, "Browser.getVersion", {}) as Protocol.Browser.GetVersionResponse;

      res.json({

        "Browser": version.product,
        "Protocol-Version": version.protocolVersion,
        "User-Agent": version.userAgent,
        "V8-Version": version.jsVersion,
        // Browser.getVersion exposes no WebKit version, so we substitute the product string to keep this response byte-compatible with Chrome's /json/version shape.
        "WebKit-Version": version.product,
        "prismcastVersion": getPackageVersion(),
        "webSocketDebuggerUrl": makeWsUrl(req, "browser/prismcast")
      });
    } catch(error) {

      LOG.warn("CDP /cdp/json/version error: %s.", formatError(error));
      res.status(500).send("Internal error");
    }
  });

  // /cdp/json and /cdp/json/list are aliases - both return the target list.
  const listHandler = async (req: Request, res: Response): Promise<void> => {

    if(!isCategoryEnabled("cdp")) {

      res.status(404).send("Not Found");

      return;
    }

    const browser = getBrowserInstance();

    if(!browser) {

      res.status(503).send("Browser not running");

      return;
    }

    try {

      const session = await getDiscoverySession(browser);
      const targets = await sendCdp(session, "Target.getTargets", {}) as Protocol.Target.GetTargetsResponse;

      // The page/<id> suffix below matches Chrome's own /json convention, but it is decorative here: the upgrade handler only checks the /cdp/devtools/ prefix,
      // so connecting to any of these URLs (or to browser/prismcast) yields the same browser-level CdpProxySession with the full Target domain multiplexed over
      // one WebSocket, not a session scoped to that individual page the way native Chrome's per-target debugging URLs are.
      const list = targets.targetInfos.map((info) => ({

        description: "",
        devtoolsFrontendUrl: "/devtools/inspector.html?ws=" + makeWsUrl(req, "page/" + info.targetId).replace(/^ws:\/\//, ""),
        id: info.targetId,
        title: info.title,
        type: info.type,
        url: info.url,
        webSocketDebuggerUrl: makeWsUrl(req, "page/" + info.targetId)
      }));

      res.json(list);
    } catch(error) {

      LOG.warn("CDP /cdp/json error: %s.", formatError(error));
      res.status(500).send("Internal error");
    }
  };

  app.get("/cdp/json", (req, res) => { void listHandler(req, res); });
  app.get("/cdp/json/list", (req, res) => { void listHandler(req, res); });
}

/**
 * Attaches the WebSocket upgrade handler for /cdp/devtools/* paths to the given HTTP server. Called from startServer after app.listen() returns the server.
 * Upgrades for paths outside /cdp/devtools/ are ignored (left to any other upgrade handler).
 * @param server - The Node HTTP server.
 */
export function attachCdpUpgradeHandler(server: HttpServer): void {

  if(wss) {

    return;
  }

  wss = new WebSocketServer({ noServer: true });

  server.on("upgrade", (req: IncomingMessage, socket: Socket, head: Buffer): void => {

    // Only the /cdp/devtools/ prefix is checked below; the suffix (browser/<id> or page/<id>, taken from the URLs handed out by the discovery endpoints above)
    // is never parsed further. Every accepted connection gets the same Target-domain-multiplexed CdpProxySession bound to the browser target, so a page/<id>
    // URL is not scoped to that individual page the way native Chrome's per-target debugging port URLs are.
    const url = req.url ?? "";

    if(!url.startsWith("/cdp/devtools/")) {

      // Not ours - leave the socket for any other upgrade handler. PrismCast doesn't currently install another, but this keeps the surface composable.
      return;
    }

    if(!isCategoryEnabled("cdp")) {

      socket.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
      socket.destroy();

      return;
    }

    /* The Same-Origin Policy does not restrict WebSocket handshakes, so any page on any site can open a socket to this endpoint and the browser will carry it
     * out...the server is the only place the decision can be made. The rule is an allowlist rather than a blanket rejection of every Origin, and it covers both
     * kinds of client this proxy serves. Every non-browser client (puppeteer.connect, curl, chrome-remote-interface) sends no Origin header at all and is
     * admitted by the first arm; the DevTools frontend is browser-hosted, does send one, and is admitted by the second. A hostile page can present neither
     * case, because the browser sets Origin from the page's own origin and script cannot override it, which is why an allowlist of frontend origins gives up
     * nothing in defense. The check sits ahead of the browser lookup so a refused origin gets the same answer whether or not Chrome happens to be running.
     */
    const origin = req.headers.origin;

    if((origin !== undefined) && !CDP_ALLOWED_ORIGINS.has(origin)) {

      LOG.warn("Refused a CDP debugging connection from origin %s.", origin);
      socket.write("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
      socket.destroy();

      return;
    }

    const browser = getBrowserInstance();

    if(!browser) {

      socket.write("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n");
      socket.destroy();

      return;
    }

    if(!wss) {

      socket.destroy();

      return;
    }

    wss.handleUpgrade(req, socket, head, (ws) => {

      // Every WS connection accepted above - regardless of whether it arrived via a browser/<id> or page/<id> suffix - gets its own CdpProxySession bound to
      // the browser target; the Target domain synthesized inside that session handles per-target routing, so no per-page scoping happens at this boundary.
      const proxySession = new CdpProxySession(ws, browser);

      void proxySession.start();
    });
  });

  LOG.info("CDP proxy listening at /cdp.");
}
