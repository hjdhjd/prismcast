/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: HDHomeRun emulation lifecycle for PrismCast.
 *
 * When HDHomeRun emulation is enabled, PrismCast runs complementary HTTP and UDP surfaces. The HTTP server (this module + discover.ts) responds to /device.xml,
 * /discover.json, /lineup.json, /lineup_status.json, and /status.json - the surface clients consume once they have located PrismCast by IP and port. The UDP
 * responder (udp.ts) answers SiliconDust LAN-discovery broadcasts on port 65001 so Plex finds PrismCast automatically without a manual address paste. The HTTP
 * and UDP surfaces are not symmetric: UDP discovery is gated on the HTTP surface being bound, so the only independent choice is to disable LAN discovery while
 * keeping HTTP HDHR running (multi-tenant boxes, environments with a real HDHomeRun already on the network); discovery never runs without HTTP. Channels DVR
 * also auto-discovers via the UDP responder but its discovery assumes port 80 for the HTTP control plane, so the lineup fetch fails unless hdhr.port is set to
 * 80; Channels DVR users typically add PrismCast manually as a Custom Channels source.
 *
 * The lifecycle is modeled as a reconciler owning self-disposing resource nodes. An HdhrController owns one HttpSurface and one UdpSurface; each surface fully
 * owns its socket, encapsulating its own bind/rebind/close cycling, and exposes [Symbol.asyncDispose]. The controller expresses policy ("HTTP on the desired
 * port; UDP up iff discoveryEnabled and HTTP is bound") and never reaches into how a surface binds or closes. reconcile() realizes the desired state it is handed
 * - CONFIG.hdhr at boot, the candidate running configuration on a save - and [Symbol.asyncDispose] is the terminal teardown. The HTTP and UDP surfaces are the single
 * source of truth for "what HDHR is actually running" - there are no module-level socket globals.
 *
 * This module additionally registers a config-change handler under the "hdhr." prefix. Each setting's reactivity class decides whether a save applies it
 * live; the handler realizes the candidate's HDHomeRun state once and reports only what the surfaces could not realize, and the reconcile commits nothing a
 * handler refused, so a refused port stays a change every later save retries. Registration happens at module-load time as a top-level side effect: ESM modules
 * load exactly once per process, so the handler is in place before any settings-save can fire. Tests that explicitly reset the reactivity registry can
 * re-register by importing and invoking registerConfigChangeHandler("hdhr.", applyHdhrConfigChanges) directly - both symbols are exported.
 */
import type { ChangeRejection, ConfigChange } from "../config/reactivity.ts";
import type { Config, HdhrConfig, Nullable } from "../types/index.ts";
import { HDHR_DISCOVERY_PORT, createUdpSurface } from "./udp.ts";
import type { AddressInfo } from "node:net";
import { CONFIG } from "../config/index.ts";
import { LOG } from "../utils/index.ts";
import type { Server } from "node:http";
import express from "express";
import { formatError } from "../utils/errors.ts";
import { registerConfigChangeHandler } from "../config/reactivity.ts";
import { setupHdhrEndpoints } from "./discover.ts";

/**
 * What an HTTP surface is asked to bind: the port and host it listens on, and the DeviceID its listening line names.
 */
interface HttpBindRequest {

  // The DeviceID the surface advertises, named in the listening line.
  readonly deviceId: string;

  // The host address to bind.
  readonly host: string;

  // The TCP port to bind. Zero asks the OS for a free port.
  readonly port: number;
}

/**
 * A self-disposing HDHomeRun HTTP surface. The node owns exactly one express HTTP server and the entire bind state machine: bind-to-desired, no-op when already
 * there, and make-before-break on a port change - the requested port is bound while the current server keeps answering, and the current server closes only once
 * the new one listens, so a refused port cannot take down a working tuner. ensureDown is exposed as [Symbol.asyncDispose]; disposal closes the current server but
 * leaves the node reusable (a later ensureBound rebinds), because this is owner-bounded, not scope-bounded, disposal.
 */
interface HttpSurface extends AsyncDisposable {

  // The port the HTTP server is bound to, or null when the surface is down. The single source of truth for "what port HTTP is actually on" - read by the UDP
  // gating decision and by the Discover reply's advertised BaseURL.
  readonly boundPort: Nullable<number>;

  // Converges the surface to listening on the requested port, binding it before closing the bound server. Returns true on success, false when the requested bind
  // failed (the caller reports the change rejected, and the bound server, if there is one, keeps answering on its port).
  ensureBound(request: HttpBindRequest): Promise<boolean>;

  // Closes the HTTP server if one is bound, resolving only after the socket is fully released. A no-op when already down. Aliased as [Symbol.asyncDispose].
  ensureDown(): Promise<void>;
}

/**
 * What a reconcile could not realize. The handler translates each failure into a rejection, so a change the surfaces did not reach is never committed.
 */
interface HdhrReconcileResult {

  // True when the HTTP surface could not bind the desired port.
  readonly httpFailed: boolean;

  // True when LAN discovery was requested and its UDP surface could not bind.
  readonly udpFailed: boolean;
}

/**
 * The HDHomeRun controller: a reconciler that owns the HTTP and UDP emulation surfaces. reconcile() drives both surfaces to the desired state it is handed and reports
 * what failed to reach it; [Symbol.asyncDispose] is the terminal teardown that brings both down.
 */
interface HdhrController extends AsyncDisposable {

  // Drives the HTTP and UDP surfaces to the desired HDHomeRun state. The bind host comes from CONFIG.server.host, a restart-class setting that always holds the
  // running value. Returns what failed to reach the desired state so a caller can refuse the change rather than report it realized.
  reconcile(desired: Readonly<HdhrConfig>): Promise<HdhrReconcileResult>;
}

/**
 * Creates an HDHomeRun HTTP surface node. The returned object fully owns its server; nothing outside the closure touches it. ensureBound is the converge-up verb
 * (with the make-before-break rebind sealed inside it), ensureDown the converge-down verb, and [Symbol.asyncDispose] aliases ensureDown so the surface is a
 * well-formed async-disposable resource.
 * @returns An HttpSurface node.
 */
function createHttpSurface(): HttpSurface {

  // The bound HTTP server, owned entirely by this node. Null when the surface is down.
  let server: Nullable<Server> = null;

  // currentPort resolves the live bound port from the server's address, or null when down. Internal helper so the public getter and ensureBound read one
  // source. Uses the resolved socket address rather than the requested port so an OS-assigned port (port 0 in tests) reports its real value.
  function currentPort(): Nullable<number> {

    // This surface always binds a TCP host:port pair, never a Unix domain socket or named pipe, so server.address()
    // cannot return its string form here, and the AddressInfo cast is safe by construction.
    return server ? ((server.address() as AddressInfo | null)?.port ?? null) : null;
  }

  /**
   * Closes an HTTP server and resolves only after the underlying socket is fully released. Server.close's callback fires after the socket is gone but the public
   * API is callback-shaped; we surface it as a Promise so callers can compose with await. The callback's error argument is ignored - the only error it can carry
   * is ERR_SERVER_NOT_RUNNING, which cannot occur here because we only close a server we confirmed listening.
   * @param target - The server to close.
   */
  async function close(target: Server): Promise<void> {

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise, resolve } = Promise.withResolvers<void>();

    target.close(() => { resolve(); });

    return promise;
  }

  /**
   * Opens a fresh HDHomeRun HTTP server on the requested port and host. Returns the server once the bind is confirmed listening, or null on bind failure, and
   * never touches the node's server reference. The low-level open primitive; ensureBound layers the make-before-break swap on top.
   *
   * Detects bind success or failure through the explicit "listening" and "error" events rather than express's listen callback. The listen callback fires even
   * when the bind fails (verified empirically: it resolves on EADDRINUSE and EADDRNOTAVAIL with a non-listening server), so it cannot be trusted to signal
   * success - relying on it would report a port conflict as a successful start. Exactly one of "listening" or "error" fires for a given
   * bind attempt, so the promise always settles. This mirrors the bind-vs-runtime error-handler split the UDP surface uses in udp.ts.
   * @param request - The port and host to bind, and the DeviceID the listening line names.
   * @returns The listening server, or null if the bind failed.
   */
  async function bind(request: HttpBindRequest): Promise<Nullable<Server>> {

    const app = express();

    // The HDHomeRun HTTP surface can sit behind a reverse proxy, so we trust the X-Forwarded-* chain. With trust proxy enabled, Express derives the client IP,
    // protocol, and hostname from the forwarded headers rather than the immediate proxy hop, so any consumer reading those request properties sees the originating
    // client. This complements resolveHostname in discover.ts, which prefers X-Forwarded-Host when composing the advertised BaseURL.
    app.set("trust proxy", true);

    // The port this server is bound to, read from its own address once it listens. The documents it serves advertise this value, so an OS-assigned port is
    // advertised as bound, and because a server's port is fixed for its lifetime, a request this server answers during a make-before-break swap still names it.
    let listeningPort = request.port;

    setupHdhrEndpoints(app, () => listeningPort);

    const { promise, resolve } = Promise.withResolvers<Nullable<Server>>();
    const candidate = app.listen(request.port, request.host);

    const onBindError = (error: NodeJS.ErrnoException): void => {

      if(error.code === "EADDRINUSE") {

        LOG.warn("HDHomeRun port %s is already in use. Check for conflicting services on this port.", request.port);
      } else {

        LOG.warn("Failed to start the HDHomeRun HTTP server: %s.", formatError(error));
      }

      resolve(null);
    };

    candidate.once("error", onBindError);

    candidate.once("listening", (): void => {

      // Bind succeeded: retire the bind-failure handler and attach a long-lived runtime handler so a later socket error is logged rather than crashing the
      // process on an unhandled "error" event.
      candidate.removeListener("error", onBindError);
      candidate.on("error", (error: NodeJS.ErrnoException): void => {

        LOG.warn("HDHomeRun HTTP server encountered a socket error: %s.", formatError(error));
      });

      // A listening server's address is the AddressInfo of its TCP bind, for the reason currentPort states.
      listeningPort = (candidate.address() as AddressInfo).port;

      LOG.info("HDHomeRun emulation is now listening on %s:%s (DeviceID: %s).", request.host, listeningPort, request.deviceId.toUpperCase());

      resolve(candidate);
    });

    return promise;
  }

  async function ensureBound(request: HttpBindRequest): Promise<boolean> {

    // Already listening on the requested port - nothing to do (covers deviceId/friendlyName-only changes, which need no rebind).
    if(currentPort() === request.port) {

      return true;
    }

    // Make before break: the requested port is bound while the current server keeps answering, so the surface answers on one port or the other at every instant
    // and a refused port costs one failed listen rather than the working tuner. The change is still reported rejected so the operator knows it did not take.
    const next = await bind(request);

    if(next === null) {

      const activePort = currentPort();

      if(activePort !== null) {

        LOG.warn("HDHomeRun port %s is unavailable; keeping the previous port %s active. The port change was rejected.", request.port, activePort);
      }

      return false;
    }

    // The new server is listening: swap the reference to it first, so every reader sees the new port from here on, and only then close the previous server.
    const previous = server;

    server = next;

    if(previous !== null) {

      await close(previous);
    }

    return true;
  }

  async function ensureDown(): Promise<void> {

    if(server === null) {

      return;
    }

    const current = server;

    // Clear the reference before the asynchronous close so a concurrent ensureBound during teardown opens a fresh server rather than racing the closing one.
    server = null;

    await close(current);
  }

  return {

    get boundPort(): Nullable<number> {

      return currentPort();
    },
    ensureBound,
    ensureDown,
    [Symbol.asyncDispose]: ensureDown
  };
}

/**
 * Creates the HDHomeRun controller. The controller owns the HTTP and UDP surface nodes for the whole process lifetime; their sockets cycle internally as reconcile drives
 * them. Failures at any surface are reported up, not thrown - the broader application continues to work even if HDHR emulation cannot bind.
 * @returns An HdhrController node.
 */
function createHdhrController(): HdhrController {

  // The HTTP and UDP surfaces this controller owns. Created once and live for the controller's lifetime; the controller expresses desired state (policy) and
  // never reaches into how a surface binds, rebinds, or closes (mechanism).
  const http = createHttpSurface();
  const udp = createUdpSurface();

  async function reconcile(desired: Readonly<HdhrConfig>): Promise<HdhrReconcileResult> {

    // Desired state "disabled": bring both surfaces down. UDP first so it stops advertising a BaseURL before the HTTP server it points at goes away. This cannot
    // fail. The surfaces remain reusable, so a later enable rebinds them.
    if(!desired.enabled) {

      await udp.ensureDown();
      await http.ensureDown();

      return { httpFailed: false, udpFailed: false };
    }

    // The surface binds with the DeviceID it is handed. The configuration layer corrects that id whenever the boot loads the file or a settings save writes it
    // while the emulation is enabled, so the id is the one the file holds, and a process write leaves the id as the file holds it.
    const httpFailed = !(await http.ensureBound({ deviceId: desired.deviceId, host: CONFIG.server.host, port: desired.port }));

    // UDP is gated on the HTTP server actually being bound: a discovery responder must never advertise a BaseURL pointing at an HTTP port with no listener, so
    // when HTTP is down we stop UDP regardless of the discoveryEnabled flag. The Discover reply advertises http.boundPort (not the desired port) so it always
    // reflects reality, including a refused port change, where HTTP stays on the port it is bound to.
    let udpFailed = false;

    if(desired.discoveryEnabled && (http.boundPort !== null)) {

      udpFailed = !(await udp.ensureUp({ httpPortProvider: () => http.boundPort }));
    } else {

      await udp.ensureDown();
    }

    return { httpFailed, udpFailed };
  }

  async function disposeAsync(): Promise<void> {

    // Terminal teardown: bring both surfaces down, UDP first (stop advertising before the advertised HTTP server dies). The surfaces remain reusable afterward -
    // a later reconcile can bring them back - because closing a socket does not poison the node; this is owner-bounded disposal, not scope-bounded.
    await udp.ensureDown();
    await http.ensureDown();
  }

  return {

    reconcile,
    [Symbol.asyncDispose]: disposeAsync
  };
}

// The process-lifetime HDHomeRun controller. Created at module load (a pure factory call - no sockets bind until reconcile runs), so the public lifecycle
// functions and the live-apply handler share one owner of the emulation surfaces.
const hdhrController = createHdhrController();

/**
 * Brings the HDHomeRun emulation surfaces (HTTP + optional UDP discovery) into line with CONFIG.hdhr. Thin delegator to the controller's reconcile so the boot
 * path and the live-apply path share one code path. Failures are reported through the reconcile result and logged at the surface layer; boot treats them as
 * graceful degradation and does not propagate them.
 */
export async function startHdhrServer(): Promise<void> {

  await hdhrController.reconcile(CONFIG.hdhr);
}

/**
 * Stops the HDHomeRun emulation surfaces and waits for the underlying sockets to fully release. Awaiting close completion matters when the caller intends to
 * immediately rebind on the same port - skipping the await races the still in-flight async close, since the old listening socket may not yet have released
 * the port, risking EADDRINUSE on the fresh bind. Safe to call more than once: a second call when nothing is running succeeds as a no-op, and a later
 * startHdhrServer rebinds cleanly.
 */
export async function stopHdhrServer(): Promise<void> {

  await hdhrController[Symbol.asyncDispose]();
}

/**
 * Realizes the HDHomeRun state of a save's candidate running configuration. The controller realizes the desired state it is handed once - the whole state, so
 * there is no per-change ordering to get wrong - and the handler reports only what the surfaces could not realize: the HTTP-driving fields (enabled, port) when
 * the HTTP surface failed to bind, and discoveryEnabled when discovery was requested and its bind failed. Each setting's reactivity class has already decided
 * that the change applies live, and the reconcile commits nothing refused here, so a refused change keeps its running value and is retried by every later save.
 * Every other HDHomeRun field, the DeviceID among them, is read where it is used, so reaching the desired state realizes it.
 *
 * Exported so tests can invoke the dispatch directly with a synthetic diff. Registered with the reactivity primitive at module load (side effect at the bottom
 * of this file) so production settings saves route here automatically.
 * @param changes - The changes of the gap whose path begins with "hdhr.".
 * @param next - The candidate running configuration whose HDHomeRun state the handler realizes.
 * @returns A rejection for each change the surfaces could not realize.
 */
export async function applyHdhrConfigChanges(changes: readonly ConfigChange[], next: Readonly<Config>): Promise<readonly ChangeRejection[]> {

  const desired = next.hdhr;
  const { httpFailed, udpFailed } = await hdhrController.reconcile(desired);
  const rejections: ChangeRejection[] = [];

  for(const change of changes) {

    switch(change.path) {

      case "hdhr.enabled":
      case "hdhr.port": {

        // These fields drive the HTTP surface, so a failed bind refuses them and the running configuration keeps the state the surface is actually in.
        if(httpFailed) {

          rejections.push({ path: change.path, reason: "HDHomeRun could not bind port " + String(desired.port) + ", so the change was not applied." });
        }

        break;
      }

      case "hdhr.discoveryEnabled": {

        // Drives the UDP surface. udpFailed is only true when discovery was requested (enabled) and the bind failed; turning discovery off cannot fail.
        if(udpFailed) {

          rejections.push({ path: change.path, reason: "HDHomeRun LAN discovery could not bind UDP port " + String(HDHR_DISCOVERY_PORT) + " (in use)." });
        }

        break;
      }

      default: {

        break;
      }
    }
  }

  return rejections;
}

// Module-load side effect: register the live-apply handler exactly once per process. ESM modules load at most once per process so this runs deterministically
// at boot - before app.ts's startHdhrServer call, before any settings save can fire, and before any test interaction. Tests that explicitly reset the registry
// via resetConfigChangeHandlers() can re-register by calling registerConfigChangeHandler("hdhr.", applyHdhrConfigChanges) themselves; both symbols are
// exported. Co-locating the registration here (rather than at startHdhrServer call time) keeps registry state and registration state from diverging.
registerConfigChangeHandler("hdhr.", applyHdhrConfigChanges);
