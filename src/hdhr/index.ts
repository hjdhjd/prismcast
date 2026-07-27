/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: HDHomeRun emulation lifecycle for PrismCast.
 *
 * When HDHomeRun emulation is enabled, PrismCast runs two complementary surfaces. The HTTP server (this module + discover.ts) responds to /device.xml,
 * /discover.json, /lineup.json, /lineup_status.json, and /status.json - the surface clients consume once they have located PrismCast by IP and port. The UDP
 * responder (udp.ts) answers SiliconDust LAN-discovery broadcasts on port 65001 so Plex finds PrismCast automatically without a manual address paste. The two
 * surfaces are not symmetric: UDP discovery is gated on the HTTP surface being bound, so the only independent choice is to disable LAN discovery while keeping HTTP
 * HDHR running (multi-tenant boxes, environments with a real HDHomeRun already on the network); discovery never runs without HTTP. Channels DVR also auto-discovers
 * via the UDP responder but its discovery assumes port 80 for the HTTP control
 * plane, so the lineup fetch fails unless hdhr.port is set to 80; Channels DVR users typically add PrismCast manually as a Custom Channels source.
 *
 * The lifecycle is modeled as a reconciler owning self-disposing resource nodes. An HdhrController owns one HttpSurface and one UdpSurface; each surface fully
 * owns its socket, encapsulating its own bind/rebind/close cycling, and exposes [Symbol.asyncDispose]. The controller expresses policy ("HTTP on
 * CONFIG.hdhr.port; UDP up iff discoveryEnabled and HTTP is bound") and never reaches into how a surface binds or closes. reconcile() drives the surfaces to the
 * state CONFIG.hdhr calls for (boot and live-apply both route through it); [Symbol.asyncDispose] is the terminal teardown. The two surfaces are the single source
 * of truth for "what HDHR is actually running" - there are no module-level socket globals.
 *
 * This module additionally registers a config-change handler under the "hdhr." prefix so HDHomeRun-related settings can take effect without a server restart.
 * The handler reconciles the surfaces to the new CONFIG.hdhr once and maps each input change to an outcome. Registration happens at module-load time as a top-
 * level side effect: ESM modules load exactly once per process, so the handler is in place before any settings-save can fire. Tests that explicitly reset the
 * reactivity registry can re-register by importing and invoking registerConfigChangeHandler("hdhr.", applyHdhrConfigChanges) directly - both symbols are exported.
 */
import type { ChangeOutcome, ConfigChange } from "../config/reactivity.ts";
import { HDHR_DISCOVERY_PORT, createUdpSurface } from "./udp.ts";
import { generateDeviceId, validateDeviceId } from "./deviceId.ts";
import type { AddressInfo } from "node:net";
import { CONFIG } from "../config/index.ts";
import { LOG } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import type { Server } from "node:http";
import express from "express";
import { formatError } from "../utils/errors.ts";
import { mutateConfig } from "../config/userConfig.ts";
import { registerConfigChangeHandler } from "../config/reactivity.ts";
import { setupHdhrEndpoints } from "./discover.ts";

/**
 * A self-disposing HDHomeRun HTTP surface. The node owns exactly one express HTTP server and the entire bind state machine: bind-to-desired, no-op when already
 * there, close-and-rebind on a port change, and the prior-port concession (a failed rebind re-binds the previous port so a typo'd port cannot take down a working
 * tuner). ensureDown is exposed as [Symbol.asyncDispose]; disposal closes the current server but leaves the node reusable (a later ensureBound rebinds), because
 * this is owner-bounded, not scope-bounded, disposal.
 */
interface HttpSurface extends AsyncDisposable {

  // The port the HTTP server is bound to, or null when the surface is down. The single source of truth for "what port HTTP is actually on" - read by the UDP
  // gating decision and by the Discover reply's advertised BaseURL.
  readonly boundPort: Nullable<number>;

  // Converges the surface to listening on the given port, owning the rebind and concession internally. Returns true on success, false when the desired bind
  // failed (the caller reports the change rejected; the concession may have kept the prior port alive).
  ensureBound(port: number, host: string): Promise<boolean>;

  // Closes the HTTP server if one is bound, resolving only after the socket is fully released. A no-op when already down. Aliased as [Symbol.asyncDispose].
  ensureDown(): Promise<void>;
}

/**
 * The HDHomeRun controller: a reconciler that owns the two emulation surfaces. reconcile() drives both surfaces to the state CONFIG.hdhr calls for and reports
 * which surfaces failed to reach it; [Symbol.asyncDispose] is the terminal teardown that brings both down.
 */
interface HdhrController extends AsyncDisposable {

  // Drives the HTTP and UDP surfaces to match the current CONFIG.hdhr. Returns which surfaces failed to reach their desired state so a caller can translate a
  // failure into a rejected outcome rather than a false "applied".
  reconcile(): Promise<{ httpFailed: boolean; udpFailed: boolean }>;
}

/**
 * Creates an HDHomeRun HTTP surface node. The returned object fully owns its server; nothing outside the closure touches it. ensureBound is the converge-up verb
 * (with the rebind + concession sealed inside it), ensureDown the converge-down verb, and [Symbol.asyncDispose] aliases ensureDown so the surface is a well-formed
 * async-disposable resource.
 * @returns An HttpSurface node.
 */
function createHttpSurface(): HttpSurface {

  // The bound HTTP server, owned entirely by this node. Null when the surface is down.
  let server: Nullable<Server> = null;

  // currentPort resolves the live bound port from the server's address, or null when down. Internal helper so both the public getter and the bind logic read one
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
   * Opens a fresh HDHomeRun HTTP server on the given port and host, capturing it in the node's server reference on success. Returns true once the bind is
   * confirmed listening, false on bind failure. The low-level open primitive; ensureBound layers the rebind + concession policy on top.
   *
   * Detects bind success or failure through the explicit "listening" and "error" events rather than express's listen callback. The listen callback fires even
   * when the bind fails (verified empirically: it resolves on EADDRINUSE and EADDRNOTAVAIL with a non-listening server), so it cannot be trusted to signal
   * success - relying on it would report a port conflict as a successful start. Exactly one of "listening" or "error" fires for a given
   * bind attempt, so the promise always settles. This mirrors the bind-vs-runtime error-handler split the UDP surface uses in udp.ts.
   * @param port - The TCP port to bind.
   * @param host - The host address to bind.
   * @returns True if the server is now listening, false if the bind failed.
   */
  async function bind(port: number, host: string): Promise<boolean> {

    const app = express();

    // The HDHomeRun HTTP surface can sit behind a reverse proxy, so we trust the X-Forwarded-* chain. With trust proxy enabled, Express derives the client IP,
    // protocol, and hostname from the forwarded headers rather than the immediate proxy hop, so any consumer reading those request properties sees the originating
    // client. This complements resolveHostname in discover.ts, which prefers X-Forwarded-Host when composing the advertised BaseURL.
    app.set("trust proxy", true);

    setupHdhrEndpoints(app);

    const { promise, resolve } = Promise.withResolvers<boolean>();
    const candidate = app.listen(port, host);

    const onBindError = (error: NodeJS.ErrnoException): void => {

      if(error.code === "EADDRINUSE") {

        LOG.warn("HDHomeRun port %s is already in use. Check for conflicting services on this port.", port);
      } else {

        LOG.warn("Failed to start the HDHomeRun HTTP server: %s.", formatError(error));
      }

      resolve(false);
    };

    candidate.once("error", onBindError);

    candidate.once("listening", (): void => {

      // Bind succeeded: retire the bind-failure handler and attach a long-lived runtime handler so a later socket error is logged rather than crashing the
      // process on an unhandled "error" event.
      candidate.removeListener("error", onBindError);
      candidate.on("error", (error: NodeJS.ErrnoException): void => {

        LOG.warn("HDHomeRun HTTP server encountered a socket error: %s.", formatError(error));
      });

      server = candidate;

      LOG.info("HDHomeRun emulation is now listening on %s:%s (DeviceID: %s).", host, currentPort() ?? port, CONFIG.hdhr.deviceId.toUpperCase());

      resolve(true);
    });

    return promise;
  }

  async function ensureBound(port: number, host: string): Promise<boolean> {

    // Already listening on the desired port - nothing to do (covers deviceId/friendlyName-only changes, which need no rebind).
    if(currentPort() === port) {

      return true;
    }

    // Capture the prior port before closing so a failed rebind can fall back to it (the concession: a typo'd port must not take down a working tuner).
    const priorPort = currentPort();

    await ensureDown();

    if(await bind(port, host)) {

      return true;
    }

    // The desired bind failed. Concession (operator-confirmed): if we were previously listening on another port, re-bind it so the tuner keeps working; the
    // change is still reported rejected so the operator knows it did not take. If there was no prior port (boot or already-down), there is nothing to restore.
    if(priorPort !== null) {

      LOG.warn("HDHomeRun port %s is unavailable; keeping the previous port %s active. The port change was rejected.", port, priorPort);

      await bind(priorPort, host);
    }

    return false;
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
 * Ensures CONFIG.hdhr.deviceId carries a checksum-valid value, generating and persisting a fresh one when missing or invalid. Called from reconcile before
 * bringing the surfaces up so a never-before-enabled HDHR setup gets a DeviceID on first activation rather than only at process boot.
 */
async function ensureDeviceId(): Promise<void> {

  // Generate a DeviceID on first run, or regenerate if the stored ID fails checksum validation (e.g., hand-edited config with a typo). Plex silently rejects
  // tuners with invalid DeviceIDs during discovery, so we catch this early.
  if(CONFIG.hdhr.deviceId && validateDeviceId(CONFIG.hdhr.deviceId)) {

    return;
  }

  if(CONFIG.hdhr.deviceId) {

    LOG.warn("HDHomeRun DeviceID '%s' has an invalid checksum. Generating a new one.", CONFIG.hdhr.deviceId.toUpperCase());
  }

  CONFIG.hdhr.deviceId = generateDeviceId();

  LOG.info("Generated HDHomeRun DeviceID: %s.", CONFIG.hdhr.deviceId.toUpperCase());

  // Save the generated ID to the user config so it persists across restarts.
  try {

    await mutateConfig((config) => {

      config.hdhr ??= {};
      config.hdhr.deviceId = CONFIG.hdhr.deviceId;
    });
  } catch(error) {

    LOG.warn("Failed to persist HDHomeRun DeviceID: %s. A new ID will be generated on next restart.", formatError(error));
  }
}

/**
 * Creates the HDHomeRun controller. The controller owns the two surface nodes for the whole process lifetime; their sockets cycle internally as reconcile drives
 * them. Failures at any surface are reported up, not thrown - the broader application continues to work even if HDHR emulation cannot bind.
 * @returns An HdhrController node.
 */
function createHdhrController(): HdhrController {

  // The two HDHomeRun surfaces this controller owns. Created once and live for the controller's lifetime; the controller expresses desired state (policy) and
  // never reaches into how a surface binds, rebinds, or closes (mechanism).
  const http = createHttpSurface();
  const udp = createUdpSurface();

  async function reconcile(): Promise<{ httpFailed: boolean; udpFailed: boolean }> {

    // Desired state "disabled": bring both surfaces down. UDP first so it stops advertising a BaseURL before the HTTP server it points at goes away. This cannot
    // fail. The surfaces remain reusable, so a later enable rebinds them.
    if(!CONFIG.hdhr.enabled) {

      await udp.ensureDown();
      await http.ensureDown();

      return { httpFailed: false, udpFailed: false };
    }

    await ensureDeviceId();

    const httpFailed = !(await http.ensureBound(CONFIG.hdhr.port, CONFIG.server.host));

    // UDP is gated on the HTTP server actually being bound: a discovery responder must never advertise a BaseURL pointing at an HTTP port with no listener, so
    // when HTTP is down we stop UDP regardless of the discoveryEnabled flag. The Discover reply advertises http.boundPort (not CONFIG.hdhr.port) so it always
    // reflects reality, including the rejected-port-change concession case where HTTP stays on the prior port.
    let udpFailed = false;

    if(CONFIG.hdhr.discoveryEnabled && (http.boundPort !== null)) {

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

  await hdhrController.reconcile();
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
 * Live-applies the subset of a config diff that lives under the "hdhr." prefix. Reconciles the HTTP + UDP surfaces to the new CONFIG.hdhr once (whole desired
 * state, no per-change ordering to get wrong) and then maps each input change to an outcome from the reconciliation result: surface-driving fields (enabled,
 * port) report rejected when their surface failed to bind; deviceId and friendlyName are read live by their consumers and always apply; discoveryEnabled reports
 * rejected only when discovery was requested but failed to bind; an unknown hdhr.* field defers so a future field cannot silently no-op.
 *
 * Exported so tests can invoke the dispatch directly with a synthetic diff. Registered with the reactivity primitive at module load (side effect at the bottom
 * of this file) so production settings saves route here automatically.
 * @param changes - The subset of the diff whose path begins with "hdhr.".
 * @returns Per-change outcomes.
 */
export async function applyHdhrConfigChanges(changes: readonly ConfigChange[]): Promise<readonly ChangeOutcome[]> {

  const { httpFailed, udpFailed } = await hdhrController.reconcile();

  return changes.map((change) => {

    switch(change.path) {

      case "hdhr.enabled":
      case "hdhr.port": {

        // These fields drive the HTTP surface. A failed bind (port in use) is surfaced as rejected; the concession inside HttpSurface.ensureBound has already
        // kept the prior port alive, so "rejected" accurately means "the change you saved did not take, but the tuner is still running on its previous configuration."
        return httpFailed ?
          { kind: "rejected", path: change.path, reason: "HDHomeRun could not bind port " + String(CONFIG.hdhr.port) + " (in use); the previous port remains active." } :
          { kind: "applied", path: change.path };
      }

      case "hdhr.discoveryEnabled": {

        // Drives the UDP surface. udpFailed is only true when discovery was requested (enabled) and the bind failed; turning discovery off cannot fail.
        return udpFailed ?
          { kind: "rejected", path: change.path, reason: "HDHomeRun LAN discovery could not bind UDP port " + String(HDHR_DISCOVERY_PORT) + " (in use)." } :
          { kind: "applied", path: change.path };
      }

      case "hdhr.deviceId":
      case "hdhr.friendlyName": {

        // Read live from CONFIG by their consumers (discover.json, the UDP discover reply). The reconcile above needed no surface change for them; the new value
        // propagates on the next request.
        return { kind: "applied", path: change.path };
      }

      default: {

        // An unknown hdhr.* path means a new field was added to HdhrConfig without extending this mapping. Conservatively defer so the operator gets a restart.
        return { kind: "deferred", path: change.path, reason: "no live-apply rule for this HDHomeRun field" };
      }
    }
  });
}

// Module-load side effect: register the live-apply handler exactly once per process. ESM modules load at most once per process so this runs deterministically
// at boot - before app.ts's startHdhrServer call, before any settings save can fire, and before any test interaction. Tests that explicitly reset the registry
// via resetConfigChangeHandlers() can re-register by calling registerConfigChangeHandler("hdhr.", applyHdhrConfigChanges) themselves; both symbols are
// exported. Co-locating the registration here (rather than at startHdhrServer call time) keeps registry state and registration state from diverging.
registerConfigChangeHandler("hdhr.", applyHdhrConfigChanges);
