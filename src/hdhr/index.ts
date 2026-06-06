/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: HDHomeRun emulation lifecycle for PrismCast.
 *
 * When HDHomeRun emulation is enabled, PrismCast runs two complementary surfaces. The HTTP server (this module + discover.ts) responds to /device.xml,
 * /discover.json, /lineup.json, /lineup_status.json, and /status.json - the surface clients consume once they have located PrismCast by IP and port. The UDP
 * responder (udp.ts) answers SiliconDust LAN-discovery broadcasts on port 65001 so Plex finds PrismCast automatically without a manual address paste. The two
 * surfaces are independent: an operator can disable LAN discovery while keeping HTTP HDHR running (multi-tenant boxes, environments with a real HDHomeRun
 * already on the network), or vice versa. Channels DVR also auto-discovers via the UDP responder but its discovery assumes port 80 for the HTTP control
 * plane, so the lineup fetch fails unless hdhr.port is set to 80; Channels DVR users typically add PrismCast manually as a Custom Channels source.
 *
 * This module additionally registers a config-change handler under the "hdhr." prefix so HDHomeRun-related settings can take effect without a server restart.
 * The handler partitions incoming changes by path and either applies them live (open/close UDP socket, rebind the HTTP port, read fresh CONFIG values) or
 * reports them as deferred so the routes layer schedules a service restart. Registration happens at module-load time as a top-level side effect: ESM modules
 * load exactly once per process, so the handler is in place before any settings-save can fire. Tests that explicitly reset the reactivity registry can re-
 * register by importing and invoking registerConfigChangeHandler("hdhr.", applyHdhrConfigChanges) directly - both symbols are exported.
 */
import type { ChangeOutcome, ConfigChange } from "../config/reactivity.ts";
import { HDHR_DISCOVERY_PORT, startHdhrUdp, stopHdhrUdp } from "./udp.ts";
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

// The HDHR HTTP server instance, used for graceful shutdown and live-toggle restarts.
let hdhrServer: Nullable<Server> = null;

/**
 * Brings the HDHomeRun emulation surfaces (HTTP + optional UDP discovery) into line with CONFIG.hdhr. Thin wrapper over reconcileHdhrSurfaces so the boot path
 * and the live-apply path share one code path - there is exactly one place that decides what "HDHR matching the configuration" means. Failures at any layer are
 * logged and treated as graceful degradation; the broader application continues to work even if HDHR emulation cannot bind.
 */
export async function startHdhrServer(): Promise<void> {

  await reconcileHdhrSurfaces();
}

/**
 * Stops the HDHomeRun emulation surfaces and waits for the underlying sockets to fully release. Awaiting close completion matters when the caller intends to
 * immediately rebind on the same port - skipping the await leaves a TIME_WAIT window during which a fresh bind would race against EADDRINUSE. Idempotent: safe
 * to call when no servers are running.
 */
export async function stopHdhrServer(): Promise<void> {

  await stopHdhrUdp();

  if(hdhrServer) {

    await closeHdhrHttpServer(hdhrServer);
    hdhrServer = null;
  }
}

/**
 * Closes an HDHomeRun HTTP server and resolves only after the underlying socket is fully released. Wrapped here because Server.close's callback fires after
 * the socket is gone, but the public API is callback-shaped; we surface it as a Promise so callers can compose with await.
 * @param server - The server instance to close.
 */
async function closeHdhrHttpServer(server: Server): Promise<void> {

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise, resolve } = Promise.withResolvers<void>();

  server.close(() => { resolve(); });

  return promise;
}

/**
 * Ensures CONFIG.hdhr.deviceId carries a checksum-valid value, generating and persisting a fresh one when missing or invalid. Called from startHdhrServer and
 * from the live-toggle handler when hdhr.enabled flips to true so a never-before-enabled HDHR setup gets a DeviceID on first activation rather than only at
 * process boot.
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
 * Binds the HDHomeRun HTTP server on the given port and reports whether the bind succeeded. Captures the bound server in the module-level hdhrServer so
 * stopHdhrServer can close it. Handles EADDRINUSE gracefully (logs, leaves hdhrServer null, returns false) so a port collision degrades rather than crashing -
 * and, crucially, communicates the failure to its caller so the live-apply path can report a rejected outcome instead of a false "applied".
 * @param port - The TCP port to bind.
 * @returns True if the server is now listening, false if the bind failed.
 */
async function startHttpServer(port: number): Promise<boolean> {

  const app = express();

  app.set("trust proxy", true);

  setupHdhrEndpoints(app);

  const { promise, resolve } = Promise.withResolvers<boolean>();
  const server = app.listen(port, CONFIG.server.host);

  /* Detect bind success or failure through the explicit "listening" and "error" events rather than express's listen callback. The listen callback fires even
   * when the bind fails (verified empirically: it resolves on EADDRINUSE and EADDRNOTAVAIL with a non-listening server), so it cannot be trusted to signal
   * success - relying on it is exactly why a port conflict used to be reported as a successful start. Exactly one of "listening" or "error" fires for a given
   * bind attempt, so the promise always settles. This mirrors the bind-vs-runtime error-handler split the UDP responder uses in udp.ts.
   */
  const onBindError = (error: NodeJS.ErrnoException): void => {

    if(error.code === "EADDRINUSE") {

      LOG.warn("HDHomeRun port %s is already in use. Check for conflicting services on this port.", port);
    } else {

      LOG.warn("Failed to start the HDHomeRun HTTP server: %s.", formatError(error));
    }

    resolve(false);
  };

  server.once("error", onBindError);

  server.once("listening", (): void => {

    // Bind succeeded: retire the bind-failure handler and attach a long-lived runtime handler so a later socket error is logged rather than crashing the
    // process on an unhandled "error" event.
    server.removeListener("error", onBindError);
    server.on("error", (error: NodeJS.ErrnoException): void => {

      LOG.warn("HDHomeRun HTTP server encountered a socket error: %s.", formatError(error));
    });

    hdhrServer = server;

    LOG.info("HDHomeRun emulation is now listening on %s:%s (DeviceID: %s).", CONFIG.server.host, port, CONFIG.hdhr.deviceId.toUpperCase());

    resolve(true);
  });

  return promise;
}

/**
 * Reconciles the HDHomeRun HTTP and UDP surfaces to match the current CONFIG.hdhr - the single source of truth for "what running state the configuration calls
 * for." Used by both boot (startHdhrServer) and live config-apply (applyHdhrConfigChanges), so neither path can drift from the other and there is no per-change
 * ordering to get wrong: the desired state is read whole from CONFIG and the surfaces are driven to match it. Returns which surfaces failed to reach the desired
 * state so the caller can translate a failure into a rejected outcome rather than a false "applied".
 * @returns Flags indicating whether the HTTP and/or UDP surface failed to reach its desired state.
 */
async function reconcileHdhrSurfaces(): Promise<{ httpFailed: boolean; udpFailed: boolean }> {

  // Desired state "disabled": tear both surfaces down. This cannot fail.
  if(!CONFIG.hdhr.enabled) {

    await stopHdhrServer();

    return { httpFailed: false, udpFailed: false };
  }

  await ensureDeviceId();

  const httpFailed = await reconcileHttpSurface();
  const udpFailed = await reconcileUdpSurface();

  return { httpFailed, udpFailed };
}

/**
 * Drives the HTTP surface to listen on CONFIG.hdhr.port. No-op when already bound to the desired port. On a port change whose new bind fails (EADDRINUSE), the
 * concession is to re-bind the prior port so the tuner stays alive while the change is reported as rejected - a typo'd port must not take down a working tuner.
 * @returns True if the surface failed to reach the desired port (the caller reports the change rejected), false on success.
 */
async function reconcileHttpSurface(): Promise<boolean> {

  const desiredPort = CONFIG.hdhr.port;
  const boundPort = hdhrServer ? (hdhrServer.address() as AddressInfo | null)?.port ?? null : null;

  // Already listening on the desired port - nothing to do (covers deviceId/friendlyName-only changes, which need no rebind).
  if(boundPort === desiredPort) {

    return false;
  }

  if(hdhrServer) {

    await closeHdhrHttpServer(hdhrServer);
    hdhrServer = null;
  }

  if(await startHttpServer(desiredPort)) {

    return false;
  }

  // The desired bind failed. Concession (operator-confirmed): if we were previously listening on another port, re-bind it so the tuner keeps working; the
  // change is still reported rejected so the operator knows it did not take. If there was no prior port (boot or already-down), there is nothing to restore.
  if(boundPort !== null) {

    LOG.warn("HDHomeRun port %s is unavailable; keeping the previous port %s active. The port change was rejected.", desiredPort, boundPort);

    await startHttpServer(boundPort);
  }

  return true;
}

/**
 * Drives the UDP discovery surface to match CONFIG.hdhr.discoveryEnabled. UDP is gated on the HTTP server actually being bound: a discovery responder must never
 * advertise a BaseURL pointing at an HTTP port with no listener, so when HTTP is down we stop UDP regardless of the discoveryEnabled flag. The discover reply
 * advertises the HTTP server's live bound port (not CONFIG.hdhr.port) so it always reflects reality, including the rejected-port-change case above.
 * @returns True if discovery was requested but failed to bind, false otherwise.
 */
async function reconcileUdpSurface(): Promise<boolean> {

  if(CONFIG.hdhr.discoveryEnabled && hdhrServer) {

    const bound = await startHdhrUdp({ httpPortProvider: () => (hdhrServer ? (hdhrServer.address() as AddressInfo | null)?.port ?? null : null) });

    return !bound;
  }

  await stopHdhrUdp();

  return false;
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

  const { httpFailed, udpFailed } = await reconcileHdhrSurfaces();

  return changes.map((change) => {

    switch(change.path) {

      case "hdhr.enabled":
      case "hdhr.port": {

        // These fields drive the HTTP surface. A failed bind (port in use) is surfaced as rejected; the concession in reconcileHttpSurface has already kept the
        // prior port alive, so "rejected" accurately means "the change you saved did not take, but the tuner is still running on its previous configuration."
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
// exported. Co-locating the registration here (rather than at startHdhrServer call time) keeps registry state and registration state from diverging - the
// previous flag-guarded version could lie about being registered after a reset.
registerConfigChangeHandler("hdhr.", applyHdhrConfigChanges);
