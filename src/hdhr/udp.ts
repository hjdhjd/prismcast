/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * udp.ts: HDHomeRun UDP discovery and control transport for PrismCast.
 *
 * Plex locates HDHomeRun devices on the LAN by broadcasting a Discover request to UDP port 65001 and consuming the unicast replies. The same protocol surface
 * can answer Get requests against this port for device and tuner state. This module binds the responder socket, dispatches incoming packets through the pure
 * protocol codec and getHandlers dispatch table, and composes replies whose BaseURL field points at the correct LAN-reachable interface address for the
 * requesting client. The pure logic lives elsewhere; this module owns sockets, IP selection, and CONFIG read paths. Other HDHR-aware clients can speak the
 * same protocol; their compatibility is incidental rather than supported (see hdhr/index.ts for the Channels DVR port-80 caveat in particular).
 *
 * Lifecycle is controlled from hdhr/index.ts: startHdhrUdp is invoked after the HTTP HDHR server is listening (so the BaseURL the reply advertises is actually
 * live), stopHdhrUdp is invoked during graceful shutdown and during live-toggle of hdhr.discoveryEnabled. A bind collision on port 65001 (real HDHR or another
 * emulator on the same host) is logged at warn level and treated as graceful "discovery not available" rather than a startup failure - the HTTP HDHR surface
 * keeps working in that scenario, only LAN auto-detect is lost.
 */
import { LOG, isCategoryEnabled } from "../utils/index.ts";
import type { NetworkInterfaceInfo, NetworkInterfaceInfoIPv4 } from "node:os";
import { PACKET_UPGRADE_REQUEST, buildDiscoverReply, buildErrorReply, buildGetReply, parsePacket } from "./protocol.ts";
import { CONFIG } from "../config/index.ts";
import { HDHR_DEVICE_TYPE_TUNER } from "./identity.ts";
import type { Nullable } from "../types/index.ts";
import type { Socket } from "node:dgram";
import { createSocket } from "node:dgram";
import { formatError } from "../utils/errors.ts";
import { getPackageVersion } from "../utils/index.ts";
import { getTunerStates } from "./tunerState.ts";
import { networkInterfaces } from "node:os";
import { resolveGet } from "./getHandlers.ts";

// Standard HDHomeRun discovery port. All HDHR-aware clients hard-code this value, so it is not configurable.
export const HDHR_DISCOVERY_PORT = 65001;

// The active responder socket, captured at module level so stopHdhrUdp can close it during shutdown or live-toggle. Null when no socket is bound.
let hdhrSocket: Nullable<Socket> = null;

// Provider for the HTTP server's live bound port, injected by the HDHR lifecycle layer at start time. The Discover reply advertises this (not CONFIG.hdhr.port)
// so the BaseURL always reflects where the HTTP server is actually listening - which matters in the rejected-port-change case, where HTTP stays on the prior
// port while CONFIG holds the new one. Null until startHdhrUdp is called with a provider; falls back to CONFIG.hdhr.port when absent (e.g. in transport tests).
let httpPortProvider: Nullable<() => Nullable<number>> = null;

/**
 * Options for startHdhrUdp. Tests pass an ephemeral port (0) so the kernel picks a free port; production passes the httpPortProvider so Discover replies
 * advertise the real bound HTTP port.
 */
export interface HdhrUdpOptions {

  // Address to bind the socket on. Defaults to "0.0.0.0" so the socket receives subnet broadcasts on every interface.
  readonly bindAddress?: string;

  // Accessor for the HTTP server's live bound port, used to build the Discover reply's BaseURL. When absent, the reply falls back to CONFIG.hdhr.port.
  readonly httpPortProvider?: () => Nullable<number>;

  // UDP port to bind. Defaults to HDHR_DISCOVERY_PORT. Tests pass 0 for an ephemeral port.
  readonly port?: number;
}

/**
 * Binds the UDP responder and returns true on success. Returns false (without throwing) when the bind fails - typically EADDRINUSE because a real HDHR or
 * another emulator owns port 65001 on the same host. The HTTP HDHR server continues to work in that case; only LAN auto-detect is lost.
 *
 * Idempotent: calling startHdhrUdp twice without an intervening stop is a no-op the second time. Safe to invoke from both initial startup and live config-
 * change application.
 * @param options - Optional bind overrides for tests.
 * @returns true when the socket is bound and listening, false on graceful failure.
 */
export async function startHdhrUdp(options?: HdhrUdpOptions): Promise<boolean> {

  if(hdhrSocket !== null) {

    return true;
  }

  // Capture the HTTP-port provider for the Discover reply's BaseURL. Set before the bind attempt; harmless if the bind then fails (no socket -> no packets).
  httpPortProvider = options?.httpPortProvider ?? null;

  const bindAddress = options?.bindAddress ?? "0.0.0.0";
  const port = options?.port ?? HDHR_DISCOVERY_PORT;
  const socket = createSocket({ reuseAddr: false, type: "udp4" });

  socket.on("message", (msg, rinfo) => {

    // Defensive: catch handler errors so a malformed packet from a buggy client cannot tear down the socket.
    try {

      handlePacket(socket, msg, rinfo);
    } catch(error) {

      LOG.debug("hdhr", "UDP responder swallowed handler error.", { error: formatError(error) });
    }
  });

  const { promise, resolve } = Promise.withResolvers<boolean>();

  // Two error handlers, attached at different phases of the socket lifecycle. The bind-failure handler is short-lived: it runs once if bind fails (typically
  // EADDRINUSE), resolves the bind promise with false, and is removed on bind success. The runtime-error handler is long-lived: it runs for any socket error
  // that occurs after a successful bind, clears the module-level reference so a subsequent startHdhrUdp can rebind cleanly, and closes the socket. Splitting
  // the two paths prevents the once-handler from misclassifying a post-bind error as a bind failure (with a misleading "port is already in use" log line)
  // and keeps hdhrSocket from pointing at a closed socket.
  const bindFailureHandler = (error: NodeJS.ErrnoException): void => {

    if(error.code === "EADDRINUSE") {

      LOG.warn("HDHomeRun LAN discovery port %d is already in use. Discovery is disabled; clients can still add PrismCast manually by IP.", port);
    } else {

      LOG.warn("HDHomeRun LAN discovery failed to start: %s. Clients can still add PrismCast manually by IP.", formatError(error));
    }

    socket.close();
    resolve(false);
  };

  const runtimeErrorHandler = (error: NodeJS.ErrnoException): void => {

    LOG.warn("HDHomeRun LAN discovery encountered a socket error and is now disabled: %s.", formatError(error));

    if(hdhrSocket === socket) {

      hdhrSocket = null;
    }

    socket.close();
  };

  socket.once("error", bindFailureHandler);

  socket.bind(port, bindAddress, () => {

    const address = socket.address();

    hdhrSocket = socket;
    socket.removeListener("error", bindFailureHandler);
    socket.on("error", runtimeErrorHandler);
    LOG.info("HDHomeRun LAN discovery is now responding on UDP %s:%d.", bindAddress, address.port);
    resolve(true);
  });

  return promise;
}

/**
 * Closes the responder socket and resolves only after the underlying socket is fully released. Awaiting close completion matters when the caller intends to
 * immediately rebind on the same port - dgram's close callback fires after release, and skipping the await would race a fresh bind against the kernel's
 * release ordering. Safe to call when no socket is bound (the function is a no-op in that case).
 */
export async function stopHdhrUdp(): Promise<void> {

  if(hdhrSocket === null) {

    return;
  }

  const socket = hdhrSocket;
  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise, resolve } = Promise.withResolvers<void>();

  hdhrSocket = null;

  socket.close(() => {

    LOG.info("HDHomeRun LAN discovery is now stopped.");
    resolve();
  });

  return promise;
}

/**
 * Returns the port the responder socket is bound to, or null when no socket is bound. Primarily a test seam - tests use an ephemeral port (0) so the kernel
 * picks a free port and the test needs to discover what it picked to address its client request. Production code uses the fixed HDHR_DISCOVERY_PORT and never
 * needs to introspect the socket.
 */
export function getBoundHdhrUdpPort(): Nullable<number> {

  return hdhrSocket ? hdhrSocket.address().port : null;
}

/**
 * Handles a single incoming UDP packet. Parses it through the pure protocol codec, dispatches by packet type, and sends the appropriate reply (or drops the
 * packet silently when it cannot or should not be answered). Pulled out of the socket.on("message", ...) inline body so the dispatch table reads cleanly.
 * @param socket - The bound UDP socket, used to send replies.
 * @param msg - The raw datagram bytes.
 * @param rinfo - The sender's address info, used both to address the reply and to pick a LAN-reachable BaseURL.
 */
function handlePacket(socket: Socket, msg: Buffer, rinfo: { address: string; port: number }): void {

  const parsed = parsePacket(msg);

  if(parsed === null) {

    LOG.debug("hdhr", "UDP responder dropped a malformed or CRC-mismatched packet.", { from: rinfo.address });

    return;
  }

  switch(parsed.type) {

    case "discover": {

      // Advertise the HTTP server's live bound port when known, falling back to CONFIG.hdhr.port. The provider reflects reality even when a rejected port
      // change has left HTTP on its prior port while CONFIG holds the new value.
      const advertisedPort = httpPortProvider?.() ?? CONFIG.hdhr.port;
      const baseUrl = "http://" + selectLanAddress(rinfo.address) + ":" + String(advertisedPort);
      const reply = buildDiscoverReply({

        baseUrl,
        deviceId: parseDeviceId(CONFIG.hdhr.deviceId),
        deviceType: HDHR_DEVICE_TYPE_TUNER,
        tunerCount: CONFIG.streaming.maxConcurrentStreams
      });

      sendReply(socket, reply, rinfo);
      logDispatch("discover", rinfo, { baseUrl });

      break;
    }

    case "get": {

      const result = resolveGet(parsed.name, { runtimeVersion: getPackageVersion(), tuners: getTunerStates() });
      const reply = (result.kind === "value") ? buildGetReply(parsed.name, result.value) : buildErrorReply(parsed.name, result.error);

      sendReply(socket, reply, rinfo);
      logDispatch("get", rinfo, { kind: result.kind, name: parsed.name });

      break;
    }

    case "set": {

      // PrismCast's emulation model is HTTP-streaming; clients tune by fetching the lineup URL. UDP Set requests are a legacy RTP-streaming control surface we
      // do not implement, but the wire convention is to reply with an explicit error rather than silently drop so the client does not block waiting for ACK.
      // The wire packet type for Set is PACKET_GET_REQUEST (0x0004) - the protocol uses the same code for Get and Set, distinguished by the value TLV.
      const reply = buildErrorReply(parsed.name, "ERROR: write protected");

      sendReply(socket, reply, rinfo);
      logDispatch("set", rinfo, { name: parsed.name });

      break;
    }

    case "unsupported": {

      // Upgrade requests and unknown packet types are dropped silently. Real HDHomeRun firmware does the same for Upgrade traffic addressed to an HDTC-2US
      // (which has no upgrade endpoint); mimicking the behavior avoids surfacing "unsupported" reply spam in client logs.
      logDispatch("unsupported", rinfo, { packetType: parsed.packetType, upgradeRequest: (parsed.packetType === PACKET_UPGRADE_REQUEST) });

      break;
    }
  }
}

/**
 * Sends a pre-built reply packet back to the requester. Wrapped so the send error handling lives in one place; UDP send is fire-and-forget but we still want
 * to surface failures at debug level for diagnostics.
 * @param socket - The bound UDP socket.
 * @param packet - The wire-formatted reply bytes.
 * @param rinfo - The destination address.
 */
function sendReply(socket: Socket, packet: Buffer, rinfo: { address: string; port: number }): void {

  socket.send(packet, rinfo.port, rinfo.address, (error) => {

    if(error) {

      LOG.debug("hdhr", "UDP responder failed to send a reply.", { error: formatError(error), to: rinfo.address });
    }
  });
}

/**
 * Selects the local IPv4 address that is reachable from the requester's subnet. Walks os.networkInterfaces, computes each interface's network address via its
 * netmask, and returns the matching interface's address. Falls back to the first non-loopback IPv4 address when no subnet matches (typical on hosts whose
 * routing crosses a layer-3 boundary). Returns "127.0.0.1" as the last resort - clients reaching us on loopback will use it; clients on the LAN that hit this
 * branch will fail to reach back, but that scenario implies a misconfigured host where LAN advertising would not help anyway.
 *
 * Exposed (and parameterized over network interfaces) primarily for tests; the production call site uses os.networkInterfaces() directly via the default
 * parameter.
 * @param targetAddress - The requester's IPv4 address (from dgram rinfo).
 * @param interfaces - Map of interface name to NetworkInterfaceInfo array (defaults to os.networkInterfaces()).
 * @returns The selected local IPv4 address.
 */
export function selectLanAddress(targetAddress: string, interfaces: NodeJS.Dict<NetworkInterfaceInfo[]> = networkInterfaces()): string {

  const ipv4Interfaces: NetworkInterfaceInfoIPv4[] = [];

  for(const list of Object.values(interfaces)) {

    if(!list) {

      continue;
    }

    for(const iface of list) {

      if((iface.family === "IPv4") && !iface.internal) {

        ipv4Interfaces.push(iface);
      }
    }
  }

  const targetInt = ipv4ToInt(targetAddress);

  if(targetInt !== null) {

    for(const iface of ipv4Interfaces) {

      const maskInt = ipv4ToInt(iface.netmask);
      const ifaceInt = ipv4ToInt(iface.address);

      if((maskInt === null) || (ifaceInt === null)) {

        continue;
      }

      // Subnet match: both addresses agree under the interface's netmask.
      if((targetInt & maskInt) === (ifaceInt & maskInt)) {

        return iface.address;
      }
    }
  }

  // No subnet match. Fall back to the first non-loopback IPv4 address so the BaseURL is at least plausible.
  if(ipv4Interfaces.length > 0) {

    return ipv4Interfaces[0]?.address ?? "127.0.0.1";
  }

  return "127.0.0.1";
}

/**
 * Converts a dotted IPv4 string to a 32-bit unsigned integer. Returns null when the input is malformed (not four octets, octet out of range). Internal to this
 * module so the bounds checking does not leak into callers.
 * @param ip - The IPv4 address.
 * @returns The 32-bit value or null.
 */
function ipv4ToInt(ip: string): Nullable<number> {

  const parts = ip.split(".");

  if(parts.length !== 4) {

    return null;
  }

  let result = 0;

  for(const part of parts) {

    const n = Number(part);

    if(!Number.isInteger(n) || (n < 0) || (n > 255)) {

      return null;
    }

    result = ((result << 8) | n) >>> 0;
  }

  return result;
}

/**
 * Converts the 8-hex-character device id string from CONFIG.hdhr.deviceId into the 32-bit unsigned integer the wire protocol expects. Falls back to zero for
 * invalid input so a hand-edited config does not crash the responder; the integrity check at startup catches the underlying error separately.
 * @param hex - The 8-character hex string from CONFIG.
 * @returns The 32-bit value.
 */
function parseDeviceId(hex: string): number {

  const value = parseInt(hex, 16);

  return Number.isNaN(value) ? 0 : (value >>> 0);
}

/**
 * Logs a dispatch event at debug level. Skipped entirely when the "hdhr" debug category is disabled so the hot path stays branch-light.
 * @param kind - Short label identifying the dispatched packet type.
 * @param rinfo - Sender info.
 * @param extra - Additional structured context.
 */
function logDispatch(kind: string, rinfo: { address: string; port: number }, extra: Record<string, unknown>): void {

  if(!isCategoryEnabled("hdhr")) {

    return;
  }

  LOG.debug("hdhr", "UDP responder dispatched packet.", { from: rinfo.address, kind, ...extra });
}
