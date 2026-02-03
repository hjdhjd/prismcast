/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clients.ts: Client tracking for PrismCast streams.
 */
import { getStream } from "./registry.js";

/*
 * CLIENT TRACKING
 *
 * This module tracks which clients are connected to each stream by protocol (HLS or MPEG-TS). MPEG-TS clients have persistent connections and are registered on
 * connect, unregistered on disconnect. HLS clients are stateless — each playlist request refreshes a TTL-based entry that expires after 30 seconds of inactivity. The
 * monitor queries client summaries every ~2 seconds to include them in SSE status updates for the UI.
 *
 * This module is intentionally separate from the stream registry to keep StreamRegistryEntry focused on capture state. The separation follows the same pattern as
 * channelToStreamId in lifecycle.ts — a lookup index maintained alongside the registry but owned by a different module.
 */

// Types.

/**
 * Client type classification by protocol.
 */
export type ClientType = "hls" | "mpegts";

/**
 * A tracked client connected to a stream.
 */
interface StreamClient {

  // Normalized client IP address (IPv6-mapped IPv4 prefix stripped).
  clientAddress: string;

  // Timestamp of last request, used for TTL expiration of HLS clients.
  lastSeen: number;

  // Protocol used to connect to the stream.
  protocol: ClientType;
}

/**
 * Count of clients for a specific client type.
 */
export interface ClientTypeCount {

  // Number of clients of this type.
  count: number;

  // The client type.
  type: ClientType;
}

/**
 * Aggregated client summary for a stream.
 */
export interface ClientSummary {

  // Per-type breakdown, sorted alphabetically by type.
  clients: ClientTypeCount[];

  // Total number of active clients.
  total: number;
}

// Constants.

// HLS clients that have not sent a playlist request within this window are considered disconnected and removed during the next summary query.
const HLS_CLIENT_TTL = 30000;

// State.

// Per-stream client maps. Outer key is numeric stream ID, inner key is a composite string "protocol:address".
const clientMaps = new Map<number, Map<string, StreamClient>>();

// Registration.

/**
 * Normalizes an IP address by stripping the IPv6-mapped IPv4 prefix. This prevents the same client from appearing twice when Express reports the address as
 * "::ffff:192.168.1.50" on some requests and "192.168.1.50" on others.
 * @param address - The raw IP address string.
 * @returns The normalized address.
 */
function normalizeAddress(address: string): string {

  return address.startsWith("::ffff:") ? address.slice(7) : address;
}

/**
 * Registers or refreshes a client for a stream. For HLS clients, this updates the lastSeen timestamp to prevent TTL expiration. For MPEG-TS clients, this creates
 * the client entry on connect. Guards against registration after stream termination by verifying the stream still exists in the registry.
 * @param streamId - The numeric stream ID.
 * @param clientAddress - The raw client IP address.
 * @param protocol - The protocol ("hls" or "mpegts").
 */
export function registerClient(streamId: number, clientAddress: string, protocol: ClientType): void {

  // Guard against registration after stream termination. If the stream no longer exists in the registry, skip silently to prevent orphaned client entries.
  if(!getStream(streamId)) {

    return;
  }

  const address = normalizeAddress(clientAddress);
  const key = protocol + ":" + address;

  let clients = clientMaps.get(streamId);

  if(!clients) {

    clients = new Map();
    clientMaps.set(streamId, clients);
  }

  clients.set(key, {

    clientAddress: address,
    lastSeen: Date.now(),
    protocol
  });
}

/**
 * Unregisters a client from a stream. Used for MPEG-TS clients on disconnect. HLS clients are not explicitly unregistered — they expire via TTL cleanup in
 * getClientSummary(). If the client map has already been cleared (e.g., during stream termination), this is a no-op.
 * @param streamId - The numeric stream ID.
 * @param clientAddress - The raw client IP address.
 * @param protocol - The protocol ("hls" or "mpegts").
 */
export function unregisterClient(streamId: number, clientAddress: string, protocol: ClientType): void {

  const clients = clientMaps.get(streamId);

  if(!clients) {

    return;
  }

  const address = normalizeAddress(clientAddress);
  const key = protocol + ":" + address;

  clients.delete(key);

  // Clean up empty maps to prevent memory accumulation from streams with no remaining clients.
  if(clients.size === 0) {

    clientMaps.delete(streamId);
  }
}

// Summary.

/**
 * Returns an aggregated client summary for a stream. Performs lazy TTL cleanup of stale HLS clients as a side effect — entries that have not been refreshed within
 * HLS_CLIENT_TTL are removed. MPEG-TS clients are managed explicitly via register/unregister and are never expired by TTL.
 *
 * Called by the monitor every ~2 seconds during status emission.
 * @param streamId - The numeric stream ID.
 * @returns The client summary with total count and per-type breakdown.
 */
export function getClientSummary(streamId: number): ClientSummary {

  const clients = clientMaps.get(streamId);

  if(!clients) {

    return { clients: [], total: 0 };
  }

  // Expire stale HLS clients.
  const now = Date.now();

  for(const [ key, client ] of clients) {

    if((client.protocol === "hls") && ((now - client.lastSeen) > HLS_CLIENT_TTL)) {

      clients.delete(key);
    }
  }

  // Clean up empty maps.
  if(clients.size === 0) {

    clientMaps.delete(streamId);

    return { clients: [], total: 0 };
  }

  // Aggregate by protocol.
  const typeCounts = new Map<ClientType, number>();

  for(const client of clients.values()) {

    typeCounts.set(client.protocol, (typeCounts.get(client.protocol) ?? 0) + 1);
  }

  // Build sorted result.
  const result: ClientTypeCount[] = [];

  for(const [ type, count ] of typeCounts) {

    result.push({ count, type });
  }

  result.sort((a, b) => a.type.localeCompare(b.type));

  return { clients: result, total: clients.size };
}

// Cleanup.

/**
 * Removes all client tracking data for a stream. Called during stream termination to prevent memory leaks.
 * @param streamId - The numeric stream ID.
 */
export function clearClients(streamId: number): void {

  clientMaps.delete(streamId);
}
