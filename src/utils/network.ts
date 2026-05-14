/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * network.ts: Network address utilities for PrismCast.
 */

/* Helpers for normalizing and inspecting the network addresses PrismCast exchanges with clients. The functions here operate on raw address strings as supplied by
 * Node and Express - they do not parse URLs or query DNS, so the rest of the codebase has a single, allocation-free seam for "give me a canonical client address"
 * decisions without three sites carrying their own copies of the same one-liner.
 */

// The IPv6-mapped IPv4 prefix that Node and Express emit when a dual-stack listener serves an IPv4 client. Stripping it produces the human-recognizable IPv4
// form that Channels DVR clients log and that HDHomeRun's TargetIP field is expected to carry.
const IPV4_MAPPED_IPV6_PREFIX = "::ffff:";

/**
 * Normalizes a client address by stripping the IPv6-mapped IPv4 prefix when present. Express, the Node HTTP server, and the WebSocket layer all surface the same
 * 192.168.1.50 client as "::ffff:192.168.1.50" on dual-stack sockets and as "192.168.1.50" on pure-IPv4 sockets; without normalization, the same client appears
 * twice in client-tracking maps and HDHomeRun TargetIP fields look unfamiliar to downstream tools that expect the bare IPv4 form.
 * @param address - The raw client address string.
 * @returns The canonical client address - IPv6 addresses pass through unchanged; IPv4 addresses are returned without the IPv6 mapping prefix.
 */
export function normalizeClientAddress(address: string): string {

  return address.startsWith(IPV4_MAPPED_IPV6_PREFIX) ? address.slice(IPV4_MAPPED_IPV6_PREFIX.length) : address;
}
