/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tunerState.ts: HDHomeRun tuner-slot state for PrismCast.
 *
 * Real HDHomeRun devices expose a fixed number of "tuner" slots indexed from zero. PrismCast does not have RF tuners but emulates the model: the configured
 * maxConcurrentStreams becomes the slot count, and each active stream binds to the next free slot in stream-ID order. Two HDHR consumers need the same slot-
 * indexed view of current state:
 *
 *   1. The HTTP /status.json endpoint serves it to monitoring dashboards (Home Assistant, Homepage) as a JSON array of HDHR-shaped tuner entries.
 *   2. The UDP control plane on port 65001 serves it to clients that issue Get requests for /tuner<N>/channel, /tuner<N>/status, /tuner<N>/target, etc.
 *
 * This module is the single source of truth for the slot-indexed projection. It returns format-neutral TunerState records; each consumer formats them into the
 * wire shape its protocol expects (JSON keys for HTTP, plain strings and space-delimited key=value pairs for UDP). Keeping the projection here means the
 * channel-map merge logic, the fallback-to-stream-name handling, and the client-address normalization live in one place, so an HDHR feature gap or a
 * refactor of the underlying registry surfaces in exactly one location.
 */
import { CONFIG } from "../config/index.ts";
import type { Nullable } from "../types/index.ts";
import { buildChannelMap } from "./channelMap.ts";
import { getAllStreams } from "../streaming/registry.ts";
import { normalizeClientAddress } from "../utils/index.ts";

/**
 * Slot-indexed projection of stream state. Every configured tuner slot produces one entry; active slots carry channel and client info while idle slots carry
 * only the resource name. Wire-format conversion (to JSON for HTTP, to plain strings or space-delimited key=value strings for UDP Get replies) happens in
 * the consumer.
 */
export interface TunerState {

  // True when an active stream is bound to this slot.
  readonly active: boolean;

  // Display name of the tuned channel, with fallback. Resolution order: channelMap.name (from the lineup), then stream.channelName (set at tune time, persists
  // when the channel is later removed from the map), then null when neither is known.
  readonly channelName: Nullable<string>;

  // Numeric channel number from the lineup. Null when the slot is idle or when the channel was removed from the lineup after the stream started.
  readonly channelNumber: Nullable<number>;

  // Normalized client IP address consuming this stream's MPEG-TS feed. Null when no client is attached (e.g., HLS-only consumption).
  readonly clientAddress: Nullable<string>;

  // The HDHR slot resource identifier ("tuner0", "tuner1", ...). Stable per slot index.
  readonly resource: string;

  // Slot index. Useful when consumers need to address a slot by integer (UDP Get keys carry the integer).
  readonly slot: number;
}

/**
 * Returns the current slot-indexed projection of stream state. Active streams (sorted by stream id) take the first N slots; remaining slots through the
 * configured maxConcurrentStreams are returned as idle entries. The array length is max(maxConcurrentStreams, activeStreams): in the normal case it equals
 * maxConcurrentStreams, but if the active stream count exceeds the configured limit - reachable by lowering maxConcurrentStreams while streams are running,
 * since live streams are not terminated by a limit change - every active stream is still reported and there are simply no idle entries. We never drop an
 * active tuner and never report fewer slots than there are active streams, because a real HDHomeRun cannot have fewer tuners than it has active recordings.
 * @returns Slot-indexed array of TunerState records.
 */
export function getTunerStates(): readonly TunerState[] {

  const channelMap = buildChannelMap();
  const channelByKey = new Map(channelMap.map((entry) => [ entry.key, entry ]));
  const streams = getAllStreams().toSorted((a, b) => a.id - b.id);

  // Slot count is the larger of the configured limit and the live active-stream count. Clamping to the configured limit would drop real active tuners from the
  // projection when the operator lowers maxConcurrentStreams mid-stream; taking the max keeps every active stream visible and lets the idle-fill loop below be
  // a no-op in the oversubscribed case rather than producing a negative range.
  const tunerCount = Math.max(CONFIG.streaming.maxConcurrentStreams, streams.length);
  const states: TunerState[] = [];

  // Active slots: one entry per running stream, in stream-id order. Channel info merges from the lineup with a fallback to stream.channelName.
  for(const [ slot, stream ] of streams.entries()) {

    const channelEntry = channelByKey.get(stream.info.storeKey);
    const channelName = channelEntry?.name ?? stream.channelName ?? null;
    const channelNumber = channelEntry ? channelEntry.number : null;
    const clientAddress = stream.clientAddress ? normalizeClientAddress(stream.clientAddress) : null;

    states.push({

      active: true,
      channelName,
      channelNumber,
      clientAddress,
      resource: "tuner" + String(slot),
      slot
    });
  }

  // Idle slots fill the remaining capacity up to tunerCount. When oversubscribed (active streams exceed the configured limit) tunerCount equals the active
  // count, so this loop runs zero times and the array is all-active - never a negative range, never a dropped tuner.
  for(let slot = streams.length; slot < tunerCount; slot++) {

    states.push({

      active: false,
      channelName: null,
      channelNumber: null,
      clientAddress: null,
      resource: "tuner" + String(slot),
      slot
    });
  }

  return states;
}
