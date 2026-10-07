/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * statusEmitter.ts: Event emitter for real-time stream and system status via SSE.
 */
import type { Nullable, StreamingMode } from "../types/index.ts";
import { CONFIG } from "../config/index.ts";
import type { ClientTypeCount } from "./clients.ts";
import { EventEmitter } from "node:events";
import { isDeepStrictEqual } from "node:util";

/* These interfaces define the structure of status updates sent to SSE clients. StreamStatus contains per-stream health information, while SystemStatus contains
 * overall system health.
 */

/**
 * Health classification for a stream based on its current state.
 */
export type StreamHealthStatus = "buffering" | "error" | "healthy" | "recovering" | "stalled";

/**
 * Detailed status information for a single stream.
 */
export interface StreamStatus {

  bufferingDuration: Nullable<number>;
  captureCodec: Nullable<string>;

  // The frame size capture encodes at, as "WIDTHxHEIGHT". Null for a native stream, whose nativeResolution is both its source and its output.
  captureResolution: Nullable<string>;
  channel: Nullable<string>;
  clientCount: number;
  clients: ClientTypeCount[];
  currentTime: number;
  duration: number;
  escalationLevel: number;
  hardwareAccelerated: boolean;
  health: StreamHealthStatus;
  id: number;
  lastIssueTime: Nullable<number>;
  lastIssueType: Nullable<string>;
  lastRecoveryTime: Nullable<number>;
  logoUrl: string;
  memoryBytes: number;
  nativeBandwidth: number;
  nativeResolution: Nullable<string>;
  networkState: number;
  pageReloadsInWindow: number;
  readyState: number;
  recoveryAttempts: number;
  serviceName: string;
  showName: string;

  // The intrinsic size of the page's video element as the monitor last read it, as "WIDTHxHEIGHT". Null until the first reading with non-zero dimensions, and
  // null for a native stream, which has no page video to measure.
  sourceResolution: Nullable<string>;
  startTime: string;
  streamingMode: StreamingMode;
  url: string;
}

/**
 * System-wide status information.
 */
export interface SystemStatus {

  browser: {

    // True while the browser is waiting to relaunch because it can no longer start captures. Its running captures continue and new stream requests receive a 503
    // back-off until the relaunch completes.
    captureImpaired: boolean;

    connected: boolean;
    pageCount: number;
  };
  memory: {

    heapUsed: number;
    rss: number;
  };
  streams: {

    active: number;
    limit: number;
  };
  uptime: number;
}

/**
 * The fields of the system status the page header renders: the browser's connection and its relaunch mark, the active stream count, and the stream limit. This
 * is the one statement of what the header renders. The client's system summary is this type and the status dedupe compares exactly these fields, so a field the
 * header starts rendering joins the comparison by joining this type. The page count, the memory figures and the uptime stay out because no client renders them,
 * so a status that differs only in them wakes no subscriber.
 */
export interface RenderedSystemStatus {

  // Whether the browser is connected, and whether it is waiting to relaunch because it can no longer start captures.
  readonly browser: Readonly<Pick<SystemStatus["browser"], "captureImpaired" | "connected">>;

  // The active stream count and the stream limit the header shows beside it.
  readonly streams: Readonly<Pick<SystemStatus["streams"], "active" | "limit">>;
}

/**
 * Initial snapshot sent when an SSE client connects. The channel table catch-up patch is composed at the route layer (routes/streams.ts) - this snapshot covers
 * only the stream and system state that statusEmitter owns.
 */
export interface StatusSnapshot {

  streams: StreamStatus[];
  system: SystemStatus;
}

/**
 * The full set of SSE wire event types. This is broader than the events this emitter dispatches... "snapshot" originates from the route layer (routes/streams.ts)
 * rather than from the in-process status emitter, so it appears in the union but is absent from StatusEmitterEventMap.
 */
export type StatusEventType = "channelUpdate" | "snapshot" | "streamAdded" | "streamHealthChanged" | "streamRemoved" | "systemStatusChanged";

/**
 * Typed event map for status notifications. Ensures event names and argument types are checked at compile time. The channelUpdate payload is intentionally
 * opaque - statusEmitter is a transport, and the patch shape is owned by routes/config/channels/healthBridge.ts (sender) and channelTable.applyPatch (receiver).
 */
interface StatusEmitterEventMap {

  channelUpdate: [patch: unknown];
  streamAdded: [status: StreamStatus];
  streamHealthChanged: [status: StreamStatus];
  streamRemoved: [info: { id: number }];
  systemStatusChanged: [status: SystemStatus];
}

/**
 * Typed EventEmitter for status notifications. Narrows Node's untyped EventEmitter to only accept the events defined in StatusEmitterEventMap.
 */
interface StatusEmitter extends EventEmitter {

  emit<K extends keyof StatusEmitterEventMap>(event: K, ...args: StatusEmitterEventMap[K]): boolean;
  off<K extends keyof StatusEmitterEventMap>(event: K, listener: (...args: StatusEmitterEventMap[K]) => void): this;
  on<K extends keyof StatusEmitterEventMap>(event: K, listener: (...args: StatusEmitterEventMap[K]) => void): this;
}

/* A singleton EventEmitter that broadcasts status updates to all subscribed SSE clients. The emitter maintains current state for all streams, allowing new clients
 * to receive a snapshot of current status immediately upon connecting.
 */

const statusEmitter = new EventEmitter() as StatusEmitter;

// Increase the default listener limit to support many concurrent SSE connections.
statusEmitter.setMaxListeners(100);

/**
 * Creates the initial StreamStatus object for a new stream. This provides a consistent starting state with all health metrics at their default values.
 * @param options - The stream initialization options.
 * @returns The initial stream status.
 */
export function createInitialStreamStatus(options: {
  captureCodec?: Nullable<string>;
  channelName: Nullable<string>;
  hardwareAccelerated?: boolean;
  logoUrl?: string;
  numericStreamId: number;
  serviceName: string;
  startTime: number;
  streamingMode?: StreamingMode;
  url: string;
}): StreamStatus {

  return {

    bufferingDuration: null,
    captureCodec: options.captureCodec ?? null,
    captureResolution: null,
    channel: options.channelName,
    clientCount: 0,
    clients: [],
    currentTime: 0,
    duration: 0,
    escalationLevel: 0,
    hardwareAccelerated: options.hardwareAccelerated ?? false,
    health: "healthy",
    id: options.numericStreamId,
    lastIssueTime: null,
    lastIssueType: null,
    lastRecoveryTime: null,
    logoUrl: options.logoUrl ?? "",
    memoryBytes: 0,
    nativeBandwidth: 0,
    nativeResolution: null,
    networkState: 0,
    pageReloadsInWindow: 0,
    readyState: 0,
    recoveryAttempts: 0,
    serviceName: options.serviceName,
    showName: "",
    sourceResolution: null,
    startTime: new Date(options.startTime).toISOString(),
    streamingMode: options.streamingMode ?? "capture",
    url: options.url
  };
}

// Current status for all active streams, keyed by stream ID.
const streamStatuses = new Map<number, StreamStatus>();

// The last system status the dedupe let through, which a connecting client's snapshot carries.
let cachedSystemStatus: Nullable<SystemStatus> = null;

/**
 * Emits a stream added event when a new stream starts.
 * @param status - The initial status of the new stream.
 */
export function emitStreamAdded(status: StreamStatus): void {

  streamStatuses.set(status.id, status);
  statusEmitter.emit("streamAdded", status);
}

/**
 * Emits a stream removed event when a stream ends.
 * @param streamId - The ID of the stream that ended.
 */
export function emitStreamRemoved(streamId: number): void {

  streamStatuses.delete(streamId);
  statusEmitter.emit("streamRemoved", { id: streamId });
}

/**
 * Emits a stream health changed event with the current stream status. Stores and emits the status to ensure SSE clients and snapshots have current data. Silently
 * drops updates for streams that have already been removed by emitStreamRemoved() to prevent zombie entries. During healthy playback the monitor calls this every
 * ~2 seconds anyway, so emitting unconditionally rather than filtering by health-state change has negligible bandwidth impact while eliminating staleness during
 * recovery/buffering periods.
 * @param status - The updated stream status.
 */
export function emitStreamHealthChanged(status: StreamStatus): void {

  // Defense-in-depth: do not re-add a stream that has already been removed by emitStreamRemoved(). This guards against any future code path that might attempt to
  // update a terminated stream's status.
  if(!streamStatuses.has(status.id)) {

    return;
  }

  streamStatuses.set(status.id, status);
  statusEmitter.emit("streamHealthChanged", status);
}

/**
 * Projects a system status onto the fields the page header renders. The return type is RenderedSystemStatus, so a field added to that type cannot compile until
 * this projection fills it.
 * @param status - The system status.
 * @returns The fields the header renders.
 */
function projectRenderedStatus(status: SystemStatus): RenderedSystemStatus {

  return {

    browser: { captureImpaired: status.browser.captureImpaired, connected: status.browser.connected },
    streams: { active: status.streams.active, limit: status.streams.limit }
  };
}

/**
 * Reports whether a status differs from the cached one in anything the page header renders, comparing their projections so the comparison reads every field the
 * projection carries. It takes a non-nullable previous so the emit's own null case stays a plain guard rather than a chain of optional accesses.
 * @param previous - The status already cached.
 * @param next - The status about to replace it.
 * @returns True when the rendered state differs.
 */
function isRenderedStateChanged(previous: SystemStatus, next: SystemStatus): boolean {

  return !isDeepStrictEqual(projectRenderedStatus(previous), projectRenderedStatus(next));
}

/**
 * Emits a system status changed event when the status changes a field the page header renders, caching it for the snapshot a connecting client receives.
 * @param status - The updated system status.
 */
export function emitSystemStatusChanged(status: SystemStatus): void {

  // The first emit has no cache to compare against, so it always fires; after that only a change to a field the header renders is worth waking every subscriber
  // for, which keeps a status that differs only in what no client renders off the wire.
  const previous = cachedSystemStatus;

  if(!previous || isRenderedStateChanged(previous, status)) {

    cachedSystemStatus = status;
    statusEmitter.emit("systemStatusChanged", status);
  }
}

/**
 * Gets the current status snapshot for all streams and the system.
 * @returns The current status snapshot.
 */
export function getStatusSnapshot(): StatusSnapshot {

  return {

    streams: Array.from(streamStatuses.values()),
    system: cachedSystemStatus ?? {

      browser: { captureImpaired: false, connected: false, pageCount: 0 },
      memory: { heapUsed: 0, rss: 0 },
      streams: { active: 0, limit: CONFIG.streaming.maxConcurrentStreams },
      uptime: 0
    }
  };
}

/**
 * Gets the current status for a specific stream.
 * @param streamId - The ID of the stream.
 * @returns The stream status, or undefined if not found.
 */
export function getStreamStatus(streamId: number): StreamStatus | undefined {

  return streamStatuses.get(streamId);
}

/**
 * Removes a stream from the status tracking without emitting an event. Used during cleanup when the stream has already been removed.
 * @param streamId - The ID of the stream to remove.
 */
export function removeStreamStatus(streamId: number): void {

  streamStatuses.delete(streamId);
}

/**
 * Emits a channel table update event. The payload is a partial patch - any combination of rows, counts, scopeCounts, and logos. SSE clients that have the
 * channels tab open apply the patch via channelTable.applyPatch. Used for server-initiated updates like logo population that have no associated client request.
 * @param patch - The partial channel table patch to emit.
 */
export function emitChannelUpdate(patch: unknown): void {

  statusEmitter.emit("channelUpdate", patch);
}

/**
 * Subscribes a callback to receive all status events. Returns an unsubscribe function.
 * @param callback - Function to call when a status event is emitted.
 * @returns A function to unsubscribe the callback.
 */
export function subscribeToStatus(
  callback: (event: StatusEventType, data: unknown) => void
): () => void {

  const channelUpdateHandler = (data: unknown): void => { callback("channelUpdate", data); };
  const streamAddedHandler = (data: StreamStatus): void => { callback("streamAdded", data); };
  const streamRemovedHandler = (data: { id: number }): void => { callback("streamRemoved", data); };
  const streamHealthChangedHandler = (data: StreamStatus): void => { callback("streamHealthChanged", data); };
  const systemStatusChangedHandler = (data: SystemStatus): void => { callback("systemStatusChanged", data); };

  statusEmitter.on("channelUpdate", channelUpdateHandler);
  statusEmitter.on("streamAdded", streamAddedHandler);
  statusEmitter.on("streamRemoved", streamRemovedHandler);
  statusEmitter.on("streamHealthChanged", streamHealthChangedHandler);
  statusEmitter.on("systemStatusChanged", systemStatusChangedHandler);

  return (): void => {

    statusEmitter.off("channelUpdate", channelUpdateHandler);
    statusEmitter.off("streamAdded", streamAddedHandler);
    statusEmitter.off("streamRemoved", streamRemovedHandler);
    statusEmitter.off("streamHealthChanged", streamHealthChangedHandler);
    statusEmitter.off("systemStatusChanged", systemStatusChangedHandler);
  };
}
