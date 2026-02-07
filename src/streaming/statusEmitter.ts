/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * statusEmitter.ts: Event emitter for real-time stream and system status via SSE.
 */
import type { ClientTypeCount } from "./clients.js";
import { EventEmitter } from "events";
import type { Nullable } from "../types/index.js";

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
  channel: Nullable<string>;
  clientCount: number;
  clients: ClientTypeCount[];
  currentTime: number;
  duration: number;
  escalationLevel: number;
  health: StreamHealthStatus;
  id: number;
  lastIssueTime: Nullable<number>;
  lastIssueType: Nullable<string>;
  lastRecoveryTime: Nullable<number>;
  logoUrl: string;
  memoryBytes: number;
  networkState: number;
  pageReloadsInWindow: number;
  providerName: string;
  readyState: number;
  recoveryAttempts: number;
  showName: string;
  startTime: string;
  url: string;
}

/**
 * System-wide status information.
 */
export interface SystemStatus {

  browser: {

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
 * Initial snapshot sent when an SSE client connects.
 */
export interface StatusSnapshot {

  streams: StreamStatus[];
  system: SystemStatus;
}

/**
 * Event types emitted by the status emitter.
 */
export type StatusEventType = "snapshot" | "streamAdded" | "streamHealthChanged" | "streamRemoved" | "systemStatusChanged";

/* A singleton EventEmitter that broadcasts status updates to all subscribed SSE clients. The emitter maintains current state for all streams, allowing new clients
 * to receive a snapshot of current status immediately upon connecting.
 */

const statusEmitter = new EventEmitter();

// Increase the default listener limit to support many concurrent SSE connections.
statusEmitter.setMaxListeners(100);

/**
 * Creates the initial StreamStatus object for a new stream. This provides a consistent starting state with all health metrics at their default values.
 * @param options - The stream initialization options.
 * @returns The initial stream status.
 */
export function createInitialStreamStatus(options: {
  channelName: Nullable<string>;
  numericStreamId: number;
  providerName: string;
  startTime: Date;
  url: string;
}): StreamStatus {

  return {

    bufferingDuration: null,
    channel: options.channelName,
    clientCount: 0,
    clients: [],
    currentTime: 0,
    duration: 0,
    escalationLevel: 0,
    health: "healthy",
    id: options.numericStreamId,
    lastIssueTime: null,
    lastIssueType: null,
    lastRecoveryTime: null,
    logoUrl: "",
    memoryBytes: 0,
    networkState: 0,
    pageReloadsInWindow: 0,
    providerName: options.providerName,
    readyState: 0,
    recoveryAttempts: 0,
    showName: "",
    startTime: options.startTime.toISOString(),
    url: options.url
  };
}

// Current status for all active streams, keyed by stream ID.
const streamStatuses = new Map<number, StreamStatus>();

// Cached system status, updated periodically and on significant events.
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
 * Emits a stream health changed event with the current stream status. This function always stores and emits the status to ensure SSE clients and snapshots have
 * current data. During healthy playback the monitor calls this every ~2 seconds anyway, so removing the previous selective filter has negligible bandwidth impact
 * while eliminating staleness during recovery/buffering periods.
 * @param status - The updated stream status.
 */
export function emitStreamHealthChanged(status: StreamStatus): void {

  streamStatuses.set(status.id, status);
  statusEmitter.emit("streamHealthChanged", status);
}

/**
 * Emits a system status changed event when browser or system state changes.
 * @param status - The updated system status.
 */
export function emitSystemStatusChanged(status: SystemStatus): void {

  // Only emit if something meaningful changed.
  if(!cachedSystemStatus ||
     (cachedSystemStatus.browser.connected !== status.browser.connected) ||
     (cachedSystemStatus.streams.active !== status.streams.active)) {

    cachedSystemStatus = status;
    statusEmitter.emit("systemStatusChanged", status);
  }
}

/**
 * Updates the cached system status without emitting an event. Used for periodic updates that should be included in snapshots but don't need to notify clients.
 * @param status - The updated system status.
 */
export function updateSystemStatus(status: SystemStatus): void {

  cachedSystemStatus = status;
}

/**
 * Gets the current status snapshot for all streams and the system.
 * @returns The current status snapshot.
 */
export function getStatusSnapshot(): StatusSnapshot {

  return {

    streams: Array.from(streamStatuses.values()),
    system: cachedSystemStatus ?? {

      browser: { connected: false, pageCount: 0 },
      memory: { heapUsed: 0, rss: 0 },
      streams: { active: 0, limit: 10 },
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
 * Subscribes a callback to receive all status events. Returns an unsubscribe function.
 * @param callback - Function to call when a status event is emitted.
 * @returns A function to unsubscribe the callback.
 */
export function subscribeToStatus(callback: (event: StatusEventType, data: StreamStatus | SystemStatus | StatusSnapshot | { id: number }) => void): () => void {

  const streamAddedHandler = (data: StreamStatus): void => callback("streamAdded", data);
  const streamRemovedHandler = (data: { id: number }): void => callback("streamRemoved", data);
  const streamHealthChangedHandler = (data: StreamStatus): void => callback("streamHealthChanged", data);
  const systemStatusChangedHandler = (data: SystemStatus): void => callback("systemStatusChanged", data);

  statusEmitter.on("streamAdded", streamAddedHandler);
  statusEmitter.on("streamRemoved", streamRemovedHandler);
  statusEmitter.on("streamHealthChanged", streamHealthChangedHandler);
  statusEmitter.on("systemStatusChanged", systemStatusChangedHandler);

  return (): void => {

    statusEmitter.off("streamAdded", streamAddedHandler);
    statusEmitter.off("streamRemoved", streamRemovedHandler);
    statusEmitter.off("streamHealthChanged", streamHealthChangedHandler);
    statusEmitter.off("systemStatusChanged", systemStatusChangedHandler);
  };
}
