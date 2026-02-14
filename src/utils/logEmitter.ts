/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logEmitter.ts: Event emitter for real-time log streaming via SSE.
 */
import { EventEmitter } from "events";

/* Represents a structured log entry that can be serialized and sent to SSE clients. This type mirrors the LogEntry interface in logs.ts but is defined here to avoid
 * circular dependencies.
 */

export interface LogEntry {

  categoryTag?: string;
  level: "debug" | "error" | "info" | "warn";
  message: string;
  timestamp: string;
}

/* A singleton EventEmitter that broadcasts log entries to all subscribed SSE clients. When a log entry is written via LOG.info/warn/error, the entry is emitted here
 * for real-time streaming to connected browsers.
 */

const logEmitter = new EventEmitter();

// Increase the default listener limit to support many concurrent SSE connections. Each browser tab viewing the Logs tab will have its own listener.
logEmitter.setMaxListeners(100);

/**
 * Emits a log entry to all subscribed SSE clients.
 * @param entry - The log entry to broadcast.
 */
export function emitLogEntry(entry: LogEntry): void {

  logEmitter.emit("log", entry);
}

/**
 * Subscribes a callback to receive log entries. Returns an unsubscribe function.
 * @param callback - Function to call when a log entry is emitted.
 * @returns A function to unsubscribe the callback.
 */
export function subscribeToLogs(callback: (entry: LogEntry) => void): () => void {

  logEmitter.on("log", callback);

  return (): void => {

    logEmitter.off("log", callback);
  };
}
