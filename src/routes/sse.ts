/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * sse.ts: Shared Server-Sent Events transport setup. Each SSE endpoint installs the same headers, heartbeat cadence, and close-cleanup; this helper owns
 * that boilerplate so endpoints describe only what to subscribe to and what payloads to push.
 */
import type { Response } from "express";

// Heartbeat cadence in milliseconds. Sent as a named heartbeat event to keep the connection alive through proxies and let clients detect staleness.
const HEARTBEAT_INTERVAL_MS = 30_000;

/**
 * SSE transport handle returned by installSseStream. Callers use sendEvent to push events with JSON payloads (eventType=null for an unnamed data event), and
 * call close() during request teardown to clear the heartbeat timer.
 */
export interface SseStream {

  close: () => void;
  sendEvent: (eventType: string | null, data: unknown) => void;
}

/**
 * Sets the SSE response headers, flushes them so the connection opens immediately, and starts the heartbeat. Returns helpers the caller uses to push events
 * and to clean up the heartbeat timer when the connection closes.
 * @param res - The Express response object.
 * @returns An SseStream handle for sending events and tearing down.
 */
export function installSseStream(res: Response): SseStream {

  // Cache-Control prevents proxies from buffering the stream; Connection: keep-alive ensures the connection stays open; Content-Type: text/event-stream is
  // the SSE protocol marker the browser EventSource needs to recognize the response.
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("Content-Type", "text/event-stream");

  // Flush headers immediately so the EventSource handshake completes before the first event arrives.
  res.flushHeaders();

  // Send a named heartbeat event every HEARTBEAT_INTERVAL_MS to keep the connection alive through proxies and allow clients to detect staleness.
  const heartbeat = setInterval(() => res.write("event: heartbeat\ndata: \n\n"), HEARTBEAT_INTERVAL_MS);

  return {

    close: (): void => { clearInterval(heartbeat); },
    sendEvent: (eventType: string | null, data: unknown): void => {

      if(eventType !== null) {

        res.write("event: " + eventType + "\n");
      }

      res.write("data: " + JSON.stringify(data) + "\n\n");
    }
  };
}
