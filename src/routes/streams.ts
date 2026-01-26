/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streams.ts: Stream management routes for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { LOG, formatError } from "../utils/index.js";
import { getAllStreams, getStream, getStreamCount, getStreamMemoryUsage, unregisterStream } from "../streaming/registry.js";
import { getStatusSnapshot, getStreamStatus, subscribeToStatus } from "../streaming/statusEmitter.js";
import { CONFIG } from "../config/index.js";
import type { Nullable } from "../types/index.js";
import type { StreamHealthStatus } from "../streaming/statusEmitter.js";

/*
 * STREAM MANAGEMENT
 *
 * The streams endpoint provides visibility into active streams and allows operators to terminate streams via the API. This is useful for debugging and for
 * integrations that need to manage stream lifecycle.
 */

/**
 * Creates an endpoint to list all active streams with their metadata.
 * @param app - The Express application.
 */
export function setupStreamsEndpoint(app: Express): void {

  app.get("/streams", (_req: Request, res: Response): void => {

    const now = Date.now();

    const streams: {
      channel: Nullable<string>;
      duration: number;
      escalationLevel: number;
      health: StreamHealthStatus;
      id: number;
      logoUrl: string;
      memory: { initSegment: number; segments: number; total: number };
      recoveryAttempts: number;
      showName: string;
      startTime: string;
      url: string;
    }[] = [];

    for(const streamInfo of getAllStreams()) {

      const status = getStreamStatus(streamInfo.id);

      streams.push({

        channel: streamInfo.channelName,
        duration: Math.round((now - streamInfo.startTime.getTime()) / 1000),
        escalationLevel: status?.escalationLevel ?? 0,
        health: status?.health ?? "healthy",
        id: streamInfo.id,
        logoUrl: status?.logoUrl ?? "",
        memory: getStreamMemoryUsage(streamInfo),
        recoveryAttempts: status?.recoveryAttempts ?? 0,
        showName: status?.showName ?? "",
        startTime: streamInfo.startTime.toISOString(),
        url: streamInfo.url
      });
    }

    res.json({

      count: getStreamCount(),
      limit: CONFIG.streaming.maxConcurrentStreams,
      streams: streams
    });
  });

  // Stream termination endpoint.
  app.delete("/streams/:id", async (req: Request, res: Response): Promise<void> => {

    const streamIdParam = parseInt((req.params as { id: string }).id);

    if(isNaN(streamIdParam)) {

      res.status(400).json({ error: "Invalid stream ID." });

      return;
    }

    const streamInfo = getStream(streamIdParam);

    if(!streamInfo) {

      res.status(404).json({ error: "Stream not found." });

      return;
    }

    LOG.info("Terminating stream %s via API request.", streamIdParam);

    // Stop monitor and close page.
    if(streamInfo.stopMonitor) {

      streamInfo.stopMonitor();
    }

    try {

      if(!streamInfo.page.isClosed()) {

        await streamInfo.page.close();
      }
    } catch(error) {

      LOG.warn("Error closing page during API termination: %s.", formatError(error));
    }

    unregisterStream(streamIdParam);

    res.json({ message: "Stream terminated.", streamId: streamIdParam });
  });

  /*
   * SSE STATUS STREAM ENDPOINT
   *
   * The /streams/status endpoint provides real-time stream and system status via Server-Sent Events. Connected clients receive an initial snapshot of all streams
   * and the system state, then receive updates as streams are added, removed, or their health changes.
   */

  app.get("/streams/status", (req: Request, res: Response): void => {

    // Set SSE headers. The Content-Type must be text/event-stream for the browser to recognize this as an SSE connection.
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("Content-Type", "text/event-stream");

    // Disable response buffering to ensure events are sent immediately.
    res.flushHeaders();

    // Send the initial snapshot so clients have current state.
    const snapshot = getStatusSnapshot();

    res.write("event: snapshot\n");
    res.write("data: " + JSON.stringify(snapshot) + "\n\n");

    // Subscribe to status events and forward them to the client.
    const unsubscribe = subscribeToStatus((eventType, data) => {

      res.write("event: " + eventType + "\n");
      res.write("data: " + JSON.stringify(data) + "\n\n");
    });

    // Send a heartbeat comment every 30 seconds to keep the connection alive through proxies.
    const heartbeatInterval = setInterval(() => {

      res.write(": heartbeat\n\n");
    }, 30000);

    // Clean up when the client disconnects.
    req.on("close", () => {

      clearInterval(heartbeatInterval);
      unsubscribe();
    });
  });
}
