/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streams.ts: Stream management routes for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { getAllStreams, getStream, getStreamCount, getStreamMemoryUsage } from "../streaming/registry.ts";
import { getStatusSnapshot, getStreamStatus, subscribeToStatus } from "../streaming/statusEmitter.ts";
import { sendNotFoundError, sendSuccess, sendValidationError } from "./config/http/envelope.ts";
import { CONFIG } from "../config/index.ts";
import type { ClientTypeCount } from "../streaming/clients.ts";
import type { Nullable } from "../types/index.ts";
import type { StreamHealthStatus } from "../streaming/statusEmitter.ts";
import { emitCurrentSystemStatus } from "../browser/index.ts";
import { installSseStream } from "./sse.ts";
import { terminateStream } from "../streaming/lifecycle.ts";

/* The streams endpoint provides visibility into active streams and allows operators to terminate streams via the API. This is useful for debugging and for
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
      clientCount: number;
      clients: ClientTypeCount[];
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
        clientCount: status?.clientCount ?? 0,
        clients: status?.clients ?? [],
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

  // Stream termination endpoint. Uses the authoritative terminateStream() function for consistent cleanup of all resources (segmenter, capture stream, FFmpeg,
  // channel mapping, client tracking, SSE events).
  app.delete("/streams/:id", (req: Request, res: Response): void => {

    const streamIdParam = parseInt((req.params as { id: string }).id);

    if(isNaN(streamIdParam)) {

      sendValidationError(res, "Invalid stream ID.");

      return;
    }

    const streamInfo = getStream(streamIdParam);

    if(!streamInfo) {

      sendNotFoundError(res, "Stream not found.");

      return;
    }

    terminateStream(streamIdParam, streamInfo.info.storeKey, "API request");
    void emitCurrentSystemStatus();

    sendSuccess(res, { data: { streamId: streamIdParam }, message: "Stream terminated." });
  });

  /* The /streams/status endpoint provides real-time stream and system status via Server-Sent Events. Connected clients receive an initial snapshot of all streams
   * and the system state, then receive updates as streams are added, removed, or their health changes.
   */

  app.get("/streams/status", (req: Request, res: Response): void => {

    const sse = installSseStream(res);

    // Send the initial snapshot so clients have current state.
    sse.sendEvent("snapshot", getStatusSnapshot());

    // Subscribe to status events and forward them to the client; the heartbeat is owned by installSseStream.
    const unsubscribe = subscribeToStatus((eventType, data) => { sse.sendEvent(eventType, data); });

    req.on("close", () => {

      sse.close();
      unsubscribe();
    });
  });
}
