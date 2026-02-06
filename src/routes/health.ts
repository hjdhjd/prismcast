/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.ts: Health check route for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { getAllStreams, getStreamCount, getTotalSegmentMemory } from "../streaming/registry.js";
import { getBrowserPages, getChromeVersion, isBrowserConnected } from "../browser/index.js";
import { getPackageVersion, isFFmpegAvailable } from "../utils/index.js";
import { CONFIG } from "../config/index.js";
import type { ClientType } from "../streaming/clients.js";
import type { HealthStatus } from "../types/index.js";
import { getClientSummary } from "../streaming/clients.js";

/* The health endpoint provides detailed metrics about the application status including browser connection, memory usage, and active stream counts. This is useful
 * for monitoring and alerting systems. Returns HTTP 503 when unhealthy to allow load balancers and monitoring systems to detect problems via status code.
 */

/**
 * Creates a health check endpoint for monitoring application status with detailed metrics.
 * @param app - The Express application.
 */
export function setupHealthEndpoint(app: Express): void {

  app.get("/health", async (_req: Request, res: Response): Promise<void> => {

    const browserConnected = isBrowserConnected();

    let pageCount = 0;

    if(browserConnected) {

      try {

        const pages = await getBrowserPages();

        pageCount = pages.length;
      } catch(_error) {

        // Ignore page count errors.
      }
    }

    const memoryUsage = process.memoryUsage();
    const segmentMemory = getTotalSegmentMemory();
    const ffmpegAvailable = await isFFmpegAvailable();

    // Aggregate client data across all active streams for the system-wide summary.
    const allClientTypes = new Map<ClientType, number>();
    let totalClients = 0;

    for(const streamInfo of getAllStreams()) {

      const summary = getClientSummary(streamInfo.id);

      totalClients += summary.total;

      for(const entry of summary.clients) {

        allClientTypes.set(entry.type, (allClientTypes.get(entry.type) ?? 0) + entry.count);
      }
    }

    const streamUtilization = getStreamCount() / CONFIG.streaming.maxConcurrentStreams;

    let status: "degraded" | "healthy" | "unhealthy" = "healthy";

    if(!browserConnected) {

      status = "unhealthy";
    } else if(streamUtilization >= 0.8) {

      status = "degraded";
    }

    const health: HealthStatus = {

      browser: {

        connected: browserConnected,
        pageCount: pageCount
      },
      captureMode: CONFIG.streaming.captureMode,
      chrome: getChromeVersion(),
      clients: {

        byType: Array.from(allClientTypes.entries()).sort(([a], [b]) => a.localeCompare(b)).map(([ type, count ]) => ({ count, type })),
        total: totalClients
      },
      ffmpegAvailable: ffmpegAvailable,
      memory: {

        heapTotal: memoryUsage.heapTotal,
        heapUsed: memoryUsage.heapUsed,
        rss: memoryUsage.rss,
        segmentBuffers: segmentMemory
      },
      status: status,
      streams: {

        active: getStreamCount(),
        limit: CONFIG.streaming.maxConcurrentStreams
      },
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      version: getPackageVersion()
    };

    if(!browserConnected) {

      health.message = "Browser is not connected.";
    } else if(streamUtilization >= 0.8) {

      health.message = "Approaching stream capacity limit.";
    }

    // Return HTTP 503 when unhealthy to allow load balancers and monitoring systems to detect problems via status code.
    const httpStatus = status === "unhealthy" ? 503 : 200;

    res.status(httpStatus).json(health);
  });
}
