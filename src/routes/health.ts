/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.ts: Health check route for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { getAllStreams, getStreamCount, getTotalSegmentMemory } from "../streaming/registry.ts";
import { getBrowserPages, getCaptureImpairment, getChromeVersion, isBrowserConnected } from "../browser/index.ts";
import { getPackageVersion, isFFmpegAvailable } from "../utils/index.ts";
import { CONFIG } from "../config/index.ts";
import type { ClientType } from "../streaming/clients.ts";
import type { HealthStatus } from "../types/index.ts";
import { getClientSummary } from "../streaming/clients.ts";

/* The health endpoint provides detailed metrics about the application status including browser connection, memory usage, and active stream counts. This is useful
 * for monitoring and alerting systems. Returns HTTP 503 when unhealthy to allow load balancers and monitoring systems to detect problems via status code.
 */

/**
 * HealthDeps is the state-reader boundary the /health handler folds into its payload: browser connection/pages/version, the stream registry counts and memory, and
 * the per-stream client summary. It is injected as a default parameter so a test can substitute in-memory readers through the same HealthDeps parameter - no
 * loader mock - while production uses the real defaultHealthDeps. isFFmpegAvailable, CONFIG, and the process memory/version are read directly because they are
 * not the substituted boundary. This mirrors the Clock port (utils/clock.ts): a typed interface plus a module-const default, consumed through a defaulted parameter.
 */
export interface HealthDeps {

  readonly getAllStreams: typeof getAllStreams;
  readonly getBrowserPages: typeof getBrowserPages;
  readonly getCaptureImpairment: typeof getCaptureImpairment;
  readonly getChromeVersion: typeof getChromeVersion;
  readonly getClientSummary: typeof getClientSummary;
  readonly getStreamCount: typeof getStreamCount;
  readonly getTotalSegmentMemory: typeof getTotalSegmentMemory;
  readonly isBrowserConnected: typeof isBrowserConnected;
}

const defaultHealthDeps: HealthDeps = { getAllStreams, getBrowserPages, getCaptureImpairment, getChromeVersion, getClientSummary, getStreamCount,
  getTotalSegmentMemory, isBrowserConnected };

/**
 * The health decision: the tri-state status, its HTTP code, and the operator message. Deriving it in one place collapses what were three separate evaluations of
 * the same two conditions (status, message, and HTTP code) into a single source of truth.
 */
interface HealthDecision {

  readonly httpStatus: 200 | 503;
  readonly message?: string;
  readonly status: "degraded" | "healthy" | "unhealthy";
}

/**
 * Derives the health status, HTTP code, and operator message from browser connectivity, the browser's ability to start captures, and stream utilization. The
 * precedence runs in that order: unhealthy (browser down) outranks degraded-because-marked, which outranks degraded-because-utilization (at or past the 0.8
 * threshold); otherwise healthy. A disconnected browser outranks a marked one because a marked browser is still serving what it started, while a disconnected one
 * is serving nothing. Pure and total - no I/O - so the branch precedence and the threshold live in exactly one place.
 * @param options - The decision inputs.
 * @param options.browserConnected - Whether the shared browser is currently connected.
 * @param options.captureImpaired - Whether the browser can no longer start captures and is waiting to relaunch.
 * @param options.streamUtilization - Active streams divided by the configured concurrency limit.
 * @returns The tri-state decision consumed by the /health handler.
 */
export function deriveHealthStatus(options: { browserConnected: boolean; captureImpaired: boolean; streamUtilization: number }): HealthDecision {

  if(!options.browserConnected) {

    return { httpStatus: 503, message: "Browser is not connected.", status: "unhealthy" };
  }

  if(options.captureImpaired) {

    return { httpStatus: 200, message: "The browser can no longer start captures and will relaunch once it is idle.", status: "degraded" };
  }

  if(options.streamUtilization >= 0.8) {

    return { httpStatus: 200, message: "Approaching stream capacity limit.", status: "degraded" };
  }

  return { httpStatus: 200, status: "healthy" };
}

/**
 * Creates a health check endpoint for monitoring application status with detailed metrics.
 * @param app - The Express application.
 * @param deps - The state readers the handler folds into its payload; defaults to defaultHealthDeps, injectable so a test can drive every branch in memory.
 */
export function setupHealthEndpoint(app: Express, deps: HealthDeps = defaultHealthDeps): void {

  app.get("/health", async (_req: Request, res: Response): Promise<void> => {

    const browserConnected = deps.isBrowserConnected();

    // Both browser facts are read here, before the first await, so the response describes one request-time instant. A request composes its own snapshot and there
    // is no cross-request cache to race, which is what lets this read sit at the top rather than beside the fields it feeds.
    const captureImpaired = deps.getCaptureImpairment() !== null;

    let pageCount = 0;

    if(browserConnected) {

      try {

        const pages = await deps.getBrowserPages();

        pageCount = pages.length;
      } catch(_error) {

        // Ignore page count errors.
      }
    }

    const memoryUsage = process.memoryUsage();
    const segmentMemory = deps.getTotalSegmentMemory();
    const ffmpegAvailable = await isFFmpegAvailable();

    // Aggregate client data across all active streams for the system-wide summary.
    const allClientTypes = new Map<ClientType, number>();
    let totalClients = 0;

    for(const streamInfo of deps.getAllStreams()) {

      const summary = deps.getClientSummary(streamInfo.id);

      totalClients += summary.total;

      for(const entry of summary.clients) {

        allClientTypes.set(entry.type, (allClientTypes.get(entry.type) ?? 0) + entry.count);
      }
    }

    // Stream utilization is the fraction of the configured concurrency limit currently in use. Once it reaches 80% we report "degraded" and surface a capacity
    // warning below, giving monitoring and alerting systems headroom to react while streams can still be served rather than only flagging trouble at full saturation.
    const streamUtilization = deps.getStreamCount() / CONFIG.streaming.maxConcurrentStreams;

    const decision = deriveHealthStatus({ browserConnected, captureImpaired, streamUtilization });

    const health: HealthStatus = {

      browser: {

        captureImpaired: captureImpaired,
        connected: browserConnected,
        pageCount: pageCount
      },
      captureMode: CONFIG.streaming.captureMode,
      chrome: deps.getChromeVersion(),
      clients: {

        byType: Array.from(allClientTypes.entries()).toSorted(([a], [b]) => a.localeCompare(b)).map(([ type, count ]) => ({ count, type })),
        total: totalClients
      },
      ffmpegAvailable: ffmpegAvailable,
      memory: {

        heapTotal: memoryUsage.heapTotal,
        heapUsed: memoryUsage.heapUsed,
        rss: memoryUsage.rss,
        segmentBuffers: segmentMemory
      },
      status: decision.status,
      streams: {

        active: deps.getStreamCount(),
        limit: CONFIG.streaming.maxConcurrentStreams
      },
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      version: getPackageVersion()
    };

    if(decision.message !== undefined) {

      health.message = decision.message;
    }

    res.status(decision.httpStatus).json(health);
  });
}
