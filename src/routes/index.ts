/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Route aggregator for PrismCast.
 */
import type { Express } from "express";
import { setupAssetEndpoints } from "./assets.ts";
import { setupAuthEndpoint } from "./auth.ts";
import { setupCdpEndpoint } from "./cdp.ts";
import { setupChannelsEndpoint } from "./channels.ts";
import { setupConfigEndpoint } from "./config/index.ts";
import { setupDebugEndpoint } from "./debug.ts";
import { setupHLSRoutes } from "./hls.ts";
import { setupHealthEndpoint } from "./health.ts";
import { setupLogsEndpoint } from "./logs.ts";
import { setupMpegTsRoutes } from "./mpegts.ts";
import { setupPlayEndpoint } from "./play.ts";
import { setupPlaylistEndpoint } from "./playlist.ts";
import { setupPrerollRoutes } from "../streaming/preroll.ts";
import { setupRootEndpoint } from "./root/index.ts";
import { setupServicesEndpoint } from "./services.ts";
import { setupStreamsEndpoint } from "./streams.ts";
import { setupUpgradeEndpoint } from "./upgrade.ts";

/* This module aggregates all route setup functions and provides a single function to configure all HTTP endpoints on the Express application.
 */

/**
 * Configures all HTTP endpoints on the Express application.
 * @param app - The Express application.
 */
export function setupRoutes(app: Express): void {

  setupAssetEndpoints(app);
  setupAuthEndpoint(app);
  setupCdpEndpoint(app);
  setupChannelsEndpoint(app);
  setupConfigEndpoint(app);
  setupDebugEndpoint(app);
  setupHealthEndpoint(app);
  setupHLSRoutes(app);
  setupLogsEndpoint(app);
  setupMpegTsRoutes(app);
  setupPlayEndpoint(app);
  setupPlaylistEndpoint(app);
  setupPrerollRoutes(app);
  setupServicesEndpoint(app);
  setupRootEndpoint(app);
  setupStreamsEndpoint(app);
  setupUpgradeEndpoint(app);
}
