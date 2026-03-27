/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Route aggregator for PrismCast.
 */
import type { Express } from "express";
import { setupAssetEndpoints } from "./assets.js";
import { setupAuthEndpoint } from "./auth.js";
import { setupChannelsEndpoint } from "./channels.js";
import { setupConfigEndpoint } from "./config/index.js";
import { setupDebugEndpoint } from "./debug.js";
import { setupHLSRoutes } from "./hls.js";
import { setupHealthEndpoint } from "./health.js";
import { setupLogsEndpoint } from "./logs.js";
import { setupMpegTsRoutes } from "./mpegts.js";
import { setupPlayEndpoint } from "./play.js";
import { setupPlaylistEndpoint } from "./playlist.js";
import { setupPrerollRoutes } from "../streaming/preroll.js";
import { setupProvidersEndpoint } from "./providers.js";
import { setupRootEndpoint } from "./root/index.js";
import { setupStreamsEndpoint } from "./streams.js";
import { setupUpgradeEndpoint } from "./upgrade.js";

/* This module aggregates all route setup functions and provides a single function to configure all HTTP endpoints on the Express application.
 */

/**
 * Configures all HTTP endpoints on the Express application.
 * @param app - The Express application.
 */
export function setupRoutes(app: Express): void {

  setupAssetEndpoints(app);
  setupAuthEndpoint(app);
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
  setupProvidersEndpoint(app);
  setupRootEndpoint(app);
  setupStreamsEndpoint(app);
  setupUpgradeEndpoint(app);
}
