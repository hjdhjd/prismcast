/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Route aggregator for PrismCast.
 */
import type { Express } from "express";
import { setupAssetEndpoints } from "./assets.js";
import { setupAuthEndpoint } from "./auth.js";
import { setupChannelsEndpoint } from "./channels.js";
import { setupConfigEndpoint } from "./config.js";
import { setupDebugEndpoint } from "./debug.js";
import { setupHLSRoutes } from "./hls.js";
import { setupHealthEndpoint } from "./health.js";
import { setupLogsEndpoint } from "./logs.js";
import { setupMpegTsRoutes } from "./mpegts.js";
import { setupPlayEndpoint } from "./play.js";
import { setupPlaylistEndpoint } from "./playlist.js";
import { setupRootEndpoint } from "./root.js";
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
  setupRootEndpoint(app);
  setupStreamsEndpoint(app);
  setupUpgradeEndpoint(app);
}

// Re-export individual setup functions for selective use if needed.
export { setupAssetEndpoints } from "./assets.js";
export { setupAuthEndpoint } from "./auth.js";
export { setupChannelsEndpoint } from "./channels.js";
export { setupConfigEndpoint } from "./config.js";
export { setupDebugEndpoint } from "./debug.js";
export { setupHealthEndpoint } from "./health.js";
export { setupHLSRoutes } from "./hls.js";
export { setupLogsEndpoint } from "./logs.js";
export { setupMpegTsRoutes } from "./mpegts.js";
export { setupPlayEndpoint } from "./play.js";
export { generatePlaylistContent, resolveBaseUrl, setupPlaylistEndpoint } from "./playlist.js";
export { setupRootEndpoint } from "./root.js";
export { setupStreamsEndpoint } from "./streams.js";
export { setupUpgradeEndpoint } from "./upgrade.js";
