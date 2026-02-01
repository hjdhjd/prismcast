/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: HDHomeRun emulation server for PrismCast.
 */
import { generateDeviceId, validateDeviceId } from "./deviceId.js";
import { loadUserConfig, saveUserConfig } from "../config/userConfig.js";
import { CONFIG } from "../config/index.js";
import { LOG } from "../utils/index.js";
import type { Nullable } from "../types/index.js";
import type { Server } from "http";
import express from "express";
import { formatError } from "../utils/errors.js";
import { setupHdhrEndpoints } from "./discover.js";

/*
 * HDHOMERUN EMULATION SERVER
 *
 * When HDHomeRun emulation is enabled, PrismCast runs a separate Express server that responds to HDHomeRun API requests from Plex. This server is intentionally
 * lightweight — it only serves device discovery and lineup metadata. All actual video streaming flows through PrismCast's main HTTP server via the HLS URLs in
 * the lineup response.
 *
 * The HDHR server runs on a configurable port (default 5004) independently of the main server. If the port is unavailable, the HDHR feature is disabled
 * gracefully without affecting the main server. Plex does not auto-detect emulated tuners — users must manually enter the address (IP:port) in Plex's DVR setup.
 */

// The HDHR HTTP server instance, used for graceful shutdown.
let hdhrServer: Nullable<Server> = null;

/**
 * Starts the HDHomeRun emulation server if enabled in configuration. This includes generating a DeviceID on first run and starting the HTTP server for discovery
 * endpoints. If the configured port is unavailable, HDHR emulation is disabled gracefully.
 */
export async function startHdhrServer(): Promise<void> {

  if(!CONFIG.hdhr.enabled) {

    return;
  }

  // Generate a DeviceID on first run, or regenerate if the stored ID fails checksum validation (e.g., hand-edited config with a typo). Plex silently rejects
  // tuners with invalid DeviceIDs during discovery, so we catch this early.
  if(!CONFIG.hdhr.deviceId || !validateDeviceId(CONFIG.hdhr.deviceId)) {

    if(CONFIG.hdhr.deviceId) {

      LOG.warn("HDHomeRun DeviceID '%s' has an invalid checksum. Generating a new one.", CONFIG.hdhr.deviceId.toUpperCase());
    }

    CONFIG.hdhr.deviceId = generateDeviceId();

    LOG.info("Generated HDHomeRun DeviceID: %s.", CONFIG.hdhr.deviceId.toUpperCase());

    // Save the generated ID to the user config so it persists across restarts. We load the current config, set the deviceId, and save to avoid overwriting other
    // settings.
    try {

      const result = await loadUserConfig();

      if(!result.parseError) {

        result.config.hdhr ??= {};
        result.config.hdhr.deviceId = CONFIG.hdhr.deviceId;

        await saveUserConfig(result.config);
      }
    } catch(error) {

      LOG.warn("Failed to persist HDHomeRun DeviceID: %s. A new ID will be generated on next restart.", formatError(error));
    }
  }

  // Build the HDHR Express application.
  const app = express();

  app.set("trust proxy", true);

  setupHdhrEndpoints(app);

  // Start listening on the configured port. Handle EADDRINUSE gracefully.
  try {

    hdhrServer = await new Promise<Server>((resolve, reject) => {

      const server = app.listen(CONFIG.hdhr.port, CONFIG.server.host, (): void => {

        resolve(server);
      });

      server.on("error", (error: NodeJS.ErrnoException): void => {

        reject(error);
      });
    });

    LOG.info("HDHomeRun emulation is now listening on %s:%s (DeviceID: %s).", CONFIG.server.host, CONFIG.hdhr.port, CONFIG.hdhr.deviceId.toUpperCase());
  } catch(error) {

    const errnoError = error as NodeJS.ErrnoException;

    if(errnoError.code === "EADDRINUSE") {

      LOG.warn("HDHomeRun port %s is already in use. HDHomeRun emulation is disabled. Check for conflicting services on this port.", CONFIG.hdhr.port);
    } else {

      LOG.warn("Failed to start HDHomeRun server: %s. HDHomeRun emulation is disabled.", formatError(error));
    }

    return;
  }
}

/**
 * Stops the HDHomeRun emulation server. Called during graceful shutdown.
 */
export function stopHdhrServer(): void {

  if(hdhrServer) {

    hdhrServer.close();

    hdhrServer = null;
  }
}
