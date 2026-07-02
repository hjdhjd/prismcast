/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * assets.ts: Static asset routes for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { dirname, join } from "node:path";
import type { Nullable } from "../types/index.ts";
import { fileURLToPath } from "node:url";
import { readFile } from "node:fs/promises";

/* This module serves static assets like the logo and favicon. Each asset is read from the project root directory on first request and cached in memory for efficient
 * serving of subsequent requests.
 */

// Cached asset data keyed by filename. Populated on first request and reused for subsequent requests.
const assetCache = new Map<string, Nullable<Buffer>>();

// Resolve the project root directory (two levels up from src/routes/).
const projectRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");

/**
 * Loads an asset file from the project root.
 * @param filename - The filename to load from the project root.
 * @returns The file contents as a Buffer, or null if the file could not be read.
 */
async function loadAsset(filename: string): Promise<Nullable<Buffer>> {

  try {

    return await readFile(join(projectRoot, filename));
  } catch(_error) {

    return null;
  }
}

/**
 * Registers a cached asset route. On first request, the asset is loaded from disk and cached; subsequent requests serve the cached copy.
 * @param app - The Express application.
 * @param routePath - The URL path to serve the asset on.
 * @param filename - The filename to load from the project root.
 * @param contentType - The Content-Type header value.
 */
function registerAssetRoute(app: Express, routePath: string, filename: string, contentType: string): void {

  app.get(routePath, async (_req: Request, res: Response): Promise<void> => {

    if(!assetCache.has(filename)) {

      // We cache the result of the very first load, including a null from a read failure. A failed load is therefore not retried and serves 404 until the process
      // restarts. This is acceptable because the served assets are committed to the repository and expected to always be present on disk.
      assetCache.set(filename, await loadAsset(filename));
    }

    const data = assetCache.get(filename) ?? null;

    if(data) {

      res.setHeader("Content-Type", contentType);
      res.setHeader("Cache-Control", "public, max-age=86400");
      res.send(data);
    } else {

      res.status(404).send("Not found");
    }
  });
}

/**
 * Configures routes for serving static assets (logo and favicon).
 * @param app - The Express application.
 */
export function setupAssetEndpoints(app: Express): void {

  registerAssetRoute(app, "/favicon.svg", "prismcast.svg", "image/svg+xml");
  registerAssetRoute(app, "/favicon.png", "prismcast.png", "image/png");
  registerAssetRoute(app, "/logo.png", "prismcast.png", "image/png");
  registerAssetRoute(app, "/logo.svg", "prismcast.svg", "image/svg+xml");
}
