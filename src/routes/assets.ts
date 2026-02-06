/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * assets.ts: Static asset routes for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { dirname, join } from "path";
import type { Nullable } from "../types/index.js";
import { fileURLToPath } from "url";
import { readFile } from "fs/promises";

/* This module serves static assets like the logo and favicon. The assets are read from the project root directory at startup and cached in memory for efficient
 * serving.
 */

// Cached asset data. Populated on first request and reused for subsequent requests.
let svgData: Nullable<Buffer> = null;
let pngData: Nullable<Buffer> = null;

// Resolve the project root directory (two levels up from src/routes/).
const projectRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");

/**
 * Loads an asset file from the project root, caching it for subsequent requests.
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
 * Configures routes for serving static assets (logo and favicon).
 * @param app - The Express application.
 */
export function setupAssetEndpoints(app: Express): void {

  // Serve the SVG favicon.
  app.get("/favicon.svg", async (_req: Request, res: Response): Promise<void> => {

    svgData ??= await loadAsset("prismcast.svg");

    if(svgData) {

      res.setHeader("Content-Type", "image/svg+xml");
      res.setHeader("Cache-Control", "public, max-age=86400");
      res.send(svgData);
    } else {

      res.status(404).send("Not found");
    }
  });

  // Serve the PNG favicon (alternative for browsers that prefer PNG).
  app.get("/favicon.png", async (_req: Request, res: Response): Promise<void> => {

    pngData ??= await loadAsset("prismcast.png");

    if(pngData) {

      res.setHeader("Content-Type", "image/png");
      res.setHeader("Cache-Control", "public, max-age=86400");
      res.send(pngData);
    } else {

      res.status(404).send("Not found");
    }
  });

  // Serve the PNG logo (used for apple-touch-icon which requires PNG format).
  app.get("/logo.png", async (_req: Request, res: Response): Promise<void> => {

    pngData ??= await loadAsset("prismcast.png");

    if(pngData) {

      res.setHeader("Content-Type", "image/png");
      res.setHeader("Cache-Control", "public, max-age=86400");
      res.send(pngData);
    } else {

      res.status(404).send("Not found");
    }
  });

  // Serve the SVG logo (used for the homepage header).
  app.get("/logo.svg", async (_req: Request, res: Response): Promise<void> => {

    svgData ??= await loadAsset("prismcast.svg");

    if(svgData) {

      res.setHeader("Content-Type", "image/svg+xml");
      res.setHeader("Cache-Control", "public, max-age=86400");
      res.send(svgData);
    } else {

      res.status(404).send("Not found");
    }
  });
}
