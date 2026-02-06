/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.ts: HLS streaming routes for PrismCast.
 */
import { handleHLSPlaylist, handleHLSSegment } from "../streaming/hls.js";
import type { Express } from "express";

/* This module registers the HTTP routes for HLS streaming:
 *
 * - GET /hls/:name/stream.m3u8 - Returns the HLS playlist for a channel (starts stream if needed)
 * - GET /hls/:name/:segment - Returns a specific segment file (init.mp4 or segmentN.m4s)
 */

/**
 * Sets up HLS streaming routes on the Express application.
 * @param app - The Express application.
 */
export function setupHLSRoutes(app: Express): void {

  // Public HLS playlist endpoint.
  app.get("/hls/:name/stream.m3u8", (req, res) => {

    void handleHLSPlaylist(req, res);
  });

  // Public HLS segment endpoint.
  app.get("/hls/:name/:segment", (req, res) => {

    handleHLSSegment(req, res);
  });
}
