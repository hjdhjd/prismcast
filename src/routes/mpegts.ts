/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * mpegts.ts: MPEG-TS streaming routes for PrismCast.
 */
import type { Express } from "express";
import { handleMpegTsStream } from "../streaming/mpegts.js";

/* This module registers the HTTP route for MPEG-TS streaming:
 *
 * - GET /stream/:name - Returns a continuous MPEG-TS byte stream for a channel (starts stream if needed)
 *
 * This endpoint is used by HDHomeRun-compatible clients (such as Plex) that expect raw MPEG-TS when tuning a channel. The stream shares the same underlying capture
 * as HLS — no additional browser tabs or capture sessions are created.
 */

/**
 * Sets up MPEG-TS streaming routes on the Express application.
 * @param app - The Express application.
 */
export function setupMpegTsRoutes(app: Express): void {

  // MPEG-TS stream endpoint for HDHomeRun-compatible clients.
  app.get("/stream/:name", (req, res) => {

    void handleMpegTsStream(req, res);
  });
}
