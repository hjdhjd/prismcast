/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * play.ts: Ad-hoc URL streaming route for PrismCast.
 */
import type { Express } from "express";
import { handlePlayStream } from "../streaming/hls.js";

/*
 * PLAY ROUTE
 *
 * This module registers the HTTP route for ad-hoc URL streaming:
 *
 * - GET /play?url=<url>&profile=<name> - Starts an HLS stream for an arbitrary URL and redirects to the HLS playlist path.
 *
 * This endpoint enables streaming URLs that are not predefined as channels. The stream is identified by a synthetic key derived from the URL hash, and the client is
 * redirected to /hls/<key>/stream.m3u8 where existing HLS handlers serve the content.
 */

/**
 * Sets up the ad-hoc streaming route on the Express application.
 * @param app - The Express application.
 */
export function setupPlayEndpoint(app: Express): void {

  app.get("/play", (req, res) => {

    void handlePlayStream(req, res);
  });
}
