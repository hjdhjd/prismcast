/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.ts: HLS streaming routes for PrismCast.
 */
import { handleHLSPlaylist, handleHLSSegment, handleHLSVariantPlaylist } from "../streaming/hls.js";
import type { Express } from "express";

/* This module registers the HTTP routes for HLS streaming:
 *
 * - GET /hls/:name/stream.m3u8 - Returns the HLS playlist for a channel (starts stream if needed)
 * - GET /hls/:name/video.m3u8 - Returns the video variant playlist for streams with separate audio
 * - GET /hls/:name/audio.m3u8 - Returns the audio variant playlist for streams with separate audio
 * - GET /hls/:name/:segment - Returns a specific segment file (init.mp4, segmentN.m4s, segmentN.ts, or audioN.ts)
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

  // Variant playlist endpoints for streams with separate audio renditions. Registered as two explicit routes because path-to-regexp does not support inline regex
  // parameter constraints (e.g., `:playlist(video\\.m3u8|audio\\.m3u8)`). These must be registered before the catch-all `:segment` route below.
  app.get("/hls/:name/video.m3u8", (req, res) => {

    handleHLSVariantPlaylist(req, res);
  });

  app.get("/hls/:name/audio.m3u8", (req, res) => {

    handleHLSVariantPlaylist(req, res);
  });

  // Public HLS segment endpoint.
  app.get("/hls/:name/:segment", (req, res) => {

    handleHLSSegment(req, res);
  });
}
