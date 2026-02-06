/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * playlist.ts: M3U playlist route for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { CONFIG } from "../config/index.js";
import { getAllChannels } from "../config/userChannels.js";
import { resolveProfile } from "../config/profiles.js";

/* The playlist endpoint generates an M3U playlist in Channels DVR format. The playlist includes all configured video channels with their stream URLs dynamically
 * constructed from the request host header so the playlist works regardless of how the server is accessed.
 */

/**
 * Resolves the base URL from an incoming request by examining headers in priority order. This ensures that playlist URLs and other generated links use the same
 * host and protocol that the client used to connect, even when behind a reverse proxy. The resolution order is:
 *
 * 1. X-Forwarded-Host header (set by reverse proxies like nginx, Traefik)
 * 2. Host header (standard HTTP/1.1 header)
 * 3. Fallback to configured server host and port
 *
 * For protocol, Express's req.protocol already respects X-Forwarded-Proto when trust proxy is enabled.
 *
 * @param req - The Express request object.
 * @returns The base URL (e.g., "http://localhost:5589" or "https://myserver.example.com").
 */
export function resolveBaseUrl(req: Request): string {

  // Express's req.protocol already handles X-Forwarded-Proto when trust proxy is enabled, so we can use it directly.
  const protocol = req.protocol;

  // Check X-Forwarded-Host first (may contain multiple hosts if proxied through multiple layers, take the first one). Then fall back to the standard Host header,
  // and finally to the configured server settings.
  const forwardedHost = req.get("x-forwarded-host");
  const host = forwardedHost ? forwardedHost.split(",")[0].trim() : req.get("host");
  const fallbackHost = CONFIG.server.host + ":" + String(CONFIG.server.port);
  const resolvedHost = host ?? fallbackHost;

  return protocol + "://" + resolvedHost;
}

/**
 * Generates the M3U playlist content for display on the landing page or the playlist endpoint. The playlist includes all configured video channels with their
 * stream URLs dynamically constructed from the provided base URL.
 * @param baseUrl - The base URL to use for stream URLs (e.g., "http://localhost:5589").
 * @returns The M3U playlist content.
 */
export function generatePlaylistContent(baseUrl: string): string {

  const channels = getAllChannels();
  const lines = [ "#EXTM3U", "" ];
  const channelNames = Object.keys(channels).sort();

  for(const name of channelNames) {

    const channel = channels[name];

    // Skip channels that are marked as static pages since they are not video streams.
    const profile = resolveProfile(channel.profile);

    if(profile.noVideo) {

      continue;
    }

    // We use the channel key as the channel-id and the friendly name for display. HLS URLs are used for Channels DVR compatibility.
    const displayName = channel.name ?? name;
    const streamUrl = baseUrl + "/hls/" + name + "/stream.m3u8";

    // Build the EXTINF line with required channel-id attribute and tvg-name for the friendly display name. Include tvc-guide-stationid for Gracenote guide data
    // when a stationId is defined.
    const stationIdAttr = channel.stationId ? " tvc-guide-stationid=\"" + channel.stationId + "\"" : "";
    const extinfLine = "#EXTINF:-1 channel-id=\"" + name + "\" tvg-name=\"" + displayName + "\"" + stationIdAttr + "," + displayName;

    lines.push(extinfLine);
    lines.push(streamUrl);
    lines.push("");
  }

  return lines.join("\n");
}

/**
 * Creates the playlist endpoint that serves an M3U playlist in Channels DVR format. The playlist lists all configured channels with their stream URLs, allowing
 * Channels DVR to import them as custom channels. The endpoint dynamically constructs URLs using the request host header so the playlist works regardless of how
 * the server is accessed (localhost, IP address, or hostname).
 * @param app - The Express application.
 */
export function setupPlaylistEndpoint(app: Express): void {

  // GET /playlist - Returns the M3U playlist file.
  app.get("/playlist", (req: Request, res: Response): void => {

    const baseUrl = resolveBaseUrl(req);

    // Generate the playlist using the shared function and send it with the correct content type.
    const playlist = generatePlaylistContent(baseUrl);

    res.set("Content-Type", "audio/x-mpegurl");
    res.send(playlist);
  });
}
