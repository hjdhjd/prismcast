/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * playlist.ts: M3U playlist route for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { getAllProviderTags, getChannelProviderTags } from "../config/providers.js";
import { CONFIG } from "../config/index.js";
import { getAllChannels } from "../config/userChannels.js";
import { resolveProfile } from "../config/profiles.js";

/* The playlist endpoint generates an M3U playlist in Channels DVR format. The playlist includes all configured video channels with their stream URLs dynamically
 * constructed from the request host header so the playlist works regardless of how the server is accessed.
 */

// Provider Filter Types.

/* A parsed provider filter specifies which channels to include or exclude based on their provider tags. In include mode, only channels matching at least one of the
 * specified tags are included. In exclude mode, channels matching any of the specified tags are excluded.
 */
interface ProviderFilter {

  readonly exclude: boolean;
  readonly tags: string[];
}

/**
 * Parses and validates a provider filter query parameter. The parameter is a comma-separated list of provider tags with optional `-` prefix for exclusion mode. All
 * tags must be either include (no prefix) or exclude (`-` prefix) — mixing is not allowed. Tags are case-insensitive and validated against known provider tags.
 * @param param - The raw query parameter string (e.g., "yttv,sling" or "-hulu,-sling").
 * @returns An object with `filter` on success, or `error` with a descriptive message and `validTags` list on failure.
 */
function parseProviderFilter(param: string): { error: string; validTags: string[] } | { filter: ProviderFilter } {

  const tokens = param.split(",").map((t) => t.trim().toLowerCase()).filter((t) => t.length > 0);

  if(tokens.length === 0) {

    return { error: "Empty provider filter.", validTags: [] };
  }

  // Classify tokens as include or exclude based on the `-` prefix.
  const excludeTokens: string[] = [];
  const includeTokens: string[] = [];

  for(const token of tokens) {

    if(token.startsWith("-")) {

      excludeTokens.push(token.slice(1));
    } else {

      includeTokens.push(token);
    }
  }

  // Reject mixed mode — all tokens must be either include or exclude.
  if((excludeTokens.length > 0) && (includeTokens.length > 0)) {

    return { error: "Cannot mix include and exclude filters. Use either \"tag1,tag2\" (include) or \"-tag1,-tag2\" (exclude).", validTags: [] };
  }

  const isExclude = excludeTokens.length > 0;
  const tags = isExclude ? excludeTokens : includeTokens;

  // Validate all tags against known provider tags.
  const allTags = getAllProviderTags();
  const knownTags = new Set(allTags.map((p) => p.tag));
  const unknownTags = tags.filter((tag) => !knownTags.has(tag));

  if(unknownTags.length > 0) {

    return { error: "Unknown provider tag(s): " + unknownTags.join(", ") + ".", validTags: allTags.map((p) => p.tag).sort() };
  }

  return { filter: { exclude: isExclude, tags } };
}

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
 * @param filter - Optional provider filter. In include mode, only channels with at least one matching tag are included. In exclude mode, channels with any matching
 * tag are excluded. When omitted, all channels are included.
 * @returns The M3U playlist content.
 */
export function generatePlaylistContent(baseUrl: string, filter?: ProviderFilter): string {

  const channels = getAllChannels();
  const lines = [ "#EXTM3U", "" ];
  const channelNames = Object.keys(channels).sort();

  for(const name of channelNames) {

    // Apply the provider filter if specified.
    if(filter) {

      const channelTags = getChannelProviderTags(name);
      const hasMatch = channelTags.some((tag) => filter.tags.includes(tag));

      // In include mode, skip channels that don't match any filter tag. In exclude mode, skip channels that match any filter tag.
      if(filter.exclude ? hasMatch : !hasMatch) {

        continue;
      }
    }

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

  // GET /playlist - Returns the M3U playlist file. Supports optional ?provider= query parameter for filtering channels by streaming provider.
  app.get("/playlist", (req: Request, res: Response): void => {

    const baseUrl = resolveBaseUrl(req);
    const providerParam = typeof req.query.provider === "string" ? req.query.provider.trim() : undefined;
    let filter: ProviderFilter | undefined;

    // Parse and validate the provider filter if specified.
    if(providerParam) {

      const result = parseProviderFilter(providerParam);

      if("error" in result) {

        res.status(400).json({ error: result.error, validTags: result.validTags });

        return;
      }

      filter = result.filter;
    }

    const playlist = generatePlaylistContent(baseUrl, filter);

    res.set("Content-Type", "audio/x-mpegurl");
    res.send(playlist);
  });
}
