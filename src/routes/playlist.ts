/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * playlist.ts: M3U playlist route for PrismCast.
 */
import type { ChannelSortField, SortDirection } from "../types/index.ts";
import type { Express, Request, Response } from "express";
import { VALID_SORT_FIELDS, compareChannelSort, getAllServiceTags, getServiceTagForChannel, resolveServiceKey } from "../config/services.ts";
import { getActiveTagVocabulary, getAllChannels, getChannelEffectiveTags, tagsMatch } from "../config/userChannels.ts";
import { CONFIG } from "../config/index.ts";
import { escapeM3uAttribute } from "../utils/m3u.ts";
import { getProfileForChannel } from "../config/profiles.ts";
import { sendValidationError } from "./config/http/envelope.ts";

/* The playlist endpoint generates an M3U playlist in Channels DVR format. The playlist includes all available channels (both video player and static capture) with
 * their stream URLs dynamically constructed from the request host header so the playlist works regardless of how the server is accessed.
 */

// Include/Exclude Filter.

/* A parsed include/exclude filter specifies which items to include or exclude based on a set of string tags. In include mode, only items matching at least one of
 * the specified tags are included. In exclude mode, items matching any of the specified tags are excluded. Used by both service filtering (?service=) and tag
 * filtering (?tag=) with different validation sources.
 */
interface IncludeExcludeFilter {

  readonly exclude: boolean;
  readonly tags: string[];
}

/**
 * Parses and validates an include/exclude filter query parameter. The parameter is a comma-separated list of values with optional `-` prefix for exclusion mode.
 * All values must be either include (no prefix) or exclude (`-` prefix) - mixing is not allowed. Values are case-insensitive and validated against a known set.
 * @param param - The raw query parameter string (e.g., "sports,news" or "-kids").
 * @param entityName - Human-readable name for error messages (e.g., "service tag", "tag").
 * @param knownValues - Array of valid values to validate against.
 * @returns An object with `filter` on success, or `error` with a descriptive message and `validTags` list on failure.
 */
function parseIncludeExcludeFilter(param: string, entityName: string,
  knownValues: string[]): { error: string; validTags: string[] } | { filter: IncludeExcludeFilter } {

  const tokens = param.split(",").map((t) => t.trim().toLowerCase()).filter((t) => t.length > 0);

  if(tokens.length === 0) {

    return { error: "Empty " + entityName + " filter.", validTags: [] };
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

  // Reject mixed mode - all tokens must be either include or exclude.
  if((excludeTokens.length > 0) && (includeTokens.length > 0)) {

    return { error: "Cannot mix include and exclude " + entityName + " filters. Use either \"a,b\" (include) or \"-a,-b\" (exclude).", validTags: [] };
  }

  const isExclude = excludeTokens.length > 0;
  const filterTags = isExclude ? excludeTokens : includeTokens;

  // Validate all values against the known set. Tokens are already lowercased, so normalize known values for case-insensitive comparison.
  const knownSet = new Set(knownValues.map((v) => v.toLowerCase()));
  const filterSet = new Set(filterTags);
  const unknownTags = [...filterSet.difference(knownSet)];

  if(unknownTags.length > 0) {

    return { error: "Unknown " + entityName + "(s): " + unknownTags.join(", ") + ".", validTags: knownValues.toSorted() };
  }

  return { filter: { exclude: isExclude, tags: filterTags } };
}

/**
 * Parses a service filter by validating against known service tags.
 * @param param - The raw query parameter string (e.g., "yttv,sling" or "-hulu,-sling").
 * @returns An object with `filter` on success, or `error` with a descriptive message and `validTags` list on failure.
 */
function parseServiceFilter(param: string): { error: string; validTags: string[] } | { filter: IncludeExcludeFilter } {

  return parseIncludeExcludeFilter(param, "service tag", getAllServiceTags().map((p) => p.tag));
}

/**
 * Parses a tag filter by validating against the active tag vocabulary.
 * @param param - The raw query parameter string (e.g., "sports,news" or "-kids").
 * @returns An object with `filter` on success, or `error` with a descriptive message and `validTags` list on failure.
 */
function parseTagFilter(param: string): { error: string; validTags: string[] } | { filter: IncludeExcludeFilter } {

  return parseIncludeExcludeFilter(param, "tag", getActiveTagVocabulary());
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
  const host = forwardedHost ? (forwardedHost.split(",")[0] ?? "").trim() : req.get("host");
  const fallbackHost = CONFIG.server.host + ":" + String(CONFIG.server.port);
  const resolvedHost = host ?? fallbackHost;

  return protocol + "://" + resolvedHost;
}

/**
 * Generates the M3U playlist content for display on the landing page or the playlist endpoint. The playlist includes all configured video channels with their
 * stream URLs dynamically constructed from the provided base URL.
 * @param baseUrl - The base URL to use for stream URLs (e.g., "http://localhost:5589").
 * @param serviceFilter - Optional service filter based on the currently selected service for each channel. In include mode, only channels whose selected
 * service matches a filter tag are included. In exclude mode, channels whose selected service matches any filter tag are excluded.
 * @param tagFilter - Optional tag filter based on the channel's effective organizational tags. In include mode, only channels with at least one matching tag are
 * included. In exclude mode, channels with any matching tag are excluded. Tag and service filters compose via intersection.
 * @param sort - Optional sort field override. When provided, channels are sorted by this field instead of the user's saved preference. Validated against
 * VALID_SORT_FIELDS before calling.
 * @param direction - Optional sort direction override ("asc" or "desc"). When provided, overrides the user's saved sort direction.
 * @returns The M3U playlist content.
 */
export function generatePlaylistContent(baseUrl: string, serviceFilter?: IncludeExcludeFilter, tagFilter?: IncludeExcludeFilter,
  sort?: ChannelSortField, direction?: SortDirection): string {

  const channels = getAllChannels();
  const lines = [ "#EXTM3U", "" ];
  const sortField = sort ?? CONFIG.channels.channelSortField;
  const sortDir = direction ?? CONFIG.channels.channelSortDirection;

  // Sort channel entries using the specified (or saved) sort field and direction. Default is name ascending. Iterating entries rather than keys keeps channel
  // references fully typed throughout the loop, avoiding repeated Record lookups that noUncheckedIndexedAccess flags as possibly-undefined.
  const channelEntries = Object.entries(channels)
    .sort(([ keyA, channelA ], [ keyB, channelB ]) => compareChannelSort(channelA, keyA, channelB, keyB, sortField, sortDir));

  for(const [ name, channel ] of channelEntries) {

    // Apply the service filter if specified.
    if(serviceFilter) {

      const selectedKey = resolveServiceKey(name);
      const selectedTag = getServiceTagForChannel(selectedKey);
      const hasMatch = serviceFilter.tags.includes(selectedTag);

      // In include mode, skip channels whose selected service doesn't match any filter tag. In exclude mode, skip channels whose selected service matches.
      if(serviceFilter.exclude ? hasMatch : !hasMatch) {

        continue;
      }
    }

    // Compute effective tags once per channel - used for both the tag filter check and M3U attribute generation. Tags are intersected with the active vocabulary
    // so deleted tags are invisible.
    const effectiveTags = getChannelEffectiveTags(channel);

    // Apply the tag filter if specified. Filter tags are lowercase (normalized by parseIncludeExcludeFilter), so compare case-insensitively.
    if(tagFilter) {

      const hasMatch = tagFilter.tags.some((filterTag) => effectiveTags.some((t) => tagsMatch(t, filterTag)));

      // In include mode, skip channels that don't have any matching tag. In exclude mode, skip channels that have a matching tag.
      if(tagFilter.exclude ? hasMatch : !hasMatch) {

        continue;
      }
    }

    // We use the channel key as the channel-id and the guide title (falling back to channel name) for display. HLS URLs are used for Channels DVR compatibility.
    const displayName = channel.guideTitle ?? channel.name ?? name;
    const streamUrl = baseUrl + "/hls/" + name + "/stream.m3u8";

    // Build the EXTINF line with required channel-id attribute and tvg-name for the friendly display name. Include channel-number when the user has specified one,
    // tvc-guide-stationid for Gracenote guide data when a stationId is defined, tvg-shift for EPG time offset, tvg-logo for custom channel logos, and group-title
    // for organizational tags (semicolon-delimited for IPTV middleware compatibility). Every user-controlled attribute value flows through escapeM3uAttribute so
    // an embedded double-quote, backslash, or line break cannot terminate the attribute early or break the EXTINF line. Structurally server-controlled values
    // (the channel key, numeric channelNumber and tvgShift, and the fixed placeholders constant) skip the helper because validation guarantees they cannot carry
    // those characters.
    const channelNumberAttr = channel.channelNumber ? " channel-number=\"" + String(channel.channelNumber) + "\"" : "";
    const groupTitleAttr = (effectiveTags.length > 0) ? " group-title=\"" + escapeM3uAttribute(effectiveTags.join(";")) + "\"" : "";
    const logoAttr = channel.logoUrl ? " tvg-logo=\"" + escapeM3uAttribute(channel.logoUrl) + "\"" : "";
    const stationIdAttr = channel.stationId ? " tvc-guide-stationid=\"" + escapeM3uAttribute(channel.stationId) + "\"" : "";
    const tvgShiftAttr = (channel.tvgShift !== undefined) ? " tvg-shift=\"" + String(channel.tvgShift) + "\"" : "";

    // For channels without EPG data, emit tvc-guide-tags so Channels DVR's Automatic Channels can filter on them. Static page channels also get
    // tvc-guide-placeholders to provide a 24-hour guide block since they display persistent content without time-based programming.
    let tvcTagsAttr = "";
    let tvcPlaceholdersAttr = "";

    if(!channel.stationId) {

      if(effectiveTags.length > 0) {

        tvcTagsAttr = " tvc-guide-tags=\"" + escapeM3uAttribute(effectiveTags.join(", ")) + "\"";
      }

      const { profile } = getProfileForChannel(channel);

      if(profile.staticCapture) {

        tvcPlaceholdersAttr = " tvc-guide-placeholders=\"86400\"";
      }
    }

    // The channel-id value is the iteration key (validated to /^[a-z0-9-]+$/ at the input boundary), so it cannot carry the structural characters that
    // escapeM3uAttribute exists to neutralize. We embed it directly. The comma-suffix display name is M3U's terminator-by-end-of-line position rather than a
    // quoted attribute, so backslash and double-quote are legal there and pass through verbatim - users see the original characters in their guide; only literal
    // CR/LF (which would split the EXTINF across multiple lines) gets collapsed to a single space.
    const attrs = "#EXTINF:-1 channel-id=\"" + name + "\"" + channelNumberAttr + groupTitleAttr + logoAttr + " tvg-name=\"" + escapeM3uAttribute(displayName) +
      "\"" + stationIdAttr + tvcPlaceholdersAttr + tvcTagsAttr + tvgShiftAttr;
    const extinfLine = attrs + "," + displayName.replace(/[\r\n]+/g, " ");

    lines.push(extinfLine);
    lines.push(streamUrl);
    lines.push("");
  }

  return lines.join("\n");
}

// Valid sort direction values for query parameter validation.
const VALID_SORT_DIRECTIONS = new Set<SortDirection>([ "asc", "desc" ]);

/**
 * Creates the playlist endpoint that serves an M3U playlist in Channels DVR format. The playlist lists all configured channels with their stream URLs, allowing
 * Channels DVR to import them as custom channels. The endpoint dynamically constructs URLs using the request host header so the playlist works regardless of how
 * the server is accessed (localhost, IP address, or hostname).
 * @param app - The Express application.
 */
export function setupPlaylistEndpoint(app: Express): void {

  // GET /playlist - Returns the M3U playlist file. Supports optional query parameters: ?service= for filtering by streaming service, ?tag= for filtering by
  // organizational tags, ?sort= for sort field override, and ?direction= for sort direction override.
  app.get("/playlist", (req: Request, res: Response): void => {

    const baseUrl = resolveBaseUrl(req);
    const serviceParam = typeof req.query["service"] === "string" ? req.query["service"].trim() : undefined;
    const tagParam = typeof req.query["tag"] === "string" ? req.query["tag"].trim() : undefined;
    const sortParam = typeof req.query["sort"] === "string" ? req.query["sort"].trim() || undefined : undefined;
    const directionParam = typeof req.query["direction"] === "string" ? req.query["direction"].trim().toLowerCase() || undefined : undefined;
    let serviceFilter: IncludeExcludeFilter | undefined;
    let tagFilter: IncludeExcludeFilter | undefined;

    // Parse and validate the service filter if specified.
    if(serviceParam) {

      const result = parseServiceFilter(serviceParam);

      if("error" in result) {

        sendValidationError(res, { error: result.error, validTags: result.validTags });

        return;
      }

      serviceFilter = result.filter;
    }

    // Parse and validate the tag filter if specified.
    if(tagParam) {

      const result = parseTagFilter(tagParam);

      if("error" in result) {

        sendValidationError(res, { error: result.error, validTags: result.validTags });

        return;
      }

      tagFilter = result.filter;
    }

    // Validate the sort field if specified.
    if(sortParam && !VALID_SORT_FIELDS.has(sortParam as ChannelSortField)) {

      sendValidationError(res, { error: "Invalid sort field: " + sortParam + ".", validFields: [...VALID_SORT_FIELDS].toSorted() });

      return;
    }

    // Validate the sort direction if specified.
    if(directionParam && !VALID_SORT_DIRECTIONS.has(directionParam as SortDirection)) {

      sendValidationError(res, { error: "Invalid sort direction: " + directionParam + ".", validDirections: [...VALID_SORT_DIRECTIONS] });

      return;
    }

    const sort = sortParam as ChannelSortField | undefined;
    const direction = directionParam as SortDirection | undefined;
    const playlist = generatePlaylistContent(baseUrl, serviceFilter, tagFilter, sort, direction);

    res.set("Content-Type", "audio/x-mpegurl");
    res.send(playlist);
  });
}
