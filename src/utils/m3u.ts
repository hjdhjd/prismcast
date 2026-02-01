/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * m3u.ts: M3U playlist parsing utilities for PrismCast.
 */
import type { Nullable } from "../types/index.js";

/*
 * M3U PARSING
 *
 * This module provides utilities for parsing M3U playlist files and extracting channel information. The parser handles extended M3U format with #EXTINF tags and extracts
 * relevant attributes like tvg-name, tvg-id, and tvc-guide-stationid.
 *
 * Standard M3U format:
 * #EXTM3U
 * #EXTINF:-1 tvg-id="cnn.us" tvg-name="CNN" tvc-guide-stationid="12345",CNN Live
 * https://example.com/cnn-stream
 */

// Maximum length for generated channel keys.
const MAX_KEY_LENGTH = 50;

/**
 * Represents a channel parsed from an M3U playlist.
 */
export interface M3UChannel {

  // Display name extracted from tvg-name or #EXTINF comma suffix.
  name: string;

  // Optional Gracenote station ID for EPG integration.
  stationId?: string;

  // Stream URL.
  url: string;
}

/**
 * Result of parsing an M3U playlist.
 */
export interface M3UParseResult {

  // Successfully parsed channels.
  channels: M3UChannel[];

  // Parse errors with line numbers for user feedback.
  errors: string[];
}

/**
 * Generates a URL-safe channel key from a display name. The key is used as the channel identifier in URLs and configuration.
 * @param name - The display name to convert.
 * @returns A lowercase, hyphen-separated key suitable for URLs.
 *
 * Examples:
 * - "CNN Live!" → "cnn-live"
 * - "BBC News 24/7" → "bbc-news-247"
 * - "  Spaces  Everywhere  " → "spaces-everywhere"
 */
export function generateChannelKey(name: string): string {

  return name
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9-]/g, "")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, MAX_KEY_LENGTH);
}

/**
 * Extracts an attribute value from an #EXTINF line. Handles both quoted and unquoted attribute values.
 * @param line - The #EXTINF line to parse.
 * @param attribute - The attribute name to extract (e.g., "tvg-name").
 * @returns The attribute value, or null if not found.
 */
function extractAttribute(line: string, attribute: string): Nullable<string> {

  // Match attribute="value" (quoted).
  const quotedPattern = new RegExp(attribute + "=\"([^\"]*)\"", "i");
  const quotedMatch = line.match(quotedPattern);

  if(quotedMatch) {

    return quotedMatch[1];
  }

  // Match attribute=value (unquoted, ends at space or comma).
  const unquotedPattern = new RegExp(attribute + "=([^\\s,\"]+)", "i");
  const unquotedMatch = line.match(unquotedPattern);

  if(unquotedMatch) {

    return unquotedMatch[1];
  }

  return null;
}

/**
 * Extracts the display name from an #EXTINF line. First tries tvg-name attribute, then falls back to the comma-separated suffix.
 * @param line - The #EXTINF line to parse.
 * @returns The display name, or null if not found.
 */
function extractName(line: string): Nullable<string> {

  // First try tvg-name attribute.
  const tvgName = extractAttribute(line, "tvg-name");

  if(tvgName && (tvgName.trim().length > 0)) {

    return tvgName.trim();
  }

  // Fall back to comma suffix (the part after the last comma in #EXTINF line). Format: #EXTINF:-1 attributes,Display Name
  const commaIndex = line.lastIndexOf(",");

  if(commaIndex !== -1) {

    const suffix = line.slice(commaIndex + 1).trim();

    if(suffix.length > 0) {

      return suffix;
    }
  }

  return null;
}

/**
 * Extracts the station ID from an #EXTINF line. Prioritizes tvc-guide-stationid (Channels DVR format), falls back to tvg-id.
 * @param line - The #EXTINF line to parse.
 * @returns The station ID, or undefined if not found.
 */
function extractStationId(line: string): string | undefined {

  // Prioritize tvc-guide-stationid for Channels DVR compatibility.
  const tvcStationId = extractAttribute(line, "tvc-guide-stationid");

  if(tvcStationId && (tvcStationId.trim().length > 0)) {

    return tvcStationId.trim();
  }

  // Fall back to tvg-id.
  const tvgId = extractAttribute(line, "tvg-id");

  if(tvgId && (tvgId.trim().length > 0)) {

    return tvgId.trim();
  }

  return undefined;
}

/**
 * Parses an M3U playlist and extracts channel information. Handles extended M3U format with #EXTINF tags.
 * @param content - The M3U file content as a string.
 * @returns Parse result containing channels and any errors encountered.
 */
export function parseM3U(content: string): M3UParseResult {

  const channels: M3UChannel[] = [];
  const errors: string[] = [];
  const lines = content.split(/\r?\n/);

  let pendingExtinf: Nullable<{ lineNumber: number; name: string; stationId?: string }> = null;

  for(let i = 0; i < lines.length; i++) {

    const lineNumber = i + 1;
    const line = lines[i].trim();

    // Skip empty lines and comments (except #EXTINF).
    if((line.length === 0) || ((line.startsWith("#")) && !line.startsWith("#EXTINF"))) {

      continue;
    }

    // Parse #EXTINF line.
    if(line.startsWith("#EXTINF:")) {

      // If we have a pending #EXTINF without a URL, report an error.
      if(pendingExtinf) {

        errors.push("Line " + String(pendingExtinf.lineNumber) + ": Missing URL after #EXTINF.");
      }

      const name = extractName(line);

      if(!name) {

        errors.push("Line " + String(lineNumber) + ": Could not extract channel name from #EXTINF.");
        pendingExtinf = null;

        continue;
      }

      pendingExtinf = {

        lineNumber,
        name,
        stationId: extractStationId(line)
      };

      continue;
    }

    // Parse URL line (must follow an #EXTINF).
    if(line.startsWith("http://") || line.startsWith("https://")) {

      if(!pendingExtinf) {

        // URL without preceding #EXTINF - skip silently (no name available).
        continue;
      }

      channels.push({

        name: pendingExtinf.name,
        stationId: pendingExtinf.stationId,
        url: line
      });

      pendingExtinf = null;

      continue;
    }

    // Unknown line format - skip silently.
  }

  // Check for trailing #EXTINF without URL.
  if(pendingExtinf) {

    errors.push("Line " + String(pendingExtinf.lineNumber) + ": Missing URL after #EXTINF.");
  }

  return { channels, errors };
}
