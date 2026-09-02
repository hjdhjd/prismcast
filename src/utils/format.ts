/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * format.ts: Formatting utilities for PrismCast.
 */
import type { Nullable } from "../types/index.ts";

/**
 * Formats the current date and time as a log timestamp string: `yyyy/mm/dd hh:mm:ss.mmm AM/PM`. Uses 12-hour time with decimalized seconds and AM/PM.
 * Single source of truth for all log timestamp formatting - used by the console wrapper in app.ts, the file logger, the Morgan HTTP request logger, and the SSE log
 * emitter.
 * @returns Formatted timestamp string.
 */
export function formatTimestamp(): string {

  const now = new Date();
  const yyyy = String(now.getFullYear());
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  let hours = now.getHours();
  const ampm = (hours >= 12) ? "PM" : "AM";

  hours = hours % 12;
  hours ||= 12;

  const hh = String(hours).padStart(2, "0");
  const min = String(now.getMinutes()).padStart(2, "0");
  const ss = String(now.getSeconds()).padStart(2, "0");
  const ms = String(now.getMilliseconds()).padStart(3, "0");

  return yyyy + "/" + mm + "/" + dd + " " + hh + ":" + min + ":" + ss + "." + ms + " " + ampm;
}

/**
 * Formats a duration as a human-readable string. The format varies based on duration length:
 * - Less than 60 seconds: "17s"
 * - Less than 1 hour: "6m 39s"
 * - 1 hour or more: "1h 23m"
 *
 * Zero-value trailing components are omitted (e.g., exactly 2 minutes returns "2m", not "2m 0s").
 * @param value - Duration value.
 * @param unit - The unit of the value: "ms" (default) or "s".
 * @returns Formatted duration string.
 */
export function formatDuration(value: number, unit: "ms" | "s" = "ms"): string {

  const totalSeconds = unit === "ms" ? Math.round(value / 1000) : value;
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if(hours > 0) {

    return minutes > 0 ? String(hours) + "h " + String(minutes) + "m" : String(hours) + "h";
  }

  if(minutes > 0) {

    return seconds > 0 ? String(minutes) + "m " + String(seconds) + "s" : String(minutes) + "m";
  }

  return String(seconds) + "s";
}

/**
 * Formats a Unix millisecond timestamp as a human-readable relative time string (e.g., "2 minutes ago", "3 hours ago", "5 days ago"). Used for health indicator
 * tooltips where absolute timestamps would be harder to interpret at a glance.
 * @param timestamp - Unix millisecond timestamp.
 * @returns Relative time string.
 */
export function formatTimeAgo(timestamp: number): string {

  const seconds = Math.floor((Date.now() - timestamp) / 1000);

  if(seconds < 60) {

    return "just now";
  }

  const minutes = Math.floor(seconds / 60);

  if(minutes < 60) {

    return String(minutes) + (minutes === 1 ? " minute ago" : " minutes ago");
  }

  const hours = Math.floor(minutes / 60);

  if(hours < 24) {

    return String(hours) + (hours === 1 ? " hour ago" : " hours ago");
  }

  const days = Math.floor(hours / 24);

  return String(days) + (days === 1 ? " day ago" : " days ago");
}

/**
 * Display labels for the standard vertical resolutions, keyed by the height in pixels. This is the single source of truth for the label a resolution is shown
 * under: the native HLS quality suffix in the log, the stream detail's codec line in the web UI, and anything else that renders a resolution to a person all
 * read it from here. A height with no entry has no standard label, and its caller falls back to the raw resolution string.
 */
export const RESOLUTION_LABELS: Record<string, string> = { "1080": "1080p", "2160": "4K", "360": "360p", "480": "480p", "720": "720p" };

/**
 * Formats a pixel size as the "WIDTHxHEIGHT" string the codebase carries resolutions in. This is the one producer of that form, and it matches the shape the
 * native probe reads verbatim out of a manifest's RESOLUTION attribute, so a resolution that came off the wire and one this function built are the same string
 * to every consumer downstream - the label lookup below included.
 * @param width - The width in pixels.
 * @param height - The height in pixels.
 * @returns The size as "WIDTHxHEIGHT".
 */
export function formatResolution(width: number, height: number): string {

  return String(width) + "x" + String(height);
}

/**
 * Formats a "WIDTHxHEIGHT" resolution string as its standard display label, falling back to the resolution string itself when the height carries no standard
 * label. The fallback is what keeps a non-standard rendition visible rather than dropped: a 1234x999 variant renders as "1234x999" instead of disappearing.
 * @param resolution - The resolution as "WIDTHxHEIGHT".
 * @returns The display label, or the resolution string when the height has no label.
 */
export function formatResolutionLabel(resolution: string): string {

  const height = resolution.split("x")[1];

  return (height ? RESOLUTION_LABELS[height] : undefined) ?? resolution;
}

/**
 * Extracts a concise domain from a URL by keeping only the last two portions of the hostname (e.g., "watch.foodnetwork.com" becomes "foodnetwork.com",
 * "www.hulu.com" becomes "hulu.com"). Used as a standard domain key for DOMAIN_CONFIG lookups and as a display fallback when no service name is configured.
 * @param url - The URL to extract the domain from.
 * @returns The concise domain, or the original URL if parsing fails.
 */
export function extractDomain(url: string): string {

  try {

    const hostname = new URL(url).hostname;
    const parts = hostname.split(".");

    // Keep only the last two parts (e.g., "foodnetwork.com"). For single-part hostnames (e.g., "localhost"), return as-is.
    if(parts.length > 2) {

      return parts.slice(-2).join(".");
    }

    return hostname;
  } catch {

    return url;
  }
}

/**
 * Extracts the pathname portion of a URL - everything from the leading slash up to but excluding the query string and fragment - or null when the URL cannot be
 * parsed. Placed beside extractDomain as the shared home for URL-part extraction, but with the opposite parse-failure contract: extractDomain returns the original
 * string as a display fallback, while extractPathname returns null so a caller can tell could-not-parse apart from a genuinely parsed pathname. Membership
 * comparisons need that distinction - treating an unparseable URL as a non-matching pathname would be actively wrong, so null lets the caller stay
 * member-conservative instead.
 * @param url - The URL to extract the pathname from.
 * @returns The pathname, or null when parsing fails.
 */
export function extractPathname(url: string): Nullable<string> {

  try {

    return new URL(url).pathname;
  } catch {

    return null;
  }
}

/**
 * Capitalizes the first letter of a string.
 * @param str - The string to capitalize.
 * @returns The string with the first letter capitalized.
 */
export function capitalize(str: string): string {

  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Serializes a value to JSON with all object keys sorted alphabetically at every depth. This ensures consistent, diff-friendly output for all persisted JSON
 * files and exported JSON responses. Array element order is preserved.
 * @param data - The value to serialize.
 * @param indent - Number of spaces for indentation (default: 2). Pass 0 for compact output.
 * @returns The JSON string.
 */
export function stringifySorted(data: unknown, indent = 2): string {

  return JSON.stringify(data, (_key: string, value: unknown) => {

    if(value && (typeof value === "object") && !Array.isArray(value)) {

      return Object.fromEntries(Object.entries(value as Record<string, unknown>).toSorted(([a], [b]) => a.localeCompare(b)));
    }

    return value;
  }, indent);
}
