/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * format.ts: Formatting utilities for PrismCast.
 */

/**
 * Formats a duration in milliseconds as a human-readable string. The format varies based on duration length:
 * - Less than 60 seconds: "17s"
 * - Less than 1 hour: "6m 39s"
 * - 1 hour or more: "1h 23m"
 * @param ms - Duration in milliseconds.
 * @returns Formatted duration string.
 */
export function formatDuration(ms: number): string {

  const totalSeconds = Math.round(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if(hours > 0) {

    return [ String(hours), "h ", String(minutes), "m" ].join("");
  }

  if(minutes > 0) {

    return [ String(minutes), "m ", String(seconds), "s" ].join("");
  }

  return [ String(seconds), "s" ].join("");
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
 * Extracts a concise domain from a URL by keeping only the last two portions of the hostname (e.g., "watch.foodnetwork.com" becomes "foodnetwork.com",
 * "www.hulu.com" becomes "hulu.com"). Used as a standard domain key for DOMAIN_CONFIG lookups and as a display fallback when no provider name is configured.
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
