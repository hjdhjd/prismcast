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
