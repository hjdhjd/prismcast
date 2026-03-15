/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * chromeFetch.ts: Chrome User-Agent aware fetch wrapper for external HTTP requests.
 */
import type { Nullable } from "../types/index.js";

/* When PrismCast fetches manifests, segments, decryption keys, and other resources from provider CDNs, the requests should appear to originate from the same Chrome
 * browser that is rendering the page. This module stores the Chrome User-Agent string (captured at browser launch) and provides a drop-in fetch() wrapper that
 * injects it into outgoing requests. The setter/getter pattern avoids a circular dependency between utils/ and browser/ — browser/index.ts calls the setter at
 * launch and cleanup, while consumer modules import chromeFetch() without importing from browser/.
 */

// The Chrome User-Agent string captured at browser launch. Null when the browser is not running.
let currentUserAgent: Nullable<string> = null;

/**
 * Sets the Chrome User-Agent string. Called by browser/index.ts when the browser launches (with the UA) and when it disconnects or closes (with null).
 * @param ua - The User-Agent string from the Chrome browser, or null to clear.
 */
export function setChromeUserAgent(ua: Nullable<string>): void {

  currentUserAgent = ua;
}

/**
 * Returns the current Chrome User-Agent string, or null if the browser is not running.
 * @returns The Chrome User-Agent string or null.
 */
export function getChromeUserAgent(): Nullable<string> {

  return currentUserAgent;
}

/**
 * Fetch wrapper that injects the Chrome User-Agent header into outgoing requests. When the browser is running, the User-Agent header is set to match Chrome's
 * actual UA string so CDN requests appear to originate from the same browser. When the browser is not running, falls through to plain fetch() with no UA
 * injection. Any caller-supplied User-Agent header takes precedence and is not overwritten.
 * @param url - The URL to fetch.
 * @param init - Optional fetch options (method, headers, signal, etc.).
 * @returns The fetch Response.
 */
export async function chromeFetch(url: string | URL, init?: RequestInit): Promise<Response> {

  const ua = currentUserAgent;

  if(!ua) {

    return fetch(url, init);
  }

  // Merge the Chrome UA into the request headers. We use the Headers constructor to normalize whatever format the caller passed (plain object, Headers instance,
  // or array of key-value pairs), then set User-Agent only if the caller did not already provide one.
  const headers = new Headers(init?.headers);

  if(!headers.has("User-Agent")) {

    headers.set("User-Agent", ua);
  }

  return fetch(url, { ...init, headers });
}
