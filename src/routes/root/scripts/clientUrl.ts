/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clientUrl.ts: The single client-side URL-safety function and the browser-runtime assignment that installs it.
 */

/* This module is the single source of truth for vetting a URL before it is placed in a URL-bearing HTML attribute (an img src, an a href, ...) in client-rendered
 * markup. It is the companion to clientEscape.ts: scheme safety and attribute encoding are independent concerns, so they are kept as two composable primitives -
 * clientSafeUrl decides whether a URL's scheme is allowed, and clientEscapeHtml encodes the result for the attribute context. Callers compose them as
 * escapeHtml(safeUrl(url)), and each concern has exactly one home. There is no server-side twin because the server never renders these values into a URL-executing
 * sink - it emits only inert data-* attributes (HTML-escaped at that boundary) which the client later reads and turns into a real img src; the client is therefore
 * the sole URL render boundary.
 */

/**
 * Returns the URL unchanged when its scheme is safe for a URL-bearing HTML attribute, or the empty string when it is dangerous. Absolute URLs are permitted only
 * with the http or https scheme; relative and protocol-relative URLs carry no scheme of their own (they inherit the page's) and pass through. Everything else -
 * javascript:, data:, vbscript:, file:, blob:, and the like - collapses to "" so the attribute renders inert (an empty src triggers the onerror fallback rather
 * than executing or loading anything). The WHATWG URL parser is used deliberately rather than a hand-rolled scheme regex: it normalizes scheme-obfuscation tricks
 * (mixed case, embedded tabs and newlines, leading control characters) per spec, and a relative URL throws because there is no base, which is exactly the signal
 * that it carries no scheme to vet. new URL is available identically in the browser and in Node, so the function is testable without a DOM and ships to the browser
 * via Function.prototype.toString(); its body references only its parameter and the URL global. The caller still HTML-escapes the result for the attribute - this
 * function owns scheme safety only, not encoding.
 * @param value - The URL to vet.
 * @returns The original URL when its scheme is http/https or it is relative, otherwise the empty string.
 */
export function clientSafeUrl(value: string): string {

  try {

    const protocol = new URL(value).protocol;

    return ((protocol === "http:") || (protocol === "https:")) ? value : "";
  } catch {

    return value;
  }
}

/**
 * Emits the browser-runtime statements that install clientSafeUrl as the global window.safeUrl plus an IIFE-local const alias, mirroring
 * generateClientEscapeAssignment. The shared utilities script concatenates this near the top of its IIFE so safeUrl resolves - as the bare identifier inside
 * shared.ts's own renderers and as window.safeUrl for any later client script - before any of them runs. The function source is serialized via
 * Function.prototype.toString(); TypeScript annotations strip to whitespace under Node's type stripping and to nothing under tsc compilation, so the runtime body
 * is identical between test and production.
 * @returns The two-line script fragment, indented two spaces to sit inside the shared utilities IIFE.
 */
export function generateClientSafeUrlAssignment(): string {

  return "  const safeUrl = " + clientSafeUrl.toString() + ";\n  window.safeUrl = safeUrl;";
}
