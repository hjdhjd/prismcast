/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tokenExpiry.ts: Strategy-based parser for token expirations embedded in HLS manifest URLs.
 */
import type { Nullable } from "../types/index.ts";

/* This module recognizes token-expiry encodings used by CDNs that protect HLS manifests with short-lived tokens. The native streaming pipeline schedules a
 * proactive refresh before the token expires; without an expiry the refresh is skipped and the stream relies on reactive recovery when the token actually fails.
 *
 * Each token format gets its own pure strategy so the registry stays additive: adding support for a new CDN's token shape is a single TokenExpiryStrategy entry,
 * not a fork in the dispatcher. Strategies are tried in registration order and the first non-null result wins, so the order encodes precedence: cheap regex
 * matchers for well-known query/path patterns run before the structural JWT walker, which would otherwise incur a JSON parse on every URL.
 *
 * The dispatcher returns expiry in milliseconds since the Unix epoch, normalized from whichever unit the underlying token format declares (HLS CDN tokens
 * commonly emit seconds; JWTs are seconds-since-epoch per RFC 7519 section 4.1.4).
 */

/**
 * A single token-expiry parsing strategy. Parsers are pure functions of the URL string and must not perform I/O. Returning null signals "this strategy does not
 * recognize this URL" - the dispatcher then falls through to the next strategy.
 */
export interface TokenExpiryStrategy {

  // Identifier for debug logging. Lowercase, hyphenated, stable.
  readonly name: string;

  // Returns the token's absolute expiry in milliseconds since the Unix epoch, or null if this strategy does not recognize the URL.
  readonly parse: (url: string) => Nullable<number>;
}

/* Numeric expiry timestamps embedded in CDN tokens are commonly 10-digit seconds since the epoch, but some emit 13-digit milliseconds directly. We treat any
 * value below 1e12 (approximately year 33658 in milliseconds, but only year 2001 in seconds) as seconds and multiply; values at or above 1e12 are taken as
 * milliseconds verbatim. The cutoff is well past any plausible seconds-since-epoch value an HLS CDN would emit during this code's lifetime.
 */
const SECONDS_TO_MS_THRESHOLD = 1e12;

/**
 * Normalizes a numeric expiry timestamp to milliseconds. 10-digit values are interpreted as seconds since the epoch; 13-digit values are taken as milliseconds.
 *
 * @param value - The numeric timestamp from a token.
 * @returns The expiry in milliseconds since the Unix epoch.
 */
function normalizeExpiryToMs(value: number): number {

  return (value < SECONDS_TO_MS_THRESHOLD) ? (value * 1000) : value;
}

/**
 * Builds a regex-driven strategy. The given pattern must capture the numeric timestamp in group 1.
 *
 * @param name - Strategy identifier for debug logs.
 * @param pattern - Regex with the timestamp in capturing group 1.
 * @returns A strategy that runs the regex and normalizes the captured value to milliseconds.
 */
function regexStrategy(name: string, pattern: RegExp): TokenExpiryStrategy {

  return {

    name,
    parse: (url: string): Nullable<number> => {

      const match = pattern.exec(url);

      return match ? normalizeExpiryToMs(Number(match[1])) : null;
    }
  };
}

/**
 * Decodes a base64url-encoded segment to its UTF-8 string representation. The base64url alphabet replaces "+" with "-" and "/" with "_", and omits padding;
 * Node's Buffer.from(..., "base64") accepts both alphabets transparently when the input is well-formed, but explicitly mapping the URL-safe characters before
 * decoding makes the intent clear and keeps the implementation robust against future Buffer changes.
 *
 * @param segment - The base64url-encoded segment.
 * @returns The decoded UTF-8 string, or null if the segment cannot be decoded.
 */
function decodeBase64UrlSegment(segment: string): Nullable<string> {

  if(!segment) {

    return null;
  }

  // Translate base64url-specific characters to standard base64. Padding is reconstructed by rounding the length up to the nearest multiple of four.
  const standard = segment.replace(/-/g, "+").replace(/_/g, "/");
  const padded = standard.padEnd(Math.ceil(standard.length / 4) * 4, "=");

  try {

    return Buffer.from(padded, "base64").toString("utf8");
  } catch(_error) {

    return null;
  }
}

/**
 * Parses a single string value as a JWT and returns the value of the `exp` claim if present. RFC 7519 declares JWT structure as three base64url segments joined
 * by literal dots (header.payload.signature); the payload's `exp` claim is "the expiration time on or after which the JWT MUST NOT be accepted for processing,"
 * expressed in NumericDate (seconds since the epoch).
 *
 * @param value - The candidate JWT string. Already URL-decoded by the URL search-params reader.
 * @returns The expiry value in seconds since the epoch, or null when the value is not a JWT or has no exp claim.
 */
function parseJwtExpClaim(value: string): Nullable<number> {

  const parts = value.split(".");

  if(parts.length !== 3) {

    return null;
  }

  const payloadJson = decodeBase64UrlSegment(parts[1] ?? "");

  if(!payloadJson) {

    return null;
  }

  let claims: unknown;

  try {

    claims = JSON.parse(payloadJson);
  } catch(_error) {

    return null;
  }

  if(!claims || (typeof claims !== "object")) {

    return null;
  }

  const exp = (claims as { exp?: unknown }).exp;

  return (typeof exp === "number") ? exp : null;
}

/**
 * Strategy for plain query parameters of the form `?exp=N` or `&exp=N` where N is a 10-13 digit timestamp.
 */
const PLAIN_EXP_QUERY = regexStrategy("plain-exp-query", /[?&]exp=(\d{10,13})/);

/**
 * Strategy for Akamai-style tokens embedded in a query parameter (e.g., `hdnea=...~exp=N~...`). The expiry is delimited by tildes within the parameter value.
 */
const AKAMAI_QUERY_TOKEN = regexStrategy("akamai-query-token", /~exp=(\d{10,13})~/);

/**
 * Strategy for Akamai-style tokens embedded in the URL path (e.g., `/exp=N~acl=...`). Some Akamai deployments encode the token segments into the path rather
 * than a query parameter.
 */
const AKAMAI_PATH_TOKEN = regexStrategy("akamai-path-token", /\/exp=(\d{10,13})~/);

/**
 * Strategy for URL-encoded `hdnts` tokens (e.g., `hdnts=exp%3DN`). The "%3D" sequence is the percent-encoding of "=" inside the parameter's value, which some
 * services emit when stuffing structured tokens into a single parameter.
 */
const URL_ENCODED_HDNTS = regexStrategy("url-encoded-hdnts", /exp%3D(\d{10,13})/);

/**
 * Strategy for JWTs carried in any query parameter. Iterates the URL's search parameters and tries to parse each value as a JWT; the first value with a valid
 * `exp` claim wins. Used by services that pack a signed JWT into an opaque parameter (e.g., Angelcam's `?token=<jwt>`), where neither the regex strategies above
 * nor the parameter name (which varies between services) is reliable.
 */
const JWT_IN_QUERY: TokenExpiryStrategy = {

  name: "jwt-in-query",
  parse: (url: string): Nullable<number> => {

    let parsed: URL;

    try {

      parsed = new URL(url);
    } catch(_error) {

      return null;
    }

    for(const value of parsed.searchParams.values()) {

      const exp = parseJwtExpClaim(value);

      if(exp !== null) {

        // JWT exp is in seconds since the epoch per RFC 7519. Normalize to milliseconds.
        return normalizeExpiryToMs(exp);
      }
    }

    return null;
  }
};

/**
 * Ordered registry of token-expiry strategies. Cheaper, more specific patterns run first; the structural JWT walker is last because it parses every query value
 * and falls back through to JSON parsing on near-miss segments. Adding a new token format is a single entry here.
 */
export const TOKEN_EXPIRY_STRATEGIES: readonly TokenExpiryStrategy[] = [

  PLAIN_EXP_QUERY,
  AKAMAI_QUERY_TOKEN,
  AKAMAI_PATH_TOKEN,
  URL_ENCODED_HDNTS,
  JWT_IN_QUERY
];

/**
 * Parses the absolute token expiration from an HLS manifest or variant URL. Iterates TOKEN_EXPIRY_STRATEGIES in order and returns the first non-null result;
 * each strategy normalizes its output to milliseconds since the Unix epoch so callers do not need to care which encoding produced the timestamp.
 *
 * @param url - The URL to parse for token expiration.
 * @returns The expiration timestamp in milliseconds since the Unix epoch, or null if no strategy recognizes the URL.
 */
export function parseTokenExpiry(url: string): Nullable<number> {

  for(const strategy of TOKEN_EXPIRY_STRATEGIES) {

    const expiry = strategy.parse(url);

    if(expiry !== null) {

      return expiry;
    }
  }

  return null;
}
