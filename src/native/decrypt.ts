/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * decrypt.ts: AES-128 key fetching and segment decryption for native HLS streaming.
 */
import { LOG, chromeFetch } from "../utils/index.js";
import type { Nullable } from "../types/index.js";
import { createDecipheriv } from "node:crypto";

/* AES-128-CBC decryption for HLS segments. The HLS spec defines AES-128 encryption where each segment is encrypted with a 16-byte key using AES-128-CBC mode. The
 * initialization vector (IV) is either explicitly provided in the #EXT-X-KEY tag or derived from the media sequence number per the HLS specification (RFC 8216
 * Section 5.2).
 *
 * This module contains pure functions with no module-level state. Key caching is handled per-stream by the proxy's own currentKey/currentKeyUrl variables, which
 * avoids cross-stream interference when multiple native streams run simultaneously with different keys.
 */

// Timeout for key fetch requests.
const KEY_FETCH_TIMEOUT = 10000;

/**
 * Fetches an AES-128 decryption key from the given URL.
 *
 * @param keyUrl - The key URL from the #EXT-X-KEY URI attribute.
 * @returns The 16-byte decryption key, or null if the fetch fails.
 */
export async function fetchDecryptionKey(keyUrl: string): Promise<Nullable<Buffer>> {

  try {

    const response = await chromeFetch(keyUrl, { signal: AbortSignal.timeout(KEY_FETCH_TIMEOUT) });

    if(!response.ok) {

      LOG.debug("native:decrypt", "Decryption key fetch failed with HTTP %s.", response.status);

      return null;
    }

    const buffer = Buffer.from(await response.arrayBuffer());

    if(buffer.length !== 16) {

      LOG.warn("Decryption key has unexpected size: %s bytes (expected 16).", buffer.length);

      return null;
    }

    LOG.debug("native:decrypt", "Decryption key fetched: %s bytes from %s.", buffer.length, keyUrl.slice(0, 80));

    return buffer;
  } catch(error) {

    LOG.debug("native:decrypt", "Decryption key fetch error: %s.", String(error));

    return null;
  }
}

/**
 * Decrypts an AES-128-CBC encrypted HLS segment.
 *
 * @param data - The encrypted segment data.
 * @param key - The 16-byte AES-128 key.
 * @param iv - The 16-byte initialization vector.
 * @returns The decrypted segment data.
 */
export function decryptSegment(data: Buffer, key: Buffer, iv: Buffer): Buffer {

  const decipher = createDecipheriv("aes-128-cbc", key, iv);
  const decrypted = Buffer.concat([ decipher.update(data), decipher.final() ]);

  LOG.debug("native:decrypt", "Decrypted segment: %s bytes input, %s bytes output.", data.length, decrypted.length);

  return decrypted;
}

/**
 * Derives the initialization vector from a media sequence number per the HLS specification (RFC 8216 Section 5.2). When no explicit IV is provided in the
 * #EXT-X-KEY tag, the IV is the big-endian binary representation of the media sequence number, zero-padded to 16 bytes.
 *
 * @param mediaSequence - The media sequence number of the segment.
 * @returns A 16-byte Buffer containing the derived IV.
 */
export function deriveIvFromSequence(mediaSequence: number): Buffer {

  const iv = Buffer.alloc(16);

  // Write the sequence number as a big-endian 32-bit integer in the last 4 bytes. This matches the HLS spec: the sequence number occupies the least significant
  // bytes of the 128-bit IV.
  iv.writeUInt32BE(mediaSequence, 12);

  return iv;
}

/**
 * Parses an explicit IV value from a hex string in the #EXT-X-KEY tag. The IV is specified as IV=0x followed by 32 hex digits.
 *
 * @param ivHex - The hex string (with or without the 0x prefix).
 * @returns The 16-byte IV Buffer, or null if parsing fails.
 */
export function parseExplicitIv(ivHex: string): Nullable<Buffer> {

  // Strip the 0x or 0X prefix if present.
  const cleaned = ivHex.startsWith("0x") || ivHex.startsWith("0X") ? ivHex.slice(2) : ivHex;

  if(cleaned.length !== 32) {

    LOG.debug("native:decrypt", "Invalid IV hex length: %s (expected 32).", cleaned.length);

    return null;
  }

  return Buffer.from(cleaned, "hex");
}
