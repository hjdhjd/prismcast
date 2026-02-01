/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * deviceId.ts: HDHomeRun DeviceID generation for PrismCast.
 */
import crypto from "node:crypto";

/*
 * HDHOMERUN DEVICE ID
 *
 * HDHomeRun devices are identified by an 8-character hex string (32 bits). The checksum is computed using an XOR-based algorithm from the libhdhomerun reference
 * implementation (hdhomerun_discover_validate_device_id in hdhomerun_discover.c). The checksum ensures that mistyped or corrupted device IDs are detected. Plex
 * validates this checksum when discovering tuners, so we must generate conformant IDs.
 *
 * Algorithm: The 32-bit value is processed as 8 nibbles (4 bits each). Nibbles at even positions (0, 2, 4, 6 counting from the most significant) are
 * transformed through a lookup table before XOR. Nibbles at odd positions (1, 3, 5, 7) are XORed directly. A valid ID produces a checksum of zero.
 *
 * We generate a random 24-bit prefix (6 hex chars), then brute-force the final byte (2 hex chars) to satisfy the checksum constraint.
 */

// Lookup table from libhdhomerun for DeviceID checksum validation. Applied to nibbles at even positions only.
const DEVICEID_LOOKUP = [

  0xA, 0x5, 0xF, 0x6, 0x7, 0xC, 0x1, 0xB,
  0x9, 0x2, 0x8, 0xD, 0x4, 0x3, 0xE, 0x0
];

/**
 * Computes the DeviceID checksum matching libhdhomerun's hdhomerun_discover_validate_device_id. Even-positioned nibbles (0, 2, 4, 6) go through the lookup
 * table; odd-positioned nibbles (1, 3, 5, 7) are XORed directly. A valid DeviceID produces a checksum of zero.
 * @param nibbles - Array of 8 nibble values (0-15), most significant first.
 * @returns The checksum value. Zero indicates a valid DeviceID.
 */
function computeChecksum(nibbles: number[]): number {

  let checksum = 0;

  for(let i = 0; i < 8; i++) {

    // Even positions use the lookup table, odd positions use the raw nibble value.
    if((i % 2) === 0) {

      checksum ^= DEVICEID_LOOKUP[nibbles[i]];
    } else {

      checksum ^= nibbles[i];
    }
  }

  return checksum;
}

/**
 * Validates an HDHomeRun DeviceID checksum. A valid DeviceID produces zero when processed through the alternating lookup/raw XOR algorithm.
 * @param deviceId - The 8-character hex DeviceID to validate.
 * @returns True if the checksum is valid.
 */
export function validateDeviceId(deviceId: string): boolean {

  if(!/^[0-9a-fA-F]{8}$/.test(deviceId)) {

    return false;
  }

  const nibbles = Array.from(deviceId, (ch) => parseInt(ch, 16));

  return computeChecksum(nibbles) === 0;
}

/**
 * Generates a valid HDHomeRun DeviceID. Creates a random 24-bit prefix and finds a final byte that satisfies the checksum constraint.
 * @returns An 8-character lowercase hex string with a valid HDHomeRun checksum.
 */
export function generateDeviceId(): string {

  // Generate 3 random bytes (24 bits = 6 hex chars) for the prefix.
  const prefixBytes = crypto.randomBytes(3);
  const prefix = prefixBytes.toString("hex");

  // Parse the prefix into nibbles and compute the partial checksum for the first 6 nibbles (positions 0-5). We compute this directly rather than padding to 8 and
  // using computeChecksum(), because padding with zeros would include LOOKUP[0] for position 6 and skew the result.
  const nibbles = Array.from(prefix, (ch) => parseInt(ch, 16));

  let partialChecksum = 0;

  for(let i = 0; i < 6; i++) {

    if((i % 2) === 0) {

      partialChecksum ^= DEVICEID_LOOKUP[nibbles[i]];
    } else {

      partialChecksum ^= nibbles[i];
    }
  }

  // Find the final byte (2 nibbles) that makes the total checksum zero. Position 6 is even (lookup table), position 7 is odd (raw). We try all 256 values. A
  // solution is always guaranteed since for any 4-bit partial checksum, at least one of the 256 final byte combinations will zero the full checksum.
  for(let finalByte = 0; finalByte < 256; finalByte++) {

    const highNibble = (finalByte >> 4) & 0xF;
    const lowNibble = finalByte & 0xF;
    const finalChecksum = partialChecksum ^ DEVICEID_LOOKUP[highNibble] ^ lowNibble;

    if(finalChecksum === 0) {

      return prefix + finalByte.toString(16).padStart(2, "0");
    }
  }

  // This is unreachable — a solution always exists within the 256 candidates. Fall back to a known-valid ID as a safety net.
  return "1000000f";
}
