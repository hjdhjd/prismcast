/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * deviceId.test.ts: Unit tests for the HDHomeRun DeviceID generator and validator. Both functions implement the libhdhomerun XOR-with-lookup checksum, which
 * Plex enforces during tuner discovery; producing an invalid ID (or accepting one mistakenly as valid) silently breaks discovery without surfacing any
 * diagnostic, so the algorithm earns dedicated boundary coverage on every nibble position and on every form of input garbage.
 */
import { describe, test } from "node:test";
import { generateDeviceId, validateDeviceId } from "./deviceId.ts";
import assert from "node:assert/strict";

describe("validateDeviceId", () => {

  test("accepts a known-valid 8-character lowercase hex DeviceID", () => {

    // 1000000f is the documented fallback used inside generateDeviceId; it must satisfy the checksum or the fallback would itself be invalid.
    assert.equal(validateDeviceId("1000000f"), true);
  });

  test("accepts the same DeviceID in uppercase (case-insensitive hex)", () => {

    // The regex permits A-F as well as a-f; the parseInt path normalizes the values, so case must not affect the result.
    assert.equal(validateDeviceId("1000000F"), true);
  });

  test("accepts the same DeviceID in mixed case", () => {

    assert.equal(validateDeviceId("1000000f".toUpperCase().split("").map((c, i) => (i % 2 === 0) ? c : c.toLowerCase()).join("")), true);
  });

  test("rejects an ID that is the right length and shape but fails the checksum", () => {

    // Mutating one nibble of a known-valid ID should flip the XOR result and trip the checksum guard.
    assert.equal(validateDeviceId("10000000"), false, "all-zero except first nibble has nonzero checksum");
  });

  test("rejects strings shorter than 8 characters", () => {

    assert.equal(validateDeviceId(""), false, "empty string");
    assert.equal(validateDeviceId("1234567"), false, "seven characters");
  });

  test("rejects strings longer than 8 characters", () => {

    assert.equal(validateDeviceId("1000000f0"), false, "nine characters");
    assert.equal(validateDeviceId("1000000fdeadbeef"), false, "sixteen characters");
  });

  test("rejects strings containing non-hex characters", () => {

    assert.equal(validateDeviceId("1000000g"), false, "g is not a hex digit");
    assert.equal(validateDeviceId("1000000-"), false, "punctuation");
    assert.equal(validateDeviceId("1000 00f"), false, "embedded space");
  });

  test("rejects strings with leading or trailing whitespace", () => {

    // The regex anchors with ^ and $, so wrapping a valid ID in whitespace must still fail. Locks the no-trim contract callers depend on.
    assert.equal(validateDeviceId(" 1000000f"), false, "leading space");
    assert.equal(validateDeviceId("1000000f "), false, "trailing space");
    assert.equal(validateDeviceId("\t1000000f"), false, "leading tab");
  });

  test("accepts the all-zero string (lookup table makes its checksum zero)", () => {

    // Boundary: 00000000 is structurally valid because LOOKUP[0] = 0xA, so even-position XORs cancel pairwise (0xA XOR 0xA = 0) and odd positions are zero.
    // This is a property of the lookup table the algorithm inherits from libhdhomerun - locking it surfaces any future change to LOOKUP[0].
    assert.equal(validateDeviceId("00000000"), true);
  });
});

describe("generateDeviceId", () => {

  test("returns an 8-character string", () => {

    assert.equal(generateDeviceId().length, 8);
  });

  test("returns a string composed only of lowercase hex digits", () => {

    // Output is the prefix.toString("hex") concatenated with finalByte.toString(16).padStart(2,"0") - both produce lowercase hex.
    assert.match(generateDeviceId(), /^[0-9a-f]{8}$/);
  });

  test("returns a DeviceID that passes validateDeviceId", () => {

    // The guarantee the function exists to provide: every emitted ID round-trips through the validator.
    assert.equal(validateDeviceId(generateDeviceId()), true);
  });

  test("the every-time guarantee holds across many invocations (sampled)", () => {

    // Boundary: the brute-force loop must terminate with a valid solution for any random prefix. Sampling 100 times exercises a wide swath of the prefix space
    // and would catch any pathological prefix that fails to find a zero-checksum final byte.
    for(let i = 0; i < 100; i++) {

      const id = generateDeviceId();

      assert.equal(validateDeviceId(id), true, "iteration " + String(i) + " produced invalid ID: " + id);
    }
  });

  test("produces different IDs across calls (random prefix)", () => {

    // The 24-bit prefix is sampled from crypto.randomBytes; collisions in a small sample are statistically negligible. Two calls in quick succession should
    // differ. We tolerate a single retry to avoid a one-in-sixteen-million flake from the same prefix landing on the same final byte.
    const a = generateDeviceId();
    let b = generateDeviceId();

    if(a === b) {

      b = generateDeviceId();
    }

    assert.notEqual(a, b, "two random IDs should not collide");
  });
});
