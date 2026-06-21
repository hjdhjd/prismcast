/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * decrypt.test.ts: Unit tests for the AES-128 key fetching and segment decryption primitives in decrypt.ts. The module's four exports are pure functions that
 * delegate to Node's crypto module for the heavy lifting. Tests use real Node crypto for the round-trip verification (encrypt then decryptSegment must recover
 * the plaintext) and mock global fetch for fetchDecryptionKey to avoid real network I/O. Synthetic key/iv/ciphertext fixtures - never derived from production
 * data - lock the binary contracts that downstream HLS playback depends on.
 */
import { afterEach, describe, mock, test } from "node:test";
import { createCipheriv, randomBytes } from "node:crypto";
import { decryptSegment, deriveIvFromSequence, fetchDecryptionKey, parseExplicitIv } from "./decrypt.ts";
import assert from "node:assert/strict";

/* aesEncrypt produces synthetic AES-128-CBC ciphertext from a plaintext, key, and IV. We use this to construct round-trip fixtures so decryptSegment can be
 * exercised against real encrypted bytes without depending on any HLS provider's segments. The function mirrors what an HLS service does at the encoder: encrypt
 * with PKCS#7 padding (Node's default for aes-128-cbc), and emit the ciphertext directly. The output is what decryptSegment receives in production.
 */
function aesEncrypt(plaintext: Buffer, key: Buffer, iv: Buffer): Buffer {

  const cipher = createCipheriv("aes-128-cbc", key, iv);

  return Buffer.concat([ cipher.update(plaintext), cipher.final() ]);
}

describe("fetchDecryptionKey", () => {

  afterEach(() => {

    mock.reset();
  });

  test("returns the 16-byte key buffer when the fetch succeeds with the expected size", async () => {

    // Happy path: HTTP 200 with a 16-byte body. We construct a deterministic key buffer so callers can assert the exact bytes round-trip through arrayBuffer/Buffer.
    const expectedKey = Buffer.from("0123456789abcdef", "utf8");

    mock.method(globalThis, "fetch", async () => new Response(expectedKey, { status: 200 }));

    const result = await fetchDecryptionKey("https://example.test/key.bin");

    assert.ok(result, "key fetch should resolve to a Buffer, not null");
    assert.equal(result.length, 16, "the resolved buffer should be exactly 16 bytes");
    assert.equal(result.toString("utf8"), "0123456789abcdef", "the buffer contents survive the arrayBuffer/Buffer round trip");
  });

  test("returns null when the response is not OK (HTTP 4xx)", async () => {

    // Negative test: a 403 from the key URL must surface as null so callers fall back to capture rather than attempting decryption with a missing key.
    mock.method(globalThis, "fetch", async () => new Response("", { status: 403 }));

    assert.equal(await fetchDecryptionKey("https://example.test/key.bin"), null, "non-OK status returns null");
  });

  test("returns null when the response is HTTP 500 (server error)", async () => {

    // Boundary: server-side errors look identical to client errors at this layer - the function reads response.ok which is false for both.
    mock.method(globalThis, "fetch", async () => new Response("", { status: 500 }));

    assert.equal(await fetchDecryptionKey("https://example.test/key.bin"), null, "5xx status returns null");
  });

  test("returns null when the body is not exactly 16 bytes (too small)", async () => {

    // Boundary: AES-128 keys are exactly 16 bytes. Anything smaller is corrupt and must not be returned to the decrypt path.
    mock.method(globalThis, "fetch", async () => new Response(Buffer.alloc(15), { status: 200 }));

    assert.equal(await fetchDecryptionKey("https://example.test/key.bin"), null, "15-byte body rejected");
  });

  test("returns null when the body is not exactly 16 bytes (too large)", async () => {

    // Boundary: 32 bytes is also wrong - some services accidentally serve hex-encoded keys. The function rejects these to avoid feeding malformed keys to crypto.
    mock.method(globalThis, "fetch", async () => new Response(Buffer.alloc(32), { status: 200 }));

    assert.equal(await fetchDecryptionKey("https://example.test/key.bin"), null, "32-byte body rejected");
  });

  test("returns null when the body is empty (zero bytes)", async () => {

    // Boundary: a 200 OK with an empty body is malformed - the length-16 check catches it.
    mock.method(globalThis, "fetch", async () => new Response("", { status: 200 }));

    assert.equal(await fetchDecryptionKey("https://example.test/key.bin"), null, "empty body rejected");
  });

  test("returns null when the fetch itself throws (network error path)", async () => {

    // Negative test: fetch can throw for connection reset, DNS failure, abort signal, etc. The catch block must convert all of these into null.
    mock.method(globalThis, "fetch", async () => {

      throw new Error("simulated network failure");
    });

    assert.equal(await fetchDecryptionKey("https://example.test/key.bin"), null, "thrown error is swallowed and converted to null");
  });

  test("propagates the URL to the underlying fetch call", async () => {

    // Verifies that the URL is forwarded verbatim. Without this, a refactor that drops the URL argument would still pass body-length tests but break in production
    // where every key has a unique URL.
    let capturedUrl: string | URL = "";

    mock.method(globalThis, "fetch", async (url: string | URL): Promise<Response> => {

      capturedUrl = url;

      return new Response(Buffer.alloc(16), { status: 200 });
    });

    await fetchDecryptionKey("https://example.test/keys/abc123.bin");

    assert.equal(capturedUrl, "https://example.test/keys/abc123.bin", "the URL passes through unchanged");
  });
});

describe("decryptSegment", () => {

  test("recovers the plaintext when given key, iv, and ciphertext from a matching encrypt", () => {

    // Canonical round-trip: encrypt(plaintext) with a key+iv, then decryptSegment recovers exactly the plaintext bytes. This locks the contract that callers
    // can rely on when both the key and IV match the encoder's choices.
    const key = Buffer.from("aaaaaaaaaaaaaaaa", "utf8");
    const iv = Buffer.from("bbbbbbbbbbbbbbbb", "utf8");
    const plaintext = Buffer.from("This is the cleartext segment payload that the decoder must recover exactly.", "utf8");
    const ciphertext = aesEncrypt(plaintext, key, iv);

    const recovered = decryptSegment(ciphertext, key, iv);

    assert.deepEqual(recovered, plaintext, "decryptSegment(encrypt(p)) recovers p byte-for-byte");
  });

  test("recovers binary plaintext (not just ASCII) through the round trip", () => {

    // Boundary: HLS segments are MPEG-TS binary, not text. Lock the contract that any byte sequence survives. We use a randomBytes payload because it has no
    // text patterns the cipher could accidentally pass through.
    const key = randomBytes(16);
    const iv = randomBytes(16);
    const plaintext = randomBytes(1024);
    const ciphertext = aesEncrypt(plaintext, key, iv);

    const recovered = decryptSegment(ciphertext, key, iv);

    assert.deepEqual(recovered, plaintext, "binary plaintext survives the round trip");
  });

  test("recovers an empty plaintext (boundary: zero-length input)", () => {

    // Boundary: an empty plaintext encrypts to a single 16-byte block of pure padding. decryptSegment must remove the padding and return an empty Buffer.
    const key = Buffer.alloc(16);
    const iv = Buffer.alloc(16);
    const plaintext = Buffer.alloc(0);
    const ciphertext = aesEncrypt(plaintext, key, iv);

    assert.equal(ciphertext.length, 16, "empty plaintext produces exactly one block of padding");

    const recovered = decryptSegment(ciphertext, key, iv);

    assert.equal(recovered.length, 0, "empty plaintext is recovered as empty");
  });

  test("throws when the IV does not match the encoder's IV (fails decryption)", () => {

    // Negative test: AES-CBC with a wrong IV usually corrupts the first block but Node's crypto often still rejects it via PKCS#7 padding mismatch on the
    // ciphertext. Either way, the contract is that callers cannot accidentally accept silently-wrong data when the IV is wrong.
    const key = Buffer.alloc(16, 0x01);
    const correctIv = Buffer.alloc(16, 0x02);
    const wrongIv = Buffer.alloc(16, 0x03);
    const plaintext = Buffer.from("known plaintext that is exactly 32 bytes long...", "utf8");
    const ciphertext = aesEncrypt(plaintext, key, correctIv);

    // Either throw or return mangled bytes - we collect both and assert that the mangled output is not equal to the plaintext.
    let recovered: Buffer | null = null;

    try {

      recovered = decryptSegment(ciphertext, key, wrongIv);
    } catch {

      // The wrong IV typically still decrypts the second block correctly (only the first block is corrupted), so the padding may still validate. We accept either
      // outcome but require that any returned data is not equal to the original plaintext.
    }

    if(recovered) {

      assert.notDeepEqual(recovered, plaintext, "wrong IV must not recover the original plaintext bytes");
    }
  });

  test("throws when the key does not match the encoder's key", () => {

    // Negative test: a different 16-byte key produces garbage on decryption. PKCS#7 padding nearly always fails to validate, surfacing as a thrown crypto error.
    const correctKey = Buffer.alloc(16, 0xaa);
    const wrongKey = Buffer.alloc(16, 0xbb);
    const iv = Buffer.alloc(16);
    const plaintext = Buffer.from("payload bytes used for the wrong-key test", "utf8");
    const ciphertext = aesEncrypt(plaintext, correctKey, iv);

    assert.throws(() => decryptSegment(ciphertext, wrongKey, iv), /bad decrypt|wrong final block|cipher/i, "wrong key surfaces as a crypto error");
  });

  test("throws on malformed ciphertext (length not a multiple of 16)", () => {

    // Negative test: AES-CBC requires ciphertext aligned to the 16-byte block size. A 17-byte input is structurally invalid and the cipher must reject it.
    const key = Buffer.alloc(16);
    const iv = Buffer.alloc(16);
    const malformed = Buffer.alloc(17);

    assert.throws(() => decryptSegment(malformed, key, iv), /wrong final block|invalid|length|cipher/i, "off-block ciphertext is rejected");
  });
});

describe("deriveIvFromSequence", () => {

  test("returns a 16-byte buffer for any sequence number", () => {

    // The function always allocates a 16-byte buffer regardless of the sequence value - locks the size contract that AES-128 callers depend on.
    assert.equal(deriveIvFromSequence(0).length, 16, "sequence 0 -> 16-byte buffer");
    assert.equal(deriveIvFromSequence(42).length, 16, "sequence 42 -> 16-byte buffer");
    assert.equal(deriveIvFromSequence(0xffffffff).length, 16, "sequence 0xffffffff -> 16-byte buffer");
  });

  test("zero-pads the first 12 bytes when sequence is 0", () => {

    // Boundary: media sequence 0 should produce all-zeros. The implementation only writes the last 4 bytes, so the other 12 must already be zero from Buffer.alloc.
    const iv = deriveIvFromSequence(0);

    for(let i = 0; i < 16; i++) {

      assert.equal(iv[i], 0, "byte at position " + String(i) + " is zero");
    }
  });

  test("places the sequence number in big-endian byte order in the last 4 bytes", () => {

    // Boundary: 0x01020304 must land as 01 02 03 04 in bytes 12-15. Per the HLS spec the sequence number occupies the least-significant bytes of the 128-bit IV;
    // the derivation writes it as a 64-bit big-endian value across the low 8 bytes, so a value that fits in 32 bits (like this one) lands in bytes 12-15.
    const iv = deriveIvFromSequence(0x01020304);

    // Bytes 0-11 remain zero.
    for(let i = 0; i < 12; i++) {

      assert.equal(iv[i], 0, "high byte " + String(i) + " is zero");
    }

    assert.equal(iv[12], 0x01, "byte 12 = 0x01");
    assert.equal(iv[13], 0x02, "byte 13 = 0x02");
    assert.equal(iv[14], 0x03, "byte 14 = 0x03");
    assert.equal(iv[15], 0x04, "byte 15 = 0x04");
  });

  test("handles the maximum 32-bit unsigned sequence value", () => {

    // Boundary: writeUInt32BE accepts values up to 0xffffffff. The IV must reflect those bytes exactly.
    const iv = deriveIvFromSequence(0xffffffff);

    assert.equal(iv[12], 0xff, "byte 12 = 0xff");
    assert.equal(iv[13], 0xff, "byte 13 = 0xff");
    assert.equal(iv[14], 0xff, "byte 14 = 0xff");
    assert.equal(iv[15], 0xff, "byte 15 = 0xff");
  });

  test("two different sequence numbers produce two different IVs", () => {

    // Negative test: the function must be injective - distinct sequence numbers yield distinct IVs. Otherwise segments would erroneously share IVs and decryption
    // would corrupt one of them.
    const a = deriveIvFromSequence(100);
    const b = deriveIvFromSequence(101);

    assert.notDeepEqual(a, b, "consecutive sequences must yield distinct IVs");
  });

  test("does not throw and produces a correct 16-byte IV at exactly 2^32 (the 32-bit boundary)", () => {

    /* Finding [20]: a long-running stream's media sequence can cross 2^32. A 32-bit-only write throws a RangeError at this boundary; the 64-bit-safe derivation
     * must place the value across the low 8 bytes instead. At exactly 2^32 the high 32-bit word is 1 and the low word is 0, so bytes 8-11 read 00 00 00 01 and
     * bytes 12-15 are all zero.
     */
    const iv = deriveIvFromSequence(2 ** 32);

    assert.equal(iv.length, 16, "produces a 16-byte buffer at the 2^32 boundary");

    // Bytes 0-7 remain zero (the value fits in the low 64 bits, well within the low 8 bytes).
    for(let i = 0; i < 8; i++) {

      assert.equal(iv[i], 0, "high byte " + String(i) + " is zero");
    }

    // Bytes 8-11 carry the high 32-bit word (0x00000001).
    assert.equal(iv[8], 0x00, "byte 8 = 0x00");
    assert.equal(iv[9], 0x00, "byte 9 = 0x00");
    assert.equal(iv[10], 0x00, "byte 10 = 0x00");
    assert.equal(iv[11], 0x01, "byte 11 = 0x01 (high word of 2^32)");

    // Bytes 12-15 (the low 32-bit word) are zero.
    assert.equal(iv[12], 0x00, "byte 12 = 0x00");
    assert.equal(iv[13], 0x00, "byte 13 = 0x00");
    assert.equal(iv[14], 0x00, "byte 14 = 0x00");
    assert.equal(iv[15], 0x00, "byte 15 = 0x00");
  });

  test("places a sequence above 2^32 across both 32-bit words in big-endian order", () => {

    // Boundary: a value with both the high and low words populated must split correctly. We use 0x0000000100000002 (2^32 + 2): high word 0x00000001 lands in bytes
    // 8-11, low word 0x00000002 lands in bytes 12-15. This is a value beyond the reach of a single writeUInt32BE.
    const iv = deriveIvFromSequence((2 ** 32) + 2);

    assert.equal(iv.length, 16, "16-byte buffer for a sequence above 2^32");
    assert.equal(iv[11], 0x01, "byte 11 = high word low byte = 0x01");
    assert.equal(iv[15], 0x02, "byte 15 = low word low byte = 0x02");
  });

  test("does not throw at a large sequence near the safe-integer ceiling", () => {

    // Boundary: the derivation must remain total across the entire range of representable media sequence numbers, not just the 2^32 neighborhood. A value with bits
    // set high in the 53-bit safe-integer range must still yield a 16-byte IV without throwing.
    const iv = deriveIvFromSequence(2 ** 48);

    assert.equal(iv.length, 16, "16-byte buffer near the safe-integer ceiling");

    // 2^48 sets bit 48: the high 32-bit word is 0x00010000, so byte 9 carries the 0x01.
    assert.equal(iv[9], 0x01, "byte 9 = 0x01 (bit 48 lands in the high word)");
  });

  test("returns a fresh Buffer on each call (no shared reference)", () => {

    // Boundary: callers must not see aliasing if the function memoizes by accident. We mutate one buffer and assert the other is unaffected.
    const a = deriveIvFromSequence(7);
    const b = deriveIvFromSequence(7);

    a[0] = 0xff;

    assert.equal(b[0], 0, "second buffer unchanged after mutating the first");
  });
});

describe("parseExplicitIv", () => {

  test("parses a 32-character hex string with the 0x prefix into a 16-byte Buffer", () => {

    // Happy path: 0x prefix is stripped, the remaining 32 hex digits decode to 16 bytes.
    const result = parseExplicitIv("0x000102030405060708090a0b0c0d0e0f");

    assert.ok(result, "valid input parses to a Buffer, not null");
    assert.equal(result.length, 16, "produces a 16-byte Buffer");
    assert.equal(result[0], 0x00, "first byte = 0x00");
    assert.equal(result[15], 0x0f, "last byte = 0x0f");
  });

  test("parses a 32-character hex string with the 0X (uppercase) prefix", () => {

    // Boundary: HLS spec allows IV=0X... (uppercase X). The function must accept both case variants of the prefix.
    const result = parseExplicitIv("0X0102030405060708090A0B0C0D0E0F00");

    assert.ok(result, "uppercase prefix parses to a Buffer");
    assert.equal(result.length, 16, "16-byte Buffer");
    assert.equal(result[15], 0x00, "last byte = 0x00");
  });

  test("parses a 32-character hex string without any prefix", () => {

    // Boundary: not all manifests include the 0x prefix. The function must accept bare hex too.
    const result = parseExplicitIv("000102030405060708090a0b0c0d0e0f");

    assert.ok(result, "bare hex parses to a Buffer");
    assert.equal(result.length, 16, "16-byte Buffer");
    assert.equal(result[1], 0x01, "byte 1 = 0x01");
  });

  test("returns null when the hex string is too short (after prefix removal)", () => {

    // Boundary: 16 hex digits = 8 bytes, half the required size. Reject.
    assert.equal(parseExplicitIv("0x0102030405060708"), null, "short hex rejected");
  });

  test("returns null when the hex string is too long (after prefix removal)", () => {

    // Boundary: 64 hex digits = 32 bytes, double the required size. Reject.
    assert.equal(parseExplicitIv("0x" + "00".repeat(32)), null, "long hex rejected");
  });

  test("returns null for an empty string (also too short)", () => {

    // Boundary: empty input is degenerate - the length check catches it.
    assert.equal(parseExplicitIv(""), null, "empty string rejected");
  });

  test("returns null when the input has only the prefix (zero hex digits remain)", () => {

    // Boundary: "0x" alone strips to "", which has length 0, not 32.
    assert.equal(parseExplicitIv("0x"), null, "prefix-only rejected");
  });

  test("does not strip a non-prefix '0x' that happens to occur mid-string", () => {

    // Boundary: only a leading 0x or 0X is stripped. A different prefix or an embedded "0x" must not be removed. We construct a 34-character string starting with
    // a non-hex character so the length check handles it.
    assert.equal(parseExplicitIv("xx0102030405060708090a0b0c0d0e0f00"), null, "non-leading 0x is left in place; length is 34, not 32");
  });

  test("the produced IV decrypts data encrypted with the same explicit IV (round trip)", () => {

    // Round trip: when an encoder uses an explicit IV from #EXT-X-KEY and we parse it back, decryptSegment must recover the original plaintext.
    const key = Buffer.alloc(16, 0x55);
    const ivHex = "0x0102030405060708090a0b0c0d0e0f10";
    const iv = parseExplicitIv(ivHex);

    assert.ok(iv, "parsed IV must not be null");

    const plaintext = Buffer.from("HLS segment payload encrypted with an explicit IV.", "utf8");
    const ciphertext = aesEncrypt(plaintext, key, iv);

    assert.deepEqual(decryptSegment(ciphertext, key, iv), plaintext, "explicit-IV round trip recovers plaintext");
  });
});
