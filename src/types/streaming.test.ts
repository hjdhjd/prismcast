/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streaming.test.ts: Unit tests for the runtime exports of streaming.ts. RECOGNIZED_CODECS is a readonly tuple that drives the CaptureCodec union, the MIME type
 * lookup in codec.ts, and the captureCodecs allowlist validation in CONFIG; the tests assert its literal membership, ordering, and disjointness, plus the
 * type-level relationship between the array and the derived CaptureCodec union via @ts-expect-error. CAPTURE_BASELINE_CODEC is held to its value and to the
 * literal type the Exclude consumers read, and CAPTURE_SOURCE_UNAVAILABLE_MESSAGE to the wording Chrome reports, the one place a test fixes that wording.
 */
import { CAPTURE_BASELINE_CODEC, CAPTURE_SOURCE_UNAVAILABLE_MESSAGE, RECOGNIZED_CODECS } from "./streaming.ts";
import { describe, test } from "node:test";
import type { CaptureCodec } from "./streaming.ts";
import assert from "node:assert/strict";

describe("RECOGNIZED_CODECS", () => {

  test("contains exactly h264 and hevc in priority order", () => {

    // The array is the single source of truth for capture codec identifiers. h264 appears first as the universal baseline; hevc requires GPU hardware encoding.
    // The order matters for any consumer that iterates - it should walk lowest-priority (universal baseline) to highest-priority (GPU-gated).
    assert.deepEqual([...RECOGNIZED_CODECS], [ "h264", "hevc" ]);
  });

  test("includes h264 as the universal baseline codec", () => {

    // h264 must always be present - it is the codec that does not require GPU support. Removing it would leave systems without GPU encoding with no usable codec.
    const codecs: string[] = [...RECOGNIZED_CODECS];

    assert.ok(codecs.includes("h264"), "h264 must be present as the universal baseline");
  });

  test("has no duplicate entries", () => {

    // A duplicate codec identifier would not change the derived union but would muddle iteration order. We assert distinctness explicitly.
    const set = new Set(RECOGNIZED_CODECS);

    assert.equal(set.size, RECOGNIZED_CODECS.length, "every codec identifier should be unique");
  });

  test("declares the expected number of codecs", () => {

    // CONFIG validation (config/index.ts) iterates this array assuming at least one entry; codec.ts consumes only the derived CaptureCodec type, not the
    // array itself. We assert the exact count so adding or removing a codec surfaces here as well as in the deepEqual membership test above.
    assert.equal(RECOGNIZED_CODECS.length, 2);
  });
});

describe("CaptureCodec (type-level)", () => {

  test("accepts every literal member of RECOGNIZED_CODECS", () => {

    // CaptureCodec is `typeof RECOGNIZED_CODECS[number]`, so the literal types of each array entry must be assignable to it. We assert both members.
    const baseline: CaptureCodec = "h264";
    const accelerated: CaptureCodec = "hevc";

    assert.equal(baseline, "h264");
    assert.equal(accelerated, "hevc");
  });

  test("rejects literals not in RECOGNIZED_CODECS", () => {

    // av1 is a plausible future codec but is not currently in the array; it must not be assignable to CaptureCodec. Removing the @ts-expect-error directive
    // should produce a real error - if it does not, the union has drifted from the array.
    // @ts-expect-error - av1 is not a recognized codec.
    const bad: CaptureCodec = "av1";

    assert.equal(bad, "av1", "the runtime string still exists");
  });

  test("rejects arbitrary strings (the union is closed)", () => {

    // The whole point of a string-literal union is that it is closed. An arbitrary string must not be assignable.
    // @ts-expect-error - arbitrary strings are not assignable to the closed CaptureCodec union.
    const bad: CaptureCodec = "not-a-codec";

    assert.equal(bad, "not-a-codec", "the runtime string still exists");
  });

  test("array entries widen to CaptureCodec when iterated", () => {

    // A `for...of` over RECOGNIZED_CODECS yields values that are still assignable to CaptureCodec. This locks the relationship that consumers rely on when
    // building lookup tables keyed by codec. We collect the iterated values and compare against the expected literal set rather than asserting per-iteration,
    // which avoids the always-true narrowing the type system already proves.
    const collected: CaptureCodec[] = [];

    for(const codec of RECOGNIZED_CODECS) {

      collected.push(codec);
    }

    assert.deepEqual(collected, [ "h264", "hevc" ], "iterated codecs match the array contents");
  });
});

describe("CAPTURE_BASELINE_CODEC", () => {

  test("is h264, a recognized codec", () => {

    assert.equal(CAPTURE_BASELINE_CODEC, "h264", "the baseline is the universally encodable codec");
    assert.ok(RECOGNIZED_CODECS.includes(CAPTURE_BASELINE_CODEC), "and it is one of the recognized codecs");
  });

  test("keeps its literal type, so a type can exclude the baseline by name", () => {

    /* The codec module and the settings form key their GPU-gated tables by Exclude<CaptureCodec, typeof CAPTURE_BASELINE_CODEC>. That only names the accelerated
     * codecs while the constant keeps its literal type: widened to CaptureCodec, the Exclude collapses to never and the accelerated assignment below stops compiling,
     * which is the line that catches the widening. The directive on the baseline assignment cannot catch it, because "h264" is no more assignable to never than to
     * the accelerated codecs...that directive asserts the baseline is excluded by name, and it stays in use whether the constant is literal or widened.
     */
    const accelerated: Exclude<CaptureCodec, typeof CAPTURE_BASELINE_CODEC> = "hevc";

    // @ts-expect-error - the baseline is excluded by name.
    const baseline: Exclude<CaptureCodec, typeof CAPTURE_BASELINE_CODEC> = "h264";

    assert.equal(accelerated, "hevc");
    assert.equal(baseline, "h264", "the runtime string still exists");
  });
});

describe("CAPTURE_SOURCE_UNAVAILABLE_MESSAGE", () => {

  test("is the wording Chrome reports when a capture source cannot start", () => {

    // The capture path and the classifier match this text in errors Chrome raises, so a change here has to follow a change in Chrome's own wording.
    assert.equal(CAPTURE_SOURCE_UNAVAILABLE_MESSAGE, "Could not start video source");
  });
});
