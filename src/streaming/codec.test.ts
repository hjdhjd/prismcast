/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * codec.test.ts: Unit tests for the capture codec selection SSOT in codec.ts. The module's three exports - getEffectiveCaptureCodec, getCaptureMimeType,
 * isCaptureHardwareAccelerated - all derive from two module-level inputs: the user's allowlist on CONFIG.streaming.captureCodecs and the GPU capabilities cached
 * in browser/display.ts. Tests save and restore both pieces of global state around each case so they're independent of one another and of any other test files
 * that touch CONFIG.
 */
import type { CaptureCodec, Nullable } from "../types/index.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { getCaptureMimeType, getEffectiveCaptureCodec, isCaptureHardwareAccelerated } from "./codec.ts";
import { getGpuCapabilities, setGpuCapabilities } from "../browser/display.ts";
import { CONFIG } from "../config/index.ts";
import type { GpuCapabilities } from "../browser/display.ts";
import assert from "node:assert/strict";

/* makeGpuCapabilities builds a GpuCapabilities literal with sensible defaults (no hardware encoding, software renderer). Tests override only the fields they
 * care about - a hardware-HEVC test passes { hevcHardwareEncoding: true } and the rest stay false.
 */
function makeGpuCapabilities(overrides: Partial<GpuCapabilities> = {}): GpuCapabilities {

  return {

    av1HardwareEncoding: false,
    h264HardwareEncoding: false,
    hevcHardwareEncoding: false,
    renderer: "test-renderer",
    ...overrides
  };
}

describe("getEffectiveCaptureCodec", () => {

  let originalCodecs: string[];
  let originalGpu: Nullable<GpuCapabilities>;

  beforeEach(() => {

    originalCodecs = CONFIG.streaming.captureCodecs;
    originalGpu = getGpuCapabilities();
  });

  afterEach(() => {

    CONFIG.streaming.captureCodecs = originalCodecs;

    if(originalGpu) {

      setGpuCapabilities(originalGpu);
    }
  });

  function setAllowlist(codecs: CaptureCodec[]): void {

    CONFIG.streaming.captureCodecs = codecs;
  }

  test("returns h264 when no GPU capabilities have been detected (initial state)", () => {

    // Default fall-through: without GPU detection results, only the H.264 baseline applies. The implementation guards via gpuCaps?.[...] short-circuit.
    setGpuCapabilities(makeGpuCapabilities());
    setAllowlist([ "h264", "hevc" ]);

    assert.equal(getEffectiveCaptureCodec(), "h264", "no hardware -> h264 baseline");
  });

  test("returns hevc when both the allowlist and GPU permit it", () => {

    setGpuCapabilities(makeGpuCapabilities({ hevcHardwareEncoding: true }));
    setAllowlist([ "h264", "hevc" ]);

    assert.equal(getEffectiveCaptureCodec(), "hevc", "allowlist + GPU -> hevc wins over h264");
  });

  test("returns h264 when GPU supports hevc but the allowlist excludes it", () => {

    // Negative test: GPU capability alone does not promote a codec - the user's allowlist is the gating layer. This is the contract that lets users force h264
    // for compatibility with downstream consumers that don't speak HEVC.
    setGpuCapabilities(makeGpuCapabilities({ hevcHardwareEncoding: true }));
    setAllowlist(["h264"]);

    assert.equal(getEffectiveCaptureCodec(), "h264", "allowlist excludes hevc -> h264 even though GPU could do hevc");
  });

  test("returns h264 when allowlist permits hevc but GPU does not support it", () => {

    // Inverse of the above: allowlist alone is not sufficient. Without GPU support the codec falls back to the baseline.
    setGpuCapabilities(makeGpuCapabilities({ hevcHardwareEncoding: false }));
    setAllowlist([ "h264", "hevc" ]);

    assert.equal(getEffectiveCaptureCodec(), "h264", "GPU lacks hevc -> h264 even though allowlist permits hevc");
  });

  test("returns h264 when the allowlist is empty (no codec was configured)", () => {

    // Boundary: an empty allowlist falls through every priority entry and lands on the h264 baseline. The function does not throw on empty input - h264 is the
    // safe default.
    setGpuCapabilities(makeGpuCapabilities({ hevcHardwareEncoding: true }));
    setAllowlist([]);

    assert.equal(getEffectiveCaptureCodec(), "h264", "empty allowlist -> h264 fallback");
  });
});

describe("getCaptureMimeType", () => {

  let originalCodecs: string[];
  let originalGpu: Nullable<GpuCapabilities>;

  beforeEach(() => {

    originalCodecs = CONFIG.streaming.captureCodecs;
    originalGpu = getGpuCapabilities();
  });

  afterEach(() => {

    CONFIG.streaming.captureCodecs = originalCodecs;

    if(originalGpu) {

      setGpuCapabilities(originalGpu);
    }
  });

  function setAllowlist(codecs: CaptureCodec[]): void {

    CONFIG.streaming.captureCodecs = codecs;
  }

  test("returns the h264 Matroska MIME type when h264 is the effective codec", () => {

    setGpuCapabilities(makeGpuCapabilities());
    setAllowlist(["h264"]);

    assert.equal(getCaptureMimeType(), "video/x-matroska;codecs=h264,opus", "h264 MIME literal");
  });

  test("returns the hevc Matroska MIME type when hevc is the effective codec", () => {

    setGpuCapabilities(makeGpuCapabilities({ hevcHardwareEncoding: true }));
    setAllowlist([ "h264", "hevc" ]);

    assert.equal(getCaptureMimeType(), "video/x-matroska;codecs=hvc1.1.6.L93.B0,opus", "hevc MIME literal");
  });

  test("MIME type tracks the effective codec when GPU capabilities change", () => {

    // Roundtrip: same allowlist, two GPU configurations -> two different MIME types. Locks the derived-from relationship between the two functions.
    setAllowlist([ "h264", "hevc" ]);

    setGpuCapabilities(makeGpuCapabilities());
    const mimeWithoutHevc = getCaptureMimeType();

    setGpuCapabilities(makeGpuCapabilities({ hevcHardwareEncoding: true }));
    const mimeWithHevc = getCaptureMimeType();

    assert.equal(mimeWithoutHevc, "video/x-matroska;codecs=h264,opus", "without GPU hevc -> h264 MIME");
    assert.equal(mimeWithHevc, "video/x-matroska;codecs=hvc1.1.6.L93.B0,opus", "with GPU hevc -> hevc MIME");
    assert.notEqual(mimeWithoutHevc, mimeWithHevc, "MIME differs across the GPU swap");
  });
});

describe("isCaptureHardwareAccelerated", () => {

  let originalCodecs: string[];
  let originalGpu: Nullable<GpuCapabilities>;

  beforeEach(() => {

    originalCodecs = CONFIG.streaming.captureCodecs;
    originalGpu = getGpuCapabilities();
  });

  afterEach(() => {

    CONFIG.streaming.captureCodecs = originalCodecs;

    if(originalGpu) {

      setGpuCapabilities(originalGpu);
    }
  });

  function setAllowlist(codecs: CaptureCodec[]): void {

    CONFIG.streaming.captureCodecs = codecs;
  }

  test("returns true when h264 is effective and the GPU advertises h264 hardware encoding", () => {

    setGpuCapabilities(makeGpuCapabilities({ h264HardwareEncoding: true }));
    setAllowlist(["h264"]);

    assert.equal(isCaptureHardwareAccelerated(), true, "h264 + hardware h264 -> accelerated");
  });

  test("returns false when h264 is effective and the GPU does not advertise h264 hardware encoding", () => {

    setGpuCapabilities(makeGpuCapabilities({ h264HardwareEncoding: false }));
    setAllowlist(["h264"]);

    assert.equal(isCaptureHardwareAccelerated(), false, "h264 + software-only GPU -> not accelerated");
  });

  test("returns true when hevc is effective (hevc is only selected when its hardware capability is true)", () => {

    // The hevc branch in getEffectiveCaptureCodec only fires when hevcHardwareEncoding is true, so reaching that branch guarantees hardware acceleration. The
    // function explicitly verifies the capability rather than implicitly trusting the selection logic.
    setGpuCapabilities(makeGpuCapabilities({ hevcHardwareEncoding: true }));
    setAllowlist([ "h264", "hevc" ]);

    assert.equal(isCaptureHardwareAccelerated(), true, "hevc selection implies hardware acceleration");
  });

  test("returns false when no GPU capabilities have been detected", () => {

    // Boundary: the entire result chain depends on getGpuCapabilities() returning non-null. With no detection done, the function reports not accelerated.
    setGpuCapabilities(makeGpuCapabilities());
    setAllowlist(["h264"]);

    assert.equal(isCaptureHardwareAccelerated(), false, "no GPU detection -> not accelerated");
  });
});
