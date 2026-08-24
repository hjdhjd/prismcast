/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * display.test.ts: Unit tests for the GPU capability cache in display.ts. The module exposes one setter and one getter over a single module-level slot holding the
 * capability bundle the launch-time probe detects. The cache is intentionally minimal (no validation, no normalization, last-write-wins) so the tests focus on the
 * round-trip contract rather than complex behavior. The describe block saves and restores the slot so state from one test cannot leak into another or into other
 * test files that rely on the cache.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { getGpuCapabilities, setGpuCapabilities } from "./display.ts";
import type { GpuCapabilities } from "./display.ts";
import type { Nullable } from "../types/index.ts";
import assert from "node:assert/strict";

/* makeGpuCapabilities builds a GpuCapabilities literal with sensible defaults (no hardware encoding, recognizable renderer). Tests override only the fields they
 * care about. The factory matches the pattern used by codec.test.ts so the GPU shape stays consistent across the suite.
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

describe("setGpuCapabilities / getGpuCapabilities", () => {

  let original: Nullable<GpuCapabilities>;

  beforeEach(() => {

    original = getGpuCapabilities();
  });

  afterEach(() => {

    if(original) {

      setGpuCapabilities(original);
    }
  });

  test("round-trips a GpuCapabilities object through the setter and getter", () => {

    const capabilities = makeGpuCapabilities({ h264HardwareEncoding: true, renderer: "Apple M1" });

    setGpuCapabilities(capabilities);

    assert.deepEqual(getGpuCapabilities(), capabilities, "stored capabilities surface unchanged");
  });

  test("returns the same reference the setter received (no defensive copy)", () => {

    // The cache stores the object by reference. Mutating the stored object after the setter call is observable through the getter. We lock this so callers know
    // they must clone before storing if they intend to use the value as an immutable snapshot.
    const capabilities = makeGpuCapabilities();

    setGpuCapabilities(capabilities);

    assert.equal(getGpuCapabilities(), capabilities, "returned reference is the same object the setter was given");
  });

  test("the second setter call replaces the earlier capabilities object entirely", () => {

    setGpuCapabilities(makeGpuCapabilities({ h264HardwareEncoding: true }));
    setGpuCapabilities(makeGpuCapabilities({ hevcHardwareEncoding: true, renderer: "second-call" }));

    const result = getGpuCapabilities();

    assert.ok(result, "GPU slot is populated after the second write");
    assert.equal(result.h264HardwareEncoding, false, "stale h264 flag from the earlier write is gone");
    assert.equal(result.hevcHardwareEncoding, true, "later hevc flag is in place");
    assert.equal(result.renderer, "second-call", "later renderer string surfaces");
  });

  test("accepts a fully-populated hardware-acceleration capabilities object", () => {

    // Boundary: the maximally-capable case. All hardware-encoding flags true. We lock that the cache stores every field independently rather than collapsing to a
    // single boolean.
    const capabilities = makeGpuCapabilities({ av1HardwareEncoding: true, h264HardwareEncoding: true, hevcHardwareEncoding: true, renderer: "unobtainium" });

    setGpuCapabilities(capabilities);

    assert.deepEqual(getGpuCapabilities(), capabilities, "all flags and renderer surface intact");
  });
});
