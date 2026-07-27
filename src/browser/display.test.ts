/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * display.test.ts: Unit tests for the display dimension and GPU capability cache in display.ts. The module exposes setters and getters over independent
 * module-level slots for the maximum supported viewport, the browser chrome dimensions, and the GPU capability bundle. The cache is intentionally minimal (no
 * validation, no normalization, last-write-wins) so the tests focus on the round-trip contract and inter-slot independence rather than complex behavior.
 * Each describe block saves and restores the slot it touches so state from one test cannot leak into another or into other test files that rely on the cache.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { getBrowserChrome, getGpuCapabilities, getMaxSupportedViewport, setBrowserChrome, setGpuCapabilities,
  setMaxSupportedViewport } from "./display.ts";
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

describe("setMaxSupportedViewport / getMaxSupportedViewport", () => {

  let original: Nullable<{ height: number; width: number }>;

  beforeEach(() => {

    original = getMaxSupportedViewport();
  });

  afterEach(() => {

    // Restore by writing back the original snapshot. If the slot was null at start, the best we can do is leave the most recent setter call in place - the module
    // does not expose a clear() helper. None of the production code reads the slot during teardown, so leaving a value here does not affect other tests.
    if(original) {

      setMaxSupportedViewport(original.width, original.height);
    }
  });

  test("round-trips a width/height pair through the setter and getter", () => {

    setMaxSupportedViewport(1920, 1080);

    const result = getMaxSupportedViewport();

    assert.deepEqual(result, { height: 1080, width: 1920 }, "stored object surfaces in field-named form");
  });

  test("the second setter call wins (last-write semantics)", () => {

    setMaxSupportedViewport(1280, 720);
    setMaxSupportedViewport(3840, 2160);

    assert.deepEqual(getMaxSupportedViewport(), { height: 2160, width: 3840 }, "later write replaces the earlier value");
  });

  test("accepts a zero dimension without throwing or normalizing (no validation)", () => {

    // Boundary: the setter performs no validation. A zero dimension is allowed through to the cache verbatim. Locks the contract that callers (browser detection)
    // are responsible for sanitizing input before storing.
    setMaxSupportedViewport(0, 0);

    assert.deepEqual(getMaxSupportedViewport(), { height: 0, width: 0 }, "zero is stored as-is");
  });

  test("accepts negative dimensions verbatim (no validation)", () => {

    // Negative test: the setter does not guard against negative values - the cache is a thin slot, not a validator. We lock the contract so future readers know
    // to sanitize at the call site rather than expecting the cache to fail loudly.
    setMaxSupportedViewport(-100, -200);

    assert.deepEqual(getMaxSupportedViewport(), { height: -200, width: -100 }, "negatives flow through unchanged");
  });
});

describe("setBrowserChrome / getBrowserChrome", () => {

  let original: Nullable<{ height: number; width: number }>;

  beforeEach(() => {

    original = getBrowserChrome();
  });

  afterEach(() => {

    if(original) {

      setBrowserChrome(original.width, original.height);
    }
  });

  test("round-trips a width/height pair through the setter and getter", () => {

    setBrowserChrome(8, 84);

    assert.deepEqual(getBrowserChrome(), { height: 84, width: 8 }, "stored chrome dimensions surface in field-named form");
  });

  test("the second setter call wins (last-write semantics)", () => {

    setBrowserChrome(0, 70);
    setBrowserChrome(2, 90);

    assert.deepEqual(getBrowserChrome(), { height: 90, width: 2 }, "later write replaces the earlier value");
  });

  test("accepts zero chrome dimensions (the borderless case)", () => {

    // Boundary: a kiosk-mode or fullscreen browser may report zero chrome. The cache must accept this - it's a legitimate measurement, not invalid input.
    setBrowserChrome(0, 0);

    assert.deepEqual(getBrowserChrome(), { height: 0, width: 0 }, "zero chrome dimensions are stored as a valid measurement");
  });
});

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

describe("cache slot independence", () => {

  // These slots back independent module-level state. Mutating one must not affect the others. This guards against accidental refactors that pull the slots
  // into a shared object or share storage between the setters.

  let originalMax: Nullable<{ height: number; width: number }>;
  let originalChrome: Nullable<{ height: number; width: number }>;
  let originalGpu: Nullable<GpuCapabilities>;

  beforeEach(() => {

    originalMax = getMaxSupportedViewport();
    originalChrome = getBrowserChrome();
    originalGpu = getGpuCapabilities();
  });

  afterEach(() => {

    if(originalMax) {

      setMaxSupportedViewport(originalMax.width, originalMax.height);
    }

    if(originalChrome) {

      setBrowserChrome(originalChrome.width, originalChrome.height);
    }

    if(originalGpu) {

      setGpuCapabilities(originalGpu);
    }
  });

  test("setting the viewport does not perturb the chrome or GPU slots", () => {

    setBrowserChrome(8, 84);
    setGpuCapabilities(makeGpuCapabilities({ renderer: "preserved" }));

    setMaxSupportedViewport(1920, 1080);

    assert.deepEqual(getBrowserChrome(), { height: 84, width: 8 }, "chrome slot untouched");
    assert.equal(getGpuCapabilities()?.renderer, "preserved", "GPU slot untouched");
  });

  test("setting the chrome does not perturb the viewport or GPU slots", () => {

    setMaxSupportedViewport(1920, 1080);
    setGpuCapabilities(makeGpuCapabilities({ renderer: "preserved" }));

    setBrowserChrome(8, 84);

    assert.deepEqual(getMaxSupportedViewport(), { height: 1080, width: 1920 }, "viewport slot untouched");
    assert.equal(getGpuCapabilities()?.renderer, "preserved", "GPU slot untouched");
  });

  test("setting the GPU capabilities does not perturb the viewport or chrome slots", () => {

    setMaxSupportedViewport(1920, 1080);
    setBrowserChrome(8, 84);

    setGpuCapabilities(makeGpuCapabilities({ renderer: "fresh" }));

    assert.deepEqual(getMaxSupportedViewport(), { height: 1080, width: 1920 }, "viewport slot untouched");
    assert.deepEqual(getBrowserChrome(), { height: 84, width: 8 }, "chrome slot untouched");
  });
});
