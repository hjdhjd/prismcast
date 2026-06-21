/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * presets.test.ts: Unit tests for the quality preset module. The presets are the SSOT for capture viewport dimensions, bitrates, and frame rates; the
 * effective-preset resolver gates whether the configured preset can run on the current display. We exercise every selector, the degradation logic at the
 * resolution boundary (configured fits / does not fit / no preset fits), and the formatting helpers.
 */
import { VIDEO_QUALITY_PRESETS, findBestFittingPreset, formatPresetStatus, getEffectivePreset, getEffectiveViewport, getPresetOptionsWithDegradation,
  getPresetViewport, getValidPresetIds } from "./presets.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { getMaxSupportedViewport, setMaxSupportedViewport } from "../browser/display.ts";
import type { Config } from "../types/index.ts";
import assert from "node:assert/strict";

// makeConfig builds the minimal Config shape needed for preset resolution. Only streaming.qualityPreset is read by the functions under test.
function makeConfig(qualityPreset: string): Config {

  return { streaming: { qualityPreset } } as Config;
}

describe("VIDEO_QUALITY_PRESETS", () => {

  test("contains six presets in low-to-high resolution order", () => {

    assert.equal(VIDEO_QUALITY_PRESETS.length, 6, "six preset entries");
    assert.deepEqual(VIDEO_QUALITY_PRESETS.map((p) => p.id), [ "480p", "720p", "720p-high", "1080p", "1080p-high", "4k" ]);
  });

  test("each preset declares all four required setting paths", () => {

    for(const preset of VIDEO_QUALITY_PRESETS) {

      assert.ok(typeof preset.values["browser.viewport.width"] === "number", preset.id + " has width");
      assert.ok(typeof preset.values["browser.viewport.height"] === "number", preset.id + " has height");
      assert.ok(typeof preset.values["streaming.frameRate"] === "number", preset.id + " has frameRate");
      assert.ok(typeof preset.values["streaming.videoBitsPerSecond"] === "number", preset.id + " has videoBitsPerSecond");
    }
  });
});

describe("getValidPresetIds", () => {

  test("returns the IDs of every declared preset", () => {

    assert.deepEqual(getValidPresetIds(), [ "480p", "720p", "720p-high", "1080p", "1080p-high", "4k" ]);
  });
});

describe("getPresetViewport", () => {

  test("returns the configured preset's viewport dimensions", () => {

    assert.deepEqual(getPresetViewport(makeConfig("1080p")), { height: 1080, width: 1920 });
  });

  test("returns the 480p baseline for the lowest preset", () => {

    assert.deepEqual(getPresetViewport(makeConfig("480p")), { height: 480, width: 854 });
  });

  test("returns the 4K dimensions for the highest preset", () => {

    assert.deepEqual(getPresetViewport(makeConfig("4k")), { height: 2160, width: 3840 });
  });

  test("falls back to the 720p default viewport when the configured preset is unknown", () => {

    // Boundary: a typo'd preset id returns undefined from the lookup. The function falls back to the documented default { height: 720, width: 1280 }.
    assert.deepEqual(getPresetViewport(makeConfig("not-a-real-preset")), { height: 720, width: 1280 });
  });
});

describe("findBestFittingPreset", () => {

  test("returns the highest-resolution preset that fits within the display", () => {

    // 1920x1080 fits 1080p-high but not 4k.
    const result = findBestFittingPreset(1920, 1080);

    assert.equal(result?.id, "1080p-high", "1080p-high is the largest preset that fits 1920x1080");
  });

  test("returns 4k when the display is larger than 4k", () => {

    const result = findBestFittingPreset(7680, 4320);

    assert.equal(result?.id, "4k");
  });

  test("returns 480p when only the smallest preset fits", () => {

    const result = findBestFittingPreset(854, 480);

    assert.equal(result?.id, "480p");
  });

  test("returns null when no preset fits the display (extremely small)", () => {

    // Boundary: 800x400 is below every preset's required dimensions, so the iteration falls through and the function returns null.
    assert.equal(findBestFittingPreset(800, 400), null);
  });

  test("a width that fits but a height that does not still rules out the preset", () => {

    // 480p needs 854x480. A 854x100 display has the right width but insufficient height; the function must return null.
    assert.equal(findBestFittingPreset(854, 100), null);
  });
});

describe("getEffectivePreset", () => {

  let originalViewport: ReturnType<typeof getMaxSupportedViewport> | null = null;

  beforeEach(() => {

    originalViewport = getMaxSupportedViewport();
  });

  afterEach(() => {

    // Restore the prior value to keep cross-suite state stable. The setter is the only public mutator; if there was no prior value we set a permissive
    // baseline so subsequent suites see a consistent starting state.
    if(originalViewport) {

      setMaxSupportedViewport(originalViewport.width, originalViewport.height);
    } else {

      setMaxSupportedViewport(7680, 4320);
    }
  });

  test("returns the configured preset under a permissive (4K-or-larger) display with no degradation", () => {

    setMaxSupportedViewport(7680, 4320);

    const result = getEffectivePreset(makeConfig("720p-high"));

    assert.equal(result.degraded, false);
    assert.equal(result.configuredPreset.id, "720p-high");
    assert.equal(result.effectivePreset.id, "720p-high");
  });

  test("returns the configured preset (no degradation) when it fits within the display", () => {

    setMaxSupportedViewport(1920, 1080);

    const result = getEffectivePreset(makeConfig("720p"));

    assert.equal(result.degraded, false);
    assert.equal(result.configuredPreset.id, "720p");
    assert.equal(result.effectivePreset.id, "720p");
  });

  test("degrades to the largest fitting preset when configured preset exceeds the display", () => {

    setMaxSupportedViewport(1920, 1080);

    const result = getEffectivePreset(makeConfig("4k"));

    assert.equal(result.degraded, true, "4k on a 1080p display must degrade");
    assert.equal(result.configuredPreset.id, "4k");
    assert.equal(result.effectivePreset.id, "1080p-high", "1080p-high is the highest fitting preset");
  });

  test("degrades to 480p (minimum) when no preset fits the display (extremely small)", () => {

    // Boundary: a sub-480p display fails findBestFittingPreset; the function defaults to MINIMUM_PRESET (480p) so capture still attempts something.
    setMaxSupportedViewport(640, 360);

    const result = getEffectivePreset(makeConfig("4k"));

    assert.equal(result.degraded, true);
    assert.equal(result.effectivePreset.id, "480p", "minimum preset is 480p when nothing else fits");
  });

  test("falls back to 720p as the default-configured preset when the configured ID is unknown", () => {

    // Boundary: a typo on the configured preset name. The function uses ?? DEFAULT_PRESET (720p) so the resolution still produces valid output.
    setMaxSupportedViewport(7680, 4320);

    const result = getEffectivePreset(makeConfig("not-a-real-preset"));

    assert.equal(result.configuredPreset.id, "720p", "unknown preset name falls back to 720p default");
    assert.equal(result.degraded, false);
  });
});

describe("getEffectiveViewport", () => {

  test("returns the configured preset's viewport when no degradation occurs", () => {

    setMaxSupportedViewport(1920, 1080);
    assert.deepEqual(getEffectiveViewport(makeConfig("1080p")), { height: 1080, width: 1920 });
  });

  test("returns the degraded preset's viewport when degradation occurs", () => {

    setMaxSupportedViewport(1280, 720);
    assert.deepEqual(getEffectiveViewport(makeConfig("4k")), { height: 720, width: 1280 });
  });
});

describe("formatPresetStatus", () => {

  test("formats non-degraded result as id (WxH)", () => {

    const result = {

      configuredPreset: VIDEO_QUALITY_PRESETS[3]!,
      degraded: false,
      effectivePreset: VIDEO_QUALITY_PRESETS[3]!,
      maxViewport: null
    };

    assert.equal(formatPresetStatus(result), "1080p (1920×1080)");
  });

  test("formats degraded result with the limited-by-display marker", () => {

    const configured = VIDEO_QUALITY_PRESETS[5]!;
    const effective = VIDEO_QUALITY_PRESETS[3]!;
    const result = {

      configuredPreset: configured,
      degraded: true,
      effectivePreset: effective,
      maxViewport: { height: 1080, width: 1920 }
    };

    assert.equal(formatPresetStatus(result), "4k (limited to 1080p by display)");
  });
});

describe("getPresetOptionsWithDegradation", () => {

  test("returns every preset as non-degraded when the display can fit the largest preset", () => {

    setMaxSupportedViewport(7680, 4320);

    const result = getPresetOptionsWithDegradation();

    assert.equal(result.options.length, VIDEO_QUALITY_PRESETS.length);

    for(const option of result.options) {

      assert.equal(option.degradedTo, null, option.preset.id + " on a 4K-or-larger display does not degrade");
    }
  });

  test("marks presets that exceed the display as degraded to the largest fitting preset", () => {

    setMaxSupportedViewport(1920, 1080);

    const result = getPresetOptionsWithDegradation();
    const fourK = result.options.find((o) => o.preset.id === "4k");

    assert.ok(fourK, "4k preset present in options");
    assert.equal(fourK.degradedTo?.id, "1080p-high", "4k degrades to 1080p-high on a 1080p display");

    const onePoEightyHigh = result.options.find((o) => o.preset.id === "1080p-high");

    assert.equal(onePoEightyHigh?.degradedTo, null, "1080p-high fits 1080p exactly so does not degrade");
  });
});
