/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * presets.test.ts: Unit tests for the quality preset module. The presets are the single source of truth for the capture surface's dimensions, bitrates, and frame
 * rates, and getPresetViewport is the one getter every consumer reads. Nothing outside configuration participates in the answer: the surface is emulated at the
 * configured preset on every page, so there is no display, window, or cache state for these tests to arrange. We exercise the table, the id list, and every
 * branch of the viewport getter including its fallback.
 */
import { VIDEO_QUALITY_PRESETS, getPresetViewport, getValidPresetIds } from "./presets.ts";
import { describe, test } from "node:test";
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

  test("falls back to the default preset's own dimensions when the configured preset is unknown", () => {

    /* Boundary: a typo'd preset id matches no entry. The fallback reads the default preset's own row in the table rather than a separately maintained pair of
     * numbers, so this assertion reads the table too - editing 720p's dimensions moves both sides together, and a fallback that had drifted from the table
     * would fail here where a hardcoded expectation could not tell the difference.
     */
    const defaultPreset = VIDEO_QUALITY_PRESETS.find((preset) => preset.id === "720p");

    assert.ok(defaultPreset, "the default preset is present in the table");

    assert.deepEqual(getPresetViewport(makeConfig("not-a-real-preset")),
      { height: defaultPreset.values["browser.viewport.height"], width: defaultPreset.values["browser.viewport.width"] },
      "an unknown id yields the default preset's dimensions");
  });

  test("returns the same dimensions on every call for a given preset (nothing outside configuration is consulted)", () => {

    /* Nothing but configuration feeds this getter: the surface is emulated at the configured preset, so two calls at any point in the process lifetime have to
     * agree. A regression that introduced ambient state - a cache, a live measurement, a display read - would show up here as two different answers.
     */
    const first = getPresetViewport(makeConfig("4k"));
    const second = getPresetViewport(makeConfig("4k"));

    assert.deepEqual(first, second, "repeated calls agree");
    assert.deepEqual(first, { height: 2160, width: 3840 }, "and both are the configured preset's own dimensions");
  });
});
