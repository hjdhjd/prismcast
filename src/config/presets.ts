/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * presets.ts: Quality presets for PrismCast configuration.
 */
import type { Config } from "../types/index.ts";

/* Presets define video quality profiles that determine capture resolution (viewport) and provide default values for bitrate and frame rate. The selected preset is
 * stored in configuration and determines the viewport dimensions at runtime.
 *
 * Viewport is derived from the preset via getPresetViewport() and is not stored in CONFIG. That one getter is the whole viewport story: the browser launches with
 * its result as Puppeteer's viewport, so every page renders at those dimensions, and capture constraints, preroll sizing, and resolution monitoring all measure
 * against the same numbers.
 */

/**
 * Setting paths that every quality preset populates. Declaring them as a tight object type (instead of a general Record<string, number>) keeps the lookups
 * in this module statically typed - TypeScript knows each key is present, so we never have to narrow the result of preset.values[...] at call sites.
 */
export interface PresetValues {

  "browser.viewport.height": number;
  "browser.viewport.width": number;
  "streaming.frameRate": number;
  "streaming.videoBitsPerSecond": number;
}

/**
 * A quality preset that sets multiple configuration values at once.
 */
export interface QualityPreset {

  // Brief description of the preset's use case.
  description: string;

  // Unique identifier for the preset.
  id: string;

  // Display name shown in the preset selector.
  name: string;

  // Setting values to apply when this preset is selected.
  values: PresetValues;
}

/* Each preset is declared as a standalone const so downstream code can reference a specific preset by name (PRESET_720P as the default) with fully-narrowed
 * types. Exporting the assembled array gives consumers the ordered list; the individual consts give us stable references we can use without re-indexing into
 * the array and fighting the type system.
 */

const PRESET_480P: QualityPreset = {

  description: "Low bandwidth, older devices, minimal resource usage.",
  id: "480p",
  name: "480p",
  values: {

    "browser.viewport.height": 480,
    "browser.viewport.width": 854,
    "streaming.frameRate": 30,
    "streaming.videoBitsPerSecond": 3000000
  }
};

const PRESET_720P: QualityPreset = {

  description: "Balanced quality and bandwidth. Good for most content.",
  id: "720p",
  name: "720p",
  values: {

    "browser.viewport.height": 720,
    "browser.viewport.width": 1280,
    "streaming.frameRate": 60,
    "streaming.videoBitsPerSecond": 8000000
  }
};

const PRESET_720P_HIGH: QualityPreset = {

  description: "HD with higher bitrate. Best for sports and fast motion at 720p.",
  id: "720p-high",
  name: "720p High",
  values: {

    "browser.viewport.height": 720,
    "browser.viewport.width": 1280,
    "streaming.frameRate": 60,
    "streaming.videoBitsPerSecond": 12000000
  }
};

const PRESET_1080P: QualityPreset = {

  description: "Full HD resolution. Requires good bandwidth.",
  id: "1080p",
  name: "1080p",
  values: {

    "browser.viewport.height": 1080,
    "browser.viewport.width": 1920,
    "streaming.frameRate": 60,
    "streaming.videoBitsPerSecond": 15000000
  }
};

const PRESET_1080P_HIGH: QualityPreset = {

  description: "Full HD with higher bitrate. Best for sports and fast motion.",
  id: "1080p-high",
  name: "1080p High",
  values: {

    "browser.viewport.height": 1080,
    "browser.viewport.width": 1920,
    "streaming.frameRate": 60,
    "streaming.videoBitsPerSecond": 20000000
  }
};

const PRESET_4K: QualityPreset = {

  description: "4K resolution. High resource and bandwidth usage.",
  id: "4k",
  name: "4K",
  values: {

    "browser.viewport.height": 2160,
    "browser.viewport.width": 3840,
    "streaming.frameRate": 60,
    "streaming.videoBitsPerSecond": 35000000
  }
};

/**
 * Available video quality presets. These presets configure viewport dimensions, video bitrate, and frame rate for common use cases. The presets are ordered
 * from lowest to highest quality.
 */
export const VIDEO_QUALITY_PRESETS: QualityPreset[] = [
  PRESET_480P, PRESET_720P, PRESET_720P_HIGH, PRESET_1080P, PRESET_1080P_HIGH, PRESET_4K
];

// The default preset when no preset is configured. 720p is the established default.
const DEFAULT_PRESET = PRESET_720P;

/**
 * Returns the list of valid preset IDs.
 * @returns Array of preset ID strings.
 */
export function getValidPresetIds(): string[] {

  return VIDEO_QUALITY_PRESETS.map((p) => p.id);
}

/**
 * Returns the viewport dimensions for the currently configured quality preset. This is the single source of truth for the capture surface: Chrome launches with
 * these dimensions as Puppeteer's viewport, which emulates them on every page, and every consumer that has to agree with what capture produces - the capture
 * constraints, the preroll encode, the resolution monitor - reads them from here. Viewport is derived on demand rather than stored in CONFIG. The CONFIG
 * parameter is passed explicitly to avoid circular dependency issues between presets.ts and config/index.ts.
 * @param config - The configuration object containing the quality preset.
 * @returns The viewport dimensions for the configured preset, or the default preset's dimensions when the configured id matches no preset.
 */
export function getPresetViewport(config: Config): { height: number; width: number } {

  const presetId = config.streaming.qualityPreset;
  const preset = VIDEO_QUALITY_PRESETS.find((p) => p.id === presetId) ?? DEFAULT_PRESET;

  return {

    height: preset.values["browser.viewport.height"],
    width: preset.values["browser.viewport.width"]
  };
}
