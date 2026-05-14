/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * presets.ts: Quality presets for PrismCast configuration.
 */
import type { Config, Nullable } from "../types/index.ts";
import { getMaxSupportedViewport } from "../browser/display.ts";

/* Presets define video quality profiles that determine capture resolution (viewport) and provide default values for bitrate and frame rate. The selected preset is
 * stored in configuration and determines the viewport dimensions at runtime.
 *
 * Viewport is derived from the preset via getPresetViewport() and is not stored in CONFIG. For runtime operations that need display-aware viewport dimensions, use
 * getEffectiveViewport() which accounts for display limitations.
 */

// Default viewport dimensions used as fallback if preset lookup fails. Matches 720p preset.
const DEFAULT_VIEWPORT = { height: 720, width: 1280 };

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

/* Each preset is declared as a standalone const so downstream code can reference specific presets by name (PRESET_480P as the minimum fallback, PRESET_720P
 * as the default) with fully-narrowed types. Exporting the assembled array gives consumers the ordered list; exporting the individual consts gives us stable
 * references we can use without re-indexing into the array and fighting the type system.
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

// The minimum preset used when even the lowest-resolution preset does not fit the display - Chrome will constrain the actual viewport further.
const MINIMUM_PRESET = PRESET_480P;

/**
 * Returns the list of valid preset IDs.
 * @returns Array of preset ID strings.
 */
export function getValidPresetIds(): string[] {

  return VIDEO_QUALITY_PRESETS.map((p) => p.id);
}

/**
 * Returns the viewport dimensions for the currently configured quality preset. This function derives viewport on-demand from the preset rather than storing it in
 * CONFIG. The CONFIG parameter is passed explicitly to avoid circular dependency issues between presets.ts and config/index.ts.
 *
 * WARNING: This function returns the configured preset viewport without considering display limitations. For runtime operations (stream capture, window sizing), use
 * getEffectiveViewport() instead, which accounts for display size constraints.
 * @param config - The configuration object containing the quality preset.
 * @returns The viewport dimensions for the configured preset, or default 720p dimensions if preset not found.
 */
export function getPresetViewport(config: Config): { height: number; width: number } {

  const preset = VIDEO_QUALITY_PRESETS.find((p) => p.id === config.streaming.qualityPreset);

  if(!preset) {

    return DEFAULT_VIEWPORT;
  }

  return {

    height: preset.values["browser.viewport.height"],
    width: preset.values["browser.viewport.width"]
  };
}

/* When the configured preset requires a viewport larger than the user's display can accommodate, we gracefully degrade to the largest supported preset. This ensures
 * video capture works correctly even when the user has configured a resolution their display cannot support.
 *
 * The effective preset system:
 * 1. Detects maximum supported viewport during browser initialization (see browser/display.ts)
 * 2. Compares configured preset against display limits
 * 3. Selects the largest preset that fits within the display
 * 4. Provides status information for logging and UI display
 */

/**
 * Result of effective preset resolution, containing both configured and effective presets along with degradation status.
 */
export interface EffectivePresetResult {

  // The preset the user configured in settings.
  configuredPreset: QualityPreset;

  // Whether the effective preset differs from the configured preset due to display limitations.
  degraded: boolean;

  // The preset that will actually be used, accounting for display limitations.
  effectivePreset: QualityPreset;

  // The maximum supported viewport dimensions, or null if not yet detected.
  maxViewport: Nullable<{ height: number; width: number }>;
}

/**
 * Finds the largest preset that fits within the given maximum dimensions. Presets are checked from highest to lowest resolution (4K down to 480p).
 * @param maxWidth - Maximum available viewport width.
 * @param maxHeight - Maximum available viewport height.
 * @returns The largest fitting preset, or null if no preset fits (extremely small display).
 */
export function findBestFittingPreset(maxWidth: number, maxHeight: number): Nullable<QualityPreset> {

  // Iterate from highest to lowest resolution via toReversed() so the search order reads naturally as "try the largest first" and the loop body deals in
  // fully-typed QualityPreset values rather than array-index lookups.
  for(const preset of VIDEO_QUALITY_PRESETS.toReversed()) {

    const presetWidth = preset.values["browser.viewport.width"];
    const presetHeight = preset.values["browser.viewport.height"];

    if((presetWidth <= maxWidth) && (presetHeight <= maxHeight)) {

      return preset;
    }
  }

  return null;
}

/**
 * Resolves the effective preset to use based on the configured preset and display limitations. If the configured preset fits within the display, it is returned
 * unchanged. Otherwise, the largest fitting preset is selected.
 * @param config - The configuration object containing the quality preset.
 * @returns The effective preset result with configured preset, effective preset, and degradation status.
 */
export function getEffectivePreset(config: Config): EffectivePresetResult {

  // Find the configured preset.
  const configuredPreset = VIDEO_QUALITY_PRESETS.find((p) => p.id === config.streaming.qualityPreset) ?? DEFAULT_PRESET;
  const maxViewport = getMaxSupportedViewport();

  // If display detection hasn't completed yet, use configured preset without degradation.
  if(!maxViewport) {

    return {

      configuredPreset,
      degraded: false,
      effectivePreset: configuredPreset,
      maxViewport: null
    };
  }

  // Check if configured preset fits within display limits.
  const configuredWidth = configuredPreset.values["browser.viewport.width"];
  const configuredHeight = configuredPreset.values["browser.viewport.height"];

  if((configuredWidth <= maxViewport.width) && (configuredHeight <= maxViewport.height)) {

    return {

      configuredPreset,
      degraded: false,
      effectivePreset: configuredPreset,
      maxViewport
    };
  }

  // Configured preset doesn't fit - find the best alternative.
  const bestFitting = findBestFittingPreset(maxViewport.width, maxViewport.height);

  // Edge case: no preset fits (extremely small display). Use the minimum preset and let Chrome constrain the actual viewport further.
  if(!bestFitting) {

    return {

      configuredPreset,
      degraded: true,
      effectivePreset: MINIMUM_PRESET,
      maxViewport
    };
  }

  return {

    configuredPreset,
    degraded: true,
    effectivePreset: bestFitting,
    maxViewport
  };
}

/**
 * Returns the effective viewport dimensions accounting for display limitations. This is the primary function for runtime code paths that need viewport dimensions
 * for stream capture and window sizing.
 * @param config - The configuration object containing the quality preset.
 * @returns The effective viewport dimensions.
 */
export function getEffectiveViewport(config: Config): { height: number; width: number } {

  const result = getEffectivePreset(config);

  return {

    height: result.effectivePreset.values["browser.viewport.height"],
    width: result.effectivePreset.values["browser.viewport.width"]
  };
}

/**
 * Formats the preset status for display in logs, health endpoint, and UI. Returns a concise string showing the configured preset and any degradation.
 * @param result - The effective preset result from getEffectivePreset().
 * @returns Formatted status string.
 */
export function formatPresetStatus(result: EffectivePresetResult): string {

  const effectiveWidth = result.effectivePreset.values["browser.viewport.width"];
  const effectiveHeight = result.effectivePreset.values["browser.viewport.height"];

  if(!result.degraded) {

    return result.configuredPreset.id + " (" + String(effectiveWidth) + "\u00d7" + String(effectiveHeight) + ")";
  }

  return result.configuredPreset.id + " (limited to " + result.effectivePreset.id + " by display)";
}

/**
 * A preset option with degradation information for UI display.
 */
export interface PresetOption {

  // The preset this option represents.
  preset: QualityPreset;

  // The preset this will degrade to, or null if no degradation needed.
  degradedTo: Nullable<QualityPreset>;
}

/**
 * Result of preset options with display capability information.
 */
export interface PresetOptionsResult {

  // Maximum supported viewport dimensions, or null if display detection hasn't completed.
  maxViewport: Nullable<{ height: number; width: number }>;

  // All available presets with their degradation status.
  options: PresetOption[];
}

/**
 * Returns all preset options with degradation information for UI display. Each preset includes information about what it will degrade to (if anything) based on
 * current display capabilities. If display detection hasn't completed yet, all presets are returned without degradation information.
 * @returns Preset options with degradation status and max viewport info.
 */
export function getPresetOptionsWithDegradation(): PresetOptionsResult {

  const maxViewport = getMaxSupportedViewport();

  // If display detection hasn't completed, return all presets without degradation info.
  if(!maxViewport) {

    return {

      maxViewport: null,
      options: VIDEO_QUALITY_PRESETS.map((preset) => ({

        degradedTo: null,
        preset
      }))
    };
  }

  // Find the best fitting preset for this display.
  const bestFitting = findBestFittingPreset(maxViewport.width, maxViewport.height);

  // Build options with degradation info for each preset.
  const options: PresetOption[] = VIDEO_QUALITY_PRESETS.map((preset) => {

    const presetWidth = preset.values["browser.viewport.width"];
    const presetHeight = preset.values["browser.viewport.height"];

    // Check if this preset fits within display limits.
    if((presetWidth <= maxViewport.width) && (presetHeight <= maxViewport.height)) {

      return {

        degradedTo: null,
        preset
      };
    }

    // This preset doesn't fit - it will degrade to the best fitting preset.
    return {

      degradedTo: bestFitting ?? MINIMUM_PRESET,
      preset
    };
  });

  return {

    maxViewport,
    options
  };
}
