/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streamSettings.helpers.ts: Test-only factory for StreamSettings fixtures. Co-located with streamSettings.ts, the configuration-layer module that owns the
 * snapshot's shape and its builder. Consumed by the configuration and streaming test suites wherever a row constructs a stream's setup, capture, monitor, or
 * segmenter. Excluded from the build emit by the *.helpers.ts pattern in tsconfig.build.json.
 *
 * Every row starts from the defaults, never CONFIG, so no row inherits a value another row assigned to the running configuration. A row that cares about a
 * setting states it through the overrides, and the factory builds through the production builder so the fixture's shape is the snapshot's own.
 */
import { DEFAULTS } from "./userConfig.ts";
import type { StreamSettings } from "./streamSettings.ts";
import { declareKeysOf } from "../testing.helpers.ts";
import { snapshotStreamSettings } from "./streamSettings.ts";

/**
 * Compile-time-complete enumeration of every key in StreamSettings. Pair with assertSameShape in streamSettings.helpers.test.ts to catch drift in either
 * direction:
 *
 * - If StreamSettings gains a key, declareKeysOf's completeness check fails to compile - the array must be updated.
 * - Once the array is updated, the assertSameShape test fails - the factory must populate the new key.
 */
export const STREAM_SETTINGS_KEYS = declareKeysOf<StreamSettings>()([

  "audioBitsPerSecond",
  "frameRate",
  "monitorInterval",
  "segmentDuration",
  "videoBitsPerSecond"
] as const);

/**
 * Constructs a stream's settings for a test: the snapshot of the defaults with the row's overrides applied, frozen as production freezes the snapshot.
 * @param overrides - The members the row states, each replacing its default.
 * @returns A frozen StreamSettings.
 */
export function makeStreamSettings(overrides: Partial<StreamSettings> = {}): StreamSettings {

  return Object.freeze({ ...snapshotStreamSettings(DEFAULTS), ...overrides });
}
