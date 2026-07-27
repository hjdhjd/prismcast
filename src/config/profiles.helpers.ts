/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * profiles.helpers.ts: Test-only factory for ResolvedSiteProfile fixtures. Co-located with profiles.ts, the production module that owns ResolvedSiteProfile
 * construction via resolveProfile and the URL-to-profile matching pipeline. Consumed across types/, browser/, and streaming/ test files. Excluded from the
 * build emit by the *.helpers.ts pattern in tsconfig.build.json.
 *
 * waitForNetworkIdle defaults to false, matching the production type's opt-in semantic for that flag.
 */
import type { ResolvedSiteProfile } from "../types/index.ts";
import { declareKeysOf } from "../testing.helpers.ts";

/**
 * Compile-time-complete enumeration of every key in ResolvedSiteProfile. Pair with assertSameShape in profiles.helpers.test.ts to catch drift in either
 * direction:
 *
 * - If ResolvedSiteProfile gains a key, declareKeysOf's completeness check fails to compile - the array must be updated.
 * - Once the array is updated, the assertSameShape test fails - the factory must populate the new key.
 *
 * makeProfile populates every field of ResolvedSiteProfile by design (the factory's purpose is producing a fully-shaped neutral profile), so strict-keyset
 * parity applies. Compare with makeChannel, which deliberately populates only the structural minimum and omits optional fields - no runtime parity check is
 * needed there because TypeScript's required-field check at the factory's return type covers required-field drift.
 */
export const RESOLVED_SITE_PROFILE_KEYS = declareKeysOf<ResolvedSiteProfile>()([

  "channelSelection",
  "channelSelector",
  "clickSelector",
  "clickToPlay",
  "dismissSelector",
  "fullscreenKey",
  "fullscreenSelector",
  "hideSelector",
  "lockVolumeProperties",
  "maxContinuousPlayback",
  "needsIframeHandling",
  "selectReadyVideo",
  "staticCapture",
  "useRequestFullscreen",
  "videoTimeout",
  "waitForNetworkIdle"
] as const);

/**
 * Constructs a ResolvedSiteProfile with all-neutral defaults (every flag off, every selector null, no fullscreen key, network-idle wait disabled, no continuous-
 * playback cap). The "channelSelection" default is { strategy: "none" } - the simplest valid strategy that satisfies the discriminated union. Tests that exercise
 * a specific strategy override via the overrides parameter.
 *
 * @param overrides - Partial fields to override the defaults.
 * @returns A ResolvedSiteProfile suitable for tests.
 */
export function makeProfile(overrides: Partial<ResolvedSiteProfile> = {}): ResolvedSiteProfile {

  return {

    channelSelection: { strategy: "none" },
    channelSelector: null,
    clickSelector: null,
    clickToPlay: false,
    dismissSelector: null,
    fullscreenKey: null,
    fullscreenSelector: null,
    hideSelector: null,
    lockVolumeProperties: false,
    maxContinuousPlayback: null,
    needsIframeHandling: false,
    selectReadyVideo: false,
    staticCapture: false,
    useRequestFullscreen: false,
    videoTimeout: null,
    waitForNetworkIdle: false,
    ...overrides
  };
}
