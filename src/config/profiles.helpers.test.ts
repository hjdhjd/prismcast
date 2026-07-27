/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * profiles.helpers.test.ts: Tests for the makeProfile factory. The factory is consumed across types/, browser/, and streaming/ test files; a bug in defaults
 * or override-merging would cascade into every dependent suite.
 */
import { RESOLVED_SITE_PROFILE_KEYS, makeProfile } from "./profiles.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { assertSameShape } from "../testing.helpers.ts";

describe("makeProfile", () => {

  test("populates every ResolvedSiteProfile key (parity check against the type's complete key set)", () => {

    /* Two-layer drift catch (see registry.helpers.ts STREAM_REGISTRY_ENTRY_KEYS for the same pattern). The compile-time completeness check on
     * RESOLVED_SITE_PROFILE_KEYS forces the array to track ResolvedSiteProfile, and the runtime keyset check below forces the factory to populate every
     * key in the array.
     */
    const profile = makeProfile();
    const reference = Object.fromEntries(RESOLVED_SITE_PROFILE_KEYS.map((k) => [ k, undefined ]));

    assertSameShape(profile, reference, "makeProfile vs ResolvedSiteProfile's declared key set");
  });

  test("returns a fully-populated ResolvedSiteProfile with neutral defaults", () => {

    const profile = makeProfile();

    assert.deepEqual(profile.channelSelection, { strategy: "none" });
    assert.equal(profile.channelSelector, null);
    assert.equal(profile.clickSelector, null);
    assert.equal(profile.clickToPlay, false);
    assert.equal(profile.dismissSelector, null);
    assert.equal(profile.fullscreenKey, null);
    assert.equal(profile.fullscreenSelector, null);
    assert.equal(profile.hideSelector, null);
    assert.equal(profile.lockVolumeProperties, false);
    assert.equal(profile.maxContinuousPlayback, null);
    assert.equal(profile.needsIframeHandling, false);
    assert.equal(profile.selectReadyVideo, false);
    assert.equal(profile.staticCapture, false);
    assert.equal(profile.useRequestFullscreen, false);
    assert.equal(profile.videoTimeout, null);
    assert.equal(profile.waitForNetworkIdle, false);
  });

  test("merges overrides shallowly on top of defaults", () => {

    const profile = makeProfile({ clickToPlay: true, waitForNetworkIdle: true });

    assert.equal(profile.clickToPlay, true);
    assert.equal(profile.waitForNetworkIdle, true);
    assert.equal(profile.channelSelector, null, "non-overridden default survives");
  });

  test("returns a fresh object on each call (no shared reference)", () => {

    const a = makeProfile();
    const b = makeProfile();

    assert.notEqual(a, b);
    // The nested channelSelection should also be a fresh object on each call - object spreads do not deep-clone, but the literal in the factory is constructed
    // anew on each invocation.
    assert.notEqual(a.channelSelection, b.channelSelection);
  });

  test("accepts a non-default channelSelection strategy via overrides", () => {

    const profile = makeProfile({

      channelSelection: { matchSelector: ".channel-row", strategy: "thumbnailRow" }
    });

    assert.equal(profile.channelSelection.strategy, "thumbnailRow");
  });
});
