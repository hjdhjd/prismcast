/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * manifestInterceptor.selection.test.ts: Unit tests for the pure selectInterceptedManifest helper that drives installManifestInterceptor()'s finalize() resolution.
 * This file isolates the selection rules - master-priority across kinds and direct-vs-guide first/latest semantics - so they remain locked against silent
 * regression independent of the surrounding orchestration. The main orchestrators (installManifestInterceptor, awaitMatchingManifest) are tested in the
 * companion file manifestInterceptor.test.ts.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { selectInterceptedManifest } from "./manifestInterceptor.ts";

describe("selectInterceptedManifest", () => {

  test("returns the first master URL on a direct tune when only master URLs were captured", () => {

    // Happy path: a direct-tune master-based site (e.g., A&E TVE). The first URL captured during page load is the player's loaded master and must be returned.
    const result = selectInterceptedManifest({

      directTune: true,
      firstMasterUrl: "https://cdn.test/first-master.m3u8",
      firstMediaUrl: null,
      latestMasterUrl: "https://cdn.test/latest-master.m3u8",
      latestMediaUrl: null
    });

    assert.equal(result, "https://cdn.test/first-master.m3u8");
  });

  test("returns the latest master URL on a guide tune when only master URLs were captured", () => {

    // Happy path: a guide-tune master-based site (e.g., Hulu, Sling). The latest URL captured is the one from the channel-switch click and must win over older
    // captures from the page's default channel.
    const result = selectInterceptedManifest({

      directTune: false,
      firstMasterUrl: "https://cdn.test/first-master.m3u8",
      firstMediaUrl: null,
      latestMasterUrl: "https://cdn.test/latest-master.m3u8",
      latestMediaUrl: null
    });

    assert.equal(result, "https://cdn.test/latest-master.m3u8");
  });

  test("returns the first media URL on a direct tune when only media URLs were captured", () => {

    // Happy path: a direct-tune media-only site (e.g., Angelcam from issue #34). No master playlist is ever served; the first media playlist captured must be
    // selected so the probe can normalize it into a MediaFeed.
    const result = selectInterceptedManifest({

      directTune: true,
      firstMasterUrl: null,
      firstMediaUrl: "https://cdn.test/first-media.m3u8",
      latestMasterUrl: null,
      latestMediaUrl: "https://cdn.test/latest-media.m3u8"
    });

    assert.equal(result, "https://cdn.test/first-media.m3u8");
  });

  test("returns the latest media URL on a guide tune when only media URLs were captured", () => {

    // Happy path: a hypothetical guide-tune media-only site. Same latest-wins semantics as guide-tune master-based sites.
    const result = selectInterceptedManifest({

      directTune: false,
      firstMasterUrl: null,
      firstMediaUrl: "https://cdn.test/first-media.m3u8",
      latestMasterUrl: null,
      latestMediaUrl: "https://cdn.test/latest-media.m3u8"
    });

    assert.equal(result, "https://cdn.test/latest-media.m3u8");
  });

  test("master URL outranks media URL on a direct tune when both kinds were captured", () => {

    // Boundary: master priority. A master-based site whose player happens to also load a media playlist (variant, prefetch, etc.) must select the master URL,
    // not the incidentally-captured media URL. Without master priority, downstream probing would lose access to the master's metadata (variant bandwidth,
    // resolution, separate audio rendition) and degrade to media-only treatment.
    const result = selectInterceptedManifest({

      directTune: true,
      firstMasterUrl: "https://cdn.test/master.m3u8",
      firstMediaUrl: "https://cdn.test/media.m3u8",
      latestMasterUrl: "https://cdn.test/master.m3u8",
      latestMediaUrl: "https://cdn.test/media.m3u8"
    });

    assert.equal(result, "https://cdn.test/master.m3u8", "master wins on direct tune");
  });

  test("master URL outranks media URL on a guide tune when both kinds were captured", () => {

    // Boundary on guide tunes: master priority must hold for both resolution modes. Locks the rule against an asymmetric reordering that would only honor
    // priority on direct tunes.
    const result = selectInterceptedManifest({

      directTune: false,
      firstMasterUrl: "https://cdn.test/master.m3u8",
      firstMediaUrl: "https://cdn.test/media.m3u8",
      latestMasterUrl: "https://cdn.test/latest-master.m3u8",
      latestMediaUrl: "https://cdn.test/latest-media.m3u8"
    });

    assert.equal(result, "https://cdn.test/latest-master.m3u8", "latest master wins on guide tune");
  });

  test("falls back from null first-master to first-media on a direct tune (mixed asymmetric capture)", () => {

    // Boundary: a site may emit a master URL only after the player processes a click, while emitting a media URL up front. On a direct tune, the first-master
    // slot may be null even though a latest-master eventually arrived; the selection must not silently jump to the latest-master path because direct tune
    // semantics demand first-wins. Falling back to first-media is the right answer because that is the earliest qualifying URL.
    const result = selectInterceptedManifest({

      directTune: true,
      firstMasterUrl: null,
      firstMediaUrl: "https://cdn.test/first-media.m3u8",
      latestMasterUrl: "https://cdn.test/late-master.m3u8",
      latestMediaUrl: "https://cdn.test/latest-media.m3u8"
    });

    assert.equal(result, "https://cdn.test/first-media.m3u8");
  });

  test("returns null when no URLs of either kind were captured", () => {

    // Negative test: no qualifying URL during the interception window. The selection helper must report null so the caller can fall back to capture mode rather
    // than feeding a malformed value to the probe.
    const result = selectInterceptedManifest({

      directTune: true,
      firstMasterUrl: null,
      firstMediaUrl: null,
      latestMasterUrl: null,
      latestMediaUrl: null
    });

    assert.equal(result, null);
  });
});
