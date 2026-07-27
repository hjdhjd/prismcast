/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * manifestInterceptor.selection.test.ts: Unit tests for the pure selectInterceptedManifest helper that drives installManifestInterceptor()'s finalize() resolution.
 * This file isolates the selection rules - the direct-tune first-URL semantics and the guide-tune three-signal rule (channel-selection epoch, playlist membership,
 * liveness override) - so they remain locked against silent regression independent of the surrounding orchestration. The main orchestrators
 * (installManifestInterceptor, awaitMatchingManifest) are tested in the companion file manifestInterceptor.test.ts.
 */
import type { InterceptedManifestState, InterceptedMasterFact, InterceptedMediaFact, SelectedManifest } from "./manifestInterceptor.ts";
import { describe, test } from "node:test";
import type { Nullable } from "../types/index.ts";
import assert from "node:assert/strict";
import { selectInterceptedManifest } from "./manifestInterceptor.ts";

// Neutral state factory: every field starts at its no-capture default (guide tune, no first URLs, no latest facts, no epoch), so each test overrides only the
// clauses it exercises and its outcome can come from nothing else.
function makeState(overrides: Partial<InterceptedManifestState> = {}): InterceptedManifestState {

  return { directTune: false, firstMasterUrl: null, firstMediaUrl: null, latestMaster: null, latestMedia: null, markOrdinal: null, ...overrides };
}

function makeMaster(url: string, ordinal: number, childUrls: readonly string[] = []): InterceptedMasterFact {

  return { childUrls, ordinal, url };
}

function makeMedia(url: string, ordinal: number, live: boolean): InterceptedMediaFact {

  return { live, ordinal, url };
}

// Narrows a selection to non-null and asserts its URL and kind in one place, so each case reads as a single line and the widened return type's null arm is handled
// once rather than optional-chained at every assertion.
function assertSelection(result: Nullable<SelectedManifest>, expectedUrl: string, expectedKind: SelectedManifest["kind"], message?: string): void {

  const label = message ?? "a selection was returned";

  assert.ok(result, label);
  assert.equal(result.url, expectedUrl, label);
  assert.equal(result.kind, expectedKind, label);
}

describe("selectInterceptedManifest", () => {

  test("returns the first master URL on a direct tune when only master URLs were captured", () => {

    // Happy path: a direct-tune master-based site (e.g., A&E TVE). The first URL captured during page load is the player's loaded master and must be returned.
    const result = selectInterceptedManifest(makeState({

      directTune: true,
      firstMasterUrl: "https://cdn.test/first-master.m3u8",
      latestMaster: makeMaster("https://cdn.test/latest-master.m3u8", 2)
    }));

    assertSelection(result, "https://cdn.test/first-master.m3u8", "master");
  });

  test("returns the latest master URL on a guide tune when only master URLs were captured", () => {

    // Happy path: a guide-tune master-based site (e.g., Hulu, Sling) with no epoch declared. The latest master captured wins because, absent a click, there is no
    // fresher truth to prefer - exactly today's outcome.
    const result = selectInterceptedManifest(makeState({

      directTune: false,
      firstMasterUrl: "https://cdn.test/first-master.m3u8",
      latestMaster: makeMaster("https://cdn.test/latest-master.m3u8", 2)
    }));

    assertSelection(result, "https://cdn.test/latest-master.m3u8", "master");
  });

  test("returns the first media URL on a direct tune when only media URLs were captured", () => {

    // Happy path: a direct-tune media-only site (e.g., Angelcam from issue #34). No master playlist is ever served; the first media playlist captured must be
    // selected so the probe can normalize it into a MediaFeed.
    const result = selectInterceptedManifest(makeState({

      directTune: true,
      firstMediaUrl: "https://cdn.test/first-media.m3u8",
      latestMedia: makeMedia("https://cdn.test/latest-media.m3u8", 2, true)
    }));

    assertSelection(result, "https://cdn.test/first-media.m3u8", "media");
  });

  test("returns the latest media URL on a guide tune when only media URLs were captured", () => {

    // Happy path: a guide-tune media-only site. With no master to judge, the media fallback answers.
    const result = selectInterceptedManifest(makeState({

      directTune: false,
      firstMediaUrl: "https://cdn.test/first-media.m3u8",
      latestMedia: makeMedia("https://cdn.test/latest-media.m3u8", 2, true)
    }));

    assertSelection(result, "https://cdn.test/latest-media.m3u8", "media");
  });

  test("prefers the first master over a first media on a direct tune when both kinds were captured", () => {

    // On a direct tune, the branch takes firstMasterUrl before firstMediaUrl, so a master-based site whose player also loads a media playlist selects the master.
    // Downstream probing keeps access to the master's metadata (variant bandwidth, resolution, separate audio rendition) instead of degrading to media-only. The
    // epoch never participates on the direct branch.
    const result = selectInterceptedManifest(makeState({

      directTune: true,
      firstMasterUrl: "https://cdn.test/master.m3u8",
      firstMediaUrl: "https://cdn.test/media.m3u8",
      latestMaster: makeMaster("https://cdn.test/master.m3u8", 1),
      latestMedia: makeMedia("https://cdn.test/media.m3u8", 2, true)
    }));

    assertSelection(result, "https://cdn.test/master.m3u8", "master", "master wins on direct tune");
  });

  test("prefers the latest master over a media on a guide tune when no epoch was declared", () => {

    // On a guide tune with no epoch declared, the master wins over a later live media because the media override requires a declared epoch; without a click there
    // is no fresher truth, so the master answers - today's outcome for both resolution modes.
    const result = selectInterceptedManifest(makeState({

      directTune: false,
      latestMaster: makeMaster("https://cdn.test/latest-master.m3u8", 1),
      latestMedia: makeMedia("https://cdn.test/latest-media.m3u8", 2, true)
    }));

    assertSelection(result, "https://cdn.test/latest-master.m3u8", "master", "latest master wins on guide tune with no epoch");
  });

  test("falls back from null first-master to first-media on a direct tune (mixed asymmetric capture)", () => {

    // Boundary: a site may emit a master URL only after the player processes a click, while emitting a media URL up front. On a direct tune, the first-master
    // slot may be null even though a latest-master eventually arrived; the selection must not silently jump to the latest-master path because direct tune
    // semantics demand first-wins. Falling back to first-media is the right answer because that is the earliest qualifying URL.
    const result = selectInterceptedManifest(makeState({

      directTune: true,
      firstMediaUrl: "https://cdn.test/first-media.m3u8",
      latestMaster: makeMaster("https://cdn.test/late-master.m3u8", 3),
      latestMedia: makeMedia("https://cdn.test/latest-media.m3u8", 2, true)
    }));

    assertSelection(result, "https://cdn.test/first-media.m3u8", "media");
  });

  test("returns null when no URLs of either kind were captured", () => {

    // Negative test: no qualifying URL during the interception window. The selection helper must report null so the caller can fall back to capture mode rather
    // than feeding a malformed value to the probe.
    const result = selectInterceptedManifest(makeState({ directTune: true }));

    assert.equal(result, null);
  });
});

/* Acceptance cases for the guide-tune three-signal rule. Cases (a) through (j) and (m) each vary EXACTLY ONE clause from the flagship case (a), so a pass can only
 * come from the clause under test. Cases (k) and (l) are branch pins. FLAGSHIP is the shared baseline: a pre-epoch master (ordinal 3, below the mark of 5, with
 * non-matching children) and a post-epoch live non-member media (ordinal 7). The master's declared children never include the media's pathname, so membership is
 * a genuine non-member unless a case makes it one.
 */
describe("selectInterceptedManifest guide-tune three-signal rule", () => {

  const MARK = 5;
  const FLAG_MASTER = "https://master.test/chan-a/master.m3u8";
  const FLAG_MASTER_CHILDREN = [ "https://master.test/chan-a/720p.m3u8", "https://master.test/chan-a/1080p.m3u8" ];
  const FLAG_MEDIA = "https://cdn.test/chan-b/720p/chunklist.m3u8";

  test("(a) the fix: a pre-epoch master with a post-epoch live non-member media resolves the media", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 3, FLAG_MASTER_CHILDREN),
      latestMedia: makeMedia(FLAG_MEDIA, 7, true),
      markOrdinal: MARK
    }));

    assertSelection(result, FLAG_MEDIA, "media", "the clicked channel's live non-member media overrides the stale page-load master");
  });

  test("(b) a post-epoch master wins categorically even with a later foreign live media", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 6, FLAG_MASTER_CHILDREN),
      latestMedia: makeMedia(FLAG_MEDIA, 7, true),
      markOrdinal: MARK
    }));

    assertSelection(result, FLAG_MASTER, "master", "a master observed after the mark answers the click categorically");
  });

  test("(c) a member media (pathname equal across differing hosts, the redirect shape) keeps the master", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 3, FLAG_MASTER_CHILDREN),
      latestMedia: makeMedia("https://edge-cdn.test/chan-a/720p.m3u8", 7, true),
      markOrdinal: MARK
    }));

    assertSelection(result, FLAG_MASTER, "master", "a media whose pathname matches a declared child is a member and does not override");
  });

  test("(d) a non-live media does not override the master", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 3, FLAG_MASTER_CHILDREN),
      latestMedia: makeMedia(FLAG_MEDIA, 7, false),
      markOrdinal: MARK
    }));

    assertSelection(result, FLAG_MASTER, "master", "only a live media may override; a VOD-typed ad pod may not");
  });

  test("(e) a pre-epoch media does not override, at both the strictly-below and equal-to-mark ordinals", () => {

    const below = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 3, FLAG_MASTER_CHILDREN),
      latestMedia: makeMedia(FLAG_MEDIA, 4, true),
      markOrdinal: MARK
    }));

    assertSelection(below, FLAG_MASTER, "master", "a media strictly below the mark is pre-epoch and does not override");

    const atMark = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 3, FLAG_MASTER_CHILDREN),
      latestMedia: makeMedia(FLAG_MEDIA, MARK, true),
      markOrdinal: MARK
    }));

    assertSelection(atMark, FLAG_MASTER, "master", "a media exactly at the mark is pre-epoch (the media-side fence direction) and does not override");
  });

  test("(f) with no epoch declared the master wins even against a later live non-member media", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 3, FLAG_MASTER_CHILDREN),
      latestMedia: makeMedia(FLAG_MEDIA, 7, true),
      markOrdinal: null
    }));

    assertSelection(result, FLAG_MASTER, "master", "an absent epoch means no click, so no fresher truth exists and the master answers");
  });

  test("(g) an unparseable media URL keeps the master (member-conservative)", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 3, FLAG_MASTER_CHILDREN),
      latestMedia: makeMedia("not a url", 7, true),
      markOrdinal: MARK
    }));

    assertSelection(result, FLAG_MASTER, "master", "a media URL that cannot be parsed cannot be proven foreign, so it reads as a member");
  });

  test("(h) a master exactly at the mark is pre-epoch and the media overrides (master-side fence direction)", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, MARK, FLAG_MASTER_CHILDREN),
      latestMedia: makeMedia(FLAG_MEDIA, 7, true),
      markOrdinal: MARK
    }));

    assertSelection(result, FLAG_MEDIA, "media", "a master whose ordinal equals the mark arrived before the stamp and is overridable");
  });

  test("(i) an empty child list keeps the master (never trivial non-membership)", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 3, []),
      latestMedia: makeMedia(FLAG_MEDIA, 7, true),
      markOrdinal: MARK
    }));

    assertSelection(result, FLAG_MASTER, "master", "with no declared children, membership cannot be disproven, so the master wins");
  });

  test("(j) an all-unparseable child list keeps the master (empty effective set)", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 3, [ "not a url", "also not a url" ]),
      latestMedia: makeMedia(FLAG_MEDIA, 7, true),
      markOrdinal: MARK
    }));

    assertSelection(result, FLAG_MASTER, "master", "children that all fail to parse yield an empty effective set, so the master wins");
  });

  test("(m) a mixed child list drops the malformed entry and overrides via the remaining non-member child", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 3, [ "not a url", "https://master.test/chan-a/720p.m3u8" ]),
      latestMedia: makeMedia(FLAG_MEDIA, 7, true),
      markOrdinal: MARK
    }));

    assertSelection(result, FLAG_MEDIA, "media", "the malformed child drops, the valid non-matching child leaves a non-empty set, and the media is a proven non-member");
  });

  test("(k) an epoch stamped before any observation (markOrdinal 0) makes the first master post-epoch and categorical", () => {

    const result = selectInterceptedManifest(makeState({

      latestMaster: makeMaster(FLAG_MASTER, 1, FLAG_MASTER_CHILDREN),
      latestMedia: makeMedia(FLAG_MEDIA, 2, true),
      markOrdinal: 0
    }));

    assertSelection(result, FLAG_MASTER, "master", "markOrdinal 0 is a real epoch distinct from null, so the ordinal-1 master reads post-epoch and wins");
  });

  test("(l) the no-master media fallback ignores liveness", () => {

    const result = selectInterceptedManifest(makeState({

      latestMedia: makeMedia(FLAG_MEDIA, 3, false),
      markOrdinal: MARK
    }));

    assertSelection(result, FLAG_MEDIA, "media", "with no master to judge, the media fallback answers regardless of liveness or the epoch");
  });
});
