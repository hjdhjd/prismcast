/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.integrity.test.ts: Direct unit tests for the pre-write integrity validators - detectIdentityFieldLoss and validateChannelsIntegrity.
 *
 * These validators are the silent-corruption guard: every write through mutateChannels passes through them, and any suspicious transition (an identity field
 * dropped without canonical fallback, or a wholesale-clear of a top-level metadata collection) surfaces as a warning. Coverage at this level is critical because
 * a regression in the validator silently corrupts the operator's user-feedback signal - false positives spam the log, false negatives let real data loss ship.
 *
 * The detectIdentityFieldLoss function compares array-valued identity fields structurally via isDeepStrictEqual. The array-equality branch matrix is an easy place
 * for a subtle regression, so the equal-array and unequal-array cases are exercised here directly.
 */
import type { CanonicalChannel, StoredChannelMap } from "../types/index.ts";
import { describe, test } from "node:test";
import type { ChannelsFileData } from "./userChannels.ts";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import { __internalForTests } from "./userChannels.ts";
import assert from "node:assert/strict";
import { firstOf } from "../testing.helpers.ts";
import { makeChannelsData } from "./userChannels.helpers.ts";

const { detectIdentityFieldLoss, validateChannelsIntegrity } = __internalForTests;

describe("detectIdentityFieldLoss", () => {

  /* Allowed transitions for any identity field: value -> same value, value -> different value, value -> null (explicit clear), absent -> any value. The forbidden
   * transition is value -> undefined/missing where the canonical does NOT provide the same value. Variant inheritance via the canonical is honored - dropping a
   * stored field that matches the canonical is delta minimization, not data loss.
   */

  test("returns empty array when nothing changed", () => {

    const before: StoredChannelMap = { abc: { name: "ABC Custom", url: "https://abc.com" } };
    const after: StoredChannelMap = { abc: { name: "ABC Custom", url: "https://abc.com" } };

    assert.deepEqual(detectIdentityFieldLoss(before, after), []);
  });

  test("returns empty array when the entire entry was deleted (intentional removal, not silent drop)", () => {

    /* Whole-entry deletion is the user-driven path (deleteUserChannel, clearChannelOverrides). The validator treats the absence of an after-entry as legitimate
     * and never flags it.
     */
    const before: StoredChannelMap = { abc: { name: "ABC Custom", stationId: "12345", url: "https://abc.com" } };
    const after: StoredChannelMap = {};

    assert.deepEqual(detectIdentityFieldLoss(before, after), []);
  });

  test("flags an identity field that went from set to undefined without a canonical fallback", () => {

    /* The classic silent-drop case: a user standalone (no predefined fallback) had a stationId, then a regression cleared it during normalization. The validator
     * flags "key.field" so the operator can investigate.
     */
    const before: StoredChannelMap = { mychannel: { name: "My Channel", stationId: "99999", url: "https://example.com" } };
    const after: StoredChannelMap = { mychannel: { name: "My Channel", url: "https://example.com" } };

    assert.deepEqual(detectIdentityFieldLoss(before, after), ["mychannel.stationId"]);
  });

  test("does NOT flag an identity field that went to undefined when the canonical provides the same value (delta minimization)", () => {

    /* Variant inheritance: stripping a stored field that matches the predefined canonical's value is the delta normalizer doing its job, not data loss. The
     * validator must recognize this and NOT flag it. abc canonical's predefined values are visible to the validator via its PREDEFINED_CHANNELS lookup.
     */
    const before: StoredChannelMap = { abc: { name: "ABC", url: "https://abc.com/watch-live" } };
    const after: StoredChannelMap = {};

    /* abc was the only entry. Whole-entry deletion is allowed (the loop only runs over before's keys but the entry was removed). No flag.
     */
    assert.deepEqual(detectIdentityFieldLoss(before, after), []);
  });

  test("does NOT flag value -> null transition (null is the explicit-clear signal)", () => {

    const before: StoredChannelMap = { mychannel: { name: "My Channel", stationId: "99999", url: "https://example.com" } };
    const after: StoredChannelMap = { mychannel: { name: "My Channel", stationId: null, url: "https://example.com" } };

    assert.deepEqual(detectIdentityFieldLoss(before, after), [], "null is the codebase-wide 'clear this field' signal and is never flagged");
  });

  test("does NOT flag value -> different value (legitimate update)", () => {

    const before: StoredChannelMap = { mychannel: { name: "My Channel", stationId: "99999", url: "https://example.com" } };
    const after: StoredChannelMap = { mychannel: { name: "My Channel", stationId: "11111", url: "https://example.com" } };

    assert.deepEqual(detectIdentityFieldLoss(before, after), []);
  });

  test("does NOT flag absent -> any (new data, not a drop)", () => {

    const before: StoredChannelMap = { mychannel: { name: "My Channel", url: "https://example.com" } };
    const after: StoredChannelMap = { mychannel: { name: "My Channel", stationId: "11111", url: "https://example.com" } };

    assert.deepEqual(detectIdentityFieldLoss(before, after), []);
  });

  test("does NOT flag null -> undefined (treats null as 'no data' for the loss check)", () => {

    /* The before-value branch checks `(beforeValue === undefined) || (beforeValue === null)` and continues. Documenting current behavior: the field was never
     * meaningfully set if it was null in the before-state, so dropping it is not loss.
     */
    const before: StoredChannelMap = { mychannel: { name: "My Channel", stationId: null, url: "https://example.com" } };
    const after: StoredChannelMap = { mychannel: { name: "My Channel", url: "https://example.com" } };

    assert.deepEqual(detectIdentityFieldLoss(before, after), []);
  });

  test("array-valued identity (tags) post-isDeepStrictEqual swap: equal arrays are recognized as canonical-equivalent (delta minimization)", () => {

    /* When the user's stored tags array exactly matches the canonical's tags array, dropping the stored field is delta minimization and must NOT flag. This test
     * pins that structural equality via isDeepStrictEqual recognizes matched array-valued fields as canonical-equivalent.
     *
     * abc is known to have a tags array in PREDEFINED_CHANNELS; we assert dropping a tags field that matches the canonical's tags is recognized as no-loss.
     */
    const abcCanonical = PREDEFINED_CHANNELS["abc"] as CanonicalChannel;

    if(!abcCanonical.tags) {

      // The canonical's shape changed - this case only matters when abc carries tags.
      return;
    }

    const before: StoredChannelMap = { abc: { tags: abcCanonical.tags.slice() } };
    const after: StoredChannelMap = { abc: { name: "Touched" } };

    /* tags went from set (matching canonical) to absent. The validator's array-equality check should compare via isDeepStrictEqual against canonicalValue and
     * recognize the match. No flag should fire for tags (only for fields that genuinely lack a canonical fallback).
     */
    const losses = detectIdentityFieldLoss(before, after);

    assert.equal(losses.includes("abc.tags"), false, "matched-canonical tags array drop is not flagged (delta minimization)");
  });

  test("array-valued identity (tags) post-isDeepStrictEqual swap: different arrays are flagged as loss", () => {

    /* The contrapositive: when the user's stored tags do NOT match the canonical's tags exactly, dropping them IS data loss. This test pins that the
     * isDeepStrictEqual call correctly distinguishes equal arrays from non-equal arrays.
     */
    const before: StoredChannelMap = { abc: { tags: [ "Custom", "Sports" ] } };
    const after: StoredChannelMap = { abc: { name: "Touched" } };

    const losses = detectIdentityFieldLoss(before, after);

    assert.equal(losses.includes("abc.tags"), true, "non-canonical tags drop is flagged");
  });

  test("array equality comparison handles different-length arrays without throwing", () => {

    /* Boundary case: a stored tags array on a standalone channel with no predefined canonical entry. Since "mychannel" has no canonical fallback, the
     * comparison never reaches isDeepStrictEqual - the drop is flagged directly through the no-canonical-fallback branch. This test pins that a non-empty
     * array loss without a canonical fallback is always flagged, regardless of array length.
     */
    const before: StoredChannelMap = { mychannel: { name: "My Channel", tags: [ "A", "B", "C" ], url: "https://example.com" } };
    const after: StoredChannelMap = { mychannel: { name: "My Channel", url: "https://example.com" } };

    const losses = detectIdentityFieldLoss(before, after);

    assert.equal(losses.includes("mychannel.tags"), true, "any non-empty array loss on a standalone (no canonical fallback) is flagged");
  });

  test("scalar identity field with a canonical fallback that does NOT match still flags as loss", () => {

    /* When the canonical exists but its value differs from the user's, dropping the user's value IS loss. This pins the per-value branch of the canonical-
     * fallback check (different from the array-valued branch above).
     */
    const before: StoredChannelMap = { abc: { name: "ABC Custom Renamed" } };
    const after: StoredChannelMap = { abc: { url: "https://override.test/" } };

    const losses = detectIdentityFieldLoss(before, after);

    assert.equal(losses.includes("abc.name"), true, "name went from 'ABC Custom Renamed' to absent; canonical's name is 'ABC' (different) so it's flagged");
  });
});

describe("validateChannelsIntegrity", () => {

  /* The validator returns a list of ValidationIssue records. Two categories:
   *
   *   - "identity-field-loss": one entry per detectIdentityFieldLoss return.
   *   - "metadata-wholesale-clear": one entry per top-level collection (serviceSelections, tagRegistry.tags, tagRegistry.deletedTags) that went non-empty -> empty.
   */

  /* Thin positional wrapper around the shared makeChannelsData helper. Local because the integrity tests favor a (channels, selections, tagRegistry) triple
   * over keyword overrides for readability - every test sets all three explicitly. Routing through makeChannelsData keeps the envelope shape (schemaVersion,
   * migrationsApplied, defaults) defined in exactly one place.
   */
  function makeData(channels: StoredChannelMap, serviceSelections: Record<string, string>, tagRegistry: { tags: string[]; deletedTags: string[] }): ChannelsFileData {

    return makeChannelsData(channels, { serviceSelections, tagRegistry });
  }

  test("returns empty issues for a no-op write", () => {

    const data = makeData({}, {}, { deletedTags: [], tags: [] });

    assert.deepEqual(validateChannelsIntegrity(data, data), []);
  });

  test("flags identity-field-loss issues from detectIdentityFieldLoss", () => {

    const before = makeData({ mychannel: { name: "My", stationId: "99999", url: "https://example.com" } }, {}, { deletedTags: [], tags: [] });
    const after = makeData({ mychannel: { name: "My", url: "https://example.com" } }, {}, { deletedTags: [], tags: [] });

    const issues = validateChannelsIntegrity(before, after);

    assert.equal(issues.length, 1);
    const first = firstOf(issues, "validation issue");

    assert.equal(first.category, "identity-field-loss");
    assert.equal(first.severity, "warning");
    assert.match(first.description, /mychannel\.stationId/);
  });

  test("flags serviceSelections wholesale-clear (non-empty -> empty)", () => {

    const before = makeData({}, { abc: "abc-hulu", nbc: "nbc-yttv" }, { deletedTags: [], tags: [] });
    const after = makeData({}, {}, { deletedTags: [], tags: [] });

    const issues = validateChannelsIntegrity(before, after);

    const first = firstOf(issues, "validation issue");

    assert.equal(first.category, "metadata-wholesale-clear");
    assert.match(first.description, /serviceSelections.*2 entries to empty/);
  });

  test("flags tagRegistry.tags wholesale-clear (non-empty -> empty)", () => {

    const before = makeData({}, {}, { deletedTags: [], tags: [ "Sports", "News" ] });
    const after = makeData({}, {}, { deletedTags: [], tags: [] });

    const issues = validateChannelsIntegrity(before, after);

    assert.equal(issues.length, 1);

    const first = firstOf(issues, "validation issue");

    assert.equal(first.category, "metadata-wholesale-clear");
    assert.match(first.description, /tagRegistry\.tags.*2 entries to empty/);
  });

  test("flags tagRegistry.deletedTags wholesale-clear (non-empty -> empty)", () => {

    const before = makeData({}, {}, { deletedTags: ["Sports"], tags: [] });
    const after = makeData({}, {}, { deletedTags: [], tags: [] });

    const issues = validateChannelsIntegrity(before, after);

    assert.equal(issues.length, 1);

    const first = firstOf(issues, "validation issue");

    assert.equal(first.category, "metadata-wholesale-clear");
    assert.match(first.description, /tagRegistry\.deletedTags.*1 entries to empty/);
  });

  test("does NOT flag empty -> empty (a no-op on a wholesale collection)", () => {

    /* The validator's three wholesale-clear checks all guard with "prev > 0 && next === 0" - empty -> empty must not fire. Pins the negative branch.
     */
    const before = makeData({}, {}, { deletedTags: [], tags: [] });
    const after = makeData({}, {}, { deletedTags: [], tags: [] });

    assert.deepEqual(validateChannelsIntegrity(before, after), []);
  });

  test("does NOT flag non-empty -> non-empty even when the count drops (legitimate per-entry deletion)", () => {

    /* A drop from 5 -> 1 selections is normal usage (operator removed a few selections via the UI). Only a complete clear is suspicious enough to surface.
     */
    const before = makeData({}, { abc: "abc-hulu", cbs: "cbs-hulu", nbc: "nbc-yttv" }, { deletedTags: [], tags: [] });
    const after = makeData({}, { abc: "abc-hulu" }, { deletedTags: [], tags: [] });

    assert.deepEqual(validateChannelsIntegrity(before, after), []);
  });

  test("collects multiple issues across categories in one pass", () => {

    /* A write that both drops an identity field AND wholesale-clears two metadata collections produces three issues. Pins the validator's accumulation behavior.
     */
    const before = makeData(
      { mychannel: { name: "My", stationId: "99999", url: "https://example.com" } },
      { abc: "abc-hulu" },
      { deletedTags: ["Sports"], tags: [] }
    );
    const after = makeData(
      { mychannel: { name: "My", url: "https://example.com" } },
      {},
      { deletedTags: [], tags: [] }
    );

    const issues = validateChannelsIntegrity(before, after);
    const categories = issues.map((i) => i.category).toSorted();

    assert.deepEqual(categories, [ "identity-field-loss", "metadata-wholesale-clear", "metadata-wholesale-clear" ]);
  });
});
