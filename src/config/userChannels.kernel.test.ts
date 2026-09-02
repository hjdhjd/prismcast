/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.kernel.test.ts: Direct unit tests for the overlay kernel and the field-shape gates that feed it.
 *
 * Coverage scope:
 *
 *   - applyOverlayKernel: the consolidated control-flow shared by overlayDelta (passThroughOthers: true) and overlayVariantBinding (passThroughOthers: false).
 *     Public callers transitively cover both option-bag combinations, but the direct branch matrix - allowed-field gate, value-classification (null/undefined/
 *     value), passThroughOthers vs canonicalKey carve-out, defensive tag copy - is asserted here in isolation so a regression in the consolidation surfaces
 *     locally.
 *
 *   - getAllowedFieldsForShape: the single source of truth for "what fields are allowed in this entry's stored shape" - canonical-shaped (full delta surface)
 *     vs variant-shaped (binding-only plus the canonicalKey tag). filterToDeltaSurface and getChannelCustomizations both consume this; asserting the
 *     classifier-driven branches prevents drift between the two consumers.
 *
 *   - filterToDeltaSurface: the storage-write-time shape enforcer. Called from normalizeChannelDeltas on every save, so the strip-vs-keep decision is the gate
 *     that prevents legacy orphan fields (non-delta-eligible identity, identity-on-variants, DOM hooks) from persisting forward.
 *
 * applyOverlayKernel is the kernel shared by overlayDelta and overlayVariantBinding. The public callers' tests confirm consumer-level guarantees;
 * these tests assert the kernel's contract directly so a future refactor that breaks the abstraction (e.g., reintroduces a divergent branch in one of the wrappers)
 * fails locally rather than only via downstream callers.
 */
import type { ResolvedChannel, StoredChannel } from "../types/index.ts";
import { describe, test } from "node:test";
import { __internalForTests } from "./userChannels.ts";
import assert from "node:assert/strict";

const { applyOverlayKernel, filterToDeltaSurface, getAllowedFieldsForShape, overlayVariantBinding } = __internalForTests;

describe("applyOverlayKernel", () => {

  /* The kernel is parameterized by allowedFields and passThroughOthers. The behavior matrix:
   *
   *   field IN allowedFields, value !== undefined, value !== null  -> override on resolved
   *   field IN allowedFields, value === null                       -> delete from resolved (clear semantic)
   *   field IN allowedFields, value === undefined                  -> skip (inherit base)
   *   field NOT in allowedFields, passThroughOthers === true       -> override on resolved (delta carve-out)
   *   field NOT in allowedFields, passThroughOthers === false, field === "canonicalKey" -> override (relationship-metadata carve-out)
   *   field NOT in allowedFields, passThroughOthers === false, field !== "canonicalKey" -> drop silently (variant overlay rule)
   *
   * Each test below asserts one cell in this matrix.
   */

  test("allowed field with concrete value overrides the base", () => {

    const base = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;
    const stored = { name: "ABC Custom" } as StoredChannel;
    const result = applyOverlayKernel(base, stored, { allowedFields: new Set(["name"]), passThroughOthers: false });

    assert.equal(result.name, "ABC Custom");
    assert.equal(result.url, "https://abc.com", "non-overridden base field survives");
  });

  test("allowed field with null clears the field on the resolved object", () => {

    const base = { name: "ABC", stationId: "12345", url: "https://abc.com" } as ResolvedChannel;
    const stored = { stationId: null } as StoredChannel;
    const result = applyOverlayKernel(base, stored, { allowedFields: new Set(["stationId"]), passThroughOthers: false });

    assert.equal("stationId" in result, false, "null delta on an allowed field deletes the property");
  });

  test("allowed field with undefined inherits from the base (no overwrite)", () => {

    const base = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;
    const stored = { name: undefined } as StoredChannel;
    const result = applyOverlayKernel(base, stored, { allowedFields: new Set(["name"]), passThroughOthers: false });

    assert.equal(result.name, "ABC", "undefined is a no-op even on an allowed field");
  });

  test("non-allowed field passes through when passThroughOthers is true (delta-mode behavior)", () => {

    // Delta mode: the kernel relays any non-allowed field whose value is concrete. This is the overlayDelta contract.
    const base = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;
    const stored = { name: "ABC Custom" } as StoredChannel;
    const result = applyOverlayKernel(base, stored, { allowedFields: new Set(), passThroughOthers: true });

    assert.equal(result.name, "ABC Custom", "passThroughOthers=true relays the override even though name is not in allowedFields");
  });

  test("non-allowed canonicalKey ALWAYS passes through even when passThroughOthers is false (variant-overlay carve-out)", () => {

    // Variant overlay: identity is dropped (per the architectural principle), but canonicalKey is relationship metadata that must survive resolution.
    const base = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;
    const stored = { canonicalKey: "abc", name: "Should Be Dropped" } as StoredChannel;
    const result = applyOverlayKernel(base, stored, { allowedFields: new Set(["url"]), passThroughOthers: false });

    assert.equal(result.canonicalKey, "abc", "canonicalKey passes through despite passThroughOthers=false");
    assert.equal(result.name, "ABC", "name (a non-allowed identity field) is silently dropped");
  });

  test("non-allowed non-canonicalKey field is silently dropped when passThroughOthers is false (variant-overlay rule)", () => {

    // Variant entry carrying an identity field (e.g., a legacy stationId on a variant). The kernel drops it without warning - the architectural rule that
    // identity always wins from the canonical means variants with identity fields are inert at resolution time.
    const base = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;
    const stored = { stationId: "12345" } as StoredChannel;
    const result = applyOverlayKernel(base, stored, { allowedFields: new Set(["url"]), passThroughOthers: false });

    assert.equal("stationId" in result, false, "non-allowed field outside the canonicalKey carve-out is silently dropped");
  });

  test("undefined-valued non-allowed field is skipped regardless of passThroughOthers", () => {

    /* The "undefined inherits" rule applies uniformly across both gates. This test asserts the boundary case where a non-allowed field has an undefined value -
     * passThroughOthers=true would otherwise relay it, but the value-classification check supersedes the allowlist gate.
     */
    const base = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;
    const stored = { stationId: undefined } as StoredChannel;
    const result = applyOverlayKernel(base, stored, { allowedFields: new Set(), passThroughOthers: true });

    assert.equal("stationId" in result, false, "undefined value on a non-allowed field is skipped");
  });

  test("returns a fresh reference (callers can mutate the result without touching the base)", () => {

    const base = { name: "ABC", url: "https://abc.com" } as ResolvedChannel;
    const result = applyOverlayKernel(base, {}, { allowedFields: new Set(), passThroughOthers: false });

    assert.notEqual(result, base, "result is a fresh object");
    result.name = "Mutated";
    assert.equal(base.name, "ABC", "mutation on the result does not leak into the base");
  });

  test("defensively copies the tags array when present on the base (no shared reference)", () => {

    const base = { name: "ABC", tags: [ "Local", "News" ], url: "https://abc.com" } as ResolvedChannel;
    const result = applyOverlayKernel(base, {}, { allowedFields: new Set(), passThroughOthers: false });

    result.tags!.push("Mutated");
    assert.deepEqual(base.tags, [ "Local", "News" ], "base tags are not affected by mutations on the resolved tags");
  });

  test("defensively copies the tags array after a delta replaces it", () => {

    /* The kernel's spread-then-overlay sequence places the stored entry's tags onto the resolved object. The defensive copy at the end ensures the resolved.tags
     * is detached from any shared reference - both the base's array (when no delta) and the stored entry's array (when a delta is applied).
     */
    const base = { name: "ABC", tags: ["News"], url: "https://abc.com" } as ResolvedChannel;
    const storedTags = [ "Local", "Sports" ];
    const stored = { tags: storedTags } as StoredChannel;
    const result = applyOverlayKernel(base, stored, { allowedFields: new Set(["tags"]), passThroughOthers: false });

    result.tags!.push("Mutated");
    assert.deepEqual(storedTags, [ "Local", "Sports" ], "the original delta tags array is not mutated by changes to the result");
  });
});

describe("getAllowedFieldsForShape", () => {

  /* The classifier-driven branches:
   *
   *   classification.kind === "variant"            -> DELTA_ELIGIBLE_BINDING_KEYS + the canonicalKey tag
   *   classification.kind === "canonical"          -> full delta surface (identity ∪ binding, both delta-eligible)
   *   classification.kind === "standalone"         -> full delta surface (treated as canonical-shaped, since standalones carry identity + binding)
   */

  test("returns the variant binding-only set plus canonicalKey for a variant-classified entry", () => {

    // abc-hulu is a known predefined variant: classifyEntry sees canonicalKey on the predefined entry.
    const allowed = getAllowedFieldsForShape("abc-hulu", { channelSelector: "ABC" });

    assert.equal(allowed.has("canonicalKey"), true, "canonicalKey is allowed on variant-shaped entries");
    assert.equal(allowed.has("channelSelector"), true, "binding fields are allowed");
    assert.equal(allowed.has("url"), true, "url is binding");
    assert.equal(allowed.has("profile"), true, "profile is binding");
    assert.equal(allowed.has("stationId"), false, "identity fields are NOT allowed on variants");
    assert.equal(allowed.has("channelNumber"), false, "channelNumber is identity");
    assert.equal(allowed.has("name"), false, "name is identity");
  });

  test("returns the full delta surface for a canonical-classified entry", () => {

    const allowed = getAllowedFieldsForShape("abc", { name: "ABC Custom" });

    assert.equal(allowed.has("name"), true, "identity fields allowed on canonical-shaped entries");
    assert.equal(allowed.has("stationId"), true);
    assert.equal(allowed.has("channelNumber"), true);
    assert.equal(allowed.has("tags"), true);
    assert.equal(allowed.has("url"), true, "binding fields also allowed");
    assert.equal(allowed.has("channelSelector"), true);
  });

  test("treats a user-only standalone (no canonicalKey, no predefined match) as canonical-shaped", () => {

    /* Standalone classification follows the same shape rules as canonical: identity + binding both allowed. This is the symmetric path - a user-created channel
     * with no predefined catalog entry has nothing to inherit from, so its full delta surface is its own storage surface.
     */
    const allowed = getAllowedFieldsForShape("mychannel", { name: "My Channel", url: "https://example.com" });

    assert.equal(allowed.has("name"), true);
    assert.equal(allowed.has("url"), true);
    assert.equal(allowed.has("stationId"), true);
  });
});

describe("filterToDeltaSurface", () => {

  /* The shape-strip is enforced at storage-write time, so test inputs are full of legacy "orphan" fields that should be cleaned. The function returns both the
   * filtered entry and a list of stripped field names so callers can warn the operator.
   */

  test("strips identity fields from a variant-shaped entry (the architectural rule that variants are pure binding)", () => {

    /* A user-created variant entry that mistakenly carries identity (channelNumber, stationId). The shape filter strips them and reports the names. The strip
     * happens during normalizeChannelDeltas on next save, so legacy data with this shape cleans up automatically.
     */
    const stored = { canonicalKey: "abc", channelNumber: 7, channelSelector: "KABC", stationId: "57342", url: "https://example.com" } as StoredChannel;
    const { filtered, stripped } = filterToDeltaSurface("abc-kabc", stored);

    assert.equal((filtered as Record<string, unknown>)["canonicalKey"], "abc", "canonicalKey survives");
    assert.equal((filtered as Record<string, unknown>)["channelSelector"], "KABC", "binding survives");
    assert.equal((filtered as Record<string, unknown>)["url"], "https://example.com", "url survives");
    assert.equal("channelNumber" in filtered, false, "identity field stripped from variant");
    assert.equal("stationId" in filtered, false, "identity field stripped from variant");
    assert.deepEqual(stripped.toSorted(), [ "channelNumber", "stationId" ].toSorted(), "stripped list reports the names so the caller can warn");
  });

  test("strips DOM-hook binding fields from a canonical-shaped entry (only delta-eligible fields survive)", () => {

    /* Internal DOM hooks (dismissSelector, scrollSelector, etc.) are populated by site profiles, never by user input. A stored entry carrying them is a legacy
     * artifact - the shape filter strips them on next save.
     */
    const stored = { dismissSelector: "#dismiss", name: "ABC", scrollSelector: ".scroll", url: "https://abc.com" } as unknown as StoredChannel;
    const { filtered, stripped } = filterToDeltaSurface("abc", stored);

    assert.equal((filtered as Record<string, unknown>)["name"], "ABC");
    assert.equal((filtered as Record<string, unknown>)["url"], "https://abc.com");
    assert.equal("dismissSelector" in filtered, false, "DOM-hook field stripped");
    assert.equal("scrollSelector" in filtered, false, "DOM-hook field stripped");
    assert.deepEqual(stripped.toSorted(), [ "dismissSelector", "scrollSelector" ].toSorted());
  });

  test("returns an empty stripped list when every field is allowed", () => {

    /* Happy path: a clean canonical entry with no orphan fields. The filter returns the entry unchanged plus an empty stripped list (so no warning fires).
     */
    const stored = { name: "ABC Custom", stationId: "10068" } as StoredChannel;
    const { filtered, stripped } = filterToDeltaSurface("abc", stored);

    assert.deepEqual(filtered, stored);
    assert.deepEqual(stripped, []);
  });
});

describe("overlayVariantBinding", () => {

  /* Variant overlay is a thin wrapper over applyOverlayKernel with passThroughOthers=false. The branch matrix is covered by applyOverlayKernel's direct tests;
   * this is a single contract test confirming the wrapper picks the right options and returns the expected variant-shaped result.
   */

  test("layers variant binding fields onto canonical identity (canonical wins on identity, variant wins on binding)", () => {

    const canonical = { name: "ABC", tags: ["News"], url: "https://abc.com" } as ResolvedChannel;
    const variantStored = { canonicalKey: "abc", channelNumber: 99, channelSelector: "HULU", url: "https://hulu.com/abc" } as StoredChannel;
    const result = overlayVariantBinding(canonical, variantStored);

    assert.equal(result.name, "ABC", "canonical identity survives the variant overlay");
    assert.deepEqual(result.tags, ["News"], "canonical tags survive");
    assert.equal(result.url, "https://hulu.com/abc", "variant binding wins for url");
    assert.equal(result.channelSelector, "HULU", "variant binding wins for channelSelector");
    assert.equal(result.canonicalKey, "abc", "canonicalKey passes through as relationship metadata");
    assert.equal("channelNumber" in result, false, "channelNumber (identity on a variant) is silently dropped");
  });
});
