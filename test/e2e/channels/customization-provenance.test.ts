/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * customization-provenance.test.ts: Integration coverage for getChannelCustomizations - the per-field provenance accessor that drives the channel edit form's
 * "modified from default" indicators. The accessor reports for each customized field where the override is stored (canonical or variant) and what value the
 * field would reset to if the override were removed.
 *
 * Provenance correctness is foundational to the form save path: without it, the form cannot tell which entry to write to or which value to display when the
 * user clicks "reset." Bugs here surface as misrouted saves (binding fields landing on canonical when a variant is active, or vice versa) and incorrect
 * reset values.
 */
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { getChannelCustomizations, mutateChannels } from "../../../src/config/userChannels.ts";
import assert from "node:assert/strict";
import { setServiceSelection } from "../../../src/config/services.ts";

describe("getChannelCustomizations - provenance reporting", () => {

  test("a canonical-only channel with no overrides reports an empty customizations map", async () => {

    /* No user data on disk -> no customizations to surface. The form would show every field at its predefined default with no "modified" indicators.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const customizations = getChannelCustomizations("abc");

    assert.equal(customizations.customizations.size, 0, "no customizations on a clean channel");
    assert.equal(customizations.activeVariantKey, undefined, "no active variant when canonical service is active and no selection");
  });

  test("a user-set channelNumber on the canonical reports as a canonical-stored customization", async () => {

    /* Identity field on canonical -> customizations.get("channelNumber").storedIn === "canonical". The reset value comes from computeResetValue, which for a
     * canonical-stored field reads from the predefined canonical itself.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7 };
    });

    const customizations = getChannelCustomizations("abc");
    const channelNumberEntry = customizations.customizations.get("channelNumber");

    assert.ok(channelNumberEntry, "channelNumber should be in the customizations map");
    assert.equal(channelNumberEntry.storedIn, "canonical", "channelNumber override is stored on canonical");
  });

  test("multiple customizations on different storage locations are reported correctly", async () => {

    /* Set several overrides at once and verify each is reported with the correct storedIn. The map size should equal the number of distinct customized fields
     * (canonicalKey is structural and excluded by the recordCustomizations filter).
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7, stationId: "10068" };
    });

    const customizations = getChannelCustomizations("abc");

    assert.equal(customizations.customizations.size, 2, "two customized fields should be reported");
    assert.ok(customizations.customizations.has("channelNumber"), "channelNumber present");
    assert.ok(customizations.customizations.has("stationId"), "stationId present");

    for(const entry of customizations.customizations.values()) {

      assert.equal(entry.storedIn, "canonical", "both identity overrides are canonical-stored");
    }
  });

  test("a non-existent canonical key returns an empty customizations map without throwing", async () => {

    /* Defensive: a key with no predefined canonical and no user entry has nothing to report. The accessor must not throw - the form would then render an
     * empty edit state.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const customizations = getChannelCustomizations("definitely-not-a-channel-x9z2");

    assert.equal(customizations.customizations.size, 0, "no customizations for a non-existent key");
    assert.equal(customizations.activeVariantKey, undefined, "no active variant for a non-existent key");
  });

  test("Pass 2 (variant-stored): a binding override on the active variant reports as variant-stored with activeVariantKey set", async () => {

    /* When a non-canonical service is active and the user has stored a binding override on the variant entry, getChannelCustomizations reports the override
     * with storedIn="variant" and exposes activeVariantKey. This exercises the variant walk (Pass 2), which surfaces overrides stored on the active variant
     * entry rather than the canonical.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    /* Set a service selection to switch active variant from canonical (abc) to abc-hulu, then write a stored binding override on abc-hulu.
     */
    await setServiceSelection("abc", "abc-hulu");

    await mutateChannels((data) => {

      data.channels["abc-hulu"] = { canonicalKey: "abc", channelSelector: "ABC-CUSTOM" };
    });

    const customizations = getChannelCustomizations("abc");

    assert.equal(customizations.activeVariantKey, "abc-hulu", "active variant matches the service selection");

    const channelSelectorEntry = customizations.customizations.get("channelSelector");

    assert.ok(channelSelectorEntry, "channelSelector should be in the customizations map (Pass 2)");
    assert.equal(channelSelectorEntry.storedIn, "variant", "binding override on active variant reports storedIn='variant'");
  });

  test("Pass 2 silently drops identity fields encountered on a variant-stored entry (allowed-fields gate)", async () => {

    /* The recordCustomizations helper restricts variant-pass walks via getAllowedFieldsForShape, which forbids identity fields on variants. If a stored variant
     * entry carries identity (e.g., legacy data), Pass 2 must NOT surface it as a customization. This asserts the field-gate branch.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await setServiceSelection("abc", "abc-hulu");

    /* Write a stored variant entry with both binding (channelSelector, allowed) and identity (name, NOT allowed on variants). Pass 2 should surface only the
     * binding field.
     */
    await mutateChannels((data) => {

      data.channels["abc-hulu"] = { canonicalKey: "abc", channelSelector: "ABC-CUSTOM", name: "Should Be Dropped" };
    });

    const customizations = getChannelCustomizations("abc");

    assert.ok(customizations.customizations.has("channelSelector"), "binding field surfaces");
    assert.equal(customizations.customizations.has("name"), false, "identity field on variant entry is silently dropped");
  });

  test("Pass 2 variant-stored override wins over Pass 1 canonical-stored override on the same field name (last-overlay-wins matches resolution)", async () => {

    /* If the same field name is set on both canonical and variant entries, the variant overlay applies last during resolution. The customizations accessor
     * mirrors that: Pass 2 entries overwrite Pass 1 entries on the same key. This asserts the order-of-overlay rule for the customization map.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await setServiceSelection("abc", "abc-hulu");

    await mutateChannels((data) => {

      // Pass 1: a canonical-stored binding override (channelSelector). Normally this would route to a variant via the form router, but a stored entry can carry
      // it directly via direct mutation. The point is to seed both layers with the same field name to force the precedence test.
      data.channels["abc"] = { channelSelector: "FROM-CANONICAL" };
      data.channels["abc-hulu"] = { canonicalKey: "abc", channelSelector: "FROM-VARIANT" };
    });

    const customizations = getChannelCustomizations("abc");
    const entry = customizations.customizations.get("channelSelector");

    assert.ok(entry, "channelSelector is in the customizations map");
    assert.equal(entry.storedIn, "variant", "Pass 2 (variant) wins over Pass 1 (canonical) on the same field name");
  });
});
