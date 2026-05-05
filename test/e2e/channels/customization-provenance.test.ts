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
import assert from "node:assert/strict";
import { getChannelCustomizations } from "../../../src/config/userChannels.ts";
import { mutateChannels } from "../../../src/config/userChannels.ts";

describe("getChannelCustomizations - provenance reporting", () => {

  test("a canonical-only channel with no overrides reports an empty customizations map", async () => {

    /* No user data on disk -> no customizations to surface. The form would show every field at its predefined default with no "modified" indicators.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
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

    void ctx;
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

    void ctx;
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

    void ctx;
    await initializePersistence(ctx);

    const customizations = getChannelCustomizations("definitely-not-a-channel-x9z2");

    assert.equal(customizations.customizations.size, 0, "no customizations for a non-existent key");
    assert.equal(customizations.activeVariantKey, undefined, "no active variant for a non-existent key");
  });
});
