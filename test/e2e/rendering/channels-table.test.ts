/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channels-table.test.ts: HTML-rendering integration coverage for the channel-table generator. The generator at routes/config/channels/table.ts produces
 * row HTML for each channel based on the resolved listing - identity from canonical, binding from variant - plus row classes that reflect state (predefined
 * vs user vs override, disabled, service-filtered). Render bugs in this layer surface to operators as channels rendered in the wrong state class even though
 * the underlying data is correct.
 *
 * The generator is a pure string-producing function. We don't need a DOM here: assert via substring/regex on the generated string. The integration value over
 * unit-tier coverage is exercising the generator against the REAL resolved listing produced by getChannelListing() against a populated channel store -
 * regressions that only manifest when the listing pipeline disagrees with the renderer's expectations show up here.
 */
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { generateChannelRowHtml } from "../../../src/routes/config/channels/table.ts";
import { getProfiles } from "../../../src/config/profiles.ts";
import { mutateChannels } from "../../../src/config/userChannels.ts";

describe("generateChannelRowHtml - canonical / variant / override visual classes", () => {

  test("a predefined canonical with no user customization renders WITHOUT the override or user-channel classes", async () => {

    /* The "abc" canonical is a real predefined channel. Without any user customization on disk, its row should reflect predefined-only state: no
     * channel-override class (no user delta), no user-channel class (not user-created).
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const { displayRow } = generateChannelRowHtml("abc", getProfiles());

    assert.ok(displayRow.length > 0, "displayRow should be generated for a real predefined channel");
    assert.doesNotMatch(displayRow, /\bclass="[^"]*\bchannel-override\b/, "no override class without a user override");
    assert.doesNotMatch(displayRow, /\bclass="[^"]*\buser-channel\b/, "no user-channel class for a predefined-only channel");
  });

  test("a predefined canonical with a user channelNumber override renders WITH the channel-override class", async () => {

    /* The override pattern: the user types a channelNumber into the inline-editable cell on a predefined row. Stored as a delta on the canonical entry. The
     * display row should pick up the channel-override class, signaling "modified from defaults" via CSS.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7 };
    });

    const { displayRow } = generateChannelRowHtml("abc", getProfiles());

    assert.match(displayRow, /\bclass="[^"]*\bchannel-override\b/, "override delta on a predefined channel produces the channel-override class");
    assert.doesNotMatch(displayRow, /\bclass="[^"]*\buser-channel\b/, "an override on a predefined is NOT a user-only channel");
  });

  test("a fully user-defined channel renders WITH the user-channel class and WITHOUT channel-override", async () => {

    /* A standalone user channel (key not present in PREDEFINED_CHANNELS) is purely user-authored. It should carry user-channel and no override class.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["custom-user-channel"] = { name: "Custom", url: "https://example.test/custom" };
    });

    const { displayRow } = generateChannelRowHtml("custom-user-channel", getProfiles());

    assert.ok(displayRow.length > 0, "displayRow should be generated for a user channel");
    assert.match(displayRow, /\bclass="[^"]*\buser-channel\b/, "a fully-user channel carries the user-channel class");
    assert.doesNotMatch(displayRow, /\bclass="[^"]*\bchannel-override\b/, "a user-only channel is not an override");
  });

  test("the user-set channelNumber surfaces in the display row text content", async () => {

    /* Beyond the class assertions, the resolved channelNumber should appear somewhere in the display row HTML - the cell that holds it would otherwise show
     * the predefined default (or empty), which would be a render-vs-data divergence the user notices first.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7 };
    });

    const { displayRow } = generateChannelRowHtml("abc", getProfiles());

    /* Look for the channelNumber 7 in the row. The cell wraps it in HTML so the literal "7" appears between tags - we just need the digit somewhere in the
     * output. We avoid pinning the exact cell wrapper because that's a UI detail; the data invariant is what matters.
     */
    assert.match(displayRow, />7</, "channelNumber 7 should appear in the rendered display row");
  });

  test("a non-existent channel key produces empty rows (defensive)", async () => {

    /* Boundary: the generator looks up the listing by key and returns empty rows when not found. The route handler should never call this for an unknown key,
     * but the defensive contract pins the safe-by-default behavior.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const result = generateChannelRowHtml("definitely-not-a-channel-x9z2", getProfiles());

    assert.equal(result.displayRow, "", "displayRow should be empty for a missing key");
    assert.equal(result.editRow, "", "editRow should be empty for a missing key");
  });
});
