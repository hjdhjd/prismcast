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

describe("generateChannelRowHtml - data-default reset-button contract for customized override fields", () => {

  /* The channel edit form emits per-field `data-default` attributes that the client-side resetSetting / resetAllToDefaults handlers read to restore the
   * predefined value when the operator clicks the per-field reset button. The wire format is:
   *
   *   - String fields (channelNumber, guideTitle, logoUrl, stationId, channelSelector): data-default="<predefined-value>" (HTML-escaped).
   *   - Tags array: data-default="News, Sports" (comma-space-joined, matching the form's hidden-input representation).
   *   - hdhrEnabled boolean: data-default="true" or data-default="false" (pre-stringified strings, NOT JS booleans).
   *
   * The DOM-runtime suite covers the CLIENT side of the contract via synthesized fixture inputs. The accessor tier covers `getChannelCustomizations` /
   * `computeResetValue` directly. Neither tier covers the channel-edit-form output END-TO-END: this test fills that gap by rendering the actual edit form via
   * generateChannelRowHtml against a customized override and asserting the literal data-default attribute values appear on the right inputs.
   */

  test("emits data-default with the predefined defaults for every customized field on an override channel", async () => {

    // The "abc" canonical has predefined name="ABC", tags=["Local"], no channelNumber, no stationId, no channelSelector at canonical level (selectors live on
    // service variants). We customize channelNumber, guideTitle, logoUrl, tags, and hdhrEnabled, then assert that the edit form emits data-default attributes
    // with the predefined values (including the empty string for fields that have no predefined default but are still customized).
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["abc"] = {

        channelNumber: 7,
        guideTitle: "Custom ABC",
        hdhrEnabled: false,
        logoUrl: "https://example.test/logo.png",
        tags: [ "News", "Sports" ]
      };
    });

    const { editRow } = generateChannelRowHtml("abc", getProfiles());

    // Tags hidden input carries data-default="Local" because the predefined default ["Local"] is joined by ", ". Verify the comma-join contract.
    assert.match(editRow, /name="tags"[^>]*data-default="Local"/,
      "tags field carries data-default with the predefined tags joined by comma-space");

    // hdhrEnabled checkbox: predefined default is implicit true (channels default to enabled in the lineup). After customization to false, data-default emits
    // the pre-stringified literal "true" - NOT the boolean true (which would render as data-default="true" anyway, but the contract is the string).
    assert.match(editRow, /id="edit-abc-hdhrEnabled"[^>]*data-default="true"/,
      "hdhrEnabled checkbox carries data-default=\"true\" (string, matching the resetValueFor stringification)");

    // channelNumber field: predefined has no channelNumber, so the data-default is the empty string (resetValueFor maps the undefined predefined value to ""
    // - see table.ts).
    assert.match(editRow, /name="channelNumber"[^>]*data-default=""/,
      "channelNumber field carries data-default=\"\" when the predefined had no value");

    // guideTitle field: similarly defaults to empty string.
    assert.match(editRow, /name="guideTitle"[^>]*data-default=""/,
      "guideTitle field carries data-default=\"\" when the predefined had no value");

    // logoUrl field: similarly defaults to empty string.
    assert.match(editRow, /name="logoUrl"[^>]*data-default=""/,
      "logoUrl field carries data-default=\"\" when the predefined had no value");
  });

  test("does NOT emit data-default attributes on a non-customized predefined channel (predefined-only path skips defaults)", async () => {

    // Negative test: when the channel has no override (predefined-only), generateChannelRowHtml passes defaults: undefined to generateAdvancedFields, and the
    // helpers skip the data-default attribute entirely (the defaultAttr ternary in generateTextField: `(options.defaultValue !== undefined) ? ... : ""`).
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const { editRow } = generateChannelRowHtml("abc", getProfiles());

    // The hdhr checkbox row should not carry data-default when the channel is not an override.
    const hdhrLine = editRow.split("\n").find((line) => line.includes("id=\"edit-abc-hdhrEnabled\""));

    assert.ok(hdhrLine, "hdhrEnabled row was rendered");
    assert.equal(hdhrLine.includes("data-default"), false, "no data-default attribute on hdhrEnabled when not customized");
  });

  test("HTML-escapes the data-default value to defend against quote injection from logoUrl", async () => {

    // Boundary: a customized logoUrl with embedded quotes or HTML-special characters would, if not escaped, break out of the value attribute on the input. The
    // escapeHtml(value) call at table.ts:131 is the defense; we exercise it by setting a logoUrl that contains an embedded '&' character which must surface as
    // &amp; in the rendered value attribute (the assertion below matches value="...&amp;...").
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    // Set a customization so the field renders with the operator's value. We inspect the value attribute carrying the customized URL with its escaped & marker.
    await mutateChannels((data) => {

      data.channels["abc"] = { logoUrl: "https://example.test/logo.png?id=1&type=logo" };
    });

    const { editRow } = generateChannelRowHtml("abc", getProfiles());

    // The current value (the customization) is in the value attribute, and a literal & must appear as &amp; in the rendered HTML.
    assert.match(editRow, /value="https:\/\/example\.test\/logo\.png\?id=1&amp;type=logo"/,
      "ampersand in customized logoUrl is HTML-escaped to &amp; in the value attribute");
  });
});
