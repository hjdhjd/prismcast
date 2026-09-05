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
import { createIntegrationContext, initializePersistence, pathInDataDir } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { generateChannelRowHtml, generateChannelsPanel } from "../../../src/routes/config/channels/table.ts";
import { getChannelsParseErrorMessage, getUserChannelsFilePath, mutateChannels } from "../../../src/config/userChannels.ts";
import assert from "node:assert/strict";
import { escapeHtml } from "../../../src/utils/index.ts";
import { getProfiles } from "../../../src/config/profiles.ts";
import { mutateProfiles } from "../../../src/config/userProfiles.ts";
import { writeFile } from "node:fs/promises";

describe("generateChannelRowHtml - canonical / variant / override visual classes", () => {

  test("a predefined canonical with no user customization renders WITHOUT the override or user-channel classes", async () => {

    /* The "abc" canonical is a real predefined channel. Without any user customization on disk, its row should reflect predefined-only state: no
     * channel-override class (no user delta), no user-channel class (not user-created).
     */
    await using ctx = await createIntegrationContext();

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

    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7 };
    });

    const { displayRow } = generateChannelRowHtml("abc", getProfiles());

    /* Look for the channelNumber 7 in the row. The cell wraps it in HTML so the literal "7" appears between tags - we just need the digit somewhere in the
     * output. We avoid asserting the exact cell wrapper because that's a UI detail; what matters is that the data is present.
     */
    assert.match(displayRow, />7</, "channelNumber 7 should appear in the rendered display row");
  });

  test("a non-existent channel key produces empty rows (defensive)", async () => {

    /* Boundary: the generator looks up the listing by key and returns empty rows when not found. The route handler should never call this for an unknown key,
     * but the defensive contract asserts the safe-by-default behavior.
     */
    await using ctx = await createIntegrationContext();

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

describe("generateChannelRowHtml - Profile column explicit vs auto-resolved branch", () => {

  test("an explicit channel.profile renders the Profile cell as the profile name verbatim, without the (auto) marker or muted styling", async () => {

    /* The Profile column has two rendering branches (table.ts). When channel.profile is set, the explicit branch emits a bare <td> whose content is the profile
     * name run through escapeHtml - no wrapping span, no "(auto)" suffix, no text-muted class. The auto-resolved branch (no explicit profile) instead wraps the
     * derived service label in <span class="text-muted">Label (auto)</span>. This test asserts the explicit branch so a regression that accidentally routed an
     * explicit assignment through the muted/auto styling - visually implying "we guessed this" when the operator set it deliberately - is caught.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed a user channel that names a REAL builtin profile explicitly. "fullscreenApi" is an existing SITE_PROFILES entry, so this resolves to a real profile
    // and exercises the explicit branch (channel.profile truthy) rather than the auto-resolved fallback.
    await mutateChannels((data) => {

      data.channels["explicit-profile-channel"] = { name: "Explicit", profile: "fullscreenApi", url: "https://example.test/explicit" };
    });

    const { displayRow } = generateChannelRowHtml("explicit-profile-channel", getProfiles());

    // Isolate the Profile column cell (col-profile). The explicit branch emits a single-line <td> with the escaped profile name and no child <span>.
    const profileCell = displayRow.split("\n").find((line) => line.includes("class=\"col-profile\""));

    assert.ok(profileCell, "the Profile column cell must be present in the display row");

    // The cell content is the profile name verbatim, closing directly with </td> - the explicit branch does not wrap it in a span.
    assert.match(profileCell, />fullscreenApi<\/td>/, "the explicit profile name renders verbatim as the Profile cell content");

    // Distinct from the auto-resolved branch: neither the "(auto)" suffix nor the muted styling appears on an explicit assignment.
    assert.equal(profileCell.includes("(auto)"), false, "an explicit profile must not carry the (auto) marker the auto-resolved branch appends");
    assert.equal(profileCell.includes("text-muted"), false, "an explicit profile must not carry the text-muted styling the auto-resolved branch applies");
  });
});

describe("generateChannelsPanel - validation errors block", () => {

  test("a non-empty formErrors map renders a Validation Errors block with one HTML-escaped <li> per field", async () => {

    /* formErrors is the fifth positional parameter of generateChannelsPanel (channelMessage, channelError, editingChannelKey, showAddForm, formErrors,
     * formValues). When it is non-empty the panel emits a "Validation Errors" block with a <ul> carrying one <li> per entry, each rendering the field name in a
     * <strong> and the message after it - both through escapeHtml. This asserts the count-per-field rule and the escaping of BOTH positions, so a regression
     * that dropped escaping on either the field name or the message (an XSS vector, since messages can echo user input) surfaces.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Put HTML markup in BOTH the field name and the message so a regression dropping escaping on either position is caught. A second plain entry asserts the
    // one-<li>-per-field count.
    const formErrors = new Map<string, string>();

    formErrors.set("na<me>&", "Value has <script> & \"quotes\"");
    formErrors.set("url", "Must be a valid URL");

    const panel = generateChannelsPanel(undefined, undefined, undefined, undefined, formErrors);

    assert.match(panel, /<div class="error-title">Validation Errors<\/div>/, "the Validation Errors block header must be present");

    // Isolate the <ul> list so the <li> count reflects only the validation errors and not any incidental list elsewhere in the panel.
    const ulStart = panel.indexOf("Please correct the following errors:");
    const ulEnd = panel.indexOf("</ul>", ulStart);

    assert.ok((ulStart >= 0) && (ulEnd > ulStart), "the validation error list must be delimited by the intro text and its closing </ul>");

    const listBlock = panel.slice(ulStart, ulEnd);
    const itemCount = (listBlock.match(/<li>/g) ?? []).length;

    assert.equal(itemCount, formErrors.size, "exactly one <li> is rendered per formErrors entry");

    // Both the field name and the message render with entity-encoded special characters, and the raw markup must not appear anywhere in the list block.
    assert.match(listBlock, /<strong>na&lt;me&gt;&amp;<\/strong>: Value has &lt;script&gt; &amp; &quot;quotes&quot;/,
      "both the field name and the message render with entity-encoded special characters");
    assert.equal(listBlock.includes("na<me>&"), false, "the raw unescaped field name must not appear in the list block");
    assert.equal(listBlock.includes("<script>"), false, "the raw unescaped message markup must not appear in the list block");
  });
});

describe("generateChannelsPanel - channels file parse error block", () => {

  test("a malformed channels.json surfaces a Channels File Error block with the escaped path and escaped parse-error message", async () => {

    /* When the channels loader records a parse error (hasChannelsParseError() true), generateChannelsPanel renders a "Channels File Error" block that reports
     * the user-channels file path inside a <code> element and, when present, the JSON.parse error message inside a second <code> element - both through
     * escapeHtml. We write malformed JSON to channels.json BEFORE initializePersistence so the loader records the error, and we embed <, >, and & in the content
     * so the JSON.parse message (which quotes the offending input) genuinely carries markup the escape must neutralize.
     */
    await using ctx = await createIntegrationContext();

    // Write malformed content directly (bypassing the JSON-only writePersistedJson helper) so the loader hits a genuine parse failure. The embedded markup forces
    // the parse-error message to contain <, >, and & - see the escaping assertions below.
    await writeFile(pathInDataDir(ctx, "channels.json"), "{ \"channels\": <>& }", "utf8");

    await initializePersistence(ctx);

    const panel = generateChannelsPanel();

    assert.match(panel, /<div class="error-title">Channels File Error<\/div>/, "the Channels File Error block header must be present");

    // The user-channels file path renders inside a <code> element, HTML-escaped. escapeHtml is identity for a plain temp path, but comparing against it asserts the
    // escaping as the contract - a path containing markup would still be neutralized.
    const expectedPath = "<code>" + escapeHtml(getUserChannelsFilePath()) + "</code>";

    assert.ok(panel.includes(expectedPath), "the escaped channels file path must render inside a <code> element");

    // The parse-error message renders inside a <code> element, HTML-escaped. Our crafted malformed input forces the JSON.parse message to contain <, >, and &,
    // so the escape is genuinely exercised: the entity-encoded form appears and the raw (markup-bearing) message does not.
    const parseMessage = getChannelsParseErrorMessage();

    assert.ok(parseMessage, "the parse-error message must be recorded for a malformed channels file");
    assert.ok((parseMessage.includes("<") || parseMessage.includes("&")),
      "the crafted malformed input must yield a parse message containing markup so escaping is actually exercised");
    assert.ok(panel.includes("<code>" + escapeHtml(parseMessage) + "</code>"), "the escaped parse-error message must render inside a <code> element");
    assert.equal(panel.includes("<code>" + parseMessage + "</code>"), false, "the raw unescaped parse-error message must not render");
  });
});

describe("generateChannelsPanel - custom (user-defined) profile groups", () => {

  test("seeded custom profiles surface a Custom optgroup and a Custom Profiles reference category, with the no-description fallback", async () => {

    /* generateProfileDropdown and generateProfileReference are internal to table.ts (not exported), so we exercise their custom-group behavior through the
     * exported generateChannelsPanel, which composes both. getProfiles() tags a user profile that declares no category as "custom", and categorizeProfiles routes
     * it into the custom group that the dropdown renders as an <optgroup label="Custom"> and the reference renders as a "Custom Profiles" category. One seeded
     * profile carries a description; the other omits it so the "No description provided." fallback in the reference listing is exercised.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["myDescribedProfile"] = { description: "Handles a bespoke embedded player." };
      data.profiles["myBareProfile"] = {};
    });

    const panel = generateChannelsPanel();

    // The dropdown emits a Custom optgroup listing both user profiles as options.
    assert.match(panel, /<optgroup label="Custom">/, "a Custom optgroup must appear in the profile dropdown when user profiles exist");
    assert.match(panel, /<option value="myDescribedProfile"/, "the described custom profile must appear as a dropdown option");
    assert.match(panel, /<option value="myBareProfile"/, "the bare custom profile must appear as a dropdown option");

    // The profile reference emits a Custom Profiles category with a <dt>/<dd> per profile, on adjacent joined lines.
    assert.match(panel, /<h4>Custom Profiles<\/h4>/, "a Custom Profiles category must appear in the profile reference");
    assert.match(panel, /<dt>myDescribedProfile<\/dt>\n<dd>Handles a bespoke embedded player\.<\/dd>/,
      "the described custom profile lists its description in the reference");
    assert.match(panel, /<dt>myBareProfile<\/dt>\n<dd>No description provided\.<\/dd>/,
      "the bare custom profile lists the No description provided. fallback in the reference");
  });
});

describe("generateChannelsPanel - prose elements render as single lines", () => {

  test("no paragraph or definition in the panel carries a newline inside its own text", async () => {

    /* The panel is assembled as an array of lines joined with "\n", so a sentence pushed as two array elements renders with a newline through its middle. A
     * browser collapses that to a space when it lays the paragraph out, but the text a copy, a search, or a screen reader reads is broken across it, and the
     * same string placed in a title or aria-label attribute would show the break directly. Every prose element is therefore pushed as one element, however
     * many source lines its string is concatenated across.
     *
     * The check runs over the whole panel rather than the profile reference alone, so a paragraph split anywhere in the generator is caught, and it reads the
     * rendered output rather than the source, which is the only place the join actually happens.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const panel = generateChannelsPanel();
    const elements = Array.from(panel.matchAll(/<(p|dd)\b[^>]*>[\s\S]*?<\/\1>/g), (match) => match[0]);

    // Sanity: a pattern that matched nothing would make the filter below vacuously empty.
    assert.ok(elements.length > 10, "the panel should render many paragraphs and definitions (sanity check); got " + String(elements.length));

    const broken = elements.filter((element) => element.includes("\n")).map((element) => element.slice(0, 80));

    assert.deepEqual(broken, [], "no <p> or <dd> may carry a newline inside its text; offenders: " + JSON.stringify(broken));
  });
});
