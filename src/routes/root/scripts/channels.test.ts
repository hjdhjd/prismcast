/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channels.test.ts: Unit tests for the channels subtab client-side script generator. The module exports a single function that returns an HTML <script> block
 * exposing window.* handlers for the profile wizard, browse modal, setup wizard, service pack import/export, and tag manager. We test the generated string for
 * structural invariants - presence of expected window handlers, wizard initialization, and subtab wiring - without executing the script in any DOM runtime.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../../../testing.helpers.ts";
import { generateChannelsSubtabScript } from "./channels.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("generateChannelsSubtabScript", () => {

  test("returns a non-empty <script> wrapped in an IIFE", () => {

    const script = generateChannelsSubtabScript();

    assert.equal(typeof script, "string");
    assert.ok(script.length > 5000, "script should be substantial; got " + String(script.length));
    assert.match(script, /^<script>/);
    assert.match(script, /<\/script>$/);
    assert.match(script, /\(function\(\)\s*\{/);
    assert.match(script, /\}\)\(\);/);
  });

  test("does not produce template-literal artifacts", () => {

    const script = generateChannelsSubtabScript();

    assert.doesNotMatch(script, /\$\{undefined\}/);
    assert.doesNotMatch(script, /\[object Object\]/);
  });

  test("wires the channels subtab switcher with createSubtabSwitcher", () => {

    // The channels subtab uses the createSubtabSwitcher factory from shared.ts. The subtab key is 'prismcast-channels-subtab' and the panels live under the
    // 'channels-subtab-' prefix.
    const script = generateChannelsSubtabScript();

    assert.match(script, /createSubtabSwitcher\(\{/);
    assert.match(script, /storageKey:\s*['"]prismcast-channels-subtab['"]/);
    assert.match(script, /window\.switchChannelsSubtab\s*=/);
  });

  test("exposes the profile management handlers (delete, edit, save, openWizard)", () => {

    // The Custom Profiles subtab is driven by these four handlers. deleteUserProfile and editUserProfile work against /config/profiles. saveProfile is the
    // wizard's final-step submit handler. openWizard opens the modal in create mode.
    const script = generateChannelsSubtabScript();

    assert.match(script, /window\.deleteUserProfile\s*=/);
    assert.match(script, /window\.editUserProfile\s*=/);
    assert.match(script, /window\.saveProfile\s*=/);
    assert.match(script, /window\.openWizard\s*=/);
  });

  test("exposes the service pack import/export modal handlers", () => {

    // Service packs round-trip through these four handlers. startServiceImport opens the file picker, executeImport sends the payload, and the export pair
    // mirrors the same flow for download.
    const script = generateChannelsSubtabScript();

    assert.match(script, /window\.startServiceImport\s*=/);
    assert.match(script, /window\.closeImportModal\s*=/);
    assert.match(script, /window\.executeImport\s*=/);
    assert.match(script, /window\.startServiceExport\s*=/);
    assert.match(script, /window\.closeExportModal\s*=/);
    assert.match(script, /window\.executeExport\s*=/);
  });

  test("exposes the profile test flow handlers", () => {

    // Profile testing opens a Chrome window and lets the user verify selectors. startProfileTest opens the test session, checkSelectors runs the validators,
    // endProfileTest closes the test session.
    const script = generateChannelsSubtabScript();

    assert.match(script, /window\.startProfileTest\s*=/);
    assert.match(script, /window\.checkSelectors\s*=/);
    assert.match(script, /window\.endProfileTest\s*=/);
  });

  test("exposes the Browse Channels modal handlers", () => {

    // openBrowseModal opens the modal, submitBrowseChannels submits the chosen channels for batch update.
    const script = generateChannelsSubtabScript();

    assert.match(script, /window\.openBrowseModal\s*=/);
    assert.match(script, /window\.submitBrowseChannels\s*=/);
  });

  test("exposes the Setup Wizard handlers (openSetupWizard, skipSetup, finishSetup)", () => {

    // The Setup Wizard auto-opens on first visit when setupCompleted=false. The three handlers cover open + skip + finish paths.
    const script = generateChannelsSubtabScript();

    assert.match(script, /window\.openSetupWizard\s*=/);
    assert.match(script, /window\.skipSetup\s*=/);
    assert.match(script, /window\.finishSetup\s*=/);
  });

  test("exposes the tag manager modal handlers (open, close, create, delete, restore, rename)", () => {

    // The tag manager modal lives entirely in this script. createTag, deleteTag, restoreTag, and startTagRename are the four mutation paths.
    const script = generateChannelsSubtabScript();

    assert.match(script, /window\.openTagManager\s*=/);
    assert.match(script, /window\.closeTagManager\s*=/);
    assert.match(script, /window\.createTag\s*=/);
    assert.match(script, /window\.deleteTag\s*=/);
    assert.match(script, /window\.restoreTag\s*=/);
    assert.match(script, /window\.startTagRename\s*=/);
  });

  test("exposes window.applyTagResponse for shared tag-mutation patches", () => {

    // applyTagResponse handles the tag mutation envelope (table patch + toast). It's invoked by both bulkToggleTag (config.ts) and the tag manager modal
    // (this file), so it must be exposed here for the cross-script call.
    const script = generateChannelsSubtabScript();

    assert.match(script, /window\.applyTagResponse\s*=/);
  });

  test("exposes the tag column filter handlers", () => {

    // applyTagColumnFilter and toggleTagColumnFilter drive the dropdown in the column header that hides rows whose tags don't match.
    const script = generateChannelsSubtabScript();

    assert.match(script, /window\.applyTagColumnFilter\s*=/);
    assert.match(script, /window\.toggleTagColumnFilter\s*=/);
  });

  test("exposes the playlist hint show + copy handlers", () => {

    // showPlaylistHint opens the dropdown next to the column header tag filter, copyPlaylistHintUrl writes the suggested playlist URL to the clipboard.
    const script = generateChannelsSubtabScript();

    assert.match(script, /window\.showPlaylistHint\s*=/);
    assert.match(script, /window\.copyPlaylistHintUrl\s*=/);
  });

  test("invokes channelTable.processLogos and processServiceDisplays on init", () => {

    // The channels subtab script ends with a call to channelTable.processLogos() and processServiceDisplays() so the initial server-rendered DOM picks up
    // logos and provider icons. Without these calls, page load would show plain-text channels rather than logos.
    const script = generateChannelsSubtabScript();

    assert.match(script, /channelTable\.processLogos\(\)/);
    assert.match(script, /processServiceDisplays\(\)/);
  });

  test("auto-opens setup wizard when data-setup-completed is false", () => {

    // The setup-modal element carries data-setup-completed='true' or 'false'. On 'false', the script invokes openSetupWizard() at the bottom of the IIFE.
    const script = generateChannelsSubtabScript();

    assert.match(script, /data-setup-completed/);
    assert.match(script, /openSetupWizard\(\)/);
  });

  test("calls initSubtab at the bottom of the IIFE for subtab persistence", () => {

    // initSubtab wires the hash > localStorage > default subtab logic. Same factory used in config.ts but with the channels-specific keys.
    const script = generateChannelsSubtabScript();

    assert.match(script, /initSubtab\(\{/);
    assert.match(script, /storageKey:\s*['"]prismcast-channels-subtab['"]/);
  });

  test("returns identical output across calls (pure derivation)", () => {

    // The script body has no time- or random-derived content; output is byte-stable.
    assert.equal(generateChannelsSubtabScript(), generateChannelsSubtabScript());
  });

  test("balances parentheses across the entire generated script", () => {

    const script = generateChannelsSubtabScript();
    const opens = (script.match(/\(/g) ?? []).length;
    const closes = (script.match(/\)/g) ?? []).length;

    assert.equal(opens, closes, "paren balance (opens=" + String(opens) + ", closes=" + String(closes) + ")");
  });
});
