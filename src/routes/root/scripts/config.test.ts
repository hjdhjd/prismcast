/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * config.test.ts: Unit tests for the configuration subtab client-side script generator. The module exports a single function that returns an HTML <script>
 * block exposing a large surface of window.* handlers for settings forms, channel CRUD, bulk operations, login flows, tag management, and import/export. We
 * test the generated string for structural properties - presence of expected window handlers, preset interpolation, and form lifecycle hooks.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../../../testing.helpers.ts";
import { generateConfigSubtabScript } from "./config.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("generateConfigSubtabScript", () => {

  test("returns a non-empty <script> wrapped in an IIFE", () => {

    const script = generateConfigSubtabScript();

    assert.equal(typeof script, "string");
    assert.ok(script.length > 5000, "script should be substantial; got " + String(script.length));
    assert.match(script, /^<script>/);
    assert.match(script, /<\/script>$/);
    assert.match(script, /\(function\(\)\s*\{/);
    assert.match(script, /\}\)\(\);/);
  });

  test("does not produce template-literal artifacts", () => {

    const script = generateConfigSubtabScript();

    assert.doesNotMatch(script, /\$\{undefined\}/);
    assert.doesNotMatch(script, /\[object Object\]/);
  });

  test("names the Save button through the one label constant, emitted once", () => {

    // Every message that names the Save button reads the label the page renders, so the script carries the constant once, through JSON.stringify, and no
    // mode-dependent copy of a label.
    const script = generateConfigSubtabScript();

    assert.doesNotMatch(script, /Save & Restart/);
    assert.equal(script.split("const SAVE_SETTINGS_LABEL = \"Save Settings\";").length - 1, 1, "the label constant is emitted exactly once");
  });

  test("interpolates VIDEO_QUALITY_PRESETS into the presetValues map", () => {

    // The generator iterates VIDEO_QUALITY_PRESETS and emits one entry per preset id. Each entry has streaming-videoBitsPerSecond and streaming-frameRate keys.
    // The presence of the map and at least one preset entry confirms the iteration ran.
    const script = generateConfigSubtabScript();

    assert.match(script, /const presetValues\s*=\s*\{/);
    assert.match(script, /'streaming-videoBitsPerSecond':/);
    assert.match(script, /'streaming-frameRate':/);
  });

  test("declares the onPresetChange auto-fill helper", () => {

    // onPresetChange is wired to the preset dropdown's onchange. When the user picks a preset, bitrate and frame rate inputs auto-fill from presetValues.
    const script = generateConfigSubtabScript();

    assert.match(script, /function onPresetChange\(presetId\)/);
  });

  test("exposes the changelog modal lifecycle on window", () => {

    // openChangelogModal/closeChangelogModal are invoked from the version display and the modal's close button respectively.
    const script = generateConfigSubtabScript();

    assert.match(script, /window\.openChangelogModal\s*=/);
    assert.match(script, /window\.closeChangelogModal\s*=/);
  });

  test("exposes the version-check and upgrade flows on window", () => {

    // checkForUpdates is the manual version-check handler. startUpgrade is wired to the changelog modal upgrade button. waitForServerRestart drives the polling
    // loop after either a settings save (with restart) or an upgrade.
    const script = generateConfigSubtabScript();

    assert.match(script, /window\.checkForUpdates\s*=/);
    assert.match(script, /window\.startUpgrade\s*=/);
    assert.match(script, /function waitForServerRestart\(/);
  });

  test("renders the upgrade outcome the server reported rather than a fixed sentence", () => {

    // POST /upgrade owns the wording, and its success outcomes read differently: an upgrade handed to a detached helper is still running when the response
    // arrives and names the helper log the user can follow, while an upgrade that ran here is done. The toast reads the message, appends the log path when one
    // came back, and shows the still-running case as a longer-lived advisory, so a change to the route's wording reaches the user with no client edit.
    const script = generateConfigSubtabScript();

    assert.match(script, /let msg = result\.message \|\| 'Upgrade complete\./);
    assert.match(script, /msg \+= ' Helper log: ' \+ result\.logPath;/);
    assert.match(script, /showToast\(msg, result\.logPath \? 'info' : 'success', result\.logPath \? 12000 : 8000\);/);
  });

  test("exposes the restart dialog and force-restart controls on window", () => {

    // showPendingRestartDialog opens the modal, updateRestartDialogStatus is called by the streamRemoved SSE handler, cancelPendingRestart closes the modal,
    // and forceRestart triggers an immediate restart regardless of active streams.
    const script = generateConfigSubtabScript();

    assert.match(script, /window\.updateRestartDialogStatus\s*=/);
    assert.match(script, /window\.cancelPendingRestart\s*=/);
    assert.match(script, /window\.forceRestart\s*=/);
  });

  test("exposes the settings form submit and reset paths", () => {

    // submitSettingsForm is the handler the action dispatcher calls for the settings form's data-submit-action. resetSetting/resetTabToDefaults/resetAllToDefaults
    // reset values client-side without persisting until the user clicks Save.
    const script = generateConfigSubtabScript();

    assert.match(script, /window\.submitSettingsForm\s*=/);
    assert.match(script, /window\.resetSetting\s*=/);
    assert.match(script, /window\.resetTabToDefaults\s*=/);
    assert.match(script, /window\.resetAllToDefaults\s*=/);
  });

  test("exposes the export and import handlers for settings, channels, and M3U", () => {

    // The Backup subtab uses the JSON export/import pairs (exportConfig/importConfig, exportChannels/importChannels). importM3U has no matching export
    // counterpart and is triggered from the import dropdown in the Channels tab toolbar.
    const script = generateConfigSubtabScript();

    assert.match(script, /window\.exportConfig\s*=/);
    assert.match(script, /window\.importConfig\s*=/);
    assert.match(script, /window\.exportChannels\s*=/);
    assert.match(script, /window\.importChannels\s*=/);
    assert.match(script, /window\.importM3U\s*=/);
  });

  test("exposes channel CRUD handlers (submit, delete, predefined toggle, revert)", () => {

    // submitChannelForm handles create + replace. deleteChannel removes user channels. togglePredefinedChannel enables/disables the predefined version, and
    // revertChannel removes a predefined override.
    const script = generateConfigSubtabScript();

    assert.match(script, /window\.submitChannelForm\s*=/);
    assert.match(script, /window\.deleteChannel\s*=/);
    assert.match(script, /window\.togglePredefinedChannel\s*=/);
    assert.match(script, /window\.revertChannel\s*=/);
  });

  test("exposes the bulk action handlers", () => {

    // The bulk action set includes auto-numbering, hdhr toggling, tag toggling, predefined-by-scope, and service bulk assignment.
    const script = generateConfigSubtabScript();

    assert.match(script, /window\.autoNumberChannels\s*=/);
    assert.match(script, /window\.bulkToggleHdhr\s*=/);
    assert.match(script, /window\.bulkToggleTag\s*=/);
    assert.match(script, /window\.bulkTogglePredefined\s*=/);
    assert.match(script, /window\.bulkAssignService\s*=/);
  });

  test("exposes the inline edit and inline tag dropdown handlers", () => {

    // startInlineEdit drives table-cell editing, and toggleInlineTagDropdown opens the inline tag editor portal, which saves its tags by PATCH when it closes.
    // updateTagsHidden copies the channel edit form's tag checkboxes into that form's hidden tags input. Tag-manager modal handlers
    // (createTag/deleteTag/restoreTag/startTagRename) live in channels.ts, not here.
    const script = generateConfigSubtabScript();

    assert.match(script, /window\.startInlineEdit\s*=/);
    assert.match(script, /window\.toggleInlineTagDropdown\s*=/);
    assert.match(script, /window\.updateTagsHidden\s*=/);
  });

  test("exposes the channel login flow on window", () => {

    // startChannelLogin opens login mode for a channel key, endLogin closes login mode and the modal.
    const script = generateConfigSubtabScript();

    assert.match(script, /window\.startChannelLogin\s*=/);
    assert.match(script, /window\.endLogin\s*=/);
  });

  test("exposes the column-toggle and visibility handlers for the channel table", () => {

    // toggleColumn drives the column picker checkboxes. toggleDisabledVisibility hides/shows disabled rows.
    const script = generateConfigSubtabScript();

    assert.match(script, /window\.toggleColumn\s*=/);
    assert.match(script, /window\.toggleDisabledVisibility\s*=/);
  });

  test("declares the PLAYLIST_HINT constant for change-success toasts", () => {

    // PLAYLIST_HINT is appended to success toasts when a channel mutation changes the M3U playlist content. The constant is the SSOT for the hint text.
    const script = generateConfigSubtabScript();

    assert.match(script, /const PLAYLIST_HINT\s*=/);
    assert.match(script, /Reload the playlist in Channels DVR/);
  });

  test("calls initSubtab at the bottom of the IIFE for subtab persistence", () => {

    // initSubtab is called near the end of the setup phase, immediately before the action-registration block, to wire the hash > localStorage > default subtab logic.
    // The presence of this call indicates the lifecycle is set up.
    const script = generateConfigSubtabScript();

    assert.match(script, /initSubtab\(\{/);
    assert.match(script, /storageKey:\s*['"]prismcast-config-subtab['"]/);
  });

  test("returns identical output across calls (pure derivation)", () => {

    // The generator depends on VIDEO_QUALITY_PRESETS and the settings vocabulary's constants. Each is fixed for a given runtime, so repeated calls produce
    // byte-identical output.
    assert.equal(generateConfigSubtabScript(), generateConfigSubtabScript());
  });

  test("balances parentheses across the entire generated script", () => {

    const script = generateConfigSubtabScript();
    const opens = (script.match(/\(/g) ?? []).length;
    const closes = (script.match(/\)/g) ?? []).length;

    assert.equal(opens, closes, "paren balance (opens=" + String(opens) + ", closes=" + String(closes) + ")");
  });
});
