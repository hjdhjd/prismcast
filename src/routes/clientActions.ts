/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clientActions.ts: Typed identifiers for the project-wide client-side action dispatcher.
 */

/* This module is the single source of truth for client-side action names. The values are the kebab-case identifiers the dispatcher in routes/root/scripts/shared.ts
 * routes events against (data-<event>-action="name", window.registerAction("name", handler)); the keys are camelCase identifiers callers use to reference them from
 * TypeScript. Server-side renderers (the routes/* tree) and the script-blob registration sites both flow through ACTIONS.<name>, so:
 *
 *   - A typo at any callsite is a compile error - the property access on ACTIONS fails type-check.
 *   - Renaming an action is one edit in this file; both the HTML emission and the JS registration update through their shared reference.
 *   - Deleting an action breaks every callsite that consumes it - removing the key cascades through the type system to every dangling reference.
 *
 * Coverage of the action surface is enforced by the runtime collision check (window.registerAction throws on duplicates), the runtime typo warning (the dispatcher
 * console.warns on unregistered names), and the test-time coverage assertion in routes/root/index.test.ts (every emitted data-*-action has a matching registration).
 * This file's role is the fourth layer: compile-time linkage so identifiers cannot drift in either direction during normal development.
 *
 * Conventions for adding a new action:
 *   1. Choose a kebab-case identifier that names the action's intent (verb-object), not the function it calls. Examples: "save-profile", "open-tag-manager".
 *   2. Add a camelCase key here whose string value is that identifier. Keep the list alphabetical by key.
 *   3. Reference ACTIONS.<key> in the renderer's data-<event>-action emission AND in the corresponding window.registerAction call. Never re-type the string.
 */
export const ACTIONS = {

  applyTagColumnFilter: "apply-tag-column-filter",
  authDone: "auth-done",
  authSkip: "auth-skip",
  authStart: "auth-start",
  autoNumberChannels: "auto-number-channels",
  bulkAssignService: "bulk-assign-service",
  bulkToggleHdhr: "bulk-toggle-hdhr",
  bulkTogglePredefined: "bulk-toggle-predefined",
  bulkToggleTag: "bulk-toggle-tag",
  cancelPendingRestart: "cancel-pending-restart",
  channelTableSort: "channel-table-sort",
  checkForUpdates: "check-for-updates",
  checkSelectors: "check-selectors",
  closeChangelogModal: "close-changelog-modal",
  closeExportModal: "close-export-modal",
  closeImportModal: "close-import-modal",
  closeTagManager: "close-tag-manager",
  copyOverviewPlaylistUrl: "copy-overview-playlist-url",
  copyPlaylistHintUrl: "copy-playlist-hint-url",
  copyStreamUrl: "copy-stream-url",
  createTag: "create-tag",
  createTagOnEnter: "create-tag-on-enter",
  deleteChannel: "delete-channel",
  deleteTag: "delete-tag",
  deleteUserProfile: "delete-user-profile",
  editUserProfile: "edit-user-profile",
  endLogin: "end-login",
  endProfileTest: "end-profile-test",
  executeExport: "execute-export",
  executeImport: "execute-import",
  exportChannels: "export-channels",
  exportConfig: "export-config",
  finishSetup: "finish-setup",
  forceRestart: "force-restart",
  hideAddChannelForm: "hide-add-channel-form",
  hideEditForm: "hide-edit-form",
  importChannels: "import-channels",
  importConfig: "import-config",
  importM3u: "import-m3u",
  logLevelChange: "log-level-change",
  openBrowseModal: "open-browse-modal",
  openChangelogModal: "open-changelog-modal",
  openSetupWizard: "open-setup-wizard",
  openTagManager: "open-tag-manager",
  openWizard: "open-wizard",
  reloadLogs: "reload-logs",
  removeServiceChip: "remove-service-chip",
  resetAllToDefaults: "reset-all-to-defaults",
  resetChannelField: "reset-channel-field",
  resetSetting: "reset-setting",
  resetTabToDefaults: "reset-tab-to-defaults",
  restoreTag: "restore-tag",
  revertChannel: "revert-channel",
  saveProfile: "save-profile",
  selectServicePill: "select-service-pill",
  showAddChannelForm: "show-add-channel-form",
  showEditForm: "show-edit-form",
  showPlaylistHint: "show-playlist-hint",
  skipSetup: "skip-setup",
  startChannelLogin: "start-channel-login",
  startInlineEdit: "start-inline-edit",
  startServiceExport: "start-service-export",
  startServiceImport: "start-service-import",
  startTagRename: "start-tag-rename",
  startUpgrade: "start-upgrade",
  submitBrowseChannels: "submit-browse-channels",
  submitChannelFormAdd: "submit-channel-form-add",
  submitChannelFormEdit: "submit-channel-form-edit",
  submitSettingsForm: "submit-settings-form",
  toggleAdvancedFields: "toggle-advanced-fields",
  toggleColumn: "toggle-column",
  toggleDisabledVisibility: "toggle-disabled-visibility",
  toggleDropdown: "toggle-dropdown",
  toggleExportAll: "toggle-export-all",
  toggleHdhr: "toggle-hdhr",
  toggleInlineTagDropdown: "toggle-inline-tag-dropdown",
  togglePredefinedChannel: "toggle-predefined-channel",
  toggleProfileReference: "toggle-profile-reference",
  toggleSection: "toggle-section",
  toggleServiceTag: "toggle-service-tag",
  toggleStreamDetails: "toggle-stream-details",
  toggleStreamPopover: "toggle-stream-popover",
  toggleTagColumnFilter: "toggle-tag-column-filter",
  triggerChannelsImport: "trigger-channels-import",
  triggerM3uImport: "trigger-m3u-import",
  triggerSettingsImport: "trigger-settings-import",
  updateCheckboxList: "update-checkbox-list",
  updateServiceSelection: "update-service-selection",
  updateTagsHidden: "update-tags-hidden"
} as const;

/**
 * Compile-time union of every valid action name. Derived from ACTIONS so it cannot drift. Parameters of action-accepting helpers (e.g., WizardModalButton.action,
 * future renderer helpers) are typed against this union, so callers can pass either ACTIONS.<name> directly or a string literal that the TypeScript compiler can
 * narrow to this union - either way, an invalid name is a type error.
 */
export type ActionName = (typeof ACTIONS)[keyof typeof ACTIONS];
