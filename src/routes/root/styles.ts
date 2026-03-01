/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * styles.ts: Landing page CSS generator for PrismCast.
 */
import { OPTIONAL_COLUMNS } from "../config/index.js";

/**
 * Generates additional CSS styles specific to the landing page. Uses CSS custom properties for theme support.
 * @returns CSS styles as a string.
 */
export function generateLandingPageStyles(): string {

  return [

    // Override header to use space-between for logo/title on left and status on right.
    ".header { justify-content: space-between; }",
    ".header-left { display: flex; align-items: center; gap: 20px; }",

    // Header links (GitHub, More by HJD).
    ".header-links { display: flex; align-items: center; gap: 8px; font-size: 13px; }",
    ".header-links a { color: var(--text-muted); text-decoration: none; transition: color 0.2s; }",
    ".header-links a:hover { color: var(--text-primary); }",
    ".header-links-sep { color: var(--text-muted); }",

    // Header status bar styles.
    ".header-status { display: flex; gap: 20px; align-items: center; font-size: 13px; color: var(--text-secondary); }",
    ".header-status span { white-space: nowrap; }",

    // Stream count popover. Clickable when streams are active; popover drops from the right edge of the header.
    "#stream-count { background: none; border: none; color: inherit; font: inherit; padding: 0; }",
    "#stream-count.clickable { cursor: pointer; }",
    "#stream-count.clickable:hover { color: var(--text-primary); }",
    ".stream-popover .dropdown-menu { right: 0; left: auto; min-width: 220px; }",
    ".stream-popover-row { display: flex; align-items: center; gap: 8px; padding: 6px 12px; font-size: 13px; white-space: nowrap; }",
    ".stream-popover-logo { height: 18px; width: auto; max-width: 80px; vertical-align: middle; }",
    ".stream-popover-channel { color: var(--text-primary); }",
    ".stream-popover-show { color: var(--text-muted); }",
    ".stream-popover-duration { color: var(--text-muted); margin-left: auto; }",

    // Subtab styles for Configuration tab.
    ".subtab-bar { display: flex; border-bottom: 1px solid var(--border-default); margin-bottom: 20px; gap: 2px; flex-wrap: wrap; }",
    ".subtab-btn { padding: 8px 16px; border: none; background: var(--subtab-bg); cursor: pointer; font-size: 13px; font-weight: 500; ",
    "color: var(--tab-text); border-radius: var(--radius-md) var(--radius-md) 0 0; transition: all 0.2s; }",
    ".subtab-btn:hover { background: var(--subtab-bg-hover); color: var(--tab-text-hover); }",
    ".subtab-btn.active { background: var(--subtab-bg-active); color: var(--subtab-text-active); border-bottom: 2px solid var(--subtab-border-active); }",
    ".subtab-panel { display: none; }",
    ".subtab-panel.active { display: block; }",

    // Channels tab subtab styles. Scoped to avoid collision with Configuration tab subtabs.
    ".channels-subtab-bar { display: flex; border-bottom: 1px solid var(--border-default); margin-bottom: 20px; gap: 2px; flex-wrap: wrap; }",
    ".channels-subtab-btn { padding: 8px 16px; border: none; background: var(--subtab-bg); cursor: pointer; font-size: 13px; font-weight: 500; ",
    "color: var(--tab-text); border-radius: var(--radius-md) var(--radius-md) 0 0; transition: all 0.2s; }",
    ".channels-subtab-btn:hover { background: var(--subtab-bg-hover); color: var(--tab-text-hover); }",
    ".channels-subtab-btn.active { background: var(--subtab-bg-active); color: var(--subtab-text-active); ",
    "border-bottom: 2px solid var(--subtab-border-active); }",
    ".channels-subtab-panel { display: none; }",
    ".channels-subtab-panel.active { display: block; }",

    // Empty state styling for Providers panel.
    ".empty-state { text-align: center; padding: 40px 20px; color: var(--text-secondary); }",
    ".empty-state-title { font-size: 16px; font-weight: 600; color: var(--text-primary); margin: 0 0 8px 0; }",
    ".empty-state-text { font-size: 14px; margin: 0; max-width: 500px; margin-left: auto; margin-right: auto; line-height: 1.5; }",

    // Wizard modal styles.
    ".wizard-modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 1000; ",
    "justify-content: center; align-items: center; }",
    ".wizard-modal-content { background: var(--surface-overlay); border-radius: var(--radius-lg); box-shadow: 0 8px 32px rgba(0,0,0,0.3); ",
    "width: 640px; max-width: 90vw; max-height: 85vh; display: flex; flex-direction: column; overflow: hidden; }",
    ".wizard-header { display: flex; justify-content: space-between; align-items: center; padding: 16px 20px; ",
    "border-bottom: 1px solid var(--border-default); }",
    ".wizard-header h3 { margin: 0; font-size: 18px; color: var(--text-heading); }",
    ".wizard-close { background: none; border: none; font-size: 20px; cursor: pointer; color: var(--text-muted); padding: 4px 8px; }",
    ".wizard-close:hover { color: var(--text-primary); }",

    // Step indicator.
    ".wizard-steps { display: flex; align-items: center; justify-content: center; padding: 16px 20px; gap: 4px; }",
    ".wizard-step { display: flex; flex-direction: column; align-items: center; gap: 4px; }",
    ".wizard-step-line { width: 32px; height: 2px; background: var(--border-default); margin-bottom: 18px; }",
    ".step-circle { width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; ",
    "font-size: 13px; font-weight: 600; border: 2px solid var(--border-default); color: var(--text-muted); background: var(--surface-overlay); }",
    ".wizard-step.active .step-circle { border-color: var(--interactive-primary); color: var(--interactive-primary); background: var(--surface-overlay); }",
    ".wizard-step.completed .step-circle { border-color: var(--interactive-primary); color: #fff; background: var(--interactive-primary); }",
    ".step-label { font-size: 11px; color: var(--text-muted); }",
    ".wizard-step.active .step-label { color: var(--interactive-primary); font-weight: 600; }",
    ".wizard-step.clickable { cursor: pointer; }",
    ".wizard-step.clickable:not(.completed) .step-circle:hover { border-color: var(--interactive-primary); color: var(--interactive-primary); }",

    // Wizard content area.
    ".wizard-content { padding: 20px; overflow-y: auto; flex: 1; min-height: 300px; }",
    ".wizard-content label { display: block; margin-bottom: 6px; font-weight: 500; color: var(--text-primary); font-size: 14px; }",
    ".wizard-content input[type=\"text\"], .wizard-content textarea { width: 100%; padding: 8px 10px; border: 1px solid var(--border-default); ",
    "border-radius: var(--radius-md); font-size: 14px; font-family: inherit; background: var(--surface-overlay); color: var(--text-primary); ",
    "box-sizing: border-box; }",
    ".wizard-content input[type=\"text\"]:focus, .wizard-content textarea:focus { outline: none; border-color: var(--interactive-primary); }",
    ".wizard-content textarea { resize: vertical; min-height: 60px; }",
    ".wizard-content .field-hint { font-size: 12px; color: var(--text-muted); margin-top: 4px; margin-bottom: 12px; }",
    ".wizard-content .field-group { margin-bottom: 16px; }",

    // Wizard profile radio buttons.
    ".wizard-profile-category { margin-bottom: 16px; }",
    ".wizard-profile-category h4 { font-size: 13px; color: var(--text-secondary); margin: 0 0 4px 0; text-transform: uppercase; letter-spacing: 0.5px; }",
    ".wizard-category-desc { font-size: 12px; color: var(--text-muted); margin-bottom: 8px; }",
    ".wizard-content label.wizard-profile-option { display: flex; align-items: flex-start; gap: 8px; padding: 8px 10px; ",
    "border: 1px solid var(--border-default); border-radius: var(--radius-md); margin-bottom: 4px; cursor: pointer; }",
    ".wizard-profile-option:hover { background: var(--surface-hover); }",
    ".wizard-profile-option input[type=\"radio\"] { margin-top: 5px; flex-shrink: 0; }",
    ".wizard-profile-option .profile-option-name { font-weight: 500; font-size: 14px; color: var(--text-primary); }",
    ".wizard-profile-option .profile-option-summary { font-weight: 400; font-size: 12px; color: var(--text-muted); }",
    ".wizard-profile-option .profile-option-desc { font-size: 12px; color: var(--text-secondary); margin-top: 2px; }",

    // Wizard strategy radio buttons.
    ".wizard-content label.wizard-strategy-option { display: flex; align-items: flex-start; gap: 8px; padding: 10px 12px; ",
    "border: 1px solid var(--border-default); border-radius: var(--radius-md); margin-bottom: 6px; cursor: pointer; }",
    ".wizard-strategy-option:hover { background: var(--surface-hover); }",
    ".wizard-strategy-option input[type=\"radio\"] { margin-top: 5px; flex-shrink: 0; }",
    ".wizard-strategy-label { font-weight: 500; font-size: 14px; color: var(--text-primary); }",
    ".wizard-strategy-desc { font-size: 12px; color: var(--text-secondary); margin-top: 2px; }",

    // Wizard flag checkboxes.
    ".wizard-content label.wizard-flag { display: flex; align-items: flex-start; gap: 8px; padding: 8px 10px; margin-bottom: 4px; }",
    ".wizard-flag input[type=\"checkbox\"] { margin-top: 5px; flex-shrink: 0; }",
    ".wizard-flag-label { font-size: 14px; color: var(--text-primary); }",
    ".wizard-flag-desc { font-size: 12px; color: var(--text-secondary); margin-top: 2px; }",

    // Wizard review summary.
    ".wizard-review-table { width: 100%; border-collapse: collapse; font-size: 14px; }",
    ".wizard-review-table td { padding: 6px 8px; border-bottom: 1px solid var(--border-default); }",
    ".wizard-review-table td:first-child { font-weight: 500; color: var(--text-secondary); width: 120px; white-space: nowrap; }",

    // Wizard validation error.
    ".wizard-error { padding: 0 20px; color: var(--status-error-text); font-size: 13px; }",

    // Wizard buttons.
    ".wizard-buttons { display: flex; justify-content: space-between; align-items: center; padding: 16px 20px; ",
    "border-top: 1px solid var(--border-default); }",
    ".wizard-buttons-right { display: flex; gap: 8px; }",

    // Domain row for adding multiple domains.
    ".wizard-domain-row { border: 1px solid var(--border-default); border-radius: var(--radius-md); padding: 12px; margin-bottom: 8px; }",
    ".wizard-domain-row .field-group:last-child { margin-bottom: 0; }",
    ".wizard-add-domain { background: none; border: none; color: var(--interactive-primary); cursor: pointer; font-size: 13px; padding: 4px 0; }",
    ".wizard-add-domain:hover { text-decoration: underline; }",

    // Export and import modal styles: body area, profile list items, section header, and option labels.
    ".export-modal-body { padding: 20px; overflow-y: auto; font-size: 14px; }",
    ".export-section-header { margin-bottom: 12px; font-weight: 500; }",
    ".export-option-label { display: flex; align-items: center; gap: 8px; cursor: pointer; }",
    ".export-profile-item { display: flex; align-items: center; gap: 10px; padding: 8px 10px; margin-bottom: 4px; ",
    "border: 1px solid var(--border-default); border-radius: var(--radius-md); cursor: pointer; }",
    ".export-profile-item:hover { background: var(--surface-hover); }",
    ".export-profile-info { min-width: 0; }",
    ".export-profile-name { font-weight: 600; font-size: 14px; }",
    ".export-profile-meta { font-size: 12px; color: var(--text-muted); }",
    ".export-divider { border-top: 1px solid var(--border-default); margin: 16px 0; }",
    ".export-hint { font-size: 12px; color: var(--text-muted); margin-top: 6px; margin-left: 24px; }",
    ".import-summary-table { width: 100%; font-size: 13px; margin: 12px 0; }",
    ".import-summary-table td { padding: 4px 0; }",
    ".import-summary-label { color: var(--text-muted); }",

    // Panel header layout for description and reset link alignment.
    ".panel-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }",

    // Settings panel description styling (replaces redundant header titles).
    ".settings-panel-description { margin: 0; font-size: 15px; color: var(--text-primary); }",
    ".settings-panel-description p { margin: 0; }",
    ".description-hint { font-size: 13px; color: var(--text-secondary); margin-top: 4px; }",

    // Streams table container - outer border with rounded corners.
    "#streams-container { border: 1px solid var(--border-default); border-radius: var(--radius-md); overflow: hidden; margin-bottom: 20px; }",

    // Streams table - minimal design with no borders between columns.
    ".streams-table { width: 100%; border-collapse: collapse; margin: 0; }",
    ".streams-table td { padding: 6px 10px; border: none; color: var(--text-primary); vertical-align: middle; }",
    ".streams-table td:first-child { padding-left: 12px; }",
    ".streams-table td:last-child { padding-right: 12px; }",
    ".streams-table .empty-row td { padding: 10px 12px; text-align: center; color: var(--text-muted); }",
    ".streams-table .empty-row:hover { background: transparent; }",
    ".streams-table .stream-row { cursor: pointer; }",
    ".streams-table .stream-row:hover { background: var(--table-row-hover); }",
    ".streams-table .chevron { width: 20px; color: var(--text-muted); font-size: 10px; }",
    ".streams-table .stream-info { width: 180px; max-width: 180px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 500; font-size: 13px; }",
    ".streams-table .stream-duration { font-weight: 400; color: var(--text-secondary); }",
    ".streams-table .channel-logo { height: 24px; width: auto; max-width: 100px; vertical-align: middle; margin-right: 4px; }",
    ".streams-table .channel-text { vertical-align: middle; }",
    ".streams-table .stream-show { max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text-secondary); font-size: 13px; }",
    ".streams-table .stream-health { text-align: right; white-space: nowrap; font-size: 13px; }",
    ".streams-table .stream-details td { padding: 10px 12px 12px 32px; background: var(--surface-sunken); }",
    ".streams-table .details-content { font-size: 12px; color: var(--text-secondary); }",
    ".streams-table .details-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 20px; margin-bottom: 10px; }",
    ".streams-table .details-url { word-break: break-all; flex: 1; min-width: 0; }",
    ".streams-table .details-started { white-space: nowrap; flex-shrink: 0; }",
    ".streams-table .details-metrics { display: flex; align-items: baseline; gap: 20px; }",
    ".streams-table .details-issue { flex: 1; min-width: 0; }",
    ".streams-table .details-recovery { white-space: nowrap; flex-shrink: 0; }",
    ".streams-table .details-memory { white-space: nowrap; flex-shrink: 0; }",
    ".streams-table .client-count { font-size: 0.85em; color: var(--text-muted); margin-right: 8px; white-space: nowrap; }",

    // Log viewer styles.
    ".log-viewer { background: var(--dark-surface-bg); color: var(--dark-text-secondary); padding: 15px; border-radius: var(--radius-lg); ",
    "font-family: 'SF Mono', Monaco, monospace; font-size: 12px; height: 500px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; }",
    ".log-viewer::-webkit-scrollbar { width: 8px; }",
    ".log-viewer::-webkit-scrollbar-track { background: var(--dark-scrollbar-track); }",
    ".log-viewer::-webkit-scrollbar-thumb { background: var(--dark-scrollbar-thumb); border-radius: var(--radius-md); }",
    ".log-viewer::-webkit-scrollbar-thumb:hover { background: var(--dark-scrollbar-thumb-hover); }",
    ".log-entry { color: var(--dark-text-secondary); }",
    ".log-error { color: var(--dark-text-error); }",
    ".log-warn { color: var(--dark-text-warn); }",
    ".log-debug { color: var(--dark-text-debug); }",
    ".log-muted { color: var(--dark-text-muted); }",
    ".log-connecting { color: var(--dark-text-muted); }",

    // Channel table styles. The wrapper uses fit-content so it shrinks when columns are hidden, with max-width: 100% to prevent viewport overflow. We use
    // border-collapse: separate so that border-radius works on the header cells. Overflow is auto so the table scrolls on narrow screens; dropdown menus escape
    // the container via portal to <body>.
    ".channel-table-wrapper { width: fit-content; max-width: 100%; margin: 0 auto 20px; border: 1px solid var(--border-default); " +
      "border-radius: var(--radius-lg); overflow: auto; }",
    ".channel-table { width: 100%; border-collapse: separate; border-spacing: 0; table-layout: auto; min-width: 650px; margin: 0; }",
    ".channel-table th, .channel-table td { padding: 10px 12px; text-align: left; border: none; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }",
    ".channel-table th { background: var(--table-header-bg); font-weight: 600; font-size: 13px; border-bottom: 1px solid var(--border-default); }",
    ".channel-table tbody tr:nth-child(even):not(.user-channel) { background: var(--table-row-even); }",
    ".channel-table tr:hover { background: var(--table-row-hover); }",
    ".channel-table .col-key { min-width: 170px; }",
    ".channel-table .col-name { min-width: 200px; }",
    ".channel-table .col-provider { min-width: 200px; }",
    ".channel-table .col-actions, .channel-table td:last-child { min-width: 168px; white-space: nowrap; overflow: visible; }",
    ".provider-select { width: 100%; padding: 2px 4px; font-size: 12px; border: 1px solid var(--form-input-border); ",
    "border-radius: 3px; background: var(--form-input-bg); color: var(--text-primary); }",

    // Optional column width, alignment, and visibility rules. Generated from OPTIONAL_COLUMNS to keep a single source of truth for column metadata.
    ...OPTIONAL_COLUMNS.flatMap((col) => [
      ".channel-table ." + col.cssClass + " { min-width: " + col.width + (col.align === "center" ? "; text-align: center" : "") + "; }",
      ".channel-table.hide-" + col.cssClass + " ." + col.cssClass + " { display: none; }"
    ]),

    // Sortable header styles. Clickable headers with pointer cursor and subtle hover effect.
    ".channel-table th.sortable { cursor: pointer; user-select: none; }",
    ".channel-table th.sortable:hover { background: var(--surface-hover); }",

    // Column picker dropdown. The Actions header uses flex layout to push the ellipsis button to the far right.
    ".channel-table th.col-actions { display: flex; align-items: center; justify-content: space-between; }",
    ".column-picker { position: relative; }",
    ".btn-col-picker { font-size: 16px; line-height: 1; vertical-align: middle; padding: 0 4px; color: var(--text-muted); cursor: pointer; " +
      "background: none; border: none; border-radius: 3px; }",
    ".btn-col-picker:hover { color: var(--text-primary); background: var(--surface-hover); }",
    ".column-picker-menu { min-width: 140px; }",

    // Key column styling: monospace at a slightly smaller size with secondary color to reduce visual weight.
    ".ch-key { color: var(--text-secondary); font-family: var(--font-mono); font-size: 13px; }",

    // Responsive: hide Key and all optional columns on phones.
    "@media (max-width: 768px) { .channel-table .col-key, .channel-table td:nth-child(1), .channel-table th:nth-child(1) { display: none; } " +
      OPTIONAL_COLUMNS.map((col) => ".channel-table ." + col.cssClass).join(", ") + " { display: none; } }",

    // User channel row tinting to distinguish custom/override channels from predefined.
    ".channel-table tr.user-channel { background: var(--user-channel-tint); }",
    ".channel-table tr.user-channel:hover { background: var(--user-channel-tint-hover); }",

    // Disabled predefined channel row styling and hide-disabled toggle.
    ".channel-table tr.channel-disabled { opacity: 0.5; }",
    ".channel-table tr.channel-disabled td { color: var(--text-tertiary); }",
    ".channel-table.hide-disabled tr.channel-disabled { display: none; }",

    // Provider-filtered channel row styling. Uses reduced opacity and italic text to distinguish from manually disabled rows. The compound selector ensures that rows
    // which are both disabled and provider-filtered render at the disabled-level opacity (0.5) rather than the more aggressive unavailable-level opacity (0.4).
    ".channel-table tr.channel-unavailable { opacity: 0.4; font-style: italic; }",
    ".channel-table tr.channel-unavailable td { color: var(--text-tertiary); }",
    ".channel-table tr.channel-unavailable.channel-disabled { opacity: 0.5; }",
    ".channel-table.hide-disabled tr.channel-unavailable { display: none; }",
    ".no-provider-label { color: var(--text-tertiary); font-size: 12px; }",
    ".text-muted { color: var(--text-muted); }",

    // Provider filter toolbar layout.
    ".provider-toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-bottom: 10px; }",
    ".provider-toolbar .toolbar-group { display: flex; align-items: center; gap: 6px; }",
    ".provider-toolbar .toolbar-label { font-size: 13px; color: var(--text-secondary); white-space: nowrap; }",
    ".provider-toolbar .toolbar-spacer { flex: 1; }",

    // Provider dropdown multi-select.
    ".provider-dropdown-menu { min-width: 200px; max-height: 300px; overflow-y: auto; }",
    ".provider-option { display: flex; align-items: center; gap: 6px; padding: 5px 12px; font-size: 13px; cursor: pointer; color: var(--text-primary); }",
    ".provider-option:hover { background: var(--surface-sunken); }",
    ".provider-option input[type=\"checkbox\"] { margin: 0; }",

    // Provider chips.
    ".provider-chips { display: flex; flex-wrap: wrap; align-items: center; gap: 4px; }",
    ".provider-chip { display: inline-flex; align-items: center; gap: 4px; background: var(--surface-elevated); border: 1px solid var(--border-default); ",
    "border-radius: 12px; padding: 2px 8px 2px 10px; font-size: 12px; color: var(--text-secondary); min-height: 24px; }",
    ".chip-close { background: none; border: none; cursor: pointer; font-size: 14px; line-height: 1; padding: 0 2px; color: var(--text-muted); ",
    "transition: color 0.2s; }",
    ".chip-close:hover { color: var(--text-primary); }",

    // Bulk assign dropdown.
    ".bulk-assign-select { font-size: 13px; padding: 4px 8px; border: 1px solid var(--border-default); border-radius: var(--radius-md); ",
    "background: var(--surface-page); color: var(--text-primary); cursor: pointer; }",

    // Responsive: stack provider toolbar groups vertically on small screens.
    "@media (max-width: 768px) { .provider-toolbar { flex-direction: column; align-items: flex-start; } }",

    // Icon button styling for channel action buttons.
    ".btn-icon { display: inline-flex; align-items: center; justify-content: center; width: 28px; height: 28px; padding: 0; border: none; ",
    "border-radius: var(--radius-md); background: transparent; cursor: pointer; color: var(--text-secondary); transition: color 0.15s, background 0.15s; }",
    ".btn-icon:hover { background: var(--surface-hover); }",
    ".user-channel .btn-icon:hover { background: var(--user-channel-tint-hover); }",
    ".btn-icon-edit:hover { color: var(--interactive-edit); }",
    ".btn-icon-delete:hover { color: var(--interactive-delete); }",
    ".btn-icon-revert:hover { color: var(--interactive-edit); }",
    ".btn-icon-enable:hover { color: var(--interactive-success); }",
    ".btn-icon-disable:hover { color: var(--interactive-delete); }",
    ".btn-icon-login:hover { color: var(--interactive-primary); }",
    ".btn-icon-copy:hover { color: var(--interactive-primary); }",
    ".btn-icon-placeholder { display: inline-block; width: 28px; height: 28px; }",

    // Health indicator colors. The login icon only uses health-success (provider verified). The health icon uses both (channel last-tune result).
    ".health-success { color: var(--interactive-success); }",
    ".health-failed { color: var(--interactive-delete); }",

    // Health icon is non-interactive — no hover effect, default cursor. The .user-channel override needs matching specificity (0-3-0) to prevent the
    // tinted hover background from making the health icon look clickable on user-defined channel rows.
    ".btn-icon-health { cursor: default; }",
    ".btn-icon-health:hover { background: transparent; color: var(--text-secondary); }",
    ".user-channel .btn-icon-health:hover { background: transparent; }",

    // Override hover to preserve health color when set.
    ".btn-icon-health.health-success:hover { color: var(--interactive-success); }",
    ".btn-icon-health.health-failed:hover { color: var(--interactive-delete); }",
    ".copy-dropdown .dropdown-item { font-size: 12px; }",

    // JS tooltip styling. The tooltip element is appended to <body> and positioned via getBoundingClientRect() so it's immune to overflow and stacking contexts.
    // Only activated when the primary input can't hover (hover: none), targeting iPadOS where Safari doesn't show native title tooltips. On pure-touch
    // devices without a trackpad, the JS loads but mouseenter never fires so the tooltip stays hidden. Desktop (hover: hover) skips initialization entirely.
    ".btn-icon-tooltip { position: fixed; padding: 4px 8px; border-radius: var(--radius-sm); background: var(--surface-overlay); color: var(--text-primary); ",
    "font-size: 12px; white-space: nowrap; pointer-events: none; opacity: 0; transition: opacity 0.5s; z-index: 10000; ",
    "box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15); }",
    ".btn-icon-tooltip.visible { opacity: 1; transition: opacity 0.1s; }",

    // Channel toolbar with operation buttons and display controls.
    ".channel-toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-top: 10px; margin-bottom: 15px; }",
    ".channel-toolbar .toolbar-group { display: flex; align-items: center; gap: 6px; }",
    ".channel-toolbar .toolbar-spacer { flex: 1; }",
    ".channel-toolbar .toggle-label { font-size: 12px; color: var(--text-secondary); cursor: pointer; display: flex; align-items: center; gap: 4px; ",
    "user-select: none; }",

    // Dropdown menu used by the Import button in the channel toolbar.
    ".dropdown { position: relative; display: inline-block; }",
    ".dropdown-menu { display: none; position: absolute; top: 100%; left: 0; z-index: 1000; min-width: 180px; padding: 4px 0; margin-top: 2px; ",
    "background: var(--surface-overlay); border: 1px solid var(--border-default); border-radius: var(--radius-md); ",
    "box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15); }",
    ".dropdown-menu.show { display: block; }",
    ".dropdown-item { padding: 6px 12px; font-size: 13px; cursor: pointer; color: var(--text-primary); }",
    ".dropdown-item:hover { background: var(--surface-sunken); }",
    ".dropdown-option { display: block; padding: 2px 12px 6px 24px; font-size: 12px; color: var(--text-secondary); cursor: pointer; user-select: none; }",
    ".dropdown-divider { height: 1px; margin: 4px 0; background: var(--border-default); }",

    // Channel form styles. Inputs use full width; selects use width classes from ui.ts for consistency with settings forms.
    ".channel-form { background: var(--form-bg); border: 1px solid var(--border-default); border-radius: var(--radius-lg); padding: 20px; margin-bottom: 20px; }",
    ".channel-form h3 { margin-top: 0; margin-bottom: 15px; color: var(--text-heading-secondary); }",
    ".channel-form .form-row { margin-bottom: 4px; }",
    ".channel-form .form-row:last-child { margin-bottom: 0; }",
    ".channel-form .form-input { width: 100%; box-sizing: border-box; }",

    // Advanced toggle styles.
    ".advanced-toggle { color: var(--interactive-primary); cursor: pointer; font-size: 13px; margin-top: 5px; margin-bottom: 15px; }",
    ".advanced-toggle:hover { text-decoration: underline; }",
    ".advanced-fields { display: none; }",
    ".advanced-fields.show { display: block; }",

    // Profile reference section styles.
    ".profile-reference { background: var(--surface-elevated); border: 1px solid var(--border-default); border-radius: var(--radius-lg); margin: 20px 0; ",
    "padding: 20px; }",
    ".profile-reference-header { display: flex; justify-content: space-between; align-items: flex-start; }",
    ".profile-reference h3 { margin: 0 0 10px 0; color: var(--text-heading-secondary); }",
    ".profile-reference-close { color: var(--text-secondary); font-size: 18px; background: none; border: none; cursor: pointer; padding: 0 5px; }",
    ".profile-reference-close:hover { color: var(--text-primary); }",
    ".reference-intro { color: var(--text-secondary); font-size: 13px; margin-bottom: 20px; }",
    ".profile-category { margin-bottom: 20px; }",
    ".profile-category:last-child { margin-bottom: 0; }",
    ".profile-category h4 { color: var(--text-heading-secondary); font-size: 14px; font-weight: 600; margin: 0 0 8px 0; }",
    ".category-desc { color: var(--text-tertiary); font-size: 12px; margin: 0 0 10px 0; }",
    ".profile-list { margin: 0; padding: 0; }",
    ".profile-list dt { font-family: var(--font-mono); font-size: 13px; font-weight: 600; margin-top: 10px; color: var(--text-primary); }",
    ".profile-list dt:first-child { margin-top: 0; }",
    ".profile-list dd { color: var(--text-secondary); font-size: 13px; margin: 4px 0 0 0; }",
    ".selector-guide-heading { margin-top: 20px !important; border-top: 1px solid var(--border-default); padding-top: 16px; }",

    // API Reference index.
    ".api-index { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px 24px; margin-top: 12px; }",
    ".api-index-group { display: flex; flex-direction: column; gap: 2px; }",
    ".api-index a { color: var(--text-secondary); text-decoration: none; font-size: 12px; line-height: 1.5; }",
    ".api-index a:hover { color: var(--interactive-primary); }",
    ".api-index a code { font-size: 11px; }",
    ".api-index-heading { font-weight: 600; font-size: 13px !important; color: var(--text-primary) !important; margin-bottom: 1px; }",
    ".api-index-desc { color: var(--text-muted); font-size: 11px; margin-bottom: 3px; }",

    // Other landing page styles.
    ".endpoint code { font-size: 13px; }",

    // Modified value indicator styling.
    ".form-group.modified { border-left: 3px solid var(--interactive-primary); padding-left: 12px; }",
    ".modified-dot { display: inline-block; width: 8px; height: 8px; background: var(--interactive-primary); border-radius: 50%; margin-right: 6px; ",
    "vertical-align: middle; }",

    // Per-setting reset button styling.
    ".btn-reset { background: transparent; border: 1px solid var(--border-default); border-radius: var(--radius-md); padding: 4px 8px; margin-left: 8px; ",
    "cursor: pointer; font-size: 14px; color: var(--text-secondary); transition: all 0.15s ease; }",
    ".btn-reset:hover { background: var(--surface-elevated); border-color: var(--interactive-primary); color: var(--interactive-primary); }",

    // Backup subtab section styling.
    ".backup-group { margin-bottom: 35px; }",
    ".backup-group-title { font-size: 16px; font-weight: 600; margin-bottom: 15px; color: var(--text-heading); ",
    "padding-bottom: 8px; border-bottom: 1px solid var(--border-default); }",
    ".backup-section { margin-bottom: 20px; padding: 20px; background: var(--surface-elevated); border-radius: var(--radius-lg); ",
    "border: 1px solid var(--border-default); }",
    ".backup-section h3 { margin-top: 0; margin-bottom: 10px; color: var(--text-heading-secondary); font-size: 15px; }",
    ".backup-section p { color: var(--text-secondary); margin-bottom: 15px; font-size: 14px; }",
    ".backup-section code { background: var(--surface-code); padding: 2px 5px; border-radius: 3px; font-size: 12px; }",
    "#import-settings-file, #import-channels-file, #import-m3u-file { display: none; }",
    ".btn-export { background: var(--surface-elevated); border: 1px solid var(--border-default); color: var(--text-primary); ",
    "padding: 10px 20px; border-radius: var(--radius-md); font-size: 14px; cursor: pointer; transition: all 0.15s ease; }",
    ".btn-export:hover { border-color: var(--interactive-primary); color: var(--interactive-primary); }",
    ".btn-import { background: var(--surface-elevated); border: 1px solid var(--border-default); color: var(--text-primary); ",
    "padding: 10px 20px; border-radius: var(--radius-md); font-size: 14px; cursor: pointer; transition: all 0.15s ease; }",
    ".btn-import:hover { border-color: var(--interactive-primary); color: var(--interactive-primary); }",

    // Login modal styles for channel authentication.
    ".login-modal { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0, 0, 0, 0.5); display: flex; ",
    "align-items: center; justify-content: center; z-index: 1000; }",
    ".login-modal-content { background: var(--surface-overlay); padding: 30px; border-radius: var(--radius-lg); max-width: 450px; width: 90%; ",
    "box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3); }",
    ".login-modal-content h3 { margin-top: 0; margin-bottom: 15px; color: var(--text-heading); }",
    ".login-modal-content p { color: var(--text-secondary); margin-bottom: 15px; font-size: 14px; line-height: 1.5; }",
    ".login-modal-hint { font-size: 13px; color: var(--text-muted); }",
    ".login-modal-buttons { margin-top: 20px; text-align: right; }",

    // Restart dialog modal styles for pending restart notification.
    ".restart-modal { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0, 0, 0, 0.5); display: none; ",
    "align-items: center; justify-content: center; z-index: 1000; }",
    ".restart-modal-content { background: var(--surface-overlay); padding: 30px; border-radius: var(--radius-lg); max-width: 400px; width: 90%; ",
    "box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3); text-align: center; }",
    ".restart-modal-content h3 { margin-top: 0; margin-bottom: 15px; color: var(--text-heading); }",
    ".restart-modal-content p { color: var(--text-secondary); margin-bottom: 0; font-size: 14px; line-height: 1.5; }",
    ".restart-modal-status { margin: 16px 0; color: var(--text-muted); font-size: 13px; }",
    ".restart-modal-buttons { display: flex; gap: 12px; justify-content: center; margin-top: 20px; }",
    ".btn-danger { background: var(--interactive-danger); color: white; border: none; padding: 10px 20px; border-radius: var(--radius-md); ",
    "font-size: 14px; cursor: pointer; transition: all 0.15s ease; }",
    ".btn-danger:hover { opacity: 0.9; }",

    // Toast notification container: fixed top-right, above all modals.
    ".toast-container { position: fixed; top: 20px; right: 20px; z-index: 1001; display: flex; flex-direction: column; gap: 8px; pointer-events: none; }",

    // Individual toast: themed colors, slide-in animation, close button.
    ".toast { padding: 12px 36px 12px 16px; border-radius: var(--radius-md); box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15); min-width: 280px; max-width: 420px; ",
    "font-size: 13px; line-height: 1.4; white-space: pre-line; position: relative; pointer-events: auto; animation: toastIn 0.3s ease-out; }",
    ".toast.toast-exit { animation: toastOut 0.3s ease-in forwards; }",

    // Type variants using existing theme status variables.
    ".toast.success { background: var(--status-success-bg); border: 1px solid var(--status-success-border); color: var(--status-success-text); }",
    ".toast.error { background: var(--status-error-bg); border: 1px solid var(--status-error-border); color: var(--status-error-text); }",
    ".toast.warning { background: var(--status-warning-bg); border: 1px solid var(--status-warning-border); color: var(--status-warning-text); }",
    ".toast.info { background: var(--status-info-bg); border: 1px solid var(--status-info-border); color: var(--status-info-text); }",

    // Close button positioned top-right within each toast.
    ".toast-close { position: absolute; top: 8px; right: 8px; background: none; border: none; cursor: pointer; font-size: 16px; line-height: 1; padding: 0 4px; ",
    "color: inherit; opacity: 0.6; }",
    ".toast-close:hover { opacity: 1; }",

    // Action button for toasts with an undo or similar inline action.
    ".toast-action { display: inline-block; margin-left: 8px; padding: 2px 10px; border: 1px solid currentColor; border-radius: var(--radius-sm); ",
    "background: none; color: inherit; cursor: pointer; font-size: 12px; font-weight: 600; opacity: 0.8; vertical-align: baseline; }",
    ".toast-action:hover { opacity: 1; background: rgba(0, 0, 0, 0.1); }",

    // Toast slide animations.
    "@keyframes toastIn { from { transform: translateX(120%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }",
    "@keyframes toastOut { from { transform: translateX(0); opacity: 1; } to { transform: translateX(120%); opacity: 0; } }",

    // Responsive: full-width toasts on narrow screens.
    "@media (max-width: 768px) { .toast-container { left: 20px; right: 20px; } .toast { min-width: 0; max-width: none; } }",

    // Loading state for buttons.
    ".btn.loading { opacity: 0.7; pointer-events: none; }",
    ".btn.loading::after { content: '...'; }",

    // Inline copy button for Quick Start section.
    ".btn-copy-inline { background: var(--surface-elevated); border: 1px solid var(--border-default); padding: 2px 8px; font-size: 12px; ",
    "border-radius: var(--radius-sm); cursor: pointer; color: var(--text-secondary); margin-left: 6px; vertical-align: middle; }",
    ".btn-copy-inline:hover { background: var(--surface-hover); color: var(--text-primary); }",
    ".copy-feedback-inline { color: var(--stream-healthy); font-size: 12px; margin-left: 8px; display: none; }",

    // Version display styles.
    ".version-container { display: inline-flex; align-items: center; gap: 6px; }",
    ".version { font-size: 13px; color: var(--text-muted); font-weight: 400; text-decoration: none; transition: color 0.2s; }",
    ".version:hover { color: var(--text-primary); }",
    ".version.version-update { color: var(--interactive-primary); }",
    ".version.version-update:hover { color: var(--interactive-primary-hover, var(--interactive-primary)); text-decoration: underline; }",
    ".version-check { background: none; border: none; padding: 0; margin: 0; cursor: pointer; font-size: 14px; color: var(--text-muted); ",
    "line-height: 1; transition: color 0.2s, transform 0.3s; opacity: 0.7; }",
    ".version-check:hover { color: var(--text-primary); opacity: 1; }",
    ".version-check.checking { animation: spin 1s linear infinite; pointer-events: none; }",
    ".version-check.up-to-date { color: var(--stream-healthy); opacity: 1; }",
    ".version-check.check-error { color: var(--stream-error); opacity: 1; }",
    "@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }",

    // Changelog modal styles.
    ".changelog-modal { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0, 0, 0, 0.5); display: none; ",
    "align-items: center; justify-content: center; z-index: 1000; }",
    ".changelog-modal-content { background: var(--surface-overlay); padding: 30px; border-radius: var(--radius-lg); max-width: 500px; width: 90%; ",
    "max-height: 80vh; overflow-y: auto; box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3); }",
    ".changelog-modal-content h3 { margin-top: 0; margin-bottom: 20px; color: var(--text-heading); }",
    ".changelog-list { margin: 0 0 20px 0; padding: 0 0 0 20px; color: var(--text-secondary); font-size: 14px; line-height: 1.6; }",
    ".changelog-list li { margin-bottom: 8px; }",
    ".changelog-list li:last-child { margin-bottom: 0; }",
    ".changelog-modal-buttons { display: flex; gap: 12px; justify-content: flex-end; }"
  ].join("\n");
}
