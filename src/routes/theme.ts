/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * theme.ts: Centralized theme system with CSS custom properties for PrismCast UI.
 */

/* This module provides a centralized color and styling system for all PrismCast web pages. It uses CSS custom properties (variables) to enable consistent theming
 * and automatic dark mode support via the prefers-color-scheme media query.
 *
 * Color tokens are organized semantically:
 * - Surface: Background colors for pages, cards, sections
 * - Text: Typography colors at various emphasis levels
 * - Border: Dividers and boundaries
 * - Interactive: Links, buttons, focus states
 * - Status: Success, warning, error, info feedback
 * - Stream: Health indicators for stream status
 */

/**
 * Generates CSS custom property definitions for the theme system. Includes both light theme (default) and dark theme via prefers-color-scheme media query.
 * @returns CSS string with :root variables and dark mode overrides.
 */
export function generateThemeStyles(): string {

  return [

    // Light theme (default).
    ":root {",

    // Surface colors - backgrounds at different elevation levels.
    "  --surface-page: #ffffff;",
    "  --surface-elevated: #f9f9f9;",
    "  --surface-sunken: #f4f4f4;",
    "  --surface-overlay: #ffffff;",
    "  --surface-code: #f4f4f4;",
    "  --surface-pre: #f8f8f8;",

    // Text colors - typography at different emphasis levels.
    "  --text-primary: #333333;",
    "  --text-secondary: #666666;",
    "  --text-muted: #888888;",
    "  --text-disabled: #999999;",
    "  --text-heading: #2c3e50;",
    "  --text-heading-secondary: #34495e;",
    "  --text-inverse: #ffffff;",

    // Border colors - dividers and boundaries.
    "  --border-default: #dddddd;",
    "  --border-light: #eeeeee;",
    "  --border-strong: #cccccc;",
    "  --border-focus: rgba(52, 152, 219, 0.2);",

    // Interactive colors - links, buttons, focus states.
    "  --interactive-primary: #3498db;",
    "  --interactive-primary-hover: #2980b9;",
    "  --interactive-secondary: #95a5a6;",
    "  --interactive-secondary-hover: #7f8c8d;",
    "  --interactive-danger: #e74c3c;",
    "  --interactive-danger-hover: #c0392b;",
    "  --interactive-edit: #17a2b8;",
    "  --interactive-edit-hover: #138496;",
    "  --interactive-delete: #dc3545;",
    "  --interactive-delete-hover: #c82333;",

    // Status colors - feedback messages.
    "  --status-success-bg: #d4edda;",
    "  --status-success-border: #c3e6cb;",
    "  --status-success-text: #155724;",
    "  --status-warning-bg: #fff3cd;",
    "  --status-warning-border: #ffc107;",
    "  --status-warning-text: #856404;",
    "  --status-error-bg: #f8d7da;",
    "  --status-error-border: #f5c6cb;",
    "  --status-error-text: #721c24;",

    // Stream health indicator colors.
    "  --stream-healthy: #27ae60;",
    "  --stream-buffering: #f39c12;",
    "  --stream-recovering: #e67e22;",
    "  --stream-stalled: #e74c3c;",
    "  --stream-error: #c0392b;",

    // Stream row tint colors - subtle background tints for health status.
    "  --stream-tint-buffering: rgba(243, 156, 18, 0.08);",
    "  --stream-tint-recovering: rgba(230, 126, 34, 0.08);",
    "  --stream-tint-stalled: rgba(230, 126, 34, 0.12);",
    "  --stream-tint-error: rgba(192, 57, 43, 0.1);",

    // Badge colors.
    "  --badge-builtin-bg: #e9ecef;",
    "  --badge-builtin-text: #6c757d;",
    "  --badge-custom-bg: #d4edda;",
    "  --badge-custom-text: #155724;",
    "  --badge-override-bg: #fff3cd;",
    "  --badge-override-text: #856404;",
    "  --badge-env-bg: #ffc107;",
    "  --badge-env-text: #856404;",
    "  --badge-flag-bg: #3498db;",
    "  --badge-flag-text: #ffffff;",

    // User channel row tint - subtle highlight for custom/override channels.
    "  --user-channel-tint: rgba(52, 152, 219, 0.06);",
    "  --user-channel-tint-hover: rgba(52, 152, 219, 0.12);",

    // Tab colors.
    "  --tab-bg: #f4f4f4;",
    "  --tab-bg-hover: #e8e8e8;",
    "  --tab-bg-active: #ffffff;",
    "  --tab-text: #666666;",
    "  --tab-text-hover: #333333;",
    "  --tab-text-active: #3498db;",
    "  --tab-border: #dddddd;",
    "  --tab-error: #dc3545;",

    // Subtab colors.
    "  --subtab-bg: #f8f8f8;",
    "  --subtab-bg-hover: #e8e8e8;",
    "  --subtab-bg-active: #e3f2fd;",
    "  --subtab-text-active: #1976d2;",
    "  --subtab-border-active: #1976d2;",

    // Form colors.
    "  --form-bg: #f9f9f9;",
    "  --form-bg-disabled: #e9ecef;",
    "  --form-input-bg: #ffffff;",
    "  --form-input-border: #dddddd;",

    // Table colors.
    "  --table-header-bg: #f4f4f4;",
    "  --table-row-even: #fafafa;",
    "  --table-row-hover: #f5f5f5;",

    // Dark surface colors (used for logs, streams table in light mode too).
    "  --dark-surface-bg: #1e1e1e;",
    "  --dark-surface-elevated: #2d2d2d;",
    "  --dark-surface-header: #3d3d3d;",
    "  --dark-surface-row: #252525;",
    "  --dark-surface-row-hover: #353535;",
    "  --dark-text-primary: #e0e0e0;",
    "  --dark-text-secondary: #d4d4d4;",
    "  --dark-text-muted: #999999;",
    "  --dark-text-error: #f44747;",
    "  --dark-text-warn: #dcdcaa;",
    "  --dark-border: #3d3d3d;",
    "  --dark-scrollbar-track: #2d2d2d;",
    "  --dark-scrollbar-thumb: #555555;",
    "  --dark-scrollbar-thumb-hover: #666666;",

    // Spacing and sizing tokens.
    "  --radius-sm: 3px;",
    "  --radius-md: 4px;",
    "  --radius-lg: 5px;",
    "  --radius-xl: 6px;",
    "}",

    // Dark theme overrides.
    "@media (prefers-color-scheme: dark) {",
    "  :root {",

    // Surface colors - dark backgrounds.
    "    --surface-page: #1a1a1a;",
    "    --surface-elevated: #2a2a2a;",
    "    --surface-sunken: #151515;",
    "    --surface-overlay: #2d2d2d;",
    "    --surface-code: #2d2d2d;",
    "    --surface-pre: #252525;",

    // Text colors - light text on dark backgrounds.
    "    --text-primary: #e0e0e0;",
    "    --text-secondary: #b0b0b0;",
    "    --text-muted: #888888;",
    "    --text-disabled: #666666;",
    "    --text-heading: #ffffff;",
    "    --text-heading-secondary: #e0e0e0;",
    "    --text-inverse: #1a1a1a;",

    // Border colors - subtle borders for dark mode.
    "    --border-default: #444444;",
    "    --border-light: #333333;",
    "    --border-strong: #555555;",
    "    --border-focus: rgba(52, 152, 219, 0.4);",

    // Interactive colors - slightly brighter for dark mode contrast.
    "    --interactive-primary: #5dade2;",
    "    --interactive-primary-hover: #3498db;",
    "    --interactive-secondary: #7f8c8d;",
    "    --interactive-secondary-hover: #95a5a6;",
    "    --interactive-danger: #e74c3c;",
    "    --interactive-danger-hover: #c0392b;",
    "    --interactive-edit: #5bc0de;",
    "    --interactive-edit-hover: #46b8da;",
    "    --interactive-delete: #e74c3c;",
    "    --interactive-delete-hover: #c0392b;",

    // Status colors - darker backgrounds with adjusted contrast.
    "    --status-success-bg: #1e3a2f;",
    "    --status-success-border: #2d5a45;",
    "    --status-success-text: #75d99c;",
    "    --status-warning-bg: #3d3520;",
    "    --status-warning-border: #5a4d2a;",
    "    --status-warning-text: #f5d67b;",
    "    --status-error-bg: #3d2020;",
    "    --status-error-border: #5a2a2a;",
    "    --status-error-text: #f5a5a5;",

    // Stream health indicator colors - same in dark mode for consistency.
    "    --stream-healthy: #2ecc71;",
    "    --stream-buffering: #f1c40f;",
    "    --stream-recovering: #e67e22;",
    "    --stream-stalled: #e74c3c;",
    "    --stream-error: #c0392b;",

    // Stream row tint colors - slightly stronger for dark backgrounds.
    "    --stream-tint-buffering: rgba(243, 156, 18, 0.15);",
    "    --stream-tint-recovering: rgba(230, 126, 34, 0.15);",
    "    --stream-tint-stalled: rgba(230, 126, 34, 0.2);",
    "    --stream-tint-error: rgba(192, 57, 43, 0.2);",

    // Badge colors - adjusted for dark backgrounds.
    "    --badge-builtin-bg: #3d3d3d;",
    "    --badge-builtin-text: #b0b0b0;",
    "    --badge-custom-bg: #1e3a2f;",
    "    --badge-custom-text: #75d99c;",
    "    --badge-override-bg: #3d3520;",
    "    --badge-override-text: #f5d67b;",
    "    --badge-env-bg: #5a4d2a;",
    "    --badge-env-text: #f5d67b;",
    "    --badge-flag-bg: #2980b9;",
    "    --badge-flag-text: #ffffff;",

    // User channel row tint - slightly stronger for dark backgrounds.
    "    --user-channel-tint: rgba(93, 173, 226, 0.12);",
    "    --user-channel-tint-hover: rgba(93, 173, 226, 0.2);",

    // Tab colors - dark mode tabs.
    "    --tab-bg: #2d2d2d;",
    "    --tab-bg-hover: #3d3d3d;",
    "    --tab-bg-active: #1a1a1a;",
    "    --tab-text: #b0b0b0;",
    "    --tab-text-hover: #e0e0e0;",
    "    --tab-text-active: #5dade2;",
    "    --tab-border: #444444;",
    "    --tab-error: #e74c3c;",

    // Subtab colors - dark mode subtabs.
    "    --subtab-bg: #2d2d2d;",
    "    --subtab-bg-hover: #3d3d3d;",
    "    --subtab-bg-active: #1a3a5c;",
    "    --subtab-text-active: #5dade2;",
    "    --subtab-border-active: #5dade2;",

    // Form colors - dark mode forms.
    "    --form-bg: #2a2a2a;",
    "    --form-bg-disabled: #1f1f1f;",
    "    --form-input-bg: #333333;",
    "    --form-input-border: #444444;",

    // Table colors - dark mode tables.
    "    --table-header-bg: #2d2d2d;",
    "    --table-row-even: #252525;",
    "    --table-row-hover: #333333;",

    // Dark surface colors remain the same in dark mode (already dark).
    "    --dark-surface-bg: #1e1e1e;",
    "    --dark-surface-elevated: #2d2d2d;",
    "    --dark-surface-header: #3d3d3d;",
    "    --dark-surface-row: #252525;",
    "    --dark-surface-row-hover: #353535;",
    "    --dark-text-primary: #e0e0e0;",
    "    --dark-text-secondary: #d4d4d4;",
    "    --dark-text-muted: #999999;",
    "    --dark-text-error: #f44747;",
    "    --dark-text-warn: #dcdcaa;",
    "    --dark-border: #3d3d3d;",
    "  }",
    "}"
  ].join("\n");
}

/**
 * Returns CSS variable references for stream health colors. Used by JavaScript to read theme-aware colors at runtime.
 * @returns Object mapping health states to CSS variable names.
 */
export function getStreamHealthColorVars(): Record<string, string> {

  return {

    buffering: "var(--stream-buffering)",
    error: "var(--stream-error)",
    healthy: "var(--stream-healthy)",
    recovering: "var(--stream-recovering)",
    stalled: "var(--stream-stalled)"
  };
}

/**
 * Returns CSS variable references for log level colors. Used by JavaScript to read theme-aware colors at runtime.
 * @returns Object mapping log levels to CSS variable names.
 */
export function getLogLevelColorVars(): Record<string, string> {

  return {

    default: "var(--dark-text-secondary)",
    error: "var(--dark-text-error)",
    warn: "var(--dark-text-warn)"
  };
}
