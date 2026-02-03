/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ui.ts: Shared UI components and utilities for PrismCast web pages.
 */
import { generateThemeStyles } from "./theme.js";

/*
 * SHARED UI COMPONENTS
 *
 * This module provides reusable UI components used across PrismCast web pages. It ensures consistent styling and behavior between the landing page and
 * configuration page by extracting common CSS and JavaScript patterns. All styles use CSS custom properties from theme.ts for automatic dark mode support.
 */

// Re-export components for convenience.
export { generateAlert, generateBadge, generateButton, generateCodeBlock, generatePanelHeader, generateSection, generateSelect, generateSimpleAlert,
  generateStatusIndicator, generateTextInput } from "./components.js";
export type { AlertType, BadgeVariant, ButtonOptions, ButtonSize, ButtonVariant, SelectOption, SelectOptions, TextInputOptions } from "./components.js";

/**
 * Options for configuring the tab script behavior.
 */
export interface TabScriptOptions {

  // localStorage key for persisting tab selection.
  localStorageKey?: string;

  // Whether to hide an element when on a specific tab.
  hideElementOnTab?: {

    elementId: string;
    tabName: string;
  };
}

/**
 * Generates the base CSS styles shared across PrismCast pages. These styles provide consistent typography, layout, and basic component styling using CSS custom
 * properties for theme support.
 * @returns CSS styles as a string.
 */
export function generateBaseStyles(): string {

  return [

    // Base styles.
    "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 2000px; margin: 40px auto; padding: 0 20px; ",
    "line-height: 1.6; color: var(--text-primary); background: var(--surface-page); }",
    "h1 { color: var(--text-heading); border-bottom: 2px solid var(--interactive-primary); padding-bottom: 10px; }",
    "h2 { color: var(--text-heading-secondary); margin-top: 0; margin-bottom: 20px; }",
    "h3 { color: var(--text-heading-secondary); margin-top: 25px; margin-bottom: 15px; }",

    // Link styles.
    "a { color: var(--interactive-primary); text-decoration: none; }",
    "a:hover { text-decoration: underline; }",

    // Code styles.
    "code { background: var(--surface-code); padding: 2px 6px; border-radius: var(--radius-sm); font-family: 'SF Mono', Monaco, monospace; }",
    "pre { background: var(--surface-pre); padding: 15px; border-radius: var(--radius-lg); overflow-x: auto; border: 1px solid var(--border-default); ",
    "font-size: 13px; margin: 15px 0; }",

    // Table styles.
    "table { border-collapse: collapse; width: 100%; margin: 15px 0; }",
    "th, td { text-align: left; padding: 10px 12px; border: 1px solid var(--border-default); }",
    "th { background: var(--table-header-bg); font-weight: 600; }",
    "tr:nth-child(even) { background: var(--table-row-even); }",

    // Header styles.
    ".header { display: flex; align-items: center; gap: 20px; border-bottom: 2px solid var(--interactive-primary); padding-bottom: 15px; margin-bottom: 20px; }",
    ".logo { height: 80px; width: auto; }",
    ".header h1 { border-bottom: none; padding-bottom: 0; margin: 0; }",

    // Section styles.
    ".section { margin-bottom: 30px; }",

    // Alert styles (unified for success, error, warning).
    ".alert { padding: 15px; border-radius: var(--radius-lg); margin-bottom: 20px; }",
    ".alert-title { font-weight: bold; margin-bottom: 5px; }",
    ".alert-success, .success { background: var(--status-success-bg); border: 1px solid var(--status-success-border); }",
    ".alert-success .alert-title, .success-title { color: var(--status-success-text); }",
    ".alert-error, .error { background: var(--status-error-bg); border: 1px solid var(--status-error-border); }",
    ".alert-error .alert-title, .error-title { color: var(--status-error-text); }",
    ".alert-warning, .warning { background: var(--status-warning-bg); border: 1px solid var(--status-warning-border); }",
    ".alert-warning .alert-title, .warning-title { color: var(--status-warning-text); }",

    // Button styles.
    ".btn { padding: 10px 20px; border: none; border-radius: var(--radius-lg); cursor: pointer; font-size: 14px; font-weight: 500; ",
    "transition: background-color 0.2s; }",
    ".btn-primary { background: var(--interactive-primary); color: var(--text-inverse); }",
    ".btn-primary:hover { background: var(--interactive-primary-hover); }",
    ".btn-secondary { background: var(--interactive-secondary); color: var(--text-inverse); }",
    ".btn-secondary:hover { background: var(--interactive-secondary-hover); }",
    ".btn-danger { background: var(--interactive-danger); color: var(--text-inverse); }",
    ".btn-danger:hover { background: var(--interactive-danger-hover); }",
    ".btn-edit { background: var(--interactive-edit); color: var(--text-inverse); }",
    ".btn-edit:hover { background: var(--interactive-edit-hover); }",
    ".btn-delete { background: var(--interactive-delete); color: var(--text-inverse); }",
    ".btn-delete:hover { background: var(--interactive-delete-hover); }",
    ".btn-group { display: flex; gap: 6px; }",
    ".btn-sm { padding: 6px 12px; font-size: 12px; white-space: nowrap; }",
    ".btn:disabled { opacity: 0.6; cursor: not-allowed; }",

    // Badge styles.
    ".badge { display: inline-block; padding: 3px 8px; border-radius: var(--radius-sm); font-size: 11px; font-weight: 600; }",
    ".badge-builtin { background: var(--badge-builtin-bg); color: var(--badge-builtin-text); }",
    ".badge-custom { background: var(--badge-custom-bg); color: var(--badge-custom-text); }",
    ".badge-override { background: var(--badge-override-bg); color: var(--badge-override-text); }",
    ".badge-env { background: var(--badge-env-bg); color: var(--badge-env-text); }",
    ".badge-flag { background: var(--badge-flag-bg); color: var(--badge-flag-text); }",

    // Status indicator styles.
    ".status-indicator { white-space: nowrap; }",
    ".status-dot { margin-right: 4px; }",
    ".status-healthy .status-dot { color: var(--stream-healthy); }",
    ".status-buffering .status-dot { color: var(--stream-buffering); }",
    ".status-recovering .status-dot { color: var(--stream-recovering); }",
    ".status-stalled .status-dot { color: var(--stream-stalled); }",
    ".status-error .status-dot { color: var(--stream-error); }",

    // Form styles.
    ".form-group { margin-bottom: 20px; padding: 15px; background: var(--form-bg); border-radius: var(--radius-lg); }",
    ".form-group.disabled { background: var(--form-bg-disabled); }",
    ".form-group.depends-disabled { opacity: 0.45; pointer-events: none; }",
    ".form-row { display: flex; align-items: center; gap: 15px; margin-bottom: 8px; }",
    ".form-row label { display: block; font-weight: 600; margin-bottom: 5px; font-size: 13px; }",
    ".form-label { font-weight: 600; min-width: 200px; }",

    // Base input and select styles. Uses class selectors only for predictable specificity. All form inputs should use the .form-input class, and all form selects
    // should use the .form-select class.
    ".form-input { flex: 1; padding: 8px 12px; border: 1px solid var(--form-input-border); border-radius: var(--radius-md); font-size: 14px; ",
    "background: var(--form-input-bg); color: var(--text-primary); }",
    ".form-select { flex: 0 0 auto; padding: 8px 12px; border: 1px solid var(--form-input-border); border-radius: var(--radius-md); ",
    "background: var(--form-input-bg); color: var(--text-primary); font-size: 14px; cursor: pointer; }",
    ".form-checkbox { flex: none; width: 18px; height: 18px; cursor: pointer; accent-color: var(--interactive-primary); }",

    // Focus states.
    ".form-input:focus, .form-select:focus { border-color: var(--interactive-primary); outline: none; box-shadow: 0 0 0 2px var(--border-focus); }",

    // Disabled states.
    ".form-input:disabled, .form-select:disabled { background: var(--form-bg-disabled); cursor: not-allowed; color: var(--text-disabled); }",
    ".form-checkbox:disabled { cursor: not-allowed; opacity: 0.5; }",

    // Error states.
    ".form-input.error, .form-select.error { border-color: var(--status-error-border); background: var(--status-error-bg); }",

    // Field width classes using compound selectors for sufficient specificity to override the base flex: 1 rule. Inputs use fixed widths for predictable numeric
    // content. Selects use min-width to ensure minimum sizing while allowing growth for longer option text.
    ".form-input.field-narrow { flex: none; width: 55px; }",
    ".form-input.field-medium { flex: none; width: 100px; }",
    ".form-input.field-wide { flex: none; width: 200px; }",
    ".form-select.field-narrow { flex: 0 0 auto; min-width: 55px; }",
    ".form-select.field-medium { flex: 0 0 auto; min-width: 100px; }",
    ".form-select.field-wide { flex: 0 0 auto; min-width: 200px; }",

    // Form metadata styles.
    ".form-unit { color: var(--text-secondary); font-size: 13px; min-width: 40px; }",
    ".form-description { color: var(--text-secondary); font-size: 13px; margin-top: 5px; }",
    ".form-default { color: var(--text-muted); font-size: 12px; margin-top: 3px; }",
    ".form-env { color: var(--status-warning-text); font-size: 12px; margin-top: 3px; font-style: italic; }",
    ".form-error { color: var(--status-error-text); font-size: 12px; margin-top: 3px; }",
    ".form-warning { color: var(--status-warning-text); font-size: 12px; margin-top: 5px; padding: 6px 10px; background: var(--status-warning-bg); ",
    "border-radius: var(--radius-sm); }",
    ".hint { color: var(--text-secondary); font-size: 12px; margin-top: 1px; margin-bottom: 15px; }",

    // Env badge in form labels.
    ".env-badge { display: inline-block; background: var(--badge-env-bg); color: var(--badge-env-text); padding: 2px 8px; ",
    "border-radius: var(--radius-sm); font-size: 11px; font-weight: 600; margin-left: 10px; }",

    // Button row.
    ".button-row { margin-top: 30px; padding: 20px 0; border-top: 1px solid var(--border-default); display: flex; gap: 15px; }",
    ".form-buttons { display: flex; gap: 10px; margin-top: 20px; }",

    // Config path display.
    ".config-path { margin-top: 20px; padding-top: 15px; border-top: 1px solid var(--border-default); font-size: 13px; color: var(--text-secondary); }",

    // Panel header.
    ".panel-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 15px; ",
    "border-bottom: 1px solid var(--border-light); }",
    ".panel-reset { font-size: 13px; }",

    // Collapsible sections for Advanced tab.
    ".advanced-section { margin-bottom: 16px; border: 1px solid var(--border-default); border-radius: var(--radius-lg); overflow: hidden; }",
    ".section-header { display: flex; align-items: center; padding: 14px 18px; cursor: pointer; background: var(--surface-elevated); ",
    "transition: background 0.15s; user-select: none; }",
    ".section-header:hover { background: var(--surface-code); }",
    ".section-chevron { margin-right: 12px; font-size: 10px; color: var(--text-secondary); transition: transform 0.2s; }",
    ".section-header.expanded .section-chevron { transform: rotate(90deg); }",
    ".section-title { font-weight: 600; font-size: 14px; color: var(--text-primary); }",
    ".section-count { margin-left: 10px; color: var(--text-muted); font-size: 13px; font-weight: normal; }",
    ".section-content { display: none; padding: 20px; border-top: 1px solid var(--border-default); background: var(--surface-page); }",
    ".section-content.expanded { display: block; }",
    ".section-content .form-group:last-child { margin-bottom: 0; }",

    // Non-collapsible sections for Settings tab.
    ".settings-section { margin-bottom: 28px; }",
    ".settings-section:last-child { margin-bottom: 0; }",
    ".settings-section-header { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-secondary); ",
    "margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid var(--border-light); }"
  ].join("\n");
}

/**
 * Generates the CSS styles for the tabbed interface. These styles provide the tab bar, tab buttons, and panel containers.
 * @returns CSS styles as a string.
 */
export function generateTabStyles(): string {

  return [

    // Tab bar container.
    ".tab-bar { display: flex; border-bottom: 2px solid var(--tab-border); margin-bottom: 0; gap: 4px; flex-wrap: wrap; }",

    // Tab buttons.
    ".tab-btn { padding: 12px 20px; border: none; background: var(--tab-bg); cursor: pointer; font-size: 14px; font-weight: 500; color: var(--tab-text); ",
    "border-radius: var(--radius-lg) var(--radius-lg) 0 0; transition: all 0.2s; position: relative; top: 2px; }",
    ".tab-btn:hover { background: var(--tab-bg-hover); color: var(--tab-text-hover); }",
    ".tab-btn.active { background: var(--tab-bg-active); color: var(--tab-text-active); border: 2px solid var(--tab-border); ",
    "border-bottom: 2px solid var(--tab-bg-active); }",
    ".tab-btn.has-error { color: var(--tab-error); }",
    ".tab-btn.has-error.active { color: var(--tab-error); }",

    // Tab panels.
    ".tab-panel { display: none; padding: 25px; border: 2px solid var(--tab-border); border-top: none; border-radius: 0 0 var(--radius-lg) var(--radius-lg); ",
    "background: var(--surface-page); }",
    ".tab-panel.active { display: block; }"
  ].join("\n");
}

/**
 * Generates the JavaScript for tab switching functionality. This script handles click events on tab buttons, keyboard navigation, hash-based URL navigation,
 * and localStorage persistence of the selected tab.
 * @param options - Configuration options for the tab script.
 * @returns JavaScript code as a string wrapped in script tags.
 */
export function generateTabScript(options: TabScriptOptions = {}): string {

  const { hideElementOnTab, localStorageKey = "prismcast-tab" } = options;

  const hideLogic = hideElementOnTab ?
    "    if(hideElement) {\n      hideElement.style.display = (category === '" + hideElementOnTab.tabName + "') ? 'none' : 'flex';\n    }" :
    "";

  const hideElementSelector = hideElementOnTab ?
    "  var hideElement = document.getElementById('" + hideElementOnTab.elementId + "');\n" :
    "";

  return [
    "<script>",
    "(function() {",

    // Parse hash to extract main tab and optional subtab.
    "  function parseHash() {",
    "    var hash = window.location.hash.slice(1);",
    "    if(!hash) return { tab: null, subtab: null };",
    "    var parts = hash.split('/');",
    "    return { tab: parts[0] || null, subtab: parts[1] || null };",
    "  }",

    // Update URL hash without triggering hashchange handler.
    "  var updatingHash = false;",
    "  function updateHash(tab, subtab) {",
    "    updatingHash = true;",
    "    var newHash = '#' + tab + (subtab ? '/' + subtab : '');",
    "    if(window.location.hash !== newHash) {",
    "      window.location.hash = newHash;",
    "    }",
    "    setTimeout(function() { updatingHash = false; }, 0);",
    "  }",

    // Tab switching function.
    "  function switchTab(category, updateUrl) {",
    "    var tabs = document.querySelectorAll('.tab-btn');",
    "    var panels = document.querySelectorAll('.tab-panel');",
    hideElementSelector,
    "    for(var i = 0; i < tabs.length; i++) {",
    "      tabs[i].classList.remove('active');",
    "      tabs[i].setAttribute('aria-selected', 'false');",
    "      tabs[i].setAttribute('tabindex', '-1');",
    "      if(tabs[i].getAttribute('data-category') === category) {",
    "        tabs[i].classList.add('active');",
    "        tabs[i].setAttribute('aria-selected', 'true');",
    "        tabs[i].setAttribute('tabindex', '0');",
    "      }",
    "    }",
    "    for(var j = 0; j < panels.length; j++) {",
    "      panels[j].classList.remove('active');",
    "      if(panels[j].id === 'panel-' + category) {",
    "        panels[j].classList.add('active');",
    "      }",
    "    }",
    hideLogic,
    "    try { localStorage.setItem('" + localStorageKey + "', category); } catch(e) {}",
    "    if(updateUrl !== false) {",
    "      var parsed = parseHash();",
    "      var subtab = (category === 'config') ? parsed.subtab : null;",
    "      updateHash(category, subtab);",
    "    }",
    "    document.dispatchEvent(new CustomEvent('tabactivated', { detail: { category: category } }));",
    "  }",

    // Expose switchTab globally for subtab script to use.
    "  window.switchMainTab = switchTab;",

    // Attach click handlers to tabs.
    "  var tabBtns = document.querySelectorAll('.tab-btn');",
    "  for(var i = 0; i < tabBtns.length; i++) {",
    "    tabBtns[i].addEventListener('click', function() {",
    "      switchTab(this.getAttribute('data-category'));",
    "    });",
    "  }",

    // Keyboard navigation for tabs.
    "  var tabBar = document.querySelector('.tab-bar');",
    "  if(tabBar) {",
    "    tabBar.addEventListener('keydown', function(e) {",
    "      if(e.key === 'ArrowRight' || e.key === 'ArrowLeft') {",
    "        var tabs = Array.prototype.slice.call(document.querySelectorAll('.tab-btn'));",
    "        var current = document.querySelector('.tab-btn.active');",
    "        var idx = tabs.indexOf(current);",
    "        if(e.key === 'ArrowRight') { idx = (idx + 1) % tabs.length; }",
    "        else { idx = (idx - 1 + tabs.length) % tabs.length; }",
    "        tabs[idx].focus();",
    "        switchTab(tabs[idx].getAttribute('data-category'));",
    "      }",
    "    });",
    "  }",

    // Handle browser back/forward navigation.
    "  window.addEventListener('hashchange', function() {",
    "    if(updatingHash) return;",
    "    var parsed = parseHash();",
    "    if(parsed.tab && document.querySelector('.tab-btn[data-category=\"' + parsed.tab + '\"]')) {",
    "      switchTab(parsed.tab, false);",
    "      if(parsed.tab === 'config' && parsed.subtab && window.switchConfigSubtab) {",
    "        window.switchConfigSubtab(parsed.subtab, false);",
    "      }",
    "    }",
    "  });",

    // Determine initial tab: hash > localStorage > default.
    "  var parsed = parseHash();",
    "  var initialTab = parsed.tab;",
    "  if(!initialTab) {",
    "    try { initialTab = localStorage.getItem('" + localStorageKey + "'); } catch(e) {}",
    "  }",
    "  if(initialTab && document.querySelector('.tab-btn[data-category=\"' + initialTab + '\"]')) {",
    "    switchTab(initialTab, !parsed.tab);",
    "  } else {",
    "    var activeTab = document.querySelector('.tab-btn.active');",
    "    if(activeTab) {",
    "      var category = activeTab.getAttribute('data-category');",
    "      if(!parsed.tab) updateHash(category, null);",
    "      document.dispatchEvent(new CustomEvent('tabactivated', { detail: { category: category } }));",
    "    }",
    "  }",

    // Store parsed hash for subtab script to use.
    "  window.initialHashSubtab = parsed.subtab;",

    "})();",
    "</script>"
  ].join("\n");
}

/**
 * Generates a tab button HTML element.
 * @param category - The category identifier for the tab.
 * @param label - The display label for the tab.
 * @param isActive - Whether this tab is initially active.
 * @param hasError - Whether to show an error indicator on the tab.
 * @returns HTML string for the tab button.
 */
export function generateTabButton(category: string, label: string, isActive: boolean, hasError = false): string {

  const activeClass = isActive ? " active" : "";
  const errorClass = hasError ? " has-error" : "";
  const ariaSelected = isActive ? "true" : "false";
  const tabIndex = isActive ? "0" : "-1";

  return "<button type=\"button\" class=\"tab-btn" + activeClass + errorClass + "\" data-category=\"" + category +
    "\" role=\"tab\" aria-selected=\"" + ariaSelected + "\" aria-controls=\"panel-" + category + "\" tabindex=\"" + tabIndex + "\">" + label + "</button>";
}

/**
 * Generates a tab panel container HTML element.
 * @param category - The category identifier for the panel.
 * @param content - The HTML content of the panel.
 * @param isActive - Whether this panel is initially visible.
 * @returns HTML string for the tab panel.
 */
export function generateTabPanel(category: string, content: string, isActive: boolean): string {

  const activeClass = isActive ? " active" : "";

  return "<div id=\"panel-" + category + "\" class=\"tab-panel" + activeClass + "\" role=\"tabpanel\">" + content + "</div>";
}

/**
 * Generates the common page wrapper HTML structure with head, styles, and body. Automatically includes theme styles for dark mode support.
 * @param title - The page title.
 * @param styles - CSS styles to include in the head.
 * @param bodyContent - HTML content for the body.
 * @param scripts - Optional JavaScript to include at the end of the body.
 * @returns Complete HTML document string.
 */
export function generatePageWrapper(title: string, styles: string, bodyContent: string, scripts = ""): string {

  // Theme styles must come first so other styles can reference CSS variables.
  const allStyles = [ generateThemeStyles(), styles ].join("\n");

  return [
    "<!DOCTYPE html>",
    "<html lang=\"en\">",
    "<head>",
    "<meta charset=\"UTF-8\">",
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">",
    "<meta name=\"color-scheme\" content=\"light dark\">",
    "<title>" + title + "</title>",
    "<link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">",
    "<link rel=\"icon\" type=\"image/png\" sizes=\"32x32\" href=\"/favicon.png\">",
    "<link rel=\"apple-touch-icon\" sizes=\"180x180\" href=\"/logo.png\">",
    "<style>",
    allStyles,
    "</style>",
    "</head>",
    "<body>",
    bodyContent,
    scripts,
    "</body>",
    "</html>"
  ].join("\n");
}
