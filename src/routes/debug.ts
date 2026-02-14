/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * debug.ts: Debug logging configuration endpoint for PrismCast.
 */
import { DEBUG_CATEGORIES, LOG, escapeHtml, getCurrentPattern, initDebugFilter, isCategoryEnabled } from "../utils/index.js";
import type { Express, Request, Response } from "express";
import { generateBaseStyles, generatePageWrapper } from "./ui.js";

/* This module provides a hidden (undocumented) web page at /debug for runtime control of debug logging categories. The page renders all known categories as
 * hierarchical checkboxes grouped by namespace prefix. Toggling a parent group enables or disables all children. Changes are applied immediately via POST without
 * requiring a server restart.
 */

// Types.

/**
 * A group of debug categories sharing a common namespace prefix.
 */
interface CategoryGroup {

  // Children categories (the part after the colon, or the full name for standalone items).
  children: Array<{ category: string; description: string }>;

  // The group prefix (e.g., "browser", "streaming"). For standalone categories, this equals the category name.
  prefix: string;
}

// Helpers.

/**
 * Organizes the flat category list into hierarchical groups by splitting on the first colon. Categories without a colon that are also a prefix of other categories
 * are treated as both a group parent and a child within that group. Standalone categories with no colon and no sub-categories form single-child groups.
 * @returns Sorted array of category groups.
 */
function buildCategoryGroups(): CategoryGroup[] {

  const groupMap = new Map<string, Array<{ category: string; description: string }>>();

  for(const entry of DEBUG_CATEGORIES) {

    const colonIndex = entry.category.indexOf(":");
    const prefix = (colonIndex === -1) ? entry.category : entry.category.substring(0, colonIndex);

    let group = groupMap.get(prefix);

    if(!group) {

      group = [];
      groupMap.set(prefix, group);
    }

    group.push({ category: entry.category, description: entry.description });
  }

  // Sort groups alphabetically by prefix, and children alphabetically within each group.
  const groups: CategoryGroup[] = [];

  for(const [ prefix, children ] of groupMap) {

    children.sort((a, b) => a.category.localeCompare(b.category));
    groups.push({ children, prefix });
  }

  groups.sort((a, b) => a.prefix.localeCompare(b.prefix));

  return groups;
}

/**
 * Generates the page-specific CSS styles for the debug endpoint.
 * @returns CSS string for the debug page.
 */
function generateDebugStyles(): string {

  return [

    ".debug-container { max-width: 800px; margin: 0 auto; padding: 24px; }",
    ".debug-header { margin-bottom: 24px; }",
    ".debug-header h1 { margin: 0 0 8px 0; font-size: 1.5rem; color: var(--text-heading); }",
    ".debug-header p { margin: 0; color: var(--text-secondary); font-size: 0.9rem; }",

    // Current status banner.
    ".debug-status { background: var(--surface-elevated); border: 1px solid var(--border-default); border-radius: 8px; padding: 12px 16px;",
    "  margin-bottom: 24px; font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace; font-size: 0.85rem; color: var(--text-primary);",
    "  word-break: break-all; }",
    ".debug-status-label { color: var(--text-muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }",

    // Category groups.
    ".debug-groups { display: flex; flex-direction: column; gap: 16px; margin-bottom: 24px; }",
    ".debug-group { background: var(--surface-elevated); border: 1px solid var(--border-default); border-radius: 8px; padding: 16px; }",
    ".debug-group-header { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }",
    ".debug-group-header label { font-weight: 600; font-size: 0.95rem; color: var(--text-heading); cursor: pointer; }",
    ".debug-group-children { padding-left: 28px; display: flex; flex-direction: column; gap: 6px; }",
    ".debug-child { display: flex; align-items: flex-start; gap: 8px; }",
    ".debug-child label { cursor: pointer; font-size: 0.9rem; color: var(--text-primary); }",
    ".debug-child-desc { color: var(--text-muted); font-size: 0.8rem; margin-left: 4px; }",

    // Checkbox styling.
    "input[type='checkbox'] { margin-top: 3px; cursor: pointer; accent-color: var(--interactive-primary); }",

    // Action bar.
    ".debug-actions { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-bottom: 24px; }",
    ".debug-actions button { padding: 8px 20px; border-radius: 6px; border: 1px solid var(--border-default); cursor: pointer; font-size: 0.9rem;",
    "  font-weight: 500; transition: background 0.15s, border-color 0.15s; }",
    ".debug-btn-apply { background: var(--interactive-primary); color: var(--text-inverse); border-color: var(--interactive-primary); }",
    ".debug-btn-apply:hover { background: var(--interactive-primary-hover); border-color: var(--interactive-primary-hover); }",
    ".debug-btn-secondary { background: var(--surface-elevated); color: var(--text-primary); }",
    ".debug-btn-secondary:hover { background: var(--surface-code); border-color: var(--border-strong); }",

    // Raw pattern input.
    ".debug-raw { margin-bottom: 24px; }",
    ".debug-raw label { display: block; font-size: 0.85rem; color: var(--text-secondary); margin-bottom: 6px; }",
    ".debug-raw input { width: 100%; box-sizing: border-box; padding: 8px 12px; border: 1px solid var(--border-default); border-radius: 6px;",
    "  font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace; font-size: 0.85rem; background: var(--surface-page); color: var(--text-primary); }",
    ".debug-raw input:focus { outline: none; border-color: var(--border-focus); box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15); }",
    ".debug-raw .debug-raw-hint { font-size: 0.75rem; color: var(--text-muted); margin-top: 4px; }"

  ].join("\n");
}

/**
 * Generates the client-side JavaScript for checkbox behavior: parent toggles all children, child changes update parent state, select all / deselect all, and
 * synchronization between checkboxes and the raw pattern input.
 * @returns JavaScript string (without script tags).
 */
function generateDebugScript(): string {

  return [

    "function updateParentState(prefix) {",
    "  var parent = document.getElementById('group-' + prefix);",
    "  if(!parent) return;",
    "  var children = document.querySelectorAll('input[data-group=\"' + prefix + '\"]');",
    "  var checked = 0;",
    "  for(var i = 0; i < children.length; i++) { if(children[i].checked) checked++; }",
    "  parent.checked = (checked === children.length);",
    "  parent.indeterminate = (checked > 0) && (checked < children.length);",
    "}",

    "function onParentToggle(prefix) {",
    "  var parent = document.getElementById('group-' + prefix);",
    "  var children = document.querySelectorAll('input[data-group=\"' + prefix + '\"]');",
    "  for(var i = 0; i < children.length; i++) { children[i].checked = parent.checked; }",
    "  parent.indeterminate = false;",
    "  syncRawFromCheckboxes();",
    "}",

    "function onChildToggle(prefix) {",
    "  updateParentState(prefix);",
    "  syncRawFromCheckboxes();",
    "}",

    "function selectAll(checked) {",
    "  var boxes = document.querySelectorAll('input[type=\"checkbox\"]');",
    "  for(var i = 0; i < boxes.length; i++) { boxes[i].checked = checked; boxes[i].indeterminate = false; }",
    "  syncRawFromCheckboxes();",
    "}",

    "function syncRawFromCheckboxes() {",
    "  var all = document.querySelectorAll('input[data-category]');",
    "  var selected = [];",
    "  var total = all.length;",
    "  for(var i = 0; i < all.length; i++) { if(all[i].checked) selected.push(all[i].getAttribute('data-category')); }",
    "  var input = document.getElementById('raw-pattern');",
    "  if(selected.length === total) { input.value = '*'; }",
    "  else if(selected.length === 0) { input.value = ''; }",
    "  else { input.value = selected.join(','); }",
    "}",

    "function syncCheckboxesFromRaw() {",
    "  var raw = document.getElementById('raw-pattern').value.trim();",
    "  var all = document.querySelectorAll('input[data-category]');",
    "  if(raw === '') { for(var i = 0; i < all.length; i++) { all[i].checked = false; } }",
    "  else {",
    "    var rawParts = raw.split(',').map(function(p) { return p.trim(); }).filter(function(p) { return p.length > 0; });",
    "    var hasWildcard = rawParts.indexOf('*') !== -1;",
    "    var includes = rawParts.filter(function(p) { return p !== '*' && p[0] !== '-'; });",
    "    var excludes = rawParts.filter(function(p) { return p[0] === '-'; }).map(function(p) { return p.substring(1); });",
    "    for(var i = 0; i < all.length; i++) {",
    "      var cat = all[i].getAttribute('data-category');",
    "      var excluded = false;",
    "      for(var j = 0; j < excludes.length; j++) {",
    "        if(cat === excludes[j] || cat.indexOf(excludes[j] + ':') === 0) { excluded = true; break; }",
    "      }",
    "      if(excluded) { all[i].checked = false; continue; }",
    "      if(hasWildcard) { all[i].checked = true; continue; }",
    "      var match = false;",
    "      for(var j = 0; j < includes.length; j++) {",
    "        if(cat === includes[j] || cat.indexOf(includes[j] + ':') === 0) { match = true; break; }",
    "      }",
    "      all[i].checked = match;",
    "    }",
    "  }",
    "  var prefixes = {};",
    "  for(var i = 0; i < all.length; i++) {",
    "    var g = all[i].getAttribute('data-group');",
    "    if(g) prefixes[g] = true;",
    "  }",
    "  for(var p in prefixes) { updateParentState(p); }",
    "}",

    "function applyPattern() {",
    "  var input = document.getElementById('raw-pattern');",
    "  document.getElementById('debug-form-pattern').value = input.value;",
    "  document.getElementById('debug-form').submit();",
    "}",

    "document.getElementById('raw-pattern').addEventListener('keydown', function(e) {",
    "  if(e.key === 'Enter') { e.preventDefault(); applyPattern(); }",
    "});",

    "document.getElementById('raw-pattern').addEventListener('input', function() {",
    "  syncCheckboxesFromRaw();",
    "});"

  ].join("\n");
}

/**
 * Generates the HTML body content for the debug page.
 * @returns HTML string for the page body.
 */
function generateDebugBody(): string {

  const currentPattern = getCurrentPattern();
  const groups = buildCategoryGroups();
  const parts: string[] = [];

  parts.push("<div class=\"debug-container\">");

  // Header.
  parts.push("<div class=\"debug-header\">");
  parts.push("<h1>Debug Logging</h1>");
  parts.push("<p>Select categories to enable debug output. Changes take effect immediately without restarting the server.</p>");
  parts.push("</div>");

  // Current status.
  parts.push("<div class=\"debug-status\">");
  parts.push("<div class=\"debug-status-label\">Current Filter</div>");
  parts.push(currentPattern ? escapeHtml(currentPattern) : "<em style=\"color: var(--text-muted);\">No debug categories enabled.</em>");
  parts.push("</div>");

  // Action buttons.
  parts.push("<div class=\"debug-actions\">");
  parts.push("<button type=\"button\" class=\"debug-btn-apply\" onclick=\"applyPattern()\">Apply</button>");
  parts.push("<button type=\"button\" class=\"debug-btn-secondary\" onclick=\"selectAll(true)\">Select All</button>");
  parts.push("<button type=\"button\" class=\"debug-btn-secondary\" onclick=\"selectAll(false)\">Deselect All</button>");
  parts.push("</div>");

  // Raw pattern input.
  parts.push("<div class=\"debug-raw\">");
  parts.push("<label for=\"raw-pattern\">PRISMCAST_DEBUG pattern</label>");
  parts.push("<input type=\"text\" id=\"raw-pattern\" value=\"" + escapeHtml(currentPattern) + "\"");
  parts.push(" placeholder=\"e.g. *,-streaming:ffmpeg or tuning:hulu,recovery\">");
  parts.push("<div class=\"debug-raw-hint\">Comma-separated. Use * for all, prefix with - to exclude.</div>");
  parts.push("</div>");

  // Category groups with checkboxes.
  parts.push("<div class=\"debug-groups\">");

  for(const group of groups) {

    const groupId = "group-" + group.prefix;
    const isSingleChild = (group.children.length === 1) && (group.children[0].category === group.prefix);

    parts.push("<div class=\"debug-group\">");

    if(isSingleChild) {

      // Standalone category (no colon, no sub-categories). Render as a single checkbox.
      const child = group.children[0];
      const checked = isCategoryEnabled(child.category) ? " checked" : "";

      parts.push("<div class=\"debug-group-header\">");
      parts.push("<input type=\"checkbox\" id=\"cat-" + escapeHtml(child.category) + "\" data-category=\"" + escapeHtml(child.category) + "\"");
      parts.push(" data-group=\"" + escapeHtml(group.prefix) + "\"" + checked);
      parts.push(" onchange=\"syncRawFromCheckboxes()\">");
      parts.push("<label for=\"cat-" + escapeHtml(child.category) + "\">" + escapeHtml(child.category) + "</label>");
      parts.push("<span class=\"debug-child-desc\">" + escapeHtml(child.description) + "</span>");
      parts.push("</div>");
    } else {

      // Group with children. Render parent checkbox and indented children.
      const allChecked = group.children.every((c) => isCategoryEnabled(c.category));
      const someChecked = group.children.some((c) => isCategoryEnabled(c.category));
      const parentChecked = allChecked ? " checked" : "";
      const parentIndeterminate = (!allChecked && someChecked) ? " data-indeterminate=\"true\"" : "";

      parts.push("<div class=\"debug-group-header\">");
      parts.push("<input type=\"checkbox\" id=\"" + groupId + "\"" + parentChecked + parentIndeterminate);
      parts.push(" onchange=\"onParentToggle('" + escapeHtml(group.prefix) + "')\">");
      parts.push("<label for=\"" + groupId + "\">" + escapeHtml(group.prefix) + "</label>");
      parts.push("</div>");

      parts.push("<div class=\"debug-group-children\">");

      for(const child of group.children) {

        const checked = isCategoryEnabled(child.category) ? " checked" : "";

        parts.push("<div class=\"debug-child\">");
        parts.push("<input type=\"checkbox\" id=\"cat-" + escapeHtml(child.category) + "\" data-category=\"" + escapeHtml(child.category) + "\"");
        parts.push(" data-group=\"" + escapeHtml(group.prefix) + "\"" + checked);
        parts.push(" onchange=\"onChildToggle('" + escapeHtml(group.prefix) + "')\">");
        parts.push("<label for=\"cat-" + escapeHtml(child.category) + "\">" + escapeHtml(child.category) + "</label>");
        parts.push("<span class=\"debug-child-desc\">" + escapeHtml(child.description) + "</span>");
        parts.push("</div>");
      }

      parts.push("</div>");
    }

    parts.push("</div>");
  }

  parts.push("</div>");

  // Hidden form for POST submission.
  parts.push("<form id=\"debug-form\" method=\"POST\" action=\"/debug\" style=\"display: none;\">");
  parts.push("<input type=\"hidden\" id=\"debug-form-pattern\" name=\"pattern\" value=\"\">");
  parts.push("</form>");

  parts.push("</div>");

  // Script to set initial indeterminate state on page load. The HTML checked attribute cannot express the indeterminate state, so we set it via JavaScript.
  const initScript = [
    "document.querySelectorAll('[data-indeterminate=\"true\"]').forEach(function(el) { el.indeterminate = true; });"
  ].join("\n");

  parts.push("<script>" + initScript + "\n" + generateDebugScript() + "</script>");

  return parts.join("\n");
}

// Endpoint Setup.

/**
 * Configures the /debug endpoint on the Express application.
 * @param app - The Express application.
 */
export function setupDebugEndpoint(app: Express): void {

  // GET /debug — Renders the debug category management page.
  app.get("/debug", (_req: Request, res: Response): void => {

    const html = generatePageWrapper(
      "Debug Logging",
      generateBaseStyles() + "\n" + generateDebugStyles(),
      generateDebugBody()
    );

    res.setHeader("Content-Type", "text/html");
    res.send(html);
  });

  // POST /debug — Applies a new debug filter pattern and redirects back to the page.
  app.post("/debug", (req: Request, res: Response): void => {

    const pattern = typeof req.body?.pattern === "string" ? req.body.pattern.trim() : "";
    const previousPattern = getCurrentPattern();

    initDebugFilter(pattern);

    LOG.info("Debug filter updated: \"%s\" -> \"%s\".", previousPattern, getCurrentPattern());

    res.redirect(303, "/debug");
  });
}
