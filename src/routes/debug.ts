/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * debug.ts: Debug logging configuration endpoint for PrismCast.
 */
import { DEBUG_CATEGORIES, LOG, escapeHtml, formatError, getCurrentPattern, initDebugFilter, isCategoryEnabled, serializeAttrs } from "../utils/index.ts";
import type { Express, Request, Response } from "express";
import { generateBaseStyles, generatePageWrapper } from "./ui.ts";
import { CONFIG } from "../config/index.ts";
import type { DebugCategory } from "../utils/index.ts";
import { getDebugEnv } from "../config/paths.ts";
import { mutateConfig } from "../config/userConfig.ts";

/* This module provides a hidden (undocumented) web page at /debug for runtime control of debug logging categories. Toggling a category enables or disables its
 * debug output immediately; the runtime filter is updated in place and the canonical form is persisted to config.json so the change survives restarts.
 *
 * The page is laid out as a vertical stack of cards (".debug-section"), each holding one or more rows (".debug-row"). The row is the only visual primitive on
 * the page, and every variant - group header, grouped leaf, standalone leaf - emits the same DOM shape; the variants differ only in which named tracks of a
 * shared CSS Grid each cell occupies. ".debug-section" owns the grid template; ".debug-row" inherits the column tracks via subgrid, so the column geometry
 * exists in exactly one CSS rule and every row participates in the same coordinate system.
 *
 * Identifier conventions (ID prefixes, data-attribute names) are hoisted to module constants and templated into both the server-side HTML emission and the
 * client-side script string. Renaming any of them is a one-line change and the server and client stay in lockstep. The client uses delegated change-event
 * handling, so the rendered HTML carries no inline onchange handlers - the only function-name surface between the two sides is the constants themselves.
 */

// Identifier Conventions.

/* ID prefixes, data-attribute names, and action values referenced by both the server-side renderer (when emitting markup) and the client-side script string
 * (when querying or matching against it). Both sides interpolate these constants rather than hard-coding the literals, so renaming any of them is a one-line
 * change and the server and client stay in lockstep.
 */

// HTML id prefixes: leaves and section parent toggles.
const LEAF_ID_PREFIX = "cat-";
const SECTION_ID_PREFIX = "group-";

// Data-attribute names carried by leaf and action elements.
const ATTR_ACTION = "data-debug-action";
const ATTR_CATEGORY = "data-category";
const ATTR_GROUP = "data-group";
const ATTR_INDETERMINATE = "data-indeterminate";

// Action values for the data-debug-action attribute on the action-bar buttons.
const ACTION_APPLY = "apply";
const ACTION_DESELECT_ALL = "deselect-all";
const ACTION_SELECT_ALL = "select-all";

// Types.

/**
 * A discriminated union describing one top-level entry on the /debug page. A "section" wraps one or more namespaced leaves under a shared prefix (e.g., the
 * "browser" section holds "browser:lifecycle" and "browser:video"). A "standalone" is a single namespaceless leaf (e.g., "cdp", "precache", "retry") rendered
 * as its own one-row card. The view model is built once per render from the flat DEBUG_CATEGORIES list; the renderer is an exhaustive switch over this union,
 * so the prefix-vs-standalone classification lives in exactly one place.
 */
type CategoryNode =
  { readonly kind: "section"; readonly leaves: readonly DebugCategory[]; readonly prefix: string } |
  { readonly kind: "standalone"; readonly leaf: DebugCategory };

/* The three visual row shapes. "leaf" is the default (grouped child); "header" is the section's parent toggle; "standalone" is a namespaceless category
 * rendered as its own bold row. Variant-specific styling is applied via a CSS modifier class derived from this discriminator.
 */
type RowVariant = "header" | "leaf" | "standalone";

/**
 * A pure data description of one row on the page. The renderer takes this and produces the HTML; all escaping happens inside the renderer so callers pass raw
 * values. This is the single shape every row variant funnels through, which is what makes the four-track grid template on ".debug-section" the sole source of
 * truth for column alignment - there is no second markup path that could drift.
 */
interface RowSpec {

  readonly description: string;
  readonly inputAttrs: Record<string, string | boolean | undefined>;
  readonly inputId: string;
  readonly labelText: string;
  readonly variant: RowVariant;
}

// Helpers.

/**
 * Builds the view model from the flat DEBUG_CATEGORIES list. Categories with a colon are grouped under the substring preceding the colon; categories without
 * a colon become standalone leaves. The resulting list is sorted alphabetically by display key (section prefix for groups, category name for standalones) so
 * the page renders deterministically regardless of registry insertion order.
 * @returns Sorted array of top-level category nodes.
 */
function buildCategoryNodes(): readonly CategoryNode[] {

  const sections = new Map<string, DebugCategory[]>();
  const standalones: DebugCategory[] = [];

  for(const entry of DEBUG_CATEGORIES) {

    const colonIndex = entry.category.indexOf(":");

    if(colonIndex === -1) {

      standalones.push({ category: entry.category, description: entry.description });

      continue;
    }

    const prefix = entry.category.substring(0, colonIndex);
    let leaves = sections.get(prefix);

    if(!leaves) {

      leaves = [];
      sections.set(prefix, leaves);
    }

    leaves.push({ category: entry.category, description: entry.description });
  }

  const nodes: CategoryNode[] = [];

  for(const [ prefix, leaves ] of sections) {

    leaves.sort((a, b) => a.category.localeCompare(b.category));
    nodes.push({ kind: "section", leaves, prefix });
  }

  for(const leaf of standalones) {

    nodes.push({ kind: "standalone", leaf });
  }

  nodes.sort((a, b) => {

    const aKey = (a.kind === "section") ? a.prefix : a.leaf.category;
    const bKey = (b.kind === "section") ? b.prefix : b.leaf.category;

    return aKey.localeCompare(bKey);
  });

  return nodes;
}

/**
 * Generates the page-specific CSS styles for the debug endpoint. Layout is owned by ".debug-section" - a CSS Grid with four named tracks. Each ".debug-row"
 * inherits those tracks via subgrid, so the column geometry exists in exactly one rule and every row participates in the same coordinate system. Variants
 * (header, leaf, standalone) place their cells into the appropriate named tracks via modifier classes; description-column alignment falls out of the grid
 * template itself rather than depending on per-row CSS staying in sync.
 * @returns CSS string for the debug page.
 */
function generateDebugStyles(): string {

  return [

    // Column geometry. These five variables are the sole source of truth for the grid's column widths and gaps; the rules below reference them rather than
    // restating numeric values, so tuning the layout means editing this one line. Scoped to ".debug-container" - the only consumer subtree - so they don't
    // leak into the rest of the document's cascade.
    ".debug-container { max-width: 800px; margin: 0 auto; padding: 24px;",
    "  --debug-indent-col: 28px; --debug-checkbox-col: 14px; --debug-label-col: 11rem; --debug-col-gap: 8px; --debug-row-gap: 6px; }",
    ".debug-header { margin-bottom: 24px; }",
    ".debug-header h1 { margin: 0 0 8px 0; font-size: 1.5rem; color: var(--text-heading); }",
    ".debug-header p { margin: 0; color: var(--text-secondary); font-size: 0.9rem; }",

    // Current status banner.
    ".debug-status { background: var(--surface-elevated); border: 1px solid var(--border-default); border-radius: 8px; padding: 12px 16px;",
    "  margin-bottom: 24px; font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace; font-size: 0.85rem; color: var(--text-primary);",
    "  word-break: break-all; }",
    ".debug-status-label { color: var(--text-muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }",

    // Section cards. ".debug-section" is the grid container that owns the four-track column template; every row inside it inherits these tracks via subgrid,
    // so the column x-positions are computed once per card and shared across all of the card's rows.
    ".debug-pane { display: flex; flex-direction: column; gap: 16px; margin-bottom: 24px; }",
    ".debug-section { background: var(--surface-elevated); border: 1px solid var(--border-default); border-radius: 8px; padding: 16px;",
    "  display: grid; column-gap: var(--debug-col-gap); row-gap: var(--debug-row-gap);",
    "  grid-template-columns: [indent] var(--debug-indent-col) [checkbox] var(--debug-checkbox-col) [label] var(--debug-label-col) [desc] 1fr; }",

    // The row template. Rows span all four section columns and inherit those tracks via "grid-template-columns: subgrid", so every cell across every row
    // (across every section) lives in the same coordinate system. There is no second column declaration anywhere.
    ".debug-row { display: grid; grid-column: 1 / -1; grid-template-columns: subgrid; align-items: baseline; }",

    // Default cell placement is the grouped-leaf shape: checkbox in [checkbox], label in [label], description in [desc]. Visually this is the indented child
    // sitting under a parent header row inside the same section card.
    ".debug-row > input[type=\"checkbox\"] { grid-column: checkbox; }",
    ".debug-row > label { grid-column: label; cursor: pointer; font-size: 0.9rem; color: var(--text-primary); }",
    ".debug-row > .debug-row__desc { grid-column: desc; color: var(--text-muted); font-size: 0.8rem; }",

    // Header and standalone variants share the same column slotting: the checkbox sits in [indent] (visually at the section's left edge), and the label spans
    // the [checkbox] and [label] tracks so the bold prefix label can stretch wider than a regular leaf label. The description, when present (standalone only -
    // header rows leave it empty), still lands in [desc], which is what aligns it with every other description on the page.
    ".debug-row--header > input[type=\"checkbox\"], .debug-row--standalone > input[type=\"checkbox\"] { grid-column: indent; }",
    ".debug-row--header > label, .debug-row--standalone > label { grid-column: checkbox / desc; font-weight: 600; font-size: 0.95rem;",
    "  color: var(--text-heading); }",

    // Checkbox styling. Shared by every checkbox on the page (group headers, leaves, standalones).
    "input[type='checkbox'] { cursor: pointer; accent-color: var(--interactive-primary); position: relative; top: 2px; }",

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
    ".debug-raw .debug-raw-hint { font-size: 0.75rem; color: var(--text-muted); margin-top: 4px; }",

    // Environment variable override warning.
    ".debug-env-warning { background: var(--status-warning-bg); border: 1px solid var(--status-warning-border); border-radius: 8px; padding: 12px 16px;",
    "  margin-bottom: 24px; font-size: 0.85rem; color: var(--status-warning-text); line-height: 1.5; }",
    ".debug-env-warning code { font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace; background: rgba(128, 128, 128, 0.15);",
    "  padding: 1px 5px; border-radius: 3px; font-size: 0.8rem; }"

  ].join("\n");
}

/**
 * Generates the client-side JavaScript for checkbox and action-button behavior. Two delegated listeners cover the whole page: one "change" listener on
 * ".debug-pane" dispatches checkbox events by identity (id prefix vs. presence of the group data-attribute), and one "click" listener on ".debug-actions"
 * dispatches button clicks by the data-debug-action attribute. The rendered HTML therefore carries no inline event handlers, so the JS function names are
 * only referenced from inside this script. The ID prefixes, attribute names, and action values are templated in from the module constants above, so the
 * server-side renderer and the client-side script cannot drift.
 * @returns JavaScript string (without script tags).
 */
function generateDebugScript(): string {

  return [

    // Initial state. The HTML "checked" attribute cannot express the indeterminate state, so we apply it from JS once the rows have rendered. Matches by
    // attribute presence (no value), aligned with the HTML5 boolean-attribute form the renderer emits.
    "for(const el of document.querySelectorAll('[" + ATTR_INDETERMINATE + "]')) el.indeterminate = true;",

    "function updateParentState(prefix) {",
    "  const parent = document.getElementById('" + SECTION_ID_PREFIX + "' + prefix);",
    "  if(!parent) return;",
    "  const children = document.querySelectorAll('input[" + ATTR_GROUP + "=\"' + prefix + '\"]');",
    "  const checked = Array.from(children).filter((child) => child.checked).length;",
    "  parent.checked = (checked === children.length);",
    "  parent.indeterminate = (checked > 0) && (checked < children.length);",
    "}",

    "function selectAll(checked) {",
    "  const boxes = document.querySelectorAll('input[type=\"checkbox\"]');",
    "  for(const box of boxes) { box.checked = checked; box.indeterminate = false; }",
    "  syncRawFromCheckboxes();",
    "}",

    "function syncRawFromCheckboxes() {",
    "  const all = document.querySelectorAll('input[" + ATTR_CATEGORY + "]');",
    "  const selected = Array.from(all).filter((cb) => cb.checked).map((cb) => cb.getAttribute('" + ATTR_CATEGORY + "'));",
    "  const input = document.getElementById('raw-pattern');",
    "  if(selected.length === all.length) { input.value = '*'; }",
    "  else if(selected.length === 0) { input.value = ''; }",
    "  else { input.value = selected.join(','); }",
    "}",

    "function syncCheckboxesFromRaw() {",
    "  const raw = document.getElementById('raw-pattern').value.trim();",
    "  const all = document.querySelectorAll('input[" + ATTR_CATEGORY + "]');",
    "  if(raw === '') {",
    "    for(const cb of all) cb.checked = false;",
    "  } else {",
    "    const rawParts = raw.split(',').map((p) => p.trim()).filter((p) => p.length > 0);",
    "    const hasWildcard = rawParts.includes('*');",
    "    const includes = rawParts.filter((p) => (p !== '*') && (p[0] !== '-'));",
    "    const excludes = rawParts.filter((p) => p[0] === '-').map((p) => p.substring(1));",
    "    for(const cb of all) {",
    "      const cat = cb.getAttribute('" + ATTR_CATEGORY + "');",
    "      const isExcluded = excludes.some((ex) => (cat === ex) || cat.startsWith(ex + ':'));",
    "      if(isExcluded) { cb.checked = false; continue; }",
    "      if(hasWildcard) { cb.checked = true; continue; }",
    "      cb.checked = includes.some((inc) => (cat === inc) || cat.startsWith(inc + ':'));",
    "    }",
    "  }",
    "  const prefixes = new Set();",
    "  for(const cb of all) {",
    "    const g = cb.getAttribute('" + ATTR_GROUP + "');",
    "    if(g) prefixes.add(g);",
    "  }",
    "  for(const p of prefixes) updateParentState(p);",
    "}",

    "function applyPattern() {",
    "  const input = document.getElementById('raw-pattern');",
    "  document.getElementById('debug-form-pattern').value = input.value;",
    "  document.getElementById('debug-form').submit();",
    "}",

    // Delegated change listener for every checkbox on the page. The handler dispatches on identity: id starting with the section prefix means a parent
    // toggle, presence of the group data-attribute means a grouped child, neither means a standalone. Any change syncs the raw pattern input afterward.
    "const pane = document.querySelector('.debug-pane');",
    "pane.addEventListener('change', (e) => {",
    "  const target = e.target;",
    "  if((target.tagName !== 'INPUT') || (target.type !== 'checkbox')) return;",
    "  if(target.id.startsWith('" + SECTION_ID_PREFIX + "')) {",
    "    const prefix = target.id.substring(" + String(SECTION_ID_PREFIX.length) + ");",
    "    const children = document.querySelectorAll('input[" + ATTR_GROUP + "=\"' + prefix + '\"]');",
    "    for(const child of children) child.checked = target.checked;",
    "    target.indeterminate = false;",
    "  } else if(target.hasAttribute('" + ATTR_GROUP + "')) {",
    "    updateParentState(target.getAttribute('" + ATTR_GROUP + "'));",
    "  }",
    "  syncRawFromCheckboxes();",
    "});",

    // Delegated click listener for the action bar. Buttons declare their intent via data-debug-action; the handler closest()-walks to find the nearest
    // ancestor carrying the attribute, so clicks landing on child elements of a button still resolve correctly.
    "const actions = document.querySelector('.debug-actions');",
    "actions.addEventListener('click', (e) => {",
    "  const button = e.target.closest('[" + ATTR_ACTION + "]');",
    "  if(!button) return;",
    "  switch(button.getAttribute('" + ATTR_ACTION + "')) {",
    "    case '" + ACTION_APPLY + "': applyPattern(); break;",
    "    case '" + ACTION_SELECT_ALL + "': selectAll(true); break;",
    "    case '" + ACTION_DESELECT_ALL + "': selectAll(false); break;",
    "  }",
    "});",

    "const rawPattern = document.getElementById('raw-pattern');",
    "rawPattern.addEventListener('keydown', (e) => { if(e.key === 'Enter') { e.preventDefault(); applyPattern(); } });",
    "rawPattern.addEventListener('input', syncCheckboxesFromRaw);"

  ].join("\n");
}

/**
 * Emits one row of the page from a typed RowSpec. Every visual variant (group header, grouped leaf, standalone leaf) flows through this single template; the
 * variant's modifier class and per-cell content are all that change between calls. All HTML escaping happens here, so the variant builders pass raw values
 * and stay declarative.
 * @param spec - Row data description.
 * @returns The row HTML.
 */
function renderRow(spec: RowSpec): string {

  const variantSuffix = (spec.variant === "leaf") ? "" : " debug-row--" + spec.variant;
  const escapedId = escapeHtml(spec.inputId);

  return [

    "<div class=\"debug-row" + variantSuffix + "\">",
    "<input type=\"checkbox\" id=\"" + escapedId + "\"" + serializeAttrs(spec.inputAttrs) + ">",
    "<label for=\"" + escapedId + "\">" + escapeHtml(spec.labelText) + "</label>",
    "<span class=\"debug-row__desc\">" + escapeHtml(spec.description) + "</span>",
    "</div>"

  ].join("\n");
}

/**
 * Renders a section's parent-toggle header row. The checkbox toggles every leaf below it via the delegated change handler; its checked/indeterminate state
 * mirrors how many children are currently enabled.
 * @param prefix - The namespace prefix shared by the section's leaves (also the displayed label text).
 * @param allChecked - True when every child leaf is currently enabled.
 * @param someChecked - True when at least one child leaf is currently enabled; combined with !allChecked this surfaces as the checkbox's indeterminate state.
 * @returns The header row HTML.
 */
function renderHeaderRow(prefix: string, allChecked: boolean, someChecked: boolean): string {

  return renderRow({

    description: "",
    inputAttrs: {

      // HTML5 boolean attribute form: emit a bare "data-indeterminate" (no value) when the row is partially checked, otherwise omit the attribute entirely.
      // The init script in the script blob below matches on presence ([data-indeterminate]), so the "true" literal lives in exactly zero places.
      [ATTR_INDETERMINATE]: (!allChecked) && someChecked,
      checked: allChecked
    },
    inputId: SECTION_ID_PREFIX + prefix,
    labelText: prefix,
    variant: "header"
  });
}

/**
 * Renders a grouped leaf row inside a section. The leaf carries data-group so the delegated handler can route the change to the section's parent header at
 * "group-<prefix>".
 * @param prefix - The section prefix owning this leaf.
 * @param leaf - The leaf category to render.
 * @returns The leaf row HTML.
 */
function renderLeafRow(prefix: string, leaf: DebugCategory): string {

  return renderRow({

    description: leaf.description,
    inputAttrs: {

      [ATTR_CATEGORY]: leaf.category,
      [ATTR_GROUP]: prefix,
      checked: isCategoryEnabled(leaf.category)
    },
    inputId: LEAF_ID_PREFIX + leaf.category,
    labelText: leaf.category,
    variant: "leaf"
  });
}

/**
 * Renders a standalone (namespaceless) leaf row. Standalones have no parent group, so the row carries no data-group; the modifier class makes it visually
 * match a section header (bold, left-aligned at the section's edge) while the description still lands in the shared [desc] track, so it aligns with every
 * other description on the page.
 * @param leaf - The standalone leaf category to render.
 * @returns The standalone row HTML.
 */
function renderStandaloneRow(leaf: DebugCategory): string {

  return renderRow({

    description: leaf.description,
    inputAttrs: {

      [ATTR_CATEGORY]: leaf.category,
      checked: isCategoryEnabled(leaf.category)
    },
    inputId: LEAF_ID_PREFIX + leaf.category,
    labelText: leaf.category,
    variant: "standalone"
  });
}

/**
 * Generates the HTML body content for the debug page.
 * @returns HTML string for the page body.
 */
function generateDebugBody(): string {

  const currentPattern = getCurrentPattern();
  const nodes = buildCategoryNodes();
  const parts: string[] = [];

  parts.push("<div class=\"debug-container\">");

  // Header.
  parts.push("<div class=\"debug-header\">");
  parts.push("<h1>Debug Logging</h1>");
  parts.push("<p>Select categories to enable debug output. Changes take effect immediately and are saved across restarts.</p>");
  parts.push("</div>");

  // Environment variable override warning. When PRISMCAST_DEBUG is set, the env var takes precedence at startup. Changes from the UI are still saved to
  // config.json for when the env var is removed.
  const debugEnv = getDebugEnv();

  if(debugEnv) {

    parts.push("<div class=\"debug-env-warning\">");
    parts.push("<strong>PRISMCAST_DEBUG environment variable is active:</strong> <code>" + escapeHtml(debugEnv) + "</code><br>");
    parts.push("Changes below will be saved to config.json but the environment variable takes precedence at startup. ");
    parts.push("Remove PRISMCAST_DEBUG to use the saved filter.");
    parts.push("</div>");
  }

  // Current status.
  parts.push("<div class=\"debug-status\">");
  parts.push("<div class=\"debug-status-label\">Current Filter</div>");
  parts.push(currentPattern ? escapeHtml(currentPattern) : "<em style=\"color: var(--text-muted);\">No debug categories enabled.</em>");
  parts.push("</div>");

  // Action buttons. Each entry carries the semantic intent ("primary" vs. the default secondary visual treatment), the action identifier dispatched by the
  // delegated click listener on ".debug-actions", and the display label. The "primary"-to-class-name mapping lives in exactly one place below, so changing
  // the CSS class scheme is a one-line edit rather than three. No inline onclick handlers, so the JS function names are not referenced from the HTML.
  const actionButtons: readonly { readonly action: string; readonly label: string; readonly primary?: boolean }[] = [

    { action: ACTION_APPLY, label: "Apply", primary: true },
    { action: ACTION_SELECT_ALL, label: "Select All" },
    { action: ACTION_DESELECT_ALL, label: "Deselect All" }
  ];

  parts.push("<div class=\"debug-actions\">");

  for(const button of actionButtons) {

    // Routed through serializeAttrs so the leading-space contract, HTML escaping, and conditional emission live in exactly one place - same shape the row
    // renderer uses below. Keys are listed in the source-name alphabetical order the project's sort-keys rule expects; serializeAttrs emits in insertion
    // order, so the rendered output mirrors that ordering and tests assert on attribute presence rather than positional layout.
    parts.push("<button" + serializeAttrs({

      [ATTR_ACTION]: button.action,
      "class": button.primary ? "debug-btn-apply" : "debug-btn-secondary",
      type: "button"
    }) + ">" + escapeHtml(button.label) + "</button>");
  }

  parts.push("</div>");

  // Raw pattern input.
  parts.push("<div class=\"debug-raw\">");
  parts.push("<label for=\"raw-pattern\">PRISMCAST_DEBUG pattern</label>");
  parts.push("<input type=\"text\" id=\"raw-pattern\" value=\"" + escapeHtml(currentPattern) + "\"");
  parts.push(" placeholder=\"e.g. *,-streaming:ffmpeg or tuning:hulu,recovery\">");
  parts.push("<div class=\"debug-raw-hint\">Comma-separated. Use * for all, prefix with - to exclude.</div>");
  parts.push("</div>");

  // Category sections. Every top-level CategoryNode produces one ".debug-section" card; sections expand into a header row plus one leaf row per child,
  // standalones into a single row. The renderer is an exhaustive switch over the discriminated union, so adding a new variant means adding a case here and
  // a small builder above - no branch in the HTML emission itself.
  parts.push("<div class=\"debug-pane\">");

  for(const node of nodes) {

    parts.push("<section class=\"debug-section\">");

    switch(node.kind) {

      case "section": {

        const allChecked = node.leaves.every((leaf) => isCategoryEnabled(leaf.category));
        const someChecked = node.leaves.some((leaf) => isCategoryEnabled(leaf.category));

        parts.push(renderHeaderRow(node.prefix, allChecked, someChecked));

        for(const leaf of node.leaves) {

          parts.push(renderLeafRow(node.prefix, leaf));
        }

        break;
      }

      case "standalone": {

        parts.push(renderStandaloneRow(node.leaf));

        break;
      }
    }

    parts.push("</section>");
  }

  parts.push("</div>");

  // Hidden form for POST submission.
  parts.push("<form id=\"debug-form\" method=\"POST\" action=\"/debug\" style=\"display: none;\">");
  parts.push("<input type=\"hidden\" id=\"debug-form-pattern\" name=\"pattern\" value=\"\">");
  parts.push("</form>");

  parts.push("</div>");

  parts.push("<script>" + generateDebugScript() + "</script>");

  return parts.join("\n");
}

// Endpoint Setup.

/**
 * Configures the /debug endpoint on the Express application.
 * @param app - The Express application.
 */
export function setupDebugEndpoint(app: Express): void {

  // GET /debug - Renders the debug category management page.
  app.get("/debug", (_req: Request, res: Response): void => {

    const html = generatePageWrapper(
      "Debug Logging",
      generateBaseStyles() + "\n" + generateDebugStyles(),
      generateDebugBody()
    );

    res.setHeader("Content-Type", "text/html");
    res.send(html);
  });

  // POST /debug - Applies a new debug filter pattern, persists it to config.json, and redirects back to the page.
  app.post("/debug", async (req: Request, res: Response): Promise<void> => {

    const body = req.body as Record<string, unknown>;
    const pattern = typeof body["pattern"] === "string" ? body["pattern"].trim() : "";
    const previousPattern = getCurrentPattern();

    // Apply the filter immediately at runtime.
    initDebugFilter(pattern);

    // Use the canonical form after parsing. initDebugFilter normalizes whitespace around commas, so "tuning:hulu, recovery" becomes "tuning:hulu,recovery".
    // Storing the normalized form ensures consistent comparisons at startup.
    const normalizedPattern = getCurrentPattern();

    // Keep the in-memory CONFIG consistent with the persisted value.
    CONFIG.logging.debugFilter = normalizedPattern;

    LOG.info("Debug filter updated: \"%s\" -> \"%s\".", previousPattern, normalizedPattern);

    // Persist to config.json so the filter survives restarts. Wrap in try/catch so persistence failure doesn't break the runtime update.
    try {

      await mutateConfig((config) => {

        config.logging ??= {};
        config.logging.debugFilter = normalizedPattern;
      });
    } catch(error) {

      LOG.warn("Failed to persist debug filter to config.json: %s.", formatError(error));
    }

    res.redirect(303, "/debug");
  });
}
