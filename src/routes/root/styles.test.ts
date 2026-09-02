/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * styles.test.ts: Unit tests for the landing page CSS generator. The module exports a single function that produces a long string of CSS rules. We do not run
 * the CSS through a parser (that would add a dependency) - instead we lock in these structural rules: the output is a non-empty string, the major class
 * groups (header, wizard modal, channel table, toast, etc.) are present, theme variables drive colors rather than literal hex codes for most rules, and the
 * generator stays composed from the OPTIONAL_COLUMNS source of truth.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../../testing.helpers.ts";
import { generateLandingPageStyles } from "./styles.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("generateLandingPageStyles", () => {

  test("returns a non-empty string longer than a trivial template would produce", () => {

    // A non-trivial style block is many kilobytes; 1000 chars is a comfortable lower bound that catches an empty or accidentally-truncated output.
    const css = generateLandingPageStyles();

    assert.equal(typeof css, "string", "result should be a string");
    assert.ok(css.length > 1000, "CSS should be substantial; got " + String(css.length) + " characters");
  });

  test("does not contain template-literal artifacts from missing values", () => {

    // The generator builds with array.join, not template literals, so neither artifact should ever appear. Both are red flags for stringifying a missing
    // import (e.g., interpolating an undefined OPTIONAL_COLUMNS field).
    const css = generateLandingPageStyles();

    assert.doesNotMatch(css, /\$\{undefined\}/, "no ${undefined} interpolation");
    assert.doesNotMatch(css, /\[object Object\]/, "no [object Object] coercion");
  });

  test("includes the header, header-status, and version-container rule blocks", () => {

    // These three class groups establish the page-level chrome layout. Their absence would mean the header section was dropped during a refactor.
    const css = generateLandingPageStyles();

    assert.match(css, /\.header\s*\{/, "header rule");
    assert.match(css, /\.header-status\s*\{/, "header-status rule");
    assert.match(css, /\.version-container\s*\{/, "version-container rule");
  });

  test("includes the wizard modal class hierarchy", () => {

    // Wizard modals share a documented shell architecture (.wizard-modal -> .wizard-modal-content -> .wizard-header etc.). The shell builder relies on these
    // classes being styled. Locking the four core classes keeps a refactor from removing one and silently breaking modal layout.
    const css = generateLandingPageStyles();

    assert.match(css, /\.wizard-modal\s*\{/);
    assert.match(css, /\.wizard-modal-content\s*\{/);
    assert.match(css, /\.wizard-header\s*\{/);
    assert.match(css, /\.wizard-content\s*\{/);
  });

  test("includes the channel table class hierarchy and wrapper", () => {

    const css = generateLandingPageStyles();

    assert.match(css, /\.channel-table-wrapper\s*\{/, "wrapper exists");
    assert.match(css, /\.channel-table\s*\{/, "table base rule");
    assert.match(css, /\.channel-table\s+th/, "header cell rule");
  });

  test("includes streams table styling for the overview tab", () => {

    const css = generateLandingPageStyles();

    assert.match(css, /\.streams-table\s*\{/);
    assert.match(css, /#streams-container/);
  });

  test("includes log viewer styling with dark theme variables", () => {

    const css = generateLandingPageStyles();

    assert.match(css, /\.log-viewer\s*\{/);
    assert.match(css, /\.log-error\b/);
    assert.match(css, /\.log-warn\b/);
  });

  test("includes toast notification animations and variants", () => {

    // Toasts have four type variants (success/error/warning/info) plus slide-in/out animations. All four variants should be present.
    const css = generateLandingPageStyles();

    assert.match(css, /\.toast\s*\{/);
    assert.match(css, /\.toast\.success/);
    assert.match(css, /\.toast\.error/);
    assert.match(css, /\.toast\.warning/);
    assert.match(css, /\.toast\.info/);
    assert.match(css, /@keyframes\s+toastIn/);
    assert.match(css, /@keyframes\s+toastOut/);
  });

  test("preserves the login icon's needs-sign-in red on hover", () => {

    /* The generic .btn-icon-login:hover rule recolors the icon with the interactive-primary color. The needs-sign-in state must survive hover (matching the
     * .btn-icon-health precedent), so a more specific compound rule locks in the delete/red variable. Without this rule, hovering a flagged channel's login icon
     * would flash it back to the neutral interactive color and hide the state the icon exists to surface.
     */
    const css = generateLandingPageStyles();

    assert.match(css, /\.btn-icon-login\.health-failed:hover\s*\{\s*color:\s*var\(--interactive-delete\);\s*\}/, "hover-preservation rule present");
  });

  test("includes login modal and restart modal styling", () => {

    const css = generateLandingPageStyles();

    assert.match(css, /\.login-modal\s*\{/);
    assert.match(css, /\.restart-modal\s*\{/);
  });

  test("includes the spin keyframe used by the version-check refresh animation", () => {

    // The version-check button rotates while a manual update check is in flight, driven by @keyframes spin.
    const css = generateLandingPageStyles();

    assert.match(css, /@keyframes\s+spin/);
  });

  test("includes responsive media queries for narrow viewports", () => {

    // The source defines at least two @media (max-width: 768px) blocks (column hiding, toolbar stacking, toast-container layout), so the assertion floors at two.
    const css = generateLandingPageStyles();

    const mediaQueries = css.match(/@media\s*\(max-width:\s*768px\)/g) ?? [];

    assert.ok(mediaQueries.length >= 2, "should have at least two 768px breakpoint blocks; got " + String(mediaQueries.length));
  });

  test("uses CSS custom properties from the theme system rather than literal colors", () => {

    // The styles module documents theme support via var(--*). A spot check confirms several theme variables are referenced; hard-coded hex would mean a theme
    // override wouldn't apply.
    const css = generateLandingPageStyles();

    assert.match(css, /var\(--text-muted\)/);
    assert.match(css, /var\(--text-primary\)/);
    assert.match(css, /var\(--border-default\)/);
    assert.match(css, /var\(--surface-overlay\)/);
  });

  test("emits per-column rules generated from OPTIONAL_COLUMNS", () => {

    // The styles module flat-maps OPTIONAL_COLUMNS into channel-table column rules. We verify the shape (.channel-table .col-* with min-width) is present
    // multiple times rather than asserting on specific column names, since OPTIONAL_COLUMNS may grow without invalidating the test.
    const css = generateLandingPageStyles();

    const columnRules = css.match(/\.channel-table\s+\.col-[\w-]+\s*\{[^}]*min-width:/g) ?? [];

    assert.ok(columnRules.length >= 1, "expected at least one OPTIONAL_COLUMNS-driven column rule; got " + String(columnRules.length));
  });

  test("emits hide-* rules paired with each OPTIONAL_COLUMNS entry", () => {

    // The flat-map produces both a min-width rule and a paired hide-* rule. The hide rule drives column visibility toggling. If one rule shape is missing the
    // generator regressed.
    const css = generateLandingPageStyles();

    const hideRules = css.match(/\.channel-table\.hide-[\w-]+\s+\.col-[\w-]+\s*\{\s*display:\s*none/g) ?? [];

    assert.ok(hideRules.length >= 1, "expected paired hide- rules from OPTIONAL_COLUMNS; got " + String(hideRules.length));
  });

  test("includes provider toolbar, chip, and dropdown rules used by the channel filter", () => {

    const css = generateLandingPageStyles();

    assert.match(css, /\.provider-toolbar\s*\{/);
    assert.match(css, /\.provider-chip\s*\{/);
    assert.match(css, /\.provider-dropdown-menu\s*\{/);
  });

  test("includes browse-channel-list styling for the channel browse modal", () => {

    const css = generateLandingPageStyles();

    assert.match(css, /\.browse-channel-list\s*\{/);
    assert.match(css, /\.browse-toolbar\s*\{/);
    assert.match(css, /\.browse-tier-badge/);
  });

  test("returns identical output across calls", () => {

    // The function should be a pure derivation from OPTIONAL_COLUMNS. Two calls produce byte-identical output; locking this catches any accidental Date.now()
    // or other nondeterministic input creep.
    assert.equal(generateLandingPageStyles(), generateLandingPageStyles());
  });

  test("opens and closes braces in matched pairs", () => {

    // Crude structural check - count opening vs. closing braces. A mismatched pair would mean the CSS will not parse correctly in the browser. This is a cheap
    // syntactic rule that does not require a full CSS parser.
    const css = generateLandingPageStyles();
    const opens = (css.match(/\{/g) ?? []).length;
    const closes = (css.match(/\}/g) ?? []).length;

    assert.equal(opens, closes, "brace count mismatch (opens=" + String(opens) + ", closes=" + String(closes) + ")");
  });
});
