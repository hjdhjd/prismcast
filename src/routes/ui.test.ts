/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ui.test.ts: Unit tests for the shared UI building blocks in ui.ts. The exports come in two groups: pure CSS/HTML/JS string generators (generateBaseStyles,
 * generateTabStyles, generateTabScript, generateTabButton, generateTabPanel, generatePageWrapper) and a barrel re-export of components.ts. The tests verify
 * structural invariants (key class names, balanced braces, hash navigation hooks) and the conditional fork inside generateTabScript that wires the optional
 * hideElementOnTab feature.
 */
import { describe, test } from "node:test";
import { generateBaseStyles, generatePageWrapper, generateTabButton, generateTabPanel, generateTabScript, generateTabStyles } from "./ui.ts";
import assert from "node:assert/strict";

describe("generateBaseStyles", () => {

  test("returns a non-empty CSS string", () => {

    const css = generateBaseStyles();

    assert.equal(typeof css, "string");
    assert.ok(css.length > 0);
  });

  test("declares core layout selectors (body, headings, links)", () => {

    const css = generateBaseStyles();

    assert.match(css, /body \{/);
    assert.match(css, /h1 \{/);
    assert.match(css, /h2 \{/);
    assert.match(css, /h3 \{/);
    assert.match(css, /a \{/);
  });

  test("declares the alert and badge component classes", () => {

    const css = generateBaseStyles();

    assert.match(css, /\.alert \{/);
    assert.match(css, /\.alert-success/);
    assert.match(css, /\.alert-error/);
    assert.match(css, /\.alert-warning/);
    assert.match(css, /\.badge \{/);
  });

  test("declares button variant classes", () => {

    const css = generateBaseStyles();

    for(const variant of [ "primary", "secondary", "danger", "success", "edit", "delete" ]) {

      assert.match(css, new RegExp("\\.btn-" + variant), "should declare .btn-" + variant);
    }
  });

  test("declares form input/select/checkbox classes", () => {

    const css = generateBaseStyles();

    assert.match(css, /\.form-input \{/);
    assert.match(css, /\.form-select \{/);
    assert.match(css, /\.form-checkbox \{/);
  });

  test("uses var(--*) references throughout (theme-aware)", () => {

    // Negative test: hard-coded colors would defeat dark-mode support. We sample a few known variables to ensure the convention is followed.
    const css = generateBaseStyles();

    assert.match(css, /var\(--text-primary\)/);
    assert.match(css, /var\(--surface-page\)/);
    assert.match(css, /var\(--interactive-primary\)/);
  });

  test("contains balanced braces", () => {

    const css = generateBaseStyles();
    const opens = (css.match(/\{/g) ?? []).length;
    const closes = (css.match(/\}/g) ?? []).length;

    assert.equal(opens, closes);
  });

  test("produces stable output across two calls", () => {

    assert.equal(generateBaseStyles(), generateBaseStyles());
  });
});

describe("generateTabStyles", () => {

  test("declares the tab-bar, tab-btn, and tab-panel classes", () => {

    const css = generateTabStyles();

    assert.match(css, /\.tab-bar \{/);
    assert.match(css, /\.tab-btn \{/);
    assert.match(css, /\.tab-panel \{/);
  });

  test("declares the active and has-error tab states", () => {

    const css = generateTabStyles();

    assert.match(css, /\.tab-btn\.active/);
    assert.match(css, /\.tab-btn\.has-error/);
    assert.match(css, /\.tab-panel\.active/);
  });

  test("contains balanced braces", () => {

    const css = generateTabStyles();
    const opens = (css.match(/\{/g) ?? []).length;
    const closes = (css.match(/\}/g) ?? []).length;

    assert.equal(opens, closes);
  });
});

describe("generateTabScript", () => {

  test("wraps the script in <script> tags", () => {

    const js = generateTabScript();

    assert.match(js, /^<script>/);
    assert.match(js, /<\/script>$/);
  });

  test("uses the default localStorage key when none is provided", () => {

    // Boundary: omitting localStorageKey defaults to "prismcast-tab".
    const js = generateTabScript();

    assert.match(js, /'prismcast-tab'/, "should reference the default localStorage key");
  });

  test("uses the custom localStorage key when provided", () => {

    const js = generateTabScript({ localStorageKey: "my-key" });

    assert.match(js, /'my-key'/);
    assert.doesNotMatch(js, /'prismcast-tab'/);
  });

  test("includes hash parsing and tab switching logic", () => {

    // The script must wire hash navigation, click handlers, and keyboard navigation. We sample identifiers from each section.
    const js = generateTabScript();

    assert.match(js, /function parseHash/);
    assert.match(js, /function switchTab/);
    assert.match(js, /window\.switchMainTab = switchTab/);
    assert.match(js, /addEventListener\('click'/);
    assert.match(js, /addEventListener\('keydown'/);
    assert.match(js, /window\.addEventListener\('hashchange'/);
  });

  test("omits hide-element logic by default", () => {

    // Negative test: when hideElementOnTab is unset, the resulting script must not reference hideElement (which would otherwise produce undefined references).
    const js = generateTabScript();

    assert.doesNotMatch(js, /var hideElement = document\.getElementById/);
  });

  test("includes hide-element logic when hideElementOnTab is provided", () => {

    const js = generateTabScript({ hideElementOnTab: { elementId: "logo", tabName: "config" } });

    assert.match(js, /var hideElement = document\.getElementById\('logo'\)/);
    // The conditional must check against the configured tab name.
    assert.match(js, /category === 'config'/);
  });

  test("dispatches the tabactivated custom event for subtab scripts to listen to", () => {

    // The activated event is the documented integration point for subtabs (config and channels) - they listen for it to render their content.
    const js = generateTabScript();

    assert.match(js, /CustomEvent\('tabactivated'/);
  });

  test("handles arrow key navigation between tabs", () => {

    const js = generateTabScript();

    assert.match(js, /e\.key === 'ArrowRight'/);
    assert.match(js, /e\.key === 'ArrowLeft'/);
  });
});

describe("generateTabButton", () => {

  test("emits a button with category, label, role, and active class when active", () => {

    const html = generateTabButton("config", "Config", true);

    assert.match(html, /class="tab-btn active"/);
    assert.match(html, /data-category="config"/);
    assert.match(html, /role="tab"/);
    assert.match(html, /aria-selected="true"/);
    assert.match(html, /aria-controls="panel-config"/);
    assert.match(html, /tabindex="0"/);
    assert.match(html, />Config</);
  });

  test("omits the active class when not active and sets tabindex=-1", () => {

    const html = generateTabButton("status", "Status", false);

    assert.match(html, /class="tab-btn"/);
    assert.doesNotMatch(html, /class="tab-btn active"/);
    assert.match(html, /aria-selected="false"/);
    assert.match(html, /tabindex="-1"/);
  });

  test("includes has-error when hasError=true", () => {

    const html = generateTabButton("config", "Config", true, true);

    assert.match(html, /class="tab-btn active has-error"/);
  });

  test("does not include has-error by default", () => {

    const html = generateTabButton("config", "Config", true);

    assert.doesNotMatch(html, /has-error/);
  });

  test("does not escape the label (label is server-controlled, not user input)", () => {

    // The label is always a hard-coded constant; the function does not escape it, allowing simple HTML in tab labels if ever needed. Locking the contract.
    const html = generateTabButton("c", "Bold <b>Text</b>", false);

    assert.match(html, />Bold <b>Text<\/b></);
  });
});

describe("generateTabPanel", () => {

  test("wraps the content in a tab-panel div with the correct id", () => {

    const html = generateTabPanel("config", "<p>body</p>", true);

    assert.match(html, /<div id="panel-config" class="tab-panel active" role="tabpanel">/);
    assert.match(html, /<p>body<\/p>/);
    assert.match(html, /<\/div>$/);
  });

  test("omits the active class when not active", () => {

    const html = generateTabPanel("config", "x", false);

    assert.match(html, /class="tab-panel"/);
    assert.doesNotMatch(html, /class="tab-panel active"/);
  });
});

describe("generatePageWrapper", () => {

  test("returns a complete HTML document with DOCTYPE, html, head, body", () => {

    const html = generatePageWrapper("Title", "/* css */", "<p>body</p>");

    assert.match(html, /^<!DOCTYPE html>/);
    assert.match(html, /<html lang="en">/);
    assert.match(html, /<head>/);
    assert.match(html, /<\/head>/);
    assert.match(html, /<body>/);
    assert.match(html, /<\/body>/);
    assert.match(html, /<\/html>$/);
  });

  test("places the title in the <title> tag", () => {

    const html = generatePageWrapper("My Page", "", "");

    assert.match(html, /<title>My Page<\/title>/);
  });

  test("includes the favicon and apple-touch-icon links", () => {

    const html = generatePageWrapper("T", "", "");

    assert.match(html, /<link rel="icon" type="image\/svg\+xml" href="\/favicon\.svg">/);
    assert.match(html, /<link rel="icon" type="image\/png" sizes="32x32" href="\/favicon\.png">/);
    assert.match(html, /<link rel="apple-touch-icon"/);
  });

  test("includes the viewport meta tag for mobile rendering", () => {

    const html = generatePageWrapper("T", "", "");

    assert.match(html, /<meta name="viewport" content="width=device-width, initial-scale=1\.0">/);
  });

  test("includes the color-scheme meta tag for native dark mode hints", () => {

    const html = generatePageWrapper("T", "", "");

    assert.match(html, /<meta name="color-scheme" content="light dark">/);
  });

  test("places theme styles before the caller's styles inside <style>", () => {

    // Boundary: theme styles MUST come first so caller-supplied styles can reference the CSS variables. We use a sentinel string in caller styles that should
    // appear after the theme-side --surface-page declaration.
    const html = generatePageWrapper("T", ".my-class { color: red; }", "");
    const styleStart = html.indexOf("<style>");
    const styleEnd = html.indexOf("</style>");

    assert.notEqual(styleStart, -1, "<style> tag should be present");
    assert.notEqual(styleEnd, -1, "</style> tag should be present");

    const inside = html.slice(styleStart, styleEnd);
    const surfaceIdx = inside.indexOf("--surface-page");
    const classIdx = inside.indexOf(".my-class");

    assert.notEqual(surfaceIdx, -1, "theme variables should appear inside <style>");
    assert.notEqual(classIdx, -1, "caller styles should appear inside <style>");
    assert.ok(surfaceIdx < classIdx, "theme variables should appear before caller styles");
  });

  test("inserts body content inside <body>", () => {

    const html = generatePageWrapper("T", "", "<main>hello</main>");

    assert.match(html, /<body>\n<main>hello<\/main>/);
  });

  test("includes optional scripts at the end of the body", () => {

    const html = generatePageWrapper("T", "", "<p>x</p>", "<script>boot();</script>");
    const bodyStart = html.indexOf("<body>");
    const bodyEnd = html.indexOf("</body>");
    const inside = html.slice(bodyStart, bodyEnd);

    assert.match(inside, /<script>boot\(\);<\/script>/);

    const pIdx = inside.indexOf("<p>x</p>");
    const scriptIdx = inside.indexOf("<script>boot");

    assert.ok(pIdx < scriptIdx, "scripts should appear after body content");
  });

  test("renders without scripts when none are provided (boundary)", () => {

    const html = generatePageWrapper("T", "", "<p>x</p>");

    // The default empty string produces no <script> tag in the body section beyond what's in body content.
    const bodyStart = html.indexOf("<body>");
    const bodyEnd = html.indexOf("</body>");
    const inside = html.slice(bodyStart, bodyEnd);

    assert.doesNotMatch(inside, /<script/);
  });
});
