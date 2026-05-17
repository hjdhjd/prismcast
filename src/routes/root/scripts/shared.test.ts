/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * shared.test.ts: Unit tests for the shared client-side utilities script generator. The module exports a single function that returns an HTML <script> block
 * containing toast notifications, dropdown management, channel display rendering, wizard controller factory, and the channelTable namespace. We test the
 * generated string output for structural invariants - presence of expected exposed window.* names and core function definitions - without executing the script
 * since DOM behavior would require a browser-equivalent runtime that the project conventions exclude.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../../../testing.helpers.ts";
import { generateSharedUtilitiesScript } from "./shared.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("generateSharedUtilitiesScript", () => {

  test("returns a non-empty <script> wrapped in an IIFE", () => {

    // The script body lives inside (function() { ... })(); so the symbols stay scoped. Anything exposed via window.* survives.
    const script = generateSharedUtilitiesScript();

    assert.equal(typeof script, "string");
    assert.ok(script.length > 1000, "script should be substantial; got " + String(script.length));
    assert.match(script, /^<script>/);
    assert.match(script, /<\/script>$/);
    assert.match(script, /\(function\(\)\s*\{/);
    assert.match(script, /\}\)\(\);/);
  });

  test("does not produce template-literal artifacts in the generated script", () => {

    const script = generateSharedUtilitiesScript();

    assert.doesNotMatch(script, /\$\{undefined\}/);
    assert.doesNotMatch(script, /\[object Object\]/);
  });

  test("exposes the toast notification API on window.showToast", () => {

    // showToast is the canonical client-side notification entry point used by every other script. Locking the window.showToast assignment keeps cross-script
    // calls from breaking on a refactor.
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.showToast\s*=\s*showToast/);
    assert.match(script, /function showToast\(message,\s*type/);
  });

  test("exposes window.extractErrorMessage as the error-shape SSOT", () => {

    // extractErrorMessage parses both error envelope shapes ({error: string} and {errors: Record<field, string>}). The test confirms the symbol is exposed and
    // the function references both response.error and response.errors.
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.extractErrorMessage\s*=/);
    assert.match(script, /response\.errors/);
    assert.match(script, /response\.error\b/);
  });

  test("exposes safeStorageGet, safeStorageSet, and safeStorageRemove on window", () => {

    // These three wrappers must be present together because they share the try/catch SSOT for localStorage access. Removing one without removing the others
    // would leave private-browsing failures unhandled in some call sites.
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.safeStorageGet\s*=/);
    assert.match(script, /window\.safeStorageSet\s*=/);
    assert.match(script, /window\.safeStorageRemove\s*=/);
  });

  test("exposes the dropdowns namespace with addHook and close as property references", () => {

    // The dropdowns namespace is the closure-backed variant of the client-side namespace pattern: addHook and close must stay as reference aliases to module
    // -scope functions so the listener identity survives addEventListener / removeEventListener pairing.
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.dropdowns\s*=\s*\{/);
    assert.match(script, /addHook:\s*addDropdownHook/);
    assert.match(script, /close:\s*closeAllDropdowns/);
    assert.match(script, /function closeAllDropdowns\(/);
    assert.match(script, /function addDropdownHook\(/);
  });

  test("exposes window.persistDisplayPrefs as the SSOT for display-prefs POSTs", () => {

    // Sort/column visibility persistence flows through this single fetch path. The implementation hits /config/channels/display-prefs with method POST.
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.persistDisplayPrefs\s*=/);
    assert.match(script, /\/config\/channels\/display-prefs/);
    assert.match(script, /method:\s*['"]POST['"]/);
  });

  test("exposes the subtab switcher and initSubtab factories", () => {

    // createSubtabSwitcher returns a parameterized switch function used by both Config and Channels subtab systems. initSubtab reads hash/localStorage and
    // delegates to that switch function.
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.createSubtabSwitcher\s*=/);
    assert.match(script, /window\.initSubtab\s*=/);
  });

  test("exposes window.copyToClipboard with a Clipboard API + execCommand fallback", () => {

    // copyToClipboard uses navigator.clipboard.writeText in secure contexts and falls back to a hidden textarea + execCommand on plain HTTP. Both code paths
    // must be present.
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.copyToClipboard\s*=/);
    assert.match(script, /navigator\.clipboard\?\.writeText/);
    assert.match(script, /execCommand\(['"]copy['"]\)/);
  });

  test("exposes window.toggleDropdown with the portal-positioning lifecycle", () => {

    // The toggleDropdown wrapper handles portal append, scroll/resize listener wiring, and the optional onOpen callback. positionPortal is the helper that
    // computes top/left with viewport clamping.
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.toggleDropdown\s*=/);
    assert.match(script, /window\.positionPortal\s*=\s*positionPortal/);
    assert.match(script, /function positionPortal\(/);
  });

  test("exposes the channel display + service icon helpers", () => {

    // channelDisplayHtml and serviceIconHtml are the shared renderers used by the channels table, status popover, and provider chips. processServiceDisplays
    // walks DOM after page load and after mutations to apply the renderer to dynamically inserted elements.
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.channelDisplayHtml\s*=/);
    assert.match(script, /window\.serviceIconHtml\s*=/);
    assert.match(script, /window\.processServiceDisplays\s*=/);
    assert.match(script, /window\.imgFallback\s*=/);
  });

  test("exposes the wizard controller factory and its full method surface", () => {

    // createWizardController is the SSOT for stepped wizard navigation. The returned controller exposes back, close, getStep, goToStep, hide, next, open,
    // setError, setTitle, show. We confirm a representative subset rather than every name to keep the test stable when the closure adds peripheral helpers.
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.createWizardController\s*=/);

    for(const method of [ "back", "close", "getStep", "goToStep", "next", "open", "setError", "setTitle" ]) {

      const re = new RegExp("\\b" + method + "\\s*\\(");

      assert.match(script, re, "wizard controller should declare " + method + "()");
    }
  });

  test("exposes window.channelTable with the documented method surface", () => {

    // channelTable is the SSOT namespace for client-side channel table DOM operations. The methods are: applyPatch, insertRow, removeRow, filter, refilter,
    // sort, processLogos, getEnabledFilterTags. Each must be defined on the namespace so external callers can invoke channelTable.<method>().
    const script = generateSharedUtilitiesScript();

    assert.match(script, /window\.channelTable\s*=\s*\{/);

    for(const method of [ "applyPatch", "insertRow", "removeRow", "filter", "refilter", "sort", "processLogos", "getEnabledFilterTags" ]) {

      const re = new RegExp("^\\s*" + method + "\\s*\\(", "m");

      assert.match(script, re, "channelTable namespace should declare " + method + "()");
    }
  });

  test("uses this.* method calls inside the channelTable namespace", () => {

    // The channelTable namespace is the this.*-backed variant - methods invoke siblings via this.*. We spot check several internal calls to confirm the pattern
    // is preserved (calling sibling methods, not module-scope functions).
    const script = generateSharedUtilitiesScript();

    assert.match(script, /this\.refilter\(\)/);
    assert.match(script, /this\.processLogos\(\)/);
    assert.match(script, /this\._getSortValue/);
  });

  test("returns identical output across calls (pure derivation)", () => {

    // The script is a constant - no Date.now() or random input.
    assert.equal(generateSharedUtilitiesScript(), generateSharedUtilitiesScript());
  });

  test("balances parentheses across the entire generated script", () => {

    // A mismatched paren in a string template is a load-bearing bug - the script wouldn't parse in the browser, breaking every page. Crude balance check works
    // because the script does not contain unbalanced parens inside string literals (it joins many short strings).
    const script = generateSharedUtilitiesScript();
    const opens = (script.match(/\(/g) ?? []).length;
    const closes = (script.match(/\)/g) ?? []).length;

    assert.equal(opens, closes, "paren balance (opens=" + String(opens) + ", closes=" + String(closes) + ")");
  });

  test("installs the project-wide action dispatcher with collision detection and a typo warning", () => {

    // The dispatcher is the load-bearing primitive every page depends on. We lock its shape so a regression that loses the collision throw, the typo warning,
    // or the modifier walk would fail this test before it could ship.
    const script = generateSharedUtilitiesScript();

    // Registration API exposed on window with uniqueness enforcement.
    assert.match(script, /window\.registerAction = \(name, handler\) =>/, "registerAction is exposed on window");
    assert.match(script, /if\(actionHandlers\.has\(name\)\)/, "registerAction guards against re-registration");
    assert.match(script, /throw new Error\('Action "/, "registerAction throws on collision so silent overwrites are impossible");

    // Modifier walk is event-type-scoped: the selector is constructed from event.type so data-<event>-prevent-default fires only for its own event type. This
    // is what prevents a <form data-submit-prevent-default> from blocking keydown events on input fields inside it.
    assert.match(script, /const prefix = 'data-' \+ event\.type \+ '-'/, "modifier selector is event-type-prefixed");
    assert.match(script, /closest\('\[' \+ prefix \+ 'prevent-default\], \[' \+ prefix \+ 'stop-propagation\], \[' \+ prefix \+ 'close-dropdown\]'\)/,
      "modifier walk uses the event-type-prefixed selector");
    assert.match(script, /hasAttribute\(prefix \+ 'prevent-default'\)\) event\.preventDefault\(\)/);
    assert.match(script, /hasAttribute\(prefix \+ 'stop-propagation'\)\) event\.stopPropagation\(\)/);
    assert.match(script, /hasAttribute\(prefix \+ 'close-dropdown'\) && window\.dropdowns\) window\.dropdowns\.close\(\)/);

    // Action walk: matches the closest [data-<type>-action] ancestor and warns when no handler is registered.
    assert.match(script, /const attrName = 'data-' \+ event\.type \+ '-action'/);
    assert.match(script, /console\.warn\('No handler registered for ' \+ attrName/,
      "missing handlers log a console warning so typos surface fast");

    // Four event types delegated through document-level listeners: a capture-phase listener for the modifier walk (so stopPropagation fires before any
    // intermediate bubble-phase listener can run), and a bubble-phase listener for action dispatch (so element-level listeners get a chance to fire first).
    assert.match(script, /for\(const type of \[ 'click', 'change', 'keydown', 'submit' \]\)/, "all four event types delegated in one loop");
    assert.match(script, /document\.addEventListener\(type, dispatchModifiers, \{ capture: true \}\)/, "modifier walk uses the modern { capture: true } object form");
    assert.match(script, /document\.addEventListener\(type, dispatchAction\)/, "action dispatch is bubble-phase");
  });
});
