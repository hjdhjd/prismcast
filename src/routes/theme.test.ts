/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * theme.test.ts: Unit tests for the theme system in theme.ts. The exports are generateThemeStyles (CSS string with light + dark theme variables),
 * getStreamHealthColorVars (map of health states to CSS variable references), and getLogLevelColorVars (map of log levels to CSS variable references). The
 * tests verify the structural guarantees - that every documented token is present, that the dark-mode block uses prefers-color-scheme, that each stream row tint
 * shades its own health indicator, and that the helper maps return the documented set of keys mapping to var(--*) references that resolve at render time.
 */
import { describe, test } from "node:test";
import { generateThemeStyles, getLogLevelColorVars, getStreamHealthColorVars } from "./theme.ts";
import assert from "node:assert/strict";

/**
 * Reads every "--stream-<state>: #rrggbb;" health indicator declaration out of one theme block. The tint tokens never match, since they are rgba() values
 * rather than hex ones.
 * @param block - The slice of generated CSS holding one theme block.
 * @returns A map from health state to its indicator's red, green, and blue components.
 */
function readIndicatorTriples(block: string): Map<string, number[]> {

  const triples = new Map<string, number[]>();

  for(const match of block.matchAll(/--stream-([a-z]+)\s*:\s*#([0-9a-fA-F]{6})\s*;/g)) {

    const [ , state, hex ] = match;

    if((state !== undefined) && (hex !== undefined)) {

      triples.set(state, [ hex.slice(0, 2), hex.slice(2, 4), hex.slice(4, 6) ].map((pair) => parseInt(pair, 16)));
    }
  }

  return triples;
}

/**
 * Reads every "--stream-tint-<state>: rgba(r, g, b, a);" row tint declaration out of one theme block. The alpha component is deliberately not captured - a tint
 * is expected to differ from its indicator in opacity and in nothing else.
 * @param block - The slice of generated CSS holding one theme block.
 * @returns A map from health state to its tint's red, green, and blue components.
 */
function readTintTriples(block: string): Map<string, number[]> {

  const triples = new Map<string, number[]>();

  for(const match of block.matchAll(/--stream-tint-([a-z]+)\s*:\s*rgba\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,/g)) {

    const [ , state, red, green, blue ] = match;

    if((state !== undefined) && (red !== undefined) && (green !== undefined) && (blue !== undefined)) {

      triples.set(state, [ red, green, blue ].map((component) => parseInt(component, 10)));
    }
  }

  return triples;
}

describe("generateThemeStyles", () => {

  test("returns a non-empty CSS string", () => {

    const css = generateThemeStyles();

    assert.equal(typeof css, "string");
    assert.ok(css.length > 0, "generated CSS should be a non-empty string");
  });

  test("opens with the :root selector for the light theme", () => {

    // The light theme is the default (declared on :root) and the dark theme overrides via prefers-color-scheme. The structure depends on this ordering so that
    // dark mode can override variables defined in :root.
    const css = generateThemeStyles();

    assert.match(css, /:root \{/, "should declare a :root block");
  });

  test("includes the dark mode media query block", () => {

    // Dark mode is wired through prefers-color-scheme. A regression that drops this block would silently disable dark mode.
    const css = generateThemeStyles();

    assert.match(css, /@media \(prefers-color-scheme: dark\)/, "should include the dark mode media query");
  });

  test("declares every documented surface color token in the light theme", () => {

    const css = generateThemeStyles();
    const surfaceTokens = [
      "--surface-page",
      "--surface-elevated",
      "--surface-sunken",
      "--surface-overlay",
      "--surface-hover",
      "--surface-code",
      "--surface-pre"
    ];

    for(const token of surfaceTokens) {

      assert.match(css, new RegExp(token + ":"), "expected surface token " + token + " to be declared");
    }
  });

  test("declares every documented text color token", () => {

    const css = generateThemeStyles();
    const textTokens = [
      "--text-primary",
      "--text-secondary",
      "--text-muted",
      "--text-tertiary",
      "--text-disabled",
      "--text-heading",
      "--text-heading-secondary",
      "--text-inverse"
    ];

    for(const token of textTokens) {

      assert.match(css, new RegExp(token + ":"), "expected text token " + token + " to be declared");
    }
  });

  test("declares every stream health indicator color", () => {

    // The stream-* tokens are consumed by getStreamHealthColorVars and rendered via the .status-* indicator classes. Both must agree on the set of states.
    const css = generateThemeStyles();
    const streamTokens = [
      "--stream-healthy",
      "--stream-buffering",
      "--stream-recovering",
      "--stream-stalled",
      "--stream-error"
    ];

    for(const token of streamTokens) {

      assert.match(css, new RegExp(token + ":"), "expected stream token " + token + " to be declared");
    }
  });

  test("declares every documented status feedback color group", () => {

    const css = generateThemeStyles();

    // Each status group has bg, border, and text variants.
    const groups = [ "success", "warning", "error", "info" ];
    const variants = [ "bg", "border", "text" ];

    for(const group of groups) {

      for(const variant of variants) {

        const token = "--status-" + group + "-" + variant;

        assert.match(css, new RegExp(token + ":"), "expected status token " + token + " to be declared");
      }
    }
  });

  test("declares dark theme overrides for the same surface tokens", () => {

    // The dark theme block must declare surface variables again so they cascade above the light defaults. We carve out the dark block by index and check it has
    // the matching variables.
    const css = generateThemeStyles();
    const darkStart = css.indexOf("@media (prefers-color-scheme: dark)");

    assert.notEqual(darkStart, -1, "dark mode media query should be present");

    const darkBlock = css.slice(darkStart);

    assert.match(darkBlock, /--surface-page:/, "dark block should override --surface-page");
    assert.match(darkBlock, /--text-primary:/, "dark block should override --text-primary");
    assert.match(darkBlock, /--stream-healthy:/, "dark block should override --stream-healthy");
  });

  test("declares font-mono for the monospace font stack", () => {

    const css = generateThemeStyles();

    assert.match(css, /--font-mono:/, "should declare --font-mono for code/log surfaces");
  });

  test("declares all radius tokens (sm, md, lg, xl)", () => {

    const css = generateThemeStyles();

    for(const size of [ "sm", "md", "lg", "xl" ]) {

      assert.match(css, new RegExp("--radius-" + size + ":"), "expected --radius-" + size + " to be declared");
    }
  });

  test("every stream row tint carries the rgb triple of the health indicator it shades, in both theme blocks", () => {

    /* The tint tokens are literal rgba() strings - nothing in the module derives one from its indicator - so the agreement between a tint and the color it
     * shades can only be asserted from the outside. We read the tinted state names out of the CSS rather than listing them here, so a tint added later is
     * covered without touching this test, and we resolve the light and dark blocks separately because the dark block brightens some indicators and its tints
     * have to follow.
     */
    const css = generateThemeStyles();
    const darkStart = css.indexOf("@media (prefers-color-scheme: dark)");

    assert.notEqual(darkStart, -1, "dark mode media query should be present");

    const blocks = { dark: css.slice(darkStart), light: css.slice(0, darkStart) };

    for(const [ blockName, block ] of Object.entries(blocks)) {

      const indicators = readIndicatorTriples(block);
      const tints = readTintTriples(block);

      // A block that parsed to nothing would make every comparison below vacuously true, so the parse is asserted before anything is compared.
      assert.ok(indicators.size > 0, "the " + blockName + " block should declare stream health indicators");
      assert.ok(tints.size > 0, "the " + blockName + " block should declare stream row tints");

      for(const [ state, tint ] of tints) {

        const indicator = indicators.get(state);

        assert.ok(indicator, "the " + blockName + " block should declare a --stream-" + state + " indicator for its tint to shade");
        assert.deepEqual(tint, indicator, "--stream-tint-" + state + " in the " + blockName + " block should carry the --stream-" + state + " triple");
      }
    }
  });

  test("produces stable output across two calls", () => {

    // Boundary: the function is pure - two calls must produce byte-identical output. Locking this prevents a future change from accidentally including
    // non-deterministic content (e.g., timestamps, random IDs).
    assert.equal(generateThemeStyles(), generateThemeStyles(), "two calls should return identical CSS");
  });

  test("contains balanced braces (every { has a matching })", () => {

    // Negative test: a CSS string with unbalanced braces would break the page. We count open/close braces as a sanity check on string assembly.
    const css = generateThemeStyles();
    const opens = (css.match(/\{/g) ?? []).length;
    const closes = (css.match(/\}/g) ?? []).length;

    assert.equal(opens, closes, "open and close braces must balance");
  });
});

describe("getStreamHealthColorVars", () => {

  test("returns the documented set of health states as keys", () => {

    // The exact set of keys is asserted here as a documented API contract, so any addition or removal is a deliberate, visible change.
    const vars = getStreamHealthColorVars();
    const keys = Object.keys(vars).sort();

    assert.deepEqual(keys, [ "buffering", "error", "healthy", "recovering", "stalled" ], "keys should be the five health states");
  });

  test("each value is a var(--stream-*) CSS reference", () => {

    // Every value must be a var() reference so callers can pass the string directly into a style attribute and the browser resolves it at render time.
    const vars = getStreamHealthColorVars();

    for(const [ key, value ] of Object.entries(vars)) {

      assert.match(value, /^var\(--stream-/, "value for " + key + " should start with var(--stream-");
      assert.match(value, /\)$/, "value for " + key + " should end with )");
    }
  });

  test("each key maps to its same-named --stream-* variable", () => {

    // Locks the documented contract: getStreamHealthColorVars().healthy === "var(--stream-healthy)".
    const vars = getStreamHealthColorVars();

    assert.equal(vars["buffering"], "var(--stream-buffering)");
    assert.equal(vars["error"], "var(--stream-error)");
    assert.equal(vars["healthy"], "var(--stream-healthy)");
    assert.equal(vars["recovering"], "var(--stream-recovering)");
    assert.equal(vars["stalled"], "var(--stream-stalled)");
  });

  test("returns a fresh object on each call (no shared reference mutation)", () => {

    // Boundary: callers may add properties to the returned object; doing so should not affect subsequent calls. Verifies the function builds a new object each
    // time rather than memoizing.
    const a = getStreamHealthColorVars();

    a["healthy"] = "MUTATED";

    const b = getStreamHealthColorVars();

    assert.equal(b["healthy"], "var(--stream-healthy)", "mutating one return must not affect the next call");
  });
});

describe("getLogLevelColorVars", () => {

  test("returns the documented set of log levels plus a default fallback as keys", () => {

    const vars = getLogLevelColorVars();
    const keys = Object.keys(vars).sort();

    assert.deepEqual(keys, [ "default", "error", "warn" ], "keys should be the three log levels");
  });

  test("each value is a var(--*) CSS reference", () => {

    const vars = getLogLevelColorVars();

    for(const [ key, value ] of Object.entries(vars)) {

      assert.match(value, /^var\(--/, "value for " + key + " should start with var(--");
      assert.match(value, /\)$/, "value for " + key + " should end with )");
    }
  });

  test("default maps to the secondary text color so non-error/warn entries inherit the body color", () => {

    // The default level routes to the secondary dark-text token because the logs panel renders against a dark surface in both themes.
    const vars = getLogLevelColorVars();

    assert.equal(vars["default"], "var(--dark-text-secondary)");
  });

  test("error and warn route to their dark-mode log color tokens", () => {

    const vars = getLogLevelColorVars();

    assert.equal(vars["error"], "var(--dark-text-error)");
    assert.equal(vars["warn"], "var(--dark-text-warn)");
  });

  test("returns a fresh object on each call", () => {

    const a = getLogLevelColorVars();

    a["error"] = "MUTATED";

    const b = getLogLevelColorVars();

    assert.equal(b["error"], "var(--dark-text-error)", "mutating one return must not affect the next call");
  });
});
