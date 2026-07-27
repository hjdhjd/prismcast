/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * icons.test.ts: Unit tests for the SVG icon constants in icons.ts. Each export is a string literal carrying a complete <svg> element. The tests lock the
 * structural properties documented in the module header (14x14 dimensions, viewBox 16x16, currentColor stroke), enforce that every exported name resolves to
 * an actual SVG, and pin the well-formed structure of each icon's outer wrapper so an accidental string truncation or mutation surfaces as a test failure.
 */
import * as icons from "./icons.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

/* The set of identifiers we expect the module to export. Maintained as a literal here so an accidental rename or removal of an export surfaces as a missing
 * key, while a new export that omits an entry is flagged by the structural-coverage test below.
 */
const EXPECTED_ICON_NAMES = [
  "ICON_ADD",
  "ICON_BOLT",
  "ICON_COPY",
  "ICON_DELETE",
  "ICON_DISABLE",
  "ICON_EDIT",
  "ICON_ENABLE",
  "ICON_EXPORT",
  "ICON_FILTER",
  "ICON_HEALTH",
  "ICON_IMPORT",
  "ICON_LINK",
  "ICON_LOGIN",
  "ICON_MANAGE",
  "ICON_REVERT",
  "ICON_TRANSFER"
];

describe("icons module exports", () => {

  test("exports every expected icon constant", () => {

    // Boundary: locks the canonical list of icons. New additions must be reflected here so contributors notice if they need to update consumers (the channel
    // table and service-profile HTML generators that embed these constants directly) that depend on the exhaustive list.
    for(const name of EXPECTED_ICON_NAMES) {

      const value = (icons as unknown as Record<string, string>)[name];

      assert.equal(typeof value, "string", "icon export " + name + " should be a string");
      assert.ok((value !== undefined) && (value.length > 0), "icon export " + name + " should not be an empty string");
    }
  });

  test("does not export any unexpected identifiers beyond the documented icon list", () => {

    // Negative test: catches stray exports that slip in without being added to EXPECTED_ICON_NAMES. We compare the keys in the module namespace against the
    // expected list to enforce the closed-set property.
    const actualNames = Object.keys(icons).toSorted();
    const expectedSorted = EXPECTED_ICON_NAMES.toSorted();

    assert.deepEqual(actualNames, expectedSorted, "module exports should match EXPECTED_ICON_NAMES exactly");
  });
});

describe("icon SVG structure", () => {

  // Run the same structural checks against every icon. We loop with for...of (per the convention) so a failure cleanly identifies the offending icon by name.
  for(const name of EXPECTED_ICON_NAMES) {

    test(name + " starts with an <svg> element", () => {

      const svg = (icons as unknown as Record<string, string>)[name];

      assert.ok(svg, "icon " + name + " should be defined");
      assert.match(svg, /^<svg /, "icon " + name + " should begin with the <svg> opening tag");
    });

    test(name + " ends with a </svg> closing tag", () => {

      const svg = (icons as unknown as Record<string, string>)[name];

      assert.ok(svg, "icon " + name + " should be defined");
      assert.match(svg, /<\/svg>$/, "icon " + name + " should end with the </svg> closing tag");
    });

    test(name + " declares width and height of 14", () => {

      // The module header documents that all icons render at 14x14. A mutation that changes one icon's size would visually break alignment in the UI.
      const svg = (icons as unknown as Record<string, string>)[name];

      assert.ok(svg, "icon " + name + " should be defined");
      assert.match(svg, /width="14"/, "icon " + name + " should have width=\"14\"");
      assert.match(svg, /height="14"/, "icon " + name + " should have height=\"14\"");
    });

    test(name + " uses the 16x16 viewBox", () => {

      // The 16x16 viewBox is the documented coordinate system for every icon; paths are drawn against it and scaled to 14px.
      const svg = (icons as unknown as Record<string, string>)[name];

      assert.ok(svg, "icon " + name + " should be defined");
      assert.match(svg, /viewBox="0 0 16 16"/, "icon " + name + " should use viewBox=\"0 0 16 16\"");
    });

    test(name + " uses currentColor for the stroke", () => {

      // currentColor lets icons adopt their parent text color in any theme. A literal hex stroke would defeat dark-mode adaptation.
      const svg = (icons as unknown as Record<string, string>)[name];

      assert.ok(svg, "icon " + name + " should be defined");
      assert.match(svg, /stroke="currentColor"/, "icon " + name + " should use stroke=\"currentColor\"");
    });

    test(name + " uses fill=\"none\" so only the stroke renders", () => {

      const svg = (icons as unknown as Record<string, string>)[name];

      assert.ok(svg, "icon " + name + " should be defined");
      assert.match(svg, /fill="none"/, "icon " + name + " should have fill=\"none\"");
    });
  }
});

describe("icon SVG content uniqueness", () => {

  test("every icon has a distinct SVG body (no accidental duplicates)", () => {

    // Boundary: catches copy-paste mistakes where two icon constants would resolve to the same string. Each icon must be visually distinct - duplicates would
    // be a real bug.
    const seen = new Map<string, string>();

    for(const name of EXPECTED_ICON_NAMES) {

      const svg = (icons as unknown as Record<string, string>)[name];

      assert.ok(svg, "icon " + name + " should be defined");

      const existing = seen.get(svg);

      if(existing !== undefined) {

        assert.fail("icons " + existing + " and " + name + " share the same SVG markup");
      }

      seen.set(svg, name);
    }
  });
});
