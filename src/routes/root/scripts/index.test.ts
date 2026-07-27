/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Smoke tests for the scripts barrel re-export. The module re-exports every script generator defined in this directory. The tests
 * confirm that every documented export resolves to a callable function and that calling each function returns a non-empty <script>...</script> string. This
 * catches a barrel regression where a re-export name drifts or the underlying export is removed without updating index.ts.
 */
import * as barrel from "./index.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../../../testing.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("scripts/index.ts barrel", () => {

  test("re-exports generateChannelsSubtabScript as a callable function", () => {

    assert.equal(typeof barrel.generateChannelsSubtabScript, "function");

    const out = barrel.generateChannelsSubtabScript();

    assert.ok(out.length > 100, "output should be a non-trivial script");
    assert.match(out, /<script>/);
  });

  test("re-exports generateConfigSubtabScript as a callable function", () => {

    assert.equal(typeof barrel.generateConfigSubtabScript, "function");

    const out = barrel.generateConfigSubtabScript();

    assert.ok(out.length > 100, "output should be a non-trivial script");
    assert.match(out, /<script>/);
  });

  test("re-exports generateSharedUtilitiesScript as a callable function", () => {

    assert.equal(typeof barrel.generateSharedUtilitiesScript, "function");

    const out = barrel.generateSharedUtilitiesScript();

    assert.ok(out.length > 100, "output should be a non-trivial script");
    assert.match(out, /<script>/);
  });

  test("re-exports generateStatusScript as a callable function", () => {

    assert.equal(typeof barrel.generateStatusScript, "function");

    const out = barrel.generateStatusScript();

    assert.ok(out.length > 100, "output should be a non-trivial script");
    assert.match(out, /<script>/);
  });

  test("exports exactly the four documented generator names", () => {

    // Lock the surface so a new export added without test coverage surfaces here. Keeping the barrel narrow keeps the cross-file dependency graph small.
    const expected = [ "generateChannelsSubtabScript", "generateConfigSubtabScript", "generateSharedUtilitiesScript", "generateStatusScript" ].sort();
    const actual = Object.keys(barrel).sort();

    assert.deepEqual(actual, expected, "barrel exports drifted; got " + actual.join(", "));
  });
});
