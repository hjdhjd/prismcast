/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * configLeaves.helpers.test.ts: Tests for listConfigLeafPaths, the walker the reactivity drift tests share. Coverage: plain objects recurse, arrays, primitives,
 * and null are leaves, the output is sorted, and the default root is the configuration's defaults.
 */
import { describe, test } from "node:test";
import { DEFAULTS } from "../config/userConfig.ts";
import assert from "node:assert/strict";
import { listConfigLeafPaths } from "./configLeaves.helpers.ts";

describe("listConfigLeafPaths", () => {

  test("recurses into plain objects and treats arrays, primitives, and null as leaves, in sorted order", () => {

    // The fixture's keys are deliberately out of order, so the sorted output proves the walker sorts rather than echoing insertion order.
    /* eslint-disable sort-keys */
    const fixture = { zeta: { list: [ 1, 2 ], nested: { flag: true } }, alpha: { empty: null, name: "x" }, mid: 3 };
    /* eslint-enable sort-keys */

    assert.deepEqual(listConfigLeafPaths(fixture), [ "alpha.empty", "alpha.name", "mid", "zeta.list", "zeta.nested.flag" ]);
  });

  test("an empty plain object contributes no leaf", () => {

    assert.deepEqual(listConfigLeafPaths({ a: {}, b: 1 }), ["b"]);
  });

  test("defaults to the configuration's defaults, listing every leaf DEFAULTS defines", () => {

    const paths = listConfigLeafPaths();

    assert.ok(paths.includes("server.port"), "a scalar setting is a leaf");
    assert.ok(paths.includes("streaming.captureCodecs"), "an array-valued setting is one leaf, not one per element");
    assert.ok(paths.includes("paths.chromeProfileName"), "a leaf outside the settings metadata is listed too");
    assert.deepEqual(paths, listConfigLeafPaths(DEFAULTS), "the default root is DEFAULTS");
  });
});
