/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streamSettings.helpers.test.ts: Tests for the makeStreamSettings factory. The factory is consumed across the configuration and streaming test suites; a bug in
 * its defaults or its override merging would cascade into every row that constructs a stream's setup, capture, monitor, or segmenter.
 */
import { CONFIG_METADATA, DEFAULTS, getNestedValue, getReactivityClass } from "./userConfig.ts";
import { STREAM_SETTINGS_KEYS, makeStreamSettings } from "./streamSettings.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { assertSameShape } from "../testing.helpers.ts";

// The next-stream set, derived from the classification rather than restated.
const NEXT_STREAM_PATHS = Object.values(CONFIG_METADATA).flat().map((setting) => setting.path).filter((path) => getReactivityClass(path) === "next-stream");

describe("makeStreamSettings", () => {

  test("with no overrides, each member holds its leaf's default", () => {

    const settings = makeStreamSettings() as unknown as Record<string, unknown>;

    assert.ok(NEXT_STREAM_PATHS.length > 0, "precondition: the classification declares a next-stream set");

    for(const path of NEXT_STREAM_PATHS) {

      const member = path.split(".").at(-1) ?? path;

      assert.equal(settings[member], getNestedValue(DEFAULTS, path), member + " holds the default of " + path);
    }
  });

  test("an override replaces its member alone, the others keeping their defaults", () => {

    const defaults = makeStreamSettings();
    const settings = makeStreamSettings({ frameRate: defaults.frameRate - 1 });

    assert.equal(settings.frameRate, defaults.frameRate - 1, "the overridden member holds the override");
    assert.deepEqual({ ...settings, frameRate: defaults.frameRate }, defaults, "every other member holds its default");
  });

  test("populates every StreamSettings key (parity check against the type's complete key set)", () => {

    /* Two-layer drift catch (see registry.helpers.ts STREAM_REGISTRY_ENTRY_KEYS for the same pattern). The compile-time completeness check on
     * STREAM_SETTINGS_KEYS forces the array to track StreamSettings, and the runtime keyset check below forces the factory to populate every key in the array.
     */
    const reference = Object.fromEntries(STREAM_SETTINGS_KEYS.map((key) => [ key, undefined ]));

    assertSameShape(makeStreamSettings(), reference, "makeStreamSettings vs StreamSettings' declared key set");
  });

  test("each call returns its own frozen object", () => {

    const first = makeStreamSettings();
    const second = makeStreamSettings();

    assert.notEqual(first, second, "each call builds its own object, so no row shares another's");
    assert.ok(Object.isFrozen(first) && Object.isFrozen(second), "each is frozen, as production freezes the snapshot");
  });
});
