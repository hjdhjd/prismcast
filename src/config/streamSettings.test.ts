/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streamSettings.test.ts: Tests for the per-stream settings snapshot. The contracts exercised:
 *
 *   1. The builder copies each next-stream leaf into the member named for it, so a configuration whose leaves all differ yields a snapshot of exactly those values.
 *
 *   2. The snapshot's members are the next-stream set the classification declares, one per path and named for the path's last segment, so a setting that gains
 *      or loses the class fails here until the snapshot follows it.
 *
 *   3. The snapshot is frozen and typed read-only, and neither the running configuration nor a group of it type-checks as one.
 *
 *   4. A save of a next-stream setting is realized as next-stream: the running configuration takes the value, a snapshot taken before the save keeps the value it
 *      copied, and a snapshot taken after carries the new one.
 */
import { CONFIG, initializeConfiguration, saveConfiguration } from "./index.ts";
import { CONFIG_METADATA, DEFAULTS, getNestedValue, getReactivityClass, getSettingByPath, setNestedValue } from "./userConfig.ts";
import { describe, test } from "node:test";
import type { Config } from "../types/index.ts";
import type { ConfigStore } from "./index.ts";
import type { StreamSettings } from "./streamSettings.ts";
import type { UserConfig } from "./userConfig.ts";
import assert from "node:assert/strict";
import { snapshotStreamSettings } from "./streamSettings.ts";

// The next-stream set, derived from the classification rather than restated, sorted.
const NEXT_STREAM_PATHS = Object.values(CONFIG_METADATA).flat().map((setting) => setting.path).filter((path) => getReactivityClass(path) === "next-stream")
  .toSorted();

/**
 * Returns the snapshot member a path's leaf is copied into: the path's last segment.
 * @param path - The configuration path.
 * @returns The member name.
 */
function memberOf(path: string): string {

  return path.split(".").at(-1) ?? path;
}

describe("snapshotStreamSettings", () => {

  test("copies each next-stream leaf of a configuration whose leaves differ from the defaults and from each other into the member named for it", () => {

    // Each leaf takes a value apart from its default and from every other leaf's, so a builder that copied a default, a constant, or the wrong leaf reads another
    // value than the one the row assigned.
    const config = structuredClone(DEFAULTS);

    for(const [ index, path ] of NEXT_STREAM_PATHS.entries()) {

      setNestedValue(config as unknown as Record<string, unknown>, path, ((getNestedValue(DEFAULTS, path) as number) * 3) + index + 1);
    }

    const assigned = NEXT_STREAM_PATHS.map((path) => getNestedValue(config, path));

    assert.equal(new Set(assigned).size, NEXT_STREAM_PATHS.length, "precondition: the assigned leaves differ from each other");
    assert.ok(NEXT_STREAM_PATHS.every((path) => getNestedValue(config, path) !== getNestedValue(DEFAULTS, path)), "precondition: and from their defaults");

    const snapshot = snapshotStreamSettings(config) as unknown as Record<string, unknown>;

    for(const path of NEXT_STREAM_PATHS) {

      assert.equal(snapshot[memberOf(path)], getNestedValue(config, path), memberOf(path) + " is the value of " + path);
    }
  });

  test("the snapshot of the defaults has exactly one member per next-stream path, named for the path's last segment and holding its default", () => {

    // A member per path, named for its last segment, is the naming rule: a later next-stream leaf whose last segment repeats an existing member's would leave
    // fewer distinct names than paths and fail here rather than shadow that member.
    const snapshot = snapshotStreamSettings(DEFAULTS) as unknown as Record<string, unknown>;
    const members = NEXT_STREAM_PATHS.map(memberOf);

    assert.ok(NEXT_STREAM_PATHS.length > 0, "precondition: the classification declares a next-stream set");
    assert.equal(new Set(members).size, NEXT_STREAM_PATHS.length, "every next-stream path names a member no other path names");
    assert.deepEqual(Object.keys(snapshot).toSorted(), members.toSorted(), "the snapshot's members are exactly the next-stream set's");

    for(const path of NEXT_STREAM_PATHS) {

      assert.equal(snapshot[memberOf(path)], getNestedValue(DEFAULTS, path), memberOf(path) + " holds the default of " + path);
    }
  });

  test("the snapshot is frozen, so an assignment to a member throws", () => {

    const snapshot = snapshotStreamSettings(DEFAULTS);

    assert.ok(Object.isFrozen(snapshot), "the snapshot is frozen");
    assert.throws(() => { (snapshot as { segmentDuration: number }).segmentDuration = snapshot.segmentDuration + 1; }, TypeError,
      "a write through any holder of the shared snapshot is refused at runtime");
  });

  test("neither the running configuration nor a group of it type-checks as a snapshot, and a member assignment does not type-check", () => {

    // @ts-expect-error - the flat members name no configuration group, so the running configuration cannot stand in for a stream's snapshot.
    const fromConfig: StreamSettings = CONFIG;

    // @ts-expect-error - nor can a group of it, which holds some of the leaves under other names and none of the others.
    const fromGroup: StreamSettings = CONFIG.streaming;

    const snapshot = snapshotStreamSettings(DEFAULTS);

    assert.ok(fromConfig, "the rows above are compile-time assertions; this keeps the bindings read");
    assert.ok(fromGroup);
    assert.throws(() => {

      // @ts-expect-error - every member is read-only, so a holder cannot write the shared snapshot.
      snapshot.frameRate = 30;
    }, TypeError);
  });

  test("a save of a next-stream setting is realized as next-stream, reaching a snapshot taken after it and never one taken before", async () => {

    /* The save runs through the real reconcile against an in-memory store typed by the production port. The running configuration takes the saved value, which
     * is what the class promises the streams that start afterward, while a snapshot taken before the save keeps the value it copied, which is what a running
     * stream keeps. A second save writing the original back leaves the running configuration as it began.
     */
    let file: UserConfig = {};

    const io: ConfigStore = {

      mutateConfig: async (fn) => {

        const working = structuredClone(file);

        fn(working);
        file = working;
      },
      readConfig: async () => ({ config: structuredClone(file), parseError: false, readError: false })
    };

    await initializeConfiguration(undefined, io);

    const began: Config = structuredClone(CONFIG);
    const original = CONFIG.hls.segmentDuration;
    const metadata = getSettingByPath("hls.segmentDuration");
    const saved = 5;

    assert.equal(getReactivityClass("hls.segmentDuration"), "next-stream", "precondition: the saved setting carries the next-stream class");
    assert.ok(metadata, "precondition: the saved setting has metadata");
    assert.ok(((metadata.min ?? -Infinity) <= saved) && (saved <= (metadata.max ?? Infinity)), "precondition: the saved value is inside its bounds");
    assert.notEqual(saved, original, "precondition: the save changes the value");

    const before = snapshotStreamSettings(CONFIG);
    const result = await saveConfiguration((current) => { current.hls = { ...current.hls, segmentDuration: saved }; }, io);

    assert.deepEqual(result.nextStream.map((change) => change.path), ["hls.segmentDuration"], "the save reports the change as next-stream");
    assert.deepEqual(result.applied.map((change) => change.path), [], "and not as applied");
    assert.equal(CONFIG.hls.segmentDuration, saved, "the running configuration takes the saved value for the streams that start afterward");
    assert.equal(before.segmentDuration, original, "a snapshot taken before the save keeps the value it copied");
    assert.equal(snapshotStreamSettings(CONFIG).segmentDuration, saved, "a snapshot taken after the save carries the saved value");

    await saveConfiguration((current) => { current.hls = { ...current.hls, segmentDuration: original }; }, io);

    assert.deepEqual(CONFIG, began, "writing the original back leaves the running configuration as it began");
  });
});
