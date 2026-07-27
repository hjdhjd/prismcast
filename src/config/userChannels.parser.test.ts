/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.parser.test.ts: Direct unit tests for the channels.json parser and prepare-for-write hook.
 *
 *   - parseChannelsFile: turns raw JSON into the compound data type. Handles legacy field/key normalization (provider -> service field, providerSelections key,
 *     tagRegistry validation) and tolerates schemaVersion / migrationsApplied audit-trail edge cases.
 *
 *   - prepareChannelsForWrite: serializes the compound data to the on-disk shape. Conditionally emits migrationsApplied / serviceSelections / tagRegistry only
 *     when they carry data, so the on-disk file stays minimal.
 *
 * These functions are private but reachable via __internalForTests for direct branch coverage. The test split here keeps the concern (parse / write hook) separate
 * from migration body tests, which live in userChannels.migration.test.ts.
 */
import { describe, test } from "node:test";
import { __internalForTests } from "./userChannels.ts";
import assert from "node:assert/strict";

const { parseChannelsFile, prepareChannelsForWrite } = __internalForTests;

describe("parseChannelsFile", () => {

  test("returns the default empty shape for an empty object", () => {

    const result = parseChannelsFile("{}");

    assert.deepEqual(result.channels, {});
    assert.deepEqual(result.serviceSelections, {});
    assert.deepEqual(result.tagRegistry, { deletedTags: [], tags: [] });
    assert.deepEqual(result.migrationsApplied, []);
    assert.equal(result.schemaVersion, 1, "files predating schemaVersion are treated as v1");
  });

  test("extracts schemaVersion when present and finite", () => {

    const result = parseChannelsFile(JSON.stringify({ schemaVersion: 3 }));

    assert.equal(result.schemaVersion, 3);
  });

  test("falls back to v1 when schemaVersion is non-numeric or negative", () => {

    /* Tolerance branch: the parser treats any plausibly-bad version as v1 since migrations are safe to run more than once.
     * Three sub-cases.
     */
    assert.equal(parseChannelsFile(JSON.stringify({ schemaVersion: "broken" })).schemaVersion, 1);
    assert.equal(parseChannelsFile(JSON.stringify({ schemaVersion: -5 })).schemaVersion, 1);
    assert.equal(parseChannelsFile(JSON.stringify({ schemaVersion: NaN })).schemaVersion, 1);
  });

  test("floors fractional schemaVersion to integer", () => {

    /* A user hand-edited a non-integer version. The parser floors it for the migration runner.
     */
    assert.equal(parseChannelsFile(JSON.stringify({ schemaVersion: 2.7 })).schemaVersion, 2);
  });

  test("captures string entries from migrationsApplied and drops non-string entries", () => {

    const raw = JSON.stringify({ migrationsApplied: [ "Migration A", 42, "Migration B", null ] });
    const result = parseChannelsFile(raw);

    assert.deepEqual(result.migrationsApplied, [ "Migration A", "Migration B" ]);
  });

  test("ignores migrationsApplied when not an array", () => {

    const result = parseChannelsFile(JSON.stringify({ migrationsApplied: "not an array" }));

    assert.deepEqual(result.migrationsApplied, []);
  });

  test("accepts the legacy 'providerSelections' key as a synonym for serviceSelections", () => {

    /* The v2 -> v3 migration rewrites this key under the current name on next persist. The parser tolerates the legacy form so reads work even before the
     * migration runs. Both "serviceSelections" and "providerSelections" funnel into the same accumulator, so if both keys ever appeared in one file their
     * sub-entries would merge and a shared sub-key would take the value from whichever top-level key is iterated later (file order), not from legacy-vs-current
     * status. This test exercises only the legacy-alone case below.
     */
    const result = parseChannelsFile(JSON.stringify({ providerSelections: { abc: "abc-hulu" } }));

    assert.deepEqual(result.serviceSelections, { abc: "abc-hulu" }, "legacy providerSelections is accepted");
  });

  test("extracts only string-valued entries from serviceSelections", () => {

    /* Defensive: non-string values are dropped. A hand-edited file with `{ abc: 42 }` won't crash the parser; the malformed entry is silently dropped.
     */
    const raw = JSON.stringify({ serviceSelections: { abc: "abc-hulu", also: { nested: "object" }, broken: 42 } });
    const result = parseChannelsFile(raw);

    assert.deepEqual(result.serviceSelections, { abc: "abc-hulu" });
  });

  test("ignores serviceSelections when not an object", () => {

    /* Top-level type guard: the parser checks (typeof === "object" && !== null && !Array.isArray) before iterating.
     */
    const result = parseChannelsFile(JSON.stringify({ serviceSelections: [ "not", "an", "object" ] }));

    assert.deepEqual(result.serviceSelections, {});
  });

  test("extracts tagRegistry tags and deletedTags arrays, sorts them, and drops non-string entries", () => {

    const raw = JSON.stringify({ tagRegistry: { deletedTags: [ "Local", null ], tags: [ "Sports", 42, "News" ] } });
    const result = parseChannelsFile(raw);

    assert.deepEqual(result.tagRegistry.tags, [ "News", "Sports" ], "tags are sorted; non-strings dropped");
    assert.deepEqual(result.tagRegistry.deletedTags, ["Local"]);
  });

  test("ignores tagRegistry when not an object", () => {

    const result = parseChannelsFile(JSON.stringify({ tagRegistry: "not an object" }));

    assert.deepEqual(result.tagRegistry, { deletedTags: [], tags: [] });
  });

  test("ignores tagRegistry sub-fields that are not arrays", () => {

    const raw = JSON.stringify({ tagRegistry: { deletedTags: { nested: "object" }, tags: "not an array" } });
    const result = parseChannelsFile(raw);

    assert.deepEqual(result.tagRegistry, { deletedTags: [], tags: [] });
  });

  test("treats top-level non-object/non-array fields as channel definitions", () => {

    const raw = JSON.stringify({

      "abc": { name: "ABC Custom" },
      "mychannel": { name: "My Channel", url: "https://example.com" }
    });
    const result = parseChannelsFile(raw);

    assert.equal(Object.keys(result.channels).length, 2);
    assert.deepEqual(result.channels["abc"], { name: "ABC Custom" });
  });

  test("normalizes legacy 'provider' field to 'service' on channel entries", () => {

    /* Pre-foxone-era stored entries used "provider" as the display-name override field. The parser renames it to "service" so consumer code only sees the
     * current shape. The schema migration to v3 stamps the version after normalization, ensuring the persist on next write drops the legacy field.
     */
    const raw = JSON.stringify({ "abc": { name: "ABC", provider: "Custom Provider", url: "https://example.com" } });
    const result = parseChannelsFile(raw);
    const entry = result.channels["abc"] as Record<string, unknown>;

    assert.equal(entry["service"], "Custom Provider", "legacy provider migrated to service");
    assert.equal("provider" in entry, false, "legacy provider field removed");
  });

  test("preserves an existing 'service' field when both legacy 'provider' and 'service' are present", () => {

    /* Branch: when both fields exist (improbable but possible from hand-edited files), the parser preserves the new field's value and discards the legacy one.
     * The legacy provider is just deleted; the service field stays as-is.
     */
    const raw = JSON.stringify({ "abc": { name: "ABC", provider: "Old", service: "New", url: "https://example.com" } });
    const result = parseChannelsFile(raw);
    const entry = result.channels["abc"] as Record<string, unknown>;

    assert.equal(entry["service"], "New", "existing service field wins");
    assert.equal("provider" in entry, false);
  });

  test("ignores top-level array values (channels must be objects)", () => {

    /* The structural check `typeof === "object" && !Array.isArray && !== null` excludes arrays and nulls from being mistaken for channel entries.
     */
    const raw = JSON.stringify({ "abc": [ "not", "a", "channel" ], "valid": { name: "OK", url: "https://example.com" } });
    const result = parseChannelsFile(raw);

    assert.equal("abc" in result.channels, false, "array value is not a channel");
    assert.ok(result.channels["valid"], "valid object value is captured");
  });

  test("ignores top-level null values", () => {

    /* JSON allows null values; the parser must not treat them as channel definitions.
     */
    const raw = JSON.stringify({ "abc": null, "valid": { name: "OK", url: "https://example.com" } });
    const result = parseChannelsFile(raw);

    assert.equal("abc" in result.channels, false);
  });
});

describe("prepareChannelsForWrite", () => {

  /* Conditional emission: the writer omits empty top-level metadata fields so the on-disk file stays minimal. Three independently-conditional writes
   * (migrationsApplied, serviceSelections, tagRegistry) plus the always-present schemaVersion and channels.
   */

  test("emits only schemaVersion and channels when all metadata is empty", () => {

    const output = prepareChannelsForWrite({

      channels: { "mychannel": { name: "My", url: "https://example.com" } },
      migrationsApplied: [],
      schemaVersion: 3,
      serviceSelections: {},
      tagRegistry: { deletedTags: [], tags: [] }
    }) as Record<string, unknown>;

    assert.equal(output["schemaVersion"], 3);
    assert.ok(output["mychannel"], "channel entries are spread to the top level");
    assert.equal("migrationsApplied" in output, false, "empty array omitted");
    assert.equal("serviceSelections" in output, false, "empty object omitted");
    assert.equal("tagRegistry" in output, false, "empty registry omitted");
  });

  test("emits migrationsApplied only when non-empty", () => {

    const output = prepareChannelsForWrite({

      channels: {},
      migrationsApplied: ["Stamp variants"],
      schemaVersion: 3,
      serviceSelections: {},
      tagRegistry: { deletedTags: [], tags: [] }
    }) as Record<string, unknown>;

    assert.deepEqual(output["migrationsApplied"], ["Stamp variants"]);
  });

  test("emits serviceSelections only when it has at least one entry", () => {

    const output = prepareChannelsForWrite({

      channels: {},
      migrationsApplied: [],
      schemaVersion: 3,
      serviceSelections: { abc: "abc-hulu" },
      tagRegistry: { deletedTags: [], tags: [] }
    }) as Record<string, unknown>;

    assert.deepEqual(output["serviceSelections"], { abc: "abc-hulu" });
  });

  test("emits tagRegistry when tags is non-empty", () => {

    const output = prepareChannelsForWrite({

      channels: {},
      migrationsApplied: [],
      schemaVersion: 3,
      serviceSelections: {},
      tagRegistry: { deletedTags: [], tags: ["Custom"] }
    }) as Record<string, unknown>;

    assert.deepEqual(output["tagRegistry"], { deletedTags: [], tags: ["Custom"] });
  });

  test("emits tagRegistry when deletedTags is non-empty even if tags is empty", () => {

    /* The condition is OR-shaped: tags.length > 0 OR deletedTags.length > 0. The user can have only deletedTags populated (tag delete with no user-created tags).
     */
    const output = prepareChannelsForWrite({

      channels: {},
      migrationsApplied: [],
      schemaVersion: 3,
      serviceSelections: {},
      tagRegistry: { deletedTags: ["Sports"], tags: [] }
    }) as Record<string, unknown>;

    assert.deepEqual(output["tagRegistry"], { deletedTags: ["Sports"], tags: [] });
  });

  test("preserves channel entries via top-level spread (file shape: schemaVersion + per-channel keys)", () => {

    const output = prepareChannelsForWrite({

      channels: { "abc": { name: "ABC Custom" }, "nbc": { name: "NBC Custom" } },
      migrationsApplied: [],
      schemaVersion: 3,
      serviceSelections: {},
      tagRegistry: { deletedTags: [], tags: [] }
    }) as Record<string, unknown>;

    assert.deepEqual(output["abc"], { name: "ABC Custom" });
    assert.deepEqual(output["nbc"], { name: "NBC Custom" });
  });
});
