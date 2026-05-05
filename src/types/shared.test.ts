/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * shared.test.ts: Type-level tests for the cross-cutting utility types in shared.ts. The module exports no runtime values - only Nullable<T>, ChannelSortField,
 * and SortDirection. Type-only modules normally need no tests, but these three are the cross-cutting vocabulary referenced from config, channels, streaming,
 * and selection types, so we lock their shape with @ts-expect-error directives. Removing any directive should produce a real compile error.
 */
import type { ChannelSortField, Nullable, SortDirection } from "./shared.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("Nullable<T> (type-level)", () => {

  test("accepts a value of type T", () => {

    // Nullable<T> is the union T | null. A plain T value must be assignable.
    const value: Nullable<string> = "hello";

    assert.equal(value, "hello");
  });

  test("accepts null", () => {

    // The null half of the union is the whole point of the type.
    const value: Nullable<string> = null;

    assert.equal(value, null);
  });

  test("rejects undefined (Nullable means null, not optional)", () => {

    // Nullable is intentionally narrower than `T | null | undefined`. The codebase uses absence (omitted optional) for "inherit" and null for "explicitly
    // cleared"; conflating those with undefined would erase the distinction. Removing the @ts-expect-error directive should surface a real error.
    // @ts-expect-error - Nullable<string> does not include undefined.
    const value: Nullable<string> = undefined;

    assert.equal(value, undefined, "the runtime value still exists");
  });

  test("propagates the inner type (Nullable<number> rejects strings)", () => {

    // The generic parameter must flow through. A Nullable<number> must reject a string just like a plain number would.
    // @ts-expect-error - "not a number" is not assignable to number.
    const value: Nullable<number> = "not a number";

    assert.equal(value, "not a number", "the runtime value still exists");
  });

  test("composes with object types", () => {

    // Nullable<{ a: number }> must accept either the object or null. The widened tuple below proves both halves of the union are assignable - if the runtime
    // discriminant says "object", we read the field; if it says "null", we record the null. This avoids narrowing-by-literal that would render the test
    // tautological.
    const values: Nullable<{ a: number }>[] = [ { a: 1 }, null ];

    assert.equal(values[0]?.a, 1, "object form is assignable and yields its field");
    assert.equal(values[1], null, "null form is assignable as the alternate branch of the union");
  });
});

describe("ChannelSortField (type-level)", () => {

  test("accepts every documented sort field literal", () => {

    // ChannelSortField is the closed union of sortable column field names for the channels table. Every literal must be assignable.
    const fields: ChannelSortField[] = [
      "channelNumber", "channelSelector", "hdhrEnabled", "key", "name", "profile", "service", "stationId", "tags"
    ];

    assert.equal(fields.length, 9, "all nine sort fields must be assignable");
  });

  test("rejects unknown literals (the union is closed)", () => {

    // Adding a column to the table requires updating the union; an arbitrary new column name must not type-check until the union is extended.
    // @ts-expect-error - "unknown-field" is not a known ChannelSortField.
    const bad: ChannelSortField = "unknown-field";

    assert.equal(bad, "unknown-field", "the runtime string still exists");
  });

  test("rejects values that are field names elsewhere but not sort fields", () => {

    // url is a binding field, not a sort field. The sort UI does not offer URL as a column to sort on, so the union must reject it.
    // @ts-expect-error - "url" is a binding field, not a sortable column.
    const bad: ChannelSortField = "url";

    assert.equal(bad, "url", "the runtime string still exists");
  });
});

describe("SortDirection (type-level)", () => {

  test("accepts the two documented directions", () => {

    // SortDirection is the closed union { "asc", "desc" }. Both literals must be assignable.
    const ascending: SortDirection = "asc";
    const descending: SortDirection = "desc";

    assert.equal(ascending, "asc");
    assert.equal(descending, "desc");
  });

  test("rejects arbitrary direction strings", () => {

    // ascending (the long form) is plausible but not what the union declares. The closed union must reject it.
    // @ts-expect-error - "ascending" is not a SortDirection literal.
    const bad: SortDirection = "ascending";

    assert.equal(bad, "ascending", "the runtime string still exists");
  });

  test("rejects an empty string", () => {

    // Empty string is a frequent serialization edge case; the union must not silently admit it.
    // @ts-expect-error - the empty string is not a SortDirection literal.
    const bad: SortDirection = "";

    assert.equal(bad, "", "the runtime string still exists");
  });
});
