/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * narrowing.helpers.test.ts: Tests for the indexed-access narrowing helpers (firstOf, nthOf). These collapse the destructure-and-assert.ok dance under
 * noUncheckedIndexedAccess into a labeled call. Coverage: returns, identity preservation, empty-array errors, label embedding, and readonly-array acceptance.
 */
import { describe, test } from "node:test";
import { firstOf, nthOf } from "./narrowing.helpers.ts";
import assert from "node:assert/strict";

describe("firstOf", () => {

  test("returns the first element of a non-empty array", () => {

    assert.equal(firstOf([ "a", "b", "c" ]), "a", "the first of three should be the first element");
    assert.equal(firstOf([42]), 42, "a numeric element should come back as itself");
  });

  test("works on a single-element array", () => {

    // Boundary: the smallest non-empty array. The contract is "at least one"; firstOf of a one-element array returns that element.
    assert.equal(firstOf(["only"]), "only", "the sole element should satisfy the at-least-one contract");
  });

  test("narrows the return type to T (not T | undefined)", () => {

    // The whole point of the helper is the type narrowing. We exercise it indirectly by destructuring a property off the returned value - if the return type
    // were T | undefined, this would not compile.
    const items: { name: string }[] = [{ name: "alice" }];
    const first = firstOf(items);

    assert.equal(first.name, "alice", "the narrowed value should be usable without a further check");
  });

  test("preserves element identity (returns the same reference, not a copy)", () => {

    const obj = { id: 1 };
    const items = [obj];

    assert.equal(firstOf(items), obj, "the returned reference is the same object");
  });

  test("does not mutate the input array", () => {

    const items = [ "a", "b" ];

    firstOf(items);
    assert.deepEqual(items, [ "a", "b" ], "input array survives the call");
  });

  test("throws a descriptive error when the array is empty", () => {

    assert.throws(() => firstOf([]), /firstOf: expected at least one item, got empty array/, "an empty array should throw rather than return undefined");
  });

  test("incorporates the supplied label into the error message", () => {

    // The label parameter exists so failed assertions point at the specific kind of array that was empty (e.g., "write", "execFile call").
    assert.throws(() => firstOf([], "write"), /firstOf: expected at least one write, got empty array/, "the label should name what was empty");
  });

  test("accepts readonly arrays (the parameter type is `readonly T[]`)", () => {

    // The signature uses `readonly T[]` so callers can pass `as const` arrays, frozen arrays, or function-return arrays without losing the contract.
    const items = [ "a", "b" ] as const;

    assert.equal(firstOf(items), "a", "a readonly array should be accepted unchanged");
  });
});

describe("nthOf", () => {

  test("returns the element at the requested index", () => {

    assert.equal(nthOf([ "a", "b", "c" ], 0), "a", "index zero should be the first element");
    assert.equal(nthOf([ "a", "b", "c" ], 1), "b", "index one should be the middle element");
    assert.equal(nthOf([ "a", "b", "c" ], 2), "c", "index two should be the last element");
  });

  test("works at the last valid index (boundary)", () => {

    const items = [ "a", "b", "c" ];

    assert.equal(nthOf(items, items.length - 1), "c", "the last valid index should still resolve");
  });

  test("throws a descriptive error when the index is out of range", () => {

    assert.throws(() => nthOf([ "a", "b" ], 5), /nthOf: expected at least 6 item\(s\), got 2/, "an out-of-range index should throw with both counts");
  });

  test("throws on the empty array regardless of index", () => {

    assert.throws(() => nthOf([], 0), /nthOf: expected at least 1 item\(s\), got 0/, "index zero on an empty array should still throw");
  });

  test("incorporates the supplied label into the error message", () => {

    assert.throws(() => nthOf([], 2, "execFile call"), /nthOf: expected at least 3 execFile call\(s\), got 0/, "the label should name what was short");
  });

  test("preserves element identity for object arrays", () => {

    const objects = [ { id: 1 }, { id: 2 } ];

    assert.equal(nthOf(objects, 1), objects[1], "returns the same reference");
  });
});
