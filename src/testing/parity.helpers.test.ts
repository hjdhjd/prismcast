/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * parity.helpers.test.ts: Tests for the factory-parity utilities. assertSameShape and declareKeysOf together form the runtime + compile-time pair that
 * catches silent drift between a fixture factory and its target type. The test file asserts assertSameShape's reporting contract (asymmetric difference, descriptive
 * label embedding, no-op on equal sets); declareKeysOf's contract is fundamentally compile-time so we exercise it with a representative type and verify the
 * runtime return-as-passed-through behavior.
 */
import { assertSameShape, declareKeysOf } from "./parity.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("assertSameShape", () => {

  test("does not throw when the two objects have identical key sets", () => {

    assert.doesNotThrow(() => {

      assertSameShape({ a: 1, b: 2 }, { a: "x", b: "y" }, "matching shapes");
    }, "identical key sets should pass silently");
  });

  test("does not throw on two empty objects", () => {

    // Boundary: both objects empty. The key sets are trivially equal.
    assert.doesNotThrow(() => {

      assertSameShape({}, {}, "empty shapes");
    }, "two empty objects should pass silently");
  });

  test("throws when the actual object has a key the expected object lacks", () => {

    assert.throws(

      () => { assertSameShape({ a: 1, extra: 2 }, { a: 1 }, "factory vs reference"); },
      /factory vs reference: key sets differ\. only in actual: extra\./,
      "a key only the actual object carries should be reported"
    );
  });

  test("throws when the expected object has a key the actual object lacks", () => {

    assert.throws(

      () => { assertSameShape({ a: 1 }, { a: 1, missing: 2 }, "factory vs reference"); },
      /factory vs reference: key sets differ\. only in expected: missing\./,
      "a key only the expected object carries should be reported"
    );
  });

  test("reports both directions when keys differ on both sides", () => {

    // Asymmetric difference: factory adds 'extraA'; reference adds 'extraB'. The error message must include both directions.
    let captured: Error | null = null;

    try {

      assertSameShape({ a: 1, extraA: 2 }, { a: 1, extraB: 2 }, "two-way drift");
    } catch(err) {

      captured = err as Error;
    }

    assert.ok(captured, "should have thrown");
    assert.match(captured.message, /only in actual: extraA/, "the actual-only key should be named");
    assert.match(captured.message, /only in expected: extraB/, "the expected-only key should be named too");
  });

  test("sorts the reported keys alphabetically for deterministic error messages", () => {

    // Boundary: when multiple keys drift in the same direction, the error message lists them in sorted order so failure messages are deterministic across
    // platforms and Object.keys orderings.
    let captured: Error | null = null;

    try {

      // eslint-disable-next-line sort-keys
      assertSameShape({ z: 1, a: 2, m: 3 }, {}, "ordering");
    } catch(err) {

      captured = err as Error;
    }

    assert.ok(captured, "should have thrown");
    assert.match(captured.message, /only in actual: a, m, z/, "drifted keys should be listed in sorted order");
  });

  test("treats objects with the same keys but different values as equal in shape (values are not compared)", () => {

    // assertSameShape is a key-set check, not a value check. Two objects with the same keys but wildly different values pass.
    assert.doesNotThrow(() => {

      assertSameShape({ a: 1, b: 2 }, { a: "string", b: { nested: true } }, "values differ but keys match");
    }, "differing values should not register as a shape difference");
  });
});

describe("declareKeysOf", () => {

  /* The primary contract is compile-time: a const array passed to declareKeysOf<T>() must exhaust every key of T or the call fails to compile. This suite does
   * not exercise that failure path directly. The codebase's established @ts-expect-error convention (as used in src/types/channels.test.ts) could assert the
   * missing-key compile error here too, but is omitted so this suite stays focused on the runtime contract below. The runtime tests below verify the
   * pass-through behavior - the function returns the array unchanged - which is the contract this suite actually verifies.
   */

  interface Sample { a: number; b: string; c: boolean }

  test("returns the input array unchanged when the keys exhaust the type", () => {

    const keys = declareKeysOf<Sample>()([ "a", "b", "c" ] as const);

    assert.deepEqual([...keys], [ "a", "b", "c" ], "the declared keys should come back in order");
  });

  test("preserves the const-array literal type so callers can use the array as a tuple type source", () => {

    // The return type is K (the input's literal type), not (keyof T)[]. A test cannot directly observe the literal type at runtime, but we exercise the runtime
    // shape by reading individual indices and verifying the values match the source array.
    const keys = declareKeysOf<Sample>()([ "a", "b", "c" ] as const);

    assert.equal(keys[0], "a", "the first key should survive the pass-through");
    assert.equal(keys[1], "b", "the second key should survive the pass-through");
    assert.equal(keys[2], "c", "the third key should survive the pass-through");
    assert.equal(keys.length, 3, "no key should be added or dropped");
  });

  test("supports the empty-key case for a type with no keys", () => {

    // Boundary: a type with no keys (keyof resolves to never) passes an empty const array. The completeness check is trivially satisfied. Record<never, never>
    // is the canonical "no keys" type - Record<string, never> has an index signature so keyof resolves to string, not never.
    const keys = declareKeysOf<Record<never, never>>()([] as const);

    assert.equal(keys.length, 0, "a keyless type should declare an empty array");
  });

  test("array reference is the same as the input (no copy)", () => {

    // The function returns the array verbatim - no copy, no transformation. Tests that hold the result expect identity.
    const input = [ "a", "b", "c" ] as const;
    const result = declareKeysOf<Sample>()(input);

    assert.equal(result, input, "same reference");
  });
});
