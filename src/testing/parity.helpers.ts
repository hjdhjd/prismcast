/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * parity.helpers.ts: Factory-parity utilities for catching silent drift between a fixture factory's output and the production type it's meant to mirror.
 *
 * The drift scenario these utilities protect against:
 *
 *   1. Production type T gains a new field (required or optional).
 *   2. The factory's literal does not get updated to populate it.
 *   3. Tests using the factory operate on a value that is "type-valid by exclusion of the new field" but structurally different from any real production
 *      value. Code paths that read the new field always see undefined when called via the factory, hiding regressions.
 *
 * TypeScript already catches required-field drift at compile time (the factory's return type would no longer satisfy T). The two failure modes that remain
 * are (a) optional fields that production always populates but the factory doesn't, and (b) value-shape drift where the factory's default for a field uses a
 * placeholder that production would never write. The first is caught at runtime by assertSameShape; the second is caught by tying factory defaults to
 * production formatters (see makeRegistryEntry's use of generateStreamId for an example).
 *
 * Two utilities live here:
 *
 * - assertSameShape: runtime key-set comparator with directional reporting (which keys are only in actual, which only in expected). Use this in factory
 *   tests to assert the factory's output has the same key set as a representative production value.
 *
 * - declareKeysOf<T>(): a compile-time completeness check. Wrap a const array of key names and the return type proves at compile time that every key of T is
 *   present in the array. Pair with assertSameShape to give factory tests both a compile-time gate (forces the key list to be updated when T changes) and a
 *   runtime gate (forces the factory to populate every key in the list).
 */

/**
 * Compares the key sets of two objects and throws a descriptive error when they differ. Reports the asymmetric difference - which keys are only in actual,
 * which only in expected - so a failure points exactly at the drifting fields rather than just "shapes differ."
 *
 * Use this in factory tests where the factory is meant to produce a value with every key of its target type. For factories that deliberately omit optional
 * fields, this strict comparison is too aggressive; use a subset/superset assertion instead.
 *
 * @param actual - The value under test (e.g., factory output).
 * @param expected - The reference value (e.g., a representative production value, or an object built from a complete-keys-of-T list).
 * @param description - A short label for the comparison, embedded in the failure message.
 * @throws Error when the two key sets differ, with the asymmetric difference in the message.
 */
export function assertSameShape(actual: object, expected: object, description: string): void {

  const actualKeys = new Set(Object.keys(actual));
  const expectedKeys = new Set(Object.keys(expected));

  const onlyInActual = [...actualKeys.difference(expectedKeys)].toSorted();
  const onlyInExpected = [...expectedKeys.difference(actualKeys)].toSorted();

  if((onlyInActual.length === 0) && (onlyInExpected.length === 0)) {

    return;
  }

  const parts: string[] = [];

  if(onlyInActual.length > 0) {

    parts.push("only in actual: " + onlyInActual.join(", "));
  }

  if(onlyInExpected.length > 0) {

    parts.push("only in expected: " + onlyInExpected.join(", "));
  }

  throw new Error(description + ": key sets differ. " + parts.join("; ") + ".");
}

/**
 * Declares a const array of key names and proves at compile time that the array exhausts every key of T. Returns a tagged function so the K type parameter
 * can be inferred from the array literal alone. The technique is the standard "satisfies-with-completeness-check" pattern: the return type's conditional
 * forces TypeScript to error when keys are missing, with a hint that names the missing keys in the error message.
 *
 * Usage:
 *
 *   const STREAM_REGISTRY_ENTRY_KEYS = declareKeysOf<StreamRegistryEntry>()([
 *     "channelName", "clientAddress", ... // every key
 *   ] as const);
 *
 * If StreamRegistryEntry gains a new key, the function call fails to compile with a "Type 'X' is not assignable to type" error that names the missing key,
 * forcing the array to be updated. Combine with assertSameShape to also catch drift in the factory: once the array is complete, the runtime check enforces
 * that the factory populates every key in it.
 *
 * @returns A function that accepts a const array and returns it unchanged when the array exhausts keyof T. Compile-time error otherwise.
 */
// The conditional type on the inner function's parameter produces either K (when complete) or a tagged tuple naming the missing keys (when incomplete). The
// tagged tuple cannot be assigned from a plain const array, so the call site fails to compile. The tag's literal "MISSING_KEYS:" surfaces in error messages.
export function declareKeysOf<T>(): <K extends readonly (keyof T)[]>(
  keys: K & ([Exclude<keyof T, K[number]>] extends [never] ? K : readonly ["MISSING_KEYS:", Exclude<keyof T, K[number]>])
) => K {

  return (keys) => keys;
}
