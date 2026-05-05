/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * narrowing.helpers.ts: Indexed-access narrowing helpers. Under noUncheckedIndexedAccess, every array index produces T | undefined; these helpers collapse the
 * destructure-and-assert.ok dance into a labeled call that throws with a useful error when the array is too short.
 */

/**
 * Returns the first element of an array, throwing if the array is empty. The returned value is narrowed from `T | undefined` (the type produced by indexed
 * access under noUncheckedIndexedAccess) to `T` in one call, declaring the "expected at least one" precondition at the helper boundary rather than scattering
 * a destructure-and-assert.ok dance at every site. The thrown error includes the label so failed assertions point at the specific kind of array that was
 * empty (e.g., "write", "execFile call").
 *
 * The alternative without this helper:
 *
 *   const [first] = items;
 *   assert.ok(first);
 *   first.field;
 *
 * Both narrow correctly; firstOf collapses the three lines into a labeled call.
 *
 * @param items - The array to read from. Read-only because the helper does not mutate.
 * @param label - Optional descriptive label folded into the error message when the array is empty (e.g., "write", "execFile call"). Defaults to "item".
 * @returns The first element, narrowed to T.
 * @throws Error when the array is empty.
 */
export function firstOf<T>(items: readonly T[], label = "item"): T {

  const [first] = items;

  if(first === undefined) {

    throw new Error("firstOf: expected at least one " + label + ", got empty array");
  }

  return first;
}

/**
 * Returns the element at the given index, throwing if the array does not extend that far. Same rationale as firstOf but for indexes other than 0 - in tests
 * where multiple sequential captures need to be inspected (e.g., the first execFile call, the second execFile call), this avoids repeating the destructure
 * pattern with a long ...rest tail.
 *
 * @param items - The array to read from.
 * @param index - The index to read.
 * @param label - Optional descriptive label folded into the error message.
 * @returns The element at index, narrowed to T.
 * @throws Error when the array is shorter than index + 1.
 */
export function nthOf<T>(items: readonly T[], index: number, label = "item"): T {

  const value = items[index];

  if(value === undefined) {

    throw new Error("nthOf: expected at least " + String(index + 1) + " " + label + "(s), got " + String(items.length));
  }

  return value;
}
