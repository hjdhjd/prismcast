/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * plainObject.ts: Plain-object predicate for PrismCast.
 */

/**
 * Predicate: is the value a plain object (not array, not null, not a class instance like Date)?
 * @param value - The candidate.
 * @returns True if value is a plain object literal.
 */
export function isPlainObject(value: unknown): value is Record<string, unknown> {

  if((value === null) || (typeof value !== "object") || Array.isArray(value)) {

    return false;
  }

  // Object.getPrototypeOf is typed as returning any in the standard lib. Cast through unknown so the comparisons below are type-safe.
  const proto = Object.getPrototypeOf(value) as unknown;

  return (proto === null) || (proto === Object.prototype);
}
