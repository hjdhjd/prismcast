/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fn.helpers.ts: Function-shaped test fixtures.
 */

/**
 * No-op function returning undefined. Used wherever a test needs a stub callback to satisfy a contract without doing anything observable. Centralizing this as
 * a named export rather than declaring local bare-arrow `() => {}` literals at each test site avoids the @typescript-eslint/no-empty-function rule and gives
 * the codebase a single, grep-able sentinel for "intentionally does nothing" test stubs.
 */
export function noop(): void {

  return undefined;
}
