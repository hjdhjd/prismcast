/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * sanitize.ts: Input sanitization utilities for PrismCast.
 */

/* Non-printable characters can silently corrupt string values when introduced via copy-paste operations or hand-edited JSON files. The most common case is null bytes
 * (\x00) interleaved between every visible character, which breaks CSS selector matching, URL parsing, and channel identification without any visible indication in
 * the UI. These utilities strip non-printable characters at data-collection boundaries so downstream code can trust that string values contain only visible content.
 *
 * The regex targets control characters and Unicode special characters that have no visible representation: C0 controls (except TAB, LF, CR), DEL, BOM, zero-width
 * spaces, and Unicode line/paragraph separators. TAB (\x09), LF (\x0A), and CR (\x0D) are preserved because they appear legitimately in multi-line text fields.
 */

// Pattern matching non-printable characters. Covers: C0 controls except TAB/LF/CR, DEL, BOM, zero-width spaces, directional markers, and line/paragraph separators.
// The no-control-regex rule is suppressed because this regex genuinely needs to match control characters — that is its entire purpose.
// eslint-disable-next-line no-control-regex
const NON_PRINTABLE_PATTERN = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\uFEFF\u200B-\u200F\u2028-\u2029]/g;

/**
 * Strips non-printable characters from a string and trims whitespace. Replaces `.trim()` at data-collection points to ensure string values contain only visible
 * content. Safe to call on already-clean strings — the regex replacement is a no-op when no non-printable characters are present.
 * @param value - The string to sanitize.
 * @returns The sanitized string with non-printable characters removed and whitespace trimmed.
 */
export function sanitizeString(value: string): string {

  return value.replace(NON_PRINTABLE_PATTERN, "").trim();
}

/**
 * Tests whether a string contains any non-printable characters. Used for startup warnings when loading persisted data — the warning alerts users to corruption
 * without modifying the loaded values.
 * @param value - The string to test.
 * @returns True if the string contains at least one non-printable character.
 */
export function containsNonPrintable(value: string): boolean {

  // Reset lastIndex before testing — the global flag on NON_PRINTABLE_PATTERN causes .test() to advance lastIndex on each call, which would produce alternating
  // true/false results without this reset.
  NON_PRINTABLE_PATTERN.lastIndex = 0;

  return NON_PRINTABLE_PATTERN.test(value);
}
