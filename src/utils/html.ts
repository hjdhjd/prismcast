/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * html.ts: HTML utilities for PrismCast.
 */

/* This utility provides HTML escaping for safely embedding dynamic content in HTML responses.
 */

/**
 * Escapes HTML special characters in a string to prevent XSS when displaying user-provided or dynamic content in HTML.
 * @param text - The text to escape.
 * @returns The escaped text safe for HTML display.
 */
export function escapeHtml(text: string): string {

  const replacements: Record<string, string> = {

    "\"": "&quot;",
    "&": "&amp;",
    "'": "&#39;",
    "<": "&lt;",
    ">": "&gt;"
  };

  return text.replace(/[&<>"']/g, (char) => {

    return replacements[char];
  });
}
