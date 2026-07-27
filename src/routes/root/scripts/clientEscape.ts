/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * clientEscape.ts: The single client-side HTML-escape function and the browser-runtime assignment that installs it.
 */

/* This module is the single source of truth for client-side HTML escaping. Every client script that concatenates an untrusted value into innerHTML - the shared
 * channel/service renderers in shared.ts, the status display in status.handlers.ts, the browse and profile wizards in channels.ts, the changelog modal in
 * config.ts, and the inline log viewer in content.ts - routes through the one window.escapeHtml this module emits. The one documented exception is the browse
 * and profile wizards in channels.ts, which pre-populate a handful of text-input value attributes with a quote-only replaceAll that maps the double quote to
 * &quot; instead of the full escaper, since a double quote is the only attribute-breakout character those fields need to neutralize. There is exactly one
 * client-side HTML escaper, installed on window.escapeHtml.
 *
 * It is the browser-side twin of the server-side escapeHtml single source of truth in utils/markup.ts. clientEscapeHtml cannot import markup.escapeHtml: its body
 * ships to the browser verbatim via Function.prototype.toString(), where the import binding would be undefined, so the two are necessarily separate function
 * objects. The byte-parity guard in clientEscape.test.ts pins them identical across the full character set so a future edit to either - for example swapping the
 * HTML5 numeric apostrophe &#39; for the XML &apos; - cannot merge silently.
 */

/**
 * Escapes the five HTML special characters - & < > " ' - to their HTML5 entities, including the numeric apostrophe reference &#39;. This is the client mirror of
 * markup.escapeHtml. The five-character coverage is required: client renderers concatenate escaped values into innerHTML in both text-node and attribute
 * positions, and the double quote is the attribute-breakout vector, so a text-node-only escaper (a textContent round-trip pattern that leaves " and ' raw) would
 * be unsafe for the title/alt/value/data-* attribute sites that consume this. The body is deliberately self-contained - a literal
 * regex and an inline entity table, no module-scope helpers and no imports - because it is emitted to the browser via Function.prototype.toString() and may
 * therefore reference only its parameters and browser globals.
 * @param value - The text to escape.
 * @returns The escaped text safe for innerHTML insertion in both text and attribute positions.
 */
export function clientEscapeHtml(value: string): string {

  const entities: Record<string, string> = { "\"": "&quot;", "&": "&amp;", "'": "&#39;", "<": "&lt;", ">": "&gt;" };

  return value.replace(/[&<>"']/g, (char) => entities[char] ?? char);
}

/**
 * Emits the browser-runtime statements that install clientEscapeHtml as the global window.escapeHtml plus an IIFE-local const alias. The shared utilities script
 * concatenates this near the top of its IIFE so escapeHtml resolves - as the bare identifier inside shared.ts's own renderers and as window.escapeHtml for every
 * later client script (status, channels, config, and the inline log viewer) - before any of them runs. The const alias gives shared.ts's own renderers a direct
 * lexical binding (no global property lookup on the hot render path) while window.escapeHtml is the cross-script surface. The function source is serialized via
 * Function.prototype.toString(); TypeScript annotations strip to whitespace under Node's type stripping and to nothing under tsc compilation, so the runtime body
 * is identical between test and production.
 * @returns The two-line script fragment, indented two spaces to sit inside the shared utilities IIFE.
 */
export function generateClientEscapeAssignment(): string {

  return "  const escapeHtml = " + clientEscapeHtml.toString() + ";\n  window.escapeHtml = escapeHtml;";
}
