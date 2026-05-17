/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * markup.ts: HTML and XML escape utilities for PrismCast.
 */

/* This module is the single source of truth for escape-style markup encoding. HTML escaping (used pervasively across the routes layer to embed user-provided
 * values in server-rendered pages) and XML escaping (used by the macOS launchd plist generator) escape the same five characters; only the apostrophe entity
 * differs - HTML5 spells it &#39; and XML spells it &apos;. Centralizing both surfaces over a shared regex and a Readonly entity map keeps the character class
 * in one place so adding a sixth entity to one flavor can't silently desync the other.
 */

// The shared character class for both HTML and XML escape sweeps. Defined once so the two escape variants never disagree about which characters they encode.
const MARKUP_ENTITY_REGEX = /[&<>"']/g;

// HTML5 entity map. The apostrophe entity is &#39; because the named entity &apos; was only added to HTML5 (older browsers fall back to the numeric reference).
const HTML_ENTITIES: Readonly<Record<string, string>> = {

  "\"": "&quot;",
  "&": "&amp;",
  "'": "&#39;",
  "<": "&lt;",
  ">": "&gt;"
};

// XML entity map. The apostrophe entity is &apos;, one of the five predefined XML entities and the form launchd expects in plist files.
const XML_ENTITIES: Readonly<Record<string, string>> = {

  "\"": "&quot;",
  "&": "&amp;",
  "'": "&apos;",
  "<": "&lt;",
  ">": "&gt;"
};

/**
 * Shared escape body for both HTML and XML. Returns the input with every character in MARKUP_ENTITY_REGEX replaced by its entity from the supplied table. The
 * private factor here is intentional: it commits the two public escapers to the same regex and the same Record-based lookup so behavior never drifts between them.
 * @param value - The string to escape.
 * @param entities - The entity table that owns the per-character substitution.
 * @returns The escaped string.
 */
function escapeMarkup(value: string, entities: Readonly<Record<string, string>>): string {

  return value.replaceAll(MARKUP_ENTITY_REGEX, (char) => entities[char] ?? char);
}

/**
 * Escapes HTML special characters in a string to prevent XSS when displaying user-provided or dynamic content in HTML.
 * @param value - The text to escape.
 * @returns The escaped text safe for HTML display.
 */
export function escapeHtml(value: string): string {

  return escapeMarkup(value, HTML_ENTITIES);
}

/**
 * Escapes XML special characters in a string for safe embedding in XML documents. Used by the macOS launchd plist generator; the other platforms do not emit XML.
 * @param value - The text to escape.
 * @returns The escaped text safe for XML embedding.
 */
export function escapeXml(value: string): string {

  return escapeMarkup(value, XML_ENTITIES);
}

/**
 * Serializes a record of HTML attribute names to values into the inline fragment suitable for embedding in an opening tag. Three value semantics keep this tied
 * to the way HTML actually shapes attributes: undefined and boolean false are omitted entirely; boolean true emits the attribute name alone (HTML5 boolean
 * attribute form, e.g. {@code <input disabled>}); string values are HTML-escaped via {@link escapeHtml} and emitted as {@code name="value"}. The returned
 * fragment begins with a leading space when non-empty, so callers concatenate it directly after a preceding attribute without any spacing arithmetic of their
 * own. Renderers therefore pass raw values - boolean predicates and untrusted strings alike - and the encoding contract lives in exactly one place.
 * @param attrs - Attribute name/value pairs. Keys are emitted in insertion order.
 * @returns The serialized attribute fragment, or the empty string when no attribute would be emitted.
 */
export function serializeAttrs(attrs: Readonly<Record<string, string | boolean | undefined>>): string {

  const fragments: string[] = [];

  for(const [ name, value ] of Object.entries(attrs)) {

    if((value === undefined) || (value === false)) {

      continue;
    }

    if(value === true) {

      fragments.push(name);

      continue;
    }

    fragments.push(name + "=\"" + escapeHtml(value) + "\"");
  }

  return (fragments.length > 0) ? (" " + fragments.join(" ")) : "";
}
