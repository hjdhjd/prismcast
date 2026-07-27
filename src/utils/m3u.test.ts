/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * m3u.test.ts: Unit tests for the M3U parsing and attribute-escaping primitives in m3u.ts. parseM3U is the bulk of the surface area; the tests cover happy-path
 * multi-channel parsing, the documented attribute-extraction priorities (tvc-guide-stationid > tvg-id; tvg-name > comma suffix), and the error-reporting edges (missing
 * URL, missing name). generateChannelKey is pure and exercised across the documented examples plus boundary inputs. escapeM3uAttribute is exercised against the empty
 * string and plain-text passthrough, backslash and double-quote escaping individually and combined (including escape-then-unescape round-trip), CR/LF-to-space
 * replacement (single and repeated), and non-ASCII passthrough.
 */
import { describe, test } from "node:test";
import { escapeM3uAttribute, generateChannelKey, parseM3U } from "./m3u.ts";
import assert from "node:assert/strict";

describe("generateChannelKey", () => {

  test("converts a simple name to a lowercase hyphenated key", () => {

    assert.equal(generateChannelKey("CNN Live"), "cnn-live");
  });

  test("strips non-alphanumeric punctuation", () => {

    assert.equal(generateChannelKey("CNN Live!"), "cnn-live");
  });

  test("collapses repeated whitespace into a single hyphen", () => {

    // Boundary: any run of \s+ becomes a single hyphen, not multiple.
    assert.equal(generateChannelKey("BBC   News"), "bbc-news");
  });

  test("collapses runs of hyphens that arise from multi-character separators", () => {

    // The slash in 24/7 is stripped; the surrounding spaces collapse, but the documented example shows "247" - the slash leaves no separator.
    assert.equal(generateChannelKey("BBC News 24/7"), "bbc-news-247");
  });

  test("trims leading and trailing whitespace before processing", () => {

    assert.equal(generateChannelKey("  Spaces  Everywhere  "), "spaces-everywhere");
  });

  test("returns the empty string for an empty-string input (boundary)", () => {

    assert.equal(generateChannelKey(""), "");
  });

  test("returns the empty string when the input has no alphanumerics", () => {

    // Negative test: pure punctuation strips to nothing, then trimming hyphens leaves "".
    assert.equal(generateChannelKey("!!! ???"), "");
  });

  test("preserves digits", () => {

    assert.equal(generateChannelKey("ABC 123"), "abc-123");
  });

  test("truncates to 50 characters maximum", () => {

    // Boundary: MAX_KEY_LENGTH is 50. A long input must be sliced to exactly 50 chars.
    const long = "a".repeat(200);
    const key = generateChannelKey(long);

    assert.equal(key.length, 50);
    assert.equal(key, "a".repeat(50));
  });

  test("strips leading and trailing hyphens introduced by removed punctuation", () => {

    // "!hello!" -> after lowercase/space/punctuation strip becomes "hello" with no leading/trailing hyphens.
    assert.equal(generateChannelKey("!Hello!"), "hello");
  });
});

describe("parseM3U", () => {

  test("parses a single-channel playlist with tvg-name", () => {

    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-name=\"CNN\" tvg-id=\"cnn.us\",Display Suffix",
      "https://example.test/cnn"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.errors.length, 0, "no errors for well-formed playlist");
    assert.equal(result.channels.length, 1);

    const channel = result.channels[0]!;

    assert.equal(channel.name, "CNN", "tvg-name takes precedence over comma suffix");
    assert.equal(channel.url, "https://example.test/cnn");
    assert.equal(channel.stationId, "cnn.us", "stationId from tvg-id when no tvc-guide-stationid present");
  });

  test("falls back to the comma suffix when tvg-name is absent", () => {

    // The implementation prefers tvg-name; in its absence it uses the part after the last comma in the EXTINF line.
    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-id=\"id\",Display Name",
      "https://example.test/x"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels[0]?.name, "Display Name");
  });

  test("prefers tvc-guide-stationid over tvg-id (Channels DVR compatibility)", () => {

    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-name=\"CNN\" tvc-guide-stationid=\"99999\" tvg-id=\"fallback\",CNN",
      "https://example.test/cnn"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels[0]?.stationId, "99999");
  });

  test("returns undefined stationId when neither attribute is present", () => {

    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-name=\"CNN\",CNN",
      "https://example.test/cnn"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels[0]?.stationId, undefined);
  });

  test("parses unquoted attribute values", () => {

    // Boundary: tvg-name=Channel (no quotes) is also supported. Stops at whitespace or comma.
    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-name=Channel tvg-id=cnn.us,Display",
      "https://example.test/cnn"
    ].join("\n");

    const result = parseM3U(content);

    const channel = result.channels[0]!;

    assert.equal(channel.name, "Channel");
    assert.equal(channel.stationId, "cnn.us");
  });

  test("parses multiple channels in a single playlist", () => {

    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-name=\"CNN\",CNN",
      "https://example.test/cnn",
      "#EXTINF:-1 tvg-name=\"BBC\",BBC",
      "https://example.test/bbc"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels.length, 2);
    assert.equal(result.channels[0]?.name, "CNN");
    assert.equal(result.channels[1]?.name, "BBC");
  });

  test("handles CRLF line endings transparently", () => {

    // The implementation splits on /\r?\n/ so Windows-style line endings work.
    const content = "#EXTM3U\r\n#EXTINF:-1 tvg-name=\"CNN\",CNN\r\nhttps://example.test/cnn\r\n";

    const result = parseM3U(content);

    assert.equal(result.channels.length, 1);
    assert.equal(result.channels[0]?.name, "CNN");
  });

  test("skips blank lines and unrelated comment lines", () => {

    const content = [
      "#EXTM3U",
      "",
      "# a regular comment, not extinf",
      "#EXTINF:-1 tvg-name=\"CNN\",CNN",
      "",
      "https://example.test/cnn",
      ""
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels.length, 1);
    assert.equal(result.errors.length, 0);
  });

  test("reports an error for an EXTINF line that is not followed by a URL", () => {

    // Negative test: a stray EXTINF without a URL should generate a "Missing URL" error.
    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-name=\"CNN\",CNN"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels.length, 0);
    assert.equal(result.errors.length, 1);
    assert.match(result.errors[0]!, /Line 2.*Missing URL/);
  });

  test("reports an error when one EXTINF is followed by another EXTINF (missing URL)", () => {

    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-name=\"CNN\",CNN",
      "#EXTINF:-1 tvg-name=\"BBC\",BBC",
      "https://example.test/bbc"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels.length, 1, "BBC still parses successfully");
    assert.equal(result.errors.length, 1, "the orphaned CNN EXTINF generates one error");
    assert.match(result.errors[0]!, /Line 2/);
  });

  test("reports an error when EXTINF has neither tvg-name nor a comma suffix", () => {

    // Negative test: EXTINF without an extractable name. The implementation still emits an error, then continues.
    const content = [
      "#EXTM3U",
      "#EXTINF:-1",
      "https://example.test/x"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels.length, 0);
    assert.equal(result.errors.length, 1);
    assert.match(result.errors[0]!, /Could not extract channel name/);
  });

  test("silently skips a URL line that has no preceding EXTINF", () => {

    // The implementation drops orphaned URLs without an error - there's no name to associate, so the channel cannot be constructed.
    const content = [
      "#EXTM3U",
      "https://example.test/orphan"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels.length, 0);
    assert.equal(result.errors.length, 0, "orphaned URLs are skipped silently, not flagged");
  });

  test("returns empty arrays for an empty playlist (boundary)", () => {

    const result = parseM3U("");

    assert.equal(result.channels.length, 0);
    assert.equal(result.errors.length, 0);
  });

  test("returns empty arrays for a header-only playlist (boundary)", () => {

    const result = parseM3U("#EXTM3U\n");

    assert.equal(result.channels.length, 0);
    assert.equal(result.errors.length, 0);
  });

  test("trims whitespace around tvg-name values", () => {

    // The implementation calls .trim() on extracted values; locked here.
    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-name=\"  CNN  \",CNN",
      "https://example.test/cnn"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels[0]?.name, "CNN");
  });

  test("falls back to comma suffix when tvg-name is empty/whitespace", () => {

    // Negative test: an empty tvg-name does not stop the fallback. The function must still try the comma suffix.
    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-name=\"\",Display Suffix",
      "https://example.test/x"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels[0]?.name, "Display Suffix");
  });

  test("accepts http:// URLs as well as https://", () => {

    const content = [
      "#EXTM3U",
      "#EXTINF:-1 tvg-name=\"CNN\",CNN",
      "http://example.test/cnn"
    ].join("\n");

    const result = parseM3U(content);

    assert.equal(result.channels.length, 1);
    assert.equal(result.channels[0]?.url, "http://example.test/cnn");
  });

  test("reports a trailing-EXTINF error if the file ends after EXTINF without a URL", () => {

    // The implementation checks for pendingExtinf at end-of-loop and reports if it's still set.
    const content = "#EXTM3U\n#EXTINF:-1 tvg-name=\"X\",X";

    const result = parseM3U(content);

    assert.equal(result.errors.length, 1);
    assert.match(result.errors[0]!, /Missing URL/);
  });
});

describe("escapeM3uAttribute", () => {

  test("returns the empty string unchanged (boundary)", () => {

    assert.equal(escapeM3uAttribute(""), "");
  });

  test("returns a plain alphanumeric string unchanged (no special characters present)", () => {

    // The implementation only rewrites \, ", CR, and LF - everything else (letters, digits, spaces, punctuation, semicolons, ampersands, the comma separator)
    // is a legal quoted-string character per RFC 8216 section 4.2 and must pass through verbatim.
    assert.equal(escapeM3uAttribute("CNN HD News & Sports; 24/7"), "CNN HD News & Sports; 24/7");
  });

  test("backslash-escapes an embedded double-quote character", () => {

    // The bug this helper exists to prevent: a raw double-quote inside an attribute value terminates the attribute early and corrupts the EXTINF line.
    assert.equal(escapeM3uAttribute("ESPN \"The Ocho\""), "ESPN \\\"The Ocho\\\"");
  });

  test("backslash-escapes an embedded backslash character", () => {

    // A backslash in user data must round-trip safely. If we did not escape backslash, the unescaper on the consumer side would consume our escape character and
    // mis-decode subsequent escape sequences.
    assert.equal(escapeM3uAttribute("path\\to\\file"), "path\\\\to\\\\file");
  });

  test("escapes both backslash and double-quote in the same input without re-escaping introduced backslashes", () => {

    // Order-of-operations check: replacing \ before " ensures we do not turn the backslashes we introduce around " into double-escaped \\". The expected output
    // contains exactly one extra backslash for each input \ and one extra backslash for each input " - no more.
    assert.equal(escapeM3uAttribute("a\\b\"c"), "a\\\\b\\\"c");
  });

  test("replaces a literal line feed with a single space", () => {

    // RFC 8216 section 4.2 forbids LF in a quoted-string and defines no escape sequence. The helper substitutes a single space so the attribute remains on one
    // line and the surrounding text is not silently glued together.
    assert.equal(escapeM3uAttribute("first\nsecond"), "first second");
  });

  test("replaces a literal carriage return with a single space", () => {

    // Same constraint as LF; CR is also forbidden in quoted-strings with no portable escape.
    assert.equal(escapeM3uAttribute("first\rsecond"), "first second");
  });

  test("collapses a CRLF or repeated line breaks into a single space", () => {

    // The replacement uses [\r\n]+ so a Windows-style CRLF or a multi-line block becomes one space, not two or more, avoiding visible whitespace runs in the
    // user's guide output.
    assert.equal(escapeM3uAttribute("a\r\nb\n\nc"), "a b c");
  });

  test("passes through non-ASCII Unicode characters unchanged", () => {

    // M3U is byte-oriented; quoted-strings carry whatever the producer wrote. Non-ASCII (UTF-8 multi-byte sequences, emoji, accented characters) is legal and
    // must round-trip byte-identical so localized channel names render correctly.
    assert.equal(escapeM3uAttribute("Téléfútbol 日本 \u{1F4FA}"), "Téléfútbol 日本 \u{1F4FA}");
  });

  test("escapes are recoverable: stripping the escape backslashes returns the original quotes and backslashes", () => {

    // A consumer that parses backslash-escapes (replace \" -> " and \\ -> \) on the helper's output gets back the original input. This is the round-trip
    // contract the helper must honor.
    const original = "ESPN \"The Ocho\" + path\\to\\file";
    const escaped = escapeM3uAttribute(original);

    // Decode in reverse order to undo escaping correctly: handle \" first (it was applied last), then collapse \\ back to \.
    const decoded = escaped.replace(/\\"/g, "\"").replace(/\\\\/g, "\\");

    assert.equal(decoded, original);
  });
});
