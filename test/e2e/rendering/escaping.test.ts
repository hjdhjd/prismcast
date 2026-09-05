/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * escaping.test.ts: Integration coverage for the guarantee that every server-rendered surface (HTML rows, tag manager, M3U playlist) safely handles
 * user-supplied strings. The escapeHtml helper at src/utils/markup.ts maps {<, >, &, ", '} to HTML entities; the table renderer
 * uses it on every user-content position; the M3U generator at src/routes/playlist.ts uses escapeM3uAttribute (src/utils/m3u.ts) to backslash-escape the
 * structural characters of an M3U quoted-string (`"` and `\`) and to collapse forbidden line breaks into a single space. Each test below asserts the contract
 * that surface is supposed to honor, and the tests use one fixture string with every dangerous character so a regression in any single replacement path
 * surfaces.
 *
 * The M3U tvg-name escaping test asserts the M3U attribute escaping contract: a user-defined channel whose display name carries a literal double-quote must
 * emit a backslash-escaped quote inside tvg-name="..." so the attribute terminates correctly and downstream parsers see a well-formed EXTINF line. We use
 * escapeM3uAttribute itself to compute the expected substring so the test and the helper share a single source of truth - if the helper's strategy ever
 * changes (different escape character, percent-encoding, etc.), the test continues to assert the contract without rewrites.
 *
 * Why HTTP integration vs. function-level rendering: the channel-name, channel-URL, and tag-name escaping tests call generateChannelRowHtml /
 * generateTagManagerBody directly, mirroring channels-table.test.ts and variant-display.test.ts. The M3U tvg-name escaping test hits the playlist endpoint
 * via bootApp because the M3U generator's emission path runs through Express's response pipeline - we want wire-level bytes, not the in-process function
 * output. The export/re-import round-trip test uses the export endpoint (HTTP) for the same reason: it's the surface a regression would actually hit.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { generateChannelRowHtml, generateTagManagerBody } from "../../../src/routes/config/channels/table.ts";
import { mutateChannels, setTagRegistry } from "../../../src/config/userChannels.ts";
import assert from "node:assert/strict";
import { escapeM3uAttribute } from "../../../src/utils/m3u.ts";
import { getProfiles } from "../../../src/config/profiles.ts";

// One fixture string with every escapeHtml-targeted character. Reusing the same fixture across tests means a regression in any single replacement (e.g., a
// renderer that escapes < and > but stops escaping &) is caught uniformly without duplicating the literal across each call site.
const DANGEROUS_NAME = "My <Channel> & \"Test\" 'It'";

describe("HTML escaping guarantees - table renderer", () => {

  test("a channel name with HTML special characters renders with every dangerous char escaped in the display row", async () => {

    /* The renderer at table.ts:927 wraps channel.name in a <span class="channel-name-cell">...</span> after passing it through escapeHtml. With DANGEROUS_NAME
     * as the channel name, the rendered HTML must contain the entity-encoded form (&lt;, &gt;, &amp;, &quot;, &#39;) and must NOT contain the raw < or > or
     * unescaped " inside any attribute value position - the four canonical XSS vectors that escapeHtml exists to neutralize. A regression that bypasses the
     * escape (e.g., a refactor that switches to a template literal for one of the cell builders) corrupts every row that surfaces user content.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["dangerous-name-channel"] = { name: DANGEROUS_NAME, url: "https://example.test/dangerous" };
    });

    const { displayRow } = generateChannelRowHtml("dangerous-name-channel", getProfiles());

    // Positive: every entity form must be present in the output.
    assert.match(displayRow, /&lt;Channel&gt;/, "< and > must appear as HTML entities");
    assert.match(displayRow, /&amp;/, "& must appear as &amp;");
    assert.match(displayRow, /&quot;Test&quot;/, "double-quote must appear as &quot;");
    assert.match(displayRow, /&#39;It&#39;/, "single-quote must appear as &#39;");

    // Negative: the literal raw substrings from the dangerous name must not appear unescaped anywhere in the display row. We check the whole substring rather
    // than individual characters because individual < or > or & are part of legitimate HTML structure (e.g., <td>, <span>, &amp; entities); the regression we
    // want to catch is the user content surviving without its escape.
    assert.equal(displayRow.includes("<Channel>"), false, "the literal '<Channel>' substring must not appear unescaped");
    assert.equal(displayRow.includes("\"Test\""), false, "the literal '\"Test\"' substring must not appear unescaped");
  });

  test("a channel URL with quote and ampersand characters renders with the URL safely escaped in the edit row value attribute", async () => {

    /* The edit row contains the channel URL inside an <input value="..."> attribute (table.ts:1180 -> generateTextField -> table.ts:131). A URL like
     * "https://example.test/path?a=1&b=2&c=\"3\"" carries both the ampersand (URL parameter separator) and the quote (a pathological but legal URL component
     * via percent-encoding upstream of us). The renderer must escape both so the attribute value is well-formed HTML. A regression here breaks the edit form's
     * field value (the input would render with an empty or truncated value) and is the kind of bug that only surfaces when a user happens to use unusual URLs.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const dangerousUrl = "https://example.test/path?a=1&b=2&c=\"3\"";

    await mutateChannels((data) => {

      data.channels["dangerous-url-channel"] = { name: "URL Test", url: dangerousUrl };
    });

    const { editRow } = generateChannelRowHtml("dangerous-url-channel", getProfiles());

    // The URL must appear inside an input's value="..." attribute with quotes and ampersands escaped.
    assert.match(editRow, /value="https:\/\/example\.test\/path\?a=1&amp;b=2&amp;c=&quot;3&quot;"/,
      "the URL's & must become &amp; and its embedded \" must become &quot; inside the value attribute");

    // Negative: the raw, unescaped URL must NOT appear inside any attribute value (which would corrupt the HTML by terminating the value early at the first ").
    assert.equal(editRow.includes("value=\"" + dangerousUrl + "\""), false,
      "the raw unescaped URL must not appear inside a value attribute - that would terminate the attribute at the first embedded quote");
  });

  test("a tag name with HTML special characters renders with every dangerous char escaped in the tag manager body", async () => {

    /* The tag manager body in generateTagManagerBody emits each tag's name in multiple positions per entry: once in the wrapper's data-tag="...", once in the
     * rename span's data-tag-name="...", once in the delete button's data-tag-name="...", and once as the visible label inside <span>. All positions go
     * through escapeHtml. A user who creates a tag with embedded special characters (the pattern attribute on the input does limit this, but the renderer must
     * still be safe against bypassed validation - persisted state can outlive any client-side check) needs every position to be safe.
     *
     * Note: tag-name validation upstream restricts the character set so users cannot land arbitrary characters via the UI. We seed the registry directly via
     * mutateChannels to bypass that check and verify the renderer's defense-in-depth. A regression here would mean: if validation is ever loosened, or if a
     * raw-imported channels.json carried a non-conforming tag, the tag manager would render it as exploitable HTML.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const dangerousTag = "news <important> & \"hot\"";

    // setTagRegistry routes through mutateChannels with a TagRegistry shape (deletedTags + tags) and triggers the post-write cache hydration that
    // getActiveTagVocabulary - which the renderer calls - depends on. Mutating data.tagRegistry directly would skip the cache update path and produce a stale
    // read; the tag vocabulary functions read from the module-level cache, not from the freshly-written file. Going through the public mutator is the rule.
    await setTagRegistry({ deletedTags: [], tags: [dangerousTag] });

    const body = generateTagManagerBody();

    // The visible label inside the badge must be escaped. We anchor on the badge class and the entity-encoded form so a regression that drops escaping on the
    // label specifically (but keeps it on the data attribute) surfaces here.
    assert.match(body, /class="tag-badge tag-editable"[^>]*>news &lt;important&gt; &amp; &quot;hot&quot;</,
      "the visible badge label must contain entity-encoded special characters");

    // Negative: the literal unescaped tag must not appear anywhere - that would mean at least one of the four positions skipped escaping.
    assert.equal(body.includes(dangerousTag), false, "the raw unescaped tag string must not appear anywhere in the tag manager body");
  });
});

describe("M3U escaping guarantees - playlist endpoint", () => {

  test("the M3U tvg-name attribute backslash-escapes embedded double-quote characters so the attribute terminates correctly", async () => {

    /* The M3U generator at src/routes/playlist.ts wraps every user-controlled attribute value in escapeM3uAttribute (src/utils/m3u.ts), which backslash-escapes
     * the structural characters of an RFC 8216 quoted-string (the value-terminating `"` and the escape character `\`) and collapses forbidden CR/LF into a
     * single space. A channel whose display name contains a literal double-quote (e.g., `ESPN "The Ocho"`) must emit `tvg-name="ESPN \"The Ocho\""` so the
     * attribute terminates at the closing quote rather than at the embedded one - a regression that drops the escape recurs the original bug where downstream
     * parsers (Channels DVR included) see a corrupted EXTINF line.
     *
     * We compute the expected substring by calling escapeM3uAttribute directly on the seed name, so the test and the helper share a single source of truth.
     * If the helper's escape strategy ever changes (different sequence, percent-encoding, validation-time stripping), the test continues to assert the end-to-end
     * contract: whatever the helper emits, the playlist surface must emit too.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const dangerousName = "ESPN \"The Ocho\"";

    await mutateChannels((data) => {

      data.channels["quoted-name-channel"] = { name: dangerousName, url: "https://example.test/ocho" };
    });

    const response = await fetch(urlFor("/playlist"));

    assert.equal(response.status, 200, "the playlist endpoint must return the M3U body");

    const body = await response.text();
    const extinfLine = body.split("\n").find((line) => line.includes("quoted-name-channel"));

    assert.ok(extinfLine, "the quoted-name-channel EXTINF line must be present in the playlist body");

    // Positive: the tvg-name attribute carries the helper's escaped output verbatim. Sharing the helper as the source of truth means this assertion adapts to
    // any future escape-strategy change without a manual rewrite.
    const expectedAttribute = "tvg-name=\"" + escapeM3uAttribute(dangerousName) + "\"";

    assert.ok(extinfLine.includes(expectedAttribute), "tvg-name attribute must contain the helper-escaped form: " + expectedAttribute);

    // Negative: the malformed back-to-back-quote sequence that an unescaped concatenation would produce must not appear anywhere on the line. This catches a
    // regression that re-introduces raw quoting even if the helper output happens to coincide with another substring.
    assert.doesNotMatch(extinfLine, /tvg-name="ESPN "The Ocho""/, "tvg-name attribute must not contain the raw unescaped name");

    // Structural shape: the tvg-name attribute as a whole must be a valid RFC-8216-style quoted-string with backslash-escapes. The pattern matches the opening
    // quote, then any sequence of escaped characters or non-quote characters, then the closing quote.
    assert.match(extinfLine, /tvg-name="(\\.|[^"])*"/, "tvg-name attribute must be a well-formed quoted-string with backslash-escapes");
  });
});

describe("Round-trip safety - export and re-import preserve dangerous characters", () => {

  test("a channel name with every escapeHtml-targeted character round-trips through JSON export and re-import byte-identical", async () => {

    /* The export endpoint emits channels via stringifySorted (sorted JSON), and the import endpoint replaces the channels map with the validated input. A
     * round-trip should be the identity function on the channel content - if any layer (export serializer, import validator, JSON.parse round) double-escapes,
     * decode-then-re-encode-drifts, or otherwise mutates the dangerous string, the round-trip fails. This protects users who back up their channels and later
     * restore them: the restored state must match the original byte-for-byte at the channel-content level.
     *
     * Implementation: seed a user channel with DANGEROUS_NAME, GET /config/channels/export, capture the body, POST /config/channels/import with that body,
     * read channels.json from disk, and compare the channel entry's name field. Bytes-identical at the field level is the contract; differences in the file's
     * surrounding metadata (timestamps, schema version, etc.) are not part of the channel-content guarantee.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateChannels((data) => {

      data.channels["roundtrip-channel"] = { name: DANGEROUS_NAME, url: "https://example.test/roundtrip" };
    });

    const exportResponse = await fetch(urlFor("/config/channels/export"));

    assert.equal(exportResponse.status, 200, "export must succeed");

    const exportBody = await exportResponse.text();

    // Sanity-check the export body itself preserved the dangerous name. JSON encoding turns the embedded `"` into `\"` and leaves the other special characters
    // alone (JSON syntax does not require escaping <, >, &, or '). A regression that double-escaped or HTML-encoded values inside the JSON body would surface
    // here as the wrong substring.
    assert.match(exportBody, /"name":\s*"My <Channel> & \\"Test\\" 'It'"/,
      "the export body must carry the dangerous name JSON-encoded but not HTML-encoded - JSON does not entity-encode <, >, &, or '");

    const importResponse = await fetch(urlFor("/config/channels/import"), {

      body: exportBody,
      headers: { "Content-Type": "application/json" },
      method: "POST"
    });

    assert.equal(importResponse.status, 200, "import must succeed; body: " + (await importResponse.clone().text()).slice(0, 200));

    // Read the persisted channels.json and verify the channel's name field is byte-identical to the original DANGEROUS_NAME at the field level. We use
    // readPersistedJson plus narrowing because the on-disk channels.json flattens channel entries to top-level keys alongside schemaVersion - the seeded
    // channel appears at parsed["roundtrip-channel"], not parsed.channels["roundtrip-channel"].
    const persisted = await readPersistedJson(ctx, "channels.json");

    assert.equal(typeof persisted, "object", "channels.json must parse to an object");

    const entry = (persisted as Record<string, unknown>)["roundtrip-channel"];

    assert.equal(typeof entry, "object", "the round-trip channel must exist in channels.json");

    const name = (entry as Record<string, unknown>)["name"];

    assert.equal(name, DANGEROUS_NAME, "the channel name must round-trip byte-identical through export and re-import");
  });
});
