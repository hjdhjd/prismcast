/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.validation.test.ts: Direct unit tests for the form validators - validateChannelKey, validateChannelNumber, validateChannelUrl, validateChannelName,
 * and validateChannelProfile. These are the single source of truth for channel-form validation; both the full-edit and inline-edit handlers route through them,
 * so a regression here propagates to every channel-mutation endpoint.
 *
 * Each validator returns undefined for "valid" and a sentence-cased error message for "invalid". The tests cover every branch: empty/whitespace input, format
 * rejections, length limits, valid happy paths, and trim/case quirks. The state-dependent duplicate-check branches in validateChannelKey and validateChannelNumber
 * read module state (loadedUserChannels, getChannelListing) populated by initializeUserChannels - that initialization is heavyweight (pulls in CONFIG, the
 * persistence framework, and service-group construction), so those duplicate-check branches are not exercised by any test today. The pure branches are
 * exhaustively covered here.
 */
import { describe, test } from "node:test";
import { validateChannelKey, validateChannelName, validateChannelNumber, validateChannelProfile, validateChannelUrl,
  validateImportedChannels } from "./userChannels.ts";
import assert from "node:assert/strict";

describe("validateChannelUrl", () => {

  test("returns undefined for a valid http URL", () => {

    assert.equal(validateChannelUrl("http://example.com/stream.m3u8"), undefined);
  });

  test("returns undefined for a valid https URL", () => {

    assert.equal(validateChannelUrl("https://www.hulu.com/live"), undefined);
  });

  test("returns undefined for a URL with query string and fragment", () => {

    assert.equal(validateChannelUrl("https://example.com/path?foo=bar#hash"), undefined);
  });

  test("rejects an empty string with a sentence-cased message", () => {

    assert.equal(validateChannelUrl(""), "URL is required.");
  });

  test("rejects a whitespace-only string", () => {

    // The trim() check guards against forms that submit "  " from a cleared input field.
    assert.equal(validateChannelUrl("   "), "URL is required.");
  });

  test("rejects a malformed URL string with the parse-error message", () => {

    assert.equal(validateChannelUrl("not a url"), "Invalid URL format.");
  });

  test("rejects a URL missing a scheme", () => {

    assert.equal(validateChannelUrl("example.com/stream"), "Invalid URL format.");
  });

  test("rejects a URL with the file:// scheme as a protocol violation, not a parse error", () => {

    // file:// parses fine; the protocol allowlist rejects it. The two error messages are intentionally different so the form can surface the right hint.
    assert.equal(validateChannelUrl("file:///etc/passwd"), "URL must use http or https protocol.");
  });

  test("rejects a URL with the ftp:// scheme", () => {

    assert.equal(validateChannelUrl("ftp://example.com/stream"), "URL must use http or https protocol.");
  });

  test("rejects a URL with the javascript: scheme (XSS guard)", () => {

    // The protocol allowlist is a defense-in-depth measure: even if an attacker bypasses the form to inject a javascript: URL, the validator catches it before it
    // can be persisted to channels.json and rendered in an href.
    assert.equal(validateChannelUrl("javascript:alert(1)"), "URL must use http or https protocol.");
  });

  test("rejects a URL with the ws:// scheme", () => {

    assert.equal(validateChannelUrl("ws://example.com/socket"), "URL must use http or https protocol.");
  });
});

describe("validateChannelName", () => {

  test("returns undefined for a normal channel name", () => {

    assert.equal(validateChannelName("ABC"), undefined);
  });

  test("returns undefined for a name with spaces and punctuation", () => {

    assert.equal(validateChannelName("ABC News (East)"), undefined);
  });

  test("returns undefined for a name at the 100-character upper bound", () => {

    // Boundary: the validator's contract is "100 characters or less", so a 100-char name must pass.
    const exactly100 = "X".repeat(100);

    assert.equal(validateChannelName(exactly100), undefined);
  });

  test("rejects an empty string", () => {

    assert.equal(validateChannelName(""), "Channel name is required.");
  });

  test("rejects a whitespace-only string", () => {

    assert.equal(validateChannelName("   "), "Channel name is required.");
  });

  test("rejects a name longer than 100 characters", () => {

    // Boundary: 101 chars must fail. The error wording covers both the over-101 case and the exactly-101 case from the same branch.
    const oneHundredOne = "X".repeat(101);

    assert.equal(validateChannelName(oneHundredOne), "Channel name must be 100 characters or less.");
  });

  test("does NOT trim before measuring length (trailing spaces count toward the 100-char limit)", () => {

    // The validator measures the raw string length, not the trimmed length. This matters because forms may include incidental trailing spaces that the user
    // didn't intend to add - if those push the name over 100 chars, the validator rejects it. Documented here so a future "should we trim first?" question has
    // an authoritative test asserting current behavior.

    // 98 X's plus two spaces = 100 chars total, exactly at the boundary - passes.
    assert.equal(validateChannelName("X".repeat(98) + "  "), undefined, "100-char trailing-space name passes");

    // 99 X's plus two spaces = 101 chars total, one over - fails the length check.
    assert.equal(validateChannelName("X".repeat(99) + "  "), "Channel name must be 100 characters or less.", "101-with-trailing-space name fails");
  });
});

describe("validateChannelProfile", () => {

  const validProfiles = [ "default", "fox", "hulu", "sling" ];

  /* The validator asks a predicate whether a name exists rather than reading a list, so each case supplies the oracle its assertion needs. Production
   * callers answer the same question through the single builtin lookup plus the user's own profiles.
   */
  const isKnownProfile = (name: string): boolean => validProfiles.includes(name);

  test("returns undefined for an exact match in the valid profiles list", () => {

    assert.equal(validateChannelProfile("hulu", isKnownProfile), undefined);
  });

  test("returns undefined for an empty profile (autodetect)", () => {

    // Empty profile is the explicit "autodetect" sentinel. It is always valid regardless of validProfiles content.
    assert.equal(validateChannelProfile("", isKnownProfile), undefined);
  });

  test("returns undefined when profile is undefined (autodetect)", () => {

    // The form may not always submit a value for the profile field; undefined is treated identically to empty string.
    assert.equal(validateChannelProfile(undefined, isKnownProfile), undefined);
  });

  test("returns undefined for whitespace-only profile (autodetect)", () => {

    assert.equal(validateChannelProfile("   ", isKnownProfile), undefined);
  });

  test("returns an error naming the rejected value for an unknown profile", () => {

    /* The message names the value that was rejected and nothing else. The set of acceptable names includes the provider profiles, which the profile picker in
     * the web UI deliberately omits, so spelling that set out here would offer names the picker never presents - the picker is the discovery surface.
     */
    const result = validateChannelProfile("nonexistent", isKnownProfile);

    assert.equal(result, "Unknown profile: nonexistent.");
  });

  test("is case-sensitive (HULU != hulu)", () => {

    // Profile names are case-sensitive in the storage layer; the validator mirrors that. Documented because the user-facing form may not communicate this clearly.
    const result = validateChannelProfile("HULU", isKnownProfile);

    assert.match(result ?? "", /^Unknown profile: HULU/);
  });

  test("rejects every non-empty input when the predicate recognizes no name", () => {

    // Edge case: the caller's oracle knows nothing. Empty profile still passes (autodetect); any non-empty profile fails.
    const knowsNothing = (): boolean => false;

    assert.equal(validateChannelProfile("", knowsNothing), undefined);
    assert.equal(validateChannelProfile("anything", knowsNothing), "Unknown profile: anything.");
  });
});

/* validateChannelKey and validateChannelNumber rely on module state populated by initializeUserChannels (loadedUserChannels for key duplicates,
 * getChannelListing() for number duplicates). Bringing up the full state in a unit test would require initializing CONFIG, the persistence framework, and the
 * service-group machinery, so this file keeps unit tests on the pure helpers via __internalForTests. The pure branches of those validators (empty/format/length
 * checks) are tested here against representative inputs. For validateChannelKey we pass isNew=false, which structurally skips the duplicate check. For
 * validateChannelNumber we pass excludeKey="any-key"; the listing is still iterated, but the tested format-rejection inputs return before that loop, so only
 * the format-rejection paths are exercised. This is a deliberate split: the cheap pure paths are unit-tested here for fast localized failure messages, while
 * the duplicate-check branches are not exercised by any test today.
 */

describe("validateChannelKey - pure branches (isNew=false short-circuits the duplicate check)", () => {

  /* When isNew is false, the validator's duplicate check (`if(isNew && isUserChannel(key))`) is structurally skipped, so no module state is touched. Every other
   * branch (empty, format, length) runs identically. Tests below pass isNew=false to isolate the pure paths.
   */

  test("returns undefined for a valid lowercase-hyphenated key", () => {

    assert.equal(validateChannelKey("abc-hulu", false), undefined);
  });

  test("returns undefined for a digits-only key", () => {

    assert.equal(validateChannelKey("123", false), undefined);
  });

  test("returns undefined for a single-character key", () => {

    assert.equal(validateChannelKey("a", false), undefined);
  });

  test("returns undefined at the 50-character upper bound", () => {

    const exactly50 = "a".repeat(50);

    assert.equal(validateChannelKey(exactly50, false), undefined);
  });

  test("rejects an empty string", () => {

    assert.equal(validateChannelKey("", false), "Channel key is required.");
  });

  test("rejects a whitespace-only string", () => {

    assert.equal(validateChannelKey("   ", false), "Channel key is required.");
  });

  test("rejects an uppercase letter", () => {

    // The format regex is /^[a-z0-9-]+$/ - uppercase fails to keep keys URL-safe and case-stable across platforms.
    assert.equal(validateChannelKey("ABC", false), "Channel key must contain only lowercase letters, numbers, and hyphens.");
  });

  test("rejects an underscore", () => {

    // Underscores are intentionally excluded from the format - hyphens are the canonical separator and supporting both would create user-visible inconsistency.
    assert.equal(validateChannelKey("abc_hulu", false), "Channel key must contain only lowercase letters, numbers, and hyphens.");
  });

  test("rejects a space", () => {

    assert.equal(validateChannelKey("abc hulu", false), "Channel key must contain only lowercase letters, numbers, and hyphens.");
  });

  test("accepts a leading hyphen (the regex /^[a-z0-9-]+$/ permits it; asserted to catch a future tightening)", () => {

    // The regex allows leading hyphens by construction; this test asserts the current behavior so a future tightening of the regex (e.g., to require a leading
    // alphanumeric) is detected immediately rather than silently breaking imports.
    assert.equal(validateChannelKey("-abc", false), undefined, "current regex permits leading hyphen - documented");
  });

  test("rejects a special character (period, slash, etc.)", () => {

    assert.equal(validateChannelKey("abc.com", false), "Channel key must contain only lowercase letters, numbers, and hyphens.");
    assert.equal(validateChannelKey("abc/hulu", false), "Channel key must contain only lowercase letters, numbers, and hyphens.");
  });

  test("rejects a key longer than 50 characters", () => {

    // Boundary: 51 chars must fail. The error wording covers both the over-51 case and the exactly-51 case from the same branch.
    const fiftyOne = "a".repeat(51);

    assert.equal(validateChannelKey(fiftyOne, false), "Channel key must be 50 characters or less.");
  });

  test("format check fires before length check (a 51-char invalid key reports format, not length)", () => {

    // Branch ordering matters: the format regex runs before the length check, so a key that violates both reports the format error. This keeps the user's
    // attention on the higher-priority issue (the form will still over-trigger if they fix only one).
    const fiftyOneInvalid = "A".repeat(51);

    assert.equal(validateChannelKey(fiftyOneInvalid, false), "Channel key must contain only lowercase letters, numbers, and hyphens.");
  });
});

describe("validateChannelNumber - pure branches (excludeKey-irrelevant when value fails format check)", () => {

  /* When the input fails the numeric range check, the duplicate-loop iteration over getChannelListing() never runs. Tests below exercise those format-rejection
   * paths only; the duplicate path is covered by HTTP-endpoint integration tests where module state is already populated.
   */

  test("returns undefined for an empty string (no channel number is valid)", () => {

    // Empty string is the sentinel for "no channel number assigned" - this is intentionally distinct from "0" (which is invalid).
    assert.equal(validateChannelNumber("", "any-key"), undefined);
  });

  test("rejects non-numeric input", () => {

    assert.equal(validateChannelNumber("abc", "any-key"), "Channel number must be between 1 and 99999.");
  });

  test("rejects zero (below the 1-99999 range)", () => {

    assert.equal(validateChannelNumber("0", "any-key"), "Channel number must be between 1 and 99999.");
  });

  test("rejects negative numbers", () => {

    assert.equal(validateChannelNumber("-5", "any-key"), "Channel number must be between 1 and 99999.");
  });

  test("rejects numbers above 99999", () => {

    assert.equal(validateChannelNumber("100000", "any-key"), "Channel number must be between 1 and 99999.");
  });

  test("accepts the lower bound (1)", () => {

    // Boundary: 1 is the smallest valid channel number. The duplicate loop runs after the format check passes and iterates the full predefined catalog from
    // getChannelListing(), which is enumerated regardless of module init. No predefined channel carries a channelNumber, so nothing collides with 1. We assert
    // undefined here because the format check passed AND the duplicate iteration found no entry with channelNumber 1.
    assert.equal(validateChannelNumber("1", "any-key"), undefined);
  });

  test("accepts the upper bound (99999)", () => {

    assert.equal(validateChannelNumber("99999", "any-key"), undefined);
  });

  test("parses leading-zero numbers correctly (parseInt strips them)", () => {

    // parseInt("007", 10) returns 7 - the validator parses cleanly and the value is in range.
    assert.equal(validateChannelNumber("007", "any-key"), undefined);
  });

  test("accepts floating-point notation because parseInt truncates the fractional part (1.5 parses to 1, in range)", () => {

    // parseInt("1.5", 10) returns 1, which is in range. Document the current behavior: the validator only inspects the integer part. A future tightening to
    // reject decimals should update this test rather than silently break imports.
    assert.equal(validateChannelNumber("1.5", "any-key"), undefined, "parseInt truncates - documented");
  });
});

describe("validateImportedChannels", () => {

  /* The import-path validator is the single gate that strangers' JSON has to pass before becoming user channel data. It enforces every shape rule so a hand-
   * crafted or programmatically-generated import never leaves the channels store in a malformed state. Coverage focuses on each rejection branch: structural
   * type checks, required-field presence, sanitization round-trips, optional-field type rules, and cross-entry uniqueness for channelNumber.
   */

  const validProfiles = [ "default", "fox", "hulu" ];
  const isKnownProfile = (name: string): boolean => validProfiles.includes(name);

  test("rejects a non-object input (array)", () => {

    const result = validateImportedChannels([], isKnownProfile);

    assert.equal(result.valid, false);
    assert.deepEqual(result.errors, ["Invalid format: expected an object with channel definitions."]);
    assert.deepEqual(result.channels, {});
  });

  test("rejects a non-object input (string)", () => {

    const result = validateImportedChannels("not an object", isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /Invalid format/);
  });

  test("rejects a null input", () => {

    const result = validateImportedChannels(null, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /Invalid format/);
  });

  test("accepts an empty object (no channels imported, valid)", () => {

    /* Boundary case: an empty import is structurally valid and produces no channels. This is the no-op import.
     */
    const result = validateImportedChannels({}, isKnownProfile);

    assert.equal(result.valid, true);
    assert.deepEqual(result.channels, {});
    assert.deepEqual(result.errors, []);
  });

  test("rejects a channel with an invalid key", () => {

    const result = validateImportedChannels({ "INVALID KEY!": { name: "X", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /INVALID KEY!.*lowercase/);
    assert.equal(Object.keys(result.channels).length, 0, "invalid-key entries are not added to channels");
  });

  test("rejects a channel whose value is not an object", () => {

    const result = validateImportedChannels({ "valid-key": "not an object" }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /valid-key.*expected an object/);
  });

  test("rejects a channel whose value is an array", () => {

    const result = validateImportedChannels({ "valid-key": [] }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /valid-key.*expected an object/);
  });

  test("rejects a channel without a name field", () => {

    const result = validateImportedChannels({ "valid-key": { url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /name is required/);
  });

  test("rejects a channel with a non-string name", () => {

    const result = validateImportedChannels({ "valid-key": { name: 42, url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /name is required/);
  });

  test("rejects a channel with an empty-trimmed name", () => {

    const result = validateImportedChannels({ "valid-key": { name: "   ", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /name is required/);
  });

  test("rejects a channel without a url field", () => {

    const result = validateImportedChannels({ "valid-key": { name: "X" } }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /url is required/);
  });

  test("rejects a channel with an invalid URL protocol", () => {

    const result = validateImportedChannels({ "valid-key": { name: "X", url: "ftp://example.com" } }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /must use http or https/);
  });

  test("rejects a channel with an unknown profile", () => {

    const result = validateImportedChannels({ "valid-key": { name: "X", profile: "notreal", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /Unknown profile: notreal/);
  });

  test("accepts a channel with a known profile", () => {

    const result = validateImportedChannels({ "valid-key": { name: "X", profile: "hulu", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, true);
    assert.equal(result.channels["valid-key"]?.profile, "hulu");
  });

  test("accepts a channel without a profile (autodetect)", () => {

    const result = validateImportedChannels({ "valid-key": { name: "X", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, true);
    assert.equal(result.channels["valid-key"]?.profile, undefined);
  });

  test("captures optional stationId when present and string-typed", () => {

    const result = validateImportedChannels({ "valid-key": { name: "X", stationId: "12345", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, true);
    assert.equal((result.channels["valid-key"] as { stationId?: string }).stationId, "12345");
  });

  test("ignores a non-string stationId without raising an error", () => {

    /* The validator's stationId branch is `typeof === "string"` - non-string values are silently skipped (the field is optional). Documented current behavior;
     * importing { stationId: 12345 } does NOT fail the validation, but the field is dropped.
     */
    const result = validateImportedChannels({ "valid-key": { name: "X", stationId: 12345, url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, true);
    assert.equal("stationId" in (result.channels["valid-key"] ?? {}), false);
  });

  test("captures optional channelSelector when present and string-typed", () => {

    const result = validateImportedChannels({ "valid-key": { channelSelector: "ABC", name: "X", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, true);
    assert.equal((result.channels["valid-key"] as { channelSelector?: string }).channelSelector, "ABC");
  });

  test("rejects a channelNumber outside the 1-99999 range", () => {

    const result = validateImportedChannels({ "valid-key": { channelNumber: 100000, name: "X", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /channelNumber must be an integer between 1 and 99999/);
  });

  test("rejects a non-integer channelNumber", () => {

    const result = validateImportedChannels({ "valid-key": { channelNumber: 7.5, name: "X", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /channelNumber must be an integer/);
  });

  test("captures a valid integer channelNumber", () => {

    const result = validateImportedChannels({ "valid-key": { channelNumber: 7, name: "X", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, true);
    assert.equal((result.channels["valid-key"] as { channelNumber?: number }).channelNumber, 7);
  });

  test("rejects a non-finite tvgShift (Infinity)", () => {

    const result = validateImportedChannels({ "valid-key": { name: "X", tvgShift: Infinity, url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.match(result.errors[0] ?? "", /tvgShift must be a finite number/);
  });

  test("accepts a negative finite tvgShift (West Coast feed offset case)", () => {

    /* Negative tvgShift is valid: the user is shifting an earlier feed back to the current zone. The validator must not reject negatives.
     */
    const result = validateImportedChannels({ "valid-key": { name: "X", tvgShift: -3, url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, true);
    assert.equal((result.channels["valid-key"] as { tvgShift?: number }).tvgShift, -3);
  });

  test("rejects duplicate channelNumber across imported channels", () => {

    const result = validateImportedChannels({

      "first": { channelNumber: 7, name: "First", url: "https://a.example.com" },
      "second": { channelNumber: 7, name: "Second", url: "https://b.example.com" }
    }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.ok(result.errors.some((e) => e.includes("channelNumber 7 is already used")));
  });

  test("reports each error per channel and continues validating subsequent entries", () => {

    /* The validator does not bail at the first error - it accumulates all errors and reports them together so the operator can fix them in one pass. We seed
     * two distinct errors and assert both surface.
     */
    const result = validateImportedChannels({

      // The first entry omits the required name field.
      "first": { url: "https://a.example.com" },

      // The second entry uses a disallowed protocol.
      "second": { name: "Second", url: "ftp://b.example.com" }
    }, isKnownProfile);

    assert.equal(result.valid, false);
    assert.ok(result.errors.some((e) => e.includes("first") && e.includes("name is required")));
    assert.ok(result.errors.some((e) => e.includes("second") && e.includes("must use http or https")));
  });

  test("sanitizes string fields (strips non-printable characters)", () => {

    /* Sanitization is the post-type-check step that strips control characters from imported strings. We use a string with an embedded null character to confirm
     * the sanitize call is applied.
     */
    const result = validateImportedChannels({ "valid-key": { name: "X\x00Y", url: "https://example.com" } }, isKnownProfile);

    assert.equal(result.valid, true);
    /* The sanitize helper normalizes non-printable to nothing or whitespace; we just assert the null byte didn't survive verbatim. The exact behavior is the
     * sanitize helper's contract, not this validator's, so we don't assert the post-sanitize string shape.
     */
    assert.equal((result.channels["valid-key"] as { name?: string }).name?.includes("\x00"), false);
  });
});
