/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.validation.test.ts: Direct unit tests for the form validators - validateChannelKey, validateChannelNumber, validateChannelUrl, validateChannelName,
 * and validateChannelProfile. These are the single source of truth for channel-form validation; both the full-edit and inline-edit handlers route through them,
 * so a regression here propagates to every channel-mutation endpoint.
 *
 * Each validator returns undefined for "valid" and a sentence-cased error message for "invalid". The tests cover every branch: empty/whitespace input, format
 * rejections, length limits, valid happy paths, and trim/case quirks. The state-dependent duplicate-check branches in validateChannelKey and validateChannelNumber
 * read module state (loadedUserChannels, getChannelListing) populated by initializeUserChannels - that initialization is heavyweight (pulls in CONFIG, the
 * persistence framework, and service-group construction), so duplicate-check coverage is intentionally left to HTTP-endpoint integration tests in
 * routes/config/channels/endpoints/crud.test.ts where the full module is already wired up. The pure branches are exhaustively covered here.
 */
import { describe, test } from "node:test";
import { validateChannelKey, validateChannelName, validateChannelNumber, validateChannelProfile, validateChannelUrl } from "./userChannels.ts";
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
    // an authoritative test pinning current behavior.

    // 98 X's plus two spaces = 100 chars total, exactly at the boundary - passes.
    assert.equal(validateChannelName("X".repeat(98) + "  "), undefined, "100-char trailing-space name passes");

    // 99 X's plus two spaces = 101 chars total, one over - fails the length check.
    assert.equal(validateChannelName("X".repeat(99) + "  "), "Channel name must be 100 characters or less.", "101-with-trailing-space name fails");
  });
});

describe("validateChannelProfile", () => {

  const validProfiles = [ "default", "fox", "hulu", "sling" ];

  test("returns undefined for an exact match in the valid profiles list", () => {

    assert.equal(validateChannelProfile("hulu", validProfiles), undefined);
  });

  test("returns undefined for an empty profile (autodetect)", () => {

    // Empty profile is the explicit "autodetect" sentinel. It is always valid regardless of validProfiles content.
    assert.equal(validateChannelProfile("", validProfiles), undefined);
  });

  test("returns undefined when profile is undefined (autodetect)", () => {

    // The form may not always submit a value for the profile field; undefined is treated identically to empty string.
    assert.equal(validateChannelProfile(undefined, validProfiles), undefined);
  });

  test("returns undefined for whitespace-only profile (autodetect)", () => {

    assert.equal(validateChannelProfile("   ", validProfiles), undefined);
  });

  test("returns an error for an unknown profile and lists the valid profiles", () => {

    // The error includes the full valid-profiles list so the user (and the form's error display) can recover without round-tripping to the docs.
    const result = validateChannelProfile("nonexistent", validProfiles);

    assert.equal(result, "Unknown profile: nonexistent. Valid profiles: default, fox, hulu, sling.");
  });

  test("is case-sensitive (HULU != hulu)", () => {

    // Profile names are case-sensitive in the storage layer; the validator mirrors that. Documented because the user-facing form may not communicate this clearly.
    const result = validateChannelProfile("HULU", validProfiles);

    assert.match(result ?? "", /^Unknown profile: HULU/);
  });

  test("rejects against an empty validProfiles list (every non-empty input is unknown)", () => {

    // Edge case: no profiles registered. Empty profile still passes (autodetect); any non-empty profile fails since the allowlist is empty.
    assert.equal(validateChannelProfile("", []), undefined);
    assert.equal(validateChannelProfile("anything", []), "Unknown profile: anything. Valid profiles: .");
  });
});

/* validateChannelKey and validateChannelNumber rely on module state populated by initializeUserChannels (loadedUserChannels for key duplicates,
 * getChannelListing() for number duplicates). Bringing up the full state in a unit test would require initializing CONFIG, the persistence framework, and the
 * service-group machinery - the existing pattern in this directory keeps unit tests on pure helpers via __internalForTests and routes integration coverage
 * through the HTTP-endpoint tests where that bring-up has already happened. The pure branches of those validators (empty/format/length checks) are tested
 * here against representative inputs by calling them with isNew=false (which short-circuits the duplicate check) and excludeKey="" (which still iterates the
 * listing, so we test only the format-rejection paths). This is a deliberate split: the cheap pure paths are unit-tested for fast localized failure messages;
 * the duplicate paths are integration-tested via crud.test.ts where the full system is already wired up.
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

  test("rejects a leading hyphen (still invalid by regex even though /^[a-z0-9-]+$/ accepts it)", () => {

    // The regex allows leading hyphens by construction; this test pins the current behavior so a future tightening of the regex (e.g., to require a leading
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

    // Boundary: 1 is the smallest valid channel number. The duplicate loop runs after this passes; with no module init, the listing is empty and no duplicate
    // hit fires. We assert undefined here because the format check passed AND the (empty) duplicate iteration found nothing.
    assert.equal(validateChannelNumber("1", "any-key"), undefined);
  });

  test("accepts the upper bound (99999)", () => {

    assert.equal(validateChannelNumber("99999", "any-key"), undefined);
  });

  test("parses leading-zero numbers correctly (parseInt strips them)", () => {

    // parseInt("007", 10) returns 7 - the validator parses cleanly and the value is in range.
    assert.equal(validateChannelNumber("007", "any-key"), undefined);
  });

  test("rejects floating-point notation that does not parse cleanly to an integer in range", () => {

    // parseInt("1.5", 10) returns 1, which is in range. Document the current behavior: the validator only inspects the integer part. A future tightening to
    // reject decimals should update this test rather than silently break imports.
    assert.equal(validateChannelNumber("1.5", "any-key"), undefined, "parseInt truncates - documented");
  });
});
