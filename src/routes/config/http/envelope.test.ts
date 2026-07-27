/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * envelope.test.ts: Unit tests for the response envelope helpers. The envelope is the SSOT for the /config/* endpoint response shape - every endpoint
 * routes its success/failure path through here, so the tests pin the contract: success bodies always carry success: true, error bodies always carry
 * success: false, reserved fields override caller-supplied data on collision, the playlistHint append fires only when both message and the flag are present, and
 * the FileStoreParseError branch produces a 400 with the parse details rather than a generic 500.
 */
import { describe, test } from "node:test";
import { sendConflictError, sendError, sendErrorResponse, sendFormErrors, sendNotFoundError, sendSuccess, sendValidationError } from "./envelope.ts";
import { FileStoreParseError } from "../../../config/persistence.ts";
import { PLAYLIST_HINT } from "../channels/http/playlistHint.ts";
import assert from "node:assert/strict";
import { makeReqRes } from "../../express.helpers.ts";

/* The envelope tests use the canonical makeReqRes helper; they only inspect res / status / json from the returned tuple - the req side is unused.
 */

describe("sendSuccess", () => {

  test("sends a body with success: true and no other fields when called with an empty payload", () => {

    const { json, res, status } = makeReqRes();

    sendSuccess(res);

    assert.equal(json.mock.callCount(), 1, "json should be called exactly once");
    assert.equal(status.mock.callCount(), 0, "status should not be called for success (defaults to 200)");
    assert.deepEqual(json.mock.calls[0]?.arguments[0], { success: true });
  });

  test("includes the message verbatim when no playlistHint flag is set", () => {

    const { json, res } = makeReqRes();

    sendSuccess(res, { message: "Channel saved." });

    assert.deepEqual(json.mock.calls[0]?.arguments[0], { message: "Channel saved.", success: true });
  });

  test("appends PLAYLIST_HINT when playlistHint is true and a message is present", () => {

    const { json, res } = makeReqRes();

    sendSuccess(res, { message: "Channel saved.", playlistHint: true });

    const body = json.mock.calls[0]?.arguments[0] as { message: string };

    assert.equal(body.message, "Channel saved." + PLAYLIST_HINT, "hint must be appended directly");
  });

  test("ignores playlistHint when no message is provided (no message means no append target)", () => {

    // Boundary: the playlistHint flag is silently dropped when there's no message - the helper appends to the message, not as a separate field. This protects
    // against a regression where the flag would somehow surface a hint with no preceding text.
    const { json, res } = makeReqRes();

    sendSuccess(res, { playlistHint: true });

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["message"], undefined, "no message should be set when none was provided");
  });

  test("merges data fields into the body before reserved envelope fields", () => {

    const { json, res } = makeReqRes();

    sendSuccess(res, { data: { extra: "value", isNew: true } });

    assert.deepEqual(json.mock.calls[0]?.arguments[0], { extra: "value", isNew: true, success: true });
  });

  test("reserved envelope fields override caller-supplied data on key collision (success cannot be hijacked)", () => {

    // The envelope contract guarantees success is always true on the success path regardless of what the caller passes. The body construction order writes
    // data first, then the reserved fields, so any collision resolves in favor of the reserved value.
    const { json, res } = makeReqRes();

    sendSuccess(res, { data: { message: "spoofed", success: false as unknown as boolean } });

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true, "reserved success must win over data.success");
  });

  test("attaches a serviceWarning verbatim when provided", () => {

    const { json, res } = makeReqRes();

    sendSuccess(res, { serviceWarning: { serviceLabel: "Hulu", serviceTag: "hulu" } });

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.deepEqual(body["serviceWarning"], { serviceLabel: "Hulu", serviceTag: "hulu" });
  });

  test("uses an explicit patch when provided rather than computing from affectedKeys", () => {

    // When the caller supplies a pre-built patch (e.g., counts-only patch from service-filter), the envelope must pass it through verbatim and skip the
    // buildChannelTablePatch derivation path. We mark it with a distinguishable shape so the test can verify pass-through.
    const { json, res } = makeReqRes();
    const patch = { counts: { disabled: 0, enabled: 1, predefined: 0, total: 1, user: 1 }, rows: [], scopeCounts: { all: { enabled: 1, total: 1 },
      east: { enabled: 0, total: 0 }, pacific: { enabled: 0, total: 0 } } };

    sendSuccess(res, { patch });

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["patch"], patch, "explicit patch must pass through by reference");
  });

  test("does not attach a patch when affectedKeys is absent or empty (no rows to update)", () => {

    // Boundary: an empty or absent affectedKeys array produces no patch since there's nothing to update. The envelope guards with `?.length`, which is undefined
    // for absent and 0 for empty - both falsy, both correctly skip the derivation path.
    const { json, res } = makeReqRes();

    sendSuccess(res, { affectedKeys: [] });

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal("patch" in body, false, "empty affectedKeys must not produce a patch field");

    // Sanity check: also test the absent case.
    const { json: json2, res: res2 } = makeReqRes();

    sendSuccess(res2, {});

    const body2 = json2.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal("patch" in body2, false, "absent affectedKeys must not produce a patch field");
  });
});

describe("sendError", () => {

  test("sends a 500 status with success: false and the error string", () => {

    const { json, res, status } = makeReqRes();

    sendError(res, 500, { error: "boom" });

    assert.equal(status.mock.callCount(), 1);
    assert.equal(status.mock.calls[0]?.arguments[0], 500, "status code passed through");
    assert.deepEqual(json.mock.calls[0]?.arguments[0], { error: "boom", success: false });
  });

  test("sends a field-keyed errors map when provided", () => {

    const { json, res, status } = makeReqRes();

    sendError(res, 400, { errors: { name: "Required.", url: "Invalid." } });

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
    assert.deepEqual(json.mock.calls[0]?.arguments[0], { errors: { name: "Required.", url: "Invalid." }, success: false });
  });

  test("attaches success: false even if the body somehow already declared success: true (reserved field wins)", () => {

    // The spread order writes `...body` first then `success: false` last, so a malformed caller cannot override the failure flag. Lock that contract. ErrorPayload's
    // open index signature `[extension: string]: unknown` makes `success: true` structurally valid as a payload key, so no cast is required to construct the
    // hostile input - the envelope marker still wins.
    const { json, res } = makeReqRes();

    sendError(res, 500, { error: "x", success: true });

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], false, "reserved success: false must win on collision");
  });
});

describe("sendValidationError", () => {

  test("sends a 400 with the validation message when called with a bare string (simple case)", () => {

    const { json, res, status } = makeReqRes();

    sendValidationError(res, "Channel key is required.");

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
    assert.deepEqual(json.mock.calls[0]?.arguments[0], { error: "Channel key is required.", success: false });
  });

  test("sends a 400 with extension fields preserved when called with an ErrorPayload (rich case)", () => {

    // The polymorphic helper accepts either a string or a rich ErrorPayload. Endpoints with diagnostic context (e.g., GET /playlist's "Invalid sort field"
    // response carrying validFields) pass the rich form; the extension fields are shipped verbatim alongside `error` and the envelope marker.
    const { json, res, status } = makeReqRes();

    sendValidationError(res, { error: "Invalid sort field: foo.", validFields: [ "callSign", "channelNumber", "name" ] });

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
    assert.deepEqual(json.mock.calls[0]?.arguments[0],
      { error: "Invalid sort field: foo.", success: false, validFields: [ "callSign", "channelNumber", "name" ] });
  });

  test("string and ErrorPayload forms produce byte-identical responses when the payload contains only { error }", () => {

    // The unifying guarantee: the simple case is a true special case of the rich case. A string argument and a `{ error: <string> }` payload must produce the
    // same wire format, so callers can adopt either form without affecting clients. This pin protects the polymorphic refactor from drifting into two
    // divergent code paths.
    const { json: jsonString, res: resString } = makeReqRes();

    sendValidationError(resString, "Bad input.");

    const { json: jsonPayload, res: resPayload } = makeReqRes();

    sendValidationError(resPayload, { error: "Bad input." });

    assert.deepEqual(jsonString.mock.calls[0]?.arguments[0], jsonPayload.mock.calls[0]?.arguments[0],
      "string and { error } payload must produce byte-identical responses");
  });

  test("envelope-enforced success: false wins over a caller-supplied success: true (envelope marker cannot be hijacked)", () => {

    /* The spread order in sendError writes the body first and `success: false` last, so a payload that somehow declared `success: true` cannot override the
     * failure flag. Lock that guarantee against accidental overrides via the polymorphic input.
     */
    const { json, res } = makeReqRes();

    sendValidationError(res, { error: "Bad.", success: true });

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], false, "reserved success: false must win over caller-supplied success: true");
  });
});

describe("sendNotFoundError", () => {

  test("sends a 404 with the not-found message", () => {

    const { json, res, status } = makeReqRes();

    sendNotFoundError(res, "Channel 'abc' not found.");

    assert.equal(status.mock.calls[0]?.arguments[0], 404);
    assert.deepEqual(json.mock.calls[0]?.arguments[0], { error: "Channel 'abc' not found.", success: false });
  });
});

describe("sendConflictError", () => {

  test("sends a 409 with the conflict message", () => {

    const { json, res, status } = makeReqRes();

    sendConflictError(res, "Tag already exists.");

    assert.equal(status.mock.calls[0]?.arguments[0], 409);
    assert.deepEqual(json.mock.calls[0]?.arguments[0], { error: "Tag already exists.", success: false });
  });
});

describe("sendFormErrors", () => {

  test("sends a 400 with the field-keyed errors map", () => {

    const { json, res, status } = makeReqRes();

    sendFormErrors(res, { name: "Required.", url: "Invalid URL." });

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
    assert.deepEqual(json.mock.calls[0]?.arguments[0], { errors: { name: "Required.", url: "Invalid URL." }, success: false });
  });

  test("accepts an empty errors map (caller's choice; envelope does not validate count)", () => {

    // Boundary: passing {} produces a 400 with an empty errors map. The envelope's job is to wrap, not to validate that callers supplied at least one error -
    // that's the caller's responsibility upstream.
    const { json, res } = makeReqRes();

    sendFormErrors(res, {});

    assert.deepEqual(json.mock.calls[0]?.arguments[0], { errors: {}, success: false });
  });
});

describe("sendErrorResponse", () => {

  /* sendErrorResponse calls LOG.error on the non-FileStoreParseError branch. Tests in this group accept that the production logger will emit to stderr -
   * the noise is harmless to the assertions and silencing it would require either mock.module() (which would couple us to the LOG export shape) or a
   * stderr spy whose type cast trips the lint rule. The two side-effect tests pass cleanly without suppression.
   */

  test("returns a 400 with the parse details when the error is a FileStoreParseError", () => {

    const { json, res, status } = makeReqRes();
    const err = new FileStoreParseError("channels", "/tmp/channels.json", "Unexpected token");

    sendErrorResponse(res, err, "save channel");

    assert.equal(status.mock.calls[0]?.arguments[0], 400, "FileStoreParseError must produce a 400");

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], false);
    assert.match(body["error"] as string, /channels/, "body should include the parse error message");
    assert.match(body["error"] as string, /Unexpected token/, "body should preserve the underlying parse detail");
  });

  test("returns a 500 with a formatted error message for a non-parse Error", () => {

    const { json, res, status } = makeReqRes();

    sendErrorResponse(res, new Error("disk full"), "save channel");

    assert.equal(status.mock.calls[0]?.arguments[0], 500);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], false);
    assert.match(body["error"] as string, /save channel/, "error message should include the action label");
    assert.match(body["error"] as string, /disk full/, "error message should include the underlying message");
  });

  test("returns a 500 with the formatted error for non-Error throwables (e.g., a thrown string)", () => {

    // Boundary: code that throws a non-Error value (rare, but legal in JS) flows through formatError. The envelope must still produce a 500 rather than crash
    // on an instanceof check failure.
    const { json, res, status } = makeReqRes();

    sendErrorResponse(res, "string-thrown", "save channel");

    assert.equal(status.mock.calls[0]?.arguments[0], 500);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], false);
    assert.match(body["error"] as string, /save channel/);
  });

  test("rich-payload form: ships the payload verbatim at the given status without logging or message rewriting", () => {

    /* Form 2 of the polymorphic signature: a numeric third arg selects the rich-payload mode. The helper attaches `success: false` and ships at the given
     * status; the caller's `error` text is preserved verbatim (no "Failed to <action>" wrapping) and extension fields pass through. This is the form used by
     * GET /logs's read-failure response, which needs to preserve the `entries: [], filtered, mode, total` shape that clients consume on success.
     */
    const { json, res, status } = makeReqRes();

    sendErrorResponse(res, { entries: [], error: "Failed to read log file.", filtered: 0, mode: "file", total: 0 }, 500);

    assert.equal(status.mock.calls[0]?.arguments[0], 500);
    assert.deepEqual(json.mock.calls[0]?.arguments[0],
      { entries: [], error: "Failed to read log file.", filtered: 0, mode: "file", success: false, total: 0 });
  });

  test("rich-payload form: caller-supplied success: true is overridden by the envelope marker", () => {

    // Mirror of the sendValidationError guarantee. The rich-payload form must also enforce success: false regardless of what the caller passes - the envelope
    // marker is reserved.
    const { json, res } = makeReqRes();

    sendErrorResponse(res, { error: "Bad.", success: true } as unknown as { error: string }, 500);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], false, "reserved success: false must win over caller-supplied success: true");
  });

  test("rich-payload form: simple-case-as-special-case-of-rich-case is symmetric across both helpers", () => {

    /* The unifying guarantee for sendErrorResponse: a `{ error: "X" }` rich payload at status 500 produces the same wire format as a sendValidationError-style
     * single-line error envelope at status 500 would, modulo the envelope's status logic. This is the same special-case-of-rich-case relationship that
     * sendValidationError tests, but applied to the form-2 path. Together with the sendValidationError pin, both helpers are byte-symmetric on the simple case.
     */
    const { json, res } = makeReqRes();

    sendErrorResponse(res, { error: "boom" }, 500);

    assert.deepEqual(json.mock.calls[0]?.arguments[0], { error: "boom", success: false },
      "rich payload with only { error } must produce the same wire format as the legacy ad-hoc shape");
  });
});
