/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * envelope.ts: Response envelope helpers for /config/* endpoints.
 *
 * Every mutating /config/* endpoint returns the same shape: success/failure flag, optional message, optional channel table patch, plus endpoint-specific fields.
 * This module is the single source of truth for that envelope across the entire /config tree (settings, profiles, channels). Endpoint handlers construct a typed
 * SuccessPayload or call one of the sendXError helpers, and this module handles the JSON serialization, patch construction, and tag-bundle attachment. Keeping
 * this logic in one place ensures every endpoint agrees on the response shape and that future additions (new bundle fields, new patch variants) require a change
 * in one file.
 */
import type { ChannelTableCounts, ChannelTablePatch } from "../channels/table.ts";
import { LOG, formatError } from "../../../utils/index.ts";
import { buildChannelTablePatch, generateTagFilterContent, generateTagManagerBody } from "../channels/table.ts";
import { getActiveTagVocabulary, getTagRegistry } from "../../../config/userChannels.ts";
import { FileStoreParseError } from "../../../config/persistence.ts";
import { PLAYLIST_HINT } from "../channels/http/playlistHint.ts";
import type { Response } from "express";
import { getProfiles } from "../../../config/profiles.ts";

// Shape of a counts-only patch returned when a mutation affects summary counts but not specific rows. Used by service-filter and setup-completed where CSS-level
// visibility changes are applied client-side and only the header counters need server-side values.
export interface ChannelTableCountsOnlyPatch {

  counts: ChannelTableCounts;
  rows: readonly never[];
  scopeCounts: ChannelTablePatch["scopeCounts"];
}

/**
 * Input to sendSuccess describing the response payload. Fields not provided are omitted from the response.
 *
 * Field precedence: reserved envelope fields always win over `data`. Callers may put any keys in `data`; if a key collides with a reserved field, the reserved
 * value is written last and overrides. This protects the envelope contract from accidental `data: { success: false }` style collisions.
 */
export interface SuccessPayload {

  // Channel keys affected by this mutation. When present and non-empty, sendSuccess calls buildChannelTablePatch and attaches the result under `patch`. Use `patch`
  // directly when the caller already has a patch in hand or needs a counts-only variant.
  affectedKeys?: readonly string[];

  // Additional endpoint-specific fields merged into the response body before the reserved envelope fields. Reserved fields (success, message, patch, active,
  // filterContent, modalBody, registry, serviceWarning) always win if keys collide, so this is safe to pass arbitrary extras through.
  data?: Record<string, unknown>;

  // User-facing success message. Appended with PLAYLIST_HINT when `playlistHint` is true.
  message?: string;

  // Pre-built patch. Use this when affectedKeys isn't the right input (e.g., counts-only patches from service-filter, or a patch the caller built directly).
  patch?: ChannelTablePatch | ChannelTableCountsOnlyPatch;

  // When true, the response message has PLAYLIST_HINT appended. Ignored when message is absent.
  playlistHint?: boolean;

  // Warning when the new channel's service isn't in the active filter. The client shows this as a toast with a one-click enable action.
  serviceWarning?: { serviceLabel: string; serviceTag: string };

  // When true, the response includes the tag UI bundle (active, filterContent, modalBody, registry) used by tag management endpoints to refresh the filter
  // dropdown and tag manager modal after a tag vocabulary change.
  tags?: boolean;
}

/**
 * Sends a success response with the standardized envelope. Derives the channel table patch from affectedKeys when provided, attaches the tag UI bundle when
 * requested, and merges endpoint-specific data into the response body. Reserved envelope fields are written last so they always win over data-field collisions.
 * @param res - The Express response object.
 * @param payload - The fields to include in the response.
 */
export function sendSuccess(res: Response, payload: SuccessPayload = {}): void {

  // Start with data so reserved fields written below always win on collision. This protects the envelope contract from accidental overrides by caller-provided
  // data keys that happen to collide with reserved names.
  const body: Record<string, unknown> = (payload.data !== undefined) ? { ...payload.data } : {};

  body["success"] = true;

  if(payload.message !== undefined) {

    body["message"] = payload.playlistHint ? (payload.message + PLAYLIST_HINT) : payload.message;
  }

  // Patch derivation: an explicit patch wins; otherwise compute from affectedKeys. An empty or absent affectedKeys array produces no patch since there's nothing
  // to update (`?.length` is undefined for absent, 0 for empty - both falsy).
  if(payload.patch !== undefined) {

    body["patch"] = payload.patch;
  } else if(payload.affectedKeys?.length) {

    body["patch"] = buildChannelTablePatch(payload.affectedKeys, getProfiles());
  }

  // The tag UI bundle is attached verbatim to match the existing tag-endpoint contract the client expects.
  if(payload.tags) {

    body["active"] = getActiveTagVocabulary();
    body["filterContent"] = generateTagFilterContent();
    body["modalBody"] = generateTagManagerBody();
    body["registry"] = getTagRegistry();
  }

  if(payload.serviceWarning !== undefined) {

    body["serviceWarning"] = payload.serviceWarning;
  }

  res.json(body);
}

/**
 * Rich error payload shape: a required `error` message plus arbitrary extension fields the endpoint wants to attach (e.g., `validTags`, `validFields`,
 * `entries`, etc.). The simple `{ error: string }` envelope is a special case of this shape when no extensions are present, so the polymorphic helpers below
 * unify both under one mental model. Wire format on the response is `{ ...payload, success: false }`, with the envelope marker always winning on collision so
 * a caller cannot accidentally override it.
 */
export interface ErrorPayload {

  readonly error: string;

  // Extension fields the endpoint wants to attach. Any key other than `error` (and the reserved envelope marker `success`) is passed through verbatim. Reserved
  // marker collisions are resolved in favor of the envelope - see sendError.
  readonly [extension: string]: unknown;
}

/**
 * The polymorphic input accepted by sendValidationError and the rich-payload overload of sendErrorResponse. A bare string is the simple case (treated as
 * `{ error: input }`); an ErrorPayload is the rich case (extensions attached verbatim). The simple case is a true special case of the rich case under one helper.
 */
export type ErrorInput = ErrorPayload | string;

/**
 * Body shape accepted by sendError. Either a rich error payload (with optional extensions) for top-level errors, or a field-keyed `errors` map (for form
 * submissions that report multiple field errors at once). Every error response produced by this module uses one of these two shapes.
 */
export type ErrorBody = ErrorPayload | { errors: Record<string, string> };

/**
 * Sends an error response at the given status code with the given body. This is the single source of truth for non-success response shapes - every sendXError
 * helper below delegates here, ensuring every error path produces an envelope of the form `{ ...body, success: false }`. The spread order writes body first and
 * the reserved envelope marker last so a caller cannot accidentally override `success` from inside body.
 * @param res - The Express response object.
 * @param status - HTTP status code to send.
 * @param body - Either an ErrorPayload (with optional extension fields) for top-level errors or `{ errors: Record<string, string> }` for field-level form errors.
 */
export function sendError(res: Response, status: number, body: ErrorBody): void {

  res.status(status).json({ ...body, success: false });
}

/**
 * Sends a 400 Bad Request response with a validation error. Polymorphic in input: a bare string becomes `{ error: input }` (the simple case) and an
 * ErrorPayload is shipped verbatim with its extension fields preserved (the rich case for endpoints that want to attach diagnostic context like `validTags` or
 * `validFields`). The simple case and the rich case produce byte-identical wire formats when the rich payload contains only `{ error }`, so the polymorphic
 * signature subsumes both forms under one helper - the simple case is a true special case of the rich case.
 * @param res - The Express response object.
 * @param input - Either a validation message string or a rich ErrorPayload with extension fields.
 */
export function sendValidationError(res: Response, input: ErrorInput): void {

  sendError(res, 400, (typeof input === "string") ? { error: input } : input);
}

/**
 * Sends a 404 Not Found response. Use when the requested resource (channel, tag, etc.) does not exist.
 * @param res - The Express response object.
 * @param message - The error to surface.
 */
export function sendNotFoundError(res: Response, message: string): void {

  sendError(res, 404, { error: message });
}

/**
 * Sends a 409 Conflict response. Use when the request conflicts with current state (duplicate key, already exists, etc.).
 * @param res - The Express response object.
 * @param message - The error to surface.
 */
export function sendConflictError(res: Response, message: string): void {

  sendError(res, 409, { error: message });
}

/**
 * Sends a 400 Bad Request response with field-level error messages. Used by form-submission endpoints that surface multiple field errors at once.
 * @param res - The Express response object.
 * @param errors - Map of field name to validation error message.
 */
export function sendFormErrors(res: Response, errors: Record<string, string>): void {

  sendError(res, 400, { errors });
}

/**
 * Sends an error response. Two call shapes:
 *
 *   1. `sendErrorResponse(res, error, action)` - caught-exception form. The error (anything thrown, typed `unknown` to accept the result of `catch(error)`)
 *      is logged via LOG.error and shipped as `{ error: "Failed to <action>: <details>", success: false }` at 500. FileStoreParseError (corrupt JSON) is a
 *      special case that produces a 400 with the parse details. This is the form used by the route wrapper in handler.ts so every endpoint handles
 *      exceptions uniformly.
 *
 *   2. `sendErrorResponse(res, payload, status)` - rich-payload form. The payload is shipped verbatim at the given status with `success: false` appended.
 *      No log is emitted - the caller has already decided what message and status to ship. Use this when an endpoint needs to attach extension fields
 *      (e.g., `entries: [], filtered, mode, total` for /logs's read-failure response) that don't fit the simple `{ error }` envelope.
 *
 * The form is chosen by the third argument: a string third arg selects form 1 (action label), a number third arg selects form 2 (HTTP status). The two forms
 * are distinct in semantic - form 1 is "I caught an exception, log + envelope it"; form 2 is "I have a structured payload, ship it" - and TypeScript's
 * overload resolution surfaces the correct call shape at every call site.
 * @param res - The Express response object.
 * @param input - For form 1: the caught error. For form 2: a rich ErrorPayload to ship verbatim.
 * @param actionOrStatus - For form 1: a human-readable action label for the log message. For form 2: the HTTP status code to ship at.
 */
export function sendErrorResponse(res: Response, error: unknown, action: string): void;
export function sendErrorResponse(res: Response, payload: ErrorPayload, status: number): void;
export function sendErrorResponse(res: Response, input: unknown, actionOrStatus: number | string): void {

  // Form 2 (rich-payload): a numeric third arg means the caller is shipping a structured payload at a specific status. No logging - the caller controls the
  // message and status, the helper just appends the envelope marker.
  if(typeof actionOrStatus === "number") {

    sendError(res, actionOrStatus, input as ErrorPayload);

    return;
  }

  // Form 1 (caught-exception): the existing behavior, byte-preserved for every existing caller. FileStoreParseError surfaces parse details at 400; all other
  // throwables route through formatError and ship at 500 with the action label embedded for log correlation.
  if(input instanceof FileStoreParseError) {

    sendError(res, 400, { error: input.message });

    return;
  }

  LOG.error("Failed to %s: %s.", actionOrStatus, formatError(input));
  sendError(res, 500, { error: "Failed to " + actionOrStatus + ": " + formatError(input) });
}
