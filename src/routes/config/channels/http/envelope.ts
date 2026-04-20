/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * envelope.ts: Response envelope helpers for channel config endpoints.
 *
 * Every channel config endpoint returns the same shape: success/failure flag, optional message, optional channel table patch, plus endpoint-specific fields. This
 * module is the single source of truth for that envelope. Endpoint handlers construct a typed SuccessPayload or call one of the sendXError helpers, and this
 * module handles the JSON serialization, patch construction, and tag-bundle attachment. Keeping this logic in one place ensures every endpoint agrees on the
 * response shape and that future additions (new bundle fields, new patch variants) require a change in one file.
 */
import type { ChannelTableCounts, ChannelTablePatch } from "../table.js";
import { LOG, formatError } from "../../../../utils/index.js";
import { buildChannelTablePatch, generateTagFilterContent, generateTagManagerBody } from "../table.js";
import { getActiveTagVocabulary, getTagRegistry } from "../../../../config/userChannels.js";
import { FileStoreParseError } from "../../../../config/persistence.js";
import { PLAYLIST_HINT } from "./playlistHint.js";
import type { Response } from "express";
import { getProfiles } from "../../../../config/profiles.js";

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

  body.success = true;

  if(payload.message !== undefined) {

    body.message = payload.playlistHint ? (payload.message + PLAYLIST_HINT) : payload.message;
  }

  // Patch derivation: an explicit patch wins; otherwise compute from affectedKeys. An empty or absent affectedKeys array produces no patch since there's nothing
  // to update (`?.length` is undefined for absent, 0 for empty - both falsy).
  if(payload.patch !== undefined) {

    body.patch = payload.patch;
  } else if(payload.affectedKeys?.length) {

    body.patch = buildChannelTablePatch(payload.affectedKeys, getProfiles());
  }

  // The tag UI bundle is attached verbatim to match the existing tag-endpoint contract the client expects.
  if(payload.tags) {

    body.active = getActiveTagVocabulary();
    body.filterContent = generateTagFilterContent();
    body.modalBody = generateTagManagerBody();
    body.registry = getTagRegistry();
  }

  if(payload.serviceWarning !== undefined) {

    body.serviceWarning = payload.serviceWarning;
  }

  res.json(body);
}

/**
 * Body shape accepted by sendError. Either a single `error` string (for top-level errors like validation, not-found, conflict, server failures) or a field-keyed
 * `errors` map (for form submissions that report multiple field errors at once). Every error response produced by this module uses one of these two shapes.
 */
export type ErrorBody = { error: string } | { errors: Record<string, string> };

/**
 * Sends an error response at the given status code with the given body. This is the single source of truth for non-success response shapes - every sendXError
 * helper below delegates here, ensuring every error path produces an envelope of the form `{ ...body, success: false }`.
 * @param res - The Express response object.
 * @param status - HTTP status code to send.
 * @param body - Either `{ error: string }` for top-level errors or `{ errors: Record<string, string> }` for field-level form errors.
 */
export function sendError(res: Response, status: number, body: ErrorBody): void {

  res.status(status).json({ ...body, success: false });
}

/**
 * Sends a 400 Bad Request response with a single error message. Use for invalid input that should be corrected and resubmitted.
 * @param res - The Express response object.
 * @param message - The validation error to surface.
 */
export function sendValidationError(res: Response, message: string): void {

  sendError(res, 400, { error: message });
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
 * Sends an error response for a caught exception. FileStoreParseError (corrupt JSON) produces a 400 with the parse details. All other errors produce a 500 with
 * the formatted error message. This is the single error-response path used by the route wrapper in handler.ts, so every endpoint handles exceptions uniformly.
 * @param res - The Express response object.
 * @param error - The caught error.
 * @param action - Human-readable description of the failed action for the log message (e.g., "toggle channel", "save channel").
 */
export function sendErrorResponse(res: Response, error: unknown, action: string): void {

  if(error instanceof FileStoreParseError) {

    sendError(res, 400, { error: error.message });

    return;
  }

  LOG.error("Failed to %s: %s.", action, formatError(error));
  sendError(res, 500, { error: "Failed to " + action + ": " + formatError(error) });
}
