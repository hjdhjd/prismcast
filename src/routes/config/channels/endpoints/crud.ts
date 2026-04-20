/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * crud.ts: RESTful CRUD endpoints for individual channel records.
 *
 * Five endpoints split from the legacy action-dispatched POST /config/channels. Each handler is a thin adapter: parse the typed body, delegate decisions to the
 * channelForm domain helpers, write through mutateChannels, respond with an envelope patch.
 *
 * Endpoints:
 *   POST   /config/channels                  - Create a new user channel.
 *   PUT    /config/channels/:key             - Full replace/edit. Decides between revert, variant revert, or delta override based on submitted values.
 *   DELETE /config/channels/:key             - Delete a user channel.
 *   POST   /config/channels/:key/revert      - Revert a predefined channel override to defaults.
 *   PATCH  /config/channels/:key             - Partial update (inline cell edits: channelNumber, stationId, hdhrEnabled, tags).
 */
import type { Channel, ChannelDelta } from "../../../../types/index.js";
import type { Express, Request, Response } from "express";
import { LOG, sanitizeString } from "../../../../utils/index.js";
import { type UserChannel, getPredefinedChannel, isPredefinedChannel, isUserChannel, mutateChannels, parseTagInput, sortTags, validateChannelKey,
  validateChannelName, validateChannelNumber, validateChannelProfile, validateChannelUrl } from "../../../../config/userChannels.js";
import { channelMatches, computePredefinedDelta, findMatchingVariant } from "../../../../config/channelForm.js";
import { getResolvedChannel, resolveServiceKey, setServiceSelection } from "../../../../config/services.js";
import { playlistHintForChange, playlistHintForDelta, playlistHintForStored } from "../http/playlistHint.js";
import { sendFormErrors, sendSuccess, sendValidationError } from "../http/envelope.js";
import type { ChannelFormValues } from "../../../../config/channelForm.js";
import { buildServiceFilterWarning } from "../http/serviceWarning.js";
import { getProfiles } from "../../../../config/profiles.js";
import { route } from "../http/handler.js";
import { updateChannelLogo } from "../../../../streaming/showInfo.js";

/* Single source of truth for fields that PATCH /config/channels/:key accepts. The tuple is the one place an inline-editable field is declared; the InlineEditBody
 * shape and INLINE_EDIT_LABELS record both derive from it. Adding a new inline-editable field is a single-line change to the tuple plus a label entry. The
 * `satisfies` constraint validates every entry is a real ChannelDelta key (PATCH writes through the delta model), so renaming or removing a field surfaces here
 * at compile time rather than silently orphaning the allowlist.
 */
const INLINE_EDIT_FIELDS = [ "channelNumber", "hdhrEnabled", "stationId", "tags" ] as const satisfies readonly (keyof ChannelDelta)[];

type InlineEditField = (typeof INLINE_EDIT_FIELDS)[number];

/* Inline-edit body shape accepted by PATCH /config/channels/:key. Derived from INLINE_EDIT_FIELDS + ChannelDelta so adding a field to the tuple automatically
 * extends the accepted body without a separate manual update. Each field is optional; the handler requires exactly one to be present per request.
 */
type InlineEditBody = { [K in InlineEditField]?: ChannelDelta[K] };

// Human-readable labels for inline-edit fields, used in the success message and log line. Record<InlineEditField, ...> enforces completeness at compile time.
const INLINE_EDIT_LABELS: Record<InlineEditField, string> = {

  channelNumber: "Channel number",
  hdhrEnabled: "HDHR lineup",
  stationId: "Station ID",
  tags: "Tags"
};

/**
 * Parses and sanitizes submitted form fields into the normalized ChannelFormValues shape plus the tag array.
 * @param body - The raw request body as a string-keyed record.
 * @returns The normalized form values and tag array ready for validation and comparison.
 */
function parseFormBody(body: Record<string, string | undefined>): { formValues: ChannelFormValues; tags: string[] } {

  const channelNumberStr = sanitizeString(body.channelNumber ?? "");

  const formValues: ChannelFormValues = {

    channelNumber: channelNumberStr ? parseInt(channelNumberStr, 10) : undefined,
    channelSelector: sanitizeString(body.channelSelector ?? ""),
    guideTitle: sanitizeString(body.guideTitle ?? ""),
    hdhrEnabled: body.hdhrEnabled !== "false",
    logoUrl: sanitizeString(body.logoUrl ?? ""),
    name: sanitizeString(body.name ?? ""),
    profile: sanitizeString(body.profile ?? ""),
    stationId: sanitizeString(body.stationId ?? ""),
    url: sanitizeString(body.url ?? "")
  };

  const tags = parseTagInput(sanitizeString(body.tags ?? ""));

  return { formValues, tags };
}

/**
 * Validates submitted form values and returns a map of field errors. An empty map means validation passed.
 * @param formValues - The normalized form values.
 * @param key - The channel key being created or edited.
 * @param isCreate - True for create (validate key), false for edit (key already exists).
 * @param validProfiles - List of valid profile names for the profile field check.
 * @returns Map of field name to error message. Empty when valid.
 */
function validateFormValues(formValues: ChannelFormValues, key: string, isCreate: boolean, validProfiles: string[]): Record<string, string> {

  const errors: Record<string, string> = {};

  // Channel number: re-derive the string representation since validateChannelNumber expects a string (it parses and range-checks internally).
  const channelNumberStr = (formValues.channelNumber !== undefined) ? String(formValues.channelNumber) : "";
  const channelNumberError = validateChannelNumber(channelNumberStr, key);

  if(channelNumberError) {

    errors.channelNumber = channelNumberError;
  }

  if(isCreate) {

    const keyError = validateChannelKey(key, true);

    if(keyError) {

      errors.key = keyError;
    }
  }

  const nameError = validateChannelName(formValues.name);

  if(nameError) {

    errors.name = nameError;
  }

  const urlError = validateChannelUrl(formValues.url);

  if(urlError) {

    errors.url = urlError;
  }

  const profileError = validateChannelProfile(formValues.profile, validProfiles);

  if(profileError) {

    errors.profile = profileError;
  }

  return errors;
}

/**
 * Builds a UserChannel record from validated form values and a tag array. Omits optional fields that are empty/absent so the stored record is minimal.
 * @param formValues - The normalized form values.
 * @param tags - The sorted tag array.
 * @returns The assembled UserChannel ready to store.
 */
function buildUserChannelFromForm(formValues: ChannelFormValues, tags: readonly string[]): UserChannel {

  const channel: UserChannel = {

    name: formValues.name,
    url: formValues.url
  };

  if(formValues.guideTitle) {

    channel.guideTitle = formValues.guideTitle;
  }

  if(formValues.logoUrl) {

    channel.logoUrl = formValues.logoUrl;
  }

  if(formValues.profile) {

    channel.profile = formValues.profile;
  }

  if(formValues.stationId) {

    channel.stationId = formValues.stationId;
  }

  if(formValues.channelSelector) {

    channel.channelSelector = formValues.channelSelector;
  }

  if(formValues.channelNumber !== undefined) {

    channel.channelNumber = formValues.channelNumber;
  }

  if(tags.length > 0) {

    channel.tags = [...tags];
  }

  // Absent hdhrEnabled means included by default. Only persist when explicitly disabled.
  if(!formValues.hdhrEnabled) {

    channel.hdhrEnabled = false;
  }

  return channel;
}

/**
 * Handles PUT /config/channels/:key for a channel that has a predefined base. Computes the delta, detects no-op/implicit-revert/variant-revert cases, and
 * returns the response envelope. Separated from the main handler to keep the decision tree readable.
 * @param key - The channel key being edited.
 * @param predefinedBase - The canonical predefined channel for this key.
 * @param formValues - The normalized form values.
 * @param tags - The sorted tag array.
 * @param res - The Express response.
 */
async function handlePredefinedEdit(key: string, predefinedBase: Channel, formValues: ChannelFormValues, tags: readonly string[], res: Response): Promise<void> {

  // First check: does the submission match what the form showed? The edit form is pre-populated with the resolved display channel (which may be a variant), so
  // saving without modification should be a no-op - preserve the existing override.
  const resolvedKey = resolveServiceKey(key);
  const displayChannel = getResolvedChannel(resolvedKey) ?? predefinedBase;

  if(channelMatches(displayChannel, formValues, tags)) {

    sendSuccess(res, { data: { key }, message: "No changes to save." });

    return;
  }

  // Second check: compute the delta vs the canonical predefined.
  const { delta, hasChanges } = computePredefinedDelta(predefinedBase, formValues, tags);

  if(!hasChanges) {

    // Submitted values match the predefined base exactly. If an override exists, treat as implicit revert.
    if(isUserChannel(key)) {

      let revertHint = "";

      await mutateChannels((channels) => {

        revertHint = playlistHintForStored(channels[key]);

        Reflect.deleteProperty(channels, key);
      });

      LOG.info("Channel '%s' reverted to predefined defaults (edit matched predefined values).", key);

      sendSuccess(res, { affectedKeys: [key], data: { key }, message: "Channel '" + key + "' reverted to defaults." + revertHint });

      return;
    }

    sendSuccess(res, { data: { key }, message: "No changes to save." });

    return;
  }

  // Third check: do the values match a sibling variant's predefined definition? That's an implicit revert-to-variant, not a new custom override.
  const matchedVariantKey = isUserChannel(key) ? findMatchingVariant(key, formValues, tags) : undefined;

  if(matchedVariantKey) {

    let variantRevertHint = "";

    await mutateChannels((channels) => {

      variantRevertHint = playlistHintForStored(channels[key]);

      Reflect.deleteProperty(channels, key);
    });

    setServiceSelection(key, matchedVariantKey);

    LOG.info("Channel '%s' reverted to variant '%s' (edit matched variant values).", key, matchedVariantKey);

    sendSuccess(res, { affectedKeys: [key], data: { key }, message: "Channel '" + key + "' reverted to defaults." + variantRevertHint });

    return;
  }

  // No match - store the delta and switch the service selection to the canonical key so the service dropdown shows "Custom".
  setServiceSelection(key, key);

  await mutateChannels((channels) => {

    channels[key] = delta;
  });

  LOG.info("User channel '%s' updated.", key);

  if(formValues.stationId) {

    updateChannelLogo(formValues.name || key, formValues.stationId);
  }

  sendSuccess(res, {

    affectedKeys: [key],
    data: { key },
    message: "Channel '" + key + "' updated successfully." + playlistHintForDelta(delta)
  });
}

/**
 * Registers the RESTful CRUD endpoints on the Express application.
 * @param app - The Express application.
 */
export function registerCrudRoutes(app: Express): void {

  // POST /config/channels - Create a new user channel.
  app.post("/config/channels", route("save channel", async (req: Request, res: Response) => {

    const body = req.body as Record<string, string | undefined>;
    const key = body.key?.trim();

    if(!key) {

      sendValidationError(res, "Channel key is required.");

      return;
    }

    const { formValues, tags } = parseFormBody(body);
    const profiles = getProfiles();
    const errors = validateFormValues(formValues, key, true, profiles.map((p) => p.name));

    if(Object.keys(errors).length > 0) {

      sendFormErrors(res, errors);

      return;
    }

    const channel = buildUserChannelFromForm(formValues, tags);

    await mutateChannels((channels) => {

      channels[key] = channel;
    });

    if(formValues.stationId) {

      updateChannelLogo(formValues.name || key, formValues.stationId);
    }

    LOG.info("User channel '%s' added.", key);

    sendSuccess(res, {

      affectedKeys: [key],
      data: { isNew: true, key },
      message: "Channel '" + key + "' added successfully.",
      playlistHint: true,
      serviceWarning: buildServiceFilterWarning(formValues.url)
    });
  }));

  // PUT /config/channels/:key - Edit an existing channel. Dispatches to the predefined-edit path (with delta/variant revert decisions) or the user-channel path
  // (direct replacement) based on whether a predefined base exists.
  app.put("/config/channels/:key", route("save channel", async (req: Request, res: Response) => {

    const key = (req.params as { key?: string }).key?.trim();

    if(!key) {

      sendValidationError(res, "Channel key is required.");

      return;
    }

    const body = req.body as Record<string, string | undefined>;
    const { formValues, tags } = parseFormBody(body);
    const profiles = getProfiles();
    const errors = validateFormValues(formValues, key, false, profiles.map((p) => p.name));

    if(Object.keys(errors).length > 0) {

      sendFormErrors(res, errors);

      return;
    }

    const predefinedBase = getPredefinedChannel(key);

    if(predefinedBase) {

      await handlePredefinedEdit(key, predefinedBase, formValues, tags, res);

      return;
    }

    // User-only channel: build the full record and replace. Snapshot the old record inside the mutation so we can compute the playlist hint from the actual diff.
    const channel = buildUserChannelFromForm(formValues, tags);
    let playlistHint = "";

    await mutateChannels((channels) => {

      const oldChannel = (key in channels) ? channels[key] as Channel : undefined;

      channels[key] = channel;
      playlistHint = playlistHintForChange(oldChannel, channel);
    });

    if(formValues.stationId) {

      updateChannelLogo(formValues.name || key, formValues.stationId);
    }

    LOG.info("User channel '%s' updated.", key);

    sendSuccess(res, {

      affectedKeys: [key],
      data: { key },
      message: "Channel '" + key + "' updated successfully." + playlistHint
    });
  }));

  // DELETE /config/channels/:key - Delete a user channel. If a predefined version exists with the same key, the patch replaces the row with the predefined original.
  app.delete("/config/channels/:key", route("delete channel", async (req: Request, res: Response) => {

    const key = (req.params as { key?: string }).key?.trim();

    if(!key) {

      sendValidationError(res, "Channel key is required.");

      return;
    }

    if(!isUserChannel(key)) {

      sendValidationError(res, "Cannot delete '" + key + "': it is not a user-defined channel.");

      return;
    }

    await mutateChannels((channels) => {

      Reflect.deleteProperty(channels, key);
    });

    LOG.info("User channel '%s' deleted.", key);

    sendSuccess(res, {

      affectedKeys: [key],
      data: { key },
      message: "Channel '" + key + "' deleted successfully.",
      playlistHint: true
    });
  }));

  // POST /config/channels/:key/revert - Remove the override of a predefined channel, restoring it to defaults.
  app.post("/config/channels/:key/revert", route("revert channel", async (req: Request, res: Response) => {

    const key = (req.params as { key?: string }).key?.trim();

    if(!key) {

      sendValidationError(res, "Channel key is required for revert.");

      return;
    }

    if(!isPredefinedChannel(key)) {

      sendValidationError(res, "Cannot revert '" + key + "': it is not a predefined channel.");

      return;
    }

    if(!isUserChannel(key)) {

      sendValidationError(res, "Cannot revert '" + key + "': no override exists.");

      return;
    }

    let revertHint = "";

    await mutateChannels((channels) => {

      revertHint = playlistHintForStored(channels[key]);

      Reflect.deleteProperty(channels, key);
    });

    LOG.info("Channel '%s' reverted to predefined defaults.", key);

    sendSuccess(res, {

      affectedKeys: [key],
      data: { key },
      message: "Channel '" + key + "' reverted to defaults." + revertHint
    });
  }));

  // PATCH /config/channels/:key - Partial update from inline cell edits. Accepts any subset of { channelNumber, hdhrEnabled, stationId, tags }. Each field is typed:
  // a typed value to set, null to clear.
  app.patch("/config/channels/:key", route("save channel", async (req: Request, res: Response) => {

    const key = (req.params as { key?: string }).key?.trim();

    if(!key) {

      sendValidationError(res, "Channel key is required.");

      return;
    }

    const body = req.body as InlineEditBody;

    // Filter against the explicit allowlist so unknown keys in the request body are ignored rather than mistakenly admitted. Exactly one field must be present -
    // the client's inline editors update one cell at a time.
    const presentFields = INLINE_EDIT_FIELDS.filter((k) => body[k] !== undefined);

    if(presentFields.length === 0) {

      sendValidationError(res, "No field provided.");

      return;
    }

    if(presentFields.length > 1) {

      sendValidationError(res, "Only one field may be updated per request.");

      return;
    }

    const field = presentFields[0];
    const value = body[field];

    // channelNumber validation uses the shared validator, which takes a string and re-parses. Feed it the numeric value rendered as a string.
    if(field === "channelNumber") {

      const rendered = (value === null) ? "" : String(value);
      const numberError = validateChannelNumber(rendered, key);

      if(numberError) {

        sendValidationError(res, numberError);

        return;
      }
    }

    await mutateChannels((channels) => {

      const stored = channels[key] ?? {};
      const delta = stored as ChannelDelta;

      switch(field) {

        case "channelNumber": {

          delta.channelNumber = (value === null) ? null : (value as number);

          break;
        }

        case "hdhrEnabled": {

          // The existing behavior: store false when disabling, null to revert to the default (included). true is equivalent to "clear the override".
          delta.hdhrEnabled = (value === false) ? false : null;

          break;
        }

        case "stationId": {

          delta.stationId = ((value === null) || (value === "")) ? null : (value as string);

          break;
        }

        case "tags": {

          const nextTags = (value === null) ? [] : (value as string[]);

          // Sort before storing so the normalizer's JSON.stringify equality check (used to collapse redundant deltas) sees a canonical order regardless of the
          // order the client submitted the array in.
          delta.tags = (nextTags.length > 0) ? sortTags(nextTags) : null;

          break;
        }
      }

      channels[key] = stored;
    });

    const fieldLabel = INLINE_EDIT_LABELS[field];

    // Display value for the log line. hdhrEnabled is rendered as included/excluded. Other fields show the value or "(cleared)".
    let displayValue: string;

    if(field === "hdhrEnabled") {

      displayValue = (value === false) ? "excluded" : "included";
    } else if((value === null) || (value === "") || (Array.isArray(value) && (value.length === 0))) {

      displayValue = "(cleared)";
    } else {

      displayValue = String(value);
    }

    LOG.info("Inline edit: %s for '%s' set to '%s'.", fieldLabel, key, displayValue);

    // hdhrEnabled changes don't affect the M3U playlist (HDHomeRun is a separate discovery path); every other inline edit may affect it.
    const applyPlaylistHint = (field !== "hdhrEnabled");

    sendSuccess(res, {

      affectedKeys: [key],
      data: { key },
      message: fieldLabel + " updated.",
      playlistHint: applyPlaylistHint
    });
  }));
}
