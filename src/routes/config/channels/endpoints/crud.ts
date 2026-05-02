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
import { CHANNEL_BINDING_KEYS, CHANNEL_IDENTITY_KEYS } from "../../../../types/index.js";
import type { ChannelDelta, ResolvedChannel, StoredChannel } from "../../../../types/index.js";
import type { Express, Request, Response } from "express";
import { LOG, sanitizeString } from "../../../../utils/index.js";
import { type UserChannel, clearChannelOverrides, getPredefinedChannel, isPredefinedChannel, isUserChannel, mutateChannels, parseTagInput, sortTags,
  validateChannelKey, validateChannelName, validateChannelNumber, validateChannelProfile, validateChannelUrl } from "../../../../config/userChannels.js";
import { channelMatches, computePredefinedDelta, findMatchingVariant } from "../../../../config/channelForm.js";
import { getResolvedChannel, resolveServiceKey } from "../../../../config/services.js";
import { playlistHintForChange, playlistHintForDelta, playlistHintForStored } from "../http/playlistHint.js";
import { sendFormErrors, sendSuccess, sendValidationError } from "../http/envelope.js";
import type { ChannelFormValues } from "../../../../config/channelForm.js";
import { PREDEFINED_CHANNELS } from "../../../../channels/index.js";
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

  const channelNumberStr = sanitizeString(body["channelNumber"] ?? "");

  const formValues: ChannelFormValues = {

    channelNumber: channelNumberStr ? parseInt(channelNumberStr, 10) : undefined,
    channelSelector: sanitizeString(body["channelSelector"] ?? ""),
    guideTitle: sanitizeString(body["guideTitle"] ?? ""),
    hdhrEnabled: body["hdhrEnabled"] !== "false",
    logoUrl: sanitizeString(body["logoUrl"] ?? ""),
    name: sanitizeString(body["name"] ?? ""),
    profile: sanitizeString(body["profile"] ?? ""),
    stationId: sanitizeString(body["stationId"] ?? ""),
    url: sanitizeString(body["url"] ?? "")
  };

  const tags = parseTagInput(sanitizeString(body["tags"] ?? ""));

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

    errors["channelNumber"] = channelNumberError;
  }

  if(isCreate) {

    const keyError = validateChannelKey(key, true);

    if(keyError) {

      errors["key"] = keyError;
    }
  }

  const nameError = validateChannelName(formValues.name);

  if(nameError) {

    errors["name"] = nameError;
  }

  const urlError = validateChannelUrl(formValues.url);

  if(urlError) {

    errors["url"] = urlError;
  }

  const profileError = validateChannelProfile(formValues.profile, validProfiles);

  if(profileError) {

    errors["profile"] = profileError;
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

/* Field category sets used by handlePredefinedEdit to route per-field saves to the correct stored entry. Identity fields live on the canonical entry; binding
 * fields live on the active variant entry (when a non-canonical service is resolved) or the canonical entry (when canonical is active). Built once from the
 * type-system source of truth so renaming or adding a field surfaces here at compile time via the as-const tuples in types/channels.ts.
 */
const IDENTITY_FIELD_SET = new Set<string>(CHANNEL_IDENTITY_KEYS);
const BINDING_FIELD_SET = new Set<string>(CHANNEL_BINDING_KEYS);

/**
 * Filters a delta to only the fields in the allowlist. Used to split a full delta into identity-only and binding-only halves so the routed save can write
 * each half to the correct stored entry.
 */
function filterDeltaFields(delta: ChannelDelta, allowlist: ReadonlySet<string>): ChannelDelta {

  const filtered: Record<string, unknown> = {};

  for(const [ field, value ] of Object.entries(delta)) {

    if(allowlist.has(field)) {

      filtered[field] = value;
    }
  }

  return filtered;
}

/**
 * Applies the new variant-binding delta to an existing variant stored entry while preserving any non-binding fields the entry already carries (notably
 * canonicalKey). Removes any prior binding-field overrides that the new delta does not include - the new delta is the complete declaration of binding-field
 * customization for this save. Returns null when the resulting entry would be empty (no binding overrides, no preserved non-binding fields), signalling the
 * caller to delete the entry entirely.
 */
function applyVariantDelta(existing: StoredChannel | undefined, delta: ChannelDelta): StoredChannel | null {

  const next: Record<string, unknown> = { ...(existing as Record<string, unknown> | undefined) };

  // Strip prior binding-field overrides; the new delta is authoritative for this category.
  for(const field of CHANNEL_BINDING_KEYS) {

    Reflect.deleteProperty(next, field);
  }

  // Apply the new binding-field overrides.
  Object.assign(next, delta);

  return (Object.keys(next).length > 0) ? next : null;
}

/**
 * Handles PUT /config/channels/:key for a channel that has a predefined base. Routes the submitted form values to the correct stored entry per field category:
 * identity fields go to the canonical entry; binding fields go to the active variant entry (when a non-canonical service is resolved via explicit selection or
 * service-filter fallback) or the canonical entry (when canonical service is active). All writes happen in a single atomic mutate so per-field routing is
 * indistinguishable from a single transaction.
 *
 * The form's "no changes" detection compares submitted values against the resolved display channel (which may be a variant), so saving without modifications is
 * a true no-op even when a variant is active. The "values match predefined" path is an implicit revert: with a variant active, both stored entries (canonical
 * override and variant override) are deleted; without a variant, only the canonical entry is deleted. The "values match a sibling variant" path is unchanged -
 * it switches the service selection to that variant and clears the canonical override.
 * @param key - The canonical channel key being edited.
 * @param predefinedBase - The canonical predefined channel for this key.
 * @param formValues - The normalized form values.
 * @param tags - The sorted tag array.
 * @param res - The Express response.
 */
async function handlePredefinedEdit(key: string, predefinedBase: ResolvedChannel, formValues: ChannelFormValues, tags: readonly string[], res: Response): Promise<void> {

  // First check: does the submission match what the form showed? The edit form is pre-populated with the resolved display channel (which may be a variant), so
  // saving without modification should be a no-op - preserve all existing overrides on both entries.
  const resolvedKey = resolveServiceKey(key);
  const displayChannel = getResolvedChannel(resolvedKey) ?? predefinedBase;

  if(channelMatches(displayChannel, formValues, tags)) {

    sendSuccess(res, { data: { key }, message: "No changes to save." });

    return;
  }

  // Determine the active variant. Undefined when the canonical service is active (no override needed); otherwise the variant key the canonical resolves to.
  const activeVariantKey = (resolvedKey === key) ? undefined : resolvedKey;
  const predefinedVariant = activeVariantKey ? PREDEFINED_CHANNELS[activeVariantKey] : undefined;

  // Compute the canonical-relative delta. This determines the no-op-vs-canonical case (revert path) and supplies identity-field values for the canonical
  // entry. With a variant active, binding-field values in this delta are computed against canonical's binding (e.g., Cox URL) - those values are not what we
  // want to store; the variant-relative delta below has the correct binding-field comparisons.
  const { delta: canonicalRelativeDelta, hasChanges: canonicalHasChanges } = computePredefinedDelta(predefinedBase, formValues, tags);

  if(!canonicalHasChanges) {

    // Submitted values match the canonical predefined exactly. With a variant active, this means the user has cleared all customizations from both entries
    // (identity and binding both equal canonical) - delete both entries. Without a variant, only the canonical entry exists to delete. Both cases route through
    // the shared clearChannelOverrides helper.
    const hasCanonicalEntry = isUserChannel(key);
    const hasVariantEntry = activeVariantKey ? isUserChannel(activeVariantKey) : false;

    if(hasCanonicalEntry || hasVariantEntry) {

      let clearedEntry: StoredChannel | undefined;

      await mutateChannels((data) => {

        clearedEntry = clearChannelOverrides(data.channels, key);
      });

      LOG.info("Channel '%s' reverted to predefined defaults (edit matched predefined values).", key);

      sendSuccess(res, { affectedKeys: [key], data: { key }, message: "Channel '" + key + "' reverted to defaults." + playlistHintForStored(clearedEntry) });

      return;
    }

    sendSuccess(res, { data: { key }, message: "No changes to save." });

    return;
  }

  // Sibling variant match: the submitted values match a different variant's predefined definition exactly. That's an implicit revert-to-that-variant -
  // delete the canonical override and switch the service selection. Variant-stored overrides on the active variant are also cleared since the user is
  // explicitly reverting to a sibling.
  const matchedVariantKey = isUserChannel(key) ? findMatchingVariant(key, formValues, tags) : undefined;

  if(matchedVariantKey) {

    let variantRevertHint = "";

    await mutateChannels((data) => {

      variantRevertHint = playlistHintForStored(data.channels[key]);

      Reflect.deleteProperty(data.channels, key);

      // Clear any stored override on the previously-active variant since the user is switching away from it.
      if(activeVariantKey && (activeVariantKey !== matchedVariantKey)) {

        Reflect.deleteProperty(data.channels, activeVariantKey);
      }

      data.serviceSelections[key] = matchedVariantKey;
    });

    LOG.info("Channel '%s' reverted to variant '%s' (edit matched variant values).", key, matchedVariantKey);

    sendSuccess(res, { affectedKeys: [key], data: { key }, message: "Channel '" + key + "' reverted to defaults." + variantRevertHint });

    return;
  }

  // Real edit. Route per-field to the correct stored entry. With a variant active: identity fields go to canonical, binding fields go to variant (computed
  // against the predefined variant's binding values, since that's the right baseline for the variant entry). Without a variant: everything goes to canonical
  // as before.
  if(activeVariantKey && predefinedVariant) {

    const { delta: variantRelativeDelta } = computePredefinedDelta(predefinedVariant, formValues, tags);

    const canonicalEntryDelta = filterDeltaFields(canonicalRelativeDelta, IDENTITY_FIELD_SET);
    const variantEntryDelta = filterDeltaFields(variantRelativeDelta, BINDING_FIELD_SET);

    await mutateChannels((data) => {

      // Canonical entry: replace with identity-only delta. Empty delta -> remove the entry.
      if(Object.keys(canonicalEntryDelta).length > 0) {

        data.channels[key] = canonicalEntryDelta;
      } else {

        Reflect.deleteProperty(data.channels, key);
      }

      // Variant entry: merge binding-only delta with the entry's preserved non-binding fields (notably canonicalKey). Empty result -> remove the entry.
      const next = applyVariantDelta(data.channels[activeVariantKey], variantEntryDelta);

      if(next) {

        data.channels[activeVariantKey] = next;
      } else {

        Reflect.deleteProperty(data.channels, activeVariantKey);
      }
    });
  } else {

    // No variant active - everything routes to canonical. The service selection is cleared so the dropdown reflects the "Custom" state if the user has now
    // diverged from any sibling variant.
    await mutateChannels((data) => {

      data.channels[key] = canonicalRelativeDelta;
      Reflect.deleteProperty(data.serviceSelections, key);
    });
  }

  LOG.info("User channel '%s' updated.", key);

  if(formValues.stationId) {

    updateChannelLogo(formValues.name || key, formValues.stationId);
  }

  sendSuccess(res, {

    affectedKeys: [key],
    data: { key },
    message: "Channel '" + key + "' updated successfully." + playlistHintForDelta(canonicalRelativeDelta)
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
    const key = body["key"]?.trim();

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

    await mutateChannels((data) => {

      data.channels[key] = channel;
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

    await mutateChannels((data) => {

      // User-only path: the entry is structurally a CanonicalChannel (no predefined exists for this key), which assigns to ResolvedChannel for the
      // playlistHintForChange comparison below.
      const oldChannel = (key in data.channels) ? data.channels[key] as ResolvedChannel : undefined;

      data.channels[key] = channel;
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

    await mutateChannels((data) => {

      Reflect.deleteProperty(data.channels, key);
    });

    LOG.info("User channel '%s' deleted.", key);

    sendSuccess(res, {

      affectedKeys: [key],
      data: { key },
      message: "Channel '" + key + "' deleted successfully.",
      playlistHint: true
    });
  }));

  // POST /config/channels/:key/revert - Remove the override of a predefined channel, restoring it to defaults. Mirrors the PUT handler's per-field routing model
  // in reverse: revert clears both the canonical-stored override (identity fields) and the active-variant-stored override (binding fields), since either one
  // alone would leave the channel in a partially-customized state. The active variant is determined by resolveServiceKey, so service-filter fallback and explicit
  // service selection both surface the variant entry that needs clearing.
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

    // Determine the active variant. Undefined when the canonical service is active (no variant override possible); otherwise the variant key the canonical
    // resolves to via explicit service selection or service-filter fallback.
    const resolvedKey = resolveServiceKey(key);
    const activeVariantKey = (resolvedKey === key) ? undefined : resolvedKey;

    const hasCanonicalEntry = isUserChannel(key);
    const hasVariantEntry = activeVariantKey ? isUserChannel(activeVariantKey) : false;

    if(!hasCanonicalEntry && !hasVariantEntry) {

      sendValidationError(res, "Cannot revert '" + key + "': no override exists.");

      return;
    }

    let clearedEntry: StoredChannel | undefined;

    await mutateChannels((data) => {

      clearedEntry = clearChannelOverrides(data.channels, key);
    });

    LOG.info("Channel '%s' reverted to predefined defaults.", key);

    sendSuccess(res, {

      affectedKeys: [key],
      data: { key },
      message: "Channel '" + key + "' reverted to defaults." + playlistHintForStored(clearedEntry)
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

    // presentFields.length was validated above to be exactly 1. Narrow with an explicit check - TypeScript does not propagate the length check to the index
    // access, but the guard is free at runtime (an impossible branch) and keeps the rest of the handler type-safe without a non-null assertion.
    const [field] = presentFields;

    if(field === undefined) {

      return;
    }

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

    await mutateChannels((data) => {

      // The inline-edit endpoint writes scalar override fields that all align with ChannelDelta's nullable shape. Reuse the stored entry when present so
      // existing fields survive, otherwise start from an empty delta. mutateChannels() persists the resulting record either way.
      const stored: ChannelDelta = data.channels[key] ?? {};
      const delta = stored;

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

      data.channels[key] = stored;
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
