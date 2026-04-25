/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * browse.ts: Browse-channels modal endpoint.
 *
 * The browse modal on the Channels tab shows available channels from each service and lets the user add new channels, enable disabled predefineds, switch a
 * channel's active service, or revert a channel away from a service. All four operations are dispatched from a single request body so a bulk selection can be
 * applied atomically.
 */
import type { Express, Request, Response } from "express";
import { LOG, generateChannelKey, sanitizeString } from "../../../../utils/index.js";
import { type UserChannel, disablePredefinedChannels, enablePredefinedChannels, isPredefinedChannel, mutateChannels, saveServiceSelections,
  validateChannelUrl } from "../../../../config/userChannels.js";
import { getServiceTagForChannel, resolveServiceKey, setServiceSelection } from "../../../../config/services.js";
import { sendSuccess, sendValidationError } from "../http/envelope.js";
import { PREDEFINED_CHANNELS } from "../../../../channels/index.js";
import { buildServiceFilterWarning } from "../http/serviceWarning.js";
import { route } from "../http/handler.js";

interface ModifyEntry {

  action?: string;
  canonicalKey?: string;
  channelSelector?: string;
  name?: string;
  serviceSlug?: string;
  stationId?: string;
  url?: string;
}

/**
 * Builds a UserChannel object from a modify-request entry. When canonicalKey is set, the result is a service variant stored as a delta against its canonical -
 * identity fields that the user explicitly provides (e.g., stationId for a local affiliate) are preserved so the variant carries its own identity; the delta
 * normalizer later strips any field whose value matches the canonical. Standalone channels (no canonicalKey) need the full identity on the entry itself
 * because there is no canonical to inherit from.
 *
 * Submitting a stationId for a variant is how a user persists per-variant identity - the canonical may lack a stationId (e.g., the generic "abc" network has
 * no single station ID; only its local affiliates do), and in that case the variant's value is the only source of identity for EPG matching and HDHR.
 * @param entry - The raw entry fields as submitted by the browse modal.
 * @param name - The sanitized display name.
 * @param url - The sanitized URL.
 * @param selector - The sanitized channelSelector.
 * @param canonicalKey - The canonical channel key when this is a service variant, otherwise undefined.
 * @returns The constructed UserChannel with only the relevant fields populated.
 */
function buildUserChannel(entry: ModifyEntry, name: string, url: string, selector: string, canonicalKey?: string): UserChannel {

  const channel: UserChannel = {

    ...(canonicalKey ? { canonicalKey } : { name }),
    channelSelector: selector || undefined,
    url
  };

  // Variant-specific stationId: broadcast network canonicals (abc, cbs, fox, nbc) typically lack a stationId because there is no single national ID - the
  // ID belongs to the local affiliate. Preserving the submitted stationId on the variant is how the user records that identity. Values that happen to match
  // the canonical are stripped later by the delta normalizer, so there is no redundant storage.
  if(entry.stationId) {

    channel.stationId = sanitizeString(entry.stationId);
  }

  return channel;
}

/**
 * Registers the /config/channels/modify endpoint on the Express application.
 * @param app - The Express application.
 */
export function registerBrowseRoutes(app: Express): void {

  // POST /config/channels/modify - Apply channel modifications from the browse modal. Handles four action types: 'add' creates new user channels, 'enable'
  // re-enables a disabled predefined channel and sets its service, 'switch' changes the service selection for an existing channel, and 'remove' reverts a channel
  // to its canonical service (disabling it if no alternative service is available). Channel writes and service selection changes are batched into single saves to
  // avoid redundant file I/O.
  app.post("/config/channels/modify", route("apply changes", async (req: Request, res: Response) => {

    const body = req.body as { channels?: ModifyEntry[] };
    const entries = body.channels;

    if(!Array.isArray(entries) || (entries.length === 0)) {

      sendValidationError(res, "No channels provided.");

      return;
    }

    const affectedKeys = new Set<string>();
    const keysToDisable = new Set<string>();
    const errors: string[] = [];
    let added = 0;
    let removed = 0;
    let switched = 0;

    // Flags tracking whether channels or selections changed during the mutation. Declared as a state object rather than bare booleans so that TypeScript's control-
    // flow narrowing does not produce false positives when reading the flags after the callback (which mutates them via closure).
    const modified = { channels: false, selections: false };

    // Process all entries inside a single transactional mutation to avoid TOCTOU races between load and save.
    await mutateChannels((existingChannels) => {

      const allKeys = new Set([ ...Object.keys(PREDEFINED_CHANNELS), ...Object.keys(existingChannels) ]);

      for(const entry of entries) {

        const action = entry.action ?? "add";
        const name = sanitizeString(entry.name?.trim() ?? "");
        const serviceSlug = entry.serviceSlug?.trim() ?? "";

        // Enable re-enables a disabled predefined channel, then falls through to switch logic to set the service selection. Switch changes the service selection to
        // point to the browsed service's variant, creating the variant as a user channel if needed.
        if((action === "switch") || (action === "enable")) {

          const canonicalKey = entry.canonicalKey?.trim() ?? "";

          if(!canonicalKey) {

            errors.push("Entry for '" + name + "' missing canonical key.");

            continue;
          }

          const variantKey = canonicalKey + "-" + serviceSlug;

          // If no variant exists for this service, create one as a user channel. The canonicalKey parameter tells buildUserChannel to produce a variant (service-
          // specific fields only, no identity fields).
          if(!allKeys.has(variantKey)) {

            existingChannels[variantKey] = buildUserChannel(entry, name, sanitizeString(entry.url?.trim() ?? ""),
              sanitizeString(entry.channelSelector?.trim() ?? ""), canonicalKey);
            allKeys.add(variantKey);
            modified.channels = true;
          }

          setServiceSelection(canonicalKey, variantKey);
          modified.selections = true;
          switched++;
          affectedKeys.add(canonicalKey);

          continue;
        }

        // Service removal. Three-tier fallback: clear the selection and let resolveServiceKey find the next enabled variant or canonical; if the resolved service is
        // still this service (no alternative exists), disable the predefined channel or delete the user channel.
        if(action === "remove") {

          const canonicalKey = entry.canonicalKey?.trim() ?? "";

          if(!canonicalKey) {

            errors.push("Remove entry for '" + name + "' missing canonical key.");

            continue;
          }

          setServiceSelection(canonicalKey, canonicalKey);
          modified.selections = true;

          const resolvedKey = resolveServiceKey(canonicalKey);
          const resolvedTag = getServiceTagForChannel(resolvedKey);

          if(resolvedTag === serviceSlug) {

            // No alternative service exists. Disable predefined channels or delete user channels.
            if(isPredefinedChannel(canonicalKey)) {

              keysToDisable.add(canonicalKey);
            } else {

              Reflect.deleteProperty(existingChannels, canonicalKey);
              modified.channels = true;
            }
          }

          removed++;
          affectedKeys.add(canonicalKey);

          continue;
        }

        // Add. Creates a new user channel for channels not yet in the lineup. The browse modal sends 'add' only for genuinely new channels (no existing
        // canonical). Channels that match existing canonicals appear as 'switch' state in the modal.
        const url = sanitizeString(entry.url?.trim() ?? "");
        const channelSelector = sanitizeString(entry.channelSelector?.trim() ?? "");

        if(!name) {

          errors.push("Channel entry missing name.");

          continue;
        }

        if(!url) {

          errors.push("Channel '" + name + "' missing URL.");

          continue;
        }

        const urlError = validateChannelUrl(url);

        if(urlError) {

          errors.push("Channel '" + name + "': " + urlError);

          continue;
        }

        const baseKey = generateChannelKey(name);

        if(!baseKey) {

          errors.push("Could not generate key for channel '" + name + "'.");

          continue;
        }

        const canonicalExists = allKeys.has(baseKey);
        const key = (canonicalExists && serviceSlug) ? baseKey + "-" + serviceSlug : baseKey;

        if(allKeys.has(key)) {

          continue;
        }

        // When the key differs from baseKey, this is a service variant - pass baseKey as canonicalKey so buildUserChannel produces a variant with only service-
        // specific fields. Standalone channels (key === baseKey) get the full channel object with identity fields.
        const newChannel = buildUserChannel(entry, name, url, channelSelector, (key !== baseKey) ? baseKey : undefined);

        existingChannels[key] = newChannel;
        allKeys.add(key);
        modified.channels = true;
        added++;
        affectedKeys.add(canonicalExists ? baseKey : key);
      }
    });

    // Enable predefined channels that had the "enable" action. Runs after the mutation since the config write makes the channel visible in the lineup.
    const enableKeys = entries
      .filter((e): e is typeof e & { canonicalKey: string } => (e.action === "enable") && Boolean(e.canonicalKey?.trim()))
      .map((e) => e.canonicalKey.trim());

    if(enableKeys.length > 0) {

      await enablePredefinedChannels(enableKeys);
    }

    // When channels were modified, the mutation already persisted selections as part of the write. When only selections changed, save them explicitly.
    if(!modified.channels && modified.selections) {

      await saveServiceSelections();
    }

    await disablePredefinedChannels([...keysToDisable]);

    for(const disabledKey of keysToDisable) {

      affectedKeys.add(disabledKey);
    }

    const parts: string[] = [];

    if(added > 0) {

      parts.push("Added " + String(added) + " channel" + (added === 1 ? "" : "s") + ".");
    }

    if(switched > 0) {

      parts.push("Switched " + String(switched) + " channel" + (switched === 1 ? "" : "s") + ".");
    }

    if(removed > 0) {

      parts.push("Reverted " + String(removed) + " channel" + (removed === 1 ? "" : "s") + ".");
    }

    // Append the playlist-reload hint only when the batch actually produced changes. "No changes made." needs no reload, and the envelope's playlistHint option
    // is the canonical append path - matches bulk.ts and crud.ts no-change responses which also skip the hint.
    const hasChanges = (added + switched + removed) > 0;

    LOG.info("Browse channels completed: %d added, %d switched, %d removed.", added, switched, removed);

    // Check if the browsed service isn't in the active filter. All channels in a browse batch share the same service - derive the tag from the first add entry.
    const firstAddUrl = (added > 0) ? entries.find((e) => (e.action === "add"))?.url : undefined;
    const serviceWarning = firstAddUrl ? buildServiceFilterWarning(firstAddUrl) : undefined;

    sendSuccess(res, {

      affectedKeys: [...affectedKeys],
      message: hasChanges ? parts.join(" ") : "No changes made.",
      playlistHint: hasChanges,
      serviceWarning
    });
  }));
}
