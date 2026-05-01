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
import { type UserChannel, disablePredefinedChannels, enablePredefinedChannels, isPredefinedChannel, mutateChannels,
  validateChannelUrl } from "../../../../config/userChannels.js";
import { getServiceTagForChannel, resolveServiceKey } from "../../../../config/services.js";
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
 * Builds a UserChannel object from a modify-request entry. When canonicalKey is set, the result is a service variant carrying only the binding fields needed
 * to tune the channel via that service (canonicalKey, url, channelSelector). Identity comes from the canonical at resolution time per the architectural
 * principle - variants are pure tuning data. When canonicalKey is absent, the result is a standalone canonical channel that owns its own identity (name,
 * stationId, etc.).
 *
 * Per-affiliate identity (e.g., a local Chicago Fox affiliate that needs its own station ID) is modeled as a separate canonical channel rather than as a
 * variant carrying override identity. The browse modal's "add" action (no canonicalKey) is the path for that case; "switch"/"enable" actions create variants
 * which inherit identity from their canonical.
 * @param entry - The raw entry fields as submitted by the browse modal.
 * @param name - The sanitized display name (used for standalone canonicals only; variants inherit the name from the canonical).
 * @param url - The sanitized URL.
 * @param selector - The sanitized channelSelector.
 * @param canonicalKey - The canonical channel key when this is a service variant, otherwise undefined.
 * @returns The constructed UserChannel with only the relevant fields populated.
 */
function buildUserChannel(entry: ModifyEntry, name: string, url: string, selector: string, canonicalKey?: string): UserChannel {

  if(canonicalKey) {

    // Service variant: binding fields only. stationId from the modify entry (often supplied by service-side discovery) is intentionally ignored - identity is
    // canonical-only per the architectural principle. If a user wants per-affiliate identity, they create the affiliate as a separate canonical via the "add"
    // action of the browse modal.
    return {

      canonicalKey,
      channelSelector: selector || undefined,
      url
    };
  }

  // Standalone canonical: carries its own identity. stationId from the modify entry is preserved here because the standalone is the identity authority for
  // this channel.
  const channel: UserChannel = {

    channelSelector: selector || undefined,
    name,
    url
  };

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

    // Process all entries inside a single transactional mutation. Both channel changes and service-selection changes go through data.* directly so the entire
    // batch lands as one atomic write. This eliminates the prior set-then-save pattern (sync setServiceSelection followed by saveServiceSelections) and the
    // associated "did anything change?" tracking - the framework persists exactly what the fn produced.
    await mutateChannels((data) => {

      const allKeys = new Set([ ...Object.keys(PREDEFINED_CHANNELS), ...Object.keys(data.channels) ]);

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

            data.channels[variantKey] = buildUserChannel(entry, name, sanitizeString(entry.url?.trim() ?? ""),
              sanitizeString(entry.channelSelector?.trim() ?? ""), canonicalKey);
            allKeys.add(variantKey);
          }

          // Selecting a variant: store the explicit selection. variantKey is canonicalKey + "-" + serviceSlug, so it's never equal to canonicalKey here.
          data.serviceSelections[canonicalKey] = variantKey;
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

          // Clear the selection by deleting it (selecting the canonical default is represented as no entry).
          Reflect.deleteProperty(data.serviceSelections, canonicalKey);

          const resolvedKey = resolveServiceKey(canonicalKey);
          const resolvedTag = getServiceTagForChannel(resolvedKey);

          if(resolvedTag === serviceSlug) {

            // No alternative service exists. Disable predefined channels or delete user channels.
            if(isPredefinedChannel(canonicalKey)) {

              keysToDisable.add(canonicalKey);
            } else {

              Reflect.deleteProperty(data.channels, canonicalKey);
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

        data.channels[key] = newChannel;
        allKeys.add(key);
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
