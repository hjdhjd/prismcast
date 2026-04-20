/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * service.ts: Service selection, service filter, and cross-channel service bulk endpoints.
 *
 * Service selection controls which variant of a multi-service channel is used at stream time (e.g., Hulu vs ESPN.com for ESPN). The filter restricts which
 * service tags participate in the playlist and table. Bulk assign/restore operate on all channels at once for fast switching across a provider.
 */
import type { Express, Request, Response } from "express";
import { getAllServiceTags, getCanonicalKey, getChannelServiceLabel, getEnabledServices, getResolvedChannel, getServiceGroup, getServiceSelection,
  getServiceTagForChannel, saveEnabledServices, setEnabledServices, setServiceSelection } from "../../../../config/services.js";
import { getChannelListing, saveServiceSelections } from "../../../../config/userChannels.js";
import { sendSuccess, sendValidationError } from "../http/envelope.js";
import { LOG } from "../../../../utils/index.js";
import type { Nullable } from "../../../../types/index.js";
import { buildChannelTableState } from "../table.js";
import { route } from "../http/handler.js";

/**
 * Registers the service-selection and service-filter endpoints on the Express application.
 * @param app - The Express application.
 */
export function registerServiceRoutes(app: Express): void {

  // PUT /config/channels/:key/service - Update service selection for a multi-service channel. Replaces the legacy POST /config/service endpoint.
  app.put("/config/channels/:key/service", route("update service", async (req: Request, res: Response) => {

    const params = req.params as { key?: string };
    const body = req.body as { service?: string };
    const channelKey = params.key?.trim();
    const serviceKey = body.service?.trim();

    if(!channelKey) {

      sendValidationError(res, "Channel key is required.");

      return;
    }

    if(!serviceKey) {

      sendValidationError(res, "Service key is required.");

      return;
    }

    // Canonicalize the channel key so selections are stored under the canonical, not variant keys.
    const canonicalKey = getCanonicalKey(channelKey);
    const serviceGroup = getServiceGroup(canonicalKey);

    if(!serviceGroup) {

      sendValidationError(res, "Channel '" + canonicalKey + "' does not have multiple services.");

      return;
    }

    const validServiceKeys = serviceGroup.variants.map((v) => v.key);

    if(!validServiceKeys.includes(serviceKey)) {

      sendValidationError(res, "Invalid service '" + serviceKey + "' for channel '" + canonicalKey + "'.");

      return;
    }

    setServiceSelection(canonicalKey, serviceKey);

    await saveServiceSelections();

    const canonicalChannel = getResolvedChannel(canonicalKey);
    const variantChannel = getResolvedChannel(serviceKey);
    const channelName = canonicalChannel?.name ?? canonicalKey;
    const serviceLabel = variantChannel ? getChannelServiceLabel(variantChannel) : serviceKey;

    LOG.info("Service for %s changed to %s.", channelName, serviceLabel);

    sendSuccess(res, { affectedKeys: [canonicalKey] });
  }));

  // POST /config/service-filter - Update the service filter (enabled service tags). Empty array means no filter (all services visible).
  app.post("/config/service-filter", route("update service filter", async (req: Request, res: Response) => {

    const body = req.body as { enabledServices?: string[] };
    const tags = body.enabledServices;

    if(!Array.isArray(tags)) {

      sendValidationError(res, "enabledServices must be an array.");

      return;
    }

    // Validate all tags are known. Tags already in enabledServices are accepted even if no current channel or profile produces them - this allows stale tags to be
    // removed via the UI without blocking the request.
    const knownTags = new Set(getAllServiceTags().map((t) => t.tag));
    const currentTags = new Set(getEnabledServices());

    for(const tag of tags) {

      if(!knownTags.has(tag) && !currentTags.has(tag)) {

        sendValidationError(res, "Unknown service tag: " + tag);

        return;
      }
    }

    setEnabledServices(tags);

    await saveEnabledServices();

    LOG.info("Service filter updated: %s.", tags.length > 0 ? tags.join(", ") : "all services");

    // Counts-only patch - the client applies CSS visibility changes itself and only needs updated summary counters.
    const { counts, scopeCounts } = buildChannelTableState();

    sendSuccess(res, { patch: { counts, rows: [], scopeCounts } });
  }));

  // POST /config/service-bulk-assign - Set all multi-service channels to a specific service tag, where available.
  app.post("/config/service-bulk-assign", route("bulk assign service", async (req: Request, res: Response) => {

    const body = req.body as { service?: string };
    const serviceTag = body.service?.trim();

    if(!serviceTag) {

      sendValidationError(res, "Service tag is required.");

      return;
    }

    let affected = 0;
    const previousSelections: Record<string, Nullable<string>> = {};
    const selections: Record<string, { profile: Nullable<string>; variant: string }> = {};

    const listing = getChannelListing();

    for(const entry of listing) {

      const group = getServiceGroup(entry.key);

      if(!group || (group.variants.length <= 1)) {

        continue;
      }

      const matchingVariant = group.variants.find((v) => (getServiceTagForChannel(v.key) === serviceTag));

      if(matchingVariant) {

        // Snapshot the current selection before overwriting so the client can offer undo.
        const currentVariant = getServiceSelection(entry.key);

        previousSelections[entry.key] = currentVariant ?? null;

        setServiceSelection(entry.key, matchingVariant.key);
        affected++;

        const resolvedChannel = getResolvedChannel(matchingVariant.key);

        selections[entry.key] = { profile: resolvedChannel?.profile ?? null, variant: matchingVariant.key };
      }
    }

    await saveServiceSelections();

    LOG.info("Bulk assign to '%s': %d of %d channels affected.", serviceTag, affected, listing.length);

    sendSuccess(res, { data: { affected, previousSelections, selections, total: listing.length } });
  }));

  // POST /config/service-bulk-restore - Restore previous service selections (undo bulk assign).
  app.post("/config/service-bulk-restore", route("bulk restore services", async (req: Request, res: Response) => {

    const body = req.body as { selections?: Record<string, Nullable<string>> };
    const previousSelections = body.selections;

    if(!previousSelections || (typeof previousSelections !== "object")) {

      sendValidationError(res, "Selections map is required.");

      return;
    }

    let restored = 0;
    const selections: Record<string, { profile: Nullable<string>; variant: string }> = {};

    for(const [ key, variantKey ] of Object.entries(previousSelections)) {

      const group = getServiceGroup(key);

      if(!group) {

        continue;
      }

      // A null value means the channel was using the default (canonical) selection. Restoring by setting the selection to the canonical key clears the override.
      if(variantKey === null) {

        setServiceSelection(key, key);
      } else {

        // Validate the variant belongs to this channel's service group before restoring.
        const isValid = group.variants.some((v) => (v.key === variantKey));

        if(!isValid) {

          continue;
        }

        setServiceSelection(key, variantKey);
      }

      restored++;

      // Build the same selection response format as bulk assign for client-side UI updates.
      const effectiveKey = variantKey ?? key;
      const resolvedChannel = getResolvedChannel(effectiveKey);

      selections[key] = { profile: resolvedChannel?.profile ?? null, variant: effectiveKey };
    }

    await saveServiceSelections();

    LOG.info("Bulk restore: %d channel(s) reverted.", restored);

    sendSuccess(res, { data: { restored, selections } });
  }));
}
