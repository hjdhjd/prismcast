/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * bulk.ts: Cross-channel bulk operations on the currently visible channel set.
 *
 * These endpoints operate on "visible" channels - entries that are both enabled and available under the current service filter. They reuse the same listing
 * snapshot semantics so a bulk action targets exactly what the user sees in the table.
 */
import type { Express, Request, Response } from "express";
import { VALID_SORT_FIELDS, compareChannelSort } from "../../../../config/services.js";
import { getVisibleChannels, isInVocabulary, isVisibleChannel, mutateChannels, tagsMatch,
  transformChannelTags } from "../../../../config/userChannels.js";
import { sendError, sendSuccess, sendValidationError } from "../http/envelope.js";
import type { ChannelSortField } from "../../../../types/index.js";
import { LOG } from "../../../../utils/index.js";
import { route } from "../http/handler.js";

/**
 * Registers the bulk channel-operation endpoints on the Express application.
 * @param app - The Express application.
 */
export function registerBulkRoutes(app: Express): void {

  // POST /config/channels/auto-number - Assign sequential channel numbers to visible channels in the current sort order, or clear all channel numbers when
  // start is 0. Overwrites existing channel numbers for affected channels.
  app.post("/config/channels/auto-number", route("auto-number channels", async (req: Request, res: Response) => {

    const body = req.body as { sortDirection?: string; sortField?: string; start?: number };
    const start = (typeof body.start === "number") ? body.start : 1;
    const clearMode = (start === 0);
    const sortField: ChannelSortField = (body.sortField as ChannelSortField | undefined) ?? "name";
    const sortDir = (body.sortDirection === "desc") ? "desc" : "asc";

    if(!clearMode && ((start < 1) || (start > 99999))) {

      sendValidationError(res, "Starting number must be between 1 and 99999.");

      return;
    }

    if(!VALID_SORT_FIELDS.has(sortField)) {

      sendValidationError(res, "Invalid sort field.");

      return;
    }

    const listing = getVisibleChannels();

    listing.sort((a, b) => compareChannelSort(a.channel, a.key, b.channel, b.key, sortField, sortDir));

    const affectedKeys: string[] = [];

    await mutateChannels((channels) => {

      if(clearMode) {

        // Clear channel numbers from all visible channels. Null signals "clear this field" - the normalizer handles the storage conventions.
        for(const entry of listing) {

          const existing = channels[entry.key] ?? {};

          existing.channelNumber = null;
          channels[entry.key] = existing;
          affectedKeys.push(entry.key);
        }

        return;
      }

      // Assign sequential numbers starting from the requested start value. Cap at 99999 to match the validation range.
      for(const [ i, entry ] of listing.entries()) {

        const num = start + i;

        if(num > 99999) {

          break;
        }

        const existing = channels[entry.key] ?? {};

        existing.channelNumber = num;
        channels[entry.key] = existing;
        affectedKeys.push(entry.key);
      }
    });

    const action = clearMode ? "Cleared" : "Numbered";

    LOG.info("%s channel numbers for %d channels.", action, affectedKeys.length);

    const message = clearMode ?
      ("Cleared channel numbers from " + String(affectedKeys.length) + " channels.") :
      ("Numbered " + String(affectedKeys.length) + " channels.");

    sendSuccess(res, { affectedKeys, message, playlistHint: true });
  }));

  // POST /config/channels/hdhr-bulk - Toggle HDHomeRun lineup inclusion for all visible channels.
  app.post("/config/channels/hdhr-bulk", route("toggle HDHR settings", async (req: Request, res: Response) => {

    const body = req.body as { enable?: boolean };
    const enable = body.enable === true;

    const listing = getVisibleChannels();

    const affectedKeys: string[] = [];

    // Pre-compute which channels need updating. The listing snapshot is consistent since no concurrent mutation can change it before we enter the lock.
    for(const entry of listing) {

      const current = entry.channel.hdhrEnabled !== false;

      if(current === enable) {

        continue;
      }

      affectedKeys.push(entry.key);
    }

    if(affectedKeys.length === 0) {

      sendSuccess(res, { message: "No changes needed." });

      return;
    }

    await mutateChannels((channels) => {

      for(const key of affectedKeys) {

        const existing = channels[key] ?? {};

        existing.hdhrEnabled = enable ? null : false;
        channels[key] = existing;
      }
    });

    const action = enable ? "included in" : "excluded from";

    LOG.info("Bulk HDHR toggle: %d channels %s HDHR lineup.", affectedKeys.length, action);

    const message = String(affectedKeys.length) + " channel(s) " + action + " the HDHomeRun lineup.";

    sendSuccess(res, { affectedKeys, message });
  }));

  // POST /config/channels/bulk-tags - Add or remove a tag on all enabled, service-available channels. Reuses the same visibility filter as the other bulk actions.
  // transformChannelTags handles loading, delta normalization, and persistence. Returns a channel table patch for affected rows plus the tag UI bundle so the
  // filter dropdown and tag manager modal stay in sync.
  app.post("/config/channels/bulk-tags", route("update tags", async (req: Request, res: Response) => {

    const body = req.body as { action?: string; tag?: string };
    const action = body.action;
    const tag = typeof body.tag === "string" ? body.tag.trim() : "";

    if((action !== "add") && (action !== "remove")) {

      sendValidationError(res, "Action must be 'add' or 'remove'.");

      return;
    }

    if(!tag) {

      sendValidationError(res, "Tag is required.");

      return;
    }

    if(!isInVocabulary(tag)) {

      sendValidationError(res, "Unknown tag: " + tag + ".");

      return;
    }

    // Use tagsMatch for case-insensitive membership so submitting "News" doesn't duplicate an existing "news" entry and removing "News" also clears a "news" entry.
    // Tag identity is case-insensitive throughout the system (per tagsMatch); mutation paths must honor that.
    const { affectedKeys, error } = await transformChannelTags(
      isVisibleChannel,
      (tags) => (action === "add") ?
        (tags.some((t) => tagsMatch(t, tag)) ? tags : [ ...tags, tag ]) :
        tags.filter((t) => !tagsMatch(t, tag))
    );

    if(error) {

      sendError(res, 400, { error });

      return;
    }

    if(affectedKeys.length === 0) {

      sendSuccess(res, { message: "No changes needed." });

      return;
    }

    const verb = (action === "add") ? "added to" : "removed from";

    LOG.info("Bulk tag %s: %s on %d channels.", action, tag, affectedKeys.length);

    const message = "Tag '" + tag + "' " + verb + " " + String(affectedKeys.length) + " channel(s).";

    // Include the full tag UI bundle so the filter dropdown and tag manager modal stay in sync. Tag changes don't affect the M3U playlist, so no playlist hint.
    sendSuccess(res, { affectedKeys, message, tags: true });
  }));
}
